const express = require('express');
const { renderMedia, selectComposition } = require('@remotion/renderer');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { S3Client } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const Redis = require('ioredis');
const { v4: uuidv4 } = require('uuid');
const util = require('util');

require('dotenv').config();
const app = express();
app.use(cors());
app.use(express.json());

// Configure Redis client
const redisClient = new Redis({
  host: process.env.REDIS_HOST || 'localhost',
  port: parseInt(process.env.REDIS_PORT || '6379'),
  password: process.env.REDIS_PASSWORD,
  // For Redis Cloud, we need to specify the database name rather than an index
  db: process.env.REDIS_DB ? parseInt(process.env.REDIS_DB) : 0,
  retryStrategy: (times) => {
    // Exponential backoff with max 30 seconds
    const delay = Math.min(times * 100, 30000);
    console.log(`Redis connection retry in ${delay}ms`);
    return delay;
  }
});

redisClient.on('error', (err) => {
  console.error('Redis connection error:', err);
});

redisClient.on('connect', () => {
  console.log('Connected to Redis');
});

// Configure S3 client
const s3Client = new S3Client({
  region: process.env.S3_REGION,
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY,
    secretAccessKey: process.env.S3_SECRET_KEY,
  },
});

// Job queue keys/prefixes
const JOB_QUEUE_KEY = 'remotion:job-queue';
const JOB_DATA_PREFIX = 'remotion:job:';
const JOB_STATUS_PREFIX = 'remotion:status:';
const JOB_RESULT_PREFIX = 'remotion:result:';

// Job statuses
const JOB_STATUS = {
  PENDING: 'pending',
  PROCESSING: 'processing',
  COMPLETED: 'completed',
  FAILED: 'failed',
};

// Store job in Redis
async function storeJob(jobId, jobType, inputProps) {
  try {
    // Store job data
    await redisClient.set(
      `${JOB_DATA_PREFIX}${jobId}`,
      JSON.stringify({
        jobId,
        jobType,
        inputProps,
        createdAt: new Date().toISOString(),
      }),
      'EX',
      60 * 60 * 24 * 7 // Expire after 7 days
    );

    // Set initial status
    await redisClient.set(
      `${JOB_STATUS_PREFIX}${jobId}`,
      JOB_STATUS.PENDING,
      'EX',
      60 * 60 * 24 * 7 // Expire after 7 days
    );

    // Add to queue for processing
    await redisClient.lpush(JOB_QUEUE_KEY, jobId);

    return true;
  } catch (error) {
    console.error(`Error storing job ${jobId}:`, error);
    return false;
  }
}

// Update job status
async function updateJobStatus(jobId, status, result = null) {
  try {
    await redisClient.set(
      `${JOB_STATUS_PREFIX}${jobId}`,
      status,
      'EX',
      60 * 60 * 24 * 7 // Expire after 7 days
    );

    if (result) {
      await redisClient.set(
        `${JOB_RESULT_PREFIX}${jobId}`,
        JSON.stringify(result),
        'EX',
        60 * 60 * 24 * 7 // Expire after 7 days
      );
    }

    return true;
  } catch (error) {
    console.error(`Error updating job status ${jobId}:`, error);
    return false;
  }
}

// Get job status and result
async function getJobInfo(jobId) {
  try {
    const [status, resultStr, dataStr] = await Promise.all([
      redisClient.get(`${JOB_STATUS_PREFIX}${jobId}`),
      redisClient.get(`${JOB_RESULT_PREFIX}${jobId}`),
      redisClient.get(`${JOB_DATA_PREFIX}${jobId}`)
    ]);

    if (!status) {
      return null; // Job not found
    }

    const result = resultStr ? JSON.parse(resultStr) : null;
    const data = dataStr ? JSON.parse(dataStr) : {};

    return {
      jobId,
      status,
      result,
      jobType: data.jobType,
      createdAt: data.createdAt,
      updatedAt: new Date().toISOString(),
    };
  } catch (error) {
    console.error(`Error getting job info ${jobId}:`, error);
    return null;
  }
}

// Upload to S3
async function uploadToS3(filename, fileContent, tempFilePath) {
  // Define S3 path
  const s3Key = `videos/${filename}`;

  console.log(`Uploading to S3: ${process.env.S3_BUCKET_NAME}/${s3Key}`);

  // Upload to S3 using multipart upload for larger files
  const upload = new Upload({
    client: s3Client,
    params: {
      Bucket: process.env.S3_BUCKET_NAME,
      Key: s3Key,
      Body: fileContent,
      ContentType: 'video/mp4',
      ACL: 'public-read', // Make the file publicly accessible
    },
  });

  const uploadResult = await upload.done();
  console.log("Upload result:", uploadResult);

  // Generate S3 URL
  const videoUrl = `https://${process.env.S3_BUCKET_NAME}.s3.${process.env.S3_REGION}.amazonaws.com/${s3Key}`;

  // Clean up the temporary file
  fs.unlinkSync(tempFilePath);
  console.log(`Temporary file removed: ${tempFilePath}`);

  return videoUrl;
}

// Process video jobs
async function processVideoJob(jobId) {
  try {
    console.log(`Processing job ${jobId}...`);

    // Update job status to processing
    await updateJobStatus(jobId, JOB_STATUS.PROCESSING);

    // Get job data
    const jobDataStr = await redisClient.get(`${JOB_DATA_PREFIX}${jobId}`);
    if (!jobDataStr) {
      throw new Error(`Job data not found for ${jobId}`);
    }

    const jobData = JSON.parse(jobDataStr);
    const { jobType, inputProps } = jobData;

    // Select composition based on job type
    let compositionId;

    if (jobType === 'reaction-video') {
      compositionId = 'ReactionVideo';
    } else if (jobType === 'video-compilation') {
      compositionId = 'VideoCompilation';
    } else if (jobType === 'hello') {
      compositionId = 'HelloWorld';
    } else {
      throw new Error(`Unknown job type: ${jobType}`);
    }

    // Select composition
    const composition = await selectComposition({
      serveUrl: process.env.REMOTION_SERVE_URL,
      id: compositionId,
      inputProps,
    });

    // Calculate duration based on job type
    const fps = composition.fps || 30;

    if (jobType === 'reaction-video') {
      // Calculate the total frames for a reaction video where clips play simultaneously
      const maxDuration = Math.max(
        ...inputProps.reactionVideoData.clips.map(clip => clip.durationInSeconds)
      );
      composition.durationInFrames = Math.round(maxDuration * fps);
    } else if (jobType === 'video-compilation') {
      // Calculate the total frames for a compilation where clips play sequentially
      composition.durationInFrames = inputProps.compilationData.clips.reduce((total, clip) => {
        return total + Math.round(clip.durationInSeconds * fps);
      }, 0);
    }

    // Make sure temp directory exists for temporary file storage
    const tempDir = path.join(__dirname, 'temp');
    if (!fs.existsSync(tempDir)) {
      console.log(`Creating temp directory: ${tempDir}`);
      fs.mkdirSync(tempDir, { recursive: true });
    }

    const filename = `${compositionId}-${jobId}.mp4`;
    const tempFilePath = path.join(tempDir, filename);

    console.log(`Temporary file will be saved to: ${tempFilePath}`);

    // Render the media
    const renderResult = await renderMedia({
      composition,
      serveUrl: process.env.REMOTION_SERVE_URL,
      codec: 'h264',
      outputLocation: tempFilePath,
      inputProps,
    });

    console.log("Render result:", renderResult);

    // Read the file
    const fileContent = fs.readFileSync(tempFilePath);

    // Upload to S3
    const videoUrl = await uploadToS3(filename, fileContent, tempFilePath);

    // Update job status to completed with result
    await updateJobStatus(jobId, JOB_STATUS.COMPLETED, {
      videoUrl,
      message: `${jobType} rendered and uploaded to S3 successfully`,
    });

    console.log(`Job ${jobId} completed successfully`);

    return { success: true, videoUrl };
  } catch (error) {
    console.error(`Error processing job ${jobId}:`, error);

    // Update job status to failed
    await updateJobStatus(jobId, JOB_STATUS.FAILED, {
      error: 'Failed to process video job',
      message: error.message,
    });

    return { success: false, error: error.message };
  }
}

// Worker function to process jobs from the queue
async function startJobWorker() {
  console.log('Starting job worker...');

  while (true) {
    try {
      // Get next job from queue (blocking operation with 5-second timeout)
      const jobId = await redisClient.brpop(JOB_QUEUE_KEY, 5);

      if (!jobId || !jobId[1]) {
        // No jobs available, wait a bit before checking again
        await new Promise(resolve => setTimeout(resolve, 1000));
        continue;
      }

      // Process the job
      await processVideoJob(jobId[1]);
    } catch (error) {
      console.error('Error in job worker:', error);
      // Wait a bit before retrying
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}

// Start the worker in the background
startJobWorker().catch(error => {
  console.error('Fatal error in job worker:', error);
  process.exit(1);
});

// API Routes
app.get('/api/welcome', async (req, res) => {
  res.send("Welcome to Remotion API with Redis Job Queue!");
});

// Job Status API
app.get('/api/job-status/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;

    const jobInfo = await getJobInfo(jobId);

    if (!jobInfo) {
      return res.status(404).json({
        success: false,
        error: 'Not Found',
        message: 'Job not found'
      });
    }

    // Format response based on job status
    const response = {
      success: true,
      jobId: jobInfo.jobId,
      status: jobInfo.status,
      createdAt: jobInfo.createdAt,
      updatedAt: jobInfo.updatedAt,
    };

    if (jobInfo.status === JOB_STATUS.COMPLETED && jobInfo.result) {
      response.videoUrl = jobInfo.result.videoUrl;
      response.message = jobInfo.result.message;
    } else if (jobInfo.status === JOB_STATUS.FAILED && jobInfo.result) {
      response.error = jobInfo.result.error;
      response.message = jobInfo.result.message;
    }

    res.status(200).json(response);
  } catch (error) {
    console.error('Error getting job status:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      message: error.message
    });
  }
});

// Video Compilation API
app.post('/api/video-compilation', async (req, res) => {
  try {
    const { inputProps } = req.body;

    if (!inputProps) {
      return res.status(400).json({
        success: false,
        error: 'Bad Request',
        message: 'Missing data'
      });
    }

    // Generate job ID
    const jobId = uuidv4();

    // Store job in Redis
    const stored = await storeJob(jobId, 'video-compilation', inputProps);

    if (!stored) {
      return res.status(500).json({
        success: false,
        error: 'Internal Server Error',
        message: 'Failed to queue job'
      });
    }

    // Return job ID to client
    res.status(202).json({
      success: true,
      jobId,
      message: 'Video compilation job queued for processing',
      statusUrl: `/api/job-status/${jobId}`
    });
  } catch (error) {
    console.error('Error queuing video compilation job:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      message: error.message
    });
  }
});

// Reaction Video API
app.post('/api/reaction-video', async (req, res) => {
  try {
    const { inputProps } = req.body;

    if (!inputProps) {
      return res.status(400).json({
        success: false,
        error: 'Bad Request',
        message: 'Missing data'
      });
    }

    // Generate job ID
    const jobId = uuidv4();

    // Store job in Redis
    const stored = await storeJob(jobId, 'reaction-video', inputProps);

    if (!stored) {
      return res.status(500).json({
        success: false,
        error: 'Internal Server Error',
        message: 'Failed to queue job'
      });
    }

    // Return job ID to client
    res.status(202).json({
      success: true,
      jobId,
      message: 'Reaction video job queued for processing',
      statusUrl: `/api/job-status/${jobId}`
    });
  } catch (error) {
    console.error('Error queuing reaction video job:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      message: error.message
    });
  }
});

// Hello World API
app.post('/api/hello', async (req, res) => {
  try {
    const inputProps = req.body.inputProps || {
      titleText: "API for Remotion",
      titleColor: "#000000",
      logoColor1: "#91EAE4",
      logoColor2: "#86A8E7",
    };

    // Generate job ID
    const jobId = uuidv4();

    // Store job in Redis
    const stored = await storeJob(jobId, 'hello', inputProps);

    if (!stored) {
      return res.status(500).json({
        success: false,
        error: 'Internal Server Error',
        message: 'Failed to queue job'
      });
    }

    // Return job ID to client
    res.status(202).json({
      success: true,
      jobId,
      message: 'Hello World video job queued for processing',
      statusUrl: `/api/job-status/${jobId}`
    });
  } catch (error) {
    console.error('Error queuing Hello World job:', error);
    res.status(500).json({
      success: false,
      error: 'Internal Server Error',
      message: error.message
    });
  }
});

// Start the server
const PORT = process.env.PORT || 4444;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});