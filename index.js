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
const axios = require('axios'); // Add axios for making webhook HTTP requests

// Force stdout to be unbuffered
if (process.stdout._handle && process.stdout._handle.setBlocking) {
    process.stdout._handle.setBlocking(true);
}

// Enhanced logging function to ensure console output is flushed immediately
function logWithFlush(message, ...args) {
    // Write directly to stdout for maximum reliability
    process.stdout.write(`${message} ${args.length ? JSON.stringify(args) : ''}\n`);
}

// Create a direct file logger
function logToFile(message) {
    const logFile = path.join(__dirname, 'debug.log');
    fs.appendFileSync(logFile, `${new Date().toISOString()} - ${message}\n`);
}

// Print environment diagnostics at startup
logWithFlush("===== ENVIRONMENT DIAGNOSTICS =====");
logWithFlush(`Node version: ${process.version}`);
logWithFlush(`Current working directory: ${process.cwd()}`);
logWithFlush(`stdout writable: ${process.stdout.writable}`);
logWithFlush(`stderr writable: ${process.stderr.writable}`);
logWithFlush(`Is TTY: ${process.stdout.isTTY}`);
logWithFlush("==================================");

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
        logWithFlush(`Redis connection retry in ${delay}ms`);
        return delay;
    }
});

redisClient.on('error', (err) => {
    console.error('Redis connection error:', err);
    logToFile(`Redis connection error: ${err.message}`);
});

redisClient.on('connect', () => {
    logWithFlush('Connected to Redis');
    logToFile('Connected to Redis');
});

// Configure S3 client
const s3Client = new S3Client({
    region: process.env.S3_REGION,
    credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY,
        secretAccessKey: process.env.S3_SECRET_KEY,
    },
});

// Job queue keys/prefixes - UPDATED to use sorted sets for better reliability
const JOB_DATA_PREFIX = 'remotion:job:';
const JOB_STATUS_PREFIX = 'remotion:status:';
const JOB_RESULT_PREFIX = 'remotion:result:';
const PENDING_JOBS_SET = 'remotion:pending-jobs'; // Sorted set of pending jobs
const PROCESSING_JOBS_SET = 'remotion:processing-jobs'; // Sorted set of jobs being processed

// Job statuses
const JOB_STATUS = {
    PENDING: 'pending',
    PROCESSING: 'processing',
    COMPLETED: 'completed',
    FAILED: 'failed',
};

// Store job in Redis - COMPLETELY REVISED to use sorted sets
async function storeJob(jobId, jobType, inputProps, filename="", webhookUrl = null) {
    try {
        logWithFlush(`[storeJob] Storing job ${jobId} of type ${jobType}`);
        logToFile(`[storeJob] Storing job ${jobId} of type ${jobType}`);

        // Log the filename being stored
        logWithFlush(`[storeJob] Storing with filename: ${filename || 'not provided'}`);

        // Store job data
        await redisClient.set(
            `${JOB_DATA_PREFIX}${jobId}`,
            JSON.stringify({
                jobId,
                jobType,
                inputProps,
                filename,
                webhookUrl,
                createdAt: new Date().toISOString(),
            }),
            'EX',
            60 * 60 * 24 * 7 // Expire after 7 days
        );
        logWithFlush(`[storeJob] Job data stored for ${jobId}`);

        // Set initial status
        await redisClient.set(
            `${JOB_STATUS_PREFIX}${jobId}`,
            JOB_STATUS.PENDING,
            'EX',
            60 * 60 * 24 * 7 // Expire after 7 days
        );
        logWithFlush(`[storeJob] Initial status set to PENDING for ${jobId}`);

        // Add to pending jobs sorted set with current timestamp as score for FIFO order
        const timestamp = Date.now();
        const addResult = await redisClient.zadd(PENDING_JOBS_SET, timestamp, jobId);
        logWithFlush(`[storeJob] Job ${jobId} added to pending jobs set with timestamp ${timestamp}, result: ${addResult}`);

        // Get count of pending jobs
        const pendingCount = await redisClient.zcard(PENDING_JOBS_SET);
        logWithFlush(`[storeJob] Current pending jobs count: ${pendingCount}`);

        return true;
    } catch (error) {
        console.error(`[storeJob] Error storing job ${jobId}:`, error);
        logToFile(`[storeJob] Error: ${error.message}`);
        return false;
    }
}

// Update job status
async function updateJobStatus(jobId, status, result = null) {
    try {
        logWithFlush(`[updateJobStatus] Updating job ${jobId} to status: ${status}`);
        logToFile(`[updateJobStatus] Job ${jobId} status: ${status}`);

        await redisClient.set(
            `${JOB_STATUS_PREFIX}${jobId}`,
            status,
            'EX',
            60 * 60 * 24 * 7 // Expire after 7 days
        );

        if (result) {
            logWithFlush(`[updateJobStatus] Storing result for job ${jobId}`);
            await redisClient.set(
                `${JOB_RESULT_PREFIX}${jobId}`,
                JSON.stringify(result),
                'EX',
                60 * 60 * 24 * 7 // Expire after 7 days
            );
        }

        // If job processing is complete (success or failure), remove from processing set
        if (status === JOB_STATUS.COMPLETED || status === JOB_STATUS.FAILED) {
            await redisClient.zrem(PROCESSING_JOBS_SET, jobId);
            logWithFlush(`[updateJobStatus] Removed job ${jobId} from processing set`);

            // Trigger webhook if needed
            logWithFlush(`[updateJobStatus] Job ${jobId} is ${status}, triggering webhook`);
            await triggerWebhook(jobId, status, result);
        }

        logWithFlush(`[updateJobStatus] Status update completed for job ${jobId}`);
        return true;
    } catch (error) {
        console.error(`[updateJobStatus] Error updating job status ${jobId}:`, error);
        logToFile(`[updateJobStatus] Error: ${error.message}`);
        return false;
    }
}

// Process webhook notifications
async function triggerWebhook(jobId, status, result) {
    try {
        logWithFlush(`[triggerWebhook] Starting webhook process for job ${jobId}`);

        // Get job data to retrieve webhook URL
        const jobDataStr = await redisClient.get(`${JOB_DATA_PREFIX}${jobId}`);
        if (!jobDataStr) {
            logWithFlush(`[triggerWebhook] No job data found for webhook notification: ${jobId}`);
            return false;
        }

        const jobData = JSON.parse(jobDataStr);
        const { webhookUrl } = jobData;

        // If no webhook URL was provided, skip notification
        if (!webhookUrl) {
            logWithFlush(`[triggerWebhook] No webhook URL for job ${jobId}, skipping notification.`);
            return false;
        }

        logWithFlush(`[triggerWebhook] Triggering webhook for job ${jobId} to ${webhookUrl}`);

        // Prepare notification payload
        const notificationPayload = {
            jobId,
            status,
            jobType: jobData.jobType,
            createdAt: jobData.createdAt,
            completedAt: new Date().toISOString(),
        };

        // Add result data to payload
        if (status === JOB_STATUS.COMPLETED && result) {
            notificationPayload.videoUrl = result.videoUrl;
            notificationPayload.message = result.message;
        } else if (status === JOB_STATUS.FAILED && result) {
            notificationPayload.error = result.error;
            notificationPayload.message = result.message;
        }

        logWithFlush(`[triggerWebhook] Sending webhook payload for job ${jobId}`);

        // Send the webhook notification
        const response = await axios.post(webhookUrl, notificationPayload, {
            headers: {
                'Content-Type': 'application/json',
                'User-Agent': 'Remotion-API-Webhook',
            },
            timeout: 10000, // 10 second timeout
        });

        logWithFlush(`[triggerWebhook] Webhook notification sent successfully: ${response.status}`);
        return true;
    } catch (error) {
        console.error(`[triggerWebhook] Error sending webhook notification for job ${jobId}:`, error);
        logToFile(`[triggerWebhook] Error: ${error.message}`);
        return false;
    }
}

// Get job status and result
async function getJobInfo(jobId) {
    try {
        logWithFlush(`[getJobInfo] Getting info for job ${jobId}`);

        const [status, resultStr, dataStr] = await Promise.all([
            redisClient.get(`${JOB_STATUS_PREFIX}${jobId}`),
            redisClient.get(`${JOB_RESULT_PREFIX}${jobId}`),
            redisClient.get(`${JOB_DATA_PREFIX}${jobId}`)
        ]);

        if (!status) {
            logWithFlush(`[getJobInfo] No status found for job ${jobId}`);
            return null; // Job not found
        }

        const result = resultStr ? JSON.parse(resultStr) : null;
        const data = dataStr ? JSON.parse(dataStr) : {};

        logWithFlush(`[getJobInfo] Job ${jobId} info retrieved, status: ${status}`);

        return {
            jobId,
            status,
            result,
            jobType: data.jobType,
            webhookUrl: data.webhookUrl,
            createdAt: data.createdAt,
            updatedAt: new Date().toISOString(),
        };
    } catch (error) {
        console.error(`[getJobInfo] Error getting job info ${jobId}:`, error);
        return null;
    }
}

// Upload to S3
async function uploadToS3(filename, fileContent, tempFilePath) {
    logWithFlush(`[uploadToS3] Starting S3 upload for ${filename}`);
    logToFile(`[uploadToS3] Starting S3 upload for ${filename}`);

    // Define S3 path
    const s3Key = `videos/${filename}`;

    logWithFlush(`[uploadToS3] Uploading to S3: ${process.env.S3_BUCKET_NAME}/${s3Key}`);

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
    logWithFlush("[uploadToS3] Upload result:", uploadResult);

    // Generate S3 URL
    const videoUrl = `https://${process.env.S3_BUCKET_NAME}.s3.${process.env.S3_REGION}.amazonaws.com/${s3Key}`;

    // Clean up the temporary file
    fs.unlinkSync(tempFilePath);
    logWithFlush(`[uploadToS3] Temporary file removed: ${tempFilePath}`);
    logWithFlush(`[uploadToS3] Upload completed, video URL: ${videoUrl}`);
    logToFile(`[uploadToS3] Upload completed: ${videoUrl}`);

    return videoUrl;
}

// Process video jobs
async function processVideoJob(jobId) {
    // Immediate first log with flush
    process.stdout.write(`\n[PROCESS-START] Starting to process job ${jobId} at ${new Date().toISOString()}\n`);
    logToFile(`[PROCESS-START] Starting to process job ${jobId}`);

    try {
        // Force immediate console output
        process.stdout.write(`\n[PROCESS] Processing job ${jobId}...\n`);
        logToFile(`[PROCESS] Processing job ${jobId}...`);

        // Update job status to processing
        await updateJobStatus(jobId, JOB_STATUS.PROCESSING);
        process.stdout.write(`\n[PROCESS] Job status updated to PROCESSING\n`);
        logToFile(`[PROCESS] Job status updated to PROCESSING`);

        // Get job data
        const jobDataStr = await redisClient.get(`${JOB_DATA_PREFIX}${jobId}`);
        if (!jobDataStr) {
            process.stdout.write(`\n[PROCESS-ERROR] Job data not found for ${jobId}\n`);
            logToFile(`[PROCESS-ERROR] Job data not found for ${jobId}`);
            throw new Error(`Job data not found for ${jobId}`);
        }
        process.stdout.write(`\n[PROCESS] Job data retrieved for ${jobId}\n`);
        logToFile(`[PROCESS] Job data retrieved for ${jobId}`);

        const jobData = JSON.parse(jobDataStr);
        const { jobType, inputProps, filename } = jobData;

        process.stdout.write(`\n[PROCESS] Job type: ${jobType}\n`);
        process.stdout.write(`\n[PROCESS] Filename from job data: ${filename || 'not provided'}\n`);
        logToFile(`[PROCESS] Job type: ${jobType}`);
        logToFile(`[PROCESS] Filename from job data: ${filename || 'not provided'}`);

        // Select composition based on job type
        let compositionId;

        if (jobType === 'reaction-video') {
            compositionId = 'ReactionVideo';
        } else if (jobType === 'video-compilation') {
            compositionId = 'VideoCompilation';
        } else if (jobType === 'hello') {
            compositionId = 'HelloWorld';
        } else {
            process.stdout.write(`\n[PROCESS-ERROR] Unknown job type: ${jobType}\n`);
            logToFile(`[PROCESS-ERROR] Unknown job type: ${jobType}`);
            throw new Error(`Unknown job type: ${jobType}`);
        }
        process.stdout.write(`\n[PROCESS] Composition ID: ${compositionId}\n`);
        logToFile(`[PROCESS] Composition ID: ${compositionId}`);

        // Select composition
        process.stdout.write(`\n[PROCESS] Selecting composition from Remotion server...\n`);
        logToFile(`[PROCESS] Selecting composition from Remotion server...`);
        const composition = await selectComposition({
            serveUrl: process.env.REMOTION_SERVE_URL,
            id: compositionId,
            inputProps,
        });
        process.stdout.write(`\n[PROCESS] Composition selected: ${composition.id}\n`);
        logToFile(`[PROCESS] Composition selected: ${composition.id}`);

        // Calculate duration based on job type
        const fps = composition.fps || 30;
        process.stdout.write(`\n[PROCESS] FPS: ${fps}\n`);
        logToFile(`[PROCESS] FPS: ${fps}`);

        if (jobType === 'reaction-video') {
            // Calculate the total frames for a reaction video where clips play simultaneously
            const maxDuration = Math.max(
                ...inputProps.reactionVideoData.clips.map(clip => clip.durationInSeconds)
            );
            composition.durationInFrames = Math.round(maxDuration * fps);
            process.stdout.write(`\n[PROCESS] Reaction video duration: ${maxDuration}s, ${composition.durationInFrames} frames\n`);
            logToFile(`[PROCESS] Reaction video duration: ${maxDuration}s, ${composition.durationInFrames} frames`);
        } else if (jobType === 'video-compilation') {
            // Calculate the total frames for a compilation where clips play sequentially
            composition.durationInFrames = inputProps.compilationData.clips.reduce((total, clip) => {
                return total + Math.round(clip.durationInSeconds * fps);
            }, 0);
            process.stdout.write(`\n[PROCESS] Compilation video duration: ${composition.durationInFrames/fps}s, ${composition.durationInFrames} frames\n`);
            logToFile(`[PROCESS] Compilation video duration: ${composition.durationInFrames/fps}s, ${composition.durationInFrames} frames`);
        }

        // Make sure temp directory exists for temporary file storage
        const tempDir = path.join(__dirname, 'temp');
        if (!fs.existsSync(tempDir)) {
            process.stdout.write(`\n[PROCESS] Creating temp directory: ${tempDir}\n`);
            logToFile(`[PROCESS] Creating temp directory: ${tempDir}`);
            fs.mkdirSync(tempDir, { recursive: true });
        }

        // Use the provided filename or generate a default if not provided
        // Sanitize the filename to prevent any path traversal
        const sanitizedFilename = filename ?
            filename.replace(/[^a-zA-Z0-9_-]/g, '_') :
            `${compositionId}-${jobId}`;

        // Ensure the filename has .mp4 extension
        const outputFilename = sanitizedFilename.endsWith('.mp4') ?
            sanitizedFilename : `${sanitizedFilename}.mp4`;

        const tempFilePath = path.join(tempDir, outputFilename);

        process.stdout.write(`\n[PROCESS] Using sanitized filename: ${sanitizedFilename}\n`);
        process.stdout.write(`\n[PROCESS] Output filename: ${outputFilename}\n`);
        process.stdout.write(`\n[PROCESS] Temporary file will be saved to: ${tempFilePath}\n`);
        logToFile(`[PROCESS] Using sanitized filename: ${sanitizedFilename}`);
        logToFile(`[PROCESS] Output filename: ${outputFilename}`);
        logToFile(`[PROCESS] Temporary file path: ${tempFilePath}`);

        // Render the media
        process.stdout.write(`\n[PROCESS] Starting media rendering...\n`);
        logToFile(`[PROCESS] Starting media rendering...`);
        const renderResult = await renderMedia({
            composition,
            serveUrl: process.env.REMOTION_SERVE_URL,
            codec: 'h264',
            outputLocation: tempFilePath,
            inputProps,
        });

        process.stdout.write(`\n[PROCESS] Render completed. Result: ${JSON.stringify(renderResult)}\n`);
        logToFile(`[PROCESS] Render completed`);

        // Read the file
        process.stdout.write(`\n[PROCESS] Reading rendered file...\n`);
        logToFile(`[PROCESS] Reading rendered file...`);
        const fileContent = fs.readFileSync(tempFilePath);
        process.stdout.write(`\n[PROCESS] File read, size: ${fileContent.length} bytes\n`);
        logToFile(`[PROCESS] File read, size: ${fileContent.length} bytes`);

        // Upload to S3
        process.stdout.write(`\n[PROCESS] Starting S3 upload...\n`);
        logToFile(`[PROCESS] Starting S3 upload...`);
        const videoUrl = await uploadToS3(outputFilename, fileContent, tempFilePath);
        process.stdout.write(`\n[PROCESS] S3 upload completed, URL: ${videoUrl}\n`);
        logToFile(`[PROCESS] S3 upload completed, URL: ${videoUrl}`);

        // Update job status to completed with result
        await updateJobStatus(jobId, JOB_STATUS.COMPLETED, {
            videoUrl,
            message: `${jobType} rendered and uploaded to S3 successfully`,
        });

        process.stdout.write(`\n[PROCESS-COMPLETE] Job ${jobId} completed successfully\n`);
        logToFile(`[PROCESS-COMPLETE] Job ${jobId} completed successfully`);

        return { success: true, videoUrl };
    } catch (error) {
        process.stdout.write(`\n[PROCESS-ERROR] Error processing job ${jobId}: ${error.message}\n`);
        process.stdout.write(`\n${error.stack}\n`);
        logToFile(`[PROCESS-ERROR] Error processing job ${jobId}: ${error.message}`);
        logToFile(error.stack);

        // Update job status to failed
        await updateJobStatus(jobId, JOB_STATUS.FAILED, {
            error: 'Failed to process video job',
            message: error.message,
        });

        return { success: false, error: error.message };
    }
}

// Worker function to process jobs from the queue - COMPLETELY REWRITTEN
async function startJobWorker() {
    const workerProcessId = process.pid;
    logWithFlush(`===== STARTING JOB WORKER =====`);
    logWithFlush(`[worker-${workerProcessId}] Worker process started`);
    logToFile('===== STARTING JOB WORKER =====');

    // First, create sorted sets if they don't exist
    try {
        // Check if sets exist by trying to get their sizes
        const pendingSetExists = await redisClient.exists(PENDING_JOBS_SET);
        const processingSetExists = await redisClient.exists(PROCESSING_JOBS_SET);

        logWithFlush(`[worker-${workerProcessId}] Pending jobs set exists: ${pendingSetExists}`);
        logWithFlush(`[worker-${workerProcessId}] Processing jobs set exists: ${processingSetExists}`);

        // Initialize the sorted sets if they don't exist (this is mostly for logging)
        if (!pendingSetExists && !processingSetExists) {
            logWithFlush(`[worker-${workerProcessId}] Initializing job sorted sets for first use`);
        }
    } catch (error) {
        console.error(`[worker-${workerProcessId}] Error checking job sets:`, error);
    }

    // Check for and recover any stuck jobs in the processing set
    try {
        const processingJobs = await redisClient.zrange(PROCESSING_JOBS_SET, 0, -1, 'WITHSCORES');
        if (processingJobs.length > 0) {
            logWithFlush(`[worker-${workerProcessId}] Found ${processingJobs.length/2} jobs in processing state - may be stuck`);

            // Loop through pairs (jobId, timestamp)
            for (let i = 0; i < processingJobs.length; i += 2) {
                const jobId = processingJobs[i];
                const timestamp = parseInt(processingJobs[i+1]);
                const ageInMinutes = (Date.now() - timestamp) / (60 * 1000);

                // If job has been processing for more than 30 minutes, move it back to pending
                if (ageInMinutes > 30) {
                    logWithFlush(`[worker-${workerProcessId}] Job ${jobId} has been stuck in processing for ${ageInMinutes.toFixed(2)} minutes, recovering`);

                    // Move back to pending with a new timestamp
                    await redisClient.zadd(PENDING_JOBS_SET, Date.now(), jobId);
                    await redisClient.zrem(PROCESSING_JOBS_SET, jobId);

                    // Update status back to pending
                    await redisClient.set(`${JOB_STATUS_PREFIX}${jobId}`, JOB_STATUS.PENDING);

                    logWithFlush(`[worker-${workerProcessId}] Recovered job ${jobId} to pending state`);
                }
            }
        }
    } catch (error) {
        console.error(`[worker-${workerProcessId}] Error recovering stuck jobs:`, error);
    }

    // Main worker loop
    while (true) {
        try {
            // Check for pending jobs
            const pendingCount = await redisClient.zcard(PENDING_JOBS_SET);
            logWithFlush(`[worker-${workerProcessId}] Checking for pending jobs, count: ${pendingCount}`);

            if (pendingCount > 0) {
                // Get the oldest job (lowest score = earliest timestamp)
                const pendingJobsWithScores = await redisClient.zrange(PENDING_JOBS_SET, 0, 0, 'WITHSCORES');

                if (pendingJobsWithScores.length >= 2) {
                    const jobId = pendingJobsWithScores[0];
                    const timestamp = parseInt(pendingJobsWithScores[1]);

                    logWithFlush(`[worker-${workerProcessId}] Found pending job ${jobId} from timestamp ${new Date(timestamp).toISOString()}`);

                    // Try to claim the job by moving it from pending to processing
                    const removed = await redisClient.zrem(PENDING_JOBS_SET, jobId);

                    if (removed === 1) {
                        // Successfully claimed the job
                        await redisClient.zadd(PROCESSING_JOBS_SET, Date.now(), jobId);
                        logWithFlush(`[worker-${workerProcessId}] Successfully claimed job ${jobId} and moved to processing`);

                        // Check if job data exists
                        const jobExists = await redisClient.exists(`${JOB_DATA_PREFIX}${jobId}`);

                        if (jobExists) {
                            // Process the job
                            logWithFlush(`[worker-${workerProcessId}] Starting to process job ${jobId}`);
                            await processVideoJob(jobId);
                            logWithFlush(`[worker-${workerProcessId}] Completed processing job ${jobId}`);

                            // Note: updateJobStatus will remove the job from the processing set
                        } else {
                            logWithFlush(`[worker-${workerProcessId}] Job ${jobId} has no data, removing from processing set`);
                            await redisClient.zrem(PROCESSING_JOBS_SET, jobId);
                        }
                    } else {
                        logWithFlush(`[worker-${workerProcessId}] Failed to claim job ${jobId}, it may have been taken by another worker`);
                    }
                } else {
                    logWithFlush(`[worker-${workerProcessId}] Pending jobs count is ${pendingCount} but couldn't retrieve any jobs - may be a race condition`);
                }
            } else {
                // No pending jobs, wait for a bit before checking again
                logWithFlush(`[worker-${workerProcessId}] No pending jobs, waiting for 2 seconds...`);
                await new Promise(resolve => setTimeout(resolve, 2000));
            }
        } catch (error) {
            console.error(`[worker-${workerProcessId}] Error in job worker:`, error);
            console.error(error.stack);
            logToFile(`[worker-${workerProcessId}] Error in job worker: ${error.message}`);

            // Wait before retrying
            await new Promise(resolve => setTimeout(resolve, 5000));
            logWithFlush(`[worker-${workerProcessId}] Resuming worker after error`);
        }
    }
}

// Create a minimal test version of the processing function
async function testMinimalProcessing(jobId) {
    process.stdout.write(`\n[TEST] Starting minimal processing for ${jobId}\n`);
    logToFile(`[TEST] Starting minimal processing for ${jobId}`);

    // Just do a few basic steps without all the video rendering
    await new Promise(resolve => setTimeout(resolve, 1000));
    process.stdout.write(`\n[TEST] Step 1 complete\n`);

    await new Promise(resolve => setTimeout(resolve, 1000));
    process.stdout.write(`\n[TEST] Step 2 complete\n`);

    await new Promise(resolve => setTimeout(resolve, 1000));
    process.stdout.write(`\n[TEST] Step 3 complete\n`);

    return { success: true, message: "Test processing completed" };
}

// Debug function to get all jobs
async function getAllJobs() {
    try {
        // Get all pending jobs with scores
        const pendingJobs = await redisClient.zrange(PENDING_JOBS_SET, 0, -1, 'WITHSCORES');

        // Get all processing jobs with scores
        const processingJobs = await redisClient.zrange(PROCESSING_JOBS_SET, 0, -1, 'WITHSCORES');

        // Format the data into a more readable structure
        const formattedPending = [];
        for (let i = 0; i < pendingJobs.length; i += 2) {
            formattedPending.push({
                jobId: pendingJobs[i],
                timestamp: parseInt(pendingJobs[i+1]),
                added: new Date(parseInt(pendingJobs[i+1])).toISOString()
            });
        }

        const formattedProcessing = [];
        for (let i = 0; i < processingJobs.length; i += 2) {
            formattedProcessing.push({
                jobId: processingJobs[i],
                timestamp: parseInt(processingJobs[i+1]),
                started: new Date(parseInt(processingJobs[i+1])).toISOString()
            });
        }

        return {
            pending: formattedPending,
            processing: formattedProcessing
        };
    } catch (error) {
        console.error('Error getting all jobs:', error);
        return { error: error.message };
    }
}

// Add debug processing test function
async function debugProcessing() {
    // Force immediate console output
    process.stdout.write("\n\n===== STARTING DEBUG PROCESSING TEST =====\n\n");
    logToFile("===== STARTING DEBUG PROCESSING TEST =====");

    // Create a fake job ID for testing
    const testJobId = "debug-" + Date.now();
    process.stdout.write(`Test job ID: ${testJobId}\n`);

    // Store a test job in Redis
    process.stdout.write("Storing test job in Redis...\n");
    await storeJob(testJobId, 'hello', {
        titleText: "Debug Test",
        titleColor: "#000000",
        logoColor1: "#91EAE4",
        logoColor2: "#86A8E7",
    }, "debug-test-file");  // Include test filename

    // Process the job directly, bypassing the worker queue
    process.stdout.write("Starting direct job processing...\n");
    try {
        // Remove from pending set first since we're manually processing
        await redisClient.zrem(PENDING_JOBS_SET, testJobId);

        const result = await processVideoJob(testJobId);
        process.stdout.write(`Processing complete with result: ${JSON.stringify(result)}\n`);
    } catch (error) {
        process.stdout.write(`Processing error: ${error.message}\n${error.stack}\n`);
    }

    process.stdout.write("\n\n===== DEBUG PROCESSING TEST COMPLETE =====\n\n");
    logToFile("===== DEBUG PROCESSING TEST COMPLETE =====");
}

// Start the worker in the background
startJobWorker().catch(error => {
    console.error('Fatal error in job worker:', error);
    console.error(error.stack);
    logToFile(`Fatal error in job worker: ${error.message}`);
    process.exit(1);
});

// Add debugging endpoints
app.get('/api/debug-queue', async (req, res) => {
    try {
        logWithFlush('[DEBUG] Debugging queue status');

        // Get all jobs info
        const allJobs = await getAllJobs();

        // Get all job keys for comparison
        const pendingJobs = await redisClient.keys(`${JOB_STATUS_PREFIX}*`);
        logWithFlush(`[DEBUG] All job status keys: ${JSON.stringify(pendingJobs)}`);

        // Check the specific job if requested
        const jobId = req.query.jobId || "";
        let jobStatus = null;
        let jobData = null;

        if (jobId) {
            jobStatus = await redisClient.get(`${JOB_STATUS_PREFIX}${jobId}`);
            const jobDataStr = await redisClient.get(`${JOB_DATA_PREFIX}${jobId}`);
            if (jobDataStr) {
                jobData = JSON.parse(jobDataStr);
            }
            logWithFlush(`[DEBUG] Job ${jobId} status: ${jobStatus}`);
        }

        // Return comprehensive debug info
        res.status(200).json({
            pendingCount: allJobs.pending.length,
            processingCount: allJobs.processing.length,
            pendingJobs: allJobs.pending,
            processingJobs: allJobs.processing,
            allJobStatusKeys: pendingJobs,
            specificJob: jobId ? {
                jobId,
                status: jobStatus,
                data: jobData
            } : null
        });
    } catch (error) {
        console.error('[DEBUG] Error debugging queue:', error);
        res.status(500).json({ error: error.message });
    }
});

// Enhanced debug endpoint
app.get('/api/debug-jobs', async (req, res) => {
    try {
        const allJobs = await getAllJobs();

        // Enhance with job data
        for (const job of [...allJobs.pending, ...allJobs.processing]) {
            try {
                const jobDataStr = await redisClient.get(`${JOB_DATA_PREFIX}${job.jobId}`);
                if (jobDataStr) {
                    const parsedData = JSON.parse(jobDataStr);
                    job.type = parsedData.jobType;
                    job.filename = parsedData.filename;
                    job.status = await redisClient.get(`${JOB_STATUS_PREFIX}${job.jobId}`);
                }
            } catch (error) {
                console.error(`Error getting data for job ${job.jobId}:`, error);
            }
        }

        res.json(allJobs);
    } catch (error) {
        console.error('Error debugging jobs:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/force-debug', async (req, res) => {
    process.stdout.write("\n===== FORCED DEBUG LOGGING =====\n");
    process.stdout.write(`Time: ${new Date().toISOString()}\n`);
    process.stdout.write("Standard stdout test\n");
    console.log("Console.log test");
    console.error("Console.error test");

    // Check if we can write to a file
    try {
        fs.writeFileSync(path.join(__dirname, 'debug-test.txt'), 'Debug test file write\n');
        process.stdout.write("File write successful\n");
    } catch (err) {
        process.stdout.write(`File write failed: ${err.message}\n`);
    }

    res.send("Forced debug complete");
});

app.get('/api/test-minimal', async (req, res) => {
    const testJobId = "minimal-" + Date.now();
    process.stdout.write(`\n[API] Starting minimal test for ${testJobId}\n`);

    try {
        const result = await testMinimalProcessing(testJobId);
        process.stdout.write(`\n[API] Minimal test complete: ${JSON.stringify(result)}\n`);
        res.json({ success: true, result });
    } catch (error) {
        process.stdout.write(`\n[API] Minimal test error: ${error.message}\n`);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Add endpoint to trigger debug processing
app.get('/api/run-debug-process', async (req, res) => {
    try {
        process.stdout.write("\n[API] Debug processing test triggered\n");

        // Run debug processing in the background
        debugProcessing().catch(err => {
            console.error("Debug processing failed:", err);
        });

        res.status(200).json({
            success: true,
            message: "Debug processing test started in background"
        });
    } catch (error) {
        console.error("Error starting debug process:", error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// Manual job recovery endpoint
app.post('/api/recover-job/:jobId', async (req, res) => {
    try {
        const { jobId } = req.params;
        logWithFlush(`[API] Manual recovery requested for job ${jobId}`);

        // Check if job exists
        const jobExists = await redisClient.exists(`${JOB_DATA_PREFIX}${jobId}`);

        if (!jobExists) {
            return res.status(404).json({
                success: false,
                message: `Job ${jobId} not found`
            });
        }

        // Remove from processing set if it's there
        await redisClient.zrem(PROCESSING_JOBS_SET, jobId);

        // Add back to pending set with current timestamp
        await redisClient.zadd(PENDING_JOBS_SET, Date.now(), jobId);

        // Set status back to pending
        await redisClient.set(`${JOB_STATUS_PREFIX}${jobId}`, JOB_STATUS.PENDING);

        logWithFlush(`[API] Job ${jobId} recovered and moved back to pending state`);

        res.status(200).json({
            success: true,
            message: `Job ${jobId} recovered successfully`
        });
    } catch (error) {
        console.error('[API] Error recovering job:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

// API Routes
app.get('/api/welcome', async (req, res) => {
    logWithFlush('[API] Welcome endpoint called');
    res.send("Welcome to Remotion API with Redis Job Queue!");
});

// Job Status API
app.get('/api/job-status/:jobId', async (req, res) => {
    try {
        const { jobId } = req.params;
        logWithFlush(`[API] Job status requested for ${jobId}`);

        const jobInfo = await getJobInfo(jobId);

        if (!jobInfo) {
        logWithFlush(`[API] Job ${jobId} not found`);
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

        logWithFlush(`[API] Returning job info for ${jobId}, status: ${jobInfo.status}`);
        res.status(200).json(response);
    } catch (error) {
        console.error('[API] Error getting job status:', error);
        console.error(error.stack);
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
        logWithFlush('[API] Video compilation request received');
        const { inputProps, filename, webhookUrl } = req.body;

        // Log the received filename
        logWithFlush(`[API] Received filename in request: ${filename || 'not provided'}`);

        if (!inputProps) {
            logWithFlush('[API] Missing input props in video compilation request');
            return res.status(400).json({
                success: false,
                error: 'Bad Request',
                message: 'Missing data'
            });
        }

        // Generate job ID
        const jobId = uuidv4();
        logWithFlush(`[API] Generated job ID for video compilation: ${jobId}`);

        // Store job in Redis with webhook URL
        const stored = await storeJob(jobId, 'video-compilation', inputProps, filename, webhookUrl);

        if (!stored) {
            logWithFlush(`[API] Failed to store video compilation job ${jobId}`);
            return res.status(500).json({
                success: false,
                error: 'Internal Server Error',
                message: 'Failed to queue job'
            });
        }

        // Return job ID to client
        logWithFlush(`[API] Video compilation job ${jobId} queued successfully`);
        res.status(202).json({
            success: true,
            jobId,
            message: 'Video compilation job queued for processing',
            statusUrl: `/api/job-status/${jobId}`,
            webhookEnabled: !!webhookUrl
        });
    } catch (error) {
        console.error('[API] Error queuing video compilation job:', error);
        console.error(error.stack);
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
        logWithFlush('[API] Reaction video request received');
        const { inputProps, filename, webhookUrl } = req.body;

        // Log the received filename
        logWithFlush(`[API] Received filename in request: ${filename || 'not provided'}`);

        if (!inputProps) {
            logWithFlush('[API] Missing input props in reaction video request');
            return res.status(400).json({
                success: false,
                error: 'Bad Request',
                message: 'Missing data'
            });
        }

        // Generate job ID
        const jobId = uuidv4();
        logWithFlush(`[API] Generated job ID for reaction video: ${jobId}`);

        // Store job in Redis with webhook URL
        const stored = await storeJob(jobId, 'reaction-video', inputProps, filename, webhookUrl);

        if (!stored) {
            logWithFlush(`[API] Failed to store reaction video job ${jobId}`);
            return res.status(500).json({
                success: false,
                error: 'Internal Server Error',
                message: 'Failed to queue job'
            });
        }

        // Return job ID to client
        logWithFlush(`[API] Reaction video job ${jobId} queued successfully`);
        res.status(202).json({
            success: true,
            jobId,
            message: 'Reaction video job queued for processing',
            statusUrl: `/api/job-status/${jobId}`,
            webhookEnabled: !!webhookUrl
        });
    } catch (error) {
        console.error('[API] Error queuing reaction video job:', error);
        console.error(error.stack);
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
        logWithFlush('[API] Hello world video request received');
        const webhookUrl = req.body.webhookUrl;
        const inputProps = req.body.inputProps || {
            titleText: "API for Remotion",
            titleColor: "#000000",
            logoColor1: "#91EAE4",
            logoColor2: "#86A8E7",
        };
        const filename = req.body.filename;

        // Log the received filename
        logWithFlush(`[API] Received filename in request: ${filename || 'not provided'}`);

        // Generate job ID
        const jobId = uuidv4();
        logWithFlush(`[API] Generated job ID for hello world: ${jobId}`);

        // Store job in Redis with webhook URL
        const stored = await storeJob(jobId, 'hello', inputProps, filename, webhookUrl);

        if (!stored) {
            logWithFlush(`[API] Failed to store hello world job ${jobId}`);
            return res.status(500).json({
                success: false,
                error: 'Internal Server Error',
                message: 'Failed to queue job'
            });
        }

        // Return job ID to client
        logWithFlush(`[API] Hello world job ${jobId} queued successfully`);
        res.status(202).json({
            success: true,
            jobId,
            message: 'Hello World video job queued for processing',
            statusUrl: `/api/job-status/${jobId}`,
            webhookEnabled: !!webhookUrl
        });
    } catch (error) {
        console.error('[API] Error queuing Hello World job:', error);
        console.error(error.stack);
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
    logWithFlush(`===== SERVER STARTED ON PORT ${PORT} =====`);
    logToFile(`===== SERVER STARTED ON PORT ${PORT} =====`);
});