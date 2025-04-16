const express = require('express');
const { renderMedia, selectComposition } = require('@remotion/renderer');
const cors = require('cors');
const fs = require('fs');
const path = require('path');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');

require('dotenv').config();
const app = express();
app.use(cors());
app.use(express.json());

const s3Client = new S3Client({
    region: process.env.S3_REGION,
    credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY,
        secretAccessKey: process.env.S3_SECRET_KEY,
    },
});

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

app.get('/api/welcome', async (req, res) => {
    res.send("Welcome!");
});

app.post('/api/video-compilation', async (req, res) => {
    try {

        const { inputProps } = req.body;

        if (!inputProps) {
            res.status(400).json({
                success: false,
                error: 'Bad Request',
                message: 'Missing data'
            });
        }

        const compositionId = 'VideoCompilation';

        const composition = await selectComposition({
            serveUrl: process.env.REMOTION_SERVE_URL,
            id: compositionId,
            inputProps,
        });

        // Calculate the actual duration based on the input props
        const fps = composition.fps || 30;
        const calculateTotalFrames = (data) => {
            return data.clips.reduce((total, clip) => {
                return total + Math.round(clip.durationInSeconds * fps);
            }, 0);
        };

        // Override the duration with the calculated value
        composition.durationInFrames = calculateTotalFrames(inputProps.compilationData);

        // Make sure temp directory exists for temporary file storage
        const tempDir = path.join(__dirname, 'temp');
        if (!fs.existsSync(tempDir)) {
            console.log(`Creating temp directory: ${tempDir}`);
            fs.mkdirSync(tempDir, { recursive: true });
        }

        const filename = `${compositionId}-${Date.now()}.mp4`;
        const tempFilePath = path.join(tempDir, filename);

        console.log(`Temporary file will be saved to: ${tempFilePath}`);

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

        const videoUrl = await uploadToS3(filename, fileContent, tempFilePath);

        res.status(200).json({
            success: true,
            videoUrl: videoUrl,
            message: 'Video rendered and uploaded to S3 successfully'
        });
    } catch (error) {
        console.error('Error getting compositions:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to generate compilation video',
            message: error.message
        });
    }
});

app.post('/api/reaction-video', async (req, res) => {
    try {

        const { inputProps } = req.body;

        if (!inputProps) {
            res.status(400).json({
                success: false,
                error: 'Bad Request',
                message: 'Missing data'
            });
        }

        const compositionId = 'ReactionVideo';

        const composition = await selectComposition({
            serveUrl: process.env.REMOTION_SERVE_URL,
            id: compositionId,
            inputProps,
        });

        // Calculate the actual duration based on the input props
        const fps = composition.fps || 30;
        // Calculate the total frames for a reaction video where clips play simultaneously
        const calculateTotalFrames = (data, fps) => {
            const maxDuration = Math.max(
                ...data.clips.map(clip => clip.durationInSeconds)
            );

            return Math.round(maxDuration * fps);
        };

        // Override the duration with the calculated value
        composition.durationInFrames = calculateTotalFrames(inputProps.compilationData, fps);

        // Make sure temp directory exists for temporary file storage
        const tempDir = path.join(__dirname, 'temp');
        if (!fs.existsSync(tempDir)) {
            console.log(`Creating temp directory: ${tempDir}`);
            fs.mkdirSync(tempDir, { recursive: true });
        }

        const filename = `${compositionId}-${Date.now()}.mp4`;
        const tempFilePath = path.join(tempDir, filename);

        console.log(`Temporary file will be saved to: ${tempFilePath}`);

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

        const videoUrl = await uploadToS3(filename, fileContent, tempFilePath);

        res.status(200).json({
            success: true,
            videoUrl: videoUrl,
            message: 'Video rendered and uploaded to S3 successfully'
        });
    } catch (error) {
        console.error('Error getting compositions:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to generate reaction video',
            message: error.message
        });
    }
});

app.post('/api/hello', async (req, res) => {
    try {
        const compositionId = 'HelloWorld';

        const inputProps = {
            titleText: "API for Remotion",
            titleColor: "#000000",
            logoColor1: "#91EAE4",
            logoColor2: "#86A8E7",
        };

         // Make sure temp directory exists for temporary file storage
        const tempDir = path.join(__dirname, 'temp');
        if (!fs.existsSync(tempDir)) {
            console.log(`Creating temp directory: ${tempDir}`);
            fs.mkdirSync(tempDir, { recursive: true });
        }

        const filename = `${compositionId}-${Date.now()}.mp4`;
        const tempFilePath = path.join(tempDir, filename);

        console.log(`Temporary file will be saved to: ${tempFilePath}`);

        const composition = await selectComposition({
            serveUrl: process.env.REMOTION_SERVE_URL,
            id: compositionId,
            inputProps,
        });

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

        const videoUrl = uploadToS3(filename, fileContent);

        res.status(200).json({
            success: true,
            videoUrl: videoUrl,
            message: 'Video rendered and uploaded to S3 successfully'
        });
    } catch (error) {
        console.error('Error getting compositions:', error);
        res.status(500).json({
            success: false,
            error: 'Failed to generate Hello World video',
            message: error.message
        });
    }
});

// Start the server
const PORT = process.env.PORT || 4444;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});