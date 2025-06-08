import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse

from models import (
    DownloadRequest,
    DownloadListResponse,
    ScanResponse,
    WebMEncodeResponse,
)
from services import S3Service, VideoService

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("S3_BUCKET", "ytdl-videos")
MINIO_SECURE = os.getenv("S3_SECURE", "true").lower() == "true"

# Initialize services
s3_service = S3Service(
    endpoint=MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    bucket=MINIO_BUCKET,
    secure=MINIO_SECURE,
)

video_service = VideoService(s3_service)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting encoding worker.")
    await video_service.start_encoding_worker()
    yield


# FastAPI app
app = FastAPI(title="YouTube Downloader API", version="1.0.0", lifespan=lifespan)


@app.post("/download")
async def start_download(request: DownloadRequest, background_tasks: BackgroundTasks):
    """Start downloading a video from the given URL"""
    logger.info(f"Starting download process for {request.url}")

    # Create download state immediately
    download_state = await video_service.create_download_state(
        str(request.url), request.title
    )

    # Add actual download work to background tasks
    background_tasks.add_task(
        video_service.download_and_upload_sync,
        download_state.download_id,
        str(request.url),
        request.title,
    )

    logger.info(f"Download queued, returning response for {download_state.download_id}")
    return download_state.to_response()


@app.get("/downloads", response_model=DownloadListResponse)
async def list_downloads():
    """List all downloads with their current status"""
    downloads = await video_service.get_all_downloads()
    return DownloadListResponse(downloads=downloads)


@app.delete("/downloads/{download_id}")
async def delete_download(download_id: str):
    """Delete a download and its associated files from S3"""
    success = await video_service.delete_download(download_id)

    if not success:
        raise HTTPException(status_code=404, detail="Download not found")

    return {
        "message": f"Download {download_id} and associated files deleted successfully"
    }


@app.post("/encode/scan", response_model=ScanResponse)
async def trigger_scan_and_encode():
    """
    Manually triggers a scan of the S3 bucket to find and queue non-encoded videos.
    """
    queued_count = await video_service.scan_and_encode_missing_videos()
    return ScanResponse(
        message="Scan complete. See /downloads for status.",
        queued_count=queued_count,
    )


@app.post("/encode/webm/{download_id}", response_model=WebMEncodeResponse)
async def trigger_webm_encode(download_id: str):
    """
    Manually triggers WebM encoding for a specific download.
    """
    success = await video_service.queue_webm_encoding(download_id)

    if success:
        return WebMEncodeResponse(
            message="WebM encoding queued successfully",
            download_id=download_id,
            success=True,
        )
    else:
        return WebMEncodeResponse(
            message="Failed to queue WebM encoding (already encoding, already has WebM, or download not found)",
            download_id=download_id,
            success=False,
        )


@app.get("/")
async def root():
    """Return index.html"""
    return FileResponse("index.html")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    s3_status = "connected" if s3_service.is_available() else "disconnected"
    return {"status": "healthy", "s3": s3_status, "bucket": MINIO_BUCKET}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
