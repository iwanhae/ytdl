import asyncio
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from uuid import uuid4

import yt_dlp
from fastapi import FastAPI, HTTPException, BackgroundTasks
from minio import Minio
from minio.error import S3Error
from pydantic import BaseModel, HttpUrl

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
MINIO_ENDPOINT = os.getenv("S3_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("S3_BUCKET", "ytdl-videos")
MINIO_SECURE = os.getenv("S3_SECURE", "true").lower() == "true"

# Global state for tracking downloads
downloads: Dict[str, Dict] = {}
downloads_lock = asyncio.Lock()

# Initialize MinIO client
try:
    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )

    # Create bucket if it doesn't exist
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        logger.info(f"Created bucket: {MINIO_BUCKET}")
    else:
        logger.info(f"Bucket {MINIO_BUCKET} already exists")

except Exception as e:
    logger.error(f"Failed to initialize MinIO client: {e}")
    minio_client = None

# FastAPI app
app = FastAPI(title="YouTube Downloader API", version="1.0.0")


# Pydantic models
class DownloadRequest(BaseModel):
    url: HttpUrl
    title: Optional[str] = None


class DownloadResponse(BaseModel):
    download_id: str
    url: str
    title: str
    status: str
    created_at: str
    progress: float = 0.0
    file_size: Optional[int] = None
    downloaded_size: Optional[int] = None
    speed: Optional[str] = None
    eta: Optional[str] = None
    error: Optional[str] = None


class DownloadListResponse(BaseModel):
    downloads: List[DownloadResponse]


class DownloadProgress:
    def __init__(self, download_id: str):
        self.download_id = download_id

    def __call__(self, d):
        if d["status"] == "downloading":
            total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded_bytes = d.get("downloaded_bytes", 0)

            if total_bytes:
                progress = (downloaded_bytes / total_bytes) * 100
            else:
                progress = 0.0

            self.update_progress(
                progress=progress,
                file_size=total_bytes,
                downloaded_size=downloaded_bytes,
                speed=d.get("speed_str"),
                eta=d.get("eta_str"),
            )
        elif d["status"] == "finished":
            self.update_progress(progress=100.0, status="uploading")
        elif d["status"] == "error":
            self.update_progress(
                status="error", error=str(d.get("error", "Unknown error"))
            )

    def update_progress(self, **kwargs):
        if self.download_id in downloads:
            downloads[self.download_id].update(kwargs)


def download_and_upload(download_id: str, url: str, title: str):
    """Download video and upload to S3 storage"""
    logger.info(f"download_and_upload started for {download_id}")
    try:
        # Update status to downloading (synchronous update)
        downloads[download_id].update({"status": "downloading", "progress": 0.0})

        # Create temporary directory for download
        with tempfile.TemporaryDirectory() as temp_dir:
            output_path = Path(temp_dir) / f"{download_id}.%(ext)s"

            # Configure yt-dlp options
            ydl_opts = {
                "format": "best[height<=1080]",  # Best quality up to 1080p
                "outtmpl": str(output_path),
                "progress_hooks": [DownloadProgress(download_id)],
                "no_warnings": True,
            }

            # Download video
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(url, download=False)
                if not title:
                    title = info.get("title", "Unknown")

                downloads[download_id]["title"] = title

                # Actually download the video
                ydl.download([url])

            # Find the downloaded file
            downloaded_files = list(Path(temp_dir).glob(f"{download_id}.*"))
            if not downloaded_files:
                raise Exception("No file was downloaded")

            downloaded_file = downloaded_files[0]
            file_extension = downloaded_file.suffix
            s3_object_name = f"{download_id}/{title}{file_extension}"

            # Upload to S3
            downloads[download_id].update({"status": "uploading", "progress": 100.0})

            if minio_client:
                minio_client.fput_object(
                    MINIO_BUCKET,
                    s3_object_name,
                    str(downloaded_file),
                    content_type=f"video/{file_extension[1:]}"
                    if file_extension
                    else "video/mp4",
                )

                # Update final status
                downloads[download_id].update(
                    {
                        "status": "completed",
                        "progress": 100.0,
                        "s3_object_name": s3_object_name,
                        "file_size": downloaded_file.stat().st_size,
                    }
                )

                logger.info(f"Successfully downloaded and uploaded: {title}")
            else:
                raise Exception("MinIO client not available")

    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        downloads[download_id].update({"status": "error", "error": str(e)})

    logger.info(f"download_and_upload completed for {download_id}")


@app.post("/download", response_model=DownloadResponse)
async def start_download(request: DownloadRequest, background_tasks: BackgroundTasks):
    """Start downloading a video from the given URL"""
    download_id = str(uuid4())
    logger.info(f"Starting download process for {request.url}, ID: {download_id}")

    # Initialize download record
    download_record = {
        "download_id": download_id,
        "url": str(request.url),
        "title": request.title or "Unknown",
        "status": "queued",
        "created_at": datetime.now().isoformat(),
        "progress": 0.0,
        "file_size": None,
        "downloaded_size": None,
        "speed": None,
        "eta": None,
        "error": None,
    }

    async with downloads_lock:
        downloads[download_id] = download_record

    # Start background download task
    background_tasks.add_task(
        download_and_upload, download_id, str(request.url), request.title
    )

    logger.info(f"Background task started, returning response for {download_id}")
    return DownloadResponse(**download_record)


@app.get("/downloads", response_model=DownloadListResponse)
async def list_downloads():
    """List all downloads with their current status"""
    async with downloads_lock:
        download_list = [
            DownloadResponse(**download) for download in downloads.values()
        ]

    return DownloadListResponse(downloads=download_list)


@app.get("/downloads/{download_id}", response_model=DownloadResponse)
async def get_download(download_id: str):
    """Get status of a specific download"""
    async with downloads_lock:
        if download_id not in downloads:
            raise HTTPException(status_code=404, detail="Download not found")
        return DownloadResponse(**downloads[download_id])


@app.delete("/downloads/{download_id}")
async def delete_download(download_id: str):
    """Delete a download and its associated file from S3"""
    async with downloads_lock:
        if download_id not in downloads:
            raise HTTPException(status_code=404, detail="Download not found")

        download_record = downloads[download_id]

    # Delete from S3 if it exists
    if minio_client and download_record.get("s3_object_name"):
        try:
            minio_client.remove_object(MINIO_BUCKET, download_record["s3_object_name"])
            logger.info(f"Deleted S3 object: {download_record['s3_object_name']}")
        except S3Error as e:
            logger.error(f"Error deleting S3 object: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to delete from S3: {e}"
            )

    # Remove from downloads tracking
    async with downloads_lock:
        del downloads[download_id]

    return {"message": f"Download {download_id} deleted successfully"}


@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "YouTube Downloader API",
        "version": "1.0.0",
        "endpoints": {
            "POST /download": "Start downloading a video",
            "GET /downloads": "List all downloads",
            "GET /downloads/{id}": "Get specific download status",
            "DELETE /downloads/{id}": "Delete a download",
        },
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    minio_status = "connected" if minio_client else "disconnected"
    return {"status": "healthy", "minio": minio_status, "bucket": MINIO_BUCKET}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
