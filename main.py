import asyncio
import base64
import logging
import os
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

from uuid_extensions import uuid7
import yt_dlp
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import RedirectResponse
from minio import Minio
from minio.commonconfig import Tags
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
    s3_object_name: Optional[str] = None
    thumbnail_url: Optional[str] = None


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
                file_size=int(total_bytes) if total_bytes is not None else None,
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


def generate_thumbnail(video_path: str, thumbnail_path: str) -> bool:
    """Generate thumbnail using ffmpeg"""
    try:
        # Generate thumbnail at 5 seconds into the video
        cmd = [
            "ffmpeg",
            "-i",
            video_path,
            "-ss",
            "00:00:05",  # 5 seconds into video
            "-vframes",
            "1",  # Extract 1 frame
            "-q:v",
            "2",  # High quality
            "-y",  # Overwrite output file
            thumbnail_path,
        ]

        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info(f"Thumbnail generated successfully: {thumbnail_path}")
            return True
        else:
            logger.error(f"ffmpeg error: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")
        return False


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
                "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
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
            s3_object_name = f"{download_id}{file_extension}"

            # Generate thumbnail
            thumbnail_path = Path(temp_dir) / f"{download_id}_thumbnail.jpg"
            thumbnail_generated = generate_thumbnail(
                str(downloaded_file), str(thumbnail_path)
            )

            # Upload to S3
            downloads[download_id].update({"status": "uploading", "progress": 100.0})

            if minio_client:
                # Prepare metadata tags
                title_b64 = base64.b64encode(title.encode("utf-8")).decode("utf-8")
                tags = Tags.new_object_tags()
                tags.update(
                    {
                        "title_b64": title_b64,
                        "url_b64": base64.b64encode(url.encode("utf-8")).decode(
                            "utf-8"
                        ),
                        "download_timestamp": datetime.now().isoformat(),
                    }
                )

                # Upload video file
                minio_client.fput_object(
                    MINIO_BUCKET,
                    s3_object_name,
                    str(downloaded_file),
                    content_type=f"video/{file_extension[1:]}"
                    if file_extension
                    else "video/mp4",
                    tags=tags,
                )

                # Upload thumbnail if generated successfully
                thumbnail_object_name = None
                if thumbnail_generated and thumbnail_path.exists():
                    thumbnail_object_name = f"{download_id}_thumbnail.jpg"
                    minio_client.fput_object(
                        MINIO_BUCKET,
                        thumbnail_object_name,
                        str(thumbnail_path),
                        content_type="image/jpeg",
                        tags=tags,
                    )
                    logger.info(f"Thumbnail uploaded: {thumbnail_object_name}")

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
    download_id = str(uuid7())
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
        "s3_object_name": None,
        "thumbnail_url": None,
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
    """List all downloads with their current status, fetching completed from S3."""
    all_downloads: Dict[str, DownloadResponse] = {}

    # 1. Get completed downloads from S3
    if minio_client:
        try:
            objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)
            video_objects = {}
            thumbnail_objects = {}

            # Separate video and thumbnail objects
            for obj in objects:
                obj_name = obj.object_name
                if obj_name.endswith("_thumbnail.jpg"):
                    download_id = obj_name.replace("_thumbnail.jpg", "")
                    thumbnail_objects[download_id] = obj
                else:
                    download_id = Path(obj_name).stem
                    video_objects[download_id] = obj

            # Process video objects and add thumbnail URLs
            for download_id, obj in video_objects.items():
                tags = minio_client.get_object_tags(MINIO_BUCKET, obj.object_name)

                if tags:
                    title = (
                        base64.b64decode(tags["title_b64"]).decode("utf-8")
                        if "title_b64" in tags
                        else "Unknown Title"
                    )
                    url = (
                        base64.b64decode(tags["url_b64"]).decode("utf-8")
                        if "url_b64" in tags
                        else "Unknown URL"
                    )
                    created_at = tags.get(
                        "download_timestamp", obj.last_modified.isoformat()
                    )
                else:
                    title = "Unknown Title"
                    url = "Unknown URL"
                    created_at = obj.last_modified.isoformat()

                # Generate thumbnail presigned URL if thumbnail exists (10 minutes expiry)
                thumbnail_url = None
                if download_id in thumbnail_objects:
                    try:
                        thumbnail_url = minio_client.presigned_get_object(
                            MINIO_BUCKET,
                            f"{download_id}_thumbnail.jpg",
                            expires=timedelta(minutes=10),
                        )
                    except S3Error as e:
                        logger.error(f"Error generating thumbnail presigned URL: {e}")

                all_downloads[download_id] = DownloadResponse(
                    download_id=download_id,
                    url=url,
                    title=title,
                    status="completed",
                    created_at=created_at,
                    progress=100.0,
                    file_size=obj.size,
                    downloaded_size=obj.size,
                    s3_object_name=obj.object_name,
                    thumbnail_url=thumbnail_url,
                )
        except S3Error as e:
            logger.error(f"Error listing S3 objects: {e}")

    # 2. Get in-memory downloads and overwrite/add to the list
    async with downloads_lock:
        for did, d_record in downloads.items():
            # For in-memory downloads, also try to generate thumbnail URL if completed
            thumbnail_url = None
            if d_record.get("status") == "completed" and minio_client:
                try:
                    # Check if thumbnail exists
                    minio_client.stat_object(MINIO_BUCKET, f"{did}_thumbnail.jpg")
                    thumbnail_url = minio_client.presigned_get_object(
                        MINIO_BUCKET,
                        f"{did}_thumbnail.jpg",
                        expires=timedelta(minutes=10),
                    )
                except S3Error:
                    # Thumbnail doesn't exist, ignore
                    pass

            d_record_copy = d_record.copy()
            d_record_copy["thumbnail_url"] = thumbnail_url
            all_downloads[did] = DownloadResponse(**d_record_copy)

    # 3. Sort and return
    download_list = sorted(
        list(all_downloads.values()), key=lambda d: d.created_at, reverse=True
    )
    return DownloadListResponse(downloads=download_list)


@app.get("/downloads/{download_id}")
async def get_download(download_id: str):
    """Get status of a specific download or redirect to S3 file."""
    # 1. Check in-memory downloads
    async with downloads_lock:
        download_record = downloads.get(download_id)

    if download_record:
        if download_record["status"] == "completed" and download_record.get(
            "s3_object_name"
        ):
            pass  # Fall through to S3 redirect logic
        else:
            # For non-completed, return status JSON
            return DownloadResponse(**download_record)

    # 2. Check S3 for a completed download and redirect
    if minio_client:
        s3_object_name = None
        # if we have it from the in-memory record
        if download_record and download_record.get("s3_object_name"):
            s3_object_name = download_record.get("s3_object_name")
        else:
            # If not in memory, we have to find it in S3.
            try:
                objects = minio_client.list_objects(
                    MINIO_BUCKET, prefix=download_id, recursive=False
                )
                found_objects = [
                    obj.object_name
                    for obj in objects
                    if Path(obj.object_name).stem == download_id
                ]
                if found_objects:
                    s3_object_name = found_objects[0]
            except S3Error as e:
                logger.error(
                    f"Error searching for S3 object with prefix {download_id}: {e}"
                )
                raise HTTPException(
                    status_code=500, detail="Error searching for download in S3."
                )

        if s3_object_name:
            try:
                # Generate presigned URL
                signed_url = minio_client.presigned_get_object(
                    MINIO_BUCKET,
                    s3_object_name,
                    expires=timedelta(hours=1),  # Make it valid for 1 hour
                )
                return RedirectResponse(url=signed_url)
            except S3Error as e:
                logger.error(
                    f"Error generating presigned URL for {s3_object_name}: {e}"
                )
                raise HTTPException(
                    status_code=500, detail="Could not generate download link."
                )

    # 3. If not found anywhere
    raise HTTPException(status_code=404, detail="Download not found")


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
