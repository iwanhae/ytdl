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
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error
from minio.deleteobjects import DeleteObject
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
encoding_queue = asyncio.Queue()
main_loop: Optional[asyncio.AbstractEventLoop] = None

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


@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    logger.info("Starting encoding worker.")
    asyncio.create_task(encoding_worker())
    yield


# FastAPI app
app = FastAPI(title="YouTube Downloader API", version="1.0.0", lifespan=lifespan)


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
    thumbnail_url: Optional[HttpUrl] = None
    encoding_status: Optional[str] = None
    original_video_url: Optional[HttpUrl] = None
    aac_url: Optional[HttpUrl] = None
    webm_url: Optional[HttpUrl] = None


class DownloadListResponse(BaseModel):
    downloads: List[DownloadResponse]


class ScanResponse(BaseModel):
    message: str
    queued_count: int


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

            downloads[download_id]["s3_object_name"] = s3_object_name

            # Generate thumbnail
            thumbnail_path = Path(temp_dir) / f"{download_id}.thumbnail.jpg"
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
                    thumbnail_object_name = f"{download_id}.thumbnail.jpg"
                    minio_client.fput_object(
                        MINIO_BUCKET,
                        thumbnail_object_name,
                        str(thumbnail_path),
                        content_type="image/jpeg",
                        tags=tags,
                    )
                    logger.info(f"Thumbnail uploaded: {thumbnail_object_name}")

                # Update final status
                update_data = {
                    "status": "completed",
                    "progress": 100.0,
                    "s3_object_name": s3_object_name,
                    "file_size": downloaded_file.stat().st_size,
                    "encoding_status": "queued",
                }
                downloads[download_id].update(update_data)

                # Add to encoding queue
                if main_loop:
                    future = asyncio.run_coroutine_threadsafe(
                        encoding_queue.put((download_id, s3_object_name)), main_loop
                    )
                    future.result()  # Wait for the item to be queued
                    logger.info(f"Added {download_id} to encoding queue.")

                logger.info(f"Successfully downloaded and uploaded: {title}")
            else:
                raise Exception("MinIO client not available")

    except Exception as e:
        logger.error(f"Error downloading {url}: {e}")
        downloads[download_id].update({"status": "error", "error": str(e)})

    logger.info(f"download_and_upload completed for {download_id}")


async def encoding_worker():
    """Worker to process video encoding tasks from a queue."""
    while True:
        download_id, s3_object_name = await encoding_queue.get()
        logger.info(f"Starting encoding for {download_id} ({s3_object_name})")

        async with downloads_lock:
            downloads[download_id].update({"encoding_status": "encoding"})

        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_dir_path = Path(temp_dir)
                original_video_path = temp_dir_path / s3_object_name

                # 1. Download original video from S3
                logger.info(f"Downloading {s3_object_name} for encoding.")
                minio_client.fget_object(
                    MINIO_BUCKET, s3_object_name, str(original_video_path)
                )

                # Get metadata from downloads record
                async with downloads_lock:
                    download_record = downloads.get(download_id, {})
                    video_title = download_record.get("title", "Unknown")
                    video_url = download_record.get("url", "Unknown")
                    download_time = download_record.get("created_at", "Unknown")

                # 2. Encode to AAC
                aac_filename = f"{download_id}.encoded.aac"
                aac_path = temp_dir_path / aac_filename

                # Create description with download time and original URL
                description = f"Downloaded: {download_time}, Original URL: {video_url}"

                # Download existing thumbnail from S3 for album art
                thumbnail_for_aac_path = temp_dir_path / f"{download_id}.thumbnail.jpg"
                thumbnail_object_name = f"{download_id}.thumbnail.jpg"
                thumbnail_available = False

                try:
                    logger.info(
                        f"Downloading existing thumbnail from S3 for {download_id}"
                    )
                    minio_client.fget_object(
                        MINIO_BUCKET, thumbnail_object_name, str(thumbnail_for_aac_path)
                    )
                    thumbnail_available = thumbnail_for_aac_path.exists()
                    logger.info(f"Successfully downloaded thumbnail for AAC album art")
                except Exception as e:
                    logger.warning(f"Failed to download thumbnail from S3: {e}")
                    thumbnail_available = False

                # Build FFmpeg command for AAC encoding with optional album art
                ffmpeg_aac_cmd = [
                    "ffmpeg",
                    "-i",
                    str(original_video_path),
                ]

                # Add thumbnail as input if available
                if thumbnail_available:
                    ffmpeg_aac_cmd.extend(["-i", str(thumbnail_for_aac_path)])

                ffmpeg_aac_cmd.extend(
                    [
                        "-vn",  # No video
                        "-c:a",
                        "aac",
                        "-id3v2_version",
                        "4",  # Use ID3v2.4
                    ]
                )

                # Map audio and optionally album art
                if thumbnail_available:
                    ffmpeg_aac_cmd.extend(
                        [
                            "-map",
                            "0:a:0",  # Audio from first input (video)
                            "-map",
                            "1:0",  # Image from second input (thumbnail)
                            "-c:v",
                            "mjpeg",  # Codec for album art
                            "-disposition:v",
                            "attached_pic",  # Mark as album art
                        ]
                    )

                # Extract date from created_at for ID3v2.4 TDRC tag
                try:
                    if download_time != "Unknown":
                        dt = datetime.fromisoformat(
                            download_time.replace("Z", "+00:00")
                        )
                        date_tag = dt.strftime("%Y-%m-%d")
                    else:
                        date_tag = datetime.now().strftime("%Y-%m-%d")
                except:
                    date_tag = datetime.now().strftime("%Y-%m-%d")

                # Add metadata to AAC file
                ffmpeg_aac_cmd.extend(
                    [
                        "-metadata",
                        f"title={video_title}",  # TIT2: Title
                        "-metadata",
                        "artist=YTDL",  # TPE1: Artist
                        "-metadata",
                        "album=Downloaded Videos",  # TALB: Album
                        "-metadata",
                        f"date={date_tag}",  # TDRC: Recording time
                        "-metadata",
                        f"comment={description}",  # COMM: Comments
                        "-metadata",
                        "genre=Downloaded",  # TCON: Content type
                        "-y",
                        str(aac_path),
                    ]
                )

                logger.info(f"Starting AAC encoding for {download_id}")
                result = await asyncio.to_thread(
                    subprocess.run, ffmpeg_aac_cmd, text=True
                )
                if result.returncode != 0:
                    raise Exception(
                        f"FFmpeg AAC encoding failed with return code {result.returncode}"
                    )
                logger.info(f"AAC encoding successful for {download_id}")
                minio_client.fput_object(
                    MINIO_BUCKET, aac_filename, str(aac_path), content_type="audio/aac"
                )
                logger.info(f"Uploaded {aac_filename} to S3")

                async with downloads_lock:
                    downloads[download_id].update(
                        {
                            "encoding_status": "aac_completed",
                            "s3_aac_object_name": aac_filename,
                        }
                    )
                # 3. Encode to WebM
                webm_filename = f"{download_id}.encoded.webm"
                webm_path = temp_dir_path / webm_filename
                ffmpeg_webm_cmd = [
                    "ffmpeg",
                    "-i",
                    str(original_video_path),
                    "-vf",
                    "scale=-1:720",
                    "-y",
                    str(webm_path),
                ]
                logger.info(f"Starting WebM encoding for {download_id}")
                result = await asyncio.to_thread(
                    subprocess.run, ffmpeg_webm_cmd, text=True
                )
                if result.returncode != 0:
                    raise Exception(
                        f"FFmpeg WebM encoding failed with return code {result.returncode}"
                    )
                logger.info(f"WebM encoding successful for {download_id}")
                minio_client.fput_object(
                    MINIO_BUCKET,
                    webm_filename,
                    str(webm_path),
                    content_type="video/webm",
                )
                logger.info(f"Uploaded {webm_filename} to S3")

            async with downloads_lock:
                downloads[download_id].update(
                    {
                        "encoding_status": "completed",
                        "s3_aac_object_name": aac_filename,
                        "s3_webm_object_name": webm_filename,
                    }
                )

        except Exception as e:
            logger.error(f"Encoding failed for {download_id}: {e}")
            async with downloads_lock:
                downloads[download_id].update(
                    {"encoding_status": "error", "error": str(e)}
                )
        finally:
            encoding_queue.task_done()


async def scan_and_encode_missing_videos() -> int:
    """Scans S3 for videos missing encoded versions and queues them for encoding."""
    logger.info("Starting scan for non-encoded videos in S3.")
    if not minio_client:
        logger.warning("MinIO client not available, skipping scan.")
        return 0

    try:
        s3_files_map: Dict[str, Dict[str, Optional[str]]] = {}
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=True)

        for obj in objects:
            download_id = (
                Path(obj.object_name)
                .stem.replace(".encoded", "")
                .replace(".thumbnail", "")
            )
            if download_id not in s3_files_map:
                s3_files_map[download_id] = {
                    "original": None,
                    "aac": None,
                    "webm": None,
                }

            if obj.object_name.endswith(".encoded.aac"):
                s3_files_map[download_id]["aac"] = obj.object_name
            elif obj.object_name.endswith(".encoded.webm"):
                s3_files_map[download_id]["webm"] = obj.object_name
            elif not obj.object_name.endswith(".thumbnail.jpg"):
                s3_files_map[download_id]["original"] = obj.object_name

        logger.info(f"s3_files_map: {s3_files_map}")

        queued_count = 0
        for download_id, files in s3_files_map.items():
            if files["original"] and not (files["aac"] and files["webm"]):
                is_processing = False
                async with downloads_lock:
                    if download_id in downloads:
                        # Already in memory, likely being processed or queued.
                        is_processing = True

                if not is_processing:
                    logger.info(
                        f"Found non-encoded video: {files['original']}. Queuing for encoding."
                    )
                    original_object_name = files["original"]

                    # To track progress, create a download record
                    download_record = {
                        "download_id": download_id,
                        "url": "N/A (from S3 scan)",
                        "title": "N/A (from S3 scan)",
                        "status": "completed",
                        "created_at": datetime.now().isoformat(),
                        "progress": 100.0,
                        "s3_object_name": original_object_name,
                        "encoding_status": "queued",
                    }
                    async with downloads_lock:
                        downloads[download_id] = download_record

                    await encoding_queue.put((download_id, original_object_name))
                    queued_count += 1

        logger.info(f"Scan complete. Queued {queued_count} videos for encoding.")
        return queued_count

    except S3Error as e:
        logger.error(f"Error during S3 scan for non-encoded videos: {e}")
        return 0


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
        "encoding_status": None,
        "original_video_url": None,
        "aac_url": None,
        "webm_url": None,
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
            aac_objects = {}
            webm_objects = {}

            # Separate video and thumbnail objects
            for obj in objects:
                obj_name = obj.object_name
                download_id_stem = (
                    Path(obj_name)
                    .stem.replace(".thumbnail", "")
                    .replace(".encoded", "")
                )

                if obj_name.endswith(".thumbnail.jpg"):
                    thumbnail_objects[download_id_stem] = obj
                elif obj_name.endswith(".encoded.aac"):
                    aac_objects[download_id_stem] = obj
                elif obj_name.endswith(".encoded.webm"):
                    webm_objects[download_id_stem] = obj
                else:
                    video_objects[download_id_stem] = obj

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

                expires = timedelta(hours=12)

                # Generate original video presigned URL if original video exists (12 hours expiry)
                original_video_url = None
                if download_id in video_objects:
                    try:
                        original_video_url = minio_client.presigned_get_object(
                            MINIO_BUCKET,
                            video_objects[download_id].object_name,
                            expires=expires,
                        )
                    except S3Error as e:
                        logger.error(
                            f"Error generating original video presigned URL: {e}"
                        )

                # Generate thumbnail presigned URL if thumbnail exists (12 hours expiry)
                thumbnail_url = None
                if download_id in thumbnail_objects:
                    try:
                        thumbnail_url = minio_client.presigned_get_object(
                            MINIO_BUCKET,
                            f"{download_id}.thumbnail.jpg",
                            expires=expires,
                        )
                    except S3Error as e:
                        logger.error(f"Error generating thumbnail presigned URL: {e}")

                # Generate AAC presigned URL
                aac_url = None
                if download_id in aac_objects:
                    try:
                        aac_url = minio_client.presigned_get_object(
                            MINIO_BUCKET, f"{download_id}.encoded.aac", expires=expires
                        )
                    except S3Error as e:
                        logger.error(f"Error generating AAC presigned URL: {e}")

                # Generate WebM presigned URL
                webm_url = None
                if download_id in webm_objects:
                    try:
                        webm_url = minio_client.presigned_get_object(
                            MINIO_BUCKET, f"{download_id}.encoded.webm", expires=expires
                        )
                    except S3Error as e:
                        logger.error(f"Error generating WebM presigned URL: {e}")

                encoding_status = (
                    "completed"
                    if download_id in aac_objects and download_id in webm_objects
                    else None
                )

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
                    encoding_status=encoding_status,
                    original_video_url=original_video_url,
                    aac_url=aac_url,
                    webm_url=webm_url,
                )
        except S3Error as e:
            logger.error(f"Error listing S3 objects: {e}")

    # 2. Get in-memory downloads and overwrite/add to the list
    async with downloads_lock:
        for did, d_record in downloads.items():
            expires = timedelta(hours=12)
            # For in-memory downloads, also try to generate thumbnail URL if completed
            thumbnail_url = None
            if d_record.get("status") == "completed" and minio_client:
                try:
                    # Check if thumbnail exists
                    thumb_obj_name = f"{did}.thumbnail.jpg"
                    minio_client.stat_object(MINIO_BUCKET, thumb_obj_name)
                    thumbnail_url = minio_client.presigned_get_object(
                        MINIO_BUCKET,
                        thumb_obj_name,
                        expires=expires,
                    )
                except S3Error:
                    # Thumbnail doesn't exist, ignore
                    pass

            # Generate presigned URL for original video
            original_video_url = None
            if d_record.get("s3_object_name") and minio_client:
                try:
                    original_video_url = minio_client.presigned_get_object(
                        MINIO_BUCKET, d_record["s3_object_name"], expires=expires
                    )
                except S3Error:
                    pass

            # Generate presigned URLs for encoded files
            aac_url = None
            if d_record.get("s3_aac_object_name") and minio_client:
                try:
                    aac_url = minio_client.presigned_get_object(
                        MINIO_BUCKET, d_record["s3_aac_object_name"], expires=expires
                    )
                except S3Error:
                    pass

            webm_url = None
            if d_record.get("s3_webm_object_name") and minio_client:
                try:
                    webm_url = minio_client.presigned_get_object(
                        MINIO_BUCKET, d_record["s3_webm_object_name"], expires=expires
                    )
                except S3Error:
                    pass

            d_record_copy = d_record.copy()
            d_record_copy["thumbnail_url"] = thumbnail_url
            d_record_copy["aac_url"] = aac_url
            d_record_copy["webm_url"] = webm_url
            d_record_copy["original_video_url"] = original_video_url
            all_downloads[did] = DownloadResponse(**d_record_copy)

    # 3. Sort and return
    download_list = sorted(
        list(all_downloads.values()), key=lambda d: d.created_at, reverse=True
    )
    return DownloadListResponse(downloads=download_list)


@app.delete("/downloads/{download_id}")
async def delete_download(download_id: str):
    """Delete a download and its associated files from S3"""
    # Find the download record, first in-memory, then try S3 for persisted downloads
    download_s3_object_name = None
    async with downloads_lock:
        if download_id in downloads:
            download_s3_object_name = downloads[download_id].get("s3_object_name")

    if not download_s3_object_name and minio_client:
        # If not in memory, maybe it's a completed download, check S3
        try:
            objects = minio_client.list_objects(MINIO_BUCKET, prefix=download_id)
            for obj in objects:
                if not obj.object_name.endswith(
                    (".thumbnail.jpg", ".encoded.aac", ".encoded.webm")
                ):
                    download_s3_object_name = obj.object_name
                    break
        except S3Error:
            pass  # Object not found on S3 either

    if not download_s3_object_name:
        # If still no s3_object_name, it's not found anywhere
        # but maybe it's still in memory but not uploaded, so we just remove from memory
        async with downloads_lock:
            if download_id in downloads:
                del downloads[download_id]
                return {"message": f"In-memory download {download_id} deleted."}
        raise HTTPException(status_code=404, detail="Download not found")

    # Delete from S3 if it exists
    if minio_client:
        try:
            file_extension = "".join(Path(download_s3_object_name).suffixes)
            objects_to_delete = [
                DeleteObject(f"{download_id}{file_extension}"),
                DeleteObject(f"{download_id}.thumbnail.jpg"),
                DeleteObject(f"{download_id}.encoded.aac"),
                DeleteObject(f"{download_id}.encoded.webm"),
            ]
            errors = minio_client.remove_objects(MINIO_BUCKET, objects_to_delete)
            deleted_files, error_files = [], []
            for e in errors:
                error_files.append(e.object_name)
            for d in objects_to_delete:
                if d.object_name not in error_files:
                    deleted_files.append(d.object_name)
            logger.info(f"Deleted S3 objects: {deleted_files}")
            if error_files:
                logger.error(f"Errors deleting S3 objects: {error_files}")
        except S3Error as e:
            logger.error(f"Error deleting S3 objects: {e}")
            raise HTTPException(
                status_code=500, detail=f"Failed to delete from S3: {e}"
            )

    # Remove from downloads tracking
    async with downloads_lock:
        if download_id in downloads:
            del downloads[download_id]

    return {
        "message": f"Download {download_id} and associated files deleted successfully"
    }


@app.post("/encode/scan", response_model=ScanResponse)
async def trigger_scan_and_encode():
    """
    Manually triggers a scan of the S3 bucket to find and queue non-encoded videos.
    """
    queued_count = await scan_and_encode_missing_videos()
    return ScanResponse(
        message="Scan complete. See /downloads for status.",
        queued_count=queued_count,
    )


@app.get("/")
async def root():
    """Return index.html"""
    return FileResponse("index.html")


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    minio_status = "connected" if minio_client else "disconnected"
    return {"status": "healthy", "minio": minio_status, "bucket": MINIO_BUCKET}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
