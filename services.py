import asyncio
import base64
import logging
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import yt_dlp
from minio import Minio
from minio.commonconfig import Tags
from minio.error import S3Error
from minio.deleteobjects import DeleteObject
from uuid_extensions import uuid7

from models import DownloadState, DownloadResponse, S3ObjectInfo

logger = logging.getLogger(__name__)


class S3Service:
    """Service for handling all S3 operations"""

    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
        secure: bool = True,
    ):
        self.bucket = bucket
        try:
            self.client = Minio(
                endpoint, access_key=access_key, secret_key=secret_key, secure=secure
            )
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
            else:
                logger.info(f"Bucket {bucket} already exists")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO client: {e}")
            self.client = None

    def is_available(self) -> bool:
        """Check if S3 service is available"""
        return self.client is not None

    def upload_file(
        self,
        local_path: str,
        object_name: str,
        content_type: str = None,
        title: str = None,
        url: str = None,
    ) -> bool:
        """Upload a file to S3 with optional metadata"""
        if not self.client:
            return False

        try:
            tags = None
            if title or url:
                tags = Tags.new_object_tags()
                if title:
                    tags.update(
                        {
                            "title_b64": base64.b64encode(title.encode("utf-8")).decode(
                                "utf-8"
                            )
                        }
                    )
                if url:
                    tags.update(
                        {
                            "url_b64": base64.b64encode(url.encode("utf-8")).decode(
                                "utf-8"
                            )
                        }
                    )
                tags.update({"download_timestamp": datetime.now().isoformat()})

            self.client.fput_object(
                self.bucket,
                object_name,
                local_path,
                content_type=content_type,
                tags=tags,
            )
            return True
        except S3Error as e:
            logger.error(f"Error uploading file {object_name}: {e}")
            return False

    def download_file(self, object_name: str, local_path: str) -> bool:
        """Download a file from S3"""
        if not self.client:
            return False

        try:
            self.client.fget_object(self.bucket, object_name, local_path)
            return True
        except S3Error as e:
            logger.error(f"Error downloading file {object_name}: {e}")
            return False

    def get_presigned_url(
        self, object_name: str, expires: timedelta = timedelta(hours=12)
    ) -> Optional[str]:
        """Generate a presigned URL for an object"""
        if not self.client:
            return None

        try:
            return self.client.presigned_get_object(
                self.bucket, object_name, expires=expires
            )
        except S3Error as e:
            logger.error(f"Error generating presigned URL for {object_name}: {e}")
            return None

    def list_objects_by_prefix(self, prefix: str) -> List[str]:
        """List objects with a given prefix"""
        if not self.client:
            return []

        try:
            objects = self.client.list_objects(
                self.bucket, prefix=prefix, recursive=True
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects with prefix {prefix}: {e}")
            return []

    def get_all_download_objects(self) -> Dict[str, S3ObjectInfo]:
        """Get all download objects organized by download_id"""
        if not self.client:
            return {}

        try:
            objects = self.client.list_objects(self.bucket, recursive=True)
            s3_files_map: Dict[str, S3ObjectInfo] = {}

            for obj in objects:
                download_id = (
                    Path(obj.object_name)
                    .stem.replace(".encoded", "")
                    .replace(".thumbnail", "")
                )

                if download_id not in s3_files_map:
                    s3_files_map[download_id] = S3ObjectInfo(download_id=download_id)

                if obj.object_name.endswith(".encoded.mp3"):
                    s3_files_map[download_id].mp3 = obj.object_name
                elif obj.object_name.endswith(".encoded.webm"):
                    s3_files_map[download_id].webm = obj.object_name
                elif obj.object_name.endswith(".thumbnail.jpg"):
                    s3_files_map[download_id].thumbnail = obj.object_name
                else:
                    s3_files_map[download_id].original = obj.object_name

            return s3_files_map
        except S3Error as e:
            logger.error(f"Error getting all download objects: {e}")
            return {}

    def get_object_metadata(self, object_name: str) -> Dict[str, str]:
        """Get metadata for an object"""
        if not self.client:
            return {}

        try:
            tags = self.client.get_object_tags(self.bucket, object_name)
            metadata = {}

            if "title_b64" in tags:
                metadata["title"] = base64.b64decode(tags["title_b64"]).decode("utf-8")
            if "url_b64" in tags:
                metadata["url"] = base64.b64decode(tags["url_b64"]).decode("utf-8")
            if "download_timestamp" in tags:
                metadata["created_at"] = tags["download_timestamp"]

            return metadata
        except S3Error as e:
            logger.error(f"Error getting metadata for {object_name}: {e}")
            return {}

    def delete_objects(self, object_names: List[str]) -> bool:
        """Delete multiple objects"""
        if not self.client:
            return False

        try:
            delete_objects = [DeleteObject(name) for name in object_names]
            errors = self.client.remove_objects(self.bucket, delete_objects)

            error_count = 0
            for error in errors:
                logger.error(f"Error deleting object {error.name}: {error}")
                error_count += 1

            return error_count == 0
        except S3Error as e:
            logger.error(f"Error deleting objects: {e}")
            return False

    def object_exists(self, object_name: str) -> bool:
        """Check if an object exists"""
        if not self.client:
            return False

        try:
            self.client.stat_object(self.bucket, object_name)
            return True
        except S3Error:
            return False


class VideoService:
    """Service for handling video download, encoding, and state management"""

    def __init__(self, s3_service: S3Service):
        self.s3_service = s3_service
        self.downloads: Dict[str, DownloadState] = {}
        self.downloads_lock = asyncio.Lock()
        self.encoding_queue = asyncio.Queue()
        self._encoding_worker_task = None

    async def start_encoding_worker(self):
        """Start the encoding worker task"""
        if self._encoding_worker_task is None:
            self._encoding_worker_task = asyncio.create_task(self._encoding_worker())

    async def _encoding_worker(self):
        """Worker to process video encoding tasks from a queue"""
        while True:
            try:
                download_id, s3_object_name = await self.encoding_queue.get()
                logger.info(f"Starting encoding for {download_id} ({s3_object_name})")

                async with self.downloads_lock:
                    if download_id in self.downloads:
                        self.downloads[download_id].encoding_status = "encoding"

                await self._encode_video(download_id, s3_object_name)

            except Exception as e:
                logger.error(f"Error in encoding worker: {e}")
                async with self.downloads_lock:
                    if download_id in self.downloads:
                        self.downloads[download_id].encoding_status = "error"
                        self.downloads[download_id].error = str(e)
            finally:
                self.encoding_queue.task_done()

    async def _encode_video(self, download_id: str, s3_object_name: str):
        """Encode a video to MP3 and WebM formats"""
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir_path = Path(temp_dir)
            original_video_path = temp_dir_path / s3_object_name

            # Download original video
            if not self.s3_service.download_file(
                s3_object_name, str(original_video_path)
            ):
                raise Exception(f"Failed to download {s3_object_name}")

            # Get metadata
            async with self.downloads_lock:
                download_state = self.downloads.get(download_id)
                if download_state:
                    video_title = download_state.title
                    video_url = download_state.url
                    download_time = download_state.created_at
                else:
                    video_title = "Unknown"
                    video_url = "Unknown"
                    download_time = "Unknown"

            # Encode to MP3
            await self._encode_to_mp3(
                download_id,
                original_video_path,
                temp_dir_path,
                video_title,
                video_url,
                download_time,
            )

            # Encode to WebM
            await self._encode_to_webm(download_id, original_video_path, temp_dir_path)

            async with self.downloads_lock:
                if download_id in self.downloads:
                    self.downloads[download_id].encoding_status = "completed"

    async def _encode_to_mp3(
        self,
        download_id: str,
        original_video_path: Path,
        temp_dir_path: Path,
        video_title: str,
        video_url: str,
        download_time: str,
    ):
        """Encode video to MP3 with metadata"""
        mp3_filename = f"{download_id}.encoded.mp3"
        mp3_path = temp_dir_path / mp3_filename

        # Download thumbnail for album art
        thumbnail_path = temp_dir_path / f"{download_id}.thumbnail.jpg"
        thumbnail_available = self.s3_service.download_file(
            f"{download_id}.thumbnail.jpg", str(thumbnail_path)
        )

        # Build FFmpeg command
        ffmpeg_cmd = ["ffmpeg", "-i", str(original_video_path)]

        if thumbnail_available:
            ffmpeg_cmd.extend(["-i", str(thumbnail_path)])

        ffmpeg_cmd.extend(["-c:a", "libmp3lame", "-id3v2_version", "4"])

        if thumbnail_available:
            ffmpeg_cmd.extend(
                [
                    "-map",
                    "0:a:0",
                    "-map",
                    "1:0",
                    "-c:v",
                    "copy",
                    "-disposition:v:0",
                    "attached_pic",
                ]
            )
        else:
            ffmpeg_cmd.append("-vn")

        # Add metadata
        try:
            if download_time != "Unknown":
                dt = datetime.fromisoformat(download_time.replace("Z", "+00:00"))
                date_tag = dt.strftime("%Y-%m-%d")
            else:
                date_tag = datetime.now().strftime("%Y-%m-%d")
        except:
            date_tag = datetime.now().strftime("%Y-%m-%d")

        description = f"Downloaded: {download_time}, Original URL: {video_url}"

        ffmpeg_cmd.extend(
            [
                "-metadata",
                f"title={video_title}",
                "-metadata",
                "artist=YTDL",
                "-metadata",
                "album=Downloaded Videos",
                "-metadata",
                f"date={date_tag}",
                "-metadata",
                f"comment={description}",
                "-metadata",
                "genre=Downloaded",
                "-y",
                str(mp3_path),
            ]
        )

        result = await asyncio.to_thread(subprocess.run, ffmpeg_cmd, text=True)
        if result.returncode != 0:
            raise Exception(
                f"FFmpeg MP3 encoding failed with return code {result.returncode}"
            )

        # Upload to S3
        if not self.s3_service.upload_file(str(mp3_path), mp3_filename, "audio/mpeg"):
            raise Exception(f"Failed to upload {mp3_filename}")

        async with self.downloads_lock:
            if download_id in self.downloads:
                self.downloads[download_id].encoding_status = "mp3_completed"
                self.downloads[download_id].s3_mp3_object_name = mp3_filename

    async def _encode_to_webm(
        self, download_id: str, original_video_path: Path, temp_dir_path: Path
    ):
        """Encode video to WebM format"""
        webm_filename = f"{download_id}.encoded.webm"
        webm_path = temp_dir_path / webm_filename

        ffmpeg_cmd = [
            "ffmpeg",
            "-i",
            str(original_video_path),
            "-vf",
            "scale=-1:720",
            "-y",
            str(webm_path),
        ]

        result = await asyncio.to_thread(subprocess.run, ffmpeg_cmd, text=True)
        if result.returncode != 0:
            raise Exception(
                f"FFmpeg WebM encoding failed with return code {result.returncode}"
            )

        # Upload to S3
        if not self.s3_service.upload_file(str(webm_path), webm_filename, "video/webm"):
            raise Exception(f"Failed to upload {webm_filename}")

        async with self.downloads_lock:
            if download_id in self.downloads:
                self.downloads[download_id].s3_webm_object_name = webm_filename

    def _generate_thumbnail(self, video_path: str, thumbnail_path: str) -> bool:
        """Generate thumbnail using ffmpeg"""
        try:
            cmd = [
                "ffmpeg",
                "-i",
                video_path,
                "-ss",
                "00:00:05",
                "-vframes",
                "1",
                "-q:v",
                "2",
                "-y",
                thumbnail_path,
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            return result.returncode == 0
        except Exception as e:
            logger.error(f"Error generating thumbnail: {e}")
            return False

    async def _update_download_state(self, download_id: str, **kwargs):
        """Update download state"""
        async with self.downloads_lock:
            if download_id in self.downloads:
                for key, value in kwargs.items():
                    setattr(self.downloads[download_id], key, value)

    def _update_download_state_sync(self, download_id: str, **kwargs):
        """Update download state synchronously (for use in sync context)"""
        if download_id in self.downloads:
            for key, value in kwargs.items():
                setattr(self.downloads[download_id], key, value)

    async def create_download_state(
        self, url: str, title: Optional[str] = None
    ) -> DownloadState:
        """Create download state immediately without starting background work"""
        download_id = str(uuid7())

        download_state = DownloadState(
            download_id=download_id,
            url=url,
            title=title or "Unknown",
            status="queued",
            created_at=datetime.now().isoformat(),
        )

        async with self.downloads_lock:
            self.downloads[download_id] = download_state

        return download_state

    def download_and_upload_sync(
        self, download_id: str, url: str, title: Optional[str]
    ):
        """Synchronous wrapper for download_and_upload to work with FastAPI BackgroundTasks"""
        # Direct call to synchronous method - no need for event loop
        self._download_and_upload_sync(download_id, url, title)

    async def start_download(
        self, url: str, title: Optional[str] = None
    ) -> DownloadState:
        """Start a new download"""
        download_id = str(uuid7())

        download_state = DownloadState(
            download_id=download_id,
            url=url,
            title=title or "Unknown",
            status="queued",
            created_at=datetime.now().isoformat(),
        )

        async with self.downloads_lock:
            self.downloads[download_id] = download_state

        # Start download in background without awaiting
        asyncio.create_task(self._download_and_upload(download_id, url, title))

        return download_state

    async def _download_and_upload(
        self, download_id: str, url: str, title: Optional[str]
    ):
        """Download video and upload to S3"""
        try:
            await self._update_download_state(
                download_id, status="downloading", progress=0.0
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                output_path = Path(temp_dir) / f"{download_id}.%(ext)s"

                # Configure yt-dlp
                ydl_opts = {
                    "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                    "outtmpl": str(output_path),
                    "progress_hooks": [self.DownloadProgress(self, download_id)],
                    "no_warnings": True,
                }

                # Download video
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if not title:
                        title = info.get("title", "Unknown")

                    await self._update_download_state(download_id, title=title)
                    ydl.download([url])

                # Find downloaded file
                downloaded_files = list(Path(temp_dir).glob(f"{download_id}.*"))
                if not downloaded_files:
                    raise Exception("No file was downloaded")

                downloaded_file = downloaded_files[0]
                file_extension = downloaded_file.suffix
                s3_object_name = f"{download_id}{file_extension}"

                # Generate and upload thumbnail
                thumbnail_path = Path(temp_dir) / f"{download_id}.thumbnail.jpg"
                if self._generate_thumbnail(str(downloaded_file), str(thumbnail_path)):
                    self.s3_service.upload_file(
                        str(thumbnail_path),
                        f"{download_id}.thumbnail.jpg",
                        "image/jpeg",
                        title,
                        url,
                    )

                # Upload video
                await self._update_download_state(
                    download_id, status="uploading", progress=100.0
                )

                if not self.s3_service.upload_file(
                    str(downloaded_file),
                    s3_object_name,
                    f"video/{file_extension[1:]}" if file_extension else "video/mp4",
                    title,
                    url,
                ):
                    raise Exception("Failed to upload video")

                # Update final status
                await self._update_download_state(
                    download_id,
                    status="completed",
                    progress=100.0,
                    s3_object_name=s3_object_name,
                    file_size=downloaded_file.stat().st_size,
                    encoding_status="queued",
                )

                # Queue for encoding
                await self.encoding_queue.put((download_id, s3_object_name))

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            await self._update_download_state(download_id, status="error", error=str(e))

    async def get_all_downloads(self) -> List[DownloadResponse]:
        """Get all downloads from both memory and S3"""
        all_downloads: Dict[str, DownloadResponse] = {}

        # Get completed downloads from S3
        s3_objects = self.s3_service.get_all_download_objects()

        for download_id, obj_info in s3_objects.items():
            if obj_info.original:
                metadata = self.s3_service.get_object_metadata(obj_info.original)

                # Generate presigned URLs
                original_url = (
                    self.s3_service.get_presigned_url(obj_info.original)
                    if obj_info.original
                    else None
                )
                thumbnail_url = (
                    self.s3_service.get_presigned_url(obj_info.thumbnail)
                    if obj_info.thumbnail
                    else None
                )
                mp3_url = (
                    self.s3_service.get_presigned_url(obj_info.mp3)
                    if obj_info.mp3
                    else None
                )
                webm_url = (
                    self.s3_service.get_presigned_url(obj_info.webm)
                    if obj_info.webm
                    else None
                )

                encoding_status = (
                    "completed" if obj_info.mp3 and obj_info.webm else None
                )

                all_downloads[download_id] = DownloadResponse(
                    download_id=download_id,
                    url=metadata.get("url", "Unknown"),
                    title=metadata.get("title", "Unknown"),
                    status="completed",
                    created_at=metadata.get("created_at", "Unknown"),
                    progress=100.0,
                    file_size=None,  # Could get from S3 stat if needed
                    downloaded_size=None,
                    s3_object_name=obj_info.original,
                    thumbnail_url=thumbnail_url,
                    encoding_status=encoding_status,
                    original_video_url=original_url,
                    mp3_url=mp3_url,
                    webm_url=webm_url,
                )

        # Get in-memory downloads and overwrite/add to the list
        async with self.downloads_lock:
            for download_id, download_state in self.downloads.items():
                # Generate presigned URLs for in-memory downloads
                original_url = None
                thumbnail_url = None
                mp3_url = None
                webm_url = None

                if download_state.s3_object_name:
                    original_url = self.s3_service.get_presigned_url(
                        download_state.s3_object_name
                    )

                if download_state.status == "completed":
                    thumbnail_url = self.s3_service.get_presigned_url(
                        f"{download_id}.thumbnail.jpg"
                    )

                if download_state.s3_mp3_object_name:
                    mp3_url = self.s3_service.get_presigned_url(
                        download_state.s3_mp3_object_name
                    )

                if download_state.s3_webm_object_name:
                    webm_url = self.s3_service.get_presigned_url(
                        download_state.s3_webm_object_name
                    )

                all_downloads[download_id] = download_state.to_response(
                    thumbnail_url=thumbnail_url,
                    original_video_url=original_url,
                    mp3_url=mp3_url,
                    webm_url=webm_url,
                )

        # Sort by created_at descending
        download_list = sorted(
            all_downloads.values(), key=lambda d: d.created_at, reverse=True
        )
        return download_list

    async def delete_download(self, download_id: str) -> bool:
        """Delete a download and its associated files"""
        # Remove from memory
        async with self.downloads_lock:
            if download_id in self.downloads:
                del self.downloads[download_id]

        # Delete from S3
        objects_to_delete = []
        s3_objects = self.s3_service.get_all_download_objects()

        if download_id in s3_objects:
            obj_info = s3_objects[download_id]
            if obj_info.original:
                objects_to_delete.append(obj_info.original)
            if obj_info.thumbnail:
                objects_to_delete.append(obj_info.thumbnail)
            if obj_info.mp3:
                objects_to_delete.append(obj_info.mp3)
            if obj_info.webm:
                objects_to_delete.append(obj_info.webm)

        if objects_to_delete:
            return self.s3_service.delete_objects(objects_to_delete)

        return True

    async def scan_and_encode_missing_videos(self) -> int:
        """Scan S3 for videos missing encoded versions and queue them for encoding"""
        s3_objects = self.s3_service.get_all_download_objects()
        queued_count = 0

        for download_id, obj_info in s3_objects.items():
            if obj_info.original and not (obj_info.mp3 and obj_info.webm):
                # Check if already being processed
                async with self.downloads_lock:
                    if download_id not in self.downloads:
                        # Create a download record for tracking
                        download_state = DownloadState(
                            download_id=download_id,
                            url="N/A (from S3 scan)",
                            title="N/A (from S3 scan)",
                            status="completed",
                            created_at=datetime.now().isoformat(),
                            progress=100.0,
                            s3_object_name=obj_info.original,
                            encoding_status="queued",
                        )
                        self.downloads[download_id] = download_state

                        await self.encoding_queue.put((download_id, obj_info.original))
                        queued_count += 1

        return queued_count

    class DownloadProgressSync:
        """Synchronous progress callback for yt-dlp when running in BackgroundTasks"""

        def __init__(self, video_service: "VideoService", download_id: str):
            self.video_service = video_service
            self.download_id = download_id

        def __call__(self, d):
            if d["status"] == "downloading":
                total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate")
                downloaded_bytes = d.get("downloaded_bytes", 0)

                if total_bytes:
                    progress = (downloaded_bytes / total_bytes) * 100
                else:
                    progress = 0.0

                self._update_progress(
                    progress=progress,
                    file_size=int(total_bytes) if total_bytes is not None else None,
                    downloaded_size=downloaded_bytes,
                    speed=d.get("speed_str"),
                    eta=d.get("eta_str"),
                )
            elif d["status"] == "finished":
                self._update_progress(progress=100.0, status="uploading")
            elif d["status"] == "error":
                self._update_progress(
                    status="error", error=str(d.get("error", "Unknown error"))
                )

        def _update_progress(self, **kwargs):
            self.video_service._update_download_state_sync(self.download_id, **kwargs)

    def _download_and_upload_sync(
        self, download_id: str, url: str, title: Optional[str]
    ):
        """Synchronous version of download and upload for BackgroundTasks"""
        try:
            self._update_download_state_sync(
                download_id, status="downloading", progress=0.0
            )

            with tempfile.TemporaryDirectory() as temp_dir:
                output_path = Path(temp_dir) / f"{download_id}.%(ext)s"

                # Configure yt-dlp
                ydl_opts = {
                    "format": "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best",
                    "outtmpl": str(output_path),
                    "progress_hooks": [self.DownloadProgressSync(self, download_id)],
                    "no_warnings": True,
                }

                # Download video
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                    if not title:
                        title = info.get("title", "Unknown")

                    self._update_download_state_sync(download_id, title=title)
                    ydl.download([url])

                # Find downloaded file
                downloaded_files = list(Path(temp_dir).glob(f"{download_id}.*"))
                if not downloaded_files:
                    raise Exception("No file was downloaded")

                downloaded_file = downloaded_files[0]
                file_extension = downloaded_file.suffix
                s3_object_name = f"{download_id}{file_extension}"

                # Generate and upload thumbnail
                thumbnail_path = Path(temp_dir) / f"{download_id}.thumbnail.jpg"
                if self._generate_thumbnail(str(downloaded_file), str(thumbnail_path)):
                    self.s3_service.upload_file(
                        str(thumbnail_path),
                        f"{download_id}.thumbnail.jpg",
                        "image/jpeg",
                        title,
                        url,
                    )

                # Upload video
                self._update_download_state_sync(
                    download_id, status="uploading", progress=100.0
                )

                if not self.s3_service.upload_file(
                    str(downloaded_file),
                    s3_object_name,
                    f"video/{file_extension[1:]}" if file_extension else "video/mp4",
                    title,
                    url,
                ):
                    raise Exception("Failed to upload video")

                # Update final status
                self._update_download_state_sync(
                    download_id,
                    status="completed",
                    progress=100.0,
                    s3_object_name=s3_object_name,
                    file_size=downloaded_file.stat().st_size,
                    encoding_status="queued",
                )

                # Queue for encoding - need to use thread-safe method
                import asyncio

                def add_to_queue():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    try:
                        loop.run_until_complete(
                            self.encoding_queue.put((download_id, s3_object_name))
                        )
                    finally:
                        loop.close()

                add_to_queue()

        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")
            self._update_download_state_sync(download_id, status="error", error=str(e))
