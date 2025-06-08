from typing import List, Optional
from pydantic import BaseModel, HttpUrl


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
    mp3_url: Optional[HttpUrl] = None
    webm_url: Optional[HttpUrl] = None


class DownloadListResponse(BaseModel):
    downloads: List[DownloadResponse]


class ScanResponse(BaseModel):
    message: str
    queued_count: int


class DownloadState(BaseModel):
    """Internal model for tracking download state"""

    download_id: str
    url: str
    title: str
    status: str  # "queued", "downloading", "uploading", "completed", "error"
    created_at: str
    progress: float = 0.0
    file_size: Optional[int] = None
    downloaded_size: Optional[int] = None
    speed: Optional[str] = None
    eta: Optional[str] = None
    error: Optional[str] = None
    s3_object_name: Optional[str] = None
    encoding_status: Optional[str] = (
        None  # "queued", "encoding", "mp3_completed", "completed", "error"
    )
    s3_mp3_object_name: Optional[str] = None
    s3_webm_object_name: Optional[str] = None

    def to_response(
        self,
        thumbnail_url: Optional[str] = None,
        original_video_url: Optional[str] = None,
        mp3_url: Optional[str] = None,
        webm_url: Optional[str] = None,
    ) -> DownloadResponse:
        """Convert to API response model"""
        return DownloadResponse(
            download_id=self.download_id,
            url=self.url,
            title=self.title,
            status=self.status,
            created_at=self.created_at,
            progress=self.progress,
            file_size=self.file_size,
            downloaded_size=self.downloaded_size,
            speed=self.speed,
            eta=self.eta,
            error=self.error,
            s3_object_name=self.s3_object_name,
            thumbnail_url=thumbnail_url,
            encoding_status=self.encoding_status,
            original_video_url=original_video_url,
            mp3_url=mp3_url,
            webm_url=webm_url,
        )


class S3ObjectInfo(BaseModel):
    """Information about S3 objects for a download"""

    download_id: str
    original: Optional[str] = None
    thumbnail: Optional[str] = None
    mp3: Optional[str] = None
    webm: Optional[str] = None
