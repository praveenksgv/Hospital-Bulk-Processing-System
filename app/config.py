"""Environment configuration; no host dependencies or external storage required."""

import os
from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    upstream_base_url: HttpUrl = "https://hospital-directory.onrender.com"
    max_hospitals: int = Field(default=20, ge=1, le=20)
    max_file_bytes: int = Field(default=1024 * 1024, gt=0)
    max_request_bytes: int = Field(default=2 * 1024 * 1024, gt=0)
    max_response_bytes: int = Field(default=8 * 1024 * 1024, gt=0)
    upload_concurrency: int = Field(default=4, gt=0)
    upload_timeout_seconds: float = Field(default=30, gt=0)
    batch_workers: int = Field(default=2, gt=0)
    upstream_concurrency: int = Field(default=5, gt=0)
    queue_capacity: int = Field(default=20, gt=0)
    max_jobs: int = Field(default=100, gt=0)
    retention_seconds: float = Field(default=3600, gt=0)
    queue_timeout_seconds: float = Field(default=300, gt=0)
    connect_timeout_seconds: float = Field(default=10, gt=0)
    read_timeout_seconds: float = Field(default=60, gt=0)
    write_timeout_seconds: float = Field(default=10, gt=0)
    pool_timeout_seconds: float = Field(default=10, gt=0)
    request_timeout_seconds: float = Field(default=90, gt=0)
    batch_timeout_seconds: float = Field(default=600, gt=0)
    shutdown_timeout_seconds: float = Field(default=30, gt=0)
    retry_attempts: int = Field(default=3, ge=1, le=3)
    retry_base_seconds: float = Field(default=1, ge=0)
    retry_jitter_seconds: float = Field(default=0.25, ge=0)
    maintenance_interval_seconds: float = Field(default=1, gt=0)

    @model_validator(mode="after")
    def consistent_limits(self) -> "Settings":
        if self.max_request_bytes <= self.max_file_bytes:
            raise ValueError("MAX_REQUEST_BYTES must exceed MAX_FILE_BYTES for multipart overhead")
        return self

    @classmethod
    def from_env(cls) -> "Settings":
        values: Dict[str, Any] = {
            name: os.environ[name.upper()]
            for name in cls.model_fields
            if name.upper() in os.environ
        }
        return cls.model_validate(values)
