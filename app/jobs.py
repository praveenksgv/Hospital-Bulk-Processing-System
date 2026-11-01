"""Single-event-loop in-memory jobs, atomic admission, snapshots and retention."""

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID, uuid4

from app.config import Settings
from app.csv_upload import CSVRecord, ParsedCSV
from app.errors import APIError, UpstreamError
from app.models import BatchResult, HospitalResult

SUCCESS = {"created", "created_and_activated"}
FINISHED_ROWS = SUCCESS | {"failed", "outcome_unknown"}
TERMINAL = {"completed", "failed"}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Row:
    record: CSVRecord
    status: str = "pending"
    hospital_id: Optional[int] = None
    active: Optional[bool] = None
    error: Optional[UpstreamError] = None
    retryable: bool = False
    sent: bool = False
    request_attempts: int = 0

    def result(self) -> HospitalResult:
        return HospitalResult(
            row=self.record.row,
            hospital_id=self.hospital_id,
            name=self.record.hospital.name,
            status=self.status,
            error=self.error.body() if self.error else None,
        )


@dataclass
class Job:
    parsed: ParsedCSV
    fingerprint: str
    key: Optional[str]
    future: asyncio.Future
    batch_id: UUID = field(default_factory=uuid4)
    rows: List[Row] = field(default_factory=list)
    status: str = "queued"
    activation_status: str = "not_started"
    activated: bool = False
    attempt: int = 1
    revision: int = 1
    error: Optional[Dict[str, Any]] = None
    http_status: int = 200
    recoverable: bool = True
    elapsed: float = 0
    started_mono: Optional[float] = None
    queued_mono: float = field(default_factory=time.monotonic)
    finished_mono: Optional[float] = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    finished_at: Optional[datetime] = None
    row_tasks: List[asyncio.Task] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        self.rows = [Row(record) for record in self.parsed.records]

    def touch(self):
        self.revision += 1
        self.updated_at = utc_now()

    def duration(self):
        current = time.monotonic() - self.started_mono if self.started_mono is not None else 0
        return round(self.elapsed + current, 3)

    def result(self) -> Dict[str, Any]:
        result = BatchResult(
            batch_id=self.batch_id,
            total_hospitals=len(self.rows),
            processed_hospitals=sum(row.status in FINISHED_ROWS for row in self.rows),
            failed_hospitals=sum(row.status == "failed" for row in self.rows),
            processing_time_seconds=self.duration(),
            batch_activated=self.activated,
            hospitals=[row.result() for row in self.rows],
            error=self.error,
        )
        return result.model_dump(mode="json", exclude_none=True)

    def can_resume(self):
        if self.status != "failed" or not self.recoverable:
            return False
        return all(row.status in SUCCESS for row in self.rows) or any(
            row.retryable or row.status in {"outcome_unknown", "not_attempted"} for row in self.rows
        )

    def snapshot(self, instance_id: UUID, retention: float) -> Dict[str, Any]:
        # No awaits: all counters and records come from the same event-loop turn.
        result = self.result()
        result.update(
            instance_id=str(instance_id),
            status=self.status,
            attempt_number=self.attempt,
            revision=self.revision,
            input_rows=self.parsed.input_rows,
            duplicates_removed=self.parsed.input_rows - len(self.rows),
            duplicate_rows=self.parsed.summary().duplicate_rows,
            successful_hospitals=sum(row.status in SUCCESS for row in self.rows),
            unknown_hospitals=sum(row.status == "outcome_unknown" for row in self.rows),
            pending_hospitals=sum(row.status == "pending" for row in self.rows),
            in_progress_hospitals=sum(row.status == "in_progress" for row in self.rows),
            not_attempted_hospitals=sum(row.status == "not_attempted" for row in self.rows),
            progress_percentage=round(100 * result["processed_hospitals"] / len(self.rows), 2),
            activation_status=self.activation_status,
            can_resume=self.can_resume(),
            created_at=self.created_at.isoformat(),
            updated_at=self.updated_at.isoformat(),
            finished_at=self.finished_at.isoformat() if self.finished_at else None,
            expires_at=(self.finished_at + timedelta(seconds=retention)).isoformat()
            if self.finished_at
            else None,
            row_attempts={str(row.record.row): row.request_attempts for row in self.rows},
        )
        return result

    def finish(self, error: Optional[UpstreamError] = None):
        if self.status in TERMINAL:
            return
        if self.started_mono is not None:
            self.elapsed += time.monotonic() - self.started_mono
            self.started_mono = None
        for row in self.rows:
            if row.status == "pending":
                row.status = "not_attempted"
        self.status = "failed" if error else "completed"
        self.error = error.body() if error else None
        self.http_status = error.status if error else 200
        self.finished_at = utc_now()
        self.finished_mono = time.monotonic()
        self.touch()
        if not self.future.done():
            # Freeze the attempt's result; a later resume must not change old waiters' results.
            self.future.set_result((self.http_status, self.result()))


class JobStore:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.jobs: Dict[UUID, Job] = {}
        self.keys: Dict[str, UUID] = {}
        self.queue = asyncio.Queue(maxsize=settings.queue_capacity)
        self.admission_lock = asyncio.Lock()
        self.instance_id = uuid4()
        self.accepting = True

    def expire(self):
        now = time.monotonic()
        expired = [
            batch_id
            for batch_id, job in self.jobs.items()
            if job.status in TERMINAL
            and job.finished_mono is not None
            and now - job.finished_mono >= self.settings.retention_seconds
        ]
        for batch_id in expired:
            job = self.jobs.pop(batch_id)
            if job.key:
                self.keys.pop(job.key, None)

    def get(self, batch_id: UUID) -> Job:
        self.expire()
        try:
            return self.jobs[batch_id]
        except KeyError as exc:
            raise APIError(
                404,
                "batch_not_found",
                "The batch is unknown, expired, or was lost after a restart.",
            ) from exc

    def by_key(self, key: str) -> Job:
        self.expire()
        if key not in self.keys:
            raise APIError(
                404, "batch_not_admitted", "No admitted batch is currently registered for this key."
            )
        return self.get(self.keys[key])

    def capacity(self):
        if not self.accepting:
            raise APIError(503, "service_draining", "The service is not accepting new work.")
        if self.queue.full():
            raise APIError(503, "queue_full", "The processing queue is full. Retry later.")

    async def admit(self, parsed: ParsedCSV, key: Optional[str]) -> Job:
        fingerprint = parsed.fingerprint()
        async with self.admission_lock:
            self.expire()
            if key and key in self.keys:
                existing = self.jobs[self.keys[key]]
                if existing.fingerprint != fingerprint:
                    raise APIError(
                        409, "idempotency_conflict", "This key belongs to different hospital input."
                    )
                return existing
            self.capacity()
            if len(self.jobs) >= self.settings.max_jobs:
                raise APIError(
                    503, "registry_full", "The in-memory job registry is full. Retry later."
                )
            job = Job(parsed, fingerprint, key, asyncio.get_running_loop().create_future())
            # No awaits between reservation and queue insertion: cancellation cannot split admission.
            self.queue.put_nowait((job.batch_id, job.attempt))
            self.jobs[job.batch_id] = job
            if key:
                self.keys[key] = job.batch_id
            return job

    async def resume(self, batch_id: UUID) -> Job:
        async with self.admission_lock:
            job = self.get(batch_id)
            async with job.lock:
                if job.status == "completed":
                    return job
                if job.status not in TERMINAL:
                    raise APIError(409, "batch_running", "The batch already has an active attempt.")
                if not job.can_resume():
                    raise APIError(
                        409, "resume_unavailable", "The batch has no safe recovery action."
                    )
                self.capacity()
                job.attempt += 1
                job.future = asyncio.get_running_loop().create_future()
                job.queued_mono = time.monotonic()
                job.finished_at = None
                job.finished_mono = None
                job.status = "queued"
                job.error = None
                for row in job.rows:
                    if row.status == "not_attempted" or (row.status == "failed" and row.retryable):
                        row.status = "pending"
                        row.error = None
                        row.sent = False
                job.touch()
                self.queue.put_nowait((job.batch_id, job.attempt))
                return job

    def expire_queued(self):
        now = time.monotonic()
        for job in self.jobs.values():
            if (
                job.status == "queued"
                and now - job.queued_mono >= self.settings.queue_timeout_seconds
            ):
                job.finish(
                    UpstreamError(
                        "queue_timeout", "The batch exceeded its queue wait limit.", status=503
                    )
                )
        # Remove expired queue entries so their capacity is immediately reusable.
        retained = []
        while not self.queue.empty():
            item = self.queue.get_nowait()
            self.queue.task_done()
            job = self.jobs.get(item[0])
            if job and job.status == "queued" and job.attempt == item[1]:
                retained.append(item)
        for item in retained:
            self.queue.put_nowait(item)
