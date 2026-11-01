"""Owned worker tasks, safe resume and verified batch activation."""

import asyncio
import logging
import time
from typing import List

from app.config import Settings
from app.errors import UpstreamError
from app.jobs import SUCCESS, TERMINAL, Job, JobStore, Row
from app.models import UpstreamHospital
from app.upstream import DirectoryClient

logger = logging.getLogger(__name__)


class Processor:
    def __init__(self, settings: Settings, store: JobStore, directory: DirectoryClient):
        self.settings = settings
        self.store = store
        self.directory = directory
        self.workers: List[asyncio.Task] = []
        self.maintenance = None
        self.stopping = False

    async def start(self):
        self.workers = [
            asyncio.create_task(self.worker()) for _ in range(self.settings.batch_workers)
        ]
        self.maintenance = asyncio.create_task(self.maintain())

    def healthy(self):
        return (
            not self.stopping
            and bool(self.workers)
            and all(not worker.done() for worker in self.workers)
            and self.maintenance is not None
            and not self.maintenance.done()
        )

    async def close(self):
        self.stopping = True
        self.store.accepting = False
        try:
            await asyncio.wait_for(self.store.queue.join(), self.settings.shutdown_timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning("shutdown_drain_timeout")
        for task in self.workers:
            task.cancel()
        if self.maintenance:
            self.maintenance.cancel()
        await asyncio.gather(
            *self.workers, *([self.maintenance] if self.maintenance else []), return_exceptions=True
        )
        for job in self.store.jobs.values():
            if job.status not in TERMINAL:
                job.finish(
                    UpstreamError(
                        "shutdown", "Processing stopped during application shutdown.", status=503
                    )
                )
        await self.directory.close()

    async def maintain(self):
        while True:
            await asyncio.sleep(self.settings.maintenance_interval_seconds)
            self.store.expire_queued()
            self.store.expire()
            if not self.stopping:
                for index, worker in enumerate(self.workers):
                    if worker.done():
                        if not worker.cancelled():
                            logger.error("worker_restart exception=%r", worker.exception())
                        self.workers[index] = asyncio.create_task(self.worker())

    async def worker(self):
        while True:
            batch_id, attempt = await self.store.queue.get()
            try:
                job = self.store.jobs.get(batch_id)
                if job is None or job.status != "queued" or job.attempt != attempt:
                    continue
                if time.monotonic() - job.queued_mono >= self.settings.queue_timeout_seconds:
                    job.finish(
                        UpstreamError(
                            "queue_timeout", "The batch exceeded its queue wait limit.", status=503
                        )
                    )
                    continue
                await self.run_attempt(job)
            finally:
                self.store.queue.task_done()

    async def run_attempt(self, job: Job):
        job.started_mono = time.monotonic()
        job.status = "reconciling" if job.attempt > 1 else "processing"
        job.touch()
        deadline = job.started_mono + self.settings.batch_timeout_seconds
        logger.info("batch_started batch_id=%s attempt=%s", job.batch_id, job.attempt)
        try:
            await asyncio.wait_for(self.process(job, deadline), self.settings.batch_timeout_seconds)
            job.finish()
        except asyncio.TimeoutError:
            if job.activation_status == "in_progress":
                job.activation_status = "unknown"
            job.finish(
                UpstreamError(
                    "batch_timeout", "The processing attempt exceeded its deadline.", status=504
                )
            )
        except UpstreamError as exc:
            job.finish(exc)
        except asyncio.CancelledError:
            if job.activation_status == "in_progress":
                job.activation_status = "unknown"
            job.finish(
                UpstreamError(
                    "processing_interrupted", "The processing attempt was interrupted.", status=503
                )
            )
            raise
        except Exception:
            logger.exception("batch_internal_error batch_id=%s", job.batch_id)
            job.finish(
                UpstreamError(
                    "internal_error", "An unexpected processing error occurred.", status=500
                )
            )
        finally:
            logger.info(
                "batch_finished batch_id=%s status=%s duration=%s",
                job.batch_id,
                job.status,
                job.duration(),
            )

    async def process(self, job: Job, deadline: float):
        if job.attempt > 1:
            await self.reconcile(job, deadline, repeat_unknown=True)
            if any(row.status == "outcome_unknown" for row in job.rows):
                raise self.creation_error(job)
        job.status = "processing"
        job.touch()
        attempt = job.attempt
        job.row_tasks = [
            asyncio.create_task(self.create_row(job, row, attempt, deadline))
            for row in job.rows
            if row.status == "pending"
        ]
        try:
            # Row errors are recorded independently. Unexpected exceptions still require
            # settling every sibling before terminal state or a subsequent resume.
            results = await asyncio.gather(*job.row_tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
        finally:
            for task in job.row_tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*job.row_tasks, return_exceptions=True)
            job.row_tasks = []
        if any(row.status == "outcome_unknown" for row in job.rows):
            await self.reconcile(job, deadline, repeat_unknown=True)
        if not all(row.status in SUCCESS for row in job.rows):
            raise self.creation_error(job)
        await self.activate(job, deadline)

    def creation_error(self, job: Job) -> UpstreamError:
        errors = [row.error for row in job.rows if row.error]
        uncertain = any(row.status == "outcome_unknown" for row in job.rows)
        return UpstreamError(
            "creation_outcome_unknown" if uncertain else "hospital_creation_failed",
            "Some creation outcomes remain uncertain; no activation was requested."
            if uncertain
            else "One or more hospitals could not be created; the batch was not activated.",
            status=504 if any(error.status == 504 for error in errors) else 502,
        )

    async def create_row(self, job: Job, row: Row, attempt: int, deadline: float):
        row.status = "in_progress"
        row.sent = False
        job.touch()

        def on_send(sent):
            if job.attempt == attempt:
                row.sent = sent

        def on_attempt():
            if job.attempt == attempt:
                row.request_attempts += 1
                job.touch()

        try:
            hospital = await self.directory.create(
                row.record.hospital,
                job.batch_id,
                on_send=on_send,
                on_attempt=on_attempt,
                deadline=deadline,
            )
            if job.attempt != attempt:
                return
            row.hospital_id = hospital.id
            row.active = hospital.active
            row.status = "created"
            row.error = None
            row.retryable = False
            if hospital.active:
                job.recoverable = False
                row.error = UpstreamError(
                    "premature_activation", "The directory created a batch hospital as active."
                )
        except UpstreamError as exc:
            if job.attempt != attempt:
                return
            row.status = "outcome_unknown" if exc.uncertain else "failed"
            row.error = exc
            row.retryable = exc.retryable
        except asyncio.CancelledError:
            if job.attempt == attempt:
                row.status = "outcome_unknown" if row.sent else "not_attempted"
                row.error = UpstreamError(
                    "request_interrupted",
                    "The row request was interrupted.",
                    status=504,
                    uncertain=row.sent,
                )
            raise
        except Exception:
            if job.attempt == attempt:
                row.status = "outcome_unknown" if row.sent else "not_attempted"
                row.error = UpstreamError(
                    "row_internal_error",
                    "An unexpected row-processing error occurred.",
                    status=500,
                    uncertain=row.sent,
                )
            raise
        finally:
            if job.attempt == attempt:
                job.touch()

    def match_remote(self, job: Job, remote: List[UpstreamHospital]):
        if job.activation_status == "not_started" and any(hospital.active for hospital in remote):
            job.recoverable = False
            raise UpstreamError(
                "premature_activation",
                "The upstream batch became active before activation was requested.",
            )
        by_identity = {row.record.hospital.identity(): row for row in job.rows}
        seen = set()
        for hospital in remote:
            row = by_identity.get(hospital.identity())
            if (
                row is None
                or hospital.identity() in seen
                or (row.hospital_id is not None and row.hospital_id != hospital.id)
            ):
                job.recoverable = False
                raise UpstreamError(
                    "upstream_state_conflict",
                    "The upstream batch has unexpected, duplicate, or changed records.",
                )
            seen.add(hospital.identity())
        remote_ids = {hospital.id for hospital in remote}
        if any(
            row.hospital_id is not None and row.hospital_id not in remote_ids for row in job.rows
        ):
            job.recoverable = False
            raise UpstreamError(
                "upstream_state_conflict", "A previously confirmed hospital is missing upstream."
            )
        for hospital in remote:
            row = by_identity[hospital.identity()]
            row.hospital_id = hospital.id
            row.active = hospital.active
            row.status = "created"
            row.error = None
            row.retryable = False
        job.touch()

    async def reconcile(self, job: Job, deadline: float, repeat_unknown: bool):
        for observation in range(self.settings.retry_attempts):
            remote = await self.directory.list_batch(job.batch_id, deadline=deadline)
            self.match_remote(job, remote)
            if not repeat_unknown or not any(row.status == "outcome_unknown" for row in job.rows):
                return remote
            if observation + 1 < self.settings.retry_attempts:
                await asyncio.sleep(self.directory.backoff(observation))
        return remote

    async def activate(self, job: Job, deadline: float):
        if not job.recoverable:
            raise UpstreamError(
                "upstream_contract_violation", "A hospital was active before batch activation."
            )
        job.status = "activating"
        job.touch()
        remote = await self.directory.list_batch(job.batch_id, deadline=deadline)
        self.match_remote(job, remote)
        if len(remote) != len(job.rows):
            raise UpstreamError(
                "incomplete_upstream_batch",
                "The upstream batch is incomplete; activation was not requested.",
            )

        last_error = None
        for attempt in range(self.settings.retry_attempts):
            if all(hospital.active for hospital in remote):
                job.activated = True
                job.activation_status = "confirmed"
                for row in job.rows:
                    row.status = "created_and_activated"
                    row.active = True
                job.touch()
                return
            job.activation_status = "in_progress"
            job.touch()
            try:
                await self.directory.activate(job.batch_id, deadline=deadline)
                last_error = None
            except UpstreamError as exc:
                last_error = exc
                job.activation_status = "unknown" if exc.uncertain else "failed"
                # Permanent rejections are not retried; ambiguous outcomes still need verification.
                if not exc.retryable and not exc.uncertain:
                    raise
            try:
                remote = await self.directory.list_batch(job.batch_id, deadline=deadline)
                self.match_remote(job, remote)
                if len(remote) != len(job.rows):
                    raise UpstreamError(
                        "incomplete_upstream_batch", "The upstream batch changed during activation."
                    )
            except UpstreamError:
                job.activation_status = "unknown"
                raise
            if all(hospital.active for hospital in remote):
                job.activated = True
                job.activation_status = "confirmed"
                for row in job.rows:
                    row.status = "created_and_activated"
                    row.active = True
                job.touch()
                return
            job.activation_status = (
                "partial" if any(hospital.active for hospital in remote) else "failed"
            )
            job.touch()
            if attempt + 1 < self.settings.retry_attempts:
                delay = max(
                    self.directory.backoff(attempt), last_error.retry_after if last_error else 0
                )
                if time.monotonic() + delay >= deadline:
                    raise last_error or UpstreamError(
                        "activation_timeout",
                        "Activation recovery exceeded its deadline.",
                        status=504,
                    )
                await asyncio.sleep(delay)
        raise last_error or UpstreamError(
            "activation_not_confirmed", "The directory did not activate every hospital."
        )
