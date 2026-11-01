"""Bounded asynchronous upstream calls with operation-aware retry classification."""

import asyncio
import json
import logging
import random
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, Callable, Dict, List, Optional
from uuid import UUID

import httpx
from pydantic import ValidationError

from app.config import Settings
from app.errors import UpstreamError
from app.models import HospitalInput, UpstreamHospital

logger = logging.getLogger(__name__)


def retry_after_seconds(value: Optional[str]) -> float:
    if not value:
        return 0
    try:
        return max(0, float(value))
    except ValueError:
        try:
            stamp = parsedate_to_datetime(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return max(0, (stamp - datetime.now(timezone.utc)).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return 0


class DirectoryClient:
    def __init__(self, settings: Settings, transport=None):
        self.settings = settings
        self.semaphore = asyncio.Semaphore(settings.upstream_concurrency)
        self.http = httpx.AsyncClient(
            base_url=str(settings.upstream_base_url).rstrip("/"),
            transport=transport,
            follow_redirects=False,
            timeout=httpx.Timeout(
                connect=settings.connect_timeout_seconds,
                read=settings.read_timeout_seconds,
                write=settings.write_timeout_seconds,
                pool=settings.pool_timeout_seconds,
            ),
            limits=httpx.Limits(
                max_connections=settings.upstream_concurrency,
                max_keepalive_connections=settings.upstream_concurrency,
            ),
            # Only explicit deployment configuration controls proxying.
            trust_env=False,
        )

    async def close(self):
        await self.http.aclose()

    def backoff(self, attempt: int) -> float:
        return self.settings.retry_base_seconds * (2**attempt) + random.uniform(
            0, self.settings.retry_jitter_seconds
        )

    async def _once(
        self,
        method: str,
        path: str,
        payload: Optional[Dict[str, Any]],
        on_send: Optional[Callable[[bool], None]],
    ):
        sent = False

        async def perform():
            nonlocal sent
            async with self.semaphore:
                sent = True
                if on_send:
                    on_send(True)
                async with self.http.stream(method, path, json=payload) as response:
                    if response.status_code == 404 and method == "GET":
                        return []
                    if response.status_code == 429:
                        if on_send:
                            on_send(False)
                        raise UpstreamError(
                            "upstream_rate_limited",
                            "The directory API rejected the request due to rate limiting.",
                            retryable=True,
                            retry_after=retry_after_seconds(response.headers.get("Retry-After")),
                        )
                    if not 200 <= response.status_code < 300:
                        uncertain = method != "GET" and (
                            response.status_code >= 500
                            or response.status_code == 408
                            or 300 <= response.status_code < 400
                        )
                        if on_send and not uncertain:
                            on_send(False)
                        raise UpstreamError(
                            "upstream_http_error",
                            "The directory API returned HTTP {}.".format(response.status_code),
                            retryable=method == "GET"
                            and (response.status_code >= 500 or response.status_code == 408),
                            uncertain=uncertain,
                        )
                    chunks, size = [], 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > self.settings.max_response_bytes:
                            raise UpstreamError(
                                "upstream_response_too_large",
                                "The directory response exceeded the configured size limit.",
                                uncertain=method != "GET",
                            )
                        chunks.append(chunk)
                    if method == "PATCH":
                        # Activation's response schema is unspecified; verify via GET.
                        return None
                    try:
                        return json.loads(b"".join(chunks))
                    except (ValueError, UnicodeDecodeError) as exc:
                        raise UpstreamError(
                            "invalid_upstream_response",
                            "The directory API returned invalid JSON.",
                            uncertain=method != "GET",
                        ) from exc

        try:
            return await asyncio.wait_for(perform(), self.settings.request_timeout_seconds)
        except (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            if on_send:
                on_send(False)
            raise UpstreamError(
                "upstream_connection_failed",
                "The directory request could not be sent.",
                status=504 if isinstance(exc, httpx.TimeoutException) else 502,
                retryable=True,
            ) from exc
        except (httpx.TimeoutException, asyncio.TimeoutError) as exc:
            raise UpstreamError(
                "upstream_timeout",
                "The directory request timed out.",
                status=504,
                retryable=method == "GET" or not sent,
                uncertain=method != "GET" and sent,
            ) from exc
        except httpx.RequestError as exc:
            raise UpstreamError(
                "upstream_transport_error",
                "The directory connection was interrupted.",
                retryable=method == "GET" or not sent,
                uncertain=method != "GET" and sent,
            ) from exc

    async def request(
        self,
        method: str,
        path: str,
        payload=None,
        on_send=None,
        on_attempt=None,
        deadline: Optional[float] = None,
        attempt_limit: Optional[int] = None,
    ):
        limit = attempt_limit or self.settings.retry_attempts
        for attempt in range(limit):
            if on_attempt:
                on_attempt()
            try:
                return await self._once(method, path, payload, on_send)
            except UpstreamError as exc:
                if not exc.retryable or attempt + 1 == limit:
                    raise
                delay = max(self.backoff(attempt), exc.retry_after)
                if deadline is not None and time.monotonic() + delay >= deadline:
                    raise
                logger.info(
                    "upstream_retry method=%s attempt=%s delay=%.3f code=%s",
                    method,
                    attempt + 1,
                    delay,
                    exc.code,
                )
                await asyncio.sleep(delay)

    async def create(self, hospital: HospitalInput, batch_id: UUID, **kwargs) -> UpstreamHospital:
        payload = dict(hospital.model_dump(), creation_batch_id=str(batch_id))
        data = await self.request("POST", "/hospitals/", payload, **kwargs)
        try:
            result = UpstreamHospital.model_validate(data)
        except ValidationError as exc:
            raise UpstreamError(
                "invalid_upstream_hospital",
                "The created hospital response is invalid.",
                uncertain=True,
            ) from exc
        if result.creation_batch_id != batch_id or result.identity() != hospital.identity():
            raise UpstreamError(
                "upstream_contract_violation",
                "The created hospital does not match the requested batch and values.",
                uncertain=True,
            )
        return result

    async def list_batch(self, batch_id: UUID, deadline=None) -> List[UpstreamHospital]:
        data = await self.request("GET", "/hospitals/batch/{}".format(batch_id), deadline=deadline)
        try:
            if not isinstance(data, list):
                raise ValueError("Expected an array")
            hospitals = [UpstreamHospital.model_validate(item) for item in data]
            if any(item.creation_batch_id != batch_id for item in hospitals):
                raise ValueError("Unexpected batch ID")
            if len({item.id for item in hospitals}) != len(hospitals):
                raise ValueError("Duplicate hospital IDs")
            return hospitals
        except (ValueError, ValidationError) as exc:
            raise UpstreamError(
                "invalid_upstream_batch", "The directory batch response is inconsistent or invalid."
            ) from exc

    async def activate(self, batch_id: UUID, deadline=None):
        await self.request(
            "PATCH",
            "/hospitals/batch/{}/activate".format(batch_id),
            deadline=deadline,
            # The processor owns activation retries so it can verify between writes.
            attempt_limit=1,
        )
