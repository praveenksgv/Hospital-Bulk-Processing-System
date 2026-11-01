import asyncio
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from uuid import uuid4

import httpx
import pytest

from app.errors import UpstreamError
from app.models import HospitalInput
from app.upstream import DirectoryClient, retry_after_seconds


@pytest.mark.parametrize("fault", ["connect", "connect_timeout", "pool_timeout", 429])
async def test_safe_retry_then_success(settings, directory, fault):
    directory.failures["A"] = [fault, fault]
    client = DirectoryClient(settings, httpx.MockTransport(directory))
    try:
        result = await client.create(HospitalInput(name="A", address="B"), uuid4())
        assert result.name == "A"
        assert directory.creates["A"] == 3
        assert len(directory.records) == 1
    finally:
        await client.close()


@pytest.mark.parametrize(
    "fault", ["read_timeout", "write_timeout", "read_error", 500, 502, 503, 408]
)
async def test_uncertain_creation_never_blindly_retried(settings, directory, fault):
    directory.failures["A"] = [fault]
    client = DirectoryClient(settings, httpx.MockTransport(directory))
    try:
        with pytest.raises(UpstreamError) as caught:
            await client.create(HospitalInput(name="A", address="B"), uuid4())
        assert caught.value.uncertain
        assert directory.creates["A"] == 1
    finally:
        await client.close()


@pytest.mark.parametrize("status", [400, 401, 403, 422])
async def test_permanent_rejection_not_retried(settings, directory, status):
    directory.failures["A"] = [status]
    client = DirectoryClient(settings, httpx.MockTransport(directory))
    try:
        with pytest.raises(UpstreamError) as caught:
            await client.create(HospitalInput(name="A", address="B"), uuid4())
        assert not caught.value.retryable
        assert not caught.value.uncertain
        assert directory.creates["A"] == 1
    finally:
        await client.close()


async def test_get_retries_and_missing_batch(settings, directory):
    directory.get_failures = [503, "timeout"]
    client = DirectoryClient(settings, httpx.MockTransport(directory))
    try:
        assert await client.list_batch(uuid4()) == []
        assert directory.gets == 3
    finally:
        await client.close()


async def test_total_deadline_caps_slow_stream(settings):
    async def slow(request):
        await asyncio.sleep(1)
        return httpx.Response(200, json=[])

    client = DirectoryClient(
        settings.model_copy(update={"request_timeout_seconds": 0.01, "retry_attempts": 1}),
        httpx.MockTransport(slow),
    )
    try:
        with pytest.raises(UpstreamError) as caught:
            await client.list_batch(uuid4())
        assert caught.value.status == 504
    finally:
        await client.close()


async def test_retry_after_does_not_exceed_batch_deadline(settings):
    count = 0

    async def limited(request):
        nonlocal count
        count += 1
        return httpx.Response(429, headers={"Retry-After": "100"})

    client = DirectoryClient(settings, httpx.MockTransport(limited))
    try:
        with pytest.raises(UpstreamError):
            await client.list_batch(uuid4(), deadline=time.monotonic() + 0.1)
        assert count == 1
    finally:
        await client.close()


def test_retry_after_parsing():
    assert retry_after_seconds("2") == 2
    assert retry_after_seconds("-2") == 0
    assert retry_after_seconds("nonsense") == 0
    assert retry_after_seconds(None) == 0
    value = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=10))
    assert 8 <= retry_after_seconds(value) <= 10


@pytest.mark.parametrize("data", [{}, [{"id": "invalid"}], [{"id": 1}]])
async def test_bad_batch_schemas(settings, data):
    client = DirectoryClient(
        settings, httpx.MockTransport(lambda request: httpx.Response(200, json=data))
    )
    try:
        with pytest.raises(UpstreamError) as caught:
            await client.list_batch(uuid4())
        assert caught.value.code == "invalid_upstream_batch"
    finally:
        await client.close()


async def test_response_size_limit(settings):
    client = DirectoryClient(
        settings.model_copy(update={"max_response_bytes": 10}),
        httpx.MockTransport(lambda request: httpx.Response(200, content=b"x" * 11)),
    )
    try:
        with pytest.raises(UpstreamError) as caught:
            await client.list_batch(uuid4())
        assert caught.value.code == "upstream_response_too_large"
    finally:
        await client.close()
