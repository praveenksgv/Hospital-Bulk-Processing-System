import asyncio
import json

import pytest
from pydantic import ValidationError

from app.config import Settings
from app.middleware import RequestMiddleware


def test_configuration_from_environment(monkeypatch):
    monkeypatch.setenv("UPSTREAM_CONCURRENCY", "3")
    assert Settings.from_env().upstream_concurrency == 3
    monkeypatch.setenv("MAX_HOSPITALS", "21")
    with pytest.raises(ValidationError):
        Settings.from_env()


def test_configuration_rejects_inconsistent_sizes():
    with pytest.raises(ValidationError):
        Settings(max_file_bytes=100, max_request_bytes=50)


async def exercise_middleware(settings, messages, headers=None, delay=0):
    sent, called = [], []

    async def app(scope, receive, send):
        called.append(True)
        await receive()
        scope["release_upload_slot"]()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    middleware = RequestMiddleware(app, settings)

    async def receive():
        if delay:
            await asyncio.sleep(delay)
        return messages.pop(0)

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/hospitals/bulk",
        "headers": headers or [(b"content-type", b"multipart/form-data; boundary=x")],
    }
    await middleware(scope, receive, send)
    assert middleware.uploads == 0
    return sent, called


async def test_chunked_body_limit_without_content_length(settings):
    config = settings.model_copy(update={"max_request_bytes": 10})
    sent, called = await exercise_middleware(
        config,
        [
            {"type": "http.request", "body": b"123456", "more_body": True},
            {"type": "http.request", "body": b"123456", "more_body": False},
        ],
    )
    assert not called
    assert sent[0]["status"] == 413


async def test_disconnect_before_admission(settings):
    sent, called = await exercise_middleware(settings, [{"type": "http.disconnect"}])
    assert not sent and not called


@pytest.mark.parametrize("length", [b"abc", b"-1"])
async def test_invalid_content_length(settings, length):
    sent, called = await exercise_middleware(
        settings, [], [(b"content-type", b"multipart/form-data"), (b"content-length", length)]
    )
    assert sent[0]["status"] == 400
    assert not called


async def test_slow_upload_has_total_deadline(settings):
    config = settings.model_copy(update={"upload_timeout_seconds": 0.01})
    sent, called = await exercise_middleware(
        config, [{"type": "http.request", "body": b"x"}], delay=0.1
    )
    assert sent[0]["status"] == 400
    assert json.loads(sent[1]["body"])["error"]["code"] == "upload_timeout"
    assert not called


async def test_upload_slots_are_bounded_and_released(settings):
    received = asyncio.Event()
    release = asyncio.Event()

    async def app(scope, receive, send):
        await receive()
        scope["release_upload_slot"]()

    middleware = RequestMiddleware(app, settings.model_copy(update={"upload_concurrency": 1}))
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/hospitals/bulk",
        "headers": [(b"content-type", b"multipart/form-data")],
    }

    async def slow_receive():
        received.set()
        await release.wait()
        return {"type": "http.request", "body": b"x", "more_body": False}

    sent = []

    async def send(message):
        sent.append(message)

    first = asyncio.create_task(middleware(dict(scope), slow_receive, send))
    await received.wait()
    await middleware(dict(scope), slow_receive, send)
    assert sent[0]["status"] == 503
    release.set()
    await first
    assert middleware.uploads == 0
