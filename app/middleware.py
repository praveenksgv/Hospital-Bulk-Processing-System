"""Bounded multipart admission before Starlette allocates upload files."""

import asyncio
import logging
import time
from uuid import uuid4

from starlette.responses import JSONResponse

from app.errors import APIError

logger = logging.getLogger(__name__)


class ClientGone(Exception):
    pass


class RequestMiddleware:
    def __init__(self, app, settings):
        self.app = app
        self.settings = settings
        self.uploads = 0

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        request_id = str(uuid4())
        scope["request_id"] = request_id
        started = time.monotonic()
        status = 0

        async def traced_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message = dict(message)
                message["headers"] = list(message.get("headers", [])) + [
                    (b"x-request-id", request_id.encode("ascii"))
                ]
            await send(message)

        upload = scope["method"] == "POST" and scope["path"].rstrip("/") in {
            "/hospitals/bulk",
            "/hospitals/bulk/validate",
        }
        acquired = False

        def release():
            nonlocal acquired
            if acquired:
                self.uploads -= 1
                acquired = False

        try:
            if not upload:
                return await self.app(scope, receive, traced_send)
            headers = dict(scope["headers"])
            content_type = headers.get(b"content-type", b"").split(b";", 1)[0].strip().lower()
            if content_type != b"multipart/form-data":
                raise APIError(
                    415,
                    "unsupported_media_type",
                    "Use multipart/form-data with one file field named 'file'.",
                )
            if self.uploads >= self.settings.upload_concurrency:
                raise APIError(503, "upload_capacity", "All upload parsers are busy. Retry later.")
            self.uploads += 1
            acquired = True
            scope["release_upload_slot"] = release
            if b"content-length" in headers:
                try:
                    length = int(headers[b"content-length"])
                except ValueError as exc:
                    raise APIError(
                        400, "invalid_content_length", "Content-Length must be an integer."
                    ) from exc
                if length < 0:
                    raise APIError(
                        400, "invalid_content_length", "Content-Length must not be negative."
                    )
                if length > self.settings.max_request_bytes:
                    raise APIError(
                        413, "request_too_large", "The multipart request exceeds the size limit."
                    )

            async def read_body():
                chunks, size = [], 0
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        raise ClientGone()
                    chunk = message.get("body", b"")
                    size += len(chunk)
                    if size > self.settings.max_request_bytes:
                        raise APIError(
                            413,
                            "request_too_large",
                            "The multipart request exceeds the size limit.",
                        )
                    chunks.append(chunk)
                    if not message.get("more_body", False):
                        return b"".join(chunks)

            body = await asyncio.wait_for(read_body(), self.settings.upload_timeout_seconds)
            replayed = False

            async def buffered_receive():
                nonlocal body, replayed
                if not replayed:
                    replayed = True
                    message = {"type": "http.request", "body": body, "more_body": False}
                    body = b""
                    return message
                return await receive()

            await self.app(scope, buffered_receive, traced_send)
        except ClientGone:
            logger.info("upload_disconnected request_id=%s", request_id)
        except asyncio.TimeoutError:
            await JSONResponse(
                APIError(400, "upload_timeout", "The upload exceeded its receive deadline.").body(),
                status_code=400,
            )(scope, receive, traced_send)
        except APIError as exc:
            await JSONResponse(
                exc.body(),
                status_code=exc.status,
                headers={"Retry-After": "5"} if exc.status == 503 else None,
            )(scope, receive, traced_send)
        finally:
            release()
            logger.info(
                "request_finished request_id=%s method=%s path=%s status=%s duration=%.3f",
                request_id,
                scope["method"],
                scope["path"],
                status,
                time.monotonic() - started,
            )
