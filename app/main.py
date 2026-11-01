"""FastAPI interface. POST waits for owned processing; polling stays responsive."""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, Union
from uuid import UUID

from fastapi import FastAPI, Header, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from python_multipart.exceptions import MultipartParseError
from starlette.concurrency import run_in_threadpool
from starlette.datastructures import UploadFile
from starlette.exceptions import HTTPException

from app.config import Settings
from app.csv_upload import parse_csv
from app.errors import APIError
from app.jobs import JobStore
from app.middleware import RequestMiddleware
from app.models import BatchResult, BatchStatus, ErrorResponse, ValidationResult
from app.processor import Processor
from app.upstream import DirectoryClient

logger = logging.getLogger(__name__)

UPLOAD_SCHEMA = {
    "requestBody": {
        "required": True,
        "content": {
            "multipart/form-data": {
                "schema": {
                    "type": "object",
                    "required": ["file"],
                    "additionalProperties": False,
                    "properties": {"file": {"type": "string", "format": "binary"}},
                }
            }
        },
    }
}
ERROR_RESPONSES = {
    code: {"model": ErrorResponse, "description": description}
    for code, description in {
        400: "Malformed CSV, encoding, or multipart upload",
        404: "Batch is not admitted, unknown, expired, or restart-lost",
        409: "Idempotency conflict or resume conflict",
        413: "Upload/request size limit exceeded",
        415: "Expected multipart/form-data",
        422: "CSV, header, row count, UUID, or key validation failed",
        503: "Admission capacity exhausted or service draining",
        500: "Unexpected application error",
    }.items()
}
BULK_RESPONSES = dict(ERROR_RESPONSES)
for code in (500, 502, 503, 504):
    BULK_RESPONSES[code] = {
        "model": BatchResult if code in (502, 504) else Union[BatchResult, ErrorResponse],
        "description": "Final batch failure after admission; includes batch fields and error. Before admission uses the error envelope.",
    }


def validate_key(value: Optional[str], required=False) -> Optional[str]:
    if value is None and not required:
        return None
    if (
        value is None
        or not 1 <= len(value) <= 128
        or any(not 33 <= ord(char) <= 126 for char in value)
    ):
        raise APIError(
            422,
            "invalid_idempotency_key",
            "Idempotency-Key must contain 1–128 visible ASCII characters without spaces.",
        )
    return value


def create_app(settings: Optional[Settings] = None, transport=None) -> FastAPI:
    config = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(application):
        store = JobStore(config)
        directory = DirectoryClient(config, transport=transport)
        processor = Processor(config, store, directory)
        application.state.store = store
        application.state.processor = processor
        await processor.start()
        try:
            yield
        finally:
            await processor.close()

    application = FastAPI(
        title="Hospital Bulk Processing API",
        version="1.0.0",
        description=(
            "Uploads are deduplicated and fully validated before processing. The bulk POST waits "
            "for the final result. Poll with the same Idempotency-Key from another request. "
            "State is in memory and is lost on restart."
        ),
        lifespan=lifespan,
    )
    application.state.settings = config
    application.add_middleware(RequestMiddleware, settings=config)

    @application.exception_handler(APIError)
    async def api_error(request, exc):
        return JSONResponse(
            exc.body(),
            status_code=exc.status,
            headers={"Retry-After": "5"} if exc.status == 503 else None,
        )

    @application.exception_handler(RequestValidationError)
    async def validation_error(request, exc):
        details = [
            {"field": ".".join(str(part) for part in error["loc"]), "message": error["msg"]}
            for error in exc.errors()
        ]
        return JSONResponse(
            APIError(
                422, "request_validation_failed", "The request contains invalid fields.", details
            ).body(),
            status_code=422,
        )

    @application.exception_handler(HTTPException)
    async def http_error(request, exc):
        return JSONResponse(
            APIError(exc.status_code, "http_error", str(exc.detail)).body(),
            status_code=exc.status_code,
            headers=exc.headers,
        )

    @application.exception_handler(Exception)
    async def unexpected_error(request, exc):
        logger.error(
            "request_internal_error request_id=%s", request.scope.get("request_id"), exc_info=exc
        )
        return JSONResponse(
            APIError(500, "internal_error", "An unexpected application error occurred.").body(),
            status_code=500,
        )

    async def upload(request: Request):
        form = None
        try:
            try:
                form = await request.form(max_files=1, max_fields=0)
            except MultipartParseError as exc:
                raise APIError(
                    400, "malformed_multipart", "The multipart upload could not be parsed."
                ) from exc
            except HTTPException as exc:
                if "Too many" in str(exc.detail):
                    raise APIError(
                        422, "invalid_upload_fields", "Provide exactly one file field named 'file'."
                    ) from exc
                raise APIError(
                    400, "malformed_multipart", "The multipart upload could not be parsed."
                ) from exc
            fields = form.multi_items()
            if (
                len(fields) != 1
                or fields[0][0] != "file"
                or not isinstance(fields[0][1], UploadFile)
            ):
                raise APIError(
                    422, "invalid_upload_fields", "Provide exactly one file field named 'file'."
                )
            contents = await fields[0][1].read(config.max_file_bytes + 1)
            return await run_in_threadpool(parse_csv, contents, config)
        finally:
            if form is not None:
                await form.close()
            release = request.scope.get("release_upload_slot")
            if release:
                release()

    async def final_response(job):
        # Shield only the attempt future. Cancellation still releases this request handler.
        # Worker lifetime belongs to application lifespan, never to a caller connection.
        status, result = await asyncio.shield(job.future)
        return JSONResponse(
            result,
            status_code=status,
            headers={
                "Location": "/hospitals/bulk/{}/status".format(job.batch_id),
                "Cache-Control": "no-store",
            },
        )

    @application.get("/health", tags=["health"], responses={503: {"model": ErrorResponse}})
    async def health(request: Request):
        if not request.app.state.processor.healthy():
            raise APIError(
                503, "service_unavailable", "The local processor is unavailable or draining."
            )
        return {"status": "ok", "instance_id": str(request.app.state.store.instance_id)}

    @application.post(
        "/hospitals/bulk",
        tags=["bulk"],
        response_model=BatchResult,
        response_model_exclude_none=True,
        responses=BULK_RESPONSES,
        openapi_extra=UPLOAD_SCHEMA,
    )
    async def bulk(request: Request, idempotency_key: Optional[str] = Header(default=None)):
        key = validate_key(idempotency_key)
        parsed = await upload(request)
        if not request.app.state.processor.healthy():
            raise APIError(503, "service_unavailable", "The local processor is unavailable.")
        job = await request.app.state.store.admit(parsed, key)
        return await final_response(job)

    @application.post(
        "/hospitals/bulk/validate",
        tags=["bulk"],
        response_model=ValidationResult,
        responses=ERROR_RESPONSES,
        openapi_extra=UPLOAD_SCHEMA,
    )
    async def validate(request: Request):
        return (await upload(request)).summary()

    @application.get(
        "/hospitals/bulk/status",
        tags=["bulk"],
        response_model=BatchStatus,
        response_model_exclude_none=True,
        responses=ERROR_RESPONSES,
    )
    async def status_by_key(
        request: Request, idempotency_key: Optional[str] = Header(default=None)
    ):
        key = validate_key(idempotency_key, required=True)
        store = request.app.state.store
        return JSONResponse(
            store.by_key(key).snapshot(store.instance_id, config.retention_seconds),
            headers={"Cache-Control": "no-store"},
        )

    @application.get(
        "/hospitals/bulk/{batch_id}/status",
        tags=["bulk"],
        response_model=BatchStatus,
        response_model_exclude_none=True,
        responses=ERROR_RESPONSES,
    )
    async def status_by_id(batch_id: UUID, request: Request):
        store = request.app.state.store
        return JSONResponse(
            store.get(batch_id).snapshot(store.instance_id, config.retention_seconds),
            headers={"Cache-Control": "no-store"},
        )

    @application.post(
        "/hospitals/bulk/{batch_id}/resume",
        tags=["bulk"],
        response_model=BatchResult,
        response_model_exclude_none=True,
        responses=BULK_RESPONSES,
    )
    async def resume(batch_id: UUID, request: Request):
        if not request.app.state.processor.healthy():
            raise APIError(503, "service_unavailable", "The local processor is unavailable.")
        job = await request.app.state.store.resume(batch_id)
        return await final_response(job)

    return application


app = create_app()
