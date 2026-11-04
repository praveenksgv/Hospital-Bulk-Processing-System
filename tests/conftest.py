import asyncio
import json
from collections import Counter
from contextlib import asynccontextmanager

import httpx
import pytest

from app.config import Settings
from app.main import create_app


class FakeDirectory:
    """Stateful deterministic upstream; ordinary tests never use the public API."""

    def __init__(self):
        self.records = []
        self.creates = Counter()
        self.activation_calls = 0
        self.gets = 0
        self.failures = {}
        self.activation_failures = []
        self.get_failures = []
        self.delay = 0
        self.gate = None
        self.entered = asyncio.Event()
        self.in_flight = 0
        self.peak = 0
        self.premature = False
        self.no_activation = False
        self.extra_batch = False

    def add(self, payload):
        record = dict(payload, id=len(self.records) + 1, active=self.premature)
        self.records.append(record)
        return record

    async def __call__(self, request):
        if request.method == "POST":
            payload = json.loads(request.content)
            self.creates[payload["name"]] += 1
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
            self.entered.set()
            try:
                if self.gate:
                    await self.gate.wait()
                if self.delay:
                    await asyncio.sleep(self.delay)
                failures = self.failures.get(payload["name"], [])
                failure = failures.pop(0) if failures else None
                if failure == "connect":
                    raise httpx.ConnectError("offline", request=request)
                if failure == "connect_timeout":
                    raise httpx.ConnectTimeout("timeout", request=request)
                if failure == "pool_timeout":
                    raise httpx.PoolTimeout("timeout", request=request)
                if failure == "read_timeout":
                    raise httpx.ReadTimeout("timeout", request=request)
                if failure == "write_timeout":
                    raise httpx.WriteTimeout("timeout", request=request)
                if failure == "read_error":
                    raise httpx.ReadError("connection lost", request=request)
                if failure == "lost_response":
                    self.add(payload)
                    raise httpx.ReadTimeout("response lost", request=request)
                if failure == "bad_json_after_create":
                    self.add(payload)
                    return httpx.Response(200, content=b"bad json")
                if failure == "bad_schema_after_create":
                    self.add(payload)
                    return httpx.Response(200, json={"id": "wrong"})
                if failure == "wrong_batch":
                    return httpx.Response(
                        200,
                        json=dict(
                            self.add(payload),
                            creation_batch_id="00000000-0000-4000-8000-000000000000",
                        ),
                    )
                if failure == "internal":
                    raise RuntimeError("unexpected fake failure")
                if isinstance(failure, int):
                    return httpx.Response(
                        failure, headers={"Retry-After": "0"}, json={"detail": "failure"}
                    )
                return httpx.Response(200, json=self.add(payload))
            finally:
                self.in_flight -= 1
        if request.method == "GET":
            self.gets += 1
            failure = self.get_failures.pop(0) if self.get_failures else None
            if failure == "timeout":
                raise httpx.ReadTimeout("GET timeout", request=request)
            if failure == "bad_json":
                return httpx.Response(200, content=b"bad")
            if failure == "bad_schema":
                return httpx.Response(200, json={})
            if isinstance(failure, int):
                return httpx.Response(failure)
            batch_id = request.url.path.rsplit("/", 1)[-1]
            records = [
                dict(record) for record in self.records if record["creation_batch_id"] == batch_id
            ]
            if self.extra_batch and records:
                records.append(dict(records[0], id=999, name="Unexpected"))
            return httpx.Response(200, json=records) if records else httpx.Response(404)
        if request.method == "PATCH":
            self.activation_calls += 1
            failure = self.activation_failures.pop(0) if self.activation_failures else None
            batch_id = request.url.path.split("/")[-2]
            if failure == "timeout_before":
                raise httpx.ReadTimeout("activation timeout", request=request)
            if failure == "connect":
                raise httpx.ConnectError("activation connection failed", request=request)
            if isinstance(failure, int):
                return httpx.Response(failure, headers={"Retry-After": "0"})
            if not self.no_activation:
                for record in self.records:
                    if record["creation_batch_id"] == batch_id:
                        record["active"] = True
            if failure == "lost_response":
                raise httpx.ReadTimeout("activation response lost", request=request)
            if failure == "verification_fails":
                self.get_failures = [503] * 3
            return httpx.Response(200, json={"activated": True})
        return httpx.Response(405)


@pytest.fixture
def settings():
    return Settings(
        upstream_base_url="http://directory.test",
        retry_base_seconds=0.001,
        retry_jitter_seconds=0,
        shutdown_timeout_seconds=0.1,
        maintenance_interval_seconds=0.01,
    )


@pytest.fixture
async def directory():
    return FakeDirectory()


@pytest.fixture
def app_client(settings, directory):
    @asynccontextmanager
    async def factory(**overrides):
        config = Settings.model_validate(dict(settings.model_dump(), **overrides))
        app = create_app(config, transport=httpx.MockTransport(directory))
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://bulk.test"
            ) as client:
                yield app, client

    return factory


def csv_bytes(count=1):
    return (
        "name,address,phone\n"
        + "".join("Hospital {},Address {},001{}\n".format(i, i, i) for i in range(count))
    ).encode()


async def upload(client, contents=None, key=None):
    return await client.post(
        "/hospitals/bulk",
        files={
            "file": ("hospitals.csv", contents if contents is not None else csv_bytes(), "text/csv")
        },
        headers={"Idempotency-Key": key} if key is not None else {},
    )


async def wait_until(predicate, timeout=2):
    async def poll():
        while not predicate():  # noqa: ASYNC110 - observes state that has no notification event
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout)
