"""Real sockets: upstream side effects outlive a timed-out caller."""

import asyncio
import json
from contextlib import asynccontextmanager

import httpx

from app.main import create_app
from tests.conftest import upload


@asynccontextmanager
async def delayed_directory(create_delay=0.06, activation_delay=0):
    records = []
    requests = {"POST": 0, "PATCH": 0}
    connections = set()

    async def handle(reader, writer):
        task = asyncio.current_task()
        connections.add(task)
        try:
            headers = await reader.readuntil(b"\r\n\r\n")
            lines = headers.decode("latin-1").split("\r\n")
            method, path, _ = lines[0].split(" ")
            length = next(
                (
                    int(line.split(":", 1)[1])
                    for line in lines[1:]
                    if line.lower().startswith("content-length:")
                ),
                0,
            )
            body = await reader.readexactly(length) if length else b""
            status = 200
            if method == "POST":
                requests["POST"] += 1
                # This handler belongs to the server, not the caller's HTTPX task.
                await asyncio.sleep(create_delay)
                record = dict(json.loads(body), id=len(records) + 1, active=False)
                records.append(record)
                response = record
            elif method == "PATCH":
                requests["PATCH"] += 1
                for record in records:
                    record["active"] = True
                await asyncio.sleep(activation_delay)
                response = {}
            else:
                batch_id = path.rsplit("/", 1)[-1]
                response = [record for record in records if record["creation_batch_id"] == batch_id]
                if not response:
                    status = 404
            payload = json.dumps(response).encode()
            writer.write(
                (
                    "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n".format(
                        status, len(payload)
                    )
                ).encode()
                + payload
            )
            await writer.drain()
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            connections.discard(task)

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield "http://127.0.0.1:{}".format(port), records, requests
    finally:
        server.close()
        await server.wait_closed()
        if connections:
            await asyncio.gather(*list(connections))


async def test_remote_creation_finishes_after_client_read_timeout(settings):
    async with delayed_directory() as (url, records, requests):
        config = settings.model_copy(
            update={
                "upstream_base_url": url,
                "read_timeout_seconds": 0.015,
                "retry_base_seconds": 0.04,
            }
        )
        app = create_app(config)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://bulk.test"
            ) as client:
                result = await upload(client)
                assert result.status_code == 200, result.text
                assert result.json()["batch_activated"]
        assert requests["POST"] == 1
        assert len(records) == 1
        assert records[0]["active"]


async def test_remote_activation_finishes_but_its_response_times_out(settings):
    async with delayed_directory(create_delay=0, activation_delay=0.06) as (url, records, requests):
        config = settings.model_copy(
            update={"upstream_base_url": url, "read_timeout_seconds": 0.015}
        )
        app = create_app(config)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://bulk.test"
            ) as client:
                result = await upload(client)
                assert result.status_code == 200, result.text
        assert requests == {"POST": 1, "PATCH": 1}
        assert records[0]["active"]
