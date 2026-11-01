"""Repeatable controlled I/O benchmark. Does not contact the public upstream."""

import asyncio
import json
import statistics
import time

import httpx

from app.config import Settings
from app.main import create_app
from tests.conftest import FakeDirectory, csv_bytes, upload


async def run(concurrency):
    fake = FakeDirectory()
    fake.delay = 0.05
    config = Settings(
        upstream_base_url="http://benchmark.invalid", upstream_concurrency=concurrency
    )
    app = create_app(config, transport=httpx.MockTransport(fake))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://bulk.test"
        ) as client:
            start = time.monotonic()
            result = await upload(client, csv_bytes(20))
            assert result.status_code == 200, result.text
            assert result.json()["batch_activated"]
            elapsed = time.monotonic() - start
    return elapsed, fake.peak


async def main():
    results = {}
    for concurrency in (1, 5):
        samples = [await run(concurrency) for _ in range(3)]
        results[str(concurrency)] = {
            "median_seconds": round(statistics.median(item[0] for item in samples), 3),
            "samples_seconds": [round(item[0], 3) for item in samples],
            "peak_upstream_concurrency": max(item[1] for item in samples),
        }
    results["speedup"] = round(results["1"]["median_seconds"] / results["5"]["median_seconds"], 2)
    print(
        json.dumps({"hospitals": 20, "simulated_delay_seconds": 0.05, "results": results}, indent=2)
    )


if __name__ == "__main__":
    asyncio.run(main())
