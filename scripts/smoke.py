"""Opt-in live smoke test. Creates synthetic records and deletes only its own batch."""

import argparse
import asyncio
import io
import json
import os
from uuid import uuid4

import httpx


async def main(base_url, upstream_url, cleanup):
    key = str(uuid4())
    name = "Assignment smoke {}".format(key)
    csv = "name,address,phone,ignored\n{},Synthetic test address,,demo\n".format(name)
    batch_id = None
    async with httpx.AsyncClient(timeout=1000) as api, httpx.AsyncClient(timeout=90) as upstream:
        task = asyncio.create_task(
            api.post(
                base_url.rstrip("/") + "/hospitals/bulk",
                headers={"Idempotency-Key": key},
                files={"file": ("smoke.csv", io.BytesIO(csv.encode()), "text/csv")},
            )
        )
        observations = []
        try:
            while not task.done():
                poll = await api.get(
                    base_url.rstrip("/") + "/hospitals/bulk/status",
                    headers={"Idempotency-Key": key},
                )
                if poll.status_code == 200:
                    state = poll.json()
                    batch_id = state["batch_id"]
                    observations.append(
                        {"status": state["status"], "progress": state["progress_percentage"]}
                    )
                elif poll.status_code != 404:
                    poll.raise_for_status()
                await asyncio.sleep(1)
            response = await task
            result = response.json()
            batch_id = result.get("batch_id", batch_id)
            response.raise_for_status()
            assert result["batch_activated"] and result["total_hospitals"] == 1, result
            check = await upstream.get(upstream_url.rstrip("/") + "/hospitals/batch/" + batch_id)
            check.raise_for_status()
            records = check.json()
            assert len(records) == 1 and records[0]["name"] == name and records[0]["active"], (
                records
            )
            print(
                json.dumps(
                    {"base_url": base_url, "result": result, "polling_observations": observations},
                    indent=2,
                )
            )
        finally:
            # A client-side interruption must not race deletion against a still-running job.
            try:
                response = await task
                batch_id = response.json().get("batch_id", batch_id)
            except (httpx.HTTPError, ValueError):
                pass
            if cleanup and batch_id:
                batch_url = upstream_url.rstrip("/") + "/hospitals/batch/" + batch_id
                check = await upstream.get(batch_url)
                if check.status_code == 404:
                    print("Synthetic batch already absent.")
                else:
                    check.raise_for_status()
                    records = check.json()
                    if not all(
                        record["name"] == name and record["creation_batch_id"] == batch_id
                        for record in records
                    ):
                        raise RuntimeError("Cleanup refused: unexpected records in the test batch")
                    deleted = await upstream.delete(batch_url)
                    deleted.raise_for_status()
                    verified = await upstream.get(batch_url)
                    assert verified.status_code == 404 or verified.json() == [], verified.text
                    print("Synthetic batch cleanup verified.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default=os.environ.get("BULK_BASE_URL", "http://app:8000"))
    parser.add_argument("--upstream-url", default="https://hospital-directory.onrender.com")
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Delete and verify deletion of this test's synthetic batch",
    )
    args = parser.parse_args()
    asyncio.run(main(args.base_url, args.upstream_url, args.cleanup))
