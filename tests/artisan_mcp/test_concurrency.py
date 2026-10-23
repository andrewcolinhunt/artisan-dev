"""Store reads leave the MCP event loop responsive without claiming cancellation."""

from __future__ import annotations

import asyncio
import json
import threading

import pytest
from fastmcp import Client

from artisan.orchestration.run_status import RunStatus


def _status(run_id: str) -> RunStatus:
    return RunStatus(
        pipeline_run_id=run_id,
        last_status=None,
        step_count=0,
        started_at=None,
        ended_at=None,
        steps=[],
    )


async def _request(client: Client, surface: str):
    if surface == "tool":
        result = await client.call_tool(
            "artisan_get_run_status", {"pipeline_run_id": "blocked"}
        )
        return result.structured_content
    if surface == "resource":
        result = await client.read_resource("artisan://runs/blocked")
        return json.loads(result[0].text)
    result = await client.get_prompt(
        "artisan/explain-run", {"pipeline_run_id": "blocked"}
    )
    text = result.messages[0].content.text
    start = text.index("--- BEGIN ARTISAN EVIDENCE:")
    return json.loads(text[start:].split("\n", 1)[1].split("\n--- END")[0])


@pytest.mark.parametrize("surface", ["tool", "resource", "prompt"])
def test_blocked_reader_allows_capabilities_and_independent_arguments(
    make_app, monkeypatch, tmp_path, surface
):
    import artisan.orchestration.run_status as readers

    async def check():
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()
        finished = threading.Event()
        timed_out = threading.Event()
        seen = []

        def read(root, pipeline_run_id, **kwargs):
            seen.append((root, pipeline_run_id, threading.get_ident()))
            if pipeline_run_id == "blocked":
                loop.call_soon_threadsafe(entered.set)
                try:
                    if not release.wait(timeout=10):
                        timed_out.set()
                finally:
                    finished.set()
            return _status(pipeline_run_id)

        monkeypatch.setattr(readers, "run_status", read)
        app = make_app(delta_root=tmp_path)
        async with Client(app) as client:
            pending = asyncio.create_task(_request(client, surface))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                capability = await asyncio.wait_for(
                    client.call_tool("artisan_capabilities"), timeout=3
                )
                independent = await asyncio.wait_for(
                    client.call_tool(
                        "artisan_get_run_status", {"pipeline_run_id": "independent"}
                    ),
                    timeout=3,
                )
                assert capability.structured_content["read_only"] is True
                assert (
                    independent.structured_content["pipeline_run_id"] == "independent"
                )
                assert not finished.is_set()
                assert not timed_out.is_set()
            finally:
                release.set()
                response = await asyncio.wait_for(pending, timeout=5)
            assert response["pipeline_run_id"] == "blocked"
            assert [(root, run) for root, run, _ in seen] == [
                (str(tmp_path), "blocked"),
                (str(tmp_path), "independent"),
            ]
            assert all(thread != threading.get_ident() for _, _, thread in seen)

    asyncio.run(check())


def test_client_cancellation_does_not_terminate_read_worker(
    make_app, monkeypatch, tmp_path
):
    import artisan.orchestration.run_status as readers

    async def check():
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        finished = asyncio.Event()
        release = threading.Event()
        timed_out = threading.Event()

        def read(root, pipeline_run_id, **kwargs):
            loop.call_soon_threadsafe(entered.set)
            try:
                if not release.wait(timeout=10):
                    timed_out.set()
                return _status(pipeline_run_id)
            finally:
                loop.call_soon_threadsafe(finished.set)

        monkeypatch.setattr(readers, "run_status", read)
        async with Client(make_app(delta_root=tmp_path)) as client:
            pending = asyncio.create_task(_request(client, "tool"))
            try:
                await asyncio.wait_for(entered.wait(), timeout=3)
                pending.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(pending, timeout=3)
                assert not finished.is_set()
                assert not release.is_set()
            finally:
                release.set()
                await asyncio.wait_for(finished.wait(), timeout=5)
                if not pending.done():
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
            assert not timed_out.is_set()

    asyncio.run(check())
