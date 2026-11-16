"""Concurrent preparers share one complete target, including after a failed owner."""

from __future__ import annotations

import json
import os
import select
import subprocess
import sys
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from typing import Literal, cast
from unittest.mock import patch

import pytest

PreparationKind = Literal["native", "builder", "witness"]
pytestmark = pytest.mark.skipif(
    sys.platform != "linux", reason="Native preparation requires Linux"
)


def _source(root: Path, *, name: str = "source") -> None:
    source = root / name
    source.mkdir()
    (source / "pixi.toml").write_text(
        '[workspace]\nname="example"\nplatforms=["linux-64"]\n' + f"# {name}\n"
    )
    (source / "pixi.lock").write_text(
        json.dumps(
            {
                "version": 7,
                "environments": {"default": {"packages": {"linux-64": []}}},
                "packages": [],
            }
        )
    )


@contextmanager
def _process(
    root: Path, kind: PreparationKind, role: str, *, fail_owner: bool
) -> Iterator[subprocess.Popen[str]]:
    process = subprocess.Popen(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            str(root),
            kind,
            role,
            str(int(fail_owner)),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        yield process
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=10)


def _read_line(process: subprocess.Popen[str]) -> str:
    assert process.stdout is not None
    result = bytearray()
    deadline = monotonic() + 15
    while not result.endswith(b"\n"):
        remaining = max(0, deadline - monotonic())
        assert select.select([process.stdout], [], [], remaining)[0], (
            "Child did not reach its barrier"
        )
        character = os.read(process.stdout.fileno(), 1)
        assert character, "Child exited before reaching its barrier"
        result.extend(character)
    return result.decode().strip()


@pytest.mark.parametrize("kind", ["native", "builder", "witness"])
@pytest.mark.parametrize("fail_owner", [False, True])
def test_concurrent_preparation_reuses_or_retries_owner(
    tmp_path: Path, kind: PreparationKind, fail_owner: bool
) -> None:
    _source(tmp_path)
    with _process(tmp_path, kind, "owner", fail_owner=fail_owner) as owner:
        assert _read_line(owner) == "ready"
        assert _read_line(owner) == "working"
        with _process(tmp_path, kind, "contender", fail_owner=fail_owner) as contender:
            assert _read_line(contender) == "ready"
            if kind == "native" and not fail_owner:
                _assert_other_target_proceeds(tmp_path)
            blocked = False
            try:
                contender.wait(timeout=1)
            except subprocess.TimeoutExpired:
                blocked = True
            assert owner.stdin is not None
            owner.stdin.write("continue\n")
            owner.stdin.flush()
            owner_output, owner_error = owner.communicate(timeout=15)
            contender_output, contender_error = contender.communicate(timeout=15)
            assert contender.returncode == 0, contender_error
            assert owner.returncode == (1 if fail_owner else 0), owner_error
            assert blocked, "Contender returned before the owner finished preparation"
    if fail_owner:
        assert "Owner preparation failed" in owner_error
        assert (tmp_path / "contender.work").exists()
    else:
        assert json.loads(owner_output) == json.loads(contender_output)
        assert not (tmp_path / "contender.work").exists()
    assert (tmp_path / "owner.work").exists()


def _assert_other_target_proceeds(root: Path) -> None:
    _source(root, name="other-source")
    with _process(root, "native", "other", fail_owner=False) as other:
        output, error = other.communicate(timeout=15)
        assert other.returncode == 0, error
        assert json.loads(output.splitlines()[-1])["prefix"]
    assert (root / "other.work").exists()


def _child(root: Path, kind: PreparationKind, role: str, fail_owner: bool) -> None:
    from artisan.execution.environments import command_witness, native
    from artisan.schemas.execution.command_witness import PreparedCommandWitness
    from artisan.schemas.execution.prepared_environment import (
        EnvironmentSource,
        PreparedNativeEnvironment,
    )
    from artisan.schemas.execution.worker_transport import PayloadFile
    from artisan.schemas.operation_config.environment import Environment

    announced = False
    original_workspace = native._workspace
    original_manifest = native._builder_manifest
    original_recipe = command_witness._recipe_digest
    original_verify = command_witness.verify_command_witness

    def announce() -> None:
        nonlocal announced
        if not announced:
            print("ready", flush=True)
            announced = True

    def work() -> None:
        (root / f"{role}.work").touch(exist_ok=False)
        if role == "owner":
            print("working", flush=True)
            assert sys.stdin.readline() == "continue\n"

    def fail() -> None:
        if role == "owner" and fail_owner:
            message = "Owner preparation failed"
            raise RuntimeError(message)

    def workspace(
        cache_root: Path, source: EnvironmentSource, selected_platform: str
    ) -> Path:
        result = original_workspace(cache_root, source, selected_platform)
        announce()
        return result

    def manifest(selected_platform: str) -> bytes:
        result = original_manifest(selected_platform)
        announce()
        return result

    def recipe(source_digest: str) -> str:
        result = original_recipe(source_digest)
        announce()
        return result

    def install(path: Path, name: str, *, skip: tuple[str, ...] = ()) -> None:
        (path.parent / ".pixi/envs" / name).mkdir(parents=True)
        work()
        fail()

    def lock(arguments: list[str], path: Path) -> None:
        assert arguments == ["lock", "--no-config"]
        (path.parent / "pixi.lock").write_bytes(
            (root / "source/pixi.lock").read_bytes()
        )

    def compile_witness(
        builder: PreparedNativeEnvironment, build: Path
    ) -> tuple[str, tuple[PayloadFile, ...]]:
        del builder
        (build / "witness").write_bytes(b"synthetic command helper")
        work()
        return "synthetic compiler", ()

    def verify_witness(target: PreparedCommandWitness) -> None:
        original_verify(target)
        fail()

    target: PreparedNativeEnvironment | PreparedCommandWitness
    with ExitStack() as stack:
        for replacement in (
            patch.object(native, "pixi_version"),
            patch.object(native, "native_platform", return_value="linux-64"),
            patch.object(native, "_validate_manifest"),
            patch.object(native, "_install_locked", side_effect=install),
        ):
            stack.enter_context(replacement)
        if kind == "native":
            stack.enter_context(
                patch.object(native, "_workspace", side_effect=workspace)
            )
            source = "other-source" if role == "other" else "source"
            target = native.prepare_native_environment(
                Environment.from_pixi(root / source / "pixi.toml"),
                cache_root=root / "cache",
            )
        elif kind == "builder":
            stack.enter_context(
                patch.object(native, "_builder_manifest", side_effect=manifest)
            )
            stack.enter_context(patch.object(native, "_pixi", side_effect=lock))
            target = native._prepare_builder(root / "cache", "linux-64")
        else:
            stack.enter_context(
                patch.object(command_witness, "_architecture", return_value="x86_64")
            )
            stack.enter_context(
                patch.object(command_witness, "_recipe_digest", side_effect=recipe)
            )
            stack.enter_context(
                patch.object(
                    command_witness,
                    "prepare_native_environment",
                    return_value=SimpleNamespace(digest="a" * 64),
                )
            )
            stack.enter_context(
                patch.object(command_witness, "_compile", side_effect=compile_witness)
            )
            stack.enter_context(
                patch.object(
                    command_witness,
                    "verify_command_witness",
                    side_effect=verify_witness,
                )
            )
            target = command_witness.prepare_command_witness(root / "cache")
    print(target.model_dump_json(), flush=True)


if __name__ == "__main__":
    _child(
        Path(sys.argv[1]),
        cast(PreparationKind, sys.argv[2]),
        sys.argv[3],
        bool(int(sys.argv[4])),
    )
