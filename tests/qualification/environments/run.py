"""Prepare tiny source packages and verify pure lookup and installed execution."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
from pathlib import Path
from unittest.mock import patch

from artisan.execution.environments.native import (
    lookup_native_environment,
    prepare_native_environment,
)
from artisan.execution.environments.native_target import (
    native_executable,
    verify_native_environment,
)
from artisan.execution.environments.pixi import pixi_executable
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.operation_config.environment import Environment


def _assert_miss(declaration: Environment, cache: Path) -> None:
    try:
        lookup_native_environment(declaration, cache_root=cache)
    except ValueError:
        return
    msg = "Changed software was accepted"
    raise AssertionError(msg)


def _check_lookup(
    declaration: Environment,
    cache: Path,
    target: PreparedNativeEnvironment,
) -> list[str]:
    source = Path(str(declaration.manifest)).parent
    changed = [
        source / "src/native_probe/__init__.py",
        source / "helper/src/qualification_helper/__init__.py",
        source / "assets/factor.json",
        source / "runtime/config.json",
        source / "activate.sh",
        source / ".gitignore",
    ]
    rejected = []
    with (
        patch("subprocess.run", side_effect=AssertionError("lookup subprocess")),
        patch("subprocess.Popen", side_effect=AssertionError("lookup subprocess")),
    ):
        assert lookup_native_environment(declaration, cache_root=cache) == target
        generated = source / "src/native_probe/__pycache__"
        generated.mkdir()
        (generated / "generated.pyc").write_bytes(b"interpreter cache")
        assert lookup_native_environment(declaration, cache_root=cache) == target
        for path in changed:
            previous = path.read_bytes()
            path.write_bytes(previous + b"\nchanged")
            try:
                _assert_miss(declaration, cache)
                rejected.append(path.name)
            finally:
                path.write_bytes(previous)
        for root, name in (
            (source, "src/native_probe/new.py"),
            (source, "src/native_probe/private-added.env"),
            (source, ".hgignore"),
            (source, "LICENSE"),
        ):
            path = root / name
            path.write_text("synthetic input")
            try:
                _assert_miss(declaration, cache)
                rejected.append(name)
            finally:
                path.unlink()
        assert lookup_native_environment(declaration, cache_root=cache) == target
    return rejected


def _run_installed(
    target: PreparedNativeEnvironment,
    scratch: Path,
) -> tuple[list[dict[str, object]], dict[str, object]]:
    command = [
        pixi_executable(),
        "run",
        "--no-config",
        "--offline",
        "--as-is",
        "--executable",
        "--manifest-path",
        str(target.workspace / target.source.manifest_name),
        "--environment",
        target.source.name,
    ]
    environment = {"PATH": "/usr/bin:/bin", "LC_ALL": "C.UTF-8", "TMPDIR": str(scratch)}
    executable = native_executable(target, "ProbeCLI")
    values = [
        json.loads(
            subprocess.check_output(
                [*command, str(executable), str(value)],
                cwd=scratch,
                env=environment,
                text=True,
                timeout=30,
            )
        )
        for value in (6, 4)
    ]
    script = (
        "import json,os,native_probe,importlib.metadata as m; "
        "print(json.dumps({'module':native_probe.__file__,'cwd':os.getcwd(),"
        "'versions':{n:m.version(n) for n in "
        "('qualification-native-probe','qualification-helper','packaging')},"
        "'editable':json.loads(m.distribution('qualification-native-probe').read_text("
        "'direct_url.json')).get('dir_info',{}).get('editable',False)}))"
    )
    interpreter = native_executable(target, "python")
    runtime = json.loads(
        subprocess.check_output(
            [*command, str(interpreter), "-I", "-c", script],
            cwd=scratch,
            env=environment,
            text=True,
            timeout=30,
        )
    )
    assert values == [
        {"value": 18, "activation": "qualified"},
        {"value": 12, "activation": "qualified"},
    ]
    assert runtime["editable"] is False
    assert Path(runtime["module"]).is_relative_to(target.prefix)
    assert runtime["cwd"] == str(scratch)
    return values, runtime


def qualify(work_root: Path) -> None:
    """Create a disposable project and leave its evidence for inspection."""
    work_root.mkdir(parents=True, exist_ok=False)
    source = work_root / "source"
    shutil.copytree(
        Path(__file__).parent / "source",
        source,
        ignore=shutil.ignore_patterns("__pycache__", ".pixi"),
    )
    (source / ".env").write_text("SYNTHETIC_UNRELATED=excluded\n")
    (source / "src/native_probe/private.env").write_text("SYNTHETIC_EXCLUDED=unused\n")
    declaration = Environment.from_pixi(source / "pyproject.toml")
    original = {
        name: hashlib.sha256((source / name).read_bytes()).hexdigest()
        for name in ("pyproject.toml", "pixi.lock")
    }
    cache = work_root / "cache"
    with patch.dict(os.environ, {"PIP_TARGET": str(work_root / "unintended-install")}):
        target = prepare_native_environment(declaration, cache_root=cache)
    assert not (work_root / "unintended-install").exists()
    assert len(target.wheels) == 2
    assert not (target.workspace / ".env").exists()
    assert not (target.workspace / "src/native_probe/private.env").exists()
    (work_root / "binding.json").write_text(target.model_dump_json(indent=2))
    rejected = _check_lookup(declaration, cache, target)
    scratch = work_root / "scratch"
    scratch.mkdir()
    unavailable = work_root / "unavailable-source"
    source.rename(unavailable)
    try:
        values, runtime = _run_installed(target, scratch)
    finally:
        unavailable.rename(source)
    verify_native_environment(target)
    assert original == {
        name: hashlib.sha256((source / name).read_bytes()).hexdigest()
        for name in original
    }
    report = {
        "status": "passed",
        "target_digest": target.digest,
        "numeric_results": values,
        "runtime": runtime,
        "lookup_subprocesses_forbidden": True,
        "drift_rejections": rejected,
        "source_unavailable_during_launch": True,
        "canonical_manifest_lock_unchanged": True,
        "prepared_target_available_after_launch": True,
        "excluded_inputs_not_copied": True,
        "pip_target_override_ignored": True,
        "local_wheels": [item.model_dump(mode="json") for item in target.wheels],
    }
    (work_root / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main() -> None:
    """Accept a new task-owned directory for the disposable preparation."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--work-root", type=Path, required=True)
    args = parser.parse_args()
    qualify(args.work_root.resolve())


if __name__ == "__main__":
    main()
