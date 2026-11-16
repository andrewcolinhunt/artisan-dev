"""Check an inert transport header before loading a trusted live payload."""

from __future__ import annotations

import hashlib
import importlib.metadata
import importlib.util
import json
import platform
import sys
from pathlib import Path


def runtime_identity(packages: list[str]) -> dict[str, str]:
    """Read compatibility metadata without importing payload classes."""
    return {
        "python": platform.python_version(),
        **{name: importlib.metadata.version(name) for name in packages},
    }


def framework_digest() -> str:
    """Identify framework source even when a static package version is unchanged."""
    specification = importlib.util.find_spec("artisan")
    if specification is None or specification.origin is None:
        msg = "prepared worker has no Artisan package"
        raise RuntimeError(msg)
    root = Path(specification.origin).parent
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*.py")):
        digest.update(str(path.relative_to(root)).encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def application_digest() -> str:
    """Bind the exact application schema source independently of its version."""
    return hashlib.sha256(
        Path(__file__).with_name("application_schema.py").read_bytes()
    ).hexdigest()


def target_identity(
    kind: str,
    identity: str | dict[str, str],
    *,
    scheduler: str | None = None,
    sdk: str | None = None,
    builder: str | None = None,
) -> dict[str, object]:
    """Describe the observed target without asserting equivalence across targets."""
    return {
        "kind": kind,
        "identity": identity,
        "platform": platform.machine(),
        "libc": ":".join(platform.libc_ver()),
        "pixi": "0.83.0",
        "scheduler": scheduler,
        "sdk": sdk,
        "builder": builder,
    }


def software_descriptor(
    header: dict[str, object], target: dict[str, object]
) -> dict[str, object]:
    """Construct one closed candidate software identity from qualified evidence."""
    expected = {
        "kind",
        "identity",
        "platform",
        "libc",
        "pixi",
        "scheduler",
        "sdk",
        "builder",
    }
    if set(target) != expected:
        msg = "target identity has missing or unknown fields"
        raise ValueError(msg)
    return {
        "schema": "provider-qualification-software-v1",
        "runtime": header["runtime"],
        "framework_digest": header["framework_digest"],
        "application_digest": header["application_digest"],
        "lock_digest": header["lock_digest"],
        "selected_tools": sorted(header["selected_tools"]),
        "target": target,
    }


def software_digest(descriptor: dict[str, object]) -> str:
    """Digest the closed descriptor, including the physical provider target."""
    return hashlib.sha256(
        json.dumps(descriptor, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def execute(header: dict[str, object], payload: bytes) -> dict[str, object]:
    """Reject incompatible payloads before invoking cloudpickle's loader."""
    expected = header["runtime"]
    if not isinstance(expected, dict):
        msg = "runtime must be an inert version mapping"
        raise TypeError(msg)
    actual = runtime_identity([name for name in expected if name != "python"])
    if actual != expected:
        msg = f"runtime identity mismatch: {actual!r} != {expected!r}"
        raise ValueError(msg)
    if header.get("framework_digest") != framework_digest():
        msg = "framework source identity mismatch"
        raise ValueError(msg)
    if header.get("application_digest") != application_digest():
        msg = "application schema identity mismatch"
        raise ValueError(msg)
    prepared = Path(str(header["prepared"]))
    receipt = json.loads((prepared / "receipt.json").read_text())
    lock_digest = hashlib.sha256((prepared / "pixi.lock").read_bytes()).hexdigest()
    if header["lock_digest"] != lock_digest or receipt["lock_digest"] != lock_digest:
        msg = "prepared lock identity mismatch"
        raise ValueError(msg)
    if not set(header["selected_tools"]).issubset(receipt["tools"]):
        msg = "selected tool is not prepared"
        raise ValueError(msg)
    import cloudpickle

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    operation, arguments = cloudpickle.loads(payload)
    return operation(**arguments)


def main() -> None:
    """Run one file-backed assignment in the prepared worker interpreter."""
    header_path, payload_path, result_path = map(Path, sys.argv[1:])
    result = execute(json.loads(header_path.read_text()), payload_path.read_bytes())
    result_path.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
