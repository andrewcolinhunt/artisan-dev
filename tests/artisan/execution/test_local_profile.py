"""Launch profiles cross provider boundaries as closed JSON, never live objects."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from artisan.execution.environments.observed_worker import ObservedWorker
from artisan.execution.local_profile import LocalLauncherIdentity, LocalWorkerProfile
from artisan.schemas.execution.gpu import (
    GPUDriverIdentity,
    GPUDriverLibrary,
    NativeGPUAllocation,
)
from artisan.schemas.execution.prepared_environment import (
    EnvironmentFile,
    EnvironmentSource,
    PreparedNativeEnvironment,
)
from artisan.schemas.execution.worker_transport import (
    TRANSPORT_PACKAGES,
    DistributionMetadata,
    RuntimeCompatibility,
)


@pytest.fixture(params=["observed", "prepared"])
def profile(request: pytest.FixtureRequest, tmp_path: Path) -> LocalWorkerProfile:
    compatibility = RuntimeCompatibility(
        python_implementation="CPython",
        python_version="3.12.12",
        cache_tag="cpython-312",
        packages=tuple(
            DistributionMetadata(
                name=name,
                version="1",
            )
            for name in sorted(TRANSPORT_PACKAGES)
        ),
    )
    launcher = None
    worker: ObservedWorker | PreparedNativeEnvironment
    if request.param == "observed":
        worker = ObservedWorker(
            tmp_path / "bin/python",
            tmp_path,
            tmp_path,
            "a" * 64,
            compatibility,
            {"ACTIVATION": "literal $HOME ; value", "TOKEN": "synthetic-secret"},
        )
    else:
        worker = PreparedNativeEnvironment(
            source=EnvironmentSource(
                manifest_name="pixi.toml",
                name="worker",
                files=tuple(
                    EnvironmentFile(path=name, sha256="b" * 64)
                    for name in ("pixi.lock", "pixi.toml")
                ),
            ),
            platform="linux-64",
            compatibility=compatibility,
            workspace=tmp_path,
            prefix=tmp_path / ".pixi/envs/worker",
        )
        launcher = LocalLauncherIdentity(
            "pixi 0.83.0", "d" * 64, "e" * 64, ("glibc", "2.36")
        )
    gpu = NativeGPUAllocation(
        devices=("GPU-00000000-0000-0000-0000-000000000001",),
        driver=GPUDriverIdentity(
            driver_version="synthetic-driver",
            device_models=("synthetic-gpu",),
            libcuda_sha256="f" * 64,
            nvidia_smi_sha256="0" * 64,
        ),
        libraries=tuple(
            GPUDriverLibrary(name=name, path=tmp_path / name, sha256="f" * 64)
            for name in ("libcuda.so.1", "libnvidia-ptxjitcompiler.so.1")
        ),
        shell_sha256="1" * 64,
    )
    return LocalWorkerProfile(
        worker, tmp_path / "pixi", Path("/usr/bin/env"), launcher, gpu
    )


def test_profile_json_preserves_exact_activation_without_running_or_importing_code(
    profile: LocalWorkerProfile, monkeypatch: pytest.MonkeyPatch
) -> None:
    encoded = profile.to_json()
    for function in ("importlib.import_module", "subprocess.Popen"):
        monkeypatch.setattr(
            function, lambda *a, **kw: pytest.fail("Inert profile executed code")
        )
    decoded = LocalWorkerProfile.from_json(encoded)
    assert decoded == profile
    assert decoded.worker.digest == profile.worker.digest
    assert decoded.to_json() == encoded
    if isinstance(decoded.worker, ObservedWorker):
        assert decoded.sensitive_values == ("synthetic-secret",)
        with pytest.raises(TypeError):
            decoded.worker.environment["TOKEN"] = "changed"  # type: ignore[index]


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("domain",), "arbitrary.import.name"),
        (("schema_version",), True),
        (("schema_version",), "1"),
        (("schema_version",), 2),
        (("worker_kind",), "container"),
        (("module",), "arbitrary.import.name"),
        (("pixi",), "relative/pixi"),
        (("worker", "factory"), "arbitrary.import.name"),
        (("worker", "compatibility"), None),
        (("native_gpu", "driver", "nvidia_smi_sha256"), "invalid"),
        (("native_gpu", "libraries", 0, "path"), "relative/libcuda.so.1"),
    ],
)
def test_profile_rejects_unknown_or_unqualified_wire_values(
    profile: LocalWorkerProfile, path: tuple[str | int, ...], value: object
) -> None:
    payload: Any = json.loads(profile.to_json())
    selected = payload
    for key in path[:-1]:
        selected = selected[key]
    selected[path[-1]] = value
    with pytest.raises(ValueError):
        LocalWorkerProfile.from_json(json.dumps(payload))


def test_profile_rejects_mismatched_variant_and_launcher(
    profile: LocalWorkerProfile,
) -> None:
    payload = json.loads(profile.to_json())
    payload["worker_kind"] = "prepared" if profile.observed else "observed"
    with pytest.raises(ValueError, match="exact qualified activation"):
        LocalWorkerProfile.from_json(json.dumps(payload))
    payload = json.loads(profile.to_json())
    payload["launcher"] = (
        {
            "pixi_version": "pixi 0.83.0",
            "pixi_sha256": "d" * 64,
            "system_env_sha256": "e" * 64,
            "libc": ["glibc", "2.36"],
        }
        if profile.observed
        else None
    )
    with pytest.raises(ValueError, match="exact qualified activation"):
        LocalWorkerProfile.from_json(json.dumps(payload))


@pytest.mark.parametrize("missing", ["domain", "schema_version"])
def test_profile_requires_explicit_wire_domain_and_version(
    profile: LocalWorkerProfile, missing: str
) -> None:
    payload = json.loads(profile.to_json())
    del payload[missing]
    with pytest.raises(ValueError, match="Field required"):
        LocalWorkerProfile.from_json(json.dumps(payload))
