"""Apptainer identity includes only the configured, resolved host payload."""

from __future__ import annotations

import hashlib
import struct
import subprocess
from dataclasses import dataclass, field
from pathlib import Path

import pytest

from artisan.execution.environments import apptainer_gpu, container_runtime
from artisan.schemas.execution.worker_transport import PayloadFile


def _write_elf(path: Path, *, machine: int = 62, content: bytes = b"") -> Path:
    identity = b"\x7fELF\x02\x01\x01" + bytes(9)
    header = struct.pack(
        "<16sHHIQQQIHHHHHH", identity, 3, machine, 1, 0, 0, 0, 0, 64, 56, 0, 64, 0, 0
    )
    path.write_bytes(header + content)
    path.chmod(0o755)
    return path


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _files(payload: tuple[PayloadFile, ...]) -> dict[str, str]:
    return {item.path: item.sha256 for item in payload}


@dataclass
class _Installation:
    executable: Path
    config: Path
    nvlist: Path
    binaries: Path
    libraries: Path
    buildcfg: str
    ldconfig: Path
    cache: list[str] = field(default_factory=list)
    commands: list[list[str]] = field(default_factory=list)

    def capture(self, *, nvidia: bool = True) -> tuple[PayloadFile, ...]:
        return apptainer_gpu.capture_apptainer_payload(self.executable, nvidia=nvidia)

    def add_library(self, name: str, *, machine: int = 62) -> Path:
        path = _write_elf(self.libraries / name, machine=machine, content=name.encode())
        self.cache.append(f"\t{name} (libc6,x86-64) => {path}")
        return path


def _helper_files(installation: _Installation) -> dict[str, str]:
    helpers = installation.binaries.parent / "libexec/apptainer/bin"
    return {
        name: _digest(helpers / name)
        for name in ("mksquashfs", "unsquashfs", "starter")
    }


@pytest.fixture
def installation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Installation:
    binaries = tmp_path / "bin"
    libraries = tmp_path / "lib"
    configuration = tmp_path / "etc"
    plugins = tmp_path / "plugins"
    inherited = tmp_path / "inherited"
    for directory in (binaries, libraries, configuration, plugins, inherited):
        directory.mkdir()
    executable = _write_elf(binaries / "apptainer")
    _write_elf(binaries / "ldconfig", content=b"resolver")
    helpers = tmp_path / "libexec/apptainer/bin"
    helpers.mkdir(parents=True)
    for name in ("mksquashfs", "unsquashfs", "starter"):
        _write_elf(helpers / name, content=name.encode())
    config = configuration / "apptainer.conf"
    config.write_text(
        f"binary path = {binaries}\nuse nvidia-container-cli = no\nalways use nv = no\n"
    )
    nvlist = configuration / "nvliblist.conf"
    nvlist.write_text("")
    buildcfg = "\n".join(
        (
            f"APPTAINER_CONF_FILE={config}",
            f"APPTAINER_CONFDIR={configuration}",
            f"LIBEXECDIR={tmp_path / 'libexec'}",
            f"PLUGIN_ROOTDIR={plugins}",
            "APPTAINER_SUID_INSTALL=0",
        )
    )
    installed = _Installation(
        executable, config, nvlist, binaries, libraries, buildcfg, binaries / "ldconfig"
    )

    def query(command: list[str], **kwargs: object) -> str:
        installed.commands.append(command)
        if command == [str(executable), "--version"]:
            assert kwargs == {"text": True, "timeout": 10}
            return "apptainer version 1.5.4"
        assert kwargs == {
            "text": True,
            "env": {"PATH": str(binaries), "LANG": "C.UTF-8"},
            "timeout": 10,
        }
        if command == [str(executable), "buildcfg"]:
            return installed.buildcfg
        assert command == [str(installed.ldconfig), "-p"]
        return "\n".join(installed.cache)

    monkeypatch.setattr(apptainer_gpu.subprocess, "check_output", query)
    monkeypatch.setattr(apptainer_gpu, "_LAUNCH_PATH", str(binaries))
    monkeypatch.setattr(apptainer_gpu, "_INHERITED_LIBRARIES", inherited)
    return installed


def test_capture_apptainer_payload_cpu_hashes_config_without_gpu_probe(
    installation: _Installation,
) -> None:
    installation.nvlist.unlink()
    assert _files(installation.capture(nvidia=False)) == {
        "config/apptainer.conf": _digest(installation.config),
        **_helper_files(installation),
    }
    assert installation.commands == [[str(installation.executable), "buildcfg"]]


def test_capture_apptainer_payload_hashes_resolved_libraries_binary_and_file(
    installation: _Installation,
) -> None:
    library = installation.add_library("libcuda.so.1")
    binary = _write_elf(installation.binaries / "nvidia-smi", content=b"management")
    driver_config = installation.libraries / "nvidia.json"
    driver_config.write_text('{"name":"synthetic-driver"}')
    installation.nvlist.write_text(
        f"# Selected payload\nlibcuda.so\nnvidia-smi\n{driver_config}\n"
    )

    payload = installation.capture()
    assert [item.path for item in payload] == sorted({item.path for item in payload})
    assert _files(payload) == {
        "config/apptainer.conf": _digest(installation.config),
        **_helper_files(installation),
        "gpu/config/nvliblist.conf": _digest(installation.nvlist),
        "gpu/resolver/ldconfig": _digest(installation.binaries / "ldconfig"),
        "gpu/rootfs/.singularity.d/libs/libcuda.so.1": _digest(library),
        "gpu/rootfs/usr/bin/nvidia-smi": _digest(binary),
        f"gpu/rootfs/{str(driver_config).lstrip('/')}": _digest(driver_config),
    }
    assert installation.commands == [
        [str(installation.executable), "buildcfg"],
        [str(installation.binaries / "ldconfig"), "-p"],
    ]


def test_capture_apptainer_payload_ignores_unselected_host_library_changes(
    installation: _Installation,
) -> None:
    selected = installation.add_library("libcuda.so.1")
    unselected = installation.add_library("libunrelated.so.1")
    installation.nvlist.write_text("libcuda.so\n")
    original = installation.capture()

    _write_elf(unselected, content=b"unrelated replacement")
    assert installation.capture() == original
    _write_elf(selected, content=b"selected replacement")
    assert installation.capture() != original


@pytest.mark.parametrize("name", ["apptainer.conf", "nvliblist.conf"])
def test_capture_apptainer_payload_detects_config_byte_changes(
    installation: _Installation, name: str
) -> None:
    original = installation.capture()
    config = installation.config if name == "apptainer.conf" else installation.nvlist
    config.write_text(config.read_text() + "# changed configuration bytes\n")
    assert installation.capture() != original


def test_capture_apptainer_payload_filters_library_architecture_and_missing_names(
    installation: _Installation,
) -> None:
    selected = installation.add_library("libcuda.so.1")
    wrong_machine = installation.add_library("libnvidia-other.so.1", machine=183)
    installation.nvlist.write_text(
        "libcuda.so\nlibnvidia-other.so\nlibmissing.so\nmissing-tool\n"
    )

    files = _files(installation.capture())
    assert files["gpu/rootfs/.singularity.d/libs/libcuda.so.1"] == _digest(selected)
    assert _digest(wrong_machine) not in files.values()
    assert not any("missing" in name for name in files)


def test_capture_apptainer_payload_keeps_first_cache_path_for_same_soname(
    installation: _Installation,
) -> None:
    selected = installation.add_library("libcuda.so.1")
    duplicate = _write_elf(installation.binaries / "libcuda.so.1", content=b"later")
    installation.cache.append(f"\tlibcuda.so.1 (libc6,x86-64) => {duplicate}")
    installation.nvlist.write_text("libcuda.so\n")

    files = _files(installation.capture())
    assert files["gpu/rootfs/.singularity.d/libs/libcuda.so.1"] == _digest(selected)
    assert _digest(duplicate) not in files.values()


def test_capture_apptainer_payload_hashes_absolute_library_symlink_aliases(
    installation: _Installation,
) -> None:
    library = _write_elf(installation.libraries / "libcuda.so.600", content=b"driver")
    alias = installation.libraries / "libcuda.so.1"
    alias.symlink_to(library.name)
    installation.nvlist.write_text(f"{library}\n")

    files = _files(installation.capture())
    assert files["gpu/rootfs/.singularity.d/libs/libcuda.so.1"] == _digest(library)
    assert files["gpu/rootfs/.singularity.d/libs/libcuda.so.600"] == _digest(library)


def test_capture_apptainer_payload_rejects_alternative_gpu_backend(
    installation: _Installation,
) -> None:
    installation.config.write_text("use nvidia-container-cli = yes\n")
    with pytest.raises(ValueError, match=r"nvidia-container-cli|legacy"):
        installation.capture()


def test_capture_apptainer_payload_propagates_bounded_query_timeout(
    installation: _Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    def timeout(command: list[str], **kwargs: object) -> str:
        assert kwargs["timeout"] == 10
        raise subprocess.TimeoutExpired(command, 10)

    monkeypatch.setattr(apptainer_gpu.subprocess, "check_output", timeout)
    with pytest.raises(subprocess.TimeoutExpired):
        installation.capture()


def test_capture_apptainer_payload_rejects_installed_plugins(
    installation: _Installation,
) -> None:
    plugin = installation.executable.parent.parent / "plugins" / "plugin.meta"
    plugin.write_text('{"name":"synthetic-plugin","Enabled":true}')
    with pytest.raises(ValueError, match="plugin"):
        installation.capture()


def test_capture_apptainer_payload_rejects_inherited_host_libraries(
    installation: _Installation,
) -> None:
    inherited = apptainer_gpu._INHERITED_LIBRARIES / "libcuda.so.1"
    _write_elf(inherited, content=b"outer-container-driver")
    with pytest.raises(ValueError, match=r"inherited|nested|library|libraries"):
        installation.capture()


def test_capture_apptainer_payload_allows_disabled_plugins(
    installation: _Installation,
) -> None:
    original = installation.capture()
    plugin = installation.executable.parent.parent / "plugins" / "plugin.meta"
    plugin.write_text('{"name":"synthetic-plugin","Enabled":false}')
    assert installation.capture() == original


@pytest.mark.parametrize(
    ("directive", "value"),
    [("always use nv", "yes"), ("always use rocm", "yes"), ("user bind control", "no")],
)
def test_capture_apptainer_payload_rejects_implicit_or_disabled_binds(
    installation: _Installation, directive: str, value: str
) -> None:
    installation.config.write_text(f"{directive} = {value}\n")
    with pytest.raises(ValueError, match=directive):
        installation.capture(nvidia=False)


def test_capture_apptainer_payload_does_not_fallback_from_wrong_cache_architecture(
    installation: _Installation,
) -> None:
    wrong_machine = installation.add_library("libcuda.so.1", machine=183)
    compatible = _write_elf(installation.binaries / "libcuda.so.1", content=b"later")
    installation.cache.append(f"\tlibcuda.so.1 (libc6,x86-64) => {compatible}")
    installation.nvlist.write_text("libcuda.so\n")

    files = _files(installation.capture())
    assert "gpu/rootfs/.singularity.d/libs/libcuda.so.1" not in files
    assert {_digest(wrong_machine), _digest(compatible)}.isdisjoint(files.values())


def test_capture_apptainer_payload_detects_reselected_cache_source(
    installation: _Installation,
) -> None:
    installation.add_library("libcuda.so.1")
    installation.nvlist.write_text("libcuda.so\n")
    original = installation.capture()
    replacement = _write_elf(
        installation.binaries / "libcuda.so.1", content=b"replacement-driver"
    )
    installation.cache[:] = [f"\tlibcuda.so.1 (libc6,x86-64) => {replacement}"]

    changed = installation.capture()
    assert changed != original
    assert _files(changed)["gpu/rootfs/.singularity.d/libs/libcuda.so.1"] == _digest(
        replacement
    )


def test_capture_apptainer_payload_detects_selected_absolute_file_appearing(
    installation: _Installation,
) -> None:
    manifest = installation.libraries / "nvidia.json"
    installation.nvlist.write_text(f"{manifest}\n")
    original = installation.capture()
    destination = f"gpu/rootfs/{str(manifest).lstrip('/')}"
    assert destination not in _files(original)

    manifest.write_text('{"name":"new-driver-manifest"}')
    changed = installation.capture()
    assert changed != original
    assert _files(changed)[destination] == _digest(manifest)


def test_capture_apptainer_payload_detects_selected_ldconfig_byte_changes(
    installation: _Installation,
) -> None:
    original = installation.capture()
    _write_elf(installation.ldconfig, content=b"replacement-resolver")
    changed = installation.capture()
    assert changed != original
    assert _files(changed)["gpu/resolver/ldconfig"] == _digest(installation.ldconfig)


def test_capture_apptainer_payload_rejects_distinct_sources_at_same_destination(
    installation: _Installation,
) -> None:
    first = installation.add_library("libcuda.so.1")
    second = _write_elf(installation.binaries / first.name, content=b"other-driver")
    installation.nvlist.write_text(f"{first}\n{second}\n")
    with pytest.raises(ValueError, match="same container path"):
        installation.capture()


def test_capture_apptainer_payload_prefers_ldconfig_real_before_directory_order(
    installation: _Installation,
) -> None:
    later = installation.binaries.parent / "later-bin"
    later.mkdir()
    installation.ldconfig = _write_elf(
        later / "ldconfig.real", content=b"real-resolver"
    )
    installation.config.write_text(f"binary path = {installation.binaries}:{later}\n")

    payload = installation.capture()
    assert installation.commands[-1] == [str(installation.ldconfig), "-p"]
    assert _files(payload)["gpu/resolver/ldconfig"] == _digest(installation.ldconfig)


def test_capture_apptainer_payload_finds_nvlist_binaries_in_original_launch_path(
    installation: _Installation,
) -> None:
    helper_bin = installation.binaries.parent / "helpers"
    helper_bin.mkdir()
    installation.ldconfig = _write_elf(helper_bin / "ldconfig", content=b"helper")
    launch_binary = _write_elf(installation.binaries / "nvidia-smi", content=b"launch")
    _write_elf(helper_bin / "nvidia-smi", content=b"unselected-helper")
    _write_elf(helper_bin / "helper-only", content=b"unselected-command")
    installation.config.write_text(f"binary path = {helper_bin}\n")
    installation.nvlist.write_text("nvidia-smi\nhelper-only\n")

    files = _files(installation.capture())
    assert files["gpu/resolver/ldconfig"] == _digest(installation.ldconfig)
    assert files["gpu/rootfs/usr/bin/nvidia-smi"] == _digest(launch_binary)
    assert "gpu/rootfs/usr/bin/helper-only" not in files


@pytest.mark.parametrize("name", ["mksquashfs", "unsquashfs"])
def test_apptainer_identity_tracks_selected_helper_with_unchanged_config(
    installation: _Installation, name: str
) -> None:
    selected_directory = installation.binaries.parent / "preferred-bin"
    selected_directory.mkdir()
    selected = _write_elf(selected_directory / name, content=b"selected-helper")
    installation.config.write_text(f"binary path = {selected_directory}:$PATH:\n")
    runtime = container_runtime.ApptainerRuntime(installation.executable)
    original = runtime.identity()
    assert _files(original.helpers)[name] == _digest(selected)

    fallback = installation.binaries.parent / "libexec/apptainer/bin" / name
    _write_elf(fallback, content=b"unselected-fallback-change")
    assert runtime.identity() == original
    _write_elf(selected, content=b"selected-helper-change")
    assert runtime.identity() != original


def test_apptainer_identity_tracks_buildcfg_selected_starter(
    installation: _Installation,
) -> None:
    original_helpers = installation.binaries.parent / "libexec/apptainer/bin"
    selected_libexec = installation.binaries.parent / "relocated-libexec"
    selected_helpers = selected_libexec / "apptainer/bin"
    selected_helpers.mkdir(parents=True)
    selected = _write_elf(selected_helpers / "starter", content=b"selected-starter")
    installation.buildcfg = installation.buildcfg.replace(
        f"LIBEXECDIR={installation.binaries.parent / 'libexec'}",
        f"LIBEXECDIR={selected_libexec}",
    )
    installation.config.write_text(f"binary path = {original_helpers}\n")
    runtime = container_runtime.ApptainerRuntime(installation.executable)
    original = runtime.identity()
    assert _files(original.helpers)["starter"] == _digest(selected)

    _write_elf(original_helpers / "starter", content=b"unselected-starter-change")
    assert runtime.identity() == original
    _write_elf(selected, content=b"selected-starter-change")
    assert runtime.identity() != original


def test_apptainer_identity_tracks_optional_overlay_helper_appearing(
    installation: _Installation,
) -> None:
    runtime = container_runtime.ApptainerRuntime(installation.executable)
    original = runtime.identity()
    assert "fuse-overlayfs" not in _files(original.helpers)
    selected = _write_elf(
        installation.binaries / "fuse-overlayfs", content=b"overlay-helper"
    )
    changed = runtime.identity()
    assert changed != original
    assert _files(changed.helpers)["fuse-overlayfs"] == _digest(selected)


def test_apptainer_identity_tracks_selected_overlay_helper_bytes(
    installation: _Installation,
) -> None:
    preferred = installation.binaries.parent / "preferred-bin"
    preferred.mkdir()
    selected = _write_elf(preferred / "fuse-overlayfs", content=b"selected-overlay")
    fallback = _write_elf(
        installation.binaries.parent / "libexec/apptainer/bin/fuse-overlayfs",
        content=b"fallback-overlay",
    )
    installation.config.write_text(f"binary path = {preferred}:$PATH:\n")
    runtime = container_runtime.ApptainerRuntime(installation.executable)
    original = runtime.identity()
    assert _files(original.helpers)["fuse-overlayfs"] == _digest(selected)

    _write_elf(fallback, content=b"unselected-overlay-change")
    assert runtime.identity() == original
    _write_elf(selected, content=b"selected-overlay-change")
    assert runtime.identity() != original
