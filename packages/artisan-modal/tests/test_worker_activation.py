"""Startup activation precedes SDK import and preserves placement authority."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from artisan_modal import _worker_entrypoint as entry
from artisan_modal import preparation, worker
from artisan_modal.bindings import PreparedModalImage

from artisan.orchestration.runner_api import Environment


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _config(
    workspace: Path,
    environment: str,
    *,
    manifest_name: str = "pixi.toml",
) -> bytes:
    return json.dumps(
        {
            "schema_version": 2,
            "workspace": str(workspace),
            "environment": environment,
            "manifest_name": manifest_name,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


@pytest.fixture
def activation(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, bytes]:
    for name in ("pixi.toml", "pixi.lock", "activate.sh", "launcher", "entrypoint.py"):
        (tmp_path / name).write_text("captured " + name)
    config = tmp_path / "worker-activation.json"
    monkeypatch.setattr(entry, "CONFIG", config)
    monkeypatch.setattr(entry, "PIXI", tmp_path / "launcher")
    monkeypatch.setattr(entry, "ENTRYPOINT", tmp_path / "entrypoint.py")
    encoded = _config(tmp_path, "worker")
    config.write_bytes(encoded)
    return config, encoded


@pytest.mark.parametrize(
    "name", ["pixi.toml", "pixi.lock", "activate.sh", "launcher", "entrypoint.py"]
)
def test_prepared_activation_does_not_reread_captured_inputs(
    activation: tuple[Path, bytes],
    name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, encoded = activation
    (config.parent / name).write_text("changed")
    monkeypatch.setattr(
        subprocess, "run", lambda *_args, **_kwargs: pytest.fail("activation ran")
    )
    monkeypatch.setattr(
        Path, "open", lambda *_args, **_kwargs: pytest.fail("prepared payload read")
    )
    assert entry._configuration(encoded)["environment"] == "worker"


def test_reserved_settings_preserve_presence_values_and_ordinary_activation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = {
        "MODAL_CONTAINER_ARGUMENTS_PATH": "sdk-path",
        "MODAL_TOKEN_SECRET": "secret sentinel",
        "_ARTISAN_COMMAND_SECRET": "owner sentinel",
        "CUDA_VISIBLE_DEVICES": "",
        "PATH": "/old",
        "PYTHONPATH": "/old-python",
        "SDK_UNRELATED": "keep",
    }
    changes = dict.fromkeys(original, "authored replacement")
    changes.update(
        {
            "MODAL_NEW_AUTH": "introduced",
            "CUDA_DEVICE_ORDER": "introduced",
            "NVIDIA_VISIBLE_DEVICES": "all",
            "_ARTISAN_PARENT_COMMAND": "introduced",
            "PATH": "/worker/bin",
            "PYTHONPATH": "/worker/packages",
            "WORKER_SETTING": "literal $(not a command)",
        }
    )
    monkeypatch.setattr(
        entry, "_activated_bytes", lambda *_args: json.dumps(changes).encode()
    )
    config = {
        "workspace": "/worker",
        "manifest_name": "pixi.toml",
        "environment": "worker",
    }
    result = entry._environment(config, original)
    for key in (
        "MODAL_CONTAINER_ARGUMENTS_PATH",
        "MODAL_TOKEN_SECRET",
        "_ARTISAN_COMMAND_SECRET",
        "CUDA_VISIBLE_DEVICES",
    ):
        assert result[key] == original[key]
    for key in (
        "MODAL_NEW_AUTH",
        "CUDA_DEVICE_ORDER",
        "NVIDIA_VISIBLE_DEVICES",
        "_ARTISAN_PARENT_COMMAND",
    ):
        assert key not in result
    assert result["PATH"] == "/worker/bin"
    assert result["PYTHONPATH"] == "/worker/packages"
    assert result["WORKER_SETTING"] == "literal $(not a command)"
    command = entry._activation_command(config, 7)
    assert command[1:7] == [
        "run",
        "--as-is",
        "--offline",
        "--no-config",
        "--executable",
        "--manifest-path",
    ]
    assert "secret sentinel" not in " ".join(command)
    assert command[-2:] == ["--capture-environment", "7"]


def test_activation_failure_does_not_include_sensitive_stdout_or_stderr(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    launcher = tmp_path / "pixi"
    launcher.write_text(
        "#!"
        + sys.executable
        + "\nimport sys\nprint('secret sentinel')\nprint('secret sentinel',file=sys.stderr)\nsys.exit(1)\n"
    )
    launcher.chmod(0o755)
    monkeypatch.setattr(entry, "PIXI", launcher)
    with pytest.raises(RuntimeError, match=r"^Canonical worker activation failed$"):
        entry._environment(
            {
                "workspace": "/worker",
                "manifest_name": "pixi.toml",
                "environment": "worker",
            },
            {},
        )


@pytest.mark.parametrize(
    "response",
    [
        b"secret malformed",
        b"[]",
        b'{"X":42}',
    ],
)
def test_malformed_activation_rejects_without_echoing_response(
    response: bytes, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(entry, "_activated_bytes", lambda *_args: response)
    with pytest.raises(ValueError, match=r"^Invalid Pixi activation") as failure:
        entry._environment(
            {
                "workspace": "/worker",
                "manifest_name": "pixi.toml",
                "environment": "worker",
            },
            {},
        )
    assert "secret" not in str(failure.value)


def test_startup_requires_exact_marker_and_config(
    activation: tuple[Path, bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, encoded = activation
    monkeypatch.delenv(entry.ACTIVATION_MARKER, raising=False)
    with pytest.raises(ValueError, match="not established"):
        entry.verify_activation(encoded)
    expected = hashlib.sha256(encoded).hexdigest()
    monkeypatch.setenv(entry.ACTIVATION_MARKER, expected)
    assert entry.verify_activation(encoded) == expected
    config.write_bytes(encoded + b" ")
    with pytest.raises(ValueError, match="not established"):
        entry.verify_activation(encoded)


def test_sdk_argv_and_activated_interpreter_are_exact(
    activation: tuple[Path, bytes],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config, _ = activation
    interpreter = config.parent / ".pixi/envs/worker/bin/python"
    monkeypatch.setattr(
        entry, "_environment", lambda *_args: {"PATH": str(interpreter.parent)}
    )
    monkeypatch.setattr(
        entry.shutil, "which", lambda *_args, **_kwargs: str(interpreter)
    )
    observed = []
    monkeypatch.setattr(os, "execve", lambda *args: observed.append(args))
    argv = ["python", "-m", "modal._container_entrypoint", "literal spaces;colon"]
    entry.main([str(config), *argv])
    executable, forwarded, environment = observed[0]
    assert executable == str(interpreter)
    assert forwarded == argv
    assert (
        environment[entry.ACTIVATION_MARKER]
        == hashlib.sha256(config.read_bytes()).hexdigest()
    )
    monkeypatch.setattr(
        entry.shutil, "which", lambda *_args, **_kwargs: "/other/python"
    )
    with pytest.raises(ValueError, match="captured worker interpreter"):
        entry.main([str(config), *argv])
    assert len(observed) == 1


def test_pinned_sdk_serializes_exact_fixed_entrypoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import modal
    from modal.image import _Image

    assert modal.__version__ == "1.4.1"
    commands = []
    monkeypatch.setattr(
        _Image, "dockerfile_commands", lambda _self, command: commands.append(command)
    )
    args = [
        "/usr/local/bin/python",
        "-I",
        "-B",
        "-S",
        str(entry.ENTRYPOINT),
        str(entry.CONFIG),
    ]
    _Image.entrypoint(object.__new__(_Image), args)
    assert commands == [
        "ENTRYPOINT [" + ", ".join(json.dumps(value) for value in args) + "]"
    ]


def test_image_builder_places_activation_before_sdk_command(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import modal

    calls = []

    class Image:
        def __getattr__(self, name: str):
            def call(*args: object, **kwargs: object):
                calls.append((name, args, kwargs))
                return self

            return call

    image = Image()
    monkeypatch.setattr(modal.Image, "from_registry", lambda *_args: image)
    preparation._image(
        prepared[0].startup.layout, tmp_path, tmp_path / "pixi", preparation.BASE_IMAGE
    )
    selected = next(item for item in calls if item[0] == "entrypoint")
    assert selected[1] == (
        [
            "/usr/local/bin/python",
            "-I",
            "-B",
            "-S",
            str(entry.ENTRYPOINT),
            str(entry.CONFIG),
        ],
    )
    assert [item[0] for item in calls].index("run_function") < [
        item[0] for item in calls
    ].index("entrypoint")


def test_missing_startup_activation_rejects_before_package_measurement(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = tmp_path / "layout.json"
    layout.write_text(prepared[0].startup.layout.model_dump_json())
    monkeypatch.setattr(worker, "LAYOUT_FILE", layout)

    def missing(_expected: bytes) -> str:
        msg = "activation missing"
        raise ValueError(msg)

    monkeypatch.setattr(worker, "verify_activation", missing)
    monkeypatch.setattr(
        worker,
        "capture_runtime_compatibility",
        lambda *_args: pytest.fail("metadata capture ran"),
    )
    with pytest.raises(ValueError, match="activation missing"):
        worker.startup()


def test_real_pixi_existing_prefix_activates_before_fresh_application_import(
    tmp_path: Path,
) -> None:
    prefix = Path(sys.prefix)
    workspace = prefix.parents[2]
    manifest_name = (
        "pixi.toml" if (workspace / "pixi.toml").is_file() else "pyproject.toml"
    )
    if prefix.parent.name != "envs" or not (workspace / manifest_name).is_file():
        pytest.skip("requires this checkout's existing Pixi prefix")
    pixi = Path(os.environ["PIXI_EXE"])
    resource = Path(entry.__file__)
    encoded = _config(workspace, prefix.name, manifest_name=manifest_name)
    config = tmp_path / "worker-activation.json"
    config.write_bytes(encoded)
    result = tmp_path / "observed.json"
    application = tmp_path / "activation_canary.py"
    application.write_text(
        "import json,os,sys\n"
        "from pathlib import Path\n"
        "assert os.environ['CONDA_PREFIX'] == sys.prefix\n"
        "assert os.environ['PIXI_ENVIRONMENT_NAME'] == Path(sys.prefix).name\n"
        "assert os.environ['MODAL_TOKEN_SECRET'] == 'synthetic runtime $(literal) value'\n"
        "assert os.environ['CUDA_VISIBLE_DEVICES'] == ''\n"
        "Path(sys.argv[1]).write_text(json.dumps({'argv':sys.argv[2:],"
        "'activated_before_import':True,'marker':os.environ['_ARTISAN_MODAL_ACTIVATION']}))\n"
    )
    bootstrap = """
import runpy,sys
from pathlib import Path
scope = runpy.run_path(sys.argv[1])['main'].__globals__
scope.update(PIXI=Path(sys.argv[3]), ENTRYPOINT=Path(sys.argv[1]), CONFIG=Path(sys.argv[2]))
scope['main']([sys.argv[2], sys.executable, '-c', 'import activation_canary', sys.argv[4], 'literal spaces;colon'])
"""
    original = dict(os.environ)
    original.update(
        CONDA_PREFIX="unqualified-original",
        PATH="/usr/bin:/bin",
        PYTHONPATH=str(tmp_path),
        MODAL_TOKEN_SECRET="synthetic runtime $(literal) value",
        CUDA_VISIBLE_DEVICES="",
    )
    before = {
        name: (workspace / name).read_bytes() for name in (manifest_name, "pixi.lock")
    }
    process = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            bootstrap,
            str(resource),
            str(config),
            str(pixi),
            str(result),
        ],
        env=original,
        capture_output=True,
        check=True,
        timeout=15,
    )
    observed = json.loads(result.read_bytes())
    assert observed == {
        "argv": ["literal spaces;colon"],
        "activated_before_import": True,
        "marker": hashlib.sha256(encoded).hexdigest(),
    }
    assert b"synthetic runtime $(literal) value" not in process.stdout + process.stderr
    assert process.stdout == b""
    assert before == {name: (workspace / name).read_bytes() for name in before}


def test_real_pixi_preserves_pinned_run_activation_semantics_without_install(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pixi = Path(os.environ["PIXI_EXE"])
    manifest = tmp_path / "pixi.toml"
    manifest.write_text(
        '[workspace]\nname="activation-only"\nchannels=[]\nplatforms=["linux-64"]\n'
        '[activation]\nscripts=["activate.sh"]\n'
        '[activation.env]\nWORKER_LITERAL="spaces;colon"\n'
    )
    (tmp_path / "pixi.lock").write_text("version: 7\nenvironments: {}\npackages: []\n")
    script = tmp_path / "activate.sh"
    script.write_text(
        'unset WORKER_REMOVE\nexport WORKER_SCRIPT="script literal"\nprintf "ordinary activation output\\n"\n'
    )
    config = {
        "workspace": str(tmp_path),
        "manifest_name": "pixi.toml",
        "environment": "default",
    }
    monkeypatch.setattr(entry, "ENTRYPOINT", Path(entry.__file__))
    original_pixi = entry.PIXI
    try:
        entry.PIXI = pixi
        activated = entry._environment(
            config, {"PATH": "/usr/bin:/bin", "WORKER_REMOVE": "remove me"}
        )
    finally:
        entry.PIXI = original_pixi
    assert activated["WORKER_LITERAL"] == "spaces;colon"
    assert activated["WORKER_SCRIPT"] == "script literal"
    # Pixi 0.83 run retains incoming values removed by its activation script.
    assert activated["WORKER_REMOVE"] == "remove me"
    assert not (tmp_path / ".pixi/envs/default").exists()
    assert manifest.read_text().startswith('[workspace]\nname="activation-only"')


def test_payload_binds_wrapper_and_derived_config(
    prepared: tuple[PreparedModalImage, Environment, Environment],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = prepared[0].startup.layout.model_copy(
        update={"worker_entrypoint_sha256": _sha(Path(entry.__file__))}
    )
    witness = SimpleNamespace(executable=tmp_path / "witness")
    witness.executable.write_bytes(b"measured witness")
    captures = []
    for index, environment in enumerate(layout.environments):
        context = tmp_path / str(index)
        context.mkdir()
        inputs = SimpleNamespace(captured=SimpleNamespace(source=environment.source))
        captures.append((prepared[1], inputs, context))

    def write_payload(
        _inputs: object, context: Path, *, workspace: Path
    ) -> tuple[tuple[str, ...], ...]:
        (context / "payload.tar.xz").write_bytes(b"captured archive")
        return (("literal install command", str(workspace)),)

    monkeypatch.setattr(preparation, "write_container_payload", write_payload)
    output, digest = preparation._payloads(tmp_path, layout, tuple(captures), witness)
    resource = (output / entry.ENTRYPOINT.name).read_bytes()
    activation = (output / entry.CONFIG.name).read_bytes()
    assert resource == Path(entry.__file__).read_bytes()
    assert activation == layout.activation_config()
    assert (
        digest
        == hashlib.sha256(
            preparation.INSTALL_PROGRAM.encode()
            + b"\0"
            + (output / "install.json").read_bytes()
            + b"\0"
            + resource
            + b"\0"
            + activation
        ).hexdigest()
    )


@pytest.mark.parametrize("fail", [False, True])
def test_anonymous_capture_closes_descriptor_on_success_and_failure(
    monkeypatch: pytest.MonkeyPatch, fail: bool
) -> None:
    create = entry._anonymous_descriptor
    observed = []

    def descriptor() -> int:
        value = create()
        observed.append(value)
        assert not os.get_inheritable(value)
        assert os.readlink(f"/proc/self/fd/{value}").startswith("/memfd:")
        return value

    monkeypatch.setattr(entry, "_anonymous_descriptor", descriptor)

    def run(command: list[str], **options: object) -> SimpleNamespace:
        fd = observed[0]
        assert command[-1] == str(fd)
        assert options["pass_fds"] == (fd,)
        assert options["stdout"] == options["stderr"] == subprocess.DEVNULL
        assert options["timeout"] == 30
        if fail:
            raise subprocess.TimeoutExpired(command, 30)
        os.write(fd, b'{"CAPTURED":"complete"}')
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(subprocess, "run", run)
    config = {
        "workspace": "/worker",
        "manifest_name": "pixi.toml",
        "environment": "worker",
    }
    if fail:
        with pytest.raises(subprocess.TimeoutExpired):
            entry._activated_bytes(config, {})
    else:
        assert entry._activated_bytes(config, {}) == b'{"CAPTURED":"complete"}'
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(observed[0])


def test_activation_removal_restores_only_reserved_originals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dict.fromkeys(entry.CONTROL_NAMES, "literal;$(not executed)")
    original.update(
        MODAL_CONTAINER_ARGUMENTS_PATH="original",
        ORDINARY_REMOVED="old",
        LD_LIBRARY_PATH="/original/lib",
    )
    activated = {
        "PATH": "/worker/bin",
        "PYTHONPATH": "/worker/packages",
        "LD_LIBRARY_PATH": "/canonical/lib",
        "MODAL_INJECTED": "new",
    }
    monkeypatch.setattr(
        entry, "_activated_bytes", lambda *_args: json.dumps(activated).encode()
    )
    result = entry._environment({}, original)
    assert all(result[key] == original[key] for key in entry.CONTROL_NAMES)
    assert result["MODAL_CONTAINER_ARGUMENTS_PATH"] == "original"
    assert "MODAL_INJECTED" not in result
    assert "ORDINARY_REMOVED" not in result
    assert result["LD_LIBRARY_PATH"] == "/canonical/lib"
