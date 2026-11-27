"""Explicit canonical image construction and measured startup for Modal workers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

from artisan.orchestration.runner_api import (
    ContainerBuildInputs,
    Environment,
    PreparedCommandWitness,
    capture_container_inputs,
    prepare_command_witness,
    verify_container_source,
    write_container_payload,
)
from artisan_modal._worker_entrypoint import CONFIG, ENTRYPOINT
from artisan_modal.bindings import (
    LAYOUT_FILE,
    PIXI,
    ImageEnvironment,
    ImageLayout,
    ImageStartup,
    PreparedModalImage,
)

if TYPE_CHECKING:
    import modal

BASE_IMAGE = (
    "python:3.12.12-slim-bookworm@"
    "sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c"
)
INPUT_ROOT = Path("/opt/artisan/image-inputs")
INSTALL_PROGRAM = '''"""Install only captured canonical image inputs at their final paths."""
import json
import subprocess
import tarfile
from pathlib import Path

root = Path("/opt/artisan/image-inputs")
recipe = json.loads((root / "install.json").read_text())
for name in recipe["archives"]:
    with tarfile.open(root / name) as archive:
        archive.extractall("/", filter="data")
for command in recipe["commands"]:
    subprocess.run(command, check=True)
'''


def _require_sdk(*, configure_builder: bool = False) -> None:
    import modal

    if modal.__version__ != "1.4.1":
        message = "Modal preparation requires SDK 1.4.1"
        raise ValueError(message)
    configured = os.environ.get("MODAL_IMAGE_BUILDER_VERSION")
    if configured not in (None, "2025.06"):
        message = "Modal preparation requires image builder 2025.06"
        raise ValueError(message)
    if configure_builder:
        os.environ["MODAL_IMAGE_BUILDER_VERSION"] = "2025.06"


def _capture(
    worker: Environment,
    tools: Sequence[Environment],
    directory: Path,
    cache_root: Path,
    witness: PreparedCommandWitness,
) -> tuple[ImageLayout, tuple[tuple[Environment, ContainerBuildInputs, Path], ...]]:
    captured: list[tuple[Environment, ContainerBuildInputs, Path]] = []
    declarations: list[Environment] = []
    for declaration in (worker, *tools):
        if declaration not in declarations:
            declarations.append(declaration)
    for declaration in declarations:
        context = directory / str(len(captured))
        context.mkdir()
        inputs = capture_container_inputs(declaration, context, cache_root=cache_root)
        captured.append((declaration, inputs, context))
    sources = {inputs.captured.source.digest: inputs for _, inputs, _ in captured}
    layout = ImageLayout(
        worker_source=captured[0][1].captured.source.digest,
        environments=tuple(
            ImageEnvironment(source=inputs.captured.source, wheels=inputs.wheels)
            for _, inputs in sorted(sources.items())
        ),
        pixi_sha256=captured[0][1].pixi_sha256,
        worker_entrypoint_sha256=hashlib.sha256(
            Path(__file__).with_name("_worker_entrypoint.py").read_bytes()
        ).hexdigest(),
        witness=witness.model_copy(
            update={"executable": Path("/opt/artisan/command-witness")}
        ),
    )
    if any(inputs.pixi_sha256 != layout.pixi_sha256 for _, inputs, _ in captured):
        message = "Pixi changed while capturing image requirements"
        raise ValueError(message)
    return layout, tuple(captured)


def _payloads(
    directory: Path,
    layout: ImageLayout,
    captures: tuple[tuple[Environment, ContainerBuildInputs, Path], ...],
    witness: PreparedCommandWitness,
) -> tuple[Path, str]:
    output = directory / "image-inputs"
    output.mkdir()
    shutil.copyfile(witness.executable, output / "command-witness")
    (output / "command-witness").chmod(0o555)
    archives: list[str] = []
    commands: list[tuple[str, ...]] = []
    by_source = {
        inputs.captured.source.digest: (inputs, context)
        for _, inputs, context in captures
    }
    for environment in layout.environments:
        inputs, context = by_source[environment.source.digest]
        commands.extend(
            write_container_payload(inputs, context, workspace=environment.workspace)
        )
        name = environment.source.digest + ".tar.xz"
        (context / "payload.tar.xz").replace(output / name)
        archives.append(name)
    recipe = {"archives": archives, "commands": commands}
    encoded = json.dumps(recipe, sort_keys=True, separators=(",", ":")).encode()
    (output / "install.json").write_bytes(encoded)
    (output / "install.py").write_text(INSTALL_PROGRAM)
    (output / "layout.json").write_text(layout.model_dump_json())
    entrypoint = Path(__file__).with_name("_worker_entrypoint.py").read_bytes()
    if hashlib.sha256(entrypoint).hexdigest() != layout.worker_entrypoint_sha256:
        msg = "Worker startup resource changed during preparation"
        raise ValueError(msg)
    activation = layout.activation_config()
    (output / ENTRYPOINT.name).write_bytes(entrypoint)
    (output / CONFIG.name).write_bytes(activation)
    digest = hashlib.sha256(
        INSTALL_PROGRAM.encode()
        + b"\0"
        + encoded
        + b"\0"
        + entrypoint
        + b"\0"
        + activation
    ).hexdigest()
    return output, digest


def _image(
    layout: ImageLayout,
    payloads: Path,
    pixi: Path,
    base_image: str,
) -> modal.Image:
    import modal

    image = (
        modal.Image.from_registry(base_image)
        .add_local_file(pixi, str(PIXI), copy=True)
        .add_local_dir(payloads, str(INPUT_ROOT), copy=True)
        .add_local_file(payloads / "layout.json", str(LAYOUT_FILE), copy=True)
        .add_local_file(
            payloads / "command-witness", "/opt/artisan/command-witness", copy=True
        )
    )
    image = image.run_function(
        subprocess.run,
        args=(["/usr/local/bin/python", str(INPUT_ROOT / "install.py")],),
        kwargs={"check": True},
        cpu=2,
        memory=4096,
        timeout=900,
        include_source=False,
    )
    image = image.entrypoint(
        ["/usr/local/bin/python", "-I", "-B", "-S", str(ENTRYPOINT), str(CONFIG)]
    )
    return image.env(
        {
            "PATH": str(layout.worker.prefix / "bin") + ":/usr/local/bin:/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )


def prepare_image(
    worker: Environment,
    tools: Sequence[Environment],
    *,
    package_names: tuple[str, ...],
    cache_root: Path,
    base_image: str = BASE_IMAGE,
) -> PreparedModalImage:
    """Package runner-collected requirements and measure an ephemeral startup.

    This internal image stage does not deploy a persistent assignment endpoint.
    Package names come from the trusted core collector, never a second authored
    dependency list. The caller must compare selected controller payloads against
    the measured worker compatibility before admitting any live calls.
    """
    import modal

    from artisan_modal.worker import startup

    _require_sdk(configure_builder=True)
    if re.fullmatch(r".+@sha256:[0-9a-f]{64}", base_image) is None:
        message = "Modal base image must use an immutable registry digest"
        raise ValueError(message)
    cache_root.mkdir(parents=True, exist_ok=True)
    witness = prepare_command_witness(cache_root)
    with tempfile.TemporaryDirectory(
        prefix="modal-image-", dir=cache_root
    ) as temporary:
        directory = Path(temporary)
        layout, captures = _capture(worker, tools, directory, cache_root, witness)
        payloads, recipe_digest = _payloads(directory, layout, captures, witness)
        image = _image(layout, payloads, captures[0][2] / "pixi", base_image)
        app = modal.App()
        describe = app.function(
            image=image,
            cpu=1,
            memory=1024,
            max_containers=1,
            retries=0,
            timeout=60,
            startup_timeout=300,
            include_source=False,
        )(startup)
        with app.run():
            measured = ImageStartup.model_validate_json(describe.remote(package_names))
            if measured.layout != layout:
                message = "Modal startup differs from the requested canonical image"
                raise ValueError(message)
            result = PreparedModalImage(
                image_id=image.object_id,
                base_image=base_image,
                recipe_sha256=recipe_digest,
                startup=measured,
            )
        for declaration, inputs, _ in captures:
            verify_container_source(declaration, inputs.captured.source)
        return result
