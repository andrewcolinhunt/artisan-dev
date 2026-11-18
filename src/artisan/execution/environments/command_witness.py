"""Build a static command helper from a locked internal Pixi toolchain."""

from __future__ import annotations

import hashlib
import os
import platform
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

from artisan.execution.environments.native import prepare_native_environment
from artisan.execution.environments.native_target import (
    native_executable,
    verify_native_environment,
)
from artisan.execution.environments.preparation_lock import preparation_lock
from artisan.execution.environments.source import (
    capture_environment_source,
    read_environment_inputs,
)
from artisan.schemas.execution.command_witness import (
    CommandExitReceipt,
    PreparedCommandWitness,
)
from artisan.schemas.execution.prepared_environment import PreparedNativeEnvironment
from artisan.schemas.execution.worker_transport import PayloadFile
from artisan.schemas.operation_config.environment import Environment
from artisan.utils.hashing import canonical_json_bytes

SOURCE = Path(__file__).parents[1] / "_command_witness.c"
BUILD_MANIFEST = Path(__file__).with_name("_command_witness_build") / "pixi.toml"
COMPILER = "x86_64-conda-linux-gnu-cc"
COMPILE_FLAGS = ("-std=c11", "-O2", "-static")


def _digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _architecture() -> str:
    if platform.system() != "Linux" or platform.machine() != "x86_64":
        msg = "The command witness is qualified only for Linux x86_64"
        raise ValueError(msg)
    return "x86_64"


def prepare_command_witness(cache_root: Path) -> PreparedCommandWitness:
    """Prepare the locked build environment and compile once at an owned path."""
    _architecture()
    source = SOURCE.read_bytes()
    source_digest = hashlib.sha256(source).hexdigest()
    recipe = _recipe_digest(source_digest)
    cache_root = cache_root.resolve()
    root = cache_root / "command-witness" / recipe
    with preparation_lock(root):
        if (root / "binding.json").is_file():
            return lookup_command_witness(cache_root)
        builder = prepare_native_environment(
            Environment.from_pixi(BUILD_MANIFEST), cache_root=cache_root
        )
        with tempfile.TemporaryDirectory(dir=root.parent) as temporary:
            build = Path(temporary)
            (build / "witness.c").write_bytes(source)
            identity = _compile(builder, build)
            target = PreparedCommandWitness(
                executable=root / "witness",
                architecture="x86_64",
                source_sha256=source_digest,
                binary_sha256=_digest(build / "witness"),
                recipe_sha256=recipe,
                builder_digest=builder.digest,
                compiler_version=identity[0],
                compiler_files=identity[1],
            )
            root.mkdir()
            try:
                shutil.copyfile(build / "witness", target.executable)
                target.executable.chmod(0o555)
                (root / "binding.json").write_text(target.model_dump_json(indent=2))
                verify_command_witness(target)
            except Exception:
                shutil.rmtree(root)
                raise
        return target


def _recipe_digest(source_digest: str) -> str:
    inputs = read_environment_inputs(
        Environment.from_pixi(BUILD_MANIFEST), platform="linux-64"
    )
    return hashlib.sha256(
        canonical_json_bytes(
            {
                "source": source_digest,
                "builder": capture_environment_source(inputs).source.digest,
                "compiler": COMPILER,
                "flags": COMPILE_FLAGS,
            }
        )
    ).hexdigest()


def _compile(
    builder: PreparedNativeEnvironment, build: Path
) -> tuple[str, tuple[PayloadFile, ...]]:
    compiler = native_executable(builder, COMPILER)
    environment = {
        "PATH": str(builder.prefix / "bin"),
        "LANG": "C",
        "TMPDIR": str(build),
    }

    def output(*arguments: str) -> str:
        return subprocess.check_output(
            [str(compiler), *arguments], env=environment, text=True, timeout=30
        ).strip()

    version = output("--version")
    source = build / "witness.c"
    subprocess.run(
        [str(compiler), "-std=c11", "-M", "-MF", str(build / "headers.d"), str(source)],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    paths = {compiler.resolve()}
    paths.update(
        Path(output(f"-print-prog-name={name}")).resolve(strict=True)
        for name in ("as", "ld", "cc1")
    )
    for name in (
        "crt1.o",
        "crti.o",
        "crtn.o",
        "crtbeginT.o",
        "crtend.o",
        "libc.a",
        "libgcc.a",
        "libgcc_eh.a",
    ):
        paths.add(Path(output(f"-print-file-name={name}")).resolve(strict=True))
    dependencies = shlex.split(
        (build / "headers.d").read_text().replace("\\\n", "").split(":", 1)[1]
    )
    for name in dependencies:
        path = Path(name.replace("$$", "$")).resolve()
        if path != source.resolve():
            paths.add(path)
    if any(not path.is_relative_to(builder.prefix) for path in paths):
        msg = "Command witness compiler selected inputs outside its prepared prefix"
        raise ValueError(msg)

    def inventory() -> tuple[PayloadFile, ...]:
        return tuple(
            PayloadFile(
                path=path.relative_to(builder.prefix).as_posix(), sha256=_digest(path)
            )
            for path in sorted(paths)
        )

    before = inventory()
    subprocess.run(
        [
            str(compiler),
            *COMPILE_FLAGS,
            str(source),
            "-o",
            str(build / "witness"),
        ],
        env=environment,
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    if inventory() != before:
        msg = "Selected command witness compiler inputs changed during preparation"
        raise ValueError(msg)
    verify_native_environment(builder)
    return version, before


def lookup_command_witness(cache_root: Path) -> PreparedCommandWitness:
    """Read exact existing helper coverage without invoking the compiler."""
    path = (
        cache_root.resolve()
        / "command-witness"
        / _recipe_digest(_digest(SOURCE))
        / "binding.json"
    )
    if not path.is_file():
        msg = "Command exit capability is not prepared; prepare explicitly"
        raise ValueError(msg)
    target = PreparedCommandWitness.model_validate_json(path.read_bytes())
    if target.executable != path.parent / "witness":
        msg = "Command witness locator differs from its prepared binding"
        raise ValueError(msg)
    verify_command_witness(target)
    return target


def verify_command_witness(target: PreparedCommandWitness) -> None:
    """Reject changed helper bytes/source; build tools are not runtime dependencies."""
    if (
        target.architecture != _architecture()
        or target.source_sha256 != _digest(SOURCE)
        or target.recipe_sha256 != _recipe_digest(target.source_sha256)
        or not target.executable.is_absolute()
        or not target.executable.is_file()
        or not os.access(target.executable, os.X_OK)
        or target.binary_sha256 != _digest(target.executable)
    ):
        msg = "Prepared command witness identity changed"
        raise ValueError(msg)


@dataclass(frozen=True)
class WitnessInvocation:
    """One command's private status path and nonce, created outside authored argv."""

    target: PreparedCommandWitness
    directory: Path
    nonce: str = field(default_factory=lambda: uuid4().hex)

    @property
    def receipt(self) -> Path:
        return self.directory / f"{self.nonce}.json"

    def wrap(
        self,
        command: list[str],
        *,
        prefix: Path | None = None,
        external_access: Path | None = None,
    ) -> list[str]:
        """Preserve argv and optionally require the resolved tool inside its prefix."""
        self.directory.mkdir(parents=True, exist_ok=True)
        confinement = ["--prefix", str(prefix)] if prefix is not None else []
        access = (
            ["--external-access", str(external_access)]
            if external_access is not None
            else []
        )
        return [
            str(self.target.executable),
            str(self.receipt),
            self.nonce,
            *confinement,
            *access,
            *command,
        ]

    def verify_exit(self, return_code: int) -> bool:
        """Return whether an exact real tool exit was confirmed; failures stay infra."""
        receipt = CommandExitReceipt.model_validate_json(self.receipt.read_bytes())
        expected = receipt.code if receipt.kind == "exit" else 128 + receipt.code
        if (
            receipt.nonce != self.nonce
            or receipt.kind == "exec_error"
            or expected != return_code
        ):
            msg = "Command launcher did not confirm the exact tool exit"
            raise RuntimeError(msg)
        return receipt.kind == "exit"
