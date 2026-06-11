"""Build op container images from their conventional Dockerfiles.

The image ref in ``ModalComputeConfig.image`` is the single source of
truth: ``dockerfile_for`` resolves the conventional Dockerfile for a ref
(``docker/<image-name>/Dockerfile``), and ``build_image`` tags the build
with exactly that ref — there is no second place a tag is typed, so the
built tag cannot drift from what ``deploy.build_app`` later pulls via
``from_registry``.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from artisan.execution.tool_endpoint.spec import endpoint_spec
from artisan.operations.base.operation_definition import OperationDefinition


def dockerfile_for(image: str, root: Path) -> Path:
    """Resolve the conventional Dockerfile path for an image ref.

    Strips any digest/tag, takes the last path segment as the image
    name, and expects ``<root>/docker/<name>/Dockerfile``.

    Args:
        image: Full image ref, e.g.
            ``ghcr.io/dexterity-systems/artisan-worker:latest``.
        root: Repo root the convention is resolved against.

    Returns:
        Path to the Dockerfile.

    Raises:
        FileNotFoundError: If no Dockerfile exists at the conventional
            path; the message names the expected location.
    """
    path = root / "docker" / _image_name(image) / "Dockerfile"
    if not path.is_file():
        msg = (
            f"No Dockerfile for image {image!r} — expected {path} "
            "(convention: docker/<image-name>/Dockerfile, run from the repo root)"
        )
        raise FileNotFoundError(msg)
    return path


def build_image(op_cls: type[OperationDefinition], root: Path) -> str:
    """Build the op's image, tagged with the ref its config declares.

    Runs ``docker build -f <dockerfile> -t <ref> <root>``, streaming
    build output to the caller's stdout/stderr.

    Args:
        op_cls: The registered operation class.
        root: Build context (repo root).

    Returns:
        The image ref that was built and tagged.

    Raises:
        ValueError: If the op is not a tool op or has no modal config.
        FileNotFoundError: If no Dockerfile exists at the conventional path.
        subprocess.CalledProcessError: If the docker build fails.
    """
    spec = endpoint_spec(op_cls)
    dockerfile = dockerfile_for(spec.image, root)
    subprocess.run(
        ["docker", "build", "-f", str(dockerfile), "-t", spec.image, str(root)],
        check=True,
    )
    return spec.image


def _image_name(image: str) -> str:
    """Last path segment of a ref, digest and tag stripped.

    ``ghcr.io/org/artisan-worker:0.3.0`` → ``artisan-worker``. A ``:``
    is a tag separator only after the last ``/`` — registry hosts may
    carry a port (``registry:5000/foo``).
    """
    ref = image.split("@", 1)[0]
    slash = ref.rfind("/")
    colon = ref.rfind(":")
    if colon > slash:
        ref = ref[:colon]
    return ref[slash + 1 :]
