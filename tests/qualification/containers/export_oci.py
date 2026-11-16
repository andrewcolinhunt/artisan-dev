"""Stream an image filesystem into a separately identified flattened archive.

Docker image-save can stage complete layers on the daemon's disk. Container
export streams a stopped container's filesystem instead; all archive assembly
here occurs in the caller-selected directory.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import shutil
import subprocess
import tarfile
import time
import uuid
from pathlib import Path


def digest_file(path: Path) -> str:
    """Hash complete bytes without loading the image into Python memory."""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def json_bytes(value: object) -> bytes:
    """Encode configuration metadata independently of dictionary insertion order."""
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def export(image: str, output: Path) -> None:
    """Preserve source/config/rootfs/archive identities through flattening."""
    source = json.loads(
        subprocess.check_output(["docker", "image", "inspect", image], text=True)
    )[0]
    assert not source["Config"].get("Volumes"), (
        "Volume content requires separate derivation rules"
    )
    output.mkdir(parents=True, exist_ok=False)
    name = f"artisan-container-export-{uuid.uuid4().hex[:12]}"
    container = subprocess.check_output(
        ["docker", "create", "--name", name, "--entrypoint", "/bin/true", source["Id"]],
        text=True,
    ).strip()
    minimum = shutil.disk_usage("/").free
    try:
        process = subprocess.Popen(
            ["docker", "export", "--output", str(output / "rootfs.tar"), container]
        )
        try:
            while process.poll() is None:
                minimum = min(minimum, shutil.disk_usage("/").free)
                if minimum < 500_000_000:
                    process.terminate()
                    msg = "Filesystem export crossed the host disk reserve"
                    raise RuntimeError(msg)
                time.sleep(0.5)
            assert process.returncode == 0
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=15)
    finally:
        subprocess.run(
            ["docker", "rm", "-v", container], check=True, capture_output=True
        )
    rootfs_digest = digest_file(output / "rootfs.tar")
    configuration = dict(source["Config"])
    configuration["Labels"] = {
        **(configuration.get("Labels") or {}),
        "org.artisan.source-image": source["Id"],
    }
    flattened = json_bytes(
        {
            "architecture": source["Architecture"],
            "os": source["Os"],
            "config": configuration,
            "rootfs": {"type": "layers", "diff_ids": [f"sha256:{rootfs_digest}"]},
            "history": [{"created_by": "qualification filesystem export"}],
        }
    )
    flattened_id = hashlib.sha256(flattened).hexdigest()
    manifest = json_bytes(
        [
            {
                "Config": f"{flattened_id}.json",
                "RepoTags": ["artisan-qualification:flattened"],
                "Layers": ["rootfs.tar"],
            }
        ]
    )
    with tarfile.open(output / "flattened.tar", "w") as archive:
        archive.add(output / "rootfs.tar", arcname="rootfs.tar")
        for name, content in (
            (f"{flattened_id}.json", flattened),
            ("manifest.json", manifest),
        ):
            info = tarfile.TarInfo(name)
            info.size = len(content)
            archive.addfile(info, io.BytesIO(content))
    receipt = {
        "source_image_id": source["Id"],
        "source_config_digest": hashlib.sha256(
            json_bytes(source["Config"])
        ).hexdigest(),
        "source_rootfs": source["RootFS"],
        "rootfs_digest": rootfs_digest,
        "flattened_image_id": f"sha256:{flattened_id}",
        "flattened_archive_digest": digest_file(output / "flattened.tar"),
        "minimum_disk_free_bytes": minimum,
    }
    (output / "derivation.json").write_text(json.dumps(receipt, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("image")
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    export(args.image, args.output)
