"""Bounded ephemeral Modal transport probe; never deploys an application."""

from __future__ import annotations

import argparse
import json
import os
import time
import uuid
from pathlib import Path

import bootstrap
import cloudpickle
import modal
import sequence
from application_schema import ProbeInput
from run_local import DeserializeCanary

from artisan.schemas.artifact.metric import MetricArtifact

PACKAGES = [
    "cloudpickle",
    "pydantic",
    "dexterity-artisan",
    "polars",
    "deltalake",
    "pyarrow",
]


def qualify(wheel: Path, output: Path) -> dict[str, object]:
    """Run at most eight CPU inputs against a uniquely owned temporary app."""
    os.environ["MODAL_IMAGE_BUILDER_VERSION"] = "2025.06"
    output.mkdir(parents=True, exist_ok=False)
    cloudpickle.register_pickle_by_value(sequence)
    image = (
        modal.Image.from_registry("python:3.12.12-slim-bookworm")
        .add_local_file(
            Path.home() / ".pixi/bin/pixi", "/usr/local/bin/pixi", copy=True
        )
        .add_local_dir(
            Path(__file__).resolve().parents[1],
            "/opt/qualification",
            copy=True,
            ignore=["__pycache__", "*.pyc"],
        )
        .add_local_file(wheel, f"/wheels/{wheel.name}", copy=True)
        .run_commands(
            f"python /opt/qualification/providers/prepare.py /opt/provider-probe --pixi /usr/local/bin/pixi --artisan-wheel /wheels/{wheel.name}"
        )
        .env(
            {
                "PATH": "/opt/provider-probe/.pixi/envs/worker/bin:/usr/local/bin:/usr/bin:/bin"
            }
        )
    )
    report: dict[str, object] = {
        "sdk": modal.__version__,
        "builder": "2025.06",
        "resources": {
            "cpu": 1,
            "memory_mib": 512,
            "max_containers": 1,
            "timeout": 60,
            "retries": 0,
        },
        "app_name": f"artisan-provider-qualification-{uuid.uuid4().hex[:10]}",
    }
    with modal.Volume.ephemeral() as volume:
        app = modal.App(str(report["app_name"]))

        @app.function(
            image=image,
            cpu=1,
            memory=512,
            max_containers=1,
            retries=0,
            timeout=60,
            startup_timeout=180,
            volumes={"/deliveries": volume},
            serialized=True,
            include_source=False,
        )
        def invoke(header: dict[str, object], payload: bytes) -> dict[str, object]:
            import json
            import os
            import shutil
            import sys
            import threading
            import time
            import uuid
            from pathlib import Path

            sys.path.insert(0, "/opt/qualification/providers")
            import bootstrap

            prepared = Path("/opt/provider-probe")
            if header["action"] == "describe":
                return {
                    "runtime": bootstrap.runtime_identity(PACKAGES),
                    "framework_digest": bootstrap.framework_digest(),
                    "application_digest": bootstrap.application_digest(),
                    "receipt": json.loads((prepared / "receipt.json").read_text()),
                    "environments": json.loads(
                        (prepared / "tool-environments.json").read_text()
                    ),
                    "executable": sys.executable,
                    "target": bootstrap.target_identity(
                        "modal",
                        "pending-image-id",
                        sdk=__import__("modal").__version__,
                        builder="2025.06",
                    ),
                }
            delivery = str(header["delivery_id"]) + "-" + uuid.uuid4().hex
            if header.get("hold"):

                def commit_started() -> None:
                    marker = Path(
                        f"/tmp/provider-deliveries/{delivery}/scratch/processes.json"
                    )
                    deadline = time.monotonic() + 30
                    while not marker.exists() and time.monotonic() < deadline:
                        time.sleep(0.05)
                    if marker.exists():
                        destination = Path(f"/deliveries/{delivery}/runtime.json")
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(marker, destination)
                        volume.commit()

                threading.Thread(target=commit_started, daemon=True).start()
            try:
                os.environ["QUALIFICATION_DELIVERY_ID"] = delivery
                result = bootstrap.execute(header, payload)
            except ValueError as error:
                return {
                    "rejected": str(error),
                    "canary_executed": Path(
                        f"/deliveries/{header['delivery_id']}-canary"
                    ).exists(),
                }
            finally:
                os.environ.pop("QUALIFICATION_DELIVERY_ID", None)
            local = Path(f"/tmp/provider-deliveries/{delivery}/candidate")
            remote = Path(f"/deliveries/{delivery}/candidate")
            remote.mkdir(parents=True, exist_ok=False)
            for path in local.iterdir():
                if path.name != "seal.json":
                    shutil.copyfile(path, remote / path.name)
            volume.commit()
            if header.get("partial"):
                return {
                    "delivery_id": delivery,
                    "payloads_committed": True,
                    "seal_committed": False,
                }
            shutil.copyfile(local / "seal.json", remote / "seal.json")
            volume.commit()
            return result

        with modal.enable_output(), app.run():
            report["app_id"] = app.app_id
            report["image_id"] = image.object_id
            description = invoke.remote({"action": "describe"}, b"")
            expected = bootstrap.runtime_identity(PACKAGES)
            assert description["runtime"] == expected
            assert description["framework_digest"] == bootstrap.framework_digest()
            assert description["application_digest"] == bootstrap.application_digest()
            assert description["executable"].startswith(
                "/opt/provider-probe/.pixi/envs/worker/"
            )
            report["prepared"] = description
            assert description["target"]["sdk"] == modal.__version__
            target = {**description["target"], "identity": image.object_id}

            def assignment(
                delivery: str, *, mismatch: str | None = None, hold: bool = False
            ) -> tuple[dict[str, object], bytes]:
                header = {
                    "action": "execute",
                    "runtime": dict(expected),
                    "framework_digest": description["framework_digest"],
                    "application_digest": description["application_digest"],
                    "prepared": "/opt/provider-probe",
                    "lock_digest": description["receipt"]["lock_digest"],
                    "selected_tools": ["a", "b"],
                    "delivery_id": delivery,
                    "hold": hold,
                    "partial": delivery == "partial",
                }
                if mismatch == "source":
                    header["framework_digest"] = "wrong-source"
                elif mismatch == "application":
                    header["application_digest"] = "wrong-application"
                elif mismatch:
                    header["runtime"][mismatch] = "0.0.invalid"
                descriptor = bootstrap.software_descriptor(header, target)
                if mismatch is None:
                    (output / "software-descriptor.json").write_text(
                        json.dumps(descriptor, indent=2) + "\n"
                    )
                arguments = {
                    "delivery_root": f"/tmp/provider-deliveries/{delivery}",
                    "tool_script": "/opt/qualification/providers/tool.py",
                    "environments": description["environments"],
                    "input_schema": ProbeInput(
                        metric=MetricArtifact.draft({"value": 5}, "input.json", 1)
                    ),
                    "tool_hold_seconds": 30 if hold else 0,
                    "software_descriptor": descriptor,
                    "envelope": {
                        "unit_id": "modal-unit",
                        "attempt": 0,
                        "generation": 0,
                        "delivery_id": delivery,
                        "recipe_digest": "two-tool-sequence",
                        "software_digest": bootstrap.software_digest(descriptor),
                    },
                }
                operation = (
                    DeserializeCanary(Path(f"/deliveries/{delivery}-canary"))
                    if mismatch
                    else sequence.execute_sequence
                )
                return header, cloudpickle.dumps((operation, arguments))

            report["first"] = invoke.remote(*assignment("first"))
            assert report["first"]["value"] == 16
            mismatches = {}
            for mismatch in ("python", "cloudpickle", "application"):
                result = invoke.remote(*assignment(mismatch, mismatch=mismatch))
                assert "rejected" in result
                assert not result["canary_executed"]
                mismatches[mismatch] = result
            report["mismatches"] = mismatches
            cancelled = invoke.spawn(*assignment("cancelled", hold=True))
            sibling = invoke.spawn(*assignment("sibling"))
            deadline = time.monotonic() + 35
            while time.monotonic() < deadline:
                paths = [entry.path for entry in volume.iterdir("/", recursive=True)]
                if any(
                    path.startswith("cancelled-") and path.endswith("/runtime.json")
                    for path in paths
                ):
                    break
                time.sleep(0.5)
            else:
                msg = "Modal tool descendant readiness was not committed"
                raise TimeoutError(msg)
            cancelled.cancel(terminate_containers=True)
            report["sibling"] = sibling.get(timeout=90)
            assert report["sibling"]["value"] == 16
            report["partial"] = invoke.remote(*assignment("partial"))
            assert not report["partial"]["seal_committed"]
        report["app_context_closed"] = True
        paths = [entry.path for entry in volume.iterdir("/", recursive=True)]
        assert not any(
            path.startswith("cancelled-") and path.endswith("/candidate/seal.json")
            for path in paths
        )
        assert not any(
            path.startswith("partial-") and path.endswith("/candidate/seal.json")
            for path in paths
        )
        for path in paths:
            if "/candidate/" not in path or path.endswith("/"):
                continue
            destination = output / "volume" / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(b"".join(volume.read_file(path)))
        report["downloaded_after_app_exit"] = sorted(
            str(path.relative_to(output))
            for path in (output / "volume").rglob("*")
            if path.is_file()
        )
    report["ephemeral_volume_context_closed"] = True
    (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("output", type=Path)
    arguments = parser.parse_args()
    qualify(arguments.wheel.resolve(), arguments.output.resolve())
