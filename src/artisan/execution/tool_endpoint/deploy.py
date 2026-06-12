"""Build the deployable Modal app for an operation's tool endpoint.

``build_app`` reads only class-level defaults — it never instantiates the
operation (params arrive per request). The HTTP surface is one
``@modal.asgi_app`` function whose FastAPI app is built in-container.

The endpoint function is **self-contained**: its closure carries only plain
data (op name, description, module path, the op's ``Params`` JSON schema,
input roles) plus the worker function handle, and its body imports only
packages installed in the slim
endpoint image (fastapi, jsonschema, modal). Artisan is never imported in
the endpoint container — unpickling any artisan object there would require
artisan's full dependency stack. Boundary validation runs against the baked
JSON schema instead. Importing this module requires the ``modal`` SDK.

No ``from __future__ import annotations`` here: the endpoint's route
handlers are cloudpickled and rebuilt in-container, where FastAPI resolves
their signatures — deferred (string) annotations cannot be looked up in an
unpickled function's globals, so annotations must be real objects.
"""

from typing import Any

import modal

from artisan.execution.tool_endpoint.spec import endpoint_spec
from artisan.operations.base.operation_definition import OperationDefinition

ENDPOINT_PYTHON_VERSION = "3.12"


def build_app(
    op_cls: type[OperationDefinition], overlay: list[str] | None = None
) -> modal.App:
    """Build the deployable Modal app for an operation's tool endpoint.

    One app per tool: a GPU **worker** (resolves the deployed op class,
    builds the command, runs the tool) behind a lightweight **endpoint**
    (FastAPI routes ``/schema``, ``/submit`` → ``/result`` → ``/download``
    → ``/cancel``, Swagger at ``/docs``). The worker runs the registry
    image as baked; in dev mode (``local_python_sources`` config or the
    ``overlay`` argument) local package sources are mounted on top,
    shadowing the image's versions. The endpoint image carries no artisan
    at all and validates request params against the op's baked ``Params``
    JSON schema.

    Args:
        op_cls: The registered operation class to deploy.
        overlay: Extra dev-mode source packages to mount, appended to the
            config's ``local_python_sources`` (the ``--overlay`` CLI flag).

    Returns:
        A deployable ``modal.App`` named ``artisan-tool-<name>``.
    """
    spec = endpoint_spec(op_cls)
    app = modal.App(f"artisan-tool-{spec.name}")

    worker_image = modal.Image.from_registry(
        spec.image, secret=_registry_secret(spec.image_registry_secret)
    ).env(spec.env)
    sources = list(dict.fromkeys([*spec.local_python_sources, *(overlay or [])]))
    if sources:
        worker_image = worker_image.add_local_python_source(*sources)
    endpoint_image = modal.Image.debian_slim(
        python_version=ENDPOINT_PYTHON_VERSION
    ).uv_pip_install("fastapi[standard]", "jsonschema")

    worker_kwargs: dict[str, Any] = {
        "image": worker_image,
        "name": "worker",  # closure-defined: explicit tag for from_name lookup
        "serialized": True,
        "retries": spec.retries,
        "min_containers": spec.min_containers,
        "volumes": _volumes(spec.volumes),
        "secrets": _secrets(spec.secrets),
    }
    for key in ("gpu", "cpu", "timeout", "max_containers", "scaledown_window"):
        value = getattr(spec, key)
        if value is not None:
            worker_kwargs[key] = value
    if spec.memory_mb is not None:
        worker_kwargs["memory"] = spec.memory_mb

    # Plain values only — anything richer in these closures must be
    # unpicklable without artisan (endpoint) at container start.
    op_module = spec.op_module
    op_qualname = spec.op_qualname
    op_name = spec.name
    op_description = spec.description
    params_schema = spec.params_schema
    input_roles = spec.input_roles

    @app.function(**worker_kwargs)
    @modal.concurrent(max_inputs=1)  # one job per container; fan out, don't pack
    def worker(request: dict[str, Any]) -> dict[str, Any]:
        from artisan.execution.tool_endpoint.protocol import ToolRequest
        from artisan.execution.tool_endpoint.server import (
            resolve_op,
            run_tool_request,
        )

        resolved = resolve_op(op_module, op_qualname)
        return run_tool_request(resolved, ToolRequest(**request)).model_dump()

    # webhook labels allow only [a-z0-9-]; op names may carry underscores
    label = f"artisan-tool-{op_name}".replace("_", "-")

    @app.function(image=endpoint_image, name="endpoint", serialized=True)
    @modal.asgi_app(label=label, requires_proxy_auth=True)
    def endpoint() -> Any:
        # Runs in the slim endpoint image: imports must resolve there, and
        # responses are plain dicts shaped like the protocol models.
        import io
        import json

        import jsonschema
        import modal as modal_rt
        from fastapi import FastAPI, File, Form, HTTPException, UploadFile
        from fastapi.responses import RedirectResponse, StreamingResponse

        web = FastAPI(
            title=f"artisan-tool-{op_name}",
            description=f"Tool endpoint for the '{op_name}' operation.",
        )

        @web.get("/schema")
        def schema() -> dict[str, Any]:
            """The request contract: params JSON-schema + input roles.

            ``params_schema`` is the same dict ``/submit`` validates
            against; an empty dict means the op declares no ``Params``
            (nothing is validated), not that the schema is unknown.
            """
            return {
                "operation": op_name,
                "description": op_description,
                "params_schema": params_schema,
                "inputs": input_roles,
            }

        @web.post("/submit")
        async def submit(
            params: str = Form("{}"),
            input_uris: str = Form("{}"),
            input_filenames: str = Form("{}"),
            output_store: str = Form(""),
            files: list[UploadFile] = File(default=[]),  # noqa: B008 — FastAPI DI idiom
        ) -> dict[str, str]:
            """Submit a tool job: params JSON + input files (multipart).

            ``input_filenames`` (JSON, role → original file name) lets the
            worker materialize each input under its real name; omitted
            entries fall back to the role. ``output_store`` (optional) is
            an object-store prefix or presigned PUT URL — outputs are
            delivered there instead of riding the result inline.
            """
            try:
                parsed = json.loads(params)
                uris: dict[str, str] = json.loads(input_uris)
                filenames: dict[str, str] = json.loads(input_filenames)
                if params_schema:
                    jsonschema.validate(parsed, params_schema)
            except (ValueError, jsonschema.ValidationError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            if output_store and "://" not in output_store:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "output_store must be an object-store prefix "
                        "(s3://…) or a presigned PUT URL (https://…)"
                    ),
                )
            refs: list[dict[str, Any]] = [
                {
                    "name": f.filename or "input",
                    "filename": filenames.get(f.filename or "input"),
                    "uri": None,
                    "data": await f.read(),
                }
                for f in files
            ]
            refs += [
                {
                    "name": name,
                    "filename": filenames.get(name),
                    "uri": uri,
                    "data": None,
                }
                for name, uri in uris.items()
            ]
            # async variant — submit runs on the event loop; the blocking
            # spawn would stall every concurrent request on this container
            call = await worker.spawn.aio(
                {
                    "params": parsed,
                    "inputs": refs,
                    "output_store": output_store or None,
                }
            )
            return {"call_id": call.object_id}

        def _retained(call_id: str) -> dict[str, Any] | str:
            """The worker's retained result dict, or a non-done status string."""
            fc = modal_rt.FunctionCall.from_id(call_id)
            try:
                raw: dict[str, Any] = fc.get(timeout=0)
            except TimeoutError:
                return "pending"
            except modal_rt.exception.OutputExpiredError:
                return "expired"
            return raw

        @web.get("/result")
        def result(call_id: str) -> dict[str, Any]:
            """Poll a job: pending/done/failed/expired + the control manifest."""
            raw = _retained(call_id)
            if isinstance(raw, str):
                return {"status": raw, "manifest": None}
            manifest = raw["manifest"]
            status = "failed" if manifest.get("error") else "done"
            return {"status": status, "manifest": manifest}

        @web.get("/download")
        def download(call_id: str) -> Any:
            """Stream the output tar, or redirect to its presigned store URL."""
            raw = _retained(call_id)
            if isinstance(raw, str):
                raise HTTPException(status_code=404, detail="no output tar for call")
            stored = (raw.get("manifest") or {}).get("stored")
            if stored is not None:
                if stored.get("presigned_url") is None:
                    raise HTTPException(
                        status_code=409,
                        detail="outputs were delivered to a caller-supplied "
                        "destination; fetch them there",
                    )
                return RedirectResponse(stored["presigned_url"], status_code=307)
            if raw.get("output_tar") is None:
                raise HTTPException(status_code=404, detail="no output tar for call")
            return StreamingResponse(
                io.BytesIO(raw["output_tar"]), media_type="application/x-tar"
            )

        @web.post("/cancel")
        def cancel(call_id: str) -> dict[str, bool]:
            """Cancel a running job, terminating its container."""
            modal_rt.FunctionCall.from_id(call_id).cancel(terminate_containers=True)
            return {"cancelled": True}

        return web

    return app


def _registry_secret(name: str | None) -> Any:
    """Modal Secret for private-registry pulls, or None."""
    return modal.Secret.from_name(name) if name else None


def _volumes(mapping: dict[str, str]) -> dict[str, Any]:
    """Mount path → Volume handles from the config's name mapping."""
    return {
        path: modal.Volume.from_name(name, create_if_missing=True, version=2)
        for path, name in mapping.items()
    }


def _secrets(names: list[str]) -> list[Any]:
    """Runtime-injected Modal Secrets from their names."""
    return [modal.Secret.from_name(name) for name in names]
