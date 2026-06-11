"""Build the deployable Modal app for an operation's tool endpoint.

``build_app`` reads only class-level defaults — it never instantiates the
operation (params arrive per request). The HTTP surface is one
``@modal.asgi_app`` function whose FastAPI app is built in-container.

The endpoint function is **self-contained**: its closure carries only plain
data (op name, module path, the op's ``Params`` JSON schema) plus the worker
function handle, and its body imports only packages installed in the slim
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
from pydantic import BaseModel, Field

from artisan.operations.base.operation_definition import OperationDefinition
from artisan.schemas.operation_config.compute import (
    ComputeProvider,
    ModalComputeConfig,
)
from artisan.schemas.operation_config.compute_resources import ComputeResources

ENDPOINT_PYTHON_VERSION = "3.12"


class EndpointSpec(BaseModel):
    """Flattened class-level deploy config for one tool endpoint."""

    op_module: str
    op_qualname: str
    name: str
    image: str
    image_registry_secret: str | None
    env: dict[str, str]
    volumes: dict[str, str]
    secrets: list[str]
    retries: int
    min_containers: int
    max_containers: int | None
    scaledown_window: int | None
    local_python_sources: list[str]
    gpu: str | None
    cpu: float | None
    memory_mb: int | None
    timeout: int | None
    params_schema: dict[str, Any] = Field(default_factory=dict)


def endpoint_spec(op_cls: type[OperationDefinition]) -> EndpointSpec:
    """Flatten the op's class-level tool + compute config into a deploy spec.

    Args:
        op_cls: The registered operation class to deploy.

    Returns:
        The deploy spec read from class-level field defaults, including the
        op's ``Params`` JSON schema for boundary validation.

    Raises:
        ValueError: If the op is not a tool op (ToolSpec + build_command)
            or declares no ``compute_provider.modal`` config.
    """
    has_build_command = op_cls.build_command is not OperationDefinition.build_command
    if not has_build_command or op_cls.model_fields["tool"].default is None:
        msg = (
            f"{op_cls.__name__} is not a tool op — deploying an endpoint "
            "requires a ToolSpec + build_command()"
        )
        raise ValueError(msg)
    provider = op_cls.model_fields["compute_provider"].default
    modal_cfg = provider.modal if isinstance(provider, ComputeProvider) else None
    if not isinstance(modal_cfg, ModalComputeConfig):
        msg = (
            f"{op_cls.__name__} declares no compute_provider.modal config — "
            "set one with the worker image to deploy"
        )
        raise ValueError(msg)
    resources = op_cls.model_fields["compute_resources"].default
    if not isinstance(resources, ComputeResources):
        resources = ComputeResources()
    params_cls = getattr(op_cls, "Params", None)
    params_schema = (
        params_cls.model_json_schema()
        if isinstance(params_cls, type) and issubclass(params_cls, BaseModel)
        else {}
    )
    return EndpointSpec(
        op_module=op_cls.__module__,
        op_qualname=op_cls.__qualname__,
        name=op_cls.name,
        image=modal_cfg.image,
        image_registry_secret=modal_cfg.image_registry_secret,
        env=modal_cfg.env,
        volumes=modal_cfg.volumes,
        secrets=modal_cfg.secrets,
        retries=modal_cfg.retries,
        min_containers=modal_cfg.min_containers,
        max_containers=modal_cfg.max_containers,
        scaledown_window=modal_cfg.scaledown_window,
        local_python_sources=modal_cfg.local_python_sources,
        gpu=resources.gpu,
        cpu=resources.cpu,
        memory_mb=resources.memory_gb * 1024 if resources.memory_gb else None,
        timeout=resources.timeout,
        params_schema=params_schema,
    )


def build_app(op_cls: type[OperationDefinition]) -> modal.App:
    """Build the deployable Modal app for an operation's tool endpoint.

    One app per tool: a GPU **worker** (resolves the deployed op class,
    builds the command, runs the tool) behind a lightweight **endpoint**
    (FastAPI routes ``/submit`` → ``/result`` → ``/download`` → ``/cancel``,
    Swagger at ``/docs``). The worker image mounts ``local_python_sources``
    so ``build_command`` runs without shipping code per call; the endpoint
    image carries no artisan at all and validates request params against
    the op's baked ``Params`` JSON schema.

    Args:
        op_cls: The registered operation class to deploy.

    Returns:
        A deployable ``modal.App`` named ``artisan-tool-<name>``.
    """
    spec = endpoint_spec(op_cls)
    app = modal.App(f"artisan-tool-{spec.name}")

    worker_image = modal.Image.from_registry(
        spec.image, secret=_registry_secret(spec.image_registry_secret)
    ).env(spec.env)
    if spec.local_python_sources:
        worker_image = worker_image.add_local_python_source(*spec.local_python_sources)
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
    params_schema = spec.params_schema

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
        from fastapi.responses import StreamingResponse

        web = FastAPI(
            title=f"artisan-tool-{op_name}",
            description=f"Tool endpoint for the '{op_name}' operation.",
        )

        @web.post("/submit")
        async def submit(
            params: str = Form("{}"),
            input_uris: str = Form("{}"),
            input_filenames: str = Form("{}"),
            files: list[UploadFile] = File(default=[]),  # noqa: B008 — FastAPI DI idiom
        ) -> dict[str, str]:
            """Submit a tool job: params JSON + input files (multipart).

            ``input_filenames`` (JSON, role → original file name) lets the
            worker materialize each input under its real name; omitted
            entries fall back to the role.
            """
            try:
                parsed = json.loads(params)
                uris: dict[str, str] = json.loads(input_uris)
                filenames: dict[str, str] = json.loads(input_filenames)
                if params_schema:
                    jsonschema.validate(parsed, params_schema)
            except (ValueError, jsonschema.ValidationError) as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
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
            call = worker.spawn({"params": parsed, "inputs": refs})
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
            """Stream the output tar of a completed job."""
            raw = _retained(call_id)
            if isinstance(raw, str) or raw.get("output_tar") is None:
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
