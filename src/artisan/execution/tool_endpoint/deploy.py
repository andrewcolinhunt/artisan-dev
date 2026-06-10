"""Build the deployable Modal app for an operation's tool endpoint.

``build_app`` reads only class-level defaults — it never instantiates the
operation (params arrive per request). The HTTP surface is one
``@modal.asgi_app`` function whose FastAPI app is built in-container
(``_build_fastapi``), so fastapi is a dependency of the endpoint image,
not of artisan. Importing this module requires the ``modal`` SDK.
"""

from __future__ import annotations

from typing import Any, Literal

import modal
from pydantic import BaseModel

from artisan.execution.tool_endpoint.protocol import ToolRequest
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


def endpoint_spec(op_cls: type[OperationDefinition]) -> EndpointSpec:
    """Flatten the op's class-level tool + compute config into a deploy spec.

    Args:
        op_cls: The registered operation class to deploy.

    Returns:
        The deploy spec read from class-level field defaults.

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
    )


def build_app(op_cls: type[OperationDefinition]) -> modal.App:
    """Build the deployable Modal app for an operation's tool endpoint.

    One app per tool: a GPU **worker** (resolves the deployed op class,
    builds the command, runs the tool) behind a lightweight **endpoint**
    (FastAPI routes ``/submit`` → ``/result`` → ``/download`` → ``/cancel``,
    Swagger at ``/docs``). Both images mount ``local_python_sources`` so the
    worker can run ``build_command`` and the endpoint can validate request
    params against the op's ``Params``.

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
    endpoint_image = modal.Image.debian_slim(
        python_version=ENDPOINT_PYTHON_VERSION
    ).uv_pip_install("fastapi[standard]", "pydantic>=2")
    if spec.local_python_sources:
        worker_image = worker_image.add_local_python_source(*spec.local_python_sources)
        endpoint_image = endpoint_image.add_local_python_source(
            *spec.local_python_sources
        )

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

    @app.function(**worker_kwargs)
    @modal.concurrent(max_inputs=1)  # one job per container; fan out, don't pack
    def worker(request: dict[str, Any]) -> dict[str, Any]:
        from artisan.execution.tool_endpoint.server import (
            resolve_op,
            run_tool_request,
        )

        resolved = resolve_op(spec.op_module, spec.op_qualname)
        return run_tool_request(resolved, ToolRequest(**request)).model_dump()

    # webhook labels allow only [a-z0-9-]; op names may carry underscores
    label = f"artisan-tool-{spec.name}".replace("_", "-")

    @app.function(image=endpoint_image, name="endpoint", serialized=True)
    @modal.asgi_app(label=label, requires_proxy_auth=True)
    def endpoint() -> Any:
        return _build_fastapi(spec, worker)

    return app


def _build_fastapi(spec: EndpointSpec, worker: Any) -> Any:
    """Build the FastAPI app served by the endpoint container.

    Imports fastapi and modal locally — both available in the endpoint
    image; neither required on a client machine importing this module.
    """
    import io
    import json

    import modal as modal_rt
    from fastapi import FastAPI, File, Form, HTTPException, UploadFile
    from fastapi.responses import StreamingResponse
    from pydantic import ValidationError

    from artisan.execution.tool_endpoint.protocol import (
        InputRef,
        ResultResponse,
        SubmitResponse,
        WorkerResult,
    )
    from artisan.execution.tool_endpoint.server import resolve_op

    op_cls = resolve_op(spec.op_module, spec.op_qualname)
    web = FastAPI(
        title=f"artisan-tool-{spec.name}",
        description=f"Tool endpoint for the '{spec.name}' operation.",
    )

    @web.post("/submit", response_model=SubmitResponse)
    async def submit(
        params: str = Form("{}"),
        input_uris: str = Form("{}"),
        files: list[UploadFile] = File(default=[]),  # noqa: B008 — FastAPI DI idiom
    ) -> SubmitResponse:
        """Submit a tool job: params JSON + input files (multipart)."""
        try:
            parsed = json.loads(params)
            uris: dict[str, str] = json.loads(input_uris)
            params_cls = getattr(op_cls, "Params", None)
            if params_cls is not None:
                params_cls(**parsed)  # boundary validation
        except (ValueError, ValidationError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        refs = [
            InputRef(name=f.filename or "input", data=await f.read()) for f in files
        ]
        refs += [InputRef(name=name, uri=uri) for name, uri in uris.items()]
        request = ToolRequest(params=parsed, inputs=refs)
        call = worker.spawn(request.model_dump())
        return SubmitResponse(call_id=call.object_id)

    def _retained_result(call_id: str) -> WorkerResult | str:
        """The worker's retained result, or a non-done status string."""
        fc = modal_rt.FunctionCall.from_id(call_id)
        try:
            raw = fc.get(timeout=0)
        except TimeoutError:
            return "pending"
        except modal_rt.exception.OutputExpiredError:
            return "expired"
        return WorkerResult(**raw)

    @web.get("/result", response_model=ResultResponse)
    def result(call_id: str) -> ResultResponse:
        """Poll a job: pending/done/failed/expired + the control manifest."""
        res = _retained_result(call_id)
        if isinstance(res, str):
            return ResultResponse(status=res)  # type: ignore[arg-type]
        status: Literal["done", "failed"] = "failed" if res.manifest.error else "done"
        return ResultResponse(status=status, manifest=res.manifest)

    @web.get("/download")
    def download(call_id: str) -> Any:
        """Stream the output tar of a completed job."""
        res = _retained_result(call_id)
        if isinstance(res, str) or res.output_tar is None:
            raise HTTPException(status_code=404, detail="no output tar for call")
        return StreamingResponse(
            io.BytesIO(res.output_tar), media_type="application/x-tar"
        )

    @web.post("/cancel")
    def cancel(call_id: str) -> dict[str, bool]:
        """Cancel a running job, terminating its container."""
        modal_rt.FunctionCall.from_id(call_id).cancel(terminate_containers=True)
        return {"cancelled": True}

    return web


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
