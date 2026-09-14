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
from artisan.execution.tool_endpoint.transport import MAX_INLINE_BYTES
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
    data_policy = spec.data_policy
    max_inline_bytes = MAX_INLINE_BYTES

    @app.function(**worker_kwargs)
    @modal.concurrent(max_inputs=1)  # one job per container; fan out, don't pack
    def worker(request: dict[str, Any]) -> dict[str, Any]:
        from artisan.execution.tool_endpoint.protocol import ToolRequest
        from artisan.execution.tool_endpoint.server import (
            resolve_op,
            run_tool_request,
        )
        from artisan.schemas.operation_config.endpoint_policy import (
            ToolEndpointDataPolicy,
        )

        resolved = resolve_op(op_module, op_qualname)
        policy = ToolEndpointDataPolicy.model_validate(data_policy)
        return run_tool_request(
            resolved,
            ToolRequest(**request),
            data_policy=policy,
        ).model_dump()

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

        def _string_map(raw: str, field: str) -> dict[str, str]:
            """Parse a JSON string-to-string map without losing duplicate keys."""

            def _unique(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
                parsed_map: dict[str, Any] = {}
                for key, value in pairs:
                    if key in parsed_map:
                        msg = f"{field} contains duplicate role {key!r}"
                        raise ValueError(msg)
                    parsed_map[key] = value
                return parsed_map

            parsed_map = json.loads(raw, object_pairs_hook=_unique)
            if not isinstance(parsed_map, dict) or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in parsed_map.items()
            ):
                msg = f"{field} must be a JSON object mapping strings to strings"
                raise ValueError(msg)
            return parsed_map

        def _integrity_map(raw: str) -> dict[str, dict[str, Any]]:
            """Parse URI input digest and size contracts."""
            parsed_map = json.loads(raw)
            if not isinstance(parsed_map, dict):
                msg = "input_integrity must be a JSON object"
                raise ValueError(msg)
            for role, contract in parsed_map.items():
                if (
                    not isinstance(role, str)
                    or not isinstance(contract, dict)
                    or set(contract) != {"content_digest", "size_bytes"}
                    or not isinstance(contract["content_digest"], str)
                    or len(contract["content_digest"]) != 32
                    or any(
                        char not in "0123456789abcdef"
                        for char in contract["content_digest"]
                    )
                    or not isinstance(contract["size_bytes"], int)
                    or isinstance(contract["size_bytes"], bool)
                    or contract["size_bytes"] < 0
                ):
                    msg = "input_integrity values must contain digest and byte count"
                    raise ValueError(msg)
            return parsed_map

        def _file_roles(
            uploads: list[UploadFile],
            uris: dict[str, str],
            filenames: dict[str, str],
            integrity: dict[str, dict[str, Any]],
        ) -> list[str]:
            """Validate submitted roles and return multipart roles in order."""
            roles = [upload.filename or "input" for upload in uploads]
            seen: set[str] = set()
            duplicates: set[str] = set()
            for role in [*roles, *uris]:
                if role in seen:
                    duplicates.add(role)
                seen.add(role)
            if duplicates:
                msg = f"duplicate input roles: {sorted(duplicates)}"
                raise ValueError(msg)

            known = set(input_roles)
            provided = set(roles) | set(uris)
            unknown = (provided | set(filenames) | set(integrity)) - known
            if unknown:
                msg = f"unknown input roles: {sorted(unknown)}"
                raise ValueError(msg)
            required = {
                role for role, contract in input_roles.items() if contract["required"]
            }
            missing = required - provided
            if missing:
                msg = f"missing required input roles: {sorted(missing)}"
                raise ValueError(msg)
            dangling = set(filenames) - provided
            if dangling:
                msg = f"input_filenames has no matching input: {sorted(dangling)}"
                raise ValueError(msg)
            if set(integrity) != set(uris):
                msg = (
                    "input_integrity must describe every URI input and only URI inputs"
                )
                raise ValueError(msg)
            return roles

        async def _inline_refs(
            uploads: list[UploadFile],
            roles: list[str],
            filenames: dict[str, str],
        ) -> list[dict[str, Any]]:
            """Read multipart inputs without crossing the aggregate inline bound."""
            refs: list[dict[str, Any]] = []
            total = 0
            for upload, role in zip(uploads, roles, strict=True):
                data = bytearray()
                while True:
                    remaining = max_inline_bytes - total
                    chunk = await upload.read(min(1024 * 1024, remaining + 1))
                    if not chunk:
                        break
                    total += len(chunk)
                    if total > max_inline_bytes:
                        raise HTTPException(
                            status_code=413,
                            detail=(
                                "inline inputs exceed the "
                                f"{max_inline_bytes}-byte aggregate limit"
                            ),
                        )
                    data.extend(chunk)
                refs.append(
                    {
                        "name": role,
                        "filename": filenames.get(role),
                        "uri": None,
                        "data": bytes(data),
                    }
                )
            return refs

        @web.get("/schema")
        def schema() -> dict[str, Any]:
            """The request contract: params JSON-schema + input roles.

            ``params_schema`` is the same dict ``/submit`` validates
            against. A parameter-less op serves the empty-``Params`` object
            schema (``{"type": "object", "properties": {}}``) — any JSON
            object satisfies it — not an empty ``{}``.
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
            input_integrity: str = Form("{}"),
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
                if not isinstance(parsed, dict):
                    msg = "params must be a JSON object"
                    raise ValueError(msg)
                uris = _string_map(input_uris, "input_uris")
                filenames = _string_map(input_filenames, "input_filenames")
                integrity = _integrity_map(input_integrity)
                if params_schema:
                    jsonschema.validate(parsed, params_schema)
                roles = _file_roles(files, uris, filenames, integrity)
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
            refs = await _inline_refs(files, roles, filenames)
            refs += [
                {
                    "name": name,
                    "filename": filenames.get(name),
                    "uri": uri,
                    "data": None,
                    **integrity[name],
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
            except modal_rt.exception.OutputExpiredError:
                return "expired"
            except modal_rt.exception.FunctionTimeoutError:
                return "failed"
            except modal_rt.exception.TimeoutError:
                return "pending"
            except modal_rt.exception.Error:
                return "failed"
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
        def cancel(call_id: str) -> dict[str, Any]:
            """Cancel one named job and report the outcome actually observed."""
            retained = _retained(call_id)
            if retained != "pending":
                return {
                    "call_id": call_id,
                    "status": "rejected",
                    "message": "Work completed before cancellation",
                }
            try:
                modal_rt.FunctionCall.from_id(call_id).cancel(terminate_containers=True)
            except Exception as exc:
                return {
                    "call_id": call_id,
                    "status": "unknown",
                    "message": f"Cancellation failed: {type(exc).__name__}",
                }
            return {
                "call_id": call_id,
                "status": "confirmed",
                "message": "Named worker call was terminated",
            }

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
