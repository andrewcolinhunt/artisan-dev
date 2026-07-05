"""Compute routing configuration models.

Mirrors the ``Environments`` pattern: named providers with an active
selector. Pipeline-level overrides change ``active`` via ``model_copy()``.
"""

from __future__ import annotations

from pydantic import BaseModel, Field, field_validator

ARTISAN_WORKER_IMAGE = "ghcr.io/dexterity-systems/artisan-worker:latest"


class ComputeConfig(BaseModel):
    """Base class for compute provider configs.

    Mirrors the ``EnvironmentSpec`` hierarchy — each provider
    extends this base and ``create_execute_router()`` dispatches by type.
    """


class LocalComputeConfig(ComputeConfig):
    """Local compute provider (default)."""


class ModalComputeConfig(ComputeConfig):
    """Provider-specific configuration for routing the execute phase to Modal.

    Hardware fields (gpu / cpu / memory_gb / timeout) live on
    ``ComputeResources`` so the same hardware spec can apply to any
    compute provider; this class carries Modal-specific non-hardware
    concerns only.

    Attributes:
        image: Container image for the Modal function — a registry ref.
            The image is the single source of truth for running the op
            anywhere (Modal worker or plain ``docker run``): tool
            binaries, dependencies, artisan, and the op's own code are
            baked in, by convention from
            ``docker/<image-name>/Dockerfile``. Pin an immutable tag
            (version or ``sha-<short>``) in production; CI publishes
            ``latest`` plus ``sha-<short>`` for ``artisan-worker``.
        retries: Number of retries on preemption.
        min_containers: Containers kept warm even at zero traffic.
            Set to match expected batch parallelism to eliminate
            cold starts. 0 means scale-to-zero (Modal default).
        max_containers: Upper bound on concurrent containers. None uses
            Modal's workspace-level default. Set when fanning out via
            ``experimental_spawn_map()`` to avoid spawning one
            container per input on large batches.
        scaledown_window: Seconds a container idles before shutdown.
            None uses Modal's default (60s). Max 1200s.
        image_registry_secret: Name of a Modal Secret (created via
            ``modal secret create ...``) carrying ``REGISTRY_USERNAME``
            and ``REGISTRY_PASSWORD`` for pulling private images. None
            (default) pulls without authentication; set when ``image``
            points at a private registry.
        secrets: Names of Modal Secrets to inject into the container
            environment at runtime (e.g. ``["hf-read", "aws-s3"]``).
            Created via ``modal secret create ...``. Distinct from
            ``image_registry_secret``, which authenticates the image
            pull only.
        volumes: Mount path → volume name mapping
            (e.g. ``{"/weights": "foundry-weights"}``). Each volume is
            resolved via
            ``modal.Volume.from_name(name, create_if_missing=True,
            version=2)``. Use for model weights and other caches that
            should survive across cold starts.
        env: Environment variables to set inside the container
            (e.g. ``{"HF_XET_HIGH_PERFORMANCE": "1"}``). Applied as
            an image layer so cache hits survive as long as the dict
            is stable. Deploy-time concerns only — anything the tool
            itself needs must live in the image's Dockerfile ``ENV``,
            or a plain ``docker run`` of the same image diverges from
            the Modal deploy.
        local_python_sources: Dev-mode source overlay: top-level Python
            package names shipped live from the deploy machine via
            ``modal.Image.add_local_python_source``, shadowing whatever
            versions the image baked. Default ``[]`` — op code is baked
            into the image, so what runs on Modal is exactly what the
            image carries (and what a plain ``docker run`` would run).
            Set package names (e.g. ``["artisan", "pipelines"]``) or
            pass ``artisan modal deploy --overlay`` to iterate without
            rebuilding the image; never rely on the overlay in
            production. Mounted at cold-start rather than baked —
            upload bandwidth scales with total source size.
        endpoint_url: Base URL of an externally-deployed tool endpoint.
            None (default) resolves the Artisan-deployed app
            ``artisan-tool-<op.name>`` via the Modal SDK; set this to
            consume an endpoint Artisan did not deploy.
        auth_secret: Variable-name prefix for the proxy-auth token pair
            (``<prefix>_TOKEN_ID`` / ``<prefix>_TOKEN_SECRET``), sent as
            ``Modal-Key`` / ``Modal-Secret`` headers. None uses the
            ``MODAL_PROXY`` prefix (``MODAL_PROXY_TOKEN_ID`` /
            ``MODAL_PROXY_TOKEN_SECRET``). Tokens are dashboard-created
            proxy-auth tokens, discovered from the process environment or
            the nearest ``.env`` file (see ``.env.example``).
        poll_interval: Seconds between ``/result`` polls while a tool
            job runs.
        max_concurrent_calls: Client-side cap on concurrent endpoint
            calls per unit. The execute router fans one thread per
            artifact out to ``min(max_concurrent_calls, artifacts)``;
            excess artifacts queue and results stay positionally
            aligned. The server-side sibling is ``max_containers``.
        output_store: Object-store root URI to deliver tool outputs
            under (e.g. ``s3://bucket/prefix``), sent per request. None
            (default) returns outputs inline, bounded at 100 MB. When
            set, the worker uploads
            ``<output_store>/<op-name>/<uuid>.tar.gz`` and the manifest
            carries the URI plus a presigned GET URL (7-day expiry,
            matching Modal's result retention). Uploads run with the
            *worker's* credentials: name an AWS Modal Secret in
            ``secrets`` (e.g. ``["aws-s3"]``) and scope its IAM policy
            to the prefixes callers may target. Artisan never deletes
            delivered tarballs — pair destination prefixes with a
            bucket lifecycle policy. Presigned PUT URLs are rejected
            here: they name one object and expire, so they are
            per-request wire data for external callers, never static
            config.
    """

    image: str = ARTISAN_WORKER_IMAGE
    retries: int = 3
    min_containers: int = 0
    max_containers: int | None = None
    scaledown_window: int | None = None
    image_registry_secret: str | None = None
    secrets: list[str] = Field(default_factory=list)
    volumes: dict[str, str] = Field(default_factory=dict)
    env: dict[str, str] = Field(default_factory=dict)
    local_python_sources: list[str] = Field(default_factory=list)
    endpoint_url: str | None = None
    auth_secret: str | None = None
    poll_interval: float = Field(default=2.0, gt=0)
    max_concurrent_calls: int = Field(default=64, gt=0)
    output_store: str | None = None

    @field_validator("output_store")
    @classmethod
    def _reject_presigned_put(cls, value: str | None) -> str | None:
        """Constrain ``output_store`` to object-store prefixes."""
        if value is not None and value.startswith(("http://", "https://")):
            msg = (
                "output_store must be an object-store prefix (s3://…); a "
                "presigned PUT URL names one object and expires — it is "
                "per-request wire data for external callers, not static config"
            )
            raise ValueError(msg)
        return value


class ComputeProvider(BaseModel):
    """Multi-provider compute routing configuration.

    Follows the ``Environments`` pattern: named providers with an
    active selector. Pipeline-level overrides change ``active``
    via ``model_copy()``.

    Attributes:
        active: Name of the currently selected provider.
        local: Local compute provider config (always available).
    """

    active: str = "local"
    local: LocalComputeConfig = Field(
        default_factory=LocalComputeConfig,
    )
    modal: ModalComputeConfig | None = None

    @field_validator("active")
    @classmethod
    def _validate_active(cls, value: str) -> str:
        """Constrain ``active`` to the known provider names."""
        if value not in ("local", "modal"):
            msg = f"Unknown compute provider {value!r}; expected 'local' or 'modal'"
            raise ValueError(msg)
        return value

    def current(self) -> ComputeConfig:
        """Return the active provider config.

        Raises:
            ValueError: If the active provider is not configured.
        """
        config: ComputeConfig | None = getattr(self, self.active, None)
        if config is None:
            msg = (
                f"Compute provider '{self.active}' is not configured. "
                f"Available: {self.available()}"
            )
            raise ValueError(msg)
        return config

    def available(self) -> list[str]:
        """Return names of configured providers."""
        return [name for name in ("local", "modal") if getattr(self, name) is not None]
