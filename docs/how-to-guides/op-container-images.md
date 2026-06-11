# Op Container Images

How to build, resolve, and consume the container image an operation runs
in. One image serves two consumers: the Modal tool endpoint
(`artisan modal deploy`) and any external harness — an RL loop, a batch
script — that pulls the same image and runs the tool's CLI directly,
with no artisan orchestration.

**Prerequisites:** [Configuring Execution](configuring-execution.md)

**Key types:** `ModalComputeConfig`; CLI commands `artisan op image`,
`artisan docker build`, `artisan modal deploy`

---

## The image-content contract

An op image is **self-sufficient**. It carries:

1. the tool binaries and their dependencies,
2. artisan, and
3. the op package(s) themselves — baked via `COPY` in the Dockerfile.

The image alone — no deploy-machine overlay — can run every op it
claims. What runs on Modal is exactly what a plain `docker run` of the
same ref runs.

Two conventions keep the contract honest:

- **Env split.** Anything the *tool* needs goes in the Dockerfile's
  `ENV`. `ModalComputeConfig.env` carries deploy-time concerns only
  (endpoints, cache-tuning flags) — never values the tool fails or
  misbehaves without, or `docker run` consumers silently diverge.
- **Layout.** Each image's Dockerfile lives at
  `docker/<image-name>/Dockerfile`, where `<image-name>` is the last
  path segment of the ref (tag stripped). The ref
  `ghcr.io/dexterity-systems/artisan-worker:latest` builds from
  `docker/artisan-worker/Dockerfile`.

## Point an op at its image

The image ref lives on the op's modal config — it is the single source
of truth both consumers read:

```python
class FoldSequences(OperationDefinition):
    ...
    compute_provider = ComputeProvider(
        modal=ModalComputeConfig(image="ghcr.io/dexterity-systems/fold-tool:0.3.1")
    )
```

Ops without per-op needs use the default `artisan-worker` image.

## Build the image

From the repo root:

```bash
artisan docker build fold_sequences
```

This resolves the op's image ref, finds the conventional Dockerfile,
and runs `docker build` tagged with **exactly the ref the config
declares** — a tag is never typed twice. Push it wherever the ref
points:

```bash
docker push "$(artisan op image fold_sequences)"
```

## Resolve the image from a harness

External harnesses ask artisan instead of hardcoding refs:

```bash
IMAGE=$(artisan op image fold_sequences)
docker run --rm -v /data/weights:/weights "$IMAGE" fold --input ...
```

`--json` adds what the container expects at runtime — env the deploy
would layer on, volume mount paths, hardware:

```bash
artisan op image fold_sequences --json
```

```json
{
  "image": "ghcr.io/dexterity-systems/fold-tool:0.3.1",
  "env": {},
  "volumes": { "/weights": "fold-weights" },
  "secrets": [],
  "gpu": "A100",
  "cpu": null,
  "memory_mb": null,
  "timeout": 3600
}
```

Where Modal binds named Volumes, a harness bind-mounts local
directories at the same paths.

## Pin for production

CI publishes `artisan-worker` under two tags on every `main` push that
touches image inputs: `latest` (dev default) and `sha-<short>` (an
immutable handle). Production op configs pin an immutable tag — with op
code baked in, the image ref *is* the code version, so a harness that
records the digest has recorded exactly what ran.

## Iterate without rebuilding (dev mode)

Rebuilding and pushing per source edit is slow; for development, the
deploy can overlay local package sources onto the worker image,
shadowing the baked versions:

```bash
artisan modal deploy fold_sequences --overlay artisan --overlay mypkg
```

The overlay is per-deploy and dev-only — production deploys bake code
so the deployed endpoint matches the registry image. (Equivalently, set
`ModalComputeConfig(local_python_sources=[...])` in config; the in-tree
example ops do this because they exist to exercise in-development
artisan.)

## Verify

```bash
artisan docker build wait_tool          # builds docker/artisan-worker/Dockerfile
docker run --rm "$(artisan op image wait_tool)" bash -c 'echo ok'
```

Both commands succeeding confirms the convention end-to-end: ref →
Dockerfile → built tag → runnable container.
