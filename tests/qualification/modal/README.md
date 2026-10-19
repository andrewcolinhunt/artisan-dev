# Reviewed remote cancellation qualification

These opt-in tests target a separately deployed, owned temporary app. They never
create an app, redeploy an existing application, or modify the shared WaitTool app.

The synthetic operation reuses the production endpoint builder and worker,
overlays both the complete Artisan framework and its one probe module, and
launches only bounded local processes inside its dedicated remote container.

The exact temporary app is `artisan-tool-managed_cancellation_probe`:
CPU 1, memory 1 GiB, max 3/min 0 workers, no GPU, timeout 180 seconds, retries 0,
scale-down 15 seconds. There are no secrets, volumes, bucket writes, external
input URIs, or registry image publication. The base is the existing
`ghcr.io/dexterity-systems/artisan-worker:latest` image. Modal may internally
materialize private images and uploads the reviewed source overlays.

Eight tests cover stubborn parent/child closure, independent sibling survival,
pre-command and queued cancellation, repeated cancellation, natural completion,
lost cancellation acknowledgements, races with natural completion, the real
client output boundary, and actual
pipeline persistence of completed source artifacts without cancelled outputs.
At most 18 calls may be submitted; every command is finite. The driver bounds
the complete test process to 900 seconds, then stops the exact newly created
app ID even on initialization, schema, digest, or test failure. The driver is
the supported live entry point; do not directly run pytest against a live app.

The harness retains exact call/task IDs, source digests, provider terminal input
and task records, acknowledgement/termination timings, JUnit, and cleanup
status. `TaskGetInfo` is deliberately test-only. Neither a passing timing sample
nor stopped logs establishes a general synchronous cancellation guarantee.
The public cancellation response must remain `unknown`; positive provider
termination is a separate qualification observation. For queued calls, the
harness releases its occupied workers before requiring a terminal input record.
A natural-completion race may finish successfully after an accepted cancellation
request; that outcome does not change the client's `unknown` acknowledgement.

## Local preparation (does not require deployment permission)

From the repository root, freeze the reviewed candidate immediately before
deployment so concurrent edits cannot change uploaded sources:

```sh
~/.pixi/bin/pixi run --locked -e dev python tests/qualification/modal/prepare_snapshot.py --snapshot /tmp/artisan-remote-qualification-candidate
```

The snapshot contains only framework sources and this synthetic harness.
Record its `snapshot.json` and use this frozen path for both deployment and
execution. A later source revision requires a new snapshot and qualification.

## Explicit deployment (requires user authorization)

```sh
PYTHONPATH=/tmp/artisan-remote-qualification-candidate/src:/tmp/artisan-remote-qualification-candidate/harness ~/.pixi/bin/pixi run --locked -e dev python /tmp/artisan-remote-qualification-candidate/harness/deploy_probe.py --deploy-reviewed-probe --manifest /tmp/artisan-remote-qualification-candidate/deployment.json
```

The deployer refuses an existing app with the same name. It never deploys the
shared WaitTool app. On deployment failure it obtains the exact newly created
app ID from the SDK's retained deployment context and stops that app before
raising. The manifest retains this cleanup outcome, including a failed stop.

## Live qualification and unconditional exact-app cleanup

```sh
PYTHONPATH=/tmp/artisan-remote-qualification-candidate/src:/tmp/artisan-remote-qualification-candidate/harness ~/.pixi/bin/pixi run --locked -e dev python /tmp/artisan-remote-qualification-candidate/harness/run_qualification.py --manifest /tmp/artisan-remote-qualification-candidate/deployment.json
```

Do not run broad `test-modal` or `test-modal-endpoint`: those include a fixture
that redeploys the existing WaitTool app and uses real object storage.
