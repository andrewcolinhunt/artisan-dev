# Managed Command Platform Proof

Run this test-only prototype to qualify the operating-system primitives for
managed command cancellation. It does not change Artisan's production command
launcher or cancellation behavior. Passing this stage is required before the
supervisor is integrated into production paths.

## Run the proof

From the repository root, on a native Linux or macOS host:

```bash
pixi run --locked -e dev python -m pytest tests/artisan/utils/test_command_supervisor.py -q -ra
```

The prototype is in `tests/fixtures/command_supervisor_proof.py`. It uses real
processes, process groups, Unix descriptor passing, and the host's exit-observation
primitive. Mocks and architecture emulation do not qualify a release platform.

Keep the first failing result and its process traces. Diagnose the failure and
revise the mechanism before rerunning; a later pass does not explain an earlier
ownership failure. Do not substitute worker exit, leader exit, pipe EOF, or signal
delivery for proof that the managed group is gone.

## Collect platform evidence

The `command-supervisor-proof` job in
[CI](../../../.github/workflows/ci.yaml) runs on every configured CI push,
pull request, or manual dispatch. Its matrix uses these native
[GitHub-hosted runners](https://docs.github.com/en/actions/reference/runners/github-hosted-runners):

| Locked platform | Runner label | Native architecture |
| --- | --- | --- |
| `linux-64` | `ubuntu-24.04` | x86-64 |
| `linux-aarch64` | `ubuntu-24.04-arm` | ARM64 |
| `osx-arm64` | `macos-15` | ARM64 |

Each job records the checked-out revision, OS release, kernel, architecture,
Python version, Pixi version, and lockfile hash. It checks the Python process's
architecture so an unexpected runner or translated interpreter fails the job.
The `command-supervisor-proof-<platform>` artifact retains this metadata,
`pytest.log`, JUnit results, and pytest temporary files, including after test
failure. If setup fails or the test process is interrupted before reporting,
JUnit results may be absent; that run is not qualification evidence.

For a local qualification run, retain the same metadata and use a fresh evidence
directory for `--junitxml`, `--basetemp`, and captured pytest output. Record the
exact test command and exit status. Do not overwrite a previous failing run's
evidence. Keep release evidence beyond the CI artifact retention period.

## Interpret the result

All three platform jobs must pass against the same revision. Successful cleanup
and conservative unknown outcomes both need real-process coverage. Linux uses
wait-without-reap plus supervisor-local subreaping; macOS uses the exact child's
`kqueue` exit event and a final exact wait. macOS must demonstrate positive
cleanup as well as failure handling.

The runner label does not define a minimum supported OS release. Record the
actual release tested, and qualify any additional release targets before claiming
support for them. A Linux pass does not qualify macOS or another architecture.

This proof does not qualify the complete cancellation design. Production
integration still needs creator, curator, standalone, replay, nested-command,
endpoint, deadline, publication, and secret-protection regressions plus independent
review. Pixi wrapping, Docker, Apptainer, remote providers, and destruction of the
supervisor have separate qualification boundaries. Ordinary Docker/MinIO test
passes do not establish container cancellation ownership.
