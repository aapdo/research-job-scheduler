# Validation snapshot — 2026-09-06

## Automated validation

Command: `python3 -m unittest discover -s tests -v` (Python 3.10).

Standalone packaging was also verified in an isolated virtual environment:
`research_job_scheduler-0.1.0-py3-none-any.whl` built successfully and the installed
`research-scheduler --help` entry point was checked. Use the README's venv path;
the control machine's old system pip/setuptools combination produced an incorrect
empty UNKNOWN wheel, so that artifact is not a release and remains ignored.

45 tests passed, including:

- Registration idempotency, strict keys, dependency cycle/missing job detection.
- Priority backfill, same-host multi-GPU reservation, per-device VRAM, RAM/CPU admission.
- External GPU processes, disabled devices, stale telemetry, D-state, shared-start gates.
- Detached **real CPU subprocesses** with synthetic resource telemetry for deterministic tests.
- Train → evaluation dependencies and independent training execution.
- Output existence/hash receipts; exit-zero-without-output failure.
- Lost SSH acknowledgement and controller restart without duplicate execution.
- Bounded retries, immutable distinct attempt directories, input SHA mismatch.
- Literal argv without shell interpolation; dataset path passthrough.
- Four batches × three SSH recovery retries and persisted recovery scheduling.
- Continuous D-state 600-second state transition using simulated time.
- Invalid attempt late-result rejection and execution on a simulated second node.

The multi-node failover integration uses separate local directories as two simulated hosts;
it does **not** intentionally disconnect a real server. GPU admission tests use synthetic GPU
snapshots; they do not train a real CUDA model.

## Read-only real environment validation

The standalone SSH probe successfully read an existing server's three NVIDIA RTX A5000
devices, UUIDs, VRAM usage, compute PIDs, CPU/RAM and D-state. No workload was launched on
that server and no files, Docker containers or processes were changed there.

## Actual local telemetry demo

`examples/local_demo.py` registered four CPU-only dummy jobs using actual local telemetry.
The baseline completed with a valid artifact receipt. The other three jobs were held by
the D-state gate because an unrelated local filesystem task was repeatedly in D-state.
The finite-duration demo controller exited; no background scheduler was left running.
The unrelated process was not killed and the health gate was not bypassed.

Thus the deterministic DAG execution tests passed, but the separate **actual-health local
demo was 1/4 at handoff**, not reported as 4/4. Its ignored runtime state is retained locally
so rerunning the same demo command can continue without duplicating the completed baseline.

## Scope

This is an MVP, not a production cluster migration. Existing research queues are unchanged.
Real multi-host CUDA/DDP execution, long-running outage fault injection, cgroup isolation,
multi-user security, MIG/MPS, and production fleet rollout remain unverified/unimplemented
as described in the README.

## Server-specific dataset paths update (0.1.1)

The expanded suite passes **56 tests** (45 existing + 11 new). Added coverage includes
per-node logical-name/path registration, missing and inaccessible paths, stale mapping
observations, different roots for the same experiment, immutable live-attempt bindings,
CLI registration, and old-database/direct-path compatibility. A real detached CPU job
verified the resolved path in argv, nested JSON config and environment variables.
A pre-start disappearance test confirmed that the scientific child is not launched.
These tests use temporary local datasets and synthetic machine telemetry; no real
research dataset was copied, modified, registered or trained on during this update.

## Admission utilization update (0.2.0)

The expanded suite passes **63 tests**. It also covers bounded multi-launch cycles, preservation of shared-storage
cold-start serialization while local jobs fill independent slots, and future-only
GPU enablement changes while another attempt is active. Multi-launch remains
sequentially revalidated before each reservation; it does not bypass GPU ownership,
VRAM, D-state, dataset or startup-group gates.

## Explicit external-process headroom update (0.2.1)

The suite passes **66 tests**. New cases verify that an opted-in node can admit an
exclusive scheduler job beside an already-visible external process only when GPU
utilization and per-device VRAM headroom pass, while a second scheduler reservation
on the same GPU remains prohibited. Changing this node policy affects future
placement and does not mutate active attempt snapshots. A future-only GPU margin
update is also validated independently from active attempt state.
