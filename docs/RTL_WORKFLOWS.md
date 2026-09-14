# RTL workflows: one scheduler, separate resource and validation policies

This extension adds CPU-only RTL work without changing legacy GPU defaults.
It does **not** register production servers, adopt running tmux/Vivado jobs,
restart a controller, create a live DB, or program a board. The supplied node
template is disabled and uses placeholder paths, hashes and capacities.

## Job contracts

| Kind | Resource contract | Successful completion requires |
|---|---|---|
| `rtl_sim` | `gpu_count: 0`, local filesystem | Declared PASS marker and no ERROR/FATAL line in its log |
| `rtl_ooc` | CPU/RAM/disk; large OOC must request a build slot | Nonempty routed design, no routing errors, setup/hold/pulse-width closure |
| `rtl_build` | Above plus `build_slots: 1` | OOC gates plus a nonempty bitstream |
| `board_test` | Capacity-one `board.<id>` token and registered gateway | All declared capture files match expected SHA256 |

Each RTL job must have `max_attempts: 1`, `failover_safe: false`, and
`filesystem: local`. Registration is not permission to retry a programmed
board. A deliberate retry should be a reviewed new attempt/lineage.
All validation artifacts must also be declared in `outputs`; the runner
hashes them for dependency receipts. Exit 0 with negative WNS, negative WHS,
negative WPWS, missing reports, incomplete routes or bad captures is failure.
The controller also rejects a success receipt missing the RTL validation.

The Vivado parser targets the text summary format containing overall
WNS/TNS/WHS/THS/WPWS/TPWS and routable/fully-routed/error net counts. Unknown
formats fail closed. These gates do not replace constraint coverage review,
DRC review, tool/part/source identity, clock readback or detector accuracy
validation. OOC timing is not full-system timing or a maximum-frequency claim.

## Capacity and host aliases

Nodes may declare `rtl_build_slots`, while jobs request `build_slots`.
Count one complete synthesis/place/route lineage as one slot, not one slot
per Vivado subprocess. CPU simulations and small OOC jobs need not occupy a
full-build slot. Node `max_jobs` remains the total attempt cap.

`physical_host` groups container aliases on the same machine. CPU and RAM
reservations, unknown-attempt holds, full-build slot consumption, and the
30-second build startup stagger span that group. Aliases must declare
consistent CPU/RAM limits; build-capable aliases must agree on build limits.
GPU jobs on an alias with no RTL capability remain eligible subject to the
shared CPU/RAM reservations. No GPU usage threshold is applied to CPU jobs.

Register all aliases sharing physical CPU/RAM under the same host ID before
enabling RTL scheduling. Reservations only cover work known to this DB.
Existing external GPU jobs are reflected in telemetry, but unregistered
Vivado builds do not magically consume build slots: reconcile them or wait
for them to finish before allowing new starts.

Job `disk_mib` reserves additional free space conservatively on the target
node. Active requested disk is not subtracted using an estimated consumption
curve; this can over-reserve, deliberately. Shared filesystems exposed through
multiple node IDs require an additional operator policy; disk reservations
are node-scoped, not a cross-filesystem capacity service.

## Named resources

Node `tokens` advertises capacities, and job `resources.tokens` requests them:

```json
{"tokens": {"license.site-vivado": 2, "board.cps1": 1}}
```

The same name identifies one pool across the entire scheduler DB, even on
different execution nodes. Capacities must agree across inventory entries.
The planner reserves tokens for all tentative placements and active/unknown
attempts. Unknown work is not released just because SSH recovery timed out.
Real license availability still needs a tool preflight; a token is only the
configured concurrency budget, not a license-server query.

Independent scheduler DBs do not share token reservations. Do not run two
DBs over the same resource inventory.

## Preflight and immutable execution

`rtl_ooc`, `rtl_build` and `board_test` require `preflight_argv`. It is an argv
array, not implicit shell text. The agent runs it before the workload with
its own log and heartbeat. Nonzero exit prevents workload launch. A preflight
that exceeds 60 seconds or leaves live descendants becomes `unknown`, holding
the reservation; the scheduler does not kill or blindly relaunch it.

Provide a trusted read-only preflight wrapper that checks the exact Vivado
version, part, license availability, local filesystem, available space and
source-manifest referenced-file hashes. For boards, additionally verify the
hw_server endpoint, expected single target/device identity and absence of
conflicting legacy programming processes. Pin the wrapper/source manifest
through `input_files` or verified `assets`. The generic scheduler cannot
infer these site-specific contracts from a command name.

The workload must remain in the foreground and wait for all tool children.
Use `RS_ATTEMPT_DIR` or `{attempt_dir}` for a fresh build/output directory.
Do not invoke an existing `nohup ... &` controller as if its exit were the
build's completion. Do not edit a running attempt's source snapshot.

## Board gateway and interoperability

Every physical board has exactly one registered execution gateway. Its node
declares, for example:

```json
{
  "tokens": {"board.cps1": 1},
  "board_locks": {"cps1": "/data/TT/jy/experiments/zu15eg_board_programming.lock"}
}
```

A board job declares `board_id: cps1` and requests `board.cps1: 1`. Build
executors may be FARM/CPS nodes, but board jobs run at this central gateway,
not independently at each tunnel endpoint. Duplicate board gateways are
rejected. Verified bitstream/input transfer to the gateway must be explicit;
an order-only dependency is not permission to dereference another node's
local artifact path or to skip transfer checksums.

The agent holds an OS `flock` on the declared path during preflight and
execution. It waits without stealing the lock when legacy tooling owns it.
The child inherits the lock FD as `RS_BOARD_LOCK_FD`. All non-scheduler board
clients must use the same central lock/preflight; a token cannot prevent an
uncoordinated external JTAG program.

**Do not invoke the current legacy lock-taking board wrapper unchanged under
this agent lock.** Opening the same file again and taking a second exclusive
lock can deadlock against the inherited lock. Use a reviewed scheduler entry
wrapper that accepts/validates the inherited FD and runs the existing target
preflight/measurement under it, or refactor the legacy wrapper to honor that
FD in a separately approved deployment. This local extension does not modify
the main thread's existing board scripts.

## Safe rollout

1. Keep the existing GPU deployment and running experiments untouched.
2. Use a disposable local DB and disabled templates to review plans; CPU mock
   tests require neither Vivado, GPUs nor a real board.
3. Configure host IDs, limits, tokens, pinned wrappers and inventory; enable
   only new simulation/OOC jobs first.
4. Enable full builds after checking unregistered processes and storage.
5. Enable the single board gateway only after inherited-lock integration and
   verified artifact transfer are tested. Never register live jobs as new
   queued jobs to "adopt" them.

Unknown attempts remain reserved until their real state can be reconciled.
Use operator inspection/readmission when needed; do not delete DB rows or
lock files to make capacity appear free. Automatic stale-lock fencing across
independent DBs, process adoption, and unattended board retries are not
provided by this extension.

Templates: `examples/node.rtl-build.json`, `examples/experiment.rtl.json`.
Regression: `python3 -m unittest discover -s tests -v` from `scheduler/`.
