# Scheduler code cleanup — 2026-09-26

This is a source-code maintenance audit. It does not change scheduling policy,
the state DB, or a running service.

## Findings and changes

- The package has about 9,500 Python lines. `artifacts.py`, `agent.py`,
  `planner.py`, and `controller.py` account for about 40% of them. Size alone
  is not evidence that code is unused: `agent.py` is shipped as one source file
  to remote nodes, and the other three modules are on the active daemon path.
- `artifacts.tick()` mixed transfer status reconciliation, DB job selection,
  dependency staging, retention, and optional HF publication in 336 lines.
  Status reconciliation, scoped job/attempt loading, and publication selection
  now have named helpers. The remaining orchestration is 233 lines. The
  dependency relay admission closure remains in place until its pool and
  health behavior can be isolated with more focused tests.
- A relay planning pass used to fetch active transfer reservations for each
  queued consumer and candidate. It now fetches them once per pass. The
  already-loaded group policy is also reused during transfer admission.
- `gui2_migration.py` was imported only by its test in the current package.
  It belonged to the retired Farm GUI2 migration runner, whose historical
  source remains under `experiments/farm89_gui2_migration_20260909`.
  The current package and its obsolete test no longer include it.
  A module-name reference scan found no other isolated module suitable for
  deletion without reviewing a live call path.
- The pool relay fixture still treated RP2 as train-only. It now uses FARM7
  for that case, preserving the test's intended train-only boundary. The
  deployed schema check discovers the current model service's `PYTHONPATH`
  instead of loading a hard-coded 2026-09-09 migration release.

## Remaining structural work

1. Split `artifacts.tick()` dependency relay selection and transfer admission
   into independently tested components. Preserve relay intent pinning,
   archive capacity, health gates, and HF opt-in behavior.
2. Split `planner.fit()` (about 280 lines) into host, dependency, and GPU
   admission stages. Its current pool-policy edits are still in progress in
   the working tree, so this pass did not rewrite them.
3. Keep the remote agent self-contained until the remote packaging contract
   changes; moving its functions to ordinary imported modules would break
   `Transport.call()` source injection.

## Deployment boundary

The model daemon's current user unit imports
`/home/jy/experiments/research_scheduler/releases/controller-20260920-execution-demand-v1/src`.
That release differs from the repository in several modules, including
`artifacts.py` and `planner.py`. This cleanup remains in the repository until
those changes are reconciled into a new tested release. Copying the repository
module over the running release would also replace unrelated operational fixes.

## Verification

- Artifact, archive, relay, and dispatch freshness tests: 83 passed.
- The complete repository suite initially had 558 passes and 20 failures.
  The failures assumed that an execution catalog could expand an explicitly
  restricted host list, that CPS model nodes were still eligible, or that
  RP2/FARM9/LAB1 were train-only. The fixtures now express the current host
  and pool contracts. The complete workspace suite passed 579 tests. An
  isolated scheduler checkout passed 577 tests and skips one integration test
  when the parent project's CG18 stop tool is absent.
- The running model, artifact, dashboard, hardware pipeline, physical-health,
  and CDA observer services were stopped on 2026-09-26. Their start sequence
  is documented in the README. No DB jobs or attempts were rewritten.
