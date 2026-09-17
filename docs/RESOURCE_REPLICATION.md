# Demand-driven resource replication

`resource_catalog` binds a resource revision to a pinned SHA256 manifest of
relative file names, byte sizes, and SHA256 values. `sources` and `destinations`
map registered node IDs to explicit host-local roots. Paths are not assumed to
identify shared storage across hosts. Never register live mutable output trees.

`register-resource manifest-spec.json` registers this identity and known source
locations. Known is not verified: ordinary CPU preparation jobs inspect the
destination and verify every source file needed for the transfer before copying.
They try another registered source if the first source cannot be verified.
New verified destinations become eligible source locations for later demand.

The scheduler matches queued train/eval jobs by the catalog's `dataset` and/or
`cwd`, attaches the immutable identity to future job metadata, and stages only
their allowed candidate hosts. Missing files travel through controller-local
64 MiB disk chunks. Local and receiving SHA checks precede final per-file SHA
verification and atomic publication. Matching files are reused; different bytes
are never overwritten. The destination marker becomes a scheduler asset only
after its verified worker receipt is reconciled. Launch input contracts bind
that marker too. Existing attempts, scientific settings, and GPU bans are intact.

The CPU coordinator has four job slots, four reserved CPUs and a 4 GiB RAM cap.
It does not reserve GPUs. Each resource has at most four outstanding preparation
jobs, failures have at most three jobs with a delay, and each worker has a one-hour
budget. Source lookup and large transfers do not run under the scheduler DB lock.
Farm local disks retain the 85% usage limit and relay disk reserve is checked.
Failed staging/chunks remain for inspection; successful temporary relay chunks
are removed only after destination checks, with their hash list in the receipt.

Inspect with:

```sh
PYTHONPATH=scheduler/src python3 -m research_scheduler --db /home/jy/experiments/research_scheduler/state.db resource-status
```

The result includes known/queued/running/ready/failed locations and existing HF
checkpoint transfer receipts. Source registration does not promise arbitrary
filesystem discovery: a new resource needs a frozen manifest and approved root
mappings. Host-specific annotation/config rewrites and framework compatibility
remain execution-profile gates; identical images/common payloads can be shared
without rewriting their bytes. Markers assume immutable managed data; this is
not a continuously running bit-rot scanner or a bidirectional filesystem mirror.

Initial live scope: SCDA/UV referenced images (125,498 files) and SCDA shared
input payloads across the eleven currently approved LAB/FARM/CPS model nodes.
Bootstrap and verification records: `/home/jy/experiments/resource_replication_20260911/`.

Execution preparation also reconciles an already successful GPU verification
when no matching queued consumer remains (for example, it started elsewhere).
This does not create new verification work without demand, change running jobs,
or bypass the pinned preparation receipt. Already-ready idle records are skipped.
LAB6 R18/BANKR4 recipe registration on 2026-09-13 is recorded separately in
`/home/jy/experiments/lab6_model_candidates_20260913/REGISTERED.json`;
registration is not GPU validation success or a scientific launch.

2026-09-14 JointUV: source-bound `execution_resource_budgets` may override
`vram_mib` with recorded original value and measurement evidence, alongside RAM
estimates. The JointUV training/validation admission estimate is 10240 MiB;
runtime recipes and active attempts remain immutable. Unstarted validation jobs
were revised before their first attempt; future validation creation uses the same
audited estimate. Candidate admission still requires actual GPU validation.
Receipt: `/home/jy/experiments/jointuv_vram_budget_20260914/APPLIED.json`.

The later 2026-09-14 global model policy supersedes per-campaign admission
estimates: `RS_MODEL_VRAM_MIB=10240` normalizes GPU train/eval/prepare resources,
resource variants and validated profile contracts consistently in placement,
launch requests and artifact staging. Existing frozen attempts stay unchanged.
Explicit terminated GPU OOM raises only that job's next reservation by 2048 MiB;
host-RAM OOM and ambiguous exit codes do not raise GPU memory. Normal temperature,
actual memory, ownership and bounded alternate-host retry gates remain active.

For the measured SJUV/SI16C attempts on CPS1, LAB1, FARM6 and FARM7, job metadata
`vram_admission_overrides` records a 10240 MiB accounting estimate tied to the
attempt ID and original reservation. It does not change the running request.
Planning and HF destination staging use it only with a ready running attempt,
heartbeat within 120 seconds, and attributed live VRAM at or below the estimate.
Missing ownership or growth restores the original reservation; external VRAM and
temperature limits remain counted. Evidence:
`/home/jy/experiments/shared_vram_audit_20260914/EVIDENCE.json`.

2026-09-14 dispatch: the persistent model loop permits up to eight launches in
a 20-second launch phase (previously five seconds). Candidate hosts are probed
together; that batch's measurements may be reused for ten seconds while every
new reservation is included in replanning. Expired observations are refreshed,
and startup-group staggering, temperature, GPU ownership and slot limits remain
enforced. The 20-second budget is not a promise of a 20-second whole cycle.

General train/eval jobs and GPU validations use the same batch dispatcher:
up to eight placements from one simulation are reserved atomically, then their
launch RPCs run concurrently. SQLite writes remain on the controller thread;
an uncertain ACK retains that attempt's reservation for reconciliation.
Runnable consumers sort ahead of additional GPU execution validations, allowing
a successful host validation to hand capacity to its experiments while other
hosts continue validation. Dependencies and per-node success receipts still gate
each consumer independently; no fleet-wide validation barrier is introduced.
# Dispatch request preparation (2026-09-14)

Model VRAM normalization also skips deep-copying an already normalized job on
each candidate-host check. All resource variants, original resource contracts,
and per-host contracts must match the current policy (including GPU OOM
increases) before this read-only fast path is used; changed jobs still receive
an independent deep copy. This does not bypass runtime validation or admission.

Bootstrap-bank inline training revision (2026-09-14): future queued, never-started
BCB train jobs run the unchanged frozen worker's job-specific validation and
then training inside one attempt/GPU lease. The prior smoke's actual input
dependencies are inherited; its remote result dependency is replaced by the
new attempt's local preflight receipt. Original experiment definitions and
attempt histories remain intact, with before/after job revisions in events.
Validation failure prevents training; score/statistics remain separate jobs.
Host profile admission must preserve the inline wrapper on newly admitted hosts.
The controller additionally reconciles existing attempts after dispatch work,
excluding just-launched attempts to preserve lost-start-ack recovery semantics.

2026-09-14 operator removal: the model manager no longer invokes the legacy
CSSA intermediate-checkpoint discovery/publication hook. No low-frequency
fallback is scheduled. The service launcher strips its retired publisher flags;
existing publication jobs, dependencies, checkpoint files and audit history are
preserved. The model manager now admits up to 24 launches per cycle, retaining
the existing per-GPU caps and all admission/lease checks.

2026-09-15: production model management uses `python -m research_scheduler.daemon`
with the same model DB and dispatcher lock. Deployment is
`/home/jy/experiments/research_scheduler/releases/controller-20260915/src`;
runtime state/timings are under `research_scheduler/controller_runtime` and
the log is `research_scheduler/controller.log`. The systemd service retains
15-second target cadence, 24-launch limit, VRAM/HF policy, and process-only stop.
Legacy SC polling, host remapping, per-experiment release replacement, and
repeated policy reapplication are not invoked. Existing registered policy and
active attempts remain authoritative. Old migration code is historical only.

Execution preparation compares the exact desired host overlay and resource
contract before rebuilding/revalidating an unchanged job. The comparison is
per-call (not a persistent cache), includes inline-validation wrappers, resource
variants and GPU-count mapping, and does not skip changed admission validation.

FARM6 bootstrap admission (2026-09-15): `s-bootstrap-bank-farm6-inline-v1`
matches train mode plus the exact SHA256 of the inline-validation wrapper.
CPU copy/input/runtime verification remains mandatory. GPU verification occurs
inside each admitted training attempt, including frozen operator tests scoped
to its method/route and the existing train/resume/eval smoke. TF32 is disabled
for this host profile without changing tolerance thresholds. Score/statistics
and non-inline jobs cannot use this partial qualification. Failed old global
validation records and previously started requests are not rewritten.

The model controller prepares each dispatch batch from a shared, selective
request context: selected jobs, their experiments, node inventory, and only
their dependency attempts. Dependency reads omit duplicated frozen experiment
definitions, while each new launch still records its full original experiment
specification for audit. No completed or running attempt is rewritten.

If request construction outlasts resource snapshot validity, the controller
refreshes affected nodes and replans once before retrying the batch. A second
expiration yields without a reservation. The 60-second freshness policy, health
and capacity checks, and reserve-before-RPC ordering remain intact.
