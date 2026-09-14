# Read-only GPU and campaign overview

`overview` reads one coherent SQLite snapshot using `mode=ro` and `query_only`.
It never creates a database, takes the dispatcher flock, changes state, submits
jobs, resets a failed attempt, or sends a notification. No SSH is used by default.

Memory lifecycle: lineage checking uses a module-level recursive helper with an
explicit per-call cache, not a self-referencing closure retaining the full jobs
and attempts snapshot. `test_planner_memory.py` checks immediate release even
when cyclic GC is disabled. Dashboard and live-controller deployment paths remain
separate; a dashboard-only rollout does not restart the model controller.
The overview uses summary attempt reads that omit duplicated `experiment_spec`
campaign definitions at SQL projection time. Explicit full audit reads remain
available. Resource, job-spec and lineage fields are preserved; summaries must
never be used as launch requests.

```bash
research-scheduler --db /absolute/path/state.db overview --view gpu
research-scheduler --db /absolute/path/state.db overview --view campaign
research-scheduler --db /absolute/path/state.db overview --campaign cssa-anchor --live-progress
research-scheduler --db /absolute/path/state.db overview --job SIX_MLP_s1_TRAIN
research-scheduler --db /absolute/path/state.db overview --node lab4 --format json
```

- `--view all|gpu|campaign`: Markdown presentation. JSON always contains all
  sections so it is usable by dashboards and conversational status queries.
- `--node`, `--campaign`, `--job`: exact node/campaign, substring job filter.
- `--include-waiting`: expand queued/failed job reasons in Markdown. A specific
  job query always shows its reason and resource alternatives.
- `--live-progress`: bounded parallel reads of the exact active attempt's
  `run/TRAIN_PROGRESS.json` or `CHILD_STATE.json`, never a recursive disk scan.
  Default per-node timeout 12 seconds, configurable from 1 to 30 seconds.
- `--hardware-index /absolute/path/INDEX.json`: include separate hardware build
  campaigns; build completion and board validation remain distinct.

Each GPU includes scheduler assignments, campaign membership, enablement, cached
VRAM/temperature and snapshot age/staleness. No assignment does **not** mean a GPU
is physically idle: other users' processes may use it. Multi-GPU jobs appear on
each assigned GPU; the allocation summary counts distinct jobs and physical UUIDs.
Overlapping scientific/upload/recovery campaigns must not be summed as unique
work. Historical superseded failures are retained in job detail, not resurrected
as a second "unregistered project" campaign.

Campaign status counts are computed from the snapshot's current job records and
audited replacement links. Recorded campaign/transfer status has its own age.
Normal waits are split into `dependency_wait` (선행 대기: at least one declared
prerequisite is unfinished) and `resource_wait` (자원 대기: prerequisites succeeded,
waiting for resource admission/dispatch). Order-only gates count as prerequisites.
Server/read health, RAM/VRAM, placement constraints and dispatch order remain
detailed reasons within resource waiting. Existing failed/blocked states remain
separate; a queued row retains `status=queued` for lifecycle compatibility and
exposes `display_status` plus `waiting.dependencies` for the new presentation.
Queued dependency waits are shown separately from `jobs.status=blocked` errors.
Live read failure or absent hardware metadata is reported, not changed to a zero
or a fabricated success. The stored planner's result is diagnostic only; fresh
admission must still be checked by the ordinary dispatcher before actual launch.

The last 40 storage-profile change events are inspected for repeated alternating
`max_jobs` values within ten minutes. A warning identifies settings that may keep
resetting snapshots/stable-poll counters. The query reports this condition but
does not change or repair the running controller.
