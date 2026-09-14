# Lifecycle routes and ten-minute progress

Configured on 2026-09-09. Webhook values are never stored in source, experiment
specs, SQLite event payloads, reports or command-line arguments. The private route
manifest is `~/.config/research-scheduler/slack-routes.json`; it names private
mode-0600 webhook files inside the scheduler user's mode-0700 config directory.
The original `slack-webhook` file is retained as the progress destination.

| Event | Route | Trigger |
|---|---|---|
| `started` | New start webhook | First observed starting/running job, once per campaign |
| `error` | New error webhook | Entering error, or a distinct error set while already in error |
| `recovered` | Error webhook | Confirmed recovery under the existing recovery contract |
| `complete` | New completion webhook | Full campaign completion, including required publication/board stages |
| `progress` | Original webhook | Current allocation/campaign snapshot every 600 seconds |

Registration alone, queued dependencies and reused successful validation do not
consume the start event. Already active/terminal campaigns are baselined without
retroactive start messages. Unchanged error observations are not repeated. A
hardware build finishing is not campaign completion until its board stage passes.

Lifecycle messages use each scheduler database's existing durable outbox. The
event kind determines the destination at delivery time; invalid/missing credentials
leave the message pending, and transport errors retry with backoff. Explicit
`webhook_file` overrides retain their previous behavior for tests and callers.

`tools/run_slack_progress.py` is run by the enabled user service
`research-slack-progress.service`. It only reads the scientific and hardware
registries and never allocates/restarts experiments. Its separate outbox, schedule,
latest snapshot and heartbeat are under
`/home/jy/experiments/slack_notifications_v2_20260909/progress/`.

Each report includes every registered GPU and its experiment assignments, disabled
or stale-state indicators, actual active hardware job/executor assignments, and
campaign completion/running/resource-wait/dependency-wait/error counts. Campaigns
can overlap, and GPU rows describe scheduler reservations, not all external use.
Long reports split into bounded messages without dropping GPU rows.

The first snapshot is sent immediately, then the schedule advances by 600 seconds.
The persisted schedule prevents a restart from duplicating an already enqueued
snapshot. Failed progress delivery is retried; a newer snapshot supersedes older
undelivered progress to avoid a burst of stale reports. Lifecycle alerts are never
superseded this way.

Validation: all three lifecycle destinations returned success to explicitly labeled
configuration-test messages. The first actual progress report was delivered at
2026-09-09 03:14 KST. It contained 52 GPU rows, 16 campaign summaries and five
hardware campaigns. Notification route/start/deduplication tests and hardware
pipeline tests passed. Only controller parents were reloaded; GPU scientific
children, Vivado builds and board attempt specifications were preserved.
# Progress table rendering (2026-09-09)

The ten-minute progress publisher now sends native Slack Block Kit tables for GPU
allocations, campaign counts, hardware allocations, and hardware campaign stages.
Each message contains one table, a timestamp and summary, and a complete plain-text
accessibility fallback. Job names retain literal underscores. Empty/disabled GPUs,
stale telemetry flags, temperatures, and all campaign counts remain visible.
Tables paginate at 100 rows (including repeated headers) or 9,000 cell characters,
below Slack's 10,000-character limit; no rows are silently truncated.
The existing progress route and durable delivery retry/deduplication remain in use.
No image hosting or additional Slack credentials are required.

Deployment verification: `progress-3-0` through `progress-3-3` were each accepted
by the existing incoming webhook on the first attempt (`sent`). The 15 notification
tests passed, including table pagination, complete GPU coverage and secret redaction.
Only `research-slack-progress.service` was restarted; scientific workers and hardware
controllers were not restarted or modified.
# Confirmed recovery policy (2026-09-11)

Individual training restarts also emit `recovered` on the error route. A selected
scientific train job must have an earlier failed attempt (or a failed explicit
predecessor), and its newest attempt must be `running` with `report.ready=true`.
Queued/starting retries and historical ready flags do not qualify. Each restarted
attempt is deduplicated per campaign, independent of waiting evaluations,
cancelled preparation jobs, or other campaign errors. Newly confirmed attempts
in the same observation cycle are grouped into **one message per campaign**,
with the recovered count and job/server list (11 recovered jobs means one message,
not eleven). A later newly failed/restarted attempt can enter a new recovery batch.
Previously sent individual recovery records are not replayed by this upgrade.
Messages explicitly distinguish training
recovery from full-campaign recovery/completion. Same-poll campaign recovery is
not duplicated when individual training recovery messages have just been queued.
On upgrade, only currently pending recovery targets are caught up; historical
running retries are baselined without replay. Existing outbox retry/route rules
and experiment/attempt history remain unchanged.

Confirmed recovery emits `recovered` to the same private route as `error`.
Queueing a retry alone is not recovery: affected jobs (or explicit replacements)
must be confirmed running/ready or successful and the campaign error cleared.
Recovery delivery failures remain in the durable outbox and retry. Previously
superseded historical alerts are not replayed. Direct completion emits completion.
Resolved pending error alerts are still superseded to avoid stale error delivery.
Each ten-minute progress batch prefixes only its first message with `========`.
Hardware tables include the latest 15 visible campaigns by original registration
time, omit the RTL column, and omit GPU/model summary boilerplate. RTL validation
and historical records are unchanged.

Additional periodic-display changes (2026-09-11): hardware running states are
`building`/`testing`; the auxiliary/external/additional-state section is omitted.
The six explicitly hidden campaigns (`cssa-main`, `cssa-anchor`, `cssa-context`,
`cssa-followup`, `picodet-s-cssa60`, `cssa-recovery`) are omitted from periodic
campaign tables only. Disabled GPUs are omitted and the GPU-count denominator
uses visible GPUs. None of these filters cancel jobs, delete history, or silence
individual lifecycle events.

Webhook sending is serialized by a dedicated per-database notification flock.
HTTP success acknowledgments use short SQLite transactions without reacquiring the
scientific registry flock, preventing lock contention from losing a successful send
and triggering duplicates. The webhook protocol still cannot promise exactly-once
delivery if a process crashes between Slack accepting a request and the local commit.
Regression tests cover contention after successful HTTP delivery, concurrent sender
exclusion, confirmed recovery retry, stale error retirement, and new error alerts.
# Completion duplicate correction (2026-09-09)

The scientific controller was importing the separate
`farm89_gui2_migration_20260909/scheduler/src` deployment, not repository code.
Restarting it without updating that notification module left the old post-send
registry-flock acknowledgment active. The SIX-MLP completion outbox row reached
44 attempts while remaining `sending`; the user confirmed repeated receipt.
That exact row was retired as `superseded` with an explanatory reason, not falsely
marked HTTP-confirmed `sent`. Scientific results and job states were not changed.

Notification-only modules are now synchronized in the actual scientific, drain,
and hardware controller deployments. The deployment utility resolves each actual
import path (including script-inserted paths), verifies its SHA256 against the
fixed source before restart, and records the path/hash in `CONTROLLERS.json`.
Completion has one durable outbox record per campaign ID, even if observations
temporarily leave and return to complete. Existing delivery failures retry that
record; confirmed old duplicates stay retired. Recovery remains silent.
# Typed progress counts (2026-09-10)

## Automatic registration timestamps (2026-09-10)

Every scheduler campaign already receives a server-generated `campaigns.created`
timestamp on its first registration. Registration responses, campaign-status and
read-only overview now expose `registered_at` (Unix), `registered_at_utc` and
`registered_at_kst` (ISO 8601). They are output metadata, not required input-spec
fields. Re-registration/HF updates preserve the original database timestamp.
Existing campaigns use their recorded timestamp without migration or backdating.

Slack progress automatically appends `-DDd-HHhMMm` (fixed Korean time, 24-hour clock) inside the
campaign-name cell. IDs and the existing train/eval column order are unchanged.
Example display: `context-graph18-10d-06h21m`; the underlying ID stays `context-graph18`.
The year, month, seconds, `KST` suffix and `등록` prefix are omitted from display;
the full stored registration timestamp and machine-readable fields are preserved.
Hardware campaign displays use the same field from their scheduler database.
Unknown historical registration dates are not inferred from STATE modification
time, execution start, completion, or the current clock. Registration does not
trigger a scientific-start alert. Completion-age filtering remains independent.

Regular progress tables and the read-only overview separate model campaigns into
`train`, `eval`, and support rows, with a total and lifecycle/waiting counts per
type. Jobs explicitly configured with `smoke_only` are support, not completed
scientific training. Prepare/analysis/checkpoint publication are also support;
adding publication jobs no longer inflates the displayed train denominator.
External cohorts without individual job types stay explicitly unclassified.
Hardware allocations show `build` (rtl_build/rtl_ooc) or `test` (rtl_sim/board_test).
Hardware campaign tables distinguish test: RTL and test: board, preserving the
requirement that build success alone is not campaign completion.
These are presentation fields; stored job kinds, dependencies, scheduling and
existing completion notifications are unchanged.
