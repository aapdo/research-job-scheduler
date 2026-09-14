# Health history versus launch freshness

LAB2 had healthy observations approximately 61–62 seconds apart while the
controller used the 60-second snapshot TTL to reset its health streak. Its
required three healthy polls could consequently never accumulate.

`policy.max_health_poll_gap_s` now defaults to 120 seconds and bounds consecutive
health-history observations independently of `max_snapshot_age_s`. Admission
still requires a snapshot no older than 60 seconds under the existing inventory,
and still requires three healthy polls. A failed health sample, gap over the
history limit, or changed boot ID resets the relevant streak. Calls less than
two seconds apart do not increment the count. GPU faults reset GPU streaks.

The scientific deployment at
`/home/jy/experiments/farm89_gui2_migration_20260909/scheduler/src` received the
same controller/schema change. No user holds, host lists, running attempt specs,
resource ceilings, or GPU admission requirements were changed. Only the
controller is gracefully reloaded; detached experiment workers remain running.

Verification: four cadence regressions and 68 existing scheduler tests passed;
all 14 current node specs validated against the deployed schema. Runtime reload
and preserved attempt IDs are recorded in `HEALTH_CADENCE_RELOAD.json` alongside
the deployed `CONTROLLER.json`.
