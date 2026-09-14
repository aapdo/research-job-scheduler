# Registered-process D-state policy

Latest operator instruction: 2026-09-11. Model and hardware admission must not
use system-wide D-state as a proxy for a scheduler-owned workload fault.

- Controller probes receive active/unknown attempt and artifact-transfer IDs
  and immutable directories from the registry. Runner identity requires matching
  attempt ID, boot ID, PID and process start tick in its state file. Descendants
  inherit that identity; orphaned children may be identified by the exact
  registered `RS_ATTEMPT_ID` and `RS_ATTEMPT_DIR` pair in their environment.
  Environment contents are never returned or logged.
- Track each `(boot ID, PID, start tick, attempt ID)` separately. A process must
  be observed in D-state for at least **180 seconds** to affect admission.
  Normal/nonexistent observations, a changed identity/boot or a gap over 60s
  reset the timer. Periodic observations cannot prove behavior between samples.
- Snapshot `d_state`/`d_state_pids` now contain only sustained registered
  processes. `d_state_observed` and `d_state_tracks` retain transient evidence.
  `d_state_unmanaged_count` counts D-state processes not attributed to a registered
  attempt, including those for which ownership could not be established.
  `d_state_policy=registered-process-180s-v1` marks the changed contract.
- Transient or unrelated D-state no longer suppresses dataset/read probes,
  resets healthy-poll streaks or blocks admission. A genuine read failure,
  SSH failure, temperature/RAM/VRAM restriction or operator ban remains separate.
- Sustained registered D-state pauses new placement, not existing work. It is
  not grounds for forced termination or automatic failover. Recovery restores
  normal health evaluation and the existing three-good-poll gate. Old sticky
  D-only holds are re-probed; SSH retry exhaustion stays a separate restriction.
- Physical hardware health publishers use the same tracker, with registered
  attempts from both model and hardware DBs. `persistent_d_state` is true only
  at 180s. Their historical `d_state` list retains observed registered processes.
  Canonical build counts still include external builds for slot safety.

Deployment is controller/probe-only. Frozen runner/attempt files and prior
failure history are not rewritten. Each probe ships its current helper source;
the registry-only context fields are not added to saved node specifications.

Regression coverage: `scheduler/tests/test_registered_d_state.py` verifies
ownership, orphan tokens, PID reuse, exactly 180s, recovery, missing samples,
reboots, and distinction between D-state and sticky SSH holds. The normal
scheduler/cadence suite also remains passing.
