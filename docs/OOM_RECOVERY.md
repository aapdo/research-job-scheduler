# Automatic model OOM failover

2026-09-13. Applies only to model train/eval, not hardware, board operations,
CPU preparation, cancelled attempts, or an arbitrary historical-failure sweep.

1. Status collection records exact RS_ATTEMPT_ID / RS_ATTEMPT_DIR, UID, PID,
   process start and boot identities for active attempts. It never sends signals.
2. A failed runner with an explicit OOM log (including `run/stdout.log`) is
   classified. On a directly accessible host, kernel OOM messages may also be
   matched to a registered PID observed within 60 seconds, the same boot and
   execution interval. The journal query is bounded; unavailable evidence is not
   treated as OOM. Exit 137/-9, host memory pressure or global counters alone do
   not establish which experiment was OOM-killed.
3. Surviving owned processes keep the attempt unknown and its reservation held.
   Only an execute cycle may invoke `recover_oom`. This rechecks a failed receipt,
   boot and exact process ownership, uses PID-safe file descriptors, and verifies
   termination after bounded TERM/KILL waits. No GPU process is killed merely
   because it occupies memory. Two unsuccessful cleanups leave a manual-check hold.
4. Only verified termination permits failover. The failed host is excluded for
   that job, not disabled globally. Existing candidates, scientific settings and
   normal data/runtime/VRAM/temperature/storage/fresh-health gates remain intact.
   Up to three additional OOM attempts are permitted; repeated reconciliation is
   idempotent. Original failures and checkpoints remain.

Resume is never fabricated. Existing compatible checkpoint logic may resume;
otherwise normal non-resume workers may use their approved fresh path. A
host-local `resume_from` or single-host-only recipe cannot safely be rewritten
to another host automatically and is held until a verified portable/fresh recipe
exists. Empty `hosts` means any otherwise permitted host, with exclusions applied.

Limitations: container-local PIDs are not matched against host journal PIDs;
kernel-only failures in that case require an authorized host-side identity source.
A process killed before the first identity observation may not be attributable.
No fallback silently bypasses these guards. Existing active runners stay frozen;
status/cleanup RPCs use the controller's newly deployed agent source.

Tests cover fresh/old boot and PID reuse, explicit log OOM, no inference from 137,
non-mutating status, PID-safe cleanup, execute-only controller cleanup, host-only
exclusion, bounded retries, unconstrained candidates and local-resume holds.
