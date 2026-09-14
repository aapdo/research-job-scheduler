# GPU startup failover

Partial NVIDIA inventory responses retain valid GPU rows. Devices absent from the
completed query are excluded from admission; no missing telemetry is replaced by
zero. Temperature, health streak, memory, storage and user restrictions still apply.

For single-GPU training, the status reader recognizes the combination of CUDA
unavailable and the exact visibility assertion in `smoke/rank0.stdout`, only after
termination is confirmed, with no ready marker or production progress file.
It annotates the report, preserving the immutable execution spec and logs.
Other failures, unknown processes, active training and hardware jobs are not retried
by this policy.

Confirmed startup-failure UUIDs are quarantined globally on that node for its current
boot. The planner selects another eligible GPU/server without changing scientific
settings. Each job receives at most three additional automatic infrastructure retries;
normal success and recovery notifications still require actual progress.

An operator-verified multi-device CUDA failure can set
`node.labels.gpu_runtime_quarantine` with `boot_id`, reason and evidence. This blocks
new GPU jobs, not existing workers or CPU jobs. A new boot clears the matching hold,
subject to all ordinary fresh-health gates; earlier recovery requires explicit
verified removal. No driver reset, reboot or process termination is automatic.
