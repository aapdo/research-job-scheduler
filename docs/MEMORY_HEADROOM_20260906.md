# Cgroup v2 file-cache-aware RAM admission

`memory.current` includes filesystem cache. On long-lived dataset readers a
container can approach its cgroup limit while most charged bytes are inactive
file cache, so `memory.max - memory.current` alone understates usable RAM.

The probe now estimates:

```
clean_inactive = max(0, inactive_file - file_dirty - file_writeback)
potential = max(0, memory.max - memory.current + min(memory.current, clean_inactive))
available = min(host MemAvailable, memory.max, potential)
```

All cgroup counters are in bytes. Active file cache, anonymous memory, dirty or
writeback pages are not added as available capacity. Missing counters fall back
to raw headroom. Both host and cgroup limits still apply, as do reservations,
RAM margins and D-state/read gates. This is an admission estimate, not an
allocation guarantee; cgroup hard limits are never altered.

`memory_cgroup` in the snapshot records raw headroom, limit, current usage and
the clean-inactive estimate separately. Existing attempt specifications and
embedded runners remain immutable across a controller restart.

Counter semantics: [Linux cgroup v2 memory.stat](https://docs.kernel.org/admin-guide/cgroup-v2.html).
