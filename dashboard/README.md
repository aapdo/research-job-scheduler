# Research Watch

Local dashboard at port 30091. This is **not a scheduler controller**.

- Scientific and hardware databases are opened with `mode=ro` and `query_only=ON`.
- The background collector reads current state every 30 seconds and bounded exact
  remote progress markers. HTTP requests never dispatch jobs or trigger SSH.
- Only the dashboard's separate inbox SQLite database is writable. No cookies or
  browser storage hold read status. `POST /api/notifications/read` marks a shared
  notification read; after 24 hours its dashboard copy is deleted. An import
  watermark prevents the same old notification reappearing. Source histories stay.
  `POST /api/notifications/read-many` marks an explicit batch of up to 500 IDs
  in one transaction. The UI snapshots unread IDs in the selected software/hardware
  categories, preserving hidden categories and notifications arriving afterward.
- No login: expose only on the trusted internal network. No cloud deployment or
  router/firewall changes are made. URLs, credentials and raw execution configs
  are not returned. Static routes are an allowlist; mutation endpoints are absent
  except the inbox read marker. Browser-side text is escaped.
- Ordinary GETs of `/api/state` reuse an in-memory snapshot. Old snapshots stay
  visible with explicit timestamps if collection fails.

Service: `research-dashboard.service` (user systemd, port 30091).
Source entrypoint: `scheduler/dashboard/server.py`.

Training progress can derive the in-epoch cursor from the recorded global
iteration/update count, epoch and steps-per-epoch when `step_in_epoch` is absent.
Inconsistent or missing counters remain unknown; elapsed time is not used to
invent progress. Evaluation accepts cells, images or batch numerator/denominator.
Inbox: `/home/jy/experiments/research_dashboard_20260911/inbox.db`.

GPU telemetry health and server admission health are distinct. A green GPU badge
does not authorize new work: per-job VRAM reservations and all scheduler gates
still apply. Historical failed attempts indicate recovery; they are not deleted.
The GPU view lists all registered GPUs, including disabled devices. Disabled
rows show `사용 불가` in place of job names or `스케줄러 미배정`, while retaining
usage/temperature/VRAM telemetry. Server selectors also include disabled-only
servers. Admission counts still exclude disabled GPUs; source job data remains.
GPU utilization is the arithmetic mean of distinct valid scheduler measurements
within the last **three minutes**, regardless of scheduler assignment or process
ownership. Unassigned intervals and genuine zeroes are included. Job starts,
retries, finishes and sharing changes do not reset the window. Cached probes are
counted only once, by measurement time. Missing/stale values are not zeroes. Fewer
than two samples show collecting; a short observed span is labelled collecting
even after a mean is available. Samples live in server memory, shared across
viewers; browser refresh keeps them and service restart begins collection again.
This is physical GPU utilization, not per-process utilization. Other processes
sharing that GPU can contribute. Temperature and VRAM remain latest measurements;
raw utilization and scheduler admission policies are unchanged.
Campaigns are newest-first using original registration time, not STATE mtime.
Model campaign experiment details and job search/status filtering include only
`train` and `eval`. Reports, publication, preparation and explicit smoke jobs
remain in the source/API as `support`, separate from the existing Train/Eval
counts, but are not shown as model experiments. Campaign lifecycle and alerts
remain authoritative, including support-stage failures; no job is cancelled or
deleted by this presentation filter. Physical GPU reservation display is unchanged.
The dashboard retains the overview's 36-hour completed-campaign visibility policy.

Memory recovery (2026-09-11): the read-only overview now reuses decoded rows
within its SQLite snapshot instead of loading historical attempt specifications
again for admission planning. Resource-reservation reads decode only active
artifact transfers. No history is deleted, and the running scheduler is not
restarted for this dashboard change. After recovery, the original 1 GiB service
limit was restored; repeated public/local API reads returned HTTP 200 across
cache refreshes, at approximately 0.5 GiB service memory with no restarts.
