# Campaign visibility

Slack model tables use one row per campaign with `train-finish`, `train-run`,
`train-err`, `train-res-wait`, `train-dep-wait`, then the corresponding five `eval`
columns. Missing work types are shown as a dash. Auxiliary/external work and
initializing, blocked, unknown or cancelled states remain in a separate table;
they are not silently merged into run/error or model completion counts.

Default overview and Slack progress snapshots omit campaigns whose overall
completion was at least 36 hours ago. The boundary is inclusive. This is a
presentation rule only: it does not delete records, cancel jobs, change dispatch,
or treat build success as hardware campaign completion.

Scientific campaigns must be recorded complete and have no current unfinished
jobs. Completion-alert creation times, explicit completion timestamps, and known
current-job/upload finish times determine age; the refreshing runtime `updated`
field is never used. Old cancelled alternatives do not prevent hiding a campaign
that is explicitly complete. Unregistered projects need successful job finish
evidence. Unknown times remain visible. Hardware uses its last complete lifecycle
transition in `HISTORY.jsonl`, not `STATE.json` observation time.

`overview.collect()` filters campaign and job rows while retaining the underlying
registry. `progress_notifications.messages()` also checks completed-at metadata
before rendering Slack campaign tables. The regular ten-minute progress service
uses this same collector. Event-driven start/error/new-completion alerts are not
disabled by this display rule.
