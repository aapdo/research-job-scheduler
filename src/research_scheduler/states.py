"""Explicit lifecycle transitions. INVALID attempts can never publish a result."""

ATTEMPT_TRANSITIONS = {
    "starting": {"running", "unknown", "succeeded", "failed", "invalid"},
    "running": {"unknown", "succeeded", "failed", "invalid"},
    "unknown": {"starting", "running", "succeeded", "failed", "invalid"},
    "succeeded": set(), "failed": set(), "invalid": set(),
}
JOB_TRANSITIONS = {
    "queued": {"starting", "cancelled"},
    "starting": {"running", "unknown", "succeeded", "failed", "queued", "blocked"},
    "running": {"unknown", "succeeded", "failed", "queued", "blocked"},
    "unknown": {"starting", "running", "succeeded", "failed", "queued", "blocked"},
    "succeeded": set(), "failed": {"queued"}, "cancelled": set(), "blocked": set(),
}


def transition(kind, before, after):
    table = ATTEMPT_TRANSITIONS if kind == "attempt" else JOB_TRANSITIONS
    if before != after and after not in table.get(before, set()):
        raise ValueError(f"forbidden {kind} transition: {before} -> {after}")


def recovery_due(health, now):
    return health.get("phase") != "unavailable" and now >= health.get("next_retry_at", 0)


def observe_health(old, observation, policy, now):
    """Pure node recovery FSM. Initial failed probe + 12 scheduled retries.

    Four batches of three retries start at t=0,5,10,15 minutes after the initial
    failure. Connection/response timeout duration is additional elapsed time.
    Unavailable is sticky until explicit operator readmission.
    """
    old = dict(old)
    if old.get("phase") == "unavailable":
        return old
    if observation.get("error"):
        if old.get("phase") != "ssh_retrying":
            return dict(phase="ssh_retrying", first_failure_at=now, retries_done=0,
                        next_retry_at=now, error=observation["error"])
        count = old["retries_done"] + 1
        per_round = policy["ssh_attempts_per_round"]
        offsets = policy["ssh_retry_offsets_s"]
        old.update(retries_done=count, error=observation["error"])
        if count >= len(offsets) * per_round:
            old.update(phase="unavailable", unavailable_at=now, reason="SSH/response recovery budget exhausted")
        else:
            round_index = count // per_round
            old["next_retry_at"] = max(now, old["first_failure_at"] + offsets[round_index])
        return old
    if observation.get("d_state", 0):
        since = old.get("d_since", now) if old.get("phase") == "d_state_wait" else now
        # An unobserved gap is not evidence of continuously persistent D-state.
        if now - old.get("last_d_observation", now) > policy["d_observation_max_gap_s"]:
            since = now
        if now - since >= policy["d_state_timeout_s"]:
            return dict(phase="unavailable", unavailable_at=now, d_since=since,
                        reason="continuous D-state exceeded timeout")
        return dict(phase="d_state_wait", d_since=since, last_d_observation=now)
    return {"phase": "healthy", "last_success_at": now}
