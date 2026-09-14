"""Presentation-only retention; never delete or cancel scientific history."""
import math

COMPLETED_VISIBILITY_SECONDS = 36 * 3600


def valid_timestamp(value):
    return (isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value > 0)


def expired_complete(state, completed_at, now):
    # Unknown completion times remain visible rather than inventing an age.
    return (state == 'complete' and valid_timestamp(completed_at)
            and now - completed_at >= COMPLETED_VISIBILITY_SECONDS)


def completed_jobs_at(selected, latest, publication_times):
    """All current jobs must have succeeded, with auditable completion times."""
    if not selected or any(j['status'] != 'succeeded' for j in selected):
        return None
    times = []
    for job in selected:
        attempt = latest.get(job['id'], {})
        finished = attempt.get('report', {}).get('finished')
        if attempt.get('status') != 'succeeded' or not valid_timestamp(finished):
            return None
        times.extend([finished, publication_times.get(attempt['id'], finished)])
    return max(times)
