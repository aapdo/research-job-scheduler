"""Lazy read snapshot shared by campaign observers within one locked poll.

Never retain this object between polls or use it for placement/mutations.
Attempt reads deliberately use Store.attempts(), including HF receipts.
"""
from functools import cached_property


class ObservationSnapshot:
    def __init__(self, store):
        self.store = store

    @cached_property
    def jobs(self):
        return self.store.jobs()

    @cached_property
    def jobs_by_id(self):
        return {j['id']: j for j in self.jobs}

    @cached_property
    def experiments(self):
        return self.store.specs('experiments')

    @cached_property
    def nodes(self):
        return self.store.specs('nodes')

    @cached_property
    def attempts(self):
        return self.store.attempts(summary=True)

    @cached_property
    def attempts_by_job(self):
        result = {}
        for attempt in self.attempts:
            result.setdefault(attempt['job'], []).append(attempt)
        return result

    @cached_property
    def transfers(self):
        from .artifacts import rows
        return rows(self.store)

    @cached_property
    def uploads_by_attempt(self):
        result = {}
        for transfer in self.transfers:
            if transfer['direction'] == 'upload':
                result.setdefault(transfer['attempt'], []).append(transfer)
        return result
