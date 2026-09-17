"""Lazy read snapshot shared by campaign observers within one locked poll.

Never retain this object between polls or use it for placement/mutations.
Campaign state needs fresh attempt reports, not frozen execution requests.
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
        attempts = []
        for row in self.store.observation_attempts():
            item = dict(row, report=dict(row.get('report') or {}))
            for transfer in self.uploads_by_attempt.get(item.get('id'), []):
                receipt = transfer['report'].get('artifact')
                if transfer['status'] == 'succeeded' and receipt:
                    item['report']['hf_artifact'] = receipt
            attempts.append(item)
        return attempts

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
