#!/usr/bin/env python3
"""Move only queued portable reports to LAB4 under the existing controller lock."""
import argparse
import hashlib
import json
import sqlite3

from research_scheduler.report_placement import is_report_job, on_archive_host
from research_scheduler.schema import experiment_spec, node_spec
from research_scheduler.store import Store, dumps


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    reader = sqlite3.connect('file:' + args.db + '?mode=ro', uri=True)
    reader.row_factory = sqlite3.Row
    lab4 = node_spec(json.loads(reader.execute("SELECT spec FROM nodes WHERE id='lab4'").fetchone()[0]))
    changes = []
    for row in reader.execute("SELECT id,spec FROM jobs WHERE status='queued'"):
        original = json.loads(row['spec'])
        if not is_report_job(original):
            continue
        revised = on_archive_host(original, lab4)
        experiment_spec(dict(id='report-policy-check', name='check', rq='LAB4 placement', jobs=[revised]))
        if revised != original:
            changes.append((row['id'], row['spec'], revised))
    reader.close()
    print(json.dumps(dict(execute=args.execute, jobs=[key for key, _, _ in changes])))
    if not args.execute:
        return
    store = Store(args.db)
    try:
        with store.lock(timeout=180), store.db:
            for key, frozen, revised in changes:
                row = store.db.execute('SELECT status,spec FROM jobs WHERE id=?', (key,)).fetchone()
                if row['status'] != 'queued' or row['spec'] != frozen:
                    print('skipped changed job', key)
                    continue
                store.db.execute('UPDATE jobs SET spec=? WHERE id=?', (dumps(revised), key))
                store.event('report_lab4_policy_applied', key, dict(
                    old_hosts=json.loads(frozen).get('hosts'), new_hosts=['lab4'],
                    previous_spec_sha256=hashlib.sha256(frozen.encode()).hexdigest(),
                    new_spec_sha256=hashlib.sha256(dumps(revised).encode()).hexdigest()))
                print('updated', key)
    finally:
        store.db.close()


if __name__ == '__main__':
    main()
