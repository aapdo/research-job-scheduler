#!/usr/bin/env python3
"""One-time audited model-node rename/addition for 2026-09-19.

The old physical RP1 remains available as RP3.  Historical attempts are
re-attributed to RP3 without changing their frozen attempt specs.  A distinct
new Runpod pod is registered as disabled RP1 until its local data/runtime and
GPU validation are complete.
"""
import argparse
import hashlib
import json
import sqlite3
import time


def dumps(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False)


def digest(value):
    return hashlib.sha256(dumps(value).encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    args = parser.parse_args()
    db = sqlite3.connect(args.db, timeout=30)
    db.row_factory = sqlite3.Row
    now = time.time()
    with db:
        rows = {r["id"]: json.loads(r["spec"]) for r in db.execute(
            "SELECT id,spec FROM nodes WHERE id IN ('rp1','rp2','rp3')")}
        if "rp3" in rows:
            raise SystemExit("rp3 already exists; refusing a second migration")
        if set(rows) != {"rp1", "rp2"}:
            raise SystemExit("expected existing rp1 and rp2 only")
        active = db.execute(
            "SELECT id,status FROM attempts WHERE node='rp1' "
            "AND status IN ('starting','running','unknown')").fetchall()
        if active:
            raise SystemExit("old rp1 has active/unknown attempts: " + repr([tuple(x) for x in active]))

        old = rows["rp1"]
        old_hash = digest(old)
        rp3 = json.loads(dumps(old))
        rp3.update(id="rp3", target="rp3_runpod", storage_domain="rp3-local")
        rp3.setdefault("labels", {}).update(
            renamed_from="rp1", renamed_at=now, runpod_role="rp3")

        template = rows["rp2"]
        rp1 = json.loads(dumps(template))
        rp1.update(
            id="rp1", target="rp1_runpod", storage_domain="rp1-local",
            enabled=False, max_jobs=4, admission_priority=495,
            gpus=[
                {"enabled": True, "index": 0, "memory_mib": 32607.0,
                 "name": "NVIDIA GeForce RTX 5090",
                 "uuid": "GPU-9da48dd2-bbbd-50b7-49ba-8f6aeb8a4b94"},
                {"enabled": True, "index": 1, "memory_mib": 32607.0,
                 "name": "NVIDIA GeForce RTX 5090",
                 "uuid": "GPU-f9b1036c-c70a-088f-eb81-1d19fa7a962a"},
            ],
            assets={},
        )
        rp1["labels"] = {
            "provider": "runpod",
            "runpod_pod_id": "ac0tg5829ai597",
            "runpod_role": "rp1",
            "storage": "runpod-local",
            "framework": "paddle-3.4.0-cuda12.9-pending",
            "admission_state": "disabled_pending_rp2_clone_and_gpu_validation",
            "execution_profile_gate": "required_per_workload",
            "ordinary_gpu_admission": "disabled_pending_job_specific_host_validation",
            "dashboard_visible_when_disabled": True,
            "added_at": now,
        }

        db.execute("DELETE FROM nodes WHERE id='rp1'")
        db.execute("INSERT INTO nodes(id,spec) VALUES(?,?)", ("rp3", dumps(rp3)))
        db.execute("INSERT INTO nodes(id,spec) VALUES(?,?)", ("rp1", dumps(rp1)))
        moved_attempts = db.execute("UPDATE attempts SET node='rp3' WHERE node='rp1'").rowcount
        moved_snapshots = db.execute("UPDATE snapshots SET node='rp3' WHERE node='rp1'").rowcount
        moved_health = db.execute("UPDATE node_health SET node='rp3' WHERE node='rp1'").rowcount
        moved_preps = {}
        for table in ("dataset_preparations", "execution_preparations",
                      "resource_locations", "artifact_transfers"):
            moved_preps[table] = db.execute(
                f"UPDATE {table} SET node='rp3' WHERE node='rp1'").rowcount

        event = {
            "old_node": "rp1", "new_node": "rp3", "old_spec_sha256": old_hash,
            "frozen_attempt_specs_changed": False,
            "moved_attempt_rows": moved_attempts,
            "moved_snapshot_rows": moved_snapshots,
            "moved_health_rows": moved_health,
            "moved_auxiliary_rows": moved_preps,
        }
        db.execute("INSERT INTO events(time,kind,subject,data) VALUES(?,?,?,?)",
                   (now, "node_renamed", "rp3", dumps(event)))
        db.execute("INSERT INTO events(time,kind,subject,data) VALUES(?,?,?,?)",
                   (now, "node_registered", "rp1", dumps(rp1)))

    print(dumps({
        "status": "applied", "old_rp1_is_now": "rp3",
        "new_rp1_pod": "ac0tg5829ai597", "new_rp1_enabled": False,
        "rp1_spec_sha256": digest(rp1), "rp3_spec_sha256": digest(rp3),
        **event,
    }))


if __name__ == "__main__":
    main()
