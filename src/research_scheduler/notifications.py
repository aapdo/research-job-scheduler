"""Campaign-level durable notifications with an optional secret webhook file."""
import hashlib
import json
import os
import stat
import time
import urllib.request
from pathlib import Path

from .schema import check, fields, identifier
from .store import dumps


ALERT_STATES = {"complete", "error"}
ACTIVE_JOB_STATES = {"queued", "starting", "running", "unknown"}
ERROR_JOB_STATES = {"failed", "blocked"}


def ensure_tables(store):
    store.db.executescript("""
        CREATE TABLE IF NOT EXISTS campaigns(
            id TEXT PRIMARY KEY, spec TEXT NOT NULL, created REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS campaign_runtime(
            id TEXT PRIMARY KEY, state TEXT NOT NULL, generation INTEGER NOT NULL,
            details TEXT NOT NULL, updated REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS notification_outbox(
            id TEXT PRIMARY KEY, campaign TEXT NOT NULL, state TEXT NOT NULL,
            payload TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
            next_attempt REAL NOT NULL, created REAL NOT NULL, sent REAL, error TEXT NOT NULL DEFAULT '');
    """)


def campaign_spec(raw):
    value = json.loads(dumps(raw))
    fields(value, "id name rq projects experiments external enabled hf")
    if value.get("hf") is not None:
        from .artifacts import hf_spec
        value["hf"] = hf_spec(value["hf"])
    identifier(value["id"])
    for key in ("name", "rq"):
        check(isinstance(value.get(key), str) and value[key].strip(), key + " is required")
    for key in ("projects", "experiments"):
        value.setdefault(key, [])
        check(isinstance(value[key], list) and all(isinstance(v, str) and v for v in value[key]),
              key + " must contain strings")
        check(len(value[key]) == len(set(value[key])), "duplicate " + key)
    value.setdefault("external", False)
    value.setdefault("enabled", True)
    check(isinstance(value["external"], bool) and isinstance(value["enabled"], bool),
          "external/enabled must be boolean")
    if value["external"]:
        check(not value["projects"] and not value["experiments"],
              "external campaign cannot also select scheduler experiments")
        check(not value.get('hf'), 'external campaign must import its jobs before enabling automatic HF publication')
    else:
        check(value["projects"] or value["experiments"],
              "campaign needs a project or experiment")
    return value


def register_campaign(store, raw):
    value = campaign_spec(raw)
    with store.lock(), store.db:
        ensure_tables(store)
        known = set(store.specs("experiments"))
        check(set(value["experiments"]) <= known, "unknown explicit experiment")
        if value.get('hf'):
            experiments = store.specs('experiments')
            def selected(c):
                return set(c['experiments']) | {k for k,e in experiments.items() if e['project'] in c['projects']}
            for other in campaign_specs(store).values():
                if other['id'] != value['id'] and other.get('hf') and other['enabled']:
                    check(not (set(other['projects']) & set(value['projects']) or selected(other) & selected(value)),
                          'overlapping HF campaigns are not allowed')
        row = store.db.execute("SELECT spec FROM campaigns WHERE id=?", (value["id"],)).fetchone()
        if row is not None:
            old = json.loads(row[0])
            check({k:v for k,v in old.items() if k != "hf"} ==
                  {k:v for k,v in value.items() if k != "hf"},
                  "campaign already exists with another specification")
            store.db.execute("UPDATE campaigns SET spec=? WHERE id=?", (dumps(value), value["id"]))
            if old != value:
                store.event("campaign_hf_changed", value["id"], {"hf": value.get("hf")})
            return value
        store.db.execute("INSERT INTO campaigns VALUES(?,?,?)",
                         (value["id"], dumps(value), time.time()))
        store.event("campaign_registered", value["id"],
                    {"projects": value["projects"], "experiments": value["experiments"],
                     "external": value["external"]})
    return value


def campaign_specs(store):
    ensure_tables(store)
    return {row["id"]: json.loads(row["spec"])
            for row in store.db.execute("SELECT * FROM campaigns ORDER BY id")}


def _job_observation(store, spec):
    experiments = store.specs("experiments")
    selected = set(spec["experiments"])
    selected.update(key for key, value in experiments.items()
                    if value.get("project", "general") in spec["projects"])
    jobs = [job for job in store.jobs() if job["experiment"] in selected]
    counts = {}
    for job in jobs:
        counts[job["status"]] = counts.get(job["status"], 0) + 1
    errors = [dict(job=job["id"], status=job["status"], reason=job["reason"])
              for job in jobs if job["status"] in ERROR_JOB_STATES]
    if errors:
        state = "error"
    elif any(job["status"] in ACTIVE_JOB_STATES for job in jobs):
        state = "running"
    elif jobs and all(job["status"] in {"succeeded", "cancelled"} for job in jobs):
        state = "complete"
    else:
        state = "pending"
    publication = None
    if spec.get('hf'):
        from .artifacts import publication_summary
        publication = publication_summary(store, spec)
        if publication['errors']:
            errors.extend(publication['errors'])
            state = 'error'
        elif publication['counts']['pending'] and state == 'complete':
            state = 'running'
    return {"state": state, "counts": counts, "jobs": len(jobs),
            "experiments": len(selected), "errors": errors[:8], 'publication': publication}


def _message(spec, observation):
    icon = "✅" if observation["state"] == "complete" else "🚨"
    title = "complete" if observation["state"] == "complete" else "error"
    counts = ", ".join(f"{key}={value}" for key, value in sorted(observation.get("counts", {}).items()))
    lines = [f"{icon} Research campaign {title}", f"{spec['name']} (`{spec['id']}`)",
             "RQ: " + spec["rq"], "Counts: " + (counts or "external campaign")]
    if spec.get('hf'):
        h = spec['hf']
        lines.append('HF: https://huggingface.co/' + ('datasets/' if h['repo_type']=='dataset' else '') + h['repo_id'])
        lines.append('Artifact publication: ' + dumps(observation.get('publication', {})))
    for error in observation.get("errors", [])[:5]:
        reason = (error.get("reason") or "no reason recorded").replace("\n", " ")[:240]
        lines.append(f"- {error.get('job', 'external')}: {error.get('status', 'error')} — {reason}")
    return "\n".join(lines)


def _record_observation(store, spec, observation, now):
    check(observation.get("state") in {"pending", "running", "complete", "error"},
          "invalid campaign observation")
    row = store.db.execute("SELECT state,generation FROM campaign_runtime WHERE id=?",
                           (spec["id"],)).fetchone()
    previous, generation = (row[0], row[1]) if row else (None, 0)
    if previous != observation["state"]:
        generation += 1
    store.db.execute("INSERT OR REPLACE INTO campaign_runtime VALUES(?,?,?,?,?)",
                     (spec["id"], observation["state"], generation, dumps(observation), now))
    if previous != observation["state"] and observation["state"] in ALERT_STATES:
        key = hashlib.sha256(f"{spec['id']}\0{generation}\0{observation['state']}".encode()).hexdigest()
        payload = {"text": _message(spec, observation)}
        store.db.execute("INSERT OR IGNORE INTO notification_outbox "
                         "(id,campaign,state,payload,status,next_attempt,created) VALUES(?,?,?,?,?,?,?)",
                         (key, spec["id"], observation["state"], dumps(payload), "pending", now, now))
        store.event("campaign_alert_queued", spec["id"],
                    {"state": observation["state"], "generation": generation, "notification": key})


def webhook_path(explicit=None):
    """Persist via the user config directory; explicit empty string disables."""
    if explicit is not None:
        return str(explicit)
    if 'RS_SLACK_WEBHOOK_FILE' in os.environ:
        return os.environ['RS_SLACK_WEBHOOK_FILE']
    path = Path.home() / '.config/research-scheduler/slack-webhook'
    return str(path) if path.is_file() else ''


def _webhook(path):
    if not path:
        return None
    file = os.path.abspath(path)
    info = os.stat(file)
    check(info.st_uid == os.getuid(), "webhook file must be owned by the scheduler user")
    check(stat.S_IMODE(info.st_mode) & 0o077 == 0, "webhook file must not be group/world accessible")
    value = Path(file).read_text(encoding="utf-8").strip()
    check(value.startswith("https://hooks.slack.com/services/") and "\n" not in value,
          "invalid Slack webhook file")
    return value


def _send(url, payload, timeout=10):
    request = urllib.request.Request(url, data=dumps(payload).encode(),
                                     headers={"Content-Type": "application/json",
                                              "User-Agent": "research-job-scheduler"}, method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read(1024).decode(errors="replace").strip()
        if not 200 <= response.status < 300 or body.lower() != "ok":
            raise RuntimeError("Slack webhook returned a non-success response")


def poll_campaigns(store, external_observations=None, webhook_file=None, sender=_send):
    """Observe campaign transitions, enqueue alerts, and deliver due outbox rows."""
    external_observations = external_observations or {}
    now = time.time()
    with store.lock(), store.db:
        ensure_tables(store)
        specs = campaign_specs(store)
        for key, spec in specs.items():
            if not spec["enabled"]:
                continue
            if spec["external"]:
                observation = external_observations.get(key)
                if observation is None:
                    continue
            else:
                observation = _job_observation(store, spec)
            _record_observation(store, spec, observation, now)
        pending = store.db.execute(
            "SELECT * FROM notification_outbox WHERE status IN ('pending','sending') "
            "AND next_attempt<=?", (now,)).fetchall()
    try:
        url = _webhook(webhook_path(webhook_file))
    except (OSError, ValueError) as exc:
        return {"enabled": False, "config_error": type(exc).__name__, "queued": len(pending), "sent": 0}
    if not url:
        return {"enabled": False, "queued": len(pending), "sent": 0}
    with store.lock(), store.db:
        due = list(store.db.execute(
            "SELECT * FROM notification_outbox WHERE status IN ('pending','sending') "
            "AND next_attempt<=? ORDER BY created LIMIT 8", (time.time(),)))
        for row in due:
            store.db.execute("UPDATE notification_outbox SET status='sending',attempts=attempts+1,"
                             "next_attempt=? WHERE id=?", (now + 60, row["id"]))
    sent, failed = 0, 0
    for row in due:
        try:
            sender(url, json.loads(row["payload"]))
        except Exception as exc:
            failed += 1
            attempts = int(row["attempts"]) + 1
            error = type(exc).__name__
            if hasattr(exc, "code"):
                error += ":HTTP" + str(exc.code)
            with store.lock(), store.db:
                store.db.execute("UPDATE notification_outbox SET status='pending',next_attempt=?,error=? WHERE id=?",
                                 (time.time() + min(300, 10 * 2 ** min(attempts, 5)), error, row["id"]))
                store.event("campaign_alert_failed", row["campaign"],
                            {"notification": row["id"], "error": error, "attempt": attempts})
        else:
            sent += 1
            with store.lock(), store.db:
                store.db.execute("UPDATE notification_outbox SET status='sent',sent=?,error='' WHERE id=?",
                                 (time.time(), row["id"]))
                store.event("campaign_alert_sent", row["campaign"],
                            {"notification": row["id"], "state": row["state"]})
    return {"enabled": True, "queued": len(due), "sent": sent, "failed": failed}


def status(store):
    ensure_tables(store)
    specs = campaign_specs(store)
    runtime = {row["id"]: {"state": row["state"], "generation": row["generation"],
                            "details": json.loads(row["details"]), "updated": row["updated"]}
               for row in store.db.execute("SELECT * FROM campaign_runtime")}
    outbox = [dict(row) for row in store.db.execute(
        "SELECT id,campaign,state,status,attempts,next_attempt,created,sent,error "
        "FROM notification_outbox ORDER BY created")]
    return {"campaigns": specs, "runtime": runtime, "outbox": outbox,
            "webhook_configured": bool(webhook_path())}
