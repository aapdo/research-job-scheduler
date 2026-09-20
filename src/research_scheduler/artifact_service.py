"""Independent artifact queue owner; remote I/O never holds the registry lock."""
import argparse
import fcntl
import json
import signal
import threading
import time
from pathlib import Path

from .controller import Controller, Transport
from .store import Store


def external_artifacts_enabled(store):
    if not store.db.execute("SELECT 1 FROM sqlite_master WHERE name='artifact_service_config'").fetchone():
        return False
    row = store.db.execute("SELECT enabled FROM artifact_service_config WHERE id=1").fetchone()
    return bool(row and row[0])


class RegistrySection:
    """Same flock as the controller, released only outside DB transactions."""
    def __init__(self, store):
        self.store = store
        self.file = None
        self.held = False
        self.locked_s = 0.0
        self.waited_s = 0.0

    def acquire(self):
        started = time.monotonic()
        fcntl.flock(self.file, fcntl.LOCK_EX)
        self.waited_s += time.monotonic() - started
        self.held = True
        self.since = time.monotonic()

    def release(self):
        if self.store.db.in_transaction:
            raise RuntimeError('cannot release registry with an open SQLite transaction')
        self.locked_s += time.monotonic() - self.since
        fcntl.flock(self.file, fcntl.LOCK_UN)
        self.held = False

    def __enter__(self):
        self.file = self.store.path.with_suffix(self.store.path.suffix + '.lock').open('a')
        self.acquire()
        return self

    def __exit__(self, *exc):
        if self.store.db.in_transaction:
            self.store.db.rollback()
        if self.held:
            self.release()
        self.file.close()


class UnlockedTransport:
    def __init__(self, section, transport=None):
        self.section = section
        self.transport = transport or Transport()
        self.rpc_s = 0.0

    def call(self, node, action, request):
        self.section.release()
        started = time.monotonic()
        try:
            return self.transport.call(node, action, request)
        finally:
            self.rpc_s += time.monotonic() - started
            self.section.acquire()


def cycle(store, cleanup=False):
    from .artifacts import (tick, attempt_archive_policy, cleanup_archived_sources,
                            ensure_attempt_archive_index)
    with RegistrySection(store) as section:
        if not external_artifacts_enabled(store):
            raise RuntimeError('external artifact ownership is not enabled')
        ensure_attempt_archive_index(store)
        transport = UnlockedTransport(section)
        controller = Controller(store, transport)
        if cleanup:
            policy = attempt_archive_policy(store)
            if policy:
                cleanup_archived_sources(controller, store.specs('nodes'), policy)
        else:
            controller.artifact_cleanup_deferred = True
            tick(controller, execute=True)
    return dict(registry_wait_s=section.waited_s,
                registry_locked_s=section.locked_s, remote_rpc_s=transport.rpc_s,
                artifact_phases=getattr(controller,'artifact_phase_times',{}))


def loop_wait_s(interval, elapsed, cleanup=False):
    """Guarantee other controller/registration operations a fair lock window."""
    minimum_yield = 30 if cleanup else max(5, interval)
    return max(minimum_yield, interval - elapsed)


def run_loop(args, stop, cleanup=False):
    """Each lane owns a SQLite connection; only committed reservations cross."""
    store = None
    name = 'CLEANUP' if cleanup else 'STATE'
    try:
        while not stop.is_set():
            started = time.monotonic()
            result = dict(time=time.time(), lane='cleanup' if cleanup else 'transfers')
            try:
                if store is None:
                    store = Store(args.db)
                result.update(cycle(store, cleanup=cleanup), status='ok')
            except Exception as exc:
                result.update(status='error', error=type(exc).__name__, message=str(exc)[-1000:])
            result['elapsed_s'] = time.monotonic() - started
            temporary = args.output / (name+'.tmp')
            temporary.write_text(json.dumps(result, indent=2) + '\n')
            temporary.replace(args.output / (name+'.json'))
            print(json.dumps(result), flush=True)
            # Always yield a scheduling opportunity even if a transfer cycle
            # exceeds its target interval; retention must not spin on the lock.
            stop.wait(loop_wait_s(args.interval, result['elapsed_s'], cleanup))
    finally:
        if store:
            store.db.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--interval', type=float, default=5)
    args = parser.parse_args(argv)
    if not 1 <= args.interval <= 3600:
        parser.error('invalid interval')
    stop = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stop.set())
    args.output.mkdir(parents=True, exist_ok=True)
    # OS releases ownership on death. Existing starting/unknown DB reservations
    # and immutable runner claims are reconciled, never expired into duplicates.
    with args.db.with_suffix('.artifacts.lock').open('a') as owner:
        fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
        cleanup_thread = threading.Thread(target=run_loop, args=(args, stop, True),
                                          name='archive-cleanup', daemon=True)
        cleanup_thread.start()
        try:
            run_loop(args, stop)
        finally:
            stop.set()
            cleanup_thread.join(timeout=35)


if __name__ == '__main__':
    main()
