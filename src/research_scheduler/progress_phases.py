"""Read-only phase evidence for legacy Bootstrap workers without phase telemetry."""


def legacy_posterior_phase(directory, progress, now):
    import json
    from pathlib import Path
    root=Path(directory)
    def read(path):
        with path.open('rb') as f:raw=f.read(131073)
        if len(raw)>131072:raise ValueError('oversized phase evidence')
        return json.loads(raw)
    try:
        cfg=read(root/'config.json')
        if cfg.get('arm')!='bayesian' or cfg.get('mode')!='train':return {}
        epoch=progress.get('epoch');member=progress.get('member')
        if epoch not in (5,10,20) or member!=0:return {}
        model=root/f'models/m0_e{epoch:02d}.pdparams'
        resume=root/f'resume/m0_e{epoch:02d}.pdstate'
        posterior=root/f'models/posterior_e{epoch:02d}.npz'
        if not model.is_file() or not resume.is_file():return {}
        started=model.stat().st_mtime
        if started>now or model.stat().st_size<=0 or resume.stat().st_size<=0:return {}
        with (root/'LEARNING_CURVE.jsonl').open('rb') as f:
            f.seek(max(0,(root/'LEARNING_CURVE.jsonl').stat().st_size-16384))
            curve=[json.loads(line) for line in f.read(16384).splitlines()[-1:]]
        if not curve or curve[-1].get('epoch')!=epoch or curve[-1].get('member')!=0:return {}
        state=read(root/'state.json')
        if state.get('attempt')!=root.name or state.get('status') not in ('running','starting'):return {}
        if not 0<=now-state.get('heartbeat',0)<120:return {}
        if state.get('boot_id')!=Path('/proc/sys/kernel/random/boot_id').read_text().strip():return {}
        raw=Path('/proc',str(state['runner_pid']),'stat').read_text()
        proc=raw[raw.rfind(')')+2:].split()
        if proc[0]=='Z' or proc[19]!=state.get('runner_start'):return {}
        if posterior.exists():
            if not 0<=now-posterior.stat().st_mtime<120:return {}
            phase='posterior_complete'
        else:phase='posterior'
        return dict(phase=phase,phase_inferred=True,phase_elapsed_s=round(now-started,1),
                    phase_evidence='completed epoch + saved checkpoint + live runner; no batch telemetry')
    except (OSError,ValueError,KeyError,TypeError,IndexError):return {}
