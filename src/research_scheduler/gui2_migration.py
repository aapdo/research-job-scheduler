"""Future-only host remapping after an explicitly activated GUI2 replacement."""
import copy
from .store import dumps


def remap_pending_hosts(store):
    with store.lock(),store.db:
        replacements={n['labels']['replaces_drained_node']:key for key,n in store.specs('nodes').items()
                      if n['enabled'] and n.get('labels',{}).get('replaces_drained_node')}
        changed=[]
        for row in store.jobs():
            if row['status']!='queued':continue
            spec=row['spec']
            # A publication job must stay beside its original producing attempt.
            if spec.get('config',{}).get('mode')=='publish_epoch_checkpoint':continue
            hosts=list(dict.fromkeys(replacements.get(h,h) for h in spec['hosts']))
            if hosts==spec['hosts']:continue
            after=copy.deepcopy(spec);after['hosts']=hosts
            store.db.execute('UPDATE jobs SET spec=? WHERE id=?',(dumps(after),row['id']))
            store.event('gui2_pending_hosts_remapped',row['id'],dict(before=spec['hosts'],after=hosts,
                        scientific_configuration_unchanged=True))
            changed.append(row['id'])
        return changed
