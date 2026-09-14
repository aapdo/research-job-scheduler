import json
from pathlib import Path
import tempfile
import unittest
from test_scheduler import node,job,experiment
from research_scheduler.store import Store
from research_scheduler.gui2_migration import remap_pending_hosts


class MigrationTests(unittest.TestCase):
    def test_only_pending_scientific_hosts_move(self):
        with tempfile.TemporaryDirectory() as tmp:
            s=Store(Path(tmp)/'state.db')
            old=node(key='farm8');old.update(enabled=False,transport='ssh',target='old');s.register_node(old)
            new=node(key='farm8-gui2');new.update(transport='ssh',target='new');new['labels']['replaces_drained_node']='farm8';s.register_node(new)
            a=job('pending');a['hosts']=['farm8']
            b=job('running');b['hosts']=['farm8']
            p=job('CSSA_EPOCH_keep',gpu_count=0);p.update(kind='prepare',hosts=['farm8'],config={'mode':'publish_epoch_checkpoint'})
            s.register_experiment(experiment([a,b,p]))
            with s.db:s.db.execute("UPDATE jobs SET status='running' WHERE id='running'")
            self.assertEqual(remap_pending_hosts(s),['pending'])
            rows={j['id']:j['spec'] for j in s.jobs()}
            self.assertEqual(rows['pending']['hosts'],['farm8-gui2'])
            self.assertEqual(rows['running']['hosts'],['farm8'])
            self.assertEqual(rows['CSSA_EPOCH_keep']['hosts'],['farm8'])
            self.assertEqual(remap_pending_hosts(s),[])
            s.db.close()


if __name__=='__main__':unittest.main()
