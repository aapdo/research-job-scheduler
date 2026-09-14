"""Validate node inventory against the actual scientific controller deployment."""
import importlib.util
from pathlib import Path
import unittest
from test_scheduler import node

DEPLOYED=Path('/home/jy/experiments/farm89_gui2_migration_20260909/scheduler/src/research_scheduler/schema.py')


@unittest.skipUnless(DEPLOYED.is_file(),'scientific deployment not installed')
class DeployedNodeSchemaTest(unittest.TestCase):
    def test_preparation_only_node_accepts_physical_host(self):
        spec=importlib.util.spec_from_file_location('deployed_node_schema',DEPLOYED)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        n=node(key='lab2');n.update(enabled=False,physical_host='lab2',cpu_limit=48,ram_limit_mib=192032)
        result=module.node_spec(n)
        self.assertEqual(result['physical_host'],'lab2')
        self.assertFalse(result['enabled'])
        with self.assertRaises(ValueError):module.node_spec(dict(n,physical_host='../invalid'))
        with self.assertRaises(ValueError):module.node_spec(dict(n,unknown_inventory_field=True))


if __name__=='__main__':unittest.main()
