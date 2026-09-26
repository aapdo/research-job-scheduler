"""Validate node inventory against the actual scientific controller deployment."""
import importlib.util
import os
from pathlib import Path
import shlex
import subprocess
import unittest
from test_scheduler import node


def deployed_schema_path():
    explicit = os.environ.get('RS_DEPLOYED_SCHEMA')
    if explicit:
        return Path(explicit)
    try:
        result = subprocess.run(
            ['systemctl', '--user', 'show', 'research-model-controller.service',
             '-p', 'Environment', '--value', '--no-pager'],
            capture_output=True, text=True, timeout=3)
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode:
        return None
    for assignment in shlex.split(result.stdout):
        key, _, value = assignment.partition('=')
        if key == 'PYTHONPATH':
            for root in value.split(os.pathsep):
                candidate = Path(root) / 'research_scheduler' / 'schema.py'
                if candidate.is_file():
                    return candidate
    return None


DEPLOYED = deployed_schema_path()


@unittest.skipUnless(DEPLOYED and DEPLOYED.is_file(),'scientific deployment not installed')
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
