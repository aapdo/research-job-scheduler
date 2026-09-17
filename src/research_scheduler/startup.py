"""Cold-start grouping by approved data location, not code location."""
from .schema import node_filesystem

def startup_group(job,node):
    path=node.get('datasets',{}).get(job.get('dataset')) if job.get('dataset') else job.get('dataset_path')
    approved=node.get('labels',{}).get('local_startup_dataset_paths',[])
    if (node_filesystem(node)=='local' and path and path in approved
            and job.get('kind') in ('train','eval','prepare')):
        return ''
    return node.get('startup_group','')
