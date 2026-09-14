"""Shared model admission policy, independent of frozen scientific settings."""
import copy
import os


def default_mib():
    raw=os.environ.get('RS_MODEL_VRAM_MIB')
    if raw is None:return None
    value=int(raw)
    if not 1024<=value<=262144:raise ValueError('RS_MODEL_VRAM_MIB must be 1024..262144')
    return value


def reservation(spec):
    base=default_mib()
    if base is None or not spec.get('resources',{}).get('gpu_count') or spec.get('kind') not in ('train','eval','prepare'):return None
    raised=spec.get('metadata',{}).get('vram_after_oom_mib',base)
    if type(raised) is not int or raised<base:raised=base
    return max(base,raised)


def normalize(spec):
    value=reservation(spec)
    if value is None:return spec
    # Planning calls this again for every candidate host/resource variant.
    # Already-normalized specs are read-only here; avoid copying input manifests
    # and all host profiles again when no admission value changes.
    resources=[spec['resources'],*spec.get('resource_variants',[]),
               *spec.get('metadata',{}).get('execution_original_resources',[])]
    resources.extend(p['resource_contract'] for p in
                     spec.get('metadata',{}).get('execution_profiles',{}).values()
                     if p.get('resource_contract'))
    if all(not r.get('gpu_count') or r.get('vram_mib')==value for r in resources):
        return spec
    result=copy.deepcopy(spec)
    resources=[result['resources'],*result.get('resource_variants',[]),
               *result.get('metadata',{}).get('execution_original_resources',[])]
    for profile in result.get('metadata',{}).get('execution_profiles',{}).values():
        if profile.get('resource_contract'):resources.append(profile['resource_contract'])
    for resource in resources:
        if resource.get('gpu_count'):resource['vram_mib']=value
    return result
