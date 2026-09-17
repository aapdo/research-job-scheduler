"""Shared model admission policy, independent of frozen scientific settings."""
import copy
import os


def default_mib(kind=None):
    raw=os.environ.get('RS_MODEL_EVAL_VRAM_MIB') if kind=='eval' else None
    if raw is None:raw=os.environ.get('RS_MODEL_VRAM_MIB')
    if raw is None:return None
    value=int(raw)
    if not 1024<=value<=262144:raise ValueError('RS_MODEL_VRAM_MIB must be 1024..262144')
    return value


def reservation(spec):
    base=default_mib(spec.get('kind'))
    if base is None or not spec.get('resources',{}).get('gpu_count') or spec.get('kind') not in ('train','eval','prepare'):return None
    override=spec.get('metadata',{}).get('vram_reservation_override_mib')
    if override is not None:
        if type(override) is not int or not 1024<=override<=262144:
            raise ValueError('vram_reservation_override_mib must be 1024..262144')
        base=override
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
    variants=[]
    for resource in result.get('resource_variants',[]):
        if resource!=result['resources'] and resource not in variants:variants.append(resource)
    if 'resource_variants' in result:result['resource_variants']=variants
    return result
