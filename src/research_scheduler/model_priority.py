"""User-approved priority bands for model quantization and final bank sweeps."""
import re

QUANTIZATION_FLOOR = 30000
BANK_FINAL_CEILING = 29000


def priority_for(experiment):
    project = experiment.get('project', '')
    priority = experiment['priority']
    # Match model campaign tokens, not arbitrary prose (e.g. ColorQuant).
    quantized = project.startswith('s-') and re.search(
        r'(?:^|-)(?:ptq|qat|int8|w8a8|w8a32|w32a8|a8|a16|a16a32|quantized|perchannel|w0qat)(?:-|$)', project)
    if quantized:
        return max(priority, QUANTIZATION_FLOOR)
    if project.startswith('s-bank-final-'):
        return min(priority, BANK_FINAL_CEILING)
    return priority
