"""Rebuild fact_cutoffs from scratch with the fixed parser."""
import yaml
from src.curate.visa_bulletin_loader import load_visa_bulletin

with open('configs/paths.yaml') as f:
    cfg = yaml.safe_load(f)

result = load_visa_bulletin(cfg['data_root'], cfg['artifacts_root'])
print(f'Output: {result}')
