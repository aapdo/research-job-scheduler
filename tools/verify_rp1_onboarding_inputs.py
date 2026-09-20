#!/usr/bin/env python3
"""Verify the bounded input set required by the RP1 GPU onboarding smoke."""

import hashlib
import json
import time
from pathlib import Path


PROFILE = Path("/workspace/datasets/i3-k6-full-v1/INPUTS.json")
PROFILE_SHA256 = "1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c"
COCO_SHA256 = "2bc8085763d5b53118a9f006673f34ac4fe40645b1fb95129dc41971dcb1a02b"
RECEIPT = Path("/workspace/RP1_VALIDATION_DATA_READY.json")
CONDITIONS = ("Clean", "ColorQuant", "Fog", "LowLight", "MotionBlur", "Snow", "Brightness")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


assert sha256(PROFILE) == PROFILE_SHA256
profile = json.loads(PROFILE.read_text())
annotation = Path(profile["views"]["balanced7"]["annotation"])
assert sha256(annotation) == COCO_SHA256 == profile["views"]["balanced7"]["annotation_sha256"]
coco = json.loads(annotation.read_text())
assert len(coco.get("images", [])) == 85848
assert len(coco.get("annotations", [])) == 527737
assert len(coco.get("categories", [])) == 1

selected = []
for condition in CONDITIONS:
    rows = [row for row in coco["images"] if row["cg_condition"] == condition]
    selected.extend(rows[: 6 if condition == "Brightness" else 7])
assert len(selected) == 48
for row in selected:
    image = Path(row["file_name"])
    assert image.is_file() and image.stat().st_size > 0

metadata = profile["evaluation"]["calibration"]
assert len(metadata) == 61
for entry in metadata.values():
    path = Path(entry["path"])
    assert path.is_file() and sha256(path) == entry["sha256"]

payload = {
    "status": "verified",
    "node": "rp1",
    "pod_id": "ac0tg5829ai597",
    "revision": "b24af737e954b71ab575fa96b022a5eb71645a5c",
    "profile_sha256": PROFILE_SHA256,
    "full_coco_sha256": COCO_SHA256,
    "images": len(coco["images"]),
    "annotations": len(coco["annotations"]),
    "categories": len(coco["categories"]),
    "validation_images": len(selected),
    "calibration_metadata": len(metadata),
    "scope": "bounded GPU onboarding validation; bulk train/eval images remain on-demand HF chunks",
    "verified_at": time.time(),
}
RECEIPT.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
print(json.dumps(payload, sort_keys=True))
