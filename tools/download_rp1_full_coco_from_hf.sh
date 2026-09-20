#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"
export HF_HUB_DOWNLOAD_TIMEOUT=120
repo=apdoa/continuous-cg-full-grid
revision=b24af737e954b71ab575fa96b022a5eb71645a5c
chunk=subset_chunk_002
member=carla_data/nuimages_robobev_continuous_g1_v1_20260903_experiments/balanced7_adapter_comparison_v2_20260904/data/seed_7301_balanced7/coco/train/balanced7_universal.coco.json
expected=2bc8085763d5b53118a9f006673f34ac4fe40645b1fb95129dc41971dcb1a02b
stage=/workspace/hf-coco-fix-rp1
target=/workspace/$member
receipt=/workspace/RP1_FULL_COCO_READY.json

if test -s "$target" && test "$(sha256sum "$target" | awk '{print $1}')" = "$expected"; then
  echo full_coco_already_verified
else
  free_kib=$(df --output=avail /workspace | tail -n 1)
  test "$free_kib" -ge $((20 * 1024 * 1024))
  mkdir -p "$stage"
  hf download "$repo" "data/$chunk.tar" --repo-type dataset --revision "$revision" --local-dir "$stage"
  archive="$stage/data/$chunk.tar"
  test -s "$archive"
  tar -xf "$archive" -C /workspace "$member"
  test "$(sha256sum "$target" | awk '{print $1}')" = "$expected"
  unlink "$archive"
fi

python3 - "$target" "$receipt" "$repo" "$revision" "$expected" <<'PY'
import json, pathlib, sys, time
source = pathlib.Path(sys.argv[1])
value = json.loads(source.read_text())
assert len(value.get("images", [])) == 85848
assert len(value.get("annotations", [])) == 527737
assert len(value.get("categories", [])) == 1
payload = {
    "status": "verified", "node": "rp1", "pod_id": "ac0tg5829ai597",
    "repo_id": sys.argv[3], "revision": sys.argv[4], "sha256": sys.argv[5],
    "path": str(source), "bytes": source.stat().st_size,
    "images": len(value["images"]), "annotations": len(value["annotations"]),
    "categories": len(value["categories"]), "archive_retained": False,
    "verified_at": time.time(),
}
pathlib.Path(sys.argv[2]).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

echo full_coco_complete
