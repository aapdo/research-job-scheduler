#!/usr/bin/env bash
set -euo pipefail

stage=/home/jy/tmp/runpod-rp1-ac0tg5829ai597
lock=/home/jy/tmp/runpod-rp1-ac0tg5829ai597.lock
exec 9>"$lock"
flock -n 9 || { echo "clone already running"; exit 1; }

mkdir -p "$stage/workspace/runtime" "$stage/workspace/carla_data" \
  "$stage/workspace/datasets" "$stage/workspace/validation" "$stage/home/jy"

pull() {
  rsync -a --partial --info=stats2 -e ssh "rp2_runpod:$1/" "$stage$1/"
}
push() {
  ssh rp1_runpod "mkdir -p '$1'"
  rsync -a --partial --info=stats2 -e ssh "$stage$1/" "rp1_runpod:$1/"
}

paths=(
  /workspace/runtime/paddle-cu129-py312
  /workspace/carla_data
  /workspace/datasets/k6ng-warm20-v9
  /workspace/datasets/k6ng-warm20-v11
  /workspace/validation/k6ng-joint-runtime-v1
  /home/jy/carla_data
)

pids=()
for path in "${paths[@]}"; do pull "$path" & pids+=("$!"); done
for pid in "${pids[@]}"; do wait "$pid"; done

pids=()
for path in "${paths[@]}"; do push "$path" & pids+=("$!"); done
for pid in "${pids[@]}"; do wait "$pid"; done

manifest() {
  local root=$1 out=$2
  (cd "$root" && find . -type f -print0 | sort -z | xargs -0 sha256sum) >"$out"
  sha256sum "$out" | awk '{print $1}'
}

manifest_file=/home/jy/tmp/runpod-rp1-ac0tg5829ai597.FILE_SHA256SUMS
local_sha=$(manifest "$stage" "$manifest_file")
remote_sha=$(ssh rp1_runpod 'tmp=$(mktemp); cd /; find ./workspace/runtime/paddle-cu129-py312 ./workspace/carla_data ./workspace/datasets/k6ng-warm20-v9 ./workspace/datasets/k6ng-warm20-v11 ./workspace/validation/k6ng-joint-runtime-v1 ./home/jy/carla_data -type f -print0 | sort -z | xargs -0 sha256sum >"$tmp"; sha256sum "$tmp" | awk "{print \\$1}"; rm -f "$tmp"')
test "$local_sha" = "$remote_sha"

ssh rp1_runpod '/workspace/runtime/paddle-cu129-py312/bin/python - <<"PY"
import cv2, numpy, paddle, yaml
print("runtime", paddle.__version__, paddle.version.cuda(), numpy.__version__, cv2.__version__, yaml.__version__)
assert paddle.device.cuda.device_count() == 2
PY
test -r /workspace/datasets/k6ng-warm20-v11/INPUTS.json
test -r /home/jy/carla_data/software/releases/bootstrap_k6_nominal_gate_v6_20260919/SHA256SUMS
df -h /workspace'

echo "tree_sha256=$local_sha"
python3 - "$local_sha" <<'PY'
import json, pathlib, sys, time
path = pathlib.Path("/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_TRANSFER_RECEIPT_v1.json")
payload = {
    "status": "verified",
    "node": "rp1",
    "pod_id": "ac0tg5829ai597",
    "source_node": "rp2",
    "route": "rp2 -> controller staging -> rp1",
    "tree_sha256": sys.argv[1],
    "profile": "/workspace/datasets/k6ng-warm20-v11",
    "profile_sha256": "db7eff51d496760a48bfb1c54dfb7c9dfcfb1fee83b099b4f6a346d7737cb46d",
    "completed_at": time.time(),
}
path.parent.mkdir(parents=True, exist_ok=True)
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY
rm -rf -- "$stage"
rm -f -- "$manifest_file"
echo "clone_complete"
