#!/usr/bin/env bash
set -euo pipefail

root=/home/jy/carla_online_switch
bootstrap_local=/home/jy/tmp/rp1-ac0tg5829-bootstrap-minimal.tar.xz
bootstrap_remote=/tmp/rp1-bootstrap-minimal.tar.xz
coco_script=$root/scheduler/tools/download_rp1_full_coco_from_hf.sh
input_verifier=$root/scheduler/tools/verify_rp1_onboarding_inputs.py
profile_repair=$root/scheduler/tools/repair_rp1_k6ng_profile_paths.py
runtime_installer=$root/scheduler/tools/install_rp1_official_runtime.sh
paddle_downloader=$root/scheduler/tools/download_rp1_paddle_official_wheel.sh
wheel_downloader=$root/scheduler/tools/download_verified_wheel_ranges.sh
requirements_local=$root/experiments/bootstrap_k6_nominal_gate_0919/rp1_pinned_requirements.txt
requirements_remote=/tmp/rp1-pinned-requirements.txt
full_profile_local=/home/jy/tmp/rp1-i3-k6-full-profile-v2.tar.xz
full_profile_remote=/tmp/rp1-i3-k6-full-profile-v2.tar.xz
receipt_local=/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_TRANSFER_RECEIPT_v1.json
coco_receipt_local=/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_FULL_COCO_RECEIPT_v1.json
profile_receipt_local=/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_PROFILE_V2_RECEIPT.json

# Idempotent terminal check: once V4 has succeeded and RP1 has been promoted,
# a service restart must exit cleanly instead of replaying onboarding stages.
if python3 - <<'PY'
import json
import sqlite3

db = "/home/jy/experiments/research_scheduler/state.db"
c = sqlite3.connect("file:" + db + "?mode=ro", uri=True)
node_row = c.execute("select spec from nodes where id='rp1'").fetchone()
job_row = c.execute(
    "select status from jobs where id='K6NG_RP1_FULL_PROFILE_VALIDATE_V4'"
).fetchone()
if not node_row or not job_row:
    raise SystemExit(1)
node = json.loads(node_row[0])
labels = node.get("labels", {})
assert job_row[0] == "succeeded"
assert node.get("enabled") is True and node.get("max_jobs") == 4
assert labels.get("admission_state") == "user_promoted_after_new_rp1_full_profile_validation"
assert labels.get("k6ng_full_profile_validation_attempt", "").startswith(
    "K6NG_RP1_FULL_PROFILE_VALIDATE_V4."
)
assert len(labels.get("k6ng_full_profile_validation_sha256", "")) == 64
PY
then
  echo new_rp1_onboarding_complete
  exit 0
fi

wait_for_matching_remote_file() {
  local local_path=$1 remote_path=$2
  local expected actual local_sha remote_sha
  expected=$(stat -c %s "$local_path")
  while true; do
    actual=$(ssh rp1_runpod "stat -c %s '$remote_path' 2>/dev/null || echo 0")
    if [[ "$actual" == "$expected" ]]; then break; fi
    sleep 15
  done
  local_sha=$(sha256sum "$local_path" | awk '{print $1}')
  remote_sha=$(ssh rp1_runpod "sha256sum '$remote_path'" | awk '{print $1}')
  test "$local_sha" = "$remote_sha"
}

if ! ssh rp1_runpod 'sha256sum /home/jy/carla_data/software/releases/bootstrap_k6_nominal_gate_v6_20260919/SHA256SUMS 2>/dev/null | grep -q "^0013b277f7d237f0f30d8a7a8f1c139d40524736fefc4f5b9e192338c028f3fc  "'; then
  rsync -a --partial --append-verify -e ssh "$bootstrap_local" "rp1_runpod:$bootstrap_remote"
  wait_for_matching_remote_file "$bootstrap_local" "$bootstrap_remote"
  ssh rp1_runpod "tar -xJf '$bootstrap_remote' -C /; unlink '$bootstrap_remote'"
fi
if ! ssh rp1_runpod 'sha256sum /workspace/datasets/i3-k6-full-v1/INPUTS.json 2>/dev/null | grep -q "^1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c  "'; then
  scp -q -C "$full_profile_local" "rp1_runpod:$full_profile_remote"
  wait_for_matching_remote_file "$full_profile_local" "$full_profile_remote"
  ssh rp1_runpod "mkdir -p /workspace/datasets/i3-k6-full-v1; tar -xJf '$full_profile_remote' -C /workspace/datasets/i3-k6-full-v1; unlink '$full_profile_remote'"
fi
scp -q "$coco_script" rp1_runpod:/tmp/download_rp1_full_coco_from_hf.sh
ssh rp1_runpod 'chmod 700 /tmp/download_rp1_full_coco_from_hf.sh; if ! pgrep -f "[d]ownload_rp1_full_coco_from_hf.sh" >/dev/null && ! test -s /workspace/RP1_FULL_COCO_READY.json; then nohup /tmp/download_rp1_full_coco_from_hf.sh >/workspace/rp1-full-coco.log 2>&1 </dev/null & fi'
scp -q "$input_verifier" rp1_runpod:/tmp/verify_rp1_onboarding_inputs.py
scp -q "$runtime_installer" rp1_runpod:/tmp/install_rp1_official_runtime.sh
scp -q "$paddle_downloader" rp1_runpod:/tmp/download_rp1_paddle_official_wheel.sh
scp -q "$wheel_downloader" rp1_runpod:/tmp/download_verified_wheel_ranges.sh
scp -q "$requirements_local" "rp1_runpod:$requirements_remote"
ssh rp1_runpod 'chmod 700 /tmp/install_rp1_official_runtime.sh /tmp/download_rp1_paddle_official_wheel.sh /tmp/download_verified_wheel_ranges.sh'

# Install the exact frozen environment on RP1 itself. Paddle's CDN is sometimes
# slow, so keep partial pip downloads and relaunch the same immutable request
# after transient HTTP/SSH failures instead of failing the whole onboarding.
deadline=$((SECONDS + 14400))
while ! ssh rp1_runpod '/workspace/runtime/paddle-cu129-py312/bin/python - <<"PY" >/dev/null 2>&1
import cv2, numpy, paddle, yaml
assert paddle.__version__ == "3.4.0"
assert paddle.device.cuda.device_count() == 2
PY'; do
  if (( SECONDS >= deadline )); then echo "runtime setup timeout"; exit 1; fi
  if ! ssh rp1_runpod 'pgrep -f "[i]nstall_rp1_official_runtime.sh" >/dev/null'; then
    ssh rp1_runpod 'chmod 700 /tmp/install_rp1_official_runtime.sh; nohup /tmp/install_rp1_official_runtime.sh >>/workspace/rp1-runtime-pip.log 2>&1 </dev/null &'
  fi
  sleep 30
done

while ! ssh rp1_runpod 'test -s /workspace/RP1_FULL_COCO_READY.json'; do
  if (( SECONDS >= deadline )); then echo "full COCO setup timeout"; exit 1; fi
  if ! ssh rp1_runpod 'pgrep -f "[d]ownload_rp1_full_coco_from_hf.sh" >/dev/null'; then
    scp -q "$coco_script" rp1_runpod:/tmp/download_rp1_full_coco_from_hf.sh
    ssh rp1_runpod 'chmod 700 /tmp/download_rp1_full_coco_from_hf.sh; nohup /tmp/download_rp1_full_coco_from_hf.sh >>/workspace/rp1-full-coco.log 2>&1 </dev/null &'
  fi
  sleep 30
done

ssh rp1_runpod 'sha256sum /home/jy/carla_data/software/releases/bootstrap_k6_nominal_gate_v6_20260919/SHA256SUMS | grep -q "^0013b277f7d237f0f30d8a7a8f1c139d40524736fefc4f5b9e192338c028f3fc  "; sha256sum /workspace/datasets/i3-k6-full-v1/INPUTS.json | grep -q "^1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c  "; sha256sum /workspace/carla_data/nuimages_robobev_continuous_g1_v1_20260903_experiments/balanced7_adapter_comparison_v2_20260904/data/seed_7301_balanced7/coco/train/balanced7_universal.coco.json | grep -q "^2bc8085763d5b53118a9f006673f34ac4fe40645b1fb95129dc41971dcb1a02b  "'
ssh rp1_runpod 'python3 /tmp/verify_rp1_onboarding_inputs.py'
scp -q rp1_runpod:/workspace/RP1_VALIDATION_DATA_READY.json "$receipt_local"
scp -q rp1_runpod:/workspace/RP1_FULL_COCO_READY.json "$coco_receipt_local"
scp -q "$profile_repair" rp1_runpod:/tmp/repair_rp1_k6ng_profile_paths.py
ssh rp1_runpod 'python3 /tmp/repair_rp1_k6ng_profile_paths.py'
scp -q rp1_runpod:/workspace/RP1_K6NG_PROFILE_V2_READY.json "$profile_receipt_local"

cd "$root/experiments/bootstrap_k6_nominal_gate_0919"
python3 register_new_rp1_profile_v4.py --execute

deadline=$((SECONDS + 14400))
while true; do
  status=$(python3 - <<'PY'
import sqlite3
c=sqlite3.connect('file:/home/jy/experiments/research_scheduler/state.db?mode=ro',uri=True)
r=c.execute("select status from attempts where job='K6NG_RP1_FULL_PROFILE_VALIDATE_V4' order by created desc limit 1").fetchone()
print(r[0] if r else 'pending')
PY
  )
  case "$status" in
    succeeded) break ;;
    failed|cancelled) echo "validation ended: $status"; exit 1 ;;
  esac
  if (( SECONDS >= deadline )); then echo "validation timeout"; exit 1; fi
  sleep 20
done

python3 finalize_new_rp1.py --execute
echo new_rp1_onboarding_complete
