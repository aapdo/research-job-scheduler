#!/usr/bin/env bash
set -euo pipefail

export PATH="$HOME/.local/bin:$PATH"
export HF_HUB_DOWNLOAD_TIMEOUT=120

repo=apdoa/continuous-cg-full-grid
revision=b24af737e954b71ab575fa96b022a5eb71645a5c
selection=/workspace/hf-selection-rp1
stage=/workspace/hf-download-rp1
receipt=/workspace/RP1_HF_DATA_READY.json
archive=/tmp/rp1-hf-selections.tar.xz

hf version >/dev/null
mkdir -p "$selection" "$stage" /workspace/carla_data
tar -xJf "$archive" -C /tmp
cp -a /tmp/rp1-ac0tg5829-hf-selections/. "$selection/"

chunks=(
  subset_chunk_000 subset_chunk_001 subset_chunk_002
  full_chunk_000 full_chunk_005 full_chunk_006 full_chunk_009
  full_chunk_014 full_chunk_015 full_chunk_021 full_chunk_022
)

for base in "${chunks[@]}"; do
  selected="$selection/$base.selected"
  checksums="$selection/$base.selected.sha256"
  test -s "$selected"
  test -s "$checksums"
  if (cd /workspace && sha256sum --quiet -c "$checksums") >/dev/null 2>&1; then
    echo "$base already verified"
    continue
  fi
  free_kib=$(df --output=avail /workspace | tail -n 1)
  test "$free_kib" -ge $((20 * 1024 * 1024))
  hf download "$repo" "data/$base.tar" \
    --repo-type dataset --revision "$revision" --local-dir "$stage"
  tar_path="$stage/data/$base.tar"
  test -s "$tar_path"
  tar -xf "$tar_path" -C /workspace -T "$selected"
  (cd /workspace && sha256sum --quiet -c "$checksums")
  unlink "$tar_path"
  echo "$base verified and archive removed"
done

for base in "${chunks[@]}"; do
  (cd /workspace && sha256sum --quiet -c "$selection/$base.selected.sha256")
done

python3 - "$receipt" "$repo" "$revision" <<'PY'
import json, pathlib, sys, time
path = pathlib.Path(sys.argv[1])
payload = {
    "status": "verified",
    "node": "rp1",
    "pod_id": "ac0tg5829ai597",
    "repo_id": sys.argv[2],
    "revision": sys.argv[3],
    "required_files": 284037,
    "profile_sha256": "1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c",
    "chunks": [
        "full_chunk_000", "full_chunk_005", "full_chunk_006", "full_chunk_009",
        "full_chunk_014", "full_chunk_015", "full_chunk_021", "full_chunk_022",
        "subset_chunk_000", "subset_chunk_001", "subset_chunk_002",
    ],
    "archives_retained": False,
    "verified_at": time.time(),
}
path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
PY

du -sh /workspace/carla_data
df -h /workspace
echo hf_data_complete
