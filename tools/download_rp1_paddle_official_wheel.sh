#!/usr/bin/env bash
set -euo pipefail

url=https://paddle-whl.cdn.bcebos.com/stable/cu129/paddlepaddle-gpu/paddlepaddle_gpu-3.4.0-cp312-cp312-linux_x86_64.whl
cache_dir=/workspace/cache/paddle-official
wheel=$cache_dir/paddlepaddle_gpu-3.4.0-cp312-cp312-linux_x86_64.whl
parts_dir=$wheel.parts
expected_size=3345865395
expected_crc32=1844265352
connections=${RP1_PADDLE_DOWNLOAD_CONNECTIONS:-32}

mkdir -p "$cache_dir" "$parts_dir"
exec 9>"$cache_dir/.paddle-3.4.0-cp312-cu129-download.lock"
flock 9

verify_wheel() {
  test -s "$wheel" || return 1
  test "$(stat -c %s "$wheel")" = "$expected_size" || return 1
  python3 - "$wheel" "$expected_crc32" <<'PY'
import pathlib
import sys
import zipfile
import zlib

path = pathlib.Path(sys.argv[1])
expected = int(sys.argv[2])
crc = 0
with path.open('rb') as stream:
    while block := stream.read(8 * 1024 * 1024):
        crc = zlib.crc32(block, crc)
assert crc & 0xFFFFFFFF == expected, (crc & 0xFFFFFFFF, expected)
with zipfile.ZipFile(path) as archive:
    assert archive.testzip() is None
PY
}

if verify_wheel; then
  exit 0
fi

rm -f "$wheel"
chunk_size=$(((expected_size + connections - 1) / connections))

download_part() {
  local index=$1 start end target part current request_start
  start=$((index * chunk_size))
  end=$((start + chunk_size - 1))
  if (( end >= expected_size )); then end=$((expected_size - 1)); fi
  if (( start > end )); then return 0; fi
  part=$(printf '%s/part-%03d' "$parts_dir" "$index")
  target=$((end - start + 1))
  current=$(stat -c %s "$part" 2>/dev/null || echo 0)
  if (( current > target )); then
    rm -f "$part"
    current=0
  fi
  while (( current < target )); do
    request_start=$((start + current))
    if ! curl --fail --location --silent --show-error \
      --connect-timeout 30 --speed-limit 1024 --speed-time 180 \
      --range "${request_start}-${end}" "$url" >>"$part"; then
      # curl may already have appended a valid prefix. Recompute the byte
      # offset and resume that exact range instead of downloading it again.
      current=$(stat -c %s "$part" 2>/dev/null || echo 0)
      sleep 2
      continue
    fi
    current=$(stat -c %s "$part")
    if (( current > target )); then
      echo "range response exceeded expected size for part $index" >&2
      return 1
    fi
  done
}

pids=()
for ((index = 0; index < connections; index++)); do
  download_part "$index" &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=1
done
test "$failed" = 0

assembled=$wheel.assembling
rm -f "$assembled"
for ((index = 0; index < connections; index++)); do
  cat "$(printf '%s/part-%03d' "$parts_dir" "$index")" >>"$assembled"
done
mv "$assembled" "$wheel"
verify_wheel
rm -rf "$parts_dir"
printf '%s\n' "$wheel"
