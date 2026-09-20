#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 URL EXPECTED_SIZE EXPECTED_SHA256 DESTINATION" >&2
  exit 2
fi
url=$1
expected_size=$2
expected_sha256=$3
wheel=$4
connections=${WHEEL_DOWNLOAD_CONNECTIONS:-16}
parts_dir=$wheel.parts

mkdir -p "$(dirname "$wheel")" "$parts_dir"
exec 9>"$wheel.lock"
flock 9
if [[ -s "$wheel" ]] && [[ $(stat -c %s "$wheel") = "$expected_size" ]] && \
   printf '%s  %s\n' "$expected_sha256" "$wheel" | sha256sum -c - >/dev/null; then
  exit 0
fi
rm -f "$wheel"
chunk_size=$(((expected_size + connections - 1) / connections))

download_part() {
  local index=$1 start end target part current request_start
  start=$((index * chunk_size))
  end=$((start + chunk_size - 1))
  if ((end >= expected_size)); then end=$((expected_size - 1)); fi
  if ((start > end)); then return 0; fi
  part=$(printf '%s/part-%03d' "$parts_dir" "$index")
  target=$((end - start + 1))
  current=$(stat -c %s "$part" 2>/dev/null || echo 0)
  if ((current > target)); then rm -f "$part"; current=0; fi
  while ((current < target)); do
    request_start=$((start + current))
    if ! curl --fail --location --silent --show-error \
      --connect-timeout 30 --speed-limit 1024 --speed-time 180 \
      --range "${request_start}-${end}" "$url" >>"$part"; then
      current=$(stat -c %s "$part" 2>/dev/null || echo 0)
      sleep 2
      continue
    fi
    current=$(stat -c %s "$part")
    ((current <= target)) || return 1
  done
}

pids=()
for ((index = 0; index < connections; index++)); do
  download_part "$index" &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
test "$failed" = 0

assembled=$wheel.assembling
rm -f "$assembled"
for ((index = 0; index < connections; index++)); do
  cat "$(printf '%s/part-%03d' "$parts_dir" "$index")" >>"$assembled"
done
test "$(stat -c %s "$assembled")" = "$expected_size"
printf '%s  %s\n' "$expected_sha256" "$assembled" | sha256sum -c - >/dev/null
mv "$assembled" "$wheel"
rm -rf "$parts_dir"
printf '%s\n' "$wheel"
