#!/usr/bin/env bash
set -euo pipefail

python=/workspace/runtime/paddle-cu129-py312/bin/python
uv=/usr/bin/uv
requirements=/tmp/rp1-pinned-requirements.txt
dependencies=/tmp/rp1-pypi-requirements.txt
paddle_downloader=/tmp/download_rp1_paddle_official_wheel.sh
paddle_wheel=/workspace/cache/paddle-official/paddlepaddle_gpu-3.4.0-cp312-cp312-linux_x86_64.whl
wheel_downloader=/tmp/download_verified_wheel_ranges.sh
pypi_cache=/workspace/cache/pypi-official
cusparselt_wheel=$pypi_cache/nvidia_cusparselt_cu12-0.7.1-py3-none-manylinux2014_x86_64.whl
nccl_wheel=$pypi_cache/nvidia_nccl_cu12-2.28.3-py3-none-manylinux_2_18_x86_64.whl

test -x "$python"
test -x "$uv"
test -s "$requirements"
test -x "$paddle_downloader"
test -x "$wheel_downloader"
grep -v '^paddlepaddle-gpu==' "$requirements" > "$dependencies"

# These are the two largest PyPI-only dependencies. Download their immutable
# PyPI files concurrently with range requests and verify PyPI's SHA256 before
# installing. This avoids turning a fresh-node setup into a long serial fetch.
"$wheel_downloader" \
  https://files.pythonhosted.org/packages/56/79/12978b96bd44274fe38b5dde5cfb660b1d114f70a65ef962bcbbed99b549/nvidia_cusparselt_cu12-0.7.1-py3-none-manylinux2014_x86_64.whl \
  287193691 f1bb701d6b930d5a7cea44c19ceb973311500847f81b634d802b7b539dc55623 \
  "$cusparselt_wheel" &
cusparselt_pid=$!
"$wheel_downloader" \
  https://files.pythonhosted.org/packages/5d/24/11df42593d1a6d10b3ffef049cec064832f108e77bc5cac12726e4ec1cb2/nvidia_nccl_cu12-2.28.3-py3-none-manylinux_2_18_x86_64.whl \
  295901337 79cf0412094e4a552889e5cb7757d92c010ead557ec722c5eebe6a94b1d8681c \
  "$nccl_wheel" &
nccl_pid=$!
wait "$cusparselt_pid"
wait "$nccl_pid"
"$uv" pip install --python "$python" --no-deps "$cusparselt_wheel" "$nccl_wheel"

# Frozen third-party and NVIDIA wheels come from their canonical PyPI index.
# --no-deps is intentional: the complete, exact environment is already frozen.
env UV_HTTP_TIMEOUT=600 "$uv" pip install --python "$python" \
  --default-index https://pypi.org/simple --no-deps -r "$dependencies"

# Paddle itself is downloaded directly by RP1 from Paddle's official CUDA 12.9
# CDN. The downloader uses resumable HTTP ranges and validates the official
# Content-Length/CRC32 fingerprint plus the wheel ZIP before installation.
"$paddle_downloader"
"$uv" pip install --python "$python" --no-deps "$paddle_wheel"

"$python" - "$requirements" <<'PY'
import importlib.metadata
import pathlib
import sys

for line in pathlib.Path(sys.argv[1]).read_text().splitlines():
    name, expected = line.split('==', 1)
    actual = importlib.metadata.version(name)
    assert actual == expected, (name, expected, actual)

import cv2
import numpy
import paddle
import yaml
assert paddle.__version__ == '3.4.0'
assert numpy.__version__ == '1.26.4'
assert cv2.__version__ == '4.10.0'
assert paddle.device.cuda.device_count() == 2
paddle.set_device('gpu:0')
value = paddle.arange(256, dtype='float32').reshape([16, 16])
assert float((value @ value).sum()) > 0
print('rp1_official_runtime_ready')
PY

# The installed environment is authoritative; retain no multi-gigabyte wheel
# after version/import/CUDA execution validation succeeds.
rm -f "$paddle_wheel"
rm -f "$cusparselt_wheel" "$nccl_wheel"
echo complete > /workspace/RP1_OFFICIAL_RUNTIME_READY
