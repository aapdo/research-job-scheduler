# Runpod 모델 노드 온보딩과 K6NG RP 사례

정리 기준: 2026-09-19. 이 문서는 RP2에서 K6NG 전체 profile을 준비하고 실제 학습을 시작한 방법을
재사용 가능한 절차로 정리한다. 아래 attempt 상태와 용량은 당시 audit이며 실시간 상태가 아니다.
현재 상태는 모델 DB와 fresh health snapshot을 다시 조회한다.

## 목표와 완료 조건

노드가 SSH와 `nvidia-smi`에 응답하는 것만으로 모델 실행 가능으로 판정하지 않는다. 다음 조건을
순서대로 충족해야 한다.

1. 사용자 사용 승인, GPU enabled와 fresh health 3회
2. 작업이 요구하는 Python/Paddle/CUDA 및 PaddleDetection import
3. immutable worker/runtime, W0, 입력 profile의 파일별 SHA-256 검증
4. profile에 기록된 실제 train/calibration 이미지 경로의 존재 확인
5. 스케줄러가 소유한 실제 GPU validation 성공
6. 성공 receipt를 해당 노드·작업 profile에 연결한 뒤 본 작업 배정

등록, 파일 전송, import 성공과 실제 GPU validation을 서로 다른 단계로 기록한다. 검증 전에는 일반
작업 후보로 승격하지 않으며, 검증 실패 이력을 성공으로 덮어쓰지 않는다.

## RP2에서 확인된 최종 구성

| 항목 | RP2 값 |
|---|---|
| 스케줄러 노드 / SSH target | `rp2` / `rp2_runpod` |
| Python | `/workspace/runtime/paddle-cu129-py312/bin/python` |
| Paddle / CUDA runtime | Paddle 3.4.0 / CUDA 12.9 |
| PaddleDetection | `/home/jy/carla_data/software/third_party/PaddleDetection_2.9` |
| K6NG release | `/home/jy/carla_data/software/releases/bootstrap_k6_nominal_gate_v6_20260919` |
| local profile | `/workspace/datasets/k6ng-warm20-v11` |
| profile SHA-256 | `db7eff51d496760a48bfb1c54dfb7c9dfcfb1fee83b099b4f6a346d7737cb46d` |
| full GPU validation | `K6NG_WARM20_RP2_VALIDATE_V11` |
| 검증 범위 | real images 48, joint optimizer 3 step, posthoc 20 step, checkpoint reload, exact warm start |

2026-09-19 노드 추가에 따라 기존 RP1은 실제 장비와 이력을 유지한 채 RP3로 이름만 변경됐다.
새 Pod `ac0tg5829ai597`이 RP1이다. 2026-09-20 V4 진단에서 node-local path binding,
실제 이미지 48장, optimizer 3+20 step, checkpoint reload와 exact warm-start를 통과한 뒤 활성화됐다.

| 현재 노드 | 의미 | runtime / 상태 |
|---|---|---|
| RP2 | 기존 RTX 5090 3-GPU Pod | 검증 완료 |

2026-09-20 사용자가 RP1·RP3 서버를 실제로 삭제해 모델 스케줄러 인벤토리에서도 제거했다.
과거 검증·attempt 기록의 node 이름은 감사 이력으로 유지한다. 현재 Runpod train 노드는 RP2뿐이며,
전체 1차 train pool은 RP2 → CPS2 → CPS1 → FARM9 순서다. 새 RP 노드는 동일한 온보딩 검증을
통과하고 명시적으로 train pool에 추가되기 전에는 실행 후보가 아니다.

## 1. 사전 조사

먼저 읽기 전용 DB 조회와 원격 probe를 분리한다.

- DB에서 `enabled`, GPU UUID/enabled, `work_root`, `storage_domain`, disk/RAM/temperature gate를 확인한다.
- SSH alias는 node의 `target`과 일치시킨다. Docker 이름이나 표시 이름을 SSH target으로 추측하지 않는다.
- `df`, `free`, `nvidia-smi`, boot ID를 읽고 활성 scheduler attempt와 외부 GPU PID를 구분한다.
- `/workspace`의 기존 데이터와 runtime을 조사한 후 전송 목록을 만든다.

RP2에는 약 46GB의 continuous corruption 데이터와 약 2.1GB의 원본 nuImages 데이터가 이미 있었다.
따라서 399GB 전체 shard를 새로 복제할 필요가 없었다. 필요한 것은 기존 파일을 가리키는 profile,
worker/runtime, PaddleDetection과 W0 payload였다.

## 2. Python 환경 정규화

초기 Runpod runtime은 Paddle/CUDA 최소 checkpoint 검증은 통과했지만 PaddleDetection 전체 import에
필요한 PyYAML, OpenCV, pycocotools, scikit-image 등이 없었고 NumPy가 2.x였다. K6NG/PaddleDetection
profile은 NumPy 1.26.4로 고정했다.

주요 고정 버전은 다음과 같다.

- `numpy==1.26.4`
- `PyYAML==6.0.3`
- `opencv-python-headless==4.10.0.84`
- `scipy==1.14.1`, `scikit-learn==1.5.2`, `scikit-image==0.24.0`
- `pycocotools==2.0.8`, `terminaltables==3.1.10`, `visualdl==2.5.3`
- `Pillow==11.0.0`, `setuptools==75.6.0`

`imgaug==0.4.0`은 metadata상 `opencv-python`을 요구하며 최신 wheel을 함께 설치하면 Python 3.12에서
NumPy 2.x로 다시 올라갈 수 있다. 이 환경에서는 headless OpenCV를 먼저 고정하고 `imgaug`, `lapx`,
`motmetrics`, `pyclipper`를 `--no-deps`로 설치한 뒤, 필요한 image dependency를 고정 버전으로 설치했다.
마지막에는 반드시 같은 interpreter로 다음을 확인한다.

```bash
"$RUNTIME_PYTHON" - <<'PY'
import sys
sys.path.insert(0, "/home/jy/carla_data/software/third_party/PaddleDetection_2.9")
import cv2, numpy, paddle, yaml, ppdet
print(paddle.__version__, numpy.__version__, cv2.__version__, yaml.__version__)
PY
```

import 성공은 CPU 수준 확인일 뿐 GPU execution receipt가 아니다.

새 RP 노드의 환경은 다른 RP 노드에서 복사하지 않는다. 대상 노드 내부에서 frozen requirements를
설치하되 채널을 분리한다. 일반 Python 패키지와 NVIDIA CUDA wheel은 공식 PyPI에서 받고,
`paddlepaddle-gpu` wheel만 Paddle 공식 CUDA 채널에서 `--no-deps`로 받는다. 완전한 freeze를
각각 `--no-deps`로 설치한 뒤 모든 distribution 버전, Paddle import, CUDA device 수와 실제 tensor
연산을 다시 검증한다. `--extra-index-url` 하나에 두 채널을 섞으면 NVIDIA wheel까지 느린 Paddle CDN을
선택할 수 있으므로 사용하지 않는다. Paddle wheel처럼 큰 파일은 대상 노드가 공식 CDN에서 resumable
HTTP range로 병렬 다운로드하고, 공식 응답의 파일 크기·CRC32와 wheel ZIP 무결성을 모두 확인한 뒤
설치한다. RP1↔RP2/RP3 직접 환경·데이터 복제나 SSH 전송은 사용하지 않는다. 온보딩 데이터는 HF의
pinned revision과 압축 artifact를 우선 사용하고, HF에 없는 입력만 원본 보유 서버에서 controller-local
bounded relay용 단일 압축 묶음으로 전달한다.

## 3. controller staging과 전송

서로 다른 local filesystem 사이의 전송은 다음 순서를 사용한다.

```text
검증된 원본 서버 → 제어 머신의 bounded staging → Runpod 노드
```

RP2/RP1에 전달한 작은 구성 요소는 다음과 같다.

- frozen K6NG source/release: 약 77MB
- PaddleDetection 2.9: 약 122MB
- PicoDet-S W0 payload: 약 4.6MB

profile archive는 `INPUTS.json`만으로 완전하지 않다. worker가 참조하는 W0는 해당 노드 release의
실제 경로와 SHA가 일치해야 하며, `published/s_cda_suite_v2`의 `READY.json`, `BASIS_READY.json`,
`BOOTSTRAP.pdparams`, `BOOTSTRAP_RESULT.json`, `NORMALIZATION.json`, `SUITE_BASIS.npz`도 plan SHA와
개별 파일 SHA를 검증해 같은 profile root에 포함해야 한다. RP1 V2는 오래된 W0 절대경로 때문에,
V3는 이 공통 artifact 누락 때문에 실패했다. 실패 attempt는 보존하고, 둘을 모두 바로잡은 immutable
`i3-k6-full-v2-rp1` profile과 V4 진단으로 승격했다. 이후 온보딩은 profile 디렉터리 존재만으로
준비 완료를 선언하지 않고 이 전체 계약을 검사한다.
- K6NG 전용 metadata/profile: 약 112MB

staging에서 상대 경로 기준 manifest를 만들고 target에서 전체 검증한다.

```bash
cd "$STAGING_ROOT"
find <approved-roots...> -type f -print0 | sort -z | xargs -0 sha256sum > MANIFEST.sha256
rsync -aH --relative <approved-roots...> "$TARGET":/
ssh "$TARGET" 'cd / && sha256sum --quiet -c /workspace/<manifest>'
```

target 검증과 receipt 저장이 모두 끝난 뒤에만 controller staging을 삭제한다. source와 실패 attempt,
원격 실행 로그는 삭제하지 않는다. 비밀번호, token, private key는 manifest나 receipt에 기록하지 않는다.

환경과 데이터를 같은 방식으로 배포하지 않는다. Python/Paddle 환경은 대상 RP 노드가 frozen requirements와
공식 package index로 직접 설치한다. 데이터는 HF pinned revision의 압축 chunk를 우선 사용한다. HF에 없는
입력만 원본 서버에서 tar.zst/tar.xz 한 묶음으로 만들고 백그라운드 전송하며, 파일별 반복 scp는 금지한다.
대용량 전체 replica를 노드 온보딩 선행 조건으로 만들지 않고 실제 작업 manifest가 요구하는 범위만 준비한다.

### 새 RP1의 HF 데이터 준비

대용량 이미지 데이터는 공개 HF dataset
`apdoa/continuous-cg-full-grid`의 고정 revision
`b24af737e954b71ab575fa96b022a5eb71645a5c`에서 받는다. profile이 실제 참조하는 284,037개 파일을
manifest와 대조한 결과 필요한 tar chunk 11개만 받는다. 각 chunk는 한 번에 하나씩 내려받아 필요한
member만 `/workspace/carla_data`에 추출하고, 제공된 파일별 SHA-256을 모두 확인한 뒤 tar를 삭제한다.
이 방식은 399GB 전체 grid 복제와 여러 17GB tar의 동시 보관을 피한다.

HF token은 노드의 0600 credential 파일로만 전달하고 명령행·receipt·로그에 기록하지 않는다.
노드 온보딩 자체는 284,037개 전체를 미리 받지 않는다. 완전 COCO, calibration metadata 압축본과
실제 GPU smoke가 소비하는 48개 이미지만 먼저 검증한다. 일반 train/eval 데이터는 작업별 manifest로
필요한 HF 압축 chunk만 받아 SHA 검증한 뒤 시작한다. runtime/release/profile은 별도 SHA 검증 대상이며
일부 데이터 준비만으로 일반 작업의 입력 호환성을 승인하지 않는다.
따라서 bounded smoke 성공만으로 node의 일반 dataset catalog에 전체 replica를 등록하지 않는다.
각 작업의 HF download/압축 relay receipt가 완성된 뒤에만 그 execution profile의 후보로 추가한다.

탐지 학습·평가가 소비하는 COCO annotation은 이미지 선택 manifest와 별개다. RP2·RP3의
`/workspace/datasets/k6ng-warm20-v11/balanced7_images.json`은 `images` 85,848개만 포함하고
`annotations`와 `categories`가 없어 실행 profile의 annotation으로 사용할 수 없다. 새 RP1에서는 다음
완전한 COCO를 필수 입력으로 검증한다.

| 항목 | 고정 값 |
|---|---|
| 경로 | `/workspace/carla_data/nuimages_robobev_continuous_g1_v1_20260903_experiments/balanced7_adapter_comparison_v2_20260904/data/seed_7301_balanced7/coco/train/balanced7_universal.coco.json` |
| SHA-256 | `2bc8085763d5b53118a9f006673f34ac4fe40645b1fb95129dc41971dcb1a02b` |
| 구조 | images 85,848 / annotations 527,737 / categories 1 |
| HF source | `data/subset_chunk_002.tar`, 위와 같은 고정 revision |

파일명이나 존재만 확인하지 않는다. 세 COCO key와 개수, SHA-256이 모두 맞아야 transfer receipt를
발급한다. 이미지 목록만 있는 JSON은 relay 성공으로도, GPU validation 입력 준비 완료로도 판정하지 않는다.

## 4. host-local profile 변환

기존 FARM/LAB profile은 이미지 절대 경로가 `/data/jy/carla_data/...`였다. Runpod에는 동일한 검증된
데이터가 `/workspace/carla_data/...`에 있었으므로 이미지 bytes를 다시 복사하지 않고 다음 두 prefix만
새 immutable profile에서 변환했다.

| 원본 prefix | Runpod prefix |
|---|---|
| `/data/jy/carla_data/nuimages_robobev_2d_v1` | `/workspace/carla_data/nuimages_robobev_2d_v1` |
| `/data/jy/carla_data/nuimages_robobev_continuous_g1_v1_20260903` | `/workspace/carla_data/nuimages_robobev_continuous_g1_v1_20260903` |

원본 profile은 그대로 두고 RP2에는 `k6ng-warm20-v11`, 새 RP1에는 완전 COCO를 포함한
`i3-k6-full-v1`을 새로 만들었다. RP1 `INPUTS.json` SHA-256은
`1e868d99cb1d249201676d3a1731b45d317a8693d22fa4d4bcea3c50a0c9662c`이다. 변환 뒤 다음을 확인했다.

- train rows 85,848 / 파일 존재 85,848
- calibration rows 39,650 / 파일 존재 39,650
- train annotation과 122개 evaluation metadata의 새 SHA-256
- profile 전체 SHA-256

K6NG 학습은 calibration metadata를 split 생성에 사용한다. reporting 이미지가 RP 노드에 없더라도
학습 validation에는 필요하지 않으며, 최종 eval은 reporting 데이터가 검증된 기존 LAB/FARM 후보에서
수행한다. 작업별 소비 범위를 확인하지 않고 단순히 profile에 등장하는 모든 경로를 복제하지 않는다.

## 5. 스케줄러 GPU 검증과 승격

GPU 검증도 일반 스케줄러 job으로 등록한다. 원격 shell에서 worker를 직접 실행해 성공한 결과를
스케줄러 receipt로 대신하지 않는다.

검증 job은 다음 계약을 사용했다.

- host는 대상 RP node 하나로 제한
- `dataset_path`는 immutable Runpod profile
- release `SHA256SUMS`와 node asset SHA를 입력 계약으로 결합
- GPU 1개, VRAM 9GiB, TF32 off
- real 48-image batch, joint optimizer 3 step
- checkpoint save/reload 및 warm-start exactness 확인

검증 성공 후 `VALIDATED.json`의 경로와 SHA를 node asset/label에 기록한다. 이 receipt는 K6NG profile에
대한 것이며 다른 worker나 dataset을 자동 승인하지 않는다. 새 캠페인은 자기 execution profile과
작업별 host 검증을 계속 요구한다.

## 6. 실제로 발생한 실패와 교훈

| revision | 실패 | 수정 |
|---|---|---|
| RP2 V9 | `ModuleNotFoundError: yaml` | PaddleDetection 전체 의존성과 NumPy 1.26.4를 같은 runtime에 고정 |
| RP2 V10 | FARM/LAB 절대 이미지 경로가 profile에 남음 | 기존 `/workspace` 데이터로 두 prefix를 바꾼 immutable V11 생성 |
| RP2 V11 | 성공 | 검증 receipt 후 `K6NG_WARM20_TRAIN_V11`을 RP2에 배정 |
| RP1 images-only profile | `balanced7_images.json`에 COCO annotation/category 없음 | 완전 COCO를 포함하는 immutable `i3-k6-full-v1`으로 교체하고 key/count/SHA를 선검증 |

`current-rp123-farm9-full-v4` execution catalog는 불완전한 입력 검증을 피하기 위해 철회됐다. 새 RP1
온보딩이나 후속 캠페인에서 이 catalog를 다시 활성화하지 않는다. 완전 COCO receipt와 새 profile SHA를
사용한 작업별 validation revision을 등록한다.

V9/V10 실패를 job 성공으로 수정하지 않고 취소된 superseded revision으로 남겼다. stdout/stderr,
attempt state와 registration event도 보존한다. 본 학습은 partial predecessor optimizer를 가져오지 않고
승인된 K6 E20 source checkpoint에서 warm-start했다.

## 7. health, disk gate와 정리

- SSH recovery budget이 소진된 node는 실제 SSH가 복구되어도 `unavailable`에 남는다. 수동 SSH/GPU/RAM/
  disk 확인, 활성 attempt 부재 확인 후 recovery state를 readmit하고 fresh health 3회를 다시 쌓는다.
- 기존 16GiB local-disk gate를 쓰던 활성 모델 노드는 2026-09-19부터 8GiB를 남긴다.
  `set-min-free-disk NODE 8192`는 향후 배정에만 적용하고 실행 중 attempt를 변경하지 않는다.
- FARM6/7/8/9의 filesystem별 15% free 정책과 하드웨어 스케줄러 disk 정책은 별도이며 완화하지 않는다.
- 전송 후 사용하지 않는 task-local 복제본과 controller staging은 열린 파일/참조가 없음을 확인한 뒤
  지운다. 원본 데이터, 활성 attempt, 실패 로그와 receipt는 보존한다.

## 감사 자료

운영 receipt는 Git에 넣지 않고 다음 경로에 보관한다.

- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP2_TRANSFER_RECEIPT_v9.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP2_PATH_PROFILE_RECEIPT_v11.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/WARM20_RP2_REGISTERED_v11.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_PROFILE_RECEIPT_v1.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_PROFILE_VALIDATED_v1.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_TRANSFER_RECEIPT_v1.json`
- `/home/jy/experiments/bootstrap_k6_nominal_gate_20260919/RP1_AC0TG5829_FULL_COCO_RECEIPT_v1.json`

관련 등록 코드는 `experiments/bootstrap_k6_nominal_gate_0919/`에 있다. 실제 재사용 전에는 receipt의
날짜나 과거 서버 상태를 현재 승인으로 간주하지 말고 DB, node spec, profile SHA와 fresh health를 다시
대조한다.
