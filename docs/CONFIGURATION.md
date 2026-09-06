# 설정 레퍼런스

[README로 돌아가기](../README.md)

현재 구현의 JSON 필드와 정책을 설명합니다. 경로는 별도 설명이 없는 한 **실행 서버 기준**입니다.
RAM·VRAM 단위는 MiB, 시간 단위는 초입니다. 지원하지 않는 필드는 등록 시 오류로 처리합니다.

## 서버 설정

전체 예제: [node.ssh.json](../examples/node.ssh.json).

| 필드 | 기본값 | 의미 |
|---|---|---|
| `id` | 필수 | 서버 식별자 |
| `transport` | `ssh` | `ssh` 또는 `local` |
| `target` | SSH일 때 필수 | SSH alias. port·key·사용자는 `~/.ssh/config`에 설정 |
| `python` | `python3` | 원격 조회·실행 도우미용 Python 3.10+ |
| `work_root` | 필수 | 실행별 로그·설정·결과를 저장할 전용 절대 경로 |
| `enabled` | `false` | 신규 작업 배치 허용 여부 |
| `max_jobs` | `1` | 서버당 active/unknown attempt 수 상한 |
| `cpu_limit` | 생략 | 관측 CPU 용량에 적용할 추가 상한 |
| `ram_limit_mib` | 생략 | RAM admission 계산에 적용할 추가 상한 |
| `labels` | `{}` | 환경·스토리지 등의 사용자 분류. job의 `labels`와 대조 |
| `gpus` | `[]` | 등록 GPU 목록. `discover --apply`로 조회·등록 가능 |
| `datasets` | `{}` | `{데이터셋 이름: 실제 절대 경로}` |
| `assets` | `{}` | `{asset 이름: {path, sha256}}` 형식의 파일 검증 계약 |
| `storage_domain` | `""` | dependency 출력에 같은 절대 경로로 접근할 수 있는 공통 저장소 이름 |
| `startup_group` | `""` | 초기 읽기 부하·시작 간격을 공유할 등록된 그룹 ID |
| `policy`, `recovery` | 아래 기본값 | 자원·health 기준과 장애 처리 설정 |

GPU 항목에는 `uuid`, `index`, `memory_mib`를 포함하고 `name`, `enabled`를 지정할 수 있습니다.
`enabled` 기본값은 false입니다. 전체 NVIDIA GPU UUID를 사용하며 MIG 장치는 지원하지 않습니다.

`register-node`로 전체 설정을 교체하거나 `discover --apply`로 GPU 목록을 다시 찾는 작업은
active/unknown attempt가 없는 시점에 수행합니다. `set-gpu`, `drain-node`, `set-dataset`은
향후 admission만 바꾸고 이미 생성된 attempt의 서버·GPU·경로 snapshot은 수정하지 않습니다.

### 서버별 데이터셋 경로

서버 설정의 일부 예시입니다.

```json
{
  "datasets": {
    "vehicle-v1": "/srv/datasets/vehicle-v1",
    "weather-v2": "/mnt/datasets/weather-v2"
  }
}
```

job에 `"dataset": "vehicle-v1"`을 넣으면 해당 node의 경로로 해석합니다.
파일·디렉터리·정상 symlink를 사용할 수 있습니다. probe에서 존재·접근 가능 여부를 확인하고,
child 실행 직전 다시 확인합니다. 같은 이름이 내용 동일성을 보증하지는 않습니다.
변경은 `set-dataset NODE NAME PATH`로 수행할 수 있습니다.

한 job의 `dataset`은 한 이름을 선택합니다. 여러 split은 같은 root 아래의
`{dataset_path}/train`, `{dataset_path}/val` 등을 config에 지정할 수 있습니다.
독립적인 여러 데이터셋 이름을 한 job에서 동시에 해석하는 기능은 아직 없습니다.

기존 `dataset_path` 직접 지정은 이름 해석·새 경로 preflight 없이 그대로 전달합니다.
`dataset`과 `dataset_path`를 동시에 지정하면 오류입니다. 데이터 복사·다운로드·symlink 생성은 하지 않습니다.
선택적 `input_files`로 manifest hash를 확인할 수 있지만, 이것만으로 전체 데이터 내용이나 split
동등성을 보증하지는 않습니다.

### Health 및 자원 정책

다음 필드는 node의 `policy` 객체 안에 넣습니다.

| 필드 | 기본값 | 의미 |
|---|---:|---|
| `stable_polls` | 3 | 신규 시작에 필요한 연속 정상 관측 횟수 |
| `max_snapshot_age_s` | 60 | 배치에 사용할 자원 snapshot의 최대 나이 |
| `max_cpu_percent` | 90 | 신규 배치 시 CPU 사용률 상한 |
| `max_gpu_percent` | 10 | 신규 배치 시 GPU 사용률 상한 |
| `min_free_ram_mib` | 1024 | 예약 후 남겨 둘 RAM 여유 |
| `min_free_disk_mib` | 1024 | 작업 파일시스템의 최소 여유 공간 |
| `gpu_margin_mib` | 1024 | GPU별 VRAM 안전 여유 |
| `max_idle_used_mib` | 256 | exclusive GPU 선택 시 허용 기존 VRAM 사용량 |
| `allow_gpu_sharing` | false | job의 shared GPU 요청 허용 |
| `allow_external_gpu_processes` | false | compute PID가 보이는 GPU의 shared 배치 허용 |
| `read_probe_path` | 생략 | 선택적 스토리지 읽기 점검 파일 |
| `read_probe_bytes` | 67108864 | 읽기 점검 크기, 기본 64 MiB |
| `read_probe_timeout_s` | 10 | 읽기 점검 timeout |

예약은 배치 기준이며 cgroup 등의 강제 제한이 아닙니다. host `MemAvailable`에는 실행 중 RSS가
이미 반영되므로, scheduler-owned process group의 RSS를 확인할 수 있으면 선언한 RAM peak까지
남은 성장분만 추가 예약합니다. RSS 측정이 없으면 전체 예약량을 차감해 보수적으로 대기합니다.
GPU sharing의 VRAM은 PID namespace 때문에 attempt별 attribution이 어려워 관측 사용량과
예약량을 더 보수적으로 함께 반영합니다.
`max_gpu_percent`는 shared mode에도 적용됩니다.

`policy.allow_external_gpu_processes: true`인 node에서는 compute PID가 보여도 사용률이
`max_gpu_percent` 이하이고 관측 free VRAM이 `vram_mib + gpu_margin_mib`를 충족하면 배치할 수
있습니다. `exclusive` job끼리는 여전히 GPU 하나당 scheduler reservation 하나만 허용합니다.
CLI에서는 `set-external-gpu-processes NODE enabled|disabled`로 향후 admission을 바꿀 수 있습니다.
GPU 안전 여유는 `set-gpu-margin NODE MIB`로 변경할 수 있으며 이미 생성된 attempt에는
영향을 주지 않습니다. 외부 프로세스가 VRAM을 더 사용할 수 있으므로 0보다는 보수적인 값을 권장합니다.

여러 scheduler job 자체를 같은 GPU에 두려면 추가로 node의 `policy.allow_gpu_sharing: true`와
job의 `resources.gpu_mode: "shared"`를 함께 지정합니다. PID namespace가 다를 수 있어 외부
프로세스의 소유자를 자동으로 신뢰하지 않습니다. 두 설정 모두 다른 프로세스를 종료하지 않지만
자원 경쟁과 외부 프로세스의 VRAM 증가 위험을 허용하므로 특정 node에만 명시적으로 적용하세요.

telemetry는 `/proc`, affinity와 `nvidia-smi` 기준입니다. cgroup v2 CPU/RAM 한도도 가능한
경우 반영하지만 v1/nested 환경이나 더 작은 사용자 할당량은 `cpu_limit`/`ram_limit_mib`로
명시해야 합니다. 2초 안의 빠른 반복 조회는 새로운 stable poll로 세지 않습니다.

### 장애 복구 정책

다음 필드는 node의 `recovery` 객체 안에 넣습니다.

| 필드 | 기본값 | 의미 |
|---|---|---|
| `ssh_attempts_per_round` | `3` | 재시도 묶음당 횟수 |
| `ssh_retry_offsets_s` | `[0, 300, 600, 900]` | 최초 장애 탐지 후 묶음 시작 시점 |
| `ssh_timeouts_s` | `[30, 60, 90]` | 각 응답 timeout. 묶음당 횟수와 배열 길이가 같아야 함 |
| `d_state_timeout_s` | `600` | 연속 D-state 사용 불가 판정 시간 |
| `d_observation_max_gap_s` | `120` | 연속 관찰로 인정할 최대 공백 |

연결 timeout은 각 응답 timeout의 1/3을 정수화하되 최소 5초로 설정합니다.
재시도 상태·다음 시각은 DB에 보존됩니다. 기본 설정은 최초 장애 probe 이후 추가 12회이며,
15분째 시작하는 마지막 묶음의 timeout 이후 사용 불가로 판정합니다.
상태 전이와 결과 차단은 [STATE_MACHINE.md](STATE_MACHINE.md)를 참고하세요.

## 실험과 job 설정

### Experiment

| 필드 | 기본값 | 의미 |
|---|---|---|
| `id`, `name`, `rq`, `jobs` | 필수 | 실험 ID·이름·질문과 하나 이상의 job |
| `project` | `general` | 연구 그룹 이름 |
| `priority` | `0` | 실험 공통 우선순위 |
| `tags` | `[]` | 사용자 분류용 문자열 목록 |

ID는 1~96자의 영문·숫자·`_`·`.`·`-`로 구성하고 첫 글자는 영문 또는 숫자여야 합니다.
job ID는 DB 전체에서 고유해야 합니다. 모든 dependency가 기존 DB 또는 같은 등록 payload
안에 있어야 하며 순환 의존성은 허용하지 않습니다.

### Job

| 필드 | 기본값 | 의미 |
|---|---|---|
| `id`, `name`, `kind`, `argv`, `cwd` | 필수 | 식별자·이름·종류·명령 배열·절대 작업 경로 |
| `kind` | 필수 | `train`, `eval`, `prepare`, `analysis` 중 하나 |
| `purpose` | `""` | 확인할 질문. 미입력 시 status에 실험 RQ 표시 |
| `config` | `{}` | 실행별 `config.json`에 저장할 값 |
| `env` | `{}` | 연구 명령에 전달할 문자열 환경 변수 |
| `dataset` / `dataset_path` | `""` | 서버별 경로를 사용할 이름 또는 직접 경로. 하나만 지정 |
| `depends_on` | `[]` | 성공해야 하는 선행 job ID 목록 |
| `priority` | `0` | 실험 우선순위에 더할 job 우선순위 |
| `hosts` | `[]` | 실행 가능한 node ID 목록. 빈 목록이면 이 제한 없음 |
| `labels` | `{}` | node label에 정확하게 일치해야 하는 key/value |
| `resources` | 아래 기본값 | GPU·CPU·RAM 요청 |
| `outputs` | `[]` | attempt 폴더 안에 생성해야 할 상대 파일 경로 |
| `input_files` | `[]` | 실행 전 SHA256을 확인할 `{path, sha256}` 목록 |
| `assets` | `{}` | 사용할 node asset의 `{이름: 기대 SHA256}` |
| `max_attempts` | `1` | 최초 시도·확인된 실패 재시도·node failover를 포함한 전체 시도 상한 |
| `failover_safe` | `false` | node 장애 시 새 attempt로 재배치해도 안전하다는 운영자 확인 |
| `metadata` | `{}` | 사용자 메모. 배치 기준이나 실행 인수로 자동 해석하지 않음 |

`argv`는 실행 파일과 인수의 배열이며 shell 문자열로 eval하지 않습니다.
`config.json` 생성만으로 설정이 학습에 적용되지는 않으므로 `--config {config_path}` 등의
인수를 연구 코드에 연결해야 합니다. 재시도 시 배치 크기·LR·epoch를 자동 조정하지 않습니다.

### Resources

| 필드 | 기본값 | 의미 |
|---|---|---|
| `gpu_count` | `0` | 같은 서버에서 필요한 GPU 수 |
| `vram_mib` | `0` | GPU 한 개당 예상 VRAM. GPU job에는 양수로 명시 |
| `cpu` | `1` | 필요한 논리 CPU 수 |
| `ram_mib` | `512` | 예상 host RAM |
| `gpu_mode` | `exclusive` | `exclusive` 또는 `shared` |
| `parameter_count` | 생략 | 참고용 parameter 수. VRAM 자동 추정에는 사용하지 않음 |

`gpu_count: 2`는 두 GPU 메모리를 합쳐 요구를 만족하는 뜻이 아닙니다. 각 GPU가 VRAM 요구를
충족해야 합니다. DDP launcher와 per-device/global batch는 사용자 명령에 명시합니다.

### 치환 값과 환경 변수

치환은 `argv`, `cwd`, `env`의 값, `config` 안의 문자열, `input_files[].path`에서 수행합니다.
`config`의 중첩 객체·배열도 지원합니다. `outputs`에는 치환 값이 아닌 상대 파일명을 지정합니다.

| 치환 값 | 의미 |
|---|---|
| `{attempt_dir}` | 이번 실행만의 출력 폴더 |
| `{config_path}` | 이번 실행의 `config.json` |
| `{dataset_path}` | 서버별로 해석한 데이터 경로 또는 직접 경로 |
| `{gpus}` | 할당된 GPU UUID들을 쉼표로 연결한 문자열 |
| `{gpu_count}` | 할당 GPU 수 |
| `{dep:job-id}` | 성공한 선행 job의 attempt 폴더 |

실행 환경에는 `CUDA_VISIBLE_DEVICES`와 아래 값이 제공됩니다.

- `RS_ATTEMPT_ID`, `RS_ATTEMPT_DIR`: 실행 ID·출력 폴더.
- `RS_CONFIG_PATH`, `RS_DATASET_PATH`: 설정 JSON·선택한 데이터 경로.
- `RS_READY_PATH`: 공유 스토리지 시작 gate에 준비 완료를 알릴 파일 경로.

사용자 `env`로 `CUDA_VISIBLE_DEVICES` 또는 `RS_*`를 덮어쓸 수 없습니다.
민감한 값은 DB와 실행 명세에 남을 수 있으므로 설정에 credential을 직접 넣지 마세요.

## 공유 스토리지 설정

`storage_domain`과 `startup_group`은 서로 다른 기능입니다.

| 설정 | 답하는 질문 |
|---|---|
| `storage_domain` | 다른 서버의 dependency 결과를 같은 절대 경로로 읽을 수 있는가? |
| `startup_group` | 새 작업의 초기 데이터 읽기를 서버 간에 순서대로 시작해야 하는가? |

### Dependency 결과 공유

실제로 같은 NFS namespace를 공유하고 **같은 절대 경로로 결과에 접근하는 서버에만**
같은 `storage_domain`을 지정합니다. 이 값은 mount나 경로 변환을 수행하지 않습니다.
기본값 `""`이면 다른 서버의 local 결과를 사용할 수 있다고 가정하지 않습니다.
데이터셋 경로를 등록했다고 checkpoint도 자동으로 전달되는 것은 아닙니다.

### 초기 읽기 부하 제한

[그룹 예제](../examples/shared-storage-group.json)를 등록합니다. `SCHEDULER_DB`는 README에서
설정한 같은 local DB를 사용합니다.

```bash
research-scheduler --db "$SCHEDULER_DB" register-group examples/shared-storage-group.json
```

관련 node에 `startup_group: "shared-storage-a"`를 설정합니다. 아래는 node JSON에 추가하는
필드의 예시이며 기존 `policy` 값과 합쳐 사용합니다.

```json
{
  "startup_group": "shared-storage-a",
  "policy": {
    "read_probe_path": "/srv/shared/health/read-probe.bin",
    "read_probe_bytes": 67108864,
    "read_probe_timeout_s": 10
  }
}
```

점검 파일은 운영자가 준비해야 하며 지정 크기 이상이어야 합니다. 스케줄러는 파일을 만들지
않습니다. 그룹 기본값은 `min_start_interval_s: 60`, `require_ready_marker: true`입니다.
GPU를 기다리는 job은 시작 slot을 잡지 않으며 한 번에 하나의 cold start만 허용합니다.
member의 읽기 장애·D-state·연락 불명은 다른 member의 신규 시작도 막을 수 있습니다.

준비 완료 marker는 import 성공이 아니라 실제 optimizer step 등 유효한 진행을 확인한 시점에
연구 wrapper가 생성해야 합니다. `RS_READY_PATH`가 제공된 실행에서의 예시입니다.

```python
import os
from pathlib import Path

# 실제 학습 진행을 확인한 뒤 호출합니다.
Path(os.environ["RS_READY_PATH"]).touch()
```

marker 이후 정상 health poll을 확인하면 시작 slot을 해제합니다. marker가 없으면 job 종료까지
유지합니다. `require_ready_marker: false`이면 marker 없이 running 상태와 정상 health poll로
해제하지만, 이것만으로 실제 학습 진행을 보증하지는 않습니다.

## 재배치 안전성

자동 failover를 허용할 job은 `failover_safe: true`와 필요한 `max_attempts`를 지정합니다.
선언한 output이 있어야 하며 모든 가변 출력을 `{attempt_dir}` 안에 분리해야 합니다.
고정 shared checkpoint 덮어쓰기, 외부 DB 갱신 등의 부작용이 있는 명령에는 사용하지 마세요.
이 계약은 임의 연구 코드의 부작용을 자동 검증하는 sandbox가 아닙니다.

node 장애로 invalid가 된 attempt는 원격에서 계속 실행될 수 있습니다. 새 attempt는 별도
폴더를 사용하고 invalid 결과를 채택하지 않지만 중복 계산 자체를 물리적으로 막는 것은 아닙니다.
격리 해제는 운영자 확인 후 별도 `readmit-node` 명령으로 수행합니다.
