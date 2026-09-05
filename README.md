# Research Job Scheduler

연구 코드와 분리된 **단일 운영자용 Linux/NVIDIA 실험 스케줄러 MVP**입니다.
이 폴더만 다른 저장소로 옮겨 사용할 수 있습니다. CARLA, PicoDet, LAB/FARM 주소,
데이터셋 이름, 학습 framework에 대한 코드 의존성이 없습니다.

## 구현된 기능

- 서버 등록, SSH/local 실행, GPU 자동 조회 및 UUID별 등록·사용 허용.
- 서버별 데이터셋 경로 등록, 실험의 데이터셋 이름을 선택 서버의 경로로 해석.
- `project → experiment(name, RQ) → job(train/eval/prepare/analysis)` 구조.
  각 job의 `purpose`에 무엇을 확인하는 실험인지 기록합니다.
- 우선순위, 성공 의존성 DAG, CPU-only 작업, 동일 서버의 여러 GPU 동시 예약.
- GPU 사용률·VRAM·compute PID, CPU 사용률·RAM·디스크·D-state·선택적 스토리지 읽기 상태 조회.
- 미등록/비활성 GPU, 자원 부족, 의존성, 오래된 측정값 등 **대기 이유** 출력.
- SQLite 실행 이력, immutable attempt 디렉터리, stdout/stderr, 설정 JSON,
  numeric exit code와 결과 파일 SHA256.
- SSH 응답 장애의 단계별 재시도, 연속 D-state 10분 후 서버 격리.
- 이전 실행을 `invalid`로 확정한 뒤, 안전성이 명시된 작업만 다른 서버로 재배치.
- `plan`/기본 `tick`/기본 `daemon`은 모의 배치. **`--execute`를 주어야 실행**됩니다.

현재 LAB/FARM 실험과 기존 큐를 자동으로 가져오거나 옮기지 않습니다.
GUI, multi-user 인증, 다중 서버 DDP, cgroup 강제 제한, 선점/강제 종료,
자동 checkpoint resume는 아직 없습니다.

## 설치·검증

제어 머신과 실행 서버 모두 Python 3.10 이상이 필요합니다. 런타임은 표준 라이브러리만 사용합니다.

```bash
git clone https://github.com/aapdo/research-job-scheduler.git
cd research-job-scheduler
python3 -m venv .venv
.venv/bin/pip install -e .
source .venv/bin/activate
python3 -m unittest discover -s tests -v
```

설치 없이도 실행할 수 있습니다.

```bash
PYTHONPATH=src python3 -m research_scheduler --help
python3 examples/local_demo.py --directory runtime/demo
```

`local_demo.py`는 현재 머신에서 CPU 더미 학습 2개와 그 결과를 읽는 평가 2개만 실행합니다.
GPU 학습, SSH 서버 변경, 데이터셋 접근은 하지 않습니다. 실제 로컬 자원 probe를 사용합니다.

## 서버와 GPU 등록

SSH port·사용자·key·ProxyJump는 사용자의 `~/.ssh/config`에서 관리합니다.
스케줄러에는 해당 SSH alias와 Python 실행 경로를 등록합니다. 비밀번호/private key를 JSON에 넣지 않습니다.

```bash
research-scheduler --db /local/path/scheduler/state.db register-node examples/node.ssh.json
research-scheduler --db /local/path/scheduler/state.db discover research-node-a --apply
research-scheduler --db /local/path/scheduler/state.db inventory
research-scheduler --db /local/path/scheduler/state.db set-gpu research-node-a GPU-ACTUAL-UUID enabled
research-scheduler --db /local/path/scheduler/state.db enable-node research-node-a
```

예제 JSON의 주소와 경로는 설치 환경에 맞게 수정합니다. `discover`는 읽기 전용이며,
`--apply`도 조회된 GPU를 **disabled** 상태로 등록할 뿐 작업을 실행하지 않습니다.
실제 GPU UUID를 사용하므로 index가 바뀌어도 다른 장치를 무심코 선택하지 않습니다.
같은 UUID를 두 서버 이름에 중복 등록할 수 없습니다. 서버/컨테이너를 여러 SSH alias로
중복 등록하지 마세요. CPU-only alias의 물리 호스트 중복은 자동 판별하지 않습니다.

`max_jobs`와 `cpu_limit`, `ram_limit_mib`, `policy`는 서버별로 지정할 수 있습니다.
서버의 단순 물리 용량과 사용자에게 허용된 자원량이 다르면 더 작은 한도를 등록하세요.
GPU를 특정 연구만 사용하게 하려면 서버 `labels`와 job `labels`/`hosts`를 사용합니다.

## 실험 등록·실행

```bash
research-scheduler --db /local/path/scheduler/state.db register-experiment examples/experiment.json
research-scheduler --db /local/path/scheduler/state.db priority study-baseline 200
research-scheduler --db /local/path/scheduler/state.db plan
research-scheduler --db /local/path/scheduler/state.db status
research-scheduler --db /local/path/scheduler/state.db daemon --execute --interval 20
```

실제 환경에서는 준비된 immutable source release와 실행 환경 경로를 지정해야 합니다.
스케줄러는 저장소 checkout, 패키지 설치, 배치 크기/LR 변경을 수행하지 않습니다.
GPU 2개 요청은 자원을 예약하고 `CUDA_VISIBLE_DEVICES`를 설정하는 것이지,
단일 GPU 학습을 자동으로 DDP로 바꾸는 것이 아닙니다. 실행 명령에 `torchrun` 등
원래 연구의 launcher를 명시합니다.

우선순위는 `experiment.priority + job.priority`이며 높은 값이 먼저입니다.
동점은 등록 순서입니다. 불가능한 큰 작업을 건너뛰고 실행 가능한 독립 작업을 배치합니다.
aging/fair-share가 없으므로 높은 우선순위 작업이 계속 유입되면 낮은 작업이 지연될 수 있습니다.

백그라운드로 실행하려면 전용 tmux 세션에서 위 daemon 명령을 실행하고 detach할 수 있습니다.
daemon은 SIGTERM/SIGINT를 받으면 **새 배치를 멈추고 종료**하며 실행 중 child는 그대로 둡니다.
재시작할 때 같은 DB를 지정하면 attempt ID로 재확인합니다. 하나의 fleet에는 하나의
canonical DB만 사용하세요. 다른 DB를 사용하는 스케줄러끼리는 예약을 공유하지 않습니다.

## Job 설정

| 필드 | 의미 |
|---|---|
| `id`, `name`, `kind`, `purpose` | 고유 ID, 표시 이름, 학습/평가 등 종류, 확인할 질문 |
| `argv` | 실행 파일과 인수 배열. shell 문자열을 eval하지 않음 |
| `cwd`, `env`, `config` | 작업 폴더, 환경 변수, attempt에 저장할 JSON 설정 |
| `dataset` | 서버 `datasets`에 등록한 논리 이름. 선택 서버의 실제 경로로 해석 |
| `dataset_path` | 기존 직접 경로 지정 방식. `dataset`과 동시에 지정 불가 |
| `resources.gpu_count` | 같은 서버에서 필요한 GPU 수. 0이면 CPU-only |
| `resources.vram_mib` | **GPU 한 개당** 예상 VRAM. 합산 용량이 아님 |
| `resources.parameter_count` | 참고용 parameter 수. VRAM으로 자동 환산하지 않음 |
| `resources.cpu`, `ram_mib` | 필요한 논리 CPU 수와 RAM MiB |
| `depends_on` | 성공 및 선언한 결과 파일 확인 후 시작할 predecessor job ID |
| `outputs` | attempt 디렉터리 안의 상대 파일 경로. exit 0이어도 파일 없으면 실패 |
| `input_files` | 선택적 `{path, sha256}` 계약. 실행 전에 hash 확인 |
| `max_attempts` | 최초 실행을 포함한 전체 실행 횟수 상한. 기본 1 |
| `failover_safe` | 독립 출력·외부 부작용 없음에 대한 운영자 확인. 기본 false |

등록된 실험과 설정은 immutable합니다. 변경 실험은 새로운 ID로 등록합니다.
동일 JSON 재등록은 기존 완료 상태를 초기화하지 않습니다. 대기 job 우선순위 변경만 별도 명령으로 허용합니다.

사용 가능한 치환 값은 다음과 같습니다. `argv`, `env`, `cwd`, `config`의 문자열에서 치환합니다.

- `{attempt_dir}`, `{config_path}`: 이번 실행만의 출력 폴더와 설정 JSON.
- `{dataset_path}`: `dataset`으로 해석한 서버별 경로 또는 직접 지정한 `dataset_path`.
- `{gpus}`, `{gpu_count}`: 할당한 GPU UUID 목록과 개수.
- `{dep:job-id}`: 성공한 dependency의 attempt 폴더.

실행 환경에는 `RS_ATTEMPT_ID`, `RS_ATTEMPT_DIR`, `RS_CONFIG_PATH`, `RS_DATASET_PATH`,
`RS_READY_PATH`도 제공됩니다. `RS_*`와 `CUDA_VISIBLE_DEVICES`는 사용자 env로 덮어쓸 수 없습니다.

### 데이터셋과 결과 경로

서버마다 다른 경로를 사용할 때는 같은 논리 이름에 서버별 실제 경로를 등록합니다.
서버 등록 JSON에 `"datasets": {"vehicle-v1": "/data/vehicle"}`를 넣거나 아래 명령으로
등록·수정할 수 있습니다. 데이터셋 이름에 버전을 포함하는 것을 권장합니다.

```bash
research-scheduler --db /local/path/scheduler/state.db set-dataset research-node-a vehicle-v1 /data/vehicle
research-scheduler --db /local/path/scheduler/state.db set-dataset research-node-b vehicle-v1 /mnt/datasets/vehicle
```

실험의 job에는 물리 경로 대신 `dataset`을 지정하고 실행 명령/설정에서 치환 값을 사용합니다.
아래는 전체 job이 아닌 관련 필드의 예시입니다.

```json
{
  "dataset": "vehicle-v1",
  "argv": ["python3", "train.py", "--data-root", "{dataset_path}"],
  "config": {"data_root": "{dataset_path}"}
}
```

`plan`에 선택 서버와 해석된 `dataset_path`가 표시됩니다. 해당 이름이 미등록이거나,
원격 probe에서 경로 존재·접근 권한을 확인하지 못한 서버에는 해당 job을 배치하지 않습니다.
runner도 child 시작 전에 경로를 다시 확인합니다. 파일·디렉터리·정상 symlink 경로를 허용합니다.
`set-dataset`은 앞으로의 배치에만 적용되며, 이미 생성된 attempt의 명령·설정·경로는 변경하지 않습니다.

기존 `dataset_path` 직접 지정도 계속 지원합니다. 이 경우 서버별 해석이나 새 path preflight를
적용하지 않고 기존처럼 그대로 전달합니다. `dataset`과 `dataset_path` 중 하나만 지정하세요.
서버별 경로 매핑은 데이터셋 **복사·다운로드·symlink 생성·재샘플링을 수행하지 않습니다**.
같은 이름을 등록했다고 데이터 내용까지 같다는 뜻은 아니며, 내용·split 동등성은 사용자가 관리합니다.
필요하면 선택적 `input_files`로 frozen manifest hash를 확인할 수 있으나,
manifest hash 일치만으로 모든 이미지 내용의 동일성을 보증하지는 않습니다.

학습 checkpoint 같은 dependency 결과는 별개입니다. 기본적으로 다른 서버의 local
출력을 같은 경로 문자열만으로 읽을 수 있다고 가정하지 않습니다. 동일 NFS를 공유하는
서버에만 같은 `storage_domain`을 지정하세요. local 출력의 cross-host 의존성은 기다리며,
자동 전송은 하지 않습니다.

### 공유 스토리지 시작 간격

서로 다른 서버가 같은 스토리지를 읽는 경우 `startup_group`을 설정하고
`examples/shared-storage-group.json`을 등록합니다. 기본적으로 group 전체에서
60초 간격, 한 번에 하나의 cold start만 허용합니다. GPU가 바쁘면 시작 slot을 잡지 않습니다.

각 서버 policy에 `read_probe_path`와 `read_probe_bytes`(기본 64 MiB)를 넣으면 실제 파일을
읽어 성공/지연 여부를 확인합니다. file은 최소 해당 크기여야 합니다.
서버별 `stable_polls` 기본값은 3입니다. 한 group의 D-state/읽기 장애/접속 불명은
공유 backend의 안전을 위해 다른 group member의 신규 시작도 막습니다.

학습이 실제 optimizer step을 진행한 후 **이번 attempt의** `RS_READY_PATH`에 파일을
생성하도록 연구 wrapper에서 연결하세요. 그 뒤 안정적인 health poll을 확인하고
cold-start slot을 해제합니다. 이 marker를 생성하지 않으면 job 종료까지 slot을 유지합니다.
프로세스 생성만으로 준비 완료라고 자동 추측하지 않습니다.

## 장애 처리와 안전 범위

자세한 상태 다이어그램은 [상태 및 복구 설계](docs/STATE_MACHINE.md)에 있습니다.

- SSH: 최초 장애 탐지 후 즉시 3회, 5/10/15분 시점에 각 3회 재시도합니다.
  각 묶음의 응답 timeout은 30/60/90초, 연결 timeout은 10/20/30초입니다.
  **15분은 마지막 묶음 시작 시점**이며 실제 unavailable 판정은 마지막 timeout 뒤입니다.
- D-state: 보이는 D-state가 있으면 즉시 신규 배치를 막고, 연속 10분 관찰 시 unavailable.
  중간에 해소되거나 관찰이 120초 이상 끊기면 연속 지속 시간은 다시 셉니다.
- unavailable 서버는 자동 재활성화하지 않습니다. `readmit-node`는 운영자가 확인한 뒤
  격리만 해제하는 명령이며 서버를 재부팅/수리하지 않습니다.
- 기존 attempt는 `invalid`로 확정합니다. 늦게 도착한 성공 결과도 채택하지 않습니다.
  `failover_safe: true`이고 `max_attempts`가 남은 작업만 새 attempt로 다시 대기합니다.
- `failover_safe`는 모든 가변 출력을 `{attempt_dir}`로 분리하고, shared checkpoint 덮어쓰기,
  외부 DB 업데이트 같은 부작용이 없는 작업에만 지정합니다. 이는 운영자의 계약이지
  임의 Python 코드의 부작용을 자동 증명하는 기능이 아닙니다.
- **invalid는 원격 프로세스 종료를 뜻하지 않습니다.** 이전 프로세스는 실행 중일 수 있습니다.
  source node는 격리하고 기존 실행을 죽이지 않습니다. 정확히 한 번의 계산이 아니라
  최대 한 개의 유효 결과 lineage를 유지하는 방식입니다.
- 시스템 재부팅, 타 사용자 프로세스 종료, Docker 작업, 서버 복구 명령은 없습니다.
  스토리지 probe timeout 시 자신이 생성한 probe child에만 종료 신호를 보냅니다.

```bash
research-scheduler --db /local/path/scheduler/state.db drain-node research-node-a
research-scheduler --db /local/path/scheduler/state.db cancel-pending study-threshold-02
research-scheduler --db /local/path/scheduler/state.db readmit-node research-node-a --ack-old-attempts-may-still-run
```

## 현재 한계

예약은 admission control이지 CPU/RAM/VRAM hard limit가 아닙니다. GPU sharing은 기본 off이며,
node `allow_gpu_sharing`과 job `gpu_mode: shared`를 함께 지정해야 합니다.
compute PID가 보이면 추가로 `allow_external_gpu_processes`도 필요합니다.
이 옵션은 다른 사용자의 프로세스를 종료하지는 않지만 자원 경쟁을 허용하는 설정이므로
서버 운영 정책을 확인해야 합니다. 메모리는 관측 사용량에 예약량을 보수적으로 추가 차감하여
안전 여유를 두므로 실제 가능한 것보다 적게 배치될 수 있습니다.

`/proc`와 컨테이너에서 보이는 telemetry를 사용하며 GPU PID가 호스트/컨테이너 namespace에서
다르게 보일 수 있습니다. cgroup v2 CPU/RAM 한도는 반영하지만 v1/nested 특수 환경은
운영자가 `cpu_limit`/`ram_limit_mib`로 더 낮은 한도를 명시해야 합니다.
MIG/MPS, GPU hard isolation, 다른 scheduler와의 전역 예약 조정은 지원하지 않습니다.
VRAM peak 자동 profiling, OOM 기반 batch 변경도 하지 않습니다.

DB는 반드시 **제어 머신 local disk**에 둡니다. SQLite WAL을 NFS/CIFS에서 사용하지 않습니다.
이 도구는 신뢰된 단일 운영자용이며 명령어 sandbox/인증 서버가 아닙니다.
DB와 로그에 비밀값이 남지 않도록 설정에 credential을 직접 넣지 마세요.

오픈소스 검토와 선택 근거는 [OPEN_SOURCE_REVIEW.md](docs/OPEN_SOURCE_REVIEW.md)에 정리했습니다.
