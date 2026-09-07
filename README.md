# Research Job Scheduler

학습·평가 작업을 등록하면 **우선순위·의존 관계·서버 자원 상태를 확인해 실행하는 연구용 스케줄러**입니다.
실험 이름과 RQ(Research Question, 실험으로 확인할 질문)를 실행 설정·결과와 함께 보관합니다.
특정 연구 코드나 데이터셋에 종속되지 않으며, 이미 준비된 Linux 서버에 SSH로 접속해 명령을 실행합니다.

현재 버전은 **단일 운영자용 CLI MVP**입니다. 등록 명령 자체는 연구 작업을 실행하지 않으며,
`--execute`를 지정한 실행기가 신규 작업을 시작합니다. 다만 `daemon --execute`가 이미 운영 중이면
새로 등록한 job도 자동 배치됩니다. 기존 연구 큐를 자동으로 가져오거나 변경하지 않습니다.

## 목차

- [주요 기능과 기본 개념](#주요-기능과-기본-개념)
- [설치](#설치)
- [빠른 시작](#빠른-시작)
- [실험 설계와 실행 설정](#실험-설계와-실행-설정)
- [운영과 결과 확인](#운영과-결과-확인)
- [장애 처리와 안전 범위](#장애-처리와-안전-범위)
- [문제 해결](#문제-해결)
- [지원 범위와 추가 문서](#지원-범위와-추가-문서)

## 주요 기능과 기본 개념

- 서버와 GPU 등록: SSH/local 실행, GPU UUID·모델·VRAM 조회, 장치별 사용 허용.
- 실험 관리: 이름·RQ·job별 목적, 설정값, 예상 자원량, 우선순위, 학습→평가 의존성.
- 의존성 우선 배치와 검증된 DDP 대안: 후속을 여는 job을 우선하고 서버에 맞는 등록된 GPU 구성을 선택.
- 실험 그룹 알림: 여러 실험/project를 하나의 campaign으로 묶어 그룹 완료·오류 전이만 통지.
- HF 결과 공유: campaign별 저장소에 결과를 올리고 다른 서버의 후속 작업이 revision·hash 검증 후 사용.
- 자원 기반 배치: GPU 사용률·VRAM·compute PID, CPU·RAM·디스크·D-state와 선택적 스토리지 읽기 점검.
- VRAM packing과 온도 보호: 저사용률 GPU에 bounded shared job을 배치하고 80/85°C 단계별 launch 제한.
- 파일시스템 제약: 실험/job을 `nfs`, `local`, `any`로 지정해 맞는 서버에만 배치.
- 서버별 데이터셋 경로: 같은 데이터셋 이름을 서버마다 다른 실제 경로로 연결.
- 실행 기록: 실행별 설정·로그·종료 코드·결과 파일 SHA256, 장애 재시도와 결과 유효성 관리.

| 개념 | 의미 | 예시 |
|---|---|---|
| Project | 실험을 묶는 연구 이름. 실험의 `project` 필드 사용 | `context-study` |
| Campaign (실험 그룹) | 하나 이상의 project/experiment를 묶는 운영·알림 단위 | `spatial-channel-v1`, `n56` |
| Experiment | 이름·RQ와 관련 job 목록을 묶은 실험 | “Context가 검출 성능을 개선하는가?” |
| Job | 완료해야 할 논리 작업. `train`, `eval`, `prepare`, `analysis` 중 하나 | baseline 학습, threshold 평가 |
| Attempt | job을 실제로 실행한 한 번의 시도. 재시도마다 새 ID·출력 폴더 생성 | 최초 실행, 장애 후 재실행 |
| Node | 등록된 실행 서버 또는 컨테이너의 접속 대상 | `research-node-a` |

`depends_on`은 기본적으로 선행 artifact의 같은 filesystem 접근과 해시 검증까지 요구합니다.
완료 순서만 기다리는 dependency는 해당 ID를 `order_only_dependencies`에도 적을 수 있습니다.
이 경우 서로 다른 local node에서도 후속을 배치할 수 있지만 `{dep:ID}` 경로 참조는 금지됩니다.
원격 artifact가 필요하면 [HF 결과 공유](docs/HF_ARTIFACTS.md)를 등록하거나 workflow의 명시적인
수집·전송 단계를 사용합니다. HF download가 검증되기 전에는 다른 local 서버에 후속 job을 배치하지 않습니다.
`cluster`는 실행 서버 집합과 혼동될 수 있어 알림 단위의 코드 명칭은 `campaign`, 화면 표현은
“실험 그룹”을 사용합니다.

## 설치

### 준비 사항

| 위치 | 필요한 환경 |
|---|---|
| 제어 머신 | Linux, Python 3.10+, Git, SSH client, DB를 둘 local disk |
| 실행 서버 | Linux, Python 3.10+, 사용자 SSH 접속, 준비된 연구 코드·실행 환경·데이터 |
| GPU 작업 서버 | NVIDIA GPU와 작동하는 `nvidia-smi` |

스케줄러 런타임은 Python 표준 라이브러리만 사용합니다. 실행 서버에 스케줄러 패키지를 별도로
설치할 필요는 없습니다. 대신 연구 명령이 해당 서버에서 실행 가능해야 하며, SSH 사용자에게
작업 폴더 쓰기 권한과 데이터셋 읽기 권한이 필요합니다.

저장소 접근 권한이 있는 계정으로 **제어 머신의 local disk**에 clone합니다.

```bash
git clone https://github.com/aapdo/research-job-scheduler.git
cd research-job-scheduler
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e .
research-scheduler --help
```

### 먼저 CPU 데모로 확인하기

```bash
python examples/local_demo.py --directory runtime/demo
```

로컬에서 CPU 더미 학습 2개와 그 결과를 읽는 평가 2개를 실제 실행합니다. GPU·SSH 서버·연구
데이터셋은 사용하지 않습니다. 실제 health gate를 사용하므로 D-state나 자원 부족이 있으면
기다리다가 기본 60초 후 종료할 수 있습니다. 상태와 결과는 `runtime/demo/`에 보존되며,
같은 명령을 다시 실행하면 완료된 job을 중복 실행하지 않습니다.

회귀 테스트는 별도로 실행합니다.

```bash
python -m unittest discover -s tests -v
```

## 빠른 시작

아래 명령은 설치한 제어 머신의 저장소 폴더에서 실행합니다. 예제의 `/srv/...`, 서버 이름,
GPU UUID, 학습 인수는 실제 환경에 맞게 바꾸세요. `train.py`·`evaluate.py`는 사용자 연구
코드이며 이 저장소에 포함되어 있지 않습니다.

### 1. 제어 DB 지정

```bash
export SCHEDULER_DB="$PWD/runtime/state.db"
```

이 경로는 local disk여야 합니다. SQLite DB를 NFS/CIFS에 두지 마세요.
같은 서버 집합을 관리하는 모든 명령에는 **동일한 DB**를 지정합니다.
서로 다른 DB의 스케줄러는 자원 예약을 공유하지 않습니다.

### 2. 서버 등록

[서버 예제](examples/node.ssh.json)의 다음 값을 환경에 맞게 수정한 뒤 등록합니다.

| 필드 | 지정할 값 |
|---|---|
| `id` | 스케줄러 안에서 사용할 서버 이름 |
| `target` | 사용자의 `~/.ssh/config`에 등록한 SSH alias |
| `python` | 원격 조회·실행 도우미용 Python 3.10+ 경로 |
| `work_root` | **실행 서버 기준** 로그·설정·결과를 저장할 전용 폴더 |
| `filesystem` | 서버의 실행 스토리지 종류. `local` 또는 `nfs` |
| `max_jobs` | 서버에 동시에 배치할 job 수 상한 |

SSH port·사용자·key·ProxyJump는 `~/.ssh/config`에서 관리합니다. 비밀번호나 private key를
등록 JSON에 넣지 않습니다. `node.python`과 학습용 Python(`job.argv[0]`)은 달라도 됩니다.

```bash
research-scheduler --db "$SCHEDULER_DB" register-node examples/node.ssh.json
```

### 3. GPU 조회 및 사용 허용

```bash
research-scheduler --db "$SCHEDULER_DB" discover research-node-a --apply
research-scheduler --db "$SCHEDULER_DB" inventory
research-scheduler --db "$SCHEDULER_DB" set-gpu research-node-a GPU-ACTUAL-UUID enabled
research-scheduler --db "$SCHEDULER_DB" enable-node research-node-a
```

`GPU-ACTUAL-UUID`는 조회 결과의 실제 UUID로 바꿉니다. 여러 GPU를 사용할 경우 각각 허용하세요.
`discover --apply`는 GPU 목록을 **모두 disabled 상태로 등록**하므로 재실행 시에도 사용 허용을
다시 확인해야 합니다. CPU-only 서버는 GPU 등록을 생략하고 node만 활성화하면 됩니다.

같은 서버를 서로 다른 SSH alias로 중복 등록하지 마세요. GPU UUID 중복 등록은 차단하지만,
CPU-only alias가 같은 물리 호스트를 가리키는지는 자동 판별하지 않습니다.

### 4. 서버별 데이터셋 경로 등록

```bash
research-scheduler --db "$SCHEDULER_DB" set-dataset research-node-a vehicle-v1 /data/vehicle
```

다른 서버도 2~3단계로 등록했다면 같은 이름에 그 서버의 경로를 연결할 수 있습니다.

```bash
research-scheduler --db "$SCHEDULER_DB" set-dataset research-node-b vehicle-v1 /mnt/datasets/vehicle
```

실험에는 `"dataset": "vehicle-v1"`을 지정합니다. 명령·설정의 `{dataset_path}`와 환경 변수
`RS_DATASET_PATH`에 **선택 서버의 실제 경로**가 전달됩니다.

미등록·접근 불가 경로는 해당 실험의 배치 대상에서 제외합니다. 데이터 복사·다운로드·symlink
생성은 하지 않으며 같은 이름의 데이터 내용·버전·split 동등성은 사용자가 관리합니다.
기존 `dataset_path` 직접 지정도 지원하지만 `dataset`과 동시에 지정할 수는 없습니다.

실행 중인 attempt를 유지하면서 node의 **향후** local/NFS 경로를 전환하려면 storage profile을
사용합니다. 아래 명령은 기존 attempt의 frozen 경로를 바꾸지 않고 신규 배치 설정만 교체합니다.

```bash
research-scheduler --db "$SCHEDULER_DB" set-storage-profile \
  research-node-a examples/storage-profile.local.json
```

### 5. 실험 등록과 모의 배치

[서버별 데이터셋을 사용하는 실험 예제](examples/experiment.named-dataset.json)의
실행 파일·작업 경로·설정·자원량을 수정한 뒤 등록합니다.

```bash
research-scheduler --db "$SCHEDULER_DB" register-experiment examples/experiment.named-dataset.json
research-scheduler --db "$SCHEDULER_DB" priority named-dataset-train 200
research-scheduler --db "$SCHEDULER_DB" plan
research-scheduler --db "$SCHEDULER_DB" status
```

`plan`은 배치 가능한 서버·GPU·데이터 경로와 대기 이유를 보여 줍니다. 신규 job을 실행하지는
않지만 서버 조회, 기존 실행과의 대조, DB 상태 갱신은 수행합니다.
기본적으로 안정적인 health poll 3회가 필요하므로 처음에는 대기로 보일 수 있습니다.

### 6. 실행

```bash
research-scheduler --db "$SCHEDULER_DB" daemon --execute --interval 20 --max-launches-per-cycle 8
```

20초 대기 간격으로 상태를 확인하고 배치합니다. 한 cycle에는 기본 최대 8개의 신규 job을
순서대로 검증·시작하며, 이미 시작된 job들도 병렬 실행됩니다. 공유 시작 그룹은 이 값과 무관하게
그룹 규칙을 유지합니다. 조회·재시도 시간만큼 실제 cycle은 길어질 수 있습니다.
상한은 `--max-launches-per-cycle`로 조절할 수 있습니다. 한 번만 배치하려면 `tick --execute`,
실행 없이 반복 확인하려면 `daemon --interval 20`을 사용합니다.

터미널을 닫아도 운영하려면 전용 tmux 세션 안에서 설치 환경과 `SCHEDULER_DB`를 설정하고 위 명령을
실행한 뒤 detach하세요. daemon을 Ctrl+C 또는 SIGTERM으로 종료하면 신규 배치를 멈추지만,
이미 시작된 작업은 계속 실행됩니다. 같은 DB로 다시 시작하면 실행 상태를 재확인합니다.

## 실험 설계와 실행 설정

실험 JSON은 `name`, `rq`, `jobs`를 포함합니다. job의 `purpose`에는 “이 작업으로 무엇을
확인하는가”를 적습니다. 실험 ID와 job ID는 각각 DB 전체에서 고유해야 합니다.
등록된 실험 설정을 바꾸려면 새 ID를 사용합니다. 동일 설정 재등록은 완료 상태를 초기화하지 않습니다.

실험의 `filesystem`에는 `any`(기본값), `local`, `nfs`를 지정할 수 있습니다. 모든 job이 이를
상속하며, 개별 job에서 덮어쓸 수 있습니다. 선택된 실제 종류는 `{filesystem}` 치환값과
`RS_FILESYSTEM` 환경 변수로 전달됩니다. 이 설정은 배치 대상을 제한할 뿐 mount·복사·동기화를
수행하지 않습니다.

### 학습과 평가의 의존 관계

[학습·평가 DAG 예제](examples/experiment.json)는 다음 세 작업을 묶습니다.

| Job | 목적 | 선행 작업 |
|---|---|---|
| `study-baseline` | 공통 baseline checkpoint 생성 | 없음 |
| `study-threshold-02` | 같은 checkpoint로 threshold 0.2 평가 | `study-baseline` 성공 |
| `study-independent-baseline` | 독립적인 다른 baseline 학습 | 없음 |

평가 job의 `depends_on`에 학습 job ID를 넣고 checkpoint 경로는
`{dep:study-baseline}/model.pt`처럼 참조합니다. 여러 threshold를 비교하려면 평가 job들을
각각 등록하고 같은 학습 job에 의존하도록 합니다. 평가 목록을 자동 생성하지는 않습니다.

성공은 exit code 0과 `outputs`에 선언한 파일 존재를 함께 확인합니다. 결과 파일 SHA256을
기록하고 downstream 시작 전에 다시 검사합니다. `outputs`를 생략하면 결과 파일 완전성을
검사할 수 없으므로 checkpoint·metrics 파일을 명시하세요.

### 실험 그룹 완료·오류 알림

HF 업로드를 사용하는 전체 캠페인과 부분집합 모니터는 목적지 설정이 완전히 같으면
겹칠 수 있습니다. 작업당 업로드는 하나이며 두 모니터가 같은 영수증을 공유합니다.
다른 HF 목적지로 겹치는 설정은 계속 거부합니다. 후속 작업이 필요로 하는 산출물은
의존성 우선순위로 먼저 업로드하고, 다운로드는 고정 commit·checksum으로 검증합니다.
HF/Xet 캐시는 각 전송 작업의 filesystem에 둡니다.

[campaign 예제](examples/campaign.json)는 `projects`와 선택적인 개별 `experiments`를 합쳐
하나의 알림 단위를 만듭니다. 이름과 RQ도 campaign에 보관됩니다.

```bash
research-scheduler --db "$SCHEDULER_DB" register-campaign examples/campaign.json
research-scheduler --db "$SCHEDULER_DB" campaign-status
```

campaign은 개별 job이 끝날 때마다 알리지 않습니다. 그룹이 처음 `complete` 또는 `error`로
전이한 감지 시각을 `YYYY-MM-DD HH:MM:SS KST` 형식으로 한국어 알림에 포함합니다. 전송 지연이나
재시도에도 outbox에 저장된 최초 시각이 유지됩니다. 그룹이 `complete` 또는 `error`로
전이할 때 한 번만 알립니다. `failed`/`blocked` job이 하나라도 생기면 `error`, 아직 실행할
job이 있으면 `running`, 모든 job이 `succeeded`/`cancelled`이면 `complete`입니다. 오류 뒤
재시도로 오류가 해소되고 이전 실패 작업 모두가 실제 실행 준비 완료(`ready`) 또는 성공으로
확인되면 **복구 · 정상 실행 재개** 알림을 한 번 보냅니다. 재등록·대기열 복귀만으로는 보내지
않으며, 다른 작업이 계속 실행 중이라는 이유만으로 복구를 판정하지 않습니다. 복구 대기는
DB에 보존하므로 컨트롤러 재시작에도 유지됩니다. 외부 campaign은 `recovery_ready: true`를
명시해야 합니다. 바로 완료된 경우에는 복구 알림 없이 완료 알림만 보냅니다.
이후 완료되면 완료 알림을 추가로 보냅니다. 같은 오류 상태에서
실패가 더 늘어나는 경우에는 반복 알림하지 않습니다.

Slack Incoming Webhook은 JSON, DB, 명령행 인수나 Git 저장소에 넣지 않고 제어 머신의 별도
파일에 둡니다. 환경 변수를 생략하면 `~/.config/research-scheduler/slack-webhook`을 자동으로
읽으므로 컨트롤러 재시작에도 설정이 유지됩니다. 해당 파일도 없으면 알림 전송은 비활성화되며,
상태 관찰과 pending outbox는 보존됩니다.

`RS_SLACK_WEBHOOK_FILE`을 명시하면 그 경로를 우선 사용하고, 빈 문자열로 지정하면 알림을
명시적으로 끕니다. secret 파일명 `slack-webhook`과 `slack-webhook.*`는 Git에서 제외합니다.

```bash
install -d -m 700 "$HOME/.config/research-scheduler"
$EDITOR "$HOME/.config/research-scheduler/slack-webhook"
chmod 600 "$HOME/.config/research-scheduler/slack-webhook"
export RS_SLACK_WEBHOOK_FILE="$HOME/.config/research-scheduler/slack-webhook"
```

daemon은 전이와 전송 결과를 durable outbox와 event log에 기록하므로 재시작해도 같은 전이를
중복 발송하지 않습니다. 전송 실패 시 webhook 값이나 응답 본문을 기록하지 않고 오류 종류만
남긴 뒤 제한적으로 재시도합니다. 외부 legacy queue를 campaign으로 관찰하는 통합도 가능하지만,
그 상태를 scheduler에 전달하는 별도 controller가 필요합니다.

### 자원량과 경로를 지정할 때

- `resources.gpu_count`는 같은 서버에서 사용할 GPU 수입니다. 0이면 CPU-only입니다.
- `resources.vram_mib`는 **GPU 한 개당** 예상 VRAM입니다. parameter 수만으로 자동 추정하지 않습니다.
- `resources.cpu`, `resources.ram_mib`는 예약량입니다. 실제 사용량을 강제로 제한하지는 않습니다.
- 우선순위는 `experiment.priority + job.priority`이며 큰 값부터 배치합니다.
  자원이 부족한 높은 우선순위 job은 건너뛰고 실행 가능한 독립 job을 먼저 시작할 수 있습니다.
- 여러 GPU를 요청해도 학습 명령이 자동으로 DDP로 바뀌지는 않습니다. 연구 코드의 launcher를
  `argv`에 직접 지정하고 출력은 `{attempt_dir}` 아래에 저장하세요.
- `gpu_mode: "shared"`와 node의 `allow_gpu_sharing`을 함께 켜야 같은 GPU에 여러 job이 배치됩니다.
  스케줄러는 먼저 빈 GPU에 분산한 뒤, 사용률·VRAM·온도·job 수 제한을 모두 통과할 때만 packing합니다.

전체 필드·치환 값·기본값·NFS 정책은 [설정 레퍼런스](docs/CONFIGURATION.md)에 정리했습니다.

## 운영과 결과 확인

명령은 모두 `research-scheduler --db "$SCHEDULER_DB"` 뒤에 붙입니다.

| 명령 | 하는 일 |
|---|---|
| `status` | DB에 저장된 실험 이름·RQ·job별 목적·상태·배치 위치 표시 |
| `status --json` | 상세 job/attempt, 최근 resource snapshot, node 장애 상태 출력 |
| `inventory` | 등록된 서버·GPU·데이터셋 경로·그룹 설정 확인 |
| `probe` | 서버 자원을 새로 조회하고 DB에 기록. 신규 job 실행 없음 |
| `plan` | 상태를 갱신하고 배치 예상 및 대기 이유 확인. 신규 job 실행 없음 |
| `events` | 등록·설정 변경·장애·실행 상태 전이 이력 확인 |
| `register-campaign FILE` | project/experiment들을 완료·오류 알림 단위로 등록 |
| `campaign-status` | campaign 상태와 비밀값을 제외한 알림 outbox 확인 |
| `register-dataset FILE` | 데이터 버전·기대 해시·서버별 준비/검증 명령 등록 |
| `dataset-status` | 자동 CPU 준비 작업과 검증된 데이터 경로 확인 |
| `artifact-status` | HF upload/download 상태와 완료 revision·링크 확인 |
| `set-node-hf NODE FILE` | node의 HF Python과 비밀 token 파일 경로 등록 |
| `set-job-hf-artifacts JOB FILE` | 첫 전송 전에 export 파일 glob 및 JSON 경로 변환 계약 등록 |
| `priority JOB_ID VALUE` | 대기 중인 job 우선순위 변경 |
| `set-job-resource-variants JOB FILE` | 검증된 대체 GPU/VRAM 구성 배열을 대기 job에 등록 |
| `set-dataset NODE NAME PATH` | 앞으로 실행할 job의 서버별 데이터 경로 등록·변경 |
| `set-storage-profile NODE FILE` | active attempt는 유지하고 향후 local/NFS 경로·admission을 원자적으로 전환 |
| `retry-failed JOB_ID` | 실패 증거를 보존하면서 retry budget 1회를 추가하고 재대기 |
| `set-gpu NODE UUID enabled|disabled` | active attempt를 바꾸지 않고 향후 GPU 배치 허용 여부 변경 |
| `set-external-gpu-processes NODE enabled|disabled` | 특정 node에서 외부 PID와 VRAM headroom 기반 공존 허용 |
| `set-gpu-margin NODE MIB` | 향후 배치에 적용할 GPU별 VRAM 안전 여유 변경 |
| `set-gpu-packing NODE enabled|disabled` | scheduler job 간 VRAM 기반 shared 배치 설정 |
| `set-temperature-policy NODE` | 기본 80°C warm cap·85°C hard launch limit 설정 |
| `set-job-gpu-mode JOB shared|exclusive` | 대기 중인 job의 GPU packing 방식 변경 |
| `replace-job-path-prefix JOB OLD NEW` | 대기 중인 job만 packing-aware immutable release로 재바인딩 |
| `drain-node NODE` | 기존 작업은 유지하고 해당 서버의 신규 배치 중지 |
| `cancel-pending JOB_ID` | 아직 시작하지 않은 job 취소. 실행 중 프로세스는 종료하지 않음 |

`status`만으로 원격 상태를 새로 조회하지는 않습니다. daemon이 멈춰 있으면 `plan`으로
실행 상태를 갱신한 뒤 확인하세요. `set-dataset`은 이미 생성된 attempt의 경로를 바꾸지 않습니다.

로그와 결과는 제어 머신이 아니라 **배치된 서버의** `work_root/attempts/<attempt-id>/`에 있습니다.

| 파일 | 내용 |
|---|---|
| `spec.json`, `config.json` | 실제 서버·GPU·경로로 확정된 실행 명세와 설정 |
| `stdout.log`, `stderr.log` | 연구 명령의 출력·오류. 시작 전 검사에서 실패하면 없을 수 있음 |
| `state.json` | PID, heartbeat, 종료 코드, 결과 파일 hash 등 실행 상태 |
| `runner.py`, `runner.log` | 이번 실행에 복사된 실행 도우미와 도우미 오류 로그 |
| 선언한 `outputs` 파일 | checkpoint, metrics 등 사용자 연구 결과 |

학습 loss·condition/severity 예측·평가 지표를 자동으로 추출하지는 않습니다. 연구 코드가
로그나 결과 파일로 남기도록 연결해야 합니다. DB·가상환경·runtime 산출물은 Git에서 제외합니다.

## 장애 처리와 안전 범위

| 상황 | 처리 |
|---|---|
| SSH/응답 장애 | 최초 장애 뒤 즉시·5분·10분·15분에 각 3회, 총 12회 재시도 |
| 재시도 timeout | 묶음 안의 응답 timeout 30/60/90초, 연결 timeout 10/20/30초 |
| D-state 관찰 | 즉시 신규 배치 중지. 연속 10분 관찰 시 서버 `unavailable` |
| 서버 `unavailable` | 기존 미완료 attempt를 `invalid`로 확정하고 늦은 결과는 채택하지 않음 |
| 다른 서버로 재배치 | `failover_safe: true`이고 `max_attempts`가 남은 job만 새 attempt로 재시도 |

15분은 마지막 SSH 재시도 묶음의 **시작 시점**입니다. 최종 사용 불가 판정은 마지막 timeout
이후이므로 더 늦어질 수 있습니다. 최초 장애 탐지 probe는 12회에 포함하지 않습니다.
D-state가 해소되거나 관찰 공백이 120초를 넘으면 연속 지속 시간은 다시 계산합니다.

**`invalid`는 원격 프로세스 종료를 뜻하지 않습니다.** 접속 불가 서버의 작업이 계속 실행될 수
있으므로 자동 재배치는 모든 가변 출력을 attempt별로 분리하고 외부 부작용이 없는 작업에만
허용해야 합니다. 기본값은 `failover_safe: false`, `max_attempts: 1`입니다. 실패 원인을 수정한 뒤
운영자가 재시도를 결정한 경우에만 `retry-failed`를 사용합니다. 기존 failed attempt는 그대로
보존되고 job의 `max_attempts`와 상태 전환은 event log에 기록됩니다.

격리된 서버는 자동 재활성화하지 않습니다. 운영자가 상태를 확인한 뒤 아래 명령으로 격리를
해제하면 새 health poll부터 확인합니다. 기존 invalid 결과는 계속 무효입니다.

```bash
research-scheduler --db "$SCHEDULER_DB" readmit-node research-node-a --ack-old-attempts-may-still-run
```

시스템 재부팅, 타 사용자 프로세스 종료, Docker 작업, 서버 복구 명령은 수행하지 않습니다.
스토리지 probe timeout 시에는 자신이 만든 probe child에만 종료 신호를 보냅니다.
상세 전이는 [상태 머신 문서](docs/STATE_MACHINE.md)를 참고하세요.

## 문제 해결

| 증상 또는 대기 이유 | 확인할 내용 |
|---|---|
| GPU 사용률이 0인데 배치되지 않음 | compute PID, VRAM, 장치 enabled, 자원 예약을 함께 확인. 기본값은 exclusive |
| `waiting for stable health polls` | 한 번의 조회로는 부족할 수 있음. daemon으로 정상 poll이 누적되는지 확인 |
| `dataset path not registered` / `unavailable` | 선택 서버의 이름→경로 등록과 원격 읽기 권한 확인 |
| `dependency artifacts on another local filesystem` | 선행 결과가 다른 local disk에 있음. HF campaign/node 설정 및 upload/download 상태 확인 |
| `shared-storage cold-start slot occupied` | 준비 완료 marker와 그룹 health 확인. [공유 스토리지 설정](docs/CONFIGURATION.md#공유-스토리지-설정) 참고 |
| `unknown` / `blocked` | 단순 성공·실패로 해석하지 말고 node 상태, attempt 로그와 재시도 예산 확인 |
| `another controller/registry operation holds the scheduler lock` | 같은 DB의 진행 중 조회·배치가 끝난 뒤 재시도. DB를 새로 만들어 중복 실행하지 않음 |

## 지원 범위와 추가 문서

현재는 신뢰된 단일 운영자용 도구입니다. GUI·multi-user 인증·명령 sandbox·다중 서버 DDP·
MIG/MPS·선점·자동 checkpoint resume·VRAM peak 자동 profiling은 지원하지 않습니다.
다른 scheduler와의 전역 예약 조정, CPU/RAM/VRAM 강제 격리, aging/fair-share도 없습니다.
보수적인 예약 때문에 여유 자원을 모두 사용하지 못할 수 있으며, 비밀값을 등록 설정에 넣지 마세요.

| 문서 / 예제 | 용도 |
|---|---|
| [CONFIGURATION.md](docs/CONFIGURATION.md) | 서버·job 필드, 기본값, 치환 값, GPU 공유 및 NFS 설정 |
| [STATE_MACHINE.md](docs/STATE_MACHINE.md) | job/attempt/node 상태와 재시도·invalid 처리 |
| [HF_ARTIFACTS.md](docs/HF_ARTIFACTS.md) | campaign HF 저장소, 자동 upload/download와 checkpoint 경로 변환 |
| [DATASET_PREPARATION.md](docs/DATASET_PREPARATION.md) | 미등록 데이터 준비·검증·경로 등록 자동화 |
| [OPEN_SOURCE_REVIEW.md](docs/OPEN_SOURCE_REVIEW.md) | Slurm·ClearML·Ray 검토와 구현 선택 근거 |
| [VALIDATION_20260906.md](docs/VALIDATION_20260906.md) | 배치·전송·의존성 회귀 테스트와 실제/모의 검증 범위 |
| [node.ssh.json](examples/node.ssh.json) | 서버 등록 예제 |
| [experiment.named-dataset.json](examples/experiment.named-dataset.json) | 서버별 데이터 경로를 사용하는 단일 학습 예제 |
| [experiment.json](examples/experiment.json) | baseline 학습→평가와 독립 학습을 묶는 DAG 예제 |
| [campaign.json](examples/campaign.json) | 여러 실험을 하나의 완료·오류 알림 단위로 묶는 예제 |
| [shared-storage-group.json](examples/shared-storage-group.json) | 공유 스토리지 시작 간격 설정 |
