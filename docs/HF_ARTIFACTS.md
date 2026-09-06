# Campaign 결과 공유: Hugging Face

서버별 local disk에 있는 결과를 HF 저장소를 통해 후속 서버로 전달합니다. HF 설정이 없는
campaign은 기존 동작을 유지합니다. 실행 서버에 등록한 HF Python에는 `huggingface_hub`가
필요하며 제어 머신의 기본 scheduler 설치에는 추가 의존성이 없습니다.

## 등록

운영자가 준비한 **기존** model 또는 dataset 저장소를 사용합니다. 저장소를 자동 생성하거나
공개 범위를 변경하지 않습니다. campaign의 `hf` 필드에는 비밀값을 넣지 않습니다.

```json
{
  "hf": {
    "repo_id": "YOUR_ACCOUNT/experiment-results",
    "repo_type": "model",
    "revision": "main",
    "path_prefix": "scheduler-artifacts"
  }
}
```

기존 campaign JSON에 위 필드만 추가해 재등록할 수 있습니다. 완료된 학습도 publication
대상에 포함되지만 기존 성공 상태와 실행 명세는 초기화하지 않습니다. 한 experiment가 두
HF campaign에 동시에 포함되면 모호한 업로드를 막기 위해 오류로 처리합니다. 설정 변경은
아직 시작하지 않은 publication에 적용되며 기존 결과의 revision은 유지됩니다.

```bash
research-scheduler --db "$SCHEDULER_DB" register-campaign examples/campaign.hf.json
research-scheduler --db "$SCHEDULER_DB" set-node-hf server-a examples/node.hf.json
research-scheduler --db "$SCHEDULER_DB" set-node-hf server-b node-b.hf.json
```

`node.hf.json`은 실행 서버 기준의 HF Python과 선택적인 token 파일 **경로**입니다. 해당
Python에 `pip install 'huggingface-hub>=0.24,<2'`로 설치하거나 scheduler의 `[hf]` extra를
설치할 수 있습니다. `token_file`을 생략하면 그 서버 사용자의 기존 HF login을 사용합니다.
지정한 token 파일은 실행 사용자 소유이고 group/world 권한이 없어야 합니다(예: `0600`).
token 값은 JSON, CLI 인수, Git 또는 job.env에 넣지 않습니다. 서버마다 읽기/쓰기 권한이 있는
인증을 별도로 준비해야 하며 scheduler가 token을 다른 서버로 복사하지는 않습니다.

## 어떤 파일을 올리는가

기본적으로 job의 `outputs`에 명시한 결과를 업로드합니다. 결과 JSON만 `outputs`에 선언한
연구 코드에서는 checkpoint를 `hf_artifacts`에 추가해야 합니다. 파일 또는 파일 glob만
지원합니다. directory 자체, symlink, attempt 밖의 파일, `..` 경로는 거부합니다.

```json
{
  "outputs": ["TRAIN_RESULT.json"],
  "hf_artifacts": ["run/epochs/*/model.pdparams", "run/TRAIN_CONFIG.yml"],
  "hf_relocate_json": ["TRAIN_RESULT.json"]
}
```

`hf_relocate_json`에 지정한 descriptor에서는 원래 attempt 폴더를 가리키는 절대 경로 값을
다운로드된 payload 위치로 바꿉니다. 원본 JSON은 HF에 그대로 보존하고, 파생 JSON의 변경 전후
SHA256을 다운로드 receipt에 기록합니다. checkpoint 등 나머지 파일은 byte-identical합니다.
descriptor에서 참조하는 attempt 내부 파일을 내보내지 않았으면 업로드 전에 오류를 냅니다.
다른 dataset/W0 경로나 attempt 밖의 파일은 자동 변환·추적하지 않습니다. 연구 코드가 그
입력을 별도 dataset/asset 또는 명시적인 dependency로 선언해야 합니다.

기존 job도 첫 transfer 전에만 export contract를 추가할 수 있습니다.

```bash
research-scheduler --db "$SCHEDULER_DB" set-job-hf-artifacts JOB_ID examples/job.hf-artifacts.json
```

## 실행 및 검증

1. 학습이 성공하면 완료 receipt의 SHA256과 export 파일을 검사합니다.
2. 원래 실행 서버에서 GPU를 예약하지 않는 별도 CPU upload task를 시작합니다.
3. `prefix/campaign/job/attempt/`에 파일과 `HF_MANIFEST.json`을 한 commit으로 올립니다.
4. 성공 결과에 `hf_artifact`를 붙입니다. repo ID/type, commit SHA, 경로, 링크, manifest 및 파일
   hash가 포함됩니다. branch 이름 대신 전체 commit SHA가 다운로드 기준입니다.
5. 다른 local 서버에서 대기 중인 후속 job을 실행할 자원이 있으면 CPU download task를 만듭니다.
6. 모든 다운로드 파일의 크기·SHA256을 검증하고 필요하면 지정 JSON 경로를 변환합니다.
7. 검증된 목적지 경로로 `{dep:JOB_ID}`를 해석합니다. scientific child 직전 파일 hash를 다시
   검사한 뒤 실행합니다. 다운로드 동안 후속 job의 GPU를 예약하지 않습니다.

같은 서버나 명시적인 shared storage domain에서는 기존 local 경로를 우선 사용합니다.
기본 전송 상한은 cluster 전체 2개, 서버당 1개이며 GPU/D-state/NFS 초기화 정책을 우회하지
않습니다. 전송 reservation에는 CPU 2개, RAM 1 GiB를 선언합니다. 전체 cache를 미리 복제하지
않고 후속 실행이 가능한 node에 필요한 dependency를 staging합니다. 동시 실행 상태가 바뀌면
실제 배치는 다시 검증되므로 staging이 GPU 예약을 보장하지는 않습니다.

동일 transfer의 ACK가 유실되면 같은 ID로 상태를 확인합니다. 명확히 실패한 전송만 새 ID로
최대 3회 시도하고, 이전 시작부터 최소 60초 간격을 둡니다. 연락 불명 transfer는 중복 실행하지
않고 상태 확인을 기다립니다. 실패하거나 부분 다운로드된 폴더는 증거로 보존하며 자동 삭제하지
않습니다. cache eviction과 서버 간 token 배포는 지원하지 않습니다.

## 상태와 알림

```bash
research-scheduler --db "$SCHEDULER_DB" artifact-status
research-scheduler --db "$SCHEDULER_DB" status --json
research-scheduler --db "$SCHEDULER_DB" campaign-status
```

학습 성공과 publication 성공은 구분합니다. publication이 실패해도 성공한 학습을 다시 실행하지
않습니다. HF를 등록한 campaign은 publication이 완료되어야 완료 알림을 보내며, source node
설정 누락이나 전송 재시도 소진은 campaign 오류에 포함됩니다. `artifact-status`는 전송 ID,
상태, 목적지 node, pinned revision과 링크를 보여 줍니다. 개별 전송의 `HF_RECEIPT.json`과
제어 DB에 결과가 남습니다. custom finite controller는 `publication_summary()`의 pending과
`reservations()`가 남아 있는 동안 종료하지 않아야 합니다.

자동 테스트는 가짜 Hub와 서로 다른 local 폴더로 transfer·경로 변환·입력 검증·scheduler
재시작을 확인합니다. 실제 HF upload는 운영자가 repository 및 node 인증을 등록한 뒤 동작합니다.

사용한 API: [HF 업로드 문서](https://huggingface.co/docs/huggingface_hub/guides/upload),
[revision 지정 다운로드](https://huggingface.co/docs/huggingface_hub/guides/download).
