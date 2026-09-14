# Campaign 결과 공유: Hugging Face

서버별 local disk에 있는 결과를 HF 저장소를 통해 후속 서버로 전달합니다. HF 설정이 없는
campaign은 기존 동작을 유지합니다. 실행 서버에 등록한 HF Python에는 `huggingface_hub`가
필요하며 제어 머신의 기본 scheduler 설치에는 추가 의존성이 없습니다.

## 등록

운영 모델 스케줄러의 공통 업로드 간격 옵션은 환경 변수
`RS_HF_UPLOAD_INTERVAL_S`(1~3600초)입니다. 현재 서비스 설정은 `20`이며,
캠페인별 `commit_interval_s`보다 우선하여 모든 HF 저장소에 적용됩니다.
저장소별 동시 업로드 1건, 429의 서버 지정 대기시간은 그대로 유지합니다.
서비스 환경 옵션을 변경한 뒤 모델 관리 서비스만 재시작하면 적용됩니다.
옵션을 지정하지 않는 다른 실행기는 기존 캠페인 정책을 사용합니다.

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

선택 정책: `hf.archive_payload=true`는 이후 업로드도 압축 파일/manifest 2개로
저장합니다. `hf.commit_interval_s=20`은 같은 repo ID/type의 모든 브랜치·서버에
업로드 직렬화와 최소 간격을 적용합니다. 다운로드는 이 업로드 제한에 포함하지 않습니다.
2026-09-13 사용자 승인으로 picodet-s-cssa-epoch20 저장소의 SCDA8/R18/HM4 정책을
120초에서 20초로 변경했습니다. 기존 실행 중 전송 명세와 429 대기는 유지합니다.
429는 `Retry-After`/`RateLimit`을 읽고, 시간당 커밋 제한 또는 헤더 누락 시 보수적으로
1시간 뒤로 미룹니다. 전송은 `HF_RETRY.json`을 남기고 종료하여 슬롯/시작 잠금을
해제합니다. 스케줄러가 기록된 시각 이후 최대 3회 추가 시도하며, 과거 실패 이력은
유지합니다. 승인된 별도 복구 revision만 새 유한 재시도 구간을 시작합니다.
운영 모델 관리 프로세스는 `research-model-controller.service`에서 단일 인스턴스로
실행합니다. 재시작용 임시 unit의 자식으로 띄우지 않습니다.
2026-09-13 완료 회수 중단 복구 후 서비스는 `Restart=always`를 사용합니다.
정상 코드로 예기치 않게 종료돼도 다시 시작하지만, 운영자의 명시적
`systemctl stop`은 자동 재시작하지 않습니다. 단일 관리자 중복 검사와 기존 DB 잠금을 유지합니다.

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

2026-09-13부터 스케줄러가 새로 만드는 업로드는 `archive_payload` 생략 시에도
압축을 기본 적용합니다. 명시적인 `hf.archive_payload=false`는 호환성 예외이며,
기존 전송/receipt/다운로드는 변경하지 않습니다. 파일 수가 찬 브랜치는 압축만으로
해결되지 않으므로 별도 승인된 publication 브랜치를 사용해야 합니다.

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
3. `prefix/campaign/job/attempt/`에 최대 50개 파일 단위로 나누어 commit합니다.
   `HF_MANIFEST.json`은 마지막 commit에만 포함하며 최종 commit SHA를 receipt에 기록합니다.
   중간 실패나 source SHA 변경 시 완료 receipt를 만들지 않습니다. 부분 업로드는 성공으로
   간주하지 않으며, 재시도해도 원본과 실패 이력은 보존합니다.
   HTTP 400의 명시적 `too many files` 거부에만 batch를 절반으로 줄입니다(최소 1개).
   1개도 거부되거나 다른 오류이면 중단하며 무한 재시도·인증/용량 제한 우회는 하지 않습니다.
   `git repo would contain ... files`는 저장소 전체 파일 제한이므로 batch 축소를 하지 않습니다.
   승인된 전송의 `archive_payload=true`는 원본 파일을 `HF_PAYLOAD.tar.gz`와 manifest 두 파일로
   보관합니다. 원본별 크기/SHA와 논리 경로는 manifest에 유지합니다. 다운로드는 압축 파일의
   SHA 검증 후 선언된 일반 파일만 복원하며, 각 파일 SHA 검증 전 완료 처리하지 않습니다.
   기존 개별 파일 전송 receipt도 계속 지원합니다. 기존 원본이나 HF 이력을 삭제하지 않습니다.
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

승인된 업로드 복구는 `artifact_repair_queue`에 만료 시각과 별도 repair revision을
기록해 단일 스케줄러가 일반 publication보다 먼저 처리할 수 있습니다.
슬롯·health·저장소별 간격·429 대기를 우회하지 않으며, 제출 후 기존 유한 재시도
정책으로 인계합니다. 이번 BANKR4 E5 복구 요청의 유효기간은 1시간입니다.

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
