# LAB4 attempt archive

모델 attempt가 성공·실패로 종료되고 현재 사용 중이지 않으며 최근 5분 동안 scheduler 실행/전송에서
사용되지 않았다면 HF 없이 LAB4에 보관한다. `configs/attempt_archive_lab4_v2.json`의 27개 캠페인은 기존
이력을 포함한 backfill 대상이다. `automatic_since` 이후 생성 또는 종료된 attempt는 캠페인 목록을
추가하지 않아도 보관한다. 무캠페인 실험은 `uncategorized` 아래에 보관한다. 하드웨어 DB에는 적용하지 않는다.
보관 단위는 attempt 디렉터리의 모든 일반 파일이며 dataset/runtime/cache 등의 외부 참조는 별도다.

`backfill_nodes`는 디스크 회수가 필요한 노드의 과거 terminal attempt를 캠페인과 무관하게 우선
보관한다. 현재 RP2가 지정돼 있다. 제거된 RP1의 과거 archive/attempt 이력은 보존하지만 새 보관
source로 조회하지 않는다. 실행 중 attempt와 최근 5분 이내 사용된 attempt는 제외하며,
LAB4의 전체 파일 SHA 재검증과 원본 무사용 검사가 모두 끝난 뒤에만 원본을 삭제한다.

## 경로

```text
<LAB4 work_root>/attempt-archive/
  <primary-campaign-id>/
    <experiment-id>/
      <job-id>/
        <attempt-id>/
```

하나의 experiment가 여러 대상 캠페인에 속하면 정책 파일에서 먼저 나온 캠페인이 물리 경로를 소유한다.
archive receipt의 `campaigns`에는 관련 대상 캠페인을 모두 기록한다.

원본이 삭제된 뒤 해당 dependency가 다시 필요하면 scheduler는 옛 원본 경로를 사용하지 않는다. LAB4의
검증된 complete-attempt archive를 source로 선택해 필요한 실행 서버로 다시 relay한 뒤 작업을 시작한다.
원본 정리는 `backfill_nodes` 순서를 우선하며, 검증 불가 원본은 보존하고 주기당 최대 3건만 재검사해
다른 archive 및 모델 배정을 막지 않는다.

## 포함 범위

`spec.json`, `config.json`, `state.json`, runner와 stdout/stderr log, attempt 아래에 생성된 checkpoint와
중간·최종 산출물을 모두 파일별 크기와 SHA-256으로 고정한다. attempt 밖의 dataset, runtime, cache 또는
외부 절대경로 checkpoint는 보관 대상이 아니다. symlink나 특수 파일이 있으면 일부만 복사하지 않고 해당
archive를 실패시켜 원본을 유지한다.

## 전송

- FARM 원본: 원본 → controller local bounded staging → LAB4
- LAB/CPS/RP 원본: LAB4가 원본의 검증된 SSH endpoint에 직접 연결하여 rsync pull
- LAB4 원본: LAB4 내부 복사 및 검증
- resource-control 원본: 원본인 제어 머신 → LAB4 stream
- HF upload/download: 사용하지 않음

클러스터 전송 슬롯은 두 개이며, 실행 가능한 후속 작업의 dependency relay가 일반 보관보다 먼저 최대
한 슬롯을 사용한다. LAB4 전체 attempt 보관은 동시에 최대 한 슬롯만 사용한다. 따라서 과거 attempt
backfill이 계속 남아 있어도 후속 학습·평가 입력 relay가 매 주기 보관 예약에 밀리지 않는다.

직접 연결에는 controller의 수명이 제한된 SSH agent를 LAB4에 전달하며 private key 파일은 서버에 복제하지
않는다. 원본 host key는 기존 신뢰된 known_hosts를 사용하고, 직접 연결 실패 시 원본을 보존한다.
LAB4의 `.staging` 경로에 전송한 뒤 모든 파일의 크기·SHA-256을 다시 읽어 검증하고, 검증이 끝난 디렉터리만
최종 경로로 원자적으로 이동한다. archive receipt가 등록되면 이후 dependency relay는 삭제된 원본이 아니라
LAB4 보관본을 source authority로 사용한다. FARM의 controller staging은 검증 receipt를 먼저
디스크에 동기화한 후 삭제한다. ACK 유실 후 기존 archive가 있으면 전체 manifest가 같을 때만 재사용한다.

## 원본 삭제

LAB4 archive가 완전하게 검증된 뒤에도 바로 삭제하지 않는다. 다음 조건을 모두 충족해야 한다.

- source attempt가 `succeeded` 또는 `failed` terminal 상태
- 원본의 현재 전체 manifest가 LAB4 receipt와 동일
- 등록 runner/자식 프로세스가 살아 있지 않음
- active attempt/transfer가 원본 경로를 참조하지 않음
- attempt 또는 transfer의 마지막 완료·사용·archive 시점에서 최소 300초 경과
- queued 작업에 원본 절대경로가 고정된 경우 삭제 보류
- 삭제 직전 LAB4 전체 보관본 재검증 및 원본의 현재 열린 파일/runner 확인

삭제 RPC는 frozen attempt의 ID, work root, manifest SHA를 다시 검증한다. 조건 불충족·SSH 오류·manifest
변경 시 원본은 그대로 유지하며, 삭제 성공 receipt를 DB event와 archive transfer report에 기록한다.
삭제 RPC 전에 DB에 `origin_retired`를 기록하므로 응답 유실 후에도 새 요청은 옛 원본 경로를 선택하지 않는다.
과거 파일 접근을 filesystem atime만으로 추정하지 않으며, scheduler 사용 기록과 현재 프로세스의 열린
파일을 확인한다. 확인 권한이 없으면 삭제를 보류한다.

## 별도 전송 서비스

`research-artifact-retention.service`는 `research_scheduler.artifact_service`를 5초 목표 간격으로
실행한다. 모델 controller와 같은 DB를 사용하며 `artifact_service_config(id=1, enabled=1)`로
전송 처리 소유를 지정한다. 모델 controller는 이 설정에서 artifact tick을 수행하지 않는다.
기존 `artifact_transfers`의 starting/running/unknown 예약을 이어서 회수하므로 worker를 중복 실행하지 않는다.

서비스 단일 실행은 `<DB stem>.artifacts.lock`으로 보장한다. 예약은 공용 registry flock 안에서 DB에
먼저 commit하고, SSH launch/status/SHA 검증/삭제는 registry flock과 SQLite transaction 밖에서 수행한다.
응답 후 잠금을 다시 잡아 receipt를 기록한다. 원본 삭제는 검증 후 잠금 아래에서 현재 참조를 재확인하고
`origin_retired`를 commit한 뒤 수행한다. 실패 또는 응답 유실은 미확인 예약을 보존하며, 시간 경과만으로
중복 실행을 허용하지 않는다. 정상 종료나 프로세스 죽음에는 OS가 서비스 소유 잠금을 해제한다.

`artifact_runtime/STATE.json`은 주기 시간, registry 잠금 보유 시간, 원격 RPC 시간을 기록한다.
원본 삭제는 같은 서비스의 독립 cleanup thread와 별도 SQLite connection에서 처리하고
`artifact_runtime/CLEANUP.json`에 기록한다. 삭제 RPC가 느려도 전송 큐는 다음 요청을 처리할 수 있다.
별도 서비스는 전송 처리만 하며 모델 배정·GPU probe·실험 등록을 수행하지 않는다. 전송 계획 계산에는
공용 잠금을 사용하므로 DB 계산 자체의 비용은 여전히 측정 대상이다.
HF 비활성 상태에서는 전송 계획이 queued 작업·직접 선행·활성 attempt의 작업 profile만 읽는다.
종료된 전송은 worker 실행 코드(argv)를 제외하고 조회한다. 목표 주기를 초과하더라도 다음 전송 주기
전에 최소 1초를 양보하며, controller는 짧은 registry 경합을 최대 15초 기다린다.

원복 시에는 먼저 artifact 서비스를 중지하고 공용 registry 잠금 아래 `artifact_service_config.enabled=0`으로
바꾼다. 새 controller는 그때만 artifact tick을 재개한다. 서비스 장애를 이유로 DB나 두 번째 모델
controller를 생성하지 않는다.

## Report 실행

새 report는 LAB4에서 작성한다. `report_placement.on_archive_host()` 또는 공용 등록의 자동 변환을 통해
`report_execution=archive_host`와 LAB4 실행 profile을 고정한다. 기존 producer-host 정책은 이전 frozen
attempt의 해석을 위해 남기며 신규 등록 정책은 LAB4를 따른다. LAB4 profile이 없는 비표준 runtime은
다른 host로 fallback하지 않고 명시적 준비를 요구한다.
