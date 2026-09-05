# 오픈소스 검토 및 첫 구현 선택

확인일: 2026-09-06. 공식 문서 기준이며 상용 기능과 OSS 기능을 구분합니다.

| 후보 | 적합한 점 | 이번 환경에서 추가로 필요한 부분 |
|---|---|---|
| Slurm | 클러스터 자원 할당·batch 실행, 성공 의존성, GPU GRES | 상시 daemon/클러스터 운영 구성, 연구 RQ·설정 UI, 현존 비관리 프로세스 및 NFS health 정책과 통합 |
| ClearML | 실험 추적, worker/queue 기반 작업 실행 | 원하는 동적 GPU 수 할당은 공식 문서상 Enterprise 기능; 우리 복구·NFS gate 정책은 별도 검토 필요 |
| Ray | Python distributed task/actor, CPU/GPU/custom resource 스케줄링 | logical resource를 실제 VRAM 사용 보장으로 볼 수 없음; 기존 임의 shell/학습 launcher와 health/장애 규칙 통합 필요 |

Slurm은 자원 할당과 job 관리를 위한 클러스터 시스템입니다. `sbatch --dependency=afterok:...`
같은 의존성 및 GRES GPU 자원을 지원합니다.
[Slurm overview](https://slurm.schedmd.com/overview.html),
[sbatch dependencies](https://slurm.schedmd.com/sbatch.html),
[GPU GRES](https://slurm.schedmd.com/gres.html).

ClearML은 agent가 queue의 task를 실행하는 방식입니다. 공식 문서는 `--dynamic-gpus`를
Enterprise 기능으로 명시하며 Docker mode를 요구합니다. 이를 전부 OSS 기본 기능이라고
간주해 이번 요구를 충족한다고 설명하지 않습니다.
[Workers and queues](https://clear.ml/docs/latest/docs/fundamentals/agents_and_queues/),
[Dynamic GPU allocation](https://clear.ml/docs/latest/docs/clearml_agent/clearml_agent_dynamic_gpus/).

Ray의 CPU/GPU/custom resource는 logical admission resource이며, 물리적인 자원 사용량을
그 값으로 강제 제한하는 것과 다릅니다. 실제 idle/VRAM/D-state 기반 admission은 추가 계층이
필요하다는 판단입니다.
[Ray resources](https://docs.ray.io/en/latest/ray-core/scheduling/resources.html).

## 선택

첫 버전은 **독립된 stdlib Python 패키지 + local SQLite + SSH 실행기**로 구현했습니다.
서버 재부팅·Docker 작업·다른 사용자 프로세스 종료 없이, 이미 준비된 사용자 환경의
명령을 실행해야 한다는 제약과 작은 설치 범위에 맞춘 선택입니다.

이는 Slurm 수준의 multi-user/HPC scheduler를 새로 대체하겠다는 의미가 아닙니다.
연구 registry/상태 모델과 실행 transport를 분리했으므로 추후 Slurm 등의 executor를
연결하는 방향이 가능합니다. 해당 backend는 아직 구현하지 않았습니다.

OSS 구현 코드를 복사하거나 vendor하지 않았으며, 위 문서의 기능/운영 모델을 참고했습니다.
장기 multi-user 배포, hard resource isolation, 인증/감사 요구가 생기면 기존 운영 플랫폼
도입 비용과 자체 유지 비용을 다시 비교해야 합니다.
