[![Build Status](https://img.shields.io/github/actions/workflow/status/aws/karpenter-core/presubmit.yaml?branch=main)](https://github.com/aws/karpenter-core/actions/workflows/presubmit.yaml)
![GitHub stars](https://img.shields.io/github/stars/aws/karpenter-core)
![GitHub forks](https://img.shields.io/github/forks/aws/karpenter-core)
[![GitHub License](https://img.shields.io/badge/License-Apache%202.0-ff69b4.svg)](https://github.com/aws/karpenter-core/blob/main/LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/aws/karpenter-core)](https://goreportcard.com/report/github.com/aws/karpenter-core)
[![Coverage Status](https://coveralls.io/repos/github/aws/karpenter-core/badge.svg?branch=main)](https://coveralls.io/github/aws/karpenter-core?branch=main)
[![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)](https://github.com/aws/karpenter-core/issues)

# Karpenter

Karpenter는 다음과 같은 방법으로 Kubernetes 클러스터에서 워크로드 실행의 효율성과 비용을 개선합니다:

* Kubernetes 스케줄러가 스케줄 불가능으로 표시한 파드를 **감시**합니다.
* 파드가 요청한 스케줄링 제약 조건(리소스 요청, 노드 선택기, 어피니티, 톨러레이션, 토폴로지 스프레드 제약 조건)을 **평가**합니다.
* 파드의 요구 사항을 충족하는 노드를 **프로비저닝**합니다.
* 노드가 더 이상 필요하지 않을 때 노드를 **제거**합니다.

## Karpenter 구현체
Karpenter는 다음과 같은 클라우드 제공업체에 의해 구현된 멀티 클라우드 프로젝트입니다:
- [AWS](https://github.com/aws/karpenter-provider-aws)
- [Azure](https://github.com/Azure/karpenter-provider-azure)
- [AlibabaCloud](https://github.com/cloudpilot-ai/karpenter-provider-alibabacloud)
- [Cluster API](https://github.com/kubernetes-sigs/karpenter-provider-cluster-api)
- [GCP](https://github.com/cloudpilot-ai/karpenter-provider-gcp)
- [Proxmox](https://github.com/sergelogvinov/karpenter-provider-proxmox)

## 커뮤니티, 토론, 기여 및 지원

질문이 있거나 최신 프로젝트 소식을 얻고 싶다면 다음과 같은 방법으로 연결할 수 있습니다:
- **Karpenter를 사용하고 배포하시나요?** [Kubernetes 슬랙](https://slack.k8s.io/)의 [#karpenter](https://kubernetes.slack.com/archives/C02SFFZSA2K) 채널에서 Karpenter 구성 또는 문제 해결에 대한 질문을 할 수 있습니다.
- **Karpenter에 기여하거나 개발하시나요?** [Kubernetes 슬랙](https://slack.k8s.io/)의 [#karpenter-dev](https://kubernetes.slack.com/archives/C04JW2J5J5P) 채널에서 기여에 대한 심층적인 질문을 하거나 설계 논의에 참여할 수 있습니다.

### 워킹 그룹 미팅
격주로 목요일 @ 9:00 PT ([시간대 변환](http://www.thetimezoneconverter.com/?t=9:00&tz=Seattle))와 목요일 @ 15:00 PT ([시간대 변환](http://www.thetimezoneconverter.com/?t=15:00&tz=Seattle)) 사이에 번갈아 진행됩니다.

### 이슈 분류 미팅
저장소와 시간 슬롯 사이에 번갈아 가며 매주 미팅이 있습니다. 특정 날짜는 캘린더 초대를 확인하세요:

**kubernetes-sigs/karpenter**:
- 월요일 @ 9:00 PT ([시간대 변환](http://www.thetimezoneconverter.com/?t=9:00&tz=Seattle))와 @ 15:00 PT [시간대 변환](http://www.thetimezoneconverter.com/?t=15:00&tz=Seattle) 사이에 매월 번갈아 진행

**aws/karpenter-provider-aws**:
- 월요일 @ 9:00 PT ([시간대 변환](http://www.thetimezoneconverter.com/?t=9:00&tz=Seattle))와 @ 15:00 PT [시간대 변환](http://www.thetimezoneconverter.com/?t=15:00&tz=Seattle) 사이에 매월 번갈아 진행

#### 미팅 리소스
- **Zoom 링크**: [미팅 참여](https://zoom.us/j/95618088729) (비밀번호: 77777)
- **캘린더**: [Google 캘린더](https://calendar.google.com/calendar/u/0?cid=N3FmZGVvZjVoZWJkZjZpMnJrMmplZzVqYmtAZ3JvdXAuY2FsZW5kYXIuZ29vZ2xlLmNvbQ) 구독
- **미팅 노트**: [워킹 그룹 로그](https://docs.google.com/document/d/18BT0AIMugpNpiSPJNlcAL2rv69yAE6Z06gUVj7v_clg/edit?usp=sharing) 보기

풀 리퀘스트와 이슈에 대한 피드백은 매우 환영합니다!
어디서부터 시작해야 할지 모르겠다면 [이슈 트래커](https://github.com/aws/karpenter-core/issues)를 참조하세요. 특히 [Good first issue](https://github.com/aws/karpenter-core/issues?q=is%3Aopen+is%3Aissue+label%3Agood-first-issue)와 [Help wanted](https://github.com/aws/karpenter-core/issues?utf8=%E2%9C%93&q=is%3Aopen+is%3Aissue+label%3Ahelp-wanted) 태그가 있는 이슈를 확인하고, 논의를 위해 자유롭게 연락하세요.

또한 [기여자 가이드](CONTRIBUTING.md)와 Kubernetes [커뮤니티 페이지](https://kubernetes.io/community)에서 참여 방법에 대한 자세한 내용을 확인하세요.

### 행동 강령

Kubernetes 커뮤니티 참여는 [Kubernetes 행동 강령](code-of-conduct.md)에 의해 관리됩니다.

## 발표 자료
- 09/08/2022 [Karpenter를 사용한 워크로드 통합](https://youtu.be/BnksdJ3oOEs)
- 05/19/2022 [은행이나 정신을 깨지 않고 K8s 노드 확장하기](https://www.youtube.com/watch?v=UBb8wbfSc34)
- 03/25/2022 [AWS 커뮤니티 데이 2022의 Karpenter](https://youtu.be/sxDtmzbNHwE?t=3931)
- 12/20/2021 [Karpenter로 Kubernetes 클러스터를 자동 확장하는 방법](https://youtu.be/C-2v7HT-uSA)
- 11/30/2021 [Karpenter vs Kubernetes Cluster Autoscaler](https://youtu.be/3QsVRHVdOnM)
- 11/19/2021 [Container Day의 Karpenter](https://youtu.be/qxWJRUF6JJc)
- 05/14/2021 [Kubecon의 그룹 없는 자동 확장 with Karpenter](https://www.youtube.com/watch?v=43g8uPohTgc)
- 05/04/2021 [Container Day의 Karpenter](https://youtu.be/MZ-4HzOC_ac?t=7137)

## Karpenter 워크플로우

Karpenter는 다음과 같은 워크플로우로 작동합니다:

### 1. 파드 감시
Karpenter는 Kubernetes API 서버를 지속적으로 모니터링하여 스케줄링되지 않은 파드를 감지합니다. 이러한 파드는 일반적으로 클러스터에 적절한 노드가 없어서 `Pending` 상태로 남아 있습니다.

### 2. 요구사항 평가
파드가 감지되면 Karpenter는 다음과 같은 파드의 스케줄링 요구사항을 분석합니다:
- 리소스 요청 (CPU, 메모리, GPU 등)
- 노드 선택기 (nodeSelector)
- 어피니티 규칙 (affinity)
- 톨러레이션 (tolerations)
- 토폴로지 스프레드 제약 조건 (topologySpreadConstraints)

### 3. 노드 프로비저닝
Karpenter는 파드의 요구사항을 충족하는 최적의 노드 유형을 결정하고 해당 노드를 프로비저닝합니다. 이 과정에서 Karpenter는:
- 비용 최적화를 위해 노드 유형을 선택합니다
- 필요한 용량을 정확히 프로비저닝합니다
- 클라우드 제공업체 API를 사용하여 노드를 생성합니다
- 노드가 클러스터에 등록되고 준비될 때까지 기다립니다

### 4. 노드 제거
Karpenter는 다음과 같은 경우에 노드를 제거합니다:
- 노드가 비어 있을 때 (파드가 없음)
- 노드가 만료되었을 때 (TTL 초과)
- 노드가 드리프트되었을 때 (구성이 변경됨)
- 통합 기회가 있을 때 (더 효율적인 노드 구성으로 이동)

이 프로세스는 클러스터 리소스 사용을 최적화하고 비용을 절감하는 데 도움이 됩니다.

## 주요 구성 요소

### NodePool
NodePool은 Karpenter가 노드를 프로비저닝하는 방법을 정의하는 사용자 정의 리소스입니다. 이는 다음을 지정합니다:
- 노드 요구사항 (인스턴스 유형, 영역 등)
- 제약 조건 (최소/최대 노드 수)
- 만료 설정
- 중단 설정

### NodeClaim
NodeClaim은 Karpenter가 프로비저닝한 노드를 나타내는 내부 리소스입니다. 이는 다음을 포함합니다:
- 노드 메타데이터
- 프로비저닝 상태
- 클라우드 제공업체 정보

### 컨트롤러
Karpenter는 여러 컨트롤러로 구성되어 있으며, 각각 특정 기능을 담당합니다:
- 프로비저닝 컨트롤러: 노드 생성 관리
- 중단 컨트롤러: 노드 제거 관리
- 노드 상태 컨트롤러: 노드 상태 모니터링
- 만료 컨트롤러: TTL 기반 노드 제거 관리