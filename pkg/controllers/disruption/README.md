# Karpenter Disruption 컨트롤러

이 문서는 Karpenter의 Disruption 컨트롤러와 그 작동 방식을 설명합니다.

## 개요

Disruption 컨트롤러는 Karpenter의 핵심 구성 요소로, 클러스터의 효율성과 비용을 최적화하기 위해 노드를 관리합니다. 주요 기능은 다음과 같습니다:

- 빈 노드 제거 (Emptiness)
- 드리프트된 노드 감지 및 제거 (Drift)
- 저활용 노드 통합 (Consolidation)
  - 단일 노드 통합 (SingleNodeConsolidation)
  - 다중 노드 통합 (MultiNodeConsolidation)

## 중단 워크플로우

```
                                +-------------------+
                                |   Disruption      |
                                |   Controller      |
                                +--------+---------+
                                         |
                                         v
                 +--------------------+-+-------------------+
                 |                    |                     |
                 v                    v                     v
        +--------+-------+   +--------+-------+    +--------+-------+
        |                |   |                |    |                |
        | Emptiness      |   | Drift          |    | Consolidation  |
        | (빈 노드 제거)   |   | (드리프트 감지) |    | (노드 통합)    |
        |                |   |                |    |                |
        +--------+-------+   +--------+-------+    +--------+-------+
                                                           |
                                                           |
                                         +-----------------+-----------------+
                                         |                                   |
                                         v                                   v
                                +--------+-------+                  +--------+-------+
                                |                |                  |                |
                                | SingleNode     |                  | MultiNode      |
                                | Consolidation  |                  | Consolidation  |
                                |                |                  |                |
                                +----------------+                  +----------------+
```

## 저활용(Underutilized) 노드 판별 알고리즘

Karpenter는 다음과 같은 방식으로 노드가 저활용 상태인지 판별합니다:

### 1. 통합 자격 확인

노드가 통합 대상이 될 수 있는지는 다음 조건으로 결정됩니다:

- **시간 기반 자격**: 노드가 생성된 후 `ConsolidateAfter` 시간(NodePool에 설정된 값)이 지나야 함
- **통합 정책**: NodePool의 `ConsolidationPolicy`가 `WhenEmptyOrUnderutilized`로 설정되어 있어야 함
- **Consolidatable 조건**: NodeClaim의 `Consolidatable` 상태 조건이 True여야 함

```go
// NodeClaim이 통합 가능한지 확인
if cn.NodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
    return false
}
if cn.NodePool.Spec.Disruption.ConsolidationPolicy != v1.ConsolidationPolicyWhenEmptyOrUnderutilized {
    return false
}
return cn.NodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()
```

### 2. 저활용 판별 프로세스

노드가 저활용 상태인지 판별하는 프로세스는 다음과 같습니다:

1. **후보 식별**:
   - 통합 자격이 있는 노드를 식별
   - 중단 비용(DisruptionCost)이 낮은 순서로 정렬

2. **파드 재스케줄링 시뮬레이션**:
   - `SimulateScheduling` 함수를 사용하여 노드의 파드를 다른 노드로 이동할 수 있는지 확인
   - 모든 파드가 다른 노드로 이동 가능해야 함

3. **비용 효율성 확인**:
   - 현재 노드의 비용 계산: `getCandidatePrices` 함수 사용
   - 대체 노드의 비용 계산: `RemoveInstanceTypeOptionsByPriceAndMinValues` 함수로 현재 노드보다 저렴한 인스턴스 유형만 필터링
   - 대체 노드가 현재 노드보다 저렴해야 함

4. **통합 검증**:
   - 일정 시간(consolidationTTL = 15초) 동안 대기
   - 클러스터 상태가 변경되었는지 확인
   - 파드가 여전히 재스케줄링 가능한지 확인

### 3. 저활용 판별 상세 워크플로우

```
+-------------------+
| 노드 후보 식별     |
+--------+----------+
         |
         v
+--------+----------+
| 통합 자격 확인     |
| - ConsolidateAfter|
| - 통합 정책 확인   |
+--------+----------+
         |
         v
+--------+----------+
| 파드 재스케줄링    |
| 시뮬레이션        |
+--------+----------+
         |
         v
+--------+----------+     No     +-------------------+
| 모든 파드가 이동   +----------->| 저활용 아님        |
| 가능한가?         |            +-------------------+
+--------+----------+
         | Yes
         v
+--------+----------+
| 비용 효율성 확인   |
| - 현재 노드 비용   |
| - 대체 노드 비용   |
+--------+----------+
         |
         v
+--------+----------+     No     +-------------------+
| 대체가 더 저렴한가?+----------->| 저활용 아님        |
+--------+----------+            +-------------------+
         | Yes
         v
+--------+----------+
| 통합 검증         |
| - 15초 대기       |
| - 상태 변경 확인   |
+--------+----------+
         |
         v
+--------+----------+     No     +-------------------+
| 여전히 유효한가?   +----------->| 저활용 아님        |
+--------+----------+            +-------------------+
         | Yes
         v
+--------+----------+
| 저활용으로 판별    |
| 통합 실행         |
+-------------------+
```

## 비용 계산 로직

Karpenter는 다음과 같은 방식으로 노드의 비용을 계산합니다:

### 1. 현재 노드 비용 계산

```go
func getCandidatePrices(candidates []*Candidate) (float64, error) {
    var price float64
    for _, c := range candidates {
        reqs := scheduling.NewLabelRequirements(c.StateNode.Labels())
        compatibleOfferings := c.instanceType.Offerings.Compatible(reqs)
        if len(compatibleOfferings) == 0 {
            // 예약된 용량은 무료로 처리
            if reqs.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved) {
                return 0.0, nil
            }
            return 0.0, fmt.Errorf("unable to determine offering")
        }
        price += compatibleOfferings.Cheapest().Price
    }
    return price, nil
}
```

### 2. 대체 노드 비용 필터링

```go
// 현재 노드보다 가격이 낮은 인스턴스 유형만 필터링
results.NewNodeClaims[0], err = results.NewNodeClaims[0].RemoveInstanceTypeOptionsByPriceAndMinValues(
    results.NewNodeClaims[0].Requirements, candidatePrice)
```

### 3. 비용 계산 워크플로우

```
+-------------------+
| 현재 노드 비용 계산 |
+--------+----------+
         |
         v
+--------+----------+
| 호환되는 오퍼링 찾기|
| - 인스턴스 유형    |
| - 용량 유형        |
| - 영역            |
+--------+----------+
         |
         v
+--------+----------+
| 가장 저렴한 오퍼링 |
| 가격 합산         |
+--------+----------+
         |
         v
+--------+----------+
| 대체 노드 비용 필터링|
+--------+----------+
         |
         v
+--------+----------+
| 현재 노드보다 저렴한|
| 인스턴스 유형만 선택|
+--------+----------+
         |
         v
+--------+----------+     No     +-------------------+
| 저렴한 대안이 있는가?+----------->| 통합 불가능        |
+--------+----------+            +-------------------+
         | Yes
         v
+--------+----------+
| 통합 명령 생성     |
+-------------------+
```

## 통합 방법

### 1. 단일 노드 통합 (SingleNodeConsolidation)

단일 노드 통합은 하나의 노드를 더 효율적인 노드로 대체하는 방법입니다:

1. 중단 비용이 낮은 순서로 노드를 정렬
2. 각 노드에 대해 통합 가능성 평가
3. 노드의 파드가 더 비용 효율적인 노드로 이동 가능한지 시뮬레이션
4. 비용 효율적인 대안이 있으면 노드를 대체

### 2. 다중 노드 통합 (MultiNodeConsolidation)

다중 노드 통합은 여러 노드를 하나의 노드로 통합하는 방법입니다:

1. 중단 비용이 낮은 순서로 노드를 정렬
2. 이진 검색을 사용하여 통합 가능한 최대 노드 수를 찾음
3. 선택된 노드들의 파드가 하나의 노드로 통합 가능한지 시뮬레이션
4. 통합이 비용 효율적인지 확인

```go
func (m *MultiNodeConsolidation) firstNConsolidationOption(ctx context.Context, candidates []*Candidate, max int) (Command, scheduling.Results, error) {
    // 이진 검색으로 통합 가능한 최대 노드 수 찾기
    min := 1
    if len(candidates) <= max {
        max = len(candidates) - 1
    }
    
    for min <= max {
        mid := (min + max) / 2
        candidatesToConsolidate := candidates[0 : mid+1]
        
        cmd, results, err := m.computeConsolidation(ctx, candidatesToConsolidate...)
        // 통합 가능하면 더 많은 노드 시도
        if cmd.Decision() != NoOpDecision {
            min = mid + 1
        } else {
            max = mid - 1
        }
    }
    
    return lastSavedCommand, lastSavedResults, nil
}
```

### 3. 다중 노드 통합 워크플로우

```
+-------------------+
| 후보 노드 정렬     |
| (중단 비용 기준)   |
+--------+----------+
         |
         v
+--------+----------+
| 이진 검색 시작     |
| min=1, max=N-1    |
+--------+----------+
         |
         v
+--------+----------+
| mid = (min+max)/2 |
| 노드[0:mid+1] 평가 |
+--------+----------+
         |
         v
+--------+----------+     Yes    +-------------------+
| 통합 가능한가?     +----------->| min = mid + 1     |
+--------+----------+            +-------------------+
         | No                             |
         v                                v
+--------+----------+            +--------+----------+
| max = mid - 1     |<-----------+ 계속 검색         |
+--------+----------+            +-------------------+
         |
         v
+--------+----------+     No     +-------------------+
| min <= max?       +----------->| 최적 통합 명령 반환|
+--------+----------+            +-------------------+
         | Yes
         v
+--------+----------+
| 다음 반복 계속     |
+-------------------+
```

### 4. 스팟-투-스팟 통합 특별 처리

스팟 인스턴스 간 통합은 특별한 로직을 따릅니다:

1. `SpotToSpotConsolidation` 기능 플래그가 활성화되어 있어야 함
2. 단일 노드 통합의 경우, 최소 15개의 더 저렴한 인스턴스 유형 옵션이 있어야 함
3. 현재 노드의 인스턴스 유형이 가장 저렴한 15개 옵션에 포함되지 않아야 함

```go
// 스팟-투-스팟 통합 조건 확인
if len(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions) < MinInstanceTypesForSpotToSpotConsolidation {
    return Command{}, scheduling.Results{}, nil
}

// 인스턴스 유형 제한
results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions = lo.Slice(
    results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions, 
    0, MinInstanceTypesForSpotToSpotConsolidation)
```

## 중단 제어 메커니즘

Karpenter는 중단을 제어하기 위한 여러 메커니즘을 제공합니다:

1. **중단 예산**: NodePool별로 동시에 중단할 수 있는 노드 수 제한
2. **통합 정책**: `WhenEmpty` 또는 `WhenEmptyOrUnderutilized`
3. **통합 지연**: `ConsolidateAfter` 설정으로 노드가 생성된 후 통합을 시작하기 전 대기 시간 설정
4. **검증**: 중단 전 파드 재스케줄링 가능성 확인

## 전체 Disruption 워크플로우

```
+-------------------+
| Disruption 컨트롤러|
| 시작              |
+--------+----------+
         |
         v
+--------+----------+
| 중단 방법 시도     |
| 1. Emptiness      |
| 2. Drift          |
| 3. MultiNode      |
| 4. SingleNode     |
+--------+----------+
         |
         v
+--------+----------+     Yes    +-------------------+
| 빈 노드가 있는가?  +----------->| 빈 노드 제거      |
+--------+----------+            +-------------------+
         | No                             |
         v                                v
+--------+----------+     Yes    +-------------------+
| 드리프트된 노드가  +----------->| 드리프트 노드 제거 |
| 있는가?           |            +-------------------+
+--------+----------+                     |
         | No                             v
         v                       +--------+----------+
+--------+----------+            | 대체 노드 생성     |
| 다중 노드 통합     |            | (필요한 경우)     |
| 시도              |            +-------------------+
+--------+----------+                     |
         |                                v
         v                       +--------+----------+
+--------+----------+     Yes    | 노드 테인트 적용   |
| 통합 가능한가?     +----------->| 파드 마이그레이션  |
+--------+----------+            +-------------------+
         | No                             |
         v                                v
+--------+----------+            +--------+----------+
| 단일 노드 통합     |            | 오케스트레이션 큐에|
| 시도              |            | 명령 추가         |
+--------+----------+            +-------------------+
         |                                |
         v                                v
+--------+----------+     Yes    +--------+----------+
| 통합 가능한가?     +----------->| 노드 제거 실행    |
+--------+----------+            +-------------------+
         | No
         v
+--------+----------+
| 다음 폴링 대기     |
+-------------------+
```

## 결론

Karpenter의 Disruption 컨트롤러는 클러스터의 효율성과 비용을 최적화하기 위한 다양한 방법을 제공합니다. 특히 저활용 노드 판별 알고리즘과 비용 계산 로직을 통해 클러스터 리소스를 효율적으로 관리하면서도 워크로드 중단을 최소화합니다.