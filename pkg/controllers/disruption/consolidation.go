/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// consolidation.go 파일은 노드 통합(consolidation)의 기본 기능을 구현합니다.
// 이 파일은 여러 노드를 더 효율적인 노드로 통합하는 로직을 제공합니다.
// 통합은 비용 최적화를 위해 중요한 기능으로, 클러스터의 리소스 사용률을 높이고 비용을 절감합니다.
package disruption

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/orchestration"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	pscheduling "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// consolidationTTL은 통합 명령을 생성하고 해당 명령이 여전히 작동하는지 검증하는 사이의 TTL입니다.
// 이 시간 동안 클러스터 상태가 변경되지 않으면 통합 명령이 유효하다고 간주됩니다.
const consolidationTTL = 15 * time.Second

// MinInstanceTypesForSpotToSpotConsolidation은 스팟-투-스팟 단일 노드 통합을 트리거하는 데 필요한 NodeClaim의 최소 인스턴스 유형 수입니다.
// 이 값은 스팟 인스턴스 간의 반복적인 통합을 방지하기 위해 사용됩니다.
const MinInstanceTypesForSpotToSpotConsolidation = 15

// consolidation은 다양한 통합 방법에서 사용되는 공통 기능을 제공하는 기본 통합 컨트롤러입니다.
// 이 구조체는 SingleNodeConsolidation과 MultiNodeConsolidation에서 상속되어 사용됩니다.
type consolidation struct {
	// queue는 검증을 위해 통합이 알고 있어야 하는 오케스트레이션 큐입니다.
	queue                  *orchestration.Queue
	// clock은 시간 관련 작업에 사용됩니다.
	clock                  clock.Clock
	// cluster는 클러스터 상태 정보를 제공합니다.
	cluster                *state.Cluster
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient             client.Client
	// provisioner는 노드 프로비저닝을 담당합니다.
	provisioner            *provisioning.Provisioner
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider          cloudprovider.CloudProvider
	// recorder는 이벤트를 기록하는 데 사용됩니다.
	recorder               events.Recorder
	// lastConsolidationState는 마지막 통합 상태의 시간을 추적합니다.
	lastConsolidationState time.Time
}

// MakeConsolidation은 새로운 통합 구조체를 생성합니다.
// 이 함수는 통합에 필요한 모든 의존성을 주입하고 초기화된 통합 구조체를 반환합니다.
func MakeConsolidation(clock clock.Clock, cluster *state.Cluster, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cloudProvider cloudprovider.CloudProvider, recorder events.Recorder, queue *orchestration.Queue) consolidation {
	return consolidation{
		queue:         queue,
		clock:         clock,
		cluster:       cluster,
		kubeClient:    kubeClient,
		provisioner:   provisioner,
		cloudProvider: cloudProvider,
		recorder:      recorder,
	}
}

// IsConsolidated는 markConsolidated가 호출된 이후 변경된 것이 없으면 true를 반환합니다.
// 이 함수는 클러스터 상태가 마지막 통합 이후 변경되었는지 확인하는 데 사용됩니다.
func (c *consolidation) IsConsolidated() bool {
	return c.lastConsolidationState.Equal(c.cluster.ConsolidationState())
}

// markConsolidated는 클러스터의 현재 상태를 기록합니다.
// 이 함수는 통합 작업이 완료된 후 클러스터 상태를 저장하는 데 사용됩니다.
func (c *consolidation) markConsolidated() {
	c.lastConsolidationState = c.cluster.ConsolidationState()
}

// ShouldDisrupt는 후보를 필터링하는 데 사용되는 조건자입니다.
// 이 함수는 주어진 후보 노드가 통합 대상이 될 수 있는지 평가합니다.
// 인스턴스 유형, 용량 유형, 영역 등의 조건을 확인하여 통합 가능 여부를 결정합니다.
func (c *consolidation) ShouldDisrupt(_ context.Context, cn *Candidate) bool {
	// We need the following to know what the price of the instance for price comparison. If one of these doesn't exist, we can't
	// compute consolidation decisions for this candidate.
	// 1. Instance Type
	// 2. Capacity Type
	// 3. Zone
	if cn.instanceType == nil {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Instance Type %q not found", cn.Labels()[corev1.LabelInstanceTypeStable]))...)
		return false
	}
	if _, ok := cn.Labels()[v1.CapacityTypeLabelKey]; !ok {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Node does not have label %q", v1.CapacityTypeLabelKey))...)
		return false
	}
	if _, ok := cn.Labels()[corev1.LabelTopologyZone]; !ok {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("Node does not have label %q", corev1.LabelTopologyZone))...)
		return false
	}
	if cn.NodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("NodePool %q has consolidation disabled", cn.NodePool.Name))...)
		return false
	}
	// If we don't have the "WhenEmptyOrUnderutilized" policy set, we should not do any of the consolidation methods, but
	// we should also not fire an event here to users since this can be confusing when the field on the NodePool
	// is named "consolidationPolicy"
	if cn.NodePool.Spec.Disruption.ConsolidationPolicy != v1.ConsolidationPolicyWhenEmptyOrUnderutilized {
		c.recorder.Publish(disruptionevents.Unconsolidatable(cn.Node, cn.NodeClaim, fmt.Sprintf("NodePool %q has non-empty consolidation disabled", cn.NodePool.Name))...)
		return false
	}
	// return true if consolidatable
	return cn.NodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable).IsTrue()
}

// sortCandidates는 중단 비용(가장 낮은 중단 비용이 먼저)으로 후보를 정렬하고 결과를 반환합니다.
// 이 함수는 중단 비용이 가장 낮은 노드부터 평가하여 클러스터 중단을 최소화하는 데 사용됩니다.
func (c *consolidation) sortCandidates(candidates []*Candidate) []*Candidate {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].DisruptionCost < candidates[j].DisruptionCost
	})
	return candidates
}

// computeConsolidation은 수행할 통합 작업을 계산합니다.
// 이 함수는 주어진 후보 노드들을 평가하고 적절한 통합 명령을 생성합니다.
// 통합 명령은 노드 삭제 또는 노드 대체를 포함할 수 있습니다.
//
// nolint:gocyclo
func (c *consolidation) computeConsolidation(ctx context.Context, candidates ...*Candidate) (Command, pscheduling.Results, error) {
	var err error
	// Run scheduling simulation to compute consolidation option
	results, err := SimulateScheduling(ctx, c.kubeClient, c.cluster, c.provisioner, candidates...)
	if err != nil {
		// if a candidate node is now deleting, just retry
		if errors.Is(err, errCandidateDeleting) {
			return Command{}, pscheduling.Results{}, nil
		}
		return Command{}, pscheduling.Results{}, err
	}

	// if not all of the pods were scheduled, we can't do anything
	if !results.AllNonPendingPodsScheduled() {
		// This method is used by multi-node consolidation as well, so we'll only report in the single node case
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
		}
		return Command{}, pscheduling.Results{}, nil
	}

	// were we able to schedule all the pods on the inflight candidates?
	if len(results.NewNodeClaims) == 0 {
		return Command{
			candidates: candidates,
		}, results, nil
	}

	// we're not going to turn a single node into multiple candidates
	if len(results.NewNodeClaims) != 1 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Can't remove without creating %d candidates", len(results.NewNodeClaims)))...)
		}
		return Command{}, pscheduling.Results{}, nil
	}

	// get the current node price based on the offering
	// fallback if we can't find the specific zonal pricing data
	candidatePrice, err := getCandidatePrices(candidates)
	if err != nil {
		return Command{}, pscheduling.Results{}, fmt.Errorf("getting offering price from candidate node, %w", err)
	}

	allExistingAreSpot := true
	for _, cn := range candidates {
		if cn.capacityType != v1.CapacityTypeSpot {
			allExistingAreSpot = false
		}
	}

	// sort the instanceTypes by price before we take any actions like truncation for spot-to-spot consolidation or finding the nodeclaim
	// that meets the minimum requirement after filteringByPrice
	results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions = results.NewNodeClaims[0].InstanceTypeOptions.OrderByPrice(results.NewNodeClaims[0].Requirements)

	if allExistingAreSpot &&
		results.NewNodeClaims[0].Requirements.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeSpot) {
		return c.computeSpotToSpotConsolidation(ctx, candidates, results, candidatePrice)
	}

	// filterByPrice는 현재 후보보다 가격이 낮은 인스턴스 유형과 입력을 필터링할 수 없음을 나타내는 오류를 반환합니다.
	// 이것을 스팟-투-스팟 통합에 직접 사용하면 목록에서 스팟 인스턴스를 시작하기로 선택하는 전략이 반복적인 통합을 초래할 수 있습니다.
	// 이는 가용성과 가격을 기반으로 하므로 목록에서 가장 낮은 가격의 인스턴스가 아닌 인스턴스를 선택/시작할 수 있습니다.
	// 따라서 가장 낮은 가격의 인스턴스에 도달할 때까지 이 루프를 계속 반복하게 되어 변동을 일으키고 가용성이 낮은 스팟 인스턴스로 이어져 결국 더 높은 중단을 초래합니다.
	results.NewNodeClaims[0], err = results.NewNodeClaims[0].RemoveInstanceTypeOptionsByPriceAndMinValues(results.NewNodeClaims[0].Requirements, candidatePrice)

	if err != nil {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Filtering by price: %v", err))...)
		}
		return Command{}, pscheduling.Results{}, nil
	}
	if len(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions) == 0 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "Can't replace with a cheaper node")...)
		}
		return Command{}, pscheduling.Results{}, nil
	}

	// We are consolidating a node from OD -> [OD,Spot] but have filtered the instance types by cost based on the
	// assumption, that the spot variant will launch. We also need to add a requirement to the node to ensure that if
	// spot capacity is insufficient we don't replace the node with a more expensive on-demand node.  Instead the launch
	// should fail and we'll just leave the node alone. We don't need to do the same for reserved since the requirements
	// are injected on by the scheduler.
	ctReq := results.NewNodeClaims[0].Requirements.Get(v1.CapacityTypeLabelKey)
	if ctReq.Has(v1.CapacityTypeSpot) && ctReq.Has(v1.CapacityTypeOnDemand) {
		results.NewNodeClaims[0].Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	}

	return Command{
		candidates:   candidates,
		replacements: results.NewNodeClaims,
	}, results, nil
}

// Compute command to execute spot-to-spot consolidation if:
//  1. The SpotToSpotConsolidation feature flag is set to true.
//  2. For single-node consolidation:
//     a. There are at least 15 cheapest instance type replacement options to consolidate.
//     b. The current candidate is NOT part of the first 15 cheapest instance types inorder to avoid repeated consolidation.
//
// computeSpotToSpotConsolidation은 다음 조건이 충족될 때 스팟-투-스팟 통합을 실행하는 명령을 계산합니다:
//  1. SpotToSpotConsolidation 기능 플래그가 true로 설정되어 있음.
//  2. 단일 노드 통합의 경우:
//     a. 통합할 수 있는 가장 저렴한 인스턴스 유형 대체 옵션이 최소 15개 있음.
//     b. 반복적인 통합을 방지하기 위해 현재 후보는 가장 저렴한 15개 인스턴스 유형에 포함되지 않음.
func (c *consolidation) computeSpotToSpotConsolidation(ctx context.Context, candidates []*Candidate, results pscheduling.Results,
	candidatePrice float64) (Command, pscheduling.Results, error) {

	// Spot consolidation is turned off.
	if !options.FromContext(ctx).FeatureGates.SpotToSpotConsolidation {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "SpotToSpotConsolidation is disabled, can't replace a spot node with a spot node")...)
		}
		return Command{}, pscheduling.Results{}, nil
	}

	// Since we are sure that the replacement nodeclaim considered for the spot candidates are spot, we will enforce it through the requirements.
	results.NewNodeClaims[0].Requirements.Add(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	// All possible replacements for the current candidate compatible with spot offerings
	results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions = results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions.Compatible(results.NewNodeClaims[0].Requirements)

	// filterByPrice returns the instanceTypes that are lower priced than the current candidate and any error that indicates the input couldn't be filtered.
	var err error
	results.NewNodeClaims[0], err = results.NewNodeClaims[0].RemoveInstanceTypeOptionsByPriceAndMinValues(results.NewNodeClaims[0].Requirements, candidatePrice)
	if err != nil {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("Filtering by price: %v", err))...)
		}
		return Command{}, pscheduling.Results{}, nil
	}
	if len(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions) == 0 {
		if len(candidates) == 1 {
			c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, "Can't replace with a cheaper node")...)
		}
		return Command{}, pscheduling.Results{}, nil
	}

	// For multi-node consolidation:
	// We don't have any requirement to check the remaining instance type flexibility, so exit early in this case.
	if len(candidates) > 1 {
		return Command{
			candidates:   candidates,
			replacements: results.NewNodeClaims,
		}, results, nil
	}

	// For single-node consolidation:

	// We check whether we have 15 cheaper instances than the current candidate instance. If this is the case, we know the following things:
	//   1) The current candidate is not in the set of the 15 cheapest instance types and
	//   2) There were at least 15 options cheaper than the current candidate.
	if len(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions) < MinInstanceTypesForSpotToSpotConsolidation {
		c.recorder.Publish(disruptionevents.Unconsolidatable(candidates[0].Node, candidates[0].NodeClaim, fmt.Sprintf("SpotToSpotConsolidation requires %d cheaper instance type options than the current candidate to consolidate, got %d",
			MinInstanceTypesForSpotToSpotConsolidation, len(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions)))...)
		return Command{}, pscheduling.Results{}, nil
	}

	// If a user has minValues set in their NodePool requirements, then we cap the number of instancetypes at 100 which would be the actual number of instancetypes sent for launch to enable spot-to-spot consolidation.
	// If no minValues in the NodePool requirement, then we follow the default 15 to cap the instance types for launch to enable a spot-to-spot consolidation.
	// Restrict the InstanceTypeOptions for launch to 15(if default) so we don't get into a continual consolidation situation.
	// For example:
	// 1) Suppose we have 5 instance types, (A, B, C, D, E) in order of price with the minimum flexibility 3 and they’ll all work for our pod.  We send CreateInstanceFromTypes(A,B,C,D,E) and it gives us a E type based on price and availability of spot.
	// 2) We check if E is part of (A,B,C,D) and it isn't, so we will immediately have consolidation send a CreateInstanceFromTypes(A,B,C,D), since they’re cheaper than E.
	// 3) Assuming CreateInstanceFromTypes(A,B,C,D) returned D, we check if D is part of (A,B,C) and it isn't, so will have another consolidation send a CreateInstanceFromTypes(A,B,C), since they’re cheaper than D resulting in continual consolidation.
	// If we had restricted instance types to min flexibility at launch at step (1) i.e CreateInstanceFromTypes(A,B,C), we would have received the instance type part of the list preventing immediate consolidation.
	// Taking this to 15 types, we need to only send the 15 cheapest types in the CreateInstanceFromTypes call so that the resulting instance is always in that set of 15 and we won’t immediately consolidate.
	if results.NewNodeClaims[0].Requirements.HasMinValues() {
		// Here we are trying to get the max of the minimum instances required to satisfy the minimum requirement and the default 15 to cap the instances for spot-to-spot consolidation.
		minInstanceTypes, _ := results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions.SatisfiesMinValues(results.NewNodeClaims[0].Requirements)
		results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions = lo.Slice(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions, 0, lo.Max([]int{MinInstanceTypesForSpotToSpotConsolidation, minInstanceTypes}))
	} else {
		results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions = lo.Slice(results.NewNodeClaims[0].NodeClaimTemplate.InstanceTypeOptions, 0, MinInstanceTypesForSpotToSpotConsolidation)
	}

	return Command{
		candidates:   candidates,
		replacements: results.NewNodeClaims,
	}, results, nil
}

// getCandidatePrices는 주어진 후보들의 가격 합계를 반환합니다.
// 이 함수는 통합 결정을 내리기 위해 현재 노드의 비용을 계산하는 데 사용됩니다.
func getCandidatePrices(candidates []*Candidate) (float64, error) {
	var price float64
	for _, c := range candidates {
		reqs := scheduling.NewLabelRequirements(c.StateNode.Labels())
		compatibleOfferings := c.instanceType.Offerings.Compatible(reqs)
		if len(compatibleOfferings) == 0 {
			// It's expected that offerings may no longer exist for capacity reservations once a NodeClass stops selecting on
			// them (or they are no longer considered for some other reason on by the cloudprovider). By definition though,
			// reserved capacity is free. By modeling it as free, consolidation won't be able to succeed, but the node should be
			// disrupted via drift regardless.
			if reqs.Get(v1.CapacityTypeLabelKey).Has(v1.CapacityTypeReserved) {
				return 0.0, nil
			}
			return 0.0, serrors.Wrap(fmt.Errorf("unable to determine offering"), "instance-type", c.instanceType.Name, "capacity-type", c.capacityType, "zone", c.zone)
		}
		price += compatibleOfferings.Cheapest().Price
	}
	return price, nil
}
