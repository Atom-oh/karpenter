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

// singlenodeconsolidation.go 파일은 단일 노드 통합(single-node consolidation) 로직을 구현합니다.
// 단일 노드 통합은 하나의 저활용 노드를 더 효율적인 노드로 대체하는 기능을 제공합니다.
// 이 파일은 개별 노드를 평가하고 더 비용 효율적인 대안으로 대체하는 알고리즘을 구현합니다.
package disruption

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

var SingleNodeConsolidationTimeoutDuration = 3 * time.Minute

const SingleNodeConsolidationType = "single"

// SingleNodeConsolidation은 단일 노드 통합을 수행하는 통합 컨트롤러입니다.
// 이 구조체는 개별 노드를 평가하고 더 비용 효율적인 대안으로 대체하는 기능을 제공합니다.
type SingleNodeConsolidation struct {
	// consolidation은 기본 통합 기능을 상속받습니다.
	consolidation
	// PreviouslyUnseenNodePools는 이전에 보지 못한 노드풀을 추적합니다.
	PreviouslyUnseenNodePools sets.Set[string]
	// Validator는 노드 제거 전에 검증을 수행합니다.
	Validator
}

func NewSingleNodeConsolidation(c consolidation) *SingleNodeConsolidation {
	return &SingleNodeConsolidation{
		consolidation:             c,
		PreviouslyUnseenNodePools: sets.New[string](),
		Validator:                 NewSingleConsolidationValidator(c),
	}
}

// ComputeCommand generates a disruption command given candidates
// nolint:gocyclo
func (s *SingleNodeConsolidation) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (Command, scheduling.Results, error) {
	if s.IsConsolidated() {
		return Command{}, scheduling.Results{}, nil
	}
	candidates = s.SortCandidates(ctx, candidates)

	// Set a timeout
	timeout := s.clock.Now().Add(SingleNodeConsolidationTimeoutDuration)
	constrainedByBudgets := false

	unseenNodePools := sets.New(lo.Map(candidates, func(c *Candidate, _ int) string { return c.NodePool.Name })...)

	for i, candidate := range candidates {
		if s.clock.Now().After(timeout) {
			ConsolidationTimeoutsTotal.Inc(map[string]string{ConsolidationTypeLabel: s.ConsolidationType()})
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abandoning single-node consolidation due to timeout after evaluating %d candidates", i))

			s.PreviouslyUnseenNodePools = unseenNodePools

			return Command{}, scheduling.Results{}, nil
		}
		// Track that we've seen this nodepool
		unseenNodePools.Delete(candidate.NodePool.Name)

		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since single node consolidation commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			constrainedByBudgets = true
			continue
		}
		// Filter out empty candidates. If there was an empty node that wasn't consolidated before this, we should
		// assume that it was due to budgets. If we don't filter out budgets, users who set a budget for `empty`
		// can find their nodes disrupted here.
		if len(candidate.reschedulablePods) == 0 {
			continue
		}

		// compute a possible consolidation option
		cmd, results, err := s.computeConsolidation(ctx, candidate)
		if err != nil {
			log.FromContext(ctx).Error(err, "failed computing consolidation")
			continue
		}
		if cmd.Decision() == NoOpDecision {
			continue
		}
		if _, err = s.Validate(ctx, cmd, consolidationTTL); err != nil {
			if IsValidationError(err) {
				log.FromContext(ctx).V(1).WithValues(cmd.LogValues()...).Info("abandoning single-node consolidation attempt due to pod churn, command is no longer valid")
				return Command{}, scheduling.Results{}, nil
			}
			return Command{}, scheduling.Results{}, fmt.Errorf("validating consolidation, %w", err)
		}
		return cmd, results, nil
	}

	if !constrainedByBudgets {
		// if there are no candidates because of a budget, don't mark
		// as consolidated, as it's possible it should be consolidatable
		// the next time we try to disrupt.
		s.markConsolidated()
	}

	s.PreviouslyUnseenNodePools = unseenNodePools

	return Command{}, scheduling.Results{}, nil
}

func (s *SingleNodeConsolidation) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonUnderutilized
}

func (s *SingleNodeConsolidation) Class() string {
	return GracefulDisruptionClass
}

func (s *SingleNodeConsolidation) ConsolidationType() string {
	return SingleNodeConsolidationType
}

// sortCandidates interweaves candidates from different nodepools and prioritizes nodepools
// that timed out in previous runs
func (s *SingleNodeConsolidation) SortCandidates(ctx context.Context, candidates []*Candidate) []*Candidate {

	// First sort by disruption cost as the base ordering
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].DisruptionCost < candidates[j].DisruptionCost
	})

	return s.shuffleCandidates(ctx, lo.GroupBy(candidates, func(c *Candidate) string { return c.NodePool.Name }))
}

func (s *SingleNodeConsolidation) shuffleCandidates(ctx context.Context, nodePoolCandidates map[string][]*Candidate) []*Candidate {
	var result []*Candidate
	// Log any timed out nodepools that we're prioritizing
	if s.PreviouslyUnseenNodePools.Len() != 0 {
		log.FromContext(ctx).V(1).Info(fmt.Sprintf("prioritizing nodepools that have not yet been considered due to timeouts in previous runs: %s", strings.Join(s.PreviouslyUnseenNodePools.UnsortedList(), ", ")))
	}
	sortedNodePools := s.PreviouslyUnseenNodePools.UnsortedList()
	sortedNodePools = append(sortedNodePools, lo.Filter(lo.Keys(nodePoolCandidates), func(nodePoolName string, _ int) bool {
		return !s.PreviouslyUnseenNodePools.Has(nodePoolName)
	})...)

	// Find the maximum number of candidates in any nodepool
	maxCandidatesPerNodePool := lo.MaxBy(lo.Values(nodePoolCandidates), func(a, b []*Candidate) bool {
		return len(a) > len(b)
	})

	// Interweave candidates from different nodepools
	for i := range maxCandidatesPerNodePool {
		for _, nodePoolName := range sortedNodePools {
			if i < len(nodePoolCandidates[nodePoolName]) {
				result = append(result, nodePoolCandidates[nodePoolName][i])
			}
		}
	}

	return result
}
