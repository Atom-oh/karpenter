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

// drift.go 파일은 드리프트(drift) 감지 및 처리 로직을 구현합니다.
// 드리프트는 노드가 원래 프로비저닝된 사양에서 벗어난 상태를 의미합니다.
// 이 파일은 드리프트된 노드를 감지하고 제거하는 기능을 제공합니다.
package disruption

import (
	"context"
	"errors"
	"sort"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/karpenter/pkg/utils/pretty"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	disruptionevents "sigs.k8s.io/karpenter/pkg/controllers/disruption/events"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
)

// Drift는 드리프트된 후보 노드를 삭제하는 서브리컨실러입니다.
// 이 구조체는 노드가 원래 프로비저닝된 사양에서 벗어났을 때 해당 노드를 감지하고 제거합니다.
type Drift struct {
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient  client.Client
	// cluster는 클러스터 상태 정보를 제공합니다.
	cluster     *state.Cluster
	// provisioner는 노드 프로비저닝을 담당합니다.
	provisioner *provisioning.Provisioner
	// recorder는 이벤트를 기록하는 데 사용됩니다.
	recorder    events.Recorder
}

func NewDrift(kubeClient client.Client, cluster *state.Cluster, provisioner *provisioning.Provisioner, recorder events.Recorder) *Drift {
	return &Drift{
		kubeClient:  kubeClient,
		cluster:     cluster,
		provisioner: provisioner,
		recorder:    recorder,
	}
}

// ShouldDisrupt is a predicate used to filter candidates
func (d *Drift) ShouldDisrupt(ctx context.Context, c *Candidate) bool {
	return c.NodeClaim.StatusConditions().Get(string(d.Reason())).IsTrue()
}

// ComputeCommand generates a disruption command given candidates
func (d *Drift) ComputeCommand(ctx context.Context, disruptionBudgetMapping map[string]int, candidates ...*Candidate) (Command, scheduling.Results, error) {
	sort.Slice(candidates, func(i int, j int) bool {
		return candidates[i].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time.Before(
			candidates[j].NodeClaim.StatusConditions().Get(string(d.Reason())).LastTransitionTime.Time)
	})

	for _, candidate := range candidates {
		// Filter out empty candidates. If there was an empty node that wasn't consolidated before this, we should
		// assume that it was due to budgets. If we don't filter out budgets, users who set a budget for `empty`
		// can find their nodes disrupted here, which while that in itself isn't an issue for empty nodes, it could
		// constrain the `drift` budget.
		if len(candidate.reschedulablePods) == 0 {
			continue
		}
		// If the disruption budget doesn't allow this candidate to be disrupted,
		// continue to the next candidate. We don't need to decrement any budget
		// counter since drift commands can only have one candidate.
		if disruptionBudgetMapping[candidate.NodePool.Name] == 0 {
			continue
		}
		// Check if we need to create any NodeClaims.
		results, err := SimulateScheduling(ctx, d.kubeClient, d.cluster, d.provisioner, candidate)
		if err != nil {
			// if a candidate is now deleting, just retry
			if errors.Is(err, errCandidateDeleting) {
				continue
			}
			return Command{}, scheduling.Results{}, err
		}
		// Emit an event that we couldn't reschedule the pods on the node.
		if !results.AllNonPendingPodsScheduled() {
			d.recorder.Publish(disruptionevents.Blocked(candidate.Node, candidate.NodeClaim, pretty.Sentence(results.NonPendingPodSchedulingErrors()))...)
			continue
		}

		return Command{
			candidates:   []*Candidate{candidate},
			replacements: results.NewNodeClaims,
		}, results, nil
	}
	return Command{}, scheduling.Results{}, nil
}

func (d *Drift) Reason() v1.DisruptionReason {
	return v1.DisruptionReasonDrifted
}

func (d *Drift) Class() string {
	return EventualDisruptionClass
}

func (d *Drift) ConsolidationType() string {
	return ""
}
