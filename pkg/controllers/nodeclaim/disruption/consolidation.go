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

// consolidation.go 파일은 NodeClaim의 통합 가능성을 평가하는 로직을 구현합니다.
// 이 파일은 NodePool의 consolidateAfter 설정에 따라 NodeClaim에 통합 가능 상태 조건을
// 추가하거나 제거하는 기능을 제공합니다. 이를 통해 중단 컨트롤러가 통합 대상 노드를
// 식별할 수 있습니다.
package disruption

import (
	"context"

	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Consolidation은 consolidateAfter 설정에 따라 빈 NodeClaim에 상태 조건을 추가하거나 제거하는 서브 컨트롤러입니다.
// 이 컨트롤러는 NodeClaim이 통합 가능한지 여부를 결정하고, 적절한 상태 조건을 설정합니다.
type Consolidation struct {
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient client.Client
	// clock은 시간 관련 작업에 사용됩니다.
	clock      clock.Clock
}

//nolint:gocyclo
func (c *Consolidation) Reconcile(ctx context.Context, nodePool *v1.NodePool, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	hasConsolidatableCondition := nodeClaim.StatusConditions().Get(v1.ConditionTypeConsolidatable) != nil

	// 1. If Consolidation isn't enabled, remove the consolidatable status condition
	if nodePool.Spec.Disruption.ConsolidateAfter.Duration == nil {
		if hasConsolidatableCondition {
			_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeConsolidatable)
			log.FromContext(ctx).V(1).Info("removing consolidatable status condition, consolidation is disabled")
		}
		return reconcile.Result{}, nil
	}
	initialized := nodeClaim.StatusConditions().Get(v1.ConditionTypeInitialized)
	// 2. If NodeClaim is not initialized, remove the consolidatable status condition
	if !initialized.IsTrue() {
		if hasConsolidatableCondition {
			_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeConsolidatable)
			log.FromContext(ctx).V(1).Info("removing consolidatable status condition, isn't initialized")
		}
		return reconcile.Result{}, nil
	}

	// If the lastPodEvent is zero, use the time that the nodeclaim was initialized, as that's when Karpenter recognizes that pods could have started scheduling
	timeToCheck := lo.Ternary(!nodeClaim.Status.LastPodEventTime.IsZero(), nodeClaim.Status.LastPodEventTime.Time, initialized.LastTransitionTime.Time)

	// Consider a node consolidatable by looking at the lastPodEvent status field on the nodeclaim.
	if c.clock.Since(timeToCheck) < lo.FromPtr(nodePool.Spec.Disruption.ConsolidateAfter.Duration) {
		if hasConsolidatableCondition {
			_ = nodeClaim.StatusConditions().Clear(v1.ConditionTypeConsolidatable)
			log.FromContext(ctx).V(1).Info("removing consolidatable status condition")
		}
		consolidatableTime := timeToCheck.Add(lo.FromPtr(nodePool.Spec.Disruption.ConsolidateAfter.Duration))
		return reconcile.Result{RequeueAfter: consolidatableTime.Sub(c.clock.Now())}, nil
	}

	// 6. Otherwise, add the consolidatable status condition
	nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsolidatable)
	if !hasConsolidatableCondition {
		log.FromContext(ctx).V(1).Info("marking consolidatable")
	}
	return reconcile.Result{}, nil
}
