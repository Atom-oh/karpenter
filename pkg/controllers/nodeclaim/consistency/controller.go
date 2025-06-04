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

// consistency 패키지는 NodeClaim과 실제 노드 간의 일관성을 유지하는 컨트롤러를 구현합니다.
// 이 패키지는 NodeClaim과 해당 노드 간의 불일치를 감지하고 보고하는 기능을 제공합니다.
// 주요 기능으로는 노드 형태(shape) 검사, 레이블 일관성 확인 등이 있습니다.
package consistency

import (
	"context"
	stderrors "errors"
	"fmt"
	"time"

	"github.com/patrickmn/go-cache"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// Controller는 NodeClaim과 노드 간의 일관성을 확인하는 컨트롤러입니다.
// 이 컨트롤러는 정기적으로 NodeClaim과 해당 노드를 검사하여 불일치를 감지하고 보고합니다.
type Controller struct {
	// clock은 시간 관련 작업에 사용됩니다.
	clock         clock.Clock
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient    client.Client
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider cloudprovider.CloudProvider
	// checks는 수행할 일관성 검사 목록입니다.
	checks        []Check
	// recorder는 이벤트를 기록하는 데 사용됩니다.
	recorder      events.Recorder
	// lastScanned는 마지막 검사 시간을 추적하는 캐시입니다.
	lastScanned   *cache.Cache
}

type Issue string

type Check interface {
	// Check performs the consistency check, this should return a list of slice discovered, or an empty
	// slice if no issues were found
	Check(context.Context, *corev1.Node, *v1.NodeClaim) ([]Issue, error)
}

// scanPeriod is how often we inspect and report issues that are found.
const scanPeriod = 10 * time.Minute

func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider, recorder events.Recorder) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
		recorder:      recorder,
		lastScanned:   cache.New(scanPeriod, 1*time.Minute),
		checks: []Check{
			NewNodeShape(),
		},
	}
}

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, "nodeclaim.consistency")
	if nodeClaim.Status.NodeName != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef("", nodeClaim.Status.NodeName)))
	}

	if !nodeclaimutils.IsManaged(nodeClaim, c.cloudProvider) {
		return reconcile.Result{}, nil
	}
	if nodeClaim.Status.ProviderID == "" {
		return reconcile.Result{}, nil
	}

	stored := nodeClaim.DeepCopy()
	// If we get an event before we should check for consistency checks, we ignore and wait
	if lastTime, ok := c.lastScanned.Get(string(nodeClaim.UID)); ok {
		if lastTime, ok := lastTime.(time.Time); ok {
			remaining := scanPeriod - c.clock.Since(lastTime)
			return reconcile.Result{RequeueAfter: remaining}, nil
		}
		// the above should always succeed
		return reconcile.Result{RequeueAfter: scanPeriod}, nil
	}
	c.lastScanned.SetDefault(string(nodeClaim.UID), c.clock.Now())

	// We assume the invariant that there is a single node for a single nodeClaim. If this invariant is violated,
	// then we assume this is bubbled up through the nodeClaim lifecycle controller and don't perform consistency checks
	node, err := nodeclaimutils.NodeForNodeClaim(ctx, c.kubeClient, nodeClaim)
	if err != nil {
		return reconcile.Result{}, nodeclaimutils.IgnoreDuplicateNodeError(nodeclaimutils.IgnoreNodeNotFoundError(err))
	}
	if err = c.checkConsistency(ctx, nodeClaim, node); err != nil {
		return reconcile.Result{}, err
	}
	if !equality.Semantic.DeepEqual(stored, nodeClaim) {
		// We use client.MergeFromWithOptimisticLock because patching a list with a JSON merge patch
		// can cause races due to the fact that it fully replaces the list on a change
		// Here, we are updating the status condition list
		if err = c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFromWithOptions(stored, client.MergeFromWithOptimisticLock{})); client.IgnoreNotFound(err) != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, err
		}
	}
	return reconcile.Result{RequeueAfter: scanPeriod}, nil
}

func (c *Controller) checkConsistency(ctx context.Context, nodeClaim *v1.NodeClaim, node *corev1.Node) error {
	hasIssues := false
	for _, check := range c.checks {
		issues, err := check.Check(ctx, node, nodeClaim)
		if err != nil {
			return fmt.Errorf("checking node with %T, %w", check, err)
		}
		for _, issue := range issues {
			log.FromContext(ctx).Error(stderrors.New(string(issue)), "consistency error")
			c.recorder.Publish(FailedConsistencyCheckEvent(nodeClaim, string(issue)))
		}
		hasIssues = hasIssues || (len(issues) > 0)
	}
	// If status condition for consistent state is not true and no issues are found, set the status condition to true
	if !nodeClaim.StatusConditions().IsTrue(v1.ConditionTypeConsistentStateFound) && !hasIssues {
		nodeClaim.StatusConditions().SetTrue(v1.ConditionTypeConsistentStateFound)
	}
	// If there are issues then set the status condition for consistent state as false
	if hasIssues {
		nodeClaim.StatusConditions().SetFalse(v1.ConditionTypeConsistentStateFound, "ConsistencyCheckFailed", "Consistency Check Failed")
	}
	return nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.consistency").
		For(&v1.NodeClaim{}, builder.WithPredicates(nodeclaimutils.IsManagedPredicateFuncs(c.cloudProvider))).
		Watches(
			&corev1.Node{},
			nodeclaimutils.NodeEventHandler(c.kubeClient, c.cloudProvider),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
