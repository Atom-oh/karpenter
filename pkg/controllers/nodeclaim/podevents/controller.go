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

// podevents 패키지는 NodeClaim에 파드 이벤트 정보를 추가하는 컨트롤러를 구현합니다.
// 이 패키지는 파드의 스케줄링, 종료 등의 이벤트를 감지하고 관련 NodeClaim의
// lastPodEvent 상태를 업데이트하는 기능을 제공합니다.
// 이를 통해 통합(consolidation) 결정을 무효화하는 등의 작업을 수행할 수 있습니다.
package podevents

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
	podutils "sigs.k8s.io/karpenter/pkg/utils/pod"
)

// dedupeTimeout is 10 seconds to reduce the number of writes to the APIServer, since pod scheduling and deletion events are very frequent.
// The smaller this value is, the more writes Karpenter will make in a busy cluster. This timeout is intentionally smaller than the consolidation
// 15 second validation period, so that we can ensure that we invalidate consolidation commands that are decided while we're de-duping pod events.
const dedupeTimeout = 10 * time.Second

// Controller는 NodeClaim에 lastPodEvent 상태를 추가하는 컨트롤러입니다.
// 이 컨트롤러는 파드의 스케줄링, 종료 등의 이벤트를 감지하고 관련 NodeClaim의
// 상태를 업데이트하여 통합 결정을 무효화하는 등의 작업을 지원합니다.
type Controller struct {
	// clock은 시간 관련 작업에 사용됩니다.
	clock         clock.Clock
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient    client.Client
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider cloudprovider.CloudProvider
}

// NewController constructs a nodeclaim disruption controller
func NewController(clk clock.Clock, kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		clock:         clk,
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

//nolint:gocyclo
func (c *Controller) Reconcile(ctx context.Context, pod *corev1.Pod) (reconcile.Result, error) {
	// If the pod doesn't have a node name, we don't know which node this pod refers to.
	// or if this is a daemonset
	if pod.Spec.NodeName == "" || podutils.IsOwnedByDaemonSet(pod) {
		return reconcile.Result{}, nil
	}

	node := &corev1.Node{}
	if err := c.kubeClient.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(fmt.Errorf("getting node, %w", err))
	}
	// If there's no associated node claim, it's not a karpenter owned node.
	nc, err := nodeutils.NodeClaimForNode(ctx, c.kubeClient, node)
	if err != nil {
		// if the nodeclaim doesn't exist, or has duplicates, ignore.
		return reconcile.Result{}, nodeutils.IgnoreDuplicateNodeClaimError(nodeutils.IgnoreNodeClaimNotFoundError(fmt.Errorf("getting nodeclaims for node, %w", err)))
	}
	if !nodeclaimutils.IsManaged(nc, c.cloudProvider) {
		return reconcile.Result{}, nil
	}

	// If we've set the lastPodEvent before and it hasn't been before the timeout, don't do anything
	if !nc.Status.LastPodEventTime.Time.IsZero() && c.clock.Since(nc.Status.LastPodEventTime.Time) < dedupeTimeout {
		return reconcile.Result{}, nil
	}

	// otherwise, set the pod event time to now
	stored := nc.DeepCopy()
	nc.Status.LastPodEventTime.Time = c.clock.Now()
	if !equality.Semantic.DeepEqual(stored, nc) {
		if err = c.kubeClient.Status().Patch(ctx, nc, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Register(ctx context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.podevents").
		For(&corev1.Pod{}).
		WithEventFilter(predicate.TypedFuncs[client.Object]{
			// If a pod is bound to a node or goes terminal
			UpdateFunc: func(e event.TypedUpdateEvent[client.Object]) bool {
				oldPod := (e.ObjectOld).(*corev1.Pod)
				newPod := (e.ObjectNew).(*corev1.Pod)
				// if this is a newly bound pod
				bound := oldPod.Spec.NodeName == "" && newPod.Spec.NodeName != ""
				// if this is a newly terminal pod
				terminal := (newPod.Spec.NodeName != "" && !podutils.IsTerminal(oldPod) && podutils.IsTerminal(newPod))
				// if this is a newly terminating pod
				terminating := (newPod.Spec.NodeName != "" && !podutils.IsTerminating(oldPod) && podutils.IsTerminating(newPod))
				// return true if it was bound to a node, went terminal, or went terminating
				return bound || terminal || terminating
			},
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
