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

// expiration 패키지는 NodeClaim의 만료를 관리하는 컨트롤러를 구현합니다.
// 이 패키지는 expireAfter 설정에 따라 만료된 NodeClaim을 삭제하는 기능을 제공합니다.
// NodeClaim이 생성된 후 지정된 시간이 지나면 자동으로 삭제되어 클러스터 리소스를 최적화합니다.
package expiration

import (
	"context"
	"strings"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/operator/injection"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/metrics"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// Controller는 expireAfter 설정에 따라 만료된 NodeClaim을 삭제하는 컨트롤러입니다.
// 이 컨트롤러는 NodeClaim의 생성 시간과 expireAfter 설정을 비교하여 만료 여부를 결정합니다.
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

func (c *Controller) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if nodeClaim.Status.NodeName != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef("", nodeClaim.Status.NodeName)))
	}

	if !nodeclaimutils.IsManaged(nodeClaim, c.cloudProvider) {
		return reconcile.Result{}, nil
	}
	if !nodeClaim.DeletionTimestamp.IsZero() {
		return reconcile.Result{}, nil
	}
	// From here there are three scenarios to handle:
	// 1. If ExpireAfter is not configured, exit expiration loop
	if nodeClaim.Spec.ExpireAfter.Duration == nil {
		return reconcile.Result{}, nil
	}
	expirationTime := nodeClaim.CreationTimestamp.Add(*nodeClaim.Spec.ExpireAfter.Duration)
	// 2. If the NodeClaim isn't expired leave the reconcile loop.
	if c.clock.Now().Before(expirationTime) {
		// Use t.Sub(clock.Now()) instead of time.Until() to ensure we're using the injected clock.
		return reconcile.Result{RequeueAfter: expirationTime.Sub(c.clock.Now())}, nil
	}
	// 3. Otherwise, if the NodeClaim is expired we can forcefully expire the nodeclaim (by deleting it)
	if err := c.kubeClient.Delete(ctx, nodeClaim); err != nil {
		return reconcile.Result{}, client.IgnoreNotFound(err)
	}
	// 4. The deletion timestamp has successfully been set for the NodeClaim, update relevant metrics.
	log.FromContext(ctx).V(1).Info("deleting expired nodeclaim")
	metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       strings.ToLower(metrics.ExpiredReason),
		metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
	})
	// We sleep here after the delete operation since we want to ensure that we are able to read our own writes so that
	// we avoid duplicating metrics and log lines due to quick re-queues.
	// USE CAUTION when determining whether to increase this timeout or remove this line
	time.Sleep(time.Second)
	return reconcile.Result{}, nil
}

func (c *Controller) Name() string {
	return "nodeclaim.expiration"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("nodeclaim.expiration").
		For(&v1.NodeClaim{}, builder.WithPredicates(nodeclaimutils.IsManagedPredicateFuncs(c.cloudProvider))).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
