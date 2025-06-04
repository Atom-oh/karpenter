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

// hydration 패키지는 NodeClaim에 필요한 정보를 채우는 컨트롤러를 구현합니다.
// 이 패키지는 Karpenter의 새 버전에서 필요하지만 기존 NodeClaim에 존재하지 않을 수 있는
// 정보를 추가하는 기능을 제공합니다. 주로 레이블과 같은 메타데이터를 업데이트합니다.
package hydration

import (
	"context"

	"github.com/awslabs/operatorpkg/reasonable"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/controller-runtime/pkg/controller"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

// Controller는 Karpenter의 새 버전에서 필요하지만 기존 NodeClaim에 존재하지 않을 수 있는
// 정보를 NodeClaim에 채우는 컨트롤러입니다.
// 이 컨트롤러는 주로 NodeClass 레이블과 같은 메타데이터를 업데이트합니다.
type Controller struct {
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient    client.Client
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider cloudprovider.CloudProvider
}

func NewController(kubeClient client.Client, cloudProvider cloudprovider.CloudProvider) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		cloudProvider: cloudProvider,
	}
}

func (c *Controller) Reconcile(ctx context.Context, nc *v1.NodeClaim) (reconcile.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())
	if nc.Status.NodeName != "" {
		ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("Node", klog.KRef("", nc.Status.NodeName)))
	}

	if !nodeclaimutils.IsManaged(nc, c.cloudProvider) {
		return reconcile.Result{}, nil
	}

	stored := nc.DeepCopy()
	nc.Labels = lo.Assign(nc.Labels, map[string]string{
		v1.NodeClassLabelKey(nc.Spec.NodeClassRef.GroupKind()): nc.Spec.NodeClassRef.Name,
	})
	if !equality.Semantic.DeepEqual(stored, nc) {
		if err := c.kubeClient.Patch(ctx, nc, client.MergeFrom(stored)); err != nil {
			return reconcile.Result{}, client.IgnoreNotFound(err)
		}
	}
	return reconcile.Result{}, nil
}

func (c *Controller) Name() string {
	return "nodeclaim.hydration"
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		For(&v1.NodeClaim{}, builder.WithPredicates(nodeclaimutils.IsManagedPredicateFuncs(c.cloudProvider))).
		WithOptions(controller.Options{
			RateLimiter:             reasonable.RateLimiter(),
			MaxConcurrentReconciles: 1000,
		}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
