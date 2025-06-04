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

// provisioning 패키지는 Karpenter의 노드 프로비저닝 로직을 구현합니다.
// 이 패키지는 스케줄링 불가능한 파드를 감지하고, 적절한 노드를 프로비저닝하여
// 클러스터의 용량을 동적으로 조정합니다.
package provisioning

import (
	"context"
	"time"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/utils/pod"
)

// PodController는 파드 리소스를 감시하는 컨트롤러입니다.
// 이 컨트롤러는 스케줄링 불가능한 파드를 감지하고 프로비저너를 트리거하여
// 적절한 노드를 생성합니다.
type PodController struct {
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient  client.Client
	// provisioner는 노드 프로비저닝을 담당합니다.
	provisioner *Provisioner
	// cluster는 클러스터 상태 정보를 제공합니다.
	cluster     *state.Cluster
}

// NewPodController는 새로운 파드 컨트롤러 인스턴스를 생성합니다.
// 이 함수는 파드 컨트롤러를 초기화하고 필요한 의존성을 주입합니다.
func NewPodController(kubeClient client.Client, provisioner *Provisioner, cluster *state.Cluster) *PodController {
	return &PodController{
		kubeClient:  kubeClient,
		provisioner: provisioner,
		cluster:     cluster,
	}
}

// Reconcile은 파드 리소스를 조정합니다.
// 이 함수는 프로비저닝 가능한 파드를 감지하고 프로비저너를 트리거하여
// 적절한 노드를 생성합니다.
func (c *PodController) Reconcile(ctx context.Context, p *corev1.Pod) (reconcile.Result, error) {
	// 컨트롤러 이름을 컨텍스트에 주입합니다.
	ctx = injection.WithControllerName(ctx, "provisioner.trigger.pod") //nolint:ineffassign,staticcheck

	// 파드가 프로비저닝 가능한지 확인합니다.
	if !pod.IsProvisionable(p) {
		return reconcile.Result{}, nil
	}
	// 프로비저너를 트리거하여 노드 생성을 시작합니다.
	c.provisioner.Trigger(p.UID)
	// 처음 관찰될 때 대기 중인 파드를 확인하여 Karpenter로 인해 대기하는 총 시간을 추적합니다.
	c.cluster.AckPods(p)
	// 파드가 더 이상 프로비저닝 가능하지 않을 때까지 계속 재큐합니다.
	// 노드가 온라인 상태가 되는 동안 새 파드가 생성되면 예상대로 스케줄링되지 않을 수 있습니다.
	// 프로비저닝 루프가 성공하더라도 파드가 스케줄 가능해지려면 다른 프로비저닝 루프가 필요할 수 있습니다.
	return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
}

// Register는 파드 컨트롤러를 매니저에 등록합니다.
// 이 함수는 컨트롤러가 파드 리소스를 감시하도록 설정하고,
// 최대 10개의 동시 조정을 허용합니다.
func (c *PodController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("provisioner.trigger.pod").
		For(&corev1.Pod{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}

// NodeController는 노드 리소스를 감시하는 컨트롤러입니다.
// 이 컨트롤러는 중단된 노드를 감지하고 프로비저너를 트리거하여
// 필요한 경우 새 노드를 생성합니다.
type NodeController struct {
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient  client.Client
	// provisioner는 노드 프로비저닝을 담당합니다.
	provisioner *Provisioner
}

// NewNodeController는 새로운 노드 컨트롤러 인스턴스를 생성합니다.
// 이 함수는 노드 컨트롤러를 초기화하고 필요한 의존성을 주입합니다.
func NewNodeController(kubeClient client.Client, provisioner *Provisioner) *NodeController {
	return &NodeController{
		kubeClient:  kubeClient,
		provisioner: provisioner,
	}
}

// Reconcile은 노드 리소스를 조정합니다.
// 이 함수는 중단된 노드를 감지하고 프로비저너를 트리거하여
// 필요한 경우 새 노드를 생성합니다.
func (c *NodeController) Reconcile(ctx context.Context, n *corev1.Node) (reconcile.Result, error) {
	// 컨트롤러 이름을 컨텍스트에 주입합니다.
	//nolint:ineffassign
	ctx = injection.WithControllerName(ctx, "provisioner.trigger.node") //nolint:ineffassign,staticcheck

	// 중단 테인트가 존재하지 않고 삭제 타임스탬프가 설정되지 않은 경우 중단되지 않습니다.
	// 여기서는 삭제 타임스탬프를 확인하지 않습니다. 종료 컨트롤러가 결국 노드가 삭제되는 것을 
	// 감지할 때 테인트를 설정할 것으로 예상하기 때문입니다.
	if !lo.ContainsBy(n.Spec.Taints, func(taint corev1.Taint) bool {
		return taint.MatchTaint(&v1.DisruptedNoScheduleTaint)
	}) {
		return reconcile.Result{}, nil
	}
	// 프로비저너를 트리거하여 노드 생성을 시작합니다.
	c.provisioner.Trigger(n.UID)
	// 노드가 더 이상 프로비저닝 가능하지 않을 때까지 계속 재큐합니다.
	// 노드가 온라인 상태가 되는 동안 새 파드가 생성되면 예상대로 스케줄링되지 않을 수 있습니다.
	// 프로비저닝 루프가 성공하더라도 파드가 스케줄 가능해지려면 다른 프로비저닝 루프가 필요할 수 있습니다.
	return reconcile.Result{RequeueAfter: 10 * time.Second}, nil
}

// Register는 노드 컨트롤러를 매니저에 등록합니다.
// 이 함수는 컨트롤러가 노드 리소스를 감시하도록 설정하고,
// 최대 10개의 동시 조정을 허용합니다.
func (c *NodeController) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("provisioner.trigger.node").
		For(&corev1.Node{}).
		WithOptions(controller.Options{MaxConcurrentReconciles: 10}).
		Complete(reconcile.AsReconciler(m.GetClient(), c))
}
