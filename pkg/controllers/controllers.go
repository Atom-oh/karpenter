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

// controllers 패키지는 Karpenter의 모든 컨트롤러를 관리합니다.
// 이 패키지는 노드 프로비저닝, 노드 종료, 노드 클레임 관리 등을 담당하는 
// 다양한 컨트롤러를 초기화하고 등록하는 기능을 제공합니다.
package controllers

import (
	"context"

	"github.com/awslabs/operatorpkg/controller"
	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/orchestration"
	metricsnode "sigs.k8s.io/karpenter/pkg/controllers/metrics/node"
	metricsnodepool "sigs.k8s.io/karpenter/pkg/controllers/metrics/nodepool"
	metricspod "sigs.k8s.io/karpenter/pkg/controllers/metrics/pod"
	"sigs.k8s.io/karpenter/pkg/controllers/node/health"
	nodehydration "sigs.k8s.io/karpenter/pkg/controllers/node/hydration"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination"
	"sigs.k8s.io/karpenter/pkg/controllers/node/termination/terminator"
	nodeclaimconsistency "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/consistency"
	nodeclaimdisruption "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/expiration"
	nodeclaimgarbagecollection "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/garbagecollection"
	nodeclaimhydration "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/hydration"
	nodeclaimlifecycle "sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/lifecycle"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeclaim/podevents"
	nodepoolcounter "sigs.k8s.io/karpenter/pkg/controllers/nodepool/counter"
	nodepoolhash "sigs.k8s.io/karpenter/pkg/controllers/nodepool/hash"
	nodepoolreadiness "sigs.k8s.io/karpenter/pkg/controllers/nodepool/readiness"
	nodepoolregistrationhealth "sigs.k8s.io/karpenter/pkg/controllers/nodepool/registrationhealth"
	nodepoolvalidation "sigs.k8s.io/karpenter/pkg/controllers/nodepool/validation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
)

// NewControllers는 Karpenter의 모든 컨트롤러를 초기화하고 반환합니다.
// 이 함수는 프로비저닝, 노드 종료, 노드 클레임 관리 등을 담당하는 다양한 컨트롤러를 생성합니다.
func NewControllers(
	ctx context.Context,
	mgr manager.Manager,
	clock clock.Clock,
	kubeClient client.Client,
	recorder events.Recorder,
	cloudProvider cloudprovider.CloudProvider,
	cluster *state.Cluster,
) []controller.Controller {
	// 프로비저너 초기화
	p := provisioning.NewProvisioner(kubeClient, recorder, cloudProvider, cluster, clock)
	// 퇴거 큐 초기화
	evictionQueue := terminator.NewQueue(kubeClient, recorder)
	// 중단 큐 초기화
	disruptionQueue := orchestration.NewQueue(kubeClient, recorder, cluster, clock, p)

	// 모든 컨트롤러 목록 생성
	controllers := []controller.Controller{
		p, evictionQueue, disruptionQueue,
		// 중단 컨트롤러
		disruption.NewController(clock, kubeClient, p, cloudProvider, recorder, cluster, disruptionQueue),
		// 프로비저닝 관련 컨트롤러
		provisioning.NewPodController(kubeClient, p, cluster),
		provisioning.NewNodeController(kubeClient, p),
		// 노드풀 관련 컨트롤러
		nodepoolhash.NewController(kubeClient, cloudProvider),
		// 만료 컨트롤러
		expiration.NewController(clock, kubeClient, cloudProvider),
		// 인포머 컨트롤러
		informer.NewDaemonSetController(kubeClient, cluster),
		informer.NewNodeController(kubeClient, cluster),
		informer.NewPodController(kubeClient, cluster),
		informer.NewNodePoolController(kubeClient, cloudProvider, cluster),
		informer.NewNodeClaimController(kubeClient, cloudProvider, cluster),
		// 종료 컨트롤러
		termination.NewController(clock, kubeClient, cloudProvider, terminator.NewTerminator(clock, kubeClient, evictionQueue, recorder), recorder),
		// 메트릭 컨트롤러
		metricspod.NewController(kubeClient, cluster),
		metricsnodepool.NewController(kubeClient, cloudProvider),
		metricsnode.NewController(cluster),
		// 노드풀 관련 컨트롤러
		nodepoolreadiness.NewController(kubeClient, cloudProvider),
		nodepoolregistrationhealth.NewController(kubeClient, cloudProvider),
		nodepoolcounter.NewController(kubeClient, cloudProvider, cluster),
		nodepoolvalidation.NewController(kubeClient, cloudProvider),
		// 파드 이벤트 컨트롤러
		podevents.NewController(clock, kubeClient, cloudProvider),
		// 노드 클레임 관련 컨트롤러
		nodeclaimconsistency.NewController(clock, kubeClient, cloudProvider, recorder),
		nodeclaimlifecycle.NewController(clock, kubeClient, cloudProvider, recorder),
		nodeclaimgarbagecollection.NewController(clock, kubeClient, cloudProvider),
		nodeclaimdisruption.NewController(clock, kubeClient, cloudProvider),
		nodeclaimhydration.NewController(kubeClient, cloudProvider),
		// 노드 하이드레이션 컨트롤러
		nodehydration.NewController(kubeClient, cloudProvider),
		// 상태 컨트롤러
		status.NewController[*v1.NodeClaim](kubeClient, mgr.GetEventRecorderFor("karpenter"), status.EmitDeprecatedMetrics, status.WithLabels(append(lo.Map(cloudProvider.GetSupportedNodeClasses(), func(obj status.Object, _ int) string { return v1.NodeClassLabelKey(object.GVK(obj).GroupKind()) }), v1.NodePoolLabelKey)...)),
		status.NewController[*v1.NodePool](kubeClient, mgr.GetEventRecorderFor("karpenter"), status.EmitDeprecatedMetrics),
		status.NewGenericObjectController[*corev1.Node](kubeClient, mgr.GetEventRecorderFor("karpenter"), status.WithLabels(append(lo.Map(cloudProvider.GetSupportedNodeClasses(), func(obj status.Object, _ int) string { return v1.NodeClassLabelKey(object.GVK(obj).GroupKind()) }), v1.NodePoolLabelKey, v1.NodeInitializedLabelKey)...)),
	}

	// 클라우드 프로바이더가 비정상 노드를 감지하기 위한 노드 복구 컨트롤러에 사용할 상태 조건을 정의해야 합니다.
	if len(cloudProvider.RepairPolicies()) != 0 && options.FromContext(ctx).FeatureGates.NodeRepair {
		controllers = append(controllers, health.NewController(kubeClient, cloudProvider, clock, recorder))
	}

	return controllers
}
