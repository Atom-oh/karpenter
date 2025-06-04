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
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/awslabs/operatorpkg/option"
	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/karpenter/pkg/operator/options"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	scheduler "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	nodeutils "sigs.k8s.io/karpenter/pkg/utils/node"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// LaunchOptions는 스케줄링 중에 특정 작업과 구성을 트리거하는 데 사용할 수 있는 옵션 집합입니다.
// 이 구조체는 노드 생성 시 추가 동작을 구성하는 데 사용됩니다.
type LaunchOptions struct {
	// RecordPodNomination은 파드 지명 이벤트를 노드에 기록할지 여부를 나타냅니다.
	RecordPodNomination bool
	// Reason은 노드 생성 이유를 나타냅니다.
	Reason              string
}

// RecordPodNomination은 파드 지명 이벤트를 노드에 기록하도록 합니다.
// 이 함수는 LaunchOptions의 RecordPodNomination 필드를 true로 설정합니다.
func RecordPodNomination(o *LaunchOptions) {
	o.RecordPodNomination = true
}

// WithReason은 노드 생성 이유를 설정하는 함수를 반환합니다.
// 이 함수는 LaunchOptions의 Reason 필드를 설정하는 함수를 반환합니다.
func WithReason(reason string) func(*LaunchOptions) {
	return func(o *LaunchOptions) { o.Reason = reason }
}

// Provisioner는 대기열에 있는 파드를 기다리고, 배치하고, 용량을 생성하고, 파드를 용량에 바인딩합니다.
// 이 구조체는 Karpenter의 핵심 프로비저닝 로직을 구현합니다.
type Provisioner struct {
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider  cloudprovider.CloudProvider
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient     client.Client
	// batcher는 파드를 배치하는 데 사용됩니다.
	batcher        *Batcher[types.UID]
	// volumeTopology는 볼륨 토폴로지 제약 조건을 처리합니다.
	volumeTopology *scheduler.VolumeTopology
	// cluster는 클러스터 상태 정보를 제공합니다.
	cluster        *state.Cluster
	// recorder는 이벤트를 기록하는 데 사용됩니다.
	recorder       events.Recorder
	// cm은 변경 사항을 모니터링하는 데 사용됩니다.
	cm             *pretty.ChangeMonitor
	// clock은 시간 관련 작업에 사용됩니다.
	clock          clock.Clock
}

// NewProvisioner는 새로운 프로비저너를 생성합니다.
// 이 함수는 프로비저너를 초기화하고 필요한 의존성을 주입합니다.
func NewProvisioner(kubeClient client.Client, recorder events.Recorder,
	cloudProvider cloudprovider.CloudProvider, cluster *state.Cluster,
	clock clock.Clock,
) *Provisioner {
	p := &Provisioner{
		// 배처를 초기화합니다.
		batcher:        NewBatcher[types.UID](clock),
		// 클라우드 프로바이더를 설정합니다.
		cloudProvider:  cloudProvider,
		// Kubernetes 클라이언트를 설정합니다.
		kubeClient:     kubeClient,
		// 볼륨 토폴로지를 초기화합니다.
		volumeTopology: scheduler.NewVolumeTopology(kubeClient),
		// 클러스터 상태를 설정합니다.
		cluster:        cluster,
		// 이벤트 레코더를 설정합니다.
		recorder:       recorder,
		// 변경 모니터를 초기화합니다.
		cm:             pretty.NewChangeMonitor(),
		// 시계를 설정합니다.
		clock:          clock,
	}
	return p
}

// Trigger는 지정된 UID로 배처를 트리거합니다.
// 이 함수는 파드나 노드가 프로비저닝이 필요함을 알리는 데 사용됩니다.
func (p *Provisioner) Trigger(uid types.UID) {
	p.batcher.Trigger(uid)
}

// Register는 프로비저너를 매니저에 등록합니다.
// 이 함수는 프로비저너가 싱글톤 소스를 감시하도록 설정합니다.
func (p *Provisioner) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("provisioner").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(p))
}

// Reconcile은 프로비저너의 조정 로직을 구현합니다.
// 이 함수는 파드를 배치하고, 스케줄링하고, 필요한 노드를 생성합니다.
func (p *Provisioner) Reconcile(ctx context.Context) (result reconcile.Result, err error) {
	// 컨트롤러 이름을 컨텍스트에 주입합니다.
	ctx = injection.WithControllerName(ctx, "provisioner")

	// 파드를 배치합니다.
	if triggered := p.batcher.Wait(ctx); !triggered {
		return reconcile.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	}
	// 내부 클러스터 상태 메커니즘이 동기화되었는지 확인해야 합니다.
	// 그렇지 않으면 실제로 존재하는 것보다 클러스터 상태의 더 작은 노드 하위 집합을 기반으로 
	// 스케줄링 결정을 내릴 가능성이 있습니다.
	if !p.cluster.Synced(ctx) {
		return reconcile.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	}

	// 파드를 잠재적 노드에 스케줄링하고, 할 일이 없으면 종료합니다.
	results, err := p.Schedule(ctx)
	if err != nil {
		return reconcile.Result{}, err
	}
	if len(results.NewNodeClaims) == 0 {
		return reconcile.Result{RequeueAfter: singleton.RequeueImmediately}, nil
	}
	// 노드 클레임을 생성하고 파드 지명을 기록합니다.
	if _, err = p.CreateNodeClaims(ctx, results.NewNodeClaims, WithReason(metrics.ProvisionedReason), RecordPodNomination); err != nil {
		return reconcile.Result{}, err
	}
	return reconcile.Result{RequeueAfter: singleton.RequeueImmediately}, nil
}

// CreateNodeClaims launches nodes passed into the function in parallel. It returns a slice of the successfully created node
// names as well as a multierr of any errors that occurred while launching nodes
func (p *Provisioner) CreateNodeClaims(ctx context.Context, nodeClaims []*scheduler.NodeClaim, opts ...option.Function[LaunchOptions]) ([]string, error) {
	// Create capacity and bind pods
	errs := make([]error, len(nodeClaims))
	nodeClaimNames := make([]string, len(nodeClaims))
	workqueue.ParallelizeUntil(ctx, len(nodeClaims), len(nodeClaims), func(i int) {
		// create a new context to avoid a data race on the ctx variable
		if name, err := p.Create(ctx, nodeClaims[i], opts...); err != nil {
			errs[i] = fmt.Errorf("creating node claim, %w", err)
		} else {
			nodeClaimNames[i] = name
		}
	})
	return nodeClaimNames, multierr.Combine(errs...)
}

func (p *Provisioner) GetPendingPods(ctx context.Context) ([]*corev1.Pod, error) {
	// filter for provisionable pods first, so we don't check for validity/PVCs on pods we won't provision anyway
	// (e.g. those owned by daemonsets)
	pods, err := nodeutils.GetProvisionablePods(ctx, p.kubeClient)
	if err != nil {
		return nil, fmt.Errorf("listing pods, %w", err)
	}
	rejectedPods, pods := lo.FilterReject(pods, func(po *corev1.Pod, _ int) bool {
		if err := p.Validate(ctx, po); err != nil {
			// Mark in memory that this pod is unschedulable
			p.cluster.MarkPodSchedulingDecisions(ctx, map[*corev1.Pod]error{po: fmt.Errorf("ignoring pod, %w", err)}, nil, nil)
			log.FromContext(ctx).WithValues("Pod", klog.KObj(po)).V(1).Info(fmt.Sprintf("ignoring pod, %s", err))
			return true
		}
		return false
	})
	scheduler.IgnoredPodCount.Set(float64(len(rejectedPods)), nil)
	p.consolidationWarnings(ctx, pods)
	return pods, nil
}

// consolidationWarnings potentially writes logs warning about possible unexpected interactions
// between scheduling constraints and consolidation
// nolint:gocyclo
func (p *Provisioner) consolidationWarnings(ctx context.Context, pods []*corev1.Pod) {
	// We have pending pods that have preferred anti-affinity or topology spread constraints.  These can interact
	// unexpectedly with consolidation, so we warn once per hour when we see these pods.
	antiAffinityPods := lo.FilterMap(pods, func(po *corev1.Pod, _ int) (client.ObjectKey, bool) {
		if po.Spec.Affinity != nil && po.Spec.Affinity.PodAntiAffinity != nil {
			if len(po.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution) != 0 && options.FromContext(ctx).PreferencePolicy != options.PreferencePolicyIgnore {
				if p.cm.HasChanged(string(po.UID), "pod-antiaffinity") {
					return client.ObjectKeyFromObject(po), true
				}
			}
		}
		return client.ObjectKey{}, false
	})
	topologySpreadPods := lo.FilterMap(pods, func(po *corev1.Pod, _ int) (client.ObjectKey, bool) {
		for _, tsc := range po.Spec.TopologySpreadConstraints {
			if tsc.WhenUnsatisfiable == corev1.ScheduleAnyway && options.FromContext(ctx).PreferencePolicy != options.PreferencePolicyIgnore {
				if p.cm.HasChanged(string(po.UID), "pod-topology-spread") {
					return client.ObjectKeyFromObject(po), true
				}
			}
		}
		return client.ObjectKey{}, false
	})
	// We reduce the amount of logging that we do per-pod by grouping log lines like this together
	if len(antiAffinityPods) > 0 {
		log.FromContext(ctx).WithValues("pods", pretty.Slice(antiAffinityPods, 10)).Info("pod(s) have a preferred Anti-Affinity which can prevent consolidation")
	}
	if len(topologySpreadPods) > 0 {
		log.FromContext(ctx).WithValues("pods", pretty.Slice(topologySpreadPods, 10)).Info("pod(s) have a preferred TopologySpreadConstraint which can prevent consolidation")
	}
}

var ErrNodePoolsNotFound = errors.New("no nodepools found")

//nolint:gocyclo
func (p *Provisioner) NewScheduler(
	ctx context.Context,
	pods []*corev1.Pod,
	stateNodes []*state.StateNode,
	opts ...scheduler.Options,
) (*scheduler.Scheduler, error) {
	nodePools, err := nodepoolutils.ListManaged(ctx, p.kubeClient, p.cloudProvider)
	if err != nil {
		return nil, fmt.Errorf("listing nodepools, %w", err)
	}
	nodePools = lo.Filter(nodePools, func(np *v1.NodePool, _ int) bool {
		if !np.StatusConditions().IsTrue(status.ConditionReady) {
			log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "ignoring nodepool, not ready")
			return false
		}
		return np.DeletionTimestamp.IsZero()
	})
	if len(nodePools) == 0 {
		return nil, ErrNodePoolsNotFound
	}

	// nodeTemplates generated from NodePools are ordered by weight
	// since they are stored within a slice and scheduling
	// will always attempt to schedule on the first nodeTemplate
	nodepoolutils.OrderByWeight(nodePools)

	instanceTypes := map[string][]*cloudprovider.InstanceType{}
	for _, np := range nodePools {
		its, err := p.cloudProvider.GetInstanceTypes(ctx, np)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, fmt.Errorf("getting instance types, %w", err)
			}
			log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Error(err, "skipping, unable to resolve instance types")
			continue
		}
		if len(its) == 0 {
			log.FromContext(ctx).WithValues("NodePool", klog.KObj(np)).Info("skipping, no resolved instance types found")
			continue
		}
		instanceTypes[np.Name] = its
	}

	// inject topology constraints
	pods, err = p.injectVolumeTopologyRequirements(ctx, pods)
	if err != nil {
		return nil, fmt.Errorf("injecting volume topology requirements, %w", err)
	}

	// Calculate cluster topology, if a context error occurs, it is wrapped and returned
	topology, err := scheduler.NewTopology(ctx, p.kubeClient, p.cluster, stateNodes, nodePools, instanceTypes, pods, opts...)
	if err != nil {
		return nil, fmt.Errorf("tracking topology counts, %w", err)
	}
	daemonSetPods, err := p.getDaemonSetPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting daemon pods, %w", err)
	}
	return scheduler.NewScheduler(ctx, p.kubeClient, nodePools, p.cluster, stateNodes, topology, instanceTypes, daemonSetPods, p.recorder, p.clock, opts...), nil
}

func (p *Provisioner) Schedule(ctx context.Context) (scheduler.Results, error) {
	defer metrics.Measure(scheduler.DurationSeconds, map[string]string{scheduler.ControllerLabel: injection.GetControllerName(ctx)})()
	start := time.Now()

	// We collect the nodes with their used capacities before we get the list of pending pods. This ensures that
	// the node capacities we schedule against are always >= what the actual capacity is at any given instance. This
	// prevents over-provisioning at the cost of potentially under-provisioning which will self-heal during the next
	// scheduling loop when we launch a new node.  When this order is reversed, our node capacity may be reduced by pods
	// that have bound which we then provision new un-needed capacity for.
	// -------
	// We don't consider the nodes that are MarkedForDeletion since this capacity shouldn't be considered
	// as persistent capacity for the cluster (since it will soon be removed). Additionally, we are scheduling for
	// the pods that are on these nodes so the MarkedForDeletion node capacity can't be considered.
	nodes := p.cluster.Nodes()

	// Get pods, exit if nothing to do
	pendingPods, err := p.GetPendingPods(ctx)
	if err != nil {
		return scheduler.Results{}, err
	}

	// Get pods from nodes that are preparing for deletion
	// We do this after getting the pending pods so that we undershoot if pods are
	// actively migrating from a node that is being deleted
	// NOTE: The assumption is that these nodes are cordoned and no additional pods will schedule to them
	deletingNodePods, err := nodes.Deleting().CurrentlyReschedulablePods(ctx, p.kubeClient)
	if err != nil {
		return scheduler.Results{}, err
	}

	pods := append(pendingPods, deletingNodePods...)
	// nothing to schedule, so just return success
	if len(pods) == 0 {
		return scheduler.Results{}, nil
	}
	log.FromContext(ctx).V(1).WithValues("pending-pods", len(pendingPods), "deleting-pods", len(deletingNodePods)).Info("computing scheduling decision for provisionable pod(s)")

	opts := []scheduler.Options{scheduler.DisableReservedCapacityFallback, scheduler.NumConcurrentReconciles(int(math.Ceil(float64(options.FromContext(ctx).CPURequests) / 1000.0)))}
	if options.FromContext(ctx).PreferencePolicy == options.PreferencePolicyIgnore {
		opts = append(opts, scheduler.IgnorePreferences)
	}
	s, err := p.NewScheduler(
		ctx,
		pods,
		nodes.Active(),
		opts...,
	)
	if err != nil {
		if errors.Is(err, ErrNodePoolsNotFound) {
			log.FromContext(ctx).Info("no nodepools found")
			p.cluster.MarkPodSchedulingDecisions(ctx, lo.SliceToMap(pods, func(p *corev1.Pod) (*corev1.Pod, error) {
				return p, fmt.Errorf("no nodepools found")
			}), nil, nil)
			return scheduler.Results{}, nil
		}
		return scheduler.Results{}, fmt.Errorf("creating scheduler, %w", err)
	}

	// Timeout the Solve() method after 1m to ensure that we move faster through provisioning
	timeoutCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	results, err := s.Solve(timeoutCtx, pods)
	// context errors are ignored because we want to finish provisioning for what has already been scheduled
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return scheduler.Results{}, err
	}
	results = results.TruncateInstanceTypes(scheduler.MaxInstanceTypes)
	reservedOfferingErrors := results.ReservedOfferingErrors()
	if len(reservedOfferingErrors) != 0 {
		log.FromContext(ctx).V(1).WithValues(
			"Pods", pretty.Slice(lo.Map(lo.Keys(reservedOfferingErrors), func(p *corev1.Pod, _ int) string {
				return klog.KRef(p.Namespace, p.Name).String()
			}), 5),
		).Info("deferring scheduling decision for provisionable pod(s) to future simulation due to limited reserved offering capacity")
	}
	scheduler.UnschedulablePodsCount.Set(
		// A reserved offering error doesn't indicate a pod is unschedulable, just that the scheduling decision was deferred.
		float64(len(results.PodErrors)-len(reservedOfferingErrors)),
		map[string]string{
			scheduler.ControllerLabel: injection.GetControllerName(ctx),
		},
	)
	if len(results.NewNodeClaims) > 0 {
		log.FromContext(ctx).WithValues(
			"Pods", pretty.Slice(lo.Map(pods, func(p *corev1.Pod, _ int) string {
				return klog.KObj(p).String()
			}), 5),
			"duration", time.Since(start),
		).Info("found provisionable pod(s)")
	}
	// Mark in memory when these pods were marked as schedulable or when we made a decision on the pods
	p.cluster.MarkPodSchedulingDecisions(ctx, results.PodErrors, results.NodePoolToPodMapping(),
		// Only passing existing nodes here and not new nodeClaims because
		// these nodeClaims don't have a name until they are created
		results.ExistingNodeToPodMapping())
	results.Record(ctx, p.recorder, p.cluster)
	return results, nil
}

func (p *Provisioner) Create(ctx context.Context, n *scheduler.NodeClaim, opts ...option.Function[LaunchOptions]) (string, error) {
	ctx = log.IntoContext(ctx, log.FromContext(ctx).WithValues("NodePool", klog.KRef("", n.NodePoolName)))
	options := option.Resolve(opts...)
	latest := &v1.NodePool{}
	if err := p.kubeClient.Get(ctx, types.NamespacedName{Name: n.NodePoolName}, latest); err != nil {
		return "", fmt.Errorf("getting current resource usage, %w", err)
	}
	if err := latest.Spec.Limits.ExceededBy(p.cluster.NodePoolResourcesFor(n.NodePoolName)); err != nil {
		return "", err
	}
	nodeClaim := n.ToNodeClaim()

	if err := p.kubeClient.Create(ctx, nodeClaim); err != nil {
		return "", err
	}

	// Update pod to nodeClaim mapping for newly created nodeClaims. We do
	// this here because nodeClaim does not have a name until it is created.
	p.cluster.UpdatePodToNodeClaimMapping(map[string][]*corev1.Pod{nodeClaim.Name: n.Pods})

	instanceTypeRequirement, _ := lo.Find(nodeClaim.Spec.Requirements, func(req v1.NodeSelectorRequirementWithMinValues) bool {
		return req.Key == corev1.LabelInstanceTypeStable
	})

	log.FromContext(ctx).WithValues("NodeClaim", klog.KObj(nodeClaim), "requests", nodeClaim.Spec.Resources.Requests, "instance-types", instanceTypeList(instanceTypeRequirement.Values)).
		Info("created nodeclaim")
	metrics.NodeClaimsCreatedTotal.Inc(map[string]string{
		metrics.ReasonLabel:       options.Reason,
		metrics.NodePoolLabel:     nodeClaim.Labels[v1.NodePoolLabelKey],
		metrics.CapacityTypeLabel: nodeClaim.Labels[v1.CapacityTypeLabelKey],
	})
	// Update the nodeclaim manually in state to avoid eventual consistency delay races with our watcher.
	// This is essential to avoiding races where disruption can create a replacement node, then immediately
	// requeue. This can race with controller-runtime's internal cache as it watches events on the cluster
	// to then trigger cluster state updates. Triggering it manually ensures that Karpenter waits for the
	// internal cache to sync before moving onto another disruption loop.
	p.cluster.UpdateNodeClaim(nodeClaim)
	if option.Resolve(opts...).RecordPodNomination {
		for _, pod := range n.Pods {
			p.recorder.Publish(scheduler.NominatePodEvent(pod, nil, nodeClaim))
		}
	}
	return nodeClaim.Name, nil
}

func instanceTypeList(names []string) string {
	var itSb strings.Builder
	for i, name := range names {
		// print the first 5 instance types only (indices 0-4)
		if i > 4 {
			lo.Must(fmt.Fprintf(&itSb, " and %d other(s)", len(names)-i))
			break
		} else if i > 0 {
			lo.Must(fmt.Fprint(&itSb, ", "))
		}
		lo.Must(fmt.Fprint(&itSb, name))
	}
	return itSb.String()
}

func (p *Provisioner) getDaemonSetPods(ctx context.Context) ([]*corev1.Pod, error) {
	daemonSetList := &appsv1.DaemonSetList{}
	if err := p.kubeClient.List(ctx, daemonSetList); err != nil {
		return nil, fmt.Errorf("listing daemonsets, %w", err)
	}

	return lo.Map(daemonSetList.Items, func(d appsv1.DaemonSet, _ int) *corev1.Pod {
		pod := p.cluster.GetDaemonSetPod(&d)
		if pod == nil {
			pod = &corev1.Pod{Spec: d.Spec.Template.Spec}
		}
		// Replacing retrieved pod affinity with daemonset pod template required node affinity since this is overridden
		// by the daemonset controller during pod creation
		// https://github.com/kubernetes/kubernetes/blob/c5cf0ac1889f55ab51749798bec684aed876709d/pkg/controller/daemon/util/daemonset_util.go#L176
		if d.Spec.Template.Spec.Affinity != nil && d.Spec.Template.Spec.Affinity.NodeAffinity != nil && d.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			if pod.Spec.Affinity == nil {
				pod.Spec.Affinity = &corev1.Affinity{}
			}
			if pod.Spec.Affinity.NodeAffinity == nil {
				pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
			}
			pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = d.Spec.Template.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
		}
		return pod
	}), nil
}

func (p *Provisioner) Validate(ctx context.Context, pod *corev1.Pod) error {
	return multierr.Combine(
		validateKarpenterManagedLabelCanExist(pod),
		validateNodeSelector(ctx, pod),
		validateAffinity(ctx, pod),
		p.volumeTopology.ValidatePersistentVolumeClaims(ctx, pod),
	)
}

// validateKarpenterManagedLabelCanExist provides a more clear error message in the event of scheduling a pod that specifically doesn't
// want to run on a Karpenter node (e.g. a Karpenter controller replica).
func validateKarpenterManagedLabelCanExist(p *corev1.Pod) error {
	for _, req := range scheduling.NewPodRequirements(p) {
		if req.Key == v1.NodePoolLabelKey && req.Operator() == corev1.NodeSelectorOpDoesNotExist {
			return serrors.Wrap(fmt.Errorf("configured to not run on a Karpenter provisioned node"), "requirement", fmt.Sprintf("%s %s", v1.NodePoolLabelKey, corev1.NodeSelectorOpDoesNotExist))
		}
	}
	return nil
}

func (p *Provisioner) injectVolumeTopologyRequirements(ctx context.Context, pods []*corev1.Pod) ([]*corev1.Pod, error) {
	var schedulablePods []*corev1.Pod
	for _, pod := range pods {
		if err := p.volumeTopology.Inject(ctx, pod); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			log.FromContext(ctx).WithValues("Pod", klog.KObj(pod)).Error(err, "failed getting volume topology requirements")
		} else {
			schedulablePods = append(schedulablePods, pod)
		}
	}
	return schedulablePods, nil
}

func validateNodeSelector(ctx context.Context, p *corev1.Pod) (errs error) {
	terms := lo.MapToSlice(p.Spec.NodeSelector, func(k string, v string) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{
			MatchExpressions: []corev1.NodeSelectorRequirement{
				{
					Key:      k,
					Operator: corev1.NodeSelectorOpIn,
					Values:   []string{v},
				},
			},
		}
	})
	for _, term := range terms {
		errs = multierr.Append(errs, validateNodeSelectorTerm(ctx, term))
	}
	return errs
}

func validateAffinity(ctx context.Context, p *corev1.Pod) (errs error) {
	if p.Spec.Affinity == nil {
		return nil
	}
	if p.Spec.Affinity.NodeAffinity != nil {
		if p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			for _, term := range p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				errs = multierr.Append(errs, validateNodeSelectorTerm(ctx, term))
			}
		}
	}
	return errs
}

func validateNodeSelectorTerm(ctx context.Context, term corev1.NodeSelectorTerm) (errs error) {
	if term.MatchFields != nil {
		errs = multierr.Append(errs, fmt.Errorf("node selector term with matchFields is not supported"))
	}
	if term.MatchExpressions != nil {
		for _, requirement := range term.MatchExpressions {
			errs = multierr.Append(errs, v1.ValidateRequirement(ctx,
				v1.NodeSelectorRequirementWithMinValues{
					NodeSelectorRequirement: requirement,
				}))
		}
	}
	return errs
}
