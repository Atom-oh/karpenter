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

// disruption 패키지는 노드 중단 관리를 담당합니다.
// 이 패키지는 노드 통합(consolidation), 드리프트(drift) 감지, 빈 노드 제거(emptiness) 등의 기능을 제공하여
// 클러스터의 효율성과 비용을 최적화합니다.
// 
// 주요 중단 방법:
// - Emptiness: 빈 노드를 감지하고 제거합니다.
// - Drift: 프로비저닝 사양에서 드리프트된 노드를 감지하고 제거합니다.
// - SingleNodeConsolidation: 단일 노드를 더 효율적인 노드로 통합합니다.
// - MultiNodeConsolidation: 여러 노드를 하나의 더 효율적인 노드로 통합합니다.
package disruption

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/singleton"
	"github.com/samber/lo"
	"go.uber.org/multierr"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/clock"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption/orchestration"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	operatorlogging "sigs.k8s.io/karpenter/pkg/operator/logging"
	nodepoolutils "sigs.k8s.io/karpenter/pkg/utils/nodepool"
	"sigs.k8s.io/karpenter/pkg/utils/pretty"
)

// Controller는 노드 중단을 관리하는 컨트롤러입니다.
// 이 컨트롤러는 다양한 중단 방법(빈 노드 제거, 드리프트 감지, 노드 통합 등)을 
// 주기적으로 실행하여 클러스터의 효율성을 최적화합니다.
type Controller struct {
	// queue는 중단 작업을 관리하는 오케스트레이션 큐입니다.
	queue         *orchestration.Queue
	// kubeClient는 Kubernetes API와 통신하기 위한 클라이언트입니다.
	kubeClient    client.Client
	// cluster는 클러스터 상태 정보를 제공합니다.
	cluster       *state.Cluster
	// provisioner는 노드 프로비저닝을 담당합니다.
	provisioner   *provisioning.Provisioner
	// recorder는 이벤트를 기록하는 데 사용됩니다.
	recorder      events.Recorder
	// clock은 시간 관련 작업에 사용됩니다.
	clock         clock.Clock
	// cloudProvider는 클라우드 프로바이더와의 상호 작용을 담당합니다.
	cloudProvider cloudprovider.CloudProvider
	// methods는 사용 가능한 중단 방법 목록입니다.
	methods       []Method
	// mu는 동시성 제어를 위한 뮤텍스입니다.
	mu            sync.Mutex
	// lastRun은 각 중단 방법의 마지막 실행 시간을 추적합니다.
	lastRun       map[string]time.Time
}

// pollingPeriod는 클러스터를 검사하여 중단 기회를 찾는 주기입니다.
const pollingPeriod = 10 * time.Second

// NewController는 새로운 중단 컨트롤러를 생성합니다.
// 이 함수는 다양한 중단 방법을 초기화하고 컨트롤러를 구성합니다.
func NewController(clk clock.Clock, kubeClient client.Client, provisioner *provisioning.Provisioner,
	cp cloudprovider.CloudProvider, recorder events.Recorder, cluster *state.Cluster, queue *orchestration.Queue,
) *Controller {
	// 통합 기능을 위한 공통 컨텍스트를 생성합니다.
	c := MakeConsolidation(clk, cluster, kubeClient, provisioner, cp, recorder, queue)

	return &Controller{
		queue:         queue,
		clock:         clk,
		kubeClient:    kubeClient,
		cluster:       cluster,
		provisioner:   provisioner,
		recorder:      recorder,
		cloudProvider: cp,
		lastRun:       map[string]time.Time{},
		methods: []Method{
			// 빈 NodeClaim을 삭제합니다. 중단 비용이 전혀 없습니다.
			NewEmptiness(c),
			// 프로비저닝 사양에서 드리프트된 NodeClaim을 종료하여 파드가 다시 스케줄링되도록 합니다.
			NewDrift(kubeClient, cluster, provisioner, recorder),
			// 파드 변동을 줄이기 위해 동시에 통합할 수 있는 여러 NodeClaim을 식별합니다.
			NewMultiNodeConsolidation(c),
			// 마지막으로 클러스터 비용을 더 줄이기 위해 단일 NodeClaim 통합으로 대체합니다.
			NewSingleNodeConsolidation(c),
		},
	}
}

// Register는 컨트롤러를 매니저에 등록합니다.
// 이 함수는 컨트롤러가 싱글톤 소스를 감시하도록 설정합니다.
func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named("disruption").
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

// Reconcile는 중단 컨트롤러의 조정 로직을 구현합니다.
// 이 함수는 클러스터 상태를 검사하고 적절한 중단 방법을 실행합니다.
func (c *Controller) Reconcile(ctx context.Context) (reconcile.Result, error) {
	// 컨트롤러 이름을 컨텍스트에 주입합니다.
	ctx = injection.WithControllerName(ctx, "disruption")

	// 이것은 조정 루프가 영원히 멈추는 경우를 잡지는 못하지만, 다른 문제는 잡을 수 있습니다.
	c.logAbnormalRuns(ctx)
	defer c.logAbnormalRuns(ctx)
	c.recordRun("disruption-loop")

	// 검증에서 잡지 못한 잘못 구성된 예산이 있는지 로그를 남깁니다.
	// CEL 검증이 잘못된 중단 이유를 잡기 때문에 첫 번째 이유만 검증합니다.
	c.logInvalidBudgets(ctx)

	// 상태 노드에서 스케줄링 결정을 내리기 전에 내부 클러스터 상태 메커니즘이 동기화되었는지 확인해야 합니다.
	// 그렇지 않으면 실제로 존재하는 것보다 클러스터 상태의 더 작은 노드 하위 집합을 기반으로 
	// 스케줄링 결정을 내릴 가능성이 있습니다.
	if !c.cluster.Synced(ctx) {
		return reconcile.Result{RequeueAfter: time.Second}, nil
	}

	// Karpenter는 중단 프로세스의 일부로 메모리에서 진행되는 동안 karpenter.sh/disruption 테인트로 노드를 테인트합니다.
	// Karpenter가 중단 작업 중에 오류로 재시작하거나 실패하면 일부 노드가 테인트된 상태로 남을 수 있습니다.
	// 계속하기 전에 오케스트레이션 큐에 없는 후보에서 이 테인트를 멱등적으로 제거합니다.
	outdatedNodes := lo.Filter(c.cluster.Nodes(), func(s *state.StateNode, _ int) bool {
		return !c.queue.HasAny(s.ProviderID()) && !s.Deleted()
	})
	if err := state.RequireNoScheduleTaint(ctx, c.kubeClient, false, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, serrors.Wrap(fmt.Errorf("removing taint from nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}
	if err := state.ClearNodeClaimsCondition(ctx, c.kubeClient, v1.ConditionTypeDisruptionReason, outdatedNodes...); err != nil {
		if errors.IsConflict(err) {
			return reconcile.Result{Requeue: true}, nil
		}
		return reconcile.Result{}, serrors.Wrap(fmt.Errorf("removing condition from nodeclaims, %w", err), "condition", v1.ConditionTypeDisruptionReason)
	}

	// 다양한 중단 방법을 시도합니다. 하나의 방법만 작업을 수행하도록 합니다.
	for _, m := range c.methods {
		c.recordRun(fmt.Sprintf("%T", m))
		success, err := c.disrupt(ctx, m)
		if err != nil {
			if errors.IsConflict(err) {
				return reconcile.Result{Requeue: true}, nil
			}
			return reconcile.Result{}, serrors.Wrap(fmt.Errorf("disrupting, %w", err), strings.ToLower(string(m.Reason())), "reason")
		}
		if success {
			return reconcile.Result{RequeueAfter: singleton.RequeueImmediately}, nil
		}
	}

	// 모든 방법이 아무것도 하지 않았으므로 할 일이 없음을 반환합니다.
	return reconcile.Result{RequeueAfter: pollingPeriod}, nil
}

func (c *Controller) disrupt(ctx context.Context, disruption Method) (bool, error) {
	defer metrics.Measure(EvaluationDurationSeconds, map[string]string{
		metrics.ReasonLabel:    strings.ToLower(string(disruption.Reason())),
		ConsolidationTypeLabel: disruption.ConsolidationType(),
	})()
	candidates, err := GetCandidates(ctx, c.cluster, c.kubeClient, c.recorder, c.clock, c.cloudProvider, disruption.ShouldDisrupt, disruption.Class(), c.queue)
	if err != nil {
		return false, fmt.Errorf("determining candidates, %w", err)
	}
	EligibleNodes.Set(float64(len(candidates)), map[string]string{
		metrics.ReasonLabel: strings.ToLower(string(disruption.Reason())),
	})

	// If there are no candidates, move to the next disruption
	if len(candidates) == 0 {
		return false, nil
	}
	disruptionBudgetMapping, err := BuildDisruptionBudgetMapping(ctx, c.cluster, c.clock, c.kubeClient, c.cloudProvider, c.recorder, disruption.Reason())
	if err != nil {
		return false, fmt.Errorf("building disruption budgets, %w", err)
	}
	// Determine the disruption action
	cmd, schedulingResults, err := disruption.ComputeCommand(ctx, disruptionBudgetMapping, candidates...)
	if err != nil {
		return false, fmt.Errorf("computing disruption decision, %w", err)
	}
	if cmd.Decision() == NoOpDecision {
		return false, nil
	}

	// Attempt to disrupt
	if err := c.executeCommand(ctx, disruption, cmd, schedulingResults); err != nil {
		return false, fmt.Errorf("disrupting candidates, %w", err)
	}
	return true, nil
}

// executeCommand will do the following, untainting if the step fails.
// 1. Taint candidate nodes
// 2. Spin up replacement nodes
// 3. Add Command to orchestration.Queue to wait to delete the candiates.
func (c *Controller) executeCommand(ctx context.Context, m Method, cmd Command, schedulingResults scheduling.Results) error {
	commandID := uuid.NewUUID()
	log.FromContext(ctx).WithValues(append([]any{"command-id", string(commandID), "reason", strings.ToLower(string(m.Reason()))}, cmd.LogValues()...)...).Info("disrupting node(s)")

	// Cordon the old nodes before we launch the replacements to prevent new pods from scheduling to the old nodes
	if err := c.MarkDisrupted(ctx, m, cmd.candidates...); err != nil {
		return serrors.Wrap(fmt.Errorf("marking disrupted, %w", err), "command-id", commandID)
	}

	var nodeClaimNames []string
	var err error
	if len(cmd.replacements) > 0 {
		if nodeClaimNames, err = c.createReplacementNodeClaims(ctx, m, cmd); err != nil {
			// If we failed to launch the replacement, don't disrupt.  If this is some permanent failure,
			// we don't want to disrupt workloads with no way to provision new nodes for them.
			return serrors.Wrap(fmt.Errorf("launching replacement nodeclaim, %w", err), "command-id", commandID)
		}
	}

	// Nominate each node for scheduling and emit pod nomination events
	// We emit all nominations before we exit the disruption loop as
	// we want to ensure that nodes that are nominated are respected in the subsequent
	// disruption reconciliation. This is essential in correctly modeling multiple
	// disruption commands in parallel.
	// This will only nominate nodes for 2 * batchingWindow. Once the candidates are
	// tainted with the Karpenter taint, the provisioning controller will continue
	// to do scheduling simulations and nominate the pods on the candidate nodes until
	// the node is cleaned up.
	schedulingResults.Record(log.IntoContext(ctx, operatorlogging.NopLogger), c.recorder, c.cluster)

	stateNodes := lo.Map(cmd.candidates, func(c *Candidate, _ int) *state.StateNode { return c.StateNode })
	if err = c.queue.Add(orchestration.NewCommand(nodeClaimNames, stateNodes, commandID, m.Reason(), m.ConsolidationType())); err != nil {
		providerIDs := lo.Map(cmd.candidates, func(c *Candidate, _ int) string { return c.ProviderID() })
		c.cluster.UnmarkForDeletion(providerIDs...)
		return serrors.Wrap(fmt.Errorf("adding command to queue, %w", err), "command-id", commandID)
	}

	// An action is only performed and pods/nodes are only disrupted after a successful add to the queue
	DecisionsPerformedTotal.Inc(map[string]string{
		decisionLabel:          string(cmd.Decision()),
		metrics.ReasonLabel:    strings.ToLower(string(m.Reason())),
		ConsolidationTypeLabel: m.ConsolidationType(),
	})
	return nil
}

// createReplacementNodeClaims creates replacement NodeClaims
func (c *Controller) createReplacementNodeClaims(ctx context.Context, m Method, cmd Command) ([]string, error) {
	nodeClaimNames, err := c.provisioner.CreateNodeClaims(ctx, cmd.replacements, provisioning.WithReason(strings.ToLower(string(m.Reason()))))
	if err != nil {
		return nil, err
	}
	if len(nodeClaimNames) != len(cmd.replacements) {
		// shouldn't ever occur since a partially failed CreateNodeClaims should return an error
		return nil, serrors.Wrap(fmt.Errorf("expected replacement count did not equal actual replacement count"), "expected-count", len(cmd.replacements), "actual-count", len(nodeClaimNames))
	}
	return nodeClaimNames, nil
}

func (c *Controller) MarkDisrupted(ctx context.Context, m Method, candidates ...*Candidate) error {
	stateNodes := lo.Map(candidates, func(c *Candidate, _ int) *state.StateNode {
		return c.StateNode
	})
	if err := state.RequireNoScheduleTaint(ctx, c.kubeClient, true, stateNodes...); err != nil {
		return serrors.Wrap(fmt.Errorf("tainting nodes, %w", err), "taint", pretty.Taint(v1.DisruptedNoScheduleTaint))
	}

	providerIDs := lo.Map(candidates, func(c *Candidate, _ int) string { return c.ProviderID() })
	c.cluster.MarkForDeletion(providerIDs...)

	errs := make([]error, len(candidates))
	workqueue.ParallelizeUntil(ctx, len(candidates), len(candidates), func(i int) {
		// refresh nodeclaim before updating status
		nodeClaim := &v1.NodeClaim{}

		if err := c.kubeClient.Get(ctx, client.ObjectKeyFromObject(candidates[i].NodeClaim), nodeClaim); err != nil {
			errs[i] = client.IgnoreNotFound(err)
			return
		}
		stored := nodeClaim.DeepCopy()
		nodeClaim.StatusConditions().SetTrueWithReason(v1.ConditionTypeDisruptionReason, string(m.Reason()), string(m.Reason()))
		errs[i] = client.IgnoreNotFound(c.kubeClient.Status().Patch(ctx, nodeClaim, client.MergeFrom(stored)))
	})
	return multierr.Combine(errs...)
}

func (c *Controller) recordRun(s string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastRun[s] = c.clock.Now()
}

func (c *Controller) logAbnormalRuns(ctx context.Context) {
	const AbnormalTimeLimit = 15 * time.Minute
	c.mu.Lock()
	defer c.mu.Unlock()
	for name, runTime := range c.lastRun {
		if timeSince := c.clock.Since(runTime); timeSince > AbnormalTimeLimit {
			log.FromContext(ctx).V(1).Info(fmt.Sprintf("abnormal time between runs of %s = %s", name, timeSince))
		}
	}
}

// logInvalidBudgets will log if there are any invalid schedules detected
func (c *Controller) logInvalidBudgets(ctx context.Context) {
	nps, err := nodepoolutils.ListManaged(ctx, c.kubeClient, c.cloudProvider)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed listing nodepools")
		return
	}
	var buf bytes.Buffer
	for _, np := range nps {
		// Use a dummy value of 100 since we only care if this errors.
		for _, method := range c.methods {
			if _, err := np.GetAllowedDisruptionsByReason(c.clock, 100, method.Reason()); err != nil {
				fmt.Fprintf(&buf, "invalid disruption budgets in nodepool %s, %s", np.Name, err)
				break // Prevent duplicate error message
			}
		}
	}
	if buf.Len() > 0 {
		log.FromContext(ctx).Error(stderrors.New(buf.String()), "detected disruption budget errors")
	}
}
