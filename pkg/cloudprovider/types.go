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

// cloudprovider 패키지는 클라우드 프로바이더 인터페이스와 관련 타입을 정의합니다.
// 이 패키지는 다양한 클라우드 프로바이더(AWS, Azure, GCP 등)와의 통합을 위한 추상화 계층을 제공합니다.
package cloudprovider

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/awslabs/operatorpkg/serrors"
	"github.com/awslabs/operatorpkg/status"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/scheduling"
	"sigs.k8s.io/karpenter/pkg/utils/resources"
)

var (
	// SpotRequirement는 스팟 인스턴스 타입에 대한 요구사항을 정의합니다.
	SpotRequirement     = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeSpot))
	// OnDemandRequirement는 온디맨드 인스턴스 타입에 대한 요구사항을 정의합니다.
	OnDemandRequirement = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand))
	// ReservedRequirement는 예약된 인스턴스 타입에 대한 요구사항을 정의합니다.
	ReservedRequirement = scheduling.NewRequirements(scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeReserved))

	// ReservationIDLabel은 예약된 오퍼링의 요구사항에 주입되는 레이블로, 예약을 고유하게 식별하는 데 사용됩니다.
	// 예를 들어, 예약은 여러 NodePool에서 공유될 수 있으며, 이 요구사항에 인코딩된 값은 
	// 하나의 예약이 다른 예약에 영향을 미쳐야 함을 스케줄러에 알리는 데 사용됩니다.
	ReservationIDLabel string
)

// DriftReason은 노드가 드리프트된 이유를 나타내는 문자열 타입입니다.
type DriftReason string

// RepairPolicy는 노드 복구 정책을 정의하는 구조체입니다.
// 이 구조체는 노드가 비정상 상태일 때 어떻게 처리할지에 대한 정책을 정의합니다.
type RepairPolicy struct {
	// ConditionType은 노드에서 발견된 비정상 상태의 조건 유형입니다.
	ConditionType corev1.NodeConditionType
	// ConditionStatus는 노드가 비정상일 때의 조건 상태입니다.
	ConditionStatus corev1.ConditionStatus
	// TolerationDuration은 컨트롤러가 비정상 노드를 강제 종료하기 전에 대기하는 시간입니다.
	TolerationDuration time.Duration
}

// CloudProvider 인터페이스는 프로비저닝을 지원하기 위해 클라우드 프로바이더에 의해 구현됩니다.
// 이 인터페이스는 노드 생성, 삭제, 조회 등의 기능을 제공합니다.
type CloudProvider interface {
	// Create는 주어진 리소스 요청과 요구사항으로 NodeClaim을 시작하고 
	// 시작된 NodeClaim에 대한 해결된 NodeClaim 레이블이 포함된 hydrated NodeClaim을 반환합니다.
	Create(context.Context, *v1.NodeClaim) (*v1.NodeClaim, error)
	
	// Delete는 프로바이더 ID로 클라우드 프로바이더에서 NodeClaim을 제거합니다.
	// 클라우드 프로바이더 인스턴스가 이미 종료된 경우 NodeClaimNotFoundError를 반환하고,
	// 삭제가 트리거된 경우 nil을 반환해야 합니다.
	// Karpenter는 Delete가 NodeClaimNotFound 오류를 반환할 때까지 계속 재시도합니다.
	Delete(context.Context, *v1.NodeClaim) error
	
	// Get은 프로바이더 ID로 클라우드 프로바이더에서 NodeClaim을 검색합니다.
	Get(context.Context, string) (*v1.NodeClaim, error)
	
	// List는 클라우드 프로바이더에서 모든 NodeClaim을 검색합니다.
	List(context.Context) ([]*v1.NodeClaim, error)
	
	// GetInstanceTypes는 클라우드 프로바이더가 지원하는 인스턴스 유형을 반환합니다.
	// 유형이나 영역의 가용성은 노드풀이나 시간에 따라 달라질 수 있습니다.
	// 가용성에 관계없이 GetInstanceTypes 메서드는 항상 사용 가능한 오퍼링이 없는 
	// 인스턴스 유형을 포함한 모든 인스턴스 유형을 반환해야 합니다.
	GetInstanceTypes(context.Context, *v1.NodePool) ([]*InstanceType, error)
	
	// IsDrifted는 NodeClaim이 연결된 프로비저닝 요구사항에서 드리프트되었는지 여부를 반환합니다.
	IsDrifted(context.Context, *v1.NodeClaim) (DriftReason, error)
	
	// RepairPolicies는 클라우드 프로바이더가 Karpenter가 노드에서 모니터링할 
	// 비정상 조건 집합을 정의하기 위한 것입니다.
	RepairPolicies() []RepairPolicy
	
	// Name은 CloudProvider 구현 이름을 반환합니다.
	Name() string
	
	// GetSupportedNodeClasses는 status.Object를 구현하는 CloudProvider NodeClass를 반환합니다.
	// 참고: 첫 번째 요소가 기본 NodeClass여야 하는 목록을 반환합니다.
	GetSupportedNodeClasses() []status.Object
}

// InstanceType describes the properties of a potential node (either concrete attributes of an instance of this type
// or supported options in the case of arrays)
type InstanceType struct {
	// Name of the instance type, must correspond to corev1.LabelInstanceTypeStable
	Name string
	// Requirements returns a flexible set of properties that may be selected
	// for scheduling. Must be defined for every well known label, even if empty.
	Requirements scheduling.Requirements
	// Note that though this is an array it is expected that all the Offerings are unique from one another
	Offerings Offerings
	// Resources are the full resource capacities for this instance type
	Capacity corev1.ResourceList
	// Overhead is the amount of resource overhead expected to be used by kubelet and any other system daemons outside
	// of Kubernetes.
	Overhead *InstanceTypeOverhead

	once        sync.Once
	allocatable corev1.ResourceList
}

type InstanceTypes []*InstanceType

// precompute is used to ensure we only compute the allocatable resources onces as its called many times
// and the operation is fairly expensive.
func (i *InstanceType) precompute() {
	i.allocatable = resources.Subtract(i.Capacity, i.Overhead.Total())
}

func (i *InstanceType) Allocatable() corev1.ResourceList {
	i.once.Do(i.precompute)
	return i.allocatable
}

func (its InstanceTypes) OrderByPrice(reqs scheduling.Requirements) InstanceTypes {
	// Order instance types so that we get the cheapest instance types of the available offerings
	sort.Slice(its, func(i, j int) bool {
		iPrice := math.MaxFloat64
		jPrice := math.MaxFloat64

		for _, of := range its[i].Offerings {
			if of.Available && reqs.IsCompatible(of.Requirements, scheduling.AllowUndefinedWellKnownLabels) && of.Price < iPrice {
				iPrice = of.Price
			}
		}
		for _, of := range its[j].Offerings {
			if of.Available && reqs.IsCompatible(of.Requirements, scheduling.AllowUndefinedWellKnownLabels) && of.Price < jPrice {
				jPrice = of.Price
			}
		}
		return iPrice < jPrice
	})
	return its
}

// Compatible returns the list of instanceTypes based on the supported capacityType and zones in the requirements
func (its InstanceTypes) Compatible(requirements scheduling.Requirements) InstanceTypes {
	var filteredInstanceTypes []*InstanceType
	for _, instanceType := range its {
		if instanceType.Offerings.Available().HasCompatible(requirements) {
			filteredInstanceTypes = append(filteredInstanceTypes, instanceType)
		}
	}
	return filteredInstanceTypes
}

// SatisfiesMinValues validates whether the InstanceTypes satisfies the minValues requirements
// It returns the minimum number of needed instance types to satisfy the minValues requirement and an error
// that indicates whether the InstanceTypes satisfy the passed-in requirements
// This minNeededInstanceTypes value is dependent on the ordering of instance types, so relying on this value in a
// deterministic way implies that the instance types are sorted ahead of using this method
// For example:
// Requirements:
//   - key: node.kubernetes.io/instance-type
//     operator: In
//     values: ["c4.large","c4.xlarge","c5.large","c5.xlarge","m4.large","m4.xlarge"]
//     minValues: 3
//   - key: karpenter.kwok.sh/instance-family
//     operator: In
//     values: ["c4","c5","m4"]
//     minValues: 3
//
// InstanceTypes: ["c4.large","c5.xlarge","m4.2xlarge"], it PASSES the requirements
//
//		we get the map as : {
//			node.kubernetes.io/instance-type:  ["c4.large","c5.xlarge","m4.2xlarge"],
//			karpenter.k8s.aws/instance-family: ["c4","c5","m4"]
//		}
//	 so it returns 3 and a nil error to indicate a minimum of 3 instance types were required to fulfill the minValues requirements
//
// And if InstanceTypes: ["c4.large","c4.xlarge","c5.2xlarge"], it FAILS the requirements
//
//		we get the map as : {
//			node.kubernetes.io/instance-type:  ["c4.large","c4.xlarge","c5.2xlarge"],
//			karpenter.k8s.aws/instance-family: ["c4","c5"] // minimum requirement failed for this.
//		}
//	  so it returns 3 and a non-nil error to indicate that the instance types weren't able to fulfill the minValues requirements
func (its InstanceTypes) SatisfiesMinValues(requirements scheduling.Requirements) (minNeededInstanceTypes int, err error) {
	if !requirements.HasMinValues() {
		return 0, nil
	}
	valuesForKey := map[string]sets.Set[string]{}
	// We validate if sorting by price and truncating the number of instance types to minItems breaks the minValue requirement.
	// If minValue requirement fails, we return an error that indicates the first requirement key that couldn't be satisfied.
	var incompatibleKey string
	for i, it := range its {
		for _, req := range requirements {
			if req.MinValues != nil {
				if _, ok := valuesForKey[req.Key]; !ok {
					valuesForKey[req.Key] = sets.New[string]()
				}
				valuesForKey[req.Key] = valuesForKey[req.Key].Insert(it.Requirements.Get(req.Key).Values()...)
			}
		}
		incompatibleKey = func() string {
			for k, v := range valuesForKey {
				// Break if any of the MinValues of requirement is not honored
				if len(v) < lo.FromPtr(requirements.Get(k).MinValues) {
					return k
				}
			}
			return ""
		}()
		if incompatibleKey == "" {
			return i + 1, nil
		}
	}
	if incompatibleKey != "" {
		return len(its), serrors.Wrap(fmt.Errorf("minValues requirement is not met for label"), "label", incompatibleKey)
	}
	return len(its), nil
}

// Truncate truncates the InstanceTypes based on the passed-in requirements
// It returns an error if it isn't possible to truncate the instance types on maxItems without violating minValues
func (its InstanceTypes) Truncate(requirements scheduling.Requirements, maxItems int) (InstanceTypes, error) {
	truncatedInstanceTypes := lo.Slice(its.OrderByPrice(requirements), 0, maxItems)
	// Only check for a validity of NodeClaim if its requirement has minValues in it.
	if requirements.HasMinValues() {
		if _, err := truncatedInstanceTypes.SatisfiesMinValues(requirements); err != nil {
			return its, fmt.Errorf("validating minValues, %w", err)
		}
	}
	return truncatedInstanceTypes, nil
}

type InstanceTypeOverhead struct {
	// KubeReserved returns the default resources allocated to kubernetes system daemons by default
	KubeReserved corev1.ResourceList
	// SystemReserved returns the default resources allocated to the OS system daemons by default
	SystemReserved corev1.ResourceList
	// EvictionThreshold returns the resources used to maintain a hard eviction threshold
	EvictionThreshold corev1.ResourceList
}

func (i InstanceTypeOverhead) Total() corev1.ResourceList {
	return resources.Merge(i.KubeReserved, i.SystemReserved, i.EvictionThreshold)
}

// An Offering describes where an InstanceType is available to be used, with the expectation that its properties
// may be tightly coupled (e.g. the availability of an instance type in some zone is scoped to a capacity type) and
// these properties are captured with labels in Requirements.
// Requirements are required to contain the keys v1.CapacityTypeLabelKey and corev1.LabelTopologyZone.
type Offering struct {
	Requirements        scheduling.Requirements
	Price               float64
	Available           bool
	ReservationCapacity int
}

func (o *Offering) CapacityType() string {
	return o.Requirements.Get(v1.CapacityTypeLabelKey).Any()
}

func (o *Offering) Zone() string {
	return o.Requirements.Get(corev1.LabelTopologyZone).Any()
}

func (o *Offering) ReservationID() string {
	return o.Requirements.Get(ReservationIDLabel).Any()
}

type Offerings []*Offering

// Available filters the available offerings from the returned offerings
func (ofs Offerings) Available() Offerings {
	return lo.Filter(ofs, func(o *Offering, _ int) bool {
		return o.Available
	})
}

// Compatible returns the offerings based on the passed requirements
func (ofs Offerings) Compatible(reqs scheduling.Requirements) Offerings {
	return lo.Filter(ofs, func(offering *Offering, _ int) bool {
		return reqs.IsCompatible(offering.Requirements, scheduling.AllowUndefinedWellKnownLabels)
	})
}

// HasCompatible returns whether there is a compatible offering based on the passed requirements
func (ofs Offerings) HasCompatible(reqs scheduling.Requirements) bool {
	for _, of := range ofs {
		if reqs.IsCompatible(of.Requirements, scheduling.AllowUndefinedWellKnownLabels) {
			return true
		}
	}
	return false
}

// Cheapest returns the cheapest offering from the returned offerings
func (ofs Offerings) Cheapest() *Offering {
	return lo.MinBy(ofs, func(a, b *Offering) bool {
		return a.Price < b.Price
	})
}

// MostExpensive returns the most expensive offering from the return offerings
func (ofs Offerings) MostExpensive() *Offering {
	return lo.MaxBy(ofs, func(a, b *Offering) bool {
		return a.Price > b.Price
	})
}

// WorstLaunchPrice gets the worst-case launch price from the offerings that are offered on an instance type. Only
// offerings for the capacity type we will launch with are considered. The following precedence order is used to
// determine which capacity type is used: reserved, spot, on-demand.
func (ofs Offerings) WorstLaunchPrice(reqs scheduling.Requirements) float64 {
	for _, ctReqs := range []scheduling.Requirements{
		ReservedRequirement,
		SpotRequirement,
		OnDemandRequirement,
	} {
		if compatOfs := ofs.Compatible(reqs).Compatible(ctReqs); len(compatOfs) != 0 {
			return compatOfs.MostExpensive().Price
		}
	}
	return math.MaxFloat64
}

// NodeClaimNotFoundError is an error type returned by CloudProviders when the reason for failure is NotFound
type NodeClaimNotFoundError struct {
	error
}

func NewNodeClaimNotFoundError(err error) *NodeClaimNotFoundError {
	return &NodeClaimNotFoundError{
		error: err,
	}
}

func (e *NodeClaimNotFoundError) Error() string {
	return fmt.Sprintf("nodeclaim not found, %s", e.error)
}

func (e *NodeClaimNotFoundError) Unwrap() error {
	return e.error
}

func IsNodeClaimNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	var ncnfErr *NodeClaimNotFoundError
	return errors.As(err, &ncnfErr)
}

func IgnoreNodeClaimNotFoundError(err error) error {
	if IsNodeClaimNotFoundError(err) {
		return nil
	}
	return err
}

// InsufficientCapacityError is an error type returned by CloudProviders when a launch fails due to a lack of capacity from NodeClaim requirements
type InsufficientCapacityError struct {
	error
}

func NewInsufficientCapacityError(err error) *InsufficientCapacityError {
	return &InsufficientCapacityError{
		error: err,
	}
}

func (e *InsufficientCapacityError) Error() string {
	return fmt.Sprintf("insufficient capacity, %s", e.error)
}

func (e *InsufficientCapacityError) Unwrap() error {
	return e.error
}

func IsInsufficientCapacityError(err error) bool {
	if err == nil {
		return false
	}
	var icErr *InsufficientCapacityError
	return errors.As(err, &icErr)
}

// NodeClassNotReadyError is an error type returned by CloudProviders when a NodeClass that is used by the launch process doesn't have all its resolved fields
type NodeClassNotReadyError struct {
	error
}

func NewNodeClassNotReadyError(err error) *NodeClassNotReadyError {
	return &NodeClassNotReadyError{
		error: err,
	}
}

func (e *NodeClassNotReadyError) Error() string {
	return fmt.Sprintf("NodeClassRef not ready, %s", e.error)
}

func (e *NodeClassNotReadyError) Unwrap() error {
	return e.error
}

func IsNodeClassNotReadyError(err error) bool {
	if err == nil {
		return false
	}
	var nrError *NodeClassNotReadyError
	return errors.As(err, &nrError)
}

// CreateError is an error type returned by CloudProviders when instance creation fails
type CreateError struct {
	error
	ConditionReason  string
	ConditionMessage string
}

func NewCreateError(err error, reason, message string) *CreateError {
	return &CreateError{
		error:            err,
		ConditionReason:  reason,
		ConditionMessage: message,
	}
}

func (e *CreateError) Error() string {
	return fmt.Sprintf("creating nodeclaim, %s", e.error)
}

func (e *CreateError) Unwrap() error {
	return e.error
}
