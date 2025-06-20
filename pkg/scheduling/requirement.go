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

// scheduling 패키지는 Karpenter의 스케줄링 로직을 구현합니다.
// 이 패키지는 노드 선택 요구사항, 리소스 요구사항 및 스케줄링 제약 조건을 처리합니다.
package scheduling

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

// Requirement는 corev1.NodeSelectorRequirement의 효율적인 표현입니다.
// 이 구조체는 노드 선택 요구사항을 효율적으로 표현하고 처리하기 위한 내부 구조를 제공합니다.
type Requirement struct {
	// Key는 요구사항의 키입니다.
	Key         string
	// complement는 요구사항이 보완적인지 여부를 나타냅니다.
	complement  bool
	// values는 요구사항의 값 집합입니다.
	values      sets.Set[string]
	// greaterThan은 값이 이 값보다 커야 함을 나타냅니다.
	greaterThan *int
	// lessThan은 값이 이 값보다 작아야 함을 나타냅니다.
	lessThan    *int
	// MinValues는 요구사항을 충족하기 위한 최소 값 수를 나타냅니다.
	MinValues   *int
}

// NewRequirementWithFlexibility constructs new requirement from the combination of key, values, minValues and the operator that
// connects the keys and values.
func NewRequirementWithFlexibility(key string, operator corev1.NodeSelectorOperator, minValues *int, values ...string) *Requirement {
	if normalized, ok := v1.NormalizedLabels[key]; ok {
		key = normalized
	}

	// This is a super-common case, so optimize for it an inline everything.
	if operator == corev1.NodeSelectorOpIn {
		s := make(sets.Set[string], len(values))
		for _, value := range values {
			s[value] = sets.Empty{}
		}
		return &Requirement{
			Key:        key,
			values:     s,
			complement: false,
			MinValues:  minValues,
		}
	}

	r := &Requirement{
		Key:        key,
		values:     sets.New[string](),
		complement: true,
		MinValues:  minValues,
	}
	if operator == corev1.NodeSelectorOpIn || operator == corev1.NodeSelectorOpDoesNotExist {
		r.complement = false
	}
	if operator == corev1.NodeSelectorOpIn || operator == corev1.NodeSelectorOpNotIn {
		r.values.Insert(values...)
	}
	if operator == corev1.NodeSelectorOpGt {
		value, _ := strconv.Atoi(values[0]) // prevalidated
		r.greaterThan = &value
	}
	if operator == corev1.NodeSelectorOpLt {
		value, _ := strconv.Atoi(values[0]) // prevalidated
		r.lessThan = &value
	}
	return r
}

func NewRequirement(key string, operator corev1.NodeSelectorOperator, values ...string) *Requirement {
	return NewRequirementWithFlexibility(key, operator, nil, values...)
}

func (r *Requirement) NodeSelectorRequirement() v1.NodeSelectorRequirementWithMinValues {
	switch {
	case r.greaterThan != nil:
		return v1.NodeSelectorRequirementWithMinValues{
			NodeSelectorRequirement: corev1.NodeSelectorRequirement{
				Key:      r.Key,
				Operator: corev1.NodeSelectorOpGt,
				Values:   []string{strconv.FormatInt(int64(lo.FromPtr(r.greaterThan)), 10)},
			},
			MinValues: r.MinValues,
		}
	case r.lessThan != nil:
		return v1.NodeSelectorRequirementWithMinValues{
			NodeSelectorRequirement: corev1.NodeSelectorRequirement{
				Key:      r.Key,
				Operator: corev1.NodeSelectorOpLt,
				Values:   []string{strconv.FormatInt(int64(lo.FromPtr(r.lessThan)), 10)},
			},
			MinValues: r.MinValues,
		}
	case r.complement:
		switch {
		case len(r.values) > 0:
			return v1.NodeSelectorRequirementWithMinValues{
				NodeSelectorRequirement: corev1.NodeSelectorRequirement{
					Key:      r.Key,
					Operator: corev1.NodeSelectorOpNotIn,
					Values:   sets.List(r.values),
				},
				MinValues: r.MinValues,
			}
		default:
			return v1.NodeSelectorRequirementWithMinValues{
				NodeSelectorRequirement: corev1.NodeSelectorRequirement{
					Key:      r.Key,
					Operator: corev1.NodeSelectorOpExists,
				},
				MinValues: r.MinValues,
			}
		}
	default:
		switch {
		case len(r.values) > 0:
			return v1.NodeSelectorRequirementWithMinValues{
				NodeSelectorRequirement: corev1.NodeSelectorRequirement{
					Key:      r.Key,
					Operator: corev1.NodeSelectorOpIn,
					Values:   sets.List(r.values),
				},
				MinValues: r.MinValues,
			}
		default:
			return v1.NodeSelectorRequirementWithMinValues{
				NodeSelectorRequirement: corev1.NodeSelectorRequirement{
					Key:      r.Key,
					Operator: corev1.NodeSelectorOpDoesNotExist,
				},
				MinValues: r.MinValues,
			}
		}
	}
}

// Intersection constraints the Requirement from the incoming requirements
// nolint:gocyclo
func (r *Requirement) Intersection(requirement *Requirement) *Requirement {
	// Complement
	complement := r.complement && requirement.complement

	// Boundaries
	greaterThan := maxIntPtr(r.greaterThan, requirement.greaterThan)
	lessThan := minIntPtr(r.lessThan, requirement.lessThan)
	minValues := maxIntPtr(r.MinValues, requirement.MinValues)
	if greaterThan != nil && lessThan != nil && *greaterThan >= *lessThan {
		return NewRequirementWithFlexibility(r.Key, corev1.NodeSelectorOpDoesNotExist, minValues)
	}

	// Values
	var values sets.Set[string]
	if r.complement && requirement.complement {
		values = r.values.Union(requirement.values)
	} else if r.complement && !requirement.complement {
		values = requirement.values.Difference(r.values)
	} else if !r.complement && requirement.complement {
		values = r.values.Difference(requirement.values)
	} else {
		values = r.values.Intersection(requirement.values)
	}
	for value := range values {
		if !withinIntPtrs(value, greaterThan, lessThan) {
			values.Delete(value)
		}
	}
	// Remove boundaries for concrete sets
	if !complement {
		greaterThan, lessThan = nil, nil
	}
	return &Requirement{Key: r.Key, values: values, complement: complement, greaterThan: greaterThan, lessThan: lessThan, MinValues: minValues}
}

// nolint:gocyclo
// HasIntersection is a more efficient implementation of Intersection
// It validates whether there is an intersection between the two requirements without actually creating the sets
// This prevents the garbage collector from having to spend cycles cleaning up all of these created set objects
func (r *Requirement) HasIntersection(requirement *Requirement) bool {
	greaterThan := maxIntPtr(r.greaterThan, requirement.greaterThan)
	lessThan := minIntPtr(r.lessThan, requirement.lessThan)
	if greaterThan != nil && lessThan != nil && *greaterThan >= *lessThan {
		return false
	}
	// Both requirements have a complement
	if r.complement && requirement.complement {
		return true
	}
	// Only one requirement has a complement
	if r.complement && !requirement.complement {
		for value := range requirement.values {
			if !r.values.Has(value) && withinIntPtrs(value, greaterThan, lessThan) {
				return true
			}
		}
		return false
	}
	if !r.complement && requirement.complement {
		for value := range r.values {
			if !requirement.values.Has(value) && withinIntPtrs(value, greaterThan, lessThan) {
				return true
			}
		}
		return false
	}
	// Both requirements are non-complement requirements
	for value := range r.values {
		if requirement.values.Has(value) && withinIntPtrs(value, greaterThan, lessThan) {
			return true
		}
	}
	return false
}

func (r *Requirement) Any() string {
	switch r.Operator() {
	case corev1.NodeSelectorOpIn:
		return r.values.UnsortedList()[0]
	case corev1.NodeSelectorOpNotIn, corev1.NodeSelectorOpExists:
		min := 0
		max := math.MaxInt64
		if r.greaterThan != nil {
			min = *r.greaterThan + 1
		}
		if r.lessThan != nil {
			max = *r.lessThan
		}
		return fmt.Sprint(rand.Intn(max-min) + min) //nolint:gosec
	}
	return ""
}

// Has returns true if the requirement allows the value
func (r *Requirement) Has(value string) bool {
	if r.complement {
		return !r.values.Has(value) && withinIntPtrs(value, r.greaterThan, r.lessThan)
	}
	return r.values.Has(value) && withinIntPtrs(value, r.greaterThan, r.lessThan)
}

func (r *Requirement) Values() []string {
	return r.values.UnsortedList()
}

func (r *Requirement) Insert(items ...string) {
	r.values.Insert(items...)
}

func (r *Requirement) Operator() corev1.NodeSelectorOperator {
	if r.complement {
		if r.Len() < math.MaxInt64 {
			return corev1.NodeSelectorOpNotIn
		}
		return corev1.NodeSelectorOpExists // corev1.NodeSelectorOpGt and corev1.NodeSelectorOpLt are treated as "Exists" with bounds
	}
	if r.Len() > 0 {
		return corev1.NodeSelectorOpIn
	}
	return corev1.NodeSelectorOpDoesNotExist
}

func (r *Requirement) Len() int {
	if r.complement {
		return math.MaxInt64 - r.values.Len()
	}
	return r.values.Len()
}

func (r *Requirement) String() string {
	var s string
	switch r.Operator() {
	case corev1.NodeSelectorOpExists, corev1.NodeSelectorOpDoesNotExist:
		s = fmt.Sprintf("%s %s", r.Key, r.Operator())
	default:
		values := sets.List(r.values)
		if length := len(values); length > 5 {
			values = append(values[:5], fmt.Sprintf("and %d others", length-5))
		}
		s = fmt.Sprintf("%s %s %s", r.Key, r.Operator(), values)
	}
	if r.greaterThan != nil {
		s += fmt.Sprintf(" >%d", *r.greaterThan)
	}
	if r.lessThan != nil {
		s += fmt.Sprintf(" <%d", *r.lessThan)
	}
	if r.MinValues != nil {
		s += fmt.Sprintf(" minValues %d", *r.MinValues)
	}
	return s
}

func withinIntPtrs(valueAsString string, greaterThan, lessThan *int) bool {
	if greaterThan == nil && lessThan == nil {
		return true
	}
	// If bounds are set, non integer values are invalid
	value, err := strconv.Atoi(valueAsString)
	if err != nil {
		return false
	}
	if greaterThan != nil && *greaterThan >= value {
		return false
	}
	if lessThan != nil && *lessThan <= value {
		return false
	}
	return true
}

func minIntPtr(a, b *int) *int {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a < *b {
		return a
	}
	return b
}

func maxIntPtr(a, b *int) *int {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	if *a > *b {
		return a
	}
	return b
}
