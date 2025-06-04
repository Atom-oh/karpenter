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

// events.go 파일은 일관성 검사 관련 이벤트를 정의합니다.
// 이 파일은 일관성 검사 실패 시 발생하는 이벤트를 생성하는 함수를 제공합니다.
// 이러한 이벤트는 NodeClaim과 노드 간의 불일치를 사용자에게 알리는 데 사용됩니다.
package consistency

import (
	corev1 "k8s.io/api/core/v1"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

// FailedConsistencyCheckEvent는 일관성 검사 실패 시 발생하는 이벤트를 생성합니다.
// 이 함수는 NodeClaim과 실패 메시지를 받아 경고 유형의 이벤트를 반환합니다.
// 중복 이벤트를 방지하기 위해 NodeClaim UID와 메시지를 중복 제거 값으로 사용합니다.
func FailedConsistencyCheckEvent(nodeClaim *v1.NodeClaim, message string) events.Event {
	return events.Event{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.FailedConsistencyCheck,
		Message:        message,
		DedupeValues:   []string{string(nodeClaim.UID), message},
	}
}
