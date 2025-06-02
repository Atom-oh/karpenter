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

// 메인 패키지는 Karpenter의 진입점입니다.
// 이 패키지는 Karpenter 컨트롤러를 초기화하고 실행합니다.
package main

import (
	"sigs.k8s.io/controller-runtime/pkg/log"

	// kwok 클라우드 프로바이더 패키지를 가져옵니다.
	kwok "sigs.k8s.io/karpenter/kwok/cloudprovider"
	// 컨트롤러 패키지를 가져옵니다.
	"sigs.k8s.io/karpenter/pkg/controllers"
	// 상태 관리 패키지를 가져옵니다.
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	// 오퍼레이터 패키지를 가져옵니다.
	"sigs.k8s.io/karpenter/pkg/operator"
)

// main 함수는 Karpenter의 진입점입니다.
// 이 함수는 오퍼레이터를 초기화하고, 인스턴스 타입을 구성하며, 클라우드 프로바이더와 클러스터 상태를 설정합니다.
// 그리고 모든 컨트롤러를 등록하고 시작합니다.
func main() {
	// 오퍼레이터를 초기화합니다.
	ctx, op := operator.NewOperator()
	// 인스턴스 타입을 구성합니다.
	instanceTypes, err := kwok.ConstructInstanceTypes(ctx)
	if err != nil {
		log.FromContext(ctx).Error(err, "failed constructing instance types")
	}

	// 클라우드 프로바이더를 초기화합니다.
	cloudProvider := kwok.NewCloudProvider(ctx, op.GetClient(), instanceTypes)
	// 클러스터 상태를 초기화합니다.
	clusterState := state.NewCluster(op.Clock, op.GetClient(), cloudProvider)
	// 컨트롤러를 등록하고 시작합니다.
	op.
		WithControllers(ctx, controllers.NewControllers(
			ctx,
			op.Manager,
			op.Clock,
			op.GetClient(),
			op.EventRecorder,
			cloudProvider,
			clusterState,
		)...).Start(ctx)
}
