/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
package druid

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	druidapi "github.com/apache/druid-operator/pkg/druidapi"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type noopEventEmitter struct{}

func (noopEventEmitter) EmitEventGeneric(obj object, eventReason, msg string, err error)         {}
func (noopEventEmitter) EmitEventRollingDeployWait(obj, k8sObj object, nodeSpecUniqueStr string) {}
func (noopEventEmitter) EmitEventOnGetError(obj, getObj object, err error)                       {}
func (noopEventEmitter) EmitEventOnUpdate(obj, updateObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnDelete(obj, deleteObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnCreate(obj, createObj object, err error)                      {}
func (noopEventEmitter) EmitEventOnPatch(obj, patchObj object, err error)                        {}
func (noopEventEmitter) EmitEventOnList(obj object, listObj objectList, err error)               {}

func TestComputeDeploymentLifecycleRevisionChangesWithForceRedeployToken(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 3,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			ForceRedeployToken:      "token-a",
		},
	}

	first, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	drd.Spec.ForceRedeployToken = "token-b"
	second, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	assert.NotEqual(t, first, second)
}

func TestComputeDeploymentLifecycleRevisionIgnoresExpectedBuildRevision(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 3,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "build-a",
		},
	}

	first, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	drd.Spec.ExpectedBuildRevision = "build-b"
	second, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	assert.Equal(t, first, second)
}

func TestDeploymentLifecycleStatusPatchIncludesNullsForClearedFields(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	payload, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"deploymentLifecycle": deploymentLifecycleStatusPatch(v1alpha1.DeploymentLifecycleStatus{
				Revision:               "rev-1",
				Phase:                  v1alpha1.DeploymentLifecycleInProgress,
				Reason:                 "Waiting for rollout",
				ObservedGeneration:     3,
				StartedAt:              &startedAt,
				CompletedAt:            nil,
				ExpectedBuildRevision:  "36.0.0",
				ObservedBuildRevisions: nil,
			}),
		},
	})
	assert.NoError(t, err)

	var decoded map[string]interface{}
	assert.NoError(t, json.Unmarshal(payload, &decoded))

	status := decoded["status"].(map[string]interface{})
	lifecycle := status["deploymentLifecycle"].(map[string]interface{})
	assert.Equal(t, "rev-1", lifecycle["revision"])
	assert.Equal(t, nil, lifecycle["completedAt"])
	assert.Equal(t, nil, lifecycle["observedBuildRevisions"])
	assert.NotNil(t, lifecycle["startedAt"])
}

func newLifecycleTestClient(t *testing.T, drd *v1alpha1.Druid, objects ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	assert.NoError(t, v1alpha1.AddToScheme(scheme))
	assert.NoError(t, appsv1.AddToScheme(scheme))
	assert.NoError(t, v1.AddToScheme(scheme))

	allObjects := append([]client.Object{drd}, objects...)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(drd).
		WithObjects(allObjects...).
		Build()
}

func readyManagedObjects(drd *v1alpha1.Druid) (*appsv1.Deployment, *v1.Pod) {
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-broker",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 2,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
		},
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-broker-0",
			Namespace: drd.Namespace,
			Labels:    makeLabelsForDruid(drd),
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	return deployment, pod
}

func readyManagedStatefulSetObjects(drd *v1alpha1.Druid) (*appsv1.StatefulSet, *v1.Pod) {
	replicas := int32(1)
	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-historical",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 2,
			CurrentRevision:    "rev-2",
			UpdateRevision:     "rev-2",
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example-historical-0",
			Namespace: drd.Namespace,
			Labels:    makeLabelsForDruid(drd),
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	return statefulSet, pod
}

func TestReconcileDeploymentLifecycleSucceedsWithoutExpectedBuildRevision(t *testing.T) {

	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Deployment lifecycle completed after managed workloads became ready", stored.Status.DeploymentLifecycle.Reason)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.NotEmpty(t, stored.Status.DeploymentLifecycle.Revision)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleWaitsForExpectedBuildRevision(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return []string{"old-build", "new-build"}, nil
		},
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleInProgress, stored.Status.DeploymentLifecycle.Phase)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, "build revision [new-build]")
	assert.Equal(t, "new-build", stored.Status.DeploymentLifecycle.ExpectedBuildRevision)
	assert.Equal(t, []string{"old-build", "new-build"}, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
	assert.Nil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleWaitsForExpectedBuildRevisionForImageRollout(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleInProgress, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, stored.Status.DeploymentLifecycle.Trigger)
	assert.Equal(t, "Waiting for spec.expectedBuildRevision before completing image rollout", stored.Status.DeploymentLifecycle.Reason)
	assert.Nil(t, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
	assert.Nil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleSucceedsForImageRolloutWhenBuildRevisionMatches(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return []string{"new-build"}, nil
		},
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, stored.Status.DeploymentLifecycle.Trigger)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, "completed for build revision [new-build]")
	assert.Equal(t, []string{"new-build"}, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
	assert.Equal(t, "apache/druid:31.0.0", stored.Status.DeploymentLifecycle.LastSuccessfulImage)
	assert.Equal(t, "", stored.Status.DeploymentLifecycle.LastSuccessfulForceRedeployToken)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestDetermineDeploymentTriggerUsesLastSuccessfulInputs(t *testing.T) {
	lastSuccessful := v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage:              "apache/druid:30.0.0",
		LastSuccessfulImageInputsHash:    "hash-a",
		LastSuccessfulForceRedeployToken: "token-a",
	}

	assert.Equal(t, v1alpha1.DeploymentTriggerSpecChange, determineDeploymentTrigger(deploymentLifecycleInputs{
		Image:              "apache/druid:30.0.0",
		ForceRedeployToken: "token-a",
		ImageInputsHash:    "hash-a",
	}, lastSuccessful))

	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, determineDeploymentTrigger(deploymentLifecycleInputs{
		Image:              "apache/druid:31.0.0",
		ForceRedeployToken: "token-a",
		ImageInputsHash:    "hash-b",
	}, lastSuccessful))

	assert.Equal(t, v1alpha1.DeploymentTriggerManualRollout, determineDeploymentTrigger(deploymentLifecycleInputs{
		Image:              "apache/druid:31.0.0",
		ForceRedeployToken: "token-b",
		ImageInputsHash:    "hash-b",
	}, lastSuccessful))
}

func TestDetermineDeploymentTriggerTreatsMissingLifecycleHistoryAsBootstrap(t *testing.T) {
	assert.Equal(t, v1alpha1.DeploymentTriggerSpecChange, determineDeploymentTrigger(deploymentLifecycleInputs{
		Image:              "apache/druid:31.0.0",
		ForceRedeployToken: "token-a",
		ImageInputsHash:    "hash-a",
	}, v1alpha1.DeploymentLifecycleStatus{}))
}

func TestReconcileDeploymentLifecyclePersistsManualRolloutInputsOnSuccess(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ForceRedeployToken:      "token-b",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage:              "apache/druid:31.0.0",
				LastSuccessfulForceRedeployToken: "token-a",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return []string{"new-build"}, nil
		},
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, v1alpha1.DeploymentTriggerManualRollout, stored.Status.DeploymentLifecycle.Trigger)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, "completed for build revision [new-build]")
	assert.Equal(t, "token-b", stored.Status.DeploymentLifecycle.LastSuccessfulForceRedeployToken)
	assert.Equal(t, "apache/druid:31.0.0", stored.Status.DeploymentLifecycle.LastSuccessfulImage)
	assert.NotEmpty(t, stored.Status.DeploymentLifecycle.LastSuccessfulImageInputsHash)
}

func TestReconcileDeploymentLifecycleIgnoresExpectedBuildRevisionForSpecChange(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:31.0.0",
				LastSuccessfulImageInputsHash: func() string {
					inputs, err := currentDeploymentLifecycleInputs(&v1alpha1.Druid{Spec: v1alpha1.DruidSpec{Image: "apache/druid:31.0.0"}})
					assert.NoError(t, err)
					return inputs.ImageInputsHash
				}(),
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, v1alpha1.DeploymentTriggerSpecChange, stored.Status.DeploymentLifecycle.Trigger)
	assert.Equal(t, "Deployment lifecycle completed after managed workloads became ready", stored.Status.DeploymentLifecycle.Reason)
	assert.Nil(t, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
}

func TestEnsureDeploymentLifecycleStartedPublishesObservedGenerationAndRevisionForCurrentRollout(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 5,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleSucceeded,
				ObservedGeneration: 4,
				Revision:           "old-revision",
			},
		},
	}

	expectedRevision, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	k8sClient := newLifecycleTestClient(t, drd)
	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecyclePending, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.Equal(t, expectedRevision, stored.Status.DeploymentLifecycle.Revision)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.StartedAt)
}

func TestEnsureDeploymentLifecycleStartedReplacesStaleSucceededStatusForNewGeneration(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	completedAt := metav1.NewTime(time.Unix(160, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 6,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleSucceeded,
				Reason:             "Deployment lifecycle completed after managed workloads became ready",
				ObservedGeneration: 5,
				Revision:           "revision-for-generation-5",
				StartedAt:          &startedAt,
				CompletedAt:        &completedAt,
			},
		},
	}

	k8sClient := newLifecycleTestClient(t, drd)
	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecyclePending, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Waiting for Druid workloads to roll out", stored.Status.DeploymentLifecycle.Reason)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.NotEqual(t, "revision-for-generation-5", stored.Status.DeploymentLifecycle.Revision)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.StartedAt)
	assert.False(t, stored.Status.DeploymentLifecycle.StartedAt.Time.Equal(startedAt.Time))
}

func TestExpectedBuildRevisionUpdateKeepsRevisionStableForCurrentRollout(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 7,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:               v1alpha1.DeploymentLifecycleInProgress,
				Trigger:             v1alpha1.DeploymentTriggerImageChange,
				Reason:              "Waiting for spec.expectedBuildRevision before completing image rollout",
				ObservedGeneration:  7,
				StartedAt:           &startedAt,
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	revisionBefore, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)
	drd.Status.DeploymentLifecycle.Revision = revisionBefore

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return []string{"new-build"}, nil
		},
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleSucceeded, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.Equal(t, revisionBefore, stored.Status.DeploymentLifecycle.Revision)
}

func TestDetermineDeploymentTriggerDetectsNodeLevelImageChanges(t *testing.T) {
	lastSuccessful := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					Image: "apache/druid:31.0.0",
				},
			},
		},
	}
	lastSuccessfulInputs, err := currentDeploymentLifecycleInputs(lastSuccessful)
	assert.NoError(t, err)

	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					Image: "apache/druid:32.0.0",
				},
			},
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage:           "apache/druid:31.0.0",
				LastSuccessfulImageInputsHash: lastSuccessfulInputs.ImageInputsHash,
			},
		},
	}

	current, err := currentDeploymentLifecycleInputs(drd)
	assert.NoError(t, err)
	trigger := determineDeploymentTrigger(current, drd.Status.DeploymentLifecycle)
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestDetermineDeploymentTriggerTreatsLegacyNodeLevelOverridesAsImageChange(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					Image: "apache/druid:32.0.0",
				},
			},
		},
	}

	current, err := currentDeploymentLifecycleInputs(drd)
	assert.NoError(t, err)

	trigger := determineDeploymentTrigger(current, v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage: "apache/druid:31.0.0",
	})
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestDetermineDeploymentTriggerDetectsClusterSidecarImageChanges(t *testing.T) {
	lastSuccessful := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			AdditionalContainer: []v1alpha1.AdditionalContainer{
				{
					ContainerName: "log-forwarder",
					Image:         "sidecar:v1",
				},
			},
		},
	}
	lastSuccessfulInputs, err := currentDeploymentLifecycleInputs(lastSuccessful)
	assert.NoError(t, err)

	current := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			AdditionalContainer: []v1alpha1.AdditionalContainer{
				{
					ContainerName: "log-forwarder",
					Image:         "sidecar:v2",
				},
			},
		},
	}
	currentInputs, err := currentDeploymentLifecycleInputs(current)
	assert.NoError(t, err)

	trigger := determineDeploymentTrigger(currentInputs, v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage:           "apache/druid:31.0.0",
		LastSuccessfulImageInputsHash: lastSuccessfulInputs.ImageInputsHash,
	})
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestDetermineDeploymentTriggerTreatsLegacyClusterSidecarImageChangesAsImageChange(t *testing.T) {
	current := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			AdditionalContainer: []v1alpha1.AdditionalContainer{
				{
					ContainerName: "log-forwarder",
					Image:         "sidecar:v2",
				},
			},
		},
	}
	currentInputs, err := currentDeploymentLifecycleInputs(current)
	assert.NoError(t, err)

	trigger := determineDeploymentTrigger(currentInputs, v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage: "apache/druid:31.0.0",
	})
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestDetermineDeploymentTriggerDetectsNodeInitContainerImageChanges(t *testing.T) {
	lastSuccessful := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					AdditionalContainer: []v1alpha1.AdditionalContainer{
						{
							RunAsInit:     true,
							ContainerName: "warm-cache",
							Image:         "init:v1",
						},
					},
				},
			},
		},
	}
	lastSuccessfulInputs, err := currentDeploymentLifecycleInputs(lastSuccessful)
	assert.NoError(t, err)

	current := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					AdditionalContainer: []v1alpha1.AdditionalContainer{
						{
							RunAsInit:     true,
							ContainerName: "warm-cache",
							Image:         "init:v2",
						},
					},
				},
			},
		},
	}
	currentInputs, err := currentDeploymentLifecycleInputs(current)
	assert.NoError(t, err)

	trigger := determineDeploymentTrigger(currentInputs, v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage:           "apache/druid:31.0.0",
		LastSuccessfulImageInputsHash: lastSuccessfulInputs.ImageInputsHash,
	})
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestDetermineDeploymentTriggerTreatsLegacyNodeInitContainerImageChangesAsImageChange(t *testing.T) {
	current := &v1alpha1.Druid{
		Spec: v1alpha1.DruidSpec{
			Image: "apache/druid:31.0.0",
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"brokers": {
					AdditionalContainer: []v1alpha1.AdditionalContainer{
						{
							RunAsInit:     true,
							ContainerName: "warm-cache",
							Image:         "init:v2",
						},
					},
				},
			},
		},
	}
	currentInputs, err := currentDeploymentLifecycleInputs(current)
	assert.NoError(t, err)

	trigger := determineDeploymentTrigger(currentInputs, v1alpha1.DeploymentLifecycleStatus{
		LastSuccessfulImage: "apache/druid:31.0.0",
	})
	assert.Equal(t, v1alpha1.DeploymentTriggerImageChange, trigger)
}

func TestAreManagedWorkloadsReadyWaitsForDeploymentObservedGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	replicas := int32(1)
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example-broker",
			Namespace:  drd.Namespace,
			Labels:     makeLabelsForDruid(drd),
			Generation: 2,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration: 1,
			Replicas:           1,
			ReadyReplicas:      1,
			UpdatedReplicas:    1,
		},
	}

	k8sClient := newLifecycleTestClient(t, drd, deployment)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, "Waiting for Deployment [example-broker] controller to observe generation", reason)
}

func TestAreManagedWorkloadsReadyWaitsForStatefulSetObservedGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	statefulSet, _ := readyManagedStatefulSetObjects(drd)
	statefulSet.Status.ObservedGeneration = 1

	k8sClient := newLifecycleTestClient(t, drd, statefulSet)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.False(t, ready)
	assert.Equal(t, "Waiting for StatefulSet [example-historical] controller to observe generation", reason)
}

func TestAreManagedWorkloadsReadyAcceptsObservedStatefulSetGeneration(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}

	statefulSet, _ := readyManagedStatefulSetObjects(drd)

	k8sClient := newLifecycleTestClient(t, drd, statefulSet)
	ready, reason, err := areManagedWorkloadsReady(context.Background(), k8sClient, drd)
	assert.NoError(t, err)
	assert.True(t, ready)
	assert.Equal(t, "", reason)
}

func TestReconcileDeploymentLifecycleWaitsWhenBuildRevisionLookupIsUnavailable(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	assert.NoError(t, reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return nil, &druidapi.BuildRevisionLookupPendingError{
				Reason: druidapi.BuildRevisionPendingQueryServiceDiscovery,
			}
		},
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleInProgress, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Waiting for Druid query service discovery", stored.Status.DeploymentLifecycle.Reason)
	assert.Nil(t, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
	assert.Nil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleReturnsCanceledErrorWithoutOverwritingPendingStatus(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	err := reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return nil, context.Canceled
		},
	})
	assert.ErrorIs(t, err, context.Canceled)

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecyclePending, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "Waiting for Druid workloads to roll out", stored.Status.DeploymentLifecycle.Reason)
	assert.Nil(t, stored.Status.DeploymentLifecycle.ObservedBuildRevisions)
	assert.Nil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileDeploymentLifecycleReturnsTerminalBuildRevisionLookupError(t *testing.T) {
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
			ExpectedBuildRevision:   "new-build",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				LastSuccessfulImage: "apache/druid:30.0.0",
			},
		},
	}

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)

	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))
	err := reconcileDeploymentLifecycleWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, lifecycleDependencies{
		now: time.Now,
		getBuildRevisions: func(ctx context.Context, sdk client.Client, namespace, druidClusterName string, auth druidapi.Auth) ([]string, error) {
			return nil, errors.New("unauthorized build revision query")
		},
	})
	assert.Error(t, err)
	assert.True(t, shouldPersistDeploymentLifecycleFailure(err))
	assert.Contains(t, err.Error(), "unauthorized build revision query")
}

func TestMarkDeploymentLifecycleFailedPreservesStartedAtAndRevisionMetadata(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	completedAt := time.Unix(180, 0)
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 3,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:     v1alpha1.DeploymentLifecycleInProgress,
				StartedAt: &startedAt,
				Trigger:   v1alpha1.DeploymentTriggerSpecChange,
			},
		},
	}

	expectedRevision, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)
	drd.Status.DeploymentLifecycle.Revision = expectedRevision
	k8sClient := newLifecycleTestClient(t, drd)

	assert.NoError(t, markDeploymentLifecycleFailedWithDeps(context.Background(), k8sClient, drd, noopEventEmitter{}, errors.New("deploy failed"), lifecycleDependencies{
		now: func() time.Time { return completedAt },
	}))

	stored := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleFailed, stored.Status.DeploymentLifecycle.Phase)
	assert.Equal(t, "deploy failed", stored.Status.DeploymentLifecycle.Reason)
	assert.Equal(t, expectedRevision, stored.Status.DeploymentLifecycle.Revision)
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.StartedAt)
	assert.True(t, stored.Status.DeploymentLifecycle.StartedAt.Time.Equal(startedAt.Time))
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
	assert.True(t, stored.Status.DeploymentLifecycle.CompletedAt.Time.Equal(completedAt))
}

func TestBuildLifecycleStatusForRevisionClearsObservedBuildRevisionsOnNewRevision(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 4,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
			Image:                   "apache/druid:31.0.0",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Revision:               "old-revision",
				ObservedBuildRevisions: []string{"old-build"},
			},
		},
	}

	desiredRevision, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)

	updated, err := buildLifecycleStatusForRevision(drd, drd.Status.DeploymentLifecycle, desiredRevision)
	assert.NoError(t, err)
	assert.Nil(t, updated.ObservedBuildRevisions)
}

func TestReconcileDeploymentLifecycleIsIdempotentAfterSuccess(t *testing.T) {
	startedAt := metav1.NewTime(time.Unix(100, 0))
	completedAt := metav1.NewTime(time.Unix(160, 0))
	drd := &v1alpha1.Druid{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "druid.apache.org/v1alpha1",
			Kind:       "Druid",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:       "example",
			Namespace:  "default",
			Generation: 2,
		},
		Spec: v1alpha1.DruidSpec{
			CommonRuntimeProperties: "druid.service=druid/router",
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase:              v1alpha1.DeploymentLifecycleSucceeded,
				Trigger:            v1alpha1.DeploymentTriggerSpecChange,
				Reason:             "Deployment lifecycle completed after managed workloads became ready",
				ObservedGeneration: 2,
				StartedAt:          &startedAt,
				CompletedAt:        &completedAt,
			},
		},
	}
	revision, err := computeDeploymentLifecycleRevision(drd)
	assert.NoError(t, err)
	drd.Status.DeploymentLifecycle.Revision = revision

	deployment, pod := readyManagedObjects(drd)
	k8sClient := newLifecycleTestClient(t, drd, deployment, pod)
	assert.NoError(t, ensureDeploymentLifecycleStarted(context.Background(), k8sClient, drd, noopEventEmitter{}))

	before := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), before))

	assert.NoError(t, reconcileDeploymentLifecycle(context.Background(), k8sClient, drd, noopEventEmitter{}))

	after := &v1alpha1.Druid{}
	assert.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), after))
	assert.Equal(t, before.Status.DeploymentLifecycle, after.Status.DeploymentLifecycle)
}
