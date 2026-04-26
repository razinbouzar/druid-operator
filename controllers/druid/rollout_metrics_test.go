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
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	testNamespace = "default"
	testCluster   = "example"
)

func TestDruidRolloutMetricsSyncExportsCurrentRolloutState(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDruidRolloutMetrics(registry)

	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCluster,
			Namespace: testNamespace,
		},
		Spec: v1alpha1.DruidSpec{
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"historicals": {
					NodeType: "historical",
					Kind:     "StatefulSet",
					Replicas: 3,
				},
				"brokers": {
					NodeType: "broker",
					Kind:     "Deployment",
					Replicas: 2,
				},
			},
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase: v1alpha1.DeploymentLifecycleInProgress,
			},
		},
	}

	historicalLabels := map[string]string{
		"app":               "druid",
		"druid_cr":          drd.Name,
		"nodeSpecUniqueStr": "druid-example-historicals",
		"component":         "historical",
	}
	brokerLabels := map[string]string{
		"app":               "druid",
		"druid_cr":          drd.Name,
		"nodeSpecUniqueStr": "druid-example-brokers",
		"component":         "broker",
	}

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "druid-example-historicals",
			Namespace:  drd.Namespace,
			Generation: 7,
			Labels:     historicalLabels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(3),
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 7,
			CurrentRevision:    "old",
			UpdateRevision:     "new",
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "druid-example-brokers",
			Namespace:  drd.Namespace,
			Generation: 4,
			Labels:     brokerLabels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: int32Ptr(2),
		},
		Status: appsv1.DeploymentStatus{
			ObservedGeneration:  4,
			UpdatedReplicas:     2,
			ReadyReplicas:       2,
			Replicas:            2,
			UnavailableReplicas: 0,
		},
	}

	k8sClient := newLifecycleTestClient(t, drd, statefulSet, deployment)

	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(100, 0)))
	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(145, 0)))

	families, err := registry.Gather()
	require.NoError(t, err)

	assertMetricValue(t, families, "druid_operator_cluster_fully_deployed", clusterMetricLabels(), 0.0)
	assertMetricValue(t, families, "druid_operator_cluster_deployment_phase", clusterMetricLabels("phase", "in_progress"), 1.0)
	assertMetricValue(t, families, "druid_operator_workload_rollout_in_progress", workloadMetricLabelSet("druid-example-historicals", "historical", "StatefulSet"), 1.0)
	assertMetricValue(t, families, "druid_operator_workload_rollout_duration_seconds", workloadMetricLabelSet("druid-example-historicals", "historical", "StatefulSet"), 45.0)
	assertMetricValue(t, families, "druid_operator_workload_desired_replicas", workloadMetricLabelSet("druid-example-historicals", "historical", "StatefulSet"), 3.0)
	assertMetricValue(t, families, "druid_operator_workload_updated_replicas", workloadMetricLabelSet("druid-example-historicals", "historical", "StatefulSet"), 1.0)
	assertMetricValue(t, families, "druid_operator_workload_ready_replicas", workloadMetricLabelSet("druid-example-historicals", "historical", "StatefulSet"), 1.0)
	assertMetricValue(t, families, "druid_operator_workload_rollout_in_progress", workloadMetricLabelSet("druid-example-brokers", "broker", "Deployment"), 0.0)
}

func TestDruidRolloutMetricsSyncRestartsDurationForNewWorkloadIdentity(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDruidRolloutMetrics(registry)

	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCluster,
			Namespace: testNamespace,
		},
		Status: v1alpha1.DruidClusterStatus{
			DeploymentLifecycle: v1alpha1.DeploymentLifecycleStatus{
				Phase: v1alpha1.DeploymentLifecycleInProgress,
			},
		},
	}

	first := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "druid-example-historicals-v1",
			Namespace:  drd.Namespace,
			Generation: 1,
			Labels: map[string]string{
				"app":               "druid",
				"druid_cr":          drd.Name,
				"nodeSpecUniqueStr": "druid-example-historicals-v1",
				"component":         "historical",
			},
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(2),
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1,
			CurrentRevision:    "old",
			UpdateRevision:     "new",
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}
	second := first.DeepCopy()
	second.Name = "druid-example-historicals-v2"
	second.Labels["nodeSpecUniqueStr"] = "druid-example-historicals-v2"

	k8sClient := newLifecycleTestClient(t, drd, first)
	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(100, 0)))
	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(145, 0)))

	k8sClient = newLifecycleTestClient(t, drd, second)
	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(200, 0)))

	families, err := registry.Gather()
	require.NoError(t, err)

	assertMetricValue(t, families, "druid_operator_workload_rollout_duration_seconds", workloadMetricLabelSet("druid-example-historicals-v1", "historical", "StatefulSet"), 0.0)
	assertMetricValue(t, families, "druid_operator_workload_rollout_duration_seconds", workloadMetricLabelSet("druid-example-historicals-v2", "historical", "StatefulSet"), 0.0)
	assertMetricValue(t, families, "druid_operator_workload_rollout_in_progress", workloadMetricLabelSet("druid-example-historicals-v2", "historical", "StatefulSet"), 1.0)
}

func TestDruidRolloutMetricsSyncExportsForceDeleteHealthAndActions(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDruidRolloutMetrics(registry)

	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testCluster,
			Namespace: testNamespace,
		},
		Spec: v1alpha1.DruidSpec{
			ForceDeleteStsPodOnError: true,
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"historicals": {
					NodeType:            "historical",
					Kind:                "StatefulSet",
					Replicas:            1,
					PodManagementPolicy: appsv1.OrderedReadyPodManagement,
				},
			},
		},
	}

	nodeSpec := drd.Spec.Nodes["historicals"]
	nodeSpecUniqueStr := makeNodeSpecificUniqueString(drd, "historicals")
	labels := makeLabelsForNodeSpec(&nodeSpec, drd, drd.Name, nodeSpecUniqueStr)

	statefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeSpecUniqueStr,
			Namespace: drd.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: int32Ptr(1),
		},
		Status: appsv1.StatefulSetStatus{
			ObservedGeneration: 1,
			CurrentRevision:    "rev",
			UpdateRevision:     "rev",
			UpdatedReplicas:    1,
			ReadyReplicas:      1,
		},
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeSpecUniqueStr + "-0",
			Namespace: drd.Namespace,
			Labels:    labels,
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{{
				Type:   v1.ContainersReady,
				Status: v1.ConditionFalse,
			}},
			ContainerStatuses: []v1.ContainerStatus{{
				Name:         "druid",
				RestartCount: 3,
			}},
		},
	}

	k8sClient := newLifecycleTestClient(t, drd, statefulSet, pod)

	require.NoError(t, metrics.syncWithNow(context.Background(), k8sClient, drd, time.Unix(200, 0)))
	require.NoError(t, checkCrashStatusWithMetrics(context.Background(), k8sClient, &nodeSpec, drd, nodeSpecUniqueStr, noopEventEmitter{}, metrics))

	families, err := registry.Gather()
	require.NoError(t, err)

	assertMetricValue(t, families, "druid_operator_workload_force_delete_healthcheck_healthy", workloadMetricLabelSet(nodeSpecUniqueStr, "historical", "StatefulSet"), 0.0)
	assertMetricValue(t, families, "druid_operator_workload_force_delete_actions_total", forceDeleteActionLabels(nodeSpecUniqueStr, "historical", "crashloop"), 1.0)
}

func TestDruidRolloutMetricsDeleteClusterRemovesForceDeleteActionSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDruidRolloutMetrics(registry)

	metrics.recordForceDeleteAction(testNamespace, testCluster, "druid-example-historicals", "historical", "crashloop")

	beforeFamilies, err := registry.Gather()
	require.NoError(t, err)
	assertMetricValue(t, beforeFamilies, "druid_operator_workload_force_delete_actions_total", forceDeleteActionLabels("druid-example-historicals", "historical", "crashloop"), 1.0)

	metrics.deleteCluster(testNamespace, testCluster)

	afterFamilies, err := registry.Gather()
	require.NoError(t, err)
	assertMetricValue(t, afterFamilies, "druid_operator_workload_force_delete_actions_total", forceDeleteActionLabels("druid-example-historicals", "historical", "crashloop"), 0.0)
}

func int32Ptr(value int32) *int32 {
	return &value
}

func assertMetricValue(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string, want float64) {
	t.Helper()
	assert.Equal(t, want, metricValue(t, families, name, labels))
}

func clusterMetricLabels(extra ...string) map[string]string {
	labels := map[string]string{
		"namespace":      testNamespace,
		"druid_instance": testCluster,
	}
	for i := 0; i+1 < len(extra); i += 2 {
		labels[extra[i]] = extra[i+1]
	}
	return labels
}

func workloadMetricLabelSet(workload, nodeType, kind string) map[string]string {
	labels := clusterMetricLabels()
	labels["workload"] = workload
	labels["node_type"] = nodeType
	labels["kind"] = kind
	return labels
}

func forceDeleteActionLabels(workload, nodeType, reason string) map[string]string {
	labels := clusterMetricLabels()
	labels["workload"] = workload
	labels["node_type"] = nodeType
	labels["reason"] = reason
	return labels
}
