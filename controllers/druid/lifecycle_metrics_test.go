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
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

const (
	testLifecycleNamespace = "default"
	testLifecycleCluster   = "example"
)

func TestDeploymentLifecycleMetricsRecordsPhaseTransitionsAndTerminalOutcome(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)

	startedAt := metav1TimePtr(time.Unix(100, 0))
	completedAt := metav1TimePtr(time.Unix(160, 0))

	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerImageChange,
			StartedAt: startedAt,
		},
	)
	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerImageChange,
			StartedAt: startedAt,
		},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:       v1alpha1.DeploymentLifecycleSucceeded,
			Trigger:     v1alpha1.DeploymentTriggerImageChange,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
		},
	)

	families, err := registry.Gather()
	require.NoError(t, err)

	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "pending",
		"trigger":        "image_change",
	}))
	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "succeeded",
		"trigger":        "image_change",
	}))
	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_completions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"result":         "succeeded",
		"trigger":        "image_change",
	}))
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"trigger":        "image_change",
	}))
	assert.Equal(t, uint64(1), histogramSampleCount(t, families, "druid_operator_deployment_lifecycle_duration_seconds", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"result":         "succeeded",
		"trigger":        "image_change",
	}))
	assert.InDelta(t, 60.0, histogramSampleSum(t, families, "druid_operator_deployment_lifecycle_duration_seconds", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"result":         "succeeded",
		"trigger":        "image_change",
	}), 0.0001)
}

func TestDeploymentLifecycleMetricsDoesNotDoubleCountNoOpUpdates(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)

	startedAt := metav1TimePtr(time.Unix(100, 0))
	current := v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecycleInProgress,
		Trigger:   v1alpha1.DeploymentTriggerSpecChange,
		StartedAt: startedAt,
		Reason:    "Waiting for Pod [example] readiness",
	}

	metrics.recordTransition(testLifecycleNamespace, testLifecycleCluster, current, current)

	families, err := registry.Gather()
	require.NoError(t, err)

	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "in_progress",
		"trigger":        "spec_change",
	}))
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"trigger":        "spec_change",
	}))
}

func TestDeploymentLifecycleMetricsKeepsInProgressGaugeAcrossActiveTransitions(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)

	startedAt := metav1TimePtr(time.Unix(100, 0))
	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerManualRollout,
			StartedAt: startedAt,
		},
	)
	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerManualRollout,
			StartedAt: startedAt,
		},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecycleInProgress,
			Trigger:   v1alpha1.DeploymentTriggerManualRollout,
			StartedAt: startedAt,
		},
	)

	families, err := registry.Gather()
	require.NoError(t, err)

	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"trigger":        "manual_rollout",
	}))
	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "pending",
		"trigger":        "manual_rollout",
	}))
	assert.Equal(t, 1.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "in_progress",
		"trigger":        "manual_rollout",
	}))
}

func TestDefaultDeploymentLifecycleMetricsAreRegistered(t *testing.T) {
	startedAt := metav1TimePtr(time.Unix(100, 0))
	defaultDeploymentLifecycleMetrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerImageChange,
			StartedAt: startedAt,
		},
	)
	defaultDeploymentLifecycleMetrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerImageChange,
			StartedAt: startedAt,
		},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:       v1alpha1.DeploymentLifecycleSucceeded,
			Trigger:     v1alpha1.DeploymentTriggerImageChange,
			StartedAt:   startedAt,
			CompletedAt: metav1TimePtr(time.Unix(160, 0)),
		},
	)

	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)

	assertMetricFamilyExists(t, families, "druid_operator_deployment_lifecycle_transitions_total")
	assertMetricFamilyExists(t, families, "druid_operator_deployment_lifecycle_completions_total")
	assertMetricFamilyExists(t, families, "druid_operator_deployment_lifecycle_duration_seconds")
	assertMetricFamilyExists(t, families, "druid_operator_deployment_lifecycle_in_progress")
}

func TestDefaultDeploymentLifecycleMetricsAreExposedByMetricsHandler(t *testing.T) {
	defaultDeploymentLifecycleMetrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerManualRollout,
			StartedAt: metav1TimePtr(time.Unix(100, 0)),
		},
	)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	recorder := httptest.NewRecorder()
	handler := promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{})
	handler.ServeHTTP(recorder, req)

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Contains(t, recorder.Body.String(), "druid_operator_deployment_lifecycle_transitions_total")
	assert.Contains(t, recorder.Body.String(), `trigger="manual_rollout"`)
	assert.Contains(t, recorder.Body.String(), `namespace="default"`)
	assert.Contains(t, recorder.Body.String(), `druid_instance="example"`)
}

func TestPatchDeploymentLifecycleStatusRecordsMetricsAfterSuccessfulPatch(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}
	k8sClient := newLifecycleTestClient(t, drd)

	updated := v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecyclePending,
		Trigger:   v1alpha1.DeploymentTriggerSpecChange,
		StartedAt: metav1TimePtr(time.Unix(100, 0)),
		Reason:    "Waiting for Druid workloads to roll out",
	}

	before := globalMetricValue(t, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"phase":          "pending",
		"trigger":        "spec_change",
	})
	beforeGauge := globalMetricValue(t, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"trigger":        "spec_change",
	})

	require.NoError(t, patchDeploymentLifecycleStatus(context.Background(), k8sClient, drd, updated, noopEventEmitter{}))

	after := globalMetricValue(t, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"phase":          "pending",
		"trigger":        "spec_change",
	})
	afterGauge := globalMetricValue(t, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"trigger":        "spec_change",
	})

	assert.Equal(t, before+1, after)
	assert.Equal(t, beforeGauge+1, afterGauge)
}

func TestPatchDeploymentLifecycleStatusDoesNotRecordMetricsWhenPatchFails(t *testing.T) {
	drd := &v1alpha1.Druid{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "example",
			Namespace: "default",
		},
	}
	k8sClient := newLifecycleTestClient(t, drd)

	updated := v1alpha1.DeploymentLifecycleStatus{
		Phase:     v1alpha1.DeploymentLifecyclePending,
		Trigger:   v1alpha1.DeploymentTriggerSpecChange,
		StartedAt: metav1TimePtr(time.Unix(100, 0)),
		Reason:    "Waiting for Druid workloads to roll out",
	}

	previousWriter := writers
	writers = failingPatchWriter{err: errors.New("patch failed")}
	t.Cleanup(func() {
		writers = previousWriter
	})

	before := globalMetricValue(t, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"phase":          "pending",
		"trigger":        "spec_change",
	})

	err := patchDeploymentLifecycleStatus(context.Background(), k8sClient, drd, updated, noopEventEmitter{})
	require.EqualError(t, err, "patch failed")

	after := globalMetricValue(t, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      drd.Namespace,
		"druid_instance": drd.Name,
		"phase":          "pending",
		"trigger":        "spec_change",
	})

	assert.Equal(t, before, after)
}

func TestDeploymentLifecycleMetricsDeleteClusterRemovesPerClusterSeries(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics := newDeploymentLifecycleMetrics(registry)

	startedAt := metav1TimePtr(time.Unix(100, 0))
	completedAt := metav1TimePtr(time.Unix(160, 0))
	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerSpecChange,
			StartedAt: startedAt,
		},
	)
	metrics.recordTransition(
		testLifecycleNamespace,
		testLifecycleCluster,
		v1alpha1.DeploymentLifecycleStatus{
			Phase:     v1alpha1.DeploymentLifecyclePending,
			Trigger:   v1alpha1.DeploymentTriggerSpecChange,
			StartedAt: startedAt,
		},
		v1alpha1.DeploymentLifecycleStatus{
			Phase:       v1alpha1.DeploymentLifecycleSucceeded,
			Trigger:     v1alpha1.DeploymentTriggerSpecChange,
			StartedAt:   startedAt,
			CompletedAt: completedAt,
		},
	)

	metrics.deleteCluster(testLifecycleNamespace, testLifecycleCluster)

	families, err := registry.Gather()
	require.NoError(t, err)
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_transitions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"phase":          "pending",
		"trigger":        "spec_change",
	}))
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_completions_total", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"result":         "succeeded",
		"trigger":        "spec_change",
	}))
	assert.Equal(t, 0.0, metricValue(t, families, "druid_operator_deployment_lifecycle_in_progress", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"trigger":        "spec_change",
	}))
	assert.False(t, metricExists(families, "druid_operator_deployment_lifecycle_duration_seconds", map[string]string{
		"namespace":      testLifecycleNamespace,
		"druid_instance": testLifecycleCluster,
		"result":         "succeeded",
		"trigger":        "spec_change",
	}))
}

func metricValue(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	metric := findMetric(t, families, name, labels)
	switch {
	case metric.Counter != nil:
		return metric.Counter.GetValue()
	case metric.Gauge != nil:
		return metric.Gauge.GetValue()
	default:
		return 0
	}
}

func globalMetricValue(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := ctrlmetrics.Registry.Gather()
	require.NoError(t, err)
	return metricValue(t, families, name, labels)
}

func histogramSampleCount(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) uint64 {
	t.Helper()
	metric := findMetric(t, families, name, labels)
	require.NotNil(t, metric.Histogram)
	return metric.Histogram.GetSampleCount()
}

func histogramSampleSum(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) float64 {
	t.Helper()
	metric := findMetric(t, families, name, labels)
	require.NotNil(t, metric.Histogram)
	return metric.Histogram.GetSampleSum()
}

func findMetric(t *testing.T, families []*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if hasLabels(metric, labels) {
				return metric
			}
		}
	}
	return &dto.Metric{}
}

func metricExists(families []*dto.MetricFamily, name string, labels map[string]string) bool {
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.Metric {
			if hasLabels(metric, labels) {
				return true
			}
		}
	}
	return false
}

func assertMetricFamilyExists(t *testing.T, families []*dto.MetricFamily, name string) {
	t.Helper()
	for _, family := range families {
		if family.GetName() == name {
			return
		}
	}
	t.Fatalf("metric family %s not found", name)
}

func hasLabels(metric *dto.Metric, labels map[string]string) bool {
	if len(labels) == 0 {
		return true
	}
	matched := 0
	for _, label := range metric.Label {
		expected, ok := labels[label.GetName()]
		if !ok {
			continue
		}
		if expected != label.GetValue() {
			return false
		}
		matched++
	}
	return matched == len(labels)
}

func metav1TimePtr(t time.Time) *metav1.Time {
	value := metav1.NewTime(t)
	return &value
}

type failingPatchWriter struct {
	err error
}

func (w failingPatchWriter) Delete(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter, deleteOptions ...client.DeleteOption) error {
	return nil
}

func (w failingPatchWriter) Create(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {
	return "", nil
}

func (w failingPatchWriter) Update(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, emitEvent EventEmitter) (DruidNodeStatus, error) {
	return "", nil
}

func (w failingPatchWriter) Patch(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, status bool, patch client.Patch, emitEvent EventEmitter) error {
	return w.err
}
