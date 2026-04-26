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
	"fmt"
	"sync"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type deploymentLifecycleMetrics struct {
	transitions *prometheus.CounterVec
	completions *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	inProgress  *prometheus.GaugeVec
}

var defaultDeploymentLifecycleMetrics = newDeploymentLifecycleMetrics(ctrlmetrics.Registry)

var deploymentLifecycleTriggers = []string{"spec_change", "image_change", "manual_rollout", "unknown"}
var deploymentLifecycleResults = []string{"succeeded", "failed", "unknown"}

func newDeploymentLifecycleMetrics(registerer prometheus.Registerer) *deploymentLifecycleMetrics {
	metrics := &deploymentLifecycleMetrics{
		transitions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_operator_deployment_lifecycle_transitions_total",
				Help: "Total number of Druid deployment lifecycle phase transitions.",
			},
			[]string{"namespace", "druid_instance", "phase", "trigger"},
		),
		completions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_operator_deployment_lifecycle_completions_total",
				Help: "Total number of terminal Druid deployment lifecycle results.",
			},
			[]string{"namespace", "druid_instance", "result", "trigger"},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "druid_operator_deployment_lifecycle_duration_seconds",
				Help:    "Duration of terminal Druid deployment lifecycle runs.",
				Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600},
			},
			[]string{"namespace", "druid_instance", "result", "trigger"},
		),
		inProgress: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_deployment_lifecycle_in_progress",
				Help: "Current number of active Druid deployment lifecycle runs.",
			},
			[]string{"namespace", "druid_instance", "trigger"},
		),
	}

	if registerer != nil {
		registerer.MustRegister(
			metrics.transitions,
			metrics.completions,
			metrics.duration,
			metrics.inProgress,
		)
	}

	return metrics
}

func (m *deploymentLifecycleMetrics) recordTransition(namespace, druidInstance string, previous, current v1alpha1.DeploymentLifecycleStatus) {
	if m == nil {
		return
	}

	previousActive := isActiveLifecyclePhase(previous.Phase)
	currentActive := isActiveLifecyclePhase(current.Phase)
	previousTrigger := lifecycleTriggerLabel(previous.Trigger)
	currentTrigger := lifecycleTriggerLabel(current.Trigger)

	switch {
	case previousActive && currentActive && previousTrigger != currentTrigger:
		m.inProgress.WithLabelValues(namespace, druidInstance, previousTrigger).Dec()
		m.inProgress.WithLabelValues(namespace, druidInstance, currentTrigger).Inc()
	case !previousActive && currentActive:
		m.inProgress.WithLabelValues(namespace, druidInstance, currentTrigger).Inc()
	case previousActive && !currentActive:
		m.inProgress.WithLabelValues(namespace, druidInstance, previousTrigger).Dec()
	}

	if previous.Phase == current.Phase {
		return
	}

	phase := lifecyclePhaseLabel(current.Phase)
	m.transitions.WithLabelValues(namespace, druidInstance, phase, currentTrigger).Inc()

	if !isTerminalLifecyclePhase(current.Phase) {
		return
	}

	result := lifecycleResultLabel(current.Phase)
	m.completions.WithLabelValues(namespace, druidInstance, result, currentTrigger).Inc()
	startedAt := current.StartedAt
	if startedAt == nil {
		startedAt = previous.StartedAt
	}
	if startedAt == nil || current.CompletedAt == nil {
		return
	}

	durationSeconds := current.CompletedAt.Sub(startedAt.Time).Seconds()
	if durationSeconds < 0 {
		durationSeconds = 0
	}
	m.duration.WithLabelValues(namespace, druidInstance, result, currentTrigger).Observe(durationSeconds)
}

func (m *deploymentLifecycleMetrics) deleteCluster(namespace, druidInstance string) {
	if m == nil {
		return
	}

	for _, trigger := range deploymentLifecycleTriggers {
		m.inProgress.DeleteLabelValues(namespace, druidInstance, trigger)
		for _, phase := range deploymentLifecyclePhases {
			m.transitions.DeleteLabelValues(namespace, druidInstance, phase, trigger)
		}
		for _, result := range deploymentLifecycleResults {
			m.completions.DeleteLabelValues(namespace, druidInstance, result, trigger)
			m.duration.DeleteLabelValues(namespace, druidInstance, result, trigger)
		}
	}
}

func isActiveLifecyclePhase(phase v1alpha1.DeploymentLifecyclePhase) bool {
	return phase == v1alpha1.DeploymentLifecyclePending || phase == v1alpha1.DeploymentLifecycleInProgress
}

func isTerminalLifecyclePhase(phase v1alpha1.DeploymentLifecyclePhase) bool {
	return phase == v1alpha1.DeploymentLifecycleSucceeded || phase == v1alpha1.DeploymentLifecycleFailed
}

func lifecyclePhaseLabel(phase v1alpha1.DeploymentLifecyclePhase) string {
	switch phase {
	case v1alpha1.DeploymentLifecyclePending:
		return "pending"
	case v1alpha1.DeploymentLifecycleInProgress:
		return "in_progress"
	case v1alpha1.DeploymentLifecycleSucceeded:
		return "succeeded"
	case v1alpha1.DeploymentLifecycleFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func lifecycleResultLabel(phase v1alpha1.DeploymentLifecyclePhase) string {
	switch phase {
	case v1alpha1.DeploymentLifecycleSucceeded:
		return "succeeded"
	case v1alpha1.DeploymentLifecycleFailed:
		return "failed"
	default:
		return "unknown"
	}
}

func lifecycleTriggerLabel(trigger v1alpha1.DeploymentLifecycleTrigger) string {
	switch trigger {
	case v1alpha1.DeploymentTriggerSpecChange:
		return "spec_change"
	case v1alpha1.DeploymentTriggerImageChange:
		return "image_change"
	case v1alpha1.DeploymentTriggerManualRollout:
		return "manual_rollout"
	default:
		return "unknown"
	}
}

type druidRolloutMetrics struct {
	clusterFullyDeployed *prometheus.GaugeVec
	clusterPhase         *prometheus.GaugeVec

	workloadRolloutInProgress  *prometheus.GaugeVec
	workloadRolloutDuration    *prometheus.GaugeVec
	workloadDesiredReplicas    *prometheus.GaugeVec
	workloadUpdatedReplicas    *prometheus.GaugeVec
	workloadReadyReplicas      *prometheus.GaugeVec
	workloadObservedGeneration *prometheus.GaugeVec
	workloadGeneration         *prometheus.GaugeVec
	workloadForceDeleteHealthy *prometheus.GaugeVec
	workloadForceDeleteActions *prometheus.CounterVec

	mu                      sync.Mutex
	activeRolloutStarts     map[string]time.Time
	knownWorkloads          map[string]map[string]workloadMetricLabels
	knownForceDeleteChecks  map[string]map[string]workloadMetricLabels
	knownForceDeleteActions map[string]map[string]forceDeleteActionMetricLabels
}

type workloadMetricLabels struct {
	namespace     string
	druidInstance string
	workload      string
	nodeType      string
	kind          string
}

type workloadRolloutSnapshot struct {
	labels             workloadMetricLabels
	desiredReplicas    int32
	updatedReplicas    int32
	readyReplicas      int32
	observedGeneration int64
	generation         int64
	inProgress         bool
}

type forceDeleteCandidate struct {
	podName       string
	containerName string
	reason        string
}

type forceDeleteActionMetricLabels struct {
	namespace     string
	druidInstance string
	workload      string
	nodeType      string
	reason        string
}

var deploymentLifecyclePhases = []string{"pending", "in_progress", "succeeded", "failed", "unknown"}

var defaultDruidRolloutMetrics = newDruidRolloutMetrics(ctrlmetrics.Registry)

func newDruidRolloutMetrics(registerer prometheus.Registerer) *druidRolloutMetrics {
	metrics := &druidRolloutMetrics{
		clusterFullyDeployed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_cluster_fully_deployed",
				Help: "Whether the Druid cluster deployment lifecycle is fully deployed.",
			},
			[]string{"namespace", "druid_instance"},
		),
		clusterPhase: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_cluster_deployment_phase",
				Help: "One-hot gauge for the current Druid cluster deployment lifecycle phase.",
			},
			[]string{"namespace", "druid_instance", "phase"},
		),
		workloadRolloutInProgress: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_rollout_in_progress",
				Help: "Whether a managed Druid workload is currently rolling out.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadRolloutDuration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_rollout_duration_seconds",
				Help: "Last observed duration of the current managed Druid workload rollout at reconcile time.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadDesiredReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_desired_replicas",
				Help: "Desired replicas for a managed Druid workload.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadUpdatedReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_updated_replicas",
				Help: "Updated replicas reported by a managed Druid workload.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadReadyReplicas: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_ready_replicas",
				Help: "Ready replicas reported by a managed Druid workload.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadObservedGeneration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_observed_generation",
				Help: "Observed generation reported by a managed Druid workload controller.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadGeneration: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_generation",
				Help: "Metadata generation for a managed Druid workload.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadForceDeleteHealthy: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "druid_operator_workload_force_delete_healthcheck_healthy",
				Help: "Whether the OrderedReady StatefulSet force-delete health check is currently passing.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "kind"},
		),
		workloadForceDeleteActions: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "druid_operator_workload_force_delete_actions_total",
				Help: "Total number of pod force-delete remediation actions taken by the operator.",
			},
			[]string{"namespace", "druid_instance", "workload", "node_type", "reason"},
		),
		activeRolloutStarts:     map[string]time.Time{},
		knownWorkloads:          map[string]map[string]workloadMetricLabels{},
		knownForceDeleteChecks:  map[string]map[string]workloadMetricLabels{},
		knownForceDeleteActions: map[string]map[string]forceDeleteActionMetricLabels{},
	}

	if registerer != nil {
		registerer.MustRegister(
			metrics.clusterFullyDeployed,
			metrics.clusterPhase,
			metrics.workloadRolloutInProgress,
			metrics.workloadRolloutDuration,
			metrics.workloadDesiredReplicas,
			metrics.workloadUpdatedReplicas,
			metrics.workloadReadyReplicas,
			metrics.workloadObservedGeneration,
			metrics.workloadGeneration,
			metrics.workloadForceDeleteHealthy,
			metrics.workloadForceDeleteActions,
		)
	}

	return metrics
}

func (m *druidRolloutMetrics) sync(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid) error {
	return m.syncWithNow(ctx, sdk, drd, time.Now())
}

func (m *druidRolloutMetrics) syncWithNow(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, now time.Time) error {
	if m == nil || drd == nil {
		return nil
	}

	workloadSnapshots, err := m.collectWorkloadSnapshots(ctx, sdk, drd)
	if err != nil {
		return err
	}
	forceDeleteChecks, err := m.collectForceDeleteChecks(ctx, sdk, drd)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	clusterKey := metricClusterKey(drd.Namespace, drd.Name)
	m.updateClusterState(drd)

	currentWorkloads := map[string]workloadMetricLabels{}
	for workloadKey, snapshot := range workloadSnapshots {
		currentWorkloads[workloadKey] = snapshot.labels
		m.updateWorkloadSnapshot(workloadKey, snapshot, now)
	}

	for workloadKey, labels := range m.knownWorkloads[clusterKey] {
		if _, exists := currentWorkloads[workloadKey]; exists {
			continue
		}
		m.deleteWorkloadMetrics(workloadKey, labels)
	}
	m.knownWorkloads[clusterKey] = currentWorkloads

	currentChecks := map[string]workloadMetricLabels{}
	for workloadKey, check := range forceDeleteChecks {
		currentChecks[workloadKey] = check.labels
		m.workloadForceDeleteHealthy.WithLabelValues(check.labels.values()...).Set(boolToFloat(check.healthy))
	}
	for workloadKey, labels := range m.knownForceDeleteChecks[clusterKey] {
		if _, exists := currentChecks[workloadKey]; exists {
			continue
		}
		m.workloadForceDeleteHealthy.DeleteLabelValues(labels.values()...)
	}
	m.knownForceDeleteChecks[clusterKey] = currentChecks

	return nil
}

func (m *druidRolloutMetrics) deleteCluster(namespace, druidName string) {
	if m == nil {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	clusterKey := metricClusterKey(namespace, druidName)
	m.clusterFullyDeployed.DeleteLabelValues(namespace, druidName)
	for _, phase := range deploymentLifecyclePhases {
		m.clusterPhase.DeleteLabelValues(namespace, druidName, phase)
	}

	for workloadKey, labels := range m.knownWorkloads[clusterKey] {
		m.deleteWorkloadMetrics(workloadKey, labels)
	}
	delete(m.knownWorkloads, clusterKey)

	for _, labels := range m.knownForceDeleteChecks[clusterKey] {
		m.workloadForceDeleteHealthy.DeleteLabelValues(labels.values()...)
	}
	delete(m.knownForceDeleteChecks, clusterKey)

	for _, labels := range m.knownForceDeleteActions[clusterKey] {
		m.workloadForceDeleteActions.DeleteLabelValues(labels.values()...)
	}
	delete(m.knownForceDeleteActions, clusterKey)
}

func (m *druidRolloutMetrics) recordForceDeleteAction(
	namespace, druidInstance, workload, nodeType, reason string,
) {
	if m == nil {
		return
	}

	labels := forceDeleteActionMetricLabels{
		namespace:     namespace,
		druidInstance: druidInstance,
		workload:      workload,
		nodeType:      nodeType,
		reason:        reason,
	}

	m.mu.Lock()
	clusterKey := metricClusterKey(namespace, druidInstance)
	if _, exists := m.knownForceDeleteActions[clusterKey]; !exists {
		m.knownForceDeleteActions[clusterKey] = map[string]forceDeleteActionMetricLabels{}
	}
	m.knownForceDeleteActions[clusterKey][metricForceDeleteActionKey(labels)] = labels
	m.mu.Unlock()

	m.workloadForceDeleteActions.WithLabelValues(labels.values()...).Inc()
}

func (m *druidRolloutMetrics) updateClusterState(drd *v1alpha1.Druid) {
	phase := lifecyclePhaseLabel(drd.Status.DeploymentLifecycle.Phase)
	for _, candidate := range deploymentLifecyclePhases {
		value := 0.0
		if candidate == phase {
			value = 1
		}
		m.clusterPhase.WithLabelValues(drd.Namespace, drd.Name, candidate).Set(value)
	}

	m.clusterFullyDeployed.WithLabelValues(drd.Namespace, drd.Name).Set(boolToFloat(
		drd.Status.DeploymentLifecycle.Phase == v1alpha1.DeploymentLifecycleSucceeded,
	))
}

func (m *druidRolloutMetrics) updateWorkloadSnapshot(
	workloadKey string,
	snapshot workloadRolloutSnapshot,
	now time.Time,
) {
	values := snapshot.labels.values()
	m.workloadDesiredReplicas.WithLabelValues(values...).Set(float64(snapshot.desiredReplicas))
	m.workloadUpdatedReplicas.WithLabelValues(values...).Set(float64(snapshot.updatedReplicas))
	m.workloadReadyReplicas.WithLabelValues(values...).Set(float64(snapshot.readyReplicas))
	m.workloadObservedGeneration.WithLabelValues(values...).Set(float64(snapshot.observedGeneration))
	m.workloadGeneration.WithLabelValues(values...).Set(float64(snapshot.generation))

	if !snapshot.inProgress {
		delete(m.activeRolloutStarts, workloadKey)
		m.workloadRolloutInProgress.WithLabelValues(values...).Set(0)
		m.workloadRolloutDuration.WithLabelValues(values...).Set(0)
		return
	}

	startedAt := m.rolloutStartTime(workloadKey, now)
	durationSeconds := now.Sub(startedAt).Seconds()
	if durationSeconds < 0 {
		durationSeconds = 0
	}
	m.workloadRolloutInProgress.WithLabelValues(values...).Set(1)
	m.workloadRolloutDuration.WithLabelValues(values...).Set(durationSeconds)
}

func (m *druidRolloutMetrics) rolloutStartTime(workloadKey string, now time.Time) time.Time {
	startedAt, exists := m.activeRolloutStarts[workloadKey]
	if exists {
		return startedAt
	}

	m.activeRolloutStarts[workloadKey] = now
	return now
}

func (m *druidRolloutMetrics) deleteWorkloadMetrics(workloadKey string, labels workloadMetricLabels) {
	delete(m.activeRolloutStarts, workloadKey)
	values := labels.values()
	m.workloadRolloutInProgress.DeleteLabelValues(values...)
	m.workloadRolloutDuration.DeleteLabelValues(values...)
	m.workloadDesiredReplicas.DeleteLabelValues(values...)
	m.workloadUpdatedReplicas.DeleteLabelValues(values...)
	m.workloadReadyReplicas.DeleteLabelValues(values...)
	m.workloadObservedGeneration.DeleteLabelValues(values...)
	m.workloadGeneration.DeleteLabelValues(values...)
}

func (m *druidRolloutMetrics) collectWorkloadSnapshots(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
) (map[string]workloadRolloutSnapshot, error) {
	listOpts := []client.ListOption{
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(makeLabelsForDruid(drd)),
	}

	snapshots := map[string]workloadRolloutSnapshot{}

	deployments := &appsv1.DeploymentList{}
	if err := sdk.List(ctx, deployments, listOpts...); err != nil {
		return nil, err
	}
	for _, deployment := range deployments.Items {
		snapshot := workloadRolloutSnapshot{
			labels:             labelsForManagedObject(drd, &deployment, "Deployment"),
			desiredReplicas:    desiredReplicaCount(deployment.Spec.Replicas),
			updatedReplicas:    deployment.Status.UpdatedReplicas,
			readyReplicas:      deployment.Status.ReadyReplicas,
			observedGeneration: deployment.Status.ObservedGeneration,
			generation:         deployment.Generation,
			inProgress:         deploymentRolloutInProgress(&deployment),
		}
		snapshots[metricWorkloadKey(snapshot.labels)] = snapshot
	}

	statefulSets := &appsv1.StatefulSetList{}
	if err := sdk.List(ctx, statefulSets, listOpts...); err != nil {
		return nil, err
	}
	for _, statefulSet := range statefulSets.Items {
		snapshot := workloadRolloutSnapshot{
			labels:             labelsForManagedObject(drd, &statefulSet, "StatefulSet"),
			desiredReplicas:    desiredReplicaCount(statefulSet.Spec.Replicas),
			updatedReplicas:    statefulSet.Status.UpdatedReplicas,
			readyReplicas:      statefulSet.Status.ReadyReplicas,
			observedGeneration: statefulSet.Status.ObservedGeneration,
			generation:         statefulSet.Generation,
			inProgress:         statefulSetRolloutInProgress(&statefulSet),
		}
		snapshots[metricWorkloadKey(snapshot.labels)] = snapshot
	}

	return snapshots, nil
}

type forceDeleteHealthCheck struct {
	labels  workloadMetricLabels
	healthy bool
}

func (m *druidRolloutMetrics) collectForceDeleteChecks(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
) (map[string]forceDeleteHealthCheck, error) {
	checks := map[string]forceDeleteHealthCheck{}
	if !drd.Spec.ForceDeleteStsPodOnError {
		return checks, nil
	}

	for key, nodeSpec := range drd.Spec.Nodes {
		if nodeSpec.PodManagementPolicy != appsv1.OrderedReadyPodManagement {
			continue
		}
		kind := firstNonEmptyStr(nodeSpec.Kind, "StatefulSet")
		if kind != "StatefulSet" {
			continue
		}

		nodeSpecUniqueStr := makeNodeSpecificUniqueString(drd, key)
		labels := workloadMetricLabels{
			namespace:     drd.Namespace,
			druidInstance: drd.Name,
			workload:      nodeSpecUniqueStr,
			nodeType:      nodeSpec.NodeType,
			kind:          "StatefulSet",
		}
		pods, err := listNodePods(ctx, sdk, &nodeSpec, drd, nodeSpecUniqueStr)
		if err != nil {
			return nil, err
		}
		checks[metricWorkloadKey(labels)] = forceDeleteHealthCheck{
			labels:  labels,
			healthy: len(findForceDeleteCandidates(pods)) == 0,
		}
	}

	return checks, nil
}

func listNodePods(
	ctx context.Context,
	sdk client.Client,
	nodeSpec *v1alpha1.DruidNodeSpec,
	drd *v1alpha1.Druid,
	nodeSpecUniqueStr string,
) ([]object, error) {
	podList := &v1.PodList{}
	if err := sdk.List(ctx, podList,
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(makeLabelsForNodeSpec(nodeSpec, drd, drd.Name, nodeSpecUniqueStr)),
	); err != nil {
		return nil, err
	}

	result := make([]object, len(podList.Items))
	for i := range podList.Items {
		result[i] = &podList.Items[i]
	}
	return result, nil
}

func findForceDeleteCandidates(podList []object) []forceDeleteCandidate {
	candidates := []forceDeleteCandidate{}

	for _, podObj := range podList {
		pod := podObj.(*v1.Pod)
		if pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodUnknown {
			candidates = append(candidates, forceDeleteCandidate{
				podName: pod.Name,
				reason:  "pod_phase",
			})
			continue
		}

		containersReadyFalse := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == v1.ContainersReady && condition.Status == v1.ConditionFalse {
				containersReadyFalse = true
				break
			}
		}
		if !containersReadyFalse {
			continue
		}

		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.RestartCount > 1 {
				candidates = append(candidates, forceDeleteCandidate{
					podName:       pod.Name,
					containerName: containerStatus.Name,
					reason:        "crashloop",
				})
				break
			}
		}
	}

	return candidates
}

func labelsForManagedObject(
	drd *v1alpha1.Druid,
	obj metav1.Object,
	kind string,
) workloadMetricLabels {
	nodeType := obj.GetLabels()["component"]
	if nodeType == "" {
		nodeType = "unknown"
	}

	return workloadMetricLabels{
		namespace:     drd.Namespace,
		druidInstance: drd.Name,
		workload:      obj.GetName(),
		nodeType:      nodeType,
		kind:          kind,
	}
}

func (l workloadMetricLabels) values() []string {
	return []string{l.namespace, l.druidInstance, l.workload, l.nodeType, l.kind}
}

func (l forceDeleteActionMetricLabels) values() []string {
	return []string{l.namespace, l.druidInstance, l.workload, l.nodeType, l.reason}
}

func metricClusterKey(namespace, druidInstance string) string {
	return fmt.Sprintf("%s/%s", namespace, druidInstance)
}

func metricWorkloadKey(labels workloadMetricLabels) string {
	return fmt.Sprintf("%s/%s/%s/%s", labels.namespace, labels.druidInstance, labels.kind, labels.workload)
}

func metricForceDeleteActionKey(labels forceDeleteActionMetricLabels) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", labels.namespace, labels.druidInstance, labels.workload, labels.nodeType, labels.reason)
}

func boolToFloat(value bool) float64 {
	if value {
		return 1
	}
	return 0
}

func desiredReplicaCount(replicas *int32) int32 {
	if replicas == nil {
		return 1
	}
	return *replicas
}

func deploymentRolloutInProgress(deployment *appsv1.Deployment) bool {
	desiredReplicas := desiredReplicaCount(deployment.Spec.Replicas)
	return deployment.Status.ObservedGeneration < deployment.Generation ||
		deployment.Status.UpdatedReplicas != desiredReplicas ||
		deployment.Status.ReadyReplicas != desiredReplicas ||
		deployment.Status.Replicas != desiredReplicas ||
		deployment.Status.UnavailableReplicas != 0
}

func statefulSetRolloutInProgress(statefulSet *appsv1.StatefulSet) bool {
	desiredReplicas := desiredReplicaCount(statefulSet.Spec.Replicas)
	return statefulSet.Status.ObservedGeneration < statefulSet.Generation ||
		statefulSet.Status.CurrentRevision != statefulSet.Status.UpdateRevision ||
		statefulSet.Status.UpdatedReplicas != desiredReplicas ||
		statefulSet.Status.ReadyReplicas != desiredReplicas
}
