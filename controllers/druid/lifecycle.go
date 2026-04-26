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
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	druidapi "github.com/apache/druid-operator/pkg/druidapi"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var errDeploymentLifecycleTerminal = errors.New("terminal deployment lifecycle failure")

type terminalDeploymentLifecycleError struct {
	err error
}

func (e terminalDeploymentLifecycleError) Error() string {
	return e.err.Error()
}

func (e terminalDeploymentLifecycleError) Unwrap() error {
	return e.err
}

func (e terminalDeploymentLifecycleError) Is(target error) bool {
	return target == errDeploymentLifecycleTerminal
}

type lifecycleRevisionInput struct {
	Generation int64              `json:"generation"`
	Spec       v1alpha1.DruidSpec `json:"spec"`
}

type lifecycleDependencies struct {
	now               func() time.Time
	getBuildRevisions func(context.Context, client.Client, string, string, druidapi.Auth) ([]string, error)
}

func defaultLifecycleDependencies() lifecycleDependencies {
	return lifecycleDependencies{
		now:               time.Now,
		getBuildRevisions: druidapi.GetClusterBuildRevisions,
	}
}

type deploymentLifecycleInputs struct {
	Image              string
	ForceRedeployToken string
	ImageInputsHash    string
}

func computeDeploymentLifecycleRevision(drd *v1alpha1.Druid) (string, error) {
	specForRevision := drd.Spec.DeepCopy()
	specForRevision.ExpectedBuildRevision = ""

	payload, err := json.Marshal(lifecycleRevisionInput{
		Generation: drd.Generation,
		Spec:       *specForRevision,
	})
	if err != nil {
		return "", err
	}

	sum := sha1.Sum(payload)
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

func currentDeploymentLifecycleInputs(drd *v1alpha1.Druid) (deploymentLifecycleInputs, error) {
	imageInputsHash, err := computeEffectiveImageInputsHash(drd)
	if err != nil {
		return deploymentLifecycleInputs{}, err
	}

	return deploymentLifecycleInputs{
		Image:              drd.Spec.Image,
		ForceRedeployToken: drd.Spec.ForceRedeployToken,
		ImageInputsHash:    imageInputsHash,
	}, nil
}

type effectiveImageInput struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

func computeEffectiveImageInputsHash(drd *v1alpha1.Druid) (string, error) {
	inputs := make([]effectiveImageInput, 0, len(drd.Spec.Nodes)+1)
	inputs = append(inputs, effectiveImageInput{
		Name:  "__default__/druid",
		Image: drd.Spec.Image,
	})
	inputs = appendEffectiveAdditionalContainerImages(inputs, "cluster", drd.Spec.AdditionalContainer)

	nodeNames := make([]string, 0, len(drd.Spec.Nodes))
	for nodeName := range drd.Spec.Nodes {
		nodeNames = append(nodeNames, nodeName)
	}
	sort.Strings(nodeNames)

	for _, nodeName := range nodeNames {
		nodeSpec := drd.Spec.Nodes[nodeName]
		inputs = append(inputs, effectiveImageInput{
			Name:  fmt.Sprintf("node/%s/druid", nodeName),
			Image: firstNonEmptyStr(nodeSpec.Image, drd.Spec.Image),
		})
		inputs = appendEffectiveAdditionalContainerImages(inputs, "node/"+nodeName, nodeSpec.AdditionalContainer)
	}

	payload, err := json.Marshal(inputs)
	if err != nil {
		return "", err
	}

	sum := sha1.Sum(payload)
	return base64.StdEncoding.EncodeToString(sum[:]), nil
}

func determineDeploymentTrigger(
	current deploymentLifecycleInputs,
	lastSuccessful v1alpha1.DeploymentLifecycleStatus,
) v1alpha1.DeploymentLifecycleTrigger {
	if !hasDeploymentLifecycleBaseline(lastSuccessful) {
		return v1alpha1.DeploymentTriggerSpecChange
	}

	if current.ForceRedeployToken != lastSuccessful.LastSuccessfulForceRedeployToken {
		return v1alpha1.DeploymentTriggerManualRollout
	}

	lastSuccessfulImageHash := lastSuccessful.LastSuccessfulImageInputsHash
	if lastSuccessfulImageHash == "" {
		lastSuccessfulImageHash = computeLegacyImageInputsHash(lastSuccessful)
	}

	if lastSuccessfulImageHash != "" &&
		current.ImageInputsHash != "" &&
		current.ImageInputsHash != lastSuccessfulImageHash {
		return v1alpha1.DeploymentTriggerImageChange
	}

	if current.Image != lastSuccessful.LastSuccessfulImage {
		return v1alpha1.DeploymentTriggerImageChange
	}

	return v1alpha1.DeploymentTriggerSpecChange
}

func appendEffectiveAdditionalContainerImages(
	inputs []effectiveImageInput,
	scope string,
	containers []v1alpha1.AdditionalContainer,
) []effectiveImageInput {
	for _, container := range containers {
		containerType := "sidecar"
		if container.RunAsInit {
			containerType = "init"
		}

		inputs = append(inputs, effectiveImageInput{
			Name:  fmt.Sprintf("%s/%s/%s", scope, containerType, container.ContainerName),
			Image: container.Image,
		})
	}
	return inputs
}

func computeLegacyImageInputsHash(lastSuccessful v1alpha1.DeploymentLifecycleStatus) string {
	payload, err := json.Marshal([]effectiveImageInput{{
		Name:  "__default__/druid",
		Image: lastSuccessful.LastSuccessfulImage,
	}})
	if err != nil {
		return ""
	}

	sum := sha1.Sum(payload)
	return base64.StdEncoding.EncodeToString(sum[:])
}

func hasDeploymentLifecycleBaseline(status v1alpha1.DeploymentLifecycleStatus) bool {
	return status.LastSuccessfulImage != "" ||
		status.LastSuccessfulImageInputsHash != "" ||
		status.LastSuccessfulForceRedeployToken != "" ||
		status.CompletedAt != nil
}

func markTerminalDeploymentLifecycleError(err error) error {
	if err == nil {
		return nil
	}
	return terminalDeploymentLifecycleError{err: err}
}

func shouldPersistDeploymentLifecycleFailure(err error) bool {
	return errors.Is(err, errDeploymentLifecycleTerminal)
}

func requiresBuildRevisionValidation(trigger v1alpha1.DeploymentLifecycleTrigger) bool {
	return trigger == v1alpha1.DeploymentTriggerImageChange || trigger == v1alpha1.DeploymentTriggerManualRollout
}

func ensureDeploymentLifecycleStarted(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
) error {
	return ensureDeploymentLifecycleStartedWithDeps(
		ctx,
		sdk,
		drd,
		emitEvent,
		defaultLifecycleDependencies(),
	)
}

func ensureDeploymentLifecycleStartedWithDeps(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	deps lifecycleDependencies,
) error {
	desiredRevision, err := computeDeploymentLifecycleRevision(drd)
	if err != nil {
		return err
	}

	current := drd.Status.DeploymentLifecycle
	if current.Revision == desiredRevision && current.Phase != "" {
		return nil
	}

	updated, err := buildLifecycleStatusForRevision(drd, current, desiredRevision)
	if err != nil {
		return err
	}
	updated.Phase = v1alpha1.DeploymentLifecyclePending
	updated.Reason = "Waiting for Druid workloads to roll out"
	now := metav1.NewTime(deps.now())
	updated.StartedAt = &now

	return patchDeploymentLifecycleStatus(ctx, sdk, drd, updated, emitEvent)
}

func markDeploymentLifecycleFailed(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	err error,
) error {
	return markDeploymentLifecycleFailedWithDeps(
		ctx,
		sdk,
		drd,
		emitEvent,
		err,
		defaultLifecycleDependencies(),
	)
}

func markDeploymentLifecycleFailedWithDeps(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	err error,
	deps lifecycleDependencies,
) error {
	current := drd.Status.DeploymentLifecycle
	desiredRevision, revisionErr := computeDeploymentLifecycleRevision(drd)
	if revisionErr != nil {
		return revisionErr
	}
	updated, buildErr := buildLifecycleStatusForRevision(drd, current, desiredRevision)
	if buildErr != nil {
		return buildErr
	}
	if updated.StartedAt == nil {
		now := metav1.NewTime(deps.now())
		updated.StartedAt = &now
	}
	updated.Phase = v1alpha1.DeploymentLifecycleFailed
	updated.Reason = err.Error()
	completedAt := metav1.NewTime(deps.now())
	updated.CompletedAt = &completedAt

	return patchDeploymentLifecycleStatus(ctx, sdk, drd, updated, emitEvent)
}

func reconcileDeploymentLifecycle(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
) error {
	return reconcileDeploymentLifecycleWithDeps(
		ctx,
		sdk,
		drd,
		emitEvent,
		defaultLifecycleDependencies(),
	)
}

func reconcileDeploymentLifecycleWithDeps(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	deps lifecycleDependencies,
) error {
	desiredRevision, err := computeDeploymentLifecycleRevision(drd)
	if err != nil {
		return err
	}

	updated, err := buildLifecycleStatusForRevision(drd, drd.Status.DeploymentLifecycle, desiredRevision)
	if err != nil {
		return err
	}
	if updated.StartedAt == nil {
		now := metav1.NewTime(deps.now())
		updated.StartedAt = &now
	}
	inputs, err := currentDeploymentLifecycleInputs(drd)
	if err != nil {
		return err
	}

	workloadsReady, reason, err := areManagedWorkloadsReady(ctx, sdk, drd)
	if err != nil {
		return err
	}
	if !workloadsReady {
		return patchDeploymentLifecycleStatus(ctx, sdk, drd, withDeploymentLifecycleInProgress(updated, reason), emitEvent)
	}

	podsReady, reason, err := areManagedPodsReady(ctx, sdk, drd)
	if err != nil {
		return err
	}
	if !podsReady {
		return patchDeploymentLifecycleStatus(ctx, sdk, drd, withDeploymentLifecycleInProgress(updated, reason), emitEvent)
	}

	buildRevisionValidationRequired := requiresBuildRevisionValidation(updated.Trigger)
	if buildRevisionValidationRequired {
		if drd.Spec.ExpectedBuildRevision == "" {
			updated = withDeploymentLifecycleInProgress(updated, "Waiting for spec.expectedBuildRevision before completing image rollout")
			updated.ObservedBuildRevisions = nil
			return patchDeploymentLifecycleStatus(ctx, sdk, drd, updated, emitEvent)
		}

		revisions, err := deps.getBuildRevisions(ctx, sdk, drd.Namespace, drd.Name, drd.Spec.Auth)
		if err != nil {
			var pendingErr *druidapi.BuildRevisionLookupPendingError
			if errors.As(err, &pendingErr) {
				updated.ObservedBuildRevisions = nil
				return patchDeploymentLifecycleStatus(ctx, sdk, drd, withDeploymentLifecycleInProgress(updated, buildRevisionPendingMessage(pendingErr)), emitEvent)
			}
			if errors.Is(err, context.Canceled) {
				return err
			}
			return markTerminalDeploymentLifecycleError(err)
		}

		updated.ObservedBuildRevisions = append([]string(nil), revisions...)
		if len(revisions) == 0 {
			return patchDeploymentLifecycleStatus(ctx, sdk, drd, withDeploymentLifecycleInProgress(updated, "Waiting for Druid servers to report a build revision"), emitEvent)
		}

		if len(revisions) != 1 || revisions[0] != drd.Spec.ExpectedBuildRevision {
			return patchDeploymentLifecycleStatus(
				ctx,
				sdk,
				drd,
				withDeploymentLifecycleInProgress(updated, fmt.Sprintf("Waiting for all Druid servers to report build revision [%s]", drd.Spec.ExpectedBuildRevision)),
				emitEvent,
			)
		}
	} else {
		updated.ObservedBuildRevisions = nil
	}

	if updated.Phase == v1alpha1.DeploymentLifecycleSucceeded {
		return nil
	}

	updated = withDeploymentLifecycleSucceeded(updated, drd.Spec.ExpectedBuildRevision, buildRevisionValidationRequired, deps.now())
	updated.LastSuccessfulImage = inputs.Image
	updated.LastSuccessfulImageInputsHash = inputs.ImageInputsHash
	updated.LastSuccessfulForceRedeployToken = inputs.ForceRedeployToken
	return patchDeploymentLifecycleStatus(ctx, sdk, drd, updated, emitEvent)
}

func withDeploymentLifecycleInProgress(
	status v1alpha1.DeploymentLifecycleStatus,
	reason string,
) v1alpha1.DeploymentLifecycleStatus {
	status.Phase = v1alpha1.DeploymentLifecycleInProgress
	status.Reason = reason
	status.CompletedAt = nil
	return status
}

func withDeploymentLifecycleSucceeded(
	status v1alpha1.DeploymentLifecycleStatus,
	expectedBuildRevision string,
	buildRevisionValidated bool,
	now time.Time,
) v1alpha1.DeploymentLifecycleStatus {
	completedAt := metav1.NewTime(now)
	status.Phase = v1alpha1.DeploymentLifecycleSucceeded
	if buildRevisionValidated {
		status.Reason = fmt.Sprintf("Deployment lifecycle completed for build revision [%s]", expectedBuildRevision)
	} else {
		status.Reason = "Deployment lifecycle completed after managed workloads became ready"
	}
	status.CompletedAt = &completedAt
	return status
}

func buildLifecycleStatusForRevision(
	drd *v1alpha1.Druid,
	current v1alpha1.DeploymentLifecycleStatus,
	desiredRevision string,
) (v1alpha1.DeploymentLifecycleStatus, error) {
	inputs, err := currentDeploymentLifecycleInputs(drd)
	if err != nil {
		return v1alpha1.DeploymentLifecycleStatus{}, err
	}

	updated := current
	updated.Revision = desiredRevision
	updated.ObservedGeneration = drd.Generation
	updated.Trigger = determineDeploymentTrigger(inputs, current)
	updated.ExpectedBuildRevision = drd.Spec.ExpectedBuildRevision
	updated.CompletedAt = nil
	if updated.Revision != current.Revision {
		updated.StartedAt = nil
		updated.ObservedBuildRevisions = nil
	}
	return updated, nil
}

func buildRevisionPendingMessage(err *druidapi.BuildRevisionLookupPendingError) string {
	switch err.Reason {
	case druidapi.BuildRevisionPendingQueryServiceDiscovery:
		return "Waiting for Druid query service discovery"
	case druidapi.BuildRevisionPendingQueryEndpoint:
		return "Waiting for the Druid build revision query endpoint"
	case druidapi.BuildRevisionPendingQueryResponse:
		return "Waiting for the Druid build revision query to succeed"
	default:
		return "Waiting for Druid build revision verification"
	}
}

func areManagedWorkloadsReady(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid) (bool, string, error) {
	listOpts := []client.ListOption{
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(makeLabelsForDruid(drd)),
	}

	deployments := &appsv1.DeploymentList{}
	if err := sdk.List(ctx, deployments, listOpts...); err != nil {
		return false, "", err
	}
	for _, deployment := range deployments.Items {
		for _, condition := range deployment.Status.Conditions {
			if condition.Type == appsv1.DeploymentReplicaFailure {
				return false, "", markTerminalDeploymentLifecycleError(fmt.Errorf("deployment [%s] replica failure: %s", deployment.Name, condition.Reason))
			}
		}
		specReplicas := int32(1)
		if deployment.Spec.Replicas != nil {
			specReplicas = *deployment.Spec.Replicas
		}
		if deployment.Status.ObservedGeneration < deployment.Generation {
			return false, fmt.Sprintf("Waiting for Deployment [%s] controller to observe generation", deployment.Name), nil
		}
		if deployment.Status.UpdatedReplicas != specReplicas ||
			deployment.Status.ReadyReplicas != specReplicas ||
			deployment.Status.Replicas != specReplicas ||
			deployment.Status.UnavailableReplicas != 0 {
			return false, fmt.Sprintf("Waiting for Deployment [%s] rollout", deployment.Name), nil
		}
	}

	statefulSets := &appsv1.StatefulSetList{}
	if err := sdk.List(ctx, statefulSets, listOpts...); err != nil {
		return false, "", err
	}
	for _, statefulSet := range statefulSets.Items {
		specReplicas := int32(1)
		if statefulSet.Spec.Replicas != nil {
			specReplicas = *statefulSet.Spec.Replicas
		}
		if statefulSet.Status.ObservedGeneration < statefulSet.Generation {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] controller to observe generation", statefulSet.Name), nil
		}
		if statefulSet.Status.CurrentRevision != statefulSet.Status.UpdateRevision ||
			statefulSet.Status.UpdatedReplicas != specReplicas {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] revision rollout", statefulSet.Name), nil
		}
		if statefulSet.Status.ReadyReplicas != specReplicas {
			return false, fmt.Sprintf("Waiting for StatefulSet [%s] ready replicas", statefulSet.Name), nil
		}
	}

	return true, "", nil
}

func areManagedPodsReady(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid) (bool, string, error) {
	pods := &v1.PodList{}
	if err := sdk.List(ctx, pods,
		client.InNamespace(drd.Namespace),
		client.MatchingLabels(makeLabelsForDruid(drd)),
	); err != nil {
		return false, "", err
	}

	if len(pods.Items) == 0 {
		return false, "Waiting for Druid pods to be created", nil
	}

	for _, pod := range pods.Items {
		if pod.Status.Phase != v1.PodRunning {
			return false, fmt.Sprintf("Waiting for Pod [%s] to be running", pod.Name), nil
		}

		ready := false
		for _, condition := range pod.Status.Conditions {
			if condition.Type == v1.PodReady {
				if condition.Status == v1.ConditionTrue {
					ready = true
				} else {
					return false, fmt.Sprintf("Waiting for Pod [%s] readiness", pod.Name), nil
				}
			}
		}
		if !ready {
			return false, fmt.Sprintf("Waiting for Pod [%s] readiness", pod.Name), nil
		}
	}

	return true, "", nil
}

func patchDeploymentLifecycleStatus(
	ctx context.Context,
	sdk client.Client,
	drd *v1alpha1.Druid,
	updated v1alpha1.DeploymentLifecycleStatus,
	emitEvent EventEmitter,
) error {
	current := drd.Status.DeploymentLifecycle
	if reflect.DeepEqual(current, updated) {
		return nil
	}

	patchBytes, err := json.Marshal(map[string]interface{}{
		"status": map[string]interface{}{
			"deploymentLifecycle": deploymentLifecycleStatusPatch(updated),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to serialize deployment lifecycle status patch: %v", err)
	}

	if err := writers.Patch(ctx, sdk, drd, drd, true, client.RawPatch(types.MergePatchType, patchBytes), emitEvent); err != nil {
		return err
	}

	defaultDeploymentLifecycleMetrics.recordTransition(drd.Namespace, drd.Name, current, updated)
	drd.Status.DeploymentLifecycle = updated
	emitDeploymentLifecycleEvent(drd, emitEvent, current, updated)
	return nil
}

func deploymentLifecycleStatusPatch(status v1alpha1.DeploymentLifecycleStatus) map[string]interface{} {
	return map[string]interface{}{
		"revision":                         status.Revision,
		"phase":                            status.Phase,
		"reason":                           status.Reason,
		"observedGeneration":               status.ObservedGeneration,
		"startedAt":                        status.StartedAt,
		"completedAt":                      status.CompletedAt,
		"lastSuccessfulImage":              status.LastSuccessfulImage,
		"lastSuccessfulImageInputsHash":    status.LastSuccessfulImageInputsHash,
		"lastSuccessfulForceRedeployToken": status.LastSuccessfulForceRedeployToken,
		"trigger":                          status.Trigger,
		"expectedBuildRevision":            status.ExpectedBuildRevision,
		"observedBuildRevisions":           status.ObservedBuildRevisions,
	}
}

func emitDeploymentLifecycleEvent(
	drd *v1alpha1.Druid,
	emitEvent EventEmitter,
	previous, current v1alpha1.DeploymentLifecycleStatus,
) {
	if previous.Phase == current.Phase && previous.Revision == current.Revision && previous.Reason == current.Reason {
		return
	}

	msg := fmt.Sprintf("revision=%s phase=%s reason=%s", current.Revision, current.Phase, current.Reason)
	switch current.Phase {
	case v1alpha1.DeploymentLifecyclePending, v1alpha1.DeploymentLifecycleInProgress:
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleStarted), msg, nil)
	case v1alpha1.DeploymentLifecycleSucceeded:
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleSucceeded), msg, nil)
	case v1alpha1.DeploymentLifecycleFailed:
		emitEvent.EmitEventGeneric(drd, string(druidDeploymentLifecycleFailed), msg, errors.New(current.Reason))
	}
}
