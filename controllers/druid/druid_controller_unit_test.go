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
	"testing"
	"time"

	"github.com/apache/druid-operator/apis/druid/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

func TestReconcileMarksLifecycleFailedAndReturnsOriginalError(t *testing.T) {
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
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"bad_key": {},
			},
		},
	}

	k8sClient := newLifecycleTestClient(t, drd)
	reconciler := &DruidReconciler{
		Client:        k8sClient,
		Log:           ctrl.Log.WithName("test").WithName("Druid"),
		ReconcileWait: time.Second,
		Recorder:      record.NewFakeRecorder(10),
	}

	result, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(drd),
	})
	require.Error(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.Contains(t, err.Error(), "invalid DruidSpec")

	stored := &v1alpha1.Druid{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.Equal(t, v1alpha1.DeploymentLifecycleFailed, stored.Status.DeploymentLifecycle.Phase)
	assert.Contains(t, stored.Status.DeploymentLifecycle.Reason, "invalid DruidSpec")
	assert.Equal(t, drd.Generation, stored.Status.DeploymentLifecycle.ObservedGeneration)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.StartedAt)
	assert.NotNil(t, stored.Status.DeploymentLifecycle.CompletedAt)
}

func TestReconcileReturnsJoinedErrorWhenFailureStatusPatchFails(t *testing.T) {
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
			Nodes: map[string]v1alpha1.DruidNodeSpec{
				"bad_key": {},
			},
		},
	}

	k8sClient := newLifecycleTestClient(t, drd)
	reconciler := &DruidReconciler{
		Client:        k8sClient,
		Log:           ctrl.Log.WithName("test").WithName("Druid"),
		ReconcileWait: time.Second,
		Recorder:      record.NewFakeRecorder(10),
	}

	previousWriter := writers
	writers = &failOnNthPatchWriter{Writer: previousWriter, failOnPatchCall: 2, err: errors.New("status patch failed")}
	t.Cleanup(func() {
		writers = previousWriter
	})

	result, err := reconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: client.ObjectKeyFromObject(drd),
	})
	require.Error(t, err)
	assert.Equal(t, ctrl.Result{}, result)
	assert.Contains(t, err.Error(), "invalid DruidSpec")
	assert.Contains(t, err.Error(), "status patch failed")

	stored := &v1alpha1.Druid{}
	require.NoError(t, k8sClient.Get(context.Background(), client.ObjectKeyFromObject(drd), stored))
	assert.NotEqual(t, v1alpha1.DeploymentLifecycleFailed, stored.Status.DeploymentLifecycle.Phase)
}

func TestShouldPersistDeploymentLifecycleFailure(t *testing.T) {
	assert.True(t, shouldPersistDeploymentLifecycleFailure(markTerminalDeploymentLifecycleError(errors.New("invalid spec"))))
	assert.False(t, shouldPersistDeploymentLifecycleFailure(errors.New("temporary api timeout")))
	assert.False(t, shouldPersistDeploymentLifecycleFailure(context.Canceled))
}

type failOnNthPatchWriter struct {
	Writer
	patchCalls      int
	failOnPatchCall int
	err             error
}

func (w *failOnNthPatchWriter) Patch(ctx context.Context, sdk client.Client, drd *v1alpha1.Druid, obj object, status bool, patch client.Patch, emitEvent EventEmitter) error {
	w.patchCalls++
	if w.patchCalls == w.failOnPatchCall {
		return w.err
	}
	return w.Writer.Patch(ctx, sdk, drd, obj, status, patch, emitEvent)
}
