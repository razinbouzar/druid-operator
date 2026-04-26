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
package druidapi

import (
	"context"
	"errors"
	internalhttp "github.com/apache/druid-operator/pkg/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"testing"
)

type fakeDruidHTTP struct {
	response *internalhttp.Response
	err      error
	doFunc   func(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error)
}

func (f fakeDruidHTTP) Do(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error) {
	if f.doFunc != nil {
		return f.doFunc(ctx, method, url, body)
	}
	return f.response, f.err
}

func TestGetAuthCreds(t *testing.T) {
	tests := []struct {
		name      string
		auth      Auth
		expected  internalhttp.BasicAuth
		expectErr bool
	}{
		{
			name: "default keys present",
			auth: Auth{
				Type:      BasicAuth,
				SecretRef: v1.SecretReference{Name: "test-default", Namespace: "test"},
			},
			expected:  internalhttp.BasicAuth{UserName: "test-user", Password: "test-password"},
			expectErr: false,
		},
		{
			name: "custom keys present",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
				PasswordKey: "pwd",
			},
			expected:  internalhttp.BasicAuth{UserName: "admin", Password: "admin"},
			expectErr: false,
		},
		{
			name: "custom user key is  missing",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "nope",
				PasswordKey: "pwd",
			},
			expected:  internalhttp.BasicAuth{},
			expectErr: true,
		},
		{
			name: "custom user key with default password key",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
			},
			expected:  internalhttp.BasicAuth{UserName: "admin", Password: "also-admin"},
			expectErr: false,
		},
		{
			name: "custom password key is missing",
			auth: Auth{
				Type:        BasicAuth,
				SecretRef:   v1.SecretReference{Name: "test", Namespace: "default"},
				UsernameKey: "usr",
				PasswordKey: "nope",
			},
			expected:  internalhttp.BasicAuth{},
			expectErr: true,
		},
		{
			name:      "empty auth struct returns no creds",
			auth:      Auth{},
			expected:  internalhttp.BasicAuth{},
			expectErr: false,
		},
	}

	client := fake.NewClientBuilder().
		WithObjects(&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-default",
				Namespace: "test",
			},
			Data: map[string][]byte{
				OperatorUserName: []byte("test-user"),
				OperatorPassword: []byte("test-password"),
			},
		}).
		WithObjects(&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test",
				Namespace: "default",
			},
			Data: map[string][]byte{
				"usr":            []byte("admin"),
				"pwd":            []byte("admin"),
				OperatorPassword: []byte("also-admin"),
			},
		}).Build()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := GetAuthCreds(context.TODO(), client, tt.auth)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestMakePath(t *testing.T) {
	tests := []struct {
		name            string
		baseURL         string
		componentType   string
		apiType         string
		additionalPaths []string
		expected        string
	}{
		{
			name:          "NoAdditionalPath",
			baseURL:       "http://example-druid-service",
			componentType: "indexer",
			apiType:       "task",
			expected:      "http://example-druid-service/druid/indexer/v1/task",
		},
		{
			name:            "OneAdditionalPath",
			baseURL:         "http://example-druid-service",
			componentType:   "indexer",
			apiType:         "task",
			additionalPaths: []string{"extra"},
			expected:        "http://example-druid-service/druid/indexer/v1/task/extra",
		},
		{
			name:            "MultipleAdditionalPaths",
			baseURL:         "http://example-druid-service",
			componentType:   "coordinator",
			apiType:         "rules",
			additionalPaths: []string{"wikipedia", "history"},
			expected:        "http://example-druid-service/druid/coordinator/v1/rules/wikipedia/history",
		},
		{
			name:          "EmptyBaseURL",
			baseURL:       "",
			componentType: "indexer",
			apiType:       "task",
			expected:      "druid/indexer/v1/task",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := MakePath(tt.baseURL, tt.componentType, tt.apiType, tt.additionalPaths...)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestMakeSQLPath(t *testing.T) {
	actual, err := MakeSQLPath("http://router.druid.svc.cluster.local:8088")
	assert.NoError(t, err)
	assert.Equal(t, "http://router.druid.svc.cluster.local:8088/druid/v2/sql", actual)
}

func TestGetClusterBuildRevisions(t *testing.T) {
	k8sClient := fake.NewClientBuilder().
		WithObjects(&v1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-default",
				Namespace: "test",
			},
			Data: map[string][]byte{
				OperatorUserName: []byte("test-user"),
				OperatorPassword: []byte("test-password"),
			},
		}).
		Build()

	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{
		Type:      BasicAuth,
		SecretRef: v1.SecretReference{Name: "test-default", Namespace: "test"},
	}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				response: &internalhttp.Response{
					StatusCode:   200,
					ResponseBody: `[{"build_revision":"abc123"},{"build_revision":"abc123"},{"build_revision":"def456"}]`,
				},
			}
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"abc123", "def456"}, revisions)
}

func TestGetClusterBuildRevisionsFallsBackToVersionWhenBuildRevisionColumnIsUnavailable(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	queries := make([]string, 0, 2)

	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				doFunc: func(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error) {
					queries = append(queries, string(body))
					if len(queries) == 1 {
						return &internalhttp.Response{
							StatusCode:   400,
							ResponseBody: `{"error":"druidException","errorCode":"invalidInput","errorMessage":"Column 'build_revision' not found in any table"}`,
						}, nil
					}
					return &internalhttp.Response{
						StatusCode:   200,
						ResponseBody: `[{"build_revision":"36.0.0"},{"build_revision":"36.0.0"}]`,
					}, nil
				},
			}
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"36.0.0"}, revisions)
	require.Len(t, queries, 2)
	assert.Contains(t, queries[0], buildRevisionSQLQuery)
	assert.Contains(t, queries[1], versionSQLQuery)
}

func TestDefaultBuildRevisionLookupDependenciesUsesBrokerService(t *testing.T) {
	deps := defaultBuildRevisionLookupDependencies()
	require.NotNil(t, deps.resolveQueryServiceURL)

	k8sClient := fake.NewClientBuilder().
		WithObjects(&v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-broker",
				Namespace: "test",
				Labels: map[string]string{
					"druid_cr":  "example",
					"component": "broker",
				},
			},
		}).
		WithObjects(&v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-router",
				Namespace: "test",
				Labels: map[string]string{
					"druid_cr":  "example",
					"component": "router",
				},
			},
		}).
		Build()

	url, err := deps.resolveQueryServiceURL(context.Background(), "test", "example", k8sClient)
	require.NoError(t, err)
	assert.Equal(t, "http://example-broker.test:8088", url)
}

func TestGetClusterBuildRevisionsReturnsPendingErrorWhenQueryServiceIsUnavailable(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "", errors.New("service not found")
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{}
		},
	})
	assert.Nil(t, revisions)
	assert.Error(t, err)
	assert.True(t, IsBuildRevisionLookupPending(err))

	var pendingErr *BuildRevisionLookupPendingError
	assert.ErrorAs(t, err, &pendingErr)
	assert.Equal(t, BuildRevisionPendingQueryServiceDiscovery, pendingErr.Reason)
}

func TestGetClusterBuildRevisionsReturnsPendingErrorWhenQueryRequestTimesOut(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				doFunc: func(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error) {
					return nil, context.DeadlineExceeded
				},
			}
		},
	})
	assert.Nil(t, revisions)
	assert.Error(t, err)
	assert.True(t, IsBuildRevisionLookupPending(err))

	var pendingErr *BuildRevisionLookupPendingError
	assert.ErrorAs(t, err, &pendingErr)
	assert.Equal(t, BuildRevisionPendingQueryEndpoint, pendingErr.Reason)
}

func TestGetClusterBuildRevisionsReturnsCanceledErrorWithoutPendingClassification(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				doFunc: func(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error) {
					return nil, context.Canceled
				},
			}
		},
	})
	assert.Nil(t, revisions)
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, IsBuildRevisionLookupPending(err))
}

func TestGetClusterBuildRevisionsReturnsTerminalErrorForUnauthorizedQuery(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				response: &internalhttp.Response{
					StatusCode:   401,
					ResponseBody: "unauthorized",
				},
			}
		},
	})
	assert.Nil(t, revisions)
	assert.EqualError(t, err, "status code: 401, response body: unauthorized")
	assert.False(t, IsBuildRevisionLookupPending(err))
}

func TestGetClusterBuildRevisionsDoesNotFallbackForOtherTerminalQueryErrors(t *testing.T) {
	k8sClient := fake.NewClientBuilder().Build()
	queries := make([]string, 0, 2)

	revisions, err := getClusterBuildRevisionsWithDeps(context.TODO(), k8sClient, "test", "example", Auth{}, buildRevisionLookupDependencies{
		resolveQueryServiceURL: func(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
			return "http://router.druid.svc.cluster.local:8088", nil
		},
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return fakeDruidHTTP{
				doFunc: func(ctx context.Context, method, url string, body []byte) (*internalhttp.Response, error) {
					queries = append(queries, string(body))
					return &internalhttp.Response{
						StatusCode:   400,
						ResponseBody: `{"error":"druidException","errorCode":"invalidInput","errorMessage":"Table 'sys.servers' not found"}`,
					}, nil
				},
			}
		},
	})
	assert.Nil(t, revisions)
	assert.EqualError(t, err, `status code: 400, response body: {"error":"druidException","errorCode":"invalidInput","errorMessage":"Table 'sys.servers' not found"}`)
	require.Len(t, queries, 1)
	assert.Contains(t, queries[0], buildRevisionSQLQuery)
}
