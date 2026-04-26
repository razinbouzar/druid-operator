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
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	internalhttp "github.com/apache/druid-operator/pkg/http"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	DruidRouterPort  = "8088"
	OperatorUserName = "OperatorUserName"
	OperatorPassword = "OperatorPassword"
)

type BuildRevisionLookupPendingReason string

const (
	BuildRevisionPendingQueryServiceDiscovery BuildRevisionLookupPendingReason = "QueryServiceDiscovery"
	BuildRevisionPendingQueryEndpoint         BuildRevisionLookupPendingReason = "QueryEndpoint"
	BuildRevisionPendingQueryResponse         BuildRevisionLookupPendingReason = "QueryResponse"
)

type buildRevisionLookupDependencies struct {
	resolveQueryServiceURL func(context.Context, string, string, client.Client) (string, error)
	newHTTPClient          func(*internalhttp.Auth) internalhttp.DruidHTTP
}

var logger = logf.Log.WithName("druidapi")

func defaultBuildRevisionLookupDependencies() buildRevisionLookupDependencies {
	return buildRevisionLookupDependencies{
		resolveQueryServiceURL: GetBrokerSvcUrl,
		newHTTPClient: func(auth *internalhttp.Auth) internalhttp.DruidHTTP {
			return internalhttp.NewHTTPClient(&http.Client{Timeout: 30 * time.Second}, auth)
		},
	}
}

type BuildRevisionLookupPendingError struct {
	Reason BuildRevisionLookupPendingReason
	Err    error
}

func (e *BuildRevisionLookupPendingError) Error() string {
	message := "waiting for Druid build revision verification"
	switch e.Reason {
	case BuildRevisionPendingQueryServiceDiscovery:
		message = "waiting for Druid query service discovery"
	case BuildRevisionPendingQueryEndpoint:
		message = "waiting for Druid build revision query endpoint"
	case BuildRevisionPendingQueryResponse:
		message = "waiting for Druid build revision query response"
	}
	if e.Err == nil {
		return message
	}
	return fmt.Sprintf("%s: %v", message, e.Err)
}

func (e *BuildRevisionLookupPendingError) Unwrap() error {
	return e.Err
}

func IsBuildRevisionLookupPending(err error) bool {
	var pendingErr *BuildRevisionLookupPendingError
	return errors.As(err, &pendingErr)
}

type AuthType string

const (
	BasicAuth AuthType = "basic-auth"
)

type Auth struct {
	// +required
	Type AuthType `json:"type"`
	// +required
	SecretRef v1.SecretReference `json:"secretRef"`

	// UsernameKey specifies the key within the Kubernetes secret that contains the username for authentication.
	UsernameKey string `json:"usernameKey,omitempty"`

	// PasswordKey specifies the key within the Kubernetes secret that contains the password for authentication.
	PasswordKey string `json:"passwordKey,omitempty"`
}

// GetAuthCreds retrieves basic authentication credentials from a Kubernetes secret.
func GetAuthCreds(
	ctx context.Context,
	c client.Client,
	auth Auth,
) (internalhttp.BasicAuth, error) {
	userNameKey := OperatorUserName
	passwordKey := OperatorPassword

	if auth.UsernameKey != "" {
		userNameKey = auth.UsernameKey
	}

	if auth.PasswordKey != "" {
		passwordKey = auth.PasswordKey
	}

	// Check if the mentioned secret exists
	if auth != (Auth{}) {
		secret := v1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{
			Namespace: auth.SecretRef.Namespace,
			Name:      auth.SecretRef.Name,
		}, &secret); err != nil {
			return internalhttp.BasicAuth{}, err
		}

		if _, ok := secret.Data[userNameKey]; !ok {
			return internalhttp.BasicAuth{}, fmt.Errorf("username key %q not found in secret %s/%s", userNameKey, auth.SecretRef.Namespace, auth.SecretRef.Name)
		}

		if _, ok := secret.Data[passwordKey]; !ok {
			return internalhttp.BasicAuth{}, fmt.Errorf("password key %q not found in secret %s/%s", passwordKey, auth.SecretRef.Namespace, auth.SecretRef.Name)
		}

		creds := internalhttp.BasicAuth{
			UserName: string(secret.Data[userNameKey]),
			Password: string(secret.Data[passwordKey]),
		}

		return creds, nil
	}

	return internalhttp.BasicAuth{}, nil
}

// MakePath builds a Druid API path from the supplied base URL and path components.
func MakePath(baseURL, componentType, apiType string, additionalPaths ...string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse Druid base URL %q: %w", baseURL, err)
	}

	// Construct the initial path
	u.Path = path.Join("druid", componentType, "v1", apiType)

	// Append additional path components
	for _, p := range additionalPaths {
		u.Path = path.Join(u.Path, p)
	}

	return u.String(), nil
}

// GetRouterSvcUrl returns the URL of the Druid router service.
func GetRouterSvcUrl(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
	return getComponentSvcURL(ctx, namespace, druidClusterName, "router", c)
}

// GetBrokerSvcUrl returns the URL of the Druid broker service.
func GetBrokerSvcUrl(ctx context.Context, namespace, druidClusterName string, c client.Client) (string, error) {
	return getComponentSvcURL(ctx, namespace, druidClusterName, "broker", c)
}

func getComponentSvcURL(ctx context.Context, namespace, druidClusterName, component string, c client.Client) (string, error) {
	listOpts := []client.ListOption{
		client.InNamespace(namespace),
		client.MatchingLabels(map[string]string{
			"druid_cr":  druidClusterName,
			"component": component,
		}),
	}
	svcList := &v1.ServiceList{}
	if err := c.List(ctx, svcList, listOpts...); err != nil {
		return "", err
	}
	if len(svcList.Items) == 0 {
		return "", fmt.Errorf("%s svc discovery fail", component)
	}

	svcName := svcList.Items[0].Name
	newName := "http://" + svcName + "." + namespace + ":" + DruidRouterPort

	return newName, nil
}

type sqlQueryRequest struct {
	Query string `json:"query"`
}

type sqlServerBuildRevision struct {
	BuildRevision string `json:"build_revision"`
}

const (
	buildRevisionSQLQuery = "SELECT DISTINCT(build_revision) AS build_revision FROM sys.servers"
	versionSQLQuery       = "SELECT DISTINCT(version) AS build_revision FROM sys.servers"
)

func GetClusterBuildRevisions(
	ctx context.Context,
	c client.Client,
	namespace, druidClusterName string,
	auth Auth,
) ([]string, error) {
	return getClusterBuildRevisionsWithDeps(
		ctx,
		c,
		namespace,
		druidClusterName,
		auth,
		defaultBuildRevisionLookupDependencies(),
	)
}

// GetClusterBuildRevisions returns the unique build revisions reported by live Druid servers.
func getClusterBuildRevisionsWithDeps(
	ctx context.Context,
	c client.Client,
	namespace, druidClusterName string,
	auth Auth,
	deps buildRevisionLookupDependencies,
) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	basicAuth, err := GetAuthCreds(ctx, c, auth)
	if err != nil {
		return nil, err
	}

	httpClient := deps.newHTTPClient(&internalhttp.Auth{BasicAuth: basicAuth})

	svcURL, resolveErr := deps.resolveQueryServiceURL(ctx, namespace, druidClusterName, c)
	if resolveErr != nil {
		return nil, &BuildRevisionLookupPendingError{
			Reason: BuildRevisionPendingQueryServiceDiscovery,
			Err:    resolveErr,
		}
	}

	sqlPath, pathErr := MakeSQLPath(svcURL)
	if pathErr != nil {
		return nil, pathErr
	}

	rows, err := executeBuildRevisionQuery(ctx, httpClient, sqlPath, buildRevisionSQLQuery)
	if err == nil {
		return normalizeBuildRevisions(rows), nil
	}
	if !isMissingBuildRevisionColumnError(err) {
		return nil, err
	}

	logger.Info(
		"Falling back to sys.servers.version for deployment lifecycle build verification because sys.servers.build_revision is unavailable",
		"namespace", namespace,
		"druidInstance", druidClusterName,
	)

	rows, err = executeBuildRevisionQuery(ctx, httpClient, sqlPath, versionSQLQuery)
	if err != nil {
		return nil, err
	}
	return normalizeBuildRevisions(rows), nil
}

func MakeSQLPath(baseURL string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse Druid SQL base URL %q: %w", baseURL, err)
	}

	u.Path = path.Join("druid", "v2", "sql")
	return u.String(), nil
}

func normalizeBuildRevisions(rows []sqlServerBuildRevision) []string {
	set := make(map[string]struct{})
	for _, row := range rows {
		trimmed := strings.TrimSpace(row.BuildRevision)
		if trimmed == "" {
			continue
		}
		set[trimmed] = struct{}{}
	}

	revisions := make([]string, 0, len(set))
	for revision := range set {
		revisions = append(revisions, revision)
	}
	sort.Strings(revisions)
	return revisions
}

func executeBuildRevisionQuery(
	ctx context.Context,
	httpClient internalhttp.DruidHTTP,
	sqlPath, query string,
) ([]sqlServerBuildRevision, error) {
	requestBody, err := json.Marshal(sqlQueryRequest{Query: query})
	if err != nil {
		return nil, err
	}

	response, requestErr := httpClient.Do(ctx, "POST", sqlPath, requestBody)
	if requestErr != nil {
		if isRetryableBuildRevisionRequestError(requestErr) {
			return nil, &BuildRevisionLookupPendingError{
				Reason: BuildRevisionPendingQueryEndpoint,
				Err:    requestErr,
			}
		}
		return nil, requestErr
	}

	if response.StatusCode != http.StatusOK {
		err := fmt.Errorf("status code: %d, response body: %s", response.StatusCode, response.ResponseBody)
		if isRetryableBuildRevisionStatus(response.StatusCode) {
			return nil, &BuildRevisionLookupPendingError{
				Reason: BuildRevisionPendingQueryResponse,
				Err:    err,
			}
		}
		return nil, err
	}

	var rows []sqlServerBuildRevision
	if err := json.Unmarshal([]byte(response.ResponseBody), &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func isMissingBuildRevisionColumnError(err error) bool {
	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "build_revision") && strings.Contains(message, "not found")
}

func isRetryableBuildRevisionRequestError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var netErr net.Error
	return errors.As(err, &netErr)
}

func isRetryableBuildRevisionStatus(statusCode int) bool {
	return statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}
