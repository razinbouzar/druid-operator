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
package http

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type blockingRoundTripper struct{}

func (blockingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	<-req.Context().Done()
	return nil, req.Context().Err()
}

func TestDoHonorsContextCancellation(t *testing.T) {
	client := NewHTTPClient(&http.Client{Transport: blockingRoundTripper{}}, &Auth{}).(*DruidClient)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := client.Do(ctx, http.MethodGet, "http://example.invalid", nil)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
	assert.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestDoSetsBasicAuth(t *testing.T) {
	var authHeader string
	client := NewHTTPClient(&http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			authHeader = req.Header.Get("Authorization")
			return &http.Response{
				StatusCode: 200,
				Body:       http.NoBody,
				Header:     make(http.Header),
			}, nil
		}),
	}, &Auth{BasicAuth: BasicAuth{UserName: "user", Password: "pass"}}).(*DruidClient)

	_, err := client.Do(context.Background(), http.MethodGet, "http://example.invalid", nil)
	assert.NoError(t, err)
	assert.True(t, strings.HasPrefix(authHeader, "Basic "))
}

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
