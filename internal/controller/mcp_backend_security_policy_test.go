// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"testing"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	fakekube "k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	aigv1a1 "github.com/envoyproxy/ai-gateway/api/v1alpha1"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	internaltesting "github.com/envoyproxy/ai-gateway/internal/testing"
)

func TestMCPRouteController_mcpRuleWithAPIKeyBackendSecurity(t *testing.T) {
	c := requireNewFakeClientWithIndexesForMCP(t)
	eventCh := internaltesting.NewControllerEventChan[*gwapiv1.Gateway]()
	ctrlr := NewMCPRouteController(c, fakekube.NewClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "some-secret", Namespace: "default"},
			Data:       map[string][]byte{"apiKey": []byte("secretvalue")},
		},
	), logr.Discard(), eventCh.Ch)

	httpRule, err := ctrlr.mcpBackendToHTTPRouteRule(t.Context(),
		aigv1a1.MCPRouteBackendRef{
			BackendObjectReference: gwapiv1.BackendObjectReference{
				Name:      "svc-a",
				Namespace: ptr.To(gwapiv1.Namespace("default")),
			},
			SecurityPolicy: &aigv1a1.MCPBackendSecurityPolicy{
				APIKey: &aigv1a1.MCPBackendAPIKey{
					SecretRef: &gwapiv1.SecretObjectReference{
						Name: "some-secret",
					},
				},
			},
		},
		&aigv1a1.MCPRoute{ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "default"}},
	)
	require.NoError(t, err)
	require.Len(t, httpRule.Matches, 1)
	require.Equal(t, "/", *httpRule.Matches[0].Path.Value)
	require.Len(t, httpRule.Matches[0].Headers, 1)
	require.Equal(t, internalapi.MCPBackendHeader, string(httpRule.Matches[0].Headers[0].Name))
	require.Equal(t, "svc-a", httpRule.Matches[0].Headers[0].Value)
	require.Len(t, httpRule.Filters, 1)
	require.Equal(t, gwapiv1.HTTPRouteFilterExtensionRef, httpRule.Filters[0].Type)
	require.NotNil(t, httpRule.Filters[0].ExtensionRef)
	require.Equal(t, gwapiv1.Group("gateway.envoyproxy.io"), httpRule.Filters[0].ExtensionRef.Group)
	require.Equal(t, gwapiv1.Kind("HTTPRouteFilter"), httpRule.Filters[0].ExtensionRef.Kind)
	require.Contains(t, string(httpRule.Filters[0].ExtensionRef.Name), internalapi.MCPBackendFilterPrefix)

	var httpFilter egv1a1.HTTPRouteFilter
	err = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: string(httpRule.Filters[0].ExtensionRef.Name)}, &httpFilter)
	require.NoError(t, err)

	require.NotNil(t, httpFilter.Spec.CredentialInjection)
	require.Equal(t, "Authorization", ptr.Deref(httpFilter.Spec.CredentialInjection.Header, ""))
	require.Equal(t,
		httpFilter.Name+"-credential",
		string(httpFilter.Spec.CredentialInjection.Credential.ValueRef.Name),
	)

	require.NotNil(t, httpFilter.Spec.URLRewrite)
	require.NotNil(t, httpFilter.Spec.URLRewrite.Hostname)
	require.Equal(t, egv1a1.BackendHTTPHostnameModifier, httpFilter.Spec.URLRewrite.Hostname.Type)
}

func TestMCPRouteController_syncBackendSecurityPolicy(t *testing.T) {
	c := requireNewFakeClientWithIndexesForMCP(t)
	eventCh := internaltesting.NewControllerEventChan[*gwapiv1.Gateway]()
	ctrlr := NewMCPRouteController(c, fakekube.NewClientset(
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "test-secret", Namespace: "default"},
			Data:       map[string][]byte{"apiKey": []byte("test-api-key")},
		},
	), logr.Discard(), eventCh.Ch)

	mcpRoute := &aigv1a1.MCPRoute{
		ObjectMeta: metav1.ObjectMeta{Name: "test-route", Namespace: "default"},
	}
	err := c.Create(t.Context(), mcpRoute)
	require.NoError(t, err)

	ref := aigv1a1.MCPRouteBackendRef{
		BackendObjectReference: gwapiv1.BackendObjectReference{
			Name:      "test-backend",
			Namespace: ptr.To(gwapiv1.Namespace("default")),
		},
		SecurityPolicy: &aigv1a1.MCPBackendSecurityPolicy{
			APIKey: &aigv1a1.MCPBackendAPIKey{
				SecretRef: &gwapiv1.SecretObjectReference{
					Name: "test-secret",
				},
			},
		},
	}

	filterName, err := ctrlr.syncBackendSecurityPolicy(t.Context(), ref, mcpRoute)
	require.NoError(t, err)
	require.NotEmpty(t, filterName)

	// Verify HTTPRouteFilter was created.
	var httpFilter egv1a1.HTTPRouteFilter
	err = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: filterName}, &httpFilter)
	require.NoError(t, err)

	// Verify filter has credential injection configured.
	require.NotNil(t, httpFilter.Spec.CredentialInjection)
	require.Equal(t, "Authorization", ptr.Deref(httpFilter.Spec.CredentialInjection.Header, ""))
	require.Equal(t, filterName+"-credential", string(httpFilter.Spec.CredentialInjection.Credential.ValueRef.Name))

	// Update the route without API key and ensure the filter is deleted.
	ref.SecurityPolicy = nil
	filterName, err = ctrlr.syncBackendSecurityPolicy(t.Context(), ref, mcpRoute)
	require.NoError(t, err)
	require.NotEmpty(t, filterName)

	// Check that the HTTPRouteFilter doesn't have CredentialInjection anymore.
	err = c.Get(t.Context(), types.NamespacedName{Namespace: "default", Name: filterName}, &httpFilter)
	require.NoError(t, err)
	require.Nil(t, httpFilter.Spec.CredentialInjection)
}
