// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"context"
	"fmt"

	egv1a1 "github.com/envoyproxy/gateway/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlutil "sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	gwapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	aigv1a1 "github.com/envoyproxy/ai-gateway/api/v1alpha1"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
)

func backendHTTPFilterName(backend string) string {
	return fmt.Sprintf("%s%s", internalapi.MCPBackendFilterPrefix, backend)
}

func (c *MCPRouteController) ensureMCPBackendHTTPFilter(ctx context.Context, ref aigv1a1.MCPRouteBackendRef, apiKey *aigv1a1.MCPBackendAPIKey, mcpRoute *aigv1a1.MCPRoute) (string, error) {
	filterName := backendHTTPFilterName(string(ref.Name))

	// Rewrite the hostname to the backend service name.
	// This allows Envoy to route to public MCP services with SNI matching the service name.
	// This could be a standalone filter and moved to the main mcp gateway route logic.
	filter := egv1a1.HTTPRouteFilter{
		ObjectMeta: metav1.ObjectMeta{
			Name:      filterName,
			Namespace: mcpRoute.Namespace,
		},
		Spec: egv1a1.HTTPRouteFilterSpec{
			URLRewrite: &egv1a1.HTTPURLRewriteFilter{
				Hostname: &egv1a1.HTTPHostnameModifier{
					Type: egv1a1.BackendHTTPHostnameModifier,
				},
			},
		},
	}
	if err := ctrlutil.SetControllerReference(mcpRoute, &filter, c.client.Scheme()); err != nil {
		return "", fmt.Errorf("failed to set controller reference for HTTPRouteFilter: %w", err)
	}

	// add credential injection if apiKey is specified.
	if apiKey != nil {
		secretName := fmt.Sprintf("%s-credential", filterName)
		if secretErr := c.ensureCredentialSecret(ctx, mcpRoute.Namespace, secretName, apiKey, mcpRoute); secretErr != nil {
			return "", fmt.Errorf("failed to ensure credential secret: %w", secretErr)
		}

		filter.Spec.CredentialInjection = &egv1a1.HTTPCredentialInjectionFilter{
			Header:    ptr.To("Authorization"),
			Overwrite: ptr.To(true),
			Credential: egv1a1.InjectedCredential{
				ValueRef: gwapiv1.SecretObjectReference{
					Name: gwapiv1.ObjectName(secretName),
				},
			},
		}
	}

	// Create or Update the HTTPRouteFilter.
	var existingFilter egv1a1.HTTPRouteFilter
	err := c.client.Get(ctx, client.ObjectKey{Name: filterName, Namespace: mcpRoute.Namespace}, &existingFilter)
	if err != nil && !apierrors.IsNotFound(err) {
		return "", fmt.Errorf("failed to get HTTPRouteFilter: %w", err)
	}

	if apierrors.IsNotFound(err) {
		c.logger.Info("Creating HTTPRouteFilter", "namespace", filter.Namespace, "name", filter.Name)
		if err = c.client.Create(ctx, &filter); err != nil {
			return "", fmt.Errorf("failed to create HTTPRouteFilter: %w", err)
		}
	} else {
		// Update existing filter unconditionally to ensure it matches the desired state.
		existingFilter.Spec = filter.Spec
		c.logger.Info("Updating HTTPRouteFilter", "namespace", existingFilter.Namespace, "name", existingFilter.Name)
		if err = c.client.Update(ctx, &existingFilter); err != nil {
			return "", fmt.Errorf("failed to update HTTPRouteFilter: %w", err)
		}
	}
	return filterName, nil
}

func (c *MCPRouteController) ensureCredentialSecret(ctx context.Context, namespace, secretName string, apiKey *aigv1a1.MCPBackendAPIKey, mcpRoute *aigv1a1.MCPRoute) error {
	var credentialValue string

	key := ptr.Deref(apiKey.Inline, "")
	if key == "" {
		secretRef := apiKey.SecretRef
		secret, err := c.kube.CoreV1().Secrets(namespace).Get(ctx, string(secretRef.Name), metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get secret for API key: %w", err)
		}
		if k, ok := secret.Data["apiKey"]; ok {
			key = string(k)
		} else if key, ok = secret.StringData["apiKey"]; !ok {
			return fmt.Errorf("secret %s/%s does not contain 'apiKey' key", namespace, secretRef.Name)
		}
	}

	credentialValue = fmt.Sprintf("Bearer %s", key)

	existingSecret, secretErr := c.kube.CoreV1().Secrets(namespace).Get(ctx, secretName, metav1.GetOptions{})
	if secretErr != nil && !apierrors.IsNotFound(secretErr) {
		return fmt.Errorf("failed to get credential secret: %w", secretErr)
	}

	secretData := map[string][]byte{
		egv1a1.InjectedCredentialKey: []byte(credentialValue),
	}

	if apierrors.IsNotFound(secretErr) {
		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: namespace,
			},
			Data: secretData,
		}

		if mcpRoute != nil {
			if err := ctrlutil.SetControllerReference(mcpRoute, secret, c.client.Scheme()); err != nil {
				return fmt.Errorf("failed to set controller reference for credential secret: %w", err)
			}
		}

		c.logger.Info("Creating credential secret", "namespace", namespace, "name", secretName)
		if _, err := c.kube.CoreV1().Secrets(namespace).Create(ctx, secret, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create credential secret: %w", err)
		}
	} else if existingSecret.Data == nil || string(existingSecret.Data[egv1a1.InjectedCredentialKey]) != credentialValue {
		existingSecret.Data = secretData
		c.logger.Info("Updating credential secret", "namespace", namespace, "name", secretName)
		if _, err := c.kube.CoreV1().Secrets(namespace).Update(ctx, existingSecret, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update credential secret: %w", secretErr)
		}
	}
	return nil
}

// syncBackendSecurityPolicy reconciles MCPRouteBackendRef.SecurityPolicy and ensures backend credentials are properly configured.
// Returns the filter name for the created/updated HTTPRouteFilter.
func (c *MCPRouteController) syncBackendSecurityPolicy(ctx context.Context, ref aigv1a1.MCPRouteBackendRef, mcpRoute *aigv1a1.MCPRoute) (string, error) {
	var apiKey *aigv1a1.MCPBackendAPIKey
	if ref.SecurityPolicy != nil && ref.SecurityPolicy.APIKey != nil {
		apiKey = ref.SecurityPolicy.APIKey
	}

	// Ensure the HTTPRouteFilter for this backend with its security configuration.
	filterName, err := c.ensureMCPBackendHTTPFilter(ctx, ref, apiKey, mcpRoute)
	if err != nil {
		return "", fmt.Errorf("failed to ensure MCP backend API key HTTP filter: %w", err)
	}

	return filterName, nil
}
