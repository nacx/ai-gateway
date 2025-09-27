// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/tests/internal/e2elib"
	"github.com/envoyproxy/ai-gateway/tests/internal/testmcp"
)

type requestHeaderInjector struct{}

func (h requestHeaderInjector) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Set("x-tenant", "tenant-a")
	return http.DefaultTransport.RoundTrip(req)
}

// Use a custom HTTP client that injects the tenant header for testing the header-based routing.
var requestHeaderHTTPClient = &http.Client{Transport: requestHeaderInjector{}}

func TestMCP(t *testing.T) {
	const manifest = "testdata/mcp_route.yaml"
	require.NoError(t, e2elib.KubectlApplyManifest(t.Context(), manifest))

	const egSelector = "gateway.envoyproxy.io/owning-gateway-name=mcp-gateway"
	e2elib.RequireWaitForGatewayPodReady(t, egSelector)

	fwd := e2elib.RequireNewHTTPPortForwarder(t, e2elib.EnvoyGatewayNamespace, egSelector, e2elib.EnvoyGatewayDefaultServicePort)
	defer fwd.Kill()
	// Create an MCP client and connect to the server over Streamable HTTP.
	client := mcp.NewClient(&mcp.Implementation{Name: "demo-http-client", Version: "0.1.0"}, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)

	t.Run("default route", func(t *testing.T) {
		testMCPRouteTools(ctx, t, client, fwd.Address(), "mcp-backend__", nil)
	})

	t.Run("tenant route", func(t *testing.T) {
		testMCPRouteTools(ctx, t, client, fwd.Address(), "mcp-backend-tenant__", requestHeaderHTTPClient)
	})
}

func testMCPRouteTools(ctx context.Context, t *testing.T, client *mcp.Client, fwdAddress string, prefix string, mcpRouteTenantHeaderClient *http.Client) {
	var sess *mcp.ClientSession
	require.Eventually(t, func() bool {
		var err error
		sess, err = client.Connect(
			ctx,
			&mcp.StreamableClientTransport{
				Endpoint:   fmt.Sprintf("%s/mcp", fwdAddress),
				HTTPClient: mcpRouteTenantHeaderClient,
			}, nil)
		if err != nil {
			t.Logf("failed to connect to MCP server: %v", err)
			return false
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "failed to connect to MCP server")
	t.Cleanup(func() { _ = sess.Close() })

	// List tools and verify the expected tool names are present.
	tools, err := sess.ListTools(ctx, &mcp.ListToolsParams{})
	require.NoError(t, err)
	var names []string
	for _, tool := range tools.Tools {
		names = append(names, tool.Name)
	}

	expected := []string{
		prefix + testmcp.ToolEcho.Tool.Name,
		prefix + testmcp.ToolSum.Tool.Name,
		prefix + testmcp.ToolError.Tool.Name,
		prefix + testmcp.ToolCountDown.Tool.Name,
		prefix + testmcp.ToolContainsRootTool.Tool.Name,
		prefix + testmcp.ToolDelay.Tool.Name,
		prefix + testmcp.ToolAddPromptName,
		prefix + testmcp.ToolResourceUpdateNotificationName,
		prefix + testmcp.ToolAddOrDeleteDummyResourceName,
		prefix + testmcp.ToolElicitEmail.Tool.Name,
		prefix + testmcp.ToolCreateMessage.Tool.Name,
		prefix + testmcp.ToolNotificationCountsName,
	}

	require.ElementsMatch(t, expected, names, "tool names do not match")

	// Call the echo tool and verify the response content.
	const hello = "hello MCP"
	res, err := sess.CallTool(ctx, &mcp.CallToolParams{
		Name:      prefix + testmcp.ToolEcho.Tool.Name,
		Arguments: testmcp.ToolEchoArgs{Text: hello},
	})
	require.NoError(t, err)
	require.False(t, res.IsError)
	require.Len(t, res.Content, 1)
	txt, ok := res.Content[0].(*mcp.TextContent)
	require.True(t, ok)
	require.Equal(t, hello, txt.Text)

	// Call the sum tool and verify the result content is "42".
	res, err = sess.CallTool(ctx, &mcp.CallToolParams{
		Name:      prefix + testmcp.ToolSum.Tool.Name,
		Arguments: testmcp.ToolSumArgs{A: 41, B: 1},
	})
	require.NoError(t, err)
	require.False(t, res.IsError)
	require.Len(t, res.Content, 1)
	txt2, ok2 := res.Content[0].(*mcp.TextContent)
	require.True(t, ok2)
	require.Equal(t, "42", txt2.Text)
}
