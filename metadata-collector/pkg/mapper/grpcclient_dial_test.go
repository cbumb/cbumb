// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mapper

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	v1 "k8s.io/kubelet/pkg/apis/podresources/v1"
)

func TestDialPodResourcesClient_LargeResponseFitsCap(t *testing.T) {
	const fiveMiB = 5 * 1024 * 1024
	response := buildLargeListPodResourcesResponse(fiveMiB)

	socketPath := startTestPodResourcesServer(t, response)

	conn, client, err := dialPodResourcesClient(socketPath)
	require.NoError(t, err)

	defer conn.Close()

	got, err := client.List(context.Background(), &v1.ListPodResourcesRequest{})
	require.NoError(t, err, "List must succeed for >4 MiB response when MaxCallRecvMsgSize is set")
	assert.NotEmpty(t, got.GetPodResources())
}

// TestDialPodResourcesClient_DefaultCapWouldFail pins the regression: a client
// dialed WITHOUT MaxCallRecvMsgSize must fail on the same response that
// dialPodResourcesClient successfully decodes. If this test starts passing
// (i.e., the default cap is no longer hit at 5 MiB), reconsider whether the
// cap in dialPodResourcesClient is still load-bearing before removing it.
func TestDialPodResourcesClient_DefaultCapWouldFail(t *testing.T) {
	const fiveMiB = 5 * 1024 * 1024
	response := buildLargeListPodResourcesResponse(fiveMiB)

	socketPath := startTestPodResourcesServer(t, response)

	conn, err := grpc.NewClient(
		socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, "unix", addr)
		}),
	)
	require.NoError(t, err)

	defer conn.Close()

	client := v1.NewPodResourcesListerClient(conn)
	_, err = client.List(context.Background(), &v1.ListPodResourcesRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ResourceExhausted")
	assert.Contains(t, err.Error(), "received message larger than max")
}

func startTestPodResourcesServer(t *testing.T, response *v1.ListPodResourcesResponse) string {
	t.Helper()

	// Use /tmp directly rather than t.TempDir(): unix sockets cap the path at
	// 104 bytes on macOS / 108 on Linux, and the default test tempdir on
	// macOS (/var/folders/...) blows past that.
	dir, err := os.MkdirTemp("/tmp", "nvs-podres-")
	require.NoError(t, err)

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	socketPath := filepath.Join(dir, "k.sock")

	listener, err := net.Listen("unix", socketPath)
	require.NoError(t, err)

	server := grpc.NewServer()
	v1.RegisterPodResourcesListerServer(server, &largeListPodResourcesServer{response: response})

	serverDone := make(chan struct{})

	go func() {
		defer close(serverDone)

		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.GracefulStop()
		<-serverDone
	})

	return socketPath
}

// buildLargeListPodResourcesResponse builds a response whose serialized size
// is at least targetBytes. It stuffs many synthetic pods, each with one
// synthetic device whose ID is a long padded string. The exact byte count
// intentionally over-shoots the target.
func buildLargeListPodResourcesResponse(targetBytes int) *v1.ListPodResourcesResponse {
	const idPadBytes = 4096

	pad := strings.Repeat("x", idPadBytes)
	podCount := (targetBytes / idPadBytes) + 1
	pods := make([]*v1.PodResources, 0, podCount)

	for i := 0; i < podCount; i++ {
		pods = append(pods, &v1.PodResources{
			Name:      fmt.Sprintf("test-pod-%d", i),
			Namespace: "default",
			Containers: []*v1.ContainerResources{
				{
					Name: "container-0",
					Devices: []*v1.ContainerDevices{
						{
							ResourceName: "nvidia.com/gpu",
							DeviceIds:    []string{fmt.Sprintf("GPU-%d-%s", i, pad)},
						},
					},
				},
			},
		})
	}

	return &v1.ListPodResourcesResponse{PodResources: pods}
}

// largeListPodResourcesServer returns a fixed ListPodResourcesResponse to
// every List call, used to exercise the gRPC client receive cap.
type largeListPodResourcesServer struct {
	v1.UnimplementedPodResourcesListerServer
	response *v1.ListPodResourcesResponse
}

func (s *largeListPodResourcesServer) List(_ context.Context,
	_ *v1.ListPodResourcesRequest) (*v1.ListPodResourcesResponse, error) {
	return s.response, nil
}
