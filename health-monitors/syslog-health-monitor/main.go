// Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/nvidia/nvsentinel/commons/pkg/logger"
	"github.com/nvidia/nvsentinel/commons/pkg/server"
	"github.com/nvidia/nvsentinel/commons/pkg/stringutil"
	pb "github.com/nvidia/nvsentinel/data-models/pkg/protos"
	fd "github.com/nvidia/nvsentinel/health-monitors/syslog-health-monitor/pkg/syslog-monitor"
)

const (
	defaultAgentName       = "syslog-health-monitor"
	defaultComponentClass  = "GPU"                                // Or a more specific class if applicable
	defaultPollingInterval = "30m"                                // Default polling interval
	defaultStateFilePath   = "/var/run/syslog_monitor/state.json" // Default state file path
)

var (
	// These variables will be populated during the build process
	version = "dev"
	commit  = "none"
	date    = "unknown"

	// Command-line flags
	checksList = flag.String("checks", "SysLogsXIDError,SysLogsSXIDError,SysLogsGPUFallenOff",
		"Comma separated listed of checks to enable")
	platformConnectorSocket = flag.String("platform-connector-socket", "unix:///var/run/nvsentinel.sock",
		"Path to the platform-connector UDS socket.")
	nodeNameEnv         = flag.String("node-name", os.Getenv("NODE_NAME"), "Node name. Defaults to NODE_NAME env var.")
	pollingIntervalFlag = flag.String("polling-interval", defaultPollingInterval,
		"Polling interval for health checks (e.g., 15m, 1h).")
	stateFileFlag = flag.String("state-file", defaultStateFilePath,
		"Path to state file for cursor persistence.")
	metricsPort         = flag.String("metrics-port", "2112", "Port to expose Prometheus metrics on")
	xidAnalyserEndpoint = flag.String("xid-analyser-endpoint", "",
		"Endpoint to the XID analyser service.")
	kataEnabled = flag.String("kata-enabled", "false",
		"Indicates if this monitor is running in Kata Containers mode (set by DaemonSet variant).")
	metadataPath = flag.String("metadata-path", "/var/lib/nvsentinel/gpu_metadata.json",
		"Path to GPU metadata JSON file.")
	processingStrategyFlag = flag.String("processing-strategy", "EXECUTE_REMEDIATION",
		"Event processing strategy: EXECUTE_REMEDIATION or STORE_ONLY")
)

var checks []fd.CheckDefinition

// healthRunner runs a health check; used to avoid coupling run() to concrete monitor type.
type healthRunner interface {
	Run() error
}

func main() {
	logger.SetDefaultStructuredLogger(defaultAgentName, version)
	slog.Info("Starting syslog-health-monitor", "version", version, "commit", commit, "date", date)

	if err := run(); err != nil {
		slog.Error("Fatal error", "error", err)
		os.Exit(1)
	}
}

// run coordinates process wiring, gRPC client, monitor creation, and background tasks.
func run() error {
	flag.Parse()
	slog.Info("Parsed command line flags successfully")

	nodeName := *nodeNameEnv
	if nodeName == "" {
		return fmt.Errorf("NODE_NAME env not set and --node-name flag not provided, cannot run")
	}

	slog.Info("Configuration", "node", nodeName, "kata-enabled", *kataEnabled)

	root := context.Background()
	ctx, stop := signal.NotifyContext(root, os.Interrupt, syscall.SIGTERM)

	defer stop()

	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	slog.Info("Creating gRPC client to platform connector", "socket", *platformConnectorSocket)

	conn, err := dialWithRetry(ctx, *platformConnectorSocket, dialOpts...)
	if err != nil {
		return fmt.Errorf("failed to create gRPC client after retries: %w", err)
	}

	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			slog.Error("Error closing gRPC connection", "error", closeErr)
		}
	}()

	client := pb.NewPlatformConnectorClient(conn)

	checks = buildChecksFromFlag()
	if len(checks) == 0 {
		return fmt.Errorf("no checks defined in the config file")
	}

	checks = applyKataConfig(checks, stringutil.IsTruthyValue(*kataEnabled))
	slog.Info("Creating syslog monitor", "checksCount", len(checks))

	value, ok := pb.ProcessingStrategy_value[*processingStrategyFlag]
	if !ok {
		return fmt.Errorf("unexpected processingStrategy value: %q", *processingStrategyFlag)
	}

	slog.Info("Event handling strategy configured", "processingStrategy", *processingStrategyFlag)

	processingStrategy := pb.ProcessingStrategy(value)

	fdHealthMonitor, err := fd.NewSyslogMonitor(
		nodeName, checks, client, defaultAgentName, defaultComponentClass,
		*pollingIntervalFlag, *stateFileFlag, *xidAnalyserEndpoint, *metadataPath,
		processingStrategy,
	)
	if err != nil {
		return fmt.Errorf("error creating syslog health monitor: %w", err)
	}

	pollingInterval, err := time.ParseDuration(*pollingIntervalFlag)
	if err != nil {
		return fmt.Errorf("error parsing polling interval: %w", err)
	}

	slog.Info("Polling interval configured", "interval", pollingInterval)

	portInt, err := strconv.Atoi(*metricsPort)
	if err != nil {
		return fmt.Errorf("invalid metrics port: %w", err)
	}

	srv := server.NewServer(
		server.WithPort(portInt),
		server.WithPrometheusMetrics(),
		server.WithSimpleHealth(),
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		slog.Info("Starting metrics server", "port", portInt)

		if err := srv.Serve(gCtx); err != nil {
			slog.Error("Metrics server failed - continuing without metrics", "error", err)
		}

		return nil
	})
	g.Go(func() error {
		return runPollingLoop(gCtx, fdHealthMonitor, pollingInterval)
	})

	return g.Wait()
}

func buildChecksFromFlag() []fd.CheckDefinition {
	list := make([]fd.CheckDefinition, 0)

	for c := range strings.SplitSeq((*checksList), ",") {
		list = append(list, fd.CheckDefinition{
			Name:        c,
			JournalPath: "/nvsentinel/var/log/journal/",
		})
	}

	return list
}

func applyKataConfig(checks []fd.CheckDefinition, kataEnabled bool) []fd.CheckDefinition {
	if !kataEnabled {
		return checks
	}

	slog.Info("Kata mode enabled, adding containerd service filter and removing SysLogsSXIDError check")

	for i := range checks {
		if checks[i].Tags == nil {
			checks[i].Tags = []string{"-u", "containerd.service"}
		} else {
			checks[i].Tags = append(checks[i].Tags, "-u", "containerd.service")
		}
	}

	filtered := make([]fd.CheckDefinition, 0, len(checks))
	for _, check := range checks {
		if check.Name != "SysLogsSXIDError" {
			filtered = append(filtered, check)
		}
	}

	return filtered
}

func runPollingLoop(ctx context.Context, monitor healthRunner, pollingInterval time.Duration) error {
	ticker := time.NewTicker(pollingInterval)
	defer ticker.Stop()

	slog.Info("Configured checks", "checks", checks)
	slog.Info("Syslog health monitor initialization complete, starting polling loop...")

	var backoff time.Duration

	for {
		select {
		case <-ctx.Done():
			slog.Info("Polling loop stopped due to context cancellation")
			return nil
		case <-ticker.C:
			slog.Info("Performing scheduled health check run...")

			if err := monitor.Run(); err != nil {
				backoff = nextBackoff(backoff)
				slog.Error("Health check run failed; will retry after backoff", "error", err, "backoff", backoff)
				waitBackoff(ctx, backoff)

				continue
			}

			backoff = 0
		}
	}
}

func nextBackoff(current time.Duration) time.Duration {
	if current == 0 {
		return 2 * time.Second
	}

	next := current * 2
	if next > 30*time.Second {
		next = 30 * time.Second
	}

	return next
}

func waitBackoff(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		slog.Info("Polling loop stopped during backoff due to context cancellation")
	case <-timer.C:
	}
}

// dialWithRetry dials a gRPC target with bounded retries and per-attempt timeout.
// It also verifies a unix domain socket path exists when scheme unix:// is used.
func dialWithRetry(ctx context.Context, target string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	const (
		maxRetries        = 10
		perAttemptTimeout = 5 * time.Second
	)

	for attempt := 1; attempt <= maxRetries; attempt++ {
		slog.Info("Checking platform connector socket availability",
			"attempt", attempt,
			"maxRetries", maxRetries,
			"target", target,
		)

		// For unix:// ensure the socket path exists before dialing.
		if strings.HasPrefix(target, "unix://") {
			socketPath := strings.TrimPrefix(target, "unix://")
			if _, statErr := os.Stat(socketPath); statErr != nil {
				slog.Warn("Platform connector socket file does not exist",
					"attempt", attempt, "maxRetries", maxRetries, "error", statErr)

				if attempt < maxRetries {
					time.Sleep(time.Duration(attempt) * time.Second)
					continue
				}

				return nil, fmt.Errorf("platform connector socket file not found after retries: %w", statErr)
			}
		}

		// Create client connection (non-blocking).
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			slog.Warn("Error creating gRPC client", "attempt", attempt, "maxRetries", maxRetries, "error", err)

			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}

			return nil, fmt.Errorf("failed to create gRPC client after retries: %w", err)
		}

		// Actively connect and wait until Ready (or timeout/cancel).
		if err := waitUntilReady(ctx, conn, perAttemptTimeout); err != nil {
			_ = conn.Close()

			slog.Warn("gRPC client not ready before timeout",
				"attempt", attempt,
				"maxRetries", maxRetries,
				"error", err,
			)

			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}

			return nil, fmt.Errorf("gRPC client not ready after retries: %w", err)
		}

		slog.Info("Successfully connected to platform connector", "attempt", attempt)

		return conn, nil
	}

	// Unreachable, but keeps compiler happy.
	return nil, fmt.Errorf("exhausted retries without creating gRPC client")
}

// waitUntilReady triggers connection establishment and blocks until the ClientConn
// reaches connectivity.Ready or the timeout/context expires.
func waitUntilReady(parent context.Context, conn *grpc.ClientConn, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	conn.Connect()

	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return nil
		}

		// Wait for a state change or context expiry.
		if !conn.WaitForStateChange(ctx, state) {
			// Context expired or canceled.
			return ctx.Err()
		}
	}
}
