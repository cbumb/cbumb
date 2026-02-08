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

package initializer

import (
	"context"
	"fmt"

	"log/slog"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/nvidia/nvsentinel/commons/pkg/configmanager"
	"github.com/nvidia/nvsentinel/commons/pkg/statemanager"
	"github.com/nvidia/nvsentinel/fault-remediation/pkg/config"
	"github.com/nvidia/nvsentinel/fault-remediation/pkg/reconciler"
	"github.com/nvidia/nvsentinel/fault-remediation/pkg/remediation"
	"github.com/nvidia/nvsentinel/store-client/pkg/client"
	storeconfig "github.com/nvidia/nvsentinel/store-client/pkg/config"
	"github.com/nvidia/nvsentinel/store-client/pkg/datastore"
	_ "github.com/nvidia/nvsentinel/store-client/pkg/datastore/providers"
	"github.com/nvidia/nvsentinel/store-client/pkg/watcher"
	ctrlruntimeClient "sigs.k8s.io/controller-runtime/pkg/client"
)

type InitializationParams struct {
	Config             *rest.Config
	TomlConfigPath     string
	DryRun             bool
	EnableLogCollector bool
}

type Components struct {
	FaultRemediationReconciler reconciler.FaultRemediationReconciler
}

func InitializeAll(
	ctx context.Context,
	params InitializationParams,
	ctrlruntimeClient ctrlruntimeClient.Client,
) (*Components, error) {
	slog.Info("Starting fault remediation module initialization")

	tokenConfig, pipeline, err := loadTokenConfigAndPipeline()
	if err != nil {
		return nil, err
	}

	tomlConfig, err := loadAndValidateToml(params.TomlConfigPath)
	if err != nil {
		return nil, err
	}

	logInitMode(params)

	remediationClient, stateManager, err := initRemediationClientAndStateManager(
		params.Config, tomlConfig, ctrlruntimeClient, params.DryRun)
	if err != nil {
		return nil, err
	}

	ds, watcherInstance, healthEventStore, datastoreConfig, clientTokenConfig, err := initDatastoreAndWatcher(
		ctx, tokenConfig, pipeline)
	if err != nil {
		return nil, err
	}

	reconcilerCfg := reconciler.ReconcilerConfig{
		DataStoreConfig:    *datastoreConfig,
		TokenConfig:        clientTokenConfig,
		Pipeline:           pipeline,
		RemediationClient:  remediationClient,
		StateManager:       stateManager,
		EnableLogCollector: params.EnableLogCollector,
		UpdateMaxRetries:   tomlConfig.UpdateRetry.MaxRetries,
		UpdateRetryDelay:   time.Duration(tomlConfig.UpdateRetry.RetryDelaySeconds) * time.Second,
	}

	slog.Info("Initialization completed successfully")

	return &Components{
		FaultRemediationReconciler: reconciler.NewFaultRemediationReconciler(
			ds, watcherInstance, healthEventStore, reconcilerCfg, params.DryRun),
	}, nil
}

func loadTokenConfigAndPipeline() (storeconfig.TokenConfig, datastore.Pipeline, error) {
	tokenConfig, err := storeconfig.TokenConfigFromEnv("fault-remediation")
	if err != nil {
		return storeconfig.TokenConfig{}, nil, fmt.Errorf("failed to load token configuration: %w", err)
	}
	builder := client.GetPipelineBuilder()
	pipeline := builder.BuildQuarantinedAndDrainedNodesPipeline()
	return tokenConfig, pipeline, nil
}

func loadAndValidateToml(tomlConfigPath string) (config.TomlConfig, error) {
	var tomlConfig config.TomlConfig
	if err := configmanager.LoadTOMLConfig(tomlConfigPath, &tomlConfig); err != nil {
		return config.TomlConfig{}, fmt.Errorf("error while loading the toml Config: %w", err)
	}
	if err := tomlConfig.Validate(); err != nil {
		return config.TomlConfig{}, fmt.Errorf("configuration validation failed: %w", err)
	}
	return tomlConfig, nil
}

func logInitMode(params InitializationParams) {
	if params.DryRun {
		slog.Info("Running in dry-run mode")
	}
	if params.EnableLogCollector {
		slog.Info("Log collector enabled")
	}
}

func initRemediationClientAndStateManager(
	restConfig *rest.Config,
	tomlConfig config.TomlConfig,
	ctrlruntimeClient ctrlruntimeClient.Client,
	dryRun bool,
) (*remediation.Client, *statemanager.StateManager, error) {
	remediationClient, err := remediation.NewRemediationClient(ctrlruntimeClient, dryRun, tomlConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error while initializing remediation client: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("error init kube client for state manager: %w", err)
	}
	stateManager := statemanager.NewStateManager(kubeClient)
	slog.Info("Successfully initialized client")
	return remediationClient, stateManager, nil
}

func initDatastoreAndWatcher(
	ctx context.Context,
	tokenConfig storeconfig.TokenConfig,
	pipeline datastore.Pipeline,
) (datastore.DataStore, datastore.ChangeStreamWatcher, datastore.HealthEventStore, *datastore.DataStoreConfig, client.TokenConfig, error) {
	datastoreConfig, err := datastore.LoadDatastoreConfig()
	if err != nil {
		return nil, nil, nil, nil, client.TokenConfig{}, fmt.Errorf("failed to load datastore configuration: %w", err)
	}
	clientTokenConfig := client.TokenConfig{
		ClientName:      tokenConfig.ClientName,
		TokenDatabase:   tokenConfig.TokenDatabase,
		TokenCollection: tokenConfig.TokenCollection,
	}
	ds, err := datastore.NewDataStore(ctx, *datastoreConfig)
	if err != nil {
		return nil, nil, nil, nil, client.TokenConfig{}, fmt.Errorf("error initializing datastore: %w", err)
	}
	watcherConfig := watcher.WatcherConfig{
		Pipeline:       pipeline,
		CollectionName: "HealthEvents",
	}
	watcherInstance, err := watcher.CreateChangeStreamWatcher(ctx, ds, watcherConfig)
	if err != nil {
		return nil, nil, nil, nil, client.TokenConfig{}, fmt.Errorf("error initializing change stream watcher: %w", err)
	}
	healthEventStore := ds.HealthEventStore()
	return ds, watcherInstance, healthEventStore, datastoreConfig, clientTokenConfig, nil
}
