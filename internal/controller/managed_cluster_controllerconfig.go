package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

func (r *NiFiClusterReconciler) controllerConfigClient() nifi.ControllerConfigClient {
	if r.ControllerConfigClient != nil {
		return r.ControllerConfigClient
	}
	return nifi.HTTPControllerConfigClient{}
}

// reconcileManagedClusterControllerConfig enforces the cluster-wide controller settings
// modelled on the spec (currently maxTimerDrivenThreadCount) through the NiFi API. It is a
// no-op when nothing is configured, and declarative: it reads the live config on every
// reconcile and rewrites it only when it drifts from the spec. The caller must already have
// registered an authenticated client for the endpoint (configureClusterHTTPClient).
func (r *NiFiClusterReconciler) reconcileManagedClusterControllerConfig(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, endpoint string) error {
	desired := cluster.Spec.MaxTimerDrivenThreadCount
	if desired == nil {
		return nil
	}

	client := r.controllerConfigClient()
	current, err := client.GetControllerConfig(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("read controller config: %w", err)
	}

	if live := current.Component.MaxTimerDrivenThreadCount; live != nil && *live == *desired {
		return nil
	}

	update := nifi.ControllerConfigurationEntity{
		Revision:  current.Revision,
		Component: nifi.ControllerConfigurationDTO{MaxTimerDrivenThreadCount: desired},
	}
	if _, err := client.UpdateControllerConfig(ctx, endpoint, update); err != nil {
		return fmt.Errorf("update controller config: %w", err)
	}
	return nil
}
