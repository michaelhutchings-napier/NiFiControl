package controller

import (
	"context"
	"testing"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"k8s.io/utils/ptr"
)

type fakeControllerConfigClient struct {
	getEntity   *nifi.ControllerConfigurationEntity
	getErr      error
	updated     *nifi.ControllerConfigurationEntity
	getCalls    int
	updateCalls int
}

func (f *fakeControllerConfigClient) GetControllerConfig(_ context.Context, _ string) (*nifi.ControllerConfigurationEntity, error) {
	f.getCalls++
	return f.getEntity, f.getErr
}

func (f *fakeControllerConfigClient) UpdateControllerConfig(_ context.Context, _ string, entity nifi.ControllerConfigurationEntity) (*nifi.ControllerConfigurationEntity, error) {
	f.updateCalls++
	e := entity
	f.updated = &e
	return &e, nil
}

func TestReconcileControllerConfigNoopWhenUnset(t *testing.T) {
	fake := &fakeControllerConfigClient{}
	r := &NiFiClusterReconciler{ControllerConfigClient: fake}
	cluster := hardeningCluster()
	if err := r.reconcileManagedClusterControllerConfig(context.Background(), cluster, "http://nifi:8080"); err != nil {
		t.Fatal(err)
	}
	if fake.getCalls != 0 || fake.updateCalls != 0 {
		t.Fatalf("expected no API calls when unset, got get=%d update=%d", fake.getCalls, fake.updateCalls)
	}
}

func TestReconcileControllerConfigUpdatesOnDrift(t *testing.T) {
	fake := &fakeControllerConfigClient{
		getEntity: &nifi.ControllerConfigurationEntity{
			Revision:  nifi.Revision{Version: 4},
			Component: nifi.ControllerConfigurationDTO{MaxTimerDrivenThreadCount: ptr.To[int32](10)},
		},
	}
	r := &NiFiClusterReconciler{ControllerConfigClient: fake}
	cluster := hardeningCluster()
	cluster.Spec.MaxTimerDrivenThreadCount = ptr.To[int32](25)

	if err := r.reconcileManagedClusterControllerConfig(context.Background(), cluster, "http://nifi:8080"); err != nil {
		t.Fatal(err)
	}
	if fake.updateCalls != 1 {
		t.Fatalf("expected one update, got %d", fake.updateCalls)
	}
	if fake.updated == nil || fake.updated.Revision.Version != 4 ||
		fake.updated.Component.MaxTimerDrivenThreadCount == nil || *fake.updated.Component.MaxTimerDrivenThreadCount != 25 {
		t.Fatalf("update payload = %#v", fake.updated)
	}
}

func TestReconcileControllerConfigNoopWhenAlreadyCorrect(t *testing.T) {
	fake := &fakeControllerConfigClient{
		getEntity: &nifi.ControllerConfigurationEntity{
			Revision:  nifi.Revision{Version: 7},
			Component: nifi.ControllerConfigurationDTO{MaxTimerDrivenThreadCount: ptr.To[int32](25)},
		},
	}
	r := &NiFiClusterReconciler{ControllerConfigClient: fake}
	cluster := hardeningCluster()
	cluster.Spec.MaxTimerDrivenThreadCount = ptr.To[int32](25)

	if err := r.reconcileManagedClusterControllerConfig(context.Background(), cluster, "http://nifi:8080"); err != nil {
		t.Fatal(err)
	}
	if fake.updateCalls != 0 {
		t.Fatalf("expected no update when already correct, got %d", fake.updateCalls)
	}
}
