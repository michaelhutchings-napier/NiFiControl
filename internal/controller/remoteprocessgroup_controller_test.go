package controller

import (
	"context"
	"net/http"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeRemoteProcessGroupClient struct {
	store         *nifi.RemoteProcessGroupEntity
	created       []nifi.RemoteProcessGroupEntity
	updated       []nifi.RemoteProcessGroupEntity
	runStatus     []string
	deleted       []string
	portUpdates   []nifi.RemoteProcessGroupPortEntity
	portRunStatus []string
}

func (f *fakeRemoteProcessGroupClient) GetRemoteProcessGroup(ctx context.Context, baseURI, id string) (*nifi.RemoteProcessGroupEntity, error) {
	if f.store != nil && nifi.RemoteProcessGroupEntityID(*f.store) == id {
		s := *f.store
		return &s, nil
	}
	return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
}

func (f *fakeRemoteProcessGroupClient) ListRemoteProcessGroups(ctx context.Context, baseURI, parentID string) ([]nifi.RemoteProcessGroupEntity, error) {
	if f.store != nil {
		return []nifi.RemoteProcessGroupEntity{*f.store}, nil
	}
	return nil, nil
}

func (f *fakeRemoteProcessGroupClient) CreateRemoteProcessGroup(ctx context.Context, baseURI, parentID string, entity nifi.RemoteProcessGroupEntity) (*nifi.RemoteProcessGroupEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "rpg-created"
	created.Component.ID = "rpg-created"
	created.Component.ParentGroupID = parentID
	created.Component.Transmitting = false
	f.store = &created
	s := created
	return &s, nil
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroup(ctx context.Context, baseURI string, entity nifi.RemoteProcessGroupEntity) (*nifi.RemoteProcessGroupEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	if f.store != nil {
		// Preserve observed fields NiFi computes.
		updated.Component.Transmitting = f.store.Component.Transmitting
		updated.Component.TargetSecure = f.store.Component.TargetSecure
	}
	f.store = &updated
	s := updated
	return &s, nil
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupRunStatus(ctx context.Context, baseURI, id string, revisionVersion int64, state string) (*nifi.RemoteProcessGroupEntity, error) {
	f.runStatus = append(f.runStatus, state)
	if f.store == nil {
		return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
	}
	s := *f.store
	s.Component.Transmitting = state == "TRANSMITTING"
	s.Revision.Version = revisionVersion + 1
	f.store = &s
	out := s
	return &out, nil
}

func (f *fakeRemoteProcessGroupClient) DeleteRemoteProcessGroup(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	f.store = nil
	return nil
}

func (f *fakeRemoteProcessGroupClient) portByID(id string, output bool) *nifi.RemoteProcessGroupPort {
	if f.store == nil || f.store.Component.Contents == nil {
		return nil
	}
	ports := f.store.Component.Contents.InputPorts
	if output {
		ports = f.store.Component.Contents.OutputPorts
	}
	for i := range ports {
		if ports[i].ID == id {
			return &ports[i]
		}
	}
	return nil
}

func (f *fakeRemoteProcessGroupClient) updatePort(entity nifi.RemoteProcessGroupPortEntity, output bool) (*nifi.RemoteProcessGroupPortEntity, error) {
	f.portUpdates = append(f.portUpdates, entity)
	if p := f.portByID(entity.RemoteProcessGroupPort.ID, output); p != nil {
		p.UseCompression = entity.RemoteProcessGroupPort.UseCompression
		p.ConcurrentlySchedulableTaskCount = entity.RemoteProcessGroupPort.ConcurrentlySchedulableTaskCount
		p.BatchSettings = entity.RemoteProcessGroupPort.BatchSettings
	}
	if f.store != nil {
		f.store.Revision.Version++
	}
	s := entity
	return &s, nil
}

func (f *fakeRemoteProcessGroupClient) portRunStatusUpdate(portID, state string, output bool) (*nifi.RemoteProcessGroupPortEntity, error) {
	f.portRunStatus = append(f.portRunStatus, state)
	if p := f.portByID(portID, output); p != nil {
		p.Transmitting = state == "TRANSMITTING"
	}
	if f.store != nil {
		f.store.Revision.Version++
	}
	return &nifi.RemoteProcessGroupPortEntity{}, nil
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupInputPort(ctx context.Context, baseURI, rpgID string, entity nifi.RemoteProcessGroupPortEntity) (*nifi.RemoteProcessGroupPortEntity, error) {
	return f.updatePort(entity, false)
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupOutputPort(ctx context.Context, baseURI, rpgID string, entity nifi.RemoteProcessGroupPortEntity) (*nifi.RemoteProcessGroupPortEntity, error) {
	return f.updatePort(entity, true)
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupInputPortRunStatus(ctx context.Context, baseURI, rpgID, portID string, revisionVersion int64, state string) (*nifi.RemoteProcessGroupPortEntity, error) {
	return f.portRunStatusUpdate(portID, state, false)
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupOutputPortRunStatus(ctx context.Context, baseURI, rpgID, portID string, revisionVersion int64, state string) (*nifi.RemoteProcessGroupPortEntity, error) {
	return f.portRunStatusUpdate(portID, state, true)
}

func remoteProcessGroupTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRemoteProcessGroup{}).
		Build()
}

func newRemoteProcessGroup(name string) *nifiv1alpha1.NiFiRemoteProcessGroup {
	return &nifiv1alpha1.NiFiRemoteProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRemoteProcessGroupSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: "production"},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Root: true},
			TargetURIs:            []string{"https://central-nifi.example.com:8443/nifi"},
			TransportProtocol:     "HTTP",
			CommunicationsTimeout: "30 sec",
			YieldDuration:         "10 sec",
		},
	}
}

func TestNiFiRemoteProcessGroupReconcileCreates(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 1 {
		t.Fatalf("create remote process groups = %#v", rpgs.created)
	}
	if got := rpgs.created[0].Component.TargetURIs; got != "https://central-nifi.example.com:8443/nifi" {
		t.Fatalf("targetUris sent to NiFi = %q", got)
	}
	if got := rpgs.created[0].Component.TransportProtocol; got != "HTTP" {
		t.Fatalf("transportProtocol sent to NiFi = %q", got)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "rpg-created" {
		t.Fatalf("status = %+v", got.Status)
	}
	if got.Status.TransmissionStatus != "Stopped" {
		t.Fatalf("transmission status = %q", got.Status.TransmissionStatus)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "RemoteProcessGroupReady")
}

func TestNiFiRemoteProcessGroupAdoptsByID(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.AdoptionPolicy = nifiv1alpha1.AdoptionPolicy{Mode: nifiv1alpha1.AdoptionPolicyAdoptByID, NiFiID: "existing-rpg"}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	// An RPG already exists in NiFi with the id the CR asks to adopt, matching the desired config.
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "existing-rpg",
		Revision: nifi.Revision{Version: 7},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "existing-rpg", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec",
		},
	}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 0 {
		t.Fatalf("adoption must not create a new RPG: %#v", rpgs.created)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "existing-rpg" {
		t.Fatalf("expected the CR to adopt existing-rpg, status = %+v", got.Status)
	}
}

func TestNiFiRemoteProcessGroupUpdateStopsTransmissionFirst(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.YieldDuration = "5 sec" // a spec change (generation moved past observed) -> always applied
	rpg.Generation = 2
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{
		CommonStatus:         nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1},
		ParentProcessGroupID: "root",
		TransmissionStatus:   "Transmitting",
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "rpg-1",
		Revision: nifi.Revision{Version: 4},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "rpg-1", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec", Transmitting: true,
		},
	}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.runStatus) == 0 || rpgs.runStatus[0] != "STOPPED" {
		t.Fatalf("a transmitting RPG must be stopped before update: run-status calls = %#v", rpgs.runStatus)
	}
	if len(rpgs.updated) == 0 {
		t.Fatalf("expected a config update, got none")
	}
	if got := rpgs.updated[len(rpgs.updated)-1].Component.YieldDuration; got != "5 sec" {
		t.Fatalf("update did not apply the new yieldDuration: %q", got)
	}
}

func driftTestRPG(name, driftMode string) *nifiv1alpha1.NiFiRemoteProcessGroup {
	rpg := newRemoteProcessGroup(name)
	if driftMode != "" {
		rpg.Spec.DriftPolicy = nifiv1alpha1.DriftPolicy{Mode: nifiv1alpha1.DriftPolicyMode(driftMode)}
	}
	// Spec unchanged (observedGeneration == generation): a config difference is true NiFi-side drift.
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{
		CommonStatus:         nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1},
		ParentProcessGroupID: "root",
	}
	return rpg
}

func driftTestStore() *nifi.RemoteProcessGroupEntity {
	return &nifi.RemoteProcessGroupEntity{
		ID:       "rpg-1",
		Revision: nifi.Revision{Version: 4},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "rpg-1", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "99 sec", // drifted from the desired "10 sec"
		},
	}
}

func TestNiFiRemoteProcessGroupDriftWarnDoesNotCorrect(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := driftTestRPG("central", "") // unset -> Warn default
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: driftTestStore()}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.updated) != 0 {
		t.Fatalf("Warn drift policy must not correct config: %#v", rpgs.updated)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if got.Status.Drift.Status != "OutOfSync" || len(got.Status.Drift.Differences) == 0 {
		t.Fatalf("expected drift to be reported, drift = %+v", got.Status.Drift)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionDriftDetected, metav1.ConditionTrue, "DriftDetected")
}

func TestNiFiRemoteProcessGroupDriftReconcileCorrects(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := driftTestRPG("central", "Reconcile")
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: driftTestStore()}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.updated) == 0 {
		t.Fatalf("Reconcile drift policy must correct config, got no update")
	}
	if got := rpgs.updated[len(rpgs.updated)-1].Component.YieldDuration; got != "10 sec" {
		t.Fatalf("drift correction did not restore the desired yieldDuration: %q", got)
	}
}

func TestNiFiRemoteProcessGroupWaitsForProxyPasswordSecret(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.Proxy = &nifiv1alpha1.RemoteProcessGroupProxy{
		Host:              "proxy.example.com",
		Port:              3128,
		PasswordSecretRef: &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "proxy-secret"}, Key: "password"}},
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 0 {
		t.Fatalf("RPG must not be created while the proxy password secret is missing: %#v", rpgs.created)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if got.Status.Ready || got.Status.Dependencies.Ready {
		t.Fatalf("expected waiting-for-dependencies status, got %+v", got.Status)
	}
}

func TestNiFiRemoteProcessGroupAdoptsByName(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.AdoptionPolicy = nifiv1alpha1.AdoptionPolicy{Mode: nifiv1alpha1.AdoptionPolicyAdoptByName}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	// An RPG named "central" already exists under the parent, matching the desired config.
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "existing-rpg",
		Revision: nifi.Revision{Version: 4},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "existing-rpg", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec",
		},
	}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 0 {
		t.Fatalf("adoption by name must not create a new RPG: %#v", rpgs.created)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "existing-rpg" {
		t.Fatalf("expected the CR to adopt existing-rpg by name, status = %+v", got.Status)
	}
}

func TestNiFiRemoteProcessGroupConfiguresAndTransmitsConnectedPort(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.InputPorts = []nifiv1alpha1.RemoteProcessGroupPortConfig{{Name: "ingest", Transmitting: true, ConcurrentTasks: 2, UseCompression: true}}
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{
		CommonStatus:         nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1},
		ParentProcessGroupID: "root",
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	// The RPG has discovered a connected input port "ingest" from the target.
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "rpg-1",
		Revision: nifi.Revision{Version: 3},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "rpg-1", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec",
			Contents: &nifi.RemoteProcessGroupContents{
				InputPorts: []nifi.RemoteProcessGroupPort{{ID: "ingest-id", Name: "ingest", Connected: true, Exists: true}},
			},
		},
	}}
	rec := record.NewFakeRecorder(10)
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs, Recorder: rec}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.portUpdates) == 0 {
		t.Fatalf("expected the port to be configured, portUpdates empty")
	}
	transmitEvent := false
	for len(rec.Events) > 0 {
		if strings.Contains(<-rec.Events, "PortTransmissionStarted") {
			transmitEvent = true
		}
	}
	if !transmitEvent {
		t.Fatalf("expected a PortTransmissionStarted event to be recorded")
	}
	last := rpgs.portUpdates[len(rpgs.portUpdates)-1].RemoteProcessGroupPort
	if !last.UseCompression || last.ConcurrentlySchedulableTaskCount != 2 {
		t.Fatalf("port config not applied: %#v", last)
	}
	if len(rpgs.portRunStatus) == 0 || rpgs.portRunStatus[len(rpgs.portRunStatus)-1] != "TRANSMITTING" {
		t.Fatalf("connected port should be started: portRunStatus = %#v", rpgs.portRunStatus)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if !got.Status.Ready {
		t.Fatalf("status = %+v", got.Status)
	}
	if len(got.Status.DiscoveredInputPorts) != 1 || got.Status.DiscoveredInputPorts[0].NiFiID != "ingest-id" {
		t.Fatalf("discovered ports = %+v", got.Status.DiscoveredInputPorts)
	}
}

func TestNiFiRemoteProcessGroupPortPendingUntilConnected(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.InputPorts = []nifiv1alpha1.RemoteProcessGroupPortConfig{{Name: "ingest", Transmitting: true}}
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{
		CommonStatus:         nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1},
		ParentProcessGroupID: "root",
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	// The port is discovered but not yet connected (no NiFiConnection to it).
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "rpg-1",
		Revision: nifi.Revision{Version: 3},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "rpg-1", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec",
			Contents: &nifi.RemoteProcessGroupContents{
				InputPorts: []nifi.RemoteProcessGroupPort{{ID: "ingest-id", Name: "ingest", Connected: false, Exists: true}},
			},
		},
	}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.portRunStatus) != 0 {
		t.Fatalf("a not-connected port must not be started: portRunStatus = %#v", rpgs.portRunStatus)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if got.Status.Ready {
		t.Fatalf("RPG should stay NotReady until the port is connected: %+v", got.Status)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "PortsPending")
	// Discovered ports must still be published so a NiFiConnection can resolve the port id.
	if len(got.Status.DiscoveredInputPorts) != 1 || got.Status.DiscoveredInputPorts[0].NiFiID != "ingest-id" {
		t.Fatalf("discovered ports must be published while pending: %+v", got.Status.DiscoveredInputPorts)
	}
}

func TestNiFiConnectionResolvesRemoteInputPort(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("edge", "pg-edge")
	processor := readyTestProcessor("generate", "proc-1", "pg-edge")
	rpg := &nifiv1alpha1.NiFiRemoteProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "central-rpg", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRemoteProcessGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			TargetURIs: []string{"https://central:8443/nifi"},
		},
		// The RPG has discovered the remote input port but it is not connected yet (this connection
		// is what will connect it): resolution must not require the RPG to be Ready.
		Status: nifiv1alpha1.NiFiRemoteProcessGroupStatus{
			CommonStatus:         nifiv1alpha1.CommonStatus{NiFiID: "rpg-1", ObservedGeneration: 1},
			DiscoveredInputPorts: []nifiv1alpha1.RemoteProcessGroupPortStatus{{Name: "ingest", NiFiID: "remote-port-1", Exists: true}},
		},
	}
	connection := &nifiv1alpha1.NiFiConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "to-central", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiConnectionSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Source:                nifiv1alpha1.ConnectableReference{Type: nifiv1alpha1.ConnectableTypeProcessor, Name: processor.Name},
			Destination:           nifiv1alpha1.ConnectableReference{Type: nifiv1alpha1.ConnectableTypeRemoteInputPort, Name: rpg.Name, PortName: "ingest"},
			SelectedRelationships: []string{"success"},
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, processor, rpg, connection)
	nifiClient := &fakeConnectionClient{}
	reconciler := &NiFiConnectionReconciler{Client: k8sClient, Scheme: scheme, ConnectionClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: connection.Name, Namespace: connection.Namespace}}

	reconcileConnectionTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	dest := nifiClient.created[0].Component.Destination
	if dest.ID != "remote-port-1" || dest.Type != "REMOTE_INPUT_PORT" || dest.GroupID != "rpg-1" {
		t.Fatalf("destination = %+v, want id=remote-port-1 type=REMOTE_INPUT_PORT groupId=rpg-1", dest)
	}
}

func TestNiFiRemoteProcessGroupDeleteStopsThenDeletes(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Finalizers = []string{NiFiControlFinalizer}
	rpg.Spec.DeletionPolicy = nifiv1alpha1.DeletionPolicyDelete
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1}}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{ID: "rpg-1", Revision: nifi.Revision{Version: 2}, Component: nifi.RemoteProcessGroupComponent{ID: "rpg-1", Transmitting: true}}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}

	if err := k8sClient.Delete(context.Background(), rpg); err != nil {
		t.Fatal(err)
	}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.runStatus) == 0 || rpgs.runStatus[0] != "STOPPED" {
		t.Fatalf("a transmitting RPG should be stopped before deletion: run-status calls = %#v", rpgs.runStatus)
	}
	if len(rpgs.deleted) != 1 || rpgs.deleted[0] != "rpg-1" {
		t.Fatalf("expected the RPG to be deleted: %#v", rpgs.deleted)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer should be removed and the RPG deleted; got err=%v", err)
	}
}
