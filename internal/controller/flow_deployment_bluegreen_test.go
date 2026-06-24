package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// fakeNiFi is an in-memory NiFi topology used to drive the BlueGreen state machine. It
// implements the snapshot, process-group, scheduler, and BlueGreen client interfaces.
type fakeNiFi struct {
	pgs         map[string]*fakePG
	connections []nifi.ConnectionEntity
	queues      map[string]int64
	runStatus   map[string]string
	csEnabled   map[string]bool
	candidate   *fakeCandidate
	ids         int
}

type fakePG struct {
	id           string
	name         string
	invalidCount int32
	revision     int64
	inputs       []nifi.PortEntity
	outputs      []nifi.PortEntity
	deleted      bool
}

// fakeCandidate describes the green process group a future import should create.
type fakeCandidate struct {
	id           string
	inputs       []string
	outputs      []string
	invalidCount int32
}

func newFakeNiFi() *fakeNiFi {
	return &fakeNiFi{
		pgs:       map[string]*fakePG{},
		queues:    map[string]int64{},
		runStatus: map[string]string{},
		csEnabled: map[string]bool{},
	}
}

func (f *fakeNiFi) nextID(prefix string) string {
	f.ids++
	return fmt.Sprintf("%s-%d", prefix, f.ids)
}

func port(id, name string) nifi.PortEntity {
	return nifi.PortEntity{ID: id, Component: nifi.PortComponent{ID: id, Name: name}}
}

// --- nifi.FlowSnapshotClient ---

func (f *fakeNiFi) ImportProcessGroup(_ context.Context, _ string, _ string, snapshot json.RawMessage) (*nifi.ProcessGroupEntity, error) {
	var decoded struct {
		FlowContents struct {
			Name string `json:"name"`
		} `json:"flowContents"`
	}
	_ = json.Unmarshal(snapshot, &decoded)
	cand := f.candidate
	if cand == nil {
		cand = &fakeCandidate{inputs: []string{"in"}, outputs: []string{"out"}}
	}
	id := cand.id
	if id == "" {
		id = f.nextID("green")
	}
	pg := &fakePG{id: id, name: decoded.FlowContents.Name, invalidCount: cand.invalidCount}
	for _, name := range cand.inputs {
		pg.inputs = append(pg.inputs, port(f.nextID("gin"), name))
	}
	for _, name := range cand.outputs {
		pg.outputs = append(pg.outputs, port(f.nextID("gout"), name))
	}
	f.pgs[id] = pg
	return &nifi.ProcessGroupEntity{ID: id, Component: nifi.ProcessGroupComponent{ID: id, Name: pg.name}}, nil
}

func (f *fakeNiFi) CreateProcessGroupReplaceRequest(context.Context, string, string, int64, json.RawMessage) (*nifi.ProcessGroupReplaceRequestEntity, error) {
	return nil, nil
}
func (f *fakeNiFi) GetProcessGroupReplaceRequest(context.Context, string, string) (*nifi.ProcessGroupReplaceRequestEntity, error) {
	return nil, nil
}
func (f *fakeNiFi) DeleteProcessGroupReplaceRequest(context.Context, string, string) error {
	return nil
}

// --- nifi.ProcessGroupClient ---

func (f *fakeNiFi) GetProcessGroup(_ context.Context, _ string, id string) (*nifi.ProcessGroupEntity, error) {
	pg, ok := f.pgs[id]
	if !ok || pg.deleted {
		return nil, &nifi.HTTPStatusError{StatusCode: 404}
	}
	return &nifi.ProcessGroupEntity{
		ID:           pg.id,
		Revision:     nifi.Revision{Version: pg.revision},
		Component:    nifi.ProcessGroupComponent{ID: pg.id, Name: pg.name},
		InvalidCount: pg.invalidCount,
	}, nil
}

func (f *fakeNiFi) CreateProcessGroup(context.Context, string, string, nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	return nil, nil
}

func (f *fakeNiFi) UpdateProcessGroup(_ context.Context, _ string, entity nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	pg, ok := f.pgs[entity.ID]
	if !ok {
		return nil, &nifi.HTTPStatusError{StatusCode: 404}
	}
	if entity.Component.Name != "" {
		pg.name = entity.Component.Name
	}
	pg.revision++
	return &nifi.ProcessGroupEntity{ID: pg.id, Revision: nifi.Revision{Version: pg.revision}, Component: nifi.ProcessGroupComponent{ID: pg.id, Name: pg.name}}, nil
}

func (f *fakeNiFi) DeleteProcessGroup(_ context.Context, _ string, id string, _ int64) error {
	if pg, ok := f.pgs[id]; ok {
		pg.deleted = true
	}
	return nil
}

// --- nifi.ProcessGroupScheduler ---

func (f *fakeNiFi) ScheduleProcessGroup(_ context.Context, _ string, id string, state string) error {
	f.runStatus["pg:"+id] = state
	return nil
}

// --- nifi.BlueGreenClient ---

func (f *fakeNiFi) ListProcessGroupConnections(_ context.Context, _ string, _ string) ([]nifi.ConnectionEntity, error) {
	active := []nifi.ConnectionEntity{}
	for _, conn := range f.connections {
		active = append(active, conn)
	}
	return active, nil
}

func (f *fakeNiFi) ListProcessGroupInputPorts(_ context.Context, _ string, pgID string) ([]nifi.PortEntity, error) {
	if pg, ok := f.pgs[pgID]; ok {
		return pg.inputs, nil
	}
	return nil, nil
}

func (f *fakeNiFi) ListProcessGroupOutputPorts(_ context.Context, _ string, pgID string) ([]nifi.PortEntity, error) {
	if pg, ok := f.pgs[pgID]; ok {
		return pg.outputs, nil
	}
	return nil, nil
}

func (f *fakeNiFi) GetConnection(_ context.Context, _ string, id string) (*nifi.ConnectionEntity, error) {
	for i := range f.connections {
		if f.connections[i].ID == id {
			conn := f.connections[i]
			return &conn, nil
		}
	}
	return nil, &nifi.HTTPStatusError{StatusCode: 404}
}

func (f *fakeNiFi) CreateConnection(_ context.Context, _ string, parentID string, entity nifi.ConnectionEntity) (*nifi.ConnectionEntity, error) {
	id := f.nextID("conn")
	entity.ID = id
	entity.Component.ID = id
	entity.Component.ParentGroupID = parentID
	f.connections = append(f.connections, entity)
	f.queues[id] = 0
	return &entity, nil
}

func (f *fakeNiFi) DeleteConnection(_ context.Context, _ string, id string, _ int64) error {
	remaining := f.connections[:0]
	for _, conn := range f.connections {
		if conn.ID != id {
			remaining = append(remaining, conn)
		}
	}
	f.connections = remaining
	delete(f.queues, id)
	return nil
}

func (f *fakeNiFi) ConnectionQueueCount(_ context.Context, _ string, id string) (int64, error) {
	return f.queues[id], nil
}

func (f *fakeNiFi) DropConnectionQueue(_ context.Context, _ string, id string) error {
	f.queues[id] = 0
	return nil
}

func (f *fakeNiFi) SetComponentRunStatus(_ context.Context, _ string, _ string, id string, state string) error {
	f.runStatus[id] = state
	return nil
}

func (f *fakeNiFi) EnableControllerServices(_ context.Context, _ string, pgID string) error {
	f.csEnabled[pgID] = true
	if pg, ok := f.pgs[pgID]; ok {
		pg.invalidCount = 0
	}
	return nil
}

func (f *fakeNiFi) connectionByID(id string) (nifi.ConnectionEntity, bool) {
	for _, conn := range f.connections {
		if conn.ID == id {
			return conn, true
		}
	}
	return nifi.ConnectionEntity{}, false
}

// --- test helpers ---

func bgConn(id, srcID, srcType, srcGroup, dstID, dstType, dstGroup string) nifi.ConnectionEntity {
	return nifi.ConnectionEntity{
		ID: id,
		Component: nifi.ConnectionComponent{
			ID:                    id,
			Source:                nifi.Connectable{ID: srcID, Type: srcType, GroupID: srcGroup},
			Destination:           nifi.Connectable{ID: dstID, Type: dstType, GroupID: dstGroup},
			SelectedRelationships: []string{"success"},
		},
	}
}

func bgBlueDeployment() *nifiv1alpha1.NiFiFlowDeployment {
	return &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Target:     nifiv1alpha1.FlowDeploymentTarget{ProcessGroupName: "payments"},
			Rollout:    nifiv1alpha1.RolloutStrategy{Strategy: "BlueGreen"},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			ProcessGroupID:  "blue",
			DeployedVersion: "v1",
			ArtifactDigest:  "sha256:old",
		},
	}
}

func bgSeedBlue(f *fakeNiFi) {
	f.pgs["blue"] = &fakePG{id: "blue", name: "payments", inputs: []nifi.PortEntity{port("blue-in", "in")}, outputs: []nifi.PortEntity{port("blue-out", "out")}}
	f.pgs["root"] = &fakePG{id: "root", name: "root"}
	f.connections = []nifi.ConnectionEntity{
		bgConn("c-in", "ext-src", nifi.ConnectableProcessor, "root", "blue-in", nifi.ConnectableInputPort, "blue"),
		bgConn("c-out", "blue-out", nifi.ConnectableOutputPort, "blue", "ext-dst", nifi.ConnectableProcessor, "root"),
	}
}

func TestBlueGreenRolloutSwitchesTrafficAndPromotes(t *testing.T) {
	deployment := bgBlueDeployment()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).WithStatusSubresource(&nifiv1alpha1.NiFiFlowDeployment{}).Build()

	nf := newFakeNiFi()
	bgSeedBlue(nf)
	nf.candidate = &fakeCandidate{id: "green", inputs: []string{"in"}, outputs: []string{"out"}}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}

	snapshot := json.RawMessage(`{"flowContents":{"name":"payments","inputPorts":[{"name":"in"}],"outputPorts":[{"name":"out"}]}}`)
	key := types.NamespacedName{Name: "payments", Namespace: "default"}

	driveBlueGreen(t, r, k8sClient, key, snapshot)

	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ProcessGroupID != "green" {
		t.Fatalf("ProcessGroupID = %q, want green", current.Status.ProcessGroupID)
	}
	if current.Status.RetiringProcessGroupID != "blue" {
		t.Fatalf("RetiringProcessGroupID = %q, want blue", current.Status.RetiringProcessGroupID)
	}
	if !current.Status.Ready {
		t.Fatal("deployment should be Ready after promotion")
	}
	if current.Status.ActiveRollout != nil {
		t.Fatalf("ActiveRollout should be cleared, got %#v", current.Status.ActiveRollout)
	}
	// Both boundary connections now point at green ports; none reference blue ports.
	if _, ok := nf.connectionByID("c-in"); ok {
		t.Fatal("blue inbound connection c-in should have been deleted")
	}
	if _, ok := nf.connectionByID("c-out"); ok {
		t.Fatal("blue outbound connection c-out should have been deleted")
	}
	greenInbound, greenOutbound := false, false
	for _, conn := range nf.connections {
		if conn.Component.Destination.ID == "gin-2" || conn.Component.Destination.GroupID == "green" {
			greenInbound = true
		}
		if conn.Component.Source.GroupID == "green" {
			greenOutbound = true
		}
	}
	if !greenInbound || !greenOutbound {
		t.Fatalf("expected inbound and outbound connections to green, got %#v", nf.connections)
	}
	// The external source stopped during the inbound switch is restarted on promotion.
	if nf.runStatus["ext-src"] != nifi.RunStateRunning {
		t.Fatalf("ext-src run status = %q, want RUNNING", nf.runStatus["ext-src"])
	}

	// One more reconcile retires blue.
	if err := r.retireBlueProcessGroup(context.Background(), current, "https://nifi"); err != nil {
		t.Fatal(err)
	}
	if !nf.pgs["blue"].deleted {
		t.Fatal("blue process group should be deleted after retirement")
	}
}

func TestBlueGreenRollsBackTrafficWhenCandidatePortMissing(t *testing.T) {
	deployment := bgBlueDeployment()
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).WithStatusSubresource(&nifiv1alpha1.NiFiFlowDeployment{}).Build()

	nf := newFakeNiFi()
	bgSeedBlue(nf)
	// Candidate is missing the "out" output port, so the outbound switch fails after the
	// inbound switch already succeeded, exercising traffic rollback.
	nf.candidate = &fakeCandidate{id: "green", inputs: []string{"in"}, outputs: []string{"other"}}
	r := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nf, FlowSnapshotClient: nf, ProcessGroupScheduler: nf, BlueGreenClient: nf}

	snapshot := json.RawMessage(`{"flowContents":{"name":"payments","inputPorts":[{"name":"in"}],"outputPorts":[{"name":"other"}]}}`)
	key := types.NamespacedName{Name: "payments", Namespace: "default"}

	driveBlueGreen(t, r, k8sClient, key, snapshot)

	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), key, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.ProcessGroupID != "blue" {
		t.Fatalf("ProcessGroupID = %q, want blue (rollback)", current.Status.ProcessGroupID)
	}
	if current.Status.Ready {
		t.Fatal("deployment must not be Ready after a rolled-back BlueGreen rollout")
	}
	if current.Status.LastRollback == nil || current.Status.LastRollback.FailedDigest != "sha256:new" {
		t.Fatalf("LastRollback = %#v, want failed digest recorded", current.Status.LastRollback)
	}
	if !nf.pgs["green"].deleted {
		t.Fatal("green candidate should be deleted after rollback")
	}
	// Every boundary connection points back at blue ports; none at green.
	for _, conn := range nf.connections {
		if conn.Component.Source.GroupID == "green" || conn.Component.Destination.GroupID == "green" {
			t.Fatalf("connection still references green after rollback: %#v", conn)
		}
	}
	inbound, outbound := false, false
	for _, conn := range nf.connections {
		if conn.Component.Destination.ID == "blue-in" {
			inbound = true
		}
		if conn.Component.Source.ID == "blue-out" {
			outbound = true
		}
	}
	if !inbound || !outbound {
		t.Fatalf("blue boundary connections were not restored: %#v", nf.connections)
	}
}

func TestBlueGreenInventoryRejectsUnsupportedSource(t *testing.T) {
	nf := newFakeNiFi()
	nf.pgs["blue"] = &fakePG{id: "blue", inputs: []nifi.PortEntity{port("blue-in", "in")}}
	nf.connections = []nifi.ConnectionEntity{
		bgConn("c-in", "funnel-1", nifi.ConnectableFunnel, "root", "blue-in", nifi.ConnectableInputPort, "blue"),
	}
	r := &NiFiFlowDeploymentReconciler{BlueGreenClient: nf}
	_, err := r.blueGreenInventory(context.Background(), "https://nifi", "blue", "root")
	if err == nil {
		t.Fatal("expected inventory to reject a FUNNEL source that cannot be stopped")
	}
}

// driveBlueGreen runs the BlueGreen state machine to a terminal state (ActiveRollout
// cleared), re-reading the deployment each step like a real reconcile.
func driveBlueGreen(t *testing.T, r *NiFiFlowDeploymentReconciler, k8sClient client.Client, key types.NamespacedName, snapshot json.RawMessage) {
	t.Helper()
	for i := 0; i < 50; i++ {
		current := &nifiv1alpha1.NiFiFlowDeployment{}
		if err := k8sClient.Get(context.Background(), key, current); err != nil {
			t.Fatal(err)
		}
		if i > 0 && current.Status.ActiveRollout == nil {
			return
		}
		if _, err := r.reconcileBlueGreenRollout(context.Background(), current, "https://nifi", "root", snapshot, "v2", "sha256:new"); err != nil {
			t.Fatalf("reconcile %d: %v", i, err)
		}
	}
	t.Fatal("BlueGreen rollout did not reach a terminal state")
}
