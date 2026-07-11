package controller

import (
	"context"
	"encoding/json"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeProcessGroupClient struct {
	entities []nifi.ProcessGroupEntity
	created  []nifi.ProcessGroupEntity
	updated  []nifi.ProcessGroupEntity
	deleted  []string
	err      error
}

type fakeFlowSnapshotClient struct {
	imported        []json.RawMessage
	replacements    []json.RawMessage
	deleted         []string
	importedEntity  nifi.ProcessGroupEntity
	createdRequest  nifi.ProcessGroupReplaceRequestEntity
	createdRequests []nifi.ProcessGroupReplaceRequestEntity
	observedRequest nifi.ProcessGroupReplaceRequestEntity
	liveSnapshot    json.RawMessage
	err             error
}

func (f *fakeFlowSnapshotClient) ImportProcessGroup(ctx context.Context, baseURI string, parentID string, snapshot json.RawMessage) (*nifi.ProcessGroupEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.imported = append(f.imported, append(json.RawMessage(nil), snapshot...))
	f.liveSnapshot = append(json.RawMessage(nil), snapshot...)
	return &f.importedEntity, nil
}

func (f *fakeFlowSnapshotClient) CreateProcessGroupReplaceRequest(ctx context.Context, baseURI string, processGroupID string, revisionVersion int64, snapshot json.RawMessage) (*nifi.ProcessGroupReplaceRequestEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.replacements = append(f.replacements, append(json.RawMessage(nil), snapshot...))
	f.liveSnapshot = append(json.RawMessage(nil), snapshot...)
	if index := len(f.replacements) - 1; index < len(f.createdRequests) {
		return &f.createdRequests[index], nil
	}
	return &f.createdRequest, nil
}

func (f *fakeFlowSnapshotClient) DownloadProcessGroup(ctx context.Context, baseURI string, processGroupID string) (json.RawMessage, error) {
	if f.err != nil {
		return nil, f.err
	}
	return append(json.RawMessage(nil), f.liveSnapshot...), nil
}

type fakeProcessGroupScheduler struct {
	states []string
	err    error
}

func (f *fakeProcessGroupScheduler) ScheduleProcessGroup(ctx context.Context, baseURI string, processGroupID string, state string) error {
	if f.err != nil {
		return f.err
	}
	f.states = append(f.states, state)
	return nil
}

func (f *fakeFlowSnapshotClient) GetProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) (*nifi.ProcessGroupReplaceRequestEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	return &f.observedRequest, nil
}

func (f *fakeFlowSnapshotClient) DeleteProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, requestID)
	return nil
}

func (f *fakeProcessGroupClient) GetProcessGroup(ctx context.Context, baseURI string, id string) (*nifi.ProcessGroupEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.entities {
		if processGroupEntityID(f.entities[i]) == id {
			return &f.entities[i], nil
		}
	}
	return nil, nil
}

func (f *fakeProcessGroupClient) CreateProcessGroup(ctx context.Context, baseURI string, parentID string, entity nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "pg-created"
	created.Component.ID = "pg-created"
	created.Component.ParentGroupID = parentID
	created.Revision.Version = 0
	f.entities = append(f.entities, created)
	return &created, nil
}

func (f *fakeProcessGroupClient) UpdateProcessGroup(ctx context.Context, baseURI string, entity nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	f.entities = append(f.entities, updated)
	return &updated, nil
}

func (f *fakeProcessGroupClient) DeleteProcessGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeControllerServiceClient struct {
	entities  []nifi.ControllerServiceEntity
	created   []nifi.ControllerServiceEntity
	updated   []nifi.ControllerServiceEntity
	deleted   []string
	runStatus []string
	err       error
}

func (f *fakeControllerServiceClient) UpdateControllerServiceRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*nifi.ControllerServiceEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.runStatus = append(f.runStatus, state)
	for i := range f.entities {
		if controllerServiceEntityID(f.entities[i]) == id {
			f.entities[i].Component.State = state
			f.entities[i].Revision.Version = revisionVersion + 1
			e := f.entities[i]
			return &e, nil
		}
	}
	return &nifi.ControllerServiceEntity{ID: id, Revision: nifi.Revision{Version: revisionVersion + 1}, Component: nifi.ControllerServiceComponent{ID: id, State: state, ValidationStatus: "VALID"}}, nil
}

func (f *fakeControllerServiceClient) GetControllerService(ctx context.Context, baseURI string, id string) (*nifi.ControllerServiceEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.entities {
		if controllerServiceEntityID(f.entities[i]) == id {
			return &f.entities[i], nil
		}
	}
	return nil, nil
}

func (f *fakeControllerServiceClient) CreateControllerService(ctx context.Context, baseURI string, parentID string, entity nifi.ControllerServiceEntity) (*nifi.ControllerServiceEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "controller-service-created"
	created.Component.ID = "controller-service-created"
	created.Component.ParentGroupID = parentID
	created.Component.ValidationStatus = "VALID"
	created.Revision.Version = 0
	f.entities = append(f.entities, created)
	return &created, nil
}

func (f *fakeControllerServiceClient) UpdateControllerService(ctx context.Context, baseURI string, entity nifi.ControllerServiceEntity) (*nifi.ControllerServiceEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	f.entities = append(f.entities, updated)
	return &updated, nil
}

func (f *fakeControllerServiceClient) DeleteControllerService(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeProcessorClient struct {
	entities  []nifi.ProcessorEntity
	created   []nifi.ProcessorEntity
	updated   []nifi.ProcessorEntity
	deleted   []string
	runStatus []string
	err       error
}

func (f *fakeProcessorClient) UpdateProcessorRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*nifi.ProcessorEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.runStatus = append(f.runStatus, state)
	for i := range f.entities {
		if processorEntityID(f.entities[i]) == id {
			f.entities[i].Component.State = state
			f.entities[i].Revision.Version = revisionVersion + 1
			e := f.entities[i]
			return &e, nil
		}
	}
	return &nifi.ProcessorEntity{ID: id, Revision: nifi.Revision{Version: revisionVersion + 1}, Component: nifi.ProcessorComponent{ID: id, State: state, ValidationStatus: "VALID"}}, nil
}

func (f *fakeProcessorClient) GetProcessor(ctx context.Context, baseURI string, id string) (*nifi.ProcessorEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.entities {
		if processorEntityID(f.entities[i]) == id {
			return &f.entities[i], nil
		}
	}
	return nil, nil
}

func (f *fakeProcessorClient) CreateProcessor(ctx context.Context, baseURI string, parentID string, entity nifi.ProcessorEntity) (*nifi.ProcessorEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "processor-created"
	created.Component.ID = "processor-created"
	created.Component.ParentGroupID = parentID
	created.Component.ValidationStatus = "VALID"
	created.Revision.Version = 0
	f.entities = append(f.entities, created)
	return &created, nil
}

func (f *fakeProcessorClient) UpdateProcessor(ctx context.Context, baseURI string, entity nifi.ProcessorEntity) (*nifi.ProcessorEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	f.entities = append(f.entities, updated)
	return &updated, nil
}

func (f *fakeProcessorClient) DeleteProcessor(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeFunnelClient struct {
	created []nifi.FunnelEntity
	deleted []string
	err     error
}

func (f *fakeFunnelClient) GetFunnel(ctx context.Context, baseURI string, id string) (*nifi.FunnelEntity, error) {
	return nil, f.err
}

func (f *fakeFunnelClient) CreateFunnel(ctx context.Context, baseURI string, parentID string, entity nifi.FunnelEntity) (*nifi.FunnelEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "funnel-created"
	created.Component.ID = "funnel-created"
	created.Component.ParentGroupID = parentID
	return &created, nil
}

func (f *fakeFunnelClient) UpdateFunnel(ctx context.Context, baseURI string, entity nifi.FunnelEntity) (*nifi.FunnelEntity, error) {
	return &entity, f.err
}

func (f *fakeFunnelClient) DeleteFunnel(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	return f.err
}

type fakeLabelClient struct {
	created []nifi.LabelEntity
	err     error
}

func (f *fakeLabelClient) GetLabel(ctx context.Context, baseURI string, id string) (*nifi.LabelEntity, error) {
	return nil, f.err
}

func (f *fakeLabelClient) CreateLabel(ctx context.Context, baseURI string, parentID string, entity nifi.LabelEntity) (*nifi.LabelEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "label-created"
	created.Component.ID = "label-created"
	created.Component.ParentGroupID = parentID
	return &created, nil
}

func (f *fakeLabelClient) UpdateLabel(ctx context.Context, baseURI string, entity nifi.LabelEntity) (*nifi.LabelEntity, error) {
	return &entity, f.err
}

func (f *fakeLabelClient) DeleteLabel(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return f.err
}

type fakeInputPortClient struct {
	created []nifi.PortEntity
	err     error
}

func (f *fakeInputPortClient) GetInputPort(ctx context.Context, baseURI string, id string) (*nifi.PortEntity, error) {
	return nil, f.err
}

func (f *fakeInputPortClient) CreateInputPort(ctx context.Context, baseURI string, parentID string, entity nifi.PortEntity) (*nifi.PortEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "input-port-created"
	created.Component.ID = "input-port-created"
	created.Component.ParentGroupID = parentID
	return &created, nil
}

func (f *fakeInputPortClient) UpdateInputPort(ctx context.Context, baseURI string, entity nifi.PortEntity) (*nifi.PortEntity, error) {
	return &entity, f.err
}

func (f *fakeInputPortClient) UpdateInputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*nifi.PortEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	e := nifi.PortEntity{ID: id, Revision: nifi.Revision{Version: revisionVersion + 1}, Component: nifi.PortComponent{ID: id, State: state, ValidationStatus: "VALID"}}
	return &e, nil
}

func (f *fakeInputPortClient) DeleteInputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return f.err
}

type fakeOutputPortClient struct {
	created     []nifi.PortEntity
	getEntity   *nifi.PortEntity // returned by GetOutputPort when set (e.g. to exercise delete refetch)
	deletedRev  int64            // revision passed to the last DeleteOutputPort call
	deleteCalls int
	err         error
}

func (f *fakeOutputPortClient) GetOutputPort(ctx context.Context, baseURI string, id string) (*nifi.PortEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.getEntity, nil
}

func (f *fakeOutputPortClient) CreateOutputPort(ctx context.Context, baseURI string, parentID string, entity nifi.PortEntity) (*nifi.PortEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "output-port-created"
	created.Component.ID = "output-port-created"
	created.Component.ParentGroupID = parentID
	return &created, nil
}

func (f *fakeOutputPortClient) UpdateOutputPort(ctx context.Context, baseURI string, entity nifi.PortEntity) (*nifi.PortEntity, error) {
	return &entity, f.err
}

func (f *fakeOutputPortClient) UpdateOutputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*nifi.PortEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	e := nifi.PortEntity{ID: id, Revision: nifi.Revision{Version: revisionVersion + 1}, Component: nifi.PortComponent{ID: id, State: state, ValidationStatus: "VALID"}}
	return &e, nil
}

func (f *fakeOutputPortClient) DeleteOutputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	f.deleteCalls++
	f.deletedRev = revisionVersion
	return f.err
}

type fakeConnectionClient struct {
	created []nifi.ConnectionEntity
	err     error
}

func (f *fakeConnectionClient) GetConnection(ctx context.Context, baseURI string, id string) (*nifi.ConnectionEntity, error) {
	return nil, f.err
}

func (f *fakeConnectionClient) CreateConnection(ctx context.Context, baseURI string, parentID string, entity nifi.ConnectionEntity) (*nifi.ConnectionEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "connection-created"
	created.Component.ID = "connection-created"
	created.Component.ParentGroupID = parentID
	return &created, nil
}

func (f *fakeConnectionClient) UpdateConnection(ctx context.Context, baseURI string, entity nifi.ConnectionEntity) (*nifi.ConnectionEntity, error) {
	return &entity, f.err
}

func (f *fakeConnectionClient) DeleteConnection(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return f.err
}

func TestNiFiProcessGroupReconcileCreatesProcessGroup(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	processGroup := &nifiv1alpha1.NiFiProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessGroupSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DisplayName: "Payments",
			Comments:    "Payments flow",
			Position:    &nifiv1alpha1.Position{X: 10, Y: 20},
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, processGroup)
	nifiClient := &fakeProcessGroupClient{}
	reconciler := &NiFiProcessGroupReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processGroup.Name, Namespace: processGroup.Namespace}}

	reconcileProcessGroupTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	if nifiClient.created[0].Component.Name != "Payments" {
		t.Fatalf("created name = %q, want Payments", nifiClient.created[0].Component.Name)
	}

	current := &nifiv1alpha1.NiFiProcessGroup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("process group ready = false, want true")
	}
	if current.Status.NiFiID != "pg-created" {
		t.Fatalf("status nifi id = %q, want pg-created", current.Status.NiFiID)
	}
}

func TestNiFiProcessorReconcileCreatesProcessor(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	processor := &nifiv1alpha1.NiFiProcessor{
		ObjectMeta: metav1.ObjectMeta{Name: "generate-payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessorSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Type:                  "org.apache.nifi.processors.standard.GenerateFlowFile",
			DisplayName:           "Generate Payments",
			Properties:            map[string]string{"Batch Size": "1"},
			State:                 nifiv1alpha1.RuntimeStateStopped,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, processor)
	nifiClient := &fakeProcessorClient{}
	reconciler := &NiFiProcessorReconciler{Client: k8sClient, Scheme: scheme, ProcessorClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processor.Name, Namespace: processor.Namespace}}

	reconcileProcessorTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.ParentGroupID != "pg-payments" {
		t.Fatalf("parent id = %q, want pg-payments", created.Component.ParentGroupID)
	}
	if created.Component.Config.Properties["Batch Size"] != "1" {
		t.Fatalf("batch size = %q, want 1", created.Component.Config.Properties["Batch Size"])
	}

	current := &nifiv1alpha1.NiFiProcessor{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("processor ready = false, want true")
	}
	if current.Status.NiFiID != "processor-created" {
		t.Fatalf("status nifi id = %q, want processor-created", current.Status.NiFiID)
	}
	if current.Status.ValidationStatus != "VALID" {
		t.Fatalf("validation status = %q, want VALID", current.Status.ValidationStatus)
	}
}

func TestNiFiProcessorReconcileStartsViaRunStatus(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	processor := &nifiv1alpha1.NiFiProcessor{
		ObjectMeta: metav1.ObjectMeta{Name: "gen", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessorSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Type:                  "org.apache.nifi.processors.standard.GenerateFlowFile",
			State:                 nifiv1alpha1.RuntimeStateRunning,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, processor)
	nifiClient := &fakeProcessorClient{}
	reconciler := &NiFiProcessorReconciler{Client: k8sClient, Scheme: scheme, ProcessorClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: processor.Name, Namespace: processor.Namespace}}

	reconcileProcessorTwice(t, reconciler, request)

	// Run state must go through the run-status endpoint, never the component create/update body.
	if len(nifiClient.created) == 0 || nifiClient.created[0].Component.State != "" {
		t.Fatalf("component create must not carry run state: %#v", nifiClient.created)
	}
	if len(nifiClient.runStatus) == 0 || nifiClient.runStatus[len(nifiClient.runStatus)-1] != "RUNNING" {
		t.Fatalf("processor should be started via run-status: %#v", nifiClient.runStatus)
	}
	current := &nifiv1alpha1.NiFiProcessor{}
	_ = k8sClient.Get(context.Background(), request.NamespacedName, current)
	if !current.Status.Ready {
		t.Fatalf("status = %+v", current.Status)
	}
}

func TestNiFiControllerServiceReconcileEnablesViaRunStatus(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	cs := &nifiv1alpha1.NiFiControllerService{
		ObjectMeta: metav1.ObjectMeta{Name: "dbcp", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiControllerServiceSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Type:                  "org.apache.nifi.dbcp.DBCPConnectionPool",
			State:                 nifiv1alpha1.RuntimeStateEnabled,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, cs)
	nifiClient := &fakeControllerServiceClient{}
	reconciler := &NiFiControllerServiceReconciler{Client: k8sClient, Scheme: scheme, ControllerServiceClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: cs.Name, Namespace: cs.Namespace}}

	reconcileControllerServiceTwice(t, reconciler, request)

	if len(nifiClient.created) == 0 || nifiClient.created[0].Component.State != "" {
		t.Fatalf("component create must not carry run state: %#v", nifiClient.created)
	}
	if len(nifiClient.runStatus) == 0 || nifiClient.runStatus[len(nifiClient.runStatus)-1] != "ENABLED" {
		t.Fatalf("controller service should be enabled via run-status: %#v", nifiClient.runStatus)
	}
	current := &nifiv1alpha1.NiFiControllerService{}
	_ = k8sClient.Get(context.Background(), request.NamespacedName, current)
	if !current.Status.Ready {
		t.Fatalf("status = %+v", current.Status)
	}
}

func TestNiFiControllerServiceReconcileCreatesControllerService(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	controllerService := &nifiv1alpha1.NiFiControllerService{
		ObjectMeta: metav1.ObjectMeta{Name: "dbcp", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiControllerServiceSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Type:                  "org.apache.nifi.dbcp.DBCPConnectionPool",
			Properties:            map[string]string{"Database Connection URL": "jdbc:postgresql://postgres/payments"},
			State:                 nifiv1alpha1.RuntimeStateDisabled,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, controllerService)
	nifiClient := &fakeControllerServiceClient{}
	reconciler := &NiFiControllerServiceReconciler{Client: k8sClient, Scheme: scheme, ControllerServiceClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: controllerService.Name, Namespace: controllerService.Namespace}}

	reconcileControllerServiceTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.ParentGroupID != "pg-payments" {
		t.Fatalf("parent id = %q, want pg-payments", created.Component.ParentGroupID)
	}
	if created.Component.Properties["Database Connection URL"] != "jdbc:postgresql://postgres/payments" {
		t.Fatalf("database url = %q", created.Component.Properties["Database Connection URL"])
	}
	current := &nifiv1alpha1.NiFiControllerService{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "controller-service-created" {
		t.Fatalf("status ready/id = %v/%q, want true/controller-service-created", current.Status.Ready, current.Status.NiFiID)
	}
	if current.Status.ValidationStatus != "VALID" {
		t.Fatalf("validation status = %q, want VALID", current.Status.ValidationStatus)
	}
}

func TestNiFiFlowDeploymentResolvesBundleArtifact(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	snapshot := testFlowSnapshot("Original name", "Generate payload")
	_, digest, err := canonicalFlowSnapshot(snapshot, "")
	if err != nil {
		t.Fatal(err)
	}
	flowBundle := readyTestFlowBundle("payments", digest, "commit-1")
	flowDeployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source: nifiv1alpha1.FlowDeploymentSource{
				BundleRef: &nifiv1alpha1.LocalObjectReference{Name: flowBundle.Name},
			},
			Target: nifiv1alpha1.FlowDeploymentTarget{
				ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
				ProcessGroupName:      "Payments",
			},
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, flowBundle, flowDeployment)
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{
		ID: "pg-imported", Revision: nifi.Revision{Version: 2},
		Component: nifi.ProcessGroupComponent{ID: "pg-imported", ParentGroupID: "pg-platform", Name: "Payments"},
	}}}
	flowSnapshots := &fakeFlowSnapshotClient{importedEntity: processGroups.entities[0]}
	reconciler := &NiFiFlowDeploymentReconciler{
		Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots,
		ArtifactResolver: fakeFlowArtifactResolver{artifact: &flowartifact.Artifact{Snapshot: *snapshot, Revision: "commit-1"}},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowDeployment.Name, Namespace: flowDeployment.Namespace}}

	for range 3 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	if len(flowSnapshots.imported) != 1 || len(processGroups.created) != 0 {
		t.Fatalf("import/create counts = %d/%d, want 1/0", len(flowSnapshots.imported), len(processGroups.created))
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.ProcessGroupID != "pg-imported" {
		t.Fatalf("status ready/process group id = %v/%q, want true/pg-imported", current.Status.Ready, current.Status.ProcessGroupID)
	}
	if current.Status.ArtifactDigest != digest || current.Status.DeployedVersion != "commit-1" {
		t.Fatalf("status digest/version = %q/%q, want %s/commit-1", current.Status.ArtifactDigest, current.Status.DeployedVersion, digest)
	}
}

func TestNiFiFlowDeploymentImportsFullSnapshot(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	snapshot := testFlowSnapshot("Original name", "Generate payload")
	_, digest, err := canonicalFlowSnapshot(snapshot, "")
	if err != nil {
		t.Fatal(err)
	}
	flowBundle := readyTestSnapshotFlowBundle("payments", snapshot, digest, "release-1")
	flowDeployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:     nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: flowBundle.Name}},
			Target: nifiv1alpha1.FlowDeploymentTarget{
				ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
				ProcessGroupName:      "Payments",
			},
		},
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{
		ID: "pg-imported", Revision: nifi.Revision{Version: 4},
		Component: nifi.ProcessGroupComponent{ID: "pg-imported", ParentGroupID: "pg-platform", Name: "Payments"},
	}}}
	flowSnapshots := &fakeFlowSnapshotClient{importedEntity: processGroups.entities[0]}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, flowBundle, flowDeployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowDeployment.Name, Namespace: flowDeployment.Namespace}}

	for range 3 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	if len(flowSnapshots.imported) != 1 {
		t.Fatalf("import count = %d, want 1", len(flowSnapshots.imported))
	}
	var imported map[string]any
	if err := json.Unmarshal(flowSnapshots.imported[0], &imported); err != nil {
		t.Fatal(err)
	}
	flowContents := imported["flowContents"].(map[string]any)
	if flowContents["name"] != "Payments" {
		t.Fatalf("imported flow name = %q, want Payments", flowContents["name"])
	}
	processors := flowContents["processors"].([]any)
	if len(processors) != 1 || processors[0].(map[string]any)["name"] != "Generate payload" {
		t.Fatalf("imported processors = %#v", processors)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.ProcessGroupID != "pg-imported" || current.Status.SyncState != "InSync" {
		t.Fatalf("status ready/id/sync = %v/%q/%q", current.Status.Ready, current.Status.ProcessGroupID, current.Status.SyncState)
	}
	if current.Status.ArtifactDigest != digest || current.Status.DeployedVersion != "release-1" {
		t.Fatalf("status digest/version = %q/%q", current.Status.ArtifactDigest, current.Status.DeployedVersion)
	}
}

func TestNiFiFlowDeploymentReplacesChangedSnapshot(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	snapshot := testFlowSnapshot("Payments", "Generate v2")
	_, digest, err := canonicalFlowSnapshot(snapshot, "")
	if err != nil {
		t.Fatal(err)
	}
	flowBundle := readyTestSnapshotFlowBundle("payments", snapshot, digest, "release-2")
	flowDeployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:     nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: flowBundle.Name}},
			Target: nifiv1alpha1.FlowDeploymentTarget{
				ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
				ProcessGroupName:      "Payments",
			},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			CommonStatus:    nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1, NiFiID: "pg-imported", Dependencies: nifiv1alpha1.DependencyStatus{Ready: true}},
			ProcessGroupID:  "pg-imported",
			ArtifactDigest:  "sha256:old",
			DeployedVersion: "release-1",
		},
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{
		ID: "pg-imported", Revision: nifi.Revision{Version: 8}, Component: nifi.ProcessGroupComponent{ID: "pg-imported", Name: "Payments"},
	}}}
	flowSnapshots := &fakeFlowSnapshotClient{
		createdRequest:  nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Stopping Processors", PercentCompleted: 20}},
		observedRequest: nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Complete", Complete: true, PercentCompleted: 100}},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, flowBundle, flowDeployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: flowDeployment.Name, Namespace: flowDeployment.Namespace}}

	for range 4 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}

	if len(flowSnapshots.replacements) != 1 || len(flowSnapshots.deleted) != 1 || flowSnapshots.deleted[0] != "replace-1" {
		t.Fatalf("replace/delete counts = %d/%#v", len(flowSnapshots.replacements), flowSnapshots.deleted)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.ArtifactDigest != digest || current.Status.DeployedVersion != "release-2" {
		t.Fatalf("status ready/digest/version = %v/%q/%q", current.Status.Ready, current.Status.ArtifactDigest, current.Status.DeployedVersion)
	}
	if current.Status.LatestReplaceRequest != nil || len(current.Status.RolloutHistory) != 1 || current.Status.RolloutHistory[0].Result != "Succeeded" {
		t.Fatalf("replace status/history = %#v/%#v", current.Status.LatestReplaceRequest, current.Status.RolloutHistory)
	}
}

func TestNiFiFlowDeploymentWarnsOnLiveDrift(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	desired := testFlowSnapshot("Payments", "Generate desired")
	_, digest, err := canonicalFlowSnapshot(desired, "Payments")
	if err != nil {
		t.Fatal(err)
	}
	bundle := readyTestSnapshotFlowBundle("payments", desired, digest, "release-1")
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:      nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: bundle.Name}},
			Target:      nifiv1alpha1.FlowDeploymentTarget{ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name}, ProcessGroupName: "Payments"},
			DriftPolicy: nifiv1alpha1.DriftPolicy{Mode: nifiv1alpha1.DriftPolicyWarn},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			CommonStatus:   nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1, NiFiID: "pg-imported", Dependencies: nifiv1alpha1.DependencyStatus{Ready: true}},
			ProcessGroupID: "pg-imported", ArtifactDigest: digest, DeployedVersion: "release-1",
		},
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{
		ID: "pg-imported", Revision: nifi.Revision{Version: 4}, Component: nifi.ProcessGroupComponent{ID: "pg-imported", Name: "Payments"},
	}}}
	flowSnapshots := &fakeFlowSnapshotClient{liveSnapshot: testFlowSnapshot("Payments", "Generate changed in UI").Raw}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, bundle, deployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}}

	for range 2 {
		if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.Drift.Status != "Drifted" || len(current.Status.Drift.Differences) == 0 {
		t.Fatalf("ready/drift = %v/%#v", current.Status.Ready, current.Status.Drift)
	}
	if len(flowSnapshots.replacements) != 0 {
		t.Fatalf("warn policy created %d replacement(s)", len(flowSnapshots.replacements))
	}
}

func TestNiFiFlowDeploymentReconcilesLiveDrift(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	desired := testFlowSnapshot("Payments", "Generate desired")
	_, digest, err := canonicalFlowSnapshot(desired, "Payments")
	if err != nil {
		t.Fatal(err)
	}
	bundle := readyTestSnapshotFlowBundle("payments", desired, digest, "release-1")
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:      nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: bundle.Name}},
			Target:      nifiv1alpha1.FlowDeploymentTarget{ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name}, ProcessGroupName: "Payments"},
			Rollout:     nifiv1alpha1.RolloutStrategy{Strategy: "ChangedOnly"},
			DriftPolicy: nifiv1alpha1.DriftPolicy{Mode: nifiv1alpha1.DriftPolicyReconcile},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			CommonStatus:   nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1, NiFiID: "pg-imported", Dependencies: nifiv1alpha1.DependencyStatus{Ready: true}},
			ProcessGroupID: "pg-imported", ArtifactDigest: digest, DeployedVersion: "release-1",
		},
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{ID: "pg-imported", Revision: nifi.Revision{Version: 4}, Component: nifi.ProcessGroupComponent{ID: "pg-imported", Name: "Payments"}}}}
	flowSnapshots := &fakeFlowSnapshotClient{
		liveSnapshot:   testFlowSnapshot("Payments", "Generate changed in UI").Raw,
		createdRequest: nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-drift", State: "Running"}},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, bundle, deployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}}

	for range 2 {
		if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	if len(flowSnapshots.replacements) != 1 {
		t.Fatalf("replacement count = %d, want 1", len(flowSnapshots.replacements))
	}
}

func TestNiFiFlowDeploymentStopAllThenApplySchedulesAroundRollout(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	desired := testFlowSnapshot("Payments", "Generate v2")
	_, digest, err := canonicalFlowSnapshot(desired, "Payments")
	if err != nil {
		t.Fatal(err)
	}
	bundle := readyTestSnapshotFlowBundle("payments", desired, digest, "release-2")
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:     nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: bundle.Name}},
			Target:     nifiv1alpha1.FlowDeploymentTarget{ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name}, ProcessGroupName: "Payments"},
			Rollout:    nifiv1alpha1.RolloutStrategy{Strategy: "StopAllThenApply"},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1}, ProcessGroupID: "pg-imported", ArtifactDigest: "sha256:old", DeployedVersion: "release-1"},
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{ID: "pg-imported", Revision: nifi.Revision{Version: 4}, Component: nifi.ProcessGroupComponent{ID: "pg-imported", Name: "Payments"}}}}
	flowSnapshots := &fakeFlowSnapshotClient{
		createdRequest:  nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Running"}},
		observedRequest: nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-1", State: "Complete", Complete: true, PercentCompleted: 100}},
	}
	scheduler := &fakeProcessGroupScheduler{}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, bundle, deployment)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots, ProcessGroupScheduler: scheduler}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}}

	for range 5 {
		if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	if len(scheduler.states) != 2 || scheduler.states[0] != "STOPPED" || scheduler.states[1] != "RUNNING" || len(flowSnapshots.replacements) != 1 {
		t.Fatalf("states/replacements = %#v/%d", scheduler.states, len(flowSnapshots.replacements))
	}
}

func TestNiFiFlowDeploymentAutomaticallyRollsBackFailedReplacement(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("platform", "pg-platform")
	previous := testFlowSnapshot("Payments", "Generate v1")
	_, previousDigest, err := canonicalFlowSnapshot(previous, "Payments")
	if err != nil {
		t.Fatal(err)
	}
	desired := testFlowSnapshot("Payments", "Generate v2")
	_, desiredDigest, err := canonicalFlowSnapshot(desired, "Payments")
	if err != nil {
		t.Fatal(err)
	}
	bundle := readyTestSnapshotFlowBundle("payments", desired, desiredDigest, "release-2")
	history := nifiv1alpha1.FlowDeploymentHistory{Version: "release-1", Digest: previousDigest, SnapshotConfigMap: "payments-history-v1", Result: "Succeeded", DeployedAt: metav1.Now()}
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:     nifiv1alpha1.FlowDeploymentSource{BundleRef: &nifiv1alpha1.LocalObjectReference{Name: bundle.Name}},
			Target:     nifiv1alpha1.FlowDeploymentTarget{ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name}, ProcessGroupName: "Payments"},
			Rollout:    nifiv1alpha1.RolloutStrategy{Strategy: "ChangedOnly"},
			Rollback:   nifiv1alpha1.RollbackStrategy{Enabled: true, HistoryLimit: 5},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1}, ProcessGroupID: "pg-imported",
			ArtifactDigest: previousDigest, DeployedVersion: "release-1", LastSuccessful: history.DeepCopy(), RolloutHistory: []nifiv1alpha1.FlowDeploymentHistory{history},
		},
	}
	historyConfigMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: history.SnapshotConfigMap, Namespace: "default"}, Data: map[string]string{flowSnapshotDataKey: string(previous.Raw)}}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{ID: "pg-imported", Revision: nifi.Revision{Version: 4}, Component: nifi.ProcessGroupComponent{ID: "pg-imported", Name: "Payments"}}}}
	flowSnapshots := &fakeFlowSnapshotClient{createdRequests: []nifi.ProcessGroupReplaceRequestEntity{
		{Request: nifi.ProcessGroupReplaceRequest{RequestID: "replace-failed", Complete: true, FailureReason: "invalid replacement"}},
		{Request: nifi.ProcessGroupReplaceRequest{RequestID: "rollback-1", State: "Running"}},
	}, observedRequest: nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: "rollback-1", State: "Complete", Complete: true, PercentCompleted: 100}}}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, bundle, deployment, historyConfigMap)
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: flowSnapshots}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: deployment.Name, Namespace: deployment.Namespace}}

	for range 5 {
		if _, err := reconciler.Reconcile(t.Context(), request); err != nil {
			t.Fatal(err)
		}
	}
	if len(flowSnapshots.replacements) != 2 || string(flowSnapshots.replacements[1]) != string(previous.Raw) {
		t.Fatalf("rollback replacements = %#v", flowSnapshots.replacements)
	}
	current := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := k8sClient.Get(t.Context(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.LatestReplaceRequest != nil || current.Status.LastRollback == nil || current.Status.LastRollback.CompletedAt == nil || current.Status.LastRollback.FailedDigest != desiredDigest {
		t.Fatalf("rollback status = %#v / %#v", current.Status.LatestReplaceRequest, current.Status.LastRollback)
	}
	if current.Status.Ready || current.Status.ArtifactDigest != previousDigest || current.Status.SyncState != "RolledBack" {
		t.Fatalf("rolled back ready/digest/state = %v/%q/%q", current.Status.Ready, current.Status.ArtifactDigest, current.Status.SyncState)
	}
}

func TestNiFiFunnelReconcileCreatesFunnel(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	funnel := &nifiv1alpha1.NiFiFunnel{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-merge", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFunnelSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Position:              &nifiv1alpha1.Position{X: 10, Y: 20},
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, funnel)
	nifiClient := &fakeFunnelClient{}
	reconciler := &NiFiFunnelReconciler{Client: k8sClient, Scheme: scheme, FunnelClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: funnel.Name, Namespace: funnel.Namespace}}

	reconcileFunnelTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	if nifiClient.created[0].Component.ParentGroupID != "pg-payments" {
		t.Fatalf("parent id = %q, want pg-payments", nifiClient.created[0].Component.ParentGroupID)
	}
	current := &nifiv1alpha1.NiFiFunnel{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "funnel-created" {
		t.Fatalf("status ready/id = %v/%q, want true/funnel-created", current.Status.Ready, current.Status.NiFiID)
	}
}

func TestNiFiFunnelDeleteDropsFinalizerWhenClusterGone(t *testing.T) {
	scheme := testScheme()
	// No NiFiCluster object exists, so clusterForDeletion reports the cluster gone.
	funnel := &nifiv1alpha1.NiFiFunnel{
		ObjectMeta: metav1.ObjectMeta{Name: "orphan", Namespace: "default", Generation: 1, Finalizers: []string{NiFiControlFinalizer}},
		Spec: nifiv1alpha1.NiFiFunnelSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: "gone-cluster"},
			DeletionPolicy: nifiv1alpha1.DeletionPolicyDelete,
		},
		Status: nifiv1alpha1.NiFiFunnelStatus{CommonStatus: nifiv1alpha1.CommonStatus{NiFiID: "funnel-1"}},
	}
	k8sClient := newCanvasTestClient(scheme, funnel)
	nifiClient := &fakeFunnelClient{}
	reconciler := &NiFiFunnelReconciler{Client: k8sClient, Scheme: scheme, FunnelClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: funnel.Name, Namespace: funnel.Namespace}}

	if err := k8sClient.Delete(context.Background(), funnel); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	got := &nifiv1alpha1.NiFiFunnel{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer must be dropped when the cluster is gone (no deadlock); got err=%v finalizers=%v", err, got.Finalizers)
	}
	if len(nifiClient.deleted) != 0 {
		t.Fatalf("must not call NiFi delete when the cluster is gone: %#v", nifiClient.deleted)
	}
}

// A canvas component's stored revision can fall behind NiFi's current revision; deleting with the
// stale revision returns HTTP 400 "not the most up-to-date revision" and the finalizer deadlocks.
// The delete path must refetch the current revision (proven end to end by test-canvas-kind.sh).
func TestNiFiOutputPortDeleteUsesRefetchedRevision(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	outputPort := &nifiv1alpha1.NiFiOutputPort{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-out", Namespace: "default", Generation: 1, Finalizers: []string{NiFiControlFinalizer}},
		Spec: nifiv1alpha1.NiFiOutputPortSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DeletionPolicy: nifiv1alpha1.DeletionPolicyDelete,
		},
		Status: nifiv1alpha1.NiFiOutputPortStatus{CommonStatus: nifiv1alpha1.CommonStatus{
			NiFiID:   "output-port-1",
			Revision: nifiv1alpha1.RevisionStatus{Version: 1}, // stale
		}},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, outputPort)
	// NiFi reports the current (advanced) revision; the delete must use it, not the stored 1.
	nifiClient := &fakeOutputPortClient{getEntity: &nifi.PortEntity{ID: "output-port-1", Revision: nifi.Revision{Version: 99}}}
	reconciler := &NiFiOutputPortReconciler{Client: k8sClient, Scheme: scheme, OutputPortClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: outputPort.Name, Namespace: outputPort.Namespace}}

	if err := k8sClient.Delete(context.Background(), outputPort); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	if nifiClient.deleteCalls != 1 || nifiClient.deletedRev != 99 {
		t.Fatalf("delete calls=%d revision=%d, want 1 call using the refetched revision 99 (not the stale 1)", nifiClient.deleteCalls, nifiClient.deletedRev)
	}
	got := &nifiv1alpha1.NiFiOutputPort{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer must be dropped after a successful delete; got err=%v", err)
	}
}

func TestNiFiLabelReconcileCreatesLabel(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	label := &nifiv1alpha1.NiFiLabel{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-note", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiLabelSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Text:                  "Payments flow",
			Width:                 300,
			Height:                100,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, label)
	nifiClient := &fakeLabelClient{}
	reconciler := &NiFiLabelReconciler{Client: k8sClient, Scheme: scheme, LabelClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: label.Name, Namespace: label.Namespace}}

	reconcileLabelTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	if nifiClient.created[0].Component.Label != "Payments flow" {
		t.Fatalf("label = %q, want Payments flow", nifiClient.created[0].Component.Label)
	}
	current := &nifiv1alpha1.NiFiLabel{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "label-created" {
		t.Fatalf("status ready/id = %v/%q, want true/label-created", current.Status.Ready, current.Status.NiFiID)
	}
}

func TestNiFiInputPortReconcileCreatesInputPort(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	inputPort := &nifiv1alpha1.NiFiInputPort{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-in", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiInputPortSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			DisplayName:           "Payments In",
			State:                 nifiv1alpha1.RuntimeStateStopped,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, inputPort)
	nifiClient := &fakeInputPortClient{}
	reconciler := &NiFiInputPortReconciler{Client: k8sClient, Scheme: scheme, InputPortClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: inputPort.Name, Namespace: inputPort.Namespace}}

	reconcileInputPortTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	if nifiClient.created[0].Component.Name != "Payments In" {
		t.Fatalf("created name = %q, want Payments In", nifiClient.created[0].Component.Name)
	}
	current := &nifiv1alpha1.NiFiInputPort{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "input-port-created" {
		t.Fatalf("status ready/id = %v/%q, want true/input-port-created", current.Status.Ready, current.Status.NiFiID)
	}
}

func TestNiFiOutputPortReconcileCreatesOutputPort(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	outputPort := &nifiv1alpha1.NiFiOutputPort{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-out", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiOutputPortSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			DisplayName:           "Payments Out",
			State:                 nifiv1alpha1.RuntimeStateStopped,
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, outputPort)
	nifiClient := &fakeOutputPortClient{}
	reconciler := &NiFiOutputPortReconciler{Client: k8sClient, Scheme: scheme, OutputPortClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: outputPort.Name, Namespace: outputPort.Namespace}}

	reconcileOutputPortTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	if nifiClient.created[0].Component.Name != "Payments Out" {
		t.Fatalf("created name = %q, want Payments Out", nifiClient.created[0].Component.Name)
	}
	current := &nifiv1alpha1.NiFiOutputPort{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "output-port-created" {
		t.Fatalf("status ready/id = %v/%q, want true/output-port-created", current.Status.Ready, current.Status.NiFiID)
	}
}

func TestNiFiConnectionReconcileCreatesConnection(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parent := readyTestProcessGroup("payments", "pg-payments")
	processor := readyTestProcessor("generate-payments", "processor-1", "pg-payments")
	outputPort := readyTestOutputPort("payments-out", "output-port-1", "pg-payments")
	connection := &nifiv1alpha1.NiFiConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-generated", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiConnectionSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Name: parent.Name},
			Source:                nifiv1alpha1.ConnectableReference{Type: nifiv1alpha1.ConnectableTypeProcessor, Name: processor.Name},
			Destination:           nifiv1alpha1.ConnectableReference{Type: nifiv1alpha1.ConnectableTypeOutputPort, Name: outputPort.Name},
			SelectedRelationships: []string{"success"},
		},
	}
	k8sClient := newCanvasTestClient(scheme, cluster, parent, processor, outputPort, connection)
	nifiClient := &fakeConnectionClient{}
	reconciler := &NiFiConnectionReconciler{Client: k8sClient, Scheme: scheme, ConnectionClient: nifiClient}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: connection.Name, Namespace: connection.Namespace}}

	reconcileConnectionTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.Source.ID != "processor-1" || created.Component.Destination.ID != "output-port-1" {
		t.Fatalf("source/destination = %q/%q, want processor-1/output-port-1", created.Component.Source.ID, created.Component.Destination.ID)
	}
	if created.Component.Source.Type != "PROCESSOR" || created.Component.Destination.Type != "OUTPUT_PORT" {
		t.Fatalf("source/destination types = %q/%q", created.Component.Source.Type, created.Component.Destination.Type)
	}
	current := &nifiv1alpha1.NiFiConnection{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready || current.Status.NiFiID != "connection-created" {
		t.Fatalf("status ready/id = %v/%q, want true/connection-created", current.Status.Ready, current.Status.NiFiID)
	}
	if current.Status.SourceID != "processor-1" || current.Status.DestinationID != "output-port-1" {
		t.Fatalf("status source/destination = %q/%q", current.Status.SourceID, current.Status.DestinationID)
	}
}

func readyTestProcessGroup(name string, nifiID string) *nifiv1alpha1.NiFiProcessGroup {
	return &nifiv1alpha1.NiFiProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
		},
		Status: nifiv1alpha1.NiFiProcessGroupStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				Ready:              true,
				ObservedGeneration: 1,
				NiFiID:             nifiID,
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
		},
	}
}

func readyTestProcessor(name string, nifiID string, parentID string) *nifiv1alpha1.NiFiProcessor {
	return &nifiv1alpha1.NiFiProcessor{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiProcessorSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Type:       "org.apache.nifi.processors.standard.GenerateFlowFile",
		},
		Status: nifiv1alpha1.NiFiProcessorStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				Ready:              true,
				ObservedGeneration: 1,
				NiFiID:             nifiID,
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
			ParentProcessGroupID: parentID,
		},
	}
}

func readyTestOutputPort(name string, nifiID string, parentID string) *nifiv1alpha1.NiFiOutputPort {
	return &nifiv1alpha1.NiFiOutputPort{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiOutputPortSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
		},
		Status: nifiv1alpha1.NiFiOutputPortStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				Ready:              true,
				ObservedGeneration: 1,
				NiFiID:             nifiID,
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
			ParentProcessGroupID: parentID,
		},
	}
}

func readyTestFlowBundle(name string, artifactDigest string, resolvedRevision string) *nifiv1alpha1.NiFiFlowBundle {
	return &nifiv1alpha1.NiFiFlowBundle{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiFlowBundleSpec{
			Source:  nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{URL: "https://example.test/flows.git", Ref: resolvedRevision}},
			Version: resolvedRevision,
		},
		Status: nifiv1alpha1.NiFiFlowBundleStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				Ready:              true,
				ObservedGeneration: 1,
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
			ArtifactDigest:   artifactDigest,
			ResolvedRevision: resolvedRevision,
		},
	}
}

func readyTestSnapshotFlowBundle(name string, snapshot *runtime.RawExtension, artifactDigest string, resolvedRevision string) *nifiv1alpha1.NiFiFlowBundle {
	bundle := readyTestFlowBundle(name, artifactDigest, resolvedRevision)
	bundle.Spec.Source = nifiv1alpha1.FlowBundleSource{Snapshot: snapshot}
	return bundle
}

func testFlowSnapshot(name string, processorName string) *runtime.RawExtension {
	return &runtime.RawExtension{Raw: []byte(`{
  "snapshotMetadata": {"version": 2, "author": "NiFiControl tests"},
  "flowContents": {
    "identifier": "root-flow",
    "name": "` + name + `",
    "processors": [{
      "identifier": "generate-1",
      "name": "` + processorName + `",
      "type": "org.apache.nifi.processors.standard.GenerateFlowFile",
      "properties": {"File Size": "1 KB"},
      "scheduledState": "ENABLED"
    }],
    "controllerServices": [],
    "inputPorts": [],
    "outputPorts": [],
    "funnels": [],
    "labels": [],
    "connections": [],
    "processGroups": []
  }
}`)}
}

func newCanvasTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(
			&nifiv1alpha1.NiFiCluster{},
			&nifiv1alpha1.NiFiProcessGroup{},
			&nifiv1alpha1.NiFiControllerService{},
			&nifiv1alpha1.NiFiFlowBundle{},
			&nifiv1alpha1.NiFiFlowDeployment{},
			&nifiv1alpha1.NiFiProcessor{},
			&nifiv1alpha1.NiFiInputPort{},
			&nifiv1alpha1.NiFiOutputPort{},
			&nifiv1alpha1.NiFiConnection{},
			&nifiv1alpha1.NiFiFunnel{},
			&nifiv1alpha1.NiFiLabel{},
		).
		Build()
}

func reconcileInputPortTwice(t *testing.T, reconciler *NiFiInputPortReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileOutputPortTwice(t *testing.T, reconciler *NiFiOutputPortReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileFunnelTwice(t *testing.T, reconciler *NiFiFunnelReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}

func reconcileLabelTwice(t *testing.T, reconciler *NiFiLabelReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
