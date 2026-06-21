package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
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

type fakeProcessorClient struct {
	entities []nifi.ProcessorEntity
	created  []nifi.ProcessorEntity
	updated  []nifi.ProcessorEntity
	deleted  []string
	err      error
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

func newCanvasTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiProcessGroup{}, &nifiv1alpha1.NiFiProcessor{}, &nifiv1alpha1.NiFiFunnel{}, &nifiv1alpha1.NiFiLabel{}).
		Build()
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
