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

type fakeRegistryClientClient struct {
	entities []nifi.RegistryClientEntity
	created  []nifi.RegistryClientEntity
	updated  []nifi.RegistryClientEntity
	deleted  []string
	err      error
}

func (f *fakeRegistryClientClient) GetRegistryClient(ctx context.Context, baseURI string, id string) (*nifi.RegistryClientEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.entities {
		if registryClientEntityID(f.entities[i]) == id {
			return &f.entities[i], nil
		}
	}
	return nil, nil
}

func (f *fakeRegistryClientClient) CreateRegistryClient(ctx context.Context, baseURI string, entity nifi.RegistryClientEntity) (*nifi.RegistryClientEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "registry-created"
	created.Component.ID = "registry-created"
	created.Revision.Version = 0
	f.entities = append(f.entities, created)
	return &created, nil
}

func (f *fakeRegistryClientClient) UpdateRegistryClient(ctx context.Context, baseURI string, entity nifi.RegistryClientEntity) (*nifi.RegistryClientEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	for i := range f.entities {
		if registryClientEntityID(f.entities[i]) == registryClientEntityID(entity) {
			f.entities[i] = updated
			return &updated, nil
		}
	}
	f.entities = append(f.entities, updated)
	return &updated, nil
}

func (f *fakeRegistryClientClient) DeleteRegistryClient(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

func TestNiFiRegistryClientReconcileCreatesRegistryClient(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	registry := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-flows", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Type:        nifiv1alpha1.RegistryClientTypeNiFiRegistry,
			URI:         "https://registry.example.com/nifi-registry",
			Description: "Shared flow registry",
		},
	}
	k8sClient := newRegistryClientTestClient(scheme, cluster, registry)
	nifiClient := &fakeRegistryClientClient{}
	reconciler := &NiFiRegistryClientReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		RegistryClientClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: registry.Name, Namespace: registry.Namespace}}

	reconcileRegistryClientTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.Name != "platform-flows" {
		t.Fatalf("created name = %q, want platform-flows", created.Component.Name)
	}
	if created.Component.Type != registryClientType(nifiv1alpha1.RegistryClientTypeNiFiRegistry) {
		t.Fatalf("created type = %q", created.Component.Type)
	}
	if created.Component.Properties["url"] != "https://registry.example.com/nifi-registry" {
		t.Fatalf("created url = %q", created.Component.Properties["url"])
	}

	current := &nifiv1alpha1.NiFiRegistryClient{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("registry client ready = false, want true")
	}
	if current.Status.NiFiID != "registry-created" {
		t.Fatalf("status nifi id = %q, want registry-created", current.Status.NiFiID)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "RegistryClientReady")
}

func TestNiFiRegistryClientReconcileUpdatesRegistryClient(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	registry := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-flows", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Type:        nifiv1alpha1.RegistryClientTypeNiFiRegistry,
			URI:         "https://registry.example.com/new",
			Description: "New description",
		},
		Status: nifiv1alpha1.NiFiRegistryClientStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				ObservedGeneration: 1,
				Ready:              true,
				NiFiID:             "registry-existing",
				Revision:           nifiv1alpha1.RevisionStatus{Version: 3},
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
			ResolvedType: registryClientType(nifiv1alpha1.RegistryClientTypeNiFiRegistry),
		},
	}
	k8sClient := newRegistryClientTestClient(scheme, cluster, registry)
	nifiClient := &fakeRegistryClientClient{
		entities: []nifi.RegistryClientEntity{
			{
				ID:       "registry-existing",
				Revision: nifi.Revision{Version: 3},
				Component: nifi.RegistryClientComponent{
					ID:          "registry-existing",
					Name:        "platform-flows",
					Type:        registryClientType(nifiv1alpha1.RegistryClientTypeNiFiRegistry),
					Description: "Old description",
					Properties:  map[string]string{"url": "https://registry.example.com/old"},
				},
			},
		},
	}
	reconciler := &NiFiRegistryClientReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		RegistryClientClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: registry.Name, Namespace: registry.Namespace}}

	reconcileRegistryClientTwice(t, reconciler, request)

	if len(nifiClient.updated) != 1 {
		t.Fatalf("updated count = %d, want 1", len(nifiClient.updated))
	}
	if nifiClient.updated[0].Component.Properties["url"] != "https://registry.example.com/new" {
		t.Fatalf("updated url = %q", nifiClient.updated[0].Component.Properties["url"])
	}
	current := &nifiv1alpha1.NiFiRegistryClient{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("registry client ready = false, want true")
	}
	if current.Status.Revision.Version != 4 {
		t.Fatalf("revision = %d, want 4", current.Status.Revision.Version)
	}
}

func TestNiFiRegistryClientReconcileAdoptsByID(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	registry := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "platform-flows", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Type:       nifiv1alpha1.RegistryClientTypeNiFiRegistry,
			URI:        "https://registry.example.com/nifi-registry",
			AdoptionPolicy: nifiv1alpha1.AdoptionPolicy{
				Mode:   nifiv1alpha1.AdoptionPolicyAdoptByID,
				NiFiID: "registry-existing",
			},
		},
	}
	k8sClient := newRegistryClientTestClient(scheme, cluster, registry)
	nifiClient := &fakeRegistryClientClient{
		entities: []nifi.RegistryClientEntity{
			{
				ID:       "registry-existing",
				Revision: nifi.Revision{Version: 9},
				Component: nifi.RegistryClientComponent{
					ID:         "registry-existing",
					Name:       "platform-flows",
					Type:       registryClientType(nifiv1alpha1.RegistryClientTypeNiFiRegistry),
					Properties: map[string]string{"url": "https://registry.example.com/nifi-registry"},
				},
			},
		},
	}
	reconciler := &NiFiRegistryClientReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		RegistryClientClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: registry.Name, Namespace: registry.Namespace}}

	reconcileRegistryClientTwice(t, reconciler, request)

	if len(nifiClient.created) != 0 {
		t.Fatalf("created count = %d, want 0", len(nifiClient.created))
	}
	current := &nifiv1alpha1.NiFiRegistryClient{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.NiFiID != "registry-existing" {
		t.Fatalf("status nifi id = %q, want registry-existing", current.Status.NiFiID)
	}
}

func TestNiFiRegistryClientReconcileMarksUnsupportedType(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	registry := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{Name: "github-flows", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Type:       nifiv1alpha1.RegistryClientTypeGitHub,
			URI:        "https://github.com/example/flows",
		},
	}
	k8sClient := newRegistryClientTestClient(scheme, cluster, registry)
	reconciler := &NiFiRegistryClientReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		RegistryClientClient: &fakeRegistryClientClient{},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: registry.Name, Namespace: registry.Namespace}}

	reconcileRegistryClientTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiRegistryClient{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Ready {
		t.Fatal("registry client ready = true, want false")
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "RegistryClientTypeUnsupported")
}

func TestNiFiRegistryClientReconcileDeletesNiFiClientWhenPolicyDelete(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	deletionTime := metav1.Now()
	registry := &nifiv1alpha1.NiFiRegistryClient{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "platform-flows",
			Namespace:         "default",
			Generation:        1,
			Finalizers:        []string{NiFiControlFinalizer},
			DeletionTimestamp: &deletionTime,
		},
		Spec: nifiv1alpha1.NiFiRegistryClientSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DeletionPolicy: nifiv1alpha1.DeletionPolicyDelete,
		},
		Status: nifiv1alpha1.NiFiRegistryClientStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				NiFiID:   "registry-existing",
				Revision: nifiv1alpha1.RevisionStatus{Version: 12},
			},
		},
	}
	k8sClient := newRegistryClientTestClient(scheme, cluster, registry)
	nifiClient := &fakeRegistryClientClient{}
	reconciler := &NiFiRegistryClientReconciler{
		Client:               k8sClient,
		Scheme:               scheme,
		RegistryClientClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: registry.Name, Namespace: registry.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	if len(nifiClient.deleted) != 1 || nifiClient.deleted[0] != "registry-existing" {
		t.Fatalf("deleted = %#v, want registry-existing", nifiClient.deleted)
	}
}

func newRegistryClientTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRegistryClient{}).
		Build()
}

func reconcileRegistryClientTwice(t *testing.T, reconciler *NiFiRegistryClientReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
