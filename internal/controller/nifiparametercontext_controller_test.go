package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeParameterContextClient struct {
	contexts      []nifi.ParameterContextEntity
	created       []nifi.ParameterContextEntity
	updated       []nifi.ParameterContextEntity
	deleted       []string
	updateRequest nifi.ParameterContextUpdateRequestEntity
	err           error
}

func (f *fakeParameterContextClient) ListParameterContexts(ctx context.Context, baseURI string) ([]nifi.ParameterContextEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.contexts, nil
}

func (f *fakeParameterContextClient) GetParameterContext(ctx context.Context, baseURI string, id string) (*nifi.ParameterContextEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	for i := range f.contexts {
		if parameterContextEntityID(f.contexts[i]) == id {
			return &f.contexts[i], nil
		}
	}
	return nil, nil
}

func (f *fakeParameterContextClient) CreateParameterContext(ctx context.Context, baseURI string, entity nifi.ParameterContextEntity) (*nifi.ParameterContextEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.created = append(f.created, entity)
	created := entity
	created.ID = "pc-created"
	created.Component.ID = "pc-created"
	created.Revision.Version = 0
	f.contexts = append(f.contexts, created)
	return &created, nil
}

func (f *fakeParameterContextClient) DeleteParameterContext(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	if f.err != nil {
		return f.err
	}
	f.deleted = append(f.deleted, id)
	return nil
}

func (f *fakeParameterContextClient) CreateParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, entity nifi.ParameterContextEntity) (*nifi.ParameterContextUpdateRequestEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.updated = append(f.updated, entity)
	if f.updateRequest.Request.RequestID == "" {
		f.updateRequest = nifi.ParameterContextUpdateRequestEntity{
			Request: nifi.ParameterContextUpdateRequest{
				RequestID:        "update-1",
				Complete:         false,
				PercentCompleted: 50,
				State:            "Updating",
			},
		}
	}
	return &f.updateRequest, nil
}

func (f *fakeParameterContextClient) GetParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, requestID string) (*nifi.ParameterContextUpdateRequestEntity, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.updateRequest.Request.RequestID == "" {
		f.updateRequest = nifi.ParameterContextUpdateRequestEntity{
			Request: nifi.ParameterContextUpdateRequest{
				RequestID:        requestID,
				Complete:         true,
				PercentCompleted: 100,
				State:            "Complete",
			},
		}
	}
	return &f.updateRequest, nil
}

func TestNiFiParameterContextReconcileCreatesParameterContext(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Description: "Payments parameters",
			Parameters: []nifiv1alpha1.Parameter{
				{Name: "kafka.bootstrap.servers", Value: "kafka:9092"},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	created := nifiClient.created[0]
	if created.Component.Name != "payments" {
		t.Fatalf("created name = %q, want payments", created.Component.Name)
	}
	if len(created.Component.Parameters) != 1 {
		t.Fatalf("created parameters = %d, want 1", len(created.Component.Parameters))
	}
	value := created.Component.Parameters[0].Parameter.Value
	if value == nil || *value != "kafka:9092" {
		t.Fatalf("created parameter value = %v, want kafka:9092", value)
	}

	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("parameter context ready = false, want true")
	}
	if current.Status.NiFiID != "pc-created" {
		t.Fatalf("status nifi id = %q, want pc-created", current.Status.NiFiID)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "ParameterContextReady")
}

func TestNiFiParameterContextReconcileReadsSensitiveParameterSecret(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "payments-db", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("super-secret")},
	}
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Parameters: []nifiv1alpha1.Parameter{
				{
					Name: "db.password",
					SensitiveValueFrom: &nifiv1alpha1.SensitivePropertySource{
						SecretKeyRef: &nifiv1alpha1.SecretKeyRef{
							SecretKeySelector: corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: secret.Name},
								Key:                  "password",
							},
						},
					},
				},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, secret, parameterContext)
	nifiClient := &fakeParameterContextClient{}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 1 {
		t.Fatalf("created count = %d, want 1", len(nifiClient.created))
	}
	parameter := nifiClient.created[0].Component.Parameters[0].Parameter
	if !parameter.Sensitive {
		t.Fatal("sensitive = false, want true")
	}
	if parameter.Value == nil || *parameter.Value != "super-secret" {
		t.Fatalf("sensitive value = %v, want secret value", parameter.Value)
	}
}

func TestNiFiParameterContextReconcileAdoptsExistingByNameWhenAllowed(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			AdoptionPolicy: nifiv1alpha1.AdoptionPolicy{Mode: nifiv1alpha1.AdoptionPolicyAdoptByName},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{
		contexts: []nifi.ParameterContextEntity{
			{ID: "pc-existing", Revision: nifi.Revision{Version: 12}, Component: nifi.ParameterContextComponent{Name: "payments"}},
		},
	}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.created) != 0 {
		t.Fatalf("created count = %d, want 0", len(nifiClient.created))
	}
	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.NiFiID != "pc-existing" {
		t.Fatalf("status nifi id = %q, want pc-existing", current.Status.NiFiID)
	}
	if current.Status.Revision.Version != 12 {
		t.Fatalf("status revision = %d, want 12", current.Status.Revision.Version)
	}
}

func TestNiFiParameterContextReconcileSubmitsUpdateRequest(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Description: "Changed description",
		},
		Status: nifiv1alpha1.NiFiParameterContextStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				ObservedGeneration: 1,
				Ready:              true,
				NiFiID:             "pc-existing",
				Revision:           nifiv1alpha1.RevisionStatus{Version: 12},
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{
		contexts: []nifi.ParameterContextEntity{
			{ID: "pc-existing", Revision: nifi.Revision{Version: 12}, Component: nifi.ParameterContextComponent{Name: "payments", Description: "Old description"}},
		},
	}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	if len(nifiClient.updated) != 1 {
		t.Fatalf("updated count = %d, want 1", len(nifiClient.updated))
	}
	if nifiClient.updated[0].Component.Description != "Changed description" {
		t.Fatalf("updated description = %q, want Changed description", nifiClient.updated[0].Component.Description)
	}
	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.LatestUpdateRequest == nil || current.Status.LatestUpdateRequest.ID != "update-1" {
		t.Fatalf("latest update request = %#v, want update-1", current.Status.LatestUpdateRequest)
	}
	assertControllerCondition(t, current.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "UpdateRunning")
}

func TestNiFiParameterContextReconcilePollsUpdateRequestReady(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:  nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Description: "Changed description",
		},
		Status: nifiv1alpha1.NiFiParameterContextStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				ObservedGeneration: 2,
				Ready:              false,
				NiFiID:             "pc-existing",
				Revision:           nifiv1alpha1.RevisionStatus{Version: 12},
				Dependencies:       nifiv1alpha1.DependencyStatus{Ready: true},
			},
			LatestUpdateRequest: &nifiv1alpha1.ParameterContextUpdateRequestStatus{
				ID:       "update-1",
				Complete: false,
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{
		contexts: []nifi.ParameterContextEntity{
			{ID: "pc-existing", Revision: nifi.Revision{Version: 13}, Component: nifi.ParameterContextComponent{Name: "payments", Description: "Changed description"}},
		},
		updateRequest: nifi.ParameterContextUpdateRequestEntity{
			Request: nifi.ParameterContextUpdateRequest{
				RequestID:        "update-1",
				Complete:         true,
				PercentCompleted: 100,
				State:            "Complete",
			},
		},
	}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	reconcileParameterContextTwice(t, reconciler, request)

	current := &nifiv1alpha1.NiFiParameterContext{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if !current.Status.Ready {
		t.Fatal("parameter context ready = false, want true")
	}
	if current.Status.Revision.Version != 13 {
		t.Fatalf("revision = %d, want 13", current.Status.Revision.Version)
	}
	if current.Status.LatestUpdateRequest == nil || !current.Status.LatestUpdateRequest.Complete {
		t.Fatalf("latest update request = %#v, want complete", current.Status.LatestUpdateRequest)
	}
}

func TestNiFiParameterContextReconcileDeletesNiFiContextWhenPolicyDelete(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	deletionTime := metav1.Now()
	parameterContext := &nifiv1alpha1.NiFiParameterContext{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "payments",
			Namespace:         "default",
			Generation:        1,
			Finalizers:        []string{NiFiControlFinalizer},
			DeletionTimestamp: &deletionTime,
		},
		Spec: nifiv1alpha1.NiFiParameterContextSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			DeletionPolicy: nifiv1alpha1.DeletionPolicyDelete,
			AdoptionPolicy: nifiv1alpha1.AdoptionPolicy{Mode: nifiv1alpha1.AdoptionPolicyAdoptByName},
			Reconciliation: nifiv1alpha1.ReconciliationPolicy{},
			DriftPolicy:    nifiv1alpha1.DriftPolicy{},
			InheritedRefs:  nil,
			Parameters:     nil,
			Description:    "",
		},
		Status: nifiv1alpha1.NiFiParameterContextStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{
				NiFiID:   "pc-existing",
				Revision: nifiv1alpha1.RevisionStatus{Version: 12},
			},
		},
	}
	k8sClient := newParameterContextTestClient(scheme, cluster, parameterContext)
	nifiClient := &fakeParameterContextClient{}
	reconciler := &NiFiParameterContextReconciler{
		Client:                 k8sClient,
		Scheme:                 scheme,
		ParameterContextClient: nifiClient,
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: parameterContext.Name, Namespace: parameterContext.Namespace}}

	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	if len(nifiClient.deleted) != 1 || nifiClient.deleted[0] != "pc-existing" {
		t.Fatalf("deleted = %#v, want pc-existing", nifiClient.deleted)
	}
}

func testScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	return scheme
}

func readyTestCluster() *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "production", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			API: &nifiv1alpha1.NiFiClusterAPISpec{URI: "https://nifi.example.com"},
		},
		Status: nifiv1alpha1.NiFiClusterStatus{
			CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, ObservedGeneration: 1},
			Endpoint:     "https://nifi.example.com",
		},
	}
}

func newParameterContextTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiParameterContext{}).
		Build()
}

func reconcileParameterContextTwice(t *testing.T, reconciler *NiFiParameterContextReconciler, request ctrl.Request) {
	t.Helper()
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}
}
