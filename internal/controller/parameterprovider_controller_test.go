package controller

import (
	"context"
	"net/http"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeParameterProviderClient struct {
	validation string // validation status reported on create (default VALID)
	store      *nifi.ParameterProviderEntity
	created    []nifi.ParameterProviderEntity
	updated    []nifi.ParameterProviderEntity
	deleted    []string
}

func (f *fakeParameterProviderClient) GetParameterProvider(ctx context.Context, baseURI, id string) (*nifi.ParameterProviderEntity, error) {
	if f.store != nil && nifi.ParameterProviderEntityID(*f.store) == id {
		s := *f.store
		return &s, nil
	}
	return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
}

func (f *fakeParameterProviderClient) CreateParameterProvider(ctx context.Context, baseURI string, entity nifi.ParameterProviderEntity) (*nifi.ParameterProviderEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "pp-created"
	created.Component.ID = "pp-created"
	created.Component.ValidationStatus = "VALID"
	if f.validation != "" {
		created.Component.ValidationStatus = f.validation
	}
	f.store = &created
	s := created
	return &s, nil
}

func (f *fakeParameterProviderClient) UpdateParameterProvider(ctx context.Context, baseURI string, entity nifi.ParameterProviderEntity) (*nifi.ParameterProviderEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	if f.store != nil {
		updated.Component.ValidationStatus = f.store.Component.ValidationStatus
	}
	f.store = &updated
	s := updated
	return &s, nil
}

func (f *fakeParameterProviderClient) DeleteParameterProvider(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	f.store = nil
	return nil
}

func parameterProviderTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiParameterProvider{}).
		Build()
}

func newParameterProvider(name string) *nifiv1alpha1.NiFiParameterProvider {
	return &nifiv1alpha1.NiFiParameterProvider{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiParameterProviderSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Type:       "org.apache.nifi.parameter.EnvironmentVariableParameterProvider",
			Properties: map[string]string{"Parameter Group Name": "envs"},
		},
	}
}

func TestNiFiParameterProviderReconcileCreates(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	pp := newParameterProvider("secrets")
	k8sClient := parameterProviderTestClient(scheme, cluster, pp)
	providers := &fakeParameterProviderClient{}
	r := &NiFiParameterProviderReconciler{Client: k8sClient, Scheme: scheme, ParameterProviderClient: providers}
	reconcileTwice(t, r, pp.Name)

	if len(providers.created) != 1 {
		t.Fatalf("create parameter providers = %#v", providers.created)
	}
	if providers.created[0].Component.Type != "org.apache.nifi.parameter.EnvironmentVariableParameterProvider" {
		t.Fatalf("create payload = %#v", providers.created[0].Component)
	}
	got := &nifiv1alpha1.NiFiParameterProvider{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: pp.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "pp-created" || got.Status.ValidationStatus != "VALID" {
		t.Fatalf("status = %+v", got.Status)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "ParameterProviderReady")
}

func TestNiFiParameterProviderReconcileResolvesSensitiveProperty(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "aws-creds", Namespace: "default"},
		Data:       map[string][]byte{"secret-access-key": []byte("s3cr3t")},
	}
	pp := newParameterProvider("secrets")
	pp.Spec.SensitiveProperties = map[string]nifiv1alpha1.SensitivePropertySource{
		"Secret Access Key": {SecretKeyRef: &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "aws-creds"}, Key: "secret-access-key"}}},
	}
	k8sClient := parameterProviderTestClient(scheme, cluster, secret, pp)
	providers := &fakeParameterProviderClient{}
	r := &NiFiParameterProviderReconciler{Client: k8sClient, Scheme: scheme, ParameterProviderClient: providers}
	reconcileTwice(t, r, pp.Name)

	if len(providers.created) != 1 {
		t.Fatalf("create parameter providers = %#v", providers.created)
	}
	if providers.created[0].Component.Properties["Secret Access Key"] != "s3cr3t" {
		t.Fatalf("sensitive property was not resolved from the Secret: %#v", providers.created[0].Component.Properties)
	}
}

func TestNiFiParameterProviderReconcileWaitsForMissingSecret(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	pp := newParameterProvider("secrets")
	pp.Spec.SensitiveProperties = map[string]nifiv1alpha1.SensitivePropertySource{
		"Secret Access Key": {SecretKeyRef: &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "absent"}, Key: "secret-access-key"}}},
	}
	k8sClient := parameterProviderTestClient(scheme, cluster, pp)
	providers := &fakeParameterProviderClient{}
	r := &NiFiParameterProviderReconciler{Client: k8sClient, Scheme: scheme, ParameterProviderClient: providers}
	reconcileTwice(t, r, pp.Name)

	if len(providers.created) != 0 {
		t.Fatalf("provider must not be created until its Secret exists: %#v", providers.created)
	}
	got := &nifiv1alpha1.NiFiParameterProvider{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: pp.Name, Namespace: "default"}, got)
	if got.Status.Ready || got.Status.Dependencies.Ready {
		t.Fatalf("expected waiting-for-dependencies, status = %+v", got.Status)
	}
}

func TestNiFiParameterProviderReconcileUpdatesOnDrift(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	pp := newParameterProvider("secrets")
	pp.Status = nifiv1alpha1.NiFiParameterProviderStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "pp-1", ObservedGeneration: 1, Dependencies: nifiv1alpha1.DependencyStatus{Ready: true}}}
	k8sClient := parameterProviderTestClient(scheme, cluster, pp)
	providers := &fakeParameterProviderClient{store: &nifi.ParameterProviderEntity{
		ID:       "pp-1",
		Revision: nifi.Revision{Version: 3},
		Component: nifi.ParameterProviderComponent{
			ID:               "pp-1",
			Name:             "secrets",
			Type:             "org.apache.nifi.parameter.EnvironmentVariableParameterProvider",
			Properties:       map[string]string{"Parameter Group Name": "STALE"},
			ValidationStatus: "VALID",
		},
	}}
	r := &NiFiParameterProviderReconciler{Client: k8sClient, Scheme: scheme, ParameterProviderClient: providers}
	reconcileTwice(t, r, pp.Name)

	if len(providers.updated) == 0 {
		t.Fatalf("expected an update to reconcile the drifted property")
	}
	if providers.updated[0].Component.Properties["Parameter Group Name"] != "envs" {
		t.Fatalf("update payload did not carry the desired property: %#v", providers.updated[0].Component.Properties)
	}
}

func TestNiFiParameterProviderDeleteRemovesFromNiFi(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	pp := newParameterProvider("secrets")
	pp.Finalizers = []string{NiFiControlFinalizer}
	pp.Spec.DeletionPolicy = nifiv1alpha1.DeletionPolicyDelete
	pp.Status = nifiv1alpha1.NiFiParameterProviderStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "pp-1", ObservedGeneration: 1}}
	k8sClient := parameterProviderTestClient(scheme, cluster, pp)
	providers := &fakeParameterProviderClient{store: &nifi.ParameterProviderEntity{ID: "pp-1", Revision: nifi.Revision{Version: 2}, Component: nifi.ParameterProviderComponent{ID: "pp-1"}}}
	r := &NiFiParameterProviderReconciler{Client: k8sClient, Scheme: scheme, ParameterProviderClient: providers}

	if err := k8sClient.Delete(context.Background(), pp); err != nil {
		t.Fatal(err)
	}
	reconcileTwice(t, r, pp.Name)

	if len(providers.deleted) != 1 || providers.deleted[0] != "pp-1" {
		t.Fatalf("expected the provider to be deleted: %#v", providers.deleted)
	}
	got := &nifiv1alpha1.NiFiParameterProvider{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: pp.Name, Namespace: "default"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer should be removed and the provider deleted; got err=%v", err)
	}
}
