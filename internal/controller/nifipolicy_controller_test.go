package controller

import (
	"context"
	"net/http"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

type fakeAccessPolicyClient struct {
	byResource map[string]nifi.AccessPolicyEntity
	created    []nifi.AccessPolicyEntity
	updated    []nifi.AccessPolicyEntity
	deleted    []string
}

func policyKey(action, resource string) string { return action + "|" + resource }

func (f *fakeAccessPolicyClient) GetAccessPolicyForResource(ctx context.Context, baseURI, action, resource string) (*nifi.AccessPolicyEntity, error) {
	if f.byResource != nil {
		if p, ok := f.byResource[policyKey(action, resource)]; ok {
			return &p, nil
		}
	}
	return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
}
func (f *fakeAccessPolicyClient) GetAccessPolicy(ctx context.Context, baseURI, id string) (*nifi.AccessPolicyEntity, error) {
	for _, p := range f.byResource {
		if nifi.AccessPolicyEntityID(p) == id {
			return &p, nil
		}
	}
	return nil, nil
}
func (f *fakeAccessPolicyClient) CreateAccessPolicy(ctx context.Context, baseURI string, entity nifi.AccessPolicyEntity) (*nifi.AccessPolicyEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "policy-created"
	created.Component.ID = "policy-created"
	if f.byResource == nil {
		f.byResource = map[string]nifi.AccessPolicyEntity{}
	}
	f.byResource[policyKey(entity.Component.Action, entity.Component.Resource)] = created
	return &created, nil
}
func (f *fakeAccessPolicyClient) UpdateAccessPolicy(ctx context.Context, baseURI string, entity nifi.AccessPolicyEntity) (*nifi.AccessPolicyEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	f.byResource[policyKey(entity.Component.Action, entity.Component.Resource)] = updated
	return &updated, nil
}
func (f *fakeAccessPolicyClient) DeleteAccessPolicy(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	return nil
}

func policyTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiUser{}, &nifiv1alpha1.NiFiPolicy{}).
		Build()
}

func readyUser(name, nifiID, identity string) *nifiv1alpha1.NiFiUser {
	return &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"}, Identity: identity},
		Status:     nifiv1alpha1.NiFiUserStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: nifiID, ObservedGeneration: 1}},
	}
}

func TestNiFiPolicyWatchesWakeCrossNamespaceReferences(t *testing.T) {
	// A policy in team-a may grant a NiFiUser/NiFiUserGroup or reference a NiFiCluster in another
	// namespace. Because the reconcile does not requeue while waiting on a dependency, the watch
	// mappers must find that policy when the cross-namespace dependency changes — even though the
	// changed object lives in a different namespace than the policy.
	scheme := testScheme()
	policy := &nifiv1alpha1.NiFiPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "grant", Namespace: "team-a", Generation: 1},
		Spec: nifiv1alpha1.NiFiPolicySpec{
			ClusterRef:    nifiv1alpha1.ClusterReference{Name: "prod", Namespace: "team-d"},
			Resource:      "/flow",
			Action:        "read",
			UserRefs:      []nifiv1alpha1.LocalObjectReference{{Name: "alice", Namespace: "team-b"}},
			UserGroupRefs: []nifiv1alpha1.LocalObjectReference{{Name: "readers", Namespace: "team-c"}},
		},
	}
	r := &NiFiPolicyReconciler{Client: policyTestClient(scheme, policy), Scheme: scheme}
	want := types.NamespacedName{Name: "grant", Namespace: "team-a"}

	assertWakes := func(name string, reqs []reconcile.Request) {
		t.Helper()
		if len(reqs) != 1 || reqs[0].NamespacedName != want {
			t.Fatalf("%s: expected the cross-namespace policy team-a/grant to be enqueued, got %#v", name, reqs)
		}
	}
	assertWakes("user", r.requestsForPolicyUser(context.Background(), &nifiv1alpha1.NiFiUser{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "team-b"}}))
	assertWakes("userGroup", r.requestsForPolicyUserGroup(context.Background(), &nifiv1alpha1.NiFiUserGroup{ObjectMeta: metav1.ObjectMeta{Name: "readers", Namespace: "team-c"}}))
	assertWakes("cluster", r.requestsForPolicyCluster(context.Background(), &nifiv1alpha1.NiFiCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "team-d"}}))

	// Namespace must still be discriminating: a same-named object in another namespace must NOT wake it.
	if reqs := r.requestsForPolicyUser(context.Background(), &nifiv1alpha1.NiFiUser{ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "team-x"}}); len(reqs) != 0 {
		t.Fatalf("user in a non-referenced namespace must not wake the policy, got %#v", reqs)
	}
	if reqs := r.requestsForPolicyCluster(context.Background(), &nifiv1alpha1.NiFiCluster{ObjectMeta: metav1.ObjectMeta{Name: "prod", Namespace: "team-x"}}); len(reqs) != 0 {
		t.Fatalf("cluster of the same name in another namespace must not wake the policy, got %#v", reqs)
	}
}

func TestNiFiPolicyReconcileCreatesPolicy(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := readyUser("scraper", "u-scraper", "CN=scraper")
	policy := &nifiv1alpha1.NiFiPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper-read-flow", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiPolicySpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Resource:   "/flow",
			Action:     "read",
			UserRefs:   []nifiv1alpha1.LocalObjectReference{{Name: "scraper"}},
		},
	}
	k8sClient := policyTestClient(scheme, cluster, user, policy)
	policies := &fakeAccessPolicyClient{}
	r := &NiFiPolicyReconciler{Client: k8sClient, Scheme: scheme, AccessPolicyClient: policies}
	reconcileTwice(t, r, policy.Name)

	if len(policies.created) != 1 {
		t.Fatalf("create policies = %#v", policies.created)
	}
	created := policies.created[0]
	if created.Component.Resource != "/flow" || created.Component.Action != "read" || len(created.Component.Users) != 1 || created.Component.Users[0].ID != "u-scraper" {
		t.Fatalf("created policy = %#v", created.Component)
	}
	got := &nifiv1alpha1.NiFiPolicy{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: policy.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "policy-created" || len(got.Status.UserIDs) != 1 || got.Status.UserIDs[0] != "u-scraper" {
		t.Fatalf("status = %+v", got.Status)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "PolicyReady")
}

func TestNiFiPolicyReconcileAdoptsExactPolicyAndAddsUser(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := readyUser("scraper", "u-scraper", "CN=scraper")
	policy := &nifiv1alpha1.NiFiPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper-read-flow", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiPolicySpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Resource:   "/flow",
			Action:     "read",
			UserRefs:   []nifiv1alpha1.LocalObjectReference{{Name: "scraper"}},
		},
	}
	// An exact policy already exists for /flow read, with no users.
	policies := &fakeAccessPolicyClient{byResource: map[string]nifi.AccessPolicyEntity{
		policyKey("read", "/flow"): {ID: "p-existing", Revision: nifi.Revision{Version: 2}, Component: nifi.AccessPolicyComponent{ID: "p-existing", Resource: "/flow", Action: "read"}},
	}}
	k8sClient := policyTestClient(scheme, cluster, user, policy)
	r := &NiFiPolicyReconciler{Client: k8sClient, Scheme: scheme, AccessPolicyClient: policies}
	reconcileTwice(t, r, policy.Name)

	if len(policies.created) != 0 {
		t.Fatalf("should adopt, not create: %#v", policies.created)
	}
	if len(policies.updated) != 1 || len(policies.updated[0].Component.Users) != 1 || policies.updated[0].Component.Users[0].ID != "u-scraper" {
		t.Fatalf("update = %#v", policies.updated)
	}
	got := &nifiv1alpha1.NiFiPolicy{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: policy.Name, Namespace: "default"}, got)
	if got.Status.NiFiID != "p-existing" {
		t.Fatalf("adopted id = %q", got.Status.NiFiID)
	}
}

func TestNiFiPolicyReconcileCreatesWhenOnlyInheritedPolicyExists(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := readyUser("scraper", "u-scraper", "CN=scraper")
	policy := &nifiv1alpha1.NiFiPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "pg-read", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiPolicySpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Resource:   "/process-groups/pg-1",
			Action:     "read",
			UserRefs:   []nifiv1alpha1.LocalObjectReference{{Name: "scraper"}},
		},
	}
	// NiFi returns an inherited policy for an ancestor resource (different Resource).
	policies := &fakeAccessPolicyClient{byResource: map[string]nifi.AccessPolicyEntity{
		policyKey("read", "/process-groups/pg-1"): {ID: "p-inherited", Component: nifi.AccessPolicyComponent{ID: "p-inherited", Resource: "/process-groups/root", Action: "read"}},
	}}
	k8sClient := policyTestClient(scheme, cluster, user, policy)
	r := &NiFiPolicyReconciler{Client: k8sClient, Scheme: scheme, AccessPolicyClient: policies}
	reconcileTwice(t, r, policy.Name)

	if len(policies.created) != 1 {
		t.Fatalf("should create an exact policy when only an inherited one exists: %#v", policies.created)
	}
	if policies.created[0].Component.Resource != "/process-groups/pg-1" {
		t.Fatalf("created resource = %q", policies.created[0].Component.Resource)
	}
}

func TestNiFiPolicyReconcileRejectsConflictingTuple(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := readyUser("scraper", "u-scraper", "CN=scraper")
	tuple := func(name string, created int64) *nifiv1alpha1.NiFiPolicy {
		return &nifiv1alpha1.NiFiPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1, CreationTimestamp: metav1.NewTime(time.Unix(created, 0))},
			Spec: nifiv1alpha1.NiFiPolicySpec{
				ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
				Resource:   "/flow",
				Action:     "read",
				UserRefs:   []nifiv1alpha1.LocalObjectReference{{Name: "scraper"}},
			},
		}
	}
	owner := tuple("aaa-owner", 100) // older -> owns the tuple
	loser := tuple("zzz-loser", 200) // newer -> must yield
	k8sClient := policyTestClient(scheme, cluster, user, owner, loser)
	policies := &fakeAccessPolicyClient{}
	r := &NiFiPolicyReconciler{Client: k8sClient, Scheme: scheme, AccessPolicyClient: policies}

	// The loser must not touch NiFi and must report a conflict.
	reconcileTwice(t, r, loser.Name)
	if len(policies.created) != 0 || len(policies.updated) != 0 {
		t.Fatalf("loser must not write to NiFi: created=%#v updated=%#v", policies.created, policies.updated)
	}
	gotLoser := &nifiv1alpha1.NiFiPolicy{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: loser.Name, Namespace: "default"}, gotLoser)
	assertControllerCondition(t, gotLoser.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionFalse, "PolicyConflict")

	// The owner reconciles normally.
	reconcileTwice(t, r, owner.Name)
	if len(policies.created) != 1 {
		t.Fatalf("owner should create exactly one policy: %#v", policies.created)
	}
	gotOwner := &nifiv1alpha1.NiFiPolicy{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: owner.Name, Namespace: "default"}, gotOwner)
	if !gotOwner.Status.Ready {
		t.Fatalf("owner status = %+v", gotOwner.Status)
	}
}

func TestNiFiPolicyReconcileWaitsForUser(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	// User exists but is not Ready.
	user := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, Identity: "CN=scraper"},
	}
	policy := &nifiv1alpha1.NiFiPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper-read-flow", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiPolicySpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Resource:   "/flow",
			Action:     "read",
			UserRefs:   []nifiv1alpha1.LocalObjectReference{{Name: "scraper"}},
		},
	}
	k8sClient := policyTestClient(scheme, cluster, user, policy)
	policies := &fakeAccessPolicyClient{}
	r := &NiFiPolicyReconciler{Client: k8sClient, Scheme: scheme, AccessPolicyClient: policies}
	reconcileTwice(t, r, policy.Name)

	if len(policies.created) != 0 {
		t.Fatalf("should wait for the user, not create: %#v", policies.created)
	}
	got := &nifiv1alpha1.NiFiPolicy{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: policy.Name, Namespace: "default"}, got)
	if got.Status.Dependencies.Ready {
		t.Fatal("dependencies should not be ready while the user is not ready")
	}
}
