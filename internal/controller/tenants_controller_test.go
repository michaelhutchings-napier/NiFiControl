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

type fakeUserClient struct {
	users   []nifi.UserEntity
	created []nifi.UserEntity
	updated []nifi.UserEntity
	deleted []string
}

func (f *fakeUserClient) ListUsers(ctx context.Context, baseURI string) ([]nifi.UserEntity, error) {
	return f.users, nil
}
func (f *fakeUserClient) GetUser(ctx context.Context, baseURI, id string) (*nifi.UserEntity, error) {
	for i := range f.users {
		if nifi.UserEntityID(f.users[i]) == id {
			return &f.users[i], nil
		}
	}
	return nil, nil
}
func (f *fakeUserClient) CreateUser(ctx context.Context, baseURI string, entity nifi.UserEntity) (*nifi.UserEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "user-created"
	created.Component.ID = "user-created"
	f.users = append(f.users, created)
	return &created, nil
}
func (f *fakeUserClient) UpdateUser(ctx context.Context, baseURI string, entity nifi.UserEntity) (*nifi.UserEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	return &updated, nil
}
func (f *fakeUserClient) DeleteUser(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	return nil
}

type fakeUserGroupClient struct {
	groups  []nifi.UserGroupEntity
	created []nifi.UserGroupEntity
	updated []nifi.UserGroupEntity
}

func (f *fakeUserGroupClient) ListUserGroups(ctx context.Context, baseURI string) ([]nifi.UserGroupEntity, error) {
	return f.groups, nil
}
func (f *fakeUserGroupClient) GetUserGroup(ctx context.Context, baseURI, id string) (*nifi.UserGroupEntity, error) {
	for i := range f.groups {
		if nifi.UserGroupEntityID(f.groups[i]) == id {
			return &f.groups[i], nil
		}
	}
	return nil, nil
}
func (f *fakeUserGroupClient) CreateUserGroup(ctx context.Context, baseURI string, entity nifi.UserGroupEntity) (*nifi.UserGroupEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "group-created"
	created.Component.ID = "group-created"
	f.groups = append(f.groups, created)
	return &created, nil
}
func (f *fakeUserGroupClient) UpdateUserGroup(ctx context.Context, baseURI string, entity nifi.UserGroupEntity) (*nifi.UserGroupEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	return &updated, nil
}
func (f *fakeUserGroupClient) DeleteUserGroup(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	return nil
}

func tenantTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiUser{}, &nifiv1alpha1.NiFiUserGroup{}).
		Build()
}

func reconcileTwice(t *testing.T, r interface {
	Reconcile(context.Context, ctrl.Request) (ctrl.Result, error)
}, name string) {
	t.Helper()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: name, Namespace: "default"}}
	for range 2 {
		if _, err := r.Reconcile(context.Background(), req); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNiFiUserReconcileCreatesTenant(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, Identity: "CN=scraper"},
	}
	k8sClient := tenantTestClient(scheme, cluster, user)
	users := &fakeUserClient{}
	r := &NiFiUserReconciler{Client: k8sClient, Scheme: scheme, UserClient: users}
	reconcileTwice(t, r, user.Name)

	if len(users.created) != 1 || users.created[0].Component.Identity != "CN=scraper" {
		t.Fatalf("create users = %#v", users.created)
	}
	got := &nifiv1alpha1.NiFiUser{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: user.Name, Namespace: "default"}, got); err != nil {
		t.Fatal(err)
	}
	if !got.Status.Ready || got.Status.NiFiID != "user-created" {
		t.Fatalf("status = %+v", got.Status)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "UserReady")
}

func TestNiFiUserReconcileAdoptsByIdentity(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	user := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "scraper", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, Identity: "CN=scraper"},
	}
	k8sClient := tenantTestClient(scheme, cluster, user)
	users := &fakeUserClient{users: []nifi.UserEntity{
		{ID: "existing-u", Component: nifi.UserComponent{ID: "existing-u", Identity: "CN=scraper"}},
	}}
	r := &NiFiUserReconciler{Client: k8sClient, Scheme: scheme, UserClient: users}
	reconcileTwice(t, r, user.Name)

	if len(users.created) != 0 {
		t.Fatalf("should adopt, not create: %#v", users.created)
	}
	got := &nifiv1alpha1.NiFiUser{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: user.Name, Namespace: "default"}, got)
	if got.Status.NiFiID != "existing-u" {
		t.Fatalf("adopted id = %q", got.Status.NiFiID)
	}
}

func TestNiFiUserGroupReconcileCreatesWithMembers(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	alice := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, Identity: "CN=alice"},
		Status:     nifiv1alpha1.NiFiUserStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "u-alice", ObservedGeneration: 1}},
	}
	group := &nifiv1alpha1.NiFiUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "editors", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Identity:   "editors",
			Users:      []nifiv1alpha1.UserGroupMember{{UserRef: nifiv1alpha1.LocalObjectReference{Name: "alice"}}},
		},
	}
	k8sClient := tenantTestClient(scheme, cluster, alice, group)
	groups := &fakeUserGroupClient{}
	r := &NiFiUserGroupReconciler{Client: k8sClient, Scheme: scheme, UserGroupClient: groups}
	reconcileTwice(t, r, group.Name)

	if len(groups.created) != 1 {
		t.Fatalf("create groups = %#v", groups.created)
	}
	created := groups.created[0]
	if created.Component.Identity != "editors" || len(created.Component.Users) != 1 || created.Component.Users[0].ID != "u-alice" {
		t.Fatalf("created group = %#v", created.Component)
	}
	got := &nifiv1alpha1.NiFiUserGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: group.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "group-created" || len(got.Status.MemberIDs) != 1 || got.Status.MemberIDs[0] != "u-alice" {
		t.Fatalf("status = %+v", got.Status)
	}
}

func TestNiFiUserGroupReconcileWaitsForMembers(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	// Member user exists but is not Ready yet.
	alice := &nifiv1alpha1.NiFiUser{
		ObjectMeta: metav1.ObjectMeta{Name: "alice", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiUserSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, Identity: "CN=alice"},
	}
	group := &nifiv1alpha1.NiFiUserGroup{
		ObjectMeta: metav1.ObjectMeta{Name: "editors", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiUserGroupSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Identity:   "editors",
			Users:      []nifiv1alpha1.UserGroupMember{{UserRef: nifiv1alpha1.LocalObjectReference{Name: "alice"}}},
		},
	}
	k8sClient := tenantTestClient(scheme, cluster, alice, group)
	groups := &fakeUserGroupClient{}
	r := &NiFiUserGroupReconciler{Client: k8sClient, Scheme: scheme, UserGroupClient: groups}
	reconcileTwice(t, r, group.Name)

	if len(groups.created) != 0 {
		t.Fatalf("should wait for members, not create: %#v", groups.created)
	}
	got := &nifiv1alpha1.NiFiUserGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: group.Name, Namespace: "default"}, got)
	if got.Status.Dependencies.Ready {
		t.Fatal("dependencies should not be ready while a member is not ready")
	}
}
