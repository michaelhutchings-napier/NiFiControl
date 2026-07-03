package controller

import (
	"context"
	"testing"

	nifi "github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

func TestUserTenantIDForIdentity(t *testing.T) {
	users := &fakeUserClient{users: []nifi.UserEntity{
		{ID: "u-node", Component: nifi.UserComponent{ID: "u-node", Identity: "CN=central-node"}},
		{ID: "u-op", Component: nifi.UserComponent{ID: "u-op", Identity: "CN=central-operator"}},
	}}

	id, err := userTenantIDForIdentity(context.Background(), users, "https://nifi", "CN=central-operator")
	if err != nil {
		t.Fatal(err)
	}
	if id != "u-op" {
		t.Fatalf("tenant id = %q, want u-op", id)
	}

	missing, err := userTenantIDForIdentity(context.Background(), users, "https://nifi", "CN=absent")
	if err != nil {
		t.Fatal(err)
	}
	if missing != "" {
		t.Fatalf("tenant id for absent identity = %q, want empty", missing)
	}
}

func TestEnsureTenantGrantedPolicyCreatesWhenAbsent(t *testing.T) {
	policies := &fakeAccessPolicyClient{}
	if err := ensureTenantGrantedPolicy(context.Background(), policies, "https://nifi", "write", "/process-groups/root-id", "u-op"); err != nil {
		t.Fatal(err)
	}
	if len(policies.created) != 1 {
		t.Fatalf("created %d policies, want 1", len(policies.created))
	}
	created := policies.created[0].Component
	if created.Resource != "/process-groups/root-id" || created.Action != "write" {
		t.Fatalf("created policy = %+v", created)
	}
	if len(created.Users) != 1 || created.Users[0].ID != "u-op" {
		t.Fatalf("created policy users = %+v, want [u-op]", created.Users)
	}
}

func TestEnsureTenantGrantedPolicyAddsUserToExisting(t *testing.T) {
	policies := &fakeAccessPolicyClient{byResource: map[string]nifi.AccessPolicyEntity{
		policyKey("write", "/process-groups/root-id"): {
			ID: "p1",
			Component: nifi.AccessPolicyComponent{
				ID: "p1", Resource: "/process-groups/root-id", Action: "write",
				Users: []nifi.TenantRef{{ID: "u-other"}},
			},
		},
	}}
	if err := ensureTenantGrantedPolicy(context.Background(), policies, "https://nifi", "write", "/process-groups/root-id", "u-op"); err != nil {
		t.Fatal(err)
	}
	if len(policies.created) != 0 {
		t.Fatalf("created %d policies, want 0 (should update)", len(policies.created))
	}
	if len(policies.updated) != 1 {
		t.Fatalf("updated %d policies, want 1", len(policies.updated))
	}
	users := policies.updated[0].Component.Users
	if len(users) != 2 || users[0].ID != "u-other" || users[1].ID != "u-op" {
		t.Fatalf("updated policy users = %+v, want [u-other u-op]", users)
	}
}

func TestEnsureTenantGrantedPolicyIdempotentWhenUserPresent(t *testing.T) {
	policies := &fakeAccessPolicyClient{byResource: map[string]nifi.AccessPolicyEntity{
		policyKey("read", "/process-groups/root-id"): {
			ID: "p1",
			Component: nifi.AccessPolicyComponent{
				ID: "p1", Resource: "/process-groups/root-id", Action: "read",
				Users: []nifi.TenantRef{{ID: "u-op"}},
			},
		},
	}}
	if err := ensureTenantGrantedPolicy(context.Background(), policies, "https://nifi", "read", "/process-groups/root-id", "u-op"); err != nil {
		t.Fatal(err)
	}
	if len(policies.created) != 0 || len(policies.updated) != 0 {
		t.Fatalf("expected no writes for an already-granted policy; created=%d updated=%d", len(policies.created), len(policies.updated))
	}
}

// An inherited (effective) policy NiFi returns for an ancestor resource must not be mistaken for the
// exact policy; the operator must create a policy for the concrete resource instead.
func TestEnsureTenantGrantedPolicyCreatesWhenOnlyInheritedExists(t *testing.T) {
	policies := &fakeAccessPolicyClient{byResource: map[string]nifi.AccessPolicyEntity{
		policyKey("write", "/process-groups/root-id"): {
			ID: "parent",
			Component: nifi.AccessPolicyComponent{
				ID: "parent", Resource: "/process-groups/parent-id", Action: "write",
				Users: []nifi.TenantRef{{ID: "u-op"}},
			},
		},
	}}
	if err := ensureTenantGrantedPolicy(context.Background(), policies, "https://nifi", "write", "/process-groups/root-id", "u-op"); err != nil {
		t.Fatal(err)
	}
	if len(policies.created) != 1 {
		t.Fatalf("created %d policies, want 1 (inherited policy is not the exact policy)", len(policies.created))
	}
	if policies.created[0].Component.Resource != "/process-groups/root-id" {
		t.Fatalf("created policy resource = %q, want /process-groups/root-id", policies.created[0].Component.Resource)
	}
}
