package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

// The root process group policies the operator's initial-admin identity needs in order to manage
// the canvas of a secured cluster. NiFi seeds the initial admin with the global policies (/flow,
// /tenants, /policies, /controller) but not these, so the operator grants them to itself. /data/*
// covers flow-file operations (queue listing and drops during rollouts).
var operatorRootGroupPolicyResources = []string{"/process-groups/%s", "/data/process-groups/%s"}

// ensureOperatorCanvasAccess grants the operator's initial-admin identity read/write access to the
// root process group of a secured, operator-managed cluster.
//
// NiFi's file access policy provider seeds the initial admin with the global policies but omits the
// root-process-group policies whenever the flow — and therefore the root group id — does not yet
// exist at first-boot authorizer initialization. That is always the case for a fresh managed
// cluster, so the operator (the initial admin) is authenticated but cannot create or manage any
// canvas component; every canvas reconcile fails with HTTP 403 "No applicable policies could be
// found". Because the initial admin does hold /policies and /tenants read/write, the operator
// repairs this by granting its own identity the root-group policies NiFi would otherwise have
// seeded. It is a no-op on insecure clusters (no managed authorizer) and on external clusters (not
// the operator's to bootstrap), and it is idempotent once the grants exist.
func (r *NiFiClusterReconciler) ensureOperatorCanvasAccess(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, endpoint string) error {
	if resolvedClusterMode(cluster) != nifiv1alpha1.ClusterModeInternal || !internalTLSEnabled(cluster) {
		return nil
	}
	if cluster.Status.TLS == nil || cluster.Status.TLS.InitialAdminIdentity == "" {
		return nil
	}
	identity := cluster.Status.TLS.InitialAdminIdentity

	rootID, err := nifi.HTTPProcessGroupClient{}.RootProcessGroupID(ctx, endpoint)
	if err != nil {
		return fmt.Errorf("resolve root process group id: %w", err)
	}
	if rootID == "" {
		return fmt.Errorf("root process group id is not available yet")
	}

	userID, err := userTenantIDForIdentity(ctx, nifi.HTTPUserClient{}, endpoint, identity)
	if err != nil {
		return fmt.Errorf("resolve operator tenant %q: %w", identity, err)
	}
	if userID == "" {
		return fmt.Errorf("operator tenant %q is not present yet", identity)
	}

	policies := nifi.HTTPAccessPolicyClient{}
	for _, resource := range operatorRootGroupPolicyResources {
		resource = fmt.Sprintf(resource, rootID)
		for _, action := range []string{"read", "write"} {
			if err := ensureTenantGrantedPolicy(ctx, policies, endpoint, action, resource, userID); err != nil {
				return fmt.Errorf("grant %s %s to the operator: %w", action, resource, err)
			}
		}
	}
	return nil
}

// userTenantIDForIdentity returns the NiFi tenant id of the user with the given identity, or an
// empty string when no such user exists yet.
func userTenantIDForIdentity(ctx context.Context, users nifi.UserClient, endpoint, identity string) (string, error) {
	list, err := users.ListUsers(ctx, endpoint)
	if err != nil {
		return "", err
	}
	for _, user := range list {
		if user.Component.Identity == identity {
			return nifi.UserEntityID(user), nil
		}
	}
	return "", nil
}

// ensureTenantGrantedPolicy makes sure the (action, resource) access policy exists and grants the
// given user tenant. It creates the policy when absent and adds the user when an existing policy
// does not already include it.
func ensureTenantGrantedPolicy(ctx context.Context, policies nifi.AccessPolicyClient, endpoint, action, resource, userID string) error {
	existing, err := policies.GetAccessPolicyForResource(ctx, endpoint, action, resource)
	if err != nil && !nifi.IsNotFound(err) {
		return err
	}
	// A non-404 response can be an inherited (effective) policy for an ancestor resource; only treat
	// it as the exact policy when the resource and action match.
	if existing != nil && existing.Component.Resource == resource && existing.Component.Action == action {
		for _, u := range existing.Component.Users {
			if u.ID == userID {
				return nil
			}
		}
		id := nifi.AccessPolicyEntityID(*existing)
		update := nifi.AccessPolicyEntity{
			Revision: existing.Revision,
			ID:       id,
			Component: nifi.AccessPolicyComponent{
				ID:         id,
				Resource:   resource,
				Action:     action,
				Users:      append(existing.Component.Users, nifi.TenantRef{ID: userID}),
				UserGroups: existing.Component.UserGroups,
			},
		}
		_, err := policies.UpdateAccessPolicy(ctx, endpoint, update)
		return err
	}
	_, err = policies.CreateAccessPolicy(ctx, endpoint, nifi.AccessPolicyEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: nifi.AccessPolicyComponent{Resource: resource, Action: action, Users: []nifi.TenantRef{{ID: userID}}},
	})
	return err
}
