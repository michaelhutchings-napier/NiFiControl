package controller

import (
	"context"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// --- users ------------------------------------------------------------------

func findUserByIdentity(users []nifi.UserEntity, identity string) *nifi.UserEntity {
	for i := range users {
		if users[i].Component.Identity == identity {
			return &users[i]
		}
	}
	return nil
}

func markUserReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUser, nifiID string, revisionVersion int64) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "UserReady", "The NiFi user tenant is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markUserNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUser, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func userStatusMatches(obj *nifiv1alpha1.NiFiUser, nifiID string, revisionVersion int64) bool {
	return obj.Status.ObservedGeneration == obj.Generation &&
		obj.Status.Ready &&
		obj.Status.Dependencies.Ready &&
		obj.Status.NiFiID == nifiID &&
		obj.Status.Revision.Version == revisionVersion
}

func shouldMarkUserNotReady(obj *nifiv1alpha1.NiFiUser, reason, message string) bool {
	if obj.Status.ObservedGeneration != obj.Generation || obj.Status.Ready || obj.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range obj.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

// --- user groups ------------------------------------------------------------

func tenantRefs(ids []string) []nifi.TenantRef {
	refs := make([]nifi.TenantRef, 0, len(ids))
	for _, id := range ids {
		refs = append(refs, nifi.TenantRef{ID: id})
	}
	return refs
}

func tenantRefID(ref nifi.TenantRef) string {
	if ref.ID != "" {
		return ref.ID
	}
	if ref.Component != nil {
		return ref.Component.ID
	}
	return ""
}

func sameTenantSet(left, right []nifi.TenantRef) bool {
	if len(left) != len(right) {
		return false
	}
	set := make(map[string]struct{}, len(left))
	for _, ref := range left {
		set[tenantRefID(ref)] = struct{}{}
	}
	for _, ref := range right {
		if _, ok := set[tenantRefID(ref)]; !ok {
			return false
		}
	}
	return true
}

func sameStringSet(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	set := make(map[string]struct{}, len(left))
	for _, value := range left {
		set[value] = struct{}{}
	}
	for _, value := range right {
		if _, ok := set[value]; !ok {
			return false
		}
	}
	return true
}

func findUserGroupByIdentity(groups []nifi.UserGroupEntity, identity string) *nifi.UserGroupEntity {
	for i := range groups {
		if groups[i].Component.Identity == identity {
			return &groups[i]
		}
	}
	return nil
}

func userGroupNeedsUpdate(existing nifi.UserGroupEntity, desired nifi.UserGroupComponent) bool {
	if existing.Component.Identity != desired.Identity {
		return true
	}
	return !sameTenantSet(existing.Component.Users, desired.Users)
}

func markUserGroupReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUserGroup, nifiID string, revisionVersion int64, memberIDs []string) error {
	obj.Status.CommonStatus.MarkReady(obj.Generation, "UserGroupReady", "The NiFi user group tenant is reconciled.")
	obj.Status.NiFiID = nifiID
	obj.Status.Revision.Version = revisionVersion
	obj.Status.MemberIDs = memberIDs
	obj.Status.Sync.LastError = ""
	return c.Status().Update(ctx, obj)
}

func markUserGroupNotReady(ctx context.Context, c client.Client, obj *nifiv1alpha1.NiFiUserGroup, reason, message string) error {
	obj.Status.CommonStatus.MarkNotReady(obj.Generation, reason, message)
	obj.Status.Dependencies.Ready = true
	obj.Status.Dependencies.WaitingFor = nil
	obj.Status.Sync.LastError = message
	return c.Status().Update(ctx, obj)
}

func userGroupStatusMatches(obj *nifiv1alpha1.NiFiUserGroup, nifiID string, revisionVersion int64, memberIDs []string) bool {
	return obj.Status.ObservedGeneration == obj.Generation &&
		obj.Status.Ready &&
		obj.Status.Dependencies.Ready &&
		obj.Status.NiFiID == nifiID &&
		obj.Status.Revision.Version == revisionVersion &&
		sameStringSet(obj.Status.MemberIDs, memberIDs)
}

func shouldMarkUserGroupNotReady(obj *nifiv1alpha1.NiFiUserGroup, reason, message string) bool {
	if obj.Status.ObservedGeneration != obj.Generation || obj.Status.Ready || obj.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range obj.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}
