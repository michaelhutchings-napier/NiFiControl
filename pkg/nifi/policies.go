package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

// Access policy entities model the NiFi /policies API. An access policy grants a (resource,
// action) tuple to a set of user and user-group tenants. Policies exist only on a secured NiFi
// with a managed authorizer.

// AccessPolicyComponent is the writable subset of a NiFi access policy.
type AccessPolicyComponent struct {
	ID         string      `json:"id,omitempty"`
	Resource   string      `json:"resource,omitempty"`
	Action     string      `json:"action,omitempty"`
	Users      []TenantRef `json:"users,omitempty"`
	UserGroups []TenantRef `json:"userGroups,omitempty"`
}

// AccessPolicyEntity is a NiFi access policy.
type AccessPolicyEntity struct {
	Revision  Revision              `json:"revision"`
	ID        string                `json:"id,omitempty"`
	Component AccessPolicyComponent `json:"component"`
}

// AccessPolicyClient manages NiFi access policies.
type AccessPolicyClient interface {
	// GetAccessPolicyForResource returns the policy for an action+resource, or a 404 error
	// (see IsNotFound) when no policy exists for that exact action+resource.
	GetAccessPolicyForResource(ctx context.Context, baseURI string, action, resource string) (*AccessPolicyEntity, error)
	GetAccessPolicy(ctx context.Context, baseURI string, id string) (*AccessPolicyEntity, error)
	CreateAccessPolicy(ctx context.Context, baseURI string, entity AccessPolicyEntity) (*AccessPolicyEntity, error)
	UpdateAccessPolicy(ctx context.Context, baseURI string, entity AccessPolicyEntity) (*AccessPolicyEntity, error)
	DeleteAccessPolicy(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// HTTPAccessPolicyClient is the HTTP implementation of AccessPolicyClient.
type HTTPAccessPolicyClient struct {
	Client *http.Client
}

func (c HTTPAccessPolicyClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPAccessPolicyClient) GetAccessPolicyForResource(ctx context.Context, baseURI string, action, resource string) (*AccessPolicyEntity, error) {
	// The resource path may contain slashes (e.g. /process-groups/{id}); they must stay as path
	// separators, so the resource is appended without escaping its slashes.
	apiPath := fmt.Sprintf("/policies/%s/%s", url.PathEscape(action), strings.TrimPrefix(resource, "/"))
	endpoint, err := apiURL(baseURI, apiPath)
	if err != nil {
		return nil, err
	}
	var response AccessPolicyEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPAccessPolicyClient) GetAccessPolicy(ctx context.Context, baseURI string, id string) (*AccessPolicyEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/policies/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response AccessPolicyEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPAccessPolicyClient) CreateAccessPolicy(ctx context.Context, baseURI string, entity AccessPolicyEntity) (*AccessPolicyEntity, error) {
	endpoint, err := apiURL(baseURI, "/policies")
	if err != nil {
		return nil, err
	}
	var response AccessPolicyEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPAccessPolicyClient) UpdateAccessPolicy(ctx context.Context, baseURI string, entity AccessPolicyEntity) (*AccessPolicyEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/policies/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response AccessPolicyEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPAccessPolicyClient) DeleteAccessPolicy(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/policies/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// AccessPolicyEntityID returns the stable id of an access policy entity.
func AccessPolicyEntityID(entity AccessPolicyEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}
