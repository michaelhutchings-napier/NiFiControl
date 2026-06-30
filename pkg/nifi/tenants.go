package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Tenant entities model the NiFi /tenants API (users and user groups). Tenants exist only on a
// secured NiFi with a managed authorizer; an insecure NiFi rejects these endpoints.

// TenantRefComponent carries the readable fields of a referenced tenant.
type TenantRefComponent struct {
	ID       string `json:"id,omitempty"`
	Identity string `json:"identity,omitempty"`
}

// TenantRef references a tenant (user or group). Writes need only the id; reads populate the
// component.
type TenantRef struct {
	ID        string              `json:"id,omitempty"`
	Component *TenantRefComponent `json:"component,omitempty"`
}

// UserComponent is the writable subset of a NiFi user.
type UserComponent struct {
	ID       string `json:"id,omitempty"`
	Identity string `json:"identity,omitempty"`
}

// UserEntity is a NiFi user tenant.
type UserEntity struct {
	Revision  Revision      `json:"revision"`
	ID        string        `json:"id,omitempty"`
	Component UserComponent `json:"component"`
}

// UsersEntity is the response of GET /tenants/users.
type UsersEntity struct {
	Users []UserEntity `json:"users"`
}

// UserGroupComponent is the writable subset of a NiFi user group. Users is the set of member
// tenant references.
type UserGroupComponent struct {
	ID       string      `json:"id,omitempty"`
	Identity string      `json:"identity,omitempty"`
	Users    []TenantRef `json:"users,omitempty"`
}

// UserGroupEntity is a NiFi user-group tenant.
type UserGroupEntity struct {
	Revision  Revision           `json:"revision"`
	ID        string             `json:"id,omitempty"`
	Component UserGroupComponent `json:"component"`
}

// UserGroupsEntity is the response of GET /tenants/user-groups.
type UserGroupsEntity struct {
	UserGroups []UserGroupEntity `json:"userGroups"`
}

// UserClient manages NiFi user tenants.
type UserClient interface {
	ListUsers(ctx context.Context, baseURI string) ([]UserEntity, error)
	GetUser(ctx context.Context, baseURI string, id string) (*UserEntity, error)
	CreateUser(ctx context.Context, baseURI string, entity UserEntity) (*UserEntity, error)
	UpdateUser(ctx context.Context, baseURI string, entity UserEntity) (*UserEntity, error)
	DeleteUser(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// UserGroupClient manages NiFi user-group tenants.
type UserGroupClient interface {
	ListUserGroups(ctx context.Context, baseURI string) ([]UserGroupEntity, error)
	GetUserGroup(ctx context.Context, baseURI string, id string) (*UserGroupEntity, error)
	CreateUserGroup(ctx context.Context, baseURI string, entity UserGroupEntity) (*UserGroupEntity, error)
	UpdateUserGroup(ctx context.Context, baseURI string, entity UserGroupEntity) (*UserGroupEntity, error)
	DeleteUserGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// HTTPUserClient is the HTTP implementation of UserClient.
type HTTPUserClient struct {
	Client *http.Client
}

func (c HTTPUserClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPUserClient) ListUsers(ctx context.Context, baseURI string) ([]UserEntity, error) {
	endpoint, err := apiURL(baseURI, "/tenants/users")
	if err != nil {
		return nil, err
	}
	var response UsersEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.Users, nil
}

func (c HTTPUserClient) GetUser(ctx context.Context, baseURI string, id string) (*UserEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/users/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response UserEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserClient) CreateUser(ctx context.Context, baseURI string, entity UserEntity) (*UserEntity, error) {
	endpoint, err := apiURL(baseURI, "/tenants/users")
	if err != nil {
		return nil, err
	}
	var response UserEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserClient) UpdateUser(ctx context.Context, baseURI string, entity UserEntity) (*UserEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/users/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response UserEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserClient) DeleteUser(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/users/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// HTTPUserGroupClient is the HTTP implementation of UserGroupClient.
type HTTPUserGroupClient struct {
	Client *http.Client
}

func (c HTTPUserGroupClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPUserGroupClient) ListUserGroups(ctx context.Context, baseURI string) ([]UserGroupEntity, error) {
	endpoint, err := apiURL(baseURI, "/tenants/user-groups")
	if err != nil {
		return nil, err
	}
	var response UserGroupsEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.UserGroups, nil
}

func (c HTTPUserGroupClient) GetUserGroup(ctx context.Context, baseURI string, id string) (*UserGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/user-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response UserGroupEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserGroupClient) CreateUserGroup(ctx context.Context, baseURI string, entity UserGroupEntity) (*UserGroupEntity, error) {
	endpoint, err := apiURL(baseURI, "/tenants/user-groups")
	if err != nil {
		return nil, err
	}
	var response UserGroupEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserGroupClient) UpdateUserGroup(ctx context.Context, baseURI string, entity UserGroupEntity) (*UserGroupEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/user-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response UserGroupEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPUserGroupClient) DeleteUserGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/tenants/user-groups/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// UserEntityID returns the stable id of a user entity.
func UserEntityID(entity UserEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

// UserGroupEntityID returns the stable id of a user-group entity.
func UserGroupEntityID(entity UserGroupEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}
