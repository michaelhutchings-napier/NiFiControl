package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Parameter provider entities model the NiFi parameter-provider API. A parameter provider is a
// controller-level component (it has no parent process group): it is created under
// /controller/parameter-providers and then addressed at /parameter-providers/{id}. Unlike a
// reporting task it has no run state — it is passive configuration that parameter contexts fetch
// from — so it is created, updated, and deleted directly with no start/stop step.

// ParameterProviderComponent is the writable subset of a NiFi parameter provider.
type ParameterProviderComponent struct {
	ID               string            `json:"id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Type             string            `json:"type,omitempty"`
	Bundle           *Bundle           `json:"bundle,omitempty"`
	Comments         string            `json:"comments,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
	ValidationStatus string            `json:"validationStatus,omitempty"`
}

// ParameterProviderEntity is a NiFi parameter provider.
type ParameterProviderEntity struct {
	ID        string                     `json:"id,omitempty"`
	Revision  Revision                   `json:"revision"`
	Component ParameterProviderComponent `json:"component"`
}

// ParameterProviderClient manages NiFi parameter providers.
type ParameterProviderClient interface {
	GetParameterProvider(ctx context.Context, baseURI string, id string) (*ParameterProviderEntity, error)
	CreateParameterProvider(ctx context.Context, baseURI string, entity ParameterProviderEntity) (*ParameterProviderEntity, error)
	UpdateParameterProvider(ctx context.Context, baseURI string, entity ParameterProviderEntity) (*ParameterProviderEntity, error)
	DeleteParameterProvider(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// HTTPParameterProviderClient is the HTTP implementation of ParameterProviderClient.
type HTTPParameterProviderClient struct {
	Client *http.Client
}

func (c HTTPParameterProviderClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPParameterProviderClient) GetParameterProvider(ctx context.Context, baseURI string, id string) (*ParameterProviderEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-providers/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response ParameterProviderEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterProviderClient) CreateParameterProvider(ctx context.Context, baseURI string, entity ParameterProviderEntity) (*ParameterProviderEntity, error) {
	// Parameter providers are controller-level components, created under /controller.
	endpoint, err := apiURL(baseURI, "/controller/parameter-providers")
	if err != nil {
		return nil, err
	}
	var response ParameterProviderEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterProviderClient) UpdateParameterProvider(ctx context.Context, baseURI string, entity ParameterProviderEntity) (*ParameterProviderEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-providers/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response ParameterProviderEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterProviderClient) DeleteParameterProvider(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-providers/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// ParameterProviderEntityID returns the stable id of a parameter provider entity.
func ParameterProviderEntityID(entity ParameterProviderEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}
