package nifi

import (
	"context"
	"net/http"
)

// ControllerConfigurationEntity is the NiFi /controller/config resource: the cluster-wide
// controller settings plus the revision required to update them.
type ControllerConfigurationEntity struct {
	Revision  Revision                   `json:"revision,omitempty"`
	Component ControllerConfigurationDTO `json:"component"`
}

// ControllerConfigurationDTO carries the controller-level settings. Only the fields the
// operator manages are modelled; NiFi preserves the rest on update. NiFi 2.0 removed
// event-driven scheduling, so maxTimerDrivenThreadCount is the sole thread-pool knob.
type ControllerConfigurationDTO struct {
	MaxTimerDrivenThreadCount *int32 `json:"maxTimerDrivenThreadCount,omitempty"`
}

// ControllerConfigClient reads and updates the cluster-wide controller configuration.
type ControllerConfigClient interface {
	GetControllerConfig(ctx context.Context, baseURI string) (*ControllerConfigurationEntity, error)
	UpdateControllerConfig(ctx context.Context, baseURI string, entity ControllerConfigurationEntity) (*ControllerConfigurationEntity, error)
}

// HTTPControllerConfigClient is the HTTP implementation of ControllerConfigClient. A nil
// Client falls back to the per-endpoint client registered by RegisterHTTPClient.
type HTTPControllerConfigClient struct {
	Client *http.Client
}

func (c HTTPControllerConfigClient) GetControllerConfig(ctx context.Context, baseURI string) (*ControllerConfigurationEntity, error) {
	endpoint, err := apiURL(baseURI, "/controller/config")
	if err != nil {
		return nil, err
	}
	var response ControllerConfigurationEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPControllerConfigClient) UpdateControllerConfig(ctx context.Context, baseURI string, entity ControllerConfigurationEntity) (*ControllerConfigurationEntity, error) {
	endpoint, err := apiURL(baseURI, "/controller/config")
	if err != nil {
		return nil, err
	}
	var response ControllerConfigurationEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPControllerConfigClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}
