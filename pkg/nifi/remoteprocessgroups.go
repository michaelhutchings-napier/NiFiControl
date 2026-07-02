package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Remote process group entities model the NiFi remote-process-group API. A remote process group
// (RPG) is a canvas component: it is created under a parent process group at
// /process-groups/{parentId}/remote-process-groups and then addressed at
// /remote-process-groups/{id}. Its transmission state (TRANSMITTING/STOPPED) is changed through a
// dedicated run-status endpoint, not by a component PUT, and NiFi requires the RPG to be stopped
// before its configuration is updated or it is deleted.
//
// Note: targetUris is a single comma-separated String in the DTO (not a JSON array) even though
// the field name is plural, and proxyPassword is masked on read.

// RemoteProcessGroupComponent is the writable-plus-observed subset of a NiFi remote process group.
type RemoteProcessGroupComponent struct {
	ID                    string    `json:"id,omitempty"`
	ParentGroupID         string    `json:"parentGroupId,omitempty"`
	Name                  string    `json:"name,omitempty"`
	Comments              string    `json:"comments,omitempty"`
	TargetURIs            string    `json:"targetUris,omitempty"`
	TransportProtocol     string    `json:"transportProtocol,omitempty"`
	CommunicationsTimeout string    `json:"communicationsTimeout,omitempty"`
	YieldDuration         string    `json:"yieldDuration,omitempty"`
	LocalNetworkInterface string    `json:"localNetworkInterface,omitempty"`
	ProxyHost             string    `json:"proxyHost,omitempty"`
	ProxyPort             int32     `json:"proxyPort,omitempty"`
	ProxyUser             string    `json:"proxyUser,omitempty"`
	ProxyPassword         string    `json:"proxyPassword,omitempty"`
	Position              *Position `json:"position,omitempty"`
	// Observed (read-only) fields returned by NiFi.
	Transmitting    bool  `json:"transmitting,omitempty"`
	TargetSecure    bool  `json:"targetSecure,omitempty"`
	InputPortCount  int32 `json:"inputPortCount,omitempty"`
	OutputPortCount int32 `json:"outputPortCount,omitempty"`
}

// RemoteProcessGroupEntity is a NiFi remote process group.
type RemoteProcessGroupEntity struct {
	ID                           string                      `json:"id,omitempty"`
	Revision                     Revision                    `json:"revision"`
	Component                    RemoteProcessGroupComponent `json:"component"`
	DisconnectedNodeAcknowledged bool                        `json:"disconnectedNodeAcknowledged,omitempty"`
}

// RemoteProcessGroupRunStatusEntity changes an RPG's transmission state (TRANSMITTING/STOPPED).
type RemoteProcessGroupRunStatusEntity struct {
	Revision                     Revision `json:"revision"`
	State                        string   `json:"state"`
	DisconnectedNodeAcknowledged bool     `json:"disconnectedNodeAcknowledged,omitempty"`
}

// RemoteProcessGroupClient manages NiFi remote process groups.
type RemoteProcessGroupClient interface {
	GetRemoteProcessGroup(ctx context.Context, baseURI string, id string) (*RemoteProcessGroupEntity, error)
	CreateRemoteProcessGroup(ctx context.Context, baseURI string, parentID string, entity RemoteProcessGroupEntity) (*RemoteProcessGroupEntity, error)
	UpdateRemoteProcessGroup(ctx context.Context, baseURI string, entity RemoteProcessGroupEntity) (*RemoteProcessGroupEntity, error)
	// UpdateRemoteProcessGroupRunStatus starts or stops transmission (state TRANSMITTING or STOPPED).
	UpdateRemoteProcessGroupRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*RemoteProcessGroupEntity, error)
	DeleteRemoteProcessGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// HTTPRemoteProcessGroupClient is the HTTP implementation of RemoteProcessGroupClient.
type HTTPRemoteProcessGroupClient struct {
	Client *http.Client
}

func (c HTTPRemoteProcessGroupClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPRemoteProcessGroupClient) GetRemoteProcessGroup(ctx context.Context, baseURI string, id string) (*RemoteProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/remote-process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response RemoteProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRemoteProcessGroupClient) CreateRemoteProcessGroup(ctx context.Context, baseURI string, parentID string, entity RemoteProcessGroupEntity) (*RemoteProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/remote-process-groups", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}
	var response RemoteProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRemoteProcessGroupClient) UpdateRemoteProcessGroup(ctx context.Context, baseURI string, entity RemoteProcessGroupEntity) (*RemoteProcessGroupEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/remote-process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response RemoteProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRemoteProcessGroupClient) UpdateRemoteProcessGroupRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*RemoteProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/remote-process-groups/%s/run-status", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	body := RemoteProcessGroupRunStatusEntity{Revision: Revision{Version: revisionVersion}, State: state}
	var response RemoteProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRemoteProcessGroupClient) DeleteRemoteProcessGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/remote-process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// RemoteProcessGroupEntityID returns the stable id of a remote process group entity.
func RemoteProcessGroupEntityID(entity RemoteProcessGroupEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

// RemoteProcessGroupTransmissionState maps a desired transmitting bool to the NiFi run state.
func RemoteProcessGroupTransmissionState(transmitting bool) string {
	if transmitting {
		return "TRANSMITTING"
	}
	return "STOPPED"
}
