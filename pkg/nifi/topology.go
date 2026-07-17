package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Connectable component types used on connection endpoints.
const (
	ConnectableProcessor  = "PROCESSOR"
	ConnectableInputPort  = "INPUT_PORT"
	ConnectableOutputPort = "OUTPUT_PORT"
	ConnectableFunnel     = "FUNNEL"

	RunStateRunning  = "RUNNING"
	RunStateStopped  = "STOPPED"
	RunStateEnabled  = "ENABLED"
	RunStateDisabled = "DISABLED"
)

// BlueGreenClient is the NiFi 2.x surface a transactional BlueGreen rollout needs beyond
// the process-group and snapshot clients: enumerating boundary connections and ports,
// inspecting and draining queues, toggling component run status, and enabling controller
// services on the candidate.
type BlueGreenClient interface {
	ListProcessGroupConnections(ctx context.Context, baseURI string, processGroupID string) ([]ConnectionEntity, error)
	ListProcessGroupInputPorts(ctx context.Context, baseURI string, processGroupID string) ([]PortEntity, error)
	ListProcessGroupOutputPorts(ctx context.Context, baseURI string, processGroupID string) ([]PortEntity, error)
	GetConnection(ctx context.Context, baseURI string, id string) (*ConnectionEntity, error)
	CreateConnection(ctx context.Context, baseURI string, parentID string, entity ConnectionEntity) (*ConnectionEntity, error)
	DeleteConnection(ctx context.Context, baseURI string, id string, revisionVersion int64) error
	ConnectionQueueCount(ctx context.Context, baseURI string, connectionID string) (int64, error)
	DropConnectionQueue(ctx context.Context, baseURI string, connectionID string) error
	SetComponentRunStatus(ctx context.Context, baseURI string, componentType string, id string, state string) error
	EnableControllerServices(ctx context.Context, baseURI string, processGroupID string) error
}

// HTTPBlueGreenClient implements BlueGreenClient against the NiFi REST API.
type HTTPBlueGreenClient struct {
	Client *http.Client
}

type connectionsResponse struct {
	Connections []ConnectionEntity `json:"connections"`
}

type inputPortsResponse struct {
	InputPorts []PortEntity `json:"inputPorts"`
}

type outputPortsResponse struct {
	OutputPorts []PortEntity `json:"outputPorts"`
}

type connectionStatusResponse struct {
	ConnectionStatus struct {
		AggregateSnapshot struct {
			FlowFilesQueued int64 `json:"flowFilesQueued"`
		} `json:"aggregateSnapshot"`
	} `json:"connectionStatus"`
}

type dropRequestEntity struct {
	DropRequest struct {
		ID       string `json:"id"`
		Finished bool   `json:"finished"`
		Failure  string `json:"failureReason"`
	} `json:"dropRequest"`
}

type activateControllerServicesEntity struct {
	ID                           string `json:"id"`
	State                        string `json:"state"`
	DisconnectedNodeAcknowledged bool   `json:"disconnectedNodeAcknowledged"`
}

type runStatusEntity struct {
	Revision                     Revision `json:"revision"`
	State                        string   `json:"state"`
	DisconnectedNodeAcknowledged bool     `json:"disconnectedNodeAcknowledged"`
}

func (c HTTPBlueGreenClient) doJSON(ctx context.Context, method, endpoint string, body, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPBlueGreenClient) ListProcessGroupConnections(ctx context.Context, baseURI string, processGroupID string) ([]ConnectionEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/connections", url.PathEscape(processGroupID)))
	if err != nil {
		return nil, err
	}
	var response connectionsResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.Connections, nil
}

func (c HTTPBlueGreenClient) ListProcessGroupInputPorts(ctx context.Context, baseURI string, processGroupID string) ([]PortEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/input-ports", url.PathEscape(processGroupID)))
	if err != nil {
		return nil, err
	}
	var response inputPortsResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.InputPorts, nil
}

func (c HTTPBlueGreenClient) ListProcessGroupOutputPorts(ctx context.Context, baseURI string, processGroupID string) ([]PortEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/output-ports", url.PathEscape(processGroupID)))
	if err != nil {
		return nil, err
	}
	var response outputPortsResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.OutputPorts, nil
}

func (c HTTPBlueGreenClient) GetConnection(ctx context.Context, baseURI string, id string) (*ConnectionEntity, error) {
	return HTTPConnectionClient{Client: c.Client}.GetConnection(ctx, baseURI, id)
}

func (c HTTPBlueGreenClient) CreateConnection(ctx context.Context, baseURI string, parentID string, entity ConnectionEntity) (*ConnectionEntity, error) {
	return HTTPConnectionClient{Client: c.Client}.CreateConnection(ctx, baseURI, parentID, entity)
}

func (c HTTPBlueGreenClient) DeleteConnection(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return HTTPConnectionClient{Client: c.Client}.DeleteConnection(ctx, baseURI, id, revisionVersion)
}

func (c HTTPBlueGreenClient) ConnectionQueueCount(ctx context.Context, baseURI string, connectionID string) (int64, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flow/connections/%s/status", url.PathEscape(connectionID)))
	if err != nil {
		return 0, err
	}
	var response connectionStatusResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return 0, err
	}
	return response.ConnectionStatus.AggregateSnapshot.FlowFilesQueued, nil
}

// DropConnectionQueue empties a connection's queue via a NiFi drop request, polling for
// completion. It is only used by the Drop on-drain-timeout policy.
func (c HTTPBlueGreenClient) DropConnectionQueue(ctx context.Context, baseURI string, connectionID string) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flowfile-queues/%s/drop-requests", url.PathEscape(connectionID)))
	if err != nil {
		return err
	}
	var created dropRequestEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, nil, &created); err != nil {
		return err
	}
	requestID := created.DropRequest.ID
	if requestID == "" {
		return fmt.Errorf("NiFi returned no drop request id for connection %s", connectionID)
	}
	statusURL, err := apiURL(baseURI, fmt.Sprintf("/flowfile-queues/%s/drop-requests/%s", url.PathEscape(connectionID), url.PathEscape(requestID)))
	if err != nil {
		return err
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		var status dropRequestEntity
		if err := c.doJSON(ctx, http.MethodGet, statusURL, nil, &status); err != nil {
			return err
		}
		if status.DropRequest.Failure != "" {
			_ = c.doJSON(ctx, http.MethodDelete, statusURL, nil, nil)
			return fmt.Errorf("NiFi drop request failed: %s", status.DropRequest.Failure)
		}
		if status.DropRequest.Finished {
			return c.doJSON(ctx, http.MethodDelete, statusURL, nil, nil)
		}
		if time.Now().After(deadline) {
			_ = c.doJSON(ctx, http.MethodDelete, statusURL, nil, nil)
			return fmt.Errorf("NiFi drop request for connection %s did not finish in time", connectionID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// SetComponentRunStatus stops or starts a single processor or port. Funnels (which cannot
// be stopped) and other types are rejected so callers fail safely rather than silently.
func (c HTTPBlueGreenClient) SetComponentRunStatus(ctx context.Context, baseURI string, componentType string, id string, state string) error {
	var path string
	switch strings.ToUpper(componentType) {
	case ConnectableProcessor:
		path = "/processors/%s"
	case ConnectableInputPort:
		path = "/input-ports/%s"
	case ConnectableOutputPort:
		path = "/output-ports/%s"
	default:
		return fmt.Errorf("cannot change run status of component type %q", componentType)
	}
	getURL, err := apiURL(baseURI, fmt.Sprintf(path, url.PathEscape(id)))
	if err != nil {
		return err
	}
	var current struct {
		Revision Revision `json:"revision"`
	}
	if err := c.doJSON(ctx, http.MethodGet, getURL, nil, &current); err != nil {
		return err
	}
	runURL, err := apiURL(baseURI, fmt.Sprintf(path+"/run-status", url.PathEscape(id)))
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodPut, runURL, runStatusEntity{Revision: current.Revision, State: state}, nil)
}

// EnableControllerServices enables every controller service in a process group.
func (c HTTPBlueGreenClient) EnableControllerServices(ctx context.Context, baseURI string, processGroupID string) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flow/process-groups/%s/controller-services", url.PathEscape(processGroupID)))
	if err != nil {
		return err
	}
	body := activateControllerServicesEntity{ID: processGroupID, State: RunStateEnabled}
	return c.doJSON(ctx, http.MethodPut, endpoint, body, nil)
}
