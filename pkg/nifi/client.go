package nifi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultTimeout = 10 * time.Second

type HTTPStatusError struct {
	StatusCode int
	Message    string
}

func (e *HTTPStatusError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("nifi api returned HTTP %d: %s", e.StatusCode, e.Message)
	}
	return fmt.Sprintf("nifi api returned HTTP %d", e.StatusCode)
}

func IsNotFound(err error) bool {
	var statusErr *HTTPStatusError
	return errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound
}

type ReachabilityChecker interface {
	CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error
}

type HTTPReachabilityChecker struct {
	Client *http.Client
}

type ParameterContextClient interface {
	ListParameterContexts(ctx context.Context, baseURI string) ([]ParameterContextEntity, error)
	GetParameterContext(ctx context.Context, baseURI string, id string) (*ParameterContextEntity, error)
	CreateParameterContext(ctx context.Context, baseURI string, entity ParameterContextEntity) (*ParameterContextEntity, error)
	DeleteParameterContext(ctx context.Context, baseURI string, id string, revisionVersion int64) error
	CreateParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, entity ParameterContextEntity) (*ParameterContextUpdateRequestEntity, error)
	GetParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, requestID string) (*ParameterContextUpdateRequestEntity, error)
}

type RegistryClientClient interface {
	GetRegistryClient(ctx context.Context, baseURI string, id string) (*RegistryClientEntity, error)
	CreateRegistryClient(ctx context.Context, baseURI string, entity RegistryClientEntity) (*RegistryClientEntity, error)
	UpdateRegistryClient(ctx context.Context, baseURI string, entity RegistryClientEntity) (*RegistryClientEntity, error)
	DeleteRegistryClient(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type ProcessGroupClient interface {
	GetProcessGroup(ctx context.Context, baseURI string, id string) (*ProcessGroupEntity, error)
	CreateProcessGroup(ctx context.Context, baseURI string, parentID string, entity ProcessGroupEntity) (*ProcessGroupEntity, error)
	UpdateProcessGroup(ctx context.Context, baseURI string, entity ProcessGroupEntity) (*ProcessGroupEntity, error)
	DeleteProcessGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type FlowSnapshotClient interface {
	ImportProcessGroup(ctx context.Context, baseURI string, parentID string, snapshot json.RawMessage) (*ProcessGroupEntity, error)
	CreateProcessGroupReplaceRequest(ctx context.Context, baseURI string, processGroupID string, revisionVersion int64, snapshot json.RawMessage) (*ProcessGroupReplaceRequestEntity, error)
	GetProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) (*ProcessGroupReplaceRequestEntity, error)
	DeleteProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) error
}

type FlowSnapshotReader interface {
	DownloadProcessGroup(ctx context.Context, baseURI string, processGroupID string) (json.RawMessage, error)
}

type ProcessGroupScheduler interface {
	ScheduleProcessGroup(ctx context.Context, baseURI string, processGroupID string, state string) error
}

type ControllerServiceClient interface {
	GetControllerService(ctx context.Context, baseURI string, id string) (*ControllerServiceEntity, error)
	CreateControllerService(ctx context.Context, baseURI string, parentID string, entity ControllerServiceEntity) (*ControllerServiceEntity, error)
	UpdateControllerService(ctx context.Context, baseURI string, entity ControllerServiceEntity) (*ControllerServiceEntity, error)
	DeleteControllerService(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type ProcessorClient interface {
	GetProcessor(ctx context.Context, baseURI string, id string) (*ProcessorEntity, error)
	CreateProcessor(ctx context.Context, baseURI string, parentID string, entity ProcessorEntity) (*ProcessorEntity, error)
	UpdateProcessor(ctx context.Context, baseURI string, entity ProcessorEntity) (*ProcessorEntity, error)
	DeleteProcessor(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type FunnelClient interface {
	GetFunnel(ctx context.Context, baseURI string, id string) (*FunnelEntity, error)
	CreateFunnel(ctx context.Context, baseURI string, parentID string, entity FunnelEntity) (*FunnelEntity, error)
	UpdateFunnel(ctx context.Context, baseURI string, entity FunnelEntity) (*FunnelEntity, error)
	DeleteFunnel(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type LabelClient interface {
	GetLabel(ctx context.Context, baseURI string, id string) (*LabelEntity, error)
	CreateLabel(ctx context.Context, baseURI string, parentID string, entity LabelEntity) (*LabelEntity, error)
	UpdateLabel(ctx context.Context, baseURI string, entity LabelEntity) (*LabelEntity, error)
	DeleteLabel(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type InputPortClient interface {
	GetInputPort(ctx context.Context, baseURI string, id string) (*PortEntity, error)
	CreateInputPort(ctx context.Context, baseURI string, parentID string, entity PortEntity) (*PortEntity, error)
	UpdateInputPort(ctx context.Context, baseURI string, entity PortEntity) (*PortEntity, error)
	UpdateInputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*PortEntity, error)
	DeleteInputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type OutputPortClient interface {
	GetOutputPort(ctx context.Context, baseURI string, id string) (*PortEntity, error)
	CreateOutputPort(ctx context.Context, baseURI string, parentID string, entity PortEntity) (*PortEntity, error)
	UpdateOutputPort(ctx context.Context, baseURI string, entity PortEntity) (*PortEntity, error)
	UpdateOutputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*PortEntity, error)
	DeleteOutputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type ConnectionClient interface {
	GetConnection(ctx context.Context, baseURI string, id string) (*ConnectionEntity, error)
	CreateConnection(ctx context.Context, baseURI string, parentID string, entity ConnectionEntity) (*ConnectionEntity, error)
	UpdateConnection(ctx context.Context, baseURI string, entity ConnectionEntity) (*ConnectionEntity, error)
	DeleteConnection(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

type HTTPParameterContextClient struct {
	Client *http.Client
}

type HTTPRegistryClientClient struct {
	Client *http.Client
}

type HTTPProcessGroupClient struct {
	Client *http.Client
}

type HTTPFlowSnapshotClient struct {
	Client *http.Client
}

type HTTPProcessGroupScheduler struct {
	Client *http.Client
}

type HTTPControllerServiceClient struct {
	Client *http.Client
}

type HTTPProcessorClient struct {
	Client *http.Client
}

type HTTPFunnelClient struct {
	Client *http.Client
}

type HTTPLabelClient struct {
	Client *http.Client
}

type HTTPInputPortClient struct {
	Client *http.Client
}

type HTTPOutputPortClient struct {
	Client *http.Client
}

type HTTPConnectionClient struct {
	Client *http.Client
}

type Revision struct {
	// Version must be serialized even when zero: NiFi requires an explicit "version": 0
	// when creating a new component, so omitempty must not drop it.
	Version int64 `json:"version"`
}

type Position struct {
	// X and Y must be serialized even when zero: NiFi rejects a position that omits a
	// coordinate ("The x and y coordinate of the proposed position must be specified"),
	// so omitempty must not drop a 0 value. A component with no position uses a nil
	// *Position, which is omitted entirely.
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type ComponentReference struct {
	ID string `json:"id,omitempty"`
}

type Bundle struct {
	Group    string `json:"group,omitempty"`
	Artifact string `json:"artifact,omitempty"`
	Version  string `json:"version,omitempty"`
}

type ParameterContextEntity struct {
	ID        string                    `json:"id,omitempty"`
	Revision  Revision                  `json:"revision,omitempty"`
	Component ParameterContextComponent `json:"component,omitempty"`
}

type ParameterContextComponent struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	Description string            `json:"description,omitempty"`
	Parameters  []ParameterEntity `json:"parameters,omitempty"`
}

type ParameterEntity struct {
	Parameter Parameter `json:"parameter,omitempty"`
}

type Parameter struct {
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Sensitive   bool    `json:"sensitive,omitempty"`
	Value       *string `json:"value,omitempty"`
}

type ParameterContextsResponse struct {
	ParameterContexts []ParameterContextEntity `json:"parameterContexts"`
}

type ParameterContextUpdateRequestEntity struct {
	Request ParameterContextUpdateRequest `json:"request,omitempty"`
}

type ParameterContextUpdateRequest struct {
	RequestID        string `json:"requestId,omitempty"`
	URI              string `json:"uri,omitempty"`
	SubmissionTime   string `json:"submissionTime,omitempty"`
	LastUpdated      string `json:"lastUpdated,omitempty"`
	Complete         bool   `json:"complete,omitempty"`
	FailureReason    string `json:"failureReason,omitempty"`
	PercentCompleted int32  `json:"percentCompleted,omitempty"`
	State            string `json:"state,omitempty"`
}

type RegistryClientEntity struct {
	ID        string                  `json:"id,omitempty"`
	Revision  Revision                `json:"revision,omitempty"`
	Component RegistryClientComponent `json:"component,omitempty"`
}

type RegistryClientComponent struct {
	ID          string            `json:"id,omitempty"`
	Name        string            `json:"name,omitempty"`
	Type        string            `json:"type,omitempty"`
	Description string            `json:"description,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
}

type ProcessGroupEntity struct {
	ID        string                `json:"id,omitempty"`
	Revision  Revision              `json:"revision,omitempty"`
	Component ProcessGroupComponent `json:"component,omitempty"`
	// Aggregate component counts reported by NiFi for the group (recursive). Used by
	// BlueGreen readiness gating.
	RunningCount  int32 `json:"runningCount,omitempty"`
	StoppedCount  int32 `json:"stoppedCount,omitempty"`
	InvalidCount  int32 `json:"invalidCount,omitempty"`
	DisabledCount int32 `json:"disabledCount,omitempty"`
}

type ProcessGroupComponent struct {
	ID               string              `json:"id,omitempty"`
	ParentGroupID    string              `json:"parentGroupId,omitempty"`
	Name             string              `json:"name,omitempty"`
	Comments         string              `json:"comments,omitempty"`
	Position         *Position           `json:"position,omitempty"`
	ParameterContext *ComponentReference `json:"parameterContext,omitempty"`
}

type ProcessGroupImportEntity struct {
	ProcessGroupRevision         *Revision       `json:"processGroupRevision,omitempty"`
	DisconnectedNodeAcknowledged bool            `json:"disconnectedNodeAcknowledged,omitempty"`
	VersionedFlowSnapshot        json.RawMessage `json:"versionedFlowSnapshot"`
}

type ProcessGroupUploadEntity struct {
	GroupName                    string          `json:"groupName,omitempty"`
	DisconnectedNodeAcknowledged bool            `json:"disconnectedNodeAcknowledged,omitempty"`
	FlowSnapshot                 json.RawMessage `json:"flowSnapshot"`
	Revision                     Revision        `json:"revisionDTO"`
}

type ProcessGroupReplaceRequestEntity struct {
	ProcessGroupRevision  *Revision                  `json:"processGroupRevision,omitempty"`
	Request               ProcessGroupReplaceRequest `json:"request,omitempty"`
	VersionedFlowSnapshot json.RawMessage            `json:"versionedFlowSnapshot,omitempty"`
}

type ProcessGroupReplaceRequest struct {
	RequestID        string `json:"requestId,omitempty"`
	ProcessGroupID   string `json:"processGroupId,omitempty"`
	URI              string `json:"uri,omitempty"`
	LastUpdated      string `json:"lastUpdated,omitempty"`
	Complete         bool   `json:"complete,omitempty"`
	FailureReason    string `json:"failureReason,omitempty"`
	PercentCompleted int32  `json:"percentCompleted,omitempty"`
	State            string `json:"state,omitempty"`
}

type ScheduleComponentsEntity struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

type ControllerServiceEntity struct {
	ID        string                     `json:"id,omitempty"`
	Revision  Revision                   `json:"revision,omitempty"`
	Component ControllerServiceComponent `json:"component,omitempty"`
}

type ControllerServiceComponent struct {
	ID               string            `json:"id,omitempty"`
	ParentGroupID    string            `json:"parentGroupId,omitempty"`
	Name             string            `json:"name,omitempty"`
	Type             string            `json:"type,omitempty"`
	Bundle           *Bundle           `json:"bundle,omitempty"`
	Properties       map[string]string `json:"properties,omitempty"`
	State            string            `json:"state,omitempty"`
	ValidationStatus string            `json:"validationStatus,omitempty"`
}

type ProcessorEntity struct {
	ID        string             `json:"id,omitempty"`
	Revision  Revision           `json:"revision,omitempty"`
	Component ProcessorComponent `json:"component,omitempty"`
}

type ProcessorComponent struct {
	ID               string          `json:"id,omitempty"`
	ParentGroupID    string          `json:"parentGroupId,omitempty"`
	Name             string          `json:"name,omitempty"`
	Type             string          `json:"type,omitempty"`
	Bundle           *Bundle         `json:"bundle,omitempty"`
	Position         *Position       `json:"position,omitempty"`
	State            string          `json:"state,omitempty"`
	ValidationStatus string          `json:"validationStatus,omitempty"`
	Config           ProcessorConfig `json:"config,omitempty"`
}

type ProcessorConfig struct {
	Properties                       map[string]string `json:"properties,omitempty"`
	SchedulingStrategy               string            `json:"schedulingStrategy,omitempty"`
	SchedulingPeriod                 string            `json:"schedulingPeriod,omitempty"`
	ConcurrentlySchedulableTaskCount int32             `json:"concurrentlySchedulableTaskCount,omitempty"`
	AutoTerminatedRelationships      []string          `json:"autoTerminatedRelationships,omitempty"`
}

type FunnelEntity struct {
	ID        string          `json:"id,omitempty"`
	Revision  Revision        `json:"revision,omitempty"`
	Component FunnelComponent `json:"component,omitempty"`
}

type FunnelComponent struct {
	ID            string    `json:"id,omitempty"`
	ParentGroupID string    `json:"parentGroupId,omitempty"`
	Position      *Position `json:"position,omitempty"`
}

type LabelEntity struct {
	ID        string         `json:"id,omitempty"`
	Revision  Revision       `json:"revision,omitempty"`
	Component LabelComponent `json:"component,omitempty"`
}

type LabelComponent struct {
	ID            string            `json:"id,omitempty"`
	ParentGroupID string            `json:"parentGroupId,omitempty"`
	Label         string            `json:"label,omitempty"`
	Position      *Position         `json:"position,omitempty"`
	Width         int32             `json:"width,omitempty"`
	Height        int32             `json:"height,omitempty"`
	Style         map[string]string `json:"style,omitempty"`
}

type PortEntity struct {
	ID        string        `json:"id,omitempty"`
	Revision  Revision      `json:"revision,omitempty"`
	Component PortComponent `json:"component,omitempty"`
}

type PortComponent struct {
	ID                               string    `json:"id,omitempty"`
	ParentGroupID                    string    `json:"parentGroupId,omitempty"`
	Name                             string    `json:"name,omitempty"`
	Position                         *Position `json:"position,omitempty"`
	State                            string    `json:"state,omitempty"`
	ConcurrentlySchedulableTaskCount int32     `json:"concurrentlySchedulableTaskCount,omitempty"`
	ValidationStatus                 string    `json:"validationStatus,omitempty"`
	ValidationErrors                 []string  `json:"validationErrors,omitempty"`
}

// PortRunStatusEntity changes a port's run state (RUNNING/STOPPED/DISABLED).
type PortRunStatusEntity struct {
	Revision                     Revision `json:"revision"`
	State                        string   `json:"state"`
	DisconnectedNodeAcknowledged bool     `json:"disconnectedNodeAcknowledged,omitempty"`
}

type ConnectionEntity struct {
	ID        string              `json:"id,omitempty"`
	Revision  Revision            `json:"revision,omitempty"`
	Component ConnectionComponent `json:"component,omitempty"`
}

type ConnectionComponent struct {
	ID                    string      `json:"id,omitempty"`
	ParentGroupID         string      `json:"parentGroupId,omitempty"`
	Source                Connectable `json:"source,omitempty"`
	Destination           Connectable `json:"destination,omitempty"`
	SelectedRelationships []string    `json:"selectedRelationships,omitempty"`
	// BackPressureObjectThreshold is a count; NiFi serializes it as a JSON number.
	BackPressureObjectThreshold   int64      `json:"backPressureObjectThreshold,omitempty"`
	BackPressureDataSizeThreshold string     `json:"backPressureDataSizeThreshold,omitempty"`
	FlowFileExpiration            string     `json:"flowFileExpiration,omitempty"`
	Prioritizers                  []string   `json:"prioritizers,omitempty"`
	Bends                         []Position `json:"bends,omitempty"`
	LoadBalanceStrategy           string     `json:"loadBalanceStrategy,omitempty"`
	LoadBalancePartitionAttribute string     `json:"loadBalancePartitionAttribute,omitempty"`
}

type Connectable struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type,omitempty"`
	GroupID string `json:"groupId,omitempty"`
}

func (c HTTPReachabilityChecker) CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	endpoint, err := apiURL(baseURI, "/flow/about")
	if err != nil {
		return err
	}

	client := clientForEndpoint(c.Client, endpoint)
	if c.Client == nil {
		configured := *client
		configured.Timeout = timeout
		client = &configured
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return fmt.Errorf("nifi api returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func flowAboutURL(baseURI string) (string, error) {
	return apiURL(baseURI, "/flow/about")
}

func (c HTTPParameterContextClient) ListParameterContexts(ctx context.Context, baseURI string) ([]ParameterContextEntity, error) {
	endpoint, err := apiURL(baseURI, "/flow/parameter-contexts")
	if err != nil {
		return nil, err
	}

	var response ParameterContextsResponse
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.ParameterContexts, nil
}

func (c HTTPParameterContextClient) GetParameterContext(ctx context.Context, baseURI string, id string) (*ParameterContextEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-contexts/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	endpoint += "?includeInheritedParameters=false"

	var response ParameterContextEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterContextClient) CreateParameterContext(ctx context.Context, baseURI string, entity ParameterContextEntity) (*ParameterContextEntity, error) {
	endpoint, err := apiURL(baseURI, "/parameter-contexts")
	if err != nil {
		return nil, err
	}

	var response ParameterContextEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterContextClient) DeleteParameterContext(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-contexts/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPParameterContextClient) CreateParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, entity ParameterContextEntity) (*ParameterContextUpdateRequestEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-contexts/%s/update-requests", url.PathEscape(contextID)))
	if err != nil {
		return nil, err
	}

	var response ParameterContextUpdateRequestEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPParameterContextClient) GetParameterContextUpdateRequest(ctx context.Context, baseURI string, contextID string, requestID string) (*ParameterContextUpdateRequestEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/parameter-contexts/%s/update-requests/%s", url.PathEscape(contextID), url.PathEscape(requestID)))
	if err != nil {
		return nil, err
	}

	var response ParameterContextUpdateRequestEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRegistryClientClient) GetRegistryClient(ctx context.Context, baseURI string, id string) (*RegistryClientEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller/registry-clients/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response RegistryClientEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRegistryClientClient) CreateRegistryClient(ctx context.Context, baseURI string, entity RegistryClientEntity) (*RegistryClientEntity, error) {
	endpoint, err := apiURL(baseURI, "/controller/registry-clients")
	if err != nil {
		return nil, err
	}

	var response RegistryClientEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRegistryClientClient) UpdateRegistryClient(ctx context.Context, baseURI string, entity RegistryClientEntity) (*RegistryClientEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller/registry-clients/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response RegistryClientEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPRegistryClientClient) DeleteRegistryClient(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller/registry-clients/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPProcessGroupClient) GetProcessGroup(ctx context.Context, baseURI string, id string) (*ProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessGroupClient) CreateProcessGroup(ctx context.Context, baseURI string, parentID string, entity ProcessGroupEntity) (*ProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/process-groups", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response ProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessGroupClient) UpdateProcessGroup(ctx context.Context, baseURI string, entity ProcessGroupEntity) (*ProcessGroupEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessGroupClient) DeleteProcessGroup(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPFlowSnapshotClient) ImportProcessGroup(ctx context.Context, baseURI string, parentID string, snapshot json.RawMessage) (*ProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/process-groups/import", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}
	var descriptor struct {
		FlowContents struct {
			Name string `json:"name"`
		} `json:"flowContents"`
	}
	if err := json.Unmarshal(snapshot, &descriptor); err != nil {
		return nil, fmt.Errorf("decode flow snapshot for import: %w", err)
	}
	body := ProcessGroupUploadEntity{GroupName: descriptor.FlowContents.Name, FlowSnapshot: snapshot, Revision: Revision{Version: 0}}
	var response ProcessGroupEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFlowSnapshotClient) CreateProcessGroupReplaceRequest(ctx context.Context, baseURI string, processGroupID string, revisionVersion int64, snapshot json.RawMessage) (*ProcessGroupReplaceRequestEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/replace-requests", url.PathEscape(processGroupID)))
	if err != nil {
		return nil, err
	}
	body := ProcessGroupImportEntity{
		ProcessGroupRevision:  &Revision{Version: revisionVersion},
		VersionedFlowSnapshot: snapshot,
	}
	var response ProcessGroupReplaceRequestEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFlowSnapshotClient) GetProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) (*ProcessGroupReplaceRequestEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/replace-requests/%s", url.PathEscape(requestID)))
	if err != nil {
		return nil, err
	}
	var response ProcessGroupReplaceRequestEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFlowSnapshotClient) DeleteProcessGroupReplaceRequest(ctx context.Context, baseURI string, requestID string) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/replace-requests/%s", url.PathEscape(requestID)))
	if err != nil {
		return err
	}
	err = c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func (c HTTPFlowSnapshotClient) DownloadProcessGroup(ctx context.Context, baseURI string, processGroupID string) (json.RawMessage, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/download", url.PathEscape(processGroupID)))
	if err != nil {
		return nil, err
	}
	var snapshot json.RawMessage
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (c HTTPProcessGroupScheduler) ScheduleProcessGroup(ctx context.Context, baseURI string, processGroupID string, state string) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flow/process-groups/%s", url.PathEscape(processGroupID)))
	if err != nil {
		return err
	}
	return doJSON(ctx, c.Client, http.MethodPut, endpoint, ScheduleComponentsEntity{ID: processGroupID, State: state}, nil)
}

func (c HTTPControllerServiceClient) GetControllerService(ctx context.Context, baseURI string, id string) (*ControllerServiceEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller-services/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ControllerServiceEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPControllerServiceClient) CreateControllerService(ctx context.Context, baseURI string, parentID string, entity ControllerServiceEntity) (*ControllerServiceEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/controller-services", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response ControllerServiceEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPControllerServiceClient) UpdateControllerService(ctx context.Context, baseURI string, entity ControllerServiceEntity) (*ControllerServiceEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller-services/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ControllerServiceEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPControllerServiceClient) DeleteControllerService(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller-services/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPProcessorClient) GetProcessor(ctx context.Context, baseURI string, id string) (*ProcessorEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/processors/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ProcessorEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessorClient) CreateProcessor(ctx context.Context, baseURI string, parentID string, entity ProcessorEntity) (*ProcessorEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/processors", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response ProcessorEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessorClient) UpdateProcessor(ctx context.Context, baseURI string, entity ProcessorEntity) (*ProcessorEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/processors/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ProcessorEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPProcessorClient) DeleteProcessor(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/processors/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPFunnelClient) GetFunnel(ctx context.Context, baseURI string, id string) (*FunnelEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/funnels/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response FunnelEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFunnelClient) CreateFunnel(ctx context.Context, baseURI string, parentID string, entity FunnelEntity) (*FunnelEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/funnels", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response FunnelEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFunnelClient) UpdateFunnel(ctx context.Context, baseURI string, entity FunnelEntity) (*FunnelEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/funnels/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response FunnelEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPFunnelClient) DeleteFunnel(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/funnels/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPLabelClient) GetLabel(ctx context.Context, baseURI string, id string) (*LabelEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/labels/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response LabelEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPLabelClient) CreateLabel(ctx context.Context, baseURI string, parentID string, entity LabelEntity) (*LabelEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/labels", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response LabelEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPLabelClient) UpdateLabel(ctx context.Context, baseURI string, entity LabelEntity) (*LabelEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/labels/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response LabelEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPLabelClient) DeleteLabel(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/labels/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPInputPortClient) GetInputPort(ctx context.Context, baseURI string, id string) (*PortEntity, error) {
	return c.getPort(ctx, baseURI, "/input-ports/%s", id)
}

func (c HTTPInputPortClient) CreateInputPort(ctx context.Context, baseURI string, parentID string, entity PortEntity) (*PortEntity, error) {
	return c.createPort(ctx, baseURI, "/process-groups/%s/input-ports", parentID, entity)
}

func (c HTTPInputPortClient) UpdateInputPort(ctx context.Context, baseURI string, entity PortEntity) (*PortEntity, error) {
	return c.updatePort(ctx, baseURI, "/input-ports/%s", entity)
}

func (c HTTPInputPortClient) DeleteInputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return c.deletePort(ctx, baseURI, "/input-ports/%s", id, revisionVersion)
}

func (c HTTPInputPortClient) UpdateInputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*PortEntity, error) {
	return c.runStatusPort(ctx, baseURI, "/input-ports/%s/run-status", id, revisionVersion, state)
}

// runStatusPort changes a port's run state (RUNNING/STOPPED/DISABLED) through the dedicated
// run-status endpoint. NiFi rejects run-state changes made through a component PUT.
func (c HTTPInputPortClient) runStatusPort(ctx context.Context, baseURI string, pathFormat string, id string, revisionVersion int64, state string) (*PortEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf(pathFormat, url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	body := PortRunStatusEntity{Revision: Revision{Version: revisionVersion}, State: state}
	var response PortEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPOutputPortClient) GetOutputPort(ctx context.Context, baseURI string, id string) (*PortEntity, error) {
	return c.getPort(ctx, baseURI, "/output-ports/%s", id)
}

func (c HTTPOutputPortClient) CreateOutputPort(ctx context.Context, baseURI string, parentID string, entity PortEntity) (*PortEntity, error) {
	return c.createPort(ctx, baseURI, "/process-groups/%s/output-ports", parentID, entity)
}

func (c HTTPOutputPortClient) UpdateOutputPort(ctx context.Context, baseURI string, entity PortEntity) (*PortEntity, error) {
	return c.updatePort(ctx, baseURI, "/output-ports/%s", entity)
}

func (c HTTPOutputPortClient) DeleteOutputPort(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	return c.deletePort(ctx, baseURI, "/output-ports/%s", id, revisionVersion)
}

func (c HTTPOutputPortClient) UpdateOutputPortRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*PortEntity, error) {
	return (HTTPInputPortClient{Client: c.Client}).runStatusPort(ctx, baseURI, "/output-ports/%s/run-status", id, revisionVersion, state)
}

func (c HTTPConnectionClient) GetConnection(ctx context.Context, baseURI string, id string) (*ConnectionEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/connections/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ConnectionEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPConnectionClient) CreateConnection(ctx context.Context, baseURI string, parentID string, entity ConnectionEntity) (*ConnectionEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/connections", url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response ConnectionEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPConnectionClient) UpdateConnection(ctx context.Context, baseURI string, entity ConnectionEntity) (*ConnectionEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/connections/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response ConnectionEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPConnectionClient) DeleteConnection(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/connections/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPInputPortClient) getPort(ctx context.Context, baseURI string, pathFormat string, id string) (*PortEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf(pathFormat, url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response PortEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPInputPortClient) createPort(ctx context.Context, baseURI string, pathFormat string, parentID string, entity PortEntity) (*PortEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf(pathFormat, url.PathEscape(parentID)))
	if err != nil {
		return nil, err
	}

	var response PortEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPInputPortClient) updatePort(ctx context.Context, baseURI string, pathFormat string, entity PortEntity) (*PortEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf(pathFormat, url.PathEscape(id)))
	if err != nil {
		return nil, err
	}

	var response PortEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPInputPortClient) deletePort(ctx context.Context, baseURI string, pathFormat string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf(pathFormat, url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)

	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

func (c HTTPOutputPortClient) getPort(ctx context.Context, baseURI string, pathFormat string, id string) (*PortEntity, error) {
	return (HTTPInputPortClient{Client: c.Client}).getPort(ctx, baseURI, pathFormat, id)
}

func (c HTTPOutputPortClient) createPort(ctx context.Context, baseURI string, pathFormat string, parentID string, entity PortEntity) (*PortEntity, error) {
	return (HTTPInputPortClient{Client: c.Client}).createPort(ctx, baseURI, pathFormat, parentID, entity)
}

func (c HTTPOutputPortClient) updatePort(ctx context.Context, baseURI string, pathFormat string, entity PortEntity) (*PortEntity, error) {
	return (HTTPInputPortClient{Client: c.Client}).updatePort(ctx, baseURI, pathFormat, entity)
}

func (c HTTPOutputPortClient) deletePort(ctx context.Context, baseURI string, pathFormat string, id string, revisionVersion int64) error {
	return (HTTPInputPortClient{Client: c.Client}).deletePort(ctx, baseURI, pathFormat, id, revisionVersion)
}

func (c HTTPParameterContextClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPRegistryClientClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPProcessGroupClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPFlowSnapshotClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPControllerServiceClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPProcessorClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPFunnelClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPLabelClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPInputPortClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPConnectionClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func apiURL(baseURI string, apiPath string) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURI, "/"))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("nifi api uri must include scheme and host")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/nifi-api/" + strings.TrimLeft(apiPath, "/")
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func doJSON(ctx context.Context, client *http.Client, method, endpoint string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	client = clientForEndpoint(client, endpoint)

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &HTTPStatusError{StatusCode: resp.StatusCode, Message: strings.TrimSpace(string(message))}
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
