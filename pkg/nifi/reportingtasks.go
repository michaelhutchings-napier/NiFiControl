package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Reporting task entities model the NiFi reporting-task API. A reporting task is a
// controller-level component (it has no parent process group): it is created under
// /controller/reporting-tasks and then addressed at /reporting-tasks/{id}. Its run state
// (RUNNING/STOPPED) is changed through a dedicated run-status endpoint, not by a component PUT.

// ReportingTaskComponent is the writable subset of a NiFi reporting task.
type ReportingTaskComponent struct {
	ID                 string            `json:"id,omitempty"`
	Name               string            `json:"name,omitempty"`
	Type               string            `json:"type,omitempty"`
	Bundle             *Bundle           `json:"bundle,omitempty"`
	Comments           string            `json:"comments,omitempty"`
	Properties         map[string]string `json:"properties,omitempty"`
	SchedulingStrategy string            `json:"schedulingStrategy,omitempty"`
	SchedulingPeriod   string            `json:"schedulingPeriod,omitempty"`
	State              string            `json:"state,omitempty"`
	ValidationStatus   string            `json:"validationStatus,omitempty"`
}

// ReportingTaskEntity is a NiFi reporting task.
type ReportingTaskEntity struct {
	ID        string                 `json:"id,omitempty"`
	Revision  Revision               `json:"revision"`
	Component ReportingTaskComponent `json:"component"`
}

// ReportingTaskRunStatusEntity changes a reporting task's run state (RUNNING/STOPPED).
type ReportingTaskRunStatusEntity struct {
	Revision                     Revision `json:"revision"`
	State                        string   `json:"state"`
	DisconnectedNodeAcknowledged bool     `json:"disconnectedNodeAcknowledged,omitempty"`
}

// ReportingTaskClient manages NiFi reporting tasks.
type ReportingTaskClient interface {
	GetReportingTask(ctx context.Context, baseURI string, id string) (*ReportingTaskEntity, error)
	CreateReportingTask(ctx context.Context, baseURI string, entity ReportingTaskEntity) (*ReportingTaskEntity, error)
	UpdateReportingTask(ctx context.Context, baseURI string, entity ReportingTaskEntity) (*ReportingTaskEntity, error)
	// UpdateReportingTaskRunStatus starts or stops a reporting task (state RUNNING or STOPPED).
	UpdateReportingTaskRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*ReportingTaskEntity, error)
	DeleteReportingTask(ctx context.Context, baseURI string, id string, revisionVersion int64) error
}

// HTTPReportingTaskClient is the HTTP implementation of ReportingTaskClient.
type HTTPReportingTaskClient struct {
	Client *http.Client
}

func (c HTTPReportingTaskClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPReportingTaskClient) GetReportingTask(ctx context.Context, baseURI string, id string) (*ReportingTaskEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/reporting-tasks/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response ReportingTaskEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPReportingTaskClient) CreateReportingTask(ctx context.Context, baseURI string, entity ReportingTaskEntity) (*ReportingTaskEntity, error) {
	// Reporting tasks are controller-level components, created under /controller.
	endpoint, err := apiURL(baseURI, "/controller/reporting-tasks")
	if err != nil {
		return nil, err
	}
	var response ReportingTaskEntity
	if err := c.doJSON(ctx, http.MethodPost, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPReportingTaskClient) UpdateReportingTask(ctx context.Context, baseURI string, entity ReportingTaskEntity) (*ReportingTaskEntity, error) {
	id := entity.ID
	if id == "" {
		id = entity.Component.ID
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/reporting-tasks/%s", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	var response ReportingTaskEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, entity, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPReportingTaskClient) UpdateReportingTaskRunStatus(ctx context.Context, baseURI string, id string, revisionVersion int64, state string) (*ReportingTaskEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/reporting-tasks/%s/run-status", url.PathEscape(id)))
	if err != nil {
		return nil, err
	}
	body := ReportingTaskRunStatusEntity{Revision: Revision{Version: revisionVersion}, State: state}
	var response ReportingTaskEntity
	if err := c.doJSON(ctx, http.MethodPut, endpoint, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func (c HTTPReportingTaskClient) DeleteReportingTask(ctx context.Context, baseURI string, id string, revisionVersion int64) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/reporting-tasks/%s", url.PathEscape(id)))
	if err != nil {
		return err
	}
	endpoint += fmt.Sprintf("?version=%d", revisionVersion)
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}

// ReportingTaskEntityID returns the stable id of a reporting task entity.
func ReportingTaskEntityID(entity ReportingTaskEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

// ReportingTaskRunState maps the CRD's desired state to the NiFi reporting-task run state.
// A reporting task is either actively RUNNING or STOPPED.
func ReportingTaskRunState(enabled bool) string {
	if enabled {
		return "RUNNING"
	}
	return "STOPPED"
}
