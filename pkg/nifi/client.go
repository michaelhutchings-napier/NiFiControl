package nifi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const defaultTimeout = 10 * time.Second

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

type HTTPParameterContextClient struct {
	Client *http.Client
}

type Revision struct {
	Version int64 `json:"version,omitempty"`
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

func (c HTTPReachabilityChecker) CheckReachable(ctx context.Context, baseURI string, timeout time.Duration) error {
	if timeout == 0 {
		timeout = defaultTimeout
	}

	endpoint, err := apiURL(baseURI, "/flow/about")
	if err != nil {
		return err
	}

	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: timeout}
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

func (c HTTPParameterContextClient) doJSON(ctx context.Context, method, endpoint string, body any, out any) error {
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

	client := c.Client
	if client == nil {
		client = &http.Client{Timeout: defaultTimeout}
	}

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		message, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		if len(message) > 0 {
			return fmt.Errorf("nifi api returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(message)))
		}
		return fmt.Errorf("nifi api returned HTTP %d", resp.StatusCode)
	}
	if out == nil || resp.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
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
