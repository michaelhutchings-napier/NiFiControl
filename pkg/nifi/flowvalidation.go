package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// Component validation status values reported by NiFi.
const (
	ValidationStatusValid      = "VALID"
	ValidationStatusInvalid    = "INVALID"
	ValidationStatusValidating = "VALIDATING"
)

// Component kinds reported by a validation inspection.
const (
	ValidationKindProcessor         = "Processor"
	ValidationKindControllerService = "ControllerService"
)

// InvalidComponent identifies a single processor or controller service whose validation
// status is INVALID, along with the validation errors NiFi reported for it.
type InvalidComponent struct {
	Kind             string
	ID               string
	Name             string
	Type             string
	ProcessGroupID   string
	ValidationErrors []string
}

// FlowValidationReport summarises the validation state of a process group subtree.
type FlowValidationReport struct {
	// Invalid lists every INVALID processor and controller service found.
	Invalid []InvalidComponent
	// ValidatingCount is the number of components still validating (not yet settled). A caller
	// should wait for this to reach zero before treating Invalid as definitive.
	ValidatingCount int32
	// Total is the number of processors and controller services inspected.
	Total int32
}

// FlowValidationClient inspects and toggles a process group subtree for a dry-run validation.
type FlowValidationClient interface {
	// InspectValidation walks a process group subtree and reports its component validation state.
	InspectValidation(ctx context.Context, baseURI string, processGroupID string) (FlowValidationReport, error)
	// SetControllerServicesState enables or disables every controller service in the subtree
	// (state ENABLED or DISABLED).
	SetControllerServicesState(ctx context.Context, baseURI string, processGroupID string, state string) error
	// ListChildProcessGroups lists the process groups directly under the given parent. It is used
	// to find and clean up a leaked temporary validation group by its deterministic name.
	ListChildProcessGroups(ctx context.Context, baseURI string, parentID string) ([]ProcessGroupEntity, error)
}

// HTTPFlowValidationClient implements FlowValidationClient against the NiFi REST API.
type HTTPFlowValidationClient struct {
	Client *http.Client
}

func (c HTTPFlowValidationClient) doJSON(ctx context.Context, method, endpoint string, body, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

func (c HTTPFlowValidationClient) SetControllerServicesState(ctx context.Context, baseURI string, processGroupID string, state string) error {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flow/process-groups/%s/controller-services", url.PathEscape(processGroupID)))
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodPut, endpoint, activateControllerServicesEntity{ID: processGroupID, State: state}, nil)
}

func (c HTTPFlowValidationClient) InspectValidation(ctx context.Context, baseURI string, processGroupID string) (FlowValidationReport, error) {
	var report FlowValidationReport
	// Breadth-first walk of the subtree so a deeply nested invalid component is still surfaced.
	queue := []string{processGroupID}
	for len(queue) > 0 {
		groupID := queue[0]
		queue = queue[1:]

		processors, err := c.listProcessors(ctx, baseURI, groupID)
		if err != nil {
			return FlowValidationReport{}, err
		}
		for i := range processors {
			comp := processors[i].Component
			report.Total++
			switch comp.ValidationStatus {
			case ValidationStatusValidating:
				report.ValidatingCount++
			case ValidationStatusInvalid:
				report.Invalid = append(report.Invalid, InvalidComponent{
					Kind: ValidationKindProcessor, ID: comp.ID, Name: comp.Name, Type: comp.Type,
					ProcessGroupID: groupID, ValidationErrors: comp.ValidationErrors,
				})
			}
		}

		services, err := c.listControllerServices(ctx, baseURI, groupID)
		if err != nil {
			return FlowValidationReport{}, err
		}
		for i := range services {
			comp := services[i].Component
			report.Total++
			switch comp.ValidationStatus {
			case ValidationStatusValidating:
				report.ValidatingCount++
			case ValidationStatusInvalid:
				report.Invalid = append(report.Invalid, InvalidComponent{
					Kind: ValidationKindControllerService, ID: comp.ID, Name: comp.Name, Type: comp.Type,
					ProcessGroupID: groupID, ValidationErrors: comp.ValidationErrors,
				})
			}
		}

		children, err := c.listChildGroups(ctx, baseURI, groupID)
		if err != nil {
			return FlowValidationReport{}, err
		}
		for i := range children {
			id := children[i].ID
			if id == "" {
				id = children[i].Component.ID
			}
			if id != "" {
				queue = append(queue, id)
			}
		}
	}
	return report, nil
}

func (c HTTPFlowValidationClient) listProcessors(ctx context.Context, baseURI string, groupID string) ([]ProcessorEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/processors", url.PathEscape(groupID)))
	if err != nil {
		return nil, err
	}
	var response struct {
		Processors []ProcessorEntity `json:"processors"`
	}
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.Processors, nil
}

func (c HTTPFlowValidationClient) listControllerServices(ctx context.Context, baseURI string, groupID string) ([]ControllerServiceEntity, error) {
	// Listing controller services is a /flow endpoint (the /process-groups path only supports
	// create). Restrict to the group itself: ancestor scope would surface unrelated services and
	// descendant scope is covered by the walk, which would otherwise double-count them.
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/flow/process-groups/%s/controller-services", url.PathEscape(groupID)))
	if err != nil {
		return nil, err
	}
	endpoint += "?includeAncestorGroups=false&includeDescendantGroups=false"
	var response struct {
		ControllerServices []ControllerServiceEntity `json:"controllerServices"`
	}
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.ControllerServices, nil
}

// ListChildProcessGroups returns the process groups directly under parentID.
func (c HTTPFlowValidationClient) ListChildProcessGroups(ctx context.Context, baseURI string, parentID string) ([]ProcessGroupEntity, error) {
	return c.listChildGroups(ctx, baseURI, parentID)
}

func (c HTTPFlowValidationClient) listChildGroups(ctx context.Context, baseURI string, groupID string) ([]ProcessGroupEntity, error) {
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/process-groups/%s/process-groups", url.PathEscape(groupID)))
	if err != nil {
		return nil, err
	}
	var response struct {
		ProcessGroups []ProcessGroupEntity `json:"processGroups"`
	}
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.ProcessGroups, nil
}
