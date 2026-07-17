package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestInspectValidationWalksSubtree verifies the recursive walk surfaces invalid components
// from a child group, counts still-validating components, and reports validation errors.
func TestInspectValidationWalksSubtree(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method %s %s", r.Method, r.URL.Path)
		}
		switch r.URL.Path {
		case "/nifi-api/process-groups/root/processors":
			_ = json.NewEncoder(w).Encode(map[string]any{"processors": []ProcessorEntity{
				{ID: "p1", Component: ProcessorComponent{ID: "p1", Name: "Good", Type: "t", ValidationStatus: ValidationStatusValid}},
				{ID: "p2", Component: ProcessorComponent{ID: "p2", Name: "Settling", Type: "t", ValidationStatus: ValidationStatusValidating}},
			}})
		case "/nifi-api/flow/process-groups/root/controller-services":
			if r.URL.Query().Get("includeAncestorGroups") != "false" || r.URL.Query().Get("includeDescendantGroups") != "false" {
				t.Fatalf("controller-services scope query = %q", r.URL.RawQuery)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"controllerServices": []ControllerServiceEntity{}})
		case "/nifi-api/process-groups/root/process-groups":
			_ = json.NewEncoder(w).Encode(map[string]any{"processGroups": []ProcessGroupEntity{
				{ID: "child", Component: ProcessGroupComponent{ID: "child"}},
			}})
		case "/nifi-api/process-groups/child/processors":
			_ = json.NewEncoder(w).Encode(map[string]any{"processors": []ProcessorEntity{
				{ID: "p3", Component: ProcessorComponent{ID: "p3", Name: "Broken", Type: "org.apache.nifi.X", ValidationStatus: ValidationStatusInvalid, ValidationErrors: []string{"'Prop' is invalid"}}},
			}})
		case "/nifi-api/flow/process-groups/child/controller-services":
			_ = json.NewEncoder(w).Encode(map[string]any{"controllerServices": []ControllerServiceEntity{
				{ID: "cs1", Component: ControllerServiceComponent{ID: "cs1", Name: "BadService", Type: "org.apache.nifi.CS", ValidationStatus: ValidationStatusInvalid, ValidationErrors: []string{"missing property"}}},
			}})
		case "/nifi-api/process-groups/child/process-groups":
			_ = json.NewEncoder(w).Encode(map[string]any{"processGroups": []ProcessGroupEntity{}})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	report, err := (HTTPFlowValidationClient{}).InspectValidation(t.Context(), server.URL, "root")
	if err != nil {
		t.Fatal(err)
	}
	if report.Total != 4 {
		t.Fatalf("total inspected = %d, want 4", report.Total)
	}
	if report.ValidatingCount != 1 {
		t.Fatalf("validating count = %d, want 1", report.ValidatingCount)
	}
	if len(report.Invalid) != 2 {
		t.Fatalf("invalid components = %d, want 2: %#v", len(report.Invalid), report.Invalid)
	}
	var sawProcessor, sawService bool
	for _, c := range report.Invalid {
		switch c.Kind {
		case ValidationKindProcessor:
			sawProcessor = true
			if c.ID != "p3" || c.ProcessGroupID != "child" || len(c.ValidationErrors) != 1 {
				t.Fatalf("invalid processor = %#v", c)
			}
		case ValidationKindControllerService:
			sawService = true
			if c.ID != "cs1" || c.Name != "BadService" {
				t.Fatalf("invalid service = %#v", c)
			}
		}
	}
	if !sawProcessor || !sawService {
		t.Fatalf("expected both an invalid processor and service, got %#v", report.Invalid)
	}
}

func TestSetControllerServicesState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/flow/process-groups/pg1/controller-services" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in activateControllerServicesEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.ID != "pg1" || in.State != RunStateEnabled {
			t.Fatalf("payload = %#v", in)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPFlowValidationClient{}).SetControllerServicesState(t.Context(), server.URL, "pg1", RunStateEnabled); err != nil {
		t.Fatal(err)
	}
}
