package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPProcessorClientRunStatusUsesRunStatusEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/processors/p1/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ProcessorRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "RUNNING" || in.Revision.Version != 2 {
			t.Fatalf("run-status payload = %#v", in)
		}
		_ = json.NewEncoder(w).Encode(ProcessorEntity{ID: "p1", Revision: Revision{Version: 3}, Component: ProcessorComponent{ID: "p1", State: "RUNNING"}})
	}))
	defer server.Close()

	updated, err := (HTTPProcessorClient{}).UpdateProcessorRunStatus(t.Context(), server.URL, "p1", 2, "RUNNING")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Component.State != "RUNNING" || updated.Revision.Version != 3 {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestHTTPControllerServiceClientRunStatusUsesRunStatusEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/controller-services/cs1/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ControllerServiceRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "ENABLED" {
			t.Fatalf("state = %q", in.State)
		}
		_ = json.NewEncoder(w).Encode(ControllerServiceEntity{ID: "cs1", Revision: Revision{Version: 4}, Component: ControllerServiceComponent{ID: "cs1", State: "ENABLING"}})
	}))
	defer server.Close()

	updated, err := (HTTPControllerServiceClient{}).UpdateControllerServiceRunStatus(t.Context(), server.URL, "cs1", 3, "ENABLED")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Component.State != "ENABLING" {
		t.Fatalf("updated = %#v", updated)
	}
}
