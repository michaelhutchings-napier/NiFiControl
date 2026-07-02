package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPInputPortClientRunStatusUsesRunStatusEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/input-ports/p1/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in PortRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "RUNNING" || in.Revision.Version != 4 {
			t.Fatalf("run-status payload = %#v", in)
		}
		_ = json.NewEncoder(w).Encode(PortEntity{ID: "p1", Revision: Revision{Version: 5}, Component: PortComponent{ID: "p1", State: "RUNNING"}})
	}))
	defer server.Close()

	updated, err := (HTTPInputPortClient{}).UpdateInputPortRunStatus(t.Context(), server.URL, "p1", 4, "RUNNING")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Component.State != "RUNNING" || updated.Revision.Version != 5 {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestHTTPOutputPortClientRunStatusUsesRunStatusEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/output-ports/p2/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in PortRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "STOPPED" {
			t.Fatalf("state = %q", in.State)
		}
		_ = json.NewEncoder(w).Encode(PortEntity{ID: "p2", Revision: Revision{Version: 3}, Component: PortComponent{ID: "p2", State: "STOPPED"}})
	}))
	defer server.Close()

	if _, err := (HTTPOutputPortClient{}).UpdateOutputPortRunStatus(t.Context(), server.URL, "p2", 2, "STOPPED"); err != nil {
		t.Fatal(err)
	}
}
