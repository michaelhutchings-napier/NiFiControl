package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/utils/ptr"
)

func TestHTTPControllerConfigClientGet(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/nifi-api/controller/config" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(ControllerConfigurationEntity{
			Revision:  Revision{Version: 4},
			Component: ControllerConfigurationDTO{MaxTimerDrivenThreadCount: ptr.To[int32](10)},
		})
	}))
	defer server.Close()

	got, err := (HTTPControllerConfigClient{}).GetControllerConfig(t.Context(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got.Revision.Version != 4 || got.Component.MaxTimerDrivenThreadCount == nil || *got.Component.MaxTimerDrivenThreadCount != 10 {
		t.Fatalf("get = %#v", got)
	}
}

func TestHTTPControllerConfigClientUpdatePutsWithRevision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/controller/config" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ControllerConfigurationEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Revision.Version != 4 || in.Component.MaxTimerDrivenThreadCount == nil || *in.Component.MaxTimerDrivenThreadCount != 25 {
			t.Fatalf("update payload = %#v (rev %d)", in.Component, in.Revision.Version)
		}
		out := in
		out.Revision.Version = 5
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	updated, err := (HTTPControllerConfigClient{}).UpdateControllerConfig(t.Context(), server.URL, ControllerConfigurationEntity{
		Revision:  Revision{Version: 4},
		Component: ControllerConfigurationDTO{MaxTimerDrivenThreadCount: ptr.To[int32](25)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision.Version != 5 {
		t.Fatalf("updated revision = %d, want 5", updated.Revision.Version)
	}
}
