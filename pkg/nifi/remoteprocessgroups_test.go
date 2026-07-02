package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPRemoteProcessGroupClientCreatePostsUnderParent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/process-groups/root/remote-process-groups" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in RemoteProcessGroupEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.TargetURIs != "https://central:8443/nifi" || in.Component.TransportProtocol != "HTTP" {
			t.Fatalf("create payload = %#v", in.Component)
		}
		out := in
		out.ID = "rpg1"
		out.Component.ID = "rpg1"
		out.Component.TargetSecure = true
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	created, err := (HTTPRemoteProcessGroupClient{}).CreateRemoteProcessGroup(t.Context(), server.URL, "root", RemoteProcessGroupEntity{
		Revision: Revision{Version: 0},
		Component: RemoteProcessGroupComponent{
			TargetURIs:        "https://central:8443/nifi",
			TransportProtocol: "HTTP",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if RemoteProcessGroupEntityID(*created) != "rpg1" || !created.Component.TargetSecure {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPRemoteProcessGroupClientRunStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/remote-process-groups/rpg1/run-status" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in RemoteProcessGroupRunStatusEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.State != "STOPPED" || in.Revision.Version != 2 {
			t.Fatalf("run-status payload = %#v", in)
		}
		_ = json.NewEncoder(w).Encode(RemoteProcessGroupEntity{ID: "rpg1", Revision: Revision{Version: 3}, Component: RemoteProcessGroupComponent{ID: "rpg1", Transmitting: false}})
	}))
	defer server.Close()

	updated, err := (HTTPRemoteProcessGroupClient{}).UpdateRemoteProcessGroupRunStatus(t.Context(), server.URL, "rpg1", 2, "STOPPED")
	if err != nil {
		t.Fatal(err)
	}
	if updated.Component.Transmitting || updated.Revision.Version != 3 {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestHTTPRemoteProcessGroupClientDeleteSendsVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/nifi-api/remote-process-groups/rpg1" || r.URL.Query().Get("version") != "5" {
			t.Fatalf("got %s %s ?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPRemoteProcessGroupClient{}).DeleteRemoteProcessGroup(t.Context(), server.URL, "rpg1", 5); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPRemoteProcessGroupClientGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no rpg", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := (HTTPRemoteProcessGroupClient{}).GetRemoteProcessGroup(t.Context(), server.URL, "missing")
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}
}

func TestRemoteProcessGroupTransmissionState(t *testing.T) {
	if RemoteProcessGroupTransmissionState(true) != "TRANSMITTING" {
		t.Fatal("true should map to TRANSMITTING")
	}
	if RemoteProcessGroupTransmissionState(false) != "STOPPED" {
		t.Fatal("false should map to STOPPED")
	}
}
