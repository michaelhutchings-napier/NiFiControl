package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPParameterProviderClientCreatePostsToController(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/controller/parameter-providers" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ParameterProviderEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.Type != "org.apache.nifi.parameter.EnvironmentVariableParameterProvider" || in.Component.Name != "env" {
			t.Fatalf("create payload = %#v", in.Component)
		}
		out := in
		out.ID = "pp1"
		out.Component.ID = "pp1"
		out.Component.ValidationStatus = "VALID"
		_ = json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()

	created, err := (HTTPParameterProviderClient{}).CreateParameterProvider(t.Context(), server.URL, ParameterProviderEntity{
		Revision: Revision{Version: 0},
		Component: ParameterProviderComponent{
			Name: "env",
			Type: "org.apache.nifi.parameter.EnvironmentVariableParameterProvider",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if ParameterProviderEntityID(*created) != "pp1" || created.Component.ValidationStatus != "VALID" {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPParameterProviderClientUpdatePutsToProvider(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut || r.URL.Path != "/nifi-api/parameter-providers/pp1" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in ParameterProviderEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Revision.Version != 4 || in.Component.Properties["Parameter Group Name"] != "envs" {
			t.Fatalf("update payload = %#v", in)
		}
		_ = json.NewEncoder(w).Encode(ParameterProviderEntity{ID: "pp1", Revision: Revision{Version: 5}, Component: ParameterProviderComponent{ID: "pp1", ValidationStatus: "VALID"}})
	}))
	defer server.Close()

	updated, err := (HTTPParameterProviderClient{}).UpdateParameterProvider(t.Context(), server.URL, ParameterProviderEntity{
		ID:       "pp1",
		Revision: Revision{Version: 4},
		Component: ParameterProviderComponent{
			ID:         "pp1",
			Properties: map[string]string{"Parameter Group Name": "envs"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Revision.Version != 5 {
		t.Fatalf("updated = %#v", updated)
	}
}

func TestHTTPParameterProviderClientDeleteSendsVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/nifi-api/parameter-providers/pp1" || r.URL.Query().Get("version") != "5" {
			t.Fatalf("got %s %s ?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPParameterProviderClient{}).DeleteParameterProvider(t.Context(), server.URL, "pp1", 5); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPParameterProviderClientGetNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no provider", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := (HTTPParameterProviderClient{}).GetParameterProvider(t.Context(), server.URL, "missing")
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}
}
