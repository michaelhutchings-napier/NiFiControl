package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPAccessPolicyClientGetForResourceKeepsResourceSlashes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nifi-api/policies/read/process-groups/pg-1" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(AccessPolicyEntity{ID: "p1", Revision: Revision{Version: 3}, Component: AccessPolicyComponent{ID: "p1", Resource: "/process-groups/pg-1", Action: "read"}})
	}))
	defer server.Close()

	policy, err := (HTTPAccessPolicyClient{}).GetAccessPolicyForResource(t.Context(), server.URL, "read", "/process-groups/pg-1")
	if err != nil {
		t.Fatal(err)
	}
	if AccessPolicyEntityID(*policy) != "p1" || policy.Component.Resource != "/process-groups/pg-1" {
		t.Fatalf("policy = %#v", policy)
	}
}

func TestHTTPAccessPolicyClientGetForResourceNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no policy", http.StatusNotFound)
	}))
	defer server.Close()

	_, err := (HTTPAccessPolicyClient{}).GetAccessPolicyForResource(t.Context(), server.URL, "read", "/flow")
	if !IsNotFound(err) {
		t.Fatalf("expected IsNotFound, got %v", err)
	}
}

func TestHTTPAccessPolicyClientCreate(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/nifi-api/policies" {
			t.Fatalf("got %s %s", r.Method, r.URL.Path)
		}
		var in AccessPolicyEntity
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Component.Resource != "/flow" || in.Component.Action != "read" || len(in.Component.Users) != 1 || in.Component.Users[0].ID != "u1" {
			t.Fatalf("create payload = %#v", in.Component)
		}
		_ = json.NewEncoder(w).Encode(AccessPolicyEntity{ID: "p1", Revision: Revision{Version: 0}, Component: in.Component})
	}))
	defer server.Close()

	created, err := (HTTPAccessPolicyClient{}).CreateAccessPolicy(t.Context(), server.URL, AccessPolicyEntity{
		Revision:  Revision{Version: 0},
		Component: AccessPolicyComponent{Resource: "/flow", Action: "read", Users: []TenantRef{{ID: "u1"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if AccessPolicyEntityID(*created) != "p1" {
		t.Fatalf("created = %#v", created)
	}
}

func TestHTTPAccessPolicyClientDeleteSendsVersion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.URL.Path != "/nifi-api/policies/p1" || r.URL.Query().Get("version") != "4" {
			t.Fatalf("got %s %s ?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPAccessPolicyClient{}).DeleteAccessPolicy(t.Context(), server.URL, "p1", 4); err != nil {
		t.Fatal(err)
	}
}
