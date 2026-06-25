package nifi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPClusterNodeClientListClusterNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/cluster" {
			t.Fatalf("path = %s, want /nifi-api/controller/cluster", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"cluster":{"generated":"now","nodes":[
			{"nodeId":"n0","address":"nifi-0.headless.ns.svc","apiPort":8443,"status":"CONNECTED"},
			{"nodeId":"n1","address":"nifi-1.headless.ns.svc","apiPort":8443,"status":"OFFLOADED"}
		]}}`))
	}))
	defer server.Close()

	nodes, err := (HTTPClusterNodeClient{}).ListClusterNodes(t.Context(), server.URL)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes length = %d, want 2", len(nodes))
	}
	if nodes[0].NodeID != "n0" || nodes[0].Address != "nifi-0.headless.ns.svc" || nodes[0].Status != NodeStatusConnected {
		t.Fatalf("node[0] = %#v", nodes[0])
	}
	if nodes[1].Status != NodeStatusOffloaded {
		t.Fatalf("node[1] status = %q, want OFFLOADED", nodes[1].Status)
	}
}

func TestHTTPClusterNodeClientSetClusterNodeState(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("method = %s, want PUT", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/cluster/nodes/n1" {
			t.Fatalf("path = %s, want /nifi-api/controller/cluster/nodes/n1", r.URL.Path)
		}
		var body nodeEntity
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if body.Node.NodeID != "n1" || body.Node.Status != NodeStatusOffloading {
			t.Fatalf("body node = %#v, want nodeId n1 status OFFLOADING", body.Node)
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	defer server.Close()

	if err := (HTTPClusterNodeClient{}).SetClusterNodeState(t.Context(), server.URL, "n1", NodeStatusOffloading); err != nil {
		t.Fatal(err)
	}
}

func TestHTTPClusterNodeClientDeleteClusterNode(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.Method != http.MethodDelete {
			t.Fatalf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/nifi-api/controller/cluster/nodes/n1" {
			t.Fatalf("path = %s, want /nifi-api/controller/cluster/nodes/n1", r.URL.Path)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	if err := (HTTPClusterNodeClient{}).DeleteClusterNode(t.Context(), server.URL, "n1"); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("delete endpoint was not called")
	}
}

func TestHTTPClusterNodeClientRequiresNodeID(t *testing.T) {
	if err := (HTTPClusterNodeClient{}).SetClusterNodeState(t.Context(), "http://nifi.example.com", "", NodeStatusDisconnecting); err == nil {
		t.Fatal("expected error for empty node id")
	}
	if err := (HTTPClusterNodeClient{}).DeleteClusterNode(t.Context(), "http://nifi.example.com", ""); err == nil {
		t.Fatal("expected error for empty node id")
	}
}
