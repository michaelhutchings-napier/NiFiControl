//go:build integration

package controller

import (
	"os"
	"testing"
	"time"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

// TestNiFi210NodeOffloadAgainstLiveCluster drives the NiFi cluster-node lifecycle against a
// real, clustered Apache NiFi 2.10 (two or more connected nodes). It disconnects, offloads,
// and removes the highest-addressed node — exactly the sequence the operator performs on a
// graceful scale-down — and verifies the node leaves the cluster.
//
// hack/test-offload-kind.sh provisions a 2-node clustered NiFi inside a kind cluster and
// sets NIFI_API_URI to the cluster coordinator.
func TestNiFi210NodeOffloadAgainstLiveCluster(t *testing.T) {
	baseURI := os.Getenv("NIFI_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_API_URI is not set")
	}
	ctx := t.Context()
	client := nifi.HTTPClusterNodeClient{}

	nodes, err := client.ListClusterNodes(ctx, baseURI)
	if err != nil {
		t.Fatalf("list cluster nodes: %v", err)
	}
	connected := connectedNodes(nodes)
	if len(connected) < 2 {
		t.Fatalf("need at least 2 connected nodes, got %d (%#v)", len(connected), nodes)
	}
	t.Logf("cluster has %d connected nodes", len(connected))

	// Remove the node with the highest address, mirroring StatefulSet highest-ordinal-first.
	target := connected[0]
	for _, node := range connected {
		if node.Address > target.Address {
			target = node
		}
	}
	t.Logf("offloading node %s (%s)", target.NodeID, target.Address)

	if err := client.SetClusterNodeState(ctx, baseURI, target.NodeID, nifi.NodeStatusDisconnecting); err != nil {
		t.Fatalf("disconnect node: %v", err)
	}
	waitForNodeStatus(t, client, baseURI, target.NodeID, nifi.NodeStatusDisconnected)

	if err := client.SetClusterNodeState(ctx, baseURI, target.NodeID, nifi.NodeStatusOffloading); err != nil {
		t.Fatalf("offload node: %v", err)
	}
	waitForNodeStatus(t, client, baseURI, target.NodeID, nifi.NodeStatusOffloaded)

	if err := client.DeleteClusterNode(ctx, baseURI, target.NodeID); err != nil {
		t.Fatalf("delete node: %v", err)
	}

	// The node must leave the cluster.
	deadline := time.Now().Add(2 * time.Minute)
	for {
		nodes, err := client.ListClusterNodes(ctx, baseURI)
		if err != nil {
			t.Fatalf("list cluster nodes after delete: %v", err)
		}
		if findOffloadNode(nodes, target.NodeID) == nil {
			t.Logf("node %s removed; cluster now has %d nodes", target.NodeID, len(nodes))
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s still present after delete", target.NodeID)
		}
		time.Sleep(2 * time.Second)
	}
}

func connectedNodes(nodes []nifi.ClusterNode) []nifi.ClusterNode {
	var connected []nifi.ClusterNode
	for _, node := range nodes {
		if node.Status == nifi.NodeStatusConnected {
			connected = append(connected, node)
		}
	}
	return connected
}

func findOffloadNode(nodes []nifi.ClusterNode, nodeID string) *nifi.ClusterNode {
	for i := range nodes {
		if nodes[i].NodeID == nodeID {
			return &nodes[i]
		}
	}
	return nil
}

func waitForNodeStatus(t *testing.T, client nifi.ClusterNodeClient, baseURI, nodeID, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	for {
		nodes, err := client.ListClusterNodes(t.Context(), baseURI)
		if err != nil {
			t.Fatalf("list cluster nodes: %v", err)
		}
		node := findOffloadNode(nodes, nodeID)
		if node != nil && node.Status == want {
			return
		}
		if time.Now().After(deadline) {
			status := "<absent>"
			if node != nil {
				status = node.Status
			}
			t.Fatalf("node %s did not reach %s (current %s)", nodeID, want, status)
		}
		time.Sleep(2 * time.Second)
	}
}
