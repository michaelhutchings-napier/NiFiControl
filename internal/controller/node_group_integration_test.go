//go:build integration

package controller

import (
	"os"
	"strings"
	"testing"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

// TestNiFi210NodeGroupFormsOneCluster verifies that two separate StatefulSets — modelling a
// cluster's primary pool and a NiFiNodeGroup pool — join a single Apache NiFi 2.10 cluster
// through a shared headless Service, ZooKeeper, and sensitive-properties key. It asserts the
// cluster reports connected nodes from both pools (distinct StatefulSet address prefixes).
//
// hack/test-nodegroups-kind.sh provisions the two pools in a kind cluster and sets
// NIFI_API_URI to a node.
func TestNiFi210NodeGroupFormsOneCluster(t *testing.T) {
	baseURI := os.Getenv("NIFI_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_API_URI is not set")
	}
	nodes, err := (nifi.HTTPClusterNodeClient{}).ListClusterNodes(t.Context(), baseURI)
	if err != nil {
		t.Fatalf("list cluster nodes: %v", err)
	}
	var primary, workers int
	for _, node := range nodes {
		if node.Status != nifi.NodeStatusConnected {
			continue
		}
		switch {
		case strings.HasPrefix(node.Address, "nifi-workers-"):
			workers++
		case strings.HasPrefix(node.Address, "nifi-"):
			primary++
		}
	}
	t.Logf("connected nodes: primary=%d workers=%d (%d total)", primary, workers, len(nodes))
	if primary < 1 || workers < 1 {
		t.Fatalf("expected at least one connected node in each pool, got primary=%d workers=%d from %#v", primary, workers, nodes)
	}
}
