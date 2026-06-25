package nifi

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

// NiFi cluster node connection states. A node is gracefully removed from a cluster by
// requesting DISCONNECTING (until DISCONNECTED), then OFFLOADING (until OFFLOADED, which
// redistributes its queued FlowFiles to the remaining connected nodes), then deleting it.
const (
	NodeStatusConnected     = "CONNECTED"
	NodeStatusConnecting    = "CONNECTING"
	NodeStatusDisconnecting = "DISCONNECTING"
	NodeStatusDisconnected  = "DISCONNECTED"
	NodeStatusOffloading    = "OFFLOADING"
	NodeStatusOffloaded     = "OFFLOADED"
)

// ClusterNode is a single node reported by the NiFi cluster controller endpoint.
type ClusterNode struct {
	NodeID  string `json:"nodeId,omitempty"`
	Address string `json:"address,omitempty"`
	APIPort int32  `json:"apiPort,omitempty"`
	Status  string `json:"status,omitempty"`
}

type clusterEntity struct {
	Cluster struct {
		Nodes []ClusterNode `json:"nodes"`
	} `json:"cluster"`
}

// nodeEntity wraps a single node for the cluster node endpoints.
type nodeEntity struct {
	Node ClusterNode `json:"node"`
}

// ClusterNodeClient is the NiFi 2.x cluster controller surface used to gracefully scale a
// managed cluster down: enumerate nodes, drive a node through disconnect/offload, and
// remove an offloaded node so the operator can delete its pod without losing data.
type ClusterNodeClient interface {
	ListClusterNodes(ctx context.Context, baseURI string) ([]ClusterNode, error)
	SetClusterNodeState(ctx context.Context, baseURI string, nodeID string, state string) error
	DeleteClusterNode(ctx context.Context, baseURI string, nodeID string) error
}

// HTTPClusterNodeClient implements ClusterNodeClient against the NiFi REST API.
type HTTPClusterNodeClient struct {
	Client *http.Client
}

func (c HTTPClusterNodeClient) doJSON(ctx context.Context, method, endpoint string, body, out any) error {
	return doJSON(ctx, c.Client, method, endpoint, body, out)
}

// ListClusterNodes returns the nodes of the NiFi cluster. The endpoint is only served by a
// node that is connected to a cluster; a standalone node returns an error.
func (c HTTPClusterNodeClient) ListClusterNodes(ctx context.Context, baseURI string) ([]ClusterNode, error) {
	endpoint, err := apiURL(baseURI, "/controller/cluster")
	if err != nil {
		return nil, err
	}
	var response clusterEntity
	if err := c.doJSON(ctx, http.MethodGet, endpoint, nil, &response); err != nil {
		return nil, err
	}
	return response.Cluster.Nodes, nil
}

// SetClusterNodeState requests a new connection state for a node. Valid request states are
// CONNECTING, DISCONNECTING, and OFFLOADING; NiFi transitions the node asynchronously to
// the corresponding terminal state.
func (c HTTPClusterNodeClient) SetClusterNodeState(ctx context.Context, baseURI string, nodeID string, state string) error {
	if nodeID == "" {
		return fmt.Errorf("cluster node id is required")
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller/cluster/nodes/%s", url.PathEscape(nodeID)))
	if err != nil {
		return err
	}
	body := nodeEntity{Node: ClusterNode{NodeID: nodeID, Status: state}}
	return c.doJSON(ctx, http.MethodPut, endpoint, body, nil)
}

// DeleteClusterNode removes a disconnected or offloaded node from the cluster.
func (c HTTPClusterNodeClient) DeleteClusterNode(ctx context.Context, baseURI string, nodeID string) error {
	if nodeID == "" {
		return fmt.Errorf("cluster node id is required")
	}
	endpoint, err := apiURL(baseURI, fmt.Sprintf("/controller/cluster/nodes/%s", url.PathEscape(nodeID)))
	if err != nil {
		return err
	}
	return c.doJSON(ctx, http.MethodDelete, endpoint, nil, nil)
}
