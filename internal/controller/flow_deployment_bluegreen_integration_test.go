//go:build integration

package controller

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestNiFi210BlueGreenSwitchAgainstLiveNiFi drives the transactional BlueGreen state
// machine against a real Apache NiFi 2.10 instance. It builds a blue process group with
// boundary connections (an external input-port source feeding the blue input port, and
// the blue output port feeding an external funnel), then promotes a green candidate and
// verifies the boundary connections are re-pointed to the green ports.
//
// hack/test-bluegreen-kind.sh provisions NiFi inside a kind cluster and sets NIFI_API_URI.
func TestNiFi210BlueGreenSwitchAgainstLiveNiFi(t *testing.T) {
	baseURI := os.Getenv("NIFI_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_API_URI is not set")
	}
	ctx := t.Context()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	blueName := "bg-blue-" + suffix

	processGroups := nifi.HTTPProcessGroupClient{}
	inputPorts := nifi.HTTPInputPortClient{}
	outputPorts := nifi.HTTPOutputPortClient{}
	connections := nifi.HTTPConnectionClient{}
	funnels := nifi.HTTPFunnelClient{}
	snapshots := nifi.HTTPFlowSnapshotClient{}
	scheduler := nifi.HTTPProcessGroupScheduler{}
	bgClient := nifi.HTTPBlueGreenClient{}

	root, err := processGroups.GetProcessGroup(ctx, baseURI, "root")
	if err != nil {
		t.Fatalf("get root: %v", err)
	}
	rootID := processGroupEntityID(*root)

	// Track everything created so the persistent kind cluster stays clean across runs.
	var createdPGs []string
	deletePG := func(id string) {
		_ = scheduler.ScheduleProcessGroup(ctx, baseURI, id, nifi.RunStateStopped)
		if e, err := processGroups.GetProcessGroup(ctx, baseURI, id); err == nil && e != nil {
			_ = processGroups.DeleteProcessGroup(ctx, baseURI, id, e.Revision.Version)
		}
	}
	t.Cleanup(func() {
		// Delete boundary connections first, then ports/funnels, then process groups.
		if conns, err := bgClient.ListProcessGroupConnections(ctx, baseURI, rootID); err == nil {
			for _, conn := range conns {
				for _, pg := range createdPGs {
					if conn.Component.Source.GroupID == pg || conn.Component.Destination.GroupID == pg {
						id := conn.ID
						if id == "" {
							id = conn.Component.ID
						}
						_ = bgClient.DeleteConnection(ctx, baseURI, id, conn.Revision.Version)
					}
				}
			}
		}
		for _, id := range createdPGs {
			deletePG(id)
		}
	})

	blue, err := processGroups.CreateProcessGroup(ctx, baseURI, rootID, nifi.ProcessGroupEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.ProcessGroupComponent{Name: blueName, Position: &nifi.Position{X: 100, Y: 100}}})
	if err != nil {
		t.Fatalf("create blue: %v", err)
	}
	blueID := processGroupEntityID(*blue)
	createdPGs = append(createdPGs, blueID)

	blueIn, err := inputPorts.CreateInputPort(ctx, baseURI, blueID, nifi.PortEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.PortComponent{Name: "in", Position: &nifi.Position{X: 100, Y: 100}}})
	if err != nil {
		t.Fatalf("create blue input port: %v", err)
	}
	blueOut, err := outputPorts.CreateOutputPort(ctx, baseURI, blueID, nifi.PortEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.PortComponent{Name: "out", Position: &nifi.Position{X: 100, Y: 400}}})
	if err != nil {
		t.Fatalf("create blue output port: %v", err)
	}
	// Wire the input port to the output port inside blue so both ports are valid; an
	// unconnected port is invalid in NiFi and would never pass the readiness gate.
	if _, err := connections.CreateConnection(ctx, baseURI, blueID, nifi.ConnectionEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.ConnectionComponent{
		Source:      nifi.Connectable{ID: bgPortEntityID(blueIn), Type: nifi.ConnectableInputPort, GroupID: blueID},
		Destination: nifi.Connectable{ID: bgPortEntityID(blueOut), Type: nifi.ConnectableOutputPort, GroupID: blueID},
	}}); err != nil {
		t.Fatalf("create blue internal connection: %v", err)
	}

	// External boundary endpoints in root: a feed input port (stoppable source) and a sink funnel.
	feed, err := inputPorts.CreateInputPort(ctx, baseURI, rootID, nifi.PortEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.PortComponent{Name: "bg-feed-" + suffix, Position: &nifi.Position{X: 400, Y: -200}}})
	if err != nil {
		t.Fatalf("create feed: %v", err)
	}
	feedID := bgPortEntityID(feed)
	t.Cleanup(func() {
		if e, err := inputPorts.GetInputPort(ctx, baseURI, feedID); err == nil && e != nil {
			_ = inputPorts.DeleteInputPort(ctx, baseURI, feedID, e.Revision.Version)
		}
	})
	sink, err := funnels.CreateFunnel(ctx, baseURI, rootID, nifi.FunnelEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.FunnelComponent{Position: &nifi.Position{X: 400, Y: 400}}})
	if err != nil {
		t.Fatalf("create sink funnel: %v", err)
	}
	sinkID := sink.ID
	if sinkID == "" {
		sinkID = sink.Component.ID
	}
	t.Cleanup(func() {
		if e, err := funnels.GetFunnel(ctx, baseURI, sinkID); err == nil && e != nil {
			_ = funnels.DeleteFunnel(ctx, baseURI, sinkID, e.Revision.Version)
		}
	})

	if _, err := connections.CreateConnection(ctx, baseURI, rootID, nifi.ConnectionEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.ConnectionComponent{
		Source:      nifi.Connectable{ID: feedID, Type: nifi.ConnectableInputPort, GroupID: rootID},
		Destination: nifi.Connectable{ID: bgPortEntityID(blueIn), Type: nifi.ConnectableInputPort, GroupID: blueID},
	}}); err != nil {
		t.Fatalf("create inbound boundary connection: %v", err)
	}
	if _, err := connections.CreateConnection(ctx, baseURI, rootID, nifi.ConnectionEntity{Revision: nifi.Revision{Version: 0}, Component: nifi.ConnectionComponent{
		Source:      nifi.Connectable{ID: bgPortEntityID(blueOut), Type: nifi.ConnectableOutputPort, GroupID: blueID},
		Destination: nifi.Connectable{ID: sinkID, Type: nifi.ConnectableFunnel, GroupID: rootID},
	}}); err != nil {
		t.Fatalf("create outbound boundary connection: %v", err)
	}

	// A real, valid snapshot of blue to deploy as the green candidate.
	snapshot, err := snapshots.DownloadProcessGroup(ctx, baseURI, blueID)
	if err != nil {
		t.Fatalf("download blue snapshot: %v", err)
	}
	t.Logf("downloaded snapshot bytes=%d connectionsInSnapshot=%d", len(snapshot), countSnapshotConnections(t, snapshot))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: blueName, Namespace: "default", Generation: 2},
		Spec: nifiv1alpha1.NiFiFlowDeploymentSpec{
			ClusterRef: nifiv1alpha1.ClusterReference{Name: "production"},
			Target:     nifiv1alpha1.FlowDeploymentTarget{ProcessGroupName: blueName},
			Rollout:    nifiv1alpha1.RolloutStrategy{Strategy: "BlueGreen", BlueGreen: &nifiv1alpha1.BlueGreenStrategy{DrainTimeoutSeconds: 30, OnDrainTimeout: "Fail", ReadinessTimeoutSeconds: 120}},
		},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{ProcessGroupID: blueID, DeployedVersion: "v1", ArtifactDigest: "sha256:old"},
	}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(deployment).WithStatusSubresource(&nifiv1alpha1.NiFiFlowDeployment{}).Build()
	reconciler := &NiFiFlowDeploymentReconciler{Client: k8sClient, Scheme: scheme, ProcessGroupClient: processGroups, FlowSnapshotClient: snapshots, ProcessGroupScheduler: scheduler, BlueGreenClient: bgClient}

	key := types.NamespacedName{Name: blueName, Namespace: "default"}
	deadline := time.Now().Add(2 * time.Minute)
	var greenID string
	lastPhase := ""
	promoted := false
	for iteration := 0; time.Now().Before(deadline); iteration++ {
		current := &nifiv1alpha1.NiFiFlowDeployment{}
		if err := k8sClient.Get(ctx, key, current); err != nil {
			t.Fatal(err)
		}
		if current.Status.ActiveRollout != nil && current.Status.ActiveRollout.BlueGreen != nil && current.Status.ActiveRollout.BlueGreen.CandidateProcessGroupID != "" {
			greenID = current.Status.ActiveRollout.BlueGreen.CandidateProcessGroupID
		}
		if current.Status.ProcessGroupID != "" && current.Status.ProcessGroupID != blueID {
			greenID = current.Status.ProcessGroupID
		}
		if greenID != "" {
			ensureTracked(&createdPGs, greenID)
		}
		phase := ""
		if current.Status.ActiveRollout != nil {
			phase = current.Status.ActiveRollout.Phase
		}
		if phase != lastPhase || iteration%15 == 0 {
			lastPhase = phase
			diag := ""
			if greenID != "" {
				if pg, err := processGroups.GetProcessGroup(ctx, baseURI, greenID); err != nil {
					diag = fmt.Sprintf(" candidateGetErr=%v", err)
				} else if pg != nil {
					ins, _ := bgClient.ListProcessGroupInputPorts(ctx, baseURI, greenID)
					outs, _ := bgClient.ListProcessGroupOutputPorts(ctx, baseURI, greenID)
					conns, _ := bgClient.ListProcessGroupConnections(ctx, baseURI, greenID)
					diag = fmt.Sprintf(" candidate{invalid=%d running=%d stopped=%d disabled=%d inPorts=%d outPorts=%d internalConns=%d}", pg.InvalidCount, pg.RunningCount, pg.StoppedCount, pg.DisabledCount, len(ins), len(outs), len(conns))
					for _, p := range append(append([]nifi.PortEntity{}, ins...), outs...) {
						if p.Component.ValidationStatus != "" && p.Component.ValidationStatus != "VALID" {
							diag += fmt.Sprintf(" port[%s]=%s%v", p.Component.Name, p.Component.ValidationStatus, p.Component.ValidationErrors)
						}
					}
				}
			}
			t.Logf("iter=%d phase=%s syncState=%s lastError=%q%s", iteration, phase, current.Status.SyncState, current.Status.Sync.LastError, diag)
		}
		if current.Status.ActiveRollout == nil && current.Status.Sync.LastError != "" {
			t.Fatalf("BlueGreen rollout failed: %s", current.Status.Sync.LastError)
		}
		if current.Status.ProcessGroupID != "" && current.Status.ProcessGroupID != blueID && current.Status.ActiveRollout == nil {
			promoted = true
			break
		}
		if _, err := reconciler.reconcileBlueGreenRollout(ctx, current, baseURI, rootID, snapshot, "v2", "sha256:new"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		time.Sleep(time.Second)
	}
	if !promoted {
		final := &nifiv1alpha1.NiFiFlowDeployment{}
		_ = k8sClient.Get(ctx, key, final)
		t.Fatalf("BlueGreen rollout did not complete: ProcessGroupID=%q phase=%q syncState=%q lastError=%q", final.Status.ProcessGroupID, lastPhase, final.Status.SyncState, final.Status.Sync.LastError)
	}
	if greenID == "" || greenID == blueID {
		t.Fatalf("green was not promoted; greenID=%q blueID=%q", greenID, blueID)
	}
	// The promoted candidate must be fully valid (boundary ports connected) after the switch.
	if pg, err := processGroups.GetProcessGroup(ctx, baseURI, greenID); err != nil {
		t.Fatalf("get promoted green: %v", err)
	} else if pg.InvalidCount != 0 {
		t.Fatalf("promoted green has %d invalid component(s), want 0", pg.InvalidCount)
	}

	// The boundary connections must now reference the green ports, not blue.
	rootConnections, err := bgClient.ListProcessGroupConnections(ctx, baseURI, rootID)
	if err != nil {
		t.Fatalf("list root connections: %v", err)
	}
	inboundToGreen, outboundFromGreen := false, false
	for _, conn := range rootConnections {
		if conn.Component.Destination.GroupID == greenID {
			inboundToGreen = true
		}
		if conn.Component.Source.GroupID == greenID {
			outboundFromGreen = true
		}
		if conn.Component.Destination.GroupID == blueID || conn.Component.Source.GroupID == blueID {
			t.Fatalf("a boundary connection still references blue: %#v", conn.Component)
		}
	}
	if !inboundToGreen || !outboundFromGreen {
		t.Fatalf("boundary connections were not switched to green: inbound=%v outbound=%v", inboundToGreen, outboundFromGreen)
	}
}

func countSnapshotConnections(t *testing.T, snapshot []byte) int {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(snapshot, &decoded); err != nil {
		return -1
	}
	if wrapped, ok := decoded["versionedFlowSnapshot"].(map[string]any); ok {
		decoded = wrapped
	}
	flowContents, ok := decoded["flowContents"].(map[string]any)
	if !ok {
		return -1
	}
	conns, _ := flowContents["connections"].([]any)
	return len(conns)
}

func ensureTracked(ids *[]string, id string) {
	for _, existing := range *ids {
		if existing == id {
			return
		}
	}
	*ids = append(*ids, id)
}

func bgPortEntityID(port *nifi.PortEntity) string {
	if port == nil {
		return ""
	}
	if port.ID != "" {
		return port.ID
	}
	return port.Component.ID
}
