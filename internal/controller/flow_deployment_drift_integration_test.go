//go:build integration

package controller

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

func TestNiFi210DownloadedSnapshotNormalizesToDesiredFlow(t *testing.T) {
	baseURI := os.Getenv("NIFI_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_API_URI is not set")
	}
	desired := json.RawMessage(`{
      "snapshotMetadata":{"version":1,"author":"NiFiControl integration"},
      "flowContents":{
        "identifier":"66fa611a-d45f-49d1-9e21-fc70d14383a2",
        "name":"NiFiControl drift integration",
        "comments":"drift baseline",
        "componentType":"PROCESS_GROUP",
        "position":{"x":0,"y":0},
        "processors":[],"controllerServices":[],"processGroups":[],
        "remoteProcessGroups":[],"inputPorts":[],"outputPorts":[],
        "connections":[],"funnels":[],"labels":[]
      }
    }`)
	snapshots := nifi.HTTPFlowSnapshotClient{}
	processGroups := nifi.HTTPProcessGroupClient{}
	imported, err := snapshots.ImportProcessGroup(t.Context(), baseURI, "root", desired)
	if err != nil {
		t.Fatal(err)
	}
	processGroupID := processGroupEntityID(*imported)
	t.Cleanup(func() {
		current, getErr := processGroups.GetProcessGroup(t.Context(), baseURI, processGroupID)
		if getErr == nil && current != nil {
			_ = processGroups.DeleteProcessGroup(t.Context(), baseURI, processGroupID, current.Revision.Version)
		}
	})
	live, err := snapshots.DownloadProcessGroup(t.Context(), baseURI, processGroupID)
	if err != nil {
		t.Fatal(err)
	}
	desiredDigest, liveDigest, differences, err := compareFlowSnapshots(desired, live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(differences) != 0 {
		t.Fatalf("downloaded snapshot drifted immediately: desired=%s live=%s differences=%#v\nlive=%s", desiredDigest, liveDigest, differences, live)
	}
}
