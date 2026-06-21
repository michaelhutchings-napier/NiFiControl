//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

func TestNiFi210FlowSnapshotImportAndReplace(t *testing.T) {
	baseURI := os.Getenv("NIFI_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_API_URI is not set")
	}
	processGroups := nifi.HTTPProcessGroupClient{}
	snapshots := nifi.HTTPFlowSnapshotClient{}
	initial := integrationSnapshot("NiFiControl integration", "initial snapshot")
	imported, err := snapshots.ImportProcessGroup(t.Context(), baseURI, "root", initial)
	if err != nil {
		t.Fatal(err)
	}
	processGroupID := imported.ID
	if processGroupID == "" {
		processGroupID = imported.Component.ID
	}
	if processGroupID == "" {
		t.Fatal("NiFi returned no imported process group ID")
	}
	t.Cleanup(func() {
		current, err := processGroups.GetProcessGroup(t.Context(), baseURI, processGroupID)
		if err == nil && current != nil {
			_ = processGroups.DeleteProcessGroup(t.Context(), baseURI, processGroupID, current.Revision.Version)
		}
	})

	current, err := processGroups.GetProcessGroup(t.Context(), baseURI, processGroupID)
	if err != nil {
		t.Fatal(err)
	}
	replace, err := snapshots.CreateProcessGroupReplaceRequest(t.Context(), baseURI, processGroupID, current.Revision.Version, integrationSnapshot("NiFiControl integration", "replacement snapshot"))
	if err != nil {
		t.Fatal(err)
	}
	requestID := replace.Request.RequestID
	if requestID == "" {
		t.Fatal("NiFi returned no replace request ID")
	}
	defer snapshots.DeleteProcessGroupReplaceRequest(t.Context(), baseURI, requestID)

	deadline := time.Now().Add(90 * time.Second)
	for !replace.Request.Complete && time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		replace, err = snapshots.GetProcessGroupReplaceRequest(t.Context(), baseURI, requestID)
		if err != nil {
			t.Fatal(err)
		}
	}
	if !replace.Request.Complete {
		t.Fatalf("replace request did not complete: %#v", replace.Request)
	}
	if replace.Request.FailureReason != "" {
		t.Fatalf("replace request failed: %s", replace.Request.FailureReason)
	}
	if err := snapshots.DeleteProcessGroupReplaceRequest(t.Context(), baseURI, requestID); err != nil {
		t.Fatal(err)
	}
}

func integrationSnapshot(name string, comments string) json.RawMessage {
	return json.RawMessage(`{
  "snapshotMetadata": {"version": 1, "author": "NiFiControl integration"},
  "flowContents": {
    "identifier": "36fa611a-d45f-49d1-9e21-fc70d14383a2",
    "name": "` + name + `",
    "comments": "` + comments + `",
    "componentType": "PROCESS_GROUP",
    "position": {"x": 0, "y": 0},
    "processors": [],
    "controllerServices": [],
    "processGroups": [],
    "remoteProcessGroups": [],
    "inputPorts": [],
    "outputPorts": [],
    "connections": [],
    "funnels": [],
    "labels": []
  }
}`)
}
