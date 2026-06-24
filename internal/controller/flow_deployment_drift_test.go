package controller

import (
	"testing"
)

func TestCompareFlowSnapshotsIgnoresMetadataAndComponentOrder(t *testing.T) {
	desired := []byte(`{
      "snapshotMetadata":{"version":1},
      "flowContents":{"name":"Payments","processors":[
        {"identifier":"b","name":"Second"},
        {"identifier":"a","name":"First"}
      ]}
    }`)
	live := []byte(`{
      "flowContents":{"processors":[
        {"name":"First","identifier":"a"},
        {"name":"Second","identifier":"b"}
      ],"name":"Payments"}
    }`)

	desiredDigest, liveDigest, differences, err := compareFlowSnapshots(desired, live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if desiredDigest != liveDigest || len(differences) != 0 {
		t.Fatalf("digests/differences = %s/%s/%#v", desiredDigest, liveDigest, differences)
	}
}

func TestCompareFlowSnapshotsReportsAndIgnoresConfiguredDrift(t *testing.T) {
	desired := []byte(`{"flowContents":{"name":"Payments","comments":"desired"}}`)
	live := []byte(`{"flowContents":{"name":"Payments","comments":"changed"}}`)

	_, _, differences, err := compareFlowSnapshots(desired, live, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(differences) != 1 || differences[0] != "$.flowContents.comments" {
		t.Fatalf("differences = %#v", differences)
	}
	_, _, differences, err = compareFlowSnapshots(desired, live, []string{"component.comments"})
	if err != nil {
		t.Fatal(err)
	}
	if len(differences) != 0 {
		t.Fatalf("ignored differences = %#v", differences)
	}
}

// BlueGreen rollouts are now handled by the transactional state machine and are routed
// away from the in-place replace path before prepareFlowRollout is reached. The
// end-to-end behaviour is covered in flow_deployment_bluegreen_test.go.
