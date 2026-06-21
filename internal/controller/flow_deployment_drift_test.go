package controller

import (
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
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

func TestBlueGreenRolloutIsRejectedBeforeMutation(t *testing.T) {
	deployment := &nifiv1alpha1.NiFiFlowDeployment{
		Spec:   nifiv1alpha1.NiFiFlowDeploymentSpec{Rollout: nifiv1alpha1.RolloutStrategy{Strategy: "BlueGreen"}},
		Status: nifiv1alpha1.NiFiFlowDeploymentStatus{ProcessGroupID: "pg-1"},
	}
	prepared, err := (&NiFiFlowDeploymentReconciler{}).prepareFlowRollout(t.Context(), deployment, "https://nifi.example.com", "v2", "sha256:new")
	if err == nil || prepared {
		t.Fatalf("prepared/error = %v/%v, want explicit rejection", prepared, err)
	}
}
