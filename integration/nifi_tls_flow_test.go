//go:build integration

package integration_test

import (
	"os"
	"testing"
	"time"

	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
)

// TestNiFi210MutualTLSFlowImportAndReplace drives a real Apache NiFi 2.10 instance
// secured with HTTPS and client-certificate authentication. The operator's mTLS REST
// client must reach the API, import a flow, and replace it. hack/test-nifi-2.10-tls.sh
// provisions the secured container and sets the environment below.
func TestNiFi210MutualTLSFlowImportAndReplace(t *testing.T) {
	baseURI := os.Getenv("NIFI_TLS_API_URI")
	if baseURI == "" {
		t.Skip("NIFI_TLS_API_URI is not set")
	}
	caData := readFileEnv(t, "NIFI_TLS_CA")
	certData := readFileEnv(t, "NIFI_TLS_CLIENT_CERT")
	keyData := readFileEnv(t, "NIFI_TLS_CLIENT_KEY")

	client, err := nifi.NewHTTPClient(nifi.HTTPClientConfig{
		BaseURI:        baseURI,
		CAData:         caData,
		ClientCertData: certData,
		ClientKeyData:  keyData,
		Timeout:        30 * time.Second,
	})
	if err != nil {
		t.Fatalf("build mTLS client: %v", err)
	}
	if err := nifi.RegisterHTTPClient(baseURI, client); err != nil {
		t.Fatal(err)
	}

	// Prove reachability over mTLS first.
	if err := (nifi.HTTPReachabilityChecker{Client: client}).CheckReachable(t.Context(), baseURI, 30*time.Second); err != nil {
		t.Fatalf("NiFi API is not reachable over mTLS: %v", err)
	}

	processGroups := nifi.HTTPProcessGroupClient{}
	snapshots := nifi.HTTPFlowSnapshotClient{}

	imported, err := snapshots.ImportProcessGroup(t.Context(), baseURI, "root", integrationSnapshot("NiFiControl mTLS integration", "initial snapshot"))
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
	replace, err := snapshots.CreateProcessGroupReplaceRequest(t.Context(), baseURI, processGroupID, current.Revision.Version, integrationSnapshot("NiFiControl mTLS integration", "replacement snapshot"))
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
}

func readFileEnv(t *testing.T, name string) []byte {
	t.Helper()
	path := os.Getenv(name)
	if path == "" {
		t.Skipf("%s is not set", name)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s (%s): %v", name, path, err)
	}
	return data
}
