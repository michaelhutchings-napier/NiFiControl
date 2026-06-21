package flowartifact

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
)

func TestDefaultResolverResolvesGitSnapshotAndCommit(t *testing.T) {
	repositoryPath := t.TempDir()
	repository, err := git.PlainInitWithOptions(repositoryPath, &git.PlainInitOptions{
		InitOptions: git.InitOptions{DefaultBranch: plumbing.NewBranchReferenceName("main")},
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshotPath := filepath.Join(repositoryPath, "flows", "payments.yaml")
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(snapshotPath, []byte("flowContents:\n  name: Payments\n  processors: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	worktree, err := repository.Worktree()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worktree.Add("flows/payments.yaml"); err != nil {
		t.Fatal(err)
	}
	commit, err := worktree.Commit("add flow", &git.CommitOptions{Author: &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}})
	if err != nil {
		t.Fatal(err)
	}

	artifact, err := (DefaultResolver{}).Resolve(t.Context(), Request{Source: nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
		URL: repositoryPath, Ref: "main", Path: "flows/payments.yaml",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Revision != commit.String() {
		t.Fatalf("revision = %q, want %q", artifact.Revision, commit)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(artifact.Snapshot.Raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot["flowContents"].(map[string]any)["name"] != "Payments" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestDefaultResolverFetchesLatestRegistrySnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Fatalf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/nifi-registry-api/buckets/bucket-1/flows/flow-1/versions/latest" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"snapshotMetadata":{"version":7},"flowContents":{"name":"Payments"}}`))
	}))
	defer server.Close()

	artifact, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL,
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Revision != "7" {
		t.Fatalf("revision = %q, want 7", artifact.Revision)
	}
}

func TestDefaultResolverFetchesPinnedRegistrySnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nifi-registry-api/buckets/bucket-1/flows/flow-1/versions/4" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"snapshotMetadata":{"version":4},"flowContents":{"name":"Payments"}}`))
	}))
	defer server.Close()

	_, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL + "/nifi-registry-api",
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1", Version: "4",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestDefaultResolverPreservesPinnedVersionWithoutSnapshotMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/nifi-registry-api/buckets/bucket-1/flows/flow-1/versions/4" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"flowContents":{"name":"Payments"}}`))
	}))
	defer server.Close()

	artifact, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL + "/nifi-registry",
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1", Version: "4",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Revision != "4" {
		t.Fatalf("revision = %q, want 4", artifact.Revision)
	}
}

func TestSecureSnapshotPathRejectsTraversal(t *testing.T) {
	if _, err := secureSnapshotPath(t.TempDir(), "../flow.json"); err == nil {
		t.Fatal("expected traversal path to be rejected")
	}
}

func TestDefaultResolverRejectsOversizedRegistrySnapshot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, maxSnapshotBytes+1))
	}))
	defer server.Close()

	_, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL,
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1",
		}},
	})
	if err == nil {
		t.Fatal("expected oversized snapshot error")
	}
}
