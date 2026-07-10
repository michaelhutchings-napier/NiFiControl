package flowartifact

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
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

func TestDefaultResolverResolvesOCIImageSnapshot(t *testing.T) {
	server := httptest.NewServer(registry.New())
	defer server.Close()
	reference, err := name.ParseReference(strings.TrimPrefix(server.URL, "http://")+"/flows/payments:v1", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	var layerContents bytes.Buffer
	tarWriter := tar.NewWriter(&layerContents)
	payload := []byte("flowContents:\n  name: Payments\n  processors: []\n")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "flows/payments.yaml", Mode: 0o600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromReader(bytes.NewReader(layerContents.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(reference, image); err != nil {
		t.Fatal(err)
	}
	wantDigest, err := image.Digest()
	if err != nil {
		t.Fatal(err)
	}

	artifact, err := (DefaultResolver{AllowInsecureOCI: true}).Resolve(t.Context(), Request{Source: nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{
		Image: reference.Name(), Path: "flows/payments.yaml",
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if artifact.Revision != wantDigest.String() {
		t.Fatalf("revision = %q, want %q", artifact.Revision, wantDigest)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(artifact.Snapshot.Raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot["flowContents"].(map[string]any)["name"] != "Payments" {
		t.Fatalf("snapshot = %#v", snapshot)
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

func TestDefaultResolverAuthenticatesRegistryRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		username, password, ok := r.BasicAuth()
		if !ok || username != "registry-user" || password != "registry-password" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"snapshotMetadata":{"version":4},"flowContents":{"name":"Payments"}}`))
	}))
	defer server.Close()

	_, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL,
		Credentials: Credentials{Username: "registry-user", Password: "registry-password"},
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1", Version: "4",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGitAuthenticationUsesTokenAsPassword(t *testing.T) {
	method, err := gitAuthentication("https://example.com/org/repo.git", Credentials{Token: "git-token"})
	if err != nil {
		t.Fatal(err)
	}
	auth, ok := method.(*githttp.BasicAuth)
	if !ok {
		t.Fatalf("auth type = %T", method)
	}
	if auth.Username != "oauth2" || auth.Password != "git-token" {
		t.Fatalf("auth = %#v", auth)
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

// TestDefaultResolverAuthenticatesOCIPull proves a private OCI source: the registry rejects an
// anonymous pull and the snapshot resolves only once the resolver presents the configured
// credentials.
func TestDefaultResolverAuthenticatesOCIPull(t *testing.T) {
	const user, pass = "oci-user", "oci-pass"
	inner := registry.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if u, p, ok := r.BasicAuth(); !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="registry"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		inner.ServeHTTP(w, r)
	}))
	defer server.Close()

	reference, err := name.ParseReference(strings.TrimPrefix(server.URL, "http://")+"/flows/payments:v1", name.Insecure)
	if err != nil {
		t.Fatal(err)
	}
	var layerContents bytes.Buffer
	tarWriter := tar.NewWriter(&layerContents)
	payload := []byte("flowContents:\n  name: Payments\n  processors: []\n")
	if err := tarWriter.WriteHeader(&tar.Header{Name: "flow.json", Mode: 0o600, Size: int64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if _, err := tarWriter.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := tarWriter.Close(); err != nil {
		t.Fatal(err)
	}
	layer, err := tarball.LayerFromReader(bytes.NewReader(layerContents.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	image, err := mutate.AppendLayers(empty.Image, layer)
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(reference, image, remote.WithAuth(&authn.Basic{Username: user, Password: pass})); err != nil {
		t.Fatal(err)
	}

	source := nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{Image: reference.Name()}}

	if _, err := (DefaultResolver{AllowInsecureOCI: true}).Resolve(t.Context(), Request{Source: source}); err == nil {
		t.Fatal("expected an authentication failure pulling a private OCI source without credentials")
	}

	artifact, err := (DefaultResolver{AllowInsecureOCI: true}).Resolve(t.Context(), Request{
		Source:      source,
		Credentials: Credentials{Username: user, Password: pass},
	})
	if err != nil {
		t.Fatalf("authenticated OCI pull failed: %v", err)
	}
	var snapshot map[string]any
	if err := json.Unmarshal(artifact.Snapshot.Raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot["flowContents"].(map[string]any)["name"] != "Payments" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestDefaultResolverUsesRegistryBearerToken(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer registry-token" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`{"snapshotMetadata":{"version":4},"flowContents":{"name":"Payments"}}`))
	}))
	defer server.Close()

	if _, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL,
		Credentials: Credentials{Token: "registry-token"},
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1", Version: "4",
		}},
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultResolverRejectsUnauthenticatedRegistry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()

	_, err := (DefaultResolver{}).Resolve(t.Context(), Request{
		RegistryURI: server.URL,
		Source: nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "bucket-1", FlowID: "flow-1",
		}},
	})
	if err == nil {
		t.Fatal("expected an unauthorized NiFi Registry error")
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
