package flowartifact

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/storage/memory"
	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

const (
	defaultSnapshotPath = "flow.json"
	maxSnapshotBytes    = 16 << 20
	defaultHTTPTimeout  = 30 * time.Second
)

type Request struct {
	Source      nifiv1alpha1.FlowBundleSource
	RegistryURI string
}

type Artifact struct {
	Snapshot runtime.RawExtension
	Revision string
}

type Resolver interface {
	Resolve(ctx context.Context, request Request) (*Artifact, error)
}

type DefaultResolver struct {
	HTTPClient *http.Client
}

func (r DefaultResolver) Resolve(ctx context.Context, request Request) (*Artifact, error) {
	switch {
	case request.Source.Snapshot != nil:
		return &Artifact{Snapshot: *request.Source.Snapshot.DeepCopy()}, nil
	case request.Source.Git != nil:
		return r.resolveGit(ctx, *request.Source.Git)
	case request.Source.Registry != nil:
		return r.resolveRegistry(ctx, request.RegistryURI, *request.Source.Registry)
	case request.Source.OCI != nil:
		return nil, fmt.Errorf("OCI flow artifact fetching is not implemented")
	default:
		return nil, fmt.Errorf("flow artifact source is not configured")
	}
}

func (r DefaultResolver) resolveGit(ctx context.Context, source nifiv1alpha1.GitSource) (*Artifact, error) {
	referenceName, err := resolveRemoteReference(ctx, source.URL, source.Ref)
	if err != nil {
		return nil, err
	}
	directory, err := os.MkdirTemp("", "nificontrol-flow-*")
	if err != nil {
		return nil, fmt.Errorf("create temporary Git checkout: %w", err)
	}
	defer os.RemoveAll(directory)

	options := &git.CloneOptions{URL: source.URL, Depth: 1}
	if referenceName != "" {
		options.ReferenceName = referenceName
		options.SingleBranch = true
	}
	repository, err := git.PlainCloneContext(ctx, directory, false, options)
	if err != nil {
		return nil, fmt.Errorf("clone Git flow source: %w", err)
	}
	head, err := repository.Head()
	if err != nil {
		return nil, fmt.Errorf("resolve checked out Git revision: %w", err)
	}
	if plumbing.IsHash(source.Ref) && head.Hash().String() != source.Ref {
		return nil, fmt.Errorf("checked out Git revision %s does not match requested commit %s", head.Hash(), source.Ref)
	}

	snapshotPath, err := secureSnapshotPath(directory, source.Path)
	if err != nil {
		return nil, err
	}
	payload, err := readBoundedFile(snapshotPath)
	if err != nil {
		return nil, fmt.Errorf("read Git flow snapshot %q: %w", source.Path, err)
	}
	normalized, err := normalizeSnapshot(payload)
	if err != nil {
		return nil, fmt.Errorf("decode Git flow snapshot %q: %w", source.Path, err)
	}
	return &Artifact{Snapshot: runtime.RawExtension{Raw: normalized}, Revision: head.Hash().String()}, nil
}

func (r DefaultResolver) resolveRegistry(ctx context.Context, registryURI string, source nifiv1alpha1.RegistryFlowSource) (*Artifact, error) {
	endpoint, err := registrySnapshotURL(registryURI, source)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	client := r.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	response, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch NiFi Registry flow snapshot: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 512))
		return nil, fmt.Errorf("NiFi Registry returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(message)))
	}
	payload, err := readBounded(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read NiFi Registry flow snapshot: %w", err)
	}
	normalized, err := normalizeSnapshot(payload)
	if err != nil {
		return nil, fmt.Errorf("decode NiFi Registry flow snapshot: %w", err)
	}
	revision := source.Version
	var metadata struct {
		SnapshotMetadata struct {
			Version *int32 `json:"version"`
		} `json:"snapshotMetadata"`
	}
	if err := json.Unmarshal(normalized, &metadata); err == nil && metadata.SnapshotMetadata.Version != nil {
		revision = strconv.FormatInt(int64(*metadata.SnapshotMetadata.Version), 10)
	}
	return &Artifact{Snapshot: runtime.RawExtension{Raw: normalized}, Revision: revision}, nil
}

func resolveRemoteReference(ctx context.Context, repositoryURL string, requested string) (plumbing.ReferenceName, error) {
	if requested == "" {
		return "", nil
	}
	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{Name: "origin", URLs: []string{repositoryURL}})
	references, err := remote.ListContext(ctx, &git.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list Git flow source references: %w", err)
	}
	candidates := []plumbing.ReferenceName{
		plumbing.ReferenceName(requested),
		plumbing.NewBranchReferenceName(requested),
		plumbing.NewTagReferenceName(requested),
	}
	for _, candidate := range candidates {
		for _, reference := range references {
			if reference.Name() == candidate {
				return reference.Name(), nil
			}
		}
	}
	if plumbing.IsHash(requested) {
		for _, reference := range references {
			if reference.Hash().String() == requested && !strings.HasSuffix(reference.Name().String(), "^{}") {
				return reference.Name(), nil
			}
		}
	}
	return "", fmt.Errorf("Git flow source ref %q was not found", requested)
}

func secureSnapshotPath(root string, configured string) (string, error) {
	if configured == "" {
		configured = defaultSnapshotPath
	}
	clean := filepath.Clean(configured)
	if filepath.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("flow snapshot path %q escapes the Git repository", configured)
	}
	return filepath.Join(root, clean), nil
}

func readBoundedFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return readBounded(file)
}

func readBounded(reader io.Reader) ([]byte, error) {
	payload, err := io.ReadAll(io.LimitReader(reader, maxSnapshotBytes+1))
	if err != nil {
		return nil, err
	}
	if len(payload) > maxSnapshotBytes {
		return nil, fmt.Errorf("flow snapshot exceeds %d bytes", maxSnapshotBytes)
	}
	return payload, nil
}

func normalizeSnapshot(payload []byte) ([]byte, error) {
	if json.Valid(payload) {
		return payload, nil
	}
	normalized, err := yaml.YAMLToJSON(payload)
	if err != nil {
		return nil, err
	}
	if !json.Valid(normalized) {
		return nil, fmt.Errorf("flow snapshot is not valid JSON or YAML")
	}
	return normalized, nil
}

func registrySnapshotURL(baseURI string, source nifiv1alpha1.RegistryFlowSource) (string, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURI, "/"))
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("NiFi Registry URI must include scheme and host")
	}
	basePath := strings.TrimRight(parsed.Path, "/")
	if strings.HasSuffix(basePath, "/nifi-registry") {
		basePath = strings.TrimSuffix(basePath, "/nifi-registry") + "/nifi-registry-api"
	} else if !strings.HasSuffix(basePath, "/nifi-registry-api") {
		basePath += "/nifi-registry-api"
	}
	version := source.Version
	if version == "" || strings.EqualFold(version, "latest") {
		version = "latest"
	}
	parsed.Path = fmt.Sprintf("%s/buckets/%s/flows/%s/versions/%s", basePath, url.PathEscape(source.BucketID), url.PathEscape(source.FlowID), url.PathEscape(version))
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}
