package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func canonicalFlowSnapshot(snapshot *runtime.RawExtension, targetName string) (json.RawMessage, string, error) {
	if snapshot == nil {
		return nil, "", nil
	}
	raw := snapshot.Raw
	if len(raw) == 0 && snapshot.Object != nil {
		marshaled, err := json.Marshal(snapshot.Object)
		if err != nil {
			return nil, "", err
		}
		raw = marshaled
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, "", fmt.Errorf("snapshot is empty")
	}

	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded map[string]any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, "", fmt.Errorf("snapshot is not valid JSON: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, "", fmt.Errorf("snapshot must contain exactly one JSON value")
	}
	if wrapped, ok := decoded["versionedFlowSnapshot"].(map[string]any); ok {
		decoded = wrapped
	}
	flowContents, ok := decoded["flowContents"].(map[string]any)
	if !ok {
		return nil, "", fmt.Errorf("snapshot must contain an object field named flowContents")
	}
	canonicalSource, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	digest := fmt.Sprintf("sha256:%x", sha256.Sum256(canonicalSource))
	if targetName != "" {
		flowContents["name"] = targetName
	}
	canonicalTarget, err := json.Marshal(decoded)
	if err != nil {
		return nil, "", err
	}
	return canonicalTarget, digest, nil
}

func resolvedFlowBundleArtifact(ctx context.Context, c client.Client, resolver flowartifact.Resolver, bundle *nifiv1alpha1.NiFiFlowBundle) (string, string, error) {
	_, artifactRevision, digest, err := resolvedFlowArtifact(ctx, c, resolver, bundle.Namespace, &bundle.Spec.Source, "")
	if err != nil {
		return "", "", err
	}
	resolvedRevision := bundle.Spec.Version
	if resolvedRevision == "" {
		resolvedRevision = artifactRevision
	}
	if resolvedRevision == "" {
		resolvedRevision = digest
	}
	return digest, resolvedRevision, nil
}

func resolvedFlowDeploymentSnapshot(ctx context.Context, c client.Client, resolver flowartifact.Resolver, deployment *nifiv1alpha1.NiFiFlowDeployment) (json.RawMessage, string, string, error) {
	version := deployment.Spec.Source.Version
	var source *nifiv1alpha1.FlowBundleSource
	sourceNamespace := deployment.Namespace
	expectedDigest := ""
	if deployment.Spec.Source.BundleRef != nil {
		ref := *deployment.Spec.Source.BundleRef
		bundle := &nifiv1alpha1.NiFiFlowBundle{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(deployment.Namespace, ref)}
		if err := c.Get(ctx, key, bundle); err != nil {
			return nil, "", "", err
		}
		source = &bundle.Spec.Source
		sourceNamespace = bundle.Namespace
		expectedDigest = bundle.Status.ArtifactDigest
		if version == "" {
			version = bundle.Status.ResolvedRevision
		}
		if version == "" {
			version = bundle.Spec.Version
		}
	} else {
		source = deployment.Spec.Source.Inline
	}
	if source == nil {
		return nil, "", "", nil
	}
	snapshot, artifactRevision, digest, err := resolvedFlowArtifact(ctx, c, resolver, sourceNamespace, source, deployment.Spec.Target.ProcessGroupName)
	if err != nil {
		return nil, "", "", err
	}
	if expectedDigest != "" && expectedDigest != digest {
		return nil, "", "", fmt.Errorf("resolved artifact digest %s does not match ready bundle digest %s", digest, expectedDigest)
	}
	if version == "" {
		version = artifactRevision
	}
	if version == "" {
		version = digest
	}
	return snapshot, version, digest, nil
}

func resolvedFlowArtifact(ctx context.Context, c client.Client, resolver flowartifact.Resolver, namespace string, source *nifiv1alpha1.FlowBundleSource, targetName string) (json.RawMessage, string, string, error) {
	if source == nil {
		return nil, "", "", fmt.Errorf("flow artifact source is not configured")
	}
	registryURI := ""
	if source.Registry != nil {
		ref := source.Registry.RegistryClientRef
		registryClient := &nifiv1alpha1.NiFiRegistryClient{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(namespace, ref)}
		if err := c.Get(ctx, key, registryClient); err != nil {
			return nil, "", "", err
		}
		if registryClient.Spec.Type != "" && registryClient.Spec.Type != nifiv1alpha1.RegistryClientTypeNiFiRegistry {
			return nil, "", "", fmt.Errorf("referenced NiFiRegistryClient %s has type %s; direct snapshot fetching requires NiFiRegistry", key.String(), registryClient.Spec.Type)
		}
		registryURI = registryClient.Spec.URI
		if registryURI == "" {
			return nil, "", "", fmt.Errorf("referenced NiFiRegistryClient %s has no URI", key.String())
		}
	}
	if resolver == nil {
		resolver = flowartifact.DefaultResolver{}
	}
	credentials, err := resolvedFlowArtifactCredentials(ctx, c, namespace, source)
	if err != nil {
		return nil, "", "", err
	}
	verification, err := resolvedFlowArtifactVerification(ctx, c, namespace, source)
	if err != nil {
		return nil, "", "", err
	}
	artifact, err := resolver.Resolve(ctx, flowartifact.Request{Source: *source, RegistryURI: registryURI, Credentials: credentials, Verification: verification})
	if err != nil {
		return nil, "", "", err
	}
	if artifact == nil {
		return nil, "", "", fmt.Errorf("artifact resolver returned no flow snapshot")
	}
	snapshot, digest, err := canonicalFlowSnapshot(&artifact.Snapshot, targetName)
	if err != nil {
		return nil, "", "", err
	}
	return snapshot, artifact.Revision, digest, nil
}

func resolvedFlowArtifactCredentials(ctx context.Context, c client.Client, namespace string, source *nifiv1alpha1.FlowBundleSource) (flowartifact.Credentials, error) {
	var declared *nifiv1alpha1.FlowArtifactCredentials
	switch {
	case source.Git != nil:
		declared = source.Git.Credentials
	case source.OCI != nil:
		declared = source.OCI.Credentials
	case source.Registry != nil:
		declared = source.Registry.Credentials
	}
	if declared == nil {
		return flowartifact.Credentials{}, nil
	}
	// Reject auth methods scoped to a different source kind rather than silently ignoring a
	// security credential: SSH is Git-only, and client certificates / OIDC are for the HTTPS
	// registry (and OCI, for the client certificate) rather than Git.
	if declared.SSHPrivateKeySecretKeyRef != nil && source.Git == nil {
		return flowartifact.Credentials{}, fmt.Errorf("credentials.sshPrivateKeySecretKeyRef is only supported for git sources")
	}
	if (declared.ClientCertificateSecretKeyRef != nil || declared.ClientKeySecretKeyRef != nil) && source.Git != nil {
		return flowartifact.Credentials{}, fmt.Errorf("credentials client certificate authentication is not supported for git sources; use SSH or HTTPS token authentication")
	}
	resolved := flowartifact.Credentials{InsecureSkipVerify: declared.InsecureSkipVerify, SSHInsecureIgnoreHostKey: declared.SSHInsecureIgnoreHostKey}
	values := []struct {
		ref    *nifiv1alpha1.SecretKeyRef
		target *string
		trim   bool
	}{
		{declared.UsernameSecretKeyRef, &resolved.Username, false},
		{declared.PasswordSecretKeyRef, &resolved.Password, false},
		{declared.TokenSecretKeyRef, &resolved.Token, true},
	}
	for _, value := range values {
		if value.ref == nil {
			continue
		}
		data, err := secretKeyValue(ctx, c, namespace, value.ref)
		if err != nil {
			return flowartifact.Credentials{}, err
		}
		*value.target = string(data)
		if value.trim {
			*value.target = strings.TrimSpace(*value.target)
		}
	}
	// Byte-valued material (CA bundle, SSH key/known-hosts, client certificate) is used verbatim.
	byteValues := []struct {
		ref    *nifiv1alpha1.SecretKeyRef
		target *[]byte
	}{
		{declared.CASecretKeyRef, &resolved.CAData},
		{declared.SSHPrivateKeySecretKeyRef, &resolved.SSHPrivateKey},
		{declared.SSHPrivateKeyPassphraseSecretKeyRef, &resolved.SSHPrivateKeyPassphrase},
		{declared.SSHKnownHostsSecretKeyRef, &resolved.SSHKnownHosts},
		{declared.ClientCertificateSecretKeyRef, &resolved.ClientCertData},
		{declared.ClientKeySecretKeyRef, &resolved.ClientKeyData},
	}
	for _, value := range byteValues {
		if value.ref == nil {
			continue
		}
		data, err := secretKeyValue(ctx, c, namespace, value.ref)
		if err != nil {
			return flowartifact.Credentials{}, err
		}
		*value.target = data
	}
	if declared.OIDC != nil {
		if source.Registry == nil {
			return flowartifact.Credentials{}, fmt.Errorf("credentials.oidc is only supported for registry sources")
		}
		token, err := flowArtifactOIDCToken(ctx, c, namespace, declared.OIDC, resolved.CAData, declared.InsecureSkipVerify)
		if err != nil {
			return flowartifact.Credentials{}, err
		}
		resolved.Token = token
	}
	return resolved, nil
}

// flowArtifactOIDCToken performs the OAuth2 client-credentials grant and returns a bearer
// token for a registry source. The token endpoint is contacted with the source's CA/skip-verify
// settings so a privately-issued token endpoint is trusted the same way as the registry.
func flowArtifactOIDCToken(ctx context.Context, c client.Client, namespace string, oidc *nifiv1alpha1.FlowArtifactOIDC, caData []byte, insecureSkipVerify bool) (string, error) {
	clientID, err := secretKeyValue(ctx, c, namespace, oidc.ClientIDSecretKeyRef)
	if err != nil {
		return "", err
	}
	clientSecret, err := secretKeyValue(ctx, c, namespace, oidc.ClientSecretSecretKeyRef)
	if err != nil {
		return "", err
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if len(caData) > 0 && !roots.AppendCertsFromPEM(caData) {
		return "", fmt.Errorf("credentials.oidc: caSecretKeyRef does not contain a PEM certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, InsecureSkipVerify: insecureSkipVerify} // #nosec G402 -- explicitly configured by the source owner.
	httpClient := &http.Client{Timeout: 30 * time.Second, Transport: transport}

	config := clientcredentials.Config{
		ClientID:     strings.TrimSpace(string(clientID)),
		ClientSecret: strings.TrimSpace(string(clientSecret)),
		TokenURL:     oidc.TokenURL,
		Scopes:       oidc.Scopes,
	}
	if oidc.Audience != "" {
		config.EndpointParams = url.Values{"audience": {oidc.Audience}}
	}
	token, err := config.Token(context.WithValue(ctx, oauth2.HTTPClient, httpClient))
	if err != nil {
		return "", fmt.Errorf("credentials.oidc: obtain token: %w", err)
	}
	return token.AccessToken, nil
}

// resolvedFlowArtifactVerification loads the cosign public key an OCI source must be signed with, or
// returns nil when signature verification is not configured.
func resolvedFlowArtifactVerification(ctx context.Context, c client.Client, namespace string, source *nifiv1alpha1.FlowBundleSource) (*flowartifact.Verification, error) {
	if source.OCI == nil || source.OCI.Verify == nil || source.OCI.Verify.CosignPublicKeySecretRef == nil {
		return nil, nil
	}
	data, err := secretKeyValue(ctx, c, namespace, source.OCI.Verify.CosignPublicKeySecretRef)
	if err != nil {
		return nil, err
	}
	return &flowartifact.Verification{CosignPublicKeyPEM: data}, nil
}

func (r *NiFiFlowDeploymentReconciler) reconcileSnapshotFlowDeployment(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, parentID string, snapshot json.RawMessage, version string, digest string) (ctrl.Result, error) {
	flowSnapshots := r.FlowSnapshotClient
	if flowSnapshots == nil {
		flowSnapshots = nifi.HTTPFlowSnapshotClient{}
	}
	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}

	// Retire a process group promoted away by a BlueGreen switch one reconcile earlier.
	if deployment.Status.RetiringProcessGroupID != "" {
		if err := r.retireBlueProcessGroup(ctx, deployment, endpoint); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	// Cancel an in-flight rollout on request before any further rollout work.
	if deployment.Spec.Rollout.Cancel && deployment.Status.ActiveRollout != nil {
		return r.reconcileRolloutCancellation(ctx, deployment, endpoint)
	}

	// Resume a post-replace readiness wait for an in-place rollout.
	if active := deployment.Status.ActiveRollout; active != nil && active.Operation == "Rollout" && active.Phase == bgPhaseAwaitingReadiness {
		return r.reconcileRolloutReadiness(ctx, deployment, endpoint, version, digest, snapshot)
	}

	if pending := deployment.Status.LatestReplaceRequest; pending != nil && pending.ID != "" {
		if pending.FailureReason != "" {
			return r.handleFlowReplaceFailure(ctx, deployment, endpoint, flowSnapshots, processGroups, pending, fmt.Errorf("%s", pending.FailureReason))
		}
		if pending.Complete {
			return r.completeFlowReplace(ctx, deployment, endpoint, flowSnapshots, processGroups, pending, snapshot)
		}
		return r.reconcilePendingFlowReplace(ctx, deployment, endpoint, flowSnapshots, processGroups)
	}

	if deployment.Status.ProcessGroupID == "" {
		ensureActiveFlowRollout(deployment, version, digest, "Rollout")
		imported, err := flowSnapshots.ImportProcessGroup(ctx, endpoint, parentID, snapshot)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("failed to import NiFi flow snapshot: %w", err))
		}
		if imported == nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("NiFi returned an empty process group import response"))
		}
		processGroupID := processGroupEntityID(*imported)
		if processGroupID == "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotImportFailed", fmt.Errorf("NiFi did not return an imported process group ID"))
		}
		if err := markFlowDeploymentSnapshotImported(ctx, r.Client, deployment, processGroupID, imported.Revision.Version, version, digest); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}

	existing, err := processGroups.GetProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiGetFailed", fmt.Errorf("failed to get imported process group: %w", err))
	}
	if existing == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiGetFailed", fmt.Errorf("NiFi returned an empty imported process group response"))
	}

	if rollbackBlocksTarget(deployment, digest) {
		return rolloutRequeue(), nil
	}

	forceReplace := false
	desiredContentDigest := ""
	liveContentDigest := ""
	if deployment.Status.ArtifactDigest == digest && deployment.Status.DeployedVersion == version {
		mode := resolvedDriftPolicy(deployment)
		if mode == nifiv1alpha1.DriftPolicyIgnore {
			_, desiredContentDigest, err = normalizeFlowSnapshot(snapshot, deployment.Spec.DriftPolicy.IgnoreFields)
			if err != nil {
				return r.snapshotDeploymentFailed(ctx, deployment, "DriftCheckFailed", err)
			}
			liveContentDigest = desiredContentDigest
		} else {
			liveSnapshot, downloadErr := r.snapshotReader().DownloadProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
			if downloadErr != nil {
				return r.snapshotDeploymentFailed(ctx, deployment, "DriftCheckFailed", fmt.Errorf("download live NiFi flow: %w", downloadErr))
			}
			var differences []string
			desiredContentDigest, liveContentDigest, differences, err = compareFlowSnapshots(snapshot, liveSnapshot, deployment.Spec.DriftPolicy.IgnoreFields)
			if err != nil {
				return r.snapshotDeploymentFailed(ctx, deployment, "DriftCheckFailed", err)
			}
			if len(differences) > 0 {
				switch mode {
				case nifiv1alpha1.DriftPolicyReconcile:
					forceReplace = true
				case nifiv1alpha1.DriftPolicyWarn, nifiv1alpha1.DriftPolicyFail:
					if err := r.markSnapshotDeploymentDrift(ctx, deployment, desiredContentDigest, liveContentDigest, differences, mode); err != nil {
						return ctrl.Result{}, err
					}
					return rolloutRequeue(), nil
				}
			}
		}
		if !forceReplace {
			current, metadataErr := r.reconcileSnapshotDeploymentMetadata(ctx, deployment, endpoint, processGroups, existing)
			if metadataErr != nil {
				return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotMetadataFailed", metadataErr)
			}
			needsCompletion := deployment.Status.LastSuccessful == nil || deployment.Status.LastSuccessful.Digest != digest || deployment.Status.ActiveRollout != nil
			statusChanged := !flowDeploymentStatusMatches(deployment, deployment.Status.ProcessGroupID, current.Revision.Version, version, digest, "InSync") ||
				deployment.Status.DesiredContentDigest != desiredContentDigest || deployment.Status.LiveContentDigest != liveContentDigest || deployment.Status.Drift.Status != "InSync"
			if needsCompletion {
				if err := r.finalizeFlowRolloutState(ctx, deployment, endpoint); err != nil {
					return r.snapshotDeploymentFailed(ctx, deployment, "RolloutFinalizeFailed", err)
				}
			}
			if needsCompletion || statusChanged {
				if err := r.markSnapshotDeploymentInSync(ctx, deployment, deployment.Status.ProcessGroupID, current.Revision.Version, version, digest, desiredContentDigest, liveContentDigest, snapshot); err != nil {
					return ctrl.Result{}, err
				}
			}
			return rolloutRequeue(), nil
		}
	}

	// Transactional BlueGreen deploys a candidate beside the live group and switches the
	// external boundary connections, rather than replacing the live group's contents.
	if resolvedRolloutStrategy(deployment) == "BlueGreen" {
		return r.reconcileBlueGreenRollout(ctx, deployment, endpoint, parentID, snapshot, version, digest)
	}

	prepared, err := r.prepareFlowRollout(ctx, deployment, endpoint, version, digest)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RolloutPreparationFailed", err)
	}
	if !prepared {
		return ctrl.Result{RequeueAfter: time.Second}, nil
	}
	snapshotConfigMap, err := r.persistFlowDeploymentSnapshot(ctx, deployment, snapshot, digest)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RolloutCheckpointFailed", err)
	}
	if deployment.Status.ActiveRollout != nil {
		deployment.Status.ActiveRollout.Phase = "Replacing"
	}
	{
		request, err := flowSnapshots.CreateProcessGroupReplaceRequest(ctx, endpoint, deployment.Status.ProcessGroupID, existing.Revision.Version, snapshot)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCreateFailed", fmt.Errorf("failed to create NiFi flow replace request: %w", err))
		}
		status := flowReplaceRequestStatus(request, digest, version, "Rollout", snapshotConfigMap)
		if status.ID == "" {
			return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCreateFailed", fmt.Errorf("NiFi did not return a flow replace request ID"))
		}
		deployment.Status.LatestReplaceRequest = status
		if status.FailureReason != "" {
			if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err == nil {
				status.ID = ""
			}
			return r.handleFlowReplaceFailure(ctx, deployment, endpoint, flowSnapshots, processGroups, status, fmt.Errorf("%s", status.FailureReason))
		}
		if status.Complete {
			if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
}

func (r *NiFiFlowDeploymentReconciler) reconcilePendingFlowReplace(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, flowSnapshots nifi.FlowSnapshotClient, processGroups nifi.ProcessGroupClient) (ctrl.Result, error) {
	pending := deployment.Status.LatestReplaceRequest
	request, err := flowSnapshots.GetProcessGroupReplaceRequest(ctx, endpoint, pending.ID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceGetFailed", fmt.Errorf("failed to get NiFi flow replace request: %w", err))
	}
	status := flowReplaceRequestStatus(request, pending.TargetDigest, pending.TargetVersion, pending.Operation, pending.SnapshotConfigMap)
	if status.ID == "" {
		status.ID = pending.ID
	}
	deployment.Status.LatestReplaceRequest = status
	if status.FailureReason != "" {
		if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err == nil {
			status.ID = ""
		}
		return r.handleFlowReplaceFailure(ctx, deployment, endpoint, flowSnapshots, processGroups, status, fmt.Errorf("%s", status.FailureReason))
	}
	if !status.Complete {
		if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func (r *NiFiFlowDeploymentReconciler) completeFlowReplace(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, flowSnapshots nifi.FlowSnapshotClient, processGroups nifi.ProcessGroupClient, status *nifiv1alpha1.FlowReplaceRequestStatus, desiredSnapshot json.RawMessage) (ctrl.Result, error) {
	existing, err := processGroups.GetProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiRefreshFailed", fmt.Errorf("failed to refresh replaced process group: %w", err))
	}
	if existing == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "NiFiRefreshFailed", fmt.Errorf("NiFi returned an empty replaced process group response"))
	}
	current, err := r.reconcileSnapshotDeploymentMetadata(ctx, deployment, endpoint, processGroups, existing)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "SnapshotMetadataFailed", err)
	}
	if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, status.ID); err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceCleanupFailed", fmt.Errorf("failed to clean up NiFi flow replace request: %w", err))
	}
	status.Complete = true
	status.ID = ""
	deployment.Status.LatestReplaceRequest = status
	if err := r.finalizeFlowRolloutState(ctx, deployment, endpoint); err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RolloutFinalizeFailed", err)
	}
	targetSnapshot := []byte(desiredSnapshot)
	if status.SnapshotConfigMap != "" {
		checkpoint, checkpointErr := r.flowDeploymentSnapshotFromConfigMap(ctx, deployment.Namespace, status.SnapshotConfigMap)
		if checkpointErr != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "RolloutCheckpointMissing", checkpointErr)
		}
		targetSnapshot = checkpoint
	}
	if status.Operation == "Rollback" {
		rollbackSnapshot, history, err := r.rollbackSnapshot(ctx, deployment)
		if err != nil {
			return r.snapshotDeploymentFailed(ctx, deployment, "RollbackSnapshotMissing", err)
		}
		return ctrl.Result{}, r.markSnapshotDeploymentRolledBack(ctx, deployment, current.Revision.Version, history, rollbackSnapshot)
	}
	// Gate marking the rollout in sync on the deployed flow becoming healthy.
	if readinessGateEnabled(deployment) {
		return r.enterRolloutReadiness(ctx, deployment, status.TargetVersion, status.TargetDigest)
	}
	if result, err := r.finalizeSuccessfulRollout(ctx, deployment, endpoint, deployment.Status.ProcessGroupID, status.TargetVersion, status.TargetDigest, targetSnapshot); err != nil {
		return result, err
	}
	return rolloutRequeue(), nil
}

func (r *NiFiFlowDeploymentReconciler) reconcileSnapshotDeploymentMetadata(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, endpoint string, processGroups nifi.ProcessGroupClient, existing *nifi.ProcessGroupEntity) (*nifi.ProcessGroupEntity, error) {
	update := *existing
	needsUpdate := false
	if deployment.Spec.Target.ProcessGroupName != "" && update.Component.Name != deployment.Spec.Target.ProcessGroupName {
		update.Component.Name = deployment.Spec.Target.ProcessGroupName
		needsUpdate = true
	}
	if deployment.Spec.ParameterContextRef != nil {
		ref := *deployment.Spec.ParameterContextRef
		parameterContext := &nifiv1alpha1.NiFiParameterContext{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(deployment.Namespace, ref)}
		if err := r.Get(ctx, key, parameterContext); err != nil {
			return nil, err
		}
		if componentReferenceID(update.Component.ParameterContext) != parameterContext.Status.NiFiID {
			update.Component.ParameterContext = &nifi.ComponentReference{ID: parameterContext.Status.NiFiID}
			needsUpdate = true
		}
	}
	if !needsUpdate {
		return existing, nil
	}
	update.ID = processGroupEntityID(*existing)
	update.Component.ID = update.ID
	updated, err := processGroups.UpdateProcessGroup(ctx, endpoint, update)
	if err != nil {
		return nil, fmt.Errorf("failed to update imported process group metadata: %w", err)
	}
	if updated == nil {
		return existing, nil
	}
	return updated, nil
}

func (r *NiFiFlowDeploymentReconciler) snapshotDeploymentFailed(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, reason string, failure error) (ctrl.Result, error) {
	message := failure.Error()
	if shouldMarkFlowDeploymentNotReady(deployment, reason, message) {
		if err := markFlowDeploymentNotReady(ctx, r.Client, deployment, reason, message); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *NiFiFlowDeploymentReconciler) handleFlowReplaceFailure(
	ctx context.Context,
	deployment *nifiv1alpha1.NiFiFlowDeployment,
	endpoint string,
	flowSnapshots nifi.FlowSnapshotClient,
	processGroups nifi.ProcessGroupClient,
	failed *nifiv1alpha1.FlowReplaceRequestStatus,
	failure error,
) (ctrl.Result, error) {
	if failed.ID != "" {
		if err := flowSnapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, failed.ID); err == nil {
			failed.ID = ""
		}
	}
	r.recordFailedFlowDeployment(deployment, failed.TargetVersion, failed.TargetDigest, failed.SnapshotConfigMap, failure.Error())
	r.trimFlowDeploymentHistory(ctx, deployment)
	if failed.Operation == "Rollback" {
		deployment.Status.LatestReplaceRequest = failed
		deployment.Status.ActiveRollout = nil
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackFailed", fmt.Errorf("automatic rollback failed: %w", failure))
	}
	// Re-attempt the rollout before falling back to rollback when retries remain.
	if retried, result, err := r.retryRolloutIfAllowed(ctx, deployment, failure.Error()); retried {
		return result, err
	}
	if !deployment.Spec.Rollback.Enabled {
		deployment.Status.LatestReplaceRequest = failed
		deployment.Status.ActiveRollout = nil
		return r.snapshotDeploymentFailed(ctx, deployment, "FlowReplaceFailed", failure)
	}

	rollbackSnapshot, history, err := r.rollbackSnapshot(ctx, deployment)
	if err != nil {
		deployment.Status.LatestReplaceRequest = failed
		deployment.Status.ActiveRollout = nil
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackUnavailable", fmt.Errorf("%v; %w", failure, err))
	}
	existing, err := processGroups.GetProcessGroup(ctx, endpoint, deployment.Status.ProcessGroupID)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackPreparationFailed", fmt.Errorf("refresh process group for rollback: %w", err))
	}
	if existing == nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackPreparationFailed", fmt.Errorf("NiFi returned no process group for rollback"))
	}

	request, err := flowSnapshots.CreateProcessGroupReplaceRequest(ctx, endpoint, deployment.Status.ProcessGroupID, existing.Revision.Version, rollbackSnapshot)
	if err != nil {
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackCreateFailed", fmt.Errorf("create automatic rollback request: %w", err))
	}
	status := flowReplaceRequestStatus(request, history.Digest, history.Version, "Rollback", history.SnapshotConfigMap)
	if status.ID == "" {
		return r.snapshotDeploymentFailed(ctx, deployment, "RollbackCreateFailed", fmt.Errorf("NiFi did not return an automatic rollback request ID"))
	}
	deployment.Status.LastRollback = &nifiv1alpha1.FlowRollbackStatus{
		FailedGeneration: deployment.Generation,
		FailedVersion:    failed.TargetVersion,
		FailedDigest:     failed.TargetDigest,
		RestoredVersion:  history.Version,
		RestoredDigest:   history.Digest,
		Message:          failure.Error(),
	}
	active := ensureActiveFlowRollout(deployment, history.Version, history.Digest, "Rollback")
	active.Phase = "RollingBack"
	active.Strategy = resolvedRolloutStrategy(deployment)
	deployment.Status.LatestReplaceRequest = status
	if status.FailureReason != "" {
		return r.handleFlowReplaceFailure(ctx, deployment, endpoint, flowSnapshots, processGroups, status, fmt.Errorf("%s", status.FailureReason))
	}
	if err := markFlowDeploymentReplaceRunning(ctx, r.Client, deployment, status); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

func flowReplaceRequestStatus(entity *nifi.ProcessGroupReplaceRequestEntity, targetDigest string, targetVersion string, operation string, snapshotConfigMap string) *nifiv1alpha1.FlowReplaceRequestStatus {
	status := &nifiv1alpha1.FlowReplaceRequestStatus{
		TargetDigest: targetDigest, TargetVersion: targetVersion, Operation: operation, SnapshotConfigMap: snapshotConfigMap,
	}
	if entity == nil {
		return status
	}
	status.ID = entity.Request.RequestID
	status.State = entity.Request.State
	status.Complete = entity.Request.Complete
	status.FailureReason = entity.Request.FailureReason
	status.PercentCompleted = entity.Request.PercentCompleted
	return status
}
