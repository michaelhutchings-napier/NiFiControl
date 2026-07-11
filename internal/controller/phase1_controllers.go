package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters/finalizers,verbs=update
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services;persistentvolumeclaims,verbs=get;list;watch;create;update;patch;delete
// Kubernetes coordination mode: provision the NiFi pods' ServiceAccount and the Role/RoleBinding
// granting the Lease + ConfigMap access the KubernetesLeaderElectionManager and ConfigMap state
// provider need. The operator already holds leases and configmaps above, so it can grant them.
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=roles;rolebindings,verbs=get;list;watch;create;update;patch;delete
// OpenShift only: grant the node pods 'use' on an SCC (spec.pod.openShiftSCC). The operator
// must hold 'use' itself to grant it; harmless (inert) on non-OpenShift clusters.
// +kubebuilder:rbac:groups=security.openshift.io,resources=securitycontextconstraints,verbs=use
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=storage.k8s.io,resources=csidrivers,verbs=get;list;watch
// +kubebuilder:rbac:groups=networking.k8s.io,resources=ingresses,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiregistryclients,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiregistryclients/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiregistryclients/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparametercontexts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparametercontexts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparametercontexts/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusers/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusergroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusergroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiusergroups/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessgroups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessgroups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessgroups/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups=monitoring.coreos.com,resources=servicemonitors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=certificates;issuers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cert-manager.io,resources=clusterissuers,verbs=get;list;watch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiprocessors/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiinputports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiinputports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiinputports/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifioutputports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifioutputports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifioutputports/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificonnections,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificonnections/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificonnections/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifireportingtasks,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifireportingtasks/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifireportingtasks/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparameterproviders,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparameterproviders/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifiparameterproviders/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififunnels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififunnels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififunnels/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifilabels,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifilabels/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifilabels/finalizers,verbs=update

type NiFiClusterReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	ReachabilityChecker nifi.ReachabilityChecker
	// ClusterNodeClient drives the NiFi cluster API for graceful node offload on scale-down.
	ClusterNodeClient nifi.ClusterNodeClient
	// ControllerConfigClient applies cluster-wide controller settings (maxTimerDrivenThreadCount).
	ControllerConfigClient nifi.ControllerConfigClient
	// Recorder emits Kubernetes Events for notable lifecycle transitions. It is optional;
	// reconcilers constructed without one (notably in unit tests) simply emit no Events.
	Recorder record.EventRecorder
}

func (r *NiFiClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiCluster{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileClusterDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	if resolvedClusterMode(instance) == nifiv1alpha1.ClusterModeInternal {
		return r.reconcileManagedCluster(ctx, instance)
	}
	if instance.Spec.API != nil && instance.Spec.API.URI != "" {
		if err := configureClusterHTTPClient(ctx, r.Client, instance); err != nil {
			return ctrl.Result{}, err
		}
		timeout := time.Duration(0)
		if instance.Spec.API.Timeout != nil {
			timeout = instance.Spec.API.Timeout.Duration
		}
		checker := r.ReachabilityChecker
		if checker == nil {
			checker = nifi.HTTPReachabilityChecker{}
		}
		if err := checker.CheckReachable(ctx, instance.Spec.API.URI, timeout); err != nil {
			if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready {
				return ctrl.Result{}, markClusterUnreachable(ctx, r.Client, instance, err.Error())
			}
			return ctrl.Result{}, nil
		}
		if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Ready || instance.Status.Endpoint != instance.Spec.API.URI {
			return ctrl.Result{}, markClusterReachable(ctx, r.Client, instance)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation {
		return ctrl.Result{}, markClusterAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiCluster{}).
		Watches(&appsv1.StatefulSet{}, handler.EnqueueRequestsFromMapFunc(r.requestsForManagedClusterResource)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(r.requestsForManagedClusterResource)).
		Watches(&policyv1.PodDisruptionBudget{}, handler.EnqueueRequestsFromMapFunc(r.requestsForManagedClusterResource)).
		Watches(&networkingv1.Ingress{}, handler.EnqueueRequestsFromMapFunc(r.requestsForManagedClusterResource)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForAPISecret)).
		Complete(r)
}

type NiFiRegistryClientReconciler struct {
	client.Client
	Scheme               *runtime.Scheme
	RegistryClientClient nifi.RegistryClientClient
}

func (r *NiFiRegistryClientReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiRegistryClient{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileRegistryClientDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markRegistryClientWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkRegistryClientNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markRegistryClientNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	entity, resolvedType, sensitiveKeys, waitingFor, err := r.desiredRegistryClient(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markRegistryClientWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	registryClient := r.RegistryClientClient
	if registryClient == nil {
		registryClient = nifi.HTTPRegistryClientClient{}
	}

	if instance.Status.NiFiID != "" {
		existing, err := registryClient.GetRegistryClient(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi registry client: %v", err)
			if shouldMarkRegistryClientNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingRegistryClient(ctx, instance, endpoint, registryClient, entity, existing, resolvedType, sensitiveKeys)
	}

	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := registryClient.GetRegistryClient(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi registry client: %v", err)
			if shouldMarkRegistryClientNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingRegistryClient(ctx, instance, endpoint, registryClient, entity, existing, resolvedType, sensitiveKeys)
	}

	created, err := registryClient.CreateRegistryClient(ctx, endpoint, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi registry client: %v", err)
		if shouldMarkRegistryClientNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty registry client response."
		if shouldMarkRegistryClientNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := registryClientEntityID(*created)
	if !registryClientStatusMatches(instance, nifiID, created.Revision.Version, resolvedType) {
		return ctrl.Result{}, markRegistryClientReady(ctx, r.Client, instance, nifiID, created.Revision.Version, resolvedType)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiRegistryClientReconciler) reconcileRegistryClientDelete(ctx context.Context, instance *nifiv1alpha1.NiFiRegistryClient) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	registryClient := r.RegistryClientClient
	if registryClient == nil {
		registryClient = nifi.HTTPRegistryClientClient{}
	}
	if err := registryClient.DeleteRegistryClient(ctx, endpoint, instance.Status.NiFiID, instance.Status.Revision.Version); err != nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiRegistryClientReconciler) reconcileExistingRegistryClient(ctx context.Context, instance *nifiv1alpha1.NiFiRegistryClient, endpoint string, registryClient nifi.RegistryClientClient, desired nifi.RegistryClientEntity, existing *nifi.RegistryClientEntity, resolvedType string, sensitiveKeys map[string]bool) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty registry client response."
		if shouldMarkRegistryClientNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nifiID := registryClientEntityID(*existing)
	if registryClientNeedsUpdate(desired, *existing, sensitiveKeys) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := registryClient.UpdateRegistryClient(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi registry client: %v", err)
			if shouldMarkRegistryClientNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			nifiID = registryClientEntityID(*updated)
			return ctrl.Result{}, markRegistryClientReady(ctx, r.Client, instance, nifiID, updated.Revision.Version, resolvedType)
		}
	}

	if !registryClientStatusMatches(instance, nifiID, existing.Revision.Version, resolvedType) {
		return ctrl.Result{}, markRegistryClientReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, resolvedType)
	}
	return ctrl.Result{}, nil
}

// desiredRegistryClient builds the NiFi registry client entity for the resource's type. It maps
// the typed GitHub/GitLab fields (and NiFiRegistry's URI) to NiFi's component properties,
// resolves any token/sensitive Secrets, and returns the set of sensitive property keys (which
// NiFi masks on read and are therefore excluded from drift detection) plus any unresolved
// dependencies to wait on.
func (r *NiFiRegistryClientReconciler) desiredRegistryClient(ctx context.Context, instance *nifiv1alpha1.NiFiRegistryClient) (nifi.RegistryClientEntity, string, map[string]bool, []string, error) {
	resolvedType := registryClientType(instance.Spec.Type)
	properties := map[string]string{}
	sensitiveKeys := map[string]bool{}
	waitingFor := make([]string, 0)

	switch instance.Spec.Type {
	case nifiv1alpha1.RegistryClientTypeGitHub:
		gh := instance.Spec.GitHub
		if gh == nil {
			waitingFor = append(waitingFor, "github")
			break
		}
		properties["Repository Owner"] = gh.RepositoryOwner
		properties["Repository Name"] = gh.RepositoryName
		if gh.RepositoryPath != "" {
			properties["Repository Path"] = gh.RepositoryPath
		}
		properties["Default Branch"] = stringOrDefault(gh.DefaultBranch, "main")
		properties["GitHub API URL"] = stringOrDefault(gh.APIURL, "https://api.github.com/")
		if gh.PersonalAccessTokenSecretRef != nil {
			properties["Authentication Type"] = "PERSONAL_ACCESS_TOKEN"
			sensitiveKeys["Personal Access Token"] = true
			token, wait, err := r.resolveSecretKey(ctx, instance.Namespace, gh.PersonalAccessTokenSecretRef)
			if err != nil {
				return nifi.RegistryClientEntity{}, resolvedType, nil, nil, err
			}
			if wait != "" {
				waitingFor = append(waitingFor, wait)
			} else {
				properties["Personal Access Token"] = token
			}
		} else {
			properties["Authentication Type"] = "NONE"
		}
	case nifiv1alpha1.RegistryClientTypeGitLab:
		gl := instance.Spec.GitLab
		if gl == nil {
			waitingFor = append(waitingFor, "gitlab")
			break
		}
		properties["Repository Namespace"] = gl.RepositoryNamespace
		properties["Repository Name"] = gl.RepositoryName
		if gl.RepositoryPath != "" {
			properties["Repository Path"] = gl.RepositoryPath
		}
		properties["Default Branch"] = stringOrDefault(gl.DefaultBranch, "main")
		properties["GitLab API URL"] = stringOrDefault(gl.APIURL, "https://gitlab.com/")
		properties["Authentication Type"] = "ACCESS_TOKEN"
		if gl.AccessTokenSecretRef != nil {
			sensitiveKeys["Access Token"] = true
			token, wait, err := r.resolveSecretKey(ctx, instance.Namespace, gl.AccessTokenSecretRef)
			if err != nil {
				return nifi.RegistryClientEntity{}, resolvedType, nil, nil, err
			}
			if wait != "" {
				waitingFor = append(waitingFor, wait)
			} else {
				properties["Access Token"] = token
			}
		}
	default: // NiFiRegistry
		if instance.Spec.URI != "" {
			properties["url"] = instance.Spec.URI
		}
	}

	// Generic passthrough overrides/supplements the typed fields (e.g. GitHub App Installation).
	for key, value := range instance.Spec.Properties {
		properties[key] = value
	}
	for name, source := range instance.Spec.SensitiveProperties {
		sensitiveKeys[name] = true
		if source.SecretKeyRef == nil {
			waitingFor = append(waitingFor, fmt.Sprintf("sensitiveProperties.%s.secretKeyRef", name))
			continue
		}
		value, wait, err := r.resolveSecretKey(ctx, instance.Namespace, source.SecretKeyRef)
		if err != nil {
			return nifi.RegistryClientEntity{}, resolvedType, nil, nil, err
		}
		if wait != "" {
			waitingFor = append(waitingFor, wait)
			continue
		}
		properties[name] = value
	}

	if len(properties) == 0 {
		properties = nil
	}
	entity := nifi.RegistryClientEntity{
		Revision: nifi.Revision{Version: 0},
		Component: nifi.RegistryClientComponent{
			Name:        instance.Name,
			Type:        resolvedType,
			Description: instance.Spec.Description,
			Properties:  properties,
		},
	}
	return entity, resolvedType, sensitiveKeys, waitingFor, nil
}

// resolveSecretKey reads a Secret value. It returns a non-empty "waitFor" string (and empty
// value) when the Secret or key is missing but not optional, so the caller can wait for it.
func (r *NiFiRegistryClientReconciler) resolveSecretKey(ctx context.Context, namespace string, ref *nifiv1alpha1.SecretKeyRef) (string, string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: namespace}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Sprintf("Secret/%s/%s", namespace, ref.Name), nil
		}
		return "", "", err
	}
	data, ok := secret.Data[ref.Key]
	if !ok {
		if ref.Optional != nil && *ref.Optional {
			return "", "", nil
		}
		return "", fmt.Sprintf("Secret/%s/%s:%s", namespace, ref.Name, ref.Key), nil
	}
	return string(data), "", nil
}

func stringOrDefault(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func registryClientType(registryType nifiv1alpha1.RegistryClientType) string {
	switch registryType {
	case nifiv1alpha1.RegistryClientTypeGitHub:
		return "org.apache.nifi.github.GitHubFlowRegistryClient"
	case nifiv1alpha1.RegistryClientTypeGitLab:
		return "org.apache.nifi.gitlab.GitLabFlowRegistryClient"
	default:
		return "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient"
	}
}

func registryClientEntityID(entity nifi.RegistryClientEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func registryClientNeedsUpdate(desired nifi.RegistryClientEntity, existing nifi.RegistryClientEntity, sensitiveKeys map[string]bool) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type ||
		desired.Component.Description != existing.Component.Description {
		return true
	}
	// Compare only the properties we manage, skipping sensitive ones (NiFi masks them on read) and
	// ignoring any additional default properties NiFi returns that we did not set.
	for key, value := range desired.Component.Properties {
		if sensitiveKeys[key] {
			continue
		}
		if existing.Component.Properties[key] != value {
			return true
		}
	}
	return false
}

func registryClientStatusMatches(instance *nifiv1alpha1.NiFiRegistryClient, nifiID string, revisionVersion int64, resolvedType string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ResolvedType == resolvedType
}

func shouldMarkRegistryClientNotReady(instance *nifiv1alpha1.NiFiRegistryClient, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func (r *NiFiRegistryClientReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiRegistryClient{}, clusterRefIndexField, indexRegistryClientClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiRegistryClient{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiParameterContextReconciler struct {
	client.Client
	Scheme                 *runtime.Scheme
	ParameterContextClient nifi.ParameterContextClient
}

func (r *NiFiParameterContextReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiParameterContext{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileParameterContextDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markParameterContextWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	entity, waitingFor, err := r.desiredParameterContext(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markParameterContextWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkParameterContextNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markParameterContextNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	pcClient := r.ParameterContextClient
	if pcClient == nil {
		pcClient = nifi.HTTPParameterContextClient{}
	}
	contexts, err := pcClient.ListParameterContexts(ctx, endpoint)
	if err != nil {
		message := fmt.Sprintf("Failed to list NiFi parameter contexts: %v", err)
		if shouldMarkParameterContextNotReady(instance, "NiFiListFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiListFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	if existing := parameterContextByID(contexts, instance.Status.NiFiID); existing != nil {
		full, err := pcClient.GetParameterContext(ctx, endpoint, parameterContextEntityID(*existing))
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi parameter context: %v", err)
			if shouldMarkParameterContextNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingParameterContext(ctx, instance, endpoint, pcClient, entity, full)
	}
	if pending := instance.Status.LatestUpdateRequest; pending != nil && !pending.Complete {
		message := fmt.Sprintf("NiFi parameter context update request %q is pending, but parameter context %q was not found.", pending.ID, instance.Status.NiFiID)
		if shouldMarkParameterContextNotReady(instance, "UpdateTargetNotFound", message) {
			return ctrl.Result{}, markParameterContextNotReady(ctx, r.Client, instance, "UpdateTargetNotFound", message)
		}
		return ctrl.Result{}, nil
	}
	if existing := parameterContextByName(contexts, instance.Name); existing != nil {
		if !parameterContextAdoptionAllowed(instance.Spec.AdoptionPolicy) && instance.Status.NiFiID == "" {
			message := fmt.Sprintf("A NiFi parameter context named %q already exists; set adoptionPolicy.mode to AdoptByName or IfExists to manage it.", instance.Name)
			if shouldMarkParameterContextNotReady(instance, "AdoptionRequired", message) {
				return ctrl.Result{}, markParameterContextNotReady(ctx, r.Client, instance, "AdoptionRequired", message)
			}
			return ctrl.Result{}, nil
		}
		full, err := pcClient.GetParameterContext(ctx, endpoint, parameterContextEntityID(*existing))
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi parameter context: %v", err)
			if shouldMarkParameterContextNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingParameterContext(ctx, instance, endpoint, pcClient, entity, full)
	}

	created, err := pcClient.CreateParameterContext(ctx, endpoint, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi parameter context: %v", err)
		if shouldMarkParameterContextNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty parameter context response."
		if shouldMarkParameterContextNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := parameterContextEntityID(*created)
	if !parameterContextStatusMatches(instance, nifiID, created.Revision.Version) {
		return ctrl.Result{}, markParameterContextReady(ctx, r.Client, instance, nifiID, created.Revision.Version)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiParameterContextReconciler) reconcileParameterContextDelete(ctx context.Context, instance *nifiv1alpha1.NiFiParameterContext) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	pcClient := r.ParameterContextClient
	if pcClient == nil {
		pcClient = nifi.HTTPParameterContextClient{}
	}
	current, err := pcClient.GetParameterContext(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := pcClient.DeleteParameterContext(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiParameterContextReconciler) reconcileExistingParameterContext(ctx context.Context, instance *nifiv1alpha1.NiFiParameterContext, endpoint string, pcClient nifi.ParameterContextClient, desired nifi.ParameterContextEntity, existing *nifi.ParameterContextEntity) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty parameter context response."
		if shouldMarkParameterContextNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nifiID := parameterContextEntityID(*existing)
	if pending := instance.Status.LatestUpdateRequest; pending != nil && !pending.Complete {
		request, err := pcClient.GetParameterContextUpdateRequest(ctx, endpoint, nifiID, pending.ID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi parameter context update request: %v", err)
			if shouldMarkParameterContextNotReady(instance, "UpdateRequestGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "UpdateRequestGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		status := parameterContextUpdateRequestStatus(request)
		if status.FailureReason != "" {
			if shouldMarkParameterContextNotReady(instance, "UpdateRequestFailed", status.FailureReason) {
				instance.Status.LatestUpdateRequest = status
				return ctrl.Result{}, markParameterContextNotReady(ctx, r.Client, instance, "UpdateRequestFailed", status.FailureReason)
			}
			return ctrl.Result{}, nil
		}
		if !status.Complete {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, markParameterContextUpdateRunning(ctx, r.Client, instance, status)
		}
		instance.Status.LatestUpdateRequest = status
		return r.markParameterContextReadyFromNiFi(ctx, instance, endpoint, pcClient, nifiID, existing.Revision.Version)
	}

	if parameterContextNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		request, err := pcClient.CreateParameterContextUpdateRequest(ctx, endpoint, nifiID, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to create NiFi parameter context update request: %v", err)
			if shouldMarkParameterContextNotReady(instance, "UpdateRequestCreateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "UpdateRequestCreateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		status := parameterContextUpdateRequestStatus(request)
		if status.FailureReason != "" {
			if shouldMarkParameterContextNotReady(instance, "UpdateRequestFailed", status.FailureReason) {
				instance.Status.LatestUpdateRequest = status
				return ctrl.Result{}, markParameterContextNotReady(ctx, r.Client, instance, "UpdateRequestFailed", status.FailureReason)
			}
			return ctrl.Result{}, nil
		}
		if !status.Complete {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, markParameterContextUpdateRunning(ctx, r.Client, instance, status)
		}
		instance.Status.LatestUpdateRequest = status
		return r.markParameterContextReadyFromNiFi(ctx, instance, endpoint, pcClient, nifiID, existing.Revision.Version)
	}

	if !parameterContextStatusMatches(instance, nifiID, existing.Revision.Version) {
		return ctrl.Result{}, markParameterContextReady(ctx, r.Client, instance, nifiID, existing.Revision.Version)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiParameterContextReconciler) markParameterContextReadyFromNiFi(ctx context.Context, instance *nifiv1alpha1.NiFiParameterContext, endpoint string, pcClient nifi.ParameterContextClient, nifiID string, fallbackRevision int64) (ctrl.Result, error) {
	revision := fallbackRevision
	current, err := pcClient.GetParameterContext(ctx, endpoint, nifiID)
	if err != nil {
		message := fmt.Sprintf("Failed to refresh NiFi parameter context after update: %v", err)
		if shouldMarkParameterContextNotReady(instance, "NiFiRefreshFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterContextNotReady(ctx, r.Client, instance, "NiFiRefreshFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if current != nil {
		revision = current.Revision.Version
	}
	return ctrl.Result{}, markParameterContextReady(ctx, r.Client, instance, nifiID, revision)
}

func (r *NiFiParameterContextReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiParameterContext{}, clusterRefIndexField, indexParameterContextClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiParameterContext{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

func (r *NiFiParameterContextReconciler) desiredParameterContext(ctx context.Context, instance *nifiv1alpha1.NiFiParameterContext) (nifi.ParameterContextEntity, []string, error) {
	parameters := make([]nifi.ParameterEntity, 0, len(instance.Spec.Parameters))
	waitingFor := make([]string, 0)
	for _, declared := range instance.Spec.Parameters {
		value := declared.Value
		sensitive := declared.SensitiveValueFrom != nil
		if sensitive {
			secretRef := declared.SensitiveValueFrom.SecretKeyRef
			if secretRef == nil {
				waitingFor = append(waitingFor, fmt.Sprintf("Parameter/%s:sensitiveValueFrom.secretKeyRef", declared.Name))
				continue
			}
			secret := &corev1.Secret{}
			key := types.NamespacedName{Name: secretRef.Name, Namespace: instance.Namespace}
			if err := r.Get(ctx, key, secret); err != nil {
				if apierrors.IsNotFound(err) {
					waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s", instance.Namespace, secretRef.Name))
					continue
				}
				return nifi.ParameterContextEntity{}, nil, err
			}
			data, ok := secret.Data[secretRef.Key]
			if !ok {
				if secretRef.Optional != nil && *secretRef.Optional {
					value = ""
				} else {
					waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s:%s", instance.Namespace, secretRef.Name, secretRef.Key))
					continue
				}
			} else {
				value = string(data)
			}
		}
		parameters = append(parameters, nifi.ParameterEntity{
			Parameter: nifi.Parameter{
				Name:        declared.Name,
				Description: declared.Description,
				Sensitive:   sensitive,
				Value:       &value,
			},
		})
	}

	return nifi.ParameterContextEntity{
		Revision: nifi.Revision{Version: 0},
		Component: nifi.ParameterContextComponent{
			Name:        instance.Name,
			Description: instance.Spec.Description,
			Parameters:  parameters,
		},
	}, waitingFor, nil
}

func parameterContextByID(contexts []nifi.ParameterContextEntity, id string) *nifi.ParameterContextEntity {
	if id == "" {
		return nil
	}
	for i := range contexts {
		if contexts[i].ID == id || contexts[i].Component.ID == id {
			return &contexts[i]
		}
	}
	return nil
}

func parameterContextByName(contexts []nifi.ParameterContextEntity, name string) *nifi.ParameterContextEntity {
	for i := range contexts {
		if contexts[i].Component.Name == name {
			return &contexts[i]
		}
	}
	return nil
}

func parameterContextEntityID(entity nifi.ParameterContextEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func parameterContextUpdateRequestStatus(entity *nifi.ParameterContextUpdateRequestEntity) *nifiv1alpha1.ParameterContextUpdateRequestStatus {
	if entity == nil {
		return &nifiv1alpha1.ParameterContextUpdateRequestStatus{}
	}
	request := entity.Request
	return &nifiv1alpha1.ParameterContextUpdateRequestStatus{
		ID:               request.RequestID,
		URI:              request.URI,
		SubmissionTime:   request.SubmissionTime,
		LastUpdated:      request.LastUpdated,
		Complete:         request.Complete,
		FailureReason:    request.FailureReason,
		PercentCompleted: request.PercentCompleted,
		State:            request.State,
	}
}

func parameterContextNeedsUpdate(desired nifi.ParameterContextEntity, existing nifi.ParameterContextEntity) bool {
	if desired.Component.Name != existing.Component.Name || desired.Component.Description != existing.Component.Description {
		return true
	}
	if len(desired.Component.Parameters) != len(existing.Component.Parameters) {
		return true
	}

	existingParameters := make(map[string]nifi.Parameter, len(existing.Component.Parameters))
	for _, parameterEntity := range existing.Component.Parameters {
		existingParameters[parameterEntity.Parameter.Name] = parameterEntity.Parameter
	}
	for _, desiredEntity := range desired.Component.Parameters {
		desiredParameter := desiredEntity.Parameter
		existingParameter, ok := existingParameters[desiredParameter.Name]
		if !ok {
			return true
		}
		if desiredParameter.Description != existingParameter.Description || desiredParameter.Sensitive != existingParameter.Sensitive {
			return true
		}
		if !desiredParameter.Sensitive && parameterValue(desiredParameter) != parameterValue(existingParameter) {
			return true
		}
	}
	return false
}

func parameterValue(parameter nifi.Parameter) string {
	if parameter.Value == nil {
		return ""
	}
	return *parameter.Value
}

func parameterContextAdoptionAllowed(policy nifiv1alpha1.AdoptionPolicy) bool {
	switch policy.Mode {
	case nifiv1alpha1.AdoptionPolicyIfExists, nifiv1alpha1.AdoptionPolicyAdoptByName:
		return true
	default:
		return false
	}
}

func parameterContextStatusMatches(instance *nifiv1alpha1.NiFiParameterContext, nifiID string, revisionVersion int64) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion
}

func shouldMarkParameterContextNotReady(instance *nifiv1alpha1.NiFiParameterContext, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

type NiFiUserReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	UserClient nifi.UserClient
}

func (r *NiFiUserReconciler) userClient() nifi.UserClient {
	if r.UserClient != nil {
		return r.UserClient
	}
	return nifi.HTTPUserClient{}
}

func (r *NiFiUserReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiUser{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileUserDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markUserWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkUserNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markUserNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	users := r.userClient()

	// Resolve the existing tenant: by recorded id, then by identity (adoption), else create.
	if instance.Status.NiFiID != "" {
		existing, err := users.GetUser(ctx, endpoint, instance.Status.NiFiID)
		if err != nil && !nifi.IsNotFound(err) {
			message := fmt.Sprintf("Failed to get NiFi user: %v", err)
			if shouldMarkUserNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingUser(ctx, instance, endpoint, users, existing)
		}
	}

	all, err := users.ListUsers(ctx, endpoint)
	if err != nil {
		message := fmt.Sprintf("Failed to list NiFi users: %v", err)
		if shouldMarkUserNotReady(instance, "NiFiListFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserNotReady(ctx, r.Client, instance, "NiFiListFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if existing := findUserByIdentity(all, instance.Spec.Identity); existing != nil {
		return r.reconcileExistingUser(ctx, instance, endpoint, users, existing)
	}

	created, err := users.CreateUser(ctx, endpoint, nifi.UserEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: nifi.UserComponent{Identity: instance.Spec.Identity},
	})
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi user: %v", err)
		if shouldMarkUserNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, markUserReady(ctx, r.Client, instance, nifi.UserEntityID(*created), created.Revision.Version)
}

func (r *NiFiUserReconciler) reconcileExistingUser(ctx context.Context, instance *nifiv1alpha1.NiFiUser, endpoint string, users nifi.UserClient, existing *nifi.UserEntity) (ctrl.Result, error) {
	nifiID := nifi.UserEntityID(*existing)
	// Rename the tenant if the desired identity changed.
	if existing.Component.Identity != instance.Spec.Identity {
		update := *existing
		update.ID = nifiID
		update.Component.ID = nifiID
		update.Component.Identity = instance.Spec.Identity
		updated, err := users.UpdateUser(ctx, endpoint, update)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi user: %v", err)
			if shouldMarkUserNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			return ctrl.Result{}, markUserReady(ctx, r.Client, instance, nifi.UserEntityID(*updated), updated.Revision.Version)
		}
	}
	if !userStatusMatches(instance, nifiID, existing.Revision.Version) {
		return ctrl.Result{}, markUserReady(ctx, r.Client, instance, nifiID, existing.Revision.Version)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiUserReconciler) reconcileUserDelete(ctx context.Context, instance *nifiv1alpha1.NiFiUser) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its NiFi tenant) is gone; nothing to delete remotely.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.userClient().DeleteUser(ctx, endpoint, instance.Status.NiFiID, instance.Status.Revision.Version); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiUserReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiUser{}, clusterRefIndexField, indexUserClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiUser{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiUserGroupReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	UserGroupClient nifi.UserGroupClient
}

func (r *NiFiUserGroupReconciler) userGroupClient() nifi.UserGroupClient {
	if r.UserGroupClient != nil {
		return r.UserGroupClient
	}
	return nifi.HTTPUserGroupClient{}
}

func (r *NiFiUserGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiUserGroup{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileUserGroupDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, userGroupMemberDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markUserGroupWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkUserGroupNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markUserGroupNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// A NiFi user group can only contain tenants from its own NiFi; a member NiFiUser bound to a
	// different cluster has a NiFiID that is meaningless here, so reject it rather than adding a
	// foreign id to the group.
	mismatch, err := userGroupMemberClusterMismatch(ctx, r.Client, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if mismatch != "" {
		message := fmt.Sprintf("Member %q references a different NiFiCluster than the group; a NiFi user group can only contain users from its own cluster.", mismatch)
		if shouldMarkUserGroupNotReady(instance, "MemberClusterMismatch", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserGroupNotReady(ctx, r.Client, instance, "MemberClusterMismatch", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	memberIDs, err := r.resolveUserGroupMemberIDs(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}

	groups := r.userGroupClient()
	desired := nifi.UserGroupComponent{Identity: instance.Spec.Identity, Users: tenantRefs(memberIDs)}

	if instance.Status.NiFiID != "" {
		existing, err := groups.GetUserGroup(ctx, endpoint, instance.Status.NiFiID)
		if err != nil && !nifi.IsNotFound(err) {
			message := fmt.Sprintf("Failed to get NiFi user group: %v", err)
			if shouldMarkUserGroupNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserGroupNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingUserGroup(ctx, instance, endpoint, groups, existing, desired, memberIDs)
		}
	}

	all, err := groups.ListUserGroups(ctx, endpoint)
	if err != nil {
		message := fmt.Sprintf("Failed to list NiFi user groups: %v", err)
		if shouldMarkUserGroupNotReady(instance, "NiFiListFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserGroupNotReady(ctx, r.Client, instance, "NiFiListFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if existing := findUserGroupByIdentity(all, instance.Spec.Identity); existing != nil {
		return r.reconcileExistingUserGroup(ctx, instance, endpoint, groups, existing, desired, memberIDs)
	}

	created, err := groups.CreateUserGroup(ctx, endpoint, nifi.UserGroupEntity{Revision: nifi.Revision{Version: 0}, Component: desired})
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi user group: %v", err)
		if shouldMarkUserGroupNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserGroupNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return ctrl.Result{}, markUserGroupReady(ctx, r.Client, instance, nifi.UserGroupEntityID(*created), created.Revision.Version, memberIDs)
}

func (r *NiFiUserGroupReconciler) reconcileExistingUserGroup(ctx context.Context, instance *nifiv1alpha1.NiFiUserGroup, endpoint string, groups nifi.UserGroupClient, existing *nifi.UserGroupEntity, desired nifi.UserGroupComponent, memberIDs []string) (ctrl.Result, error) {
	nifiID := nifi.UserGroupEntityID(*existing)
	if userGroupNeedsUpdate(*existing, desired) {
		update := nifi.UserGroupEntity{
			Revision:  existing.Revision,
			ID:        nifiID,
			Component: nifi.UserGroupComponent{ID: nifiID, Identity: desired.Identity, Users: desired.Users},
		}
		updated, err := groups.UpdateUserGroup(ctx, endpoint, update)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi user group: %v", err)
			if shouldMarkUserGroupNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markUserGroupNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			return ctrl.Result{}, markUserGroupReady(ctx, r.Client, instance, nifi.UserGroupEntityID(*updated), updated.Revision.Version, memberIDs)
		}
	}
	if !userGroupStatusMatches(instance, nifiID, existing.Revision.Version, memberIDs) {
		return ctrl.Result{}, markUserGroupReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, memberIDs)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiUserGroupReconciler) resolveUserGroupMemberIDs(ctx context.Context, instance *nifiv1alpha1.NiFiUserGroup) ([]string, error) {
	memberIDs := make([]string, 0, len(instance.Spec.Users))
	for _, member := range instance.Spec.Users {
		namespace := instance.Namespace
		if member.UserRef.Namespace != "" {
			namespace = member.UserRef.Namespace
		}
		user := &nifiv1alpha1.NiFiUser{}
		if err := r.Get(ctx, types.NamespacedName{Name: member.UserRef.Name, Namespace: namespace}, user); err != nil {
			return nil, err
		}
		if user.Status.NiFiID != "" {
			memberIDs = append(memberIDs, user.Status.NiFiID)
		}
	}
	return memberIDs, nil
}

func (r *NiFiUserGroupReconciler) reconcileUserGroupDelete(ctx context.Context, instance *nifiv1alpha1.NiFiUserGroup) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its NiFi user group) is gone; nothing to delete remotely.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if err := r.userGroupClient().DeleteUserGroup(ctx, endpoint, instance.Status.NiFiID, instance.Status.Revision.Version); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiUserGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiUserGroup{}, clusterRefIndexField, indexUserGroupClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiUserGroup{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiUser{}, handler.EnqueueRequestsFromMapFunc(r.requestsForMemberUser)).
		Complete(r)
}

// requestsForMemberUser enqueues every NiFiUserGroup that lists the changed NiFiUser as a member,
// so a group waiting on that user reconciles as soon as the user becomes Ready (a member change
// would otherwise never trigger the group).
func (r *NiFiUserGroupReconciler) requestsForMemberUser(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiUserGroupList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	var requests []reconcile.Request
	for i := range list.Items {
		group := &list.Items[i]
		for _, member := range group.Spec.Users {
			namespace := group.Namespace
			if member.UserRef.Namespace != "" {
				namespace = member.UserRef.Namespace
			}
			if member.UserRef.Name == obj.GetName() && namespace == obj.GetNamespace() {
				requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: group.Name, Namespace: group.Namespace}})
				break
			}
		}
	}
	return requests
}

type NiFiProcessGroupReconciler struct {
	client.Client
	Scheme             *runtime.Scheme
	ProcessGroupClient nifi.ProcessGroupClient
}

func (r *NiFiProcessGroupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiProcessGroup{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileProcessGroupDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, processGroupDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markProcessGroupWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkProcessGroupNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markProcessGroupNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkProcessGroupNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markProcessGroupNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	entity, err := desiredProcessGroup(ctx, r.Client, instance, parentID)
	if err != nil {
		return ctrl.Result{}, err
	}
	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}

	if instance.Status.NiFiID != "" {
		existing, err := processGroups.GetProcessGroup(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi process group: %v", err)
			if shouldMarkProcessGroupNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingProcessGroup(ctx, instance, endpoint, processGroups, entity, existing, parentID)
	}

	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := processGroups.GetProcessGroup(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi process group: %v", err)
			if shouldMarkProcessGroupNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingProcessGroup(ctx, instance, endpoint, processGroups, entity, existing, parentID)
	}

	created, err := processGroups.CreateProcessGroup(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi process group: %v", err)
		if shouldMarkProcessGroupNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty process group response."
		if shouldMarkProcessGroupNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := processGroupEntityID(*created)
	if !processGroupStatusMatches(instance, nifiID, created.Revision.Version, parentID) {
		return ctrl.Result{}, markProcessGroupReady(ctx, r.Client, instance, nifiID, created.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiProcessGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiProcessGroup{}, clusterRefIndexField, indexProcessGroupClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiProcessGroup{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiParameterContext{}, handler.EnqueueRequestsFromMapFunc(r.requestsForParameterContext)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiProcessGroupReconciler) reconcileProcessGroupDelete(ctx context.Context, instance *nifiv1alpha1.NiFiProcessGroup) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}
	current, err := processGroups.GetProcessGroup(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := processGroups.DeleteProcessGroup(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiProcessGroupReconciler) reconcileExistingProcessGroup(ctx context.Context, instance *nifiv1alpha1.NiFiProcessGroup, endpoint string, processGroups nifi.ProcessGroupClient, desired nifi.ProcessGroupEntity, existing *nifi.ProcessGroupEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty process group response."
		if shouldMarkProcessGroupNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nifiID := processGroupEntityID(*existing)
	if processGroupNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := processGroups.UpdateProcessGroup(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi process group: %v", err)
			if shouldMarkProcessGroupNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessGroupNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			nifiID = processGroupEntityID(*updated)
			return ctrl.Result{}, markProcessGroupReady(ctx, r.Client, instance, nifiID, updated.Revision.Version, parentID)
		}
	}

	if !processGroupStatusMatches(instance, nifiID, existing.Revision.Version, parentID) {
		return ctrl.Result{}, markProcessGroupReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

type NiFiControllerServiceReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	ControllerServiceClient nifi.ControllerServiceClient
}

func (r *NiFiControllerServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiControllerService{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileControllerServiceDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, controllerServiceDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markControllerServiceWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkControllerServiceNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markControllerServiceNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkControllerServiceNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markControllerServiceNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	entity, waitingFor, err := r.desiredControllerService(ctx, instance, parentID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markControllerServiceWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	controllerServices := r.ControllerServiceClient
	if controllerServices == nil {
		controllerServices = nifi.HTTPControllerServiceClient{}
	}
	if instance.Status.NiFiID != "" {
		existing, err := controllerServices.GetControllerService(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi controller service: %v", err)
			if shouldMarkControllerServiceNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingControllerService(ctx, instance, endpoint, controllerServices, entity, existing)
	}
	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := controllerServices.GetControllerService(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi controller service: %v", err)
			if shouldMarkControllerServiceNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingControllerService(ctx, instance, endpoint, controllerServices, entity, existing)
	}

	created, err := controllerServices.CreateControllerService(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi controller service: %v", err)
		if shouldMarkControllerServiceNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty controller service response."
		if shouldMarkControllerServiceNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// Route the freshly created (DISABLED) service through the existing-reconcile path so its desired
	// enabled/disabled state is applied through the run-status endpoint.
	return r.reconcileExistingControllerService(ctx, instance, endpoint, controllerServices, entity, created)
}

func (r *NiFiControllerServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiControllerService{}, clusterRefIndexField, indexControllerServiceClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiControllerService{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiParameterContext{}, handler.EnqueueRequestsFromMapFunc(r.requestsForParameterContext)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiControllerServiceReconciler) reconcileControllerServiceDelete(ctx context.Context, instance *nifiv1alpha1.NiFiControllerService) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	controllerServices := r.ControllerServiceClient
	if controllerServices == nil {
		controllerServices = nifi.HTTPControllerServiceClient{}
	}
	// An enabled controller service cannot be deleted; read it for the current revision, disable it
	// if needed, then delete.
	current, err := controllerServices.GetControllerService(ctx, endpoint, instance.Status.NiFiID)
	if err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	if current != nil {
		revision := current.Revision.Version
		if current.Component.State == "ENABLED" || current.Component.State == "ENABLING" {
			disabled, err := controllerServices.UpdateControllerServiceRunStatus(ctx, endpoint, instance.Status.NiFiID, revision, "DISABLED")
			if err != nil {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, err
			}
			if disabled != nil {
				revision = disabled.Revision.Version
			}
		}
		if err := controllerServices.DeleteControllerService(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiControllerServiceReconciler) reconcileExistingControllerService(ctx context.Context, instance *nifiv1alpha1.NiFiControllerService, endpoint string, controllerServices nifi.ControllerServiceClient, desired nifi.ControllerServiceEntity, existing *nifi.ControllerServiceEntity) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty controller service response."
		if shouldMarkControllerServiceNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := controllerServiceEntityID(*existing)
	current := existing
	if controllerServiceNeedsUpdate(desired, *current) {
		// NiFi requires the service DISABLED to change its config.
		if current.Component.State == "ENABLED" || current.Component.State == "ENABLING" {
			disabled, err := controllerServices.UpdateControllerServiceRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "DISABLED")
			if err != nil {
				return r.controllerServiceWriteFailed(ctx, instance, "NiFiUpdateFailed", "disable", err)
			}
			if disabled != nil {
				current = disabled
			}
		}
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = current.Revision.Version
		updated, err := controllerServices.UpdateControllerService(ctx, endpoint, updateEntity)
		if err != nil {
			return r.controllerServiceWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	desiredState := nifiScheduledState(instance.Spec.State)
	if desiredState != "" && !controllerServiceStateSatisfied(current.Component.State, desiredState) {
		if desiredState == "ENABLED" {
			switch current.Component.ValidationStatus {
			case "INVALID":
				message := "The controller service is INVALID and cannot be enabled; check its properties."
				if shouldMarkControllerServiceNotReady(instance, "ControllerServiceInvalid", message) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, "ControllerServiceInvalid", message)
				}
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			case "VALIDATING":
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
		}
		changed, err := controllerServices.UpdateControllerServiceRunStatus(ctx, endpoint, nifiID, current.Revision.Version, desiredState)
		if err != nil {
			return r.controllerServiceWriteFailed(ctx, instance, "NiFiStateFailed", "set state of", err)
		}
		if changed != nil {
			current = changed
		}
	}

	if !controllerServiceStatusMatches(instance, nifiID, current.Revision.Version, current.Component.ValidationStatus) {
		return ctrl.Result{}, markControllerServiceReady(ctx, r.Client, instance, nifiID, current.Revision.Version, current.Component.ValidationStatus)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiControllerServiceReconciler) controllerServiceWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiControllerService, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi controller service: %v", verb, err)
	if shouldMarkControllerServiceNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markControllerServiceNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// controllerServiceStateSatisfied treats the transient ENABLING/DISABLING states as already heading
// to the desired ENABLED/DISABLED, so the operator does not re-issue run-status while NiFi settles.
func controllerServiceStateSatisfied(current, desired string) bool {
	switch desired {
	case "ENABLED":
		return current == "ENABLED" || current == "ENABLING"
	case "DISABLED":
		return current == "DISABLED" || current == "DISABLING"
	}
	return current == desired
}

type NiFiFlowBundleReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ArtifactResolver flowartifact.Resolver
}

func (r *NiFiFlowBundleReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiFlowBundle{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor := flowBundleDependenciesWaitingFor(ctx, r.Client, instance)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markFlowBundleWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	artifactDigest, resolvedRevision, err := resolvedFlowBundleArtifact(ctx, r.Client, r.ArtifactResolver, instance)
	if err != nil {
		message := fmt.Sprintf("Failed to resolve flow bundle artifact: %v", err)
		if shouldMarkFlowBundleNotReady(instance, "ArtifactResolutionFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowBundleNotReady(ctx, r.Client, instance, "ArtifactResolutionFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	result := ctrl.Result{}
	if instance.Spec.Source.Snapshot == nil {
		result.RequeueAfter = 5 * time.Minute
	}
	if !flowBundleStatusMatches(instance, artifactDigest, resolvedRevision) {
		return result, markFlowBundleReady(ctx, r.Client, instance, artifactDigest, resolvedRevision)
	}
	return result, nil
}

func (r *NiFiFlowBundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiFlowBundle{}).
		Watches(&nifiv1alpha1.NiFiRegistryClient{}, handler.EnqueueRequestsFromMapFunc(r.requestsForRegistryClient)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCredentialSecret)).
		Complete(r)
}

type NiFiFlowDeploymentReconciler struct {
	client.Client
	Scheme                *runtime.Scheme
	ProcessGroupClient    nifi.ProcessGroupClient
	FlowSnapshotClient    nifi.FlowSnapshotClient
	FlowSnapshotReader    nifi.FlowSnapshotReader
	ProcessGroupScheduler nifi.ProcessGroupScheduler
	BlueGreenClient       nifi.BlueGreenClient
	ArtifactResolver      flowartifact.Resolver
}

func (r *NiFiFlowDeploymentReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiFlowDeployment{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileFlowDeploymentDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, flowDeploymentDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markFlowDeploymentWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkFlowDeploymentNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markFlowDeploymentNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.Target.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The target parent process group ID is not available yet."
		if shouldMarkFlowDeploymentNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markFlowDeploymentNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	snapshot, snapshotVersion, snapshotDigest, err := resolvedFlowDeploymentSnapshot(ctx, r.Client, r.ArtifactResolver, instance)
	if err != nil {
		message := fmt.Sprintf("Failed to resolve flow snapshot: %v", err)
		if shouldMarkFlowDeploymentNotReady(instance, "InvalidFlowSnapshot", message) {
			return ctrl.Result{}, markFlowDeploymentNotReady(ctx, r.Client, instance, "InvalidFlowSnapshot", message)
		}
		return ctrl.Result{}, nil
	}
	if len(snapshot) > 0 {
		return r.reconcileSnapshotFlowDeployment(ctx, instance, endpoint, parentID, snapshot, snapshotVersion, snapshotDigest)
	}
	entity, deployedVersion, artifactDigest, err := desiredFlowDeploymentProcessGroup(ctx, r.Client, instance, parentID)
	if err != nil {
		return ctrl.Result{}, err
	}
	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}
	if instance.Status.ProcessGroupID != "" {
		existing, err := processGroups.GetProcessGroup(ctx, endpoint, instance.Status.ProcessGroupID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi flow deployment process group: %v", err)
			if shouldMarkFlowDeploymentNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingFlowDeploymentProcessGroup(ctx, instance, endpoint, processGroups, entity, existing, deployedVersion, artifactDigest)
	}
	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := processGroups.GetProcessGroup(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi flow deployment process group: %v", err)
			if shouldMarkFlowDeploymentNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingFlowDeploymentProcessGroup(ctx, instance, endpoint, processGroups, entity, existing, deployedVersion, artifactDigest)
	}

	created, err := processGroups.CreateProcessGroup(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi flow deployment process group: %v", err)
		if shouldMarkFlowDeploymentNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty flow deployment process group response."
		if shouldMarkFlowDeploymentNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	processGroupID := processGroupEntityID(*created)
	if !flowDeploymentStatusMatches(instance, processGroupID, created.Revision.Version, deployedVersion, artifactDigest, "ProcessGroupReconciled") {
		return ctrl.Result{}, markFlowDeploymentReady(ctx, r.Client, instance, processGroupID, created.Revision.Version, deployedVersion, artifactDigest, "ProcessGroupReconciled")
	}
	return ctrl.Result{}, nil
}

func (r *NiFiFlowDeploymentReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiFlowDeployment{}, clusterRefIndexField, indexFlowDeploymentClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiFlowDeployment{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiFlowBundle{}, handler.EnqueueRequestsFromMapFunc(r.requestsForFlowBundle)).
		Watches(&nifiv1alpha1.NiFiParameterContext{}, handler.EnqueueRequestsFromMapFunc(r.requestsForParameterContext)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCredentialSecret)).
		Complete(r)
}

func (r *NiFiFlowDeploymentReconciler) reconcileFlowDeploymentDelete(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment) (ctrl.Result, error) {
	processGroupID := instance.Status.ProcessGroupID
	if processGroupID == "" {
		processGroupID = instance.Status.NiFiID
	}
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || processGroupID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	processGroups := r.ProcessGroupClient
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}
	current, err := processGroups.GetProcessGroup(ctx, endpoint, processGroupID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := processGroups.DeleteProcessGroup(ctx, endpoint, processGroupID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiFlowDeploymentReconciler) reconcileExistingFlowDeploymentProcessGroup(ctx context.Context, instance *nifiv1alpha1.NiFiFlowDeployment, endpoint string, processGroups nifi.ProcessGroupClient, desired nifi.ProcessGroupEntity, existing *nifi.ProcessGroupEntity, deployedVersion string, artifactDigest string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty flow deployment process group response."
		if shouldMarkFlowDeploymentNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	processGroupID := processGroupEntityID(*existing)
	if processGroupNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = processGroupID
		updateEntity.Component.ID = processGroupID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := processGroups.UpdateProcessGroup(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi flow deployment process group: %v", err)
			if shouldMarkFlowDeploymentNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markFlowDeploymentNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			processGroupID = processGroupEntityID(*updated)
			return ctrl.Result{}, markFlowDeploymentReady(ctx, r.Client, instance, processGroupID, updated.Revision.Version, deployedVersion, artifactDigest, "ProcessGroupReconciled")
		}
	}
	if !flowDeploymentStatusMatches(instance, processGroupID, existing.Revision.Version, deployedVersion, artifactDigest, "ProcessGroupReconciled") {
		return ctrl.Result{}, markFlowDeploymentReady(ctx, r.Client, instance, processGroupID, existing.Revision.Version, deployedVersion, artifactDigest, "ProcessGroupReconciled")
	}
	return ctrl.Result{}, nil
}

type NiFiProcessorReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	ProcessorClient nifi.ProcessorClient
}

func (r *NiFiProcessorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiProcessor{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileProcessorDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, processorDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markProcessorWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkProcessorNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markProcessorNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processorParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkProcessorNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markProcessorNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	entity := desiredProcessor(instance, parentID)
	processors := r.ProcessorClient
	if processors == nil {
		processors = nifi.HTTPProcessorClient{}
	}
	if instance.Status.NiFiID != "" {
		existing, err := processors.GetProcessor(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi processor: %v", err)
			if shouldMarkProcessorNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingProcessor(ctx, instance, endpoint, processors, entity, existing, parentID)
	}
	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := processors.GetProcessor(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi processor: %v", err)
			if shouldMarkProcessorNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingProcessor(ctx, instance, endpoint, processors, entity, existing, parentID)
	}

	created, err := processors.CreateProcessor(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi processor: %v", err)
		if shouldMarkProcessorNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty processor response."
		if shouldMarkProcessorNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// Route the freshly created (STOPPED) processor through the existing-reconcile path so its run
	// state is applied through the run-status endpoint.
	return r.reconcileExistingProcessor(ctx, instance, endpoint, processors, entity, created, parentID)
}

func (r *NiFiProcessorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiProcessor{}, clusterRefIndexField, indexProcessorClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiProcessor{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiProcessorReconciler) reconcileProcessorDelete(ctx context.Context, instance *nifiv1alpha1.NiFiProcessor) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := cluster.Status.Endpoint
	if endpoint == "" && cluster.Spec.API != nil {
		endpoint = cluster.Spec.API.URI
	}
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	processors := r.ProcessorClient
	if processors == nil {
		processors = nifi.HTTPProcessorClient{}
	}
	// A running processor cannot be deleted; read it for the current revision, stop it if needed,
	// then delete.
	current, err := processors.GetProcessor(ctx, endpoint, instance.Status.NiFiID)
	if err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	if current != nil {
		revision := current.Revision.Version
		if current.Component.State == "RUNNING" {
			stopped, err := processors.UpdateProcessorRunStatus(ctx, endpoint, instance.Status.NiFiID, revision, "STOPPED")
			if err != nil {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, err
			}
			if stopped != nil {
				revision = stopped.Revision.Version
			}
		}
		if err := processors.DeleteProcessor(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiProcessorReconciler) reconcileExistingProcessor(ctx context.Context, instance *nifiv1alpha1.NiFiProcessor, endpoint string, processors nifi.ProcessorClient, desired nifi.ProcessorEntity, existing *nifi.ProcessorEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty processor response."
		if shouldMarkProcessorNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := processorEntityID(*existing)
	current := existing
	if processorNeedsUpdate(desired, *current) {
		// NiFi requires the processor stopped to change its config.
		if current.Component.State == "RUNNING" {
			stopped, err := processors.UpdateProcessorRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "STOPPED")
			if err != nil {
				return r.processorWriteFailed(ctx, instance, "NiFiUpdateFailed", "stop", err)
			}
			if stopped != nil {
				current = stopped
			}
		}
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = current.Revision.Version
		updated, err := processors.UpdateProcessor(ctx, endpoint, updateEntity)
		if err != nil {
			return r.processorWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	desiredState := nifiScheduledState(instance.Spec.State)
	if desiredState != "" && current.Component.State != desiredState {
		if desiredState == "RUNNING" {
			switch current.Component.ValidationStatus {
			case "INVALID":
				message := "The processor is INVALID and cannot be started; check its properties, relationships, and referenced services."
				if shouldMarkProcessorNotReady(instance, "ProcessorInvalid", message) {
					return ctrl.Result{RequeueAfter: 15 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, "ProcessorInvalid", message)
				}
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			case "VALIDATING":
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
		}
		changed, err := processors.UpdateProcessorRunStatus(ctx, endpoint, nifiID, current.Revision.Version, desiredState)
		if err != nil {
			return r.processorWriteFailed(ctx, instance, "NiFiStateFailed", "set run state of", err)
		}
		if changed != nil {
			current = changed
		}
	}

	if !processorStatusMatches(instance, nifiID, current.Revision.Version, parentID, current.Component.ValidationStatus) {
		return ctrl.Result{}, markProcessorReady(ctx, r.Client, instance, nifiID, current.Revision.Version, parentID, current.Component.ValidationStatus)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiProcessorReconciler) processorWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiProcessor, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi processor: %v", verb, err)
	if shouldMarkProcessorNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markProcessorNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

type NiFiInputPortReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	InputPortClient nifi.InputPortClient
}

func (r *NiFiInputPortReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiInputPort{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileInputPortDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, inputPortDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markInputPortWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkInputPortNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markInputPortNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkInputPortNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markInputPortNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	ports := r.InputPortClient
	if ports == nil {
		ports = nifi.HTTPInputPortClient{}
	}
	entity := desiredInputPort(instance, parentID)
	if instance.Status.NiFiID != "" {
		existing, err := ports.GetInputPort(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi input port: %v", err)
			if shouldMarkInputPortNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingInputPort(ctx, instance, endpoint, ports, entity, existing, parentID)
	}
	created, err := ports.CreateInputPort(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi input port: %v", err)
		if shouldMarkInputPortNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty input port response."
		if shouldMarkInputPortNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// Route the freshly created (STOPPED) port through the existing-reconcile path so its run state
	// is applied through the run-status endpoint.
	return r.reconcileExistingInputPort(ctx, instance, endpoint, ports, entity, created, parentID)
}

func (r *NiFiInputPortReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiInputPort{}, clusterRefIndexField, indexInputPortClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiInputPort{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiInputPortReconciler) reconcileInputPortDelete(ctx context.Context, instance *nifiv1alpha1.NiFiInputPort) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	ports := r.InputPortClient
	if ports == nil {
		ports = nifi.HTTPInputPortClient{}
	}
	current, err := ports.GetInputPort(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := ports.DeleteInputPort(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiInputPortReconciler) reconcileExistingInputPort(ctx context.Context, instance *nifiv1alpha1.NiFiInputPort, endpoint string, ports nifi.InputPortClient, desired nifi.PortEntity, existing *nifi.PortEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty input port response."
		if shouldMarkInputPortNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := portEntityID(*existing)
	current := existing
	if portNeedsUpdate(desired, *current) {
		// NiFi requires the port stopped to change its config.
		if current.Component.State == "RUNNING" {
			stopped, err := ports.UpdateInputPortRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "STOPPED")
			if err != nil {
				return r.inputPortWriteFailed(ctx, instance, "NiFiUpdateFailed", "stop", err)
			}
			if stopped != nil {
				current = stopped
			}
		}
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = current.Revision.Version
		updated, err := ports.UpdateInputPort(ctx, endpoint, updateEntity)
		if err != nil {
			return r.inputPortWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	current, pendingReason, pendingMsg, err := reconcilePortRunState(nifiID, current, nifiScheduledState(instance.Spec.State),
		func(id string, revisionVersion int64, state string) (*nifi.PortEntity, error) {
			return ports.UpdateInputPortRunStatus(ctx, endpoint, id, revisionVersion, state)
		})
	if err != nil {
		return r.inputPortWriteFailed(ctx, instance, "NiFiStateFailed", "set run state of", err)
	}
	if pendingReason != "" {
		if shouldMarkInputPortNotReady(instance, pendingReason, pendingMsg) {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, pendingReason, pendingMsg)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if !inputPortStatusMatches(instance, nifiID, current.Revision.Version, parentID) {
		return ctrl.Result{}, markInputPortReady(ctx, r.Client, instance, nifiID, current.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiInputPortReconciler) inputPortWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiInputPort, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi input port: %v", verb, err)
	if shouldMarkInputPortNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markInputPortNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

type NiFiOutputPortReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	OutputPortClient nifi.OutputPortClient
}

func (r *NiFiOutputPortReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiOutputPort{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileOutputPortDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, outputPortDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markOutputPortWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkOutputPortNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markOutputPortNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkOutputPortNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markOutputPortNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	ports := r.OutputPortClient
	if ports == nil {
		ports = nifi.HTTPOutputPortClient{}
	}
	entity := desiredOutputPort(instance, parentID)
	if instance.Status.NiFiID != "" {
		existing, err := ports.GetOutputPort(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi output port: %v", err)
			if shouldMarkOutputPortNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingOutputPort(ctx, instance, endpoint, ports, entity, existing, parentID)
	}
	created, err := ports.CreateOutputPort(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi output port: %v", err)
		if shouldMarkOutputPortNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty output port response."
		if shouldMarkOutputPortNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return r.reconcileExistingOutputPort(ctx, instance, endpoint, ports, entity, created, parentID)
}

func (r *NiFiOutputPortReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiOutputPort{}, clusterRefIndexField, indexOutputPortClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiOutputPort{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiOutputPortReconciler) reconcileOutputPortDelete(ctx context.Context, instance *nifiv1alpha1.NiFiOutputPort) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	ports := r.OutputPortClient
	if ports == nil {
		ports = nifi.HTTPOutputPortClient{}
	}
	current, err := ports.GetOutputPort(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := ports.DeleteOutputPort(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiOutputPortReconciler) reconcileExistingOutputPort(ctx context.Context, instance *nifiv1alpha1.NiFiOutputPort, endpoint string, ports nifi.OutputPortClient, desired nifi.PortEntity, existing *nifi.PortEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty output port response."
		if shouldMarkOutputPortNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := portEntityID(*existing)
	current := existing
	if portNeedsUpdate(desired, *current) {
		if current.Component.State == "RUNNING" {
			stopped, err := ports.UpdateOutputPortRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "STOPPED")
			if err != nil {
				return r.outputPortWriteFailed(ctx, instance, "NiFiUpdateFailed", "stop", err)
			}
			if stopped != nil {
				current = stopped
			}
		}
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = current.Revision.Version
		updated, err := ports.UpdateOutputPort(ctx, endpoint, updateEntity)
		if err != nil {
			return r.outputPortWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	current, pendingReason, pendingMsg, err := reconcilePortRunState(nifiID, current, nifiScheduledState(instance.Spec.State),
		func(id string, revisionVersion int64, state string) (*nifi.PortEntity, error) {
			return ports.UpdateOutputPortRunStatus(ctx, endpoint, id, revisionVersion, state)
		})
	if err != nil {
		return r.outputPortWriteFailed(ctx, instance, "NiFiStateFailed", "set run state of", err)
	}
	if pendingReason != "" {
		if shouldMarkOutputPortNotReady(instance, pendingReason, pendingMsg) {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, pendingReason, pendingMsg)
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if !outputPortStatusMatches(instance, nifiID, current.Revision.Version, parentID) {
		return ctrl.Result{}, markOutputPortReady(ctx, r.Client, instance, nifiID, current.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiOutputPortReconciler) outputPortWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiOutputPort, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi output port: %v", verb, err)
	if shouldMarkOutputPortNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markOutputPortNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

type NiFiConnectionReconciler struct {
	client.Client
	Scheme           *runtime.Scheme
	ConnectionClient nifi.ConnectionClient
}

func (r *NiFiConnectionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiConnection{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileConnectionDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, connectionDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markConnectionWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkConnectionNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markConnectionNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkConnectionNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markConnectionNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	entity, sourceID, destinationID, err := desiredConnection(ctx, r.Client, instance, parentID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if sourceID == "" || destinationID == "" {
		message := "The connection source or destination ID is not available yet."
		if shouldMarkConnectionNotReady(instance, "ConnectableIDMissing", message) {
			return ctrl.Result{}, markConnectionNotReady(ctx, r.Client, instance, "ConnectableIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	connections := r.ConnectionClient
	if connections == nil {
		connections = nifi.HTTPConnectionClient{}
	}
	if instance.Status.NiFiID != "" {
		existing, err := connections.GetConnection(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi connection: %v", err)
			if shouldMarkConnectionNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markConnectionNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingConnection(ctx, instance, endpoint, connections, entity, existing, sourceID, destinationID)
	}
	created, err := connections.CreateConnection(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi connection: %v", err)
		if shouldMarkConnectionNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markConnectionNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty connection response."
		if shouldMarkConnectionNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markConnectionNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := connectionEntityID(*created)
	if !connectionStatusMatches(instance, nifiID, created.Revision.Version, sourceID, destinationID) {
		return ctrl.Result{}, markConnectionReady(ctx, r.Client, instance, nifiID, created.Revision.Version, sourceID, destinationID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiConnectionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiConnection{}, clusterRefIndexField, indexConnectionClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiConnection{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Watches(&nifiv1alpha1.NiFiProcessor{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessor)).
		Watches(&nifiv1alpha1.NiFiInputPort{}, handler.EnqueueRequestsFromMapFunc(r.requestsForInputPort)).
		Watches(&nifiv1alpha1.NiFiOutputPort{}, handler.EnqueueRequestsFromMapFunc(r.requestsForOutputPort)).
		Watches(&nifiv1alpha1.NiFiFunnel{}, handler.EnqueueRequestsFromMapFunc(r.requestsForFunnel)).
		Watches(&nifiv1alpha1.NiFiRemoteProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForRemoteProcessGroup)).
		Complete(r)
}

func (r *NiFiConnectionReconciler) requestsForRemoteProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiConnectionList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if connectableReferenceMatches(item.Namespace, item.Spec.Source, nifiv1alpha1.ConnectableTypeRemoteInputPort, obj) ||
			connectableReferenceMatches(item.Namespace, item.Spec.Source, nifiv1alpha1.ConnectableTypeRemoteOutputPort, obj) ||
			connectableReferenceMatches(item.Namespace, item.Spec.Destination, nifiv1alpha1.ConnectableTypeRemoteInputPort, obj) ||
			connectableReferenceMatches(item.Namespace, item.Spec.Destination, nifiv1alpha1.ConnectableTypeRemoteOutputPort, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiConnectionReconciler) reconcileConnectionDelete(ctx context.Context, instance *nifiv1alpha1.NiFiConnection) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	connections := r.ConnectionClient
	if connections == nil {
		connections = nifi.HTTPConnectionClient{}
	}
	current, err := connections.GetConnection(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := connections.DeleteConnection(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiConnectionReconciler) reconcileExistingConnection(ctx context.Context, instance *nifiv1alpha1.NiFiConnection, endpoint string, connections nifi.ConnectionClient, desired nifi.ConnectionEntity, existing *nifi.ConnectionEntity, sourceID string, destinationID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty connection response."
		if shouldMarkConnectionNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markConnectionNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := connectionEntityID(*existing)
	if connectionNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := connections.UpdateConnection(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi connection: %v", err)
			if shouldMarkConnectionNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markConnectionNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			nifiID = connectionEntityID(*updated)
			return ctrl.Result{}, markConnectionReady(ctx, r.Client, instance, nifiID, updated.Revision.Version, sourceID, destinationID)
		}
	}
	if !connectionStatusMatches(instance, nifiID, existing.Revision.Version, sourceID, destinationID) {
		return ctrl.Result{}, markConnectionReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, sourceID, destinationID)
	}
	return ctrl.Result{}, nil
}

type NiFiReportingTaskReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	ReportingTaskClient nifi.ReportingTaskClient
}

func (r *NiFiReportingTaskReconciler) reportingTaskClient() nifi.ReportingTaskClient {
	if r.ReportingTaskClient != nil {
		return r.ReportingTaskClient
	}
	return nifi.HTTPReportingTaskClient{}
}

func (r *NiFiReportingTaskReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiReportingTask{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileReportingTaskDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markReportingTaskWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkReportingTaskNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markReportingTaskNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	entity, waitingFor, err := r.desiredReportingTask(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markReportingTaskWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	reportingTasks := r.reportingTaskClient()
	if instance.Status.NiFiID != "" {
		existing, err := reportingTasks.GetReportingTask(ctx, endpoint, instance.Status.NiFiID)
		if err != nil && !nifi.IsNotFound(err) {
			message := fmt.Sprintf("Failed to get NiFi reporting task: %v", err)
			if shouldMarkReportingTaskNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingReportingTask(ctx, instance, endpoint, reportingTasks, entity, existing)
		}
	}
	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := reportingTasks.GetReportingTask(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi reporting task: %v", err)
			if shouldMarkReportingTaskNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingReportingTask(ctx, instance, endpoint, reportingTasks, entity, existing)
		}
	}

	created, err := reportingTasks.CreateReportingTask(ctx, endpoint, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi reporting task: %v", err)
		if shouldMarkReportingTaskNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty reporting task response."
		if shouldMarkReportingTaskNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	// A freshly created task has no config drift; reconcileExistingReportingTask then applies the
	// desired run state (a reporting task is created STOPPED).
	return r.reconcileExistingReportingTask(ctx, instance, endpoint, reportingTasks, entity, created)
}

// reconcileExistingReportingTask brings an existing NiFi reporting task to the desired config and
// run state. Config changes require the task to be STOPPED, so it is stopped first if running; the
// desired run state (RUNNING/STOPPED) is then applied through the run-status endpoint.
func (r *NiFiReportingTaskReconciler) reconcileExistingReportingTask(ctx context.Context, instance *nifiv1alpha1.NiFiReportingTask, endpoint string, reportingTasks nifi.ReportingTaskClient, desired nifi.ReportingTaskEntity, existing *nifi.ReportingTaskEntity) (ctrl.Result, error) {
	nifiID := nifi.ReportingTaskEntityID(*existing)
	current := existing

	if reportingTaskNeedsUpdate(desired, *current) {
		if current.Component.State == "RUNNING" {
			stopped, err := reportingTasks.UpdateReportingTaskRunStatus(ctx, endpoint, nifiID, current.Revision.Version, "STOPPED")
			if err != nil {
				return r.reportingTaskWriteFailed(ctx, instance, "NiFiUpdateFailed", "stop", err)
			}
			if stopped != nil {
				current = stopped
			}
		}
		update := desired
		update.ID = nifiID
		update.Component.ID = nifiID
		update.Revision.Version = current.Revision.Version
		updated, err := reportingTasks.UpdateReportingTask(ctx, endpoint, update)
		if err != nil {
			return r.reportingTaskWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	desiredState := nifi.ReportingTaskRunState(instance.Spec.State == nifiv1alpha1.RuntimeStateEnabled)
	if desiredState == "RUNNING" {
		switch current.Component.ValidationStatus {
		case "INVALID":
			message := "The reporting task is INVALID and cannot be started; check its properties and bundle."
			if shouldMarkReportingTaskNotReady(instance, "ReportingTaskInvalid", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, "ReportingTaskInvalid", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		case "VALIDATING":
			// NiFi is still validating; re-check shortly before attempting to start.
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}
	if current.Component.State != desiredState {
		changed, err := reportingTasks.UpdateReportingTaskRunStatus(ctx, endpoint, nifiID, current.Revision.Version, desiredState)
		if err != nil {
			return r.reportingTaskWriteFailed(ctx, instance, "NiFiStateFailed", "set run state of", err)
		}
		if changed != nil {
			current = changed
		}
	}

	if !reportingTaskStatusMatches(instance, nifiID, current.Revision.Version, current.Component.ValidationStatus) {
		return ctrl.Result{}, markReportingTaskReady(ctx, r.Client, instance, nifiID, current.Revision.Version, current.Component.ValidationStatus)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiReportingTaskReconciler) reportingTaskWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiReportingTask, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi reporting task: %v", verb, err)
	if shouldMarkReportingTaskNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markReportingTaskNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *NiFiReportingTaskReconciler) reconcileReportingTaskDelete(ctx context.Context, instance *nifiv1alpha1.NiFiReportingTask) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its reporting task) is gone; nothing to delete remotely.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	reportingTasks := r.reportingTaskClient()
	// A running reporting task cannot be deleted; read it for the current revision, stop it if
	// needed, then delete.
	current, err := reportingTasks.GetReportingTask(ctx, endpoint, instance.Status.NiFiID)
	if err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	if current != nil {
		revision := current.Revision.Version
		if current.Component.State == "RUNNING" {
			stopped, err := reportingTasks.UpdateReportingTaskRunStatus(ctx, endpoint, instance.Status.NiFiID, revision, "STOPPED")
			if err != nil {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, err
			}
			if stopped != nil {
				revision = stopped.Revision.Version
			}
		}
		if err := reportingTasks.DeleteReportingTask(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiReportingTaskReconciler) desiredReportingTask(ctx context.Context, instance *nifiv1alpha1.NiFiReportingTask) (nifi.ReportingTaskEntity, []string, error) {
	properties := make(map[string]string, len(instance.Spec.Properties)+len(instance.Spec.SensitiveProperties))
	for key, value := range instance.Spec.Properties {
		properties[key] = value
	}
	waitingFor := make([]string, 0)
	for propertyName, source := range instance.Spec.SensitiveProperties {
		if source.SecretKeyRef == nil {
			waitingFor = append(waitingFor, fmt.Sprintf("sensitiveProperties.%s.secretKeyRef", propertyName))
			continue
		}
		secretRef := source.SecretKeyRef
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: secretRef.Name, Namespace: instance.Namespace}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s", instance.Namespace, secretRef.Name))
				continue
			}
			return nifi.ReportingTaskEntity{}, nil, err
		}
		data, ok := secret.Data[secretRef.Key]
		if !ok {
			if secretRef.Optional != nil && *secretRef.Optional {
				properties[propertyName] = ""
			} else {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s:%s", instance.Namespace, secretRef.Name, secretRef.Key))
			}
			continue
		}
		properties[propertyName] = string(data)
	}
	if len(properties) == 0 {
		properties = nil
	}

	component := nifi.ReportingTaskComponent{
		Name:               instance.Name,
		Type:               instance.Spec.Type,
		Properties:         properties,
		SchedulingStrategy: instance.Spec.Scheduling.Strategy,
		SchedulingPeriod:   instance.Spec.Scheduling.Period,
	}
	if instance.Spec.Bundle != nil {
		component.Bundle = &nifi.Bundle{
			Group:    instance.Spec.Bundle.Group,
			Artifact: instance.Spec.Bundle.Artifact,
			Version:  instance.Spec.Bundle.Version,
		}
	}
	return nifi.ReportingTaskEntity{Revision: nifi.Revision{Version: 0}, Component: component}, waitingFor, nil
}

func reportingTaskNeedsUpdate(desired nifi.ReportingTaskEntity, existing nifi.ReportingTaskEntity) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type ||
		desired.Component.SchedulingStrategy != "" && desired.Component.SchedulingStrategy != existing.Component.SchedulingStrategy ||
		desired.Component.SchedulingPeriod != "" && desired.Component.SchedulingPeriod != existing.Component.SchedulingPeriod {
		return true
	}
	if !nifiBundlesEqual(desired.Component.Bundle, existing.Component.Bundle) {
		return true
	}
	return !stringMapsEqual(desired.Component.Properties, existing.Component.Properties)
}

func reportingTaskStatusMatches(instance *nifiv1alpha1.NiFiReportingTask, nifiID string, revisionVersion int64, validationStatus string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ValidationStatus == validationStatus
}

func shouldMarkReportingTaskNotReady(instance *nifiv1alpha1.NiFiReportingTask, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func (r *NiFiReportingTaskReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiReportingTask{}, clusterRefIndexField, indexReportingTaskClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiReportingTask{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiParameterProviderReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	ParameterProviderClient nifi.ParameterProviderClient
}

func (r *NiFiParameterProviderReconciler) parameterProviderClient() nifi.ParameterProviderClient {
	if r.ParameterProviderClient != nil {
		return r.ParameterProviderClient
	}
	return nifi.HTTPParameterProviderClient{}
}

func (r *NiFiParameterProviderReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiParameterProvider{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileParameterProviderDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markParameterProviderWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkParameterProviderNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markParameterProviderNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	entity, waitingFor, err := r.desiredParameterProvider(ctx, instance)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markParameterProviderWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	providers := r.parameterProviderClient()
	if instance.Status.NiFiID != "" {
		existing, err := providers.GetParameterProvider(ctx, endpoint, instance.Status.NiFiID)
		if err != nil && !nifi.IsNotFound(err) {
			message := fmt.Sprintf("Failed to get NiFi parameter provider: %v", err)
			if shouldMarkParameterProviderNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterProviderNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingParameterProvider(ctx, instance, endpoint, providers, entity, existing)
		}
	}
	if instance.Spec.AdoptionPolicy.Mode == nifiv1alpha1.AdoptionPolicyAdoptByID && instance.Spec.AdoptionPolicy.NiFiID != "" {
		existing, err := providers.GetParameterProvider(ctx, endpoint, instance.Spec.AdoptionPolicy.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to adopt NiFi parameter provider: %v", err)
			if shouldMarkParameterProviderNotReady(instance, "AdoptionFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterProviderNotReady(ctx, r.Client, instance, "AdoptionFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if existing != nil {
			return r.reconcileExistingParameterProvider(ctx, instance, endpoint, providers, entity, existing)
		}
	}

	created, err := providers.CreateParameterProvider(ctx, endpoint, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi parameter provider: %v", err)
		if shouldMarkParameterProviderNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterProviderNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty parameter provider response."
		if shouldMarkParameterProviderNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterProviderNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	return r.reconcileExistingParameterProvider(ctx, instance, endpoint, providers, entity, created)
}

// reconcileExistingParameterProvider brings an existing NiFi parameter provider to the desired
// config. A parameter provider has no run state, so a change is a direct PUT with the current
// revision — no stop/start dance is needed.
func (r *NiFiParameterProviderReconciler) reconcileExistingParameterProvider(ctx context.Context, instance *nifiv1alpha1.NiFiParameterProvider, endpoint string, providers nifi.ParameterProviderClient, desired nifi.ParameterProviderEntity, existing *nifi.ParameterProviderEntity) (ctrl.Result, error) {
	nifiID := nifi.ParameterProviderEntityID(*existing)
	current := existing

	if parameterProviderNeedsUpdate(desired, *current) {
		update := desired
		update.ID = nifiID
		update.Component.ID = nifiID
		update.Revision.Version = current.Revision.Version
		updated, err := providers.UpdateParameterProvider(ctx, endpoint, update)
		if err != nil {
			return r.parameterProviderWriteFailed(ctx, instance, "NiFiUpdateFailed", "update", err)
		}
		if updated != nil {
			current = updated
		}
	}

	if !parameterProviderStatusMatches(instance, nifiID, current.Revision.Version, current.Component.ValidationStatus) {
		return ctrl.Result{}, markParameterProviderReady(ctx, r.Client, instance, nifiID, current.Revision.Version, current.Component.ValidationStatus)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiParameterProviderReconciler) parameterProviderWriteFailed(ctx context.Context, instance *nifiv1alpha1.NiFiParameterProvider, reason, verb string, err error) (ctrl.Result, error) {
	message := fmt.Sprintf("Failed to %s NiFi parameter provider: %v", verb, err)
	if shouldMarkParameterProviderNotReady(instance, reason, message) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, markParameterProviderNotReady(ctx, r.Client, instance, reason, message)
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func (r *NiFiParameterProviderReconciler) reconcileParameterProviderDelete(ctx context.Context, instance *nifiv1alpha1.NiFiParameterProvider) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and its parameter provider) is gone; nothing to delete remotely.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	providers := r.parameterProviderClient()
	// Read the provider for its current revision, then delete. Refetching avoids a stale-revision
	// rejection if the provider changed since the CR last observed it.
	current, err := providers.GetParameterProvider(ctx, endpoint, instance.Status.NiFiID)
	if err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	if current != nil {
		if err := providers.DeleteParameterProvider(ctx, endpoint, instance.Status.NiFiID, current.Revision.Version); err != nil && !nifi.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, err
		}
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiParameterProviderReconciler) desiredParameterProvider(ctx context.Context, instance *nifiv1alpha1.NiFiParameterProvider) (nifi.ParameterProviderEntity, []string, error) {
	properties := make(map[string]string, len(instance.Spec.Properties)+len(instance.Spec.SensitiveProperties))
	for key, value := range instance.Spec.Properties {
		properties[key] = value
	}
	waitingFor := make([]string, 0)
	for propertyName, source := range instance.Spec.SensitiveProperties {
		if source.SecretKeyRef == nil {
			waitingFor = append(waitingFor, fmt.Sprintf("sensitiveProperties.%s.secretKeyRef", propertyName))
			continue
		}
		secretRef := source.SecretKeyRef
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: secretRef.Name, Namespace: instance.Namespace}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s", instance.Namespace, secretRef.Name))
				continue
			}
			return nifi.ParameterProviderEntity{}, nil, err
		}
		data, ok := secret.Data[secretRef.Key]
		if !ok {
			if secretRef.Optional != nil && *secretRef.Optional {
				properties[propertyName] = ""
			} else {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s:%s", instance.Namespace, secretRef.Name, secretRef.Key))
			}
			continue
		}
		properties[propertyName] = string(data)
	}
	if len(properties) == 0 {
		properties = nil
	}

	component := nifi.ParameterProviderComponent{
		Name:       instance.Name,
		Type:       instance.Spec.Type,
		Properties: properties,
	}
	if instance.Spec.Bundle != nil {
		component.Bundle = &nifi.Bundle{
			Group:    instance.Spec.Bundle.Group,
			Artifact: instance.Spec.Bundle.Artifact,
			Version:  instance.Spec.Bundle.Version,
		}
	}
	return nifi.ParameterProviderEntity{Revision: nifi.Revision{Version: 0}, Component: component}, waitingFor, nil
}

func parameterProviderNeedsUpdate(desired nifi.ParameterProviderEntity, existing nifi.ParameterProviderEntity) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type {
		return true
	}
	if !nifiBundlesEqual(desired.Component.Bundle, existing.Component.Bundle) {
		return true
	}
	return !stringMapsEqual(desired.Component.Properties, existing.Component.Properties)
}

func parameterProviderStatusMatches(instance *nifiv1alpha1.NiFiParameterProvider, nifiID string, revisionVersion int64, validationStatus string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ValidationStatus == validationStatus
}

func shouldMarkParameterProviderNotReady(instance *nifiv1alpha1.NiFiParameterProvider, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func (r *NiFiParameterProviderReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiParameterProvider{}, clusterRefIndexField, indexParameterProviderClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiParameterProvider{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiFunnelReconciler struct {
	client.Client
	Scheme       *runtime.Scheme
	FunnelClient nifi.FunnelClient
}

func (r *NiFiFunnelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiFunnel{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileFunnelDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, funnelDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markFunnelWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkFunnelNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markFunnelNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkFunnelNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markFunnelNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	funnels := r.FunnelClient
	if funnels == nil {
		funnels = nifi.HTTPFunnelClient{}
	}
	entity := desiredFunnel(instance, parentID)
	if instance.Status.NiFiID != "" {
		existing, err := funnels.GetFunnel(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi funnel: %v", err)
			if shouldMarkFunnelNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markFunnelNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingFunnel(ctx, instance, endpoint, funnels, entity, existing, parentID)
	}
	created, err := funnels.CreateFunnel(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi funnel: %v", err)
		if shouldMarkFunnelNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFunnelNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty funnel response."
		if shouldMarkFunnelNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFunnelNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := funnelEntityID(*created)
	if !funnelStatusMatches(instance, nifiID, created.Revision.Version, parentID) {
		return ctrl.Result{}, markFunnelReady(ctx, r.Client, instance, nifiID, created.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiFunnelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiFunnel{}, clusterRefIndexField, indexFunnelClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiFunnel{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiFunnelReconciler) reconcileFunnelDelete(ctx context.Context, instance *nifiv1alpha1.NiFiFunnel) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	funnels := r.FunnelClient
	if funnels == nil {
		funnels = nifi.HTTPFunnelClient{}
	}
	current, err := funnels.GetFunnel(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := funnels.DeleteFunnel(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiFunnelReconciler) reconcileExistingFunnel(ctx context.Context, instance *nifiv1alpha1.NiFiFunnel, endpoint string, funnels nifi.FunnelClient, desired nifi.FunnelEntity, existing *nifi.FunnelEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty funnel response."
		if shouldMarkFunnelNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markFunnelNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := funnelEntityID(*existing)
	if funnelNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := funnels.UpdateFunnel(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi funnel: %v", err)
			if shouldMarkFunnelNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markFunnelNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			nifiID = funnelEntityID(*updated)
			return ctrl.Result{}, markFunnelReady(ctx, r.Client, instance, nifiID, updated.Revision.Version, parentID)
		}
	}
	if !funnelStatusMatches(instance, nifiID, existing.Revision.Version, parentID) {
		return ctrl.Result{}, markFunnelReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

type NiFiLabelReconciler struct {
	client.Client
	Scheme      *runtime.Scheme
	LabelClient nifi.LabelClient
}

func (r *NiFiLabelReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiLabel{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return r.reconcileLabelDelete(ctx, instance)
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	waitingFor = append(waitingFor, labelDependenciesWaitingFor(ctx, r.Client, instance)...)
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markLabelWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		message := "Referenced NiFiCluster is ready but does not expose a NiFi API endpoint."
		if shouldMarkLabelNotReady(instance, "ClusterEndpointMissing", message) {
			return ctrl.Result{}, markLabelNotReady(ctx, r.Client, instance, "ClusterEndpointMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	parentID, err := processGroupParentID(ctx, r.Client, instance.Namespace, cluster, instance.Spec.ParentProcessGroupRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if parentID == "" {
		message := "The parent process group ID is not available yet."
		if shouldMarkLabelNotReady(instance, "ParentProcessGroupIDMissing", message) {
			return ctrl.Result{}, markLabelNotReady(ctx, r.Client, instance, "ParentProcessGroupIDMissing", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	labels := r.LabelClient
	if labels == nil {
		labels = nifi.HTTPLabelClient{}
	}
	entity := desiredLabel(instance, parentID)
	if instance.Status.NiFiID != "" {
		existing, err := labels.GetLabel(ctx, endpoint, instance.Status.NiFiID)
		if err != nil {
			message := fmt.Sprintf("Failed to get NiFi label: %v", err)
			if shouldMarkLabelNotReady(instance, "NiFiGetFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markLabelNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return r.reconcileExistingLabel(ctx, instance, endpoint, labels, entity, existing, parentID)
	}
	created, err := labels.CreateLabel(ctx, endpoint, parentID, entity)
	if err != nil {
		message := fmt.Sprintf("Failed to create NiFi label: %v", err)
		if shouldMarkLabelNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markLabelNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	if created == nil {
		message := "NiFi returned an empty label response."
		if shouldMarkLabelNotReady(instance, "NiFiCreateFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markLabelNotReady(ctx, r.Client, instance, "NiFiCreateFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := labelEntityID(*created)
	if !labelStatusMatches(instance, nifiID, created.Revision.Version, parentID) {
		return ctrl.Result{}, markLabelReady(ctx, r.Client, instance, nifiID, created.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiLabelReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiLabel{}, clusterRefIndexField, indexLabelClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiLabel{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Watches(&nifiv1alpha1.NiFiProcessGroup{}, handler.EnqueueRequestsFromMapFunc(r.requestsForProcessGroup)).
		Complete(r)
}

func (r *NiFiLabelReconciler) reconcileLabelDelete(ctx context.Context, instance *nifiv1alpha1.NiFiLabel) (ctrl.Result, error) {
	if instance.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete || instance.Status.NiFiID == "" {
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	cluster, gone, err := clusterForDeletion(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if gone {
		// The cluster (and this component with it) is gone; drop the finalizer instead of waiting
		// forever for a cluster that will never return.
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if cluster == nil {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	labels := r.LabelClient
	if labels == nil {
		labels = nifi.HTTPLabelClient{}
	}
	current, err := labels.GetLabel(ctx, endpoint, instance.Status.NiFiID)
	if err != nil {
		if nifi.IsNotFound(err) {
			_, err := removeFinalizer(ctx, r.Client, instance)
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	revision := instance.Status.Revision.Version
	if current != nil {
		revision = current.Revision.Version
	}
	if err := labels.DeleteLabel(ctx, endpoint, instance.Status.NiFiID, revision); err != nil && !nifi.IsNotFound(err) {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, err
	}
	_, err = removeFinalizer(ctx, r.Client, instance)
	return ctrl.Result{}, err
}

func (r *NiFiLabelReconciler) reconcileExistingLabel(ctx context.Context, instance *nifiv1alpha1.NiFiLabel, endpoint string, labels nifi.LabelClient, desired nifi.LabelEntity, existing *nifi.LabelEntity, parentID string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty label response."
		if shouldMarkLabelNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markLabelNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}
	nifiID := labelEntityID(*existing)
	if labelNeedsUpdate(desired, *existing) {
		updateEntity := desired
		updateEntity.ID = nifiID
		updateEntity.Component.ID = nifiID
		updateEntity.Revision.Version = existing.Revision.Version
		updated, err := labels.UpdateLabel(ctx, endpoint, updateEntity)
		if err != nil {
			message := fmt.Sprintf("Failed to update NiFi label: %v", err)
			if shouldMarkLabelNotReady(instance, "NiFiUpdateFailed", message) {
				return ctrl.Result{RequeueAfter: 30 * time.Second}, markLabelNotReady(ctx, r.Client, instance, "NiFiUpdateFailed", message)
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		if updated != nil {
			nifiID = labelEntityID(*updated)
			return ctrl.Result{}, markLabelReady(ctx, r.Client, instance, nifiID, updated.Revision.Version, parentID)
		}
	}
	if !labelStatusMatches(instance, nifiID, existing.Revision.Version, parentID) {
		return ctrl.Result{}, markLabelReady(ctx, r.Client, instance, nifiID, existing.Revision.Version, parentID)
	}
	return ctrl.Result{}, nil
}

func indexRegistryClientClusterRef(obj client.Object) []string {
	registryClient, ok := obj.(*nifiv1alpha1.NiFiRegistryClient)
	if !ok {
		return nil
	}
	return indexClusterRef(registryClient.Namespace, registryClient.Spec.ClusterRef)
}

func indexParameterContextClusterRef(obj client.Object) []string {
	parameterContext, ok := obj.(*nifiv1alpha1.NiFiParameterContext)
	if !ok {
		return nil
	}
	return indexClusterRef(parameterContext.Namespace, parameterContext.Spec.ClusterRef)
}

func indexUserClusterRef(obj client.Object) []string {
	user, ok := obj.(*nifiv1alpha1.NiFiUser)
	if !ok {
		return nil
	}
	return indexClusterRef(user.Namespace, user.Spec.ClusterRef)
}

func indexUserGroupClusterRef(obj client.Object) []string {
	userGroup, ok := obj.(*nifiv1alpha1.NiFiUserGroup)
	if !ok {
		return nil
	}
	return indexClusterRef(userGroup.Namespace, userGroup.Spec.ClusterRef)
}

func indexProcessGroupClusterRef(obj client.Object) []string {
	processGroup, ok := obj.(*nifiv1alpha1.NiFiProcessGroup)
	if !ok {
		return nil
	}
	return indexClusterRef(processGroup.Namespace, processGroup.Spec.ClusterRef)
}

func indexControllerServiceClusterRef(obj client.Object) []string {
	controllerService, ok := obj.(*nifiv1alpha1.NiFiControllerService)
	if !ok {
		return nil
	}
	return indexClusterRef(controllerService.Namespace, controllerService.Spec.ClusterRef)
}

func indexFlowDeploymentClusterRef(obj client.Object) []string {
	flowDeployment, ok := obj.(*nifiv1alpha1.NiFiFlowDeployment)
	if !ok {
		return nil
	}
	return indexClusterRef(flowDeployment.Namespace, flowDeployment.Spec.ClusterRef)
}

func indexProcessorClusterRef(obj client.Object) []string {
	processor, ok := obj.(*nifiv1alpha1.NiFiProcessor)
	if !ok {
		return nil
	}
	return indexClusterRef(processor.Namespace, processor.Spec.ClusterRef)
}

func indexInputPortClusterRef(obj client.Object) []string {
	inputPort, ok := obj.(*nifiv1alpha1.NiFiInputPort)
	if !ok {
		return nil
	}
	return indexClusterRef(inputPort.Namespace, inputPort.Spec.ClusterRef)
}

func indexOutputPortClusterRef(obj client.Object) []string {
	outputPort, ok := obj.(*nifiv1alpha1.NiFiOutputPort)
	if !ok {
		return nil
	}
	return indexClusterRef(outputPort.Namespace, outputPort.Spec.ClusterRef)
}

func indexConnectionClusterRef(obj client.Object) []string {
	connection, ok := obj.(*nifiv1alpha1.NiFiConnection)
	if !ok {
		return nil
	}
	return indexClusterRef(connection.Namespace, connection.Spec.ClusterRef)
}

func indexReportingTaskClusterRef(obj client.Object) []string {
	reportingTask, ok := obj.(*nifiv1alpha1.NiFiReportingTask)
	if !ok {
		return nil
	}
	return indexClusterRef(reportingTask.Namespace, reportingTask.Spec.ClusterRef)
}

func indexParameterProviderClusterRef(obj client.Object) []string {
	provider, ok := obj.(*nifiv1alpha1.NiFiParameterProvider)
	if !ok {
		return nil
	}
	return indexClusterRef(provider.Namespace, provider.Spec.ClusterRef)
}

func indexFunnelClusterRef(obj client.Object) []string {
	funnel, ok := obj.(*nifiv1alpha1.NiFiFunnel)
	if !ok {
		return nil
	}
	return indexClusterRef(funnel.Namespace, funnel.Spec.ClusterRef)
}

func indexLabelClusterRef(obj client.Object) []string {
	label, ok := obj.(*nifiv1alpha1.NiFiLabel)
	if !ok {
		return nil
	}
	return indexClusterRef(label.Namespace, label.Spec.ClusterRef)
}

func indexClusterRef(namespace string, ref nifiv1alpha1.ClusterReference) []string {
	value := clusterRefIndexValue(namespace, ref)
	if value == "" {
		return nil
	}
	return []string{value}
}

func (r *NiFiRegistryClientReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiRegistryClientList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiParameterContextReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiParameterContextList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiUserReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiUserList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiUserGroupReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiUserGroupList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiProcessGroupReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiProcessGroupList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiControllerServiceReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiControllerServiceList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiFlowDeploymentReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowDeploymentList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiProcessorReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiProcessorList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiInputPortReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiInputPortList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiOutputPortReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiOutputPortList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiConnectionReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiConnectionList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiReportingTaskReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiReportingTaskList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiParameterProviderReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiParameterProviderList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiFunnelReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFunnelList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiLabelReconciler) requestsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiLabelList{}
	if err := listByClusterRef(ctx, r.Client, obj, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
	}
	return requests
}

func (r *NiFiProcessGroupReconciler) requestsForParameterContext(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiProcessGroupList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if item.Spec.ParameterContextRef != nil && localObjectReferenceMatches(item.Namespace, *item.Spec.ParameterContextRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiProcessGroupReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiProcessGroupList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiControllerServiceReconciler) requestsForParameterContext(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiControllerServiceList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if item.Spec.ParameterContextRef != nil && localObjectReferenceMatches(item.Namespace, *item.Spec.ParameterContextRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiControllerServiceReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiControllerServiceList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFlowBundleReconciler) requestsForRegistryClient(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowBundleList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if item.Spec.Source.Registry != nil && localObjectReferenceMatches(item.Namespace, item.Spec.Source.Registry.RegistryClientRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiClusterReconciler) requestsForAPISecret(ctx context.Context, obj client.Object) []reconcile.Request {
	// A managed TLS Secret (keystore/truststore/CA) is owned by its NiFiCluster; enqueue
	// the owner directly so certificate rotation re-reconciles and rolls the StatefulSet
	// through the TLS checksum annotation.
	if secret, ok := obj.(*corev1.Secret); ok {
		if owner, found := managedClusterSecretOwner(secret); found {
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: owner, Namespace: obj.GetNamespace()}}}
		}
	}
	list := &nifiv1alpha1.NiFiClusterList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for _, item := range list.Items {
		if clusterAPIReferencesSecret(&item, obj.GetName()) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFlowBundleReconciler) requestsForCredentialSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowBundleList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for _, item := range list.Items {
		if flowSourceReferencesSecret(&item.Spec.Source, obj.GetName()) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFlowDeploymentReconciler) requestsForCredentialSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowDeploymentList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	requests := []reconcile.Request{}
	for _, item := range list.Items {
		if item.Spec.Source.Inline != nil && flowSourceReferencesSecret(item.Spec.Source.Inline, obj.GetName()) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func clusterAPIReferencesSecret(cluster *nifiv1alpha1.NiFiCluster, secretName string) bool {
	// Secret-sourced configuration overrides: a content change must re-render the
	// overrides payload and roll the nodes through the checksum annotation.
	if cluster.Spec.ConfigOverrides != nil {
		for _, reference := range cluster.Spec.ConfigOverrides.NiFiPropertiesFrom {
			if reference.Name == secretName {
				return true
			}
		}
	}
	if cluster.Spec.API == nil {
		return false
	}
	refs := []*nifiv1alpha1.SecretKeyRef{}
	if cluster.Spec.API.TLS != nil {
		refs = append(refs, cluster.Spec.API.TLS.CASecretKeyRef)
	}
	if cluster.Spec.API.Auth != nil {
		refs = append(refs, cluster.Spec.API.Auth.BearerTokenSecretKeyRef, cluster.Spec.API.Auth.UsernameSecretKeyRef, cluster.Spec.API.Auth.PasswordSecretKeyRef)
		if cluster.Spec.API.Auth.ClientCertificate != nil && cluster.Spec.API.Auth.ClientCertificate.SecretName == secretName {
			return true
		}
	}
	return secretRefsContainName(refs, secretName)
}

func flowSourceReferencesSecret(source *nifiv1alpha1.FlowBundleSource, secretName string) bool {
	if source == nil {
		return false
	}
	var credentials *nifiv1alpha1.FlowArtifactCredentials
	switch {
	case source.Git != nil:
		credentials = source.Git.Credentials
	case source.OCI != nil:
		credentials = source.OCI.Credentials
	case source.Registry != nil:
		credentials = source.Registry.Credentials
	}
	refs := []*nifiv1alpha1.SecretKeyRef{}
	if credentials != nil {
		refs = append(refs, credentials.UsernameSecretKeyRef, credentials.PasswordSecretKeyRef, credentials.TokenSecretKeyRef, credentials.CASecretKeyRef,
			credentials.SSHPrivateKeySecretKeyRef, credentials.SSHPrivateKeyPassphraseSecretKeyRef, credentials.SSHKnownHostsSecretKeyRef,
			credentials.ClientCertificateSecretKeyRef, credentials.ClientKeySecretKeyRef)
		if credentials.OIDC != nil {
			refs = append(refs, credentials.OIDC.ClientIDSecretKeyRef, credentials.OIDC.ClientSecretSecretKeyRef)
		}
	}
	if source.OCI != nil && source.OCI.Verify != nil {
		refs = append(refs, source.OCI.Verify.CosignPublicKeySecretRef)
	}
	return secretRefsContainName(refs, secretName)
}

func secretRefsContainName(refs []*nifiv1alpha1.SecretKeyRef, name string) bool {
	for _, ref := range refs {
		if ref != nil && ref.Name == name {
			return true
		}
	}
	return false
}

func (r *NiFiFlowDeploymentReconciler) requestsForFlowBundle(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowDeploymentList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if item.Spec.Source.BundleRef != nil && localObjectReferenceMatches(item.Namespace, *item.Spec.Source.BundleRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFlowDeploymentReconciler) requestsForParameterContext(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowDeploymentList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if item.Spec.ParameterContextRef != nil && localObjectReferenceMatches(item.Namespace, *item.Spec.ParameterContextRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFlowDeploymentReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFlowDeploymentList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.Target.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiProcessorReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiProcessorList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiInputPortReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiInputPortList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiOutputPortReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiOutputPortList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiConnectionReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiConnectionList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiConnectionReconciler) requestsForProcessor(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForConnectable(ctx, obj, nifiv1alpha1.ConnectableTypeProcessor)
}

func (r *NiFiConnectionReconciler) requestsForInputPort(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForConnectable(ctx, obj, nifiv1alpha1.ConnectableTypeInputPort)
}

func (r *NiFiConnectionReconciler) requestsForOutputPort(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForConnectable(ctx, obj, nifiv1alpha1.ConnectableTypeOutputPort)
}

func (r *NiFiConnectionReconciler) requestsForFunnel(ctx context.Context, obj client.Object) []reconcile.Request {
	return r.requestsForConnectable(ctx, obj, nifiv1alpha1.ConnectableTypeFunnel)
}

func (r *NiFiConnectionReconciler) requestsForConnectable(ctx context.Context, obj client.Object, connectableType nifiv1alpha1.ConnectableType) []reconcile.Request {
	list := &nifiv1alpha1.NiFiConnectionList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if connectableReferenceMatches(item.Namespace, item.Spec.Source, connectableType, obj) ||
			connectableReferenceMatches(item.Namespace, item.Spec.Destination, connectableType, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiFunnelReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiFunnelList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func (r *NiFiLabelReconciler) requestsForProcessGroup(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &nifiv1alpha1.NiFiLabelList{}
	if err := r.List(ctx, list); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0, len(list.Items))
	for _, item := range list.Items {
		if processGroupReferenceMatches(item.Namespace, item.Spec.ParentProcessGroupRef, obj) {
			requests = append(requests, reconcile.Request{NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace}})
		}
	}
	return requests
}

func listByClusterRef(ctx context.Context, c client.Client, cluster client.Object, list client.ObjectList) error {
	indexValue := fmt.Sprintf("%s/%s", cluster.GetNamespace(), cluster.GetName())
	return c.List(ctx, list, client.MatchingFields{clusterRefIndexField: indexValue})
}

func userGroupMemberDependenciesWaitingFor(ctx context.Context, c client.Client, userGroup *nifiv1alpha1.NiFiUserGroup) []string {
	waitingFor := make([]string, 0)
	for _, member := range userGroup.Spec.Users {
		if member.UserRef.Name == "" {
			waitingFor = append(waitingFor, "users[].userRef.name")
			continue
		}
		namespace := userGroup.Namespace
		if member.UserRef.Namespace != "" {
			namespace = member.UserRef.Namespace
		}
		user := &nifiv1alpha1.NiFiUser{}
		key := types.NamespacedName{Name: member.UserRef.Name, Namespace: namespace}
		if err := c.Get(ctx, key, user); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s", namespace, member.UserRef.Name))
				continue
			}
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s:GetError", namespace, member.UserRef.Name))
			continue
		}
		if !user.Status.Ready {
			waitingFor = append(waitingFor, fmt.Sprintf("NiFiUser/%s/%s:Ready", namespace, member.UserRef.Name))
		}
	}
	return waitingFor
}

// userGroupMemberClusterMismatch returns the "namespace/name" of the first member NiFiUser whose
// resolved ClusterRef differs from the group's, or "" when every member belongs to the group's
// cluster. It is called only after the members are known to exist and be Ready.
func userGroupMemberClusterMismatch(ctx context.Context, c client.Client, userGroup *nifiv1alpha1.NiFiUserGroup) (string, error) {
	groupCluster := clusterRefIndexValue(userGroup.Namespace, userGroup.Spec.ClusterRef)
	for _, member := range userGroup.Spec.Users {
		if member.UserRef.Name == "" {
			continue
		}
		namespace := userGroup.Namespace
		if member.UserRef.Namespace != "" {
			namespace = member.UserRef.Namespace
		}
		user := &nifiv1alpha1.NiFiUser{}
		if err := c.Get(ctx, types.NamespacedName{Name: member.UserRef.Name, Namespace: namespace}, user); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", err
		}
		if clusterRefIndexValue(namespace, user.Spec.ClusterRef) != groupCluster {
			return fmt.Sprintf("%s/%s", namespace, member.UserRef.Name), nil
		}
	}
	return "", nil
}

func processGroupDependenciesWaitingFor(ctx context.Context, c client.Client, processGroup *nifiv1alpha1.NiFiProcessGroup) []string {
	waitingFor := processGroupReferenceDependencyWaitingFor(ctx, c, processGroup.Namespace, processGroup.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
	waitingFor = append(waitingFor, parameterContextDependencyWaitingFor(ctx, c, processGroup.Namespace, processGroup.Spec.ParameterContextRef, "parameterContextRef")...)
	return waitingFor
}

func desiredProcessGroup(ctx context.Context, c client.Client, processGroup *nifiv1alpha1.NiFiProcessGroup, parentID string) (nifi.ProcessGroupEntity, error) {
	component := nifi.ProcessGroupComponent{
		Name:          processGroupDisplayName(processGroup),
		Comments:      processGroup.Spec.Comments,
		ParentGroupID: parentID,
	}
	if processGroup.Spec.Position != nil {
		component.Position = &nifi.Position{X: float64(processGroup.Spec.Position.X), Y: float64(processGroup.Spec.Position.Y)}
	}
	if processGroup.Spec.ParameterContextRef != nil {
		parameterContext := &nifiv1alpha1.NiFiParameterContext{}
		ref := *processGroup.Spec.ParameterContextRef
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(processGroup.Namespace, ref)}
		if err := c.Get(ctx, key, parameterContext); err != nil {
			return nifi.ProcessGroupEntity{}, err
		}
		if parameterContext.Status.NiFiID != "" {
			component.ParameterContext = &nifi.ComponentReference{ID: parameterContext.Status.NiFiID}
		}
	}
	return nifi.ProcessGroupEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}, nil
}

func processGroupDisplayName(processGroup *nifiv1alpha1.NiFiProcessGroup) string {
	if processGroup.Spec.DisplayName != "" {
		return processGroup.Spec.DisplayName
	}
	return processGroup.Name
}

func processGroupParentID(ctx context.Context, c client.Client, namespace string, cluster *nifiv1alpha1.NiFiCluster, ref nifiv1alpha1.ProcessGroupReference) (string, error) {
	if ref.Root || ref.Name == "" {
		if cluster.Status.RootProcessGroupID != "" {
			return cluster.Status.RootProcessGroupID, nil
		}
		return "root", nil
	}
	parent := &nifiv1alpha1.NiFiProcessGroup{}
	key := types.NamespacedName{Name: ref.Name, Namespace: processGroupRefNamespace(namespace, ref)}
	if err := c.Get(ctx, key, parent); err != nil {
		return "", err
	}
	return parent.Status.NiFiID, nil
}

func clusterEndpoint(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Status.Endpoint != "" {
		return cluster.Status.Endpoint
	}
	if cluster.Spec.API != nil {
		return cluster.Spec.API.URI
	}
	return ""
}

func processGroupEntityID(entity nifi.ProcessGroupEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func processGroupNeedsUpdate(desired nifi.ProcessGroupEntity, existing nifi.ProcessGroupEntity) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Comments != existing.Component.Comments ||
		desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if !nifiPositionsEqual(desired.Component.Position, existing.Component.Position) {
		return true
	}
	return componentReferenceID(desired.Component.ParameterContext) != componentReferenceID(existing.Component.ParameterContext)
}

func processGroupStatusMatches(instance *nifiv1alpha1.NiFiProcessGroup, nifiID string, revisionVersion int64, parentID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID
}

func shouldMarkProcessGroupNotReady(instance *nifiv1alpha1.NiFiProcessGroup, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func nifiPositionsEqual(left *nifi.Position, right *nifi.Position) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.X == right.X && left.Y == right.Y
}

func nifiPositionSlicesEqual(left []nifi.Position, right []nifi.Position) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].X != right[i].X || left[i].Y != right[i].Y {
			return false
		}
	}
	return true
}

func nifiConnectablesEqual(left nifi.Connectable, right nifi.Connectable) bool {
	return left.ID == right.ID && left.Type == right.Type && left.GroupID == right.GroupID
}

func componentReferenceID(ref *nifi.ComponentReference) string {
	if ref == nil {
		return ""
	}
	return ref.ID
}

func nifiBundlesEqual(left *nifi.Bundle, right *nifi.Bundle) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Group == right.Group && left.Artifact == right.Artifact && left.Version == right.Version
}

func stringMapsEqual(left map[string]string, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		if right[key] != leftValue {
			return false
		}
	}
	return true
}

func stringSlicesEqual(left []string, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func controllerServiceDependenciesWaitingFor(ctx context.Context, c client.Client, controllerService *nifiv1alpha1.NiFiControllerService) []string {
	waitingFor := processGroupReferenceDependencyWaitingFor(ctx, c, controllerService.Namespace, controllerService.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
	waitingFor = append(waitingFor, parameterContextDependencyWaitingFor(ctx, c, controllerService.Namespace, controllerService.Spec.ParameterContextRef, "parameterContextRef")...)
	return waitingFor
}

func (r *NiFiControllerServiceReconciler) desiredControllerService(ctx context.Context, controllerService *nifiv1alpha1.NiFiControllerService, parentID string) (nifi.ControllerServiceEntity, []string, error) {
	properties := make(map[string]string, len(controllerService.Spec.Properties)+len(controllerService.Spec.SensitiveProperties))
	for key, value := range controllerService.Spec.Properties {
		properties[key] = value
	}
	waitingFor := make([]string, 0)
	for propertyName, source := range controllerService.Spec.SensitiveProperties {
		if source.SecretKeyRef == nil {
			waitingFor = append(waitingFor, fmt.Sprintf("sensitiveProperties.%s.secretKeyRef", propertyName))
			continue
		}
		secretRef := source.SecretKeyRef
		secret := &corev1.Secret{}
		key := types.NamespacedName{Name: secretRef.Name, Namespace: controllerService.Namespace}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s", controllerService.Namespace, secretRef.Name))
				continue
			}
			return nifi.ControllerServiceEntity{}, nil, err
		}
		data, ok := secret.Data[secretRef.Key]
		if !ok {
			if secretRef.Optional != nil && *secretRef.Optional {
				properties[propertyName] = ""
			} else {
				waitingFor = append(waitingFor, fmt.Sprintf("Secret/%s/%s:%s", controllerService.Namespace, secretRef.Name, secretRef.Key))
			}
			continue
		}
		properties[propertyName] = string(data)
	}
	if len(properties) == 0 {
		properties = nil
	}

	// State is intentionally omitted: a controller service is enabled/disabled through the
	// run-status endpoint, not a component write.
	component := nifi.ControllerServiceComponent{
		ParentGroupID: parentID,
		Name:          controllerService.Name,
		Type:          controllerService.Spec.Type,
		Properties:    properties,
	}
	if controllerService.Spec.Bundle != nil {
		component.Bundle = &nifi.Bundle{
			Group:    controllerService.Spec.Bundle.Group,
			Artifact: controllerService.Spec.Bundle.Artifact,
			Version:  controllerService.Spec.Bundle.Version,
		}
	}
	return nifi.ControllerServiceEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}, waitingFor, nil
}

func controllerServiceEntityID(entity nifi.ControllerServiceEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func controllerServiceNeedsUpdate(desired nifi.ControllerServiceEntity, existing nifi.ControllerServiceEntity) bool {
	// Enabled/disabled state is reconciled separately via the run-status endpoint, so it is
	// excluded here.
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type ||
		desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if !nifiBundlesEqual(desired.Component.Bundle, existing.Component.Bundle) {
		return true
	}
	return !stringMapsEqual(desired.Component.Properties, existing.Component.Properties)
}

func controllerServiceStatusMatches(instance *nifiv1alpha1.NiFiControllerService, nifiID string, revisionVersion int64, validationStatus string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ValidationStatus == validationStatus
}

func shouldMarkControllerServiceNotReady(instance *nifiv1alpha1.NiFiControllerService, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func flowBundleDependenciesWaitingFor(ctx context.Context, c client.Client, flowBundle *nifiv1alpha1.NiFiFlowBundle) []string {
	if flowBundle.Spec.Source.Registry == nil {
		return nil
	}
	return registryClientDependencyWaitingFor(ctx, c, flowBundle.Namespace, flowBundle.Spec.Source.Registry.RegistryClientRef, "source.registry.registryClientRef")
}

func flowBundleStatusMatches(instance *nifiv1alpha1.NiFiFlowBundle, artifactDigest string, resolvedRevision string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.ArtifactDigest == artifactDigest &&
		instance.Status.ResolvedRevision == resolvedRevision
}

func shouldMarkFlowBundleNotReady(instance *nifiv1alpha1.NiFiFlowBundle, reason string, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func flowDeploymentDependenciesWaitingFor(ctx context.Context, c client.Client, flowDeployment *nifiv1alpha1.NiFiFlowDeployment) []string {
	waitingFor := flowBundleDependencyWaitingFor(ctx, c, flowDeployment.Namespace, flowDeployment.Spec.Source.BundleRef, "source.bundleRef")
	waitingFor = append(waitingFor, flowBundleSourceDependenciesWaitingFor(ctx, c, flowDeployment.Namespace, flowDeployment.Spec.Source.Inline, "source.inline")...)
	waitingFor = append(waitingFor, processGroupReferenceDependencyWaitingFor(ctx, c, flowDeployment.Namespace, flowDeployment.Spec.Target.ParentProcessGroupRef, "target.parentProcessGroupRef")...)
	waitingFor = append(waitingFor, parameterContextDependencyWaitingFor(ctx, c, flowDeployment.Namespace, flowDeployment.Spec.ParameterContextRef, "parameterContextRef")...)
	return waitingFor
}

func desiredFlowDeploymentProcessGroup(ctx context.Context, c client.Client, flowDeployment *nifiv1alpha1.NiFiFlowDeployment, parentID string) (nifi.ProcessGroupEntity, string, string, error) {
	name := flowDeployment.Spec.Target.ProcessGroupName
	if name == "" {
		name = flowDeployment.Name
	}
	component := nifi.ProcessGroupComponent{
		Name:          name,
		ParentGroupID: parentID,
		Comments:      fmt.Sprintf("Managed by NiFiControl FlowDeployment %s/%s.", flowDeployment.Namespace, flowDeployment.Name),
	}
	if flowDeployment.Spec.ParameterContextRef != nil {
		ref := *flowDeployment.Spec.ParameterContextRef
		parameterContext := &nifiv1alpha1.NiFiParameterContext{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(flowDeployment.Namespace, ref)}
		if err := c.Get(ctx, key, parameterContext); err != nil {
			return nifi.ProcessGroupEntity{}, "", "", err
		}
		if parameterContext.Status.NiFiID != "" {
			component.ParameterContext = &nifi.ComponentReference{ID: parameterContext.Status.NiFiID}
		}
	}
	deployedVersion, artifactDigest, err := flowDeploymentSourceMetadata(ctx, c, flowDeployment)
	if err != nil {
		return nifi.ProcessGroupEntity{}, "", "", err
	}
	return nifi.ProcessGroupEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}, deployedVersion, artifactDigest, nil
}

func flowDeploymentSourceMetadata(ctx context.Context, c client.Client, flowDeployment *nifiv1alpha1.NiFiFlowDeployment) (string, string, error) {
	deployedVersion := flowDeployment.Spec.Source.Version
	artifactDigest := ""
	if flowDeployment.Spec.Source.BundleRef != nil {
		ref := *flowDeployment.Spec.Source.BundleRef
		bundle := &nifiv1alpha1.NiFiFlowBundle{}
		key := types.NamespacedName{Name: ref.Name, Namespace: localObjectRefNamespace(flowDeployment.Namespace, ref)}
		if err := c.Get(ctx, key, bundle); err != nil {
			return "", "", err
		}
		if deployedVersion == "" {
			deployedVersion = bundle.Status.ResolvedRevision
		}
		if deployedVersion == "" {
			deployedVersion = bundle.Spec.Version
		}
		artifactDigest = bundle.Status.ArtifactDigest
		return deployedVersion, artifactDigest, nil
	}
	if inline := flowDeployment.Spec.Source.Inline; inline != nil {
		switch {
		case inline.Registry != nil:
			if deployedVersion == "" {
				deployedVersion = inline.Registry.Version
			}
			if deployedVersion == "" {
				deployedVersion = inline.Registry.FlowID
			}
		case inline.Git != nil:
			if deployedVersion == "" {
				deployedVersion = inline.Git.Ref
			}
			artifactDigest = inline.Git.Path
		case inline.OCI != nil:
			if deployedVersion == "" {
				deployedVersion = inline.OCI.Digest
			}
			artifactDigest = inline.OCI.Digest
		}
	}
	return deployedVersion, artifactDigest, nil
}

func flowDeploymentStatusMatches(instance *nifiv1alpha1.NiFiFlowDeployment, processGroupID string, revisionVersion int64, deployedVersion string, artifactDigest string, syncState string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == processGroupID &&
		instance.Status.ProcessGroupID == processGroupID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.DeployedVersion == deployedVersion &&
		instance.Status.ArtifactDigest == artifactDigest &&
		instance.Status.SyncState == syncState
}

func shouldMarkFlowDeploymentNotReady(instance *nifiv1alpha1.NiFiFlowDeployment, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func processorDependenciesWaitingFor(ctx context.Context, c client.Client, processor *nifiv1alpha1.NiFiProcessor) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, processor.Namespace, processor.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func desiredProcessor(processor *nifiv1alpha1.NiFiProcessor, parentID string) nifi.ProcessorEntity {
	// State is intentionally omitted: a processor's run state is changed through the run-status
	// endpoint, not a component write (NiFi rejects a run-state change made via a component PUT).
	component := nifi.ProcessorComponent{
		ParentGroupID: parentID,
		Name:          processorDisplayName(processor),
		Type:          processor.Spec.Type,
		Config: nifi.ProcessorConfig{
			Properties:                       processor.Spec.Properties,
			SchedulingStrategy:               processor.Spec.Scheduling.Strategy,
			SchedulingPeriod:                 processor.Spec.Scheduling.Period,
			ConcurrentlySchedulableTaskCount: processor.Spec.Scheduling.ConcurrentlySchedulableTaskCount,
			AutoTerminatedRelationships:      processor.Spec.AutoTerminatedRelationships,
		},
	}
	if processor.Spec.Bundle != nil {
		component.Bundle = &nifi.Bundle{
			Group:    processor.Spec.Bundle.Group,
			Artifact: processor.Spec.Bundle.Artifact,
			Version:  processor.Spec.Bundle.Version,
		}
	}
	if processor.Spec.Position != nil {
		component.Position = &nifi.Position{X: float64(processor.Spec.Position.X), Y: float64(processor.Spec.Position.Y)}
	}
	return nifi.ProcessorEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}
}

func processorDisplayName(processor *nifiv1alpha1.NiFiProcessor) string {
	if processor.Spec.DisplayName != "" {
		return processor.Spec.DisplayName
	}
	return processor.Name
}

func processorParentID(ctx context.Context, c client.Client, namespace string, cluster *nifiv1alpha1.NiFiCluster, ref nifiv1alpha1.ProcessGroupReference) (string, error) {
	return processGroupParentID(ctx, c, namespace, cluster, ref)
}

func processorEntityID(entity nifi.ProcessorEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func processorNeedsUpdate(desired nifi.ProcessorEntity, existing nifi.ProcessorEntity) bool {
	// Run state is reconciled separately via the run-status endpoint, so it is excluded here.
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type ||
		desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if !nifiPositionsEqual(desired.Component.Position, existing.Component.Position) {
		return true
	}
	if !nifiBundlesEqual(desired.Component.Bundle, existing.Component.Bundle) {
		return true
	}
	return processorConfigNeedsUpdate(desired.Component.Config, existing.Component.Config)
}

func processorConfigNeedsUpdate(desired nifi.ProcessorConfig, existing nifi.ProcessorConfig) bool {
	if desired.SchedulingStrategy != existing.SchedulingStrategy ||
		desired.SchedulingPeriod != existing.SchedulingPeriod ||
		desired.ConcurrentlySchedulableTaskCount != existing.ConcurrentlySchedulableTaskCount {
		return true
	}
	if !stringMapsEqual(desired.Properties, existing.Properties) {
		return true
	}
	return !stringSlicesEqual(desired.AutoTerminatedRelationships, existing.AutoTerminatedRelationships)
}

func processorStatusMatches(instance *nifiv1alpha1.NiFiProcessor, nifiID string, revisionVersion int64, parentID string, validationStatus string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID &&
		instance.Status.ValidationStatus == validationStatus
}

func shouldMarkProcessorNotReady(instance *nifiv1alpha1.NiFiProcessor, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func inputPortDependenciesWaitingFor(ctx context.Context, c client.Client, inputPort *nifiv1alpha1.NiFiInputPort) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, inputPort.Namespace, inputPort.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func desiredInputPort(inputPort *nifiv1alpha1.NiFiInputPort, parentID string) nifi.PortEntity {
	return desiredPort(inputPortDisplayName(inputPort), inputPort.Spec.Position, inputPort.Spec.ConcurrentlySchedulableTaskCount, parentID)
}

func inputPortDisplayName(inputPort *nifiv1alpha1.NiFiInputPort) string {
	if inputPort.Spec.DisplayName != "" {
		return inputPort.Spec.DisplayName
	}
	return inputPort.Name
}

func inputPortStatusMatches(instance *nifiv1alpha1.NiFiInputPort, nifiID string, revisionVersion int64, parentID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID
}

func shouldMarkInputPortNotReady(instance *nifiv1alpha1.NiFiInputPort, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func outputPortDependenciesWaitingFor(ctx context.Context, c client.Client, outputPort *nifiv1alpha1.NiFiOutputPort) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, outputPort.Namespace, outputPort.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func desiredOutputPort(outputPort *nifiv1alpha1.NiFiOutputPort, parentID string) nifi.PortEntity {
	return desiredPort(outputPortDisplayName(outputPort), outputPort.Spec.Position, outputPort.Spec.ConcurrentlySchedulableTaskCount, parentID)
}

func outputPortDisplayName(outputPort *nifiv1alpha1.NiFiOutputPort) string {
	if outputPort.Spec.DisplayName != "" {
		return outputPort.Spec.DisplayName
	}
	return outputPort.Name
}

func outputPortStatusMatches(instance *nifiv1alpha1.NiFiOutputPort, nifiID string, revisionVersion int64, parentID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID
}

func shouldMarkOutputPortNotReady(instance *nifiv1alpha1.NiFiOutputPort, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

// nifiScheduledState maps the CRD's RuntimeState to NiFi's ScheduledState enum, which is
// upper-case (RUNNING/STOPPED/DISABLED, and ENABLED for controller services). Sending the CRD's
// title-case value verbatim makes NiFi reject the request ("No enum constant ScheduledState.Stopped").
func nifiScheduledState(state nifiv1alpha1.RuntimeState) string {
	switch state {
	case nifiv1alpha1.RuntimeStateRunning:
		return "RUNNING"
	case nifiv1alpha1.RuntimeStateStopped:
		return "STOPPED"
	case nifiv1alpha1.RuntimeStateEnabled:
		return "ENABLED"
	case nifiv1alpha1.RuntimeStateDisabled:
		return "DISABLED"
	default:
		return ""
	}
}

// desiredPort builds the component config for a port. It deliberately does NOT set State: a port's
// run state (RUNNING/STOPPED/DISABLED) is changed through the dedicated run-status endpoint, not a
// component write (NiFi rejects a run-state change made via a component PUT).
func desiredPort(name string, position *nifiv1alpha1.Position, concurrentlySchedulableTaskCount int32, parentID string) nifi.PortEntity {
	component := nifi.PortComponent{
		ParentGroupID:                    parentID,
		Name:                             name,
		ConcurrentlySchedulableTaskCount: concurrentlySchedulableTaskCount,
	}
	if position != nil {
		component.Position = &nifi.Position{X: float64(position.X), Y: float64(position.Y)}
	}
	return nifi.PortEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}
}

func portEntityID(entity nifi.PortEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

// portNeedsUpdate compares only the port's component config (run state is reconciled separately
// through the run-status endpoint, so it is intentionally excluded here).
func portNeedsUpdate(desired nifi.PortEntity, existing nifi.PortEntity) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID ||
		desired.Component.ConcurrentlySchedulableTaskCount != existing.Component.ConcurrentlySchedulableTaskCount {
		return true
	}
	return !nifiPositionsEqual(desired.Component.Position, existing.Component.Position)
}

// reconcilePortRunState brings a port to desiredState (RUNNING/STOPPED/DISABLED) through the
// run-status endpoint. It returns the refreshed entity, a non-empty pendingReason/pendingMsg when
// the port cannot reach the state yet (e.g. INVALID and needing a connection, or still VALIDATING),
// and an error when the run-status call itself fails. runStatus wraps the input/output client call.
func reconcilePortRunState(nifiID string, current *nifi.PortEntity, desiredState string, runStatus func(id string, revisionVersion int64, state string) (*nifi.PortEntity, error)) (*nifi.PortEntity, string, string, error) {
	if desiredState == "" || current.Component.State == desiredState {
		return current, "", "", nil
	}
	if desiredState == "RUNNING" {
		switch current.Component.ValidationStatus {
		case "INVALID":
			return current, "PortInvalid", "The port is INVALID and cannot be started; it needs at least one valid connection.", nil
		case "VALIDATING":
			return current, "PortValidating", "The port is still being validated by NiFi.", nil
		}
	}
	changed, err := runStatus(nifiID, current.Revision.Version, desiredState)
	if err != nil {
		return current, "", "", err
	}
	if changed != nil {
		current = changed
	}
	return current, "", "", nil
}

func connectionDependenciesWaitingFor(ctx context.Context, c client.Client, connection *nifiv1alpha1.NiFiConnection) []string {
	waitingFor := processGroupReferenceDependencyWaitingFor(ctx, c, connection.Namespace, connection.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
	waitingFor = append(waitingFor, connectableReferenceDependencyWaitingFor(ctx, c, connection.Namespace, connection.Spec.Source, "source")...)
	waitingFor = append(waitingFor, connectableReferenceDependencyWaitingFor(ctx, c, connection.Namespace, connection.Spec.Destination, "destination")...)
	return waitingFor
}

func desiredConnection(ctx context.Context, c client.Client, connection *nifiv1alpha1.NiFiConnection, parentID string) (nifi.ConnectionEntity, string, string, error) {
	source, err := nifiConnectable(ctx, c, connection.Namespace, connection.Spec.Source, parentID)
	if err != nil {
		return nifi.ConnectionEntity{}, "", "", err
	}
	destination, err := nifiConnectable(ctx, c, connection.Namespace, connection.Spec.Destination, parentID)
	if err != nil {
		return nifi.ConnectionEntity{}, "", "", err
	}
	component := nifi.ConnectionComponent{
		ParentGroupID:                 parentID,
		Source:                        source,
		Destination:                   destination,
		SelectedRelationships:         connection.Spec.SelectedRelationships,
		BackPressureObjectThreshold:   parseBackPressureObjectThreshold(connection.Spec.BackPressureObjectThreshold),
		BackPressureDataSizeThreshold: connection.Spec.BackPressureDataSizeThreshold,
		FlowFileExpiration:            connection.Spec.FlowFileExpiration,
		Prioritizers:                  connection.Spec.Prioritizers,
		LoadBalanceStrategy:           connection.Spec.LoadBalanceStrategy,
		LoadBalancePartitionAttribute: connection.Spec.LoadBalancePartitionAttribute,
	}
	if len(connection.Spec.Bends) > 0 {
		component.Bends = make([]nifi.Position, 0, len(connection.Spec.Bends))
		for _, bend := range connection.Spec.Bends {
			component.Bends = append(component.Bends, nifi.Position{X: float64(bend.X), Y: float64(bend.Y)})
		}
	}
	return nifi.ConnectionEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}, source.ID, destination.ID, nil
}

func nifiConnectable(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ConnectableReference, fallbackGroupID string) (nifi.Connectable, error) {
	connectable := nifi.Connectable{
		ID:      ref.NiFiID,
		Type:    nifiConnectableType(ref.Type),
		GroupID: fallbackGroupID,
	}
	if ref.Name == "" {
		return connectable, nil
	}
	refNamespace := connectableRefNamespace(namespace, ref)
	switch ref.Type {
	case nifiv1alpha1.ConnectableTypeProcessor:
		processor := &nifiv1alpha1.NiFiProcessor{}
		if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: refNamespace}, processor); err != nil {
			return nifi.Connectable{}, err
		}
		connectable.ID = processor.Status.NiFiID
		connectable.GroupID = processor.Status.ParentProcessGroupID
	case nifiv1alpha1.ConnectableTypeInputPort:
		inputPort := &nifiv1alpha1.NiFiInputPort{}
		if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: refNamespace}, inputPort); err != nil {
			return nifi.Connectable{}, err
		}
		connectable.ID = inputPort.Status.NiFiID
		connectable.GroupID = inputPort.Status.ParentProcessGroupID
	case nifiv1alpha1.ConnectableTypeOutputPort:
		outputPort := &nifiv1alpha1.NiFiOutputPort{}
		if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: refNamespace}, outputPort); err != nil {
			return nifi.Connectable{}, err
		}
		connectable.ID = outputPort.Status.NiFiID
		connectable.GroupID = outputPort.Status.ParentProcessGroupID
	case nifiv1alpha1.ConnectableTypeFunnel:
		funnel := &nifiv1alpha1.NiFiFunnel{}
		if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: refNamespace}, funnel); err != nil {
			return nifi.Connectable{}, err
		}
		connectable.ID = funnel.Status.NiFiID
		connectable.GroupID = funnel.Status.ParentProcessGroupID
	case nifiv1alpha1.ConnectableTypeRemoteInputPort, nifiv1alpha1.ConnectableTypeRemoteOutputPort:
		// A remote port belongs to a NiFiRemoteProcessGroup: its groupId is the RPG's NiFi id and
		// its id comes from the ports the RPG discovered from the target (matched by PortName).
		rpg := &nifiv1alpha1.NiFiRemoteProcessGroup{}
		if err := c.Get(ctx, types.NamespacedName{Name: ref.Name, Namespace: refNamespace}, rpg); err != nil {
			return nifi.Connectable{}, err
		}
		ports := rpg.Status.DiscoveredInputPorts
		if ref.Type == nifiv1alpha1.ConnectableTypeRemoteOutputPort {
			ports = rpg.Status.DiscoveredOutputPorts
		}
		for _, p := range ports {
			if p.Name == ref.PortName {
				connectable.ID = p.NiFiID
				break
			}
		}
		connectable.GroupID = rpg.Status.NiFiID
	}
	if connectable.GroupID == "" {
		connectable.GroupID = fallbackGroupID
	}
	return connectable, nil
}

func nifiConnectableType(connectableType nifiv1alpha1.ConnectableType) string {
	switch connectableType {
	case nifiv1alpha1.ConnectableTypeProcessor:
		return "PROCESSOR"
	case nifiv1alpha1.ConnectableTypeInputPort:
		return "INPUT_PORT"
	case nifiv1alpha1.ConnectableTypeOutputPort:
		return "OUTPUT_PORT"
	case nifiv1alpha1.ConnectableTypeFunnel:
		return "FUNNEL"
	case nifiv1alpha1.ConnectableTypeRemoteInputPort:
		return "REMOTE_INPUT_PORT"
	case nifiv1alpha1.ConnectableTypeRemoteOutputPort:
		return "REMOTE_OUTPUT_PORT"
	default:
		return ""
	}
}

// parseBackPressureObjectThreshold converts the connection's string spec value to the
// numeric count NiFi expects. An empty or unparseable value yields 0, which omitempty
// drops so NiFi applies its default threshold.
func parseBackPressureObjectThreshold(value string) int64 {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return parsed
}

func connectionEntityID(entity nifi.ConnectionEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func connectionNeedsUpdate(desired nifi.ConnectionEntity, existing nifi.ConnectionEntity) bool {
	if desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if !nifiConnectablesEqual(desired.Component.Source, existing.Component.Source) ||
		!nifiConnectablesEqual(desired.Component.Destination, existing.Component.Destination) {
		return true
	}
	if desired.Component.BackPressureObjectThreshold != existing.Component.BackPressureObjectThreshold ||
		desired.Component.BackPressureDataSizeThreshold != existing.Component.BackPressureDataSizeThreshold ||
		desired.Component.FlowFileExpiration != existing.Component.FlowFileExpiration ||
		desired.Component.LoadBalanceStrategy != existing.Component.LoadBalanceStrategy ||
		desired.Component.LoadBalancePartitionAttribute != existing.Component.LoadBalancePartitionAttribute {
		return true
	}
	if !stringSlicesEqual(desired.Component.SelectedRelationships, existing.Component.SelectedRelationships) ||
		!stringSlicesEqual(desired.Component.Prioritizers, existing.Component.Prioritizers) {
		return true
	}
	return !nifiPositionSlicesEqual(desired.Component.Bends, existing.Component.Bends)
}

func connectionStatusMatches(instance *nifiv1alpha1.NiFiConnection, nifiID string, revisionVersion int64, sourceID string, destinationID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.SourceID == sourceID &&
		instance.Status.DestinationID == destinationID
}

func shouldMarkConnectionNotReady(instance *nifiv1alpha1.NiFiConnection, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func funnelDependenciesWaitingFor(ctx context.Context, c client.Client, funnel *nifiv1alpha1.NiFiFunnel) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, funnel.Namespace, funnel.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func desiredFunnel(funnel *nifiv1alpha1.NiFiFunnel, parentID string) nifi.FunnelEntity {
	component := nifi.FunnelComponent{ParentGroupID: parentID}
	if funnel.Spec.Position != nil {
		component.Position = &nifi.Position{X: float64(funnel.Spec.Position.X), Y: float64(funnel.Spec.Position.Y)}
	}
	return nifi.FunnelEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}
}

func funnelEntityID(entity nifi.FunnelEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func funnelNeedsUpdate(desired nifi.FunnelEntity, existing nifi.FunnelEntity) bool {
	if desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	return !nifiPositionsEqual(desired.Component.Position, existing.Component.Position)
}

func funnelStatusMatches(instance *nifiv1alpha1.NiFiFunnel, nifiID string, revisionVersion int64, parentID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID
}

func shouldMarkFunnelNotReady(instance *nifiv1alpha1.NiFiFunnel, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func labelDependenciesWaitingFor(ctx context.Context, c client.Client, label *nifiv1alpha1.NiFiLabel) []string {
	return processGroupReferenceDependencyWaitingFor(ctx, c, label.Namespace, label.Spec.ParentProcessGroupRef, "parentProcessGroupRef")
}

func desiredLabel(label *nifiv1alpha1.NiFiLabel, parentID string) nifi.LabelEntity {
	component := nifi.LabelComponent{
		ParentGroupID: parentID,
		Label:         label.Spec.Text,
		Width:         float64(label.Spec.Width),
		Height:        float64(label.Spec.Height),
		Style:         label.Spec.Style,
	}
	if label.Spec.Position != nil {
		component.Position = &nifi.Position{X: float64(label.Spec.Position.X), Y: float64(label.Spec.Position.Y)}
	}
	return nifi.LabelEntity{
		Revision:  nifi.Revision{Version: 0},
		Component: component,
	}
}

func labelEntityID(entity nifi.LabelEntity) string {
	if entity.ID != "" {
		return entity.ID
	}
	return entity.Component.ID
}

func labelNeedsUpdate(desired nifi.LabelEntity, existing nifi.LabelEntity) bool {
	if desired.Component.ParentGroupID != "" && desired.Component.ParentGroupID != existing.Component.ParentGroupID {
		return true
	}
	if desired.Component.Label != existing.Component.Label ||
		desired.Component.Width != existing.Component.Width ||
		desired.Component.Height != existing.Component.Height {
		return true
	}
	if !nifiPositionsEqual(desired.Component.Position, existing.Component.Position) {
		return true
	}
	return !stringMapsEqual(desired.Component.Style, existing.Component.Style)
}

func labelStatusMatches(instance *nifiv1alpha1.NiFiLabel, nifiID string, revisionVersion int64, parentID string) bool {
	return instance.Status.ObservedGeneration == instance.Generation &&
		instance.Status.Ready &&
		instance.Status.Dependencies.Ready &&
		instance.Status.NiFiID == nifiID &&
		instance.Status.Revision.Version == revisionVersion &&
		instance.Status.ParentProcessGroupID == parentID
}

func shouldMarkLabelNotReady(instance *nifiv1alpha1.NiFiLabel, reason, message string) bool {
	if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Ready || instance.Status.Sync.LastError != message {
		return true
	}
	for _, condition := range instance.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func flowBundleSourceDependenciesWaitingFor(ctx context.Context, c client.Client, namespace string, source *nifiv1alpha1.FlowBundleSource, fieldPath string) []string {
	if source == nil || source.Registry == nil {
		return nil
	}
	return registryClientDependencyWaitingFor(ctx, c, namespace, source.Registry.RegistryClientRef, fieldPath+".registry.registryClientRef")
}

func parameterContextDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref *nifiv1alpha1.LocalObjectReference, fieldPath string) []string {
	if ref == nil {
		return nil
	}
	if ref.Name == "" {
		return []string{fieldPath + ".name"}
	}
	refNamespace := localObjectRefNamespace(namespace, *ref)
	parameterContext := &nifiv1alpha1.NiFiParameterContext{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, parameterContext); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiParameterContext/%s/%s", refNamespace, ref.Name)}
		}
		return []string{fmt.Sprintf("NiFiParameterContext/%s/%s:GetError", refNamespace, ref.Name)}
	}
	if !parameterContext.Status.Ready {
		return []string{fmt.Sprintf("NiFiParameterContext/%s/%s:Ready", refNamespace, ref.Name)}
	}
	return nil
}

func flowBundleDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref *nifiv1alpha1.LocalObjectReference, fieldPath string) []string {
	if ref == nil {
		return nil
	}
	if ref.Name == "" {
		return []string{fieldPath + ".name"}
	}
	refNamespace := localObjectRefNamespace(namespace, *ref)
	flowBundle := &nifiv1alpha1.NiFiFlowBundle{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, flowBundle); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiFlowBundle/%s/%s", refNamespace, ref.Name)}
		}
		return []string{fmt.Sprintf("NiFiFlowBundle/%s/%s:GetError", refNamespace, ref.Name)}
	}
	if !flowBundle.Status.Ready {
		return []string{fmt.Sprintf("NiFiFlowBundle/%s/%s:Ready", refNamespace, ref.Name)}
	}
	return nil
}

func registryClientDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.LocalObjectReference, fieldPath string) []string {
	if ref.Name == "" {
		return []string{fieldPath + ".name"}
	}
	refNamespace := localObjectRefNamespace(namespace, ref)
	registryClient := &nifiv1alpha1.NiFiRegistryClient{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, registryClient); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiRegistryClient/%s/%s", refNamespace, ref.Name)}
		}
		return []string{fmt.Sprintf("NiFiRegistryClient/%s/%s:GetError", refNamespace, ref.Name)}
	}
	if !registryClient.Status.Ready {
		return []string{fmt.Sprintf("NiFiRegistryClient/%s/%s:Ready", refNamespace, ref.Name)}
	}
	return nil
}

func processGroupReferenceDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ProcessGroupReference, _ string) []string {
	if ref.Root || ref.Name == "" {
		return nil
	}
	refNamespace := processGroupRefNamespace(namespace, ref)
	processGroup := &nifiv1alpha1.NiFiProcessGroup{}
	key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
	if err := c.Get(ctx, key, processGroup); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiProcessGroup/%s/%s", refNamespace, ref.Name)}
		}
		return []string{fmt.Sprintf("NiFiProcessGroup/%s/%s:GetError", refNamespace, ref.Name)}
	}
	if !processGroup.Status.Ready {
		return []string{fmt.Sprintf("NiFiProcessGroup/%s/%s:Ready", refNamespace, ref.Name)}
	}
	return nil
}

func connectableReferenceDependencyWaitingFor(ctx context.Context, c client.Client, namespace string, ref nifiv1alpha1.ConnectableReference, fieldPath string) []string {
	if ref.Name == "" {
		if ref.NiFiID != "" {
			return nil
		}
		return []string{fieldPath + ".name"}
	}
	refNamespace := connectableRefNamespace(namespace, ref)
	switch ref.Type {
	case nifiv1alpha1.ConnectableTypeProcessor:
		processor := &nifiv1alpha1.NiFiProcessor{}
		key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
		if err := c.Get(ctx, key, processor); err != nil {
			if apierrors.IsNotFound(err) {
				return []string{fmt.Sprintf("NiFiProcessor/%s/%s", refNamespace, ref.Name)}
			}
			return []string{fmt.Sprintf("NiFiProcessor/%s/%s:GetError", refNamespace, ref.Name)}
		}
		if !processor.Status.Ready {
			return []string{fmt.Sprintf("NiFiProcessor/%s/%s:Ready", refNamespace, ref.Name)}
		}
	case nifiv1alpha1.ConnectableTypeInputPort:
		inputPort := &nifiv1alpha1.NiFiInputPort{}
		key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
		if err := c.Get(ctx, key, inputPort); err != nil {
			if apierrors.IsNotFound(err) {
				return []string{fmt.Sprintf("NiFiInputPort/%s/%s", refNamespace, ref.Name)}
			}
			return []string{fmt.Sprintf("NiFiInputPort/%s/%s:GetError", refNamespace, ref.Name)}
		}
		if !inputPort.Status.Ready {
			return []string{fmt.Sprintf("NiFiInputPort/%s/%s:Ready", refNamespace, ref.Name)}
		}
	case nifiv1alpha1.ConnectableTypeOutputPort:
		outputPort := &nifiv1alpha1.NiFiOutputPort{}
		key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
		if err := c.Get(ctx, key, outputPort); err != nil {
			if apierrors.IsNotFound(err) {
				return []string{fmt.Sprintf("NiFiOutputPort/%s/%s", refNamespace, ref.Name)}
			}
			return []string{fmt.Sprintf("NiFiOutputPort/%s/%s:GetError", refNamespace, ref.Name)}
		}
		if !outputPort.Status.Ready {
			return []string{fmt.Sprintf("NiFiOutputPort/%s/%s:Ready", refNamespace, ref.Name)}
		}
	case nifiv1alpha1.ConnectableTypeFunnel:
		funnel := &nifiv1alpha1.NiFiFunnel{}
		key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
		if err := c.Get(ctx, key, funnel); err != nil {
			if apierrors.IsNotFound(err) {
				return []string{fmt.Sprintf("NiFiFunnel/%s/%s", refNamespace, ref.Name)}
			}
			return []string{fmt.Sprintf("NiFiFunnel/%s/%s:GetError", refNamespace, ref.Name)}
		}
		if !funnel.Status.Ready {
			return []string{fmt.Sprintf("NiFiFunnel/%s/%s:Ready", refNamespace, ref.Name)}
		}
	case nifiv1alpha1.ConnectableTypeRemoteInputPort, nifiv1alpha1.ConnectableTypeRemoteOutputPort:
		// Wait only until the RPG has discovered the named port (its id is published), NOT until the
		// RPG is Ready: an RPG stays NotReady until its port is connected, but the port cannot be
		// connected until this connection is created — requiring Ready would deadlock the two.
		rpg := &nifiv1alpha1.NiFiRemoteProcessGroup{}
		key := types.NamespacedName{Name: ref.Name, Namespace: refNamespace}
		if err := c.Get(ctx, key, rpg); err != nil {
			if apierrors.IsNotFound(err) {
				return []string{fmt.Sprintf("NiFiRemoteProcessGroup/%s/%s", refNamespace, ref.Name)}
			}
			return []string{fmt.Sprintf("NiFiRemoteProcessGroup/%s/%s:GetError", refNamespace, ref.Name)}
		}
		ports := rpg.Status.DiscoveredInputPorts
		if ref.Type == nifiv1alpha1.ConnectableTypeRemoteOutputPort {
			ports = rpg.Status.DiscoveredOutputPorts
		}
		discovered := false
		for _, p := range ports {
			if p.Name == ref.PortName && p.NiFiID != "" {
				discovered = true
				break
			}
		}
		if !discovered {
			return []string{fmt.Sprintf("NiFiRemoteProcessGroup/%s/%s:port/%s", refNamespace, ref.Name, ref.PortName)}
		}
	default:
		return []string{fieldPath + ".type"}
	}
	return nil
}

func localObjectReferenceMatches(defaultNamespace string, ref nifiv1alpha1.LocalObjectReference, obj client.Object) bool {
	if ref.Name == "" {
		return false
	}
	return ref.Name == obj.GetName() && localObjectRefNamespace(defaultNamespace, ref) == obj.GetNamespace()
}

func processGroupReferenceMatches(defaultNamespace string, ref nifiv1alpha1.ProcessGroupReference, obj client.Object) bool {
	if ref.Root || ref.Name == "" {
		return false
	}
	return ref.Name == obj.GetName() && processGroupRefNamespace(defaultNamespace, ref) == obj.GetNamespace()
}

func connectableReferenceMatches(defaultNamespace string, ref nifiv1alpha1.ConnectableReference, connectableType nifiv1alpha1.ConnectableType, obj client.Object) bool {
	if ref.Type != connectableType || ref.Name == "" {
		return false
	}
	return ref.Name == obj.GetName() && connectableRefNamespace(defaultNamespace, ref) == obj.GetNamespace()
}

func localObjectRefNamespace(defaultNamespace string, ref nifiv1alpha1.LocalObjectReference) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return defaultNamespace
}

func processGroupRefNamespace(defaultNamespace string, ref nifiv1alpha1.ProcessGroupReference) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return defaultNamespace
}

func connectableRefNamespace(defaultNamespace string, ref nifiv1alpha1.ConnectableReference) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return defaultNamespace
}
