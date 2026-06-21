package controller

import (
	"context"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificlusters/finalizers,verbs=update
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
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nificontrollerservices/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowbundles/finalizers,verbs=update
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nififlowdeployments/finalizers,verbs=update

type NiFiClusterReconciler struct {
	client.Client
	Scheme              *runtime.Scheme
	ReachabilityChecker nifi.ReachabilityChecker
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	if instance.Spec.API != nil && instance.Spec.API.URI != "" {
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
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiCluster{}).Complete(r)
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

	entity, resolvedType, supported := desiredRegistryClient(instance)
	if !supported {
		message := fmt.Sprintf("Registry client type %q is not implemented yet.", instance.Spec.Type)
		if shouldMarkRegistryClientNotReady(instance, "RegistryClientTypeUnsupported", message) {
			return ctrl.Result{}, markRegistryClientNotReady(ctx, r.Client, instance, "RegistryClientTypeUnsupported", message)
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
		return r.reconcileExistingRegistryClient(ctx, instance, endpoint, registryClient, entity, existing, resolvedType)
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
		return r.reconcileExistingRegistryClient(ctx, instance, endpoint, registryClient, entity, existing, resolvedType)
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
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
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

func (r *NiFiRegistryClientReconciler) reconcileExistingRegistryClient(ctx context.Context, instance *nifiv1alpha1.NiFiRegistryClient, endpoint string, registryClient nifi.RegistryClientClient, desired nifi.RegistryClientEntity, existing *nifi.RegistryClientEntity, resolvedType string) (ctrl.Result, error) {
	if existing == nil {
		message := "NiFi returned an empty registry client response."
		if shouldMarkRegistryClientNotReady(instance, "NiFiGetFailed", message) {
			return ctrl.Result{RequeueAfter: 30 * time.Second}, markRegistryClientNotReady(ctx, r.Client, instance, "NiFiGetFailed", message)
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	nifiID := registryClientEntityID(*existing)
	if registryClientNeedsUpdate(desired, *existing) {
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

func desiredRegistryClient(instance *nifiv1alpha1.NiFiRegistryClient) (nifi.RegistryClientEntity, string, bool) {
	resolvedType := registryClientType(instance.Spec.Type)
	if instance.Spec.Type != "" && instance.Spec.Type != nifiv1alpha1.RegistryClientTypeNiFiRegistry {
		return nifi.RegistryClientEntity{}, resolvedType, false
	}
	return nifi.RegistryClientEntity{
		Revision: nifi.Revision{Version: 0},
		Component: nifi.RegistryClientComponent{
			Name:        instance.Name,
			Type:        resolvedType,
			Description: instance.Spec.Description,
			Properties: map[string]string{
				"url": instance.Spec.URI,
			},
		},
	}, resolvedType, true
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

func registryClientNeedsUpdate(desired nifi.RegistryClientEntity, existing nifi.RegistryClientEntity) bool {
	if desired.Component.Name != existing.Component.Name ||
		desired.Component.Type != existing.Component.Type ||
		desired.Component.Description != existing.Component.Description {
		return true
	}
	return desired.Component.Properties["url"] != existing.Component.Properties["url"]
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
	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
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
	if err := pcClient.DeleteParameterContext(ctx, endpoint, instance.Status.NiFiID, instance.Status.Revision.Version); err != nil {
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
	Scheme *runtime.Scheme
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor, err := clusterDependencyWaitingFor(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markUserWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markUserAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
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
	Scheme *runtime.Scheme
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor, err := clusterDependencyWaitingFor(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
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
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markUserGroupAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiUserGroupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiUserGroup{}, clusterRefIndexField, indexUserGroupClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiUserGroup{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiProcessGroupReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor, err := clusterDependencyWaitingFor(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
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
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markProcessGroupAccepted(ctx, r.Client, instance)
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
		Complete(r)
}

type NiFiControllerServiceReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor, err := clusterDependencyWaitingFor(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markControllerServiceWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markControllerServiceAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiControllerServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &nifiv1alpha1.NiFiControllerService{}, clusterRefIndexField, indexControllerServiceClusterRef); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiControllerService{}).
		Watches(&nifiv1alpha1.NiFiCluster{}, handler.EnqueueRequestsFromMapFunc(r.requestsForCluster)).
		Complete(r)
}

type NiFiFlowBundleReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
	if instance.Status.ObservedGeneration != instance.Generation {
		return ctrl.Result{}, markFlowBundleAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiFlowBundleReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiFlowBundle{}).Complete(r)
}

type NiFiFlowDeploymentReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
		_, err := removeFinalizer(ctx, r.Client, instance)
		return ctrl.Result{}, err
	}
	if updated, err := ensureFinalizer(ctx, r.Client, instance); err != nil || updated {
		return ctrl.Result{}, err
	}
	waitingFor, err := clusterDependencyWaitingFor(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		if instance.Status.ObservedGeneration != instance.Generation || instance.Status.Dependencies.Ready || waitingForChanged(instance.Status.Dependencies.WaitingFor, waitingFor) {
			return ctrl.Result{}, markFlowDeploymentWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markFlowDeploymentAccepted(ctx, r.Client, instance)
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
		Complete(r)
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

func processGroupDependenciesWaitingFor(ctx context.Context, c client.Client, processGroup *nifiv1alpha1.NiFiProcessGroup) []string {
	if processGroup.Spec.ParameterContextRef == nil {
		return nil
	}
	ref := *processGroup.Spec.ParameterContextRef
	if ref.Name == "" {
		return []string{"parameterContextRef.name"}
	}
	namespace := processGroup.Namespace
	if ref.Namespace != "" {
		namespace = ref.Namespace
	}
	parameterContext := &nifiv1alpha1.NiFiParameterContext{}
	key := types.NamespacedName{Name: ref.Name, Namespace: namespace}
	if err := c.Get(ctx, key, parameterContext); err != nil {
		if apierrors.IsNotFound(err) {
			return []string{fmt.Sprintf("NiFiParameterContext/%s/%s", namespace, ref.Name)}
		}
		return []string{fmt.Sprintf("NiFiParameterContext/%s/%s:GetError", namespace, ref.Name)}
	}
	if !parameterContext.Status.Ready {
		return []string{fmt.Sprintf("NiFiParameterContext/%s/%s:Ready", namespace, ref.Name)}
	}
	return nil
}
