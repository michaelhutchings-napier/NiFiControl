package controller

import (
	"context"
	"fmt"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
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
	Scheme *runtime.Scheme
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
	Scheme *runtime.Scheme
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
			return ctrl.Result{}, markRegistryClientWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markRegistryClientAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
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
	Scheme *runtime.Scheme
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
			return ctrl.Result{}, markParameterContextWaitingForDependencies(ctx, r.Client, instance, waitingFor)
		}
		return ctrl.Result{}, nil
	}
	if instance.Status.ObservedGeneration != instance.Generation || !instance.Status.Dependencies.Ready {
		return ctrl.Result{}, markParameterContextAccepted(ctx, r.Client, instance)
	}
	return ctrl.Result{}, nil
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
