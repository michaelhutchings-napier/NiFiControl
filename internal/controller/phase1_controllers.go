package controller

import (
	"context"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiRegistryClient{}).Complete(r)
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
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiParameterContext{}).Complete(r)
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
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiControllerService{}).Complete(r)
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
	return ctrl.NewControllerManagedBy(mgr).For(&nifiv1alpha1.NiFiFlowDeployment{}).Complete(r)
}
