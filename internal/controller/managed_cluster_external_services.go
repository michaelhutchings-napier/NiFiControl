package controller

import (
	"context"
	"fmt"
	"strconv"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// reconcileManagedClusterExternalServices creates or updates the additional Services
// declared in spec.externalServices and prunes any the operator previously created that
// are no longer declared. Each Service selects the managed node pods and is tracked by the
// cluster annotation and an external-service label (not an owner reference), so — like the
// operator's own Services — it survives an Orphan deletion and is removed only under the
// Delete policy or when dropped from the spec.
func (r *NiFiClusterReconciler) reconcileManagedClusterExternalServices(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	desired := make(map[string]struct{}, len(cluster.Spec.ExternalServices))
	for i := range cluster.Spec.ExternalServices {
		spec := &cluster.Spec.ExternalServices[i]
		desired[spec.Name] = struct{}{}
		if err := r.reconcileManagedClusterExternalService(ctx, cluster, spec); err != nil {
			return err
		}
	}
	return r.pruneManagedClusterExternalServices(ctx, cluster, desired)
}

func (r *NiFiClusterReconciler) reconcileManagedClusterExternalService(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, spec *nifiv1alpha1.NiFiClusterExternalService) error {
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: spec.Name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := assertManagedExternalService(service, cluster); err != nil {
			return err
		}
		labels := managedClusterLabels(cluster)
		for key, value := range spec.Labels {
			if key != managedClusterLabel && key != managedExternalServiceLabel {
				labels[key] = value
			}
		}
		labels[managedExternalServiceLabel] = managedClusterResourceName(cluster)
		service.Labels = labels
		service.Annotations = mergeExternalServiceAnnotations(cluster, spec.Annotations)
		service.Spec.Selector = managedClusterPodLabels(cluster)
		service.Spec.Type = externalServiceType(spec.Type)
		service.Spec.Ports = externalServicePorts(spec, service.Spec.Ports)
		service.Spec.LoadBalancerIP = spec.LoadBalancerIP
		service.Spec.LoadBalancerSourceRanges = spec.LoadBalancerSourceRanges
		service.Spec.ExternalTrafficPolicy = ""
		if spec.ExternalTrafficPolicy != "" && service.Spec.Type != corev1.ServiceTypeClusterIP {
			service.Spec.ExternalTrafficPolicy = spec.ExternalTrafficPolicy
		}
		applyServiceNetworking(service, spec.IPFamilies, spec.IPFamilyPolicy, spec.SessionAffinity, spec.SessionAffinityConfig, true)
		return nil
	})
	return err
}

// pruneManagedClusterExternalServices deletes operator-created external Services that are
// no longer present in the spec.
func (r *NiFiClusterReconciler) pruneManagedClusterExternalServices(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, desired map[string]struct{}) error {
	existing := &corev1.ServiceList{}
	if err := r.List(ctx, existing, client.InNamespace(cluster.Namespace), client.MatchingLabels{managedExternalServiceLabel: managedClusterResourceName(cluster)}); err != nil {
		return err
	}
	for i := range existing.Items {
		service := &existing.Items[i]
		if _, keep := desired[service.Name]; keep {
			continue
		}
		if err := r.deleteManagedClusterResource(ctx, cluster, service); err != nil {
			return err
		}
	}
	return nil
}

func externalServiceType(serviceType corev1.ServiceType) corev1.ServiceType {
	if serviceType != "" {
		return serviceType
	}
	return corev1.ServiceTypeClusterIP
}

func externalServicePorts(spec *nifiv1alpha1.NiFiClusterExternalService, existing []corev1.ServicePort) []corev1.ServicePort {
	// Preserve node ports Kubernetes auto-allocated for a NodePort/LoadBalancer Service so
	// re-reconciling with an unset (0) nodePort does not force a fresh allocation each pass.
	allocated := make(map[string]int32, len(existing))
	for _, port := range existing {
		if port.NodePort != 0 {
			allocated[port.Name] = port.NodePort
		}
	}
	ports := make([]corev1.ServicePort, 0, len(spec.Ports))
	for _, port := range spec.Ports {
		protocol := port.Protocol
		if protocol == "" {
			protocol = corev1.ProtocolTCP
		}
		servicePort := corev1.ServicePort{
			Name:       port.Name,
			Port:       port.Port,
			Protocol:   protocol,
			TargetPort: externalServiceTargetPort(port),
		}
		if servicePort.Protocol != corev1.ProtocolUDP && spec.Type != corev1.ServiceTypeClusterIP {
			if port.NodePort != 0 {
				servicePort.NodePort = port.NodePort
			} else {
				servicePort.NodePort = allocated[port.Name]
			}
		}
		ports = append(ports, servicePort)
	}
	return ports
}

// externalServiceTargetPort resolves a port's target: a named container port when a name
// is given, a number when numeric, and the service port itself when unset.
func externalServiceTargetPort(port nifiv1alpha1.NiFiClusterExternalServicePort) intstr.IntOrString {
	if port.TargetPort == "" {
		return intstr.FromInt32(port.Port)
	}
	if numeric, err := strconv.Atoi(port.TargetPort); err == nil {
		return intstr.FromInt32(int32(numeric))
	}
	return intstr.FromString(port.TargetPort)
}

func mergeExternalServiceAnnotations(cluster *nifiv1alpha1.NiFiCluster, extra map[string]string) map[string]string {
	annotations := make(map[string]string, len(extra)+1)
	for key, value := range extra {
		if key == managedClusterAnnotation {
			continue
		}
		annotations[key] = value
	}
	annotations[managedClusterAnnotation] = cluster.Name
	return annotations
}

// assertManagedExternalService guards against adopting a pre-existing Service that the
// operator did not create for this cluster.
func assertManagedExternalService(service *corev1.Service, cluster *nifiv1alpha1.NiFiCluster) error {
	if service.GetResourceVersion() == "" {
		return nil
	}
	if service.Labels[managedExternalServiceLabel] != managedClusterResourceName(cluster) {
		return fmt.Errorf("Service %s/%s already exists and is not an external Service managed by NiFiCluster %s", service.Namespace, service.Name, cluster.Name)
	}
	return nil
}
