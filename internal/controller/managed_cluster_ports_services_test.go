package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func containerPortByName(ports []corev1.ContainerPort, name string) (int32, bool) {
	for _, port := range ports {
		if port.Name == name {
			return port.ContainerPort, true
		}
	}
	return 0, false
}

func servicePortByName(ports []corev1.ServicePort, name string) (corev1.ServicePort, bool) {
	for _, port := range ports {
		if port.Name == name {
			return port, true
		}
	}
	return corev1.ServicePort{}, false
}

func TestManagedClusterPortsCustomization(t *testing.T) {
	cluster := hardeningCluster()
	cluster.Spec.Replicas = 3
	cluster.Spec.Coordination = &nifiv1alpha1.NiFiClusterCoordinationSpec{ZooKeeperConnectString: "zk.default.svc:2181"}
	cluster.Spec.Ports = &nifiv1alpha1.NiFiClusterPortsSpec{
		HTTP:            8090,
		ClusterProtocol: 12443,
		RemoteInput:     10001,
		LoadBalance:     6343,
	}

	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	ports := spec.Template.Spec.Containers[0].Ports
	for _, tc := range []struct {
		name string
		want int32
	}{
		{"web", 8090},
		{"cluster", 12443},
		{"s2s", 10001},
		{"load-balance", 6343},
	} {
		got, ok := containerPortByName(ports, tc.name)
		if !ok || got != tc.want {
			t.Fatalf("container port %q = %d (found=%v), want %d", tc.name, got, ok, tc.want)
		}
	}

	env := managedClusterEnvironment(cluster, nil, nil)
	assertEnvironmentValue(t, env, "NIFI_WEB_HTTP_PORT", "8090")
	assertEnvironmentValue(t, env, "NIFI_CLUSTER_NODE_PROTOCOL_PORT", "12443")
	assertEnvironmentValue(t, env, "NIFI_REMOTE_INPUT_SOCKET_PORT", "10001")
	assertEnvironmentValue(t, env, "NIFI_CLUSTER_LOAD_BALANCE_PORT", "6343")

	for _, command := range []string{managedNiFiStartCommand, managedNiFiStartCommandTLS} {
		if !strings.Contains(command, "nifi.cluster.load.balance.port") {
			t.Fatal("start command must configure nifi.cluster.load.balance.port")
		}
	}
}

func TestManagedClusterPortsDefaults(t *testing.T) {
	cluster := hardeningCluster()
	env := managedClusterEnvironment(cluster, nil, nil)
	assertEnvironmentValue(t, env, "NIFI_WEB_HTTP_PORT", "8080")
	assertEnvironmentValue(t, env, "NIFI_REMOTE_INPUT_SOCKET_PORT", "10000")
	assertEnvironmentValue(t, env, "NIFI_CLUSTER_LOAD_BALANCE_PORT", "6342")
	if got := managedClusterClusterProtocolPort(cluster); got != 11443 {
		t.Fatalf("default cluster protocol port = %d, want 11443", got)
	}
}

func TestManagedClusterHeadlessServiceCustomPorts(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	cluster.Spec.Ports = &nifiv1alpha1.NiFiClusterPortsSpec{ClusterProtocol: 12443, LoadBalance: 6343}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterService(context.Background(), cluster, true); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{}
	key := types.NamespacedName{Name: managedClusterHeadlessServiceName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, service); err != nil {
		t.Fatal(err)
	}
	if port, ok := servicePortByName(service.Spec.Ports, "cluster"); !ok || port.Port != 12443 {
		t.Fatalf("headless cluster port = %#v, want 12443", port)
	}
	if port, ok := servicePortByName(service.Spec.Ports, "load-balance"); !ok || port.Port != 6343 {
		t.Fatalf("headless load-balance port = %#v, want 6343", port)
	}
}

func TestManagedClusterAdditiveProxyHosts(t *testing.T) {
	cluster := hardeningCluster()
	cluster.Spec.Ingress = &nifiv1alpha1.NiFiClusterIngressSpec{Enabled: true, Host: "nifi.example.com"}
	cluster.Spec.AdditionalProxyHosts = []nifiv1alpha1.ProxyHost{"lb.example.com", "lb.example.com:8443", "  "}

	got := managedClusterProxyHost(cluster, nil)
	for _, want := range []string{"nifi.example.com", "lb.example.com", "lb.example.com:8443"} {
		if !strings.Contains(got, want) {
			t.Fatalf("proxy host allow-list %q missing %q", got, want)
		}
	}
	// The blank entry must not produce an empty allow-list token.
	for _, token := range strings.Split(got, ",") {
		if strings.TrimSpace(token) == "" {
			t.Fatalf("proxy host allow-list %q contains an empty entry", got)
		}
	}
	env := managedClusterEnvironment(cluster, nil, nil)
	assertEnvironmentValue(t, env, "NIFI_WEB_PROXY_HOST", got)
}

func TestManagedClusterExternalServicesReconcileAndPrune(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	cluster.Spec.ExternalServices = []nifiv1alpha1.NiFiClusterExternalService{{
		Name:        "nifi-lb",
		Type:        corev1.ServiceTypeLoadBalancer,
		Annotations: map[string]string{"external-dns.alpha.kubernetes.io/hostname": "nifi.example.com"},
		Ports: []nifiv1alpha1.NiFiClusterExternalServicePort{
			{Name: "https", Port: 8443, TargetPort: "web"},
			{Name: "s2s", Port: 10001, TargetPort: "s2s"},
		},
		LoadBalancerSourceRanges: []string{"10.0.0.0/8"},
		ExternalTrafficPolicy:    corev1.ServiceExternalTrafficPolicyTypeLocal,
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterExternalServices(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	service := &corev1.Service{}
	key := types.NamespacedName{Name: "nifi-lb", Namespace: cluster.Namespace}
	if err := k8sClient.Get(context.Background(), key, service); err != nil {
		t.Fatalf("external Service not created: %v", err)
	}
	if service.Spec.Type != corev1.ServiceTypeLoadBalancer {
		t.Fatalf("type = %q, want LoadBalancer", service.Spec.Type)
	}
	if service.Spec.Selector["app.kubernetes.io/component"] != "nifi-node" {
		t.Fatalf("selector = %#v, want the node-pod selector", service.Spec.Selector)
	}
	if service.Labels[managedExternalServiceLabel] != managedClusterResourceName(cluster) {
		t.Fatalf("external-service label = %q", service.Labels[managedExternalServiceLabel])
	}
	if service.Annotations[managedClusterAnnotation] != cluster.Name {
		t.Fatalf("missing owning-cluster annotation: %#v", service.Annotations)
	}
	if port, ok := servicePortByName(service.Spec.Ports, "https"); !ok || port.TargetPort.StrVal != "web" || port.Port != 8443 {
		t.Fatalf("https port = %#v", port)
	}
	if service.Spec.ExternalTrafficPolicy != corev1.ServiceExternalTrafficPolicyTypeLocal {
		t.Fatalf("externalTrafficPolicy = %q, want Local", service.Spec.ExternalTrafficPolicy)
	}

	// Dropping the Service from the spec prunes it.
	cluster.Spec.ExternalServices = nil
	if err := r.reconcileManagedClusterExternalServices(context.Background(), cluster); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(context.Background(), key, service); !apierrors.IsNotFound(err) {
		t.Fatalf("external Service should be pruned when removed from spec, err=%v", err)
	}
}

func TestExternalServicePortsPreserveAllocatedNodePort(t *testing.T) {
	spec := &nifiv1alpha1.NiFiClusterExternalService{
		Type:  corev1.ServiceTypeNodePort,
		Ports: []nifiv1alpha1.NiFiClusterExternalServicePort{{Name: "https", Port: 8443, TargetPort: "web"}},
	}
	// A prior reconcile left an API-allocated nodePort; an unset spec nodePort must keep it.
	existing := []corev1.ServicePort{{Name: "https", Port: 8443, NodePort: 31234}}
	ports := externalServicePorts(spec, existing)
	if len(ports) != 1 || ports[0].NodePort != 31234 {
		t.Fatalf("allocated nodePort not preserved: %#v", ports)
	}
	// An explicit spec nodePort wins over the allocated one.
	spec.Ports[0].NodePort = 32000
	if got := externalServicePorts(spec, existing); got[0].NodePort != 32000 {
		t.Fatalf("explicit nodePort not honored: %#v", got)
	}
	// A ClusterIP Service never carries a nodePort.
	spec.Type = corev1.ServiceTypeClusterIP
	if got := externalServicePorts(spec, existing); got[0].NodePort != 0 {
		t.Fatalf("ClusterIP Service must not set a nodePort: %#v", got)
	}
}

func TestExternalServiceRefusesForeignService(t *testing.T) {
	scheme := managedClusterTestScheme()
	cluster := hardeningCluster()
	foreign := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "nifi-lb", Namespace: cluster.Namespace},
		Spec:       corev1.ServiceSpec{Ports: []corev1.ServicePort{{Name: "http", Port: 80}}},
	}
	cluster.Spec.ExternalServices = []nifiv1alpha1.NiFiClusterExternalService{{
		Name:  "nifi-lb",
		Ports: []nifiv1alpha1.NiFiClusterExternalServicePort{{Name: "https", Port: 8443, TargetPort: "web"}},
	}}
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}).Build()
	r := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}

	if err := r.reconcileManagedClusterExternalServices(context.Background(), cluster); err == nil {
		t.Fatal("expected reconcile to refuse adopting a foreign Service")
	}
}
