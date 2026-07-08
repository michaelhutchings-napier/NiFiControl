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
	"k8s.io/utils/ptr"
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

func TestManagedClusterDomainInSANsAndProxyHosts(t *testing.T) {
	cluster := hardeningCluster()
	// Default: cluster.local.
	if got := managedClusterDomain(cluster); got != "cluster.local" {
		t.Fatalf("default cluster domain = %q, want cluster.local", got)
	}
	names := managedClusterServerDNSNames(cluster)
	proxy := managedClusterProxyHosts(cluster)
	for _, want := range []string{"production-nifi.default.svc", "production-nifi.default.svc.cluster.local"} {
		if !containsString(names, want) {
			t.Fatalf("default SANs %v missing %q", names, want)
		}
	}
	if !strings.Contains(proxy, "production-nifi.default.svc.cluster.local") {
		t.Fatalf("default proxy hosts %q missing cluster.local FQDN", proxy)
	}

	// Custom domain flows into the FQDN SANs and proxy hosts; the short .svc names remain.
	cluster.Spec.ClusterDomain = "cluster.internal"
	names = managedClusterServerDNSNames(cluster)
	proxy = managedClusterProxyHosts(cluster)
	for _, want := range []string{
		"production-nifi.default.svc",
		"production-nifi.default.svc.cluster.internal",
		"*.production-nifi-headless.default.svc.cluster.internal",
	} {
		if !containsString(names, want) {
			t.Fatalf("custom-domain SANs %v missing %q", names, want)
		}
	}
	if containsString(names, "production-nifi.default.svc.cluster.local") {
		t.Fatalf("custom-domain SANs must not carry the default cluster.local FQDN: %v", names)
	}
	if !strings.Contains(proxy, "production-nifi.default.svc.cluster.internal") {
		t.Fatalf("custom-domain proxy hosts %q missing the custom FQDN", proxy)
	}
	if strings.Contains(proxy, "cluster.local") {
		t.Fatalf("custom-domain proxy hosts %q must not carry cluster.local", proxy)
	}
}

func TestManagedClusterPodSecurityContext(t *testing.T) {
	// Default: the apache/nifi image's uid:gid (1000:1000) is encoded here — fsGroup for
	// volume writability plus a numeric runAsUser/runAsGroup so the pod is verifiably
	// non-root (the image's non-numeric USER "nifi" cannot be verified against
	// runAsNonRoot: true on its own).
	cluster := hardeningCluster()
	sc := managedClusterPodSecurityContext(cluster)
	if sc.FSGroup == nil || *sc.FSGroup != 1000 || sc.FSGroupChangePolicy == nil || *sc.FSGroupChangePolicy != corev1.FSGroupChangeOnRootMismatch {
		t.Fatalf("default security context = %#v", sc)
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 1000 || sc.RunAsGroup == nil || *sc.RunAsGroup != 1000 {
		t.Fatalf("default runAsUser/runAsGroup not 1000: %#v", sc)
	}

	// Restricted-PSA scenario: the user opts in with runAsNonRoot + seccompProfile only.
	// The operator must fill in the numeric runAsUser/runAsGroup so the kubelet can verify
	// non-root against the stock image and the pod actually starts (regression: without a
	// numeric runAsUser the container fails with CreateContainerConfigError).
	cluster.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{
		SecurityContext: &corev1.PodSecurityContext{
			RunAsNonRoot:   ptr.To(true),
			SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
		},
	}
	sc = managedClusterPodSecurityContext(cluster)
	if sc.RunAsNonRoot == nil || !*sc.RunAsNonRoot {
		t.Fatalf("runAsNonRoot not preserved: %#v", sc)
	}
	if sc.RunAsUser == nil || *sc.RunAsUser != 1000 || sc.RunAsGroup == nil || *sc.RunAsGroup != 1000 {
		t.Fatalf("numeric runAsUser/runAsGroup not defaulted for restricted PSA: %#v", sc)
	}
	if sc.FSGroup == nil || *sc.FSGroup != 1000 {
		t.Fatalf("fsGroup default not preserved when unset: %#v", sc)
	}

	// Custom runAsUser/runAsGroup win over the defaults; fsGroup default is still applied.
	cluster.Spec.Pod.SecurityContext = &corev1.PodSecurityContext{
		RunAsUser: ptr.To[int64](2000), RunAsGroup: ptr.To[int64](2000), RunAsNonRoot: ptr.To(true),
	}
	sc = managedClusterPodSecurityContext(cluster)
	if sc.RunAsUser == nil || *sc.RunAsUser != 2000 || sc.RunAsGroup == nil || *sc.RunAsGroup != 2000 {
		t.Fatalf("custom runAsUser/runAsGroup not honored: %#v", sc)
	}
	if sc.FSGroup == nil || *sc.FSGroup != 1000 {
		t.Fatalf("fsGroup default not preserved when unset: %#v", sc)
	}

	// Explicit fsGroup wins over the default.
	cluster.Spec.Pod.SecurityContext.FSGroup = ptr.To[int64](3000)
	if sc = managedClusterPodSecurityContext(cluster); sc.FSGroup == nil || *sc.FSGroup != 3000 {
		t.Fatalf("explicit fsGroup not honored: %#v", sc)
	}

	// The resolver returns a copy; it must not mutate the spec's fsGroup.
	cluster.Spec.Pod.SecurityContext.FSGroup = nil
	_ = managedClusterPodSecurityContext(cluster)
	if cluster.Spec.Pod.SecurityContext.FSGroup != nil {
		t.Fatal("resolver mutated the spec's securityContext")
	}
}

func TestManagedClusterContainerSecurityContext(t *testing.T) {
	cluster := hardeningCluster()
	// Default: no container security context on either operator container.
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if spec.Template.Spec.Containers[0].SecurityContext != nil {
		t.Fatalf("nifi container security context = %#v, want nil by default", spec.Template.Spec.Containers[0].SecurityContext)
	}
	if spec.Template.Spec.InitContainers[0].SecurityContext != nil {
		t.Fatalf("init container security context = %#v, want nil by default", spec.Template.Spec.InitContainers[0].SecurityContext)
	}

	// Set: the restricted-PSA baseline lands on the NiFi container and the init container.
	cluster.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{
		ContainerSecurityContext: &corev1.SecurityContext{
			AllowPrivilegeEscalation: ptr.To(false),
			Capabilities:             &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
		},
	}
	spec = desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	for _, c := range []corev1.Container{spec.Template.Spec.Containers[0], spec.Template.Spec.InitContainers[0]} {
		if c.SecurityContext == nil || c.SecurityContext.AllowPrivilegeEscalation == nil || *c.SecurityContext.AllowPrivilegeEscalation {
			t.Fatalf("%s: allowPrivilegeEscalation not false: %#v", c.Name, c.SecurityContext)
		}
		if c.SecurityContext.Capabilities == nil || len(c.SecurityContext.Capabilities.Drop) != 1 || c.SecurityContext.Capabilities.Drop[0] != "ALL" {
			t.Fatalf("%s: capabilities drop ALL missing: %#v", c.Name, c.SecurityContext)
		}
	}
}

func TestManagedClusterTerminationGracePeriodSeconds(t *testing.T) {
	// Default: NiFi-appropriate 60s (not Kubernetes' 30s) so the flow stops gracefully.
	cluster := hardeningCluster()
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if got := spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != defaultTerminationGracePeriodSeconds {
		t.Fatalf("default terminationGracePeriodSeconds = %v, want %d", got, defaultTerminationGracePeriodSeconds)
	}

	// A custom value is honored verbatim.
	cluster.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{TerminationGracePeriodSeconds: ptr.To[int64](120)}
	spec = desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if got := spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 120 {
		t.Fatalf("custom terminationGracePeriodSeconds = %v, want 120", got)
	}

	// An explicit 0 (immediate SIGKILL) is honored, not replaced by the default.
	cluster.Spec.Pod.TerminationGracePeriodSeconds = ptr.To[int64](0)
	spec = desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if got := spec.Template.Spec.TerminationGracePeriodSeconds; got == nil || *got != 0 {
		t.Fatalf("explicit 0 terminationGracePeriodSeconds = %v, want 0", got)
	}
}

func TestManagedClusterProbeTuning(t *testing.T) {
	// Defaults (no spec.pod.probes): the operator's hardcoded schedule and actions.
	cluster := hardeningCluster()
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	c := spec.Template.Spec.Containers[0]
	if c.StartupProbe.PeriodSeconds != 10 || c.StartupProbe.FailureThreshold != 60 {
		t.Fatalf("default startup probe = %#v", c.StartupProbe)
	}
	if c.LivenessProbe.PeriodSeconds != 20 || c.LivenessProbe.FailureThreshold != 3 {
		t.Fatalf("default liveness probe = %#v", c.LivenessProbe)
	}
	if c.ReadinessProbe.PeriodSeconds != 10 || c.ReadinessProbe.FailureThreshold != 3 {
		t.Fatalf("default readiness probe = %#v", c.ReadinessProbe)
	}
	// The non-TLS action is an httpGet against the NiFi about endpoint.
	if c.StartupProbe.HTTPGet == nil || c.StartupProbe.HTTPGet.Path != "/nifi-api/flow/about" {
		t.Fatalf("default startup probe action changed: %#v", c.StartupProbe.ProbeHandler)
	}

	// Tuning: widen the startup boot window, slow the liveness cadence, lengthen the
	// readiness timeout. Unset fields keep their defaults; the action stays operator-managed.
	cluster.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{
		Probes: &nifiv1alpha1.NiFiClusterProbesSpec{
			Startup:   &nifiv1alpha1.NiFiClusterProbeTuning{PeriodSeconds: ptr.To[int32](15), FailureThreshold: ptr.To[int32](120)},
			Liveness:  &nifiv1alpha1.NiFiClusterProbeTuning{PeriodSeconds: ptr.To[int32](30), FailureThreshold: ptr.To[int32](5)},
			Readiness: &nifiv1alpha1.NiFiClusterProbeTuning{TimeoutSeconds: ptr.To[int32](8), InitialDelaySeconds: ptr.To[int32](12)},
		},
	}
	spec = desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	c = spec.Template.Spec.Containers[0]
	if c.StartupProbe.PeriodSeconds != 15 || c.StartupProbe.FailureThreshold != 120 {
		t.Fatalf("tuned startup probe = %#v", c.StartupProbe)
	}
	if c.StartupProbe.TimeoutSeconds != 3 { // unset -> default preserved
		t.Fatalf("tuned startup probe clobbered timeout: %#v", c.StartupProbe)
	}
	if c.StartupProbe.HTTPGet == nil || c.StartupProbe.HTTPGet.Path != "/nifi-api/flow/about" {
		t.Fatalf("tuning must not change the probe action: %#v", c.StartupProbe.ProbeHandler)
	}
	if c.LivenessProbe.PeriodSeconds != 30 || c.LivenessProbe.FailureThreshold != 5 {
		t.Fatalf("tuned liveness probe = %#v", c.LivenessProbe)
	}
	if c.ReadinessProbe.TimeoutSeconds != 8 || c.ReadinessProbe.InitialDelaySeconds != 12 {
		t.Fatalf("tuned readiness probe = %#v", c.ReadinessProbe)
	}
	if c.ReadinessProbe.PeriodSeconds != 10 { // unset -> default preserved
		t.Fatalf("tuned readiness probe clobbered period: %#v", c.ReadinessProbe)
	}
}

func TestManagedClusterPodNetworking(t *testing.T) {
	// Default: no host aliases, no explicit DNS policy/config.
	cluster := hardeningCluster()
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if len(spec.Template.Spec.HostAliases) != 0 || spec.Template.Spec.DNSPolicy != "" || spec.Template.Spec.DNSConfig != nil {
		t.Fatalf("default pod networking not empty: hostAliases=%v dnsPolicy=%q dnsConfig=%v",
			spec.Template.Spec.HostAliases, spec.Template.Spec.DNSPolicy, spec.Template.Spec.DNSConfig)
	}

	// Set: hostAliases, dnsPolicy, and dnsConfig all pass through to the pod spec.
	cluster.Spec.Pod = &nifiv1alpha1.NiFiClusterPodSpec{
		HostAliases: []corev1.HostAlias{{IP: "10.9.8.7", Hostnames: []string{"ldap.internal.example.com"}}},
		DNSPolicy:   ptr.To(corev1.DNSClusterFirst),
		DNSConfig:   &corev1.PodDNSConfig{Nameservers: []string{"10.9.8.53"}, Searches: []string{"internal.example.com"}},
	}
	spec = desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	ps := spec.Template.Spec
	if len(ps.HostAliases) != 1 || ps.HostAliases[0].IP != "10.9.8.7" || ps.HostAliases[0].Hostnames[0] != "ldap.internal.example.com" {
		t.Fatalf("hostAliases not applied: %#v", ps.HostAliases)
	}
	if ps.DNSPolicy != corev1.DNSClusterFirst {
		t.Fatalf("dnsPolicy not applied: %q", ps.DNSPolicy)
	}
	if ps.DNSConfig == nil || len(ps.DNSConfig.Searches) != 1 || ps.DNSConfig.Searches[0] != "internal.example.com" {
		t.Fatalf("dnsConfig not applied: %#v", ps.DNSConfig)
	}

	// The same pod networking applies to NiFiNodeGroup pools.
	group := &nifiv1alpha1.NiFiNodeGroup{Spec: nifiv1alpha1.NiFiNodeGroupSpec{Replicas: 1}}
	ngSpec := desiredNodeGroupStatefulSetSpec(cluster, group, nil, 1, "", "", nil)
	if len(ngSpec.Template.Spec.HostAliases) != 1 || ngSpec.Template.Spec.DNSConfig == nil {
		t.Fatalf("node group pod networking not applied: hostAliases=%v dnsConfig=%v",
			ngSpec.Template.Spec.HostAliases, ngSpec.Template.Spec.DNSConfig)
	}
}

func TestManagedClusterOneNiFiNodePerNode(t *testing.T) {
	// Off (default): no affinity synthesized.
	cluster := hardeningCluster()
	if aff := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil).Template.Spec.Affinity; aff != nil {
		t.Fatalf("default affinity = %#v, want nil", aff)
	}

	// On: a required host anti-affinity term selecting this cluster's NiFi pods.
	cluster.Spec.Scheduling = &nifiv1alpha1.NiFiClusterScheduling{OneNiFiNodePerNode: true}
	spec := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	aff := spec.Template.Spec.Affinity
	if aff == nil || aff.PodAntiAffinity == nil || len(aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("one-node-per-node affinity = %#v", aff)
	}
	term := aff.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.TopologyKey != "kubernetes.io/hostname" {
		t.Fatalf("topologyKey = %q, want kubernetes.io/hostname", term.TopologyKey)
	}
	if term.LabelSelector == nil || term.LabelSelector.MatchLabels[managedClusterLabel] != managedClusterResourceName(cluster) {
		t.Fatalf("anti-affinity selector = %#v", term.LabelSelector)
	}

	// Building the spec again must not accumulate terms (input spec is not mutated).
	spec2 := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil)
	if got := len(spec2.Template.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution); got != 1 {
		t.Fatalf("anti-affinity terms accumulated across reconciles: %d", got)
	}
	if cluster.Spec.Scheduling.Affinity != nil {
		t.Fatal("resolver mutated the spec's scheduling.affinity")
	}

	// Merges with a user-provided nodeAffinity: both survive.
	cluster.Spec.Scheduling.Affinity = &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{
					MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "disktype", Operator: corev1.NodeSelectorOpIn, Values: []string{"ssd"}}},
				}},
			},
		},
	}
	merged := desiredManagedClusterStatefulSetSpec(cluster, nil, "", nil).Template.Spec.Affinity
	if merged.NodeAffinity == nil {
		t.Fatal("user nodeAffinity dropped when merging one-node-per-node")
	}
	if merged.PodAntiAffinity == nil || len(merged.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("host anti-affinity not merged in: %#v", merged.PodAntiAffinity)
	}

	// Applies to NiFiNodeGroup pools too.
	group := &nifiv1alpha1.NiFiNodeGroup{Spec: nifiv1alpha1.NiFiNodeGroupSpec{Replicas: 1}}
	ng := desiredNodeGroupStatefulSetSpec(cluster, group, nil, 1, "", "", nil).Template.Spec.Affinity
	if ng == nil || ng.PodAntiAffinity == nil || len(ng.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution) != 1 {
		t.Fatalf("node group host anti-affinity missing: %#v", ng)
	}
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
