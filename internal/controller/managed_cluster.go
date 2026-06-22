package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strconv"
	"strings"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	managedClusterAnnotation = "nifi.controlnifi.io/cluster-name"
	managedClusterLabel      = "nifi.controlnifi.io/cluster"
	defaultNiFiImage         = "apache/nifi:2.10.0"
	defaultNiFiWebPort       = int32(8080)
	defaultClusterPort       = int32(11443)
	managedDataVolume        = "data"
)

const managedNiFiStartCommand = `. /opt/nifi/scripts/common.sh
prop_replace 'java.arg.2' "-Xms${NIFI_JVM_HEAP_INIT}" "${nifi_bootstrap_file}"
prop_replace 'java.arg.3' "-Xmx${NIFI_JVM_HEAP_MAX}" "${nifi_bootstrap_file}"
uncomment 'nifi.python.command' "${nifi_props_file}"
prop_replace 'nifi.python.extensions.source.directory.default' "${NIFI_HOME}/python_extensions"
prop_replace 'nifi.nar.library.autoload.directory' "${NIFI_HOME}/nar_extensions"
prop_replace 'nifi.web.http.host' "${NIFI_WEB_HTTP_HOST:-0.0.0.0}"
prop_replace 'nifi.web.http.port' "${NIFI_WEB_HTTP_PORT:-8080}"
prop_replace 'nifi.web.https.host' ''
prop_replace 'nifi.web.https.port' ''
prop_replace 'nifi.security.keystore' ''
prop_replace 'nifi.security.keystoreType' ''
prop_replace 'nifi.security.keystorePasswd' ''
prop_replace 'nifi.security.keyPasswd' ''
prop_replace 'nifi.security.truststore' ''
prop_replace 'nifi.security.truststoreType' ''
prop_replace 'nifi.security.truststorePasswd' ''
prop_replace 'nifi.remote.input.host' "${NIFI_REMOTE_INPUT_HOST:-${HOSTNAME}}"
prop_replace 'nifi.remote.input.socket.port' "${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
prop_replace 'nifi.remote.input.secure' 'false'
prop_replace 'nifi.cluster.is.node' "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.node.address' "${NIFI_CLUSTER_ADDRESS:-${HOSTNAME}}"
prop_replace 'nifi.cluster.node.protocol.port' "${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
prop_replace 'nifi.cluster.load.balance.host' "${NIFI_CLUSTER_LOAD_BALANCE_HOST:-}"
prop_replace 'nifi.zookeeper.connect.string' "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node' "${NIFI_ZK_ROOT_NODE:-/nifi}"
prop_replace 'nifi.cluster.flow.election.max.wait.time' "${NIFI_ELECTION_MAX_WAIT:-5 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates' "${NIFI_ELECTION_MAX_CANDIDATES:-}"
exec "${NIFI_HOME}/bin/nifi.sh" run`

// managedNiFiStartCommandTLS configures NiFi 2.10 for HTTPS and certificate
// authentication. The PKCS12 keystore/truststore are mounted from a cert-manager (or
// externally supplied) Secret; the password is injected from a Secret, never a literal.
// The operator-rendered authorizers.xml is copied over the seeded conf file so the
// initial admin identity matches the operator client certificate. login-identity-
// providers.xml is intentionally left untouched: pure certificate authentication needs
// only the managed authorizer.
const managedNiFiStartCommandTLS = `. /opt/nifi/scripts/common.sh
prop_replace 'java.arg.2' "-Xms${NIFI_JVM_HEAP_INIT}" "${nifi_bootstrap_file}"
prop_replace 'java.arg.3' "-Xmx${NIFI_JVM_HEAP_MAX}" "${nifi_bootstrap_file}"
uncomment 'nifi.python.command' "${nifi_props_file}"
prop_replace 'nifi.python.extensions.source.directory.default' "${NIFI_HOME}/python_extensions"
prop_replace 'nifi.nar.library.autoload.directory' "${NIFI_HOME}/nar_extensions"
prop_replace 'nifi.web.http.host' ''
prop_replace 'nifi.web.http.port' ''
prop_replace 'nifi.web.https.host' '0.0.0.0'
prop_replace 'nifi.web.https.port' "${NIFI_WEB_HTTPS_PORT}"
prop_replace 'nifi.web.proxy.host' "${NIFI_WEB_PROXY_HOST}"
prop_replace 'nifi.security.keystore' "${NIFI_SECURITY_DIR}/keystore.p12"
prop_replace 'nifi.security.keystoreType' 'PKCS12'
prop_replace 'nifi.security.keystorePasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.keyPasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.truststore' "${NIFI_SECURITY_DIR}/truststore.p12"
prop_replace 'nifi.security.truststoreType' 'PKCS12'
prop_replace 'nifi.security.truststorePasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.needClientAuth' 'true'
prop_replace 'nifi.security.allow.anonymous.authentication' 'false'
prop_replace 'nifi.security.user.authorizer' 'managed-authorizer'
prop_replace 'nifi.security.user.login.identity.provider' ''
prop_replace 'nifi.remote.input.host' "${NIFI_REMOTE_INPUT_HOST:-${HOSTNAME}}"
prop_replace 'nifi.remote.input.socket.port' "${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
prop_replace 'nifi.remote.input.secure' 'true'
prop_replace 'nifi.cluster.protocol.is.secure' "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.is.node' "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.node.address' "${NIFI_CLUSTER_ADDRESS:-${HOSTNAME}}"
prop_replace 'nifi.cluster.node.protocol.port' "${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
prop_replace 'nifi.cluster.load.balance.host' "${NIFI_CLUSTER_LOAD_BALANCE_HOST:-}"
prop_replace 'nifi.zookeeper.connect.string' "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node' "${NIFI_ZK_ROOT_NODE:-/nifi}"
prop_replace 'nifi.cluster.flow.election.max.wait.time' "${NIFI_ELECTION_MAX_WAIT:-2 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates' "${NIFI_ELECTION_MAX_CANDIDATES:-}"
cp "${NIFI_CONFIG_DIR}/authorizers.xml" "${NIFI_HOME}/conf/authorizers.xml"
exec "${NIFI_HOME}/bin/nifi.sh" run`

var managedDataDirectories = []string{
	"conf",
	"database_repository",
	"flowfile_repository",
	"content_repository",
	"provenance_repository",
	"state",
}

func resolvedClusterMode(cluster *nifiv1alpha1.NiFiCluster) nifiv1alpha1.ClusterMode {
	if cluster.Spec.Mode != "" {
		return cluster.Spec.Mode
	}
	if cluster.Spec.API != nil && cluster.Spec.API.URI != "" {
		return nifiv1alpha1.ClusterModeExternal
	}
	return nifiv1alpha1.ClusterModeInternal
}

func (r *NiFiClusterReconciler) reconcileManagedCluster(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) (ctrl.Result, error) {
	replicas := managedClusterReplicas(cluster)
	if replicas > 1 && (cluster.Spec.Coordination == nil || cluster.Spec.Coordination.ZooKeeperConnectString == "") {
		message := "spec.coordination.zookeeperConnectString is required when replicas is greater than one."
		if managedClusterStatusNeedsUpdate(cluster, false, managedClusterEndpoint(cluster), nil, "ConfigurationInvalid") {
			return ctrl.Result{}, markManagedClusterNotReady(ctx, r.Client, cluster, "ConfigurationInvalid", message, managedClusterEndpoint(cluster), nil)
		}
		return ctrl.Result{}, nil
	}

	var tlsMaterials *clusterTLSMaterials
	if internalTLSEnabled(cluster) {
		materials, ready, err := r.reconcileManagedClusterTLS(ctx, cluster)
		if err != nil {
			return r.managedClusterReconcileFailed(ctx, cluster, "TLSReconcileFailed", err)
		}
		if !ready {
			// reconcileManagedClusterTLS already recorded the pending/error status.
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
		tlsMaterials = materials
	}

	if err := r.reconcileManagedClusterService(ctx, cluster, false); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ServiceReconcileFailed", err)
	}
	if err := r.reconcileManagedClusterService(ctx, cluster, true); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ServiceReconcileFailed", err)
	}
	statefulSet, err := r.reconcileManagedClusterStatefulSet(ctx, cluster, tlsMaterials)
	if err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "StatefulSetReconcileFailed", err)
	}

	endpoint := managedClusterEndpoint(cluster)
	if err := configureClusterHTTPClient(ctx, r.Client, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "APIClientConfigurationFailed", err)
	}
	workload := &nifiv1alpha1.NiFiClusterWorkloadStatus{
		StatefulSetName: statefulSet.Name,
		ServiceName:     managedClusterResourceName(cluster),
		HeadlessService: managedClusterHeadlessServiceName(cluster),
		Replicas:        replicas,
		ReadyReplicas:   statefulSet.Status.ReadyReplicas,
	}
	if statefulSet.Status.ReadyReplicas < replicas {
		message := fmt.Sprintf("Waiting for NiFi StatefulSet replicas: %d/%d ready.", statefulSet.Status.ReadyReplicas, replicas)
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, workload, "Provisioning") {
			if err := markManagedClusterNotReady(ctx, r.Client, cluster, "Provisioning", message, endpoint, workload); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	timeout := time.Duration(0)
	if cluster.Spec.API != nil && cluster.Spec.API.Timeout != nil {
		timeout = cluster.Spec.API.Timeout.Duration
	}
	checker := r.ReachabilityChecker
	if checker == nil {
		checker = nifi.HTTPReachabilityChecker{}
	}
	if err := checker.CheckReachable(ctx, endpoint, timeout); err != nil {
		message := fmt.Sprintf("The managed NiFi workload is ready, but its API is not reachable: %v", err)
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, workload, "ClusterUnreachable") {
			if statusErr := markManagedClusterNotReady(ctx, r.Client, cluster, "ClusterUnreachable", message, endpoint, workload); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if managedClusterStatusNeedsUpdate(cluster, true, endpoint, workload, "ClusterReachable") {
		return ctrl.Result{}, markManagedClusterReady(ctx, r.Client, cluster, endpoint, workload)
	}
	return ctrl.Result{}, nil
}

func (r *NiFiClusterReconciler) reconcileManagedClusterService(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, headless bool) error {
	name := managedClusterResourceName(cluster)
	if headless {
		name = managedClusterHeadlessServiceName(cluster)
	}
	service := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		if err := assertManagedClusterResource(service, cluster); err != nil {
			return err
		}
		service.Labels = managedClusterLabels(cluster)
		if headless {
			service.Annotations = managedClusterAnnotations(cluster)
		} else {
			service.Annotations = managedClusterServiceAnnotations(cluster)
		}
		service.Spec.Selector = managedClusterPodLabels(cluster)
		service.Spec.Ports = []corev1.ServicePort{
			{
				Name:       "web",
				Port:       managedClusterServicePort(cluster),
				TargetPort: intstrFromString("web"),
				Protocol:   corev1.ProtocolTCP,
				NodePort:   managedClusterNodePort(cluster, headless),
			},
		}
		if headless {
			service.Spec.Type = corev1.ServiceTypeClusterIP
			service.Spec.PublishNotReadyAddresses = true
			service.Spec.Ports = append(service.Spec.Ports, corev1.ServicePort{
				Name:       "cluster",
				Port:       defaultClusterPort,
				TargetPort: intstrFromString("cluster"),
				Protocol:   corev1.ProtocolTCP,
			})
			if service.ResourceVersion == "" {
				service.Spec.ClusterIP = corev1.ClusterIPNone
			}
			return nil
		}
		service.Spec.Type = managedClusterServiceType(cluster)
		service.Spec.PublishNotReadyAddresses = false
		return nil
	})
	return err
}

func (r *NiFiClusterReconciler) reconcileManagedClusterStatefulSet(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) (*appsv1.StatefulSet, error) {
	name := managedClusterResourceName(cluster)
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		if err := assertManagedClusterResource(statefulSet, cluster); err != nil {
			return err
		}
		statefulSet.Labels = managedClusterLabels(cluster)
		statefulSet.Annotations = managedClusterAnnotations(cluster)
		desired := desiredManagedClusterStatefulSetSpec(cluster, tls)
		if statefulSet.ResourceVersion != "" {
			desired.ServiceName = statefulSet.Spec.ServiceName
			if statefulSet.Spec.Selector != nil {
				desired.Selector = statefulSet.Spec.Selector.DeepCopy()
			}
			desired.VolumeClaimTemplates = statefulSet.Spec.VolumeClaimTemplates
		}
		statefulSet.Spec = desired
		return nil
	})
	return statefulSet, err
}

func desiredManagedClusterStatefulSetSpec(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) appsv1.StatefulSetSpec {
	podLabels := managedClusterPodLabels(cluster)
	webPort := defaultNiFiWebPort
	startCommand := managedNiFiStartCommand
	if tls != nil {
		webPort = tls.httpsPort
		startCommand = managedNiFiStartCommandTLS
	}
	container := corev1.Container{
		Name:            "nifi",
		Image:           managedClusterImage(cluster),
		ImagePullPolicy: managedClusterImagePullPolicy(cluster),
		Command:         []string{"/bin/bash", "-ec", startCommand},
		Env:             managedClusterEnvironment(cluster, tls),
		Ports: []corev1.ContainerPort{
			{Name: "web", ContainerPort: webPort, Protocol: corev1.ProtocolTCP},
			{Name: "cluster", ContainerPort: defaultClusterPort, Protocol: corev1.ProtocolTCP},
		},
		Resources:      cluster.Spec.Resources,
		StartupProbe:   managedClusterStartupProbe(tls),
		LivenessProbe:  managedClusterLivenessProbe(tls),
		ReadinessProbe: managedClusterReadinessProbe(tls),
		VolumeMounts:   managedClusterVolumeMounts(tls),
	}
	podSpec := corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{
			FSGroup:             ptr.To[int64](1000),
			FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		},
		InitContainers: []corev1.Container{managedClusterDataInitializer(cluster)},
		Containers:     []corev1.Container{container},
		Volumes:        managedClusterVolumes(cluster, tls),
	}
	annotations := managedClusterAnnotations(cluster)
	if tls != nil && tls.checksum != "" {
		annotations[managedTLSChecksumAnnotation] = tls.checksum
	}
	spec := appsv1.StatefulSetSpec{
		ServiceName:          managedClusterHeadlessServiceName(cluster),
		Replicas:             ptr.To(managedClusterReplicas(cluster)),
		RevisionHistoryLimit: ptr.To[int32](10),
		PodManagementPolicy:  appsv1.ParallelPodManagement,
		Selector:             &metav1.LabelSelector{MatchLabels: podLabels},
		UpdateStrategy:       appsv1.StatefulSetUpdateStrategy{Type: appsv1.RollingUpdateStatefulSetStrategyType},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: annotations,
			},
			Spec: podSpec,
		},
	}
	if managedClusterStorageEnabled(cluster) {
		spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{managedClusterVolumeClaim(cluster)}
	}
	return spec
}

// managedClusterVolumes returns the pod volumes, adding read-only mounts for the TLS
// Secret and operator-rendered config when internal TLS is enabled.
func managedClusterVolumes(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) []corev1.Volume {
	volumes := []corev1.Volume{}
	if !managedClusterStorageEnabled(cluster) {
		volumes = append(volumes, corev1.Volume{
			Name:         managedDataVolume,
			VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}},
		})
	}
	if tls != nil {
		volumes = append(volumes,
			corev1.Volume{
				Name: managedTLSVolume,
				VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
					SecretName:  tls.serverSecretName,
					DefaultMode: ptr.To[int32](0o440),
				}},
			},
			corev1.Volume{
				Name: managedTLSConfigVol,
				VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: tls.configMapName},
					DefaultMode:          ptr.To[int32](0o550),
				}},
			},
		)
	}
	return volumes
}

func managedClusterStartupProbe(tls *clusterTLSMaterials) *corev1.Probe {
	if tls != nil {
		return &corev1.Probe{ProbeHandler: tlsReadinessExecHandler(), PeriodSeconds: 10, TimeoutSeconds: 5, FailureThreshold: 60}
	}
	return &corev1.Probe{ProbeHandler: httpProbeHandler("/nifi-api/flow/about", "web"), PeriodSeconds: 10, TimeoutSeconds: 3, FailureThreshold: 60}
}

func managedClusterLivenessProbe(tls *clusterTLSMaterials) *corev1.Probe {
	if tls != nil {
		// Liveness only needs the secured port to be accepting connections; an httpGet
		// probe cannot present a client certificate under needClientAuth.
		return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstrFromString("web")}}, PeriodSeconds: 20, TimeoutSeconds: 3, FailureThreshold: 3}
	}
	return &corev1.Probe{ProbeHandler: httpProbeHandler("/nifi-api/flow/about", "web"), PeriodSeconds: 20, TimeoutSeconds: 3, FailureThreshold: 3}
}

func managedClusterReadinessProbe(tls *clusterTLSMaterials) *corev1.Probe {
	if tls != nil {
		return &corev1.Probe{ProbeHandler: tlsReadinessExecHandler(), PeriodSeconds: 10, TimeoutSeconds: 5, FailureThreshold: 3}
	}
	return &corev1.Probe{ProbeHandler: httpProbeHandler("/nifi-api/flow/about", "web"), PeriodSeconds: 10, TimeoutSeconds: 3, FailureThreshold: 3}
}

func tlsReadinessExecHandler() corev1.ProbeHandler {
	return corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/bash", managedTLSConfigDir + "/tls-readiness.sh"}}}
}

func managedClusterDataInitializer(cluster *nifiv1alpha1.NiFiCluster) corev1.Container {
	return corev1.Container{
		Name:            "initialize-data",
		Image:           managedClusterImage(cluster),
		ImagePullPolicy: managedClusterImagePullPolicy(cluster),
		Command: []string{
			"/bin/bash",
			"-ec",
			"mkdir -p /mnt/data/{conf,database_repository,flowfile_repository,content_repository,provenance_repository,state}; if [ ! -f /mnt/data/conf/nifi.properties ]; then cp -a /opt/nifi/nifi-current/conf/. /mnt/data/conf/; fi",
		},
		VolumeMounts: []corev1.VolumeMount{{Name: managedDataVolume, MountPath: "/mnt/data"}},
	}
}

func managedClusterVolumeMounts(tls *clusterTLSMaterials) []corev1.VolumeMount {
	mounts := make([]corev1.VolumeMount, 0, len(managedDataDirectories)+2)
	for _, directory := range managedDataDirectories {
		mounts = append(mounts, corev1.VolumeMount{
			Name:      managedDataVolume,
			MountPath: "/opt/nifi/nifi-current/" + directory,
			SubPath:   directory,
		})
	}
	if tls != nil {
		mounts = append(mounts,
			corev1.VolumeMount{Name: managedTLSVolume, MountPath: managedTLSSecurityDir, ReadOnly: true},
			corev1.VolumeMount{Name: managedTLSConfigVol, MountPath: managedTLSConfigDir, ReadOnly: true},
		)
	}
	return mounts
}

func managedClusterVolumeClaim(cluster *nifiv1alpha1.NiFiCluster) corev1.PersistentVolumeClaim {
	accessModes := cluster.Spec.Storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        managedDataVolume,
			Labels:      managedClusterLabels(cluster),
			Annotations: managedClusterAnnotations(cluster),
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: cluster.Spec.Storage.StorageClassName,
			VolumeMode:       ptr.To(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: managedClusterStorageSize(cluster)},
			},
		},
	}
}

func managedClusterEnvironment(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials) []corev1.EnvVar {
	environment := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "NIFI_JVM_HEAP_INIT", Value: managedClusterHeapInitial(cluster)},
		{Name: "NIFI_JVM_HEAP_MAX", Value: managedClusterHeapMax(cluster)},
	}
	if tls != nil {
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_WEB_HTTPS_PORT", Value: strconv.Itoa(int(tls.httpsPort))},
			corev1.EnvVar{Name: "NIFI_SECURITY_DIR", Value: managedTLSSecurityDir},
			corev1.EnvVar{Name: "NIFI_CONFIG_DIR", Value: managedTLSConfigDir},
			corev1.EnvVar{Name: "NIFI_WEB_PROXY_HOST", Value: tls.proxyHosts},
			corev1.EnvVar{Name: "NIFI_KEYSTORE_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: tls.passwordSecretName},
				Key:                  tls.passwordSecretKey,
			}}},
		)
	} else {
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_WEB_HTTP_HOST", Value: "0.0.0.0"},
			corev1.EnvVar{Name: "NIFI_WEB_HTTP_PORT", Value: strconv.Itoa(int(defaultNiFiWebPort))},
		)
	}
	if managedClusterReplicas(cluster) > 1 {
		coordination := cluster.Spec.Coordination
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_CLUSTER_IS_NODE", Value: "true"},
			corev1.EnvVar{Name: "NIFI_CLUSTER_ADDRESS", Value: fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc", managedClusterHeadlessServiceName(cluster))},
			corev1.EnvVar{Name: "NIFI_CLUSTER_NODE_PROTOCOL_PORT", Value: strconv.Itoa(int(defaultClusterPort))},
			corev1.EnvVar{Name: "NIFI_ZK_CONNECT_STRING", Value: coordination.ZooKeeperConnectString},
			corev1.EnvVar{Name: "NIFI_ZK_ROOT_NODE", Value: managedClusterZooKeeperRootNode(cluster)},
			corev1.EnvVar{Name: "NIFI_ELECTION_MAX_WAIT", Value: managedClusterElectionMaxWait(cluster)},
			corev1.EnvVar{Name: "NIFI_ELECTION_MAX_CANDIDATES", Value: strconv.Itoa(int(managedClusterReplicas(cluster)))},
		)
	} else {
		environment = append(environment, corev1.EnvVar{Name: "NIFI_CLUSTER_IS_NODE", Value: "false"})
	}
	return mergeEnvironment(environment, cluster.Spec.AdditionalEnv)
}

func mergeEnvironment(base []corev1.EnvVar, overrides []corev1.EnvVar) []corev1.EnvVar {
	positions := make(map[string]int, len(base))
	for i := range base {
		positions[base[i].Name] = i
	}
	for _, override := range overrides {
		if position, ok := positions[override.Name]; ok {
			base[position] = override
			continue
		}
		positions[override.Name] = len(base)
		base = append(base, override)
	}
	return base
}

func (r *NiFiClusterReconciler) reconcileClusterDelete(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) (ctrl.Result, error) {
	if resolvedClusterMode(cluster) != nifiv1alpha1.ClusterModeInternal || cluster.Spec.DeletionPolicy != nifiv1alpha1.DeletionPolicyDelete {
		_, err := removeFinalizer(ctx, r.Client, cluster)
		return ctrl.Result{}, err
	}

	resources := []client.Object{
		&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: managedClusterHeadlessServiceName(cluster), Namespace: cluster.Namespace}},
	}
	for _, object := range resources {
		if err := r.deleteManagedClusterResource(ctx, cluster, object); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(cluster.Namespace), client.MatchingLabels{managedClusterLabel: managedClusterResourceName(cluster)}); err != nil {
		return ctrl.Result{}, err
	}
	for i := range pvcs.Items {
		if pvcs.Items[i].Annotations[managedClusterAnnotation] != cluster.Name {
			continue
		}
		if err := r.Delete(ctx, &pvcs.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}
	_, err := removeFinalizer(ctx, r.Client, cluster)
	return ctrl.Result{}, err
}

func (r *NiFiClusterReconciler) deleteManagedClusterResource(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, object client.Object) error {
	key := client.ObjectKeyFromObject(object)
	if err := r.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	if object.GetAnnotations()[managedClusterAnnotation] != cluster.Name {
		return fmt.Errorf("refusing to delete %s/%s because it is not managed by NiFiCluster %s", object.GetNamespace(), object.GetName(), cluster.Name)
	}
	return r.Delete(ctx, object)
}

func (r *NiFiClusterReconciler) requestsForManagedClusterResource(_ context.Context, obj client.Object) []reconcile.Request {
	clusterName := obj.GetAnnotations()[managedClusterAnnotation]
	if clusterName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: clusterName, Namespace: obj.GetNamespace()}}}
}

func (r *NiFiClusterReconciler) managedClusterReconcileFailed(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, reason string, reconciliationError error) (ctrl.Result, error) {
	message := reconciliationError.Error()
	if managedClusterStatusNeedsUpdate(cluster, false, managedClusterEndpoint(cluster), cluster.Status.Workload, reason) {
		if err := markManagedClusterNotReady(ctx, r.Client, cluster, reason, message, managedClusterEndpoint(cluster), cluster.Status.Workload); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
}

func markManagedClusterReady(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster, endpoint string, workload *nifiv1alpha1.NiFiClusterWorkloadStatus) error {
	cluster.Status.CommonStatus.MarkReady(cluster.Generation, "ClusterReachable", "The managed NiFi API endpoint is reachable.")
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionClusterReachable, metav1.ConditionTrue, "ClusterReachable", "The managed NiFi API endpoint is reachable.", cluster.Generation)
	cluster.Status.Endpoint = endpoint
	cluster.Status.Workload = workload
	cluster.Status.Sync.LastError = ""
	return c.Status().Update(ctx, cluster)
}

func markManagedClusterNotReady(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster, reason string, message string, endpoint string, workload *nifiv1alpha1.NiFiClusterWorkloadStatus) error {
	cluster.Status.CommonStatus.MarkNotReady(cluster.Generation, reason, message)
	cluster.Status.Dependencies.Ready = true
	cluster.Status.Dependencies.WaitingFor = nil
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionClusterReachable, metav1.ConditionFalse, reason, message, cluster.Generation)
	cluster.Status.Endpoint = endpoint
	cluster.Status.Workload = workload
	cluster.Status.Sync.LastError = message
	return c.Status().Update(ctx, cluster)
}

func managedClusterStatusNeedsUpdate(cluster *nifiv1alpha1.NiFiCluster, ready bool, endpoint string, workload *nifiv1alpha1.NiFiClusterWorkloadStatus, reason string) bool {
	if cluster.Status.ObservedGeneration != cluster.Generation || cluster.Status.Ready != ready || cluster.Status.Endpoint != endpoint {
		return true
	}
	if !managedWorkloadStatusEqual(cluster.Status.Workload, workload) {
		return true
	}
	for _, condition := range cluster.Status.Conditions {
		if condition.Type == string(nifiv1alpha1.ConditionReady) {
			return condition.Reason != reason
		}
	}
	return true
}

func managedWorkloadStatusEqual(left *nifiv1alpha1.NiFiClusterWorkloadStatus, right *nifiv1alpha1.NiFiClusterWorkloadStatus) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.StatefulSetName == right.StatefulSetName &&
		left.ServiceName == right.ServiceName &&
		left.HeadlessService == right.HeadlessService &&
		left.Replicas == right.Replicas &&
		left.ReadyReplicas == right.ReadyReplicas
}

func assertManagedClusterResource(obj client.Object, cluster *nifiv1alpha1.NiFiCluster) error {
	if obj.GetResourceVersion() == "" {
		return nil
	}
	if obj.GetAnnotations()[managedClusterAnnotation] != cluster.Name {
		return fmt.Errorf("%s %s/%s already exists and is not managed by NiFiCluster %s", obj.GetObjectKind().GroupVersionKind().Kind, obj.GetNamespace(), obj.GetName(), cluster.Name)
	}
	return nil
}

func managedClusterResourceName(cluster *nifiv1alpha1.NiFiCluster) string {
	return boundedManagedName(cluster.Name, "nifi")
}

func managedClusterHeadlessServiceName(cluster *nifiv1alpha1.NiFiCluster) string {
	return boundedManagedName(cluster.Name, "nifi-headless")
}

func boundedManagedName(base string, suffix string) string {
	candidate := strings.TrimSuffix(base, "-") + "-" + suffix
	if len(candidate) <= 63 {
		return candidate
	}
	digest := fmt.Sprintf("%x", sha256.Sum256([]byte(candidate)))[:8]
	maximumBaseLength := 63 - len(suffix) - len(digest) - 2
	trimmedBase := strings.TrimRight(base[:maximumBaseLength], "-")
	return fmt.Sprintf("%s-%s-%s", trimmedBase, digest, suffix)
}

func managedClusterLabels(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "apache-nifi",
		"app.kubernetes.io/instance":   managedClusterResourceName(cluster),
		"app.kubernetes.io/managed-by": "nificontrol",
		managedClusterLabel:            managedClusterResourceName(cluster),
	}
}

func managedClusterPodLabels(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	labels := managedClusterLabels(cluster)
	labels["app.kubernetes.io/component"] = "nifi-node"
	return labels
}

func managedClusterAnnotations(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	return map[string]string{managedClusterAnnotation: cluster.Name}
}

func managedClusterServiceAnnotations(cluster *nifiv1alpha1.NiFiCluster) map[string]string {
	annotations := make(map[string]string, len(cluster.Spec.Service.Annotations)+1)
	for key, value := range cluster.Spec.Service.Annotations {
		if key == managedClusterAnnotation {
			continue
		}
		annotations[key] = value
	}
	annotations[managedClusterAnnotation] = cluster.Name
	return annotations
}

func managedClusterEndpoint(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.API != nil && cluster.Spec.API.URI != "" {
		return cluster.Spec.API.URI
	}
	scheme := "http"
	if internalTLSEnabled(cluster) {
		scheme = "https"
	}
	return fmt.Sprintf("%s://%s.%s.svc:%d", scheme, managedClusterResourceName(cluster), cluster.Namespace, managedClusterServicePort(cluster))
}

func managedClusterImage(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Image != "" {
		return cluster.Spec.Image
	}
	return defaultNiFiImage
}

func managedClusterImagePullPolicy(cluster *nifiv1alpha1.NiFiCluster) corev1.PullPolicy {
	if cluster.Spec.ImagePullPolicy != "" {
		return cluster.Spec.ImagePullPolicy
	}
	return corev1.PullIfNotPresent
}

func managedClusterReplicas(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if cluster.Spec.Replicas > 0 {
		return cluster.Spec.Replicas
	}
	return 1
}

func managedClusterServicePort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	// In TLS mode the managed Service always exposes the HTTPS port; spec.service.port
	// applies to HTTP (development) mode only.
	if internalTLSEnabled(cluster) {
		return managedClusterHTTPSPort(cluster)
	}
	if cluster.Spec.Service.Port > 0 {
		return cluster.Spec.Service.Port
	}
	return defaultNiFiWebPort
}

func managedClusterServiceType(cluster *nifiv1alpha1.NiFiCluster) corev1.ServiceType {
	if cluster.Spec.Service.Type != "" {
		return cluster.Spec.Service.Type
	}
	return corev1.ServiceTypeClusterIP
}

func managedClusterNodePort(cluster *nifiv1alpha1.NiFiCluster, headless bool) int32 {
	if headless || managedClusterServiceType(cluster) == corev1.ServiceTypeClusterIP {
		return 0
	}
	return cluster.Spec.Service.NodePort
}

func managedClusterStorageEnabled(cluster *nifiv1alpha1.NiFiCluster) bool {
	return cluster.Spec.Storage.Enabled == nil || *cluster.Spec.Storage.Enabled
}

func managedClusterStorageSize(cluster *nifiv1alpha1.NiFiCluster) resource.Quantity {
	if !cluster.Spec.Storage.Size.IsZero() {
		return cluster.Spec.Storage.Size.DeepCopy()
	}
	return resource.MustParse("10Gi")
}

func managedClusterHeapInitial(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.JVM.HeapInitial != "" {
		return cluster.Spec.JVM.HeapInitial
	}
	return "1g"
}

func managedClusterHeapMax(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.JVM.HeapMax != "" {
		return cluster.Spec.JVM.HeapMax
	}
	return "1g"
}

func managedClusterZooKeeperRootNode(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Coordination != nil && cluster.Spec.Coordination.ZooKeeperRootNode != "" {
		return cluster.Spec.Coordination.ZooKeeperRootNode
	}
	return "/nifi"
}

func managedClusterElectionMaxWait(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.Coordination != nil && cluster.Spec.Coordination.ElectionMaxWait != "" {
		return cluster.Spec.Coordination.ElectionMaxWait
	}
	return "2 mins"
}

func httpProbeHandler(path string, portName string) corev1.ProbeHandler {
	return corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstrFromString(portName), Scheme: corev1.URISchemeHTTP}}
}

func intstrFromString(value string) intstr.IntOrString {
	return intstr.FromString(value)
}
