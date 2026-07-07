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
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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
	defaultRemoteInputPort   = int32(10000)
	defaultLoadBalancePort   = int32(6342)
	managedDataVolume        = "data"
	// managedExternalServiceLabel marks Services the operator provisions from
	// spec.externalServices so they can be listed and pruned when dropped from the spec.
	managedExternalServiceLabel = "nifi.controlnifi.io/external-service"
)

const managedNiFiStartCommand = `. /opt/nifi/scripts/common.sh
prop_replace 'java.arg.2' "-Xms${NIFI_JVM_HEAP_INIT}" "${nifi_bootstrap_file}"
prop_replace 'java.arg.3' "-Xmx${NIFI_JVM_HEAP_MAX}" "${nifi_bootstrap_file}"
uncomment 'nifi.python.command' "${nifi_props_file}"
prop_replace 'nifi.python.extensions.source.directory.default' "${NIFI_HOME}/python_extensions"
prop_replace 'nifi.nar.library.autoload.directory' "${NIFI_HOME}/nar_extensions"
current_sensitive_key=$(grep '^nifi.sensitive.props.key=' "${nifi_props_file}" | head -1 | cut -d= -f2-)
if [ -n "${NIFI_SENSITIVE_PROPS_KEY:-}" ] && [ -n "${current_sensitive_key}" ] && [ "${current_sensitive_key}" != "${NIFI_SENSITIVE_PROPS_KEY}" ] && [ -f "${NIFI_HOME}/conf/flow.json.gz" ]; then
  "${NIFI_HOME}/bin/nifi.sh" set-sensitive-properties-key "${NIFI_SENSITIVE_PROPS_KEY}" >/dev/null
fi
prop_replace 'nifi.sensitive.props.key' "${NIFI_SENSITIVE_PROPS_KEY:-}"
prop_replace 'nifi.web.http.host' "${NIFI_WEB_HTTP_HOST:-0.0.0.0}"
prop_replace 'nifi.web.http.port' "${NIFI_WEB_HTTP_PORT:-8080}"
prop_replace 'nifi.web.proxy.host' "${NIFI_WEB_PROXY_HOST:-}"
prop_replace 'nifi.web.proxy.context.path' "${NIFI_WEB_PROXY_CONTEXT_PATH:-}"
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
prop_replace 'nifi.cluster.load.balance.port' "${NIFI_CLUSTER_LOAD_BALANCE_PORT:-6342}"
prop_replace 'nifi.zookeeper.connect.string' "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node' "${NIFI_ZK_ROOT_NODE:-/nifi}"
if [ -n "${NIFI_ZK_CONNECT_STRING:-}" ]; then
  sed -i "s|<property name=\"Connect String\">[^<]*</property>|<property name=\"Connect String\">${NIFI_ZK_CONNECT_STRING}</property>|" "${NIFI_HOME}/conf/state-management.xml"
  sed -i "s|<property name=\"Root Node\">[^<]*</property>|<property name=\"Root Node\">${NIFI_ZK_ROOT_NODE:-/nifi}</property>|" "${NIFI_HOME}/conf/state-management.xml"
fi
prop_replace 'nifi.cluster.flow.election.max.wait.time' "${NIFI_ELECTION_MAX_WAIT:-5 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates' "${NIFI_ELECTION_MAX_CANDIDATES:-}"
` + applyConfigOverridesScript + `exec "${NIFI_HOME}/bin/nifi.sh" run`

// managedNiFiStartCommandTLS configures NiFi 2.10 for HTTPS and certificate
// authentication. The PKCS12 keystore/truststore are mounted from a cert-manager (or
// externally supplied) Secret; the password is injected from a Secret, never a literal.
// The operator-rendered authorizers.xml is copied over the seeded conf file so the
// initial admin identity matches the operator client certificate. When
// spec.authentication is set, the login provider is selected through
// NIFI_LOGIN_IDENTITY_PROVIDER (single-user or LDAP; OIDC is properties-only), an
// operator-rendered login-identity-providers.xml is copied in for LDAP, and single-user
// credentials are applied with nifi.sh set-single-user-credentials so NiFi hashes the
// password itself. Certificate authentication always runs first, so the operator's mTLS
// access never depends on the login provider.
const managedNiFiStartCommandTLS = `. /opt/nifi/scripts/common.sh
prop_replace 'java.arg.2' "-Xms${NIFI_JVM_HEAP_INIT}" "${nifi_bootstrap_file}"
prop_replace 'java.arg.3' "-Xmx${NIFI_JVM_HEAP_MAX}" "${nifi_bootstrap_file}"
uncomment 'nifi.python.command' "${nifi_props_file}"
prop_replace 'nifi.python.extensions.source.directory.default' "${NIFI_HOME}/python_extensions"
prop_replace 'nifi.nar.library.autoload.directory' "${NIFI_HOME}/nar_extensions"
current_sensitive_key=$(grep '^nifi.sensitive.props.key=' "${nifi_props_file}" | head -1 | cut -d= -f2-)
if [ -n "${NIFI_SENSITIVE_PROPS_KEY:-}" ] && [ -n "${current_sensitive_key}" ] && [ "${current_sensitive_key}" != "${NIFI_SENSITIVE_PROPS_KEY}" ] && [ -f "${NIFI_HOME}/conf/flow.json.gz" ]; then
  "${NIFI_HOME}/bin/nifi.sh" set-sensitive-properties-key "${NIFI_SENSITIVE_PROPS_KEY}" >/dev/null
fi
prop_replace 'nifi.sensitive.props.key' "${NIFI_SENSITIVE_PROPS_KEY:-}"
prop_replace 'nifi.web.http.host' ''
prop_replace 'nifi.web.http.port' ''
prop_replace 'nifi.web.https.host' "${NIFI_WEB_HTTPS_HOST:-0.0.0.0}"
prop_replace 'nifi.web.https.port' "${NIFI_WEB_HTTPS_PORT}"
prop_replace 'nifi.web.proxy.host' "${NIFI_WEB_PROXY_HOST}"
prop_replace 'nifi.web.proxy.context.path' "${NIFI_WEB_PROXY_CONTEXT_PATH:-}"
prop_replace 'nifi.security.keystore' "${NIFI_SECURITY_DIR}/keystore.p12"
prop_replace 'nifi.security.keystoreType' 'PKCS12'
prop_replace 'nifi.security.keystorePasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.keyPasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.truststore' "${NIFI_SECURITY_DIR}/truststore.p12"
prop_replace 'nifi.security.truststoreType' 'PKCS12'
prop_replace 'nifi.security.truststorePasswd' "${NIFI_KEYSTORE_PASSWORD}"
prop_replace 'nifi.security.needClientAuth' "${NIFI_NEED_CLIENT_AUTH:-true}"
prop_replace 'nifi.security.allow.anonymous.authentication' 'false'
prop_replace 'nifi.security.user.authorizer' 'managed-authorizer'
prop_replace 'nifi.security.user.login.identity.provider' "${NIFI_LOGIN_IDENTITY_PROVIDER:-}"
prop_replace 'nifi.security.user.oidc.discovery.url' "${NIFI_OIDC_DISCOVERY_URL:-}"
prop_replace 'nifi.security.user.oidc.client.id' "${NIFI_OIDC_CLIENT_ID:-}"
prop_replace 'nifi.security.user.oidc.client.secret' "${NIFI_OIDC_CLIENT_SECRET:-}"
prop_replace 'nifi.security.user.oidc.claim.identifying.user' "${NIFI_OIDC_CLAIM_IDENTIFYING_USER:-}"
prop_replace 'nifi.security.user.oidc.additional.scopes' "${NIFI_OIDC_ADDITIONAL_SCOPES:-}"
if [ -f "${NIFI_AUTH_DIR:-/opt/nifi/nificontrol-auth}/login-identity-providers.xml" ]; then
  cp "${NIFI_AUTH_DIR:-/opt/nifi/nificontrol-auth}/login-identity-providers.xml" "${NIFI_HOME}/conf/login-identity-providers.xml"
fi
# Private-CA trust for LDAPS / OIDC over HTTPS. The mounted PEM CA bundle is built into a
# PKCS12 truststore with the JDK keytool (cacerts itself is not writable by the nifi user).
# The truststores live under the image install directory, which the nifi user can write;
# conf/ is a mounted subPath where new files cannot be created.
auth_ca="${NIFI_AUTH_DIR:-/opt/nifi/nificontrol-auth}/auth-ca.crt"
if [ -f "${auth_ca}" ]; then
  keytool_bin="${JAVA_HOME}/bin/keytool"
  ts_dir="/opt/nifi/nifi-current/nificontrol-truststores"
  mkdir -p "${ts_dir}"
  if [ -n "${NIFI_OIDC_PRIVATE_CA:-}" ]; then
    # OIDC has no custom truststore path: add the CA to a writable copy of the server
    # truststore (a superset that still trusts the cluster/mTLS CA) and use strategy NIFI.
    oidc_ts="${ts_dir}/oidc-truststore.p12"
    cp "${NIFI_SECURITY_DIR}/truststore.p12" "${oidc_ts}"
    # cp inherits the read-only mode of the Secret-mounted source; keytool must rewrite it.
    chmod u+w "${oidc_ts}"
    "${keytool_bin}" -importcert -noprompt -alias nificontrol-oidc-ca -file "${auth_ca}" \
      -keystore "${oidc_ts}" -storetype PKCS12 -storepass "${NIFI_KEYSTORE_PASSWORD}"
    prop_replace 'nifi.security.truststore' "${oidc_ts}"
    prop_replace 'nifi.security.user.oidc.truststore.strategy' 'NIFI'
  else
    # LDAP: dedicated truststore referenced by the rendered login-identity-providers.xml.
    ldap_ts="${ts_dir}/ldap-truststore.p12"
    rm -f "${ldap_ts}"
    "${keytool_bin}" -importcert -noprompt -alias nificontrol-ldap-ca -file "${auth_ca}" \
      -keystore "${ldap_ts}" -storetype PKCS12 -storepass changeit
  fi
fi
if [ -n "${NIFI_SINGLE_USER_USERNAME:-}" ]; then
  "${NIFI_HOME}/bin/nifi.sh" set-single-user-credentials "${NIFI_SINGLE_USER_USERNAME}" "${NIFI_SINGLE_USER_PASSWORD}" >/dev/null 2>&1
fi
prop_replace 'nifi.remote.input.host' "${NIFI_REMOTE_INPUT_HOST:-${HOSTNAME}}"
prop_replace 'nifi.remote.input.socket.port' "${NIFI_REMOTE_INPUT_SOCKET_PORT:-10000}"
prop_replace 'nifi.remote.input.secure' 'true'
prop_replace 'nifi.cluster.protocol.is.secure' "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.is.node' "${NIFI_CLUSTER_IS_NODE:-false}"
prop_replace 'nifi.cluster.node.address' "${NIFI_CLUSTER_ADDRESS:-${HOSTNAME}}"
prop_replace 'nifi.cluster.node.protocol.port' "${NIFI_CLUSTER_NODE_PROTOCOL_PORT:-}"
prop_replace 'nifi.cluster.load.balance.host' "${NIFI_CLUSTER_LOAD_BALANCE_HOST:-}"
prop_replace 'nifi.cluster.load.balance.port' "${NIFI_CLUSTER_LOAD_BALANCE_PORT:-6342}"
prop_replace 'nifi.zookeeper.connect.string' "${NIFI_ZK_CONNECT_STRING:-}"
prop_replace 'nifi.zookeeper.root.node' "${NIFI_ZK_ROOT_NODE:-/nifi}"
if [ -n "${NIFI_ZK_CONNECT_STRING:-}" ]; then
  sed -i "s|<property name=\"Connect String\">[^<]*</property>|<property name=\"Connect String\">${NIFI_ZK_CONNECT_STRING}</property>|" "${NIFI_HOME}/conf/state-management.xml"
  sed -i "s|<property name=\"Root Node\">[^<]*</property>|<property name=\"Root Node\">${NIFI_ZK_ROOT_NODE:-/nifi}</property>|" "${NIFI_HOME}/conf/state-management.xml"
fi
prop_replace 'nifi.cluster.flow.election.max.wait.time' "${NIFI_ELECTION_MAX_WAIT:-2 mins}"
prop_replace 'nifi.cluster.flow.election.max.candidates' "${NIFI_ELECTION_MAX_CANDIDATES:-}"
cp "${NIFI_CONFIG_DIR}/authorizers.xml" "${NIFI_HOME}/conf/authorizers.xml"
` + applyConfigOverridesScript + `exec "${NIFI_HOME}/bin/nifi.sh" run`

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
	if err := r.reconcileManagedClusterExternalServices(ctx, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ExternalServiceReconcileFailed", err)
	}
	// Every managed node needs a stable sensitive properties key, including a single
	// standalone node: NiFi self-generates one into nifi.properties when it is blank, and
	// the start script re-blanks the property on the next restart, which loses the key
	// and strands the persisted encrypted flow.
	if err := r.reconcileSensitivePropsKeySecret(ctx, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "SensitivePropsKeyReconcileFailed", err)
	}
	resolvedOverrides, err := r.reconcileManagedClusterConfigOverrides(ctx, cluster)
	if err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ConfigOverridesInvalid", err)
	}
	resolvedAuth, err := resolveClusterAuthentication(ctx, r.Client, cluster)
	if err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "AuthenticationInvalid", err)
	}
	if err := r.reconcileManagedClusterAuthSecret(ctx, cluster, resolvedAuth); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "AuthenticationInvalid", err)
	}

	// Metrics are best-effort and never block cluster readiness; the MetricsReady condition
	// records a missing Prometheus Operator or ServiceMonitor apply problem.
	if err := r.reconcileManagedClusterMetrics(ctx, cluster, tlsMaterials); err != nil {
		recordEvent(r.Recorder, cluster, corev1.EventTypeWarning, "MetricsDegraded",
			fmt.Sprintf("Failed to reconcile metrics ServiceMonitor: %v", err))
	}

	endpoint := managedClusterEndpoint(cluster)

	// Determine the replica count the StatefulSet should currently have, gracefully
	// offloading NiFi nodes through the cluster API before their pods are removed.
	existing := &appsv1.StatefulSet{}
	if err := r.Get(ctx, types.NamespacedName{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}, existing); err != nil {
		if !apierrors.IsNotFound(err) {
			return r.managedClusterReconcileFailed(ctx, cluster, "StatefulSetReconcileFailed", err)
		}
		existing = nil
	}
	scaleDown, err := r.reconcileManagedClusterScaleDown(ctx, cluster, existing)
	if err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ScaleDownFailed", err)
	}

	statefulSet, err := r.reconcileManagedClusterStatefulSet(ctx, cluster, tlsMaterials, scaleDown.replicas, resolvedOverrides.checksum, resolvedAuth)
	if err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "StatefulSetReconcileFailed", err)
	}
	// Only after the pod template has dropped its reference is a stale overrides
	// ConfigMap safe to remove.
	if err := r.cleanupManagedClusterConfigOverrides(ctx, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "ConfigOverridesReconcileFailed", err)
	}
	if scaleDown.active {
		message := fmt.Sprintf("Gracefully offloading NiFi nodes during scale-down to %d replicas.", replicas)
		if cluster.Status.ScaleDown != nil && cluster.Status.ScaleDown.NodeAddress != "" {
			message = fmt.Sprintf("Scaling down: %s node %s.", cluster.Status.ScaleDown.Phase, cluster.Status.ScaleDown.NodeAddress)
		}
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, cluster.Status.Workload, "ScalingDown") {
			recordEvent(r.Recorder, cluster, corev1.EventTypeNormal, "ScalingDown", message)
			if err := markManagedClusterNotReady(ctx, r.Client, cluster, "ScalingDown", message, endpoint, cluster.Status.Workload); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	if err := r.reconcileManagedClusterPDB(ctx, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "PodDisruptionBudgetReconcileFailed", err)
	}
	if err := r.reconcileManagedClusterIngress(ctx, cluster); err != nil {
		return r.managedClusterReconcileFailed(ctx, cluster, "IngressReconcileFailed", err)
	}

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
	// A version upgrade is in progress when the StatefulSet is rolling a new revision.
	upgrading := statefulSet.Status.UpdateRevision != "" && statefulSet.Status.CurrentRevision != "" &&
		statefulSet.Status.UpdateRevision != statefulSet.Status.CurrentRevision
	if statefulSet.Status.ReadyReplicas < replicas || (upgrading && statefulSet.Status.UpdatedReplicas < replicas) {
		reason := "Provisioning"
		message := fmt.Sprintf("Waiting for NiFi StatefulSet replicas: %d/%d ready.", statefulSet.Status.ReadyReplicas, replicas)
		if upgrading {
			reason = "Upgrading"
			message = fmt.Sprintf("Upgrading NiFi nodes: %d/%d updated, %d/%d ready.", statefulSet.Status.UpdatedReplicas, replicas, statefulSet.Status.ReadyReplicas, replicas)
		}
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, workload, reason) {
			if err := markManagedClusterNotReady(ctx, r.Client, cluster, reason, message, endpoint, workload); err != nil {
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

	// A freshly secured NiFi does not seed the initial admin (the operator) with root-process-group
	// policies, so the operator grants them to itself before the cluster is considered ready —
	// otherwise every canvas reconcile against it would fail authorization. This is a no-op on
	// insecure and external clusters.
	if err := r.ensureOperatorCanvasAccess(ctx, cluster, endpoint); err != nil {
		message := fmt.Sprintf("The managed NiFi is reachable, but bootstrapping the operator's canvas authorization is not complete: %v", err)
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, workload, "AuthorizationBootstrapPending") {
			if statusErr := markManagedClusterNotReady(ctx, r.Client, cluster, "AuthorizationBootstrapPending", message, endpoint, workload); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}
	// Seed the configured admin identities with the administrative policy set so people
	// can log in and administer the cluster without a manual grant step.
	if err := r.ensureManagedAdminAccess(ctx, cluster, endpoint); err != nil {
		message := fmt.Sprintf("The managed NiFi is reachable, but granting the configured admin identities is not complete: %v", err)
		if managedClusterStatusNeedsUpdate(cluster, false, endpoint, workload, "AuthorizationBootstrapPending") {
			if statusErr := markManagedClusterNotReady(ctx, r.Client, cluster, "AuthorizationBootstrapPending", message, endpoint, workload); statusErr != nil {
				return ctrl.Result{}, statusErr
			}
		}
		return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
	}

	if managedClusterStatusNeedsUpdate(cluster, true, endpoint, workload, "ClusterReachable") {
		recordEvent(r.Recorder, cluster, corev1.EventTypeNormal, "Ready",
			fmt.Sprintf("NiFi cluster is ready (%d/%d nodes); API reachable at %s.", workload.ReadyReplicas, workload.Replicas, endpoint))
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
			// Mark the client-facing (non-headless) Service as the metrics scrape target so a
			// ServiceMonitor selects exactly one Service (both carry identical cluster labels,
			// and the headless Service also publishes not-ready pods).
			service.Labels[managedClusterMetricsServiceLabel] = "true"
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
			service.Spec.Ports = append(service.Spec.Ports,
				corev1.ServicePort{
					Name:       "cluster",
					Port:       managedClusterClusterProtocolPort(cluster),
					TargetPort: intstrFromString("cluster"),
					Protocol:   corev1.ProtocolTCP,
				},
				corev1.ServicePort{
					Name:       "load-balance",
					Port:       managedClusterLoadBalancePort(cluster),
					TargetPort: intstrFromString("load-balance"),
					Protocol:   corev1.ProtocolTCP,
				},
			)
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

func (r *NiFiClusterReconciler) reconcileManagedClusterStatefulSet(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, replicas int32, overridesChecksum string, auth *resolvedClusterAuth) (*appsv1.StatefulSet, error) {
	name := managedClusterResourceName(cluster)
	if err := r.recreateStatefulSetOnClaimChange(ctx, name, cluster.Namespace, desiredManagedClusterStatefulSetSpec(cluster, tls, overridesChecksum, auth).VolumeClaimTemplates); err != nil {
		return nil, err
	}
	statefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, statefulSet, func() error {
		if err := assertManagedClusterResource(statefulSet, cluster); err != nil {
			return err
		}
		statefulSet.Labels = managedClusterLabels(cluster)
		statefulSet.Annotations = managedClusterAnnotations(cluster)
		desired := desiredManagedClusterStatefulSetSpec(cluster, tls, overridesChecksum, auth)
		// The effective replica count steps down gradually during a graceful scale-down.
		desired.Replicas = ptr.To(replicas)
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

func desiredManagedClusterStatefulSetSpec(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, overridesChecksum string, auth *resolvedClusterAuth) appsv1.StatefulSetSpec {
	podLabels := managedClusterPodLabels(cluster)
	webPort := managedClusterHTTPPort(cluster)
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
		Env:             managedClusterEnvironment(cluster, tls, auth),
		Ports: []corev1.ContainerPort{
			{Name: "web", ContainerPort: webPort, Protocol: corev1.ProtocolTCP},
			{Name: "cluster", ContainerPort: managedClusterClusterProtocolPort(cluster), Protocol: corev1.ProtocolTCP},
			{Name: "s2s", ContainerPort: managedClusterRemoteInputPort(cluster), Protocol: corev1.ProtocolTCP},
			{Name: "load-balance", ContainerPort: managedClusterLoadBalancePort(cluster), Protocol: corev1.ProtocolTCP},
		},
		Resources:      cluster.Spec.Resources,
		StartupProbe:   managedClusterStartupProbe(tls),
		LivenessProbe:  managedClusterLivenessProbe(tls),
		ReadinessProbe: managedClusterReadinessProbe(tls),
		VolumeMounts:   managedClusterVolumeMounts(cluster.Spec.Storage, tls, hasConfigOverrides(cluster), managedClusterAuthVolumeSource(cluster, auth) != ""),
	}
	podSpec := corev1.PodSpec{
		SecurityContext: &corev1.PodSecurityContext{
			FSGroup:             ptr.To[int64](1000),
			FSGroupChangePolicy: ptr.To(corev1.FSGroupChangeOnRootMismatch),
		},
		InitContainers: []corev1.Container{managedClusterDataInitializer(cluster)},
		Containers:     []corev1.Container{container},
		Volumes:        managedClusterVolumes(cluster, tls, auth),
	}
	applyManagedClusterScheduling(&podSpec, cluster)
	annotations := managedClusterAnnotations(cluster)
	if tls != nil && tls.checksum != "" {
		annotations[managedTLSChecksumAnnotation] = tls.checksum
	}
	if overridesChecksum != "" {
		annotations[managedOverridesChecksumAnnotation] = overridesChecksum
	}
	if auth != nil && auth.checksum != "" {
		annotations[managedAuthChecksumAnnotation] = auth.checksum
	}
	spec := appsv1.StatefulSetSpec{
		ServiceName:          managedClusterHeadlessServiceName(cluster),
		Replicas:             ptr.To(managedClusterReplicas(cluster)),
		RevisionHistoryLimit: ptr.To[int32](10),
		MinReadySeconds:      managedClusterMinReadySeconds(cluster),
		PodManagementPolicy:  appsv1.ParallelPodManagement,
		Selector:             &metav1.LabelSelector{MatchLabels: podLabels},
		UpdateStrategy:       managedClusterUpdateStrategy(cluster),
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: annotations,
			},
			Spec: podSpec,
		},
	}
	applyPodCustomization(cluster, &spec.Template)
	if managedClusterStorageEnabled(cluster) {
		spec.VolumeClaimTemplates = nodeVolumeClaims(cluster.Spec.Storage, managedClusterLabels(cluster), managedClusterAnnotations(cluster))
	}
	return spec
}

// managedClusterVolumes returns the pod volumes, adding read-only mounts for the TLS
// Secret and operator-rendered config when internal TLS is enabled.
func managedClusterVolumes(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, auth *resolvedClusterAuth) []corev1.Volume {
	return nodeVolumes(managedClusterStorageEnabled(cluster), tls, managedClusterOverridesVolumeSource(cluster), managedClusterAuthVolumeSource(cluster, auth))
}

// nodeVolumes returns the pod volumes for a node pool. When persistent storage is disabled
// the data directory is an emptyDir; the TLS Secret, config, and configuration-overrides
// volumes are added when set.
func nodeVolumes(storageEnabled bool, tls *clusterTLSMaterials, overridesSecret, authSecret string) []corev1.Volume {
	volumes := []corev1.Volume{}
	if !storageEnabled {
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
	if overridesSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: managedOverridesVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  overridesSecret,
				DefaultMode: ptr.To[int32](0o440),
			}},
		})
	}
	if authSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: managedAuthVolume,
			VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{
				SecretName:  authSecret,
				DefaultMode: ptr.To[int32](0o440),
			}},
		})
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
	return nodeDataInitializer(managedClusterImage(cluster), managedClusterImagePullPolicy(cluster))
}

func nodeDataInitializer(image string, pullPolicy corev1.PullPolicy) corev1.Container {
	return corev1.Container{
		Name:            "initialize-data",
		Image:           image,
		ImagePullPolicy: pullPolicy,
		Command: []string{
			"/bin/bash",
			"-ec",
			// The image-default copies are refreshed on every start (the init container sees the
			// image's pristine conf, unshadowed by the data mount) so removed configuration
			// overrides can be restored to the running image's shipped values.
			"mkdir -p /mnt/data/{conf,database_repository,flowfile_repository,content_repository,provenance_repository,state}; if [ ! -f /mnt/data/conf/nifi.properties ]; then cp -a /opt/nifi/nifi-current/conf/. /mnt/data/conf/; fi; cp /opt/nifi/nifi-current/conf/nifi.properties /mnt/data/conf/nifi.properties.image-default; cp /opt/nifi/nifi-current/conf/bootstrap.conf /mnt/data/conf/bootstrap.conf.image-default; cp /opt/nifi/nifi-current/conf/logback.xml /mnt/data/conf/logback.xml.image-default",
		},
		VolumeMounts: []corev1.VolumeMount{{Name: managedDataVolume, MountPath: "/mnt/data"}},
	}
}

func managedClusterVolumeMounts(storage nifiv1alpha1.NiFiClusterStorageSpec, tls *clusterTLSMaterials, overrides, auth bool) []corev1.VolumeMount {
	dedicated := map[string]string{}
	for _, binding := range repositoryVolumeBindings(storage) {
		dedicated[binding.directory] = binding.claimName
	}
	mounts := make([]corev1.VolumeMount, 0, len(managedDataDirectories)+len(dedicated)+3)
	for _, directory := range managedDataDirectories {
		if claim, ok := dedicated[directory]; ok {
			// The repository lives on its own claim; the StatefulSet controller injects a
			// pod volume named after the claim template.
			mounts = append(mounts, corev1.VolumeMount{
				Name:      claim,
				MountPath: "/opt/nifi/nifi-current/" + directory,
			})
			continue
		}
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
	if overrides {
		mounts = append(mounts, corev1.VolumeMount{Name: managedOverridesVolume, MountPath: managedOverridesDir, ReadOnly: true})
	}
	if auth {
		mounts = append(mounts, corev1.VolumeMount{Name: managedAuthVolume, MountPath: managedAuthDir, ReadOnly: true})
	}
	return mounts
}

func managedClusterVolumeClaim(cluster *nifiv1alpha1.NiFiCluster) corev1.PersistentVolumeClaim {
	return nodeVolumeClaim(cluster.Spec.Storage, managedClusterLabels(cluster), managedClusterAnnotations(cluster))
}

func nodeVolumeClaim(storage nifiv1alpha1.NiFiClusterStorageSpec, labels, annotations map[string]string) corev1.PersistentVolumeClaim {
	accessModes := storage.AccessModes
	if len(accessModes) == 0 {
		accessModes = []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
	}
	size := resource.MustParse("10Gi")
	if !storage.Size.IsZero() {
		size = storage.Size.DeepCopy()
	}
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        managedDataVolume,
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      accessModes,
			StorageClassName: storage.StorageClassName,
			VolumeMode:       ptr.To(corev1.PersistentVolumeFilesystem),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: size},
			},
		},
	}
}

func managedClusterEnvironment(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, auth *resolvedClusterAuth) []corev1.EnvVar {
	return nodeEnvironment(cluster, tls, managedClusterHeapInitial(cluster), managedClusterHeapMax(cluster), cluster.Spec.AdditionalEnv, managedClusterReplicas(cluster) > 1, auth)
}

// nodeEnvironment builds the container environment shared by the cluster's primary pool and
// any NiFiNodeGroup pool. Heap, additional env, and whether the node joins a cluster vary by
// pool; ZooKeeper, the sensitive-properties key, proxy host, and the cluster address are
// shared from the parent cluster so every pool's nodes are peers in one NiFi cluster.
func nodeEnvironment(cluster *nifiv1alpha1.NiFiCluster, tls *clusterTLSMaterials, heapInitial, heapMax string, additionalEnv []corev1.EnvVar, clustered bool, auth *resolvedClusterAuth) []corev1.EnvVar {
	environment := []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		{Name: "NIFI_JVM_HEAP_INIT", Value: heapInitial},
		{Name: "NIFI_JVM_HEAP_MAX", Value: heapMax},
		// Where the start script looks for mounted configuration overrides; the mount only
		// exists when spec.configOverrides has entries, and the script no-ops without it.
		{Name: "NIFI_OVERRIDES_DIR", Value: managedOverridesDir},
		// Every node — standalone included — boots with the operator-generated sensitive
		// properties key (see reconcileSensitivePropsKeySecret): when the property is left
		// blank NiFi generates its own into nifi.properties, and the start script would
		// blank it again on the next restart, stranding the persisted encrypted flow.
		{Name: "NIFI_SENSITIVE_PROPS_KEY", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: managedClusterSensitivePropsSecretName(cluster)},
			Key:                  sensitivePropsKeyKey,
		}}},
	}
	// In a cluster, NiFi advertises the node's web host as its API address in
	// /controller/cluster, which the operator matches when offloading a node on scale-down.
	// Bind to (and advertise) the pod's stable headless DNS name so each node is uniquely
	// addressable; a standalone node keeps binding 0.0.0.0. The name resolves to the pod IP,
	// so the kubelet httpGet probe and Service routing still reach it.
	webHTTPHost := "0.0.0.0"
	webHTTPSHost := "0.0.0.0"
	advertisedHost := ""
	if clustered && cluster.Spec.Coordination != nil {
		advertisedHost = fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc", managedClusterHeadlessServiceName(cluster))
		webHTTPHost = advertisedHost
		webHTTPSHost = advertisedHost
	}
	if tls != nil {
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_WEB_HTTPS_HOST", Value: webHTTPSHost},
			corev1.EnvVar{Name: "NIFI_WEB_HTTPS_PORT", Value: strconv.Itoa(int(tls.httpsPort))},
			corev1.EnvVar{Name: "NIFI_WEB_ADVERTISED_HOST", Value: advertisedHost},
			corev1.EnvVar{Name: "NIFI_SECURITY_DIR", Value: managedTLSSecurityDir},
			corev1.EnvVar{Name: "NIFI_CONFIG_DIR", Value: managedTLSConfigDir},
			corev1.EnvVar{Name: "NIFI_KEYSTORE_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: tls.passwordSecretName},
				Key:                  tls.passwordSecretKey,
			}}},
		)
		// User authentication (single-user, LDAP, OIDC) only exists over HTTPS; the mode's
		// login provider, credentials, and OIDC settings arrive through the environment.
		if auth != nil {
			environment = append(environment, corev1.EnvVar{Name: "NIFI_AUTH_DIR", Value: managedAuthDir})
			environment = append(environment, auth.env...)
		}
	} else {
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_WEB_HTTP_HOST", Value: webHTTPHost},
			corev1.EnvVar{Name: "NIFI_WEB_HTTP_PORT", Value: strconv.Itoa(int(managedClusterHTTPPort(cluster)))},
		)
	}
	// Site-to-site and load-balance ports apply to every node regardless of TLS mode.
	environment = append(environment,
		corev1.EnvVar{Name: "NIFI_REMOTE_INPUT_SOCKET_PORT", Value: strconv.Itoa(int(managedClusterRemoteInputPort(cluster)))},
		corev1.EnvVar{Name: "NIFI_CLUSTER_LOAD_BALANCE_PORT", Value: strconv.Itoa(int(managedClusterLoadBalancePort(cluster)))},
	)
	// Allowed proxy host and context path (TLS Service DNS names plus any Ingress host).
	environment = append(environment,
		corev1.EnvVar{Name: "NIFI_WEB_PROXY_HOST", Value: managedClusterProxyHost(cluster, tls)},
		corev1.EnvVar{Name: "NIFI_WEB_PROXY_CONTEXT_PATH", Value: managedClusterProxyContextPath(cluster)},
	)
	if clustered && cluster.Spec.Coordination != nil {
		coordination := cluster.Spec.Coordination
		environment = append(environment,
			corev1.EnvVar{Name: "NIFI_CLUSTER_IS_NODE", Value: "true"},
			corev1.EnvVar{Name: "NIFI_CLUSTER_ADDRESS", Value: fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc", managedClusterHeadlessServiceName(cluster))},
			corev1.EnvVar{Name: "NIFI_CLUSTER_NODE_PROTOCOL_PORT", Value: strconv.Itoa(int(managedClusterClusterProtocolPort(cluster)))},
			corev1.EnvVar{Name: "NIFI_ZK_CONNECT_STRING", Value: coordination.ZooKeeperConnectString},
			corev1.EnvVar{Name: "NIFI_ZK_ROOT_NODE", Value: managedClusterZooKeeperRootNode(cluster)},
			corev1.EnvVar{Name: "NIFI_ELECTION_MAX_WAIT", Value: managedClusterElectionMaxWait(cluster)},
			corev1.EnvVar{Name: "NIFI_ELECTION_MAX_CANDIDATES", Value: strconv.Itoa(int(managedClusterReplicas(cluster)))},
		)
	} else {
		environment = append(environment, corev1.EnvVar{Name: "NIFI_CLUSTER_IS_NODE", Value: "false"})
	}
	return mergeEnvironment(environment, additionalEnv)
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
		&policyv1.PodDisruptionBudget{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}},
		&networkingv1.Ingress{ObjectMeta: metav1.ObjectMeta{Name: managedClusterResourceName(cluster), Namespace: cluster.Namespace}},
	}
	for _, object := range resources {
		if err := r.deleteManagedClusterResource(ctx, cluster, object); err != nil {
			return ctrl.Result{RequeueAfter: 5 * time.Second}, err
		}
	}
	// External Services carry no owner reference (so Orphan leaves them), so remove them
	// explicitly under the Delete policy.
	if err := r.pruneManagedClusterExternalServices(ctx, cluster, nil); err != nil {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, err
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
		recordEvent(r.Recorder, cluster, corev1.EventTypeWarning, reason, message)
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
	setManagedClusterScaleStatus(cluster, workload)
	cluster.Status.Sync.LastError = ""
	return c.Status().Update(ctx, cluster)
}

// setManagedClusterScaleStatus populates the scale subresource status fields.
func setManagedClusterScaleStatus(cluster *nifiv1alpha1.NiFiCluster, workload *nifiv1alpha1.NiFiClusterWorkloadStatus) {
	cluster.Status.Selector = managedClusterScaleSelector(cluster)
	if workload != nil {
		cluster.Status.Replicas = workload.ReadyReplicas
	}
}

func markManagedClusterNotReady(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster, reason string, message string, endpoint string, workload *nifiv1alpha1.NiFiClusterWorkloadStatus) error {
	cluster.Status.CommonStatus.MarkNotReady(cluster.Generation, reason, message)
	cluster.Status.Dependencies.Ready = true
	cluster.Status.Dependencies.WaitingFor = nil
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionClusterReachable, metav1.ConditionFalse, reason, message, cluster.Generation)
	cluster.Status.Endpoint = endpoint
	cluster.Status.Workload = workload
	setManagedClusterScaleStatus(cluster, workload)
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
	// Keep the scale subresource fields fresh (selector is static; replicas tracks readiness).
	if cluster.Status.Selector != managedClusterScaleSelector(cluster) {
		return true
	}
	if workload != nil && cluster.Status.Replicas != workload.ReadyReplicas {
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
	podLabels := managedClusterLabels(cluster)
	podLabels["app.kubernetes.io/component"] = "nifi-node"
	return podLabels
}

// managedClusterScaleSelector returns the serialized label selector matching the managed
// NiFi pods, used for the NiFiCluster scale subresource.
func managedClusterScaleSelector(cluster *nifiv1alpha1.NiFiCluster) string {
	return labels.SelectorFromSet(managedClusterPodLabels(cluster)).String()
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

// managedClusterHTTPPort is the plaintext web port NiFi binds in non-TLS mode.
func managedClusterHTTPPort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if cluster.Spec.Ports != nil && cluster.Spec.Ports.HTTP > 0 {
		return cluster.Spec.Ports.HTTP
	}
	return defaultNiFiWebPort
}

// managedClusterClusterProtocolPort is the node-to-node cluster protocol port.
func managedClusterClusterProtocolPort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if cluster.Spec.Ports != nil && cluster.Spec.Ports.ClusterProtocol > 0 {
		return cluster.Spec.Ports.ClusterProtocol
	}
	return defaultClusterPort
}

// managedClusterRemoteInputPort is the site-to-site raw socket port.
func managedClusterRemoteInputPort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if cluster.Spec.Ports != nil && cluster.Spec.Ports.RemoteInput > 0 {
		return cluster.Spec.Ports.RemoteInput
	}
	return defaultRemoteInputPort
}

// managedClusterLoadBalancePort is the cluster connection load-balance port.
func managedClusterLoadBalancePort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if cluster.Spec.Ports != nil && cluster.Spec.Ports.LoadBalance > 0 {
		return cluster.Spec.Ports.LoadBalance
	}
	return defaultLoadBalancePort
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
