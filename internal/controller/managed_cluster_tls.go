package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/certmanager"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	// Container paths for mounted TLS materials and operator-rendered config.
	managedTLSSecurityDir = "/opt/nifi/nificontrol-tls"
	managedTLSConfigDir   = "/opt/nifi/nificontrol-config"
	managedTLSVolume      = "nificontrol-tls"
	managedTLSConfigVol   = "nificontrol-config"

	// Keys expected in PKCS12-bearing Secrets.
	tlsKeystoreKey      = "keystore.p12"
	tlsTruststoreKey    = "truststore.p12"
	tlsCAKey            = "ca.crt"
	tlsCertKey          = "tls.crt"
	tlsKeyKey           = "tls.key"
	keystorePasswordKey = "password"

	defaultHTTPSPort = int32(8443)

	// Annotation that rolls the StatefulSet when TLS materials or config change.
	managedTLSChecksumAnnotation = "nifi.controlnifi.io/tls-checksum"
)

var requiredManagedTLSSecretKeys = []string{tlsKeystoreKey, tlsTruststoreKey, tlsCertKey, tlsKeyKey}

// clusterTLSMaterials is the resolved internal-TLS state used by the StatefulSet and the
// operator's mTLS REST client. A nil value means TLS is disabled for the cluster.
type clusterTLSMaterials struct {
	httpsPort            int32
	serverSecretName     string
	clientSecretName     string
	configMapName        string
	passwordSecretName   string
	passwordSecretKey    string
	initialAdminIdentity string
	nodeIdentity         string
	proxyHosts           string
	checksum             string
}

// internalTLSEnabled reports whether the cluster requests operator-managed HTTPS/mTLS.
func internalTLSEnabled(cluster *nifiv1alpha1.NiFiCluster) bool {
	return cluster.Spec.InternalTLS != nil && cluster.Spec.InternalTLS.Enabled
}

func managedClusterHTTPSPort(cluster *nifiv1alpha1.NiFiCluster) int32 {
	if internalTLSEnabled(cluster) && cluster.Spec.InternalTLS.HTTPSPort > 0 {
		return cluster.Spec.InternalTLS.HTTPSPort
	}
	return defaultHTTPSPort
}

// reconcileManagedClusterTLS provisions cert-manager resources and the operator-rendered
// config for an Internal cluster with internalTLS enabled. It returns the resolved
// materials once the PKCS12 and PEM certificate material are available. A nil
// materials result with a nil error means TLS is not yet ready; the caller has already
// recorded status and should requeue.
func (r *NiFiClusterReconciler) reconcileManagedClusterTLS(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) (*clusterTLSMaterials, bool, error) {
	tls := cluster.Spec.InternalTLS
	plan := resolveTLSPlan(cluster)

	if tls.External != nil {
		return r.reconcileExternalTLS(ctx, cluster, plan)
	}

	// cert-manager modes: optionally build a two-stage self-signed CA chain, then the
	// keystore password Secret, then the server and operator-client leaf certificates.
	issuer, err := r.reconcileTLSIssuers(ctx, cluster, plan)
	if err != nil {
		if certmanager.IsCRDNotInstalled(err) {
			return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "CertManagerMissing",
				"cert-manager CRDs are not installed; install cert-manager before enabling internalTLS.")
		}
		return nil, false, err
	}

	if err := r.reconcileKeystorePasswordSecret(ctx, cluster, plan.passwordSecretName); err != nil {
		return nil, false, err
	}

	if err := r.reconcileServerCertificate(ctx, cluster, plan, issuer); err != nil {
		if certmanager.IsCRDNotInstalled(err) {
			return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "CertManagerMissing",
				"cert-manager CRDs are not installed; install cert-manager before enabling internalTLS.")
		}
		return nil, false, err
	}
	if err := r.reconcileClientCertificate(ctx, cluster, plan, issuer); err != nil {
		if certmanager.IsCRDNotInstalled(err) {
			return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "CertManagerMissing",
				"cert-manager CRDs are not installed; install cert-manager before enabling internalTLS.")
		}
		return nil, false, err
	}

	if err := r.reconcileTLSConfigMap(ctx, cluster, plan); err != nil {
		return nil, false, err
	}

	// Require the issued materials before declaring TLS ready. A not-yet-created Secret
	// means cert-manager has not finished issuing, which is pending rather than an error.
	serverData, serverMissing, err := r.tlsSecretMaterials(ctx, cluster.Namespace, plan.serverSecretName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			serverMissing = requiredManagedTLSSecretKeys
		} else {
			return nil, false, err
		}
	}
	_, clientMissing, err := r.tlsSecretMaterials(ctx, cluster.Namespace, plan.clientSecretName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			clientMissing = requiredManagedTLSSecretKeys
		} else {
			return nil, false, err
		}
	}
	if len(serverMissing) > 0 || len(clientMissing) > 0 {
		return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "TLSPending",
			tlsMissingMessage("Waiting for cert-manager to issue TLS materials.", plan, serverMissing, clientMissing))
	}

	materials := plan.materials()
	materials.checksum = tlsChecksum(serverData, r.tlsConfigChecksumInput(plan))
	if err := r.markManagedClusterTLSReady(ctx, cluster, plan); err != nil {
		return nil, false, err
	}
	return materials, true, nil
}

// reconcileExternalTLS validates externally supplied PKCS12 Secrets and renders config.
func (r *NiFiClusterReconciler) reconcileExternalTLS(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan) (*clusterTLSMaterials, bool, error) {
	if err := r.reconcileTLSConfigMap(ctx, cluster, plan); err != nil {
		return nil, false, err
	}
	serverData, serverMissing, err := r.tlsSecretMaterials(ctx, cluster.Namespace, plan.serverSecretName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "TLSSecretMissing",
				fmt.Sprintf("Externally supplied server TLS Secret %q was not found.", plan.serverSecretName))
		}
		return nil, false, err
	}
	_, clientMissing, err := r.tlsSecretMaterials(ctx, cluster.Namespace, plan.clientSecretName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "TLSSecretMissing",
				fmt.Sprintf("Externally supplied client TLS Secret %q was not found.", plan.clientSecretName))
		}
		return nil, false, err
	}
	if len(serverMissing) > 0 || len(clientMissing) > 0 {
		return nil, false, r.markManagedClusterTLSNotReady(ctx, cluster, "TLSPending",
			tlsMissingMessage("Externally supplied TLS Secrets are incomplete.", plan, serverMissing, clientMissing))
	}
	materials := plan.materials()
	materials.checksum = tlsChecksum(serverData, r.tlsConfigChecksumInput(plan))
	if err := r.markManagedClusterTLSReady(ctx, cluster, plan); err != nil {
		return nil, false, err
	}
	return materials, true, nil
}

// reconcileTLSIssuers builds the self-signed CA chain when requested and returns the
// issuerRef that signs the leaf certificates.
func (r *NiFiClusterReconciler) reconcileTLSIssuers(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan) (certmanager.IssuerRef, error) {
	tls := cluster.Spec.InternalTLS
	if tls.IssuerRef != nil {
		kind := tls.IssuerRef.Kind
		if kind == "" {
			kind = certmanager.KindIssuer
		}
		group := tls.IssuerRef.Group
		if group == "" {
			group = certmanager.GroupName
		}
		return certmanager.IssuerRef{Name: tls.IssuerRef.Name, Kind: kind, Group: group}, nil
	}

	// Self-signed two-stage chain: a SelfSigned Issuer signs a CA Certificate, which
	// backs a CA Issuer used for the leaf certificates. Leaf certificates are never
	// signed directly by a SelfSigned issuer.
	selfSigned, err := certmanager.NewIssuer(plan.selfSignedIssuerName, cluster.Namespace, certmanager.IssuerSpec{
		SelfSigned: &certmanager.SelfSignedIssuer{},
	})
	if err != nil {
		return certmanager.IssuerRef{}, err
	}
	if err := r.applyCertManagerObject(ctx, cluster, selfSigned); err != nil {
		return certmanager.IssuerRef{}, err
	}

	caCert, err := certmanager.NewCertificate(plan.caCertName, cluster.Namespace, certmanager.CertificateSpec{
		SecretName: plan.caSecretName,
		CommonName: plan.caCommonName,
		Duration:   plan.caDuration,
		IsCA:       true,
		PrivateKey: defaultPrivateKey(),
		IssuerRef:  certmanager.IssuerRef{Name: plan.selfSignedIssuerName, Kind: certmanager.KindIssuer, Group: certmanager.GroupName},
	})
	if err != nil {
		return certmanager.IssuerRef{}, err
	}
	if err := r.applyCertManagerObject(ctx, cluster, caCert); err != nil {
		return certmanager.IssuerRef{}, err
	}

	caIssuer, err := certmanager.NewIssuer(plan.caIssuerName, cluster.Namespace, certmanager.IssuerSpec{
		CA: &certmanager.CAIssuer{SecretName: plan.caSecretName},
	})
	if err != nil {
		return certmanager.IssuerRef{}, err
	}
	if err := r.applyCertManagerObject(ctx, cluster, caIssuer); err != nil {
		return certmanager.IssuerRef{}, err
	}
	return certmanager.IssuerRef{Name: plan.caIssuerName, Kind: certmanager.KindIssuer, Group: certmanager.GroupName}, nil
}

func (r *NiFiClusterReconciler) reconcileServerCertificate(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan, issuer certmanager.IssuerRef) error {
	cert, err := certmanager.NewCertificate(plan.serverCertName, cluster.Namespace, certmanager.CertificateSpec{
		SecretName:  plan.serverSecretName,
		CommonName:  plan.nodeCommonName,
		Duration:    plan.leafDuration,
		RenewBefore: plan.renewBefore,
		DNSNames:    plan.serverDNSNames,
		IPAddresses: []string{"127.0.0.1"},
		// Server/node certificates need both serverAuth (NiFi web + cluster server) and
		// clientAuth (each node acts as a client to its peers during clustering).
		Usages:     []string{certmanager.UsageServerAuth, certmanager.UsageClientAuth, certmanager.UsageDigitalSignature, certmanager.UsageKeyEncipherment},
		IssuerRef:  issuer,
		PrivateKey: defaultPrivateKey(),
		Keystores:  pkcs12Keystore(plan.passwordSecretName),
	})
	if err != nil {
		return err
	}
	return r.applyCertManagerObject(ctx, cluster, cert)
}

func (r *NiFiClusterReconciler) reconcileClientCertificate(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan, issuer certmanager.IssuerRef) error {
	cert, err := certmanager.NewCertificate(plan.clientCertName, cluster.Namespace, certmanager.CertificateSpec{
		SecretName:  plan.clientSecretName,
		CommonName:  plan.operatorCommonName,
		Duration:    plan.leafDuration,
		RenewBefore: plan.renewBefore,
		Usages:      []string{certmanager.UsageClientAuth, certmanager.UsageDigitalSignature, certmanager.UsageKeyEncipherment},
		IssuerRef:   issuer,
		PrivateKey:  defaultPrivateKey(),
		Keystores:   pkcs12Keystore(plan.passwordSecretName),
	})
	if err != nil {
		return err
	}
	return r.applyCertManagerObject(ctx, cluster, cert)
}

// applyCertManagerObject creates or updates an unstructured cert-manager object owned by
// the NiFiCluster.
func (r *NiFiClusterReconciler) applyCertManagerObject(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, desired *unstructured.Unstructured) error {
	existing := certmanager.New(desired.GroupVersionKind())
	existing.SetName(desired.GetName())
	existing.SetNamespace(desired.GetNamespace())
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, existing, func() error {
		existing.Object["spec"] = desired.Object["spec"]
		return controllerutil.SetControllerReference(cluster, existing, r.Scheme)
	})
	return err
}

func (r *NiFiClusterReconciler) reconcileKeystorePasswordSecret(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, name string) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		if len(secret.Data[keystorePasswordKey]) == 0 {
			password, genErr := generatePassword()
			if genErr != nil {
				return genErr
			}
			secret.Data[keystorePasswordKey] = []byte(password)
		}
		secret.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(cluster, secret, r.Scheme)
	})
	return err
}

func (r *NiFiClusterReconciler) reconcileTLSConfigMap(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan) error {
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: plan.configMapName, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = managedClusterLabels(cluster)
		configMap.Data = map[string]string{
			"authorizers.xml":  renderAuthorizersXML(plan.initialAdminIdentity, plan.nodeIdentity),
			"tls-readiness.sh": renderTLSReadinessScript(),
		}
		return controllerutil.SetControllerReference(cluster, configMap, r.Scheme)
	})
	return err
}

// tlsSecretMaterials fetches a PKCS12 Secret and reports missing required keys. ca.crt is
// intentionally optional: when present it is used to pin trust, otherwise the operator and
// readiness probe fall back to the system trust store.
func (r *NiFiClusterReconciler) tlsSecretMaterials(ctx context.Context, namespace, name string) (map[string][]byte, []string, error) {
	secret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		return nil, nil, err
	}
	return secret.Data, missingSecretKeys(secret.Data, requiredManagedTLSSecretKeys), nil
}

func missingSecretKeys(data map[string][]byte, required []string) []string {
	missing := []string{}
	for _, key := range required {
		if len(data[key]) == 0 {
			missing = append(missing, key)
		}
	}
	return missing
}

func tlsMissingMessage(prefix string, plan tlsPlan, serverMissing, clientMissing []string) string {
	parts := []string{prefix}
	if len(serverMissing) > 0 {
		parts = append(parts, fmt.Sprintf("server Secret %q missing %s", plan.serverSecretName, strings.Join(serverMissing, ", ")))
	}
	if len(clientMissing) > 0 {
		parts = append(parts, fmt.Sprintf("client Secret %q missing %s", plan.clientSecretName, strings.Join(clientMissing, ", ")))
	}
	parts = append(parts, "ca.crt is optional and is used for explicit trust when present.")
	return strings.Join(parts, " ")
}

func (r *NiFiClusterReconciler) tlsConfigChecksumInput(plan tlsPlan) string {
	return renderAuthorizersXML(plan.initialAdminIdentity, plan.nodeIdentity) + renderTLSReadinessScript() + plan.proxyHosts
}

func (r *NiFiClusterReconciler) markManagedClusterTLSReady(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, plan tlsPlan) error {
	status := &nifiv1alpha1.NiFiClusterTLSStatus{
		Mode:                 plan.mode,
		IssuerName:           plan.statusIssuerName,
		IssuerKind:           plan.statusIssuerKind,
		ServerSecretName:     plan.serverSecretName,
		ClientSecretName:     plan.clientSecretName,
		InitialAdminIdentity: plan.initialAdminIdentity,
		NodeIdentity:         plan.nodeIdentity,
		Ready:                true,
	}
	if tlsStatusEqual(cluster.Status.TLS, status) && conditionMatches(cluster.Status.Conditions, nifiv1alpha1.ConditionTLSReady, metav1.ConditionTrue, "TLSReady") {
		return nil
	}
	cluster.Status.TLS = status
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionTLSReady, metav1.ConditionTrue, "TLSReady", "Internal TLS materials are available.", cluster.Generation)
	return r.Status().Update(ctx, cluster)
}

func (r *NiFiClusterReconciler) markManagedClusterTLSNotReady(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, reason, message string) error {
	if cluster.Status.TLS == nil {
		cluster.Status.TLS = &nifiv1alpha1.NiFiClusterTLSStatus{}
	}
	if cluster.Status.TLS.Ready == false && conditionMatches(cluster.Status.Conditions, nifiv1alpha1.ConditionTLSReady, metav1.ConditionFalse, reason) &&
		cluster.Status.ObservedGeneration == cluster.Generation {
		return nil
	}
	cluster.Status.TLS.Ready = false
	cluster.Status.CommonStatus.MarkNotReady(cluster.Generation, reason, message)
	cluster.Status.CommonStatus.SetCondition(nifiv1alpha1.ConditionTLSReady, metav1.ConditionFalse, reason, message, cluster.Generation)
	cluster.Status.Sync.LastError = message
	return r.Status().Update(ctx, cluster)
}

func conditionMatches(conditions []metav1.Condition, conditionType nifiv1alpha1.ConditionType, status metav1.ConditionStatus, reason string) bool {
	for _, condition := range conditions {
		if condition.Type == string(conditionType) {
			return condition.Status == status && condition.Reason == reason
		}
	}
	return false
}

func tlsStatusEqual(left, right *nifiv1alpha1.NiFiClusterTLSStatus) bool {
	if left == nil || right == nil {
		return left == right
	}
	return *left == *right
}

// requestsForManagedClusterSecret maps a Secret to its owning NiFiCluster so that TLS
// material rotation re-reconciles the cluster (and rolls pods through the checksum).
func managedClusterSecretOwner(secret *corev1.Secret) (string, bool) {
	for _, owner := range secret.OwnerReferences {
		if owner.Kind == "NiFiCluster" && strings.HasPrefix(owner.APIVersion, nifiv1alpha1.GroupVersion.Group) {
			return owner.Name, true
		}
	}
	return "", false
}

// --- plan -------------------------------------------------------------------

type tlsPlan struct {
	mode             string
	httpsPort        int32
	statusIssuerName string
	statusIssuerKind string

	selfSignedIssuerName string
	caIssuerName         string
	caCertName           string
	caSecretName         string
	caCommonName         string
	caDuration           string

	serverCertName     string
	serverSecretName   string
	clientCertName     string
	clientSecretName   string
	configMapName      string
	passwordSecretName string
	passwordSecretKey  string

	nodeCommonName       string
	operatorCommonName   string
	initialAdminIdentity string
	nodeIdentity         string

	serverDNSNames []string
	leafDuration   string
	renewBefore    string
	proxyHosts     string
}

func (p tlsPlan) materials() *clusterTLSMaterials {
	return &clusterTLSMaterials{
		httpsPort:            p.httpsPort,
		serverSecretName:     p.serverSecretName,
		clientSecretName:     p.clientSecretName,
		configMapName:        p.configMapName,
		passwordSecretName:   p.passwordSecretName,
		passwordSecretKey:    p.passwordSecretKey,
		initialAdminIdentity: p.initialAdminIdentity,
		nodeIdentity:         p.nodeIdentity,
		proxyHosts:           p.proxyHosts,
	}
}

func resolveTLSPlan(cluster *nifiv1alpha1.NiFiCluster) tlsPlan {
	tls := cluster.Spec.InternalTLS
	plan := tlsPlan{
		httpsPort:            managedClusterHTTPSPort(cluster),
		selfSignedIssuerName: boundedManagedName(cluster.Name, "nifi-selfsigned"),
		caIssuerName:         boundedManagedName(cluster.Name, "nifi-ca-issuer"),
		caCertName:           boundedManagedName(cluster.Name, "nifi-ca"),
		caSecretName:         boundedManagedName(cluster.Name, "nifi-ca-tls"),
		caCommonName:         boundedCommonName(cluster.Name, "ca"),
		caDuration:           "8760h",
		serverCertName:       boundedManagedName(cluster.Name, "nifi-server"),
		serverSecretName:     boundedManagedName(cluster.Name, "nifi-server-tls"),
		clientCertName:       boundedManagedName(cluster.Name, "nifi-operator"),
		clientSecretName:     boundedManagedName(cluster.Name, "nifi-operator-tls"),
		configMapName:        boundedManagedName(cluster.Name, "nifi-tls-config"),
		passwordSecretName:   boundedManagedName(cluster.Name, "nifi-keystore-pw"),
		passwordSecretKey:    keystorePasswordKey,
		leafDuration:         "2160h",
		renewBefore:          "360h",
	}
	plan.serverDNSNames = managedClusterServerDNSNames(cluster)
	plan.proxyHosts = managedClusterProxyHosts(cluster)

	if certificate := tls.Certificate; certificate != nil {
		if certificate.Duration != "" {
			plan.leafDuration = certificate.Duration
		}
		if certificate.RenewBefore != "" {
			plan.renewBefore = certificate.RenewBefore
		}
		if len(certificate.AdditionalServerSANs) > 0 {
			plan.serverDNSNames = append(plan.serverDNSNames, certificate.AdditionalServerSANs...)
		}
	}

	switch {
	case tls.External != nil:
		plan.mode = "External"
		plan.serverSecretName = tls.External.ServerSecretName
		plan.clientSecretName = tls.External.ClientSecretName
		plan.initialAdminIdentity = tls.External.InitialAdminIdentity
		plan.nodeIdentity = tls.External.NodeIdentity
		if tls.External.KeystorePasswordSecretRef != nil {
			plan.passwordSecretName = tls.External.KeystorePasswordSecretRef.Name
			plan.passwordSecretKey = tls.External.KeystorePasswordSecretRef.Key
		}
	case tls.IssuerRef != nil:
		plan.mode = "CertManagerIssuer"
		kind := tls.IssuerRef.Kind
		if kind == "" {
			kind = certmanager.KindIssuer
		}
		plan.statusIssuerName = tls.IssuerRef.Name
		plan.statusIssuerKind = kind
		plan.applyDerivedIdentities(cluster)
	default:
		plan.mode = "SelfSigned"
		plan.statusIssuerName = plan.caIssuerName
		plan.statusIssuerKind = certmanager.KindIssuer
		if tls.SelfSigned != nil {
			if tls.SelfSigned.CACommonName != "" {
				plan.caCommonName = tls.SelfSigned.CACommonName
			}
			if tls.SelfSigned.CADuration != "" {
				plan.caDuration = tls.SelfSigned.CADuration
			}
		}
		plan.applyDerivedIdentities(cluster)
	}
	return plan
}

func (p *tlsPlan) applyDerivedIdentities(cluster *nifiv1alpha1.NiFiCluster) {
	p.nodeCommonName = boundedCommonName(cluster.Name, "node")
	p.operatorCommonName = boundedCommonName(cluster.Name, "operator")
	if certificate := cluster.Spec.InternalTLS.Certificate; certificate != nil {
		if certificate.NodeCommonName != "" {
			p.nodeCommonName = certificate.NodeCommonName
		}
		if certificate.OperatorCommonName != "" {
			p.operatorCommonName = certificate.OperatorCommonName
		}
	}
	p.nodeIdentity = "CN=" + p.nodeCommonName
	p.initialAdminIdentity = "CN=" + p.operatorCommonName
}

// managedClusterDomain is the Kubernetes cluster DNS domain used to build fully-qualified
// Service names, defaulting to cluster.local when spec.clusterDomain is unset.
func managedClusterDomain(cluster *nifiv1alpha1.NiFiCluster) string {
	if cluster.Spec.ClusterDomain != "" {
		return cluster.Spec.ClusterDomain
	}
	return "cluster.local"
}

// managedClusterServerDNSNames returns the DNS SANs for the shared server/node
// certificate. Per-pod headless addresses are covered with a wildcard rather than an
// enumerated list, so the certificate does not need regenerating when replicas change.
func managedClusterServerDNSNames(cluster *nifiv1alpha1.NiFiCluster) []string {
	service := managedClusterResourceName(cluster)
	headless := managedClusterHeadlessServiceName(cluster)
	ns := cluster.Namespace
	domain := managedClusterDomain(cluster)
	names := []string{
		"localhost",
		service,
		fmt.Sprintf("%s.%s.svc", service, ns),
		fmt.Sprintf("%s.%s.svc.%s", service, ns, domain),
		fmt.Sprintf("%s.%s.svc", headless, ns),
		fmt.Sprintf("%s.%s.svc.%s", headless, ns, domain),
		fmt.Sprintf("*.%s.%s.svc", headless, ns),
		fmt.Sprintf("*.%s.%s.svc.%s", headless, ns, domain),
	}
	return names
}

// managedClusterProxyHosts returns the comma-separated nifi.web.proxy.host allow-list so
// the operator can reach NiFi by its Service DNS names without tripping host-header checks.
func managedClusterProxyHosts(cluster *nifiv1alpha1.NiFiCluster) string {
	service := managedClusterResourceName(cluster)
	headless := managedClusterHeadlessServiceName(cluster)
	ns := cluster.Namespace
	domain := managedClusterDomain(cluster)
	port := managedClusterHTTPSPort(cluster)
	hosts := map[string]struct{}{}
	for _, host := range []string{
		"localhost",
		service,
		fmt.Sprintf("%s.%s.svc", service, ns),
		fmt.Sprintf("%s.%s.svc.%s", service, ns, domain),
		fmt.Sprintf("%s.%s.svc", headless, ns),
		fmt.Sprintf("%s.%s.svc.%s", headless, ns, domain),
	} {
		hosts[host] = struct{}{}
		hosts[fmt.Sprintf("%s:%d", host, port)] = struct{}{}
	}
	ordered := make([]string, 0, len(hosts))
	for host := range hosts {
		ordered = append(ordered, host)
	}
	sort.Strings(ordered)
	return strings.Join(ordered, ",")
}

func boundedCommonName(base, suffix string) string {
	candidate := strings.TrimSuffix(base, "-") + "-" + suffix
	if len(candidate) <= 64 {
		return candidate
	}
	return candidate[:64]
}

func defaultPrivateKey() *certmanager.PrivateKey {
	return &certmanager.PrivateKey{Algorithm: "RSA", Size: 2048, Encoding: "PKCS8", RotationPolicy: "Always"}
}

func pkcs12Keystore(passwordSecretName string) *certmanager.Keystores {
	return &certmanager.Keystores{PKCS12: &certmanager.PKCS12Keystore{
		Create:            true,
		PasswordSecretRef: certmanager.SecretKeySelector{Name: passwordSecretName, Key: keystorePasswordKey},
	}}
}

func generatePassword() (string, error) {
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return "", err
	}
	return hex.EncodeToString(buffer), nil
}

func tlsChecksum(serverData map[string][]byte, configInput string) string {
	hasher := sha256.New()
	keys := make([]string, 0, len(serverData))
	for key := range serverData {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		hasher.Write([]byte(key))
		hasher.Write(serverData[key])
	}
	hasher.Write([]byte(configInput))
	return hex.EncodeToString(hasher.Sum(nil))[:32]
}

func renderAuthorizersXML(initialAdminIdentity, nodeIdentity string) string {
	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<authorizers>
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">./conf/users.xml</property>
        <property name="Initial User Identity admin">%[1]s</property>
        <property name="Initial User Identity node">%[2]s</property>
    </userGroupProvider>
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="User Group Provider">file-user-group-provider</property>
        <property name="Authorizations File">./conf/authorizations.xml</property>
        <property name="Initial Admin Identity">%[1]s</property>
        <property name="Node Identity node">%[2]s</property>
    </accessPolicyProvider>
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.authorization.StandardManagedAuthorizer</class>
        <property name="Access Policy Provider">file-access-policy-provider</property>
    </authorizer>
</authorizers>
`, escapeXML(initialAdminIdentity), escapeXML(nodeIdentity))
}

func renderTLSReadinessScript() string {
	return `#!/bin/bash
# mTLS readiness probe: present the node's own client certificate to the local NiFi
# HTTPS endpoint. An ordinary httpGet probe cannot present a certificate, so NiFi would
# reject it when needClientAuth is enabled. Any HTTP response (including 401/403) over a
# verified TLS handshake means the secured web server is up.
set -eu
port="${NIFI_WEB_HTTPS_PORT:-8443}"
dir="${NIFI_SECURITY_DIR:-/opt/nifi/nificontrol-tls}"
# In a cluster NiFi binds its routable headless DNS name (advertised host) rather than all
# interfaces, so probe that name; it also matches the server certificate's wildcard SAN.
host="${NIFI_WEB_ADVERTISED_HOST:-localhost}"
url="https://${host}:${port}/nifi-api/flow/about"
if command -v curl >/dev/null 2>&1; then
  args=(-sS -o /dev/null --cert "${dir}/tls.crt" --key "${dir}/tls.key")
  if [ -s "${dir}/ca.crt" ]; then
    args+=(--cacert "${dir}/ca.crt")
  fi
  exec curl "${args[@]}" "${url}"
fi
args=(s_client -connect "${host}:${port}" -cert "${dir}/tls.crt" -key "${dir}/tls.key" -verify_return_error -quiet)
if [ -s "${dir}/ca.crt" ]; then
  args+=(-CAfile "${dir}/ca.crt")
fi
exec openssl "${args[@]}" </dev/null
`
}

func escapeXML(value string) string {
	replacer := strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;", "\"", "&quot;", "'", "&apos;")
	return replacer.Replace(value)
}
