package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/internal/controller"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

// namespaceCacheConfig parses a comma-separated namespace list into a controller-runtime cache
// restriction. An empty list returns nil, meaning watch all namespaces (cluster-scoped).
func namespaceCacheConfig(watchNamespaces string) map[string]cache.Config {
	config := map[string]cache.Config{}
	for _, ns := range strings.Split(watchNamespaces, ",") {
		if ns = strings.TrimSpace(ns); ns != "" {
			config[ns] = cache.Config{}
		}
	}
	if len(config) == 0 {
		return nil
	}
	return config
}

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElection bool
	var leaseDuration, renewDeadline, retryPeriod time.Duration

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	// Leader-election timing. Defaults match controller-runtime's own; tune them on slow or
	// contended API servers to avoid spurious leadership loss. Must satisfy
	// leaseDuration > renewDeadline > retryPeriod.
	flag.DurationVar(&leaseDuration, "leader-elect-lease-duration", 15*time.Second, "Duration non-leaders wait before force-acquiring leadership.")
	flag.DurationVar(&renewDeadline, "leader-elect-renew-deadline", 10*time.Second, "Duration the acting leader retries refreshing leadership before giving up.")
	flag.DurationVar(&retryPeriod, "leader-elect-retry-period", 2*time.Second, "Interval between leader-election action attempts.")
	var watchNamespaces string
	flag.StringVar(&watchNamespaces, "watch-namespaces", os.Getenv("WATCH_NAMESPACES"),
		"Comma-separated namespaces to watch. Empty (the default) watches all namespaces. Defaults to the WATCH_NAMESPACES env var.")
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Without a configured logger, controller-runtime discards all logs ("log.SetLogger was never
	// called; logs will not be displayed"), so the operator would run silently.
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	options := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                serverOptions(metricsAddr),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "nificontrol.nifi.controlnifi.io",
		LeaseDuration:          &leaseDuration,
		RenewDeadline:          &renewDeadline,
		RetryPeriod:            &retryPeriod,
	}
	// Restrict the cache (and therefore the reconcilers) to specific namespaces when requested, so
	// the operator can run namespace-scoped (one per team) instead of cluster-wide.
	if nsConfig := namespaceCacheConfig(watchNamespaces); nsConfig != nil {
		options.Cache = cache.Options{DefaultNamespaces: nsConfig}
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), options)
	if err != nil {
		os.Exit(1)
	}

	if err := (&controller.NiFiClusterReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ReachabilityChecker: nifi.HTTPReachabilityChecker{}, ClusterNodeClient: nifi.HTTPClusterNodeClient{}, Recorder: mgr.GetEventRecorderFor("nificluster-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiRegistryClientReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), RegistryClientClient: nifi.HTTPRegistryClientClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiParameterContextReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ParameterContextClient: nifi.HTTPParameterContextClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiUserReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), UserClient: nifi.HTTPUserClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiUserGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), UserGroupClient: nifi.HTTPUserGroupClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiProcessGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ProcessGroupClient: nifi.HTTPProcessGroupClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiControllerServiceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ControllerServiceClient: nifi.HTTPControllerServiceClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	artifactResolver := flowartifact.DefaultResolver{}
	if err := (&controller.NiFiFlowBundleReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ArtifactResolver: artifactResolver}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiFlowDeploymentReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ProcessGroupClient: nifi.HTTPProcessGroupClient{}, FlowSnapshotClient: nifi.HTTPFlowSnapshotClient{}, ArtifactResolver: artifactResolver}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiProcessorReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ProcessorClient: nifi.HTTPProcessorClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiInputPortReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), InputPortClient: nifi.HTTPInputPortClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiOutputPortReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), OutputPortClient: nifi.HTTPOutputPortClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiConnectionReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ConnectionClient: nifi.HTTPConnectionClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiReportingTaskReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ReportingTaskClient: nifi.HTTPReportingTaskClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiParameterProviderReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ParameterProviderClient: nifi.HTTPParameterProviderClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiFunnelReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), FunnelClient: nifi.HTTPFunnelClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiNodeGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ClusterNodeClient: nifi.HTTPClusterNodeClient{}, Recorder: mgr.GetEventRecorderFor("nifinodegroup-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiBackupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), SnapshotReader: nifi.HTTPFlowSnapshotClient{}, ProcessGroups: nifi.HTTPProcessGroupClient{}, Recorder: mgr.GetEventRecorderFor("nifibackup-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiRestoreReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Snapshots: nifi.HTTPFlowSnapshotClient{}, ProcessGroups: nifi.HTTPProcessGroupClient{}, Recorder: mgr.GetEventRecorderFor("nifirestore-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiLabelReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), LabelClient: nifi.HTTPLabelClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiAutoscalerReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Recorder: mgr.GetEventRecorderFor("nifiautoscaler-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiPolicyReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), AccessPolicyClient: nifi.HTTPAccessPolicyClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiRemoteProcessGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), RemoteProcessGroupClient: nifi.HTTPRemoteProcessGroupClient{}, Recorder: mgr.GetEventRecorderFor("nifiremoteprocessgroup-controller")}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		os.Exit(1)
	}
}
