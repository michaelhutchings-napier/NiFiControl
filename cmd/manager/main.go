package main

import (
	"flag"
	"os"

	"github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/internal/controller"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/flowartifact"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	zapOpts := zap.Options{Development: false}
	zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	// Without a configured logger, controller-runtime discards all logs ("log.SetLogger was never
	// called; logs will not be displayed"), so the operator would run silently.
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1alpha1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                serverOptions(metricsAddr),
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElection,
		LeaderElectionID:       "nificontrol.nifi.controlnifi.io",
	})
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
