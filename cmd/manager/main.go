package main

import (
	"flag"
	"os"

	"github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/internal/controller"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
)

func main() {
	var metricsAddr string
	var probeAddr string
	var leaderElection bool

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&leaderElection, "leader-elect", false, "Enable leader election for controller manager.")
	flag.Parse()

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

	if err := (&controller.NiFiClusterReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ReachabilityChecker: nifi.HTTPReachabilityChecker{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiRegistryClientReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), RegistryClientClient: nifi.HTTPRegistryClientClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiParameterContextReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), ParameterContextClient: nifi.HTTPParameterContextClient{}}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiUserReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiUserGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiProcessGroupReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiControllerServiceReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiFlowBundleReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiFlowDeploymentReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiProcessorReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiInputPortReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiOutputPortReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiConnectionReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
		os.Exit(1)
	}
	if err := (&controller.NiFiReportingTaskReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}).SetupWithManager(mgr); err != nil {
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
