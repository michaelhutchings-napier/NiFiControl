package controller

import (
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

func TestApplyProbeTuningCustomHandlerReplacesActionButKeepsTiming(t *testing.T) {
	p := &corev1.Probe{
		ProbeHandler:  corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/nifi-api/flow/about"}},
		PeriodSeconds: 20,
	}
	period := int32(45)
	applyProbeTuning(p, &nifiv1alpha1.NiFiClusterProbeTuning{
		PeriodSeconds: &period,
		Handler:       &corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "true"}}},
	})

	if p.ProbeHandler.HTTPGet != nil {
		t.Error("custom handler should replace the default httpGet action")
	}
	if p.ProbeHandler.Exec == nil || len(p.ProbeHandler.Exec.Command) != 3 {
		t.Errorf("expected the custom exec handler, got %+v", p.ProbeHandler)
	}
	if p.PeriodSeconds != 45 {
		t.Errorf("timing tuning should still apply alongside a custom handler, periodSeconds = %d", p.PeriodSeconds)
	}
}

func TestApplyProbeTuningEmptyHandlerKeepsDefaultAction(t *testing.T) {
	// An empty handler ({}) must not blank out the default action — Kubernetes rejects an
	// action-less probe, so the operator ignores it.
	p := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: "/x"}}}
	applyProbeTuning(p, &nifiv1alpha1.NiFiClusterProbeTuning{Handler: &corev1.ProbeHandler{}})
	if p.ProbeHandler.HTTPGet == nil {
		t.Fatal("an empty handler must leave the operator's default action intact")
	}
}

func TestManagedClusterLivenessProbeUsesCustomHandler(t *testing.T) {
	cluster := &nifiv1alpha1.NiFiCluster{
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Pod: &nifiv1alpha1.NiFiClusterPodSpec{
				Probes: &nifiv1alpha1.NiFiClusterProbesSpec{
					Liveness: &nifiv1alpha1.NiFiClusterProbeTuning{
						Handler: &corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{}},
					},
				},
			},
		},
	}
	p := managedClusterLivenessProbe(cluster, nil)
	if p.ProbeHandler.TCPSocket == nil {
		t.Fatalf("liveness probe should use the custom tcpSocket handler, got %+v", p.ProbeHandler)
	}
}
