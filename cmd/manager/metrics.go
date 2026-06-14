package main

import metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

func serverOptions(bindAddress string) metricsserver.Options {
	return metricsserver.Options{BindAddress: bindAddress}
}
