CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
HELM ?= helm
HELM_CHART ?= charts/nificontrol
HELM_CLUSTER_CHART ?= charts/nifi-cluster

.PHONY: all
all: test

.PHONY: generate
generate:
	$(CONTROLLER_GEN) object:headerFile=/dev/null paths=./api/...

.PHONY: manifests
manifests:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=nificontrol-manager-role paths=./internal/controller/... output:rbac:artifacts:config=config/rbac

.PHONY: helm-sync-crds
helm-sync-crds:
	mkdir -p $(HELM_CHART)/crds
	rm -f $(HELM_CHART)/crds/*.yaml
	cp config/crd/bases/*.yaml $(HELM_CHART)/crds/

.PHONY: helm-crds-check
helm-crds-check:
	diff -qr config/crd/bases $(HELM_CHART)/crds

.PHONY: helm-lint
helm-lint: helm-crds-check
	$(HELM) lint $(HELM_CHART)
	$(HELM) lint $(HELM_CLUSTER_CHART)

.PHONY: helm-template
helm-template: helm-crds-check
	$(HELM) template nificontrol $(HELM_CHART) --namespace nificontrol-system --include-crds >/dev/null
	$(HELM) template custom $(HELM_CHART) --namespace operators --set rbac.create=false --set serviceAccount.create=false --set serviceAccount.name=existing --set metrics.service.enabled=false --set podDisruptionBudget.enabled=true >/dev/null
	$(HELM) template production $(HELM_CLUSTER_CHART) --namespace dataflows >/dev/null
	$(HELM) template clustered $(HELM_CLUSTER_CHART) --namespace dataflows --set replicas=3 --set coordination.zookeeperConnectString=zookeeper.dataflows.svc:2181 >/dev/null
	$(HELM) template metrics $(HELM_CLUSTER_CHART) --namespace dataflows --set replicas=3 --set coordination.zookeeperConnectString=zookeeper.dataflows.svc:2181 --set metrics.enabled=true --set metrics.serviceMonitor.enabled=true >/dev/null
	$(HELM) template operator-sm $(HELM_CHART) --namespace nificontrol-system --set metrics.serviceMonitor.enabled=true >/dev/null

.PHONY: helm-verify
helm-verify: helm-lint helm-template

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test
test: generate fmt
	go test ./...

.PHONY: integration-nifi
integration-nifi:
	./hack/test-nifi-2.10.sh

.PHONY: integration-nifi-tls
integration-nifi-tls:
	./hack/test-nifi-2.10-tls.sh

.PHONY: integration-bluegreen-kind
integration-bluegreen-kind:
	./hack/test-bluegreen-kind.sh

.PHONY: integration-offload-kind
integration-offload-kind:
	./hack/test-offload-kind.sh

.PHONY: integration-nodegroups-kind
integration-nodegroups-kind:
	./hack/test-nodegroups-kind.sh

.PHONY: integration-hpa-kind
integration-hpa-kind:
	./hack/test-hpa-kind.sh

.PHONY: integration-observability-kind
integration-observability-kind:
	./hack/test-observability-kind.sh

.PHONY: integration-autoscaler-kind
integration-autoscaler-kind:
	./hack/test-autoscaler-kind.sh

.PHONY: integration-authz-kind
integration-authz-kind:
	./hack/test-authz-kind.sh
