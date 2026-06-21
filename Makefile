CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5
HELM ?= helm
HELM_CHART ?= charts/nificontrol

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

.PHONY: helm-template
helm-template: helm-crds-check
	$(HELM) template nificontrol $(HELM_CHART) --namespace nificontrol-system --include-crds >/dev/null
	$(HELM) template custom $(HELM_CHART) --namespace operators --set rbac.create=false --set serviceAccount.create=false --set serviceAccount.name=existing --set metrics.service.enabled=false --set podDisruptionBudget.enabled=true >/dev/null

.PHONY: helm-verify
helm-verify: helm-lint helm-template

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test
test: generate fmt
	go test ./...
