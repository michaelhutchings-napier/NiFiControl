CONTROLLER_GEN ?= go run sigs.k8s.io/controller-tools/cmd/controller-gen@v0.16.5

.PHONY: all
all: test

.PHONY: generate
generate:
	$(CONTROLLER_GEN) object:headerFile=/dev/null paths=./api/...

.PHONY: manifests
manifests:
	$(CONTROLLER_GEN) crd:allowDangerousTypes=true paths=./api/... output:crd:artifacts:config=config/crd/bases
	$(CONTROLLER_GEN) rbac:roleName=nificontrol-manager-role paths=./internal/controller/... output:rbac:artifacts:config=config/rbac

.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: test
test: generate fmt
	go test ./...

