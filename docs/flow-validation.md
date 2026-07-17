# Flow validation (dry run)

`spec.validateOnly` turns a `NiFiFlowDeployment` into a **pre-flight check**: it imports the
resolved flow into a temporary, detached process group, enables its controller services, inspects
every component's validity, records the result in `status.validationResult`, and then deletes the
temporary group. Nothing is ever wired into the live flow, so it is safe to run against a production
cluster and to gate a CI pipeline on.

This complements the deploy-time [readiness gate](rollout-readiness.md): the readiness gate reacts
*after* a rollout and rolls back an unhealthy flow, while `validateOnly` answers "would this flow
deploy cleanly?" *before* anything changes.

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata: {name: payments-check}
spec:
  clusterRef: {name: production}
  validateOnly: true
  source:
    bundleRef: {name: payments-flow}     # or source.inline: {...}
  target:
    parentProcessGroupRef: {root: true}
```

## What it checks

The dry run imports the flow, enables its controller services (so components that reference a service
validate as they would on a real deploy, rather than reporting a spurious "service disabled"), waits
for validation to settle, and reports the result. `Ready` reflects the outcome:

- **Valid flow** → `Ready=true` (reason `ValidationSucceeded`), `status.validationResult.valid=true`.
- **Invalid flow** → `Ready=false` (reason `ValidationFailed`), with each offending processor and
  controller service listed in `status.validationResult.invalidComponents` (kind, name, type, and
  NiFi's validation errors).

Because the deployment never creates a live process group in this mode, `status.processGroupId` stays
empty. The temporary group is always deleted, whether the flow was valid or not.

```yaml
status:
  ready: false
  validationResult:
    valid: false
    checkedVersion: "1.4.0"
    invalidCount: 1
    invalidComponents:
      - kind: Processor
        name: Write to warehouse
        type: org.apache.nifi.processors.standard.PutDatabaseRecord
        errors:
          - "'Database Connection Pooling Service' is invalid because ..."
    message: "Flow validation failed: 1 invalid component(s) of 6 checked."
```

## Gating CI

Apply the resource and wait on the `Ready` condition — it succeeds for a valid flow and times out for
an invalid one, so it works as a build gate:

```bash
kubectl apply -f payments-check.yaml
kubectl wait --for=condition=Ready nififlowdeployment/payments-check --timeout=2m
kubectl get nififlowdeployment/payments-check -o jsonpath='{.status.validationResult}' | jq
```

A validated result is cached against the resolved artifact digest, so an unchanged flow is not
re-imported on every reconcile; changing the source (or the referenced bundle's content) triggers a
fresh validation.

## Notes and limits

- `validateOnly` requires a source that resolves to a flow snapshot (`inline` or `bundleRef`).
- It cannot be enabled on a deployment that already created a live process group (that would orphan
  it); use a separate `NiFiFlowDeployment` for validation.
- This validates **component configuration** — the same notion of "valid" NiFi shows on the canvas. It
  does **not** run data through the flow. Functional, data-driven flow testing belongs in CI with
  NiFi's JVM-side test framework (`nifi-mock` / `TestRunner`), not in the operator.
