# Deploying NiFiControl resources with Argo CD

Argo CD does not know how to assess the health of custom resources it has never seen, so by default
a `NiFiCluster` (or any `nifi.controlnifi.io` kind) shows as **Progressing** forever — Argo CD can
tell the object exists but not whether the operator has made it Ready. Every NiFiControl kind reports
a standard `Ready` condition (and `status.observedGeneration`), so one small Lua health check makes
Argo CD track them correctly.

## Health check

Every NiFiControl kind shares the same status shape, so the same Lua works for all of them:

```lua
local hs = {}
hs.status = "Progressing"
hs.message = "Waiting for the operator to report status"

-- Status is stale until the operator has observed the current spec generation.
if obj.status ~= nil and obj.status.observedGeneration ~= nil and obj.metadata.generation ~= nil then
  if obj.status.observedGeneration < obj.metadata.generation then
    hs.message = "Waiting for the operator to observe the latest spec"
    return hs
  end
end

if obj.status ~= nil and obj.status.conditions ~= nil then
  for _, c in ipairs(obj.status.conditions) do
    if c.type == "Ready" then
      if c.message ~= nil and c.message ~= "" then
        hs.message = c.message
      end
      if c.status == "True" then
        hs.status = "Healthy"
      else
        -- Ready=False covers both transient waiting (e.g. WaitingForDependencies, retrying) and
        -- hard errors. It is mapped to Progressing so Argo CD keeps polling rather than marking a
        -- still-reconciling resource Degraded. To surface genuine failures as Degraded (so
        -- `argocd app sync --wait` fails fast instead of hanging), treat specific reasons as
        -- Degraded, e.g.:
        --   if c.reason == "PolicyConflict" or c.reason == "NiFiCreateFailed" then
        --     hs.status = "Degraded"
        --   end
        hs.status = "Progressing"
      end
      return hs
    end
  end
end

return hs
```

## Registering it

Add the check to the `argocd-cm` ConfigMap, one key per `group_kind`. The Lua is identical for
every kind, so register it for each NiFiControl kind you deploy through Argo CD:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: argocd-cm
  namespace: argocd
data:
  resource.customizations.health.nifi.controlnifi.io_NiFiCluster: |
    <paste the Lua above>
  resource.customizations.health.nifi.controlnifi.io_NiFiParameterContext: |
    <paste the Lua above>
  resource.customizations.health.nifi.controlnifi.io_NiFiFlowDeployment: |
    <paste the Lua above>
  # …repeat for every kind you use. The full set:
  # NiFiCluster, NiFiNodeGroup, NiFiUser, NiFiUserGroup, NiFiPolicy, NiFiParameterContext,
  # NiFiParameterProvider, NiFiRegistryClient, NiFiProcessGroup, NiFiProcessor, NiFiConnection,
  # NiFiInputPort, NiFiOutputPort, NiFiFunnel, NiFiLabel, NiFiControllerService, NiFiReportingTask,
  # NiFiRemoteProcessGroup, NiFiFlowBundle, NiFiFlowDeployment, NiFiBackup, NiFiRestore,
  # NiFiAutoscaler
```

## Ordering: CRDs, then cluster, then flows

NiFiControl resources have a dependency order that Argo CD should respect:

1. **Install the operator and CRDs first.** Keep the operator (Helm chart, CRDs) in its own
   Application or an earlier sync wave. The `nificlusters` CRD is large — apply CRDs with
   **server-side apply** (`ServerSideApply=true` sync option, or a CRD-only wave), because it
   exceeds the client-side `last-applied-configuration` annotation limit and a client-side apply
   fails. See the CRD apply note in the install docs.

2. **Order dependent resources with sync waves.** A canvas/tenant resource is only reconciled once
   its `clusterRef` cluster is Ready, so let Argo CD apply and wait on the cluster first:

   ```yaml
   metadata:
     annotations:
       argocd.argoproj.io/sync-wave: "0"    # NiFiCluster / NiFiNodeGroup
   ---
   metadata:
     annotations:
       argocd.argoproj.io/sync-wave: "1"    # users, groups, policies, parameter contexts, registry clients
   ---
   metadata:
     annotations:
       argocd.argoproj.io/sync-wave: "2"    # process groups, processors, connections, flow deployments
   ```

   With the health check above, Argo CD waits for each wave's resources to become **Healthy**
   (Ready) before starting the next, so it will not push a flow deployment at a cluster that is not
   up yet. Without waves the resources still converge — they just reconcile with transient
   `WaitingForDependencies` retries and Argo CD may report Progressing longer.

## Notes

- The check keys off the `Ready` condition and `observedGeneration`, both part of every kind's
  common status, so it stays correct as new kinds are added.
- A resource waiting on a dependency reports `Ready=False` with reason `WaitingForDependencies`;
  that is normal during a first sync and clears once the dependency is Ready.
