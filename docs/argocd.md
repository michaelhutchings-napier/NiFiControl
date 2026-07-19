# Argo CD

NiFiControl resources expose a standard `Ready` condition. Add a Lua health check
so Argo CD shows `Healthy`, `Progressing`, or `Degraded` instead of only seeing
that the CR exists.

## Health Check

```lua
hs = { status = "Progressing", message = "Waiting for NiFiControl status" }
if obj.status and obj.status.conditions then
  for _, c in ipairs(obj.status.conditions) do
    if c.type == "Ready" then
      hs.message = c.message or c.reason or hs.message
      if c.status == "True" then
        hs.status = "Healthy"
      elseif c.status == "False" then
        hs.status = "Degraded"
      end
      return hs
    end
  end
end
return hs
```

Register it in `argocd-cm` for each kind you deploy:

```yaml
data:
  resource.customizations.health.nifi.controlnifi.io_NiFiCluster: |
    <lua above>
  resource.customizations.health.nifi.controlnifi.io_NiFiFlowDeployment: |
    <lua above>
```

## Sync Waves

Use waves when one app contains multiple CRDs:

| Wave | Resources |
| --- | --- |
| `-1` | CRDs/operator |
| `0` | `NiFiCluster`, `NiFiNodeGroup` |
| `1` | tenants, parameter contexts, registry clients |
| `2` | canvas components, flow bundles |
| `3` | flow deployments, backups, autoscalers |

Keep the operator installed separately from workloads so finalizers keep working
during cluster upgrades and deletes.
