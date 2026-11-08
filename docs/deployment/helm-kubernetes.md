---
title: "Helm / Kubernetes"
description: "Helm chart installation, production values, scaling, multi-tenancy, troubleshooting"
---

# Helm / Kubernetes

Deploy Pangrova on Kubernetes with the bundled Helm chart.

The chart's embedded YAML is a single-Coordinator development profile. It uses
an external PostgreSQL database for durable metadata and state and explicitly
selects the `NOOP` user authenticator. `NOOP` accepts unauthenticated users, so
the chart **refuses to render by default**. Set `developmentMode=true` only for
an isolated development cluster, or replace `config.coordinator` with a real
OAUTH2/LDAP configuration. The chart also rejects multiple Coordinator replicas
unless `coordinator.ha.enabled=true` is configured; that flag alone does not
prove HA. Rendering and linting do not prove the external database, Secrets,
catalogs, transport TLS, or spill capacity are usable. Verify these before
production deployment.

Before installation, create a PostgreSQL database named `pangrova_meta` with
an account allowed to create and migrate tables. Expose it as
`pangrova-postgresql:5432` in the release namespace, or override
`metadata.host`, `metadata.port`, and `metadata.database`. Create a
`pangrova-metadata-core` Secret with `username` and `password` keys; override
`metadata.existingSecret` if it has a different name. Missing Secret keys cause
Kubernetes to reject the Coordinator container at startup, while an unreachable
database causes Coordinator metadata bootstrap to fail. Also create a
`pangrova-internal-auth` Secret with an `internal-auth-secret` key of at least
32 random bytes in the release namespace.
For multi-tenant deployment, create the same Secret in every tenant namespace.
The chart references the Secret through `internalAuth.existingSecret` and never
stores its value in chart values. The release namespace must exist before the
Secrets are created; the chart leaves it under the operator's ownership by
default (`manageNamespace: false`).
The image runs as a dynamically allocated non-root `pangrova` user (no fixed
uid/gid); the chart sets pod `fsGroup: 999` so mounted log and Worker spill
volumes are writable. A storage driver that does not honor `fsGroup` needs
equivalent ownership set by the operator.
Coordinator and Worker liveness probes call `/v1/health/live`; readiness probes
call `/v1/health`. Coordinator readiness requires a Worker and catalog, so the
chart also publishes `pangrova-coordinator-bootstrap` for Worker registration
and initial catalog administration before the query-facing Coordinator Service
has ready endpoints. The bootstrap Service deliberately publishes not-ready
addresses and should remain cluster-internal. Worker data volumes must provide
at least the configured Native shuffle spill quota plus headroom; the default
chart mounts `/opt/pangrova/data` even when persistent volumes are disabled.


## Kubernetes — Helm Chart {#helm-kubernetes}

```bash
# Create the namespace and prerequisites; substitute your database credentials.
kubectl create namespace pangrova
kubectl create secret generic pangrova-internal-auth -n pangrova \
    --from-literal=internal-auth-secret="$(openssl rand -base64 48)"
kubectl create secret generic pangrova-metadata-core -n pangrova \
    --from-literal=username='<database-user>' \
    --from-literal=password='<database-password>'

# Install the development profile after PostgreSQL exists. This explicitly
# accepts unauthenticated client traffic; do not expose it publicly.
helm install pangrova deploy/helm/pangrova -n pangrova --set developmentMode=true

# Alternatively, install with overrides (run this instead of the command above).
helm install pangrova deploy/helm/pangrova -n pangrova \
    --set developmentMode=true \
    --set metadata.host=postgres.example.internal \
    --set worker.replicas=5 \
    --set image.tag=0.1.0

# Upgrade
helm upgrade pangrova deploy/helm/pangrova -n pangrova --reuse-values

# Uninstall
helm uninstall pangrova -n pangrova
```

---

## Helm Chart — Advanced Usage

### Custom Values File

This example changes image, database endpoint, and Worker size while retaining
the development `NOOP` authenticator. It is **not** a production security
configuration. Production requires an environment-specific `config.coordinator`
with OAUTH2 or LDAP, transport TLS, and catalog bootstrap; retain the
`coordinator.internal-auth-secret`, JDBC metadata settings, and Worker-control
authentication when replacing the embedded YAML.

Create a `development-values.yaml` for a cluster with the prerequisites above:

```yaml
# development-values.yaml
developmentMode: true
image:
  repository: my-registry.example.com/pangrova/pangrova
  tag: "0.1.0"

metadata:
  host: pg-meta.example.internal
  port: 5432
  database: pangrova_meta
  existingSecret: pangrova-metadata-core

coordinator:
  replicas: 1
  resources:
    requests: { cpu: "4", memory: "8Gi" }
    limits: { cpu: "8", memory: "16Gi" }
  javaOpts: "-Xms4g -Xmx10g"
  nodeSelector:
    node-role: coordinator
  podAnnotations:
    prometheus.io/scrape: "true"
    prometheus.io/port: "8080"

worker:
  replicas: 5
  resources:
    requests: { cpu: "8", memory: "32Gi" }
    limits: { cpu: "16", memory: "64Gi" }
  javaOpts: "-Xms4g -Xmx8g -XX:MaxDirectMemorySize=16g"
  nodeSelector:
    node-role: worker
  affinity:
    podAntiAffinity:
      preferredDuringSchedulingIgnoredDuringExecution:
        - weight: 100
          podAffinityTerm:
            labelSelector:
              matchExpressions:
                - key: component
                  operator: In
                  values: ["worker"]
            topologyKey: kubernetes.io/hostname

persistence:
  enabled: true
  size: 500Gi
  storageClass: gp3

autoscaling:
  enabled: true
  minReplicas: 5
  maxReplicas: 20
  targetCPUUtilizationPercentage: 65

ingress:
  enabled: true
  className: nginx
  annotations:
    cert-manager.io/cluster-issuer: letsencrypt-prod
  hosts:
    - host: pangrova.example.com
      paths:
        - path: /
          pathType: Prefix
  tls:
    - secretName: pangrova-tls
      hosts:
        - pangrova.example.com
```

Install the development example with:

```bash
helm install pangrova deploy/helm/pangrova -n pangrova -f development-values.yaml
```

For production, leave `developmentMode: false` and supply a full
`config.coordinator` with `security.enabled: true`, `security.auth.type:
OAUTH2` or `LDAP`, and the backend-specific authenticator properties. Preserve
the JDBC metadata placeholders and `coordinator.internal-auth-secret` when
overriding that YAML. The runtime then validates the authenticator settings;
the Helm gate prevents an accidental `NOOP` or missing security section.

### Override Configuration via --set-file

Inject external config files directly:

```bash
helm install pangrova deploy/helm/pangrova -n pangrova \
  --set-file config.coordinator=etc/coordinator.yml \
  --set-file config.worker=etc/worker.yml
```

### Scaling Workers

```bash
# Manual scale
kubectl scale statefulset pangrova-worker -n pangrova --replicas=10

# Or via Helm
helm upgrade pangrova deploy/helm/pangrova -n pangrova --set worker.replicas=10

# HPA handles auto-scaling when autoscaling.enabled=true
kubectl get hpa -n pangrova
```

### Monitoring the Deployment

```bash
# Check pod status
kubectl get pods -n pangrova -l app=pangrova

# Check Coordinator logs
kubectl logs -n pangrova -l component=coordinator --tail=100

# Check Worker logs
kubectl logs -n pangrova -l component=worker --tail=100

# Check health
kubectl exec -n pangrova deploy/pangrova-coordinator -- curl -s http://localhost:8080/v1/health

# Check HPA status
kubectl describe hpa pangrova-worker -n pangrova
```

### Helm Values Reference

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `image.repository` | string | `pangrova/pangrova` | Container image repository |
| `image.tag` | string | `latest` | Container image tag |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy |
| `coordinator.replicas` | int | `1` | Number of Coordinator pods; multiple replicas require shared JDBC state and HA configuration |
| `internalAuth.existingSecret` | string | `pangrova-internal-auth` | Existing Secret with `internal-auth-secret` key in each deployment namespace |
| `coordinator.resources` | object | 2 CPU / 4Gi | Coordinator resource requests/limits |
| `coordinator.javaOpts` | string | `-Xms1g -Xmx4g` | Coordinator JVM options |
| `coordinator.nodeSelector` | object | `{}` | Coordinator node selector |
| `coordinator.tolerations` | list | `[]` | Coordinator tolerations |
| `coordinator.affinity` | object | `{}` | Coordinator affinity rules |
| `worker.replicas` | int | `3` | Number of Worker pods |
| `worker.resources` | object | 4 CPU / 16Gi | Worker resource requests/limits |
| `worker.javaOpts` | string | `-Xms2g -Xmx16g -XX:MaxDirectMemorySize=4g` | Worker JVM options |
| `worker.nodeSelector` | object | `{}` | Worker node selector |
| `config.coordinator` | string | (embedded) | Full coordinator.yml content |
| `config.worker` | string | (embedded) | Full worker.yml content |
| `persistence.enabled` | bool | `false` | Enable Worker persistent volumes |
| `persistence.size` | string | `100Gi` | PVC size per Worker |
| `persistence.storageClass` | string | `""` | Storage class (empty = default) |
| `autoscaling.enabled` | bool | `true` | Enable Worker HPA |
| `autoscaling.minReplicas` | int | `3` | HPA minimum replicas |
| `autoscaling.maxReplicas` | int | `10` | HPA maximum replicas |
| `autoscaling.targetCPUUtilizationPercentage` | int | `70` | HPA CPU target |
| `ingress.enabled` | bool | `false` | Enable Ingress |
| `ingress.className` | string | `""` | Ingress class name |

### Troubleshooting Kubernetes Deployment

| Symptom | Diagnosis | Fix |
|---------|-----------|-----|
| Pods stuck in `Pending` | `kubectl describe pod` — check events | Insufficient resources → scale node pool or reduce requests |
| Pods in `CrashLoopBackOff` | `kubectl logs <pod>` — check Java errors | Wrong JVM opts, missing config, or port conflict |
| Workers not registering | Check Worker logs for registration errors | Verify `PANGROVA_COORDINATOR_URL` env var points to Coordinator Service |
| HPA not scaling | `kubectl describe hpa` — check metrics | Ensure metrics-server is installed: `kubectl top pods -n pangrova` |
| ConfigMap not updating | Pods use old config after `helm upgrade` | Restart pods: `kubectl rollout restart statefulset/pangrova-worker -n pangrova` |
| Ingress not routing | `kubectl describe ingress -n pangrova` | Check ingress controller logs, verify TLS secret exists |
| PVC stuck in `Pending` | `kubectl describe pvc` | Check StorageClass exists and has available capacity |

---

## Multi-Tenancy — Namespace Isolation

The Helm chart can render per-tenant namespace isolation resources. Production
SQL tenant isolation additionally requires `tenant.enabled: true`, OAUTH2 or
LDAP authentication, membership provisioning and a verified HA/control-plane
configuration. The embedded `NOOP` development profile cannot meet those
requirements. In the installation example below,
`secure-coordinator-values.yaml` is an operator-provided file containing the
authenticated Coordinator configuration and related production overrides.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Control Plane Namespace (pangrova)                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ Coordinator  │  │ Coordinator  │  │ Shared Worker│     │
│  │   Pod #1     │  │   Pod #2     │  │    Pool      │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
         │                    │
         ▼                    ▼
┌─────────────────┐  ┌─────────────────┐
│ Tenant-Alpha NS │  │ Tenant-Beta NS  │
│ ┌─────────────┐ │  │ ┌─────────────┐ │
│ │ Worker Pool │ │  │ │ Worker Pool │ │
│ │ (isolated)  │ │  │ │ (isolated)  │ │
│ └─────────────┘ │  │ └─────────────┘ │
│ ResourceQuota   │  │ ResourceQuota   │
│ NetworkPolicy   │  │ NetworkPolicy   │
│ LimitRange      │  │ LimitRange      │
│ HPA             │  │ HPA             │
└─────────────────┘  └─────────────────┘
     ✗ No cross-tenant traffic ✗
```

### Enable Multi-Tenancy

```yaml
# multi-tenant-values.yaml
multiTenancy:
  enabled: true
  tenants:
    - name: tenant-alpha
      namespace: pangrova-tenant-alpha
      resourceQuota:
        cpu: "16"
        memory: "64Gi"
        pods: "20"
        persistentVolumeClaims: "10"
        requestsStorage: "500Gi"
      workerReplicas: 3
      workerResources:
        requests: { cpu: "4", memory: "16Gi" }
        limits: { cpu: "8", memory: "32Gi" }
      autoscaling:
        enabled: true
        minReplicas: 2
        maxReplicas: 10
        targetCPUUtilizationPercentage: 70
        targetMemoryUtilizationPercentage: 80
    - name: tenant-beta
      namespace: pangrova-tenant-beta
      resourceQuota:
        cpu: "8"
        memory: "32Gi"
        pods: "10"
      workerReplicas: 2
  networkPolicy:
    allowFromControlPlane: true
```

Install:

```bash
helm install pangrova deploy/helm/pangrova -n pangrova \
    -f secure-coordinator-values.yaml -f multi-tenant-values.yaml
```

### What Gets Created Per Tenant

| Resource | Purpose |
|----------|---------|
| Namespace | Physical isolation boundary |
| ResourceQuota | CPU/memory/pod limits per tenant |
| LimitRange | Default container resource limits |
| NetworkPolicy (deny-all) | Block all cross-namespace traffic by default |
| NetworkPolicy (allow-intra) | Allow pods within same tenant to communicate |
| NetworkPolicy (allow-dns) | Allow DNS resolution for service discovery |
| NetworkPolicy (allow-control-plane) | Allow coordinator to reach tenant workers |
| StatefulSet (worker) | Isolated worker pool per tenant |
| HPA | Auto-scaling per tenant worker pool |
| Service (headless) | Worker discovery within tenant namespace |
| ConfigMap | Worker configuration |

### Network Isolation Model

- **Default deny**: All ingress and egress blocked by default
- **Intra-namespace**: Pods within the same tenant can communicate freely
- **Control plane**: Coordinator namespace can reach tenant workers (configurable)
- **DNS**: All pods can resolve DNS via kube-system
- **Cross-tenant**: Explicitly blocked — tenant-alpha cannot reach tenant-beta

### Testing Namespace Isolation

```bash
# Run the automated test (requires a running K8s cluster)
./deploy/helm/pangrova/tests/test-namespace-isolation.sh

# Cleanup after testing
./deploy/helm/pangrova/tests/test-namespace-isolation.sh --cleanup
```
