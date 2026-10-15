---
title: "Helm / Kubernetes"
description: "Helm chart installation, production values, scaling, multi-tenancy, troubleshooting"
---

# Helm / Kubernetes

Deploy Pangrova on Kubernetes with the bundled Helm chart.

The chart's embedded YAML is a development profile: it uses in-memory state,
has no authentication, and has no preconfigured catalog. Rendering and linting
the chart do not establish production readiness. Supply persistent shared JDBC
state, metadata storage, authentication, transport TLS, catalog bootstrap and
sized Worker spill volumes in environment-specific values before production.
Raising `coordinator.replicas` alone does not enable HA.

Before installation, create a `pangrova-internal-auth` Secret with an
`internal-auth-secret` key of at least 32 random bytes in the release namespace.
For multi-tenant deployment, create the same Secret in every tenant namespace.
The chart references the Secret through `internalAuth.existingSecret` and never
stores its value in chart values.
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
# Install
helm install pangrova deploy/helm/pangrova -n pangrova --create-namespace

# Custom values
helm install pangrova deploy/helm/pangrova -n pangrova --create-namespace \
    --set coordinator.replicas=2 \
    --set worker.replicas=5 \
    --set image.tag=0.1.0

# Upgrade
helm upgrade pangrova deploy/helm/pangrova -n pangrova

# Uninstall
helm uninstall pangrova -n pangrova
```

---

## Helm Chart — Advanced Usage

### Custom Values File

Create a `production-values.yaml` for your environment:

```yaml
# production-values.yaml
image:
  repository: my-registry.example.com/pangrova/pangrova
  tag: "0.1.0"
  pullSecrets:
    - name: my-registry-secret

coordinator:
  replicas: 2
  resources:
    requests: { cpu: "4", memory: "8Gi" }
    limits: { cpu: "8", memory: "16Gi" }
  javaOpts: "-Xms4g -Xmx16g"
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

config:
  coordinator: |
    coordinator:
      id: "${HOSTNAME}"
      host: "0.0.0.0"
      port: 8080
      ha:
        enabled: true
        jdbc-url: "jdbc:postgresql://pg-ha:5432/pangrova_ha"
      query:
        timeout: "10m"
        max-concurrent: 200
    state-store:
      type: JDBC
    metadata:
      enabled: true
      jdbc-url: "jdbc:postgresql://pg-meta:5432/pangrova_metadata"
      cache:
        l1-max-size: 50000
        l1-ttl: "120s"
    observability:
      enabled: true
    security:
      enabled: true
  worker: |
    worker:
      host: "0.0.0.0"
      port: 8081
      heartbeat-interval: "10s"
      transport:
        port: 9090
      shuffle:
        buffer-capacity: 300
        max-memory-bytes: 4294967296
        spill-threshold: 0.75
      memory:
        budget-bytes: 17179869184
        system-reserved-bytes: 1073741824
        node-bytes: 51539607552
      threads:
        query: 16
        io: 4
        scheduler: 2
      spill:
        directory: "/data/pangrova-spill"
      registration:
        max-attempts: 15
    observability:
      enabled: true
      service-name: "pangrova-worker"
      otlp-endpoint: "http://otel-collector:4317"
    security:
      enabled: true

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

Install with:

```bash
helm install pangrova deploy/helm/pangrova -n pangrova --create-namespace -f production-values.yaml
```

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
| `coordinator.javaOpts` | string | `-Xms2g -Xmx8g` | Coordinator JVM options |
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
| Workers not registering | Check Worker logs for registration errors | Verify `WORKER_COORDINATOR_URL` env var points to Coordinator Service |
| HPA not scaling | `kubectl describe hpa` — check metrics | Ensure metrics-server is installed: `kubectl top pods -n pangrova` |
| ConfigMap not updating | Pods use old config after `helm upgrade` | Restart pods: `kubectl rollout restart statefulset/pangrova-worker -n pangrova` |
| Ingress not routing | `kubectl describe ingress -n pangrova` | Check ingress controller logs, verify TLS secret exists |
| PVC stuck in `Pending` | `kubectl describe pvc` | Check StorageClass exists and has available capacity |

---

## Multi-Tenancy — Namespace Isolation

The Helm chart supports per-tenant namespace isolation for production multi-tenant deployments.

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
helm install pangrova deploy/helm/pangrova -n pangrova --create-namespace -f multi-tenant-values.yaml
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
