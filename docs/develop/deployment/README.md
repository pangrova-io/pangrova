---
title: Deployment Guide
sidebar_position: 1
---

# Pangrova — Deployment Guide

This directory contains all deployment artifacts for the Pangrova distributed SQL query engine.

## Deployment Options

| Method | Use Case | Guide |
|--------|----------|-------|
| **Bare-metal / VM** | Production distributed deployment with S3/OSS/MinIO | [DISTRIBUTED-DEPLOYMENT.md](DISTRIBUTED-DEPLOYMENT.md) |
| **Docker Compose** | Development and testing | See [Quick Start](#quick-start) below |
| **Kubernetes (Helm)** | Cloud-native production deployment | See [Helm Deployment](#helm-kubernetes) below |

**New to Pangrova?** Start with [DISTRIBUTED-DEPLOYMENT.md](DISTRIBUTED-DEPLOYMENT.md) for a step-by-step walkthrough of a full cluster deployment with object storage.

## Directory Structure

```
docs/develop/deployment/
├── DISTRIBUTED-DEPLOYMENT.md  End-to-end bare-metal deployment guide (S3/OSS/MinIO)
├── PRODUCTION-CHECKLIST.md    Pre-go-live validation checklist
├── docker/
│   ├── Dockerfile                  Multi-stage build (builder + JRE runtime)
│   ├── Dockerfile.native-build     Native library reproducible build environment
│   ├── docker-compose.yml          Single-node dev cluster (1 Coordinator + 1 Worker)
│   ├── docker-compose-cluster.yml  Distributed cluster (2 Coordinators + 3 Workers)
│   └── .env.example                Environment variable defaults for docker-compose
├── helm/pangrova/
│   ├── Chart.yaml                  Helm chart metadata
│   ├── values.yaml                 Default configuration values
│   └── templates/
│       ├── coordinator-deployment.yaml
│       ├── worker-statefulset.yaml
│       ├── service.yaml
│       ├── configmap.yaml
│       ├── namespace.yaml
│       └── hpa.yaml                Worker HorizontalPodAutoscaler
└── README.md                       This file
```

## Quick Start

### Docker — Single Node (Development)

```bash
cd docs/develop/deployment/docker
cp .env.example .env        # adjust if needed
docker compose up -d
docker compose ps            # verify both services are healthy
```

Coordinator is available at `http://localhost:8080`. Worker registers automatically.

To stop:

```bash
docker compose down
```

### Docker — Distributed Cluster

```bash
cd docs/develop/deployment/docker
docker compose -f docker-compose-cluster.yml up -d
docker compose -f docker-compose-cluster.yml ps
```

This starts 2 Coordinators (ports 8080, 8082) and 3 Workers (ports 8081, 8083, 8084).

### Docker — Build Image from Source

```bash
# From project root
docker build -t pangrova/pangrova:latest -f docs/develop/deployment/docker/Dockerfile .

# Multi-arch build (requires Docker Buildx)
docker buildx build --platform linux/amd64,linux/arm64 \
    -t pangrova/pangrova:latest -f docs/develop/deployment/docker/Dockerfile .
```

### Kubernetes — Helm Chart {#helm-kubernetes}

```bash
# Install
helm install pangrova docs/develop/deployment/helm/pangrova -n pangrova --create-namespace

# Custom values
helm install pangrova docs/develop/deployment/helm/pangrova -n pangrova --create-namespace \
    --set coordinator.replicas=2 \
    --set worker.replicas=5 \
    --set image.tag=0.1.0

# Upgrade
helm upgrade pangrova docs/develop/deployment/helm/pangrova -n pangrova

# Uninstall
helm uninstall pangrova -n pangrova
```

## Configuration

All services read configuration from `/opt/pangrova/config/` inside the container. Override via:

- Environment variables (`PANGROVA_JAVA_OPTS`, `WORKER_COORDINATOR_URL`, etc.)
- Helm `values.yaml` — `config.coordinator` and `config.worker` embed full YAML content
- Kubernetes ConfigMap — mounted as `coordinator.yml` / `worker.yml`

See [Configuration Reference](../configuration/CONFIG.md) for the complete configuration reference.

## Health Checks

All services expose `GET /v1/health` for readiness and liveness probes.

| Component   | Default Port | Health Endpoint                    |
|-------------|-------------|------------------------------------|
| Coordinator | 8080        | `http://localhost:8080/v1/health`  |
| Worker      | 8081        | `http://localhost:8081/v1/health`  |

## Image Versioning

Docker Compose files use `${PANGROVA_VERSION:-latest}`. Override via:

```bash
PANGROVA_VERSION=0.1.0 docker compose up -d
```

Helm chart uses `image.tag` in `values.yaml`.

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
helm install pangrova docs/develop/deployment/helm/pangrova -n pangrova --create-namespace -f production-values.yaml
```

### Override Configuration via --set-file

Inject external config files directly:

```bash
helm install pangrova docs/develop/deployment/helm/pangrova -n pangrova \
  --set-file config.coordinator=docs/develop/configuration/coordinator.yml \
  --set-file config.worker=docs/develop/configuration/worker.yml
```

### Scaling Workers

```bash
# Manual scale
kubectl scale statefulset pangrova-worker -n pangrova --replicas=10

# Or via Helm
helm upgrade pangrova docs/develop/deployment/helm/pangrova -n pangrova --set worker.replicas=10

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
| `coordinator.replicas` | int | `2` | Number of Coordinator pods |
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
helm install pangrova docs/develop/deployment/helm/pangrova -n pangrova --create-namespace -f multi-tenant-values.yaml
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
./docs/develop/deployment/helm/pangrova/tests/test-namespace-isolation.sh

# Cleanup after testing
./docs/develop/deployment/helm/pangrova/tests/test-namespace-isolation.sh --cleanup
```
