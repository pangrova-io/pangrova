---
title: "Distributed Architecture"
sidebar_label: "Distributed Architecture"
---

# Pangrova — Distributed Architecture

> **Accuracy scope.** This is a living architecture document. Content explicitly marked
> draft, proposed, planned, phase, target, or remaining work is design intent rather
> than implemented support. For current behavior, code wiring and end-to-end tests
> take precedence.

> **Summary.** The Coordinator–Worker topology, multi-active high availability and leader election, worker registration and health checking, and shuffle durability.

## 1. Coordinator High-Availability Architecture

### 1.1 Multi-Active Architecture Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Coordinator Cluster (Multi-Active)                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐             │
│  │  Coordinator-1  │  │  Coordinator-2  │  │  Coordinator-3  │             │
│  │  (Active)       │  │  (Active)       │  │  (Active)       │             │
│  │                 │  │                 │  │                 │             │
│  │ ┌─────────────┐ │  │ ┌─────────────┐ │  │ ┌─────────────┐ │             │
│  │ │Query Handler│ │  │ │Query Handler│ │  │ │Query Handler│ │             │
│  │ │SQL Parser   │ │  │ │SQL Parser   │ │  │ │SQL Parser   │ │             │
│  │ │Optimizer    │ │  │ │Optimizer    │ │  │ │Optimizer    │ │             │
│  │ │Scheduler    │ │  │ │Scheduler    │ │  │ │Scheduler    │ │             │
│  │ └─────────────┘ │  │ └─────────────┘ │  │ └─────────────┘ │             │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘             │
│           │                    │                    │                       │
│           └────────────────────┼────────────────────┘                       │
│                                │                                            │
│  ┌─────────────────────────────┴─────────────────────────────┐             │
│  │                    Shared State Layer                      │             │
│  │                                                            │             │
│  │  ┌──────────────────┐  ┌──────────────────┐               │             │
│  │  │    ZooKeeper     │  │   Redis Cluster  │               │             │
│  │  │                  │  │                  │               │             │
│  │  │ • Leader Election│  │ • Session Cache  │               │             │
│  │  │ • Config Sync    │  │ • Metadata Cache │               │             │
│  │  │ • Distributed    │  │ • Query Plan     │               │             │
│  │  │   Lock           │  │   Cache          │               │             │
│  │  └──────────────────┘  └──────────────────┘               │             │
│  │                                                            │             │
│  │  ┌──────────────────────────────────────────────────────┐ │             │
│  │  │                PostgreSQL Cluster                      │ │             │
│  │  │ • Query State Persistence                             │ │             │
│  │  │ • Task Assignment History                             │ │             │
│  │  │ • Audit Logs                                          │ │             │
│  │  └──────────────────────────────────────────────────────┘ │             │
│  └───────────────────────────────────────────────────────────┘             │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Coordinator Core Components

```java
public class PangrovaCoordinator implements Lifecycle {

    private final CoordinatorConfig config;
    private final QueryManager queryManager;
    private final WorkerManager workerManager;
    private final MetadataService metadataService;
    private final SchedulerService schedulerService;
    private final HACoordinationService haService;

    private final HttpServer httpServer;

    @Override
    public void start() {
        haService.start();
        haService.registerAsCandidate();
        metadataService.initialize();
        workerManager.startDiscovery();
        httpServer.start();
    }
}
```

### 1.3 Query Lifecycle Management

```
┌─────────────────────────────────────────────────────────────────┐
│                      Query Lifecycle                             │
├─────────────────────────────────────────────────────────────────┤
│  Client Request                                                  │
│       │                                                          │
│       ▼                                                          │
│  ┌─────────────┐                                                │
│  │   QUEUED    │  → Query enqueued, waiting for resources         │
│  └──────┬──────┘                                                │
│         │         ╲                                              │
│         ▼          ╲ (cancel / resource timeout)                 │
│  ┌─────────────┐    ╲                                           │
│  │  PLANNING   │  → SQL parse → optimize → generate execution plan │
│  └──────┬──────┘    ╲                                           │
│         │            ╲ (cancel / syntax error / permission)      │
│         ▼             ╲                                          │
│  ┌─────────────┐       ╲                                        │
│  │ SCHEDULING  │  → Assign Splits → schedule Tasks                │
│  └──────┬──────┘        ╲                                       │
│         │                ╲ (cancel / no available Worker)        │
│         ▼                 ▼                                      │
│  ┌─────────────┐   ┌───────────┐                                │
│  │  RUNNING    │   │ CANCELLED │  ← User cancel (POST /v1/query/{id}/cancel) │
│  └──────┬──────┘   └───────────┘    or system cancel              │
│         │          ╱                                             │
│    ┌────┴────┐   ╱ (cancel during execution)                    │
│    ▼         ▼ ╱                                                │
│ ┌──────┐ ┌──────┐                                               │
│ │FINISH│ │FAILED│  ← Unrecoverable error                          │
│ └──────┘ └──────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```


**State Transition Table**:

| From | To | Trigger |
|------|----|---------|
| QUEUED | PLANNING | Resources available, dequeued |
| QUEUED | FAILED | Resource acquisition timeout |
| QUEUED | CANCELLED | User cancel (`POST /v1/query/{id}/cancel`) |
| PLANNING | SCHEDULING | Plan generated successfully |
| PLANNING | FAILED | Syntax error, permission denied, optimizer failure |
| PLANNING | CANCELLED | User cancel |
| SCHEDULING | RUNNING | All Tasks dispatched |
| SCHEDULING | FAILED | No available Worker (after retry exhaustion) |
| SCHEDULING | CANCELLED | User cancel |
| RUNNING | FINISH | All Tasks completed successfully |
| RUNNING | FAILED | Unrecoverable task failure (§4.1) |
| RUNNING | CANCELLED | User cancel or system-initiated cancel (e.g., quota exceeded) |

Terminal states: `FINISH`, `FAILED`, `CANCELLED`. No transitions out of terminal states.

### 1.4 Coordinator Module Structure

```
pangrova-coordinator/
├── src/main/java/org/pangrova/coordinator/
│   ├── PangrovaCoordinator.java           # Main entry point
│   ├── CoordinatorConfig.java           # Configuration
│   ├── server/                          # Service layer
│   │   └── http/                        # HTTP REST API
│   ├── query/                           # Query management
│   │   ├── QueryManager.java
│   │   ├── QueryStateMachine.java
│   │   └── QueryTracker.java
│   ├── scheduler/                       # Scheduler
│   │   ├── SchedulerService.java
│   │   ├── StageScheduler.java
│   │   └── strategy/                    # Scheduling strategies
│   ├── worker/                          # Worker management
│   │   ├── WorkerManager.java
│   │   └── WorkerRegistry.java
│   ├── metadata/                        # Metadata management
│   │   ├── MetadataService.java
│   │   └── CatalogRegistry.java
│   ├── ha/                              # High availability
│   │   ├── HACoordinationService.java
│   │   └── LeaderElection.java
│   └── session/                         # Session management
│       └── SessionManager.java
```

---

## 2. Worker Architecture Design

### 2.1 Worker Internal Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Worker Node                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                         Java Layer (JVM)                               │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │  HTTP Server    │  │  Task Manager   │  │ Memory Manager  │       │  │
│  │  │  (Control)      │  │ • Task Queue    │  │ • Pool Mgmt     │       │  │
│  │  │ • Task Submit   │  │ • Task Executor │  │ • Spill Control │       │  │
│  │  │ • Status/Cancel │  │ • Progress Track│  │ • GC Tuning     │       │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │ Arrow Flight    │  │ Connector Mgr   │  │ Metrics Export  │       │  │
│  │  │ (Data Plane)    │  │ • Plugin Load   │  │ • Prometheus    │       │  │
│  │  │ • Shuffle/Result│  │ • ClassLoader   │  │ • OpenTelemetry │       │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │  │
│  └───────────────────────────────┬───────────────────────────────────────┘  │
│                                  │                                           │
│                    JNI Bridge (Arrow C Data Interface)                       │
│                                  │                                           │
│  ┌───────────────────────────────┴───────────────────────────────────────┐  │
│  │                         C++ Layer (Velox)                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │  Velox Engine   │  │   Native I/O    │  │Shuffle Partitioner│     │  │
│  │  │ • Plan Execute  │  │ • Parquet Read  │  │ • Partition     │       │  │
│  │  │ • Vectorized    │  │ • ORC Read      │  │ • Serialize     │       │  │
│  │  │ • SIMD Optimize │  │ • S3/HDFS       │  │ • In-Memory Only│       │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │  │
│  │  │  Memory Pool    │  │   Spill Mgr     │  │ Expression Eval │       │  │
│  │  │ • Arrow Buffer  │  │ • NVMe SSD      │  │ • Codegen       │       │  │
│  │  │ • Shared Mem    │  │ • Arrow IPC     │  │ • SIMD          │       │  │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘       │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Read/Write Separation Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Read/Write Separation                              │
├─────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      READ PATH (C++ Velox)                           │   │
│  │  Substrait Plan → Plan Parser → Velox Plan → Executor                │   │
│  │       ↓                                                              │   │
│  │  Native I/O (Parquet/ORC) ← Data Source Connector ← Split Info       │   │
│  │       ↓                                                              │   │
│  │  Arrow RecordBatch (Zero-Copy) → Arrow Flight → Next Stage           │   │
│  │                                                                      │   │
│  │  Characteristics: vectorization, SIMD, zero-copy, extreme performance │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                      WRITE PATH (Java)                               │   │
│  │  Write Request → Transaction Manager → Data Writer → Committer       │   │
│  │       ↓                                                              │   │
│  │  Iceberg/Paimon SDK → Manifest Writer → Metadata Update              │   │
│  │                                                                      │   │
│  │  Characteristics: full transaction support, ACID, upstream-compatible │   │
│  │                                                                      │   │
│  │  Transaction boundary:                                               │   │
│  │  ┌─ ACID BEGIN ──────────────────────────────────────────────────┐  │   │
│  │  │ 1. Begin Transaction (Coordinator)                            │  │   │
│  │  │ 2. Write Data Files (Worker Java TableWriter, parallel)         │  │   │
│  │  │ 3. Report Write Results (Workers → Coordinator)               │  │   │
│  │  │ 4. Commit Transaction (Coordinator, atomic operation)          │  │   │
│  │  └─ ACID END ────────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

Note: On the write path, the Worker-side Java `TableWriter` is responsible for flushing data files to storage and producing statistics; the Coordinator only handles transaction orchestration and metadata commit, and does not write data files directly.

### 2.3 Worker Module Structure

```
pangrova-worker/
├── src/main/java/org/pangrova/worker/
│   ├── PangrovaWorker.java                # Main entry point
│   ├── WorkerConfig.java                # Configuration
│   ├── server/                          # Service layer
│   │   ├── http/                        # HTTP REST API
│   │   └── flight/                      # Arrow Flight service
│   ├── task/                            # Task management
│   │   ├── TaskManager.java
│   │   └── TaskExecutor.java
│   ├── execution/                       # Execution layer
│   │   └── ExecutionManager.java
│   ├── memory/                          # Native/JVM memory coordination
│   │   ├── NativeMemoryCoordinator.java
│   │   └── NativeSpillBridge.java
│   ├── exchange/                        # Data exchange
│   │   └── ExchangeManager.java
│   ├── jni/                             # JNI bridge
│   │   ├── NativeRuntime.java
│   │   └── NativeLibraryLoader.java
│   ├── connector/                       # Connector management
│   │   └── ConnectorManager.java
│   └── registry/                        # Service registration
│       └── ServiceRegistry.java

pangrova-worker-native/                    # C++ native layer
├── CMakeLists.txt
├── src/
│   ├── jni/                             # JNI interface
│   ├── velox/                           # Velox integration
│   ├── io/                              # Native I/O
│   ├── memory/                          # Memory management
│   └── flight/                          # Arrow Flight
├── include/                             # Header files
└── test/                                # Tests
```

### 2.4 Worker Startup Sequence

Symmetric to the Coordinator startup sequence (§1.2 `PangrovaCoordinator.start()`), the Worker startup sequence defines the complete initialization flow from process launch to being ready to accept Tasks.

```java
public class PangrovaWorker implements Lifecycle {

    private final WorkerConfig config;
    private final NativeRuntime nativeBridge;
    private final ResourceManager resourceManager;   // pangrova-resource; see 11 §3
    private final ConnectorManager connectorManager;
    private final TaskManager taskManager;
    private final HttpServer httpServer;
    private final FlightServer flightServer;
    private final ServiceRegistry serviceRegistry;
    private final MetricsExporter metricsExporter;

    @Override
    public void start() {
        // Phase 1: Native Engine Initialization
        nativeBridge.loadLibrary();                  // 1. JNI System.loadLibrary("pangrova_native")
        nativeBridge.initializeVelox(config);         // 2. Velox engine init (thread pools, memory pools)
        // Note: initializeVelox() is a Java wrapper that internally calls
        // NativeRuntime.nativeInit(configJson) — see native-execution.md §5.3
        // for the JNI interface definition.

        // Phase 2: Java Layer Initialization
        resourceManager.initialize(config);           // 3. Memory pools (JVM Heap + Off-Heap + Native budget)
        connectorManager.loadPlugins(config);         // 4. Connector plugins (ClassLoader isolation)
        taskManager.initialize();                     // 5. Task queue + executor threads

        // Phase 3: Network Services
        flightServer.start();                         // 6. Arrow Flight server (Data Plane, Shuffle + Result)
        httpServer.start();                           // 7. HTTP server (Control Plane, Task submit/status/cancel)
        metricsExporter.start();                      // 8. Prometheus + OpenTelemetry exporters

        // Phase 4: Cluster Registration
        serviceRegistry.register();                   // 9. ZooKeeper ephemeral node (§3.3)
        serviceRegistry.startHeartbeat();             // 10. Periodic heartbeat with resource feedback (§5.2)
    }

    @Override
    public void stop() {
        // Reverse order: deregister first, then drain, then shutdown
        serviceRegistry.deregister();                 // 1. Remove ZK ephemeral node (stop receiving new tasks)
        taskManager.drainAndWait(config.getShutdownGracePeriod()); // 2. Wait for running tasks to complete
        httpServer.stop();                            // 3. Stop control plane
        flightServer.stop();                          // 4. Stop data plane
        metricsExporter.stop();                       // 5. Flush final metrics
        connectorManager.shutdown();                  // 6. Close connector resources
        nativeBridge.shutdown();                      // 7. Release native memory pools, destroy Velox engine
        resourceManager.shutdown();                   // 8. Final memory cleanup
    }
}
```

**Startup Sequence Diagram**

```
PangrovaWorker.start()
    │
    ├── Phase 1: Native Engine ──────────────────────────────────────────────
    │   ├── loadLibrary()              ← JNI: System.loadLibrary("pangrova_native")
    │   │                                 Failure → abort startup (fail-fast)
    │   └── initializeVelox()          ← Create Velox MemoryPool, IO ThreadPool,
    │                                     Compute ThreadPool, SpillManager
    │                                     Internally calls NativeRuntime.nativeInit(configJson)
    │                                     (see native-execution.md §5.3)
    │                                     Failure → abort startup (fail-fast)


    │
    ├── Phase 2: Java Layer ─────────────────────────────────────────────────
    │   ├── resourceManager.initialize() ← Set Native memory limit (resource-management.md §2)
    │   │                                 Register JVM GC listener
    │   ├── connectorManager.loadPlugins() ← Scan plugin directory, create isolated ClassLoader
    │   │                                     (connectors.md §2.3)
    │   └── taskManager.initialize()   ← Create Task queue, Pull Loop threads
    │
    ├── Phase 3: Network Services ───────────────────────────────────────────
    │   ├── flightServer.start()       ← Bind Data Plane port (default 8082)
    │   │                                 Now able to accept Shuffle/Result connections
    │   ├── httpServer.start()         ← Bind Control Plane port (default 8080)
    │   │                                 Now able to accept Task submissions
    │   └── metricsExporter.start()    ← Prometheus /metrics endpoint ready
    │
    └── Phase 4: Cluster Registration ──────────────────────────────────────
        ├── serviceRegistry.register() ← ZK creates /pangrova/workers/{workerId}
        │                                 ephemeral node (§3.3)
        │                                 Coordinator discovers this Worker via Watch
        └── startHeartbeat()           ← Report resource status every 10s (§5.2)
                                          Includes memory_pressure, running_task_count,
                                          available_memory_bytes, cpu_usage
```

**Startup Sequence Constraints**

| Constraint | Description | Rationale |
|------|------|------|
| Native must be initialized before the Java layer | `nativeBridge.loadLibrary()` is the first step | The Java `ResourceManager` needs to know the Native memory pool capacity |
| Network must start before Registration | Register with ZK only after the Flight + HTTP ports are ready | Prevents the Coordinator from sending Tasks after discovering a Worker whose ports are not yet ready |
| Registration is the last step | ZK registration means "ready to accept work" | Ensures a Worker is only exposed after it is fully ready |
| Any Phase 1/2 failure → fail-fast | Do not register with ZK; exit the process | Prevents a half-initialized Worker from joining the cluster |

**Coordination with Coordinator Startup**

```
Timeline →

ZooKeeper          Coordinator                    Worker
    │                  │                             │
    │  ZK Ready        │                             │
    ├─────────────────►│                             │
    │                  │ start()                     │
    │                  │ ├── haService.start()       │
    │                  │ ├── metadataService.init()  │
    │                  │ ├── workerManager.          │
    │                  │ │   startDiscovery()        │
    │                  │ │   (Watch /pangrova/workers) │
    │                  │ └── httpServer.start()      │
    │                  │                             │
    │                  │                             │ start()
    │                  │                             │ ├── Phase 1-3 (internal init)
    │                  │                             │ └── Phase 4: register()
    │                  │                             │     → ZK: create ephemeral node
    │                  │  CHILD_ADDED event           │
    │                  │◄─────────────────────────────┤
    │                  │                             │
    │                  │ Coordinator discovers Worker │
    │                  │ Adds to scheduling candidates│
    │                  │                             │
    │                  │ Waiting for first Heartbeat  │
    │                  │◄──────────────── heartbeat ──┤
    │                  │                             │
    │                  │ Worker state = ACTIVE        │
    │                  │ Can schedule Task            │
    │                  │                             │
```

> **Cross-Reference**: For Native engine initialization details see [native-execution.md](./native-execution.md); for memory pool initialization see [resource-management.md §2](./resource-management.md); for Connector plugin loading see [connectors.md §2.3](./connectors.md); for heartbeat resource feedback see [§5.2](#52-scheduling-resource-closed-loop).

---

## 3. High Availability Design

### 3.1 Overall HA Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           High Availability Architecture                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         Client Layer                                     │   │
│  │  ┌─────────────┐                                                        │   │
│  │  │ Load        │  • Health check • Failover • Session affinity (optional)│   │
│  │  │ Balancer    │                                                        │   │
│  │  └──────┬──────┘                                                        │   │
│  └─────────┼────────────────────────────────────────────────────────────────┘   │
│            ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Coordinator Cluster (Multi-Active)                    │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │   │
│  │  │Coordinator-1│  │Coordinator-2│  │Coordinator-3│                     │   │
│  │  │   (Active)  │  │   (Active)  │  │   (Active)  │                     │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                     │   │
│  │         └────────────────┼────────────────┘                             │   │
│  │  ┌───────────────────────┴───────────────────────────────────────────┐ │   │
│  │  │                    Shared State Layer                              │ │   │
│  │  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │ │   │
│  │  │  │     ZooKeeper    │  │      Redis       │  │   PostgreSQL     │  │ │   │
│  │  │  │• Leader election │  │• Metadata cache  │  │• Query state     │  │ │   │
│  │  │  │• Distributed lock│  │• Session state   │  │• Audit logs      │  │ │   │
│  │  │  └──────────────────┘  └──────────────────┘  └──────────────────┘  │ │   │
│  │  └────────────────────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
│            ▼                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Worker Cluster (Auto-Scaling)                         │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐   │   │
│  │  │  Worker-1   │  │  Worker-2   │  │  Worker-3   │  │  Worker-N   │   │   │
│  │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘   │   │
│  │         └────────────────┴────────────────┴────────────────┘          │   │
│  │                          ┌────────▼────────┐                          │   │
│  │                          │ Service Registry│                          │   │
│  │                          │ (ZooKeeper)     │                          │   │
│  │                          │ • Ephemeral Node│                          │   │
│  │                          │ • Health check  │                          │   │
│  │                          │ • Discovery     │                          │   │
│  │                          └─────────────────┘                          │   │
│  └──────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 Query Ownership Management

```java
/**
 * Query ownership management
 */
public class QueryOwnershipManager {

    private final CuratorFramework curator;
    private final String coordinatorId;
    private final QueryStateStore stateStore;

    /**
     * Acquire query ownership
     */
    public boolean acquireOwnership(QueryId queryId) {
        String path = "/pangrova/queries/" + queryId + "/owner";
        try {
            curator.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(path, coordinatorId.getBytes());
            return true;
        } catch (KeeperException.NodeExistsException e) {
            return false;
        }
    }

    /**
     * Watch for orphan queries (owner has crashed)
     *
     * [DEF-020 fix] Use TreeCache instead of PathChildrenCache to detect
     * deletion of the ephemeral `/owner` child node.  PathChildrenCache on
     * `/pangrova/queries` only sees {queryId}-level changes, but the ephemeral
     * node created by acquireOwnership() lives at
     * `/pangrova/queries/{queryId}/owner`.  When a Coordinator's ZK session
     * expires, ZK deletes the `/owner` ephemeral node — not the persistent
     * `{queryId}` node — so PathChildrenCache CHILD_REMOVED never fires.
     * TreeCache watches the full subtree and correctly detects `/owner`
     * removal.
     */
    public void watchOrphanQueries(Consumer<QueryId> handler) {
        TreeCache cache = TreeCache.newBuilder(curator, "/pangrova/queries")
            .setCacheData(true)
            .setMaxDepth(2)   // /pangrova/queries/{queryId}/owner
            .build();

        cache.getListenable().addListener((client, event) -> {
            if (event.getType() == TreeCacheEvent.Type.NODE_REMOVED) {
                String removedPath = event.getData().getPath();
                // Only react to /owner node removal (session expiry)
                if (removedPath.endsWith("/owner")) {
                    String queryIdStr = removedPath.split("/")[3]; // /pangrova/queries/{queryId}/owner
                    QueryId queryId = QueryId.valueOf(queryIdStr);
                    if (acquireOwnership(queryId)) {
                        handler.accept(queryId);
                    }
                }
            }
        });
        cache.start();
    }
}
```

### 3.3 Worker Service Registration

```java
/**
 * Worker service registration (ZooKeeper)
 */
public class WorkerServiceRegistry {

    private final CuratorFramework curator;
    private final WorkerInfo workerInfo;

    private static final long SESSION_TIMEOUT_MS = 30000;
    private static final long HEARTBEAT_INTERVAL_SECONDS = 10;

    public void register() throws Exception {
        // 1. Create an Ephemeral Node (auto-deleted when the session disconnects)
        String path = "/pangrova/workers/" + workerInfo.getId();
        String value = objectMapper.writeValueAsString(workerInfo);

        curator.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.EPHEMERAL)
            .forPath(path, value.getBytes(StandardCharsets.UTF_8));

        // 2. Start heartbeat (periodically update Worker state)
        startHeartbeat();
    }

    private void startHeartbeat() {
        scheduler.scheduleAtFixedRate(() -> {
            try {
                String path = "/pangrova/workers/" + workerInfo.getId();
                String value = objectMapper.writeValueAsString(workerInfo);
                curator.setData().forPath(path, value.getBytes(StandardCharsets.UTF_8));
            } catch (Exception e) {
                log.error("Heartbeat failed", e);
                tryReregister();
            }
        }, 0, HEARTBEAT_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }
}
```


> **Detection Timing**: Heartbeat timeout (20s = 2× interval, see §5.2.3) is the Coordinator's fast detection mechanism — marks Worker as SUSPECT and stops scheduling. ZK session expiry (30s) is the final failure confirmation — ephemeral node deletion triggers Task rescheduling. The two mechanisms are complementary: heartbeat provides fast response, ZK provides eventual consistency guarantee.

### 3.4 Shared State Responsibility Matrix

| Component | Responsibilities | Consistency Requirement | Failure Impact |
|------|------|-----------|----------|
| ZooKeeper | Leader election, distributed lock, service registry (ephemeral node), config sync, catalog change notification (Watch) | Strong consistency (ZAB protocol) | Cluster unavailable (quorum required) |
| Redis | Metadata cache, session cache, query plan cache, query result cache (L2), cross-Coordinator event broadcast (Pub/Sub), data cache (remote tier) | Eventually consistent (may be lost) | Performance degradation + potential stale security cache (bounded by TTL) |
| PostgreSQL | Query state persistence, audit logs (security + resource), DDL job state, tenant/quota data, **catalog metadata** ([07 §3.1](./metadata.md)), **security policies** (RBAC/RLS/column masks, [12 §3-5](./security.md)), **Shuffle Location Registry** (via `query_checkpoints.shuffle_locations`, eventually consistent — async persist) | Strong consistency (transactional) | **Unavailable**: new query state cannot persist, Coordinator failover degrades to query re-execution, authorization blocked, catalog registry read-only (from cache). **Data loss (disaster)**: audit logs and historical query state unrecoverable, Shuffle data requires recomputation. |








**Key design decisions**:
- ZooKeeper is the single source of consistency; all coordination operations that require strong consistency (leader election, distributed lock, service registry) uniformly use ZooKeeper
- Redis is used only for caching and does not store any consistency-critical state. When Redis is unavailable, system functionality is unaffected — only performance degrades
- Worker service registration uses a ZooKeeper Ephemeral Node; when a Worker crashes the node is automatically deleted, requiring no additional health-check mechanism


**Redis Consistency Refinement**: The "lossy" semantics for Redis apply to cached data only — cache misses result in re-computation, not data loss. However, cache *invalidation* operations (especially security-policy-triggered invalidations from [05 §3.3](./query-optimization.md)) must be treated as best-effort with a safety fallback: all Redis cache entries carry a mandatory TTL upper bound (default: 1 hour). On Redis restart, all Query Result Cache entries are purged (Redis configured with `save ""` to disable RDB persistence for cache databases). This ensures that stale cache entries from a previous security policy epoch cannot survive a Redis restart, preventing cross-security-boundary data leakage.

### 3.5 Multi-Active Semantics

The Pangrova Coordinator cluster uses a **Full Multi-Active** model: all Coordinators accept both read and write requests simultaneously, with no primary/secondary distinction. Clients can route requests to any Coordinator via a Load Balancer.

#### 3.5.1 Multi-Active Definition

| Operation Type | Requires Leader | Note |
|----------|------------|------|
| Query reception and execution (SELECT/INSERT/UPDATE/DELETE) | No | Any Coordinator receives and is responsible end-to-end |
| DDL operations (CREATE/ALTER/DROP TABLE) | Yes | Leader coordinates to avoid concurrent DDL conflicts |
| Cluster config change (SET GLOBAL) | Yes | Leader broadcasts config to all nodes |
| Query ownership migration | No | Auto-detected via ZooKeeper ephemeral node, no Leader involvement needed |

**Relationship with Leader Election**: Leader Election (see 3.1 HA architecture) is used only for DDL coordination and cluster config broadcast. The Leader is not a traffic entry point and does not take on additional query load. When the Leader crashes, the remaining Coordinators automatically elect a new Leader; during this period DDL operations are temporarily unavailable, but read/write queries are unaffected.



**DDL Request Routing Strategy**: When a client sends a DDL request to a non-Leader Coordinator via the Load Balancer, the non-Leader transparently forwards the request to the current Leader. The forwarding is invisible to the client. If the Leader is unavailable (e.g., during leader election), the non-Leader returns `SERVICE_UNAVAILABLE` (HTTP 503) with a `Retry-After` header. The client should retry after the specified interval. During leader election (timeout: 30s, aligned with ZK session timeout), all DDL operations return `LEADER_NOT_AVAILABLE` error code. In-flight DDL operations that were submitted but not yet committed are recovered by the new Leader from the PostgreSQL DDL job state table (`ddl_jobs`).

#### 3.5.2 Query Ownership Migration Flow

When a Coordinator crashes, the in-flight queries it owns must be taken over by other Coordinators. The migration flow is auto-detected via ZooKeeper ephemeral nodes and requires no Leader coordination.

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     Query Ownership Migration Flow                               │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Normal operation:                                                              │
│  Coordinator-A creates ephemeral node: /pangrova/queries/{queryId}/owner          │
│                                                                                  │
│  Failure detection:                                                              │
│  Coordinator-A crashes → ZK session expires → ephemeral node auto-deleted      │
│                                                                                  │
│  Competitive takeover:                                                           │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                 │
│  │ Coordinator-B   │  │ Coordinator-C   │  │ Coordinator-D   │                 │
│  │ TreeCache        │  │ TreeCache        │  │ TreeCache        │                │
│  │ watches /owner   │  │ watches /owner   │  │ watches /owner   │                 │
│  │ node-deleted evt│  │ node-deleted evt│  │ node-deleted evt│                 │
│  │       │         │  │       │         │  │       │         │                 │
│  │       ▼         │  │       ▼         │  │       ▼         │                 │
│  │ acquireOwnership│  │ acquireOwnership│  │ acquireOwnership│                 │
│  │   (create ephemeral│  │   (create ephemeral│  │   (create ephemeral│                │
│  │    node race)   │  │    node race)   │  │    node race)   │                 │
│  │       │         │  │       │         │  │       │         │                 │
│  │    ✓ success    │  │    ✗ failure   │  │    ✗ failure   │                 │
│  └─────────────────┘  └─────────────────┘  └─────────────────┘                 │
│                                                                                  │
│  State recovery (Coordinator-B):                                                 │
│  1. Load QueryCheckpoint from PostgreSQL (query state, Stage progress, Task assignment) │
│  2. Check whether the Worker hosting each Task is alive (via ZK service registry) │
│  3. Alive Worker → continue monitoring Task execution                           │
│  4. Crashed Worker → reschedule unfinished Splits to other Workers              │
│  5. Restore Shuffle location info; downstream Stage continues consuming         │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**Detailed migration steps**:

| Step | Action | Component | Note |
|------|------|------|------|
| 1 | Failure detection | ZooKeeper | Coordinator-A session expires, ephemeral node `/pangrova/queries/{queryId}/owner` auto-deleted |
| 2 | Event notification | ZooKeeper → TreeCache | All alive Coordinators receive NODE_REMOVED event (watching `/owner` child-node deletion)  |
| 3 | Competitive acquisition | Coordinator-B/C/D | Call `acquireOwnership()` to create a new ephemeral node; ZK guarantees only one succeeds |
| 4 | State loading | Coordinator-B (winner) | Load query execution state from the PostgreSQL `query_checkpoints` table |
| 5 | Task recovery | Coordinator-B | Check Worker liveness, reschedule unfinished Splits of failed Tasks |

**In-Flight Query Handling**:
- **QUEUED/PLANNING state queries**: re-executed from scratch after takeover (low cost)
- **SCHEDULING/RUNNING state queries**: recovered from PostgreSQL checkpoint, only unfinished Splits are rescheduled
- **Completed but not-yet-returned queries**: the client reconnects and fetches results from the new Coordinator (results cached in Redis). **Note**: Since Redis is configured as lossy cache (§3.4), if Redis is unavailable or the cached result has been evicted, the completed query result is unrecoverable and the client must re-submit the query. §3.4 failure impact for Redis is updated accordingly.




**Zombie Query Fencing**: When a Coordinator recovers from a false-death scenario (e.g., GC pause or network partition causing ZK session expiry), it must not continue operating on queries whose ownership has been migrated. The fencing mechanism works as follows:

1. Each ownership migration increments an `ownership_epoch` counter in PostgreSQL (`query_checkpoints.ownership_epoch`).
2. The new owner Coordinator includes the current epoch in all instructions sent to Workers.
3. Workers reject any instruction with an epoch lower than the last seen epoch for that query, returning `STALE_EPOCH` error.
4. A Coordinator that detects its ZK session has expired must immediately stop all local query processing and discard local query state. It re-registers with ZK and can only accept new queries after re-registration.

---

## 4. Task Fault Tolerance

### 4.1 Task Retry Strategy

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Task Fault Tolerance                                   │
├─────────────────────────────────────────────────────────────────────────────────┤
│  Task Failure Scenarios:                                                         │
│  1. Worker crash → Detection: ZooKeeper Ephemeral Node deleted → reschedule all tasks on that Worker │
│  2. Task execution failure → Detection: Worker reports failure → decide whether to retry per the retry policy │
│  3. Network partition → Detection: heartbeat timeout → mark Worker as SUSPECT, wait for recovery or reschedule after timeout │
│  4. Data-skew timeout → Detection: execution time exceeds threshold → speculative execution (Speculative Execution, see [§8](#8-speculative-execution-opt-003))    │

├─────────────────────────────────────────────────────────────────────────────────┤
│  Retry Policy:                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Retryable errors: Worker crash, network timeout, transient I/O error, OOM │   │
│  │  Non-retryable errors: syntax error, permission error, data-format error, user cancel │   │
│  │  Retry parameters: maxRetries=3, retryBackoff=1s/2s/4s, retryTimeout=5min │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Split-Level Retry

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Split-Level Retry Design                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: Task-level retry is too coarse-grained; one Split failure causes the entire Task to rerun │
│  Solution: support Split-level retry; completed Split results can be reused           │
│                                                                                  │
│  Task internal state:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Task-1 (10 Splits)                                                      │   │
│  │  ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐                     │   │
│  │  │Split-1│ │Split-2│ │Split-3│ │Split-4│ │Split-5│                     │   │
│  │  │  ✓    │ │  ✓    │ │  ✗    │ │  ◐    │ │  ○    │  ...                │   │
│  │  │DONE   │ │DONE   │ │FAILED │ │RUNNING│ │PENDING│                     │   │
│  │  └───────┘ └───────┘ └───────┘ └───────┘ └───────┘                     │   │
│  │                         │                                                │   │
│  │                         ▼ Retry Split-3 only                            │   │
│  │                    ┌───────┐                                            │   │
│  │                    │Split-3│ → schedule to another Worker              │   │
│  │                    │RETRY  │                                            │   │
│  │                    └───────┘                                            │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Split state machine:                                                            │
│  PENDING → RUNNING → DONE                                                       │
│              ↓                                                                   │
│           FAILED → RETRY → RUNNING → DONE                                       │
│              ↓                                                                   │
│           ABANDONED (retry count exceeded)                                      │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Split-level retry management
 */
public class SplitRetryManager {

    private final Map<SplitId, SplitState> splitStates = new ConcurrentHashMap<>();
    private final int maxSplitRetries = 3;

    public void handleSplitFailure(TaskId taskId, SplitId splitId, Throwable error) {
        SplitState state = splitStates.get(splitId);

        if (!isRetryable(error) || state.getRetryCount() >= maxSplitRetries) {
            state.setState(SplitStatus.ABANDONED);
            // Fail entire task only if split is abandoned
            failTask(taskId, splitId, error);
            return;
        }

        // Retry only this split
        state.incrementRetryCount();
        state.setState(SplitStatus.RETRY);

        // Schedule to different worker (avoid same failure)
        WorkerId failedWorker = state.getLastWorker();
        WorkerId newWorker = selectWorkerExcluding(failedWorker);

        scheduleSplit(taskId, splitId, newWorker);
    }

    /** Check if all splits are done */
    public boolean isTaskComplete(TaskId taskId) {
        return splitStates.values().stream()
            .filter(s -> s.getTaskId().equals(taskId))
            .allMatch(s -> s.getStatus() == SplitStatus.DONE);
    }
}
```

### 4.3 Shuffle Data Persistence

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Shuffle Persistence Architecture                         │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Design goals:                                                                   │
│  • Cross-Worker fault tolerance: Shuffle data recoverable after a Worker crash  │
│  • Low-latency first: the normal path adds no latency; persistence is asynchronous │
│  • Configurable: choose persistence strategy per query/Stage level              │
│                                                                                  │
│  Persistence modes:                                                              │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │  Mode              │ Latency│ Fault tol.     │ Use case                     │ │
│  │─────────────────────────────────────────────────────────────────────────────│ │
│  │  MEMORY_ONLY       │ Lowest │ None           │ Short queries, recomputable  │ │
│  │  MEMORY_AND_DISK   │ Low    │ Worker process │ Medium queries               │ │
│  │  MEMORY_AND_REMOTE │ Medium │ Cross-Worker   │ Long queries, critical tasks │ │
│  │  REMOTE_ONLY       │ High   │ Fully durable  │ Very large Shuffle, resumable│ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    MEMORY_AND_REMOTE Mode Data Flow                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Producer Worker (Stage N)                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  Velox Pipeline Output                                                   │   │
│  │         │                                                                │   │
│  │         ▼                                                                │   │
│  │  ┌─────────────────┐                                                    │   │
│  │  │Shuffle Partitioner│                                                   │   │
│  │  │ (Partitioner)   │                                                    │   │
│  │  └────────┬────────┘                                                    │   │
│  │           │                                                              │   │
│  │     ┌─────┴─────┐                                                       │   │
│  │     ▼           ▼                                                       │   │
│  │  ┌──────┐  ┌──────────┐                                                │   │
│  │  │Memory│  │Async S3  │  ← Async write, does not block the main path   │   │
│  │  │Buffer│  │Writer    │                                                │   │
│  │  └──┬───┘  └────┬─────┘                                                │   │
│  │     │           │                                                       │   │
│  │     │           ▼                                                       │   │
│  │     │    ┌─────────────┐                                               │   │
│  │     │    │ S3 Bucket   │                                               │   │
│  │     │    │ /shuffle/   │                                               │   │
│  │     │    │ {query_id}/ │                                               │   │
│  │     │    │ {stage_id}/ │                                               │   │
│  │     │    │ {part_id}/  │                                               │   │
│  │     │    └─────────────┘                                               │   │
│  │     ▼                                                                   │   │
│  │  ┌──────────────┐                                                      │   │
│  │  │ Arrow Flight │ ──────────────────────────────────────┐              │   │
│  │  │ (Primary)    │                                       │              │   │
│  │  └──────────────┘                                       │              │   │
│  └─────────────────────────────────────────────────────────┼───────────────┘   │
│                                                             │                   │
│  Consumer Worker (Stage N+1)                                ▼                   │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  ┌──────────────────┐                                                   │   │
│  │  │ Shuffle Reader   │                                                   │   │
│  │  │                  │                                                   │   │
│  │  │ 1. Try Flight    │ ← Read from memory first (low latency)           │   │
│  │  │ 2. Fallback S3   │ ← Read from S3 when Producer is down             │   │
│  │  └──────────────────┘                                                   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Shuffle persistence manager
 */
public class ShufflePersistenceManager {

    private final S3AsyncClient s3Client;
    private final String shuffleBucket;
    private final ExecutorService asyncWriteExecutor;

    /**
     * Asynchronously persist Shuffle data to S3
     *
     *
     * Data lifecycle: ShuffleDrainThread pulls a batch from Native via
     * nativeGetShuffleBatch() (Arrow C Data zero-copy). The batch is then:
     * (1) Synchronously serialized to Arrow IPC bytes (deep copy) on the
     *     ShuffleDrainThread — this decouples the batch lifetime from S3 write.
     * (2) The IPC bytes are submitted to asyncWriteExecutor for S3 upload.
     * (3) The original VectorSchemaRoot is enqueued into ShuffleBuffer for
     *     Flight Server streaming (normal path).
     * This ensures the S3 write does not hold a reference to the original
     * batch after serialization, avoiding use-after-free on Flight send.
     */
    public CompletableFuture<ShuffleLocation> persistAsync(
            QueryId queryId, StageId stageId, int partitionId,
            VectorSchemaRoot data) {

        String s3Key = String.format("shuffle/%s/%s/%d/%s.arrow",
            queryId, stageId, partitionId, UUID.randomUUID());

        return CompletableFuture.supplyAsync(() -> {
            // Serialize to Arrow IPC format
            byte[] arrowBytes = serializeToArrowIpc(data);

            // Upload to S3
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(shuffleBucket)
                    .key(s3Key)
                    .build(),
                AsyncRequestBody.fromBytes(arrowBytes)
            ).join();

            return new ShuffleLocation(ShuffleLocationType.S3, s3Key);
        }, asyncWriteExecutor);
    }

    /**
     * Recover Shuffle data from S3
     */
    public VectorSchemaRoot recover(ShuffleLocation location, BufferAllocator allocator) {
        byte[] arrowBytes = s3Client.getObject(
            GetObjectRequest.builder()
                .bucket(shuffleBucket)
                .key(location.getPath())
                .build(),
            AsyncResponseTransformer.toBytes()
        ).join().asByteArray();

        return deserializeFromArrowIpc(arrowBytes, allocator);
    }
}

/**
 * Shuffle reader - supports failover
 */
public class FaultTolerantShuffleReader {

    private final FlightClient flightClient;
    private final ShufflePersistenceManager persistenceManager;
    private final ShuffleLocationRegistry locationRegistry;

    public VectorSchemaRoot read(QueryId queryId, StageId stageId, int partitionId) {
        ShuffleLocation location = locationRegistry.getLocation(queryId, stageId, partitionId);

        // 1. Try primary path (Arrow Flight from producer worker)
        if (location.getType() == ShuffleLocationType.WORKER) {
            try {
                return readFromFlight(location.getWorkerId(), queryId, stageId, partitionId);
            } catch (FlightRuntimeException e) {
                if (isWorkerUnavailable(e)) {
                    log.warn("Producer worker unavailable, falling back to S3");
                    // Fall through to S3 recovery
                } else {
                    throw e;
                }
            }
        }

        // 2. Fallback to S3
        ShuffleLocation s3Location = locationRegistry.getS3Location(queryId, stageId, partitionId);
        if (s3Location != null) {
            return persistenceManager.recover(s3Location, allocator);
        }

        throw new ShuffleDataLostException(
            "Shuffle data lost: query=%s, stage=%s, partition=%d",
            queryId, stageId, partitionId);
    }
}
```

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Shuffle Cleanup Strategy                                 │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Cleanup timing:                                                                 │
│  • Query completes successfully: immediately clean all Shuffle data of the query │
│  • Query failed/cancelled: clean immediately                                     │
│  • Query timeout: clean                                                          │
│  • Scheduled cleanup: hourly scan, clean orphan data older than 24 hours         │
│                                                                                  │
│  S3 lifecycle policy:                                                            │
│  • Objects under the shuffle/ prefix are auto-deleted after 7 days (safety net)  │
│  • Configured via S3 Lifecycle Rules                                             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.4 Query Checkpoint

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Query Checkpoint Design                                  │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: after Coordinator failover, Running query state may be lost            │
│  Solution: periodically Checkpoint query execution progress to durable storage   │
│                                                                                  │
│  Checkpoint contents:                                                            │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │  QueryCheckpoint {                                                       │   │
│  │    queryId, state, startTime,                                           │   │
│  │    stages: [{stageId, state, completedSplits, totalSplits}],            │   │
│  │    tasks: [{taskId, workerId, state, splitStates}],                     │   │
│  │    shuffleLocations: [{stageId, partitionId, workerId, path}]           │   │
│  │  }                                                                       │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Checkpoint strategy:                                                            │
│  • Trigger timing: Stage completion, Task state change, periodic (30s)           │
│  • Storage location: PostgreSQL (query_checkpoints table)                        │
│  • Retention policy: kept for 1 hour after query completion, for auditing        │
│                                                                                  │
│  Failure recovery flow:                                                          │
│  1. New Coordinator starts, loads Checkpoints of unfinished queries              │
│  2. Check whether the Worker hosting each Task is alive                          │
│  3. Alive → keep monitoring; down → reschedule unfinished Splits                 │
│  4. Restore Shuffle location info; downstream Stage can keep consuming           │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Resource Management

### 5.1 Resource Queue & Scheduling

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Resource Queue & Scheduling                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: no priority control on contention; large queries may starve small ones │
│  Solution: introduce resource queues + fair scheduler                            │
│                                                                                  │
│  Resource queue hierarchy:                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                         Root Queue (100%)                                │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐       │   │   │
│  │  │  │  Production   │  │  Development  │  │    System     │       │   │   │
│  │  │  │    (60%)      │  │    (30%)      │  │    (10%)      │       │   │   │
│  │  │  │               │  │               │  │               │       │   │   │
│  │  │  │ ┌───┐ ┌───┐  │  │ ┌───┐ ┌───┐  │  │ ┌───────────┐ │       │   │   │
│  │  │  │ │ETL│ │BI │  │  │ │Ad │ │Test│  │  │ │  Admin    │ │       │   │   │
│  │  │  │ │40%│ │20%│  │  │ │hoc│ │   │  │  │ │  Queries  │ │       │   │   │
│  │  │  │ └───┘ └───┘  │  │ └───┘ └───┘  │  │ └───────────┘ │       │   │   │
│  │  │  └───────────────┘  └───────────────┘  └───────────────┘       │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Scheduling strategy:                                                            │
│  • Fair Scheduling: fair in-queue allocation; small queries not starved          │
│  • Preemption: high-priority queues can preempt low-priority queues              │
│  • Elasticity: idle queue resources can be borrowed by other queues              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

```java
/**
 * Resource queue configuration
 */
public class ResourceQueue {
    private final String name;
    private final double capacityPercent;      // Guaranteed capacity
    private final double maxCapacityPercent;   // Max borrowable capacity
    private final int maxConcurrentQueries;
    private final Duration queryTimeout;
    private final List<ResourceQueue> children;

    /** Match query to queue by user/source/tags */
    public static ResourceQueue matchQueue(QueryContext context) {
        // 1. Check explicit queue hint: SET SESSION resource_queue = 'etl'
        // 2. Match by user group
        // 3. Match by query source (jdbc/cli/bi-tool)
        // 4. Default queue
    }
}

/**
 * Fair scheduler
 */
public class FairScheduler {

    public WorkerResources allocate(QueryId queryId, ResourceRequest request) {
        ResourceQueue queue = getQueueForQuery(queryId);

        // 1. Check queue has available capacity
        if (!queue.hasCapacity(request)) {
            // Try borrow from parent/sibling queues
            if (!tryBorrowCapacity(queue, request)) {
                // Queue query, wait for resources
                return queueAndWait(queryId, request);
            }
        }

        // 2. Allocate from available workers
        return allocateFromWorkers(request);
    }
}
```

### 5.2 Scheduling-Resource Closed Loop

The Coordinator scheduler makes scheduling decisions based on the real-time resource feedback reported via Worker heartbeats, forming a scheduling–resource closed loop that prevents over-scheduling onto high-pressure Workers.

#### 5.2.1 Scheduling Rules by Memory Pressure

| Worker Memory Pressure | Scheduling Decision | Rationale |
|------------------------|---------------------|-----------|
| `CRITICAL` (>95% usage) | **Do not schedule any new tasks** | Worker is near OOM; new tasks would cause cascading failures |
| `HIGH` (85-95% usage) | **Only schedule small-memory tasks** (estimated < 1GB) | Reserve headroom for spill and memory fluctuation of running tasks |


**Task Memory Estimation Heuristics** (used for HIGH-pressure scheduling decisions):

| Method | Description | Accuracy |
|--------|-------------|----------|
| Split-size static estimate | `splitDataSize × expansionFactor` (default factor: 3× for Parquet, 5× for ORC due to decompression + columnar→row expansion) | Low — ignores operator type |
| Historical statistics | Average peak memory of same query pattern (matched by plan signature hash) from `query_stats` table | Medium — requires history |
| Plan-based rule estimate | Hash Join > Sort Merge Join > Broadcast Join; aggregation with high-cardinality GROUP BY adds 2× multiplier | Medium — heuristic |

The Scheduler applies these methods in priority order: historical → plan-based → split-size fallback. If no estimate is available, the task is conservatively treated as "large" and not scheduled to HIGH-pressure Workers.
| `MEDIUM` (70-85% usage) | Normal scheduling | Spill mechanism already active; can absorb new load |
| `NONE` (<50% usage) | Normal scheduling | Ample resources |
| `LOW` (50-70% usage) | Normal scheduling | Ample resources (monitoring active) |




#### 5.2.2 Scheduling Decision Flow

```
Coordinator Scheduler receives a new Task:
    │
    ▼
Iterate the candidate Worker list (obtained from the ZooKeeper Service Registry)
    │
    ├─ Worker state = SUSPECT / DEAD → skip
    │
    ├─ Worker memory_pressure = CRITICAL → skip
    │
    ├─ Worker memory_pressure = HIGH
    │       │
    │       ├─ Task estimated memory < 1GB → allow scheduling
    │       └─ Task estimated memory >= 1GB → skip
    │
    ├─ Worker memory_pressure = MEDIUM / NONE / LOW → allow scheduling
    │  (NONE and LOW have identical scheduling behavior; see [06 §3](./resource-management.md) for canonical enum)
    │
    ▼
From the allowed Workers, select the best Worker by the Fair Scheduling strategy
(considering running_task_count, available_memory_bytes, cpu_usage)
```

#### 5.2.3 Feedback Latency Tolerance

| Parameter | Value |
|-----------|-------|
| Heartbeat interval | 10s |
| Max state staleness | 20s |
| SUSPECT threshold | No heartbeat for >20s |

The Coordinator tolerates at most **20 seconds** of Worker state staleness. This means that after a Worker enters `CRITICAL`, the Coordinator stops scheduling new tasks to it within at most 20 seconds. A Worker from which no heartbeat has been received for more than 20 seconds is marked `SUSPECT` and no longer participates in scheduling.

> **Cross-Reference:** For Worker heartbeat resource-feedback field definitions (`MemoryPressureLevel`, `HeartbeatRequest` extensions) and the complete feedback latency budget, see [`resource-management.md` Section 12: Worker Heartbeat Resource Feedback](resource-management.md).



### 5.3 Unified Memory Management



```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         Unified Memory Management                                │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│  Problem: Native and JVM memory are split; the two sides cannot coordinate       │
│  Solution: unified memory budget; Native memory placed under JVM control         │
│                                                                                  │
│  Worker memory layout (authoritative definition: 06 §2 / §12.4):                 │
│  ┌─────────────────────────────────────────────────────────────────────────┐   │
│  │                    Worker Total Memory: 64 GB                            │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Reserved: 4 GB                                                   │   │   │
│  │  │  • JVM Heap, Metadata Cache, System Overhead                    │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  │  ┌─────────────────────────────────────────────────────────────────┐   │   │
│  │  │  Root Memory Arbitrator: 60 GB (Java manages budget)            │   │   │
│  │  │  ┌───────────────────────────────────────────────────────────┐ │   │   │
│  │  │  │  Query Pools: 50 GB (dynamic, split across queries)        │ │   │   │
│  │  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐                      │ │   │   │
│  │  │  │  │ Task-1  │ │ Task-2  │ │ Task-3  │                      │ │   │   │
│  │  │  │  └─────────┘ └─────────┘ └─────────┘                      │ │   │   │
│  │  │  └───────────────────────────────────────────────────────────┘ │   │   │
│  │  │  ┌───────────────────────────────────────────────────────────┐ │   │   │
│  │  │  │  System Pool: 10 GB (reserved)                             │ │   │   │
│  │  │  └───────────────────────────────────────────────────────────┘ │   │   │
│  │  └─────────────────────────────────────────────────────────────────┘   │   │
│  └─────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
│  Coordination mechanism:                                                         │
│  1. Java layer sets the memory limit via TaskRequest.resource_limit at Task start│
│  2. Native-layer Velox MemoryArbitrator arbitrates and Spills within the limit   │
│  3. The Java layer polls memory usage via nativeGetTaskStats()                   │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```



> **Authoritative class hierarchy**: Worker memory management is implemented by `ResourceManager` + `MemoryArbitrator` defined in [resource-management.md §3-4](./resource-management.md). There is no separate `MemoryManager` class — the Worker startup sequence (§2.4) calls `ResourceManager` directly for budget allocation and spill coordination. See 06 §4 for `MemoryArbitrator.requestMemory()`, `triggerSpill()`, and the complete arbitration algorithm.

---

## 6. Service Discovery & Communication

### 6.1 Control Plane (HTTP/REST)

Workers register with ZooKeeper, and the Coordinator watches for Worker changes, enabling automatic discovery and health checking.

### 6.2 Data Plane (Arrow Flight)

```java
public class PangrovaFlightProducer extends NoOpFlightProducer {

    private final ExchangeManager exchangeManager;

    @Override
    public void getStream(CallContext context, Ticket ticket,
                          ServerStreamListener listener) {
        FlightTicket flightTicket = FlightTicket.parseFrom(ticket.getBytes());

        ExchangeSource source = exchangeManager.getSource(
            flightTicket.getQueryId(),
            flightTicket.getTaskId(),
            flightTicket.getOutputPartition());

        try (VectorSchemaRoot root = source.getSchema()) {
            listener.start(root);
            while (source.hasNext()) {
                ArrowRecordBatch batch = source.next();
                root.clear();
                loadBatch(root, batch);
                listener.putNext();
            }
            listener.completed();
        }
    }
}
```


---



## 7. Observability Integration

This section defines the metrics and tracing integration points for the distributed architecture layer. For the full observability design, see [observability.md](./observability.md).

### 7.1 Metrics

The following metrics from [14 §3](./observability.md) are emitted by the distributed architecture components:

| Metric | Source Component | Description |
|--------|-----------------|-------------|
| `pangrova.coordinator.workers.active` | WorkerManager | Active Worker count |
| `pangrova.coordinator.scheduling.latency` | TaskScheduler | Task scheduling latency |
| `pangrova.coordinator.tasks.scheduled` | TaskScheduler | Total tasks scheduled |
| `pangrova.worker.tasks.received` | TaskManager | Tasks received by Worker |
| `pangrova.worker.tasks.completed` | TaskManager | Tasks completed |
| `pangrova.worker.tasks.failed` | TaskManager | Tasks failed |
| `pangrova.worker.memory.used` | ResourceMonitor | Current memory usage |

### 7.2 Tracing Spans

From the span hierarchy defined in [14 §8.2](./observability.md):

- `task.submit` — Coordinator dispatches task to Worker (CLIENT span)
- `task.execute` — Worker executes task (SERVER span)

### 7.3 Log Events

- Worker registration/deregistration events (INFO)
- Leader election transitions (WARN on failover)
- Heartbeat timeout events (WARN)
- Task retry events with retry count and reason (INFO)

See [14 §7](./observability.md) for structured logging format.


## 8. Speculative Execution (OPT-003)



Speculative execution detects straggler tasks and races duplicate copies on healthy Workers to bound tail latency. This addresses slow Workers caused by disk degradation, network jitter, or noisy neighbors — scenarios not covered by the crash-based fault tolerance in §4.

### 8.1 Architecture

```
┌──────────────────────────────────────────────────────┐
│                   Coordinator                         │
│                                                       │
│  ┌─────────────────┐    ┌──────────────────────────┐ │
│  │ TaskDuration     │───▶│ StragglerDetector        │ │
│  │ Tracker          │    │ (median × multiplier)    │ │
│  │ (per-stage stats)│    └───────────┬──────────────┘ │
│  └─────────────────┘                │                 │
│                                     ▼                 │
│                          ┌──────────────────────────┐ │
│                          │ SpeculativeTaskLauncher   │ │
│                          │ (launch duplicate on      │ │
│                          │  different Worker)        │ │
│                          └───────────┬──────────────┘ │
│                                     ▼                 │
│                          ┌──────────────────────────┐ │
│                          │ DuplicateTaskResolver     │ │
│                          │ (first-to-finish wins,    │ │
│                          │  cancel loser, dedup      │ │
│                          │  shuffle output)          │ │
│                          └──────────────────────────┘ │
└──────────────────────────────────────────────────────┘
```

### 8.2 Pseudocode

**Straggler Detection**:

```pseudocode
class StragglerDetector:
    config:
        multiplier = 1.5
        minCompletedFraction = 0.75
        minTaskDuration = 10s
        maxSpeculativeTasks = 0.1 × totalTasks

    function detectStragglers(stage):
        completed = stage.getCompletedTasks()
        running = stage.getRunningTasks()
        if completed.size < stage.totalTasks * minCompletedFraction:
            return []
        medianDuration = median(completed.map(t => t.duration))
        threshold = medianDuration * multiplier
        return running.filter(t =>
            t.runningTime > threshold
            AND t.runningTime > minTaskDuration
            AND NOT t.hasSpeculativeCopy
            AND stage.speculativeCount < maxSpeculativeTasks)
```

**Duplicate Task Management**:

```pseudocode
class DuplicateTaskResolver:
    function onTaskComplete(task):
        if task.isSpeculative:
            original = getTask(task.originalTaskId)
            if original.isRunning: cancelTask(original); useResult(task)
            else: discardResult(task)
        else:
            specCopy = getSpeculativeCopy(task)
            if specCopy != null AND specCopy.isRunning: cancelTask(specCopy)
            useResult(task)
```

**Shuffle Output Deduplication**:

```pseudocode
class ShuffleOutputDedup:
    function registerShuffleOutput(taskId, partitionId, location):
        key = (stage, partitionId)
        if shuffleRegistry.putIfAbsent(key, location) != null:
            log.info("Duplicate shuffle output for {}, keeping first", key)
```

### 8.3 Trade-offs

| Aspect | Current | With Speculative Execution |
|--------|---------|---------------------------|
| Tail latency | Unbounded by slow Workers; 60s+ penalty | Detected in seconds; bounded by healthy Worker speed |
| Resource overhead | None | Up to 10% extra tasks (configurable cap) |
| Complexity | Simple | Medium — duplicate management + shuffle dedup |
| Correctness | N/A | Shuffle dedup ensures exactly-once downstream semantics |

### 8.4 Migration Path

1. **Phase 1**: Add per-task duration reporting in Worker heartbeat. Coordinator computes per-stage median. Metrics only (`pangrova.speculation.stragglers_detected`).
2. **Phase 2**: Implement `StragglerDetector` + alerting. Log detected stragglers.
3. **Phase 3**: Implement `SpeculativeTaskLauncher` + `DuplicateTaskResolver`. Enable for scan-only stages first.
4. **Phase 4**: Implement `ShuffleOutputDedup` for shuffle-producing stages. Full support.


## 9. Data Locality-Aware Scheduling (OPT-008)



Enhances the scheduler to consider cache state when assigning splits to Workers, maximizing cache hit rates and reducing S3 I/O.

### 9.1 Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  CacheAffinityScheduler (Coordinator)                         │
│                                                               │
│  ConsistentHashRing ──┐                                       │
│  (split → Worker       │──▶ SoftAffinitySelector              │
│   default affinity)    │    (prefer cached Worker,            │
│                        │     fallback to least-loaded)        │
│  WorkerCacheRegistry ──┘                                      │
│  (bloom filter of cached files from heartbeat)                │
└──────────────────────────────────────────────────────────────┘
```

### 9.2 Pseudocode

**Consistent Hashing**:

```pseudocode
class ConsistentHashRing:
    virtualNodes = 150 per Worker
    function getPreferredWorker(split):
        splitHash = murmurHash(split.fileId + ":" + split.offset)
        return ring.ceilingEntry(splitHash).value
```

**Soft-Affinity Scoring**:

```pseudocode
class SoftAffinitySelector:
    localityWeight = 0.6, loadWeight = 0.4

    function selectWorker(split, candidates):
        for worker in candidates:
            localityScore = worker.cacheBloom.mightContain(split.fileId) ? 1.0
                          : consistentHash.rank(split, worker) based 0.8/0.6/0.4
            loadScore = 1.0 - normalize(worker.runningTasks)
            totalScore = localityWeight * localityScore + loadWeight * loadScore
        return maxScore worker
```

**Cache Metadata Reporting** (in Worker heartbeat, ~1-2KB overhead):

```pseudocode
class WorkerCacheReporter:
    function buildCacheReport():
        bloom = BloomFilter(cacheManager.fileCount, fpp=0.01)
        for entry in cacheManager.entries: bloom.add(entry.fileId)
        return CacheReport(bloom, totalCachedBytes, recentHitRate)
```

### 9.3 Trade-offs

| Aspect | Current | With Locality-Aware Scheduling |
|--------|---------|-------------------------------|
| Cache hit rate | ~30% (random) | ~80%+ (locality-aware) |
| Scan latency | S3 fetch per split | 3-5x reduction for cached data |
| Scheduling latency | Minimal | +~0.1ms per task (bloom filter + scoring) |
| Load balance | Optimal | Slightly less balanced; configurable weights + max imbalance cap (1.5x) |

### 9.4 Migration Path

1. **Phase 1**: Implement `ConsistentHashRing`. Pure hash-based affinity, no cache reporting.
2. **Phase 2**: Add `CacheReport` to Worker heartbeat. Build `WorkerCacheRegistry`. Log metrics.
3. **Phase 3**: Implement `SoftAffinitySelector` with configurable weights. Enable by default.
4. **Phase 4**: Auto-tune weights based on observed cache hit rates.

---



## 10. Worker Performance Scoring & Graceful Degradation (OPT-013)

This section introduces a composite Worker Performance Score and a weighted fair CPU scheduler, replacing the current binary health model (alive/dead) with multi-dimensional performance awareness.

### 10.1 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│              Worker Performance Scoring                          │
│                                                                  │
│  Worker Heartbeat (enhanced)                                     │
│    ├─ memory_pressure (existing)                                 │
│    ├─ cpu_utilization (new)                                      │
│    ├─ p99_disk_latency_ms (new)                                  │
│    └─ p99_network_latency_ms (new)                               │
│              │                                                   │
│              ▼                                                   │
│  ┌──────────────────────────┐                                    │
│  │ WorkerPerformanceScorer  │                                    │
│  │  memory  × 0.3           │                                    │
│  │  cpu     × 0.3           │     Score > 0.4 → normal           │
│  │  io      × 0.2           │──▶  Score 0.2–0.4 → deprioritize  │
│  │  network × 0.2           │     Score < 0.2 → blacklist        │
│  └──────────────────────────┘                                    │
│              │                                                   │
│              ▼                                                   │
│  Coordinator StageScheduler                                      │
│    → prefer high-score Workers for task assignment               │
│                                                                  │
│  Worker-side: WeightedFairScheduler                              │
│    interactive (weight 3.0) > batch (1.0) > background (0.5)    │
└─────────────────────────────────────────────────────────────────┘
```

**Problem**: Worker health is binary (alive/dead based on heartbeat). A Worker with degraded NVMe SSD (10× slower reads) or noisy neighbor (CPU throttled) continues receiving tasks at the same rate as healthy Workers. No CPU fairness exists between interactive and batch queries.

### 10.2 Pseudocode

**Composite Performance Score**:

```pseudocode
class WorkerPerformanceScorer:
    memoryWeight = 0.3
    cpuWeight = 0.3
    ioWeight = 0.2
    networkWeight = 0.2
    slowThreshold = 0.4       // below → deprioritize
    blacklistThreshold = 0.2  // below → blacklist

    function computeScore(worker: WorkerStats):
        memoryScore = 1.0 - worker.memoryPressureRatio
        cpuScore = 1.0 - worker.cpuUtilization
        ioScore = normalizeLatency(worker.p99DiskLatency)
        networkScore = normalizeLatency(worker.p99NetworkLatency)

        return memoryWeight * memoryScore
             + cpuWeight * cpuScore
             + ioWeight * ioScore
             + networkWeight * networkScore

    function normalizeLatency(latencyMs: double):
        // Sigmoid: 1.0 = fast (1ms), 0.0 = degraded (50ms)
        return 1.0 / (1.0 + exp((latencyMs - 10.0) / 5.0))
```

**Enhanced Worker Heartbeat**:

```pseudocode
message WorkerHeartbeat:
    // Existing fields
    MemoryPressure memory_pressure
    int32 running_tasks
    int64 available_memory_bytes
    // New performance metrics
    double cpu_utilization          // 0.0–1.0
    double p99_disk_latency_ms      // from iostat / io_uring
    double p99_network_latency_ms   // from recent shuffle RTT
    map<int64, int64> task_durations // taskId → running duration
```

**Weighted Fair CPU Scheduler (Worker-side)**:

```pseudocode
class WeightedFairScheduler:
    queues:
        interactive: PriorityQueue  // weight 3.0
        batch: PriorityQueue        // weight 1.0
        background: PriorityQueue   // weight 0.5

    function submitTask(task: Task):
        queues[task.queueType].add(task)

    function schedule():
        totalWeight = sum(q.weight * q.nonEmpty for q in queues)
        for queue in queues:
            if queue.nonEmpty:
                share = queue.weight / totalWeight
                cpuTimeSlice = SCHEDULING_INTERVAL * share
                runTasksForDuration(queue, cpuTimeSlice)
```

### 10.3 Trade-offs

| Aspect | Current (Binary Health) | With Performance Scoring |
|--------|------------------------|--------------------------|
| Performance | Slow Workers receive equal task load | Slow Workers deprioritized; interactive queries get priority CPU |
| Complexity | Simple binary health + memory pressure | Medium — multi-dimensional scoring, enhanced heartbeat |
| Risk | None | Low — scoring degrades gracefully if metrics unavailable |
| Heartbeat overhead | Minimal | +~100 bytes per heartbeat (new metrics fields) |

### 10.4 Migration Path

| Phase | Scope | Description |
|-------|-------|-------------|
| 1 | Enhanced Heartbeat | Add CPU, disk latency, network latency to Worker heartbeat. Coordinator logs scores without acting. |
| 2 | Performance-Aware Scheduling | Coordinator uses composite score to deprioritize slow Workers. Blacklist below threshold. |
| 3 | Weighted Fair CPU Scheduler | Replace basic thread pool with weighted fair scheduler on Worker side. |
| 4 | NUMA Awareness | Allocate native memory from local NUMA node. Pin pipeline threads to NUMA-local cores. |

> **Cross-reference**: Performance scoring enables better speculation decisions in Speculative Execution (§8 OPT-003). The enhanced heartbeat adds rack ID needed by Topology-Aware Costing (OPT-011 in [05 §11](./query-optimization.md)). Resource queue configuration is defined in §5.
