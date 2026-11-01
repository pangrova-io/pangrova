# Worker bootstrap ownership

The Worker has one startup sequence and one cleanup sequence, shared by normal
shutdown and failed startup. `WorkerBootstrapLifecycle` admits configuration,
startup and cleanup serially without holding its monitor during I/O. Readiness
is published only after every required phase completes. A failed cleanup keeps
the Worker unready and retains ownership so a later shutdown can retry.

The packages in `pangrova-worker-core` separate these responsibilities:

| Package under `org.pangrova.worker` | Responsibility |
|---|---|
| `bootstrap` | Process entry, startup ordering and cleanup admission |
| `bootstrap.config` | Immutable deployment configuration |
| `bootstrap.ownership` | Installed components and partial task/function resource ledgers |
| `bootstrap.phase` | Resource, Native, task, HTTP and heartbeat assembly |
| `bootstrap.registration` | Registration and deregistration HTTP transport |
| `bootstrap.lifecycle` | Readiness checks, draining and ordered shared-resource release |
| `resource.memory` | Memory accounting, including the process-lifetime shuffle lease |

Phases and lifecycle checks consume ownership records; ownership records never
invoke startup phases. Registration transport has no dependency on assembly or
the resource ledger. These are internal packages within the Worker process
artifact, rather than independently published contracts or storage providers.

Cleanup first attempts to stop CDC, heartbeat, HTTP and tasks. Independent
failures are retained, and dependent cleanup cannot proceed until those owners
drain. A retained successful registration must then receive a deregistration
acknowledgement before its client is closed. This applies equally when startup
fails after registration and when a running Worker shuts down. Without a retained
registration client, no deregistration request is sent.

Writer and function owners drain next. Shared resource release closes shuffle
buffers, stops the metrics scheduler, shuts down Native, releases the shuffle memory
lease, closes telemetry and finally releases the resource manager. Failed Native
shutdown retains its memory lease. The process incarnation is released only
after cleanup succeeds. Task-owned result expiration and transport resources
remain with task ownership; shared shutdown does not acquire a second owner.

The obsolete standalone graceful-shutdown configuration had no production
consumer and has been removed. Shutdown policy follows the actual lifecycle and
owner barriers above.
