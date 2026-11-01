# Streaming 模块深度 Review（2026-10-31 当前 worktree）

Status: Closed — P0 已在模块拆分中修复；P1-1..P1-10 已全部修复并逐项核验
（§3.1 修复记录）；第二轮深度 review 见 §6，无新增 P0/P1，残余编译阻塞
全部属于并发 refactor 的所有权，不在 streaming 范围内。

## 1. 结论

功能主链已通过测试，但当前 worktree 存在**两套并行的 streaming 持久化架构**，这是本轮最高优先级问题：

1. 本次物理拆分方案：`pangrova-spi.CdcCommitStateStore` + `pangrova-streaming-jdbc`
   叶子（`org.pangrova.streaming.jdbc`）。
2. 并发落地的方案：`org.pangrova.streaming.control` 端口 +
   `org.pangrova.streaming.store.jdbc` JDBC 适配器，仍在 `pangrova-streaming`
   核心 jar 内，coordinator 已改为依赖它。

两套方案同时存在导致：核心 jar 违反“无 JDBC”边界、`control` 端口与 SPI
存储 API 归属不一致、metadata 模块测试反向依赖 streaming、物理拆分的
`pangrova-streaming-jdbc` 只承载 commit state。必须先统一，否则后续维护会在
两条路径间摇摆。

## 2. P0 发现

### P0-1 两套持久化架构并存

- 新代码：[StreamStateStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/control/StreamStateStore.java)、[JdbcStreamStateStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/JdbcStreamStateStore.java)、[JdbcStreamOffsetStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/JdbcStreamOffsetStore.java)、[JdbcWatermarkStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/JdbcWatermarkStore.java)。
- 本次方案：[CdcCommitStateStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-spi/src/main/java/org/pangrova/spi/streaming/CdcCommitStateStore.java)、[JdbcCdcCommitStateStore](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming-jdbc/src/main/java/org/pangrova/streaming/jdbc/JdbcCdcCommitStateStore.java)。
- `org.pangrova.streaming.store.jdbc` 位于 core，直接使用 `javax.sql.DataSource`，
  违反 `.pangrova/rules/architecture.md` 中新增的 “streaming core 不得包含
  JDBC/Jackson” 红线。
- `control` 端口未放入 SPI，而 commit-state API 在 SPI；同类持久化契约的归属
  不一致，未来替换后端时无法只依赖一个契约模块。
- `ModuleDependencyPropertyTest` 未登记 `org.pangrova.streaming.store.jdbc`，
  默认映射为 core，进一步掩盖边界问题。

**建议（保持已批准的物理拆分）**：
- 将 `org.pangrova.streaming.store.jdbc` 三个适配器迁入
  `pangrova-streaming-jdbc`（可统一到 `org.pangrova.streaming.jdbc`），core 只保留
  `control` 端口与 `watermark.InMemoryWatermarkStore`。
- 将 `StreamStateStore`/`StreamOffsetStore`/`StreamState`/`StreamLifecycleState`
  作为标准端口放入 `pangrova-spi`（与 `CdcCommitStateStore` 同级），或明确保留在
  core `.control` 并在架构矩阵中声明为 exported port；二者择一。
- 更新 coordinator 导入、`ModuleDependencyPropertyTest`、`SpiWiringVerificationTest`
  和 `docs/architecture/long-term-modular-architecture.md` 的布局描述。

### P0-2 metadata 测试反向依赖 streaming

- `pangrova-metadata/pom.xml` 新增 `pangrova-streaming` **test** 依赖；
  `pangrova-metadata/src/test/java/org/pangrova/metadata/streaming/*` 与
  `metadata/property/streaming/*` 测试的是 `streaming.store.jdbc` 类。
- 架构矩阵规定 `metadata` 只允许 `spi/common/governance/security/materialization`，
  不含 streaming。测试依赖同样破坏边界，且测试包名仍为 `metadata.streaming`。
- **建议**：把这些 store 测试迁入 `pangrova-streaming-jdbc`（或 core，取决于
  P0-1 的最终布局），从 metadata pom 删除 streaming 依赖。

## 3. P1 发现

| ID | 问题 | 证据 | 建议 |
|---|---|---|---|
| P1-1 | 偏移只增不减：`JdbcStreamOffsetStore` 的 upsert 使用 `CASE WHEN EXCLUDED > existing`，流重建/回退 seek 时无法持久化更低的 offset，重启会跳过数据。 | [JdbcStreamOffsetStore.java:59](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/JdbcStreamOffsetStore.java#L59) | 偏移持久化应允许显式 reset；单调性应由调用方/epoch 保证，而不是 SQL 隐式拒绝。 |
| P1-2 | `JdbcStreamStateStore.saveState(state)` 用 `state.epoch()` 作为 CAS 期望值；若调用方传入已递增 epoch 的新状态，会必然抛 `StaleEpochException`。 | [JdbcStreamStateStore.java:67](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/JdbcStreamStateStore.java#L67) | 删除无参期望值的 `saveState`，只保留显式 `saveStateIfEpoch(state, expected)`，避免误用。 |
| P1-3 | coordinator 的 CDC policy 硬编码：watermark 固定默认值、late data 固定 `UPDATE(0)`，未读取 `streamProperties` 的 `watermark_delay`/`allowed_lateness`。 | [StreamScheduler.java:197](file:///Users/sunxiaojian/workspace/pangrova/pangrova-coordinator/src/main/java/org/pangrova/coordinator/streaming/StreamScheduler.java#L197) | 从 `StreamState.streamProperties` 解析并校验，或在 StreamConfig 中显式建模。 |
| P1-4 | CDC 写授权只检查 INSERT。Upsert/Delete 实际需要 UPDATE/DELETE 权限，当前实现可能放行未授权更新/删除。 | [StreamScheduler.java:230](file:///Users/sunxiaojian/workspace/pangrova/pangrova-coordinator/src/main/java/org/pangrova/coordinator/streaming/StreamScheduler.java#L230) | 将操作类型传入 `WritePolicyEnforcer`，按批内 `CdcOperation` 组合检查；或要求调用方显式声明所需权限。 |
| P1-5 | `CdcPipelineRuntimeHooks` 仍提供 3 参数 public 构造器，默认 SYSTEM/no-op 安全与 no-op metrics；生产代码误用会静默绕过安全/指标。 | [CdcPipelineRuntimeHooks.java:49](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/cdc/CdcPipelineRuntimeHooks.java#L49) | 改为 package-private 或 `forTests()` 工厂，生产构造器强制传安全/围栏/指标。 |
| P1-6 | `CdcPipelineLoop` 在持有 per-table maintenance lock 时调用 `onWriteComplete`，会立即派发 compaction；该任务 5 秒拿不到同一把锁即被丢弃，直到下一次周期扫描。 | [CdcPipelineLoop.java:290](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/cdc/CdcPipelineLoop.java#L290)、[CdcPipelineLoop.java:393](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/cdc/CdcPipelineLoop.java#L393) | 在释放表锁后再通知 maintenance，或让 scheduler 在锁忙时保留任务而不是丢弃。 |
| P1-7 | `MaintenanceScheduler.close()` 只 `shutdownNow`，不取消已发出的 SPI future；in-flight 维护可能在关闭后继续写存储。 | [MaintenanceScheduler.java](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming-maintenance/src/main/java/org/pangrova/streaming/maintenance/MaintenanceScheduler.java) | 跟踪 issued future，close 时 `cancel(true)`；或要求 SPI 提供 shutdown 语义。 |
| P1-8 | `WatermarkState.STALE` 已无任何转移/写入点；状态机文档仍宣称 regression→STALE。 | [WatermarkState.java:10](file:///Users/sunxiaojian/workspace/pangrova/pangrova-streaming/src/main/java/org/pangrova/streaming/watermark/WatermarkState.java#L10) | 删除 STALE，或把它接到显式的 stale/lag 检测；同步文档与测试。 |
| P1-9 | `CdcDeadLetterHandler` 在 coordinator 中只记录日志；若未来启用 `SKIP_AND_RECORD`，坏记录会被静默丢弃而非持久化。 | [StreamScheduler.java](file:///Users/sunxiaojian/workspace/pangrova/pangrova-coordinator/src/main/java/org/pangrova/coordinator/streaming/StreamScheduler.java) | 默认保持 `FAIL_PIPELINE`；`SKIP_AND_RECORD` 必须绑定持久 DLQ 或禁止启用。 |
| P1-10 | `StreamConfig.commitStateRetention` 仅 `withDefaults()`，未接入 coordinator YAML；运维无法调整。 | [StreamConfig.java](file:///Users/sunxiaojian/workspace/pangrova/pangrova-coordinator/src/main/java/org/pangrova/coordinator/streaming/StreamConfig.java) | 加入 `CoordinatorOptions`/配置映射并校验。 |

## 3.1 P1 修复记录（2026-10-31）

| ID | 修复内容 | 落点 |
|---|---|---|
| P1-1 | 偏移 upsert 改为 plain-replace（`DO UPDATE SET committed_offset=EXCLUDED...`）：显式 reset/rewind 可持久化，重启不丢；单调性由 pipeline 提交协议 + HA epoch fencing 保证。测试 `saveOffsetsPersistsExplicitRewind` 覆盖。 | `pangrova-streaming-jdbc` `JdbcStreamOffsetStore` |
| P1-2 | 删除隐式 epoch 的 `saveState(state)`；`StreamStateStore` 只保留 `saveStateIfEpoch(state, expectedEpoch)` CAS。`StreamDdlExecutor.create()` 改用 `saveStateIfEpoch(running, creating.epoch())`；全部测试改显式调用。 | `pangrova-spi` `StreamStateStore`、`pangrova-streaming-jdbc`、coordinator `StreamDdlExecutor` |
| P1-3 | 新增 `CdcPipelinePolicies.fromStreamProperties(properties, epoch)`：解析 `watermark_delay`/`late_data_policy`/`allowed_lateness`，非法值抛 `IllegalArgumentException` 使 pipeline 以 FAILED 启动（不再静默默认）。`StreamScheduler.createLoop` 包装异常并附流名。 | `pangrova-streaming-runtime` `CdcPipelinePolicies`、coordinator `StreamScheduler` |
| P1-4 | `WritePolicyEnforcer.enforceWriteAccess(context, table, Set<CdcOperation>)`：按批内操作组合映射 `checkCanInsertIntoTable`/`checkCanUpdateTable`/`checkCanDeleteFromTable`；`CdcPipelineLoop` 与 `CdcTwoPhaseCommitter` 均先取批内 distinct operations 再授权。 | `pangrova-streaming-runtime` `WritePolicyEnforcer`、`CdcPipelineLoop`、`CdcTwoPhaseCommitter`、coordinator `StreamScheduler` |
| P1-5 | 移除 3 参 public 构造器，新增 `CdcPipelineRuntimeHooks.forTests(listener, retryBackoff, commitCoordinator)`；生产构造器强制传 securityContext/enforcer/fencingToken/deadLetterHandler/metrics。13 处测试调用点已改名。 | `pangrova-streaming-runtime` `CdcPipelineRuntimeHooks` |
| P1-6 | `CdcPipelineLoop.runWriteCycle` 在 finally 释放 per-table maintenance lock 之后才调用 `onWriteComplete`；2PC 路径 `commitWithTwoPhaseProtocol` 返回 `CdcWriteResult`，由 loop 统一在锁外通知。 | `pangrova-streaming-runtime` `CdcPipelineLoop`、`CdcTwoPhaseCommitter` |
| P1-7 | `MaintenanceScheduler` 跟踪全部 issued SPI future（`ConcurrentHashMap.newKeySet()`），`close()` 先 `cancel(true)` 再 `shutdownNow`。新增 `closeCancelsInFlightSpiFutures` 测试。 | `pangrova-streaming-maintenance` `MaintenanceScheduler` |
| P1-8 | 删除 `WatermarkState.STALE`；回归（非前进候选）不再移动状态机，tracker 记录日志并保持当前水位。`docs/architecture/streaming.md` 状态机表已同步。 | `pangrova-streaming-runtime` `WatermarkState` |
| P1-9 | `CdcDeadLetterHandler.isDurable()`（默认 false）；`CdcPipelineLoop` 构造时若 policy 为 `SKIP_AND_RECORD` 且 handler 非持久则抛 `IllegalArgumentException`。生产保持 `FAIL_PIPELINE`。新增正/负测试。 | `pangrova-streaming-runtime` `CdcDeadLetterHandler`、`CdcPipelineLoop` |
| P1-10 | `CoordinatorOptions.STREAM_COMMIT_STATE_RETENTION`（`streaming.commit-state-retention`，duration，默认 7d）+ `ConfigValidator.positiveDuration`；`buildBootstrapConfig` 经系统属性 `pangrova.streaming.commit-state-retention-seconds` 桥接到 `CoordinatorBootstrapConfig.streamCommitStateRetention()`，传入 `StreamingBootstrap.build` 与 `StreamConfig.withCommitStateRetention`。YAML 默认与 `etc/coordinator.yml`、配置文档已同步。 | `pangrova-config`、`pangrova-coordinator` |

## 4. 本轮 review 中已修复

- `LateDataConfig` 删除 `sideOutputTableSuffix`/`lateDataRetention`/`enabled()` 死字段，
  只保留 `policy` + `allowedLateness`。
- `MaintenanceScheduler` 在 `taskExecutor` 拒绝任务时重新入队，避免任务丢失。
- `StreamingBootstrap` 的 commit-state pruning 改为 leader-gated：当选时启动、
  失去 leadership/close 时取消，避免 follower 并发清理。
- `CdcPipelineConfig.flushInterval`、`StreamConfig.defaultFlushInterval`、
  `MaintenanceConfig.orphanCleanupScanInterval`、`snapshotRetainCount` 删除；
  `WatermarkPropagator` 删除；`CdcPipelineLoop` magic number 常量化。

## 5. 验证证据

- streaming 四叶子：**171 tests**（core 64 + format 48 + maintenance 18 + jdbc 41）。
- coordinator streaming 相关：**91 tests**（StreamScheduler、StreamingBootstrap、
  StreamingGovernanceChain、ModuleIntegration、BootstrapAssembly）。
- e2e streaming + architecture：**8 tests**。
- module dependency / state enum property：**10 tests**。
- 四叶子 `-Pstatic-analysis verify`：通过。
- `check-doc-class-refs.sh`、`detect-dead-stubs.sh`、`check-spi-annotations.sh`：通过。
- 使用 Python 3.12 覆盖后，coordinator + 上游主代码全量编译通过。

**限制**：全仓测试源码编译仍被并发的 metadata/materialization/indexing 重构阻塞；
本 review 的结论以 streaming 范围内已通过的测试和门禁为准。新增的
`control`/`store.jdbc` 尚无 streaming 叶内的直接单测（metadata 测试位置也不合规），
在 P0-1/P0-2 收口前不应视为完整验证。

## 6. 第二轮深度 review（2026-10-31，修复后收口）

### 6.1 P1 修复逐项核验（当前代码位置）

| ID | 核验结果 |
|---|---|
| P1-1 | `JdbcStreamOffsetStore` upsert 为 plain-replace（`JdbcStreamOffsetStore.java:56-62`，注释明确语义），测试 `saveOffsetsPersistsExplicitRewind`（150→50 逐字持久化）通过。 |
| P1-2 | `StreamStateStore` 仅剩 `saveStateIfEpoch(state, expectedEpoch)`；全仓 grep 无任何 `.saveState(` 调用点残留。`JdbcStreamStateStore.java:109-126` 的 WHERE 同时匹配 epoch/tenant/checkpoint，比契约要求更强。 |
| P1-3 | `CdcPipelinePolicies.fromStreamProperties`（`CdcPipelinePolicies.java:67-81`）严格解析三个属性，非法值抛异常；`StreamScheduler.createLoop`（`StreamScheduler.java:196-204`）包装异常附流名，pipeline 以 FAILED 启动。新增 10 个单测覆盖默认值、三种 duration 格式、非法值失败路径。 |
| P1-4 | `WritePolicyEnforcer.enforceWriteAccess(context, table, Set<CdcOperation>)`（`WritePolicyEnforcer.java:40-41`）；`CdcPipelineLoop.java:294-305` 取批内 distinct operations；coordinator 侧 switch 映射三类表权限（`StreamScheduler.java:226-243`）。 |
| P1-5 | 3 参 public 构造器已删除；`forTests()` 在 main 源码中零引用（仅测试用）。生产构造器 8 参全部必填且 `requireNonNull`。 |
| P1-6 | `CdcPipelineLoop.runWriteCycle` 在 finally 释放表锁后调用 `onWriteComplete`（`CdcPipelineLoop.java:342-360`，F-STREAM-06 注释在位）。 |
| P1-7 | `issuedSpiFutures` 跟踪（`MaintenanceScheduler.java:46-47`），`close()` 先 cancel 再 shutdownNow（`:216-219`），`executeTask` 完成/超时后移除（`:328-347`）。新增 `closeCancelsInFlightSpiFutures` 测试通过。 |
| P1-8 | `WatermarkState` 仅 INITIALIZING/ACTIVE/IDLE/FAILED（`WatermarkState.java:9-14`）；`docs/architecture/streaming.md` 状态机表同步为「回归→记录日志、保持水位」。 |
| P1-9 | `CdcDeadLetterHandler.isDurable()` 默认 false（`CdcDeadLetterHandler.java:23-25`）；`CdcPipelineLoop` 构造守卫（`CdcPipelineLoop.java:115-121`）对 SKIP_AND_RECORD + 非持久 handler 抛 IAE。正/负测试均通过。 |
| P1-10 | 全链路核验：`CoordinatorOptions.STREAM_COMMIT_STATE_RETENTION`（默认 7d）→ `ConfigValidator.positiveDuration` → `PangrovaCoordinatorMain.java:142-143` 系统属性桥 → `CoordinatorBootstrapConfig.streamCommitStateRetention()` → `StreamingBootstrap.build(..., commitStateRetention)` → `StreamConfig.withCommitStateRetention` → 消费点 `StreamingBootstrap.java:188-189`（prune 截止时间，替代原固定 24h）。 |

### 6.2 第二轮新发现

| ID | 级别 | 发现与处置 |
|---|---|---|
| O-1 | P3（已修） | `MaintenanceScheduler.onWriteComplete` 的 javadoc 宣称方法 synchronized，实际无 `synchronized` 修饰（原子性由 `reserveAndEnqueue` 内部锁提供）。本轮已改写 javadoc 描述真实语义。 |
| O-2 | 设计备忘 | `dead_letter_policy` 尚无可解析的 stream property；`fromStreamProperties` 硬编码 `FAIL_PIPELINE`，coordinator 的 handler 仍为 log-only。因此 SKIP_AND_RECORD 在生产不可达——安全性成立；将来若开放该属性，必须同时接线持久 DLQ（构造守卫已强制）。 |
| O-3 | P3（有意保留） | `StreamScheduler.parseIntProperty` 对 `batch_size`/`max_consecutive_failures` 非法值仍静默回落默认值，与 P1-3 的严格策略不一致。属调优旋钮 vs 正确性属性的刻意区分，暂不修改。 |
| O-4 | 已接受风险 | `MaintenanceScheduler.close()` 的 future 取消为 best-effort：与 `executeTask` 注册存在弱一致竞争窗口，极端情况下个别 future 可能晚于 close 迭代注册。由 `shutdownNow` + 契约文档（best-effort）兜底，可接受。 |

### 6.3 验证矩阵（修复后）

- streaming 四叶子 **217 tests 全绿**：runtime 75（含新增 `CdcPipelinePoliciesTest`×10、`CdcPipelineLoopTest`×14、SKIP_AND_RECORD 负例）、format 48、maintenance 19（含 `closeCancelsInFlightSpiFutures`）、jdbc 75（含 `saveOffsetsPersistsExplicitRewind` 与 CAS 单测）。
- coordinator streaming 包 + 2 个 bootstrap 测试类：**62/62 通过**（JUnit Launcher 直跑；其中 20 个容器测试需要 `TESTCONTAINERS_RYUK_DISABLED=true` + `JAVA_TOOL_OPTIONS=-Dapi.version=1.41` 规避本机 Docker 29/Ryuk 问题——环境规避，非代码问题）。
- `pangrova-config` 测试全绿（含新增 retention 默认值/解析 2 个测试）。
- `StreamingCdcWriterE2ETest` 对当前 artifact 单独编译通过。

### 6.4 残余限制与结论

- 4 个 coordinator 测试文件（Paimon/NativeDatasetControl/WriteCommitReconciler）编译失败于 `org.pangrova.connector.spi.*` 类缺失；e2e 模块整体 test-compile 失败于 `ShuffleServiceConfig`/`NodeMemoryPool`——均为**并发 session 的 connector/shuffle 重构**所致，本 session 未触碰这些文件，不属于 streaming 范围。
- 结论：P0/P1 全部关闭，第二轮 review 无新增 P0/P1。streaming 范围内代码、测试、文档三者一致；收口完成。
