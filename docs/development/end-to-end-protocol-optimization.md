---
title: "Pangrova 端到端协议与数据库热路径优化"
description: "逐节点审查 SQL 提交、调度、Shuffle、结果读取和提交协议"
unlisted: true
---

# Pangrova 端到端协议与数据库热路径优化

协议审查与数据库证据保留在本页。实施以 [一次性重构与清理计划](./execution-clean-cut-plan.md) 为准，其中替换了下文的兼容入口、双协议、混部和对象迁移安排。

优先改造两个会随数据量或流量放大的路径。**第一，远端 Shuffle 消费每个 batch 都同步查 PostgreSQL，连读请求也会拿 execution anchor 共享锁和 query scope 排他行锁。第二，任务控制面以 JSON 承载 Base64 Protobuf，在绑定、HTTP、Worker Java 与 JNI 之间重复编解码。** 紧随其后的是跨 Coordinator 状态轮询读 journal、Worker ACK 和报告线程耦合，以及大结果整段解码。真正的 AQE、扫描和分布式输出决定大查询的吞吐上限，先建立正确的分区覆盖和字节预算，再做结构性改造。

代码证据来自 2026-11-21 的 Pangrova 工作树，属于历史调用链基线。同步往返、复制点和当时事务粒度可静态确认，但后续 lease/window、事件驱动 drain、owner fencing 和 Profile 实现已改变部分路径。当前未完成项以 [执行链路计划](./execution-clean-cut-plan.md#2026-11-25-代码审计与剩余工作) 的 A/B/C 审计为准；生产关键路径占比和收益仍须用同源部署归因。既有线性分片微测和 Worker 线程池隔离实验见 [全链路性能分析与修复方案](./performance-remediation-plan.md)。

## 逐节点审查

| 节点与协议 | 当前耗时来源及读库情况 | 处置 |
|---|---|---|
| 1 客户端 → Coordinator HTTP JSON | 连接、认证、提交响应、就绪等待分属不同阶段。CLI/JDBC/TPC 各自轮询 `/v1/query/{id}/state`，短查询可能主要在等待下一次 HTTP | **深改等待协议**。复用连接和提交响应，由 owner 推送状态增量或支持有界等待 |
| 2 鉴权、幂等和租户准入 | `AuthenticationFilter` 调认证 SPI，后端与配置决定外部 I/O。`IdempotencyKeyRegistry` 主路径为进程内 map。失败后判断提交是否已记录可能查 journal。租户排队与规划线程占用要独立计时 | **局部修复**。认证、幂等、准入分别计数。只在确有 I/O 的后端复用具备版本、有效期和撤销事件的权威快照。连接复用后也逐请求校验身份 |
| 3 SQL → AST → Calcite | `StatementDispatchPhase` 先解析类型，`CompilationPhase` 随后做语法/Time Travel、绑定、策略、优化。目录、统计或策略后端访问是 I/O 候选，语义改写本身是 CPU | **先测再改**。消除同查询等价的重复解析，限制慢目录调用的并发，单独记录 stats 缺失和估计误差 |
| 4 目录、快照、策略 → split source | `SplitFetcher` 优先使用规划提供的 `readBinding`，缺失才再次取 handle 和 bind。`IcebergSplitManager` 进入自己的目录加载及 manifest 规划路径。一次 Iceberg SDK 调用可能对应目录数据库、REST、对象存储或缓存，须在各 provider 内计数 | **深改查询级元数据复用**。认证一次绑定，split/CBO 复用固定快照的只读元数据，保留当前 authority、reader pin 和证据校验 |
| 5 Substrait → 任务模板 | `TaskRequestFactory` 对各任务复制并计算 plan digest。多个分区携带同一计划；后续绑定与请求包装可重复编码。直连 exchange 每个 consumer 嵌入各 producer 的描述和票据 | **先改构建边界，再按证据改 ABI**。stage bytes/digest 按阶段计算，分区差量最后绑定。测量共享模板和票据描述的扩展性 |
| 6 split 分配和 Worker 放置 | `StageSplitPartitions` 按编码字符串长度估算，逐 split 扫描目标分区。放置读进程内 Worker 快照，按分区重复筛选及估算资源，路径样本含“刚提交”的文件 | **深改有界调度**。按物理扫描权重分配，在途 grant 进账，与租户资源和完成/取消进度联动。热路径避免每 split 查目录/持久 Worker 表 |
| 7 Coordinator → Worker 提交 HTTP | `SubmitTaskRequest.from/withBoundTask` 将 Protobuf 编码成 Base64，然后构造函数又解码并解析。 `StagePartitionDispatcher` 与 `RemoteTaskDispatcher` 都走绑定路径；即使结果字节相同，某些路径仍重建 wrapper。HTTP 再编码 JSON。HA 调度的正常 owner resolver 读本地 anchor | **最高优先级的协议改造**。单一冻结点和版本化 binary protobuf；保留 exact request 校验与 COPY 私有传输 |
| 8 Worker admission → JNI 创建 → 提交 ACK | 现有正常路径由 Worker 同步创建、绑定、启动及发布 RUNNING 后返回 ACK，JNI 创建位于 ACK **之前**。REST 已 parse，`NativeTaskExecutor` 再从 bytes parse，绑定本地存储资源后 serialize，C++ 再 parse；Java 与 Native 的必要校验各自存在。Worker catalog 命中本地 revision cache，冷 miss 才拉取部署并解析租户凭据 | **深改线程归属及重复解析**。使用已解析对象执行 Worker 检查，一次 serialize 本地化后的 JNI 请求。真实 Ready 和提前接纳分开 |
| 9 Worker JNI 状态 → Coordinator HTTP report | 常规监控约每 50 ms 搬运完整 canonical report。HTTP 报告混合 JSON 字段和 Base64 canonical bytes。 `TaskStatusReceiver` 正常 HA 查询通过本地 anchor 确认 owner；选择持久 journal 的终态路径可能同步等日志提交 | **深改**。状态标量或完成事件、二进制可信报告、用 sequence 去重。保留终态持久化等待，隔离同步 RPC 和资源释放 |
| 10 Velox → Parquet/对象存储 | 读取带宽、GET 放大、manifest/row-group 剪裁、动态过滤迟到、arbitration 和 spill 等都可能主导。已有 Velox 过滤、内存和 spill 基础设施 | **按扫描类负载深改**。先减少读取、优化小文件和准确度量，再调调度/下载窗口；每 batch 禁止目录数据库查表 |
| 11a 直连 Shuffle/Flight | producer ACK 回传各分区票据，consumer 绑定及 JNI 连接逐来源处理。每条边都有执行身份和 ticket 生命周期。产销背压和 fan-out 放大控制开销 | **分级**。小查询保留直连低延迟，优先减少重复描述、分配和解码。大 fan-out 测 `producer × consumer` 正常边数与重试放大 |
| 11b Remote Shuffle gRPC + PostgreSQL + 对象存储 | 写端每 batch 两事务夹一次 immutable PUT。消费端 `readPage` 和 `readPayload` 都查 pin，`readPayload` 每 batch 调用；元数据分页另查 allocation、anchor 和 batch rows。chunk 切片、digest 及 Worker gRPC→JNI protobuf 又带来多次复制 | **最高优先级的数据库热路径重构**。持久 pin/lease 与 finalized manifest 前移，稳态 batch/segment/chunk 零同步 SQL，减小对象和事务数量 |
| 12 OUTPUT → Native Flight `DoGet` | `LeafStageDispatcher` 将特定 live 末级固定为一个任务。结果绕过 Coordinator 数据平面直达客户端。Native 解码内部 IPC 页后交 Flight，chain buffer 可能需要线性拷贝；单端点可能受 Worker 带宽和消费者限制 | **拆两阶段**。先用有界 Arrow 消费减少复制；再按大结果证据做并行输出或持久结果段 |
| 13 Flight client → CLI/JDBC/应用 | `FlightResultFetcher` 整流 IPC 写回堆并复制成 `byte[]`；stream API 仍逐批再序列化。每批 Future 调度及 JDBC 行对象分配另计 | **深改**。forward-only 路径直接迭代 Arrow batch，有界读窗口和单个流生命周期，删除旧 byte[] 结果 API |
| 14 查询状态 / 资源释放 | 本地 query 状态读内存。跨 Coordinator 或本地缺失时，`QueryJournalLifecycle.lookup` 可能每次执行 latest journal SELECT。旧响应还从本地取 endpoint/error。结果释放依赖 Native 实际退出、单线程 sweep、状态发布、journal 和租户回收 | **深改**。按 owner 路由或版本化状态投影，事件驱动定向 drain，保留单次恢复读取；客户端轮询和 drain 检查都走本地状态 |
| 15 写入、COPY、fenced commit | Worker 写文件并返回 fragments。Coordinator 执行质量校验、持久 intent、fence、connector commit 与持久确认。`JdbcWriteCommitJournal.isCommitted` 本身读取 entry，已提交分支又读一次；fence 的 `isCurrent` 与本地 `currentAnchor` 不同，会查权威 backend | **先去重复查询与文件放大**。读取状态一次，必要 fence 并入可证明原子性的事务边界。按历史持久状态处理未知结果 |

## 改造一 让 PostgreSQL 只承担需要持久权威的工作

### 可复核的数据库调用（历史基线与当前边界）

- **Shuffle 读取。** 历史调用链中，`ShuffleServiceEngine.readPayload` 每个 batch 调一次 `PostgresShuffleStore.requirePinnedRead`，PostgreSQL provider 执行 execution anchor SELECT、query scope 锁定读取和 pin expiry SELECT，因此 `B` 个 batch 对应 `B` 笔检查事务和 `3B` 条读取语句。该形态已被后续 read lease/window 取代，不再作为当前逐 batch 成本。当前硬门槛是已建立流的数据面逐 batch/chunk 零授权 SQL；建流、续租、关闭、恢复和 durable revocation 单独计数，后者仍见 A11。
- **Shuffle 写入。** 历史基线为 `beginWriteIntent` 与 `completeWriteIntent` 每 batch 各一个事务、每窗口八条 SQL。当前生产端按最多 128 batch 的 publication window 预留和发布，SQL 成本必须按窗口/流统计，不能继续标为“每 batch 八 SQL”。逐 batch 所有权、顺序和对象完整性约束仍保留。
- **查询状态。** 本地 owner 的状态读走内存；本地 miss 或显式历史读取可能调用 journal。当前非 owner live wait 尚无可信 owner directory/forward，直接返回 `QUERY_OWNER_UNAVAILABLE`，不能声称跨 Coordinator directed wait 已零读取。Profile/audit 历史读取单独计量。
- **持久 journal。** 当前 `JournalEventSql` 对 query/task 事件执行 `INSERT ... ON CONFLICT DO NOTHING`，通过 `executeUpdate()` 判断插入；只有 event ID 冲突时才 SELECT 整行核对。正常新事件没有独立读回。`INSERT ... RETURNING` 是历史设计验证和 A1 的可选方案，不是当前实现。
- **目录与提交。** 固定快照已有显式 read binding。`NativeTaskWriteCompletion` 的已提交 fast path 合并状态读取并保留 COPY 发布证明；Worker `ExecutionOwnerFence` 与 Coordinator 本地 anchor 校验是内存路径。普通 distributed write 的长期 receipt retention 仍见 A12。

### 目标设计

1. **状态流和读取投影。** 按 query owner/epoch 发布单调版本的轻量视图，包含权限范围、Ready、endpoint、错误或终态。submit 提供 owner 路由信息或用稳定入口按目录视图转发；`GET`/bounded wait 只读投影。沿持久 journal 的确认边界发布最终状态，owner 接管时从同一 watermark 重放。冷启动读取由 single-flight 或批量重放驱动；多客户端重复 miss 不发起各自的 SQL。未知 ID、未就绪和过期 tombstone 采用有界内存，并从认证后的路由结果确定。失去投影进度时显式返回可重试的查询状态，保留诊断恢复工具。
2. **Shuffle read lease。** 打开流时在一笔权威事务内绑定 exact execution owner、分区与 committed winner、immutable manifest digest、读 lease、保护时间和权限截止。manifest 可按内容地址缓存或从对象存储分页取，逐 batch 从已绑定索引取 offset、长度和 checksum。读 lease 持久记录防止 cleaner 提前删除，进程内 reader 只在当前 lease 和 owner 仍有效时授权下一次读取；续约由共享控制调度批量完成，避免按 segment/chunk 续约。对必须立即失效的边界，使用带版本的撤销与消费栅栏。若订阅落后、进程换代、租约耗尽或无法证明当前权威，主动停止发新读并关闭流。普通 TTL cache 不能替代 revocation 协议。
3. **与接管、取消和清理协调。** 通过持久 lease 或具有等价一致性保证的授权边界定义旧 epoch 最后允许发起的读，定义新 owner 何时可以发布。保护期覆盖已发出对象请求的实际退出，或者等到其有证据的上界。发布、接管、撤销、删除各自保持原授权范围和原子条件。证明旧 token 不会在允许边界之后取得新字节、删除不越过任何在途读、重复 release 幂等。该证明完成后才移除 per-batch SQL。
4. **目录以 binding 为单位。** 绑定当前 catalog deployment、表 UUID、ref/snapshot、read schema 与约束，向规划和 split 供给同一不可变事实。只有共享同一事实的查询才可合并首次 metadata 读取。可跨查询保留不可变 manifest/schema，当前 catalog/ref 和当前权限采用版本化失效或权威重读。相同查询的权限校验应复用已获取的 authority 对象，保留动态撤销需要的校验。Worker 已有 tenant/catalog/incarnation/revision 维度的本地部署缓存和 single-flight。保留这条热路径，针对滚动放量或大量租户冷启动测部署 RPC、凭据解析与缓存失效，避免各 attempt 重新拉取同一 revision。
5. **去除 journal 正常写入后的同步读。** 优先用 `INSERT ... RETURNING` 的同次往返结果验证新插入行，或先证明字段和 JSON 规范化等价后使用插入结果。只有冲突、失败恢复或 commit 结果未知时再按 exact event 查证。已存在事件必须比较完整事实，拒绝同 ID 异 payload。保持 lineage 同事务、错误回滚、提交 ACK 顺序和重放结果。再考虑批量 SQL，将同类事件按数量、字节和时间窗口合并，保留逐事件完成证据。
6. **写入采用有限大小的 durable intent。** 远端 Shuffle 先实测每 batch 事务和锁竞争，再按同一 attempt/分区的连续批次组成 segment。一个 immutable PUT，提交事务中原子登记 batch 索引、序号、digest 和 receipt。SQL DML 继续按 query 聚合 fragments 并做一次 fenced catalog commit。相邻重复读合并到同一次读取或条件更新，跨非原子外部 catalog 调用的 fence 仍需证明。PostgreSQL 写入、故障恢复、权限变更和 durability 事件与常规 read path 分开计量。 [Trino FTE 的交换持久化](https://trino.io/docs/current/admin/fault-tolerant-execution.html) · [Iceberg 乐观并发与快照提交](https://iceberg.apache.org/docs/latest/reliability/)

**验收。** 对 peer 状态读取，稳态重复状态请求的 JDBC SELECT 数为零。常规新 journal 事件的独立读回 SQL 为零，冲突与未知结果核对按用例单独计数。对已建立的远端 Shuffle 流，不论发送 1、100 或 10,000 个 batch，数据面同步 SELECT 增量为零，SQL 只出现在建立、明确续约、关闭或恢复。对同一流重复目录身份解析的增量为零，connector 允许有界首次加载和换版本查询。用 JDBC count assertions 与真实 PostgreSQL/TestContainers 验证这些上界，加入旧 owner、延迟撤销、断开、时钟偏移、partial read、重放和并发回收测试。基于 `JdbcQueryStateJournalIntegrationTest` 和 `NativeExchangeTransportExternalIntegrationTest` 验证行为与计数。统计每查询、每分区、每 GiB 的 SQL/对象操作量，并区分读事务、写事务、锁等待与连接池等待。

## 改造二 缩短 Java、JNI 与 Flight 的编解码边界

### 任务提交与报告

1. **第一份 PR 保持现有线协议。** 在 Coordinator 侧延后 JSON/Base64 序列化，将 plan bytes 和 SHA-256 在 stage 冻结时计算一次；绑定链用已有 parsed message 派生，unchanged 直接返回原对象，已经最终冻结的 envelope 复用 exact bytes。Worker REST parse 一次，内部传已有 typed `TaskRequest`；需要注册凭据、Native 路径/COPY 本地化时，构建新的 Worker-local contract 并单独校验，跨 JNI 才 serialize。兼容入口仍可接收原 `byte[]`。不要用序列化后再 parse 实现内部校验。JVM 与 C++ 是独立边界，可以分别校验收到的工件。保留未知字段策略、资源限制、实际传输的字节身份和 COPY 私有票据隔离。
2. **第二份 PR 加协商后的二进制 HTTP。** 保留现有 HTTP、认证/mTLS 与端点并运营，版本化的 Protobuf envelope 承载冻结的 request、ACK、report。优先普通 task，COPY 在独立私有 route 保持等价验证后才推进。不要将常规 report、写片段和 release receipt 全部按同一频率发送。RUNNING 减载，终态 canonical report 与 receipt 保留完整证据，状态和错误映射由模板测试证明一致。4 MiB canonical payload 在 Base64 后为 5,592,408 字节，约多三分之一，尚未计 JSON、HTTP 或压缩。双协议对照记录带宽、CPU、分配和峰值堆。
3. **大型 stage 模板按证据复用。** 以 stage/plan epoch、精确内容和版本、授权作用域及 schema 绑定模板，只缓存可证明不依赖 attempt 或 Worker local resource 的部分。每请求仍附 split、exchange、grant、租户和 attempt。需要重试时按老规则找原 exact identity，不能让 cache miss 重复执行已经接纳的尝试。目标控制面 CPU/字节随真正变化的参数和物理边增长。
4. **监控和线程。** JNI 轮询先改轻量状态或有界完成信号，异常再取完整 report。Worker 让启动、快监控、报告、drain 独立推进。Coordinator 阶段 listener 只提交有序事件，禁止在报告和 HTTP future 完成线程中准备整个下游 stage。跟踪已有 durable journal 的 flush 等待、ACK 与应用语义，明确哪个线程等待哪个资源。

### Shuffle 与结果数据

1. **先试无协议语义变化的优化。** `ShuffleReadPump.batchFrame` 对原字节范围计算相同 chunk digest，去掉 `ByteString.toByteArray()` 临时副本。读端按 gRPC readiness 和有界 credit 控制活跃字节，验证不同 batch 与 codec 的序列化 CPU、校验 CPU 和复制字节。
2. **减少 Remote Shuffle chunk → JNI 复制。** gRPC Java reader 验证 chunk 后，`NativeExchangeTransport` 将控制元数据和有界 payload 分开交给 JNI。C++ 直接取得一份拥有所有权的 payload，验证 exact attempt、frame sequence、长度和摘要后交给现有 ingress。页组装仍受原有上限，失败和取消仍沿相同入口释放。这个改动覆盖 JNI 交接，远端网络和消费者实测通过独立压力窗口验收。
3. **结果先在客户端消除 IPC 往返。** 在 Transport 内按 Arrow 批消费，接入 CLI 和 JDBC forward-only。批的所有权在 consumer 退出时归还，支持取消、最终 SQL 错误和数据顺序。Native Flight 如果内部 IPC 页解码仍有明显占比，再验证有生命周期的 Arrow buffer 直接交接原型。线上压缩、TLS 和序列化成本单独保留计数。 [Arrow Flight 结果流与端点顺序](https://arrow.apache.org/docs/format/Flight.html)

## 改造三 部署业界做法时保留各条查询的优势

| 需要深度优化的负载 | 参考做法 | Pangrova 交付与触发条件 |
|---|---|---|
| 短查询与多租户 | Trino Resource Groups 按组限制并发和排队，并提供调度策略。Trino 客户端协议将请求、可继续获取和完成状态关联。 [Resource Groups](https://trino.io/docs/current/admin/resource-groups.html) · [客户端协议](https://trino.io/docs/current/develop/client-protocol.html) | 重点是准入后规划到端点的等待、状态感知、取消以及真实 drain。保留小结果直连。资源组策略在现有租户维度演进，限制准备任务数和字节，同时留出完成、取消和解除背压的执行容量 |
| 大扫描与复杂 SQL | Iceberg 利用 manifest list 和列统计剪裁。Trino 用 connector stats 选择 join 顺序/分布，以动态过滤减少 probe 侧需要读取的数据，并约束收集大小。 [Iceberg 规范](https://iceberg.apache.org/spec/#scan-planning) · [Trino CBO](https://trino.io/docs/current/optimizer/cost-based-optimizations.html) · [动态过滤](https://trino.io/docs/current/admin/dynamic-filtering.html) | 按物理 range、预计解码、delete files 成本权衡 split 大小。只对高选择性且预计快完成的 build 等待动态过滤，给 wait 严格预算；记录过滤产生、可用、应用和已经发出的对象读取。统计缺失保持明确的保守计划，并记录缺失原因。CBO 先覆盖数据估算和 broadcast 每 Worker 预算 |
| 大 Join/Group By/Sort | Spark AQE 按实际 shuffle map 输出统计合并小分区、拆分倾斜数据和调整 join。Trino FTE 按 split 体量和 task 内存估算分配节点。 [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution) · [Trino FTE](https://trino.io/docs/current/admin/fault-tolerant-execution.html) | 已从派发中移除仅调整并行度的策略。以已提交 Shuffle 的统计和 epoch 为证据，定义每个 consumer 的原分区及 range、证明覆盖无空洞或重叠。先部署安全的 coalesce，再按 join 类型启用 skew split；共享另一侧时限定复制字节，核验 outer/semi/anti、NULL、顺序和重试。没有覆盖证明的调整不进入签发；已签发阶段不能就地改变既有身份 |
| Native 内存与 I/O | Velox 的 task 驱动与内存仲裁把查询、算子和数据缓存纳入有界管理，memory pressure 触发 reclaim/spill。 [Velox Task](https://facebookincubator.github.io/velox/develop/task.html) · [Velox 内存](https://facebookincubator.github.io/velox/develop/memory.html) | 先从现有 arbitrator、cache 和 spill 分离出排队、blocked reason、I/O、回收及峰值采样。按 stage 一次 Worker 快照、扣在途 grant，以数据减少和 tail partition 为先。只有当剩余成本落在 scan decode 或具体算子，再调 batch、并行度、prefetch 或 spill 大小 |
| 大结果和慢客户端 | Flight 支持多个端点，应用负责完整消费及顺序。Trino 可并行写持久结果段，使执行资源能够早于客户端读完而释放。 [Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) · [Trino Spooling](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol) | 先用原单端点实施 bounded batch。对无序大结果且单 Worker 出口确认吃满时才启用并行端点。有序结果用有界有序合并或保证范围顺序，正确执行 LIMIT。只有慢消费者或补偿读取成本占主导时再评估 spooling，包括持久结果预算、对象操作、可见快照、短期读取授权与 GC。确认转移所有权后才提前释放执行资源 |
| DML/COPY 和批量写 | Iceberg 通过可复用的 manifest、乐观提交与快照元数据交换发布。文件大小形成 metadata 成本与重写代价。 [Iceberg 可靠性](https://iceberg.apache.org/docs/latest/reliability/) · [表属性](https://iceberg.apache.org/docs/latest/configuration/) | 先按 connector 统计输出文件数、实际 file-size 分布、manifest/quality/fence/journal 和提交的关键等待。治理小文件、写分区 skew、COPY 物化、COW 命中文件裁剪。保持写前授权、保留未命中行、commit proof 和未知结果一致性 |

## 分批交付与验收

| 交付批次 | 交付物 | 停止或继续的判断 |
|---|---|---|
| P0 按路径测量 | SQL submit、Worker ACK、Native create、首/末批、查询终态、物理释放跨进程关联。记录 DB calls、tx、pool wait、锁、encode/decode 次数、输入/输出/复制字节以及开销窗口 | 基线能分别解释短查询、扫描、Shuffle、大结果和 DML，历史 Profile 的派生 IO 不能作实测。采样范围与扰动有记录。 [Dapper 分布式追踪](https://research.google/pubs/dapper-a-large-scale-distributed-systems-tracing-infrastructure/) |
| P1 控制与状态 | 拆 Worker/Coordinator 回调、query owner 状态读、重复 task wrapper 和 JNI parse、journal 写后读、轻量 completion、按真实证据 drain | 零 peer 稳态轮询 SELECT，零正常 JVM/HTTP 线程内长监控。旧入口与新入口下的相同 SQL、授权、错误及 exact attempt 结果一致 |
| P1 远端读取 | 持久 read lease、不可变分区 manifest、内存 fast path、受控撤销并发测试 | 稳态数据读的同步数据库调用为零，故障/撤销用例无过期流、无提前删除。达到该条件再扩大新路径 |
| P2 协议与写入粒度 | 二进制 task/ACK/report、分段 Shuffle、直接批消费 | 独立 A/B，确认每 GiB 的事务、对象、分配和复制下降，比较首批、吞吐、尾延迟，兼容旧读取和重放 |
| P3 物理优化 | byte-weighted split、动态过滤、真实 AQE 读取映射、多端点或结果段、DML 文件布局 | 当前版本的瓶颈指标达到启用条件才改造。同一固定快照、同一 SQL 和资源配额下证明结果和实际资源释放 |

基准按 [TPC 方法](../testing/tpc-benchmark-methodology.md) 固定产物和数据。测试组包括短查询/多租户、冷热 Iceberg 与大量小文件、均匀/倾斜 Shuffle、快速/慢速大结果、同表/异表 DML、断连/取消/接管。比较 P50/P95、有效吞吐、CPU/对象请求/事务每 GiB、峰值 heap/direct/native 和从 EOF 到物理退出。三组独立窗口，报告失败、放弃和恢复样本。阈值在基线冻结后按场景确定，硬性约束为重复同步 SQL 上界和所有正确性/资源约束。

上线前先验证同机可执行性，再用真实远端 PostgreSQL、对象存储以及独立 Worker 做 A/B。兼容功能按能力和查询 cohort 启用。任何状态跳跃、重复执行、错误提交、读取覆盖缺失、权限撤销失败、提前删除或资源未归还都会停止扩大覆盖。回退前停止分配新 cohort，旧查询与旧对象按其版本 drain，保留故障复现材料。

本轮完成公开一手资料对照、代码路径审查与设计方案。文中的 SQL 和字节数字是固定路径计数，性能归因与上线阈值由 P0 实测确定。
