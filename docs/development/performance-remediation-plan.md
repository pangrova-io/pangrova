---
title: "Pangrova 全链路性能分析与修复方案"
description: "SQL 关键路径的性能证据、修复顺序、验证和上线条件"
unlisted: true
---

# Pangrova 全链路性能分析与修复方案

早期证据与方案保留在本页。实施以 [一次性重构与清理计划](./execution-clean-cut-plan.md) 为准，其中替换了下文的旧协议保留、迁移、混部和并行运行安排。

以 2026-11-21 的 Pangrova 工作树为分析对象，基点为 `e89dee74f`，包含当时尚未提交的调度、检索、CDC 和 Flight 改动。范围覆盖 SQL 读写主链路，另列持久化恢复和检索、CDC、后台维护共用资源的专项验证。本次交付分析和实施方案。性能排序先按代码证据、负载规模和影响面制定，生产优先级由同源部署实测校准。

优先做三件事：补齐关键路径测量，减少 Worker JNI 完成检测和客户端 HTTP 状态轮询，再治理 Flight 大结果内存与 Coordinator 分片准备。最小堆只在高扇出微测中稳定获益。末级单任务和 Shuffle 尾延迟需要分开验证，再决定是否调整分区编排或数据传输。

逐节点的 HTTP、Protobuf、JNI、Shuffle、Flight 与数据库读写审查，见 [端到端协议与数据库热路径优化](./end-to-end-protocol-optimization.md)。

## 关键路径与测量口径

```text
客户端提交 → HTTP 鉴权/幂等/租户准入 → 语法/目录/快照/策略/优化
→ Substrait 分片 → split 枚举/分配/放置 → Worker HTTP ACK → JNI 创建
→ Velox 任务与文件读取 → 交换发布/消费 → OUTPUT → Native Flight → 客户端读取
→ 查询终态 → 任务、结果、Shuffle、租户槽位实际释放

写入分支
Velox 写出 → Worker 终态和文件片段 → Coordinator journal 与 fenced commit
→ 目录快照可见 → 查询终态与物理释放
```

- 客户端用单调时钟记录提交到响应、结果端点可读、第一批、最后一批、查询终态。大结果同时记录行数、字节数和峰值堆内存。写入记录快照可见时间，CDC 记录源位置确认时间。
- Coordinator 记录准入/排队、语法与目录绑定、授权、优化、Substrait、每个来源的 split 枚举/剪裁、分配、放置、ACK 和 journal 等待；Worker 记录入队、JNI 创建、驱动执行、状态探测、终态报告与物理释放。Shuffle 记录序列化/压缩、发布、RPC/存储、消费者等待、验证和尾分区；Flight 记录可读到首批、读取等待、IPC 编解码和慢消费者背压。
- 用 query、stage、partition、attempt、plan epoch 和 trace 关联样本。在单进程内计算持续时间，用因果事件关联跨进程关键路径。并行阶段的 wall time、累计 CPU 和不同资源的峰值各自汇报。分别记录结果可读、成功终态和实际资源退出。
- 通过现有 Telemetry 接口增加低基数直方图和计数器。查询、任务标识进入抽样 trace 或诊断记录。内存分别采集 JVM 堆、Arrow/direct、Velox pool、进程 RSS 与 spill；流量分别记录逻辑有效载荷、RPC 编码和实际链路流量。诊断采用分段计数、JFR 和 Native CPU/锁/分配采样交叉验证。

## 已有证据

| 证据 | 观察 | 适用范围 |
|---|---|---|
| 2026-11-14 SQL 进程对照 | `constant` 的中位数 3,994.75 → 3,527.89 ms，`iceberg-sum` 为 5,065.55 → 4,651.81 ms。候选第二轮的 `iceberg-sum` 中位数和样本 P95 都高于基线第一轮 | 各版本、各 SQL 24 条。含 CLI 进程成本，且跨 fork 波动明显；用于决定短查询和 Iceberg 路径应独立测量 |
| 2026-11-14 JNI 报告探针 | 两个候选进程共 318 次成功状态读取，拷贝 127,521 字节；共 14 次 release receipt，拷贝 6,232 字节。对应基线状态读取为 186 次、85,680 字节 | 历史探针，统计成功调用和报告字节。CPU 占比与增长归因需当前版本测量 |
| 2026-11-14 Remote Shuffle Java 读对照 | 4,453,760 字节分区，每版本 96 条。中位数 166.68 → 164.65 ms，P95 216.37 → 237.55 ms，吞吐 23.90 → 24.04 MiB/s | macOS 单机共置服务和夹具，测量期间有独立 Maven 构建，覆盖读路径与验证 |
| 2026-11-21 split 微测 | 编译当前 `StageSplitPartitions`。同一批 10 万个合成 split，最小堆按累计负载、分区编号排序，各规模的分片与顺序完全相同。1,024 分区在线性扫描为 71.9–81.8 ms，堆为 39.5–47.0 ms。8 分区分别为 27.0–35.7 ms 和 34.4–40.4 ms | macOS arm64、Corretto 17、512 MiB 堆，3 个独立 JVM，每种规模预热 12 对、交替顺序测量 25 对。区间为各 JVM 中位数范围，含排序和分配，排除 I/O。64 分区堆更慢，256 分区表现接近且方向不一致 |

历史 SQL 与 JNI 原始样本在 `pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-{process,resources}/`。远程读实验的拓扑、样本和复现参数参见 [Shuffle 读对照](../testing/shuffle-read-comparison.md)。当前工作树尚无专用 Linux 多机、固定快照、同源产物的生产级端到端耗时及瓶颈占比。

## 修复顺序

| 批次 | 工作项 | 主要归属 | 完成条件 |
|---|---|---|---|
| P0 | A 阶段归因、Profile 与基准口径 | Observability、Testing、Query | 一次运行能按 attempt 复核关键路径、可读/终态/释放，冻结可重复的基线 |
| P1 | B 任务完成检测，C 客户端查询状态等待 | Worker、Native、Client、Coordinator HTTP | 降低状态探测字节和 HTTP 请求，验证错误、取消和真实释放 |
| P1 | D Flight 读路径，E Coordinator split 与 DAG 准备 | Transport、Client、Native、Coordination、Connectors | 控制大结果峰值，减少高扇出 CPU 和排队，保留结果与交换身份 |
| P2 | F 扫描/优化/OUTPUT，G Shuffle，H 写入与共用资源 | Planner、Connectors、Native、Shuffle、Persistence、Streaming | 对最慢阶段做单因素对照，通过正确性、故障和多机门禁 |

A 完成基线后，B、C、E 中互不改变同一变量的改动可独立推进。D 先做 Java 消费端，再用同一指标判断 Native 结果编码的价值。F、G、H 的实现顺序由实际关键路径贡献决定。

### A 让基准识别实际算子证据

Profile 以已签发的 Native attempt 为身份保留最新的测量报告，可以区分不同 Stage、重试和同名算子。TPC 基准采集在具有完整终态坐标和算子计数的报告上才标记算子数据合格。查询的审计总量、单任务等待和并行算子时间仍各自解释。

1. 补齐获胜 attempt 与可执行 DAG 依赖的同源证据。分别用重试和推测执行查询验证关键路径、排队、实际阻塞、撤销与释放。量化采集路径的 CPU、序列化字节、丢样和保留成本。
2. 冻结 Git/工作树内容散列、Native/JAR、JDK、配置、数据布局和快照、服务拓扑、CPU/RAM、并发与缓存状态。短 SQL 同时用持久客户端测引擎延迟，用独立 CLI 测冷启动。TCP/TLS 建连与连接复用分别采样。
3. Query Profile 读取失败、有实际统计却被错误降级、重复报告、过期和部分覆盖设单独用例。生产级基线要求所有选定模板完成、正确性通过，同步保存错误、取消和释放证据。对热路径测采集开/关的扰动。

### B 把 Worker 任务完成检测从完整报告中拆出

代码证据：`TaskCompletionMonitor.pollUntilFinished` 成功路径每 50 ms 探测一次。`NativeTaskExecutor.isFinished` 持 handle lease，调用 `nativeGetTaskStatus0`；Native 序列化并复制完整报告到 Java byte[]，Java 解析后主要读取状态，失败时取诊断。50 ms 周期意味着单个持续运行任务每秒约 20 次检查上限，实际次数取决于循环耗时和错误退避。终态可见后才开始的释放也会受探测周期影响。

1. 在已有句柄租约上提供轻量 Native 状态读取；运行中走标量返回。检查到失败时读取原有完整报告，正式终态和实际释放继续使用 canonical report/receipt。后续若完成检测延迟仍占关键路径，复用 Native 完成信号唤醒 Worker，有上限地合并通知；配额采样保留独立周期。两个改动分开提交并可各自回退。
2. Java、JNI 实现和导出表同步变更。验证并发取消/关闭、句柄释放竞态、重复报告、失败详情、Worker 重启和慢 Flight 消费者。结果、Shuffle 保留与租户配额释放继续由实际所有权和释放证据驱动。
3. 用 `SqlJniCopyProbe.cpp` 对照每个 attempt 的 JNI 次数、报告字节、序列化/解析 CPU 和 Native 终态到验收释放的 P95。验收要求常态探测无完整报告，状态转换与 release ACK 行为等价，整体物理释放 P95 不退化。

### C 去除短查询的状态轮询等待

代码证据：`SqlShell.consumeAndComplete/poll` 先查 HTTP 状态，未达到条件时每 500 ms 再查；`QueryCompletionWaiter` 对端点就绪和终态各做一轮查询，间隔从 200 ms 指数增长至 5 秒；TPC 桥用 20 ms 循环。若状态在两次轮询之间变更，调用方会额外等待，且不同入口的负载与延迟口径不同。

1. CLI、TPC 桥先复用提交响应中的合法就绪状态，与 JDBC 的立即路径对齐。再给状态读取增加版本化、有界等待，由现有 query state 变更唤醒；保留旧客户端即时 GET 与回退路径。限制每租户并发 waiter 和等待时长，断连、取消和超时解绑监听器。客户端在 Ready 时读 Flight，读完后等待 canonical 终态，最后只由现有执行所有者发起取消。
2. 用空结果、无结果集、端点先就绪、读完后提交失败、旧服务端、多个客户端并发和 Coordinator 切换验证。不让 Ready 和 FINISHED 合并成一个事件。观察状态发布到客户端感知、每条查询 GET 次数、高并发下等待器内存和 HTTP 工作线程。短查询 P95 的变化与引擎执行时间分别报告。

### D 为 Flight 结果建立有界消费

代码证据：`FlightResultFetcher.serializeStream` 读取并重写整个 Arrow IPC 流到 `ByteArrayOutputStream`，结束后 `toByteArray` 再复制；JDBC 和 CLI 走该 API。Arrow allocator 的上限覆盖 Arrow 分配，完整 IPC byte[] 与解码出的行对象还占 JVM 堆。`fetchStreamOwned` 也会逐批重新写 IPC，线程局部缓冲可能保留曾经增长到的大容量。`awaitNextBatch` 每批提交 Future 到 cached thread pool 并阻塞等待。Native `ResultBatchStreamReader::fetchPages` 解码内部 IPC 页后送 Flight；有链页时先线性复制，连续页用带所有权的别名缓冲。

1. 在 Transport 的 Arrow 边界引入有生命周期的直接消费 Flight 批次，删除整流及逐批 byte[] 查询结果接口。CLI 分组显示，JDBC 使用 forward-only 游标，在 EOF 确认查询终态，提前关闭时归还流所有权并取消活跃查询。
2. 将逐批 Future 成本与堆峰值分开测。若有线程或调度瓶颈，再把读生命周期收敛为每流有界的协调机制，保持 idle timeout、绝对 deadline、真实 reader 退出与资源归还。并发容量和等待超时按流算，不把队列饱和误判为网络超时。
3. Native 只有在页解码或链复制有可观占比时再做原型，优先复用有所有权的 Arrow 缓冲。保留类型/向量验证、序列、EOF、ACK、权限撤销和背压，预算计入共享缓冲的保留周期。对宽行、多批、慢消费者、中断、读失败、空结果、Decimal、NULL、VECTOR 比对结果；采集首批、吞吐、堆/direct/RSS、复制字节、活跃流和关闭后的残留。

### E 缩短 split 枚举、分配和 DAG 控制路径

代码证据：`SplitFetcher` 按来源处理并将批次加入全量列表，`DAGDispatchEngine.prepareStage` 在派发前获取这批 split。`StageSplitPartitions.partitionSplits` 排序后逐 split 扫描 P 个分区，时间复杂度为 O(S log S + S×P)，以 split 序列化字符串长度估计负载。Iceberg 完整扫描在 `IcebergSplitManager.planFullSplits` 的 `planFiles()` 结果上构造列表，已有特定范围的 manifest cache。Lance 的分页 split 接口在 Coordinator 汇聚后整体派发。文件大小和内容相差较大时，字符串长度不足以预测扫描偏斜。

1. 第一份变更保持所有分配结果等价，面向高扇出比较线性实现、可复用堆节点的最小堆。按累计工作量和分区编号保留稳定 tie-break，基于实际 task count 分布与 JMH 选择阈值。普通路径有 active workers × 4 上限，要求精确交换 cohort 的路径按计划并行度，分别采样。微测用随机种子 42，100,000 个唯一字符串，长度 70–197；两种实现沿用同一稳定排序，每对测量交替执行顺序。
2. 另开变更引入连接器可提供的物理扫描字节估算，优先用 file/range/row-group 字节及必要的 delete 成本。估算不可得时使用记录原因的既有序列化大小权重。固定快照、过滤条件、重复对象处理、检索 artifact 同组策略和精确 exchange identity。用大文件/小文件混合及 row-group、删除文件和冷热缓存验证最大分区/中位分区耗时之比。
3. 再对分页生产者先限制 Coordinator 每来源在途页数与字节，确保可取消并关闭 split source；针对适合追加的执行模式评估分批派发。如果阶段必须全局均衡或需要事先持久化 cohort，用内存有界的准备路径保留完整覆盖，按模式显式选择。配合 Iceberg、Paimon、Lance 的独立集成测试，检查固定快照、增量/删除/过滤语义、取消和失败清理。
4. `DAGDispatchEngine.scheduleReadyDownstream` 多次扫描就绪阶段，`DAGExecutionState` 的 dispatch records 用 CopyOnWriteArrayList，`ExchangeSourceBuilder.buildExchangeInputs` 按消费分区重新遍历边与已派发记录。以高扇出 trace 定位重复扫描和复制；确认后，为 DAG 构建邻接表和 ready 计数，为成功 producer 建立按 stage、epoch/attempt 的索引和只读快照；复用共享描述，按 partition 绑定变量。保留 BARRIER/流水依赖、获胜 attempt、重试重排、输出就绪和持久化恢复。将分发窗口与现有公平调度与 CDC 调度做隔离验证。

### F 校准规划、存储读和单分区输出

- **规划开销。** `QueryManager` 在执行前做租户和集群准入。`StatementDispatchPhase` 先解析用于路由，`CompilationPhase` 再提取 time travel、绑定会话与快照、应用策略、执行优化。记录每次解析、目录往返、统计查询和优化规则耗时；若重复解析可见，复用同一查询的语法产物并核对规范化与 SQL 位置，必要时继续为改写文本解析。按认证会话和固定快照合并可复用的只读元数据访问。跨查询共享仅考虑带明确版本的非授权语法/物理元数据；每次执行仍绑定当前权限、列掩码、行过滤、快照与凭据。比对 Time Travel、带策略的查询、CBO 和计划输出字段。
- **扫描及算子。** 现有 Iceberg 路径已下传过滤和投影，Native 存在 `FilterDecomposer` 和动态过滤。对 Iceberg Parquet、Paimon 含 delete 组件、Lance、S3/OSS 和本地分别测 manifest/对象请求次数、读字节、file/row-group 跳过、解码 CPU、blocked 原因、spill 和 skew。用冷/热固定快照区分元数据、I/O 与计算，记录动态过滤到达时间及此前已发出的读。读放大占主导时先修剪裁和过小文件，再按 connector 与存储延迟调并行读或字节预算；Join/Aggregation/Sort 被证实占主导时再验证统计误差、join 选择、数据倾斜、spill 阈值。任何物理优化都比对已固定的行集、类型、顺序语义和删除可见性。
- **串行输出。** `LeafStageDispatcher.taskCount` 将无下游、无 Shuffle 配置且非分布式检索的 `LIVE_QUERY` 末级固定为一个任务。纯扫描或宽结果可能把可并行工作压到一台 Worker。先对小聚合、纯扫描和全局 ORDER BY 分别测末级 CPU、首批、出口带宽及上游等待。若瓶颈成立，先把允许并行的扫描/投影拆至上游、保留单个结果端点；验证增加的 Exchange 是否划算。多端点 stream/merge 另立设计，只针对有明确收益的负载，并验证全局排序、LIMIT、取消和一次性完成。常量、小结果、全局聚合等保留现有效率。

### G 降低 Shuffle 尾延迟和资源放大

`DAGDispatchEngine` 可在流水配置及非 durable 模式下提早调度下游，durable 有单独的调度条件。Native 本地读的背压与远端发布/读取链路不同。`StreamingBatchChunkValidator` 按 chunk 更新 digest 并校验，`PangrovaPartitionRead` 读取已发布分区。历史远端读的尾部信号需在独立服务、多机与高并发下复测。

1. 固定分区大小和倾斜分布，分别跑当前支持的 local 与 remote 模式。追踪 producer 末批到发布、消费者等首包、对象存储读取、TLS/gRPC、验证/解压、最后一分区到下游 Stage 继续和物理回收。采集 inflight/spill/retention 字节，并把无消费、慢消费和 retry 后回收独立计量。对随机和重复值区分编码/压缩收益。
2. 仅对已定位阶段试验。有网络/存储等待时对已发布分区做字节有界的 read-ahead/并发读；编码或复制占主导时检查 batch 切分、codec、带所有权分段缓冲；热点占主导时用字节分布调整分区图，在适用 join 验证 skew 处理。每次只变一项，先比较同一协议、校验和提交语义。
3. 保持 ticket、grant、digest、receipt、attempt fencing、发布及 EOF/ACK 顺序。运行 `NativeExchangeTransportExternalIntegrationTest`，以及两个真实 Worker 上的慢消费者、重试/推测、服务重启、部分读、损坏、取消和租约过期用例。验收比较阶段 P95/P99、MiB/s、RPC/存储放大和各队列字节高水位。

### H 限制写入放大与后台负载干扰

- **DML 与 COPY。** Relational DML 走 Native 写出与 Coordinator fenced commit。受保护的 UPDATE/DELETE/MERGE 有 copy-on-write 替换及保留幸存行的约束。`CopyNativeTaskInput` 校验并物化固定版本 Parquet 后交给 Native。测 COPY 物化与重复磁盘读、DML 读/写放大、每次 commit 文件数、journal 锁等待及快照提交时间。若写放大主导，先验证影响文件的精确裁剪、目标文件尺寸和 file count；连接器级行删除或更新能力须单独证明语义和兼容性。若 COPY 临时文件主导，先确认拷贝字节、哈希与开销，再设计保持精确版本和完整验证的有界读取路径，涵盖释放和校验失败。
- **HA 与恢复。** 将 PostgreSQL journal、连接池、fence 与外部目录提交分别计时。按同表/异表并发和失败点采集锁等待、pool 等待、提交及恢复时延。如果证实 SQL/索引问题，针对真实查询用 `EXPLAIN (ANALYZE, BUFFERS)` 在隔离数据验证，保持锁顺序、fence、幂等令牌、未知结果与 outbox；批量减少往返仅在原子性和 crash/recovery 用例通过后采用。
- **CDC、检索、维护。** 测 CDC source lag、Worker ingest/flush、目录提交和日志确认，保持 offset 在持久提交后前进。检索拆分 artifact 解析/读、fan-out、merge 与 provider（含 rerank）等待，测 recall/Top-K 确定性；异步索引、物化刷新、压缩单独统计执行与前台 SELECT 受影响的 P95。用固定语料、固定 provider/模型版本和分离的工作负载分别测满载、混跑和取消，目标是确认共享池、连接、I/O 或内存的竞争点。只有证实相互干扰才按租户/工作类别设置有界资源预算。

## 验收矩阵和上线

| 场景 | 负载和对照 | 必查指标 |
|---|---|---|
| 控制链路、短查询 | 持久客户端和独立 CLI，`SELECT 1`、小过滤、Prepared/JDBC；空载及多租户混跑 | 端点感知、首批、完整结果与终态 P50/P95/P99，状态 HTTP/JNI 调用、CPU、真实释放延迟 |
| 高扇出分片与扫描 | 10³/10⁴/10⁵ split，分区数按实际拓扑覆盖；小文件、倾斜、冷/热、三连接器与多 Worker | Coordinator 枚举/分配 CPU 和内存、派发到 ACK、真实扫描及 tail partition、读取放大 |
| 分布式 JOIN、聚合和排序 | 先用现有 Q9/CBO 冒烟定位，再按 TPC 方法跑 SF1 正确性、SF100 单流和 1/4/8 流、SF1000 多机与强扩展 | 正确性摘要、spill、有效网络字节、读写等待、吞吐、完成率和尾延迟 |
| 大结果与慢客户端 | 窄/宽行、空/可空、10 MiB/100 MiB/1 GiB 级；CLI/JDBC/REST、断连和 deadline，以及并发消费 | first/last batch、吞吐、峰值堆/direct/RSS、在途字节、关闭后所有者和缓冲数 |
| 写入、恢复和共用资源 | COPY、INSERT SELECT、低/高命中 DML、并发 CDC、检索、维护任务；重启、慢对象存储与慢 journal | 读写放大、commit/offset lag、未决写、前台 P95、recall、取消后实际 drain |

1. 用仓库 [TPC 方法和命令](../testing/tpc-benchmark-methodology.md) 固定数据身份、参考结果和同源产物。阶段基线用三组独立窗口，候选和基线交替次序。正式比较保留原有专用主机尾部校准；只有样本量支持时报告 P99。已有 JMH 和微测适合定位局部开销，集成/E2E 用于兼容性和跨进程语义，规模试验用于性能门禁。
2. 每份实现先跑 owning module 的最近回归，用相同负载比对结果及资源退出，再扩大范围。JNI、协议、Native 或执行边界变更按仓库规则跑 Native build/tests、重新打包、Java consumers、真实多进程 SQL/Smoke；Native 所有权和并发加 ASan。分别覆盖取消、失败、重试、推测、慢消费者和混合版本。涉及新行为时随代码维护对应的 SQL/API、架构与运维文档。
3. 建议首轮项目验收目标，待基线冻结再确定正式数值。目标工作负载的阶段成本至少改善 15%，或短查询/大结果的端到端 P95 至少改善 10%，且效果超过独立窗口噪声。小规模、冷缓存和非目标负载的 P95/吞吐不得超出现有回归门禁；尚无适用门禁的场景，先依据基线噪声设置预算并书面固定。大结果流式路径峰值随在途窗口受控，随着总结果增长无同量级线性堆增长。结果差异、失败率增加、资源未释放、重复提交或 offset 错误触发回退。
4. 对影响运行路径的批次保留可验证的旧版本或兼容入口。先隔离环境，再少量租户，最后扩大覆盖；按同构流量比较 P95、失败率、排队和资源释放。先暂停扩量，排空在途查询，回退对应功能或回滚同源的 Java/Native 产物。等待器与 Flight 协议兼容路径先完成混部验证。状态数据库或协议若需要变化，采用可在混部期间读取的增量演进，等回滚窗口结束后再收敛旧路径。

## 本次验证范围

- 当前 Java 分片实现和最小堆原型完成 3 个独立 JVM 对照，并逐分区比对分片及顺序。
- 仓库基准 Python 脚本测试通过 113 项。使用临时 Python 3.12 环境与仓库固定依赖。
- 现有本地 Native 测试二进制的结果流、所有权、Shuffle 边界/背压和任务控制通过 33 项。本次未重新构建或打包 Native，也未执行全量生产回归。
- 当前工作树的多机 SQL 端到端部署基线留给 P0。历史导出、当前静态路径和分片微测作为本次归因的输入。

## 设计级根因与实施计划

以下工作项细化前面的 A–H。代码路径以 2026-11-21 第二次检查的工作树为准。同步调用、容量设置和事务粒度可从代码确认。对其他查询的延迟、吞吐和峰值内存影响按各项验证矩阵量化。

| 实施顺序 | 设计根因 | 最小交付 | 依赖 |
|---|---|---|---|
| 0 | 端点就绪、完成通知、状态 ACK 和物理退出混在同一段耗时 | 将关键等待点记入 trace，创建专用基线并加入确定性并发夹具 | A |
| 1.1 | Worker 提交线程承担启动、同步状态 RPC，过载时承担完成监控 | 分离启动与 HTTP 的线程归属，完成监控改为短任务，发布与本地释放独立推进 | 0、B |
| 1.2 | Coordinator 状态回调驱动下一轮 I/O 和任务扇出 | 将阶段推进移到有界调度执行器，逐分区限流派发 | 0，可与 1.1 独立推进；流量压测覆盖两项组合 |
| 1.3 | 结果释放由通用扫描器发现并同步执行 | 批次/reader 实际退出触发定向回收，保留慢速巡检 | 0、1.1 的状态发布边界；与 D 联测 |
| 2.1 | 路由使用历史路径代替缓存驻留，资源估算依赖滞后快照 | 修正信号语义，按 Worker/attempt 计入尚未反映的在途容量 | 1.2 的派发与退出边界、E |
| 2.2 | 每个 Shuffle batch 锁住同一个 query scope 两次，读取逐批 GET | 先去除读取冗余复制，量化按 batch 的事务和对象操作，再试验有限大小的 segment | 0、G；可独立推进，涉及协议时再做混部 |
| 2.3 | 计划和直连交换元数据随分区数反复传输和绑定 | 先加大小预算和压测，证实开销后拆出可复用计划或 producer 描述 | 0、1.2、E |
| 3 | 注册表全局锁、外部 Shuffle reader、扫描准备存在竞争 | 按 trace 单项验证，确认后隔离各类工作和所有权 | 对应的 1.x/2.x 指标与测试完成 |

### 1.1 Worker 启动、完成检查和报告是三个不同的容量域

**调用链。** Worker `TaskResource.executeSubmit` 同步调用 `TaskManager.submitTask`，在返回 HTTP ACK 前经过 `TaskSubmissionHandler.submitReserved` 和 `TaskLifecycleManager.executeTask`。后者创建 Native executor、绑定 exchange、注册必要的 Shuffle 服务，并由 `startExecutor` 同步发布 RUNNING。`RemoteTaskStatusReporter.sendPost` 调用阻塞 HTTP，完成监控发布终态也使用同一同步发送路径，终态失败会在原调用中退避。最典型的跨进程等待是 Coordinator 等 Worker 的提交 ACK，同时 Worker 提交线程在等 Coordinator 的 RUNNING 响应。现有代码在发请求前已登记任务身份，链路支持提前报告，但此类往返会占住提交线程。

`WorkerTaskBootstrap.createOrchestrationExecutor` 用 2 个 core、2×CPU 个 max、有界 LinkedBlockingQueue 和 CallerRunsPolicy。监控提交是长循环，队列填满前只有 core 工作。用 JDK 17 和同参数的 4 核等效队列复现，18 个阻塞监控在途时是 2 个运行和 16 个排队，第 25 个提交在调用者线程执行。原型替换的是监控体，未测量实际 Worker 并发或达到饱和所需的请求数。若 Native 已启而监控排队，完成和回收都被延后。若提交线程进入 CallerRuns，则 ACK 要等监控退出。OUTPUT 需要客户端读取才能继续时，ACK、结果端点可见和 Native 退出可能形成等待环，需要用真实 Worker 复现触发条件。

**实施。**

1. 第一份变更改完成监控的工作单位。以已接纳 attempt 为索引，在 Native 或 Java 后端发出轻量事件，或由短时扫描做一次有界检查后让出线程；保留独立的执行配额巡检。移除任务内持续 sleep 与 CallerRuns 的组合。接纳 task 时同时取得完成追踪额度，饱和在创建前由既有 admission 语义处理。事件顺序、丢唤醒与后台巡检保底加入测试。
2. 用有界发布队列承接 RUNNING 和终态报告。冻结与精确 attempt 对应的报告后，由发布器处理 HTTP、退避和 ACK。不得用普通淘汰策略丢弃终态或 drain receipt；队列额度在任务接纳时计入，持久恢复模式沿用可恢复确认。RUNNING 顺序与终态 report sequence 必须稳定，完成监控线程与 Native 清理可独立推进。记录本地物理释放和 Coordinator 接受释放的两个时间。先验证成功与失败的 canonical report、重复 ACK、取消、断网、重启和关闭期间的最终可达性。
3. 再把可能阻塞的启动准备交给有界启动执行器。既有客户端继续以真实服务就绪和正确的 endpoint 作为 OK 的含义，HTTP 可异步等待就绪以归还处理线程。若引入更早的准入回执，作为版本化能力单独提供，Coordinator 收到真实端点前不发布可读结果；旧 Worker/Coordinator 继续走同一验证过的等待语义。
4. 在同一 Worker 上让旧监控额度全满并由 latch 控制状态 RPC、COPY 源或 Flight 消费，再发短查询和取消。记录 reserved→started、started→ACK、监控入队→首探测、完成→释放、报告待发→ACK。验收要求零个监控在 HTTP/native driver 线程运行，状态网络故障不会阻塞本地完成检查，饱和提交能在明确时限内返回就绪或受控拒绝。任务容量与资源实际释放均按原有身份验证。

### 1.2 DAG 从同步回调变为可背压的阶段推进

**调用链。** `StageStateMachine` 在 CAS 成功后直接调用 listener。 `DAGDispatchEngine.awaitStageDispatch` 用 `allOf(...).thenRun`，通常在完成最后一个 future 的线程或附加 continuation 的线程上执行。 `finishStageDispatch` 先整理 ACK 及 cohort，切到 RUNNING 后，listener 可同步进入下游 `scheduleStage → prepareStage`，再做 Substrait、split I/O、授予资源并逐分区构造和派发任务。当前阶段的 `registerTasks` 在 RUNNING 调用返回后执行。FINISHED 监听也能进入下游准备，从状态上报路径传导到由 Coordinator 同步处理的报告 ACK。DML 的 deferred completion 另需考虑 journal/fenced commit，同时保持查询终态在提交成功之后。JDK 17 实验直接编译当前 `StageStateMachine`，让 listener 在 latch 上等待，观测到 RUNNING 已可见、触发线程仍未返回。

**实施。**

1. listener 只记录原 stage、generation、变更和可调度信号；每查询按序处理状态并在已有 DAG 所有权边界合并就绪检查。选择与现有调度策略一致的公平/有界消费，优先让 cancel seal 与完成信号有进展。避免新建一个全局串行执行器去运行 split I/O；有界后台准备在独立池运行，准备完成后回到查询所有者校验 generation。每个事件持有必要的操作所有权，超时或取消释放入队、运行和回调三类义务。
2. 固定前序契约并验证次序。先建立已签发尝试的身份索引和 cohort，再处理早到报告或发布阶段可读状态。保持 fast Worker 在提交 ACK 前上报、ACK 超时后回来的原尝试、durable winner 恢复、推测重试和 BARRIER/流式依赖。对每 Worker 和每租户/查询限制任务准备字节和 HTTP in-flight，在 ACK 或有证据的封存后移交额度。拥塞的子阶段可以等进展，已持有输入资源的 producer/consumer 必须留有解除背压的执行额度。
3. 控制面与 DML 最终提交分两份变更。协调报告仍由同步身份判定给出 ACK。若要把 durable 发布切出报告线程，引入明确的“已接收”和“已应用”含义，并在可恢复的存储边界记录；旧 Worker 继续得到原有语义。只在提交 fence、journal 和 connector receipt 都成功后进入查询 FINISHED。
4. 在两个查询并发时暂停一个下游的 split lookup、Worker ACK 或目录 commit。另一个查询、取消和已入站状态 ACK 应继续。新增 ready 重复合并、快速报告、stage 重试、取消早于迟到 ACK、控制权切换、队列关闭等回归，复用 `DAGDispatchEngineStageRetryTest`、`DAGDispatchEngineCancelFuturesTest`，并用独立进程覆盖生产 HTTP。

### 1.3 用实际 Drain 事件归还资源

`TaskLifecycleManager.monitorResultTask` 在任务逻辑完成后保留 Native executor 及结果缓冲。 `TaskCleanupScheduler.ConsumedResultSweep` 每 100 ms fixed delay 最多访问通用注册表 64 项，遍历中同步执行 cleanup/retry；周期性全量清理也走同一个单线程 executor。仅就 10,000 个稳定条目和满批遍历计算，走完一圈最短约 15.7 秒，尚未计入探测或释放时间。这描述扫描周期，单个任务实际回收等多久取决于位置、全量清理时机和所有权状态。 `QueryManager.releasePhysicalQuery` 将 reader pin、journal 清理和 admission 释放串联，前序等待会延长实际归还时间。

1. Native 在 producer 已结束、grant 已撤销、队列已清空且所有 reader/batch 均已退出后产生带精确 attempt 的 drain 信号，根据现有真实证据判定，由每任务所有者合并一次。通知独立于持有 Native mutex 的路径。Worker 定向进入回收，使慢释放或 HTTP 报告等待无权阻塞通用巡检。无信号、关闭和旧版本仍走低频扫查。
2. 将释放服务分成可追踪小阶段，独立度量 revoke、reader/batch drop、Native join、Worker receipt、Coordinator 接收、pin/journal 与槽位释放。Native 内存、结果保留和租户容量的任何提前拆分都须在各自真实资源退出后生效；保留固定快照 reader pin。若慢客户端长期占用，先给结果保留单独的 byte/query 预算。只有成本证据充分才设计结果临时存储，按明确的所有权转交点接管 TTL、撤权和读取。
3. 复用 `WorkerTaskRetentionWiringTest`、`ResultOwnershipStateTest`、`ResultBatchStreamReaderTest`。增加注册表混合存量、多路慢客户端、阻塞 release、Flight 断开以及重复/迟到通知的定向回收测试。验收要求正常消费后的 P95 回收延迟和通用注册表大小解耦，逐 attempt 证明实际资源与租户槽位归还。

### 2.1 将 Worker 放置建在可验证的容量和缓存信号上

`TaskSubmissionHandler.recordSplitFileAccess` 在任务执行前把每个 split 路径记进 `RecentFileAccessTracker`，心跳将其作为 cached paths 发送。 `DefaultWorkerSelector.selectByCachedFiles` 根据这批路径的匹配数直接选 Worker，再走 rank/哈希路径。提交后尚未读取、失败、取消、不同版本和已被淘汰的文件都能造成虚假亲和。重复路径还会反复计分。与此同时，每分区取 Worker 列表并计算 cluster memory。 `LeafStageDispatcher.buildTaskRequest` 把并行阶段数固定传 1。StageResourceGrant 是估算值，Worker 的实际接纳仍是权威。

1. 路径样本继续兼容旧心跳，明确表征近期调度历史。将实际成功读、缓存入驻或命中与快照/对象版本、过期时间、Worker incarnation 关联，优先复用 Native 计数和采样。按去重后的预计可命中字节计分，受容量、租户池、local shuffle 硬约束和 capability 约束。影子运行只比较候选，不改变现有选址，采集实际命中字节和排队成本再切换。
2. 在 1.2 的派发窗口取一次按 Worker incarnation 的容量快照，并计入尚未进入心跳观测的在途 grant。账本至少区分 prepared、issued、accepted 和实际 drained，按版本水位避免 ACK 与心跳重复扣减。Coordinator 的账本优化负载分配，跨 Coordinator 仍以 Worker 原子接纳和 exact grant 为准。验证新旧 Worker 混部、(CPU/IO/内存)不同类型、慢心跳、同 Worker 聚集、查无可用容量和租户公平。
3. 用冷热读混跑和多个大 stage 同时开始比较每 Worker 分配、实际命中字节、拒绝率、峰值内存、spill、短请求 P95。固定成员、快照和物理布局，同时报告取候选表次数、路径匹配次数及单 stage 调度 CPU，避免把缓存性能和筛选 CPU 混为一项。

### 2.2 Remote Shuffle 以 batch 为事务、对象和校验单位

单个已完成 batch 在 `ShuffleServiceEngine.acceptBatch` 中创建 write intent、执行 immutable PUT、完成 intent。PostgreSQL 两条写事务均调用 `lockWritableAllocation`，检查 execution anchor、锁同一 query scope 行与 allocation；完成事务还检查 attempt、重复 batch、连续序号，写 batch 并递增版本。同查询的并行 producers 因此在元数据操作处有共享锁竞争，网络 PUT 位于这两笔事务之间。读取 `ShuffleReadPump.preparePayload` 分页查询元数据，但每个 batch 仍要复核读 pin、执行一个对象 GET 和整批 digest。 `batchFrame` 为 chunk 创建 ByteString 再 `toByteArray` 做 SHA-256，第二次拷贝可独立消除。以 1,000 个不同的成功 batch 为例，仅这段写路径就有 2,000 笔元数据事务和 1,000 次 PUT，不含失败、重放或关账。

1. 第一份变更用既有 digest API 支持对 chunk 字节范围或只读 ByteBuffer 计算相同校验，保留相同线协议。按服务读并发、chunk 大小、ByteString 分配与校验 CPU 验收，并对照已有生产服务集成测试。
2. 用真实 PostgreSQL+对象存储跑同查询多 producer 与多查询独立 producer 两组负载，分别记录连接池等候、anchor/scope 锁等待、intent/complete SQL、PUT/GET 和校验开销。固定总字节，改变 batch 数或每段目标字节。增长主要随对象次数或锁等待时，再原型化 segment。
3. Segment 以单个 producer attempt 的连续分区区间为单位，约束最大字节、批数和驻留时间。先登记段 intent，再提交 immutable 段，最后原子写入批索引/offset 与 receipt；消费者可做有界 range read 或按窗口读段。每个 canonical batch 的序号、大小、digest 和已验证 EOF 语义保留。遇到超时但对象落盘、部分 range、同内容重发、旧/败选 producer、cleaner 竞态时都按原 fencing 处理。如果锁粒度还要细化，必须证明 query seal 与 write intent 的原子边界和锁序。
4. 分别报告每 GiB 的对象操作和事务、首批与尾间隔、读放大、超时率及 spill/保留高水位。对旧新布局保证受支持的读取与清理路径，已有分区按创建时的格式读到过期。混部、取消、GC、重启后重放通过再扩大流量。

### 2.3 控制面负载随 exchange 扇出和计划大小增长

直连 Shuffle 每个 producer 在 Worker 提交 ACK 中回传各分区的签名票据。 `ExchangeSourceBuilder.directInput` 为每个 consumer 分区逐 producer 写入一条带完整 attempt 和票据的 source。矩阵的物理边数由计划决定。256 个 producer × 256 个 consumer 对应 65,536 个 source 描述，尚未计入重试、producer ACK、重复的 endpoint 字段和 base64 处理。External Shuffle 用不同 binding，仍需检查 `buildExchangeInputs` 在外部路径提前扫描 producer list 的成本。

同一阶段的 `planBytes` 进入每个 `TaskRequest`，`RemoteTaskDispatcher.dispatchAsync` 编码整个请求体；Native `PlanExecutionDriver` 再为每任务运行 artifact preparation 与 Velox plan conversion。可能复用的模板与必须绑定的 split、operator 权限、交换和资源参数在这条链上叠加。 `CoordinatorTaskTracker`/身份 registry 还需保留实际签发请求用于重试、drain 和 fencing。

1. 先利用 E 的 DAG/producer 索引，将外部模式跳过无用扫描，并为已定义的直连边预测 ACK / request 字节和在途签发成本。记录按阶段的计划体大小、exchange 描述字节、序列化次数、Native parse/convert CPU、在途与 tracker retained bytes；测试同一 plan 的 1/32/128/256 分区。先减少重复遍历和驻留，再判断协议压缩是否值得。
2. 若传输占主导，Worker 按能力协商加载经过校验的只读 stage 模板；task 仅引用确定的内容摘要并带原有 attempt、所有权、授予和绑定。模板按 byte/TTL 限额，在任务和实际 release 之前有明确 pin；旧 Worker 可接收现有完整请求。stage plan 的授权表达式、产物和 grant 作用域必须匹配。若 Native 转换占主导，单独证明转换过程哪些输入仅依赖不可变模板；可复用的是解析或纯编译产物，执行状态、memory pool、connector 权限、split/reader 和 artifact 绑定按 attempt 保持独立。
3. 若直连票据是主因，先复用跨分区重复的 producer 身份和 endpoint 元数据。按需取票据或共享分发目录是后续版本化设计，必须保留每个 consumer/partition 的执行和读权限，以及获胜 attempt 的票据生命周期。验证缺失模板、冷启动、计划变化、epoch 切换和生产者重试。报告实际的物理边数与传输字节的变化。

### 3 竞争路径补充验证

- **锁与回调。** `CoordinatorTaskTracker.unregisterDrainedTaskOwned` 在进程级 `drainRetirementLock` 下可能触发 `completionSignal().completeExceptionally`。有同步 continuation 时，持锁时间包含该回调。 `TaskIdentityRegistry` 也在全局 monitor 下处理发行、收到报告、seal 和 retirement。先用线程和 JFR monitor 事件区分等待与持有，注入慢 continuation 验证另一查询是否受阻。最小修复是锁内固定证据和后续动作、锁外完成 future，再根据实测按 execution owner 隔离注册表锁。保留 compaction 与 drain 验证的原子关系和锁序。
- **远端 reader 调度。** `NativeExchangeTransport` 先打开 Shuffle reader、挂到 session，再将每个 input 的持续读取循环放入固定线程池。远端读取慢或 push 受下游背压时，线程与 reader 所有权可能一起被占用，其他读取只能排队。先对比外部 Shuffle 的 admission、reader open/first read、队列等待、每次 push 与真正关闭的时间。若出现无关流相互阻塞，调度前一起预订 reader 容量与字节窗口，再逐次处理有界帧或运用读/写就绪回调让出执行线程。保持 pending 与运行中的 RPC 都能取消，实际退出后才归还 credits。
- **存储清单和调度分片。** 按 E/F 记录 cache miss 并发。同一 Iceberg 固定快照、同读取 schema 的无约束计划若反复同时执行 `planFiles()`，再做受限的同 key 共享加载。请求自己的 reader pin 和取消独立，共享只读结果按身份和字节预算保留。按约束方式/格式分别验证，避免共享一次目录授权或把仅局部过滤的结果复用于全表计划。

## 设计变更的合并条件

- **基线先行。** 本地 latch 实验只证明线程行为，静态事务计数描述固定代码路径。生产量级用现有 TPC 驱动、独立 Worker、真实 PostgreSQL 和对象存储复测。阶段耗时报告 P50/P95，样本足够时才报告 P99。每项记录同时跑短查询和饱和场景，验收不得用总体均值遮盖单租户饥饿。
- **逐份交付。** 先合观测与回归夹具，后合执行路径。每份 PR 写明改变的队列、等待边、所有者、容量/字节上界和退出条件。用集中检查衡量 callback thread 是否阻塞、队列是否拒绝/泄漏、是否少了真实完成或 drain 事件。涉及 JNI、所有权和运行时的工作需完成 Native 重建/打包、Java consumer、独立进程 E2E、Smoke 和适用的 ASan。
- **退出与混部。** 兼容入口按 producer/consumer 两侧能力启用。已接纳任务沿原结算状态机退场，回退先暂停新流量并排空受影响 cohort。任何结果差异、错误 ACK、未归还资源、失去可恢复的 commit 证明或异常的 rejection/timeout 上升都触发回退。保留原始基线、故障样本和每项前后对照。

## 本轮验证

Corretto 17 确定性实验通过两个断言组。第一组编译当前 `StageStateMachine`，验证状态变更线程被同线程 listener 挂起的同时，CAS 更新已可见。第二组按 `WorkerTaskBootstrap` 的 4 核等效参数构造真实 JDK 线程池，验证 2 线程加 16 排队以及第 25 个提交的 CallerRuns。两组使用闩锁证明调度顺序。端到端的触发频率、耗时与生产排名留给第 0 阶段。
