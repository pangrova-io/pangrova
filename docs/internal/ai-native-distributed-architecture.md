# Pangrova AI Native 分布式架构研究与实施规格

> **Status: Target architecture.** 本文定义目标边界和验收规格，不表示所有能力
> 已在当前工作区闭环；当前事实与定向证据见 `ai-native-roadmap.md` 第 0 节。

## 1. 范围与结论

本文定义 Pangrova AI Native 基础设施的唯一目标架构。范围包括模型对象与调用、AI SQL、Embedding、
VECTOR、全文与混合检索、索引生命周期、FILE/BLOB 多模态、Semantic Cache、Feature Store、治理、
可观测性和评估。

结论是采用“高可用 JVM 控制面 + 全阶段 Native 数据面 + 持久湖仓/对象存储”的三层结构：

- Coordinator 只负责认证、授权、目录、快照绑定、逻辑/物理规划、阶段拓扑、资源租约、重试、提交和
  有界状态汇总。它不得接收候选行、向量、Embedding、模型正文、检索分数或 rerank 输入，不执行
  Top-K、RRF、Linear、Cross-Encoder 或结果融合。
- Worker JVM 只负责进程生命周期、统一协议校验、JNI 句柄和 Native 任务监督。查询数据不穿过 JVM
  对象模型，跨 Worker 数据经 Native Arrow Flight exchange 传输。
- C++/Velox 执行扫描、过滤、投影、精确距离、ANN、BM25、候选填充、融合、rerank、全局 Top-K、
  AI batch invoke 和结果序列化。分区局部候选先在叶阶段有界化，再经 exchange 进入 Native 单例或
  分层归并阶段，Coordinator 不参与数据归并。
- 所有功能使用同一套 Capability、Relation Detail、Artifact、Model、Policy、Resource Grant 和
  Result Envelope 契约。功能差异由 capability descriptor、具名参数、具名 option 和不可变 artifact
  format 表达，不再新增 vector/BM25/model 专用任务协议，也不保留旧协议兼容分支。

这不是“未来不会出现任何问题”的不可证承诺。可落地的保证是：把正确性、容量、兼容性和失败语义
变为可执行契约；任何未知能力、格式漂移、快照不一致、覆盖不足、预算不足或统计不完整均 fail closed，
再用容量测试、故障注入和逐版本基准证明发布门槛。

## 2. 行业架构复核

### 2.1 Snowflake：控制服务、计算和存储分离

Snowflake 官方架构把持久数据库存储、独立 MPP compute cluster 和 cloud services 分开。Cloud
services 承担认证、访问控制、元数据、解析和优化；每个 virtual warehouse 是互不共享计算资源的
独立集群。官方还说明 cloud services 使用跨可用区的无状态计算和高可用分布式元数据存储。这个边界
支持 Pangrova 把 Coordinator 明确限定为控制服务，而不是 JVM 数据归并节点。[^1][^2]

Multi-cluster warehouse 通过动态增加独立 cluster 处理并发，不把单条查询结果拉到控制服务；Query
Acceleration Service 则把可并行的扫描/过滤片段卸载到共享 serverless compute，并按实际需求和资源
可用性申请并行度。这说明“控制面决定租约和拓扑，数据面执行 fragment”比在 Coordinator 融合更适合
处理高并发和尾延迟。[^3][^4]

Cortex Search 是独立对象，创建时物化 source query、构建索引，并以 `TARGET_LAG` 驱动增量刷新。
索引和 serving 可独立暂停；有主键时只处理变化行，同时周期性全量 rebuild/compaction；查询使用向量、
关键词和 semantic reranking 的 ensemble。刷新 warehouse 与低延迟 serving compute 也彼此分离。
这为 Pangrova Search Service 提供了对象、刷新、服务和压实四个独立生命周期的直接基线。[^5][^6]

Snowflake 将 embedding token、刷新 warehouse、serving、存储和 cloud services 成本分别记录，并提供
按服务的使用历史和预算。Pangrova 因而不能只记录“调用次数”；资源 grant 和 usage 必须覆盖 provider
并发、输入/输出 token、媒体字节、对象存储读写、候选行/字节、Native 内存和 spill。[^5][^7]

### 2.2 Spark 与 Gluten：控制流保留，计算整体下沉

Apache Gluten 的基本原则是复用 Spark 分布式控制流，把物理计划转换为 Substrait，经 JNI 构造
Native operator chain；底层采用 Arrow 表示，并提供统一 Native 内存管理、列式 Shuffle 和 Native
metrics。关键价值不在 JNI 本身，而在一个 stage 内连续执行 Native pipeline，避免逐算子 JVM/Native
往返。[^8]

Gluten 复用 Spark shuffle service，但在数据面使用 columnar exchange。其 Velox 配置进一步暴露
split preload、I/O 线程、remote-read coalescing、flushable partial aggregation、spill、SSD cache、
动态过滤和 shuffle batch merge。这些机制表明大规模 Native 查询必须同时治理 CPU slot、I/O 并发、
内存、spill、缓存和 batch 粒度，不能只设置一个 query memory limit。[^9]

Spark AQE 使用运行时 map-output statistics 合并过小 shuffle partition、拆分 skew partition，并在
运行时改变 join 策略；动态资源分配依赖可靠 shuffle 存储或 shuffle tracking，保证 executor 下线不会
丢失中间结果；fair scheduler pool 为并发用户提供权重和最小份额。Pangrova 应借鉴其“统计驱动的下一
阶段重规划”和“可回收 compute、不可丢 shuffle”的原则，但保持已发布 stage contract 和 attempt
fencing，不允许 Coordinator 查看数据 payload。[^10][^11]

Gluten 的 fallback 通过 Columnar-to-Row/Row-to-Columnar 回到 Spark；官方限制文档同时列出语义不兼容
和大量回退条件。Pangrova 是新系统，不需要兼容 JVM 执行语义，因此不采用这种回退：能力协商必须在
调度前完成，任一 worker 缺能力时重选 worker 或拒绝查询，绝不在运行中跨回 JVM。[^8][^12]

### 2.3 Velox 与 velox4j：执行任务与绑定层的边界

Velox 的 `Task` 执行一个 plan fragment，支持并行 driver、动态 split 输入和 ExchangeNode；它需要由
上层提供唯一 task id、destination、QueryCtx、内存池、executor 和 session properties。这与 Pangrova
的正确映射是“一次 Native task 对应一个 fenced stage attempt”，不是“一次 JNI 调用对应一个函数”。
[^13]

velox4j 展示了 Java API 到 Velox type/plan/expression/connector 的直接映射、显式 MemoryManager、
Session、Task 和 split 生命周期，并提供 Arrow Java 互操作。它目前仍明确标为 experimental，且采用
Velox JSON serde；Pangrova 可以借鉴对象生命周期和一一映射，不能以它替换跨节点统一协议，也不应
引入第二套 JSON plan。Pangrova 继续以 Substrait + protobuf 为唯一 wire contract。[^14]

## 3. Pangrova 唯一目标架构

### 3.1 控制面

Coordinator 的状态必须可在任一实例恢复，持久状态包括 query/stage/attempt、resource lease、artifact
build/refresh、model route、policy version、budget reservation 和 atomic commit journal。内存中只保留
可重建缓存和有界运行状态。每个写操作携带 owner epoch、fencing token 和 idempotency key；失去租约的
旧 Coordinator 或旧 task 不得提交状态。

调度流程固定为：

1. 在一个 source snapshot 上完成 SQL、权限、policy、model、artifact generation 和 capability 绑定。
2. 依据 exchange 边界切成 Native stage DAG，并为每个 stage 计算强类型资源需求。
3. 只选择 capability inventory、artifact locality、故障域和资源都满足的 worker。
4. 分发不可变 plan fragment、split、binding、resource grant、deadline 和 fencing token。
5. 接收 task status、metrics、shuffle manifest、artifact manifest 或最终结果句柄等有界元数据。
6. 基于完整性证据启动下游 stage，或取消/重试相同逻辑 partition 的新 attempt。

Coordinator 可以汇总 `expected/completed/failed partition count`、bytes、rows、checksum 和 freshness；
不得解析 Arrow batch、候选行或检索分数。全局 Top-K 是一个 Native singleton stage，必要时在大型 fan-in
前增加树形 partial merge stage，避免单点连接数和内存随 worker 数线性失控。

### 3.2 统一执行契约

一条 TaskRequest 只能携带以下通用结构：

- capability inventory/ref：语义签名、实现 build、placement、execution mode、modality、确定性、重试、
  权限、format 和 limits；
- relation detail：retrieval source/merge、rerank、candidate fill、AI invoke、artifact build 等通用算子形状；
- binding：artifact、model、policy、exchange input、output artifact；
- stage resource grant：CPU、JVM、Native、shuffle、spill、object store、candidate、provider、token 和媒体预算；
- result envelope：完成状态、partition coverage、snapshot、artifact freshness、usage、error class 和结果句柄。

所有 option 必须具名且带逻辑类型；未知 option、重复 option、缺少 option、类型错误全部拒绝。Capability
signature 对参数和 option schema 做摘要，Java catalog、worker inventory 和 Native converter 必须精确
一致。禁止 JSON side channel、feature-local proto、`Map<String,Object>`、默认 provider 和宽松格式探测。

### 3.3 Native 数据面与 exchange

叶 stage 在数据所在 worker 执行 scan/filter/vector/BM25，并将每个 source 的候选控制在明确上限。
候选 schema 至少包含稳定 row identity、source id、source rank、raw score 和 snapshot identity。RRF/Linear
先在 Native 归一化 score direction，再按稳定键去重；Cross-Encoder 只接收已填充且受 token/media budget
约束的 top-N。最后的 Native singleton stage 输出全局稳定 Top-K。

Exchange source list 必须在逻辑 partition 维度去重，旧 attempt 由 fencing token 排除。`no_more_sources`
之前不得宣告完整；require-complete 的下游 stage 只有在每个预期 partition 都有一个有效 checksum 后启动。
超时默认失败，不把缺失 shard 当作空结果。只有 SQL/API 显式允许 partial 时才返回 `PARTIAL`，并附覆盖率、
缺失 partition、source snapshot 和 artifact freshness；精确回退必须在同一 snapshot 的 Native stage 完成。

### 3.4 资源、背压和局部性

每个 stage 先 reserve 再 dispatch。Worker 对 grant 实施硬上限，Native memory pool、candidate heap、模型
batch、object-store reader、shuffle sender/receiver 和 spill writer 都从同一 stage ledger 扣减。内存压力
顺序为释放可重建 cache、flush/spill 可溢出算子、降低 prefetch，再拒绝新 input；禁止 OOM 后隐式转 JVM。

调度 locality 依次考虑已解析 artifact RAM、artifact SSD、source data cache、同机 shuffle 和故障域。
locality 只影响 worker 选择，不改变 artifact 内容身份。不可变对象由 checksum 定址，RAM/SSD 两级 cache
使用 pin-aware LRU，持有 lease 的对象不能被逐出；remote read 必须 coalesce、prefetch 有界并校验 checksum。

AQE 只允许改变尚未启动的 stage：根据 map-output bytes/rows、candidate cardinality、spill 和 skew 选择
coalesce/split、树形 merge fan-in、worker 数和 resource grant。不得改变 source snapshot、capability
signature、artifact generation、policy/model version、score direction 或结果 schema。

## 4. Search 与索引规格

### 4.1 Search Service 对象

Search Service 是 catalog 一等对象，而不是一组松散函数。对象至少固定 source query、搜索列、返回列、
filter attributes、primary key/row locator、embedding model、vector/text artifact format、ranking pipeline、
target lag、refresh warehouse/resource pool、serving policy、budget 和 owner-rights security policy。

生命周期分为 `CREATING / READY / REFRESHING / DEGRADED / SUSPENDED_INDEXING /
SUSPENDED_SERVING / FAILED / DROPPING`。Indexing 与 serving 独立；refresh 失败不能破坏上一个已发布 generation。
只有完整 manifest 通过 checksum、source coverage、format、row locator 和 snapshot 校验后，catalog 才以单次
compare-and-swap 发布新 generation。

### 4.2 Durable refresh 和 compaction

Backfill/refresh 使用持久 page 状态机：`QUEUED → LEASED → RUNNING → OUTPUT_READY → COMMITTED`，
失败进入带 next-attempt 的 `RETRYABLE` 或 `TERMINAL`。每页绑定 source object id 和 source snapshot；重试
写入 attempt staging path，最终由 fencing token 选择一个输出。删除只清理未被 manifest/read epoch 引用的
对象。

增量 refresh 根据 primary key/row identity 只处理新增和变化行，删除生成 tombstone segment。查询读取一个
原子 generation 的 base + delta segments；后台依据 segment count、delete ratio、read amplification 和
cache miss 做 Native compaction。Compaction 不重新计算未变化 Embedding，且新 generation 发布前旧版本
持续 serving。

### 4.3 BM25 全局正确性

每 source-object postings segment 只能保存 document length、term frequency 和局部 posting。局部 document
frequency 不能作为全局 IDF；把每 shard 的局部分数直接做 Top-K 会改变跨 shard 排序。

BM25 generation 因此必须额外包含一个 generation-scoped corpus statistics artifact：全局 document count、
total document length、每 term document frequency、analyzer identity 和 checksum。构建阶段由 Native
map task 产生 source stats shard，经 Native hash exchange/reduce 合并为 corpus stats；Coordinator 只验证
manifest 元数据。查询 worker 同时绑定 source postings 与同一 generation 的 corpus stats，使用全局 IDF 和
average length，再做局部 top-N 与 Native 全局 Top-K。

性能实现采用词典 FST/紧凑 term table、delta/varint posting、block max score 和 Block-Max WAND；短 query
避免为整个命中集合建立 `unordered_map`。所有 workspace 从 candidate/native-memory grant 预约，超限失败
或按明确策略减小 oversampling，绝不返回未声明的近似结果。

### 4.4 Vector、Hybrid 与 rerank

VECTOR 是定维一等类型；DDL 固定 element type、dimension、metric 和 null policy。Embedding provider 的
batch contract 保证输入输出数量、顺序和维度完全一致，要么整批提交，要么整批失败。DiskANN/IVFPQ/
HNSW/Lance 都通过相同 retrieval source 和 artifact binding 表达，format descriptor 决定 Native reader，
不产生算法专用 TaskRequest。

Hybrid query 产生独立 vector 和 BM25 candidate source stage。二者通过 Native exchange 进入 merge stage：
RRF 只依赖 source rank；Linear 必须固定 normalization（例如 min-max/z-score/calibrated sigmoid）和权重版本；
Cross-Encoder/Cascade 绑定精确 model version、policy、batch/token budget，并只处理有界候选。候选正文填充在
Native、同 snapshot 的 table scan 上执行，不通过 Coordinator。

## 5. 模型、Embedding 和多模态规格

MODEL 是 catalog 一等对象，包含 provider、provider model id、不可变 version、input/output schema、modality、
batch limits、timeout、retry class、determinism、credential ref、region/data policy、cost profile 和 lifecycle。
模型名必须精确解析；未知模型或缺权限直接拒绝，禁止选择默认 provider。

所有调用统一为 `AiInvocationRequest / Result / Usage`：request 固定 call id、model binding、typed batch、
parameters、deadline、idempotency key、policy bindings 和 budgets；result 固定逐项 output/error、provider request
id、finish reason、latency 和真实 usage。只有 provider 明确保证幂等，或在发送前失败，才允许自动重试。

SYNC 调用用于有界交互查询；ASYNC 写入 durable invocation job/queue，由 worker 按 provider quota、tenant
fairness 和 deadline 拉取。Embedding backfill 是相同 durable artifact workflow，输出定维 VECTOR segment，
在 source snapshot 和全部 page 完成后原子发布。

FILE/BLOB 只保存受治理的对象句柄和 metadata，不把任意 URL 或大字节内联进 SQL 值。URL ingestion 经
allowlist DNS/IP 校验、redirect 重校验、TLS、长度/类型限制和 malware hook，落到租户隔离对象存储；调用时
以短期签名读取。每个媒体对象、临时对象和 provider upload 都有 owner、retention、引用计数和清理 journal。

## 6. Cache、Feature Store、治理与评估

Semantic Cache key 必须包含规范化请求、精确 model/version、参数、policy/security signature、tenant、
source/artifact snapshot、embedding model 和 output schema。命中仍执行授权与预算策略；value 是加密的不可变
artifact，带 TTL、quality、usage 和 provenance。缓存读取在 provider reservation 前，写入只在完整成功后
原子提交。

Feature Store 由 feature view、entity key、event timestamp、source snapshot、transformation plan、offline
artifact 和 online serving generation 构成。point-in-time join 在 Native 执行，避免训练泄漏；materialize
job 复用 durable artifact state machine，发布和回滚按 generation 完成。

Guardrail 不是普通 UDF，而是模型调用前后的强制 policy stage。调用前执行 RBAC、row/column policy、数据
域、prompt/media size、provider region 和预算；调用后执行 output classification/redaction。每次调用写审计
事件，但正文默认不落日志，仅保存哈希、模型/策略版本、usage、结果分类和可授权 trace reference。

评估对象固定 dataset snapshot、model version、prompt/template version、metric capability、judge model、
随机种子和阈值。执行是可重放 Native/AI batch job；结果包含逐样本和聚合指标、失败类别、token/cost、
latency distribution 和基线差异。发布 model route 或 ranking pipeline 前必须通过离线质量、正确性、成本、
P95/P99 延迟和故障注入门槛。

## 7. 失败语义与可观测性

错误至少分为 validation、authorization、capability mismatch、artifact stale/missing、resource exhausted、
provider throttled/transient/permanent、deadline、data corruption、partial coverage 和 internal。每类明确是否
可重试、是否换 worker/provider、是否计费以及是否允许 partial。不得用 `runtime unavailable` 覆盖根因。

Trace 贯穿 query → stage → task attempt → operator → provider/artifact request。Metrics 至少包括 queue、
lease、driver、scan、cache、remote I/O、shuffle、candidate、merge、rerank、provider、token/media、spill、
coverage/freshness 和 cost；高基数字段进入 trace/log，不进入无界 metric label。Query profile 展示控制面
时间和各 Native stage critical path，但不把候选 payload 回传 Coordinator。

## 8. 分阶段落地顺序

1. 正确性地基：删除旧协议与静默 fallback；统一 capability、result completeness、resource grant、
   Native global Top-K、attempt fencing 和真实测试。
2. Search 闭环：持久 artifact build/refresh、DiskANN、全局统计 BM25、filter/locator、Native Hybrid 与两级 cache。
3. 模型闭环：MODEL、统一 invocation/usage、provider quota/budget、Embedding VECTOR、ASYNC queue 和多模态对象。
4. 数据服务：Semantic Cache、Feature Store、训练数据和增量 materialization。
5. 治理评估：强制 RBAC/guardrail/audit、成本归集、可重放 evaluation、capacity/chaos 门槛和自动优化。

每阶段完成都要求协议 Java/C++ descriptor 一致、Release/ASan Native、Java 全仓、真实 JNI、三 Worker
exchange、Docker/PostgreSQL/Ollama E2E、故障注入和至少 10× 目标规模的容量证明。`assumeTrue`、固定真
断言、组件直连或环境不可用均不计为通过。

## Sources

[^1]: Snowflake. [Snowflake key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts). Accessed 2026-09-11.
[^2]: Snowflake. [Understanding compute cost](https://docs.snowflake.com/en/user-guide/cost-understanding-compute). Accessed 2026-09-11.
[^3]: Snowflake. [Multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster). Accessed 2026-09-11.
[^4]: Snowflake. [Using the Query Acceleration Service](https://docs.snowflake.com/en/user-guide/query-acceleration-service). Accessed 2026-09-11.
[^5]: Snowflake. [Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview). Accessed 2026-09-11.
[^6]: Snowflake. [ALTER CORTEX SEARCH SERVICE](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search). Accessed 2026-09-11.
[^7]: Snowflake. [Understanding cost for Cortex Search Services](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-costs). Accessed 2026-09-11.
[^8]: Apache Gluten. [A Middle Layer for Offloading JVM-Based SQL Execution to Native Engines](https://github.com/apache/gluten/blob/main/README.md). Accessed 2026-09-11.
[^9]: Apache Gluten. [Gluten Velox backend configurations](https://github.com/apache/gluten/blob/main/docs/velox-configuration.md). Accessed 2026-09-11.
[^10]: Apache Spark. [Performance Tuning: Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution). Accessed 2026-09-11.
[^11]: Apache Spark. [Job Scheduling](https://spark.apache.org/docs/latest/job-scheduling.html). Accessed 2026-09-11.
[^12]: Apache Gluten. [Velox backend limitations](https://gluten.apache.org/docs/v1.5.0/velox-backend/velox-backend-limitations.html). Accessed 2026-09-11.
[^13]: Velox. [Task execution contract](https://github.com/facebookincubator/velox/blob/main/velox/exec/Task.h). Accessed 2026-09-11.
[^14]: BoostScale. [Velox4J: Community Java Bindings for Velox](https://github.com/boostscale/velox4j). Accessed 2026-09-11.
