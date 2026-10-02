---
title: "AI Native 历史审计与当前实施状态"
description: "Time-scoped internal audit evidence and implementation status; not a public feature commitment"
---

# Pangrova AI Native 历史审计与当前实施状态

> 目标架构与实施批次以
> [`ai-native-target-architecture-and-implementation-plan.md`](ai-native-target-architecture-and-implementation-plan.md)
> 为准；[Snowflake/Spark/Gluten 行业架构对标](ai-native-industry-benchmark.md)
> 单独记录一手资料及规模设计取舍。本文件保留声明—实现审计账本、历史实测
> 证据和按日期复证的当前状态。

> 时间轴：第 0 节按日期记录增量实施状态（最新为 **2026-10-02**）；第 1–6 节和第 10 节是
> **2026-08-28 历史审计快照**，其中路径、缺陷和命令结果均不得解释为当前代码状态。
> 第 7–9 节是对标与目标规格，第 11–12 节是尚需满足的验收条件。
>
> 报告性质：内部事实审计、根因分析与后续实现规格，不代表公共功能承诺
>
> 审计链路：入口 → 规划 → 协议 → JNI/Native → 分布式执行 → 结果 → 治理 → 测试
>
> “已落地”只表示第 0 节列出的源码链路和定向验证可复证；不等于完整 E2E、R7、R8
> 或发布门禁已经通过。历史段落中的 `PASS`、`FAIL`、`BLOCKED`、`SKIPPED` 保留原意。

> **2026-09-27 范围调整**：数据库交付门槛是大规模写入、持久存储和分布式查询。
> VECTOR/FILE、Embedding 物化、索引、混合检索、Native exchange/reduce、作业发布及
> 数据对象权限仍在范围内；Agent 编排、提示词/模型发布评估、训练数据产品、
> Semantic View/Analyst 与 LLM token/费用 usage ledger 退出数据库核心范围。
> Worker 的并发、内存、超时和网络资源保护仍是必要的执行准入，不等于计费账本。
> 冲突时以[更新后的目标架构范围决议](ai-native-target-architecture-and-implementation-plan.md)
> 为准；下方旧审计和旧 R7/R8 文字只作历史证据，不是新的验收承诺。

> **2026-10-01 最新约束**：用户重新明确 Feature Store、数据库内评估、模型治理与多模态
> 属于内核范围；Agent 编排和 LLM Usage Ledger 仍排除。唯一 Calcite SQL 前端、Substrait
> 下发 IR、Velox 执行、Arrow 互操作/传输是验收目标，**不是当前完成状态**。用户要求
> 按深度调研选择合理的物理表示边界，详见
> [单路径根因与实施规格](ai-native-single-path-root-cause-design.md)。三线只读复证发现
> parser/optimizer/serde/type/function 多个真实差距；不能用已有组件绿灯覆盖这些缺陷。

## 0. 当前实施状态（按日期增量复证；最新为 2026-10-02）

本节覆盖 2026-08-28 审计之后已经落地的生产边界及最近定向验证。若与后续历史快照冲突，
以本节为当前状态；未在本节明确复证的能力继续保持未验收。

| 领域 | 当前可复证状态 | 尚未验收或仍需收口 |
|---|---|---|
| 统一协议 | AI/retrieval 执行链使用 canonical v2 `TaskRequest`、`OperatorBinding`、capability inventory 和 `TaskReport`；Coordinator 绑定、Worker/JNI 解析与 Native plan conversion 共用同一协议族，协议描述符 API 漂移已修正。 | 尚无覆盖全部 AI SQL/检索形态的 public-entry round-trip E2E。 |
| VECTOR 输出契约 | canonical v2 的 `ModelCapabilityContract` 将 capability 与精确 `LogicalType` 绑定，`ModelBinding` 复用同一 output type；`AI_EMBED` 唯一合法语义类型为模型绑定的 `VECTOR(n)`（物理为 required FP32 list 并带维度约束）。Planner、Coordinator admission、Worker 匹配、JNI/Native callback、Java provider 返回与 generated-column build 均对同一维度 fail-closed；没有 vector 专用 task proto 或兼容旁路。 | 已通过 protocol/planner/worker/coordinator 定向测试及 Native 6/6 contract tests；尚需 public SQL、持久化 generated column 与跨 Worker E2E。 |
| Embedding Model SQL 目录与执行准入 | **部分闭环**。`CREATE/ALTER/DROP/SHOW EMBEDDING MODEL` 已接入生产 statement task；缺失身份或 ADMIN 权限在读取 Worker deployment metadata 前 fail-closed。CREATE 只接受 canonical `max_requests_per_second`，要求 provider、endpoint、dimension；Bearer 模式要求非空 `api_key_secret` 和 HTTPS，显式 `authentication='none'` 禁止 secret 且 HTTP 仅允许 literal loopback。目录把同名 active Worker 的 canonical v2 model version 与 SHA-256 signature 持久化为 revision fence。ALTER 强制显式 `expected_version`，只允许可变 serving/route 字段，先捕获不可变 model incarnation，再构造下一 revision、重新验证 exact Worker deployment contract，并以 `name + version + incarnation` CAS 原子持久化 revision 与新 fence；并发 DROP/同名 CREATE 不会把旧 ALTER 应用于新模型。旧的 unbound registry API 已移除。`AiInvocationOperatorBinder` 使用的 `DefaultWorkerSelector` 先要求模型存在于 SQL catalog，再解析 exact Worker `ModelAvailability`，并复验 fence、VECTOR 维度和 max batch；Worker identity 漂移均拒绝执行。Coordinator 只做 authorize/catalog/bind/admit/schedule/fence，provider 调用和数据行仍在 Native Worker。 | 现将 `DROP EMBEDDING MODEL` 的 RESTRICT/CASCADE 均临时 fail-closed 为 `AI-0007`，避免在真实 generated-column 依赖未被原子枚举和级联时误删模型；生命周期尚未闭环。后续按 [依赖与生命周期规格](embedding-model-dependency-lifecycle.md) 实现 durable reservation 与可恢复 CASCADE saga。 |
| 控制面/数据面 | Coordinator 负责规划、binding、admission、调度与终态校验；旧的 Java 向量分片抓取、结果物化和 Top-K merge 生产路径已删除。 | 2026-10-02仍确认 scalar index 在 Coordinator 读行并构建，Native retrieval部分工作在Task创建前执行，write还存在JVM row upcall。尚未满足纯控制面与唯一Velox生命周期；也未通过缺 shard、超时、stale generation、跨 Worker shuffle 的完整故障注入 E2E。 |
| Governed invocation | `AiInvocationOperatorBinder` 将 exact model id/version/capability signature 和内核资源预算写入 binding；Native JVM callback 经 `GovernedAiScalarFunction` 进入唯一 `AiInvocationService`，再按精确配置解析 provider。secret reference 缺失或解析失败时 fail-closed；授权、输入、批次、结果数量/顺序/维度、错误分类和资源准入消费同一契约。OpenAI chat adapter 只声明 TEXT/IMAGE；`openai-responses` 以 `input_image`/`input_file` 发送 IMAGE/DOCUMENT；`openai-transcription` 以 multipart 发送单个 AUDIO FILE。三种方言都复用 governed loader 并在 egress 前精确复验 size/SHA-256，统一携带稳定 idempotency key、受 task deadline 约束；LLM usage/token/cost 统计已从调用结果移除，任务报告只记录实际发送/实体字节等内核资源。文档/音频不再伪装成 chat `image_url`。 | 最近组件专项见0.20；真实外部 provider 的 SQL→Native callback→provider→audit 全链 E2E、在途 HTTP 取消及最终资源回收尚未验收。 |
| FILE/BLOB | `FILE_FROM_BYTES` 与受限 HTTPS import 都进入唯一 `GovernedBlobStore` 治理语义，只返回 checksum 绑定的 opaque `pangrova-blob://` identity。生产配置必须显式选择 S3-compatible durable backend；tenant-scope/SHA-256 内容寻址、`If-None-Match: *`、HEAD size/checksum/SSE/KMS 复验、exact GET/no-list read 已接通。跨 Worker capability 由外部 secret reference 的共享 master key 按 tenant 派生，节点不再随机生成或落盘 key。filesystem backend 仅用于 contract test。HTTPS import 继续执行 host/MIME allowlist、public-address 校验、禁止 redirect、DNS-to-connect pin、wire/decoded size limit 和 timeout。两实例 roundtrip、32 路并发、forgery、partial failure、cleanup 与 S3 条件写/加密拒绝测试通过。 | 仍需真实 S3-compatible 服务的跨 AZ 故障注入/生命周期 E2E；GCS/ADLS 不在当前唯一生产路径内。provider dialect 的真实外部服务 E2E 另行验收。 |
| Result、资源与审计 | 2026-10-02：生产返回边界已移除 `AiUsage`/`TokenUsage` 及 token/费用/币种统计。`AiInvocationResult` 保留业务输出、实际 provider request identity 与错误。唯一 v2 grant 和 Worker admission 计量 logical items、physical application attempts、encoded HTTP entity bytes 及 media；失败重试不退款，返回 body 完成前持有并发许可。最近 Protocol4 + Worker152 项通过，详见 0.19。 | 这是资源治理与组件验证，不是 LLM Usage Ledger，也不证明 TCP 实际投递/重传、SDK 堆峰值、真实模型、SQL/JNI 或独立 Worker E2E。任务取消的立即 HTTP 中断及最终全链门禁仍需验证。 |
| Semantic Cache 与终态统计 | Native cache 按 tenant partition 隔离，租户只能驱逐自身条目，Worker 固定硬上限在 admission 时拒绝；resident accounting 包含 value、identity、向量与索引/容器预留。`NativeTaskStats` 在 handle release 前捕获并校验，成功任务缺失或畸形统计 fail-closed，cache hit/miss/eviction/rejection 进入 canonical terminal report。 | Native/Java 定向测试已通过；真实 Ollama 重复 SQL 的 provider-call-count E2E 在本轮统一 Native 重新打包后仍需复跑。 |
| Feature View / offline export | `CREATE/PUBLISH/MATERIALIZE FEATURE VIEW` 已接入 tenant-scoped catalog 与 RBAC。单源、固定 snapshot 的 materialize 使用 canonical v2 `TaskRequest`、`TABULAR_EXPORT` capability 和通用 durable artifact job/page/publication；catalog export link、job、dispatch spec 与 pages 在同一 JDBC 事务创建。零输入或零行输出仍产生 schema-bearing Arrow stream、checksum、`row_count=0` 和独立 manifest，不能与执行失败混淆。 | 当前只证明单源 offline export 的最小闭环；point-in-time 多源 join、online serving、CDC watermark、training/serving skew 和 public-entry 分布式 E2E 尚未验收。 |
| Training Dataset | catalog 可持久化 immutable dataset definition/version，但没有 SQL 入口、多源 durable export、Native plan wiring、原子 generation publication 或 streaming reader。 | **未接通**；不得从 catalog record 推导为 Training Dataset 服务已经实现，也不得发布 capability。 |
| Evaluation / release gate | JDBC catalog 已能持久化 immutable dataset version、version-pinned run、原子 terminal run+metrics 和单次 release decision。批准候选时会拒绝失败指标，并要求 candidate 覆盖 pinned baseline 的全部 metric name、保持相同 unit 且 baseline 指标自身通过；candidate 可额外携带只受绝对策略约束的指标。迟到 terminal report 不能覆盖既有终态。 | **仅控制面元数据，数据面未接通**：canonical v2 当前没有 Evaluation capability，也没有携带 metric value、run attempt 与 fencing token 的可信 terminal report；generic artifact report 只能证明对象发布，不能证明指标。因而没有生产 run submission/wiring、RBAC subject resolver 或 public SQL/API，不得把 store 单测表述为 Evaluation 闭环。 |
| Coverage 与资源对账 | canonical `CoverageReport`/`TaskReport` 表达 COMPLETE、PARTIAL、FALLBACK_EXACT、work-unit、source snapshot、artifact freshness、exact fallback 和 resource usage；Native `StatusSerializer` 生成报告，Coordinator `CanonicalTaskReportValidator` 对照派发请求与 resource grant 校验，未显式允许的 partial 会被拒绝。 | 分布式故障矩阵尚未全部执行，不能仅凭 schema/validator 宣称完整性闭环已验收。 |
| Lance | 当前源码强制 Rust FFI/C ABI10；真实物理 schema 适配拷贝量接入统一 ResourceUsage，失败后的已发生拷贝不丢失。Step0 r1 因生产计数未消费而出现 warning，按门槛判 FAIL；根因修复后 r2 0 warning/error。完整 Rust r2 **35 PASS、0 failed/ignored/filtered、0 warning**，保留原33项并增加真实部分适配失败计账和非法getter验证。 | 最新 ABI10 Native writer/OOM/public SQL 尚未验收。SDK0.39↔Native0.20 双向互读、durable publication、writer cancel/drain、SDK外部heap与全局spill硬计账均未完成；旧 ABI7/8/9 Release/ASan 结果不能覆盖当前源码。 |
| PostgreSQL 与 required E2E gate | 容器测试基类不再实现 skip hook；Docker 不可用、native library 不兼容、result server 缺失、SQL runner 未初始化或共享 PostgreSQL 未就绪都会失败。Registry E2E 通过 `requirePostgres()` 要求真实运行中的共享 PostgreSQL。 | 本轮只验证门禁契约与 test-compile，没有把 PostgreSQL/全查询 E2E 跑通，不能记为 E2E PASS。 |

### 0.1 2026-09-19 定向验证证据

| 范围 | 结果 | 说明 |
|---|---|---|
| JDK17 E2E reactor test-compile | PASS | `mvn -q -pl pangrova-testing/pangrova-test-e2e -am test-compile`。 |
| Embedding Model 目录、准入与持久化 fence | PASS（定向） | JDK17：`EmbeddingModelDescriptorTest` 3/3、`EmbeddingModelExecutionAdmissionTest` 3/3、`EmbeddingModelTaskIntegrationTest` 10/10、`DefaultWorkerSelectorModelResolutionTest` 4/4、`ConcurrentMapEmbeddingModelRegistryTest` 19/19；真实 PostgreSQL Testcontainers 的 `JdbcEmbeddingModelRegistryIntegrationTest` 18/18。覆盖 DROP/CREATE 同版本 ABA；证明 parser→task→catalog 与 catalog→exact Worker admission/fence；不折算为 SQL→Native provider E2E。 |
| Required infrastructure contracts | PASS | `ContainerTestBaseTest` 3/3、`RequiredE2EInfrastructureTest` 5/5，均为 0 skip、0 failure。 |
| Canonical VECTOR/model output contract | PASS | JDK17 protocol/planner/worker/coordinator 定向测试和 Native `LogicalTypeContract`、task parser、plan converter/JVM callback 6/6 通过。 |
| Semantic cache/statistics contract | PASS | Native cache/stats 9/9、Java `NativeOperatorStatsCollectorTest` 9/9；不折算为真实 provider E2E。 |
| Feature View durable export contract | PASS | Native artifact export 6/6、protocol round-trip 1/1、metadata 7/7、Coordinator Feature SQL 3/3、artifact binding resolver 4/4；只证明定向合同，不折算为 point-in-time/online/Training Dataset E2E。 |
| Lance real FFI/read connector | PASS | 固定 release FFI build；`pangrova_native_tests --gtest_filter='LanceBatchReaderTest.*'` 20/20，包含真实 dataset 的物理 rename、INTEGER→BIGINT、缺失列 default 和 partition 注入；native parser/write format 定向测试合计 21/21。Java `NativeWriteExecutorTest` 10/10，证明 Lance table write 在打开 connector sink 前 fail-closed。该证据不折算为 production table-write 或 public-entry E2E。 |
| 完整 Java/Native/E2E pipeline | 未执行 | 本节不把定向 compile/unit/CTest 折算成全仓、provider 或分布式 E2E。 |

### 0.2 明确未完成

- Public-entry AI SQL/检索的真实 provider E2E、完整八步构建、全量 CI、ASan 和发布门禁尚未通过。
- Embedding Model 的 CREATE/ALTER/DROP/SHOW 与 exact deployment fence 已有定向闭环，
  但 `DROP ... CASCADE` 仍未与 dependent generated-column definitions 做同事务级联删除；
  因此不得扩大表述为完整模型生命周期。
- 目标计划 R7 的退出条件尚未满足。Feature View 的单源 durable offline export 已接通，
  但 point-in-time/online 能力未完成；Training Dataset 仍只有 catalog 数据结构，没有执行链。
  删除 Coordinator 数据扫描或死实现只是边界纠正，不是能力完成。
- 目标计划 R8（Semantic View、统一评估、文档与发布 hardening）的退出条件尚未满足；本次文档校正不能替代
  evaluation/regression、全链 E2E 或 release 证据。现有 Evaluation JDBC store 只证明 immutable catalog、
  terminal+metric 原子提交和 metadata release gate；在 canonical Native evaluator、fenced metric report、
  subject-version admission 与 RBAC wiring 完成前，run submission 必须 fail-closed。
- Lance production read connector 已完成定向实测，但 SQL write dispatch 仍固定 Parquet，table-write contract
  也缺 output allocation 与 external-file commit metadata；直接 Lance task 明确拒绝。已删除会把 Parquet
  round-trip 误报为 Lance 的伪 E2E，完成通用 contract 前不得声称 Lance INSERT/E2E 闭环。

### 0.3 2026-09-23 增量复证（不构成发布验收）

- REMOTE Shuffle 的 Coordinator lifecycle 与 Worker/Native chunk 契约正在收口，但生产
  `ShuffleService` 服务端、持久化恢复及多节点故障 E2E 尚未完成。只有测试目录中的 gRPC
  service 实现不能证明 REMOTE 可部署；不得把定向 Coordinator/Worker/Native 单测折算
  为完整分布式查询通过。
- `ObjectStoreShuffleManagerFactory` 不再把 `LocalFsSpoolStore` 的
  `LOCAL_SPILL` 语义伪装成 `DURABLE`。未显式配置真实 durable backend、backend 不存在
  或 backend 只提供本地盘时均 fail-closed。JDK17 下
  `ShuffleProviderResolverTest` 与 `QuerySchedulingEngineWriteAdmissionTest` 定向
  通过；未因此获得生产 object-store shuffle 能力。
- DML 调度在缺少 commit tracker、catalog 或有效 write context 时现在派发前失败，
  不再吞掉注册异常继续执行 Native 写任务；未知 write mode 不再隐式变为覆盖写。
  Iceberg 恢复重建也拒绝未知 mode，而非隐式改成 INSERT。此修复只证明准入失败语义，不等于 Lance
  table-write 完成。Lance 0.20 的当前 FFI `Dataset::write` 会在 `finish` 发布版本；
  生产分布式写入仍需未提交 fragment 与单一 connector-owned manifest commit。
- Worker 现有 `NativeWriteExecutor` 结果通道可按 canonical task id 接收 Native 产生的
  非空 commit fragment；相同字节重试幂等，不同字节冲突拒绝。`TaskStatusCollector` 只在
  `FINISHED` 消费该片段并缓存于任务状态，使重复终态上报携带相同 metadata。
  JDK17 `NativeWriteExecutorTest,TaskStatusReportEnrichmentTest` 定向通过；它不是
  Lance public SQL write 或跨进程结果重放的证明。测试专用 `SimulatedTaskExecutor`
  现在返回合法 canonical `NativeTaskStats`，不再把缺少统计的模拟执行误作成功。
- 生产 Worker 写入 wiring 仍未贯通：`PangrovaWorkerMain` 未注入
  `CatalogResourceResolver`，全仓生产路径没有调用
  `WorkerBootstrap.setCatalogResourceResolver`；`WorkerTaskBootstrap.registerNativeWriter`
  因此在真实独立 Worker 部署中跳过 JNI writer 注册。E2E 基类自己注入 resolver
  不能证明生产部署闭环。修复必须通过合法 connector SPI/目录分发让 Worker 获得
  task-scoped、tenant-scoped 的 write resources，不能把 Coordinator metadata
  实例跨模块或跨进程共享。
- JDK17 邻近复验：`mvn -q -pl pangrova-coordinator -am test
  -Dtest=CanonicalTaskReportValidatorTest,QuerySchedulingEngineWriteAdmissionTest,RetrievalIntentCollectorTest,CanonicalSplitParserTest,DAGDispatchEngineRetrievalBindingTest,JdbcArtifactBuildStoreTest
  -Dsurefire.failIfNoSpecifiedTests=false` 为 **PASS（34 tests，0 failure/error/skip）**；
  包含真实 PostgreSQL 的 durable artifact store 9 项。该范围不含 Native/E2E。
- Iceberg crash recovery 的持久 intent 现记录 tenant、原始 base snapshot 与稳定的
  SHA-256 commit fingerprint；首次提交和恢复使用同一 context，connector 在 Iceberg
  snapshot summary 写入外部幂等 marker，并在 retained lineage 中检查可能已完成的
  ambiguous commit。JDK17 定向验证：SPI fingerprint 3/3、真实 Iceberg
  HadoopCatalog atomic/replay/retry 16/16、Coordinator recovery/status/journal 27/27、
  PostgreSQL Testcontainers journal 15/15，均 PASS。Paimon 尚无同等级外部 marker，
  恢复仍 fail-closed quarantine；Iceberg snapshot retention 必须覆盖 recovery SLA。
  这些定向测试不代替 Coordinator crash + 外部 catalog/Object Store 故障注入 E2E。

### 0.4 2026-09-24 增量复证（发布门禁进行中）

本小节更新 0.3 中曾标为未接通的 ShuffleService 与 Worker catalog wiring。0.3 保留为
当日快照，不再作为当前能力结论。以下 `PASS` 仅对应列出的定向范围；完整 Step 0–8
及跨 AZ、多租户规模压测未通过之前，REMOTE shuffle、Lance table write 和 AI Native
产品闭环仍不得标为发布就绪。

| 链路 | 当前源码边界与实测 | 剩余资格条件 |
|---|---|---|
| REMOTE ShuffleService | canonical v2 manifest checksum 集中在 shuffle-core；服务端基于 PostgreSQL 持久批次元数据分页提交，不再为 finalize 下载完整对象。读取逐批复验真实 payload SHA-256 和权威 `row_count`，零批次 producer 仅在 DB 也无批次时可提交。真实 PostgreSQL 16 + MinIO 集成测试 1/1 PASS；真实证书 mTLS/SAN allowlist 测试 3/3 PASS；shuffle-core 静态分析 PASS。Worker producer/reader 已加入 manifest 与 `row_count` 消费，相关 21 项定向测试 PASS。 | 需要真实多 Worker DAG、进程重启、超时、取消、腐败与偏斜压力 E2E；shuffle-service 全 reactor 静态分析与 Step 0–8 尚在执行。定向服务端/Worker 测试不能替代端到端查询。 |
| Worker catalog/write binding | 生产启动路径要求内部认证密钥与 Coordinator URL，按 task-scoped、tenant-scoped 的 exact catalog incarnation/revision/fingerprint 获取 deployment，并在 Native task 创建前绑定、结束后释放；输出 handle 校验 canonical checksum/codec，不依赖测试基类向 Worker 内注入共享 metadata 实例。E2E reactor `test-compile` PASS。 | 仍需独立 Worker 真实进程、真实 connector/catalog、崩溃恢复与 fail-closed 权限测试；test-compile 不证明 INSERT 提交闭环。 |
| Catalog 精确版本与 Worker 资源租约 | V35 新增不可变 catalog descriptor 历史，保存当前版本 CAS 与历史插入同一 PostgreSQL 事务；内部部署端点只读取任务绑定的 exact incarnation/revision，DROP/重建后仍可读取旧 incarnation，缺失时不回退。Worker connector 资源按 tenant 隔离，task lease 防止活跃实例被 LRU 逐出；启动/绑定失败会释放资源。真实 PostgreSQL 迁移、原子可见性与 endpoint 定向 9/9 PASS，Worker cache/lifecycle 定向 9/9 PASS，均 0 skip。 | 历史版本暂无限保留：没有持久 task→revision lease 时不能安全 GC，容量需监控；共享 `X-Internal-Auth`、未强制 TLS、缺 tenant-scoped endpoint 授权仍为 P1 安全债。详见 [版本生命周期规格](catalog-deployment-revision-lifecycle.md)。这些定向测试不等于独立 Worker 写入 E2E。 |
| Lance reference-only write | Rust sidecar、C++ Native binding 与 generic connector admission 改为未发布 fragment 的 reference/commit/recovery/discard 路径，消除 `Dataset::write` 在 `finish` 时提前发布的旧设计。Recovery identity 包含 tenant、generation、schema/version 与 attempt fence。运行时不再持有永不释放的 Tokio `Arc`；关闭全局引用后，存活 reader/writer 仍持有自己的引用，可重新初始化。`cargo test --locked` 7/7 PASS、Lance FFI Release 构建 PASS、Native Release 构建 PASS，Lance/parse/write/cancel/JNI 定向测试 77/77 PASS。 | 必须验证 public SQL → task → Native fragment → connector-owned 单次 commit 与故障恢复；仅凭组件定向测试不得宣称生产 Lance INSERT 可用。 |
| DDL fail-closed 与维护门禁 | `ALTER TABLE` 对 connector SPI 无法表示的 `NOT NULL` 新列及 nullability change 在任何 connector mutation 前拒绝；混合变更不会先执行部分操作。`DefaultMetadataServiceAlterTableTest` 6/6 PASS。Metadata/parser 的拆分类改动经 CleanCode/PackageStructure 属性测试 9/9 PASS。 | DDL 更广泛的跨 connector 原子性、所有静态分析与全仓测试仍需执行。 |

后续定向复证：默认 Worker 在未配置模型及 FILE 时经真实 ServiceLoader 与
`NativeFunctionBootstrap` 注册零个 AI 函数并成功启动；仅文本模型不依赖 S3，关闭
`autoRegister` 不再广告模型。JDK17 Worker 聚焦测试 39/39 PASS、Worker Checkstyle PASS。
Coordinator 成功的后台 Build/Export 任务必须携带合法 artifact report；对应
`CanonicalTaskReportValidatorTest` 17/17 PASS。REMOTE Shuffle 的新 stage 清理 capability
可按过期时间单调续期，并拒绝同过期时间换 token；
`CoordinatorShuffleLifecycleManagerTest` 12/12 PASS。上述仍仅为定向验证。

REMOTE Shuffle 清理仍有三个明确未闭环项：Coordinator 重启后本地 cleanup owner/token
映射丢失，当前清理调用会直接返回；初次 query scope 的 capability 到期后，长查询
可能无法清理；服务端原 `cleanupPlan` 一次性载入全部 batch object key，在大型查询下
有 OOM 风险。服务端分页删除与 `CLEANING` 状态恢复正在修复；`ACTIVE` 查询不能仅按
capability TTL 自动回收，否则会误删可配置的长查询，必须引入带 query/owner/fence 的
持久租约或可信终态标记。未有该证据之前，不得把进程故障后的 Shuffle 回收标为闭环。

2026-09-24 串行全链门禁当前进度：Step 0（Lance Release FFI）PASS，Step 1
（Native Release configure/build）PASS，Step 2（当次完整 CTest）**PASS：3,354/3,354**，
均在最后一次 Rust runtime 修订后重新执行。早先两次 CTest 失败分别暴露了测试二进制
瞬时 `exec format error`、Velox 进程退出析构崩溃与测试临时端口收到 HTTP/1.1
而非 gRPC；修复了测试主入口显式 runtime teardown 和 HMAC 测试独占临时端口，
不得抹去这些失败历史。

Step 3 **FAIL**：此前 ASan 构建通过，但直接 `ctest --test-dir
pangrova-worker-native/build-asan --output-on-failure` 在测试发现阶段报告未同等插桩的
Release Velox `inferTypeWithSpaces` / libc++ `container-overflow`。现已移除
`dev-asan` 预设中关闭容器检测的选项，`FindVelox.cmake` 对 ASan 强制检查所选 Velox
archive 与 CMake 编译 flags；独立插桩 Velox 仍在构建，尚无新一轮 ASan PASS。
此前即使关闭容器检测，LeakSanitizer 仍报告 Tokio
`Builder::new` 初始化时一次性 14,392 字节分配。已修复 Lance 自身全局 runtime
引用不释放及 worker 线程泄漏，但不能把剩余第三方报告或预设盲区记为 ASan 通过。
需要完成同等插桩的 Velox 依赖构建，并确认 Tokio 分配的所有权或明确修复。
按 `build-pipeline.md` 的逐步门禁，Step 4–8 **未执行**，不得记为通过。

独立 JDK17 静态分析诊断：`mvn -q -pl pangrova-worker,pangrova-metadata,pangrova-parser
-am -DskipTests verify -Pstatic-analysis` 在 observability 模块被 36 项 SpotBugs
medium 报告阻断；直接 Checkstyle 诊断在 metadata 模块仍有 27 项规范错误，均未伪报
为全仓静态门禁通过。协议四项错误已修复，Worker 与 Parser 的独立 Checkstyle
均 PASS。JDK17 定向测试：Worker TaskManager/终态上报/Shuffle 合计 27/27，
protocol `CanonicalRemoteShuffleAdmissionTest` 3/3，metadata ALTER TABLE 6/6、
artifact store 9/9，均 0 failure/error/skip。`JdbcAuditStore` 在队列满或连续失败时
会丢弃审计事件，不能将其视作可靠、fail-closed 的 AI audit ledger。

本轮继续复证（同日，仍非发布验收）：canonical v2 `RenewAccess` 已支持按 READ/WRITE
批次边界续期和读流从下一已验证 sequence 重开；Worker 定向 17/17、ShuffleService
续期/mTLS/真实 PostgreSQL 定向 11/11 PASS。Coordinator 现以 mTLS 控制身份对精确
`TaskAttemptId` 签发 READ/WRITE grant；Worker 只能凭同一 attempt 票续期，stage 票
不能用于数据 RPC。Coordinator、Worker、Service 的联合 JDK17 定向 47/47 PASS，
包含跨 stage consumer epoch 与错误 attempt；尚无真实多 Worker 故障 E2E。
Coordinator 的终态前持久
cleanup intent、同库 execution-anchor fencing、注册响应丢失后的恢复清理及 PUT 前持久
write-intent 已接入。后续实现用数据库时钟限定 write-intent 的阻塞期，到期清理事务
先持久化异常 shuffle 前缀墓碑，再允许释放查询；后台以可接管的 PostgreSQL 租约反复
清扫，空扫不会删除墓碑。生产启动验证所有实例共享同一 bucket/prefix/region/endpoint。
真实 PostgreSQL/MinIO 的 V1→V6 新建及 V4→V6 升级、晚到 PUT、重启、双实例接管与
命名空间拒绝定向 17/17 PASS。异常墓碑长期保留并周期扫描，元数据/扫描成本尚无有限
上界；没有真实多 Worker 故障 E2E 和规模压测，不得宣称大规模生产资格。另有
pre-anchor cancellation 状态机门槛已落地，JDK17 Coordinator 并发/日志故障定向
12/12 PASS；HA bootstrap 进程崩溃注入仍未证明。

同日复证了一个无 Worker 故障路径：`QueryDispatcher` 原先直接把 state machine 置为
FAILED，绕开 `QueryManager` 的持久终态、活动计数、admission release 与清理，导致
HTTP 虽返回 503、Coordinator drain 却等待约 30 秒。现在同步/异步派发异常均由
`QueryManager.failQuery` 收口；HTTP 实测新增活动查询数归零断言。
`CoordinatorBootstrapTest` 11/11、`QueryManagerTenantTest` 6/6、journal 4/4、
drain 8/8、async 5/5、`SecurityChainTest` 定向 reactor exit 0，均不等于真实
Worker SQL 成功链。多租户启动在 YAML 与 programmatic bootstrap 均拒绝未认证或
NOOP authenticator；默认单租户模式显式使用 `default` tenant。客户端注入的
`tenant_id` 不会覆盖权威身份，DML write context 会携带权威 tenant。
仍有一个配置语义待决：`security.enabled=false` 时 HTTP 不注入 `UserIdentity`，而
canonical v2 `TaskRequestFactory` 要求非空 principal；因此该模式即使有 Worker 也
不能当作可运行 SQL 的生产配置。应明确把无认证模式限制为控制面/dev，或引入显式
opt-in 的低权限匿名身份并在协议中标明认证来源；不得隐式伪造“已认证”principal。

Worker 的生产模型查找已按精确配置名拒绝未知模型，不再回退到首个 Provider。
`ConfiguredAiProviderRegistryTest` JDK17 定向 21/21 PASS，新增断言确认
`MODEL_NOT_FOUND` 发生在 Secret 解析和 Provider 创建之前。此项只复证 Worker 本地
deployment 查找，不能替代 Catalog Model 生命周期与 SQL→Native→Provider E2E。

下一步修复规格：把 query 的逻辑终态与对象物理回收拆开。写入意图先于 PUT 持久化，
终态事务持久封禁新写入；明确成功的 PUT 可原子登记 batch，明确失败可撤销意图，
超时/断连这种未知结果必须转为持久 GC tombstone，不能静默当作失败。独立租约化
GC 对异常 shuffle 前缀重复扫描和幂等删除。当前采取永久墓碑以维持迟到 PUT 的
最终补偿：若要有限期删除墓碑，必须先由对象存储侧提供可验证的迟到写入上界，
或验证专用前缀的 lifecycle 过期策略并明确保留期/审计契约；普通 S3 API 超时本身
不能证明物理写入已结束。还需为墓碑数量、扫描速率、滞留年龄建立容量限额和告警。
验收需在 PUT 已发送但 ACK 丢失、
进程崩溃、清理扫描之后才可见的晚到对象、GC 租约接管和跨 AZ 重试处故障注入，
证明查询 owner 可释放、对象最终回收且不会误删新 generation，并单独标注
物理回收是最终一致而非严格有界。

最新正式门禁重跑：Step 0 Lance Rust Release FFI PASS；Step 1 Native Release build
PASS；Step 2 完整 Native CTest **3,357/3,357 PASS**（453.51 秒、exit 0）。
此前 Step 3 的两次失败分别为未同等插桩 Protobuf 的 `RepeatedField`
container-overflow，以及混链 Abseil 静态初始化崩溃；已改用同一工具链插桩的
Velox、Protobuf、Abseil，ASan configure/build 均 exit 0。全量 CTest
**FAIL：3,351/3,357 PASS、6 FAIL、exit 8、2,374.15 秒**，失败集中在
`PlanExecutionDriverCancelTest.CancelAfterCompletionIsNoop`、`ShuffleOutputTest`
两项及 `TaskExecutorStartTest` 三项。六项均由 AddressSanitizer 报告同类
heap-use-after-free：`PlanExecutionDriver` 先销毁 Velox memory pool，异步 Velox
driver/task 随后析构 `ValuesNode` 的 `RowVector`，在 `AlignedBuffer::freeToPool()`
读取已释放 pool。这是此前的失败证据；随后修复了真实所有权/销毁顺序：
`VeloxTaskRunner` 等待 task/driver 真正析构，`PlanExecutionDriver` 才释放 Values
转换 memory pool，未用 sleep 或 sanitizer suppression 掩盖。

**最终 Native 门禁**：Step 0 Lance Rust FFI PASS；Step 1 最终代码 Release
configure/build PASS；Step 2 **3,357/3,357 PASS**（458.60 秒）；Step 3
同等插桩 Velox/Protobuf/Abseil configure/build PASS、ASan/LSan 全量
**3,357/3,357 PASS**（2,166.98 秒、0 fail、0 skip、0 未抑制 finding）；Lance
24/24 单独运行通过。Step 4 Release 库打包 PASS，并验证 Lance FFI ABI。
Step 5 JDK17/Maven 3.9.16 全仓 `-DskipTests install` PASS。Step 6 全仓
`mvn -q test` 仍在逐项修复：首次在 protocol 的 `Set` DTO 不可变性规则失败，
二次在 metadata 的 V30 Worker 迁移旧断言失败；两项均已定向修复并重新启动
全仓测试。Step 7–8 尚未执行，不得宣称发布门禁通过。

CI `native-sanitizers` job 已加入独立 ASan Velox/Protobuf/Abseil 构建及缓存，
但该工作流尚未在托管 runner 上跑通，不能把本机 PASS 等同于 CI PASS。

### 0.5 2026-09-24 当前门禁与端到端证据缺口

本小节是最新工作区复证，取代上方 0.4 中过时的门禁进行中快照；此前失败历史仍保留。
同一源码快照的 Step 0 Lance FFI、Step 1 Native Release、Step 2 完整 CTest
**3358/3358 PASS**（453.27 秒）和 Step 3 同等插桩 Velox/Protobuf/Abseil 的完整
ASan/LSan CTest **3358/3358 PASS**（1106.96 秒）均 exit 0。Step 4 Release
Native 库打包 PASS，Lance ABI 符号已验证，打包文件与 build 输出 SHA-256 一致。
JDK 17.0.20/Maven 3.9.16 的 Step 5 全仓 `-DskipTests install` 在最新 Java 源码上
PASS；Step 6 `mvn -q test` 正在重新执行，Step 7–8 仍无最新成功证据。
这不构成生产发布或大规模分布式资格。

全仓回归先揭示了三个真实一致性问题并已定向复证：Worker 取消状态必须先停留在
非终态 `CANCELLING`，Native join 完成后才释放内存和执行所有权并上报终态；
Coordinator 健康 timer 不能执行远端 Shuffle 清理 RPC，终态与 Shuffle 清理应使用
独立有界执行器；异步 SQL 提交若在结果端点发布前失败，必须向 HTTP 调用者传播
原始分发错误而不是返回 `200/FAILED`。另将 artifact report 的最高已见序号保持单调：
持久提交失败允许同序号重试，但低序号不可越过。上述定向测试通过；完整回归结果
仍以本轮 Step 6 最终退出码和报告为准。

以下是**验收证据不足**，不是仅凭测试缺失就断定生产实现有缺陷：

| 缺口 | 当前可核查证据 | 必须补充的真实端到端验收 |
|---|---|---|
| Provider/Embedding/多模态 | `OllamaAiSqlE2ETest` 真实 Provider 只覆盖 AI_COMPLETE；planner 另注册 AI_EMBED 等函数。 | Ollama embedding 维度/批次顺序/失败原子性、生成列 SYNC/ASYNC/backfill、FILE/BLOB 多模态从 SQL 到 Native/Provider/VECTOR/结果。 |
| 全文与融合检索 | `VectorHybridSearchE2ETest` 是向量距离加分类过滤，未证明 BM25＋向量源、RRF/Linear/Rerank。 | 构建真实全文和向量索引，经 public SQL 断言 Native 融合计划、候选与最终排序。 |
| 多 Worker 分片 | `SqlE2ETestBase` 只启动一个 Worker；`DiskAnnDistributedE2ETest` 不能凭名称证明跨 Worker 路由。 | 两个独立 Worker、真实分片、故障/超时/重试、覆盖率及 COMPLETE/PARTIAL/FALLBACK_EXACT 语义。 |
| ANN 索引实际命中 | HNSW/IVFPQ E2E 只验证结果，精确回退也可满足断言。 | 验证选中索引 ID/代际、Native 候选数和回退原因，并与 exact baseline 对照。 |
| Feature Store/评估/治理 | SQL 语法和服务存在，缺少 public SQL→持久状态→Native 作业→可查询结果的完整测试。 | Feature View 创建/发布/物化、权限、恢复及真实 Provider usage/budget/audit/guardrail、evaluation release gate。 |

在以上验收和 Step 0–8 全链、跨 AZ/多租户规模与故障压测通过前，相关能力维持
`部分闭环/证据不足`，不可写成“业界顶尖”或“生产就绪”。

### 0.6 2026-09-27 重新复证与未完成边界

本小节取代 0.5 的“最新”状态表述，但保留此前失败历史。上一代码快照的顺序门禁
Step 0 Lance Rust FFI、Step 1 Native Release 构建、Step 2 完整 CTest
**3,363/3,363 PASS**、Step 3 同等插桩的 ASan/LSan 完整 CTest
**3,363/3,363 PASS**、Step 4 Native 库打包与 SHA-256 一致性、Step 5
JDK17/Maven 全仓 `-DskipTests install` 均 PASS；日志为
`/tmp/pangrova-step{0,1,2,3,4,5}-*r5-20260927.log`。该快照之后仍有
Java、Native 和 Lance FFI 修改，因此这些门禁**不能冒充最终代码的验收**。

Step 6 全仓 `mvn -q test` 的 r5 尝试 **FAIL**：真实 Iceberg DML E2E 揭示静态
分区 `INSERT OVERWRITE` 只追加不覆盖、`MERGE UPDATE + INSERT` 的分阶段执行
丢失 parent query 的持久 execution anchor。随后将 DML E2E 基类接到生产
`executeDmlStatement`，并让重试/上下文富化使用 Pangrova `AstPrinter` 的合法
SQL 方言；Native MERGE 改为同一计划、同一 Native write、同一 catalog commit，
不再以多个子查询提交模拟原子操作。JDK17 下真实 Docker/Testcontainers 的
`mvn -q -pl pangrova-testing/pangrova-test-e2e -am
-Dtest=WriteDmlTransactionE2ETest -Dsurefire.failIfNoSpecifiedTests=false test`
目前 **12/12 PASS、0 skip**；静态分区用精确行值而非行数断言，包含 MERGE、
并发 OCC 与 crash-idempotency。它是修复后的**专项 E2E**，不是 Step 6 全仓
重新通过；Step 7–8 仍无本次最终代码证据。
当前 MERGE 能力边界不是“完整 MERGE”：`UPDATE + INSERT` 的单计划、单快照
路径已由专项 E2E 证明；`DELETE` 与另一 action 的复合语句在写前 fail-closed，
因为现有 Calcite `SqlMerge` 表示不能保持多个 matched action 的完整语义；
同一 target 行被多个 source 行命中时，尚无跨分片行身份/唯一性验证，不能证明
符合 SQL MERGE 冲突语义。
后续真实 Iceberg `WriteDmlTransactionE2ETest$UpdateDeleteMerge` 专项
**5/5 PASS、0 skip**，新增 nullable source 首列、reordered/partial INSERT
的精确行值断言，以及复合 DELETE action 写前拒绝且表数据不变；`SET` 列名
大小写变体也通过。它不替代完整 WriteDml 套件重跑或全仓 Step 6。
后续改动以标准 Substrait `LEFT_SINGLE` 标记 MERGE 的目标保留 JOIN，
Native 在对应 JoinNode 的 probe `inputPositions` 与 `outputPositions` 上执行
精确基数检查，且必须在 worker write fragment 标记成功之前失败。这不依赖目标
用户列或完整行 hash，因此相同值的两条目标行仍可各自匹配一次。JDK17 下
`SubstraitDmlRootNamesTest` **3/3 PASS**；Release Native 增量构建、JNI
打包及 `SingleMatchJoinGuardTest`/`VeloxPlanConverterTest` 聚焦 **9/9 PASS**；
真实 Docker/Testcontainers `WriteDmlTransactionE2ETest$UpdateDeleteMerge`
**7/7 PASS、0 skip**，包含重复 source 命中同一 target 时失败且表不变、相同值
的两条物理 target 行合法更新。此为专项验证，完整 Step 0–8 尚未在当前代码
快照重跑。当前无 shuffle 的
DML 输出阶段被限制为单 task，所有 source/target splits 在同一个 Native task；
这只能证明单 task 的匹配完整性，**不能视为分布式 MERGE 完成**。若以后并行化，
必须先在 canonical v2 stage DAG 中证明每个目标分区接收完整匹配 source
（按 JOIN key 重分区或完整广播），并以 source snapshot/覆盖率/fencing 保证
跨任务校验不会漏报；直接提高 taskCount 会破坏该基数检查的前提。

生成列写入准入原先使用合成 `system` 身份且不带真实 tenant，已改为会话中的
authenticated identity/tenant；相关 Coordinator 定向测试 **15/15 PASS**。
这个修复尚需在最终代码上重跑完整构建与 public SQL E2E。

Lance 仍不得被写成可大规模部署的云对象存储写入：此前的 reference commit
在旧 `read_version` 与当前最新版本冲突时可接受覆盖，存在丢失并发提交风险。
本地/file 的精确版本、原子发布修复已通过组件复证；对 `s3://` 等远端，如果没有
可验证的条件写/CAS commit handler，必须 fail-closed，不能退回 Lance 默认
unsafe handler。远端写入现于 FFI writer-open 准入时即拒绝，而非先暂存碎片到
远端再在 commit 阶段失败；Rust 对此入口和错误语义的 11/11 测试通过。即使
本地回归通过，远端 CAS、跨进程竞争与失联恢复仍是明确
未闭环项。
修复后 Lance Rust FFI Release 构建 PASS、Rust 10/10、Release Native Lance
26/26 PASS；并发双写用例重复 20 次全部通过且 0 SKIPPED。重复运行曾暴露的
Lance 测试夹具 memory-pool 生命周期问题已修；这些仍是组件验证，不是 public
SQL 分布式写入 E2E。

本轮数据库核心剩余硬门槛：最终源码重跑 Step 0–8；至少双 Worker 的
分片/Shuffle/Top-K 故障矩阵；ANN/BM25/融合检索的 Native 命中证明；
durable embedding/backfill、snapshot-pinned Feature View 物化及远端 Lance CAS。
Training Dataset 产品、模型发布 Evaluation 与 LLM 用量计费已划至外围。
任何一项数据库核心能力不能用 SPI、组件直连、单 Worker
测试或静默回退抵充。Coordinator 继续仅承担 JVM 控制面，融合与行级执行归
Native Worker；统一使用 canonical v2 通用协议，不新增 VECTOR 私有协议。
首次重新运行真实 Ollama AI SQL E2E **FAIL（非环境阻塞）**：在执行测试 SQL 前，
Native Velox 无法解析 `ai_ocr` 的 `ROW<uri:VARCHAR,...>` UDF 参数签名，导致
认证 AI UDF 注册失败。必须修 Native 签名/注册合同、重打包 JNI 库并重跑，
不能通过跳过多模态函数注册或把未执行的测试写成通过。
Native 签名生成改为从 Velox TypePtr 递归生成 positional
`row(...)`/`array(...)`/`map(...)`；Native UDF 11/11 PASS、JNI 重包 PASS。
第二次 Ollama E2E 已穿过全部 14 个 AI UDF 注册，但在模型广告阶段再次
**FAIL（非环境阻塞）**：`auth=none` 的空 secret reference 被
`EmbeddingModelBindingIdentity.sha256` 当作非法输入。需为无认证模型定义
明确的签名身份、复验 Worker/Coordinator 一致性后重跑 SQL，不得用假 secret
掩盖配置语义。
Embedding SPI 已迁移为唯一 `EmbeddingBatchResult`（有序向量、真实
`TokenUsage`、provider request id），旧数组/列表返回 API 已移除；JDK17
SPI/Worker 聚焦 **58/58 PASS**。这只证明 Provider 合同和 Worker 对账。
临时 Worker token/media 用量账本及其 task-report 合并接线现已移除；
`AiUsage` 仍是单次 Provider 返回合同，不构成持久计费账本。
按 2026-09-27 范围决议，调用级 token/费用持久账本和模型发布评估由外围系统
负责，不新增数据库协议、Coordinator sink 或 JDBC migration。数据库仍必须
保证 provider 出站受任务 deadline、并发和物理请求上限保护，且不能把当前
统计称作精确计费；Native CPU/内存/网络/IO 统计继续走通用 `TaskReport`。
此前 `ConcurrentQueryTest` 中所谓“双 Worker”服务未参与任何查询，结果获取还会把
失败/空结果归为空列表；这套未使用的测试脚手架已删除，JOIN/聚合用例明确标注为
单 Worker 正确性测试。它们不能计入双 Worker 验收。
独立双 Worker public SQL E2E 的读审还发现 JDBC 结果时序缺陷：Coordinator
在 `RUNNING/FINISHING` 已发布端点，但 `QueryCompletionWaiter` 只在
`FINISHED` 才允许 DoGet，Native OUTPUT 又要等 DoGet 消费才能完成，形成
循环等待。修复应在受信端点可读时开始流式读取，并在同一超时预算下继续确认
最终终态；不能把端点存在等同查询成功。现有单 Worker E2E 基类有独立读取
规避，不能证明 public JDBC 正确。真正的双 Worker harness 还需两个独立生产
Worker、共享 catalog/仓库、真实 shuffle 与可观测故障注入；当前测试容器配置
没有满足这些前置条件。
JDBC 已改为先等待可读端点、DoGet 完成后继续核验终态；`QueryCompletionWaiterTest`
和相邻 `PangrovaStatementExecuteAndTransactionTest` 聚焦 **31/31 PASS**。
Flight 读取本身仍按 120 秒 batch-idle 而非 JDBC 整体 query timeout 限制，且
当前 `fetchResult` 把所有 Arrow 批次聚成一个 `byte[]`；因此这只是死锁修复，
不能据此宣称大结果集流式 JDBC 或客户端超时契约完全闭环。

### 0.7 2026-09-27 真实 Provider 链路复证与回归风险

真实 Ollama/Testcontainers `OllamaAiSqlE2ETest` 在最终 AI SQL 修订上
**5/5 PASS、0 FAIL/ERROR/SKIP**（Surefire XML `time=43.516s`）。覆盖
Coordinator HTTP → canonical v2 planning/stage → Worker/JNI → Native AI UDF →
真实 OpenAI-compatible Ollama → VECTOR/文本结果：单行 `AI_COMPLETE` 首次调用
恰好一次 provider 请求，重复 SQL 的 Native semantic cache 第二次请求增量为零；
两条独立 `AI_EMBED` SQL 恰好两次 provider 请求，并逐元素复验真实的 384 维
VECTOR；未知模型和错误模型用途均在 provider 调用前拒绝。邻近聚焦证据为
Planner 26/26、Native 21/21、Worker/SPI 52/52 PASS，均不能代替完整
Step 0–8 或多 Worker/故障注入验收。

该 E2E 过程中先后暴露的生产根因不能从报告中删掉：Native 复合 UDF 签名
曾拒绝 ROW；匿名模型的空 secret identity 曾被误判；通用 ExchangeRel /
ExtensionLeafRel 的 validator/capability 广告曾漂移；跨 stage 的 VECTOR
model origin 曾丢失。最严重的是纯 `Values` 计划被会话并行度覆盖成四个
compute task，单行 SQL 实际向 Ollama 请求四次。现以纯 `Values` 阶段单 task
和 E2E 精确请求数断言修复；不得将“最终结果行正确”代替 provider
调用/计费唯一性证据。

Embedding Model `CREATE` 现支持显式 `authentication='none'`：不得同时声明
`api_key_secret`，HTTP 仅允许 literal loopback，Bearer 仍要求非空 secret
reference 与 HTTPS。JDBC registry 已补旧表新增 deployment fence 两列的
schema migration；真实 PostgreSQL Testcontainers
`JdbcEmbeddingModelRegistryIntegrationTest` **19/19 PASS、0 skip**，含旧表
升级且原记录保留。旧记录以未绑定
fence 保留，执行必须重新绑定 exact Worker deployment，不可静默采用当前模型。

JDBC public client 的 Flight DoGet 现同时携带整个 gRPC call deadline 与每批
读取 deadline，并在超时映射 `SQLTimeoutException`/取消查询。Transport/JDBC
聚焦已通过；真实阻塞 FlightServer 的 `FlightResultFetcherDeadlineTest`
**1/1 PASS、0 skip**，Transport/JDBC 邻近聚焦合计 **37/37 PASS**；最终源码的
全仓门禁仍待跑。
JDBC 仍一次性缓冲 Arrow IPC 结果，不得宣称 bounded-memory streaming。

### 0.8 2026-09-27 分布式执行与物理出站预算复证（进行中）

本节记录本轮后续源码的增量结果；不覆盖前述失败历史，也不把定向通过折算为
Step 0–8 完成。统一 canonical v2 仍是唯一任务/结果协议；Coordinator JVM 只做
控制面绑定、调度和终态核验，Native Worker 执行 shuffle、融合、行级计算与写入。

- AI 出站预算现在按**真实物理发送**逐次 admission：内置 OpenAI-compatible、
  Responses、Transcription、Embedding、Reranking transport 在发送前记账；重试
  同时消耗 task/operator 的请求及 token envelope，耗尽即阻止下一次发送。
  `AiUsage.providerCalls` 反映已观测物理发送；模型并发 semaphore 以 exact deployment
  租约共享，最后一个调用结束后回收，避免 Worker 长时运行积累历史模型版本。
  Embedding batch 的单次上游请求只扣一次 `maxProviderInvocations`，逻辑向量数量仍
  受 capability batch/row 上限与结果基数校验。`TokenUsage` 已移到 SPI 的公共 AI
  契约层，避免 inference 与 embedding 子包成环。JDK17 的
  `AiInvocationServiceTest` 23/23、`AiModelBudgetRegistryTest` 2/2 聚焦 PASS，
  `AiInvocationBudgetManagerTest` 1/1、`AiProviderCallContextTest` 4/4 聚焦 PASS，
  均 0 skip。Task budget registry 加入固定容量并拒绝超量新 task；出站前再次校验
  截止时间，避免预算已预留但过期后仍发送。改动后的全仓回归仍待完成。
- JDBC `executeUpdate` 保留 `SQLTimeoutException` 的异常类型，不再把它包成普通
  `SQLException`；最近邻 JDBC 测试 PASS。Worker reactor 在本轮较早源码快照下
  `mvn -q -pl pangrova-worker -am test` PASS；此后仍有 AI 预算、Worker 进程和
  Native 改动，不能引用该结果充当最终源码 Step 6。
- Lance FFI 对 S3 manifest 提交使用条件创建，并在发布前验证对象存储是否真正拒绝
  重复 create。旧 MinIO 忽略条件头时 fail-closed；较新 MinIO 的两个独立客户端
  并发同版本提交为单赢家、单冲突。`cargo test --locked --lib` 14/14、
  Release FFI 构建、Lance CTest 29/29 PASS，0 skip。详细边界见
  [Lance 写入 ADR](ai-native-lance-write-adr.md)：这仍未证明 SQL 双 Worker
  fragment/journal/catalog CAS、断联恢复和孤儿对象清理。
- Native direct shuffle 的 commit/abort 与 Flight quiesce 已完成定向 50/50 PASS；
  全量 CTest 首轮 **3387/3389 PASS、2 FAIL**，分别是 capability inventory
  期望与生产 converter 漂移，以及 overlay 测试输出向量持有时间超过 Velox
  pool 生命周期。修订后重链并完整复跑，Step 2 **3389/3389 PASS、0 FAIL**
  （125.27 秒）；同等插桩的 Step 3 ASan 完整 CTest **3389/3389 PASS、
  0 FAIL、0 SKIP**（347.60 秒）。Step 4 Native 库打包 PASS，包内 dylib 与
  Release 构建 SHA-256 一致；Step 5 JDK17 全仓 `mvn -q -DskipTests install`
  PASS，Step 6 Java 全仓测试正在运行，E2E 尚未完成，
  不能把 Native 两道门禁当成发布完成。
- 两个独立 Worker JVM 的诊断性 SQL JOIN + 全局 Top-K 返回了正确的 10 行，但
  调度只记录 Worker 1，因此跨 Worker 验收 **FAIL**，不能用结果正确掩盖。
  根因是 Worker 主入口 `bootstrap.start()` 后返回、daemon 线程不足以保持子
  JVM 存活；显式进程 await/shutdown 已加入生产主入口，双节点重跑尚未完成。最初的 loopback
  地址注册失败属于测试 harness 配置，生产地址准入未放宽。
- `WorkerBootstrapTest` 过去因 `native-required` 标签在默认 Surefire 中完全没有
  执行。现在测试配置可显式开启该标签；首次真实运行 **6 tests、5 errors**，根因是
  测试 fixture 未提供当前生产启动所要求的 catalog runtime，属于真实失败而非
  `SKIPPED` 或通过。fixture 注入 test-scoped catalog resolver 后，显式
  `-Dpangrova.worker.excludedTestGroups= -Dgroups=native-required`
  重跑 **6/6 PASS、0 skip**；生产 fail-closed 要求未放宽。
- Step 6 JDK17 全仓 `mvn -q test` 首轮在 E2E 模块出现真实失败后中断，退出码
  **130**，不得记为全仓 PASS：Paimon/动态表 INSERT 报
  `Distributed write handle lacks a recoverable target table`，其后 async submit 的
  30 秒等待将根因遮蔽为 `Result endpoint was not ready`；部分后续用例出现
  `WORKER_FATAL` 级联错误。Paimon output handle 现携带目标表、模式和原始 snapshot，
  生产提交要求稳定的 durable context，并使用 Paimon commit-user/identifier
  重放过滤；相关 connector 聚焦 **26/26 PASS、0 skip**。静态分区覆盖写因当前
  WAL 不能保存分区约束，崩溃重放明确 quarantine，不声称完全可恢复。仍需
  完成真实 Paimon SQL E2E，并重新从 Step 5/6 复跑。Coordinator async submit
  现监听带错误明细的终态，在 result endpoint 发布前及时返回原始提交失败；
  `QueryManagerAsyncSubmitTest` **9/9 PASS、0 skip**，尚未折算为全仓门禁。
- 双 Worker 物理 E2E 严格断言发现正确 10 行并不等于分布式执行：最初仅 stage 0。
  E2E fixture 与生产 CBO/统计装配已对齐；CBO 原先跨 planner 注册 RelNode、
  失败后静默退回，以及序列化阶段临时生成 BroadcastExchange 的旁路均已定位并
  修订。Planner/CBO/Fragmenter 聚焦 **29/29 PASS**，真实多 stage/双进程
  E2E 仍 FAIL，继续追踪物理候选选择，不放宽断言。后续诊断已让 stage0 规划
  16 个本地 scan splits，producer 完成后 Flight 开始读页；但 r19 在
  `ShuffleFlightBatchReader::ReadNext` 释放仍依赖 producer Velox pool 的页时
  **SIGSEGV**。Native queue 入队边界改为独立持有 bytes，同时统一 producer
  Exchange 的 callback 路径，并修复混合 stage 漏取本地 split。r20b 重新
  打包后真实两个独立 Worker 进程的物理 E2E **1/1 PASS、0 skip**：严格断言
  至少两个 stage、两个 Worker ID/端点、两次 JOIN、聚合以及全局 Top-K 精确
  10 行（79..70）。邻近 Native 聚焦 **24/24 PASS**。这是单条真实物理链路，
  尚不能覆盖故障矩阵、ASan 或最终 Step 0–8。清理诊断日志并重链后的 r21
  Native Release 完整 CTest **3391/3391 PASS、0 fail/skip**（约 461 秒）；
  r21 最终库重打包后同源码双 Worker E2E 再次 **1/1 PASS、0 fail/error/skip**
  （Surefire XML `time=28.72s`）。
  当时的 count-only queue 与 256 MiB/4 MiB 发送上限漂移已在后续资源修复中
  改为 4 MiB canonical 页、入队前 slot+bytes 信用和 Worker 任务的足额
  Native grant 预留；此处 r21 小数据双 Worker PASS 不证明修复后的规模表现。
  剩余 P1 是 Flight 活跃流：demux 释放 retention charge 后，replay、
  IOBuf coalesce 和 Arrow builder 的临时/响应内存仍只受 128 路数量限制，
  未按字节进入 server-wide 预算。须在慢消费者、取消、linger 与多路并发下
  验证完整的 byte credit 和释放时序，不能以队列预算替代整条链路预算。
- 2026-09-27 严格全链复跑：Step 0 Lance FFI PASS；Step 1 Native Release
  configure/build PASS；Step 2 完整 CTest **3391/3391 PASS**（463.65 秒）；
  Step 3 同等插桩 Velox/Protobuf/Abseil 的 ASan/LSan configure/build 和
  完整 CTest **3391/3391 PASS**（778.18 秒，`detect_leaks=1`，0 failed/not-run）；
  Step 4 打包 PASS，包内库与 Release build SHA-256 一致并含 Lance ABI 符号；
  Step 5 JDK17 全仓 `mvn -q -DskipTests install` PASS。Step 6 首次全仓
  `mvn -q test` **FAIL**：CleanCode 两项门槛和新 CDC state enum inventory
  一项门槛。方法抽取与显式 inventory 修正后，两个质量属性测试及 Paimon
  聚焦提交测试 PASS，Step 5 再次 PASS；但 Step 6 全仓尚未在最终源码复跑，
  Step 7–8 未执行。Native shuffle 资源修复仍在进行，以上 Step 0–5 不能
  折算为该修复完成后的最终发布门禁。
- 后续 Step 6 复跑继续暴露旧 Planner Exchange fixture、Coordinator 资源
  拒绝文案/拆账断言，以及 INSERT/DELETE 测试连接器缺少精确 output-handle codec。
  Planner 邻近测试修正后 PASS；Coordinator 资源拒绝聚焦 **30/30 PASS**，
  可往返的 codec fixture 与 INSERT/DELETE 聚焦 **70/70 PASS**，均 0 skip。
  随后的 Step 6 r4 到达真实 E2E 后发现三类独立失败：Paimon split 将
  `FULL` 映射为 Native 不接受的未指定读模式；HA epoch 由 1 跳至 2 时 Worker
  尚未通过心跳确认新 epoch，Coordinator 把预接纳拒绝归类为永久失败；以及
  多 GroupBy task 依据过期 heartbeat 并发领取超过 Worker 仲裁器可用容量的
  full grant。该轮在大量失败后中断，退出码 **130，FAIL/未完成**，绝非
  Step 6 PASS；Step 7–8 未执行。Paimon snapshot split 已按表类型明确编码
  `APPEND`/`PRIMARY_KEY`，incremental split 编码 `INCREMENTAL`，无效模式在
  Java 构造边界拒绝；最近邻 JDK17 **23/23 PASS、0 skip**。Worker 现在把
  `taskEpoch > observedEpoch` 作为无接纳副作用的 HTTP 425，仍只接受认证控制通道
  推进本地 epoch；Coordinator 仅对这项明确拒绝做最多 15 秒的同一 attempt
  重试，每次重新校验当前 epoch 和 execution owner，其他冲突、网络超时均不重试。
  Worker fencing **8/8**、Coordinator 真 HTTP 重试/breaker **7/7 PASS**，
  均 0 skip。上述均是局部复证；full grant 调度与 Flight 服务级预算仍在修复，
  未在最终源码重跑完整 Step 0–8 或 HA/多 Worker E2E。
- 资源接纳必须与 HTTP 200 语义一致：当前修订将 Worker task 的 Native/serialized
  buffer reservation 前移到任务 index 注册前，完整 grant 不足时释放已获配额度并
  以 503 拒绝，不能先答应再异步报 `WORKER_FATAL`。JDK17 邻近测试已过，
  真实多 Worker E2E 尚未跑，状态为**局部通过、端到端待验证**。所有 canonical
  task 的 Native 内存和 buffer 均按 v2 grant 足额预留，不再允许非-shuffle
  task 部分内存运行或隐式多申请 30% buffer；JDK17 Worker 最近邻五组
  **29/29 PASS、0 skip**。Coordinator 仍用心跳瞬时
  `availableMemoryBytes` 和固定 64 MiB 选择门槛，并对同 Worker 的并发 task
  分别按旧快照计算 grant；这不是可线性化容量预约。生产级修复需为每个 Worker
  维护带 heartbeat generation 的 in-flight shadow credit，在选址与 grant 签发
  同一原子步骤扣减，按终态/取消/Worker incarnation 释放；跨 Coordinator 超发
  仍由 Worker 权威准入拒绝，Coordinator 对明确预接纳的资源 503 进行有界排队、
  重选和公平退避，不可把其当作已启动 task 或静默降级内存 grant。验收须包含
  多 Coordinator 同时派发、心跳延迟、Worker 缩容、慢任务、取消与 admission
  ACK 丢失，要求无超额内存、无重复写入、无成功响应后的资源申请失败，并以
  长时间负载测试量化吞吐和排队延迟。在此之前大规模负载可安全拒绝但不能
  宣称高可用调度闭环。
- 2026-09-27 并发复证纠正上述“Native grant 足额预留”的错误模型：真实
  `NativeResultE2ETest.concurrentQueries` 在单 Worker 上失败，首个 task 把
  1,946,157,056 字节 Native 上限当成已用内存预留，后续三个并发查询收到
  503 `WORKER_BUSY`；更严重的是 Native callback 对真实增长又在同一
  `MemoryArbitrator` 记账，形成上限与实际占用的双重计费。官方 Velox 内存
  设计把 query pool 的固定最大容量与动态增长、全局仲裁分开；Trino 官方
  Resource Groups 同样区分并发/排队与实际内存压力（见下列一手资料）。
  当前修订将 canonical `native_memory_bytes` 明确为每任务硬上限，不在 HTTP
  admission 预扣；Native callback 的实际增长在 NodeMemoryPool 和共享
  MemoryArbitrator 各记一笔、释放时各还一笔。前置精确预留仍适用于
  `shuffle_buffer_bytes`。启动顺序调整为 Resource/NodePool → Native callback
  → Flight 服务租约，避免 callback 脱离节点预算。JDK17 三组 Worker
  定向测试 **30/30 PASS、0 skip**；真实 SQL→Worker→JNI 的
  `NativeResultE2ETest.concurrentQueries` **1/1 PASS、0 skip**。显式开启默认
  排除的 `native-required` 后，两组 bootstrap 夹具补齐 catalog resolver，
  **22/22 PASS、0 skip**；heartbeat 改为报告 NodePool 与 arbitrator 的
  较小可用量，邻近 **4/4 PASS**。同源码全仓 Step 5–8 尚在复跑，不得声明
  大规模部署闭环。此修订不是最终调度方案：
  跨 Coordinator 仍需要 bounded fair queue、CPU/transport credits、
  generation-aware shadow placement；Worker 的实际内存仲裁是最终权威，
  Native spill 与 OOM 分类、每任务真实峰值和 P99 排队/吞吐仍需压测。
  不能用固定“四等分 grant”替代动态容量，也不能把 max cap 当作承诺可用量。
  参考：[Velox Memory Management](https://facebookincubator.github.io/velox/develop/memory.html)，
  [Trino Resource Groups](https://trino.io/docs/current/admin/resource-groups.html)。
- 本轮后续全仓 Step 6 两次均非 PASS：首次在 Worker 旧式 full-grant 测试断言
  失败；第二次进入真实 E2E 后，Paimon 的 INSERT-only MERGE 绑定了 OVERWRITE
  handle，而 Native 写计划是 INSERT，另有多分片 `GROUP BY` 返回同键多行。
  Paimon handle 已按仅含 `WHEN NOT MATCHED INSERT` 的语句选用追加写，
  `PaimonMergeIntoTest` 真实 SQL 专项重跑 exit 0。聚合缺陷源于任务局部 Aggregate
  后直接 gather；Planner 现将 grouped Aggregate 的输入按 group key HASH 分区，
  无分组 Aggregate 的输入集中到单个 Native worker，完整排序在 Native 全局边界后
  执行。`PlanFragmenterTest` **9/9 PASS**，但第一次真实三阶段 E2E 因
  `SCHEDULING` 被误判为上游可读而报 `No producer tasks`；调度器改为仅在上游
  `RUNNING`（已有 producer records）后开放 pipeline，邻近测试 **19/19 PASS**。
  第二次同 E2E 继续发现 Native writer 用 LZ4、Velox Exchange consumer 留在
  默认 `none`，大页报 `NoCompressionCodec: invalid uncompressed length`；两端
  QueryConfig 现绑定同一 codec，压缩页经真实 Flight→Velox 解码的新增 Native
  专项 **1/1 PASS**。本修订后的 Step 0 Lance FFI、Step 1 Release build 和
  Step 2 全量 CTest **3,406/3,406 PASS**；Step 3 使用仓库 `dev-asan`
  preset（`detect_leaks=1`）完整 **3,406/3,406 PASS**，Step 4 打包与
  Release 库 SHA-256 一致，Step 5 JDK17 全仓 `-DskipTests install` PASS。
  首次手工 ASan 命令漏带 preset 中限定的第三方进程级抑制，在 CTest 发现阶段
  以 LeakSanitizer 退出，记为失败尝试而非通过；正式 preset 的完整结果如上。
  Step 6 Java 全仓测试在真实 SQL E2E 揭示第二条 codec 缺口后中断、退出 130，
  记为 **FAIL/未完成**：统一 Velox QueryConfig 启用 LZ4 后，Flight result 服务仍
  用默认 `none` 解码 OutputBuffer 页，多个 DML 读回发生
  `NoCompressionCodec: invalid uncompressed length`。Result task 的 type、VECTOR
  维度和 compression kind 现按 task 一起注册，结果解码复用同一 codec；这一
  后续源码的 Step 0–2 已重新通过（Release CTest **3,406/3,406**）；
  Step 3 `dev-asan` preset（`detect_leaks=1`）完整 **3,406/3,406 PASS**；
  Step 4 包内库与 Release 构建 SHA-256 相同，Step 5 JDK17 全仓安装 PASS。
  上述两类失败的真实 SQL 定向复证中，`MultiCoordinatorRoleRoutingE2ETest`
  14/14、`WriteDmlTransactionE2ETest` 15/15 PASS，均 0 skip。
  这不是 Step 6/7 全量通过：Step 6 正在重新执行，Step 7–8 尚未运行。
  不能把上述
  局部 PASS 折算为 Stage 0–8 PASS；全局 singleton 排序的规模性能尚未验收。
- 2026-09-27 随后的 Step 6 全仓运行仍为 **FAIL/未完成**，不能沿用上一条
  “正在重新执行”的表述。真实 E2E 的首个失败是共享 PostgreSQL
  `distributed_lease` 被旧故障转移测试的无条件清理删除；运行中的 Worker
  已观察到较高控制纪元，而重建的 Coordinator 从新租约纪元重新开始，
  Worker 正确以 HTTP 409 拒绝回退任务。这是测试隔离缺陷，不能放宽生产
  fencing。所有使用该共享租约表的 E2E 类现改为逐测试唯一的真实 JDBC
  租约命名空间，不删除 `coordinator_leader` 或其他测试的 epoch 高水位；
  命名空间单元测试 PASS，10 组真实 PostgreSQL E2E 的 Surefire 汇总
  **81/81 PASS、0 fail、0 skip**。本次聚焦结果只证明该隔离修复，完整
  Step 6、Step 7–8 仍待最终同源码重跑。
- Lance 现态复核：本机 Release Rust FFI 构建 PASS、`cargo test --locked --lib`
  **14/14 PASS**，固定版本读取改动的 Lance Native CTest **32/32 PASS、0 skip**。
  `source_snapshot` 现须是非零十进制 Lance version，FFI ABI 4 的 reader 只打开
  catalog 固定版本；后续 append 后仍可读取旧版本的 Native 测试已通过。旧审计里的
  `SKIPPED` 不再代表当前组件状态；然而 Java catalog/connector 尚未生成
  可提交的 Lance read/write binding，FFI commit/discard 也未贯通任务终态与
  恢复，因而 public SQL→Lance 仍是**未闭环**，不得把 FFI 绿灯写成生产可用。
- 共享 E2E 的下一轮 Step 6 诊断测试在缓存压力用例后大面积失败，已中断并记为
  **FAIL/未完成**。最早失败不是租约纪元，而是该用例把相同 Worker ID 重新注册为
  `localhost` 且 Native result port 为 `0`，污染后续任务放置与结果端点校验。
  测试现经真实 `WorkerManager.onHeartbeat` 仅更新压力，保留注册端点和 capability；
  该类 **6/6 PASS**，邻近 Cache/Optimizer E2E 及 Worker AI/TaskExecutor 定向测试
  均 PASS。上述复证不能替代全仓 Step 6，也不放宽生产端点一致性检查。
- CDC 的 coordinator-local writer 现在只允许在 catalog 发布尝试前清理 staged
  files；发布成功、并发发布或发布结果不明时均禁止 `discard()` 删除文件。
  stream tenant 已传入 connector session；第二阶段 staging 失败会清理第一阶段
  未发布文件；未接通的 nullable Native dispatch hook 和不提交 catalog 的重复
  writer 已移除。bootstrap 也不再提前创建完全未使用的 INSERT handle，纯 DELETE
  批次不再依赖该多余步骤。JDK17 的 CDC writer **9/9**、bootstrap wiring **3/3**、
  operation fidelity **8/8** 聚焦 PASS、0 skip。但生产 CDC 仍在 Coordinator JVM，
  Paimon 拒绝 context-free commit，
  DELETE/INSERT 分阶段非原子，不能宣称分布式 exactly-once 写入。CDC commit
  state 现增加 PostgreSQL `PREPARED→COMMITTING` CAS 单赢家、发布失败保留
  `COMMITTING`、恢复时隔离未知结果、以及 V39 rollback 后同 batch key 可重试；
  带参与者的回滚也先 CAS `PREPARED→ROLLING_BACK` 再清理暂存文件，防止另一
  coordinator 在状态读取与清理间抢先发布。对应 PostgreSQL coordinator
  **29/29 PASS**、pipeline **5/5 PASS**、Flyway V39 迁移 **1/1 PASS**，
  均 0 skip。当前 2PC 的 JDBC 行只持久化最低分区的 representative range；
  `findLastCommittedOffsets` 既丢失其余分区，又把 stream `SourceId` 当作真实
  topic 重建 key。多 topic/多分区 failover seek 因而不可靠，须把完整
  `(topic, partition, next_offset)` 集合与 batch 身份持久化，并令可见的
  `COMMITTED` 状态和 offset 集合同事务提交；在此之前不能宣称 source 恢复
  闭环，更不构成跨 catalog/offset 原子提交。
- Index commit log 已使用 V38 持久 per-table head 和有界公平领取，运行时
  不再对整批 backlog 执行 `ROW_NUMBER()`。JDK17 metadata **16/16**、
  coordinator **15/15**、显式 100 万 hot + 1 other PostgreSQL scale test
  **1/1 PASS、0 skip**；`EXPLAIN ANALYZE` 使用两个队首/日志索引、无日志
  Seq Scan 或 WindowAgg，执行 0.660 ms，另一表首返回。这证明该规模下
  单次领取的有界计划，不等于千万级写入吞吐、持续消费或故障恢复验收。
  外部 processor 成功而 ACK 丢失时仍为 at-least-once，依赖消费端幂等。
  Iceberg 恢复的 table UUID、当前祖先链
  marker 判定、codec v2 聚焦 **14/14 PASS、0 skip**；catalog-level 原子
  claim/fence 仍缺，超时旧提交与恢复并发可重复发布，保持 P1 未闭环。
  `IndexMetadataStore` 的默认 `pollUnprocessedCommitEvents()` 和
  `markCommitEventProcessed()` 已从静默空返回/无操作改为 fail-closed；
  生产 JDBC provider 覆写两者。非 JDBC 装配的启动验收和全仓复证仍待完成，
  不能依赖静态 dead-stub gate 将其 SPI 默认行为豁免。
- 连续查询仍有独立接线缺口：生产 `StreamingBootstrap.build()` 先以仅写日志的
  refresh executor 启动 `ContinuousQueryConsumer`；该 consumer 的 tick 只会
  记录日志，不能计作 watermark 驱动刷新。`DynamicTableDdlExecutor` 在组件调用
  时还会向独立 `RefreshScheduler` 注册周期任务，故不能推断组件级周期刷新也
  不工作；但仓库生产源码没有构造该 DDL executor 或注册
  `CreateContinuousQueryTask`，仅 E2E fixture 条件注册，公开 SQL 入口未接通。
  整改应删除空转路径，并把唯一真实、持久且可恢复的刷新作业接到生产入口。
- `.pangrova/setup.sh --check`、SPI annotation/stability、dead-stub、
  architecture class references 与 doc freshness 静态检查 PASS；
  `git diff --check` PASS。它们均不替代完整 Java/Native/ASan/E2E 门禁。

### 0.9 2026-09-30 根因收口与验收状态

本次改变跨语言执行契约，因此旧 Native archive、旧 JNI package 和此前定向 PASS
不能作为当前发布证据。完整 Step 0–8 必须在最终源码上顺序重跑；未完成前不宣称生产级
或行业领先，也不承诺任何架构未来绝无问题。

- Lance 的事实边界已更正：Iceberg 标准数据文件格式不包含 Lance；Paimon
  release-1.4.1 有官方 `paimon-lance` 模块，其 writer 使用单文件
  `LanceFileWriter`。本仓未集成该模块或对应 Native 单文件适配，不等于上游不支持。
  独立 Lance Dataset 的 version/manifest 发布与 Paimon snapshot/CommitMessage
  提交不可混同；向量、Embedding 和 Paimon Lance 文件不应以独立 Dataset connector
  为前提。详见 [Lance 归属与提交边界更正](ai-native-lance-write-adr.md)。
- SQL 元数据主链正统一传递 authenticated session，而非 `system`/空属性：包括
  DDL、CTAS、SHOW、VIEW、CALL、预热、time travel 和 split 枚举。Branch、索引
  后台作业及服务权限仍在复证；只修主链不代表所有用户入口已闭环。
- canonical v2 新增通用物理 scan partition 和 Worker-local object-store binding，
  Java 接口已稳定，Native/Rust ABI 6 消费链和真实分区 fixture 正在验证。
  分片必须读取 pinned manifest 的指定物理 fragment；严禁每个 split 重读全 Dataset。
  存储凭据只能由 Worker 从可信目录/租户绑定解析，不能信任客户端 task 内注入的凭据。
- 认证配置已归一为 YAML kebab-case，未知或跨 provider 配置以及签名旁路配置拒绝；
  认证管理器关闭后拒绝新请求。真实本地 OIDC/JWKS 签名测试与认证合同定向验证
  **PASS：45 tests，0 failure/error/skip**。命令：
  `mvn -q -pl pangrova-security -am test -Dtest=OAuth2AuthenticatorTest,AuthenticationManagerTest,SecureJwtParserVisibilityTest,AuthenticationContractTest -Dsurefire.failIfNoSpecifiedTests=false`。
  新增 production bootstrap + PostgreSQL 授权集成测试尚未实测，不折算为 SQL/Native E2E。
- 生产 Linux 镜像改为同一源码的 Bake Native/Java 构建图，Native production 模式
  要求静态第三方依赖、ELF 架构匹配、无 RPATH 与系统 ABI-only 动态依赖。
  Bake 图解析和 shell 静态检查已通过；实际镜像、多进程 Worker 与故障矩阵尚未执行。
- 当前 JDK17 coordinator/E2E reactor `test-compile` 初次复跑为 **FAIL**：
  `SqlE2ETestBase` 未迁移 statistics provider 的 session 参数，
  `NativeCapabilityHandshakeE2ETest` 缺失显式 connector 配置。修复后同命令
  `mvn -q -pl pangrova-coordinator,pangrova-testing/pangrova-test-e2e -am test-compile`
  已复跑 **PASS**，但这不是实际 E2E 执行。
- 认证健康状态不再改变匹配的认证器；无效凭证计入拒绝指标，不触发后续认证器
  降级，也不使有效用户永久无法认证。JDK17 的认证/配置/真实 PostgreSQL 授权及
  planner session/Native control RPC 邻近验证 **PASS：102 tests，0 failure/error/skip**。
  测试集合为 `OAuth2AuthenticatorTest,AuthenticationManagerTest,AuthenticationManagerObservabilityTest,SecureJwtParserVisibilityTest,AuthenticationContractTest,SecurityConfigurationLoaderTest,PangrovaCoordinatorMainTest,SignedOidcSecurityBootstrapIntegrationTest,TableResolverSessionTest,PlannerWiringSessionIntegrationTest,NativeDatasetControlClientTest,NativeDatasetControlRpcExceptionTest,WorkerNativeDatasetControlTest,NativeDatasetControlBootstrapTest`，
  命令为 `mvn -q -pl pangrova-coordinator -am test -Dtest=<上述集合> -Dsurefire.failIfNoSpecifiedTests=false`。
  其中真实签名测试连续拒绝八次无签名 token 后仍接受合法签名；RPC 测试覆盖
  明确授权拒绝、UNKNOWN 发布结果、关闭取消与租约/owner 校验。它们是组件集成，
  不计作 Native 数据执行或多 Worker E2E。
- 最新目标明确要求数据库核心的 Feature Store/评估等数据执行链闭环。
  2026-09-27 的外围裁剪只移除 Agent 编排和 LLM 计费，不得以此豁免数据库侧
  snapshot、权限、durable materialization、指标计算和分布式验收。当前单源 offline
  export 或评估目录元数据仍不满足完整能力，必须继续实现和验证。

### 0.10 2026-09-30 未提交更改复审后的修复与实测

本批次修复复审发现，不把拒绝不安全行为等同于能力全部完成。生产入口不再接受
客户端执行计划；Feature 定义保存 SQL 逻辑意图，CREATE/MATERIALIZE 走当前用户的
同一个生产 compiler、SELECT 权限、RLS 和列脱敏链。持久化计划仅作审计证据，不能
替代新一轮权限与策略编译。编译记录的是实际使用的策略表达式（含明确的无策略），
不是另一次读取或虚构 policy epoch。共享的 RLS 决策缓存和受保护 fragment 的裸 SQL
重规划重试已删除；不支持的算子/表达式/普通函数在派发前拒绝，只有 converter 产生的
精确 governed AI invocation 可以进入后续模型绑定。

后台执行契约要求明确 source SELECT、target ALTER、完整读取列及 RAW/TRANSFORMED
语义；创建、领取、续租、派发和发布复核当前直接角色、授权、策略、ACTIVE tenant 与
成员关系。PostgreSQL statement trigger 对直接 SQL 的权限/策略/租户变更也取得事务门禁，
发布在同一 connection 内执行复核及元数据 CAS。普通元数据读取不借用构建者的执行授权，
避免撤销构建者权限后隐藏已经发布的合法对象。后台身份不使用 `system` 或旧 IdP groups
伪装当前权限；没有当前外部 group authority 时，仅直接角色可授权 durable work。

以下结果按执行窗口记录，不覆盖之后的源码变更：

| 检查 | 结果 | 证据与边界 |
|---|---|---|
| canonical Rust 锁定依赖构建 | PASS | `build-lance-ffi.sh`，修复 Cargo.lock 根 package 的 prost/prost-types 依赖；日志 `/tmp/pangrova-fixes-chain-step0.log`。该窗口为 ABI 6。 |
| Native Release 编译 | PASS | `cmake --preset dev-release -S pangrova-worker-native` 与 `cmake --build pangrova-worker-native/build --parallel 4`；日志 `/tmp/pangrova-fixes-chain-step1-build.log`。 |
| Native 全量 CTest | PASS | 3480/3480，零失败；日志 `/tmp/pangrova-fixes-chain-step2.log`。 |
| matching-ASan 构建与全量测试 | PASS | 使用相同 Clang ABI 的 ASan Velox、Protobuf 25.3 和 Abseil；3480/3480，零报告错误；日志 `/tmp/pangrova-fixes-chain-step3-tests.log`。不宣称 Rust unsafe 代码已由 ASan 完整覆盖。 |
| classpath JNI 打包 | PASS | `package-native-lib.sh`；日志 `/tmp/pangrova-fixes-chain-step4.log`。之后的 ABI 7/physical-file 改动必须重新构建并打包。 |
| 规划与 Native 准入专项 | PASS | `SqlCompilerTest,NativeAdmissionTest,SubstraitPlanValidatorTest,PartitionLocalPlanInspectorTest`，34 tests，零失败/错误/跳过；日志 `/tmp/pangrova-fixes-planner-tests.log`。 |
| PostgreSQL 权限与持久化发布专项 | PASS | `PersistentRoleStoreTest,ArtifactBuildContractTest,JdbcArtifactBuildStoreTest,JdbcIndexArtifactPublicationStoreTest,JdbcTabularExportPublicationStoreTest`，37 tests，零失败/错误/跳过；日志 `/tmp/pangrova-fixes-authority-store-tests.log`。 |
| 当前权限与直接 SQL 撤权并发 | PASS | `ArtifactCurrentAuthorityIntegrationTest` 5 tests；显式设置 `-Dpangrova.metadata.excludedTestGroups=none` 才实际运行，默认排除 integration 的零测试构建不计通过。日志 `/tmp/pangrova-fixes-authority-concurrency-tests-enabled.log`。 |
| 全 Java install | FAIL，继续修复 | 初次执行被 LanceSplitManager 的测试构造迁移阻断；不能以 scoped main compile 替代完整 install。日志 `/tmp/pangrova-fixes-chain-step5.log`。 |
| Paimon 实际 SDK fixture | FAIL，继续修复 | 暴露 footer 版本假设、绝对路径解析及 append bucket-key 测试配置错误。必须按 SDK 实际文件格式复证，而非弱化断言或改写 footer。日志 `/tmp/pangrova-fixes-feature-compilation-tests.log`。 |
| Linux 生产镜像 | FAIL，重试中 | 官方 Abseil Git 拉取遇到 GnuTLS 连接中断；未完成 Linux Native 编译、镜像启动或多进程 Worker 验收。日志 `/tmp/pangrova-production-linux-bake.log`、`/tmp/pangrova-production-linux-bake-retry.log`。 |

尚需完成：最新 ABI 7 的 Paimon physical-file Native decoder 与真实 SDK 跨读、历史快照
schema/manifest 的 public-entry E2E、跨分片 aggregate/window/sort/join 的持久化通用
StageGraph（不能每 split 执行完整 SQL）、末级 export 的完整覆盖发布，以及最终源码上的
完整 Step 0–8、全 Java/container 回归、独立 Worker 与故障恢复矩阵。单源 map 的安全准入
不是这些全局执行能力的替代方案；尚不能声明生产级交付。

### 0.11 2026-10-01 增量修复与最终回归前的状态

以下均是限定源码窗口的证据，不替代最终 Step 0–8 或独立进程分布式验收：

| 范围 | 结果 | 事实与限制 |
|---|---|---|
| ABI 7 Rust/Native Release | PASS（窗口） | 锁定 Rust 构建及 Native Release 已通过；之后逐文件 schema evolution 与 merge policy 的协议/Native 修改使其不再代表最终源码。最终构建、CTest、matching-ASan、打包必须重新顺序执行。 |
| Rust Lance 真实文件/存储测试 | PASS | `cargo +1.98.1 test --locked --release --target aarch64-apple-darwin --manifest-path pangrova-worker-native/lance-ffi/Cargo.toml`，18/18、零失败/忽略。使用实际官方 writer 的 `(0,3)` footer，验证精确读取范围、projection、nullable VECTOR、Unicode、并发及真实 MinIO 条件提交；不改写 footer，不声称其他版本 corpus 通过。 |
| PostgreSQL invalidation | PASS（定向） | `JdbcInvalidationPollerTest,JdbcInvalidationPollerIntegrationTest`，30 tests、零失败/错误/跳过。真实迁移与 PostgreSQL statement gate 在分配 ID 前串行化追加直至提交；跨事务 rollback/gap、旧 timestamp、1000 条分页、远端未知 watermark 的清理安全均实测。 |
| 规划 COW 与分布式图 | PASS（定向） | `DmlCopyOnWritePolicyTest,SubstraitDmlRootNamesTest,AuthorizedStageGraphTest,PlanFragmenterTest`，26 tests、零失败/错误/跳过。验证策略范围外原始目标行保留、无 WHERE 删除、掩码仅用于匹配/赋值、SET 顺序，以及 aggregate/window/export 的分区规划；不折算为 Worker/JNI 或多进程 E2E。 |
| Typed receipt 与持久化图 | PASS（定向窗口） | SPI fingerprint 5、security scope 5、metadata 41，合计 51 项、零失败/错误/跳过；真实 PostgreSQL 验证 WAL、artifact publication、当前授权、immutable graph 与有数据迁移拒绝。之后 V46 owner fence 和 outbox ACK 修改仍须重新验证，不能外推为接管闭环。 |
| 受保护源物理行号 | PASS（规划定向） | `PhysicalRowLocatorPlanAugmenterTest` 5/5，真实 SQL compiler 的 filter/project/函数扩展及 emit ordinal 保留，拒绝错误 FQN/伪造隐藏列/坏 mapping。尚未验证 generated Worker/JNI 写入与行号 sidecar 跨读。 |
| Native Step 0–4（此前窗口） | PASS（历史窗口，不代表新增读取链） | 先修正 bucket-root warehouse 误拒绝、TABULAR 测试缺读取 grant、SDK 不存在的 COUNT 与旧 LISTAGG 断言；随后按顺序重跑锁定 Rust FFI、Release 构建、CTest 3493/3493、匹配 Velox/Protobuf/Abseil 的 ASan 3493/3493，并打包实际 macOS/aarch64 JNI 库。Release 用时 505.83s，ASan 1293.14s。之后补 Lance Dataset 真实物理行号输出，已从 Step 0 开始重新验证，未完成前不能沿用旧 PASS。 |
| Java 写入准入/当前授权 | FAIL 后修复重验 | Coordinator 最近 220 项中 219 项通过、1 项失败，无跳过：实际 SQL+PG 策略测试暴露优化器根据未脱敏匹配谓词把原始存储列常量化。现保留 validated mutation 到策略与 COW 转换，匹配条件不再提前改写原值；修复后的 DML/物理行号最近回归已通过。随后新增普通查询与后台源的统一“绑定→授权/策略→优化”顺序及 fail-closed 配置测试，仍需最终全仓复跑。 |
| Linux 生产构建 | BLOCKED，未通过 | 默认 BuildKit 网络不能连接同一 GitHub IP，构建时 host 网络可连接；运行时隔离不变。依赖 clone 只缓存完整 pinned checkout，失败有界重传，缓存 revision 不一致明确失败。2 CPU/4 GB 的 arm64 Docker VM 内执行 amd64 模拟编译已到 gRPC 781/1932；为释放资源给真实容器回归，已停止本任务的构建进程、保留构建缓存，未更改 Docker/VM 或用户容器。尚无最终镜像或独立 Worker 集群 PASS。 |
| Durable graph/写入授权/outbox | 实施中，未通过 | 单一持久化 StageGraph、普通写与 generated 写同一 authenticated receipt、当前模型角色和 COMMITTED WAL 保留已落源；同事务 outbox/ack、真实作业 execution-owner fence、shuffle barrier、外部 connector declaration CAS、受保护 generated source 仍须完成并实测。 |
| Worker/JNI 最近验证 | PASS（定向） | `NativeLibraryLoaderTest,NativeCapabilityLoaderTest,EnvironmentConnectorSecretResolverTest,WorkerCatalogRuntimeTest,NativeWriteBatchTest,NativeWriteBatchFailureTest`，133 项、零失败/错误/跳过；实际打包 JNI 可用且 functional。它们是组件/JNI 验证，不是 SQL 端到端或独立进程分布式验收。 |
| Iceberg/Paimon SDK 最近验证 | PASS（定向） | Iceberg 30 项、Paimon 18 项，48 项零失败/错误/跳过；真实 Hadoop/Parquet SDK 验证空静态覆盖只删除原 scope、恢复保留 scope、范围外文件拒绝，以及历史 Paimon schema/merge admission。空 overwrite 的真实 SDK `dataSpec()` 异常已修复：仅有新增文件时启用 added-file scope 验证，原 scope 与冲突检查不变。 |
| Java 全仓最近门禁 | FAIL，修复中 | Step 5 曾 39 modules 编译安装成功；随后源码变化使其不代表最终版本。第一次 Step 6 有两个 PostgreSQL 启动错误，同测试在释放本任务模拟编译资源后 25/25 通过；不能由此证明 OOM。全仓重跑又暴露包规模、类长度与方法分支门禁，按职责拆分且不改阈值；须重新编译及全量回归。 |
| 覆盖率门禁真实性 | PASS（门禁脚本测试），实际门禁 FAIL | 旧脚本 `BASE...HEAD` 遗漏全部本地变更，且文件级测量可能掩盖未覆盖新行。现在计入 staged/unstaged/untracked 的新增可执行行，按各模块实际 JaCoCo XML 测量，缺失/过期报告明确失败。17 项真实临时 Git/index/XML/JDK 17 classfile 测试通过；仅当同一 SourceFile 的全部新鲜 classfile 确实没有 method Code 时记为零可执行行，不增加覆盖信用，也不豁免 default method、其他顶层类或过期/损坏 classfile。最近实际 `--base-ref HEAD --threshold 60` 因 896 个缺失/过期测量失败，必须完成带 coverage 的最终回归，不能声称达到 60%。 |
| Generated 值查询读取 | 实施中，未通过 | canonical Read overlay、历史 schema/定义校验与实际分片资源预算已落 Java 源；真实 PostgreSQL 已验证两 source publication 仅绑定 query 所需 source 的对象。Lance Dataset 已补 SDK row-address 输出和逐行 partition/offset 校验，普通扫描保持原返回模式，禁止用连续返回行号冒充删除后的真实 offset。新增 Native/Rust 源已冻结并启动 Step 0–4 复跑；仍待 Java 与 public SQL E2E。另发现 Iceberg/Paimon 返回 metadata 时丢弃 SDK 属性，正在修复真实 generated 声明保真和历史证据，不以 Java binder 存在计能力通过。 |
| 精确分区接管准入 | PASS（定向） | `JdbcArtifactStageExchangeStoreTest` 11 项、零失败/错误/跳过；真实同一 PostgreSQL 事务验证权限/owner、精确 stage/partition/page 坐标、重复领取、binding 回滚，以及外部 exchange 未 finalize 时不得消费。尚不等于整个后台 DAG 重启与终态闭环。 |
| Coordinator 资源生命周期 | PASS（定向） | 30 项、零失败/错误/跳过；启动阶段异常按依赖顺序释放所有已初始化资源，单个清理失败不阻断后续资源。真实 OAuth2 issuer/PG 测试验证 metadata pool 在内部认证准入失败后实际关闭且 HTTP 未启动；普通启动/停止与属性测试同时通过。 |
| 最近质量门禁 | PASS（定向窗口） | `CleanCodeMetricsPropertyTest,PackageStructurePropertyTest,PackageDependencyAcyclicityPropertyTest`，10 项、零失败/错误/跳过；穷举源码验证类/方法/分支/构造参数和包结构，不降低阈值。后续新增源码仍须重新复验。 |
| 回归测试选择真实性 | PASS（配置验证），全量测试进行中 | 默认 Surefire 排除 `*IntegrationTest`，Worker/Coordinator/Metadata/E2E 子模块还覆盖 tag 或文件筛选；新增 `full-regression` 显式清除日常筛选，保留默认日常配置，发布回归关闭 Worker 的失败自动重试。四个模块真实 Maven effective POM 的最终 default-test 配置验证通过，7 项选择/覆盖率接线测试通过；模块 JVM 参数使用 late-bound JaCoCo agent，实测产生 execution data/report。CI sanitizer 使用已有 CTest preset 的正确 ASAN/LSAN 参数；新增 production-regression 接 canonical same-source Linux Bake image→实际 classifier/JNI→全量回归/coverage→E2E/Smoke，日常 JDK17/21 filtered job 不冒充完整回归。新 CI job 未执行，配置正确不代表镜像、产品或分布式测试通过。 |
| Lance Dataset Connector 身份读取 | PASS（Native Step 0–4 窗口），Java/端到端待验收 | 最近 35 项 Native 测试中曾 34 PASS、1 FAIL；生产 `applyLanceIdentity` 按 physical children 数组写 hidden channel 越界，形成 type=3/children=1 的非法 RowVector。现按完整 output schema 精确组装并校验普通列 type/size，测试保留真实 source/offset 断言并覆盖隐藏列不同位置与 partition 注入。顺序重跑 Step 0 Rust、Step 1 Release 构建 PASS；nearest 35/35、完整 Step 2 CTest 3496/3496 PASS（469.82s）。Step 3 匹配 Velox/Protobuf/Abseil 的 ASan 构建及全量 3496/3496 PASS（2351.64s，已有第三方/runtime suppressions 未改）；Step 4 实际 osx/aarch64 JNI 打包 PASS。日志 `/tmp/pangrova-production-row-identity-final-step{0,1-build,2,3-tests,4}.log`。不宣称 Rust unsafe 已被 ASan 全覆盖，完整 Java/SQL 与独立多 Worker 验收仍未完成。 |
| 历史声明与冻结执行模板 | PASS（定向窗口） | SPI 1、Metadata 16、Iceberg 16、Paimon 15、Coordinator 13，合计 61 项零失败/错误/跳过。真实 SDK 验证 current FULL 使用当前声明、TIME_TRAVEL 使用 exact snapshot schema 与 summary/相邻 metadata 文件证据；历史证据缺失显式拒绝，不猜测为空。真实 PostgreSQL 验证旧 graph envelope 拒绝但原 jobs/pages 保留、maxPoolSize=1 的同连接证据读取与纯 materialize，提交后连接释放。实际后台 DAG runtime 仍须接通并验证。 |
| 编译声明、SDK 提交约束与 durable report | PASS（最近定向窗口），全仓进行中 | JDK 17 合并验证 146 项：Planner 5、Metadata 14、Iceberg 41、Paimon 4、Coordinator 82，零失败/错误/跳过。实际 protected SQL 证明 VALUES 没有虚构 source READ，编译后 schema/生成声明变化在 authority supplier/SDK allocation 前拒绝；receipt 必须匹配同次编译声明。真实 PG/SDK 双客户端覆盖声明改变时的 CAS 拒绝、仅追加数据时的安全重试、成功提交后的 DDL 不撤销 publication。真实 PG report 验证 original wire 保真、exact allocation/page attempt、takeover、PENDING/未登记 terminal 拒绝及 SCHEDULED→FINISHED 网络乱序；durable producer 提交先于普通 DAG terminal。日志 `/tmp/pangrova-production-compilation-admission-sdk-durable-reports-nearest-tests.log`。 |
| Protocol report properties 与声明边界 | PASS（定向窗口） | 另行运行 13 项真实 report 重放/并发属性测试和 1 项实际 PG publication（maxPoolSize=1）均通过；SPI 声明 3 边界 + 1 固定种子 100 次属性验证通过，ordered typed names 必须与 connector names 精确一致。日志 `/tmp/pangrova-production-report-properties-publication-nearest-tests.log`、`/tmp/pangrova-production-write-declaration-boundaries-nearest-tests.log`。它们不能替代实际 Worker HTTP、provider 或独立分布式部署。 |

新发现的高风险根因是把 RLS 当普通源过滤器放入覆盖式 DML：权限范围外的数据会从
replacement 中丢失；掩码投影也可能把存储原值替换成脱敏值。目标侧策略现限制 mutation
match，未匹配行仍输出原始列；MERGE 将目标策略并入 join match 并保留目标关系，来源侧
维持普通读保护。DELETE 已删除 Java metadata fast path 和无 WHERE 的 INSERT 改写，保留
原始 DELETE 经同一权限编译/Native/持久化提交链。此处的定向 PASS 不代表完整 DML 发布验收。

进一步发现专用 DML pipeline 未应用策略、Factory 在编译前提前分配 connector 写句柄。
现已删除 Factory 重复分配入口，由同次实际 `applyPoliciesObserved` 产生写入读政策证据，
Scheduling 在当前权限/定义 receipt 校验后唯一分配。原 AST 的 UPDATE 和 MERGE clause
权限不再由物理 OVERWRITE 推断为 INSERT/DELETE。上述新增执行链、真实 SQL COW E2E
和重复调用权限一致性测试仍待 Java 与 JNI 实测，不按源码存在计闭环。

当前仍不可声明“全部生产级”：最终全仓编译/Java/container 测试、后续 Native 改动的全链复跑、
真实 provider 与历史 schema 跨读、持久化全局 export/failover、独立 Worker 的大规模/故障
矩阵尚未通过。环境失败、未执行项和安全拒绝均不能记为能力已闭环。

Native 本轮日志为 `/tmp/pangrova-production-chain-final-step{0,1-build,2,3-tests,4}.log`；
最近写入准入日志为 `/tmp/pangrova-production-coordinator-closest-final-tests.log`，
修复后的 mutation 日志为 `/tmp/pangrova-production-protected-mutation-final-tests.log`。
最近 Worker 日志为 `/tmp/pangrova-production-worker-package-jni-tests-import-fixed.log`，
SDK 日志为 `/tmp/pangrova-production-static-overwrite-sdk-tests-empty-fixed.log`，
覆盖率脚本测试为 `/tmp/pangrova-production-diff-coverage-gate-unit-tests.log`。
日志属于当前执行环境，不是仓库中永久归档的发布证明；每次源码变化必须重新验证受影响门禁。

### 0.12 2026-10-01 全量回归暴露的后续根因与重新验收

本节取代此前小节的“最新状态”，不抹去失败历史，不把不同源码窗口的 PASS 拼成发布验收。

- Java Step 5 全仓 39-module compile/install 曾 PASS（45.191s）；其后的
  `-Pfull-regression,coverage verify` 在 Lance 模块 FAIL。真实 metadata 路径需要
  持久目录，但旧 allocation 测试使用永不连接的假 JDBC 地址。现迁到真实 PostgreSQL，
  保留 INSERT/OVERWRITE allocation、类型、租户断言，并新增 DROP 后旧 handle 拒绝；
  Lance 最近 18/18 PASS。Planner 与 durable runtime 后续改动仍要求重新执行 Step 5–6。
- 任务身份登记已抽成单一 package-private registry，解决 full-regression 捕获的
  Tracker 超过 500 行问题，不调整阈值。最近质量 9 项和 Tracker/Receiver/report 属性
  34 项合计 43/43 PASS；后续全量回归实际通过质量检查并继续到 Lance，不代表整个 reactor 通过。
- Generated-column pre-policy 重写接入实际 SQL/AST 共用入口，INSERT 使用完整 NEW row，
  UPDATE 使用同时赋值的 NEW 源，MERGE 分别处理真实 INSERT/UPDATE action，DELETE 和
  未修改源列的 UPDATE 不生成 AI 调用。首次定向 32 项有 3 FAIL：INSERT 字面量/目标
  nullability 不一致及早期声明拒绝的异常包装。已对真实普通 NEW 值作精确目标类型 cast，
  保留早拒绝 root-cause 断言；最终 Planner 32 与 canonical capability 7 合计 **39/39 PASS**，
  属性验证固定 seed=8675309、30 次。日志
  `/tmp/pangrova-production-pre-policy-generated-planner-nearest-tests-r3.log`。
  SYNC DDL/admission 拒绝仍保留，未声明同一 atomic write/public SQL 闭环。
- 复证 ASYNC NULL 缺陷后，生成值 segment 与 Native overlay 已统一为唯一 v2：
  validity bitmap 保留混合/全 NULL 和每条 physical locator；NULL payload 使用 canonical
  positive zero，不返回伪造零向量。双方 build/overlay capability 2.0.0 与 format major 2
  指纹一致，旧格式显式拒绝，不增加双读兼容。新增分配前 dense memory grant 检查。
  Native 最近 **33/33 PASS**，涵盖真实不可变对象、locator 跨读、损坏位图/NULL payload、
  旧格式拒绝与超预算无 publication。日志
  `/tmp/pangrova-production-null-generated-final-nearest-tests.log`。
  此修改使此前 3496/3496 Release/ASan 窗口不再代表最终 Native；本窗口 Step 0 Rust、
  Step 1 Release configure/build PASS（零 warnings/errors），Step 2 全量 CTest
  **3499/3499 PASS（452.22s）**；Step 3 匹配依赖 ASan configure/build PASS，
  首次全部测试 **3485 PASS / 14 Timeout（17853.71s）**，不是 PASS。
  主机 `pmset` 复证反复 Clamshell Sleep，与约 15 分钟的测试耗时跳变对应；
  原 timeout 下失败 14 项诊断重跑 **14/14 PASS（17.18s）**，仅是诊断。
  `caffeinate -is` 下保持原 300s timeout/全 3499 范围重新运行，不放宽门禁；
  合盖休眠仍需保持机器唤醒或使用不休眠验收环境。
  全量 R2 最终 **3498 PASS / 1 FAIL（2873.35s）**，不是环境阻塞或 PASS：
  `PlanExecutionDriverCancelTest.CancelDuringVeloxExecution` 实际观察 FAILED 后输出队列仍未关闭。
  根因是 plan conversion/post-completion 异常路径先发布 FAILED，部分分支漏关队列，
  而 terminal cancel 直接返回。现统一 failure channel 关闭先于终态发布；cancel/deadline/
  外部 failure 同样先终止生产者与关闭队列，再发布 CANCELLED/FAILED，保持 committed producer fence。
  新回归要求损坏 plan 的 FAILED 可见时 queue 已关闭且阻塞消费者实际被唤醒，不能只 join 后断言。
  Native 源码窗口再次变化，重新从 Step 0 执行；Release JNI cache 全部显式收敛 JDK 17，
  不混用此前缓存的 JDK 21 JVM/AWT library。R2 日志
  `/tmp/pangrova-production-null-generated-final-step3-tests-r2.log` 保留原失败。
  Step 4–8 尚未通过；public SQL/provider NULL 验收仍未运行。
- SPI 4 类验证最近 **60/60 PASS**，8 项 Checkstyle 错误已按原约束修复。
  重跑原 static-analysis 门禁又发现 allocation 路径正则 REDOS：已改成长度有界的
  ASCII component/character 校验，拒绝空段、dot segment 和非 ASCII 字符，新增边界回归。
  SPI 原 SpotBugs 已通过（零报告）；同一原门禁继续捕获协议层 3 项复杂度。
  已按 Native evidence、memory/exchange/invocation grant、fenced attempt 校验职责拆分，
  还修复 uint64 用量在 Java 中呈负数时绕过 budget 比较的缺陷。最近 Protocol
  **27/27 PASS**，包括全部 12 类 grant 边界/超额/不可表示值、CPU signed overflow、
  缺失/不可表示的 grant 与 malformed/foreign report。校验收敛为唯一
  `CanonicalTaskRequests.usageWithinGrant`，普通/Artifact Coordinator report 共用；
  两条接收路径新增 13 类 unsigned overflow 回归均已通过。
  原门禁曾因 observability 36 项报告 FAIL：实际修复队列已接收 batch 的容量保留与
  显式拒绝/关闭竞态、指标 LONG typed zero、数组/列表拷贝与换行。最终最近
  **111/111 PASS**（真实 PostgreSQL，无跳过）；共享 counter/bean/no-op 等仅在身份与
  不可变测试证据下作 exact class/method/field/pattern 误报匹配，不扩大排除或调整阈值。
  原 static-analysis 已越过 Observability，随后 TestCommon 7 项和 Security 3 项报告 FAIL。
  TestCommon 保留共享 network/borrowed container/确定性 seed 语义并修复 schema 防御拷贝，
  最近合计 **28/28 PASS**。Security 删除冗余 issuer 分支、冻结排序后的 model 快照、
  在 trigger DDL 前强制固定 authority table allowlist；getter/固定内部 SQL 仅作精确误报匹配，
  最近 **69/69 PASS**（包含真实 PostgreSQL role/grant/tenant）。
  再复查发现 flush 在 JDBC I/O 期间持锁，已改为 atomic single-owner completion，
  shutdown 等待真实 in-flight 并 drain 全部 batch；真实 PostgreSQL 新测试验证无 monitor、
  并发 flush 不等待 JDBC、201 个 queued/in-flight 的容量及关闭后逐条落库，
  并将 auto-flush 的固定 sleep/部分条数断言改为有界实际落库观察及全部 batch 断言。
  本次 Observability **112/112 PASS**，Security 与其合计 **181/181 PASS**，日志
  `/tmp/pangrova-production-security-audit-no-lock-nearest-tests.log`。
  最终静态门禁和全仓覆盖率仍须在同一源码窗口重跑。
  原 static-analysis r7 已越过 Security，Planner 因 20 项 Checkstyle 报告 FAIL。
  已拆出 per-conversion `AiInvocationBinder`，将 window eligibility 收敛已有 window converter，
  按原规范补 graph encoding 语句/Javadoc；重复 AI invocation 精确 model/唯一 ID 新回归已通过，
  最终原门禁尚待重跑，不通过调整 FileLength/复杂度或排除生产源代码解围。
  异步 query audit 仍不是持久 ledger，process crash 与 ambiguous commit 不能记闭环。
- 持久作业 runtime 正在接入同一普通 DAG/Leaf 与 typed job control；原 terminal report
  字节和 producer attempt 保留，当前 takeover/read/finalize 权限单独校验。
  完成 signal 与 FINISHED 状态分离，迟到失败不得绕过 commit 提前释放等待者；
  新源码 checkpoint 的 Coordinator 及其 29-module 上游 compile/test-compile
  **PASS（52.977s）**；这不是最终 Step 5，也不代表执行测试。并发、真实 PostgreSQL
  和共享 runtime 最近合计 **220 项，219 PASS / 1 FAIL / 0 SKIPPED**：
  Observability 111 与 Metadata 14 全通过，Coordinator 94/95；唯一失败是 Feature
  全局 aggregate 旧拒绝断言（新路径已接受，但实际分布式结果证据仍待补）。
  此窗口发现 recovered winner 空 DispatchResult 与 FINISHED/HTTP ACK 清理竞态，
  已修复为 winner replay 不读取 Worker result、ACK 唯一登记先于 future 发布、
  HTTP 失败/取消/deadline 显式释放 tracker；删除旧 DAGContext forwarding constructor，
  使用唯一 canonical 14 参数。DAG/Shuffle 职责拆分保持 500 行约束，未调整阈值。
  新最近测试与完整 E2E 尚未闭环。旧失败日志
  `/tmp/pangrova-production-durable-runtime-observability-checkpoint-tests.log`。
  再复查修复 pollOnce 的持 monitor PG/SDK I/O，新增 atomic owner/shutdown barrier；
  external shuffle 的 register/finalize/recovery RPC 也从持 stripe monitor 改成 exact-execution
  completion queue，实际 I/O 在 admission monitor 外执行，消除不同 execution hash collision 的串行阻塞。
  以上新增控制/并发测试不冒充独立 Worker E2E；最近编译曾暴露拆分后的 nested definition import 遗漏，
  已补单一实际类型 import，正在重跑完整最近集合。

当前 Linux 生产镜像、独立多 Worker 故障矩阵与实际 changed-line coverage≥60% 仍无 PASS。
新 production CI 接线不等于已执行该镜像或完成规模化生产部署验收。

### 0.13 2026-10-01 终态输出、拓扑顺序与规划对象所有权复验

以下结果仅更新对应源码窗口与测试范围，不能与历史窗口拼接为完整发布验收。

- Native 合法 VALUES 取消测试现实际包含一条 INT64 输入，成功/取消不能用 FAILED
  代替；完成后取消要求原状态为 FINISHED。新增损坏计划的阻塞消费者证明，FAILED
  可见时输出已经关闭。最近 42/42 PASS，两个竞态用例各重复 100 次 PASS（34.44s）。
  从 Step 0 重跑的 Rust FFI、显式 JDK17 JNI Release configure/build 均 PASS；最新
  Step 2 全量 **3500/3500 PASS（496.18s）**。同等插桩 ASan configure/build PASS，
  Step 3 全量 **3500/3500 PASS（2631.14s）**；Step 4 打包 PASS，Lance FFI 符号
  已实际验证。本窗口 Step 5 全仓 39-module install r3 PASS（51.016s）；Step 6–8
  尚无最终 PASS。日志前缀
  `/tmp/pangrova-production-terminal-output-final-`。
- Feature export 的 stage ID 按根向叶编号，不能把数字排序当依赖排序。原创建持久页时
  下游 page ordinal 小于上游，真实 PostgreSQL 的依赖校验拒绝。现使用确定性拓扑排序
  分配 job-global ordinal，保留 stage identity，不放宽 JDBC 校验。SQL/Paimon/PG 测试
  查询实际存储的 dependency，断言所有上游 ordinal 严格小于下游；terminal claim
  使用实际持久 partition 的 ordinal，不硬编码为 0。最近 **257/257 PASS、0 skip**：
  Planner 110、Metadata 14、Coordinator 133，耗时 106s。日志
  `/tmp/pangrova-production-durable-planner-final-nearest-tests-r5.log`。
  包含普通/Durable ACK、winner replay、deadline、真实 PG 终态、Feature SQL 与 shuffle
  lifecycle 测试；并非独立 Worker 的 REMOTE shuffle E2E。
- 原 static-analysis r8 **FAIL（170 项）**，修复真实数组/集合暴露、重复 nullable getter、
  window NULL/negative bound、冗余 sentinel 比较及匿名类继承方法歧义后，r10
  **FAIL（138 项）**；37 项 Planner 与 7 项 capability 测试 PASS 不代表静态门禁通过。
  新增向量查询/metadata copy、conversion hint/marker 与四种 join config 所有权证明。
  join 的 caller-owned correlation sets 真实冻结；Calcite cluster/traits/children 只保留
  正确的 planner-owned identity，以 exact class/method/field/pattern 匹配已复证误报，
  不扩大到变量集合、数组、整个 planner package 或其他规则。
- CTE 规则的全局 digest 修复后，进一步追踪发现临时扫描没有 Native converter 和真实
  producer。现删除四个未接通的物化实现及生产注册，保留可执行的普通 CTE 展开；
  `CteCompilationContractTest` 从真实 SQL 编译验证重复 CTE 两个真实源均进入 canonical
  Substrait，后续单次查询仍只有自己的源。此证据不是 CTE materialization 或 Native E2E。
  LIMIT 回归断言真实 connector limit 与 Sort，不依赖 global fired。
  `PlanTable` 对未知/缺失 compiled table 报 PLANNER_VALIDATION_FAILURE，不读 catalog 补证。
- CBO 的统计没有绑定精确源快照，不能消除关系；原整表剪枝还把不等式当成 equality
  范围判断。删除这条不安全规则和全局 fired 状态。新的真实 SQL 编译回归覆盖等式、
  不等式、反向比较和 nullable 列，9/9 PASS；Connector 的数据级剪枝保留。
- 未接线的 `PlanCache` 及未注册 listener 已删除；`RelNode.copy()` 不脱离 cluster/children，
  原“deep copy / thread-safe”声明错误。已接线的结果缓存没有删除。三种规划包装明确为
  query-owned / NotThreadSafe，测试证明不同查询 cluster 独立、同查询内部借用同一 tree；
  不把 Calcite 树当跨查询 immutable DTO。
- static-analysis r14 FAIL（108 项测试中 1 个动态表 fixture 缺定义），r15 测试
  **108/108 PASS、0 skip**，静态检查仍 FAIL（24 项）。继续修复重复 nullable getter、
  静态集合暴露和上述不安全路径，增加 live statistics、query tree、immutable Bloom 的
  实际所有权证明；exact class/method/field 误报范围不覆盖数组或 DTO 可变集合。
  最新 r16 **168/168 PASS、0 skip，Checkstyle 0、SpotBugs 0**，13 项 reactor 均成功，
  耗时 90s；不是全仓静态分析或生产分布式证明。日志
  `/tmp/pangrova-production-pre-policy-generated-planner-static-analysis-r16.log`。
- Step 5 `mvn clean -DskipTests install` r1 FAIL：E2E fixture 仍引用重构前的
  `metadata.artifact.jdbc.JdbcArtifactBuildStore`。已迁移至真实 `.jdbc.build` 实现，
  全仓 clean install r2 PASS（61s）。Step 6 r1 FAIL：QueryBootstrap 达到 501 行，
  超过 500 行门禁；DAG engine 直接实现既有 StageCompletionCallback，删除重复转发匿名类。
  最近质量/Coordinator 回归 30/30 PASS、0 skip，随后 Step 5 r3 全仓 PASS。
- Step 6 r2 FAIL（6m33s）：Metadata 出现共享 schema 污染、未迁移的 invalidation_log、
  旧 Tabular Export 角色断言。Quality/CDC store 每个测试独立 schema；stream state
  property 改用完整生产迁移，不再删除共享表或另造 schema；cleanup/sync 的 poller
  夹具应用真实迁移。Tabular Export 断言 WORK_UNIT / RELATIONAL_PARTITION，与真实
  持久发布路径一致。`-Pfull-regression` 最近 52/52 PASS、0 skip（31.180s）；日常
  筛选的 34 项不冒充这次完整选择。日志 `/tmp/pangrova-production-metadata-full-failures-nearest-r2.log`。
- E2E 静态复核发现先前真实性门禁漏检：11 个 SQL 测试类捕获 AssertionError，将
  执行失败接受为“非语法错误”或直接吞掉。已移除这些成功测试的软失败分支；除零
  负向测试要求实际错误并复验后续查询；空间查询核对具体 ID/名称，Subquery 核对
  行值且禁止空集导致 vacuous pass。新门禁拒绝 caught assertions、parser-only
  error acceptance 和 collection-size tautology，门禁自身 7/7 PASS、0 skip。
  此修订没有让相应 SQL 功能通过 E2E；真实 fixture/执行失败必须继续修复，不能恢复
  宽松断言。日志 `/tmp/pangrova-production-e2e-truthfulness-nearest-r1.log`。
- Step 5 r4 全仓 PASS（47.169s）；Step 6 r3 在 Metadata **504/504 PASS、0 skip**
  后继续到 Worker，最终 FAIL（11m13s）：Worker 1290 项中 1 failure / 20 errors。
  原 bootstrap 夹具未注入必需 catalog resolver；writer 注册测试没有初始化 runtime；
  模拟任务使用空 bytes 而没有 canonical attempt；shutdown property 缺必需 shuffle
  spill 配额/目录。修订测试生命周期、契约与共享 fixture，保留生产 fail-closed 校验；
  shutdown property 实际循环提高至 5 次，不再声称 runtime 不能重启。模拟 executor
  只证明组件行为，不冒充 Native E2E。邻近 r1 编译 FAIL（遗漏测试 dependency/错误
  TaskKind），已修正为父 BOM 管理的 test-scope Awaitility 与 canonical READ kind。
  r2 **43/43 PASS**，最后 r3 扩展至 bootstrap/assembly 共 **65/65 PASS、0 skip
  （12.803s）**。注册 stub 补齐真实 DELETE 路由，避免不存在的端点触发重试；
  metrics/expiration 测试删除 Thread.sleep，使用明确条件验证。Step 5 r5 全仓 PASS
  （48.373s）；Step 6 r4 FAIL（3m47s），Security 350 项中 1 error：LocalStack 3.4
  内部 edge server 启动超时并退出，不是 KMS 断言失败。相同 fixture 的独立复测
  **8/8 PASS、0 skip**（14.11s），未提高超时、未绕过实际 KMS；全量运行仍须复证。
  日志 `/tmp/pangrova-production-worker-bootstrap-full-failures-nearest-r3.log`、
  `/tmp/pangrova-production-terminal-output-final-step6-r4.log`、
  `/tmp/pangrova-production-kms-localstack-nearest-r1.log`。
- TPC-H 复核发现执行失败被转为空集，空 JSON 参考又退化为列名检查甚至直接接受空集。
  已删除这两个假通过路径：同一批 SQL 输入分别写入真实 Pangrova 与独立 PostgreSQL
  schema，22 个查询对照参考引擎逐行验证数值、完整列集、顺序和重复行；空结果只有
  独立参考同样为空才接受。该数据是 schema-compatible deterministic fixture，
  不是官方 dbgen SF=1，也不是性能 benchmark。Oracle/比较器组件测试 **11/11
  PASS、0 skip**（10.682s）；此证据不代表 22 个 Native 查询通过。门禁扩展至
  所有 Java helper/base 文件并拒绝 RuntimeException→empty-result，门禁回归
  **9/9 PASS、0 skip**。Step 5 r6 全仓 PASS（46.464s）；Step 6 r5 FAIL（10m55s），
  Metadata **504/504 PASS**、Worker **1290/1290 PASS、0 skip**，停在 Iceberg
  396 项中 4 errors：两个原 Integration fixture 未携带必需 tenant_id。
  修正 fixture 身份与 PageSink try-with-resources，邻近 **5/5 PASS、0 skip**，
  未移除生产租户校验。TPC-H 组件扩展回归 r2 为 **28 项/7 errors**：E2E pom
  把 JVM tmpdir 指向不存在的 target/tmp，独立测试在基类初始化前无法创建 @TempDir。
  已删除这一自定义路径，使用 JDK 默认临时根目录；扩展回归 r3 **28/28 PASS、
  0 skip**（18.557s，含真实 Parquet fixture）。输入加载改为按表名
  排序后消费种子，消除 Map.copyOf 跨 JVM 遍历顺序造成的不确定性。
  日志 `/tmp/pangrova-production-tpch-reference-nearest-r1.log`、
  `/tmp/pangrova-production-e2e-truthfulness-nearest-r2.log`。
  另见 `/tmp/pangrova-production-terminal-output-final-step6-r5.log`、
  `/tmp/pangrova-production-iceberg-tenant-fixture-nearest-r1.log`、
  `/tmp/pangrova-production-tpch-reference-nearest-r2.log`。
  最新组件日志 `/tmp/pangrova-production-tpch-reference-nearest-r3.log`。全仓
  install r7 **PASS（46.396s）**，随后从 Iceberg 继续诊断剩余 reactor：Iceberg
  **396/396 PASS**，Paimon PASS，Coordinator 的正向 bootstrap fixture 缺完整
  鉴权/租户接线及 Native shuffle 资源配置，已见约 21 个 failure/error。诊断运行
  为修复这些失败而中断，退出码 **130，FAIL/未完成**；`-rf` 不是完整 Step 6。
- Coordinator 正向 fixture 改用真实签名 OIDC、隔离 PostgreSQL 权限后端、活跃
  tenant/membership 与明确配额；源无关 SELECT 的组件 fixture 配齐真实 RBAC、
  mask/filter，但没有任何表 grant，不用 NoOp 绕过授权。Native 生命周期 fixture
  显式传入 shuffle budget 与 JUnit 所有的临时目录，拒绝实际 catalog task；
  注册桩按真实 DELETE 路由确认注销。原鉴权缺失拒绝用例仍要求 **403**。
  扩展 nearest r1 为 **35 项/5 failures**（tenant 未提供 quota、持久化为零限制）；
  配置显式 quota 后 r2 **97 项/1 failure**（Profiler 请求漏认证头）；修复后 r3
  **99/99 PASS、0 skip（1m21s）**。它们是 HTTP/Native lifecycle 组件验证，
  不计 SQL 结果或多 Worker E2E。日志
  `/tmp/pangrova-production-coordinator-auth-fixture-nearest-r{1,2,3}.log`。
- 同时修复真实 HTTP 错误分类：tenant quota 拒绝从 **500** 改为 **429**，保留
  `TENANT-0001`/`USER_ERROR`，不虚构 Retry-After。真实签名身份 + PG tenant
  + HTTP `/v1/query/submit` 验证 **503（无 Worker）/429（配额耗尽）/401（无身份）**；
  异常 mapper、tenant guard 与 QueryManager fixture 合计 **17/17 PASS、0 skip
  （21.230s）**。中间 r1 为 **14 项/3 errors**：QueryManager 组件缺策略接线；
  配齐后 r2 **14/14 PASS**，没有弱化生产权限条件。日志
  `/tmp/pangrova-production-coordinator-quota-http-nearest-r{1,2,3}.log`。
  Step 5 r8 全部 39 模块 **PASS（55.783s）**；Step 6 r6 为修复新暴露的
  Coordinator fixture 问题而中断，退出码 **130，FAIL/未完成**。随后 Java 源码
  又有修订，这一 install 不是最终冻结源码证明，必须重新执行 Step 5–8。
- Coordinator 专项复测暴露真实终态审计缺陷：提交带身份，失败/完成事件 principal
  却为 null。现在在 QueryStateMachine 发布前捕获提交 principal，terminal emitter
  使用 query-owned principal 与 resolved tenant，不保留 token/session properties，
  不新增无界身份缓存；匿名拒绝明确记录 anonymous。三个 terminal caller 同步接线，
  覆盖多身份隔离、匿名拒绝、真实异步 emitter 和空参数。该修复**不证明** journal
  failover attribution 或早期 admission rejection 的完整终态审计，后者仍待修复。
  Source-free 组件使用完整真实 deny-all 策略，没有表 grant；Drain 并发测试移除
  sleep 与任意异常即通过，改为 latch/future 同步和精确 Worker admission 失败。
  PipelineIntegrationTest 明确为单 JVM HTTP-wiring-only 组件验证，不算 Native E2E；
  终态报告改用实际下发 attempt、task type 与 canonical coverage，拒绝缺失凭据后
  相同 sequence 的合法报告仍能被接受，未放宽生产校验。专项 r1 **25 项/1 failure**，
  r2 **36 项/6 failures**，r3 **编译 FAIL**（测试错误使用 AutoCloseable），r4
  **54 项/5 failures**（测试 OUTPUT task type 错写 COMPUTE），修复后 r5
  **54/54 PASS、0 skip（38.395s）**；新增缺凭据防 sequence poisoning 的 Pipeline
  复测 **11/11 PASS、0 skip（16.926s）**。日志
  `/tmp/pangrova-production-coordinator-source-free-policy-nearest-r{1,2,3,4,5}.log`、
  `/tmp/pangrova-production-coordinator-terminal-evidence-nearest-r1.log`。
  父 POM 固定实际已使用的 resources plugin 3.4.0 / jar plugin 3.5.0，消除子模块
  未声明版本的模型警告，没有升级依赖。全 Coordinator 无过滤诊断 r2 发现
  Bootstrap 测试在 async terminal cleanup 前立即断言活动数零的竞态；该次运行
  中断为 **130，FAIL/未完成**，不代替 Step 6。
- 同一诊断日志反复出现已关闭数据库的 invalidation polling：两种模块 owner 只停止
  cleanup timer，遗漏独立 JdbcInvalidationPoller scheduler。现在初始化前注册 owner，
  正常 shutdown/启动失败关闭并等待 poller executor，先于 JDBC pool release。
  新测试使用真实 migrated PostgreSQL DataSource，观察实际执行 JDBC 的专属线程。
  nearest r1 **43 项/1 failure**：JDK ThreadPoolExecutor 在 worker 最后栈帧退出前
  已 signal TERMINATED，立即 Thread.isAlive 断言竞态；已读本机 JDK17 src.zip
  `tryTerminate/processWorkerExit/runWorker` 复证，改为有界等待真实线程退出，保留
  相同最终断言，未延长生产超时。r2 **43/43 PASS、0 skip（33.961s）**。
  Step 5 r9 全部 39 模块 **PASS（50.176s）**；Step 6 r7 **FAIL（18:20）**，
  Coordinator 2846 项、24 failures、49 errors、0 skip，后续模块未运行。
  本次后续 Window/Parser、write recovery 和统计存储源码变化使 r9 不再是最终快照证明。
  日志 `/tmp/pangrova-production-coordinator-unfiltered-diagnostic-r2.log`、
  `/tmp/pangrova-production-coordinator-owned-poller-nearest-r{1,2}.log`、
  `/tmp/pangrova-production-terminal-output-final-step5-r9.log`、
  `/tmp/pangrova-production-terminal-output-final-step6-r7.log`。

当前仍未通过：最终源码的全仓 coverage verify、全部 E2E、
真实 ActivationSmoke、Linux 生产镜像、独立多 Worker/恢复/故障矩阵及 changed-line
coverage≥60%。Linux 新 context 已包含 terminal-output 修订：amd64 模拟构建 r3 在
第三方 Velox setup 的 `uv tool install` SIGSEGV（exit 139）；arm64 原生构建 r2 的
Ubuntu 依赖下载 DNS 失败（exit 100）。只读容器 DNS 复验已成功，arm64 同命令 r3
在 apt 完成后 pip 安装 cmake 3.28.3 失败（exit 1，No matching distribution）；
PyPI 元数据与 host-network 容器实测确认 arm64 wheel 存在，而默认 bridge 网络的
HTTPS 请求超时。apt/pip 拆成独立缓存层并要求固定版本 binary wheel，arm64 r4
完成 apt（1509.7s）与 cmake wheel 安装（691.2s），随后 rustup-init 下载遇到
HTTP/2 PROTOCOL_ERROR，**exit 1，FAIL**。修订为固定 Rustup 1.28.2 的官方
架构二进制、官方 SHA256 校验、有界 HTTP/1.1 下载，不再用 curl→shell 管道；
不安装无关 latest stable，Cargo 使用仓库中的 Rust toolchain pin。arm64 r5
已通过 Rustup bootstrap 与 protobuf/gRPC/Abseil 安装（1789.6s），但 uv wheel 下载
仅完成 9.7/19.8MB 后发生 `files.pythonhosted.org` ReadTimeout（1053.6s，exit 2），
该次构建 **FAIL**，无镜像 PASS。宿主机同一官方 wheel 的 HTTP/1.1 range 复验为
206、1MiB/31.093s、约33.7KB/s；外部下载存在实测带宽限制，不推定为 Native 编译缺陷。
依照 uv 官方 Docker 指引，已换为同版本 0.12.21 的固定 OCI index digest（同时含
amd64/arm64），不再通过 pip 流式下载安装 uv。r6/r7 版本检查失败是官方二进制
版本行包含架构后缀，r7 实测 `uv 0.12.21 (aarch64-unknown-linux-musl)` 后修正为
精确版本 token 检查，未忽略检查。r8 已通过实际 arm64 二进制、Velox clone 和 cmake
安装，但 GitHub fmt 11.2.0 下载 `curl` 连接失败，exit 7，**FAIL**；没有生产镜像 PASS。
[官方指引](https://docs.astral.sh/uv/guides/integration/docker/)。Git 使用 scoped
HTTP/1.1，不关闭 TLS、不改 VM 或用户容器。还需修复/复证
REMOTE shuffle 缺失配置的永久准入失败语义与 E2E fixture 的真实服务接线。

本轮新增根因复证与修复：

- Window 的 LogicalWindow/RexOver 共用精确 literal 转换，删除 default 0/1 与数值截断，
  保留 NTILE BIGINT、ROWS/RANGE、NULL 排序；public SQL property 首次失败证明
  `ExpressionToSqlNodeConverter.buildOverCall` 恒传 `isRows=false`，同时 AstPrinter 恒打印
  ROWS。两处根因已修。nearest r1 158 项/1 failure，原断言重跑 r2 **158/158 PASS、
  0 skip（16.857s）**。这仍是 SQL→规范 protobuf 证据，不是 Native SQL 行值验收。
- write recovery 的已确认 ABORTED 与普通 GC 共用 purge，但 purge 正确地只允许已确认
  deferred-work 结束的 COMMITTED，导致已确认 abort 不能被移除。新增 exact fingerprint
  原删除式 abort API 仅 fenced reconciliation 调用；任务 FAILED 不构成外部 catalog
  abort 证明，保留 PENDING redo。该历史删除方案在0.21复证为会丢失重试屏障，不能保留。
- `WritePolicyObservationTest` 原 AbstractTable 无 connector declaration，现用真实 PG
  directory/权限及 Iceberg SDK 表/ServiceLoader schema；nearest r1 该类 **9/9 PASS**。
  同次 Coordinator 64 项/1 failure（streaming fixture 未显式 requireComplete=false），
  不将该命令标 PASS。SPI lifecycle 在注册 query 前拒绝缺 trusted tenant/principal 的会话。
- 索引后台 resolver 未持久授权，采集鉴权异常被 best-effort 吞掉；真实两端统计读取 latest
  而非事件 snapshot；Paimon event basename 与采集完整路径不同；统计 SQL 遗漏无默认值的
  NOT NULL created_at。最后一项已补服务器 CURRENT_TIMESTAMP 和真实 PG 四项测试，
  nearest r3 **4/4 PASS、0 skip**（包括失败批次原子回滚，2.590s）。当前唯一键仍缺失，
  重复插入的真实测试明确暴露该缺口，不把它说成幂等通过。其余必须按 purpose-bound 维护授权、精确 snapshot/files、租约下逐工作项
  disposition 原子接通，不能造 maintenance 主体或补假的 forSession 来把4项tests变绿。
- source-free routing fixture 缺完整 deny-all policy 已补。最近 r2 移除 autoFinish 后，
  HTTP-wiring-only Worker 不产生终态，线程栈停在 DAGCompletionEvaluator.awaitCompletion；
  该次由本任务停止，**BLOCKED/未通过**，不加 timeout 或假 FINISHED。需真实执行器与报告
  接线后重跑。进一步确认 Native OUTPUT 必须由真实 Flight 消费至 EOF 才终结，
  同步等待 dispatch 返回再消费形成等待环；真实 fixture 与消费者正在实施，不改生产 DAG。
  安全 DML 三类 tests 已迁移真 PG authority/SDK SQL；nearest r3 **20/20 PASS、0 skip**。
- nearest r3 的 Coordinator **84/84 PASS、0 skip**，同命令 Parser **39/39 PASS** 与
  Metadata **22/22 PASS**（真实 PG）。范围包含 write recovery/journal、broadcast 显式
  completeness、当前安全策略和 Operations signed OIDC；总耗时 96s。此选择没有执行上述
  七类未完成的 routing fixture，不能替代先前挂起命令或 Step 6 全仓验证。
- `QueryDispatcherCleanupOnFailureTest` 删除 fake connector/store，改为真实 migrated PG、
  ServiceLoader Iceberg SDK、Parquet 文件与 snapshot，精确验证 query-scoped pins 清理及
  未注册/其他 query 的资源保留；nearest r1 **3/3 PASS、0 skip（36.588s）**。
  这是失败路径生命周期组件测试，不是 Native SELECT E2E。
- Linux r8 网络根因复证：同一 GitHub URL 在 host 成功，Docker 同 DNS/IP 直连超时；
  经既有用户代理的临时只读容器下载完整 fmt **200、705327 bytes、TLS verify=0**。
  r9 仅在原 Bake 命令传标准 HTTP_PROXY/HTTPS_PROXY build arguments，保留 TLS 校验，
  不修改 Dockerfile 网络配置或用户 VM；构建正在进行，尚无 image PASS。
- 真实 source-free Native fixture 原第一次 test-compile FAIL 是 Flight client/stream
  关闭需要 checked exception，现传播原 cause 并恢复线程中断。原同命令 r2
  **7/7 PASS、0 skip（40.858s）**：关闭重建两次 SELECT42 的实际行值、独占 runtime
  拒绝借用，以及 UnifiedPath 五项真实执行。canonical terminal 明确为 NATIVE origin，
  实际消费者读到 EOF，receiver/validator/tracker/DAG 接线不再制造成功字段。
  此证据是单 JVM 的 JNI/HTTP/Flight 小闭环，不代表两进程分布式或 provider E2E；
  其余 routing fixture 尚在迁移，Step 6 原失败范围尚未全通过。
- DECIMAL 复证公开 SQL `DECIMAL(38,18)` 静默变 `(19,18)`，非法 `(39,18)`、
  `(10,11)` 被接受。已统一生产严格 factory、38 位边界、精确 Literal/TypeMapper；
  multiply/divide/mod 类型推导对齐当前实际 Native registry 并拒绝不可表示 scale/rescale。
  nearest r1 **103 项/2 failure/18 errors**，实际发现省略 scale 的 SqlBasicTypeNameSpec
  错误参数、测试读取已替换的 VirtualTable.values，以及合法省略 scale 哨兵被误当非法。
  修正前端参数和真实协议读取（保留全部 schema/value 断言及额外行/列/tag 校验）后，
  原同命令 r2 **103/103 PASS、0 skip（14.704s）**；Native 隐式 output-driven cast 仍改变 short/long
  decimal 参数，普通 array 又被 Java 全局降成 FP64，需完整前后端语义单元修复，
  不能把修正 planner 上限说成最终值已经正确。
- Linux r9 依赖编译中 **FAIL/EOF**，不是编译 PASS。诊断确认 VM 内 daemon
  仍 started、原 PID 存活、原 Unix API `_ping=OK`，没有观测到 OOM/daemon 重启；
  主机 Lima SSH control 转发丢失，Docker socket 为无监听者的 stale socket。
  仅重建 guestagent/原 Docker socket forwarding，stale socket 移到临时目录保留，
  复验原 host Docker `_ping=OK`、server29.5.3。未重启 VM/daemon/container、未修改资源。
  新 Native 转换单元将改变构建源码；待其冻结后再重跑，不重复构建旧 checkpoint。

相关日志：`/tmp/pangrova-production-planner-window-exact-nearest-r{1,2}.log`、
`/tmp/pangrova-production-coordinator-authority-recovery-nearest-r{1,2}.log`、
`/tmp/pangrova-production-coordinator-authority-recovery-nearest-r2-thread-dump.log`、
`/tmp/pangrova-production-coordinator-current-policy-pg-nearest-r3.log`、
`/tmp/pangrova-production-coordinator-cleanup-real-pg-nearest-r1.log`、
`/tmp/pangrova-production-source-free-real-native-nearest-r{1,2}.log`、
`/tmp/pangrova-production-decimal-canonical-nearest-r{1,2}.log`、
`/tmp/pangrova-production-linux-arm64-native-final-build-r{6,7,8,9}.log`。

### 0.14 生产准入目标与未实测边界

用户要求按生产环境标准推进。以下是本项目待验证的验收规格，不是已测性能、已部署
资源或行业保证；实际生产集群尚未提供，本轮不创建付费基础设施或改用户 VM。

| 准入维度 | 必须取得的证据 |
|---|---|
| 发布构建 | 同一冻结源码的 Rust/Release/ASan/package/39-module full regression/全部 E2E/ActivationSmoke；真实 changed-line coverage≥60%，不能拼接旧窗口或日常筛选结果。 |
| 控制/数据分离 | Coordinator 只传计划、授权、租约和提交元数据；检索、融合、排序、扫描和导出字节由 Native Worker 执行。历史 streaming/dynamic-table 路径仍需逐项清理与复证。 |
| 正确性 | 完整分片覆盖默认 fail-closed；NULL/维度/模型/快照/索引 freshness 明确；近似搜索公开 recall 与 fallback 状态；重试、过期 owner、重复 ACK 不可导致错误结果或重复发布。 |
| 部署验收基线 | 建议 3 个独立 Coordinator、3 个独立持久 Shuffle 服务及 16 个 Worker（每台 16 vCPU/64 GiB），隔离的 PostgreSQL/Object Store/模型服务；不同 Worker 必须有独立进程和端点。先做 2/4/8/16 Worker 伸缩阶梯。此规格尚未部署。 |
| 数据与负载 | 建议覆盖 1 亿条 384 维向量、真实 NULL/更新/删除/冷热分片及 TB 级关系数据；128 并发检索与持续 ingestion/backfill 并行，24 小时 soak。模型服务负载单独计量，不把 provider 时间算作引擎开销。 |
| 性能验收 | 暂定 ANN recall@10≥0.95，128 并发下稳态 p95≤500ms/p99≤1s（不含模型生成）；同数据、资源配额和 recall，4→16 Worker 吞吐至少达到 3 倍。精确全扫与低选择率混合查询另设基线，不宣称满足此 ANN SLO。 |
| 恢复与有界资源 | kill/restart Worker、Coordinator、Shuffle、PG 断连、对象存储超时、网络分区、过期租约/owner fencing 矩阵；任务在配置 deadline/lease/retry budget 内收敛，持久作业重启恢复，无重复提交，资源与队列不随请求数无限增长。 |
| 可运维与安全 | 身份/租户/权限 fail-closed，内核执行资源拒绝和可审计错误，不含LLM usage/计费；队列/内存/spill/覆盖率/freshness/重试/租约可观测；滚动重启、备份恢复、秘钥轮换实际演练。 |

本机 2 CPU / 约 3.8 GiB Docker VM 只能支持局部构建与小规模正确性验证，不能证明上述
吞吐、尾延迟、24 小时稳定性或大规模扩展优势。任何 FAIL/SKIPPED/BLOCKED 都阻止
“全部贯通/生产级”结论；不保证未来不存在任何架构问题。

### 0.15 2026-10-02 canonical 类型、真实取消与重新冻结验收

当前 Native 源码已经变化，0.13 的 3500 项 Release/ASan、package 和 Java install
仅保留为历史记录，不能计入本窗口。没有完整回归、独立进程 E2E 或生产规模 PASS。

- Native scalar/list/VirtualTable 统一 literal 解码；DECIMAL 严格检查 16-byte LE、
  p/s 和值域，普通 typed NULL/nested array 不替换为零。删除 output-driven cast 与
  比较参数猜测，经实际 Velox registry 精确绑定并校验输出，保留 plan MemoryPool
  ownership。新增真实 Native Task 值/类型 oracle。Java 普通 ARRAY 同步保留 declared
  component type，distance 仅在自身签名处写显式 REAL/DOUBLE CAST；CASE/scalar
  保留 Rex 类型，SUBSTRING/ROUND 声明对齐实际签名。这些新增 Java 专项尚待运行。
- 当前 Step0 Rust PASS；Step1 r1 **FAIL** 是固定 Velox API 的实际 namespace 不含
  `functions`；r2 编译退出 0 但存在一项 deprecated protobuf API warning，按零警告
  门禁仍为 **FAIL**。改为公开 protobuf reflection 构造原拒绝输入，不屏蔽警告、不改
  生成源、不删除断言；原同命令 r3 configure/build **PASS、0 warning/0 error**。
  Step2 全量发现 **3532 项**，结果 **FAIL：3531 PASS/1 FAIL（517.21s）**：
  空 schema 的坏输入 helper 未实际编码 schema，Native 又把缺失 schema 默认成 BIGINT，
  从而报错类别不符合严格契约。已定位完整 caller 迁移范围，不能只改错误字符串。
  当前 expressions-only VirtualTable、禁止默认 schema 与死 Java reader 清理正在实施；
  完成后从 Step0 重跑，不沿用此次 PASS 子集。ASan/package/Step5–8 尚未完成。
- 真实 source-free fixture 原 r3 **45 项/1 failure/19 errors/0 skip（43.512s）**。
  首因是调度测试改变未参与 `computeScore` 的 performanceScore，改为真实 CPU 输入。
  关闭失败又确认两处生产问题：HTTP DELETE/dynamic-filter 的 canonical task-ID 没有
  解析到内部 task key；DAG 清理只删路由元数据，不终结仍在等待的 completion future。
  现 HTTP 控制入口只接受 canonical ID，并显式映射；live dispatch 在调度前注册
  completion owner，清理异常终结并停止继续调度，完成结果先于移除 DAG 发布；等待
  支持中断/保留中断标志，超时异常终结 owned future。不制造 FINISHED。
- fixture 实际 cancel/drain、graceful report executor 排空与异常累积关闭，失败也必须
  关闭 owned Native。补完整 canonical CANCELLED 报告与 dispatch 排空，以及未消费
  输出关闭后再次 SELECT42 的真实验证。最近同原八类范围加新增控制/lifecycle 验证的
  r4 **FAIL：56 项/1 error/0 skip**，唯一遗留用例仍同步等待未消费 OUTPUT；修复为
  实际 Flight 消费并保留原无 CoordinatorResultService 契约与断言后，同原十一类命令
  r5 **57/57 PASS、0 skip（51.743s）**：Worker3、Coordinator54，含新增并发 property
  的30次注册/清理竞态及真实 canonical CANCELLED/关闭重建。它仍加载旧已打包 Native，
  因此仅验证调度/取消/fixture 链，不能证明本窗口新 Native 类型；打包后必须同命令复跑。
- 另确认 Coordinator 的 scalar index 构建仍直接调用 `ColumnValueReader.readColumns`
  和 Java provider.build；完整迁移规格已记入根因账本，尚未实现，不能宣称控制面纯净。
  函数 capability 的真实 inventory 上报与 Planner 注入也未接通，当前静态 defaults
  不能作为实际 Native 支持证据；不得仅补函数名把声明冒充实现。
- Java 类型原十五类 r1 **FAIL：186 项/1 failure/3 errors/0 skip（17.717s）**；
  ANN 同名列场景的 SQL 被 Calcite 内联成两个常量，原测试并没有实际 column ref。
  改为真实两行 UNION ALL source 后保留原索引 lookup 与不兼容 cast 拒绝 oracle，
  原同命令 r2 **FAIL：186 项/0 failure/3 errors/0 skip（15.554s）**，剩余三项均是
  ROUND 真实生产准入。Worker inventory 整体握手已接通，精确断点是 Native 不填
  SQL function inventory、PlannerWiring 仍注入 defaults、validator 不校验完整签名和
  aggregate measure。下一完整契约单元调研设计中；未修改名单或绕过 validator 放绿。
- Linux Bake r10 使用同一恢复后的 Docker socket、原官方下载地址/标准 build proxy
  参数执行当前 Native 源码，正在构建。未更改用户 VM 资源、关闭 TLS 或重启容器；
  尚无 Linux image、JNI classifier 或部署验收 PASS。

日志：`/tmp/pangrova-production-canonical-literals-final-step0-r1.log`、
`/tmp/pangrova-production-canonical-literals-final-step1-r{1,2,3}.log`、
`/tmp/pangrova-production-canonical-literals-final-step2-r1.log`、
`/tmp/pangrova-production-source-free-real-fixture-old-native-nearest-r{3,4,5}.log`、
`/tmp/pangrova-production-expression-signature-canonical-nearest-r{1,2}.log`、
`/tmp/pangrova-production-linux-arm64-native-final-build-r10.log`。

### 0.16 2026-10-02 ABI 8 / canonical values 检查点与后续原子迁移

本节取代 0.15 的进行中状态；仅代表列出的源码检查点，不代表生产验收完成。

- scalar/list/VirtualTable 已共用严格 literal decoder；VirtualTable 只接受 expressions，
  Read 必须显式提供 base schema，包括合法的零列 schema。缺失 schema 不再猜 BIGINT。
  所有真实 caller 和 fixtures 同步迁移。删除无生产 caller 的 Java plan deserializer，
  测试直接核验唯一 generated protobuf，保留原 round-trip oracle。
- Lance 删除私有字符串 filter 的 C++/C ABI/Rust 入口，双端 ABI 为 8。原 reader/commit
  断言保留；四个 filter 场景改用真实 writer/commit、canonical FilterRel、生产 connector
  和 Velox Task。它们是 Native execution 证据，不是 public SQL E2E。
- Step0 Rust **PASS：0 error/0 warning，2m13s**；Step1 Release r1 **PASS：0 error/0
  warning**。全量 Step2 r1 **FAIL：3530 PASS/4 FAIL/3534（559.12s）**，仅四个新
  consumer 失败。根因是 Folly 默认 ContinueFuture 已就绪，Task 终止时不会替换这个
  out-parameter；consumer 因而再次拉取已结束 Task。改为显式 makeEmpty，不捕获吞错、
  不改取消/成功语义、不删断言。Step1 r2 **PASS：0 error/0 warning**；原全量命令
  Step2 r2 **PASS：3534/3534（548.06s）**。
- Java serializer/round-trip 最近验证 **PASS：16/16、0 skip（17.409s）**。
  doc class refs **PASS：174 references**；E2E truthfulness 和 dead-stub gates **PASS**。
  后两者只是静态门禁，不证明人工已发现的双路径和未接通能力全部清除。
- 本检查点尚未完成同源 ASan/package/Java full regression/E2E/smoke。后续函数目录、
  关系布局与 typed Arrow、Paimon physical write 已解除源码冻结；其变化将重新触发完整
  Step0–8，不能借本检查点的 3534 PASS 宣称新代码通过。
- 完整实施规格：[函数契约](ai-native-function-contract-implementation.md)、
  [关系布局与 Arrow](ai-native-canonical-layout-and-arrow-execution.md)、
  [Paimon 物理写入](ai-native-paimon-native-physical-write-implementation.md)、
  [Embedding purpose](ai-native-embedding-purpose-implementation.md)。这些规格不是实现证据。
  当前仍有 non-BOOLEAN filter fail-open、emit/layout、opaque shuffle page、Paimon JVM
  row write、Native function admission 和 LLM usage 统计残留等未闭合项。

命令与日志：

```bash
./pangrova-worker-native/bin/build-lance-ffi.sh
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
ctest --test-dir pangrova-worker-native/build --output-on-failure
mvn -pl pangrova-planner -am \
  -Dtest=SubstraitPlanSerializerTest,SubstraitRoundTripTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Native 日志前缀 `/tmp/pangrova-production-canonical-values-lance-abi8-`：
`step0-r1.log`、`step1-r1.log`、`step2-r1.log`、`step1-r2.log`、`step2-r2.log`。
Java 日志 `/tmp/pangrova-production-canonical-protocol-reader-removal-nearest-r1.log`。
Linux Bake r10 仍在真实依赖构建中；其 source checkpoint 早于本节，不计当前源码验收。

### 0.17 2026-10-02 真实函数注册、关系布局与类型边界近端验证

本节只记录当前最近验证；不将仍在原子迁移的执行入口写为已贯通。

- 真实 Native function exporter 已 Release 编译 **PASS，0 warning/error**，直接
  使用生产同一 registration routine，不创建假的 Worker AI callbacks。实际归一化得到
  **877 identities：SCALAR 470 / AGGREGATE 320 / WINDOW 87，另 9 special forms**。
  canonical type descriptor projection 为 30 messages。注册数量不是 SQL 可用/值正确证据。
  ROUND(decimal(12,3))、ROUND(decimal(12,3),integer)、SUBSTRING(varchar,bigint,bigint)
  已实际绑定输出类型；尚未等价于 SQL value oracle 或 runtime cohort admission。
  原合法 SQL 映射没有删减；ARRAY_CONTAINS_ALL、五项 spatial、aggregate/window SUM0
  的实际注册缺口仍需补齐并验收，不能由静态目录代替实现。
- Canonical relation layout 原同范围最近验证 r1 **FAIL，24 PASS/1 FAIL**：单输入
  schema 被无条件重建，顶层 struct nullability 从 UNSPECIFIED 改成 REQUIRED。
  保留单输入真实 schema 及 root metadata，新增 variation/nullability oracle 后 r2
  **25/25 PASS，0 skip，12.192s**。只是 Java schema/绑定证据，Native emit/mask/
  alias/typed Arrow/UNNEST 的原子迁移仍未验收。
- Paimon 完整 declared schema、SDK type mapping 和持久 handle 最近 r1 **FAIL，
  8 PASS/1 ERROR**，真实 SDK 拒绝有桶 append 表缺少 bucket-key。明确 `bucket-key=id`，
  保留 bucket=1 和原类型/持久化断言后 r2 **9/9 PASS，0 skip，13.142s**。
  Rust physical schema adapter 用真实 Arrow 数组复证 sliced DOUBLE、NULL parent/
  element、嵌套完整字段、非法维度/精度拒绝；零列 N 行物理写显式拒绝而非静默丢行。
  最新 r2 **4/4 PASS，0 ignored，编译 0 warning/error（2m16s）**。
  allocation、Native FileWriter、typed receipts、SDK publication/recovery 和双向 SQL/MinIO
  尚未贯通；Rust writer cache budget 不能当作完整外部分配峰值硬计账。
- Embedding typed batch/purpose SPI 最近 **7/7 PASS，0 skip（5.045s）**。
  Worker reactor compile r1 **FAIL**于唯一旧函数字段 caller，迁移完整 typed inventory 后
  r2 **PASS（12.203s）**。新纯 codec、真实 Ollama/Toxiproxy 和实际 Worker function
  integration 测试仍待测试编译与执行；旧 service/Governed fake fixtures 尚待替换。
  Public SQL 默认归一化、QUERY consumer 和最新 JNI artifact 尚未完成验收。
- Linux ARM64 Bake r10 从 Boost 推进到 Fizz，实际 cc1plus 仍在运行且累计 CPU 增长。
  这不是构建完成证据；该 source checkpoint 早于以上更改，不计当前部署验收。

命令与日志：

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --target pangrova_function_export --parallel
mvn -pl pangrova-protocol -am \
  -Dtest=SubstraitRelationLayoutTest,SubstraitRelationLayoutPropertyTest,RetrievalTopKPlanBinderTest,GeneratedValuesOverlayPlanBinderTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -Pfull-regression -pl pangrova-connector/pangrova-connector-paimon -am \
  -Dtest=PaimonSchemaBuilderTest,PaimonSdkVectorTypeTest,PaimonHandleSchemaIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
cd pangrova-worker-native/lance-ffi
cargo test --locked --release --target aarch64-apple-darwin physical_schema::tests
```

最近日志 `/tmp/pangrova-production-function-export-nearest-r1.log`、
`/tmp/pangrova-production-canonical-relation-layout-protocol-nearest-r{1,2}.log`、
`/tmp/pangrova-production-paimon-canonical-schema-handle-nearest-r{1,2}.log`、
`/tmp/pangrova-production-lance-physical-schema-nearest-r{1,2}.log`。
实际函数注册/descriptor JSON 为 `/tmp/pangrova-production-registered-functions-actual-r1.json`、
`/tmp/pangrova-production-type-descriptor-actual-r1.json`，不将机器输出提交为手写生成源码。
完整同源 Step0–8 仍未完成，分布式多进程故障、数据规模和生产 SLO 尚无实测 PASS。

### 0.18 2026-10-02 真实治理测试与资源契约迁移检查点

以下 PASS 都是指定源码检查点的近端证据，不替代当前完整构建或 E2E。

- Public Worker 13 个 AI + 2 个 FILE callback 的 metadata/signatures 已消费唯一
  binding declaration。protocol declaration/formula 最近 r2 **11/11 PASS，0 skip，
  11.270s**，Worker 16-module reactor compile **PASS，11.666s**。后续新增同源 hidden
  layout 的测试也实际执行，不能以声明存在推导 runtime cohort admission 已完成。
- 删除两个治理测试中的假 inference/embedding/rerank business Provider 实现。
  调用前检查改用真实 ConfiguredAiProviderRegistry/生产 admission 和校验器；成功、
  顺序、purpose、尾批、传输故障和预算拒绝由真实 Ollama/Toxiproxy integration 承担。
  Worker 最近 r2 **FAIL，testCompile 12 errors，17.934s**，旧 service fixtures 仍调用
  已移除 API；替换后 r3 完整 testCompile 及指定七类 **63/63 PASS，0 skip，18.157s**。
  其中 service 18、Governed 4、authorizer 5、budget registry 1、provider codec 24、
  vector validator 3、function 8。真实 HTTP integration 尚未执行；不将这些 PASS
  写为 Provider、SQL/JNI、分布式或生产验收。
- Rust physical schema r3 **6/6 PASS，0 ignored，23 filtered，0 warning/error，2m22s**。
  真实首 offset/全部 stride/child type 完全吻合的 sliced FLOAT64 payload 共享原 buffer；
  NULL parent 缺 slot 等不能共享的布局仍 concat，并以实际输出 buffers 计拷贝字节。
  不等于完整 Rust allocator 峰值、Native writer 或原子 publication 已验收。
- Paimon physical/allocation r1 **FAIL，protocol 9 PASS，Paimon 9 PASS/1 ERROR，
  17.904s**：真实 SDK 普通 Lance 格式明确拒绝 MAP。保留原 bucket=1 Lance 合法完整
  schema，并新增真实 SDK mixed Parquet + dedicated Lance 形状；不删除 MAP/VECTOR
  oracle。r2 **FAIL，protocol 11 PASS，Paimon testCompile 2 errors，14.234s**：
  TIMESTAMP_LTZ/tableExists 不是 pinned SDK API。后续按实际 SDK 类型/不存在异常
  修正；该版本尚无 mixed writer/publication/MinIO SQL PASS。
- UNNEST/layout r1 **FAIL，protocol 39/39 PASS，Planner 3 PASS/1 ERROR，0 skip，
  19.219s**。实际 ARRAY<ROW> runtime constructor 在 ExpressionConverter 被拒绝，
  因此不能由其余三例证明完整 UNNEST 已贯通。保留原 computed/ROW/MAP 输入；
  Native Cartesian、post-join filter、emit/mask 和 typed Arrow 尚需实际值/故障验收。
- 当前下一原子单元已开始，整体 Java/Native **尚不可视为可编译检查点**：唯一 v2
  task/capability/operator resource contract 删除累计 token/旧 invocation 字段，明确
  logical items、physical attempts、encoded HTTP request/response body bytes；失败重试
  消耗物理尝试和字节，不依赖 LLM usage 退款。模型 request token 限制与任务预算分离。
  JNI batch callback 收敛唯一 ExecutionCallbackContext bytes，复用完整 canonical
  task attempt/owner/fences 和 authority validator，删除旧位置参数 ABI。
  新 SPI 请求约束/模型签名最近 **25/25 PASS，0 skip，4.964s**；这不证明 Worker
  transport/admission 已迁完。完整 TaskAttemptId 必须参与预算与请求重试身份，不能
  只按 tenant/query/task 字符串跨 epoch 混账。ALL LLM usage stats 的生产残留仍待清理。

日志分别为 `/tmp/pangrova-production-function-callback-declarations-nearest-r2.log`、
`/tmp/pangrova-production-function-callback-worker-compile-r1.log`、
`/tmp/pangrova-production-embedding-governance-worker-nearest-r{2,3}.log`、
`/tmp/pangrova-production-lance-physical-schema-nearest-r3.log`、
`/tmp/pangrova-production-paimon-physical-allocation-nearest-r{1,2}.log`、
`/tmp/pangrova-production-canonical-unnest-layout-nearest-r1.log`、
`/tmp/pangrova-production-ai-request-resource-spi-nearest-r1.log`。
唯一 Calcite 公开入口仍按完整 consumer 单元迁移，合法 VALUES 和严格 negative 的失败
不能绕过。Linux ARM64 r10 仍在 dependency 编译，未完成且不是最新源产物。完整同源
Step0–8、大规模独立进程故障与生产 SLO 仍无 PASS，不作生产级完成声明。

### 0.19 2026-10-02 资源契约与构建根因复证

以下证据只覆盖对应源码检查点，不将组件测试或依赖构建写成生产通过：

| 范围 | 结果 | 命令、根因与限制 |
|---|---|---|
| Worker 完整测试编译 | PASS | JDK17，`mvn -Pfull-regression -pl pangrova-worker -am -DskipTests test-compile`，r3 15.022s。原 r2 的四个编译错误全部来自 Rerank 单测引用已移除的用量接口和旧 positional context；现使用实际注册的 canonical task authority，保留原分组/尾批/顺序断言并补拒绝漂移与缺失授权。其确定性 business provider 仅为单测，不证明真实 cross-encoder。 |
| AI 资源/Provider codec/FILE/JNI callback 单元 | PASS | 最近二十类，Protocol4 + Worker152，**156/156、0 skip、18.937s**。r1/r2 各 155 PASS/1 FAIL：启动测试硬编码 21 hidden 参数和旧 Embedding 签名。已从唯一声明取 hidden layout，并断言实际强制 purpose 的三参数签名，不放宽 descriptor 数量或有序业务输出。HTTP 合约测试使用本机受控响应，不记真实模型/E2E。 |
| Paimon 实际 SDK 最近范围 | PASS | 原九类最近范围，Protocol11 + Paimon16，**27/27、0 skip、16.393s**，含 allocation/schema/handle/SDK 类型/physical fixture。不是 Native ABI9 writer、SDK0.39↔Native0.20 SQL 双向互读或原子发布通过。 |
| Linux ARM64 构建 r10 | FAIL | BuildKit EOF 不是构建成功。实际 guest kernel 日志证明全局 OOM 杀死 `lima-guestagent` 和 SSH forwarding session；dockerd 仍存活且 guest Unix API 正常。恢复原 guest service 与精确失效 Unix socket forwarding 后，同一 Docker daemon 可达。未重启/删除用户容器，未猜测为依赖或网络错误。 |
| Linux ARM64 最终产物验收 | BLOCKED | r11 单编译进程实际占用约1.6GiB，加上 k3s/dockerd 等使4GiB guest仅剩55MiB可用且无swap，两个实际MinIO启动出现大量major faults。仅中断本次 owned buildx（exit130）后，可用内存恢复约1.5GiB，原完整Rust命令两项MinIO测试通过；未重启/删除用户服务，未增加超时。r11不是最终源产物，必须串行重新构建。 |

原范围 Worker 命令选择：`AiInvocationBudgetManagerTest,BoundAiInvocationTest,
AiEmbedFunctionTest,AiProviderCallContextTest,AiExecutionContextTest,GovernedBlobStoreTest,
HttpsBlobImporterTest,FileFromBytesFunctionTest,OpenAiCompatibleEmbeddingProviderTest,
OpenAiCompatibleInferenceProviderTest,OpenAiResponsesInferenceProviderTest,
OpenAiTranscriptionInferenceProviderTest,ConfiguredAiProviderRegistryTest,
WorkerBootstrapCapabilityAdvertisementTest,NativeFunctionBootstrapTest,NativeFunctionBridgeTest,
AiRerankScoreFunctionTest,ConfigurableAiInvocationAuthorizerTest,GovernedAiBindingsTest,
GovernedBindingProjectionTest`，`-Dsurefire.failIfNoSpecifiedTests=false test`。
Paimon 原范围选择：`VectorLogicalTypeTest,PhysicalWriteTypesTest,NativeWriteBindingsTest,
PaimonSchemaBuilderTest,PaimonSdkVectorTypeTest,PaimonHandleSchemaIntegrationTest,
PaimonPhysicalAllocationIntegrationTest,PaimonPhysicalSchemaIntegrationTest,
PaimonLancePhysicalFileFixtureTest`。二者均用 `-Pfull-regression -pl <module> -am`。

日志：`/tmp/pangrova-production-ai-physical-request-worker-testcompile-r{2,3}.log`、
`/tmp/pangrova-production-ai-resource-admission-nearest-r{1,2,3}.log`、
`/tmp/pangrova-production-paimon-physical-allocation-nearest-r4.log`、
`/tmp/pangrova-production-linux-arm64-native-final-build-r{10,11}.log`、
`/tmp/pangrova-production-linux-r10-kernel-oom-evidence.log`。
完整同源 Rust → Release → 全 CTest → matching ASan → JNI package → 全 Java →
全回归/coverage → 全 E2E → activation smoke 尚未全部完成。

### 0.20 2026-10-02 真实响应资源、取消准入与函数契约复证

| 范围 | 结果 | 证据与界限 |
|---|---|---|
| 扩大资源/响应专项 | PASS（执行结果，非完整验收） | Protocol43 + Worker161，204/204、0 skip、19.561s。真实资源准入、响应 overshoot、属性60 checks 和 result ticket 两例均执行。旧 Rerank 单测仍有确定性 business provider，违反附件“测试无 fake”要求，必须迁移真实模型并保留分组/尾批/顺序 oracle，不能以这组总数关闭该缺陷。 |
| Coordinator 失败报告专项 | PASS | 独立 `CanonicalTaskReportValidatorTest` 45/45、0 skip、36.354s。实际 Worker terminal report 保留 admitted50/observed1001 并按完整 TaskAttempt 校验；FINISHED50/51 被拒绝。该测试验证报告契约，不证明 JNI/实际 HTTP 已贯通。 |
| 函数/Window/AI 取消最新专项 | PASS | r2 Protocol19 + Planner118 + Worker12 + Coordinator45，共194/194、0 skip，约1m36s；保留原192项并增加两个ROW_NUMBER契约测试。Window properties 8+50 checks，AI property60 checks，不能当成额外158个测试计数。此检查点不覆盖随后新增JNI生命周期和ResourceUsage19；未运行真实模型或 Native Task。 |
| 原 Planner 合法 VALUES 专项 | FAIL | `ConversionSnapshotOwnershipTest` 原合法 `FROM (VALUES ...)` 被生产 ANTLR 拒绝；对应 r1 Protocol3 PASS、Planner12 PASS/1 ERROR，Coordinator未到达。独立 Coordinator PASS 不覆盖该 FAIL。保留合法 SQL 与原断言，唯一 Calcite 前端完整迁移尚未完成。 |
| 完整 Rust 最近范围 | PASS | r2 `cargo test --locked --release --target aarch64-apple-darwin`，35 passed、0 failed/ignored/filtered；编译2m08s、测试1.99s、0 warning/error。实际MinIO conditional Create 单赢家与不支持条件创建的 fail-closed 均执行。不是 SDK0.39/ABI10 Native writer、public SQL 或最终全链验收。 |
| 真实 SDK registration snapshot | PASS（SDK构建） | repository-owned patch 已按固定commit及源码blob校验实际应用。旧libvelox.a的 `record` 校验明确FAIL（不定义新API），拒绝假provenance；同源Release实际120/120重建、0 warning/error，新API定义校验和三行provenance记录通过。不是函数全集准入/实际值差分通过。 |
| Java 生命周期/撤权契约 | PASS（纯契约） | r2原22项全范围22/22、0 skip、14.946s，Worker全部main/testCompile通过。Native状态14、真实semaphore部分许可回滚2、terminal evidence3、drain authority3。r1 20 PASS/2 FAIL：proto3默认state0序列化为空，roles空列表清空并未篡改；修正真实测试输入而非放宽生产校验，七种authority篡改均保留。不是JNI/Flight/SQL执行通过。 |
| 原范围扩大复验 | PASS（纯契约） | 原194项与上述22项、SPI指纹5项合并复验，5+19+118+34+45=221/221、0 skip、37.254s。保留各范围全部有效断言，不含模型、JNI或SQL实际执行。日志`/tmp/pangrova-production-functions-lifecycle-abort-nearest-r1.log`。 |
| 同源 Native/ASan/JNI/全仓/分布式性能 | FAIL（完整 CTest） | 最新 ABI10 Step0 r2 PASS（2m06s、0 warning/error），Release configure及完整Release r9 PASS、0 warning/error；r1–r8实际编译失败均保留记录。完整CTest r1发现3612项，1995 PASS / 1617 FAIL，517.38s、exit8，细分见0.22。当前源matching ASan、package、全Java回归/E2E尚未通过；旧 Native 测试或 Linux 依赖缓存不是当前源验收。无全 Step0–8 或大规模部署 SLO PASS。 |

根因与修复：实际 HTTP delivered chunk 可超过可保留额度。原先把这些观察值写入
`provider_response_bytes` 后，Coordinator 用 grant 校验而丢弃真实 FAILED 报告，可能
让任务无法正确终结。统一 `result.proto` 现定义 admitted/retained response bytes，
另加通用 `observed_provider_response_bytes`：后者包含被拒绝 chunk，允许超过保留
额度但必须非负且不小于 admitted。FINISHED 必须两者一致，FAILED/CANCELLED 可携带
overshoot 证据。没有 LLM token/cost Usage Ledger，也没有第二 AI 协议。
最初 Protocol 原 fixture admitted10/observed0 被正确拒绝；修复 fixture 的真实观察值，
原 10/11/-1 边界仍保留，不能删除约束让报告通过。

取消与回收现在是不同状态：Native cancel/加入 release 前撤销 Worker AI authority，
真实 semaphore 等待线程被唤醒，新发送/后续 retention fail-closed。取消不删除计账、
注册槽或模型物理许可；只有实际 Task join 后 retirement、所有 admission owner 关闭
才允许移除。真实在途 HTTP 终止、远端 inference abort 和 Native 取消仍未验收，
不能把准入取消 PASS 当作远端工作已停止的证明。

任务关闭不能用日志代替失败结果：`NativeTaskExecutor.close()` 逐句柄尝试释放，
聚合并抛出失败；未成功join的句柄、资源authority和外部shuffle释放状态保持可重试。
`isFinished()` 不再将JNI错误当FINISHED或吞成轮询false，已关闭但仍active的句柄不能
伪装完成，empty/UNSPEC/unknown report必须报协议错误。最新修改尚待真实JNI回归，
原反射注入句柄/NOOP release测试已迁移为真实任务、真实JNI注册表和确定性锁竞争，
原六项release加两项concurrent-close oracle保留；打包后才允许执行，不计当前通过。

Arrow/Velox 选择与固定源码证据详见
[边界决策](ai-native-arrow-velox-boundary-decision.md)：Arrow 唯一互操作/wire，Velox
唯一任务内表示；必要转换逐 buffer 计账，保留任务内字典，禁止私有编码或 fallback。
边界固定逻辑 schema 的字典展开是当前取舍，不宣称物理必需或未经实测的性能最优。

全部使用 JDK17 与 `-Pfull-regression -pl <module> -am
-Dsurefire.failIfNoSpecifiedTests=false test`。独立 Coordinator 的 `-Dtest` 如上；
最新194项的选择为：
`DeclaredFunctionOptionTest,FunctionBindingDeclarationsTest,FunctionTypeFormulaEvaluatorTest,
WindowInvocationContractTest,WindowBoundLiteralContractTest,WindowProjectionContractTest,
WindowLiteralProtocolPropertyTest,DeclaredSqlBindingsTest,FunctionMapperTest,
CanonicalTaskReportValidatorTest,AiInvocationBudgetManagerTest,AiPhysicalInvocationAdmissionTest,
AiPhysicalResourceEnvelopePropertyTest`，目标模块 `pangrova-coordinator`。

日志：`/tmp/pangrova-production-ai-observed-response-admission-nearest-r1.log`、
`/tmp/pangrova-production-provider-response-observation-protocol-r1.log`、
`/tmp/pangrova-production-provider-failure-terminal-planner-coordinator-nearest-r1.log`、
`/tmp/pangrova-production-observed-response-coordinator-nearest-r1.log`、
`/tmp/pangrova-production-canonical-functions-ai-cancellation-nearest-r2.log`、
`/tmp/pangrova-production-lance-full-rust-nearest-r2.log`、
`/tmp/pangrova-production-lance-frozen-step0-r{1,2}.log`、
`/tmp/pangrova-production-velox-metadata-snapshot-release-r1.log`、
`/tmp/pangrova-production-velox-metadata-snapshot-provenance-r1.log`、
`/tmp/pangrova-production-native-frozen-release-r{1,2,3,4,5,6,7,8,9}.log`、
`/tmp/pangrova-production-native-frozen-ctest-r1.log`、
`/tmp/pangrova-production-native-lifecycle-contract-nearest-r{1,2}.log`。

### 0.21 2026-10-02 持久化 abort 屏障（待实测）

根因：确认abort后DELETE journal使迟到的相同query/fingerprint重新INSERT PENDING。
唯一API改为`markAborted`，不保留旧删除接口。JDBC单行条件UPDATE只接收相同fingerprint、
未发布的PENDING/ABORTED；publication与abort竞争必须仅一方成功，已发布PENDING不可abort。
ABORTED redo identity持久保留；重复abort幂等，缺intent不能伪装成功，普通GC不能删除，
迟到recordIntent/recordPublication/markCommitted拒绝。V49约束状态集合与ABORTED无publication。
同时修正markCommitted缺intent日志NOOP，改为明确失败，仅相同COMMITTED重放幂等。

新增真实PostgreSQL重开、abort/publication竞争、已发布拒绝和缺失证据测试；保留原全部有效
fingerprint、replay、commit、GC断言。SPI真实不可变对象状态/指纹oracles已扩展。
源码修复不计验收：29模块dependent main/testCompile PASS（59.437s），SPI指纹/不可变
对象专项5/5 PASS（4.503s、0 skip）。随后真实PostgreSQL原完整两类专项18+9=27/27，
与SPI5项合计32/32 PASS、0 skip、24.982s；使用JDK17和当前Docker Unix endpoint，
每个数据库用例执行完整生产Flyway迁移，包括V49，未用内存数据库或预设reconciler。
现有recovery测试仍有预设结果reconciler，不得计实际connector恢复证据。
execution-anchor的检查和journal变更尚不在同一事务，外部catalogCAS/publication证明、
UNKNOWN结果、旧attempt receipt绑定与安全terminal-retention仍是独立待闭环门槛。
最近日志：`/tmp/pangrova-production-write-abort-barrier-test-compile-r1.log`、
`/tmp/pangrova-production-write-abort-barrier-spi-nearest-r1.log`、
`/tmp/pangrova-production-durable-abort-postgres-nearest-r1.log`。

LLM统计清理补充：全库caller/wiring检查仅发现`MultimodalAuditEnricher`与其DTO互相
引用，无真实audit sink/flush注册；`MultimodalCostEstimator`/默认实现亦无production
caller或ServiceLoader声明，仅E2E wiring豁免字符串。已移除这四个未接线类及对应豁免，
不保留估算tokens/cost旁路。真实鉴权审计、kernel bytes/attempts/items与单次请求约束
不属于Usage Ledger，仍保留。`maxInputTokens`的实际provider执行约束仍需独立复证，
不能仅因配置hash和advertisement存在就宣称已enforce。删除后31模块E2E依赖
main/testCompile PASS（42.906s）；这不是测试执行或SPI wiring E2E通过。

### 0.22 2026-10-02 完整 Native 失败分类与严格根因修复（未验收）

完整Release r9构建通过、0 warning/error后，实际执行未过滤的
`ctest --test-dir pangrova-worker-native/build --output-on-failure`：发现3612项，
1995 PASS / 1617 FAIL，517.38s、exit8。按每个失败CTest块分类，1502项出现
显式字段引用root缺失、54项出现RelRoot输出names缺失，61项为其他失败。
这不是1617个独立缺陷，也不是仅旧fixture问题：生产typed检索/物化生成端也有
缺失root，而Java类型推导曾默认input schema，导致Java放行、Native拒绝。
统一生产生成端使用协议模块的rootFieldReference，Java与Native均严格拒绝缺失来源；
合法fixtures补真实声明，不能放宽Native来兼容畸形计划。

其他已定位缺口：任务内字段已按ordinal使用物理alias，但artifact/overlay/retrieval
消费者仍将其与逻辑契约名称比较或按逻辑名查找物理列。改为现有logicalNodeType
校验名称和语义，再按同一ordinal读取物理列；保留类型、稳定键、locator及schema
漂移拒绝。Arrow serde旧fixture读取私有4byte长度前缀，实际流首为canonical
continuation marker；现按真正decoder验证完整schema、所有六个BIGINT值及EOF，
不再验证已删除私有编码，opaque类型仍必须在写出任何payload前失败。
这些源码修改尚未完整重建或通过CTest。

SUM0与RANGE的六个崩溃并未被当成语义算法错误猜测修补。真实r9 LLDB复现显示
FlatVector/ValuesNode析构后访问失效MemoryPool；局部converter持有Values pool，
返回裸plan的测试consumer未保留owner。生产driver另有显式holder移交，仍需把
公共转换产物的资源所有权收敛为不可遗漏契约，并验证子vector/输出batch逃逸。
STRUCT literal支持缺口、DISTINCT窗口和SUM0实际值问题仍须独立修复，不能用
生命周期修复的预期结果代替运行证据。

| 检查点 | 状态 | 实测边界 |
|---|---|---|
| E2E dependent main/testCompile | PASS | JDK17，31模块、42.906s；未执行JNI/E2E。 |
| strict root/layout最近范围 r1 | FAIL | 原完整42项：41 PASS / 1 ERROR、0 skip；FanOut绑定报expected1/bound0，继续查实际builder/fixture，未删除用例。 |
| 独立owned Lance SDK drain最近范围 r3 | PASS | 原完整6/6、0 ignored/filtered/warning/error；真实MinIO multipart与响应丢失后的独立HEAD/GET均执行。r1/r2实际失败保留，未将超时当成UNKNOWN证明。主ABI/Task取消尚未接线验收。 |
| canonical descriptor gate | FAIL | Java/C++共用source，无第二vector协议；语义复证显示函数manifest、资源字节/attempts/items、physical receipt及result/drain authority的实际变更，而非仅文件排序。旧golden未盲目刷新，须完成双端wiring后生成并重跑。 |

Lance SDK已按固定crate checksum、逐source及全tree哈希纳入repository-owned patch；
最新主Cargo接入与唯一ABI11 drain/destroy分离正在实施。旧ABI10 Release/CTest
检查点因此是历史证据，不覆盖新输入；必须重新从Step0执行完整链。
显式drain保留handle/runtime，destroy仅接收真实joined证据；private stage未清理
保留Task-owned writer供重试，UNKNOWN/已发布final object不得作为删除授权。
源码接线和独立SDK测试不证明NativeTask即时取消、全局Rust heap/spill或发布恢复已闭环。

日志：`/tmp/pangrova-production-native-frozen-release-r9.log`、
`/tmp/pangrova-production-native-frozen-ctest-r1.log`、
`/tmp/pangrova-production-real-fixture-whole-chain-test-compile-r1.log`、
`/tmp/pangrova-production-strict-root-layout-nearest-r1.log`、
`/tmp/pangrova-production-lance-owned-sdk-drain-nearest-r{1,2,3}.log`、
`/tmp/pangrova-production-proto-descriptor-current-r1.log`、
`/tmp/pangrova-production-sumzero-actual-r9-lldb-r2.log`、
`/tmp/pangrova-production-range-window-actual-r9-lldb-r2.log`。

### 0.23 2026-10-02 ABI11、完成回执与提交片段身份（未验收）

ABI11接入唯一repository-owned Lance SDK后，Step0实际构建成功，0 warning/error，
3m11s。完整Rust测试第一次独立执行因shell未继承`PROTOC`失败；已核对固定工具链
存在，显式设置`PROTOC`、`PROTOC_INCLUDE`与macOS 13部署基线后，原完整范围
36/36 PASS，0 failed/ignored/filtered/warning/error，编译4m22s、测试1.68s。
真实MinIO用例执行；新增真实drain/destroy拒绝及private清理失败后的同handle恢复
用例不等同于Task/JNI即时取消验收。随后同一TaskReport增加final NativeTaskStats
字段的源码变更必须重新执行Step0；上述结果不覆盖后来的协议输入。

strict root/layout r2仍有FanOut候选类型不一致：BM25实际FP64 Nullable，而原
codec fixture未声明nullability。按生产distance的DOUBLE_NULLABLE声明和已有
BM25 score契约修正fixture，并新增UNSPECIFIED明确拒绝；未放松schema等值检查。
r3扩大到Planner三种artifact sink：Protocol完整50/50 PASS，Planner10项中9 PASS /
1 FAIL。GeneratedValue测试把实际Project输入当Read读取，取得protobuf默认空对象；
已改用唯一Layout描述Project输出，同时保留底层Read真实字段顺序断言，尚待重跑。
r4原范围重跑在SPI testCompile失败：release契约原子迁移尚未覆盖两个旧匿名假
TaskExecutor；没有补造Optional成功，也没有缩小scope。后续须在完整源码稳定后
重跑同一范围。r5在SPI及同一TaskReport field11源码稳定后实际重跑相同扩大范围：
Protocol50/50、Planner三sink10/10，共60 PASS，0 failed/errors/skips，23.131s。
GeneratedValue的真实Project及底层Read两组字段/emit断言全部保留并通过。
局部Protocol codec PASS不是写入发布或SQL端到端通过。

真实写入审计发现，TaskReport外层attempt已验证，但PhysicalFileWriteFragment原来
直到最终聚合才按片段自报attempt推导allocation，未先绑定实际dispatch身份。
现在共用PhysicalFileWriteReceipts的严格校验，在CanonicalTaskReportValidator阶段
检查完整trusted attempt、已下发attempt prefix和全部allocation身份，再允许任务状态、
DAG及提交副作用；FINISHED物理写缺失/空片段明确拒绝。新增codec tests覆盖所有
attempt字段漂移、allocation错绑和畸形/超限/行数声明；WriteResultDescriptor accessor
返回owned copy，避免校验后的可变字节逃逸。Coordinator接线已随31-module生产源码
编译通过（21.352s）；本次只执行compile，不含testCompile或任何测试。依赖收集有
外部Yetus effective-model的2个问题警告，不宣称零warning。Coordinator新增测试
仍尚待编译和执行。
这不解决外部catalog发布与execution-owner fence之间的原子性缺口。

完成资源释放契约改为同一次真实Native join返回最终canonical TaskReport，再移交
TaskInfo共享future，最后退款。Registry必须在erase前完成序列化与JNI分配；任一
失败保留handle以便恢复。NativeTaskExecutor仅在其余外部清理未完成时保留真实回执，
全部清理完成后移交并删除，不设无界tombstone。Optional.empty表示不存在可移交回执，
不是完成证明；未知handle的isFinished也不再伪造true。取消保持CANCELLED及真实
错误，清理成功不等于业务成功。上述JNI/Worker完整链仍在实施，尚未执行验收。

Velox同一固定源码patch新增BaseVector与Arrow导出真实MemoryPool所有权，涵盖
独立slice、非byte对齐null bitmap拷贝和inline Constant导出，并合入Arrow import异常
所有权转移修复。旧archive的record实际明确拒绝缺失ownsMemoryPool定义；匹配SDK
Release整库正在重建，不能用新header配旧archive，也不能从静态apply检查推定内存安全。

日志：`/tmp/pangrova-production-lance-abi11-step0-r1.log`、
`/tmp/pangrova-production-lance-abi11-full-rust-r{1,2}.log`、
`/tmp/pangrova-production-strict-root-layout-nearest-r{2,3,4,5}.log`、
`/tmp/pangrova-production-velox-owner-sdk-apply-r1.log`、
`/tmp/pangrova-production-velox-owner-old-archive-record-r1.log`、
`/tmp/pangrova-production-velox-owner-sdk-release-r1.log`、
`/tmp/pangrova-production-final-report-main-chain-compile-r1.log`。

独立Dataset另外确认资源回收断点：其InsertBuilder的GenericWriter被stream/progress/
write/finish错误提前drop，外层线程join不能证明内部encoder和multipart已joined。
physical ABI11 drain不覆盖该实际producer；下一源码单元统一为唯一ABI12，删除
void吞错close，并将GenericWriter所有真实实现/调用者纳入同一owned SDK实际abort/drain
契约。它在实施而非PASS，新的SDK/协议/ABI输入仍须重新执行完整链。

当前external catalog fence仍未原子闭环。源码在finishDistributedWrite前后各检查
execution-owner，DistributedWriteCommitContext未携带可由外部提交权威原子校验的
owner条件。上游Paimon的OCC、commitUser幂等和snapshot原子发布不是同一execution
anchor的校验，不能由两个检查推定排除旧owner在检查间提交。可借用上游catalog-managed
snapshot commit机制，但必须在最终提交权威里证明该条件，不能只在Coordinator额外重试。
这是结合当前源码与[Paimon官方并发提交机制](https://paimon.apache.org/docs/master/concepts/concurrency-control/)
得到的架构缺口判断，不代表本仓固定SDK已经具备该owner-fence接口；固定版本实现还需复证。
随后已核对本机Paimon 1.4.1 sources jar及release-1.4.1 tag：最终SnapshotCommit
接口没有execution owner参数，catalog adapter只传表UUID等snapshot信息，rename
adapter的lock也未验证Pangrova execution anchor。已将最终可见性权威、UNKNOWN
迟到请求以及真实双Coordinator验收规格记录到
[Lance写入ADR当前复证](ai-native-lance-write-adr.md#固定-paimon-141-的最终提交权威复证)。
这是源码复证与修复规格，不是故障实测或已实现的原子fence。

### 0.24 2026-10-02 远端 producer 清理与真实重试（待运行验收）

实际源码确认 `RemoteShuffleByteEgress.onTaskCompleted` 的finally在abort失败时
仍删除task，close先clear再逐任务abort会在首个异常后漏掉其它任务。现在完成路径
只在finish成功或实际abort/drain成功后移除owner；close先停止准入、逐task try-all，
失败保留task及pool供重试。Task scoped职责抽到`RemoteShuffleTaskEgress`，不增加
第二执行器或协议。producer取消唤醒readiness阻塞，统一有界cleanup grace等待
active write/finish退出及真实gRPC terminal callback；发送cancel本身不计drain。
该本机client-side证据不证明server已停止业务I/O或归还remote grant，仍须真实service
观察验收；gRPC明确指出server收到取消后可能继续处理。
[gRPC取消契约](https://grpc.github.io/grpc-java/javadoc/io/grpc/stub/ClientCallStreamObserver.html#cancel-java.lang.String-java.lang.Throwable-)。

TaskCompletionMonitor另确认真实release失败使future结束，但原sole cleanup sweep
只处理terminal任务，CANCELLING/FINISHING的owner和grant可能永久无人重试。
已保留受TaskInfo容量限制的原release continuation，由同一cleanup sweep按精确
owner重驱；不按RUNNING状态推断成功，不新增timer或假回执。完成资源清理与
业务结果报告仍分开。Native释放实测待最新打包，不把纯FSM/metadata tests当E2E。

全31生产源码第二轮compile为PASS，22.087s，覆盖上述source checkpoint；只有
compile，没有testCompile或测试。外部Yetus effective-model仍有2个问题warning。
后续connector owner释放成功后才refund的顺序变更发生在此编译之后，须再验证。
现有模拟执行器及录制shuffle service测试正迁到真实Task/服务，完整testCompile
仍未通过，原数据/取消/并发/auth/失败oracle不得因迁移删除或缩小。

日志：`/tmp/pangrova-production-final-report-main-chain-compile-r2.log`。
Release Velox SDK仍在整库构建，完成后还要合入已定位的RowContainer nullable
accumulator destroy根因并重建同源archive。之后才能开始最新ABI12/source cohort
的全Native CTest、匹配依赖ASan及Step0–8；目前没有这些PASS证据。

## 1. 2026-08-28 历史审计结论

截至该审计基线，Pangrova 已有数量可观的 AI/Vector 组件，但尚未形成“AI Native 数据库产品”的
端到端闭环。当时最成熟的是 VECTOR 类型及 native SIMD 精确距离原语；AI SQL、自动 Embedding、混合检索、
分布式 Top-K、多模态、Semantic Cache 和治理链路均存在未接通、静默降级或错误结果风险。

本次复证得到六个 P0：

1. 15 个 AI SQL 函数只注册到 Java 内存桥，未注册到 C++ `NativeUdfRegistry`。
2. 未命中的模型名会静默选择任意 provider，可能调用错误模型、区域和计费对象。
3. worker/coordinator 会过滤分片超时或解析失败，仍以成功返回未声明的部分 Top-K。
4. Hybrid/Rerank 的 coordinator、worker、factory 配置协议不一致，并存在两次排序方向反转。
5. BM25 Java 声明与 C++ 导出 JNI 符号不一致，且异常可把整个 native runtime 标记为不可用。
6. FILE URL 可直接读取任意 HTTP(S) 地址且无大小上限；inline FILE 返回未真实落盘的 URI。

因此该审计时点的口径是：**具备部分 AI Native 原语和实验性组件，不具备完整可运营的 AI
SQL 或 Search 产品闭环。**
生产修复必须从正确性与测试真实性开始。

## 2. 审计方法

只有同时存在生产入口、规划/协议表达、运行时 wiring、真实实现、明确失败语义、治理约束及
有效测试，才标为“已闭环”。组件直连测试、ServiceLoader 成功、AST 可解析均不能单独证明闭环。

| 状态 | 判定标准 |
|---|---|
| 已闭环 | 入口、实现、wiring、失败语义、治理和有效测试均可复证 |
| 部分闭环 | 主链可用，但持久化、分布式语义、治理或测试有缺口 |
| 未接通 | 类型或实现存在，但生产入口到实现之间缺 caller/registration/dispatch |
| 仅设计 | 只见文档、SPI、AST、计划节点或无 caller 的接口 |
| 存在缺陷 | 可触发错误结果、错误模型、安全风险、静默部分结果或伪通过 |

证据优先级为：生产调用链 > wiring/JNI/协议双端 > 实测 > 局部单测 > 文档/SPI/注释。
仓库全局引用搜索未发现 caller 时，本报告将其作为负向 wiring 证据。

## 3. 2026-08-28 声明—实现账本

### 3.1 十条能力链

| # | 能力链 | 状态 | 端到端判断与证据 |
|---:|---|---|---|
| 1 | AI SQL 函数与推理 Provider | 存在缺陷 | factory 可构造 15 个函数；descriptor 只写日志，native registry 启动时为空。`AiFunctionFactory.java:72-112`; `NativeFunctionBootstrap.java:46-72`; `NativeEngineLifecycle.cpp:440-443`; `NativeCommandGetCapabilitiesTest.cpp:124-130` |
| 2 | 模型、配置、Secret、生命周期 | 存在缺陷 | provider 配置存在；模型精确解析、统一对象、secret reference、生命周期和 fail-closed RBAC 不存在。`AiFunctionFactory.java:91-112`; `ConfigurableInferenceProvider.java:43-71` |
| 3 | Embedding 类型、SYNC/ASYNC、DDL、队列、CDC、backfill | 部分闭环 | execution 只注册 CREATE/DROP/SHOW；ALTER/backfill/cancel/status、rewrite、durable worker 链未接通；`AI_EMBED` 返回 JSON VARCHAR。`QueryExecutionBootstrap.java:433-450`; `DistributedBackfillCoordinator.java:45-212` |
| 4 | VECTOR、Substrait、JNI、Native SIMD 精确距离 | 已闭环 | VECTOR、距离映射、JNI/native SIMD 和 C++ 测试构成当前最完整原语。`VectorSearchRewriter.java`; `VectorDistance.cpp`; `VectorDistanceFunctions.cpp`; `VectorDistanceTest.cpp` |
| 5 | HNSW、IVFPQ、DiskANN、Lance、索引生命周期 | 部分闭环 | HNSW/IVFPQ 与 DiskANN 有实现；Lance Java 主入口抛 UOE，C++ 是 brute-force；统一 freshness/lifecycle 缺失。`LanceVectorEngine.java:73-83`; `LanceVectorIndex.cpp:1-17` |
| 6 | BM25、Hybrid、RRF/Linear/Cross-Encoder/Cascade | 存在缺陷 | 无全文候选合流；协议字段不一致；RRF/全局 merge 方向错误；BM25 JNI ABI 不匹配。`VectorSearchTaskSupport.java:184-265`; `DispatchContextEnricher.java:144-152`; `RrfReranker.java:40-52`; `Bm25NativeScorer.java:40-50`; `JniBridge.cpp:771` |
| 7 | 分布式路由、Top-K、超时与部分结果 | 存在缺陷 | 失败被转为空列表后继续成功，客户端无 coverage/status/snapshot/freshness。`QueryVectorResultMerger.java:71-138`; `GlobalTopKMerger.java:81-119` |
| 8 | FILE/BLOB、多模态、存储、安全、清理 | 存在缺陷 | inline bytes 未存储；URL 拉取无 SSRF/大小控制；治理 helper 未 wiring。`FileFromUrlFunction.java:46-80`; `FileFromBytesFunction.java:41-92`; `FileStructHelper.java:221-235`; `OpenAiCompatibleEmbeddingProvider.java:307-329` |
| 9 | Semantic Cache、自优化、Feature Store、训练数据 | 存在缺陷 | cache 只写空结果且无读取；principal 未参与隔离；Feature Store 仅 SPI；训练服务默认禁用。`QueryPipelineExecutor.java:158-178`; `InMemorySemanticCache.java:54-84`; `FeatureStore.java:44` |
| 10 | RBAC、Guardrail、成本、审计、可观测性、基础设施评估 | 部分闭环 | 有通用 RBAC/审计和局部检索质量采样；缺不可绕过的 AI admission、usage ledger 和统一评估门禁 |

### 3.2 子能力

| 子能力 | 状态 | 真实完成度 |
|---|---|---|
| 15 个 AI SQL operator | 未接通 | operator/factory 存在；无 Java callback 注册到 C++ 的生产路径，native capability 为空 |
| OpenAI-compatible inference | 部分闭环 | HTTP 调用与 usage 解析存在；未受统一 MODEL、RBAC、预算和审计控制 |
| Embedding provider SPI | 未接通 | `EmbeddingProviderLookup` service 文件没有实现条目 |
| Embedding model DDL | 部分闭环 | parser 覆盖 CREATE/ALTER/DROP/SHOW；execution 只注册 CREATE/DROP/SHOW |
| SYNC auto embedding | 未接通 | rewrite rule 只在测试中直接构造，未证实进入 production planner |
| ASYNC queue/CDC | 部分闭环 | listener/queue metadata 存在；未证实 durable consumer 到原子提交 |
| Distributed backfill | 存在缺陷 | job/checkpoint 为内存状态，partition loop 串行，完成即删除 checkpoint |
| Exact L2/cosine/inner product | 已闭环 | native 实现和 Release CTest 通过 |
| HNSW/IVFPQ | 部分闭环 | Java 实现和局部测试通过；缺 service lifecycle/snapshot/freshness |
| DiskANN | 部分闭环 | JNI 实现/符号存在；本次 native suite 无 DiskANN CTest |
| Lance | 存在缺陷 | Java 主入口不可用，native 为 brute-force，测试在依赖不可用时 SKIPPED |
| MATCH/BM25 | 存在缺陷 | planner metadata 不等于 retrieval；JNI 方法名不一致导致 native 不可达 |
| Hybrid candidate fusion | 未接通 | worker 候选全部标为 `vector`，没有 production FTS candidate source |
| RRF | 存在缺陷 | 对 vector distance 降序排名，优先更远候选 |
| Cross-encoder/cascade | 存在缺陷 | 三端参数不一致，异常被静默回退 |
| Global Top-K | 存在缺陷 | lower-distance merge 存在；输入可能是 higher-is-better rerank score，且可静默缺分片 |
| FILE URL/bytes | 存在缺陷 | URL 未防 SSRF/OOM；bytes URI 无对象；当前时间戳却声明 deterministic |
| Semantic Cache | 存在缺陷 | 只写空 result/空表依赖；无 production `findSimilar` caller；principal 未使用 |
| Layout/index recommendation | 部分闭环 | 有 advisor/scheduler；多处只日志、空 file list 或 completed-future no-op |
| Feature Store | 仅设计 | 只有 SPI，无生产实现/wiring |
| Training data service | 未接通 | bootstrap 采用 disabled defaults；扫描路径还违反 coordinator 边界 |
| AI 基础设施评估 | 部分闭环 | quality sampler 只覆盖局部检索指标，缺模型、Embedding 和 Search 的统一回归门禁 |

## 4. 2026-08-28 十条链路复证摘要

### 4.1 AI SQL 与模型调用

真实链路在 Java/native 注册边界中断：operator table 和 `FunctionMapper` 只能证明 planner
识别，`NativeFunctionBootstrap` 构造的 descriptor 最终只记录日志。native engine 创建空
`NativeUdfRegistry` 后立即 `registerAllWithVelox()`，之后没有可证明的 GlobalRef/callback 安装。
native capability 测试明确期望 supported functions 为 0。

模型仍是请求字符串和 provider 默认配置。未知模型会选择 map 中第一个 provider；endpoint、
API key 和默认 model 来自 system property，默认 key 为 `no-key`。无统一 MODEL 对象、版本冻结、
credential reference、区域/价格/权限和精确解析。

### 4.2 Embedding、CDC 与 Backfill

parser 语法面大于 execution wiring。CREATE/ALTER/DROP/SHOW 及 BACKFILL/CANCEL/STATUS 可形成
AST，但当时 bootstrap 只注册 CREATE/DROP/SHOW。`EmbeddingBootstrap`、`AutoEmbedRewriteRule` 和
dispatcher/checkpoint 组件当时没有形成生产链。后续修复删除了没有生产 caller 的
`AutoEmbedRewriteRule`；mutation 改为在实际 Planner.convert 中、policy application 之前使用
冻结的 canonical GeneratedColumnSet 注入 NEW/action 表达式，不保留 executor directive 侧路。

`DistributedBackfillCoordinator` 使用进程内 active job/checkpoint，partition loop 实际串行，
完成后删除 checkpoint，无法满足重启恢复、租约接管、幂等重试和原子发布。通用 embedding
contract 未统一约束 batch 数量、维度、顺序和失败原子性；SQL 返回 JSON `VARCHAR`，绕过
VECTOR 维度类型检查。

### 4.3 VECTOR 与 ANN

native SIMD 精确距离是唯一可标“已闭环”的核心原语，Release 构建和相关 CTest 已通过。
planner rewrite 仍只覆盖 top-level sort、literal query vector 和升序距离，不等价于完整 Search
Service。HNSW/IVFPQ/DiskANN 有实质代码，但没有统一 manifest、source snapshot、freshness、
refresh、suspend/resume 和回收对象。Lance Java engine 抛 UOE，native 明确为 brute-force。

### 4.4 全文、Hybrid 与 Rerank

MATCH rewrite 只携带 metadata，没有查到把全文倒排候选送入 hybrid reranker 的生产链；worker
给所有候选标记 `vector`。coordinator 生成 `weighted:w1:w2`、`cross_encoder:model:topN`、
`cascade:stageCount`，worker/factory 却期待不同键名和 stage 列表，异常后静默返回原 vector 结果。

RRF 把距离当作越大越好降序排名；重排分数又写入名为 `distance` 的字段，coordinator 随后
按距离升序 merge，造成跨 worker 的第二次反转。

### 4.5 分布式 Top-K

worker 对文件超时/失败继续返回其余结果；coordinator 将 fetch timeout、解析和解压失败转为空
列表并过滤。只要仍有一个结果，查询可成功，却没有 COMPLETE/PARTIAL/FALLBACK_EXACT、
expected/responded shard、coverage、source snapshot 或 index freshness。

### 4.6 FILE/BLOB 与多模态

在该审计基线，FILE 是 ROW/STRUCT sugar。`FILE_FROM_BYTES` 生成 `blob://inline/<sha>` 却未写入
对象；两个 constructor 都带当前时间但 metadata 声明 deterministic。URL 只校验 scheme，provider 随后
直接 `openStream().readAllBytes()`，无 private-IP/redirect/allowlist/size/MIME/checksum/timeout，
形成 SSRF、OOM 和不可重放风险。已有 blob/audit/security helper 未找到完整 production wiring。

### 4.7 Semantic Cache、自优化与训练数据

`QueryPipelineExecutor` 只在 completion 后写 cache，且写入 `Set.of()` 表依赖和 `new byte[0]`
结果。production source 中未发现 `findSimilar` caller。两种 cache 都忽略 principal；HNSW 先跨
tenant ANN 再过滤 tenant，召回和隔离均不可证明。Feature Store 只有 SPI；训练服务固定采用
disabled defaults，且 coordinator 扫描路径违反模块边界。advisor/scheduler 仍是局部组件。

### 4.8 治理、可观测性与基础设施评估

`AI_GUARD` 是普通可选 UDF，不是不可绕过的 policy gate。provider usage 没有统一进入 cost
ledger、budget、audit/trace，未知 usage 可被表示为 0。没有 AI function/model privilege，
也没有覆盖模型、Embedding、检索召回和资源成本的统一基础设施评估门禁。

## 5. 2026-08-28 P0–P3 发现与修复规格

### P0-01：AI SQL UDF 未进入 Native 注册

- **影响/触发**：任一 `ai:*` 计划可能解析成功但执行端无法解析 function。
- **调用链**：operator → mapper → capability → `NativeUdfRegistry`；Java descriptor 未跨 JNI。
- **根因/证据**：三套 registry 无单一事实源；`NativeFunctionBootstrap.java:46-72` 只登记/记录，`NativeEngineLifecycle.cpp:440-443` 注册空 registry，capability test 期望 0。
- **修复/依赖**：统一版本化 Capability Descriptor；成功安装 GlobalRef/callback/native invoker 后才上报 capability；依赖 JNI 生命周期和 worker handshake。
- **验收**：从 JDBC 运行每类 AI 函数；planner/worker/native descriptor hash 相同；移除注册时在执行前 fail；不得直连 factory。

### P0-02：未知模型静默调用错误 Provider

- **影响/触发**：不存在或歧义 model alias 导致结果、成本、区域和合规不可控。
- **调用链**：model argument → provider map → first-provider fallback → HTTP。
- **根因/证据**：模型不是 catalog object；`AiFunctionFactory.java:91-112`; `ConfigurableInferenceProvider.java:43-71`。
- **修复/依赖**：schema-scoped MODEL 精确解析；未知、停用、无权限或能力不匹配 fail-closed；依赖 catalog、credential、RBAC、audit。
- **验收**：四类失败均在网络调用前发生；审计记录 resolved object/version；解析不受 discovery 顺序影响。

### P0-03：分布式 Top-K 静默部分成功

- **影响/触发**：任一 shard/file 超时或损坏时，客户端收到非全局 Top-K 且看似成功。
- **调用链**：worker partial → coordinator failure-to-empty → filter → global merge → success。
- **根因/证据**：结果协议没有完整性语义；`QueryVectorResultMerger.java:71-138`; `GlobalTopKMerger.java:81-119`。
- **修复/依赖**：增加 COMPLETE/PARTIAL/FALLBACK_EXACT、coverage、snapshot/freshness；依赖协议、query snapshot 和 index manifest。
- **验收**：注入单 shard timeout 默认失败；显式 partial 返回准确 coverage；exact fallback 与同 snapshot oracle 一致。

### P0-04：Hybrid/Rerank 协议和排序产生错误结果

- **影响/触发**：RRF 优先更远向量，跨 worker 重排再反转；四种 strategy 均可能静默 fallback。
- **调用链**：AST → string encoding → worker parsing → factory → score-as-distance → ascending merge。
- **根因/证据**：无 typed protocol，distance/relevance 共用字段；`DispatchContextEnricher.java:144-152`; `VectorSearchTaskSupport.java:184-265`; `RrfReranker.java:40-52`。
- **修复/依赖**：protobuf `RerankSpec` 与 `{value,direction,stage}` score；显式 vector/FTS source 和 fallback status；依赖真实 FTS retrieval、协议生成和结果 envelope。
- **验收**：golden dataset 覆盖四 strategy、单/多 worker；与离线 oracle 一致；错误配置不得普通成功。

### P0-05：BM25 JNI ABI 不匹配并污染 Native 状态

- **影响/触发**：首次 BM25 native 调用缺符号，回退 Java，并可使后续 native 能力被视为不可用。
- **调用链**：Java `nativeScoreBatchRequest` -X-> C++ `nativeScoreBatch` → `UnsatisfiedLinkError` → fallback + `markUnsupported()`。
- **根因/证据**：JNI 未同源生成且 feature-local 失败修改全局 cache；`Bm25NativeScorer.java:40-50`; `JniBridge.cpp:771`; `NativeLibraryLoader.java:95-128`。
- **修复/依赖**：生成/统一 binding，capability 按 feature/version 表达；依赖 ABI smoke 和 capability descriptor。
- **验收**：`javap -s` 与符号自动比对；真实 Java→JNI BM25 golden test；移除 BM25 不影响 vector native。

### P0-06：FILE SSRF/OOM 与虚假 inline 存储

- **影响/触发**：URL 可访问内网/metadata 或耗尽内存；bytes URI 下游不可读，结果不可重放。
- **调用链**：FILE constructor → struct → provider resolver → raw URL/readAllBytes 或不存在 inline URI。
- **根因/证据**：无可信 resolver/storage lifecycle，determinism metadata 错误；`FileFromUrlFunction.java:46-80`; `FileFromBytesFunction.java:41-92`; `FileStructHelper.java:221-235`; `OpenAiCompatibleEmbeddingProvider.java:307-329`。
- **修复/依赖**：只允许 BlobStore reference 或受控 fetch；校验 DNS/IP/redirect/allowlist/size/MIME/hash/timeout；bytes 原子落盘；依赖 quota、audit 和 cleanup。
- **验收**：私网、重绑定、redirect、超限、MIME 欺骗均 fail-closed；bytes round-trip hash 一致；失败无 orphan。

### P1-01：Embedding 类型和批处理 Contract 不统一

- **影响/触发**：少/多/乱序/维度漂移会错配行；JSON 向量逃逸类型检查。
- **调用链**：`AI_EMBED`/auto embedding → provider batch → JSON VARCHAR/column update。
- **根因/证据**：SPI 未统一 count/dimension/order/atomicity；`AiEmbedFunction.java`; `SyncEmbeddingExecutor.java`; provider implementations。
- **修复/依赖**：返回 `VECTOR<F32,N>`，stable item id，gateway 统一验证；依赖 MODEL metadata、VECTOR protocol、invocation contract。
- **验收**：四类异常均无部分写入；model dimension 与列约束冲突在 admission 前失败。

### P1-02：Embedding DDL/CDC/Backfill 非 Durable 闭环

- **影响/触发**：命令不可执行；重启后任务丢失/重复；并发度不生效。
- **调用链**：AST → 缺 handler/listener → queue → dispatcher/checkpoint → commit。
- **根因/证据**：按组件而非 job lifecycle 接线；`QueryExecutionBootstrap.java:433-450`; `DistributedBackfillCoordinator.java:45-212`。
- **修复/依赖**：持久化 CREATED→LEASED→RUNNING→COMMITTING→终态，lease/attempt/idempotency/atomic manifest；依赖 job tables、RPC、snapshot/commit。
- **验收**：crash/restart/lease steal/duplicate/cancel/provider failure 均 exactly-once visible 或无可见提交。

### P1-03：能力目录与协议 descriptor 漂移

- **影响/触发**：planner 下发不受支持计划或屏蔽能力；升级兼容不可判定。
- **调用链**：operator → mapper → protobuf/Substrait → worker/native capability。
- **根因/证据**：手工字符串/列表；本次 `proto-descriptor-diff.sh` 的 Java/native 均 FAIL。
- **修复/依赖**：统一可哈希、可协商 descriptor，CI 强制 golden；依赖 protocol generation 和 rolling-upgrade policy。
- **验收**：descriptor gate PASS；参数/模式漂移在 CI/admission 失败；混部按公共能力或 placement 规划。

### P1-04：Semantic Cache 为 write-only 空结果且隔离不足

- **影响/触发**：浪费 embedding/内存；未来接 read 会跨 principal 或返回过期/空结果。
- **调用链**：completion → embed SQL → put(empty result, empty tables)；无 production lookup。
- **根因/证据**：cache 不是 pre-read/post-commit；entry 缺安全/快照指纹；`QueryPipelineExecutor.java:158-178`; `InMemorySemanticCache.java:54-84`; `HnswBackedSemanticCache.java:72-110`。
- **修复/依赖**：key 包含 normalized query、model/version、tenant、principal/role policy、source snapshots；只缓存真实 typed result；先 tenant partition 后 ANN。
- **验收**：真实 hit 返回等价结果；role/tenant/model/snapshot 变化必 miss；entry 不得为空；invalidation 可复证。

### P1-05：AI RBAC、Guardrail、Usage、Budget、Audit 不可强制

- **影响/触发**：任何直接 provider caller 可绕过策略，成本无法准入/对账，错误和数据流向不可追踪。
- **调用链**：UDF/backfill/search 等直接 provider；无统一 before/after admission。
- **根因/证据**：治理是 helper/普通函数，不是执行控制面；`AiGuardFunction.java`; `InferenceResult.java`; caller audit。
- **修复/依赖**：resolve→authorize→policy→estimate→reserve→execute→reconcile→audit；unknown usage 不得记 0；依赖 MODEL/capability/ledger/OTel。
- **验收**：gateway 外 provider caller 静态 gate 失败；权限/预算/guardrail 在网络前生效；usage/request id/error/lineage 可查。

### P1-06：测试真实性和构建基线不足

- **影响/触发**：CI 绿色不能证明 SQL→JNI→provider；runtime 缺失或宽松断言掩盖缺陷。
- **调用链**：assumption/fallback → skip/acceptable runtime error → suite green。
- **根因/证据**：组件直连命名为 E2E；本次 planner compile、worker testCompile、默认 Testcontainers reactor 均 FAIL。
- **修复/依赖**：分清 unit/component/integration/e2e；E2E 从公共入口使用 packaged native 和可控 provider；依赖 JDK17、BOM、Docker 和 P0 wiring。
- **验收**：断开任一 wiring/JNI/provider 时 E2E 必 FAIL；SKIPPED/BLOCKED 不计通过；无常量真或 runtime unavailable 宽松断言。

### P2-01：Lance/ANN 声明高于实现

- **影响/触发**：用户预期 ANN，实际 UOE、brute-force 或 skip。
- **调用链**：index selection → Java/native Lance → UOE/brute-force。
- **根因/证据**：实验 adapter 与产品能力共名；`LanceVectorEngine.java:73-83`; `LanceVectorIndex.cpp:1-17`; 本次 Lance CTest SKIPPED。
- **修复/依赖**：显式 EXPERIMENTAL/EXACT_FALLBACK/ANN；依赖 manifest、benchmark 和 packaging。
- **验收**：recall/latency/memory/refresh 达门槛；依赖缺失时 admission 失败或声明 exact fallback。

### P2-02：自优化、Feature Store、训练数据未产品化

- **影响/触发**：配置看似开启却只日志/no-op；训练扫描可能落在 coordinator。
- **调用链**：advisor/scheduler/SPI → no-op/empty files/disabled defaults。
- **根因/证据**：无 durable object/job/metric/rollback；`FeatureStore.java:44`; `TrainingDataServerConfig.defaults()`。
- **修复/依赖**：定义 owner/state/snapshot/job/metric/rollback 服务对象；依赖 durable jobs、catalog、worker scan、evaluation。
- **验收**：refresh/rebuild/export 可恢复/取消/审计，收益可度量，失败原子回滚。

### P2-03：公共文档声明漂移

- **影响/触发**：用户把设计/原语理解为 production auto ANN/hybrid/backfill/multimodal。
- **调用链**：公共声明 → SQL 尝试 → 未接通/静默 fallback/错误结果。
- **根因/证据**：文档未绑定 capability/test；`vector-search.md:8,217,250-253,317-341,381`; `ai-functions.md:8-26,321-353,638-839,884-889`。
- **修复/依赖**：本轮只记录；后续标 GA/Preview/Experimental/Design 并接 public-entry conformance tests。
- **验收**：每个公开示例同版本自动测试；未闭环能力不使用“支持/自动/透明”等完成态措辞。

## 6. 2026-08-28 文档漂移建议

`docs/architecture/ai-native.md` 开头声明为目标架构，但大量代码片段和模块表仍容易被当成
现状。后续应逐节标 `Current / Target / Gap` 并链接能力账本。

公共 SQL 文档把 native HNSW/IVFPQ、自动 ANN/fallback、自动 Hybrid RRF、Cross-Encoder/
Cascade、INSERT 自动 embedding，以及全部 AI 函数、多模态、ALTER model、SYNC/ASYNC、
backfill/cancel/status 和 FILE 自动属性写成已提供能力。本轮遵守边界，未修改公共文档；
Phase 0 应建立文档 conformance：只有 public-entry E2E 为 PASS 才能使用完成态表述。

## 7. Snowflake 官方产品闭环基线

### 7.1 AI/Search 产品闭环

仅使用截至审计时可访问的 Snowflake 官方一手资料。对标重点是对象、权限、生命周期、成本和
评估闭环，不是复制 API。

| 能力域 | Snowflake 当前闭环 | Pangrova 差距 |
|---|---|---|
| AI SQL | complete/classify/filter/aggregate/embed/extract 等文本、图像、文档函数和计费 token | factory 存在但 SQL→native 未接通，类型/usage 缺失 |
| Model/RBAC | `SNOWFLAKE.MODELS` 对象，account/per-function privilege 与 model RBAC | model 是字符串/fallback；无对象 lifecycle/RBAC |
| Search Service | schema object，vector+keyword+semantic rerank；source query、target lag、refresh、PK、suspend/resume、cost | 只有底层组件，无 service object/SLA/freshness |
| Observability | 调用 trace/span 记录 token、latency、model 和 error，并以独立权限保护敏感内容 | 无统一 AI trace 对象/敏感内容权限 |
| Evaluation | 模型和 Search 服务可通过可重复数据集、指标和运行记录做版本比较 | 只有局部 sampler，缺统一 regression gate |
| Guardrail | AI 输入输出由受治理的 policy 层处理并记录使用 | 普通 `AI_GUARD` 可绕过 |
| Budget | Search 支持 tag-based monthly budgets/threshold actions；官方明确执行可延迟 2–8 小时 | 无 AI/Search ledger/admission |

官方资料：

- [AI SQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [AI SQL privileges and model access](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access)
- [Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Cortex Search resource budgets](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-resource-budgets)
- [Cortex AI Guardrails](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-ai-guardrails)

### 7.2 Snowflake、Spark 与 Apache Gluten 的分布式执行启示

Snowflake 公开架构把 cloud services 的认证、元数据、优化和 query dispatch 与独立 virtual
warehouse compute 分开；warehouse 过载时先排队，多集群 warehouse 用额外 cluster 扩展并发，
Query Acceleration Service 只把可并行的 plan 部分租给有成本上限的共享计算。Cortex Search 又把
`INDEXING` 与 `SERVING` 做成可独立 suspend/resume 的状态面：warehouse 执行 source query、
embedding 和 refresh，独立 serving compute 提供低延迟查询；`TARGET_LAG`、primary-key 增量刷新
与周期 compact/full rebuild 构成 freshness contract。这说明搜索不是 Coordinator 内的函数，
而是有独立资源、状态、成本和发布边界的持久化服务。

Spark 的 Driver/SparkContext 构造 stage DAG、申请 executor 并派发 partition task，executor 执行
数据处理并持有 shuffle/cache。Apache Gluten 沿用这套分布式控制流，把 Spark physical plan 转为
Substrait，在 executor 内经 JNI 构造 Native operator pipeline；数据以 columnar batch 返回，shuffle
保持列式，Native memory 仍受 Spark task/executor 的统一资源管理。Gluten 暴露 Native metrics、
spill、split preload、soft affinity、per-task memory isolation 与 remote shuffle，但它为兼容 Spark
保留 Row/Columnar 转换和 JVM fallback。Pangrova 是新系统，不复制这条兼容双路径：能力在调度前
完成验证，不支持就 fail closed；需要精确降级时生成另一个显式的 Native exact plan，并在结果中
标记 `FALLBACK_EXACT`，绝不退回 JVM 数据执行。

由此固定 Pangrova 的部署拓扑和职责：

1. JVM Coordinator 只负责 SQL/metadata、DAG、placement、lease/fence、admission 和最终状态，
   不扫描数据、不承载向量、不做 Top-K/RRF/Linear 融合。
2. Local Top-K 在持有 shard/artifact 的 Native Worker 产生；global merge 是一个或多级 Native
   reduce stage。Coordinator 只按通用 TaskRequest 调度 reducer，并验证 coverage、snapshot、
   generation 与 completion status。
3. 所有 query、index、embedding、feature 和 evaluation 任务共享同一 Substrait + TaskRequest +
   OperatorBinding + CapabilityDescriptor 协议；不存在 vector/search 专用控制协议或第二套 JNI。
4. 数据交换保持 Native columnar：同 Worker pipeline 直接传 batch，跨 Worker 使用有界、可背压、
   可 spill 的 shuffle；Coordinator 永远不在数据面中转。
5. 调度单位是带 resource grant 的 stage/task。placement 同时匹配 capability signature、artifact
   generation、source snapshot、CPU/SIMD、Native memory、provider locality 和失败域；运行时 usage
   必须回写并与 grant 对账。
6. Indexing 与 serving 使用独立队列/资源池和状态机。增量 segment 先写 immutable staging，经过
   checksum/coverage 验证后原子切换 generation；compaction 和 full rebuild 不阻塞 active generation。
7. 分布式 reducer 使用有界 fan-in tree，避免单节点候选量随 shard 数线性失控；任何 shard 缺失
   默认终止结果，显式 partial policy 才能返回带精确 coverage 的 `PARTIAL`。
8. Native cache key 必须包含 tenant/policy、source snapshot、artifact generation、model exact version
   和 capability signature；soft affinity 只能优化 placement，不能改变正确性或 fence。

直接采用这些原则而不复制兼容层，Pangrova 的关键路径固定为：

```text
JVM plan/control -> generic TaskRequest -> Worker JNI -> Native pipeline
  -> Native columnar shuffle -> Native reduction/result -> JVM status/catalog commit
```

官方一手资料：

- [Snowflake key concepts and architecture](https://docs.snowflake.com/en/user-guide/intro-key-concepts)
- [Snowflake multi-cluster warehouses](https://docs.snowflake.com/en/user-guide/warehouses-multicluster)
- [Snowflake Query Acceleration Service](https://docs.snowflake.com/en/user-guide/query-acceleration-service)
- [Cortex Search indexing and serving states](https://docs.snowflake.com/en/sql-reference/sql/alter-cortex-search)
- [Cortex Search refresh and primary-key compaction](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Spark cluster mode overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Spark stage-level scheduling on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes)
- [Apache Gluten architecture](https://github.com/apache/gluten/blob/main/README.md)
- [Apache Gluten Velox execution, spill and remote shuffle](https://github.com/apache/gluten/blob/main/docs/get-started/Velox.md)
- [Apache Gluten memory, preload and cache controls](https://github.com/apache/gluten/blob/main/docs/velox-configuration.md)

## 8. 目标接口与架构规格

### 8.1 AI Capability Descriptor

```text
AiCapabilityDescriptor
  id, name, version, kind
  arguments[], returnType
  executionMode: NATIVE | WORKER_CALLBACK | COORDINATOR_SERVICE
  modalities[], batchSupport, streamingSupport
  deterministic, sideEffect, retryPolicy
  requiredPrivileges[], requiredModelCapabilities[]
  protocolVersion, implementationDigest
```

descriptor 是 operator、planner、worker 和 native 的单一事实源；实现注册成功后才能上报。
plan 携带 id/version，不匹配在执行前失败。rolling upgrade 使用最小公共能力或显式 placement。

### 8.2 MODEL 与统一 Invocation Contract

```text
MODEL
  objectId, schema, name, owner
  provider, providerModelId, version, lifecycle
  capabilities[], modalities[], input/output schema
  embeddingDimension, distanceMetric
  endpointRef, credentialRef, region, residencyPolicy
  timeout, retryClass, rateLimit, priceMetadata, policyTags

AiInvocationRequest
  invocationId, capabilityId/version, modelObjectId/version
  tenant, principal, roles, queryId, traceContext
  typedInputs[], batchItemIds[], idempotencyKey
  timeout, retryBudget, outputSchema, policyContext

AiInvocationResult
  typedOutputs[], itemStatuses[], providerRequestId
  finishReason, warnings[], usage, modelVersion

AiUsage
  inputTokens?, outputTokens?, cachedTokens?, mediaUnits?
  requests, providerCost?, currency?, source: REPORTED|ESTIMATED|UNKNOWN
```

所有调用强制执行：

```text
resolve exact model -> authorize capability/model/data
  -> input/file/output policy -> estimate/reserve budget
  -> execute idempotently -> validate typed/batch result
  -> reconcile usage -> audit/trace/lineage -> output policy
```

未知 usage 标为 `UNKNOWN`，禁止记 0。

### 8.3 Embedding 与 Search 结果

- Embedding 返回 `VECTOR<F32,N>`；N 来自 MODEL 并与列约束一致。
- batch 每项有 stable id；数量相等、id 唯一、顺序可重建、维度一致；默认原子失败。
- SYNC 在事务提交前验证；ASYNC/backfill 以 source row version + model version 幂等。

```text
SearchResultEnvelope
  status: COMPLETE | PARTIAL | FALLBACK_EXACT
  rows[] {rowId, score {value, direction, stage}, source}
  expectedShards, respondedShards, coverage
  sourceSnapshot, indexSnapshot, indexFreshness
  failedShards[], warnings[], fallbackReason?
```

默认只允许 COMPLETE 或同一 source snapshot 上完成的 FALLBACK_EXACT。PARTIAL 必须显式
opt-in，并在 JDBC/REST metadata 和 audit 可见。

### 8.4 Durable Backfill/Refresh

```text
CREATED -> LEASED -> RUNNING -> COMMITTING -> SUCCEEDED
                         |             |
                         +-> RETRY_WAIT+
                         +-> FAILED / CANCELLED
```

持久化 job/object/model/source snapshot、partition、lease owner/expiry、attempt、idempotency key、
checkpoint、staged manifest、error class 和时间。全部 partition 验证后原子发布；过期 lease 不得提交。

### 8.5 基础设施对象边界

- `SEARCH SERVICE`：source query/PK/vector/text/model/index/refresh/snapshot/freshness/rerank/RBAC/budget/suspend。
- `EVALUATION DATASET/RUN`：immutable inputs/evidence、model/index versions、metrics、traces、comparison/release gate。

## 9. 分阶段整改

### Phase 0：正确性与测试真实性

接通/禁用 AI UDF capability；删除错误模型 fallback；修 BM25 JNI、hybrid protocol/score、partial
Top-K 和 FILE 安全；修 Testcontainers BOM、planner/worker compile、proto drift；重写真 E2E 和
同步公共文档。**退出门槛**：P0 全关，JDK17 下 Java/native/JNI/E2E PASS，故障注入能使测试 FAIL。

### Phase 1：统一模型调用与治理

MODEL catalog、credential、Capability Descriptor、Invocation/Usage、RBAC/guardrail/budget/audit/
trace，以及一等 VECTOR embedding。**退出门槛**：无 gateway 外 provider caller，每次调用的
权限、usage/request id/error/lineage 可查询。

### Phase 2：Search Service 与 Durable Embedding

Search Service object、vector+BM25、typed rerank、完整性 envelope、durable/atomic backfill/refresh、
索引 capability/quality/fallback。**退出门槛**：重启、超时、stale、缺 shard、fallback 和升级语义确定。

### Phase 3：基础设施评估与自动优化收口

收口 Semantic Cache、retrieval quality、index lifecycle、layout/cardinality feedback、Feature Store 和
training data service；建立模型、Embedding、Search 的 evaluation dataset/run 和 release gate。
**退出门槛**：质量、成本、安全和索引 freshness 可重复回归，自动动作可审计、可取消、可回滚。

## 10. 2026-08-28 历史实测记录

以下命令、环境和结果按原审计时点保留，用于解释当时结论，不是 2026-09-19 的当前 gate
状态。尤其是旧记录中的 Lance `SKIPPED`、JDK21 和 Maven `BLOCKED`，只可由第 0 节列出的同范围
新证据覆盖，不得删去或改写成从未发生。

### 10.1 历史环境

| 项目 | 结果 |
|---|---|
| OS/arch | macOS arm64 |
| Java | JDK 21.0.12；项目要求 JDK17，当前环境不合规 |
| Maven | 3.9.16 |
| CMake | 4.4.2，C++20 Release build |
| Docker | daemon 29.5.3；提升 sandbox 权限后可访问 |
| Native package | 本轮 Release rebuild 后已打包到 worker resources |

### 10.2 历史结果

| 状态 | 命令/范围 | 结果与根因 |
|---|---|---|
| PASS | `.pangrova/setup.sh --check` | adapters 校验通过 |
| PASS | `check-spi-annotations.sh` | 141 annotated，baseline gap 0 |
| PASS | `check-spi-stability.sh` | 19 stable SPI implementors 通过 |
| PASS | `detect-dead-stubs.sh` | 脚本通过；不推翻人工发现的 Experimental Lance stub |
| PASS | `check-doc-class-refs.sh` | architecture docs 中 150 个 class reference 全部存在 |
| PASS | `check-doc-staleness.sh` | 21 个 architecture docs 均在 60 天 freshness threshold 内 |
| FAIL | `proto-descriptor-diff.sh` | Java/native descriptor 均与 golden 漂移 |
| FAIL | Maven AI 专项，默认 `testcontainers.version=2.0.5` | reactor 中 10 个旧 artifact 缺 version，2.0 BOM 坐标不兼容 |
| PASS | 命令行覆盖 `-Dtestcontainers.version=1.21.3`：SPI | 90 tests；JDK21 release17，不等同 JDK17 合规 |
| PASS | 同覆盖：parser | 9 tests |
| PASS | 同覆盖：protocol vector wiring | 8 tests |
| PASS | 同覆盖：indexing vector/BM25 | 68 tests |
| PASS | 同覆盖：streaming embedding | 9 tests |
| FAIL | 同覆盖：planner | `PhysicalJoinConverter.java:196` 的 `JoinRelType` switch 非穷尽，production compile FAIL |
| FAIL | 同覆盖：worker | production compile PASS；`WorkerTracingFilterTest.java:125,165,207` 缺 Jakarta REST 4 methods，testCompile FAIL |
| BLOCKED | coordinator/worker JNI/AI E2E/Smoke | 上游 planner/worker baseline 无法编译 |
| PASS | `cmake --build pangrova-worker-native/build --parallel 4` | Release library/test binary 构建通过 |
| PASS | filtered CTest：VectorDistance/UDF registry/capability/Lance/runtime 等 | 45 selected：44 PASS、1 SKIPPED、0 FAIL |
| SKIPPED | Lance selected CTest | dependency/runtime unavailable，不计通过 |
| BLOCKED | DiskANN/BM25 native CTest | inventory 无对应测试名；符号存在不等于链路通过 |
| PASS | `pangrova-worker-native/bin/package-native-lib.sh` | packaged dylib 生成成功 |
| PASS | Docker daemon probe | server 29.5.3 可访问 |
| BLOCKED | Ollama/TestContainers E2E | Java reactor compile 先失败；未用组件直连/宽松断言替代 |
| SKIPPED | 完整八步全仓回归、ASan | 本轮明确不执行，生产修复阶段强制 |

PASS 只代表所列范围。JDK21 `--release 17` 不等于 JDK17 验证；native 44 PASS 不证明
SQL→planner→protocol→JNI→provider E2E；`assumeTrue`、常量真、runtime unavailable 可接受和
factory 直连均不计 E2E。Docker 可用也不能消除上游 compile 的 BLOCKED。本轮未修改任何
POM、编译错误、协议 golden 或生产实现；命令行版本覆盖不是修复。

### 10.3 历史命令记录

以下为本轮 gate 和专项测试的执行形式；Maven override 只在命令行生效，没有写入 POM：

```bash
.pangrova/setup.sh --check
.pangrova/scripts/check-spi-annotations.sh
.pangrova/scripts/check-spi-stability.sh
.pangrova/scripts/detect-dead-stubs.sh
.pangrova/scripts/check-doc-class-refs.sh
.pangrova/scripts/check-doc-staleness.sh
.pangrova/scripts/proto-descriptor-diff.sh

mvn -Dtestcontainers.version=1.21.3 -pl pangrova-spi -Dtest='*Vector*' test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-parser -Dtest='*Vector*' test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-protocol -Dtest='VectorSearchFieldWiringTest' test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-indexing -Dtest='*Vector*,*Bm25*' test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-streaming -Dtest='*Embedding*' test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-planner test
mvn -Dtestcontainers.version=1.21.3 -pl pangrova-worker test

cmake --build pangrova-worker-native/build --parallel 4
ctest --test-dir pangrova-worker-native/build --output-on-failure \
  -R '(VectorDistanceTest|NativeUdfRegistryTest|NativeCommandGetCapabilitiesTest|LanceBatchReaderTest|DiskAnn|Bm25|RuntimeInitializerTest)'
pangrova-worker-native/bin/package-native-lib.sh
docker info --format '{{.ServerVersion}}'
```

### 10.4 历史关键证据路径索引

- AI SQL/provider：`pangrova-worker/src/main/java/org/pangrova/worker/ai/function/`、
  `pangrova-worker/src/main/java/org/pangrova/worker/bootstrap/phase/NativeFunctionBootstrap.java`。
- Native registry/capability：`pangrova-worker-native/src/velox/runtime/NativeEngineLifecycle.cpp`、
  `pangrova-worker-native/test/jni/NativeCommandGetCapabilitiesTest.cpp`。
- Embedding/backfill：`pangrova-coordinator/src/main/java/org/pangrova/coordinator/ai/embedding/`、
  `pangrova-worker/src/main/java/org/pangrova/worker/ai/embedding/`。
- Exact vector/ANN：`pangrova-worker-native/src/vector/`、
  `pangrova-indexing/src/main/java/org/pangrova/indexing/engine/vector/`。
- Hybrid/merge：`pangrova-worker/src/main/java/org/pangrova/worker/task/VectorSearchTaskSupport.java`、
  `pangrova-coordinator/src/main/java/org/pangrova/coordinator/query/execution/QueryVectorResultMerger.java`、
  `pangrova-coordinator/src/main/java/org/pangrova/coordinator/ai/vector/GlobalTopKMerger.java`。
- FILE/multimodal：`pangrova-worker/src/main/java/org/pangrova/worker/ai/function/`、
  `pangrova-worker/src/main/java/org/pangrova/worker/ai/embedding/provider/OpenAiCompatibleEmbeddingProvider.java`。
- Semantic Cache：`pangrova-coordinator/src/main/java/org/pangrova/coordinator/ai/cache/`、
  `pangrova-coordinator/src/main/java/org/pangrova/coordinator/query/execution/QueryPipelineExecutor.java`。
- 文档声明：`docs/architecture/ai-native.md`、`docs/sql-reference/functions/ai-functions.md`、
  `docs/sql-reference/functions/vector-search.md`。

## 11. 后续强制验收矩阵

| Gate | 必须验证 | 通过标准 |
|---|---|---|
| Static | ServiceLoader、配置消费、descriptor、协议双端、JNI ABI、caller、资源 lifecycle | 无 orphan SPI/handler；descriptor/golden、JNI symbol diff 全绿 |
| Java | parser/planner/SPI/indexing/worker/coordinator/streaming | JDK17；无宽松错误断言；边界/错误分类覆盖 |
| Native | Vector/Lance/DiskANN/UDF/BM25/Capability/JNI | Java→JNI smoke + CTest；Lance 依赖/ABI 缺失必须 FAIL，不接受 unavailable skip |
| Distributed fault | timeout/corruption/stale/retry/fallback | status/coverage/snapshot 与 oracle 一致 |
| Provider | count/dimension/order/timeout/rate-limit/usage | 无错位/部分提交；usage 来源明确 |
| Durable jobs | crash/restart/lease steal/cancel/duplicate | 幂等、可恢复、原子 publish，无 checkpoint 丢失 |
| HNSW COSINE | DDL→durable build→统一 capability/format v2→Native graph→exact rescore→分布式结果；v1 inventory/重建 | 原始 FLOAT32、非单位/零向量与 SQL cosine 语义一致；L2 与 COSINE 排序差异数据集证明真实索引命中；无 v1 双读 shim，旧代际失效并 durable rebuild 后原子 READY；详见 `ai-native-retrieval-execution-design.md` 第 15 节 |
| Security | model/RBAC/SSRF/redirect/oversize/cache isolation | 网络/执行前 fail-closed，无跨域/跨租户 |
| E2E | JDBC→planner→protocol→worker→packaged JNI→provider→result | 生产入口真实执行；断开任一 wiring、Docker/PostgreSQL/native/provider 不可用均必须 FAIL，不接受 assume/return 静默跳过 |
| Release | 八步 build pipeline + CI gates + ASan | 生产修复阶段全部通过，环境阻塞不算通过 |

## 12. 完成定义

AI Native 不能以类、SPI、AST 或单测数量验收。某项能力只有同时满足以下条件才可升级为
“已闭环”：

1. 公共入口真实可达，planner/protocol/worker/native/provider 使用同一版本化能力定义。
2. 未知模型、能力漂移、权限、预算和安全策略 fail-closed。
3. batch、分片、rerank、fallback、snapshot 和 freshness 对客户端可观测。
4. 作业、索引和服务对象具备 durable lifecycle、owner、RBAC、审计、取消和恢复。
5. 内核资源、错误、lineage、trace 和内核评估可查询并可作发布门禁；Agent 编排与
   LLM token/费用/币种统计不属于本轮保留范围。
6. JDK17、Release native、packaged JNI、Docker/provider 的 public-entry E2E 为 PASS；任何
   SKIPPED/BLOCKED 保持原状态，不折算通过。

截至 2026-09-20，统一协议、控制面/Native 数据面边界、governed invocation、coverage 校验、
Lance fail-closed 构建链和 required E2E 基础设施门禁已有第 0 节所列增量证据。整体完成定义仍未
满足：第 0.2 节的 E2E、R7、R8 与 release 证据全部补齐前，本路线图保持开放状态。
