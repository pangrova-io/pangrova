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

> 时间轴：第 0 节按日期记录增量实施状态（最新为 **2026-10-04**）；第 1–6 节和第 10 节是
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

## 0. 当前实施状态（按日期增量复证；最新为 2026-10-07）

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

### 0.25 2026-10-03 接续基线与构建真实性

当前工作区基线已由用户提交为`e92532a`，不回退该提交或任何此前更改。
ABI12首次Step0命令exit0、生成185MiB库，实际输出4个owned Lance SDK警告，
耗时4m24s，**零warning门槛为FAIL**，不能沿用脚本OK文字当通过。四个根因是
多余括号、两个隐藏借用生命周期和私有返回类型方法的过宽可见性，已精确修
sole owned patch/source，禁止allow/suppress或编辑Cargo registry。
`build-lance-ffi.sh`已增加原始编译诊断捕获、关闭ANSI着色及warning非零退出；
语法和原失败日志识别已验证；最新Step0 r2实际exit0，3m01s、185MiB、
0 warning/error，**PASS（构建）**。原Rust38完整执行exit0，38 PASS、0
failed/ignored/filtered、0 warning，运行2.32s。owned SDK首轮r4因主线遗漏
测试要求的`LANCE_CPU_THREADS=1`，8 PASS/2 FAIL，明确保留失败；按原文档显式
配置单CPU slot后r5完整10/10 PASS、0 failed/ignored/filtered、0 warning，
运行5.80s。并行编码oracle另起真实two-slot子进程，未改生产默认或削弱任何断言。
三个真实MinIO/Toxiproxy远端oracle及两处真实取消CPU owner oracle均已执行。
这些是实际writer契约验证，不是SQL/E2E或外部catalog原子发布证明。

Velox五文件ownership SDK整库实际1036/1036完成，exit0，无warning/error；
通过bash执行其record成功。直接执行record脚本的第一次命令因无执行权限
exit126，未视为通过，也未改系统权限。该五文件产物不是最终六文件输入，
DISTINCT nullable accumulator destroy补丁仍须重建并验证。不能将旧archive
重新标记为新patch来绕过构建。

六文件patch现已实际apply；在记录了五文件产物的同一archive上，record以exit1
明确拒绝`Archive is unchanged from the recorded prior patch; rebuild it`。
同一SDK target增量构建实际4/4完成（仅重新编译RowContainer并重链），exit0、
无warning/error，随后record exit0。当前patch SHA-256为
`fc8929f25c22fbef65ebfd461f1e9ec1d40921e3ffaad0b5602c301bcd821643`。
该证据只证明SDK重建及已知旧库拒绝，不是Native DISTINCT运行、ASan或E2E通过。
`.pangrova/setup.sh --check`实际PASS，两处agent adapter均有效。

JDK17的Coordinator physical receipt admission定向命令实际FAIL，停止在Worker
testCompile：旧`SimulatedTaskExecutor`和`CapturingExecutor`仍返回void release，
另有已删除的`attempt()`调用。尚未运行该目标的三个metadata admission测试；
正在迁移真实Native/RPC fixture，禁止把业务替身改回兼容签名来掩盖失败。

发现canonical `model.proto`仍残留无生产caller的`AiUsage`及result field4，
已移除message并保留field4/name的reserved，未删除`ResourceUsage`或资源准入。
首次增量生成因旧target保留`AiUsage.java`实际编译FAIL；使用正常Maven clean
并由同一protoc再生成后，descriptor/模型资源/真实response资源证据13/13 PASS、
0 failure/error/skip（11.139s）。未手改生成源。descriptor golden gate实际FAIL，
必须先逐项核查完整协议差异，不能盲目refresh。

日志：`/tmp/pangrova-production-lance-abi12-step0-r1.log`、
`/tmp/pangrova-production-velox-owner-sdk-release-r1.log`、
`/tmp/pangrova-production-velox-owner-sdk-five-record-r{1,2}.log`。
新增日志：`/tmp/pangrova-production-velox-six-apply-r1.log`、
`/tmp/pangrova-production-lance-abi12-step0-r2.log`、
`/tmp/pangrova-production-velox-six-old-five-record-r1.log`、
`/tmp/pangrova-production-velox-six-sdk-release-r1.log`、
`/tmp/pangrova-production-velox-six-new-record-r1.log`、
`/tmp/pangrova-production-physical-report-admission-nearest-r1.log`。
Rust日志：`/tmp/pangrova-production-lance-abi12-full-rust-r1.log`、
`/tmp/pangrova-production-lance-owned-sdk-full-r{4,5}.log`。
协议日志：`/tmp/pangrova-production-protocol-ledger-removal-nearest-r{1,2}.log`、
`/tmp/pangrova-production-proto-descriptor-oct3-r1.log`。

### 0.26 2026-10-03 实际写入 owner 与释放错误传播

实际调用链为Native output drain → `NativeWriteExecutor::abortWrite` → Java
`NativeWriteExecutor` → connector sink → SDK/文件IO → task `afterRelease` →
memory refund。旧链有三处真实断点：JNI `kSwallow/noexcept`、Java在清理前remove
session且只记录错误、connector quiet close/提前标记aborted。现源码统一为显式
abort及真实异常传播，不在析构抛异常；Java `NativeWriteSession`序列化append/finish/
abort，只有实际abort/close完成才移除session。失败的task cleanup保持catalog lease
与memory grant，由原cleanup sweep重试；shutdown封新session admission并try-all，
不清空失败owner。该Java生产主链compile实际31模块PASS（14.861s），非测试或E2E。

Iceberg sink将finished/aborted标记移到实际操作成功后，缓存精确prepared fragment，
所有writer与失败删除可观察；LRU已关闭的writer只保留owned path及DataFile，不保留
完整encoder。abort涵盖被淘汰分区，删除失败保留精确path供重试。Paimon adapter不再
quiet close，实际writer close失败时不先关闭commit/spill owner；不重复已完成的close。
真实Hadoop/Parquet/本地Paimon最近验证r1与r2均31/31 PASS、0 failure/error/skip
（26.045s / 18.968s），r2复证LRU只保留closed path、不保留encoder的收敛，
包含新增4项实际非空目录删除失败/恢复、130分区LRU文件清理、prepared bytes保留、
snapshot mismatch失败不得改为空metadata。它们不是跨Worker/JNI或SQL发布证明。

仍需明确：固定Paimon SDK内部`FileStoreCommitImpl.abort`使用`deleteQuietly`，
`TableCommitImpl.close`只调用`shutdownNow`；不能仅从adapter close返回推断远端
objects全部删除或后台执行器全部join。此处不能靠复制SDK abort或用本地锁证明，
须对实际SDK资源/外部catalog权威继续复证及补齐故障验收。

删除全生产创建/注册/caller检索为零的死`WriteOperator`及其第二套IPC encoder。
原14项有效value/schema/null/complex/dictionary/UTF8/row-limit/EOF断言已迁到
真实task pool与唯一`WriteBatchStream`→`CanonicalBatchStream`入口，不减少断言。
删除可由Git恢复；最新Native尚未运行这些迁移测试，不计通过。

Native完整Release r1、r2实际编译FAIL，分别是拆分后的queue alias重复forward
declaration/runtime protobuf漏include、Arrow Schema漏完整type include；均按实际
类型修复。r3实际编译FAIL，新增fixture有Arrow空container overload歧义和
RowVector漏完整类型include，正在集中修复direct依赖，未修改任何有效断言。
无全CTest/ASan/package/Java全回归/E2E PASS证据。
日志：`/tmp/pangrova-production-java-write-owner-main-compile-r1.log`、
`/tmp/pangrova-production-connector-write-cleanup-nearest-r{1,2}.log`、
`/tmp/pangrova-production-native-abi12-six-{configure,release}-r{1,2}.log`、
`/tmp/pangrova-production-native-abi12-six-release-r3.log`。

### 0.27 2026-10-03 Catalog shutdown 与协议快照复证

- 根因：`WorkerCatalogRuntime.close()` 原先在 connector shutdown 前清空 cache/task binding，
  shutdown 失败或 deployment fetch 尚未完成时仍丢失 owner；LRU eviction 与 codec/storage
  初始化失败也有同类问题。现在 admission 先封闭，活跃 task/control lease 或未完成初始化
  必须报错并保留 owner。只有实际 shutdown 返回才移除 entry；eviction 转移至受控 retiring
  集合，失败后阻止新 catalog admission，后续 close 可重试。`CatalogShutdownOwner` 对并发
  shutdown 显式 claim，不持锁执行 connector I/O。租户/revision key 提取为不可变
  `CatalogRevisionKey`，保持原精确身份与字节数组防御性复制。
- Connector 根因：Iceberg 原先仅 `shutdownNow()`，未 join scheduler 或 close catalog；Paimon
  原先吞掉 `catalog.close()` 异常。现在 Iceberg 等待真实 scheduler 退出后再 close catalog，
  超时/中断/close 失败均传播，Paimon 只在真实 catalog close 成功后清 registry。失败不标记
  closed，允许重试。此修改不证明 Paimon SDK 内部所有异步任务或 quiet-delete 已闭环。
- Java main：`mvn -Pfull-regression -pl pangrova-testing/pangrova-test-e2e -am compile`
  **PASS**，31 模块，15.691s；日志
  `/tmp/pangrova-production-java-catalog-owner-main-compile-r1.log`。这不是 testCompile/JNI/E2E。
- 真 connector 近测 r3：**33 PASS / 0 FAIL / 0 SKIPPED**，18.451s，日志
  `/tmp/pangrova-production-connector-write-cleanup-nearest-r3.log`。其中新增 2 项使用实际
  HadoopCatalog 与 connector 自有 scheduler，验证真实 join、重复 shutdown、中断传播与恢复
  后 retry。其余 31 项保留真实 Parquet/Paimon 写入、清理与读取 oracle。随后补充实际
  in-flight Future 的完成检查，r4 重跑 **33 PASS / 0 FAIL / 0 SKIPPED**，29.768s；日志
  `/tmp/pangrova-production-connector-write-cleanup-nearest-r4.log`。两轮均有第三方 yetus
  dependency effective-model warning，不能声称 Maven dependency diagnostics 为零。
- Native Release r4：**FAIL**，83% 时 14 条诊断源于 Arrow C ABI 不完整类型和
  `pangrova::memory` 遮蔽实际 Velox MemoryPool；日志
  `/tmp/pangrova-production-native-abi12-six-release-r4.log`。已明确引入 C ABI 和 pool 类型，
  保留所有生命周期 oracle；r5 随后完整 Release 构建 **PASS**，编译诊断中 0 warning/error，
  日志 `/tmp/pangrova-production-native-abi12-six-release-r5.log`。这不是 CTest/ASan/E2E 通过。
- 本次序列化 output 根因修复：Java/Native direct-result 与 shuffle producer 均要求正的
  serialized-buffer grant；同一精确 grant 传入 SDK 两个 output-buffer threshold，不能由
  session 放大。Task memory grant 也补齐到 runner（原先遗漏导致使用 node total memory）。
  背压时间仅累计实际 `blockedWaitForConsumerWallNanos`，不使用混合 I/O 的 blocked 总时间。
  SDK enqueue 后才 block，因此 threshold **不是硬 heap cap**；仍须实际 pool cap/在途 page
  overshoot 与真实大 batch/消费者暂停测试。上述新增断言仍待当前 Native 与 Java 验证。
- 协议 gate 的旧 golden 漂移已完整解码、按 file 名归一并逐项审查（2381 行 semantic diff），
  同时通过真实 Protobuf descriptor 比较：保留字段与旧 enum value 未改变；11 个删除字段
  均 reserve 原编号和名称；新增 31 message；唯一删除 message 为无人调用的 `AiUsage`。
  审查日志 `/tmp/pangrova-production-proto-semantic-review-oct3-r1.log`，failures=0。
  依仓库流程用 pinned protoc 35.1 执行 `proto-descriptor-diff.sh --update`，未手改生成源码；
  随后 gate **PASS**，日志 `/tmp/pangrova-production-proto-reviewed-golden-gate-oct3-r2.log`。
  此通过仅证明当前 canonical v2 shape 与已审 snapshot 相符，不证明 function selection、
  physical commit fence、resource drain 或整个 SQL 执行已通过。
- 三类 NativeWriteExecutor 假业务测试正在整体迁至真实部署/SDK/codec 的 lifecycle fixture，
  原 12+5 方法及 100-trial、1–100 会话 property 范围保持；公共 SQL INSERT/SELECT 单独
  验证真实 JNI。第四类 WorkerCatalogRuntime 的租户、1024 LRU、并发与初始化失败 oracle
  也迁真实 provider/HTTP。当前迁移仅源码 checkpoint，未宣称 testCompile 或 E2E 通过。

### 0.28 2026-10-03 实际全 CTest 失败账本与根因整改

- ABI12/SDK6 Release r5 最近范围 **139 PASS / 3 FAIL / 0 SKIPPED**，142 项、196ms，
  日志 `/tmp/pangrova-production-native-abi12-six-nearest-r1.log`。失败分别为 Arrow 根对象
  与叶子 buffer 的 ownership 粒度混淆、正 serialized-output grant 缺失的旧测试请求，以及
  MARK JOIN 三值逻辑。其他写流 14 项、实际 pool ownership 5 项、SUM0/RANGE/Distinct
  和写 consumer 失败后 join 等近测保留并通过；不能据此称整个执行链通过。
- 同一冻结二进制的**未过滤全 CTest FAIL**：3,645 项中 **3,190 PASS / 455 FAIL**，
  255.45s；日志 `/tmp/pangrova-production-native-abi12-six-full-ctest-r1.log`。
  失败分布：TopNRowNumber 216、RowNumber 81、Expand/GroupId 81、MergeJoin 48、
  Timestamp Precision 12、基础计划转换 7、DynamicFilter reception 6、TypedExchange 1、
  grant fixture 1、Arrow owner 1、MARK 1。不得删参数、降低有效断言或写成全量通过。
- MARK 根因：dummy-key null-aware HashJoin 的 filter 把 UNKNOWN 当 WHERE 不匹配，
  实际返回 FALSE。唯一 Velox SDK 增加显式 null-aware-marker NLJ 状态，按每 probe 行跨
  build batch 累积 `TRUE > UNKNOWN > FALSE`；构造、Builder、serde 与执行同一契约。
  任意谓词仍为 O(probe×build)，不声称 hash 等值连接的线性扩展性。sole SDK patch
  扩为 10 文件，SHA256 `393b242913ff5ffae1748e316200474be1e1828dcef8e8ac19612c945cb0eafd`；
  SDK6→10 精确 source 升级已执行，旧 archive 不允许重记为新 patch，正在实际重建。
- Arrow 复证保留原 double/null 数据，检查实际叶子 values/null bitmap 的地址 identity 和
  C array/schema 最后释放各一次；无 buffer 的根 ROW 提前释放不能替代真实叶子所有权证据。
  修订当前仅 source，尚未重新编译/运行。
- 基础计划测试按 canonical append→emit、typed key materialization/rebase 和完整 Root DFS
  标签校核，不跳过任意 Project；原 51 项测试分拆为四个小于 800 行的 TU，共用同一 fixture
  类型，无用例减少。Timestamp/DynamicFilter/RowNumber/GroupingSets/MergeJoin 继续按实际
  生产结构及值复证。TopN 另发现唯一 window 输出 Project 阻断优化及 strict INT64_MIN
  减一溢出，正在修精确 field lineage 上的优化；不是把所有失败归为断言漂移。
- 最近纯 Java output admission/watermark 范围 **18 PASS / 0 FAIL / 0 SKIPPED**，14.321s，
  日志 `/tmp/pangrova-production-output-admission-watermark-nearest-r1.log`。实际 backpressure
  等待还需新 Native `consumer_blocked_drivers`（canonical NativeTaskStats field 18）与
  JNI/Flight 验证；该新增字段需要重新审 descriptor，不能沿用上一版 golden PASS。
- Maven Yetus warning 根因：Hadoop→ZooKeeper→Yetus 0.5.0 POM 无条件依赖 JDK8 tools.jar，
  与规定 JDK17 不符。统一 dependencyManagement 为已有 SDK 家族的 0.12.0，未过滤 warning
  或改变 JDK。真实 connector 原 33 项 r5 重跑 **33 PASS / 0 FAIL / 0 SKIPPED**，22.091s，
  本轮无 Maven warning/error；日志 `/tmp/pangrova-production-connector-write-cleanup-nearest-r5.log`。
- 当前停留 Step 1/2 修复与复证；没有最新全 CTest PASS、匹配 ASan、重新打包、全 Java
  回归/覆盖率、全 E2E、ActivationSmoke 或独立进程集群/SLO 通过证据，不宣称生产完成。

### 0.29 2026-10-03 最新同源 SDK 与无锁内 I/O owner 校核

- SDK10 实际目标重建 **PASS**，410/410 步，无 warning/error，日志
  `/tmp/pangrova-production-velox-ten-sdk-release-r1.log`。旧 SDK6 archive 尝试重记新 patch
  被明确拒绝（EXPECTED FAIL），真正新 archive 才 record 成功，日志
  `/tmp/pangrova-production-velox-ten-old-six-record-r2.log` 与
  `/tmp/pangrova-production-velox-ten-new-record-r1.log`。第一次主 Native configure 在 record
  尚未结束时撞到 provenance barrier，实际 **FAIL**，保留 r1 日志；record 完成后的同命令
  r2 **PASS**，未绕开 barrier。主库与新值测试仍待重建/回归。
- canonical field 18 经实际 Java 生成类与已审 golden 比较：只新增通用
  `NativeTaskStats.consumer_blocked_drivers:uint64`，其余所有归一后的 file/field/enum/option
  全等；日志 `/tmp/pangrova-production-proto-field18-review-r2.log`。最初 JShell 诊断因方法引用
  与缺 Substrait classpath 失败，不计通过；改为 JDK17 单一 main、失败即非零退出的完整比较。
  pinned generator 更新 golden 后 gate **PASS**，日志
  `/tmp/pangrova-production-proto-field18-golden-gate-r1.log`。双端执行语义仍待新 Native/JNI。
- Java 最近同范围复跑 r2 在 reactor model 阶段 **FAIL**：新真实 shuffle-service 测试依赖
  未加入根 dependencyManagement。补唯一 `${project.version}` 管理项后，同命令 r3
  **19 PASS / 0 FAIL / 0 SKIPPED**，15.363s，日志
  `/tmp/pangrova-production-output-admission-watermark-nearest-r3.log`，含新增通用 field18 的
  type/tag/round-trip 与 live count/elapsed wait 分离验证。没有隐藏该 r2 失败。
- 写入全局 `synchronized` 原先覆盖 deployment fetch、开 sink 和 shutdown；session monitor
  也跨 append/finish/abort 实际 connector I/O，限制并发并令 close 无法及时观察 in-flight owner。
  现 `NativeWriteAdmission` 只在短 monitor 内封 admission/计数/claim，实际业务与清理锁外执行。
  session 在开真实 sink 之前发布 owner；操作独占 claim 在 actual finally 才归还。close 遇
  initialization/append/finish/abort 尚未退出必须失败并保 owner，后续 cleanup retry 不提前退款。
  shutdown 拒新 admission，bind 在封闭竞态中成功获取的 catalog lease 真实释放后才退出。
  空 taskId 必须在任何 task cleanup 前拒绝，不能借 null 意外清空全部 sessions。
- 此写入版本 main compile **PASS**，含真实 shuffle-service 的 32 模块，16.390s，日志
  `/tmp/pangrova-production-java-write-admission-main-compile-r1.log`。随后纯生产 admission
  源码与原包 JUnit 测试用 JDK17 `javac -Xlint:all -Werror` 编译，并用 pinned JUnit 6.1.3
  Launcher 跑 **7 PASS / 0 FAIL / 0 SKIPPED/ABORTED**，62ms；日志
  `/tmp/pangrova-production-write-admission-junit-compile-r1.log` 与
  `/tmp/pangrova-production-write-admission-junit-nearest-r1.log`。这是不加载 JNI 的状态机近测，
  **不是 Maven 全 testCompile/全回归或 HTTP/storage/E2E 通过**。
- 新真实 HTTP bind/close barrier、两 task 并行 Parquet 写入、shuffle RPC/pump 最后退出，
  以及 canonical typed failure first-evidence 传播均已落源码；业务 ACK 不由测试替身生成。
  原生命周期与参数覆盖保留；真实服务测试未运行，不计通过。
- Fetch/检索/Rerank 的 partial→final TopN/Limit 原缺实际 pipeline gather，Velox 因 final 的
  `requiresSingleThread` 把其 source pipeline 一并压至单 driver。正在补唯一 Native
  `LocalPartitionNode::gather` 边界与精确排序/输出恢复。此修复仅解除结构限制，不是实测
  扫描并发、跨机器吞吐或 SLO 达标证明；计算排序项静默丢弃也在同一最小单元整改中。

### 0.30 2026-10-03 最新冻结源码与实际传输缺口

- 同源 Lance ABI12 Step 0 r3 **PASS，0 warning/error，4m38s**，日志
  `/tmp/pangrova-production-lance-abi12-step0-r3.log`。随后 SDK10 provenance 校验的主
  Native Release configure r3 **PASS**，日志
  `/tmp/pangrova-production-native-abi12-ten-configure-r3.log`。主 Release r1 **FAIL** 于
  `ColumnarImportOwnershipTest` 的两个 `Buffer::data()` 调用；pinned SDK 并无该 API。
  查真实 `FlatVector::rawValues` / `BaseVector::rawNulls` 后修正调用，原实际指针共享、
  NULL、值与 C Data root/child 恰好一次释放断言全部保留。r2 **FAIL** 于 RowNumber 新测试
  引用不存在的 `findNode`；改为精确的 Project→RowNumber→LocalPartition→Project 类型路径，
  保留原 162 个参数用例。r3、r4 原命令全范围重建均 **PASS、0 compiler warning/error**；
  `/tmp/pangrova-production-native-abi12-ten-release-r{1,2,3,4}.log`，失败历史不抹除。
- 32 模块 main compile r2 **PASS，16.540s**，日志
  `/tmp/pangrova-production-java-write-admission-main-compile-r2.log`。该命令没有运行
  testCompile/JNI/服务测试，且不覆盖随后新增的连接池生命周期更改。
- 新 Sort/Fetch 单元已冻结：真实计算排序键只物化一次，保完整输出 schema、NULL
  与四种普通排序方向；缺失/未知/clustered/custom comparator 明确拒绝。TopN、Limit、
  retrieval 与 rerank 使用真实 gather，新增实际 Values Task 结果验证。原测试没有删减；
  定向 r1 为 **1332 discovered / 1327 PASS / 5 FAIL**：四方向测试将 canonical
  arithmetic.add 的实际 `plus` 实现名错误断言为 `add`，Exchange 用例遗漏真实字段身份
  Project。查唯一函数声明与实际转换后修正精确断言，不删任何值/NULL/schema/方向覆盖。
  原命令 r2 **1332/1332 PASS、508ms**，日志
  `/tmp/pangrova-production-native-abi12-ten-nearest-r{1,2}.log`。旧 ownership/write/window
  近测同范围现发现 150 个用例，r1 **146 PASS / 4 FAIL**（同一 Sort 名称断言），r2
  **150/150 PASS、201ms**，日志
  `/tmp/pangrova-production-native-abi12-ten-ownership-nearest-r{1,2}.log`。
  两范围有重叠，不累加为独立总数；这些实际 Task 近测不能推导独立进程扩展性。
- 主完整 CTest r1 **3840 discovered / 3839 PASS / 1 FAIL、270.79s、exit 8**，原未过滤
  命令 `ctest --test-dir pangrova-worker-native/build --output-on-failure --parallel 2`，日志
  `/tmp/pangrova-production-native-abi12-ten-full-ctest-r1.log`。剩余失败
  `NativeCommandExchangeTest.PropagatesStructuredExternalFailure` 仍从 message 字符串寻找
  code/message 标签，而新生产路径在 ingress 唤醒前将完整 canonical `ExecutionError`
  记录到 TaskLifecycle。原无效 failure 拒绝用例保留，新增验证拒绝后无错误 owner/ingress
  仍 pending、接受后完整 class/code/message/retryable/component 全等、实际 FAILED status
  序列化仍全等；不恢复字符串标签解析。修复尚待重建及原完整命令复跑，不计 PASS。
- 上述 canonical error 用例在 Release r5 **PASS、0 compiler warning/error** 后，command/evidence
  近测 **15/15 PASS、58ms**，两个原定向范围 r3 分别 **1332/1332 PASS、508ms** 与
  **150/150 PASS、235ms**。随后相同未过滤完整 CTest r2 **3840/3840 PASS、277.53s**，日志
  `/tmp/pangrova-production-native-abi12-ten-full-ctest-r2.log`。这是该冻结 Native 源码的 Step 2
  证据，不代替 ASan、JNI、SQL、多进程或生产规模验收。
- 32 模块 JDK17 `mvn -Pfull-regression -pl pangrova-testing/pangrova-test-e2e -am clean
  test-compile` **PASS、54.621s**，日志
  `/tmp/pangrova-production-real-fixture-whole-chain-test-compile-r2.log`。clean 清除已删测试类和
  已迁移 Flyway 资源的旧输出；此命令没有执行测试。该轮仍有 ANTLR `SORT` implicit-token
  warning，根因不是可忽略的噪声：`ALTER TABLE ... SET SORT ORDER` 实际不可解析。新增
  真实 SQL 近测 r1 **7 discovered / 3 PASS / 4 ERROR / 0 SKIPPED**，日志
  `/tmp/pangrova-production-parser-sort-token-nearest-r1.log`。补唯一 case-insensitive lexer token
  并保留 `sort` 非保留标识符后，同命令 r2 **7/7 PASS、5.099s、0 warning**，涵盖大小写、
  方向/列序、ZORDER、标识符、打印往返与非法列清单；这不是 catalog mutation/SQL JNI 验收。
- 同一 SORT 修订的 parser 全模块及依赖回归 **1456 PASS、0 failure/error/skip、5.596s**
  （common 237、SPI 701、parser 518），日志
  `/tmp/pangrova-production-parser-sort-token-full-r1.log`。连接生命周期最近纯 Java 检查
  **24/24 PASS、46.831s**（SPI 控制契约 3、实际连接池 11、session/pump 退出状态机 3、
  write admission 7），日志 `/tmp/pangrova-production-java-lease-admission-nearest-r1.log`。
  session 清理现仅在 binding 与 pump 实际 join 成功后关闭 source；join 失败保留资源
  owner，归还失败的 lease 不再允许 client 访问，但可重试清理。这些近测不替代实际
  reader/RPC/TLS/未退出 pump 联合 E2E，也不证明连接数硬上限。
- callback determinism 已从唯一声明生成到 canonical descriptor、JNI、Native spec 和
  Velox 两种注册入口；同名重载标记冲突在发布前拒绝。生成器 **54/54 PASS**，Java
  描述符/bridge/bootstrap **47/47 PASS、23.643s、0 warning/skip**；日志
  `/tmp/pangrova-production-determinism-{generator,java-nearest}-r1.log`。主 Release
  `/tmp/pangrova-production-native-determinism-release-r1.log` **PASS、0 compiler warning/error**；
  最近 Native CTest **85/85 PASS、31.19s**，包括真实 stateful/stateless factory metadata
  与启用 constant folding 的 FILE_FROM_URL 表达式编译，日志
  `/tmp/pangrova-production-native-determinism-nearest-r1.log`。未调用模型/JNI/provider；
  该冻结单元的未过滤完整 CTest 后续结果为 3846/3846 PASS，见 0.31；不覆盖新 Arrow 原子迁移。
- matching-ASan SDK 依赖准备 r1 **FAIL**：PATH 未含已存在 Ninja；r2 **FAIL**：ASan
  bootstrap 缺少 Release 使用的 macOS OpenSSL/Flex prefix；r3 **FAIL**：命中系统
  Bison 2.3 而非已存在的 3.8.2。已按实际 Release cache 补平台依赖和工具路径，
  Protobuf/Abseil 仍严格选独立 ASan prefix，不加入 Release native-deps。r4 当前构建中；
  该轮还暴露 SDK Arrow ExternalProject 的 prefix 分隔与第三方配置 warnings，仍需核查，
  不记为零警告/ASan PASS。日志 `/tmp/pangrova-production-native-abi12-ten-asan-sdk-setup-r{1,2,3,4}.log`。
  同时限制嵌套 CMake/Make 的并行度，不能仅用外层 Ninja jobs 声称限制了所有编译进程。
- E2E truthfulness 与 BOM usage 静态 gate **PASS**，日志
  `/tmp/pangrova-production-e2e-truthfulness-r1.log` 与
  `/tmp/pangrova-production-bom-usage-r1.log`。静态 gate 不是运行时闭环证据。
- 真实 External Shuffle 用例明确保留 `BATCH_SERDE_ARROW_BATCH`：官方 Arrow 编码、
  真实服务/权限/双 chunk、Native Exchange 与 Flight 结果必须同时复证。源码审查发现
  `SubstraitRelConversion.cpp::resolveExchangeSerde` 仍拒绝 Arrow，只接受旧 Native row
  serde；现有 `TypedExchangePlanConversionTest` 也记录该拒绝。这条 Arrow→JNI 链当前
  **未接通、未运行**，不得改回旧 serde 或将源码 fixture 记为 E2E PASS。后续修复必须
  同步 planner/stage、producer、consumer、schema/压缩/分批边界及真实结果测试。
- Shuffle reader 的实际 RPC/pump drain 与 gRPC channel drain 是不同证据。旧连接池
  仅异步 eviction/invalidate，旧 client 在 5s await 超时后只调用 shutdownNow；均不能
  证明线程/资源真正退出。正在补 active borrower lease、封闭准入、实际 channel join、
  失败 owner 保留与可重试清理；不将 cache 清空或 cancel 信号当作释放完成。

当前 determinism 修订后的主 Native 全 CTest 已通过；匹配 ASan、打包、全 Java 回归/覆盖率、全 E2E、
真实 ActivationSmoke 与独立多进程集群/SLO 验收，不宣称生产级完成。

### 0.31 2026-10-03 确定性复归与唯一 Arrow adapter 追加解码

determinism 冻结修订的未过滤 CTest 实际 **3846/3846 PASS、284.30s**，日志
`/tmp/pangrova-production-native-determinism-full-ctest-r1.log`。这是此前 0.30 的后续
结果，不是新增 Arrow 修订或全部生产链的通过证据。

查 pinned SDK `Exchange::getOutputFromColumnarPages` 与 `VectorSerde` 六参数契约后，
先添加真实 Arrow stream/SDK API 复现，**9 discovered / 5 PASS / 4 FAIL、9.04s**，
日志 `/tmp/pangrova-production-arrow-append-repro-test-r1.log`。有效两页追加、多 batch
stream、prefix offset 与零列行数均落入 SDK 未实现的 decode；五个异常用例当时只能
证明 generic unsupported 拒绝，不能证明正确失败原因。

唯一 `ArrowBatchVectorSerde` 已从 result 移至 `velox/serde`，全部实际 registration 与
原测试 caller 迁移，不保留旧实现或 forwarding 类。Native 依赖规则只放开 shuffle
到该公开 serde 边界，不放开 result/executor ownership。六参数 decode 显式支持
append，验证非负/已有 prefix/精确 schema/checked total rows，完整 decode 和 copy
成功后才发布新 result，不覆写仍由上游持有的旧输出。C Data 使用原 RAII slots 交给
SDK，不先将 release callbacks 从原 guard 清空；分配或转换失败仍有 exact owner。

同命令最近 r2 **9/9 PASS、8.60s**。进一步将所有错误测试收敛为明确 reason oracle，
新增旧 prefix schema 不一致、offset=0 replacement 不修改已发布 owner、Unicode/
长字符串/嵌套数组/NULL 在两个 wire page 销毁后保持真实值；最近 r3 **12/12 PASS、
9.00s**，日志 `/tmp/pangrova-production-arrow-append-nearest-r{2,3}.log`。实际 owner/
type/schema/原 252 参数 roundtrip 最近范围 **293/293 PASS、31ms**，日志
`/tmp/pangrova-production-arrow-append-owner-type-nearest-r1.log`；此范围包含上述 12 项，
不能重复累计。两次 Release rebuild **PASS、0 compiler warning/error**，日志
`/tmp/pangrova-production-arrow-append-build-r{2,3}.log`；该 adapter 冻结版本的未过滤
CTest **3858/3858 PASS、290.89s**，日志 `/tmp/pangrova-production-arrow-append-full-ctest-r1.log`。

这些证据只关闭 shared adapter 的 SDK append 缺口。生产 planner/producer/consumer
尚未原子迁移为唯一 Arrow，标准压缩/精确 identity schema/Native I/O last-exit 与
真实 external service/JNI/Flight 验收仍未完成，状态仍 **NOT_CONNECTED**。必要 append
copy 在 task allocator 下发生，但全边界 copy bytes/CPU/SLO 未测，不能宣称零拷贝或
性能优势。具体原子迁移规格见 [唯一路径设计](ai-native-single-path-root-cause-design.md)。

完整八步、全 Java 覆盖率、独立多进程故障恢复及生产规模验收仍未完成，不宣称生产级。

### 0.32 2026-10-03 唯一 Exchange 原子迁移：源码落地与新增根因

当前正在修改新的原子单元，**0.31 的 3858 PASS 不覆盖本节改动**；未执行的测试、
新匹配 ASan、打包、Java 全回归和 public SQL E2E 不记 PASS。

- Canonical v2 保留 Arrow 枚举值 2，旧值 1/name 预留；Java planner/stage/REST/dispatch
  逐层只接收同一格式、精确小写 `none/lz4/zstd` 与正 signed-int 行数上限。Native 原先
  丢弃 serde/codec/batch_size、只让 remote `NONE` 准入、再用 session/default 覆写 codec；
  本单元显式保存并传入真实 runner/serializer，session 不再选择另一 wire codec。
- shared serde 消费实际 SDK Options，以标准 Arrow IPC buffer compression 映射 `lz4`
  到 LZ4_FRAME、`zstd` 到 ZSTD，无私有帧或双重页面压缩；writer/reader 关闭 Arrow
  process-global CPU pool，使用 task allocator。输出 stream 每次 Write 前检查 wire ceiling。
  Shuffle 删除持有 raw pool 的 thread-local serializer，真实 complete page 受 4 MiB 与
  admitted batch_size 双约束；未知 codec 和格式明确拒绝。首轮专项实测暴露依赖构建缺口：
  当前选中的 Arrow 包 `ARROW_WITH_LZ4/ZSTD=OFF`，压缩 6 项 FAIL，不接受 codec-unavailable
  跳过。已修改 bootstrap 打开两种标准 codec，Native configure 明确拒绝缺 codec 的依赖包；
  重建与安装后的同 scope 复证仍待完成。
- Native typed Exchange 直接使用 `$pangrova$column_N` ordinal physical names，逻辑
  schema 仍来自唯一 relation layout；允许显式零列 schema，拒绝未绑定 schema。Java
  compile 按拓扑冻结实际 producer NamedStruct 并绑定 typed consumer，replay 仅严格校验。
  Root presentation labels 不参与 wire 重命名。
- 深查固定 SDK 确认 `PartitionedOutput.cpp` 两处只接受旧 serde，实际调用的是 rows
  estimate/iterative append，完成统计 `VectorSerde::kindByName` 也未识别 ArrowBatch。
  组件 serialize/deserialize 通过无法关闭这些接缝。新增 generic rows/ranges 接口和
  sparse/duplicate/NULL、nonzero range size-pointer canary oracle；实际 SDK Task 测试与
  首轮 3 个实际 SDK Task/kind 测试全部 FAIL；已纳入唯一固定 SDK 13 文件补丁。Release
  SDK 已实际重建并记录 provenance；matching-ASan SDK 正在重建。构建期间不修改其
  source/patch cohort；主 Native 最近测试仍须使用新 archive 和 codecs 重新执行。
- 新追加解码存在源码确定的 prefix 累积复制风险：SDK 逐 page 调 append，本实现每页
  copy 全 prefix，N 小页可产生平方级复制。共享已发布输出必须保持不变，不能用不安全
  原地 resize 冒充优化；真实 copy bytes/CPU、task cap 和吞吐实测仍待闭环。
- 原旧 serde 的 lossless TIMESTAMP 路径删除后，默认 Arrow 纳秒表示会在 2263/9999
  等合法远期时间溢出（固定 SDK `Timestamp::toNanos` checked multiplication）。这是
  **待修复的语义缺口**，不能根据数据值选 unit、静默转微秒或削减原精度。统一逻辑精度/
  范围到标准 Arrow 类型或公开 extension/storage 的契约需独立原子实现，并验负 epoch、
  纳秒余数、远期、NULL、嵌套与 sliced values；现测试数量不证明已解决。
- Worker-wide channel admission 源码已冻结：ingress/egress 共用 owned+creating credits，
  live borrower/retired/failed join 不退款，producer 所有 endpoint 在 JNI 前预准入，精确
  typed ADMISSION report 与可重试 partial-bootstrap owner 清理已补。新增 16 个 source
  test，原 Pool 11/TLS 5 保留。尚未执行，且 ManagedChannel 上限不等于 OS TCP/FD/RPC 上限。
- Paimon 1.4.1 最终 commit 的前检查→SDK commit→后检查是实际 TOCTOU；fingerprint
  去重、OCC、catalog lock 都不是 execution-owner fence。仅只读调研，尚未修复。
  后续须由同一资源端事务验证 anchor/当前 authority、不可变 intent/winners、表 generation
  和 parent head，再发布 SDK snapshot/journal/outbox；不能持 PG 锁包外部 FS rename。
  latest/history/CDC/expire 及 APPEND+COMPACT 多 snapshot 必须共用 authority，不能只
  改 latest 指针即称全部原子提交。该跨 catalog 单元的真实 ACK-loss/接管/迟到 I/O
  验收待实现，Native PASS 不覆盖它。

本节只记录当前源码和可定位根因，不宣称 Exchange、分布式写入或生产级验收完成。

当前新增实测账本：

| 状态 | 范围 | 实际结果 / 证据 |
|---|---|---|
| PASS | Rust Step 0 | `/tmp/pangrova-production-arrow-exchange-step0-r1.log`；0 编译警告 |
| PASS | Native 首轮 Release 编译 | `/tmp/pangrova-production-arrow-exchange-native-repro-build-r1.log`；0 编译警告，使用原 10 文件 SDK，不证明新 SDK 闭环 |
| FAIL | Arrow/配置/实际 SDK Task 最近 CTest | `/tmp/pangrova-production-arrow-exchange-nearest-repro-r1.log`；102 项，93 PASS、9 FAIL、0 SKIPPED：6 codec 构建缺失、3 SDK 硬编码/kind 缺口 |
| PASS | 协议官方 golden 更新 | `/tmp/pangrova-production-arrow-exchange-proto-golden-r1.log`；统一源 protoc 35.1；Java/Native 已分别经官方生成器生成，不手改 generated sources |
| FAIL | Java 最近测试首轮 | `/tmp/pangrova-production-arrow-exchange-java-nearest-r1.log`；remote-shuffle fixture 缺 serialized-buffer grant，6 项在目标校验前失败；只补真实资源额度，未放宽生产准入 |
| PASS | 相同 Java scope 加 serialized-output grant 回归 | `/tmp/pangrova-production-arrow-exchange-java-nearest-r2.log`；protocol 47 + planner 26 = 73 PASS、0 FAIL、0 SKIPPED，24.322 s；不包含 Worker JNI/Coordinator durable replay/E2E |

### 0.33 2026-10-03 实际 owner drain 与 Arrow 动态依赖隔离

- `ManagedHttpClient.close` 原来在超时后仅 `shutdownNow` 就返回，未证明实际 HTTP
  executor 退出。现在二次 join；未退出/中断明确失败并保留同一 owner 可重试。测试用
  实际 executor、entered/interrupt/finally-exit 屏障，不将 cancelled Future 当作退出。
- Heartbeat 默认生产构造器使用同一 owned-client 路径；无实际工作时可能已经退出的
  executor 不能被测试假定为必然抛中断异常。该首轮测试失败保留，修复 fixture 后复证。
  Worker NEW 状态已装入的 CDC/catalog owner 也由真实 shutdown 关闭；失败保留
  CLEANUP_PENDING、成功才完成 latch，并发 start/stop 不得越过 STOPPING。
- 旧 `ShufflePageValidator` 只有测试调用、没有生产 wiring；其未使用的私有13-byte
  页头校验器与29项合成页测试删除，Git 可恢复。真实生产 serializer/SDK Task/服务
  测试保留；删除这些合成断言不是新的 E2E 通过证据。
- Arrow 首轮 codec 构建虽成功，但 Flight 静态依赖的 Protobuf/gRPC 符号可见性冲突
  产生链接 warning。隐藏所有 C++ 编译符号仍留下冲突；namespace wildcard export
  list 又试图提升上游 deliberately-hidden helpers，产生大量 warning，均不计零警告。
  首轮精确私有 namespace 隔离后构建无 warning，但真实 `nm` 仍发现 TLS、gRPC flags
  等静态依赖外泄。bootstrap 现在由首轮实际可见符号生成 exact private list，按 Arrow
  上游 `symbols.map` 公开策略保留已有 ABI；第二轮链接后逐库验证公开符号集合完全
  不变且私有依赖 export 为零，不隐藏诊断或扩大公开 helper ABI。
- shared iterative serializer 的多 range append 先暂存所有 slice owner，全部成功后才
  发布；后续非法 range/分配失败不能把早期合法 range 留在 buffer 中。新增真实
  flush/decode prefix oracle，**尚未编译/运行**，不纳入既有 PASS 数量。

| 状态 | 范围 | 实际结果 / 证据 |
|---|---|---|
| PASS | 固定 SDK13 Release 与 archive provenance | `/tmp/pangrova-production-arrow-sdk13-release-r1.log`，442/442、0 warning；`/tmp/pangrova-production-arrow-sdk13-release-provenance-r1.log`；不是主 Native 全回归 |
| PASS | Managed HTTP 最近专项 | `/tmp/pangrova-production-managed-http-owner-nearest-r1.log`；13 PASS、0 FAIL、0 SKIPPED，20.751 s |
| FAIL | Worker owner 首轮 clean 最近专项 | `/tmp/pangrova-production-worker-actual-owner-clean-nearest-r1.log`；Worker73项中1项 Heartbeat fixture 失败；未抹去失败或放宽生产 close |
| PASS | 相同 owner scope 加启动前实际 owner | `/tmp/pangrova-production-worker-actual-owner-clean-nearest-r2.log`；32模块 clean compile/test-compile，common13+SPI3+Worker76=92 PASS、0 FAIL、0 SKIPPED，83 s；不是JNI或全仓测试 |
| PASS | Coordinator/metadata 最近纯 Java 契约 | `/tmp/pangrova-production-arrow-exchange-coordinator-pure-nearest-r1.log`；metadata3+Coordinator24=27 PASS、0 FAIL、0 SKIPPED，36.692 s；不执行真实任务或数据库 |
| PASS | Arrow codec/shared-library 首轮零警告 | `/tmp/pangrova-production-arrow-codecs-build-r4.log`；210/210；仍存在私有依赖 exports，不能用于隔离验收 |
| PASS | Arrow 精确私有导出隔离 | `/tmp/pangrova-production-arrow-codecs-export-isolation-{configure,build}-r5.log`；0 warning；actual `nm` 验证 Arrow4865/Flight950 公开符号不变、私有 exports 为0；尚未安装并做 Native/JNI 回归 |
| PASS | Mach-O 符号公开策略专项 | `python3 pangrova-worker-native/bin/test_isolate_arrow_macos_exports.py`；3项测试；静态依赖/误导名称明确拒绝，不是动态加载测试 |
| NOT_VERIFIED | 新 SDK13 matching-ASan、主 Native、打包与全回归 | SDK 构建尚在执行；当前主库仍为旧依赖 cohort。不得复用旧窗口全量 PASS 或提前执行 JNI/E2E |

时间戳宽范围、append 累积复制、Paimon resource authority、独立多进程故障恢复及生产
规模 SLO 尚未闭环。本节不宣称全链路完成或生产级通过。

### 0.34 2026-10-03 私有依赖隔离暴露 RE2 header/archive 混用

主 Native SDK13 Release 首轮链接 **FAIL**，不是继续接受 Flight 依赖符号泄露的理由。
`libvelox.a` 和 gRPC 请求 `re2::StringPiece` ABI，canonical RE2 header/config 也是该版本；
bootstrap 的 `unify_re2` 却只用 deps-install 的 string_view archive 覆盖 library，没有同步
headers、CMake package 或已编译消费者。两个 archive SHA256 完全相同证明实际错误
复制；旧 Flight 的私有 RE2 exports 曾偶然填补缺符号。这不是一个可承诺的统一版本。

已删除 archive-only copy，canonical gRPC 所安装的完整 RE2 package 成为唯一选择；
Release 与 ASan SDK configure 显式固定同一 `re2_DIR`。主 configure 校验 SDK 实际选中的
package，并编译/链接真实 FullMatch/capture/GlobalReplace 契约；bootstrap 还执行该程序。
不合并两版本 archive、不重新开放私有 exports、不对 unresolved symbols 降级。

本机保留完整错误 archive 的独立副本，用原 gRPC matching archive 修复已知错误覆盖；
原 backup 未覆盖。真实 package probe 修复前 FAIL（StringPiece/Abseil 缺符号），修复后
编译、链接、匹配、捕获、替换 **PASS**。主库 Release 已重新构建；最新全量 CTest 仍在执行。

| 状态 | 范围 | 实际结果 / 证据 |
|---|---|---|
| FAIL | SDK13 主 Native Release 首轮 | `/tmp/pangrova-production-arrow-sdk13-native-release-build-r1.log`；两个目标链接缺 RE2 StringPiece ABI；没有进入后续 gate |
| FAIL | 实际 RE2 package 契约修复前 | `/tmp/pangrova-production-re2-contract-before-repair-r1.log`；精确重现 header/archive 不一致 |
| PASS | 相同 package 契约修复后 | `/tmp/pangrova-production-re2-contract-after-repair-r1.log`；真实编译、链接及执行 exit0，无 warning |
| PASS | 主 Native 新依赖 configure | `/tmp/pangrova-production-arrow-sdk13-native-release-configure-r2.log`；canonical RE2 ABI/link probe 通过 |
| PASS | 主 Native Release 重构建 | `/tmp/pangrova-production-arrow-sdk13-native-release-build-r2.log`、`/tmp/pangrova-production-arrow-sdk13-native-release-build-r3.log`；均 exit0，0 compiler/link warning；r3 使用私有导入闭环的 Arrow/Flight r6 |
| PASS | 保留原范围并扩大专项 | `/tmp/pangrova-production-arrow-sdk13-nearest-ctest-r3.log`；120/120，0 skip，15.37s；包括真实 PartitionedOutput、codec、append、typed Exchange 和 Native retrieval merge |
| FAIL | 全量 CTest 首轮 | `/tmp/pangrova-production-arrow-sdk13-full-ctest-r1.log`；3874 PASS/1 FAIL，258.93s；RRF fixture 仍断言已删除的 identity Project。改为直接 Exchange 并补强 serde、schema 和三个绑定通道的断言；不恢复多余生产节点 |
| PASS | Arrow/Flight 私有依赖导入闭环 | `/tmp/pangrova-production-arrow-codecs-re2-matching-symbols-r6.log`、`/tmp/pangrova-production-arrow-codecs-re2-matching-install-r6.log`；保留 Arrow 4865/Flight 950 个原公开符号，0 private exports，0 unresolved private/dynamic imports。仅安装已验证 dylib，未改变正在编译的 SDK headers |
| PASS | 符号策略与未解析导入检查 | `/tmp/pangrova-production-arrow-private-import-policy-tests-r2.log`；5项 Python专项和实际 dylib 验证通过；不是 JVM 动态加载/E2E 证明 |
| BUILD_ONLY | SDK13 matching ASan 首轮 | `/tmp/pangrova-production-arrow-sdk13-asan-setup-r1.log`；442/442完成、source provenance已记录；RE2 package 与主库不同，不能作为当前 matching gate，须重建 |
| PASS | 最新主 Native 全量 Release CTest | `/tmp/pangrova-production-arrow-sdk13-full-ctest-r2.log`；3875/3875，0失败、0 skip，270.27s；未使用过滤范围，不覆盖 JVM/JNI 加载或独立进程集群 |
| NOT_VERIFIED | matching ASan 与后续完整链 | SDK `/tmp/pangrova-production-arrow-sdk13-asan-re2-matching-setup-r2.log` 尚在执行；主 ASan、打包、Java全量、E2E、Smoke及独立分布式验收未完成 |

append 的另一根因也已复证：平方复制发生在 SDK Exchange 每个输出批次内逐 page 追加，
不是整个查询无限累计。最小完整修复是在该实际 callsite 本轮独立解码 chunks、checked
累计 cardinality、最后一次拼接并成功后发布；上一轮共享 result 不可修改。仅检查 root
use_count 或调用 ensureWritable/resize/copy，不能提供 nested/string 的强异常保证。
该优化尚未实施；copy bytes 与实际 Exchange 小页/共享 owner/OOM 验收不可由估算替代。

### 0.35 2026-10-03 全量 Release 检查点后的生产贯通根因

§0.34 的 3875/3875 是 SDK13/Arrow r6 检查点的真实 Release 结果，不是后续协议和
所有权改动的验证结果。以下原子单元进入实施后，必须重新生成协议并重跑完整链；
不能以旧 binary 的 PASS 宣称当前工作区全部通过。

1. **取消 ACK 不等于 Native 退出。** `TaskCancelRetrier` 将 HTTP cancel ACK 直接写成
   tracker terminal；durable dispatcher 又用 terminal/future 判定清理，并在 cancel
   失败后 unregister/丢弃 active owner。统一修复必须保留 exact attempt，依据同一
   `TaskReport` 的真实 execution drain evidence 释放所有权；PG publication 拒绝与
   authenticated issued-attempt 的资源退出证明分离，不能授予旧 owner 新的 winner 权限。
   `handle=0`、缺统计、Worker-reconciled 标签均不能推断 never-created/已释放。
2. **通用 durable cancel 锁序错误。** 原 JDBC cancel 是 page→job，claim/completion
   是 job→page；两个 READ COMMITTED statement 还可能选中不同 generation/job 集合。
   修复使用同事务、确定性锁住 exact live jobs，再处理这些 jobs 的 pages 与 job rows。
   PostgreSQL 真实并发/回滚测试源码准备中，不将源码准备计为通过。
3. **实际 function inventory 未接入。** `GET_CAPABILITIES` 未填实际 catalogue；生产
   `SqlCompiler` 使用 defaults，scheduler 未验证完整 `FunctionRequirement`。SDK type
   resolver 与实际 ExprCompiler 的 scalar 选择顺序也不同。按已批准 function contract
   规格实施唯一声明生成的 selection evaluator、actual immutable entries、manifest、
   query-frozen view、exact eligible cohort、最终 plan requirement 重算和双端准入；不另
   加函数名单、不靠 online 逐函数 RPC 猜类型。SDK 改动待当前 SDK 编译结束后统一应用。
4. **E2E PG 曾主动关闭持久保障。** 已删除 `fsync/synchronous_commit/full_page_writes=off`；
   readiness 与现有 restart 均真实查询 `pg_settings` 要求三项 on。使用 Awaitility 替代
   sleep，不增加 timeout/retry/内存；初始化失败传播原因并 try-all 清理。新增两项真实
   PG 配置测试仍待打包后的执行，不能据此宣称 crash/restart 验收已通过。
5. **Lance SQL 与分布式证据必须分开。** 当前五类 public SQL 测试源码共14项，仍为
   单 JVM/单 Worker/local warehouse；其中 Paimon 7项读测试由 SDK 写源数据，不证明
   SQL PK/partition/multi-bucket 写。Native 组件及 Rust MinIO/Toxiproxy 不替代实际
   多进程 SQL publication、取消、ACK 丢失、重启与 stale owner 验收。Paimon physical
   non-publication object disposition、完整合法写形态及真实 Linux 镜像验证仍未闭环。

matching ASan SDK 仍在执行，主 ASan、最新 native package、Java全量/coverage、全部
SQL E2E、Smoke及大规模集群 SLO 均未完成。上述根因修复需要真实测试结果后才能改为 PASS。

### 0.36 2026-10-03 exact admission seal 与 Worker 重放边界

生产源码复证了两个不同的窗口，必须分别封闭，不能用 cancel ACK 或 NOT_FOUND 推断：

- `TaskSubmissionHandler` 原字符串 index 仅按 taskId 去重，不比较 attempt/security/grant/plan；
  `TaskManager.removeCoordinatorIndex` 在 terminal 时删除该 index，`TaskCleanupScheduler`
  又允许消费完结果后在 admission deadline 前移除 TaskInfo。因此延迟 POST 可再次创建
  同一个 exact attempt；`CanonicalTaskRequests.requireDeadline` 原来只检查正数，没有
  Worker 的真实过期准入检查。
- `StagePartitionDispatcher` 的 dispatch future timeout 不证明实际 HTTP POST 已经停止；
  cancel 若先到达一个尚未接收 POST 的 Worker，NOT_FOUND 也没有封闭后续 admission。

同一个准入 authority 的源码修复已进入实施：有硬容量上限的 `TaskAdmissionRegistry`
替换字符串 index，只保 deterministic 原始 NETWORK request SHA-256、exact attempt、
deadline、本地 ID 和退出专用 compact receipt。未过期 identity 不能因 terminal/result
清理而消失；同 ID 不同 payload 拒绝；过期 POST 在 result ticket 和资源取得之前拒绝。
资源 reservation、JNI、等待与实际清理均不持有 registry 锁。关闭先 seal 全部准入，
等待 pending reservation 真正移交 TaskInfo 或退还 grant，再关闭依赖；超时保留真实
owner/dependencies 供重试，不假定 HTTP future 完成等于物理退出。

唯一 DELETE endpoint 正在原子迁至统一 v2 `TaskAdmissionSealRequest`，不是第二套任务
协议或 feature proto：exact issued attempt + 原 NETWORK request checksum + deadline。
Worker 接到尚未 submit 的 seal 时，在同一个 registry 原子封闭该 identity；已有 pending
reservation 必须完成真实 pre-owner rollback 才能返回无 owner 证明；已移交 TaskInfo 的
owner 则必须走真实 creation/release checkpoint。200 只携带真实 canonical compact
`TaskReport`；202 表示退出尚未证实。Coordinator 使用退出专用 validator，不能拿 compact
receipt 发布 winner/artifact。已曾创建 Native 的 owner 不得因 TaskInfo 消失而伪称
never-created；过期且已经遗忘的 identity 不得制造历史退出证明。

另外复证了配额泄漏：原 `TenantExecutionQuotaEnforcer.canAdmitTask` 实际取得 query slot，
而 `registerTask` 在 memory reservation 之后才登记 owner，buffer admission 失败会丢失
可释放的 slot identity。源码已收敛为同一个预登记 TaskQuota owner 的 `tryReserveTask`，
并将实际 rollback 覆盖 grant、quota 和 query cancellation index；原 quota 数量与隔离
断言保留，新增 duplicate/idempotent release/identity conflict 验证。

| 结果 | 检查范围 | 证据及限制 |
|---|---|---|
| PASS | seal 协议迁移之前的 registry/quota 最近检查点 | JDK17 `javac --release 17 -encoding UTF-8 -Xlint:all -Werror`，真实 JUnit 16/16、0 skip、235ms；`/tmp/pangrova-production-task-admission-isolated-nearest-r4.log`；不是 JNI/E2E 或后续 seal 源码结果 |
| FAIL | 最近诊断 r1–r3 | r1 临时 javac 缺显式 UTF-8；r2 新测试误用非实际 proto field accessor；r3 临时 classpath 缺 actual Substrait core，15/16；均保留日志，没有删断言缩小范围 |
| PASS | 已知 exact identity 的只读 cleanup epoch fence | JDK17 `javac --release 17 -encoding UTF-8 -Xlint:all -Werror`，真实 JUnit 5/5、0 skip、64ms；`/tmp/pangrova-production-task-cleanup-epoch-isolated-nearest-r1.log`；仅控制组件，不是实际 HA handoff 或 Native drain |
| PASS | 统一 seal 最近组件检查点 | 正式插件生成最新 proto 后，重新编译实际 helper/registry/quota/fence；JDK17 `-Xlint:all -Werror`，JUnit 25/25、0 skip、186ms；`/tmp/pangrova-production-task-admission-isolated-nearest-r5.log`；不是 Worker/JNI/SQL E2E |
| BLOCKED | 当前统一 seal 原子源码验证 | protocol/function/native owner 原子单元尚未全部冻结、重新生成和打包；新增 registry seal、真实 HTTP/SQL cancellation 5项及原 cancel retrier 断言迁移，尚未计 PASS |

已知 exact identity 的退出使用只读 fence：允许已独立观察到的旧 epoch 清理，不能
readmit 或将旧 owner 恢复为 ACTIVE；尚未接收 POST 的 absent seal 仍要求当前 admission
epoch/owner。取消 endpoint 与 submit 同样区分未握手 503、未观察到未来 epoch 425 与
永久冲突 409；这两个 HTTP 分支的新断言尚待最近编译运行，不以源码存在当通过。

函数物理桥最近检查点使用既有 Python 3.12.14：完整 discovery 保留原 67 项并增加
typed recursive projection 断言，69/69、0 skip；
`/tmp/pangrova-production-function-contract-python-nearest-r3.log`。系统 Python 3.9.6
低于生成器显式最低 3.10 的失败仍保留，不降低版本 guard；真实 Native factory 导出、
Java 差分与完整 planner/worker cohort 尚未验证，Python 通过不是 capability manifest 闭环。
六类 protocol 最近测试已完成：26/26、0 skip，
`/tmp/pangrova-production-function-typed-projection-protocol-nearest-r2.log`，10.226s；
r1 编译 FAIL 原因为误用实际 Protobuf 不存在的 `Descriptor.findOneofByName`，已改成
同一个实际 descriptor helper 的 `getOneofs`，未改原断言或类型域。

租户内存计数另有可复现溢出：有限额分配使用 `current + bytes` 作准入比较，unlimited
直接 `addAndGet`，越过 signed long 边界会被接受并生成负 usage。`TenantResourceTrackerTest`
新增两个真实计数回归在修复前均 FAIL（9项中2失败，0 skip，
`/tmp/pangrova-production-tenant-memory-overflow-nearest-r1.log`）。源码已收敛为同一个
CAS reservation：先以 `bytes > limit - current` 判定，unlimited 仍受可表示的
`Long.MAX_VALUE` 上限约束；并发 query count 也不允许自增环绕。最近修复验证为
11/11、0 skip，含真实并发大额 reservation 和固定 seed 的 200 轮完整正 long 域
property；`/tmp/pangrova-production-tenant-memory-overflow-nearest-r2.log`，2.507s。
这里证明的是计数、拒绝和退款，不声称已分配对应量的物理 RAM 或完成集群配额验收。
随后完整 resource owning-module 与依赖 discovery 通过：Common 240、SPI 701、Resource
176，共 1117 项、0 fail/0 error/0 skip，19.989s；
`/tmp/pangrova-production-resource-full-module-r1.log`。这不是全仓 `full-regression` 或覆盖率验收。

正式部署配置最近检查点已通过：`CoordinatorOptionsTest` 20项与
`ProcessDeploymentConfigurationTest` 3项，23/23、0 skip，2.395s；
`/tmp/pangrova-production-process-deployment-config-nearest-r1.log`。YAML/environment 的
内部密钥、进程身份和 endpoint 已证明真实消费，Loader/Main/真实容器启动的全链仍未测。

实际 SDK `Exchange::getOutputFromColumnarPages` 原本每页通过非零 offset append
重复制累计 prefix，多小页合并可能出现 O(N²) 行复制。当前源码改为每个 chunk 使用
offset 0 独立 owner，校验累计 vector row capacity 后只在最终多 chunk 合并时复制一次；
单 chunk 原 owner 直接交付，不修改下游持有的上一 output。实际 operator 记录
`columnarCoalesceInputBatches` / `columnarCoalesceCopiedRows`，不能把该行数指标冒充
网络、字符串 buffer 或整条 Arrow 边界的绝对零拷贝。`ColumnarExchangeCoalescingTest`
新增 128 小页/NULL/长字符串旧 owner、单页、晚到坏页 strong publish、零列 cardinality
四项**组件**源码；以 actual compile command 的 C++20 `-fsyntax-only -Werror` 检查
SDK hunk 与测试均 PASS、0 warning，
`/tmp/pangrova-production-sdk-exchange-coalescing-syntax-r2.log` /
`/tmp/pangrova-production-exchange-coalescing-test-syntax-r2.log`。r1 为最近检查工具误覆盖
SDK 既有 warning flags 与新测试 namespace 错误，日志保留；r2 保留真实生产 flags，
没有增加 suppression。尚未执行这四项、全量 Native 或 ASan；该 hunk 必须进入同一个
SDK patch/provenance 并与完整 function snapshot 原子单元重新编译。

**跨 Worker 重启仍未闭环。** 实际 `WorkerRegistrationRequest`、heartbeat、`WorkerInfo`
和 JDBC registry 没有 Worker process incarnation；注册 UPDATE 保留旧 registered_at，
TaskAttemptId 的 execution owner 是 Coordinator 身份，不是目标 Worker 身份。进程重建
会丢失 process-local admission registry；Result ticket 的 producer-local signing key
保护结果读取，但不保护 TaskRequest/seal 重新准入。下一原子单元必须将实际 Worker
incarnation / backend-issued registration generation 贯穿登记、heartbeat、deregister CAS、
query-frozen eligible view、issued TaskRequest/receipt identity 和双端 admission，并用真实
独立进程重启、旧 POST、旧 heartbeat/delete 和旧 receipt 验证；不能用重新命名 WorkerId
或 registered_at 作为 proof。具体协议与持久 ownership 生命周期须先复证全部 caller，
不把本段设计写成已实现。

一手架构依据：etcd 的 revision/CAS transaction 与 lease 区分“独立取得的 ownership”与
“连接存活”，Kafka producer 将实际 producer ID/epoch 用于 fencing。由这些机制推导的
本项目设计结论是必须显式绑定新进程 generation，**不是**引入 etcd/Kafka 作为第二个
Pangrova control authority。[etcd v3 API](https://etcd.io/docs/v3.6/learning/api/)、
[KafkaProducer 官方 API](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html)。

### 0.37 2026-10-03 执行交接 deadline 的实际红绿回归

真实准入仍有一段 deadline 缺口：`Reservation.publish` 已在同一个 registry lock 内
检查截止时间，但后续登记和统计调用可能跨过 deadline，`Reservation.admitted` 原本只
检查 open/sealed 就把 `handedOff` 标记为 true。新增可控 Clock 的组件回归实际复现：
deadline 恰好到达时，交接未被拒绝，27 项中 1 FAIL、0 skip；
`/tmp/pangrova-production-task-admission-isolated-nearest-r6.log`。修复在同一个实际交接
临界区再次 `requireUnexpired`，使既有 `rejectBeforeOwnership` 执行真实退款；拒绝本身
不产生 no-owner proof，只有退款完成后才允许实际 seal 回执。

保持相同 27 项 discovery 重跑：JDK17 `--release 17 -encoding UTF-8 -Xlint:all -Werror`，
27/27、0 fail/0 skip、194ms；
`/tmp/pangrova-production-task-admission-isolated-nearest-r7.log`。包含 publication 前
deadline 边界与 publication 后、execution handoff 前的边界，不使用 sleep 或模拟
Native 成功。它不是 JNI、HTTP 或真实分布式测试，不能覆盖真实执行中的超时取消。

最新 Rust Step 0 构建检查点 PASS、0 warning/error，4m34s；
`/tmp/pangrova-production-current-rust-step0-r2.log`。当前完整 function registry snapshot、
Native construction recovery 与精确 remote egress cleanup 原子源码仍未全部冻结。
不能将旧 SDK13 的全量 CTest 检查点与正在修改的 SDK/协议混为最终 release 证据；
同源 Release、matching ASan、重打包、全仓 clean Java/regression/coverage、E2E 和
activation smoke 均需在完整冻结的源码上按顺序重新执行。

配额 wiring 的独立复证发现，`TenantQuotaMapper` 没有任何生产 caller，它的 JavaDoc
却宣称 resource 的配额模型与 Worker 租户 tracking 已移除、Coordinator runtime check
已接通；实际三者均不成立。已删除这个未消费的设计 mapper 及其仅验证 mapper 取值的
11 项测试；没有删除实际 `QuotaChecker`、admission controller 或 Worker tracking 的
行为回归。删除前全仓引用仅来自该类与这份测试，文件没有重叠的未提交用户修改；
可由 Git 基线恢复。全量 clean 编译/discovery 仍待最终验证，不能把减少无生产路径的
测试数当作修复 runtime governance。实际 Worker `recordCpuTime` / `recordScanBytes`
和 `TenantResourceTracker.setQuota` 没有生产 caller，仍是后续必修 wiring 缺口。

### 0.38 2026-10-03 配额准入的实际算术边界复证

`AdmissionRequest` 原允许负 memory estimate，`TenantQuotaAdmissionController` 的 local
与 shared-slot 路径又分别使用 `clusterUsed + estimated` 比较。真实回归先确认负值入口
13 项中 1 FAIL（`/tmp/pangrova-production-cluster-admission-memory-nearest-r1.log`）；
入口改为显式拒绝负值后，同命令继续揭示两个实际 admission 路径在 signed-long
环绕时返回 ADMIT 而非 REJECT（`/tmp/pangrova-production-cluster-admission-memory-nearest-r2.log`）。
这两个失败没有删测试或改 oracle，源码收敛到同一个 memory-reject 判断：先验证已用
内存，再以 `estimated > limit - clusterUsed` 检查，避免先做溢出加法；拒绝后退款实际
已保留的 slot。负 observational usage 同样 fail-closed。

最近完整六类 discovery 为 SPI 13 + Security 34，共 47/47、0 fail/0 error/0 skip，4.079s；
`/tmp/pangrova-production-cluster-admission-memory-nearest-r5.log`。包含 local 与实际
`InMemoryClusterConcurrencySlots` 两种组件路径、最大可表示边界、负 usage 和固定
seed 的 300 轮完整非负 long 域 property；property 使用 `BigInteger` 无界加法作为
独立 oracle，而不是照抄修复条件。没有分配 Long.MAX_VALUE 对应的物理内存，也没有
把 in-memory slots 或固定 usage sample 冒充跨进程测试。

实际 unused `QuotaChecker` 构造依赖已从 controller、唯一生产 bootstrap 与全部最近
caller 删除，不留旧重载 shim；它原本从未被 controller 调用，不能作为 runtime/daily
quota wiring 的证据。`QuotaChecker` 在真正消费它的 `TenantQueryGuard` 路径仍保留。
这不解决跨 Coordinator anonymous slot 的精确 owner、重复 release/重启恢复或物理
memory reservation；这些仍需真实持久 ownership 和全链路验收。

最新 Rust 全量 `cargo test --locked --release --target aarch64-apple-darwin --lib -- --test-threads=1`
为 38/38、0 failed/ignored/filtered、0 warning，编译 4m47s，实际执行 3.07s；
`/tmp/pangrova-production-current-rust-tests-r2.log`。它只验证 Rust crate，不能代替 Native
ASan、JNI、Java 全仓与 SQL E2E。

`NativeOperationLifetime` 已以实际 entered lease 和 completion barrier 取代跨 JNI/远端
I/O 持有 RWLock 的 owner 实现。最近纯线程组件检查点 PASS：5/5、0 skip、284ms，
JDK17 `-Xlint:all -Werror`；
`/tmp/pangrova-production-native-operation-isolated-nearest-r2.log`。r1 为 self-join
回归中 try-resource 变量未使用导致 warning-as-error，保留日志并修复测试观察，不加
suppression。真实在途 lease 退出前，release/close 不得执行清理；interrupt 不代表退出；
同线程 self-join 必须拒绝。最近测试没有执行 JNI，不能代替 packaged-runtime 证明。
既有 Native release-race/ConcurrentClose 测试已迁移到实际 `acquireHandleLease`，
保留关闭阻塞、物理 Native handle 不可再读取和幂等回收断言，并使测试 finally join
全部已启动关闭线程；尚未运行当前 JNI 源码与包。

### 0.39 2026-10-03 同源 SDK 重建与终态/物理退出边界

SDK14 的唯一 31-path ownership/selection/exchange 补丁已经应用并逐文件复验，
零 context 格式转换不改变任何目标源码 blob；`git diff --check` 重新 PASS。
补丁 SHA-256 为 `f5609b4666bb0c45c9fae13ff794c91e5e962e9b6383fef39b2575b79011b170`。
对实际旧 SDK13 `lib/libvelox.a` 执行 record 被明确拒绝：归档与上一补丁的已登记
归档完全相同，必须重建；日志
`/tmp/pangrova-production-sdk14-old-archive-negative-r2.log`。r1 使用错误归档路径，
仅证明 missing-file 拒绝，不计入旧库漂移防护证据。

Release configure r1 出现真实 simdjson 版本不匹配与 CMP0167 policy 告警，不能记作
零告警。显式使用 SDK 固定版本的 bundled simdjson，并选择与 ASan bootstrap 相同
的 Boost package policy 后，r2 configure 为 PASS、零告警；
`/tmp/pangrova-production-sdk14-release-configure-r2.log`。生产 Release bootstrap 已
同步这两个参数。SDK Release build r1 实际 FAIL：`VectorFunction.cpp` 的模板 helper
以三元表达式混合普通 metadata/signature 返回值与 `nullopt`，两个真实实例化报错；
`/tmp/pangrova-production-sdk14-release-build-r1.log`。须保留普通与 optional 返回的
语义，并修正模板控制流后重跑。继续复证还确认 overwrite/remove/clear 可在 registry
锁内销毁旧 factory captured owner，JNI GlobalRef 最后回收可能发生在该锁内；正在
收齐唯一 registrar 的锁外 retirement，不以正常注册顺序或注释替代证明。
9 项真实 SDK registry owner 执行测试与 4 项 exchange coalescing 测试未据此升级为
PASS。Native/ASan、打包、
全量 Java/coverage、E2E、Smoke 仍需顺序验收。

统一 admission-seal/physical-drain 协议的最近官方 Maven discovery 为 9/9、零
failure/error/skip，12.306s；
`/tmp/pangrova-production-canonical-admission-drain-protocol-nearest-r1.log`。
这只证明协议 helper 与 round-trip，不能证明 QueryManager 已等待物理退出。

继续沿生产 caller 复证确认：`QueryManager.checkClusterAdmission` 在逻辑 terminal
listener 立即 release，早期失败的 rollback 还可能重复 release；
`TenantQuotaAdmissionController` 与 shared slots 仅持有 tenant 匿名计数，因此
unknown/重复 release 可能释放另一查询的容量。QUEUE 被 QueryManager 当拒绝返回
时也没有平衡其排队 reservation。另一方面，terminal completion 先递减逻辑 active
count 再调用 task cancel/cleanup；retained retry 不使已有 terminal listener 重新等待
退出。`TaskCancelRetrier` 的真实 receipt 检查不能自动修复调用者的 void/enqueue
语义。该缺口保持未关闭：配额归还、关闭等待和 issued identity 清理必须核对每个
真实派发 attempt 的 exact physical-drain receipt；不能用 HTTP ACK、空集合、Future
终态或逻辑 active count 代替。修复须保留唯一 issued-attempt authority 和真实失败
重试，不新增旁路执行账本。

### 0.40 2026-10-03 时间旅行 source-span 的实际红绿回归

新 Parser helper 复用已有唯一 grammar/AstBuilder，按真实 time-travel clause token
span 移除子句，不用 regex 搜索 SQL 文本；ANTLR code-point offset 显式映射 Java
UTF-16，保留其他原始字节与非 BMP 字符。其最近 r1 真实 7 项中 1 ERROR：末尾
分号被 lexer 拒绝；原 `SqlParser` 使用同一 grammar 也有该行为。
`/tmp/pangrova-production-time-travel-parser-nearest-r1.log` 保留，未删分号 oracle。

根因修复在唯一 `PangrovaSql.g4` 增加单个可选 statement terminator，并经 Maven
ANTLR 自动生成；没有手改 generated sources 或在 helper 内私自预处理另一种 SQL。
保留原 7 项，新增当前 quoted/unquoted case 策略与多 statement 拒绝检查后，最近
r2 为 10/10、零 failure/error/skip，147ms，reactor 5.051s；
`/tmp/pangrova-production-time-travel-parser-nearest-r2.log`。

随后无筛选 `mvn -B -Pfull-regression -pl pangrova-parser -am test` 为
Common 240 + SPI 708 + Parser 528 = 1476/1476、零 failure/error/skip，20.591s；
`/tmp/pangrova-production-parser-full-module-r1.log`。包括真实 grammar/source-span、
CTE visibility、scalar subquery、INSERT SELECT/set sources、literal/comment/quoted
name 保持及 strict invalid input。当前 Planner 的实际 identifier 策略仍是 quoted 与
unquoted 均 case-insensitive；helper 按同一策略匹配 CTE，不改变 source/AST 名字。
这不是 ANSI quoted-case-sensitive 支持，也不证明唯一 Calcite frontend 迁移完成。

Coordinator 的 trusted-session qualification、selected snapshot/schema/stats/split
共享绑定仍在收齐。统计入口还发现 `join(".")` 与 `split(".")` 不能表达含点号的
单个标识符；正在复用既有 `QualifiedTableName` 贯通实际 provider/cache/cost/event
callers，不以私有猜测编码或另建 snapshot authority 绕过。当前 Parser 模块通过
不等于这条 read binding、JNI 或 public SQL 时间旅行链路已经闭环。

门禁工具自身最近验证：diff-coverage 真实 Git/class/report fixtures 17/17、Arrow
symbol-policy helper 5/5 均 PASS；环境路径门禁、E2E truthfulness 静态门禁及文档
174 个 class 引用检查 PASS。这些不是实际 changed-line coverage 达标、Native ABI
或 E2E 通过；最终仍须使用最新源码的完整 JaCoCo、匹配 native 库与真实执行验证。

### 0.41 2026-10-03 精确配额 owner 与当前 SDK Release 检查点

`TenantAdmissionOwnershipTest` 的新增 10 项最近实测 r1 全部 FAIL，暴露未知 query
release、重复 release、重复 admit、错误 tenant 消耗 queued owner 和重复 QUEUE 占位。
根因是匿名租户计数不能代表某个 query 的归还权；queue 的 check/increment 也不原子。
引入同一进程 live admission owner、实际 entered reservation 的补偿动作和锁外 completion
barrier 后，同一原始 10 项 r2 全部 PASS。后续进一步迁移唯一
`ClusterConcurrencySlots` 的实现和全部 caller：PostgreSQL 在一个事务内锁住实际 tenant
行、保存全局 query/tenant live owner 并更新计数；未知、重复和错 tenant 归还不能扣除
别的 owner。Backend reserve/release/usage 错误明确抛出，不能将其当零 usage 或成功归还。
Admission 失败仍保留实际 entered 的补偿义务；release 失败保留同 owner 供重试，不持
Java 锁等待 JDBC I/O。旧匿名非零计数没有 query 证据，启动明确拒绝，不能重标为新
owner 或直接清零。Query 标识必须来自实际 query authority，物理结束后不得复用；
这不是新的 Task/Worker authority。

最近完整专项 r8 PASS：SPI 13 + Security 51 = **64/64，0 failure/error/skip，7.159s**；
`/tmp/pangrova-production-tenant-admission-ownership-nearest-r8.log`。包括实际 PostgreSQL
两对象并发限额、replacement slot/controller 的精确归还、真实错误密码导致 backend
不可用后的 owner 保留/恢复、旧匿名计数拒绝且不修改、24 线程重复 admit/release 与
queue 上限，及固定种子 `20261003` 的 300 次独立集合模型 property（物理关闭后使用新
query identity）。这些是组件/存储检查，不是独立进程 HA 恢复或 SQL E2E。持久化
issued TaskRequest、terminal outbox、查询物理退出后的配额归还生产 caller 与 Worker
incarnation 仍须全链验证，不能凭 logical terminal、租约到期或空重放任务集合退款。

读绑定/typed table key 最近 SPI 近测 PASS：8/8、0 skip、4.838s；
`/tmp/pangrova-production-typed-read-identity-spi-nearest-r1.log`，不代表时间旅行 SQL E2E。
Client typed authentication/header/config 与原 timeout/completion 近测 r2 PASS：REST 4 +
JDBC 19 + CLI 24 = **47/47，0 skip，21.351s**；
`/tmp/pangrova-production-client-auth-consumption-nearest-r2.log`。r1 因 Root 未显式配置
已有 Python 3.12、generator 拒绝系统 Python 3.9 而 FAIL，未执行测试；保持原测试
selection、使用合规解释器重跑，不添加 generator fallback。后续实际 TLS endpoint
准入、连接池 protocol identity 与 async IPC/schema 展示修复已 SOURCE_READY，尚需新
cohort 验证。该 expanded cohort 的最近 r3 为 PASS：Protocol 47 + REST 4 + JDBC 19 +
CLI 36 = **106/106，0 failure/error/skip，21.995s**；
`/tmp/pangrova-production-client-auth-consumption-nearest-r3.log`。新 schema/type-template
的 Protocol Java 同时完成正式生成/编译，不代表 GETCAP/Planner 的完整 catalogue
已经发布。既有 fake bearer fixture 不计为真实生产认证，正式独立进程 Auth/CLI
验证仍待同源 image 和新 native package。

SDK 唯一补丁当前为 **32 paths**，补丁 SHA-256
`40b743fd39fcf37d341c9f63c27a176944fac91f640b2fbf0f87f883331aae9a`。
`VectorFunction` 普通/optional 模板返回与 registry 锁外 retirement 根因修复已应用
并通过逐文件 verify。Release configure r1 PASS、0 warnings；Release build r1
**665/665，exit 0，0 compiler warnings/errors**；日志分别为
`/tmp/pangrova-production-sdk-retirement-release-configure-r1.log` 和
`/tmp/pangrova-production-sdk-retirement-release-build-r1.log`。对重建后的实际
`lib/libvelox.a` record 为 PASS（`/tmp/pangrova-production-sdk-retirement-release-provenance-r1.log`）；
重建前的旧归档拒绝证明保留于
`/tmp/pangrova-production-sdk-retirement-old-archive-negative-r1.log`。
11 项 actual SDK Task owner 和 8 项 retirement 测试源码已收齐；retirement 实际 compile
database `-fsyntax-only -Werror` 通过不能计为 8 项执行通过。当前正在重建同一补丁的
匹配 SDK ASan 依赖；Main Native build/完整 CTest/ASan、打包、clean Java 全仓编译、
全量回归/真实 diff coverage、全部 E2E 和 activation smoke 仍未据此升级为 PASS。

### 0.42 2026-10-03 无筛选模块回归暴露的结构门禁

以原命令 `mvn -B -Pfull-regression -pl pangrova-security -am test` 扩大验证，r2 实际
**FAIL**：在进入 Security 模块前，test-common 83 项中 6 项失败、0 error/skip；
`/tmp/pangrova-production-security-full-module-r2.log`。发现的是当前工作区真实结构
问题，不能按“非 Security”忽略，也不能以专项 green 替代原命令通过：

- `RetrievalFanOutPlanBinder` 504 行超过 500，需要按职责抽取。
- `TaskSubmissionHandler.rollbackBeforeOwnership` 10 branches 超过 8。
- Protocol `function`/`substrait` 双向依赖，需纠正类型契约 owner，不能建立第二映射。
- Worker `exchange.remote` 17 files 超过 15，需要按真正子能力划分。
- `WorkerBootstrapLifecycle.State` 未被已知状态 owner inventory 收齐，须确认其与 SPI
  WorkerState 的不同生命周期，不能扩大忽略清单。
- Resource golden 仍要求已移除的 token-budget fields 13/14，未核保留号和当前完整
 实际 resource grant inventory。

Root 已将实际 pre-Native resource rollback 的“全部尝试、保留首个失败”职责抽至
`TaskAdmissionRollback`，仍在 memory/quota/cancellation cleanup 全部成功后才能移除
原 TaskInfo。还防止同一 Throwable 二次 self-suppression 导致剩余 cleanup 未尝试。
保持原 admission 27 项并增加 rollback 4 项，最近实际 JDK17
`-Xlint:all -Werror` 为 **31/31 PASS、0 skip、305ms**；
`/tmp/pangrova-production-task-admission-rollback-isolated-nearest-r1.log`。这不是 Handler
完整 Worker/JNI 执行证明。Resource golden 改为当前全部 live field 的确定性 exact
inventory，并断言已退役的 13/14/17 编号及名称保持 reserved，没有恢复 token-ledger。
最近 repo golden+metrics 近测为 **13 项 1 FAIL**：8 个 golden 通过，rollback branch
gate 恢复，尚存 Binder 504 行；
`/tmp/pangrova-production-repo-golden-rollback-closest-r1.log`。其它结构根因仍在修复，
须保持阈值和原测试 oracle，在源码稳定后完整重跑原无筛选模块命令，再完成最终
全仓链路。

### 0.43 2026-10-03 模块原范围复跑与许可所有权根因

原无筛选 Security reactor 命令继续保持不变。r3 在 test-common 86 项中 **5 FAIL**：
两个 Linux bootstrap 测试进程错误使用 Python 3.9；其余为 PlanFragmenter 509 行、
AI invocation 18 files 和 SPI SnapshotState owner inventory。合规 Python 3.12 PATH
及职责修复后，r4 上述五项消失，但仍为 **86 项 2 FAIL、0 error/skip**：
`JdbcArtifactBuildStore` 501 行、Worker AI function 包 21 files 超出已固定 20。
日志 `/tmp/pangrova-production-security-full-module-r3.log` 和
`/tmp/pangrova-production-security-full-module-r4.log`。不得据专项通过或旧失败消失
宣称原模块完整通过。Store 将 execution-route 只读查询移至已有 Reader，保同一
DataSource、SQL、异常和事务语义；函数包仍按真实治理/回调职责归属调整，不提高阈值。

Linux Docker SDK bootstrap 的实际 Python import closure 原本少复制四个依赖。原真实
隔离文件生成测试 r1 **2 项 1 FAIL**（ModuleNotFoundError），补齐 Docker COPY 后 r2
**2/2 PASS、0 skip**，r4 原模块范围也实际通过这两项。日志
`/tmp/pangrova-production-linux-bootstrap-imports-nearest-r1.log`、`r2.log`。这是 early
bootstrap 的依赖闭包证明，不是完整 Linux 镜像或部署验收。

Protocol 类型公式、签名、物理投影、同源布局命名和 retrieval score 近测实际
**49/49 PASS、0 fail/error/skip、13.128s**，日志
`/tmp/pangrova-production-canonical-function-type-signature-nearest-r1.log`。此结果先于
后续 canonical URN 源码变更，不代表完整 Native 函数目录、GETCAP 或 eligible cohort。

Root 复现了另一个真实容量错误：QueryThrottlingService.release 无发行 owner 校验，
重复返回、复制值及其它 service 的 permit 均直接增加 semaphore；32 个并发归还将
配置容量 1 变成 32。取得全局 slot 后的 identity/config 异常也会漏归还。原 13 项
与新增 6 项一起 r1 **19 项 6 FAIL**。修复采用 exact-object live owner 登记和 finally
退还未发布 slot，配置和动态更新预先验证有限正 rate；r2 仍发现 Semaphore 构造允许
负初始值，显式校验后同范围 r3 **19/19 PASS、0 skip、131ms**。日志
`/tmp/pangrova-production-throttle-ownership-nearest-r1.log` 至 `r3.log`。新增独立 set
模型 300 次属性测试及 Maven planner 扩围尚待执行结果，不能由隔离近测推导分布式限流。

真实 PostgreSQL 最近验证 **23/23 PASS、0 skip、33.102s**：原 artifact store 13 项
保持不变，journal 10 项包含第二阶段 DELETE 权限失败时整事务 rollback、保留两个
owned rows、恢复权限后精确 retry/duplicate purge 和不删除其它 query，以及 closed
journal 拒绝 purge。日志 `/tmp/pangrova-production-journal-artifact-reader-nearest-r1.log`。
该结果证明 purge 事务失败可见性与既有 Reader 路由行为；不是 durable issued-task
authority、journal close/append/flush 或 Coordinator HA 的完整证明。

当前 SDK matching-ASan 仍在构建。Main Native Release/full CTest/main ASan/full CTest、
重新打包、全仓 clean Java install、全量 coverage regression、真实 E2E 和 smoke 均
不得记为 PASS；仍须按强制顺序完成。

### 0.44 2026-10-03 同源构建与实际失败继续复证

SDK 同一 32-file patch（SHA-256
`40b743fd39fcf37d341c9f63c27a176944fac91f640b2fbf0f87f883331aae9a`）的
Release 与 matching-ASan 依赖构建均已完成，后者实际 1565/1565 build steps、exit 0，
没有编译 warning/error；日志
`/tmp/pangrova-production-sdk-retirement-asan-matching-setup-r1.log`。
这是 SDK、ASan Abseil/Protobuf 依赖构建，不是 Main ASan 或 sanitizer 测试通过。

Main Native Release 原全目标 r1 因 exporter 使用错误命名空间下的 `sanitizeName`
而 **FAIL**。依 pinned SDK 的 `exec::sanitizeName` 声明修复后，同全目标 r2
**PASS、exit 0、0 compiler warning/error**，日志
`/tmp/pangrova-production-main-native-release-build-r1.log`、`r2.log`。
实际 Native 最近验证随后为 **59 项、58 PASS / 1 FAIL**：全 SDK 注册签名投影
将 ROW 子类型集合的声明路径错误当作已索引 substitution slot，提前要求 ordinal，
抛出 cardinality 异常。日志
`/tmp/pangrova-production-native-owned-signatures-nearest-r1.log`。
修复须区分 collection declaration 与最终 indexed slot，保留所有真实注册遍历，
并验证实际 `map_entries` nested ROW 与 variance intermediate ROW；不能放宽 decoder、
删除失败签名或把未知能力标成 executable。全 CTest、Main ASan、打包及全链仍待通过。

Java Protocol 最新近测 **83/83 PASS、0 fail/error/skip、11.735s**，覆盖最新
declaration/formula、task/descriptor 职责迁移；日志
`/tmp/pangrova-production-canonical-function-type-signature-nearest-r4.log`。
Python contract 近测 **73/73 PASS**。Planner 扩围 r2 实际
**104/104 PASS、0 skip**，包含精确 throttle owner 的 300 次独立模型属性验证；
日志 `/tmp/pangrova-production-planner-throttle-function-nearest-r2.log`。
最新客户端原范围复跑 **106/106 PASS、0 skip、21.781s**，包括迁移后的同一 auth
类型、真实结果 codec、TLS downgrade 拒绝及 CLI deadline；日志
`/tmp/pangrova-production-client-auth-consumption-nearest-r4.log`。
这些均是限定范围近测，不是全仓 regression 或 Native/mTLS 分布式 E2E。

Security 原无筛选模块范围继续保持不变：r5–r9 均 **FAIL**。此前 Factory 501 行、
JNI write、Protocol root/function、Client REST 包职责问题已按各自消费闭包修复，
但 r9 仍为 test-common **86 项、1 FAIL、0 error/skip**：Coordinator query 包
17 files 超过 15；独立按原 gate 规则枚举还发现 Coordinator metadata 包 17 files。
日志 `/tmp/pangrova-production-security-full-module-r5.log` 至 `r9.log`。
不得提高阈值或将 reactor 下游 SKIPPED 计为通过；应迁移真实独立 session/read-binding
职责闭包，并在 clean 编译及原范围回归中验证全部消费者。

生产链仍有已确认、尚未完成的缺口：函数目录尚未完整接入 GETCAP/Planner/任务 owner；
客户端未向实际结果 transport 提供 mutual-TLS CA/client certificate/private key；
journal 的 stable event identity、flush/close ownership、transition listener 与 terminal
report publication/purge 竞态需要同 issued scope 修复。真实 resource-drain receipt 不等于
journal 已提交，失败后的同 sequence 重试不能被提前 compact ACK 丢弃。独立进程 Spec、
durable issued/outbox、HA incarnation 及大规模 SLO 尚无完成实测。继续保持明确未闭环口径。

### 0.45 2026-10-03 完整 Native Release 与 Java 干净编译复证

注册签名投影修复区分实际 child collection 声明与有 ordinal 的 substitution slot，
没有放宽严格 decoder。原真实注册遍历全部保留，新增实际 nested ROW/variance 断言后，
最近验证 r2 **61/61 PASS**。完整 CTest r1 实际 **3918 项、3916 PASS / 2 FAIL**：
两个 literal 测试的值与拒绝行为正确，但生成的类型桥丢失具体字段、边界和实际值诊断。
唯一声明生成器现输出实际 kind/field/range/value，constraint 诊断保留实际表达式与参数；
原 decimal、temporal 无效值断言均保留。生成器修复首次编译 r4 因两个字符串 literal
相加而 **FAIL**，修正生成源后 r5 全目标 **PASS、0 compiler warning/error**，
Python **75/75 PASS**、扩围 Native **84/84 PASS**。

同源码完整 CTest r2 **3918/3918 PASS、无跳过、520.01s、exit 0**，日志
`/tmp/pangrova-production-main-native-release-full-ctest-r2.log`；相关首次失败与修复日志
仍保留在 `main-native-release-full-ctest-r1.log`、`main-native-release-build-r4.log`
及 `r5.log`、`function-contract-diagnostics-python-r3.log`、
`native-owned-signatures-nearest-r3.log`。这证明该 Release checkpoint，后续源码变更须重跑。

Coordinator 同依赖 reactor 的原命令 `mvn -B -pl pangrova-coordinator -am clean compile
test-compile`：r1 **FAIL**，两个真实 epoch 测试定义同一类名；分别改为 admission
与 cleanup 测试类后原 11 项断言全部保留。r2 **FAIL**，旧测试调用遗漏正式内部认证
上下文、speculative fixture 仍调用已删除的假取消 ACK API。前者迁移到实际 HTTP
server + AuthenticationFilter，后者暂以明确 FAILED 表达没有真实 drain owner，仅完成
编译对齐，不能算 speculative 测试闭环；原 14 项仍需真实独立进程替换。r3
**29-module reactor 全部编译及 test-compile PASS、59.658s**，日志
`/tmp/pangrova-production-coordinator-checkpoint-clean-compile-r3.log`。

原无筛选 Security 模块命令 `mvn -B -Pfull-regression -pl pangrova-security -am test`
r10 **FAIL**：package gate 真实统计包括两个已跟踪的 0-byte Java 文件，Paimon 为
25 files。两个空文件无代码、消费者或测试引用，按死代码策略删除，未调整 gate pin。
r11 **7 个实际测试模块合计 1995 PASS、0 fail/error/skip、02:56min、exit 0**，日志
`/tmp/pangrova-production-security-full-module-r11.log`。这是原范围模块回归，不是全仓回归。

Main ASan configure r1 **FAIL**：执行命令错误地将仅包含 ASan Abseil/Protobuf 的前缀
当作含 RE2/Arrow 的完整依赖前缀。纠正后 r2 **FAIL**：SDK 已记录同一 canonical RE2
真实路径，但 CMake 的合法 `UNINITIALIZED` command-line cache type 被 gate 错判为缺失。
gate 现接受 PATH/UNINITIALIZED 并继续复验真实路径及实际编译链接 ABI；两 bootstrap
明确输出 PATH。新增真正 CMake configure 测试验证两种类型接受、异路径与缺失选择拒绝，
**4/4 PASS**；首次近测命令误指定不存在的 cmake 路径为 FAIL，未隐藏记录。
日志 `canonical-re2-config-nearest-r1.log`、`r2.log`。
Main ASan configure r3 **PASS、0 configure warning/error**，日志
`/tmp/pangrova-production-main-native-asan-configure-r3.log`，使用已复证的 SDK32 与匹配
ASan Abseil/Protobuf，保留 canonical RE2/Arrow 实际前缀。当前 Main ASan 全目标构建
仍在执行；sanitizer 全 CTest、新增 gate 后 Release 复跑、重新打包、全仓 clean install、
完整 coverage regression、真实 E2E、smoke 和多节点性能资格仍未完成，不得记为 PASS。

### 0.46 2026-10-03 依赖 ABI 根因与客户端真实终止复证

上一节的 Main ASan build 现已结束为 **FAIL**，并非仍在执行：r1 链接缺少
`EVP_MD_get_size` 与直接调用的 gRPC ServerBuilder API。SDK 使用 OpenSSL 3，但
Main 在 OTel 关闭时选中了旧 libcrypto；shared Arrow Flight 也未声明服务器 hook
直接依赖的 gRPC。现将 OpenSSL 选择从 OTel 开关中移出，Flight 的直接 gRPC package
依赖不再随 OTel 消失。configure r4 PASS 后，build r2 又实际 **FAIL**，出现 160 个
RE2 duplicate symbol；日志 `main-native-asan-build-r1.log`、`r2.log` 均保留。

实物归档检查确认 canonical `libre2.a` 中有 **22 个重复 object member**：曾将两个
版本的对象追加进同一归档，单点 ABI probe 无法证明全归档一致。实际 SDK 引用
`std::string_view` 接口，旧 gRPC 引用 `re2::StringPiece` 接口；完整旧 package 已备份。
修复为同一 pinned RE2 `2024-07-02` 完整安装，并让 gRPC 使用 package provider 重建；
不得以重复符号豁免、追加对象或更换执行路径掩盖。新增归档完整性 gate 后，原四个
最近 configure 测试对损坏包实际 **2 FAIL / 2 PASS**，保留 red 证明；新增真实归档复制、
追加同一对象的 negative oracle，待完整重建后执行。

Main ASan 实际 link closure 还混入 SDK 的 Release Abseil archives。Release、ASan、
SDK 三处 public headers 全目录比较均无差异，但这不代表归档 instrumentation 一致。
现在要求 SDK 记录的 Abseil package 与 Main 一致，SDK 的 `libabsl_*.a` 逐一映射到
该 package 的实际 imported target，不再混入第二组 Release archive。真实 ASan package
及 SDK archive inventory 的最近检查 **4/4 PASS、2.199s**，日志
`/tmp/pangrova-production-canonical-abseil-asan-package-nearest-r1.log`；不是 Main ASan
构建或 sanitizer 测试通过。

完整 dependency bootstrap r1 意外启动非必要 Homebrew 工具升级，已终止准确进程树
（exit 143），没有完成依赖重建。现改为只校验已安装工具，`--check-tools` 实际 PASS，
不再自动调用包管理器安装/升级。Homebrew 自动产生的 stash 保留以便恢复，未盲目
覆盖原状态。完整 RE2/gRPC package bootstrap r2 已实际完成 package 安装和 ABI
executable，但最终 **exit 2 / FAIL**：Root 在运行期间修改同一个 shell 文件，导致
Bash 读取尾部 offset 失配。当前源码 `bash -n` PASS；已冻结脚本并按原范围重跑 r3，
仍未记为 PASS。第三方依赖的 upstream deprecation warnings 单独保留，未称零警告构建。

完整现代 RE2 最近检查 r4 **3 FAIL / 2 PASS**，暴露 ABI probe 漏声明真实 Abseil
package 依赖；补齐后 r5 **1 FAIL / 4 PASS**，negative fixture 的 symlink relocation
错误改变 Abseil imported archive prefix。现显式选真实 Abseil package 并仅复制故障
目标 RE2；原五项 r6 **5/5 PASS、4.340s**，包括真实重复 object 拒绝，日志
`/tmp/pangrova-production-canonical-re2-package-nearest-r6.log`。Release Abseil package
最近检查亦 **4/4 PASS、2.350s**（`canonical-abseil-release-package-nearest-r1.log`）。
实际 canonical RE2 归档重复 object 数为 0，gRPC 的实际未解析 RE2 ABI 已切换为
与 SDK 一致的 `std::string_view`；不是仅凭同一路径推断一致。

另删除 Native CMake 的 headers-only/disabled Flight source 排除及 RPATH 旁路：缺少
Flight library 必须 configure 失败，不能产出无法结果传输/远端交换的半执行 runtime。
实际完整 Flight/gRPC package 接受、仅真实 headers 拒绝、缺 package 拒绝最近
**3/3 PASS、1.890s**，日志 `/tmp/pangrova-production-canonical-flight-package-nearest-r1.log`；
这些是依赖 admission 证明，不是 Native 运行或网络 E2E。

实际结果 transport 的 PEM CA/client chain/key 从 REST/CLI/JDBC 正式消费至唯一
官方客户端构建入口。最初新 fixture 使用的 EC PEM 不被 JDK 17 接受，最近 r1 为
**6 ERROR**，下游模块 SKIPPED；迁移到实际 JDK keytool 生成并由 CA 签名的不同
server/client leaves 后，r2 **126/126 PASS**。进一步通过官方公开 builder 保留实际
ManagedChannel owner，shutdown + bounded awaitTermination + isTerminated 成功后
才能关闭 SDK client/allocator，失败保留 owner 供重试；没有反射或第二传输栈。
同一原 selection 的 clean reactor r3 **127/127 PASS、0 failure/error/skip、45.491s**
（Transport 61、REST 7、JDBC 21、CLI 38），日志
`/tmp/pangrova-production-client-result-tls-owner-nearest-r3.log`。包括实际 mTLS、错误 CA、
过期证书及 channel termination；仍不是 SQL→JNI 或独立进程 E2E 证明。
其后将 stream cleanup 的实际 executor scheduling 移至锁外，并用同一 FutureTask
identity 防止旧失败 waiter 擦除新 cleanup retry；增加实际已 shutdown executor 的拒绝
测试。原 127 项全部保留，原 selection 的 clean r4 **128/128 PASS、0 failure/error/skip、
42.150s**（Transport 62、REST 7、JDBC 21、CLI 38），日志
`/tmp/pangrova-production-client-result-tls-owner-nearest-r4.log`。包含最新 common/SPI
声明编译，不代表尚在迁移的 Coordinator/Metadata 行为通过。

完整 dependency bootstrap 原范围 r3 已 **exit 0 / PASS**，日志
`/tmp/pangrova-production-canonical-re2-grpc-whole-package-setup-r3.log`，actual RE2 ABI
executable 成功；没有构建 SDK/Main Native 或将第三方 deprecation warning 消除。
Repository adapters、SPI annotation（139 types、baseline 0）与 E2E truthfulness 静态
gates 最近 r3 PASS；静态 E2E gate 不是实际容器/SQL 测试结果。

先前干净编译 checkpoint 的 HTTP/epoch/session 近测 **38/38 PASS**、metadata read
bindings **38/38 PASS**、query owner/journal **34/34 PASS**。直接 surefire 曾加载旧
`.m2` dependency 引发 FAIL，保持原 selection 并先从同一 reactor compiled baseline
生成 jars 后通过；未隐藏首次失败。这些结果早于当前 journal/FSM/shutdown 原子迁移，
不得作为新迁移行为通过的证据。

当前完整函数目录/owned special 与 aggregate/window、runtime callback retirement，
以及日志提交/状态发布/实际执行退出迁移仍在实施。SDK32/3918 Release 和上述 Java
结果均只是明确源码 checkpoint。最终同源 Release、无筛选 CTest、ASan、打包、全仓
clean install、coverage regression、全部 E2E、ActivationSmoke、独立多节点故障与规模
SLO 验收尚未完成，继续保持未达到生产发布标准的结论。

### 0.47 2026-10-03–04 函数生成输入证明与 HTTPS 信任链复证

旧 SDK provenance 仅绑定 commit、patch 和 archive，未覆盖函数声明及实际生成器输入；
修改声明后，旧 archive 可能被新源码重新认证。现源码改为严格五行证明，包含版本、
commit、patch、实际 Python import closure/declaration 指纹及实际 archive SHA256。
新增 SDK out-of-line identity translation unit 与实际链接 archive 的 inspector：不能
读取新头文件就为旧库盖章。Release/ASan bootstrap、Docker 输入和 Native CMake
消费同一证明；三行旧证明严格拒绝，须保留历史证据后真实重建。
这些改动目前为 **Release SDK 已真实重建、matching ASan SDK 构建中、Main 当前 cohort 构建未执行**；`bash -n`、`git diff --check`
PASS 不是归档 identity 的执行证明。实际 Python 3.12 最近测试 **85/85 PASS、0 skip、
0.193s**，日志 `/tmp/pangrova-production-function-generator-input-provenance-nearest-r1.log`，
包含 transitive generator 输入变化与越界 import 拒绝；不计为 Native 或 E2E PASS。

JDBC 自定义 HTTPS JKS/PKCS12 原先会被共享 HTTP client 覆盖。现显式信任库使用
独立 SSLContext，系统信任路径继续共享；拒绝 shared client 与 explicit store 混用、
HTTP 上未消费的 store、空路径和孤立 password。真实 `HttpClient.send` 的 entered
request 保持 owner，关闭封住新 admission，只有请求退出及已分配 executor 真正终止
后才能成功返回，失败保留可重试 owner。JDK 17 HttpClient 没有公开 socket-close API，
不把 executor termination 说成 TCP 全部退出。新增真实 HTTPS/证书/连接隔离/并发关闭
19 项，原 128 项保留。新 147 项 selection 的 clean reactor r1 实际 **FAIL**：
Transport 原 62 项中 61 PASS / 1 ERROR，`FlightResultFetcherDeadlineTest` 在 close
等待实际 stream cleanup 时 TimeoutException；下游 REST/JDBC/CLI 模块 SKIPPED，
新增 HTTPS 测试尚未执行。日志
`/tmp/pangrova-production-client-result-tls-https-jks-owner-nearest-r1.log`；继续查真实
reader/channel/executor 退出根因，不增加 timeout 或减少原 selection。

独立 SDK54 依赖源码冻结后，真实 prior32→54 apply r1 **exit 1 / FAIL**：upgrade
完成后，精确 blob 校验发现脚本 `patched_blobs` 数组错位/重复，不等同于补丁编译失败。
日志 `/tmp/pangrova-production-sdk54-owned-publication-apply-r1.log` 和
`/tmp/pangrova-production-sdk54-owned-publication-blob-mismatch-r1.log`；须以 sole patch
在独立 pinned base 上实际应用的 blobs 核实，而非把当前树直接认证。旧三行 sidecars
及两个归档当时尚未替换，SDK/Main 重建当时未启动。

SDK54 apply 原范围 r2 **PASS**：独立 pinned tree 的 54 个实际 blobs 与升级后的
source 完全一致，修正的是预期表而非放宽校验；新增所有表 cardinality 检查。
old Release archive 的实际 inspector 链接 **FAIL**，缺失 out-of-line generation
identity，证明新头文件不能认证旧库；日志
`/tmp/pangrova-production-sdk54-old-archive-identity-negative-r1.log`。
旧两份三行证明已可恢复地移入
`/tmp/pangrova-production-sdk32-provenance-backup.UQzvXr`，归档进入真实重建。
完整 Release setup r1 **FAIL** 于 Folly ABI gate：实际 archive 有 3 个匹配符号，但
`nm | grep -q` 在 `pipefail` 下实测为 **74 / 0**，提前关闭 pipe 造成误报和无谓重建。
改为完整消费 `nm` 输出后最近 pipeline 实测 **0 / 0**。
保留第三方 compiler warnings，不作为 SDK/Main 零警告成功。

原完整 setup r2 **FAIL**：上游 sourced `setup-macos.sh` 重赋通用 `SCRIPT_DIR`，
导致 Pangrova declaration/generator 路径错误指向 `.pangrova`。修复本项目 bootstrap
为专属 `PANGROVA_BOOTSTRAP_SCRIPT_DIR`，不修改上游变量或跳过原依赖阶段；同范围
r3 原完整范围实际 **PASS / exit 0**，日志
`/tmp/pangrova-production-sdk54-owned-publication-release-setup-r3.log`。
实际 Release archive SHA256 为
`75b0ca2786f469e10ae739587d0a0a493b6187152c0649e717cdf0c69cc7bb0b`，
actual generator input SHA256 为
`08ad9cfb45bad4aefff6f065ed74be7c9849c69aab4b85baf1261e3350090ac5`；
五行 sidecar 由实际链接该 archive 的 out-of-line identity inspector 产生。
上游第三方 compiler warnings 仍保留，此结果不是 Main 零警告、CTest 或 E2E 的证明。
同 SDK54 输入冻结后的 matching ASan setup 已实际 **PASS / exit 0**，完成
1503 个构建节点，日志
`/tmp/pangrova-production-sdk54-owned-publication-asan-matching-setup-r1.log`。
实际 ASan archive SHA256 为
`f32e84966c44c474457fad845e6f77edc31758e71d06a31cd7c6218a8a9921ee`，
匹配的五行 proof 使用同一 patch/generator 输入。该实际归档的 provenance 最近
**6/6 PASS、0 skip、38.895s**，日志
`/tmp/pangrova-production-sdk54-asan-actual-archive-provenance-nearest-r1.log`。
这是 ASan 依赖构建及归档准入证明，不是 Main ASan CTest 或 E2E PASS。

原 deadline 单类的 compiled-baseline 两次诊断均 PASS，未复现完整 selection 的
失败。第二次 thread dump 误选进程，attach 失败，不是有效线程退出/SDK race 证明。
直接核对 pinned Arrow 19 源码确认 `next()` 取出 DONE_EX 后的 `queue.put` 可被
线程中断，close 又需 drain；超时取消应针对真实 RPC，不应打断 SDK reader。
现正式路径改为 `stream.cancel` 后 `future.cancel(false)`，真实 reader ownership 仍
保持至其 finally 退出。原 3s deadline / 5s 等待不变，同测试增强实际 producer 退出、
executor join、第二次读取 42/EOF、allocator 归零。原 147 项 clean reactor r2
**147/147 PASS、0 failure/error/skip、50.127s**（Transport 62、REST 17、JDBC 30、
CLI 38），日志 `/tmp/pangrova-production-client-result-tls-https-jks-owner-nearest-r2.log`。
不把未捕获的 sentinel 竞态写成实测证实，也不将组件测试当 SQL/JNI E2E。

通用 `ExecutorDrain` 最近真实线程测试 **6/6 PASS、0 failure/error/skip、0.068s**，
clean common reactor 3.150s；日志
`/tmp/pangrova-production-common-actual-executor-drain-nearest-r1.log`。覆盖真实退出、
timeout 保 owner、interrupt 恢复、自 join 拒绝、Future done 不等于退出、null 输入
在改变 admission 前拒绝；Coordinator/Metadata 新关闭图尚未由此验证。
Root 新增 `canonical_function_provenance` CTest，使用实际 SDK archive/proof，并拒绝
三行证明、输入/归档指纹漂移、额外行和真实 selected header 漂移。对上述实际
Release archive 的最近检查 **6/6 PASS、0 skip、11.555s**，日志
`/tmp/pangrova-production-sdk54-release-actual-archive-provenance-nearest-r1.log`。
此证明只覆盖真实 archive provenance gate，不计为 Main Native、ASan 或 E2E PASS。

JDBC `poolSize` 现严格解析并落实为 owned HTTP executor 的线程数，不与单 endpoint
连接上限混淆；系统共享 owner 的显式冲突值拒绝，省略值沿用已创建 owner。共享
关闭不再持锁等待线程，也不在 timeout/interrupt 后丢弃 owner；封住 admission 并
保留实际 executor，真正 termination 后才能重建。原 147 项 + pool 配置 6 项 +
原 SharedClient 5 项及其新增 4 项（整类 9 项）同 selection clean reactor
**162/162 PASS、0 failure/error/skip、52.970s**（Transport 62、REST 19、JDBC 43、
CLI 38），日志
`/tmp/pangrova-production-client-result-tls-https-jks-shared-owner-nearest-r1.log`。
Driver deregistration 的 connection-aware lease 尚未接通，未添加会杀死其他现存连接
的全局 DriverAction；JDK 17 socket 关闭限制继续保留，不声明全客户端生产闭环。

journal/FSM/report/shutdown 有限源码单元已冻结待统一 clean 编译及真实 PostgreSQL
验证。Native 既有测试正在显式传递实际注册后的同一函数 snapshot owner；不保留
无参 converter 或生产路径中的隐式 registry 捕获，不删弱原值/NULL/资源断言。
上述 Native fixture 目前 **UNRUN**。Metadata 真实 PostgreSQL clean nearest r1
**15/15 PASS、0 failure/error/skip、25.945s**，包括保留原 10 项、实际服务端 COMMIT
响应丢失后的同事件 ID/全 payload 核对、唯一行及原 ordering ID 证明；日志
`/tmp/pangrova-production-journal-commit-owned-pg-nearest-r1.log`。其 reactor 不包含
Protocol/Planner/Worker，不代表 Coordinator 关闭图、Native 或跨进程 HA 通过；
Coordinator 完整 clean 编译待 C 的同源 Java 子单元完成后执行。

SDK54 Docker early-COPY 的真实生成器输入最近 clean test-common reactor
**2/2 PASS、0 failure/error/skip、9.122s**（单类 0.400s），正向执行实际复制闭包、
负向移除实际 transitive Python 输入后拒绝。日志
`/tmp/pangrova-production-sdk54-bootstrap-copy-journal-spi-nearest-r1.log`。
该 selection 中上游没有匹配的 journal SPI 测试类，不能将它们写成已执行；此证明
仅覆盖 early-COPY 生成器闭包，不覆盖整个镜像、实际 SDK inspector 链接或 E2E。

扩大到原范围无筛选 Metadata reactor r1 实际 **FAIL**（1:05min）：TestCommon
86 项中 3 failure，`QueryBootstrap` 506 行超过 500、关闭方法 10 branches 超过 8、
task split 子包 12 files 超过 8。Security/Governance/Metadata 下游 **SKIPPED**；日志
`/tmp/pangrova-production-journal-metadata-reactor-unfiltered-r1.log`。须按已存在职责拆分
而非修改阈值、删除断言或压缩格式规避；待修复后重跑同一完整 reactor。此前真实
PostgreSQL 15 项的近测 PASS 仍只是该源码检查点，不能覆盖这次扩大回归的 FAIL。

原无筛选 Metadata reactor r2 仍 **FAIL**：前三项已消除，但 TestCommon 86 项中
1 failure，`TaskIdentityRegistry` 504 行超过实际 gate 的 500；下游仍 SKIPPED。
日志 `/tmp/pangrova-production-journal-metadata-reactor-unfiltered-r2.log`。
现将原报告事实原样移入既有 `TaskReportPublication.Fact`，保留唯一 Registry
monitor、权限和发布状态；Registry 降至 463 行。原范围 r3 实际 **PASS / exit 0**，
12 个 reactor 模块均 SUCCESS，9 个测试模块合计 **2631 项、0 failure/error/skip**，
其中 Metadata 524 项；总耗时 **8:17min**。命令仍为
`mvn -B -Pfull-regression -pl pangrova-metadata -am test`，日志
`/tmp/pangrova-production-journal-metadata-reactor-unfiltered-r3.log`。
此结果不包含 Coordinator/Protocol/Planner/Worker，不替代其 clean 编译、最新 JNI、
跨进程故障测试或全仓 E2E。

同模块 `mvn -B -Pfull-regression,coverage -pl pangrova-metadata -am verify`
最近 **PASS / exit 0、8:48min**，12 个模块均 SUCCESS，9 个真实 JaCoCo bundle
report 已生成；当前执行 **2625 项、0 failure/error/skip**（TestCommon 80，其他
模块计数不变）。日志
`/tmp/pangrova-production-journal-metadata-reactor-unfiltered-coverage-verify-r1.log`。
计数变化的已核实原因：工作区已有 `CleanCodeMetricsPropertyTest` 删除（5 项）和
`NamingConventionPropertyTest` 修改（3→2）；原 r3 testCompile 显示 Nothing to
compile，运行了旧 class，此 verify 重新编译 18 个 test-common source 文件。
本修复单元未删除或弱化这些门禁，也不覆盖其他工作区改动；原 2631 项结果只能
保留为 compiled-baseline 检查点，不能充当当前 clean 源码门禁证明。此 verify
没有先 clean，不能证明 JaCoCo execution data 仅含当前一次执行；全仓当前源码
changed-line coverage 仍须在最终 clean cohort 上度量，不计为全仓覆盖率 gate PASS。

Native fixture 已显式使用 sole declaration 的 URN/name/kind，并按实际 IR 调用类型
区分 scalar/aggregate/window；NULL-safe predicate 按唯一声明 DAG 展开，不在生产
decoder 中恢复短名猜测或隐式 registry。新增 adapter 正向与非法 URN 拒绝测试及
原值/NULL/资源断言目前均 **UNRUN**，须进入真实 Native 编译和执行。
进一步追踪发现 pinned SDK 的最终 `PartitionedOutput` 仍创建默认 serde options，
未携带任务的 canonical schema；只修显式 shuffle serializer 不能闭合结果边界。
typed options 的唯一任务 owner 接线仍在设计/实施；当前 SDK54 冻结产物仅为依赖
检查点，若新增 SDK seam，所有最终构建、catalogue 和验收必须来自新同源 cohort。
实际 pinned `Bridge.cpp` 的字段导出默认 `ARROW_FLAG_NULLABLE`；当前 serializer
虽然已验证部分 canonical required 值，却未将递归 nullability 应用到真实 Arrow
schema，deserialize 的 `Options*` 也尚未消费。物理 RowType equality 无法证明
wire nullability 一致。这两处已纳入同一结果/Exchange 修复单元，须用真实 IPC
schema flags、required NULL 拒绝及晚批次失败不改写已发布 result 的测试复证。

仓库 instruction check 和 E2E truthfulness 源码 gate 本次均 **PASS / exit 0**，
日志分别为 `/tmp/pangrova-production-repository-instructions-check-r1.log` 和
`/tmp/pangrova-production-e2e-truthfulness-source-gate-r1.log`。后者只证明其静态
模式未发现禁用、常量断言及伪多 Worker 标签，不证明测试实际执行、覆盖全部能力，
也不能将旧组件直连测试计为独立进程 E2E。

SDK54 Main Release dependency configure 最近 **PASS / exit 0**，日志
`/tmp/pangrova-production-sdk54-main-catalogue-dependency-configure-r1.log`；
仅 `pangrova_function_export --parallel 1` 的真实构建 r1 **FAIL / exit 2**：
`SubstraitFunctionConversion.cpp:61` 的 `checkedScalarCall` 声明仍为 static，
实现已使用实例持有的 `function_registry_`，编译器拒绝该所有权断裂。
日志 `/tmp/pangrova-production-sdk54-main-catalogue-dependency-build-r1.log`。
核对全部调用者后修正为显式实例 owner，不捕获全局 registry、不跳过该对象。
原目标 r2 **FAIL / exit 2**，`CanonicalResultNullability.cpp:62` 比较真实
`size_t childrenSize()` 与 protobuf 非负 `int types_size()`，被 `-Werror` 拒绝；
根因修复为在 protobuf count 边界显式转 `size_t`，保留 arity 拒绝，不压警告。
日志 `/tmp/pangrova-production-sdk54-main-catalogue-dependency-build-r2.log`。
同一目标 r3 **PASS / exit 0**，未发现 compiler warning/error，日志
`/tmp/pangrova-production-sdk54-main-catalogue-dependency-build-r3.log`。
真实 executable `--catalogue` r1 **PASS / exit 0**：生产 registration/snapshot/
assembler 输出 **781837 bytes**，base64 后 **1042452 bytes < 4 MiB**，不是手工
manifest。实际目录 payload SHA256（非 exporter/JNI 二进制身份）为
`145a00aeec0da64fb688a4ff51651f83963c8ac2bbcd7da9a2261b2f6b79fa00`，
日志 `/tmp/pangrova-production-sdk54-actual-capability-catalogue-r1.log`；此独立
exporter 没有 JVM AI callback，不能将其清单中的不可用 AI 条目当作已接通。
仍未完成当前 Main 测试、JNI package、Java 清单消费、E2E 或最终新 SDK cohort。

Java canonical nullability 最近原 4-class / 24 项 clean selection r1 **FAIL**：
`readFilterUsesBaseSchemaBeforeItsPermissionProjection` 把 `NamedStruct` 外层容器
直接作 expression ROW，strict value nullability 因其 UNSPECIFIED 拒绝正常字段引用。
根因修复先严格准入所有真实 children，仅为 root reference 的局部容器视图赋
REQUIRED；原 IR 不变，expression-root ROW 没有该例外，nullable ROW 仍保持 nullable。
原 selection r2 **24/24 PASS、0 failure/error/skip、12.210s**，含 2 个各 300 次
检查的 properties。日志分别为
`/tmp/pangrova-production-canonical-nullability-admission-protocol-nearest-r1.log` 和
`/tmp/pangrova-production-canonical-nullability-admission-protocol-nearest-r2.log`。
这不是 planner/worker/JNI 或全仓当前 cohort 的验收结果。

真实 SDK54 exporter binary 作为唯一 protocol test resource 后，完整 catalogue
最近 clean 验证 **10/10 PASS、0 failure/error/skip、13.406s reactor**，日志
`/tmp/pangrova-production-sdk54-actual-function-catalogue-protocol-nearest-r1.log`。
紧邻将 catalogue 校验接入 `CapabilityInventoryCodec.validate` 的正式入口，保留
原编解码/跨节点 DTO/property 正负断言，最近 **65/65 PASS、0 failure/error/skip、
17.957s**，日志
`/tmp/pangrova-production-sdk54-actual-function-catalogue-codec-protocol-nearest-r1.log`。
缺 manifest、unknown/duplicate/drift availability 均 fail-closed；不以空清单认证。
这些是实际 exporter 目录 payload 的协议消费证明，不是已打包 JNI 或完整 Planner 门禁。

当前 Native fixture build r1/r2 分别 **FAIL / exit 2**：MarkDistinct 与 SumZero
helper 使用另一方法局部的 registry，编译器拒绝所有者作用域断裂。修复为转换与
执行显式传同一 shared snapshot，保留原数值、NULL、partial/final 断言；未再次捕获
全局 registry。r3 编译通过但有 1 个 `getConnector` deprecation warning，不能计
零警告；改用 SDK 正式 `ConnectorRegistry::tryGet` 后 r4 **PASS / exit 0、未发现
compiler warning/error**。日志为
`/tmp/pangrova-production-sdk54-native-canonical-fixtures-build-r1.log` 至
`/tmp/pangrova-production-sdk54-native-canonical-fixtures-build-r4.log`。
该 binary 的原 44 项最近 CTest r1 **35 PASS / 9 FAIL / 0 skip、15.20s**：6 个
GET_CAPABILITIES 正测没有实际 startup finalize；2 个 nullability fixture 重复创建
仍活跃的同名 leaf pool；1 个 allocator oracle 在不跟踪用量的 system pool 上断言
tracked usage。对应修复分别是实际 runtime incarnation/command 22 发布边界、每项
测试单一 owned pool 贯通所有 vector、managed tracked root/leaf；生产 GET 不恢复
懒捕获、不放宽 required NULL 校验。新增 read-only GET 不能替代 finalize 和 repeated
finalize 不能替换 publication 的两项真实 dispatcher 验证后，原 scope r2
**46/46 PASS、0 skip、16.36s**，日志
`/tmp/pangrova-production-sdk54-native-canonical-fixtures-nearest-r1.log` 和
`/tmp/pangrova-production-sdk54-native-canonical-fixtures-nearest-r2.log`。
这是 SDK54 已链接 binary 检查点，不是下一 typed-serde SDK cohort 或全量 CTest。

跨 module 真实 catalogue 最近 29-module reactor r1 **FAIL** 于 Coordinator
`CoordinatorShutdownSequence.stopHttp` 未处理 `CoordinatorHttpServer.stop()` 的
checked Exception；Protocol 16、test-common 1、Worker 17 项均实际 PASS / 0 skip，
Coordinator 的选择/关闭测试尚未执行。现实际 callers 是 Consumer/Runnable，没有
可承接 checked throws 的 CheckedAction；修复在该 HTTP 边界保留 Runtime/Error，
仅 checked Exception 带原 cause 包装，交现有失败保 owner/try-all suppression 路径。
日志 `/tmp/pangrova-production-sdk54-actual-function-catalogue-consumers-coordinator-nearest-r1.log`；
同 scope r2 **FAIL** 于 5 个测试调用仍使用已移除的 `ExecutorDrain.close`；核对
全部调用者后迁移至唯一 `shutdownAndAwait(executor, 3000)`，不增加兼容 API，
不改变原 3 秒期限。r3 **60/60 PASS、0 failure/error/skip、40.392s**，29 个
reactor modules 均 SUCCESS，日志
`/tmp/pangrova-production-sdk54-actual-function-catalogue-consumers-coordinator-nearest-r2.log` 和
`/tmp/pangrova-production-sdk54-actual-function-catalogue-consumers-coordinator-nearest-r3.log`。
其中 Coordinator 26 项，包括实际 report publication 退出、scheduler executor
terminated/rejected 与 HTTP/allocator 关闭；它们不是 JNI 或独立进程 E2E。

SDK54 已链接 Native binary 的扩大执行检查点实际 **1279 PASS / 3 FAIL / 0 skip、
93.89s**，日志
`/tmp/pangrova-production-sdk54-native-canonical-execution-checkpoint-r1.log`。
三个失败均在原 literal 拒绝测试：未绑定 anchor 被混同为错误 invocation kind、
缺失 scalar 被测试 adapter 提前拦截，以及 special-form 拒绝仍期待旧文本诊断。
根因修复分别保留独立 unbound/kind 分类、对实际 qualified IR 注入不存在的声明，
以及验证实际 SDK USER/INVALID_ARGUMENT、非重试与 resolveDeclaredType 阶段。
原非法输入、类型和数值断言保留；该修复当前 **UNRUN**，不将旧 1279 项折算为
新 typed-serde cohort 的通过证据。

Coordinator journal/lifecycle 扩大最近实际 **99 PASS / 1 FAIL / 0 skip**（100 项，
真实 PostgreSQL，65 秒），日志
`/tmp/pangrova-production-coordinator-journal-lifecycle-pg-nearest-r1.log`。
`QueryTerminalJournalIntegrationTest.failedTerminalJournalCanBeRetried` 在权限拒绝
后的异常因果链断言失败；正核查原失败分类和持久化 retry 状态，不放宽超时或
将日志组件测试冒充 Native 全链验收。
已核实根因是测试要求 direct SQL cause，而正式 SPI 保留
`ACCEPTED_UNESTABLISHED` 和 root `SQLException` / SQLSTATE `42501`。
测试改为同时校验这些事实与原 active owner、数据库 0→1、retry/purge 断言；
生产状态机未改。原两项最近 r1 因本次命令遗漏 Python 3.12 PATH 而在生成器
前置 FAIL，实际 0 tests；纠正环境后 r2 **2/2 PASS、0 skip、52.360s reactor**，
日志 `/tmp/pangrova-production-coordinator-terminal-journal-pg-nearest-r1.log` 和
`/tmp/pangrova-production-coordinator-terminal-journal-pg-nearest-r2.log`。
原 18-class 范围 r2 **100/100 PASS、0 failure/error/skip、73s reactor**，日志
`/tmp/pangrova-production-coordinator-journal-lifecycle-pg-nearest-r2.log`；
不折算为匹配 JNI、全关闭故障图、全仓 clean coverage 或 E2E。

SDK typed-serde seam 的 sole patch 现覆盖 57 个精确源码 blobs，SHA256
`4637a1cb1b18f856862491ec984793899b85770a24cf69ea89685200a300df6d`。
实际安装源码的 apply/verify **PASS**；旧 SDK54 archive 的 record **exit 1 /
预期拒绝**，日志
`/tmp/pangrova-production-sdk57-typed-serde-apply-r1.log` 和
`/tmp/pangrova-production-sdk57-old-sdk54-archive-rejection-r1.log`。
Exchange、PartitionedOutput、MergeExchange 从实际 query/node 取独立 serde
options；Pangrova 的 required owner 接线、真实 Arrow 递归 schema 和读端校验
已通过下述有限专项。SDK57 Release archive 已实际构建，匹配 ASan archive、
完整下游链和生产验收仍未通过。
Main typed-wire 源码现将 converter 的实际 Exchange pre-emit schema 与最终
PartitionedOutput schema 绑定到 query-owned factory；独立 options 只共享不可变
schema，压缩设置不共享。wire schema 复用唯一物理投影和 SDK Bridge，再递归
应用 canonical 字段名/NULL 约束；Map entry/key 始终 required。接收端在 import
及 result publication 前验证实际 wire schema 和每批 required 值；任务内部仅允许
顶层物理 alias，不能抹去嵌套字段名。Shuffle 构造准入也消费同一契约，空输入不能
绕过缺 schema/nullability 检查。已有 serde/Exchange/Flight fixtures 保留原断言并
显式携带契约，新增晚批次 NULL、wire nullability drift、missing contract、实际
pre-emit/result writer 与 converter-reuse owner 验证；已纳入下述原 386 项专项。
SDK57 Release setup 现 **PASS / exit 0**，实际 archive SHA256
`1dce67a70bbf0b7d06ea71fec75a03ce5a6c881279a824830a6e12e156a3e1b5`。
其 actual archive provenance 正负准入六项 **6/6 PASS、11.212s**，日志
`/tmp/pangrova-production-sdk57-release-actual-archive-provenance-nearest-r1.log`。
已观察到 pinned DuckDB 使用 LLVM 17 不认识的
`-Wno-missing-template-arg-list-after-template-kw` 警告，保留于
`/tmp/pangrova-production-sdk57-typed-serde-release-setup-r1.log`，不计零警告。

复核实际末端又发现 `ResultBatchStreamReader` 仍从 task-local RowType 通过 SDK
zero-row export 推导 wire schema，丢失 canonical required 标记和逻辑字段名；
新的合法 PartitionedOutput 数据因此会被旧读端拒绝。现由 `VeloxTaskRunner`
从同一 admitted canonical output 生成一次 Arrow wire schema，核对真实物理
嵌套类型后，连同 authority、allocator、ownership 原子注册；结果服务不另建
SQL 类型解释器，也不把 Arrow 转回 Velox 校验。Arrow-only required-value
校验位于 `columnar/`，遵循现有层界。
此外，原 `fetchPages` 每解析一页便推进 pending/sequence；后续页失败会留下
前面的待发布结果。现对整个 fetched group 暂存 batch、cursor 和 EOF，全组
校验通过才提交并 ACK，错误成为 reader 的终止状态。新增实际 Runner → SDK
PartitionedOutput → OutputBuffer → Result reader 验证，以及晚页 NULL/schema
drift 不发布、不 ACK、重复读取保持失败和单页晚 batch NULL 拒绝测试；这些是
实际组件集成，不能称独立进程 SQL E2E；六项均已通过下述真实专项。
实际读取及 fetched group 提交前均重验 live grant，不能因为取数/解码开始时
授权有效，就在过期或被撤销后继续提交；新增撤销注册不发布、不 ACK 验证。
最近 `.pangrova/setup.sh --check` 与 dead-stub 扫描 **PASS**，后者日志
`/tmp/pangrova-production-typed-result-dead-stubs-r1.log`；它们仅是静态门禁。

SDK57 Main nearest r1 实际 **386 项 / 385 PASS / 1 FAIL / 0 SKIPPED**。
唯一失败为 MAP entry bitmap 夹具：pinned Arrow 的零 NULL 数归一化丢弃 bitmap，
后改 ArrayData 未刷新 Array 缓存指针。现保持真实 child identity，在合法数据上
重 box，再实际置 NULL；原 bitmap identity 和 required-entry 拒绝断言保留。
原 386 项完整复跑 **386 PASS / 0 FAIL / 0 SKIPPED、1.503s**，日志
`/tmp/pangrova-production-sdk57-main-original-386-nearest-r2.log`。
Main build r1 的 deprecated Values 编码警告亦由真实调用链确认：生产转换器禁止
该编码。夹具迁至 expressions 字段，未抑制警告；r2/r3 编译 **PASS / exit 0**，
日志 `/tmp/pangrova-production-sdk57-main-typed-result-build-r2.log`、
`/tmp/pangrova-production-sdk57-main-typed-result-build-r3.log`；尚非全 Native 构建。

新增两项实际 MAP wire 探针均 **FAIL / process abort / exit 134**：未引用的
NULL key、NULL MAP 父行下的存储 NULL key，被 SDK Bridge compact-offset
路径导出为全体 child，Arrow MapArray 导入因 keys 含 NULL 直接终止。日志
`/tmp/pangrova-production-sdk57-map-hidden-key-wire-nearest-r1.log`、
`/tmp/pangrova-production-sdk57-map-null-parent-wire-nearest-r1.log`。
386 PASS 不包含这两项独立失败探针，不能据此宣称 typed-wire 完成或全回归通过；
SDK 的可见 child 选择及 NULL 父传播根因修复仍在实施。

当前实际 SDK57 exporter 输出 **781837 bytes**，payload SHA256
`145a00aeec0da64fb688a4ff51651f83963c8ac2bbcd7da9a2261b2f6b79fa00`，
与 sole protocol test resource 字节完全一致；这是目录 payload 哈希，
不是 exporter 可执行文件或 packaged JNI 的二进制身份认证。
新增不可变 `FunctionCatalogView` 只消费 sole actual inventory，并复用真实 scalar
选择顺序及 aggregate/window 签名；没有 factory I/O、按期望输出重选、FINAL
state 来源推断或 Planner defaults 回退。原 65 项加 View/Binder/Selector 共
**95 PASS / 0 FAIL / 0 SKIPPED、14.143s**，日志
`/tmp/pangrova-production-sdk57-function-view-protocol-nearest-r2.log`。
r1 唯一旧 ROW fixture 缺显式 NULL 约束的失败保留于同名 r1 日志；修为显式
required 并增加 UNSPEC 拒绝断言，未放宽生产契约。正式 Planner/cohort 接线、
字段 18 collector/Native 比对、实际 binary attestation 和全量回归仍未闭环。

协议模块未筛选 `-Pfull-regression -pl pangrova-protocol -am test` r1 **FAIL**：
Protocol **369 项 / 352 PASS / 1 assertion FAIL / 16 ERROR / 0 SKIPPED**，
日志 `/tmp/pangrova-production-sdk57-function-view-protocol-unfiltered-r1.log`。
完整调用栈定位到五个旧 binder/field-reference fixtures 的真实 value 类型省略
NULL 约束，不是 NamedStruct 容器；一个 BM25 负例因此未到其原定拒绝点。
仅补真实字段、嵌套 LIST/ROW 和函数输出的显式声明，原 schema/emit/完整覆盖/
稳定顺序/错误原因断言保留，生产 strict 准入不变。原五类连同 nullability
admission 专项 **30 PASS / 0 FAIL / 0 SKIPPED、11.611s**，日志
`/tmp/pangrova-production-sdk57-protocol-nullability-callers-nearest-r1.log`；
原未筛选范围 r2 **PASS / exit 0、35.116s**：Protocol **369 PASS / 0 FAIL /
0 ERROR / 0 SKIPPED**，Common 和 SPI 的未筛选上游测试也实际运行，日志
`/tmp/pangrova-production-sdk57-function-view-protocol-unfiltered-r2.log`。
这不是 clean 全仓、最终 SDK cohort 或匹配 JNI 回归。

独立全 Java `clean -DskipTests install` r1 **FAIL / exit 1**，39-module reactor
在 E2E test-compile 暴露 10 个实际错误，日志
`/tmp/pangrova-production-sdk57-independent-java-clean-install-r1.log`。
九处仍调用已不存在的 `TaskAttemptId.getQueryId()`；sole v2 field 1 是
`query_or_job_id`，已迁为 generated `getQueryOrJobId()`，不增加旧协议 shim。
另一处捕获 `JdbcCatalog.close()` 不声明的 `IOException`；实际 1.11.0 SDK
签名由 `javap` 复证，仅该 cleanup 捕获改为实际 unchecked failure 并仍挂
suppressed，源文件写入的 checked failure 处理保留。原 E2E 模块 349 个测试
源码 `compile test-compile` r2 **PASS / exit 0、8.081s**，日志
`/tmp/pangrova-production-sdk57-speculative-process-test-compile-r2.log`。
编译未运行测试，复制的开发 JNI 仍不是本次最终匹配 package，不能折算 E2E。

Native 扩大诊断实际执行 **3988 项 / 3966 PASS / 22 FAIL / 0 SKIPPED、8.293s**，
日志 `/tmp/pangrova-production-sdk57-main-diagnostic-known-map-aborts-excluded-r1.log`。
此诊断明确仅排除两个已独立复现 process abort 的新增 MAP 探针；不是未筛选
CTest 门禁，不能隐藏或折算两个 FAIL。失败分别为 ANN/Lance 旧声明省略 NULL
约束、TopN 正例在已限定 URN 的 plan 上新增 add 却未给 scalar URN、SUM
companion 夹具假设无后缀 extract 名称、executor 旧 value schema 省略 NULL
约束，以及五项 creation 夹具跳过真实启动 finalize 后直接 GET 目录。
原值、稳定位置、索引 exact-rescore、取消/EOF/所有权断言保留；各有限修复
正在实施和验证，不改生产准入为 implicit-nullable，也不伪造 actual inventory。

SDK MAP 可见性根因修复现为 sole patch 的 59 个精确源码 blobs，SHA256
`3ae7864a869f5be6f6e160bb5ecd2ff66059d82212704f91b554affb25262abb`。
实际安装源码 SDK57→59 apply/verify **PASS / exit 0**，日志
`/tmp/pangrova-production-sdk59-map-visibility-apply-r1.log`。补丁按实际被引用
的 child 选择导出，传播 NULL STRUCT 父行的不可见性，保留字典/常量的有效
编码及行数；可见 NULL MAP key 在 Arrow import 前以 USER/INVALID_ARGUMENT
拒绝。新增 SDK C Data Interface 验证与空输入、全 NULL 字典、NULL 常量边界，
仍须用重建 archive 实测。DuckDB 的诊断选项改为实际编译器能力探测，不按
版本号猜支持性。SDK59 Release setup 已 **PASS / exit 0**，实际 archive
SHA256 `943f5a642ac6b22a6a4855a3787724286d96d007c036450accc1f5a7e6fa5002`；
actual archive 六项准入 **6/6 PASS、11.809s**，日志
`/tmp/pangrova-production-sdk59-map-visibility-release-setup-r1.log`、
`/tmp/pangrova-production-sdk59-release-actual-archive-provenance-nearest-r1.log`。
Root 独立复核 clean-pin 与 exact57 升级两个 source tree 均为 **59/59 exact
blobs**，日志 `/tmp/pangrova-production-sdk59-independent-blob-proof-r1.log`；
这两个 archive 解压树没有 `.git`，直接使用 production apply verifier 会以
“Pinned source or patch is missing”拒绝，不能误记该命令通过。source blob
核查与 installed SDK/archive 准入是不同证据。
实际 setup 另经 `scripts/setup-macos.sh::install_duckdb_clang` →
`install_duckdb` → `cmake_install` 注入同一未知选项，共 **150 个 compiler
warnings**；CMake 模块探测没有覆盖这条 installer 路径。下一完整修复须
覆盖实际 CXX 的 installer probe，当前不计零警告。Main configure **PASS**，
full build **PASS / exit 0、0 compiler warnings**，日志
`/tmp/pangrova-production-sdk59-main-map-visibility-full-build-r1.log`。
真实 MAP/原 wire 探针 **15 项 / 10 PASS / 5 FAIL / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk59-map-visibility-real-wire-nearest-r1.log`；两个原
process-abort wire 用例已通过，但五个 flatten 模式仍失败。根因是 public SDK
schema 导出将复杂 dictionary 声明为零 children，且拒绝复杂 constant flatten，
与实际 array 导出不一致。不能以编码模式通过替代 flatten 模式。
同一冻结二进制的未筛选 CTest **4009 项 / 4003 PASS / 6 FAIL、276.75s**，日志
`/tmp/pangrova-production-sdk59-native-unfiltered-ctest-r1.log`。另一个失败来自
SUM companion 测试同时存活的两个 plan 重复创建同名 pool；正在修复为共享同一
输入源。此前 signature binder 的临时 actualTypes 引用已修正为有明确生命周期
的 local vector，不能忽略后续真实异常。完整 Native gate 尚未通过。

客户端原 162 项加取消协议 32 项最近 r3 **194 PASS / 0 FAIL / 0 ERROR /
0 SKIPPED**，实际 Transport 62、REST 51、JDBC 43、CLI 38，日志
`/tmp/pangrova-production-sdk59-client-cancel-protocol-nearest-r3.log`。
r1 错选父模块，**0 tests，不计通过**；r2 缺 Python 3.12 PATH，生成器前置
**FAIL / 0 tests**，分别保留于同名 r1/r2 日志。新增验证通过真实本地 HTTP
边界确认权限错误、身份不符、损坏响应及代理伪 404 不会成为逻辑成功；它们
不是实际查询取消、Native 物理退出或独立进程 SQL E2E 的证明。
JDBC 取消 RPC 失败后丢失查询身份已改为：保留 active identity，仅精确匹配的
有效 CANCEL_ACCEPTED/ALREADY_TERMINAL 才能 compare-and-set 清理；404/权限/
损坏响应均保留身份，旧响应不得清理新查询。真实 HTTP、SDK result endpoint
和物理消费者 drain 回归最近 **204 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、47.417s**，
Transport62、REST51、JDBC53、CLI38，日志
`/tmp/pangrova-production-sdk59-jdbc-cancel-identity-client-204-nearest-r2.log`。
同名 r1 在 test-compile 暴露四处 SQLException 同时实现 Iterable 导致的 AssertJ
重载歧义，未计通过；显式 Throwable 选择保留原错误类型和 message 断言。
完整客户端 owning suite 正在执行；最终匹配 Native 的全仓回归、ASan、coverage、
E2E、Smoke 仍未通过。

独立 Planner 未筛选 regression（含上游）进一步暴露 **1302 项 / 1256 PASS /
28 FAIL / 18 ERROR / 0 SKIPPED**，日志
`/tmp/pangrova-production-jackson-bom-planner-unfiltered-r1.log`。27 个计划
golden 差异、一个 AI_EMBED 旧 arity 断言，以及 VALUES 解析、ROUND/嵌套
LIST 静态准入、四项正式 artifact build plan 缺显式 NULL 约束，均保留原失败。
这里 artifact builder 是生产 caller 缺陷，不是仅旧测试 fixture；已按实际
physical locator required、源列 nullable、vector 元素 required 补声明和精准
schema 断言，最近八项回归 **8 PASS / 0 SKIPPED、15.993s**，日志
`/tmp/pangrova-production-artifact-build-nullability-nearest-r1.log`，不弱化
canonical admission。Native 索引
算子现仍拒绝实际 NULL text/vector，不将声明 nullable 解读为已支持跳过 NULL。

实际依赖树与旧全 Java shade 日志显示 Jackson core 2.18.6、jdk8/yaml 2.22.0、
TOML 2.15.2 混用，且不同模块的 YAML 版本由 Maven 路径深度决定。父 POM
现保留原版本选择 2.18.6，以单个 `jackson-bom` import 替代四个零散版本。
三模块实际 dependency-tree r2 **PASS**，jdk8/YAML/TOML 均由同一 BOM
管理为 2.18.6，日志
`/tmp/pangrova-production-jackson-cohort-dependency-tree-r1.log`、同名 r2。
这是对实际 classpath 漂移和仓库 BOM 规则的修复，不声称已复现
NoSuchMethodError；上述 Planner FAIL 也不能写为依赖整改后全回归通过。
原理依据为 [FasterXML 官方 BOM 说明](https://github.com/FasterXML/jackson-bom)，
最终仍须匹配整仓编译、序列化回归与实际 E2E。

本轮持续复证：客户端 owning 未筛选 regression r1 **FAIL**（JDBC115 项中
114 PASS/1 FAIL，CLI 因上游失败未执行），日志
`/tmp/pangrova-production-sdk59-client-owning-unfiltered-r1.log`。BI metadata
fixture 只提供 SQL submit/result，却没有真实 metadata REST 边界；生产 metadata
路径同时存在异常静默 empty、过滤/ordinal/type 问题，正在按正式 caller 根因修复。
这与 204 项专项 PASS 是不同范围，不能互相覆盖。
AI_EMBED 旧 arity 测试已对齐正式可选 purpose 的 2..3 参数，增加明确拒绝 1/4 参数
的边界，最近完整 operator table **23 PASS / 0 SKIPPED、15.913s**，日志
`/tmp/pangrova-production-ai-embed-arity-nearest-r1.log`。合法 VALUES/parser
故障仍然存在，未通过改变原 SQL、添加第二 parser 或扩大静态函数名单隐藏。

Native snapshot/TopN 扩大 r3 **270 项 / 269 PASS / 1 FAIL、249ms**，日志
`/tmp/pangrova-production-sdk59-native-companion-and-topn-nearest-r3.log`。
252 个参数化 TopN 和三项 negative 均实际运行；SUM companion 同名 pool 修复后，
retirement 后执行进一步暴露 SDK `AggregateInfo::toAggregateInfo` 仍从 live
registry 解析 intermediate type 和 metadata。`Aggregate::create` 已冻结 factory
并不能补足这一接缝。`ColumnStatsCollector` 的正式 TableWriter/TableWriteMerge
调用也有同一 intermediate-type 漏点。修复必须用同 query-owned snapshot 的
实际 binder/entry，同时保留旧任务运行值、distinct 元数据和写入统计的强 oracle。
下一 SDK 候选尚未发布或构建；候选 script 的 heredoc 因生成器 replacement-string
解释 `$1/$2` 损坏，及 CMake SOURCE 属性未作用于实际 mono target 两项根因已被
审查指出，必须以实际 script 两路升级和 linked identity 证明消除，不能只计
独立 `git apply` 或 source hash 通过。

上述聚合根因已用实际 SDK 执行单独复现：四项 oracle **0 PASS / 4 FAIL、10ms**，
日志 `/tmp/pangrova-production-sdk59-owned-aggregate-metadata-stats-real-failure-r1.log`。
其中 Hash 与 Streaming 的旧 admitted `SUM(DISTINCT y)` 均返回 **30 而非 10**，
当前替换后 registry 的 30 则符合其 metadata；另两项 retirement 后报 live registry
not registered。该证据确认错误结果影响，不仅是源码接缝或抽象风险。
修复现已审阅并发布为 sole SDK patch 的 **64 个精确 blobs**，SHA256
`69ae7ed30e6ee6ce9af52b51ae3a8b3b9053b229b0eab0fdaeef133ef942d3bd`；
实际 installed59→64 apply/verify **PASS / exit 0**，日志
`/tmp/pangrova-production-sdk64-installed-apply-r1.log`。
完整 actual Release setup 现已 **PASS / exit 0**，日志
`/tmp/pangrova-production-sdk64-owned-aggregate-map-release-setup-r1.log`。
actual archive SHA256 为
`2326323b8747ecbbe6fcdc4305d0d524892c564d3a1331808afbbfff794876c1`；
linked inspector 的 patch SHA 与上述 sole64 patch 精确相同，actual archive
provenance 最近 **7 PASS、34.337s**，日志
`/tmp/pangrova-production-sdk64-release-actual-archive-provenance-nearest-r1.log`。
Main 重建、MAP15、聚合4、未筛选 CTest、ASan 尚未通过，不能按 SDK 构建关闭。
独立真实 `.git` 的 clean pin→64、59→64、幂等 apply/verify 及两路64/64 blobs
证明见 `/tmp/pangrova-sdk64-candidate-script-real-git-proof-r1.log`；Root 审阅时发现
的两个候选生成缺陷已修正后才发布。编译 patch 身份来自实际 Identity TU/mono
target，record 比较 linked inspector 的 inputs/patch/archive，仍保留五行 v2 与
unchanged archive barrier，不删除旧 sidecar 伪造新构建。新增 patch drift 准入
oracle 在旧实际 SDK59 archive 的最近验证 **7 PASS、14.169s**，日志
`/tmp/pangrova-production-sdk59-release-actual-archive-provenance-nearest-r2.log`；
它证明准入边界，不是 SDK64 archive 或性能验证。

2026-10-04 本次继续复证：唯一函数声明/签名/生成器未筛选工具测试 **90 PASS /
0 SKIPPED、0.229s**，日志
`/tmp/pangrova-production-sdk64-function-generator-unfiltered-r1.log`；仓库
adapter `setup.sh --check` 与 E2E truthfulness 静态门禁均 PASS，后者日志为
`/tmp/pangrova-production-sdk64-e2e-truthfulness-r1.log`。这些工具检查不构成
运行时 SQL、JNI 或独立进程 E2E 通过。

Planner 27 个原失败 golden 已逐项核对 actual/expected 与实际依赖源码：23 项
涉及 SUM 及其后续 DECIMAL precision19→38，遵循 Calcite1.42 实际
`deriveSumType` 与 pinned Native SUM 的 DECIMAL(38,s) 返回签名；TPC-H08/14
两处 CASE 的 precision19→31 来自实际乘减公式；五项 SUBSTRING 位置 literal
显式 BIGINT 来自现有 `SubstringSqlFunction`。其 BIGINT normalization **尚未**
写入 sole binding 声明，不能声称已生成统一政策。27 个完整计划树逐字比较除
上述 token 外均相等，本轮只更新这27份期望的67行，不改 SQL、算子树、字段顺序
或等值断言；另移除缺失 golden 时自动生成并成功返回的假通过路径，改为明确
检查 reviewed 文件存在。最近原 TPC-H21＋TPC-DS31＋TypeSystem3 **55 PASS /
0 FAIL / 0 ERROR / 0 SKIPPED、24.959s**，日志
`/tmp/pangrova-production-sdk64-reviewed-tpc-golden-nearest-r1.log`。命令另含一个
不存在的 `DecimalTypeValidationTest` 名称，未发现测试，**不计**其验证；扩大范围
使用实际存在的 `PlannerDecimalArithmeticTest` 等类继续验证。此处快照更新须在
未来合并时按既有 plan-change 审批门禁审阅，不伪造 PR label/审批结果。
这些 planner 检查不是 Native 最终值或完整 owning suite；新增真实 DECIMAL
SUM/CASE/SUBSTRING Task 值 oracle 正在实施，既有 VALUES/ROUND/ARRAY 断点仍未关闭。

SDK64 的完整冷依赖 setup 已 exit0。实际日志暴露第三方编译 **13 条
deprecated-declarations warnings**：Protobuf25.3 `MessageLite::GetCachedSize`
内部调用其 deprecated `ByteSize` 两次编译，以及 gRPC1.65/OpenSSL3 ENGINE API
十一处。未压制警告、升级未核验依赖或在运行中改构建输入；不能把 command exit0
或 Main 零警告换写为整条冷依赖链零警告。此前 installer 的未知 warning option
在本次完整 setup 实际编译输出中为零；十三条弃用警告仍须独立修复。

客户端 metadata 失败已进一步追到正式服务端：
`CatalogRestOperations.listTablesInSchema` 把 `%` 当单表名 resolve，
`loadTable` 在真实 resolve 后人为返回空 columns，并把所有 RuntimeException
当 NOT_FOUND；其固定 `rest-catalog-service` 主体也没有请求 tenant authority。
`ConnectorTableMetadata.Column` 当前没有 nullable 字段，复杂 Type 也未保留完整
NULL 声明，不能默认 nullable 或只加 REST boolean 宣称保真。正在沿正式
Connector schema、认证 session 和 namespace 双端修复；客户端 HTTP fixture
通过不得替代该服务端闭环。多级 namespace 的唯一编码按
[Apache 官方 REST Catalog 规范](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
使用 U+001F / URL `%1F`，不以点号拼接或保留第二身份解析路径。

扩大后的 Planner 类型/协议最近 **155 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、
23.777s**，实际七类为 TPC-H21、TPC-DS31、PlannerTypeSystem3、
PlannerDecimalArithmetic16、DecimalSqlProtocol26、TypeMapper25、
TypeSystemConsistency33；日志
`/tmp/pangrova-production-sdk64-reviewed-tpc-type-protocol-nearest-r1.log`。
同 owning 未筛选 r2 仍为 **FAIL**：Planner **1302 项 / 1288 PASS / 0 assertion
FAIL / 14 ERROR / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-reviewed-golden-planner-unfiltered-r2.log`。
其中十项是原合法 VALUES SQL 仍走 ANTLR 入口而拒绝，三项是 ROUND 被静态
defaults 拒绝，一项是 nested LIST 同类缺口；不改 SQL、不扩大硬编码能力集隐藏。
Native 新增九项真实 published-registry Task 值/类型 oracle 已 source-ready，
仍待统一 Main 构建和实际运行，不能折算上述155项为 Native 最终值通过。

客户端 metadata + 原204项最近实际 **229 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、
1m09s**，Transport62、REST51、JDBC78、CLI38，日志
`/tmp/pangrova-production-sdk64-client-metadata-and-original-204-nearest-r1.log`。
BI 原五项保留，并修复原 extractor 搜索不存在的 `sql` 字段后返回整个 JSON 的
假 oracle：改为正式 `SubmitQueryRequest.query` 解码、完整 SQL 相等、Bob/175/EOF
实际结果断言。metadata 新20项为实际 HTTP 客户端协议回归，非正式 catalog server。
健康检查另实际复现旧 `ping` 仅拒绝>=500：REST17项14 FAIL、JDBC15项12 FAIL，
分别见 `/tmp/pangrova-production-sdk64-health-protocol-root-cause-red-r1.log`、
`/tmp/pangrova-production-sdk64-jdbc-health-root-cause-red-r1.log`。
最小修复只接受正式 readiness 的 HTTP200、复用既有 credential header；保留
原单次执行/timeout/error 类别。最近 **32 PASS / 0 SKIPPED、19.736s**，日志
`/tmp/pangrova-production-sdk64-health-protocol-nearest-green-r1.log`。正式 Coordinator
明确豁免 health 认证，header 测试仅证明客户端/代理凭据一致性，非 backend 登录。
同 owning 未筛选 r2 已 **PASS / exit0、2m08s**，三个客户端叶模块 **286 PASS**
（REST98、JDBC150、CLI38），连同实际11个上游/叶测试模块共 **2322 PASS /
0 FAIL / 0 ERROR / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-client-owning-unfiltered-r2.log`。此前 r1 失败保留；
这不是39-module最终匹配JNI全仓 regression，也不关闭 Driver deregistration lease
或 JDK17 socket physical-exit 缺口。

同 cohort Step0 Rust Lance FFI Release **PASS / 0 errors / 0 warnings、4m49s**，日志
`/tmp/pangrova-production-sdk64-lance-rust-build-r1.log`；Main Release configure
已 PASS、full targets build RUNNING，日志
`/tmp/pangrova-production-sdk64-main-full-configure-r1.log`、
`/tmp/pangrova-production-sdk64-main-full-build-r1.log`。
Main build r1 随后 **FAIL / exit2、七个 compile diagnostics、零 warnings**：新
published Task 测试 helper 直接使用 runtime 消息却缺少其 generated header。
已按 canonical `runtime.proto`/正式 dispatcher caller 补唯一 `runtime.pb.h` include，
不复制消息定义、不改 generated 源码或弱化断言。全目标 r2 重建实际
**PASS / exit0 / 0 compiler warnings**，日志
`/tmp/pangrova-production-sdk64-main-full-build-r2.log`。

同 cohort 最近 MAP/owned aggregate/DECIMAL/TPC 专项 r1 **FAIL：31 项中30 PASS、
1 FAIL、0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-map-aggregate-tpc-value-nearest-r1.log`。
未筛选完整 CTest r1 **FAIL / exit8：4021 项中4020 PASS、1 FAIL、0 SKIPPED、
575.51s**，日志 `/tmp/pangrova-production-sdk64-native-unfiltered-ctest-r1.log`。
唯一失败是 ColumnStats fixture 使用 sysRoot 下不跟踪内存的 leaf pool；SDK
GroupingSet 实际要求 tracked pool。修复使用真实 QueryCtx root 的 leaf child，
保留实际输出6、EOF/finished、missing owned snapshot 错误原因断言。初次重建 r3
因传入 const/lvalue QueryConfig 而 **FAIL / exit2 / 1 compile error**；按 SDK
`QueryCtx::create(QueryConfig&&)` 改为显式 move 后，完整 r4 **PASS / exit0 /
0 compiler warnings**，日志 `/tmp/pangrova-production-sdk64-main-full-build-r4.log`。
同31项专项 r2 已 **31 PASS / 0 SKIPPED、1.145s**，日志
`/tmp/pangrova-production-sdk64-map-aggregate-tpc-value-nearest-r2.log`。
完整 CTest r2 实际 **PASS / exit0：4021/4021 PASS / 0 SKIPPED、563.94s**，日志
`/tmp/pangrova-production-sdk64-native-unfiltered-ctest-r2.log`。这关闭同 cohort
Release CTest 失败，不证明 public SQL、JNI、network FINAL aggregate 或分布式 E2E；
匹配 ASan、重新包装、最终干净 Java/全仓回归和部署验收尚未完成。

Paimon 的两个正式 Column 构造入口已补接现有参数化 mapper，而非新建类型表。
14 个新增实际 SDK 类型案例覆盖 DECIMAL、ordered nested types、floating VECTOR、
属性维度拒绝与 property VARIANT 的既有 JSON storage 类型；原断言保留。
正式 `PaimonMetadata` 和 `PaimonReadBindings` 共用此入口。最近实际五类 **81 PASS /
0 FAIL / 0 ERROR / 0 SKIPPED、19.096s**，日志
`/tmp/pangrova-production-sdk64-paimon-column-parameters-nearest-r1.log`。
同 owning 未筛选 **PASS / exit0、54.004s**：Paimon **347 PASS**，连同 Common246、
SPI706、Protocol369 共 **1668 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-paimon-owning-unfiltered-r1.log`。
该修复不声称完整 NULL 声明保真：SPI 缺递归 nullability，现有 temporal precision
与 Native schema compiler 默认值仍未关闭。VARIANT 参数化表示复用现有 SPI 的
UNBOUNDED VARCHAR 存储投影，不声称已增加完整逻辑 VARIANT 执行能力。

客户端与 Paimon combined owning `full-regression,coverage verify` 实际
**PASS / exit0、2669 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、3m15s**，日志
`/tmp/pangrova-production-sdk64-client-paimon-owning-coverage-verify-r1.log`。
随后针对真实 review base `e92532a`（含 staged/unstaged/untracked）执行全 diff
coverage gate **FAIL / exit1**，日志
`/tmp/pangrova-production-sdk64-actual-whole-diff-coverage-r1.log`：312 个
missing/stale/source-report 错误，涉及尚未执行的模块；报告不完整不能计算或宣称
全 diff ≥60%，不得以 owning 通过替代全仓 coverage。

后续静态追踪确认属性型 VECTOR 还有两个独立写入断点。持久化 SDK
ARRAY(FLOAT/DOUBLE) 加维度 property 会经 `beginInsert` → write lifecycle →
physical allocation → `PaimonDeclaredMetadata`，按物理 ArrayType 重建参数而
丢失 mapper 的 VECTOR dimension，正式 codec/template 因此拒绝。维度修复应按
逻辑 signature ARRAY/MAP 重建，不能覆盖 VECTOR(dim,element)。即使参数保真，
该 SDK ARRAY 的真实 Arrow schema 仍是 LIST，Native storage VECTOR 校验要求
FixedSizeList；现有正例使用 SDK 原生 VectorType，不能覆盖此缺口。不得伪造 SDK
schema 或宽松 Native validator 将它记为写入贯通；需独立证明真实布局、逐值
维度、NULL、所有权与持久化回读。

属性向量的 Java 声明修复现已实施：集合参数重建以 mapper 的实际逻辑
signature ARRAY/MAP 为条件，保留 VECTOR(dim,element)，生产只改该条件。
两个真实持久化 SDK/catalog 测试新增10项，FLOAT/DOUBLE 与父/元素 nullable
组合保留实际 field ID、LIST schema、普通嵌套 DECIMAL 参数和 handle/template
roundtrip。冻结红测实际 **FAIL / exit1：14项中4 PASS、10 FAIL、0 ERROR /
0 SKIPPED、17.198s**，日志
`/tmp/pangrova-production-sdk64-paimon-property-vector-red-r1.log`。
原样绿测加相邻 schema builder/SDK vector 两类实际 **22 PASS / 0 SKIPPED、
17.095s**，日志 `/tmp/pangrova-production-sdk64-paimon-property-vector-green-r1.log`。
未修改 Native validator，仍不计为 LIST→Native VECTOR 物理写贯通。
该修复后 owning 未筛选 `full-regression,coverage verify` 实际 **PASS / exit0、
2m03s**：Paimon **357 PASS**，连同 Common246、SPI706、Protocol369 共
**1678 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-paimon-property-vector-owning-coverage-r1.log`。

同 source/current ABI Rust FFI 未筛选 release lib tests 实际 **38 PASS /
0 FAIL / 0 ignored / 0 filtered、4.20s**，日志
`/tmp/pangrova-production-sdk64-lance-rust-unfiltered-tests-r1.log`；测试可执行文件
编译亦无 compiler warning。匹配 SDK64 的独立 ASan dependency setup
实际 **PASS / exit0**，日志 `/tmp/pangrova-production-sdk64-asan-owned-dependencies-setup-r1.log`。
其实际 archive SHA256 为
`a4493760adfccc5f69f2c34df1dbada5732d9abc2a25bc0630708a487b927919`，
source pin、patch64 和 generation-input identity 与本轮 Release 对应。
Main ASan configure 实际 **PASS / exit0**，完整构建 **RUNNING**，日志
`/tmp/pangrova-production-sdk64-main-asan-configure-r1.log`、
`/tmp/pangrova-production-sdk64-main-asan-full-build-r1.log`。
这些 prerequisite/configure 不构成 Main ASan 测试或零 sanitizer findings。

独立全 Java reactor 编译诊断 `mvn -B -DskipTests install` 实际
**39/39 projects BUILD SUCCESS / exit0、59.070s**，日志
`/tmp/pangrova-production-sdk64-java-full-reactor-compile-diagnostic-r1.log`。
该命令跳过测试、没有 clean，且在匹配 JNI 重新包装之前执行；不能充当最终
Step5/6、零 warning 或端到端验收。空 native bundle/依赖 shade 的 warning 仍需
与正式 classifier/image packaging 区分和复证。

本轮 CI gate policy 工具未筛选 **34 PASS / 0 SKIPPED、5.717s**，日志
`/tmp/pangrova-production-sdk64-ci-gate-policy-unfiltered-r1.log`；architecture
文档 class reference 门禁 **174/174 PASS**，日志
`/tmp/pangrova-production-sdk64-doc-class-refs-r1.log`。后者仅证明类型名存在，
不证明其生产 wiring、能力实现或分布式正确性。

扩大后的 control owning `full-regression,coverage verify` 实际 **PASS / exit0、
3659 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、10m06s**，日志
`/tmp/pangrova-production-sdk64-kernel-control-owning-unfiltered-coverage-r1.log`。
17 个 reactor project 中14个实际测试模块为 Common246、Config164、SPI706、
Resilience121、Protocol369、Observability280、TestCommon79、Security389、
Operations132、Resource176、Governance115、Metadata524、Indexing135、Streaming223。
包含实际 PostgreSQL/container 测试；不替代尚未完成的匹配 JNI 全仓回归或
独立 Worker 分布式 E2E。各轮重叠模块的计数不能相加为唯一测试总数。

canonical descriptor gate 本轮首先实际 **FAIL / exit1**，日志
`/tmp/pangrova-production-sdk64-canonical-descriptor-gate-r1.log`。根因是 checked-in
golden 未反映当前13个 owned v2 IDL，而非 protoc 噪声。使用 Maven pinned protoc35.1
和同版本 WKT 独立生成，完整审阅930行 decoded diff：函数契约/物理投影/manifest、
Task admission seal、execution drain、catalog finalize 与 executor recovery 是
实际协议变化；删除字段均保留 reserved number/name，未重新利用字段编号。
复证现有 Java/C++ consumer 后，以唯一 `proto-descriptor-diff.sh --update` 生成
golden，再运行只读 gate 并与独立生成文件逐字节比较，实际 **PASS / exit0**，日志
`/tmp/pangrova-production-sdk64-canonical-descriptor-gate-green-r1.log`；golden SHA256
为 `6819c1f913c1f3aa65a4038fba438dc91fd5b4f2d44eb08927246866d974146f`。
原 golden 和完整差异分别保留于本机调查目录及
`/tmp/pangrova-production-sdk64-descriptor-exact-root-cause-r1.diff`。未手改生成源，
不保留旧协议双路径；该 gate 只证明 descriptor 一致，**不关闭** field18 无正式
producer/consumer、query cohort freeze 和分布式故障恢复的独立缺口。

真实 Checkstyle 边界工具实际 **PASS / exit0**：6个允许 fixture 通过，8个超限/
嵌套违规 fixture 被真实 Checkstyle 拒绝，日志
`/tmp/pangrova-production-sdk64-checkstyle-real-boundaries-r1.log`。这不是生产源码
静态分析通过。生产客户端/Paimon owning 静态分析 r1 实际 **FAIL / exit1**：
`ExecutorDrain` 三处 whitespace/brace 违规；保留逻辑和断言，仅修正源码格式，
r2 继续暴露 Config 的超长行，r3/r4 暴露 SPI 的数值边界命名/超长行；保留所有
原阈值和行为，修正后 Common/Config/SPI 未筛选 `full-regression,coverage verify`
实际 **1116 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、42.235s**，日志
`/tmp/pangrova-production-sdk64-static-root-format-owning-unfiltered-r1.log`。
静态分析 r5 这三模块的 Checkstyle/SpotBugs 实际通过，但 Protocol 随后实际
**FAIL / exit1：152项 Checkstyle 违规、38.611s**，日志
`/tmp/pangrova-production-sdk64-client-paimon-real-static-analysis-r5.log`。
三线按物理文件写入、Substrait/task/retrieval、function 独占源码修正，冻结后
Root 最近30类实际 **168 PASS / 0 SKIPPED、16.303s**。完整 owning r1 的
Common246、SPI706、Protocol369 共 **1321 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED**，
三模块 Checkstyle 违规全部归零，但整体 **FAIL / exit1**：Protocol 剩2项 SpotBugs。
日志 `/tmp/pangrova-production-sdk64-protocol-quality-nearest-r1.log`、
`/tmp/pangrova-production-sdk64-protocol-quality-owning-full-gates-r1.log`。

数值告警的实际 FindSecBugs1.13.0 analyzer 仅按量词分组深度判定，不分析
数字与小数点/指数标记的互斥关系；不能据该告警宣称已复现指数级 ReDoS。
唯一 governed decimal grammar 已改为线性、常量栈扫描，保留严格 ASCII、无前导
零、负零、指数、Java double 精确舍入及 nonfinite 拒绝语义。新增48项 lexical/
边界测试，r2 完整 owning **1369 PASS / 0 SKIPPED**，Checkstyle 通过，整体仍
**FAIL / exit1**，剩 `FunctionBindingDeclarations.declarationSha256` 的
`MS_EXPOSE_REP`。根因是 SpotBugs4.8.3 `MutableClasses` 将 immutable
`ByteString.writeTo(...):void` 的 `write` 前缀误判为 setter；官方4.10.4亦未修正，
不可凭升级、复制 immutable 值或换 getter 名字冒充根因修复。

已补真实 canonical SHA256、`toByteArray` 修改隔离与只读 buffer 拒绝修改的
不可变性测试，保留生产 `ByteString.copyFrom` 的所有权和共享语义。唯一误报
分类精确限定类、零参数方法及返回 ByteString、同名字段及 ByteString 类型、
`MS_EXPOSE_REP`，不添加包级排除。真实 pinned Filter API 证明该新增 Match 的
1正例/8类改变签名、字段、类型或规则的负例，实际 **PASS / exit0**，日志
`/tmp/pangrova-production-sdk64-immutable-spotbugs-exact-boundaries-r4.log`。
它只证明新增分类的边界；既有 protocol 包级 EI 排除仍是独立质量门禁局限，
不能把旧排除后的零告警说成完全不可变性证明。
最近三类 **61 PASS / 0 SKIPPED、16.566s**，完整 owning r3 的 Common246、
SPI706、Protocol418 共 **1370 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、1m39s**，
Checkstyle 与 SpotBugs 实际通过，整体 **PASS / exit0**，日志
`/tmp/pangrova-production-sdk64-protocol-decimal-immutable-contract-nearest-r1.log`、
`/tmp/pangrova-production-sdk64-protocol-quality-owning-full-gates-r3.log`。
未删除断言或放宽数值/结构阈值；误报分类不计为修复了一个实际可变性漏洞。

Parser、Iceberg/Lance、Shuffle 的 owning 未筛选 `full-regression,coverage verify`
实际 **PASS / exit0：2961 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、4m12s**，日志
`/tmp/pangrova-production-sdk64-parser-formats-shuffle-owning-unfiltered-r1.log`。
11个实际测试模块为 Common246、Config164、SPI706、ShuffleCore121、Protocol369、
Observability280、TestCommon79、ShuffleService44、Parser528、Lance18、Iceberg406。
本轮 Protocol 编译发生在质量修正之前，不充当该后续修正的验收；Parser 528项
通过也不关闭公开入口合法 VALUES 的14项 Planner错误或唯一 Calcite 前端迁移。

当前 Release artifact 的只读 JNI symbol gate 实际 **PASS / exit0：22方法严格
双向对称、仅 JNI exports**，日志
`/tmp/pangrova-production-sdk64-release-jni-symbol-symmetry-r1.log`；不证明 JNI 执行
或最终重新打包。同 source SPI annotation gate **PASS：139 annotated public types、
0 baseline**，stability gate **PASS：22 checked Stable types**，日志
`/tmp/pangrova-production-sdk64-spi-annotation-gate-r1.log`、
`/tmp/pangrova-production-sdk64-spi-stability-gate-r1.log`。文本实现存在性不是生产
wiring 或真实执行的证明。

temporal 根因已按实际 Paimon1.4.1 SDK 拆分：TIMESTAMP precision0 对应 Arrow
SECOND，1–3/MILLI、4–6/MICRO、7–9/NANO；Native storage validator 错把 precision0
与 TIME 的 MILLI 规则共用。新增13项总测试 inventory（原4项保留，加7个 timestamp
precision case、1个 TIME(0) 隔离守卫和1个 zoned timestamp0案例）已实际运行：
红测 **FAIL / exit8：11 PASS / 2 FAIL、9.24s**；最小生产修复仅将 timestamp
precision0 分开为 SECOND，TIME 分支不变。重新编译所有 Release targets 实际
**PASS / exit0、0 compiler warnings/errors**，原13项绿测 **PASS / exit0、9.00s**，
日志 `/tmp/pangrova-production-sdk64-storage-timestamp-nearest-red-r1.log`、
`/tmp/pangrova-production-sdk64-main-timestamp-green-build-r1.log`、
`/tmp/pangrova-production-sdk64-storage-timestamp-nearest-green-r1.log`。
随后 Native 未筛选完整 CTest 实际 **4030/4030 PASS / 0 SKIPPED、569.10s**，
日志 `/tmp/pangrova-production-sdk64-native-unfiltered-ctest-r3.log`。
实际 SDK zoned visitor 同样使用 SECOND，timezone 来自 JVM systemDefault，
不能把 Lance format 的支持限制与 Arrow visitor 混为一谈。此修复只涉及 schema
admission，不冒充真实写入值转换：当前统一 timestamp NANO export 无法覆盖混合/
嵌套目标精度；Java physical TIME 仍请求已删除的 `time` 字段，SPI temporal 类型也
尚未完整保留精度。这些独立根因必须继续修复并实测。

### 0.48 2026-10-04 全量 ASan 暴露依赖布局漂移与测试证书隔离

Main ASan 所有 targets 实际构建 **PASS / exit0、0 compiler warning/error**，日志
`/tmp/pangrova-production-sdk64-main-asan-full-build-r1.log`；temporal 最近13项
实际 **PASS / 0 SKIPPED、72.16s**，日志
`/tmp/pangrova-production-sdk64-main-asan-storage-timestamp-nearest-r1.log`。
随后未筛选完整4030项 ASan CTest 已实际结束为 **FAIL / exit8：1821 PASS、
2209 FAIL、0 SKIPPED、1441.45s**，不是全量通过：
`/tmp/pangrova-production-sdk64-main-asan-unfiltered-ctest-r1.log`。多个真实执行计划、
Lance、远端交换测试在 `raw_hash_set.h:1616` 崩溃，调用链为 SDK/transport →
Release `re2::DFA` → ASan `HashSetResizeHelper::InitializeSlots`。实际日志有2206项
该 SEGV，另外3项在原始负例退出后报告单个256-byte LeakSanitizer 分配。

源码与实际缓存共同证明：`setup-native-asan-env.sh` 的 `RE2_PREFIX` 复用 Release；
Main/SDK 虽然选择 ASan Abseil/Protobuf，仍链接 Release RE2/gRPC/Arrow Flight。
Abseil 20240116 的 `ABSL_SWISSTABLE_ENABLE_GENERATIONS` 随 sanitizer 编译宏改变
CommonFields 的布局；相同头文件、namespace 或 package 路径不能证明 ABI 一致。
原 RE2 admission 只 `try_compile`，没有运行 template/regex 混用路径。

独立同源 oracle 的实际对照保留全部 hash/regex 操作：Release 配套控制 **PASS**；
ASan consumer（显式 `flat_hash_set<uint64_t>`）+ Release RE2 + ASan Abseil 实际
**FAIL / exit134**；在实际 owned thread 上同样崩溃。独立完整 ASan RE2 + 同 ASan
Abseil、相同 owned thread 并实际 join，`detect_leaks=1` + 仓库既有 LSAN 配置实际
**PASS / exit0**，没有新增 suppression、关闭 leak 或删断言。日志分别为
`/tmp/pangrova-production-sdk64-re2-sanitizer-abi-release-control-r1.log`、
`/tmp/pangrova-production-sdk64-re2-sanitizer-abi-owned-thread-mixed-red-r1.log`、
`/tmp/pangrova-production-sdk64-re2-sanitizer-abi-owned-thread-matched-green-r1.log`。
早先 main-thread oracle 两次 leak check 为 **FAIL**（65-byte 系统 TLS slot，未豁免）；
owned thread 退出真实释放该 slot。以上只关闭 RE2 根因对照，完整依赖 bootstrap、
匹配 SDK/Main 重建、全量 ASan 重跑仍未完成，不得将局部对照当生产 gate PASS。

Java 扩展 quality reactor r6 因 TestCommon 42 个 Checkstyle findings 实际 **FAIL**。
格式修正保留 TLS、结果和退出语义；IllegalImport 错拒 JDK17 公开
`jdk.httpserver/com.sun.net.httpserver`，现仅允许其精确公开 namespace，内部与近似
前缀仍拒绝。真实 pinned checker oracle 原公开3例 RED，修正后 **9 allowed /
14 rejected PASS**，日志
`/tmp/pangrova-production-sdk64-jdk-public-api-checkstyle-boundaries-green-r1.log`。
原 TLS/cancel/result 最近 selection **58 PASS / 0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-testkit-format-tls-cancel-nearest-r1.log`。
扩展原 reactor r7 在8个实际测试模块 **2014 PASS / 0 SKIPPED** 后，因 TestCommon
SpotBugs 两项实际 **FAIL**，client/Paimon 后续 quality 未执行，日志
`/tmp/pangrova-production-sdk64-client-paimon-real-static-analysis-r7.log`。

其中 `TestTlsAuthority` 将私有 temp directory 改为0755，leaf private key 未显式
0600，且 `IndependentSpeculationProcesses` 将 CA keystore 与所有角色私钥所在
workspace 整树0777/RW bind 到所有容器，是真实安全 fixture 缺陷。不能仅 chmod700
后保留旧容器 host-path 引用；必须宿主 CA/keys owner-only、仅每角色自身 leaf/root
复制到实际非root runtime owner、仅挂共享 warehouse。新增权限/注入边界2项最近
实际 **FAIL：1 PASS / 1 FAIL、0 SKIPPED**，日志
`/tmp/pangrova-production-sdk64-tls-authority-permissions-red-r1.log`；修复及真实
Docker 用户所有权验证的后续结果见下一检查点；当时另一项 COMMAND_INJECTION
尚未完成实际 argument vector、固定 flags 和输入边界复证。

### 0.49 2026-10-04 完整依赖 cohort、证书选项边界和结果所有权复证

本检查点基于用户已提交的 `2eb6a00` 及继续修复的工作区；没有回退或覆盖用户提交。
最终全链仍未验收完成，以下结果只覆盖注明的真实边界。

| 边界 | 状态 | 实际证据与范围 |
|---|---|---|
| RE2 runtime ABI 准入 | PASS | 保留 archive/package/header 检查，并实际执行 hash insert/contains、regex capture/replace；Release及ASan分别6/6，包含真实相反插桩模板模式被拒绝。日志 `re2-runtime-layout-admission-{release,asan}-nearest-r1.log`（下述统一日志前缀）。交叉编译无 emulator 明确拒绝；emulator 路径尚未实测。 |
| 完整 ASan 依赖 bootstrap | FAIL（r1），r2待验 | 不再复用 Release RE2/gRPC/Arrow Flight；实际固定 checkout 的 Abseil、Protobuf、RE2、gRPC及SDK1569节点构建完成。Arrow18保留LZ4/ZSTD，但LZ4官方地址empty reply、镜像404导致r1失败，未生成完整env。r2使用与实际Arrow锁定SHA256一致的LZ4/ZSTD源码tar，通过官方URL环境变量输入，仍从源码重新编译ASan而不复用Release库。日志 `asan-complete-abi-cohort-setup-r{1,2}.log`；r2未结束，不记PASS。 |
| Flight 缓存库准入 | PASS（工具边界） | 原 `find_library` 在切换prefix后仍接受旧缓存库，真实红测5项中4 PASS/1 FAIL；现在验证实际库存在且real path属于所选package/lib，5/5绿测。日志 `flight-stale-package-{red,green}-r1.log`。不等于Flight服务器或端到端执行PASS。 |
| CI cohort wiring | PASS（语法），运行待验证 | ASan configure显式选择完整prefix、SDK、Arrow/Flight、RE2、gRPC、Protobuf、Abseil，并显式清理旧Flight library cache后按唯一所选prefix查找。实际Ruby YAML解析及60个run block的 `bash -n`通过；没有执行hosted CI。 |
| CA/角色材料真实 owner | PASS（专项） | CA目录0700、store/key0600；独立容器只接收自身leaf/root，tar写前限额、真实非root Config.User所有权及缓冲清零，Spec仅挂warehouse。最近13项含4项真实Docker ownership测试通过、0 SKIPPED；日志 `tls-authority-runtime-owner-nearest-green-r1.log`。没有当前同源Pangrova Linux镜像，因此不冒充Spec进程E2E。 |
| keytool选项边界 | PASS | 独立argv可防shell解释，但仍不能防keytool把 `-importcert` 名称识别为第二操作。实际正确拒绝oracle先FAIL，收紧name首字符后4/4 PASS；同时实际验证含shell语法的workspace作为字面路径处理。日志 `keytool-option-alias-{red,green}-r1.log`。 |
| 精确静态误报分类 | PASS（工具边界） | 先修真实选项/权限缺陷，再只匹配private `TestTlsAuthority.run(List):void` 的 COMMAND_INJECTION。真实pinned SpotBugs/FindSecBugs Filter API：新增1正例/7类、方法、签名、bug变更负例通过，原immutable hash的1正例/8负例保留通过。日志 `keytool-spotbugs-exact-filter-boundaries-r1.log`；没有全包command豁免。 |
| TestCommon完整门禁 | PASS | 扩展reactor r8实际89项PASS、0 SKIPPED、Checkstyle0、SpotBugs0；比原87项增加2个实际keytool边界。日志 `tls-keytool-client-paimon-full-gates-r8.log`。 |
| 扩展client/Paimon门禁 r8 | FAIL | 继续到Transport时71项测试PASS、0 SKIPPED，但46项Checkstyle格式问题使后续client/Paimon未执行。修正只展开控制流和换行，不改变原生命周期/错误语义。随后最近21项PASS、Checkstyle0，但ResultBatch的4项真实mutable representation告警仍FAIL；日志 `result-transport-format-closest-green-r1.log`。 |
| ResultBatch输入/输出所有权 | PASS（所属模块），完整consumer待验 | 原public record直接保存并返回可变columns/rows，嵌套vector/binary/Text/Timestamp可经输入或getter修改。有效红测5/5 FAIL；第一次test-compile的wildcard断言错误不计行为红测，已修正后执行。日志 `result-batch-owner-red-r{1,2}.log`。修复按真实客户端heap边界快照、保留NULL和timestamp纳秒；未知mutable cell及递归cycle明确拒绝。最近31项PASS后，r9完整Transport76项PASS、0 SKIPPED、Checkstyle/SpotBugs0。CLI只消费一次detached snapshot，避免重复getter整批复制；不新增Arrow执行旁路，不宣称heap物化客户端已支持有界大结果流。r9在REST质量门禁失败，CLI/JDBC/Paimon未执行。 |

上表日志统一位于 `/tmp/pangrova-production-sdk64-` 加所列文件名。
§0.48的3项单个256-byte报告仍保持FAIL，不新增suppression或关闭leak check。
独立无Pangrova/SDK链接的LLVM17/macOS oracle实际对照：仅构造并销毁runtime_error
的1次和1000次均PASS（使用仓库既有配置；1000次日志也记录既有libc++abi的
单个256-byte suppression，不是完全无抑制的泄漏否定证据）；复制runtime_error/out_of_range以及失败的
`dlopen(RTLD_NOLOAD)` + `dlerror()`，1次/1000次均只残留同一个256-byte分配、FAIL。
这与libc++异常字符串复制触发dyld TLS错误缓存的实现一致，支持系统cache/root扫描
缺口，而非随失败次数累积的业务对象泄漏；不替代匹配cohort后的原3项测试复跑和
实际Lance drain/destroy/pool验证。日志
`/tmp/pangrova-production-sdk64-system-exception-owner-{construct,runtime-copy,logic-copy,dlopen}-{1,1000}-r1.log`。

仍需在同源冻结checkpoint完成Main Release/ASan、未筛选CTest、JNI重打包、全Java
regression/coverage、全部E2E和真实Smoke。合法VALUES入口、函数/cohort消费、真实
混合/嵌套timestamp写入、TIME精度和大规模部署指标的独立缺口仍未关闭；不得因
以上局部PASS宣称“全部修复”“生产就绪”或未来架构无风险。

### 0.50 2026-10-04 REST 重试/错误协议边界和失败资源基线

扩展client/Paimon reactor r9已实际结束：**FAIL**，已到达的9个测试模块合计
**2198 PASS / 0 SKIPPED**，其中Transport76、REST98。REST的23项Checkstyle阻断
CLI/JDBC/Paimon；展开控制流/Javadoc后最近质量检查Checkstyle0，但暴露9项
SpotBugs，不能将测试通过和格式通过写成完整门禁通过。
日志 `/tmp/pangrova-production-sdk64-result-owner-client-paimon-full-gates-r9.log`、
`/tmp/pangrova-production-sdk64-rest-format-auth-owner-nearest-green-r1.log`。

有效行为红测19项：**14 PASS / 4 FAIL / 1 ERROR / 0 SKIPPED**。真实缺陷为
`RetryPolicy` 接受NaN/Infinity、首轮不受maxDelay约束、零delay被改成1ms、
Long.MAX_VALUE毫秒的inclusive随机上界溢出，以及`parseColumns(null)`直接NPE。
日志 `/tmp/pangrova-production-sdk64-rest-metadata-retry-boundaries-red-r1.log`。
生产修复约束有限multiplier、首轮cap、保留零delay、使用`nextLong(base)+1`避免上界
溢出；metadata的实际对象/字段边界直接抛typed protocol error，不返回空元数据。
错误响应只捕获Jackson解析和明确无效payload，保留cause及HTTP4xx/5xx分类；
新增8个真实本地HTTP错误body案例，JSON null/缺失字段不得伪造有效ErrorDetail。
本轮最近绿测r1因PATH选择系统Python<3.10在canonical generator失败，**未运行REST**；
已恢复既有Python/JDK17工具链后r2重跑：**45 PASS / 0 SKIPPED**，Checkstyle0、
SpotBugs0；13个reactor模块构建成功，未匹配专项测试的上游模块不计测试PASS。
REST/CLI/JDBC/Paimon完整未筛选reactor r10已失败：到达的10个模块共2571 PASS、
0 SKIPPED；Paimon357项通过，但40项Checkstyle阻断后续CLI/JDBC，后者未执行。日志
`/tmp/pangrova-production-sdk64-rest-metadata-retry-error-green-r{1,2}.log`。

6项metadata空值告警按真实guard修复，2项广域catch按错误契约收窄，没有排除。
Retry jitter仅作为`RetryableHttpExecutor`的等待，不生成认证材料/标识/nonce；
精确分类限定owner、int参数、Duration返回、唯一method和PREDICTABLE_RANDOM。
真实pinned Filter API新增1正例/7变更负例**PASS**，原2组分类边界保留通过：
`/tmp/pangrova-production-sdk64-retry-jitter-exact-filter-boundaries-r1.log`。
这不是认证随机数安全性豁免，也不是完整REST质量门禁通过。

Lance原失败用例保留全部旧断言，增加真实C ABI `close`/`isDrainedAndCleaned`/
`releaseDrained`/幂等关闭及actual pool baseline。新增两个同进程32次失败循环，
验证实际allocator bytes、pool数和weak owner均回到基线，文件保持不变或不发布；
最近组合专项已实际构建执行：Governor28、Lance10，**38 PASS / 0 SKIPPED**。
组合drained/cleaned标记不能证明
全部Rust heap峰值或每个编码任务计数。现另发现governor active pool计数随spill
reporting递减而非随真实pool lifetime释放，已用weak registry修复并复验，不用`reportSpillBytes(0)`
当作资源释放证据。

Darwin arm64 LSan的3个原256-byte失败仍未关闭。LLVM实际macOS TLS root扫描
`TlsSize()==0`，官方LSan目标测试仅列Darwin x86_64；公开Apple Silicon报告未修复，
升级或ObjC/dyld suppression不构成已验证TLS修复。必须保留原ASan测试结果，并在
同源Linux匹配依赖上执行完整ASan，不能为获得绿色移动原gtest到另一线程或新增
suppression。当前没有该实际Linux全量运行证据，也没有大规模部署SLO实测。

### 0.51 2026-10-04 实际 pool 所有权、Paimon DTO 和 RE2 探针缓存根因

Governor新增9项真实SDK测试，修复前28项中20 PASS / 8 FAIL / 0 SKIPPED；
源码原active计数在spill report时递减，失败或未report任务则遗留计数，甚至变负。
现以短锁保护weak root registry，在create/snapshot清理expired owner；不把spill
统计当作task释放。4项旧测试的错误oracle改为真实root/leaf退出，原spill值、
实际buffer分配、Proto字段断言保留。修复后Governor28和Lance10合计38 PASS。
Lance新增2项各32次同进程失败循环，真实C ABI drain/destroy、pool/allocator/weak
owner基线不累积。日志 `/tmp/pangrova-production-sdk64-governor-owner-red-r1.log`、
`/tmp/pangrova-production-sdk64-governor-lance-owner-green-{build-r1,r1}.log`。
这不是原ASan三项已通过或Rust全部任务内存已测量的声明。

Paimon40项Checkstyle修复后最近7项PASS/格式0，却真实暴露41项SpotBugs，未将
这轮失败写成成功。随后两个公开record构造器的输入/getter mutation红测：
18项中11 PASS / 7 FAIL / 0 SKIPPED；真实SDK VARCHAR的VARIANT handle也会随
调用者修改options漂移。现在record唯一owner冻结集合，factory不重复预复制。
Split六集合原本已冻结，内联copyOf仅令分析器识别不可变返回；原null→empty
和完整proto字节稳定性保持。溢出校验保留ArithmeticException，重复write分支及
无消费者postpone字段删除，固定/动态bucket最近真实SDK写读断言保持。
组合最近专项：**45 PASS / 0 SKIPPED、Checkstyle0、SpotBugs0**，1m12s。
日志 `/tmp/pangrova-production-sdk64-paimon-mutable-schema-dto-{red-r1,green-r1}.log`。
19项Catalog/Table/FileIO/registry/service告警仅分类为borrowed DI identity，精确
限定class/method完整签名/field类型/单bug。pinned Filter API每项1正例/9变更负例
全部PASS；原hash/keytool/jitter边界保留。日志
`/tmp/pangrova-production-sdk64-paimon-borrowed-service-filter-boundaries-r1.log`。
这些分类不证明线程安全或shutdown drain：当前Connector只有catalog.close和
registry.clear；system-table Java行设计、missing split假EOF、query注册/清理未
接通、Java incremental/warmup/index路径及隐藏的3个`.java.broken`测试仍待处理。
扩大未筛选client/Paimon reactor r11正在执行，不预记PASS。

完整匹配ASan依赖bootstrap r2已PASS，SDK355增量目标及Arrow/Flight225目标完成；
源码缓存codec tar按官方SHA复核再编译，不借用Release codec二进制。首次Main
配置仍FAIL：嵌套`re2-contract-check`缓存的re2_DIR选中Release包，实际link.txt
证明混用ASan Abseil与Release RE2。新增真实CMake缓存切换红测1 FAIL；现外层和
try_compile显式绑定已核定的canonical SDK RE2目录。Release与ASan各7项真实
package测试PASS，含不匹配sanitizer及重复archive拒绝；Main配置r2 PASS，子探针
实际缓存/link均指向ASan cohort。依赖bootstrap与配置通过不等于主程序ASan通过。
日志 `/tmp/pangrova-production-sdk64-asan-complete-abi-cohort-setup-r2.log`、
`/tmp/pangrova-production-sdk64-re2-cached-probe-{red-r1,green-release-r1,green-asan-r1}.log`、
`/tmp/pangrova-production-sdk64-main-asan-complete-cohort-configure-r{1,2}.log`。

本轮完整同源Release/ASan、打包JNI、全Java、E2E/Smoke、多进程大规模SLO验收
尚未完成；唯一Calcite前端和TIME/timestamp精度方案仍是设计，不写成已落地。

### 0.52 2026-10-04 Release 全量通过与 Java 严格门禁失败复证

当前 Native 源码的完整 Release 构建通过；实际 pool、Lance、Columnar 所有权
最近 41 项通过，未过滤 CTest **4041 PASS / 0 FAIL / 0 SKIPPED**，耗时636.46秒。
日志 `/tmp/pangrova-production-sdk64-pool-re2-release-nearest-r1.log`、
`/tmp/pangrova-production-sdk64-pool-re2-release-unfiltered-r1.log`。
Main ASan 完整 cohort 的构建仍在执行，不能由 Release 或依赖构建通过推导 ASan 通过。

Java client/Paimon 严格回归 r11 中十个上游模块2583项通过且质量门禁通过；
JDBC149项通过、1项旧测试错误地要求暴露调用方 VECTOR 数组，导致整次FAIL，CLI未执行。
现保留全部值/维度/SQL Array断言，并加强跨 ResultSet 与调用方数组所有权隔离验证。
最近 JDBC/Transport 16项通过、Checkstyle零错误，但 SpotBugs 19项仍导致FAIL。
日志 `/tmp/pangrova-production-sdk64-result-owner-client-paimon-full-gates-r11.log`、
`/tmp/pangrova-production-sdk64-jdbc-result-vector-owner-nearest-r2.log`。

r12 使用 Maven fail-at-end，仅使独立 CLI 也获得执行机会，没有放宽门禁：
全12个实际测试模块共 **2772 PASS / 0 FAIL / 0 SKIPPED**；整次构建仍为 **FAIL**，
因为 JDBC19个SpotBugs告警与CLI3个Checkstyle错误，不能记作全量回归通过。
JDBC告警包含6个JDBC实际句柄身份getter、11个原样SQL转交重载、1个真实的
Prepared参数拼接安全问题及1个nullable Boolean分类接口；不能把真实参数注入
用SQL告警排除规则掩盖。任意Number/BigDecimal及时间子类可覆盖文本方法，
必须先用公开setObject绑定路径复现，再使用封闭数值类型和规范时间值修复。
日志 `/tmp/pangrova-production-sdk64-result-owner-client-paimon-full-gates-r12.log`。

隐藏的27项Paimon/Iceberg测试、唯一Calcite公开入口迁移、实际Native能力快照消费、
TIME/timestamp精度、同源JNI打包、全仓Java及真实分布式E2E仍未验收完成。
上述局部通过不构成生产级或大规模性能结论。

### 0.53 2026-10-04 完整 ASan 与 JDBC 实际调用所有权复证

完整匹配依赖 cohort 的 Main ASan 构建已通过；未过滤 CTest 实际执行全部4041项：
**4036 PASS / 5 FAIL / 0 SKIPPED**，1841.98秒，退出码8。四项真实 Lance
条件创建/拒绝/32次失败循环及一项 NullAwareMark 执行测试的业务断言通过，但进程
退出各报告同一栈的256字节分配，因此这五项及整个 ASan 门禁仍为 **FAIL**。
日志 `/tmp/pangrova-production-sdk64-main-asan-complete-cohort-unfiltered-r1.log`。
完整 cohort 消除了此前混合 ABI/RE2 的崩溃，不等于发布链已通过。

根因证据进一步落实到实际分配，而不只按报告大小猜测：无 Pangrova/Velox 链接的
异常复制 oracle 与原 Lance 测试均在同一主线程运行；LLDB 在 dyld
`setErrorString+268` 跟踪256字节分配，指针随后原样传入 `pthread_setspecific`
的 key `0x102`，`pthread_getspecific` 返回同一指针，内存头记录长度256。
原 Lance 栈由系统 libc++ 的 `runtime_error` 复制进入该路径；继续原测试后业务
断言通过、LSan仍报告该栈并以1退出。未替换线程、关闭 leak detection 或增加
suppression。日志 `system-exception-owner-lldb-tls-attribution-r4.log` 与
`native-lance-dyld-tls-attribution-r1.log`，均位于上述 `/tmp/pangrova-production-sdk64-`
前缀下。这证明本机报告的分配所有者是 Darwin dyld TLS，不证明其他运行环境
无泄漏；同源 Linux 全量 ASan 仍无结果，不能将当前失败改记PASS。

JDBC 真实公开参数路径红测复现五项缺陷：任意 Number/BigDecimal 子类及 JDBC
日期/时间子类的可覆盖文本方法可进入未引用 SQL。现在仅接受封闭的精确数值类，
拒绝非有限浮点值；时间值经规范类型转换后编码，保留时间戳纳秒。新增10项
公开绑定、实际零提交拒绝和文字边界测试通过；没有为 Prepared SQL 拼接增加
SpotBugs 排除规则。日志 `jdbc-prepared-literal-red-r1.log`。

PreparedStatement 原每次建立临时 Statement，取消、超时、ResultSet producer 和
关闭均无法指向实际执行 owner。八项真实 HTTP/Flight 客户端边界红测全部失败；
现由一个持久 delegate 承担唯一执行和取消，返回 public PreparedStatement
作为结果 producer，关闭先封闭 admission、忙执行要求取消并重试关闭，取消失败
保留同一 queryId 和 suppressed 原因。进一步强断言复现 timeout 的双重取消：
第一次 ACK 后第二次 NOT_FOUND 会使下一次执行被未释放身份阻断。现在 waiter
只报告超时，由唯一 execution owner 取消；第九项真实 ACK/NOT_FOUND 边界通过。
最近专项 **98 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、Checkstyle0**；四项真实
句柄身份 getter 的静态分析分类当时仍导致整次 verify FAIL，不混记绿色构建。
日志 `jdbc-prepared-owner-red-literal-green-r1.log`、
`jdbc-timeout-double-cancel-red-r1.log`、`jdbc-prepared-owner-green-r2.log`。

11条原样 SQL 转交及4条句柄身份 getter 仅精确匹配类、完整方法签名、相应字段
及单项 bug；锁定 SpotBugs Filter API 的全部原有边界和新增身份正/负例通过，
完整过滤器仍拒绝 Prepared SQL 构造、nullable Boolean 和已删除的 Prepared
getter 告警。新增实际 HTTP/Flight 身份4项通过；未筛选完整 client 严格门禁 r13
**2437 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED**、所有模块 Checkstyle/SpotBugs0，
BUILD SUCCESS，4分06秒，日志 `client-full-gates-r13.log`。
实际测试模块计数：Common246、Config164、SPI706、ShuffleCore121、Protocol418、
Observability280、TestCommon89、Transport76、REST114、JDBC185、CLI38。
这次未包含正在修改 query authority 的 Paimon，不是全仓回归；上述客户端 peer
测试不是 Coordinator→Native SQL E2E。
Paimon 实际 query authority、唯一 Calcite 前端、能力快照、TIME/精度以及完整
JNI/Java/E2E/Smoke/多进程 SLO 的整改与验收仍开放。

随后 REST leaf 最近边界实测 **21项：16 PASS / 4 FAIL / 1 ERROR / 0 SKIPPED**：
最大重试次数使 `maxRetries+1` 溢出并在首请求前抛NPE；两退避字段和两HTTP计时
字段接受超出实际毫秒域的Duration。修复使用long attempt、真实毫秒表示校验及
明确的HTTP计时窗口（半个signed-long毫秒域的算术余量，不承诺任意wall-clock
跳变安全），不施加错误的纳秒域限制。最近 r1 的24项中一个新增测试误要求
非幂等submit POST重放，实际生产入口明确禁止重放；只纠正该错误测试为实际
一次POST/原文/503异常，并另测幂等GET 503→200，没有改生产语义迎合测试。
r2最近 **25 PASS / 0 SKIPPED**；完整 leaf REST 严格门禁 **124 PASS / 0 FAIL /
0 ERROR / 0 SKIPPED、Checkstyle0、SpotBugs0**，18.775秒。leaf使用已有依赖
artifact，不是正在修改的query authority全reactor验收；日志
`rest-retry-timer-leaf-{red-r1,green-r1,green-r2,full-gates-r1}.log`。

当前Release库的Java/JNI导出对称检查 **22项PASS**；为独立本机JNI诊断，已将
同一本次Release库重新包装进osx/aarch64 classpath（Lance C ABI验证通过）。
日志 `release-jni-symbol-symmetry-r1.log`、`local-release-jni-package-r1.log`。
这不是Linux生产打包或ASan失败后的发布链放行；实际Java加载、E2E和Smoke仍
未完成。Docker实查为aarch64、2CPU、4102721536字节，且无本次同源Pangrova镜像；
生产staging脚本要求Linux x86_64，不能以本机dylib替代该生产证据。

### 0.54 2026-10-04 REST 端点归属与 Prepared 绑定准入复证

真实HTTP端点归属红测实际执行3项：**2 PASS / 1 FAIL / 0 ERROR / 0 SKIPPED**。
另一请求把首节点A切换到B后，此前已构造的A请求失败会被全局计数误归为B失败，
实际重试序列为A→C，跳过健康B。使用唯一有界端点选择owner，将实际请求与
创建时的不可变selection绑定；以对象身份CAS推进匹配的preferred selection，
每个请求独立顺序遍历端点，删除重复读取全局计数和整数环绕。无关闭failover
时的端点扩展，也无非幂等SQL POST重放。新增加实际三轮环绕/过期generation和
四并发请求响应屏障，原三个断言及旧线程安全测试保留。
最近组合 **33 PASS / 0 SKIPPED**；完整REST leaf严格回归两轮各
**129 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、Checkstyle0、SpotBugs0**，
分别18.785秒和22.499秒。第二轮安装实际验证过的当前artifact。
日志 `/tmp/pangrova-production-sdk64-rest-endpoint-attribution-leaf-`
`red-r3.log`、`green-r1.log`、`full-gates-r1.log`、`full-gates-install-r2.log`。
此前red-r1是其它正在修改POM的模型错误，red-r2是新增测试漏import的编译错误；
二者不是行为红测，不计为故障转移测试失败或通过。

Prepared绑定准入红测 **45项：2 PASS / 43 FAIL / 0 ERROR / 0 SKIPPED**，
实际21种已支持setter接受非法index，多数setter和clearParameters在关闭后仍
修改绑定。现在唯一bind入口先检查关闭与实际marker范围，再写入原参数owner；
参数数量与ParameterMetaData共用现有占位符识别，非法index立即报07009且保留
原有效绑定。引号/注释中的问号、合法重绑定、clear和原执行取消所有权均保留。
最近四类 **88 PASS / 0 SKIPPED**；完整JDBC leaf最新
**230 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、Checkstyle0、SpotBugs0**，31.261秒。
日志 `/tmp/pangrova-production-sdk64-jdbc-binding-admission-leaf-`
`red-r1.log`、`green-r1.log`、`full-gates-r2.log`。

首次JDBC leaf全量仍 **FAIL：228 PASS / 2 FAIL / 0 ERROR / 0 SKIPPED**。
实际javap证明本地Maven缓存的旧Transport ResultBatch构造器/getter直接保留并返回
rows，当前reactor classes则调用snapshotRows。未改VECTOR断言或在JDBC复制一个
兼容路径；从当前Transport源码完整严格测试并安装后，两项原断言通过。
Transport **76 PASS / 0 SKIPPED、Checkstyle0、SpotBugs0**，20.756秒，日志
`/tmp/pangrova-production-sdk64-transport-current-snapshot-leaf-install-r1.log`。
这些leaf运行不扫描尚未完成的Coordinator/Paimon模型，也不验证正在迁移的
query authority新SPI；完整冻结后的同源reactor仍必须重跑。

本节记录时真实report drain listener组件回归仅准备源码，query authority/Paimon
有限迁移单元尚未冻结；后续编译与运行结果见0.55，不将历史未执行状态计为PASS。
唯一Calcite前端、能力快照消费、
TIME/精度、同源Linux/ASan、真实JNI/E2E/Smoke及独立Worker规模SLO仍未验收。
本节不新增生产级、HA或大规模性能通过结论。

### 0.55 2026-10-04 查询生命周期原子编译及排空通知复证

实际 JDK17、`full-regression,coverage` 的29模块 `clean test-compile` 首轮
**FAIL / exit1**：Coordinator 测试两处不符合实际 Paimon1.4.1 SDK。
`DelegateCatalog` 仍要求 `catalogLoader()`，`DataTypes.FIELD` 要求显式 field ID。
前28模块及Coordinator生产源码编译成功，不计测试通过。修正为实际底层Catalog
loader委托和真实schema字段0；保留原断言，不增加fake实现或改变SDK版本。
日志 `/tmp/pangrova-production-sdk64-connector-authority-clean-testcompile-r1.log`。
随后的同源29模块测试调用已完成全部 main/test 编译。实际生成的Paimon测试classifier
仅含 `org.pangrova.testfixtures.tableformat` 两个helper及package-info，独立
`Automatic-Module-Name`，不含迁移前生命周期测试或production split package。

排空通知红测实际 **6项：1 PASS / 5 FAIL / 0 ERROR / 0 SKIPPED**。
`exitReport`已claim通知且仍持有Scope操作，但callback再次要求
`notificationsCompleted`，导致整个监听器fanout被静默跳过。修复为已claim回调
直接发布原保留receipt，成功完成后才标记通知完成；并发后退出的publication
不得重放已完成fanout。失败仍保留事实、重试整个幂等fanout；普通直接取消的
通知ready门禁保持不变，无第二owner或成功监听器journal。
日志 `/tmp/pangrova-production-sdk64-task-drain-notification-red-r1.log`。

最近组合实际 **18 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED、47.695秒**：
SPI清理结果1、issued Scope2、Registry drain7、通知6、Query authority2。
日志 `/tmp/pangrova-production-sdk64-task-drain-notification-green-r1.log`。
这是实际Tracker/listener和查询状态所有权组件证据，测试中生成的receipt不是
Native物理退出或独立Worker故障恢复证据。真实SDK生命周期、终态日志及更广
消费者回归正在执行；全仓Java、当前JNI、E2E、Smoke、Linux/ASan及规模SLO
仍未全部通过，整体生产验收保持开放。

首次扩展回归已实际关闭 **FAIL / exit1、1分22秒**：SPI1 PASS；Coordinator
152项中 **134 PASS / 3 FAIL / 15 ERROR / 0 SKIPPED**。真实Paimon registry
13项已正式发现并全部运行通过；真实SDK读写、首访问binding、Metadata失败聚合
及PlanningReadBindings各自通过，不将整个失败cohort改写为PASS。失败分为：

- Paimon关闭测试应精确检查既有 `ConnectorException` 的 `IllegalStateException`
  cause；仍保留active-reader计数、关闭/清理失败及真实reader退出后的重试断言。
- public `submitQuery(queryId,sql,unboundSession)` 在新增authority绑定前未将未准入
  context规范化为实际queryId。修复创建owner前的唯一 `withQueryId`，保留原并发
  fixture和断言；已准入context跨query拒绝且不创建新owner。
- 9项Iceberg相关ERROR来自实际classpath的Avro1.7.7，缺Conversion/LogicalTypes。
  Maven实际dependency tree确认Coordinator直接Hadoop mapreduce路径更短，覆盖了
  Iceberg core所需1.12.1。parent唯一管理Avro1.12.1，与
  [Iceberg1.11.0官方依赖](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/gradle/libs.versions.toml)
  对齐；复查dependency tree已只有该版本，不复制JDBC/SDK兼容路径。
- 4项真实JNI/report测试在初始化runtime后直接GET_CAPABILITIES，缺真实startup
  function catalogue publication。正修复既有component fixture初始化顺序；禁止
  capability读取隐式finalize或伪造manifest。

日志 `/tmp/pangrova-production-sdk64-connector-authority-lifecycle-focused-r1.log`、
`/tmp/pangrova-production-sdk64-avro-dependency-tree-r{1,2}.log`。依赖图检查不是
SDK执行通过；修复后必须复跑原cohort及全仓消费者。

### 0.56 2026-10-07 当前基线复证与实际报告失败边界

基线为 `058fb13`，保留用户暂存的 `description`、`index`、`packed-refs` 删除。
本次环境为 Corretto17.0.20、Maven3.9.16、macOS/aarch64；实际 Docker VM
仅2CPU/约4GB，不能作为生产规模环境。`.pangrova/setup.sh --check` PASS。

本次已执行的结果（历史 PASS 不自动升级为当前整链验收）：

| 检查 | 实际结果 | 证据与边界 |
|---|---|---|
| 全39模块 `mvn -DskipTests install` | PASS，54.535秒 | main及test源码编译完成；行为测试明确SKIPPED。日志 `/tmp/pangrova-production-20261007-current-full-java-install-r1.log`。 |
| 原查询/SDK生命周期扩展回归 | FAIL，164项中161 PASS / 3 ERROR / 0 SKIPPED | SPI1、Coordinator163；真实PG denied-owner10项通过，不能把失败cohort计为PASS。日志 `/tmp/pangrova-production-20261007-lifecycle-current-r1.log`。 |
| 最近Worker/JNI链 | PASS，8项 / 0 SKIPPED | `NativeLibraryLoaderTest,NativeCreationTransactionIntegrationTest,WorkerExecutionChainTest`；真实JNI、Flight、Native receipt与handle回收，不是公开SQL或独立进程规模证据。日志 `/tmp/pangrova-production-20261007-worker-jni-current-r1.log`。 |
| 当前Release构建与全量CTest | PASS，4041项，566.76秒 | `cmake --build pangrova-worker-native/build --parallel 2` 及无筛选 `ctest --test-dir pangrova-worker-native/build --output-on-failure`。产物和已打包资源SHA256均为 `f2c46b5758df1615c5bc4233f0b1f2be3ce5a8b71c748a7a762bdf60ce4829f1`。日志 `/tmp/pangrova-production-20261007-native-release-{build,ctest}-r1.log`；不是ASan或独立Worker证明。 |
| RUNNING报告修复最近组合 | FAIL，6项中4 PASS / 2 ERROR / 0 SKIPPED | 3项冻结身份合同及新增真实认证HTTP/JNI进度重试通过；permit及非终态PG观察器仍失败。日志 `/tmp/pangrova-production-20261007-progress-report-green-r1.log`。 |
| 客户端及上游严格模块回归 | PASS，2498项 / 0 SKIPPED，3分07秒 | `mvn -Pfull-regression,coverage,static-analysis -pl pangrova-client/pangrova-client-jdbc,pangrova-client/pangrova-client-cli -am install`；各模块Checkstyle/SpotBugs均0。日志 `/tmp/pangrova-production-20261007-clients-current-full-gates-r1.log`。不包含全部Planner/Worker/Coordinator/E2E。 |
| 两阶段终态与观察器最近组合 | FAIL，12项中11 PASS / 1 ERROR / 0 SKIPPED | 真实pre-release FINISHED丢ACK/耗尽ACK后原事实重试及更晚actual-release、PG非终态异步/终态同步、signed ticket/实际关闭重初始化通过；permit仍被同journal retained失败批次阻断。日志 `/tmp/pangrova-production-20261007-terminal-publication-green-r2.log`。 |
| 两阶段故障oracle复跑r3 | FAIL，12项中11 PASS / 1 ERROR / 0 SKIPPED | permit已断言真实cleanup异常及PostgreSQL42501；随后错误要求未获原报告ACK前出现released报告，实际仅有冻结seq2/FINISHED/drain UNSPECIFIED。日志 `/tmp/pangrova-production-20261007-terminal-publication-green-r3.log`。须校准阶段边界，不能制造receipt或提前归还许可。 |
| PostgreSQL readiness修复 | PASS，2项 / 0 SKIPPED，11.410秒 | `mvn -Pfull-regression,coverage -pl pangrova-testing/pangrova-test-e2e -Dtest=SharedTestContainersConfigurationIntegrationTest test`，真实容器SELECT1、三项持久化配置及异步提交拒绝均通过。日志 `/tmp/pangrova-production-20261007-postgres-bootstrap-green-r3.log`。 |
| 公开Smoke/Lance/Ollama链路r3 | FAIL，7项中4 FAIL / 3 ERROR / 0 SKIPPED，50.732秒 | 启动已越过模型、Native publication、PG、HTTP；真实tenant零quota拒绝429及重复create默认schema阻断业务验收。日志 `/tmp/pangrova-production-20261007-shared-ai-lance-e2e-r3.log`。不是完整业务E2E通过。 |
| 真实query-terminal PG拒绝/恢复r4 | PASS，1项 / 0 SKIPPED | 原FINISHED报告未ACK时保持exact body/seq/time、没有drain receipt、许可仍持有；恢复后actual released报告ACK、FINISHED只落库一次、许可只归还一次。日志 `/tmp/pangrova-production-20261007-terminal-permit-green-r4.log`。 |
| Worker完整严格回归r2 | FAIL，运行至159项，1 FAIL / 12 ERROR，随后JVM134中止 | 实际占用端口触发FlightResultServer中的外部Status CHECK并终止进程，不是OOM；还有空注册batch和过期测试合同。日志 `/tmp/pangrova-production-20261007-worker-full-gates-r2.log`。整套未跑完。 |
| 新Native端口修复Release专项r3 | PASS，4项 / 0 SKIPPED | 双栈真实监听、独立IPv4/IPv6 bind均证明EADDRINUSE；失败以exception/canonical响应返回，registry未发布，可重启并实际排空。日志 `/tmp/pangrova-production-20261007-native-result-port-{build,nearest}-r3.log`。 |
| 新Native端口修复ASan专项r3 | PASS，4项 / 0 SKIPPED | 同源ASan SDK/依赖增量构建，真实端口失败、TLS错误、RAII和重启断言全通过。日志 `/tmp/pangrova-production-20261007-native-asan-result-port-{build,nearest}-r3.log`。不是全量ASan通过。 |
| 新库Worker最近组合r4 | FAIL，30项中29 PASS / 1 ERROR / 0 SKIPPED | 原WorkerResultPortIntegrationTest 3项全部通过；default启动与empty registration修复通过。唯一剩余为同handle重复shuffle start被Native拒绝，不能把组合计为PASS。日志 `/tmp/pangrova-production-20261007-worker-port-bootstrap-nearest-r4.log`。其中既有stub bootstrap仅组件测试，不作为真实Coordinator验收。 |
| query隔离journal实际PG最近检查 | 行为PASS，18项 / 0 SKIPPED；严格verify FAIL | r1已实际运行18项再被Checkstyle151项阻断；r2行为复跑也18项通过。日志 `/tmp/pangrova-production-20261007-journal-query-isolation-{green-r1,behavior-r2}.log`。静态门槛保持不变，尚未形成模块PASS。 |
| 公开部署fixture业务链r4/r5 | FAIL，r4为2 ERROR；r5为2 FAIL / 0 SKIPPED | r4真实HadoopCatalog仓库root不存在；补齐部署root后r5越过该边界。r5为opaque结果票据误当taskId、Worker catalog fetch实际HTTP401。日志 `/tmp/pangrova-production-20261007-shared-deployment-green-{r4,r5}.log`。模型pull的DNS超时曾自然恢复，最终不是环境阻塞而是业务失败。 |
| signed-result客户端合同r6/r7 | FAIL，r6编译失败；r7为13项中12 PASS / 1 FAIL / 0 SKIPPED | r6缺SubmitTaskRequest import已修；r7签名票据合同通过，独立control identity fixture仍缺必需资源grant。补齐有限合同grant并新增clear-grant拒绝断言，r8待验证。日志 `/tmp/pangrova-production-20261007-e2e-signed-result-contract-{r6,r7}.log`。此类合同测试不是公开业务E2E。 |
| signed-result及控制身份合同r8 | PASS，13项 / 0 SKIPPED，1分17秒 | 有限resource grant保留、clear-grant显式拒绝；opaque票据/真实签发、歧义binding拒绝及closed-port等原断言全部通过。日志 `/tmp/pangrova-production-20261007-e2e-signed-result-contract-r8.log`。非公开业务/独立进程规模验收。 |
| 新Native端口cohort完整Release CTest r3 | PASS，4043项，564.93秒 | 新4项专项已包含在无筛选全套。日志 `/tmp/pangrova-production-20261007-native-release-ctest-r3.log`。完整ASan及后续shuffle整改不自动继承此PASS。 |
| Metadata全模块严格回归r4 | 行为PASS，502项 / 0 SKIPPED；严格verify FAIL | Checkstyle从151降为0，SpotBugs仍50项。日志 `/tmp/pangrova-production-20261007-metadata-full-gates-r4.log`。共享DI身份、已冻结集合和实际可变数据须分别核实，不能通过复制协作者或blanket suppression制造PASS。 |
| 目录部署/模型部署源码全39模块刷新r5/r6 | r5 FAIL；r6编译PASS，52.138秒 | 移动真实owner测试遗漏既有native-fixtures classifier，补test-scope依赖后39模块编译成功；行为测试SKIPPED。日志 `/tmp/pangrova-production-20261007-auth-deployment-full-java-refresh-{r5,r6}.log`。 |
| 实际认证HTTP/Worker owner最近r6/r7 | r6 FAIL，15项中11 PASS / 4 ERROR；r7 PASS，18项 / 0 SKIPPED | r6保留生产loopback拒绝，fixture改绑定实际非回环NIC；旧null QueryDispatcher/假认证器换成真实dispatcher及签名OIDC。原owner四项、catalog PG/HTTP/真实client三项、secret四项、filter三项、mapper四项全通过。日志 `/tmp/pangrova-production-20261007-coordinator-auth-owner-nearest-{r6,r7}.log`。 |
| 实际Ollama部署最近r6 | PASS，2项 / 0 SKIPPED | 同一有限deadline中实际pull两模型并核库存digest；真实pause后timeout、日志和owned-container实际删除通过。日志 `/tmp/pangrova-production-20261007-model-deployment-nearest-r6.log`。不是原AI SQL九项通过。 |
| 公开SQL部署链r6 | FAIL，2项中1 PASS / 1 FAIL / 0 SKIPPED | finite-quota公开SELECT/零quota拒绝/恢复通过；目录401及结果票据问题已越过，真实Iceberg INSERT 7后SELECT读回null。日志 `/tmp/pangrova-production-20261007-shared-deployment-green-r6.log`。不能缩小为仅SELECT常量的业务验收。 |
| Metadata列表快照最近r5 | PASS，17项 / 0 SKIPPED | 事件与reconciliation报告在构造边界冻结，caller/getter mutation拒绝；cache及reconcile原断言保留。日志 `/tmp/pangrova-production-20261007-metadata-immutable-events-nearest-r5.log`。 |
| Metadata canonical JSON最近r6 | PASS，47项 / 0 SKIPPED | 移除可修改共享ObjectMapper的公开instance，全部metadata/Coordinator caller原子迁移；固定配置/缓存复用/原canonical bytes保持，PG持久化和artifact原合同通过。日志 `/tmp/pangrova-production-20261007-metadata-canonical-json-nearest-r6.log`。 |
| Metadata显式journal owner最近r7 | 行为PASS，42项 / 0 SKIPPED；严格verify FAIL | 实际PG18、owner admission2、dependency16、JSON/immutable6；Checkstyle0，SpotBugs由50降为36。删除无caller的可写store accessor；不改真实SQL与原ACK/COMMIT边界。日志 `/tmp/pangrova-production-20261007-metadata-static-owner-nearest-r7.log`。 |
| Coordinator canonical JSON/Feature/Semantic最近r8 | FAIL，12项中11 PASS / 1 ERROR / 0 SKIPPED | canonical JSON/semantic正反验证通过；Feature materialization实际Paimon split枚举拒绝缺issued query lifetime，仍需完整授权链根因分析。日志 `/tmp/pangrova-production-20261007-coordinator-canonical-json-nearest-r8.log`。 |
| Worker canonical write最近r5 | FAIL，243项中233 PASS / 10 FAIL / 0 SKIPPED | 新canonical alias actual batch/direct/columnar正反测试通过；旧sink失败cleanup oracle仍要求abort失败时close也立即失败，实际仅abort failure，须核真实资源owner/retry语义，禁止弱化断言。日志 `/tmp/pangrova-production-20261007-worker-canonical-write-nearest-r5.log`。 |
| 公开SQL部署链r7 | PASS，2项 / 0 SKIPPED，53.28秒 | 原真实Iceberg CREATE/INSERT 7/SELECT 7L、重复注册及仓库边界、有限配额/零配额拒绝/恢复均通过；同一canonical write alias绑定贯通实际SDK Parquet写入与Native扫描。日志 `/tmp/pangrova-production-20261007-shared-deployment-green-r7.log`。非独立多Worker性能证据。 |
| Coordinator Feature/Semantic最近r9 | PASS，12项 / 0 SKIPPED | Feature fixture改用实际QueryManager公开提交与同一issued query lifetime，原Paimon split/materialize、policy、snapshot和Semantic正反断言不变；不是Native HA E2E。日志 `/tmp/pangrova-production-20261007-coordinator-canonical-json-nearest-r9.log`。 |
| Metadata artifact page所有权最近r8 | PASS，15项 / 0 SKIPPED | 真实PG artifact原13项与nullable byte[]输入/输出快照2项；快照测试不是Native成功报告。日志 `/tmp/pangrova-production-20261007-artifact-page-ownership-nearest-r8.log`。 |
| Metadata canonical dependency最近r9 | 行为PASS，63项 / 0 SKIPPED；严格verify FAIL | 实际PG未知kind六项明确拒绝，既有合法TABLE/EXTERNAL保护语义保留；Checkstyle0，SpotBugs34项未清零，不修改过滤器制造PASS。日志 `/tmp/pangrova-production-20261007-metadata-canonical-dependency-nearest-r9.log`。 |
| 新shuffle owner Release构建/最近r4 | PASS，构建零告警、92项 / 0 SKIPPED，23.52秒 | 实际START lease/close race、exact replay/HMAC Flight数据及EOF、取消/截止时间/唯一join owner等9项新增测试包括在最近组合。日志 `/tmp/pangrova-production-20261007-native-shuffle-owner-{build,nearest}-r4.log`。 |
| 新shuffle owner完整Release CTest r4 | PASS，4052项，568.36秒 | 无筛选全套，新产物SHA256 `921cca2f4230514ef4e66508f4943148f9bc9bf26267569a49ebbd42966e6a97`。日志 `/tmp/pangrova-production-20261007-native-release-ctest-r4.log`。不是完整ASan、JNI或部署验收。 |
| 新shuffle owner ASan构建/最近r4 | PASS，92项 / 0 SKIPPED，47.77秒 | 同源ASan依赖与增量构建、并发/资源专项全部通过；完整ASan仍在运行，不能继承旧cohort结论。日志 `/tmp/pangrova-production-20261007-native-asan-shuffle-owner-{build,nearest}-r4.log`。 |

新Native端口修复已重新打包并Worker clean install，build与resource SHA256同为
`8c7b740b5fcdfc60ef25b01f856326e4cb050512b9edae9515472e7e74db417c`；
无筛选4043项Release CTest已全通过。此前4041项产物
`f2c46...`的PASS仅属于旧cohort。原OwnerDrain四项测试移到Coordinator集成边界，
使用真实认证HTTP、WorkerManager及生产插件发现；所有原清理/失败/并发断言保留，
新增真实注册及撤销观察，r7四项已通过，未用stub补齐缺失Coordinator URL。

该owner四项在r7已实际通过，生产loopback校验没有放宽。后续Native shuffle原子注册/
关闭修改已独立构建并通过完整Release4052项及最近ASan92项；完整ASan与新库Java消费
链尚未验收，不继承上述8c cohort证据。重新打包及Worker编译刷新不等同于行为通过。

公开Iceberg写入null根因已追到真实生产边界：新统一plan实际字段为
`SubstraitRelationLayout.physicalAlias(0)`，Native Values与write descriptor均一致；
`NativeWriteExecutor`和`WritePageProjector`仅识别旧`proj_`，使实际sink columns不含
目标`id`，`AbstractIcebergPageSink`按缺名写null并真实commit。修复以既有统一alias
匹配完整零基有序布局，将同一已验证output handle的logical writeColumnNames绑定为
session事实，后续每批不得改布局；宽度、乱序、混合及失名均拒绝，不补null或加兼容
分支。原CREATE/INSERT/SELECT及7L oracle未修改，r7公开写→读复验已通过。

写入失败清理的独立根因也已确定：`NativeWriteSession.abort` 在实际discard抛错后
跳过close，而旧假sink测试错误要求owner立即归零。修复分别记录真实abort/close完成
阶段，abort失败仍尝试close，保留原异常树；任何未完成阶段保留原handle、catalog lease
与owner，停止append/finish，恢复后仅重试未完成阶段。真实认证部署/SDK Parquet文件
阻断与恢复的75项替代测试已SOURCE_READY，尚未实测；旧10项失败未删除、跳过或
改为错误的owner=0。SDK组件清理测试与公开SQL端到端验收分别记录。

持久化dependency未知kind原来降为EXTERNAL，可能取消TABLE/VIEW的drop保护并误放行
cascade。现对不存在、未知、非canonical枚举明确报METADATA_METASTORE_ERROR，合法
EXTERNAL仍按原契约处理，实际PG篡改行测试验证拒绝且原行不被改写；日志仅使用已
解析的kind，避免原始持久化字段注入换行。未更改事务或schema迁移。

共享metadata mapper的实际问题是公开mutable配置对象：任何caller可改变后续所有
canonical JSON/摘要。现只公开固定序列化/类型读写与新树操作，配置对象和缓存不外泄，
无每调用mapper复制或第二serialization实现；与[Jackson同版本上游说明](https://raw.githubusercontent.com/FasterXML/jackson-databind/jackson-databind-2.18.6/src/main/java/com/fasterxml/jackson/databind/ObjectMapper.java)
关于首次读写后不能更改配置的线程安全约束一致。尚未解决的静态项继续保持FAIL。

本次实际发现并修复的第一根因：`TaskStatusCollector` 发布RUNNING时合法没有
Native终态字节，既有 `CanonicalTaskReportValidator` 仅对终态要求该证据；
`TaskReportPublication.Fact` 却无条件 `digest(null)`。真实Worker经
`RemoteTaskStatusReporter -> CoordinatorTaskResource -> TaskStatusReceiver` 同样
经过此处，不是仅direct fixture问题。修复保留null及所有不可变REST字段，重试须
完整内容一致；终态canonical校验未放宽。新增真实HTTP用例验证原事实ACK、同序列
篡改stats被拒、再重放原事实ACK及实际结果42/EOF/排空。

最近组合继续暴露两条后续根因，未通过前不得关闭验收：

- `TaskJournalStoreFixture` 的观察器也从RUNNING不存在的terminal bytes推导事件ID；
  应从同一实际issued request冻结完整attempt，保持真实PG lock、ack-before-commit
  及0→1 committed观察，不制造Native报告或从不完整REST attempt重建控制authority。
- permit用例全表REVOKE同时影响同一journal flush中的异步RUNNING与task终态，
  尚未到达其query-terminal目标故障边界。生产Worker的monitor又把已成功Native后的
  `onCompleted` 报告异常送入 `onError`，把报告失败误判为执行失败；原FINISHED事实
  尚未完成，新失败事实被实际owner拒绝。须隔离真实PG FINISHED-query fault，并
  分离Native执行/统计捕获失败与后续release/publication失败，保留原exact continuation
  和可观测失败。不能修改FINISHED oracle、吞异常或增加超时求绿。

上述Worker修复已扩展为同一 `TaskTerminalPublication` owner顺序覆盖
EXECUTION_TERMINAL和RELEASED_TERMINAL：Native物理释放可以先发生，但旧实际报告
先收到ACK，才能生成actual-drain的新序列；两个阶段共享最初有限delivery horizon，
不延长、不添加第二registry或假receipt。真实HTTP丢/耗尽ACK新增用例已通过；
查询仍FINISHED、Native无FAILED、完整body/seq/attempt一致，恢复后实际释放与结果42。
permit r4在正确阶段边界已通过：真实PG42501期间cleanup明确抛错，冻结原报告重试，
恢复后同一continuation ACK并生成更晚actual-release报告；physical release与metadata
ACK不能等同，没有把cleanup异常吞掉或将未ACK的报告替换为新事实。

SourceFree关闭用例中旧 `taskId == ticket` oracle与正式签名grant冲突；现从同一
Tracker冻结实际issued request，验证opaque ticket等于同一真实producer signer
发行的exact grant，且不等于taskId，再核CANCELLED Native报告完整attempt一致。
原实际取消、shutdown与重新初始化断言保留，最近3项通过。这仍是单JVM routing
integration，不是独立Worker部署验收。

共享AI启动也发现负向接线证据：新增 `SharedNativeFunctionStartup` 原无caller，
base先GET_CAPABILITIES、Ollama类后注册，违反实际Native显式publication门禁。
现已修改为真实模型准备→唯一Native初始化→同refs注册/finalize→读取精确库存→
Worker注册；Provider仅在Native实际退出后关闭。禁止借用foreign runtime，单个
shutdown-hook，失败半启动保留原异常且后续fail-closed。四文件编译通过；真实
Ollama/Lance/Smoke首轮 **FAIL，4个类级startup ERROR / 0 SKIPPED**，尚未进入
各业务oracle。真实两模型准备、实际factory注册/finalize及精确库存发布已成功，
随后被Shared PostgreSQL readiness阻断。日志
`/tmp/pangrova-production-20261007-shared-ai-lance-e2e-r1.log`。

单独Smoke复现采集了现场线程栈
`/tmp/pangrova-production-20261007-postgres-startup-thread-r2.log`：main在
`SharedTestContainers.<clinit> -> probeJdbcReady -> FutureTask.get` 等待；
`awaitility[shared-postgres-jdbc-ready]` 明确等待同一个Class initialization
monitor。probe的outer静态lambda不能在外部线程完成该类初始化。与此同时实际
owned PostgreSQL容器内 `SELECT 1 AS ready` 返回1，排除把该现场直接归因为数据库
未ready；修复必须移除该class-init依赖，保留实际JDBC与durability检查和原超时，
不能增加超时或跳过探针。现改为独立nested Callable执行真实JDBC，不依赖outer
class-init；最近真实容器2项通过。原E2E r3已越过此边界，但业务链仍失败。

公开E2E r3两条新增根因已定位：`E2eCurrentAuthority.tenants` 创建tenant时
quota=null，而`JdbcTenantStore`准确持久化为八项0，真实QuotaChecker据此拒绝
所有公开SQL。测试部署须显式提供有限quota，不修改生产零配额拒绝语义、不覆盖
既有tenant授权。`TestCatalogRegistrar.ensureDefaultSchema` 在每次fixture
register时无条件create实际已存在的Iceberg默认schema；须在同实际session/catalog
查询存在性，仅缺失时创建，保留真实错误传播。原部署链r5已越过这两处，完整业务
仍被后续opaque票据/实际内部鉴权协议不一致阻断，不能据此前置修复声称E2E通过。

结果票据与控制身份必须分离：Native DoGet传完整producer签名ticket，taskId只从
Coordinator原始issued direct-result TaskRequest及其完整query/task binding选取；
多个候选、空授权或foreign identity必须拒绝，不能解析票据为控制authority或unsigned
fallback。目录部署请求的401已追到生产双方契约：client仅发X-Internal-Auth，
filter仅接受Bearer且缺精确catalog-deployments内部路径，resource二次X-header校验
因此永远到不了。修复原子统一Bearer、精确路径边界及requireInternalServiceIdentity，
真实HTTP正负身份测试必须验证普通认证用户和相似prefix仍拒绝，不能关闭auth。

生产journal还存在独立跨query队头阻塞：`JournalPendingWrites.awaitBatch` 从
全局LinkedHashMap头取事件，`JournalEventSql.commit` 将不同query同事务提交；
保留的失败query每轮占据头部并回滚健康query。完整整改应在同一有界owner内
按query保序、公平分组事务；原事件身份、未知COMMIT、连接关闭、capacity和purge
义务均须保留，以真实PostgreSQL行级拒绝及lostCOMMIT验收，不能增加失败fallback。
该生产隔离修复已实施：同一锁下query-key派生索引、公平轮转、同query保序事务，
没有第二authority registry或失败fallback。真实PG新增三项及原15项均通过；
单query故障不再阻断健康query，同query后续事件不能越过失败头，容量/精确重放/
purge/close/lostCOMMIT原义务保留。严格模块门槛尚有静态失败，继续修复，不能把行为
PASS等同整体验收或把metadata ACK失败计为Native执行失败/物理释放成功。

只读复证还确认实际函数目录的query冻结、Planner消费、TaskRequest field18双端
尚未实现，分布式aggregate phase广告与实际state执行不一致；pinned SDK的TIME(6)
与每列timestamp Arrow桥仍存在精度/布局缺口。完整原子切换及验收规格见
[函数cohort实施规格](ai-native-function-cohort-plan.md)，不是已实现能力。
Coordinator仍生产可达的 `IncrementalReadTask` 与 `TableWarmupExecutor` JVM源扫描
也未迁移完成。统一Calcite公开前端、当前全仓严格回归、全部真实E2E、Linux同源
ASan和独立进程规模/SLO均仍未验收；整体目标保持开放。

### 0.57 2026-10-07 真实 AI 失败链与 SDK 根因继续复证

下列证据属于各自冻结的源码/产物闭包，不表示全部当前改动或生产验收通过。
环境继续为 JDK17/macOS/aarch64、真实 Docker PostgreSQL/Ollama；没有降低测试、
使用 unavailable skip，或修改 ASan leak 过滤规则。

| 检查 | 实际结果 | 日志与边界 |
|---|---|---|
| 新 shuffle owner 完整 ASan r4 | PASS，4052项，1640.86秒 | `/tmp/pangrova-production-20261007-native-asan-full-r4.log`；`-fsanitize=address` 构建，未设置关闭 leak 的测试环境。只证明该冻结 Native cohort。 |
| 新库 Worker/JNI 最近 r5 | PASS，22项 / 0 SKIPPED，15.183秒 | `/tmp/pangrova-production-20261007-worker-shuffle-jni-nearest-r5.log`；原 shuffle/exchange/result-port/library/creation/execution 六套。不是独立进程公开SQL或SLO。 |
| 全39模块刷新 r7 | 编译PASS，54.181秒；行为SKIPPED | `/tmp/pangrova-production-20261007-current-full-java-refresh-r7.log`；不包含本节后续 trace、嵌套schema与结果取消整改。 |
| Common canonical摘要最近 | PASS，16项 / 0 SKIPPED；Checkstyle/SpotBugs0 | `/tmp/pangrova-production-20261007-common-digest-nearest-r1.log`；包括新增两项各100次properties，非SQL E2E。 |
| Metadata摘要消费最近 | PASS，29项 / 0 SKIPPED | `/tmp/pangrova-production-20261007-metadata-digest-nearest-r1.log`；实际PG stage exchange14项及原canonical合同。不能替代全模块静态门槛。 |
| 公开 AI/Lance/Smoke r8 | FAIL，18项：6 PASS / 6 FAIL / 6 ERROR / 0 SKIPPED；fork最终134 | `/tmp/pangrova-production-20261007-ai-lance-smoke-real-r8.log`，运行3分45秒。AI trace缺失、嵌套DDL拒绝、failed reader保留和原生析构abort均须分别解决。 |
| 新真实写入清理 r1/r2 | FAIL，testCompile失败，未进入行为断言 | `/tmp/pangrova-production-20261007-write-cleanup-real-nearest-{r1,r2}.log`；错误使用不属于当前base的helper及Record泛型，已修编译，不计通过。 |
| 写入清理 r3 | FAIL，5项：3 FAIL / 2 ERROR / 0 SKIPPED | `/tmp/pangrova-production-20261007-write-cleanup-real-nearest-r3.log`；实际SDK单行默认不会立刻创建文件，fixture低阈值也因生产未消费表配置而无效。 |
| Iceberg writer配置及原相关回归 | PASS，25项 / 0 SKIPPED，8.514秒 | `/tmp/pangrova-production-20261007-iceberg-parquet-config-r1.log`；新增8项真实SDK分区/非分区/GZIP/metrics/自然flush/abort/default lazy及非法参数拒绝。 |
| 写入清理 r4 | FAIL，5 FAIL / 0 SKIPPED | `/tmp/pangrova-production-20261007-write-cleanup-real-nearest-r4.log`；新断言误要求finish拒绝文案含stopped，生产实际精确文案为cannot finish after failure or abort；仅校准新断言，未改生产拒绝语义。 |
| 写入清理 r5 | FAIL，5项：3 PASS / 2 ERROR / 0 SKIPPED | `/tmp/pangrova-production-20261007-write-cleanup-real-nearest-r5.log`；真实decoder异常与文件阻断/恢复2项、global close/lease/恢复1项通过；Decimal失败2项完整读取owned文件失败，严格7、1断言未删除。 |
| execution trace adapter最近 | PASS，4项 / 0 SKIPPED，2.957秒 | `/tmp/pangrova-production-20261007-execution-trace-context-r1.log`；实际SDK sampled/unsampled span、真实线程交接、W3C propagator验证。不是AI SQL闭环。 |
| execution trace adapter r2 | PASS，4项 / 0 SKIPPED，8.213秒；Checkstyle/SpotBugs0 | `/tmp/pangrova-production-20261007-execution-trace-context-r2.log`；补齐traceState访问器Javadoc后独立叶模块复验，不代表Coordinator新改动、公开AI SQL或原生结果链验收。 |
| Coordinator trace首轮 | FAIL，6项：5 PASS / 1 ERROR / 0 SKIPPED | `/tmp/pangrova-production-20261007-coordinator-trace-request-r1.log`；新增callback断言错误使用尚未stamp execution-owner的factory请求，不放宽admission；改在真实已issued请求链核验。 |
| Coordinator trace r2/r3 | PASS，分别15/20项，0 SKIPPED | `/tmp/pangrova-production-20261007-coordinator-trace-request-{r2,r3}.log`；r3含真实PG issued artifact deadline链5项，callback投影现有统一trace字段。后续MDC恢复改动须另验，不能沿用此结果。 |
| Coordinator trace r4 | FAIL，编译失败，行为未运行，9.703秒 | `/tmp/pangrova-production-20261007-coordinator-trace-request-r4.log`；当前StagePlanAccess调用新增resultColumnTypes但已安装上游尚无此方法，需刷新完整依赖闭包。 |
| Worker统一trace r2 | PASS，12项 / 0 SKIPPED，13.975秒 | `/tmp/pangrova-production-20261007-worker-canonical-trace-r2.log`；full-regression选择实际JNI creation、trace、loader与执行链。daily选择r1仅6项，不把未选中的Native4项算通过；非公开AI SQL。 |
| Iceberg规范新表field IDs | PASS，16项 / 0 SKIPPED，9.812秒 | `/tmp/pangrova-production-20261007-iceberg-canonical-schema-r1.log`；真实Hadoop/JDBC与原nano-schema回归，不等于ASYNC embedding公开链已通过。 |
| Metadata同事务projection/outbox最近 | PASS，77项 / 0 SKIPPED，约60秒 | `/tmp/pangrova-production-20261007-metadata-atomic-invalidation-r1.log`；包括真实PG提交/回滚/丢失COMMIT ACK及sync source/sink组件，既有组件测试不冒称湖仓公开E2E。 |
| Metadata当前完整门槛 r10/r11 | FAIL；r10行为537项全PASS/0 SKIPPED，但Checkstyle1；Javadoc修正后r11 Checkstyle0、SpotBugs27 | `/tmp/pangrova-production-20261007-metadata-current-{full-gates-r10,static-r11}.log`；r11行为SKIPPED，仅静态复验。未修改过滤规则求绿；整体模块门槛未通过。 |
| Metadata集合所有权最近 | PASS，7项 / 0 SKIPPED，16.783秒 | `/tmp/pangrova-production-20261007-metadata-immutable-ownership-r1.log`；实际值对象覆盖11个不可变集合访问器、嵌套冻结、输入变更隔离及canonical字节/摘要稳定性；仅叶模块test，不包含完整门槛。 |
| Metadata不可变集合精确分类验证 | PASS；11条规则隔离及完整过滤器正例、12类负控制 | `/tmp/pangrova-production-20261007-metadata-immutable-filter-r1.log`；使用实际SpotBugs Filter API验证Class/Method/params/returns/Field/type/Bug精确匹配，不扩大为package/record过滤；其余16条原告警未隐藏，当前完整静态门槛须另验。 |
| Metadata静态门槛 r12 | FAIL，35.252秒；Checkstyle0、SpotBugs16；行为SKIPPED | `/tmp/pangrova-production-20261007-metadata-static-r12.log`；独立叶模块verify未与其他构建共享该模块target。精确不可变分类仅消除已验证11条，其余借用依赖14条、公开graph hash比较1条、drift采样1条仍原样报告，完整门槛未通过。 |
| SDK 1.11五源最小回补隔离矩阵 | PASS，18个standalone场景；不是JUnit全量或生产E2E | `/tmp/pangrova-iceberg-111-repair.bDH9UC/acceptance-{rows,deletes-ordered,io,delete-io,footer}.log`；分别4/10/1/1/2场景。同一最终候选JAR验证实际完整读、失败后永久拒绝文件元数据、真实OS I/O及FD0，生产依赖未切换。 |
| SDK 1.11源码/产物闭包只读复核 | PASS，376个运行JAR内容hash；601/59/152个模块源码、class集合对照 | `/tmp/pangrova-production-20261007-sdk-{runtime-manifest,source-closure}-r1.log`；core/parquet无class差异；Hadoop仅3个已声明annotation-free package-info差异。完整javac记录可追溯，但缺官方shade/资源/可重复发布构建，不能判为发布级PASS。 |
| Native reader新闭包构建 r5/r6 | FAIL，均未进入CTest | `/tmp/pangrova-production-20261007-native-result-reader-build-{r5,r6}.log`；r5测试误用Folly私有SemiFuture.wait，已保留原2秒及断言改用实际public Future API；r6出现新增VECTOR契约的signedness WERROR。完整源码闭包待协调，未回退未知改动、未打包失败产物。 |
| 全39模块刷新 r8 | FAIL，1分56秒；行为未运行 | `/tmp/pangrova-production-20261007-current-full-java-refresh-r8.log`；Paimon编译读SPI target JAR报invalid stored block lengths/block type，不能把后续missing symbol当源码根因。只读检查确认同路径JAR在本轮中被另一Maven重写，17:37:49产物随后unzip-t正常；并发构建尚未协调，不盲目清target或回退源码。 |
| Native reader新闭包 r7 | BLOCKED，Root构建已中断，exit130；CTest未运行 | `/tmp/pangrova-production-20261007-native-result-reader-build-r7.log`；Root仅将已验证范围内dimension转为int32_t以修WERROR，不改变VECTOR语义。另一个CMake正在写同一build目录及目标，已停止本轮自己的进程，未停止其他任务、未打包冲突产物。 |

**真实 trace 缺口。** `TaskRequestFactory.canonicalRequest` 从未写统一 v2 field11；
`CanonicalTaskRequests.callbackFor` 与 Native parser 已正确投影该字段，但收到空值，
`AiExecutionContext.requireGovernedIdentity` 因此明确拒绝。不能删除审计身份校验、
把 query UUID 冒充 trace，或从异步 dispatch 的空 ThreadLocal 读取零值求绿。
当前整改将不可变执行上下文随 session/live/deferred 控制对象显式携带，在唯一 task
builder 写现有字段，Worker JNI 也读取同一请求事实。无 ambient parent 时，观测adapter
使用实际 OTel SDK IdGenerator 建合法 unsampled local root，并用官方 `Span.wrap`
激活非recording context；这仅建立执行相关性，不声明 exporter、recorded span 或费用账本
开启。依据 [OTel API](https://opentelemetry.io/docs/specs/otel/trace/api/) 及
[SDK sampling规范](https://opentelemetry.io/docs/specs/otel/trace/sdk/)，未引入私有协议。
恢复后的 durable attempt 新相关性不等于原 job trace 已持久化，该范围仍须单独验收。

**真实嵌套 CREATE 缺口。** r8 真实 `v1.metadata.json` 的 VECTOR element-id为4，
输入builder为3000；同版SDK新表必定assignFreshIds，而output factory比较原schema JSON，
把SDK合法规范化误判为并发schema漂移。整改在创建前使用官方
`TypeUtil.assignIncreasingFreshIds`，同一规范schema用于spec/sort/properties/create/handle；
保留原exactJSONfence，真实并发alter仍必须拒绝。新增实际Hadoop/JDBC四项及原相关回归共16项已通过；公开ASYNC与完整AI链仍待新Native产物复验。

**真实失败 reader 缺口。** Velox task terminate/removeTask删除route，但producer
termination不唤醒destination consumer；其后deleteResults已找不到route。reader自己持有
promise，不能依赖broken future，且DoGet丢弃实际ServerCallContext取消。结果是failed
reader等待deadline、真实release拒绝、FAILED交付滞留。服务stop先Shutdown再revoke又
等待reader；现场SIGABRT第一faultThread为DoGet worker中的`ResultBatchReader`析构，
不是只凭日志猜为global task析构。必须显式唤醒真实pending read、传播实际取消，保持
error非EOF，捕获实际buffer shared owner并在Shutdown前seal/revoke/wake；原无真实drain
不归还预算的门禁不变。该Native闭包源码已完成，首次构建因实际Folly API不匹配失败；
精确修正后再次构建被当前工作区新增VECTOR契约的WERROR阻断。没有成功的新闭包
Native测试、打包或公开E2E结果，旧4052项PASS不覆盖新源码。
后续只读进程/文件证据确认另一Maven输出到
`/tmp/pangrova-ai-native-cli-verify-20261007.log`，另一CMake输出到
`/tmp/pangrova-ai-native-native-build-20261007.log`，两者使用同工作区target/build。
这不是缺JDK/Docker，也不是通过重试即可可靠验收；须单一构建owner、冻结完整源码闭包，
再刷新上游、Native、classpath资源及JNI消费者。用户未提交的并发源码改动全部保留。

**SDK Decimal失败原子性尚未解决。** 与Pangrova清理完全独立的同版SDK最小复现保留在
`/tmp/pangrova-parquet-sdk-repro.ezvDW8`：Iceberg1.11.0 / Parquet1.17.1写7、1后拒绝
7.01，close成功但footer第三块rows1/valueCount0，完整读取同样报past page end。
仅诊断程序退出0，读验证为FAIL。SDK的计数先于model.write/endRecord，当前官方main
仍保留该顺序，不能声称升级自然解决。异常后未提交文件不对SQL发布，删除失败保留
owner并重试，但这不证明文件可完整读取或多字段失败行原子。用户已允许完整验收前提下
评估独立SDK补丁；尚未切换生产依赖。不使用反射改SDK状态、不伪造SDK异常、不删除7、1 oracle。

追加独立SDK评估发现：仅把计数移至model.write之后，单列能读不等于多字段行原子；
默认buffer的晚字段失败仍产生marker列3值、id列2值的非法cohort。丢弃污染的当前group
只能保留此前已封闭的group，不能承诺保留未flush的成功行。实际路径被非空目录阻断时，
SDK首次close失败而第二次close返回成功、文件仍不存在；ParquetFileWriter.abort后close
实际文件描述符仍为1，重试亦未释放。完整诊断保留在
`/tmp/pangrova-iceberg-sdk-evaluation.msmmAm`，这些均为FAIL证据，不是已部署修复。
需同时处理ParquetWriter失败/释放状态、DataWriter永久拒绝失败attempt发布、
ParquetFileWriter的abort及实际stream关闭，并以完整独立版本SDK验收；禁止class影子覆盖。
[官方最新发布1.12.0](https://iceberg.apache.org/releases/)的
[固定tag写入源码](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/parquet/src/main/java/org/apache/iceberg/parquet/ParquetWriter.java)
仍在model.write前计数、flush前标记closed，升级本身不作为此问题通过证据。

**SDK升级兼容性必须独立验收。** 已隔离全部1.11 JAR并用完整官方1.12依赖重现上述
写入问题，不以版本更新代替根因修复。1.12的
[固定tag DeleteWriteBuilder](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java)
在buildPositionWriter拒绝非空rowSchema；当前主源码IcebergDeleteWriter通过forTable(table)
设置该schema并写emptyRow，直接切换将导致该组件的position-delete构建失败。全仓调用
追踪仅发现测试直接使用这些helper，尚未证明公开SQL删除接线；不将组件兼容性风险
冒称已发生的生产SQL事故。当前优先评估
1.11最小可追溯回补，保留1.12作为隔离候选，未更改生产POM或替换本机依赖缓存。
隔离完整源码候选已初步证明sealed group可完整读取、失败DataWriter不发布以及真实
abort后FD归零；1.11最小回补进一步完成数据/delete永久拒绝发布、实际I/O及构造/footer
故障共18项独立实测，但这仅属于指定临时classpath矩阵。正式上游构建/依赖shade、
完整SDK回归和原生产写入链仍需验收。Iceberg ParquetWriter构造器在model/store创建前
取得compressor的失败释放边界未验收，不能将ParquetFileWriter的encryption构造FD测试
泛化至整个SDK。当前主源码delete helpers缺少write异常后的finally/try-with-resources，
矩阵中的显式close不能证明该caller退出安全；helper接线及owner清理仍为OPEN。
临时编译JAR不等于可发布SDK，不允许影子类覆盖。
五份补丁、官方输入与候选SHA、精确编译/复证命令、失败资源与正式门槛详见
[SDK写失败整改与验收规格](iceberg-sdk-write-failure-repair-plan.md)。当前仅为SOURCE_READY，
不作为已完成生产依赖升级。必要SDK升级/修复已获用户授权，不把未完成的技术验收
虚构为还需新增本地升级审批；对外发布SDK和改动其他活动任务不在当前授权范围。

**可空VECTOR坐标客户端缺口。** 当前Native合法双层nullability的正向用例明确接受
`[1.0, NULL, 3.0]`，而ArrowBatchDecoder仍只构造float[]/double[]并拒绝NULL坐标，
JDBC VectorSqlArray同样只支持基本类型数组。需要required-child保留primitive fast path、
nullable-child保留Float[]/Double[]坐标空值，并同时贯通SPI、结果脱离batch所有权、CLI、
JDBC元数据及Array生命周期；不能以NULL→0或Float64降精度规避。AI严格Float32/
非空坐标契约保持不变。该缺口仅完成只读复证，相关源码仍由同工作区另一活动任务
修改；未声称已修复或获得多阶段/独立进程验收。

另外已复证metadata projection与独立invalidation写入不原子，生产sync没有writer接线且
异常被吞掉；当前只授权同PG事务projection+outbox闭包。所有实际缓存消费者、关闭join
以及外部湖仓DDL的durable intent/pending fence另须开发验收，不能仅传poller参数宣称分布式
闭环。全仓严格回归、公开全部业务链、同源Linux以及生产规模/SLO仍未验收。

### 0.58 2026-10-07 构建归属、旧 ASan 产物与可空 VECTOR 验收

用户已明确授权协调共享工作区构建。另一活动任务停止了新共享构建，并将后续开发
迁至独立 worktree/依赖缓存；本工作区由 Root 统一运行 Maven、Native、打包和回归。
现有未提交源码、暂存删除及失败日志均保留，不使用 reset、stash 或协议影子覆盖。

**旧 ASan 产物的实际根因。** 原完整 CTest 在四个新 reader 用例的 TearDown 卡住。
只读 LLDB 栈显示 `releaseTaskAndWaitForDeletion → deletion_future.wait`；运行二进制的
TearDown 反汇编在 `cancel` 后直接调用删除等待，缺少当前源码已有的
`runner_pause_owned_` 分支和 `Task::resume`。证据为
`/tmp/pangrova-production-20261007-reader-asan-lldb-layout-r2.log`，调试器已 detach。
源码修改时间17:53:42、对象完成时间17:53:48不能证明编译器读取的是修改后内容；
编译期间修改源码导致 mtime 增量检查漏掉旧输入。本轮旧 CTest 已由其 owner 中止，
不折算完整 PASS，也不修改生产取消逻辑或放宽超时。
旧对象 SHA256 `beb5b2a60192c9954b772e54f55a052c9c0b148786b719b37b9325e187f98ee5`
已可恢复隔离至 `/tmp/pangrova-native-stale-object.74JYWc/ResultBatchStreamReaderTest.cpp.o`。
新 Native 输入清单为 `/tmp/pangrova-production-20261007-native-input-r1.sha256`；
`cmake --build pangrova-worker-native/build-asan --clean-first --parallel 2` 已 exit0，
日志 `/tmp/pangrova-production-20261007-native-asan-build-r1.log`；615项输入重新核验通过。
Reader最近12项全部通过，但随后十次重复选择在Flight shutdown用例失败，不能计稳定通过。
实际gRPC trace显示应用层取消状态已发送，而无deadline的Shutdown仍等待idle client的
GOAWAY/PING回复约5秒。证据为
`/tmp/pangrova-production-20261007-reader-asan-shutdown-grpc-trace-r7.log`；这是独立于旧对象的真实缺陷。
当前修复在权限撤销后调用实际Arrow deadline shutdown取消transport，同时仍join真实handler，
新增不再Next/Cancel的idle client用例；不放宽2秒断言或制造排空receipt。
`/tmp/pangrova-production-20261007-native-asan-shutdown-build-r2.log` 已 exit0；最近23项
通过（19.06秒），五项并发相关用例各重复25次，共125次通过（46.63秒）。日志分别为
`/tmp/pangrova-production-20261007-reader-asan-shutdown-green-r8.log` 和
`/tmp/pangrova-production-20261007-reader-asan-shutdown-repeat-green-r9.log`。
这不代表完整Release、打包JNI或公开SQL链已验收。当前无筛选ASan发现4069项，仍在运行；
两个Lance重复失败用例已报告各256字节泄漏，完整回归不能计PASS。独立、不链接
Pangrova/Velox/Arrow/Rust的同工具链程序复现同一分配；LLDB定位到Apple libc++
`runtime_error`兼容性探针的`dlopen`失败路径及dyld线程局部错误字符串。1/32/4096次
copy均只有一个256字节分配，自然退出并join的线程不保留该分配。此证据区分系统TLS
与每任务累积泄漏，但不是消除报告或通过验收的理由；不修改生产throw、预热线程或新增
suppression求绿。需匹配的生产平台/工具链完整重验，当前结果保持FAIL。
独立报告为`/tmp/pangrova-runtime-error-asan.riwhp8/root-cause.md`，SHA256
`d2241d5e04dec2b6cd21ee1547278eecd21385fb449304b8538cfef7087a5ffd`；
全部原始诊断输出`diagnostic-runs.log`的SHA256为
`c3ea04ebd3e64c28418117f9bd37e601e720bd2eb7a3f8621b586c743da23d6d`。
当前Docker是Linux arm64基础/服务镜像，没有匹配的完整instrumented Linux依赖和
Pangrova构建产物；不能将Docker可连接记成Linux生产ASan已经通过。
完整运行继续暴露额外失败：截至第753项，已有六个256字节LSan报告，另有
`SubstraitTimestampValidationTest.RejectsInvalidPrecision`在异常展开的
`libunwind::CFI_Parser::decodeFDE`发生SEGV。该崩溃与TLS报告分开定位；系统库栈位置
不是根因结论，更不能据此排除测试。原始崩溃栈保留于同一r10完整日志。
进一步现场复证：ASan测试二进制`__eh_frame`约31.9MB，原TestBody的真实FDE
偏移`0x1189db0`被compact-unwind的24位字段截成`0x189db0`，此处没有有效FDE；
实际LLDB cursor因此将NULL传给decodeFDE。ASan shared dylib同样超过16MiB并有
截断记录；当前旧Release的EH段未超阈值。单独`-no_compact_unwind`不是已验方案：
隔离诊断的真实throw/catch因此abort，说明还必须验证完整DWARF/LSDA保留。
链接修复仅在独立临时副本验证，当前生产/测试CMake输入与原完整cohort仍冻结。

**可空 VECTOR Java 闭包。** ColumnDescriptor 保留既有统一 LogicalType，而非另立
VECTOR 协议；结果 decoder 由完整 schema 声明选择 float[]/double[] 或 Float[]/Double[]，
nullable child 即使当批全非空仍保持 boxed 载体。required parent/child 拒绝真实 NULL，
维度和两种精度消费统一声明。SPI、脱离 batch 的快照、CLI、JDBC 元数据及 Array
切片/free 已同批迁移，无旧构造器 shim。AI 的严格 Float32/非空坐标契约未放宽。

| 检查 | 实际结果 | 命令、日志与边界 |
|---|---|---|
| 可空 VECTOR 最近 Java reactor | PASS，67项 / 0 FAIL / 0 ERROR / 0 SKIPPED，15.198秒 | JDK17，`mvn -B -pl pangrova-spi,pangrova-transport,pangrova-client/pangrova-client-cli,pangrova-client/pangrova-client-jdbc -am -Dtest=ArrowBatchDecoderVectorTest,ResultBatchOwnershipTest,ColumnValueVectorTest,ResultBatchDecoderTest,PangrovaResultSetVectorTest -Dsurefire.failIfNoSpecifiedTests=false test`；`/tmp/pangrova-production-20261007-vector-nullable-nearest-r1.log`。实际五类分别27/6/9/14/11项，真实 Arrow IPC 到客户端组件，不是 public SQL 或分布式 E2E。 |
| VECTOR/客户端完整及静态门槛 | PASS，15模块，2550项 / 0 FAIL / 0 ERROR / 0 SKIPPED，4分08秒；Checkstyle/SpotBugs0 | `mvn -B -pl pangrova-spi,pangrova-transport,pangrova-client/pangrova-client-cli,pangrova-client/pangrova-client-jdbc -am -Pfull-regression,coverage,static-analysis verify`；`/tmp/pangrova-production-20261007-vector-clients-full-static-r1.log`。包含真实PG及TLS等组件回归，不作为已部署 Native/public AI SQL 或独立进程分布式验收。 |
| E2E真实性静态门禁 | PASS；不是行为回归 | `.pangrova/scripts/check-e2e-truthfulness.sh`；`/tmp/pangrova-production-20261007-e2e-truthfulness-r1.log`。 |
| Metadata新借用/drift/graph最近 r1 | PASS仅27项；新borrow7/drift4未选中 | `/tmp/pangrova-production-20261007-metadata-owner-decisions-nearest-r1.log`；daily profile过滤新integration类，不将未运行11项记为PASS。 |
| Metadata新闭包真实选择 r2 | FAIL，38项：34 PASS / 1 FAIL / 3 ERROR / 0 SKIPPED，2分51秒 | 使用`-Pfull-regression`及六类selector，`/tmp/pangrova-production-20261007-metadata-owner-decisions-nearest-r2.log`；新增fixture的sink/peer未建schema父记录被实际PG23503拒绝；pool关闭断言错误要求参数化SQL异常包含bound值。仅修fixture建数及精确实际异常断言，不改PG外键、生产异常或关闭拒绝语义；修后结果另列r3，不改写首轮FAIL。 |
| Metadata新闭包真实选择 r3 | PASS，38项 / 0 FAIL / 0 ERROR / 0 SKIPPED，2分58秒 | `/tmp/pangrova-production-20261007-metadata-owner-decisions-nearest-r3.log`；六类分别15/7/7/3/2/4项，新增真实PG借用7项和deterministic drift4项已实际运行。保留原10%采样概率，直接复证同一生产drift detector及实际42501事务拒绝；不是所有consumer、shutdown join或公开SQL闭环。 |
| Metadata精确分类脚本 | PASS；行为证明须独立运行 | JDK17，`python3 .pangrova/scripts/tests/spotbugs-immutable-filter-test.py`；`/tmp/pangrova-production-20261007-metadata-owner-filter-r1.log`。实际pinned Filter API核11不可变getter/10借用constructor/公开graph identity及非安全采样，isolated/full规则和13类边界负控；未验证Default dispatcher及3个未接通constructor仍可见。不是完整静态门槛通过。 |
| 统一声明owner与SDK dispatcher最近 r3 | PASS，29模块选择27项 / 0 FAIL / 0 ERROR / 0 SKIPPED，1分18秒 | `mvn -B -pl pangrova-connector/pangrova-connector-paimon,pangrova-coordinator -am -Pfull-regression -Dtest=DeclaredTypeTest,DeclaredLogicalTypesTest,PaimonHandleSchemaIntegrationTest,MetadataSharedDispatcherSdkIntegrationTest,PlanningReadBindingsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`；`/tmp/pangrova-production-20261007-declared-owner-dispatcher-nearest-r3.log`。五类分别3/4/11/1/8项。原nested declaration移至中立SPI，唯一projector及全caller迁移，无新VECTOR协议或兼容别名；仍是既有physical-write支持域，非source nullability/public SQL验收。SDK bus前轮FAIL为独立fixture Paimon默认缓存命中旧Table；仅刷新该真实读取oracle再验证物理absence，两个Pangrova cache断言不变。该1项证明provided dispatcher身份，不等于生产bootstrap/peer SDK cache闭环。 |
| 扩大声明/Metadata完整及静态回归 r1 | FAIL；Governance 115项行为PASS、0 SKIPPED，但SpotBugs 11项失败 | `/tmp/pangrova-production-20261007-declared-metadata-full-static-r1.log`。当前SPI/Protocol通过行为和静态检查；Governance阻断后续Metadata/Paimon完整验证，不能计整条链PASS。实际生产quality store的Statement未关闭，且SQL仍为PostgreSQL不支持的`MERGE ... KEY`；读/初始化/结果写错误还会被吞掉，正按真实调用链修复。其余DI身份提示需实际借用证据与精确边界分类，不扩大排除规则。 |
| SplitFetcher中断边界最近 r1 | PASS，28项 / 0 FAIL / 0 ERROR / 0 SKIPPED，2分34秒 | `mvn -Pfull-regression -pl pangrova-coordinator -am -Dtest=SplitFetcherInterruptionTest,SplitFetcherSdkIntegrationTest,SplitFetchFailFastPropertyTest,PlanningReadBindingsIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false test`；`/tmp/pangrova-production-20261007-split-interruption-nearest-r1.log`。四类分别10/2/8/8项；实际PG/plugin/Paimon source已执行。中断不再break后返回部分splits；保留interrupt、原始失败及真实close异常，source只关闭一次。不是分布式/public SQL E2E，也不证明selected-provider租约与catalog retirement已闭合。 |
| Default dispatcher精确分类 r2 | PASS；不作为完整SpotBugs通过 | `/tmp/pangrova-production-20261007-metadata-dispatcher-filter-r2.log`。依据前述实际SDK bus的provided identity证据，仅新增原6参数storing constructor及typed field的EI_EXPOSE_REP2分类；11项借用constructor及13类边界负控通过，4参数forwarding constructor、3个未接通组件及敏感bytes/token仍不匹配。 |
| Lineage构造及近测 r1 | PASS，14项 / 0 FAIL / 0 ERROR / 0 SKIPPED，1分09秒 | `/tmp/pangrova-production-20261007-lineage-construction-nearest-r1.log`。全仓无派生类，持有consumer executor的具体实现改为final，消除失败构造器被finalizer子类捕获的入口；不删除参数校验或扩大静态排除。构造边界4项、原shutdown7项、原query listener3项执行；不证明durable lineage或多Coordinator治理已闭合。 |
| Governance PG及bootstrap最近 r1 | FAIL，30项：28 PASS / 0 FAIL / 2 ERROR / 0 SKIPPED | `/tmp/pangrova-production-20261007-governance-pg-nearest-r1.log`。新增实际PG规则/结果及精确3F000/42P01/23505验证3项通过；Lineage构造4项通过；Coordinator store失败前不创建未被context拥有的consumer验证1项通过。原bootstrap22项有两项旧fixture缺internal-service authority，被真实HTTP安全门禁拒绝；仅修真实授权fixture，保留门禁和原断言，修后r2另列。新增PG write gate验证无规则允许、实际读失败必须拒绝，不宣称质量SQL已实际执行。 |
| Governance PG及bootstrap最近 r2 | PASS，31项 / 0 FAIL / 0 ERROR / 0 SKIPPED，2分20秒 | `/tmp/pangrova-production-20261007-governance-pg-nearest-r2.log`。PG store及真实read-failure write gate4项、构造4项、bootstrap22项、失败初始化1项全部执行。两旧fixture改为原Signed OIDC及持久authority helper，不改变生产鉴权、原断言或拒绝语义。扩大Governance行为/coverage/static回归继续执行。 |
| SDK正式Parquet最近 r1 | PASS，61项 / 0 SKIPPED；ABI/RAT/Spotless/SBOM通过 | `/tmp/pangrova-sdk-release-candidate.OC3Ids/logs/parquet-nearest-r1.log`。实际官方工具链8模块，44 unit与17 Failsafe；exact官方1.17.1的japicmp无API变化。不是完整SDK或Pangrova生产E2E。 |
| SDK正式Parquet完整 r1 | FAIL，1589项：1564 PASS / 1 FAIL / 24 SKIPPED / 0 ERROR，14分50秒 | `/tmp/pangrova-sdk-release-candidate.OC3Ids/logs/parquet-full-cohort-r1.log`。Hadoop模块650项中625 PASS/1 FAIL/24 SKIPPED；失败为原`testNoFlushAfterException`：真实close释放FD时底层流flush先前MAGIC，原路径靠未关闭流维持len0。不能恢复泄漏或修改oracle求绿；OutputFile/PositionOutputStream缺通用delete/discard契约，需实际owner根因方案。默认Hadoop3.3.0的24个bridge用例因缺相应FS API SKIPPED，后续需实际3.4.3 runtime cohort复验；六个后续模块被阻断，Iceberg正式constructor及最终shaded矩阵未运行。生产依赖未切换。 |
| SDK 正式构建准备 | SOURCE_PLAN_READY；发布验收未通过 | 独立方案 `/tmp/pangrova-sdk-release-audit.00dX3p/release-preparation-plan.md`，SHA256 `ba7d1f2aa725d2fe7f2b7bfec1c7bb7c90696ce81b8cb2e2bbb56e9ccc242600`。官方源校验和通过，签名、Thrift0.22、官方 Immutables2.12.1、shading/资源/ABI/完整回归仍待执行。已授权独立目录/专用 cache 构建，不切换生产依赖或对外发布。 |

ConnectorColumnTypeMapper 当前 storage VECTOR 投影 child REQUIRED，nullable-child 的
公开声明/source 入口尚待统一契约复证；不能用 ARRAY 或手工协议作为公开 SQL 通过证据。
Metadata 的真实消费者/cache fill fencing、SDK 构造/释放闭包、函数能力冻结、全部公开
SQL/JNI/Smoke、同源 Linux 和生产规模/SLO 仍未完成，本节不改变整体 OPEN 状态。
Governance仍有实质生产wiring缺口：bootstrap创建的auto-mask policy写入独立内存
mask store，而实际query enforcer使用另一持久store；`ColumnTagService`仅测试直接消费，
生产metadata没有注入其事件链。`DefaultQualityRuleExecutor`在生产assembly仍只检查
expression非空，不读取实际表数据；`GovernanceQueryRunner`/SQL executor及write gate
未在生产闭合。当前PG持久化/异常修复不解决这些缺口，必须先统一实际policy authority、
native查询入口和tenant/snapshot边界，不能以扩大EI排除或手工结果模拟作为验收。

### 0.59 2026-10-07 当前环境缩比验收与治理失败复证

用户要求在当前环境等比例模拟。验收缩小数据、并发、线程和内存预算，不省略实际
进程边界、跨Worker数据交换、权限、失败、重试、重启及排空。目标为独立Coordinator
和至少两个独立Worker JVM，加载同源新Release JNI，通过真实公共SQL和结果传输验证；
现有单JVM组件链不能计为独立进程分布式通过。Docker的2 CPU/4GiB用于真实依赖服务，
macOS独立进程结果只证明该平台缩比功能，不能外推Linux ASan、规模吞吐或延迟SLO。
过程harness及矩阵尚待落地，当前不计PASS。

扩大Governance回归r2实际失败：123项，122 PASS / 1 FAIL / 0 SKIPPED，日志
`/tmp/pangrova-production-20261007-governance-full-static-r2.log`。失败为旧
`LineageQueryEventListenerTest.queryCreatedDoesNotEmitLineage`：生产listener实际发送
START事件，旧测试sleep200ms后断言零事件，而consumer最长500ms轮询会偶然掩盖该
错误。修正为真实latch等待，断言START、runId、SQL及空输入/输出，并使用真实关闭；
不删除START或放宽生产语义。最近r3已通过18项、0 SKIPPED、45.048秒，包括PG4项、
构造4项、关闭7项和listener3项，日志
`/tmp/pangrova-production-20261007-governance-listener-nearest-r3.log`。
完整行为/静态重验仍未通过，不能将未到达的Governance SpotBugs记成通过。

列标签SQL另有真实假成功：SET/UNSET原分支仅DEBUG后return，然后invalidate/event并
返回成功，未调用持久标签authority，也未接通实际查询的mask authority。当前最小
正确性修复拒绝此请求且先于connector/cache/event副作用；真实PG/Paimon/parsing及
query authority最近r4已通过28项、0 SKIPPED、2分11秒，日志
`/tmp/pangrova-production-20261007-governance-tag-nearest-r4.log`。
随后将拒绝收敛为既有`PARSER_UNSUPPORTED_FEATURE`结构化错误，不新增UOE stub；
当前HTTP mapper对该错误仍映射500，不能宣称4xx已验收。明确拒绝不等于完成标签能力。后续必须统一
tenant-scoped持久标签和实际Security policy authority，原子派生或durable outbox/
reconcile，验证重复SET、UNSET、跨Coordinator、重启及存储失败的fail-closed语义。

质量SQL的三值逻辑也经真实PG复现：四行amount为1/-1/0/NULL时，原
`NOT(amount > 0)`只计2个失败，遗漏UNKNOWN。原契约明确只有TRUE通过；新计数和
sample统一使用`IS NOT TRUE`，复用现有Planner统一truth predicate投影，不新增协议。
复现r1为FAIL（1项，expected3/actual2，36.255秒），日志
`/tmp/pangrova-production-20261007-quality-null-repro-r1.log`；修后最近r2共18项全部
通过、0 SKIPPED、1分45秒，日志
`/tmp/pangrova-production-20261007-quality-null-tag-nearest-r2.log`。包含真实PG检查1项、
SQL构造/处理7项、原ALTER9项和结构化列标签拒绝SDK链1项，真实PG同时验证NULL失败
计数及样本。该PG组件验证不是Native/public quality SQL或生产wiring通过。

SDK正式r2最近编译失败（0行为测试）：重赋的blockSize被lambda捕获，不满足Java的
effectively-final约束。该日志保留于独立候选目录，不折算行为失败或成功；下一cohort
只复制已计算值再获取真实流。owned-header方案只延迟实际4字节PAR1/PARE，早期abort
丢弃尚未交付的自有字节并真实关闭，不能回滚已交付body。原空文件oracle、实际FD零、
完整官方reactor及最终shaded真实失败矩阵必须同时通过后才可切换生产依赖。

原完整Native ASan r10已结束：4069项，4061 PASS / 8 FAIL / 0 SKIPPED，exit8，
3497.33秒，未缩减选择。七项分别为Lance filter实际split失败1项、Lance physical
write4项、result reader1项和NULL-aware mark1项，各报告256字节；另一个timestamp
precision拒绝在真实compact-unwind展开崩溃。全部记录于
`/tmp/pangrova-production-20261007-native-asan-full-shutdown-r10.log`，SHA256
`238da9a9b661f451a0ccd5806d15070fd215862d5d7dd66b5290b1dc02ea2d5c`。
615项冻结源码的结束后核验仍通过；该失败不被后续nearest覆盖。

Native compact-unwind O1候选只在独立临时源/构建验证，当前共享源码不改、原完整
ASan失败不覆盖。保留全部instrumentation和异常；三个最终Mach-O都需验证FDE范围，
单case通过或EH段大小下降不足以计修复闭环，256字节系统TLS报告独立保持FAIL。

### 0.60 2026-10-07 新Release全量通过与实际产物身份

同一615项冻结输入的Release clean-first构建已完成，WERROR开启，exit0；日志
`/tmp/pangrova-production-20261007-native-release-clean-shutdown-r4.log`。
最近53项全部通过（0 SKIPPED，13.68秒），随后无过滤完整CTest发现4069项，
**4069 PASS / 0 FAIL / 0 SKIPPED**，438.78秒、exit0；日志
`/tmp/pangrova-production-20261007-native-release-full-shutdown-r7.log`，SHA256
`a805b4fc4c5ba26a481670546ac470559ba374f75151599e9f9da71d12558836`。
结束后冻结输入核验通过。实际新JNI库SHA256为
`6738bfbad1bf77a54e4f44e85640bf7d6e7ec13cb03577d343e84984e44f9607`。
22项JNI符号静态对称检查通过，但不能替代真实JNI调用。

实际打包路径发现架构别名漂移：三个Maven raw-copy execution将CMake输出复制到
`osx/aarch_64`，而loader读取canonical `osx/aarch64`；旧canonical库可能被选中。
删除独立raw-copy，统一由既有`package-native-lib.sh`产生canonical资源，再由Maven
正常复制。Loader对所有同名classpath库与实际已提取字节作流式SHA256检查，冲突
在`System.load`前拒绝，字节相同的重复资源允许。最近3项真实URLClassLoader/文件
提取组件测试通过（0 SKIPPED，29.010秒），日志
`/tmp/pangrova-production-20261007-native-resource-identity-nearest-r1.log`；它们不执行
JNI。新Release资源已为**独立macOS功能诊断**打包，源库和资源SHA相同，日志
`/tmp/pangrova-production-20261007-native-mac-functional-package-r8.log`。
原ASan完整FAIL仍阻止正式Step0–8发布验收；此次Mac打包不计正式生产Step4通过。

扩大Governance r3行为测试**124 PASS / 0 FAIL / 0 SKIPPED**，Checkstyle零违规；
同一verify命令在Gov SpotBugs阶段以7项告警失败，整体为FAIL，10分33秒。日志
`/tmp/pangrova-production-20261007-governance-full-static-r3.log`。告警涉及Context暴露
可变标签service/具体policy、Deps的policy引用以及三个store借用引用；没有扩大
exclude或把未接通的生产质量/标签链路算作通过。仅匹配bare-stub的静态脚本通过
也不能证明`DefaultQualityRuleExecutor`实际执行质量SQL。

正式Parquet候选r4完整16-module clean install结束：2445项中**2414 PASS /
31 SKIPPED / 0 FAIL**（Surefire 2397 PASS、31 SKIPPED，Failsafe17 PASS），13分38秒；
独立候选`/tmp/pangrova-sdk-release-candidate.OC3Ids/logs/parquet-full-cohort-r4.log`，
SHA256 `2a2b2ad1bf3e9621447176500842659b895bd0d1f38ca4b0347d14db007b8e06`。
默认Hadoop桥接所跳过24项随后使用最终shaded候选JAR和真实Hadoop3.4.3运行，
24 PASS、零ignore/assumption；余7项上游disabled/unsupported仍为SKIPPED。
最近候选62项和public writer/reader实际6项通过，后者验证PAR1/PARE早期abort文件空、
FD归零、零行footer读回和重复关闭；不是数据库E2E。SDK尚未切换生产依赖，正式
Iceberg构造/失败矩阵及完整acceptance未结束，不计SDK修复全闭环。

隔离O1候选JNI实际EH段仍为23,686,716字节，严格直接association审计中89,192项未
命中覆盖该函数的FDE；这不是89,192个实际崩溃证明。官方lld可用较早的CFI起点
作为合法搜索hint，错误PC范围计数还需区分此语义与真实截断/非CFI起址。O1没有
满足实际元数据及异常闭包门禁，不接受为修复，不应用共享flags。
原256字节报告已追到实际dyld pthread key258长期owner和LLVM17 Darwin-arm64空TLS
扫描范围；拟议runtime补丁尚未构建/验收，不能通过新suppressions消除报告。

### 0.61 2026-10-07 JNI实际加载与bundle安装修复

新canonical Release库的真实loader最近r2通过：4 PASS / 0 SKIPPED，39.100秒，
包括真实`System.load`1项和资源身份组件3项；日志
`/tmp/pangrova-production-20261007-native-resource-loader-nearest-r2.log`。
尚不是整个JNI执行/公开SQL/独立分布式验收。

标准classifier install r9实际FAIL：module按jar packaging只生成attachment、没有主JAR，
Maven install拒绝。改为packaging-only POM并显式绑定classifier jar goal后，r10 Maven
exit0但实际产物没有Native entry；该结果为**打包内容FAIL**，不是PASS。POM生命周期
没有默认resources绑定，已显式在classifier profile绑定resources→jar，保持唯一
canonical资源路径。r10原空产物保留于
`/tmp/pangrova-native-bundle-empty-r10.FQ2CnM/empty-classifier.jar`，SHA256
`46007f0e3fcce9ea02af7f603d932959455180f9458eafa0e08198efb2ebd9df`。
修后r11标准clean install及实际JAR内容检查通过，canonical entry为321,848,949字节，
提取SHA与Release `6738bfbad1bf77a54e4f44e85640bf7d6e7ec13cb03577d343e84984e44f9607`
一致；日志`/tmp/pangrova-production-20261007-native-mac-functional-bundle-green-r11.log`。
该Mac诊断产物不消除原ASan FAIL，也不代替Linux发布artifact gate。

重新clean install全部38个Java reactor projects（bundle已独立标准安装）通过，
`native-runtime`使用新classifier，1分32秒、exit0；日志
`/tmp/pangrova-production-20261007-mac-functional-java-clean-install-r12.log`。
该命令`-DskipTests`只证明干净编译/打包/安装，不是全量测试；存在shade资源/模块
元数据重叠及空smoke JAR告警，未声称Java零warning或全部生产验收通过。

列标签结构化拒绝另有HTTP分类缺陷：已有domain code未被mapper识别，返回500。
红测r1实际6项中5 PASS / 1 FAIL，expected400/actual500；只为既有
`PARSER_UNSUPPORTED_FEATURE`添加400映射，保留domain code和所有存储内部失败500。
绿测r2共7 PASS / 0 SKIPPED，1分55秒，含真实PG/Paimon列标签拒绝链1项与mapper6项。
日志`/tmp/pangrova-production-20261007-unsupported-sql-http-{red-r1,green-r2}.log`。
这是mapper及DDL SDK组件证明；异步SQL提交/最终FAILED仍需public入口复证，不把
此7项算作公共SQL HTTP全链路通过。

### 0.62 2026-10-07 Worker JNI扩展回归与独立进程缩比验收

Worker JNI/write最近r13真实FAIL：336项，318 PASS / 10 FAIL / 8 ERROR /
0 SKIPPED；日志`/tmp/pangrova-production-20261007-mac-functional-worker-jni-r13.log`。
8 ERROR的夹具验证schema只有`id`，而descriptor/实际batch是`id,name`，被现有生产
列数校验正确拒绝。10 FAIL的旧oracle假定abort/close失败立即移除owner，并假定
close失败平铺在主异常上；实际Session按abort→close组合异常，并保留未完成阶段的
所有权。修正真实两列夹具和清理断言，没有移除或弱化生产校验。

新增四个direct/heap与abort/close阶段组合验证，确认已成功阶段不重复，失败阶段在
清除注入故障后重试，owner仅在实际清理完成后释放，并且失败批次不能提交。原10项
同时验证嵌套异常、失败期间owner保留、后续append拒绝及重试无commit metadata。
r14共340 PASS / 0 FAIL / 0 ERROR / 0 SKIPPED，37.724秒；日志
`/tmp/pangrova-production-20261007-mac-functional-worker-jni-green-r14.log`。
包含真实Native loader/capability，但write adapter组件测试不是公共SQL分布式E2E。

按照用户要求在本机缩比，新增独立production Main进程测试：Coordinator与两个Native
Worker各自JVM，真实PG/LDAP、严格mTLS、同一Release Native哈希、运行JAR冻结manifest，
三表24个真实写文件，跨Worker JOIN/GROUP/全局Top-K结果、canonical terminal/drain
receipt、Worker排空退出及同ID新PID重入。真实IP SAN签发最近测试1 PASS / 0 SKIPPED；
日志`/tmp/pangrova-production-20261007-local-process-ip-san-r1.log`。冻结运行包370 JAR，
manifest SHA256 `a75c536063744ecf6666737775ff2562004549b240c547f337fa42d905365bd5`。
首轮独立进程E2E正在执行，尚无PASS结论；生产CI image provenance/staging接线仍待
闭合，不能用Mac host-local缩比替代Linux镜像验收、故障覆盖和生产规模SLO。

隔离Iceberg SDK原九模块完整r1已关闭FAIL：行为9580项中9019 PASS / 561上游
SKIPPED / 0行为FAIL，但原Checkstyle发现close复杂度及Set构造违规。保留原日志/XML，
隔离修复仅分解同顺序私有清理阶段及原规范要求的Set factory；后续最近静态与行为、
完整构建/API兼容/正式写入故障验收仍未全部完成。SDK未接入生产依赖，不标已验收。

### 0.63 2026-10-07 实际生产classpath启动失败与精确依赖修复

独立进程首轮r1为testCompile FAIL / 0 tests：夹具`java`字段遮蔽包名，已改为
`javaExecutable`。r2为1 ERROR / 0 SKIPPED：真实Coordinator进程退出，运行包无SLF4J2
provider，startup错误因NOP logging无堆栈。Worker/Coordinator日志实现原只在test
scope；三个进程装配模块现显式声明runtime/optional日志实现，避免传递到API消费者。
最近重新安装32模块通过（skipTests）；不标行为通过。

日志恢复后的r3仍为1 ERROR / 0 SKIPPED，29.795秒，给出真实根因：HTTP/Jersey4
AnnotationAcceptingListener初始化使用ASM API589824，生产classpath先加载Hadoop
传递的Jersey1 ClassVisitor（仅接受262144/327680），实际抛IllegalArgumentException。
真实JAR枚举、javap和dependency:tree互相吻合；不是HTTP随机启动超时。日志
`/tmp/pangrova-production-20261007-independent-local-process-e2e-r{1,2,3}.log`及
`/tmp/pangrova-production-20261007-production-classpath-conflict-tree-r1.log`保留。

在managed Hadoop common/MR client依赖边界精确排除旧server/servlet和SLF4J1绑定；
未用classpath排序、fixture删除或整个javax.ws.rs namespace排除。真实jdeps证明
WebHdfsFileSystem等使用MediaType/HttpHeaders/Response，故保留其API依赖。新38模块
install通过，1分18秒（skipTests）；日志
`/tmp/pangrova-production-20261007-production-classpath-install-r2.log`。新冻结manifest
SHA256 `db92949a5b78ff0820e2c76af45692d32a1b8bb1324c69d257e6b9d9be35a9be`，只含
Jersey4 server而仍有JSR311。真实双Worker r4正在验证，尚未声明E2E通过。

CI装配/验收接线已应用：Docker producer按实际Maven build.finalName选取principal
JAR，依赖全量复制、不在fixture过滤；完整image lib/native/manifest提取核对不可变
image ID、源码revision和所有哈希；两处full-regression Maven均传入严格artifact
inputs。LocalRuntimeArtifacts集中拥有此验证职责，Mac继续核对真实Release cache。
Shell语法/YAML解析/diff whitespace PASS；Linux镜像构建/full job仍NOT RUN，不算通过。

隔离compiler-rt17 TLS扫描修复的默认正/负对照已PASS：真实main/worker TLS边界、
唯一runtime实际加载、TSD sole-owner317保留与clear/delete精确317漏报对照、419真实
lost allocation、真实UAF/OOB负对照、1/32/4096错误复制/抛出/exception_ptr释放、329
exports与完整runtime metadata。receipt
`/tmp/pangrova-compiler-rt17-tls.I8yt9I/isolated-runtime-controls-receipt.md` SHA256
`921982ab3a27b083537eab6ff47e16898305a37464f4244ae6749a076af4cd0d`。仅为
PASS_ISOLATED_CONTROLS；原ASan full FAIL、Native大型Mach-O unwind缺陷仍开放，没有
安装/替换共享runtime、运行完整Native候选或将原八项失败改写通过。

### 0.64 2026-10-07 Coordinator进程所有权与真实启动次序复证

独立进程r4 CLOSED FAIL，1 ERROR / 0 SKIPPED，41.837秒：Jersey修复后真实HTTP
已启动，Main随即正常返回；所有剩余线程为daemon，JVM立刻执行shutdown hook。
根因不是启动异常或超时。生产入口现在由Main持有可中断的生命周期等待，只有实际
依赖清理成功返回才解除；预启动close及失败清理均不解除。未修改Jetty daemon属性。
最近CoordinatorLifecycleAdmissionTest 5 PASS / 0 SKIPPED，57.371秒；随后实际
Coordinator install PASS，23.982秒（skipTests，不标为行为验收）。日志分别为
`/tmp/pangrova-production-20261007-coordinator-process-wait-nearest-r1.log`、
`/tmp/pangrova-production-20261007-coordinator-process-wait-install-r1.log`。

重新冻结运行产物manifest SHA256
`6d0657182ec018595227fe97b8d4f8a6f8b62c3eb381815bca039319c57685b3`，Native仍为
`6738bfbad1bf77a54e4f44e85640bf7d6e7ec13cb03577d343e84984e44f9607`。
r5 CLOSED FAIL：8项中5 PASS / 2 FAIL / 1 ERROR / 0 SKIPPED，2分35秒。
真实Coordinator保持运行并返回健康JSON；夹具错误地在Worker/catalog创建前等待
集群ready，按设计必然503。另两项是新增组件测试未规范化macOS TempDir路径，
实际toRealPath为/private/var而预期为/var。夹具分开核验控制面http/transport就绪
和完成注册/catalog后的集群200；组件fixture按真实路径比较，不降低生产就绪要求。

r6 CLOSED FAIL：9项中8组件PASS / 1 E2E ERROR / 0 SKIPPED，48.429秒。
真实独立Worker完成Native加载、mTLS服务、canonical capability注册和健康200；
SELECT 42早于首个周期资源heartbeat，Coordinator注册记录的availableMemory=0，
调度器正确fail-closed。新夹具只观察真实资源heartbeat及Coordinator 200 ACK后
继续，不设置假容量、不提前宣称可调度、不重试失败SQL。r7仍在执行，未标E2E通过。
日志`/tmp/pangrova-production-20261007-independent-local-process-e2e-r{4,5,6,7}.log`。

LocalRuntimeArtifacts已按同类职责分开清单、canonical条目、本地Release cache、
image owner/receipt/Docker identity验证；无新增转发层或执行路径。新增组件覆盖
真实ZIP/SHA清单及必填输入/字节变更/过期JNI/tests JAR/非Release/不完整image
provenance拒绝，并明确拒绝ReleaseWithDebInfo子串。8项组件PASS不代表JNI或Docker
镜像E2E；全量Java静态、Linux镜像、原ASan八项失败及全部AI能力验收仍开放。

### 0.65 2026-10-07 控制结果游标、真实TCP背压与安全关闭复证

独立进程r7 CLOSED FAIL：9项中8组件PASS / 1 E2E ERROR / 0 SKIPPED，1分47秒。
真实CLI SELECT 42成功，CREATE CATALOG结果无限重放，客户端ByteArrayOutputStream
触发实际Java heap OOM；原生产CoordinatorDataProducer重复读取保留buffer而没有
请求游标/EOF。原组件夹具用全局consumed集合掩盖该生产缺陷。SPI现在统一使用
DataProducer.openStream创建请求级DataStream；保留响应每请求读取一次后EOF，独立
请求可以重新读取，不删除共享结果或添加兼容轮询路径。夹具改用真实生产producer。
新增实际TCP覆盖普通确认和空DDL结果，两次独立请求均要求实际EOF。最近专项
Worker18 + Coordinator10 PASS / 0 SKIPPED；39-reactor install PASS，66秒，
skipTests只计编译/安装。日志`/tmp/pangrova-production-20261007-control-result-cursor-{nearest-r1,full-install-r2}.log`。

r8运行产物manifest SHA256
`76103f5ebba04dfabe5509ea6e204d8fbb1a508b1e97fc0b108c156362103c9f`，Native仍为
`6738bfbad1bf77a54e4f44e85640bf7d6e7ec13cb03577d343e84984e44f9607`。
r8 CLOSED FAIL：10项中9组件PASS / 1 E2E ERROR / 0 SKIPPED，1分59秒。实际
CREATE CATALOG及三个CREATE TABLE完成；首个INSERT已执行但其确认结果使用
Coordinator明文端点，被严格TLS客户端正确拒绝。退出再暴露Worker先deregister
后发送owner释放回执，实际报告被409 worker_not_registered拒绝，Coordinator
缺失精确drain回执并被夹具SIGKILL；保留该真实退出失败。日志
`/tmp/pangrova-production-20261007-independent-local-process-e2e-r8.log`。

控制面确认服务现在消费同一已验证TLS材料并声明GRPC_TLS，实际SDK开启mTLS
client verification，保留显式未配置TLS时的GRPC行为。新增真实TCP：有效双向
证书确认+EOF、无客户端证书拒绝、显式plaintext，共3 PASS / 0 SKIPPED。
关闭顺序改为：停止新HTTP输入/CDC、任务drain与close并取得终态ACK、停止并join
heartbeat、要求typed deregistered ACK、关闭注册client及依赖资源。错误成功body、
未确认注销和中断均不算ACK。实际HTTP/本地owner边界新增3项；最近Worker14及
Coordinator5 PASS / 0 SKIPPED，未将组件结果计为双Worker E2E通过。日志
`/tmp/pangrova-production-20261007-control-tls-shutdown-nearest-r1.log`、
`/tmp/pangrova-production-20261007-shutdown-ack-nearest-r2.log`。

真实TCP31连接barrier在原单池转发中确定性FAIL（accept+31 forward占满32线程，
reverse全部排队）；显式31连接上限与独立有界reverse池修复后最近1 PASS，
关闭要求双池/socket/permit实际结束，非静默drop或无界线程。另针对真实传输
建立5项故障回归：r1 3 PASS / 2 FAIL，producer异常被Future隐藏及SDK丢弃
错误描述；r2相同失败保留。生产改为AbortPolicy禁止CallerRuns进入满队列死锁，
消费方观察完成Future并传递原异常，NULL超时/中断不发送EOF，保留SDK错误状态。
r3原专项18 + 新5 = 23 PASS / 0 SKIPPED，28.047秒。日志
`/tmp/pangrova-production-20261007-stream-failure-{red-r1,green-r2,green-r3}.log`。

追加配置核查：生产Main此前只消费TLS材料，确认服务的发布地址/port/allocator
仍硬编码为默认值；这使匹配真实LAN IP SAN的证书被错误localhost端点使用。
新增统一typed keys `coordinator.transport.{advertised-host,port,allocator-limit-bytes}`
并消费到同一TransportConfig，拒绝wildcard地址、非法端口及非正预算。48项最近
配置/wiring/属性/TCP测试PASS / 0 SKIPPED，47.695秒，日志
`/tmp/pangrova-production-20261007-control-transport-config-nearest-r1.log`。
夹具显式发布实际LAN地址并缩小确认allocator至16MiB，没有放宽证书验证。
上一39-reactor编译安装r3 PASS，1分23秒；新配置r4实际重新安装PASS，1分07秒。
SPI142类型注解gate及E2E真实性gate PASS。r6冻结运行manifest SHA256
`335f8fba5da92cee1f48ced2135e56d9bdefbe878499843a4efc1c1f692569ec`。
独立进程r9 CLOSED FAIL，10项9组件PASS / 1 E2E ERROR / 0 SKIPPED，1分04秒。
控制确认端点真实GRPC_TLS/实际LAN地址已生效，但夹具复用只有DNS localhost SAN的
CLI client证书作为Coordinator服务端证书，实际IP hostname验证正确拒绝；不是
生产TLS验证失效。夹具现在为Coordinator另发独立IP SAN身份，不改CLI client
权限或验证规则。该r9未再出现SIGKILL关闭错误。r10 CLOSED FAIL，10项9组件PASS /
1 E2E ERROR / 0 SKIPPED，1分18秒，日志
`/tmp/pangrova-production-20261007-independent-local-process-e2e-r10.log`。
严格TLS及前三次真实INSERT已成功，随后tenant concurrency 2 >= 2拒绝。
真实日志证明初次SELECT Native owner直到shutdown才释放；第三次INSERT已实际
释放Native但Coordinator dispatch/report publication operation退出后的cleanup仅
入队，未及时重试。Worker consumed-result检查被错误绑定一分钟历史扫描，
Coordinator重试也被绑定历史扫描。不得通过加大配额、等待TTL、重试被拒SQL或
放宽排空证据掩盖问题。

新增两项实际timer组件回归先RED：Worker一项FAIL（2秒内没有消费检查），
Coordinator五项中一项FAIL（小时retention sweep前没有物理完成重试）。修复将
物理检查与历史扫描分离：Worker每100ms最多64个既有registry entries，公平
延续iterator而非每次全扫/每任务timer/另一owner队列；Coordinator每100ms向既有
bounded cleanup executor提交单个重试，保留每轮4次上限及原指数退避。Native
实际drain、身份、结果有效期、tenant配额、retention均未放宽。首次最近21项
PASS / 0 SKIPPED，21.109秒，日志
`/tmp/pangrova-production-20261007-terminal-retry-green-r1.log`；新增公平/失败保留
回归已复验：22项PASS / 0 SKIPPED，22.059秒，日志
`/tmp/pangrova-production-20261007-terminal-retry-green-r2.log`。全39模块安装r5
PASS / skipTests，1分06秒；不据此标E2E PASS。

control-path static r1 CLOSED FAIL：Worker Checkstyle 177项失败，Coordinator未执行；
日志`/tmp/pangrova-production-20261007-control-path-static-r1.log`。Config检查已完成，
不将静态门禁失败或下游SKIPPED记为PASS。

当前等比例验收只降低数据/预算，未缩减真实进程、网络、JNI、身份及owner协议。
最新全仓安装后仍须重新冻结产物、复跑双Worker/重启；全Java/static、完整AI E2E、
Smoke、ASan及大规模SLO不能由以上组件PASS代替。

### 0.66 2026-10-07 隔离SDK候选冻结与LLD候选拒绝

SDK候选SOURCE_READY，NOT PRODUCTION_ACCEPTED；未切换生产POM/global m2。
独占完整证据`/tmp/pangrova-sdk-release-candidate.OC3Ids/cohort-provenance.md` SHA256
`219fea4a93a3690f1275c3eac8ac3843169919cb7b8de6c4068460c6ef4e5ed5`。
Parquet完整2445=2414 PASS / 31上游SKIPPED，随后Hadoop3.4.3原24 bridge实际
PASS；Iceberg选定9模块10339=9764 PASS / 575上游SKIPPED，其中9019行为PASS
为r2真实执行后同输入Gradle恢复，745为r3新执行。精确官方1.11六模块原ABI
分析实际执行PASS，最终169-JAR shaded runtime实际18故障PASS / 0 SKIPPED。
source/Javadoc正常生成及SBOM完成，16主JAR/31分类包/17POM材料完整锁定。
702 Fastutil重名class与官方原集合一致，但30 split packages/无显式AMN、
distribution NOTICE/service review、下层构造/OOM/release和failed-close边界、
生产集成及公共SQL故障E2E仍开放，不能将upstream条件skip写成PASS。

隔离LLD17实际三目标链接均exit0/零warning，1361输入及6994基线前后完全一致，
但候选正式REJECTED，未执行Native或替换共享runtime。完整报告
`/tmp/pangrova-lld-cohort-preparing.3OVSaf/actual-link-and-rejection-receipt.md` SHA256
`d901617a1116a72b63120383de64fe2807417d446851aa9aa4606dd849492b82`。
真实Rust匿名compact-only汇编helper被旧LLD跳过，没有覆盖FDE；官方当前源码
的匿名CUE owner机制可作下一候选的精确backport依据。另COMMON转Defined晚于
显式exports过滤，使JNI/tests额外导出三个OpenSSL符号，必须独立修复该顺序/策略。
不得通过接受额外exports、删除compact unwind、后处理metadata、合并runtime或
放宽审计取得绿灯。原ASan完整八项FAIL及此次静态metadata/ABI FAIL全部保留。

### 0.67 2026-10-07 真实HTTP来源、控制流限流与请求体边界

r7冻结manifest SHA256 `404294433ac0ea7bced8a7c9d0493af3e0f2fea6f432b01d5abc3f21828cf96d`。
独立进程r11 CLOSED FAIL，10项9组件PASS / 1 E2E ERROR / 0 SKIPPED，1分42秒，
日志`/tmp/pangrova-production-20261007-independent-local-process-e2e-r11.log`。
实际Coordinator-terminal-cleanup线程已在初次SELECT后释放执行anchor，后续
20余次写入继续成功，没有重现tenant concurrency 2 >= 2。新失败是HTTP入口
100/min限流，将所有无X-Forwarded-For的请求归入unknown桶；还盲目信任客户端
头部，因此伪造头可绕过限流。真实Worker deregister也被429拒绝，而不是ACK。

完整调用链：Jetty servlet peer → JAX-RS filters → AuthenticationFilter /
RequestLimitFilter → WorkerResource / TaskStatusResource。原peer完全未消费，
内部control与public API共桶且filter顺序未明确。新增pre-matching peer采集，
先认证再按服务器产生的typed internal身份隔离public budget；仍校验internal
route、secret、body size与控制端精确authority。移除没有caller、从未消费的
trustedProxyCount构造设计；不通过加大HTTP额度、伪造XFF或忽略deregister解决。
目前无trusted-proxy whitelist策略，代理后的真实peer预算/大规模控制吞吐仍
需要独立设计与验收，不能声明现有修改已覆盖全部生产拓扑。

真实TCP red-r2四项3 FAIL / 1 PASS / 0 SKIPPED：伪造IP得到200而非429、
control被public额度饿死429、认证得到头部IP而非真实127.0.0.1。
`/tmp/pangrova-production-20261007-http-control-limit-red-r2.log`；初次red-r1是
测试UserIdentity构造签名编译错误，保留而不算行为红灯。green-r1最近36项
PASS / 0 SKIPPED，16.124秒，`/tmp/pangrova-production-20261007-http-control-limit-green-r1.log`。

新实际TCP请求体边界再RED：恰好128B被错误500拒绝；chunked 129B原500而非
413，六项中2 FAIL / 4 PASS / 0 SKIPPED，日志
`/tmp/pangrova-production-20261007-http-body-boundary-red-r3.log`。原stream在
达到max时未检测真实EOF；读取超限IOException又被误当服务端错误。现在只
探测实际额外字节、保留overflow失败状态、正确处理零长度读取，超限产生
明确413，统一mapper保留Web错误状态/Retry-After并禁止异常产生200。新的
实际TCP/stream/认证/typed identity/mapper最近47项PASS / 0 SKIPPED，18.779秒，
日志`/tmp/pangrova-production-20261007-http-body-boundary-green-r2.log`。
中间body red-r2测试错误引用private nested ObjectMapperResolver导致编译FAIL，
修正后才取得上述真实行为红灯；不隐藏该失败。
全仓39模块`mvn -B -Pnative-runtime -DskipTests install` r6 PASS，53.756秒，
日志`/tmp/pangrova-production-20261007-http-control-full-install-r6.log`；仅是编译/
打包/安装，不是行为验收。r8冻结完整生产runtime manifest SHA256
`935fee3f4e4cdf92d2784e8b847e834ef70fef1c4005c64411bde9b397415d81`，Native仍是
`6738bfbad1bf77a54e4f44e85640bf7d6e7ec13cb03577d343e84984e44f9607`。

### 0.68 2026-10-07 双Worker公共入口复证与物理Join校验修复

r12 CLOSED FAIL，10项9组件PASS / 1 E2E ERROR / 0 SKIPPED，总2分01秒；
日志`/tmp/pangrova-production-20261007-independent-local-process-e2e-r12.log`。
24次真实Native写入全部完成，第二独立Worker进程启动并注册；未再出现前轮
unknown-IP限流或tenant concurrency配额失败。三表JOIN/聚合/Top-K在Stage 2
序列化前被`SubstraitPlanValidator`拒绝：`StandardsCompliantMergeJoin`。
因此不能计跨Worker结果、drain、重启通过。失败诊断保留于
`/var/folders/kl/t8p9lh1d0ql0_gw1kv3qlkc80000gn/T/pangrova-local-process-16009131805053430197`。

调用链：public CLI → HTTP submit → SqlCompiler / CBO physical join → fragment
serialization → SubstraitPlanValidator → NativeCapabilityRegistry。
根因是校验器按Java具体类simple name匹配，遗漏标准MergeJoin与NestedLoopJoin，
而默认registry也没有这两项；Native实际inventory和converter均已有标准支持
（`NativeCapabilityInventory.cpp`、`SubstraitPhysicalJoinConversion.cpp`、
`SubstraitLogicalJoinConversion.cpp`）。修复按SDK物理Join类型契约匹配，保持
统一Substrait/v2，不改proto、不绕过capability；同步校验MergeJoin的keys/
post-filter、NestedLoopJoin的condition，未知能力/函数仍拒绝。

最近真实converter→validator RED 33项15 FAIL / 18 PASS / 0 SKIPPED，6.497秒；
`/tmp/pangrova-production-20261007-physical-join-validation-red-r1.log`。
GREEN最近6类65项PASS / 0 SKIPPED，12.389秒，含参数化Join types、非对称
schema、撤销MergeJoin capability、join-depth key校验与条件函数fail-closed；
`/tmp/pangrova-production-20261007-physical-join-validation-green-r1.log`。
独立进程验收须用新冻结产物再跑，不能将该组件通过升级成E2E通过。

r12清理阶段另见真实Coordinator共享scheduler未终止：控制ack缓冲区的5分钟
TTL任务没有撤销，`ExecutorDrain.shutdownAndAwait`真实5秒等待失败。
最近buffer RED 14项3 FAIL / 11 PASS / 0 SKIPPED，16.326秒，
`/tmp/pangrova-production-20261007-control-buffer-lifecycle-red-r1.log`：close后
定时任务保留；调度拒绝后entry/bytes未回滚；quota拒绝静默返回。
修复为buffer拥有各entry的expiration，物理query/publication drain后封写并
撤销timer、真实join scheduler，结果transport退出后才释放保留的ack字节；
插入/限额/关闭/删除用本地内存临界区串行，调度拒绝回滚，quota失败阻止
FINISHED，expiration按entry identity避免旧回调误删新结果。不得通过
shutdownNow丢弃未知cleanup或增加drain timeout。首次green-r1编译FAIL是残留
日志变量projectedTotal，修正后继续最近测试；完整关闭仍待真实进程复证。

join/buffer扩围最近r3为Planner66项 + Coordinator34项，共100 PASS / 0 SKIPPED，
22.345秒；`/tmp/pangrova-production-20261007-join-buffer-nearest-green-r3.log`。
补充Join residual函数拒绝与buffer拒绝不得调用finisher；最新Coordinator
r4为35 PASS / 0 SKIPPED，21.044秒，含已经进入expiration回调同时seal的
实际调度器竞态、保留ack直至transport关闭、三个真实TCP/TLS场景中只cancel
buffer owned timer再graceful shutdown并实际awaitTermination，不用shutdownNow
制造成功；`/tmp/pangrova-production-20261007-control-buffer-lifecycle-green-r4.log`。
未变更Native输入，匹配既有Release构建最近`ctest -R
'MergeJoin|CanonicalJoin|NativeCommandGetCapabilities' -j2 --output-on-failure`
349 PASS / 0 SKIPPED，33.18秒；
`/tmp/pangrova-production-20261007-canonical-joins-native-nearest-r1.log`。
这些仍是最近组件/Native专项证据。全仓39模块安装r7 PASS，56.155秒，
`/tmp/pangrova-production-20261007-join-buffer-full-install-r7.log`；skipTests只
证明编译打包。r9生产runtime manifest SHA256
`f282649818b942890ac7ee861b18e8e37b8da0f602e28ec02e260da8094a6a6b`。

### 0.69 2026-10-08 SDK Join wire 输出与 Native canonical 一致性

r13 CLOSED FAIL，10项9组件PASS / 1 E2E ERROR / 0 SKIPPED，总1分59秒；
`/tmp/pangrova-production-20261007-independent-local-process-e2e-r13.log`。
24次Native写入与第二独立Worker注册完成，MergeJoin已通过Planner校验，
真实Native执行随后拒绝`Deprecated join key lists are not a canonical plan`。
下游shuffle NOT_FOUND是失败producer尚未提供结果的连带错误，不能以fallback
掩盖。跨Worker结果、drain和重启仍未通过。Coordinator真实scheduler退出日志
`executor_actually_terminated`已出现，上一轮owned TTL任务阻塞根因不再复现；
这不等于所有关闭阶段验收通过。

调用链：public CLI → SqlCompiler → SubstraitConverter →
SubstraitPlanSerializer → official SDK RelProtoConverter → v2 task →
Native SubstraitPhysicalJoinConversion::requireCanonicalJoinKeys。
复核实际SDK 0.95.0 sources，HashJoin/MergeJoin转换在所有keys为EQ时同时
输出标准ComparisonJoinKey与deprecated左右keys，属于SDK兼容输出。
Native已有统一canonical检查，修复唯一序列化边界而非放宽Native或新建协议。
Normalizer递归确认标准keys数量、EQ比较及左右字段逐项完全相同后，移除冗余
deprecated副本；旧字段独占、数量不等、冲突或有损比较一律拒绝。已canonical
的其他比较语义保持不变，输入plan不变且归一化幂等。

最近真实converter→production serializer RED 34项18 FAIL / 16 PASS / 0 SKIPPED，
7.818秒；`/tmp/pangrova-production-20261007-canonical-join-wire-red-r1.log`。
首次GREEN 68 PASS / 0 SKIPPED，11.858秒；新增递归、旧字段独占、数量、冲突、
比较语义与幂等边界后，7类82 PASS / 0 SKIPPED，11.127秒；
`/tmp/pangrova-production-20261008-canonical-join-wire-green-r2.log`。
这些仍是协议边界组件证据；新的全仓安装、冻结产物与双Worker公共入口复证
继续执行，不能提前计E2E通过。

全仓39模块r8安装PASS，1分06秒；
`/tmp/pangrova-production-20261008-canonical-join-wire-full-install-r8.log`。
冻结r10 manifest SHA256
`a93205a021ce54bf988056cc8fa4273e369e3499766704d3980bca5715dcfacd`。
r14公共进程CLOSED FAIL，10项9组件PASS / 1 E2E ERROR / 0 SKIPPED，2分17秒；
`/tmp/pangrova-production-20261008-independent-local-process-e2e-r14.log`。
Join wire旧字段错误不再复现，真实shuffle接收端随后拒绝
`Arrow wire schema differs from its admitted canonical schema`。
下游producer aborted是连带错误，不是允许部分结果的理由。具体两端schema
差异尚未取得，不把可能的名称或nullability问题写为已证根因。
诊断workspace：
`/var/folders/kl/t8p9lh1d0ql0_gw1kv3qlkc80000gn/T/pangrova-local-process-4430629578030369803`。
Coordinator真实scheduler终止、drain完成日志均已出现；正常查询drain/Worker重启
仍因主查询失败而未执行。

Native仅扩充失败诊断以输出expected/actual schema，保持严格Equals与既有
失败语义。Release / WERROR构建PASS，最近ArrowBatch/CanonicalArrow/
CanonicalSerde 50 PASS / 0 SKIPPED，13.32秒；重新打包与39模块r9安装PASS，
1分06秒。日志分别为
`/tmp/pangrova-production-20261008-arrow-wire-schema-diagnostic-release-r1.log`、
`/tmp/pangrova-production-20261008-arrow-wire-schema-diagnostic-native-nearest-r1.log`、
`/tmp/pangrova-production-20261008-arrow-wire-schema-diagnostic-package-r1.log`、
`/tmp/pangrova-production-20261008-arrow-wire-schema-diagnostic-full-install-r9.log`。
新Native SHA256
`fe8677eece5943daa37b72604589a5542aa9546332107333952c4e46a7a52de1`；
冻结r11 manifest SHA256
`4f19e3be53a599f26f96a798b40a6b0ade6dd46d21022795b4ae905fdc47f47b`。
r15 CLOSED FAIL，9组件PASS / 1 E2E setup ERROR / 0 SKIPPED，18.541秒：
实际产物guard发现native classifier仍包含旧库，业务进程未启动；
`/tmp/pangrova-production-20261008-independent-local-process-e2e-r15.log`。
普通install没有激活显式classifier包装profile。随后按实际平台显式包装并核对
classifier和Worker JAR中的Native SHA均为上述新值；包装10.044秒PASS，
`/tmp/pangrova-production-20261008-arrow-wire-schema-diagnostic-classifier-r1.log`。
发行装配的多模块dependency复制还会
合并同一库多个版本（例如Snappy、ZooKeeper、Commons），单图依赖解析与
实际package/classpath收敛仍是待修门禁，不以当前classpath顺序当兼容证明。

### 0.70 2026-10-08 普通查询绑定实际上游 canonical schema

r16 CLOSED FAIL，9组件PASS / 1 E2E ERROR / 0 SKIPPED，2分18秒；
`/tmp/pangrova-production-20261008-independent-local-process-e2e-r16.log`。
匹配Native的新诊断确认两端物理类型相同，但消费端第4列要求`id0`、生产端为`id`。
真实五列schema分别为`id, score, label, id0, region`与
`id, score, label, id, region`，不是nullability或其他猜测。

根因链：QuerySchedulingEngine.dispatchByTopology → StageDAG.fromFragmentedPlan →
DAGExecutionState.serializePlan → 单独PlanFragment序列化 → Calcite展示名称被写入
ExchangeInputDetail.output_schema；生产端CanonicalRelationSchema保持实际Join输入
的同名字段。已有AuthorizedStageExchangeBindings只接通durable/export整图路径，
普通查询未使用，故组件整图通过不能证明生产普通查询接通。

修复共用同一个Planner canonical绑定算法。普通DAG在上游完成所有生成值、检索和
模型operator绑定后，保存实际准备下发的不可变plan bytes；下游在序列化自身实际
自适应计划时，从该上游schema绑定输入。缺上游、输入身份、类型或distribution
不符均拒绝；展示结果名称不改，Native严格校验不放宽，Coordinator不处理数据。
持久化replay仍只校验冻结契约，不重新编译或修复。

组件RED r2：真实SQL→普通DAG序列化3项全部FAIL / 0 SKIPPED，14.202秒；
`/tmp/pangrova-production-20261008-live-exchange-binding-red-r2.log`。
GREEN r1：Planner6项和Coordinator5项全部PASS / 0 SKIPPED，36.407秒；
`/tmp/pangrova-production-20261008-live-exchange-binding-green-r1.log`。
覆盖computed、aggregate、同名Join及已有durable graph/replay；不是Native值/网络证据。
全仓39模块r10 install PASS，54.383秒（skipTests，仅编译安装）；
`/tmp/pangrova-production-20261008-live-exchange-binding-full-install-r10.log`。
冻结r13 manifest SHA256
`4bd0fa943265390b9ee919b354359d1d35d1416ac4a222d606ff8c188d1d7929`，
classifier与Worker内Native SHA仍为`fe8677eece5943daa37b72604589a5542aa9546332107333952c4e46a7a52de1`。
独立双Worker公共链r17 CLOSED PASS，10项 / 0 FAIL / 0 ERROR / 0 SKIPPED，
2分23秒；`/tmp/pangrova-production-20261008-independent-local-process-e2e-r17.log`。
其中1项真实进程E2E（134.2秒），9项是独立组件，不混计。
完成24次真实Native写入、两个独立Worker跨进程JOIN/GROUP/全局Top-K两轮
精确79..70结果；两Worker多stage真实FINISHED与NATIVE_OWNER_RELEASED及200回执；
第一Worker真实SIGTERM/drain/deregistration、第二Worker独立SELECT42、相同ID
新PID/epoch重启后再次查询，以及persisted tenant reservation归零。
SDK补丁、ASan、全量Java/AI E2E与生产大规模性能仍有未完成门禁，不能因此
宣称所有功能或整体验收完成。当前环境仅缩小数据与预算，不以单进程模拟分布式。

### 0.71 2026-10-08 新 Native 全测、全量 Java 回归与新增 SDK 构造期证据

普通live/durable交换、DAG取消/重试/回调、AI/检索binding与属性测试扩大GREEN r2：
Planner13项 + Coordinator67项 = 80 PASS / 0 SKIPPED，1分13秒；
`/tmp/pangrova-production-20261008-live-exchange-binding-green-r2.log`。
新诊断Native库的完整Release CTest：4069 PASS / 0 FAIL / 0 SKIPPED，386.86秒；
`/tmp/pangrova-production-20261008-release-native-full-r2.log`。
不是ASan通过，也不替代JNI/公开AI链路的验收。

全量`native-runtime,full-regression,coverage verify` r3 CLOSED FAIL，2分13秒；
`/tmp/pangrova-production-20261008-java-full-regression-r3.log`。
已运行2042项，2041 PASS / 1 FAIL / 0 SKIPPED，停在Test Common，后续reactor模块
未运行（Maven reactor SKIPPED不是测试PASS）。失败是RegressionSelectionTest匹配
旧`IMAGE=... docker buildx bake`，实际CI已加入`SOURCE_COMMIT`并使用shell续行。
修正仅规范续行、保留全部原image/full-regression顺序与覆盖门禁，并加强同一build
命令的source commit、实际image revision和staging commit断言，没有削弱测试。
第一次修正漏算续行前空格，green-r1仍FAIL并保留；green-r2实际7 PASS / 0 SKIPPED，
4.382秒；`/tmp/pangrova-production-20261008-ci-selection-source-provenance-green-r2.log`。
完整r4已重新执行，提供冻结r13给独立进程测试，未完成前不计全量通过。

SDK补丁仍不接入生产。Root新增只读169个冻结candidate JAR的公共SDK构造期oracle，
独立子JVM限制64 MiB，真实ExampleParquetWriter builder使用合法正数1 GiB page size。
实际失败位置：ByteArrayOutputStream.<init> → CodecFactory$HeapBytesCompressor.<init>
:222 → getCompressor → ParquetWriter.<init>:398；不是fake allocator或推测OOM。
`/tmp/pangrova-sdk-constructor-lease-oracle.7tq6Fmpn/candidate-red-r3.log` CLOSED FAIL：
CodecPool lease 0→1、失败文件存在、lsof真实FD=1；随后普通8行完整write/read成功，
lease仍为1。成功恢复不能折算为失败owner释放。当前candidate的公共Writer在取得
compressor之前已start输出，且构造失败没有返回可关闭owner；HeapBytesCompressor
先借lease再分配buffer，也丢失该lease。这是两个实际根因，不能仅移动一行后
宣称所有构造/登记失败闭合。r1过早文件断言、r2 oracle int/long类型错误均保留，
r3已修正oracle类型并成功真实读回；未把前两次结果计PASS。
candidate169-JAR原manifest SHA
`ad7b8c0c8970e4378338deec96e1af925ded29c06db10d90c498752dba543fd8`
复核通过；程序不写SDK、生产依赖或冻结产物。下一候选必须独立可追溯版本、
完整原上游与Pangrova链路验收后才可采用。

LLVM17独立policy-compatible配置Stage S为configure-only PASS，零surfaced告警、
34个真实cache probes保持一致（6个FALSE不覆盖），source/tool后哈希不变。
Root已编译真实official gtest支持target，3对象/2 archive exit0零告警；artifact闭包
复核中。TableGen、Stage L AsLib、实际候选链接及完整Native ASan尚未执行，
不得从配置/支持库构建通过推断其已修复。

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
