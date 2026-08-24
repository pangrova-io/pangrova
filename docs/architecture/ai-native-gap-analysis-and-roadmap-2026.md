# Pangrova AI Native 现状审计与端到端演进方案

> 状态：架构决策输入，不代表当前功能承诺  
> 审计日期：2026-08-21  
> 对标对象：Snowflake Cortex AI Functions、Cortex Search、Semantic Views、Cortex Analyst、Cortex Agents、AI Observability、AI Guardrails

## 1. 执行摘要

Pangrova 已经具备 AI Native 的关键技术原语：SQL AI 函数、可插拔推理与
Embedding SPI、FILE/BLOB、多种向量索引、SIMD 距离计算、分布式 Top-K、全文
索引、重排器、模型注册、Embedding 队列、语义缓存和 AI 指标。最成熟的链路是：

```text
SQL -> Calcite -> Substrait -> JNI -> Native/Velox
    -> SIMD vector distance or Java ANN -> distributed Top-K -> result
```

但当前实现尚不能被定义为完整的 AI Native 数据库能力。主要问题不是组件数量，
而是缺少贯穿规划、鉴权、模型解析、预算准入、执行、结果质量、计量、审计和评估的
统一契约。部分能力仅有接口或架构文档，部分 E2E 测试允许运行时失败，已知的基础
查询链路也仍有多 split 丢行、shuffle 空结果和安全隔离失败。

Snowflake 的领先点同样不只是 AI 函数数量，而是将模型、Search Service、Semantic
View、Agent、Thread、Run、Evaluation Dataset 和 Budget 建模为受 RBAC、生命周期、
成本和可观测性管理的数据库对象。Pangrova 应利用开放湖格式、可插拔模型和 native
执行的差异化优势，补齐相同级别的产品闭环，而不是直接复制 Cortex API。

推荐按以下顺序推进：

1. **先恢复真实性与正确性**：修复批处理、能力协商、错误降级和虚假 E2E。
2. **统一 AI 执行控制面**：模型对象、能力目录、鉴权、预算和 telemetry 必须在每次
   provider 调用前后执行。
3. **产品化 Search Service**：把零散向量、全文、重排和 backfill 能力收敛为可运维
   的 schema 对象。
4. **建设 Semantic View 与 Analyst**：在可信业务语义上生成和验证 SQL。
5. **最后建设 Agent**：Agent 只能调用已经受治理、可观测和可评估的工具。

## 2. 审计口径

本报告使用以下状态，避免把“存在代码”误写成“功能完成”：

| 状态 | 定义 |
|---|---|
| 已闭环 | 有生产入口、实现、注册、调用方、失败语义和有效测试 |
| 部分闭环 | 主实现存在，但治理、持久化、分布式语义或测试不完整 |
| 未接通 | 类型或实现存在，但没有生产 wiring 或调用方 |
| 仅设计 | 只存在于文档、AST 或计划节点，执行链路未证实 |
| 存在缺陷 | 已发现会产生错误结果、错误模型、静默降级或虚假通过的问题 |

结论来自当前仓库源码、测试和 `pangrova-test-e2e/FAILURES.txt`。旧的
`ai-native-evolution-report-2025.md` 是行业调研和候选设计，不应继续作为实现完成度
依据。

## 3. Pangrova 当前能力地图

| 能力域 | 当前状态 | 证据与判断 |
|---|---|---|
| AI SQL 标量函数 | 部分闭环 | `AiFunctionFactory` 注册 complete、summarize、classify、filter、embed、extract、guard、predict、translate 及多模态函数；通过 Substrait 和 JNI UDF 回调执行 |
| Embedding | 部分闭环 | batch 路径存在，但未校验 provider 返回数量；返回 JSON `VARCHAR`，不是一等 `VECTOR` |
| 模型解析 | 存在缺陷 | 未命中模型名时，`AiFunctionFactory` 静默选择第一个 provider，可能调用错误模型 |
| Planner/native 能力协商 | 存在缺陷 | planner 能映射 `ai:*`，`NativeCapabilityRegistry.defaults()` 却不包含 AI 函数；能力只按字符串名称表达，无签名、版本或执行模式 |
| 精确向量计算 | 已闭环 | native SIMD distance、Substrait 映射和 SQL 距离函数链路完整度最高 |
| ANN 搜索 | 部分闭环 | HNSW、IVFPQ、DiskANN/Lance 和 worker interceptor 存在；索引缺失时回退 native |
| 分布式 Top-K | 存在缺陷 | worker 超时、fetch/parse/decompress 失败会被过滤，coordinator 仍可返回不完整结果，客户端无法判断覆盖率 |
| 全文与混合检索 | 部分闭环 | BM25 scorer、倒排索引、MATCH rewrite、RRF/linear/cross-encoder/cascade 节点存在；尚未收敛为可管理 Search Service |
| 自动 Embedding/backfill | 未接通 | 有 DDL/descriptor、checkpoint 和 `DistributedBackfillCoordinator`；`BackfillWorkerDispatcher` 无生产实现或 wiring，且协调器循环同步 dispatch，声明的并发度并未形成并行执行 |
| 多模态 | 部分闭环 | FILE/BLOB、OCR、文档解析、转写和图像描述原语存在；多个 E2E 将“runtime not available”视为可接受 |
| AI 预算 | 不存在（已清理） | 未接通的独立预算类、测试和 schema 字段已于 2026-08-21 删除；当前不声明预算能力 |
| AI 指标 | 不存在（已清理） | 未被生产 wiring 启用且 token 恒为 0 的 wrapper/collector 已于 2026-08-21 删除；通用 observability SPI 保留 |
| AI 安全 | 部分闭环 | 有 `AI_GUARD` 和通用 RBAC 基础；缺少每函数、每模型权限及统一的输入、工具输出、最终输出策略链 |
| 模型治理 | 部分闭环 | 有 Embedding model registry/JDBC 持久化；没有统一 inference model 对象、版本、生命周期、地域、价格和 RBAC |
| Semantic Cache | 部分闭环 | 有缓存原语；缺少模型/提示词/权限/数据快照指纹和可解释失效契约 |
| Semantic View / Text-to-SQL | 仅设计 | 没有受治理的业务语义对象和可验证 SQL 生成服务 |
| Agent | 不存在 | 对话和缓存原语不等于 Agent；代码层不存在 Agent、Tool、Thread、Run 和 policy 对象 |
| AI 评估 | 不存在 | 代码层不存在数据集版本、评估运行、答案/工具指标和回归门禁 |

### 3.1 已确认的高风险缺陷

#### P1：Embedding 类型与 provider contract 不完整

`AiEmbedFunction.invokeBatch` 没有验证 provider 返回的 embedding 数量是否等于请求数量，
错误 provider 可能造成结果错位或越界。函数还将向量编码为 JSON `VARCHAR`，规划器因而
无法在类型系统中验证 dimension、元素类型和向量函数兼容性。

#### P0：能力声明与实际执行不一致

AI 函数已进入 operator table 和 `FunctionMapper`，但默认 native capability 没有对应项。
当前能力模型只有函数名集合，无法表达：

- 参数和返回类型；
- scalar、aggregate、table、streaming；
- native、JNI callback、coordinator service；
- batch、multimodal、structured output；
- provider、模型和版本要求；
- deterministic、side effect 和 retry 语义。

这会让规划期验证、运行期注册和实际 worker 能力漂移。

#### P0：基础查询链路尚不可靠

现有 E2E 记录包含多 split 丢行、shuffle/JOIN 空结果、DML 失败和角色隔离失败。AI
Search、RAG 和 Agent 都依赖同一 SQL、dispatch、result completion 和安全链路。在这些
问题解决前，不能将 AI E2E 的局部通过解释为生产就绪。

#### P1：模型解析静默回退

请求不存在的模型时调用任意第一个 `InferenceProvider` 会破坏可重复性、成本控制、
数据驻留和模型 RBAC。模型解析必须是精确、可审计且 fail-closed 的。

#### P1：静默返回部分向量结果

`QueryVectorResultMerger` 将 worker 超时和结果解析失败转成空列表；只要仍有部分结果，
查询就可能成功。`VectorSearchTaskInterceptor` 也允许少数文件失败后返回部分结果。
Top-K 的语义因此从“全局 Top-K”变成未声明的“可用分片 Top-K”。

#### P1：backfill 和测试没有闭环

- Backfill dispatcher 没有生产实现，checkpoint 也不等于 durable job state machine。
- 多个 AI E2E 用 assumption 跳过模型不可用，或断言“非语法错误”即成功。
- `DispatchE2ETest` 使用常量真断言，不能证明 dispatch wiring。

#### 已完成清理：未接通预算与伪指标

2026-08-21 已删除独立 token budget enforcer、专属数据库迁移、AI metrics collector、
scalar function instrumentation wrapper 及其单元/E2E 测试岛。`AiFunctionFactory` 不再包含
无法由生产构造启用的 nullable metrics 分支。未来预算、usage 和 trace 只能作为统一 AI
invocation contract 的组成部分重建，禁止恢复旁路 provider 的独立实现。

## 4. Snowflake AI Native 能力基线

以下能力来自 Snowflake 官方文档。对标重点是可运营的对象生命周期，而不是 API 名称。

| 能力域 | Snowflake 当前做法 | Pangrova 差距 |
|---|---|---|
| AI SQL | AISQL 覆盖 complete、classify、filter、aggregate、embed、extract、similarity、transcribe、parse、redact、translate、token count | 函数广度接近原语层，但类型、aggregate、structured output、统一权限和计量不足 |
| 模型治理 | 模型表示为 `SNOWFLAKE.MODELS` 对象，支持 model-level RBAC、生命周期与可用性查询 | 模型名主要是字符串/provider 查找，缺少统一对象和 fail-closed 授权 |
| 函数权限 | account 级 `USE AI FUNCTIONS` 与逐函数 `USE AI FUNCTION`，另有 embedding 专用角色 | 缺少 AI capability privilege |
| Search Service | schema 对象，混合 vector + keyword + semantic rerank；管理 source query、refresh、target lag、primary key、index、suspend/resume、监控和预算 | 只有底层索引与查询组件，没有 service lifecycle、SLA 和运维面 |
| Semantic View | schema 对象，定义 logical table、relationship、fact、metric、dimension，可直接查询并供 Analyst 使用 | 缺失 |
| Analyst | 基于 Semantic View 将自然语言转换为受约束 SQL，使用业务指标和 verified examples | 缺失 |
| Agent | schema 对象，组合 Search、Analyst、custom tool、web、code、MCP 和 skills；有 thread、run 和版本 | 缺失 |
| Observability | Thread -> Turn/Trace -> Span，记录规划、LLM、tool、SQL、响应、token、latency、feedback；支持 MONITOR 和内容脱敏权限 | 当前无 AI 专属 telemetry；通用 observability SPI 保留 |
| Evaluation | 版本化 dataset/run；答案正确性、逻辑一致性、工具选择、工具执行和自定义指标；支持 run 比较 | 缺失 |
| Guardrails | account 级策略，扫描 prompt 和 tool output，记录信号、token 和成本 | `AI_GUARD` 是可选函数，不是不可绕过的执行策略 |
| Budget | Search 可按 tag 归因预算，阈值触发通知、撤权和 suspend；使用量可查询 | 当前无 AI 预算实现，需在统一 invocation contract 内重建 |

### 4.1 不应照搬的部分

Snowflake 也存在明确限制：预算执行有周期延迟，部分 Agent evaluation 尚不支持 MCP、
code execution 或 skills，Guardrails 依赖特定部署条件，模型和功能受地域约束。Pangrova
不应追求表面 API 对齐，而应坚持：

- 开放湖表和外部模型优先，不绑定单云模型目录；
- provider 可插拔，但解析、权限、成本和 telemetry 不可插拔绕过；
- 索引数据与源表 snapshot/sequence 显式关联；
- 所有降级都必须可见、可配置、可审计；
- 管理面元数据使用数据库持久化，数据面继续使用 native/vectorized execution。

## 5. 目标架构

### 5.1 分层

```text
SQL / REST / JDBC
        |
AI Database Objects
MODEL | SEARCH SERVICE | SEMANTIC VIEW | AGENT | EVALUATION DATASET
        |
AI Capability Catalog + Planner Validation
        |
AI Admission Pipeline
resolve -> authorize -> policy -> estimate -> reserve -> execute -> reconcile
        |
Execution Plane
native vector/full-text | worker provider gateway | coordinator orchestration
        |
Durable State
catalog metadata | job/run state | index manifests | checkpoints | telemetry
        |
Operations
RBAC | guardrails | budgets | traces | evaluations | suspend/resume
```

### 5.2 AI Capability Catalog

以统一 descriptor 替代散落的 operator、Substrait、ServiceLoader 和字符串集合：

建议定义如下 capability descriptor：

```text
AI capability descriptor
  name, version, kind
  argumentTypes, returnType
  executionMode: NATIVE | WORKER_CALLBACK | COORDINATOR_SERVICE
  modalities, batchSupport, streamingSupport
  deterministic, retryPolicy, sideEffect
  requiredPrivileges, requiredModelCapabilities
```

Coordinator 汇总 worker 启动时上报的 descriptor，按集群最小公共能力或明确的节点约束
规划。计划中携带 capability ID 和 version；worker 不匹配时在执行前失败，不做隐式回退。

### 5.3 Model 对象

新增 schema 级 `MODEL` 元数据对象，至少包含：

- provider、provider model ID、model version、capabilities、modalities；
- input/output schema、embedding dimension、distance metric；
- endpoint reference、credential reference，不存储明文 secret；
- region/data residency、lifecycle、deprecation time；
- input/output token price、request price、rate limits；
- timeout、retry eligibility、structured output support；
- owner、USAGE/MONITOR/ALTER 权限和 policy tags。

SQL 中使用对象标识符解析模型。字符串别名仅作为显式 catalog alias，未命中必须失败。

### 5.4 不可绕过的 AI Admission Pipeline

每次 provider 调用统一经过：

1. 解析 capability 和 model object；
2. 检查函数权限、模型 USAGE、数据对象权限和 tenant policy；
3. 对 prompt、FILE metadata 和 tool output 执行 guardrail/DLP policy；
4. 估算 token、请求和多模态成本；
5. 原子预留 tenant、user、query 和 AI object budget；
6. 使用 idempotency key 调用 provider；
7. 收集真实 usage、延迟、provider request ID 和错误分类；
8. 对账预留与实际成本；
9. 记录 lineage、audit 和 OTel span；
10. 对最终输出执行策略并返回 typed result。

该链路必须位于统一的 AI invocation service，SQL UDF、backfill、Search refresh、Analyst、
Agent 和 evaluation 禁止直接调用 provider。

### 5.5 Search Service 对象

将现有 vector、BM25、rerank 和 embedding 组件收敛为 schema 对象：

```sql
CREATE SEARCH SERVICE product_search
  ON lake.products
  PRIMARY KEY (product_id)
  TEXT COLUMNS (title, description)
  VECTOR COLUMN description_embedding
  EMBEDDING MODEL catalog.models.product_embed
  REFRESH TARGET_LAG = '5 minutes'
  RERANK = RRF
  CONSISTENCY = COMPLETE;
```

对象状态至少包括 CREATING、`BACKFILLING`、`READY`、`DEGRADED`、`SUSPENDED`、
`FAILED`。索引 manifest 绑定 source snapshot、schema fingerprint、model version 和覆盖率。
查询结果必须返回或记录：

- source/index snapshot；
- indexed partitions / expected partitions；
- stale age；
- ANN parameters 和 reranker；
- `COMPLETE`、`PARTIAL` 或 `FALLBACK_EXACT` 质量状态。

默认 `CONSISTENCY=COMPLETE`：任一分片失败则查询失败，或对缺失分片执行精确扫描后再
合并。只有显式 `ALLOW PARTIAL` 才能返回部分结果，并在协议和审计中标记。

### 5.6 Durable Embedding 与 refresh

Backfill 应改为持久化状态机，而不是 coordinator 内存 Future：

```text
PENDING -> SNAPSHOTTED -> PARTITIONED -> RUNNING
        -> COMMITTING -> READY
        -> RETRY_WAIT | CANCELLED | FAILED
```

每个 partition 有 durable lease、attempt、idempotency key、input snapshot、model version、
row count、success/failure 和 commit token。worker 只生成 embedding data files 与 commit
metadata；coordinator 使用 connector SPI 做单次原子 catalog commit。失败行进入 dead-letter
记录，达到阈值时 service 进入 `DEGRADED` 或 `FAILED`，不得悄悄跳过。

增量 refresh 使用表 snapshot diff 或 CDC，模型版本变化触发全量 rebuild 或双版本切换。

### 5.7 Semantic View 与 Analyst

新增 `SEMANTIC VIEW` schema 对象：logical table、relationship、fact、dimension、metric、
synonym、time dimension、default filter 和 verified query。创建时验证 join graph、聚合 grain、
metric type 和循环依赖。

Analyst 流程：

```text
question -> semantic retrieval -> candidate logical plan
 -> deterministic semantic compiler -> Calcite validation
 -> authorization + cost guard -> read-only execution
 -> result + generated SQL + cited semantic objects
```

LLM 不能直接输出即执行 SQL。必须经过 AST parser、对象 allowlist、只读限制、row/column
security、query cost limit 和 verified example regression。

### 5.8 Agent、Tool、Thread 与 Run

在 Search 和 Analyst 稳定后再引入：

- AGENT：版本化 instruction、model、tools、policies、limits；
- TOOL：Search Service、Semantic View/Analyst、受限 SQL、UDF、HTTP/MCP；
- THREAD：持久对话，拥有 tenant、user、role 和 retention policy；
- `RUN`：一个 turn 的状态机，支持 cancel、deadline 和 idempotency；
- `RUN_STEP`：plan、model call、tool call、SQL、response；
- `EVALUATION DATASET/RUN`：版本化输入、ground truth、expected tools 和 scores。

工具权限取调用者与 Agent owner 权限的受限交集，默认使用 caller rights。工具输出返回
模型前必须做 prompt-injection scan 和敏感数据策略检查。

### 5.9 AI Observability 与 Evaluation

采用统一层次：

```text
Thread -> Turn/Run = Trace -> Span
```

Span 类型包括 model、embedding、search、rerank、tool、SQL、guardrail 和 budget。公共属性：
tenant/user/role、query/run/tool ID、model object/version、provider request ID、tokens、cost、
latency、cache hit、quality status、source snapshot、error code。Prompt 和结果内容默认脱敏，
查看原文需要独立权限。

首批评估指标：answer correctness、groundedness、citation correctness、retrieval recall@K、
NDCG、tool selection、tool execution、SQL execution accuracy、policy violation 和 cost/latency。
Evaluation Run 固定 Agent、Model、Semantic View、Search Service 和 dataset 版本，支持版本间
对比并作为发布门禁。

## 6. 协议与模块改造

| 模块 | 主要改动 |
|---|---|
| `pangrova-spi` | 新增 capability descriptor、invocation request/result/usage、typed embedding、Search quality、model/search/agent privilege；provider SPI 返回真实 usage |
| `pangrova-parser` | MODEL、SEARCH SERVICE、SEMANTIC VIEW、AGENT、EVALUATION DDL；避免只添加无执行方 AST |
| `pangrova-planner` | capability-aware validation；AI 返回类型；Search logical node；Semantic compiler；AI cost annotations |
| `pangrova-protocol` | capability version、model object ID、invocation context、usage、quality status、partition coverage、run/step events；保持字段号兼容并双端生成 |
| `pangrova-metadata` | 新对象表、RBAC grants、job/run state、index manifest、budget ledger、evaluation records 和 migration |
| `pangrova-security` | per-capability/model/object 权限；prompt/tool/output policy；unredacted telemetry 权限 |
| `pangrova-coordinator` | capability catalog、admission service、budget ledger、durable refresh scheduler、Search/Analyst/Agent control plane、global quality enforcement |
| `pangrova-worker` | provider gateway、真实 usage、typed vector、durable partition worker、Search execution、OTel spans；移除 provider fallback |
| `pangrova-worker-native` | typed VECTOR/FILE 传输、exact fallback、BM25/vector fusion execution、quality metadata；JNI 和 symbol map 同步 |
| `pangrova-observability` | AI trace schema、redaction、cost/token metrics、queryable events |
| `pangrova-governance` | model/prompt/tool/data lineage、retention、policy tags |
| `pangrova-testing` | provider contract suite、TestContainers provider、真实 full-chain AI/Search/Agent tests、evaluation golden datasets |

核心协议应返回结构化 usage，而不是由 wrapper 猜测：

```text
AiInvocationResult
  value
  modelObjectId, resolvedModelVersion
  providerRequestId
  inputTokens, outputTokens, billableUnits, estimatedCost
  finishReason, cacheStatus
  policySignals
```

## 7. 分阶段实施计划

### Phase 0：正确性和文档真实性（2-4 周）

目标：不增加新产品面，先让现有承诺可信。

- 为 `AiEmbedFunction.invokeBatch` 增加 provider 返回数量校验，并补齐边界测试。
- 删除 inference provider 未命中回退，使用明确的 unknown-model 错误码。
- 统一 operator table、FunctionMapper、worker registry 和 native capability 的生成源。
- 为分布式向量结果加入 coverage/quality；默认禁止静默 partial。
- 已删除未接通的 token budget、恒为 0 的指标包装及不真实完成度声明。
- 修复/重写允许 runtime failure、assumption 跳过和常量真断言的 AI/dispatch E2E。
- 修复已登记的 result completion、shuffle 和 security P0，再作为 AI 基线门禁。
- 更新 `18-ai-native-architecture.md` 和 2025 报告，明确 implemented/proposed 状态。

退出门槛：

- AI function registry contract test 在 planner、protocol、worker 三侧完全一致；
- batch normal/null/mixed-model/FILE/error 路径通过；
- 不存在 silent wrong-model 或 silent partial Top-K；
- AI E2E 未因 provider/model 不可用而跳过；
- 全项目 8 步验证链通过，已知 AI 依赖的基础 E2E 为零失败。

### Phase 1：统一执行与治理（4-8 周）

目标：所有 AI 调用只有一个受治理入口。

- 落地 Capability Catalog 和 MODEL 对象。
- 建立统一 AI invocation service、精确模型解析和 provider contract。
- 在统一 invocation service 内新建 function/model RBAC、guardrail/DLP、预算预留/对账，
        不复用已删除的旁路实现。
- 基于 provider 返回的真实 usage 建立 AI trace/span、redaction 和 queryable usage history；
        provider 不提供 usage 时记录 `UNKNOWN`，不得写入 0 冒充真实值。
- 将 `AI_EMBED` 返回改为 typed VECTOR；提供显式 JSON 转换函数用于兼容展示，不保留
  隐式双语义。
- 为 complete/extract/classify 增加结构化输出 schema 和校验失败语义。

退出门槛：

- 任一 SQL、backfill 或内部服务都不能绕过 admission pipeline 访问 provider；
- 并发预算测试证明无 oversell，失败/取消能正确释放或对账；
- 权限矩阵覆盖函数、模型、数据和 telemetry 原文；
- 每次调用可追溯到 model version、provider request、tokens、cost 和 policy decision。

### Phase 2：Search Service 与 Embedding 生命周期（8-12 周）

目标：将检索能力从执行技巧升级为可运营数据库服务。

- 实现 SEARCH SERVICE DDL、catalog、RBAC、status、suspend/resume。
- 实现 durable partition job、worker dispatcher、lease/retry/dead-letter 和原子 commit。
- 统一 vector、BM25、filter、RRF/cross-encoder 和 exact fallback。
- 实现 snapshot freshness、target lag、coverage 和 quality contract。
- 建立 recall/NDCG、refresh lag、query latency、index size 和 cost dashboard。

退出门槛：

- 多 worker 故障下 `COMPLETE` 查询要么精确补全，要么失败，永不静默缺结果；
- coordinator 重启、worker 重试和重复消息不产生重复 commit；
- source 更新到可搜索满足 target lag SLO；
- 10M/100M 规模基准给出 recall、P95/P99、refresh throughput 和成本基线。

### Phase 3：Semantic View 与 Analyst（8-12 周）

目标：提供可信、可复现、可授权的自然语言数据分析。

- 实现 Semantic View DDL、validation、information schema 和 lineage。
- 实现 deterministic semantic compiler 和 read-only Analyst service。
- 引入 verified queries、歧义澄清、SQL safety 和 query cost guard。
- 建立真实业务问题数据集和 execution-accuracy 门禁。

退出门槛：

- 生成 SQL 100% 通过 parser、RBAC、只读和 cost guard；
- 指标 grain、join path、row/column policy 有确定性测试；
- golden dataset 达到约定 execution accuracy，且每个答案可展示 SQL 和语义引用。

### Phase 4：Agent、工具和评估（12-16 周）

目标：在可信工具之上提供持久、可观测、可评估的 Agent。

- 实现 Agent/Tool/Thread/Run/RunStep 对象和版本。
- 先支持 Search、Analyst 和受限 SQL 工具，再扩展 HTTP/MCP。
- 实现动态 planning、deadline、cancel、idempotency 和 caller-rights sandbox。
- 实现 evaluation dataset/run、答案、检索、工具和自定义指标。
- 建立 canary、版本比较、反馈和自动回滚门禁。

退出门槛：

- 每个 run 都有完整 trace，工具调用可审计并受 caller 权限限制；
- prompt injection 测试证明恶意 tool output 不能提升权限或泄露数据；
- Agent 版本必须通过 evaluation threshold 才能晋升；
- 预算、超时和 cancel 能终止后续 model/tool 调用并完成对账。

## 8. 测试与验收矩阵

| 层级 | 必须验证 |
|---|---|
| SPI contract | provider capability、batch cardinality、usage、timeout、rate limit、idempotency、structured output |
| Unit/property | vector dimension/NaN、model resolution、budget concurrency、state transition、fusion determinism、protocol round-trip |
| Integration | PostgreSQL metadata、provider TestContainer、credential reference、RBAC、refresh recovery、atomic commit |
| Native | VECTOR/FILE schema、distance、exact fallback、BM25/fusion、JNI exception 和 ownership |
| E2E | SQL -> planner -> protocol -> JNI -> provider/native -> result；Search create/refresh/query；Analyst question -> SQL -> result |
| Failure | provider 429/5xx/timeout、worker loss、coordinator restart、partial index、stale snapshot、budget exhausted、policy block |
| Security | per-function/model privilege、row/column policy、prompt injection、tool output injection、telemetry redaction、secret absence |
| Quality | recall@K、NDCG、groundedness、citation、execution accuracy、tool selection/execution |
| Performance | batch throughput、P95/P99、index build/refresh、memory、token/cost per successful answer |

AI 测试不得把以下情况计为通过：容器/模型不可用导致 assumption skip、只验证 SQL 能解析、
捕获任意运行时异常后接受、常量真断言、未验证行数和值、未检查 partial quality。

## 9. 关键决策与优先级

### 立即执行

1. 修复模型 fallback、向量 partial 和 embedding provider contract 三个正确性问题。
2. 让 capability registry 成为单一事实源。
3. 已删除 budget/metrics 伪实现；后续只在统一 invocation contract 内重建 usage、budget 和 trace。
4. 修复基础 result/shuffle/security E2E，并清理虚假 AI E2E。

### 暂缓

- 在 Search Service 前扩展更多 ANN 算法；
- 在 Semantic View 前实现通用 Text-to-SQL；
- 在 admission、RBAC、trace 和 evaluation 前实现 Agent/MCP；
- 在 Phase 4 前添加 Agent、Tool、Thread、Run、Evaluation 空壳类型或无调用方 SPI；
- 在 typed VECTOR 前继续用 JSON `VARCHAR` 扩散 embedding API；
- 用新的 fallback、重试或缓存掩盖基础执行正确性问题。

### 成功标准

Pangrova AI Native 的目标不是“仓库中存在更多 AI 类”，而是用户可以创建受治理的
模型和 Search/Semantic/Agent 对象，并对每次调用回答以下问题：

- 使用了哪个能力、模型、版本和数据快照？
- 谁被授权调用，应用了哪些数据和内容策略？
- 是否返回完整、最新且满足声明质量的结果？
- 消耗了多少 token、请求、计算和费用？
- 失败、重试、降级和取消发生在哪里？
- 该版本在固定数据集上的质量是否比上一版更好？

只有这些问题都能由系统本身、而不是日志猜测或文档承诺来回答，Pangrova 才形成完整
的 AI Native 数据库闭环。

## 10. 官方对标资料

- [Snowflake Cortex AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [AI Functions privileges and model access](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access)
- [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Cortex Search resource budgets](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-resource-budgets)
- [Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Cortex Analyst](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst)
- [Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [Cortex Agent monitoring](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-monitor)
- [Cortex Agent evaluations](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-evaluations)
- [Cortex AI Guardrails](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-ai-guardrails)