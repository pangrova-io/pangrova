---
title: "AI Native 全能力端到端审计与整改规格"
description: "Internal evidence-based review and remediation roadmap; not a public feature commitment"
---

# Pangrova AI Native 全能力端到端深度 Review

> 审计基线：2026-08-28 当前工作区（包含现有未提交改动）
>
> 报告性质：内部事实审计、根因分析与后续实现规格，不代表公共功能承诺
>
> 审计链路：入口 → 规划 → 协议 → JNI/Native → 分布式执行 → 结果 → 治理 → 测试
>
> 本轮边界：未修改公共 API、协议、JNI、生产代码或公共文档；未执行完整八步回归和 ASan

## 1. 结论先行

Pangrova 已有数量可观的 AI/Vector 组件，但尚未形成“AI Native 数据库产品”的端到端闭环。
当前最成熟的是 VECTOR 类型及 native SIMD 精确距离原语；AI SQL、自动 Embedding、混合检索、
分布式 Top-K、多模态、Semantic Cache 和治理链路均存在未接通、静默降级或错误结果风险。

本次复证得到六个 P0：

1. 15 个 AI SQL 函数只注册到 Java 内存桥，未注册到 C++ `NativeUdfRegistry`。
2. 未命中的模型名会静默选择任意 provider，可能调用错误模型、区域和计费对象。
3. worker/coordinator 会过滤分片超时或解析失败，仍以成功返回未声明的部分 Top-K。
4. Hybrid/Rerank 的 coordinator、worker、factory 配置协议不一致，并存在两次排序方向反转。
5. BM25 Java 声明与 C++ 导出 JNI 符号不一致，且异常可把整个 native runtime 标记为不可用。
6. FILE URL 可直接读取任意 HTTP(S) 地址且无大小上限；inline FILE 返回未真实落盘的 URI。

因此当前对外口径应是：**具备部分 AI Native 原语和实验性组件，不具备完整可运营的 AI
SQL、Search、Semantic 或 Agent 产品闭环。** 生产修复必须从正确性与测试真实性开始。

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

## 3. 声明—实现账本

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
| 10 | RBAC、Guardrail、成本、审计、可观测性、评估、Semantic View、Agent | 仅设计 | 有通用 RBAC/审计原语和普通 `AI_GUARD` UDF；无不可绕过的 AI admission、usage ledger、对象模型、评估或 Agent |

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
| Semantic View / Analyst | 仅设计 | 无 catalog 对象、语义验证、NL-to-SQL 或 verified query lifecycle |
| Agent / Thread / Run / Tool | 仅设计 | 无生产对象和编排生命周期 |
| AI Evaluation | 仅设计 | quality sampler 不是版本化 dataset/run/metric/regression system |

## 4. 十条链路复证摘要

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
AST，但 bootstrap 只注册 CREATE/DROP/SHOW。`EmbeddingBootstrap`、`AutoEmbedRewriteRule` 和
dispatcher/checkpoint 组件没有形成生产链。

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

FILE 是 ROW/STRUCT sugar。`FILE_FROM_BYTES` 生成 `blob://inline/<sha>` 却未写入对象；两个
constructor 都带当前时间但 metadata 声明 deterministic。URL 只校验 scheme，provider 随后
直接 `openStream().readAllBytes()`，无 private-IP/redirect/allowlist/size/MIME/checksum/timeout，
形成 SSRF、OOM 和不可重放风险。已有 blob/audit/security helper 未找到完整 production wiring。

### 4.7 Semantic Cache、自优化与训练数据

`QueryPipelineExecutor` 只在 completion 后写 cache，且写入 `Set.of()` 表依赖和 `new byte[0]`
结果。production source 中未发现 `findSimilar` caller。两种 cache 都忽略 principal；HNSW 先跨
tenant ANN 再过滤 tenant，召回和隔离均不可证明。Feature Store 只有 SPI；训练服务固定采用
disabled defaults，且 coordinator 扫描路径违反模块边界。advisor/scheduler 仍是局部组件。

### 4.8 治理、Semantic View 与 Agent

`AI_GUARD` 是普通可选 UDF，不是不可绕过的 policy gate。provider usage 没有统一进入 cost
ledger、budget、audit/trace，未知 usage 可被表示为 0。没有 AI function/model privilege、
Semantic View、Analyst、Agent/Thread/Run/Tool 或版本化 Evaluation Dataset/Run。

## 5. P0–P3 发现与修复规格

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

### P3-01：Semantic View 与 Analyst 缺失

- **影响/触发**：无法在受治理业务指标和 verified queries 上提供可信 NL-to-SQL。
- **调用链/根因**：当前无对象和服务；尚未建立语义、验证、clarification 和 feedback lifecycle。
- **证据**：repository-wide catalog/parser/service audit 无生产对象。
- **修复/依赖**：Phase 3 新建 Semantic View、relationship/fact/metric/dimension、verified query 与 constrained generation；依赖 Phase 0–2、RBAC、validator、evaluation。
- **验收**：DDL/RBAC/versioning 完整；golden question SQL 通过语义/权限校验；歧义返回 clarification。

### P3-02：Agent、Observability 与 Evaluation 缺失

- **影响/触发**：无法安全编排 Search/Analyst/tool 或判断质量回归。
- **调用链**：不存在 Agent→Thread→Run→Trace→Tool→Evaluation run。
- **根因/证据**：上游对象未闭环；repository-wide object/reference audit，现有 sampler 只是局部 metric。
- **修复/依赖**：Phase 4 建立 versioned Agent/Tool/Thread/Run、policy、trace/span、dataset/run/metric/judge；依赖 Phase 1–3。
- **验收**：越权、预算、prompt injection、cancel/retry 状态确定；eval 可比较版本并阻断回归。

## 6. 文档漂移建议

`docs/architecture/ai-native.md` 开头声明为目标架构，但大量代码片段和模块表仍容易被当成
现状。后续应逐节标 `Current / Target / Gap` 并链接能力账本。

公共 SQL 文档把 native HNSW/IVFPQ、自动 ANN/fallback、自动 Hybrid RRF、Cross-Encoder/
Cascade、INSERT 自动 embedding，以及全部 AI 函数、多模态、ALTER model、SYNC/ASYNC、
backfill/cancel/status 和 FILE 自动属性写成已提供能力。本轮遵守边界，未修改公共文档；
Phase 0 应建立文档 conformance：只有 public-entry E2E 为 PASS 才能使用完成态表述。

## 7. Snowflake 官方产品闭环基线

仅使用截至审计时可访问的 Snowflake 官方一手资料。对标重点是对象、权限、生命周期、成本和
评估闭环，不是复制 API。

| 能力域 | Snowflake 当前闭环 | Pangrova 差距 |
|---|---|---|
| AI SQL | complete/classify/filter/aggregate/embed/extract 等文本、图像、文档函数和计费 token | factory 存在但 SQL→native 未接通，类型/usage 缺失 |
| Model/RBAC | `SNOWFLAKE.MODELS` 对象，account/per-function privilege 与 model RBAC | model 是字符串/fallback；无对象 lifecycle/RBAC |
| Search Service | schema object，vector+keyword+semantic rerank；source query、target lag、refresh、PK、suspend/resume、cost | 只有底层组件，无 service object/SLA/freshness |
| Semantic View | logical table、relationship、fact、metric、dimension，可查询并供 Analyst 使用 | 缺失 |
| Analyst | semantic view/model、clarification、multi-turn、verified query metadata、feedback | 缺失 |
| Agent | Agent object 组合 Analyst、Search、custom tools；thread 与 async run | 缺失 |
| Observability | Thread→Turn/Trace→Span，记录 LLM/tool/SQL、token、latency、model、error，受 MONITOR/内容权限控制 | 无 AI trace 对象/敏感内容权限 |
| Evaluation | dataset/run、answer correctness、logical consistency、tool selection/execution、自定义 judge | 缺失 |
| Guardrail | 当前范围聚焦 Agents/CoCo/CoWork，扫描 tool output 的 indirect prompt injection 并记录使用 | 普通 `AI_GUARD` 可绕过；不应夸大对标范围 |
| Budget | Search 支持 tag-based monthly budgets/threshold actions；官方明确执行可延迟 2–8 小时 | 无 AI/Search ledger/admission |

官方资料：

- [AI SQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)
- [AI SQL privileges and model access](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access)
- [Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)
- [Cortex Search resource budgets](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-resource-budgets)
- [Semantic Views overview](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Semantic View editor](https://docs.snowflake.com/en/user-guide/views-semantic/editor)
- [Cortex Analyst REST API](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst/rest-api)
- [Manage Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-manage)
- [Monitor Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-monitor)
- [Cortex Agents evaluations](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents-evaluations)
- [Cortex Analyst evaluations](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-analyst-evaluations)
- [Cortex AI Guardrails](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-ai-guardrails)

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
  -> input/file/tool-output policy -> estimate/reserve budget
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

### 8.5 产品对象边界

- `SEARCH SERVICE`：source query/PK/vector/text/model/index/refresh/snapshot/freshness/rerank/RBAC/budget/suspend。
- `SEMANTIC VIEW`：logical tables/relationships/dimensions/facts/metrics/synonyms/verified queries/version/RBAC。
- `AGENT`：versioned instructions、typed tools、model/data allowlist、budget、guardrail；Thread/Run 为 durable async state。
- `EVALUATION DATASET/RUN`：immutable inputs/evidence/object versions/metrics/judges/traces/comparison/release gate。

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

### Phase 3：Semantic View 与 Analyst

Semantic View catalog/RBAC/version、verified queries、clarification、constrained generation/validation、
feedback/evaluation。**退出门槛**：语义、SQL 正确性和权限 golden suite 达标，歧义不猜测。

### Phase 4：Agent 与评估

Agent/Tool/Thread/Run、async orchestration、policy/budget/guardrail/cancel/retry/trace、evaluation
dataset/run/metric/judge。**退出门槛**：Agent 只调用 Phase 1–3 已治理对象，质量和安全可回归。

## 10. 本次实测记录

### 10.1 环境

| 项目 | 结果 |
|---|---|
| OS/arch | macOS arm64 |
| Java | JDK 21.0.12；项目要求 JDK17，当前环境不合规 |
| Maven | 3.9.16 |
| CMake | 4.4.2，C++20 Release build |
| Docker | daemon 29.5.3；提升 sandbox 权限后可访问 |
| Native package | 本轮 Release rebuild 后已打包到 worker resources |

### 10.2 结果

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

### 10.3 命令记录

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

### 10.4 关键证据路径索引

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
| Native | Vector/Lance/DiskANN/UDF/BM25/Capability/JNI | Java→JNI smoke + CTest；optional skip 单列 |
| Distributed fault | timeout/corruption/stale/retry/fallback | status/coverage/snapshot 与 oracle 一致 |
| Provider | count/dimension/order/timeout/rate-limit/usage | 无错位/部分提交；usage 来源明确 |
| Durable jobs | crash/restart/lease steal/cancel/duplicate | 幂等、可恢复、原子 publish，无 checkpoint 丢失 |
| Security | model/RBAC/SSRF/redirect/oversize/cache isolation | 网络/执行前 fail-closed，无跨域/跨租户 |
| E2E | JDBC→planner→protocol→worker→packaged JNI→provider→result | 生产入口真实执行；断开任一 wiring 必 FAIL |
| Release | 八步 build pipeline + CI gates + ASan | 生产修复阶段全部通过，环境阻塞不算通过 |

## 12. 完成定义

AI Native 不能以类、SPI、AST 或单测数量验收。某项能力只有同时满足以下条件才可升级为
“已闭环”：

1. 公共入口真实可达，planner/protocol/worker/native/provider 使用同一版本化能力定义。
2. 未知模型、能力漂移、权限、预算和安全策略 fail-closed。
3. batch、分片、rerank、fallback、snapshot 和 freshness 对客户端可观测。
4. 作业、索引和服务对象具备 durable lifecycle、owner、RBAC、审计、取消和恢复。
5. usage、成本、错误、lineage、trace 和评估可查询并可作发布门禁。
6. JDK17、Release native、packaged JNI、Docker/provider 的 public-entry E2E 为 PASS；任何
   SKIPPED/BLOCKED 保持原状态，不折算通过。

本报告已完成本轮计划要求的静态审计、十条链路复证、官方基线、P0–P3 根因与修复规格、
阶段路线和实测记录。后续应以 Phase 0 为新的生产修复任务起点。
