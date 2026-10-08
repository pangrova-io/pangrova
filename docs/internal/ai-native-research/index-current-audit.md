# 索引、混合检索、模型重排与自动 embedding：真实链路核查

核查日期：2026-10-07。依据当前工作树源码及官方文档。本轮仅做只读核查并新增本文件，
没有修改生产源码、启动构建或运行测试；下文测试分类是对测试实现的核查，不能替代本轮执行结果。
固定 Velox revision 为 `3da0390118fa4b0addbfbdeb03f438ec1b1237a6`
（`pangrova-worker-native/bin/apply-velox-patches.sh` 与 CMake provenance gate）。
当前索引检索是 Pangrova 自有 Native artifact 实现，不能把最新版 Milvus/Snowflake 的能力
或最新版 Velox API 直接当作本项目已经具备的能力。

## 1. 官方方案的准确边界

| 官方来源 | 核实能力及限制 | 对本项目的判断 |
|---|---|---|
| [Snowflake Cortex Search overview](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) | 向量、关键词和语义重排组合；支持自动维护及用户提供向量；源查询需满足增量刷新约束，配置合格主键可只处理变化行。服务按 owner's rights 执行。 | 可参考声明式服务、受治理模型与增量维护；不能复制其权限模式覆盖 Pangrova 的 invoker/tenant/行策略，也不能把目标刷新延迟解释为已发布的精确快照。 |
| [Cortex Search scoring](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-customize-scoring) | 普通查询默认语义重排，可配置组件权重或关闭重排；`CORTEX_SEARCH_BATCH` 不支持重排，会忽略相应设置；查询 embedding prefix 有模型目的语义。 | 必须分别验收查询、批量与维护路径；不能承诺所有模式均重排，或套用供应商延迟作为本地性能数据。 |
| [Milvus hybrid search](https://milvus.io/docs/multi-vector-search.md) | 每条 ANN 路径先产出有限候选，再进行融合；不同字段有各自搜索请求。 | 本项目需要每条 source 的表达式和查询张量；把同一向量复用给多个字段不构成多向量能力。 |
| [Milvus BM25](https://milvus.io/docs/full-text-search.md) | 分析器处理原始文本，BM25 function 建立稀疏表示并进行词法评分，不需要神经 embedding 模型；实体有唯一主字段。 | 本项目 analyzer、代际全局词频/文档长度统计与唯一物理行身份必须共同进入绑定；不能把 BM25 当成模型调用。 |
| [Milvus weighted ranker](https://milvus.io/docs/weighted-ranker.md) | 异构评分先归一化；当前文档描述 arctan，并允许配置 `norm_score`。2.6+ 使用 Function API。 | Pangrova 当前采用每 source 候选集合的 min/max 归一化，数值语义不同；不能声称结果与 Milvus 完全一致，也不能不经过契约变更就替换归一化方法。 |
| [Milvus RRF](https://milvus.io/docs/rrf-ranker.md) | 按各检索源的排序位置融合，常用参数 k=60。 | RRF 是秩融合，不是模型语义重排；k=60 是可校准起点，无法证明所有数据集最优。 |
| [Milvus model ranker](https://milvus.io/docs/model-ranker-overview.md) | 文档当前导航为 3.0.x，页面标注兼容 2.6.x；候选 query-document pairs 交给外部模型服务评分再排序；限制一个 VARCHAR 字段，不支持 grouping search。 | 独立推理服务与数据库内候选/排序有清晰责任边界。当前 Worker HTTP provider 已存在，但独立服务迁移尚未完成。 |

上表是架构对照，不是某一产品绝对最优的证明。质量、延迟、内存和刷新成本必须用本项目
实际授权数据、真实模型与固定版本测量；禁止用接口相似或几个小数据测试代替这些证据。

## 2. SQL → Planner → 绑定：已接通与尚未接通

```text
PangrovaSql.g4 / AST
 -> PangrovaPlanner + VectorSearchRewriter / RerankerInjectionRule
 -> PlanNodeConverter: RetrievalTopKPlanHint + Substrait
 -> DispatchContextEnricher + RetrievalIntentCollector
 -> RetrievalArtifactResolver / PublishedArtifactBindingResolver
 -> RetrievalPlanBinder + RerankPlanBinder + TaskRequestFactory
 -> Worker task admission + Native plan/artifact validation
```

语法入口位于 `pangrova-parser/src/main/antlr4/org/pangrova/parser/PangrovaSql.g4`：
`RERANK BY RRF / LINEAR_COMBINATION / CROSS_ENCODER / CASCADE`，以及
`GENERATED ALWAYS AS (AI_EMBED('model', columns...)) EMBEDDING MODE SYNC|ASYNC`。
语法存在不意味着所有模式均已执行接通。

`pangrova-planner/.../vector/VectorSearchRewriter.java` 识别单一升序 distance、正 LIMIT、
literal 向量及索引候选；没有匹配索引则保留普通 Native distance + ORDER BY/LIMIT。
它设置候选放大与 ANN_THEN_FILTER 等规划提示，但
`vector/VectorHybridSearchPlanner.java` 的策略选择没有生产调用方，不能把其单元测试
解释为运行时自动选择过滤 ANN 的证明。

`planner/embedding/QueryTextEmbedRule.java` 仅产生 QueryEmbedDirective；生产源码没有调用方。
因此 `distance(vector_column, 'natural language')` 的自动查询 embedding 不能标记完成。
真实接通的是显式 `AI_EMBED` 及受治理的 ASYNC 写后物化；查询目的和文档目的的真实 provider
语义已有 Ollama SQL 测试。不能在 Coordinator 加一次模型调用就宣称补齐 Native 路径。

`planner/rerank/RerankerInjectionRule.java` 支持显式重排，也把 distance 排序与正向 conjunctive
MATCH 组合识别成隐式 RRF。OR/NOT、GROUP/HAVING 等不在该支持集合。
`BM25_SCORE` 必须与同列/同 query 的正向 MATCH、正确排序方向配对。
多 vector 的 AST 不是运行时能力：`RetrievalIntentCollector` 明确拒绝缺少每 source
表达式绑定的多向量重排，避免复用一个查询张量。

`coordinator/query/execution/DispatchContextEnricher.java` 查找 tenant-scoped READY 描述符，
绑定 index generation 与 index snapshot；BM25 要求 BM25、standard-v1 和非空索引快照。
`query/execution/retrieval/RetrievalIntentCollector.java` 只为没有 serializedPreFilters 的
vector/full-text 节点产生 artifact intent。因此现有带 category/score 过滤的 vector SQL
不证明过滤 ANN：它们在 dispatch 前选择 Native exact 路径。

`planner/substrait/PlanNodeConverter.java#createRetrievalTopKHint` 要求正 LIMIT≤100000、
无 offset、单 score、DOUBLE score 和全 scalar candidate schema；仅投影 score 或包含复杂
类型时不生成此 hint。其 stableKeys 是所有非 score 投影字段，未绑定唯一物理行身份。
`Native RetrievalMergeOperator::encodeStableKey` 又以 `Vector::toString(row)` 加长度/NULL
标记编码这些字段。两个不同物理行投影相同，可能在单 source 被当作重复契约失败，或在跨
source 融合中被合并；投影含 source-specific 值也可能使同一行无法合并。它们都不满足
SQL bag semantics。应先补 canonical 身份再扩大混合检索支持范围。

## 3. 发布索引与 exact 路径的生产边界

`coordinator/artifact/PublishedArtifactBindingResolver.java` 解析实际 DataSplit 的 source、
snapshot、dataObject，解析唯一 PUBLISHED generation，核对 manifest、格式/role、完整覆盖
与对象 checksum。它输出未分区模板；真正 task partitioning 再绑定本任务对象并签名。
分区物化时 publication 不再匹配会失败；不存在吞错后换另一代索引的运行时路径。

`coordinator/query/execution/dispatch/dag/retrieval/RetrievalArtifactResolver.java` 的 nullable
resolver 是真实 optional bootstrap 分支：`bootstrap/module/IndexingModule` 在缺少完整
IndexingBootstrap 时不绑定 IndexAwareSplitFetcher；`DAGDispatchEngine` 接收 null resolver。
即使 fetcher 存在，`IndexAwareSplitFetcher` 的 artifactBuildStore 可空，也会使 resolver 为空。
它并非测试专属入口；这里确认的是生产可达分支，不声称标准部署一定关闭 durable store。

对 vector，无 intent、resolver 为空、未发布或 snapshot/coverage 不符，绑定器在 task 发出前
编译 exact Native plan。BM25 缺少完整 immutable artifact 则失败。`RetrievalPlanBinder`
发出的运行时契约为 `FALLBACK_FORBID`：选择了 ANN 后遇到损坏、授权变化或执行失败，不得
临时改 exact 或其他模型并返回成功。所需 recall profile（如 HNSW/DiskANN .95、IVF_PQ .90）
是计划声明与合法性检查，源码没有在线 Recall/NDCG 计算器证明实际达到该数值。

`IndexAwareSplitFetcher#applyIndexFiltering` 的另一条路径是数据跳过优化：授权类异常重抛，
其他评估异常返回原始 splits 并保留 Native WHERE。这增加 I/O，并不执行 ANN，也不是损坏
ANN artifact 的重试策略。后续应显式命名/记录规划决策，且验证所有权限异常类型均归属
拒绝分支，不能机械把两种 exact/skip 路径互相替代。

## 4. Native ANN / BM25 / fusion / rerank → Flight

`pangrova-worker-native/src/executor/driver/PlanExecutionDriver.cpp` 在 Velox 转换前调用
`artifact/ArtifactRetrievalPlanner.cpp`。这里是真实索引执行入口，没有名为 VecSearch 的
独立现成生产算子可直接据名称宣称接通。

ArtifactRetrievalPlanner 校验 canonical capability、query tensor FLOAT32、维度/有限值、
索引格式/metric/完整覆盖与预算；ImmutableArtifactCache 下载并保留 lease。每个文件/对象
通过 HNSW、IVF_PQ、DiskANN 或 BM25 查候选，使用 raw vectors 做匹配 metric 的 exact rescore，
通过 ROW_LOCATORS 映射物理 row position。BM25 使用 generation-wide corpus statistics。
`velox/connector/PangrovaVeloxConnector.cpp` 将允许的物理行位置及检索评分带入实际 Native
数据扫描；未覆盖 split、维度/metric/校验损坏等失败，不切换执行器。

`velox/plan/SubstraitRetrievalSourceConversion.cpp` 检查绑定并生成 partial TopN / gather /
TopN；跨 worker 使用 Native shuffle。`RetrievalMergeOperator.cpp` 与
`retrieval/RetrievalFusion.cpp` 在 Native 收集有界候选、按 source 全局 score/rank 排序，
执行 RRF 或 linear fusion，再按 stable key 决定 tie。Coordinator 传递 splits、计划、绑定与
任务状态，不承担这个候选排序或融合。

静态可确认的独立数学缺口：linear 仅检查输入 finite，`(max-min)` 在合法 DOUBLE 极端值
±1e308 时可溢出为 infinity，分子也可能 infinity，使 normalized/fused score 为 NaN；finish
未再次检查 finite，排序比较器因而失去有效排序语义。当前 FLOAT32 vector/非负 BM25 的常规
数值范围不等于任意 DOUBLE 输入范围；这不是已复现生产事故，但与其声明的 finite DOUBLE
契约不一致，应以真正 C++ 数学 oracle 修复，不能靠降低测试数值范围掩盖。

`velox/plan/SubstraitRerankConversion.cpp` 验证精确 model/version/signature、VARCHAR query/doc、
DOUBLE 输出、候选预算、provider 幂等和禁止 fallback，再生成 governed rerank scalar call。
实际调用仍是 Native → JvmScalarFunction → NativeFunctionBridge → Worker AiInvocationService
→ OpenAiCompatibleRerankingProvider 的真实 HTTP `/v1/rerank`，然后 Native TopN。
该 endpoint 是 provider 兼容约定，不应叫做 OpenAI 官方标准接口。
Worker 验证可信 task/operator admission、模型身份、授权、输入预算与响应数量/索引/finite
score。Coordinator 只选择/绑定已广告模型；其 inventory 匹配本身不能代替执行授权。
独立推理服务及 JNI AI 回调的整体替换见 [inference-current-audit.md](inference-current-audit.md)。

最终通过 `src/result/FlightResultServer.cpp` 和 ResultBatchStreamReader 返回真实 Arrow batches。
“排序结束”不等于“client 已消费完”或“provider owner 已 drain”；当前共用结果/推理生命周期
必须保留终态、EOF、decode、取消和资源所有权的独立证据。

## 5. 写后维护、生成值与最终发布

`coordinator/index/ddl/DurableIndexBuildService.java` 为 VECTOR HNSW/IVF_PQ/DISK_ANN 和 INVERTED
BM25 产生 snapshot-pinned Native jobs；完整源快照分页 SOURCE_MAP，BM25 有有界 reduction 树
和 GENERATION_FINALIZE 汇总 corpus stats。`IndexGenerationRefresher` 通过 CAS 开新代际，
Native 分支重新枚举完整快照构建 replacement；这是 commit-driven 全量代际更新，不能声称
已有变更行 embedding/索引 delta 复用。

`ai/embedding/GeneratedValueMaterializationCoordinator.java` 从真实 COMMITTED 写日志锚点
核验 tenant、principal、fingerprint 和精确声明，通过注册的 catalog listener 处理 ASYNC。
SYNC 在任何 connector mutation 前明确拒绝，尽管语法与 planner rewrite 类存在。
`DurableGeneratedValueBuildService` 编译全源 snapshot 的 Native jobs，冻结精确模型身份、
输出维度、document purpose、主体/授权和定义摘要。它传计划而非行数据。
`pangrova-streaming/.../cdc/StreamEmbeddingInterceptor.java` 没有生产 wiring，不能用其回调
单元测试补作 CDC 自动 embedding 证据。

`DurableArtifactBuildDispatcher` 管租约、fence、重新授权、stage DAG 和真实 worker 报告；
`DurableArtifactPublisher` 取得 publication claim。`JdbcIndexArtifactPublicationStore` 和
`JdbcGeneratedValueArtifactPublicationStore` 在同一 PG transaction 调用
`ArtifactCurrentAuthority.requireCurrent`、`ArtifactExecutionFence.requirePublication`，锁定
owner/epoch/租约，校验完整输出，然后更新 manifest、active generation 和 job=PUBLISHED。
CurrentAuthority 还锁定并复核当前 tenant、deferred authorization、模型权威。旧 worker 的
成功消息、页面完成或缓存中的 grant 都不能越过最终发布权威。

`artifact/GeneratedValueReadResolver.java` 用原/历史 snapshot 的声明、schema 和精确已发布
sidecar 绑定 overlay。没有相应 published generation 则不加 overlay，读取基础列；此时不在
读路径自动发模型请求。已有 sidecar 的 `GeneratedValuesOverlayOperator` 按 dataObject+physical
position 连接；缺对象/缺行严格失败，显式 NULL 源则保留其物理行和 NULL bitmap。
ASYNC 的可见性延迟和读取基础列行为应明确公开，不能用另一个 snapshot 的 embedding 补洞。

全索引架构仍有一处 Coordinator 数据面：`IndexingBootstrap.initializeBuildServices` 保留
Java IndexBuildCoordinator，`pangrova-indexing/.../build/IndexFileBuilder.java` 调用
`reader.readColumns` → strategy `build(rows)` → serialize/write，负责普通标量/数据跳过索引。
所以 vector/BM25 Native 构建成立，并不意味着所有索引构建都已符合 Coordinator 不读行的目标。

## 6. 现有测试真正证明什么

| 测试实现 | 真实覆盖 | 不能由它推出的结论 |
|---|---|---|
| `testing/e2e/index/vector/VectorHybridSearchE2ETest` | 真实 SQL DDL、过滤/排序结果；有 category/score 条件的小表。 | artifact intent 排除这些过滤查询；没有等待发布与实际 ANN plan/manifest 消费断言，不能证明 FILTERED_ANN。 |
| `testing/e2e/index/vector/VectorIndexE2ETest` | HNSW/IVF_PQ DDL 与真实 SQL 小表结果。 | CREATE/SHOW 与立即查询不证明 published artifact 被读取，也不证明 Recall、加速、过滤 ANN。 |
| `worker-native/test/artifact/ArtifactRetrievalPlannerTest.cpp` | 真 C++ index 文件、locator/checksum、缓存复用、搜索与 metric/dimension/analyzer fence。 | 不是完整 SQL/auth/跨 worker/真实模型链。测试数据是算法 fixture，不是生产性能样本。 |
| `worker-native/test/velox/RetrievalMergeExecutionTest.cpp` / `test/retrieval/RetrievalFusionTest.cpp` | 真 Velox operator 与数学融合 oracle。 | 并未验证 SQL 投影唯一身份、真实 ANN、生产 provider 或性能收益。 |
| `worker/.../AiRerankScoreFunctionTest` | 内部绑定和批量调用断言。 | 匿名 RerankingProvider 返回人为分数，不能算真实模型 rerank E2E。 |
| `coordinator/.../GeneratedValueArtifactPublicationIntegrationTest` / ReadResolver / CommitLog tests | 真实 PG transaction、租约/授权、绑定与元数据发布；测试自己创建 job/完成报告。 | 合成 output metadata/checksum 不等于 Native 真正计算过向量；不得当真实 provider/物化性能证据。 |
| `worker-native/test/velox/GeneratedValuesOverlayExecutionTest.cpp` | 真 Native sidecar 解码/物理位置 overlay。 | 不包含提交 SQL 到模型、写 commit 再 fenced publish 的全链。 |
| `testing/e2e/ai/OllamaAiSqlE2ETest` + `GeneratedSqlPublicationAssertions` | 真实 provider 直接 embedding 对照；purpose 与 NULL；真实 INSERT→durable ASYNC→Native sidecar→PG PUBLISHED→SQL 读取向量，断言未授权/不支持目的不产生 provider I/O。 | 这是真 ASYNC 和显式 embedding 链；不证明自动查询 embedding、SYNC、多向量、真实 cross-encoder、变更行复用或全量质量门槛。 |

本轮未找到 SQL → 真实 cross-encoder 服务 → Native final TopN → Flight 的 E2E，亦未找到真实
检索结果驱动的 Recall/NDCG release gate。已有 gate/profile 字段不能补足指标 executor。

## 7. 下一轮可直接实施的原子单元与验收

| 单元与修改 owner | 一次交付必须贯通的内容 | 真实验收 oracle |
|---|---|---|
| canonical row identity：planner / protocol / Coordinator binder / Native source+merge | hidden source/table+snapshot+dataObject+physicalPosition 的 typed 身份贯穿各 source、shuffle、fusion、rerank；若用 PK，先验证唯一性及 snapshot 身份；显示投影在 final emit 去掉 hidden 列。禁止 display `toString` 作物理身份。 | 真实 SQL 重复显示值、NULL、同一行不同 source score、不同文件同位置、分区重排；比较原始物理多重集，禁止丢行或把不同 source payload 当不同文档。 |
| linear finite 正确性：Native RetrievalFusion | 保留现有 min/max 语义，使用不溢出的范围/差值算法并检查输出 finite；所有 source 归一化/权重/score direction 共同验证，不偷偷变成 Milvus arctan。 | 真 C++ 直接调用 fixture 包括 finite 极值、相等 score、反向距离、全缺某源、权重总和溢出；独立高精度数学 oracle，原断言全部保留。 |
| filtered ANN：planner / retrieval protocol / Native artifact+connector | 将完整授权/SQL filter 在精确 snapshot 下转为 Native row selection，明确 prefilter/iterative candidate policy 和 budgets；ANN 前后都守行策略，禁止无授权候选送入 reranker；task 之前可明确选择 exact，task 后失败不得 fallback。 | 已发布 artifact 的真实 SQL，与同数据 exact distance 扫描独立对照；高选择性/零命中/NULL/删除行/多 split；断言实际 artifact generation/read、Native strategy 与 RLS，不能只看输出相等。 |
| 查询文本 embedding 与多 source：planner + 同一推理协议 owner | 为每 source 绑定 column/expression、query text 与 query-purpose、精确模型/维度/metric；查询向量由 Native 执行受治理推理 stage 产出，再进入检索，Coordinator 不执行模型；删去未接通规则后以唯一接入点替换。 | 真实模型 query-purpose direct oracle 对比；每 source 不同模型/维度、无索引 literal vector、不支持 purpose 零 I/O；检查 Native DAG 与供给计数，而非 mock directive。 |
| 模型 rerank：Native RPC + 独立 inference service | 按 inference audit 整体替换 JNI AI path，bounded candidates 真 batch，固定 query/doc ordinal、auth/model generation、schema与有限 score，独立传输/服务 cancel-drain；保留 Native final sort。 | 实际部署的固定 cross-encoder，直接服务调用与 SQL 返回评分/名次匹配；deadline、kill、响应乱序/缺项/NaN 与授权撤销均 fail closed，所有 owner 真实归零后才声明关闭。 |
| ASYNC 增量复用：generated job compiler / Native writer / metadata publication | 原提交日志与精确 snapshot 锚点保留；仅可证明内容/声明/model-purpose 不变的 source object 或唯一 PK version 复用结果，删除/重写必须重映射物理 locator。BM25 全局统计对当前有效文档精确汇总；模型版本变更强制新计算。 | 真实 INSERT/UPDATE/DELETE、文件 compaction、model replace、重复/乱序 commit；真实 provider 调用计数确认仅变化内容计算；故障后唯一 PUBLISHED generation 完整、旧 locator 不可复用。 |
| 标量索引数据面迁移：indexing + Native artifact build | 将 Java `readColumns/build(rows)` 一次迁入相同 durable Native page/build/publish 流，定义真实 Native format/provider并移除该 Java 数据构建入口；Coordinator 仅存描述与调度。 | 真实 SQL 数据跳过索引创建/更新/读取，准确候选 split 和 exact WHERE 对照；断言 Coordinator 零行读取；不可新增空 execution SPI 然后称迁移完成。 |

故障矩阵必须由真实 PG、真实 Native/Flight 与模型服务组成：worker 在写对象后/页面报告前
死亡，租约过期，旧 owner 晚到报告，publication 事务前撤权限/撤模型，源 snapshot 改变，
实际 immutable 文件 checksum/role/coverage 损坏，以及 query cancel。
每项断言无越权发布/读取、无旧 generation 偷换、无同任务重试变模型、无成功 terminal/receipt
掩盖失败。fixture 输出只能校验合同，不能代替实际 executor 成功证据。

质量采用可重放数据与 qrels，分别记录 per-source candidates、候选预算、最终 K、删除/过滤
语义、模型/索引/数据版本。Recall@K 对照独立 exact metric 扫描并明确距离并列规则；NDCG@K
对照独立 qrels/gain/discount，ideal gain=0 明确标为不可计算。BM25 对照全库 N、df、avgdl 和
固定 analyzer；RRF 对照全局 source rank 数学结果，不能先分区排名再求和。
至少真实跨 worker、cold/warm cache、多租户并发、artifact rebuild、模型队列压力分组采样。
延迟从 SQL submit 到真实结果 EOF/终态的原始样本计算，记录硬件、snapshot、模型、预算、
查询参数与样本资格；样本不足不可标 P99 合格，不把最大值当 P99。成功、超时、取消、失败
分别计数，不能只保留成功快查询。完成这些证据后才能选择本项目最合适的 Pareto 配置。
