# 索引、混合检索、模型重排与自动 embedding：真实链路核查

核查日期：2026-10-07。依据当前工作树源码及官方文档。本轮仅做只读核查并新增本文件，
初次核查没有修改生产源码、启动构建或运行测试；下文测试分类是对测试实现的核查，不能替代执行结果。
后续隔离 worktree 的 linear 数值修复见第 8 节，构建和测试由统一验证流程负责。
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
 -> RetrievalGenerationResolver / PublishedGenerationBindingResolver
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

`coordinator/artifact/PublishedGenerationBindingResolver.java` 解析实际 DataSplit 的 source、
snapshot、dataObject，解析唯一 PUBLISHED generation，核对 manifest、格式/role、完整覆盖
与对象 checksum。它输出未分区模板；真正 task partitioning 再绑定本任务对象并签名。
分区物化时 publication 不再匹配会失败；不存在吞错后换另一代索引的运行时路径。

`coordinator/query/execution/dispatch/dag/retrieval/RetrievalGenerationResolver.java` 的 nullable
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

`DurableDataBuildDispatcher` 管租约、fence、重新授权、stage DAG 和真实 worker 报告；
`DurableDataBuildPublisher` 取得 publication claim。`JdbcIndexGenerationPublicationStore` 和
`JdbcGeneratedValuePublicationStore` 在同一 PG transaction 调用
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
Java IndexBuildCoordinator，`pangrova-indexing/pangrova-indexing-core/.../build/IndexFileBuilder.java` 调用
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
| `coordinator/.../GeneratedValuePublicationIntegrationTest` / ReadResolver / CommitLog tests | 真实 PG transaction、租约/授权、绑定与元数据发布；测试自己创建 job/完成报告。 | 合成 output metadata/checksum 不等于 Native 真正计算过向量；不得当真实 provider/物化性能证据。 |
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


## 8. 隔离 worktree 的 linear 数值修复（已实现，待统一运行验证）

范围仅 `pangrova-worker-native/src/retrieval/RetrievalFusion.cpp/.h` 与
`test/retrieval/RetrievalFusionTest.cpp`，以及本审计记录。保留原 min/max normalization，
没有改成 arctan、裁剪输入或增加 exact/model fallback。

当 min/max 跨正负且任一端点超过 DOUBLE_MAX/2 时，score/min/max 同比乘 0.5，
使后续范围与分子差值均可由 DOUBLE 表示。普通范围和 subnormal 小范围维持原公式；
相等 score 仍归一为 1。所有源权重先除最大合法权重，再计算有界相对总和，避免
合法有限权重（如两个 DOUBLE_MAX）之和溢出；没有候选的源仍参与权重分母。
NaN/Infinity/零/负权重与非有限输入仍拒绝。归一结果及排序前输出显式校验 finite，
不把 NaN 带入排序比较器。该变化允许各项合法、但原始相加超出 DOUBLE_MAX 的权重配置；
保留相对权重含义，并非吞掉非法配置。

原有 7 个测试的主体和断言逐字保留。新增 8 个真实 C++ 直接调用测试覆盖正常多源、
±1e308、±DOUBLE_MAX、不对称极值、相邻巨大数、subnormal 范围、相等 score、
反向 distance、缺源、全空源，以及巨大/极小/非法权重和非有限评分。
独立 oracle 使用现有 SDK Boost `cpp_bin_float_100` 的 100 位十进制精度及宽指数范围，
直接计算原始差值和未缩放的权重总和，再舍入为 DOUBLE 对照；没有复用生产缩放算法，
也没有把 Apple arm64 与 DOUBLE 同精度/范围的 long double 当作高精度。

仅完成 `git diff --check` 与原测试文本保留检查，不在子任务启动 CMake、CTest 或 Maven。
最近测试过滤器为 `RetrievalFusionTest.*`（当前 15 个测试）；统一隔离构建完成后记录实际结果。
该单元不宣称 canonical row identity、filtered ANN、查询自动 embedding 或模型 rerank 全链完成。


## 9. 非确定表达式去重策略（隔离实现，待统一运行验证）

固定版本官方 [QueryConfig.h](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/core/QueryConfig.h)
将 `expression.dedup_non_deterministic` 默认设为 true；
[ExprCompiler.cpp](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/expression/ExprCompiler.cpp)
的 `getAlreadyCompiled` 在该配置为 true 时复用非确定表达式。
官方固定版本 [ExprCompilerTest](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/expression/tests/ExprCompilerTest.cpp)
使用内置 rand 验证两种编译策略。
原 Pangrova 生产 `VeloxTaskRunner::start` 没有设定这一项，因此沿用了 SDK 默认值，
使相同非确定调用存在被共同子表达式复用的风险。

唯一 production runner 在构造 QueryConfig map 时无条件设置 SDK
`kExprDedupNonDeterministic=false`，这是 SQL 每个非确定调用独立求值的固定语义，
不添加 session 配置开关。SDK 对真正 deterministic 的表达式继续复用。
该变化依赖真实函数元数据，不能替代对 AI/provider 函数 deterministic 声明的审计。

新增 `test/velox/NonDeterministicExpressionExecutionTest.cpp` 使用实际 runtime finalize/published
registry、production runner 及其 Task QueryCtx；提交 raw SDK key 和 apparent velox session key
的 true 值也不能改写语义。真实内置 rand ExprSet 的两个同形调用必须编译为不同节点，
两者均标非确定，各处理 N 行，汇总统计恰为 2N 行及 2 vectors，并检查实际输出为 finite、
非 NULL 且处于 [0,1)。随机值允许偶然相同，独立调用采用节点和精确求值统计证明，
不以概率性的两列不等断言代替确定性回归。
另一个测试通过实际 production parallel runner 执行含两个 rand 的 Project，
验证真实两列输出、完整行数、FINISHED 和零 running drivers。
第三个测试验证同形内置 abs 仍共享节点、实际结果正确并只求值 N 行。
没有 fake UDF、模拟 provider、另一个执行引擎或调整上游 SDK 默认实现。

本子任务新增生产代码仅四行（含说明），既有 runner dirty 内容逐字保留。
最近过滤器为 `NonDeterministicExpressionExecutionTest.*`（3 个测试）。
只完成差异/空白检查与固定 SDK API 核查；构建、最近测试、全 Native 及当次 JDK17 JNI
重新配置/打包验证由统一流程执行，不在此记录未运行的通过结论。


## 10. 算法选择、优化保守性与终态异常（隔离最小修复，待统一验证）

检索规划的无索引行为与执行失败切换路径必须分开审计。当前
`VectorSearchRewriter.tryRewrite` / `RetrievalGenerationResolver.resolveOne` 可以在绑定前选择
Native exact distance + ORDER BY + LIMIT：没有已发布、满足快照和覆盖条件的 ANN artifact
时，这仍是唯一 Native 执行计划。`RetrievalPlanBinder.toProtocolBinding` 将有 artifact
的绑定设为 FALLBACK_FORBID；`ArtifactRetrievalPlanner` 对 checksum、格式、角色、覆盖及
真实图文件加载失败直接抛错。HNSW/IVFPQ 的读取失败由该调用方拒绝，不重放 exact scan。
`PlanExecutionDriver.reportExecutionFailure` 上报终态失败；当前 Java completion monitor
不提交另一个查询计划。Native `PlanExecutionLifecycle` 的备用调度线程仍调用同一个
executeVeloxPlan，不能仅凭名称把它当成另一个 SQL 引擎。

固定 SDK 的 [Task::setError](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/exec/Task.cpp)
记录异常并终止失败任务，不替用户重规划。
[Apache Gluten PartialProject](https://apache.github.io/gluten/developers/PartialProject.html)
明确描述 JVM 不支持表达式与 Native 支持表达式之间的转换，是实质的两种执行所有者；
其 [configuration](https://github.com/apache/gluten/blob/main/docs/Configuration.md)
中的验证退回策略不能机械搬成 Pangrova 运行期 ANN 错误后重放另一算法。

L0/L1 文件跳过是一种同计划优化。普通索引缺失、版本尚未支持、格式损坏、普通 IO 错误
或可选文件评估超时保留 split，让原 Native WHERE/scan 继续判断，是合法 UNKNOWN 语义。
这里真正的错误是将授权拒绝与查询取消也归入这种保守保留：
`IndexSplitFilter.invokeNativeEval` 的 broad catch、parallel awaitFuture 对 ExecutionException
的 broad catch、`IndexAwareSplitFetcher` 的外层 catch，以及 `IndexEvaluationService`
的 aggregate/evaluate/deserialization catch 都可能掩盖终态异常。

本次最小隔离单元新增 indexing 所有的 `IndexEvaluationFailures`，只展开标准
ExecutionException / CompletionException 包装，按类型原样传播 SPI AccessDeniedException、
SecurityException 和 CancellationException。InterruptedException 恢复当前线程 interrupt
flag 并以带 cause 的 CancellationException 传播；读前/读后及提交/评估边界的 interrupt
检查不清除 flag。同步 resolver/evaluator、异步收集及外层优化调用均使用同一分类。
收集中的授权或取消失败取消尚未完成的 futures，不返回已完成 prefix 的 partial splits。
普通错误的原有 UNKNOWN/KEEP、普通可选评估 timeout、合法无索引 SQL 行为继续存在。

[JDK17 CompletableFuture](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/CompletableFuture.html)
规定 get 对异步失败使用 ExecutionException，而 cancellation 是 exceptional completion；
其 cancel(true) 并不保证中断真实 producer，所以这里仅保证不再接受其结果及查询失败传播，
不能声称 remote owner 已停止、底层工作已 drain 或 native/provider deadline 已完成。
[JDK17 Thread](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Thread.html#interrupt())
说明阻塞等待的 InterruptedException 会清除 flag，生产边界须显式恢复。

新增 `IndexEvaluationAuthorityIntegrationTest`（10 个真实组件测试）使用实际 PostgreSQL
Flyway 元数据、PersistentRoleStore / PersistentGrantStore / DynamicRbacAccessControl，
真实 LocalFileSystemAccessor、真实 CompletableFuture 和 production awaitFuture/collection
同 package seam。权限授予后读取真实文件；撤权后异步拒绝必须保留原异常，不能返回 split。
也验证成功 prefix + 拒绝 suffix 不产出 partial list、pending future 取消、owner 中断且 flag
保留、普通缺文件 IO 与 timeout 保留原 split、无 index hints 原列表不变；service resolver
回调直接执行实际持久权限检查，未授权/授予/撤权以及实际缺失/损坏文件 UNKNOWN 和
service owner 中断均有断言。没有 fake provider、受保护 accessor 装饰器或合成 ANN 分数。
service 测试证明公共 resolver/evaluate 边界，不宣称当前 connector evaluator 内部执行了
授权 IO、真正的 Native index callback fault 或端到端 RLS/ANN 召回测试。

全仓 caller 核查发现 Worker `IndexEvalSplitFilter` 没有生产接入调用，且传给默认 evaluator
的 filesystem accessor 为 null；其 enum 聚合测试不能证明 Worker L2 已接通。
它与未调用的 DiskAnnSearcher preFilterBitset 必须以整体能力单元决定接通或移除，不能
通过修 decorative catch 冒充全链完成。本次不改 Worker dormant 路径。

后续闭合顺序：先固定 canonical physical source/snapshot/data object/row position identity，
再把实际过滤条件与 RLS 放到已发布真实 ANN artifact 的检索路径并与 exact metric 结果
比较 Recall；当前非空 serializedPreFilters 会阻止 ANN artifact 选择并采用合法 Native
exact plan，不能声称 filtered ANN 已完成。之后才验证真实 hybrid 重复/NULL、多 source
row identity、rerank/provider fault 与 Recall/NDCG/延迟。输出 visible string/toString 不构成
物理 row identity。此次不变更上述能力声明，不新增执行开关，不改 C++/协议或主 checkout。

仅完成 source diff、既有测试未改检查与官方语义核查；未启动 Maven/Native build。
最近过滤器为 `IndexEvaluationAuthorityIntegrationTest`；实际 PostgreSQL 运行与 owning
Coordinator/indexing 及消费者回归由统一隔离流程执行，结果尚未确认。


## 11. Real ANN acceptance closure draft (2026-10-07; execution pending)

This atomic draft closes false-green acceptance of unfiltered immutable ANN. It is not a filtered-ANN implementation or an SF100/1000 performance result. No test or build has been run for this draft. The original numeric, filter, ordering, ten-query top-1 and local timing assertions remain present.

### Root causes and runtime prerequisites

- `IndexLifecyclePerformanceE2ETest.waitForIndexReady` previously returned success for `STALE`. The draft requires `READY`, rejects terminal `STALE`/`DROPPED`, and propagates SQL/poll errors rather than swallowing them. Vector generations additionally require the real JDBC active publication and durable artifact job to be `PUBLISHED`, with identical tenant/index/generation/source snapshot and manifest SHA-256.
- `DiskAnnDistributedE2ETest` previously searched immediately after asynchronous `CREATE INDEX`. Exact Native planning could satisfy all its old row assertions. The draft waits for publication and requires actual issued ANN bindings for each unfiltered search.
- The existing shared E2E runtime did not configure `cache_enabled`, immutable SSD capacity/path or parsed-index RAM. `NativeCapabilityInventory.cpp` advertises ANN retrieval only when both `ImmutableArtifactCache` and `DiskAnnIndexCache` are enabled. `NativeEngineLifecycle.cpp` creates both under the cache-enabled lifecycle. The test fixture now supplies bounded 256 MiB immutable SSD / 64 MiB index RAM and the existing Native governor's 32 MiB RAM cache setting. This uses the canonical production RuntimeInitConfig and real bundled JNI; it does not synthesize capabilities. Production `NativeRuntimeBootstrap` already passes these canonical settings.

### Actual executor evidence, with no diagnostic flag

`VectorIndexE2ETestBase` observes the existing production `CoordinatorTaskTracker.addExecutionDrainListener`. Each observation copies the original issued canonical TaskRequest while it is retained, before the full real Native report is compacted. Captures live only around actual authenticated HTTP SQL submission, Flight consumption/FINISHED, and Native drain; they are bounded by 512 tasks and 32 MiB, tolerate exact event replay, and release their captured plans in finally. An observation overflow fails the test and clears its retained plans without rejecting the production drain notification. There is no fake TaskRequest, provider, index, receipt or executor.

The acceptance checks exact tenant/table/index/generation/snapshot, immutable manifest and output-object checksums, and `FALLBACK_POLICY_FORBID`; an exact plan with correct rows is rejected as proof of ANN use. Native terminal evidence must be FINISHED, NATIVE origin, NATIVE_OWNER_RELEASED and include actual producer statistics. The bytes in the runtime's real content-addressed immutable cache must match the published object's size and SHA-256. `ArtifactRetrievalPlanner::prepare` unconditionally materializes graph/raw vectors/locators, executes the graph search and full-precision distance rescore for this bound ANN relation before converting its scan allowlist. `PlanExecutionDriver` is the production caller. These facts establish actual ANN execution together; metadata or a issued binding alone does not.

The new cold-read fixture requires actual RAW_VECTORS objects to be absent immediately before the first bound query and present afterward, checksum-verified alongside the graph and locator objects. Existing small fixtures can legitimately share content-addressed warm objects; they do not invent downloaded-byte counters. Currently `ArtifactRetrievalPreparation.downloaded_bytes` is not propagated into query TaskReport usage; it remains a production accounting gap rather than a fabricated test metric.

### Independent quality oracle and limited scope

`NativeAnnRecallE2ETest` inserts 1,000 real FLOAT32 vectors in ten actual Iceberg source files using a fixed input seed. It obtains the complete dataset through real Native/Flight SQL before creating the index, and verifies that the dataset task splits, published generation and ANN requests pin the same snapshot. Four off-row queries use an independent BigDecimal calculation of exact squared L2 from the fetched binary32 values, with an unambiguous top-K boundary. The returned distance is checked against an 80-digit square-root calculation, row IDs cannot repeat or escape the ground-truth population, and Recall@20 must meet the threshold declared by the actual issued approximation profile (currently graph Recall 0.95). This is functional quality acceptance, not a statistically qualified recall benchmark or a performance optimum.

The three existing HNSW/IVFPQ/cosine lifecycle cases retain their old assertions and additionally require actual ANN execution, exact baseline and post-drop plans, same-snapshot baseline evidence and the actual declared Recall threshold. Their baseline is real exact Native SQL; the separate DiskANN fixture adds an independent arithmetic oracle. No original weak 0.6 HNSW assertion is removed; the declared-profile requirement is additional.

### Filtered ANN and stable identity remain separate open units

The old category WHERE case is preserved, including its count, category, top-1 and ordering assertions, and now explicitly verifies an exact Native plan. A second actual filtered exact query compares the same published snapshot/filter population against the independent distance oracle at Recall 1.0. `RetrievalIntentCollector` excludes vector artifact selection when `serializedPreFilters()` is nonempty; `DiskAnnSearcher::preFilterBitset` is not called by production immutable retrieval, which invokes Vamana directly. Therefore no filtered ANN support is claimed.

A complete filtered ANN implementation requires one coordinated planner/Substrait/binder/Native unit: compile the supported typed predicate and SQL NULL semantics; evaluate it against the same signed snapshot with actual physical row/object identity; construct a governed filter representation; apply the eligibility predicate during actual Vamana traversal and candidate acceptance; preserve traversal through nonmatching nodes where needed for reachability; size search effort and enforce sufficient candidate/quality bounds without falling back to another execution plan; and test true issued filtered-ANN bindings, real read provenance, independent filtered ground truth, low/high selectivity, NULLs, multi-file boundaries and no-match cases. Merely removing the planner exclusion or attaching the dormant bitset would not close this chain. Hybrid stable identity also still requires the real physical object/row-position key through planner, Substrait candidate schema, binder and Native merge rather than projected values.

### Official source reconciliation

- [DiskANN upstream](https://github.com/microsoft/DiskANN) now documents DiskANN3's Rust DataProvider integration, attribute-filter hooks, memory tiers and benchmark tooling. The retained C++ branch is not actively maintained. Pangrova's fixed local Vamana implementation is not evidence that every current upstream feature is available.
- [Filtered-DiskANN research](https://harsha-simhadri.org/pubs/Filtered-DiskANN23.pdf) treats filtered retrieval as a graph-search/index design problem; an exact WHERE result or a dormant post-filter bitset is insufficient evidence of filter-aware ANN.
- [Velox memory management](https://facebookincubator.github.io/velox/develop/memory.html) distinguishes query-owned pools, allocator/cache memory and other allocations. Explicit cache quotas in this fixture are runtime prerequisites; they do not prove complete query usage accounting or JVM/provider resource isolation.

Closest genuine checks after root review/application: `DiskAnnDistributedE2ETest`, `NativeAnnRecallE2ETest`, and `IndexLifecyclePerformanceE2ETest$VectorLifecycle`. Owning E2E/full JNI runtime regression remains required because the shared Native fixture now enables a real cache substrate. Missing PG/Docker/current bundled JNI is failure or an explicitly reported prerequisite, never an exact-plan fallback or green skip.


## 12. Filtered ANN 完整生产单元草案（未构建、未验证）

2026-10-07 本单元仅写入 TMP overlay，未修改主或隔离 checkout，也未运行 CMake/CTest/Maven。
它依赖第 11 节真实 ANN 验收前置草案；不能把源代码存在、协议 catalogue 或测试名称算作能力通过。
原始 SQL 数值/排序/过滤/Recall=1 断言保留，原两个受支持 category 过滤例由 assertExact 更新为
实际 filtered capability + 原 issued TaskRequest + Native OperatorStats 的强验收。旧过滤例经过
实际优化器、原查询谓词、实际 artifact，不加入人为 CASE/复杂度限制保持旧 exact 标签。

完整 draft 调用链为 protected PlannedQuery -> typed immutable scope -> 四个隐藏 physical fields ->
原 fragment/exchange -> RetrievalTopKBinding/候选合同 v2 -> issued TaskRequest -> 原 Native parser ->
同一个 PlanExecutionDriver/Task/QueryCtx -> 原 TableScan/真实 Filter -> eligibility gather ->
FilteredArtifactRetrieval -> 同一实际 Pangrova connector 的 candidate hydration -> 原 payload/距离
projection + TopN -> 原 Flight。Coordinator 不读取用户行、计算 ANN 距离或执行模型 rerank。

候选身份是 source ID + snapshot + data object + 非负物理 row position，三个 required string 和
一个 required i64 全链携带，只有最终 OUTPUT stage 删除。Native fusion 按 typed authority 字段
编码，不以用户可见 toString 作为物理键。fanout 的 source tag 不进入该物理 tuple。泛化数学
候选 VALUE_FIELDS 与物理源候选 PHYSICAL_SOURCE_ROW 分开，candidate schema epoch 精确为 2，
没有接受 0/1 的读取兼容路径；canonical proto 字段需 root 统一重新生成，真实 Native catalogue
必须通过当次已构建 exporter 重新导出，禁止手改 generated Java/C++ 或 catalogue 二进制。

过滤资格由原 typed Velox Filter 对真实 active rows 执行一次，包括 NULL/AND/OR/CASE 和实际
删除规则。图遍历保留非匹配 bridge 节点，只有 eligible 已访节点精确 L2 rescore 并进入有界 K heap。
output_limit 是 SQL K；search_list_size 是每对象探索预算；candidate/exact-rescore work 是真实
任务所绑对象数乘探索预算，不能把 K×4 当过滤正确性。已知 eligible Population 不足 K 合法返回
min(K,M)；预算不足无法填满已知结果人口必须失败，不返回部分成功，也不切换另一查询计划。
该 traversal 使用 Pangrova 现有 Vamana，不能宣称实现了论文 Filtered-DiskANN 的 filter-aware
建图或继承其性能结果。bitmap/map/heap/visited 存储记入真正 operator leaf MemoryPool。

规划选择只覆盖 actual ConnectorReadBinding 的 immutable exact snapshot，并在实际枚举的
signed splits 再验证 PARQUET/ORC、source/snapshot/object 一致且无 MOR。未支持格式、不可证明
历史声明或 merge read 只能在 Task 发出前保留原 Native exact SQL 算法。已 issued Task 的权限、
IO、checksum、版本、预算或取消失败必须传播，不能运行时回落到 Java/另一个 Native 计划。
Lance、METADATA_JSON 与 MOR 读边界不在本单元 filtered 优化范围，不声明其 task read budget
已全覆盖；这不新增执行开关、不删除无索引 SQL 的合法 exact 算法。

真实全 task read owner 使用原 StageResourceGrant.object_store_read_bytes；artifact 冷下载、
qualification DWIO 和 hydration DWIO 共享同一个 owner，不各自放大上限。所有 actual pread/
preadv backend ranges 先预留，failed attempts 也占用请求预算；上层 cache hit 不发 backend read
即记 0。固定 SDK buffers preadv 可有 null gap，预留包含整个 requested span。regions overload
允许内部 coalescing 多读，因此 draft 通过同一个真实 backend 的有界逐 region pread 保证不
偷偷增大 grant，保留每个实际输出 IOBuf 顺序。requestedBytes 是请求预留，completedReturnedBytes
仅是 SDK 返回大小（可能包括逻辑 gap），两者都不能冒充物理网络传输测量。现有 datasource
getCompletedBytes 是 decoded 估算；Task rows_read 是跨 operator 聚合工作量，不是去重物理人口。

取消先停止新 actual read；已开始的同步 backend 调用持有真实 Attempt/input 所有权。
stopAndDrain 的 condvar wait 释放 mutex，不持锁等待回调；在实际 read completion 后仍保留原
Task deletion future 等待。Pangrova datasource cancel 只设置实际原子 signal，Task token 从
真实 ConnectorQueryCtx 传播。新算子 close 与固定 SDK TableScan 同协议只 cancel、不清理
reader/context/source leases；实际 blocking future 持 Driver 到完成后，再由 Driver/operator
析构清理成员。不强制 resume 另一 reclaimer，不在 close 同步等待。

TaskBudgetReadFile 明确 hasPreadvAsync=false，真实 backend 声明 native async 时拒绝委托，
不会静默同步 fallback。SDK 明确声明同步的 LocalReadFile preadvAsync 分支保留原实际 API
结果测试，但这不是远端异步证明；callback 持 ReadFile/Attempt 不等于拥有 caller buffers。
现选定 PARQUET/ORC DWIO 实际调用同步 ReadFileInputStream::read；后台预读的 executor lambda
持 shared DwioCoalescedLoad，loadData 局部 CachePins/shared input 保留到实际 backend read 完成。
远端 async positive E2E 仍 pending，consumer 放弃 future 或取消 ACK 都不能冒充 read drain。

deadline 承接正式 TaskExecutor 构造时注册的 watchdog，经 failDeadline→execution_driver.cancel
停止新读并请求原 Task cancellation；其轮询至多约 100ms。checkpoint 自身只检查 cancellation，
不宣称严格墙钟截止后零新增读、不打断已开始 backend seek/read。真实 transport timeout/drain
仍由实际 owner 完成。新算子的 ContinueFuture 显式 makeEmpty，避免 SDK 默认有效/就绪 future
导致虚假的永续 connector 等待；zero-row RowVector 不是 EOF。

新增待执行真实验收包括：
- 实际 DiskAnnBuilder 图/raw/locator + cpp_bin_float_100 独立距离 oracle：稀疏资格、非匹配
  bridge、少于 K、empty、预算不足、真实 CancellationSource、冷读/缓存 0 读、真损坏对象。
- 固定 SDK Writer 写真实 dictionary+NULL Parquet，实际 TableScan/typed Filter/gather/live
  filtered operator/同 connector hydration 的单 Task 回归；真冷 RAW 文件被改坏而保原 digest、
  真 task read grant=1 都必须 FAILED/无结果，保留实际 Task deletion/read drain。
- 真 RetrievalMerge Task 的 typed physical identity：相同 tuple 跨源只融合一次，不同
  snapshot/object 的相同可见值仍保留；原 RRF/linear 全部旧断言保留。
- 实际 iceberg SQL 写入/真实持久 index READY/PUBLISHED 同 generation/issued bindings/
  TaskReport Native owner release/Flight，8 个 NULL/CASE/稀疏/empty/fewer-K 过滤条件从该
  snapshot 的实际 FP32 数据用独立 BigDecimal exact-binary32 oracle 计算距离与 Recall。
  另用两个距离相同、可见 payload/dist 相同但物理行不同的真实 SQL 例检查不坍缩。
- Protocol 真 producer/consumer round trip 验证 v2 物理合同、K/work 分离、最终 visible trim，
  及旧 revision、缺 identity、nullable/重复 key 的拒绝；这些数值合同不当性能数据。

以上为源码草案内容与待执行 oracles，不是 PASS。完整 Release/ASAN、JNI/protocol、owning
Java 回归、真实 SQL E2E 与真实持久权限/取消故障链由 root 统一协调。200/1000 行本地数据
只有 functional 意义，不能替代 SF100/1000、production Recall/NDCG/并发/尾延迟校准，也不能
保证“业界最优”。其他执行单元的真实 PASS 不归因于本单元。

官方源核对：采用 [Substrait logical relations](https://substrait.io/relations/logical_relations/)
的 typed schema/predicate 语义及 [Velox operators](https://facebookincubator.github.io/velox/develop/operators.html)
的实际 Task/operator input/output/blocking 所有权；[Filtered-DiskANN 原论文](https://harsha-simhadri.org/pubs/Filtered-DiskANN23.pdf)
的 filter-aware graph 质量问题只作设计核对，不能当本引擎的实测结果。固定 SDK 的 File.h:
regions preadv 契约、InputStream.cpp/CachedBufferedInput.cpp 的真实 caller 与保持 owner 的
loadData，以及 Task::taskDeletionFuture 是本地实现依据，单元 source hash 清单另存于 overlay。
