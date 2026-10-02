---
title: "AI Native 单路径内核：根因复证与实施规格"
description: "2026-10-01 当前工作区复证；唯一前端、契约、Native 执行和列式边界"
---

# AI Native 单路径内核：根因复证与实施规格

基线：2026-10-01 当前未提交工作区。依据用户最新《Pangrova AI Native 架构调研与全链路重构》及
对 Arrow/Velox 物理边界的答复“深度调研，使用最合理的方式”。本文是修复规格，不是实现或性能
验收证明。旧报告中范围、SKIPPED、独立 Lance 必需性及 fallback 设计与本文冲突时，以本文为准。

内核范围包括统一协议、分布式写存查、索引、Embedding、模型绑定与治理、多模态、缓存、Feature
Store、数据库内评估执行。Agent 编排、对话/Prompt 工作流和 LLM Usage Ledger 不进入内核。
数据库评估必须是版本固定、受权限控制的 Native 计算及可信指标提交，不是外部 Agent 发布审批。

## 1. 调研裁决：四层各有一个职责，而不是强制相同内部布局

| 层 | 唯一职责与表示 | 必须禁止 |
|---|---|---|
| Calcite | 方言扩展 parser、校验、类型推导、关系优化；内部 SqlNode/RexNode/RelNode | 第二 SQL parser、自建 AST 语义、关系 Interpreter 在 Coordinator 生成结果行 |
| Substrait | 唯一优化器→Native 下发 IR，类型/函数语义契约、扩展关系和分片边界 | JSON/session 私有计划、特性独立 proto、失败后切换 IR |
| Velox | 任务内唯一算子、表达式、Vector 编码、内存池、spill、异步 IO 和取消 | 第二执行引擎、执行前检索物化、因编码不支持重新执行任务 |
| Arrow | 唯一数据互操作/网络契约；同进程 C Data，跨进程 typed IPC/Flight | Flight binary 列内再藏另一种业务页编码、同函数 scalar/batch 回退 |

这是基于上游实现的架构裁决：Velox 官方明确其 string/array/map 布局与 Arrow 不同，并支持
constant/dictionary 等编码；不应为“统一”牺牲任务内编码和共享缓冲区优势。
[Velox vectors](https://facebookincubator.github.io/velox/develop/vectors.html)。
Arrow C Data 用于同进程指针与生命周期交换，Flight 基于 gRPC/IPC；网络、TLS、压缩和布局转换
不能被标为全链路物理零复制。[C Data](https://arrow.apache.org/docs/format/CDataInterface.html)、
[Flight](https://arrow.apache.org/docs/format/Flight.html)。目标是消除冗余复制，逐边界记录必需复制。
Calcite RelNode 和 Velox PlanNode 是框架内部树，不是需要删除的第二 wire IR。

## 2. 行业差距与机制落点

| 官方机制 | Pangrova 当前差距 | 借鉴落点与收益 |
|---|---|---|
| Snowflake 存储、独立 warehouse、Cloud Services 分离 | Java write callback 仍消费数据行；部分检索在 Velox task 创建前运行 | Coordinator 仅 metadata/authority；Native sink/source 承担数据；隔离故障域和 heap 压力 |
| Snowflake multi-cluster 扩并发，单查询扩容另行衡量 | 不能以 Worker 数增加推导单查询线性加速 | workload pools、queue age 和完整 stage resource admission；分别量 scale-out/scale-up |
| 结果缓存与 warehouse 数据缓存分层，复用受权限/数据变化约束 | 不能把未接线/未完整失效的 SQL cache 算闭环 | result identity 与 immutable artifact cache 分权；消除跨 tenant/model/snapshot 复用 |
| 微分区统计驱动裁剪 | Lakehouse 不等于 Snowflake 专有微分区布局 | Iceberg/Paimon manifest/file/row-group stats → Calcite 谓词与投影 → Native scan；减少实际 scan bytes |
| Gluten 复用 Spark 控制流，以 Substrait 下发 Native regions | parser、optimizer、serde 存在额外路径；provider JVM 内存未消失 | 保留 StageDAG 控制面、最大化 Native region、stage heap/offheap 联合预算，不复制 row fallback |
| Gluten 公开类型/ANSI/正则/格式限制与 fallback 资源代价 | 静态能力表不能证明 Native 函数实际存在 | 单真源声明+实际注册 inventory；不对齐在优化期拒绝，消除隐式执行/编码回退 |

官方一手依据：[Snowflake 架构](https://docs.snowflake.com/en/user-guide/intro-key-concepts)、
[多集群](https://docs.snowflake.com/en/user-guide/warehouses-multicluster)、
[结果复用](https://docs.snowflake.com/en/user-guide/querying-persisted-results)、
[warehouse cache](https://docs.snowflake.com/en/user-guide/performance-query-warehouse-cache)、
[微分区](https://docs.snowflake.com/en/user-guide/tables-clustering-micropartitions)、
[Gluten](https://gluten.apache.org/)、
[Gluten limitations](https://gluten.apache.org/docs/v1.5.0/velox-backend/velox-backend-limitations.html)、
[stage resources](https://gluten.apache.org/docs/v1.5.0/get-started/VeloxStageResourceAdj.html)。
完整既有调度与 fencing 调研见 [行业调研](ai-native-snowflake-gluten-distributed-architecture-review.md)。

## 3. 根因账本与最小完整替换单元

路径以仓库根为基准；行号仅对应本次复证，源码编辑后以函数为准。下列未实施项均不满足验收。

2026-10-01 23:24 增量：Window 参数、排序和 frame 转换已实施，真实 SQL→Substrait 的
158 项专项通过；同时修复上游 AST→Calcite 时把 ROWS 固定为 RANGE、打印器把 RANGE
固定为 ROWS 的根因。此处尚未取得 Native 最终行结果的完整窗口 oracle，不能将本表
Window 项标为端到端完成。公开 SQL 的 DECIMAL 最大精度另有 Calcite 默认 19 位截断，
23:51 已通过生产严格工厂、38 位边界及精确 literal/type conversion 修复，并按原六类
命令复测 **103/103 PASS、0 skip**。该证据包含真实 SQL 与 canonical 16-byte 值，不是
直接构造类型的推断；Native output-driven cast 和数组降级仍需完整单元修复。

| 优先级/问题 | 已确认调用链与根因 | 完整修复单元、依赖及验收 |
|---|---|---|
| P0 Window 常量改值 | `planner/substrait/WindowFunctionConverter.java:108–229`，LogicalWindow/RexOver 两路：DECIMAL→FP64，default→0.0，未识别 offset→1，FP64→long 截断 | 在 Rex 层按签名选目标类型，两个入口共用现有 `LiteralConverter.convert(literal,targetType)`；删除粗转换。保留 decimal 精度/scale，offset NULL/fraction/overflow 显式拒绝；真实 SQL LAG/LEAD 返回非零精确 default |
| P0 Native list literal 值/NULL 丢失 | `worker-native/src/velox/plan/SubstraitTypeConversion.cpp:135` 的 `convertListLiteral` 把 decimal 选成 DOUBLE/读取 fp64，typed NULL 未设置 bitmap | 由 canonical element type 精确构造 Velox Vector，复用唯一标量解码；拒绝不一致元素。逐元素 oracle 覆盖 decimal(38,18)、NULL、嵌套，VALUES/函数/扫描输出一致 |
| P0 Embedding 随 batch 改语义 | `worker/ai/invocation/AiEmbeddingInvocationSupport.java:75–77` 按 cardinality 选 embed/embedBatch；Provider 两方法分别应用 query/document prefix | 同一 invocation 显式固定输入语义，不以批大小推断 purpose；单例也走同一批契约。真实 provider 输入捕获及输出证明 1/N、尾 batch、分片数变化完全一致，保留数量/顺序/维度原子校验 |
| P0 结果编码失败重执行 | `worker-native/src/executor/driver/PlanExecutionDriver.cpp:1218–1315` Arrow 不支持时重建 plan 并启动第二 runner | 递归规划期 type admission + Arrow serde 完整支持，结果/shuffle 一起迁移；删除重跑、mode property 和 Presto result reader。含 AI/写副作用用例证明一次执行，失败不发布部分结果 |
| P1 SQL 双前端 | `planner/parse/PangrovaParserFactory.java:26–52` ANTLR→AST→SqlNode，策略表达式另走 Calcite parser | Calcite 扩展语法一次覆盖 SQL/DDL/DML/策略/类型，迁移全部调用者与相同断言，再删 grammar/generator/AST conversion；不保留开关/兼容入口 |
| P1 optimizer 失败回退 | `HepOptimizationPass:51`、`SqlCompiler:240`、`PlanOptimizationRunner:57` safe program/原树/字符串识别异常 | 保存失败 SQL、规则次序、中间树，修字段/去关联 invariant；一个规则集，显式错误；AQE 必须实际改变分布并验证传输，而非重复原优化 |
| P1 Coordinator 关系执行 | `planner/rewrite/ConstantFoldingRewriter.java:127–142` Interpreter 构造 Object[] 后生成 Values | 删除关系执行，只保留统一声明确定性、有限量的 Rex 常量折叠；VALUES/aggregate/generate-series 实际下发 Native；检查并删除无调用者 evaluator API |
| P1 Coordinator 标量索引数据构建 | `IndexDdlExecutor.submitAsyncBuild` → `IndexBuildServices.dataSkipping` → `IndexBuildCoordinator` → `IndexFileBuilder.build` 在 Coordinator 调用 `ColumnValueReader.readColumns`、Java provider.build 和序列化/写文件；`IndexingBootstrap.initializeBuildServices` 明确保留该分支 | 所有 scalar/vector/text index 使用同一持久 Native artifact DAG，Coordinator 仅持久 metadata/授权/提交；迁移 Bloom/bitmap/min-max 的实际源快照、schema、算法、文件格式、取消和 publication 后删除 Java row builder 与分支。真实 SDK 文件→Worker build→发布→查询裁剪/结果 oracle、撤权与重启；不能只禁用功能或替换字符串 |
| P1 shuffle 私有页 | `ShuffleBatchSerializer.cpp:118` PrestoVectorSerde → `ShuffleFlightBatchReader.h:60` binary列 → `PangrovaExchangeSource.cpp:684–761` SerializedPage；Native 拒绝 Arrow exchange | producer partition/serializer/direct+remote/spool/receiver 一次迁移 typed Arrow/schema/dictionary 生命周期；保留现有 owner 保活，再删双编码；多 Worker、spill、重试和断链 oracle |
| P1 AI JNI 行化 | `JvmScalarFunction.cpp:831–858` Object[][] boxing，VECTOR float-array copy → `NativeFunctionBridge.java:182` List<List<ColumnValue>> | Velox 扩展内唯一 C Data batch ABI，明确 release/retain/取消；provider 必需 JSON 转换独立记账；单路径批执行，不另建 SQL/调用通道 |
| P1 检索脱离 task 生命周期 | `PlanExecutionDriver.cpp:794` 先 prepare；`ArtifactRetrievalPlanner.cpp:578/686/745` 搜索/向量读回/rescore | metadata binding 留控制层，实际 IO/search/rescore 移 Velox source/operator 的 pool、blocked future、cancel、stats；索引算法库保留为算子内组件 |
| P1 Native write 仍 JVM row path | `WriteOperator.cpp:156` IPC→`AbstractPaimonPageSink.java:81–99` GenericRow→SDK；`QuerySchedulingEngine.java:381` 默认 parquet | typed physical allocation + 唯一 Native columnar sink，Java 仅 SDK metadata/commit；覆盖 bucket/row-kind/sequence/delete/schema，删 row upcall，不能只改 file-format 字符串 |
| P1 REMOTE durable job 永久配置失败重试 | `SqlE2ETestBase.java:1998` lifecycle null → `DurableDAGControl.start:28`，`DurableArtifactGraphs:76` prepare 先于验证 | 实际 Shuffle 服务与租约 wiring；配置在 prepare/persist 前准入，旧任务用 fenced FAILED 原子终结；独立进程 restart/lost ACK，不用 CANCELLED 假装失败 |
| P1 能力与注册漂移 | Java operator table/FunctionMapper、Native registerAllScalarFunctions/classifyFunctionFamily 各维护声明；JNI capability 只填 rel/expr，函数 inventory 空 | 见下一节；构建期 manifest 与运行注册集合精确一致，优化期验 Native inventory，没有“静态 defaults 即支持” |
| P1 Window frame/sort 改写 | `WindowFunctionConverter.java:248/319` 统一 ROWS 与 NULLS LAST，未传 Rex/LogicalWindow 的 frame/NULL collation | 与参数修复使用同一转换入口，保留 ROWS/RANGE 和显式 NULLS FIRST/LAST；重复排序值与空帧真实 SQL 对照，不只检查函数名 |
| P1 Native 隐式 cast 改比较 | `SubstraitTypeConversion.cpp:239/326/845` comparison family 缺 is_not_distinct_from，把数值参数转 BOOLEAN 后包装 not(distinct_from) | Calcite 完整签名推导并显式写 Cast，Native 严格验证，不根据 output type 补 cast；typed plan 的 1/2、0/NULL、NULL/NULL 与 decimal scale oracle。公开 SQL 当前也可能先被静态清单拒绝，不能宣称已测其错结果 |
| P1 aggregate 返回/空集语义 | `AggregateConverter.java:334` 强制 AVG FP64；`VeloxPlanConverter.cpp:107` SUM0→SUM | 在唯一签名声明推导 AVG 完整结果类型，Native 实现 SUM0 的空输入/all-NULL/空窗口语义，不仅改名称；父子 schema 与 Arrow 输出一致 |
| P2 schema 约束丢失 | `ConnectorColumnTypeMapper.java:38` VECTOR→ARRAY 丢维度、全部 nullable、UNKNOWN→VARCHAR；源 Column 本身缺 nullable | 先补通用源类型约束，再由唯一转换入口保真；禁止缺参数默认 DOUBLE，逐层 schema oracle；不能只改 mapper boolean |

## 4. 类型与函数单一真源规格

语义声明由统一 protocol 契约拥有，复用既有 canonical capability inventory；不建立 AI/VECTOR 独立
proto，也不手改生成源。声明驱动 Calcite operator/type inference 与 Native Velox registration。
Java/Native 各只有一个到 Substrait 类型的转换器；框架转换不可避免，但转换规则不能独立手填。
注册 inventory 从实际完成的注册生成，带同一声明 fingerprint，Worker 注册、planner admission 与
task execution 均核验，不将缺函数当作支持。不保留旧 epoch 或自动降级。

| 语义 | 统一声明与拒绝行为 | 必须覆盖 |
|---|---|---|
| 数值 | 整数宽度/精确溢出、decimal precision/scale、显式 cast 裁决；禁止 default 值替换 | 最大/最小/fraction/overflow、decimal 38 位、Values 与 Window 一致 |
| NULL/三值逻辑 | 函数 null policy 与推导结果一致，typed NULL 不变成零 | 标量、list/struct/map 内嵌 NULL、filter未知、outer join |
| 时间 | DATE、timestamp unit、timezone 和会话规则由声明固定 | DST、负 epoch、纳秒及 JDBC/Arrow/Native 读回 |
| 复杂类型 | 有序 struct 字段 identity、list element type、map key/value/null semantics | 投影/rename、嵌套、empty与NULL、dictionary |
| VECTOR | element FP32、固定维度、可空性、有限数与距离语义 | 维度不得丢为普通 ARRAY，FixedSizeList NULL、NaN/Inf 拒绝 |
| FILE/BLOB | 受治理 reference 与 LargeBinary/结构字段，不把 URL 当授权 | tenant/URI/大小/deadline/cleanup、无 Coordinator payload |
| 函数 | 完整签名、return inference、null、determinism、错误、批语义、所需权限 | Calcite/Substrait/Velox 逐签名一致，未注册/非法签名在优化期拒绝 |

`FunctionNullabilityRegistry` 与 `FallbackRateTracker` 当前没有生产调用；自身测试通过不构成语义
或零 fallback 证明。实施时不再新增旁路 registry，迁移真正调用方后删除悬空声明和清单。

## 5. Lance 决策与互通验收

Paimon 1.4.1 已有官方 Lance 单文件格式插件；Iceberg 规范没有 Lance data-file 格式。因此 Paimon
表复用 Paimon 权威 snapshot/manifest，**无需独立 Lance connector**。独立 Dataset 自有
manifest/version，直接注册这种 Dataset 为表才有独立 metadata provider 的必要；两者不得互换
提交权威，选择来自 metadata 而非失败后换 reader。
[Paimon factory](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/LanceFileFormatFactory.java)、
[writer](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/jni/LanceWriter.java)、
[Iceberg 规范](https://iceberg.apache.org/spec/)、[Lance distributed write](https://lance.org/guide/distributed_write/)。

当前源码已经有 paimon-lance 依赖、FILE_FORMAT_LANCE split、Rust physical decoder 与真实 SDK→SQL
测试，旧“全未接入”描述撤销。Paimon SDK Lance 0.39 与当前 Native Lance 0.20 的互读必须实测，
不由两个单测 PASS 推导兼容。当前没有这组 public SQL 完整报告。
无 Lance runtime-unavailable skip，Release/ASan 中 Lance reader 测试通过，但不能代替多进程 SQL。

删除仅 reader 单测调用的字符串 filter C++/C ABI/Rust 入口；生产 connector 已禁止把 Velox
toString 送入 DataFusion。原来四项过滤断言必须迁移到真实 FilterRel/Velox connector，保留行与
locator 结果。Lance decoder 是存储组件，不允许借此引入第二 SQL 执行面。

2026-10-02 增量：该旁路已完整删除，C ABI 双端为8；四项原断言迁移至真实
Lance write/commit→canonical FilterRel→production connector→Velox Task，保留 score、
字符串、bounded batch、late configuration 不可变和[(1,2),(4,5)] physical locator。
原32项全部保留，按职责分为Reader21/Commit8/Filter4，共33项；尚未完成本窗口
Native全量测试，不能据此宣布SQL E2E或SDK0.39↔Native0.20互通。

## 6. 分阶段实施和冻结门槛

先修真实值损坏（Window、typed list NULL/decimal、Embedding batching），再固定生成式语义声明；
其后作为一个完整替换单元迁移 Arrow result/shuffle/spool，接入唯一 Native AI/retrieval/write 生命周期；
最后整体迁移 Calcite parser 和基于 Substrait 的 fragment/AQE。每项迁移保持相同断言与输出 oracle，
不能把“尚未支持”变成成功或通过双路径保兼容。Native 修改后重新执行完整 0–8，不拼接旧源码窗口。

内核评估、Feature materialization、索引/backfill 都使用同一 persisted DAG/attempt/owner/lease/
publication fence；Evaluation metadata 单测不能替代 Native metrics trusted report。Agent 与 Ledger
删除范围按真实调用/wiring 清单确认，不按包含 usage/fallback 字样机械删资源准入或错误处理。

性能实测至少包含：2/4/8/16 独立 Worker、冷/热缓存、skew、高 partition、spill、Worker loss、REMOTE
恢复；吞吐/P50/P95/P99、scan裁剪、dictionary保留率、copy count/bytes、serialization CPU、JVM GC、
Native RSS、表达式复用/执行覆盖及任务重跑次数。Velox ExprSet 不能未经证据标为机器码 JIT。
当前本机环境没有生产规模集群，因此没有行业领先或生产 SLO 达标结论。

每个 PASS 必须有冻结源码、命令、环境、有效用例和真实输出。编译/组件/Native局部 PASS、没有
运行报告或环境失败，都不能替代 public SQL→planner→Coordinator→Worker→JNI→Native→result 的证明。
当前具体执行结果保存在 [审计与实测账本](ai-native-roadmap.md#013-2026-10-01-终态输出拓扑顺序与规划对象所有权复验)。

## 7. 后台统计维护：身份、快照和持久处置的完整替换单元

当前 `IndexingBootstrap` 向 `IndexCommitHandler` 传入未绑定 session 的 resolver；
统计收集捕获授权异常后只记录日志，零 READY index 时仍会完成 commit-log barrier。
不能给后台线程捏造 user/tenant 或借用任意管理员 session 使测试变绿。
`CommitEvent` 的 tenant/table/snapshot/fingerprint 不等于读取数据的授权。

需要区分两种真实职责，二者不能互相代授权：

- **写提交元数据维护**：从精确 COMMITTED journal fingerprint 恢复原 principal 与
  `WriteAuthorizationReceipt`，重新核验当前 tenant/member 和目标写权限；仅读取已经提交的
  exact snapshot/schema/file 元数据，不开放行 reader 或任意文件系统访问。写收据不能凭空
  推导为全列 SELECT 权限。
- **索引刷新数据读取**：绑定持久 index-owner 的精确 RAW_READ scope，重新核验当前授权
  和撤销；独立于本次 committer。任何 row/mask policy 与原始读取的冲突都必须明确拒绝，
  不能绕过策略在 Coordinator 读取数据。

`getFileStatistics` 当前没有 snapshot 参数；Iceberg/Paimon 实现读的是 latest snapshot。
Paimon commit delta 使用 basename，而 statistics 使用完整物理 path，可能匹配不到文件。
统计表缺 tenant 和唯一幂等键，min/max 使用字符串；`created_at NOT NULL` 未赋值的实际 SQL
错误已修复并经真实迁移 PostgreSQL 四项验证，但这不解决前述权威和语义问题。

完整实现必须同时包含以下机制，否则仍未闭环：

1. purpose-scoped authority resolver 和精确 journal/index-owner 复验；不可恢复的旧来源明确拒绝。
2. 通用 immutable statistics request 携带 tenant、table incarnation、snapshot/schema 与完整
   physical file identities；两个 connector 都从同一 source snapshot 取元数据，不 fallback 到 latest。
3. tenant/source/column 唯一幂等键、typed bound 编码和同一事务 publication；重放与顺序不改变结果。
4. 每项 durable work 的 lease-fenced disposition：永久授权拒绝与暂时 DB/SDK 错误分开，原因
   与状态原子提交；不能静默 ACK，也不能让永久拒绝阻塞其他独立且已授权的工作。
5. pruning 按真实物理列 lineage 与安全边界绑定。Project/mask 之后的同名字段、join 中另一张
   表的同名字段，不能用于裁剪原始文件。

验收使用真实 PostgreSQL 权限与 journal、Iceberg/Paimon 文件及提交：覆盖撤权、跨 tenant、
同名重建、快照推进、delta path、重放、lease 失效、masked predicate 和实际查询结果 oracle。
目前只有数据库 timestamp/批次原子性已取得 PASS，其余没有实施或验收证明。

## 8. 调度验证：真实执行与结果消费者必须同在

`TaskManager.forHttpWiringOnly()` 只接收请求，不执行 Native 任务。删除旧 autoFinish 后，
`DAGCompletionEvaluator.awaitCompletion` 等不到真实报告。即便换成 Native executor，
OUTPUT 仍必须被真实消费者读到 EOF：`FlightResultServer` 此时释放结果，并通知 Velox
`setAllOutputConsumed`。因此同步等待 dispatch 返回后才读取结果会构成等待环。

最小验收 fixture 必须真实初始化 Worker 的 resource/arbitrator、Native executor、result/exchange
transport 与 TaskManager；独占并明确释放 JVM 全局 Native runtime。独立 dispatch future 启动后，
读取提前发布的 result endpoint，使用实际 task-ID ticket 消费 Flight 至 EOF，再等待 canonical
Native terminal report 经真实 receiver/validator/tracker/DAG。不得手造 FINISHED、直接通知成功、
使用假 registration server 或修改生产完成语义。

直接 dispatcher 的独立 QueryStateMachine 与 QueryManager 管理的 query 终态所有权不同：
前者验证真实 task/DAG 完成，后者必须最终 FINISHED。协议拒绝测试从真实报告仅破坏被测字段，
不能拼造“成功证据”。关闭后重新初始化并再次查询，必须证明 allocation listener 与资源 owner
没有串用。fixture 最小验证在 23:36 **7/7 PASS、0 skip**，包含两次 SELECT42 的实际结果、
关闭重建与独占 owner 拒绝；原编译失败历史保留。其余七类 routing fixture 迁移和全仓回归
尚未通过。

2026-10-02 增量：七类真实 fixture 原 r3 **45 项/1 failure/19 errors/0 skip**。
首因是调度断言只改变未消费的 performanceScore，真实调度读取 CPU/memory；失败关闭
又暴露 canonical HTTP task-ID 未映射内部 key、清理 DAG 不终结 pending completion，
以及关闭异常跳过 owned Native shutdown。fixture 改为真实评分输入、真实报告 graceful
drain 与异常累积清理；生产 HTTP 控制入口、live DAG future ownership 与中断响应已修复，
增加最近控制/生命周期测试。原范围 r4 唯一错误仍为一项未消费 OUTPUT；保留原无结果
relay 契约并接真实消费者后，十一类原命令 r5 **57/57 PASS、0 skip**，含30次并发
property 和真实 CANCELLED/重建验证。加载的是此前已打包 Native，不是新类型库的证明。

Native scalar/list/VirtualTable literal 单解码和精确函数绑定已实施，新实际 Task oracle
验证 mixed short/long DECIMAL、NULL/nested arrays 和显式距离 cast。当前 Step0 PASS；
Step1 r1 编译命名空间错误、r2 一个 deprecated API warning 均如实保留，修复后 r3
configure/build 零错误零警告 PASS，Step2 全量 **3531/3532 PASS、1 FAIL**。空 schema
未显式编码，缺失 schema 又在 Native 被默认为 BIGINT；完整 VirtualTable canonical
expressions 与 caller 迁移已实施：缺失schema显式拒绝、0-column schema显式编码，
Java无生产caller的独立reader和deserialize API删除，原两组协议测试保留并增强，
最近命令 **16/16 PASS、0 skip、17.409s**。不是仅改变错误字符串。
新窗口Rust Step0零错误/警告 PASS，2m13s；Release Step1正在执行。
该 Native 源码已经改变，
前一窗口3500项 Release/ASan和打包通过不能作为当前源码验收。

后续完整单元见 [函数契约](ai-native-function-contract-implementation.md) 与
[Embedding用途](ai-native-embedding-purpose-implementation.md)。普通Filter/Project/
retrieval/Exchange的RelCommon.emit作用域缺陷仍在只读根因复证，未声明已修复。
