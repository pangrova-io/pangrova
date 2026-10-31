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
| P1 Native write 仍 JVM row path | 当前生产 `PlanExecutionWriteSinks.cpp` canonical stream→Java connector sink；未注册的死算子及其独立编码器已删除，原14项值/边界断言迁 `WriteBatchStreamTest.cpp`，不作为端到端通过证据 | typed physical allocation + 唯一 Native columnar sink，Java 仅 SDK metadata/commit；覆盖 bucket/row-kind/sequence/delete/schema，删 row upcall，不能只改 file-format 字符串 |
| P1 REMOTE durable job 永久配置失败重试 | `SqlE2ETestBase.java:1998` lifecycle null → `DurableDAGControl.start:28`，`DurableDataBuildGraphs:76` prepare 先于验证 | 实际 Shuffle 服务与租约 wiring；配置在 prepare/persist 前准入，旧任务用 fenced FAILED 原子终结；独立进程 restart/lost ACK，不用 CANCELLED 假装失败 |
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

### 2026-10-03 TIMESTAMP 宽范围与高精度的完整边界规格（未实施）

不能统一改微秒，也不能按单个值选择单位。当前 `PlannerTypeSystem` 允许精度0–9；
`LiteralConverter` 使用 Substrait precision_timestamp 的 signed-int64 scaled ticks。
p7/9999年和p8/0001年属于当前可表达输入，但 Velox 默认 Arrow exporter 的
`Timestamp::toNanos()` 会溢出；微秒又丢有效尾数。原 p9/2300年拒绝断言保持：更宽
SQL literal 本身是另一个契约变更，不得删除拒绝测试后宣称全年纳秒已经支持。
固定 Velox string CAST 只保存6位小数，不能用未经修改的 CAST 伪装 lossless lowering。
[Substrait 类型](https://substrait.io/types/type_classes/)、
[固定 Velox Timestamp](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/type/Timestamp.h)。

推荐在唯一 Velox↔Arrow 边界采用标准 extension 机制定义应用类型
`pangrova.timestamp`，固定 storage 为 `Decimal128(38,9)`；unscaled value 是
`seconds * 1_000_000_000 + nanos`。全列全部 batch 同一布局，metadata 严格携带
version、声明 precision、NTZ/UTC 语义及 epoch，必须与 canonical LogicalType 一致。
它不是 Apache 官方 canonical timestamp；普通客户端能读取 Decimal storage 与 metadata，
获得 SQL 时间对象必须走统一 extension-aware decoder，不能宣称自动兼容。
[Arrow extension 规范](https://arrow.apache.org/docs/format/Columnar.html#extension-types)。

完整替换单元必须同时覆盖：

1. 导出在 `toNanos()` 之前执行；导入验证 exact storage/metadata 并以 floorDiv/floorMod
   恢复负 epoch 的 seconds+nanos。递归 ARRAY/MAP/ROW、NULL、slice、dictionary/constant；
   普通 Decimal 不得被误认作时间。转换 buffer 由 Task pool 分配并记录真实 copy bytes。
2. shared serde 的 result/shuffle、预期 schema 的 `ResultBatchStreamReader`、write sink
   与两个 Lance writer 使用同一策略；不可只改最终结果展示而让 Exchange 继续溢出。
3. 既有 `ResultColumnLogicalType`/LogicalType 通用契约贯通 precision/zonedness。
   `DAGStagePlanBinder` 与 `TaskRequestParser` 当前实际只处理 VECTOR；必须一般化现有
   契约，不新增时间专用协议，也不在 Native 从物理 TIMESTAMP 猜精度。
4. `ColumnarBatch`/`BatchValues`/`ArrowBatchDecoder` 递归识别 extension，REST/CLI/JDBC
   复用同一结果解码。CTAS 不得把任意p>6重写为p9或丢声明 precision。
5. 存储 SDK 限制单独核验：Paimon1.4.1 p7–9 Arrow conversion 有 unchecked int64 nanos
   乘法、另有固定p6提取；Iceberg timestamp_ns 本来只有int64 nanos范围。不能把
   Decimal extension 冒名为 SDK Timestamp；不支持的 schema/range 在 publication 前
   明确拒绝，且不允许静默截断、换 storage 类型或失败后换 reader。
   [Paimon 官方转换](https://github.com/apache/paimon/blob/release-1.4.1/paimon-arrow/src/main/java/org/apache/paimon/arrow/ArrowUtils.java)、
   [Iceberg 规范](https://iceberg.apache.org/spec/#primitive-types)。

验收保留原 p9 范围和拒绝断言，新增 p7/9999、p8/0001、2025年的`.123456789`、−1ns、
NULL/nested/slice/dictionary、普通 Decimal 混合、metadata tamper、OOM与 owner 恰好一次
释放；实际 Task→PartitionedOutput→官方 Arrow→Java/client、双 driver、压缩和分布式
JOIN/GROUP/ORDER 必须有精确值 oracle。存储另用真实 SDK/public SQL 差分与不发布证明。
本节仅为源码与一手资料复证后的实施规格，尚无该完整单元的构建或运行 PASS。

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

### 2026-10-03 输出缓冲授权与实际背压（源码已接线，待实测）

Java 与 Native 的 direct-result producer 都要求正的 canonical serialized-buffer grant；
无输出的 compute 不补默认授权。该 exact grant 经 `makeRunnerConfig` 传至 Velox
`max_output_buffer_size` 和 `max_page_partitioning_buffer_size`，会话属性不能放大。
原来遗漏的 `native_memory_bytes` 同时传至真实 Task root-pool capacity；不能用 node 总预算
替代 Task 的授权上限。

Pinned Velox `OutputBuffer::enqueue` 在入队后才判断阈值，因此输出配置是实际背压阈值，
不是总驻留内存硬上限。单页、各 driver 编码中间态和 transport 在途 owner 的 overshoot
仍由 Task/node pool 硬上限及实际多 batch/慢消费者/取消峰值测量约束，不能宣称 grant
就是已证明的 all-allocator 峰值上限。

`NativeTaskStats.backpressure_wait_ms` 仅累加真实 Velox runtime metric
`blockedWaitForConsumerWallNanos`；不把 `blockedWallNanos` 中的 IO/connector 等等待
算成背压。协议和配置专项保留 0-grant 拒绝与 exact grant oracle；真实 JNI/Flight
large-cross 的等待、恢复、EOF、取消、资源归零测试仍待最新 Native package 后执行。

活等待另用 `NativeTaskStats.consumer_blocked_drivers`：唯一 `getTaskStats` 路径读取
actual `TaskStats.numBlockedDrivers[kWaitForConsumer]`。累计 wall time 只在解除阻塞后写入，
不能拿 `!isFinished`、内存已申请或尚未消费当作 driver 正在等待的证据。JNI 测试现在先
观察实际 blocked count，持续观测后启动真实 Flight 消费，再断言累计等待与归还；尚未执行。

### 2026-10-03 真实 fixture 的类加载与迁移资源边界（待完整编译/实测）

共享 Native fixtures 的唯一实现位于 Worker test source 的独占
`org.pangrova.testing.nativefixture` 包。`native-fixtures` classifier 只包含该包，
不发布任何 JUnit 测试类或 Worker production split package。初始化复用生产
`NativeRuntimeBootstrap.initialize` 和真实共享 node pool/arbitrator，未复制 runtime
协议编码；依赖包内 handle read lock 的本地测试 lease 不进入 classifier。

ShuffleService 与 Metadata 同在真实 E2E classloader 时，原共同 `classpath:db/migration`
位置会发现重复 V1–V6，不是数据库已提交历史冲突。Shuffle 的六个原始 SQL（版本/内容
不变）现独占 `classpath:org/pangrova/shuffle/migration`；生产 Store 和直接 migration
测试消费同一位置。Metadata 继续原目录，未过滤其他 JAR 的影子资源来绕过重复发现。
真实 PostgreSQL/Flyway、MinIO 和 mTLS gRPC 的原成功、错误、暂停、取消 oracle 迁移
仍在实施，不能把编译、协议边界测试或 RecordingService 算成数据面闭环。

### 2026-10-03 外部读取 drain 与 canonical failure（源码 checkpoint，未验收）

原读取路径先移除 session，再 `Future.cancel`，且取消向 reader 队列放入 EOF，不能证明
RPC 或 JNI pump 已退出。现在 session 封闭准入并保留 sources/pumps，独立等待实际
binding finally、RPC `onError/onCompleted` finally 和 runnable finally-exit；尚未执行的
pump 只有 CAS 阻止其未来执行才能计为无生产者。取消唤醒抛出取消错误，不产生成功 EOF。
关闭失败保留 exact session、Native handle 和 grant 重试；真实 drain 成功后才删除 owner，
接入既有 `releaseInputs → JNI final receipt → connector owner → memory refund` 顺序。

原 `ExecutionError → formatted string → INTERNAL` 会丢失 external corruption 的类、码、
重试策略和组件。TaskLifecycle 现在拥有首个 canonical error；合法 ingress failure 的
acceptance callback 仅在内存中记录它，在消费者被唤醒前完成，不持 lifecycle 锁调用
ingress/cancel/IO。统一 snapshot 同时读取状态和 error；cleanup 追加消息不改首失败分类。
无已知 canonical 分类的 Native 异常仍明确报告 INTERNAL，而非从字符串猜测错误类型。

旧 External 的三项和 ByteEgress 十三项业务替身测试现迁至真实 migrated PostgreSQL、
MinIO、生产 ShuffleServerRuntime 和 mTLS RPC。透明 relay 只观察真实帧或注入指定网络
故障，正常 ACK、授权、续租、commit、finalize 都由生产服务完成。原 1MiB+7 opaque
payload 仍只证明 chunk 字节边界；另用标准 Arrow batch 证明 JNI ExchangeNode/Flight
实际值的验收 oracle，绝不通过修改 IPC 对齐或伪 decoder 将 opaque 字节冒充 Native 成功。
三项原 External 场景另外保留 exact attempt/read token、丢失真实 terminal 后的 canonical
DATA_CORRUPTION/non-retryable TaskReport，以及实际暂停 RPC 取消/drain/CANCELLED 回执。
这些源码尚未经过最新全 testCompile/JNI/容器执行，不能计 PASS 或大规模部署证明。

当前真实 Arrow Exchange 状态为 **NOT_CONNECTED**：fixture 使用 canonical
`BATCH_SERDE_ARROW_BATCH`，现 Native `resolveBatchSerde()` 仍拒绝该值；不能改用另一个
serde 来使测试通过。上述三项真实 JNI 测试必须揭示该准入缺口，并在唯一 Arrow 链路
实施后重新验收。

### 2026-10-03 真实 channel lease 与关闭所有权（源码 checkpoint，待编译/实测）

RemoteShuffleClientPool 的唯一获取接口现在返回 lease。缓存 eviction 仅 retire lookup；
producer/consumer 尚借用的 channel 不会被提前关闭。Task 的 commit/renew 共用一份
lease，每 endpoint 的 stream 拥有实际 lease；它们仅在写操作、finish 与 RPC terminal
真实 drain 后归还。实际 channel `awaitTermination` 超时/中断会抛出并保留 owner，
shutdownNow 本身不是退出证据。Pool close 封闭准入，try-all join；失败可重试，异步
eviction 失败也必须向调用者传播，不只记录日志。

ByteEgress 的候选 task 在发布前即登记所有权；duplicate/conflict 只释放候选 lease，
不替换正在使用的 producer。关闭等待实际 admission 退出，再清理所有已发布或未发布
候选 owner。新真实服务测试保留原十三项，并增加 duplicate/conflict 的持续写入/单次
commit oracle；另增加四项真实 TLS channel idle、active lease、join 中断和并发关闭
测试。实际短 TTL READ/WRITE 续租均由生产服务签发，不篡改时钟、capability 或 ACK。

无生产 caller 的 JVM data-pull `BatchResult` 与默认 EOF 方法已删除；TaskExecutor 只保留
八项必须实现的控制面方法。DTO 工厂断言不替代真实多 batch、背压、取消及失败测试。
本次删除均可由 Git 恢复；所有当前源码仍需统一完整 testCompile 和最新包后的实测。

随后复核确认 session 原来会在 binding/pump join 失败后仍执行 `closeSources`，从而提前
归还 reader 的 channel lease。现只有实际 last-exit 屏障成功后才能关闭 sources；中断、
超时或 join 失败保留全部 source/client owners，已有 session 追踪供下一次清理重试。
lease 一旦开始归还即禁止再次 `client()` 取用，即使 channel drain 失败仍可重试 close。

新增三项纯真实 runnable/binding 生命周期边界测试，不含模拟任务成功；真实 TLS 测试
另加完成实际 reader 后 interrupted lease return、owner 保留、拒绝 stale-use 与真实
channel drain 重试。它们不能代替 session 与尚未退出 pump/reader 共同持有资源时的
真实 E2E：该组合验收仍 **PENDING**，须在唯一 Arrow exchange 接通后验证实际阻塞、
join 超时/中断、全部 RPC/最后 owner 退出与预算最后归还。

### 2026-10-03 Worker 共享 ManagedChannel 准入（源码 checkpoint，未验证）

旧 `maxConnectionsPerServer` 构造参数只有校验，没有消费；Caffeine 大小限制只约束 lookup
条目，不能约束仍被 RPC 借用的 retired channel。现删除该无效参数与构造重载，Worker
通过同一 `WorkerTaskOwnership` 向 producer/consumer 两个 pool 注入唯一 admission ledger。
配置为 `worker.shuffle.client.max-owned-channels`（默认 100）与
`worker.shuffle.client.max-owned-channels-per-endpoint`（默认 8），校验
`0 < per-endpoint <= total`。地址按规范化 host、port、精确 TLS identity 记账；它不是
DNS/IP 合并策略。Caffeine `maxCachedChannels` 只控制缓存 retention，与 owner 额度正交。

创建前原子占额；creating、缓存中 idle/active、已退休 active 以及 join 失败 owner 均占额。
构造完成后的 ownership 转移不退款；只有构造实际退出且真实 `ManagedChannel.isTerminated()`
成立才归还。池关闭不封闭其它池，共享 Worker 关闭才 seal ledger。所有 channel 建立、TLS
文件读取和 shutdown/join 都在 admission 锁外，缓存维护不回收尚未发布的创建者。
这些是 Worker 管理的 ManagedChannel 数量上限，不是 TCP、RPC、FD 或 SDK heap 硬上限。

producer 在 `NativeTaskExecutor.createTask` 调用 JNI 创建前取得 commit/renew 与全部已知
endpoint leases；额度不足显式拒绝，不等待、不换网络路由。类型化错误沿现有 canonical
`ExecutionError`/`TaskReport` 上报 ADMISSION / `SHUFFLE_MANAGED_CHANNEL_LIMIT` /
retryable=true / `remote-shuffle-channel-admission`，可信 Native 首错误仍优先。
失败候选归还已取得的 leases，剩余 channel 仍由 pool 保活并占额，不能因候选失败假退款。

启动时先登记 partial owner，再启动/注册实际 transports；初始化失败清理 try-all，join
失败保留 ledger/owners，禁止旧 owner 未退出时重新 bootstrap。TaskManager 发布后创建的
实际 cleanup scheduler 仍由 assembly 持有，先 shutdown/join，再关闭 manager；不能因
manager 已发布而丢失 scheduler 所有权。shutdown 失败保留 pending，
不会倒数成功 latch，下一次显式 shutdown 可以重试；实际全部 drain 后才允许替换 owner。

依赖关闭复审还确认：TaskManager 原来在 result/exchange/task-release/monitor join 失败后
仍关闭 Native executor，令后续 STOP_RESULT_SERVER 等唯一控制命令被 closed/handle0
校验永久拒绝。现各依赖 try-all 后，任一失败都保留底层 executor；仅真实全部退出才
关闭它。新增两项真实 JNI GET_METRICS 控制 owner＋实际线程中断/未退出屏障→失败→重试
测试源码，不合成任务完成，也不作为实际活跃 Flight RPC 失败闭环证明，运行仍待最新包。

保留原 pool 11 项、真实 TLS lifecycle 5 项；新增 admission primitive 4 项、真实 channel
pool 3 项、partial owner 3 项、配置 1 项、typed report 1 项，以及真实 TLS reader 3 项和
真实 Worker/JNI runtime→前置 endpoint 拒绝→canonical failure→预算归还 1 项源码测试。
后者只证明真实 admission 失败闭环，不是成功 Native SQL 或大规模性能证明。当前本单元
未运行编译/测试，所有新增实测状态 **PENDING**；须由统一构建窗口执行 closest，再完整
回归与最新 packaged JNI/容器验收。既有 Arrow Exchange 未接通状态未被此单元解决。

#### 2026-10-03 启动、发布与失败关闭补充（源码已改，运行待统一验收）

同一 Bootstrap 实例采用 one-shot 状态：NEW → STARTING → RUNNING → STOPPING →
STOPPED；失败清理进入 CLEANUP_PENDING，仅显式 shutdown 可重试。配置、启动、关闭
准入使用短锁，实际 I/O 在锁外。STOPPED 不可再 start，成功 latch 不重置；不能因旧
一次关闭成功而让下一次启动的 awaitShutdown 提前返回。

回滚和正常 shutdown 共用同一依赖关闭路径。heartbeat/HTTP/CDC/task owner 独立
try-all；任何失败阻止进入 writer/function/native 依赖阶段。function factories 在调用
初始化方法前登记实际 owner，关闭 Exception 或 Error 都保留失败 owner，成功者才移除。
新 owner 先存 pending 引用再分配 ledger entry，避免已发现资源在 entry 分配失败时丢失。
真实 JVM OOME 尚未实测，不能把此结构性异常安全修复写成 OOME 验收通过。

调度器 shutdownNow 后必须 await 真实线程退出，HTTP stop 失败不再吞掉。Native runtime
实际关闭成功后立即更新 refs.nativeAvailable；后续服务 lease、观测或预算清理失败仍保持
CLEANUP_PENDING，重试不得重复关闭已销毁 runtime。预算 ledger 仅在依赖 owner 全退出后
清理，不把调用 close/shutdownNow 或 Future.isDone 当作资源退出证据。

复审发现共享 ManagedHttpClient.close 仍在 await 超时后只 shutdownNow、不再等待真实
退出，且中断被吞为返回；WorkerRegistrationClient 依赖它。该 shared common owner
根因已由统一主线程修复。最近实际 HTTP 13项、32模块 clean 编译及 common13/SPI3/
Worker76 共92项纯控制/owner测试通过，0失败/跳过；包含实际退出、失败保留、重试
和启动前 owner。Bootstrap 不能仅凭调用 shutdownNow 宣称 executor drained。
该证据不覆盖 Native runtime、真实业务 RPC 或 JNI dependency drain。

HeartbeatSender.stop 的 running CAS 仅封闭发送，不再让第二次 stop 跳过失败的实际 HTTP
owner close。取消 ScheduledFuture 不再被当作 callback 退出证明；Bootstrap 仍必须等待
实际注入 scheduler 退出。增加真实 managed HTTP executor 中断失败→running=false→close
重试的纯 owner 测试源码，不产生或伪造 heartbeat 业务响应，待 common 同源修复后运行。

pool publication 异常把已退出构造的 candidate 标记 retired，creating 在 finally 精确
减一次。未完成 publication 的 owner 仍由 construction ledger 持有，实际 channel join
后才删除/退额。endpoint 计数使用原位整数，失败退款不需要重新分配 boxed counter。
新增真实并发 ManagedChannel creation/cache publication/losing candidate drain 测试源码；
它不证明人为 OOME，也不证明实际业务 RPC。

压缩 task 文本精确接受 none/lz4/zstd，全部映射外层 COMPRESSION_CODEC_NONE；列式流
内部自描述压缩由 Native codec 负责，egress 不 trim/uppercase、不再次压缩。新增实际
durable service RPC 检查官方列式 stream 原始 bytes、双 checksum、row/sequence/commit
均不变；其输入为官方未压缩 stream，只证明外层 opaque 传输，不冒充内部 LZ4/ZSTD
编码或 Native task 值闭环。对应 Native 三种编码仍由统一构建和实际值测试验收。

已删除完全无生产调用的 ShufflePageValidator、两个仅手写私有 13-byte header 的测试
文件（17 + 12 项伪 header oracle）及其 checkstyle exemption。原真实 chunk/service/JNI/
Native 测试未删除；历史 naming rename 示例保留。删除由 Git 可恢复，不新增替代私有格式。

新增纯控制/owner测试13项（lifecycle4、function owners3、dependency cleanup1、实际
channel并发1、heartbeat HTTP owner1、启动前实际owner3）已包含在上述最近通过范围。
实际 Bootstrap/JNI owner 重试/one-shot/并发/配置失败回滚4项和先前实际 JNI TaskManager
dependency drain2项保留，仍未运行，状态 **PENDING**；等待最新全量 Native/ASan/打包。

### 2026-10-03 唯一列式 Exchange 的原子迁移规格（部分落地，Exchange 未接通）

以下为实施前根因与完整替换规格，不表示已经通过全链路验收。2026-10-03 当前源码
已按本节迁移 canonical format、typed producer/consumer binding、配置和 shared adapter；
Java协议/planner最近73项、Coordinator/metadata纯契约27项通过，当前SDK13主Native
重建与相同失败范围复证正在执行，public SQL/JNI和分布式链路仍未验收。

根因复证不是 serde 枚举名称不一致这么简单。原生产 producer 的
`ShuffleBatchSerializer` 写私有 Native row page，`SubstraitRelConversion::resolveExchangeSerde`
仅接受该页面；现有 Arrow serde 只有五参数 decode，pinned SDK 的 `Exchange::getOutput`
却在 `supportsAppendInDeserialize()` 为 false 时选择另一个 row-iterator 接口。
Arrow adapter 未实现该接口，直接放开枚举会在真实执行时失败。若仅让 decoder 忽略
字段名，还会掩盖 producer 逻辑列名与 consumer 物理身份列名的绑定漂移。

实施必须一次贯通以下边界，不保留第二种页面或运行时降级：

1. 唯一 Velox↔Arrow serde 移至 `velox/serde`，结果与 shuffle 共用；`columnar` 仍仅
   负责标准 Arrow stream 与 task allocator，不解析 SQL/执行计划。shuffle 只能依赖该
   明确的公开 serde 边界，不能引入 executor/result owners。
2. serde 实现 SDK 六参数 append decode 并显式声明支持追加。offset 非负，非零时
   必须存在同精确 schema 的 output 且不超出已存在 rows；总 rows checked arithmetic。
   实际 decode/验证失败前不发布新的 result；prefix 不被覆写。输入 page 的 C Data 与
   allocator owner 必须存活到最后消费者，不使用 raw-pool-key 的 thread-local cache。
3. 唯一 encoder 使用 Arrow IPC stream 的 schema/batches/committed EOF；每个 shuffle
   page 必须至少一行，空 partition 使用 terminal control，不能发空 page 触发 SDK 的
   positive-row invariant。零列非零行必须保留 cardinality。NULL、嵌套值、dictionary/
   constant、timestamp precision、decimal 和 exact field identity 保留原断言。
4. compression 使用 Arrow 标准 buffer compression：none、LZ4_FRAME、ZSTD；不得把
   raw LZ4 block、私有 header 或整页二次压缩冒充 IPC。保留压缩阈值与 task batch cap，
   超大单行明确失败；实际序列化后验证大小，不能仅依赖平均行估计。编码/解码线程和
   buffer allocation 必须归属 task，不借用无限 global CPU pool。
5. planner fragment/stage、canonical task admission、producer、consumer 和 official
   Arrow external-service fixture 同步使用唯一 `BATCH_SERDE_ARROW_BATCH`。旧枚举号
   保留为 reserved，迁移全部 caller 与 descriptor golden，经唯一 generator 双端生成；
   旧页面明确拒绝，不加协议兼容开关。source/consumer schema 从同一绑定快照按 ordinal
   生成，不能丢字段名、放宽 schema equality 或按名称猜测映射。
6. reader cancellation、RPC terminal、Native I/O callback finally-exit 和 JNI pump
   last-exit 均完成后才释放 source/handle/allocator/grant。关闭超时或失败保留所有 owner，
   task report 不得伪装成功 EOF。IO callback 不在自身线程 join 自己；所有跨线程持有
   的 batch/pool 均有明确 shared ownership。

验收先证明真实 SDK Exchange 对两页追加、NULL/空 partition、零列 cardinality、精确
schema 失配、EOF 截断与三种 compression 的值保持；随后执行完整 Release/匹配 ASan
及原完整用例。重新打包后，运行真实 PostgreSQL/MinIO/mTLS external service → canonical
task → JNI Exchange → Flight 的多 chunk 原值、损坏终态、暂停取消与最后预算归还。
单进程近测不是独立多 Worker、重启恢复、性能 SLO 或生产可用证明。

官方依据：[Velox VectorSerde](https://facebookincubator.github.io/velox/develop/serde.html)、
[Velox task-local vectors](https://facebookincubator.github.io/velox/develop/vectors.html)、
[Arrow IPC compression](https://arrow.apache.org/docs/format/Columnar.html)、
[Arrow C++ IPC options](https://arrow.apache.org/docs/cpp/api/ipc.html)。网络、TLS、IPC 和
字符串/嵌套布局转换不能承诺绝对零拷贝；每个必需转换必须测 copy bytes、CPU 和峰值
resident bytes，且 owner-last-exit 与正确性优先于缓存/零拷贝宣传。

## 9. Worker 重启身份与查询物理退出（2026-10-03 修复规格，未验收）

当前注册、heartbeat 和 delete 按 `worker_id` 更新，TaskAttemptId、RuntimeInitConfig
与状态接收没有同一个 process incarnation/grant。重用名称不能区分前后进程，旧
POST/heartbeat/DELETE 迟到可能影响新进程。以下是对生产 caller 的修复设计，不是
已实现的能力，也不以 registry 组件测试代替真实重启故障测试。

### 唯一注册 authority 与启动顺序

- common v2 定义唯一 `WorkerRegistrationIdentity`，包含 worker id、16-byte process
  incarnation 和 backend 签发的正 generation；TaskAttemptId target、RuntimeInitConfig
  及通用注册控制复用该类型，不新增 vector/AI task proto 或私有 JSON identity。
- WorkerRegistry 的同一 PostgreSQL 事务维护 current registration 与 compact issued
  nonce grant，generation 使用不回退序列。相同 UUID/不可变启动 deadline 的重试只
  返回原 grant；仍有效的其他 incarnation 冲突。退休 nonce 即使迟到重放，也不得重新
  签发代际。仅 current row 而不保存 grant 的方案不能防止这个 ABA。
- 顺序是 reserve → 带完整 grant 初始化 Native → 注册实际函数 → GETCAP → 真实
  HTTP endpoints/inventory activate → admission READY → heartbeat。不能在 Native
  init 后才注册 generation，不能 generation=0 准入；RESERVED 不是可调度 ACTIVE。
- heartbeat/drain/retire 对完整 tuple、有限 deadline 和 operation sequence 执行 CAS；
  先通过控制 owner/epoch 验证，再提交 backend 更新，最后发布 cache 与 usage。
  旧进程不能因租约失效而自行 renew 到新代际。FENCED、lease expiry 或新进程注册
  均不证明旧进程已经停止执行。
- 原 Leaf/DML、durable stage 和 speculative copy 都必须先选择 exact frozen tuple，
  再写入唯一 issued request；不能造请求后再按 raw worker id 重新选 owner。
  当前 query-frozen eligibility 尚未生产接线，是该原子迁移的真实依赖。
- 新进程对旧 target 的 task/seal/runtime/ticket 在产生 owner 前明确拒绝，不生成
  NoOwner 假退出回执。旧真实 drain receipt 可用于 exact issued owner 的清理，却不能
  授权结果/写入发布。状态接收必须区分 cleanup authority 与 current publication fence。

机制参考：Kafka 的 process incarnation、单调 broker epoch 与 fenced readiness 提供
注册幂等及启动隔离；Pangrova 在已有 PostgreSQL authority 中实现上述事务边界是本
项目的设计推导，不复制 Kafka 的日志 authority 或额外引入 etcd。
[KIP-631 官方规格](https://cwiki.apache.org/confluence/spaces/KAFKA/pages/158864865/KIP-631%2BThe%2BQuorum-based%2BKafka%2BController)。
名称与历史对象身份分离参考
[Kubernetes UID](https://kubernetes.io/docs/concepts/overview/working-with-objects/names/)；
exact row CAS 与统一锁顺序依据
[PostgreSQL row locks](https://www.postgresql.org/docs/current/explicit-locking.html)。

### 查询终态不是物理资源归还

实际 QueryManager terminal listener 会立即释放 quota，logical active count 先减少，
再 cancel/cleanup；失败只能登记 retained retry。因此 awaitAllQueries 当前不是物理
退出屏障。取消 retrier 的 receipt 判断并不能替调用者等待所有 issued attempts。

修复必须在同一 CoordinatorTaskTracker issued authority 内逐项匹配真实 drain receipt，
支持多个有明确生命周期的退出观察者，而非覆盖 speculative listener。client 可见逻辑
终态与物理 drain 分离；配额归还、owner 清理、shutdown 完成只能发生在真实物理屏障
之后。未知、未收齐或清理失败保持 owner/issuance 可重试，不靠空集合、ACK、Future
终态、interrupt 或租约超时伪造退出。匿名 tenant slot 还须迁移为 exact tenant/query
reservation；duplicate/unknown release 不得退还其他查询的容量。

### 可执行验收

1. 真实 migrated PostgreSQL 两连接验证同 UUID/lost ACK 幂等、不同 UUID 并发只有一个
   live grant、旧 nonce 不复活、旧 heartbeat/delete 不改变新 owner、failover 不回退代际。
2. 独立 Worker A → B 重启，透明暂停并释放旧 POST/heartbeat/DELETE/status；B 拒旧
   tuple 且真实 Native handles/资源不增加，A 的实际退出可被记录但不能 commit。
3. 暂停真实 Native 执行/远端 I/O，触发逻辑 FAILED/CANCELED；配额与 shutdown 屏障
   必须仍持有，直到 exact physical receipt。重复/未知 release 与另一查询在途并发时
   不可释放其容量。失败 release 必须保留可恢复 owner，不报告完成。
4. reserve 后 Native init 失败、activate inventory 漂移、半启动超时与撤销控制 owner
   都不得开放 task readiness；必须验证真实 HTTP/JNI 路径，而非仅配置或序列化。

上述生产重启、持久 reservation 与全查询物理退出验收均未执行，保持未完成。
