# 数据库内核逐项架构核对与修正

调研日期：2026-10-07。状态：实施中；本文件不作为全链通过或“业界最优”证明。
源码基线：`058fb13a2f260125f2f9c7a2289550c3442a1e05` 加当前工作区。
既有改动和用户暂存删除保留；固定依赖与当前工作区行为分开核查。

## 已纠正的模型调用边界

数据库保留 AI SQL、自动 Embedding、分类、提取、多模态调用和模型重排。
独立推理服务承担权重、模型 runtime、CPU/GPU serving 和 provider 协议适配；
数据库承担类型规划、权限、准入、批处理、取消和受 execution-owner fencing
保护的结果发布。服务可部署在同一集群的独立资源池或远端。
Agent 对话、工具循环和工作流编排、Usage Ledger 累计 token/成本记账不在范围。
单请求上下文/输出长度限制与有界内存、网络、尝试次数和并发准入仍必需。

这是根据公开机制和本项目约束作出的设计推导，不是对厂商内部实现的猜测。
[Snowflake AI SQL](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)、
[Databricks ai_query](https://docs.databricks.com/aws/en/large-language-models/ai-query)、
[BigQuery remote inference](https://docs.cloud.google.com/bigquery/docs/inference-overview)
均支持数据库内的模型调用入口，外部服务与数据库分层并不意味着删除 AI SQL。
厂商允许的部分结果、兼容 fallback 和使用账单不自动成为本项目要求。

## 固定 Velox RPC 与最新版文档的差异

当前固定 SDK `3da0390118fa4b0addbfbdeb03f438ec1b1237a6` 已存在
`velox/expression/rpc/AsyncRPCFunction.h`、`velox/exec/rpc/RPCOperator.*`、
`RPCPlanNodeTranslator.*`、`velox/common/rpc/IRPCClient.h`。
首选复用 Velox RPC 扩展，不创建第二套执行器或集群调度器。
[官方异步 RPC 指南](https://facebookincubator.github.io/velox/develop/async-rpc-functions.html)
已发展出 typed payload、backend admission 和流控；必须核对固定源码，不能用
最新版接口说明证明当前 SDK 的能力。固定源码的重要差异：

| 检查项 | 固定源码事实 | 接入要求 |
|---|---|---|
| 错误与结果类型 | `AsyncRPCFunction::buildOutput` 默认 VARCHAR，错误变 NULL | typed Arrow 结果校验；普通调用失败必须传播，不默认吞错 |
| 真正批量 | `IRPCClient::callBatch` 默认逐请求 fan-out | Arrow Flight 真批次覆盖默认方法；按后端原生批处理能力定边界 |
| 并行度 | `RPCPlanNodeTranslator::maxDrivers` 对 BATCH 限制为 1 | stage/task 扩展并行度、单算子批量上限及尾延迟必须实测 |
| 请求与响应 | 固定 `RPCRequest.payload/RPCResponse.result` 为字符串容器 | 字符串可承载二进制 IPC，但不能据此证明共享所有权或免复制；比较实际 IPC 复制量与受管理 SDK typed payload 补丁，不把向量/媒体转成 JSON 文本 |
| 输入归属 | `RPCOperator::addInput` 预加载并 flatten 输入 | 测量分配/复制，保留选择行与 passthrough 正确性 |
| 超时与取消 | future 完成不等于 transport/服务 owner 已退出 | revoke、stream cancel、transport drain 和最后资源 owner 必须真实关联 |

当前生产仍是 `JvmScalarFunction::apply`→JNI `Object[][]`→
`GovernedAiScalarFunction`→`AiInvocationService`→Worker provider HTTP；
text provider 的 batch 会分发逐提示请求并等待 `allOf.join`。
现有权限、模型版本、媒体引用和资源治理必须迁入服务边界，不能在迁移中删除。
完整接入需最终 Substrait 中的标准扩展携带模型 binding，Native 转为 RPCNode，
服务回传 typed schema/row identity，数据库确认输出契约及发布权威后提交。
CASE/IF、AND/OR、NULL 和非确定性语义必须保留；不能把条件分支里的模型调用
无条件提升到上游 RPCNode。注册一个新 RPC 函数但仍走旧 JNI 不是闭环。

进一步核查固定 RPC 源码后，确认该 SDK 没有自动把任意标量表达式改写成 RPCNode
的通用转换器；注册 stub 只报告未完成规划改写。SQL→Substrait 的关系改写须显式
保存选择行、分支守卫、passthrough 和 NULL 语义，不能把“注册成功”作为实际异步执行证据。

数据库批次与 serving 动态批次是两个层次。前者受 stage 准入、选择行与 Arrow
所有权约束；后者由独立模型服务按模型版本、输入形状、deadline 和资源容量组批。
队列延迟、batch size 与并发必须共同测量吞吐和尾延迟；不预设最大 batch 为最优。
可参考 [Triton batching](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html)
的实测调参流程。不同 token/媒体长度须确定 padding/ragged/分桶政策及真实分配量，
不可通过缩减输入或放宽质量阈值使批量结果看似更快。

取消是分层协议：数据库先撤销发布权威，再取消 Flight，随后等待 transport 和
实际服务资源退出，才能释放相应准入。服务若不能证明 backend 已停止，数据库
仍须拒绝迟到结果，相关 serving 资源继续由服务持有至实际完成。
[Triton cancellation](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/request_cancellation.html)
也说明 backend 停止依赖具体实现，并存在检测边界；不能把 client cancelled
等同于 GPU、权重或请求 buffer 已经释放。这一机制对齐不意味着本项目已经接入 Triton。

单 parser 应通过 [Calcite parser extension](https://calcite.apache.org/docs/adapter#extending-the-parser)
及固定版 FreeMarker/JavaCC 生成流程覆盖自定义 DDL、AI、VECTOR 和 Feature 语法，
生成同一 SqlNode 权威；当前 ANTLR AST 再转 Calcite 的路线仍未满足这个约束。
迁移必须同时替换 StatementDispatcher 与查询编译入口，并保留实际语法和错误位置
回归，不保留“解析失败再换一个 parser”的运行路径。

常量优化也应使用同一 Calcite 规则流程。固定 1.42.0 的
[ValuesReduceRule](https://github.com/apache/calcite/blob/calcite-1.42.0/core/src/main/java/org/apache/calcite/rel/rules/ValuesReduceRule.java)
可按真实 Values tuple 做 projection/filter reduction，未能证明为 literal 的表达式
保留执行。已删除私有 `ConstantFoldingRewriter` 的完整关系 Interpreter 执行及
单 tuple 补救，改为现有 HEP registry 的官方 Values reduction / empty-project
规则；同时删除全仓无生产调用的 `ConstantPlanEvaluator` 和公开 wrapper。
折叠指标只计最终 LogicalValues，不把未执行查询计成 Native 运行收益。真实
多行、NULL、CASE 的 Native smoke 回归尚待本次重新打包验证。仍存在的 parser、
optimizer safe-program 和 ANN 降级结构是分别审计的独立问题；下节已删除首项的
实际重试路径，尚未完成的其余结构仍阻止整个内核“零 fallback”证明。

## 各项能力的官方依据、当前断点与验收

下表“方案”是本项目选择；“当前证据”只代表已检查的实现，不代表完成。

| 项目 | 官方标准/优秀方案 | 当前证据与根因 | 修正和完整验收 |
|---|---|---|---|
| 控制面与分布式 Native | [Snowflake 分层](https://docs.snowflake.com/en/user-guide/intro-key-concepts)、[Spark jobs/stages/tasks](https://spark.apache.org/docs/latest/cluster-overview.html)、[Gluten Substrait/Velox](https://gluten.apache.org/archives/v1.3.0/developers/how-to-use/) | 已有 StageDAG、attempt 和 durable journal；仍需逐条排除 Coordinator 源扫描/索引数据操作、parser 与 optimizer fallback | 单 Calcite parser、单 Substrait、单 Velox 执行；所有数据操作落 Worker；多进程重试/取消/失联 oracle |
| 列式协议与所有权 | [Arrow C Data](https://arrow.apache.org/docs/format/CDataInterface.html)、[Arrow Columnar](https://arrow.apache.org/docs/format/Columnar.html) | Velox Vector 内部执行合理；同进程 buffer 兼容才共享，布局转换与跨进程网络不能承诺绝对零复制 | 同进程 C Data、跨进程 IPC/Flight；嵌套/空 schema/offset/释放 oracle；逐边界 copy bytes/CPU/owner 计量 |
| 函数与类型权威 | [Substrait functions](https://substrait.io/spec/specification/)、[Velox expression evaluation](https://facebookincubator.github.io/velox/develop/expression-evaluation.html) | Native owned registry 已可发布，但 Planner 静态默认目录和未贯通 field18 仍在；VECTOR 曾退化为无维度 ARRAY，UNKNOWN 被猜成 VARCHAR | 同质完整函数 cohort 编译前冻结，最终 IR 要求在任务 writer/reader 校验；durable replay 固定身份；真实 SQL→Native→Flight 类型 oracle |
| Lance/湖仓存储 | [Lance v0.20 manifest](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/format/manifest.rs)、[Paimon 1.4.1 Lance](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/jni/LanceWriter.java)、[Iceberg 1.11 FileFormat](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/api/src/main/java/org/apache/iceberg/FileFormat.java) | Dataset 与 Paimon snapshot 是不同元数据权威；Iceberg 无 Lance 枚举；空快照查询曾在首次发布后失效；分页重复排序；读侧 copy getter 无 consumer | 保留 Dataset connector、Paimon 复用自身 connector；修复绑定隔离和分页；版本固定跨 SDK 互读，scan/MOR copy 到 canonical report |
| 大规模写入与原子可见性 | [Iceberg atomic commit](https://iceberg.apache.org/spec/)、[Paimon snapshot commit](https://paimon.apache.org/docs/1.4/) | Dataset/Paimon 最终可见性还需 execution-owner fence；physical writer 支持范围不足；UNKNOWN 与 abort cleanup 需证据 | 表 incarnation/base snapshot/intent/winner 在最终权威事务中比较；UNKNOWN 留对象查明；分区/bucket/PK/overwrite/delete/spill/多进程故障验收 |
| 索引与混合检索 | [Cortex Search](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview)、[scoring](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-customize-scoring) | ANN、BM25、fusion 与 generation 机制存在；后台构建/增量及模型 rerank 的全部调度路径仍需核查 | 固定 snapshot/model/index generation，独立 serving/refresh 池；Native 关系实现混合检索/模型重排；Recall/NDCG 与故障正确性和尾延迟一起比较 |
| 自动 Embedding | [BigQuery AI embedding](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-ai-generate-embedding)、[Cortex Search refresh](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/cortex-search-overview) | 已有 generated value 的 durable artifact 和模型 binding；目前受同步 JNI 模型路径限制 | 复用 durable job/同一计划和推理服务；固定模型维度/purpose，CDC watermark、fenced publish、撤销/重试不产生混合 generation |
| 多模态 | [Databricks AI query](https://docs.databricks.com/aws/en/large-language-models/ai-query)、[Arrow nested/binary types](https://arrow.apache.org/docs/format/Columnar.html) | 已有媒体引用、授权与有界 HTTP 实体；迁移时需保持这些机制，不能只存 URL 便称具备推理 | binary/list/struct 与受治理不可变 blob；真实输入 decode/对象身份/取消/服务响应类型与输出存储全链 |
| 缓存 | [Snowflake persisted results](https://docs.snowflake.com/en/user-guide/querying-persisted-results)、[AI response caching](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-caching) | 已取消 text binder 无条件跨任务 0.98 相似度 cache，Native 入场按实际 capability 的非确定性拒绝 reuse；原近似 cache 实现仍需迁移/清理，不能视作精确 cache 已完成 | 仅显式允许的精确可复用结果；完整输入/options/schema/auth/snapshot/model身份；随机模型输出不能隐式缓存，近似搜索必须显式建模；真实重复调用/差异提示词/权限撤销/模型更新 oracle |
| Feature Store | [Feast point-in-time joins](https://docs.feast.dev/getting-started/concepts/point-in-time-joins) | 元数据和部分 SQL 物化已存在，不证明多源 PIT/CDC 全链 | 每 entity event time 选过去有效特征，TTL 相对该 event time；显式定义 availability time 防晚到修订泄漏；watermark 固化、版本物化、训练/在线同版本 SQL/Flight oracle |
| 评估与发布门禁 | [MLflow metrics](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.metrics.html)、[evaluation lineage](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.html) | EvaluationStore/release gate 是元数据证据，尚不能证明 Worker 分布式指标计算或模型调用 | 固定 dataset/model/index/metric/方向/单位，Velox 聚合结果与终态原子提交；分类/回归/检索独立 oracle，无样本/NULL/重复候选规则明示 |
| 弹性与性能证明 | [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html)、[shuffle decommission](https://spark.apache.org/docs/latest/job-scheduling)、[NIST quantiles](https://itl.nist.gov/div898/handbook/prc/section2/prc262.htm) | JMH 曾缺 baseline/benchmark/零值成功退出；TPC 少样本最大值标 P99；CLI 曾忽略 --file 后成功退出 | 当前修复使无测量必败；显式 R7/样本资格、全 SQL digest+真实query IDs；4→8 Worker、公平硬件/数据/正确性条件、三窗口、吞吐/P99/内存/召回全部实测 |

Feast 的 event-time PIT 默认并不自动阻止后补修订：created timestamp 默认用于
同 event time 去重，严格历史可见性另需限制 availability time；本项目必须明确
两种语义，并固定源 snapshot，不能用“最后一条记录”模糊实现。
当前 Feast 已公开 `filter_by_created_timestamp` 对应控制；实际 Pangrova 源码
调用链和多源 PIT / CDC / Training Dataset 的完整实施规格见
[feature-current-audit.md](feature-current-audit.md)。

## 全链证据与当前结论

每项仅在“来源→设计→实现→注册/注入→调用者→真实正常/故障 oracle→对应构建”
全部连通后记为完成。仅 SPI 声明、元数据表、存在一个测试名或历史 PASS 不构成闭环。
详细源码修复与运行证据分别记入三个专项审计和本次验证记录。

评估存储与发布门禁的真实调用链另见
[evaluation-current-audit.md](evaluation-current-audit.md)。已修复非法完成和发布时间线，
真实 PostgreSQL 的 10 项测试通过；该证据不代表 Native 指标执行器已实现。

非确定性 AI cache 修正不改变模型调用或模型版本授权。当前 model capability 明确
`deterministic=false`，`AI_COMPLETE` 默认未声明 sampling 参数，故重复 SQL 必须真实调用。
真实提供者 E2E 的 oracle 改为两次非空结果与两次实际请求；不再要求随机结果相等，
也不把近似 prompt 的 cache hit 计为普通 SQL 的性能改善。该 E2E 正在等待本次新 bundle 验证。

本地 Docker 为 2 CPU/约 4 GiB，不具备已经确定的 Linux 4→8 Worker 大规模负载环境。
性能比较器测试验证计算和拒绝路径，不是吞吐/尾延迟校准数据。SF100/SF1000、
在线千样本窗口、混合写入/检索/特征、ANN Recall@10 门槛均需真实集群执行。
“业界最优”必须限定硬件、数据、质量、成本和 workload，使用可复现对标证明；
当前不宣称实现完成、全回归通过、达到性能门槛或获得行业全局最优。


## 单一 Calcite HEP 与失败传播（隔离工作树）

原生产链并非一个优化程序：`HepOptimizationPass` 在任意异常或负字段引用后
重跑不含 expression reduction 的 safe program；safe 再失败则返回原未优化计划。
`SqlCompiler` 在转换错误后还能再次 `planSafe(sql)`。这会将内部错误伪装成
成功编译，也使同一个 SQL 经由多套规则。原注释称已知 Calcite bug，但没有固定
revision 的可重现 upstream issue 证据可代替本项目根因分析。

本次隔离修复删除 `registerAllSafe`、`buildHepProgramSafe`、`hepProgramSafe`、
`optimizeSafe`、`planSafe` 和 compiler safe retry 的实际声明与全部调用者。
唯一 HEP 保留官方 reduction、Values、volatile guard 及各优化规则，每次新建
`HepPlanner`；运行错误/负字段引用产生 `PLANNER_OPTIMIZATION_FAILURE` 并停止
任务下发。AQE reoptimization 也不再吞失败返回旧 plan。原 PlanTable 回归仅改为
正常 `plan(sql)`，其所有有效断言保留。

这是固定官方规则的单一路径约束；不是停用 expression reduction 或新增私有
常量执行器。[Calcite 官方 HEP](https://calcite.apache.org/javadocAggregate/org/apache/calcite/plan/hep/HepPlanner.html)、
[官方常量规则及可空性参数](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/rules/ReduceExpressionsRule.html)
支持上述接口事实，不直接保证本项目全部 SQL 正确。

新增真实 CASE/SUBSTRING/LIKE/aggregate SQL 编译回归验证有效字段、完整 root
names 及重复编译 bytes；仍用现唯一 SQL frontend/converter。最近 SQL 64 tests 已实际 PASS；首次静态检查因一处空行失败，格式已修正、同轮静态重验待完成。
最近检查 `/tmp/pangrova-isolated-sole-hep-nearest-static-20261007.log` 正在执行，
不提前写 PASS。此前 owning Planner 1,335 项有 5 个 errors：3 ROUND capability
静态默认名册漏名、1 Nested protobuf 被实现类名校验拒绝、1 FROM VALUES parser
拒绝。全部保持实际失败，需实际 function inventory 与 parser 单元消除，不能
通过补另一个函数名称名单或取消校验获得通过。

本节源码基线保存在 `/tmp/pangrova-sole-hep-unit-baseline-20261007/baseline.json`。
本任务隔离结果不包含共享目录另一负责人随后修改的 Native/metadata/SDK 源码；
合并前逐文件核对输入与 delta，构建 artifact 绝不交叉使用。


## OpenLineage 发布链核对（未闭环）

官方 [RunEvent API](https://openlineage.io/apidocs/openapi/) 要求运行有 START 与
终态；[对象模型](https://openlineage.io/docs/next/spec/object-model/) 允许输入输出
数据集在后续事件补充。当前 `QueryDispatcher.execute` 在 pipeline 返回后发
START，`TaskQueryCompletion` 发 COMPLETE，`QueryAuditDispatcher.emitLineageOnFinished`
再发 START+COMPLETE；失败/取消逻辑未形成对应的同一发布链。
`QueryManagerContext.eventDispatcher` 未用于实际 QueryManager audit 构造，
`LineageQueryEventListener` 也没有生产 bootstrap 注册调用；不能仅依据其单测
认定 OpenLineage SPI 已接线。Governance full 115 tests 中旧 zero-event 断言失败，
Metadata 被阻断；保留失败和直接调用点，后续应由一次逻辑 query admission 与
一次 terminal claim 发布，task物理完成不另发query终态。真实 backend 验收尚未执行。
