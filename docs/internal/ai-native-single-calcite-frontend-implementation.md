# 唯一 Calcite SQL 前端：调用链根因与原子迁移规格

基线：2026-10-02 当前工作区，Calcite 1.42.0。状态：调查/设计，不是已实施声明。

## 1. 已实测失败及真实入口

`AiEmbeddingPurposeSqlProtocolTest` 的合法派生表 `FROM (VALUES ('a'), ('b'))`
在用途校验前被拒绝。最近同范围命令：

```bash
mvn -pl pangrova-planner -am \
  -Dtest=AiEmbeddingPurposeSqlProtocolTest,AiInvocationExpressionConverterTest,SqlCompilerTest,FunctionBindingDeclarationsTest,FunctionTypeFormulaEvaluatorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

日志 `/tmp/pangrova-production-embedding-purpose-function-catalog-planner-nearest-r1.log`：
Planner 22 项、19 PASS/3 ERROR/0 SKIP，19.260s；protocol 两组共8项PASS（formula5/binding3）。
原六个 negative 仅要求 RuntimeException，也使用同一 VALUES 输入，因此存在提前
parser failure 假绿。现已保留全部 SQL，并新增同形合法 RAW preflight、精确
PlannerException/PLANNER_VALIDATION_FAILURE/AI_EMBED purpose 原因断言。
2026-10-04 未筛选 Planner regression 实际1302项中1256 PASS、28 FAIL、18 ERROR：
用途测试9项仍在同形合法 VALUES preflight 报 parser error，另一个 DISTINCT
snapshot用例同因失败。日志
`/tmp/pangrova-production-jackson-bom-planner-unfiltered-r1.log`；它证明原负例
不能因“任意 RuntimeException”计通过。operator table 的2..3参数声明最近23项
通过，不构成 VALUES 或用途完整SQL链路通过。
不能把合法输入改为常量或另一种 SQL 以跳过这个生产入口缺陷。

真实源码不是一概“ANTLR→SQL string→Calcite parser”串行解析：

- `PangrovaParserFactory.parseToSqlNode` 调用自定义 `SqlParser.parse`，再由
  `AstToSqlNodeConverter` 直接构造 SqlNode，送入 Calcite validator。
- 该 factory 虽实现 SqlParserImplFactory，`getParser(Reader)` 永远抛异常；真实
  主查询入口并未使用 Calcite parser。这与 root architecture 的 Calcite dialect
  extensions 和目标“Calcite 唯一前端”不一致。
- `SqlCompiler` 还暴露 parseToAst/compileStatementToPlannedQuery，Coordinator
  依赖旧 AST 分派、写模式、重试、视图、时光读取、审计和表引用。
- `QueryPipelineExecutor.executeDmlStatement` 为 MERGE 与 retry 用 AstPrinter
  重建 SQL；源码明确写出 Calcite unparser 的 VALUES ROW 与旧 grammar 不兼容。
- `DynamicTableSqlRewriter` 会重复解析和 regex 替换表引用，失败时原样跳过；
  不能以保留此路径或新加 parser fallback 称为唯一前端。

只让 pure SqlCompiler 改用 Calcite parser，仍会在 Coordinator 的 AST consumer
失败或丢掉授权/写入语义，因此必须迁移完整公开入口。

## 2. 一手调研与裁决

Calcite 官方 grammar 将 VALUES 定义为 query，并允许带别名的派生 query；同一
前端可处理 SELECT/CTE/DML/EXPLAIN。这证明该测试输入不是应被排除的非法 SQL。
[Calcite SQL reference](https://calcite.apache.org/docs/reference.html)。

Calcite 提供 parser factory 的生成扩展机制，statement/types 等扩展从同一
JavaCC/FMPP grammar 生成，不需要先解析另一种 AST 再猜标准语义。
[Calcite adapters](https://calcite.apache.org/docs/adapter.html)。

Flink 的公开 SQL parser 使用 Calcite 模板与自身 FMPP parser configuration，
把自有 DDL 放入同一生成前端。此项目只借鉴“同模板扩展”的机制，不导入 Flink
运行时或其完整方言作为新的依赖/执行路径。
[Flink parser configuration](https://github.com/apache/flink/blob/master/flink-table/flink-sql-parser/src/main/codegen/config.fmpp)。

裁决：唯一受版本锁定的 Calcite 1.42 parser + Pangrova declarative dialect
extensions。不能用词首 regex 区分语法，不先尝试另一 parser，不保留 ANTLR fallback。
既有合法 DDL/SQL、错误位置、引用规则和写模式属于必须保留的验收范围。

## 3. 唯一对象与模块边界

`pangrova-parser` 拥有锁版本 parser templates、扩展 grammar 和 metadata command
descriptor。只有生成器写 generated sources。移除旧 ANTLR plugin/runtime、grammar
及其无生产 caller 的 visitors，不能保留两个可选 factory 或手改 generated Java。

`pangrova-planner` 拥有查询局部 opaque `ParsedSqlStatement`，内部保留一个准确
SqlNode；对 Coordinator 仅暴露明确 kind、读写属性、源引用、目标、typed metadata
command 和 source span。Coordinator 不接收 Calcite 类型，也不执行查询表达式。
旧中立 command DTO 可以作为同 SqlNode 的投影保留，但不能再次解析或手写推导
SELECT/表达式语义。

SqlNode 会被 validator 改写，不能把可变节点放进跨 tenant/global parse cache
复用。查询内解析、metadata scope、授权、view expansion、绑定、优化、序列化和
retry 由同一 compiler/session snapshot 拥有；不按 SQL string 重新获得最新 schema。
SQL 原文与 source spans 只用于诊断/审计，不是授权或 retry 的第二事实源。

## 4. 完整 consumer 迁移

1. `PangrovaParserFactory` 成为真实 factory；PangrovaPlanner 的 query parse 使用
   它产生的同一 SqlNode。所有 custom types/functions/operator references 消费唯一
   canonical declaration，不在 grammar 中复制函数签名。
2. `SqlCompiler.parseToAst/compileStatement*` 及各真实 caller 原子迁移至 opaque
   parsed input。DDL metadata commands 必须保留 tenant/catalog/branch target 和
   exact option/type 字段；不为方便使数据 DML 落回 Java row execution。
3. `StatementDispatchPhase`、StatementDispatcher、CompilationPhase 与
   QueryPipelineExecutor 共用 parsed input；删除吞 parseFailure 后继续的旧注释/分支。
   SHOW/metadata 可以分派 control plane，SELECT 和所有数据 DML 始终发 Native DAG。
4. `ViewExpander`、TimeTravelExtractor、DispatchContextEnricher、WritePolicyObservation、
   QueryLineageCapture、dynamic table 与 CTE analysis 使用节点级引用/改写。禁止替换
   SQL 字符串中的第一处同名文本；CTE shadowing、嵌套引用和带引号 identifiers 必须正确。
5. MERGE/INSERT OVERWRITE/static partition 的真实 typed 写模式保留；整个 MERGE
   一次 Native write/一次 publication。不能拿 ASTPrinter 重解析绕过当前语义限制。
6. snapshot/version/权限 policy 绑定前后都来自同 query authority。replay/failover
   不再按 raw SQL 重解析、重新分配 write allocation 或重新解析最新 model identity。
7. 删除 AstToSqlNodeConverter、原 SQL round-trip/reparse 和 parser-only cache 的
   死代码及旧 entrypoint；测试迁移断言同实际 generated parser，不删合法输入。

## 5. 语义与验收

- 公开 SQL→parser→validator→Rex→canonical protocol：VALUES、VALUES ROW、CTE、
  引号/大小写/嵌套 alias、参数、typed NULL、DECIMAL/ARRAY/ROW/MAP/VECTOR、AI 2/3参，
  purpose 常量/CAST/行相关拒绝；错误必须指向真正失败层，禁止“任意异常就是成功”。
- 既有全部 DDL 的语义 golden oracle：模型/embedding/backfill/index/FILE/storage/
  feature/semantic view/catalog/branch/tag/stream/maintenance/授权，以及合法原方言。
  Agent 外围和 LLM usage ledger 不因 parser 迁移重新纳入范围。
- UNNEST 必须保留 Calcite 实际 Cartesian 语义，不能把 Velox 多输入 zip-longest
  直接用作等价实现；ARRAY<ROW> flatten、MAP、整体 outer、ordinality 类型均需实际值 oracle。
- Public Coordinator HTTP→真实独立 Worker→matched JNI→Native→typed Arrow→JDBC
  验证派生 VALUES、NULL/重复标签/零列行数、authorized reuse、模型默认与显式用途、
  generated DOCUMENT 和 QUERY、DML publication/replay。组件直连仅为组件证据。
- parser/all nearest tests→full native/JNI pipeline→全仓 full-regression/coverage→
  完整 E2E/smoke；保留原 test 范围、0 skip、不提高 timeout、不加兼容/降级路径。

这份规格尚未实施，不宣称 parser 已唯一、合法 VALUES 已贯通或生产性能已通过。
