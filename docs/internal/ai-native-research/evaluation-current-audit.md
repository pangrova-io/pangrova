# 评估与发布门禁：当前源码核查

核查日期：2026-10-07。范围是当前工作树的评估元数据、发布证据和调用链，不代表模型质量或 Native 性能实测。

## 已实现的边界

`EvaluationStore` 有实际 PostgreSQL 实现。`AiInfrastructureStores.jdbc(dataSource, authority)` 构造
`JdbcEvaluationStore`，Coordinator bootstrap 构造这组 stores。当前生产
Coordinator、Planner、Worker 和 Native 源码中没有调用 `.evaluations()` 的评估任务入口，
也没有连接此存储的指标计算执行器；本次找到的直接消费者是 metadata 集成测试。
这证明存储对象已构造，不能据此声称评估任务已在生产链路执行。

| 环节 | 当前实现与证据 | 尚缺的执行语义 |
| --- | --- | --- |
| 数据集 lineage | `EvaluationDatasetVersion` 保存 tenant、版本、owner、source snapshot、manifest URI/SHA256、item count、schema；V26 外键绑定 run 到版本 | `EvaluationValidation` 只核验摘要格式和非空文本，没有读取 manifest、验证实际内容摘要、解析 schema 或核对有效样本数 |
| Run 生命周期 | `JdbcEvaluationStore.createRun()` 验证同 subject identity、同 dataset version 的 SUCCEEDED baseline；`completeRun()` 用 RUNNING 条件更新，并在同事务写指标 | 没有评估任务调度、输入/标签绑定、执行 attempt/owner fencing；条件更新只能保证一次终态发布 |
| Metric evidence | `EvaluationMetric` 拒绝非有限值，V26 拒绝 SQL NULL、NaN 和正负 Infinity；指标名在 run 内唯一 | value、unit、passed、evidence reference 都由调用者提供；没有公式版本、计算方向、有效/NULL 样本政策、Native 聚合执行与结果 provenance |
| 发布门禁 | 终态、policy version 一致；批准要求 SUCCEEDED、有指标且全部 passed；绑定 baseline 时要求 baseline 成功、指标名覆盖、unit 相同、baseline 指标 passed | `JdbcEvaluationReleaseGate` 查询的是 name/unit/passed，没有读取 metric_value，因此没有根据数值重新计算阈值、相对退化或指标方向 |
| 审计发布 | 一个 run 一个不可变 release decision，保留 reason 和 decidedBy | decidedBy 是证据字段，当前接口不证明调用者身份或权限；没有实际模型/索引部署调用此决定的全链验证 |

`pangrova-indexing` 中的 `IndexEvaluationService` 判断文件索引能否排除扫描
（SKIP/KEEP/UNKNOWN）。它不是模型、检索质量或预测误差的评估器，不能充当上述缺失实现。

## 官方方案对照

MLflow 的 `EvaluationMetric` 明确关联计算函数、指标名、方向、可选版本和计算说明，
计算函数读取 predictions/targets，并可返回逐行结果和聚合结果。Pangrova 当前只有聚合证据记录，
缺少这些计算输入和公式身份。这里只对照契约语义，不将 Python evaluator 放入 Native ABI。
[MLflow EvaluationMetric / MetricValue 官方 API](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.metrics.html)

MLflow dataset tracking 记录 name、digest、source、schema/profile 和输入 context；
其 metadata-only 数据集明确不计算实际数据哈希。Pangrova 保存 manifest 摘要仍需在执行端验证内容，
不能把“摘要字段已持久化”当作数据读取或完整性验证已经完成。
[MLflow Dataset Tracking 官方文档](https://mlflow.org/docs/latest/ml/dataset/)

MLflow validation 直接比较 candidate/baseline 数值，要求明确 greater_is_better，
支持绝对阈值、绝对改善和相对改善，并拒绝缺失的必要指标。官方当前源码以 baseline 绝对值
处理负值，相对比较的零 baseline 使用带警告的特殊分支；Pangrova 后续须明确零值和 NULL 政策，
不能无说明继承特殊分支，也不能从 passed 布尔值倒推阈值通过。
[MLflow MetricThreshold 与 validation 官方源码](https://mlflow.org/docs/latest/api_reference/_modules/mlflow/models/evaluation/validation.html)

这些对照是本次架构推论。要补齐执行链，需先固定指标版本/方向/单位/样本语义和不可变 policy，
再实现实际输入读取、Native 数值计算、证据发布及部署消费者；每个连接点均需真实数据集与执行测试。
部分 MLflow 旧 GenAI factory 已标注弃用，以上引用不意味着选择其旧 API 作为新实现基础。

## 本次完成的原子修复

此前合法状态字段允许 `completedAt < startedAt`，也允许发布决定先于评估完成，形成不可信时间线。
本次保持公共字段与存储 schema 不变，增加以下约束：

- `EvaluationRun` 构造时拒绝完成早于开始的所有终态；相等时间允许。
- JDBC 条件更新通过 `RETURNING started_at` 读取实际授权行的开始时间，在同事务内检查；
  时间非法时回滚状态更新，指标不发布。没有在更新前新增存在性检查造成竞争窗口。
- 批准和拒绝的 release decision 均不得早于 run 完成时间；非法决定不写入。

这是存储 API 的一致性修复。V26 没有新增数据库 chronology CHECK，数据库直写仍不能从本次
Java 检查推导为受保护；已有非法历史记录也没有被自动改写。

## 验证结果与限度

原 `JdbcEvaluationStoreTest` 6 项测试完整保留：baseline/dataset 绑定与持久化、失败指标阻止批准、
baseline 指标单位、缺失 baseline 指标、重复指标事务回滚、dataset/decision 不可变。
新增 2 项真实 PostgreSQL 回归验证全部终态的非法完成回滚、合法边界，以及批准/拒绝时间边界；
另有 `EvaluationRunTest` 2 项验证精确 Instant 的前后边界。

使用 JDK 17 `javac --release 17` 新编译当前 evaluation 主源码及上述测试，运行实际 JUnit Launcher，
沿用已有依赖 classpath、模块 Testcontainers 环境和 `SharedPostgresContainer` 的 PostgreSQL 16
及完整 Flyway migration 链。结果 **10 tests successful、0 failed、0 skipped**。
首次直跑因 Ryuk 启动失败未成功；采用 owning POM 已配置的 Docker API / Ryuk 环境后通过。
未启动 Maven/Native 构建，未改写旧报告或伪造执行证据。

测试中的预设指标值用于验证持久化和门禁行为，没有模型推理或指标计算，不能作为 Native evaluator、
模型质量、Recall/NDCG 或性能结果。 owning module 全量与 CI 静态检查仍由主任务统一验证。

## 可核查源码

- `pangrova-metadata/src/main/java/org/pangrova/metadata/evaluation/`
- `pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/assembly/AiInfrastructureStores.java`
- `pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/CoordinatorBootstrap.java`
- `pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/eval/IndexEvaluationService.java`
- `pangrova-schema-migrations/src/main/resources/db/migration/V26__semantic_view_and_evaluation_metadata.sql`
- `pangrova-metadata/src/test/java/org/pangrova/metadata/evaluation/`


## 2026-10-08 — quality execution is also incomplete

Both Coordinator module assembly paths instantiate `DefaultQualityRuleExecutor`. Its production `execute` checks only that the expression is nonblank and returns a passing zero-row result. It does not inspect the target table. The existing `SqlQualityRuleExecutor` has no actual Coordinator `GovernanceQueryRunner` wiring. These are execution gaps, not proof that table quality checks work.

That SQL executor's count and sample queries currently use `NOT (expression)`. Its own contract says only TRUE passes; SQL UNKNOWN therefore must fail too. `NOT UNKNOWN` remains UNKNOWN, so the current count/sample excludes NULL predicate outcomes. Correct lowering needs an explicit `IS NOT TRUE` predicate and a declared NULL policy. Existing predetermined runner-lambda tests cannot validate a real distributed execution chain.

Closing this requires authenticated rule/version inputs, the sole Calcite → Substrait → Native execution path, a pinned source snapshot for count and diagnostic sample, a bounded native diagnostic artifact, and final metric metadata tied to the actual query attempt and publication fence. A PostgreSQL JDBC query runner must not become a second production table executor. A scheduled callback or stored boolean `passed` is not evidence of native evaluation. No quality execution implementation or NULL behavior correction is claimed completed by this audit.

### 2026-10-08: TRUE-only quality predicate correction

The current executor documents that a row passes only when the rule predicate is TRUE. SQL `NOT(predicate)` fails to select UNKNOWN/NULL rows. Both the aggregate and bounded diagnostic query now use `(predicate) IS NOT TRUE`, so they apply the same explicit policy. This follows the existing Pangrova contract; it is not a claim that all quality frameworks must reject missing values. The SQL truth behavior is specified in [PostgreSQL 16 comparison predicates](https://www.postgresql.org/docs/16/functions-comparison.html). Frameworks such as [Great Expectations missingness](https://docs.greatexpectations.io/docs/reference/learn/data_quality_use_cases/missingness/) expose missing-data policies separately; an explicit policy contract is preferable to implicitly dropping NULL rows.

The seven former precomputed-runner tests are replaced with real PostgreSQL 16 SQL executions. They retain successful/violating rules, blank rejection, genuine query failure, aggregate/sample construction, and no sampling on success. The former manufactured connection failure is now an actual dropped-relation failure with exact PostgreSQL SQLSTATE 42P01 and exact propagated server message. Two further cases cover twenty UNKNOWN rows with the existing ten-row sample cap and an empty real table. All nine passed with no skips; the twelve-module nearest verification also passed Checkstyle and SpotBugs. The initial missing test-scoped PostgreSQL container dependency compile failure is retained, and the dependency is now declared explicitly.

This independent SQL oracle proves predicate construction and result handling. Production still instantiates DefaultQualityRuleExecutor, and no real GovernanceQueryRunner currently connects quality execution to the sole Calcite/Substrait/Velox chain. Count and sample snapshots, authenticated/fenced result publication, bounded native diagnostics, and replacing the production placeholder remain unfinished.

## 2026-10-18 — numerical evaluation release policy

The release gate previously checked `passed` and metric names/units but did not read
`metric_value`. It could approve a candidate with a worse numeric result when the
caller marked `passed=true`. A new tenant-scoped `EvaluationMetricPolicy` now pins an
explicit policy version to a list of metric names, units, directions, absolute
thresholds, and maximum **absolute** baseline regressions. The direction is
mandatory (`HIGHER_IS_BETTER` or `LOWER_IS_BETTER`); no metric-name heuristic is
used. A candidate must provide exactly the policy's named metrics, each with the
declared unit, and meet both the absolute threshold and its pinned baseline
comparison. The existing `passed=false` rejection remains an additional gate.
Approval checks the persisted PostgreSQL values in the release-decision transaction.

Policy publication inserts all rules and seals the version in one transaction.
PostgreSQL guards prevent later policy/rule UPDATE, DELETE, TRUNCATE or additions to
a sealed policy. A new run cannot start without a sealed policy version; a database
trigger records that the sealed policy existed at run insertion and prevents that
pin, the run's policy version, baseline, subject, dataset, and other identity
fields from being changed. It also permits only one terminal transition.
The V63 database fence stamps that transition with its PostgreSQL transaction
ID and permits metric INSERTs only in that same transaction. Later INSERTs and
all metric UPDATE, DELETE, and TRUNCATE operations are rejected. Evaluation
dataset versions and release decisions likewise reject UPDATE, DELETE, and
TRUNCATE, so an approved decision cannot silently point to edited evidence.
Evaluation runs also reject DELETE and TRUNCATE, including unpublished runs with
no dependent metric or decision rows.
The database also rejects a terminal `completed_at` before `started_at` and
checks that a directly inserted release decision matches a terminal run and is
dated at or after its completion. These checks mirror the Java API's time order
and prevent direct SQL from persisting an inverted audit timeline.
Pre-V62 runs have no such
proof and cannot be approved.
API and database
checks reject non-finite threshold and regression values, negative regression
allowances, empty policies, and duplicate names; the existing metric value checks
reject NaN and infinities. Existing runs that predate V62 remain in history. A rejected decision can
still be recorded for audit.

This contract follows the explicit metric direction and absolute threshold semantics
documented by [MLflow `MetricThreshold`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.models.html)
and its distinct metric identity/version concept in
[MLflow `EvaluationMetric`](https://mlflow.org/docs/latest/api_reference/python_api/mlflow.metrics.html).
Pangrova's `maximumBaselineRegression` is its own bounded non-regression allowance,
not MLflow's minimum-improvement setting. We deliberately do not implement a
relative percentage here: zero or negative baselines need a separately specified
policy before that can be safe.

The nearest Testcontainers/PostgreSQL 16 test inserts actual label/prediction rows,
computes 9/10 and 8/10 accuracy with SQL, durably publishes both results, and proves
that `passed=true` cannot approve the degraded candidate. Other cases exercise
lower-is-better latency, absolute threshold, allowed regression, missing policy,
and sealed-policy mutation. This is a real persistence and numerical-release-gate
slice; the SQL aggregation in that test is an independent oracle, **not** a
production Pangrova evaluator. There is still no production evaluation task caller
or Native Worker computation of metrics from version-pinned datasets, no manifest
digest verification at execution, and no deployment/index release consumer of the
recorded decision. Those remain separate production gaps.

### Published Feature View materialization-volume evidence

`EvaluationStore.completeFeatureViewMaterializationRun` reads a `FEATURE_VIEW`
run's artifact ID and generation, joins the tenant-scoped catalog export request
to its immutable Feature View definition, published artifact job, and tabular
generation, then verifies the run's dataset pins that publication's source
snapshot, manifest URI/SHA-256, and row count. It atomically records the
`materialized_rows` metric in `rows`. The numerical release gate rechecks the
publication and rejects a caller-supplied or subsequently altered count. Counts
above 2^53 are rejected because the existing evaluation metric column is a
double and could otherwise silently round an integer. A retired generation
remains addressable as historical evidence after a newer publication.

This measures exported row volume. It does **not** measure feature correctness,
freshness, leakage, model accuracy, or candidate quality. The policy's explicit
`HIGHER_IS_BETTER` minimum row threshold is an operational volume gate, not a
general recommendation that more rows are better. The real Native Feature View
E2E covers an approved two-row export, a rejected one-row export after policy
changes, and rejection of a forged two-row metric for that one-row publication.
The call is a production metadata API invoked by that end-to-end flow; automated
evaluation scheduling, multi-source point-in-time joining, and quality metric
computation in Native workers remain open gaps.
