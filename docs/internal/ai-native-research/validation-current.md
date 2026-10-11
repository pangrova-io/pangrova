# 本次修复验证记录

日期：2026-10-07。基线：`058fb13a2f260125f2f9c7a2289550c3442a1e05` 加当前工作区。
此处分别记录实际执行、发现缺陷和被前置失败阻断的步骤。既有用户改动未重置。
临时运行日志没有加入版本库；下列结果不代表全任务完成或性能最优。

## 已执行结果

| 检查 | 实际结果 | 证据边界 |
|---|---|---|
| `.pangrova/setup.sh --check`、`git diff --check` | PASS | 规则布局和差异格式，不是运行正确性 |
| 固定 Lance Rust release 构建 / 全测试 | PASS；40 passed、0 failed、0 ignored | 真实固定 SDK；不是全 Java SQL publication |
| Lance EMPTY snapshot 最近真实 PostgreSQL/SDK 测试 | PASS；1 test | 原绑定与首次发布隔离；公共 SQL E2E 另验 |
| 客户端/JDBC 所属 reactor 完整 `full-regression,coverage test` | PASS；2,517 tests、0 failure/error/skip | 15 个模块，其中 CLI 43、JDBC 231；不代表 Native E2E |
| 客户端最近 `full-regression,coverage,static-analysis verify` | PASS；60 selected tests，Checkstyle/SpotBugs 0 | 实际 FP32/FP64 decoder、JDBC、CLI；没有声明全模块 diff coverage |
| 新 `--token-env` 最近 `verify` | PASS；21 selected tests，Checkstyle/SpotBugs 0 | 环境凭据模式的成功真实进程路径另由 TPC E2E 验证 |
| Benchmark/JMH 比较器与拒绝路径 | PASS；72 tests | 包含凭据与 deadline；不是性能测量 |
| Evaluation timeline 最近测试 | PASS；10 tests | 真实 PostgreSQL rollback/CAS；不代表 Native 评估计算 |
| 最新 DDL/CTAS/常量 Java 最近测试 | 78 PASS、8 FAIL/ERROR、0 SKIP，共 86 | Parser 17、Planner 38、Iceberg 3、Paimon 3；Coordinator 25 中 8 项生成值 fixture 失败；CTAS 4 项通过 |
| 最新 Native Release / 匹配 ASan 构建 | PASS | 已修复复制 oracle 的实际 Arrow API、布局与测试 buffer/pool 生命周期错误；不是全 ASan runtime PASS |
| 最新 Native Release 全 CTest | PASS；4,068 tests、0 fail | 真实读写复制、故障/取消 report、完整 VECTOR 精度及既有 Native 用例 |
| 最新匹配 ASan 复制生命周期最近测试 | PASS；1 test | 原新增测试 UAF 修正后通过；不替代全 CTest |
| 共享目录旧 ASan 全 CTest | 中止，exit 130；已报告 4,046 PASS，4 项等待，18 项未开始 | 真实 LLDB/反汇编证实旧 object 缺失当前 pause-owner teardown；日志保留，不能记为 ASan PASS |

## 不可记为通过的检查

- 共享目录旧 ASan 的 4 个 fixture-pause 用例等待不是当前源码执行结果。
  LLDB/反汇编发现编译对象的 TearDown 没有当前 `runner_pause_owned_` 判断和
  `Task::resume` 分支；mtime 不足以证明编译输入。已按统一构建负责人的协调
  请求向本任务 CTest PID 35779 发送 INT，exit 130，四个子进程全部退出。
  证据为 `/tmp/pangrova-production-20261007-reader-asan-lldb-layout-r2.log`；
  本任务保留原日志，不变更生产 cancellation 或扩大超时。
  共享目录负责人已将旧 object 移入可恢复临时目录，固定输入并 clean-first
  重编译；其新结果属于该负责人的输入闭包，不能替代本任务隔离目录验证。
- Planner 最近 Checkstyle 的 19 项发现已经最小格式修复，重新检查通过。
  Coordinator 当前工作区静态检查实际报告 528 项，Shuffle Service 另有 13 项。
  涉及当前工作区其他 lifecycle、journal 等改动；没有抑制规则或删除检查。
  它们阻止完整静态验收，不能用客户端静态 PASS 替代。
- 当前全量 ASan 未完成，因此重新 package、Java full install、full-regression
  verify、公共 SQL E2E 和 activation smoke 按规定暂未进入。
  独立 Java 和 benchmark 检查继续执行；不得使用旧包给本轮 Native 修复背书。
- 最近生成值 8 项失败来自既有 `GeneratedBuildTestWorkers` 清单前提：实际
  canonical inventory 已有 materializer，旧 fixture 又添加一次；缺失场景
  也没有真实移除该能力。没有删除/弱化断言或制造另一份清单使测试变绿。
- 最新 Iceberg Checkstyle 仍报告两处原有单行 `if`；已最小加括号，尚未重新
  检查。Parser/Planner 同轮静态通过，但不能替代后续新函数改动的检查。
- 目前本地 Docker 资源不满足已确定的 Linux 4→8 Worker、SF100/SF1000 性能环境。
  未提交真实集群吞吐/P99/scale-efficiency/ANN-quality 结果，未写入新性能 baseline。

## 完整执行链

遵守 `.pangrova/rules/build-pipeline.md`：固定 Rust FFI → Release 编译 → 全 CTest →
匹配 instrumentation 的 ASan 编译/CTest → 新 JNI 包 → JDK17 全 reactor install →
全回归 verify → SQL E2E → activation smoke。每个新失败先记录根因再修复，
保持真实正常/故障断言。完成后更新此记录的实际结果，不以计划代替 PASS。

## 共享工作区冻结与未验收输入

同工作区另一任务已请求接管统一构建。当前本任务停止启动任何共享
Maven/CMake/CTest/打包，生产源码、协议、JNI、Native 和客户端测试冻结。
现有 ASan 日志保留为实际未完成运行；只进行其只读诊断。

最近另有 5 个 Java 文件接入既有 callback declaration 的 deterministic/dynamic
语义，并为 HEP projection/filter replacement 加 volatile producer guard：
`FunctionBindingDeclarations`、`DeclaredCallbackSqlFunction`、
`PangrovaSqlOperatorTable`、`NormalizedAiEmbeddingSqlFunction`、`HepRuleRegistry`。
这些输入仅 `git diff --check` 通过，尚未编译或新增真实 SQL 回归，不属于
上表 Planner 38 tests 的已通过输入。实际 inventory、field18、Native NULL
调用和 nondeterministic dedup 接入仍未闭环。

## 隔离目录实施与真实验证

本任务继续工作使用受管理的独立 Pangrova checkout。
其输入包含初始共享工作区的全部 tracked binary diff 和 69 个 untracked 文件；
初始 diff SHA256 为 `949ecc6e978a74dcdb72a245a6696bfbeef41c2e36644858918bfe792e10ec27`。
构建目录、Maven 本地仓库及 Lance FFI prefix 全部独立。固定 Velox/Arrow
依赖只读复用；FFI 的 18 个构建输入与已验证来源逐文件一致，archive SHA256
为 `20b315ab4db2deb9ac1996797956e53c39ae7795693e423621d3d3a19320b39f`。
不把任何隔离 JAR/object/library 安装回共享目录或默认 Maven 仓库。
下列结果只对隔离源码有效，尚未将新增 delta 合入共享目录。

| 隔离检查 | 实际结果 | 边界与证据 |
| --- | --- | --- |
| `JdbcExecutionAnchorStoreTest` 最近真实 PostgreSQL 回归 | PASS；10 tests，0 failure/error/skip | 原 3 + 新 7；锁后 wall clock、owner/token/incarnation、batch exact locked set；`/tmp/pangrova-isolated-lease-clock-nearest-20261007.log` |
| Callback SQL 第一轮 | FAIL；Planner 84，1 failure、17 errors | 暴露既有 FROM VALUES 路由限制及新测试 IR 结构假设；原目的/错误断言保持；`/tmp/pangrova-isolated-function-semantics-nearest-20261007.log` |
| Callback SQL 第二轮 | FAIL；Planner 86，7 failures、0 errors | 暴露 SqlToRelConverter 早期重复 producer、条件 NULL transform；`/tmp/pangrova-isolated-function-semantics-corrected-nearest-20261007.log` |
| Callback SQL 第三轮 | FAIL；Planner 86，5 failures、0 errors | producer 根因修复后 AI alias 已通过；剩余 FILE DFS 名称、predicate scalar 分类及 dead CASE Project 精确 oracle 修正；`/tmp/pangrova-isolated-function-semantics-root-fixed-nearest-20261007.log` |
| Callback SQL 第四轮 | PASS；Protocol 10 + Planner 86，0 failure/error/skip | 生产及测试冻结；`/tmp/pangrova-isolated-function-semantics-final-oracles-nearest-20261007.log` |
| 执行及发布 final fence 第一轮 | FAIL；12 tests，2 failures、0 errors/skip | 锁后过期与全部 rollback 通过；两个正向全 record equality 暴露 publisher 的 manifest URI 不同于 canonical persisted view，正在修真实根因；`/tmp/pangrova-isolated-artifact-final-fence-nearest-20261007.log` |
| Native fresh Release 编译 | RUNNING | 独立目录、parallel 2；融合分数 15 个真实 C++ 用例待运行；发现 FindJNI cache 默认选 JDK21，需本次构建结束后显式 JDK17 重配置/重编译再资格验证；`/tmp/pangrova-isolated-native-release-build-20261007.log` |

锁后数据库时钟修复已经实施：逐行先取得真实 row lock，然后采样
`clock_timestamp()`，batch 修改只限此次锁定集合。最终 publication 还须在
所有 job/descriptor 写入与锁等待后、同一 connection COMMIT 前再次核验当前执行
权威；该实现及新真实回归已经进入上表，不把 SOURCE_READY 当作通过。

函数 SQL producer 修复保留 sole Calcite/Substrait/Velox 路径：callback 属性由既有
canonical declaration 驱动；返回可空性使用无条件 FORCE_NULLABLE；提前 RelBuilder
project merge 交给已加 volatile guard 的唯一 HEP。官方 constant rules 保持。
原无效 FROM VALUES fixture 以等价两行 UNION ALL seed 表达，目的/NULL/error
断言保留，标准 VALUES parser 的生产支持仍为未完成项。Native nondeterministic
CSE、实际 inventory/field18 接入、NULL policy、异步服务与全链路 E2E 均不能据
这些 Planner 最近用例宣布闭环。


隔离新增结果：final fence canonical URI 根因修复后重验 12/12 PASS，0 failure/
error/skip，所有原 full-record equality 与真实 generation 表 identity/digest/
counters/rollback 断言保留；证据
`/tmp/pangrova-isolated-artifact-final-fence-canonical-nearest-20261007.log`。
Planner owning full-regression,coverage,static-analysis 实际到 Planner 测试失败：
1,335 tests，0 failure、5 errors、0 skipped；祖先模块测试与静态通过，Planner
静态阶段未执行。具体 3 ROUND、1 NestedList、1 FROM VALUES 错误见 kernel 审计；
日志 `/tmp/pangrova-isolated-planner-owning-full-static-20261007.log`。
这不撤销 callback 最近 96 PASS，但阻止把整个 Planner 标为通过。
Native 初始 fresh build 完成 exit 0；之后已将 JNI cache 全部显式指定 JDK17，
加入真实 rand/abs 非确定去重 regression 的重编译当前仍进行。不得以 JDK21
header 构建或旧库给本轮 JDK17 cohort 背书。


隔离 Metadata owning `full-regression,coverage,static-analysis` 第一轮实际 exit 1：
Governance 115 tests，1 failure、0 errors、0 skipped；
`LineageQueryEventListenerTest.queryCreatedDoesNotEmitLineage` 断言零事件，但现有
生产 listener 已发送 START。Metadata 未执行，不能记为 Metadata owning PASS。
证据 `/tmp/pangrova-isolated-metadata-owning-full-static-20261007.log`。
已核对 [OpenLineage API](https://openlineage.io/apidocs/openapi/) 对每 run 的 START
及终态要求，并追踪生产 QueryDispatcher/TaskQueryCompletion/QueryAuditDispatcher
直接发布点与尚未接线的 SPI listener；必须同时解决重复发布、失败/取消、数据集
终态补充及真实 backend 回归，不能只把旧 zero-event 断言改小或绕过上游测试。

隔离单一 HEP 最近 SQL + 静态第一轮：64 tests 全部 PASS、0 failure/error/skip；
随后 Checkstyle 发现 SqlCompiler 的一处多余空行，exit 1。已仅修正空行，
相同最近 SQL + `coverage,static-analysis` 重验正在进行；证据
`/tmp/pangrova-isolated-sole-hep-nearest-static-20261007.log` 与
`/tmp/pangrova-isolated-sole-hep-nearest-static-corrected-20261007.log`。
上述 HEP 最近回归不能替代 owning Planner 已发现的 5 项错误。


隔离 JDK17 fresh Release 已实际完成 exit 0；融合 15 +非确定求值 3 的最近
CTest 18/18 PASS，完整 Release CTest 4,079/4,079 PASS、0 failed，耗时 480.27 秒。
构建/CTest 前后相同 Native/proto 输入哈希复验全部通过；证据
`/tmp/pangrova-isolated-native-release-jdk17-nondet-build-20261007.log`、
`/tmp/pangrova-isolated-fusion-nondet-nearest-20261007.log`、
`/tmp/pangrova-isolated-native-release-full-20261007.log`。
旧共享 ASan 中止仍不是 PASS；本次匹配 ASan、package、Java install/full E2E
尚未进入，已准备的 NULL wire policy 变更尚未应用，不属于这一通过 cohort。

单一 HEP 最近第二轮仍 64/64 PASS，Checkstyle 0 findings；SpotBugs 10 findings
拦住最终静态验收（日志 `...sole-hep-nearest-static-corrected...`）。已在独立
source unit 逐项修正 nullable component/statistics 访问和不可继承的 throttle owner，
两个集合访问器显式返回 JDK immutable snapshots；没有 suppress nullable/finalizer
finding。唯一 `LogicalType` getter 是不可变 generated protobuf，依官方消息合同与
实际 nested builder/repeated-field/bytes/digest 不变性回归，加入精确
class+method+field+return type 的分类，绝不覆盖 Builder/array/其他 finding。
真实 pinned SpotBugs Filter 边界程序已 PASS，保留全部原分类及负向断言，
并核验新分类在 isolated/full filter 各拒绝 12 个边界变化；证据
`/tmp/pangrova-isolated-planner-static-immutable-boundary-20261007.log`。
source unit 最近 43 tests 全 PASS；首次 Checkstyle 一处超长换行失败，已修正。
当前显式 compile/checkstyle/SpotBugs goals 重验中，只记静态结果，不记为测试运行；
证据 `...planner-static-source-nearest...`、`...planner-static-source-goals...`。
这仍不能替代完整 Planner 的 ROUND/NESTED/VALUES 5 项 owning 错误。


2026-10-07 20:50 隔离 Planner static source unit 最终最近验证 PASS：43 tests、
0 failure/error/skip；13 个 reactor 模块均 SUCCESS，Checkstyle 0 violations、
SpotBugs 0 BugInstance/0 Error。四个最后 finding 已用显式非空分支处理真实
TableScan.getTable() 与 VECTOR component invariant；未 suppress nullable finding。
日志 `/tmp/pangrova-isolated-planner-static-source-corrected-nearest-20261007.log`。
此前 owning Planner 1,335 tests 中的 5 errors 仍未关闭，不能据此宣布整个 Planner PASS。

2026-10-07 21:05 NULL source unit 30 files 原子应用，仅隔离树，逐文件 before/after
SHA 全部吻合。声明生成 Python 最近 17/17 PASS。Protocol 最近第一轮 FAIL：
34 tests、11 failures、12 errors、0 skipped；实际 Native catalogue fixture 仍来自
旧声明/descriptor cohort，严格校验拒绝 SHA drift。必须由本轮生产 Native exporter
重新导出实际 catalogue，禁止手改/重新封装旧 manifest 来绕过完整性校验。
日志 `/tmp/pangrova-isolated-null-python-nearest-20261007.log` 与
`/tmp/pangrova-isolated-null-protocol-nearest-20261007.log`。当前 Native Release
正在构建，未运行新 NULL CTest、ASan、package 或 JNI/E2E consumer，旧 4,079 PASS
只属于此前冻结 cohort。


2026-10-07 21:19 NULL wire 最近 Protocol 使用本轮生产 exporter 的实际 inventory
后，34/34 tests PASS、0 failure/error/skip；四个 reactor 模块 SUCCESS，Checkstyle
0 violations、SpotBugs 0 BugInstance/0 Error。实际导出 781,837 bytes，payload SHA256
`76425e48d75a7dad2fd715e17d04137186c901ef5a806bf5f1e28d8e680e8fdd`，原 bytes
保存在任务私有 before 文件；没有重新封装或捏造 executable callback availability。
证据 `/tmp/pangrova-isolated-null-protocol-fresh-catalogue-nearest-r2-20261007.log`、
`/tmp/pangrova-isolated-null-actual-native-catalogue-provenance-20261007.json`。
首个 fresh-catalogue 复验在 exporter 尚未结束时被误启动，已在其 Protocol 测试前
中止 exit130；该运行不计 PASS。

NULL source Native 初轮编译 exit0，但 protoc 引用不存在的 Native 私有 proto 根
有一条 warning，未达到零警告门禁。最近 46/46 CTest PASS、0 failed，13.79秒
只作为该初轮 cohort 的行为诊断。已删除 CMake 旧私有根/输入 glob，canonical
protocol 与 pinned Substrait import 仍为唯一生成输入；重新构建进行中，完整
CTest/ASan/package/JNI尚未进入。证据 `...null-native-release-build...`、
`...null-native-nearest...`、`...null-native-canonical-root-build...`。

Query lineage 24-file 独立 Java source unit 已原子应用并复验全部 before/after SHA。
包含真正 Marquez0.50.0+PostgreSQL HTTP ingestion 回归；没有 fake transport 或
CountingBackend 作为该单元的新验收依据。最近 source+real-backend+static 验证正在
运行，尚不能记为治理链路 PASS；已有 in-memory delivery 的 losslessness 与
unsupported lineage extraction/TTL/capacity 仍在 `docs/architecture/query-lineage.md`
明确保留为待闭合项。

2026-10-07 21:35 Query lineage 最近验证 exit1，治理生产编译发现 SPI QueryState 缺失 CANCELLED；测试尚未执行。已扩展取消合同，保留独立终态、进度 polling 终止判断与 Coordinator SPI 映射；新增实际 PostgreSQL fresh-connection 持久状态断言。初轮失败不能计为 Marquez 或治理 PASS。

2026-10-07 21:48 lineage 最近 r2 FAIL：SPI QueryProgress 2/2、实际 PostgreSQL cancellation fresh-connection 1/1 PASS；真实 Marquez 两类10 tests中7 errors，SDK HTTP getUri 的 ClassNotFoundException commons-lang3。实际 SDK published POM 与官方 1.12.0 build.gradle 均漏此依赖，已在BOM显式管理 Apache Commons Lang3 3.21.0（官方2026-09-25版本、Java8+）并由 Governance声明 runtime。没有跳过 HTTP 测试或用假 transport 替代。

2026-10-07 21:55 lineage r3 FAIL：SPI2与实际PG1 PASS；Marquez10 cases中2 failures，均首次HTTP调用5秒读超时（amd64镜像在arm64模拟，CPU受限）。服务 healthcheck 已通过但未证明 ingestion就绪。新增 fixture-only启动检查：真实SDK HTTP START/COMPLETE并读取Marquez PG精确两条durable事件；该启动阶段20秒timeout，不改变正式调用的5秒、关闭窗口或任一业务断言；不作为冷启动性能验收。等待复验，仍不能记治理PASS。
Native 本轮 zero-warning Release全4085 CTest PASS，379.26s，前后源码与binary/cache指纹完全一致；ASan fresh配置完成，匹配依赖独立构建进行中，尚无ASan/JNI/package/E2E PASS。

2026-10-07 22:06 lineage r4仍1 failure：cold column facet初始化导致关闭超时；r5 test-compile错误（新fixture误用不存在的二参DatasetRef构造器），已依现有三参合同纠正。r6实际13/13 tests PASS（SPI2、PG持久取消1、Marquez query6、HTTP4），0失败/error/skip；verify仍FAIL于4项Checkstyle（Javadoc2、常量1、switch whitespace1）。全部修实际源码后复验，不能把tests PASS当作整个verify PASS。outbox生产SPI/SQL/journal/单owner emitter/query/bootstrap源草案仅TMP，尚未apply/test；原内存模式仍未证明无丢失。

## 2026-10-07 22:48 隔离输入继续验收

- NULL 合同 Release 全量 4085/4085 PASS，fresh ASan 编译完成且日志无编译警告；ASan 最近 46/46 PASS。全量 ASan 仍进行中，已经出现 LSan 256-byte/1-object FAIL，因此不得推进其依赖的正式包/JNI/E2E门槛。日志 `/tmp/pangrova-isolated-null-native-asan-full-20261007.log`。本轮没有新增 suppression、关闭 leak 检测或改变异常语义。另一个任务的独立诊断 `/tmp/pangrova-runtime-error-asan.riwhp8/root-cause.md` 指向 dyld TLS probe，但不是本全量运行的 PASS 证据。
- 血缘 r8 实际 13 测试通过，静态检查仍 FAIL（11 项）；修复 final emitter / JDBC Statement、实际 PostgreSQL upsert 和失败传播后，r10 总计 16 项真实 PostgreSQL/Marquez 最近测试 PASS，Checkstyle 0、SpotBugs 0、全部 12 个 reactor SUCCESS。日志 `/tmp/pangrova-isolated-query-lineage-reference-nearest-r10-20261007.log`。r9 镜像摘要转录错误主动中止 exit130，不计 PASS。
- SpotBugs 7 项是明确共享身份的治理服务/JDBC pool：仅 exact class/method/params/return/field/type 分类，未放宽资源、集合、数组或并发检查。全局静态门槛仍未通过。
- 查询血缘 durable outbox/SQL journal 同事务、冻结逻辑事实、lease token/fresh DB clock、实际 SDK HTTP恢复/close测试仍 TMP草稿，未应用/未编译。V52保留给此单元，V53保留给治理质量存储单一 authority迁移（当前 bootstrap 仍使用 governance 旧表，metadata已有另一实现，不能声称已完成单一存储对齐）。
- 索引最近回归按实际 `IndexEvaluationAuthorityIntegrationTest` 执行，日志 `/tmp/pangrova-isolated-index-authority-nearest-r2-20261007.log`；先前误选测试名的运行已中止 exit130，不计资格证据。

## 2026-10-07 23:07 最近实际结果

ASAN 全量结束 exit8：4,078 PASS、7 FAIL / 4,085；2067.97秒。七项均 LSan
256-byte/one-object dyld/libc++ exception-copy TLS分配；本次没有 hang 或 SEGV。
原始日志 `/tmp/pangrova-isolated-null-native-asan-full-20261007.log` 保留完整栈。
使用原 canonical suppression，未新增、关闭 leak detector 或改变异常语义。
该门槛仍 FAIL，不能推进其依赖的 Native正式package/JNI/E2E资格。前后现有source、
Release binary/cache指纹完全相同；ASAN binary/cache只有此次after单独收据，不能
虚称已有ASAN before/after指纹。

血缘 durable outbox 25-file单元已经应用，V52/journal同事务/query lifetime copy/
namespace claim/fresh database clock/真实HTTP恢复测试均需验证。最近r1在test-common
5项style失败，r2在新emitter错误GuardedBy包名失败；只修实际源码并沿用common已有
annotation，r3进行中。r3存在旧Jacoco exec与新类字节不一致warning，尚不能作为
零warning/coverage资格，须新鲜exec复验。本轮未运行任何新增outbox owning tests
前便遇到编译失败，故不能计durable lineage PASS。

索引实际权限/取消测试10/10 PASS，Paimon原共享cache测试经正式1.4 SDK getTable/
fresh Catalog读取持久仓库事实修正后1/1 PASS；r5共11 tests，全部reactor SUCCESS，
0 failure/error/skip。这次未启static，不作为全局静态门槛通过。
`/tmp/pangrova-isolated-index-authority-nearest-r5-20261007.log`。
Resilience四文件final/compound lock/exact nonsecret retry jitter分类已按before SHA
应用，尚未owning tests/static验证；不能记该模块PASS。

### 2026-10-08 00:02 — 独立单元最新实际验证

- Quality 单一 Metadata authority 的 V53 迁移、真实权限失败恢复及原有 JDBC 持久化用例，连同实际 artifact exchange graph 校验：r8 `verify` 33/33 PASS、0 skip；13 个 reactor SUCCESS，Governance/Metadata Checkstyle 与 SpotBugs 均 0。r6 的两个 fixture 失败、r7 的行长失败保留在各原始日志；旧 producer 的真实 `PreparedStatement.setTimestamp` 写入用于时间回放，不将无时区 TIMESTAMP 宣称为跨 JVM 时区已修复。日志 `/tmp/pangrova-isolated-quality-authority-nearest-r8-20261007.log`。
- 分布式租约锁等待、lineage outbox/实际 HTTP 恢复的 r5 最近用例为 44/44 PASS、0 skip；当轮整体 `verify` 因 Metadata 16 项静态问题失败。后续修正实际 graph hash 比较和只读 map 视图，精确分类现有借用 DI owner，r8 Metadata 静态检查通过；这不代替 Coordinator/JNI 全链或完整 owning suite。
- Resilience 静态源修正最近 `verify`：68/68 PASS、0 skip，4 reactor SUCCESS，Checkstyle/SpotBugs 0；原有并发、重试、背压边界与属性用例保留。日志 `/tmp/pangrova-isolated-resilience-static-nearest-r1-20261007.log`。
- 原始完整 ASAN 仍是 4078 PASS + 7 FAIL / 4085，不能改写其结果。独立 compiler-rt 17.0.6 macOS arm64 TLS candidate 已实际构建；共享安装未改。修正真实 TSD base mask，并按本机已执行 public pthread key 范围提供 6144 字节 TLS 根扫描；实际反汇编确认。CMake 上游配置诊断保留，candidate 编译日志无 compiler warning，不声称配置无 warning。
- Candidate 独立、无 LSAN suppression 的 23 项对照符合预期：真实 main/worker 首末 public key 持有可达、自然 worker destructor/join 释放、清除 key 后实际 193 字节泄漏仍被报告、UAF/OOB 仍被报告、原始未改异常构造/复制/throw/rethrow/自然 worker 1/32/4096 次均 exit 0。r1 两个 worker fixture 因占满 key 后使用 `std::thread` 需要另一实际 libc++ TLS key而失败；r2改用 public pthread API，未进行预热、未减少 key 覆盖、未修改 sanitizer 检测选项。
- 原先失败的 7 个 Native selector 在同一个 candidate runtime 下 7/7 PASS，前后源/Release/ASAN binary/cache/runtime SHA 一致；使用仓库原有 suppression 文件，未增加 suppression。零测试 Native 控制的实际 dyld load 证明使用私有 candidate；这是加载/空基线控制，不是功能测试数量。日志 `/tmp/pangrova-isolated-asan-candidate-seven-20261007.log`，before/after `/tmp/pangrova-isolated-asan-candidate-seven-{before,after}-20261007.json`。
- Candidate 无过滤完整 4085 项 CTest 正在运行，尚未给出 PASS；该 cohort 完成前不修改 Native 源，不跳过测试、不执行下游 package/JNI/E2E gate。A/B/C 尚未共同构建，全部架构完成及 SF100/SF1000 业界最优仍无证据。

### 2026-10-08 00:39 — NULL cohort 全量 ASAN 与下一源码 cohort

- 私有 compiler-rt candidate 无过滤完整 CTest 已结束 exit0：4085/4085 PASS，1790.21 秒；日志 `/tmp/pangrova-isolated-asan-candidate-full-20261007.log`。本轮 source、Release/ASAN binary/cache、原 suppression 与私有 runtime 的真实 before/after JSON 完全一致。runtime SHA256 为 `8a5abbbefc987a78d136692d0b846e33fef3cf33f16b87bbe70f475e7af5cc6f`。原安装 runtime 全量的七项失败仍保留原结果；共享 toolchain/SDK 未修改。这一 PASS 只覆盖修改前 NULL cohort，不能转用于后续源码。
- Calcite 实际 PostgreSQL-backed column lineage r1 原 10 项行为 PASS、整体因 Checkstyle 行长 FAIL；r2 扩展 correlated/UNNEST 后 14 项中 11 PASS、3 ERROR，均旧 ANTLR 前端拒绝 UNNEST，整体 FAIL。SQL 与断言保留，按 Calcite 1.42 官方 FMPP 扩展机制研究统一入口；尚未证明所有列血缘/嵌套列/前端闭合。
- 已核对并原子应用冻结 Lance authority、filtered ANN 和独立 BM25 数学共 181 文件：联合 delta SHA256 `f18100a74de9112b888781fdc1c0df055f4d68a7ee089c3a28774bd525f33c30`，manifest `/tmp/pangrova-joint-source-20261008/applied-after.json`。两个共享文件经过三方合并，PlannedQuery 另精确保留 Root 严格血缘修改。仅源码应用证据，Rust/Native/JNI/E2E 均待新 cohort 构建验证。
- 新 Lance fixture 编译 r1 失败于已移除 FINALIZE 消息对应的旧 generated Java 文件；没有手改 generated sources，r2 使用 owning reactor `clean test-compile` 正规重新生成。Rust ABI13 fresh build 并行进行中。这两步不代表 integration test PASS。

### 2026-10-08 00:53 — 实际关闭 owner 与可复现 sanitizer

GovernanceContext/legacy Wiring 不再吞 lineage owner 超时；CronScheduler 先封住定时 producer 并等待真实 executor 退出，模块 closed 只在真实关闭成功后发布，失败允许上层 lifecycle 重试。真实 HTTP 写入以 Marquez PG ACCESS EXCLUSIVE 锁阻塞，两个外层重试用例连同原三项 outbox recovery 共5/5 PASS、0 skip，r2 `test` reactor SUCCESS。r1 `verify` 未进入这些测试，被新 Lance SPI31项 Checkstyle 阻塞；其精确7-file style delta已应用，r3 正在复核，不能把r2测试通过写成静态通过。

已保存 compiler-rt 源 patch、private-prefix build script、两个实际 toolchain 对照源码和复现文档。脚本 r1 下载 LLVM archive 网络超时，主动终止，仅该脚本子进程被停止；r2明确使用已核SHA官方source archives并fresh构建成功、无 compiler warning，未修改共享安装。r2 runtime SHA `335db5f29a170315aa8f1c538ee1a50516299729090031a71ba772e83e08c7a0`；23真实TLS/exception/negative controls符合预期、actual dladdr加载核对。此新artifact只有校准资格；先前4085全量PASS仍对应原private candidate SHA，不能挪用到新artifact。新Lance/ANN源码后续将重新全量验收。

Rust ABI13第一轮实际编译完成，但unused HashSet警告使canonical脚本exit1；删唯一unused import后重新编译，不放宽零warning条件。source修正精确delta `/tmp/pangrova-joint-rust-unused-import-unit-20261008/applied-after.json`。


## 2026-10-08 01:30 — joint cohort prerequisites and owner checks

Fresh ABI13 Rust FFI build r2 succeeded with zero compilation warnings after removing one unused import. The first run's actual unused-import warning and failed status remain recorded; they are not converted into a pass. Connector-lance clean test-compile succeeded in its 15-module reactor; this is compilation evidence only.

Fresh joint Release Native runs r1/r2/r3 exposed missing actual ArtifactBuildTaskContext and Arrow Buffer definitions, then missing platform PMR runtime symbols. Exact includes were repaired without changing assertions. The 9-file allocator v2 source unit (`d566cafef8ce4aee80733ff7790d2491493cd5e513a25236faf609033c8ecdca`) is now applied and the fresh build is running. It delegates live filtered operator allocations to the same Velox leaf pool; it does not close the existing unfiltered pre-Task heap ownership gap. No joint Native test pass, ASAN pass, package or JNI result is asserted yet.

Governance close owner r7 completed 5 actual PostgreSQL/Marquez HTTP cases, with 0 failures/errors/skips, in 46.42 seconds. The latest Cron delayed-task policy was included. Static r6 passed SPI and Protocol checks but failed Metadata's 193 Checkstyle violations. Exact first-cohort Metadata style deltas (6 files followed by 3 overlapping formatting corrections) were applied with all before/after SHA checked; static r8 is running. The test-only r7 pass does not erase the static failure or establish Coordinator-wide closure.

The private compiler-rt reproduction script built a new runtime and its 23 actual controls all matched expected outcomes. Its SHA differs from the earlier fully tested runtime; the historical 4085-case PASS is not transferred. Checked-in controls are outside the Native GoogleTest source glob.

Independent Calcite generation uses templates extracted from the actual 1.42.0 core JAR; template bytes match the official tagged source. FMPP 0.9.16 / FreeMarker 2.3.29 / JavaCC 4.0 generated and compiled the default parser, retaining two upstream grammar ambiguity warnings and two generated deprecation annotation warnings. A JavaCC 7.0.5 experiment failed Java definite-assignment checks. Neither experiment is a production parser migration or a zero-warning qualification. The first probe used a reserved unquoted VALUE alias and failed; dialect compatibility must be handled explicitly rather than reported as successful query coverage.

The first Lance SDK fixture still contains an invented endpoint string. Its real PostgreSQL/socket/SDK operations cannot prove a registered Worker actor from that string. A separate actual-endpoint fixture repair is required before this cohort's Lance tests run; no fabricated process registration or reader bypass is acceptable.

### 2026-10-08 02:10: actual joint build and first runtime failures

- Metadata/Governance nearest r10: five real PostgreSQL/Marquez HTTP recovery cases passed without skips; all thirteen reactor modules passed Checkstyle/SpotBugs. The prior two static findings were corrected (constant-time request/response hash comparisons, exact borrowed shared DataSource constructor classification). This does not qualify all coordinator scheduler ownership.
- Lance SDK actor endpoint unit: eight exact source paths applied after matching before/after SHA. Fresh fifteen-module Java clean test-compile passed. The actor listens before PostgreSQL issuance, receives the exact issued protobuf, and invokes the actual fixed SDK path. No fabricated Worker identity is introduced.
- Joint Release r5 failed on three fixed-SDK test compilation errors. The test-only r6 correction retains the real FileSink connector configuration lvalue for the writer lifetime and uses explicit common TypePtr conversions. Fresh Release r6 build passed without compiler warnings. Matching actual catalogue export emitted 781837 bytes; no synthetic catalogue fixture was created.
- First joint Native nearest runtime cohort: 109 cases ran, 69 passed and 40 failed (no source/binary SHA drift between recorded before/after). Failures cover Lance actual prepare/read, two accepted-base protocol fixtures, three allocator accounting tests, and physical position in filtered artifact hydration. These are actual failures, not excluded tests or full-chain completion.
- SDK UNKNOWN diagnostic unit: actual fixed FFI last_error is now visible on the formal UNKNOWN return. Release r7 rebuild passed; the exact new actor case still failed. Its real cause is a source-object HEAD path whose slash separators became percent-encoded `_pangrova%2Fwrite-staging...`; the source objects remain retained and no DRAINED/publication outcome is manufactured. Root-cause repair and fresh Rust/native requalification are required.
- SQL quality NULL semantics: nine real PostgreSQL oracle tests passed without skips, twelve-module static nearest passed. This qualifies TRUE-only predicate construction/result handling, not production Native quality wiring. An owning-module coverage/static run is pending.
- Calcite component research r4: the generated fixed JavaCC 4.0 parser parsed four actual UNNEST/ordinality/VALUES SQLs after explicit double-quote configuration. Production ANTLR remains present and three root lineage regressions still fail before Calcite; the generated component has upstream warnings and is not a production parser migration. JavaCC 7.0.5 actual compilation failure is retained.

Production signed Worker incarnation/outbox, pre-optimization BM25 binding, generic model invocation authority, asynchronous inference transport, sole Calcite frontend, full Release/ASan/JNI/E2E, evaluation execution and qualifying Linux performance remain open. No industry-best, throughput/P99, or final quality claim follows from these local checks.

### 2026-10-08 04:20: first applied cohort Release and independent drafts

- Fresh ABI13 Rust read-layer suite: 40 tests passed, none ignored, zero compilation warnings; actual fixed-SDK/MinIO race cases included. Log: `/tmp/pangrova-isolated-lance-sdk-read-layer-rust-full-r2-20261008.log`. Matching archive build r2 passed. These results do not qualify the second signed Worker invocation/outbox draft.
- Joint Release nearest r3: 109/109 passed, source/binary fingerprints unchanged. Full r1 ran all 4123 tests and failed six (4117 passed). Two actual temporary-file collisions, a tracked-pool fixture owner and an outdated capability-count oracle were corrected without deleting assertions. After temporary ownership was repaired, three generated-value positive cases reached a real null ArtifactBuildTaskContext dereference. The interrupted full r2 is not a pass.
- Generated-value production conversion now rejects a missing or undersized task read owner before materialization. Positive fixtures provide an actual shared TaskObjectReadBudget. The nearest seven cases passed; before/after fingerprints matched. Full Release r3 then ran all 4124 tests: 4124 passed in 307.49 seconds, exit 0, source/Release binary fingerprints unchanged. Log: `/tmp/pangrova-isolated-joint-native-release-full-r3-20261008.log`; exact before/after manifests are adjacent. Fresh matching ASAN configure succeeded and build r1 is running; no package/JNI/E2E result follows yet.
- Owning Governance/Protocol full r2 passed 118 and 428 tests respectively, zero skips; twelve reactor modules passed static checks. This does not remove remaining legacy fake fixtures or qualify production quality execution. Warehouse fixture passed one actual PostgreSQL case and fifteen-module compilation.
- Technical model catalog and invocation authority independent draft passed nine actual PostgreSQL cases, zero skips. Five captured-model Coordinator adapter tests all failed because the actual builtin-only Native catalogue does not advertise registered AI bridge capabilities. Positive legal-invocation assertions remain. No artificial advertisements were substituted; real Worker plugin/bootstrap evidence is still required. Production bootstrap changes and dispatch binding digest propagation remain unqualified.
- Protected logical lineage retention independent draft compiled five production sources with JDK17 and passed two actual PostgreSQL/official Calcite pruning cases. The draft retains table/column facts before physical EMPTY pruning. It is unapplied and does not qualify EMPTY SQL/JNI/Flight, final lifecycle lineage publication or modern OpenLineage facets.
- Subsequent visible BM25 projection, pre-Task memory ownership, generic connector read semantics, candidate Native return inference, signed Lance process receipts and model authority units remain separate integration drafts. Their component evidence is not transferred to the applied cohort. Production inference transport, sole Calcite frontend, complete evaluation and qualifying Linux performance remain open.

### 2026-10-08 04:45: first cohort ASAN and exact configuration draft

- Fresh matching `build-joint-asan` configured against actual ASAN Velox/protobuf/Abseil archives and the current ABI13 Rust archive, with empty LANCE_FFI_PREFIX. The build completed exit0, no compiler warnings. Actual dyld load control confirms the private reproduced runtime SHA `335db5f29a170315aa8f1c538ee1a50516299729090031a71ba772e83e08c7a0`; that control runs zero tests and is not function evidence. Full unfiltered 4124-case ASAN CTest is running with the real PostgreSQL broker. No full ASAN/PACKAGE/JNI PASS is asserted while running. Before/after fingerprints include source, Release/ASAN artifacts, runtime and unchanged suppression file, not every SDK dependency.
- Connector-lance actual fifteen-module `test-compile` completed successfully. The resulting fixture classifier JAR contains both actual LanceSdkPreparationActor classes and required consumer fixtures, verified from ZIP bytes and saved hashes. This is compilation/classifier evidence, not signed production Worker invocation evidence. Log `/tmp/pangrova-isolated-lance-actor-classifier-compile-r1-20261008.log`; receipt `/tmp/pangrova-lance-fixture-actor-classifier-unit-20261008/jar-verified-20261008.json`.
- Technical model catalog/authority independent draft now passes ten actual PostgreSQL cases, zero skips, including credential-reference rotation preserving the public signature while advancing the configuration revision and changing the exact operation digest. New technical catalog classes moved under the capability package `metadata.catalog.model`; the same ten cases passed again after that move. Latest log `/tmp/pangrova-model-authority-unification-unit-20261008/actual-pg-draft-tests-r6.log`.
- Exact model dispatch draft contains fourteen source files. Normal Java protoc 4.35.1 regeneration and JDK17 compilation r4 passed with no diagnostics, including the captured authority, its test class containing five cases and two production bootstrap sources. Compilation of the five tests is not execution; their previous five actual failures from missing registered AI Native capabilities remain unresolved. No fabricated model advertisement was introduced. Owning constructor consumers, coverage/static, real Worker positive execution and generic invocation permission rechecks remain pending.
- Read-only integration readiness checks found all 227 frozen C r16 Java source baselines match the isolated checkout. They remain unapplied while the first Native cohort is under ASAN. Later type normalization, EMPTY lineage and model-binding drafts still require exact controlled hunk integration and fresh qualification.


### 2026-10-08 05:35: completed first Native cohort, actual Java failures

- Full Release r3 passed all 4,124 tests (307.49 seconds). Matching fresh ASAN r1 passed all 4,124 tests (1699.73 seconds). All 689 recorded ASAN source/artifact/runtime/suppression SHA entries were identical before and after; no sanitizer finding was reported. Logs `/tmp/pangrova-isolated-joint-native-release-full-r3-20261008.log` and `/tmp/pangrova-isolated-joint-native-asan-full-r1-20261008.log`. These manifests do not cover every SDK/broker class and are not a hermetic-build claim.
- Step4 formally packaged the qualified host Release dylib, SHA256 `4ff5c4be7e5d24325a1b6a7ba5618c50e4720d0b680bb835c7cd0564fa7b119b`, with the existing sole packaging script. Two automatic Maven test-resource copy executions were removed because they copied a different stale build directory into the runtime classpath. Surefire configurations and test assertions were unchanged. This is macOS development packaging, not Linux production qualification.
- Step5 full private-M2 `clean install -DskipTests` passed all Java reactor compilation/install, with no javac diagnostics. Tests were not run. Benchmark shading overlap and an empty-JAR Maven warning remain, so zero overall warnings is not claimed. Receipt and log are in `/tmp/pangrova-first-cohort-package-java-20261008/`.
- An earlier nearest command accidentally ran in the human checkout because its workdir was missing. Its generated build outputs were left intact and its result is excluded from isolated-worktree validation. Subsequent commands explicitly bind the isolated checkout or the named private compilation preview.
- First packaged-JNI nearest r2 ran 40 tests: 24 passed, one failed, 15 errored, none skipped. The closed-executor exception lost its required IllegalStateException category/message. Separately, the output test waited for FINISHED before actual result consumption; the fixed SDK correctly waits for EOF. Failed shutdown then attempted released publication from Java RUNNING and prevented genuine runtime cleanup. No timeout increase or Native completion bypass was added.
- The exact closed-admission repair passed 19 actual JNI creation/race/recovery cases with zero skips. Owning `verify` still failed on 189 Worker Checkstyle findings; this is not a module PASS. Log `/tmp/pangrova-native-closed-creation-unit-20261008/nearest-r1.log`.
- The seven-file EOF/terminal-drain unit preserves all 74 original assertion statements and adds actual FINISHED-preserving shutdown. Its first runtime nearest run had 10 passes and 12 errors out of 22, zero skips. Five initial output cases reached their real oracles, then the deliberately missing-result-transport case exposed teardown ordering: its separately fixture-owned transport was closed after the TaskManager had closed the command executor. The original explicit transport-before-manager ordering must be retained; this failure is not erased by compilation or earlier Native passes.
- Signed Worker/outbox, field18 function/type, model incarnation/tenant/operation authority, exact field7 dispatch, EMPTY lineage, visible BM25 and task/cache allocation units remain separate drafts. Actual external inference, asynchronous serving transport, sole Calcite frontend, full owning Java/static/E2E/smoke and qualifying Linux performance remain open. No all-complete or industry-best claim is justified.


### 2026-10-08 08:45: real Java lifecycle, shuffle and model-service findings

- First-cohort clean packaged-JNI r4 passed 48 creation, concurrent close, lifetime, actual EOF, cancellation and exchange tests, zero skips. Checkstyle passed. Overall verify failed on 109 SpotBugs findings. Source, packaged Release artifact and actual Native artifact fingerprints remained equal before/after. This is nearest JNI evidence, not a Worker-module, coverage or full E2E pass. Log `/tmp/pangrova-first-cohort-worker-clean-verify-r4-20261008/verify.log`.
- Defensive copies for actual write metadata, filter snapshots, file modality maps, Native capability collections and remote-channel snapshots passed 13 nearest tests, zero skips. The Native capability positive oracle uses the actual exporter, not a fabricated advertisement. Log `/tmp/pangrova-worker-immutable-payload-unit-20261008/nearest-r1.log`.
- Actual remote-shuffle client cancellation/terminal signalling now separates bounded data from closure and completion; no sentinel batch is used. Seven real PostgreSQL/MinIO/mTLS gRPC regressions initially failed because the service registered onReadyHandler after gRPC initialization. Moving registration before returning the service observer fixed that cause; r2 passed all seven with zero skips, and the 33-module reactor succeeded. The cases include a real blocked-reader cancellation and a committed empty partition's genuine EOF. Log `/tmp/pangrova-remote-shuffle-read-drain-regression-unit-20261008/nearest-r2.log`.
- Shuffle partition queue publication/removal/accounting now share their queue lock, and manager closure signals all waiting producers and consumers. Twelve nearest test methods passed with zero skips, including existing jqwik properties and actual disk-byte verification. This does not qualify physical spill I/O drain or alignment with Native memory ownership. Log `/tmp/pangrova-shuffle-buffer-close-unit-20261008/nearest-r1.log`.
- Small Worker defects were repaired without altering the semantic test oracles: secure key randomness retains a live SecureRandom; logging sanitizes only CR/LF in the emitted filename; actual filename resolution is preserved; unused state and methods are removed. r1 exposed a remaining caller of the deleted unread field and failed compilation. After removing that dead assignment, r2 passed 59 tests and Checkstyle but overall verify failed with 81 SpotBugs findings. Two stale JaCoCo class-data warnings mean that run is not coverage qualification. Logs are retained in `/tmp/pangrova-worker-small-static-defects-unit-20261008/`. Subsequent explicit nullable-parent guards and a dedicated registry lock are applied but not yet requalified; a fresh clean combined verification is running.
- The independent pinned ARM64 Ollama 0.6.2 process actually loaded learned nomic-embed-text:v1.5 bytes. Both its native embedding route and OpenAI-compatible route returned two finite 768-dimensional vectors. Exact model manifest is `0a109f422b47e3a30ba2b10eca18548e944e8a23073ee3f3e947efcf3c45e59f`. These are serving-component facts only, not database/JNI execution, authorization, quality or performance evidence.
- A genuine overlong input was sent to both routes of that same service. `/api/embed` with truncate=false rejected it with HTTP 400 and a context-length error; `/v1/embeddings` accepted it with HTTP 200 and returned a vector after the pinned implementation's implicit truncation. Actual GGUF context_length is 2048 despite model parameters advertising num_ctx=8192. The private deployment configuration was corrected to 2048. The generic provider still has no proven exact tokenizer/input-limit enforcement. The serving contract must reject semantic truncation unless explicitly admitted. Receipt is retained in the private dependency directory as `actual-truncation-receipt.json`; no credentials or machine endpoint are checked into documentation.

Asynchronous Native inference, signed Worker/outbox, exact model authority/dispatch, NULL/EMPTY type closure, sole Calcite frontend, complete evaluation and qualifying Linux performance remain open. All local passing evidence is scoped to the tested source/artifact cohort.


## 2026-10-08 09:30: actual owning tests and type identity corrections

The first packaged Native cohort remains the exact previously qualified Release/ASan 4,124-test cohort; its packaged library SHA256 is `4ff5c4be7e5d24325a1b6a7ba5618c50e4720d0b680bb835c7cd0564fa7b119b`. No later private Native type, model-binding or Lance fixture draft inherits those results.

- Clean combined Worker nearest r6 executed 132 tests, all passing with zero skips and zero Checkstyle violations; the actual static gate still failed with 66 findings. Source/config/library manifests were equal before and after. Evidence: `/tmp/pangrova-first-cohort-worker-clean-verify-r6-20261008/`. This does not establish changed-line coverage or owning-module completion.
- The FILE MIME policy now uses locale-independent case comparison and rejects absent MIME metadata under restrictive policies. Its closest combined scope executed 10 tests with zero failures/skips and zero Checkstyle violations, while the full static report still had 64 findings. Evidence: `/tmp/pangrova-file-mime-policy-unit-20261008/`. Actual catalog-to-FILE policy publication remains unfinished.
- The captured technical model-authority private draft executed five real PostgreSQL regressions using unmodified inventory bytes produced by actual ServiceLoader/JNI startup (catalogue SHA256 `4b0b7582f45b5a803745dca796ac9ee6f6ba1d351c0c158ee0f55e62893f1c14`). Evidence: `/tmp/pangrova-captured-model-actual-native-producer-unit-20261008/`. This proves the tested authority/metadata components; it does not prove SQL model execution, a separately registered serving deployment, or independent backend drain.
- The unfiltered owning Worker `clean verify` run is still in progress at this timestamp. It has already exposed the old writer-registration stub's inconsistent columns and incomplete resource grants in HTTP epoch/control fixtures. A real Ollama model download is an active prerequisite. Preserve those failures; no final owning-suite totals or PASS can be claimed yet. Evidence: `/tmp/pangrova-worker-owning-full-r1-20261008/`.
- Two private test migrations have passed strict `javac -Xlint:all -Werror` and actual component execution: seven epoch/admission tests and five canonical resource-method control tests. They use actual packaged JNI owners, production admission reservations and real Flight consumers, preserve the four original control scenarios, and add a fresh admitted values task returning `42` before exact owner release. They verify 202 while real cancellation remains unjoined and 200 only after actual Native release. The resource-method tests are not network HTTP end-to-end tests. Evidence: `/tmp/pangrova-actual-epoch-and-admission-unit-20261008/result-r2.json` and `/tmp/pangrova-actual-http-control-unit-20261008/result-r1.json`. These source drafts remain unapplied while the owning run's sources are frozen.
- Exact borrowed-collaborator/static classifications remain a private draft: 50 live dependency identities and four timing-only random backoffs passed 474 controls over actual SpotBugs BugInstance XML. They do not classify the five legacy global-output-buffer findings, two generic output-handle exposures or three operator-path findings. Evidence: `/tmp/pangrova-worker-borrowed-owner-classification-unit-20261008/`. A classification is not proof that a lifecycle or full architecture is complete.
- The NULL/user-defined type draft had conflated plan-local and registered-binding anchors. [Substrait type classes](https://substrait.io/types/type_classes/) defines `type_reference` against that plan's own declarations and allows anchor zero. The new private correction makes the source domain explicit in both Java and Native, normalizes matcher inputs once and treats registered template results separately. Its eight Java sources passed strict lint compilation; Native compilation, a matching actual NULL capability producer and full SQL/Arrow/next-stage regressions remain outstanding. Evidence: `/tmp/pangrova-explicit-type-anchor-domains-unit-20261008/`.

The independent async inference service, full registered-Worker Lance fixture, parser migration, Semantic/AI control modules, Feature Store/evaluation execution and qualifying Linux performance/quality results remain unfinished. No overall completion or industry-best claim is supported.

## 2026-10-08 09:56：完整 Worker 失败已归档，继续真实根因修复

- 唯一私有 checkout 的未过滤 Worker `full-regression,coverage,static-analysis clean verify` 已结束；Maven 汇总 **1,425 tests / 18 failures / 41 errors / 0 skipped**，输入 before/after SHA 相等。日志与完整 Surefire 目录保存于 `/tmp/pangrova-worker-owning-full-r1-20261008`。同一 classname 跨引擎的 XML 被覆盖：WorkerExecutionChainTest、ResourceGovernanceTest、BufferMemoryGovernanceTest 的多次日志计数合计比最终 XML 多 10 tests / 3 errors，因此不能只汇总最终 XML 宣称完整套件数量。
- 六源真实 control/admission 单元已按 before SHA 合入；随后 Maven 最近回归 **144 PASS / 0 skipped / Checkstyle 0**，static **64 findings，verify FAIL**，输入相等；`/tmp/pangrova-first-cohort-worker-clean-verify-r7-20261008`。其中 exact epoch、seal、真实 JNI 42、实际 Flight、owner 退出断言均保留。
- 完整回归暴露 direct result 测试先等待 FINISHED、后消费 Flight 的错误顺序。十一源修复保留全部终态、预算和实际值断言，增加 canonical 请求在任何 reservation/publication 前验证，以及真正 query-key allocation 的只读快照。私有 strict javac 无诊断，**36/36 actual JNI/Flight/resource/property tests PASS / 0 skipped**，失败 r1=34/35 与修正 r2 全量证据保留。已合入 `/tmp/pangrova-actual-output-lifecycle-order-unit-20261008`；尚无修正后的所属完整 Worker 回归通过证明。
- Resource 未过滤所属模块 **177 tests PASS / 0 skipped / Checkstyle 0**；新增 query snapshot 方法新鲜 JaCoCo LINE 3/3、INSTRUCTION 10/10。static **13 findings，install FAIL**，输入相等，证据 `/tmp/pangrova-resource-ledger-owning-r1-20261008`。部分旧 Spill fixtures 只声明字节/空 action，测试通过不能证明实际回收；审计发现 production SpillManager 根本未调用 action、没有生产 buffer registration，正在迁移真实磁盘实现和实际释放回执。Resource 未通过 install 时，不执行依赖新 Resource artifact 的正式 Worker 检查。
- 借用的真实 DI owner 必须共享同一内存/任务/allocator/tenant authority，不可复制为值；54 个 Worker exact signature/field/type/timing-only 分类与 11 个 Resource exact identity 分类分别使用真实 SpotBugs findings 的 **474/99 引擎边界控制**验证后合入。数组、集合、全局旧 result buffer、generic mutable output handle、生命周期、constructor、filesystem security findings 继续保留；不是静态全通过证明，旧泛化 filters 仍待审计。
- 第二组类型候选统一 22 files，原始叠加 SHA 核对、正常 generator 两次相同、11 Python tests、17 Java 源 strict javac 与 3 actual probes PASS。真正 Calcite `SELECT NULL` → Pangrova/官方 Substrait 已生成同计划 nullable UDT；既有真实 Native inventory 正确拒绝尚未发布的 NULL bridge。候选未合入，新的 actual Native factory publication、Arrow/Exchange/SQL、Release/ASAN 全链路未验证，不继承第一组 4,124 Native tests。
- JNI Writer GlobalRef 私有真实 JVM/GTest 7 项生命周期/epoch案例通过，但 actual JNI wrapper 与 canonical task-owned writer 捕获仍联改中；空 JVM与功能探针 ASAN 均出现 JVM 分配泄漏，保留差额且没有加 suppression/禁泄漏检查，**不作 ASAN PASS**。Iceberg/Paimon 非 Lance 写入仍有生产 Java PageSink，尚未退役。
- Calcite 1.42.0 官方 JavaCCPlugin 明确选择 JavaCC 4.0；官方建议以 `config.fmpp`/`parserImpls.ftl` 扩展 core parser。7.0.5 在实际生成后出现 definite-assignment 编译错误，不能单凭版本号升级。正在核对唯一 parser 迁移的正常生成链路；生产 ANTLR 尚未替换。[官方生成插件](https://github.com/apache/calcite/blob/calcite-1.42.0/buildSrc/subprojects/javacc/src/main/kotlin/org/apache/calcite/buildtools/javacc/JavaCCPlugin.kt)、[官方扩展机制](https://calcite.apache.org/docs/adapter#extending-the-parser)。

全链路完成、无 fake、全 static/coverage gates、独立模型服务异步 CPU/GPU runtime、Semantic/FeatureStore/eval 与指定 Linux 性能资格仍未完成，不能宣称业界最优。


## 2026-10-08 10:58：真实回收、能力合同、取消重试与统一输入层

- Resource actual spill/reclaim 十一源按精确 before SHA 合入。SpillManager 只计实际 SpillableBuffer 返回的释放字节，保留部分回收对象；真实 DiskSpillableBuffer 成功批次立即回收内存、失败后保留剩余内存和文件 owner，readBack 保持原始行序，关闭失败可重试。正式所属模块 `full-regression,coverage,static-analysis clean install` **185 PASS / 0 skipped / Checkstyle 0 / SpotBugs 0，BUILD SUCCESS**，source SHA 相等：`/tmp/pangrova-resource-actual-reclaim-owning-r2-20261008`。真实 OS 文件大小限额造成第二批失败的实际磁盘组件证明也保留。该结果不代表生产 Native operator spill 已连通。
- Worker 最近 r9 **179 PASS**，Checkstyle 两处缺少 braces 导致 FAIL；只补 braces 后 r10 **179 PASS / Checkstyle 0**，static 剩 **10 findings，verify FAIL**。五个旧 global output buffer owner、两个 generic output handle exposure、三个 operator-config filesystem finding 未豁免，不能据定向测试宣称所属 Worker 完整通过。r8 实际仅执行150项，并非先前工具中的144项文字；以真实 Maven 汇总为准。
- `generated-values.overlay` 只有实际 immutable artifact cache enabled 才发布，其 Native typed relation 明确拒绝 candidate grants，使用 fill keys/object reads。Java 误把 MATERIALIZER 当 retrieval，导致合法 cache-enabled Worker 启动被拒。最小修正保留 SOURCE/MERGE/RERANK 的正数候选预算，独立校验 materializer 输入/输出字节和 typed relation，并拒绝未知 kind。真实完整 JNI GET_CAPABILITIES inventory SHA `c6281068497cdda6c833cb47501d9dd227ccb03d82edd9a4108fe7bd142d8ace`，matching lib SHA `4ff5c4be7e5d24325a1b6a7ba5618c50e4720d0b680bb835c7cd0564fa7b119b`。正向 round trip 不删除任何 capability/function；六项负向修改同一真实清单。**7/7 strict component PASS** 后正式 Protocol **435 PASS / 0 skipped / Checkstyle 0 / SpotBugs 0，clean install SUCCESS**，source SHA 相等：`/tmp/pangrova-protocol-materializer-owning-20261008`。
- 真实 snapshot producer 的 r1 使用 cache-disabled function exporter，缺少 materializer；r2/r3/r4/r5 失败均保留。r5 post-shutdown **Node=0 / Java=0 / Root=1GiB**，Root 是 probe 自身 createNodePool 配额，并非 Native allocations。r6 先验证实际 Node/Java 归零及 Root 仅保留该配额，再释放 probe 独占配额并保留原 Root zero oracle，**严格编译、真实 JNI bootstrap/cache-enabled GET_CAPABILITIES/close、-Xcheck:jni PASS**。撤回此前缓存泄漏推断；不通过清空能力或改生产 allocator 通过测试。证据 `/tmp/pangrova-materializer-capability-semantics-unit-20261008`。
- Cancellation/reconciliation 五源按 SHA 合入：exchange revoke 失败仍尝试实际 native cancel，并传播合并失败；已物理退休 owner 的实际 staged joined TaskReport 先验证完整 attempt/drain，再进行 Java 预算协调，不重发退休句柄命令。三项新增真实 owner cases 加全部原有 cancellation oracle，私有 **11 PASS**。正式最近 Worker 生命周期链 r11 **182 PASS / 0 skipped，source/native SHA 相等**：`/tmp/pangrova-worker-cancel-owning-r11-20261008`。该阶段运行 clean test，未重跑已知 static10，不能记为 verify 或 full Worker PASS。TaskReport 本身无 SecurityContext，授权来源仍为 actual admitted immutable Native creation owner，内部 staged bytes 不是外部 attestation。
- Calcite 正常 FMPP/JavaCC7.0.13 私有 source template 通过严格生成后编译，未手改 generated Java/压 warning。对照固定官方 Calcite parser 的 **40 positive AST/deep-equality/canonical SQL + 8 negative = 48实际parity PASS**，strict compile0诊断。保留 r1 Lex错误与 r2 colon conformance错误；实际官方 enum 不允许 colon field access，原四个SQL保留为两者都拒绝的负向案例。证据 `/tmp/pangrova-calcite-typed-template-unit-20261008`。生产ANTLR、自定义80类DDL及全部消费者尚未迁移；这不是生产parser替换、SQL/JNI或性能资格。
- 单一 ABC 私有源码输入层 `/tmp/pangrova-unified-abc-source-review-20261008` 正按真实 baselines 合并 C field18、EMPTY/return inference/NULL UDT/type domains、A188、B task-owned writer19、Native memory10、visible score19、pretask25 和 Root technical-model/binding。修正 clean text merge 漏传 snapshot incarnation/attestation key；driver 同时保留实际 taskPool accounting 与 captured function catalogue/type extensions；取消方法完整保留。此层**尚未应用、未正常全量生成/构建/Release/ASAN/Java/E2E**；B早期完整Java前置和A非空历史升级增量仍核对中，不继承第一cohort4,124通过。

完整 Worker/static/changed-line coverage、无fake的全部测试与生产链、独立异步推理服务、sole Calcite、全Native Iceberg/Paimon写入、Semantic/FeatureStore/eval、符合指定Linux硬件与质量门槛的实测仍未完成。任务继续，不宣告总体完成或业界最优。


## 2026-10-08 12:46: second source assembly and actual control-plane corrections

The second canonical source assembly is still private. Its frozen 6,833-file Native
input passed fresh Rust archive compilation and fresh Release configuration using
an explicitly captured registered-Worker Java classpath. Release C++ compilation
is running; this cohort has no full CTest, ASan, packaging, Java runtime or E2E pass.
The previous 4,124-test Release/ASan result belongs only to its older exact cohort.
The independent fixture prerequisite strictly compiled eleven actual fixture inputs
against thirty-two captured production class directories and explicit released
external jars, without old internal SNAPSHOT or Native resources.

The private Java preview compiled all thirty-nine production reactor modules.
Full regression test compilation remains failed on real Coordinator caller drift.
Registered incarnation/context and real Coordinator assembly migrations are being
reviewed; old static registration acknowledgements are not accepted as evidence.
The type/NULL/function, signed source history, writer, BM25 portable format,
Native pressure ownership and current model authority assemblies remain subject to
matching artifact validation. No component result transfers qualification to this
whole assembly.

Actual TLS tests exposed an allocator close defect when Arrow allocation checks
were enabled: closing a live allocator made later buffer release fail. Drain now
checks actual live bytes before closing, retains the owner and permits release then
retry. Four actual certificate/bootstrap regressions passed with zero skips and
Checkstyle violations; configured bootstrap advertises grpc+tls and honors its
allocator limit. A later increment removes forbidden reflective test access and
wires the acknowledgement buffer into production shutdown. Its first normal Maven
run failed because the test-only accessor was absent from the fixture classifier;
that input closure is corrected and the nearest thirty-five tests plus owning
static/coverage checks are running. Full Coordinator qualification is pending.

Five genuine control-buffer regressions all failed before repair. Admission now
serializes entry count, heap-byte debit, exact publication and retirement; quota
rejection cannot acknowledge completion, conflicting duplicate payloads fail,
scheduler rejection rolls back, and close cancels owned expiration futures and
seals publication. Thirty-five related real buffer/service/producer/TLS tests
passed after repair, with zero skips. Final source Checkstyle passed after an
empty-line correction. This is bounded buffered heap ownership, not total JVM or
borrowed in-flight transport memory qualification.

Independent model-serving durability tests used actual pinned nomic embedding
responses and an actual process restart journal. Six genuine backend requests
cover durable issue before forwarding, terminal persistence before budget release,
natural cancellation drain, uncertain lost response without resend, and separate
process recovery. Local serving evidence does not establish SQL/Arrow/JNI model
execution, durable database admission recovery, GPU execution or publication fences.
Real CPU crossencoder serving and persistent catalog grants are prepared; the old
packaged Native startup guard correctly rejected its changed protocol. Two binder
tests were found, none started or skipped, and the run failed. They await a matching
new artifact rather than a fabricated capability advertisement.

Complete Native physical writes, sole Calcite frontend migration, all original
oracles without fake collaborators, Semantic/FeatureStore/evaluation, full owning
Java/static/changed-line coverage, E2E/smoke and the specified Linux performance and
quality experiment remain unfinished. The task continues; neither overall
completion nor an industry-best conclusion is supported.


## 2026-10-08 13:13 continued ownership and matching input validation

The exact current Stage retry and HTTP mapper sources passed thirty closest
Coordinator regressions: eleven new actual Stage ownership scenarios, all four
original retry scenarios, eight original Stage tests, and all four original plus
three new HTTP mapping tests. Zero tests failed or skipped and javac emitted no
warnings. Changed source Checkstyle is zero. The real Stage is now the sole retry
counter; missing/unknown cancellation causes cannot become invented runtime
failures, duplicate notifications cannot clear a newer attempt, and query plus
actual DAG object identity is checked. This is local scheduling ownership evidence;
actual distributed cancellation, durable replay and external publication are not
qualified by these tests. Accumulated closest JaCoCo evidence covers 31/32 changed
executable retry lines and 12/13 HTTP mapper lines, not the full PR coverage gate.

The acknowledgement transport's thirty-five closest tests passed, but its owning
Coordinator verify failed with 594 Checkstyle violations. Independent diagnostic
SpotBugs also failed with 308 findings and zero analysis errors. Earlier nearest
class-level style results must not be interpreted as full-module static success.
The control buffer's separate frozen closest coverage remains 37/38 changed
executable lines (97.37 percent).

Fresh unified Native production objects and the real function exporter linked.
The exporter produced 785686 actual catalogue bytes (SHA256
448452826c17f5f9d45f72a816fe9575480666c6d68440a62b4083457859a8b4).
The unmodified production Java codec and FunctionCatalogView accepted this exact
catalogue against the immutable independent fixture classpath. Binding and
selection identities match. This establishes a producer/consumer prerequisite,
not JNI startup or Native execution. No old test catalogue or Root packaged
library was overwritten. Full build r3, r4 and r5 each failed on concrete test
caller/header drift; original failures are retained. Header/factory/actual
catalogue caller corrections retain every original assertion. Exact-source r6
build continues; full CTest, matching ASAN and new packaging have not run.

Memory policy and result retention successors remain frozen review candidates.
Independent serving and demand-mask SDK evidence does not establish the full
SQL asynchronous model operator, database recovery or fenced publication chain.
All remaining acceptance requirements listed above remain active.


## 2026-10-08 13:44 fresh cohort and real fixture prerequisites

Exact-source Native r7 configure and full build passed with zero compiler warnings
and zero source drift. The matching Rust library tests passed all forty original
checks, none ignored; the archive hash was unchanged by the test run. These are
build and Rust evidence, not a complete Native runtime qualification.

Three actual closest CTest attempts retained the same binary/source hashes. All
failed their registered Worker setup, so none of the 110 execution assertions ran.
The first failed because Docker API 1.32 was below the actual daemon minimum 1.41.
The second used process-local API 1.54 but mounted the macOS client socket into
Ryuk rather than the daemon VM socket. The third used the official VM socket
override and started real Ryuk plus PostgreSQL, then failed because the sealed
fixture classpath lacked Flyway. No test or Ryuk cleanup was disabled. Official
configuration: https://docs.rancherdesktop.io/how-to-guides/using-testcontainers/.
Only exact owned failed supervisors were recovered; those control recoveries are
not successful Native/Worker/source teardown evidence. Original logs and states
remain archived, and a separate dependency-closure successor is in preparation.

Private assembled successors remove the dead Java result buffer and quota paths,
retain their original oracle mappings, and preserve actual JNI retirement as the
owner-release authority. The live-pressure successor excludes tasks with no real
statistics rather than fabricating zero. These Java sources have not acquired
matching Native runtime qualification. The r7 reactor reached the final E2E module
after Worker and Coordinator test compilation, then failed on fourteen caller
errors in six files. Exact caller migrations are being integrated with all
original assertions retained.

Fresh r8 Native inputs now include the actual governed task-root statistics and
the process-join fixture-tool correction. The statistics path keeps the acquired
executor alive through the existing driver fence, includes preparation and result
leaf usage, and removes the zero-memory fallback. The original nineteen synthetic
statistics methods and four report methods remain listed for actual producer
migration. Strict compilation of the five changed C++ translation units passed;
full r8 build is running. No new library has been packaged into Java, and full
CTest, matching ASAN, Java execution, E2E, smoke and the required Linux performance
experiment remain unqualified. The asynchronous model operator and all previously
listed acceptance gaps remain active; overall completion is not asserted.


## 2026-10-08 13:51 complete Java caller compilation

The private exact r9 source cohort (6837 files) passed the complete full-regression
reactor test-compile, including the final E2E/fuzzer/chaos/smoke modules, with zero
warning lines and zero source drift. No test was executed by this compile phase.
Nine E2E caller/helper sources were merged from their exact before inputs; the
Sql factory hunk composed cleanly with the three Root captured-admission/registry/
issued-epoch corrections. All original test methods, property scopes and assertion
starting points are retained in the frozen migration matrix. The isolated E2E
static check still has 47 existing violations (previously 60), and strict dependency
compilation retains three existing AutoCloseable warnings; those are not waived.

Native r8 configure and full build also passed. The actual r8 exporter and
production Java validated the same real catalogue identity as r7, with no old
resource replacement. Matching runtime, full CTest and ASAN remain prerequisites
for packaging. The incomplete original fixture dependency selection is being
replaced with a formal Maven conflict-resolved graph; its implicit Jar manifest
paths are being audited rather than silently accepted or stripped.

An initial spill-root audit inference was corrected after tracing the actual
QueryCtx::Builder: it already installs the QueryCtx-owned memory reclaimer. No
generic root reclaimer was added. Real positive spill/statistics evidence and the
original synthetic statistics migrations remain pending.


## 2026-10-08 14:40 real closure, terminal correlation and remaining runtime blockers

Private Java r10 and r11 complete full-regression reactor test-compile passed with
zero warnings and zero source drift (r11: 6860 input files). The E2E close/split
successors preserve all 217 original members and eleven original registry bodies;
21 combined Java sources now have zero canonical Checkstyle violations. Actual
Arrow component tests passed three tests, without loading Pangrova JNI. Those
checks do not establish Native E2E, dynamic-table refresh, CDC or target publication.
Audit identified old dynamic-table test refresh pass-through and unavailable real
CDC wiring; the previous source-table row counts cannot qualify those features.

The sealed registered fixture now contains the canonical Maven conflict-resolved
348 external artifacts, exact matching r10 production classes and eleven strictly
compiled fixture sources. Real PostgreSQL ran all 51 available migrations to v56
and passed fixture construction/retirement. CTest prerequisite r4 reached native
initialization but failed because the private shuffle directory was absent. The
owned directory creation successor fixes that cause. Prerequisite r5 then passed
native initialization and failed actual Worker registration because production
host validation rejects 127.0.0.1. Both failures and all 126 not-run assertions
remain recorded; neither run counts as passing Native tests. Actual failed broker
wait/reap and removal of its own endpoint passed, without claiming SDK execution
drain. A real assigned non-loopback advertised host successor is in preparation.

Fresh Native r11 configure/full build passed with zero warnings/source drift;
matching fresh Rust tests passed 40 tests, zero ignored. The library remains
unqualified and is not packaged for Java execution. Seven original statistics
methods and twelve original operator methods now have actual SDK producer
migration candidates, with every original method/relationship retained in their
oracle mappings. LATERAL generate_series exposed first-match dispatch and lost
cross-batch input-row cursor defects; the frozen bounded cursor successor retains
the original 50000-row oracle and INT64/NULL/memory/cancel coverage. Runtime is
pending actual registered fixture setup.

Task terminal registry generation correlation independently reproduced seven
failures in eight new actual Stage tests. Capturing the registration generation,
retaining early replay slots and enforcing actual query/stage ownership made all
eight new plus the existing 65 closest Stage/HTTP/control/TLS tests pass: 73 tests,
zero skips/failures. Strict compilation and canonical Checkstyle passed with zero
diagnostics; actual closest changed-line coverage is 40/41 (97.56%). This proves
local correlation only. Physical exact-attempt drain before retry, callback/reset
concurrency, speculative physical/logical ownership, full Native CTest/ASAN,
packaging, Java execution, full static gates, E2E/smoke and the required Linux
performance workload are still open. Asynchronous AI continuation, sole Calcite
frontend, Native writes and all other prior acceptance gaps remain active.

## 2026-10-08 15:22 actual registered Worker and newly exposed runtime failures

- Fresh unified Native r15 built without diagnostics or source drift. Its registered CTest r7 setup and cleanup actually passed: 146 assertions ran, 134 passed and 12 failed. Cursor8 and Operator12 all actually passed; this is not a full Native, ASAN or JNI qualification. The broker normal-exit/reap receipt is independently scoped and does not prove all SDK/source drain.
- Real assigned-interface registration and the short private UNIX endpoint succeeded. The immutable fixture runtime closure still uses sealed r10 production classes, 348 canonical external artifacts, and exact fixture successors; in-flight Java targets are not substituted.
- The 12 failures are retained. Frozen successors correct actual task start, normal-completion EOF interpretation, named Presto serde registration, actual parallel prestart empty-stat semantics, a real 4096+1 fragment commit construction, actual task function-publication admission, and exclusive mkdtemp test-directory creation. The original one BM25 cache-lock failure passed on exact r15 standalone rerun; its observed cause is not claimed proven. The production ownership refusal and all original result/coverage assertions remain.
- Report4 now has a real registered-source BM25 artifact task candidate rather than a fabricated artifact context; runtime qualification is pending. Its SDK task proof does not imply Coordinator ArtifactJob, SQL, JNI, or metadata artifact publication proof. Native r16 and matching closest CTest r8 are running against these exact composed sources.
- Java r13 production and test compilation passed across the assembled reactor, zero warnings and zero source drift. Paimon actual-read-mode correction is included. It does not qualify the Native changelog physical schema or source-range contracts.
- Calcite frontend migration has actual FMPP/JavaCC and AST/deep-copy parity evidence for 18 SHOW forms, with remaining DDL and production factory migration explicitly unfinished. SQL series still has Java VALUES expansion and lacks the full typed Native admission/lowering path despite SDK cursor tests passing.
- Coordinator retry still needs physical-owner drain wiring, generation-fenced mutable Stage/async dispatch, and DAG-owned query finalization. Fresh full CTest, matching ASAN, native packaging, Java runtime/E2E/static/coverage, genuine CDC target publication, and required Linux performance acceptance remain open. No overall completion or industry-optimal claim is made.

## 2026-10-08 16:08 exact compiled objects, remaining Flight failure and independent Java progress

- Native r16 closest r8 actually ran 148 assertions: 142 passed, six failed; registered Worker setup/cleanup passed. All failures and original positive spill/result/report assertions are retained. Actual spill uses a single runtime configuration, and successful reclaim can reduce used bytes while returning zero released reservation bytes; the successor checks that real decrease and retains positive physical spill, complete rows and task-stat relationships.
- Incremental source/binary hash stability did not establish matching object inputs. The registry source already rejected an empty generator, but its old object lacked that guard. Independent exact-source compilation confirmed the guard. Consequently r17 used a new empty Release build directory, disabled compiler launchers/cache, and compiled all 551 translation units, including 195 owned production and 198 owned test C++ inputs. Both build phases passed, zero warnings/source drift; the 393-source/command/object SHA manifest is retained. Pinned vendor source reuse does not imply old object reuse.
- Fresh r17 registered closest r9 ran 150 checks: setup/cleanup and 147/148 assertions passed. All six r8 failures passed, including the actual BM25 artifact task report and executable table-function registry. One real Arrow Flight shutdown assertion failed its unchanged two-second deadline. Same-binary standalone registered r10 passed, but independent unchanged direct executions reproduced the failure twice at approximately five seconds; real gRPC traces show delayed GOAWAY handling. No intermittent-failure dismissal, relaxed timeout, full CTest, ASAN, packaging or Java runtime qualification is claimed.
- Stage generation ownership passed 90 actual closest methods, zero skips, strict compilation and canonical style zero diagnostics; changed executable coverage is 36/38 for Stage and 23/24 for StageStateMachine. This does not yet fence delayed DAG dispatch callbacks or prove physical drain before retry.
- Java assembled r14 and r15 production/test compilation passed, zero warnings/source drift. r15 includes the query-owned committed-write receipt and six-failure Native source successor. Actual dynamic-table checkpoint CAS passed eleven component tests, including PostgreSQL and immutable-predecessor rejection. Committed-write receipt has three actual negative-state tests; successful Native target publication is still pending.
- DAG-owned completion remains a private draft. Nine actual lifecycle/state tests passed; the new actual QueryManager finalization tests exposed strict try-resource and package-visibility errors before execution, retained for correction. Callback consumers, synchronous lifecycle ordering, short metadata-operation ownership and durable-pass failure settlement must close before this draft is assembled.
- Sole Calcite formal frontend now covers 52/75 command forms, with 23 remaining. Generated parser/AST and real correlated series components passed without replacing the production parser factory. Async AI full-expression continuation, protocol/native operator wiring, original SQL truth production paths, durable CDC definitions/current definer/source pinning, all broader static/E2E/smoke gates and the specified Linux performance acceptance remain unfinished.

## 2026-10-08 16:54 full Release evidence and exact successor work

- Native r18 rebuilt the exact Flight shutdown successor after verifying all 393 fresh-r17 owned object inputs. Registered closest r11 passed 150/150 checks, including setup/cleanup and 148 assertions; zero source drift and unchanged binaries across the test run. The shutdown deadline remains two seconds. Full r18 Release then ran 4,172 checks: 4,164 passed and eight failed. Matching ASAN, package and Java runtime gates remain blocked by this actual full regression result.
- The eight failures are retained: two cache cases, write-failure and literal negative contracts, capability-count metadata, two governed AI structural conversions and the real rerank conversion. Reviewed successors preserve original assertions: exclusive mkdtemp cache directories; the implemented sixteenth BM25 materializer capability; actual write/literal rejection boundaries; and complete model bindings generated through the production Java contract producers from actual Native factory inventory. The AI inventory contains zero available models and the actual disabled provider refuses resolution; its two successful conversions do not establish inference or authorization. Cache parallel diagnostics passed before and after without reproducing a collision, so the historical collision cause is not claimed proven.
- These eleven exact successor inputs are composed in private Native r19. All 393 owned objects were removed before recompilation following the new test-prerequisite wiring. Configure passed; build is in progress. The frozen input/source/command/object receipts are retained. No in-flight or human checkout sources are replaced.
- Private DAG outcome draft passed 18 exact owner/notification tests and 108 broader actual component tests, zero skips/failures. The original finalizer reproduced two failures in three real QueryManager tests. Lifecycle notifications now precede reentrant outcome-consumer cleanup. Latest coverage execution and durable-pass failure propagation/observer exception handling remain under review before assembly; old 105-test coverage is retained separately.
- Durable metadata mandatory-provider migration still has 30 exact remaining getter compilation errors. Real provider successors and a common PostgreSQL JVM owner passed independent component checks, but the shared contract is not assembled. Real PostgreSQL watermark tests reproduced three failures in four methods: empty/partial histories advanced committed watermarks. A minimal history-authority and read-start fence correction is in progress; no real Iceberg CDC or Native target commit is inferred from these tests.
- Actual rerank conversion still requires the canonical TaskRequestFactory execution identity and real resource template. Its CPU deployment and authority owners are kept alive during fixture qualification; no fake idempotency key or admitted task is supplied. Sole Calcite factory migration, full asynchronous AI operator/serving, durable source/target ownership, current full static/E2E/smoke gates and the specified Linux performance workload remain open. Overall completion and industry-optimal performance are not established.

## 2026-10-08 17:54 exact r19 full regression and r20 preparation

- Unified Native r19 configure/build passed, zero warnings and source drift, with 394 owned source/command/object receipts. Registered closest r12 passed 234/234 checks. Full Release r12 ran 4,172 checks in 490.77 seconds: 4,171 passed and one rerank conversion failed. Binaries remained unchanged through both runs. All seven other r18 failures passed; their prior failed receipts remain. ASAN, packaging and Java runtime qualification remain blocked by the full failure.
- The rerank successor uses the actual pinned CPU ONNX model, real PostgreSQL tenant/model grant and authenticated actor, matching raw r19 Native inventory, ordinary SqlCompiler/TaskRequestFactory producers and actual Worker resource limits. Original TopN/schema/order/argument and model-drift assertions passed two actual component tests. The produced request intentionally remains unissued; missing task admission is rejected. This does not establish an admitted Worker model task or full SQL execution.
- Actual broker and exporter wait/reap succeeded, both exit zero. A separately qualified single absolute monotonic 120-second deadline now covers both joins. The sealed final prerequisite classpath has SHA e9b44e98350576f4b9b06596474e7f9827880ec40f06b3618616d9ff4efe149a; priority classes match the actual successful run. Per-deployment PostgreSQL ownership still needs the common JVM singleton successor before whole-Java normative qualification.
- Private r20 is recompiling all 394 owned objects with reviewed rerank inputs, shared join deadline, formal Maven-module location for the governed Java model-binding producer and sealed prerequisite classpath. The first sparse-source synchronization stopped before compilation because the upstream Java baseline was absent from the Native snapshot. Its exact five touched inputs were restored and recorded; the corrected runner validates every input before mutation. No r20 runtime qualification is claimed yet.
- The private DAG outcome draft passed 23 exact actual tests and 113 broader component tests, zero skips. Changed executable coverage is 85/97 (87.63%); DurableDAGControl contributes two uncovered lines. Real scheduling-preparation refusal previously lost its cause and now preserves it. Closed metadata-operation ownership refuses terminal publication and produces structured diagnostics. The remaining old terminal-only test callback uses fabricated Native reports; real durable-pass integration and callback contract closure are still unfinished, so the draft is not assembled.
- Real PostgreSQL watermark tests now pass all five unchanged/current oracles after filtering noncommitted histories and fencing at source-read start. The original implementation failed four of those five, including a concurrent late change. These are timestamp/history components, not opaque source offsets or Native target commits. Real metadata Cache/Alter/Resolve provider migration passed eleven original methods, with actual Iceberg, PostgreSQL and Caffeine stats. Combined mandatory-provider compilation still fails with 73 errors: 24 missing required getter implementations, 24 obsolete no-op overrides and 25 legacy Native/fixture helper gaps. The shared contract is not assembled.
- Sole Calcite frontend components cover 58/75 command forms. Normal fresh Maven production/test compilation through real Protocol protobuf/test-jar and Planner FMPP/JavaCC generators passed across thirteen modules with zero warnings. The production parser factory, remaining seventeen forms, asynchronous AI operators, genuine CDC publication, full static/E2E/smoke and specified Linux performance workload remain unfinished. No overall completion or industry-optimal claim is made.

## 2026-10-08 18:16 r20 closest passed; full prerequisite EOF retained

- r20 configure/build passed, all 394 owned objects recompiled, zero warnings/source drift. Registered closest r13 passed 251/251 checks in 130.78 seconds, including real CPU rerank original conversion/drift assertions and actual broker/exporter cleanup. Binary identities stayed unchanged. Independent Java r16 normal production/test compilation passed all 39 modules with zero warnings/source drift; no JNI runtime qualification is transferred.
- Full Release r13 failed its actual registered fixture setup after 50.42 seconds. Five independent package/provenance checks and cleanup passed; 4,165 Native assertions were NotRun. Total 58.34 seconds. The actual CPU producer threw HTTP/1.1 header-parser EOF in requireActualServingLimits at line233. Broker actually exited zero; exporter actually exited one; both were reaped. Unknown HTTP completion is not blindly resent, and NotRun entries are not counted as executed tests. ASAN/package/Java runtime remain blocked.
- Rehashed 10,303 frozen prerequisite production class/resource/external artifact/priority class inputs without drift. Sealed r10 production classes remain explicit dependencies; mutable r16 targets are not substituted. A separately qualified shared PostgreSQL JVM owner and failure diagnostics are being reviewed, while the HTTP EOF cause still requires actual framing evidence and a minimal successor. A fresh success by itself does not close the EOF.
- Private asynchronous Stage callback successor passed 27 direct and 117 related actual component tests, strict Java17 and canonical style zero diagnostics. Its extracted unfenced parent composition failed three of four Stage/future ownership checks. Incremental changed coverage is 18/36 (50%), helper10/10 and actual engine/leaf invocation still partly/unexecuted. No fabricated Worker/request/result/report is used in those four passive-observer checks. Physical drain, complete issuance under concurrent retry, the parent's remaining terminal-only fabricated-report consumer and real durable-control execution remain unfinished; the successor is not assembled.
- Direct exact-r20 Native registry lifecycle successor is frozen with nine existing sources and a document: immutable shared owner publication/capture, atomic withdrawal before actual reclaim wait/join and retained owner destruction outside the lifecycle mutex. Eight affected/existing translation units passed strict compilation. Three actual shutdown/reinitialization/held-reclaim-lease runtime oracles require a new matching ABI cohort. Typed series publication is deliberately kept in its separate unfinished frontend/producer chain.
- Real metadata query-lifecycle and transaction successors passed twelve and seven original methods respectively, including actual Iceberg snapshots, query admission and PostgreSQL ownership. Combined mandatory-provider boundary still has 65 compilation errors: twenty missing required getter implementations, twenty obsolete no-op overrides and twenty-five legacy Native/fixture symbol gaps. Source-only complete Calcite parser generation passes normal Maven; seventeen forms, production factory cutover, DEFAULT/nullability full connector execution, asynchronous AI operators, CDC target publication, full static/E2E/smoke and specified Linux performance acceptance remain open.


## 2026-10-08 18:50 actual HTTP close defect and exact r21 cohort

- The same pinned real CPU serving implementation and JDK17 reproduced the HTTP EOF with an actual connection trace: after 74 known successful responses, the closing socket returned to the client pool and the next POST reused that same socket before EOF. The unknown 75th request was not resent. Original r8/r20 failures remain separate historical receipts; their specific sockets were not captured by this diagnostic.
- The minimal serving successor adds explicit Connection: close to actual JSON responses and verifies actual health/first/replay/conflict headers, preserving original 409/400/413, TopN/schema/argument and model-drift assertions. The constructor now cleans up after AssertionError as well as Exception. A paired unchanged bounded diagnostic passed 512 known responses with actual inference count one; final normal fixture setup/original two Native checks/cleanup all passed with actual process reap. This is protocol and fixture evidence, not full admitted AI SQL execution.
- Four exact reviewed stages (immutable table-registry lifetime owner10, Common PostgreSQL infrastructure3, rerank shared database/diagnostics2 and explicit HTTP close3) assembled without conflicts, collapsing to sixteen changed input paths. Frozen priority class/resource and inherited production/external inputs were rehashed: 10,369 verified files, no drift. Sealed classpath SHA6522a109b8ea7d5768f544082cb6d78919b9b7af445a486b9ebd54bd043d7b41; newest Common owner source and its three actual-loaded class bytes match. All 394 previous owned objects were verified and removed; r21 configure passed and full owned recompilation is in progress. Closest/full CTest, matching ASAN and packaging are pending. Native snapshot is unchanged during compilation.
- Normal Java r17 test compilation exposed five real Common main-source failures: Awaitility inherited the parent's test dependency scope. The old Common model-only validation is not upgraded to compilation success. An exact one-POM successor explicitly declares compile scope. Normal nearest ten-module and full thirty-nine-module r18 production/test compilation passed, zero warnings/source drift; failed r17 logs remain. This later build-only POM successor is in the Java review cohort; it does not mutate the in-flight Native source or transfer JNI runtime qualification.
- Calcite component coverage advanced to 61/75 command forms, with genuine generated-parser CREATE TABLE/CTAS query ownership and thirteen-module normal generator compilation. Actual normalization still duplicates a single source AI call in simple CASE and COALESCE; corrected IS TRUE and searched CASE each retain one. Source-call ownership must survive normalization and whole-program execution; content-only deduplication is insufficient. Production factory cutover and fourteen remaining forms are unfinished.
- Real provider boundary r7 still failed with 61 compiler errors: eighteen required getter implementations, eighteen obsolete overrides and twenty-five legacy fabricated Native/Worker/helper dependencies. Tag properties subsequently passed all three original fifty-check properties against actual persisted Iceberg snapshots (146 real database scopes; four original equal-input returns). Real data-file statistics passed eight original methods plus actual backend-displacement/refusal/retry after stopping silent empty-statistics fallback. These newer units remain private; full mandatory-provider assembly is unfinished.
- Further lifecycle audit found startup UDF registration/publication lacks the runtime drain lease: current shutdown can destroy handles and join executors before rejecting an active startup operation. Registry deletion has not been shown to race that operation; the verified defect is partial teardown before startup operation exit. A separate exact successor is under investigation. Indexing publisher/checker/GC/poller and bootstrap scheduler shutdown also return without actual termination in several paths; startup-test extra joins are not production closure. Durable DAG control, physical Stage drain, asynchronous AI execution, CDC publication, full static/E2E/smoke and specified Linux performance remain open. No overall completion or industry-optimal claim is made.


## 2026-10-08：r21 完整 Release 与索引 scheduler 关闭证据

- r21 的 394 个自有 TU 已全部重新编译，零警告、零源码漂移；相关 258 项与完整 4,175 项 CTest 均 PASS、零 skip。完整运行 537.49 秒，setup/cleanup 成功，测试前后二进制相同。完整源码 6,860 项和二进制已存于 `/tmp/pangrova-unified-native-build-unit-20261008-r1/qualified-r21-release-archive`；这仅是 Release 资格，不转移到后继变更、ASAN、打包或 Java/JNI 运行。
- Root 索引 scheduler successor 8 项源（5 production、2 test、1 doc）已冻结并进入私有 ABC source review，未修改共享主工作区。实际 PostgreSQL 锁住 publisher 的 production SELECT 时，旧实现真实 2 FAIL；最终 successor 真正等待退出、超时报错且可重试，连同两个原启动断言共 4 PASS/0 skip。Java 17 `-Xlint:all -Werror` 与 canonical Checkstyle 通过；最终同源覆盖率 15/16=93.75%，bootstrap 子关闭异常传播的一行未覆盖。先前覆盖率运行中源码换行造成漂移，已作废；第一次结果文件名误复用的限制记录在 `coverage-history-invalidated-r1.json`，不得引用该轮 100% 为资格。完整 immutable classpath 重核、ordinary reactor 测试、build-coordinator/borrowed executor、metadata-sync/invalidation scheduler 和实际 publication 仍未完成。
- Java r19 私有普通源码同步 20 项，6,891 项输入；最近 coordinator reactor test-compile 正在运行。完整 39-module 与实际 runtime 尚未取得新资格。
- B 的 direct-r21 catalogue-entry/drain successor 12 项（11 changed、原 startup6 文件字节不变）已在独立 review 核对并组装，零合并冲突。新 sealed CP 的 1 个实际 JVM witness 类加原 385 entries 真实闭包已核验；后继 r22 加 Common Awaitility compile scope，共 12 实际变更，新的 394 TU 完整编译正在运行。新增 SDK4 和实际 raw-JNI shutdown/re-init witness 等待匹配产物运行，未借用 r21 PASS。
- A 正规 Calcite parser 已完成 63/75 forms 的组件验证（maintenance successor 原 DdlDml54+原14+新增4=72 PASS）；余12、唯一 factory/dispatcher cutover、AI source occurrence 在 simple CASE/COALESCE 规范化中的重复、DEFAULT 存储读写语义均未完成。
- C whole-provider boundary r8 真实 strict FAIL 53 errors（14 mandatory getter、14 removed-noop override、25 legacy fake/helper），不能以私有 real-provider 组件 PASS 代替全域。实际 TTL 负例还证明缺 tenant/principal 的生产 maintenance session 能删除真实 SDK branch。沿实际 CREATE TABLE policy producer 实现持久 current owner 与 provider commit fence，保留 time-bounded lease 语义；不能把 PG 行锁或 pre/postcheck 宣称远端原子提交 fencing。

研究核对：Java 17 [ExecutorService](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html) 明确 `shutdownNow` 不等待实际终止，支持此次真实 JDBC join 修复。Google [Chubby 原论文](https://research.google.com/archive/chubby-osdi06.pdf) 要求受保护服务校验 sequencer；借鉴点是源提交边界 fencing，而非把锁请求成功视为源操作仍有效。

全部完成、业界最优、大规模性能与完整验证链目前仍未验收；不以组件通过替代未闭合链路。


## 2026-10-08：r22 前置失败的真实根因与线程退出后继

- Java r19 普通 39-module production/test compilation 已通过，6,891 输入零漂移；同普通 reactor 的两个原索引启动断言与两个真实 publisher 锁阻塞断言共 4 PASS/0 skip。此处真实 PostgreSQL 为 51 migrations/maxV56，不借用 C 的私有 V57/V58。Root scheduler8 最终完整 classpath 392 entries/10,490 文件再次校验并实际运行 4 PASS，零依赖漂移。
- Native r22 的 394 个自有 TU 已完整重编译，零警告、零源码漂移；raw SHA `51aa8471c6ff2c125535059eb5bbd484434f7342abe83e6d954329e304394720`。相关 CTest r15 setup 失败，268 total 中 266 Native 用例 NotRun，setup/cleanup 当次失败。原 owner 状态改变后，再次原 owner cleanup 实际 wait/reap broker exit0、exporter exit1；不声称 SDK/source drain。完整 Release、匹配 ASAN、打包与当前 JNI 集成链未执行，r21 的 4,175 PASS 不转移。
- 已纠正最初“Worker 未就绪”的解释：实际 child 生命周期日志表明 Worker 完整启动并登记；真实 CPU container 启动耗时 PT4M48.518389S，源码在每次启动中联网执行 pip 安装，然后才初始化 deployment/JNI witness。固定 supervisor 110 秒预算先超时并关闭实际 Worker，exporter 晚到才读取已删除的 readiness 文件，造成后继 30 秒超时。JNI witness 在 19:44:56 才开始 Native init，随后的注册、关闭后拒绝操作、重新初始化与关闭均实际成功 exit0。该次 CPU container 已被正常移除，历史 pip stdout 不可恢复，不补造日志。B 正在把固定依赖准备移到正式构建前，密封完整 wheel/model/image 输入，Serving 启动离线；原 110/120/180 秒预算与 HTTP/权限/模型断言保持。
- Root IndexBuild 与 Common 后继的旧行为真实 3 FAIL/0 PASS，其中原 launcher 预期 1 PASS/2 FAIL 不符并 exit1，保留真实 guard mismatch。JDK17 源码与真实测试均确认 ThreadPoolExecutor 的 TERMINATED 信号可在最后 Worker 的 Thread.run 返回之前发出。后继由 ManagedExecutorFactory 记录实际工厂线程，ExecutorDrain 在同一预算中等待 executor termination 与线程 join；超时/中断保留 owner，自线程关闭拒绝 self-join。IndexBuild 拥有唯一 executor，并原子封闭 admission/处理 pending，不再携带无人使用的 borrowed executor 字段。
- 同源 r4 严格 Java17、canonical style 均零错误；真实 SDK/JDBC drain 与原线程/query-state/transition 断言共 19 PASS/0 skip，零源码/依赖漂移、NativeLoaded=false。同源变更行覆盖 62/90=68.89%；QueryTimeout 默认工厂、SpeculativeExecutionScheduler、Context4参构造和部分暂停/恢复分支尚未执行，不隐去这些范围。新增真实 pending-close oracle正在验证；该单元未冻结/未 ABC 接入，普通 Maven/full Native 链仍待完成。原 style13失败保留，原断言未删改为成功。
- A 正规生成 Calcite 前端组件已推进至 73/75；selector 后继保留真实 SqlSnapshot/version/branch/tag/CHANGES operands，88 JUnit/0 skip、13 modules 与 17 prior 组件通过。生产 factory/dispatcher cutover、最终 source/snapshot 授权、两项剩余形式、source AI occurrence 与 DEFAULT 读写仍未贯通。
- C TTL source commit foundation8 私有真实 PG/SDK 六场景通过，包括 source pointer 实际提交、跨 lease expiry 的 COMMIT42501 回滚、旧 epoch/真实撤权/foreign backend/造高 epoch 拒绝；私有 53 migrations/maxV58，不转移到普通51/maxV56。正式 CREATE TABLE policy producer、query owner、factory/provider wiring、原20 TTL 与真实 Unknown commit 故障仍待完成。真实 factory 审计发现 catalog-type=jdbc 原先被忽略走 Hadoop，C 正在独立修复正式 SDK factory 与 SPI 依赖方向。

参考与推导：Java17 [ExecutorService](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/ExecutorService.html) 的终止契约描述任务完成；[Thread.join](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/Thread.html#join(long)) 等待实际线程终止。结合当前固定 JDK 源码与实际失败，Pangrova 的 owner 关闭采用两层 barrier。这是项目对资源退出边界的设计要求，不把文档中的一般任务完成等同于 Native/source tree 完整退出。

整体仍未完成：唯一生产 SQL cutover、完整异步 AI/Flight、durable DAG physical drain、CDC/Feature Store/评估真实执行、全部 static/E2E/smoke/大规模 Linux 性能等仍有未闭合项。不得据组件通过宣称全部完成或业界最优。


## 2026-10-08：普通 r20 与完整生产调用方组合

- Root build/physical-thread successor13 已冻结并进入私有 ABC source review，7 production 源、零合并冲突；最终同源 20 PASS/0 skip，变更覆盖 71/90=78.89%，完整 6,541 依赖输入验证零漂移。source-manifest SHA `cdbd1300e36bdde14c6ebf19948fa828e1e0375cf3a7b7235aac7310b9bdb314`。普通 Java r20 同步13源，6,892输入；完整39模块 test-compile PASS51.513s/0漂移，普通 Maven reactor 六类20测试 PASS/0 errors/0 skip/NativeLoaded=false。原 r19 报告 XML 在普通目标重新编译前独立归档。
- Root 最初只给 Serving 后继前置 normal-r20 Common48类，遗漏同步迁移的 QueryTimeout/Speculation 调用方；实际 Worker 已关闭 Native/Flight，但 Coordinator 旧 QueryTimeoutEnforcer.stop 把未 managed 的 executor 交给新公共 barrier，真实拒绝并留下未退出 child。该混合 producer 是验证输入错误，明确保留 failure，不能转移之前正常清理的资格。孤儿实际 identity/PID12466确认后 SIGTERM，实际 PID 消失；已非可 wait 的直接子进程，exitCode/waitpid 不可用，只记 FORCED_PROCESS_RETIREMENT，不声称正常 join/reap、SDK 或 source drain。Root 已补密封正常 r20 七 production 源对应完整12 class closure，与 Common48配对使用；其实际新 Serving/Worker 清理仍待最终后继实跑。
- Serving 依赖准备后继在前两轮实际 CPU 启动分别约4秒/3.79秒，原预算内 setup ready、原2组件通过；第一轮旧 Common cleanup0/0，第二轮混合 producer cleanup失败，二者范围分开。最终 paired producer 与普通 Maven/E2E prepare phase 接线验证尚未封闭，原 r22 closest 266NotRun 的结果不改变。
- Root 新 metadata-poller/module-owner 单元正在真实锁阻塞复证。首次 strict 因测试 import 与 checked SQLException 共两轮失败，记录保留；首 runtime4项中的两项 metadata-sync 没等到真实60秒生产 schedule，因为 sole fault-holder idle transaction 被 PostgreSQL提前结束。这是实际 fixture prerequisite 缺口，不算生产关闭反证；poller正常关闭1PASS、超时不抛1FAIL单独保留。后继仅在自己的持锁 transaction `SET LOCAL idle_in_transaction_session_timeout=0`，应用连接和数据库默认值不变，原60秒生产 schedule/5秒close预算不变；新的实际 baseline 正在运行。候选未冻结/未ABC。
- A 前端组件74/75：正规13module92JUnit0skip，原 CYCLE ClassCastException 反证保留，新的 typed SEARCH/CYCLE 节点与标准递归CTE列名保留；SEARCH/CYCLE执行绑定、最后完整query形式、生产factory/caller切换仍待完成。
- C 明确记录并修复验证脚本 Checkstyle `-o`误指旧unit造成 XML 覆写。原 XML 未封入旧manifest，无法证明字节恢复，旧 qualification 不改写；新 incident successor在独立目录重跑 exact source8/probe/dependency10输入并取得 strict/style/真实六场景0，新证据不推断旧seal仍有效。新 factory4、metadata adapter7真实 SDK/PG组件继续单独保留，canonical authenticated DDL/policy/scheduler/原20TTL/Unknown故障未贯通。

总体完成、完整 CI/ASAN/当前打包、E2E/smoke、异步 AI/Flight、CDC/Feature Store/评估与指定 Linux规模性能验收仍未完成。


## 2026-10-08：r24 当前统一近端通过，完整回归继续

- Root metadata owner successor5 已冻结并进入 ABC：两个 production、两个真实测试和现有 metadata 文档。原实现有效 before-r4 是真实 1 PASS/3 FAIL；早先 before-r3 的两个 metadata-sync 场景因持锁前置不足作废，独立补充也纠正了冻结 receipt 的旧措辞，不改写原 seal。最终 strict Java17/Checkstyle 零诊断，同源 7 PASS/0 skip，变更覆盖14/19=73.68%。真实关闭等待 metadata-sync/poller 线程、超时保留 owner 可重试；重复 leader election/loss/regain 复用唯一 cleanup executor。该组件没有验证实际 cleanup SQL 或完整 HA failover。
- 普通 Java r21 的39模块 production/test compilation 已通过；8份实际 Maven XML共27测试、零 failure/error/skip、源码零漂移、NativeLoaded=false。后续 r22 仅更新 CPU Serving resource，不伪称新的 Java runtime 资格。先前怀疑 JdbcInvalidationPoller$1 是 stale class，实际核对 fresh/normal 完整类名证明它是合成类；错误假设和纠正均保留，没有删除正确产物。
- Serving11 将真实 wheel/model/image 准备移入正常 Maven process-test-classes，并使部署离线启动；同源实际 Worker/CPU/原2 Native 组件/cleanup通过，broker/exporter均实际 wait/reap exit0。其请求仍未 admission，不能当作真实模型 SQL 执行。CLI2 修复 macOS CMake 默认 app-bundle 查找把 com.docker.backend 当 docker CLI 的根因；仅接受实际 PATH docker/docker.exe，并在任何执行前拒绝错误缓存路径。r23 失败前置完整归档，真实 owned backend PID19214及其子进程在定向SIGTERM后消失，由自身父进程完成回收；没有关闭全局 Docker/Rancher。r23未执行任何 Native CTest。
- CPU EOF1 用真实 ONNX 计算、实际计数和响应缓存之后关闭连接，原HTTP 200/replay/409/400/413断言保留。单个POST确实计算一次后发生未知完成；这只是 HTTP 故障组件，不替代 governed Task 的不重发/不发布断言。首次 container stop10s exit137是强制退出，明确不算正常 drain。graceful1 后继显式关闭监听并等待真实非daemon请求线程；实际半个POST在SIGTERM后完成计算/返回200，docker wait exit0且实际移除容器。两个后继各自冻结并按依赖顺序进入 ABC。
- r24 组装8项后继输入和正常完整生产 class/resource closure。configure/build均exit0、394自有object完整核对、零warning/源码漂移/依赖漂移；复用验证过的未变r22对象，实际重建模型契约生成器及其依赖的 VeloxPlanConverterTest TU/测试程序，不声称394全新重编。当前 registered closest r16真实268/268 PASS，117.36秒，setup/cleanup成功，测试前后8个二进制身份相同。当前完整Release已发现4,179项并正在执行；尚无当前完整通过、ASAN、打包或Java/JNI E2E资格。
- A 组件75/75形式已覆盖，production sole parser/caller cutover仍私有在完善；正常13模块110JUnit零skip不替代全部Coordinator调用方闭合。真实 CASE/COALESCE 的来源身份与生产Substrait binding组件通过，但不证明每源模型只执行一次。C canonical CREATE真实SDK/PG主矩阵已通过，仍在补不可变catalog handle与当前metadata revision/properties的最终提交fence，原20 TTL maintenance及whole-provider边界未闭合。B正式supervisor retry-close和JNI既有typed uncertain outcome投影正在独立实现，均未混入当前r24测试队列。

研究依据：CMake [CMAKE_FIND_APPBUNDLE](https://cmake.org/cmake/help/latest/variable/CMAKE_FIND_APPBUNDLE.html) 的 Darwin 默认 FIRST 与 [find_program](https://cmake.org/cmake/help/latest/command/find_program.html) 的缓存/查找范围解释真实 CLI 错选；Docker [container stop](https://docs.docker.com/reference/cli/docker/container/stop/) 的 signal 与超时强杀区分实际 exit137 和正常退出。Python [socketserver](https://docs.python.org/3.12/library/socketserver.html) 的线程关闭行为需结合当前实际 ThreadingHTTPServer daemon 属性验证，不能从一般基类默认值推断；已有错误假设保留。

整体仍未完成：真实唯一SQL→受治理模型Task→Native→Arrow结果、取消/未知完成/首错分类、durable DAG physical drain、全部metadata provider/TTL、CDC/Feature Store/评估、完整static/E2E/smoke与指定Linux规模性能验收均继续推进。不宣称“全部完成”或“业界最优”。


## 2026-10-08：模型调用、AI 优化和未知完成的一级依据复核

边界结论继续保持：数据库 SQL 内核实际调用受治理模型；独立 Serving 承担模型设备和执行，数据库承担精确模型绑定、查询准入、选择行、预算、取消和结果发布 fence。SQL 推理能力需要贯通唯一 Calcite→Substrait→Velox→Arrow 链。Agent 编排和累计 Usage Ledger 仍按用户范围排除。

Snowflake 当前 [Cortex AI Functions 文档](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) 明确提供 SQL 模型函数、独立权限要求和面向吞吐的批处理使用方式。这支持数据库模型调用边界，不能证明 Pangrova 已实现同等能力或同等性能。

新核对 Snowflake 作者 [Cortex AISQL 论文](https://arxiv.org/html/2511.07663v1) §5.1/§5.2：AI 谓词位置会影响模型调用成本，作者结合成本/选择率优化执行位置与顺序；其模型级联允许预算耗尽时采用 proxy 结果。Pangrova 的裁决是把 AI 成本纳入同一个优化器，同时必须先证明选择行、条件执行、调用来源与固定模型语义保持。论文中的 proxy 替代路径不符合本项目零 fallback、精确模型契约，不能直接移植，也不能沿用其性能数字。

| 一级依据 | Pangrova 模块落点 | 本次核对与验收边界 |
| --- | --- | --- |
| Snowflake SQL 模型函数与批处理 | Planner binding、模型 catalog、Worker 服务调用 | SQL 调用、模型权限和服务身份分别核对；当前真实 CPU serving 正控只属于组件 |
| AISQL AI 谓词成本/选择率 | Calcite 优化规则、同一 Native plan | 禁止把昂贵/带调用效果的函数一律下推或普通 CSE；原 CASE/COALESCE/source occurrence 断言和真实未选行零请求 oracle 都保留 |
| [RFC9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2) | AiHttpTransport、provider retry、Stage retry | 非幂等请求的自动重发需实际幂等语义或原请求未应用的证据；连接断开本身不能充当证据 |
| [gRPC retry 文档](https://grpc.io/docs/guides/retry/) | Worker task dispatch/control、独立服务 transport | transparent retry 的安全范围是服务应用未收到调用；错误状态不能被扩展解释为模型未执行 |

已实际复现真实 ONNX 计算完成后连接 EOF，且缓存 replay 可以出现 compute=1、POST=2。因此“不重复计算”不等于“不重发”；正式 Task 验收同时查实际 send 数、服务 invocation identity、未发布结果和最终 typed error。现有协议已有 PROVIDER_OUTCOME_UNCERTAIN 类；JNI 正在独立修复真实异常 cause 投影，不能以 toString/NATIVE_TASK_FAILED 抹去完成不确定性。

当前 exact r24 Release 全4,179 PASS，515.82秒；完整源码6,872项、8个二进制、15份证据冻结于 qualified-r24-release-archive，source/CP/binary无漂移。新 ASAN configure 已通过，匹配 ASAN 全新编译进行中。该 Release 资格不证明新异步 Flight inference 路径存在，不转移到后继 Java deployment owner、ASAN、打包或 SQL 模型 E2E。实际 Testcontainers1.21.3清理使用强杀路径的缺口另行修复；原 CLI SIGTERM/held-handler退出0只证明该组件，不借用为Java部署drain。


## 2026-10-08：主链收敛、验收口径与 r25 启动阻塞

用户原始14项验收中，Lance接入方式已有可核实的技术决策；其余13项最终验收仍未闭合。Native测试数量不能替代唯一SQL前端、模型调用、结果发布、维护恢复、Feature Store、评估、全仓门禁和指定规模性能验收。总体未完成，不宣称业界最优。

- 正常Java r24完整39模块test-compile通过；实际普通reactor 10类36项JUnit全部通过，failures/errors/skipped均0，源码6920项无漂移。包括分支发布、真实Iceberg provider、原有资源关闭断言及新增启动失败断言。对应 `/tmp/pangrova-joint-java-preview-20261008/retention-ordinary-r24-result.json`。不转移为Native、维护未知完成或完整质量门禁资格。
- Worker注册envelope修复在真实CPU Worker上验证：实际注册1060894字节成功；5242906字节边界200、超一字节413，普通heartbeat超原1MiB仍413，metadata超1MiB返回400。冻结5源无冲突进入ABC source review，尚未进入正常r24或当前Native r25运行输入。没有实际SQL Task推理资格。
- 原r24 Native Release 4179项通过的精确源码及二进制归档仍保留。r24 ASAN全新394个自身编译单元编译通过，但运行前置失败，因此ASAN全量没有通过。新r25只修fixture流式checksum、ASAN启动输入与窄JDK基线suppression，Native编译源未变；复用逐源及对象哈希核实的产物，不无理由重复编译或提前启动全部回归。
- r25 ASAN CMake configure通过；启动/清理两个前置测试均失败。流式checksum已绕过此前heap OOM，CPU serving真实请求成功；嵌套JNI注册子JVM实际晚加载ASAN并abort134，外层启动env不代表嵌套ProcessBuilder启动env已贯通。实际诊断保留于 `/var/folders/kl/t8p9lh1d0ql0_gw1kv3qlkc80000gn/T/pangrova-rerank-native-witness-3932126403396065590/actual-native-registration.log`。不禁用Native leak检测，不增加预算。
- r25验证脚本在收集结果时发现CMake configure后generated header不存在而退出；这是验证收集缺口，不能把此前二进制哈希转成配置后全产物无漂移声明。原脚本、configure和CTest failure日志完整保留，后继需执行对应生成target并重新校核。
- r25实际父broker/rerank退出正在LSan符号化扫描中；只读sample已定位DoLeakCheck→Suppress→SymbolizePC→FindModuleForAddress→mac映射扫描。supervisor owner保留，cleanup retry仍失败，尚未得到实际wait/reap。小JVM内存正负控制通过不能替代该真实链退出证明。样本 `/tmp/pangrova-r25-owned-broker-8897-exit-sample.txt`。

推进顺序收敛为：唯一Calcite正式调用方闭合→同一真实Worker的治理模型Task与Native错误/取消/结果链→稳定组合的完整回归。已有同输入合格结果复用；新研究与隔离组件仅在解除主链具体阻塞时继续，避免长期停在组件资格和证据整理。

## 2026-10-09：当前受控 worktree 的增量验证与未闭环项

本节只记录当前受控 Pangrova worktree 的当次输入。旧段落中的部分 `/tmp` 私有实验目录已经不在本机，不能作为当前构建或测试的可复验产物，也不能将旧 r24/r25 Native 资格转移到本次 Java 源码。

- 注册 envelope：协议 4 MiB inventory 与 1 MiB metadata 的实际序列化边界已统一到 Worker 发送、Coordinator 入口和专属 HTTP filter，普通路由仍保持原 1 MiB。当前协议 8 项、Worker 注册序列化 5 项、Coordinator filter 11 项、真实 Jetty/Jersey 资源 10 项均 PASS/0 error/0 skip。该结果仅覆盖注册链，不证明实际模型 Task。
- 模型 POST 安全：四条 OpenAI 兼容发送路径停止在传输失败或 HTTP 错误后自动重放 POST。共享传输把 EOF/未知网络结果分类为 `PROVIDER_OUTCOME_UNCERTAIN` 且不可自动重试；已完整收到请求后断连的真实 HTTP 用例只观察到一次 POST。Responses 的 503/EOF 共 5 项测试、Embedding/旧 Inference/Rerank 各自 503 及 Embedding/Rerank 正向结果共 5 项真实 HTTP 测试通过。新增 Embedding 正向夹具曾因 JSON 括号错误失败，已修复后按失败方法增量通过并重新完成该 5 项 class 资格；不把失败的旧运行改写为成功。相关原有聚焦测试 Embedding24、旧 Inference7、Rerank 函数3 均 PASS/0 error/0 skip。物理 Task 的 Native typed error、取消与结果发布 fence 尚无同链 E2E 证明，不能把上述 Java 组件测试升级为生产验收。
- 静态：SPI 与 Worker 直接 Checkstyle PASS。Worker SpotBugs 当前仍为 7 个 `EI_EXPOSE_REP/EI_EXPOSE_REP2`，均在 `WorkerComponentRefs`、`ActiveOutputBuffer`、`OutputBufferBackpressure`、`CatalogResourceResolver`；本次模型传输改动文件没有新告警。依赖链 Checkstyle 先在 Lance 32 个既有告警处停止；Coordinator 自身存在数百个既有 Checkstyle 告警。全链静态门禁未通过。
- 共享 SPI 新类别后的全 reactor `mvn -q -DskipTests install` PASS；这只证明当次 Java 编译/打包契约可组合，不等同测试、Native 重新打包或 E2E。
- 继续修复实际 Java 函数桥的一级类别丢失：`NativeFunctionBridge` 现在原样抛出 `AiInvocationException`，普通非 AI 回调错误仍使用既有 JNI 包装。真实 HTTP“请求已收到后断开”经 `AiEmbedFunction`、治理准入和正式函数桥的 1 项测试 PASS，恰好一次物理尝试、一次服务端接收、精确 `PROVIDER_OUTCOME_UNCERTAIN` 且不可重试；既有 `NativeFunctionBridgeTest` 25 项 PASS/0 error/0 skip。Native `JvmScalarFunction::consumeJavaException` 仍将类别降为 `toString()`，TaskLifecycle 生成 generic `NATIVE_TASK_FAILED`；这段尚未修复/验证，不能把 Java 桥通过误称为 JNI 端到端通过。
- 能力/资源合同继续对齐：SPI 默认与配置注册表的模型能力均改为已有 `RetryPolicy.noRetry()`，让每个 provider request 的逻辑 reservation 对应 1 次物理发送；该政策进入不可变模型 signature。配置注册表13项、治理调用18项及真实 HTTP5项均 PASS/0 error/0 skip。首次把断言放在未接入资源上限的裸 HTTP provider 上导致3 ERROR；已按 SPI 合同移到正式配置注册表并只重跑受影响 HTTP class，不削弱发送次数或错误类别断言。通用 admission 的4尝试基础设施测试仍独立保留，不能用它宣称这些正式 provider 会重试。
- 上述 SPI 能力政策调整后的直接 SPI/Worker Checkstyle 与全 reactor `-DskipTests install` 均再次 PASS；这是新输入的组合编译，仍非最终全量测试。
- 扩大测试：`mvn -pl pangrova-worker -am test` 在 observability 依赖模块停住，251 项中 249 PASS、2 ERROR、0 skip；两个错误都来自 Docker daemon 不可用的真实 PostgreSQL Testcontainers。Worker 后续模块未执行，不能计为全模块通过。Planner 的真实 Testcontainers lineage 集成同样因 Docker 不可用未通过；没有禁用或削弱测试。

后续滚动顺序：先贯通 Worker 模型异常到 Native `ExecutionError` 的精确类别、不可重试的物理 Task 终态和零结果发布；再闭合唯一 Calcite 生产调用方、其他正式数据/服务链与各自增量验证。所有功能完成后按 `build-pipeline.md` 执行一次同源码全量重跑，包括匹配 Native/ASan、打包、Java 全回归、E2E/smoke 与指定 Linux 分布式性能；当前不得声明生产级或业界最优。

### 2026-10-09：Native AI 错误类别增量闭合

- JNI 回调边界现在把 `AiInvocationException` 通过 Worker 的正式 `NativeFunctionBridge` 投影为有界的 canonical `ExecutionError`，在抛给 Velox 前按任务 executor handle 记录；Native 终态沿用 first-failure 记录。未知结果强制不可重试，普通非 AI 回调仍走原通用错误。
- Java 函数桥 27 项 PASS/0 error/0 skip，包含 100000 字符 provider 错误消息的有界投影；Native Release 编译和相邻 19 项 CTest PASS/0 skip，包含 `PROVIDER_OUTCOME_UNCERTAIN` 的真实 TaskLifecycle→TaskReport 序列化测试。Worker Checkstyle PASS。首次尝试打包 JNI 注册测试被 Maven 默认 `native-required` 标签排除，未运行；显式纳入后，空描述符夹具被真实 Native 契约拒绝，属于测试输入错误。改用正式 14 个 AI 函数的生产注册后，打包 JNI 初始化、17 个 descriptor 注册、关闭 1 项 PASS/0 error/0 skip。上述失败和未运行均保留事实，不转作通过。
- 后续精确修改只增加 JNI 对错误枚举与未知字段的 canonical 校验；同源 ASan 增量构建与相邻 19 项 CTest 现已 PASS/0 skip。最终 Release JNI 重新打包，显式纳入 `native-required` 标签的正式 Worker 注册测试再次 1 PASS/0 error/0 skip；`git diff --check` 与规则布局检查 PASS。对应的真实 SQL Task 内 Java 异常、Native FAILED、Flight 结果与 Coordinator 决策仍未同链触发；结果发布 fence、任务重试及 Docker/独立 Serving 端到端未验收。当前 Rancher Desktop Docker socket 仍不可连接，完整 Testcontainers 链不能运行。全量回归尚未启动，不能把组件资格转为生产资格。

### 2026-10-09：Worker 滚动验证与 Flight 失败终态修复

- 本机 Rancher Desktop Docker 后续已能连接；此前“不可连接”只描述当时状态。实际 PostgreSQL Testcontainers 的 `JdbcAuditStoreTest` 14 项、`JdbcQueryEventListenerTest` 14 项均通过，未跳过。为排查启动故障临时关闭了本机 Rancher Kubernetes；该本机设置必须在验证结束时恢复原值。
- 首轮 Worker 模块 1401 项中 10 failure、29 error、0 skipped，集中于 10 类。增量根因修复包括：写入测试的精确两列 descriptor、清理失败时保留会话并实际重试 abort/close、Task 去重夹具的 canonical grant、结果 Task 在 Flight 消费前保持 RUNNING 的正确时序、缓冲 query grant 使用 `queryAllocationsSnapshot()`、失败报告使用 Worker 对账来源且保守零完成量。局部复测：写入失败类、AI 函数工厂、Task 去重、结果与缓冲类均通过。
- 低内存实测揭示独立生产缺陷：Flight 页分配报 `MEM_CAP_EXCEEDED`，但异常越过 Arrow 读取器，缓冲析构后 Native Task 报 FINISHED。Native 现将 Arrow 失败状态与 C++ 异常在读取边界映射为 `RESULT_STREAM_FAILED`，于输出缓冲释放前记录到同一 executor 的 canonical 失败；实际 JNI/Flight 低内存测试现为 FAILED、原始内存错误可见、完成量零且 owner/budget 释放。直接构造 Runner 的 Native 测试不具备注册表 handle，新回调只在真实注册 handle 存在时安装；此前 36 项相邻 CTest 的 7 项失败保留，修正后 `ResultBatchStreamReaderTest` 12/12 PASS。
- 最终重新打包 Release Native 库后的 Worker 全套本轮新生成 211 份 surefire XML，共 1397 项、0 failure、0 error、0 skipped。另有 1 份旧 XML 未被本轮更新，不计入本轮。Native Release 增量编译通过；匹配源码的 ASan 增量构建与相邻 36 项 CTest 全部通过、零跳过。该 Worker 模块资格不等于 E2E、完整 Java reactor、全 Native 回归、Linux 规模性能或总体生产验收。

### 2026-10-09：SQL 冒烟中的缓存文件 ID 根因

- 首次 `ActivationSmokeE2ETest` 的 `SELECT` Iceberg 数据，Native `CachedBufferedInput` 在 coalesced load 对 `StringIdMap::kNoId`（`2^64−1`）增持时抛 `INVALID_STATE`；客户端随后等待 Flight 60 秒才超时。直接根因是 Pangrova connector 给文件与目录都传了空 `StringIdLease`。已按同版本 Velox FileHandle 的 `fileIds()` 路径/目录身份持有真实 lease，Native Release 增量编译和打包通过。只重跑失败的 `smokeQueryAssertion` 1/1 PASS：实际 `SELECT 1`、Iceberg CREATE/INSERT/SELECT/DROP 均贯通，无该缓存异常。
- 首次失败运行在客户端取消时还卡在 `nativeCancelTask`，SIGTERM 清理未退出；已保存 `/tmp/pangrova-smoke-hung-teardown-20261009.jstack`，随后对该失败进程强制终止，不能声称正常退出。修复后上述单方法运行正常退出，但故障路径取消是否已通用闭合仍待单独验证。真实模型 SQL E2E 仍在进行中。

### 2026-10-09：真实模型 SQL 与生成值产物的滚动贯通

- `OllamaAiSqlE2ETest#aiCompleteRunsThroughTheRealDistributedSqlPipeline` 1/1 PASS，真实独立 Ollama serving 收到恰好一次请求，公共 SQL 经 Coordinator、Worker、JNI、Native、Flight 返回非空模型输出。最初其余 12 方法 7 PASS、3 failure、2 error；两项 ASYNC 生成值先因计划字段名与物理 Project 不一致失败，三项后续 SQL 遇到并发查询配额 16/16，不能把前述结果转作全类通过。
- 生成值构建依次暴露并修复：Planner sink 输入使用真实 `proj_1` 字段；Native 接受 Java 正式 `VECTOR(dimension)` 规范且拒绝旧测试的 `VECTOR(dimension, COSINE)` 伪类型；Velox 对 Native 存储上限 65536 与逻辑类型上限 65535 采用后者；Native 持久化 object ID 改为带域名的定长 SHA-256，避免长文件路径令 PostgreSQL `object_id VARCHAR(255)` 插入失败。相关 Native Release 61 项、定长对象测试 1 项、匹配 ASan 73 项和定长对象测试 1 项均 PASS。
- 发布后读回原来错误地比较完整 `DataSplit` 字节，查询专有的扫描谓词使合法的同一文件范围被拒绝。现保留构建 split 完整字节及 SHA-256 校验，由 Coordinator 比较稳定的 connector/source/snapshot/object/location/range 身份；Metadata 只负责返回有校验的原始字节，不依赖 Protobuf/Planner。发布清单 `rowCount` 已按源对象去重，读绑定取消错误的偶数要求和再次除二。局部 PostgreSQL 存储测试 13/13、Coordinator 范围绑定 6/6、终态所有权 6/6、生成值发布集成 1/1 PASS。
- 修复后 `OllamaAiSqlE2ETest#asyncGeneratedValuesFollowTheRealCommittedWriteLifecycle` 1/1 PASS：真实 SQL 写入触发 durable commit、Native 模型嵌入生成两件校验产物、原子发布 `objectCount=2,rowCount=1`，后续公共 SQL 通过 Flight 读取。另四个先前失败方法在同一运行 4/4 PASS：NULL 来源发布/NULL 读回且无额外 provider 调用、独立同表达式两次调用、CASE 选中行与未知模型拒绝。此前 429 与终态物理清理滞留有关；新增调度操作退出事件触发即时清理，并继续保留有界后备重试；报告发布操作退出也触发同一通知，精确所有权回归 7/7 与终态所有权 6/6 PASS。全类、全 E2E、全 Reactor、全 Native 和 Linux 规模/性能尚未一体化重跑，不宣称生产级完成。

### 2026-10-09：受治理 FILE SQL 增量贯通

- 首次真实 `FILE_FROM_BYTES` SQL 在 `CAST(NULL AS VARBINARY)` 被 Parser 拒绝。补齐正式 `VARBINARY` 列定义与 CAST 语法、AST 类型映射；Parser 相邻 7 项 PASS。随后 Planner 发现正式 Worker FILE 回调未进入 Native 默认能力集合；增加已核实的 `file_from_bytes` 与 `file_from_url`，能力/计划校验相邻测试 PASS。
- 真正执行时任务媒体配额为零，Worker 正确拒绝发布。Coordinator 现提供至多 64 MiB（受任务内存限制）的 FILE 基线额度，汇总模型算子时不丢失；Worker 在对象存储发送前按实际暂存字节向精确任务累计扣减，超额不会发布对象。Coordinator 9 项、Worker BlobStore 13 项、FILE/HTTPS 9 项相邻测试 PASS。
- 首次成功发布后，Flight 返回的 FILE 嵌套结构名仍是 `field_0...field_4`。Native 现在分开持有内部物理序列化 schema 和 SQL 根输出 DFS 名称，在结果流解码后仅重绑定 Arrow 元数据；Release 相邻 21/21 PASS，匹配 ASan 构建完成。ASan 与并行 E2E 争用资源时首次 20/21，一项 2 秒停止时限在 5 秒后失败；该失败单独重现时 PASS，E2E 结束后整组独占重跑 21/21 PASS，不能把首次失败抹掉。
- 首个 FILE E2E 方法 1/1 PASS：NULL 行保留，非 NULL 对象经 HTTPS/SSE S3 持久化、SHA-256/HEAD/GET 校验及 Flight 命名字段返回。其余 5 项首次 4 PASS、1 FAIL：非法 MIME 的真实 Worker/Coordinator 终态含精确原因，但客户端在 Flight 失败时只给通用错误。E2E 客户端现在从正式 HTTP 查询状态读取终态错误并保留原始传输异常；只重跑失败方法 1/1 PASS。六个方法分别通过，尚未同一批全类重跑；S3/FILE 外的全 E2E 和全量生产回归仍待最终滚动验收。
- 新输入的 Worker Checkstyle PASS；Parser/Planner Checkstyle 在 Coordinator 前执行通过。Coordinator 直接 Checkstyle 被 593 项存量告警阻断，E2E 直接 Checkstyle 被 12 项既有 TestDataSetup/TestConnectorHandleFactory/SqlGenerators 告警阻断；此次改动文件的 XML 均无新增告警。静态全门禁不能记为 PASS。

### 2026-10-11：VECTOR SQL 与交换 schema 滚动验证

- `AiSqlReadWriteE2ETest` 首轮真实端到端 12 项中 9 PASS、3 FAIL。排序距离查询暴露跨阶段 Arrow 实际列 `proj_0` 与消费者原别名 `dist` 不一致。常规 live DAG 原先未采用已存在的持久化交换契约冻结；现消费者在派发前按已派发生产者 TaskRequest 的实际 Substrait 计划绑定 schema，校验生产者分区一致、交换分布/身份及消费者类型，保留根 SQL 展示别名，不放宽 Native Arrow 校验。Planner 对应 8 项单测 PASS；原失败距离方法真实 E2E 复测 PASS。
- 索引检索扫描在表声明 schema 后追加四个来源行定位字段，原生成值读取器错误要求与表 schema 字节完全一致。现仅准许固定名称、次序、必填类型的四字段精确后缀，表字段和根结构仍逐项严格匹配；真实 Iceberg/Postgres 集成 3 项 PASS，原失败两个索引查询真实 E2E 复测 PASS。
- `CREATE INDEX` 是异步提交，两个索引重建用例原先在首次生成 READY 前立即 REBUILD，真实服务按状态机返回 400。测试现在等待真实 `SHOW INDEXES` 的 READY 后再提交重建；`AiSqlReadWriteE2ETest` 两个原失败索引方法及距离方法同次 3/3 PASS。其余 9 项沿用首轮通过证据，整类同一 JVM 尚待最终全量重跑。
- `VectorSearchSqlE2ETest` 首轮 10 项 9 PASS、1 FAIL，唯一失败同为重建时序；精确修复后仅该方法复测 1/1 PASS。10 项分别通过，整类同一 JVM 留待最终回归。
- `AiConcurrentLoadE2ETest` 首次准备阶段在固定 3 分钟预算内拉取真实 `tinyllama` 超时；5 个方法均未执行。该次为外部模型部署前置失败，重试结果另记，不扩大预算、不计入通过。
- 重试的真实模型部署在原预算内完成；并发类 5 项中大数据 2 PASS、并发 3 FAIL（两项 429、并行查询断言错误），根因为连接器元数据操作退出只减少租约计数、没有通知终态清理重试，快速查询积累至租户 16 个并发槽。`TaskIdentityRegistry` 的连接器操作退出现在通知 `CoordinatorTaskTracker`，仅当同一查询所有租约退出才触发既有终态清理监听；相邻所有权单测 PASS。只复测原失败 3 项真实 E2E，3/3 PASS；包括五路并行搜索、读写交错和索引存在时并发查询。千行 Top-K 与 128 维两项沿用首次通过证据；整类同一 JVM 留待最终回归。

以上为增量结果，不是完整 E2E、Native/ASan、Java 全量、静态门禁、独立进程分布式或 Linux 规模性能验收；仍不得宣称生产级全部完成。

### 2026-10-11：协调层依赖的 SQL 类型与表构造滚动验证

- 协调层首轮模块测试先在 `pangrova-test-common` 的结构化日志属性测试失败：`RemoteTaskStatusReporter` 八处日志将 `event=` 放在调用下一行，而该属性扫描器只检查调用首行。关键路径日志保留原字段并改为首行带事件标识；定向属性测试通过。首轮失败仍保留，不计协调层通过。
- 第二轮在 Planner 依赖处停止：1,329 项中 5 项 ERROR。Native 实际 inventory 报告 `Nested`，且已注册十进制 `round`，Planner 静态默认能力和 SDK `NestedList`/`NestedStruct` 对应关系缺失，导致四项计划过早拒绝。现映射到 Native `Nested`，对子表达式继续递归验证，默认准入补齐 `round`；十进制双参数 `ROUND` 的位置参数显式转换为 Native 声明的 `INTEGER`。原协议类 22 项、能力注册类 11 项、验证器 13 项均通过。
- 第五项 ERROR 为正式 SQL Parser 不接受 `FROM (VALUES …) AS t(X,Y)`。按 [Trino VALUES 文档](https://trino.io/docs/current/sql/values.html)的匿名表及列别名语义，保留原 `VALUES` 行集合和精确列名；依据 [Calcite Values 关系定义](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/core/Values.html)直接构造 Calcite `VALUES` 节点，而非经 `UNION ALL` 改写。Parser 对等宽与别名宽度拒绝，原 snapshot 所有权类 2 项、Parser 类 9 项以及 Calcite 形态聚焦测试通过。第一次负例测试预期错误顺序不符（实际先拒绝行宽）已更正并增加单独别名宽度负例。上述已通过的精确检查不等于完整 Planner/Coordinator 模块资格。
- [Substrait 嵌套构造表达式规范](https://substrait.io/expressions/specialized_record_expressions/)要求构造元素的类型准确，支持继续递归校验而非跳过子表达式；[Trino 数学函数文档](https://trino.io/docs/current/functions/math.html)列出 `round(x,d)` 形式，物理 `INTEGER` 位置类型以 Pangrova Native 注册表和原协议测试为准。`ROUND` 实际 SQL→Native E2E、完整协调层模块和更大回归仍待验证。
- 完整协调层依赖链通过 Planner、Governance 和 Metadata 后在 Lance Connector 停止：17 项 7 ERROR，其中六项未传匹配 Native `pangrova_lance_fixture_tool`，一项旧身份测试仍传已禁止的 connector JDBC 参数。Lance 测试夹具改用共享元数据权威接口，匹配当前 `build-joint` 的正式 fixture tool 明确传入；Lance 全模块六类、17 项现 17 PASS/0 error/0 skip。随后协调层及其依赖 `-DskipTests install` 已通过，仅代表当前 Java 契约编译安装；协调层自身测试正在进行，之前两轮未完成的依赖链不计通过。

### 2026-10-11：Coordinator 自身模块按失败类别增量收敛

- 首次 Coordinator 单模块执行到 2,596 项时有 15 failure、80 error、0 skipped；约 45 项为旧全启动夹具缺内部服务认证，10 项为测试把真实 Native inventory 已有能力重复追加。该轮失败是诊断输入，不计完整模块通过。旧夹具现仅在测试侧补真实 PostgreSQL 和内部认证；缺认证负例仍按生产拒绝策略验证。重复能力不再追加。相关生成值构建 8 项及 HNSW 内存选择 1 项定向通过。
- 八个启动/角色/观测/超时测试类的组合复测 33 项中 32 PASS、1 ERROR；唯一 ERROR 暴露 QUERY-only 角色有 indexing 标志时仍构造没有本地提交处理器的生成值服务，并误要求 `baseIndexPath`。生产接线现仅在本地 IndexingBootstrap 可注册 durable commit processor 时创建服务；角色类定向 6/6 PASS。其他 32 项沿用前轮通过证据，不把两轮拼作单次全模块通过。
- 两个 REST 状态测试类和 Web UI 测试原用 null QueryDispatcher 构造 QueryManager，现使用完整 deny-all 组件夹具；3 类共 26 项 PASS。授权关系测试改用规范可解析 Substrait plan 且显式 nullability；原失败方法定向 PASS。Top-K 绑定测试改用当前表达式式 FETCH 和显式 nullability；1/1 PASS。推测执行的两类反射审计改为检查实际 `SpeculativeTaskReportCompletion` 接线，共 21 项 PASS。
- 推测执行并发测试暴露两条真实生命周期问题：同一 exact drain receipt 在两个清理路径并发到达时重复 retirement 报错；查询逻辑完成时过早清除 winner election，可使两个并发完成都采集写入片段。现 retirement 在同一 tracker 上串行并接受相同已压缩回执的重复调用；winner election 改在实际 query issuance retirement 完成后清除。测试夹具补齐正式 cancel retrier、drain listener、Native 终态回执和有界错误身份，未把失败的取消 RPC 当成物理释放。推测执行整类仍在定向复测，不能计通过。
- `git diff --check`、规则布局检查、E2E truthfulness gate 当次通过。Coordinator 全模块、完整 Java reactor、Native/ASan 全量、全 E2E、CI 静态/发布门禁及 Linux 规模测试尚未通过，仍不具备生产级最终验收。

### 2026-10-11：Coordinator 失败类滚动复测与剩余授权缺口

- 租户签名 3 项、分区确认 3 项、原生数据集准备 2 项、能力放置诊断 1 项、周期性推测调度 3 项分别定向通过。原生数据集 HTTP 测试 Worker 现在把 Coordinator 已持久化发行的 invocation 交给实际 Native SDK 夹具执行，不再由另一夹具目录二次发行导致回执身份不一致。
- 原首次失败类别（除索引统计）组合复测 229 项，228 PASS、1 ERROR。唯一 ERROR 是并发写入时查询整体退休与 loser 单任务 drain 清理可交错压缩同一请求；两条退休路径现用同一 tracker 锁串行。修复后推测执行类 14 项及相邻 drain、查询清理、提交终态竞态类共 14 项均 PASS、0 ERROR、0 SKIP。组合失败轮不得计为完整通过轮。
- 索引后台维护暴露安全与可用性缺口：`IndexCommitHandler`、`IndexGenerationRefresher` 仍从未绑定的 `ConnectorAccessorResolver` 读取统计、快照和构建文件。`IndexCommitHandlerStatisticsTest` 原全类 10 项有 4 个失败。当前先让 `AccessDeniedException` 穿过统计 best-effort 捕获，以阻止无权限时错误确认耐久提交事件；安全负例 1 项与提交日志相邻 9 项均 PASS、0 ERROR、0 SKIP。索引主体/精确作用域持久化、当前授权复核和 Connector session 绑定仍需实现及 E2E 验证。[Trino 文件授权规则](https://trino.io/docs/current/security/file-system-access-control.html)与 [OPA 当前身份/动作授权](https://trino.io/docs/current/security/opa-access-control.html)支持按操作与主体重新判权，不支持用合成系统主体代替。
- 本轮最新 `git diff --check`、`.pangrova/setup.sh --check`、E2E truthfulness gate PASS；这不是最终静态分析/全量回归。Coordinator 全模块、全 reactor、Native/ASan 全量、全 E2E 与 Linux 规模/故障验证仍待完成。
- `ExpressionCoverageTest$MathFunctions#roundFunction` 的真实 SQL→Native E2E 本轮启动，但测试共用 Native 启动在拉取 Ollama `tinyllama:latest` 时达到固定 3 分钟部署预算，报告 1 ERROR、0 SQL 方法执行；不能计作 ROUND 功能失败或通过。该类非 AI SQL 也依赖通用 AI/FILE 模型部署，且隔离容器未保留模型下载缓存，是本轮端到端进度慢的直接环境成本；后续需要有校验 digest 的可复用真实模型镜像/缓存或合理拆分启动生命周期，不能用 fake provider 或跳过断言冒充通过。
- 测试专用 Ollama 容器现在把模型 blobs 缓存在模块 `target/ollama-models`，仍启动独立真实容器、拉取并读取当前模型 digest。新输入下模型准备在约 45 秒完成；`ExpressionCoverageTest$MathFunctions#roundFunction` 的嵌套报告为 1 PASS、0 ERROR、0 SKIP，正式 SQL→Native 路径实际执行。非 AI SQL 的通用模型启动耦合仍是独立效率改进事项，不把缓存视为 AI 能力语义验收。
- 新增 `ExpressionCoverageTest$InlineValues#namedValuesReachNativeWithoutAUnionRewrite`，真实 SQL→Parser→Planner→Native→结果链 1 PASS、0 ERROR、0 SKIP，确认带列别名的内联关系返回两行有序值；与 `ROUND` 两项均为定向证据，不替代 E2E 全量回归。

### 2026-10-11：表达式 E2E 扩面后的根因修复

- 表达式此前未覆盖的 20 项一次增量扩面发现 9 ERROR、11 PASS；失败分成 `EXTRACT(YEAR)` 能力漏报 1、混合整数/DOUBLE 减法 2、聚合 `FILTER` 交换 schema 5、`COALESCE` 的 Velox→Arrow 空值计数不一致 1。该轮不能计全类通过。
- `year(DATE)` 有 [Velox 官方签名](https://facebookincubator.github.io/velox/functions/presto/datetime.html)支持，Planner 默认能力清单补报；清单单元 11 项及原失败 `extractYear` 真实 E2E 1 项通过。算术混合类型由 Planner 显式把整数参数提升到 DOUBLE，保持 Velox 精确重载与 SQL 数值语义；单元 1 项、原两条 SQL E2E 2 项通过。
- 聚合 `FILTER` 的输入交换差异定位到 `IS TRUE` 改写：Calcite/SQL 给非空 BOOL，通用 `AND/OR` 推断成可空 BOOL；[PostgreSQL 布尔语义](https://www.postgresql.org/docs/current/functions-comparison.html)确认 `IS TRUE/FALSE` 对 NULL 也返回非 NULL。改写输出以显式 required BOOL 保留真值表，Planner 相邻 8 项及 `COUNT FILTER` 4 项 SQL E2E 通过。最后 `SUM FILTER` 经精确 Native 诊断发现 `eq(BIGINT,INTEGER)`，Planner 显式提升整数比较参数后单元 1 项及该 SQL E2E 1 项通过。上述五项按问题修复后分组通过，最终全量仍需一次重跑。
- `COALESCE(l_comment,'no comment')` 的 Native `PartitionedOutput` 报 Arrow `null_count=2` 而实际位图为 0，真实 E2E 仍失败；这与聚合/Planner 问题不同，需 Native 序列化边界复现、修复及 ASan 回归。索引后台维护授权正向链路也仍未闭合。

### 2026-10-11：Arrow 空值计数边界的增量验证

- 回归测试构造全有效 VARCHAR 位图但保留 Velox 过期缓存计数 2，修复前稳定复现 Arrow `null_count value (2) doesn't match actual number of nulls in array (0)`。Velox `exportValidityBitmap` 在未改变行集合时复用缓存计数；Pangrova 的 Arrow C Data 导出边界现对所有导出子数组依据实际有效位图重算元数据，再交 Arrow 导入，不修改共享 Velox 向量或放宽结果校验。Native Release 新回归及现有序列化对等测试 253/253 PASS；重新打包 JNI 后原失败 `COALESCE` SQL E2E 1/1 PASS。
- ASan 同源增量构建通过；开启 LeakSanitizer 的 253 项在全部断言通过后仍以 11 allocations、52,318 bytes 的 macOS 系统/未知模块泄漏报告退出 1，不能计完整 ASan PASS。单独关闭 leak 扫描的 AddressSanitizer 内存访问复测 253/253 PASS，仅证明该范围未出现 ASan 访问错误，不抹去前述 leak-gate 失败。完整表达式 E2E 组合复测仍在运行；索引授权和最终全量门禁未通过。
- 完整 `ExpressionCoverageTest` 首次组合复测 24 项中 23 PASS、1 ERROR：先前新增的 DOUBLE 算术提升误覆盖 `ROUND(x, digits)`，把原整数位数参数变成 DOUBLE。Planner 现将提升限定为普通二元算术，同时对双参数 `ROUND` 的位数统一显式转换为 Native 声明的 INTEGER；协议类 23/23 PASS。该失败轮不计表达式类通过，修复后的组合 E2E 尚待重跑。
- 修复后原失败 `ROUND` SQL E2E 1/1 PASS，随后同源码完整 `ExpressionCoverageTest` 第二轮组合复测 24/24 PASS、0 failure、0 error、0 skip，包含聚合 `FILTER`、`COALESCE`、日期、字符串、内联 `VALUES` 与混合算术。`git diff --check`、规则布局、E2E truthfulness gate 再次 PASS。LeakSanitizer 无关原生字面量测试也在自身断言 PASS 后报告 52,590 bytes/13 allocations（同类 macOS 系统/未知栈），证实 leak-gate 失败并非该 Arrow 测试独有；不能据此声明完整 ASan 合格。

### 2026-10-11：内置日期时间函数扩面

- 首轮 `BuiltinFunctionsE2ETest` 14 项有 11 PASS、3 failure，失败均属日期时间：`month/day/hour/minute` 的 Planner 默认能力漏报 2 项，`INTERVAL DAY` 字面量未实现 Java→Substrait→Native 转换 1 项。其他内置字符串、数学、条件组在该轮通过，但该轮不计整个类通过。[Velox Presto 日期函数](https://facebookincubator.github.io/velox/functions/presto/datetime.html)逐项列出四个提取函数及 `DATE + INTERVAL`；Native 本机对应函数注册也已核对。
- Planner 能力单元 11/11 PASS。Calcite 以精确毫秒存储 DAY-TIME interval，Planner 现按 [Substrait IntervalDayToSecond](https://substrait.io/types/type_classes/)日/秒/子秒结构序列化，Native 按精度与 int64 溢出检查还原到 Velox `INTERVAL DAY TO SECOND` 毫秒值，非整毫秒输入明确拒绝。Planner 协议类 24/24 与 Native 字面量/精确算术相邻 35/35 PASS；Native JNI 已重新打包，匹配 Planner/Worker 依赖已安装。原失败日期组 E2E 正在增量复测，尚不能计为通过。
- 日期组首次复测仍显示原三条错误，诊断发现本地 Maven 仓库的 Planner JAR 停留在 17:13，Worker 的 `-am` 依赖链不包含 Planner；该运行未测试新 Planner，不能算代码修复失败。单独安装同源 Planner 后原失败 `DateTimeFunctions` 3/3 E2E PASS，完整内置函数类正在组合重跑。
- 同源完整 `BuiltinFunctionsE2ETest` 第二轮组合复测 14/14 PASS、0 failure、0 error、0 skip。另按本仓 Substrait proto 对 deprecated `microseconds` 与新版 `precision+subseconds` 两种 wire 表示的要求，Native 字面量转换增补旧字段读取；该兼容分支独立组件测试及无 warning 构建仍在复测。完整 E2E 结果发生在该兼容分支打包之前，只证明 SQL 产生的新版表示链路。
- 旧版微秒字段通过 protobuf reflection 在测试中构造，避免调用标记 deprecated 的 setter 引发构建警告；新旧字段与非整毫秒拒绝的 Native 相邻 35/35 PASS、0 warning。匹配 ASan 编译及关闭 leak 扫描的内存访问验证 35/35 PASS；此前 LeakSanitizer gate 仍失败，未被此结果覆盖。更新 Native JNI 已重新打包并安装 Worker；随后 `ArrowBatchSerdeE2ETest` 整类 6/6 PASS、0 failure、0 error、0 skip，覆盖扫描、聚合、连接和结果传输的现有断言。`git diff --check`、规则布局及 E2E truthfulness gate 再次 PASS。
- `BasicReadPathTest` 完整组合复测 19/19 PASS、0 failure、0 error、0 skip，覆盖投影、8 项过滤、排序、LIMIT/OFFSET、DISTINCT 及全表扫描。下一批聚合与连接 E2E 正在增量扩面；此处不把已经通过的表达式/内置函数/基础读取测试重复计成新的验证。
- 聚合与连接组合扩面 23 项中 21 PASS、2 ERROR；两条三表连接的 Stage 1 在 Planner 拒绝 `StandardsCompliantMergeJoin`。Native 已有正式 `MergeJoinRel` 转换和执行测试；Planner 默认关系能力及 SDK 关系名映射漏掉官方 `MergeJoin` 和本仓仅修复 SDK 非对称 schema 的包装类。能力与映射已补齐，Planner 注册、验证和 Join 转换相邻 54/54 PASS。组合失败轮不能计通过；修复后的原失败 SQL 正在准备重新 E2E。失败清理日志还出现 dispatch 未退出抑制异常，待独立检查，不掩盖原始关系拒绝。
- 关系准入修复后的两条三表 SQL 已到 Native，但正式 Substrait SDK 0.95 对等值 MergeJoin 同时输出新版 `keys` 和 deprecated `left_keys/right_keys`，本仓 Native 严格拒绝旧字段；这不是 Native 缺少 MergeJoin 算子。现有 `SubstraitProtoNormalizer` 序列化边界增加校验：只有旧字段逐项与 canonical key 一致时才剔除旧字段；不一致或缺新版键仍拒绝。真实 SDK 的非对称输入 MergeJoin 串行化单元 1/1 PASS，检查输出仅保留新版键；更广泛 Planner 单元与两条原失败 E2E 正在复测。
- 更广泛 Planner 序列化、Hash/Sort-Merge Join 转换、能力与验证五类共 64/64 PASS；Native MergeJoin 邻近执行/排序 145/145 PASS。重新安装 Planner 后两条原失败的三表 SQL 2/2 PASS、0 failure、0 error、0 skip。连接整类组合复测仍在运行；原失败轮的调度清理抑制异常不再出现在已成功的这两条查询中，失败路径自身尚不能据此宣布完成。
- `JoinOperationTest` 修复后完整组合复测 10/10 PASS、0 failure、0 error、0 skip，含内、左、右、全外、交叉与三表连接；`AggregationTest` 先前扩面组合轮 13/13 PASS。两类合计 23 项现各自拥有通过的组合轮，但不等于完整 E2E 模块通过。失败路径中 dispatch 清理竞态仍需独立验证。
- 窗口 10、CTE 7、子查询与集合运算 10、分组集 3，四类同批 E2E 共 30/30 PASS、0 failure、0 error、0 skip。Join 键归一化进一步限制为 legacy 字段逐项等于 canonical 且 canonical comparison 为普通 EQ；真实 SDK 生成计划的正例及冲突字段负例均通过。该微调后的最终三表 SQL 正在增量验证；本节各测试只代表对应功能域，不算 E2E 模块总通过。

- 上述严格 EQ 归一化后的三表 Join 最终真实 SQL→Native 单方法报告 1/1 PASS、0 failure/error/skip；`/tmp/pangrova-merge-final-e2e-20261011.log` 与对应 Surefire XML 保留。该结果仅封闭此增量边界，不替代整类 Join 或全 E2E 新一轮验收。
- 同一工作区再次执行 `.pangrova/setup.sh --check`、`git diff --check`、`.pangrova/scripts/check-e2e-truthfulness.sh`，全部 PASS。Planner 所属 reactor 的 `full-regression,coverage test` 已启动，结果待记；未将进行中的运行计为通过。
- 索引后台授权设计复核：`DeferredIndexAuthorityResolver` 与 `ArtifactBuildStore` 已让 Native 替代 generation 从持久化 dispatch 读取原始主体和精确原始作用域；普通 data-skipping generation 的 `IndexDescriptor` 仍只有 tenant，无主体/作用域，`IndexCommitHandler` 和 `IndexGenerationRefresher` 仍对未绑定 accessor 调用。现有 [Trino OPA 授权](https://trino.io/docs/current/security/opa-access-control.html)分别处理主体、表/列动作及行过滤/列掩码；[Databricks 物化视图权限](https://docs.databricks.com/gcp/en/ldp/dbsql/materialized-configure)明确刷新与所有者/刷新权限关系。这些实践支持显式持久主体和当前策略复核；不能由租户 ID 或临时系统身份推导授权。正向生产链路仍未实现，索引维护与全局生产门禁保持未通过。
- Planner 所属 reactor 首轮 `full-regression,coverage test` 实际到 Planner 1,361 项，0 failure、3 ERROR、0 skip；三项是同一个 Pangrova ANTLR 前端拒绝 `UNNEST(ARRAY[...])`，尚未进入真实 Calcite/lineage。原 14 项真实 PostgreSQL/Calcite 集成测试保留断言。新增标准 `UNNEST` 与可选 `WITH ORDINALITY` 的 FROM table reference，沿用既有 Calcite `SqlUnnestOperator`，没有绕过前端直接调用 Calcite；Parser AST printer roundtrip 33/33 PASS，原失败所属 PostgreSQL 集成类 14/14 PASS。修复后的所属 reactor 全测试第二轮正在进行，不能记为通过。
- 修复后 Planner 所属 reactor 第二轮 `full-regression,coverage test` exit 0；Parser 541/541、Planner 1,361/1,361，均 0 failure/error/skip，其他前置模块亦无 Maven 失败。仅证明同源 Java 测试，不等于静态、JNI 或 SQL E2E。相关回归日志 `/tmp/pangrova-planner-owning-unnest-fixed-20261011.log`；`static-analysis` 不重跑测试的门禁当前在执行。
- Planner 所属 reactor `-Pstatic-analysis -DskipTests verify` exit 0；Checkstyle/SpotBugs 未阻断。SpotBugs 输出部分 `needed for analysis were missing` 的 JDK 动态调用名称，未将该输出等同于完全无分析盲点；日志 `/tmp/pangrova-planner-static-unnest-20261011.log`。Parser/Planner 同源 JAR 正安装至隔离 Maven 仓库，待新增 UNNEST SQL E2E。
- 新增真实 SQL→Native `UNNEST(tags) WITH ORDINALITY` E2E 首轮 1 ERROR，发生在 Coordinator 列血缘捕获、Native 前，原行值/位置精确断言尚未执行。新增实际 PostgreSQL 数组列的 Planner 集成正例首先以同一错误红灯复现。诊断显示 Calcite 关联变量类型为 `(id,tags,value,position)`，Correlate 实际左输入仅 `(id,tags)`，且被引用 `tags` 索引为 1。原实现要求两种整行类型相等而误拒绝。现在两处血缘起源/分类共用验证：关联 ID 存在、被引用字段在左输入范围，且左输入逐字段的名称和类型与变量前缀精确一致；对任意重排/类型漂移仍拒绝。真实 PostgreSQL/Calcite 集成类 15/15 PASS。更新 Planner 安装及原失败 E2E 增量复测进行中，不把当前结果冒充 Native 执行通过。
- 列血缘修复后同一 UNNEST SQL 再次推进到 Planner Native 能力校验，首次因默认注册表漏报 `ExtensionSingle` 而 1 ERROR；本地 Native 已有 typed table-function envelope→Velox `UnnestNode` 实现及原生执行测试。Planner 现在仅将正式 `UnnestRelDetail` 的 `ExtensionSingle` 列为可原生执行，未知 detail 负例仍拒绝；能力注册、验证、结构转换 30/30 PASS。更新 Planner 安装后原失败真实 SQL→Native E2E 1/1 PASS，四行实际值与 1-based ordinal 全部精确匹配。随后 `ExplodeTest` 全类 3/3 PASS，含既有两项 EXPLODE；证据 `/tmp/pangrova-unnest-e2e-validator-fixed-20261011.log`、`/tmp/pangrova-explode-e2e-full-20261011.log`。新输入的 Planner 所属 `full-regression,coverage,static-analysis verify` 正在进行，不能沿用修复前静态/全测试结果。
- 该组合门禁在 `pangrova-test-common` 的结构属性测试退出 1：本轮新增 `validation → substrait.UnnestRelDetail`，与既有 `substrait → validation` 形成包依赖环；89 项中 1 failure、0 error/skip，Planner 自身尚未开始。共享 typed detail 已移至独立 `planner.extension` 包，两侧各单向依赖，未削弱属性检查。针对原失败包 DAG 与相邻准入/转换的定向检查 exit 0；`PackageDependencyAcyclicityPropertyTest` 的 2 次 exhaustive checks 通过，原三类相邻测试通过。组合门禁仍须对修正后的输入重新执行，前述失败轮不记 PASS。
- 包边界修复后 Planner 所属 `full-regression,coverage,static-analysis verify` exit 0；Parser 541/541、Planner 1,364/1,364、测试公共模块 82/82，均 0 failure/error/skip，Checkstyle/SpotBugs 没有阻断。日志 `/tmp/pangrova-planner-final-unnest-dag-fixed-20261011.log`。随后同源 `clean -DskipTests install` exit 0；干净 JAR 只含 `planner/extension/UnnestRelDetail.class`，不含迁移前旧包类。基于此 JAR 的真实 UNNEST SQL→Native 单方法再次 1/1 PASS；`/tmp/pangrova-unnest-clean-package-e2e-20261011.log`。这些是 Planner/UNNEST 局部闭合，不覆盖索引授权、全 E2E、全 Native/ASan、性能或唯一 Calcite 前端目标。

### 2026-10-11：复杂类型扩面与逐层根因

- `ComplexTypesQueryE2ETest` 首轮真实 SQL→Iceberg→Native 11 项：5 PASS、6 failure、0 error/skip，日志 `/tmp/pangrova-complex-types-e2e-20261011.log`；不计复杂类型通过。两个数组失败来自 Substrait 正式 SQL binding 的 canonical `collection:subscript` / `array:contains` 与 Planner 默认能力表旧 `array_contains` 命名不一致。只更正能力表为实际 canonical 名，未知名仍拒绝。数组两个原失败方法的增量 E2E 2/2 PASS（`/tmp/pangrova-array-capability-e2e-20261011.log`），与 [Trino 数组下标从 1 开始及 contains 语义](https://trino.io/docs/current/functions/array.html)核对。
- 三个 MAP 失败先在 AST→Calcite 入口抛 `Unknown expression type: MapConstructor`。使用 Calcite 正式 `MAP_VALUE_CONSTRUCTOR` 转换后，Planner 定向测试通过，原失败 E2E 推进到下一层 `Unsupported operator: MAP`（`/tmp/pangrova-map-converter-e2e-20261011.log`）；这轮仍是失败。已按 [Substrait MAP 字面量与嵌套构造规范](https://substrait.io/expressions/specialized_record_expressions/)继续补精确类型 MAP 字面量生成/Native 解码与原生执行测试，正在编译验证；不能以编译中状态计通过。原规范允许 MAP 字面量 key/value 同型，Pangrova SQL 构造在非字面量、NULL 键或重复键时保持明确拒绝，不使用 JVM 写入回退。
- STRUCT 用例原先写入快照确认 2 条，但 `WHERE info.age > 27` 返回 0；新加未过滤字段投影诊断，首次增量运行查询本身返回 2 行，但按 `age` 列名读取为 null，需继续查真实返回列名及字段值。下一次诊断运行因本代理错误并行两个共用 Maven `target` 的构建，协议生成类短暂缺失，属于测试调度冲突，不能计产品失败或通过；后续构建改为顺序执行。
- 进一步诊断 `SELECT info` 在 Java 结果为 `{}`；独立读取该失败轮 Parquet 文件（PyArrow）确认两行 STRUCT 子字段确实均为 NULL。Native `ROW(...)` 按同源 physical bridge 生成 `field_0/field_1`，Iceberg 写入器却按目标 `name/age` 取 Map 值，导致静默空值。`IcebergValueCoercion` 现通过 `FunctionBindingDeclarations.anonymousComputedField(i)` 严格识别完整匿名位置映射，按目标 STRUCT 顺序写入；具名字段仅可使用目标字段，允许省略可空字段、拒绝省略必填字段，其他组合拒绝。`RowToRecordMapperTest` 15/15 PASS。物理证据与修复后的 STRUCT 查询 E2E 一致。
- 首次修复后复杂类型整类行数检查 11/11 PASS，但增强实际值断言后 8 PASS、3 failure：MAP 查询整列的 Arrow MAP 被测试/客户端解码为 entry 列表、键访问读回 NULL、STRUCT 字符串字段在结果中仍为 Arrow `Text`。这轮是语义失败，未计复杂类型通过。独立读取 MAP Parquet 文件发现 `'width'` 被 Calcite common `CHAR(6)` 补成 `'width '`，使下标精确键不匹配。结果端与 Worker PageDecoder 分别按 Arrow 字段类型递归解码 MAP/STRUCT/嵌套文本，非法/重复 MAP 键拒绝；Transport 归一化聚焦测试 PASS。Planner MAP 字符串字面量最终明确转成 VARCHAR，避免 Calcite CHAR 宽化带来的合成尾空格；一次临时 `trim` 做法经新增显式尾空格键负例证明会错误删除用户输入，已撤销。对照 [Trino CHAR→VARCHAR 语义](https://trino.io/docs/current/language/types.html)核查。
- 修复后的 `ComplexTypesQueryE2ETest` 整类以实际值核对 11/11 PASS，0 failure/error/skip，包含 ARRAY 下标/包含、MAP 写回读/下标/EXPLODE、STRUCT 存储字段/过滤；日志 `/tmp/pangrova-complex-types-exact-e2e-20261011.log`。新增“显式尾空格键仅按精确键命中”的 SQL→Native 单方法 E2E 1/1 PASS，日志 `/tmp/pangrova-map-trailing-key-e2e-20261011.log`；同一行为的 Planner canonical 协议测试先红后绿。这轮是最终完整整类回归之前的增量结果。整个 E2E 模块、Coordinator 等其他范围仍有缺口。

### 2026-10-11：复杂类型最终增量与真实夹具回归

- MAP 文本键仅在 MAP 构造器的普通字符串字面量上显式 CAST 到 VARCHAR，避免 Calcite 将短键补成 CHAR，同时保留用户明确写出的尾空格。Planner 显式尾空格回归先红后绿；真实 SQL `attrs['width '] = 100` 且 `attrs['width'] IS NULL` 单项 E2E 1/1 PASS，日志 `/tmp/pangrova-map-trailing-key-e2e-20261011.log`。
- 新增 Worker 真实 Arrow IPC 复杂值回归，核对嵌套 ARRAY/STRUCT/MAP 值及重复 MAP 键拒绝；`PageDecoderComplexTypesTest` 2/2、原 `RemoteExchangeSourceDeserializationTest` 7/7 PASS。Planner 所属 `full-regression,coverage,static-analysis verify` exit 0，Planner XML 173 类/1,367 项均 PASS、0 skipped；日志 `/tmp/pangrova-planner-complex-full-gates-20261011.log`。
- Iceberg STRUCT 所属模块回归发现不能拒绝已有的“命名输入省略可空字段”语义。最终接受只含目标字段的命名映射（必填齐全，未给的可空字段为 NULL）或完整 canonical 匿名位置字段；未知字段拒绝。新增额外字段拒绝回归，原 `IcebergValueCoercionTest` 语义保持通过。Iceberg 所属全 345/345 PASS；Transport 100/100 PASS；Worker 本轮新生成报告 175 类/1,216 项 PASS、0 failure/error/skip。增量首轮 Lance 7 ERROR 是未传配套 Native fixture tool；从失败模块带匹配当前 `build-joint/pangrova_lance_fixture_tool` 恢复。日志 `/tmp/pangrova-complex-owning-modules-20261011.log`、`/tmp/pangrova-complex-owning-resume-20261011.log`、`/tmp/pangrova-iceberg-struct-owning-20261011.log`。
- 最终 `ComplexTypesQueryE2ETest` 12/12 PASS、0 failure/error/skip（ARRAY 5、MAP 4、STRUCT 3），实际 SQL→Planner→Native→Iceberg→结果读取并对实际值断言；日志 `/tmp/pangrova-complex-types-final-12-e2e-20261011.log`。本轮 `.pangrova/setup.sh --check`、`git diff --check`、E2E 真实性脚本均 PASS。
- 匹配当前原生 MAP 源码的 ASan 测试二进制重建通过；`SubstraitLiteralExecutionTest` 20/20 PASS，包括 typed MAP 新测试；日志 `/tmp/pangrova-asan-map-build-20261011.log`、`/tmp/pangrova-asan-map-literal-class-20261011.log`。尚未取得 ASan 全量资格。
- 原生首轮 Release CTest 4,129 项有 61 FAIL：60 项因缺少真实 PG/Lance broker 前置而在 `SetUp` 拒绝，另一个 Flight shutdown 在并发时 2 秒断言超时约 5 秒。补齐仓库规定的 Docker API 1.44 与 Testcontainers VM 内 socket 后真实 PG broker 就绪，61 项增量重跑中原 60 项全部 PASS，Flight 并发时再现同一超时；Flight 单跑 36ms、30ms 两次 PASS。这个并发时序问题未闭合，不能以单跑覆盖失败。真实 broker 下完整串行 Release CTest 随后 4,129/4,129 PASS，耗时 589.26 秒；日志 `/tmp/pangrova-native-full-broker-serial-20261011.log`。初轮和增量诊断日志 `/tmp/pangrova-native-complex-full-20261011.log`、`/tmp/pangrova-native-lance-retry-20261011.log` 保留。
- 同源码 ASan 测试二进制完整 CTest 4,129/4,129 PASS，耗时 503.72 秒，日志 `/tmp/pangrova-native-asan-full-broker-20261011.log`。但显式 `ASAN_OPTIONS=detect_leaks=1` 的新增 MAP 用例断言虽 PASS，进程退出报告 52,334 bytes/12 allocations，退出 1；栈包含 glog、Protobuf descriptor pool、Velox 测试单例和 macOS 运行库，日志 `/tmp/pangrova-asan-map-lsan-explicit-20261011.log`。因此完整 ASan 执行与显式 LSan 零泄漏门禁应分开记账；后一项 FAIL，不能宣称 sanitizer 生产资格完成。
- 额外脚本门禁：SPI annotation 143 类与稳定性 23 类 PASS，dead-stub PASS，文档类引用经准确表述外部 SDK 类型/HTTP 头后 178 引用 PASS。Planner golden plan 门禁因相对 `origin/main` 的 38 个 golden 计划变更且无 PR 显式批准退出 1；不伪造批准，也不能算该门禁通过。

### 2026-10-11：Coordinator 状态回执与持久化提交增量闭合

- Coordinator 首次自身全模块测试 2,818 项中 4 failure、14 error（日志 `/tmp/pangrova-coordinator-owning-current-20261011.log`）。按失败类别修复精确 issued TaskRequest 校验、状态报告 ACK 重放夹具、真实签名结果票据和每次 property try 独立的 Worker 生命周期。并发 speculative loser 清理改为在同一锁内读取并退休已记录的真实 drain receipt；调用者提供的 receipt 仍逐字节严格比较。相邻 `CoordinatorWorkerProtocolPropertyTest` 14/14、`TaskTrackerRegistrationPropertyTest` 10/10、`SpeculativeExecutionWiringTest` 14/14 通过；并发方法额外 5 次独立 Maven 调用通过。此结果不覆盖索引维护授权缺口。
- 扩大到状态/推测相关 75 项时，真实 PostgreSQL `TaskStatusReceiverDurableProducerIntegrationTest` 有 3 个断言失败。定位到真正的提交顺序漏洞：`FINISHED` 的持久化页面在任务仍 `PENDING`、或只注册请求但尚无 Coordinator 任务时，先于任务状态校验提交。现提交前要求同一查询/任务处于 `SCHEDULED` 或 `RUNNING`；未调度报告直接拒绝，不保留序列。完全相同的报告重传可幂等 ACK，另起序列的重复终态不能再次提交。该 PostgreSQL 集成类 7/7 PASS；状态报告与推测执行 11 类/75 项同一轮 PASS、0 failure/error/skip，日志 `/tmp/pangrova-durable-producer-focused-2-20261011.log`、`/tmp/pangrova-coordinator-status-speculative-broad-2-20261011.log`。
- Coordinator `-Pstatic-analysis -DskipTests verify` 仍在 Checkstyle 阶段退出 1；XML 有 132 文件/592 项告警，含既有及此次大工作树早期改动，不能记为静态门禁通过。当前新增的 durable 调度校验行未出现在该 XML 告警中。索引后台维护仍需把原创建主体与精确表/列授权持久化并在提交事件处理时按当前权限重判；当前无范围的 `ConnectorAccessorResolver` 正确拒绝，索引统计/提交相关旧正例尚未修复。完整 Coordinator、Java reactor、所有 SQL E2E、显式 LSan、并发 Flight 稳定性、性能/规模与最终全量重跑均未通过，不具备生产级验收。
