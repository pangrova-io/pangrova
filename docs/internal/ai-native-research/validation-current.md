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
| 最新匹配 ASan 全 CTest | 未完成；已报告 4,046 PASS，4 项持续等待 | 665–668 结果 reader 用例仍在运行，余下 18 项未开始；不能记为 ASan PASS |

## 不可记为通过的检查

- 首次 Native 全测试的 4 个 fixture-pause 用例未退出。已依据固定 Velox 的
  pause-owner 语义修复测试 teardown；没有在 production cancellation 强制 resume。
  最新 Release 全 CTest 通过，但这 4 项在当前 ASan 中再次持续等待。
  测试显式持有 SDK pause，而 teardown 先调用阻塞的 `runner.cancel()`，
  然后才调用 pause owner 的 resume。固定 SDK 对 paused off-thread driver
  要求 owner resume 才完成终止；该调用顺序仍需实际栈证据与统一负责人修复。
  不在生产 cancellation 中强制 resume，也不增大超时掩盖等待。
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

`JdbcExecutionAnchorStore` 的锁后数据库时钟修复目前只完成设计，未改源码。
`CURRENT_TIMESTAMP` 是事务起点；仅替换查询字符串也不能修复锁前采样。
续租、release/yield 须先按固定顺序锁定 exact rows，再用 `clock_timestamp()`
检查租约；批量更新仅限锁定集合。真实 PostgreSQL 锁等待跨过 deadline 的
回归和最终 publication 同事务接入均尚未实施。
