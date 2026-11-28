---
title: "Pangrova 执行链路与代码命名实施计划"
description: "Pangrova 查询与写入优化、Java 类型和包更名及单机验收计划"
---

# Pangrova 执行链路与代码命名实施计划

执行链路优化的历史交付基线是 [PR #112](https://github.com/pangrova-io/pangrova/pull/112) 合入后的 `904eeba54`。现有证据包括 230 项专项回归，以及 A9 之后的 70 项 Java 和 12 项重新链接的 Native 定向回归。原执行清单的 C1–C9 文档勘误已完成。类型和包的原审计基线是 `aed718692`，本次以 `main` 的 `b63549583` 核对现存声明、包和测试入口。实施时同步最新 `origin/main`，逐项重核状态。

本轮包含 A1–A13 实现与专项验证、B1–B8 单机联合验收，以及 N1–N4 命名实施批次，覆盖 61 处 Java 类型更名、22 个待迁移的旧包和 11 处需保留的声明。两个方向共用同一提交的最终验收产物。Coordinator、Worker、Shuffle Service、客户端、PostgreSQL 和 MinIO 可在同一主机用独立进程或容器启动。故障注入、性能和资源释放证据均按同机拓扑记录。

## 实施顺序与并行边界 {#implementation-order}

1. **冻结基线。** 从包含本计划的提交接续并同步最新主线。记录同机拓扑、版本、SQL、快照、资源、测试状态、各阶段时延和实际 SQL/I/O 次数。核对调用方、服务注册与旧名的新旧状态。以真实观察确认每项性能缺口。
2. **基础更名和性能取证并行。** 先收口 N1 的 Common、JDBC 和策略持久化。同步准备 A4 的负载与阶段计量，以及 A2、A8、A11、A13 的故障和物理资源证据。共用模块由同一批次负责，性能取证可使用冻结的基线。
3. **开发并分批集成。** N2、N3 的更名先于涉及同一文件的行为修改合入，N4 汇总模块完整性。执行链路收口受理、所有权和终态（A1–A4），推进枚举、放置和读取（A5–A8），随后以实际读取证据校准动态过滤和自适应执行（A9–A10）。Shuffle 止写与回收（A11）和写入日志、CDC、fenced commit（A12–A13）由独立批次并行，同一持久化或协议权威由单一批次更新。更名与行为修复各有可审查提交。
4. **统一验收和交付。** 每批推进时先跑最贴近的检查，全部开发集成后按 B1–B8 和更名验收矩阵，以同一提交重建并跑单机全链路。按实际通过范围关闭任务，记录尚缺的证据和可复现命令。

## 实施记录 {#implementation-evidence}

### 2026-11-28 同机基线

- 取基线 `2225d2d53d8981c69bf1a638f92a8c8ca1db519e`。`git fetch origin main` 确认
  `b63549583914d12bd087b12035181bb99a720b8f` 已是基线祖先。从该提交切出
  `codex/implement-execution-backlog`，初始 worktree 无本地改动。
- 主机为 macOS 13.7.8 arm64，8 个逻辑 CPU，16 GiB 内存。Docker 28.5.1 VM 提供
  8 个 CPU 和约 7.65 GiB 内存。使用 Corretto 17.0.20、Maven 3.9.16、Python 3.12.14 和
  `en_US.UTF-8`。系统 Python 未达到 3.10 时，首次构建在协议代码生成阶段退出。
- 基线 `mvn -B -DskipTests -Pnative-runtime clean install` 成功，153 个 reactor 项目构建完成，
  此命令跳过测试。用 `deploy/docker/stage-role-classpaths.py` 冻结角色运行时和 CLI，
  清单 SHA-256 为 `08613b3eb3fa03484332f69988462285d9239addf114c0e81f4189aaced37bfa`。
  该清单包含此前安装的 Native classifier，不能作为同源验收产物。
- `build-lance-ffi.sh` 完成后，`cmake --preset dev-release -S pangrova-worker-native` 拒绝
  既有 Velox checkout 的注册源 provenance。锁定提交的独立副本已通过仓库补丁
  `apply` 和 `verify`。旧 classifier 的进程探测也在首个 Worker 的函数目录握手处失败，
  Completion/Drain 时延、SQL、I/O 及采样数均未取得。原始日志为本机证据目录中的
  `baseline-install-utf8.log`、`baseline-native.log` 和 `baseline-exploratory-topology.log`。
- 首轮可比负载采用 `sql_process_comparison.py`。该夹具使用一个 Coordinator JVM、两个
  Worker JVM、独立 CLI、PostgreSQL 16、LDAP、Iceberg 本地仓库和 TLS Flight。每轮创建独立表，
  写入三个文件，每文件为 `4, 5, 5`，然后交替运行 `SELECT 21 + 21 AS answer` 与
  `SELECT SUM(id) AS answer FROM <table>`，每种查询 3 次预热、12 次采样，结果均应为
  `42`。此负载与 MinIO、独立 Shuffle、CDC 和更大的容量矩阵分别取证。

### 范围决定

2026-11-28 从 A7 移除 Apache ORC reader 的实现与验收。A7 的 Parquet、Iceberg、Lance
和 Paimon 仍按下方的格式、范围和对象身份合同验收。

### 基线源码复核

逐项扫描声明与包路径。61 个旧类型各有一处声明，22 个旧包合计 162 个带包声明的源码文件
（含子包及测试），11 个保留项含 Java 和 Protobuf 各一处 `ColumnMapping`。
仓内命名迁移按下方映射执行。执行条目的源码和既有回归入口如下，关闭条件仍以同源运行证据为准。

| 范围 | 生产路径与回归入口 |
|---|---|
| A1/B1 | `QueryAdmission` 与 PostgreSQL query journal。以 `QueryManagerDurableAcceptanceTest`、`JdbcQueryStateJournalIntegrationTest` 为定向入口。 |
| A2/B2 | `QueryTerminalCleanup`、`QueryOwnershipTracker`、Native owner 释放和租约退役。以 `CoordinatorFailoverOrphanE2ETest`、`IndependentClientCancellationE2ETest` 和 `ReadCompletionDatabaseBudgetE2ETest` 复核。 |
| A3 | `ResultDisposition` 贯通 HTTP、CLI 与 Flight。独立进程夹具覆盖 STREAM 和 NONE，另需核对迟到 Ready、失败 EOF 与取消交错。 |
| A4/B3 | `PersistentTenantOverloadE2ETest` 已有独立双 Coordinator 固定到达率路径，`AsyncSubmitSaturationE2ETest` 覆盖提交压力。需要同机当前版本的数据与阶段归因。 |
| A5 | `PaimonAppendRangeScan` 和 `SplitPlanningBudget` 控制增量枚举，`PaimonScopedSplitExecutionTest` 覆盖固定上下文。需补对象存储故障与 I/O 关闭证据。 |
| A6 | `WorkerHeartbeatBootstrap` 发布全 Worker 在途总量，`WorkerPlacementWork` 对该总量与本 Coordinator 本地计数取最大值。两者的重叠与外部负荷需要分别核对。参考 `SplitPlacementWeightTest` 和 `WorkerHeartbeatIdentityTest`。 |
| A7/A8 | `NativeS3ReadEvidenceE2ETest` 是对象存储 I/O 与读取身份定向入口。验收聚焦受支持格式、精确对象身份与删除语义。 |
| A9 | `TaskRuntimeFilterWire` 与 `RuntimeFilterRpcIntegrationTest` 覆盖控制面，`IndependentLateRuntimeFilterE2ETest` 覆盖真实晚到过滤。主线已包含 build-key range 不可变快照修复 `b325de4c4`。 |
| A10/B5 | `DurableStageShuffleEvidence` 记录持久交换证据，`RemoteShuffleSqlCoalescingE2ETest` 和 `AdaptiveQueryExecutionE2ETest` 提供真实 SQL 入口。 |
| A11/B7 | `PostgresShuffleWindowPublication` 发布 producer window，`DurableShuffleIntegrationTest` 与 `PhysicalTwoWorkerShuffleE2ETest` 覆盖持久状态和数据面。未出现对象的止写证明仍开放。 |
| A12/B8 | 生产入口 `JournaledCdcRuntime` 由写入 journal 查询 checkpoint，`CdcWriteCommitCoordinator` 处理 fenced 发布。`IndependentCdcProcessesE2ETest` 和 `IndependentRelationalWriteRecoveryE2ETest` 提供重放、接管与回读入口。 |
| A13 | `ObservedWriteStagingRecovery` 和 `JdbcObservedWriteStaging` 驱动观测文件回收。现有回收作用域覆盖已报告的本地 Paimon SDK 文件。 |
| B4/B6 | 复用 `RemoteShuffleSqlCoalescingE2ETest` 的真实 SQL 入口与独立进程基准。以 `NativeCapabilityHandshakeE2ETest` 和物理排空夹具核对同源部署及所有权。 |

### 2026-11-28 分批回归

以下日志位于本次运行的本机证据目录。失败项和当前同源验收缺口仍在 A、B 和 N 表中。

| 批次 | 提交和实际结果 |
|---|---|
| N1 | `a11cb8f53` 完成 Common、JDBC、策略持久化和精确类加载白名单的切换。`n1-selected.log` 为 229 项通过，零失败、错误或跳过。扩大范围的首轮测试在旧 Native capability fixture 的 descriptor 身份校验处失败，见 `n1-focused.log`。 |
| N2 | `6e270165c` 完成 Parser、Planner、Protocol Java、Indexing、生成与 Connector 更名。`n2-selected-corrected.log` 为 380 项通过。扩大编译范围时发现 Apache Commons Codec 的同名引用，已恢复该外部 SDK 引用。 |
| N3 | `c70789b84` 完成启停、观测、AI、运维与测试夹具迁移。`n3-selected-independent.log` 记录 107 项，1 项 SPI 可达性失败。`baseline-spi-wiring-with-source.log` 在冻结基线复现相同四个独立库组件。加入显式消费者合同后，`57d6f3d37` 对应的 `reachability-final.log` 中 18 项通过，覆盖 SPI、ServiceLoader、Bootstrap 和 SQL 编译会话。Worker 启停的大范围复跑仍需新 Native capability fixture。 |
| N4 | 声明与路径扫描确认 61 个目标声明各唯一、22 个旧包无源码声明、11 个保留声明均在。JDBC Maven 模块名继续为 `org.pangrova.persistence.jdbc.support`。模块基线、SPI 注解、365 项文档类引用、Protobuf descriptor golden 和 244 项仓库工具回归通过。SpotBugs 过滤器的 JVM 类型路径和两处已有的精确 SQL 分类检查已同步，见 `spotbugs-filter-final.log`。 |
| A4/B6 | `471cb357a` 扩展同一条 SQL 夹具的 PostgreSQL 计数、Native 文件读取和 Flight 阶段采样。以冻结基线的 Java 测试类路径执行 `javac --release 17` 通过。运行结果等待同源 Native。 |
| A6 | `8490621ce` 在 Worker 注册表恢复时采纳同 incarnation 的较新负载样本，并保留本地退役状态。定向回归在构造 Worker 的旧 capability fixture 时被 descriptor 校验阻止，见 `a6-registry.log`。多 Coordinator 并发派发以及心跳与本地在途计数的重叠仍待验证。 |
| A1/A11/A12/A13 | `56dc818b2` 对缺少对象回执的 Paimon 暂存保留回收义务。定向 reactor 共 104 项通过，包含 PostgreSQL query journal 48 项、journal SQL 1 项、write journal 2 项、PostgreSQL/MinIO Shuffle 33 项和 Paimon 20 项，零失败、错误或跳过。见 `write-shuffle-journals.log`。Shuffle 在 32 批次共享一个写窗口的场景断言 8 条总 SQL，独立测试断言读窗口续租 2 条、关闭 1 条。完整进程路径的 SQL 和排空仍待测。 |

- `29e3204de` 增加双独立 Coordinator 的认证并发重放。4 次相同幂等键提交必须获得同一回执，
  只占用 1 个槽位，并由实际 owner 完成 Flight 交付和退役。`javac --release 17` 已用 reactor
  测试类路径编译该用例，实际进程运行等待同源 Native。
- `baf18f9d0` 的 `shuffle-window-sql-final.log` 由 JUnit launcher 在真实 PostgreSQL/MinIO
  上运行 `DurableShuffleIntegrationTest`，35 项通过。1 批 1 片、32 批 1 片和 32 批 4 片
  各 8 条窗口总 SQL。已准入的 128 次帧检查、manifest 页读取和 payload 读取合计 0 条。
- JDK 17 执行 `mvn -B -DskipTests -Pstatic-analysis verify` 完成全 reactor 的 Checkstyle 和
  SpotBugs，见 `static-analysis-complete.log`。`prepare-e2e-ai-image.py` 已生产本机测试用
  AI 模型镜像和 digest 回执。Velox 专用目录通过确认 SHA-256 的 simdjson 源归档和已安装的
  Boost 配置启动编译，证据见 `qualified-velox-warm-boost.log`。

- PostgreSQL 上的 generation exchange 43 项、JDBC 时间与事务 4 项和 Paimon 规划预算
  11 项通过，见 `extended-java-postgres.log`。同轮 CDC 在默认 Docker socket 探测报错。
  从当前 Docker context 显式传入 host 后，CDC PostgreSQL/Kafka 两组共 41 项通过，
  Paimon 固定快照与资源生命周期 12 项、持久受理 10 项通过，
  `WriteCommitJournalIdempotencyTest` 的 7 项仍在旧 Native capability fixture 校验处退出。
  见 `extended-java-postgres-docker-context.log`。
- `query-peer-http.log` 的 Coordinator HTTP 定向回归 3 项通过。覆盖同 JVM 内两个真实端口、
  PostgreSQL 下的幂等回执、并发等待、诊断零 journal SELECT、显式回执恢复与取消。
  双独立 JVM 和出错 Worker 的组合仍待同源验证。
- `b59ce4b58` 将已准备但未进入传输的本地预留加入 Worker 心跳负载，已进入的 attempt
  继续用较大值处理重叠，保留精确释放边界。控制模块 reactor 定向编译成功，20 项中
  4 项通过，16 项在旧 Native capability fixture 校验处退出，见 `a6-placement-reactor.log`。
- `50795527e` 将 manifest PUT 的对象元数据绑定到该次写入的 UUID，补偿清理只结清
  对应的对象键和预留。真实 PostgreSQL/MinIO 上 `DurableShuffleIntegrationTest`
  35 项通过，覆盖两次同键 PUT 在分批清扫中分别结清。见
  `shuffle-exact-manifest-reservation.log`。未出现对象的远端止写证明仍开放。
- `7ba8d6949` 将本地 Paimon SDK 已报告文件的 inode 与时间戳纳入回收指纹。
  `PaimonDistributedDeleteCommitTest` 在 macOS JDK 17 Maven reactor 和 Linux arm64
  Temurin 17 容器中各 21 项通过。Linux 实际验证了同长度替换保留、原对象删除与重复清理，
  两端都解码了不带指纹的历史 v2 fragment。见 `paimon-file-identity-macos.log` 和
  `paimon-file-identity-linux-staged.log`。
- `fc361160d` 把 macOS 进程 user/system CPU 合计加入角色采样。Mach 时间单位转换在本机
  对齐进程 CPU 时钟，独立 JDK 17 子进程的探测取得 3 组 CPU、RSS 和 heap 样本。
- Native SDK 使用固定 Velox 提交 `3da0390118fa4b0addbfbdeb03f438ec1b1237a6`，
  通过仓库补丁检查并重建 archive 和 provenance。Release 目标
  `pangrova_native pangrova_function_export` 链接成功，arm64 dylib SHA-256 为
  `15d51352c077e5d3b440a3b772377b86ba9e49a24dadc8365a35b257fb6146b3`。
  基线和候选的 Native 源码及 Protobuf 定义相同。见 `native-release-build.log`。
- 基线 `2225d2d53` 和中间候选 `3eec8e2b9` 以同一 Native 库各完成 153 个 Maven reactor
  构建。角色清单 SHA-256 依次为
  `95094e928f2de8f4d2889a2a5114f728af0225ccd39c8b26cfd195c057ef90eb` 和
  `56f8a88ab3b14c1be7cb5e1f7171c6d2620764e0e9b6f62fbd8e59e700809b5b`。
  `qualified-handshake.log` 的打包 JNI 握手 1 项通过，实际二进制目录已在 `a1720b2a4` 更新。
  `qualified-protocol-owner.log` 的协议测试 611 项、`qualified-coordination-selected.log`
  的真实目录关联测试 27 项通过。注册表恢复定向测试另有 9 项通过。
- 首个同源基线探测启动 Coordinator、Worker 和 CLI，DDL 已返回 NONE，基准误用
  STREAM 断言后停止，0 条计量样本。`ebb434c45` 调整 DDL/DML 注册为命令语义后，
  候选探测取得 2 条正确结果，各有 Native Flight 末批和实际 task drain，租户占用归零。
  两次普通查询各观察到 0 条 journal SELECT、2 条 journal INSERT，分别涵盖新受理与派发。
  Iceberg 扫描读取 1464 字节，3 次数据读取和 6 次元数据读取。这次探测与构建重叠，
  不用于基线性能比较。后续 `b02777e80` 增加读取公开 DRAINED 状态的断言。
- 首轮同源独立进程的本地 SQL 和 Flight 取消各通过 1 项。同轮双 Coordinator 重放返回
  429，晚到过滤缺 Native ACK，远端 Shuffle 的两项无 hint 合并检查通过、两项选择性
  Join 的生产者写流失败。见 `first-process-acceptance.log` 及其各测试日志。
  `0f91bef5b` 修复租户满额时回放已持久化且仍保留的明确幂等键。
  PostgreSQL journal 48 项、内存 journal 10 项、Coordinator 回放 11 项和 HTTP
  指纹 3 项定向通过。候选重新构建后复测进程级用例。
- `IndependentRelationalWriteRecoveryE2ETest` 使用两个独立 Worker、PostgreSQL 和
  Iceberg SDK 回读，通过 6 项。见 `first-process-acceptance.log`。
  `58cde1142` 将远端 Shuffle Service 的 executable 纳入同源 manifest 并以独立进程启动，
  接入同机 PostgreSQL、MinIO 和 TLS。首次独立 Shuffle 复跑定位了写流上限 4 过小；
  `f5038a586` 为这个 SQL 夹具设定 16 条流、48 MiB 聚合入站缓冲与 4 个 I/O 线程。
  此后 `RemoteShuffleSqlCoalescingE2ETest` 4 项通过，日志记录实际 Shuffle、Coordinator、
  两个 Worker 和 CLI 的 PID。见 `signed-and-capacity-process-acceptance.log`。
- Release Native 首轮 CTest 跑了 4391 项，4388 项通过。`de6fcfe79` 修复并发
  generated-value 夹具的临时路径碰撞，`34629a3e0` 补齐历史 Velox checkpoint 的反向
  S3 patch，相关复跑 7 项全过。见 `native-tests-full.log` 和
  `native-tests-qualification-retry.log`。
- 第三项 Native 失败来自本机既有 Arrow dylib 的弱符号合并。仓库的
  `PANGROVA_NATIVE_BUILD_PHASE=arrow-flight` 重新构建并验证了 Darwin 独立包，
  针对新包运行的 `CanonicalFlightPackageTest.py` 五项通过。见
  `qualified-darwin-flight.log` 和 `qualified-darwin-flight-test.log`。
- 以合格的 Arrow/Flight 包重新链接后的 Release Native SHA-256 为
  `8a65aff4fca301cb400ebf146656d77bb4f365dc29cedb8ad7bb4c5b79fc31ff`，其 Mach-O rpath
  指向验证过的私有 Arrow 包。新函数目录与跟踪夹具逐字节一致。带地址和未定义行为
  插桩的 generated-value 独立目标重复 5 轮，每轮 6 项通过，无 sanitizer 诊断。
  见 `native-qualified-pinned-link.log` 和 `generated-value-segment-asan-ubsan.log`。
- `50050a0fc` 下的独立晚到过滤夹具覆盖 40 个文件、10,485,760 行，比较同一份快照。
  两台 Worker 上收到精确 Native 确认，扫描算子输出从 10,485,760 行降到 8,658,946 行，
  实际读取均为 15,951,120 字节，回读结果相同，task 与租户已排空。此负载下过滤减少
  了扫描输出，已发起的 I/O 没有减少。见 `reachable-peer-and-late-read-metrics.log`。
- 同版本交付提交集中到 [PR #127](https://github.com/pangrova-io/pangrova/pull/127)。

Java 定向测试使用下面的命令结构，日志保留各批传入的选择器和 Maven reactor。
Paimon、Shuffle 与 journal 的复现命令如下。

```bash
mvn -B -pl pangrova-connector/pangrova-connector-paimon,pangrova-persistence/pangrova-persistence-query-postgresql,pangrova-shuffle/pangrova-shuffle-service -am \
  -Dtest=PaimonDistributedDeleteCommitTest,JdbcQueryStateJournalIntegrationTest,JdbcWriteCommitJournalInitializationTest,DurableShuffleIntegrationTest,JournalSqlOperationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
mvn -B -pl pangrova-testing/pangrova-testing-integration-tests -am \
  -Dtest=SpiWiringVerificationTest,ServiceLoaderReachabilityTest,BootstrapReachabilityTest,SqlCompilerSessionIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
python3 -m unittest discover -s .pangrova/scripts/tests -p 'test_*.py'
```

## A 实现工作包与单项验证 {#remaining-work}

| ID | 当前证据 | 剩余工作和关闭条件 |
|---|---|---|
| A1 · WP1.1 HTTP 受理、幂等与准入 | 普通新受理单条 INSERT，已验证本地 PostgreSQL、无幂等键写入和双 Worker | 组合两个独立 Coordinator、Worker 故障、响应丢失、未知提交、并发重放和 principal 隔离。核对原回执、唯一执行、租户槽位与正常路径 SQL 计数。见 B1/B3 |
| A2 · WP1.2 查询所有权与物理排空 | 已有签名回执定向转发、精确派发和退出凭证、租约与槽位同事务退还、本地 Flight 排空与持久接管 | 验证同机独立进程退出、旧实例迟到报告、租约过期、同名实例替换、接管与客户端重连。分别证明 Native 任务与结果、Shuffle、快照 pin 和槽位退役，并检查对外 DRAINED 的发布时间。覆盖已发布写入的成功保留和无运行所有者的回收。见 B1/B2/B6/B7 |
| A3 · WP1.3 结果状态 | `PENDING`、`STREAM`、`NONE` 已贯通客户端与服务端，本地双 Worker 和独立 CLI 已有覆盖 | 在独立进程上验证空结果、无结果集、延迟 Ready、结果读完后写提交失败、取消恢复。不允许端点 Ready 与逻辑终态互相替代。见 B2/B6/B8 |
| A4 · WP2 关键路径归因与容量 | 已有准备排队/执行、Worker 回报、胜出超时和全局物理排空等观测，本地真实 PostgreSQL 已验证排空计数不重复 | 跑同机固定到达率矩阵，覆盖短长查询、租户过载、目录阻塞、终态报告压力和 durable commit。关联真实提交、规划、ACK、JNI 创建、首末批、终态和排空，分离连接池、锁和实际执行时间。按 B3 证明饱和时独立查询与取消仍能推进 |
| A5 · WP3.2 Paimon 枚举 | 普通读取、内部增量和直接追加共用阶段上限，已加入跨窗口删除合并与受阻读取用例 | 在最新候选统一复跑并加入对象存储流中断、超时、取消和 manifest 压力。核对固定快照、完整结果、I/O 实际关闭和元素/字节/并发额度，校准阶段预算 |
| A6 · WP3.3 分片工作量与 Worker 放置 | 窗口合并持久认领，已纳入冻结 split、交换字节和精确在途计数。已区分本地预留与进入派发的 attempt | 补多 Coordinator 样本传播和并发放置验证。核对 Worker incarnation、样本新鲜度、ACK、派发前回滚、在途与心跳重叠以及实际释放。按格式、冷热、删除文件和并发校准打包/放置，测分配等待和最长任务。共享样本不可扩大查询授权 |
| A7 · WP3.4 物理读取覆盖 | Parquet/Iceberg 支持基于合法范围读取并共享对象长度，Lance/Paimon 已有各自的整文件或范围约束 | 验证受保护产物贯穿授权与读取的整对象身份、本地对象存储的 GET/HEAD 次数，以及删除文件较大时的成本与位置正确性。范围不得重叠或缺失，需核对整文件对照、删除语义、投影、压缩和对象版本不匹配。见 A8 |
| A8 · WP3.1 物理 I/O 和权限 | Native 读取计数及 S3 body 字节已进入 attempt Profile，标准 Iceberg/Paimon 已有逐目录凭证和缓存隔离实现 | 复跑真实 MinIO 与独立 Native 进程夹具，再按标准 SQL 路径及其他 SDK 实测逻辑请求、实际请求/重试、返回字节、缓存与异常。验证同地址不同凭证、目录变更、撤权及缓存隔离。HTTP header/传输帧如未实测标为不可用 |
| A9 · WP4 跨 Stage 动态过滤 | Iceberg/Paimon 同宽 TINYINT、SMALLINT、INTEGER、BIGINT 及多独立组合已实现，摘要有界。A9 已有定向 Java 和 Native 交换回归 | 以独立双 Worker 同源部署包验证晚到过滤、被接纳 attempt、Native 确认、split/row group/读取字节、结果与实际释放。覆盖共享 Stage、其他键、连接器及并行组合。按负载校准等待成本和晚到对已发 I/O 的影响，保持缺证、NULL、版本不匹配时的结果正确 |
| A10 · WP5 AQE 与持久读取布局 | 单输入和对齐双输入已有真实 SQL 合并证据，持久单/双侧拆分、接管与最终发布有 PostgreSQL 回归 | 复跑同源部署的 Shuffle → JNI → Flight 混合范围和偏斜用例。验证更复杂输入、Join 类型、NULL、重复键、投机获胜、原有 epoch、重试、durable replay、读取覆盖、最终 producer cohort 和复制上界。用混合负载校准阈值与尾 task。见 B5 |
| A11 · WP6 Shuffle 撤权、producer 停止与回收 | 已有读写 lease/window、终态封口、精确收敛、晚到对象清扫和本地 PostgreSQL/MinIO 证据 | 补未出现对象的 PUT 与 producer window 的正向停止证明，覆盖丢 ACK、迟到 PUT、清扫跨页、旧 owner、替换实例和并发 reader。撤销后阻止新的读取/发帧，在途 I/O 与实际退出有证据才删除或交还。本机多实例复跑 SQL 计数、热点并发与故障回归。见 B7 |
| A12 · WP8 写入日志与 CDC 回执 | 二进制 journal、精确回执、CDC 领导任期和 PostgreSQL 变更订阅已接入，本地写入 journal 有定向验证 | 验证各来源重放窗口、回执回收、提交 ACK 丢失、CDC 断线重连与跨 Coordinator 暂停/恢复。复跑空闲数据库计数，覆盖并发/故障与锁等待。只有目录提交可见且精确发布证明成立才确认成功或前进 offset。见 B8 |
| A13 · WP8 暂存文件与 fenced commit | 已接入 Paimon SDK 已上报文件的同 Linux 存储命名空间清理及独立租约回收 | 补未上报文件、Native 物理写出、外部根/对象存储的存储权威。用本地分离存储根和 MinIO 测试，注入部分删除、未知提交、受保护存储身份变化、租约变化和重复清理，验证匹配指纹、实际止写、回收与发布 fencing。覆盖成功快照及遗留孤儿对象，核验保留集合。见 B8 |

## B 单机联合验收 {#production-acceptance}

| ID | 现有本地证据 | 仍需交付 |
|---|---|---|
| B1 | 48 项 PostgreSQL journal 回归 | 双 Coordinator 并发、未知提交、旧实例与 Worker 故障组合，证明恢复与退役的原子性 |
| B2 | 4 项 HTTP/PostgreSQL 回归，正常状态及等待零次 journal 借用，显式历史单次 | 跨进程等待、认证转发、断连/重连、停机和取消压力，验证尾延迟、准确终态与实际排空 |
| B3 | 已覆盖阻塞提交时的容量占用与恢复 | 同机多进程固定到达率测试，叠加阻塞目录、慢对象存储、JNI/大请求和终态报告压力。报告排队、拒绝、超时和实际归还 |
| B4 | 6 项 Planner 回归，已有局部归约实现和历史双 Worker 对照 | 在同源真实 SQL 量化 identity projection、局部聚合的 CPU、首批/完成时延、实际交换量和跨 Worker 影响 |
| B5 | 43 项 PostgreSQL 持久交换回归，已有单/双输入合并对照 | 结合 A10 验证更多输入、 Join/NULL/重复/重试/不同代次、倾斜、混合负载、阈值与尾时延 |
| B6 | 66 项 Java Profile 与 Native 回报合同回归，已有本地中途断连证据 | 在最新部署产物跑实际 Flight、慢读/断连、失败 EOF 和同机长稳。验证独立客户端阶段、各层内存上界与全资源实际释放。对涉及 Native 所有权或并发的改动，使用当前主机和工具链支持的 sanitizer 跑受影响路径 |
| B7 | 40 项本地 PostgreSQL/MinIO 回归 | 本机多实例复跑热点并发、慢消费者、并发读写、租约切换、物理清理和每流/每窗口 SQL。覆盖已关闭窗口及保护时间内在途 I/O |
| B8 | 23 项 PostgreSQL 写入回归 | Iceberg/Paimon/Lance/COPY/CDC 的同表与异表高并发、未知提交、崩溃与接管、重放及实际资源归还。用本地夹具的期望结果或连接器 SDK 读回核对已提交数据 |

## 仍需跑通的专项与设计门槛 {#focused-probes}

- **A1/A4/A6 控制面与目录。** 复查跨表初始读准入、目录指针和大小写回退。分别计量授权、命名空间/表枚举、统计 SQL 和 manifest I/O。覆盖别名、CTE、视图、歧义、其他连接器具名快照、移动引用、到期、同名表重建、撤权与目录变化。两个独立 Coordinator 对照物化对象发布、暂停、删除、名字复用、重连和缓存淘汰，核对租户、发布代次与快照 pin。短查询采样 singleton 退化、冷启动、早期 ACK 和 Native 创建，依据实际路径评估安全的阶段合并。测潜在的锁内 future 回调、全局任务身份锁、并发大计划/票据扇出和 JNI 编解码。仅在 JFR 或流量证据表明跨查询阻塞、重复传输或持锁成本显著后，才调整锁、回调或票据布局。
- **A5–A9 扫描。** 验证 split 枚举、索引过滤的位置、文件打开、字节布局、实际解码和读取放大。按固定快照、约束、schema、授权和读版本对照 row group/page、冷热、宽窄、随机和删除文件。只有并发重复发现相同 manifest 成本明确时，才评估按身份共享加载，保持请求各自的 pin 和取消语义。缓存亲和依据实际命中与驻留，预取受字节预算限制。
- **A10/B4 统计、Join 与聚合。** 给 EXPLAIN 和执行 Profile 对照每算子的实际行/字节、估计误差、统计来源、物理分布及 singleton 原因。核对高频值、NDV、相关性和选择性导致的真实误判，再扩展统计或 Join 选择。广播计入每 Worker 内存与复制成本。验证分组 DECIMAL、混合可分解函数、DISTINCT 和 GROUPING SETS，区分语义证明、成本门槛与实测收益。Storage Partition Join 只有在两侧分区 spec、变换、键类型、哈希和 NULL 语义有兼容证据时才评估。
- **A11/B6/B7 数据面。** 区分建流、首包、队列、每次 push、实际关闭与跨流饥饿。若慢 reader 持有线程成为主因，评估有界预留、分帧或就绪回调。若对象次数或锁等待主导，才以相同总字节对照有界 segment。若出口带宽饱和、消费持有资源主导或单出口是显著瓶颈，再评估并行 endpoint、范围有序合并或 spooling。验证 ORDER BY/LIMIT、重复/遗漏、EOF、取消、撤权、内存/苏醒和存储回收。现有代码先分别归因对象 I/O、解码、Velox CPU、缓存、reclaim 和 spill。
- **A12/A13/B8 写入和混合负载。** 测 COPY 固定版本暂存、COW 命中与幸存行、同表热点、实际文件尺寸/数、写放大、catalog/commit/journal 锁等待、CDC source lag/offset 确认。单列后台 compaction、索引、检索和物化刷新对前台 P95 的影响，检索需保留 recall/Top-K 正确性。
- **A4/B3–B8 单机计量。** 用固定本机资源、同一数据快照和相同 SQL 对照基线与候选。可用 SF1 或现有固定规模夹具，覆盖点查、扫描、Join、聚合、排序、宽结果、写入和故障。记录客户端完整消费、P50/P95、吞吐、拒绝/超时、CPU、读写量、峰值内存和从 EOF 到物理退出。阶段百分位分开量测，样本足够时报告 P99。

## 运行合同与关闭门槛 {#hard-gates}

1. 正常状态等待、重连和诊断投影零 journal SELECT。显式历史、未知提交和接管限在对应权威边界。正常持久 journal 新事件以单条 INSERT 完成，无独立读回。目录、授权、统计和分片枚举按实际 SQL 与连接器调用分别计数，逐项消除同一权威版本的重复读取。
2. Shuffle 建流、续租、关闭及恢复可访问权威库。数据面 batch/chunk 零同步授权 SQL，生产按有界窗口预留和发布。对象身份、序号、覆盖、摘要、owner epoch 和已接纳 attempt 必须可证明。
3. 从入口到结果区分 ACK、Ready、首末批、EOF、逻辑终态和物理排空。每项发布和实际清理都需要精确身份与证据。旧 owner 不得确认新代次状态，未知目录提交不能推定成功或提前删除。
4. 同一版本交付 Coordinator、Worker、Native、Shuffle Service、CLI、JDBC 和 benchmark。候选使用独立测试 schema 和对象前缀。每条生产路径保留唯一权威入口，删除退役调用、配置和注册后用实际入口复测。协议、JNI、descriptor golden 和模块规则按仓库要求验证。把复测命令、commit、运行环境、原始日志和结果摘要写入此文件或链接到可复核产物。按实际覆盖关闭并移出对应待办。纯更名批次记录声明、运行注册和合同等价证据。

## N Java 类型与包更名 {#java-naming}

Java API 和 SPI 在同一版本统一切换，仓内声明、实现、消费者、资源和测试随所属批次一起更新，外部 Java 插件使用新的全限定名重新编译并与 Coordinator、Worker 同批部署。纯更名以模块归属、Maven 坐标、`Automatic-Module-Name`、SQL、HTTP、Protobuf、JNI 和持久化行为等价为验收基准。

原清单的 `protocol.rest.task.convert` 已随 `83a27ff60` 删除，其转换器和测试由 canonical `SubmitTaskRequest` 及现有对称性测试承接。该条纳入回归覆盖，其他 22 个源包逐项实施。

### 实施批次 {#java-naming-batches}

| 批次 | 变更范围与关闭条件 |
|---|---|
| N1 · Common、JDBC 与策略持久化 | 按下方分包和类型映射替换基础声明及直接调用方。更新 `SharedPackagePolicy` 中 `ThreadSafe` 和 `NotThreadSafe` 的精确白名单，验证 `PluginClassLoaderTest` 测到应用类路径真实存在的 `common.validation.Preconditions`。验证 JDBC 回滚、时间绑定、schema 初始化和策略隔离。 |
| N2 · Parser、Planner、Protocol Java、Indexing、生成与 Connector | 按映射移动声明、测试和真实消费者。用生产入口验证 `StatementParser`、`SqlParser`、`ConnectorIndexCapabilities`、`SourceSnapshotPinner` 等涉及的解析诊断、计划、读取绑定、物化快照固定和写入回执。同步切换 SPI 文件路径、文件内容和反射入口。 |
| N3 · 启停、观测、AI、运维与测试夹具 | 更新 `TaskWrapper`、`MaintenanceLeadershipListener` 和各注册组件的实际调用链。按映射迁移夹具与选择器，保留包私有访问、断言和测试标签。验证 Worker 启停、维护领导权切换、Shuffle Producer 注册及资源归还。 |
| N4 · 模块契约与交付核对 | 每批同步调整同一 artifact 在 `.pangrova/modules.yaml` 的包根和导出，使用 `.pangrova/scripts/generate-module-baseline.py` 更新基线。逐项盘点 61 处声明、22 个旧包和 11 处保留声明。核对唯一全限定名、真实 SPI 加载和包权限。更新受影响文档和 `CHANGELOG.md` 的 `Unreleased`，记录插件同版本重编译。Maven 模块标识 `org.pangrova.persistence.jdbc.support` 继续有效。 |

同批更新文件路径、声明、构造器、导入、嵌套类型、调用方、反射和类加载字符串、`META-INF/services`、Maven 测试选择器、Checkstyle、SpotBugs、示例和文档。按旧全限定名和声明范围逐项核对，区分外部 SDK、协议名称和 Maven 模块标识。

### 类型更名清单 {#java-type-renames}

表中的目标名为新的 Java 简单名。类型保留在原包，涉及包迁移时以[目标包清单](#java-package-moves)为准。声明、构造器、嵌套类型的所属路径、调用方和直接以该类型命名的测试在同批更新。

#### 首批确认的 17 项

| 旧名 | 新名 |
| --- | --- |
| `EndpointRouting` | `EndpointSelector` |
| `TaskWrapping` | `TaskWrapper` |
| `MdcTaskWrapping` | `MdcTaskWrapper` |
| `SourcePinning` | `SourceSnapshotPinner` |
| `CoordinatorSourcePinning` | `CoordinatorSourceSnapshotPinner` |
| `MaterializationDefinitionPlanning` | `MaterializationDefinitionPlanner` |
| `PredictiveScaling` | `PredictiveScalingPolicy` |
| `WorkerObservabilitySetup` | `WorkerTelemetryInitializer` |
| `MaintenanceLeaderAware` | `MaintenanceLeadershipListener` |
| `MaterializationDdlRegistration` | `MaterializationDdlRegistrar` |
| `EmbeddingModelTaskRegistration` | `EmbeddingModelTaskRegistrar` |
| `NativeShuffleProducerRegistration` | `NativeShuffleProducerRegistrar` |
| `JdbcSourceObjectRegistration` | `JdbcSourceObjectRegistrar` |
| `StandardMetricsRegistration` | `StandardMetricsCollectors` |
| `NativeObjectStoreBindings` | `NativeObjectStoreBindingFactory` |
| `CatalogExecutionBindings` | `CatalogExecutionBindingFactory` |
| `TestDataSetup` | `TestDatasetLoader` |

#### 扩大范围后确认的 44 项

| 所属职责 | 旧名 | 新名 |
| --- | --- | --- |
| Common | `DigestUtils` | `Sha256Digests` |
| Common | `ThreadUtils` | `ThreadSleeper` |
| 类型转换 | `PangrovaTypeMapping` | `PangrovaTypeConverter` |
| AI | `AiEmbeddingInvocationSupport` | `AiEmbeddingInvoker` |
| AI | `AiInvocationResultSupport` | `AiInvocationResultNormalizer` |
| AI | `GovernedAiBindings` | `GovernedAiBindingProjector` |
| Protocol Java | `GovernedScalarEncoding` | `GovernedScalarDecoder` |
| 治理 | `ColumnTagBindings` | `ColumnTagAuthority` |
| 文件任务 | `FileSpaceTaskSupport` | `FileSpaceTaskAuthority` |
| Planner | `AuthorizedStageExchangeBindings` | `AuthorizedStageExchangeBinder` |
| Planner | `DecorrelationSupport` | `PlannerDecorrelator` |
| Planner | `DmlMergeJoinSupport` | `DmlMergeJoinRewriter` |
| Planner | `DynamicTablePlanningSupport` | `DynamicTablePlanRewriter` |
| Planner | `PlanningReadBindings` | `PlanningReadBindingResolver` |
| 生成执行 | `FrozenStageBindings` | `GenerationStageTemplateCodec` |
| 生成执行 | `GenerationPartitionBindings` | `GenerationPartitionBinder` |
| Protocol Java | `DeclaredSqlBindings` | `DeclaredSqlBindingIndex` |
| Protocol Java | `NativeWriteBindings` | `NativeWriteBinder` |
| Protocol Java | `PhysicalFileWriteBindings` | `PhysicalFileWriteBindingFactory` |
| Protocol Java | `SourceSnapshotBindings` | `SourceSnapshotCodec` |
| Iceberg | `IcebergReadBindings` | `IcebergReadBinder` |
| Iceberg | `IcebergFileReadBindings` | `IcebergFileReadSelector` |
| Iceberg | `IcebergManifestReadBindings` | `IcebergManifestReadSelector` |
| Iceberg | `IcebergPartitionReadBindings` | `IcebergPartitionReadSelector` |
| Paimon | `PaimonReadBindings` | `PaimonReadBinder` |
| Paimon | `PaimonBranchReadBindings` | `PaimonBranchReadBinder` |
| Paimon | `PaimonFilesReadBindings` | `PaimonFilesReadBinder` |
| Paimon | `PaimonSystemReadBindings` | `PaimonSystemReadBinder` |
| Connector SPI | `ConnectorIndexSupport` | `ConnectorIndexCapabilities` |
| Paimon | `PaimonConnectorIndexSupport` | `PaimonIndexCapabilities` |
| JDBC | `JdbcSupport` | `JdbcOperations` |
| 索引持久化 | `JdbcStoreSupport` | `JdbcIndexMetadataStoreBase` |
| 策略持久化 | `JdbcPolicyStoreSupport` | `JdbcPolicyStoreBase` |
| Catalog 持久化 | `PostgresCatalogPersistenceSupport` | `PostgresCatalogPersistenceBase` |
| 生成任务持久化 | `PostgresGenerationJobStoreSupport` | `PostgresGenerationJobStoreTransitions` |
| 授权持久化 | `PrivilegeMutationEncoding` | `PrivilegeMutationEncoder` |
| 测试 | `FragmentTestSupport` | `PlanFragmentFixture` |
| 测试 | `VectorIndexResilienceE2ETest.MultiMetricSupport` | `MultiMetricIndexCases` |
| 测试 | `PaimonCommitTestSupport` | `PaimonCommitFixture` |
| 测试 | `PaimonDeleteTestSupport` | `PaimonDeleteFixture` |
| 测试 | `PlanExecutionFactoryDeleteTestSupport` | `DeleteDispatchFixture` |
| 测试 | `RegistryTestSchemaHelper` | `RegistryPostgresFixture` |
| 测试 | `TpcHTestSupport` | `TpcHResultAssertions` |
| 测试 | `WriteTestHelper` | `SqlTestRowWriter` |

#### 保留的 11 处声明

- 数据映射和领域绑定保留 `BuildFileMapping`、`ColumnKeyMapping`、Java 和 Protobuf 各一处 `ColumnMapping`、`GroupRoleMapping`、`IndexFileMapping`、`FunctionSignatureBindings`、`TablePolicyBindings`。
- 编码契约保留 `GeneratedColumnSourceEncoding` 和 `ValueEncoding`。
- 现有测试场景保留 `PartitionKeyMapping`。

### 包迁移清单 {#java-package-moves}

以下包名均省略 `org.pangrova.`。整包迁移时保持生产源码与对应测试的包归属，同步应用到既有子包。Common、JDBC 和 E2E 夹具的分包位置见下方分配表。

| 旧包 | 目标包 |
| --- | --- |
| `parser.api.parse` | `parser.api.syntax` |
| `parser.parse` | `parser.syntax` |
| `planner.parse` | `planner.sqlnode` |
| `indexing.strategies.normalize` | `indexing.strategies.normalization` |
| `common.concurrent` | `common.concurrency` |
| `common.property.concurrent` | `common.property.concurrency` |
| `ai.worker.provider.governed` | `ai.worker.provider.governance` |
| `persistence.generation.api.generated` | `persistence.generation.api.generatedvalue` |
| `persistence.ai.postgresql.assembly` | `persistence.ai.postgresql.controlplane` |
| `persistence.security.postgresql.support` | `JdbcPolicyStoreBase` 及其测试到 `persistence.security.postgresql.policy`，`CanonicalSecuritySchemaTest` 到 `persistence.security.postgresql.schema` |
| `coordinator.query.execution.dispatch.adaptive` | `coordinator.query.execution.dispatch.adaptivequery` |
| `planner.adaptive` | `planner.adaptivequery` |
| `planner.api.adaptive` | `planner.api.adaptivequery` |
| `planner.property.adaptive` | `planner.property.adaptivequery` |
| `testing.integration.e2e.adaptive` | `testing.integration.e2e.adaptivequery` |
| `coordinator.query.execution.dispatch.task.semantic` | `coordinator.query.execution.dispatch.task.semanticview` |
| `coordinator.semantic` | `coordinator.semanticview` |
| `planner.incremental` | `planner.incrementalquery` |
| `common.util` | 按 Common 职责拆分 |
| `common.property.util` | `common.property.<对应职责>` |
| `persistence.jdbc.support` | 按 JDBC 职责拆分 |
| `testing.integration.e2e.setup` | 按 E2E 夹具职责拆分 |

#### Common 分包

| 类型 | 目标包 |
| --- | --- |
| `DataSize` | `common.units` |
| `Sha256Digests` | `common.crypto` |
| `NumberConversions` | `common.numeric` |
| `OrderedMaps` | `common.collection` |
| `Preconditions` | `common.validation` |
| `SqlText`、`Utf8Order` | `common.text` |
| `Stopwatch` | `common.time` |
| `UriComponents` | `common.net` |

普通测试与被测类型同包。属性测试进入 `common.property.units`、`common.property.crypto` 或 `common.property.validation` 等对应职责包。

#### JDBC 分包

| 类型 | 目标包 |
| --- | --- |
| `DriverManagerDataSource`、`PooledDataSourceFactory` | `persistence.jdbc.datasource` |
| `JdbcOperations`、`JdbcRollbackTest` | `persistence.jdbc.operation` |
| `SchemaMigrationRunner` | `persistence.jdbc.migration` |

#### E2E 夹具分包

| 类型 | 目标包 |
| --- | --- |
| `TestDatasetLoader`、`TestDatasetLoaderTest` | `testing.integration.e2e.fixture.dataset` |
| `MetadataSchemaInitializer`、`MetadataSchemaInitializerTest` | `testing.integration.e2e.fixture.schema` |
| `TestCatalogRegistrar`、`TestConnectorHandleFactory`、`IcebergCatalogFactory`、`TestConnectorFactory`、`TestDatasetLoaderIcebergTest` | `testing.integration.e2e.fixture.catalog` |

让 `TestDatasetLoaderIcebergTest` 和 `IcebergCatalogFactory` 保持同包，以保留工厂的包私有可见性。

## 更名验收与统一验证 {#local-naming-validation}

| 层级 | 验证要求 |
|---|---|
| 每批验证 | 使用 JDK 17、Maven 3.9+ 和 Docker。先跑 `mvn -q -pl <受影响模块> -am test`，再覆盖直接消费模块。记录测试数量、失败、错误、跳过及对应提交。 |
| 基础行为与隔离 | 覆盖摘要字节、任务上下文、并发、JDBC 回滚、已有数据的 schema 初始化、权限策略和插件隔离。运行 `PluginClassLoaderTest`、`ServiceLoaderReachabilityTest` 及相关属性测试。 |
| SQL、协议与启动 | 运行 `SqlCompilerSessionIntegrationTest`、`SubmitTaskRequestSymmetryTest`、`SpiWiringVerificationTest`、`BootstrapReachabilityTest`、`AdaptiveQueryExecutionE2ETest` 和 `ClusteringE2ETest`。覆盖解析、任务契约、Coordinator 启动、计划执行和真实 Worker 暂存。 |
| Worker 与跨语言 | 使用同一提交构建并打包的 Native 库，运行 `NativeCapabilityHandshakeE2ETest`、Shuffle Producer 注册测试及适用的 Native owner 独立 JVM 用例。验证能力握手、写入绑定、注册和释放。 |
| 集成与验收 | 清理旧 Java `target` 后重建。运行全 reactor 日常测试和 `mvn -q -DskipTests verify -Pstatic-analysis`。按 `.pangrova/rules/build-pipeline.md` 执行本地生产回归、Activation Smoke 及独立 Native owner 用例。使用同一构建验证 B1–B8，复用范围一致的测试证据。 |
| 仓库检查 | 验证模块基线、SPI、文档类引用、Protobuf descriptor 和开发产物。逐项扫描旧全限定名及其路径、导入、类名字符串、服务资源和测试选择器。 |

```bash
bash .pangrova/scripts/check-module-manifest.sh
bash .pangrova/scripts/check-spi-annotations.sh
bash .pangrova/scripts/check-doc-class-refs.sh
bash .pangrova/scripts/proto-descriptor-diff.sh
git diff --check
bash .pangrova/scripts/clean-dev-artifacts.sh
bash .pangrova/scripts/check-dev-artifacts.sh --strict
```

用相同 SQL、快照和本机条件区分基线失败、重命名回归与行为回归。测试结果按实际情况记录通过、失败、跳过或阻塞及原因。

## 交付与回退 {#delivery}

按仓库规定将同一提交集合交付到一个 PR，记录变更清单、提交、复现命令、测试与日志、同机基线和候选的差异。确认主线是否已前进并复测受影响的范围。按仓库 CI 和分支保护完成合并后，从同一版本重建 Coordinator、Worker、Native bundle 和插件，并验证插件加载及关键读写链路。发生回归时按依赖逆序回退受影响的完整提交批次，使用回退后的同一提交重建并复验。

## 单机 Native 构建与验证 {#native-qualification}

一般构建、依赖固定、分层缓存、Lance/Java 夹具和清理按仓库构建规则执行。B6 使用同一提交的本机部署包，校验来源、测试报告和包身份。Release 与本机支持的 sanitizer 分别使用合格产物，按受影响范围运行 CTest 和严格诊断扫描。重建时保留源、工具链、部署目标、库、patch 和二进制的 SHA，记录实际加载的运行时。冷构建仍需实测磁盘和内存容量。

### Darwin LLVM 功能检查与 Folly {#darwin-function}

历史 macOS arm64 LLVM 17.0.6 / Folly v2026.01.05.00 资格结果只适用于实测产物。重做该组合时，使用新输出目录生成私有 LLVM，保留已安装编译器和运行时。验证实际加载、Mach-O `.alt_entry` 和有效调用，要求错误函数调用及堆越界对照能被报告。记录生成的 `build-evidence.json` 和 `canary-evidence.json`。使用私有编译器构建带锁定源码修复的 Folly，仍启用 `FOLLY_LIBRARY_SANITIZE_ADDRESS=ON` 与 `AsanFollyCompat.cmake`。

```sh
python3 pangrova-worker-native/bin/build-darwin-llvm-function-prefix.py \
  --llvm-prefix "$PANGROVA_HOME/llvm" --output "$PANGROVA_HOME/llvm-function-prefix-17.0.6"
python3 pangrova-worker-native/bin/build-darwin-asan-folly.py \
  --folly-source "$VELOX_HOME/deps-download/folly" \
  --compiler-prefix "$PANGROVA_HOME/llvm-function-prefix-17.0.6/toolchain" \
  --prefix-path "$VELOX_HOME/deps-install;$PANGROVA_HOME/homebrew" \
  --openssl-root "$PANGROVA_HOME/homebrew/opt/openssl@3" \
  --output "$PANGROVA_HOME/qualified-asan-folly" --jobs 6
export PANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_HOME/qualified-asan-folly/install"
```

Native configure 显式传入该 Folly prefix 和匹配插桩的 Protobuf/Abseil。资格证据包括 `FollyIntegerIntentControls.cpp` 的算术对照及完整 Native CTest，严格检查地址、函数类型和整数诊断。现行 macOS gate 使用 `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`。

### Darwin arm64 TLS 和 LeakSanitizer {#darwin-tls}

若本轮需要资格化 Darwin LeakSanitizer，使用与 LLVM 17.0.6 匹配的 `CC`、`CXX`、`LLVM_CONFIG` 和 CMake 3.28.x，运行 `bash pangrova-worker-native/bin/build-darwin-asan-runtime.sh /new/private/runtime-prefix`。使用私有 runtime 和校验过的 release 源，核对 `otool -L` 与实际加载证据。涉及安装名或签名只处理私有测试产物。

- 在 `detect_leaks=1:halt_on_error=1` 下，用 `DarwinTlsRuntimeControls.cpp` 验证真实 TLS 根、清除根后可检出的 193 字节泄漏、use-after-free 和 heap overflow。模式 0/1 必须正常清理，模式 2–4 必须得到对应诊断，机器的 pthread 最末 key 与校准预期匹配。
- 用 `RuntimeErrorDiagnostic.cpp` 对照异常创建、复制、抛出、重抛和自然退出的线程，在 1、32、4096 次迭代运行模式 0、1、2、3、4、8。留存原 runtime 的对照结果。
- 先通过完整 23 项校准控制，再以该 runtime SHA 和相同源码/依赖完整跑 Native CTest。分别保留 macOS 常规 gate 和额外 LeakSanitizer 运行证据。源码、Lance、ANN 或函数组合变化后重新核验受影响的产物。

## 新开任务 Prompt {#continuation-prompt}

```text
在 Pangrova 仓库实施 docs/development/execution-backlog.md 的全部剩余工作并完成验收。当前文档在 codex/consolidate-execution-backlog。先在当前 worktree 检查分支和本地改动，从包含此文档的提交接续并同步最新 origin/main。使用一个实施分支和一组可独立审查的提交。读取 AGENTS.md 和 .pangrova/rules/ 中与变更范围有关的规则。

1. 冻结同机基线并更新实施清单。逐项核对 A1–A13、B1–B8、N1–N4，检查源码、生产入口、调用方、测试和已有证据。核对 61 处类型、22 个待迁移的旧包和 11 处保留声明。对上游已实现的条目复核证据后关闭。记录起始 commit、部署拓扑、SQL、数据快照、资源、基线时延、SQL 次数和 I/O 观测。
2. 按依赖并行推进，先完成代码再做统一验收。先收口 Common、JDBC、策略持久化，同时进行性能取证。命名迁移与行为修复分提交，同一模块、协议或持久化权威的改动按依赖串行集成。更名批次同步更新声明、路径、真实消费者、SPI 注册、类加载器、模块包根、测试选择器和文档。仓内 Java API/SPI 按同版本直接切换，外部插件同步重编译。
3. 从客户端提交到资源实际释放逐段验证。覆盖 HTTP 鉴权、幂等、租户准入、语法/目录/快照/策略/优化、Substrait、split 枚举与放置、Worker ACK、JNI/Velox 任务与文件读写、Shuffle、OUTPUT、Native Flight、客户端末批和 EOF、逻辑终态、实际物理排空。写入继续覆盖文件片段、journal、fenced commit、快照可见、CDC 回执与 offset。先用计时、JFR、数据库计数和 I/O 归因，再定位重复编解码、复制、过渡层与重复读库，按清单门槛实施并清理退役路径。
4. 守住权威边界。正常状态等待、重连和诊断零 journal SELECT，新 journal 持久事件单条 INSERT，Shuffle batch/chunk 零同步授权 SQL。按路径记录授权、目录、统计、回收和异常恢复所需 SQL。保留精确授权、快照、owner/epoch、attempt、fencing、背压边界和释放证据。使用真实调用方验证协议和 JNI，从两端一起处理有依据的变更。核对 descriptor、SQL、HTTP 和持久化合同。
5. 批次内先跑定向回归，所有代码收齐后统一验收。使用同一提交构建 Coordinator、Worker、Native、Shuffle Service 和客户端，以同机独立进程或容器使用 PostgreSQL、MinIO 和 Flight。跑 B1–B8、命名验收矩阵、长稳、适用的 sanitizer、实际物理排空和全链路回归。固定本机资源、数据与负载，对比首批、完成、排空时延、P50/P95、吞吐、拒绝、SQL/I/O、CPU 和内存。对照最新版本的结果正确性与故障恢复。
6. 每批记录实际运行的命令、commit、环境、数量、状态、产物和日志。对未通过项重现并修复，满足对应验收标准后关闭，在计划中记录证据并移出待办。定期清理可再生的开发产物。把同一提交集合整理为一个 PR，按仓库规则完成发布所需验证和交付。持续推进到所有本地验收项关闭，遇到具体外部依赖或授权阻塞时记录边界并继续独立工作。
```
