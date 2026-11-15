---
title: "模块重构处置台账"
description: "维护已核查的生产调用、类归属、事务与资源边界"
---

# 模块重构处置台账

本表记录已沿生产入口追踪的类。模块 manifest 记录整个 reactor 的依赖和包归属，无法证明某个接口已被装配或某条路径支持恢复。尚未逐类核查的模块继续列为待审。

按 2026-10-24 的 `b51114d0`（首次拆分 Coordination 权威之前）和当前工作树对照，可导出每个 Java 生产顶层及成员类型的原路径、目标路径、制品和 FQN。命令会纳入未提交的新源码。重名匹配仅作为审查线索，拆分、合并、运行时调用及测试证据由下表记录。JDK 17 解析实际声明，同文件中的附属类、成员枚举和记录各有独立条目。局部类与匿名实现随包含它们的方法审查。

```bash
python3 .pangrova/scripts/export-class-inventory.py --base b51114d0 --output target/architecture/class-dispositions.tsv
```

## 16 项要求的验收范围（2026-11-14）

本次对工作树中 153 个 Maven 项目复核，包含 130 个代码或资源制品和 23 个聚合项目。表中将已验证的仓库结构与仍需发行验证的行为分别记录。

| 要求 | 已核对的证据 | 尚待验收 |
|---|---|---|
| 1、2 单子模块与局部契约 | POM 扫描未发现单子模块聚合目录。现存 28 个 `-api` 均有领域外制品直接声明非测试依赖。下文“按实际调用方复核契约制品”记录调用方及其语义。 | 领域外依赖能说明当前消费者，进一步合并仍须检查包级职责与循环。 |
| 3 多实现和工厂 | 生产服务描述符含 24 种服务契约、49 条注册，分布在 30 个制品。AI 有 OpenAI 和 Qwen 两个工厂，Connector 可从进程插件目录加载。旧 TableWriter 批写层级没有生产工厂、消费入口或关闭 owner，已随两个 writer 一起退役。 | 下文“进程服务登记与实际调用”逐项列出注册、选择、资源和验证范围。Java spool、表的内存实现和厂商线上环境按其实际支持范围验收。 |
| 4–7 平铺、契约名称、客户端 | Parser、Batch、File、Planner、Query、Semantic、Table、Worker 各由领域 POM 直接构建。Java 契约使用 `.api`。Client 实际子制品为 HTTP、CLI、JDBC。 | JDBC 取消、CLI 超时在双 Worker 发行物已运行。后续客户端资源回收变更需用对应制品复验。 |
| 8、9 客户端共用契约与 JSON | 共享客户端契约在 `common.client`，JSON 在 `common.json`，POM 中已移除原独立制品。 | 复查最终发行物的运行依赖与旧类。 |
| 10、14 七个重点领域和参考设计 | Scheduler 按 policy/strategies，Streaming 按 pipeline/source/format/writer，Metadata 按 catalog/codec，Materialization 和 Indexing 按 lifecycle/strategies，Generation 按 workflow/codec，Execution 按 binding 划分。七者的外部契约独立发布。ADR 记录 Paimon、Iceberg、Trino、SeaTunnel 相关源码和取舍。全仓 Maven 制品已无 `-core`。 | 在同源、按角色装配的发行回归中验证这些边界。 |
| 11、12 包职责、命名和孤立能力 | 包图按真实 Java 引用验收。Catalog ALTER/DESCRIBE 的 SQL 调度、版本与权限已补入工作树。查到两份相同的 `ThreadSafe` 声明，已统一到 Common 的并发包。Coordinator 中无调用且指向不存在客户端的 `TransportClientConfig` 已清除。 | `USE BRANCH` 已从客户端连接走通到 Worker 的实际读取。下方记录了授权、拒绝和两种 Connector 的实测。 |
| 13 共用工具 | 文件签名、结果票据、Flight、Shuffle、AI 租户派生和 SQL 掩码均使用 Common 创建独立的 HMAC-SHA256 实例。各领域拥有签名内容、密钥派生、编码及校验。 | 快照 `a6b4ec203` 在 106 个项目的闭包中通过 117 项门禁与消费测试。独立签名提交 `a11a56341` 在 102 个项目的闭包中通过 78 项，零跳过。最终发行回归仍需验证同源制品。 |
| 15 枚举、异常、常量 | 命名声明门禁使用 JDK AST。它覆盖默认 reactor 的生产源码根、嵌套声明、直接异常父类型和纯常量持有类。 | 快照 `a6b4ec203` 的类型归属和 922 个包的无环检查通过。后续声明变更按相同门禁复查。 |
| 16 并行交付与生产验收 | 角色集成、Native 与客户端验证分别使用隔离目录。macOS arm64 Release、ASan 各通过 4333 项，首轮 CLI/JDBC 双 Worker 取消已验证。 | 同源全量回归、Linux 实际发行物、最终 classpath 和完整性能对照仍需独立证据。 |

在 PostgreSQL 回归中，Metadata 测试先把库停在 V73 写入旧数据，再运行到 V112，因旧记录缺少 source provider 而失败。新项目从空库初始化。现由 Streaming PostgreSQL 使用当前创建接口登记带有明确 source provider 的流、写入 offset 和提交回执，再删除、同名重建。删除清理缓存，旧 checkpoint 的发布证据仍可读取，同名新实例使用独立 checkpoint。epoch fencing 和状态往返两组属性测试随同迁回 Streaming PostgreSQL，Metadata 撤销三个测试依赖。该 Provider 在真实 PostgreSQL 上通过 17 项状态仓储测试及 5 项属性测试，零跳过。


截至本次核对，16 条要求中 1–15 已具备结构、门禁与实测证据；第 16 项仍缺 Linux amd64 角色发行物、ASan 全量、最终性能/资源对照和一次全门禁同源全绿。历史 SPI 例外从 7 项降至 5 项，均有生产入口或独立库证据。这里的编号对应全仓模块化要求。

| 编号 | 剩余交付与验收证据 |
|---|---|
| 3 | 可替换能力的配置、工厂、角色装配、调用与关闭由强类型工厂/SPI 统一；`SpiWiringVerificationTest` 6 项与 `ServiceLoaderReachabilityTest` 6 项在最终源码上通过；TableWriter 双实现因无生产工厂与消费入口整体退役。 |
| 11 | 工厂迁入 provider 后，910 个 Java 生产包、5219 条跨包引用、0 个循环分量；SPI 历史可达性基线 5 项均有生产入口或独立库证据；12 项 SPI 验证在最终源码通过。 |
| 12 | 包名、类名与职责复核落盘于本台账；机械扫描确认 common 之外无 util/utils、无 -core 与 spi 包，异常/常量/枚举均在独立包，无单子模块父模块；5 项历史例外逐项有入口或独立库证据。 |
| 13 | 全部共用工具归入 Common，模块专属工具在 common 下按模块归包；非 common 的 util/utils 为 0；签名等共用实现由 Common 统一创建实例，领域保留内容、密钥派生、编码与校验职责。 |
| 16 | 同源全量 Maven、Native ASan、Linux amd64 角色发行物、最终性能与资源对照仍未全绿。macOS Release 快照 `c61dc7785` 已通过 4335/4335 项。该轮 ASan 的 Native Shuffle 双向 TLS 全量运行出现握手失败，同一 ASan 用例单独运行 24 次通过，根因尚未确认。索引权限测试与冷缓存遍历的修复已推送（`fd262a4c2`），原 11 个 PostgreSQL 用例及 6 个缓存用例复验通过。 |

## 11 月 14 日发布与续跑记录

远端 `refeactor-modules` 已包含以下变更。未完成的工作树以 `codex/modularization-checkpoint-20261114` 保存。该分支用于交接，验证状态按下表分别记录。

| 范围 | 已有证据 | 剩余验收 |
|---|---|---|
| Query 与 SQL 文本 | `009fac5c4` 消除 31 个 SQL 处理器的重复列推导，48 个用例通过。`b85743ea4` 修复行注释导致过滤条件与无过滤查询碰撞，以及扩展被替换后仍能命中旧 AST 的问题。最终 Parser 套件 92 项、Planner 基线测试 12 项通过。 | 使用最终发行物复验 SQL 主路径。 |
| Metadata 与多实现 | `842848058` 中两个 Coordinator 使用同一 PostgreSQL，在源 I/O 前登记观察版本。旧观察既不能覆盖新投影，也不能推进 checkpoint 或写 outbox。24 项故障与并发测试通过。Cache 和 Federation 的独立候选已验证，SPI 历史可达性例外从 25 项降至 7 项，相关候选在 147 个项目的定向回归通过 71 项测试。 | 整合新的 Cache、租约工厂和 Metadata fence 后重跑角色发现、权限变更与恢复，逐项处置其余 7 项。 |
| Worker 与客户端 | `abccb48c7` 将 HashJoin 的 build 键域送到同任务的扫描列。`208471094` 修复 JDBC 取消、关闭与本地化列标签，242 项通过。`60b972fd4` 区分结果通知与 Native 释放回执，macOS 双 Worker 加 PostgreSQL 客户端回归通过。`e7f2c2f1b` 提供 24 条 SQL、60 个 Native 任务和 309 次操作系统采样的性能与资源原始记录。 | 双侧 exchange 候选的过滤发生在接收端，需验证真实结果及上游读量。Linux amd64 镜像及故障恢复用例仍待实跑。 |
| 第 11–13 项 | Java 整合快照 914 个包无环。SQL 解析和诊断基线共用文本规范化。工作树还把 33 个生产消费者的 SHA-256 格式判断收敛到 Common，并修正多个模块的默认 Locale 依赖。 | 按调用方完成剩余逐类职责审阅，执行摘要、厂商协议与土耳其语环境的实际回归。 |
| 第 16 项 | 早期 macOS Release 快照 4335/4335。对应 ASan 全量运行出现一次真实的 Shuffle mTLS 握手失败，原用例单独重复 24 次通过。 | 工作树已显式要求独占 gRPC 监听端口。原实现也能通过单端口冲突用例，尚不能以该用例断定握手根因。用新二进制重跑 Release、ASan、Java/Native 与各角色发行验证，并完成同源全量回归。 |

截至本轮收尾，Cache 与 Coordination 工厂整合候选 `5cf0d2fa0` 已完成 148 个 Maven 项目的定向回归。Coordinator 租约初始化 5 项、后端选择与失败行为 5 项、SPI 装配 5 项、ServiceLoader 可达性 6 项全部通过。该候选基于 Metadata 观察版本变更之前的源码。含观察版本、Cache 和剩余可达性例外变更的整合候选 `991b7b07c` 仍在验证。

macOS arm64 源码快照 `c61dc7785` 的 ASan 全量运行已结束。4335 项中 4334 项通过，唯一失败为 `NativeExchangeDataPlaneE2ETest.MutualTlsTraversesNativeConsumer`，客户端连接阶段返回 `SSL_ERROR_SSL: wrong version number`。同一快照 Release 4335 项通过。独占监听端口改动在后续检查点，须用重建后的二进制复验。Linux 镜像尚在构建，真实容器重启用例等待匹配的不可变镜像 ID。

恢复从检查点建立独立工作树，先运行 `.pangrova/scripts/check-module-manifest.sh`。在 JDK 17、真实 PostgreSQL 和仓库规定的 Native 与 Iceberg 前置条件下，执行 `mvn -Pnative-runtime,full-regression -fae verify`。Linux 重启测试消费已构建镜像的不可变 ID，核对其 OCI 源码 revision，并验证真实 journal 和任务回执。不同源码快照的通过项分别保留。

## 11 月 14 日追加验证

- 当前源码（`85e48e618`）的生产包图扫描 910 个包、5219 条跨包引用、0 个循环分量。SPI 历史可达性基线现为 5 项，均已有逐项生产入口或独立库证据。
- 本轮从 `67d0f9100` 续跑到 `9ecbb6aaa`：补齐 Cache/Connector 的 SPI 稳定性注解与 Common 注解依赖；修复 proto golden、文档类引用门禁和协议描述符；缓存/元数据候选 `1295489229` 的 8 个测试类 BUILD SUCCESS；退役无生产工厂/消费入口的 TableWriter 批写管线及 `streaming-writers-builtin`，SPI 历史豁免降为 5 项；BranchSelection 改由 Query API 拥有；修复 Coordination/Server 的潜伏断言和 DAG 单一完成权威场景；TPC-DS q08 黄金计划按 5 次冷 JVM 一致计划刷新。
- 同源 Java 验证：152 个项目 reactor 中 142 个模块（含 Planner、Connector Lance、全部 Metadata/Cache、Client）通过；`-Pnative-runtime` 恢复的 Worker/Coordination/Server/Standalone 链中，Coordination 2610 项、Server 647 项在修复后通过。集成模块 2218 项剩余失败均为下述显式能力或发行前置。
- 同源 Native：包含递归 CTE、空间函数与 GEOGRAPHY 构造器的 macOS arm64 Release CTest 4336/4336 通过（627.18 秒，EXIT=0），库 SHA-256 `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa`；`-Pnative-runtime` 分类器从同一暂存字节安装并校验 SHA-256 一致；新增 `pangrova_native_spatial_objects` 与 `parseSubstraitPlan` 解析预算均进入该同源构建。
- 递归 CTE 执行链已实现：Substrait 没有 RepeatUnion，`RecursiveCteExpander` 在解析前把顶层 `WITH RECURSIVE ... UNION ALL` 展开为 anchor 加 15 个递归层，每层是普通查询并走完整 Native 路径，最终 CTE 为各层 UNION ALL；`UNION`（去重）、SEARCH、CYCLE 及多递归 CTE 以明确规划错误拒绝。嵌套子句会超过 protobuf 默认 100 层嵌套，Java `SubstraitPlanCodec` 与 Native `parseSubstraitPlan` 统一使用 1024 的有界递归预算。真实 Iceberg 表上 `AdvancedQueryFeaturesE2ETest$WithRecursive` 2/2 通过（序列 5 行、层级 4 行），Planner 7 项测试通过（含 Substrait 解析）。空间函数由 Native `st_geomfromtext`、`st_geographyfromtext`（WKT POINT/POLYGON 转 WKB，支持多环与洞）和 `st_within`/`st_intersects`/`st_contains` 实现；GEOGRAPHY 与 GEOMETRY 共用 WKB 表示并按平面坐标求值，测地语义在文档中明确为未实现。`SpatialFunctionsE2ETest` 4/4 通过真实 Iceberg 表与 WKB 列。Native 边界门禁已把 `spatial` 登记为独立源组，418 个文件、878 条局部包含、0 环通过。两者都保留显式拒绝：非 POINT/POLYGON 几何、空/未闭合环、以及 SEARCH/CYCLE 不做伪造结果。
- 同源全量 Java（`f5dbecc87`）：152 项目 reactor 的全部 Java 模块通过；最终源码的共享 E2E 运行保留 `native-runtime-owner` 隔离并使用真实 AI/FILE fixture，执行 2224 项、0 失败、7 错误、2 跳过，0 次 WORKER_BUSY。7 个错误全部来自未提供的发行输入：5 个独立进程用例缺 `pangrova.e2e.local.*` 暂存，`CoordinatorRestartChaosTest` 缺 `pangrova.e2e.image`；`IndexDdlLifecycleE2ETest$AddColumnWithIndex` 在整轮高负载下出现一次 Iceberg v3 元数据重复提交，单独复跑该类 14/14 通过。错误处置基座已先取消原生任务再等待终态最多 2 秒，完整 ErrorHandlingTest 15 项和取消/准入序列 16 项均通过。`GovernedFileSqlE2ETest` 6/6（真实 MinIO/S3 与本地生成的 capability key）、`LocalGovernedFileSqlE2ETest` 4/4、`IndependentLocalProcessesE2ETest` 1/1、`IndependentClientCancellationE2ETest` 1/1、`IndependentLocalDiskShuffleE2ETest` 1/1、`IndependentSpeculativeExecutionE2ETest` 10/10、`IndependentAiProcessesE2ETest` 1/1 均在本地暂存上通过；暂存由 `deploy/docker/stage-role-classpaths.py`、同源 Release dylib（SHA-256 `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa`）和 1105 项 `runtime.sha256` 组成。独立 AI 用例的审计断言读取生产 `query_audit_records` 表，而不是只有内存存储才写的 `pangrova.audit.query` 日志。
- 最终 Native 库 `3c43f95f5c4d8af0c6473a109b46add7b4f11fad9b1f2de1126f411cad21c9fa` 重新绑定本地暂存后，分类器 JAR、worker JAR 与 staging 中的 native 条目 SHA-256 三者一致，`IndependentLocalProcessesE2ETest` 1/1 通过（126.9 秒）。
- 仍未通过的同源集成项：`CoordinatorRestartChaosTest` 需要 `pangrova.e2e.image` 的 Linux amd64 OCI 镜像；`SharedAiProviderDeploymentIntegrationTest` 4/4 通过；其 hosts 断言改为查询容器实际添加的 IPv4 映射（镜像中 IPv6 DNS 会先于 IPv4 hosts 返回，原 `getent hosts` 观察不到该映射）。ASan 全量仍未执行：既有 ASan Velox 归档的 provenance 记录旧补丁哈希 `6799535a1bdf87ce9af3f559546dbbe10ef244c210f3a19f52cf6fbf56bbeadb`，当前补丁为 `7242efda7c67823843cd45444254147a054b95f101c0de9c4edd28322f448372`，需重建 ASan Velox 后才能做同源 ASan。性能/资源对照已记录 24 条 SQL、60 个 Native 任务、309 次操作系统采样以及 Shuffle 读取与 JNI 路径；该批样本来自前一发布版本，最终二进制上的增量对照尚未单独复跑。
- 旧 TableWriter 批写管线退役。TableWriter、WriteRecord、WriteContext、DataFileSink、WriteMode、TableFormat，以及 Lake、merge-tree、incremental writer 和 StreamingWriter、CdcConsumer、LateDataRouter、StreamingWiring 只有测试构造点，没有生产工厂、依赖注入或消费入口。真实 CDC 路径保持 CdcPipelineLoop → CdcTwoPhaseCommitter → ConnectorPageSinkCdcWriter；SQL 写入保持 Worker NativeWriteExecutor 与 ConnectorPageSink。模块、服务清单、公开文档、SPI 基线及回归断言同步更新。
- TPC-DS q08 的计划差异只是 customer_address 一侧被 INNER 等值连接及 20 个 ZIP 常量域蕴含的冗余 SEARCH 被消除。5 次冷 JVM 的计划逐字一致，连接仍保留完整常量域；黄金计划按该评审差异刷新。同一查询域的重复、NULL、前缀和非匹配样本核对 IN 列表与等值连接判定一致。
- Lance 48 项需要显式传入同源 `pangrova_lance_fixture_tool`。主工作树构建的 Native 源与 HEAD 逐文件一致（`src`、`cmake` 与 `CMakeLists.txt`），传入其绝对路径后 48/48 通过，JNI/SDK 使用同一批源码产物。
- AI 模型 registry 把实现 `AutoCloseable` 的厂商实例交由版本包装器释放。并发关闭等待同一个结果，关闭异常仍传给调用方。已推送 `0ed4ff20b`。
- 向量检索保留有正 OFFSET 的完整排序分页路径，零 OFFSET 仍可使用满足条件的索引。已推送 `7a374091f`。
- 归并连接的成本与 Substrait 生成复用一处键顺序判定。左右输入分别满足升序、NULLS FIRST 时才能省去该侧排序。47 个 Java 用例和 145 个 Native 用例通过。已推送 `94e71d6b8`。
- 实现成熟度由 `@ExperimentalImplementation(trackingIssue=...)` 标记，API 稳定性保留 `@Experimental`。JDK AST 门禁扫描生产源码中的具体方法体，包括 API 制品的 default 方法。7 个门禁反例与边界用例、28 个 CI 策略用例通过。已推送 `f522eceab`。
- 二进制 SQL literal 在 Parser 入口要求每字节两个 ASCII 十六进制字符，恢复诊断保留行列。Coordinator 的 DML 使用同一值类型解码。已推送 `ca083e4ab`。
- 快照 `43e081c4` 的 `SqlPrivilegeAuthorityE2ETest` 两项通过。在 PostgreSQL、认证 HTTP、Native 查询链路验证 GRANT 生效、REVOKE 后拒绝、变更回执以及未认证请求拒绝。首次独立运行未传预制 AI 测试镜像 ID，停在共享 fixture 初始化。补齐镜像 ID 后复跑，两项均执行且零跳过。该快照的全量回归单独记录。

Catalog 增量同步收到数据源的取消后直接退出。扫描线程在启动、重试退避、遍历变更或表、写完成记录等边界检查中断，并保留中断标志。已提交的投影仍以其原回执为准。已推送 `8ea8b1f09`。包含 PostgreSQL 恢复和游标测试的 44 个用例通过，新增用例核对源端只被调用一次、没有启动全量扫描、没有记录完成。

文件审计已推送 `98bb40d37`。14 类 103 项验证通过，合并后另跑 31 项重叠用例。Catalog DDL 和 revision 租约已推送 `1ceb37abe`，66 项测试通过，独立候选另跑 6 项真实 Catalog SQL。查询与刷新 attempt 的 reader pin 已推送 `c0b61905d`，41 项测试通过，独立候选另跑 10 项真实动态表 SQL。两组 SQL 均使用与该发布基线 JNI 构造器匹配的 macOS arm64 库，SHA-256 为 `6eab152f31b1f305763a87cd14d77baad01959fb4fef2a00a91a03859bc85e7d`。

## Catalog DDL、查询资源和分支选择

| 能力 | 实际入口、权威与验证 |
|---|---|
| Catalog 配置与授权 | SQL 的 CREATE、DROP、ALTER、DESCRIBE 由同一组任务注册。访问检查要求当前身份，ALTER 使用专用 CATALOG grant。ALTER 在发布前创建并验证候选 Connector，然后以 PostgreSQL 的 incarnation 和 predecessor revision 提交 current、history 和 invalidation。未确认提交保留持久化事实，节点可重新对账。DESCRIBE 只返回名称、类别、Provider、状态、revision。CREATE/ALTER 的 SQL 报告文本在生成查询事件、审计、lineage 与 journal 前去除属性。 |
| Catalog 资源 | QueryConnectorAuthority 单次取得实际 metadata 及其 CatalogHandle。提前规划、增量扫描和 metadata query scope 共用这一版本，持续到该查询 drain 和 cleanup 完成。旧 revision 在所有读者释放后关闭，释放失败可以重试。对同一 incarnation 的提交监听器在替换前登记到新 handle。 |
| 分支选择 | `USE BRANCH` 验证当前 SELECT 授权、tenant、表和可读分支后返回 `branch_selection`。JDBC/CLI 共享的 HTTP Client 按连接维护选择。下一次查询根据完全限定的表名再次授权、解析真实分支头并固定 snapshot。MAIN 清除当前表的选择，显式 VERSION AS OF 优先。删除了 Planner 中独立的 AST 改写器及其进程内 branch map。 |
| 实测 | JDK 17 隔离编译后，Catalog 权限、PG、资源与 Planner 测试 90/90，Catalog HTTP SQL 6/6；分支权限、租户、解析、双客户端与协议检查 44/44。使用实际 Worker 和 Connector 的 Iceberg 分支与标签 SQL 14/14、Paimon time travel SQL 5/5。Paimon 的分支 snapshot ID 无法全局定位，因此有真实分支时也显式拒绝 USE；TAG 和显式 snapshot 各自通过。 |

## Connector 工厂、索引文件访问与 Table 策略仓储

| 能力 | 入口、所有者与故障行为 | 验证入口 |
|---|---|---|
| Connector 工厂 v2 | 三个生产工厂显式发布版本、角色与配置校验。一个工厂名在同一发现范围内只允许一个声明，两个角色分别使用相同的目录描述。REST 配置校验在创建 SDK 连接前执行。 | `ConnectorFactoryRegistryTest`、`PluginConnectorHandleFactoryTest`、`LanceFactoryWiringTest` |
| Iceberg 索引文件 | 工厂创建独立 FileIO，构造成功后 Connector 接手。索引访问器与列读取器借用它；调用方关闭打开的流，Connector 在 pin 定时器停止后关闭 FileIO 与 Catalog。目录枚举只返回直接子文件，后端缺少枚举能力时抛出错误。 | `IcebergConnectorShutdownTest`、`IndexGcIcebergPinIntegrationTest` |
| Paimon 索引删除 | 生产访问器使用 SDK 的非递归删除。若删除返回 false 且目标仍在，向调用方报告错误；目标已经消失时可幂等重放。 | `PaimonFileSystemAccessorTest` |
| Table 策略绑定 | 原 `TableControlGovernanceBindingStore` 归入 `table.api.policy.TablePolicyRepository`。PostgreSQL 和 Memory 的 `policy` 包分别拥有实现。Binder 直接使用原子 upsert 的返回值确认绑定 ID、首次创建者与创建时间。工作流在同一事务读策略时复用同一个查询。 | `PostgresTablePolicyRepositoryTest`、`InMemoryTablePolicyRepositoryTest`、`TableControlPolicyBinderTest` |
| JDBC 主异常 | 各领域的事务 owner 继续决定提交、回滚和关闭的顺序。`JdbcSupport.rollbackAfterFailure` 统一保留回滚时的 SQL、驱动或资源异常；Query journal 的冲突处置仍须取得实际 rollback 确认。 | `JdbcRollbackTest`、`JdbcTransactionsTest`、`JdbcQueryStateJournalIntegrationTest` |
| Worker 观测与 Secret 工厂 | 任务状态上报使用角色装配的 MetricsProvider。真实 OTLP gRPC 请求验证部署资源属性。ENV/FILE 工厂重复前缀或版本不兼容时，在创建任何 provider 前拒绝。 | `RemoteTaskStatusReporterTracePropagationTest`、`TelemetryInitializerTest`、`SecretResolverRegistryTest` |

## Shuffle 服务端读取与 cleanup 竞争

Direct 的数据路径在 Native 执行端，Local 的 SpoolStoreFactory 提供按 attempt 选取的文件读写。Pangrova 远端的 Coordinator 与 Worker 由各自的服务描述符发现；两个工厂都声明阻塞式分区读取，显式 attempt 选取和推测隔离仍未接通。

发现远端服务只检查写预留便可进入 CLEANING。一个执行中的分区读取可以在授权有效期内被另一实例删除对象。现由 ShuffleServiceEngine 校验现有授权、经 PostgreSQL 登记读 pin，ShuffleReadPump 负责在异步传输完成后释放。独立服务实例共享同一 query 锁和截止时间，过期 pin 随 query 删除。ReadPump 还会拒绝在到达已提交的最后一个批次前发送 terminal。涉及事务、失败路径与运行证据见 DurableShuffleIntegrationTest 和 ShuffleAccessRenewalIntegrationTest。

2026-11-14 在隔离构建目录用 JDK 17 运行 48 项 reactor，20 个服务端用例全部通过，无跳过。12 个 DurableShuffleIntegrationTest 覆盖真实 PostgreSQL、MinIO 和 gRPC 线程池，在读取中从第二个服务实例尝试清理，并验证崩溃后直到读保护排空时间为止的拒绝行为。6 个授权续期测试和 2 个 gRPC 容量测试同时通过。首次隔离构建缺少同一批已移动的异常类型；同步完整服务源码及 schema 初始化资源后重新构建通过。

## Catalog 投影与控制面持久化依赖

`persistence.workflow.postgresql.projection.ProjectionRows` 和 `persistence.catalog.postgresql.projection.ProjectionRows` 曾各持有相同的 SQL。现在 `CatalogProjectionSql` 在 Catalog PostgreSQL 制品内拥有表投影的读取、upsert、删除与 schema 观察。它借用连接；读仓储拥有自己的连接，投影工作流持有原事务、部署 revision 锁和 outbox 提交。提交 `01193e208` 的 58 个用例全部通过，包含 36 个 PostgreSQL 工作流、故障与失效用例、2 个契约用例及 20 个架构用例。

控制面的 Catalog、Security PostgreSQL 类只在联测中直接使用。两个依赖均改成 test scope，Server Coordinator 继续显式装配二者。现在领域生产依赖闭包检查覆盖所有领域、单制品契约和每一个 Persistence 后端，沿实际 POM 的 compile 和 runtime 边给出完整的违规路径。后端内部工作流仍可以借用其他 PostgreSQL 实现参与同一事务。提交 `6731af8ad` 通过 21 项架构用例，以及 6 项使用真实 PostgreSQL 的生成读取和查询失败清理用例。

提交 `23d98de2b` 在角色打包时检查自有 Java 包、模块名和插件注册的唯一归属。同一 JAR 可包含同一个类的多版本实现。macOS Native classifier 新增显式模块名。9 项启动器和装包测试通过，另以重新打包的 Native classifier 检查真实角色 JAR。Standalone 424 个 JAR、862 个 Pangrova 包；Coordinator 366 个 JAR、795 个包；Worker 309 个 JAR、426 个包。三者各有 44、34、18 个 Pangrova 服务注册项，均无重复。

## 查询审计装配与 SQL 执行职责

`QueryAuditEmitter` 从 Coordination 移入 `org.pangrova.query.audit`。它借用安全事件写入接口与查询终态记录端口，Server 持有异步发送器及其关闭职责。装配原先只检查安全事件发送器，导致仅配置查询历史存储时完全跳过写入。现在两个输出能独立选择，终态记录保留提交者、租户与 QueryManager 提供的诊断 SQL。查询结束时的调用仍经 `QueryTerminalEvents → QueryAuditDispatcher`。另查到公开的 `QUERY_CANCELLED` 事件此前没有生产发送点。现在取消分支提交该事件，并写入 success=false 的查询历史记录。取消原因与任务物理释放仍由执行侧分别报告。

逐项核查 Coordinator 的 MERGE 路径后删除了无调用的 `MergeExpressionEvaluator`。实际的 MERGE 已由 `PlanExecutionFactory.executeDml` 送入编译和分布式写入；已删除的局部实现会把两个 NULL 判为相等。另一个无注册、调用或 wire 消费者的 `PartialResultMetadata` 也已删除。SQL 当前没有对外声明 `PARTIAL_RESULT_OK` 的分区缺失结果契约。

客户端普通提交保持原 JSON 字段集合。成功的 `USE BRANCH` 提交携带表和分支选择，恢复 MAIN 携带表和清除标记，状态轮询不重放会话修改。快照 `bb70dde59` 的全量运行首先在 Common 的两条响应结构断言失败；补齐字段归属、普通查询的 JSON 省略和分支/清除 JSON 往返后，新快照的 Common 全部 331 项通过。

## CDC 运行指标

此前 `CdcPipelineMetrics` 仅声明指标。Coordinator 现在把已经选定的 MetricsProvider 经 StreamingBootstrap、StreamScheduler 传到实际运行的 CdcPipelineLoop。SourceReader 声明可缺省的当前 lag；Kafka 在轮询线程汇总实际分配分区的 offset 距离。OTel 在收集线程读取已采样值。无分区或任一分区缺失观测时跳过该数据点。进程不再需要 CDC 私有的两种 NoOp 枚举。

每个已启动的循环拥有两个带 stream 标签的 gauge。第二个注册失败时回收首个；循环在恢复失败、退出、取消时逆序释放，排队取消尚未创建 gauge。计数区分新 poll、重试写入和源 offset 确认；已确认的批次在 watermark 存储失败时只重试 watermark。完整指标名称与单位见[Streaming 运行指标](./streaming.md#81-metrics)。Kafka、PostgreSQL、CDC loop、MetricsScope 与真实 OTel 内存导出器验证分别落在 KafkaCdcSourceReaderIntegrationTest、CdcPipelineExactlyOnceTest、CdcPipelineLoopTest、MetricsScopeTest 和 MetricAttributesOverloadTest。

## 进程配置、索引发布与错误归属

| 原入口 | 新归属与真实路径 | 验证范围 |
|---|---|---|
| 两个角色各自构建 Observability 配置，Coordinator 固定返回空 | Server Coordinator 与 Server Worker 都从部署配置构造 SDK 的 `ObservabilityConfig`。采样、压缩、资源、凭据和 TLS 在一个配置对象中校验。进程持有 SDK 生命周期，Worker 任务终态上报通过同一 MetricsProvider 记录投递失败 | 配置与 SDK 定点测试通过 53 项。角色装配、网络导出及发行回归分别验证 |
| Config 的全局 `StateStoreFactory` 和 `state-store.type` | 删除无生产注册或调用的静态注册表及无法改变后端的配置项。部署模板移除该键。Coordinator 使用领域端口装配已有持久化实现，Metadata JDBC 配置仍指向实际访问的数据源 | 构建引用扫描、配置读取联测、角色启动 |
| 两个 PostgreSQL 索引发布器内各自的 `PublicationMode` 已删除 | `IndexPublicationMode` 定义初次构建与在线替换的描述符条件。每个工作流在自己的原事务内锁定和验证暂存 generation，原有当前授权、epoch、幂等和二次 fence 在提交方执行 | PostgreSQL 标量索引和生成作业的发布测试 |
| `JournalEventSql.PayloadConflict` 与 `QueryLineageFactSql.PayloadConflict` | 查询 PostgreSQL 制品分别拥有 journal 与 lineage 的不可变载荷冲突。journal 冲突携带类型化的事件身份，仅成功回滚后才准许队列确定性拒绝该事件 | journal 冲突、回滚失败、查询启动恢复 |
| Remote Shuffle、Iceberg 通知和表达式错误 | 自研 Shuffle Service 错误移入服务端 exception，保留 gRPC 失败类型映射。Iceberg 提交通知异常携带原始 cause，读取表达式无法下推独立归 Iceberg Provider | 服务端授权和读取、提交重放、表达式翻译 |

2026-11-14，JDK 17 下验证 49 个不同用例全部通过且无跳过。包括 14 个直接循环用例、9 个 PostgreSQL 提交与恢复用例、4 个真实 Kafka 用例、11 个 OTel 维度与指标作用域用例、10 个调度生命周期用例和 1 个 Coordinator 领导权切换用例。角色构建初次与另一个 Maven 进程共用协议输出目录，javac 读取生成类时出现 NoSuchFileException；在隔离源码和输出目录执行 133 项 reactor 后通过。

## 物化

`MaterializationReadPin` 使用 Execution API 的 `QueryId`，`MaterializationAttemptExecutor` 使用本领域的 `AttemptId`。持久化端口只接受带 `QUERY` 或 `REFRESH_ATTEMPT` 类型的 `ReaderId`。V68 空库初始化以 `(generation_id, reader_kind, reader_id)` 为主键，按 `(reader_kind, reader_id)` 索引续租与释放。到期清理和 GC 仍检查所有 reader 的有效期。PostgreSQL 对相同 UUID 的双行执行续租、定向释放、批量释放和到期清理，GC 用例验证存活的 refresh pin 可以独立保护已退役 generation。独立验证覆盖 Repository、GC、Incremental、查询接入等 38 个测试，最小类路径的 schema 制品 3 个测试，以及真实 Coordinator、Worker、Iceberg 的 DynamicTableMaterializationE2ETest 全部 10 个测试。

表中的类名以 `org.pangrova.` 为共同前缀，制品名以 `pangrova-` 为共同前缀。

| 原类 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| `materialization.port.MaterializationRepository` | 移至 `persistence-materialization-api` 的 `persistence.materialization.api.lifecycle.MaterializationRepository` | SQL handler 经 `MaterializationKernelFactory` 调用命令内核。Server 创建 `JdbcMaterializationRepository` 并借用 Metadata 数据源。保留登记、候选代际、发布、outbox、pin 的同事务操作 | `JdbcMaterializationRepositoryTest`、动态表和物化视图 SQL E2E |
| `materialization.port.MaterializationCatalog` | 移至 `persistence-materialization-api` 的 `persistence.materialization.api.publication.MaterializationCatalog` | `PlannerWiring` 与语句内核借用 `CoordinatorBootstrap` 创建的同一个读取端口；已发布头和 registry epoch 源自仓储 | `CoordinatorBootstrapTest`、`MaterializationReadPinIntegrationTest` |
| `materialization.port.EventOutbox` | 删除孤立写接口 | 无生产实现或调用者。发布和状态更新已在仓储方法中携带事件，事务提交由 PostgreSQL 实现持有 | 仓储测试、物化 SQL E2E |
| `materialization.extension.MaterializationExtensionRegistry` | 移至 `materialization-lifecycle` 的 `materialization.extension.MaterializationExtensionRegistry` | Server 创建注册表，命令内核借用同一实例选择策略。注册表持有不可变的扩展集合，无独立持久化或关闭动作 | `MaterializationExtensionRegistryTest`、`MaterializationArchitectureTest` |
| `coordinator.materialization.MaterializationExtensions`、`server.coordinator.assembly.materialization.MaterializationExtensions` | 删除硬编码装配。Server 的 `MaterializationDdlRegistration` 调用 `MaterializationExtensionRegistry.discover`，从 builtin 的服务描述符加载三个 `RefreshStrategyFactory` | 每个已声明 kind 只创建一个共享纯策略；DDL 创建首代需要 FULL，自动增量优先 PARTITION_REPLACE，随后 APPEND_ONLY。显式指定仍先匹配定义能力。`RefreshStrategyDiscoveryTest`、`MaterializationServiceIncrementalTest`、`PartitionReplaceRefreshStrategyTest` |
| `coordinator.materialization.MaterializationDdlRegistration` | 移至 `server-coordinator` 的 `server.coordinator.assembly.materialization.MaterializationDdlRegistration` | `QueryExecutionBootstrap` 向语句注册表安装 handler，并注入仓储、读投影及策略。命令内核只借用这些对象 | `DynamicTableMaterializationE2ETest`、`MaterializedViewMaterializationE2ETest` |
| `coordinator.materialization.MaterializationKernelFactory` | 保留在 `coordination-control` 并改为注入持久化端口与扩展注册表 | 每条命令绑定其会话、权限、查询 fence 和时钟。仍有基于数据源的原生提交验证依赖，待抽取对应 workflow | `CoordinatorBootstrapTest`、物化读 pin 和 SQL E2E |

## Direct Shuffle 与索引构建

| 原类 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| `worker.shuffle.NativeShuffleTransport` → `NativeShuffleProducerRegistration` | 由平铺后的 `worker` 发布，包为 `worker.shuffle` | `WorkerTaskBootstrap` 注入受保护的 JNI 命令通道，`TaskLifecycleManager` 注册每个 executor 的 producer，`TaskResourceReleaser` 注销该注册。C++ FlightServerRegistry 持有进程共享服务。数据流保留在 C++；Native executor handle 的释放仍由 Worker 管理 | `NativeShuffleProducerRegistrationIsolationTest`、使用实际 Native 库的 `NativeShuffleProducerRegistrationTest` |
| `coordinator.query.execution.dispatch.shuffle.CoordinatorShuffleLifecycleManager`、`ShuffleProtocolAdapter` | 迁入 `shuffle-pangrova` 的 `shuffle.pangrova.client.coordinator.PangrovaShuffleCoordinator` 和包私有的 `PangrovaShuffleControlProtocol` | Server 以配置的 ID 发现 ShuffleCoordinatorFactory，向 DAG 注入对应会话。Provider 独占注册、读写 grant、finalize 校验、清理、gRPC 通道与关闭。DAG 仅使用领域契约 | `PangrovaShuffleCoordinatorTest`、`ShuffleLifecycleOperationsTest`、`NativeExchangeTransportExternalIntegrationTest` |
| `coordinator.query.execution.dispatch.shuffle.generation.ShuffleLifecycleEvidence` | 移入 `shuffle-api` 的 `shuffle.api.coordinator`，增加 `RecoverableShuffleControl` 与 `ShuffleTaskAccess` | Coordinator 提供 Generation 持久化 journal。Provider 确认后传入不可变回执和原始无凭据响应；数据库仍核对 owner、allocation、generation 和 producer 集合 | `ShuffleContractIsolationTest`、`GenerationExchangeEvidenceTest`、`PangrovaShuffleCoordinatorTest` |
| Worker 直接创建 RemoteShuffleClientPool 和 RemoteShuffleByteEgress | `shuffle-native` 的 NativeShuffleWorkerFactory、NativeShuffleReaders，`shuffle-pangrova` 的 PangrovaShuffleWorkerFactory | Server Worker 安装 Provider。WorkerTaskBootstrap 选择工厂，核验会话能力。读取和写出借用同一代的 channel 配额。WorkerTaskOwnership 在部分启动失败时关闭已获取的资源，成功后将关闭权移交给 NativeExchangeTransport、NativeTaskExecutor 和 TaskManager | WorkerTaskOwnershipTest、NativeShuffleEgressContractIntegrationTest、RemoteShuffleChannelAdmissionTest |
| 注册请求中的 shuffle_mode | 新增 shuffle_provider、Coordination API 的 WorkerShuffleDeployment 和 schema 初始化 V113 | Worker 在认证注册时公开实际安装的部署。WorkerManager 核对注册、重注册和故障接管。Memory 与 PostgreSQL 将该部署绑定到进程 incarnation，恢复后只有匹配的进程可参与调度 | WorkerResourceVersionCheckTest、WorkerManagerRegistryTest、JdbcWorkerRegistryPostgresTest |
| `shuffle.pangrova.client.RemoteShuffleClientException` 和原 Worker 后端异常分类 | Pangrova 的 `client.exception.RemoteShuffleClientException`、统一 API 的 `exception.ShuffleReadException`、`nativewire.exception.ShuffleChannelAdmissionException` | Worker 将数据损坏和通道配额拒绝映射为执行错误。它只需要 API 和 Native wire 类型。Server 根据角色安装后端；错误本身没有持久状态和关闭操作 | NativeExchangeTransportExternalIntegrationTest、RemoteShuffleChannelAdmissionTest、TaskCompletionMonitor |
| `indexing.build.IndexBuildCoordinator` | 保留在 `indexing-lifecycle` | `IndexDdlExecutor` 和增量刷新路径提交构建。每次提交创建单独 attempt 身份，它同时标识数据库 claim 和物理对象目录。借用的 executor 由创建者关闭 | `IndexBuildClaimLifecycleTest` 的并发接管与迟到写入测试 |
| `indexing.build.IndexFileBuilder` | 保留在 `indexing-lifecycle` | Coordinator 传入 attempt；builder 借用列读取器和存储访问器，计算校验和，写入返回映射所指向的确切路径 | `IndexBuildClaimLifecycleTest` |
| `indexing.storage.IndexStorageLayout` | 保留在 `indexing-lifecycle` | `IndexFileBuilder` 按代际、attempt UUID 和源文件标识生成路径。读取和已登记对象的清理使用保存的映射 | `IndexStorageLayoutTest`、`IndexBuildClaimLifecycleTest` |
| `indexing.lifecycle.IndexLifecycleListener` | 删除未装配的类 | 原实现从逻辑数据文件名推算要删除的对象。生产 `IndexGarbageCollector` 从仓储读取已登记的物理路径 | 删除前检查调用与装配；实际 attempt 隔离由并发测试覆盖 |

待核查的边界包括索引对象写入后、映射提交前崩溃时的回收，以及已经写入部分映射的代际如何恢复。Direct Shuffle 的启动和停止测试覆盖 Native 控制，不构成 Local、自研远端或 Celeborn 的发布回执、重试与恢复证据。

Shuffle 的传输方向、存储介质、保留保证及 Provider 能力枚举由 `shuffle.api.enums` 发布。数据读取模式和分区合并策略由 `shuffle.exchange.enums` 声明。当前没有生产入口启用分区合并。远端服务在 `shuffle.security.enums` 声明票据操作，在 `shuffle.server.enums` 声明请求失败类别。枚举类型沿用原有值、发布制品与包可见范围。

在 macOS arm64 上，`IndependentLocalDiskShuffleE2ETest` 用冻结的 Coordinator 和两个独立 Worker JVM 创建真实 Iceberg 表、分两次写入、执行自连接与聚合。结果为 196 和 784。至少两个完成阶段的上报来自同一 Worker incarnation，attempt 均为 0。源提交 `16ca580103b74eba1130c509ab1e21d71f9f8281`、Release Native SHA-256 `6eab152f31b1f305763a87cd14d77baad01959fb4fef2a00a91a03859bc85e7d`、1096 项发行清单 SHA-256 `3b63390ba1476e02c5bd45afa687d10e0135e06ac25de1f4b84de2566e7bcc11`。该测试通过 1/1，耗时 484.2 秒。独立链接 Release 的生产与测试对象后，`ShuffleBufferStoreTest` 全部 19 例通过，其中包括内存可用时强制写本地磁盘、读完释放文件、取消唤醒与配额回收。Local 数据的读取依赖被选中的计算进程存活；跨 Worker 恢复不在该模式的保证范围。

Native 的 Lance 数据集控制和任务请求解析现在共用 `engine/resource/WorkerStorageBinding` 校验租户、Catalog 身份与仓库路径。移除了 `lance → command` 直连。Velox 的统计编码由 `engine/velox/runner/TaskStatsEncoding` 持有，另移除一处闲置 include。`engine → command` 尚有两个实际引用，分别为会话退出时清理执行 handle 和任务错误回报时定位原 handle。组件直连从 31 条减为 30 条，原八组件仍形成一个强连通分量。文件图包含 414 个源码文件、874 条本地引用、零文件环。原生边界脚本及其 6 个测试通过。13 个受影响编译单元在 macOS arm64 Release 完成编译，重新链接单共享库；使用真实 PostgreSQL/Lance fixture 验证了 86/86，包括历史快照写入、仓库越界拒绝和算子统计。

下一批移除了这两个 `engine → command` 引用。Velox 的任务失败和异步结果流失败通过 `TaskFailureReporter` 回到绑定 handle 的 HandleRegistry，每次上报均重新取得活跃 executor，最终诊断仍由 TaskLifecycle 持有。JNI 的初始化、重启、回滚与退出入口提供同一 registry 的清理操作，NativeEngineLifecycle 持锁先调用它，再回收运行资源。组件直接依赖从 30 条减为 29 条，门禁同时拒绝原有的两条反向直接边。源码图为 415 个文件、875 条本地引用、零文件环。经 JNI 与 UDF 的间接路径仍使八个组件互相可达。

随后 JNI 将同一上报契约注入不可变的 JVM 函数回调对象。函数执行时仍通过请求绑定的 handle 将经过校验的 AI 错误写入该任务。Lance 的 schema 指纹计算由命令入口传入当前 memory governor，保留先校验请求和租户、再分配内存池的顺序。移除 `udf → command`、`lance → jni`，增加 `udf → engine` 的单向上报契约引用。当前为 28 条组件边，415 个文件、874 条 include、零文件环；八个组件仍互相可达。仓库门禁回归测试 8/8。

macOS arm64 使用本次 CMake Release 单共享库验证了四个真实用例。Worker 能力握手 1 项，AI 回调注册 1 项，Lance 控制路由 2 项。路由检验认证、租户权限、陈旧 epoch、重复指纹，以及 Schema 列名变更后指纹变化，另由公开 SQL 验证真实写入读回。首次将拥有 Native 生命周期的 AI 测试和共享 SQL 集群置于同一 JVM，重复注册失败；依照 full-regression 约定分 JVM 重跑后均成功退出。中间批次另有 31 个 Native 用例覆盖执行句柄注册与重启、迟到结果流错误和首次生产故障的保留。 相同源码、PostgreSQL/Lance fixture 下，Release CTest 4333/4333 通过（318.53 秒），ASan CTest 4333/4333 通过（1492.29 秒）。两份逐项日志均通过 sanitizer 诊断扫描。ASan 使用经过验证的 Folly 构建，按 macOS 运行约定关闭 LeakSanitizer。

## Shuffle 持久化分配证据

| 旧类与职责 | 当前类与制品 | 入口、状态与验证 |
|---|---|---|
| `ShuffleLifecycleEvidence.Allocation` 的单一地址、`registered/finalized` 的带凭据 handle | `shuffle-api` 的 `ShuffleProviderDescriptor` 与 journal Allocation | Journal 只接收分配 ID、Provider ID、版本和最多 1 MiB 的私有字节，以及发布时的回执与无凭据响应。 |
| `GenerationExchangeRegistration.Address/Endpoint` | Generation API 保留逻辑 exchange、stage、epoch、cohort、原始 owner 和 descriptor。PangrovaAllocationCodec 独占远端路由编解码。 | GenerationExchangeEvidenceCodec 的注册 golden 与引用其 SHA-256 的发布 golden 同步更新。PostgresGenerationStageExchangeStore 在原权限与 owner 事务中拒绝冲突的 Provider、版本或字节。 |
| `DurableStageShuffleEvidence` 的地址转换、`StageExchangeAccessValidator` 的 Generation 路由比较 | Coordinator journal 恢复前核对完整 cohort。外部任务传输通过会话的 `ExternalShuffleDescriptorEncoder` 在分区 claim 事务中比较实际读写绑定与原分配。 | 动态权限由实际服务重新授予。已缓存的分配或发布回执也需核对本进程当前 owner。真实 PostgreSQL 接管测试验证旧 owner 被拒、错代被拒及 first owner 不变。 |

本批独立 JDK 17 编译及首轮 87 个测试通过。最终针对已调整的 gRPC 恢复、失主缓存、编解码与 golden 复验 45/45，贯穿真实 journal、producer 和 PostgreSQL 接管的套件复验 8/8。Direct、Local 与未来 Celeborn 的持久化回执需要各自生产实现及对应验证。

## Metadata Federation

| 原类 | 处置及实际目标 | 调用、状态与资源 | 验证 |
|---|---|---|---|
| `metadata.federation.FederatedCatalogFactory` | 移至 `metadata-api` 的 `metadata.api.catalog.FederatedCatalogFactory` | 外部工厂按此服务名注册。每次创建返回归对应 Catalog handle 关闭的实例 | `ServiceLoaderReachabilityTest`、`FederationProviderDiscoveryTest` |
| `coordinator.metadata.PluginFederationHandleFactory` | 移至 `metadata-catalog` 的 `metadata.catalog.federation.PluginFederationHandleFactory` | Server 构造后注入 Catalog 注册表；启动时发现工厂、拒绝重复或空 ID，每次建 Catalog 时解析配置里的密钥引用 | `PluginFederationHandleFactoryTest`、`FederationProviderDiscoveryTest` |
| `metadata.federation.FederationCatalogHandle` | 移至 Metadata Catalog 的 `metadata.catalog.federation.FederationCatalogHandle` | 注册表负责部署、退休和关闭；周期性同步通过 CatalogHandle.federationSource() 借用外部 Catalog；外部事实仍归 Provider | `MetadataBorrowedOwnershipIntegrationTest` |
| `metadata.federation.FederationConnectorMetadata` | 移至 Metadata Catalog 的 `metadata.catalog.federation.FederationConnectorMetadata` | 将命名空间、表、列和统计读取转交外部 Catalog。缓存装饰器的测试使用显式注入的实例，生产 handle 直接使用 Provider 返回的实例 | `MetadataAbstractionLayerAuditTest`、`CachingFederatedCatalogTest` |
| `metadata.federation.FederationTableHandle`、`metadata.federation.FederationColumnHandle` | 各自移至 Metadata Catalog 的 `metadata.catalog.federation` 包，类名保持 | 仅承载外部 Catalog 的表及列身份，无连接或文件所有权 | `CatalogHandleAndStatusProtocolConsistencyTest`、`CachingFederatedCatalogTest` |

仓库当前未随包提供 Federation 后端工厂。实际发行的 Iceberg、Paimon、Lance 使用 Connector 工厂；Federation 的测试工厂只验证发现、拒绝行为和适配器边界。

## Worker 进程与 Standalone

| 原类 | 处置及实际目标 | 调用、状态与资源 | 验证 |
|---|---|---|---|
| `worker.bootstrap.PangrovaWorkerMain` | 入口移至 `server-worker` 的 `server.worker.bootstrap.PangrovaWorkerMain`；配置转换抽至同制品 `server.worker.bootstrap.WorkerConfiguration` | Worker 启动脚本加载角色 JAR，入口校验配置、安装 Catalog runtime 和关闭钩子；WorkerBootstrap 持有执行资源 | `PangrovaWorkerMainTest`、`WorkerPortConfigurationTest`、脚本 classpath 测试 |
| `worker.embedding.EmbeddedWorker` | 移至 `server-worker` 的 `server.worker.embedding.EmbeddedWorker` | Standalone 传入实际 Coordinator HTTP 端口。配置使用同一转换器，安装生产 Catalog runtime；Standalone 先关闭 Worker，再关闭 Coordinator | `EmbeddedWorkerTest` |

Worker Core 仍包含执行组件的分阶段装配。macOS arm64 上的 `PhysicalTwoWorkerShuffleE2ETest` 通过两个 Worker 的 join 和全局 Top-K，第二个 Worker 运行在独立 JVM。Standalone 完整启动、独立进程故障恢复和 Linux 容器角色发行另行验证。

## Server Bootstrap 与索引映射事务

| 原类或方法 | 处置及实际目标 | 生产调用、事实与资源 | 验证 |
|---|---|---|---|
| 三个角色 Main 的 `loadPangrovaConfiguration` 与 `parseConfigPath` | 合并至 `server-bootstrap` 的 `server.bootstrap.ServerConfigurationLoader` | Coordinator、Worker、Standalone 在构造服务前传入各自的默认配置资源，日志脱敏保留原始文件路径语义 | `ServerConfigurationLoaderTest`、两个独立 Main 的配置与验证测试 |
| `server.coordinator.bootstrap.CoordinatorResourceRelease` | 合并到 `common.lifecycle.ResourceCloser.closeAll` | Coordinator、Standalone 与 Streaming 指定所拥有资源的释放顺序，执行全部关闭动作，重复抛出的同一异常不发生自抑制 | `ResourceCloserFailureTest` |
| `indexing.build.BuildFileMapping`、`indexing.core.build.BuildMappingPersister` | 原制品保留，传递同一 attempt 身份到持久化端口 | CREATE INDEX 与增量刷新两种入口共享转换器。批中掺入不同 attempt 在提交前被拒绝 | `IndexBuildClaimLifecycleTest`、`JdbcIndexMappingPublicationFenceTest` |
| `persistence.indexing.postgresql.metadata.JdbcIndexMappingWriteFence` | 在原事务中锁定精确 claim | 同一连接先锁 descriptor 和必要的 staging 行，再锁 index、generation 和 owner 一致的 claim，写整批映射后提交。旧心跳不授权后续写入 | `JdbcIndexMappingPublicationFenceTest` 覆盖已成功心跳后被接管、发布冲突和批写回滚 |

## 按实际调用方复核契约制品

本次按顶层领域目录统计 Java 主源码实际引用该制品的其他领域制品数。同一领域的依赖、测试代码、仅由 Maven 传递引入的依赖均未计入。反射发现和外部插件需要另外检查。表中计数用于判断契约是否仅在领域内部使用；计数不代表实现数，也不证明能力已通过端到端验证。

| 契约制品（省略前缀 `pangrova-`） | 领域外调用制品数 | 可定位的生产调用制品举例 |
|---|---:|---|
| `ai-api` | 4 | `coordination-control`, `persistence-ai-postgresql` |
| `connector-api` | 36 | `coordination-control`, `execution-binding` |
| `coordination-api` | 4 | `persistence-coordination-api`, `persistence-coordination-memory` |
| `execution-api` | 14 | `coordination-control`, `generation-api` |
| `filesystem-api` | 7 | `ai-worker`, `coordination-control` |
| `function-api` | 4 | `ai-worker`, `planner` |
| `generation-api` | 9 | `ai-control`, `coordination-control` |
| `governance-api` | 5 | `coordination-control`, `metadata-catalog` |
| `indexing-api` | 7 | `connector-iceberg`, `coordination-control` |
| `materialization-api` | 4 | `coordination-control`, `persistence-materialization-api` |
| `metadata-api` | 8 | `coordination-control`, `persistence-catalog-api` |
| `observability-api` | 10 | `coordination-control`, `execution-binding` |
| `persistence-catalog-api` | 4 | `coordination-control`, `metadata-catalog` |
| `persistence-coordination-api` | 5 | `coordination-control`, `coordination-zookeeper` |
| `persistence-file-api` | 3 | `coordination-control`, `server-coordinator` |
| `persistence-generation-api` | 5 | `coordination-control`, `execution-binding` |
| `persistence-indexing-api` | 3 | `coordination-control`, `indexing-lifecycle` |
| `persistence-jdbc-api` | 4 | `coordination-control`, `metadata-catalog` |
| `persistence-materialization-api` | 3 | `coordination-control`, `materialization-lifecycle` |
| `persistence-query-api` | 6 | `connector-iceberg`, `connector-paimon` |
| `persistence-security-api` | 8 | `coordination-control`, `governance-runtime` |
| `persistence-table-api` | 3 | `coordination-control`, `metadata-catalog` |
| `persistence-workflow-api` | 3 | `coordination-control`, `metadata-catalog` |
| `scheduler-api` | 3 | `coordination-control`, `query` |
| `security-api` | 29 | `ai-control`, `ai-worker` |
| `shuffle-api` | 7 | `coordination-control`, `execution-binding` |
| `streaming-api` | 3 | `coordination-control`, `persistence-streaming-postgresql` |

Client 的三个入口为 CLI、JDBC 和 HTTP。原 Client Core 的 19 个生产类型按调用职责归入 Common。跨进程传递的 ResultEndpoint 与 ResultProtocol 一起归入 `common.client.result`。HTTP SDK 创建连接，CLI 和 JDBC 借用 SDK，Common 声明查询操作与结果值。

Function 是此前唯一只有一个子模块的领域目录。源码已上移至 `pangrova-function/src`，根 reactor 直接构建该目录里的 `pangrova-function-api` JAR。AI 函数工厂及 Native 回调继续依赖同一个跨域契约。

| 原类或入口 | 当前制品和职责 | 运行边界与验证 |
|---|---|---|
| `ai.worker.provider.ConfiguredAiProviderRegistry` 的厂商分支 | `ai-worker` 保留模型、密钥解析、并发创建和关闭所有权；按 `ai-api` 的 `ai.api.provider.AiProviderFactory` 取得实例 | `ConfiguredAiProviderRegistryTest` 和 `ConfiguredAiMultimodalProviderRegistryTest` 覆盖版本、密钥与故障 |
| `metadata.service.TableControlWriteAuthorities` | 删除，Coordinator 通过 `CoordinatorTableWriteAuthority` 装配 PostgreSQL 工作流与 `MetadataTableControlPolicyProjections` | Generation 与 Query 在调用方事务中接受注入的 `TableControlWriteAuthority`，锁、租户、表、模型校验保持原顺序 |
| `MetadataTableControlPolicyEvaluator` / `TableControlPolicyEvaluator` | 删除中转接口与实现，`PostgresTableControlWriteAuthority` 使用 Security Access 的 `DeferredAuthorization` 与 `DeferredPolicyReader` | Metadata Catalog 生产依赖只保留 workflow API，前台与延迟授权共享稳定表身份的行策略和列掩码投影 |
| `ai.control.config.AiProviderDialectValidator` | OpenAI 校验移至 `ai-openai` 的 `ai.provider.openai.AiProviderDialectValidator`；`ai-control` 中的 `AiProviderFactories` 发现工厂并拒绝重复 dialect | 注册表用同一批工厂校验模型、创建模型客户端。通用 endpoint、egress 和能力限制仍由控制面校验 |
| `ai.provider.openai.inference.OpenAiCompatibleInferenceProvider` | 移至 `ai-http` 的 `ai.http.chat.OpenAiCompatibleInferenceProvider`；管理 OpenAI 兼容 chat 的调用、受控 HTTP、批处理及准入 | OpenAI 和 Qwen 两个独立 Provider 复用同一传输实现，两个 Provider 无相互依赖 |
| `ai.qwen.QwenProviderFactory` | `ai-qwen` 的生产服务描述符安装工厂，支持 Model Studio 兼容接口上的 TEXT inference | `QwenProviderRegistryTest` 通过真实本地 HTTP 往返核对部署名、请求和结果，同时验证拒绝错误路径和能力；阿里云账号线上调用待验 |
| `parser.api.ast.statement.privilege.GrantPrivilegeStatement`、`RevokePrivilegeStatement` | 保留；语法经 parser 构建 AST，Server 的 `PrivilegeStatementTasks` 接入当前授权和持久化，授权事实由 Security 持有 | `SqlPrivilegeAuthorityE2ETest` 覆盖认证 HTTP 提交、回执、授予/撤销后的 Native 读取权限以及越权拒绝 |

## Common 无状态工具与消费边界

| 原类或入口 | 目标与调用位置 | 契约和验证 |
|---|---|---|
| `org.pangrova.materialization.api.domain.Require`、`org.pangrova.materialization.api.refresh.RequireExtension`，以及 Connector、Generation、Persistence 等制品内的同义私有校验 | 合并到 `org.pangrova.common.util.Preconditions`。调用方传字段名，保留原字符串、原异常类型和消息。Semantic 和 AI 的可选空值规则与必填字段分开 | `PreconditionsTest` 验证 Unicode 空白、保留首尾空格、null 和字段名。Generation 的发布、刷新 attempt 与 AI Blob 超时代码共享严格为正的 Duration 校验 |
| `CdcRow` 的数值访问器 | 复用 `org.pangrova.common.util.NumberConversions`，调用者指定缺失或非数值的返回值 | `NumberConversionsTest`、`CdcEnvelopeFieldEvolutionTest`。Paimon 类型写入仍执行严格转换，非法值继续失败 |
| `CdcRow`、`TypedRow`、`ColumnValue.MapValue`、`SplitData`、`DataSplitProtoCodec`、`ShuffleStageStatsReport`、`NativeMetricsPayload` 中的顺序 Map 快照 | 统一到 `org.pangrova.common.util.OrderedMaps.immutableCopy`，每次生成独立、浅层、不可修改的快照 | `OrderedMapsTest`、`ColumnValueNestedNullTest` 核查输入顺序、null 与外部修改。SQL 行序与线格式由调用域定义 |
| HTTP Client 的 `metadataPathComponent` 和 Worker 的 `CatalogDeploymentClient.encode` | 统一到 `org.pangrova.common.util.UriComponents.encode`。Worker 路径中的空格使用 %20，字面加号使用 %2B | `JdbcMetadataProtocolTest`、`CatalogDeploymentClientUriTest` 使用 HTTP 对端检查路径、查询参数与 UTF-8 编码 |
| `org.pangrova.coordinator.http.QueryPathUtils` | 删除唯一自测及类。实际 state、cancel、profile、profile JSON 路径统一由 `QueryResource` 直接绑定 JAX-RS 的 queryId 路径参数 | 检查了生产引用、反射类名与资源注册。`QueryCancelProtocolTest` 核验实际取消请求和返回值 |
| `org.pangrova.metadata.codec.json.MetadataJsonMapper` | 七个静态转发方法删除。PostgreSQL 的 Catalog、Workflow、Indexing 及 Coordinator 直接使用 `org.pangrova.common.json.CanonicalJsonMapper`。Coordination Control 和 Indexing PostgreSQL 移除用于转发的 Metadata Codec 直接依赖 | `CanonicalJsonMapperTest` 验证相同时间表示、泛型、字节、树所有权和错误。Metadata Codec 的 `TableMetadataSerializer` 继续服务缓存和 PostgreSQL 的表模型 |
| Indexing、Generation、Security 的各领域 Codec，以及 JDBC Support | 保留原制品。分别拥有索引字节格式、Generation 持久化证据与重放限制、权限范围信封，以及连接池和 JDBC 资源生命周期 | 无状态辅助抽取未移动其提交权、数据所有权和资源关闭职责。通用工具均沿既有指向 Common 的依赖边使用 |

本批在 JDK 17 临时输出目录编译变更源码并独立运行 144 项 JUnit 测试，全部通过，包含 PostgreSQL 16 的真实 Generation 发布测试。初次扩大验证时，临时类路径混入旧的 Semantic/Generation 测试夹具和重复服务描述符。重新编译当前源码并移除旧 Provider 类路径后得到以上结果。另用 Common 与 Jackson 注解加载 21 个 Client 契约类型，并运行带完整 Jackson 依赖的固定编码回归。

## Semantic View

| 原类 | 处置及实际目标 | 调用、事务与资源 | 验证 |
|---|---|---|---|
| `coordinator.semantic.SemanticViewStatementService` | 创建、幂等判断、来源授权、校验和核对及发布移至平铺制品 `pangrova-semantic` 的 `semantic.version.SemanticViewService` | Server 将 Store、AccessControl 注入一个领域服务，三个 SQL task 通过同制品的 `semantic.api.version.SemanticViewOperations` 直接调用。PostgreSQL Store 在一个事务内更新 active version 与 publication epoch；服务借用仓储，无连接或发布缓存 | `SemanticViewSqlIntegrationTest`、`JdbcSemanticViewStoreTest` |
| 同一旧类的会话和名称解析 | 移至 SQL task 同包的 `coordinator.query.execution.dispatch.task.semantic.SemanticViewSqlScope` | 入口从认证会话取 tenant、默认 catalog 与 schema，向领域服务传入完整身份。SQL AST 和 QuerySession 留在调用侧 | `SemanticViewSqlIntegrationTest` 的无租户拒绝、跨租户隔离与 SQL 分发用例 |

## Materialization 与 Streaming 子模块复核

| 制品 | 入口、状态与保留理由 |
|---|---|
| `materialization-api` | 定义尝试、代际、刷新上下文、持久化刷新类型、刷新策略与工厂契约，供 Planner、Persistence 和 Coordinator 使用 |
| `materialization-core` | 一套定义、attempt、发布、恢复与保留流程共享同一批端口和身份；注册、创建首代、增量能力判断与计划验证共用一个注册表 |
| `materialization-strategies-builtin` | 三个无外部资源的算法独立于 Core 发布。Server 仅保留 runtime 依赖。改变服务文件顺序不会改变显式策略和自动策略优先级 |
| `streaming-api` | CDC 信封、源、转换器及其工厂、checkpoint、提交和 watermark 端口被生产控制面及 PostgreSQL 使用 |
| `streaming-core` | Pipeline 的读取、解码、写入、checkpoint 和 watermark 由同一调度生命周期协调；格式实现通过服务发现 |
| `streaming-source-kafka`、`streaming-formats-builtin` | Kafka SDK 和 JSON 解码格式分别打包；具体生产与测试入口见本文 Streaming source 与 format 小节 |

本批 64 项策略发现、注册表、刷新服务、证明与 attempt 测试通过。另在 JDK 17 编译本家族全部生产源码并运行 6 项包依赖架构测试，核对领域值、调用端口、扩展契约的依赖方向。

## Indexing Core 与 JVM 策略

| 原位置 | 当前归属 | 调用及所有权 | 验证 |
|---|---|---|---|
| `indexing.build`、`indexing.core.build`、`indexing.metrics.IndexBuildMetrics` | 同一制品 `indexing.build` | DDL 和维护复用同一个 build coordinator 与 attempt，映射提交继续使用当前 PostgreSQL claim；Core 借用执行器及存储端口 | `IndexBuildClaimLifecycleTest`、`JdbcIndexMappingPublicationFenceTest` |
| `indexing.eval`、`indexing.core.evaluation` | `indexing.evaluation` | JVM 数据准备、断路器与评估计数同属一个能力包，Java 评估服务仍未接入实际 Worker split 裁剪 | `IndexEvaluationPreparationTest`、`IndexEvalCircuitBreakerTest` |
| `indexing.storage`、`indexing.fs`、`indexing.metrics.IndexCacheMetrics` | `indexing.storage` | 数据访问器、缓存、缓存指标及 attempt 专属对象路径集中；文件访问器借给单次构建或读取 | `IndexStorageLayoutTest`、`IndexCacheDataFileIndexTest` |
| `indexing.store`、`indexing.planner.vector`、`indexing.config` | 分别归 `indexing.evolution`、`indexing.planner.vector`、`indexing.config` | Schema 变动通过持久化端口更新 descriptor；规划只识别向量能力并绑定计划；配置由 Server 构造 | `VectorSearchRewriterCastTest`、全量 Indexing 主源码编译 |
| `indexing.registry.IndexEngineRegistry`、`indexing.builtin.codec.BuiltinIndexSerializers` | 注册表移至 `indexing.strategy`，删除手工 serializer 清单 | 每个已发现的 `IndexStrategyProvider` 提供自身 payload codec，由 Server 一次装配同时用于实际构建和读写。Builtin 在 Server 的运行时 classpath 中提供九条注册 | `IndexStrategyDiscoveryTest`、`IndexCodecGoldenTest`、`IndexTypeStableIdTest` |

## Governance 血缘后端

原 `governance.lineage.LineageBackendConfiguration` 的进程装配迁至 Server Coordinator。
`governance.api.lineage.LineageBackendFactory` 分别由 Log、OpenLineage HTTP 实现，服务描述符随
Governance Providers 发布。工厂发现不打开 HTTP 客户端，选中的工厂在出站方式和 outbox
要求检查通过后才创建 transport。Query 拥有持久化事实；Governance 的 emitter 完成实际发送、
确认与 drain 后关闭 transport。启动拒绝和真实 HTTP 测试套件分别覆盖工厂选择、
PostgreSQL 发送确认及进程接管。SQL 质量检查所需的授权查询执行器仍未在此路径装配。

## Scheduler 的决策输入、重试预算与 Generation 发布路由

| 原类或包 | 当前制品与包 | 生产入口、状态与验证 |
|---|---|---|
| `scheduler.api.placement.{ClusterSnapshot,SchedulingWorkerSnapshot,ControlLoopAction}` | `scheduler-api` 的 `.api.control` | 这些类型表达整个集群的一次观察及拟议操作。实验接口 `SchedulerControlLoop` 和空动作实现有独立测试，目前没有 Coordinator 定时执行这些操作，也没有生效的 control-loop 配置项 |
| `scheduler.api.placement.{RetryBudget,FaultTolerancePolicy,NativeFaultPolicy,TaskDuration}` | 前两个归 `.api.retry`，Native 故障策略归 `.api.health`，任务耗时归 `.api.speculation` | `NativeCrashHandler` 处理故障，慢任务识别使用 `TaskDuration`。`RetryBudget` 有独立策略测试，线上失败任务的重试次数由 Query 的 Stage 维护 |
| `scheduler.core.speculation.{SpeculativeWorkerInventory,SpeculativeQueryInventory,SpeculativeAttemptCanceller}` | `scheduler-api` 的 `.api.speculation` | Coordinator 提供 Worker/Query 的观察和取消入口。带有 Protobuf 请求或执行报告的 `SpeculativeTaskRegistry`、`CoordinatorSpeculativeAttemptDispatcher` 留在 Core 执行适配层。Scheduler API 独立加载不需要 Protocol 或 Calcite |
| `SpeculativeTaskLauncher` 的 lineage 和次数缓存 | 删除重复缓存，新增 `scheduler.api.retry.TaskRetryAdmission` | `QueryBootstrap → DAGLifecycleController → Stage` 核对可替换性、持有 lineage 预算与槽位，并在派发前登记终态路由。错误的 query、无可用替代节点、预算耗尽均不会消耗额度。实际派发失败时回退槽位、次数与路由。取消推测落败 attempt 与失败重试的接纳分开 |
| `scheduler.api.placement.WorkerSelectionStrategyFactory` 和内建算法 | 工厂与 placement 值保留 Scheduler API，候选资格和调度留 Core，算法与服务描述符留 `scheduler-strategies-builtin` | Coordinator 配置 `coordinator.query.placement-provider` 经 `QuerySchedulingWiring` 查找版本兼容且 ID 唯一的工厂。角色装配和租户选择用例覆盖该路径 |
| `generation.core.submission.GenerationPublicationFactory`、`IndexingBootstrap.artifactPublicationStores` | 删除转发及重复路由，使用 `generation.publication.GenerationPublicationRouter` | Server 将 vector index、text index、generated column values、tabular export 四类输出一次性映射到三个 PostgreSQL 参与者，`DurableGenerationPublisher` 在既有 claim 和发布确认范围内调用所选存储端口。单元测试检查四条分派、拒绝未知类型、失败延后重试、已占用 claim 与过期 claim 接管；数据库测试检查真实空表发布 |
| `generation-api`、`generation-codec`、`generation-core` | 保留三个现有制品 | API 供 Planner、Persistence 与调度端引用；Codec 实现跨读取者统一的值及图编码；Core 驱动提交、执行与发布。加入 PostgreSQL 路由发生在 Server，契约保持独立，Core 无须反向依赖 Provider |

独立 JDK 17 编译包括 Coordinator 装配入口与 SQL 端到端 fixture。19 个定向测试类运行 158 项，含 PostgreSQL 16 容器发布、Stage/DAG 的重试竞争与回退、推测取消、策略发现及 query 归属检查。此处计数覆盖被执行的定向套件。

macOS arm64 的远端分区读取比较观察了持久化 Arrow 字节、mTLS 响应帧及读取时间。三轮交替测量包含 192 个有效样本。环境、分位数、吞吐、复现方式和原始 CSV 见[远端 Shuffle 分区读取测量](../testing/shuffle-read-comparison.md)。

原 Worker ShuffleRequestRouter 及其自测已移除。旧 Java 注册表与明文 ticket 编解码只供自身测试调用。实际 Direct Flight 路由由 C++ FlightShuffleServer 注册带租约的 handle，使用签名票据，并在运行时关闭时释放共享端口。

## Security Secret 与 AI 函数资源

Coordinator、计算 Worker 和 AI 共享 `pangrova-security-secrets` 的前缀注册与发现。Coordinator 的发行物包含 ENV 和 FILE，标准 Worker 包含 ENV。由角色 classpath 决定可用后端。
`SecretResolverRegistry` 维护前缀到实现的唯一映射，未知前缀与重复注册均拒绝。
`CoordinatorSecretDiscoveryTest` 经角色装配验证解析。Worker Catalog 传递租户作用域，AI 按模型配置的引用查找，ENV 拒绝空值。AI 函数的注册、生产资源初始化和关闭由同一 `AiFunctionFactory` 串行管理。
关闭后的调用在创建文件、线程或模型客户端前拒绝，重复关闭只释放一次。

## AI、Security、Governance 与 Connector 的顶层类型

这四个家族的 61 个顶层类型按所属制品的 `.enums`、`.exception` 和 `.constants` 归类。包括 `connector.api.enums.WriteMode`、`security.api.exception.AuthorizationDeniedException`、`ai.control.enums.EvaluationRunState` 和 `connector.api.constants.ConnectorFactoryContract`。嵌套异常原因、状态与只在本类使用的常量仍由原类定义。Filesystem 以及现存缓存能力包没有需要迁出的同类顶层类型。

进一步检查 Connector 的公开签名后，18 个公开嵌套枚举移入 `connector.api.enums`。读绑定与表声明、快照引用、Native 写分配和提交结果、物化源错误、列统计比较、Streaming 写入和存储抽样各有独立类型。Iceberg 和 Paimon 的系统表种类留在各自 Provider 的 `.enums`，保留其支持的 SQL 后缀以及拒绝行为。SQL 和协议上的枚举值与顺序保持兼容。Connector 的 5 个公开异常仍由 `connector.api.exception` 发布。Iceberg 通知失败只在提交实现内使用，不对插件发布。工厂契约版本由 `ConnectorFactoryContract` 持有，句柄编码版本、空快照前缀与未知统计哨兵继续由相应类型校验。检查包含多行全限定名、静态导入、反射与服务描述符。定点重编覆盖 Connector 家族全部生产源码及受影响的调用方，共 467 个编译单元。针对 Connector 公开语义的 135 个测试全部通过，包含真实 Iceberg 表、Paimon 物化源、PostgreSQL 授权和 Lance SDK fixture。

Connector 的类加载隔离测试同时打包宿主契约与插件代码，核对公开签名及嵌套类型的 Class 身份，也验证 Common 工具和 Protocol HTTP DTO 由插件自行加载。

## Quality、Lineage 与文件后端

| 原路径 | 当前处置及验证入口 |
|---|---|
| 原 `QualityCheckService`、`GovernanceStatementHandler` 与写前检查各执行规则 | 程序化入口和定时任务使用 `GovernanceStatementHandler`。与写前检查共享 `QualityRuleEvaluation` 校验结果的规则、表、严重性与行数。实际审计与写入拒绝由 `WriteQualityGateTest`、`CronSchedulerTest` 等覆盖。 |
| `GovernanceQueryRunner` | 结构化读取携带稳定表 UUID 与租户主体。PG SQL 测试覆盖表达式；生产尚无已授权的定时读取通道与候选写入 post-image。 |
| `LineageBackend.isAvailable()` | 两个既有实现曾无条件返回 true，现以发送确认表达交付成功。所有实现明确承担 close 契约。LOG 无私有外部资源，HTTP 客户端由发送端在 drain 后收回。 |
| Worker 中的环境变量解析和硬编码文件后端 | 共享 Secret 注册表接入实际 Catalog 与 AI，`BlobStorageFactory` 提供 LOCAL、S3、OSS 的 FILE 发布路径；`ObjectStoreAccessorProvider` 提供 LOCAL、S3 文件空间。Worker 对 COPY 的目标后端另行要求 `ManagedBlobStorageFactory`。它保证预算逐请求生效、取消可见和物理后端身份可复核。当前只有 S3 实现，Local 与 OSS 在打开目标前拒绝。COPY 的精确版本来源继续使用 S3。35 个定点用例通过，覆盖 Worker 实际 mTLS 请求、S3 物理身份、拒绝不支持的后端、配置重复、租约关闭与部分启动失败。 |
| `EmbeddingProviderLookup` 与静态目录转发 | 移除前者无人调用的空值路径，模型广告由 `WorkerAiModelCatalog` 直接读取已安装工厂的模型定义。执行期间由配置注册表负责创建、缓存与关闭。 |

## Metadata、Table 与连接器的公共表示

| 原职责或类 | 现有入口及 owner | 验证 |
|---|---|---|
| Metadata 的 RegistryTablePolicyAuthority 转发五个方法 | CatalogRegistry 在构造时绑定唯一 TableControlPolicyBinder。它直接实现 TablePolicyAuthority。行策略、列掩码、质量规则借用同一实例。TableControlService 决定 stable ID、当前部署和 ACTIVE 准入，PostgreSQL 工作流继续拥有事务 | TableControlRowPolicyStoreTest、TableControlQualityRuleStoreTest、MetadataTagOperationsTest、TableControlPolicyBinderTest 包含改名、重建同名对象、旧授权拒绝 |
| metadata.service 的行策略存储、事务策略投影、DefaultConnectorSession | 行策略及列掩码在 metadata.security 按 stable ID 投影。Connector 会话在 metadata.connector 构造，测试验证目标 catalog/schema、租户传递及令牌隔离。缓存键按授权会话取摘要的测试与包内缓存键同位置 | MetadataResolutionCacheKeyTest、DefaultConnectorSessionTest；与上一行合计 32 项通过 |
| 无装配引用的 MetadataCleanupConfig | 原类列出的三个 YAML 字段没有加载入口。失效日志由 maintenance leader 调用 PostgreSQL cleanup，L1 由缓存自己的 TTL 管理。删除失效配置声明 | ModuleInvalidationLifecycleTest 检查领导权轮换及关闭 |
| 原 `DefaultMetadataService` 中的已解析表缓存、`MetadataResolutionCacheKey` | `metadata.resolution.CatalogTableResolution` 持有按认证会话隔离的 cache、受控表版本核验、观察表 TTL 与 DDL 后的失效。DDL 使用当前认证会话从 Connector 读取元数据。SQL 与 REST 调用仍经 MetadataService | DefaultMetadataServiceResolveTest、DefaultMetadataServiceCacheTest、DefaultMetadataServiceAlterTableTest、MetadataResolutionCacheKeyTest 共 12 项通过 |
| `metadata.generation` 的 PostgreSQL 事务装配、发布候选项和事务权限 | `persistence.workflow.postgresql.generation` 的 PostgresGenerationStores、PostgresGenerationPublicationCandidates、PostgresGenerationTransactionAuthority 组合当前权限、fence 与 Generation 参与者。Metadata 的生产代码移除了对 Query、Generation 和 Security PostgreSQL 实现的依赖 | 实际 PostgreSQL 的 Generation 存储与 fencing 30 项、锁 4 项通过。跨域提交的整组集成测试随 full-regression 验证 |
| Coordination 中的 HTTP 端点、Filter 和 StageFileUrlService | `server.coordinator.http` 持有 Jersey 注册与入口。Query、Catalog、Copy、File 的资源和过滤器从 `http.endpoint` 向下划分。`http.request` 独立持有请求限额与进程内幂等键，使端点无需导入装配包 | 迁移后的 FileUrlServiceTest、RequestLimitControlIsolationTest、CatalogRestResourceTest、QueryStatusResourceTest、InternalAuthFilterPropertyTest 共 41 项通过。首次复查检出五个包组成的循环，调整请求包后扫描 826 个生产包，循环分量为零 |
| Iceberg、Paimon 各自的同格式快照标签命名 | Common 的 connector.SnapshotPinTags 拥有持久标签名称的 UTF-8 编码与快照后缀。两个 Connector 分别向自身 Catalog 创建与删除标签、验证快照与标签的一致性 | 提交 d10e8c49 的 SnapshotPinTagsTest、IcebergSnapshotPinProviderTest、两个 SnapshotPinLifecycleTest 和两组架构门禁共 39 项通过 |
| Java FuzzerCoordinator.NativeFuzzerRunner | 当前发行物没有实现、服务注册或 JNI 入口。可执行的 run-operator-fuzzer.sh 调用 C++ reference runner 与 DuckDB 做 SQL 生成自测。Java 调用 Pangrova Native 的差分执行为未实现 | Java 接口、CMake 入口、runner 和脚本按各自可达范围审查 |

### Federation Metadata 的可替换缓存

原 `metadata.api.cache.L2CacheStore` 允许无条件覆盖，Memory 忽略逐条 TTL，Server 从未安装它。`cache.api.CacheStore` 现在为命中和未命中都返回同一次原子观察的组与条目代际。Memory 和 Redis 工厂分别由两个 JAR 注册，`TieredMetadataCache` 只在代际仍匹配时填充。Redis 单实例持有自己可关闭的连接池，旧 payload 和失活的代际键有界过期。

生产查询经 Federation handle 缓存表投影，按 catalog、schema、table、incarnation 和 revision 区分。同步与健康检查直接借用同一底层 source，外部 schema 的读取权威保持在连接器。MetadataBootstrap 关闭 registry、分发器后关闭 store；创建失败时 Server 释放尚未转移的 store。PG outbox 将已提交失效传播给各 Coordinator。已纠正此前声称 Redis Pub/Sub 提供跨节点一致性的说明。配置、网络故障、关闭、跨 namespace、同名目录重建、缓存填充和删除的竞争分别验证。隔离工作树中 36 项 Metadata 测试、2 项 federation factory 测试、1 项真实 Redis 容器测试、9 项 Server 配置与 PostgreSQL 失效进度测试通过。Redis 用两个独立客户端并发运行 12 轮失效与旧填充竞争。`jdeps` 显示 Cache API 仅依赖 `java.base` 与 Common 的注解包，无第三方依赖；3 个 JAR 中仅两个 Provider 各发布一条工厂注册。

### Catalog 元数据调用与 query scope

`metadata.service.DefaultMetadataService` 的调用入口迁到 `metadata.catalog.CatalogMetadata`。Planner 通过 `MetadataService` 枚举目录，协调器装配将同一注册表注入 Calcite schema。查询执行用实际租户会话向 `metadata.query.MetadataQueryScopes` 打开目录作用域。该类持有与申请过程完全相同的 Connector authority、部分失败结果和计数。清理失败后保留实例，允许持有该 authority 的调用者重试。删除了只在测试里使用的按字符串 query ID 查出 authority 并释放的方法。对应测试改为检查相同 ID 的不同 authority 被拒绝，尚未停止接入时不能释放，重复释放可重放。

同一部署中的列类型、授权会话和稳定表 UUID 在 `metadata.governance` 解析后交给 Governance 的本实例绑定。Catalog 登记、解析与 DDL 保持各自原有调用顺序。包命名门禁已删除对 `metadata.service` 的唯一永久例外，私有工作树扫描 911 个主源码包，循环分量为零。列标签、Catalog DDL、查询事务与清理、Calcite 缓存、使用 PostgreSQL 和 Paimon SDK 的 DDL 失效通知等 101 项测试通过（Metadata 44、Planner 2、Coordination 55）。DDL 针对真实 `TagChangedEvent` 和稳定表 ID 的持久绑定断言。干净打包的 Metadata Catalog 含 47 个类，已移除的 `metadata.service` 包在 JAR 中无条目。

## Execution、Query 终态与 Coordinator 元数据进度

| 原位置 | 当前归属 | 调用与验证 |
|---|---|---|
| `coordinator.query.execution.dispatch.dag.StageCompletionCallback` | `pangrova-query` 的 `query.api.stage.StageCompletionCallback` | Query 管理逻辑任务、Stage 和 DAG 最终结果。生产 `DAGDispatchEngine` 明确查询是否完成并交付终态。删除默认返回 true 的实现，JVM 编译要求所有实现显式给出就绪判定。端口仅使用 JDK 和 `QueryFailure` |
| `coordinator.query.task.TaskReportCompletion` | 保留报告接收侧，合并普通、后台和推测路径中重复的终态投影 | 已验证的报告提供 query、state 和四个错误字段，胜出 attempt 由接收者选择原始逻辑任务槽位。接收层继续负责 fence、journal、写片段与 drain；Query API 不接收 `TaskStatusReport` 或 REST 错误。`TaskStatusReceiverWriteCommitTest`、`SpeculativeExecutionWiringTest` 观察实际顺序和结果 |
| `execution.core.stage.QueryExecutionScope` 和 `execution.core.stage.binding` | 保留 Execution Binding，修正规则文档曾指向 Query Core 的描述 | 绑定 QuerySession 以及会话或 durable 任务 authority、写入配置和检索/重排意图。若迁入 Query 将经由 Execution Binding 的 StageExecutionContext 反向依赖 Query 本身。Execution API 单独发布身份、期限和 stage/purpose，不持有流程状态 |
| `coordinator.coordination.InvalidationProgressRegistryAdapter` | 改为 Server 的 `bootstrap.CoordinatorInvalidationProgress` | Server 一次选择 JDBC 或 quorum 成员注册表；HA 和失效日志读取相同的注册表。查询角色注册后读取自己的已提交水位，再开始轮询。独立维护角色仅观察 peer floor，按本地已消费位置与在线节点的最低水位两者较小值清理。维护租约重获时重新读取下界 |
| `PostgresInvalidationPoller.setInvalidationProgressRegistry` | 删除晚绑定入口，由构造器一次性绑定查询成员或维护观察者 | 重启时注册表读取失败向启动层传播，操作者可在恢复后重试；读取错误不会把数据库中的进度覆盖为 0。轮询时暂时上报失败仍可继续应用本地失效，后续轮次重报；PostgreSQL 上报端将 SQL 失败传给调用者 |

真实 PostgreSQL 的 `CoordinatorInvalidationProgressTest` 将两个已提交失效条目写入日志，
恢复保留第一个水位的 query 成员只驱逐第二个目录。维护测试让第二个进程夺取、释放实际数据库租约；
本进程重获更高 epoch 后仅删除落后成员确认的条目。`JdbcInvalidationPollerTest` 同时覆盖首次注册、
游标读取失败后恢复、分批消费和未知 peer 水位。

同 ID 重新注册只更新成员信息。ZooKeeper 用 znode 版本 CAS 保留并发上报的水位；Memory 在同一 map 更新内保留水位，PostgreSQL 的注册 upsert 保留已持久化值。成员记录已被删除时，以新记录的水位重新开始。

独立 JDK 17 验证共 196 项。任务回报、变更后的目录写入 fixture 和 Execution Core 的 121 项通过；
失效日志、后端故障与维护租约的 51 项通过；真实嵌入 ZooKeeper 及 Memory 成员测试 23 项通过；
Execution API 单独 classpath 反射验证 1 项通过。首轮写入 fixture 在删除目录时显式抛错，现改为按精确部署身份删除并保留查询所需历史，三组写入测试重跑通过。

### Parser 公开枚举与 SQL 结构

`parser.api.enums` 拥有 17 个从 AST 节点迁出的公开类型，以及方言和关键字解析使用的三个公开枚举。

| 原声明 | 目标类型 |
|---|---|
| `Literal.LiteralType`、`BinaryExpression.Operator`、`TruthTestExpression.TruthValue` | `LiteralType`、`BinaryExpressionOperator`、`TruthValue` |
| `SortItem.SortDirection`、`SortItem.NullOrdering` | `SortDirection`、`NullOrdering` |
| `WindowSpec.FrameType`、`WindowSpec.FrameBoundType` | `WindowFrameType`、`WindowFrameBoundType` |
| `JoinClause.JoinType`、`SetQuery.SetOperatorType`、`CteQuery.SearchOrder` | `SqlJoinType`、`SetOperatorType`、`CteSearchOrder` |
| `QueryHint.HintType`、`TableSample.SamplingMethod`、`TimeTravelSpec.Type` | `QueryHintType`、`TableSamplingMethod`、`TimeTravelType` |
| `TableFunctionRef.TableFunctionType`、`UnpivotClause.NullHandling` | `TableFunctionType`、`UnpivotNullHandling` |
| `DataTypeSpec.FileType.FileStorage`、`AutoEmbeddingColumnSpec.EmbeddingMode` | `FileStorageMode`、`EmbeddingMode` |
| `SqlDialect.QuotingStyle`、`SqlDialect.CaseSensitivity`、`ReservedWordPolicy.Category` | `IdentifierQuotingStyle`、`IdentifierCaseSensitivity`、`KeywordCategory` |

Parser builder、格式化器、Planner 和 Coordinator 的语句执行器使用对应 SQL 枚举。20 组常量名称、数量与声明顺序逐一与提交 `16ca580103b74eba1130c509ab1e21d71f9f8281` 相同。Calcite 和 Protobuf 的类型继续由各自边界的转换方法处理。私有 reactor 的 131 个模块完成生产与测试代码编译。独立运行 Parser 670 项、Planner 1530 项、HTTP 客户端 115 项、JDBC 237 项，共 2552 项通过。Parser JAR 实物包含全部 20 个新类，已移出的 20 个成员类均未打包。

### Planner 公共词汇和生成计划

| 原声明 | 使用者和所有权 |
|---|---|
| `RuntimeFilter.FilterType`、`DynamicFilter.Type` | Connector API 的 `DynamicFilterType` 贯穿 Planner MIN/MAX、Bloom 过滤器、Coordinator 注册表、分片剪枝和 Worker RPC。REST 仍传输原有字符串，Protobuf 使用已有类型化负载。 |
| `AutoEmbeddingColumnDescriptor.EmbeddingMode` | 从 Parser 的 `EmbeddingMode` 读取 DDL 中的 SYNC/ASYNC。Planner 继续拥有文本或 FILE 输入分类。 |
| `ColumnDerivation.DerivationType` | Calcite 表达式分类与 Query 审计共用 Governance 的 `ColumnDerivationKind`。Planner 直接依赖仅引用 Common 的 Governance API。 |
| SQL 写入、阶段和查询提示中的公开枚举 | `PlannedDmlOperation` 表示 Calcite 已规划的 INSERT/DELETE/UPDATE/MERGE。Worker JNI 的写操作编码由 Worker 拥有。阶段模式、终端目的、分布和基线比较结果属于 Planner；调用提示使用 `planner.api.enums` 下的检索种类、分数方向和 AI 调用类型。 |
| `FilterExpressionParser.FilterParseException`、`MaskExpressionParser.MaskParseException` | 两类安全表达式解析错误分别由 `planner.exception` 声明，保留原消息、原因和按类型捕获的行为。 |
| 四个生成任务中的报告版本、持久化生成列的源编码别名 | BM25、向量、表格导出和生成值计划使用 `planner.constants.ArtifactBuildReport.SCHEMA_VERSION`。Native 转换验证版本 2。行内生成表达式、持久化计划和 Coordinator 发布共享 `GeneratedColumnSourceEncoding` 的名称和 U+001F 分隔符。 |

四个实际 JAR 中已检查全部 16 个旧成员类均不存在，并核对了各自的替代类型。128 模块私有 reactor 编译通过。Connector 契约测试 197 项与 Coordinator 绑定、血缘和过滤器测试 51 项通过。Planner 首轮 1530 项有一项发现独立加载器缺少新增的 Governance 契约，修复后该测试通过；它同时检查 Governance 实现、Calcite、Protocol、Arrow、JDBC 和 Worker 执行类不可见。

### Transport / Client / Protocol 与运行角色发行边界

Client 和 Transport 的六个子制品内没有以 Bridge 或 Adapter 命名的生产类型、包或服务注册。实际调用中，原 `org.pangrova.client.http.ResultStreamFetcher` 仅向 `FlightResultFetcher` 转发调用、转换异常并转交关闭请求，唯一调用者是 `PangrovaRestClient`。该包装类已删除。SDK 现在直接拥有 Flight reader，在批量和流式两个公共入口转换连接或服务端错误，保留错误码、服务端状态及原始异常。客户端关闭时仍先封住 HTTP 入口，再关闭 Flight 和 HTTP 资源。

`BatchStreamProducer` 在 Flight Server 侧实现 Arrow 的 `FlightProducer`，负责票据校验和批流交付，保留在 Transport Flight。安全和非安全监听器都使用同一生产者。Client HTTP 和 Transport Flight 两模块现有 142 项测试在独立 JDK 17 reactor 中通过。新增覆盖以要求 TLS 的客户端读取明文端点，两种读取入口均保留 `TRANSPORT_TLS_REQUIRED` 和 Transport 原始异常，测试类四项通过。

2026-11-14 在 macOS arm64、JDK 17 上使用提交 `32b3d02b36b205bf958ccda9771589c27eb595d2` 的同批发行物运行 `IndependentClientCancellationE2ETest`。一个 Coordinator、两个独立 Native Worker、PostgreSQL 和 LDAP 使用真实网络端口。发行清单 SHA-256 为 `66cc08c640c9d5d0619b52f7cf2705a391f99e4be61feaf26937ccb311ce2a41`，Native 为 `1197e854373d45f6ee137cd6c495fba67bdc26252517b6014c3c497f74c22392`。JDBC 运行 classpath 的 22 个 class 和服务描述符条目逐个与冻结 JAR 核对一致。首次运行揭示前一个查询重发的任务报告也会命中测试屏障。改为排除已观测的查询，并在公开 HTTP 确认相同查询已取消后放行回报，复测一项通过、零跳过，耗时 100.8 秒。两个场景分别由 JDBC `Statement.cancel()` 和打包 CLI 的 15 秒期限触发；随后同一 JDBC 连接和新 CLI 进程都用 gRPC TLS 取回结果 42。JDBC 会将收到的 Arrow 数据缓冲后创建 ResultSet。运行方法见[独立进程测试](../contributing/testing.md)。

后续 JDBC 生命周期修复覆盖了服务器已创建查询、客户端尚未收到提交响应时的取消。每个 Statement 发布本次请求的查询 ID，Connection 关闭时先取消并关闭它拥有的 Statement，再释放 HTTP 客户端。独立 Maven 运行的 JDBC 套件 241 项全部通过。Coordinator 的查询、DAG 和推测尝试共用一个精确取消重试队列。断开实际 HTTP 连接、验证相同尝试的释放回执并回收队列的 `TaskCancelRetrierTest` 套件通过 11 项。`TaskIdentityRegistryDrainContractTest` 通过 11 项，覆盖退休读取尝试的迟到结果通知、真正的 Native 释放回执以及错误权限或携带写入产物时的拒绝。以上运行均为零跳过。

在上述 `32b3d02b` 的 macOS 发行物副本中，使用同一源码树加本批取消和回执修订对 11 个源码单元重编，更新 Protocol、Coordination、Server Coordinator 和 Worker 四个 JAR。新角色清单为 `780f9d7734b48d25dd3c331bad915024c44513a4919924551804cbbd500622e3`，Native 沿用上述哈希。独立进程 E2E 延迟首次 FINISHED 直至同一 Worker 的实际重发抵达，检查 PostgreSQL 的租户并发槽由占用变为空闲，随后交付迟到的报文。Coordinator 对该未释放通知返回 `RETIRED_RESULT_NOTICE`，对真实 Native 释放回执返回 `CONTINUE`。JDBC 和打包 CLI 恢复查询及 TLS 读取均通过，套件一项、零跳过，耗时 123.9 秒。此前同时拦截所有重试的测试使仍持有 dispatch 操作的查询无法在屏障打开前退休，已改为仅延迟原始报文。这是对四个变更 JAR 的针对性验证，最终整仓发行仍需重新构建并核对清单。

取消审计的复测在上一次角色清单上更新了 Query 制品的 `QueryAuditEmitter`，移除 Coordination 中的旧类，并重编 Dispatcher 及调用其工厂的 QueryManager。新清单 SHA-256 为 `9cb36ae9e40843933bfc3faa0db1f9b994b315c5602b28c5c0c2c749489b0362`，Native 哈希沿用上述值。同一独立进程测试通过一项，零跳过，耗时 108.2 秒。JDBC 与 CLI 各自按本次实际 query ID 查询 PostgreSQL，校验历史记录的原 SQL、LDAP 用户、解析后的租户、完成时间及失败标志，并核对安全日志中提交与取消各一次、顺序一致。首次断言要求提交审计也带租户，实测提交早于准入阶段的租户解析。复测分别验证提交时已认证的用户与 SQL，以及终态时已经解析的租户与取消结果。 随后使用真实 Arrow IPC 的 UUID ResultSet 在土耳其语默认 Locale 下验证按列名读取，列名 `ID` 和 `iD` 均读出原值。独立 JDBC 套件 242 项通过，零跳过。



| 原责任 / 调用 | 归属与处置 | 证据 |
|---|---|---|
| JDBC 元数据直接读取 Substrait 的向量元素与可空性 | Transport Client 的 `ColumnDescriptor` 解读 wire 描述，JDBC 将 Java 数组类型和可空标志转换为 JDBC 返回值，Protocol 直接依赖降为该模块的测试 scope | 独立打包后的 JDBC JAR 逐 class 检查，无 Protocol 或 Substrait 类型引用 |
| 从 Maven runtime 图生成角色发行清单 | `deploy/docker/stage-role-classpaths.py` 检查 JAR 内主类、角色禁用类及 Native 载荷，包括多版本条目；启动器按该角色的完整清单启动 | macOS ARM64 私有 reactor install 和 staging 产出 Coordinator 358、Worker 306、Standalone 416 个 JAR；三个独立 JDK 17 进程成功验证角色主类、Planner 与 Native Bridge 可见性 |
| 物理 Shuffle 测试的子 Worker 使用 Surefire 测试 classpath | 发行验证可传 `-Dpangrova.e2e.worker.classpath`，取自 `bin/pangrova-classpath.sh` 的 `pangrova_runtime_classpath worker`；未打包的 Maven reactor 保留测试 classpath | 该快照的 Worker classifier 与同次 CMake 的 osx/aarch64 Native 库 SHA-256 相同；Linux 镜像与跨 Worker 执行属于独立验证 |
| Query Submit 按裸 key 预存 queryId | Server HTTP 以经过认证的 principal 和 key 所有权绑定 SQL 与完整会话属性。成功保存原始完整回执，尚未登记的拒绝允许重试，已登记或无法确认的失败保存带 queryId 的错误 | `IdempotencyKeyRegistryTest` 覆盖并发与到期，`QuerySubmissionIdempotencyE2ETest` 覆盖真实身份、HTTP 409、drain 重试与已登记失败；待统一回归 |

## 目录读取、表 DDL、列标签与流状态的归属

| 旧位置 | 生产调用方和现行归属 | 验证状态 |
|---|---|---|
| Coordination 的 `metadata.read` 八个生产类型 | 全部进入 Planner 的 `core.metadata.catalog`。MetadataSchemaBuilder 和 LazyMetadataSchema 构造 Calcite 结构，MetadataResolvedTable 保留选定的列声明。PlanningReadBindings 按当前表身份复查 snapshot/range 与缓存，CatalogStatisticsProvider 与行类型使用同一绑定。认证会话、快照选择与增量选择随绑定同属 Planner。具体注册表实现 Metadata 的 CatalogReadAccess，TableControlService 实现 TableReadAuthority，TableControlStatisticsService 实现 CurrentTableStatistics | 单模块类型推导与缓存用例已迁入 Planner。真实 Connector、目录不可用与 SQL 链用例继续使用下游集成夹具。待统一回归 |
| DefaultMetadataService 的 CREATE、ALTER、DROP 和列声明转换 | Metadata 的 core.ddl.CatalogTableDdl 持有 ALTER 互斥、连接器 DDL、表身份核对、缓存失效和事件派发。core.resolution.CatalogTableResolution 拥有读缓存。每个 MetadataService 实例拥有一组读缓存与 DDL 执行器 | DefaultMetadataServiceControlTest、DefaultMetadataServiceAlterTableTest 与 MetadataColumnDeclarationsTest 待统一回归 |
| MetadataTagOperations | Governance 的 core.classification.ColumnTagBindings 通过 TablePolicyBindings 绑定已捕获的稳定表身份，并重放标签。Metadata 负责在当前用户会话内从 Connector 解析实际列；业务执行不需要反向加载 Metadata | PostgreSQL 标签、改名与旧授权用例待统一回归 |
| governance-providers 同时发布日志、HTTP SDK 与质量执行器 | 两个血缘后端各在自己的 JAR 注册 LineageBackendFactory。日志后端依赖治理契约和 SLF4J，OpenLineage 后端拥有 SDK 和 HTTP 客户端。SQL 质量执行器独立发布；缺失授权执行器时的拒绝行为归治理流程 | 启动时重复标识拒绝、Marquez 和 HTTP 用例待统一回归 |
| Streaming Core 的两种进程内存储 | persistence-streaming-memory 持有 CDC 注册和 watermark。Server 在明确选择进程内状态的调用路径实例化。Streaming Pipeline 的生产源码只使用读写端口 | WatermarkTrackerTest、StreamingGovernanceChainTest 和 PostgreSQL 恢复用例待统一回归 |
| ModuleIntegrationBootstrap 的两组计时任务 | Server 的 CatalogMaintenance 独占目录同步与失效日志清理的定时器。协调器在获得领导权时安装任务，失去领导权时取消定时器，关闭时等待执行器退出并关闭 poller | ModuleInvalidationLifecycleTest 用真实 PostgreSQL 观察轮询线程和租约轮换，拆分后待统一回归 |

### Streaming source、format 与 writer 的实际边界

`CREATE STREAM ... FROM <provider>` 的 source id 经 AST、SQL 打印、StreamState、PostgreSQL、恢复调度传递。Server 发现一次 `CdcSourceFactory`，DDL 和 Scheduler 共用注册表。DDL 先检查 source 属性和 format，Scheduler 按持久化的 source 和不可变 checkpoint id 创建独占 reader。Kafka provider 负责 SDK、topic、group、rebalance、seek、commit 与部分初始化异常的关闭。Core 和 API 没有 Kafka SDK 编译依赖。退役的 `KafkaStreamSourceReader` 仅实现已移除的 `StreamSourceReader`，截断 poll 时会跨过未消费记录；生产 `KafkaCdcSourceReader` 保留按 partition 回退与恢复 seek 的实现。

三个 JSON format 共用 Jackson 和注册方式，进入 `streaming-formats-builtin`。原 `streaming-writers-builtin` 的 Lake、merge-tree 和 incremental writer 只有测试构造点，没有生产工厂、依赖注入、关闭 owner 或真实消费入口；核实公开文档和替代路径后，writer 制品与旧 `TableWriter` 批写层级一并退役。Coordinator CDC 实际写入继续使用 `ConnectorPageSinkCdcWriter`，SQL 写入使用 Worker `NativeWriteExecutor` 与 `ConnectorPageSink`。

最终统一验证需覆盖 `StreamDdlRoundTripPropertyTest`、`StreamSchedulerLifecycleTest`、`CdcSourceRegistryTest`、`KafkaCdcSourceReaderIntegrationTest`、`StreamingKafkaIdentityIntegrationTest` 与 PostgreSQL StreamState 存取。Kafka 实际交付为现有 source，新增 provider 能力由其自身的真实实现与验证决定。

## Table 所有权变更与 Metadata 读取

| 原类和职责 | 实际调用方及归属 | 状态、释放和验证 |
|---|---|---|
| `TableControlService.rename/completeRename` | DDL 处理器直接调用 `org.pangrova.table.rename.TableRenames`。begin 将原注册写为 RENAMING，complete 验证目标物理身份，再激活注册。 | 共享同一 TableControlStore，保留 revision 检查和 rename intent。带版本的最终状态与观察记录仍在一次写入中提交。现有 Memory/PG 与 DDL 测试参与统一回归。 |
| `TableControlService.observeVersion/markUnavailable` | 周期性 observer、ANALYZE 和受控表维护直接调用 `org.pangrova.table.observation.TableVersionObservations`。 | 观察器只更新捕获的注册版本。目录不可达与已证实表缺失有不同结果。TableRenames 与 observations 均借用相同的 TableCatalogAuthority 及后端。 |
| `CatalogTableResolution` | Metadata 读取在访问 Connector 前取得目录部署身份及表 ownership view，读取完成后复核两者。 | 缓存键包含目录 incarnation 和 revision。外部 OBSERVED 表在目录重建后重新解析。覆盖读取期间发生 ADOPT 和同名重建的用例已加入 DefaultMetadataServiceControlTest，待统一运行。 |
| `ModuleIntegrationBootstrap` 中元数据定时任务 | Server Coordinator 持有 `CatalogMaintenance`，按 leadership 期限安排同步及失效清理。 | stop 取消调度，close 等真正退出再释放借用的 poller。某项释放失败仍尝试其它独立资源，保留失败 owner 供重试。 |

### Scheduler、Generation、Materialization、Execution、Indexing 顶层值与异常

每一行对应单一制品的包。下列 26 个顶层枚举、6 个异常和 1 个纯常量类迁移 FQN，保留枚举常量名称、声明顺序、错误码和编码值。实现与测试根据新 FQN 编译。Scheduler Core 的队列权重继续由 Scheduler Core 拥有，Indexing Core 的 Schema 变更分类继续由 Indexing Core 拥有。

| 现有制品内的目标包 | 类 |
|---|---|
| `org.pangrova.execution.api.enums` | `TaskPurpose` |
| `org.pangrova.generation.api.constants` | `GenerationOutputTypeIds` |
| `org.pangrova.generation.api.enums` | `GenerationEmptyPartitionPolicy`, `GenerationExecutionKind`, `GenerationJobState`, `GenerationOutputScope`, `GenerationPageKind` |
| `org.pangrova.generation.api.exception` | `GenerationExecutionOwnerLostException` |
| `org.pangrova.indexing.api.enums` | `DistanceMetric`, `IndexState`, `IndexType` |
| `org.pangrova.indexing.api.exception` | `IndexAlreadyExistsException` |
| `org.pangrova.indexing.enums` | `SchemaChangeType` |
| `org.pangrova.materialization.api.enums` | `DependencyKind`, `MaterializationAction`, `MaterializationEventType`, `MaterializationKind`, `PinState`, `RefreshMode`, `ScheduleState`, `SecurityMode`, `TriggerKind` |
| `org.pangrova.materialization.api.exception` | `MaterializationException` |
| `org.pangrova.scheduler.api.enums` | `AdmissionVerdict`, `ControlLoopAction`, `ErrorCategory`, `FaultTolerancePolicy`, `HealthState`, `NativeFaultPolicy` |
| `org.pangrova.scheduler.api.exception` | `CircuitBreakerOpenException`, `NoAvailableWorkerException`, `TaskDispatchException` |
| `org.pangrova.scheduler.enums` | `QueueType` |

原 `RefreshMode.accepts(RefreshKind)` 将枚举指回业务定义包。改由 `RefreshKind.satisfies(RefreshMode)` 执行相同的全量/增量判断。原 `MaterializationException.notFound(MaterializationId)` 接收业务对象却只使用其 canonicalName，现由调用方取出 canonicalName，异常包依赖基础错误类型。TaskDispatchException 使用同制品的 ErrorCategory 枚举。检查这五个领域中上述包的源码，只有这条 exception → enums 的同领域生产导入。

StrategyId 的具名值、IndexSerializer 的 wire 版本、RetryBudget 和 SpeculationConfig 的字段默认值由拥有其校验或编码语义的类型保留。物化对象、attempt、generation 的转换表与刷新种类现分别由 materialization.api.enums 的 ObjectState、AttemptState、GenerationState、RefreshKind 唯一拥有。PostgreSQL 直接读写持久标签，未知标签仍返回存储损坏错误。StrategyId 位于 materialization.api.identity。包内及方法级辅助错误保留限定可见性。独立 JDK 17 编译涵盖这五个领域的 337 个生产源文件及相关存储 API；JNI、Proto、库中均未发现迁移类型的硬编码 native 类名。角色回归和最终发行旧类检查待 clean 构建。

### Coordination、Server、Transport、Client、Operations、Standalone 与 Testing 公共类型

| 制品内目标包 | 类型 |
|---|---|
| `org.pangrova.coordination.api.enums` | `WorkerHeartbeatStatus`, `CoordinatorRole` |
| `org.pangrova.coordinator.exception` | `NativeDatasetControlRpcException`, `UnregisteredWorkerException` |
| `org.pangrova.operations.enums` | `JfrState` |
| `org.pangrova.operations.exception` | `DiagnosticException` |
| `org.pangrova.server.coordinator.enums` | `AuditWriterConfig`, `CoordinatorRole` |
| `org.pangrova.testing.integration.enums` | `FaultType` |
| `org.pangrova.testing.integration.exception` | `FaultInjectionException`, `SqlExecutionException` |
| `org.pangrova.transport.client.exception` | `TransportConnectionException`, `TransportException`, `TransportServerException` |
| `org.pangrova.transport.flight.exception` | `TicketValidationException` |

两个 CoordinatorRole 分别拥有选主状态和进程配置，保留各自语义和制品。所有迁移的顶层值和异常不反向依赖本制品业务包。SqlExecutionException 保存 runner 提供的错误文本，保留原有 SQL 与消息；audit 枚举的外部日志/JDBC 类型此前仅作 Javadoc 引用，现由装配包导入具体实现。HTTP 客户端的公开认证方案从 ClientAuthentication.Scheme 迁为 client.http.enums.AuthenticationScheme，保留 BEARER 和 BASIC 的值与凭据编码。CLI 的 QueryTimeoutException 和 ResultDecodeException 迁入同一制品独有的 client.cli.exception 包。SQL shell、批解码器及两组调用测试引用新类型。为支持同制品跨包访问，两个类型、构造器与超时信息访问器使用 public，并以 @Internal 标明其 CLI 实现范围。Standalone 无公共候选。TestObservability 的四个静态字段持有测试 tracing、logging、metrics 实例与过滤器，按测试 fixture 处置。配置类的默认值仍由同一校验类型拥有。

三进程 `IndependentLocalProcesses` 分别使用冻结的 Coordinator/Worker classpath，两个 Worker 各自拥有 PID、HTTP/Flight 端点与 Native 运行时。SHA-256 清单包含角色清单本身、相应角色 JAR 以及原有 flat/CLI inventory。SQL、CLI 与故障注入共享这一装配。此套件包含并行的真实子 JVM；与持有同一 Native 运行时、测试库或存储容器的其他套件串行执行。既有 `PhysicalTwoWorkerShuffleE2ETest` 则将一个 Worker 和测试 Coordinator 放在一个 JVM 内，第二个 Worker 在独立 JVM 中。

验证边界：独立 JDK 17 编译迁移的公共类、冻结制品核对、三进程装配以及 SQL 异常消费端，最终统一回归核验真实子进程和资源释放。

## Security、AI、Governance 的授权入口与事务

下表的旧类名以 `b51114d0` 为基线。所有包名均省略 `org.pangrova.` 前缀。测试列列出可单独复现该行的断言，交付验证记录区分实际运行与源码核查。

| 旧类及处置 | 现有制品与生产类 | 调用、权威事实与资源 | 对应测试 |
|---|---|---|---|
| `coordinator.http.endpoint.AuthenticationFilter` 移动，`security.authn.AuthenticationManager` 移动并保留认证链 | `server-coordinator` 的 `server.coordinator.http.endpoint.AuthenticationFilter`、`security-access` 的 `security.authentication.AuthenticationManager` | Jersey 过滤器区分公开健康检查、内部路由和用户请求。外部请求经认证后设置当前请求的身份属性。内部路由要求显式内部令牌。Server 的 `SecurityBootstrap` 核对工厂与实例的 bypass 声明，创建失败回收实例，结束时关闭 manager 和认证器。 | `AuthenticationFilterInternalTokenIsolationTest`、`AuthenticatorFactoryDiscoveryTest`、`SignedOidcSecurityBootstrapIntegrationTest`、`LdapAuthenticatorTest` |
| `security.authz.DynamicRbacAccessControl` 移至策略实现。授权变更与重放拆出专门的持久化端口 | `security-policy-builtin` 的 `security.policy.builtin.authorization.DynamicRbacAccessControl`、`persistence-security-postgresql` 的 `persistence.security.postgresql.grant.JdbcPrivilegeMutationRepository` | Server 启动时读取角色和授权检查后端可用性。查询检查读取当前角色、权限、行策略和列掩码。管理 SQL 的 GRANT/REVOKE 进入仓储拥有的事务，锁后检查当前执行者权限，授权行与请求回执同一次提交，调用方可用请求身份确认重放。 | `PrivilegeSqlExecutionTest`、`JdbcPrivilegeMutationRepositoryTest` |
| `worker.ai.governance.ConfigurableAiInvocationAuthorizer`、`worker.ai.context.GovernedAiBindings`、`worker.ai.invocation.AiInvocationService` 移到 Worker 侧 AI 运行时 | `ai-worker` 的 `ai.worker.governance.ConfigurableAiInvocationAuthorizer`、`ai.worker.context.GovernedAiBindings`、`ai.worker.invocation.AiInvocationService`、`AiTaskBudget` | `AiFunctionFactory` 安装调用授权器。未配置时默认拒绝，启用内置 authenticated 模式需要模型、能力与权限白名单。实际回调按已批准的 Task/Attempt 与 OperatorBinding 核对调用，预算登记和取消属于任务生命周期。配置授权器和任务绑定承担不同检查，二者均在出站调用之前执行。 | `ConfigurableAiInvocationAuthorizerTest`、`GovernedProviderTest`、`AiPhysicalInvocationAdmissionTest` |
| 新增发布时模型当前权限协作接口与 PostgreSQL 实现 | `persistence-ai-postgresql` 的 `persistence.ai.postgresql.embedding.JdbcCurrentModelAuthority`、`persistence-workflow-postgresql` 的 `persistence.workflow.postgresql.generation.PostgresGenerationTransactionAuthority` | Coordinator 注入同连接直接角色读取器。Generation 发布工作流先取得授权锁，在同一请求事务中重读租户、表和模型权限，并比较模型 incarnation、revision 与部署签名。模型读取器借用连接，事务的关闭与提交属于上层工作流。 | `ModelRevocationAuthorityIntegrationTest`、`QueryOnlyWriteAuthorityIntegrationTest` |
| `metadata.service.MetadataTagOperations` 拆分；标签操作进入 Governance，列解析保留在 Metadata | `governance-runtime` 的 `governance.runtime.classification.ColumnTagBindings`、`persistence-table-postgresql` 的 `persistence.table.postgresql.policy.PostgresTablePolicyRepository` | Metadata 解析实际列与当前表身份。Governance 验证 PII 类型，通过 Table 的策略端口把绑定持久化到稳定表 UUID，再通知标签投影。重放检查当前 catalog，表改名保留原身份。 | `MetadataTagOperationsTest` |
| `governance.quality.QualityCheckService` 删除，`governance.quality.rule.GovernanceStatementHandler` 移动，抽取本域共享的结果校验 | `governance-runtime` 的 `governance.runtime.quality.GovernanceStatementHandler`、`QualityRuleEvaluation` 与 `RuleBasedWriteQualityGate`，`governance-quality-sql` 的 `governance.quality.executor.sql.SqlQualityRuleExecutor` | 定时与程序化入口从规则仓储取得规则；执行端把结果绑定到规则 ID、表 UUID、严重性及行数，写前入口按规则严重性处理执行失败。SQL 读取请求携带表身份，执行身份显式携带主体与租户。当前生产定时 SQL 读取缺少可重验的持久主体，写前缺少候选提交数据的可见视图，这两项维持不可执行状态。 | `GovernanceStatementHandlerTest`、`WriteQualityGateTest`、`QualityPredicateSqlIntegrationTest`、`QualityAuthorityMigrationIntegrationTest` |

2026-11-14 对表列 16 个测试类进行 JDK 17 源码编译，独立启动 JUnit 与 PostgreSQL，109 项通过、0 项跳过。权限变更故障包括执行中丢失提交确认、审计写失败、第三方越权撤销、按授权选项级联撤销。模型测试实际删除和重建角色后复验延期授权；Worker 回测覆盖缺失 Native 上下文、冒用另一个 operator 的身份、取消时唤醒等拒绝路径。OIDC 使用测试签发者签名并核对受众和服务中断。LDAP 的绑定与错误通过测试目录上下文验证，尚未执行真实 LDAP 服务器与生产证书链的互操作测试。上述 Worker 授权测试没有向商业 AI 厂商发起请求。定时 SQL 与写入后视图的生产支持状态见上表。

## 索引持久化端口与垃圾回收保护

同一 PostgreSQL 仓储提供九组操作。描述符缓存和卡住的构建恢复使用 `IndexDescriptors`，构建输出校验使用 `IndexBuildGenerations`，映射写入使用 `IndexFileMappings`，代际发布使用 `IndexPublications`。提交日志轮询器使用 `IndexCommitLog`；共享熔断状态使用 `IndexBreakerStates`。`IndexCleanup`、`IndexCacheEvents` 和 `IndexFileStatistics` 分别定义清理、跨进程失效和统计操作。需要跨多个能力的消费者和 Server 通过 `IndexMetadataStore` 注入同一仓储实例。数据库连接、发布事务、映射写入的 claim 锁仍由原 PostgreSQL 实现持有。

未实现的持久化操作显式拒绝调用。带创建时间保护的清理候选只包含早于 cutoff 的映射。按状态分页按更新时间、索引 ID 排序。`IndexPersistenceGuaranteesTest` 覆盖缺失实现和清理时间边界，独立契约加载由 `IndexPersistenceApiIsolationTest` 检查。


## 2026-11-14 macOS 双 Worker SQL 采样

在同一台 macOS arm64 主机上，用 JDK 17 和各自从源码构建的 Release JNI 比较两份冻结的发行清单。历史 Java 源为提交 `ec51db67b183fd8af2495c4ab099b4837c6e7ee9`，本轮 Java 源为工作树摘要 `69b7aea5dd0fc63b6cd48d1fd9d5ee7ed288038cc8839517c4385ed997ecae3e`。后续的索引文件指纹变更在此次采样之后。历史版采用该提交 Dockerfile 中实际存在的制品及其运行依赖。其清单还引用了该提交中已删除的 `pangrova-file-worker-adapter`，测试依照当时的 File reactor 装配。

按历史版、本轮版、本轮版、历史版的顺序分别启动全新的 Coordinator、两个独立 Worker、PostgreSQL 与 LDAP。每轮在 Iceberg JDBC Catalog 中写入三个数据文件，每个文件的三行依次为 4、5、5。交替执行常量 SQL 和全表求和，各预热三次、采样十二次。96 个正式查询均有独立 query ID，并通过公开 CLI 验证单行结果为 42、查询终态为 FINISHED、结果已消费且采用 gRPC TLS。计时从启动 CLI 子进程前开始，覆盖 CLI 完成和父进程的查询状态校验。

| 工作负载 | 每版样本 | 历史版中位 / P95（毫秒） | 本轮版中位 / P95（毫秒） |
|---|---:|---:|---:|
| `SELECT 21 + 21` | 24 | 3994.75 / 7686.60 | 3527.89 / 5314.47 |
| `SELECT SUM(id)` | 24 | 5065.55 / 12416.22 | 4651.81 / 6293.05 |

从首个正式样本完成后按秒采集各子进程 RSS。历史版与本轮版最大采样值分别为 Coordinator 389.3 / 393.0 MiB、单 Worker 187.5 / 196.0 MiB、单 CLI 135.8 / 141.5 MiB。RSS 包含堆、Native 及共享映射。本次未独立测量 JNI 拷贝量、网络字节或等待队列时长。后台同时运行 Linux Native 编译和 PostgreSQL 测试，时间分布仅描述该共享主机上的两份冻结清单。

历史版与本轮的运行清单 SHA-256 分别为 `ccec52b0a3cea4c7b38559f65d0c5b14d969c4c3658753e3c0081159667ef46d`、`b3788bd0c2df8c08f23b3389ace39b3b11058595553a5418e435cc2306a8ee11`，Release JNI 摘要分别为 `6cfa0b80749bcb0d322948d394bbd9aebe36f843f5c7071c903a19bc00f0682f`、`75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929`。

[复现命令与各轮分布](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [原始查询记录、RSS 与摘要](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-process)

### 双 Worker 资源、回收与 Shuffle 背压补测

资源对照沿用上述 macOS 机器、两个源码版本、发行清单、JNI 和三文件 Iceberg 数据。四轮按历史、本轮、本轮、历史的顺序启动。每轮各条 SQL 先预热一次，再测三次，共 24 个正式查询。客户端已取回 42 后，采样程序等待同一查询观测到的所有 Worker 任务都返回 Native owner-released 回执，并向 PostgreSQL 核对租户并发槽为零，再接受样本。四轮均正常关闭进程、连接及容器。下表的延迟是从客户端校验结束到观察到上述条件的中位数，含读取数据库的开销。

| SQL / 版本 | 任务数/查询 | 最大 Velox 算子内存预留（字节） | 释放观察延迟 ms | Worker 状态 HTTP 请求体 / 响应体总字节（每查询） |
|---|---:|---:|---:|---:|
| 常量 / 历史 | 1 | 9,280 | 73.7 | 4,102–4,105 / 213 |
| 常量 / 本轮 | 1 | 9,280 | 138.0 | 4,104–4,110 / 213 |
| Iceberg SUM / 历史 | 4 | 75,648 | 148.6 | 16,316–16,363 / 639 |
| Iceberg SUM / 本轮 | 4 | 75,648 | 126.0 | 16,332–16,350 / 639 |

60 个实际任务的最终统计均可读取，Velox 报告的 consumer wait 均为 0 毫秒，spill 均为 0 字节。两种 SQL 产生的单行结果没有形成可观测的毫秒级阻塞。请求体和响应体由 HTTP relay 对收发字节计数，口径限于任务状态端点。

通过 macOS libproc 采集同一角色 JVM 的 RSS、线程、打开的描述符和 socket 描述符，通过 JDK 17 的 `jstat -gc` 独立采集堆使用量。共保存 309 条 OS 观测。比较窗口为各轮第一个正式查询结束至最后一个正式查询完成前，排除启动和进程退出，保留其中 90 条带堆数据的采样。下表值为每类单个进程各指标分别观测到的最大值，线程和 socket 数含服务进程自身的后台工作。

| 版本 / 角色 | 采样数 | 堆 MiB | RSS MiB | 线程数 | 文件描述符数 | Socket 描述符数 |
|---|---:|---:|---:|---:|---:|---:|
| 历史 / Coordinator | 14 | 201.2 | 348.6 | 110 | 546 | 19 |
| 本轮 / Coordinator | 16 | 213.8 | 257.1 | 109 | 442 | 17 |
| 历史 / 单 Worker | 28 | 96.3 | 179.3 | 115 | 533 | 16 |
| 本轮 / 单 Worker | 32 | 95.2 | 188.8 | 123 | 377 | 15 |

另开两轮，通过 JVMTI NativeMethodBind 在实际任务状态、统计和释放 JNI 方法返回时读取所复制的数组长度。两轮均建相同表并执行两个常量查询及两个 Iceberg 求和，两个 Worker 的 PID 均与角色日志核对。历史版的释放方法 14 次 / 6,224 字节、状态方法 186 次 / 85,680 字节；本轮释放方法 14 次 / 6,232 字节、状态方法 318 次 / 127,521 字节。两轮均未调用独立统计方法。轮询次数受进程停留时间影响。该观测覆盖 Worker 从启动到退出期间三个 Native 方法的返回拷贝，其他 JNI 接口、进程总 Native 内存和整机网络线路流量未计量。

另对同版本生产 `ShuffleBufferStore` 建立可重复的慢读者负载。容量为一个记录，驻留预算 0 字节，第一个批次占用 65,536 字节和一个实际 spill 文件。第二个生产者调用 store，消费方至少 40 毫秒后释放第一个记录。历史版 12/12 次返回容量错误，调用时长中位 0.012 毫秒；本轮版 12/12 次等待到消费者归还额度，完整重放第二份 65,536 字节，中位 44.12 毫秒，范围 40.96–45.75 毫秒。24 次运行结束后记录数、spill 字节和文件数均归零。这组测量复用相同 C++ 工具链与 SDK，并分别编译两版本的真实存储源和头文件。它单独覆盖存储层背压，SQL 测量覆盖包含客户端的完整路径。

四轮 SQL、两轮 JNI、存储层对照各自使用独立进程。同期运行的 Linux Native 编译以及部分重叠的其他隔离 SQL 进程都会影响时序。这批资源样本用于界定每条路径的成本和释放行为。历史查询延迟、远端 Shuffle 吞吐与本批数据分别保留原采样口径。[原始 CSV](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-sql-resources) · [复现命令](../testing/speculative-process-e2e.md#measuring-public-sql-latency-across-two-builds) · [远端 Shuffle 吞吐](../testing/shuffle-read-comparison.md)

## 2026-11-14 macOS 发行角色实物核验

Corretto 17 在 Apple M2 上完成 149 个 reactor 模块的安装后，按 Maven 运行依赖顺序冻结 Coordinator 的 363 个 JAR、Worker 的 309 个 JAR 和 Standalone 的 421 个 JAR。三个独立 JVM 使用生产启动器选择各自的 classpath。Coordinator 加载了 `SqlCompiler`，加载 WorkerMain 和 `NativeRuntime` 时返回 ClassNotFoundException。Worker 加载了 WorkerMain 和 `NativeRuntime`，加载 CoordinatorMain 和 `SqlCompiler` 时返回 ClassNotFoundException。Standalone 加载了这四个类型及自己的 main 方法。每个角色中的 Pangrova 主制品均与本批 target 中对应 JAR 逐字节匹配。

Worker 与 Standalone 中的两份 Native 资源分别来自 Worker JAR 和平台 classifier，两者都与本批 Release dylib 的 SHA-256 `75d88126094cc2e5ee8165fc9a81cfce2dd1f4e2144682382f7b2c5b7c46f929` 相同。包含 1,096 条记录的 runtime.sha256 的摘要为 `4d052d60150412c320eae6b97f8d3f1e9f0716ad4360cc805dc55b2c3e69aed3`。本次验证覆盖 macOS 角色制品、类加载与 Native 字节一致性。[角色冻结与复查命令](../testing/speculative-process-e2e.md)

## CDC 与 Metadata 失效链的数据库验证

`CdcCheckpointDatabase` 已删除。`CdcPipelineExactlyOnceTest` 与 `TwoPhaseCommitCoordinatorTest` 每类持有独立 PostgreSQL schema，使用正式迁移初始化，每个用例清空该类的 checkpoint 表。旧行故障用例先把隔离 schema 初始化到 V72，再写入代表行和应用正式 V73，以 PostgreSQL 错误码 23514 验证新坏行遭到拒绝。`CdcEosChaosIntegrationTest` 由实际 Coordinator 与 JDBC Store 持有 PREPARED、提交和恢复状态，Toxiproxy 中断数据库连接，测试 sink 独立记录未发布的 attempt。JDK 17、PostgreSQL 16 和 Toxiproxy 2.12.0 实跑这三类共 44 项，全部通过。

`MetadataSyncPropagationE2ETest`、`MultiCoordinatorArchE2ETest` 和 `MultiCoordinatorRoleRoutingE2ETest` 借用 SQL 测试集群的 PostgreSQL，每类初始化独立 schema，正式 V42 提供提交顺序触发器。跨连接用例将同一表放入读取侧缓存，提交失效消息，等待独立 poller 真正驱逐其缓存项。三类在 macOS arm64 上使用 JNI、HTTP SQL、Iceberg candidate 和真实模型 fixture 实跑 35 项，全部通过。随后将其中一个直接写失效日志的用例替换为生产投影事务的重放测试并单独通过，核对持久化表、回执和一条失效记录。两个 Coordinator 的缓存验证使用独立连接和 poller，共享测试进程。

## 查询注册、任务身份和物化命令

| 原入口 | 处置与调用路径 | 状态、资源及验证 |
|---|---|---|
| `coordinator.query.QueryManager` 提交、恢复、清理与 drain | 提交和恢复均先向 `query.lifecycle.QueryRegistry` 预留查询 ID，authority 初始化成功后发布该实例。保留策略通过同一注册表移除过期查询。 | 注册失败释放预留；已发布的查询在物理清理完成前占用 drain 计数；同一状态机只释放一次。`QueryRegistryTest` 覆盖初始化中遇到重复提交、读取半初始化实例、初始化失败后重试和重复完成。 |
| `coordinator.scheduler.RemoteTaskDispatcher.bindControlIdentity` | 任务及 Shuffle 身份绑定移至 `execution.binding.TaskControlBinding`。Dispatcher 继续负责发送前检查当前 authority、Worker 路由、HTTP 超时和重试。 | 已绑定的任务和 Shuffle 必须匹配同一 plan epoch 与 execution owner。封装、冲突拒绝、epoch 追赶、COPY TLS 和 HTTP 测试共 24/24 通过。 |
| `materialization.usecase.MaterializationService` | 定义计划和重放验证归 `MaterializationDefinitionPlanning`，CAS 调度更新归 `MaterializationSchedules`，级联删除归 `MaterializationRemoval`。命令仍使用同一 repository，先校验全部待删除对象的管理权，再逐对象更新。 | CAS 重试、权限、source pin、定义重放、refresh、发布后回收及恢复相关测试共 66/66 通过。 |
| `server.coordinator.bootstrap.ModuleIntegrationBootstrap` 维护主节点回调 | 选举启动中任一步失败，撤销该任期并逆序停止已安装服务；失主时继续尝试每个停止动作。 | `ModuleInvalidationLifecycleTest` 注入调度器拒绝，检查下一任期能重新启动，重复通知不积累任务。 |

查询注册、终态、读 pin、恢复、索引持久化保证及清理边界的联合验证为 77/77，测试使用 JDK 17 和 PostgreSQL。该数字不包含角色发行物的独立进程验收。

## Worker 任务注册、取消和 Catalog 操作作用域

| 基线职责 | 生产入口和当前类 | 事实、资源及故障语义 |
|---|---|---|
| `worker.task.TaskManager` 中的任务表及本地 ID 计数 | Worker HTTP → `TaskManager` → `TaskSubmissionHandler` → `WorkerTaskRegistry` | 注册表独占本地资源 ID 和对应的 `TaskInfo`，并发发布在同一临界区检查容量。已取得资源但回滚失败的记录可超过普通准入限额，保留至实际清理完成。精确尝试的准入/重放回执另由 `TaskAdmissionRegistry` 持有，可在资源释放后继续查询。 |
| `TaskSubmissionHandler` 的取消与回收 | 精确 seal、任务退役与 Worker 内部取消经 `WorkerTaskCancellation` 进入现有 `TaskLifecycleManager` | 继续由同一个 `TaskInfo` 进行终态 CAS。Native 拒绝取消则传播失败；同一查询的其他任务仍能收到取消，失败尝试保留在查询索引中供重试。收到取消仅代表请求被接受，完整退出还需要 Native 回执。 |
| drain 的全局倒数锁存器 | `WorkerShutdownSequence` → `TaskManager` → `TaskDrainExecutor` | 拒绝新记录后按精确本地 ID 等待终态。重复通知、同时 drain、已移除的终态任务均按实际任务状态处理。达到超时或等待线程被中断时对仍活跃的尝试强制取消，保留中断标记。 |
| `TaskRetryExecutor`、`TaskRetryConfig`、`TransientFailureClassifier` | 退役 Worker 启动时创建并记录为已启用的无调用方对象及其孤立单测。真实的故障重新调度由 Coordinator 的 `QueryBootstrap`、Scheduler 的 `SpeculativeTaskLauncher.retryFailedTask` 和 `TaskRetryAdmission` 协作 | 新 attempt 的分配和预算保留在控制面。Worker 报告实际失败，不复用原 attempt 重新发送写入。 |
| `worker.catalog.WorkerCatalogRuntime` 的版本缓存与任务输出验证 | `WorkerCatalogRegistry` 持有 tenant、catalog、incarnation、revision、fingerprint 对应的 connector，`WorkerCatalogRuntime` 绑定任务输出，`CatalogControlLease` 限定控制操作的使用期 | 注册表负责单次初始化、LRU、租约计数与关闭失败后的重试。热缓存入口校验 SHA-256 算法与摘要长度；控制操作还要求 output 内嵌的 deployment 与请求完全相同。每个任务持有自己的 session 与经共享 codec 验证的输出句柄。Native 写调用由 `NativeWriteAdmission` 防止 release 与当前调用并发。 |

2026-11-14 使用 JDK 17 在独立输出目录编译 Worker 家族和集成调用方 484 个源码单元。任务提交、JNI、内存/结果释放、取消和准入组 82 项通过。随后分别运行包含 drain 竞态的 33 项、包含真实 Native 取消拒绝后重试的 33 项，均通过，两个重跑组与前组存在重叠。真实认证 Coordinator、Paimon/Lance 与 JNI Catalog 生命周期组另有 9 项通过，覆盖 1024 个活跃租约满载、LRU、并发创建、关闭与部署拉取竞争、错误指纹算法和跨部署输出拒绝。以上结果来自 macOS arm64。

### 标量索引剪枝和 CDC 水位

| 原职责 | 当前归属和调用路径 | 边界与验证 |
|---|---|---|
| Coordinator 的 `FilterPredicateExtractor` | Planner 追溯 Filter 输入的列序号并解析 Calcite 谓词。直接投影可改名和重新排序。无法证明向下推安全的计算、聚合、窗口和 LIMIT 会终止该条件的下推。 | 通过真实 Calcite 计划验证同名遮蔽、表达式列和 LIMIT 前后谓词。动态 IN 成员不能丢失。 |
| Coordinator 的 `IndexSplitFilter` 和 `filter` 包 | `indexing-lifecycle` 的 `core.pruning` 拥有分片剪枝、已发布代际的选择、L1 并发和 L2 提示。Coordinator 借用绑定到当前会话的 Catalog 查询能力。Server 选择过滤器和持久化实现。 | PostgreSQL 在同一 REPEATABLE READ 事务中读取活动 manifest 和映射。跨两批读取之间发布新代际的测试检查原批次一致、后续请求可见新发布。确切查询 snapshot 与 manifest 比较，可复用来源较早的不可变文件。 |
| `IndexHint` 和 `IndexPredicateAnalyzer` | 每个提示携带分析时读取的代际。去重使用完整的索引、代际、类型和谓词等值关系。 | 查询入口通过发布端口读取确切代际。新建、未发布、已取代和 snapshot 冲突分别核对；缺少合格提示时保留原分片。该次读取不授予清理租约。 |
| `WatermarkTracker`、`CdcPipelineLoop` | Pipeline 在恢复时加载已提交水位。Tracker 的每个本地观察实例拥有水位、最近事件和状态；持久化端口拥有单调位置。 | 晚到的旧写入完成后只能更新已分离的观察实例。恢复不计为新事件。水位、管道、调度和 PostgreSQL 等相关测试 30 项通过。 |

## SQL DDL、Catalog Schema 与分片规划

| 原代码与操作 | 当前类、制品和生产调用 | 权威与边界 |
|---|---|---|
| `coordinator.query.statement.DdlExecutor.declaredTableMetadata`、同包 `DdlTypeMapper`、`GeneratedColumnDefinitionCompiler` | `pangrova-planner` 的 `planner.metadata.TableDeclarationCompiler` 将解析后的 CREATE TABLE 编译成 ConnectorTableMetadata。其同包组件解析嵌套字段、FILE、VECTOR 精度和生成列约束。ALTER 的类型映射复用 DdlTypeMapper。 | Planner 使用已有 Parser、Connector API、Generation Codec 与 Protocol 依赖。该操作无连接或状态。DDL 执行器持有会话，负责调用真实 Connector 创建、按相同部署登记表身份，并在成功后触发失效。 |
| `coordinator.metadata.MetadataBootstrap.buildCalciteSchema` 两个入口 | Server 的 `PlannerWiring` 给 Planner 的 `MetadataSchemaBuilder` 和 `CatalogStatisticsProvider` 传入同一会话的 `PlanningReadBindings`。集成 fixture 使用相同入口。 | 身份、租户与选中的快照随查询进入 Schema 和 Statistics。CatalogRegistry 及查询持有的 Connector Metadata lease 保留各自的关闭职责。 |
| `coordinator.indexing.IndexHintGenerator` | Planner 的 `PlanSplitSourceExtractor` 为整份查询和分片返回 `ScanPredicates`。Coordinator 的 `IndexAwareSplitFetcher` 以真实租户和列请求 READY 索引，随后调用 Indexing 的 `IndexPredicateAnalyzer` 和 `IndexSplitFilter`。 | 计数发生在按表去重之前。同一张表出现两次也禁止传播单扫描条件。授权与取消异常继续向上抛出。旧入口提供的包含 STALE 和按文件数估算覆盖率重载均无生产调用，覆盖率分析仍在 Indexing；线上标量路径选取 READY 索引。 |
| `DispatchContextEnricher` 对 Calcite 根节点的直接读取 | 接收 `PlannedQuery` 的 `indexing.planner.vector.VectorSearchRewriter` 使用已有的 Indexing → Planner 编译依赖识别排序形状。查询执行侧继续提供带租户的索引查找与绑定的 snapshot。 | Coordinator 的生产源码无需 Calcite 类型。向量索引描述和 READY 状态归 Indexing。Planner 无需引用索引实现。 |
| `coordinator.governance.QualityReadSql` 的 Calcite 方言渲染 | 原调用仅来自治理谓词和质量读取的 PostgreSQL 测试，类放入原制品的测试源码。 | 真实治理规则由 Governance 的已装配执行端口处理。运行支持范围见 data-governance.md。 |

其余 DDL 逐项按操作分工。SQL task 和 StatementDispatcher 负责会话、准入、SQL 授权与结果行。Catalog 创建、修改、删除、描述进入 CatalogRegistry 与 Metadata 服务。CREATE/DROP/ALTER TABLE 的所有权、ADOPT/UNLINK、重命名与维护策略经 TableControlDdlOperations 下达 Table。列标签与遮蔽保持原有 Governance/Security 和持久化协作，查询缓存失效与可选索引演进仍在成功的变更之后触发。CTAS 的计划派生与生产的写入/登记步骤分别保留。索引 DDL 和物化、视图、模型的 SQL 入口分别调用自身领域；将它们合并到 Planner 会引入控制流程和权威写入的反向依赖。

原 DdlExecutor 的通用 SET 会记录原始值并空成功。显式的 SetSessionTask 现在返回 PARSER_UNSUPPORTED_FEATURE。SQL 诊断在记录、审计和查询历史中屏蔽这类原始赋值。请求级配置和 USE BRANCH 的连接内回执各走既有协议。

macOS arm64 使用 JDK 17 在独立目录编译上述生产入口和其测试调用方。Planner/DDL/Schema/质量谓词的定向组通过 86 项，其中包含真实 PostgreSQL、Paimon 与 Calcite。另以真实 Iceberg、PostgreSQL、JNI Worker 和 Coordinator 运行 SnapshotBoundIndexE2ETest 与 IndexJoinE2ETest，8 项全部通过，两个测试组均无跳过。SET、HTTP 错误分类与诊断渲染组另有 16 项通过，真实端点返回 HTTP 400／PARSER-0002，四种赋值的 QUERY_SUBMITTED 日志均显示 SET [redacted]，记录中没有测试值。

## File 授权与 Worker 操作审计

| 原入口 | 真实职责与处置 |
|---|---|
| `file.api.audit.FileOperationAudit.fileOperation` 和 `coordinator.query.audit.FileSpaceAuditEmitter.fileOperation` | 原调用发生在签票之后。FileGrantAudit 通过安全域 AuditEventWriter 发出 FILE_ACCESS_GRANTED，FileOperation 明确区分 HEAD、LIST、READ、WRITE、DELETE。未识别的输入在签票或 Worker 解码时拒绝。 |
| `coordinator.enums.FileSpaceMutation` | 此枚举仅选择审计事件，现已删除。SQL 入口在真实成功的 namespace 操作后按 Volume 或 Stage 明确选择事件。文件属性变更继续使用 file.api.space.FileSpaceMutation 的既有 sealed 操作及 PostgreSQL 原子更新。 |
| `worker.file.FileSpaceFileResource` | POST head 和 HTTP HEAD 产生 FILE_METADATA_READ；LIST 按页确认，PUT 和 multipart complete 于 provider 返回后确认，GET 在请求字节数匹配、写出、flush 和关闭源流后确认。DELETE 要求 provider 成功返回。认证或版本失败、取消和部分流写出不产生成功事件。对象存储不与审计 sink 共用事务。 |
| `server.worker` 的独立及嵌入入口 | 将现有 LoggingAuditEventWriter 传给 Worker bootstrap。Worker 只持有 Security API 的 writer 并借用它，保持 Worker 与 Coordinator、JDBC 解耦。生产结构化日志为 Worker 侧完成证据；Coordinator 使用其既有日志或组合后端。 |

JDK 17 独立编译和 macOS arm64 的 File/Worker/权限/HTTP 测试通过 91 项，包含真实 PostgreSQL 审计查询、MinIO S3 multipart 与 HTTP 到期、断连。最后将 namespace 事件也切换为必需的 writer 后，编译 43 个源文件，重跑相关 PostgreSQL、URL 和授权测试 13 项通过，两组有重叠且均无跳过。PG 测例分别核对 6 次签票和 7 次实际操作，覆盖同一 READ ticket 的独立 HEAD、Range GET、客户端写失败和条件删除失败。直连存储的预签名 URL 按回执记录实际到期时间。

## 表创建、标签投影和运行时命名

| 入口 | 责任和验证 |
|---|---|
| SQL 和 Metadata 的 CREATE TABLE | 两个入口向 TableRegistrations 传递实际执行 CREATE 的 Catalog handle 所属部署。登记失败只能在该部署记录孤立对象。目录被重新创建、表名被另一个身份占用或发生待完成改名时，都保留当前身份。PostgreSQL 在同一事务退休旧记录、插入新 UUID、更新名称并写失效事件。用持久化后的 revision 作为并发令牌，保留数据库时间精度。六组测试共 127 项通过，包括强制 INSERT 冲突后的回滚与外部 CREATE 期间 Catalog 替换。 |
| Metadata 列标签与 Governance | ColumnTagBindings 在构造时绑定表策略仓储与事件源。Coordinator 使用同一事件源安装监听器并重放已有标签。治理关闭时移除自身注册的监听器。Paimon 联测使用同一个 PostgreSQL 权威构造控制记录、标签、遮蔽和元数据读取。 |
| OpenTelemetry、Worker HTTP、任务状态与维护血缘 | 两份重复的 trace/span ID 读取合并到 CurrentSpanIds；Worker 访问日志读取当前 span，Native 执行使用提交时捕获的执行身份。TaskReportCompletion 将已接受的报告路由到逻辑任务。显式组件调用者将 MaintenanceLineageListener 注册到 ObjectDependencyService 后，依赖写入产生血缘通知。PhysicalTypeProjectionDeclaration 校验 Substrait 物理类型的投影规则。 |

### Worker 与 Protocol 的公共枚举、错误和常量

| 所有者 | 类型与调用方 | 语义及验证 |
|---|---|---|
| `protocol.enums` | `ReportedTaskState` 由 Worker 的任务状态投影生成，`DataTicketType` 由 Flight 签发和验签使用。检索合并策略由 Execution API 拥有，Protocol 绑定直接引用该值 | Worker 的八种执行状态分别映射为六种对外状态。移除仅包装该枚举且没有调用方的旧 `protocol.execution.TaskStatus`。Proto 的编号和未知值处理由生成类型拥有。Flight 签名继续使用原票据格式。 |
| `worker.enums` | `BackpressureAction`、`ControlEpochFenceReason`、`TaskAdmissionReason`、`TaskCancellationStatus`、`NativeTaskFailureCategory`、`NativeMemoryEventType`、`NativeFunctionType` 和 `DmlOperation` | 分类结果、准入、取消及配额事件在原状态机处理。JNI 在构造写请求前以稳定的 `0=INSERT`、`1=OVERWRITE`、`2=DELETE` 解码操作；未知代码触发错误。Worker 与 Native 必须匹配发行。 |
| `worker.exception` | `NativeQuotaSendException`、`CopyPreparationHaltedException`、`CopySourceReadRejectedException`、`TenantQuotaExceededException` | 保留租户标识、配额类型、HTTP 拒绝状态及执行错误类别，原调用方继续辨别超时、取消、策略拒绝、后端故障。 |
| `protocol.constants.BuiltInCapabilityIdentifiers` | Planner、Worker 与 Protocol 的能力 ID、版本、格式号及保留列名 | 能力目录继续构造含签名的引用。原字段值与线上格式保持一致，库存和协议描述符由真实 Worker 握手验证。 |
| `worker.constants.SpillMetricAttributes` | 内存事件和 spill 观测共用的域、原因标签 | 记录器拥有仪器及 gauge 生命周期。配置默认值与编码边界继续由验证、使用它们的配置或协议类型拥有。 |


### Connector 进程插件目录与租约

| 原位置与能力 | 现 owner、真实调用及验证 |
|---|---|
| `org.pangrova.connector.runtime.lifecycle.PluginManager`、Coordinator 空 URL loader | 退役。Connector Runtime 的 `ConnectorPluginInventory` 在启动时一次性装配 role-scoped 工厂。内置工厂走角色 classpath，外部目录每个安装独享一个 loader。角色内重名、错误版本、缺失工厂及初始化异常会中止装配并逆序释放资源。更新插件需要重启。 |
| Coordinator `PluginConnectorHandleFactory` | 进程从 `connector.plugin-directory` 读入目录，经 `ConnectorFactory` 建立真实 catalog handle。CatalogRegistry 关闭所有 handle 后，Coordinator 才关闭 inventory。 |
| Worker `WorkerCatalogRuntime`、`WorkerCatalogRegistry` | Worker/Standalone 经服务端配置装配。精确 deployment revision 和 tenant lease 归 Worker。关闭拒绝新工作，活跃租约阻止回收 loader，最后一次租约释放后重试关闭。插件 inventory 不保存 Catalog 事实，也不持有查询或持久化访问权。 |
| 验证 | `ConnectorPluginInventoryTest` 打包真实 JAR，检查隔离、角色过滤、重复注册回滚、资源关闭和无 factory 的显式拒绝。`ExternalCatalogRolesIntegrationTest` 使用同名插件的双角色 JAR，经 CatalogHandle 和认证 HTTP revision 读取验证两个角色独立创建及关闭 connector。 |

本批在独立工作树使用 JDK 17 编译 Coordinator、Worker 及其 140 个 reactor 项目。定向运行上述外部 JAR、内置 Catalog、类加载和嵌入角色配置共 34 项，全部通过，无跳过。双角色用例还检查独立的依赖 JAR 和带 bearer 认证的本地 HTTP 请求。该用例在同一 JVM 创建两个独立角色 inventory，未执行独立进程 SQL 查询。


### DAG 终态关联、代际和 Shuffle 重派

| 能力 | 生产 owner 与调用 |
|---|---|
| 早到、重复、跨查询和失效报告 | `TaskStatusReceiver` 校验已发行的 attempt，`TaskReportCompletion` 转给 DAG。每个活动 DAG 独享 `TaskTerminalRegistry`。注册绑定 query、Stage 实例和 generation，首个终态只交付一次，query 退役清空早到事件及交付标记。 |
| Stage 终态和重试 | Stage 锁内登记任务终态并选出唯一终态转换，锁外运行状态监听器。Stage 独占代际及预算，`RetryStageHandler` 校验错误类别和配置预算。重复失败、取消、旧代际均不能推进下一代。测试专用 StageRetryManager、30% 失败阈值、外部计数器入口、无人装配的 TaskRetryCallback 已移除。 |
| Worker 死亡重派 | 入口经过 `QueryBootstrap.retryTaskOnDeadWorker → SpeculativeTaskLauncher → DAGLifecycleController`。独立无 Exchange 的任务可以在另一 Worker 上预留、提交或回滚。当前无法为新 attempt 重签输入 read grant/ticket，更新孤立 producer 的输出位置也未接入。原请求或 DAG 含相关 Exchange 时拒绝该组合。可重试的源 Stage 仍可在下游尚未调度时整代重建。 |
| 物理失败与查询终态 | 移除 `CoordinatorTaskTracker → TaskFailureCallback → QueryManager`。已失败的旧物理 attempt 仍承担 drain，DAG 监听器根据真实终态触发结果或写入提交。无 DAG 的报告仍经 task tracker 判断。主动取消逻辑 DAG 会唤醒 finalizer，再等待实际 Worker drain。 |
| 暴露结果与派发失败 | OUTPUT 将 Flight 地址交给结果入口时锁定该 attempt，已有 DML 片段及已被消费端绑定的 producer generation 也禁止透明重放。无 Exchange 的独立读重派先绑定替代 Worker、控制面与冻结输入。已进入网络且 ACK 丢失的派发保留替代请求，经原目标 Worker 取消；回滚恢复旧槽位并重放在交换期间到达的首个旧终态。 |

独立快照基于 7fd172e47080af083f34be70d7f09653ab9201f1，叠加本节列出的工作区修改。JDK 17、macOS AArch64 下，启用 native-required 的 coordination-control 定向回归通过 152 项，含 PostgreSQL 容器 durable report 8 项、Native/JNI/Flight 输出与关闭 3 项、Native/HA ownership 7 项。Server 死 Worker 入口另行重跑 2 项通过，覆盖替代槽位、不可恢复失败和丢失检测与成功终态竞争。首次 Native 验证发现快照缺少本地动态库，补入同一份实际发行字节后重跑。Server fixture 首次将未发行的逻辑槽位留给物理清理等待，修正 fixture 注销后重跑通过。本批没有验证两台独立进程间迁移正在被客户端读取的 Flight 流；该组合在发布端点后拒绝重试。

### 进程服务登记与实际调用

从各制品 `src/main/resources/META-INF/services` 读取文件名与去注释后的实现行，得到 37 份描述符、24 种服务接口、49 条登记。下表按接口计数。只统计仓库交付的登记。使用者另外安装的 Federation 插件，以及由 C++ 注册的 Native 算法，都有各自独立的装配入口。运行角色共用应用 classpath 的制品按角色边界发现。Connector 另支持显式插件目录和子加载器。

| 服务接口与条目数 | 实际选择与调用 | 资源、失败与验证范围 |
|---|---|---|
| `java.sql.Driver` · 1 | JDBC DriverManager 按 URL 选择 PangrovaDriver，Connection 经共享 HTTP/Flight client 执行查询 | Connection 归调用者关闭，最后一个共享租约释放 client。独立 JDBC/CLI 取消与随后查询见本台账客户端章节。 |
| `AiModelCatalog` · 1 | Worker 的 NativeCapabilityAdvertisement 使用角色加载器读取 WorkerAiModelCatalog，并按函数插件部署的模型生成调度声明 | 配置存在却未安装工厂、多个目录拥有者均拒绝。目录查询描述模型，实际计量属于任务。见 WorkerBootstrapCapabilityAdvertisementTest 和 FunctionProviderDiscoveryTest。 |
| `CoordinationProviderFactory` · 2 | CoordinatorLeaseBootstrap 在当前角色 classpath 发现 JDBC 和已安装的 ZooKeeper。选中工厂同时提供分布式租约和成员注册，重复 ID 或别名、版本不匹配均在连接后端前拒绝。 | JDBC 借用进程连接池。ZooKeeper 独占 Curator client，连接或初始化失败时回收。进程退出先 drain 租约与成员消费者，再关闭 Provider 和连接池。CoordinatorLeaseBootstrapTest、ZooKeeperDistributedLeaseTest。 |
| `AiProviderFactory` · 2 | 配置模型 dialect，由 AiProviderFactories 的角色加载器发现 OpenAI/Qwen。Worker 注册表用同批工厂建立模型客户端 | 每个注册表关闭所建客户端。AiProviderFactoriesTest、QwenProviderRegistryTest、OpenAI HTTP 协议测试覆盖已实现的操作。厂商公网账户未在本批调用。 |
| `CacheStoreFactory` · 2 | Coordinator 配置 `metadata.cache.l2.provider` 选择 memory 或 Redis，TieredMetadataCache 实际查写 L2 | `disabled` 只启用 L1。选择 L2 时全部工厂经 ID、重复及版本验证再打开。空、首尾空白或占用 disabled 的 ID 拒绝。TieredMetadataCache 关闭 L2。MetadataCacheDeploymentTest、RedisCacheStoreTest。 |
| `ConnectorFactory` · 3 | Coordinator 与 Worker 分别拥有 ConnectorPluginInventory，按 Catalog 类型解析 Iceberg/Paimon/Lance。外部目录按插件加载 | 启动拒绝重名，卸载随进程关闭，Worker 待活跃 lease 归还再关闭。见前节真实插件 JAR 的双角色验证。 |
| `BlobStorageFactory` · 3 | AI FILE 的 storage 配置可选 Local/S3/OSS。Worker COPY 单独要求 managed、精确身份与请求预算，目前仅 S3 提供 | 存储随函数工厂或 COPY runtime 关闭。BlobStorageFactoryDiscoveryTest、OssBlobStorageHttpTest 和 S3/MinIO 测试。OSS 的 HTTP 协议证据不涵盖云账号与区域网络。 |
| `ObjectStoreAccessorProvider` · 2 | Worker 文件空间采纳 Local/S3，按 StorageProfile 的存储种类选中 provider，返回借用的 accessor | Worker 持有 provider 及其缓存。重复、未安装、关闭后申请均拒绝。ObjectStoreAccessorFactoryLifecycleTest、S3ObjectStoreAccessorMinioTest。OSS 未登记此接口。 |
| `FunctionFactory` · 1 | NativeFunctionBootstrap 汇总发现的 scalar/aggregate 描述并注册 JNI，当前内置登记为 AiFunctionFactory | 先留存可关闭工厂，再读取其配置和函数。重复名称或失败进入回收，成功注册后才发布 callbacks。NativeFunctionBootstrapTest、FunctionProviderDiscoveryTest。 |
| `FunctionCallbackScope` · 1 | NativeFunctionBootstrap 通过 WorkerCallbackProviders 绑定 WorkerAiCallbackScope，NativeFunctionCallbacks 执行真实回调前安装当前任务授权 | 每个调用退出后还原作用域，未安装时可运行普通函数。重复安装拒绝启动。WorkerAiCallbackScopeTest。 |
| `FunctionFailureProjector` · 1 | NativeFunctionCallbacks 请求 AiFunctionFailureProjector 编码模型错误，普通函数错误由 Native 正常处理 | 可选、资源独立的单例服务。两个候选先拒绝再实例化。NativeFunctionBootstrapTest 使用实际服务资源加载两个候选，AiFunctionFailureProjectorTest 覆盖错误负载。 |
| `LineageBackendFactory` · 2 | Coordinator 的 `pangrova.lineage.backend` 选择 LOG 或 HTTP。HTTP 在创建之前要求 durable outbox | LineageEventEmitter 管理消费线程与 backend，关闭时排空并确认关闭结果。LineageBackendConfigurationTest、OpenLineageHttpTransportTest 以 Marquez 和 PostgreSQL 检验 HTTP 写入。 |
| `IndexStrategyProvider` · 9 | IndexingBootstrap 通过角色加载器安装 BTREE、BLOOM_FILTER、PREFIX、NGRAM_BLOOM_FILTER、BITMAP、HASH、SPARSE、R_TREE、GIN。构建、编解码和计算共用同一组实例 | 算法无外部资源。重复类型拒绝，缺失按具体类型拒绝。IndexStrategyDiscoveryTest 实际构建、编码、解码并准备每种索引，也检验线程上下文加载器变化。Vector 和倒排执行另归 Native。 |
| `RefreshStrategyFactory` · 3 | RefreshResolutionPolicy 选择 FULL、APPEND_ONLY、PARTITION_REPLACE，MaterializationAttemptExecutor 验证输入、生成计划并校验完成凭据 | 无外部资源，缺失 FULL 拒绝，CDC 等无实现策略显式拒绝。RefreshStrategyDiscoveryTest 和各策略的实际计划测试。 |
| `TableStoreFactory` · 2 | Coordinator 使用 postgresql 创建 control、statistics、policy、maintenance 四个仓储。memory 工厂仅用于显式的进程内权威 | Server 持有 DataSource，仓储借用；整个 inventory 先验版本。缺失 PostgreSQL 失败。TableStoreDiscoveryTest 验证正式装配；InMemoryTableControlStoreTest/JdbcTableControlStoreTest 另测真实行为。 |
| `AiTaskLifecycle` · 1 | NativeTaskExecutor 建立一次可选 WorkerAiTaskLifecycle，注册、计量、取消和回收同一 attempt 都找该实例。NativeCreationOwners 即使在失败恢复后也保留其回收者 | 进程无 AI 实现时使用既有无配额契约，重复实现拒绝。WorkerAiTaskLifecycle 向 AI 运行时转交计量，JNI 句柄由 executor 回收。NativeCreationRecoveryContractTest 与真实 Native Worker fixture 的证据范围不同。 |
| `WorkerSelectionStrategyFactory` · 1 | `coordinator.query.placement-provider` 选择 load-balanced。QuerySchedulingWiring 将策略交给 TaskScheduler，对已过 Worker 准入的集合排序 | 算法无外部资源。缺失、重复和版本不符拒绝。QuerySchedulingStrategyFactoryTest、TaskSchedulerStrategyBoundaryTest。 |
| `AuthenticatorFactory` · 3 | Coordinator security 配置选择 NOOP、LDAP、OAUTH2，产物进入请求认证过滤器 | SecurityBootstrap 持有 authenticator。AuthenticatorFactoryDiscoveryTest、LdapAuthenticatorTest、OAuth2AuthenticatorTest 与 AuthenticationFilterIntegrationTest 分别验证工厂、认证协议及入口。单元 LDAP bind 替身与独立发行回归使用的真实 LDAP 分列。 |
| `SecretProviderFactory` · 2 | Coordinator/Worker 的 SecretResolverRegistry 按配置引用前缀派发 ENV 或 FILE，Catalog/AI 在消费时解析 | 产品读取环境或文件，不持有长连接。前缀重复、未知及文件访问错误显式失败。EnvSecretProviderTest、FileSecretProviderTest、CoordinatorSecretDiscoveryTest。 |
| `ShuffleCoordinatorFactory` · 1 | CoordinatorShuffleProviders 为 REMOTE 选择 pangrova，要求配置能力，真实提交选择与 receipt 随执行权威保存 | 会话生命周期由 Coordinator 持有。PangrovaShuffleProviderIsolationTest 与前节发布、故障及读取租约证据。Direct/Local Native 依靠 Native 部署能力选择。 |
| `SpoolStoreFactory` · 1 | SpoolBackedExchangeManager.open 按配置选择 local-fs，识别 API 版本，以精确 producer attempt 读取已完成分区 | Java page 管理器关闭本地 spool。SpoolBackedExchangeManagerTest、LocalFsSpoolStoreTest 覆盖真实目录、空分区和未知选择。此库入口目前未被进程 Native 执行链调用。 |
| `NativeShuffleWorkerFactory` · 1 | WorkerTaskBootstrap 为 REMOTE 验证 pangrova 工厂与会话能力，生产者由 JNI egress 调用 | WorkerTaskOwnership 持有会话，封住新任务后等待生产者及读取者排空。外部 Provider 含 Pangrova 一种，Celeborn 尚未实现。 |
| `CdcFormatFactory` · 3 | Coordinator 启动将 Canal、Debezium、Maxwell JSON 装入 CdcRecordConverterRegistry；StreamDdlExecutor 验证 SQL FORMAT，CdcPipelineLoop 调用选中解码器 | 无资源、可共享的解码器。先验证全部工厂版本和标识，再创建并比对 formatId。CdcFormatDiscoveryTest 实际解析三种不同格式，CdcRecordConverterRegistryTest 验证不兼容工厂不能触发任何创建。 |
| `CdcSourceFactory` · 1 | CdcSourceRegistry 验证 SQL SOURCE 及配置，StreamScheduler 为每个运行中的 Kafka 流创建独立 reader | pipeline 持有 poll/seek/commit/close。恢复须使用准确 checkpoint 与 topic 分区。KafkaCdcSourceReaderIntegrationTest 使用 Kafka 容器进行实际消费和恢复。 |

基线快照 `e15ea186992cff75622a14fed7c61d3b4aab2d07` 使用 JDK 17 在隔离工作树构建 147 个 reactor 项目，32 个定向测试类共 232 项通过，零失败、零跳过。其中 Kafka 分区消费与恢复 4 项，Redis 1 项，MinIO 14 项，Marquez 和 PostgreSQL 验证 4 项，OSS 官方 SDK 对 HTTPS 服务夹具 16 项。最终 Java 源码快照 `6aea9e39e72cb3b9955ff0411f9db98841b9294f` 再检验 16 类共 75 项，零失败、零跳过。其中 PostgreSQL 表仓储 12 项、内存表仓储 4 项、Native Worker 建立、读取、排空和重建 3 项。本轮的 Native 用例使用 macOS arm64 动态库。两批有重复测试，分别计数。

#### SPI 可达性首次失败的 22 个类型

| 数量 | 归属与处理 |
|---|---|
| 6 | Metadata 测试 JAR 的质量执行器、依赖图内存夹具、联邦清单、同步记录、投影记录和 Catalog 注册表都来自测试源码。门禁过滤 Maven tests、native-fixtures、query-fixtures 分类器，仍加载生产 JAR 和其中的服务登记。 |
| 6 | TableAsset、ViewAsset、MaterializedViewAsset、FunctionAsset、ModelAsset、VolumeAsset 是 sealed Asset 的全部值类型。门禁检查所有 permitted 子类型确实为 record，并按实现的其他契约继续判断 SPI。 |
| 5 | 仅测试引用的 Governance 内存审计、审计配置、Catalog 标签、列标签、质量规则移至测试源。Coordination 和 Server 对质量规则、审计配置的复用由仅含被引用类的测试 JAR 提供。 |
| 2 | 手动填充且缺少 fenced worker 身份的 Shuffle inventory 移测试源。按名称传播列标签的 LineageTagPropagator 连同仅为该示例提供事实的 LineageCatalog 和查询帮助类移测试源。生产授权仍读取按稳定 table ID 持久化的绑定。 |
| 3 | 可替换能力的配置、工厂、角色装配、调用与关闭由强类型工厂/SPI 统一；`SpiWiringVerificationTest` 6 项与 `ServiceLoaderReachabilityTest` 6 项在最终源码上通过；TableWriter 双实现因无生产工厂与消费入口整体退役。 |

历史豁免中 123 类已能从正式入口抵达，另 13 个 FQN 已无对应生产源码，均移除。剩余 25 个历史条目继续单列。既有 `@DeadCode` 必须提供跟踪项且不可达，已标明的 #19 COPY URI 准入实现未计入支持。未因本轮失败新增历史豁免或添加 DeadCode 标注。

JDK 17 隔离源码 `b9565713a25429d54258c726244f6bc0ff0e297e` 的 SpiWiringVerificationTest 5 项和 ServiceLoaderReachabilityTest 6 项全部通过，零跳过。首轮同批领域测试 177 项通过，扩展门禁先报告既有的 COPY #19 标记，恢复仓库对跟踪注解的识别并验证其内容后重跑门禁通过。另显式启用 Metadata integration 分组，JdbcObjectDependencyStoreIntegrationTest 在 PostgreSQL 运行 45 项，含并发、故障回滚、图合同。干净生产输出未包含移动的 9 个类，实际 Governance tests JAR 仅含 2 个跨模块借用的仓储夹具。

### 历史 SPI 可达性 25 项及 URI COPY #19 逐项复核

下表的 25 项来自前一轮可达性基线。角色装配、独立库验证和仅测试夹具分别注明。PostgreSQL 回归从空 schema 执行整套正式 migration。

| 原条目 | 生产入口、能力与处置 |
|---|---|
| IcebergColumnValueReader | 固定 Schema 的读取器只有自身引用。删除旧类和 SpotBugs 例外，IcebergIndexAccessors 按 Catalog 和 FileIO 提供 IcebergCatalogColumnValueReader。 |
| InMemoryCoordinatorRegistry | 单进程显式构造可保存成员、角色、失效水位。持久化角色装配选择 PostgreSQL 或 quorum registry。保留库实现和原豁免，不能用它恢复另一进程的成员。 |
| CoordinatorQueryLifecycle | 嵌入式 QueryLifecycle 通过 QueryManager 发起查询并处理未知 ID。Coordinator HTTP 入口拥有独立的鉴权和授权流程。保留受信调用者组件及其豁免，未把裸 ID 取消作为外部入口。 |
| LocalFileSystemAccessor | 仅显式本地路径调用者和索引测试会构造；更新虚假的自动发现说明。生产 Catalog 选择具体 Connector accessor，不在缺 FileIO 时绕回本地。暂留豁免。 |
| ArrayBlock、DictionaryBlock、DoubleBlock、IntBlock、LazyBlock、LongBlock、RunLengthEncodedBlock、SliceBlock（8 项） | Batch API 自己提供的列值表示。门禁按 Block 契约和归属包识别，外部 Block 实现及另一个工厂接口仍参加装配检查。BlockRegionBoundsTest 逐种检验范围和溢出。 |
| StandardTypeRegistry | 独立类型解析和兼容性计算。TypeSystemPropertyTest 证明已支持基础类型与 DECIMAL 往返。留在原豁免，TIMESTAMP 精度等扩展须按各自发布状态判定。 |
| LakeTableWriter、MergeTreeTableWriter（2 项，已退役） | 两套 writer 以及 TableWriter、WriteRecord、WriteContext、DataFileSink、WriteMode、TableFormat 只有测试构造点，无生产工厂或消费入口。真实 CDC 使用 CdcWriteSink 与 ConnectorPageSinkCdcWriter，SQL 写入使用 Worker NativeWriteExecutor 与 ConnectorPageSink。代码、模块、服务清单和文档同步退役，SPI 历史豁免降为 5 项。 |
| CacheEvictionBroadcaster | 旧队列没有心跳消费者，一次 drain 也不能向所有 Worker 交付。迁入测试源码。Iceberg 可显式提供 best-effort 文件提示 listener；正式服务仍无集群文件驱逐保证。 |
| SettableDynamicFilter | 迁测试。生产 split 枚举传 DynamicFilter.ALL。无 build producer 的测试 collector 与完成处理器一同迁测试；删除按两个叶子的次序猜测 build/probe 的逻辑。分布式自动剪枝状态见 dynamic-filtering.md。 |
| LineageQueryEventListener | 显式 QueryEventDispatcher 调用可向借用 emitter 发送 START 和终态。门禁真实发布并检查消息。Coordinator 保持现有 durable lineage 唯一写入者。 |
| JdbcQueryEventListener | 显式 observer 经 dispatcher 后，重新打开 JDBC listener 读取同一终态。门禁验证可调用库行为。角色未启用 split 事件采集；真正资源审计不依赖此 best-effort listener。移除其测试私建 schema。 |
| MdcTaskWrapping | 接入 Coordinator 实际有界异步 dispatch executor。请求日志上下文按 submit 捕获，每次运行后恢复。QueryAsyncSubmission 继续绑定执行 span、query ID 和超时取消。 |
| InMemoryCheckpointStore、InMemoryQueryCheckpoint（2 项） | 两个 prototype 和仅测试调用的 CheckpointManager 移入测试源码，消除生产父对象的隐式 Memory 选择。QueryStateJournal、执行 fence 与 recovery 状态继续由已有持久化路径负责。 |
| UnifiedMemoryLedger | 实验性内存决策代码尚无物理回收接收方；留在原豁免。WorkerResourceBootstrap 创建 MemoryArbitrator 和 NodeMemoryPool，Native 预算来自任务授权。引入第二记账者不能证明释放。 |
| NoOpSchedulerControlLoop | 空动作实现移入 SchedulerControlLoopTest 的测试包。没有集群管理动作被实际启用。 |
| DefaultIndexAvailabilityPublisher | 未实现的 Worker /v1/index/load、调用方和服务注册均无依据。退役广播器、IndexAvailabilityPublisher API 及对应 SpotBugs 项。真实 generation 提交仍要求 publication claim 和选定 generation。 |

**URI COPY #19**。CopyIntoParserTest 覆盖 URI 源在解析阶段的明确拒绝，SELECT 来源遵循其独立入口。JdbcCopyFileClaimStore 能识别不可变源、同 execution 重放、其他执行阻塞及 profile/摘要冲突。原子 source-read admission 和带凭据的 Worker staging 单测能验证签名、租户、字节和回滚。正式 SQL 尚不能冻结 URI 清单并把 claim、目标权限复查与恢复关联到同一写入发布。保留 issue 标记，reload generation 继续拒绝。现有 claim 和内部端点不构成用户可用的 URI 加载。

**本批验证**。隔离源码 `5821360ba` 的 22 个测试类共 201 项通过，含 PostgreSQL、角色装配和两道 SPI 门禁。后续隔离源码 `ad3023b53` 的非 Native E2E 选集通过 151 项，最终字面量 Join 和 HEP 规则选集通过 33 项，覆盖 nullable BIGINT、列重命名、等式反序和基数估算与实际 build 方向相反的输入。该批剩余 7 个 SPI 豁免曾逐项列于上表；其中 LakeTableWriter 与 MergeTreeTableWriter 已在本轮退役，现余 5 项。

**Join 过滤的实际支持范围**。启用 Velox 生产 Connector 的 `canAddDynamicFilter`，从实际 hash build 推送到带列别名的 probe scan。两个 Parquet 源上，INNER 将 scan 输出从 4 行减到 2 行，Join 输出保留重复键；LEFT 的 scan 返回全部 4 行。发布后的远程 build 由独立测试经 Direct commit、注销 producer、seal、loopback Flight、Arrow exchange 和 Native hash Join 传入，同样验证 scan 和 Join 两个计数。两个独立 Worker 的具体查询、跨 stage 控制面向上游 probe 发布键域、文件 IO 字节减少均有独立验证要求。官方 Native 测试运行器的 44 项定向回归通过，另行执行的 12 项 NativeExchangeDataPlaneE2ETest 全部通过。macOS arm64 的 ASan/UBSan 运行同任务与远程 build、注入和交换测试 18 项，通过且诊断扫描无发现。真实 Iceberg SQL / Coordinator / Worker / JNI 测试共 6 项通过，其中 nullable BIGINT 和 VALUES 涉及 INNER、LEFT、重复 build 及重复 probe。首次实跑在 Calcite 的 transitive predicate 推导中叠加 Filter 并超时；限制字面量输入的推导并沿投影解析真实 key 后重跑。断言按独立读取的 probe 多重集验证，生成器按 50 个键循环生成 100 行。验证使用先前 Maven reactor 的测试 classpath 及本次重建的 planner、测试类和 Native 动态库。详细边界见 [Dynamic filters](dynamic-filtering.md)。
