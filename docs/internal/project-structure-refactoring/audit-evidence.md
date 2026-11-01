# 项目结构审查证据

审查日期：2026-11-01。来源为工作区，不是 HEAD，也不是历史实施日志。前文保留初审快照；最新状态见末节“47. File契约、核心与JDBC拆分”，不同批次检查结果不混用。

## 范围和方法

- 从根 POM 的默认 modules 递归读取：60 reactor 项目；48 有 Java 源码叶子，其余 12 为聚合/打包项目。profile-only、generated/shaded 和外部插件制品在最终交付验收另扫。
- 按 `src/main/java` 的 package 声明盘点：3,540 文件、649 包。数量用于覆盖检查，不用作模块质量评分。
- 包边使用现有 Testing Kit 的 JDK AST scanner，涵盖 import、static/nested 和显式限定类型引用；不把注释/字符串当依赖。反射/配置/ServiceLoader/JNI 需要独立核验。
- 阅读当前 architecture/development/code-standards/build-pipeline 规则，核对 POM、manifest、重点实现、调用者、现有测试和资源。
- 全包台账依据结构事实和各领域职责提出去向；重点源码审读不等价于逐个方法的并发/业务正确性证明。

## 实际执行的检查

使用本机 JDK 17，执行：

```bash
mvn -B -pl pangrova-testing/pangrova-testing-kit \
  -Dtest=ModuleDependencyPropertyTest,PackageStructurePropertyTest,PackageDependencyAcyclicityPropertyTest test
```

| 测试 | 数量 | 通过 | 失败 | 跳过 |
|---|---:|---:|---:|---:|
| ModuleDependencyPropertyTest | 7 | 6 | 1 | 0 |
| PackageStructurePropertyTest | 2 | 2 | 0 | 0 |
| PackageDependencyAcyclicityPropertyTest | 1 | 0 | 1 | 0 |
| 总计 | 10 | 8 | 2 | 0 |

命令退出码 1；耗时 9.097 秒。原始日志在本次环境 `/tmp/pangrova-structure-plan-audit.log`；临时日志不作为持久审计系统。下文保留可 review 的失败事实与源码见证。

1. `exportedRootsHavePackageDocumentation` 在 Artifact 的 `org.pangrova.databuild.spi.jdbc` 缺少 package-info 处失败。测试停止于首个断言；补充清单检查还发现 `org.pangrova.databuild.store.jdbc.query` 同样缺少说明。
2. `noPackageDependencyCycles` 检出以下 8 个 SCC；其全部边和源码位置见 [AST 输出](package-cycles.txt)。

| SCC | 包数 | 主要范围 | 处理批次 |
|---|---:|---|---|
| C1 | 29 | Coordinator 领域执行 | P5/P3 |
| C2 | 2 | Coordinator bootstrap | P5 |
| C3 | 5 | Coordinator HTTP | P5 |
| C4 | 18 | Planner | P4 |
| C5 | 3 | Worker bootstrap | P6 |
| C6 | 3 | Worker exchange/execution | P6 |
| C7 | 2 | Native bridge write/catalog | P6 |
| C8 | 3 | Worker task | P6 |

## 声明根的包说明补充核查

以下是对 manifest packageRoots 与 exports 联合集的文件存在性检查。测试专属根可能只有 test/java 来源，不能全部当作同一种生产 gate 失败；实施时分别补主源说明或测试根说明，并校对政策与 gate 的覆盖范围。

| 制品 | 声明根 | 同时为 exports |
|---|---|---|
| pangrova-data-build | `org.pangrova.databuild.spi.jdbc` | 是 |
| pangrova-data-build | `org.pangrova.databuild.store.jdbc.query` | 是 |
| pangrova-shuffle-service | `org.pangrova.shuffle.checksum` | 否 |
| pangrova-shuffle-service | `org.pangrova.shuffle.security` | 否 |
| pangrova-shuffle-service | `org.pangrova.shuffle.transport` | 否 |
| pangrova-testing-integration-tests | `org.pangrova.testing.integration.chaos` | 否 |
| pangrova-testing-integration-tests | `org.pangrova.testing.integration.e2e` | 否 |
| pangrova-testing-integration-tests | `org.pangrova.testing.integration.smoke` | 否 |
| pangrova-testing-benchmark | `org.pangrova.testing.benchmark` | 否 |
| pangrova-testing-fuzzer | `org.pangrova.testing.fuzzer` | 否 |

## 资源与目录事实

- Metadata `src/main/resources/db/migration`：93 个 SQL 文件；当前 runner 使用 `classpath:db/migration`。
- 现有主源 `META-INF/services`：13 个描述文件；这是描述文件数量，不是 SPI 或 provider 数量。
- Native src 七个分组文件数：bridge 38、engine 268、capabilities 52、indexing 19、vector 22、udf 4、lance 9。
- Table 与 Materialization JDBC 本叶子没有 test/java 文件；存在外部叶子测试消费，不据此断言无测试。
- 两个 manifest baselineViolations 分别为 Planner→Protocol、Indexing Core→Protocol，属于现有 M4 债务；本次没有删除或扩大 allow-list。

## 全 reactor 目录统计

| 制品 | 目录 | Java 主源文件 | 主源包 | 测试源文件 |
|---|---|---:|---:|---:|
| pangrova-bom | `.` | 0 | 0 | 0 |
| pangrova-ai | `pangrova-ai` | 0 | 0 | 0 |
| pangrova-ai-api | `pangrova-ai/pangrova-ai-api` | 33 | 6 | 10 |
| pangrova-ai-control | `pangrova-ai/pangrova-ai-control` | 37 | 8 | 7 |
| pangrova-ai-worker | `pangrova-ai/pangrova-ai-worker` | 93 | 17 | 52 |
| pangrova-data-build | `pangrova-data-build` | 44 | 12 | 8 |
| pangrova-client | `pangrova-client` | 0 | 0 | 0 |
| pangrova-client-cli | `pangrova-client/pangrova-client-cli` | 8 | 1 | 7 |
| pangrova-client-core | `pangrova-client/pangrova-client-core` | 19 | 5 | 5 |
| pangrova-client-jdbc | `pangrova-client/pangrova-client-jdbc` | 17 | 2 | 24 |
| pangrova-common | `pangrova-common` | 75 | 21 | 37 |
| pangrova-config | `pangrova-config` | 15 | 1 | 12 |
| pangrova-connector | `pangrova-connector` | 0 | 0 | 0 |
| pangrova-connector-iceberg | `pangrova-connector/pangrova-connector-iceberg` | 97 | 9 | 69 |
| pangrova-connector-lance | `pangrova-connector/pangrova-connector-lance` | 16 | 1 | 21 |
| pangrova-connector-paimon | `pangrova-connector/pangrova-connector-paimon` | 86 | 10 | 50 |
| pangrova-connector-runtime | `pangrova-connector/pangrova-connector-runtime` | 7 | 4 | 6 |
| pangrova-connector-spi | `pangrova-connector/pangrova-connector-spi` | 221 | 23 | 38 |
| pangrova-coordination | `pangrova-coordination` | 0 | 0 | 0 |
| pangrova-coordination-core | `pangrova-coordination/pangrova-coordination-core` | 596 | 99 | 587 |
| pangrova-coordination-zookeeper | `pangrova-coordination/pangrova-coordination-zookeeper` | 4 | 1 | 2 |
| pangrova-file | `pangrova-file` | 39 | 17 | 3 |
| pangrova-function | `pangrova-function` | 17 | 6 | 2 |
| pangrova-governance | `pangrova-governance` | 62 | 16 | 21 |
| pangrova-indexing | `pangrova-indexing` | 0 | 0 | 0 |
| pangrova-indexing-api | `pangrova-indexing/pangrova-indexing-api` | 42 | 12 | 8 |
| pangrova-indexing-core | `pangrova-indexing/pangrova-indexing-core` | 103 | 36 | 45 |
| pangrova-materialization | `pangrova-materialization` | 0 | 0 | 0 |
| pangrova-materialization-core | `pangrova-materialization/pangrova-materialization-core` | 114 | 6 | 25 |
| pangrova-materialization-store-jdbc | `pangrova-materialization/pangrova-materialization-store-jdbc` | 10 | 1 | 0 |
| pangrova-metadata | `pangrova-metadata` | 150 | 35 | 103 |
| pangrova-native-bundle | `pangrova-native-bundle` | 0 | 0 | 0 |
| pangrova-observability | `pangrova-observability` | 0 | 0 | 0 |
| pangrova-observability-api | `pangrova-observability/pangrova-observability-api` | 37 | 4 | 2 |
| pangrova-observability-sdk | `pangrova-observability/pangrova-observability-sdk` | 47 | 16 | 26 |
| pangrova-operations | `pangrova-operations` | 44 | 8 | 11 |
| pangrova-parser | `pangrova-parser` | 201 | 8 | 50 |
| pangrova-planner | `pangrova-planner` | 281 | 35 | 179 |
| pangrova-protocol | `pangrova-protocol` | 165 | 38 | 74 |
| pangrova-query | `pangrova-query` | 24 | 14 | 13 |
| pangrova-query-api | `pangrova-query-api` | 17 | 6 | 2 |
| pangrova-scheduler | `pangrova-scheduler` | 55 | 14 | 16 |
| pangrova-security | `pangrova-security` | 133 | 32 | 48 |
| pangrova-semantic | `pangrova-semantic` | 11 | 4 | 3 |
| pangrova-shuffle | `pangrova-shuffle` | 0 | 0 | 0 |
| pangrova-shuffle-core | `pangrova-shuffle/pangrova-shuffle-core` | 61 | 8 | 13 |
| pangrova-shuffle-service | `pangrova-shuffle/pangrova-shuffle-service` | 34 | 9 | 11 |
| pangrova-standalone | `pangrova-standalone` | 2 | 1 | 0 |
| pangrova-storage | `pangrova-storage` | 6 | 1 | 3 |
| pangrova-streaming | `pangrova-streaming` | 120 | 25 | 50 |
| pangrova-table | `pangrova-table` | 37 | 11 | 0 |
| pangrova-testing | `pangrova-testing` | 0 | 0 | 0 |
| pangrova-testing-benchmark | `pangrova-testing/pangrova-testing-benchmark` | 7 | 2 | 0 |
| pangrova-testing-fuzzer | `pangrova-testing/pangrova-testing-fuzzer` | 1 | 1 | 0 |
| pangrova-testing-integration-tests | `pangrova-testing/pangrova-testing-integration-tests` | 22 | 7 | 395 |
| pangrova-testing-kit | `pangrova-testing/pangrova-testing-kit` | 25 | 7 | 26 |
| pangrova-transport | `pangrova-transport` | 27 | 5 | 28 |
| pangrova-worker | `pangrova-worker` | 0 | 0 | 0 |
| pangrova-worker-core | `pangrova-worker/pangrova-worker-core` | 242 | 37 | 239 |
| pangrova-worker-native-bridge | `pangrova-worker/pangrova-worker-native-bridge` | 36 | 7 | 25 |

## 快照与结果边界

Java 主源路径及内容按制品路径、package、文件路径排序，以 UTF-8 路径、NUL、原始内容连接计算 SHA-256：

`23a0b4179ce574b8153bab2093e0bae27e7bafd69672ba80e802cfb8aa7188c1`

摘要只标识本次 Java 主源样本，不覆盖 POM/资源/测试/native，不作为完整构建 provenance。

本次没有修改业务源码，也没有执行全 reactor 编译、数据库升级、native build/ASan 或完整 runtime E2E。计划中的这些命令属于实施验收，不作为本次已通过结果。

## 计划交付复核

2026-10-31 07:34（Asia/Shanghai），JDK 17.0.20 / Maven 3.9.16，重新执行上文同一条三类结构测试命令。日志 `/tmp/pangrova-structure-plan-recheck.log`，耗时 9.351 秒，退出码 1：

| 测试 | 数量 | 通过 | 失败 | 错误 | 跳过 |
|---|---:|---:|---:|---:|---:|
| ModuleDependencyPropertyTest | 7 | 7 | 0 | 0 | 0 |
| PackageStructurePropertyTest | 2 | 2 | 0 | 0 | 0 |
| PackageDependencyAcyclicityPropertyTest | 1 | 0 | 1 | 0 | 0 |
| 合计 | 10 | 9 | 1 | 0 | 0 |

- 两个 Artifact 导出包说明已存在，初审的该项失败已经消除；剩余失败仍为 8 组 SCC。新生成的 `target/package-dependency-cycles.txt` 与本目录保存的源码见证逐字节相同。
- `python3 .pangrova/scripts/generate-module-baseline.py --check` 退出码 0，输出 `module baseline is current`。未修改模块允许边或豁免。
- 再次递归读取默认 POM reactor 和各 `src/main/java`：60 项目、48 Java 叶子、3,542 文件、649 包。以 `(artifactId, package)` 对逐包表做集合及重复检查：649 行，遗漏 0、多余 0、重复 0。只有两个新增包说明使初审文件计数各增加 1。
- 直接复读核对了 PlannedQuery 的公开 RelNode、QueryMaterializationWiring 的 IndexingBootstrap 条件、Common 的 provided Jackson/Jetty、ZooKeeper 对 coordination-core 的依赖，以及 Artifact fence、CurrentDeferredExecutionContext、ManagedSource fence 消费者和 Worker task/state/memory 的反向依赖。这些问题仍适用于当前源码。
- 计划第 7.1 节补齐分批内实施单元，特别记录 publication 候选查询对 anchor 的剩余读耦合；不将单个 fence helper 的移动视为整个 Artifact 边界完成。

本轮复核只更新计划/证据文档并运行结构检查，未新增业务源码变更，未运行 native、数据库或全运行时回归。原始快照数字留作历史依据；后续执行应以最新复核和实际源码为起点。


## 第 30 节实施后的结构增量

Query 事务 fence 与 Artifact execution route/fence 迁移后，源码为 3,548 文件、651 包。
逐包台账已更新为当前计数；本文前述表格与摘要仍为初审历史快照。新 AST 检查仍为
9 项通过、1 项因八组 SCC 失败；循环见证的包/边/源码行完全未变，仅扫描总包数由
649 增至 651。本目录 `package-cycles.txt` 同步更新这一总数。实施、制品及运行时验证见
[执行记录第 30 节](../project-restructure-execution-plan.md#30-query-事务-fence-与-artifact-执行路由迁移)。

## 第 31 节增量：Artifact 通用 JDBC runtime

2026-10-31 当前源码为 3,555 个 Java 主源文件、653 个主源包、48 个 Java 叶子、60 个 reactor 项目。
通用 build/stage/exchange/context 从 Metadata 迁入 Artifact；新增 Security 当前角色端口及真实
JDBC provider、Artifact 发布候选读端口和 Metadata 共享库投影、Artifact-owned 存储异常。
生成 baseline 后，10 项结构门禁仍为 9 通过/1 失败：模块 7 项、包结构 2 项通过；全图无环项
仍检出原八组 SCC。当前 AST 环报告相较第 30 节只有包总数变化，无新增环或豁免。

逐包台账 653 行与当前主源 package 集合和每包文件数一致，无遗漏/重复/多余包；四份主要计划和
证据文档中 670 个本地链接目标存在。全仓最终清理编译 60 项通过（`reactor-final.log`）；
最近 Metadata 41 项真实 PostgreSQL 回归、Security 394 项、Artifact 36 项以及真实 SQL/native
13 项通过。完整消费者与静态证据统一登记执行记录第 31 节：Coordinator 3,125 项默认测试及 19 项显式集成、AI Control 67、Lance 47、Indexing 227，另 12 项锁并发重复通过。模块静态检查通过；包环 gate 的原 1 项失败仍未消除。
本批日志前缀 `/tmp/pangrova-artifact-runtime-`，失败过程和修正结果分别保留。

## 第 32 节增量：迁移资源制品与 owner 测试

当前默认 reactor 为 61 项：48 个 Java 代码叶子、1 个纯资源 JAR、12 个聚合项目。
Java 主源为 3,556 文件、653 包；逐包台账按 `(module, package)` 和文件数核对，653 行完全匹配，
遗漏、多余、重复均为零。Java 主源净增一个 Testing Kit 的 MigratedPostgres fixture；SQL 是原文件迁移。
93 份 SQL 与 Git 暂存区中 Metadata 原路径的 blob 逐一计算 SHA-256，93 份全部相同。

本批 Java 资源/结构定向检查 15 项通过，Python manifest/tooling 8 项通过。Kit 完整 owner
执行 112 项，其中 111 通过，1 项因原八组包环失败；环报告与前批逐字节一致。没有为资源制品
放宽 Java owner 边界或为 SCC 添加例外。resourceOnly 的布局正反例和模块扫描约束独立覆盖
资源根缺失、越界、Java main/test source、bytecode 与 ServiceLoader descriptor。

全 reactor 清理构建 61 项通过（跳过测试，不记作全仓回归）。Table full-regression 101 项、
Materialization JDBC 15 项、真实 SQL/native 13 项均通过且零跳过；Table、Materialization JDBC
及 Kit 的 Checkstyle/SpotBugs 通过。Metadata full-regression 最终 579 项通过，零失败、零跳过，
Checkstyle/SpotBugs 通过；详见执行记录第 32.3 节。本批日志前缀 `/tmp/pangrova-schema-`；初次 fixture 数据及旧本地 BOM 引发的失败
与修正后的结果分别保留。

Standalone 的 runtime-only classpath 共 262 个依赖，不含 Testing Kit/JUnit。逐 JAR 核对：
93 份 SQL 均唯一来自 schema-migrations、hash 一致；Metadata 无旧资源副本，资源 JAR 无 class。
独立资源测试覆盖空库全部迁移、重复运行 no-op、V60 已有库升级与历史 checksum/catalog 行保留。
新旧库检查使用真实 PostgreSQL，部署资源归属不借助测试依赖。Native 输入未改，复用前批匹配库，
本批未重新执行 C++/ASan 构建。整体结构与行为验收仍包含 B6、八组 SCC、P1/P4–P9 及 R5/R6。
P2.1/P2.2 本批已验收；四份主要计划/证据文档当前 669 个本地链接目标存在，架构文档 235 个
类引用通过。完整目标保持进行中，不将资源制品落位当成全领域语义审查完成。

## 第 33 节增量：AI Control 装配与依赖隔离

AI Control 的 factory 归 assembly、跨域 authority 必须由真实组合根传入；catalog-export
存储异常归 AI 并保留原客户端错误码。Metadata 的生产和 test-jar 依赖均从 AI Control 删除；
对应 7 项真实跨域测试迁入 Integration，owner tests 使用统一 Kit fixture。重复 AI PostgreSQL
fixture 及失去直接消费者的两个 Testcontainers test 依赖删除，未将 Metadata 依赖转移到底层 Kit。

当前 61 项目、48 Java 叶子、1 个资源 JAR、3,558 主源文件、654 包。逐包表集合/计数完全匹配，
无遗漏/重复/多余。全图仍为原八 SCC；报告只有总包数由 653 增至 654，源码边见证完全一致。
最终结构检查为 9 通过/1 因 SCC 失败，未修改豁免或弱化边界。

AI Control 62 项、真实跨域 8 项、Coordinator 启动/Feature SQL 24 项、SQL/native 13 项通过，
零失败/跳过；AI Control、Integration 与 Coordinator 静态检查通过。61 项修正后清理编译通过。
AI JAR 新 factory/异常存在，旧 factory 不存在；独立 owner 的 classpath 不含 Metadata。
670 个计划本地链接和架构文档 234 个类引用有效，manifest current、stub pattern gate 通过。

初次缺少 AI API test 直接依赖、跨行旧 factory 调用及 E2E 镜像参数错误均分别修复并保留失败
日志；详细验证在执行记录第 33 节，日志前缀 `/tmp/pangrova-ai-boundary-`。Native 未改，复用
前批匹配库，不声称新做 native/ASan 构建。模型当前权限 provider/消费者、领域 publication、
八组 SCC 和 P1/P4–P9/R5/R6 仍是未完成事项，不能由本批编译与测试通过推断全部目标完成。

## 第 34 节增量：独立协调契约与 Worker 会话解环

当前为 62 reactor、49 Java 叶子、1 资源制品、3,561 主源文件、656 包；逐包表集合和文件数
精确匹配。11 个协调契约进入独立 coordination-api，ZooKeeper 删除对 Coordinator 进程的
依赖；API 的 JDK/Common 最小 classpath 和 provider 的无 Coordinator classpath 检查通过。
Worker 的不可变 session 与资源 resolver 下沉到能力包，未改 JNI 签名、native 源码或资源生命周期。

当前 AST 结构检查 9 项通过，无环项仍失败；SCC 从 8 降为 7。按成员集合比较只移除了
nativebridge.jni.write/catalog 这一组；其余七组明确保留，完整源码见证已更新，没有门禁豁免。

验证：API 1、ZooKeeper 16、bridge 350、Worker 消费者 18、Coordinator 消费者 161、真实
catalog/write/SQL/native 99 项及相关静态检查通过，零失败、零跳过。全 reactor clean install
62 项通过（跳过测试）。JNI 22 方法对称、精确 filter、manifest、SPI annotation/stability、
stub pattern gate 通过；架构类引用 236 项和计划本地链接 672 项有效。

日志与验证范围见执行记录第 34 节，root 前缀 `/tmp/pangrova-coordination-api-`、Worker 前缀
`/tmp/pangrova-worker-session-`。Native 复用前批相同 hash 的已打包库，未新做 C++/ASan 构建。
本批不关闭剩余七 SCC、B6 模型权限与领域发布、其余逐包语义审查和最终 R5/R6。


## 第35节：模型权限迁移后的复核

当前仍为62 reactor、49 Java叶子、1资源制品、3,561主源文件和656包。Security新事务端口、
AI Control真实provider取代Metadata旧静态实现；仅转发工厂删除后主源码净增为零。
Metadata/ManagedSource/生成写入与Query/HA/NativeDatasetControl生产装配均显式传端口，
无Metadata→AI反向依赖。Lance与Coordinator新增的Metadata test-jar均为test scope。

验证实际终态：Security402、AI71、Metadata581、Lance48、Indexing14、Coordinator分批78、
Integration分批23项通过，均无最终失败/跳过；相应Checkstyle/SpotBugs通过。
62项full reactor clean install通过（跳过测试），不能作为全仓运行时回归。
结构9项通过；无环项因相同7组SCC失败，未加豁免。SPI artifact gate为99类型/0gap、
Stable22有生产实现，docs class引用236有效，精确filter边界、manifest和stub gate通过。
临时日志前缀为 /tmp/pangrova-model-authority- 与 /tmp/pangrova-b35-；初次旧Lance
构造签名、漏test-jar传递依赖及新增query-only fixture初始化失败均保留，不伪报首次全绿。

新provider测试证明非空模型变化/撤权和调用方事务保留；Metadata adapter新回归证明两组
snapshot与同连接转发。Lance新真实SDK/PG拒绝测试使用空模型recording port，只证明消费者
同连接和原子回滚；真实非空模型receipt贯穿ManagedSource撤权发布仍待单独fixture与验证。
query-only两项用真实PG权限与Iceberg声明验证普通receipt/撤权、生成列无owner拒绝。
13项SQL/native沿用SHA匹配库，无native输入修改，不声称重新做ASan构建。

逐文件常量/可读性/职责要求已加入总计划，不将包盘点当文件语义完成。用户要求的
原 pangrova-artifact 及内部包/类型已在后续批次迁成 pangrova-data-build，但用户否决了该名称。
当前名称是过渡状态，须按领域、任务内核及存储 SPI 的最终边界重新定名；上述数字仍是第35节
快照，不能当作后续迁移已完整验收。

## 36–37. 中间命名与独立协调后端快照

当前任务模块名data-build已被用户否决，只记录中间迁移事实。中间迁移的232类型映射、
178 Java文件迁移与250文件修改已比对；SQL/proto/native/golden保护清单1,045输入SHA不变。
阶段36的内核36、Protocol602、Metadata57、Indexing27测试与static通过，Planner没有完整owner
复验；不可据此称全部消费者已验收。

JDBC/InMemory协调实现已迁至两个独立叶子；新owner分别61/41项与static通过，实际classpath
不含Core/Metadata。Core108相关消费者通过；改名造成的两行长度问题已仅换行修复，单独static/
install通过。Integration首次因漏AI镜像参数导致3个类setup失败；模型/query-only3项通过。
补已存在的不可变镜像ID后，受影响三个E2E类19项和static通过，覆盖PG failover及真实SQL/native。
64项目clean install通过（跳过测试）；模块/包/状态枚举12项通过，无环1项仍因7个SCC失败。
成员已与更名后的预期集合逐组比对，未新增环或豁免。

当前51 Java叶子、1资源制品、12聚合；3,564主源、656主源包、2,373测试源。
656台账行及51叶子计数逐项匹配，657个台账路径有效。证据日志为
`/tmp/pangrova-coordination-provider-*`、`/tmp/pangrova-coordination-jdbc-*`、
`/tmp/pangrova-coordination-inmemory-*`。Native沿用匹配库，本批未改其输入或执行ASan。
用户新增独立存储SPI/PostgreSQL实现及业务归位要求，逐文件设计正在落位；这些目标及R5/R6
未因编译通过而关闭。用户已授权提交推送阶段检查点。

## 38. 阶段/交换存储 SPI 证据

新增4个主源后当前3,568主源/657包/2,373测试源，64reactor/51Java叶子不变。
5个stage操作、3个exchange操作及8个record迁入SPI，3getter current-authority view只作provider
输出；private JDBC context及fresh resolve门禁不变。33个provider方法/构造体核对一致。
4MiB wire上限为共享私有常量；旧实现嵌套DTO在clean后的JAR中为零残留。

最近隔离/golden11项、owner36项、Metadata46项、Core46项、真实SQL/native13项通过；相应
static通过。首次owner static暴露8个接口内record冗余public，删除无效修饰符后单独static/install
通过，未放宽规则。64项目clean install通过（跳过测试）；模块/包/状态枚举12项通过，SCC项
仍失败且与前批7组成员一致。3个canonical golden SHA不变，Native/proto输入未改，未重做ASan。
台账657行、51叶子计数逐项对齐，659链接有效。日志为`/tmp/pangrova-storage-spi-*`。

这证明接口分离与原链行为，不能声称当前JAR已物理拆分。本批曾选family候选
`pangrova-derived-data-jobs`（第39节按单词要求改为generation）；core/transaction-spi/store-postgresql及Catalog/领域迁移、全文件
语义审查、7组SCC和最终R5/R6仍待完成。前一完整目录检查点`9d38fbd4`已推送远端。

## 39. 同连接提交与执行 fence 复核

新增2个Internal契约，当前3,570主源/657包/2,373测试源，64reactor/51Java叶子不变。
真实任务store直接实现caller-owned提交SPI，AI/Metadata不再调用其静态实现入口；三种execution
fence检查通过接口注入，真实方法体及Query行锁后独立时钟查询不变。全请求authority预检仍在
生成任务写入之前，各任务结构admission保持原位置；没有新增无权限默认值或隐藏provider。

任务36、AI71、Metadata50、Indexing12、Core24、Integration23项通过及相应static通过。
任务static的一行长度错误已只换行修复。全仓clean编译前60项成功，Integration暴露先前审查漏掉
第8处跨行AI工厂调用；修正后resume其余4项成功，不能报告首次clean单遍全绿。模块7/包2/状态枚举3项
通过，全图无环仍失败，7组SCC成员集合不变。日志为 `/tmp/pangrova-transaction-ports-*`。

阶段38检查点 `49d0f538` 已推送。用户要求单词名称后目标改为 `pangrova-generation`，
现有Dynamic Table/MV的Materialization family保持独立。当前data-build只是过渡路径，后续
读取SPI/物理拆分、Catalog与各领域归位及总验收继续推进。Native输入未改，未重做ASan。

## 40. 输入读取端口与新增审计证据

S3新增同连接输入SPI，真实reader六个方法体与SQL不变；Metadata业务适配器删除静态具体reader
依赖，装配显式提供实例。普通auto-commit、tenant范围、损坏canonical输入拒绝、原事务内未提交
撤权与rollback恢复由两个新增PG测试验证；编译器历史观察不被视为当前授权。

任务36、Metadata52、Indexing12、Core24、Integration23项通过；任务3处空行及Metadata两个
构造器Javadoc错误修正后，相应static/install通过。64项reactor clean install一次通过（跳过测试）。
模块/包/状态枚举12项通过，SCC项因原7组仍失败。台账657行/51Java叶子与当前3,571主源、
2,373测试源一致。日志为`/tmp/pangrova-input-ports-*`，Native未改或重做ASan。

Catalog新增只读审计确认真实同步历史/续读游标混淆，B0已列出源确认边界、scope、CAS与真实PG
修复回归设计；未将设计冒充实现。输出校验SQL仍在Metadata，Index digest仍是领域算法；
物理三制品和后续发布拆分未因S3通过而提前关闭。上一检查点`c2402293`已推送。

## 41. Generation 三制品物理隔离证据

`pangrova-generation` 单词名称及 core/transaction-spi/store-postgresql 已实际落地，旧data-build
制品退役；Materialization继续拥有独立Dynamic Table/MV领域。三叶主/测试源分别48/8、6/1、22/3。
真实core classpath排除两个兄弟制品；JDBC端口测试归transaction-spi，PG取消/锁/回滚测试归真实
provider，未保留空权限默认值或旧类型转发壳。Metadata领域writer/装配ABI仍待迁，不能混称完成。

迁移231类型/27包、241文件更新/180路径移动；独立审计239纯改名文件代码token一致，其他测试
变化按后置patch核对。1,045个SQL/proto/native/golden保护文件字节一致，240次main显式v2引用
不变。旧owner先clean，全67项目随后clean install一次通过（跳过测试）；57个JAR无映射旧类。

core33、transaction-spi1、PG6、Protocol602、AI71、Metadata580、Planner选定19、Indexing27、
Coordinator48、Integration23项均通过，相关static通过。PG及Coordinator各2处更名长行只换行
后单独static/install通过，没有重复无行为变化的测试或放宽规则。Planner不是完整owner回归。
Native复用匹配库，本批没有Native/JNI输入变更，没有重新执行ASan。

模块/包/状态枚举12项通过；无环项仍因原7组失败，按包映射对比成员一致。SPI gate覆盖104个
已标注公共类型/0gap、22个Stable有真实实现；精确filter正反例通过。文档初漏旧dispatcher引用
已修正，架构类引用241项通过。当前67reactor/53Java叶子/1资源制品、3571主源/657包/2374测试源；
657行台账及53叶子计数对齐，限定五文档895链接有效。详细日志前缀`/tmp/pangrova-generation-*`。
阶段40检查点`7c9c9d7d`已推送；本批不关闭Catalog B0、B6、Worker待修环和全项目最终验收。


## 42. Worker 包环和分配异常的增量证据

本节在第 41 节检查点 `243d3710` 后记录当前源码，前述历史数量和失败事实保持原样。
标准目录实扫 67 reactor / 53 Java 叶子 / 1 资源制品，3,574 主源、658 包、2,375 测试源。
Worker Core 由 242/37/239 增至 245 主源/38 包/240 测试源：codec 包说明、TaskMemoryAccount、
独立 TaskState 为三个新增主源，四项预算回归共用一个新测试源；原 codec 及测试只是移动。
AST 辅助扫描的非标准脚本/资源/native fixture Java 不计入标准 main/test 数。

`/tmp/pangrova-worker-ownership-structure.log` 的终态为 13 项、12 通过、1 失败、0 错误/跳过。
模块 7、包结构 2、状态枚举 3 项通过，包结构属性覆盖 658 包。全图无环仍失败；对照第 41 节
原始 report，恰好移除 exchange/remote/execution 和 task/memory/state 两组三包 SCC，没有
新增或改变其余组成员。当前 [原始 AST report](package-cycles.txt) 保留五组全部边与源码见证。

| 剩余 SCC | 包数 | 与第 41 节成员比较 |
|---|---:|---|
| Coordinator 领域执行 | 29 | 完全相同 |
| Coordinator bootstrap | 2 | 完全相同 |
| Coordinator HTTP | 5 | 完全相同 |
| Planner | 18 | 完全相同 |
| Worker bootstrap | 3 | 完全相同 |

纯迁移最近 58 项通过。descriptor-only 部分额度异常测试先在仓库得到 4 项断言失败，再于
修复后连同 9 项 canonical/统一计费测试共 13 项通过；三个日志分别是
`/tmp/pangrova-worker-ownership-focused.log`、`/tmp/pangrova-worker-partial-reservation-red.log`、
`/tmp/pangrova-worker-partial-reservation-green.log`。两预算与原 query credit 经真实实现校验，
回调/清理错误保持身份和 cause/suppressed；不把测试注入点误报成当前 canonical HTTP 泄漏。

候选初版的 logger 误改在独立审查后、应用前已修正；宽泛 token 归一化的旧证明不再有效。
最终调用选择器检查不替代真实编译和行为测试。Worker 完整回归已执行 1,298 项，0 失败/错误/跳过，
含真实 5 GiB HTTP 传输和 Native property。随后 Checkstyle 报 3 处注释/换行问题，修正后
SpotBugs 再发现 `TaskInfo.memoryAccount()` 暴露可变账本；核对调用者后收紧为包内可见，
不增加豁免。最终 static/install 通过，使用 skipTests，不重计运行时测试。日志为
`/tmp/pangrova-worker-ownership-full.log` 和 `/tmp/pangrova-worker-ownership-static-final.log`。
67 项 clean reactor 单次编译全部通过（skipTests，不是全仓测试）；Integration remote/Native/COPY
分别在独立 Maven JVM 执行 18 + 3 + 1 = 22 项，零失败/错误/跳过，Integration 静态检查通过。
日志分别为 `/tmp/pangrova-worker-ownership-reactor.log`、`/tmp/pangrova-worker-shuffle-egress.log`、
`/tmp/pangrova-worker-native-ingress.log`、`/tmp/pangrova-worker-copy-native.log`。clean 后的 Worker
JAR/classes 中旧 codec 和嵌套 TaskState 均不存在，三个新 owner 类型存在；未留兼容类。
B6、Catalog B0、剩余五 SCC、全文件语义审查与 R5/R6 仍未完成。


## 43. Worker 启动职责与注册回滚的增量证据

配置、资源所有权、注册通信分别归入 bootstrap.config / ownership / registration；内存租约
归 resource.memory；关闭参数只保留真实共享资源。删除五组无消费者的组件字段及无生产消费者
的关闭配置，未删除真实关闭/重试/失败回归。五个迁移主体代码 token 比较保持原行为。
启动注册成功后的回滚原本跳过注销；实际 HTTP 注册/依赖关闭回归先得到红灯 1 项，统一注销
路径后近端 15 项及后续扩展 42 项通过。完整验证以执行记录第43节终态为准。

当前 67 reactor、53 Java 叶子、1 资源制品；3,578 主源、661 包、2,375 测试源。Worker 为
249 主源、41 包、240 测试源；逐包台账集合与文件数完全匹配。结构共 13 项，12 通过，
1 因剩余四 SCC 失败。与第42节原始 report 比较，恰好移除 Worker bootstrap 三包组，其余
Coordinator 领域执行、bootstrap、HTTP 和 Planner 四组成员相同；未新增豁免。
本目录 AST report 为原始生成结果副本。Worker 完整 1,297 项、static/install 已通过；
Kit 完整 112 项仅四 SCC 断言失败，SPI 三项通过并不再忽略缺失的必需装配源码。
67 项 clean reactor 编译单次通过（skipTests）；最终 Worker/Kit static 通过。真实启动 E2E
5 项、Coordinator 消费者 35 项及 static 通过；SQL smoke 首次缺少必需镜像参数而中止，核对
实际镜像后正确传参执行 3 项及 Integration static 通过，均零失败/错误/跳过。clean 后 JAR
八个旧 owner 身份及其嵌套类型不存在，七个新 owner 存在。本批必要验证已结束，不将本批
改进记为全仓生产验收通过。Native 使用匹配的上一批 cohort，
本批没有 C++/ASan 输入变化或重新构建。存储 API 的 AWS 传递依赖与 LOCAL/OSS 装配缺口
已按当前源码记录执行计划43.3，尚未修改存储实现。B6、Catalog B0、剩余四 SCC、逐文件审查
与最终 R5/R6 继续开放。

## 44. 独立存储契约与本地发布的增量证据

旧storage JAR改为聚合器，API / filesystem / S3三叶主源及测试源分别5/1、2/1、4/3；
API的实际测试classpath与未过滤最小loader均无云SDK、JDBC、provider。BlobObjectId替换
字符串参数组并拒绝损坏/路径token；managed COPY端口与abort stream不暴露AWS类型。
旧package及旧参数没有转发兼容层，Docker显式runtime JAR清单同步三个叶子。

本地发布五项回归先在旧实现得到五项断言失败，修复私有copy/校验/atomic no-replace之后
扩展八项通过，含并发、空对象及symlink。真实授权/预算组件仍由FILE/COPY拥有；目录和祖先
管理员控制，Local共享namespace不等于复制，未证明进程崩溃/断电或外部并发改名隔离。

API3、S310、filesystem8及static/install通过。AI最终完整213及static/install通过；新LOCAL
工厂实际FILE_FROM_BYTES与返回identity/落盘字节通过，但不是LOCAL SQL资格证明。配置入口
候选回归另得到红1，补AiFileOptions注册/显式转发后Config166、Worker入口26及static通过。
Worker完整1297及static/install通过；配置新增1项由入口26独立覆盖，不冒称全1298运行过。

最终70项clean reactor全部通过（skipTests，只记编译），56个当前JAR无旧storage.blob类，
聚合器无旧runtime JAR。663行逐包台账与当前55Java叶子、3583主源、2377测试源完全对齐；
结构7模块+2包检查通过，SCC项仍因相同四组失败；除包总数外成员及源码见证逐字节一致。
SPI106公共类型零gap、Stable23有真实实现；manifest、stub gate、架构244类引用和Docker
RUN语法/11个显式JAR存在性检查通过。未增加架构豁免或SpotBugs过滤器。

真实FILE/SQL首次缺少fixture capability key，smoke3过而FILE BeforeAll错误1，不能记作FILE
方法已执行。补临时随机fixture key后，真实HTTPS/SSE S3 FILE SQL6+smoke3及Integration static
通过；独立JVM COPY私有票据/精确version/managed重启读取1过，全部零失败/错误/跳过。
Native/protocol输入及已打包hash不变，本批未重做C++/ASan/Linux生产镜像。日志前缀
`/tmp/pangrova-storage-*`，具体过程见执行记录第44节。第43节检查点20a75cdf已提交；
OSS provider、LOCAL SQL资格、多后端source authority、B6、Catalog B0、四SCC和R5/R6继续开放。

## 45. LOCAL FILE SQL 与部署夹具边界

FILE测试部署和S3 SQL类归入e2e.file，单一共享Native启动owner消费显式S3/LOCAL部署端口。
LOCAL新增独立JVM owner选择并接入CI第五项；原四项、镜像身份和Native provenance参数保留。
四项真实SQL/JNI测试覆盖NULL、CASE、volatile结果/内容去重、FILE列写入和后续Iceberg扫描，
另以独立filesystem provider读回发布字节。最近与clean后各4项通过，最终Integration static
通过；不当成8个不同案例，也不当成分布式文件系统或LOCAL COPY证明。

迁移夹具后的S3 FILE SQL6+smoke3与static通过；最终归包/clean后同一选择再次9项通过，零跳过。
70项clean reactor通过（skipTests）。结构模块7、包2通过；SCC项仍失败且四组原始报告逐字节
相同。工具28项、manifest、truthfulness、245架构类引用、CI shell语法通过。Native/protocol
及匹配库未变，没有新C++/ASan/Linux镜像结果。当前3583主源、663包、2380测试源，主源台账
不变。详细日志和边界见执行记录45；OSS、多后端source authority、B6、Catalog B0、四SCC及
R5/R6继续开放。

## 46. 独立OSS provider、取消与装配

新增pangrova-storage-oss，6主源/1包/3测试源，官方SDK0.6.1而非AWS endpoint替代。
只实现BlobStorage；never-versioned bucket、私有有界上传snapshot、长度/SHA-256、传输
MD5/CRC64、SSE/KMS及真实winner读取校验归此制品。API没有云SDK依赖；COPY与FileSpace
的OSS权限/版本/adapter仍开放。YAML、Worker转发、FILE工厂、BOM和12个Docker显式runtime
JAR清单同步；配置转发先红1，修复后完整Worker包含新入口。

官方默认Apache5 abort的graceful drain在HTTPS夹具2秒超时复现；独立transport先取消socket、
解绑entity再close，同一1字节/64KiB未读body回归通过。最终OSS19项及static/install、API3项及static通过；包含新增CRC与控制响应超时。实际runtime15个未过滤URL加载7类型，无AWS/JDBC/
其他provider/Kit；初次错误Maven scope参数带入test classpath的审计失败单独保留。

AI214+static通过，增加真实子进程工厂注册/关闭probe后完整215通过；Config166+static通过。
Worker完整1299+static、S3 FILE SQL6+smoke3及Integration static、独立LOCAL4、独立COPY1
通过，零failure/error/skip。Worker console合计1299；JUnit/jqwik同类XML覆盖后剩1288，
不把报告覆盖或property tries换算成新测试。71项clean reactor通过（skipTests），其后新增
probe仅由AI最终完整测试补编译，不冒称又做了一次clean。

Kit完整112有111过，唯一SCC断言仍失败；四组成员和源码见证不变，仅包总数664变化。
SPI106/Stable23、manifest、stub、truthfulness、245架构类引用、Docker RUN语法/JAR清单通过。
当前71 reactor、56Java叶子、1资源制品、14聚合；3589主源、664包、2384测试源。Native/
protocol和匹配hash未变，没有新C++/ASan/Linux生产镜像证据。详细边界、失败和日志见执行
记录46；真实OSS云端SQL/权限/KMS/多Worker、FileSpace/COPY、多后端authority、B6、Catalog
B0、四SCC与R5/R6继续开放。

## 47. File契约、核心与JDBC拆分

本批把混合File JAR改为纯聚合与API/Core/JDBC三个真实叶子，分别28/8/7主源、9/5/6包、
4/0/4测试源。ObjectLocation及multipart/URL/accessor归api.object，错误归api.storage，
依赖Security的authority归Core.spi。首次编译暴露Core→JdbcFileDirectoryIndex依赖，现以
FileDirectoryIndex端口注入；SQL与事务规则不改。旧包与nested token没有转发别名，4组真实
PostgreSQL测试迁归JDBC38项；Metadata保留真实tenant provider的authority整合测试。

对象访问工厂原字符串key可使不同LOCAL根碰撞，忽略S3 secret/token轮换，并把显式残缺或
role-arn凭据转成默认身份。回归先红9，完整StorageProfile key、严格显式凭据与fatal cleanup
后绿17；真实LOCAL写入、SDK签名HEAD和presigner token均验证。只有空map用注入/默认chain。
关闭尝试全部owner并保留Error身份，未扩大调用方credential ownership。

API9与JDBC38、三个File制品static/install通过；clean后API9与static再通过。Worker完整1308
零failure/error/skip后有1个新增括号style错误，修正后的17项与Worker static通过，不把最近
范围冒称又做了一次全量。Metadata clean361+static，Coordinator相关32+static，FILE相关
HTTP/SQL/smoke35+Integration static通过。COPY默认3项仅边界验证（owner被tag排除），另精确
方法/none配置独立运行真实Native ticket/精确版本/managed重启1项通过。74项最终clean install
通过（skipTests），不是全仓测试结果。

API真实runtime为Common与传递slf4j-api两个依赖URL；隔离loader只用API/Common，遍历所有API
类型。Core53依赖URL没有File JDBC；Worker175只有File API；Coordinator249含三个File叶子。
首次classpath审计漏算slf4j-api的失败保留为预期修正，未修改依赖。92个重建JAR无旧File类型，
Metadata test-jar无迁出的4组class；Docker15个显式JAR与RUN语法通过，未构建Linux镜像。

最终结构模块7、包2通过；SCC1因原四组失败，原始报告仅总包数667变化，成员/见证不变。
manifest、SPI115零gap、Stable23、stub、truthfulness、246架构类引用通过。当前74reactor、
58Java叶子、1资源JAR、15聚合，3594主源/667包/2386测试源，台账集合和摘要精确一致。
Native/protocol及合格库不变；没有新C++/ASan/Linux资格。Worker物理访问器迁移、OSS
FileSpace/COPY、真实云端OSS/权限/KMS/多Worker、B6/Catalog B0、四SCC、全文件审查及R5/R6
继续开放。详细命令范围、失败与日志见执行记录47。
