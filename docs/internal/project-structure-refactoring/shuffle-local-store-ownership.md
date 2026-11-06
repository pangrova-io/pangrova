# Local spool 后端隔离与数据完整性（2026-11-03）

后续 [目录与文件资源所有权审查](spool-directory.md) 修复本批之后发现的懒读取重定向，
并明确私有根目录前提与 TOCTOU 限制。下述结果保留为本批历史证据，不替代后续验证。

本批以当时工作区 8,346 个文本文件为基线，实现真实 Local 文件后端的隔离和语义修复。
它是通用 Shuffle 接入的存储基础；目前没有把 Local 装配成 Native 的第三种生产 Shuffle 模式。

## 契约与制品所有权

新增非空 `pangrova-shuffle-local`，包含原 LocalFs 实现、工厂和唯一 ServiceLoader 注册。
生产依赖只有 Shuffle API；Common 作为隔离测试的直接依赖，不引入 Core/Protocol/Worker/Native。
Core 与 Coordination Core 仅在测试依赖 Local，不反向绑定具体存储。

五项 SpoolStore/Writer/Reader/Factory/Context 契约进入统一 API 制品的 `.spi.spool` 包。
新增 `.api.spool.SpoolReadSelection` 明确 query/stage/partition 及 producer→long attempt 的精确选择，
输入不可变、有序且经过校验。全部调用方同步修改，没有旧 FQN 转发壳。
该选择只是存储读取参数，不代表 receipt、授权、application incarnation 或 exchange generation。
这些同步文件端口也不冒充最终要求的异步 Provider Writer/Reader。

## 修复的实际行为

- 不再扫描“最新” attempt，不把 long 截断成 int；严格读取显式选中的完成文件，缺失即失败。
- 文件含版本、逐批长度、SHA-256 和完成 trailer；64 MiB 单批上限，在分配前检查长度。
  多批次边界保留；损坏、截断、错误计数、额外尾部数据都不能解释成 EOF。
- 空分区必须有已完成的空 segment；缺文件与合法空数据明确区分。
- 每个 writer 使用独立临时文件；同 attempt 活跃写入或已完成文件禁止覆盖。
  完成时 force 后 atomic move，没有非原子回退；失败后不能重试成伪成功。
  关闭失败 writer 只处理自己拥有的临时文件，不删除可能已完成的最终文件。
- Store 使用操作系统目录锁，防止同 JVM/不同 JVM 的第二 owner；注册并拥有 reader/writer，
  读取到 EOF 或 writer 完成不自动释放调用者的 lease，必须显式 close 后才允许 query cleanup。
  store close 按资源实际释放情况处理异常，未成功关闭时保留可重试状态及目录锁。
- query 删除拒绝活跃资源，按字符串值匹配 query，避免 IdentityHashMap 的 identity 比较漏掉保护。
  路径校验拒绝 traversal 和符号链接绕过；删除与关闭失败不再静默吞掉。
- Core 的 Java exchange adapter 使用显式 selection resolver，只接受当前支持的完成后 ARROW_IPC 读取。
  关闭或 abort 后不能完成成空结果；write/commit/read 错误保持失败，后续调用不能变成成功或 EOF。
  source close 与懒创建读取资源互斥，避免关闭后再获取文件资源。

存储内部同步 I/O 使用 owner monitor 串行保护；本批不声称异步背压或吞吐优化。完成文件可以在正常
关闭重开后读取，但没有电源故障目录持久化、复制、存储节点故障可读或 Coordinator 恢复保证。

## 调用闭环与剩余边界

真实 Local 工厂→Store→writer→完成文件→按选中 attempt 读取→reader close→cleanup 已验证。
真实 Java exchange adapter→Local Store 路径也有测试，明确保持其尚未接入 Native 的事实。
原 SpoolingExchangeManager 和 LocalSpillLifecycleManager 的 cleanup 现在传播失败并保留可重试状态；
原 EMPTY commit 仍不是发布证明。这两个生命周期原型和 InMemoryShuffleDataInventory 的装配缺口
没有通过豁免或假接线隐藏。后续必须统一 session/publication owner，不能把不同节点的本地目录
当作共享存储，也不能从空 inventory 推导安全缩容。

14 项生产类处置逐项记录，其中两项只移动、规范化方法体 token 相同。其余逐类说明行为变化，
不是通过包扫描宣称语义审查完成。Native、Worker、Protocol、Server、Pangrova Provider、Native Adapter、
Persistence、Schema、Client 文本输入与本批基线一致。

## 测试准确性与验证

新增/更新覆盖包括真实双线程同 attempt 竞争、独立 JVM 目录锁、长 attempt、精确选择、逐字节截断、
校验损坏、空数据、重复完成、失败重放、buffer 消费与复用、reader/writer 清理保护和工厂配置拒绝。
本批发现旧 ShuffleFaultToleranceE2ETest 的八项只是 SQL 查询，没有 Local spool、attempt 去重或背压
注入。保留全部查询与断言，改名 ShuffleQueryRegressionE2ETest，按实际 SQL 内容命名内部测试。
真实双 Worker E2E 和启动 smoke 分别执行；不把普通 SQL 查询计作 Local 集成或容错证明。

首次 Local 定向 28 项通过。首次 Core 聚合在测试编译时发现 close 的受检异常未声明，补正测试签名；
修正后 47 项通过。首次集成的 57 项中两项结构失败，原因是新增测试直接使用 Common 却未声明测试
依赖，以及模块基线过时；补齐 test scope 并重新生成基线，没有扩大生产依赖或放松门禁。

完整重跑本批选择的 90 项，89 通过、1 项失败。唯一失败为上述三个已有 SPI 装配缺口。
Local Store 26 项、隔离 1 项、selection 2 项、API 隔离 1 项、Java exchange 8 项、上层 lifecycle 12 项
全部通过；结构 20 项、双 Worker 1 项、启动 smoke 3 项、八项 SQL 查询等通过。
最终 package 选定 70 项全部通过；按类取最近结果仍为 90 项中 89 通过、1 项 SPI 失败。
97 个 JAR、97 个唯一模块名、826 个编译包无审计违规；820 个源码包无循环。
三种角色实际 classpath 各只有一份本批 Shuffle API，SHA 与 reactor 制品相同，均没有误装配 Local JAR。
静态检查与 Native 制品核验结果见下列机器可读证据。
这些是选择执行，不是无筛选 full-regression verify；没有新增 ASan、性能或全仓完成结论。

## 证据

[FQN 处置](shuffle-local-store-fqn-dispositions.tsv)、
[逐类语义](shuffle-local-store-semantic-evidence.json)、
[调用引用](shuffle-local-store-callers.tsv)、
[输入校验](shuffle-local-store-backend-evidence.json)、
[包依赖图](shuffle-local-store-package-graph.txt)、
[验证命令](shuffle-local-store-validation.tsv)、
[逐测试](shuffle-local-store-tests.tsv)、
[验证汇总](shuffle-local-store-validation-summary.json)、
[发行物](shuffle-local-store-artifacts.json)、
[启动 classpath](shuffle-local-store-runtime-classpath.json)、
[静态检查](shuffle-local-store-static-evidence.json)、
[Native 制品](shuffle-local-store-native-artifacts.json)、
[文件处置](shuffle-local-store-file-dispositions.tsv)。
