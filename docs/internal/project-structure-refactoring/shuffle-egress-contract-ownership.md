# Shuffle 批数据交付契约与真实回调（2026-11-03）

本批将生产远端 egress 的实际语义纳入统一 API 制品，解决旧声明与 JNI 回调不一致的问题。
它不完成通用 Provider/session/receipt、Local spool 或 Celeborn 接入。

## 契约和实际 owner

`pangrova-shuffle-api` 内的 `org.pangrova.shuffle.spi.worker.ShuffleByteEgress` 拥有
批数据交付、生产者完成与中止的契约。旧 `.api.ShuffleByteEgress.onPartitionCommitted`
从未匹配实际 JNI 回调，现已删除旧 FQN，不保留兼容壳。

实际回调是 `onPartitionBatch(queryId, taskId, partitionId, rowCount, buffer)`、
`onTaskCompleted(queryId, taskId)`、`onTaskAborted(queryId, taskId, reason)`。
rowCount 是行数，不是 sequence。task/query 只索引已经安装的授权 attempt；完整 fencing、
worker incarnation、generation、access capability 和 canonical admission seal 仍由原 binding owner 持有。
回调不能创建新权限、覆盖绑定或选择最新 attempt。

调用方拥有 buffer；同步返回（包括异常返回）前，provider 必须完成消费或取得自己的字节副本，
不得异步持有 Native 借用内存。批交付不等于持久化或 producer commit；producer commit 不等于
exchange seal/发布。重复 write 是另一批数据，不能当成幂等重试。完成后绑定退休，重复完成或
迟到写入明确拒绝；中止缺失绑定幂等返回，但不会撤销可能已完成的远端提交或删除已发布数据。
提交异常不证明回滚。取消、deadline、背压和未完成清理仍由实际 provider 生命周期协调。

`RemoteShuffleByteEgress` 是生产实现，WorkerTaskBootstrap 的原有构造与 Native 注册路径保持。
它只增加接口和 Override 注解：逐 token 检查确认没有改变原有方法体。Native executor 的安装、
exact-request release 和 recovery 仍依赖当前具体实现；后续必须继续抽象 Worker admission/session，
本批没有把这个剩余依赖包装成已完成的 Provider 分离。

## 退役与真实能力范围

删除未装配的 DefaultShuffleByteEgress、其专用 ShuffleDrainException 和四个原型单元测试。
旧原型把每批数据当成完整 segment 提交，缓存未使用且成功路径不关闭 writer，也没有实际 JNI
调用入口。退役它不是删除一个可用的 Local/Celeborn 执行路径。生产远端实现承担新的真实契约；
原有 18 个真实服务测试保留，并新增四个边界测试和一个实际 Native 出站/入站测试。
SpoolBackedExchangeManager 的注释明确当前只有 Java-page 路径，不能宣称已连接 Native。

五项生产处置：一个接口移动并重定义、一项实际实现接入、一项仅文档纠正、两个旧原型类型删除。
对应 FQN、方法差异、调用引用和 SHA 均单列。没有添加 SPI 门禁豁免或空 Provider。

## 验证及首次失败

- 最近模块 22 个用例通过：API 全签名最小 classpath、Native adapter 隔离、Worker 启动和失败清理。
- 共享 clean package 为 85 个用例：83 通过、2 失败。一个是原有 SPI 装配门禁，现剩三项；
  另一个是新增测试错误地要求单批回调返回后已存在持久化 batch 记录。
- 修正测试依据真实交付契约，只在完成前检查不存在 producer commit，完成后仍检查完整 batch
  和 commit 记录、原始 payload 与 rowCount/sequence；没有降低生产语义。22 个远端服务用例及
  8 个 RegressionSelectionTest 全部通过。
- 独立 Maven/JVM 的 Native 出站 1 项、入站 3 项、通道配额 1 项、AI 注册 1 项均通过。
  共享 full-regression 默认排除 runtime owner，不能将这些未发现的测试计为共享阶段通过。
- 新的 Native 测试实际运行 C++ ExchangeRel producer → JNI egress → PostgreSQL/对象存储/mTLS
  Shuffle service → finalize → C++ consumer → Flight。单 producer/partition 的一行结果为 42，
  producer/consumer handle 释放后内存归零。它不替代多 producer、故障全覆盖或性能验证。

最终 package 的 30 个选定门禁用例通过。95 个 JAR、95 个唯一模块名、823 个编译包完成检查；
旧 `.api.ShuffleByteEgress`、DefaultShuffleByteEgress、ShuffleDrainException 及其生产引用均不存在，
新的 SPI 三个方法和实际实现必须存在。既有制品检查继续保留，未扩大共享包或增加例外。
当前 817 个生产源码包无循环。全部本批阶段按类取最近结果为 99 个 Java 用例：98 通过、1 个
SPI 门禁失败；其中包括上述 6 个独立 Native 用例。它不是完整 reactor 回归通过。

## 独立运行测试的 CI 补全

将 NativeShuffleEgressContractIntegrationTest 加入隔离清单。源码清点同时发现既有
NativeAiFailureProjectionRegistrationTest 未入清单，现补入并标记 native-required，避免普通
共享 JVM 执行拥有全局 Native 生命周期的测试。两个测试均已实际独立运行。
新增测试检查每个标记 native-runtime-owner 的测试源码都出现在 CI 隔离步骤中；保留原有
指定方法选择断言。隔离清单由五组变为七组。本批只重跑受影响四组，不宣称另三组已重新验证。

## 文档与静态证据

文档引用门禁首次发现十个名字：包括真正过时的装配/CALL/Flight 类名、明确退役的历史名字和
JDK RuntimeException。按实际源码改为 SecurityAdministrationBootstrap、TableProcedureCall、
BatchStreamProducer；退役名字保留为明确历史说明，JDK 类型使用完整类名。同步删除安全章节
仍声称旧 mutation handler 存在的矛盾段落。门禁脚本和排除项没有修改，303 个当前类引用检查通过。
初次 Python YAML 语法检查因本机没有 PyYAML 未执行，改用 Ruby 标准 YAML 解析成功；
RegressionSelectionTest 还实际解析 CI YAML 和检查选择规则。

Native/C++、JNI Bridge、Protocol、Schema、Persistence、Client 的本批输入未变，保留匹配源码
的 Native 制品。这里是契约声明与现有方法体对齐，没有改变 JNI 签名、缓冲区算法或所有权实现。
没有新 ASan、跨平台、全量回归或性能结果。

## 尚未完成

SPI 门禁仍拒绝三项未装配能力：InMemoryShuffleDataInventory、SpoolingExchangeManager、
LocalSpillLifecycleManager。Local 的精确 attempt 选择、原子发布、活跃读取保护和可重试清理仍要
完成，不能从“落盘”或 EMPTY commit 推导容错。通用异步 Writer/Reader、Provider factory、
worker/coordinator session、receipt 与实际 Celeborn provider 都不能用本批同步交付接口代替。

## 证据索引

[FQN 处置](shuffle-egress-contract-fqn-dispositions.tsv)、
[逐类审查](shuffle-egress-contract-semantic-evidence.json)、
[生产差异](shuffle-egress-contract-production-review.diff)、
[调用引用](shuffle-egress-contract-callers.tsv)、
[测试结果](shuffle-egress-contract-tests.tsv)、
[验证汇总](shuffle-egress-contract-validation-summary.json)、
[源码包图](shuffle-egress-contract-package-graph.txt)、
[后端输入](shuffle-egress-contract-backend-evidence.json)、
[发行物](shuffle-egress-contract-artifacts.json)、
[首次文档门禁](shuffle-egress-contract-static-initial.json)、
[最终静态门禁](shuffle-egress-contract-static-evidence.json)、
[Native 制品](shuffle-egress-contract-native-artifacts.json)、
[文件处置](shuffle-egress-contract-file-dispositions.tsv)。
