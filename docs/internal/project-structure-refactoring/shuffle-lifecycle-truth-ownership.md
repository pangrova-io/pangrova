# Shuffle 生命周期与缩容保证纠偏（2026-11-03）

本批以工作区 8,386 个文本文件为基线，逐一检查上一批装配门禁报告的三个类型及实际调用链。
发现两个 Local 生命周期包装没有生产调用，且 commit 返回 EMPTY；第三个 inventory 只有手工记录，
却从空记录或 retention 推导可安全下线。它们不能作为已实现发布或安全缩容的证据。

## 已实现的更改

- 删除未装配的 LocalSpillLifecycleManager 和 SpoolingExchangeManager。真实 LocalFsSpoolStore、
  工厂服务注册、SpoolBackedExchangeManager、选定 attempt 读取、完整性检查和读写 lease 均保留。
  Local 工厂测试直接覆盖发现、创建、写入提交、选定读取、活动 reader 阻止清理、关闭后清理。
- 原 LocalShuffleLifecycleManager 实际是 DAG 在未配置外部服务时使用的默认对象，既不创建 Local
  存储，也不拥有 Direct Native 数据。用 Coordinator 内 UnavailableExternalShuffleLifecycle 表达
  该配置状态，register/commit 明确拒绝；cleanup 无外部资源可释放。Direct 资源仍由原 Native
  task/output owner 管理。删除 ShuffleHandle.LOCAL 空端点哨兵；真实外部 handle 必须完整有效。
- ShuffleRegistration 拒绝空 query/负 stage。ShuffleCommitResult.EMPTY 仅表示空统计，不证明发布。
  ShuffleLifecycleManager 文档不再允许未注册阶段用 EMPTY 冒充提交成功。
- ShuffleDataInventory 删除乐观 default assessment，要求实现显式提供判定；新增 UNKNOWN，不能
  解释为安全、等待即可下线或需要重算。InMemoryShuffleDataInventory 保留诊断 CRUD，但所有
  assessment 返回 UNKNOWN。没有把它接到生产来消除门禁，也没有添加装配豁免。
- ShuffleDataRef 的 retention 仅是分类，不能证明数据在特定故障后可读；空 query/worker、负 stage
  拒绝。缩容判定要求完整清单、Worker incarnation、派发接纳屏障、选定 generation/attempt、活跃
  reader 和本次输出实际有效的 Provider 保证；判定不是可转交、无限有效的删除许可。

当前 API 仍包含自研服务专用生命周期和旧 inventory 模型。本批没有把这些模型包装成最终通用
ShuffleProviderFactory/session/receipt，也没有创建 Celeborn 空模块。

## 测试真实性与保留能力

旧 ShuffleLifecycleIntegrationTest 只调用测试自带的 Recording 实现，不能证明装配。替换为对真实
DAGDispatchContext 默认 binding 的验证，并在真实 ShuffleStageHandler 调度外部 stage 时验证早期
拒绝、未分配 handle。Direct DAG 取消/清理和真实远端生命周期测试继续运行。

删除的 Spooling 包装测试不代表删除 Local 实际能力：Local Provider 的真实 I/O、Factory、跨进程
目录锁、读写 lease、指定 attempt、损坏拒绝和 Core exchange 测试保留。替换 inventory 测试中从
PERSISTENT/DETACHED/空清单推导 SAFE 的断言，覆盖各 retention、消费状态与移除后的 UNKNOWN。

## 尚未完成的实际生产边界

WorkerManager.isDecommissionComplete 目前仍依靠 DECOMMISSIONING 加 heartbeat runningTaskCount=0，
WorkerHealthScheduler 据此移除节点。该条件不证明已完成任务的 Shuffle 输出没有活跃消费者。
这一路径本批未改，不能因为诊断 inventory 现在保守就宣称生产缩容安全。

后续必须由实际任务签发/执行权威和输出/reader owner 联合提供完整证据，关闭接纳并处理选点到
派发之间的竞争，按 Worker incarnation 条件移除。CoordinatorTaskTracker 的活动任务反向索引
不是已完成输出清单，不应复制为第二个权威状态机。超时/节点丢失恢复与主动安全下线需要分开。
Local Native/session/receipt、完整 exchange 身份、异步背压和最终通用 Provider 接入仍继续实施。

## 验证证据

验证结果在本批命令、逐测试和发行物台账中记录。首次 focused 命令的 Maven 多模块参数错误导致
零测试，修正后执行 125 项，其中 1 项 gRPC 收到 SSH 协议头；最小单测复现通过，不能据此抹去
首次失败。测试服务随后改为明确绑定客户端使用的 IPv4 loopback，替代通配地址，重新验证。
该异常尚无确定根因，不把后续通过解释为已证明网络故障根因修复。

API 最小 classpath 等 38 项通过；修正测试绑定后的定向 125 项通过。全 reactor clean/package
选定 189 项，188 项通过，1 项 SPI 门禁仍报告 InMemoryShuffleDataInventory 未装配。真实双 Worker、
启动、Durable Shuffle 11 项和实际远端出站/资源/授权 22 项通过；没有新增门禁基线或豁免。
独立 JVM 的真实 Native 出站用例通过，覆盖 C++ producer、远端服务/对象存储、finalize、C++
consumer 和结果传输，不是 Java fake 代替 Native。12 项生产类处置中 3 项规范化 token 相同，
215 条当前引用单列；820 个源码包无依赖环。

最终 package 的 21 项架构检查通过；按类取本批最新结果共 190 项，189 通过、1 项上述 SPI
门禁失败。这是选定验证，不是无筛选 full-regression verify。97 个 JAR、97 个唯一模块名、826 个
编译包无审计违规；旧三个生命周期类、LOCAL 哨兵和 Core spool 包不在发行物中。Local 工厂服务
描述符只有 Local Provider 一份；自研服务可执行 JAR 不携带 Shuffle Core。实际三种角色加载当前
API，Worker/Standalone 加载当前 Provider，均以 SHA 核对。Local 尚未在运行角色装配。

8 项静态检查通过；构建、Bridge 资源和 Native bundle 的共享库 SHA 一致。Native、Worker、Protocol、
Persistence、Schema、Client、Server 装配、Local 生产代码和自研远端 Provider 输入未变化。没有新
ASan、跨平台、性能、Celeborn 或全仓生产就绪结论。

[FQN 处置](shuffle-lifecycle-truth-fqn-dispositions.tsv)、
[逐类语义](shuffle-lifecycle-truth-semantic-evidence.json)、
[当前引用](shuffle-lifecycle-truth-callers.tsv)、
[输入边界](shuffle-lifecycle-truth-backend-evidence.json)、
[源码包图](shuffle-lifecycle-truth-package-graph.txt)、
[验证命令](shuffle-lifecycle-truth-validation.tsv)、
[逐测试](shuffle-lifecycle-truth-tests.tsv)、
[验证汇总](shuffle-lifecycle-truth-validation-summary.json)、
[发行物](shuffle-lifecycle-truth-artifacts.json)、
[运行 classpath](shuffle-lifecycle-truth-runtime-classpath.json)、
[静态门禁](shuffle-lifecycle-truth-static-evidence.json)、
[Native 制品](shuffle-lifecycle-truth-native-artifacts.json)、
[文件处置](shuffle-lifecycle-truth-file-dispositions.tsv)。
