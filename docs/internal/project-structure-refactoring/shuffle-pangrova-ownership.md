# Pangrova 远端 Shuffle Provider 实现边界（2026-11-03）

本批提取已有真实生产实现，不新建空 Provider，也不宣称已完成通用 session、Local 或 Celeborn。

## 模块与包归属

新增 `pangrova-shuffle-pangrova`，唯一拥有 `org.pangrova.shuffle.pangrova.client` 及其 security
子包。17 个原 Worker 远端实现类迁入，包括 producer egress/stream、consumer read、客户端、
连接池/lease/owner、通道 admission、权限续租、TLS 与 wire frame 校验。
RemoteShuffleClient 明确命名为 `PangrovaShuffleClient`，其他角色名随原有职责保留。
六个同包测试随实现迁移，避免扩大 package-private 可见性或让生产实现依赖 Worker 测试资源。

`CanonicalBatchChunks` 及其测试迁至 Protocol 的 `org.pangrova.protocol.shuffle.codec`。
它只拥有 canonical frame、分块边界和 SHA-256 编码，不拥有任务/连接/提交状态。Provider 与
Worker ingress 都通过 Protocol 复用同一实现；未修改 Proto schema 或 Java/native wire 格式。

Worker bootstrap、NativeTaskExecutor 的 exact admission/recovery/release、NativeExchangeTransport
及集成测试更新为引用新制品。生产注册仍由原 Worker bootstrap 创建真实 egress 并交给 JNI，
没有新增全局 Service Locator、兼容转发类、重复状态机或额外资源 owner。

## 资源与一致性

- Byte egress 保留原有任务 admission、完整 request seal、失败候选和取消后的清理 owner。
- Producer 保留分区统计、sequence、manifest checksum、commit 和未知提交处理路径。
- Consumer 保留分批权限续租、完整 chunk/terminal 校验、拒绝截断与原有 Native ingress。
- Pool、client lease 和通道 quota 的创建、借用、关闭、失败保留和回收顺序不变。
- mTLS、访问 capability、deadline、错误分类和服务端当前权限检查不变。
- buffer 借用及复制边界不变；没有引入 C++ SDK 或强迫 Direct 流量经过 JVM。

18 个生产类和 7 个测试移动。共 29 个生产类受影响，映射包名/类型后，方法体 token 全部一致；
比较排除 import、注释和空白，保留字面量。169 条调用/引用单列。不是仅凭包名扫描宣称语义审查。

## 本批明确未完成的依赖

Provider 的 Maven 闭包不依赖 Worker Core、Native Bridge、Native Adapter、Coordinator、Planner
或 Shuffle Service。最小模块测试加载全部实现签名，并验证这些运行时类型不在其 classpath。
这证明当前依赖闭包，不等同于插件 SDK 多版本隔离或完整 ProviderFactory 接入。

Provider 仍引用 Shuffle Core 的现有 exchange 模型和 manifest checksum；这些共享契约/编码
仍要进一步归位。Worker Core 也仍持有具体 provider admission、reader 和 bootstrap 依赖，必须
由后续通用 session/reader 契约及 Server Worker 装配消除。本批没有把 allowedImports 当作最终
架构豁免，也不满足全仓“Core 不依赖可替换实现”的最终标准。

## 需求审查结果

旧 ShuffleCapabilities 把 LOCAL_SPILL 自动推导为 retry/DETACHED，并据此声称安全缩容；
存储介质不能证明读取可达性、选中 attempt、进程恢复或活跃读取保护。这些契约及未装配的
SpoolingExchangeManager、LocalSpillLifecycleManager、InMemoryShuffleDataInventory 仍需实际
设计/实现。Native 当前已有局部落盘机制，也不能直接等同于完整 Local Provider 的持久化保证。
不通过接通 EMPTY commit、读取最新 attempt 或无条件 drain 成功来消除门禁。

## 验证中的实际失败

独立 Provider 30 项及 Worker/契约定向 52 项通过。首次全 reactor 定向 clean package 在 24 项后
出现两项结构门禁失败：旧位置遗留空包目录，以及 manifest exports 不属于声明的 packageRoots。
只删除迁移范围内确认为空的目录，收敛新模块声明为实际 client 包根；没有修改或豁免门禁。
独立 classpath 测试的 Planner 负向类型也已核对为实际存在的 PlannerContext，避免虚假隔离断言。

最终命令、用例、首次失败和当前结果以同目录 validation/tests 文件为准。Native/Proto schema、
JNI Bridge、Persistence、Schema 和 Client 输入 SHA 保持不变；Java codec 移动单独记录。
没有新的 Native 算法、内存/并发实现变更，不继承历史 ASan/性能/全量回归结果。

## 最终验证结果

- 独立 Provider 定向 30 项、Worker/契约定向 52 项通过。
- 跨模块 clean package 的 115 项中 114 项通过；唯一失败为已有 SPI 门禁的三处未装配能力：
  InMemoryShuffleDataInventory、SpoolingExchangeManager、LocalSpillLifecycleManager。
  不加入豁免，不用空提交或默认允许缩容掩盖缺口。
- 共享阶段排除的 Native owner 测试分别在独立 JVM 运行：出站 1、入站 3、任务接纳 1，全部通过。
  真实双 Worker、22 项远端服务 egress、权限续租、连接生命周期、启动失败清理在共享阶段通过。
- 最终 package 选定 63 项通过。按类取本批最近结果共 128 项：127 通过、1 项 SPI 门禁失败。
  该统计包含五项独立 Native 用例，不等于未筛选的 full-regression verify。
- 96 个实际 JAR、96 个唯一模块名、823 个编译包在审计范围内无违规；817 个源码包无循环。
- Worker 与 Standalone 的真实打包启动器分别解析 187、312 个 classpath 项；各包含且仅包含一份
  新 Provider JAR，SHA-256 与本次 reactor 产物一致，包含新的客户端类。没有继承旧本地仓库制品。
  Docker 的 dependency:copy-dependencies 路径已检查；本批未构建容器镜像或宣称部署通过。

## 证据索引

[FQN 处置](shuffle-pangrova-fqn-dispositions.tsv)、
[逐类方法比较](shuffle-pangrova-semantic-evidence.json)、
[实际调用](shuffle-pangrova-callers.tsv)、
[源码包图](shuffle-pangrova-package-graph.txt)、
[后端与 wire 输入](shuffle-pangrova-backend-evidence.json)、
[验证汇总](shuffle-pangrova-validation-summary.json)、
[逐测试结果](shuffle-pangrova-tests.tsv)、
[发行物](shuffle-pangrova-artifacts.json)、
[实际启动 classpath](shuffle-pangrova-runtime-classpath.json)、
[静态门禁](shuffle-pangrova-static-evidence.json)、
[Native 制品](shuffle-pangrova-native-artifacts.json)、
[逐文件处置](shuffle-pangrova-file-dispositions.tsv)。
