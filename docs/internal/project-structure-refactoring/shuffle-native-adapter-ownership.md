# Native Shuffle 服务绑定的模块边界（2026-11-03）

本批完成既有 Native 服务命令的模块拆分。2026-11-07 起该模块已并入 `pangrova-worker-native-bridge`
（包 `org.pangrova.worker.nativebridge.shuffle`），adapter 模块已删除；本文保留拆分时的边界依据。

## 所有权与调用

`pangrova-worker-native-bridge` 拥有 `org.pangrova.worker.nativebridge.shuffle`（原 `pangrova-shuffle-native-adapter` / `org.pangrova.shuffle.adapter.nativebridge`，模块已删除）。
迁入真实 `NativeShuffleTransport`，删除 Shuffle API 中只面向 Native executor handle 的
`ShuffleTransport`，不保留旧 FQN 转发类。Worker Core 直接依赖 native bridge；该包不依赖 Worker Core 的任务编排。

适配器通过同步 `CommandChannel` 借用任务 executor 的 `executeCommand`。
生产调用仍经过原有命令校验、handle admission 和 normal/cleanup lease，不直接调用 NativeRuntime。
创建者不转移 executor 所有权；TaskManager/TaskResourceReleaser 仍在释放 executor 前停止对应服务，
保留 staged drain receipt 校验和单一 release owner。删除只记录日志的全局 no-op close，
WorkerTaskBootstrap 不再把无资源的适配器作为独立 closeable 保存。
Native 传输仍在 C++；没有把 Direct 字节流改为 JVM 中转。

公开 ShuffleServerHandle 是端点和 opaque ticket，不携带 executor 指针。
模式枚举不再把 PUSH/PULL 自动等同于持久化、重试或安全缩容保证。
Native serving 的显式绑定仅属于适配层，不冒充通用 Provider SPI。

## 逐类证据

11 项生产处置：7 项在类型映射后方法 token 一致、3 项逐行语义审查、1 项删除旧接口。
台账保存旧/新 FQN、实际调用、所有权和验证位置；52 条调用引用及 129 行生产差异作为补充。
最小 classpath 测试证明 API 不含旧 Native serving SPI 或实现；新适配器可在不加载 Worker Core、
Coordinator 和 Server 的情况下链接。模块初始化不会加载 Native runtime。

## 实际验证与失败处置

- 首次定向编译发现旧测试还调用已删除的 no-op close；替换为真实 start、幂等 stop、
  executor 由任务 owner 释放且重复释放为空的 JNI 验证。修复后 63 个定向用例通过。
- 首次集成门禁发现 IntegrationTests 使用实际 Native adapter 但缺少直接 test 依赖。
  更新其 POM、manifest 和 SqlE2ETestBase 的受保护命令回调，未增加门禁豁免。
- 后续 clean package 为 116 个用例：115 通过、1 个既有 SPI 装配门禁失败。
  RemoteShuffleByteEgressIntegrationTest 的 18 个用例、真实双 Worker、Native activation、
  取消、release race、启动失败清理均通过。
- 在该 clean 编译结果上 package，22 个选定门禁用例通过。
- 首次 JAR 审计仍要求旧批次中的 ShuffleTransport.class 存在；保留原失败报告，
  将这个已退役类型的正向断言替换为旧类及引用不存在、新实现及 CommandChannel 必须存在。
  其他断言保留。最终 95 个 JAR、95 个唯一模块名、822 个编译包扫描无违规。
- 当前 816 个生产源码包无 SCC，实际包循环测试通过。

按测试类取最近结果：116 个 Java 用例、115 通过、1 个 SPI 门禁失败；不宣称全量回归通过。
Native/C++、JNI Bridge、Protocol、Persistence、Schema、Client 本批输入不变。
三个 Native 制品 SHA 相同；未新运行 ASan 或性能对比，不借用历史结果充当本批结果。

## 仍需实现

四项真实缺口仍保留为失败：InMemoryShuffleDataInventory、SpoolingExchangeManager、
LocalSpillLifecycleManager、DefaultShuffleByteEgress 未形成生产调用闭环。
需补齐通用 Provider/session/receipt、明确 attempt 的 Local spool、Java egress 和真实清理，
不得为消除门禁而接入会伪造提交、最新 attempt 读取或静默清理失败的旧原型。
本批没有新增 Celeborn 模块或声称 Celeborn 集成已完成。

## 证据索引

[FQN 处置](shuffle-native-adapter-fqn-dispositions.tsv)、
[逐类审查](shuffle-native-adapter-semantic-evidence.json)、
[生产差异](shuffle-native-adapter-production-review.diff)、
[调用引用](shuffle-native-adapter-callers.tsv)、
[验证命令](shuffle-native-adapter-validation.tsv)、
[验证汇总](shuffle-native-adapter-validation-summary.json)、
[首次制品审计](shuffle-native-adapter-artifacts-initial.json)、
[最终制品审计](shuffle-native-adapter-artifacts.json)、
[后端输入](shuffle-native-adapter-backend-evidence.json)、
[静态门禁](shuffle-native-adapter-static-evidence.json)、
[Native 制品](shuffle-native-adapter-native-artifacts.json)、
[文件处置](shuffle-native-adapter-file-dispositions.tsv)。
