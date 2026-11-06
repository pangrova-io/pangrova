# Shuffle 显式能力契约（2026-11-03）

本批纠正能力语义及全部现有消费者，不将未装配的 FTE/spool 原型宣称为生产能力。
本批开始前的上一轮已完成 Pangrova Provider 提取、真实 Native 链路和发行审计，是实际进展；
本批以当时工作区 8,328 个文本文件建立独立基线，不继承上一轮测试结果。

## 已实施的契约

删除原 ShuffleCapabilities 及其从 durability 推导 task retry、retention 和 safe scale-down 的
默认方法。统一 API 制品中的 `org.pangrova.shuffle.api.capability` 现在拥有：

- `ShuffleCapability`：阻塞/流水读取、consumer retry、producer retry、推测 attempt 隔离、
  compute Worker 退出后可读、storage Worker 故障后可读、Coordinator session 恢复、
  partition/range 读取、retention、cleanup，分别声明。
- `ShuffleCapabilityDescriptor`：不可变能力集合、明确支持查询、交集运算。复制集合及数组输入，
  拒绝 null；交集不能增加任一输入没有的能力。重复声明不改变值语义。

有效保证必须来自实现、部署和本次 exchange 证据的交集。descriptor 仅承载保证，不能
替代 receipt、选中 attempt、当前授权、reader lease 或缩容批准。它不保存状态机，不创建资源。
存储介质、PUSH/PULL 和 retention 分类仍保留原枚举值，但不提供任何隐含能力。
没有旧 FQN 转发壳，没有 SDK、Protocol、数据库或 Native 依赖进入契约。

## 调用与装配审查

| 能力/类型 | 当前实际状态 | 本批处置 |
|---|---|---|
| Descriptor | API、FTE policy helper、retry validator、spool prototype 使用 | 全部切换新类型；不可变及最小 classpath 验证 |
| FTE policy helper | 仅测试创建，未进入生产 DAG | 显式检查 CONSUMER_RETRY；入口改为 handleConsumerFailure，不能授权 producer 重放；原预算算法不变 |
| Retry validator | 仅测试调用，未进入启动或 dispatch | 入口为 validateConsumerRetry，明确消费端要求；删除“选 LOCAL_SPILL 即支持”的错误配置建议 |
| Spool factory | 有真实 LocalFs 工厂与 ServiceLoader，外层无生产 caller | 不再接受调用方注入能力，不从 store.durability 推导，报告空的已验证保证 |
| 生产 Stage 重试 | Stage / RetryStageHandler 拥有真实状态转换 | 本批没有接入第二套 helper/counter；相关执行测试验证原链路 |
| Pangrova 远端 Provider | 上一批已分离的真实 egress/read 路径 | 本批源码、装配与协议不变；不从其远端存储位置推导未验证保证 |

空能力集合明确表示没有已验证保证，不表示该原型实现了失败恢复。真实 Local store 的发现与
创建也必须保留空集合；契约测试检查其不能通过 consumer retry 要求。没有扩大 SPI 门禁豁免。

历史实施台账将 SpoolStoreFactory 标为 wired，只证明了 loader 存在，未证明外部可达调用链；
现已修正为 unwired，并链接当前证据。旧 FTE helper 不能直接并行接入生产成为第二个 Stage owner。

## 逐类处置与资源

13 项生产处置：新增两种契约值类型，删除一项错误默认契约；其余按实际语义更新或澄清。
6 项方法体 token 完全不变（比较保留字符串字面量），其余逐类记录具体变化。
FQN、当前调用、资源所有权和验证入口单列。没有移动 SQL、Proto 或 Native，也没有新的关闭责任。

原 spool register/EMPTY commit/cleanup 仍不满足通用 publication 与活跃 reader 保护要求，本批
没有把返回空能力当作完整实现。InMemoryShuffleDataInventory、SpoolingExchangeManager、
LocalSpillLifecycleManager 的真实装配缺口继续保留。inventory 的完整性/epoch、通用 session、
receipt、Local Native 接入、Provider 工厂、真实调度 admission 和故障后可读性仍须完成。

## 验证范围

- API clean test 35 项通过：每项能力不隐含其他能力、三层交集、不可变性、null 边界、旧类型不存在及最小 classpath。
- Coordinator 与上游定向 84 项通过：FTE 预算与显式 consumer 判断、validator、真实 ServiceLoader 创建 Local store、LocalFs I/O。
- 全 reactor 定向 clean package 135 项中 134 项通过。唯一失败为上述三项已有 SPI 装配缺口。
  实际双 Worker、启动 smoke、生产 Stage 重试 owner、模块与包依赖门禁通过。
- 契约断言已从错误的介质推导改为独立保证和交集；生产行为测试与门禁未禁用或放宽。
  删除 null-capabilities 构造器测试是因为可注入声明的构造器本身已删除；真实 factory 反向检查已补充。

- 最终 package 选定 39 项通过。按类取本批最近结果共 135 项：134 通过、1 项 SPI 门禁失败。
- 发行审计首次仍要求已删除的旧能力类存在，已把正向检查更新为新 descriptor，同时保留旧类及其
  bytecode 引用的反向检查；首次失败报告保留。当前 96 个 JAR、96 个唯一模块名、824 个编译包
  在审计范围无违规，818 个源码包无循环。

- Coordinator、Worker、Standalone 的实际打包 classpath 各只有一份本批 Shuffle API JAR，
  SHA-256 与 reactor 产物相同，且包含新 descriptor。8 项静态检查通过，三处 Native 制品 SHA 一致。

上述都是筛选执行，不是未筛选的 full-regression verify。Native/JNI/Protocol、Worker、Server 装配、
Pangrova Provider、Persistence、Schema、Client 输入与基线一致；本批没有 Native 内存/并发代码
变更，不新增 ASan、性能或三种 Provider 完整合约通过结论。最终 package、发行物及静态结果见下列证据。

## 证据

[FQN 处置](shuffle-capabilities-fqn-dispositions.tsv)、
[逐类语义](shuffle-capabilities-semantic-evidence.json)、
[调用与装配引用](shuffle-capabilities-callers.tsv)、
[包依赖图](shuffle-capabilities-package-graph.txt)、
[输入校验](shuffle-capabilities-backend-evidence.json)、
[命令与结果](shuffle-capabilities-validation.tsv)、
[逐用例](shuffle-capabilities-tests.tsv)、
[验证汇总](shuffle-capabilities-validation-summary.json)、
[发行物](shuffle-capabilities-artifacts.json)、
[首次发行审计](shuffle-capabilities-artifacts-first.json)、
[启动 classpath](shuffle-capabilities-runtime-classpath.json)、
[静态门禁](shuffle-capabilities-static-evidence.json)、
[Native 制品](shuffle-capabilities-native-artifacts.json)、
[文件处置](shuffle-capabilities-file-dispositions.tsv)。
