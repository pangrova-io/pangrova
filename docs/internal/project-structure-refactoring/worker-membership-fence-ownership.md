# Worker 注册身份与条件注销（2026-11-03）

本批以工作区 8,398 个文本文件为基线，继续处理安全下线所依赖的成员身份边界。现有代码存在三个
可触发的错误：心跳或健康检查将 DECOMMISSIONING 改回 ACTIVE；健康回调期间注册的新 Worker 被
旧决策按逻辑 ID 删除；旧进程的 HTTP 停机请求也只带逻辑 ID，可能删除同名新进程。

## 实际实现链

WorkerRegistrationClient 在首次发送注册前保留本客户端的逻辑 ID 和执行 incarnation；相同身份可
重试，不能把客户端改绑定到另一进程。实际 DELETE 发送原 WorkerExecutionIdentity JSON，并保留
原内部认证、响应确认、失败重试及关闭顺序。未尝试注册的客户端不能撤销一个任意逻辑 ID。

WorkerResource 的 DELETE 要求身份正文，缺失时拒绝；认证仍先检查。Core 将正文映射为 Coordination
API 的 WorkerRegistrationIdentity（workerId、admissionDomainId、incarnationId），不由持久化 API
依赖 Protocol。WorkerRegistry 只保留带身份的原子 unregister，无 String 重载或弱化 default。
Memory 在同一 per-key compute 内比较并删除；PostgreSQL 单条 DELETE 同时匹配三个字段。已不存在
或已经被替换返回 false，故障传播。字段沿用现有 schema，没有历史数据迁移或新增数据库。

显式停机撤销在 Coordinator 尚未恢复本地缓存时仍执行条件持久化删除；旧身份和重放可确认已退出，
但不会删除替代进程。它是注册撤销，不是 Coordinator 已证明物理资源释放的凭据。Worker 的既有
关闭顺序仍先停止入口和心跳、完成 task owner 关闭，再发送撤销；本批没有复制 Native 状态机。

## 本地并发与状态

- 注册的持久化写入和本地发布在同一 Worker key 的 compute 内，与移除串行化；持久化失败不会先
  擦除本地记录。回调和 topology 通知不在这个 compute 内执行。
- 健康检查捕获用于判定的原始 WorkerInfo，回调后只有该观察仍为当前对象才移除。期间的心跳或
  新注册使观察失效。持久化端另按 incarnation 条件删除，保护另一 Coordinator 已登记的新进程。
- DECOMMISSIONING 不被心跳或普通健康评估撤销。同 incarnation 重复注册保留下线状态及最新运行
  计数，避免初始注册的零任务数覆盖近期心跳。真正新 incarnation 可以正常进入 ACTIVE。
- loadFromRegistry 不覆盖已经发布的本地观察，避免恢复扫描把正在下线或刚注册的节点改写为旧快照。

持久化 owner 仍是原 WorkerRegistry；本地拓扑 owner 仍是 WorkerManager。本批没有建立第二份
Shuffle 输出或任务状态表，也没有把诊断 InMemoryShuffleDataInventory 装配为删除许可。

## 测试与尚未完成项

新增测试覆盖 Memory/PostgreSQL 的旧 incarnation、跨 admission domain、幂等重放和变化后的心跳
指标；PostgreSQL 行锁测试确认 DELETE 实际等待并发更新，释放锁后重新检查身份。Core 覆盖健康回调
期间替换、心跳使观察过期、同进程重复注册与恢复扫描、跨 Coordinator 替换以及持久化失败保留状态。
实际认证 HTTP 覆盖缺身份、认证失败、旧身份、重放及空缓存恢复；真实 Worker 客户端的关闭测试
检查发送的 JSON 身份。新增整链路集成使用真实客户端、Coordinator HTTP 和 PostgreSQL。

这些更改没有完成安全缩容。以下已确认的需求继续实施：

- heartbeat 仍只携带逻辑 Worker ID，需要把 incarnation 加入发送、接收和持久化条件更新。
- DECOMMISSIONING 目前只保存在本地，跨 Coordinator 恢复的持久化接纳状态尚未完成。
- 选点到实际派发之间的接纳屏障、已进入任务、完成输出及活跃读取者的完整权威证据尚未接通。
- WorkerHealthScheduler 仍使用 runningTaskCount 信号完成 decommission；它不证明物理资源安全。
- dead-worker 回调仍按逻辑 ID 通知，条件删除修复不能代替 incarnation 级任务故障处理。
- 通用 Shuffle session/receipt、Local 生产接入和既有 inventory 装配门禁仍未完成。

首次定向运行在新增 PostgreSQL 测试遗漏 throws SQLException 处编译失败，已修正；不是生产故障或
环境缺失。源码证据扫描曾与 Maven clean 删除 target 目录竞争，已改用 rg 枚举源码，重新生成证据。
新的真实客户端整链路测试还暴露模块事实基线缺少 Worker registration 包的跨模块引用记录；核对后
通过标准生成器更新事实，没有修改依赖允许/禁止规则。后续结果以本批实际命令和 XML 记录为准，
不继承先前批次通过结果。新增整链路测试首次使用回环 Worker 公布地址，被已有注册地址校验拒绝；
修正为文档保留网段的数值地址（此测试不派发 Worker 执行），实际客户端到 Coordinator 的 HTTP
仍走本机真实连接。没有放宽地址校验，也不把该元数据整链路宣称为 Worker 执行测试。

最终证据：修正初次测试编译后定向 62 项通过；包含 HTTP/关闭流程的定向 113 项通过。首次跨模块
164 项为 163 通过、1 项既有 SPI 门禁失败；加入真实客户端链路后先后记录基线失败和夹具地址拒绝。
最新跨模块 176 项中的地址夹具错误已由最终 package 的真实链路测试验证修正，22 项 package 检查
全部通过。独立 JVM 的 WorkerBootstrapE2ETest 5 项通过（其中一项保留 5 次真实启停循环）。
按类取本批最近结果共 181 项：180 通过、1 项既有 InMemoryShuffleDataInventory 装配门禁失败。
没有修改该门禁基线；以上选择执行不是无筛选 full-regression verify。

9 项生产类处置、280 条引用；820 个源码包无环。97 个 JAR、97 个唯一模块名、826 个编译包无审计
违规，实际 Persistence API JAR 不再含 String unregister 签名。三种启动角色均加载一份当前
Coordination API，包含新的身份契约；不是角色隔离全部完成的证明。8 项静态检查通过，Native 构建、
Bridge 资源与 bundle 三处 SHA 一致。未改动 Native、Bridge、Proto、Schema、Shuffle 和客户端制品
源码输入；本批变动的是 Worker 注册 HTTP 客户端。没有新的 ASan、跨平台或性能完成结论。

[FQN 处置](worker-membership-fence-fqn-dispositions.tsv)、
[逐类语义](worker-membership-fence-semantic-evidence.json)、
[当前引用](worker-membership-fence-callers.tsv)、
[未改动输入](worker-membership-fence-input-evidence.json)、
[包图](worker-membership-fence-package-graph.txt)、
[验证命令](worker-membership-fence-validation.tsv)、
[逐测试](worker-membership-fence-tests.tsv)、
[验证汇总](worker-membership-fence-validation-summary.json)、
[发行物](worker-membership-fence-artifacts.json)、
[运行 classpath](worker-membership-fence-runtime-classpath.json)、
[静态检查](worker-membership-fence-static-evidence.json)、
[Native 制品](worker-membership-fence-native-artifacts.json)、
[文件处置](worker-membership-fence-file-dispositions.tsv)。
