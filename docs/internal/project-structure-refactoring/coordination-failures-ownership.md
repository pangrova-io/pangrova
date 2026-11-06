# Coordination 后端失败与生命周期闭环

在 Coordination Persistence 拆分后的实际工作区继续实施。本批不是全仓生产验收；不新增数据库、
数据迁移工具、旧 FQN 转发壳或假生产装配。

## 契约与后端

`CoordinationStorageException` 从 PostgreSQL 实现移动到同一领域 API 的 `.api.error`。保留既有
客户端错误码和真实 cause；公开类型只依赖 Common/JDK。Registry 与 Lease 契约明确：存储失败、
损坏记录和未知写入结果不得被当作不存在、竞争或成功。失败的写入可能已提交，不能据此盲目重试
或删除状态；调用方必须重新确认权威。SDK cause 供诊断，不成为跨后端 wire 协议。

ZooKeeper Registry 的注册、删除、读取、列举、过期清理和更新不再吞掉后端错误。列举或清理中途
失败不返回看似完整的部分结果；清理可能已删除部分真实过期记录，异常不代表整个批次原子回滚。
CAS 有界重试耗尽明确失败。个别记录明确不存在、列举时记录并发消失以及正常版本竞争，仍按对应
操作契约处理，不把它们统一升级为存储宕机。

ZooKeeper Lease 的一般异常与解析异常传播，保留明确 NoNode/BadVersion/NodeExists 的既有竞争
处理。时钟节点单独封装：时钟丢失不能被外层 NoNode catch 当作没有 lease。构造初始化失败同样
明确传播；中断恢复线程标志；deadline 加法溢出拒绝。客户端仍由真实 Provider/Server 关闭，操作
不另建客户端或静默切换后端。

JDBC Coordinator 使用共享错误契约，删除读取 `last_processed_invalidation_id` 时捕获任意
SQLException 并返回 -1 的旧 schema 兼容行为。SQL NULL 仍是记录本身的既有 unknown progress；
列缺失或查询失败是错误。查询 SQL、连接所有权和锁语义未重写。JDBC Lease 仅改变错误类型 owner。

## 调用与资源闭环

```text
Provider 原始失败
  → 窄 API 错误契约
  → CoordinatorRegistryClient / LeaseBackedLeaderElection
  → HA / 独立 Maintenance 生命周期
  → 停止本地 activation、尝试必要清理、保留实际失败
```

配置了 Registry 后，其 register/heartbeat/role/unregister 错误不再被适配器吞掉。没有配置 Registry
仍是原有显式可选模式，不被当作后端故障时的降级路径。

HA 启动失败会停止已创建的选举调度器，并尝试登记和已知 grant 的清理。已确认 grant 的角色发布或
activation callback 失败会回滚本地领导状态并尝试释放。获取已提交但确认丢失时，没有本地确认的
grant；启动失败不能据此假装获得/释放了它，持久化 lease 可继续存在到确认或到期。

stepDown 先撤销本地领导状态和回调，再尝试远程释放与角色发布。远程释放失败不能使本地保持
LEADER；callback 失败也不跳过释放或 STANDBY 发布。降级回调必须幂等，失败后的关闭可以再次尝试清理。
周期续租失败只尝试一次 demotion，随后仍安排 standby 检测，
而不是被 ScheduledExecutorService 因异常永久取消。关闭先停止新选举，逐项尝试 timer、callback、
query authority yield、lease、registry、ownership 与 binding 清理，保留首异常和 suppressed 异常。
这不代表后端写入一定成功，也不将 query authority 和 singleton control lease 合成同一事实。

独立维护租约仍由 MaintenanceLeadershipBootstrap / MaintenanceLeaderElection 管理。其 poll 已有
异常时本地 demotion；本批补齐 close 同时发生 callback 与 release 错误时的异常保留。不把 HA
onDemotion 测试回调冒充实际维护装配。

## 验证范围

- 真实 ZooKeeper：关闭客户端下的 13 类操作必须失败；缺失时钟；损坏记录与不完整 topology。
- 在真实 Curator 操作外注入“提交成功后丢失确认”：登记与获取 lease 必须抛错；正常客户端读到
  实际提交结果，lease 同一 holder 的再次确认返回同一 grant，而不是假定第一次没提交。
- 对真实 registry 的 CAS 写入注入版本冲突，检查恰好三次尝试、明确失败、原 progress 不变。
- 中断 read 不被吞掉，线程 interrupt 状态保留。
- 真实临时 PostgreSQL：canonical 初始化后故意删除必需列，单项/leader/list 读取必须报错；不修改
  生产初始化资源，不建立另一套测试 schema。
- HA：启动回滚与重试、未知获取结果、失败 activation、step-down 回调失败、关闭多失败聚合、
  后端失联与 role publication 同时失败时的本地降级。
- Server 独立维护生命周期、已有 HA 切换、实际 SQL/双 Worker/Native 链继续验证。

首次近端 testCompile 因新 AssertJ `singleElement()` 结果类型使用错误失败；修正断言后近端 118 项
通过。随后补充回调传播和维护关闭异常聚合，扩大验证结果独立记录，不继承前一次通过。
全 reactor clean package 212 项中 211 通过，唯一失败仍为既有 Shuffle inventory 装配门禁。补齐
降级 callback 失败仍发布 STANDBY 的路径后，最终 package 191 项全部通过；独立 JVM Native
Bootstrap 5 项全部通过。按类取本批最新结果共 218 项：217 通过、1 failure、0 error、0 skip。

825 个源码包无环；100 个 JAR / 100 个唯一模块名 / 831 个编译包无审计违规。三个 launcher 校验
当前制品和角色闭包，8 项静态门禁全部通过。Native、Bridge、Protocol、Schema、Shuffle、Client
输入未变，三个 Native 制品摘要一致。本批没有执行 ASan、跨平台或性能基线，也未宣称完整
full-regression verify 通过。最终实际数字见 [验证汇总](coordination-failures-validation-summary.json)。

## 明确限制与后续工作

本批不证明真实 ZooKeeper 多节点 quorum 故障切换、Curator 内部所有重试/确认丢失组合、任意暂停下的
租约时钟保证或进程 incarnation 接管顺序。Coordinator membership 仍使用逻辑 ID；Registry 的路径
别名、私有 payload 编码与字段边界也需在 Coordinator 身份契约闭环中处理。Java callback 的完成和
所有跨线程启动/关闭竞争仍须继续审计，不能由本批同步故障测试推导全部满足。

Core 内现有 Query ExecutionAnchor 的隐式 Memory 构造还需随 Query Persistence 继续拆分；
本批没有据此宣称 Core 已完全无实现依赖。其它领域 Persistence 拆分、通用 Shuffle session/receipt/权威 inventory、Local 生产装配、Streaming
lease adapter、最终角色隔离、性能和完整生产回归仍未完成。既有 Shuffle SPI 装配门禁不豁免。

[逐类处置](coordination-failures-fqn-dispositions.tsv)、
[语义审查](coordination-failures-semantic-evidence.json)、
[调用引用](coordination-failures-callers.tsv)、
[输入比较](coordination-failures-input-evidence.json)、
[逐测试](coordination-failures-tests.tsv)、
[发行物](coordination-failures-artifacts.json)、
[实际 classpath](coordination-failures-runtime-classpath.json)、
[文件处置](coordination-failures-file-dispositions.tsv)。
