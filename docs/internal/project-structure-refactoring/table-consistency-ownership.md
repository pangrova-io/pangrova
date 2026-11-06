# Table Persistence 契约一致性与事务清理

本批在 [Table 后端拆分](table-persistence-ownership.md) 的工作区基线上继续，未重新开始全仓重构。
修复三个生产类及对应测试；没有增加模块、数据库产品、schema 或历史数据迁移。
逐文件基线包含 8,514 个文本文件，所有无关工作区改动保留。

## 版本观察的权威顺序

PostgreSQL 在同一表控制行的写事务中推进 revision，再写入 observation；历史查询按
`observation_id DESC`，因此使用该表接受的 revision 顺序。旧 Memory 实现按调用方
`observedAt` 排序，遇到时钟回拨或相同时间戳就与 PostgreSQL 不一致。

`TableControlStore.listVersionObservations` 现在明确：仅返回本表成功保存的观察，
按接受 revision 的倒序，非正 limit 返回空集合；时间戳是观察证据，不是排序键；
返回结果与后续写入脱离，不承诺跨表全局顺序。没有修改方法签名或 PostgreSQL SQL。
Memory 保留原有 monitor 内的 record 校验、保存和 append 原子边界，从历史尾部读取。
已有所有访问均受同一 monitor 保护，移除重复的 CopyOnWriteArrayList 复制开销。

两个真实 Provider 的测试保存三次版本变化，同时制造时间回拨及时间戳相同，断言完整
顺序、limit、其他表过滤及当前版本。首次回归中 Memory 失败，PostgreSQL 通过。
这不是把观察时间强行变为单调时间，也没有改写 Connector 提供的版本事实。

## 治理绑定事务所有权

原 save 已显式回滚，delete/deleteAll 仅靠连接关闭清理。后者丢失实际 rollback 失败信息，
也未按连接抽象显式结束失败事务。现在三种 mutation 均捕获 SQL、RuntimeException、Error，
尝试 rollback 并将清理异常附加到主异常；SQL 异常仍由既有领域异常包装。
既有 AuthorizationTransactionGate 仍在 mutation 前获取，成功时仍由操作提交和关闭连接。
没有新增通用事务 callback、自动重试、Memory fallback 或跨领域事务 owner。

测试在真实 PostgreSQL 完成 DELETE 后注入三类故障，并分别使 rollback 成功或失败，
覆盖 delete 与 deleteAll 共 12 种组合；核实目标和其他表的数据均保留、原异常身份及
suppressed 异常准确，之后新 mutation 可正常获取权限锁。失败回滚场景的实际物理连接
仍被关闭，测试不宣称断连情况下可以获知数据库最终提交结果。

另一个真实事务测试验证静态 `list(Connection, UUID)` 看见调用方未提交的删除，其他连接
仍看见已提交版本；参与者不提交、不回滚、不关闭连接，调用方 rollback 后记录恢复。
提交确认丢失后的未知结果语义不因本批得到新增实现。

## 验证与边界

首次回归 37 个测试中 7 FAIL（1 排序、6 回滚异常缺失），0 ERROR、0 SKIP。
修复后近端 clean 测试 128 PASS。全 reactor 的 clean 消费者验证 245 / 244 PASS / 1 FAIL，
0 ERROR、0 SKIP；包含真实 Table 生命周期、权限/标签/行策略、Paimon rename、Native 读写、
双 Worker 与运行角色测试。唯一失败仍是未接线的 InMemoryShuffleDataInventory。
未放宽门禁，也未给该类添加豁免。随后全 reactor package 的 21 项架构测试全部通过。
按每个测试类最后一次实际执行汇总，当前选择范围 245 / 244 PASS / 1 FAIL。
8 项静态检查通过，Native 三处制品 SHA 一致。没有修改 JNI/协议/Native 输入，
没有新增 ASan、性能或全平台验证结论。
不将测试替身或局部 PASS 作为全仓生产级完成证明。

证据：[逐类处置](table-consistency-fqn-dispositions.tsv)、
[语义及资源所有权](table-consistency-semantic-evidence.json)、
[调用引用](table-consistency-callers.tsv)、[测试汇总](table-consistency-validation-summary.json)、
[逐测试](table-consistency-tests.tsv)、[输入未变更核验](table-consistency-input-evidence.json)、
[静态检查](table-consistency-static-evidence.json)、[Native 哈希](table-consistency-native-artifacts.json)、
[逐文件处置](table-consistency-file-dispositions.tsv)。

Metadata 的默认后端选择、跨域 workflow、完整 Provider 工厂、通用 Shuffle 和最终全量
生产回归仍在原计划范围内。历史批次记录保留原当时的观察，不将本批结果倒填为历史通过。
