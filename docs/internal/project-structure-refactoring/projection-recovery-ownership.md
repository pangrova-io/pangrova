# Metadata 持久化恢复闭环（2026-11-03）

本批接续 `projection-fence-ownership.md`。上一批记录的逐操作跨轮次/重启恢复缺口由本批解决；整轮扫描一致性缺口仍保持未完成。基线为开始实施时的 8,804 个文本文件及实际工作区；此前工作不回退。本批不是整个模块化计划、整轮同步一致性或生产发布的最终验收。

## 已实现的生产调用链

```text
ModuleIntegrationBootstrap.runMetadataSync
  → MetadataSyncService.syncWithRetry / incrementalSync / fullSync
    → MetadataSyncRepository.recoverPending(原始 deployment)
    → MetadataProjectionTransactions.recoverPending(原始 deployment)
    → loadCheckpoint
    → 外部 Catalog 读取
    → 原始作用域下的 projection apply
    → 同步完成 complete
```

Server 继续选定 PostgreSQL Provider 并拥有共享 DataSource。本同步路径的 Core 只借用契约；本批未向其加入数据库、JSON、连接池、进程全局表或静态恢复状态。恢复先于源读取，恢复错误不能进入全量 fallback。原计划要求的完整 scan lease、源版本排序和完整删除盘点仍须继续实现。

## 接受、提交与恢复

两个持久化端口都采用相同的明确语义，分别拥有自己的业务事实，不合并为万能 Store：

1. 在当前 Catalog 行锁内检查既有回执、原始 deployment、请求身份和未完成操作。保存不可变 intent 后提交；请求由 Catalog 名称持久化发现，不依赖调用进程记住 UUID。
2. 第二个事务再次锁定并验证 deployment，验证 intent ID 与完整内容，完成业务变更、回执与 intent 状态转换。
3. Projection 的事务同时包含父 Schema 观察、Table/Schema 投影、invalidation outbox、原操作回执与 `APPLIED` 状态。Sync 的事务同时包含预期 checkpoint 比较、run receipt、checkpoint 与 `APPLIED` 状态。
4. 同 ID、同内容重放返回原结果；同 ID、不同内容拒绝。每个 Catalog 的每类工作流最多一个 `PENDING` intent，新 ID 不能绕过它。
5. 下一轮或重新创建的 Provider 从数据库读取 intent，以原 ID 和原内容恢复。并发恢复与原调用重放受同一行锁约束。
6. 旧 deployment 的 pending intent 只允许在当前 deployment 行锁内标记 `REJECTED`，不写入替换后的投影或进度。保留原 ID 与请求，禁止复用已拒绝的 ID。
7. COMMIT 或提交后的 close 结果未知使用原有 OutcomeUnknown 错误。已接受但执行未完成使用新增 Pending 错误；两者都禁止分配新 ID 绕过问题。已知数据库故障的原异常及回滚附加异常保留。

`findReceipt` / `findCompletion` 的空结果仍不是未提交的证明。恢复依赖持久化请求与锁，不依赖“空结果即可重试”的推断。`READ_COMMITTED` 显式设置在请求保存、执行与恢复事务上，使等锁后的回执重读看到已提交结果。

## Schema 与接受边界

表投影同时证明其父 Schema 存在。Upsert 在同一事务内观察父 Schema，避免增量源首先发现新 Schema 下的表时，外键错误将请求长期留在 pending；失败时父 Schema、表和 outbox 一起回滚。Delete 不创造父 Schema。

PostgreSQL Provider 在持久化接受前校验标识无 NUL 且不超过当前 schema 的 255 个 Unicode code point 限制；不额外限制领域原有的命名语义。该后端限制没有进入通用 API；补充字符按字符数而非 UTF-16 单元数计算。其他持久化执行失败保持显式 Pending，不能通过丢弃请求恢复可用性。

## 类与资源职责

| 类型 | 处置与职责 | 调用方 / 生命周期 |
|---|---|---|
| `MetadataProjectionTransactions` | 补充持久化接受和恢复契约，不增加 SDK/JDBC | Metadata Core；借用 Provider |
| `MetadataSyncRepository` | 补充 completion intent 和恢复契约 | Metadata Core；借用 Provider |
| `MetadataProjectionPendingException` | 原 operation ID 与原错误 | PostgreSQL → Core，终止本轮 |
| `MetadataSyncPendingException` | 原 run ID 与原错误 | PostgreSQL → Core，终止本轮 |
| `PostgresMetadataProjectionTransactions` | 唯一投影事务与状态转换 owner | Server 装配；借用 DataSource，拥有每条连接 |
| `ProjectionIntentJournal` | 原请求及 pending/terminal 状态的 SQL 参与者 | 仅所属事务协调者；借用连接，关闭自己的 statement/result set |
| `ProjectionReceiptCodec` | 复用版本化请求解码以恢复原操作；receipt 编码保持兼容 | Provider 内部，无业务授权与事务权力 |
| `ProjectionRequestValidation` | 当前 PostgreSQL 标识存储边界的接受前校验 | Provider 内部，无资源与状态 |
| `PostgresMetadataSyncRepository` | 唯一 checkpoint/run/intent 事务 owner | Server 装配；借用 DataSource，拥有每条连接 |
| `SyncCompletionRows` | 从原 repository 提取不可变 completion 编解码；复用于 intent 和 receipt 表 | Provider 内部枚举限定表名；不提交、回滚或关闭借用连接 |
| `SyncIntentJournal` | 同步完成 intent 的状态 SQL 参与者 | 仅所属事务协调者，不成为第二个状态机 |
| `MetadataSyncService` | 在源观察前调用恢复；传播 Pending/Unknown，保留明确支持的错误处理 | Server 周期入口与直接同步入口；不持有数据库恢复状态 |
| `ModuleIntegrationBootstrap` | 周期任务日志保留原 Throwable 与附加异常，避免只记录 Pending ID 而丢失原因 | 原有周期入口；不取得恢复状态或事务所有权 |

未新增生产 Memory Provider。测试 recorder 只表达同步内存测试行为，不宣称具有崩溃恢复或分布式保证；Connector-only fixture 显式拒绝恢复能力。服务注册、角色依赖和 Native/JNI 接口没有变化。

## 空库初始化

V109 定义 `metadata_projection_intents`，V110 定义 `metadata_sync_intents`，都由 `pangrova-schema-migrations` 唯一拥有，并进入 canonical checksum manifest。无 Catalog 外键，避免删除 deployment 时丢失未确认请求。部分唯一索引约束每个 Catalog 一个 pending 请求。历史脚本不修改；没有历史数据转换、双写、旧库兼容或清空用户数据库。

## 验证范围与结果入口

真实 PostgreSQL 故障测试覆盖 intent COMMIT 回执丢失、最终 COMMIT/close 回执丢失、恢复、冲突内容、新 ID 绕过拒绝、部署替换、并发恢复、原调用重放、恢复失败后修复、原 SQL 错误与回滚附加错误。Metadata 集成测试覆盖下一轮/重新创建服务后先恢复再读取源、恢复原游标、失败时禁止增量与全量读取，以及恢复 COMMIT 仍未知时停止读取。

Drift 场景通过真实 invalidation poller 验证恢复后的缓存失效。恢复存储请求不伪装为一次新完成的 drift 检测；该实例的成功计数不会追补。一次新的 full scan 会记录自己的观察，可能在原操作恢复事件之外产生新的失效事件，不能将两次不同观察当成原 ID 重放重复写。

实际命令、首次失败、修复及最新报告见 `projection-recovery-validation-summary.json` 和测试台账。后续结果只能来自本批实际执行，不能继承上批测试数字。Provider 重新创建与持久化故障测试不是整个 Coordinator 进程 kill/restart 的端到端证明。

## 仍须完成的能力

- 整轮扫描的 ownership/lease、取消、源版本与并发观察排序、完整删除盘点；逐操作 journal 不能代替它们。
- 两类 intent 的编排没有成为整轮 scan 的统一 durable plan；崩溃在两次操作之间可能重新读取源并生成新的观察 ID。已接受的 pending 操作会先恢复，但没有声明整轮 exactly-once。
- 已接受且持续执行失败的请求会阻止同类后续操作，直到故障修复或原 deployment 被正式替换。更完整的诊断、取消/拒绝操作与权限边界、超时策略、receipt/intent retention 仍需实现；不能静默删除 pending。
- 恢复原子写入通用 invalidation outbox；进程内 schema-evolution 事件、通知身份/顺序及相关业务订阅的完整恢复闭环仍须设计和验证。
- 每次首次接受增加一个事务；同步入口增加恢复检查。性能、存储增长和后端故障下的尾延迟尚未完成同环境基准比较。
- 全量生产回归、完整角色进程故障测试、通用 Shuffle、其他领域拆分及全平台最终验收仍继续执行，未宣称完成。

## 本批最终验证记录

- `consumers`：全 reactor `clean install`，714 项所选 owner/consumer/E2E 测试通过。
- `final-package`：最终源码全 reactor `clean package`，107 项原子性、恢复、契约、Schema、依赖与 Native activation 检查通过。
- 最新去重结果：714 通过，0 failure/error/skip；84 个所属测试源码文件都有本批实际执行报告。
- API 的依赖闭包正式静态 verify、使用当前本地 reactor 制品的 Provider 单模块正式静态 verify 均通过；最终制品静态报告另行重建留存，未把诊断报告当作完整 reactor 门禁。
- 110 个 JAR、110 个唯一模块名、853 个编译包，无审计发现的旧类、split package 或失效服务注册；7 个相关制品的 1,068 个 class 与编译输出逐字节一致。
- Coordinator/Worker/Standalone 实际 launcher classpath 为 262/183/325 项，均核对当前制品；控制面携带 V109/V110 及匹配 checksum，Worker 不带 Catalog 控制面持久化。
- Native/JNI/Proto 输入无变化，三个 Native 产物指纹一致；本地 macOS activation 已执行，没有宣称 ASan、其他平台或完整生产回归完成。

完整依赖闭包的静态 verify 仍被 Indexing API 的九个发现阻断。分析器报告中的 missing-class 诊断也已保留；“零已报告缺陷”不等于静态分析覆盖完整。逐次结果见 [验证汇总](projection-recovery-validation-summary.json)，首次失败见 [失败复核](projection-recovery-failure-review.json)。
