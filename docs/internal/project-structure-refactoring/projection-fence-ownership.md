# Metadata 投影原子事务与部署 fencing

本批基线是当前工作区 8,767 个文本文件，保留已有未提交工作。不是整轮模块重构完成声明。

## 实际调用与所有权

`CoordinatorBootstrap` 选择 `PostgresMetadataProjectionTransactions`，与 Catalog/Sync Repository
共享 Server 拥有的 metadata DataSource。`MetadataBootstrap` 仅接收契约并构造
`MetadataSyncService`。周期同步传递实际 Federation handle 的原始 descriptor。

增量、全量枚举、drift 修复和显式 push 回调提交强类型 mutation：

```
原始 CatalogDeploymentIdentity + operationId + 内容
→ 首次回执检查
→ 当前 Catalog 行锁 / incarnation、revision 验证
→ 锁后再次检查同一回执
→ 前值与 tenant impact / 投影 SQL / 失效 outbox / 回执
→ COMMIT
→ Core 本地失效与事件
```

一个原子操作只有一个事务协调者。Provider 显式使用 READ COMMITTED，保证等待部署行锁后的回执重读能看到赢家，不继承可能不适合重读语义的连接池默认隔离级别。`ProjectionRows` 借用连接，不提交、不回滚、不关闭。
Provider 每次获取和关闭自己的连接，不关闭共享 DataSource。提交或提交后关闭异常产生
`MetadataProjectionOutcomeUnknownException`；Core 查询原操作 ID 的完整内容确认回执。
无法确认时终止当前同步调用，不在该调用内生成新 ID 重试、不回退全量、不推进 checkpoint。

历史回执不表示当前授权：原 ID、原内容在 Catalog 删除/重建后仍可确认，但不能重新写入。
重放期间若原请求恰好提交、部署又被替换，部署冲突分支先再核对回执。
跨 Catalog 的全局 ID 竞争在唯一约束失败并回滚后确认赢家；不同内容拒绝，不遗留输家的投影或事件。

## 类级职责审查

| 类或契约 | 处置与职责 | 状态/资源 owner | 调用与证明 |
|---|---|---|---|
| `MetadataSyncScope` | 删除旧 FQN；移到 Metadata API 的 `CatalogDeploymentIdentity`，名称/incarnation/revision 不变 | 不可变值，无连接 | Completion、Repository、Sync Core、PG、测试全部更新；无兼容壳 |
| `CatalogDescriptor` | 新增 `identity()` 提取完整身份 | descriptor 本身，无新状态 | 原始 handle descriptor → mutation/checkpoint |
| `MetadataProjectionMutation` | sealed UpsertTable/DeleteTable/ObserveSchemas；目标校验、规范化 schema 集合、完整幂等内容 | 不可变请求；ID 由调用者保留 | Core 四条路径、真实 PG 契约测试 |
| `MetadataProjectionReceipt` | 请求、是否改变、锁下前值、tenant impact | PostgreSQL durable receipt | 重放、未知提交确认、schema evolution |
| `MetadataProjectionTransactions` | 唯一投影写端口及回执查询 | 实现承担事务，不把 SQL callback 暴露给 Core | Server 显式装配，Core 只依赖 API |
| 三个 Projection error | 区分部署/幂等冲突、提交未知、来源目标不匹配 | 无额外状态；未知异常保留原 ID/cause | 终止重试/回退；故障测试 |
| `TableProjectionRepository` | 删除 save/delete，保留读和既有 search | 无写 authority | drift/组件读取；search 尚无生产入口 |
| `SchemaProjectionRepository` | 删除 save，提供排序的只读 observations | 无写 authority | 真实 PG SQL 测试；当前无生产调用方 |
| `PostgresMetadataProjectionTransactions` | 唯一 mutation/rollback/commit/receipt coordinator | 每操作 Connection；借用 DataSource | 真实 PostgreSQL 并发、回滚、回执测试 |
| `ProjectionRows` | 从旧写 Repository 提取 SQL 参与者，包私有 | 借用 Connection；只关闭创建的 statement/result set | 原 SQL/时间精度保留；新事务测试 |
| `ProjectionReceiptCodec` | 包私有 versioned JSON；拒绝版本溢出、错误类型及损坏内容 | 无资源；SDK 不泄漏 | codec 测试及数据库 roundtrip |
| 两个 Postgres projection reader | 写方法移除，保留/实现读取 | 每次查询自己的 Connection；借用 DataSource | 全量读取/失效/回滚测试 |
| Sync Completion/Repository/PG | 仅统一身份值所属制品，不改 checkpoint 协议 | 原子同步完成 owner 不变 | 同步历史与未知提交回归 |
| `MetadataSyncService` | 改借用原子端口；scope 贯穿所有投影路径；前值/tenant 从 receipt 获取 | 同步局部统计；借用端口/cache/dispatcher | 原三项失败回归、drift/push/未知提交测试 |
| `MetadataBootstrap` | 传递原子端口，删除原 schema writer 参数 | 不创建/关闭 backend | 角色/Connector bootstrap 测试 |
| `CoordinatorBootstrap` | 创建 Provider 注入 MetadataBootstrap | 既有共享 DataSource 生命周期 | 实际角色装配、Native activation |
| `CurrentCatalogProjectionFixture` | 多模块测试数据准备，组合部署读取和真实 mutation port | 纯测试基础设施，借用端口 | 各消费方 fixtures；不能用于 stale-handle authority 测试 |

`org.pangrova.table.api.identity.CatalogDeploymentIdentity` 是已有的两字段 Table authority
observation（不含 catalog name）。本批未改该模型；Metadata 的完整 scope 不能与它混用。
该跨领域身份模型的统一仍需后续明确 Table API 依赖方向，不能只按同名盲目合并。

## 已复现的缺陷与修正

- 旧部署在最终 sync completion 校验前已经写入投影：改为每次 mutation 锁下验证。
- 来源绑定 A 却删除 B：批次预检与请求构造共同拒绝 foreign target。
- 旧 push 删除先读取已经不存在的源表：DELETE 直接提交条件删除；其他事件取当前源内容后检查精确目标。
- 提交未知可能触发新同步和重复写：通过原 ID/内容确认，确认失败不重试。
- 原投影测试使用纳秒时间与 PostgreSQL 微秒 timestamp 列比较，误报 foreign-delete 防护失败：改为比较实际写入前后持久值；不重写时间存储语义。

## 范围与未完成项

- V108 仅加入统一初始化资源和 checksum manifest，不创建另一套 schema，不提供历史数据迁移。
- 同一 Catalog 的 mutation 经部署行锁串行；不同 Catalog 保持独立锁，但仍遵守现有 outbox commit-order gate。
- **待完成的真实调度缺口**：Server `ModuleIntegrationBootstrap` 的周期任务会捕获同步异常后在下一轮重新调用；目前没有跨轮/重启的 durable pending-operation barrier。本批证明的是单次调用与原 ID 重放语义，不能宣称周期恢复已安全解决提交未知。后续必须持久化原操作身份并在恢复后先确认，不能用内存 Map 或吞异常替代。
- 本批是逐操作事务；不是整轮扫描的发布事务。相同部署下并发同步、source-version 条件写、完整删除盘点、scan lease/取消、receipt retention 仍未完成。
- `ObserveSchemas` 是观察到的 upsert 集合；空集合不删除任何 schema。
- 原始外部表 timestamp 的 PostgreSQL 列精度不变。操作 JSON 保存原请求精度，支持同 ID 的准确内容确认。
- Push 回调接口有真实组件测试，但生产 Federation 的订阅能力声明、注册/撤销与生命周期尚未接通，不能宣称已交付生产 push 功能。
- 事件的部署身份/顺序及 durable schema evolution delivery 仍须继续审查；当前 per-table outbox 与本地事件不是同一跨进程业务事件协议。
- 测试 fixture 用当前 descriptor 准备数据是测试行为。生产同步必须使用原 handle scope，不能通过刷新 current identity 绕过 fencing。
- 没有新增生产 Memory、数据库产品、Celeborn 模块或微服务。

实际命令、首次失败、最新测试及发行结果见本批 validation/artifact 证据；不能继承前一批通过结果。
