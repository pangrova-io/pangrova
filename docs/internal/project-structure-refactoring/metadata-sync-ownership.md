# Metadata 同步进度与诊断持久化拆分

本批以 `/tmp/pangrova-metadata-sync-before` 的工作区快照为基线，延续全仓模块化计划。它完成同步进度这一能力闭包，不代表 Metadata 全域或全仓重构完成。

## 已证实的问题

旧 `PostgresMetadataStore.getLastSyncTime` 读取 `MAX(sync_history.sync_time)`，而 `updateSyncTime` 不执行任何写入。服务即使只有部分变更成功，也会写同步历史；下一轮因此可能越过失败项。原测试检查 Memory 替身中的 setter 标志，没有验证真实 SQL 行为。

在修改实现前添加的两个 PostgreSQL 回归分别证明：失败诊断被误读为进度、显式进度写入不能在重新创建仓库后读取。两项均失败，证据保留在本批 validation 记录中。随后将其行为断言迁到独立同步仓库测试，没有保留旧接口兼容壳。

## 所有权与真实调用

| 能力 | Owner | 消费者与装配 |
|---|---|---|
| 来源变更和可选完整覆盖边界 | Metadata API 的 `MetadataChangeBatch` | `FederatedCatalog.readChangesSince` 及缓存装饰器；没有来源保证时显式返回无边界 |
| 部署身份、进度、完成请求与冲突 | Catalog Persistence API 的 `synchronization` | Metadata Core |
| 诊断、进度比较、部署 fencing 和完成回执的单事务 | `PostgresMetadataSyncRepository` | Server Coordinator 使用现有 metadata DataSource 显式创建，与投影仓库共享同一数据库 |
| 应用变更和重试决策 | `MetadataSyncService` | 周期同步从实际 Federation handle 传递 descriptor 和 adapter |
| 数据库初始化 | `pangrova-schema-migrations` 的 V107 | 统一空库初始化及测试，未添加 Store 自建 schema |

组合 `MetadataStore` 和 `PostgresMetadataStore` 不再拥有进度及诊断方法。外部 Federation 插件须重新编译并实现 `readChangesSince`，明确返回来源覆盖边界或无边界；不保留旧方法转发壳，也不将“无边界”解释成具备可靠 CDC 能力。没有增加 Production Memory 实现。纯 Connector 装配测试使用显式拒绝同步的测试替身；真实 PostgreSQL fixture 使用其原有同一个 DataSource。

## 保证

- 进度按 catalog name、incarnation、revision 隔离，删除重建或更换配置不会继承旧进度。
- 同步来源必须对 `(旧游标, completeThrough]` 的完整性负责，包含删除且不能再有延迟提交落入已覆盖区间。查询为空、最新事件时间、数据库当前时间和本地结束时间都不是这一保证。
- 没有覆盖证明时允许盘点和写投影，但不推进增量进度；完整枚举本身也不产生变更日志游标。测试 PostgreSQL inventory 的 `synced_at` 在 COMMIT 前赋值且没有删除日志，因此明确不声明覆盖能力。
- 有明确覆盖证明的空批次可以推进；部分失败保留原进度，下轮从原位置重放。
- PostgreSQL 在当前部署行上加锁，将 expected checkpoint 比较、run 回执和下一 checkpoint 放在同一事务。游标按 Instant 的完整文本保存，不经 PostgreSQL timestamp 精度截断。
- 相同 run ID、相同内容重放成功，即使后来进度已推进；同 ID 不同内容拒绝。完成回执可以在旧部署被删除后用于确认已完成操作。
- COMMIT 确认丢失或提交后 close 失败以明确的 outcome-unknown 异常报告。Core 先查原 run 回执；不能确认则不执行本次调用内的增量重试或全量回退。
- 部署和 checkpoint 冲突终止本次重试。重试入口不把包含失败项的 SyncResult 当成成功。
- DataSource 由装配者持有，仓库只关闭自己取得的 Connection。回滚失败附加到主异常，不替换它。

## 边界与未完成项

本批 fencing 保护**同步进度和完成记录事务**，并未把整个外部扫描、逐表投影写入和 schema 盘点纳入这个事务。多 Coordinator 同时盘点、扫描期间部署更换导致的投影覆盖，以及投影与来源版本的条件写仍需下一批 `MetadataProjectionTransactions` 解决，不能由 checkpoint CAS 推导出它们已经安全。

部署、表投影、schema 投影仍部分聚合在原 Store；其拆分和版本化投影事务继续实施。`FederatedCatalog` 仍在 Core，后续需要迁入合并的 API/SPI 制品及实际 Federation Adapter。未新增真实外部 Federation Provider，也不以测试 inventory 证明外部系统 CDC 完整性。

全量盘点目前仍不保证发现外部删除；没有把它宣传成删除修复或带快照的完整同步。周期调度的领导权/租约、取消、长任务并发和持久化历史保留仍待整体同步生命周期重构。Metadata API 中原来的其他弱默认方法没有被本批一并声明为修复。

旧 V1 的 `sync_history` schema 定义保留于历史初始化序列，不再有 Java 运行时读写；没有双写、旧库映射、数据回填、数据迁移工具或用户环境清空。

## 验证索引

本批 validation summary、逐用例记录、静态检查、制品和逐文件处置台账使用统一 `metadata-sync-` 前缀。首次失败与后续修正分别保留；只有本批实际运行的检查属于本批证据。Native/Proto/JNI 输入未修改，不据此宣称性能提升或新增 Native 能力。
