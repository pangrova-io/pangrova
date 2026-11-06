# Catalog 部署、表投影与 schema 投影端口

本批继续执行统一模块化方案，从本批工作区快照建立新基线，不继承上一批测试结论。当前组合 `MetadataStore` 和 `PostgresMetadataStore` 已删除，没有兼容类、组合接口继承或生产转发 Facade。

| 能力 | 契约 | 实现 | 当前生产调用 |
|---|---|---|---|
| 当前部署、精确历史 revision、发布和删除 | `CatalogDeploymentRepository` | `PostgresCatalogDeploymentRepository` | Registry、同步的当前租户影响范围解析、Server/Coordinator 装配 |
| 表投影读写和删除 | `TableProjectionRepository` | `PostgresTableProjectionRepository` | MetadataSyncService 的变更应用与漂移检查 |
| 观察到的 schema 批量 upsert | `SchemaProjectionRepository` | `PostgresSchemaProjectionRepository` | MetadataSyncService 的完整枚举入口 |
| 进度及完成回执 | `MetadataSyncRepository` | `PostgresMetadataSyncRepository` | 保留上一批独立同步状态 owner |

Server 在其已有 DataSource 上显式创建各 Provider，MetadataBootstrap 传入相应端口；没有通过大 Context 隐藏依赖，也没有把 Provider 创建移入 Metadata Core。三种投影/部署 Provider 只持有借用的 DataSource，每次调用自行取得并关闭 Connection，不关闭共享连接池。

41 个部署专用测试替身不再实现投影接口，205 个与测试目的无关的方法已经删除。同步单测的两个 Memory fixture 明确实现其实际使用的三个端口，仍仅存在于测试源码。Connector-only 装配测试通过显式拒绝的测试端口发现意外投影调用；真实数据库与生产装配使用真实 Provider。

## SQL 与事务

- 原部署 current/history/revision fence、表读写、schema batch、outbox SQL 和映射方法按能力迁入各自 Provider，保留事务顺序和逐操作原子性。33 个旧方法中 28 个方法体完全相同，两个方法只将固定 SQL 局部变量标为 final（编译期常量），另三个处置为构造器拆分、无调用 helper 删除、rollback 异常保留修复。固定 SQL 内容和参数绑定未改动。
- PostgreSQL 内部 `PostgresCatalogPersistenceSupport` 统一连接、提交、回滚和 JSON 列处理。manifest 显式登记五个包根，仅导出 catalog、projection、invalidation、synchronization；transaction 包不是消费者契约。
- 旧的、已没有调用方的 auto-commit `executeUpdate(String, binder)` helper 删除。保留的 mutation callback 只在 PostgreSQL Provider 制品内部使用，不暴露给业务 Core。
- 结构迁移先由原有测试验证，再单独补充真实 PostgreSQL 回归证明 cleanup 缺陷：outbox 写入失败后，RuntimeException/Error 型 rollback 异常原先会替换主 SQL 异常。两项修复前失败、修复后通过；清理现在保留主异常并附加次异常，避免自抑制。
- 支持类构造器对借用的 DataSource 加非空检查；不创建连接池、不选择默认后端、不静默切换 Memory。
- schema 初始化资源、数据库事实和用户数据没有改动；本批不建设数据迁移。

## 命名、包与装配

`pangrova-persistence-catalog-api` 继续是唯一 Catalog 持久化契约制品。部署契约在 `.api.catalog`，表/schema 契约在 `.api.projection`，同步契约在 `.api.synchronization`。PostgreSQL 实现有对应能力包，事务支持包仅为内部技术复用。

部署集成测试重命名并移到 `.postgresql.catalog.PostgresCatalogDeploymentRepositoryTest`；投影/失效原子性测试重命名为 `PostgresProjectionInvalidationIntegrationTest`。部署专用 fake 的类名也不再暗示具有完整 Metadata Store 能力。测试移动前后的用例身份在证据汇总中归一化，初次失败仍单独保留。

## 尚未完成

本批没有把外部扫描、所有表投影写入、来源版本比较和完整 inventory 发布变成一个原子操作。现有端口如实保留“逐操作 projection + outbox”的能力，不宣称具备整个扫描的隔离。

下一步仍需实现强类型投影原子事务端口，处理扫描期间部署更换、多个 Coordinator 覆盖、来源版本条件写、缺失对象删除与重试/提交确认。同步的 lease、取消与历史保留也仍待实施。

现有 `searchTables` 字面子串查询原语仍有实现，但没有生产调用入口；它不是全文检索或 SQL 搜索支持的证明。后续能力审计必须明确接入的实际需求或退役理由，不能凭接口存在宣称完成。

Federation SPI、Graph Provider 和 Coordinator 元数据装配尚有待迁移部分。其他领域、通用 Shuffle、全量生产回归和性能证明不因本批通过而被标记为完成。

## 静态分析与边界检查

CatalogRegistry、MetadataSyncService 和 PostgreSQL 内部支持类的六条借用依赖身份分类已迁移到准确的新类、字段与构造器签名。使用仓库锁定的 SpotBugs Filter API 验证，每条规则拒绝另外九种 owner/method/signature/field/type/bug 变化；未增加包级或任意方法豁免。

全依赖 verify 的首次静态分析在 Types 发现 BytesValue 的数组所有权缺陷（构造器与 getter 直接暴露数组）。该输入与本批基线相同，保留为后续必须修复的问题，不把选定 Metadata 测试通过解释为全量静态验证通过。诊断命令曾因缺少已安装的 reactor 制品、未指定所需 Python 版本失败；改为在同一 reactor 先 package 并指定项目要求的 Python 后生成报告。所有失败记录保留。
