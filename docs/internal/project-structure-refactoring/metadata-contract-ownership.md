# Metadata 契约、Catalog 投影 Provider 分离

本批以当前工作区为基线，保留已有全部改动；不是全仓模块化完成声明。未创建历史数据迁移或兼容 FQN。

## 实际制品与边界

- `pangrova-metadata` 改为聚合 POM；原业务与跨域适配代码进入 `pangrova-metadata-core`。
- `pangrova-metadata-api` 同时拥有 `.metadata.api` 与 `.metadata.spi`。52 个原生产契约/值/错误进入这个制品；调用契约依赖 Common、Connector API、Table API，不依赖 Core、数据库、Jackson、Iceberg 或 Server。
- `pangrova-metadata-codec` 拥有原表元数据序列化和 canonical JSON 适配。缓存和 PostgreSQL 都实际复用；独立制品依据是共享编码实现，避免 Provider 依赖 Core。时间模块仍显式配置，未改变 JSON 格式。
- `pangrova-persistence-catalog-api` 拥有投影/同步持久化端口、部署历史端口、对象依赖图端口、失效日志清理端口与存储错误。
- `pangrova-persistence-catalog-postgresql` 拥有现有投影/部署 SQL 和失效日志消费者。`PostgresMetadataStore`、`PostgresInvalidationPoller` 是原实现的唯一生产归属，没有旧类转发壳。它不依赖 Metadata Core。

## 行为与资源

`CatalogRegistry` 仅接收 `CatalogDeploymentRepository`。不可变部署读取必须由实现方定义，接口不再默认返回空；空结果仅表示精确历史不存在。现有 PostgreSQL 精确匹配 incarnation/revision；无历史能力的测试替身明确拒绝，没有把测试 fake 包装成生产 Memory Provider。

`MetadataCleanupScheduler` 只借用 `InvalidationLogMaintenance`，创建并关闭自己的定时器。Server 创建真实 PostgreSQL poller 并负责关闭。Poller 借用 DataSource/MetadataCache，拥有其轮询 executor。清理端口不授予连接或 poller 的关闭权；零计数不是追平/就绪证明。现有 best-effort 清理失败策略未改成写入或 ready 成功的证据。

投影写入、Catalog current/history 写入与 durable invalidation 仍由同一个 SQL 事务提交；未拆成“先提交投影，再写事件”。现有锁、commit-order trigger、清理 watermark 和事务异常处理不在本批改写。主导编排与 SQL 分域尚未全部完成。

IndexAlreadyExistsException 回归 Indexing API，继承通用 PangrovaException；原错误码、消息、SQL 冲突检测和显式捕获分支保持一致。Indexing Core 仍通过全限定静态调用使用 TableControlDeferredPolicy，在同一 JDBC 事务中校验当前表/tag 权限，因此本批保留其 Metadata Core 生产依赖，待完整原子 workflow 提取后删除；不能绕过权限检查来消除依赖。

Provider 自身的投影原子性/部署历史和失效轮询测试已进入 PostgreSQL 制品，使用 Testing Kit 的 MigratedPostgres 与唯一 schema bundle。Metadata Core 仅保留需要领域编排的集成测试；没有让 Provider 的测试反向依赖 Core。Semantic 的测试也直接依赖 schema bundle，不再借 Metadata Core 间接带入 SQL 资源。

API 隔离测试在只有契约闭包的独立 ClassLoader 中加载所有类，读取方法、构造器、字段、泛型、注解和 record 组件，同时拒绝 Core、Provider、codec 和 SDK。仅依赖编译成功不足以证明边界。

## 明确剩余工作

- `MetadataStore` 仍组合投影/Schema/sync 与部署端口，供现有同步和装配使用；继续拆为能力端口及原子 projection workflow，删除组合接口。
- 对象依赖图的 PostgreSQL/Memory 实现和共享图遍历仍在 Core，尚未迁完。其当前身份不含 tenant/incarnation，不能当作跨租户 SQL DROP 权威。
- Catalog Registry 仍创建 Table Memory 默认实现；需连同 Table authority 的实际装配一起移除，不能仅改名字。
- MetadataService 中治理 tag 方法与事件仍需回归 Governance；本批契约搬迁不把默认空结果升级为真实能力。
- Generation/managed source 跨域 JDBC workflow、连接池、迁移 runner 与若干后端装配仍在 Core，Core 全后端隔离尚未达标。
- Connector/Federation adapter 独立制品、Core 能力包归整和职责拆分未在本批全部完成。
- 无新增 Celeborn Provider，也没有以本批测试证明通用 Shuffle 或全量生产回归完成。

逐文件台账区分纯移动/契约变化/消费者更新。未逐条语义复核的原 Core 类保留为路径移动，不用扫描结果冒充全面职责审核。


## 本批失败诊断与修正

首次抽取把 ObjectRef 文档对 ObjectDependencyStore 的同包引用转成了 Java import，形成领域 API → Persistence API → 领域 API 的环；改为纯文字描述并移除反向依赖。Codec 显式声明自身使用的 JavaTimeModule。历史查询改为强制实现后，补齐所有无历史能力的测试替身的显式拒绝。

架构门禁发现 Provider 测试仍在 Core，以及原测试专用包/夹具归属没有完整登记。已将 Provider 自身测试迁入 Provider，保留精确的 Core 测试专用包，不放宽包所有权或允许 split package。空包已删除。

对 Indexing Core 只看 import 会漏掉全限定静态调用；编译发现当前授权仍依赖 TableControlDeferredPolicy。实际保留这条同连接授权依赖，未删减任何权限校验；索引错误本身已归回 Indexing API。

Semantic 移除不必要的 Metadata 测试依赖后，暴露 JsonMapperFactory 的 JavaTimeModule 运行时缺失。现由 Semantic 显式声明该运行时依赖，空库 SQL 资源由 schema bundle 直接提供。原 2 个错误保留，独立修正验证 5 项通过。

第一次制品扫描把已迁走的 MetadataEvent 前缀与仍合法存在的 MetadataEventDispatcher 混淆，7 处报告均为同一前缀误匹配。改为完整类型符号边界检查；未豁免任何旧类，最终 110 JAR 无旧类型引用、重复注册或 split package。最初扫描报告与最终结果均保留。

这些是实施过程中真实发现并处理的问题；本批不把既有 Shuffle 未接线门禁或全量生产回归标为通过。
