# Pangrova 全仓模块、API/SPI、存储与运行时重构实施计划

状态：执行中。本计划是当前唯一有效的全仓重构实施计划，替代此前各版计划；
阶段编号 P0–P12 以本文为准。实施证据继续记录在
[领域模块化实施记录](domain-modularization-progress.md)，目标决策见
[领域模块化 ADR](../../architecture/domain-modularization-adr.md)。

## 1. 目标与已确定的边界

本计划覆盖全仓可替换能力，最终形成：

```text
基础类型
  ↓
领域 API / 扩展 SPI / 持久化端口 / 传输协议
  ↓
领域 Core
  ↓
独立 Provider 与基础设施适配器
  ↓
Coordinator / Worker / Standalone 装配与发行
```

实施遵循以下已确定决策：

- **全仓分阶段推进**，每批必须可构建、可验证，不能用最终一次大合并掩盖中间失效。
- **Java 包名与模块直接切换**，同步修改仓库内全部调用方、测试、资源注册和配置引用，不保留旧包转发壳。
- **保留已有数据与外部行为**。SQL、HTTP/JDBC 客户端行为、既有协议语义、持久化编码及历史 migration 保持兼容。
- **采用停机升级**，本轮不承诺新旧 Java 插件或新旧节点混跑。
- **只重构和验证已有后端**，不新增 MySQL、远程元数据服务或其他数据库产品。
- **不改变 Java/C++ 执行分工**。Coordinator 保持控制面，Worker 经 JNI 调用 Native；C++ 仍以现有目标组合为一个共享库。
- **不改变部署拓扑**。不因模块拆分增加微服务；现有可选 Shuffle Service 保持独立部署能力。
- 保留当前工作区已有改动。实施从工作区事实建立基线，不能把旧文档中的模块数或历史测试结果当成当前证据。

最终验收必须同时满足：

1. 公共契约可以在不包含 Provider、进程实现和非必要 SDK 的 classpath 上加载。
2. 领域 Core 不依赖 JDBC、Memory、Redis、云 SDK 等可替换实现。
3. Coordinator 不依赖 Worker Core 或 Native Bridge。
4. 每个持久化事实、事务操作和资源生命周期都有唯一所有者。
5. 每项现有可替换能力都有契约、实现模块、装配入口和有效测试。
6. 全量回归、相关 Java/native 集成链与升级演练通过。

## 2. 目标模块、包与依赖规则

### 2.1 API/SPI 采用领域归属

不再把所有共享类型放入一个 `pangrova-spi` JAR。迁移完成后删除其代码制品及调用依赖，相关契约进入各领域家族。

固定四类契约：

| 类别 | 职责 | 包结构 |
|---|---|---|
| 领域 API | 调用领域服务、共享领域值和错误 | `org.pangrova.<domain>.api.<capability>` |
| 扩展 SPI | 工厂、Provider、策略扩展点 | `org.pangrova.<domain>.spi.<capability>` |
| 持久化端口 | 状态读取、原子变更和一致性保证 | `org.pangrova.persistence.<domain>.api.<capability>` |
| 传输协议 | RPC、结果与 JNI 的线格式 | 保持 Protocol 明确所有权 |

具体规则：

- `QueryId` 等值归领域 API；`ConnectorFactory` 等实现扩展点归 SPI。
- DTO、enum、record 可以属于 API，不因没有方法而进入 Common。
- 不为每个内部接口建立 SPI。领域内部协作接口留在 Core。
- 必须被外部实现的接口不能留在 Core。
- API 与 SPI 都不得暴露 Provider、数据库连接、SDK 客户端或进程实现类型。
- 跨领域共享必须依赖对方 API，禁止依赖对方内部包。
- 同一 Java package 只能由一个制品拥有。
- Core 内部使用领域能力包，不再统一增加无实际隔离价值的 `impl` 大包。

### 2.2 领域模块目录

采用以下目标制品；已有符合边界的模块沿用，不重复创建：

| 家族 | 目标子模块与职责 |
|---|---|
| Common | `common` 保留基础工具；`common-api` 承载 API 必需的最小基础值、注解与错误 |
| Connector | `connector-spi`、`connector-runtime`、`connector-iceberg/paimon/lance` |
| Function | `function-spi`；已有函数实现按实际执行侧归属 |
| Query | `query-api`、`query-core` |
| Metadata | `metadata-api`、`metadata-core`（含 connector/federation 句柄） |
| Table | `table-api`、`table-core` |
| Generation | `generation-api`、`generation-core`、`generation-codec` |
| Materialization | `materialization-api`、`materialization-spi`、`materialization-core`、`materialization-strategies-builtin` |
| Scheduler | `scheduler-api`、`scheduler-core`、`scheduler-strategies-builtin` |
| Coordination | `coordination-api`、`coordination-core`、`coordination-zookeeper` |
| Security | `security-api`、`security-spi`、`security-core`、现有认证/授权/Secret/KMS 实现模块 |
| AI | `ai-api`、`ai-spi`、`ai-control`、`ai-worker`、`ai-provider-openai` |
| Indexing | `indexing-api`、`indexing-spi`、`indexing-core`（含 planner 向量重写）、`indexing-strategies-builtin` |
| Governance | `governance-api`、`governance-spi`、`governance-core`、已有执行器/血缘 Provider 模块 |
| Streaming | `streaming-api`、`streaming-spi`、`streaming-core`、现有 source/format/writer 实现模块 |
| Semantic | `semantic-api`、`semantic-core` |
| File | `file-api`、`file-core` |
| Filesystem | 保留 `filesystem-api/local/s3/oss` |
| Cache | `cache-api`、`cache-memory`、`cache-redis` |
| Observability | `observability-api`、`observability-noop`、`observability-otel` |
| Shuffle | `shuffle-api`、`shuffle-core`、`shuffle-local`、`shuffle-remote`、现有 `shuffle-service` |
| Transport | `transport-api`、`transport-http`、`transport-grpc`、`transport-flight`、`transport-client` |
| Worker | `worker-api`、`worker-core`、现有 `worker-native-bridge` |
| Server | `server-bootstrap`、`server-coordinator`、`server-worker` |
| Standalone | 组合两种角色的启动与发行，不复制初始化逻辑 |

执行粒度约束：

- 相同生命周期、相同依赖、共同交付的算法实现进入一个 `*-builtin` 模块，不是一种算法一个 JAR。
- 引入独立 SDK、Native 依赖或可独立启停的实现必须独立模块。
- 不创建没有生产实现或调用者的空模块。
- 装饰器留在对应领域 Core；厂商/技术后端进入 Provider。例如治理包装器与 OpenAI Provider 不作为互斥后端。
- Parser、Planner、Operations、Client 保留现有主要职责，仅迁出泄漏的契约与适配代码。
- Protocol 本轮不改字段和 wire 编码；其中实际属于网络服务实现的代码迁到 Transport，契约类型不随意重命名。

### 2.3 Persistence 采用“按领域端口、按领域与后端实现”

统一放在 `pangrova-persistence` 家族：

```text
pangrova-persistence-<domain>-api
pangrova-persistence-<domain>-postgresql
pangrova-persistence-<domain>-memory

pangrova-persistence-jdbc-api
pangrova-persistence-jdbc-support
pangrova-persistence-workflow-api
pangrova-persistence-workflow-postgresql
pangrova-persistence-contract-tests
```

领域覆盖现有持久化事实：Catalog、Table、Query、Security、Generation、Coordination、File、Materialization、Indexing、AI、Governance、Streaming、Semantic。

只为已存在后端创建实现模块，不要求每个领域都有 Memory 实现。

职责约束：

- 领域 API 拥有业务值；Persistence API 拥有存储端口及存储结果类型。
- PostgreSQL Provider 拥有 SQL、行映射、锁和事务实现。
- `jdbc-api` 仅承载明确的同连接协作接口，允许暴露 `Connection`。
- `jdbc-support` 只承载技术工具，不包含领域 SQL。
- `workflow-api` 承载必须跨领域原子完成的操作。
- `workflow-postgresql` 是这些操作的事务协调者，组合各领域 PostgreSQL 参与者。
- 参与者不得提交、回滚、关闭或替换调用者连接。
- 普通领域 Core 不依赖 `jdbc-api`；后端协作适配器与事务实现可以依赖。
- Backend 需要共享 canonical codec 时依赖独立 codec 模块，不反向依赖领域 Core。
- `pangrova-schema-migrations` 继续唯一拥有历史 PostgreSQL migration；不按领域复制历史 SQL。

## 3. 关键接口与数据一致性设计

### 3.1 Provider 发现与装配

在运行时基础设施中实现工厂注册与生命周期管理，业务 Core 不使用全局 Service Locator。

工厂契约包含：

- 唯一 Provider ID；
- API 兼容版本；
- Coordinator/Worker 适用角色；
- 配置校验；
- 能力声明；
- 创建与关闭规则。

各能力保留强类型 Factory，例如 Connector、Filesystem、Cache、Inference，禁止用统一 `Object create(Map)` 代替领域契约。

装配规则：

1. ServiceLoader 负责发现工厂。
2. 配置明确选择 Provider。
3. 重复 ID、不兼容版本、缺失能力均在 ready 前失败。
4. 创建后通过构造器注入领域服务。
5. 工厂创建的资源由该 Provider 所属生命周期关闭。
6. 不支持热卸载；插件升级随进程重启。
7. 先消除签名泄漏，再缩减插件共享包；禁止直接切换类加载策略后靠共享更多包补洞。

### 3.2 Metadata 与 Table 分工

Metadata API 提供 Catalog 注册、解析、部署版本读取和元数据查询。

Metadata Core 负责：

- Catalog 生命周期与路由；
- 外部元数据投影；
- 同步与缓存失效编排；
- Connector/Federation 适配组合。

Table Core 负责 Pangrova 自有表控制、ownership、维护策略、统计和治理绑定。

外部表 Schema/Snapshot/提交事实由 Connector 所连接的外部 Catalog 负责；本地投影不能替代其权威。

拆分现有 `MetadataStore`：

- `CatalogDeploymentRepository`：Catalog 当前状态及不可变部署 revision。
- `TableProjectionRepository`：外部表本地投影。
- `SchemaProjectionRepository`：Schema 本地投影。
- `MetadataSyncRepository`：同步位置与历史。
- `MetadataProjectionTransactions`：原子提交投影变更与失效事件。

失效 outbox 的写入不暴露为可被业务绕过的独立步骤。消费端可以单独读取、确认和重试。

不可变 revision 查询必须区分“不存在”和“后端不支持”。删除默认返回空值的兼容行为，由全部实现明确实现。

### 3.3 原子操作与 JDBC 协作

优先保留既有事务保证，再提升后端可替换性。

先把现有 JDBC 专用协作接口迁入 `persistence-jdbc-api`，包括：

- Generation 提交、当前权限、输入读取与执行 fence；
- Query 执行 fence；
- 当前 Role/Grant/Policy 读取；
- 当前 Model authority。

随后由 `workflow-api` 提供业务级原子操作：

| 操作 | 必须同时保证的事实 |
|---|---|
| Generation 提交 | 当前权限、不可变任务输入、依赖、页集合、领域关联 |
| Catalog export 提交 | Generation job 与 AI export 关联 |
| Generated value 提交 | Generation job 与 descriptor 关联 |
| Generation 发布 | 当前执行/发布所有权、目标状态、发布记录 |
| 元数据投影提交 | 投影变更与失效 outbox |
| Query 写入恢复操作 | 原有 journal、fence、quarantine 的原子边界 |

接口使用强类型 Command/Result，禁止：

- SQL 字符串；
- 任意 JDBC callback；
- 可 `unwrap` 成数据库连接的通用事务对象；
- `Map<String,Object>` 形式的领域载荷。

状态结果区分成功、已完成的幂等重放、版本冲突、权限拒绝、所有权丢失及提交结果未知。现有幂等键与编码原样保留；不存在幂等键的路径不伪造重试安全性。

提交结果未知时先按既有操作身份确认状态，不盲目重试，也不删除可能已经发布的数据。

锁顺序、锁后数据库时间检查和当前权限检查继续以既有安全语义为准，迁移不能将它们替换为缓存快照。

### 3.4 Tenant 数据模型收敛

采用 canonical migration 和当前生产装配使用的复数表作为目标：

```text
tenants
tenant_user_memberships
```

实施决策：

- 以 Metadata 当前 Tenant 实现的生产语义为基准，迁入 Security PostgreSQL Provider。
- 删除生产装配中的重复 Tenant 后端选择。
- 单数表实现不保留为运行时备选，不新增两套表的长期双写。
- 迁移测试与后端测试全部使用 canonical migration，停止通过另一套 `initSchema()` 建立平行模型。
- 运行时 schema 初始化迁入唯一迁移入口；需要补齐的列使用新增 migration，不改历史文件。

对可能存在的单数表历史数据，提供离线迁移工具：

1. 默认只读检查，输出数量、字段映射、主键及成员关系冲突。
2. 将 `tenant`、`tenant_quota`、`tenant_property`、`tenant_membership` 归一化为目标模型。
3. `max_concurrent` 映射到 `max_concurrent_queries`，其余配额和属性按语义映射。
4. 目标缺失记录才插入；同键语义完全相同时视为已迁移。
5. 同键内容不同或同一用户存在冲突租户关系时终止，不按时间戳猜测优先级。
6. 非空且无法映射的字段终止迁移，不丢弃。
7. 校验成功后提交事务，记录迁移结果；源表保留，本轮不自动删除。
8. 应用仅在迁移完成或确认不存在该历史数据后启动。

### 3.5 Coordinator/Worker 组合与生命周期

职责分配：

```text
Server Coordinator
  → Query Core
  → Planner / Scheduler
  → Metadata / Table / Security 等服务
  → RemoteTaskClient

Server Worker
  → Worker Core
  → Native Bridge
  → Worker 侧 Connector / AI / Filesystem

Standalone
  → 两种角色的相同装配入口
```

`coordination-core` 最终只负责成员、租约、leader 与协调编排；原有查询编排迁入 Query Core。

共享 Transport Server 从 Worker 迁出，Coordinator 通过传输契约使用它。Worker API 不暴露 Native 句柄。

统一启动阶段：

```text
配置校验 → Provider 创建 → 领域服务构造 → 恢复
→ 网络入口就绪 → 注册可服务状态
```

网络端口可以因恢复流程需要提前绑定，但 ready 和对外接入必须保持关闭。

统一关闭阶段：

```text
停止接入 → 停止新调度 → drain/终止任务
→ 注销服务 → 关闭领域服务 → 关闭 Provider
→ 关闭传输与基础资源
```

部分启动失败按实际已获取资源逆序回收；共享资源只关闭一次。

## 4. 分批实施与每批验收

每批包含生产源码、测试、POM、服务描述符、manifest、文档和资源引用的完整修改。禁止只迁生产类、将装配与测试留到最后。

| 批次 | 工作内容 | 依赖 | 验收条件 |
|---|---|---|---|
| P0 基线与规则 | 固定工作区基线，建立契约/实现/调用/装配/测试台账，统一 ADR | 无 | 新旧架构要求无冲突；现有失败单独记录 |
| P1 装配分离 | 建立 Server 模块，迁移 bootstrap/HTTP 入口，提取共享传输 | P0 | Coordinator 无 Worker Core 依赖；三种启动模式可用 |
| P2 契约分域 | 拆 SPI/API、最小 Common API、更新全部消费者与类加载策略清单 | P1 | API 最小 classpath 测试通过；无旧类和 split package |
| P3 Persistence 基础 | 拆领域端口、JDBC 协作、support、contract-tests | P2 | 通用端口不暴露 JDBC；现有事务调用链保持 |
| P4 基础后端迁移 | 迁 File、Query、Coordination、Generation 已有后端；提取 codec | P3 | 原有 JDBC/Memory/ZooKeeper 测试通过；Core 无后端依赖 |
| P5 Metadata/Table/Tenant | 拆投影与 Catalog 端口，迁表控制存储，收敛 Tenant | P4 | 权威来源唯一；旧数据升级、冲突拒绝、outbox 原子性通过 |
| P6 跨领域事务 | 落地 workflow API/PostgreSQL，迁 AI/Metadata/Query 同连接调用 | P5 | Core 不再接收 Connection；fence、权限与原子性不退化 |
| P7 Query/Scheduler | 从 Coordinator 提取查询生命周期、dispatch、恢复与调度策略 | P6 | Coordinator 只保留入口、角色适配与装配；执行路径一致 |
| P8 领域存储收尾 | 迁 Materialization、Indexing、AI、Governance、Streaming、Semantic 后端 | P6 | 所有领域 Store 有明确 owner；不存在 Core 内持久化实现 |
| P9 多实现能力 | 拆安全、AI、缓存、观测、索引、刷新、Streaming Provider | P7、P8 | 工厂注册、能力校验、装饰器组合与资源关闭通过 |
| P10 Worker/Shuffle | 拆 Worker 执行契约、Shuffle local/remote，完成执行侧 Provider 装配 | P9 | Worker 无控制面实现依赖；JNI、Shuffle、取消与恢复通过 |
| P11 清理与发行 | 删除旧制品/空目录/描述符，更新发行、SBOM、运维文档 | P10 | 干净构建无旧 class/JAR；生产发行可启动 |
| P12 升级与最终验收 | 停机升级演练、全量回归、发布回退演练 | P11 | 全部完成标准满足，形成可复核证据包 |

各批具体工作要求如下。

**P0：建立唯一执行台账。**

台账每项必须包含：能力、契约类型、旧 owner、目标 owner、实现、生产调用者、装配入口、资源所有者、持久化表/编码、测试、状态。

扫描覆盖 Java、POM、ServiceLoader、反射类名、配置、资源、Native/JNI 引用和发行脚本；正则扫描结果只作为候选，不能作为语义审查完成证明。

当前历史执行计划作为进度证据保留。新增目标 ADR 明确替代中央 SPI 和巨型 Persistence JAR 的旧目标，同步更新架构规则、模块 manifest 和长期蓝图。

**P1：先建立装配层，避免后续继续把实现依赖塞回 Core。**

初次移动 bootstrap 时保持构造顺序和调用行为；不同时重写查询状态机。提取公共 Transport 后，先关闭 Coordinator→Worker 的真实依赖。

**P2：一次迁完一个能力的契约闭包。**

同时迁接口、值类型、错误、codec 契约和公共签名引用。若两个 API 产生环，将真正共享的值移到单一 owner，或由调用方端口反转依赖；禁止用新建杂项 Common 包打破环。

**P3—P4：先完成现有实现的物理隔离。**

SQL 与 canonical bytes 在移动批次保持原样。每完成一个领域后端迁移就删除旧实现位置，不留重复所有者。

Memory 后端保持明确的实例生命周期和非持久语义，不能用于替代生产持久化/分布式协调测试。

**P5：数据模型问题独立于文件搬迁处理。**

Tenant 收敛和离线迁移单独形成可审查变更；Metadata 投影、Catalog revision 与 Table 权威边界分别验收，不以类名统一代替行为验证。

**P6：事务替换逐条工作流进行。**

顺序固定为：现有语义测试 → 抽原子操作接口 → PostgreSQL 实现 → 切换调用者 → 移除旧 JDBC 调用 → 故障测试。

不引入 Saga、异步最终一致性或跨数据库事务来替代当前同库事务。

**P7—P10：落实所有多实现能力。**

每种能力按“契约、实现、工厂、装配、生命周期、测试”六项交付。已有符合边界的 Filesystem 和 Native Bridge 保留，只调整消费者和依赖，不为统一目录形式重写其行为。

**P11：检查真实发行物。**

使用 clean 构建消除旧 class 干扰；检查最终 JAR/插件目录/Native 资源，而不仅检查源码不存在旧包。

插件二进制必须重新构建，发行包不能夹带旧 SPI JAR。保留既有用户配置键和启动方式，通过新版装配读取；不保留旧 Java FQN 兼容入口。

## 5. 测试、门禁与证据

### 5.1 架构与契约测试

扩展现有模块依赖、包无环和 SPI 检查：

- API/SPI 禁止导入 Provider、Core、进程模块及未批准 SDK。
- Core 禁止依赖实现后端，测试 classpath 同样检查。
- 公开签名与泛型类型可在最小 classpath 下加载。
- Provider 描述符指向真实实现，工厂 ID 唯一，兼容版本有效。
- 无 split package、无旧包转发、无遗留反射类名。
- Coordinator classpath 不包含 Worker Core/Native Bridge。
- Worker classpath 不包含 Coordinator/Planner 等控制面实现。
- 独立插件在不同依赖版本下完成发现、实例创建和一次真实契约调用。

每批更新 manifest 后运行：

```bash
.pangrova/scripts/check-module-manifest.sh
```

指令布局变化时才运行 `.pangrova/setup.sh --check`，不将它作为每批固定前置条件。

### 5.2 后端契约与事务测试

使用共享契约测试规范，让每个 Provider 针对声明能力运行测试：

| 场景 | 验证内容 |
|---|---|
| CRUD 与分页 | 返回值、排序、边界与删除语义 |
| 幂等 | 同请求重放、同键不同内容冲突 |
| CAS | 并发写入只有符合期望版本的一方成功 |
| Lease | 过期、续租、epoch 单调与失主拒绝 |
| 原子性 | 中途失败不留下部分 job/descriptor/关联 |
| 当前权限 | 编译后权限变化仍被发布/提交拒绝 |
| 提交未知 | 断开确认后可核对状态，不误删已提交数据 |
| Catalog revision | 历史快照不可被 current 替代 |
| Outbox | 投影成功必有事件，回滚不留下事件 |
| 资源生命周期 | 创建失败、关闭失败、重复关闭和泄漏 |
| 恢复 | 进程重启后读取真实持久化状态 |
| Tenant 升级 | 空库、标准旧库、单数表数据、两套数据一致及冲突 |

PostgreSQL 使用仓库规定的 Testcontainers 与 canonical migration，不使用 H2/SQLite 冒充等价后端。故障注入遵循现有测试基础设施；禁止关闭失败测试、静默 fallback 或不可控 sleep。

### 5.3 运行链路与角色测试

至少覆盖：

- Coordinator-only 启动不加载 JNI。
- Worker-only 启动不创建规划与查询 admission 服务。
- Standalone 复用同一装配入口且资源不重复关闭。
- 一个 Coordinator、两个真实独立 Worker 的查询与故障恢复。
- Iceberg/Paimon/Lance 现有读写链路。
- COPY/FILE、物化刷新、Generation 发布、AI 执行和 Streaming 现有链路。
- Worker 取消、资源释放、Native 句柄生命周期。
- Shuffle local/remote、Spill、重试与清理。
- 分阶段启动故障和恢复未完成时不可 ready。
- 外部协议与持久化编码 golden fixture 保持一致。

### 5.4 执行命令与扩大验证范围

先确认 JDK 17、Maven 3.9+、Docker 和 Native 前置条件；Iceberg 使用仓库指定的 patched SDK candidate。

每个批次：

```bash
mvn -q -pl <受影响模块> -am test
```

共享契约、父 POM 或跨模块链路改变时，运行全 reactor 编译/安装及受影响消费者测试。涉及执行路径时运行规则要求的 Java/native 集成和 smoke。

最终执行：

```bash
mvn -q -Pfull-regression verify
```

Native 输入或包装改变时执行相应构建、测试和重新打包：

```bash
cmake --preset dev-release -S pangrova-worker-native
cmake --build pangrova-worker-native/build --parallel
python3 pangrova-worker-native/bin/run-native-tests.py
./pangrova-worker-native/bin/package-native-lib.sh
```

JNI、Native 内存、所有权或并发改变时追加 ASan；平台结果分别报告，macOS 验证不替代 Linux 发布资格。

每批证据包含：源码基线、变更模块、验证命令、实际结果、首次失败及处理、未完成项。历史通过结果不继承为本批通过。

## 6. 升级、回退与最终交付

### 6.1 停机升级流程

1. 停止接入新查询和调度新任务。
2. 排空或按现有规则终止活动任务，等待现有租约和恢复流程安全结束。
3. 停止全部节点，备份 PostgreSQL 与必要配置。
4. 执行只读 schema/Tenant 兼容检查。
5. 执行新增的向前 migration；需要时运行 Tenant 离线迁移。
6. 校验记录数、关键状态、租户归属与权限投影。
7. 安装新发行包与重新编译的插件。
8. 启动控制面并完成恢复，启动 Worker。
9. 完成角色、连接器、写入和恢复 smoke 后恢复流量。

升级不自动删除历史表、不重写已发布 Snapshot、不修改既有任务编码。

### 6.2 回退规则

- 纯代码和模块移动批次：回退整套发行包，禁止只回退其中一个角色或插件。
- 数据迁移尚未接受新写入时：使用升级前备份恢复，或执行已经验证的兼容回退路径。
- 新版本已经接受写入后：先停流量，核定旧版本可读性；不默认认为旧二进制能安全读取所有新状态。
- 本轮 migration 优先增量扩展；不通过删除列和表制造不可逆升级。
- 迁移冲突、权限不一致或恢复失败时保持不可服务状态，不带病发布 ready。

### 6.3 交付物

实施结束交付：

- 一份已生效的模块边界 ADR 与目标依赖图。
- 一份逐能力完成台账，包含契约、实现、调用、装配和测试证据。
- 重构后的模块、包、POM、服务注册与发行配置。
- 后端契约测试套件及跨领域事务测试。
- Tenant 离线检查/迁移工具及测试。
- 更新后的架构文档、开发规则和部署升级手册。
- Coordinator、Worker、Standalone 与插件发行物。
- 全量验证、升级和回退演练记录。

**完成判定以实际依赖、运行链路和数据语义为准。模块已创建、文件已移动或 Maven 编译通过，都不能单独作为该能力完成的证明。**
