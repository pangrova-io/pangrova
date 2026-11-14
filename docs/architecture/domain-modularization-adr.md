---
title: "Domain Modularization ADR"
description: "Accepted decisions for domain contracts, implementations, and process assembly"
---

# ADR: 领域契约、实现与进程装配分离

决定日期为 2026-11-01。架构已接受，正在实施，整仓发行验收待完成。

本 ADR 替代此前中央 SPI、合并领域 owner 和独立 SPI 制品的目标决策。
阶段划分与退出条件见下文“实施顺序及退出条件”；历史执行记录保留在 git 历史与 CI
记录中，不是本轮完成证明。实际生效的模块边界以 `.pangrova/modules.yaml` 为准；
未实施的目标不得写成现状。

## 决策

- 领域只有一个制品时，源码和 POM 直接放在领域目录。只有一个实现且全部调用方都在本领域的契约，和实现放在同一制品。多个实现共用的契约独立发布；外部调用方需要避免引入实现时也保留独立制品。
- 领域值、调用契约、扩展接口和强类型工厂统一使用 `.api.<能力>` 包。Provider JAR 在
  `META-INF/services/<工厂完整类名>` 注册实现。服务发现、配置校验与实例创建分别可验证。
  Paimon 的 Hive 工厂以标识选择实现，接受类型化的 CatalogContext，返回由调用方管理的 Catalog。
  Pangrova 的 AI 与 CDC 格式沿用这一方式，各能力分别声明返回值和资源所有权。
  [HiveCatalogFactory](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalogFactory.java) · [HiveCatalog](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalog.java)
- Parser、Batch、File、Planner、Query、Semantic、Table、Worker 的目标均为领域根目录直接构建单个 JAR。
  Batch 的 Arrow SDK 为可选依赖，值类型调用方可独立加载。File 的访问服务消费精确查找与刷新契约，持久化实现承担事务与存储。
- 客户端有 HTTP SDK、CLI、JDBC 三个入口。Common 的 `org.pangrova.common.client` 按 query、result、metadata、session、version、error 组织共享契约。结果端点和协议值归 result，Common 无需依赖 Transport；
- Common 的 `org.pangrova.common.json` 统一拥有 JSON mapper、持久化编码和纯 JDK 字符串转义。Jackson databind 与 JSR-310 在 Common 中为可选依赖；调用 mapper 的制品显式声明所需依赖，客户端 DTO 仅依赖 Jackson 注解。
  HTTP SDK 拥有真实 REST 请求、凭据和重试，Transport Client 拥有结果传输与 Arrow 解码。CLI 直接消费 QueryClient，HTTP 类实现该接口。
- QueryId 与 ExecutionAnchor 分别标识跨协议查询和跨查询/后台任务的执行归属，归 Execution API。Query 发布生命周期、审计与日志重放契约。依赖具体任务请求、生成列协议与 Native 运行能力的提交前校验和绑定由 Execution Binding 实施。
- Table 发布 ownership、统计、维护及治理绑定所需要的操作。Table Persistence API 独立拥有后端工厂及一组属于相同 authority 的仓储实例。Worker 单 JAR 同时包含任务控制与 JNI，Server Worker 拥有进程装配。
- Function 的单一契约制品直接由 `pangrova-function/pom.xml` 构建。AI 的多个实现共享 `pangrova-ai-api`，
  `pangrova-ai-openai` 和 `pangrova-ai-qwen` 分别发布真实工厂，`pangrova-ai-http` 复用受控 HTTP
  和兼容的 chat 编码。Server 以运行时依赖选择 Provider，Worker 和 Control 的生产代码只依赖领域契约。
- 独立流程制品以实际职责命名。包可直接以业务能力命名，例如 HTTP 请求处理归 `http.request`，血缘归 `lineage`。新增层级必须能说明调用方及其拥有的状态或资源。Provider 拥有技术实现，Server 拥有进程装配和生命周期；后端与 SDK 通过领域契约隔离。
- Java 的顶层枚举、异常、纯常量按所属制品进入 `.enums`、`.exception`、`.constants`。只在所属类型中有含义的内部状态保留类型作用域。Source wire 值与 JNI 异常变更同时更新服务描述符、原生类名和发行测试。实现选择采用工厂标识，枚举名称不承担 Provider 注册。
- Planner 实现生成可执行的 Protocol/Substrait 片段，因此实现层显式依赖 Protocol。Planner 的公共契约保持最小 classpath；由独立类加载器覆盖其公开签名。Module manifest 不再支持用 milestone 暂时放行未声明的边。
- 基础类型属于 Types，批数据属于 Batch。它们不能通过 Connector 才能被引擎使用。
- 物化调用契约和扩展契约统一由 `pangrova-materialization-api` 发布。领域值与服务端口位于
  `.api.domain`、`.api.port`。扩展点按定义、策略、刷新和改写分入 `.api`；
  注册表的重复 ID 检查、完整性校验和策略选择归 Materialization Lifecycle。
  持久化命令及已发布版本读取端口由 `pangrova-persistence-materialization-api` 拥有。
  Server 用同一数据源构造 PostgreSQL 实现，各条语句复用这两个实例。
- 查询事件、统计观察值及监听器扩展契约归 Query API，进程内分发归 Query。
  Observability API 不反向依赖 Query；事件观察不拥有查询状态、持久化提交或重放保证。
- Query 独占查询状态转换、实际 Connector 操作作用域、终态物理清理与历史保留。
  Coordinator 的编排器借用这些内部实现；等待结果不能授予调用方完成状态机内部 Future 的权力。
  逻辑终态、Native 写完成、发布完成与物理 drain 分别记录，不以一个完成标志替代。
- 所有查询写入共用 Query API 的 WriteAuthorityRequest / WriteAuthorityResolver；普通写入与
  生成列写入都在分配前通过 Execution Binding 的 WriteAuthorityAdmission 校验真实当前授权回执。
  授权实现由 Server 在构造时固定，不能运行中 setter 替换；未配置时写入显式拒绝。
  生成列 Catalog 属性 codec 归 Generation Codec，Proto wire 类型仍归 Protocol。
  当前权限事务与提交 fencing 不因移动契约而改变；同步 resolver 不宣称独立取消或超时能力。
- StatementCategory 是 Query API 的纯观察值；AST 分类、精确目标解析与 CALL 参数绑定归
  Query，只依赖 Parser API 和值契约。权限入口与执行使用同一绑定，不分别解析目标。
  分类穷尽内建语句类型；分类、注册和真实支持状态分别验证，不能以 QUERY 默认值隐藏遗漏。
- 视图创建模式由 Connector API 明确区分 CREATE、IF_NOT_EXISTS、CREATE_OR_REPLACE，
  原子冲突判断由实现拥有。Security API 的 ViewAccessControl 定义当前 CREATE_VIEW/ALTER/DROP
  检查；创建权限不隐含覆盖权限。现有 Iceberg 视图是 catalog 实例内存状态，不宣称重启持久性。
- 租户成员 SQL 归 Server 入口适配，原子成员授权与写入归 Security Persistence。
  必须检查 exact ACTIVE tenant 的当前 OWNER/ADMIN，不能信任历史 session role 或修改
  allowedSchemas 来表示成员。成员授权不支持跨租户转移或撤销特权角色；SQL 无 NOOP 认证旁路。
- 领域绑定只接收其实际输入：检索绑定接收 vector/reranker plan，制品解析接收 artifact/merge intent，
  不反向依赖完整 Query/DAG Context。精确模型匹配归 Scheduler Policy，消费调用方已确认活跃的
  capability inventory；Embedding Catalog 部署 fence 校验归 AI Control。模型身份、服务限制
  或实现不唯一时拒绝，不能按“最新”选择；Protocol 只拥有能力 wire identity 的结构比较。
- Query API 持有查询会话和 DAG 终态通知；绑定 live/durable authority、wire 写配置及检索意图的 QueryExecutionScope 归 Execution Binding，
  不依赖 Coordinator 调度入口。分片查询只接收真实 session、快照选择与写模式；序列化只接收列名。
  控制面 Worker 适配、生成结果读取和 Shuffle 持久化证据各在其能力边界，不能反向耦合 DAG 入口。
  缓存亲和路由归 placement，选择器直接借用同一个拓扑实例；不能经由 Query 分片租约管理器获取路由。
- QuerySession 归 Query API，借用实际查询 authority 和不可变 tracing snapshot。
  捕获与激活契约归 Observability API 同制品，SDK 实现由 Server 显式注入；
  Session 不隐式选择 Provider，tenant/property enrichment 必须保留全部可信执行字段。
- Parser API 拥有 AST、解析与恢复诊断契约；ANTLR tree、可变位置注册表、缓存和类型字符串
  解析留在 Core。扩展通过 API parser 委托，不接收实现类。Core 详细恢复树明确属于实现工具。
- 孤立能力按真实需求完成闭环：SQL GRANT/REVOKE 由 Server 适配到 Security 的原子持久化端口；
  PostgreSQL 在同一授权 gate/事务中完成当前校验、委派链、revision 和审计回执。
  Parser AST 存在不是支持证据；对应 SQL、真实后端、装配和 Native/HTTP 验证必须同时记录。
- Persistence 按领域端口和实际 PostgreSQL/Memory 后端拆分。JDBC 协作协议与 support
  单独隔离；跨领域事务由强类型 workflow 的一个协调者持有连接与提交权。
- Coordination 的注册、租约和查询并发配额端口统一归 Persistence Coordination API，
  其中真实 quorum 扩展放在同制品 `.api.provider`。PostgreSQL 和 Memory 是独立制品，
  Server 选择实现，WorkerManager 必须接收显式 Registry；Core 不再隐式构造 Memory。
  PostgreSQL 配额表由唯一 schema 初始化创建，Provider 只能校验，不能重建或重置活跃权威。
  Memory 只保证进程内状态；测试配额 fake 不成为生产 Provider。
- Metadata 拥有 Catalog 部署、解析、投影、同步和失效；Table 拥有引擎表身份、
  ownership、观察、注册、rename、维护与统计。外部 Catalog 仍拥有外部表事实。
- 一个事实、状态机和资源只有一个权威 owner。禁止以拆模块为由复制状态、弱化 CAS、
  fencing、权限检查、原子提交或失败处理。
- 发布代际的 reader pin 获取必须持有实际查询 authority 的 operation permit；查询
  生命周期使用同一租约服务续租和释放。逻辑终态不等于物理 drain，二者之间继续续租。
  缺少 authority 或租约服务时不得暴露可执行代际；可选物化视图改写可以拒绝候选并读取
  原始源表。元数据发现不因此提前创建查询或租约。租约不能承诺进程暂停、存储失联超过
  租期后的无限保护；恢复与失租 fencing 需要单独验证。
- Coordinator 装配控制面，Worker 装配执行面。Coordinator 不依赖 Worker 或
  JNI 执行运行时；Standalone 复用两种角色装配，不引入新的微服务。
  Worker 的独立与嵌入入口现由 `pangrova-server-worker` 发布，两者共用 `WorkerConfiguration` 并安装生产 Catalog runtime。
  三个进程入口共用 `pangrova-server-bootstrap` 加载角色配置。Standalone 启动 Worker 或注册关闭钩子失败时，按获取顺序逆序释放已启动角色，保留主异常。
- 插件工厂强类型化，启动时校验 ID、兼容性、角色、能力和作用域。缺失或失败显式拒绝，
  不静默切换 Memory。插件更新依靠重启，不支持热卸载。
- AQE 的倾斜检测通过 Shuffle API 的 SkewDetector 注入，Server 选择 builtin 实现。
  Planner 仅适配真实统计与决策，不复制检测算法、不依赖 Shuffle 实现制品。
  热分区识别、并行度启发式和实际 range split 是不同能力，不能相互替代支持声明。
- Shuffle 统一控制契约，由 Worker 的 JNI 包管理 Native 绑定，Direct 的数据传输在 C++ 内完成。
  显式表达 incarnation、exchange generation、producer/consumer attempt、seal、
  commit receipt、取消、背压和 buffer 所有权。容错保证必须由能力与部署共同确认。
- Celeborn 是未来 Provider；本轮以现有 Direct、Local、自研远端验证抽象，不创建
  Celeborn 空模块，也不宣称已集成。
- Native 保持 Java 控制、JNI 桥接、C++20 执行和单共享库发行。
- 新项目直接修改所有调用方，不保留旧 FQN 转发壳，不建设数据迁移、双写或旧库兼容。
  空库初始化资源归 schema-migrations。PostgreSQL 的 public 与 shuffle 分别维护版本历史；Shuffle Service 通过运行依赖加载同一资源制品，Coordinator 的恢复测试使用相同的两套真实 schema。

公共接口和工厂统一归 `.api`。实现包和类直接说明其拥有的操作、数据或资源。删除只转发调用的层；重复实现合并到实际被调用的类型。HTTP、Flight、JNI、OpenTelemetry 和数据库事务都按具体协议或能力命名。接口需要交代凭据的来源、调用者可见的错误和资源释放顺序。

## 平铺领域的实际包边界

这些领域的根 POM 各自发布一个 JAR。`ModuleDependencyPropertyTest` 核对根路径、制品、主源码和子模块声明。领域内部仍检查源码依赖方向。

| 制品 | 同一 JAR 内的职责 | 跨边界的调用 |
|---|---|---|
| Parser | `api.ast` 按表达式、语句族和类型组织。`parse/build/format/diagnostic` 消费这些类型。 | 调用方依赖解析结果和 AST，ANTLR 构树细节留在实现内部。 |
| Batch | `api.block/page/reader` 描述数据访问。`arrow` 持有 Arrow 表示、流验证和脱离缓冲区后的值转换。 | 只读取通用接口的调用方可在独立类加载器中加载契约。 |
| File | `api.space/profile/authority` 描述访问所需的读操作。`access/directory` 执行授权与目录管理，`worker` 实现委托访问。 | PostgreSQL 可以加载同一 File JAR，但其源码仅可引用 File 的 `.api`。Server 注入仓储。 |
| Planner | `api.fragment/metadata/statistics` 暴露规划输入输出。`analysis/validation/optimization/cost/substrait` 分别分析、校验、优化、估算和序列化。 | Coordinator 借用 Planner 准备计划。Worker 读取协议，无需装入 Planner。 |
| Query | `api.submission/session/lifecycle/journal` 描述会话、状态与重放。`lifecycle/stage/dispatch/recovery/retention` 执行业务流程。 | 状态转换归 Query。HTTP 响应映射、持久化和执行进程各自消费对应契约。 |
| Semantic | `api.identity/version/validation/publication` 定义身份、不可变版本、校验结果和发布约束。`validation` 校验定义。 | 后端持有版本和已发布指针的原子写，版本契约保留 CAS 结果。 |
| Table | `api.ownership/observation/rename/maintenance/statistics/policy` 区分表身份、维护操作和绑定。实现按同名能力组织调用。 | 后端事务与仓储选择归 Persistence 和 Server。 |
| Worker | `task.state` 拥有任务状态约束，`task.report` 拥有状态上报与确认，`execution` 执行任务，`jni` 管理 JNI 边界。 | Bootstrap 持有生产状态上报客户端，TaskManager 借用它。失败启动和正常关闭都在任务退出后释放该客户端。 |

## 七个多制品家族的划分依据

下列路径来自生产依赖。若将路径首尾合为同一 JAR，中间制品会反向依赖合并后的 JAR。判断时同时考虑入口、可替换实现与实际编译依赖。

| 家族 | 制品边界 | 保留该边界的原因 |
|---|---|---|
| Scheduler | 调度契约、调度流程、内置 placement 实现 | Query 直接使用 placement 契约。调度流程需要 Query、配额和集群状态。Server 通过 `WorkerSelectionStrategyFactory` 装入配置的策略。仅内置的 load-balanced 工厂已随包发行。 |
| Streaming | CDC 与更新契约、流处理运行时、格式与写入实现 | PostgreSQL 与 Coordinator 都消费流处理契约。运行时负责恢复、checkpoint 和 watermark。Canal、Debezium、Maxwell 各有服务描述符条目。Writer 的运行入口和能力单独验收。 |
| Metadata | Catalog 契约、Catalog 与投影运行时、表投影编码 | `metadata-catalog → persistence-catalog-api → metadata-api`。Catalog PostgreSQL 和跨域 Workflow PostgreSQL 需要相同的表投影编码，两者均无需创建 Catalog 注册表。通用 Jackson 配置归 Common。 |
| Materialization | 定义与扩展契约、物化流程、内置刷新策略 | `materialization-lifecycle → persistence-materialization-api → materialization-api`。流程持有 attempt 与发布顺序。三种刷新策略消费同一快照输入，结果仍由同一个物化流程发布。 |
| Indexing | 索引契约、构建和生命周期、查询谓词编码、内置策略 | `indexing-lifecycle → persistence-indexing-api → indexing-api`。Iceberg 和 Protocol 使用契约。构建流程持有 attempt，策略工厂与编码器都无需创建构建协调器；内置制品有九条 `IndexStrategyProvider` 注册。 |
| Generation | 生成身份与值、共享编码、提交与发布流程 | `generation-workflow → persistence-generation-api → generation-api`。Iceberg、Paimon、Planner 与 PostgreSQL 均直接使用编码。提交与发布流程负责选择现有代际、绑定输入和校验提交结果。 |
| Execution | 执行身份和期限、提交前任务绑定 | `execution-binding → generation-api → execution-api`，也有经 Protocol 返回身份契约的路径。Query、持久化和 wire 可以单独加载身份契约。另一制品只负责提交前的任务请求、选项、split、授权和租约绑定。 |

真正只服务单一实现的辅助代码与其调用方同包维护。对于已有一组能力包的流程制品，不在每个能力包下再建 `core`，也不按表中每个操作生产一个 JAR。更名或合并应保留以上调用路径与端口的可验证边界。

## 上游源码与本项目的取舍

- Iceberg 的构建分别声明 API、Core、Arrow、Parquet、云后端及其 bundle。Pangrova 采用同样的依赖方向，让调用者按契约编译，由发行层决定是否携带实现 SDK。每个领域是否拆 JAR 仍由实际调用和生命周期决定。[Iceberg 构建定义](https://github.com/apache/iceberg/blob/main/settings.gradle)
- Paimon 的 `AbstractCatalog` 在 Catalog 层组织表操作，分别使用文件访问、Schema 管理和表提交组件。Pangrova 将外部表的快照与提交交给 Connector，将本地控制面记录交给分域 Persistence。物化的命令、候选代际和已发布头共享原子操作，新增后端需要验证这些操作的并发与重放语义。[Paimon Catalog 实现](https://github.com/apache/paimon/blob/master/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java)
- SeaTunnel 将工厂标识和配置校验声明在 Factory，数据源类型和发现入口属于 TableSourceFactory。Paimon 的 HiveCatalogFactory 接受 CatalogContext，返回契约 Catalog。Pangrova 同样按能力声明强类型工厂，Server 负责选择与校验，后端负责 SDK 实例及其关闭。[SeaTunnel Factory](https://github.com/apache/seatunnel/blob/dev/seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/Factory.java) · [SeaTunnel TableSourceFactory](https://github.com/apache/seatunnel/blob/dev/seatunnel-api/src/main/java/org/apache/seatunnel/api/table/factory/TableSourceFactory.java) · [Paimon HiveCatalogFactory](https://github.com/apache/paimon/blob/master/paimon-hive/paimon-hive-catalog/src/main/java/org/apache/paimon/hive/HiveCatalogFactory.java)
- Trino 在 `ServerMainModule.setup` 读取角色配置，安装 CoordinatorModule 或 WorkerModule，然后建立共享绑定。这里的 Module 是进程内装配单元。Pangrova 把角色选择与公共生命周期放在 Server，另外用 Maven 依赖和真实 JAR 检查约束 JNI 的加载范围。[Trino ServerMainModule](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/ServerMainModule.java)

### SQL 文本与解析扩展的缓存归属

Common 的 `SqlText` 保留字面量、引号标识符、注释正文和行注释结束位置。Parser 用它折叠内置语法 token 之间的空白。Planner 的诊断基线还对引号外的 ASCII 字母折叠大小写，两者保留各自的缓存和身份语义。

注册扩展后，Parser 使用注册表快照的版本和原始 SQL 作为键。解析器选择、解析和缓存写入属于同一快照；替换或注销扩展后，旧解析即使较晚完成，也只能写入旧版本的键。缓存容量限制同样适用于旧版本条目。

## 循环依赖的判定与处理

`A → B` 表示构建、加载或使用 A 需要 B。每次分析先标明是哪一种依赖，再给出闭合路径及产生每条边的源码、POM 或装配位置。A 调用 B，B 把结果返回给 A，本身只产生一条调用依赖。

调用接口与扩展接口同属领域的 `.api.<能力>`。单实现且只在本领域使用时随领域制品发布。多种实现共用的契约独立发布。合并前应将候选制品收缩为一个节点，检查所有经外部制品返回的路径；合并后继续检查 JAR 内的包。若调用包返回 SPI 类型、SPI 又接收调用包中的请求类型，先找出双方各自消费的最小契约。例如读取方只需要批数据时，批数据及其所有权由平铺的 Batch 模块定义，读取接口和工厂分别引用它。若确属同一能力且总是共同演进，可将相关接口放入该能力包。Core、Provider 或 Server 的合并须单独论证。

| 检查对象 | 判定与处理 |
|---|---|
| Maven 制品 | 在实际启用的 profile 下检查整个 reactor 的构建顺序。生产依赖、测试依赖、classifier 和构建插件分开追踪。只要 Maven 必须先构建对方，就要纳入构建图。聚合父 POM 列出子模块不构成生产代码依赖。 |
| Java 生产包 | 检查发行制品之间及同一 JAR 内部的能力包。两个以上包互相可达即形成一个强连通分量。用真实类型引用定位边，包括方法签名、泛型、注解、工厂返回值和全限定引用。一个包内部多个类协作按职责判断。 |
| 插件与运行角色 | Server 可以装入 Provider，Provider 通过领域接口被调用。验证工厂的类加载器、服务描述符和角色发行 classpath，检查 Core 是否仍在运行时按实现类名回查 Provider。 |
| Native | 对每个目标验证 CMake 构建顺序。Native 门禁解析仓库源码内的字面量 include，任何文件闭环都报告逐边行号并失败。按 OBJECT 目标对应的源码目录汇总，command、jni、engine、capabilities、indexing、vector、udf、lance 目前属于同一个强连通分量。例如 command 引用 JNI 的 RuntimeInitializer，JNI 又引用 command 的 FlightServerRegistry。使用 `python3 .pangrova/scripts/check-native-includes.py pangrova-worker-native/src --components` 导出边和证据。组件源码依赖和 CMake 目标构建顺序分别验收。 |
| 运行时生命周期 | 将“必须等待对方完成才能继续”画成等待边。禁止 A 等 B ready 而 B 同时等 A ready，也禁止持锁回调让对方等待同一把锁。Server 可以先创建实例，再注册回调，能力确认后才开放接入。每个资源指定唯一关闭方，借用者先停止使用，由所属方释放。通过启动失败、取消、并发和逆序关闭测试验证。 |

### 断开哪一条边

1. **先找语义 owner。** 记录谁拥有值的身份与校验、谁决定状态转换、谁提交事务，以及双方实际需要哪些操作。先删除残留 import、错误 scope 或失效的依赖声明。同一对模块可能有多条独立路径，逐条检查。
2. **共享值回到定义其语义的最小 owner。** 跨 Query、Generation 和执行协议的执行租约属于 Execution。Protocol 可以引用身份契约，查询状态机无需随 wire DTO 进入 Worker。外观类似但生命周期或作用域不同的值仍分别拥有。
3. **领域定义所需操作，实现依赖该契约。** File 的授权服务需要按名称或 ID 查询空间、读取当前 profile。在目标结构中，这两个读契约由 File 声明为 `FileSpaceLookup` 和 `CurrentStorageProfile`。Persistence 的 `FileSpaceRegistry` 和 `StorageProfileRegistry` 分别扩展它们，Server 注入实际实例。编译依赖从 Persistence 指向 File，运行时 File 可以通过接口调用仓储。历史 revision、冲突和权限失败的语义必须保留。
4. **多方编排放到真正的调用者。** 两个领域协同完成一次操作时，由拥有业务流程的一方编排。涉及跨域原子写时使用强类型 workflow，保留同一连接、锁顺序、fence 和提交 owner。Server 负责装配，事务提交由工作流实现负责。
5. **按独立性确定制品。** 如果两个包共同维护同一状态机、共享同一生命周期，又没有独立调用者，可以把相关行为收回拥有该状态机的能力包，并删除转发层。只合并 Maven 模块仍须检查内部包图。多种实现共享独立契约，工厂放在领域的 `.api`，通过服务描述符发现。不得通过搬入 Common、静态 setter 或双向 lookup 隐藏依赖。
6. **后端联测放到能同时依赖两侧的位置。** 若 Table 的测试使用真实 Memory 后端，而该后端实现 Table 接口，这组测试放进 Memory 或 Integration Tests。保留原断言、真实对象和故障覆盖。生产代码读取的方法由其生产依赖提供。

### 合入条件

每一批迁移由修改该依赖的开发者提交一条可复核记录，至少列出**图的种类、闭合路径及逐边出处、语义 owner、要删除的边、替换后的依赖方向、验证命令与结果**。同一强连通分量可能包含多个环；切断示例环后须对整张图重算分量。跨域事务还要验证原事务边界，生命周期改动还要验证启动失败、取消和资源释放。

- **直接阻断合入**：启用的 Maven reactor 无法排序、Java 生产包出现多包闭环、Native 仓内源码出现文件 include 闭环，或进程必须互等才能完成启动、取消或关闭。即使图无环，违反角色边界的单向依赖也要单独拒绝，例如 Coordinator 制品携带 Worker 执行实现。
- **Native 已知组件耦合**：触及现有组件环时，记录变更前后相关组件边及源码位置，按语义 owner 移动类型、反转调用或从入口注册实现。新增或扩大的组件环须先消除。目标拆分完成时复查本次涉及的组件，只有在组件图上消除互相可达的路径后，才可将对应组件标记为解耦。当前脚本输出组件边，组件差异仍需审查。
- **允许的双向交互**：请求与响应、进度回报和完成通知可以双向传递。编译时双方依赖稳定契约；启动与关闭分别检查等待顺序。ServiceLoader、反射类名、延迟构造、事件总线、`optional` 或 `test` scope 都不能替代真实依赖方向的验证。

平铺之前，先在现有依赖图上把拟合并的制品视作一个节点。若存在一条从拟合并制品出发、经过仍独立发布的制品、再回到拟合并集合的路径，合并后就会成环。例如合并 Metadata Catalog 和 API，会把 `metadata-catalog → persistence-catalog-api → metadata-api` 变成 `metadata → persistence-catalog-api → metadata`。此时保留独立契约，或先按语义归属消除那条依赖，再重新检查。合并集合内部的直接依赖在制品图中消失，同一 JAR 内的包仍要逐个检查。

可在改 POM 前对候选叶制品执行只读预检。例如在当前仓库运行：

```bash
.pangrova/scripts/generate-module-baseline.py --check-merge pangrova-metadata-catalog pangrova-metadata-api
```

工具沿本地 POM 的显式依赖（含 test、runtime）搜索一条离开再返回合并集合的最短路径，发现时打印各边的 scope 和 POM 并返回 1。该预检覆盖当前源码树的直接声明。继承、profile、插件、生成代码和运行时类加载仍按下列门禁核对。

**同批平铺多个领域时，同时收缩所有候选组。** 例如 A 的实现使用 B 的 API，B 的实现使用 A 的 API。单独平铺任一领域时构建都能排序，同时平铺却会得到 `A → B → A`。对同一条命令重复使用 `--check-merge`，每组列出准备合并为一个制品的叶模块。工具在合并后的图上搜索经过候选组的闭环，并输出形成闭环的原始 POM 边。一次只检查其中一组会漏掉这种依赖。

- 给出修改前后依赖路径和涉及的具体类型。主源码包图中包含两个或更多包的强连通分量必须清零。调整 `allowedImports`、`friendExports` 或生成 baseline 只能记录有依据的单向边。
- 对默认及本次涉及的发行 profile 验证 reactor。运行 `PackageDependencyAcyclicityPropertyTest` 检查默认 reactor 主源码。报告针对每个包含环的强连通分量列出一条实际闭合路径，以及分量内部每条边的源码位置。`ModuleDependencyPropertyTest` 另行校验包归属、允许的边及服务注册。检查实际 JAR 与单角色运行 classpath，验证反射、生成代码和 JNI 入口。
- 对变化的路径执行至少一条真实调用及相应失败场景。带事务的路径核对失败回滚和失主拒绝，资源边界核对失败清理与只关闭一次。每条路径记录实际结果，构建、包图和运行时证据分别给出。

从仓库根目录可复查发行 profile 的构建顺序，以及生产包的循环检测和门禁自身的回归用例：

```bash
mvn -B -ntp -Pnative-runtime,full-regression validate
mvn -B -ntp -pl :pangrova-testing-kit -am \
  -Dtest=PackageDependencyGraphTest,PackageDependencyAcyclicityPropertyTest,ModuleDependencyPropertyTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

包图报告位于 `pangrova-testing/pangrova-testing-kit/target/package-dependency-cycles.txt`。验收需要报告同时列出实际扫描的包数和 `Cyclic components: 0`。Maven 的 `validate` 校验当前 profile 的 reactor 排序；发布前仍执行完整的 `verify` 和角色启动测试。

**审查示例。** 列血缘的派生种类曾由 Query 的嵌套枚举声明，Governance API 为引用这个值需要加载 Query。该词汇由 Governance 解释，现归 `governance.api.lineage.ColumnDerivationKind`。Query 的列记录和 Governance 的数据集列记录共用它，生产制品只保留 `query → governance-api` 这一方向。查询执行、状态转换与重放仍由 Query 拥有。审查类似变更时，提交记录应列出原引用、新方向、语义 owner 和相应门禁或行为测试。

**Shuffle 的双向运行时调用。** Coordinator 调度使用 ShuffleTaskAccess 与 RecoverableShuffleControl。Pangrova 远端 Provider 使用同一契约制品中的 ShuffleLifecycleEvidence 记录注册和终态回执。Server 将 DurableStageShuffleEvidence 注入 Provider，证据写入沿用 Generation 的事务边界。两侧各自只需要引用 shuffle-api，换后端时无需把协调器或持久化实现放进插件类加载器。

**Worker 的内存回调。** `NativeRuntime` 接受内存包定义的 `AllocationListener`。原先 `NativeSpillController` 的无参构造器自行绑定 `NativeRuntime::nativeRequestReclaim`，产生了 `worker.jni → worker.memory → worker.jni`。Worker 的 `NativeRuntimeBootstrap` 现在将回收操作注入 Spill 控制器。JNI 可以继续通知内存仲裁器释放计费，内存仲裁器也可以请求真实的 Native 回收，生产包只保留 `worker.jni → worker.memory`。分别检查包图、回收字节数和仲裁器释放的额度。

**Coordinator 与 Worker。** Coordinator 下发任务、Worker 上报状态，两个方向的网络消息可以同时存在。任务身份和 attempt 由 Execution 契约拥有，消息结构由 Protocol 拥有。两端分别依赖契约，并由各自的 Server 装配传输实现。独立启动时，Coordinator 等待 Worker 上线属于调度条件；若二者都要等待对方 ready 才开放注册入口，就会产生启动等待环。角色联测要覆盖两者以不同顺序启动、Worker 延迟注册和退出。

构建和包的逐条边可以通过 `.pangrova/scripts/generate-module-baseline.py --graph-dir target/architecture` 导出。module-dependencies.dot 保留本地 POM 声明的 scope，package-ownership.dot 列出所有模块及其主源码包。profile 激活后的构建顺序与真实角色发行物另外验证。

## 实施顺序及退出条件

| 阶段 | 范围 | 退出条件 |
| --- | --- | --- |
| P0 | 核对工作区、模块、FQN、能力、资源与事务台账 | 每个现存模块有归属，现状和目标可分别核验 |
| P1 | Types、Batch、Execution 契约与 Common 清理 | 基础契约以最小 classpath 加载，通用模型独立于 Connector |
| P2 | Server、Transport、Plugin 与 Standalone 装配 | 两种角色依赖隔离，验证三种启动方式、失败清理和关闭 |
| P3 | 全领域统一 API/SPI 制品，迁移包与调用方 | 实际 JAR、服务注册与插件加载一致，旧类及 split package 清零 |
| P4 | 按领域拆 Persistence 端口与已有后端 | 空库初始化归 Schema Migrations，后端契约及并发检查通过 |
| P5 | Metadata、Table、Generation、Materialization 状态与事务 | 验证当前授权、CAS、fence、outbox、提交未知和恢复 |
| P6 | Parser、Planner、Protocol、Query、Scheduler 分工 | 完整查询入口到执行路径通过，Protocol 只拥有 wire 语义 |
| P7 | Direct、Local、自研 Shuffle 统一抽象 | 三个真实后端验证 attempt、receipt、取消、背压和故障保证 |
| P8 | 其余领域与可替换 Provider | 已宣称能力可达，显式装配，资源按作用域关闭 |
| P9 | Worker、Native、客户端、测试基础设施 | JNI、取消、结果传输和独立多 Worker 验证通过 |
| P10 | 清理与发行 | 全量回归、逐类与能力复核、实际发行物检查和同环境性能对比完成 |

每批迁移完整能力闭包：生产源码、测试、POM、服务注册、反射类名、配置、
类加载策略、manifest 与文档一起更新。纯结构移动与行为改变分别验证。
编译成功或目录齐全都不构成阶段完成。

## 验证

先运行受影响模块，再扩大到消费者和全 reactor。共享契约的最小 classpath、
公开签名、模块无环、唯一包 owner、插件类身份、真实 JAR 中旧类清理均须验证。
JNI/协议/Native 行为改变按 build-pipeline 执行完整链路，涉及内存所有权或并发
增加 ASan。最终运行 `mvn -q -Pfull-regression verify`。

持久化验证包括空库、CAS 竞争、幂等冲突、失主、权限变化、关联原子性、提交未知、
恢复和 outbox。Shuffle 验证包括多 producer/partition、空分区、重试与推测隔离、
未知提交、活跃读取与清理竞争、取消、有界队列、descriptor、权限和 Native buffer。
不能用假实现替代真实 Provider 或端到端证据。

每批记录实际命令、退出状态、首次失败、修复、环境阻塞和未完成项。原有通过结果
不继承为本批通过；未支持能力显式标记，不能以类或接口存在宣称支持。

## 本轮证据

逐类处置、生产调用、事实与资源 owner 以及对应测试见[模块重构处置台账](./modularization-dispositions.md)。台账区分已核查的调用链和待验收的能力。


## 已实施的 Execution 基础与共享编码边界

`pangrova-execution-api` 拥有 SDK 无关的 task stage/purpose/deadline；
`pangrova-execution-binding` 拥有 canonical 请求/选项/split 绑定、授权上下文和精确 lease 身份。
输入不再依赖 Planner StageNode，Core 不依赖 Coordinator、Worker 或 PostgreSQL 实现。
该边界不接管调度、权限事务、lease 续租或 Native 资源。

`pangrova-shuffle-api` 拥有 Common/JDK-only 的领域契约；Planner 和 Operations 只依赖该
契约制品。原 ShuffleTransport 只实现 Native 服务绑定，现已从通用 API 删除。NativeShuffleProducerRegistration 位于 `pangrova-worker` 的
`org.pangrova.worker.shuffle` 包，通过任务所有者的受保护命令通道操作借用
的 executor handle 注册 producer。C++ FlightServerRegistry 持有进程共享监听端口。Worker 任务所有者排空活动读写后注销该 producer，再释放 executor。Direct 字节仍在 C++。
统一制品内的 `org.pangrova.shuffle.api.worker.ShuffleByteEgress` 定义真实的批数据交付、
生产者完成与中止回调，由已装配的远端实现使用。批数据完成不等于 producer commit，
producer commit 不等于 exchange 发布。调用方 buffer 仅在同步回调期间借用；rowCount
不是 batch sequence；任务和 query ID 只索引已授权的完整 attempt 绑定。
Pangrova 远端交付由两种角色的工厂装配。Worker 通过 `pangrova-shuffle-native` 的强类型契约接入 JNI，Coordinator 通过 Shuffle API 请求注册、发布和授权。`pangrova-shuffle-pangrova` 拥有客户端、egress、读取、mTLS 和连接配额。Server 在角色运行包中安装实现。旧 onPartitionCommitted 和 DefaultShuffleByteEgress 已删除。

将 `coordinator.shuffle.mode` 与 `worker.shuffle.mode` 设为 `LOCAL` 会启用 Native 本地磁盘交换。调度器给有 exchange 的 DAG 选择一个 Worker，逐次核对该 Worker 的 admission domain、incarnation、租户准入与算子能力。生产任务携带 `MEMORY_AND_DISK`，Flight 服务按 producer handle 强制把批数据写入匿名磁盘段，使用进程共享的磁盘、文件及传输配额。上游阶段全部完成后，下游用该进程签发的分区票据读取，Worker 在建立连接前核对本进程的真实 Flight 地址。读完或取消后由 Native registration 和 reader 释放数据与句柄。LOCAL 拒绝推测执行、替换 attempt、Worker 换代恢复和持久化发布回执。

Java LocalFsSpoolStore 为 Java 调用者提供显式 attempt 段和清理。Generation 能保存任意 Provider 的版本化分配描述符，当前由 Pangrova 远端实现编解码和恢复。Celeborn 接入属于后续工作。

新增 `pangrova-security-codec` 是实际共享编码基础设施的独立边界：唯一拥有
DeferredAuthorizationScope 的规范编码与摘要，依赖 Security/Connector API 和 Jackson，
不依赖数据库或运行时 Core。编码从 PostgreSQL Provider 原样迁出，Security API
不承担复杂序列化，事务与当前权限判断仍归原 owner。

具体语义、调用方与未完成边界以当前代码、测试和 `.pangrova/modules.yaml` 为准。


## Shuffle 独立能力与有效保证

统一 API 制品的 `org.pangrova.shuffle.api.capability` 拥有 ShuffleCapability 和
ShuffleCapabilityDescriptor。消费者重试、生产者重试、推测 attempt 隔离、计算 Worker 退出后
可读、存储 Worker 故障后可读、Coordinator session 恢复、阻塞/流水读取、partition/range
读取、retention 和 cleanup 分别声明；无隐含能力，也不从存储介质或 PUSH/PULL 推导。
有效保证只能是实现、部署与本次 exchange 证据的交集。descriptor 是不可变值，不能充当
commit receipt、当前授权、reader lease 或缩容批准。没有旧类型兼容壳。

旧 FTE policy helper 的失败入口明确为 consumer failure，避免把输入可重读误当作 producer
输出可安全重放。它与重试校验器尚未进入生产 DAG，不构成已上线的任务级 FTE 保证。
原 SpoolingExchangeManager 已退役。Local spool 文件自身只能确认单一 attempt、partition 的完整性。
生产重试 owner 仍是 Stage/RetryStageHandler；不得把测试中的 helper 直接并行装配成第二个
权威。后续通用 session/receipt 必须连接该真实 owner，并验证部署和每次 exchange 的状态。


## Local 文件存储原语边界

`pangrova-shuffle-local` 拥有真实 LocalFs 文件实现及唯一 SpoolStoreFactory 服务注册；
SpoolStore/Reader/Writer/Factory/Context 归统一 API 制品的 `org.pangrova.shuffle.api.spool`。
Core 的 Java 页入口 `SpoolBackedExchangeManager.open` 接收装配方的类加载器，枚举工厂后按配置的精确 ID 创建后端。缺失、空白和重复 ID 在创建 Store 前报错。Manager 在关闭时释放 Store。Core 对 Local 后端只有测试依赖。

读取必须提供不可变 SpoolReadSelection，明确选择 producer → attempt；禁止选择最大 attempt、
将 long 截为 int，或把缺少的已选 segment 当作空数据。文件格式有 magic/version、逐批边界和
SHA-256、结束记录与总数检查；一次读最多一个有界批次。写完成只完成一个 attempt/partition
segment，不等于 exchange 发布。并发写同一 attempt 和覆盖已完成 segment 均拒绝。

目录有唯一 OS 文件锁 owner；读写资源存活时拒绝 cleanup，关闭后可重试。Store 关闭先处理
其资源再释放目录，异常保留并向上传播。新项目不读取旧 raw 拼接文件，不建设数据迁移或
双写工具。文件落盘、原子移动和重新打开文件均不构成节点故障、session 恢复或缩容保证。

Java spool exchange 明确接收选择器，并只接受其支持的批格式和阻塞读模式；失败读取不转为
正常 EOF，aborted sink 不允许伪成功 finish。该组件仍未接入 Native，存储键仍缺少完整的
application incarnation/exchange generation；通用 session、receipt、异步有界数据面和 Native
适配必须继续完成后，才能声明完整 Local Provider 支持。现有生命周期原型与 inventory 的
装配缺口不因文件实现拆分而消失。

### 自研 Shuffle Provider 与共享编码边界

Server Coordinator 在 REMOTE 模式下通过 `ShuffleCoordinatorFactory` 发现配置指定的工厂。工厂验证配置，创建进程专属的 `ShuffleCoordinatorSession`。QueryExecutionBootstrap 将会话契约注入 DAG，停止查询后由 Server 关闭会话。PangrovaShuffleCoordinator 通过 Provider 自有的服务描述符注册，Server 运行发行物装入该实现。其注册、发布、清理、task grant、gRPC 通道、mTLS 及响应校验都归 `pangrova-shuffle-pangrova`。DAG 通过 `ShuffleTaskAccess` 为绑定好的 Worker incarnation 和 attempt 申请短期授权。每次 grant 独立并发，关闭时先排空已受理的 grant。实现 `RecoverableShuffleControl` 的后端可接收本次执行的 `ShuffleLifecycleEvidence` journal。Generation 在当前 owner 与 allocation 下持久化经校验的不可变回执及原始无凭据响应。回调接口随 Shuffle API 发布，Provider 和 Coordination Control 只共同依赖契约，二者无编译依赖。

ExchangeTaskAttempt、ExchangeAttemptFence、ExchangeChannelContext 的实际中立身份归统一 Shuffle API
的 `.api.identity`；不复制身份、不修改字段或 fencing 校验。共享 producer-set 与 partition-manifest
摘要算法归 Protocol 的 `.shuffle.codec`，客户端、服务端与 Coordinator 共同使用，保持编码字节不变。
这类 canonical wire 编码不放入最小 API，也不要求 Provider 依赖业务 Core。

Pangrova 远端 reader 接收自身实现制品中的 PangrovaPartitionRead，内容为已选择的 channel 和预期
服务端发布证据。凭据在批边界续期。每个 source 的 gRPC 取消域包含续期和分区读取请求，关闭时先取消两种请求，再等待读取退出和 RPC 结束，最后归还通道租约。Producer 的取消域也覆盖写入、续期及提交确认，取消后等待活动调用退出。取消提交 RPC 时保留提交结果未知的语义。Worker 负责 Native 输入绑定。
Worker 通过 `worker.shuffle.provider` 选择 `NativeShuffleWorkerFactory`，只向选中的工厂传递其配置前缀。工厂校验参数并打开会话，Worker 在发布资源前核验部署实际启用的能力。每个 Worker generation 的生产与读取池共享通道总额和端点限额。NativeShuffleReaders 从已验证的外部绑定打开对应分区，由远端实现选择其端点并校验发布证据。NativeExchangeAttempts 在 Worker 和 Provider 之间统一编码、解码执行 owner、attempt fence 和 Worker incarnation。Worker 生产制品仅编译依赖 Shuffle 契约及 Native wire 边界，Server Worker 在实际发行时装入选定实现。Worker 在认证注册时报告实际的 mode 与 Provider ID，Coordinator 在成员可参与调度前核对部署。WorkerSnapshot 将部署信息与该进程 incarnation 一并写入 Memory 或 PostgreSQL，接管、恢复和心跳重建时继续核对。

PangrovaPartitionRead 是进程内读请求。Coordinator 发布使用 `ShufflePublication` 精确指定 query、stage、generation 和 allocation；过期请求在读取缓存或调用 RPC 前被拒绝。`ShuffleCommitReceipt` 保存确认的逻辑 producer 集合摘要以及每个输出分区的末序号、行数、字节数和内容摘要。Stage 在新 generation 清除旧 receipt。阻塞交换调度在上游回执可用后才释放下游，读绑定在同一锁内捕获路由、owner 与回执。每个远端读绑定携带 allocation、epoch、逻辑 producer 集合摘要，以及指定输出分区的末序号、行数、字节数和 manifest SHA-256。Java 与 Native 在准入时检查证据完整性。Worker 在向 Native 投递 chunk 前检查已发布的上界，在向 Native 确认完成前核对 Coordinator 回执、服务终止帧和本地重算的 manifest。空分区同样需要完整回执和终止帧。
远端读取的传输失败或内容损坏通过 Shuffle API 的 `exception.ShuffleReadException` 分类，Pangrova Provider 在自身的 `client.exception` 包构造后端错误。Native wire 的通道配额错误在 `nativewire.exception` 生成可重试的执行协议诊断；Worker 通过该类型报告实占通道数和限制。

当前远端服务确认逻辑 producer 集合，尚不能核验调用方选定的推测 attempt，因此装配时拒绝与推测执行同时启用。Direct 与 Local 各自使用原有 attempt 绑定。Shuffle API 读取及写出生命周期接口仅使用领域类型。当前任务传输与 Native wire 仍表达端点、提交地址与短期授权。使用其他传输布局的 Provider 需实现相应任务绑定及显式 producer attempt 选取。

持久化 journal 接收 allocation ID、Provider ID、正整数版本和最多 1 MiB 的不可变 payload。Generation 保存这个信封及共享的执行代、生产者集合和分区数。PangrovaAllocationCodec 的 v1 只编码分配 ID、提交地址和精确的分区范围，恢复时拒绝凭据字段、未知字段、范围冲突或非规范字节。Coordinator 通过配置的控制端点，以当前 owner 的 fence 申请该既有 allocation 的 finalize grant，再申请当前访问凭据并比对原始描述符。准备分区时，实际读写绑定通过同一个 Provider 编码器，在 PostgreSQL 取得当前权限与分区锁的事务中核对注册记录。

### 撤销虚假的 Local 发布与库存安全推断（2026-11-03）

SpoolingExchangeManager 与 LocalSpillLifecycleManager 没有生产调用，且无法从单个文件完成推导
整个 exchange 发布，本轮删除这两个重复原型及其假提交测试。真实 Local Provider 的 I/O、选择
attempt、ServiceLoader、关闭和 reader 清理保护保留并验证。Native LOCAL 复用有签名票据的 Worker 级磁盘交换。持久化 session/receipt 与节点退出后读取仍属未支持能力。

DAG 缺少外部生命周期时使用显式 UnavailableExternalShuffleLifecycle：注册和提交失败，清理仅因
不存在任何外部分配而不执行动作。Direct Native 的任务/输出资源仍由原 owner 清理。ShuffleHandle
LOCAL 不使用空端点表示注册成功。未知 stage 和过期 generation 的发布请求均拒绝，空输出分区在回执中有显式记录。

ShuffleDataInventory 取消基于列表和 retention 的默认缩容判断；具体 owner 必须提供完整且 fenced
的当前证据。未接通的 InMemoryShuffleDataInventory 只是诊断记录，返回 UNKNOWN，即使为空、被标记
已消费或介质为 persistent。安全缩容仍需要 Worker incarnation、关闭接纳、完整执行/reader 库存与
移除期间有效的证明；当前 WorkerManager 的 task-count 排空逻辑尚不能替代这些要求。


### Worker 注册身份撤销边界（2026-11-03）

Coordination API 拥有 WorkerRegistrationIdentity（logical ID、admission domain、incarnation），
Persistence 的 WorkerRegistry 定义按该身份原子注销。PostgreSQL 通过同一 DELETE 的三个条件实现，
Memory 在同一 per-key compute 比较；不得退回无条件 logical ID 删除或先查后删。HTTP wire 使用
已有 WorkerExecutionIdentity，由 Coordinator 转换，不把 Protocol 引入 Persistence API。

WorkerRegistrationClient 的作用域是一项进程注册尝试，首次发送前绑定原身份，重试及实际停机
DELETE 复用该身份。WorkerManager 的本地条件移除使用原始观察，后端另保护不同 incarnation。
这不替代完整的安全缩容、持久化接纳状态或物理资源 drain；既有 Shuffle inventory 缺口继续实施。


### Worker 心跳与自动修复的身份所有权（2026-11-03）

WorkerRegistry 的 heartbeat 必须携带完整 WorkerRegistrationIdentity，并返回原子条件更新后的
WorkerSnapshot 或 empty。Core 不得先发布本地成功再持久化，也不得从逻辑 ID 推测发送者身份。
缺失与替换通过 Coordination API 的 WorkerHeartbeatStatus 表达，由 HTTP 入口映射响应。

初次注册与自动修复分开：客户端首次发送前固定进程身份，后续请求只能调用条件刷新。PostgreSQL
在一条条件 ON CONFLICT 写入内核验身份，Memory 用单 key compute；禁止先读再无条件 upsert。
明确被替换的 Worker 通过现有 TaskAdmissionRegistry 永久关闭新准入/发布，Sender 不再自动恢复。
关闭准入不接管取消、Native handle、output reader 和 shutdown 的资源所有权，也不证明已经排空。

首次无条件注册的全局排序、持久化 DECOMMISSIONING、readiness/admission 状态整合和精确库存仍属
后续工作；不得据此批交付声称安全缩容或完整 Shuffle 生命周期已经完成。


### Metadata 共享编码边界（2026-11-03）

Metadata API/SPI 同制品，领域值不依赖持久化端口。Catalog Persistence API 依赖 Metadata API；
PostgreSQL Provider 不依赖 Metadata Catalog。原表元数据编码由缓存与 PostgreSQL 两条真实路径复用，
因此 `pangrova-metadata-codec` 拥有 `TableMetadataSerializer`；通用 JSON 读写直接复用 Common 的 `CanonicalJsonMapper`。
部署历史读取不得默认返回不存在；不支持历史查询必须显式拒绝，不能用 current 代替历史 revision。
失效清理由窄化端口注入领域调度器，Server 持有真实 Provider 的生命周期。
此边界不代表剩余 Metadata 跨域 workflow、Table 默认后端和 Governance 归属已经完成。

### Metadata 同步进度的领域端口（2026-11-03）

同步进度及诊断已从组合 MetadataStore 抽出到 Catalog Persistence 的 `synchronization` 契约，PostgreSQL Provider 由 Server 显式装配。进度绑定部署 incarnation/revision，仅允许来源声明的完整覆盖边界推进；本地结束时间和诊断历史不构成进度。原子完成、幂等回执与提交未知确认的边界由当前 Catalog Persistence 实现与测试约束。投影事务、其余组合 Store 和 Federation SPI 的拆分仍属后续工作。

### Catalog 部署与投影组合 Store 退役（2026-11-03）

组合 MetadataStore 及 PostgresMetadataStore 已退役，无转发壳。部署、表投影、schema 投影、同步进度分别通过独立端口注入；Server 选择 PostgreSQL Provider。领域 Core 不依赖后端辅助类，事务支持包通过 manifest 限制为制品内部使用。原 SQL 与逐操作 outbox 原子性保留；完整扫描的条件投影事务仍是后续工作，不能由这次职责拆分推导为已完成。

### Binary scalar and heap batch ownership

Types owns scalar binary snapshots; construction and raw-value access must not expose the owned byte array. SQL NULL uses the existing NullValue representation, while an empty binary value is valid. Batch API owns NativePage as an encoded heap snapshot, not a JNI handle. New payload reads fail after close; previously returned copies and size/format metadata remain valid. Consumers that need only the encoded size use the non-copying size query. These guarantees do not imply Native zero-copy buffers or completed Shuffle buffer-lease integration.


### Metadata 投影写事务与回执（2026-11-03）

`TableProjectionRepository`、`SchemaProjectionRepository` 收敛为只读端口。写入统一通过
`MetadataProjectionTransactions`，携带原始 handle 的完整 Catalog 部署身份和操作 ID。
PostgreSQL 在同一行锁及事务下校验部署、读取前值与 tenant impact、更新投影、写失效事件和回执。
同 ID 同内容返回原回执；不同内容拒绝；提交未知通过原 ID 确认，不能重新生成 ID 盲目重试。
V108 是统一空库初始化资源，不建设历史数据迁移。Server 负责选择该 Provider，Core 无 JDBC/SDK 泄漏。
这些约束替代上文“逐操作条件投影事务尚未实现”的历史状态，但整轮扫描、source version、同部署并行
同步与生命周期仍未完成。

### Metadata durable request recovery (2026-11-03 implementation update)

Catalog Persistence now durably accepts immutable projection and completion intents before
execution. The original IDs are recoverable by Catalog across service/provider recreation;
Metadata Catalog must recover pending work before observing a new source batch. New IDs cannot
bypass pending operations. Obsolete requests are rejected under the current deployment fence.
Projection/outbox/receipt and checkpoint/run receipt retain their separate atomic owners.
This closes the per-operation recovery barrier, not the remaining whole-scan ownership,
source-version, cancellation, event-recovery or retention work.
