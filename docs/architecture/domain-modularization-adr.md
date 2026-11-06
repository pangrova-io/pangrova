# ADR: 领域契约、实现与进程装配分离

状态：已接受，实施中。决定日期：2026-11-01。

本 ADR 落实本轮用户批准的《Pangrova 全仓模块化重构完整设计与实施计划》，
替代此前中央 SPI、合并领域 owner 和独立 SPI 制品的目标决策。
历史执行记录仍是对应批次的证据，不是本轮完成证明。
实际生效的模块边界以 `.pangrova/modules.yaml` 为准；未实施的目标不得写成现状。

## 决策

- 每个领域一个 `pangrova-<domain>-api`，同时拥有 `.api` 调用契约与 `.spi`
  实现扩展契约。没有扩展点就不建空 SPI 包。Connector 也使用 `connector-api`。
- 保留 `core` 模块和包名称。Core 拥有领域流程，Provider 拥有技术实现，Server
  拥有进程装配和生命周期。Core 不依赖可替换后端，契约不依赖 Core、Server 或 SDK。
- 基础类型属于 Types，批数据属于 Batch。它们不能通过 Connector 才能被引擎使用。
- 查询事件、统计观察值及监听器扩展契约归 Query API，进程内分发归 Query Core。
  Observability API 不反向依赖 Query；事件观察不拥有查询状态、持久化提交或重放保证。
- Query Core 独占查询状态转换、实际 Connector 操作作用域、终态物理清理与历史保留。
  Coordinator 的编排器借用这些内部实现；等待结果不能授予调用方完成状态机内部 Future 的权力。
  逻辑终态、Native 写完成、发布完成与物理 drain 分别记录，不以一个完成标志替代。
- 所有查询写入共用 Query API 的 WriteAuthorityRequest / WriteAuthorityResolver；普通写入与
  生成列写入都在分配前通过 Query Core 的 WriteAuthorityAdmission 校验真实当前授权回执。
  授权实现由 Server 在构造时固定，不能运行中 setter 替换；未配置时写入显式拒绝。
  生成列 Catalog 属性 codec 归 Generation Codec，Proto wire 类型仍归 Protocol。
  当前权限事务与提交 fencing 不因移动契约而改变；同步 resolver 不宣称独立取消或超时能力。
- StatementCategory 是 Query API 的纯观察值；AST 分类、精确目标解析与 CALL 参数绑定归
  Query Core，只依赖 Parser API 和值契约。权限入口与执行使用同一绑定，不分别解析目标。
  分类穷尽内建语句类型；分类、注册和真实支持状态分别验证，不能以 QUERY 默认值隐藏遗漏。
- 视图创建模式由 Connector API 明确区分 CREATE、IF_NOT_EXISTS、CREATE_OR_REPLACE，
  原子冲突判断由实现拥有。Security API 的 ViewAccessControl 定义当前 CREATE_VIEW/ALTER/DROP
  检查；创建权限不隐含覆盖权限。现有 Iceberg 视图是 catalog 实例内存状态，不宣称重启持久性。
- 租户成员 SQL 归 Server 入口适配，原子成员授权与写入归 Security Persistence。
  必须检查 exact ACTIVE tenant 的当前 OWNER/ADMIN，不能信任历史 session role 或修改
  allowedSchemas 来表示成员。成员授权不支持跨租户转移或撤销特权角色；SQL 无 NOOP 认证旁路。
- 领域绑定只接收其实际输入：检索绑定接收 vector/reranker plan，制品解析接收 artifact/merge intent，
  不反向依赖完整 Query/DAG Context。精确模型匹配归 Scheduler Core，消费调用方已确认活跃的
  capability inventory；Embedding Catalog 部署 fence 校验归 AI Control。模型身份、服务限制
  或实现不唯一时拒绝，不能按“最新”选择；Protocol 只拥有能力 wire identity 的结构比较。
- 查询执行范围与制品/模型选择意图由 Query Core 持有，借用 Execution 的明确 live/durable authority，
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
  其中真实 quorum 扩展放在同制品 `.spi.provider`。PostgreSQL 和 Memory 是独立制品，
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
- Coordinator 装配控制面，Worker 装配执行面。Coordinator 不依赖 Worker Core 或
  Native Bridge；Standalone 复用两种角色装配，不引入新的微服务。
- 插件工厂强类型化，启动时校验 ID、兼容性、角色、能力和作用域。缺失或失败显式拒绝，
  不静默切换 Memory。插件更新依靠重启，不支持热卸载。
- AQE 的倾斜检测通过 Shuffle API 的 SkewDetector 注入，Server 选择 builtin 实现。
  Planner 仅适配真实统计与决策，不复制检测算法、不依赖 Shuffle 实现制品。
  热分区识别、并行度启发式和实际 range split 是不同能力，不能相互替代支持声明。
- Shuffle 统一控制契约，独立 Native adapter，保留 Direct 的 C++ 数据面。
  显式表达 incarnation、exchange generation、producer/consumer attempt、seal、
  commit receipt、取消、背压和 buffer 所有权。容错保证必须由能力与部署共同确认。
- Celeborn 是未来 Provider；本轮以现有 Direct、Local、自研远端验证抽象，不创建
  Celeborn 空模块，也不宣称已集成。
- Native 保持 Java 控制、JNI 桥接、C++20 执行和单共享库发行。
- 新项目直接修改所有调用方，不保留旧 FQN 转发壳，不建设数据迁移、双写或旧库兼容。
  空库初始化唯一 owner 是 schema-migrations，已有 migration 序列保留，不清空用户库。

## 实施顺序及退出条件

| 阶段 | 范围 | 退出条件 |
| --- | --- | --- |
| P0 | 工作区基线、模块/FQN/能力/事务/资源台账、规则 | 当前模块有归属，例外有依据 |
| P1 | Types、Batch、Execution、Common 清理 | 基础契约最小 classpath；通用数据无 Connector 依赖 |
| P2 | Server、Transport、Plugin | 角色隔离；启动失败清理、关闭、Standalone 验证 |
| P3 | 各领域 API/SPI 同制品 | 中央 SPI、旧 FQN、重复服务描述符删除 |
| P4 | 分域 Persistence、JDBC support、统一初始化 | Core 不创建后端；空库和已有后端测试通过 |
| P5 | Metadata/Table/Generation/Materialization、workflow | 权限、CAS、fencing、outbox、恢复原子性不退化 |
| P6 | Parser、Planner、Protocol、Query、Scheduler | 规划离开 Protocol；Coordinator 不拥有业务实现 |
| P7 | 通用 Shuffle 及现有三种实现 | 共享契约测试与真实 Java/Native 执行链通过 |
| P8 | 其余领域、Provider、策略、codec | 实现、工厂、装配、调用、生命周期闭环 |
| P9 | Worker/Native、客户端、测试设施 | 跨语言资源、取消、多 Worker 和客户端验证 |
| P10 | 发行、旧类清理、最终审查 | clean 制品检查、完整回归、性能和逐文件证据 |

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

见 [实施记录](../internal/project-structure-refactoring/domain-modularization-progress.md)
及同目录 FQN 处置台账。该记录只给已执行事项标记完成。


## 已实施的 Execution 基础与共享编码边界

`pangrova-execution-api` 拥有 SDK 无关的 task stage/purpose/deadline；
`pangrova-execution-core` 拥有 canonical 请求/选项/split 绑定、授权上下文和精确 lease 身份。
输入不再依赖 Planner StageNode，Core 不依赖 Coordinator、Worker 或 PostgreSQL 实现。
该边界不接管调度、权限事务、lease 续租或 Native 资源。

`pangrova-shuffle-api` 拥有 Common/JDK-only 的领域契约；Planner 和 Operations 只依赖该
契约制品。原 ShuffleTransport 只实现 Native 服务绑定，现已从通用 API 删除。实际
NativeShuffleTransport 进入 `pangrova-shuffle-native-adapter`，通过任务所有者的受保护
命令通道操作借用的 executor handle；适配器不创建、关闭或替换 executor，不声明全局
close 能释放逐任务服务。Worker 任务所有者停止服务后释放 executor。Direct 字节仍在 C++。
统一制品内的 `org.pangrova.shuffle.spi.worker.ShuffleByteEgress` 定义真实的批数据交付、
生产者完成与中止回调，由已装配的远端实现使用。批数据完成不等于 producer commit，
producer commit 不等于 exchange 发布。调用方 buffer 仅在同步回调期间借用；rowCount
不是 batch sequence；任务和 query ID 只索引已授权的完整 attempt 绑定。
旧 onPartitionCommitted 原型与未装配的 DefaultShuffleByteEgress 已删除，不再宣称 Local、
Object 或 Celeborn 已通过该原型实现。通用 Provider/session/receipt、可替换 Worker admission、
异步 Writer/Reader 和 Local spool 生命周期仍待实施；目前同步交付 SPI 不替代这些完整契约。
Native 适配器分离与远端回调接通不代表最终 Celeborn 契约或三类 Provider 已全部完成。

`pangrova-shuffle-pangrova` 现拥有实际远端客户端、egress、读取、mTLS 和连接/配额生命周期，
包根为 `org.pangrova.shuffle.pangrova.client`，客户端命名为 PangrovaShuffleClient。它不依赖
Worker Core、Native Bridge、Coordinator 或 Planner；canonical batch 分块及校验码 codec 归
Protocol，wire 字节语义不变。Worker 的既有装配、Native admission 和 external ingress 调用
已切换到新制品；这些具体实现依赖仍需后续 session/reader 契约及 Server Worker 装配消除。
本次后端模块提取不代表 Core 与所有 Provider 的最终隔离已经完成，也不完成 Local Provider。

新增 `pangrova-security-codec` 是实际共享编码基础设施的独立边界：唯一拥有
DeferredAuthorizationScope 的规范编码与摘要，依赖 Security/Connector API 和 Jackson，
不依赖数据库或运行时 Core。编码从 PostgreSQL Provider 原样迁出，Security API
不承担复杂序列化，事务与当前权限判断仍归原 owner。

具体语义、调用方、未完成边界见
[契约基础与执行绑定证据](../internal/project-structure-refactoring/contract-foundations-ownership.md)。


## Shuffle 独立能力与有效保证

统一 API 制品的 `org.pangrova.shuffle.api.capability` 拥有 ShuffleCapability 和
ShuffleCapabilityDescriptor。消费者重试、生产者重试、推测 attempt 隔离、计算 Worker 退出后
可读、存储 Worker 故障后可读、Coordinator session 恢复、阻塞/流水读取、partition/range
读取、retention 和 cleanup 分别声明；无隐含能力，也不从存储介质或 PUSH/PULL 推导。
有效保证只能是实现、部署与本次 exchange 证据的交集。descriptor 是不可变值，不能充当
commit receipt、当前授权、reader lease 或缩容批准。没有旧类型兼容壳。

旧 FTE policy helper 的失败入口明确为 consumer failure，避免把输入可重读误当作 producer
输出可安全重放。它与重试校验器尚未进入生产 DAG，不构成已上线的任务级 FTE 保证。
SpoolingExchangeManager 没有选中 attempt、publication receipt 与 reader retention，故不再
接受外部传入的容错声明，始终报告没有已验证保证；这不表示完整 Local Provider 已实现。
生产重试 owner 仍是 Stage/RetryStageHandler；不得把测试中的 helper 直接并行装配成第二个
权威。后续通用 session/receipt 必须连接该真实 owner，并验证部署和每次 exchange 的状态。


## Local 文件存储原语边界

`pangrova-shuffle-local` 拥有真实 LocalFs 文件实现及唯一 SpoolStoreFactory 服务注册；
SpoolStore/Reader/Writer/Factory/Context 归统一 API 制品的 `org.pangrova.shuffle.spi.spool`。
Core 的生产依赖不包含 Local 后端。测试通过明确的 test 依赖使用真实后端，不扩大生产闭包。

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

### 自研 Shuffle Provider 与共享编码边界（2026-11-03）

ExchangeTaskAttempt、ExchangeAttemptFence、ExchangeChannelContext 的实际中立身份归统一 Shuffle API
的 `.api.identity`；不复制身份、不修改字段或 fencing 校验。共享 producer-set 与 partition-manifest
摘要算法归 Protocol 的 `.shuffle.codec`，客户端、服务端与 Coordinator 共同使用，保持编码字节不变。
这类 canonical wire 编码不放入最小 API，也不要求 Provider 依赖业务 Core。

Pangrova 远端 reader 接收自身实现制品中的 PangrovaPartitionRead，内容为已选择的 channel 和预期
服务端发布证据。凭据独立传入，在实际访问边界续期/验证；Worker 适配旧 Native 输入绑定。
该对象是进程内请求，不是通用 receipt、序列化 descriptor 或 Celeborn 契约。Generic API 不引入
固定 endpoint、commit address、Protocol/SDK 类型或 Native handle。Provider 和 Shuffle Service
均不再依赖 Shuffle Core；通用 session 与 Native 输入装配的后续重构仍需要完成。

### 撤销虚假的 Local 发布与库存安全推断（2026-11-03）

SpoolingExchangeManager 与 LocalSpillLifecycleManager 没有生产调用，且无法从单个文件完成推导
整个 exchange 发布，本轮删除这两个重复原型及其假提交测试。真实 Local Provider 的 I/O、选择
attempt、ServiceLoader、关闭和 reader 清理保护保留并验证；Local Native 接入和 session/receipt
仍须实际实现，不能把删除错误原型计作交付该能力。

DAG 缺少外部生命周期时使用显式 UnavailableExternalShuffleLifecycle：注册和提交失败，清理仅因
不存在任何外部分配而不执行动作。Direct Native 的任务/输出资源仍由原 owner 清理。ShuffleHandle
不再允许 LOCAL 空端点 sentinel，ShuffleCommitResult.EMPTY 仅是统计值，不是发布或未知 stage 的成功。

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
PostgreSQL Provider 不依赖 Metadata Core。原表元数据编码由缓存与 PostgreSQL 两条真实路径复用，
因此允许独立 `pangrova-metadata-codec`，符合“真实复用的编码基础设施”拆分原则。
部署历史读取不得默认返回不存在；不支持历史查询必须显式拒绝，不能用 current 代替历史 revision。
失效清理由窄化端口注入领域调度器，Server 持有真实 Provider 的生命周期。
此边界不代表剩余 Metadata 跨域 workflow、Table 默认后端和 Governance 归属已经完成。

### Metadata 同步进度的领域端口（2026-11-03）

同步进度及诊断已从组合 MetadataStore 抽出到 Catalog Persistence 的 `synchronization` 契约，PostgreSQL Provider 由 Server 显式装配。进度绑定部署 incarnation/revision，仅允许来源声明的完整覆盖边界推进；本地结束时间和诊断历史不构成进度。原子完成、幂等回执、提交未知确认及实际未完成边界见 [同步所有权记录](../internal/project-structure-refactoring/metadata-sync-ownership.md)。投影事务、其余组合 Store 和 Federation SPI 的拆分仍按目标计划继续。

### Catalog 部署与投影组合 Store 退役（2026-11-03）

组合 MetadataStore 及 PostgresMetadataStore 已退役，无转发壳。部署、表投影、schema 投影、同步进度分别通过独立端口注入；Server 选择 PostgreSQL Provider。领域 Core 不依赖后端辅助类，事务支持包通过 manifest 限制为制品内部使用。原 SQL 与逐操作 outbox 原子性保留；完整扫描的条件投影事务仍是后续工作，不能由这次职责拆分推导为已完成。详见 [本批能力边界](../internal/project-structure-refactoring/metadata-projection-ownership.md)。

### Binary scalar and heap batch ownership

Types owns scalar binary snapshots; construction and raw-value access must not expose the owned byte array. SQL NULL uses the existing NullValue representation, while an empty binary value is valid. Batch API owns NativePage as an encoded heap snapshot, not a JNI handle. New payload reads fail after close; previously returned copies and size/format metadata remain valid. Consumers that need only the encoded size use the non-copying size query. These guarantees do not imply Native zero-copy buffers or completed Shuffle buffer-lease integration.


### Metadata 投影写事务与回执（2026-11-03）

`TableProjectionRepository`、`SchemaProjectionRepository` 收敛为只读端口。写入统一通过
`MetadataProjectionTransactions`，携带原始 handle 的完整 Catalog 部署身份和操作 ID。
PostgreSQL 在同一行锁及事务下校验部署、读取前值与 tenant impact、更新投影、写失效事件和回执。
同 ID 同内容返回原回执；不同内容拒绝；提交未知通过原 ID 确认，不能重新生成 ID 盲目重试。
V108 是统一空库初始化资源，不建设历史数据迁移。Server 负责选择该 Provider，Core 无 JDBC/SDK 泄漏。
这些约束替代上文“逐操作条件投影事务尚未实现”的历史状态，但整轮扫描、source version、同部署并行
同步与生命周期仍未完成。详细证据与限制见 [事务所有权](../internal/project-structure-refactoring/projection-fence-ownership.md)。

### Metadata durable request recovery (2026-11-03 implementation update)

Catalog Persistence now durably accepts immutable projection and completion intents before
execution. The original IDs are recoverable by Catalog across service/provider recreation;
Metadata Core must recover pending work before observing a new source batch. New IDs cannot
bypass pending operations. Obsolete requests are rejected under the current deployment fence.
Projection/outbox/receipt and checkpoint/run receipt retain their separate atomic owners.
This closes the per-operation recovery barrier, not the remaining whole-scan ownership,
source-version, cancellation, event-recovery or retention work. See
[the implementation evidence](../internal/project-structure-refactoring/projection-recovery-ownership.md).
