# Indexing 模块边界

目标以 `docs/architecture/domain-modularization-adr.md` 为准。当前包含四个真实制品：

| 制品 | 当前所有权 |
|---|---|
| `pangrova-indexing-api` | 调用模型、策略及 payload serializer 扩展契约；生产依赖仅 Common、Connector API |
| `pangrova-indexing-codec` | 谓词编码、公共索引文件封装、CRC、类型编码辅助和 serializer 注册表 |
| `pangrova-indexing-strategies-builtin` | 九种现有 Java 策略及 payload serializer、值归一化、Bloom 预解析表示 |
| `pangrova-indexing-core` | 构建、评估、缓存、生命周期；依赖独立 Persistence 端口；Calcite 向量识别与 Native retrieval 规划绑定位于其 `planner` 包 |

Core 的生产依赖不包含 builtin 或 RoaringBitmap；Server Coordinator 选择 builtin
serializer 集合，策略服务描述符由 builtin 制品拥有。注册表拒绝重复类型。
后续仍需完成 Provider 的角色、配置、生命周期和能力模型，以及 Plugin Runtime
发现与显式装配的统一。不能把本次制品分离等同于这些目标全部完成。

`IndexStrategyProvider.prepareForEvaluation` 表达可选的只读预处理，默认返回原数据。
Core 评估服务在首次读取时调用它，并使用同一结果进行首次评估和缓存；Cache 不再
识别 Bloom。预处理必须保留类型、无假阴性语义、并发读取安全和完整保留内存估算。
Bloom 估算现在包含序列化数组、独立解析数组及对象开销估计。损坏数据不得进入缓存，
评估返回 UNKNOWN，取消和权限错误仍传播。没有承诺精确 JVM 对象布局或性能提升。

公共文件 magic、版本、类型 stable ID、schema hash、writer build ID、CRC 算法未改变。
预处理表示只在进程内使用，不传入 payload serializer。IndexDescriptor 的纯值状态转换
由 Core 拥有；持久化发布仍须通过当前权限、generation 和事务 fencing。

真实调用审查纠正了上一批结论：`NativeIndexHintConsumer` 和 `IndexEvalSplitFilter`
均无生产调用，连同旧 `IndexFileEvaluator`/factory、实现和注册一起退役。它们不能证明
Worker Java 过滤已启用。随机多 hint 聚合覆盖迁到 Core 评估服务，未将其宣称为端到端覆盖。
`IndexEvaluationService` 当前只有构造而无业务调用，保留为明确未接通的 Java 能力；
不为使覆盖率好看而接入重复执行路径。实际 Worker 提示链是 SplitFetcher 将提示写入
Proto，Worker 将任务提交 JNI，C++ VeloxTaskRunner 调用 IndexHintFilter。

后续工作包括持久化能力端口细分与默认语义收紧、Java 评估能力取舍、全角色装配与
未支持能力清单。实际验证和逐类处置以当前代码与测试为准。

编码数据所有权已收紧：Bloom/N-gram 输出可写快照或独立只读 ByteBuffer；builtin
评估使用只读视图。Bitmap EQ/IN 用定点 membership，R-tree 构建/编码用坐标访问；
快照修改不会改变原 payload。BitSet 与嵌套 postings 同样隔离输入/输出修改。
这些组件级闭环不改变上文 Java 评估未接通的支持状态，也不证明任意 Comparable
键的不可变性。缓冲区所有权由当前 Codec 与评估测试覆盖。

多列 BTree 键现在由 API 的 `CompositeIndexKey` 拥有，Codec 用 tag 11 编码完整元组；
原 Provider 私有键类已删除。元组仅接受文档列出的不可变标量类型，拒绝空、嵌套、
超限组件和不兼容形状。构建器校验页大小与行形状；空键集合缺少 NULL 覆盖信息，
不能直接判定 SKIP。真实 SQL 建索引与发布文件可读性独立验证，Native BTree 裁剪
仍未接通。详见当前 `CompositeIndexKey` 实现与测试。

持久化已从 Indexing Core 迁入两个实际制品：

- `pangrova-persistence-indexing-api`：现有存储端口、映射、generation manifest、发布请求与事件值。
- `pangrova-persistence-indexing-postgresql`：现有 descriptor、状态、映射、发布、队列、统计与 GC SQL。

Server 显式传入 JDBC API 的 `CurrentTableAuthorization`，PostgreSQL 发布事务在原位置
调用当前权限检查；参与者不能提交、回滚或关闭调用方连接。Provider 不再依赖 Metadata
Core 或 Indexing Core 的生产类。初始化仍唯一归 Schema Migrations，未新增 Memory 后端。
现有宽端口和可选默认方法保留为后续 P5 工作；本次隔离不宣称已完成能力端口细分。
详见当前持久化制品实现与测试。

PostgreSQL 批量选择使用固定 `ANY (?)`/`jsonb_exists_any` SQL 和 JDBC text array。
`JdbcTextSelection` 是后端私有资源 owner，按 ResultSet、Statement、Array、Connection
的顺序释放本次操作资源，DataSource 仍归装配层。generation/snapshot、发布前不可见与
1,000 条路径批次边界保持不变。实现与故障验证见当前 PostgreSQL Provider 测试。

Scalar 构建的取消意图归每次提交所有，CANCELLED 不能授权 generation 发布。
新提交不会清除旧请求的取消状态；读取、行迭代、编码与新文件写入前检查取消。
在途调用仍按协作式取消退出，持久化 claim fencing 和未发布文件完整回收仍待补齐。
取消边界见当前提交与评估实现。
