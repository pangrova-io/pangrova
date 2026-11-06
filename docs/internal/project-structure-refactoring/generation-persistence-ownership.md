# Generation Persistence 模块归属与实施证据

本批将通用 Persistence 剩余的 Generation 契约和 PostgreSQL 实现按领域归位，删除
`pangrova-persistence-api`、`pangrova-persistence-jdbc` 两个旧制品，不保留旧 FQN 壳。
不新增 Memory、数据库产品或历史数据迁移。schema 初始化仍由 Schema Migrations 唯一拥有。

## 契约与实现

| 制品 | 包根 | 实际职责 |
| --- | --- | --- |
| persistence-generation-api | `org.pangrova.persistence.generation.api` | 六个普通存储/错误契约，包含 job、stage execution、publication、exchange、当前 authority |
| persistence-jdbc-api | `org.pangrova.persistence.jdbc.api.generation` | 五个借用连接的 submission、inputs、authority、execution fence、publication candidates 协作端口 |
| persistence-generation-postgresql | `org.pangrova.persistence.generation.postgresql` | 18 个现有 SQL、映射、事务、lease、claim、completion、exchange evidence 实现 |

普通存储 API 不依赖 JDBC 协作、数据库驱动、codec、Core 或 Provider。JDBC 协作 API
显式依赖 Generation API；不能把 Generation Persistence API 反向接入形成循环。
Connector 的共享契约白名单从退役的宽泛 Persistence 根缩至实际 Generation API 根；
该更新没有把 JDBC、PostgreSQL 或 SDK 共享给插件。

## 状态、事务和资源

Generation 继续拥有 job/page/lease/publication 和自身 exchange evidence。物理对象仍归
对象存储，远端 Shuffle 数据的提交和保留不能由持久化记录的存在推导。

Job Store 的独立操作和 Stage/Exchange Store 借用 DataSource，创建并关闭各次操作连接；
显式 `Connection` 提交接口只参加调用者事务，不提交、回滚、关闭或替换连接。
辅助查询、插入、claim、completion、revocation 类只在同一事务内工作，没有新增状态 owner。
当前权限检查、执行 fence、job/page 锁、锁后权威时间和精确 attempt/winner 字节维持原顺序。

[逐类职责记录](generation-persistence-fqn-dispositions.tsv) 与
[调用引用](generation-persistence-callers.tsv) 区分普通调用、后端创建和 JDBC 参与者。
[源码证据](generation-persistence-semantic-evidence.json) 验证生产变更符合指定 FQN 替换；
这证明移动没有暗中改写 SQL/状态机，不等于全部功能已经语义审查完毕。

## 依赖与装配

根据实际 main/test imports 和装配核对，删除或下调 21 项不必要依赖：Coordination、Indexing
的 PostgreSQL 依赖退为测试范围，Worker/Query/Scheduler 等移除无用的 Generation 存储依赖。
Server Coordinator 保留显式选择后端的生产依赖；Metadata 仍经 `GenerationStores` 创建
Job/Stage/Exchange Store，并注入当前 authority、Query fence、当前角色读取器。

本批没有把 Metadata 冒充为最终 Core，也没有声称所有传递依赖隔离完成。
Metadata 仍依赖该 PostgreSQL Provider；Catalog export/generated value/publication 工作流和
领域事务参与者尚待迁至 Workflow API/PostgreSQL，实现 Server 最终装配。

## 验证与首次失败

近端旧路径基线 9 个用例通过，迁移后第一轮 9 个用例通过。初次全 reactor 消费者检查在
`JdbcApiClasspathTest` 发现新的 `GenerationPage` 泛型类型不在隔离 classloader 中，
7 PASS / 1 ERROR。补齐唯一缺失的 Generation API CodeSource，并新增禁止加载对应
PostgreSQL/codec 类型的断言；保持遍历全部 JDBC 契约签名。修正近端检查 10 PASS。

消费者全 reactor 检查完成 246 个用例：245 PASS / 1 FAIL；唯一失败为既有
`InMemoryShuffleDataInventory` 未接线门禁，未作豁免。Native smoke 和两个真实独立
Worker 的 Shuffle 测试均在该轮通过。

最终各批命令和结果见 [验证汇总](generation-persistence-validation-summary.json)。
静态门禁、包图、真实 JAR 与三个 launcher 的证据分别单独记录；选择测试不冒充全量回归。
Native、Proto、schema 和客户端输入未变，见 [输入证据](generation-persistence-input-evidence.json)。

## 下一批明确事项

- 两处 Generation 事务 helper 的 rollback 只捕获 SQLException，unchecked rollback 可能覆盖
  主异常；将另立故障复现和修复批次，避免与纯包移动混合。
- Metadata/Workflow 同连接业务操作和装配尚未拆完。
- 通用 Shuffle 的运行时接线、完整失败恢复以及原有未接线实现门禁仍待完成。
- 本批不宣称 Celeborn 实现、全平台验证、性能改善或整个重构达到生产验收。
