# Indexing Persistence 制品隔离与同连接权限参与者

本批执行全仓计划 P4 的已有后端隔离。以当前工作区 8,883 个文本文件快照为基线，
保留此前所有改动，不引入数据迁移、Memory 替身、兼容 FQN 或新的数据库。
最终验证结果由同目录 `index-persistence-validation-summary.json` 和逐测试记录给出。

## 所有权变化

新增两个真实 Maven 制品，父 POM、依赖管理、模块清单、基线同步更新：

| 制品 | 代码与所有权 |
|---|---|
| `pangrova-persistence-indexing-api` | 11 个现有生产类：宽存储端口、generation/发布/映射值、队列和缓存事件、纯选择校验辅助 |
| `pangrova-persistence-indexing-postgresql` | 13 个现有生产类：SQL、行映射、descriptor/state/mapping/publication/queue/statistics/GC 实现 |

共迁移 24 个生产文件、14 个测试文件。所有调用方直接切换到新 FQN；旧类不保留。
Core 中的 SchemaChangeEvent、SchemaChangeType、IndexSchemaEvolutionHandler 仍是
Schema 演进业务，不随持久化实现一起搬走。API 保留 `.api.metadata` 作为本批迁移边界；
后续能力端口拆分会按 descriptor/build/mapping/publication/cleanup 细分。
这不意味着全仓包命名与全部能力已达到最终目标。

Indexing Core 和 Coordination Core 仅依赖端口。Server Coordinator 拥有 PostgreSQL
选择及实例装配。PostgreSQL 的生产依赖不含 Indexing Core 或 Metadata Core；既有
跨领域集成测试显式保留 test-scope Core 依赖。契约 API 的最小 classpath 测试检查
全部类的公开签名可加载，且不能加载 Provider、Core、Jackson、PostgreSQL Driver 或 Hikari。

## JDBC 权限协作

迁移前，JdbcIndexDescriptorStore 直接调用 Metadata Core 的 TableControlDeferredPolicy，
会使后端反向依赖业务实现。本批新增 JDBC API 的 CurrentTableAuthorization 强类型参与者。
它是同后端协作契约，包含 Connection，不能进入后端无关的 Indexing Persistence API。

Server 在两个现有构造入口明确传入 TableControlDeferredPolicy::requireCurrent；
JdbcIndexMetadataStore 必须接收参与者，不提供单参数构造或默认放行实现。
Descriptor store 保存该参与者；Publication store 复用同一个 descriptor store。
这没有复制权限状态机或创建第二个事实 owner。

发布顺序保持为：取得发布权限锁 → 锁 descriptor → 当前权限检查 → 加载映射与现有发布
→ 校验 generation/快照/对象集合 → 幂等确认或原子发布 → commit。
失败由原事务 owner 回滚并关闭连接。参与者不得 commit、rollback、close、替换连接或修改
事务模式。租户和 exact-scope 校验保留原位置；历史构建观察不能代替当前授权。

新增真实 PostgreSQL 故障用例：参与者收到非 autocommit 的原发布连接，先执行真实权限
校验，再修改同事务内 descriptor，随后抛 SQLException。发布必须失败，中间修改和
发布记录同时不存在，原 descriptor 保持 BUILDING，连接关闭。另测构造时拒绝 null
参与者。它们补充已有权限撤销、发布幂等、generation 与状态 fencing 测试。

## SQL、资源和状态审查

| 类组 | 保留的责任与限制 |
|---|---|
| Descriptor/State | 身份、当前构建权限、generation reservation、条件状态更新和 claim；SQL 谓词与锁顺序不变 |
| Publication | 单一发布事务，完整映射验证、manifest digest、epoch、绑定、退役与激活；提交结果语义未改写 |
| FileMapping | 精确 generation 选择、批量映射、条件删除与原子 copy；不承担文件系统删除 |
| CommitQueue | journal/head 顺序、lease epoch、poll 与 ack；保持已有原子事务 |
| Statistics | snapshot 文件统计及 JDBC array 生命周期；不新建统计权威 |
| GC | 持久化清理观察、缓存事件与 breaker；对象清理调度仍归原业务 owner |
| Mapper/SQL/Support | JSON 与行绑定、局部 SQL、借用 DataSource；不关闭共享连接池 |
| API 值与 helper | 值校验、确定性选择；无数据库连接、SDK 或状态机 |

逐类旧/新 FQN、制品与职责见 [处置台账](index-persistence-fqn-dispositions.json)。
全部迁移类的 Java 字符串/text block 比较见 [字面量证据](index-persistence-literal-evidence.json)：
既有 SQL 和字面值不删除；本批只新增构造参数非空错误中的 authorization/descriptors。
字面量一致仅支持 SQL 未改写结论，不能替代上述事务审查和故障测试。

空库初始化仍归 Schema Migrations。后端没有新增自建 schema，数据库历史迁移不在本批范围。
Native、JNI、Proto 和初始化 SQL 输入没有变化；现有 Native 产物一致性及 Java 执行调用
在本批选定集成链中独立检查，不宣称本批重建或跨平台验证。

## 实施中发现的问题

1. 最小 classpath 测试中用于拒绝加载 Provider 的字符串曾被 POM 迁移脚本误判为依赖，
   形成 test-scope Maven 环；已删除错误依赖，保留负向加载断言。
2. 迁移后的发布测试使用 IndexDescriptorTransitions，首次 test-compile 缺少显式
   Indexing Core test 依赖；已补充 test-scope，未增加生产反向依赖。
3. 完整静态链首次被 GenerationOutput 已有多余空行阻断；仅修正空白。
4. 第二次在 Execution Core 遇到不可变 StageResourceGrant 返回值告警。原 SpotBugs
   例外仍使用迁移前 LeafTask 类名；更新到当前精确类/方法/字段/类型，并覆盖同义
   DmlWriteTask getter。边界测试验证改变 owner、method、参数、返回类型、字段、字段类型
   或告警均不会被放行；旧 FQN 也不再匹配。未屏蔽可变资源或整包告警。
5. 第三次静态链发现 GenerationSourceSplitCodec 既有公共 decode 缺少 Javadoc；补充原有
   拒绝缺失/未知 identity 的行为说明，没有改变解码实现。
6. 同步 IndexMetadataStore 的现有精确字段类型分类，避免移动后静态规则失效。

7. 完整静态链随后停在 Parser API 五处既有多余空行；诊断记录保留，不能宣称全依赖链通过。
8. 首次 clean reactor 由模块结构测试发现迁移后两个空 JDBC 包目录；删除空目录，
   保留门禁原要求后重跑。测试目录的空父包也同步清理。

9. 受影响模块静态验证发现 IndexDescriptorTransitions 五处既有超长行；仅换行，
   状态转换逻辑不变。随后 48 项选定测试通过，但 PostgreSQL 静态门禁报告三处
   SQL_INJECTION_JDBC：deleteExcludedMappings 的动态占位符、JdbcStoreSupport 查询转发、
   JdbcStoreSupport 更新转发。诊断涉及原有占位符/条件拼接，SQL 本批未改写；没有增加
   SQL 忽略规则，静态验收仍未通过，需要继续消除构造边界的不确定性。

10. 重新打包命令首次引用尚未存在的目标制品 pangrova-server-worker，在执行前失败。
    改为当前实际存在的 Server Coordinator 与 Standalone reactor 闭包后 install 成功；
    Worker 的未变制品来自已通过的 clean reactor。没有为修正命令创建空模块。

## 验证结果与证据

- 全 reactor `clean install` 选定回归：702 项全部通过，无跳过；包含整个 Indexing
  与新 Persistence 模块测试、架构属性、发布/执行 fencing、Server 装配和真实索引 SQL。
- 此后仅对 IndexDescriptorTransitions 作换行修正；相关状态转换与持久化测试 48 项
  全部通过。该轮静态验证退出 1，原因是上述三处 SQL 构造告警。
- 完整 `-am` 静态链也仍退出 1，停在 Parser API 五处既有空行错误。不会用局部测试
  通过代替完整静态门禁通过。分析器 missing-class 提示保留在分析证据中。
- 运行角色重新打包用于核对最终 classpath；打包阶段跳过重复测试，不计为新增通过测试。
- 最终核对 112 个 JAR、112 个唯一 Automatic-Module-Name、854 个包，未发现 split
  package、旧 FQN 或重复服务注册。8 个受影响制品的 948 个 class 与当前编译输出逐字节一致。
- 实际 launcher classpath 中 Coordinator/Standalone 各包含一个当前 Indexing Persistence
  API 和 PostgreSQL JAR；Worker 不携带两者。三处 Native 产物哈希一致。
- 63 个 Indexing/新 Persistence 测试源文件均有本批执行证据；八项仓库检查通过。
- 本批没有运行无筛选的生产全回归、跨平台 Native/ASan 或正式性能对比。

证据：[命令和结果](index-persistence-validation-summary.json)、
[逐测试记录](index-persistence-tests.tsv)、[最新测试记录](index-persistence-latest-selected-tests.tsv)、
[模块测试覆盖](index-persistence-owner-coverage.json)、
[完整静态链诊断](index-persistence-static4-analysis.json)、
[本批模块静态诊断](index-persistence-ownerstatic2-analysis.json)、
[仓库检查](index-persistence-final-checks.json)、[源文件处置](index-persistence-file-dispositions.tsv)、
[JAR 审查](index-persistence-artifacts.json)、[class 一致性](index-persistence-class-parity.json)、
[角色 classpath](index-persistence-runtime-classpath.json)、
[Native 产物](index-persistence-native-artifacts.json)。

## 本批边界与后续工作

本批不将宽 IndexMetadataStore 迁移等同于 P5 完成。原接口仍存在需要单独梳理的
claim/release/heartbeat、cache/statistics 默认行为、分页/时间过滤默认行为，以及
非原子默认 copy。必须逐项追踪调用与实现，再拆分强语义端口、切换调用、删除弱默认。
PostgreSQL 仍有 Security PostgreSQL 与 Metadata Codec 协作；应由后续明确 workflow
端口收敛，不能用本次参与者接口宣称完全后端中立。

Java IndexEvaluationService 尚无业务调用、Native BTree 裁剪未实现，均保持之前支持状态。
通用 Shuffle 生命周期/三后端真实装配、其余领域隔离、最终全量生产回归和性能验证仍未完成。
