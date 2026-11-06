# File PostgreSQL 事务失败清理

基线为本批开始时工作区 8,582 个文本文件，保留全部其他修改。逐一检查六个 Provider 的
事务 owner、调用者、装配和邻近测试后，本批只改变失败清理范围；不拆分原子操作。

| Owner | 原子范围与本批变化 |
| --- | --- |
| StorageProfileRegistry | create/drop 的授权 gate 移入回滚保护范围；当前定义与历史触发器保持同一事务 |
| FileSpaceRegistry | create/mutate/rename/drop 的 gate 移入保护范围；定义和目录失效仍原子提交 |
| FileSpaceAuthority | 共享授权 gate、callback 和 commit 异常由本地连接 owner 清理；借用的 TenantStore 不关闭 |
| FileDirectoryIndex | 本地 transaction helper 覆盖所有目录操作；保持 Stage→refresh 锁顺序及 pending/checkpoint/publication 原子性 |
| CopyFileClaimStore | claim 失败触发清理；保留明确的 URI 执行未接线状态，不宣称新增 SQL 支持 |
| CopySourceReadAdmissions | issue、frozen input、redeem、staged proof 四个操作统一错误清理；当前权限和执行 fence 顺序不变 |

以上路径现在处理 SQLException、RuntimeException 和 Error，并原样重抛 unchecked/Error。
SQL 继续使用原领域异常包装且保留 cause。回滚自身失败作为 suppressed 附加到原始异常，
防止 self-suppression；try-with-resources 负责关闭本次获取的连接。DataSource/pool 仍归装配。
没有重试、吞错、返回空成功或 Memory fallback。nonce finally 清零未改变。

Test Kit 的 JdbcMutationFault 仍只服务测试：在真正 executeLargeUpdate 等写入或
Statement.execute 授权锁成功后注入失败，也能让 rollback 抛 SQL/unchecked/Error。
真实物理 close 继续执行。两个测试类分别覆盖 91 个 File transaction 场景、28 个 COPY
admission 场景：对比失败前后完整持久化事实，之后运行新的正常操作验证锁和连接释放。
目录测试同时比较 pending、checkpoint 和已发布目录，File Space 变更覆盖失效级联，
Profile 测试包括不可变历史和授权 revision。

这证明被注入的提交前失败能够保留原错误并避免半写入；不证明网络断开、进程崩溃或提交
确认丢失时必定回滚。提交结果未知仍须由调用方按契约确认，不能自动重试或删除发布对象。
FileSpaceAuthority callback 不因此获得跨连接事务保证。本批也没有新增 admission 结果查询
API，不能据此宣称提交确认丢失后的恢复能力已完成。

修复前近端 91 / 48 PASS / 43 FAIL；COPY admission 28 / 16 PASS / 12 FAIL，均 0 ERROR/SKIP。
修复后近端 131 PASS（含全部 91 故障场景）；全 reactor clean package 选择范围 345 PASS，
包括全部 COPY 故障场景、共享夹具消费者及 File SQL 端到端检查，均 0 FAIL/ERROR/SKIP。
独立 HTTP/Native COPY 4 PASS；最新选择范围 349 PASS，0 FAIL/ERROR/SKIP。
8 项静态检查通过，826 个源码包零循环。最终 106 JAR、106 个唯一模块名、832 个编译包
审核零违规；三个角色 classpath 审核通过，Worker 不包含 File PostgreSQL，所有角色无
Testing Kit。修改的生产/fixture 两个制品共 62 个 class 与最终 target/classes 字节一致。
验证平台 macOS arm64、JDK 17、Maven 3.9.16、Docker；选择范围不等同于完整生产回归。

全仓重构尚未完成。既有 InMemoryShuffleDataInventory 接线问题、旧 Worker resolver、
Provider 工厂、Generation/Persistence/workflow、运行角色和通用 Shuffle 仍待完成。
本批没有修改 Native、Bridge、Proto、schema 或客户端输入；不宣称 ASan、性能或全平台通过。

证据：[类/所有权处置](file-transaction-fqn-dispositions.tsv)、[调用与装配](file-transaction-callers.tsv)、
[语义与源码哈希](file-transaction-semantic-evidence.json)、
[SQL/权限/提交顺序核验](file-transaction-success-path-invariants.json)、
[输入未变更](file-transaction-input-evidence.json)、[测试汇总](file-transaction-validation-summary.json)、
[逐测试](file-transaction-tests.tsv)、[包图](file-transaction-package-graph.txt)、
[静态检查](file-transaction-static-evidence.json)、[Native 哈希](file-transaction-native-artifacts.json)、
[制品](file-transaction-artifacts.json)、[class 字节比对](file-transaction-class-parity.json)、[角色 classpath](file-transaction-runtime-classpath.json)、
[文件处置](file-transaction-file-dispositions.tsv)。
