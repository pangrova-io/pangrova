# Generation 事务失败清理

本批在已完成的 Generation 持久化包迁移之后单独修复失败路径。
`PostgresGenerationJobStoreMappings.inTransaction` 和
`PostgresGenerationStageExchangeStore.transaction` 原先只捕获 rollback 的 SQLException。
当 rollback 抛出 RuntimeException 或 Error 时，业务/写入原异常会被覆盖。

两个 helper 现在保留主异常，将独立的回滚异常附加为 suppressed；同一个异常实例不重复
附加，避免 self-suppression。SQL 错误的 GenerationStorageException 包装和原有 error code
不变；操作连接仍由 try-with-resources 关闭，DataSource、authority 和执行 fence 均为借用。
没有改变权限、锁顺序、权威时间、SQL、提交点、正常返回值或 JDBC 协作契约。
[逐文件语义比对](generation-transaction-semantic-evidence.json) 对这两处修改作精确限定。

28 个真实 PostgreSQL 故障场景覆盖四个实际入口：

- job/page 整体插入：页面 SQL 已执行后失败，所有 cohort 事实必须回滚。
- 精确 partition claim：lease/attempt 更新后失败，重试仍领取原 attempt。
- exchange registration：写入 allocation 后失败，不能残留部分注册。
- exchange finalization：写入确认后失败，不能打开下游 barrier。

每个入口分别覆盖 SQL/runtime/Error 主异常、SQL/runtime/Error 回滚异常、同实例异常和
正常回滚。测试保存迁移后 schema 内所有 `artifact_*` 表的有序内容，比较失败前后，
再使用正常 DataSource 执行同一操作验证连接已释放且后续请求可前进。
测试共用原有真实 authority、Query execution owner 和不可变 stage graph fixture；
没有绕过权限或重建平行 schema。finalization 场景使用既有合成 producer report 作为
持久化测试输入，验证数据库 barrier，不将它作为真实 Native 执行证据；Native 验证来自
单独选择的执行链路测试。

修复前 28 个用例有 8 FAIL；修复后 28 PASS。首次失败和全部命令分别保留在
[验证汇总](generation-transaction-validation-summary.json) 与逐用例记录中。
更广消费者检查、制品和 launcher 证据独立记录。

测试注入点全部在 commit 前。明确不宣称验证了提交确认丢失、连接池失联恢复、所有
PostgreSQL 驱动故障或远端 Shuffle 可读性。Native、Proto、schema、客户端和 Shuffle
源码未变。原有 Shuffle 接线失败没有被本批修复或豁免；整个模块化计划仍未完成。

## 消费者验证发现的 HTTP 监听缺陷

第一轮消费者检查共 268 个用例，255 PASS / 2 FAIL / 11 ERROR。13 个失败均在 SQL 提交
阶段收到 HTTP 404，包含此前通过的 Native smoke 和双 Worker；数据库故障测试全部通过。
使用同一 classpath 的独立 Jersey 路由探针确认 QueryResource 模型存在且真实 GET/POST
可服务。随后核对监听发现，测试重建时使用的固定端口被本机其他进程的回环监听占用；
通配地址监听启用 SO_REUSEADDR，在 macOS 上仍能绑定成功，而 localhost 请求进入另一进程。
诊断没有关闭、修改或向那个外部进程发送控制命令。

`EmbeddedHttpServerPortOwnershipTest` 新增确定性复现：先持有真实回环 socket，再请求
通配地址固定端口绑定。旧代码 8 个近端用例中 1 FAIL。另一个 Coordinator 启动失败测试
复现 1 FAIL：绑定异常后已获取的 Jetty server/thread pool 没有自动停止。

本批追加两个必要修复：

- `EmbeddedHttpServerFactory` 对通配/未解析地址关闭 SO_REUSEADDR；明确的单一地址
  保留 TCP 状态复用。两类地址都不启用 SO_REUSEPORT；同一明确地址的并发监听必须失败。
- `CoordinatorHttpServer.start` 在部分启动失败时停止资源并保留原异常；未启动的 context
  直接停止，不进入 graceful drain。正常停止的所有权仍归 HTTP server。

最初对所有地址禁用复用的修复使 9 个近端用例通过，但后续 HTTP 检查 66 个用例中
10 ERROR：Worker 的固定端点被旧 TCP 状态占用，刷新失败后其他用例连带不能 dispatch。
这个中间失败完整保留，没有增加等待或自动重试掩盖。最终按通配/明确接口区分策略，
SqlE2ETestBase 的 Coordinator 明确监听回环，Worker 明确监听已发布的本机地址。
新增真实 HTTP 请求后同地址重启测试，以及同地址重复监听拒绝测试；最终 11 个近端
用例通过，验证正常恢复能力。

通配地址不承诺立即可重用；冲突时生产启动显式报错，测试设施已有的动态重绑策略
可在失败清理后选择新端口。生产代码没有增加静默换端口或后端回退。
该策略覆盖共用 Jetty HTTP factory；不把结果泛化为所有 Native/Flight/TLS socket 已审查。

受影响 HTTP/认证、文件 URL、Generation E2E、Native smoke 与双 Worker 链路另行验证；
COPY Native owner 在单独 Maven/JVM 中运行，避免用测试顺序掩盖跨语言生命周期问题。

最终选择验证汇总：309 PASS / 0 FAIL/ERROR/SKIP；各次失败保留在逐轮记录。
最终 JAR 审核 106 个制品、106 个唯一模块名、832 个编译包，无违规；三个修改制品
190 个 class 与最终编译输出一致。三个运行角色均使用当前产物。
这项结果不包含对既有 Shuffle 未接线问题的修复，也不替代全量生产回归或性能验收。
