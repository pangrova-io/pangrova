---
title: "Pangrova 执行链路一次性重构与清理计划"
description: "以同版本发布为前提，对 SQL 主链路逐段验证并收敛为唯一生产路径"
unlisted: true
---

# Pangrova 执行链路一次性重构与清理计划

项目可以使用同版本 Coordinator、Worker、Native bundle、Shuffle Service、CLI、JDBC 和 benchmark 一起切换。删除只为过渡服务的协议、开关、路由和后台任务。上一轮的兼容入口、混部、双格式和迁移安排由本计划替代。底线仍是授权、快照一致性、精确 attempt、可恢复提交与资源实际归还。读 SQL、写 SQL、DDL 和 Java 执行的 CDC 是不同业务路径，各自应有明确入口和共同的任务生命周期。

待实施工作先收敛 durable acceptance、owner routing 和 result disposition，再推进 production 归因、扫描输入、动态过滤、AQE、持久 Shuffle、结果与写入恢复。已验证的协议收敛、数据库去重和所有权修复集中记入[验证记录](#验证记录与覆盖边界)。原始调用链和基线见[逐节点协议与数据库审查](./end-to-end-protocol-optimization.md)及[微测与线程分析](./performance-remediation-plan.md)。

查询性能对齐的对标引擎、扫描权重反例、跨 Stage 动态过滤、阶段聚合缺口和性能验收，见 [查询性能对齐与执行引擎修正计划](./query-performance-parity-plan.md)。

## 2026-11-27 代码审计与剩余工作 {#remaining-work}

当前仍有 23 个待关闭条目。A 表列出 14 个实施工作包及各自的剩余事项，其中已有部分工作包进入验收。B 表列出 9 个专项生产验收。两个表中的部分事项可由同一组跨主机或故障测试提供证据。C 表保留历史文档勘误。

### A 实施工作包与剩余事项

| ID | 当前事实与缺口 | 实施工作包 |
|---|---|---|
| A1 | HTTP 提交共用持久受理，未提供幂等键时按新查询生成 journal 身份。principal、key digest、canonical fingerprint、lineage 和 `QUEUED`/`PENDING` 回执原子写入，正常新受理一次 INSERT；冲突和未知提交读取权威行。分支选择通过成功终态和恢复回执发布。已通过本地真实 PostgreSQL 回执恢复、无键写入和双 Worker 链路。待补跨主机多 Coordinator、Worker 故障组合 | WP1.1 |
| A2 | 已有原子状态版本、签名回执转发和精确 Worker 派发凭证。普通 DAG、投机副本、失败重派和 Worker CDC 在发送前提交已绑定的派发凭证。接管方分批核验并按当前租约持久化匹配的退出回执。单查询最多核验一页，全局最多四页并行，等待 HTTP 不占清理线程。仍存活的原 Worker 为已过期且没有存续执行者的精确尝试出具退出回执，并禁止迟到重放和时钟回拨重新准入。PostgreSQL 将持久租户槽位与执行租约同事务准入，并在本地实际清理后同事务退还。已经取得租约但尚未写入准入 journal 的执行也可在接管后归还槽位。接管批次只读取有 journal 证据的执行 ID。已派发的运行中 SQL 在完整退出证明后原子落下终态，已发布写入保留成功语义。查询阶段与 Worker 非终态进度使用实时投影，已移除这两类 journal 写入。终态、派发凭证和接管退出回执仍持久化，终态写入与 journal 删除按原执行租约验证归属，远端 Shuffle 清理后同事务归还快照 pin、租约和槽位。普通 SQL 的 Shuffle 删除由实际排空后的终态清理触发，暖路径复用现有能力票据，无 scope 时通过 PostgreSQL 直接消除意图，已去掉空闲扫描。客户端用原签名回执在冷路径恢复已物理排空的终态，正常状态查询仍走内存投影。接管方可释放已受理但尚未取得 Worker 或 Coordinator 本地执行权的查询。本地语句在执行前按租约登记，Coordinator 本地 Flight 直接发送 Arrow 批次，缓冲和读取者纳入进程内排空。同一持久目录的替代进程取得独占锁后可证明原进程退出，并启用既有的物理回收。未取得证明的执行不进入到期认领批次。仍在运行的前任可在本地 Flight 和执行资源实际排空后凭原始令牌写入退出证明，即使续租已移除本地缓存。收尾事务开始前租约到期时，接管方凭租约行上的退出证明继续回收。本地排空与对外 DRAINED 分开发布。接管事务提交后才发布 DRAINED，仍持有投影的前任可在认证状态请求中核对租约和槽位归还。正常收尾在同一事务中清理日志、释放租约并归还槽位。到期或待提交的执行保留接管日志。已删除重建查询状态和任务跟踪的旧重放链路。物化刷新在注册前冻结 START 血缘，目录解析失败后会记录一次终态并排空，避免阻塞 Coordinator 关闭。剩余工作包括更多独立进程故障和跨主机验收 | WP1.2 |
| A3 | WP1.3 已引入显式 `PENDING` / `STREAM` / `NONE`，并贯通 dispatch、状态机、REST、JDBC、CLI 和 benchmark；本地独立 CLI、真实双 Worker 与三种连接器写入恢复验证了无结果端点、结果流和持久终态回执。跨主机 resultless/延迟发布 E2E 仍待验收 | WP1.3，本地验证后提交；生产验收待续 |
| A4 | 生产已记录派发阶段、准备排队与运行时长、准备结果、胜出超时和本地释放时间。HTTP 持久受理的提交跨度结束于受理回执，后台执行沿用已捕获的追踪上下文。持久受理路径直接排队，复用原受理回执。排队取消后可即时释放，已进入派发的回调持有独立执行操作直至退出。在有界执行器和固定 17 ms 排队时钟下验证派发等待，以及队列满、执行器停机、入口封口和租户限额的分别计量；仅启动的回调产生排队样本。全局物理排空计数在租约与租户槽位事务确认后按本地和接管路径记录，复用事务结果，不新增 SQL。真实 PostgreSQL 验证未排空、旧租约和重复确认不计数。Coordinator 生产包已删除退役的加权派发器，启动时使用配置指定的放置策略。租户准入直接读取本地租户计数，离开 QUEUED 时归还排队计数，本地物理清理后归还并发计数。成员关系、租户状态和跨节点槽位仍由现有权威校验。剩余工作包括跨主机混合容量矩阵及全局排空尾延迟测量 | WP2 |
| A5 | Paimon 使用阶段剩余 split 容量和绝对截止时间。直接追加 SQL 对每个快照单遍校验并逐个读取 manifest，普通已绑定扫描复用首次校验取得的快照。普通扫描、内部增量和直接追加在 FileIO 打开前核验源的 manifest 配额，并共用阶段的 256 MiB 编码元数据额度。计划退出后保留已进入的 I/O 所有权直至实际关闭。普通扫描和内部增量按源提交最多 32 个 manifest Future，已加入跨窗口的删除合并及受阻读取回归。待统一运行这些回归，再补对象存储故障注入，并校准阶段字节额度和跨主机混合负载并发度 | WP3.2 |
| A6 | 实时扫描已按绑定元数据估算任务工作量。Worker 放置考虑当前签发窗口、心跳和本 Coordinator 的精确在途任务，报告物理释放或派发前回滚后扣除。持久分片按现有的最多八个分片窗口共同认领，合并权威、锁定交换证据和页面选择，Worker 候选集在事务前按窗口筛选。真实 PostgreSQL 对照逐个认领和八分片窗口，执行权威、锁定交换证据、精确页面、split 清单和依赖查询分别由 8 次降到 1 次，已运行作业无需重复更新。发布清单和 pin 按窗口复用，源对象和校验值按窗口内的来源集合批读。两份不同来源的实际分片验证了对象隔离、各次发布读取各执行一次，并验证源校验失败时整窗不消耗尝试。不可认领和请求构建失败也未消耗其他分片的尝试。九页持久作业幂等重试实测分片分配、split 和生产者依赖各读一次，依赖变序和 split 校验值损坏均被拒绝。Producer 终态实测阶段合同、直接角色和锁定作业各读一次，等待锁后再次检查执行租约。持久分片初次放置使用冻结 split 和已发布交换字节，计入同窗口已分配工作、本 Coordinator 的精确在途任务及 Worker 心跳。同一派发阶段复用集群可用容量总量，各分片直接核对选中 Worker 的进程身份、健康和当前内存，避免逐分片重扫 Worker 全表。DAG 在构建时索引入边和出边，计划准备按 Stage 直接定位边；已验证的交换输入 schema 与生产者完全一致时复用 Substrait 树。已加入相同 schema 以及不一致 producer 计划的回归。同一查询的 Calcite 表副本如共享精确绑定，分片枚举前及派发前各验权一次。已加入表副本和不同快照拒绝的回归。剩余工作包括多 Coordinator 的在途估计，以及按存储、格式、冷热和并发校准估计并验证尾时延 | WP3.3 |
| A7 | Native Parquet 将 `DataSplit` 字节区间应用到行组读取，并在文件边界核验区间。位置删除使用绝对行号。Lance 使用物理行区间或固定 manifest 分区，Paimon merge 要求整个主文件。实时 Iceberg SQL 并行扫描按 manifest `split_offsets` 分片，沿用已有快照字节统计进行阶段估算，并按原始对象共享标量索引判断。 分片携带同一快照的对象总长度，Native 校验范围后直接交给 S3 reader 读取，避免每个分片重复 HEAD。待统一验证协议、范围及真实对象请求次数。剩余验收包括跨主机对象存储 I/O、含大删除文件的成本校准、受保护产物的全链路整对象身份与真实 Apache ORC reader | WP3.4 |
| A8 | Native 文件适配层的保留字节、返回字节、数据调用、元数据调用和失败调用已贯通 TaskReport 与 Profile。配置后的 Velox S3 读取用现有任务 I/O 所有权记录 SDK 尝试、重试和已接收 HTTP body 字节。已加入独立 Native 进程和真实 MinIO 的回归，验证同一文件的 HEAD 与读取量，待统一运行。标准 Iceberg S3 扫描与 Paimon S3 读取已接入 Worker 的逐目录凭证，Native 按租户、目录版本和凭证隔离文件系统及缓存，元数据与删除附件沿同一仓库授权读取。已加入复用同一地址但凭证不同的拒绝场景。待统一验证标准 SQL 路径、跨主机、缓存、故障注入及其他 SDK 的请求量。HTTP 头和传输帧没有计入 body 字节 | WP3.1 |
| A9 | Iceberg 和 Paimon 实时扫描可由同一个构建侧最多四个 INTEGER 或 BIGINT 键分别生成范围过滤。每个查询可激活最多四组互不共享 Stage 的构建与探测组合，共用 200 ms 等待期限，各自发布范围和 Profile 决策。探测放行与完整构建摘要分开，放行后到达的摘要可在实际 split 枚举开始前按精确快照和字段身份参与裁剪。Coordinator 在构建侧获胜交换发布及探测任务 RUNNING 两个边界触发晚到投递。Worker 使用同一个有界 Protobuf 绑定校验任务尝试和唯一物理扫描。已加入独立双 Worker 受控晚到过滤对照，待统一验证两侧实际尝试、Native 确认、结果、读量及物理释放。已加入有符号 INTEGER 在 REMOTE Shuffle 的结果与 Native 范围回归。Paimon 合并离散范围并复用已绑定快照统计，已加入真实分区枚举回归。其他键类型、连接器、共享 Stage 的组合及跨主机负载校准仍待补 | WP4 |
| A10 | 实时单输入范围合并已存在。普通等值 Join 按已绑定扫描的统计建立两侧哈希分区。符合键顺序、类型和分区数约束的实时双输入 HASH Join 复用同一范围映射，两侧各自保留提交证明，并按合计字节分组。本地独立双 Worker 的 FULL OUTER JOIN 已核验 4 → 1 个消费任务、全部 8 行及物理排空。REMOTE 的选择性等值 Join 固定生成两侧 HASH，已补充实际 SQL 回归，待统一验证。 已删除只生成建议的 Coordinator AQE 路径和 JSON Shuffle 统计编码，生产范围合并直接读取提交回执。待补持久重放映射、可执行 skew split、更复杂输入形态和跨主机负载验收 | WP5 |
| A11 | Shuffle 已有 lease/window/fence。主动与接管清理在单条 PostgreSQL 语句中按同一时点检查读 pin、写租约及晚到写入证据。对象清理从 PostgreSQL 只取已封口的精确 Shuffle 前缀，由强一致对象列表统一回收已发布、未登记的对象及 manifest，移除了逐批读 PostgreSQL 清单。部分尾页直接完成。已补 SQL 计数和 MinIO 跨页用例。过期清理接管从同一锁定行取得原租约期限，移除选中后对同一 scope 的重复点查。首次封口一次性更新分片，读排空后认领及过期接管均无需再次更新分片状态。清理确认在同一事务删除 scope 并写入收敛时间，封口保留到执行租约退役，恢复退役要求正向收敛证据。撤销代次绑定首次封口的执行令牌，协议回执绑定当前所有者并复用持久收敛时间。首次认领和过期接管均移除用于日志的全量 batch 聚合。读窗口续租由 3 条 SQL 降至 2 条，读流闭合由 2 条降至 1 条。过期写窗口和 manifest PUT 的精确保留记录独立于 scope，补偿清扫认领从 2 条 SQL 降到 1 条。全部确认后以一次完整的前缀清扫停止该前缀轮询。剩余工作包括进程崩溃后没有写入确认的收敛证据，以及多实例、跨主机故障验收 | WP6 |
| A12 | 普通分布式写入删除 redo 后保留带 fingerprint 的终态回执，COPY、CDC 和外部 publication 使用现有写入声明与版本字段。COPY 的不可变回执在一条 SQL 内读取或插入；仅并发可见性冲突或未知提交再读取精确回执。已加入同一回执并发重放用例。生产 CDC 由 journaled runtime 负责发现和恢复，Coordinator 启动和 DDL 已移除旧调度链。CREATE STREAM 单次持久化完整 RUNNING 定义。MAINTENANCE 暂存使用本地领导任期与任务期限，权威点查保留在受理、journal、目录发布和 offset 确认边界。CDC 发现只读 checkpoint 所需的精确来源与写入身份，实际重放才读取写入绑定和片段。同一领导任期和流版本中，末端偏移未变的空闲轮次跳过 checkpoint 与已提交 offset 读取。每轮 Kafka 观测由判空与任务规划共享。新 MAINTENANCE 批次直接申请执行租约，恢复沿用精确存续租约。CDC offset 确认把精确流版本锁与批量推进合为一条 SQL，正常提交和恢复确认均移除先前的流定义点查。表格导出发布复用同一事务已授权的阶段图，以一次查询校验分片完成状态、分配和输出覆盖。剩余工作包括收敛全局 RUNNING 流发现的周期读、验证各来源重放窗口和回执回收条件，以及跨主机故障与高并发验收 | WP8 |
| A13 | 写入 journal 已将确认未发布的精确 fingerprint 固化为 `ABORTED`，保留不确定提交的挂起记录和隔离证据。写入绑定仅重建一次即校验完整合同，接管请求终止证明时复用已核对的句柄和目录实例。Paimon 复用已解码写绑定与物理发布的表观测，回放通知使用已匹配的准确快照。有基准快照时直接校验该快照。移除 Iceberg/Paimon 从当前目录构造恢复句柄的旧 SPI 和实现，重放测试直接从 journal 的原始绑定解码。Iceberg 写分配复用已加载的目录指针，已加入 PostgreSQL 语句计数断言。已删除零调用方的 Paimon Native 文件删除接口。生产 Coordinator 已解除旧 CDC 调度、两阶段提交存储和页写入工厂接线。旧调度类和配置、旧页写入器及三参数暂存删除合同均已删除。现行回归覆盖单个绑定写入器的 CDC 行顺序和暂存失败清理。正常 journaled 恢复需要目录级终止证明，Paimon 尚无该证明。待在首次 SDK 发布前原子记录精确的发布权与终止权，或由目录提交端提供不可逆的未发布证明。持久化终止证明后仍需独立的对象清理认领和完成回执，并验收跨进程接管与部分删除故障 | WP8 |
| A14 | nightly TPC 已存在；release 不校验同源 Linux/TPC 不可变证据，SF100 零值基线仍不合格 | WP9，需先批准 CI/基础设施变更 |

本地恢复对照在同一个清理线程上阻塞一个 Worker 响应。原实现的独立查询在 5 秒验证窗口内无法持久化回执。候选在该响应仍未返回时持久化独立查询回执。测试同时验证全局四页上限、同查询新旧租约不重叠，以及已保留回执不再读回。跨主机吞吐和恢复尾延迟仍在 B3/B7 验收范围。

租户槽位与租约验证使用真实 PostgreSQL 连接断开。准入在创建 anchor 前断开，退还在删除槽位行前断开，两次均验证事务成对回滚。24 次并发准入竞争 4 个槽位，仅 4 次成功且释放后计数归零。两个经签名 OIDC 鉴权的 Coordinator 共享单槽位。首个查询在写准入 journal 前被阻塞，另一实例返回 HTTP 429。首个查询经 Flight 消费后验证槽位归还和查询物理排空。

租户占用查找用 JDK 17 在同一进程对照原扫描与新索引。两个被测租户各有四条活跃查询，其他租户有 65,536 条已物理清理的留存记录。七轮热态的每次读取耗时中位数从约 1.77 ms 降至约 4 ns。实验仅覆盖本地占用查找。64 条并发注册和取消验证同租户计数，终态留存、接管确认和重复清理均未重复扣减。

真实 Coordinator/PostgreSQL 在 Worker 控制回应 HTTP 202 时保留已派发查询的任务、租约与槽位。匹配退出回执后，三者归零。真实 PostgreSQL 验证缺少派发事实或租约换代均不能确认排空。运行中 SQL 同样等待完整退出证明，旧查询在接管后不能重建快照 pin。PostgreSQL/MinIO 的 Shuffle 服务验收在证明前拒绝清理授权，在证明后删除对象并验证重复清理不再释放字节。受控 Worker HTTP 先返回 202，再返回来自不同 Worker 实例的 200 退出回执。真实 Coordinator 在两种响应下都保留任务、租约和槽位，只在收到匹配回执后释放。运行中和已失败的前任均不重建内存查询，同名但实例标识不同的活动租约与槽位保持原状。此组验证没有终止独立 Coordinator 进程。

PostgreSQL 接管分类用例预置 4,096 条其他查询日志和 4,096 条其他任务日志。空认领批次不访问数据库；4 个候选 ID 的批次执行 1 条语句，只返回有证据的 3 个 ID。查询日志保留精确点查接口，实际接管通过分批退出回执完成。

真实 Worker Native 与 Flight 的查询回归使用已启动的 Coordinator 执行租约和 PostgreSQL 查询日志。服务端语句计数记录一次合并收尾，独立的日志清理验权 SELECT 为零。读取结果后验证状态已排空。Iceberg INSERT、CTAS 与读回一并通过。

签名回执恢复验收由两个真实 Coordinator、OIDC、PostgreSQL 和 Flight 共同完成。正常版本状态和无效签名请求均为零次 journal 读取。持有效回执时，未物理排空返回待恢复，原 Coordinator 停止且租约删除后返回原查询的终态，每次恢复请求一条 SQL。已消费的 Flight 结果可完成终态确认。未消费但已无法取回的流报告结果不可用，客户端保留原查询 ID，不重新执行 SQL。已保存的恢复终态直接供后续状态和取消判断使用。独立 Coordinator 进程强制退出和跨主机验收仍在 B1/B6 范围。

实际 QUERY Coordinator 接管仅有受理记录的执行时，写入失败终态并归还其执行租约和租户槽位。已取得本地语句执行权的查询继续持有槽位。PostgreSQL 回归验证同一工作权只授予一次，过期前任及接管租约均无法重新启动本地语句。OIDC 与 Flight 联合用例实测本地语句只发出一次工作权 UPDATE，实时等待仍没有增加 journal 读取。Coordinator 本地 Flight 已移除中间 IPC 字节、重复解码和批次填充线程。每个批次只授予一个读取者，实际关闭 Arrow 分配后才释放查询操作。关闭 Coordinator 时撤销未领取批次，已开始的读仍保有分配。实际 TCP 发送暂停时，Arrow 分配与查询操作均保持占用，恢复发送后归零。真实双 Coordinator 与 PostgreSQL 验证已完成但未读取的查询仍占用槽位，读取结束或正常关停后归还。mTLS 和 SQL `SHOW VIEWS` 经真实 Flight 验证。接管方仍需独立进程退出证据。

私有 COPY 准备接口的本地回归验证了凭证拒绝、票据精确版本暂存，以及读取任务重启后从受管对象读取同一份 Parquet 字节。夹具使用实际 HTTP、PostgreSQL、对象存储和 Worker Native 读取器。

### B 待补生产验收

| ID | 已存在机制 | 尚缺证据 |
|---|---|---|
| B1 | journal conflict reconciliation、execution anchor fencing、恢复 | 双 Coordinator 并发、未知提交、owner incarnation 与真实 PostgreSQL 故障组合 |
| B2 | `QueryStateMachine` 原子 version/waiter/drain；HTTP 使用生产指标 provider 按目标和结束原因记录资源占用，超时、断连和状态更新仅归还一次。已加入真实 HTTP 的逻辑终态、物理排空归因、租户及 principal 上限，以及停机拒绝和断连释放的用例 | 待统一运行回归，并补跨进程故障流量验收 |
| B3 | callback 隔离、query-order/admission、阶段准备和 durable commit 容量 | 固定到达率叠加阻塞目录、慢对象存储和终态报告压力 |
| B4 | identity projection 移除、local aggregate reduction | 真实聚合 CPU、首批/完成时延、跨 Worker 与 benchmark 结果 |
| B5 | 实时单输入与对齐双输入 AQE 的实际范围映射已验证 | 不同 Join/NULL/重复键/重试/混合负载的阈值与尾时延校准 |
| B6 | Native result ownership、byte credit、explicit EOF/drain、early-close failure；Flight 每批撤权检查不再读取带锁的完整所有权快照。成功关闭出口时将读取者首批、生产端 EOF 和传输结束写入精确 OUTPUT Profile。已加入空结果、正常结果、异常退出、协议验证及真实双 Worker SQL 断言 | 待统一运行回归，再补跨主机吞吐、soak、完整 ASan/UBSan、客户端消费与全资源排空分段验收 |
| B7 | Shuffle read/write window、atomic bounded publication、零逐 batch 授权 SQL | 真实 PostgreSQL 计数、热点、owner 切换、对象清理和并发读取 |
| B8 | fenced write journal、intent-before-catalog、ambiguous reconciliation | Iceberg/Paimon/Lance/COPY/CDC 高并发和故障组合、跨引擎结果 |
| B9 | 固定到达率混合样本和阶段事件 | Linux 专用 runner/config、共享 SF100/SF1000、非零三窗口与 Trino/Spark reference |

### C 历史文档勘误

| ID | 当前事实 | 文档处理 |
|---|---|---|
| C1 | `JournalEventSql` 使用 `executeUpdate()`，仅冲突时 SELECT 核对，不是当前 `RETURNING` 实现 | 将 `RETURNING` 标为目标方案；当前正常路径为一次 INSERT |
| C2 | 逐 batch `requirePinnedRead` 的 anchor/scope/pin 查询是历史基线 | 当前按 read lease/window 验收；保留历史调用链但不作为现状 |
| C3 | 逐 batch 两事务/八 SQL 是历史基线 | 当前为有界 publication window；验收逐 batch/chunk 零授权 SQL |
| C4 | 常规 100 ms `ConsumedResultSweep` 已由批次 18 删除 | 旧轮询段落标为 superseded，当前按事件驱动 drain |
| C5 | `BackpressureE2ETest.verifyAbruptFlightDisconnect` 已覆盖本地 abrupt disconnect | 本地已覆盖；跨主机和长驻留仍归 B6 |
| C6 | batch 115 的 directed-wait 记录属于历史实现；当前以签名票据单跳转发及对应读库计数为证 | 接管和跨主机覆盖归 A2 继续验收 |
| C7 | public JSON profile 已公开实际 DAG、stage observation、accepted attempt 和 Native operator counters | 区分实际字段、audit totals 与仍 unavailable 的 result phase/物理 I/O 字段 |
| C8 | migration 编号会随并行工作变化 | 文档不固定编号；实施时扫描下一个未用编号 |
| C9 | 单输入 AQE、local aggregate、write recovery 的代码机制存在 | 历史记录增加“机制状态 / 验收状态 / 后续 WP”，不写成整体完成 |

工作包按 WP1 → WP2 → WP3 → WP4/WP5/WP6 → WP7/WP8 → WP9 推进。WP1 的 durable acceptance、owner routing 和 result disposition 是后续执行合同的前置条件；WP3 先提供真实 I/O 归因，再校准 WP5 的 AQE 阈值。任何 SDK、CI、模块所有权或基础设施变更都必须先取得明确批准。

## 2026-11-24 收尾检查（历史记录）

原始提交超时、DAG 中断和 `ExchangeInputLeafDetail` 运行时缺类的修复已进入 `main`。
四表策略读取合并、Iceberg 多键剪裁和 CBO 统计批读均已进入 `main`，各项样本计数和独立回归见[验证记录](#验证记录与覆盖边界)。
[#78](https://github.com/pangrova-io/pangrova/pull/78) · [#80](https://github.com/pangrova-io/pangrova/pull/80) · [#83](https://github.com/pangrova-io/pangrova/pull/83) · [#84](https://github.com/pangrova-io/pangrova/pull/84) · [#85](https://github.com/pangrova-io/pangrova/pull/85)

| 事项 | 检查状态与续做条件 |
|---|---|
| 初始读准入、目录指针和命名空间 | 双表连接实测 2 次初始读准入和 4 次 Iceberg JDBC 目录指针读取，单表限定名另有 1 次命名空间检查。逐笔区分名称发现、选定快照和 split 前的身份核验。验证同名表重建、目录版本变化、外部快照到期、时间旅行、引号、CTE、视图和显式目录枚举后，再收敛可共享的读取。验收同时记录 SQL 次数、实际元数据对象打开和物理释放。Trino JDBC 在查询关联的目录对象内复用元数据，split 枚举继续使用已选定的快照；借鉴前应验证 Pangrova 现有的 split 前权威检查。[Trino JDBC 目录](https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/catalog/jdbc/TrinoJdbcCatalog.java) · [Trino split 枚举](https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergSplitManager.java) |
| 正式发布回归 | 以发布提交的 GitHub Actions `Validation result` 为准，包含 Linux 生产镜像回归与适用的 Native 检查。`6d83b0eca` 的 Linux 构建和 ASan/UBSan 已通过，macOS Docker 虚拟机启动失败后生产回归跳过。macOS 流水线修复已合入，`86f403861` 的复测以对应运行记录为准。 [旧运行](https://github.com/pangrova-io/pangrova/actions/runs/45412205156) · [流水线修复](https://github.com/pangrova-io/pangrova/pull/101) · [复测](https://github.com/pangrova-io/pangrova/actions/runs/45777706208) |
| 同类引擎性能验收 | TPC-H/TPC-DS SF100/SF1000 仍需共享硬件、同一快照及对应配置。收尾检查时仓库没有注册专用 runner，当前会话也缺少受控 TPC 配置。可运行时按独立窗口采集 Trino、Spark 和 Pangrova 的吞吐、P95、首批、完整消费、读写量和结果摘要。 |

下表保留逐阶段验收条件。多 Coordinator 混合负载、跨主机 Shuffle、AQE 偏斜、Velox spill 和高并发写入仍在验收范围。
组合回归夹具修复和 Iceberg 历史分片优化已合入 `main`。 [组合回归 PR #90](https://github.com/pangrova-io/pangrova/pull/90) · [历史分片 PR #91](https://github.com/pangrova-io/pangrova/pull/91)

## 证据口径

- **已验证调用链**表示能从 HTTP/SQL 接入、生产组装到调用点闭合，或已检查引用、注册和配置并确认有一侧断开。删除前还要通过全仓编译及真实入口，防止遗漏 SPI、反射注册或外部部署脚本。
- **验证了机制**表示由实际代码、JDK/Native 测试或既有实验支持。事务数、编码次数和等待边是机制证据，生产占比和目标收益由同源负载实验评估。
- **待测再设计**表示证据仅证明存在某种执行方式，如单端点、内存 spill 或排序。目标状态先定正确的协议和度量，不引入新的存储格式、传输模式或调度分支。

## 待实施与待验收

| 环节 | 剩余工作 | 验收证据 |
|---|---|---|
| 客户端、准入和状态 | 扩大已完成的本地固定到达率压测到多 Coordinator，定位过载时点查和分组 P95 上升。验证认证状态投影、客户端重连、长短查询干扰与租户槽位归还 | 每条路径的首批、P95、超时、拒绝、耗尽恢复和零正常状态 SELECT |
| 多表授权与目录 | 审查逐表的初始读准入与目录读取。量化大小写回退和显式枚举的实际使用频率。按同一查询的授权范围合并可共享的读取，继续验证其他连接器的具名快照 | PostgreSQL 分别记录授权、命名空间枚举、表枚举与目录指针读取。覆盖重复别名、大小写及歧义、CTE、展开视图、缺失或移动的引用、撤权、目录部署变化、表更替和快照到期 |
| 物化对象跨 Coordinator 验收 | 分别启动两个 Coordinator，验证跨进程发布、暂停、删除、名字复用和断线重连。测量多租户目录在频繁刷新与缓存淘汰时的加载量和尾时延 | 对照两个进程的 SQL 返回和确切发布代次，验证租户隔离、旧查询租约和新查询的可见性，按查询和变更分别统计目录加载 |
| 快照和 split 枚举 | 扩展动态过滤的连接器、类型和晚到覆盖，验证对未下发分片的影响。大文件按合法范围拆分，小文件按工作量组包。有界枚举与实际读放大按连接器验证 | 冻结相同快照与授权。比较 split 数、实际对象 GET 和读取字节，覆盖缺失、晚到、失败、删除文件、快照演进和取消 |
| 计划、分配和控制面 | 测量实际多 Worker 混合负载下，多个大请求同时解码、排队及 Native 创建时的驻留内存和尾时延。量化下游准备隔离在固定到达率、受阻目录 I/O 与写入混合负载下的 P95。以混合负载校准单输入和对齐双输入 AQE 的阈值和尾时延，再验证更多输入形态、偏斜拆分和持久重放 | 基线和候选完成同一完整结果。检查覆盖、滞后、重试、投机获胜和 durable replay |
| 远端 Shuffle | 核验更宽和变长输出的实际对象数、内存及跨主机吞吐。扩大热点并发，量化锁等待与尾时延，再校准是否需要段存储 | PostgreSQL 和对象存储真实压测，覆盖权限撤销、owner 切换、背压、未知 PUT、丢 ACK 和 GC |
| Velox 与结果 | 对剩余 I/O、解码、spill、缓存和单出口成本做归因。继续测跨主机出口、持续内存占用与链路异常下的释放 | 冷热、偏斜及宽窄负载验证精确权限、顺序、首批、EOF、资源窗口与实际归还 |
| 写入与提交 | 用高并发和故障窗口校准文件大小、写放大、提交时延及各格式恢复。原子发布和跨引擎结果独立验收 | Iceberg/Paimon/Lance/COPY/CDC 保留快照、offset、精确 fence 和资源归还证据 |
| 发布门槛 | 同硬件、同快照对比 Trino 和 Spark 的 SF100/SF1000，并执行 Linux Native 完整链路回归 | 同源产物、结果摘要、物理 Profile、全量可复核的吞吐和延迟，以及限定环境记录 |

## 固定三条稳态零读库门槛（目标状态）

1. **状态订阅和诊断读取。** 活跃 query 由 owner 的认证状态投影提供。正常状态读取与客户端重连在投影窗口内产生零 journal SELECT。接管按认领的执行 ID 查证，显式历史查询返回最后写入的准入或终态。以两个 Coordinator 和大量同 query 的客户端断言次数，覆盖 Ready endpoint 和错误细节。已验证本机直连和签名票据定向转发，接管仍见 A2。
2. **持久 journal。** 正常新受理和新事件各一次 INSERT，独立 SELECT 为零。A1 的初始 `QUEUED` 行同时承载 principal、key digest、request fingerprint、lineage 和确定性 `QUEUED`/`PENDING` 回执；`INSERT ... ON CONFLICT DO NOTHING` 的正常路径不读回，只有 event/idempotency 冲突或未知提交结果才 SELECT 核对。事务、lineage 原子性和最终 ACK 所代表的 durability 保持一致。 JDBC 在受理和终态事务内写入 lineage outbox，持久 HTTP 后端直接投递这两条记录。本地真实 PostgreSQL 对照中，一次完整查询原有 2 次重复 payload SELECT，修复后增量为零。START 和终态各保留一条记录，均取得 HTTP 确认。内存 journal 的投递保留原有行为，并通过定向回归。对已提交写的状态判断只读取一次，未知提交仍从权威证据核对。
3. **Shuffle steady state。** 只在建流、续租、最终关闭与恢复触及读租约数据库。manifest 是由已提交 attempt 发布的不可变数据。每 batch 禁止同步数据库操作，同时本地核验 scope/epoch、租约有效期、撤销与 byte credit；已有在途读取保有精确保护时间。测试每 batch/每 chunk 的 SQL 计数和撤销后新数据是否可见。durable revocation 收敛尚未实现，见 A11。

已验证本地状态和 JSON profile 的历史读取分离。两个 profile 路由共用查询留存的终态审计记录和本机任务统计。真实 QUERY Coordinator 上，HTTP 提交并经 Flight 消费结果后，两条 profile 路由共 32 个并发请求，返回一致且审计表 SELECT 增量为零。主动清除本地投影后，普通状态和 profile 请求均返回 404，journal 和审计表 SELECT 增量为零。显式 `history=true` 的 profile 返回相同审计数据，只有一次按查询和提交主体限定的 SELECT。

受理回执携带原始 Coordinator 的 ID、进程实例和实际 HTTP 地址。该地址与幂等受理事实写入同一 journal 行。两个真实 QUERY Coordinator 共用 PostgreSQL 时，第二台重放同一请求返回原回执和端口，未创建本机查询。按该地址等待状态取得原查询的结果端点，控制面 Flight 完整读至 EOF。SDK 可在提交或使用保存的回执重连时，从客户端已配置的端点中确定所有者，后续状态和取消沿同一选择发送。未配置的地址在 SDK HTTP 测试中收到零请求。

共享入口在认证用户后以签名票据确定原所有者，以一个有界 HTTP 请求转发。目标按进程实例和提交主体重新核对权限，入口验签后转交响应字节。两个实装 QUERY Coordinator 共用 PostgreSQL，使用真实 OIDC 和独立 HTTP 端点验证同一观察者的重复等待被拒绝，原等待取得控制面 Flight 端点，路由到另一节点的取消在原所有者排空。状态、REST profile 和 Web UI profile 共 32 个并发请求，journal、审计表和 Coordinator 注册表 SELECT 增量均为零。篡改票据、其他主体和 Worker 通用凭证均无法启用转发。两个节点以动态端口启动时，PostgreSQL 注册项记录两者的实际监听端口和各自的进程实例，所有者实例与签名回执一致。启动测试验证监听端口在 HTTP 开始处理请求前已经绑定，以及取消启动后端口可重新绑定。所有者接管与当前 Native 跨主机验收仍按 A2 验证。

故障恢复的日志读取以已认领的查询 ID 为入口，通过现有查询与任务索引在同一 PostgreSQL 语句快照中取得两类事件，再按原插入顺序重放。并发存在的同一租户或其他所有者查询不进入该批次。包含 65,536 个无关活跃查询和 256 个认领查询的 PostgreSQL 16 对照中，两条原 SQL 共返回 131,840 行，新 SQL 返回 768 行。五次热态采样的新 SQL 执行时间中位数为 2.54 ms，原两条 SQL 的中位数分别为 18.63 ms 和 82.86 ms。该实验仅测数据库回放语句。恢复进程注册状态投影时直接保留批次已经证实的 journal 归属。早期批次曾对 256 个认领执行合并读取清理意图。现行路径由终态清理与前任退出证明触发，空闲时没有这套 Shuffle 意图扫描。

接管回放额外返回已存在终态或仅有任务日志的执行 ID，保留这些执行的租约。接管前遗留的空认领可按精确令牌释放。幂等过期清理和键复用保留仍有关联租约的接收回执及任务事件，租约过期等待接管期间也保留。恢复查询仍使用两张日志表的查询 ID 索引。对一条关联一万条任务记录的终态查询、一个仅有任务记录的执行和一个空认领，PostgreSQL 执行计划返回两条分类记录，终态查询的任务记录未进入回放。接管后重新核对 Worker 的原始退出凭证并完成释放仍在 A2。

生产受理将执行租约的 owner、进程实例和 fencing token 一并交给 journal。PostgreSQL 在同一条写入语句锁定对应租约，取得锁后按数据库时钟核对有效期，再插入初始 `QUEUED`。新受理无需独立 SELECT。旧 Coordinator 在接管后恢复运行时，只有已提交的精确事件可重放原回执，其余受理被拒绝。

普通 DAG 和持久 Stage 分片在 Worker HTTP 派发前写入精确控制地址、租约令牌和二进制 admission seal。每批最多 16 个任务共用一条带租约校验的 PostgreSQL 语句，发送端直接使用冻结请求，ACK 后的异步 `SCHEDULED` 写入已移除。接管能够读取这些凭证。使用凭证关闭旧 Worker admission、核验物理退出和释放资源仍按 A2 交付。

## 校准后的设计决策

### 短查询和大查询采用各自的最短生产路径

**控制面先改等待方式。** 用现有 HTTP 提供基于状态版本的异步有界等待，每个客户端同一查询最多一个 waiter。状态版本与 waiter 登记必须原子衔接，再检查版本，防止状态恰在登记期间变化而丢唤醒。立即结果走同一响应，取消/超时注销等待，按 principal 和 tenant 限制并发与时长。首版由 owner 提供状态，peer 通过已有 owner 信息定向请求。跨节点流转只更新轻量视图，未证明有必要之前无需全局事件总线或常驻流连接。Ready、Terminal 和 Drained 可独立出现，快速任务和无结果集也有完整响应。Trino 的 `nextUri` 与服务端有限 `waitForResults` 支持这种请求形态。Pangrova 继续用 Flight 传数据。 [Trino 客户端协议](https://trino.io/docs/current/develop/client-protocol.html) · [Trino 实现](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java)

**跨 owner 幂等使用同一受理权威。** 初始 `QUEUED` journal 行保存 principal、幂等键 digest、canonical request fingerprint、lineage 和确定性受理回执，并以 `(principal, key_digest)` 与初始 `query_id` 的部分唯一约束选出原请求。正常请求只做一次受理 INSERT；冲突、响应丢失和恢复路径才读取并逐字段比较。重放不会再次执行 SQL，也不会依赖进程内 map。验收保留真实 PostgreSQL 双 Coordinator、响应丢失、崩溃与接管用例；当前本地集成测试已覆盖单写、冲突、未知提交、principal 隔离和 TTL 边界。

**控制协议和调度分开验收。** 保留现有 HTTP 服务设施，以一个 canonical 二进制消息替换 task/report 的 JSON+Base64 镜像，可独立测 body bytes、解析次数和 CPU/attempt。避免把新 gRPC 服务、共享计划缓存、重新编排 ACK 同时设成依赖。销毁任务与取消不能靠 HTTP 是否返回判断。Worker 快速接纳和真正端点 Ready 若需分开，以精确 attempt 和当前 generation 表达。对于不产生端点的任务无需额外 Ready RPC。DAG callback 只做小型状态发布，用查询顺序和 admission 约束交接给有界工作池，阶段准备与 durable commit 各有容量。用阻塞目录、慢对象存储、终态报告压力和关闭观察无关查询 P95、队列和 resource drain。

**小查询保留流水直连。** 持久交换支持跨 Worker 重试或 durable replay，但消耗写入、发布、存储和再次读取。Trino 也明确指出 TASK 重试适合大批查询，可能增加高频短查询延迟。按明确的恢复要求或实测负载选择路径。在同一查询中按 stage 边的依赖语义选定读取方式，已签发边保持稳定。将两种执行策略作为真实能力管理。 [Trino Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html#task)

### PostgreSQL 热路径与撤权边界

**journal 保持冲突可判定。** 当前生产实现使用 `executeUpdate()` 和 `INSERT ... ON CONFLICT DO NOTHING`；正常路径一次 INSERT、零读回，event ID 或 idempotency 冲突时才 SELECT 核对。A1 的真实 PostgreSQL 测试覆盖正常单写、跨 journal 实例重放、同键异 fingerprint、未知 commit response 和 retention。早期 `RETURNING` 实验仍只作为历史设计验证，不写成当前实现。 [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html) · [隔离级别](https://www.postgresql.org/docs/16/transaction-iso.html)

**Remote Shuffle 消费端去掉逐 batch 锁定查询（历史基线）。** 早期的 `requirePinnedRead` 事务包含 anchor、scope `FOR UPDATE` 和 pin 查询，双连接试验确认了同 scope 的 `FOR UPDATE` 等待和不同 scope 的隔离范围。该调用形态已被后续 read lease/write window 批次取代，不再作为当前逐 batch 成本。当前验收要求数据面逐 batch/chunk 零授权 SQL，并在建流、续租、关闭、恢复和撤销边界单独计数；durable revocation 仍见 A11。 [PostgreSQL 16 行锁](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)

**读取许可和对象保护设计成两个时钟。** 建流由权威事务取得绑定 producer winner、owner epoch、消费 attempt、分区、序号区间和 manifest digest 的读许可。许可截止取请求、capability、execution authority 与自己的有效窗口的最小边界，明确时钟误差与对象 API deadline。发布 EOF/ACK 前完成长度、序号、摘要和所有权验证。同一读者的状态机负责禁止发起新 GET/发帧、等待现有所有者退出和结束 pin。GC 继续使用持久保护记录，保护时间覆盖已经发出的 I/O，重启后根据持久记录安全恢复。取消/接管必须先阻止新许可，向仍有许可的服务完成带版本的撤销屏障。确认旧 epoch 无权再发新字节之后才宣告切换或执行删除。旧进程不可达时，等待证明其许可耗尽并留足 I/O 保护窗口，期间向调用方报告仍在收敛。若现有合同要求更早的完成通知，原型先证明可达。单靠进程内 TTL 或异步通知无法通过验收。

**按实测选择写入粒度。** 读租约和不可变清单可独立测量。首轮原型不要求改变对象打包方式，成功后数据面按清单取帧。只有固定字节下 PUT/GET 次数、索引体积或写锁成为显著成本，才决定改成有界 segment。发布时仍原子绑定连续序号、长度、digest 和 winner receipt。写入仍按原 proof、fence 与未知提交状态验证。每次实验只引入一个变量。目标部署只运行已选定的一种对象格式。 Uniffle 为同分区的块独立保存数据文件和索引，提供了合并对象与保留块级验证的设计对照。 [Apache Uniffle 文件格式](https://uniffle.apache.org/docs/intro/#shuffle-file-format)

### 扫描、AQE、结果和写入的适用门槛

- **扫描与 split。** 原有 10 万分片微测证明最小堆适合高扇出，小并行度可能更慢。先用实际 range bytes、对象数、delete/解码估计和 backlog 定任务尺寸，再从生产分布决定是否换分配算法。总 split 有界准备和已签发 task descriptor/重试状态使用各自的容量指标。不能为减少枚举内存而提前签发需要固定 cohort 的 stage。需要先证明完整覆盖的模式仍按固定快照生成清单。用Iceberg manifest 剪枝和现有 Velox pushdown 先减少实际输入字节。 [Iceberg 扫描规划](https://iceberg.apache.org/spec/#scan-planning) · [Trino task sizing](https://trino.io/docs/current/admin/fault-tolerant-execution.html#task-sizing)
- **真实 AQE。** 派发保持原计划的分区坐标，`ExchangeSourceBuilder` 按 consumer partition ID 取 producer 分区或 receipt。单哈希输入以及对齐的双哈希输入均已按提交回执生成、冻结并校验实际读取映射。进一步校准阈值及真实负载尾时延。用不同 join 类型、NULL、重复键、倾斜、重试和 durable replay 证明完整覆盖、去重和成本上界，再比较关键路径。Spark AQE 的参考点是使用运行时输出统计与不同读取划分。它的默认阈值不直接作为 Pangrova 门槛。 [Spark SQL AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- **结果出口。** 首先保持单 endpoint 的 SQL 行为，移除客户端 IPC 回转。Arrow Java 当前使用 19.x，依据该版本的 DoGet、batch 生命周期及顺序。限额按 bytes 同时覆盖服务端输出、客户端 Arrow allocator、消费解码和传输队列。先跑慢消费者、客户端提前关闭、读完后终态失败、分区数据齐全。只有单出口带宽或资源滞留占主要成本时，再选择并行 endpoints、range order merge 或持久结果段。全局 ORDER BY/LIMIT 必须证明完整顺序与完成结果。`FlightInfo.ordered` 只能描述已保证的端点拼接顺序。 [Arrow Flight 19](https://arrow.apache.org/docs/19.0/format/Flight.html) · [Trino 结果分段](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol)
- **DML/CDC。** 提交回执已复用，继续测量高并发和故障窗口内的 authority、journal 与行状态成本。不要因为只有一个发布 API 就把 journal 或清理做成 catalogue commit 的隐式成功。连接器 publish 与 journal 分属不同事务，前后校验不能仅因相邻而合并。COW、write file sizing、碎片包装和 COPY 本地化各用自己的证据，含快照可见、旧文件保留、未知提交和 CDC offset。 [Iceberg 可靠性](https://iceberg.apache.org/docs/latest/reliability/)

## 基线与独立出场门槛

| 场景 | 对照方法 | 独立发布门槛 |
|---|---|---|
| 短查询、多租户 | 计时提交→Ready、客户端感知终态、worker 真正接受/退出。同负载 A/B 并保持短查询与长查询混合 | 正常状态读取零 SELECT。同步报告与回调不阻塞独立查询的启动或取消。记录固定负载的成功吞吐和 P95 |
| Remote Shuffle | 同总字节改变 batch/生产者数，同 query scope 与不同 scope 对照，以 SQL/txn、锁等待、首包、吞吐和尾分区分项观测 | 消费者在授权窗口内零同步 SQL。生产端按有界窗口预留和发布，统计每窗口与每流的 SQL。跨 owner、取消和撤销验证访问和清理语义 |
| 大结果 | 固定 schema、重复值比率和宽度，让输出总字节增长一个数量级，并固定 byte window、并发消费和超时 | 峰值堆/direct 不随总结果量同阶增长，首批和实际消费者吞吐按负载达标。验证慢客户端不会拖住其他租户 |
| 大扫描与 AQE | 固定快照、存储布局、join 数据倾斜和资源容量，分开观察 split 发现、真实 GET、stage overlap、CPU/spill 与读取覆盖 | 每个物理任务读对范围，基数等价。任何启用优化在目标负载的实际输入或关键路径有可重复改善 |
| 写与恢复 | 同表/异表、append/COW/COPY、CDC、慢 journal 和故障点，记录文件大小、commit 扫描、catalog 可见与外部确认 | 查询成功时能出示 exact publication/commit receipt，撤销/重试无重复写或提前 offset。新 journal event 无独立读回 |

同源隔离基线和候选使用相同数据、配置、外部服务和资源。现有 TPC 流程用于固定窗口内的正确性与性能。短查询拥塞另跑按固定到达率的对照，记录排队、拒绝、超时和完成样本，避免只统计成功的快查询。P95 和吞吐比较固定 SLA、同提供负载及可测窗口。P99 仅在足够样本和独立窗口成立时报告。阶段百分位独立报告，查询关键路径用因果关系重建，不相加。将 “SQL 次数 = 0”、所有权和正确性用断言设置为硬门槛，性能收益按基线波动提前冻结阈值。 [仓库 TPC 基准方法](../testing/tpc-benchmark-methodology.md)

## 后续 PR 与一次性发布

1. 用真实分片、对象读和精确终态扩大跨 Stage 动态过滤。按实际负载评估其他键类型、连接器和等待成本，以及晚到过滤约束尚未发出的分片。
2. 用完整 SQL 负载校准已启用的单输入及对齐双输入范围合并，再验证热点拆分与持久重放。
3. 为读写路径运行独立过程与资源压力验收，记录普通路径、失败恢复与物理退出。先对现有行为归因，按明确阈值决定额外的存储和结果并行优化。
4. 用冻结的 Linux 配置完成同条件跨引擎基准及正式发布回归。

每份 PR 的最终运行入口保持唯一。严重依赖可以在隔离集成分支内合入和验证完整链路，达到对应测试后随新版本一起发布。生成器、源 proto 两端与 descriptor golden 同步更新，删除字段后保留原编号与名称，新增字段使用新编号。schema 改动落到 canonical SQL 迁移模块及新集群引导，候选用独立的数据库和对象前缀初始化新 schema，不安排旧库或旧结果格式迁移。基线 A/B 使用各自同源的隔离部署；候选出错暂停发布，在单独环境修正并重新验证。

每一个删除项验收同时看生产调用归零、注册/配置被移除、相邻行为经过真实入口，以及对应的性能或正确性证据。生产 rollout 只有整套新版本和全新运行环境，故障恢复用同版本状态机演练。目录现有的业务数据与历史实验保留为可重现输入。

## 验证记录与覆盖边界

以下均为 **at commit / historical evidence**：数字、SQL 次数和覆盖范围只对应各批次当时提交的角色包与配置。当前未完成项以本文上方的 A/B 审计为准；单输入 AQE、局部聚合和写恢复等“机制已实现”不等于生产验收已完成。被后续批次取代的描述在原文附近标注 superseded。

持久受理后，查询由执行期限继续约束。原来受理与结果就绪共用提交计时器，已返回 `QUEUED/PENDING` 的查询也可能在 250 ms 初始就绪期限后失败。双 Worker 基线中的无键初始化 INSERT 在进入派发前等待约 12 秒，Worker 登记后约 8.6 秒完成启动，原提交请求返回 HTTP 500。统一受理后，带键和无键请求都在 PostgreSQL 确认受理后返回 `QUEUED/PENDING`，由查询执行期限约束后续运行。本地真实双 Worker 用例完成初始化 INSERT，执行查询并通过 Native Flight 读取结果。真实 PostgreSQL 恢复用例和分支会话 E2E 验证了成功终态的分支选择。

新一轮独立进程验收覆盖 Iceberg、Paimon、Lance 的六项写入恢复，Local Disk Shuffle 的跨阶段 Worker 绑定，及两项真实 Iceberg RECLUSTER 恢复。修复前，目录解析失败的后台物化刷新缺少 START 血缘，导致 Coordinator 关闭时需要 SIGKILL。修复后，定向回归核对失败先落下唯一的 START 和 FAIL 并完成排空，Local Disk Shuffle 进程正常退出。写入恢复按原查询 ID 和 fingerprint 检查紧凑终态回执，再读取已发布结果。RECLUSTER 用例验证了 Worker 鉴权、原 Worker 退出后重做暂存、提交恢复以及精确快照。

独立 Coordinator、Kafka 和 PostgreSQL 的 MAINTENANCE 对照中，一次本地暂存至 offset 确认的执行租约点查从 7 次降至 5 次，流版本点查从 6 次降至 4 次。Paimon 目录发布后的 journal 完成故障和 offset 确认故障均能在重启后恢复。COMBINED 实际 Worker 的 Paimon 发布及 Iceberg Worker 退出后接管也通过。两种角色在 使用 checkpoint 投影的独立 PostgreSQL 回归中均使用六列 checkpoint 查询，发现路径的完整 redo 查询计数为零。独立的 16 项 PostgreSQL journal 用例通过，包括挂起优先、终态回执与流代隔离。

连续三个 Kafka 空闲轮次中，MAINTENANCE 基线查询 checkpoint 与已提交 offset 各三次。候选在 COMBINED 和 MAINTENANCE 的独立进程回归中，两类点查均为零。后续新数据触发权威 checkpoint 恢复并发布预期行，进程重启时恢复未确认的写入与 offset。全局 RUNNING 流发现仍按周期运行。任务规划共享一次 Kafka 分区与末端偏移观测，单元测试核对各调用一次并拒绝不完整观测和回退的末端偏移。

新 MAINTENANCE 批次使用执行租约申请事务的回执，移除申请前的存在性点查。独立进程与真实 PostgreSQL 核对单批执行租约点查由五次降至四次，流版本点查为四次。真实租约测试验证新申请遇到当前存续租约时不签发第二份租约，恢复可复用原令牌，接管则取得更高令牌。Kafka 范围执行在注入失败后仍以原范围重放并确认精确 offset。

Coordinator 模块全套验证暴露了 Worker 控制客户端对查询报告校验器的反向依赖。Protocol 现在从原始响应解码一次并核对带 fence 的精确尝试。普通终态报告复用同一套来源和排空状态规则。原架构门禁通过，25 项协议和 descriptor 测试、81 项 Coordinator 和 Worker 控制回归及静态检查通过。重新打包核对原 Coordinator 位置已移除旧类，Protocol 包包含解码器。

真实 PostgreSQL 认领同一批八个持久分片时，旧放置将一个 128 MiB 和七个 1 MiB 扫描范围都交给静态评分更高的 Worker。候选把大分片留在该 Worker，七个小分片交给另一个内存合格的 Worker。窗口仍只读取一次执行权威。下一窗口观察到本地已绑定任务，未进传输的任务回滚后归还计数。62 项调度、认领和排空测试及静态检查通过。独立双 Native Worker 的 GROUP BY 与全外连接核对了结果和物理排空，Lance 持久化导出核对了精确快照、行策略及发布数据。双 Worker 运行包 SHA-256 清单为 `13f8e7027eb5c1188548baaa80da45b09e5d7b0db6a306f40b4b68738622a243`。

独立进程与真实 PostgreSQL 的 CDC 对照中，MAINTENANCE 每批的完整流定义点查从四次降为三次，执行租约点查仍为四次。offset 确认实测一条包含流版本锁的批量推进语句，独立版本锁语句为零。阻塞确认的暂停事务提交后，确认按最新版本拒绝，多分区确认失败时原值全部保留。CDC 持久层 94 项测试、2 项 Coordinator DDL 集成测试和 Checkstyle、SpotBugs 通过。3 项打包端到端用例通过，覆盖真实 Kafka、COMBINED Worker HTTP、MAINTENANCE 接管和 Paimon、Iceberg 发布与 offset 故障。角色包 SHA-256 清单为 `ea9894a0f84ab969fa71b66a52bcad5341f33fc1b0c9396ceb1b1dc5994620f2`。

Shuffle 服务的 PostgreSQL 对照在过期清理接管时记录到一次重复的 scope 锁定点查。候选复用选中行的锁和原租约期限，重复点查为零。已封口 scope 重领租约的分片状态 UPDATE 从一次降为零，正常重领和过期接管都由 PostgreSQL 语句计数覆盖。接管仍按 1,000、1,000、505 条分页处理同一范围。真实 PostgreSQL 同时验证存续读 pin、终态后的新读写围栏，以及仍受保护的上传对清理的约束。

真实 PostgreSQL 对照复现了远端清理返回后、执行租约退役前，迟到的注册请求能重建 scope。候选让终态封口与执行租约同寿命，已清理和从未注册的查询均持久化收敛确认。重试复用原确认时间，退役时与租约原子删除。独立 Coordinator、两个 Native Worker 和真实 Shuffle 服务组成的 GROUP BY 与全外连接回归均核对了结果、对象清理及租户槽位归还。

真实 PostgreSQL 在 2,505 条分片记录的接管路径捕获一次 COUNT DISTINCT 和 SUM 全量聚合。原协议字段只供 Coordinator 日志使用。候选的首次认领和过期接管对此聚合均为零，仍保留 1,000、1,000、505 条有界清理分页。回执验证覆盖旧所有者封口、接管后的代次保留、重复确认，以及不确定上传交给持久前缀清扫的标记。 协议替换后的独立 Coordinator 与双 Native Worker 实测通过 GROUP BY 和全外连接，验证了结果、Shuffle 对象释放及租户槽位归还。

真实 PostgreSQL 对照中，读窗口续租移除独立 scope 点查并复用锁定锚点的到期时间，SQL 从 3 条降至 2 条。读流闭合以一条语句原子删除精确 pin 和执行窗口，兄弟流保持有效，已被清除 pin 的窗口仍能回收。并发对照验证续租等待锚点锁后能看见已提交的封口。 执行窗口被另一连接占用时，闭合先持有 pin 锁。独立双 Worker 的 GROUP BY 和全外连接通过结果及物理排空回归。

真实 PostgreSQL 基线中，删除 Shuffle allocation 会将两个仍不确定的写入保留记录级联删除。候选在 scope 移除后保留 producer window 和 manifest PUT 的两个精确 ID，补偿清扫的认领由两条语句合并为一条。MinIO 复现了清扫已结束对象扫描、写入才完成的顺序。错误 attempt 和错误 manifest 对象键均保留原记录，精确确认后旧清扫仍保留调度。下一轮删除两个迟到对象并停止周期清扫，同一 allocation ID 继续受围栏保护。没有确认的崩溃场景仍保留补偿责任。 39 项模块测试、28 项真实 PostgreSQL/MinIO 集成测试与静态检查通过。独立 Coordinator 与双 Native Worker 的 GROUP BY 和全外连接均核对结果、对象释放及租户槽位归还。运行包清单 SHA-256 为 `247a53e038138563dbcef0d7da071c670e0797f1293c36d0ce1c68da63c0fafb`。

旧 Coordinator 部署包会启动并停用 CDC 调度线程，同时构建旧两阶段提交存储和页写入工厂。候选部署包的启动字节码已移除这些依赖。真实 PostgreSQL 的 CREATE STREAM 对照从一条 INSERT 加一条 UPDATE 降为一条 RUNNING INSERT。COMBINED 的独立进程验证暂停期间 Kafka 数据不发布、恢复后按原 checkpoint 发布。MAINTENANCE 在 catalog 已发布而 journal 或 offset 确认失败时仍可重启恢复。PostgreSQL 用例另验证过期 revision 和删除后重建的 checkpoint 拒绝旧 offset 确认。

同一台 macOS arm64 上的两个独立 Coordinator 共用 PostgreSQL 和租户槽位，每个 Coordinator 有独立 Worker。两个结果流占满槽位时，按 25 ms 计划到达的 40 次提交全部返回配额拒绝。归还槽位后，按 125 ms 计划到达的 80 个混合查询均读至 EOF 并实际排空。按计划到达起算，剔除两次预热后，受理、端点就绪、首行、EOF 和排空的 P95 分别为 89.8、203.7、205.8、206.2 和 229.2 ms。24 次认证状态重连的 journal SELECT 增量为零，两个 Coordinator 均正常关闭。跨主机容量及尾延迟仍属 A4/B 项验收。

OpenTelemetry 对照把派发停在 PLANNING。旧代码已返回持久回执时仍未结束 `query.submit`；候选在返回回执时已结束该跨度，后台线程仍使用同一个 span ID 作为父上下文。运行结果和物理排空继续由查询生命周期发布。

集合查询的排序和分页由解析器绑定到整个集合结果，动态表 AST 改写保留它们，Calcite 在集合运算上生成全局排序与截断。仅指定 OFFSET 时，Substrait 使用标准 offset 表达式并留空 count，Worker 在最终流水线应用一次偏移。真实双 Worker 的 Flight 用例核对了全局 ASC、DESC、带 LIMIT 的 OFFSET、仅 OFFSET 和实际排空。Iceberg 与 Paimon 独立执行同一 UNION ALL 后返回相同的有序结果。分支自己的分页在派生表内表达，递归 CTE 的排序和分页在消费它的查询中应用。

持久受理的队列取消与运行取消使用真实 Coordinator 任务跟踪器进行 JDK 对照。基线在已排空的队列项出队时仍调用派发入口，并在受控运行回调退出前归还槽位。候选在队列取消时归还槽位且出队后跳过派发。运行取消保留槽位，直到回调退出才完成排空。HTTP 受理路径删除结果端点、写派发和终态的三组提交等待，以及已无消费者的第二份回执。同一 Corretto 17 的纯排队分配探针使用生产提交器和只接收回调的执行器。每轮 10,000 个预建状态，五轮每次入队的分配量由 864 B 降至 56 B。该数字只覆盖入队线程的分配。双 Coordinator 的 PostgreSQL 槽位、进程故障恢复、真实双 Worker 与 Native Flight 定向回归通过。

受控回归先触发 200 ms 探测放行，再提交完整构建摘要。旧实现到实际 split 枚举时没有过滤范围。候选对精确快照、字段 ID 和调用权限施加两个键范围，每次仍只调用一次分片源。摘要不会应用到同一表的另一快照，查询先终止时也无法接纳晚到摘要。原 Profile 仍记录调度超时，实际枚举日志记录晚到过滤。98 项 Java 回归和静态检查通过，四项双 Native Worker Iceberg 物理读回归通过。两条常规早到过滤分支的对照从 80 个探测分片和 26,413 B 读量降到 4 个分片和 1,826 B，重复键、NULL 和结果集合验证通过。

运行中任务的 Worker 过滤控制移除了 JSON、Base64 和 Worker Java 的嵌套消息解码。HTTP 请求携带完整任务尝试与一个类型化谓词集。Coordinator 以签发时的 Worker 身份为目标，Worker 以本地准入登记校验同一尝试。该更新没有新增数据库读取。61 项定向 Java 测试和静态检查通过，覆盖内部鉴权、旧媒体类型、错误尝试、报文上限、真实 Native 句柄接收及任务释放。53 项 Native 过滤回归通过。生产 DAG 触发投递及活跃扫描的读量验收仍属于 A9。

生产 DAG 接通了晚到投递。已发布的获胜构建和探测端 RUNNING 任一先到均能提供必要条件。只有按相同快照、schema 和字段枚举且当时缺少完整构建证据的探测代，各精确尝试最多发送一次。无任务发现或数据库点查。初始和在线过滤共享 Native 绑定，唯一扫描由转换后的任务计划验证。探测侧同一 Parquet 对照在读出前两个值后发布过滤，基线返回 6 行，候选保留 2 个先到行和 2 个符合条件的后续行。该对照量化正在运行的读取批次输出行数，跨 Worker 晚到投递的 SQL 及对象字节对照仍待完成。

真实双 Worker 的两个独立 Iceberg INNER Join 分支由 `UNION ALL` 汇合。探测文件分区为 40 个，构建侧两组均含两个可匹配键，同时覆盖重复键和 NULL。候选在同一份数据上的过滤与未过滤对照返回相同的分支结果并完成物理排空。探测 split 合计从 80 降至 4，Worker 报告的探测源读取量从 36,325 B 降至 1,826 B。Profile 记录两条独立的 `APPLIED` 决策。该样本使用本机存储，跨主机吞吐仍待校准。

双 Worker DECIMAL 聚合的 PostgreSQL 对照中，普通查询原本执行 5 次紧凑目录归属读取。第 3 和第 4 次均核验扫描源，其中一次由启动派发时为重排器无条件构建扫描源触发。重排器识别到需要索引绑定时才解析扫描源，普通查询由阶段准备解析。改动后原用例通过既有的 4 次计数门槛，读取结果和 Native 清理用例通过。

受保护的关系导出先在编译后的 Substrait 消息上绑定 Exchange，再把每个 Stage 编码一次。构建图和重放仍校验实际序列化的输入与输出 schema。本地 Corretto 17 对包含两条 Exchange 的三级图交替测量两轮，每轮五组，每组 400 次。两版的图摘要与三个片段的字节长度相同，每次编译的线程分配中位数从 375,616–375,896 B 降到 295,980–300,689 B。计时受同机负载影响。Planner 通过 1,528 项用例，另有一项生成 golden 的可选用例跳过。

本地读取配额微测复用实际 Native 配额实现，对照每轮 200 万笔 8 字节授权与归还，逐轮核对请求和返回总量。移除每次调用的 Attempt 堆分配，将返回量和调用结果合并到原有 drain 锁内发布。macOS arm64 单线程六轮 CPU 样本中位数由 135.9 ns 降到 43.6 ns。样本只测配额与计数开销，存储延迟单独验收。 10 项文件适配器定向测试通过。用实际本地文件让第二个范围到达 EOF，确认仍报告第一个范围已返回的 4 字节，并记录两次调用和一次失败。73 项协议、任务报告和 Profile 定向测试通过。 完整 Release 重建后的 86 项 Native 定向测试通过。已释放的任务在解析对象地址前被拒绝，后端调用数保持原值。

实时扫描的 Worker 放置复用任务签发与物理释放。两个同等资源的 Worker 中，给缓存节点设置一个心跳尚未覆盖的本地在途任务，再按顺序放置四个同一文件的分区。使用实际选择器的本地探针中，旧缓存优先路径的估算工作量为 1,026/0 MiB，候选为 513/513 MiB。放置探针只计量估算工作分布。定向回归覆盖租户和内存筛选、负载相当时优先使用缓存、心跳与本地计数重叠、Worker 重启身份、派发前回滚和精确物理释放。真实双 Worker Iceberg SQL 通过相同快照的整文件和区间路径读出同样的 4,094 行。Worker 物理释放回执与 Profile 的适配层计数一致。文件缓存命中的分区可只产生元数据调用。相同 Native 构建通过能力握手和 4,383 项本地 Native 全量回归。旧 split work-stealing 原型只维护虚拟计数，已无生产调用、路由和注册，本轮删除原型及仅引用它的测试。跨 Coordinator 与带存储压力的尾时延属于 A6。 [Trino 调度器](https://trino.io/docs/current/admin/properties-node-scheduler.html)

阶段归因通过派发观察、阶段计量、查询指标、attempt 计数和本地清理的 26 项定向回归。终态回执保留通过 37 项 JDBC/PostgreSQL 测试，并验证 redo 删除后重放使用同一个终态证据、跨重试时 catalog commit 不重复执行。回执回收仍按 A12 的来源重放窗口验收。

S3 传输归因需利用 SDK 请求上的接收回调和请求结果重试计数。HTTP 实体字节、请求或响应头字节、TLS 与套接字层字节各有自己的观测边界。跨主机故障测试用后端日志对齐请求，用客户端计数校验收到的实体。 [AWS 请求回调](https://docs.aws.amazon.com/sdk-for-cpp/latest/api/aws-cpp-sdk-core/html/class_aws_1_1_amazon_web_service_request.html) · [libcurl 下载字节口径](https://curl.se/libcurl/c/CURLINFO_SIZE_DOWNLOAD_T.html)

真实 HTTP SQL 测试用 Iceberg SDK 写入一个多行组 Parquet 文件，并提交含两条位置删除的快照。保持 SQL 和快照一致，只调节表的读取分片目标。整文件路径发出一个扫描分片和一个扫描任务；区间路径发出 10 个分片，由两个独立 Worker JVM 上的 8 个扫描任务读取。两次都经 Native Flight 返回同样的 4,094 行并完成物理排空。两次枚举都只规划了一个原始数据文件和一个位置删除文件。定向回归还覆盖阶段预算耗尽、无可用偏移、来源快照绑定、索引按原始对象应用判断、窄列投影、Join、Top-K 和写后读。该测试使用本地文件和 48 KiB 的小分片目标。跨主机与默认分片目标的吞吐、尾延迟和远程对象 I/O 仍按 A7/A8 验收。

两个相邻区间读取同一个 1,573,814 字节、两行组的本地 Parquet 文件。旧实现让两个区间各请求整个文件并重复返回行。候选分别请求 852,184 和 852,187 字节，按文件绝对行号删除两行，两个结果的并集与整文件读取一致。只含 footer 的有效区间返回空结果，越过文件末尾的区间失败。数字来自 Native 任务的数据文件读取预算。该批次直接向 Native 提供字节区间，跨主机 I/O 继续在 A7 验收。 [Iceberg 文件区间元数据](https://iceberg.apache.org/spec/)

macOS arm64 重编受影响的 Native 对象后，83 项隔离 Native 用例、56 项 Java 定向用例和 24 项 JNI 端到端用例通过。两个独立 Worker 完成 Join 和全局 Top-K。测试轮询现在将非终态 `NONE` 识别为无结果流的在途查询，等待提交终态。完整平台矩阵由 CI 验收。

使用生产协议类型组装的 64 个合成 Paimon 合并分片在两个任务分区中各分配 512 MiB。按字节分配时，两区分别承担 1,024 和 32 个输入文件，计入文件开启和解码后，两区均为 528 个输入文件。Iceberg SDK 夹具验证位置删除文件进入调度估计，发给 Worker 的扫描范围保持原样。分配夹具只计量文件和字节。线上尾延迟及权重校准仍需受控混合负载。 [Spark 文件开启成本](https://spark.apache.org/docs/3.5.6/sql-performance-tuning.html)

本地 Corretto 17 定向回归通过 75 项，包含两个独立 Worker 的 Join 和全局 Top-K、Iceberg DELETE SQL、Paimon MoR 与 MoW 删除向量，以及 Lance 发布和读取。Iceberg DELETE 用例的表声明了 ORC，实际读取 Parquet 并执行了覆盖写。使用已核对 Native 源码和协议输入的 macOS arm64 产物。Checkstyle、模块清单和文档引用检查通过。

Paimon 历史绑定让 schema 和统计共享首次读取的快照。真实本地文件系统样本中，同一个快照的存在探测从 3 次降到 0 次，文件开启从 4 次降到 2 次。两次开启分别用于选择快照和发布绑定前复验。计数覆盖该快照文件的 SDK 调用，远程 GET 和查询延迟仍需按存储环境测量。

真实 Paimon 1.4.1 文件目录在绑定后复用已经读取并校验的快照构造普通 SDK 扫描视图。当前 schema 路径的快照打开从 3 次降至 2 次，历史 schema 从 5 次降至 2 次，存在性探查均为 0。测试先固定旧快照，再追加新文件，随后校验分片仍只包含旧快照的一行。普通表与 data evolution 表都验证了快照在规划期间被删除时拒绝发布分片。计数仅覆盖快照文件在绑定后的实际 SDK I/O。

Paimon 普通扫描的 manifest I/O 探针使用 12 次真实 SDK 提交。把候选条目上限设为 1 时，SDK 仍打开了 12 个实际 manifest，最终返回额度错误。在 `FileIO.newInputStream` 暂停后端时中断规划线程，线程已经退出，仍有 5 个后端打开调用滞留。解除阻塞后计数归零。准入需要在提交实际打开前执行，状态发布需要等待已进入的 I/O 操作释放。 [Paimon 1.4.1 并行 manifest 读取](https://github.com/apache/paimon/blob/release-1.4.1/paimon-api/src/main/java/org/apache/paimon/utils/ThreadPoolUtils.java)

同一 12 文件样本启用打开前准入后，1 个 manifest 打开额度只发生 1 次实际打开，并拒绝整个计划。编码字节额度为 1 时，SDK 在解码元数据过程中退出，数据 manifest 打开次数为 0。通过真实 `CoordinatorTaskTracker`、Paimon 读绑定和 SDK，中断规划线程时 2 个打开调用停在受控 FileIO 边界，Coordinator 无法认领查询清理。放行后两个调用实际退出、清理认领成功，实际打开总数保持 2。这里验证的是本机 FileIO 和 Coordinator 清理栅栏。

两个实际 Paimon 表分别提交 12 次和 16 次。先测量冷目录下两表各自的编码读取量，再给每次扫描足够单独完成的同一额度。两表共用阶段时，第一张表完成并扣减实际读取的字节，第二张表返回额度错误。对第二张表单独使用同样的初始额度后，扫描完成。预算包含并行读取尚未退出的字节预约。

用 Paimon 1.4.1 的 `ThreadPoolUtils` 和计数执行器验证批次分配。输入 8,192 个 manifest 候选，普通批处理在消费前已提交 8,192 个 Future。SDK 的顺序批处理在消费前提交 0 个，取首条结果时提交 16 个。完整消费的记录与顺序相同。此验证只覆盖 SDK 调度器。生产扫描还需要保留分片生成、删除条目合并和按模式选择快照的语义。 [Paimon 1.4.1 批次调度](https://github.com/apache/paimon/blob/release-1.4.1/paimon-api/src/main/java/org/apache/paimon/utils/ThreadPoolUtils.java)

将同一批 8,192 个候选交给 Paimon 1.4.1 的真实信号量执行器，许可数设为 2。原批处理在消费第一条结果前已经完成 8,192 个处理任务，分批执行器在同一位置完成 0 个。两种方式完整消费后顺序一致。线程并行度可以约束同时运行的操作，Future 和结果的留存量仍需在消费边界限制。 复核固定版本源码确认，`AbstractFileStoreScan.readFileIterator` 只为新增条目启用分批读取。全量快照的删除条目仍经 `FileEntry.readDeletedEntries` 为每个含删除的 manifest 提交 Future。A5 的验收要分别测两类输入的峰值待处理任务、选中条目与实际对象读取次数。

Paimon 1.4.1 在普通扫描中为新增与删除条目分别调度 manifest。顺序批处理只影响新增条目，`FileEntry.readDeletedEntries` 仍一次提交全部删除候选。完整收敛需要让同一 SDK 调度窗口覆盖两类任务，并让窗口在取消、超时或额度用尽后停止补充任务。验收应覆盖有真实删除条目的快照、结果顺序、候选数量和退出后的在途 I/O。 [Paimon 1.4.1 扫描](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreScan.java) · [删除条目调度](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/manifest/FileEntry.java)

在 Paimon 直接追加区间的真实文件系统样本中，四个快照在分片准备阶段的开启次数从 2、2、2、3 降到 1、1、1、2。普通表与启用 data evolution 的表均覆盖单遍枚举和耗尽阶段容量后停止开启后续快照。上端的两次开启分别用于选定和末次复验。跨主机延迟和在途存储调用退出仍待验收。

`CHANGES BETWEEN SNAPSHOT` 已走标准鉴权、绑定、规划和 Native Flight 链路。Coordinator 的串行 SDK 行读取、全量回退接口和两个注册点已删除。真实 Iceberg 和 Paimon 用例验证区间端点和空区间，Paimon 在上端之后创建新版本并修改 schema 后仍返回历史投影。用例验证过滤、LIMIT、无隐含排序列，以及读完和主动取消后的实际排空。跨主机吞吐和退役压力仍按 B3/B6 验收。

Iceberg 的提交通知与发布收据共享提交后的目录观测。真实 PostgreSQL 的同一份非空追加，生成发布收据时的指针 SELECT 从 7 次降至 6 次，普通追加仍为 6 次。候选的必需发布空追加也为 6 次。发布收据锁定实际提交版本并哈希清单；后续追加推进当前快照后，重放仍返回原版本和原摘要。超时经底层目录核验成功的路径单独打开目录，避免接触可能仍由提交线程使用的 SDK 表。

准入发布版本直接绑定到查询编译器，移除了会话属性中的字符串转换与包循环。包循环检查、46 项定向编译与真实 PostgreSQL 回归、319 项 Iceberg 测试及 51 项 macOS 角色端到端验证通过，均无失败或跳过。组合发布包包含 739 项，SHA-256 为 `675316deab251838f6cb03e1934abecc8c73bed1a662aa369a8f51a7e7574901`。端到端包括普通查询零独立发布目录读取、Dynamic Table 发布、并发写入、双 Worker Shuffle 和 Worker 重启。

单 Coordinator 的两个真实 Worker 持有租户的两个槽位时，按 20 次/秒提交的 45 个请求均收到准入拒绝。释放后按 5 次/秒提交的 40 个请求全部经 Flight 读取到 EOF 并归还租户槽位。另按 8 次/秒提供五类共 80 条 SQL，逐条核对首行、EOF 和已排空状态。样本仍属于单 Coordinator 的本地环境，多 Coordinator 混合负载与跨主机吞吐继续按验收表验证。 [写入发布 PR #116](https://github.com/pangrova-io/pangrova/pull/116)

两套独立 Coordinator 与 Worker 进程共享同一个 PostgreSQL schema 和 LDAP 租户。每个 Coordinator 都保有一个未消费的 Native 结果时，以每秒 40 次交替提交的 40 个请求全部按租户配额拒绝。释放后按每秒 8 次交替提交的 80 条混合 SQL 全部核对首行、EOF、结果值和实际槽位归零。分别重连到两个查询所有者后，24 次状态读取没有触发 query journal SELECT。该样本运行在本机，Worker 各自向所属 Coordinator 上报。共享 Worker、共享入口和跨主机验收继续保留。产物清单为 `675316deab251838f6cb03e1934abecc8c73bed1a662aa369a8f51a7e7574901`。

Iceberg JDBC 的发现与绑定共享当前 schema 和分区规范已有的 JSON 编码。真实 PostgreSQL 基线生成了内容相同的不同字符串，候选直接复用编码。两阶段仍只读取一次目录指针。列新增后读取旧快照仍使用原 schema，分片前继续校验当前表和选定快照。候选通过 52 项定向检查、319 项 Iceberg 测试、49 项 macOS 端到端测试，产物清单为 `9f82bc8d754b8f0261a2254f41949cf1e66189dd7a2eea217428e5421014d85e`。

普通 SQL 在语句分派、视图展开、快照绑定、Calcite 转换和调度重排分析之间共享解析结果。精确文本的有界缓存只保存语法结构和源位置。视图展开按关系名的源位置替换，生成新 SQL 时才解析。Dynamic Table 发布替换直接更新 AST 并保留逻辑别名。具名和数值版本在加载查询所用 schema 前按表绑定。Coordinator 同一次历史查询只构建一个已选定版本的会话规划器。

同机 JDK 17 对照在 8,000 次预热后取 7 个窗口的中位数，每窗口 2,000 次。基线执行普通 AST 获取、时间旅行语法扫描及规划 AST 获取，候选执行共享解析入口。不同 SQL 文本的点查从 20.2 µs 和 20,008 B 降到 15.9 µs 和 11,630 B，CTE JOIN 从 47.7 µs 和 71,296 B 降到 36.2 µs 和 43,074 B。相同 SQL 文本的点查从 18.5 µs 和 9,232 B 降到 0.16 µs 和 16 B，CTE JOIN 从 20.2 µs 和 29,635 B 降到 0.32 µs 和 16 B。统计口径为调用线程的解析 CPU 和分配量。

视图端到端用例发现 Calcite 在构建字符字面量时用 ISO-8859-1 做可编码性检查。上一版本直接执行含中文和补充平面字符的 SELECT 可重现相同错误。规划器类型工厂为字符列和字面量统一提供 UTF-8 字符集及对应排序规则，保留 Substrait 和 Arrow Flight 的字符串编码。 更新了 37 个 TPC 黄金计划的字符类型标注。忽略 UTF-8 标注后，逐份对比与原计划完全一致。

最终生产代码通过 15 项解析、1,525 项 Planner 和 38 项 Coordinator 测试，另有 1 项显式生成黄金计划的用例按默认设置跳过。macOS 的 739 文件角色产物通过 81 项端到端回归和 1 项 PostgreSQL 读取计数探针。覆盖具名及数值快照、嵌套视图的 UTF-8 比较和结果返回、递归 CTE、动态表发布和物理双 Worker Shuffle。角色清单为 `0435901e115309f1bd1287acd194ae514936595ecc9276ffc40d190038ca5362`。

执行所有权回执现在携带 PostgreSQL 同一事务内观察到的租户发布版本。具名 Dynamic Table 和可选的物化视图改写共享同一份查询目录。稳态命中复用本地不可变目录，变化时在一个 SQL 快照内同时读版本和目录。真实 PostgreSQL 和 Flight 对照中，普通点查、重复点查、自连接、双表连接的独立发布目录读取分别由 1、1、1、2 次降到 0，四种查询仍返回同一行。入场语句内保留版本读取，实际选中代次仍取得持久读取租约。

96 项定向 Java 测试和 63 项 macOS 角色端到端测试通过，含 17 项物理双 Worker Shuffle、3 项激活冒烟、普通读取数据库计数及 Dynamic Table、物化视图、Iceberg 历史引用与查询生命周期。两个独立 PostgreSQL 客户端验证未提交版本不可见，提交后新受理的查询观察到新版本。两个目录实例验证发布与暂停后的变更，已固定的旧目录仍保持同一代次。角色清单为 `08bea98839fb3f45c3eca7195bd1996d7f20ec01f4ee6872c44ed536f1157e58`。

真实 QUERY 角色复现 Dynamic Table 创建语句缺少处理器。Coordinator 在组装 SQL 分发器前绑定执行权威，启动选举后再开启维护任务；停止时在查询排空后取消控制面结果的过期计时器，之后等待调度线程退出。上一版相同 Native 和协议的独立进程产物仅重编三个 Coordinator 启动类，清单为 `7a46929b26d18f7b5dba938911ecc4f15f544b622fda0acf3108e4299a256031`。双 Coordinator 和双 Native Worker 共用 PostgreSQL，验证目录发布、缓存命中零独立目录读取、暂停、删除与同名重建。暂停前已受理的查询在暂停后读完旧代次，读租约在排空后释放，同名新表返回新代次的数据。最新主分支的 Native 运行时和协议有后续改动，整合提交的完整回归仍走发布门禁。

首次取得执行所有权先在事务级 advisory lock 内尝试插入。只有插入冲突才锁行读取现有所有者。插入后的新语句以同一次数据库时间观察检查读取保护窗口并设定租约。相同 PostgreSQL 用例的独立所有权行读取从 1 次降到 0 次。两个连接在旧 anchor 删除的同时提交读取保护窗口，确认等待中的后继既未取得所有权，也未留下临时 anchor。15 项 PostgreSQL 测试和 43 项 macOS 角色端到端测试通过，含物理双 Worker Shuffle、查询生命周期、读库计数、Dynamic Table 和激活冒烟。两项时序相关用例独立复跑通过。角色清单为 `d46d6c6c8ee0fcc26f18ff49b3aeec2c402679f79cc2a75820c8a38169a9bcaa`。
基于主分支 `833a8d8c6` 重新打包 Coordinator、Worker 和 CLI，并依据相同的 Native 运行时源码、构建配置和 Protobuf 输入复用已验证的 Native 库。macOS arm64 和 Linux arm64 的角色包分别校验 739 个文件，清单 SHA-256 分别为 `df98c71f8a09101faf6845cc73b8da1f976c50aea3b03e706bcf8773bd81dbf9` 和 `e0e7c9c7f6acf29c6d9a58a35852aa7df0c32efce64a865c389c7bf8e9feeae9`。

首轮同组回归发现 COW 写入测试恢复表控制服务时，丢失了共享夹具的统计批读配置，令随后的 JOIN 独立查询统计和目录各两次。夹具恢复时复用原来配好的统计服务，366 项元数据测试和 63 项 macOS 端到端组合回归全部通过。真实 PostgreSQL 记录显示，运行过 `ANALYZE` 的两表 JOIN 只发出一次合并读取，独立统计和目录查询均为零。回归同时验证四表策略批读、双 Worker 多键剪裁、Flight 首批与 EOF、取消、COPY、CDC 和租户槽位归还。Linux arm64 的 9 项独立进程回归全部通过，覆盖过载、Iceberg/Paimon/Lance 写入恢复，并从发布包定位了 `ExchangeInputLeafDetail`。

一次编译中已经通过授权的不同表，在同一条 PostgreSQL 语句读取表身份、当前目录版本、行策略、列掩码和治理绑定。基线和候选都执行四张 Iceberg 表加一个重复别名的相同 SQL，真实 PostgreSQL 策略读取从 4 次降到 1 次，授权主体读取仍为 1 次，初始读准入仍为 4 次。Flight 逐列结果一致，候选确认 Native 执行和物理资源清理。112 项定向、53 项同源 macOS 端到端和 9 项 Linux 独立进程回归通过。首轮 Linux 在该过载夹具的状态上报代理记录一次连接写入失败，相同产物的定向复测和整组串行复测均通过。角色清单 SHA-256 分别为 750ed7d7346971fb5f964918456f3cb52e5502e2a8c2da6634d9ebbd2242bb53 和 3f921b07d5fefebb986b976af05380f57bc5f3d18b1dffbb4bcd2c0dd88b1993。

Native Flight 删除注册前的独立等待和全局唤醒。合法签名的已排空票据在 macOS 三次真实 DoGet 中，拒绝耗时中位数由 757 ms 降到 6 ms。每次均先读取值 42、验证 EOF 并观察 Native 资源归零。单领取、权限和取消的回归保留。Java 夹具按实际 OUTPUT ACK 等待，消费者等待和结果行数均使用 Velox 的 CallbackSink 检验。92 项 Worker 定向、50 项同源 macOS 端到端、6 项独立 Native Shuffle 入口及 9 项 Linux 独立进程回归通过。macOS 与 Linux arm64 各通过 35 项定向和 4,367 项 Native 全量测试。首次 Linux Native 全量运行中的 Lance Reader 记录了一次 PostgreSQL journal lookup 错误，同源单独复测和整组串行复测均通过，首次原因仍待确认。角色清单 SHA-256 分别为 f21060f2378295233bbfa2a60a22f4f959f2920cd7e1f54ff8b162e5cfe902f3 和 e585213845c88a7dcd6bfc7a9f3b1b28e5c5d64b5c32b424ead8fe312b1f1a48。

Worker OUTPUT 的唯一可读信号来自 Native Flight 注册并完成 Driver 启动。同一消息用于原始提交和精确重放。已校验的资源授权直接读取 Protobuf 类型字段。相同规范字节的 Corretto 17 对照中，普通资源授权的校验由 424 B 降到 0 B 分配，全字段由 792 B 降到 0 B。持续拒绝逐字段溢出和未知字段，Flight 核对精确签名。79 项定向回归、静态分析、macOS 与 Linux arm64 各 52 项定向和 4,367 项 Native 全量测试通过。50 项同源 macOS 回归和 9 项 Linux 独立进程回归通过，覆盖实际结果读取和释放、双 Worker、过载、PostgreSQL 查询预算、COPY、CDC 与写入恢复。角色清单 SHA-256 分别为 d83f45cf0f77aab2d4a601bf5dd684578b451d953e4d1350b992ad974558f85d 和 7b368db18675753e32e7a54b87b63a8a5e668db7699184b992aa1b6d5d240606。

先前一轮 Linux 回归中的 Iceberg 恢复超过等待上限，单独重跑覆盖 Iceberg、Paimon 与 Lance 均通过，初次超时的原因仍待确认。另一轮在 Coordinator 启动前因 LDAP 引导期间监听切换导致绑定失败。连续 6 次容器复现实测端口先可连接，LDAP 初始化随后完成。夹具现等待初始化结束与最终监听，并通过真实用户绑定验证；修正后整组独立进程回归全部通过。

已受理的 Native 扫描分片从 JNI 移交执行器，再由实际扫描任务取得，移除了两份暂存分片列表和一次重复集合去重。现有分片 ID 校验、绑定、顺序和扫描覆盖继续验证。在实际 SplitConfig 类型的受控构造探针中，1、128 和 4,096 个分片的线程分配量分别从 6,936 B 降至 1,624 B、1,044,424 B 降至 209,760 B、33,486,792 B 降至 6,717,280 B。macOS 和 Linux arm64 各通过 117 项定向测试和 4,367 项 Native 全量测试，零失败和跳过。

删除了只服务于旧任务转换的 Protocol 和 Coordinator 转换器。任务种类直接来自 canonical request，真实 HTTP 断言保留计划、分片、attempt、属性和资源配额，以及缺失配额的拒绝。82 项定向回归证实协议、Coordinator 派发、Worker 准入和实际释放。共同打包的 41 项 macOS 回归和 9 项 Linux 独立进程回归通过，覆盖双 Worker、真实向量检索、COPY、PostgreSQL 查询预算、租户过载与 Iceberg、Paimon、Lance 写入恢复。739 项角色清单 SHA-256 分别为 413e57c638d89fc3c6f247a3fe6c365e1fce0d7980429e46bb758882eeb75783 和 399475854324e6442962df5fc82841ad1b002529d7c796da8fab655abc704156。

Worker HTTP 准入、内存和回报直接读取已经校验的 canonical task。移除每份扫描 split 到 Java 描述对象的转换。Corretto 17 按相同的 1、128 和 4,096 个分片隔离测量，分别去掉 400 B、3,968 B 和 115,072 B 线程分配。定向回归验证原始任务身份、内存 grant、COPY、RESULT 和终态。1 MiB 上限用有实质内存需求的 Velox 排序负载复现 MEM_CAP_EXCEEDED，确认失败与实际资源归还。85 项定向回归、27 项同源端到端回归和 1 项隔离 JNI/远端 Shuffle 通道准入通过。角色清单 SHA-256 为 1790c778b4c7cea12ba7897b0327343210da41539e9dca09f6b034ceff7abdd9。

受控真实 Jetty/JNI/Flight 把 OUTPUT 的 Native 启动暂停在 HTTP ACK 之后。原始 HTTP 返回带有效票据的 200，首次 Flight 在客户端计时 897 ms 后返回 UNAUTHORIZED。放行后用同一票据读到一行、值 42 和准确 EOF，随后任务完成且 Native 内存归零。单独的真实 Native 重复实验得到同一现象。修复后，受控暂停 900 ms 时首次和重放均等待同一 Native 注册，Worker 归还 128 MiB 请求体预算。首次 Flight 读取成功并取得完整结果。注册前的 Native 版本不匹配返回同一因果 500，精确封口返回 409，两者释放实际句柄和内存。

Worker 的任务 HTTP 解码、排队和首次设置共用 128 MiB 编码字节预算。真实 Jetty 测试耗尽预算后验证固定长度与分块请求都收到结构化 503，归还后同一请求可继续正常校验。Native 设置的唯一所有者接收预算，重复、畸形、超限、拒绝和关闭均归还实际持有量。61 项定向测试和 35 项同源角色包回归通过，覆盖双 Worker Shuffle、COPY、CDC、AI、取消、过载与物理释放。角色清单 SHA-256 为 `91307c6c550dcbaf56fa7feea05d84d8559b020d47de9b7fab2c487368e24ee8`。预算计量编码字节，混合负载的进程实际内存与尾时延仍作为单独验收项。

Native 每份任务只解码一次 Substrait。产物检索在任何外部读取前检查同一解析结果的版本，Velox 转换复用该对象。检索准备只持有实际的分片覆盖和租约。原始扫描分片沿用执行配置。相同字节的受控解析对照中，56 B、1,119 B 和 36,836 B 样本的解析分配量分别从 1,392 B 降到 696 B、40,000 B 降到 20,000 B、1,246,272 B 降到 623,136 B。macOS arm64 与 Linux arm64 各通过 47 项定向和 4,367 项全量 Native 测试，零失败、零跳过。41 项 macOS 同源打包回归与 9 项 Linux 独立进程回归通过，覆盖真实向量检索、双 Worker、过载释放及 Iceberg、Paimon、Lance 写入恢复。角色清单 SHA-256 分别为 2af684cc28f5cd8b1c3ced528992586ed6acd7166df7b70b2c9b2ad66925fb96 和 a9ce6079f99dff8a8aea7b464a0e84e40d2a3fdf7ccba4b2c152d4b8be3b9ebf。

Iceberg JDBC 的完全限定表名优先检查精确命名空间。同一实际 PostgreSQL、HTTP 和 Native Flight 对照每秒 25 与 60 次，各完成 20 笔查询并核对结果和物理归还。额外创建 1,024 个无关命名空间后，每组命名空间读取从 40 次、20,520 行和 4,280 次共享缓冲区命中，降到 20 次、20 行和 40 次命中。候选在目录扩充前后的读取量相同。两个目录列表查询各从 20 次降到 0 次。每笔仍读两次表指针和核对四次执行所有权。测试涵盖精确大小写、无精确匹配时的大小写解析、空命名空间、外部更改及完整目录列表。37 项焦点回归和 32 项同源打包回归通过。角色清单 SHA-256 为 9acc3c47f8eb2a92e5df5bf07812bf0d3980eefe6a11dfb7123cf2428d28c8b6。 [Trino Iceberg 元数据实现](https://github.com/trinodb/trino/blob/master/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMetadata.java) · [Iceberg 的 Spark 表解析](https://github.com/apache/iceberg/blob/main/docs/docs/spark-queries.md)

Worker 的二进制任务请求通过有界流进入 Protobuf，删除 Jersey 中间整包数组。实际 Jetty 收到声明超限的请求，仅发送一个字节即返回 HTTP 413。无声明长度的合法字段流在第一个超限字节被拒绝。原有不规范字段顺序的 COPY 证据仍逐字节保留。Corretto 17 的 Jersey 与 Protobuf 解码对照中，含 1、128、4,096 个分片的合成载荷，每次解码线程分配量分别从 11,416 B 降到 5,944 B、165,688 B 降到 86,824 B、5,071,312 B 降到 2,403,880 B。4 MiB 以单个计划字段为主的载荷从 25,167,528 B 降到 8,421,368 B。四组解码后的规范字节与摘要逐一一致。75 项焦点和 34 项同源打包回归通过。角色清单 SHA-256 为 d169ade77fb0074a001bcac11c7ace21fc81a84e24da5400cdcfc7c95f3fb2cb。

Worker 首次任务初始化失败时，首次请求和精确重放收到同一个结构化拒绝。真实 JNI 夹具分别在已分配 Native 执行器之后注入运行时异常和类链接错误。原入口返回 HTTP 200 且缺失 Shuffle 端点。候选均返回 HTTP 500，带原始错误，验证了相同的重放字节、单次初始化、FAILED 终态和实际 Native 资源归还。31 项焦点和 34 项同源打包回归通过。角色清单 SHA-256 为 c518ee10e3990ee02c5b0b2c09b164922624e4663e5aa286979ccdc4c0902197。

首次 Worker 任务在取得精确所有权后交给独立执行器，最多 64 个运行、64 个排队，合计按原始请求长度限制在 128 MiB。出队前检查期限、控制与执行所有权和取消封口。受控 Jetty/JNI 对照中，72 个首次提交在两版都成功、实际资源全部释放。暂停生产者注册期间，基线健康检查在放行后返回，耗时 889 ms。候选在注册仍暂停时返回，耗时 20 ms。这是受控窗口观测。58 项焦点和 35 项同源打包回归通过，覆盖真实双 Worker、COPY、CDC、AI、过载、取消和 JNI 释放。角色清单 SHA-256 为 923a7cac06cebef689cd3dbb6ce4bb41e040995202b088711b86fb4167586dc9。 [Jakarta REST 异步资源接口](https://jakarta.ee/specifications/restful-ws/4.0/jakarta-restful-ws-spec-4.0.html#asynchronous_processing) · [Trino Worker 任务入口](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/TaskResource.java) · [Spark Executor 任务启动](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/executor/Executor.scala)

Worker 精确重复提交通过原任务的共享完成信号异步等待 ACK，HTTP 观察截止只终止该次等待。受控 Jetty/JNI 对照中，80 个重放令基线 58 个 Jetty 线程等待，候选降到 0。注册仍被暂停时，候选健康检查在本次窗口耗时 20 ms，基线在释放注册后返回，本次为 832 ms。另一个 140 个重放窗口中，128 个得到相同 ACK，多余 12 个收到结构化 HTTP 503，只执行一次真实生产者注册，等待容量全部归还。45 项焦点回归和 34 项同源打包回归通过。角色清单 SHA-256 为 84a8210950be39905386cc7d7ae2b1d074204b12c0180359e213e403d0457bef。

TaskReport 的入口解析结果和原始字节由同一不可变对象持有。真实 PostgreSQL 持久执行测试以非标准字段顺序提交 189 B 合法消息，Java 解析由 HTTP 入口和持久提交各 1 次降为仅入口 1 次。库内保存的字段顺序、字节和 SHA-256 与原始消息一致。普通查询继续只在入口解析。144 项焦点回归和 49 项角色打包回归通过，覆盖 PostgreSQL 代际、旧所有者、真实 JNI 终态、远端与双 Worker Shuffle、DML、CDC、COPY 和实际释放。角色清单 SHA-256 为 d162e0a9eb538561d86bba857838ce7d489c131dd36a7ea4ec775a703e584d11。

Worker Java 使用已经受理的任务直接构造 AI 回调授权对象，在预算注册时复用同一份精确 attempt、授权和资源限制。真实 JNI 回调仍使用二进制并校验其来源证据。双 Worker 对照两版各完成 40 笔且产生 45 次预期的配额拒绝，逐笔检查结果和实际归还。相同的 122 份任务计划按大小和类型匹配，Worker Java 的 SHA-256 从 244 次减少到 122 次，处理的计划字节从 55,952 B 降到 27,976 B。仅由本地 AI 注册触发的回调 protobuf 编解码从 122 次降到 0 次。91 项焦点回归和 43 项同源打包回归通过，包含独立进程的 AI provider 调用、FILE 预算校验、COPY、双 Worker 执行和真实 JNI 资源归还。角色清单 SHA-256 为 2a4aa8fc2be9da6b40364ab242f49a824cb43112b3a9a4e2ab77e9fc16e98e08。

Worker 的同一精确任务重复提交等待实际端点注册。真实 Jetty/JNI 用例在 Native Shuffle 注册前暂停第一个请求。基线重复请求提前返回 HTTP 200，缺失端点。候选只注册一次，两次 ACK 都等待注册，返回相同的两张分区票据。取消仍能按精确任务所有权找到初始化中的任务，已封口的准备任务不会收到成功 ACK。51 项焦点回归和 33 项同源打包回归通过，涵盖双 Worker、取消、过载、COPY 和实际归还。角色清单 SHA-256 为 da0e34222cbd003f978d2b379b964d1c2bb9600d7f233e0d6d3b160d68b3839d。

Worker 任务接入沿路复用同一份经过结构校验的请求，移除 Native 创建前在 Worker Java 中再次校验原始请求的步骤。实际双 Worker 对照完成同样的 40 笔固定到达率工作负载，并各产生 45 次预期的租户配额拒绝。按任务类型和计划大小逐项核对，共 122 个计划，Worker Java 计划 SHA-256 次数从 366 降到 244，输入字节从 83,928 B 降到 55,952 B。存量的另一遍来自 AI 生命周期把任务投影成回调上下文，随后执行 Java 内部 protobuf 编解码。独立 JNI 校验保留。42 项焦点回归和 34 项同源打包回归通过，涵盖 COPY、双 Worker Shuffle、CDC、租户准入和资源实际归还。角色清单 SHA-256 为 4cea99883374d76c9780f3cb99017693fd2de3b00ea19eeda8f1f6c6f1f0a956。

Worker 放置阶段使用一次当前租户状态和资源池投影，保留开始派发和轮次切换时的权威检查。真实 PostgreSQL 独立进程中，普通查询的成员授权读取维持 1 次，完整租户行读取从 1 次降至 0 次，候选的两列放置读取为 1 次。撤权后的下一笔请求被拒绝，槽位归零。64 个分片的同一派发使用一次投影，后续放置能看到新的资源池或停用状态。46 项焦点测试和 47 项同源打包回归通过，包含实际 Iceberg、Native Flight、双 Worker、租户配额、撤权和清理。角色清单 SHA-256 为 db7f4e0a18298ef01e267abbd121c07e8dad7142d3a7b25bde151ebb8006dc14。

前台表授权从同一 PostgreSQL 语句快照获取当前登记、目录版本、行策略、列掩码和治理绑定。使用带类型的结果行，复用现有 JDBC 解码。实际 Iceberg、HTTP 和 Native Flight 每秒 25 次对照中，两版各完成 50 笔，原来表身份与治理读取和行列策略读取各 50 次，候选合并为 50 次。每笔 4 次执行所有权校验继续通过，结果和实际归还逐笔核对。28 项真实 PostgreSQL 与 100 项同源打包端到端回归通过，包含旧表 ID 隔离、过期目录拒绝、PII、双 Worker、子查询和写入。运行包清单 SHA-256 为 `a8c0f99faf8543ff6a608fe9e0a6d22e3cbd2dbdcc2bb2d4e0650f50ef28f206`。

Native Shuffle 的标准单页 Flight 响应直接持有已经接收并对齐的 Arrow 数据缓冲区，去除入 Velox 队列前的一次整页复制。真实用例将 1,048,833 B 页克隆后销毁最初的 Arrow 持有者与 Velox 页，克隆仍读取同一地址和内容，最后一个引用释放后缓冲区随之归还。保留逐页队列计量和跨多个值时的独立缓冲区。macOS 和 Linux arm64 各通过 4,367 项 Release Native 测试，零失败、零跳过。同源 macOS 角色包 42 项和 Linux 9 项独立进程通过，包含双 Worker、远端 Shuffle、取消、实际资源归还和三种连接器写入恢复。macOS 角色清单 SHA-256 为 `1a84debf13baaae5aff306ec0c124b39f3cadc542ab0026384d738ec5092f6ec`。

同一 Worker 的并发 Native Shuffle 流共享当前 TLS 配置下的 Flight 客户端。真实双流对照中，服务端收到两个独立请求，TCP 连接从两条降到一条，队列维持原有背压。取消第一条流后，第二条读完其结果并到达 EOF。两项各重复 20 次通过。弱引用归还最后一条流持有的连接，热点端点的额外通道保留并发流容量。Linux arm64 和 macOS arm64 各通过 4,366 项 Release Native 测试。相同代理干扰下，macOS 同源角色包 42 项与 Linux 9 项独立进程回归通过。macOS 角色清单的 SHA-256 为 `b37d1d5778dc8968520ae5df1d875efb236caec9493e9c91eae5b50c9585b332`。大规模跨主机负载仍需按连接数、并发流、背压和尾时延校准。 [Arrow Flight 客户端复用](https://arrow.apache.org/docs/cpp/flight.html#re-use-clients-whenever-possible) · [gRPC 并发流](https://grpc.io/docs/guides/performance/)

前台读在同一 PostgreSQL 语句观测表登记、目录版本和稳定表 ID 所属的治理绑定。策略继续按当前本地目录和同一表身份应用。每秒 25 次的两版各有 50 笔真实点查完成，独立治理绑定 SELECT 从 50 次降到 0 次，候选执行 50 次联合读取，实际结果和资源归还逐笔核对。真实 Iceberg 查询按 PII 标签遮蔽邮件地址，Paimon 用例覆盖重命名、移除标签和再次创建同名表。49 项 PostgreSQL 与 76 项打包端到端回归通过，含列掩码、行过滤、子查询、双 Worker 执行及写入。运行包清单 SHA-256 为 `9471fdb45e2d3d5bb85fa3952ca9246e6471e717567cd261c052e664ceb3a7c9`。

Native Shuffle 直连 Coordinator 签发的 Worker 域名和端口，使用同一 Worker TLS 身份与任务票据。在可控代理对照中，基线的 gRPC 客户端错误地尝试连接进程代理，候选直连并读回三行。macOS arm64 和 Linux arm64 的 Release Native 各通过 4,365 项测试，零失败、零跳过。使用相同无效代理环境，同源 macOS 角色包的 36 项端到端回归以及 Linux 的 9 项独立进程回归通过，覆盖双 Worker 直接交换、对象存储远端交换、租户超载与资源归还，以及 Iceberg、Paimon 和 Lance 写入恢复。macOS 角色包内三处 Native 资源与 Release 库逐字节相同，清单 SHA-256 为 `e99e5f6f073bcab83e0d37a019c018a06bd04f9411cf3fade2fd9e3857ce1fbf`。

首次受理在执行锚点 INSERT 内分配并返回 fencing token。先取得同一执行 ID 的事务锁，再分配令牌，插入后以已取得的行锁为起点计算租约。真实 PostgreSQL 单笔对照中，独立令牌 SELECT 从 1 次降到 0 次。实际 HTTP、Iceberg 和 Native Flight 的每秒 25 次对照中，两版各完成 50 笔，独立令牌 SELECT 从 50 次降到 0 次，读结果和物理资源归还逐笔核对。候选每秒 60 次的同机窗口也完成 50 笔。23 项 PostgreSQL 与协调回归、58 项打包端到端回归通过，涵盖并发插入、同 ID 重用、到期接管、租户资源归还及三种连接器的独立进程恢复。运行包清单 SHA-256 为 `5a36d33d3801031b9e91c48208c5fa77415ebd1512fde73d9f780240b9ca9eb1`。

前台表策略在 PostgreSQL 的同一语句快照读取启用的行过滤和列掩码。普通点查对照使用完整限定表名、实际 Iceberg JDBC 目录、HTTP 提交及 Native Flight 读回。每秒 25 次时两版各完成 50 笔，独立的行策略与掩码 SELECT 各从 50 次降到 0 次，候选使用 50 次联合读取。逐笔核对结果和物理资源归还。候选每秒 60 次的同机窗口也完成 50 笔。保留当前表身份与治理绑定的权威检查，以及每笔 4 次执行所有权校验。7 项 PostgreSQL 与 86 项打包端到端回归通过，涵盖同名或已重命名表、行列授权、双 Worker 交换及写入。运行包清单 SHA-256 为 `531ec914de21167a107d51af2f6dedb944152d124d9b3003d25ac6ea8129f44b`。

在同一个 Iceberg 内连接中，调度器最多对四个已解析的 BIGINT 物理列联合剪裁。在等价布局的双 Worker 夹具中，同一 SQL 的探测分片从 15 个降到 6 个。定向回归中 Native 实际读取从 16,041 B 降到 6,416 B。包含其他端到端场景的独立运行再次读取 6 个分片，实际读取 7,846 B。两版均返回完全相同的 48 行，验证了重复值、NULL 和物理归还。候选的两组单键对照读取 15 和 16 个分片。构建端保持选定获胜任务、发布完成和原有 200 ms 等待边界。macOS arm64 和 Linux arm64 各通过 99 项定向及 4,368 项 Release Native 测试。同源 macOS 角色包端到端 51 项、独立 Shuffle 6 项和 Linux 独立进程 9 项通过。 macOS 和 Linux 的角色清单 SHA-256 分别为 `39a9c3cdac6771ca7c60a8e96ca04b519a11fb381cc7cbdf2755587840cd34ae` 和 `e282e6b6da8ae8a48de378639e29eaca55e3a5efb2f7e15e14c51a33b14094f5`。

复合 INNER JOIN 可在必需的等值项中选出一个能追溯到 Iceberg 物理字段的 BIGINT 键，用于派发前裁剪分片。Native 继续执行原始连接的所有条件。双 Worker 对照在同一快照上返回相同的四行，保留重复键并核验 NULL 和额外不匹配条件。探测分片从 40 个减到 3 个，Native 实际读取从 30,495 B 减到 2,389 B。55 项规划与 37 项打包端到端回归通过，覆盖单任务和多任务的过滤发布、空域、外连接未过滤、物化及资源归还。运行包清单 SHA-256 为 `3df537497af026cb061967d6f1cbedb5a12bc69598fbc28a63153fcc982df6d5`。

前台查询用一条 PostgreSQL 语句读取有效角色和授权，表访问检查及行列策略共用这次读取。每秒 25 次的两版各完成 50 笔真实点查，独立角色与授权 SELECT 各从 50 次降到 0 次，候选使用 50 次联合读取。候选在每秒 60 次的另一窗口也完成 50 笔，核对了 Flight 结果和实际资源归还。保留了无授权角色、用户直授、组授权、去重和撤权行为。70 项存储、策略及 Coordinator 安全测试和 83 项端到端回归通过，覆盖两 Worker 交换、角色及策略、LDAP 与 OAuth。角色运行包清单 SHA-256 为 `7e160bf42fff3af6895b2a281c23cfa4fb2e58a89095edff93ec6848bcafe574`。

首次查询租约用 INSERT 及取得行锁后的 UPDATE 原位采样数据库时钟，后者返回实际持有的租约。独立 PostgreSQL 16 对照中，每次首次受理的独立时钟 SELECT 从 2 次降到 0 次，回执到期时间与落库时间一致。等待不可见 INSERT 回滚及接管到期旧记录的测试继续验证锁后起算时间。每秒 25 次时，两版各完成 50 次真实点查，整个窗口的时钟 SELECT 从 105 次降到 2 次。此统计也包含后台操作。两版均保留每条查询 4 次所有权校验，结果、终态和实际归还逐条核对。14 项 PostgreSQL、57 项集成及 9 项独立进程回归通过。独立进程覆盖租户槽位耗尽与恢复、Coordinator 重启及 Worker 退出后的 Iceberg、Paimon 和 Lance 写入恢复。角色运行包清单 SHA-256 为 `e3dc823fe8973c8f61555511f4fb49c07b2adcb3f9ba1da8f75101d03642b9c8`。

Linux arm64 的 Release Native 通过与数据库联动的 4,365 项全量测试，零失败、零跳过。向量检索夹具曾有两个进程撞到同一临时路径。改为原子分配独占目录后，把 6 项相关测试按并行度 4 各重复 20 次，通过后再以并行度 3 复跑全量。Native 库 SHA-256 为 `6940c9336fd432c97bef17829eb5a5d6a690aa7f6be5f8d27e9538acf7b66db1`。25 个 JNI 方法的导出校验通过。

普通 Iceberg JDBC 表发现与读绑定共用一次当前目录观测。实例只在该次 SQL 编译内使用。真实 PostgreSQL 的两个目录会话在发现后替换同名表，编译保留了原表身份，分片前校验拒绝替换表。每秒 25 次的两版各完成 50 笔真实点查，目录指针 SELECT 从每笔 3 次降到 2 次，两版均保留每笔 4 次所有权校验。每秒 60 次的本地窗口与后台构建共享主机，基线与候选各受理 46 和 36 笔，分别计得 138 和 72 次指针 SELECT。逐笔校验受理查询的结果和资源归还。35 项规划、39 项连接器及 77 项端到端打包回归通过，运行包清单 SHA-256 为 `6f041a692e9bc5dbd29694aba9e35b0502f9e04f8e694056b1b2d43fd40c0507`。相同点查负载的全量 SQL 统计仍观察到每笔两条命名空间 SELECT 和独立角色、授权及策略读。

Iceberg 在当前读绑定内复用同一份 schema JSON 生成指纹和最终句柄，移除了临时句柄。历史读取分别核验当前身份和所选历史 schema。对同一真实 Iceberg 快照，四次独立 JVM 启动交替执行基线与候选，逐组校验快照、列数、统计和 schema 指纹。256 列时每次绑定分配从 444,412 B 降到 221,352 B，组中位耗时从 311.6 µs 降到 214.8 µs。64 列时分配从 105,340 B 降到 57,336 B，组中位耗时从 86.7 µs 降到 60.4 µs。这组对照度量绑定编码。37 项连接器测试和 66 项打包端到端测试通过，涵盖 schema 演进、历史快照、原子写入及真实双 Worker 交换。运行包清单 SHA-256 为 `fa3b3e57c1e878a6b248bdfece63229a535c21b917de9ecd751f91c10eafa811`。

完整限定的 SQL 表名优先经根目录解析。真实 Iceberg JDBC 目录的调用栈显示，旧路径曾把目录名当作默认命名空间下的表名。当同名表实际存在时，还会读取其快照声明。覆盖同名表的端到端回归中，元数据指针 SELECT 从每条查询 5 次降到 3 次，保留目标表发现、快照绑定和分片前校验。逐条核对完整限定名、带引号的限定名、默认命名空间同名表以及两者在同一个查询中的结果。无同名表时，每秒 25 次和 60 次各 50 笔真实点查均成功，指针 SELECT 从每笔 4 次降到 3 次，每笔 4 次所有权校验不变，逐条核验了结果和物理归还。68 项规划测试、81 项打包端到端测试及规划模块静态检查通过。运行包清单 SHA-256 为 `92e0a7d7b329d1c80b336cf3f18d1e4457ae2c93605e15b8610ce8cfcdc08da1`。

Iceberg JDBC 目录在同一次当前表观测中校验物理 UUID 和位置，并用该表选择快照。真实 SDK、HTTP、Native Flight 和 PostgreSQL 对照在每秒 25 次和 60 次两个窗口各完成 50 次点查，Iceberg 目录指针 SELECT 从每条查询 5 次减到 4 次。两版均保留每条查询 4 次控制面所有权校验，逐条核对结果和资源归还。另用两个独立目录会话在身份回调内删除并重建同名表，确认快照绑定保持已观测身份，后续执行校验拒绝新表。97 项打包验证覆盖连接器、规划和端到端执行。运行包清单 SHA-256 为 `833391045a96a30a768227ddc7340767fb9f3b639e9e7b94c87d30678122a161`。

TPC-H 严格计划基线更新了 15 份聚合计划，反映已有的 Native 十进制 SUM 和 AVG。重新生成后核对了扫描、连接、排序、过滤及集合操作的结构。21 项严格计划对比和 47 项十进制、协议及元数据绑定测试通过。

SQL 验证在打开连接器前用一条 PostgreSQL 语句读表登记和当前目录部署，随后核验本地目录句柄。受控点查中，每秒 25 次的两版各完成 100 次。原来各有 100 条独立登记 SELECT 和部署 SELECT，候选改为 100 条联合 SELECT。两版各有 400 条跨句柄验证、快照绑定和分片枚举的权威栅栏 SELECT。逐条核验 Flight 结果和资源归还。目录变更、目录移除和未登记表的 PostgreSQL 测试覆盖三种结果。双 Worker 的十进制聚合校验包含实际结果和精确查询次数。

Iceberg 空 INSERT 的发布证明复用第一次文件收集得出的空写入判定。用 0、1 和 32 个空工作片段分别跑真实快照提交。首次发布的片段收集从两次减为一次，重放仍返回同一快照与 manifest 摘要。目录发布阶段继续核验当前表 UUID 和精确提交标记。50 项定向测试通过，覆盖原子快照、发布、重放、错误输入和 PostgreSQL 目录预算。打包产物另通过 39 项连接器测试和 41 项端到端测试，覆盖动态表、物化视图、Lance 导出、CDC 与资源归还。运行包清单 SHA-256 为 `23b1b32cace692037b14649a68472e0db27fccdfe9b279108fb09d559789eba6`。

本地固定到达率对照用持久 SDK、HTTP、Native Flight、PostgreSQL 状态库和 Iceberg。每个版本各两次进程启动，使用 8 个数据文件、2,048 条相同逻辑数据，按固定顺序交错常量、点查、过滤聚合、分组和 INSERT。每个版本提交 1,000 次。每秒 25 次时两版各受理 500 次。候选点查、过滤聚合、分组的首行 P95 分别为 90.8、86.6、98.4 ms，基线为 107.4、113.6、146.7 ms。INSERT 实际归还 P95 从 289.9 ms 降到 230.8 ms。每秒 60 次时同一 16 槽位限制下，候选受理 356 次、拒绝 144 次，基线受理 314 次、拒绝 186 次。过载时点查和分组的首行 P95 分别升到 344.8 和 334.3 ms，需继续拆分调度、执行和归还等待。逐条核验已受理读取的值及读写的物理归还，按已受理写入数核验可见行数。状态 journal、独立归还和无关物化 SELECT 在所有窗口均为零。实测仍有每笔写入一次 journal 读取及按实际受理量发生的权威栅栏读。

过载尾部补测在点查和分组各八笔中观察到提交队列等待中位数 72.1 和 103.3 ms，编译 82.6 和 84.2 ms。另一轮的编译拆分中，关系绑定中位数 44.8 和 51.8 ms，策略阶段 17.0 和 24.0 ms。同轮视图与快照处理、优化器和 lineage 的中位数均低于关系绑定。继续核对绑定内部的验证、目录与转换成本。

Iceberg 提交尝试用同一次目录观测匹配 journal 指纹及核验完整祖先链，移除第二套提交 token 和随之触发的目录刷新。PostgreSQL 16 实测普通写入的目录指针 SELECT 从 7 条降到 6 条，要求发布回执的写入从 8 条降到 7 条。JVM 套接字事件将原来的额外查询定位到旧 token 检查。保留原子提交、通知和发布证明的权威观测。83 项定向测试覆盖 25 个后续快照后的精确重放、冲突、超时、过期、回滚、错误租户和重复指纹。打包产物通过 37 项连接器测试和 28 项写入、COPY、双 Worker CDC、Native 归还端到端测试。同一产物以规定顺序校验清单后，通过 7 项独立 CDC 与 6 项关系写入恢复测试。**机制状态：**Iceberg fenced commit、journal fingerprint 和恢复路径已实现。**当前验收状态：**本地连接器与独立进程恢复已验证；跨连接器高并发/故障矩阵、普通 distributed write 的长期 receipt retention 和 Paimon durable abort 尚未完成。**后续 WP：**A12/A13/B8，WP8。

macOS arm64 并行运行 Native 测试时，三个检索测试进程曾因共用临时路径而失败。夹具改为原子分配独占目录。8 项检索测试按并行度 4 各重复 10 次通过，之后按并行度 3 运行 4,365 项 Native 测试，零失败、零跳过。修复只涉及测试夹具。

Iceberg 写入片段直接按 UTF-8 解析。操作和文件列表共用一次 JSON 解析结果，文件描述追加到同一个写入集合。日志指纹继续覆盖原始字节。32 个任务、每任务 8 个数据文件的独立 JVM 对照中，MERGE 片段解码的中位数从 0.971 ms 降到 0.356 ms，每次分配从 3.31 MB 降到 1.30 MB。CDC MERGE 从 1.210 ms 降到 0.492 ms，分配从 4.05 MB 降到 1.81 MB。单独预热的 APPEND 对照从 0.331 ms 降到 0.262 ms，分配从 1.33 MB 降到 0.92 MB。对照均验证操作、文件数量及 UTF-8 路径。此处计量片段解码。39 项连接器测试与 34 项打包端到端测试覆盖真实目录提交、原子发布、重放、畸形输入、PostgreSQL 查询次数和物理资源归还。角色运行包清单 SHA-256 为 `b5aae943a93b38b60ca46e8e5ac70c67dbf71dc15f6929736c9f8d3c880723dc`。

Iceberg 写入预检按精确表名读取当前声明，直接返回类型、字段 ID 和表属性。PostgreSQL 16 的实际 JDBC 目录指针 SELECT 从每次两条减为一条，另一个目录会话修改 schema 后仍能立刻读到新列。真实四笔并发写入中，每笔从受理到调度的 Iceberg catalog 加载从五次减为四次。四笔写入均成功发布且读回预期数据。写入分配校验当前权限，发布核验提交栅栏。171 项打包回归包含 49 项端到端测试。角色运行包清单 SHA-256 为 `f1bacde024b36759c8cff5eedc9eb7618b6ae0e1f7230c4a03cc33124623337a`。

HTTP 提交在执行就绪前挂起，匹配 principal、请求内容及幂等键的重放订阅同一受理回执。提交期限、原始错误、断开观察者与真实执行取消各自由其所有者处理。真实 HTTP 与 Iceberg 对照中，在派发前暂停一笔写入并发出八个匹配重放。旧路径占用一条原请求线程及八条重放等待线程，候选为零。两轮对照均只受理一次，九份回执逐字节相同，目录只有一行可见，物理清理通过。最终打包 95 项定向和端到端测试与 Checkstyle 通过，涵盖请求主体、冲突、拒绝、记录失败、取消和终态。角色运行包清单 SHA-256 为 `cce026a9668632e19d04a4a63283dca76d107d9a6d91bdee0e413d8f966e5da0`。 [Jersey 异步请求处理](https://eclipse-ee4j.github.io/jersey.github.io/documentation/4.0.0/user-guide.html)

字面值 INSERT 与 INSERT OVERWRITE 在预检内共用一次表元数据，列顺序、FILE MIME、托管文件与生成列校验复用同一观测。写入分配和 fenced commit 继续验证当前权威状态。定向对照中，显式列 INSERT 的句柄与元数据调用由 2 次和 3 次变为各 1 次，省略列名的写入由 2 次和 4 次变为各 1 次。真实并发 Iceberg 对照中，四个 INSERT 在受理到调度之间各自的目录表加载从 9 次降为 5 次，可见快照和读取结果一致。此处计量的是连接器加载调用。 88 项定向回归与 Checkstyle 通过，另有真实读写并发用例通过。

Coordinator 提交线程执行规划和启动分布式 DAG，查询运行期间由完成事件推进。同步接口等待同一个结果，DML 与 CTAS 在提交栅栏通过后继续发布控制面结果，取消、阶段期限、写入期限和物理资源归还仍由执行所有者负责。真实单 Worker、HTTP 与 Flight 的旧路径对照中，四条已读完数据的查询保留其终态报告，占满四条提交线程。第五条等待 538 ms，释放一份报告后 33 ms 读到结果。候选在暂留全部四份报告时完成独立读取。四个真实 Iceberg 写入等待提交时也完成独立读取，随后各表读回预期数据并确认物理清理。这项受控对照验证调度隔离。固定到达率端到端 P95 留在发布验收。 164 项定向测试、69 项打包端到端测试及 Checkstyle、SpotBugs 通过。真实回归覆盖双 Worker Shuffle、CTAS、并发写入、拒绝与取消、物化和 Native 资源释放。角色运行包清单 SHA-256 为 `a6e4ddd526faf8da10ee046eadb9a6dcd3bce97f8854b46a01db0303831817fe`。 [Trino 查询生命周期](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/QueryExecution.java)

运行时过滤以构建侧发布、200 ms 截止或查询终态中的首个事件唤醒探测侧。探测分片枚举交给已有的读准备线程，超时走完整探测，晚到报告保持原决策。旧版真实双 Worker 记录中构建等候为 4.22 至 25.93 ms，期间占用提交线程。候选的三项真实双 Worker 验证保留精确范围和查询结果，其中稀疏范围对照从 40 个探测分片降到 4 个。36 项定向测试和 Checkstyle 通过，覆盖首个事件、空构建、超时、取消以及旧代报告。

持久关系阶段在领取分片前解析并验证一次计划与模板，阶段共用 Substrait 字节和校验和。每次领取仍使用当前权限、租约、分片、产物与资源证据，并匹配原阶段和模型声明。本地 32,827 B 计划对照中，64 个分片的准备分配从 34,246,463 B 降到 337,384 B，组中位耗时从 34.29 ms 降到 0.50 ms，计划字节一致。该测量覆盖准备与编码。精确分片领取合并坐标证明和可领取行锁查询。Producer 完成复用同一事务锁定的作业和分片行，保留当前权限与执行围栏验证，并跳过纯 Shuffle Producer 用不到的分片元数据读取。44 项 PostgreSQL 和控制面用例通过。 合并运行包另通过 71 项测试，覆盖 PostgreSQL 取消锁与阶段交换、Lance 导出、目录事务、双 Worker Shuffle 和 Native 资源释放。角色运行包清单 SHA-256 为 `803e2850497f05f7a89a7b8ec4660e4a40bb74a735e55c6c5f35239869dcf07f`。

PostgreSQL 在同一语句中读取策略表和当前目录部署证据。普通前台读校验本地句柄，持久写继续要求当前目录处于 ACTIVE 状态。真实双 Worker SQL 中国录身份查询从 6 次降到 5 次，表名查询从 2 次降到 1 次，新增 1 次合并策略查询。快照绑定和分片枚举前后四个校验随后各使用一条只返回一行标记的权威查询。该查询核对当前表名保留、有效表状态、物理 UUID 与位置，以及目录 incarnation 和 revision。合并后的真实 SQL 中，完整表主记录读取从 4 次降到 0 次，独立目录身份查询从 5 次降到 1 次。核对了 800 行输入、双 Worker 交换和最终精确的 Decimal 结果。PostgreSQL 定向用例验证数据版本推进不影响同一物理表的所有权，并拒绝目录换版、物理身份变化和缺失表名保留。 合并查询批次后，31 项定向 Java 与 39 项真实端到端通过。该批次独立通过 152 模块静态验证。合并运行角色清单为 719088ee1cce312c1658dc93afb02398f8cc0b6605b49183319c23a11d4808fc。

带谓词的窄列 Iceberg 扫描将约束传给分片剪裁，保留谓词所需输入，再把直接输出投影移到排序和 Gather 之前。同布局的双 Worker 对照从 4,948,399 B 降至 179,840 B，两个分片和 1,122 行有序结果一致。阶段准备在枚举成功后保留冻结的分片和来源描述，枚举前后各执行一次权限校验。32 个扫描位点的本地分配从 10,224 B 降至 5,160 B，耗时中位数从 3.78 µs 降至 2.44 µs。生成值来源在同表扫描位点复用已验证的发布信息、schema 和声明，每个扫描独立绑定。32 个扫描的本地分配从 1,437,792 B 降至 538,720 B，耗时中位数从 351.13 µs 降至 107.59 µs。Substrait 编译和阶段绑定共用标准不可变消息，到 Worker 请求边界编码。32 个扫描的最终字节一致，本地线程分配从 556,128 B 降至 83,656 B，耗时中位数从 176.68 µs 降至 48.52 µs。40 项定向、116 项实际端到端和 152 模块静态验证通过，覆盖双 Worker、TPC-H 正确性、生成值、Iceberg 和 Paimon/Lance 原生读写。角色清单为 `df97bdbf3b68fe762f64514e545072c4e736a3fae87352723d1f2261f86b561b`。

下游阶段准备发生 Java 类链接错误时，Coordinator 以原始错误结束 DAG 等待，进入失败清理并释放准备操作。构建隔离后的双 Worker 故障注入从私有诊断类路径移除 `ExchangeInputLeafDetail`。同一条 SQL 在 1,167 ms 返回 `COORDINATOR-0003`，记录 `FAILED`，取消 8 个已派发任务并确认物理清理。随后独立查询正常返回。正常诊断从冻结的类路径读取完整构建产物。37 项定向、30 项 COPY 和索引授权、19 项 Native 写入恢复和 Worker ACK，以及 40 项端到端用例通过。131 模块静态验证通过。角色清单为 `5f16354f46be8d99f5715b517243e89b437a80b2390722b358de6479299863aa`。

受保护编译内的行过滤和列掩码复用同一个当前表身份。实际双 Worker SQL 的目录身份读取从 7 次降到 6 次，表名查询从 3 次降到 2 次。策略按需读取，下一次编译重新取当前授权。查询状态恢复由 Query API 持有 journal admission payload，内存与 PostgreSQL 共用同一 JSON 合同。92 项定向 Java 用例与 93 项实际端到端用例通过，覆盖模块边界、目录读预算、子查询撤权、数据库权限故障、状态读取和恢复主体。重建后的角色清单为 `fbcae01b499303c2d53017a8a0fb5319f2aa0848cd1a4a08e8fb659423a83011`。

窄列 Parquet 批次将投影列数不超过表字段一半时的整文件预载窗口和 footer 探读分别设为 64 KiB。大于窗口的文件可独立读取 footer 与所选列块。在 33 列、24,000 行、两份 Iceberg 文件的实际双 Worker 对照中，单列排序查询从 3,328,539 B 降至 166,515 B，逐行结果一致。同一候选的宽输出为 4,961,691 B，带隐式排序列的窄输出为 205,306 B。另一个带 WHERE 的窄列形态仍达 4,948,399 B，后续按谓词保留必要读取列。上述计量是 Native 向文件系统请求的字节，跨主机对象请求数与 P95 仍需同源环境测量。97 项定向及 4,365 项全量 Native、92 项实际端到端和 Checkstyle 通过。正式回归约束窄读字节小于同表宽读的四分之一。同源角色清单为 `716e44642b0076861cb906759aeef4e6559a8e8bc1ba0868a04debb6a14479a9`。 [Parquet 列块和 footer](https://parquet.apache.org/docs/file-format/) · [Velox reader 配置](https://facebookincubator.github.io/velox/configs.html)

实施批次 169 使用当前目录身份查询校验名称、incarnation 和 revision。真实双 Worker Iceberg SQL 的完整目录部署 SELECT 从 7 次降到 0 次，保留 7 次精简身份读取和原有 revision 校验。普通直接投影用 Substrait ReadRel 输出映射缩小 Native 扫描赋值，保留连接器绑定的基础 schema。初版在全局排序处仍交换所有列。修正版在拆分前把符合条件的直接投影移到排序和 Gather 之前，按投影序号重绑定排序键，并覆盖排序方向、空值顺序、OFFSET 和 LIMIT。两套布局一致的实际双 Worker 对照读取相同的两份 Iceberg Parquet 文件并返回同样的 24,000 行。扫描物化由 12,963,456 B 降至 392,832 B，Native 文件读取由 4,971,586 B 降至 3,328,539 B。Velox 的默认整文件预载仍会读取窄投影用不到的文件范围。77 项定向 Java、104 项真实端到端、4,365 项 Native 全量、Checkstyle 及 152 模块静态检查通过。角色清单为 `d94c48456303f80c3bd0bd658ffdfc78a9c224bd9180c59b9404437d210edaec`。 [Trino 投影下推](https://trino.io/docs/current/optimizer/pushdown.html#projection-pushdown) · [Velox TableScan](https://facebookincubator.github.io/velox/develop/operators.html#tablescannode)

实施批次 168 将 Substrait SDK 的规范化限定到可能包含 Fetch、哈希 Join 或归并 Join 的消息路径，覆盖表达式内子查询。只有确实改动的关系及其祖先新建消息。组序交替的 Java 对照中，单扫描与 24 扫描输入得到相同的输出字节。线程分配分别由 28,040 B 降至 3,496 B 和 1,682,384 B 降至 174,664 B，组中位耗时分别为 50.10→4.19 µs 和 1.68→0.54 ms。定向回归覆盖 Fetch 子查询和已校验 Join 键。64 项定向 Java、81 项真实端到端、Checkstyle 及 152 模块静态检查通过。角色清单为 `77caa193cbc8fafd3b4ce42318359249fd9595d9db0d3f29ec430d44b2e06150`。

实施批次 167 在已编译投影及相邻过滤中证明未使用的生成列时，跳过其发布查询和产物绑定。原来普通投影、普通列过滤、生成列过滤和生成列投影分别都查一次 PostgreSQL 发布记录。候选在真实 SQL 中分别读 0、0、1、1 次，并核对生成向量及空值。Native 接纳直接保留类型化 Bloom 提示和 Paimon 删除向量，Lance Rust FFI 继续使用原始读单元编码。各 1,024 个分片的本地构造对照中，两条内部路径分别去掉 814,932 B/1,024 次解码与 735,101 B/2,048 次解码，元数据值一致。计时组中位数分别为 10.51→1.15 ms、17.17→0.32 ms。4,364 项 Native 全量、14 项定向 Java、87 项真实端到端、Checkstyle 及 152 模块静态检查通过。覆盖 Paimon/Lance 原生读写、删除向量、索引发布与剪裁、真实双 Worker 和 SQL 权限。角色清单为 `1351846992a64e9fd0affc8649598dbabb423ec0ed76e21093268bc7b04cace7`。

实施批次 166 移除生成值扫描选取与绑定之间对同一 Substrait 片段的重复解码。按同一不可变计划的扫描顺序选取，再重写受影响的扫描和父节点。无 overlay 时直接保留原有字节。真实 SQL 对照中的 198、214 和 360 B 三个片段都从解码两次降至一次，生成向量、空值和普通列结果正确。本地 Java 对照分别比较单扫描和 32 扫描片段。绑定结果字节和操作绑定一致，线程分配由 24,032 B 降至 20,224 B、218,480 B 降至 124,576 B，组中位耗时分别为 7.50→6.93 µs 和 76.31→49.87 µs。13 项定向 Java 用例、70 项端到端用例、Checkstyle 及 152 模块静态检查通过。角色清单为 `9eecaec8fa75fb1aa6df815f8f15d9f6a21d60a96c4462ae394b2f933485326b`。

实施批次 165 在一次受保护的 SQL 编译内共用主体的有效角色和授权集合。SELECT、列掩码和行过滤都使用这份角色证据，策略仍从各自的权威存储读取。真实双 Worker 单表查询中，PostgreSQL 角色成员 SELECT 从每次 3 条降至 1 条。调用跟踪确认了原来三条请求的来源，正式回归断言每次只读一次。新的授权编译能看到角色和授权的撤销。启动时校验受保护的规划使用同一个策略提供者。117 项定向 Java 用例、93 项端到端用例、Checkstyle 及 152 模块静态检查通过。角色清单为 `211d67ad006c91a52540dbcfcbdb63be0cb775c2c96849d9f9823d01fc5ec61f`。

实施批次 164 合并一次受控表物理身份校验中重复的目录部署读取。普通物理扫描的形状识别使用编译时已选绑定，真正展开连接器派生表时校验当前所有权。双 Worker 单表 Iceberg 查询的 PostgreSQL 目录定义 SELECT 从 10 次降至 7 次，调用跟踪分别确认两次身份校验重复和一次扫描形状判断。真实回归约束每次执行的目录读取上限，并验证目录版本变化会拒绝旧授权。50 项规划与目录测试、95 项真实端到端用例、Checkstyle 及 152 模块静态检查通过。角色清单为 `812a84e28745f72a3ea5217ce4e89f65208d1555f6f66465ff2e77aba6f657f5`。

实施批次 163 把执行所有权释放中的数据库时间和待恢复写入判断纳入一次 fenced 退役语句。先取得行锁，再以新的 PostgreSQL 语句快照确认当前租约和写入状态。真实 PostgreSQL 各跑五次单输出查询和八个扫描任务的分布式查询。两组独立的写 journal SELECT 都从五次降至零，新的退役语句各执行五次。每次有效释放从四条数据库语句降至两条。用实际并发事务验证行锁等待期间记录的写意图仍可由后继 owner 接管。35 项 PostgreSQL 定向测试、79 模块静态检查和 18 项端到端用例通过。端到端用例覆盖写冲突与回滚、Paimon/Lance 写入、Flight 断连及实际 Worker 重启。发行包清单为 `5d349aa986d94eebc77ce0dcfdef5c025d035fb4e47309ada0f84e9b47fa4d90`。 [PostgreSQL 16 语句快照](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED) · [PostgreSQL 16 写入 CTE](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-MODIFYING)

实施批次 162 让成本合适的全局 DECIMAL SUM 和 AVG 在并行扫描任务生成 Velox 状态，最终阶段按已注册函数的 build、签名和原始类型归并。两套相同数据和文件布局的双 Worker 对照中，800 行扫描的交换从 800 行、30,592 B 降到 16 条状态行、9,816 B，IPC 有效载荷减少 67.91%。同样的查询形态在 80 行时从 7,424 B 增到 9,816 B，成本模型加入每个 Driver 的 IPC 页开销后保留小输入的原始交换路径。精确小数位、过滤空值、相反的 38 位溢出状态归并与实际回收通过。累计页字节写入授权额度字段曾令零发送额度的任务无法确认终态。候选使用 Native 执行统计回报实际页字节。同源 Native 4,363 项、规划和回报 147 项、能力与协议 82 项、72 项真实端到端用例、Checkstyle 及全仓 152 模块静态检查通过。真实用例覆盖双 Worker、TPC-H、TPC-DS 派生、PostgreSQL 读预算、Flight 慢读与断连和写入提交。角色清单为 `80c17e67566478286fe8acc3ae0838276363c28e209a41e75b08f178ec5a5bf2`。 [Trino 的预聚合规划](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughExchange.java) · [Velox 的聚合阶段及运行时成本](https://facebookincubator.github.io/velox/develop/aggregations.html#adaptive-disabling-of-partial-aggregation)

实施批次 161 保留 DECIMAL AVG 的原生聚合。双 Worker 实际 SQL 的基线被 Calcite 改写为 SUM/COUNT，随后因 Native 没有 DECIMAL 与 BIGINT 的除法签名而失败。候选让已注册的 Velox AVG 直接处理 DECIMAL，分别验证 DECIMAL(9,2) 的 39.50 和 DECIMAL(22,7) 的 39.5000000。过滤、空输入、混合 COUNT、双 Worker 实际放置和物理回收通过。45 项规划测试、57 项同源 Java 到 Native 实际用例和 Checkstyle 通过，覆盖 TPC-H、TPC-DS 派生、Flight 慢读及断连和 PostgreSQL 读预算。角色清单为 `5a12e1f57f42c043f4c6dfa495fcbff8bec4eb9a3f3ca103f6b31546e6991665`。

批次 160 的两组同源配对使用同一断连夹具和两个确切产物。夹具的请求额度仅覆盖 schema 和首个记录批，并使用 64 KiB HTTP/2 接收窗口使进程退出点位于实际传输中。四次均逐行通过并返回 FAILED 与物理排空。8,192 行窄结果到 EOF 中位数为 617.5→626.0 ms，75,497,472 B 宽结果为 482.5→412.0 ms。宽结果首批为 116.5→127.0 ms，仍需对首批的读取、转换和消息粒度单独归因。这一组是本机顺序执行的两对样本，宽结果含固定 250 ms 慢读停顿。

实施批次 160 移除 OUTPUT 的内部 IPC 页编解码和 SDK 结果页队列。Velox Driver 按已准入的 schema 生成标准 Arrow Flight 消息，传输层复用消息和缓冲区。字节额度覆盖待发消息和传输仍持有的缓冲区，满额时挂起 Driver，最后一个缓冲区归还后恢复。Velox 完成封存队列，真实 Flight 写完后才提交成功终态。独立客户端进程强制退出返回 FAILED，保留原始流错误和物理排空证据。8,192 行窄结果及 75,497,472 B 宽结果逐行通过，并验证慢读、提前关闭、额外读库预算、双 Worker、嵌套类型和 Iceberg/Paimon/Lance 写入。4,362 项 Native 全量、60 项 Java 到 Native 实际用例和 Checkstyle 通过。同源角色清单为 `56f0fa33635b928e2be29afdbd482ee2887c1a33b01219eb4ff67db00765f77f`。Arrow 18.0.0 九组配对 64 MiB 探针产出相同的最终消息，完整流编码后再解码发送与直接生成消息的中位数为 16.9988 和 3.03163 ms。这是进程内阶段探针，跨主机整链路性能仍按同源负载验收。 [Arrow 18 Flight 传输实现](https://github.com/apache/arrow/blob/apache-arrow-18.0.0/cpp/src/arrow/flight/transport_server.cc)

实施批次 159 修复标量子查询的基数校验。实际 Iceberg SQL 的旧路径把 SINGLE_VALUE 降为 MIN，重复值、值与 NULL、不同值三种多行输入均被错误接受。同版 Coordinator 与 Native 使用携带原字段序号的类型化 Substrait 单行算子，合并两个实际 Worker 的输入后拒绝第二行，零行返回 NULL，单行保留原值或 NULL。双 Worker 验收核对不同 Worker attempt、实际交换和物理释放。4,362 项 Native 全量和 53 项实际 Java 到 Native 回归通过，覆盖双 Worker Shuffle、Iceberg、TPC 派生查询、PostgreSQL 读预算、能力握手和 Checkstyle。同源角色清单为 `b8700a4af99e49bcfb29bc15ba860ca72f177c9e53b11bc333d3ccd9d623652d`。

实施批次 158 移除交换和结果生产端的完整 Arrow IPC staging buffer。Schema、批次和 EOF 先算出精确大小，Velox 批量与迭代序列化直接写输出，超限时目标保持原有位置。JNI 写回调复用该准备过程并分配精确连续区。实际 Velox 夹具的 4 MiB 页在 2 MiB 临时池上限下由内存拒绝变为通过，输入和目标另有池。Arrow 18.0.0 的九组配对 64 MiB UTF-8 探针中，未压缩中位数 14.6849→5.00737 ms，LZ4 36.9995→27.1766 ms，Zstd 72.4042→66.2523 ms，三种编码逐字节一致。329 项定向和 4,362 项 Native 全量测试通过，另有 41 项 Java Native 验收，包含双 Worker Shuffle、大结果背压、实际数据库读预算、Iceberg 原子写、Paimon/Lance、Kafka→Paimon 及租户排空。同源 739 项角色清单为 `71663d14c7097647f129f6422b5ed58feba6c9081d47aa340f328bd7cc858e2f`。规范描述符和实际 Native 函数目录逐字节一致。探针仅归因 IPC 页编码和输出复制。 [Arrow 18.0.0 IPC](https://github.com/apache/arrow/blob/apache-arrow-18.0.0/cpp/src/arrow/ipc/writer.cc)

实施批次 157 移除共享准入返回后立即释放的排队分支。容量满时直接返回拒绝，真正的工作负载排队保留在自身调度器。已有内存估算超限时，基线仍调用共享槽位预留，候选在进入共享槽位存储前拒绝，真实 HTTP 入口当前不附带该估算。可确定的拒绝移除临时所有权，未确认的写入保留原查询的清理义务。447 项 Java 回归和 Checkstyle 通过。与 Arrow 直写一同打包的独立 JVM 通过三项 PostgreSQL 租户用例，验证撤权、45 次饱和拒绝、40 次完整 Flight 读取与物理排空。

批次 156 的后续校验将候选从动态长度占位符改为固定 SQL 和类型化数组绑定。真实 PostgreSQL 的 1,001 条候选分两次执行同一 prepared statement，另准备一次描述符加锁。跨批次共享路径只获得一次删除资格，当前映射的共享路径保留。测试覆盖微秒时间戳、非 ASCII 文件名和数组转义，17 项 PostgreSQL 回归、Checkstyle 和 SpotBugs 通过。批次 156 的原始同源角色清单 `17a3161c37128aa9a01077ec8a460de70f477a01f3c3df7593981daed42b95a8` 已通过六项独立进程回归，覆盖 Kafka→Paimon、远程 Shuffle、真实 CLI 和持久租户配额。 [PostgreSQL 数组展开](https://www.postgresql.org/docs/16/functions-array.html)

实施批次 156 将索引孤儿映射按索引分组回收。装配基线处理 100 条旧映射时重读了完整映射列表 100 次，候选为零。真实 PostgreSQL 同一索引的 100 条映射只准备一次描述符加锁和一条批量删除及路径归属 SQL，保留服务中的共享路径。两个独立数据库连接并发回收两个旧所有者时，只返回一次共享文件删除资格。事务核对原状态、代次、路径及验证时间，保留最近两代。58 项回归和 Checkstyle 通过，覆盖实际物理文件删除、失败 fence、发布、重放与当前有效映射。 [PostgreSQL 写 CTE](https://www.postgresql.org/docs/16/queries-with.html) · [PostgreSQL Read Committed](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED)

实施批次 154 让共享槽位拒绝使用租户配额响应。真实 PostgreSQL 中两个其他查询的所有者占满槽位，本地 Coordinator 尚无在途查询。基线返回 HTTP 503 `COORDINATOR-0002`，修复返回 HTTP 429 `TENANT-0001`，两条原有槽位保持精确，释放后实际 Native Flight 查询取得正确值、EOF 和槽位归零。64 项正式回归与 Checkstyle 通过。同源角色清单 `fde195b594e9fb2a16d14053abbaef429a3082682735f31dc50821faab62ab1e` 的三项独立进程验收同时覆盖撤权、45 次饱和拒绝、40 次结果和物理排空。测试由真实 PostgreSQL 注入其他查询的槽位所有者。

实施批次 155 将 Native Flight 解码的完整 Arrow 校验集中到接收的权威 schema 边界。此前解码、字段校验与显示 schema 绑定均遍历批次。现在在解码中一次核对结构、必填值和嵌套约束，显示字段绑定复用相同类型的数组和缓冲区。实际 Arrow 18.0.0 的 64 MiB UTF-8 校验探针中，三遍与一遍校验的中位数为 10.0925 ms 和 3.16426 ms。323 项定向测试、4,358 项 Native 全量测试及 25 项真实 Java 链路回归和 Checkstyle 通过。实际 Flight 逐行验证 8,192 行的窄结果和 75,497,472 B 的变长结果，并验证慢读、提前关闭、客户端进程退出、EOF 与物理排空。探针耗时仅用于归因校验阶段。 [Arrow 18.0.0 的完整校验接口](https://github.com/apache/arrow/blob/apache-arrow-18.0.0/cpp/src/arrow/record_batch.h)

实施批次 153 移除 Java GROUPING SETS 的预压缩 Project 和重复的字段重映射。在五列输入的真实 Substrait 转换夹具中，分组键、聚合参数和布尔掩码保留原 ordinal 3、1、4。Native 在同一次绑定内计算 GroupId 的列需求，旧夹具的 GroupId 输入由 6 列减至 4 列，聚合透传由 6 列减至 2 列，输出由 9 列减至 5 列。Velox 会急切加载 GroupId 的输入列，因此在该边界保留按需列裁剪。65 项规划回归、619 项 Native 定向测试、4,357 项 Native 全量测试、57 项真实 Java 到 Native 回归和 Checkstyle 通过。实际 SQL 覆盖 CUBE、ROLLUP、GROUPING SETS、TPC 派生查询、双 Worker Shuffle、Flight 结果和 PostgreSQL 读预算。

实施批次 152 让成员解析返回一次权威读取取得的租户和配额，并直接交给 Scheduler 准入。实库常量 SQL 从提交到物理释放的读取为 1 次成员查询加 2 次租户 ID 查询，候选为 1 加 1，保留每个阶段放置对当前资源池的核验。撤销成员关系后，下一次提交执行一次成员查询、返回 HTTP 403，槽位保持为零。没有跨请求权限缓存。105 项准入和所有者测试、追加的 30 项撤权和 HTTP 校验、Checkstyle 及模块事实检查通过。739 项角色清单 `0c0a404df95ddeacac57001f093475166f2409a5dea27718b454f181318ddf6a` 分别通过撤权和以 45 次配额拒绝、40 次完整读取及物理排空为门槛的独立双 Worker 验收。第一次合跑有一次 Worker 健康探针超时，同一产物的负载用例单独复测通过。

实施批次 151 让 Native 本地并行聚合复用首次绑定返回的累加器类型，Partial 构建移除一次同签名注册表绑定。DECIMAL(22,7) SUM 的实际计划验证 Partial 产出 VARBINARY，Final 保留 DECIMAL(22,7) 原始实参并返回 DECIMAL(38,7)。619 项定向测试、4,356 项 Native 全量测试和 54 项真实 Java 到 Native 回归通过，包含 TPC 派生、GROUPING SETS、双 Worker Shuffle 和 PostgreSQL 读预算。Native 函数目录逐字节一致。该结论覆盖单任务内的类型绑定和执行，跨 Stage DECIMAL 状态仍待验收。

实施批次 150 清除 Native 聚合前无作用的恒等投影。实际转换基线在聚合输入处生成一个四列 Project，分组字段被重复追加。候选从原来的三列 TableScan 直接绑定分组，计算表达式和必要的别名仍按需物化。593 项聚合相关测试和 4,355 项 Native 全量测试通过。54 项真实 Java 到 Native 回归包含 TPC-H/TPC-DS 派生夹具、GROUPING SETS、双 Worker Shuffle、能力握手和 PostgreSQL 读预算。739 项角色清单 `cc58258eb73853d64659e0f313ec8f6bf47ab8aa0ba5ddbc0866af70297ca854` 的独立进程验证覆盖双 Worker 重启、远程 Shuffle、Kafka→Paimon 和租户配额恢复。规范 Protobuf 描述符和 Native 函数目录摘要一致。已验证计划结构和结果语义，CPU 和端到端延迟仍按同条件基准归因。

实施批次 149 清除已确认拒绝时的空释放。两条基线探针分别复现 Scheduler 和 QueryManager 对已拒绝准入调用一次释放，修复后均为零。若预留确认异常，仍按原所有者完成物理核对，实际取得的槽位只释放一次。82 项正式测试和 Checkstyle 通过，包含 PostgreSQL 上限和持久所有者、重复拒绝以及已落库但确认失败的回归。

实施批次 148 将 PostgreSQL 槽位成功预留和释放分别合并为一条 SQL，保留事务边界、租户行锁与精确所有者校验。实库记录的预留语句由 5 条降到 1 条，释放由 4 条降到 1 条，计数排除 BEGIN 和 COMMIT。普通准入诊断取消额外计数读。54 项聚焦测试与 Checkstyle 通过，真实 PostgreSQL 验证并发上限、重复预留、错误所有者和损坏计数回滚。独立进程复验中，45 次饱和提交均返回配额 429。预热五种 SQL 后，以单请求在途验证恢复，40 次查询均取得精确结果、Flight EOF 和槽位归零。冷启动另行归因，恢复测试的单请求窗口用于验证物理释放。

实施批次 147 隔离租户配额拒绝和 Coordinator 系统故障。两条实际 Native 输出占满租户槽位时，基线在每秒 20 次到达的 45 次提交中产生 5 次配额 429 和 40 次全局断路器 500。修复后 45 次均为配额 429。消费两个保留结果后，PostgreSQL 槽位归零，随即以每秒 5 次完成 40 次五类 SQL。每次都核对真实 mTLS Flight 结果、EOF 和物理释放，该恢复窗口的完成至排空统计以计划到达时刻计，P95 为 137.218 ms。47 项正式回归和 Checkstyle 通过。回归包含半开探测许可归还及真实系统失败仍可打开断路器。739 项正式角色清单 `2ec0dcafe3a853f41e89a77c6df92562f6ae78fcf0fb6c0bc23a8cd3fcb991e6` 同时通过独立双 Worker 重启、对象存储远端 Shuffle 和 Kafka→Paimon。

实施批次 146 在 Native Flight 领取时一次比较完整的签名授权和已接纳任务，之后批次只读同一个不可变任务身份，并检查取消、撤权和期限。303 B 的本机 C++ 探针每轮重复 100,000 次，五轮比较阶段的中位耗时为 1.393 µs，保留的期限检查为 0.014 µs。这里测量授权微路径，真实流回归另外核对错误控制代次拒绝、合法流完整读取到 EOF、断连和资源归还。4,355 项 Native、22 项 Java 验证及 Checkstyle 通过。

实施批次 145 在索引发布和一致性检查中按索引 ID、代次直接读取 `data_file_path`。真实 PostgreSQL 夹具包含同一索引 1,005 条历史映射和所选代次 4 条映射。该路径用一次 SELECT 返回所需 4 行和 1 列，原路径读回该索引的 1,009 行完整映射。校验覆盖历史代次、其他索引、空代次、发布围栏及实际增量构建。95 项正式回归和 Checkstyle 通过。

实施批次 144 将索引正向提交的祖先判断、目标文件清单及文件增量绑定到同一次连接器观察。Iceberg 复用一次目录表加载，Paimon 使用同一个已绑定表。真实 Iceberg JDBC 目录与 PostgreSQL 的三轮对照均从 3 条目录指针 SELECT 降到 1 条，目标文件与增量逐项一致。祖先重放只判断谱系。72 项正式回归与 Checkstyle 通过，涵盖实际 Iceberg 增量索引发布、Paimon 文件索引、快照与 PostgreSQL 发布围栏。

实施批次 143 让 Native Flight 在 Velox 输出缓冲区实际注册时收到通知。签名票据、授权核对和单读取者领取保持在原有入口，同一个上限内移除了最多 25 次睡眠及重复缓冲区探测。五轮受控真实 gRPC 对照中，缓冲区就绪到客户端收到 schema 的中位数从 12.191 ms 降到 1.399 ms。每轮核对完整结果和 EOF，回归还验证了提前连接和重复领取。本地通过 4,355 项 Native CTest 及 55 项 Java 回归和 Checkstyle，覆盖慢读、取消、断连和 PostgreSQL 读预算。同源独立进程重新验证了 Coordinator、Worker 和 CLI、对象存储远端 Shuffle 及 Kafka→Paimon CDC。739 项清单摘要为 `a22d41230c420554a801a6dbc7a5c92d7cbf274db5639b79b0e36aed992ae767`。

实施批次 142 在 Worker 从 JNI 读取首份终态时校验并保留同一份规范字节和已解析消息。状态、错误详情与发布使用该证据。Worker 重整失败时直接构建类型化消息，在发布边界编码一次。Corretto 17 受控分配对照中，首份回执的 1 算子和 128 算子样本分别从 3,896 B 降到 1,928 B、从 121,384 B 降到 59,000 B；失败重整分别从 10,502 B 降到 1,592 B、从 198,952 B 降到 12,034 B。四份规范载荷摘要逐一一致。177 项正式回归与 Checkstyle 通过，覆盖实际 Native、HTTP ACK 重发、Flight、双 Worker、读库预算、COPY 和 CDC。

实施批次 141 在索引增量维护中先判断当前发布代次和快照祖先关系，再枚举待发布快照的文件。受控重放包含一个已经发布的快照与它的一个祖先，完整文件清单调用从两次降为零，两个 durable 订阅通知均已执行。真实 Iceberg SQL 与 PostgreSQL 验证追加后创建了更高的 ACTIVE 代次，发布绑定追加后的 main 快照并增加了对象数。56 项回归及 Checkstyle 通过，覆盖快照目录、旧事件、索引授权和发布围栏。 同源独立进程验收重新打包了 739 项产物，Coordinator、Worker 和 CLI、对象存储远端 Shuffle、Kafka→Paimon CDC 三项通过。

实施批次 140 修正相关子查询晚暴露的外层谓词。复用 Calcite 内连接条件下推，只在可证明两个新连接都取得条件时重排笛卡尔左子树。TPC-H Q2 确定性夹具的最大 Native 算子输出从 100 亿行降到 13,166 行，OUTPUT 执行从 29,443 ms 降到 18 ms，查询审计从 30,628 ms 降到 424 ms。相同 SQL 与 PostgreSQL 返回相同的空结果，匹配夹具数据的参数变体验证了与 PostgreSQL 顺序和值一致的 100 行。五份 TPC-H 计划移除了无谓笛卡尔积，其余受检 TPC-H 与 TPC-DS 计划一致。128 项回归及 Checkstyle 通过，另有一项生成 golden 的可选用例跳过。 [Calcite 内连接谓词下推](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/rules/FilterJoinRule.FilterIntoJoinRule.html) · [Trino 的谓词下推和连接排序](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/PlanOptimizers.java)

实施批次 139 在实时执行图记录构建侧与 Probe 的一次动态过滤决定及等待区间。结果区分已应用、空构建、缺失摘要、超时和查询终止，作用于同一代 Probe 枚举。实际双 Worker 的稀疏键样本中，单构建任务等待 59.427 ms，探测从 40 个分片、28,448 B 降至 4 个分片、2,795 B。两个构建任务等待 42.003 ms，探测从 40 个分片、29,181 B 降至 4 个分片、3,186 B。真实 Flight 查询核对完整结果和实际资源归还。39 项 Java 回归与 Checkstyle 通过，含 PostgreSQL 读预算和 22 个 TPC-H 查询的确定性参考夹具。持久 SDK 的本地对照另通过 150 次串行和每秒 8 次到达的 80 次混合样本。样本未达到跨引擎、过载或 P99 门槛。

实施批次 138 让 Native 与 JVM 执行器的最终释放回执在 Worker 内以同一份不可变消息和原始字节保存，物理退出状态核对和同一终态发布直接复用。退役后重试从已取得的回执恢复，错误 attempt 无法清除句柄。Corretto 17 的回执校验与投影探针中，115 B 样本的线程分配由 5,240 B 降到 1,720 B，6,458 B 的 128 算子样本由 192,568 B 降到 61,792 B，两份规范字节摘要一致。该探针测量分配，单算子样本未观察到耗时改善。77 项正式 Java 回归、Checkstyle 和模块边界通过，覆盖实际 Native Flight 释放、HTTP 重报、双 Worker Shuffle、PostgreSQL 读预算、COPY 和 JVM CDC。

实施批次 137 在规范 Native 回报和 JSON Profile 中保留算子总阻塞及生产者、消费者、split、连接器四类等待。计量按 Velox driver 累加，并行等待可以重叠。受控真实 Flight 测试在暂停生产者时录得 85.477 ms 的生产者累计等待，消费者和连接器等待为零，三行结果完整。实际输出背压测试在取消并物理释放后保留了消费者等待。对应 JNI 与两份能力夹具同源重采，4,354 项 Native 和 107 项 Java 验证通过，另通过描述符、Checkstyle 和模块边界检查。 [Trino 的算子等待归因](https://trino.io/docs/current/sql/explain-analyze.html)

实施批次 136 让索引构建和目录文件枚举在一次连接器观察中选择当前快照并读取其文件集合。Iceberg 复用一个已打开的目录表，Paimon 保留同一已绑定表。真实 PostgreSQL 的三次对照均从两条目录指针 SELECT 降到一条，空表保持一次。实际提交新快照后，当前文件集合包含新文件，显式历史快照仍只返回原文件。43 项回归和 Checkstyle、模块边界检查通过，涵盖 Iceberg 与 Paimon SQL、索引创建、增量构建和快照固定。

实施批次 135 让 Worker 的进度回报与独立补充统计直接使用规范 Protobuf，移除无生产 HTTP 入口的状态响应 DTO、算子 DTO 和 Native 统计投影。补充字段校验禁止携带构建键证据，重发仍核对相同状态、序号和内容。Corretto 17 用相同生产编解码器对照五组各 512 次。94 B 的进度回报往返分配由 2,848 B 降到 2,064 B，837 B 的 16 算子样本由 15,992 B 降到 9,504 B，两份载荷逐字节一致。162 项验证和 Checkstyle 通过，覆盖 HTTP 重发、双 Worker、COPY、PostgreSQL 读预算、Flight 断连及物理释放。 同源 macOS 独立进程验收使用 739 项内容清单，Coordinator、Worker 和 CLI 及对象存储远端 Shuffle 两项通过。

实施批次 134 让 Coordinator 的任务回报入口保留一份不可变的 Native Protobuf 统计，由读取 JSON Profile 时生成公开模型。移除接收时的诊断副本和重试比对中的重复投影，继续核对精确回报摘要、任务归属、递增序号与实际释放。Corretto 17 按五组各 512 次测量 HTTP 解码的线程分配。121 B 单算子回报由 2,648 B 降到 2,096 B，6,084 B 的 128 算子回报由 65,912 B 降到 51,632 B，回报字节摘要一致。176 项验证通过，覆盖真实 Worker 握手、HTTP 重发、双 Worker、PostgreSQL 读预算、慢读断连、生成列与 COPY 提交。Checkstyle 和模块边界检查通过。

实施批次 133 移除 Native 任务启动和清理中的全局 Profiler 注册表。仓库的生产入口只有任务插入和移除，SVG 端点使用显式的不可用实现，JSON Profile 继续读取规范任务回报。提取原生产注册函数的本地并发探针对插入和移除成对计时，单线程中位数 260 ns，四线程约 1,044 ns。删除了两次全局互斥锁获取和每任务注册项。4,353 项 Native 用例通过，覆盖实际结果、Flight Exchange、取消与资源清理。

实施批次 132 在 Native 单个构建任务内合并批次键区间，最多发布 16 个互不相连的 BIGINT 区间。Coordinator 继续要求获胜构建任务齐备和交换已发布。任一层合并超限时使用覆盖全部键的外包区间。真实双 Worker 冷探测的单任务场景从 35 个分片、27,885 B 降到 4 个分片、2,795 B，重复键和 NULL 的连接结果及实际资源归还通过。50,000 键的本地区间收集对照额外耗时低于 0.13 ms。规范 Protobuf 描述符、模块边界、Checkstyle、118 项 Java 定向用例、18 项实际路径回归和 4,353 项 Native 用例通过。 [Trino 动态过滤收集上限](https://trino.io/docs/current/admin/dynamic-filtering.html#dynamic-filter-collection-thresholds)

实施批次 131 让内部控制面客户端与实际 Jetty HTTP/1.1 监听器一致。真实 Jetty 探针在每组 8 条持久连接发送 80 次 Protobuf POST，Java 17 默认在每次请求附加 117 B 的 h2c 升级头，共 9,360 B。候选默认请求的升级头为零，连接数、消息体和响应协议一致。证据覆盖请求头开销。94 项回归和 Checkstyle、模块边界检查通过，含 Worker 回报、注册启动、真实双 Worker SQL、并发重试和 COPY 私有 mTLS。分片读量回归先测剪裁，再执行全量读取，避免全量扫描提前预热同一组文件。 [Java 17 HttpClient](https://docs.oracle.com/en/java/javase/17/docs/api/java.net.http/java/net/http/HttpClient.html#version())

实施批次 130 在持久分区认领事务里用一次受共享锁保护的清单读取确定当前发布状态，再在同一连接读取对象和校验源分片。移除对象读取与分片检查各自重复执行的状态查询。真实 PostgreSQL 的基线记录两条独立状态 SELECT，候选为零并保留一条受锁清单 SELECT。夹具外部持有源发布的独占行锁时，认领确实等待，释放后通过精确源对象、校验和及单连接检查。102 项 Java 回归和 Checkstyle 通过，覆盖持久交换、发布授权、动态表物化和生成值 SQL。

实施批次 129 将实时 Stage 的 Substrait 序列化、split 枚举、校验与绑定、任务提交和提交后等待整组 ACK 分开计时。每段保留 Stage ID、代次和同一个 Coordinator 的单调时钟，枚举及派发保留实际数量。真实双 Worker SQL 的一个样本中，8 个 split 的 Stage 枚举 115.75 ms、序列化 92.86 ms、绑定 5.72 ms、提交 10.88 ms、提交后 8 个 ACK 等待 236.10 ms。另一 Stage 枚举 16 个 split 用 35.41 ms，实际签发 1 个任务。此处是带回执的本机单次样本，各阶段可能并行。57 项 Java 回归和 Checkstyle 通过，随后单独复测 SQL、Flight 完整结果和物理释放。

实施批次 128 将已发布产物的读取守卫改为只读状态列和共享行锁。PostgreSQL 16 基线在 `pg_blocking_pids` 中可见一个产物读取事务被另一个读者阻塞。候选在持有另一个共享锁时完成读取，遇到独占锁仍等待，释放后读取精确对象。每个派发窗口的相关 SELECT 从 6 降到 5，清单层仍验证依赖校验和。96 项测试覆盖真实 PostgreSQL 的发布状态、授权和持久交换，以及真实 SQL 生成值读写。Checkstyle 通过。 [PostgreSQL 16 行锁](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)

实施批次 127 将实时任务产物物化收敛到最多 16 个任务的派发窗口。同一发布版本每窗口只加载一次清单及已分配源对象的产物。每个任务单独验证精确来源、对象角色和完成覆盖，再构造并校验摘要。BM25 的全局统计在多个任务共享，各自的倒排和行定位限定在自己的源对象。真实 PostgreSQL 对两个任务、每任务两个绑定节点连续测量三次，相关 SELECT 从 24 降到 6。57 项检查涵盖真实 PostgreSQL、DiskANN SQL、双 Worker、生成值写入与读取及 BM25 任务归属，Checkstyle 和模块边界检查通过。

实施批次 126 让同一 Stage 中共享表、生成列、选定快照与 split 集合的读取复用一次发布查询，包含未发布的查找结果。每个扫描仍单独校验 schema 与定义并分配精确 Node ID。PostgreSQL 16 的 `pg_stat_statements` 按实际表查询计数，8 个重复扫描每次执行从 8 次 SELECT 降到 1 次，三次执行均一致。真实发布测试验证两个 Node ID 共享同一不可变产物和资源证据。25 项回归覆盖 PostgreSQL 发布、快照校验、任务物化和异步生成值 SQL 写入与读取，Checkstyle 通过。

实施批次 125 将下游读准备的提交、执行线程开始、结束和拒绝记录在实际拥有该队列项的 Coordinator 上。每次 executor 提交对应一条 `downstreamPreparations`，使用与 Stage 转换相同的单调时钟。完整区分排队、运行、完成、取消后出队跳过、失败及拒绝，移除把合并唤醒归给第一次回调的日志。受控单线程夹具验证等待与拒绝，100 次合并通知仍只创建一条记录。真实双 Worker SQL 在 Flight 读完和物理释放后核对 Profile。34 项定向 Java、真实 SQL、PostgreSQL 预算测试、Checkstyle 与模块边界检查通过。

实施批次 124 让生成值读取枚举借用 Protobuf 的原始关系节点。没有生成值覆盖的分支只遍历，只有实际替换的叶子及其祖先重建，保留同一个物理表重复读取、集合输入顺序和原来的过滤条件。使用生成的 Substrait 类型构建 1 和 24 个扫描节点的计划，分别测量五组各 256 次，线程分配由 3,208 B 降到 2,584 B、由 67,632 B 降到 53,896 B，扫描位置、表标识和 schema 相同。30 项回归包含真实 PostgreSQL、异步生成值 SQL 写入与读取、双 Worker Shuffle，Checkstyle 通过。

实施批次 123 在 Stage 没有检索和 AI 绑定提示时直接交接已绑定的不可变计划。仍先绑定生成值读取，OUTPUT 继续验证声明的结果逻辑类型。Corretto 17 通过实际 SQL 编译得到 97 B 和 271,836 B 的 Substrait，测量 Stage 绑定至调度方读取字节，预热两组，五组各 128 次。每次线程分配分别从 1,904 B 降到 640 B、从 2,447,528 B 降到 815,848 B，两份计划的 SHA-256 逐组相同。此处计量计划交接，完整查询延迟使用单独基准。31 项生成值读取、结果类型、向量检索、双 Worker Shuffle 及 Native Flight 回归通过，Checkstyle 通过。

实施批次 122 将低基数局部归约扩到多个直接分组列。每列沿用同一选定快照的列统计，规划器按各列基数估算组合数并计入 NULL，同时校验预计交换行数和字节都下降。真实双 Worker 的 `COUNT/MIN/MAX` 查询读取 192 行，原来 192 行进入 Shuffle，候选启用后 16 行进入 Shuffle。未发布列统计时仍交换 192 行。四个结果组逐组核对，涵盖 NULL、区间边界和物理释放。54 项 Planner、真实 PostgreSQL 读绑定、JNI 命令及双 Worker Java 用例和 Checkstyle 通过。**机制状态：**多列低基数局部归约已实现。**当前验收状态：**本地双 Worker 机制与结果已验证，真实 CPU/首批/完成时延和更广聚合形态仍属 B4。**后续 WP：**WP2 归因，性能复测见 Q3。

实施批次 121 将已解析的初始 `RuntimeFilterBinding` 和运行时 JNI 命令的类型化过滤器直接送入同一 Velox 转换入口。以实际生成的 Protobuf 类型测量，262,160 字节 Bloom 的内部消息为 262,194 字节。旧路径的晚到命令序列化与解析每次约 8.86–9.20 µs，初始绑定还复制一次过滤器，每次约 14.81–15.10 µs。两条路径均移除内部往返。测量仅覆盖本机协议交接。48 项定向及 4,350 项完整 Native CTest 通过，14 项真实 SQL Java 端到端和 Checkstyle 通过。

实施批次 120 修复 Native Flight 客户端断连后 OUTPUT 未退出的问题。独立客户端消费 72 MiB 变长结果的首行后被强制终止，期间未调用 Coordinator 取消接口。基线在 15 秒内未释放；探针确认 gRPC 已销毁 reader 且清空队列，实际 Velox 生产者仍存活。修复复用已绑定的失败回执，终止对应的 SDK 任务，并保留真实读取者、缓冲区与生产者的释放条件。本地对照在 34 ms 内观测到 `FAILED` 和物理释放。完整读取与显式取消用例保持正确。独立复测的物理释放为 27 ms。31 项 Java/JNI、Checkstyle、模块边界检查与 4,349 项完整 Native CTest 通过。显式取消回归核对原生终态和相同尝试的真实释放回执。同一 macOS ARM64 正式角色包通过 Native 双 Worker 最终结果和重启，以及 mTLS Remote Shuffle、Flight 和实物清理。739 项清单摘要为 `2fb6a8b853eee3e74623ab12b74382b122deb03c55aa6d2ab4b0d70a1da0f046`。

实施批次 119 保留各获胜 Worker 的构建键区间，合并重叠和相邻区间，最多向 Iceberg 传入 16 段。超过上限使用覆盖所有键的外包区间。真实双 Worker 回归锁定同一份 Iceberg 夹具和等值 Join，两个构建任务分别报告 `[2,3]` 与 `[35,36]`。原来合并成单段时，probe 完成 35 个分片并读取 24,460 字节。修复后为 4 个分片与 2,795 字节。对照用不触发跨 Stage 过滤的等价表达式，均为 40 个分片与 28,883 字节。完整结果包含重复键和 NULL 输入，逐行对照并等待所有任务物理释放。46 项 Java 与 Checkstyle 通过，其中 11 项为真实 SQL 端到端用例。模块边界检查通过。

实施批次 118 将持久化阶段的角色检查和任务准备接到同一份事务内授权证据。任务准备继承已经验证的直接角色，并保留租户、表策略、模型权限和当前执行租约的检查。真实 PostgreSQL 的 20 分区调度夹具执行 3 个检查窗口，`pg_stat_statements` 中匹配直接角色查询的调用从 6 次降到 3 次。 107 项 Java、Checkstyle 与模块边界检查通过，涵盖撤权、取消、租约期限、持久 Shuffle 和目录导出。

实施批次 117 用 8,192 行 Iceberg 数据验证 Native Flight 窄结果和显式 `VARCHAR` 变长结果。两次读取均逐行核对。窄结果的有效文本为 31,658 字节，首行、EOF、物理释放分别为 170、186、189 ms。宽结果有效文本为 75,497,472 字节，客户端在首行后暂停 250 ms，三个观测值为 127、502、514 ms。另一次在首行后发出取消并提前关闭，实际收到 `CANCEL_ACCEPTED` 和 `CANCELLED`，Native 回执与 Coordinator cleanup 完成。数值是本地应用层样本。

同一持久客户端完成 150 次串行和 80 次每秒 8 次到达的混合读。混合首行 P95 依次为常量 23.62 ms、点查 98.18 ms、选择性扫描 58.84 ms、计数 58.19 ms、分组 69.20 ms，各 16 个样本。230 次都核对完整结果与物理释放，每次最多使用两次状态请求。正式 macOS ARM64 角色目录的 739 项清单摘要为 `caea25d7c508332d1ad6afb0b92b575f3b85983b85c37ba6d358559659be9140`。同包通过独立进程 Native 双 Worker 重启、mTLS REMOTE Shuffle、Flight 与对象和槽位实际释放。6 项真实端到端回归与 Checkstyle 通过。**状态更新：**本地的无取消指令 abrupt Flight disconnect 已由后续批次 120 覆盖；跨主机出口和长时间驻留仍属 B6，不能据此结项。

实施批次 116 在 Profile 中记录被当前 Stage 代次接纳的物理尝试及对应逻辑任务。记录点在实际终态槽位接纳之后。已取代的任务和未被接纳的报告不产生记录。重试夹具验证了逻辑任务与获胜副本 ID 不同的情形。真实双 Worker SQL 将实际接纳记录与 Native task attempt 坐标对应。115 项 Java 和 Checkstyle 通过，涵盖投机、重试、持久赢家、写提交竞争、HTTP 和释放。后续继续区分 Exchange 阻塞、队列与物理释放时间。

实施批次 115 让状态读取、状态等待和取消共用本机查询的原始提交主体。用户显式读取历史状态时，单条 journal SQL 返回最新状态与原始受理证明。恢复时从已有 journal 批次恢复主体。74 项 Java 和 Checkstyle 通过。真实双 Coordinator PostgreSQL 对照核验本机状态等待及非 owner 的定向等待均为零读取，显式历史请求每次只借用一次连接。真实 HTTP、Native Flight 和 PostgreSQL 验证其他主体的状态与取消请求直接拒绝，未执行 journal SELECT，原提交者仍可完整消费并确认物理释放。该非 owner 等待结论属于历史实现，当前以 A2 的签名票据共享入口用例及对应 SQL 计数为验收证据。

实施批次 114 将 REST 和 Web UI 查询 Profile 读取绑定到原始提交主体。已知的本机查询先核对主体，历史 Profile 在单条 SQL 内按查询 ID 与主体检索。真实 HTTP、PostgreSQL 和 Native Flight 用例核验读完、释放和 Profile 内容，确认提交者读一次审计库，其他主体访问本机已知查询的两条 Profile 路由均为零 SELECT。57 项 Java、Checkstyle 和模块边界检查通过，覆盖实际双 Worker SQL。

实施批次 113 让按租户和任务加载持久执行规格与随后的权限判断共用已获取的事务级发布锁。直接接收规格的调用仍在入口取得锁，权限、最新租户与模型校验继续执行。真实 PostgreSQL 的三窗口分区检查中，授权发布锁 SQL 从 9 次降到 6 次。33 项 Java 和 Checkstyle 通过，覆盖撤权、取消、终态上报和真实 SQL 编译的受保护 DAG。 [PostgreSQL 事务级 advisory locks](https://www.postgresql.org/docs/16/explicit-locking.html#ADVISORY-LOCKS)

实施批次 112 将持久 Stage 的分区检查改为最多 8 个连续分区一笔事务。真实 PostgreSQL 的 20 分区对照中，页索引 SELECT、已持锁的交换屏障 SELECT 和执行路由 SELECT 各由 20 次降到 3 次。这三类语句合计从 60 条降到 9 条。按索引顺序验证整个窗口的精确页坐标和 winner，缺失分区拒绝派发。真正申领仍验证当前 owner、权限和页状态。101 项 Java、Checkstyle 和缺页回归通过，覆盖实际 SQL 生成的持久图和原始 Native winner 重放。

实施批次 111 将状态等待的四个事件统一归属到共享值类型。修复查询生命周期依赖客户端 DTO 包的架构检查。HTTP 参数、版本比较、排队和物理 release 合同沿用现有行为。37 项 Java、Checkstyle 和 114 项 TPC 工具自测通过，另用 Corretto 17 编译了实际 TPC Java 适配器。

实施批次 110 在已认证 JSON Profile 公开实际派发的执行 DAG。阶段计划宽度、Exchange 分布和读取模式来自 Coordinator 注册的同一张图。阶段观测按单进程单调时钟记录，并包含重试代次。使用原本的有界遥测存储，无新增查询路径 SQL。真实 HTTP、PostgreSQL 和 Native Flight 验证了单 Stage 的转换与读查询的 SQL 预算。双 Worker SQL 验证了多 Stage、Exchange 端点、结果和实际释放。25 项 Java 与 Checkstyle 通过。获胜 attempt 和 Exchange 阻塞的关键路径证据仍列在性能对齐计划。

实施批次 109 用单条 PostgreSQL 查询读取持久执行的交换图索引、登记、终态证据和源快照。合并前后的真实单交换终态测试确认这四类 SQL 从 4 条降到 1 条。九条交换边的夹具验证了跨多个游标窗口的完整性和同租户不同任务的隔离。每次游标取 4 行，查询仍持有各证据行的共享锁。生产者赢家的权威检查继续单独执行。86 项 Java 和 Checkstyle 回归通过。 [PostgreSQL 行锁](https://www.postgresql.org/docs/16/sql-select.html#SQL-FOR-UPDATE-SHARE) · [pgJDBC 游标](https://jdbc.postgresql.org/documentation/query/#getting-results-based-on-a-cursor)

实施批次 108 在持久执行恢复与生产者终态事务中复用同一次持锁交换屏障和登记证据。整张图的摘要按事务复用。真实 PostgreSQL 的单交换终态上报中，图索引 SELECT 从 3 次降为 1 次，登记和终态证据各从 2 次降为 1 次，源快照确认保持 1 次。这四类读取合计从 8 次降为 4 次。85 项定向 Java 和 Checkstyle 通过，覆盖持久 DAG 恢复、过期 owner、缺失与损坏交换证据、取消和事务回滚。

实施批次 107 根据 Coordinator 已签发并与上报精确匹配的任务种类路由持久 Shuffle 生产者。真实 PostgreSQL 的运行上报路由 SELECT 从 1 次降为 0 次，终态从 4 次降为 3 次。终态仍在当前租户和 owner fence 下持久提交原始报告字节，失去所有权的进程无法推进赢家。56 项定向 Java、Checkstyle 及真实 SQL、Join、物化刷新回归通过。

实施批次 106 删除只在常量默认值、模板和文档出现的三项 Worker Shuffle 配置。实际队列按任务 grant 的字节限额运行，Native Flight 保留独立的保留、传输与 spill 预算。157 项配置和服务启动测试、Checkstyle、Helm lint 与渲染通过。重打包后的 NATIVE 双 Worker 查询与重启、REMOTE 双 Worker SQL 和物理释放均通过。

实施批次 105 按固定宽度列的实际选中行估计 Native Shuffle 合并成本，按实际 Arrow 编码检查页上限。本地受控用例从八个大源向量选出 128 行。编码由 8 页、3,264 字节缩为 1 页、1,304 字节，解码后的行顺序完全相同。变量宽度仍采用保守源大小上界。新 C++ Release 的 4,348 项完整回归通过，macOS ARM64 JNI 和正式角色打包通过，同包双 Worker REMOTE SQL 与 Flight、MinIO、PostgreSQL 分配和租户槽位的实际释放均验证通过。

实施批次 104 用真实 PostgreSQL 和 MinIO 对照四个已签发的签名生产者。每轮写入 512 KiB，由每路 1 批或 32 批组成。去除两轮预热后，每种分配和分批组合各 8 轮，每轮都创建 4 个预约窗口并执行 32 条相关 SQL。共享 allocation 的写入 P50 为 10.80 和 88.44 ms，独立 scope 为 9.49 和 81.42 ms。每种组合有 32 个写入者样本，共享 allocation 的单路 P95 为 11.54 和 101.31 ms。所有窗口随后按精确清单发布，字节与行计数相符，cleanup 释放相同数据量。这一窗口覆盖服务接收已编码批次并写入真实对象存储。下一步验证 Native 页合并对实际对象数、批次延迟和内存的影响。

实施批次 103 将精确任务的远端 Shuffle 能力签发和续期合并到现有 fenced 事务。生产者的分区与代次、消费者的查询身份和当前 owner 均在持锁 allocation 上查验。真实 PostgreSQL 中，Coordinator 写能力签发和 Worker 读能力续期的 Shuffle 与执行锚点语句均从 7 条减至 5 条，allocation 与 owner 各读取一次。数据库时钟检查单独计量。续票令牌与任务身份不符时，服务在访问数据库前拒绝。69 项 Shuffle 服务与 Checkstyle 回归通过。真实双 Worker SQL 继续通过完整 Flight 结果与对象、分配和槽位释放。

实施批次 102 将远端 Shuffle 的注册、提交回执和 cleanup 能力归属到单个查询。完成或移交后按查询直接移除状态，其他活跃查询的注册和发布回执继续可用。本地进程内微测预装 20,000 个查询，每个四个 Stage，分别运行两组，每组 3,000 次回收。回收 P50 从 1.56–1.67 ms 降到 0.1 µs 以内。计时粒度约为 42 ns，测量范围是进程内状态释放。36 项定向测试、Checkstyle 和正式包双 Worker REMOTE SQL 的 MinIO 与租户释放回归通过。

实施批次 101 复用 Iceberg JDBC SDK 新建表对象时读取的权威元数据。现有非空发布快照复用同一次元数据确认，要求发布回执的空 INSERT 仍提交并查验精确标记。真实 PostgreSQL `pg_stat_statements` 对照中，已绑定写入的精确 Iceberg 目录指针 SELECT 由 8 次减为 7 次，要求发布回执时由 11 次减为 8 次。50 项定向 Java 和 Checkstyle 通过，包含普通 SQL、COPY、空快照、精确重复提交、Dynamic Table 和 Materialized View。

最新本地 Release 以同一 C++ JNI 库打包 Maven classifier 和 Worker JAR。独立 Coordinator、两个 Worker 和 CLI 通过 mTLS 远端 Shuffle 执行单输入聚合和双输入 FULL OUTER JOIN。两侧各四个 Iceberg 文件，Join 从四个原始分区合为一个消费任务，Flight 逐行返回全部八行，覆盖重复键、NULL 与两侧未匹配行。两个 Worker 都有 Native 释放回执，租户槽位和 MinIO Shuffle 对象均已归还。另通过 56 项 Planner 测试以及 20 项双 Worker 回归。跨主机吞吐、尾时延、偏斜和持久重放仍按 A10/B5 验收。

实施批次 100 用实际打包的 Coordinator、两个 Worker、CLI 和 C++ Release JNI 验证了单输入 AQE。公开 SQL 从四个分开提交的 Iceberg 文件读出 20 行，四个哈希分区经 mTLS 远端 Shuffle 合并为一个消费任务。Native Flight 返回有序的四组计数 4、8、4、4。两个 Worker 回报精确 attempt 的 Native 释放，已归还租户槽位与 PostgreSQL Shuffle allocation，MinIO 桶内的实际对象已清空。现有 NATIVE 进程回归还验证了双 Worker 查询和优雅重启。34 项定向 Java 和 Checkstyle、9 项打包工具测试通过。**机制状态：**live 单输入范围合并和真实 4→1 读取映射已实现。**当前验收状态：**本地打包双 Worker 正确性与资源回收已验证；生产阈值、混合负载尾时延、多输入、durable replay 和 skew 尚未完成。**后续 WP：**B5/A10，WP5。

实施批次 99 让普通 INSERT 直接进入现有 fenced commit。真实 HTTP、Iceberg 和 PostgreSQL 对照中，物化任务查询从 1 次降到 0 次，执行所有权 SELECT 从 4 次降到 3 次。客户端携带内部物化属性的用例仍走普通授权，写入文件在实际 drain 后可读且只有一行。COPY 持久化回执后的检查与物化发布的权威查验继续生效。连接器提供的发布回执以 journal 写入确认作为持久化证据。59 项 Java 和 Checkstyle 回归通过，含真实 PostgreSQL、COPY SQL、Dynamic Table 的真实空快照及 Materialized View 的提交与刷新。

实施批次 98 复用 Calcite 生成的元数据处理器。无状态的 handler chain 由同一进程共享，统计估计器、成本参数和授权仍由各查询的规划上下文持有。两个独立范围对相同 Join 分别给出 0.001 和 0.05 的选择率，共享处理器身份。同一机器和同一持久 SDK 夹具开启 JFR，前后各 230 个完整结果及物理释放全部核验。正式窗口的规划线程采样中，Janino 元数据编译从 8/50 降到 0/28。常量、点查、选择性扫描、计数、分组的串行首行 P50 分别由 31.62、79.46、66.48、54.30、79.41 ms 降至 9.09、43.36、32.48、30.36、38.76 ms。这组延迟来自带 JFR 的单 Worker 对照。51 项定向 Java 及 Checkstyle 通过，覆盖 CBO、血缘、真实 PostgreSQL 权限和 Native 动态过滤。

实施批次 97 保留每个已签发 task attempt 的最新 Native 算子计数。组装按同一 attempt 的上报序号保序，避免较晚处理的旧报告覆盖。REST JSON 分别给出 Stage、分区、计划代次、任务、Worker、执行状态和实际背压。旧的跨 Stage 同名归并、合成树和用 wall 减 CPU 推算 I/O 的字段已移除。真实 HTTP 与双 Worker SQL 回归验证了公开数据、不同 Stage 和两端 Worker 的归属。25 项 Java 回归、Checkstyle 和 114 项 TPC 工具自测通过。执行 DAG、获胜关系及阻塞原因的关键路径证据继续保留在待办。

实施批次 96 将 HTTP 幂等回执回收改为按到期顺序处理，每次提交最多回收 16 条。并发重试、录入后的错误和同键新回执保持原有身份和期限。真实注册表的反向顺序微测预装 20,000 条有效回执，每组 5,000 次调用。新提交 P50 从 100.42–101.46 µs 降到 1.46 µs，重放从 88.21–89.54 µs 降到 1.50–1.58 µs。预装 1,000 条时两者仍在同一量级。17 项定向回归和 Checkstyle 通过，其中实际 HTTP 用例验证响应重放与 SQL 写入只产生一次副作用。

协议切换和批次 94–95 后，同一单 Worker 持久 SDK 夹具核验了 230 个完整结果及实际释放，状态交互中位数每次查询为 2 次。常量、点查、选择性扫描、计数、分组的串行首行 P50 分别为 31.88、82.08、67.96、54.98、81.15 ms。每秒 8 次到达的混合样本中，五类查询各有 16 次，首行 P95 为 52.49、103.16、80.59、134.35、92.83 ms。混合计数的 P95 高于批次 87。该样本用于跟踪本地链路及尾部波动。

实施批次 94–95 让已证实为只读的实时 DAG 跳过写提交日志。真实 HTTP 与 Native Flight 读取到 EOF 并实际释放后，PostgreSQL 的写提交日志 SELECT 由 1 次降为 0 次。终态仍校验当前执行身份和租约，并将 PostgreSQL 时钟与执行身份检查合并到同一条 SELECT。写入及待恢复的执行沿用提交日志与权威 fence。48 项定向回归和 Checkstyle 通过，覆盖真实 PostgreSQL 租约过期和接管、COPY SQL、写提交幂等与恢复。

执行协议原子切换通过独立发布候选验证。使用同一源码重新构建 C++20 Release Native，加载实际 JNI 库，运行全部 4,347 项 CTest，包括真实 PostgreSQL 的 Lance 夹具。92 项定向 Java 回归通过，覆盖 SQL 鉴权、快照与统计、双 Worker、HTTP 状态交付、端到端 Native Flight、COPY 来源、Native 读配额和 Connector 数据读取。持久 Shuffle 另通过 22 项真实 PostgreSQL 与 MinIO 回归。共享 Native 夹具冲突的用例使用独立 Maven 调用复验。锁定的 Protobuf descriptor、模块边界和定向 Checkstyle 检查通过。

实施批次 93 统一取消响应和终态上报中的物理释放回执。Worker 在 Native 已给出匹配的取消终态时沿用同一份回执，包括没有错误字段的正常取消。Coordinator 继续对已受理的释放身份、状态及错误分类作精确校验。真实故障回归丢弃前两次取消上报，第三次获 ACK，三个二进制请求逐字节一致。

实施批次 91–92 将已准入查询的目录和 schema 名称绑定到同一查询权限范围。受控的 Calcite 测试连续查找八次，只触发一次目录列表和一次 schema 列表。下一条查询能看到新 schema，真实 PostgreSQL 和 Lance 的管理会话仍即时看到各租户的创建操作。Calcite 的直接查表和显式列名共用同一元数据入口。夹具有 1,000 张无关表，直接查表记录零次枚举和一次命名句柄读取，显式列出表名新增一次枚举和零次句柄读取。36 项定向 Java 回归和 Checkstyle 通过。

实施批次 90 将大小写别名和系统表使用的名称解析统一到已认证的目录版本。首次缺失仅枚举一次。过期前的缓存缺失仍刷新一次，以发现其他 Coordinator 创建的表。受控测试确认首次和后续缺失各执行一次枚举，外部新表在下一次查找可见，目录失败继续传播。38 项定向 Java 与 15 项真实 Native 读取回归通过。 独立发布分支另通过 81 项查询绑定、统计、Calcite 与 PostgreSQL 联合测试及 Checkstyle。

实施批次 89 将 PostgreSQL 支持的 Iceberg JDBC 目录精确表名查找直接交给已认证的编译读绑定。真实 PostgreSQL 的 `pg_stat_statements` 记录一次 Iceberg 元数据指针查询和零次该命名空间内的表枚举。有 1,000 张无关表的查找用例确认精确命中不枚举也不加载它们。大小写别名仍取得规范名称。真实 Paimon 混合大小写历史快照测试保持原表名和绑定。同批复核了 37 项定向 Java 测试和 15 项真实 Native Paimon/Lance 读取测试。

实施批次 88 让 Worker 心跳、健康状态和缓存摘要复用已核验的 Native 能力清单。入网和恢复的新清单仍通过完整校验，精确相同的内容继续共享，滚动部署的不同清单保持独立。实际清单的 Worker 快照对照中，两种路径生成同值的健康快照。组中位耗时由 22.69–24.26 ms 降到 59–81 ns，调用线程分配由 10.91 MB 降到 112 字节。这组数仅覆盖快照构建。43 项定向回归通过，涵盖真实 Native 握手与 Worker 心跳和撤回身份。

实施批次 87 缩短了 Substrait 验证的固定开销。真实查询 JFR 的 9 秒窗口中，提交线程的 3,463 / 3,653 与下游准备线程的 3,757 / 3,769 个 Java 执行采样包含同一轮函数目录校验。当前存活 Worker 的不可变清单相同时，各 Stage 复用已校验的能力集合。清单变更重新核验，空集群和无效清单仍拒绝。使用 784,252 字节的实际目录做反向先后顺序对照，校验入口由 23.53–24.13 ms 与约 10.92 MB 调用线程分配降到 89–108 ns 与 24 字节，关系和函数能力判断一致。本地单 Worker 非剖析运行中，230 个样本再次核验完整结果和实际退出。相对批次 84 的相同夹具，串行首行 P50 常量由 52.43 降至 29.19 ms，点查由 126.41 降至 85.32 ms，选择性扫描由 109.80 降至 71.95 ms，计数由 101.07 降至 61.88 ms，分组由 142.88 降至 83.44 ms。52 项定向 Java 和 6 项实际握手、双 Worker 与持久客户端回归通过。

实施批次 85–86 在首个任务签发前为实时和持久 Stage 固定当批租户资源池。真实 PostgreSQL 中 64 个分区的放置读取从 64 次降到 1 次。当批保持相同 Worker 范围，下一批观察到资源池变更或停用。持久分区的资源池读取移到认领事务之前，回调只使用本地 Worker 和已绑定的资源池。实时派发的 33 项定向与实际用例通过，持久路径再通过 35 项回归，含动态表、物化视图、原始回执及接管。

实施批次 84 将客户端等待按结果可读、逻辑终态和物理释放分开。实际 HTTP 回归验证同一 observer 只占一个窗口，终态等待跨过 FINISHING，物理释放晚于逻辑终态时各自返回。460 个 B82/B84 查询样本全部核验结果与物理退出。每次状态请求由 4 次降到 2 次。两轮串行首行 P50 的差异为 1.35–5.62 ms，混合 P95 有升有降，暂不能归因端到端延迟变化。22 项定向 Java 测试和持久客户端回归通过。

实施批次 83 让索引谓词与已选中的不可变描述符一起进入分片过滤，删除按 ID 二次查询及其 30 秒缓存。过滤仍使用已发布的精确代次、快照和文件映射。缺少匹配证据的分片照常读取。真实 PostgreSQL 回归确认描述符选定后只借用一次连接读取 ACTIVE 文件映射。21 项定向 Java 与 35 项索引端到端测试通过，覆盖发布代次切换、快照、权限和读取结果。

实施批次 82 增加了复用同一个 SDK 客户端的 HTTP 与 Native Flight 实测。共享 E2E Worker 补齐生产环境已有的结果释放计时器后，逐次核验结果与 owner 的 drained 状态。本地单 Worker 和 4 个 Iceberg 文件跑了 150 次串行与每秒 8 次到达的 80 次混合样本，覆盖常量、点查、选择性扫描、全局计数和分组。串行首行 P50 依次为 51.08、120.79、107.87、97.96、139.92 ms，混合首行 P95 依次为 128.41、138.48、115.98、109.72、181.74 ms。混合物理释放 P95 依次为 135.98、147.49、131.03、118.97、191.58 ms。每类混合样本 16 次，客户端定时 P95 约 10 ms。使用 `PersistentQueryLatencyTest` 可重跑，原始阶段数据按 `event=persistent_query_sample` 输出。

实施批次 81 将实时读查询中 ACK 和 Worker 进度报告触发的下游准备交给有界线程池。每个查询合并待处理的就绪信号，排队到实际执行退出始终占用真实的查询操作租约。满队列以资源耗尽结束该查询。写入与持久 Shuffle 的确认顺序沿用各自权威路径。使用受控阻塞验证了 100 次重复信号合并、独立查询继续、排队取消仍保有租约和拒绝立即归还租约。28 项定向回归通过，包括真实双 Worker 与激活链路。

实施批次 80 删除了 Worker 中没有生产调用的 Java 分片过滤器及其 JSON 字节载荷。Coordinator 继续在选定 Iceberg 快照上剪裁分片，迟到过滤继续由现有 Worker HTTP 入口传给 Native 扫描。协议不可变性、真实双 Worker 和激活用例通过，40 → 2 个分片与 32,449 → 1,398 字节结果保持一致。另有 6 项定向 Native 动态过滤回归通过。

实施批次 79 删除了未连接实际生产者的旧动态过滤注册表、Planner 配置和 Coordinator 空过滤参数。静态 Join 字面量过滤仍在优化入口生效，Worker 原生过滤协议继续接受绑定到扫描节点的消息。独立分支通过 56 项定向测试和 Checkstyle，覆盖计划、任务协议、派发和实际 Worker 过滤入口。在完整当前工作树中，独立的双 Worker Iceberg 查询仍将 40 个分片降到 2 个，实际读取由 32,449 降到 1,398 字节。

实施批次 78 让 Paimon 和 Lance 每次分片扫描只生成一次目录部署摘要。查询与快照构建采用同一入口，逐分片仍检查目录状态与部署身份。使用同一个真实 Paimon 目录和稳定查询会话进行反向先后顺序对照，4,096 个分片的完整 Protobuf 摘要一致，线程分配由 9.81 MB 降到 0.773 MB，组中位耗时由 4.51–4.54 ms 降到约 0.206 ms。单分片分配增加 24 字节，组中位耗时由 2.48–2.53 µs 变为 2.58–2.62 µs。9 项定向 Java 和 13 项真实 Paimon/Lance 用例通过。

实施批次 77 将阶段 ACK 记录一次写入共享视图，并保留异常发生前已接纳的任务，供故障清理和取消。实际 Java 方法在相反顺序的两组对照中发布了相同的任务顺序、首个结果与结果指纹。4,096 个 ACK 的调用线程分配由 33.77 MB 降到 0.164 MB，组中位耗时由 1.96–1.97 ms 降到 45–47 µs。128 个 ACK 的分配由 39.7 KB 降到 5.4 KB，组中位耗时由 3.09–3.35 µs 变为 3.83–4.32 µs。单任务分配为 320 字节。已验证两项 ACK 用例和七项实际双 Worker 与激活链路用例。

实施批次 75 移除了 SQL、AST 与授权后优化入口各自执行的逻辑 Join 过滤分析。旧分析只描述候选，未向执行登记值。编译结果、授权计划、查询阶段和派发上下文也停止携带这些描述符。局部谓词优化仍使用现有规则。29 项定向 Java 用例通过，覆盖计划所有权、常量构建键剪裁、分片约束、局部聚合和时间戳。

实施批次 74 将已受理的 Exchange Protobuf 和完整尝试身份直接交给 Worker 的 Native 绑定。删除 Java 内部拆成端点、票据和发布 DTO 再重建的路径。端点顺序、期限、连接端口和生产者证明在同一个 canonical 校验入口核对。128 个直连生产者的 Corretto 17 对照中，全部 96,633 字节的 JNI 命令摘要相同。调用线程分配由约 638–658 KB 降到 354 KB，组中位耗时由 129–151 µs 降到 75–91 µs。该测量覆盖本地绑定和编码。 隔离验证通过 21 项定向 Java 用例。真实 COPY 保留已受理私有票据后，隔离 Native JVM 通过对象指定版本读取、Flight 结果和本地重启用例。

实施批次 73 在选定 Iceberg schema 上预先计算历史字段名与 UNKNOWN 列的映射。所有文件沿用同一不可变列表。4,096 个 Iceberg SDK 文件任务、256 列的 Java 对照各组预热 24 次、测量 13 次。常规 schema 的线程分配由 1.23 MB 降到 1.13 MB，组中位耗时由 1.23–1.30 ms 降到 0.68–0.71 ms。加入一列 UNKNOWN 后，分配由 1.46 MB 降到 1.13 MB，耗时由 3.22–3.31 ms 降到 0.65–0.71 ms。两种 schema 的分片 Protobuf 摘要在各自的先后次序对照中一致。测量范围是 SDK 文件任务转换。

实施批次 70–71 按同一 Task 和扫描 Node ID 让每个扫描 driver 消费完整的过滤发布序列。旧路径只有首次 drain 获得过滤，其他 driver 继续全读。本地 C++ 受控探针中，未发生发布时的批次检查中位耗时由 13.78–19.51 ns 降到 3.17–4.20 ns。真实 Parquet 回归还复现了读取中途修改 Velox ScanSpec 导致惰性列索引失配。现在打开 reader 前仅下推已证明物理列、类型和读取语义一致的条件，迟到条件在删除、映射及类型提升后的逻辑批次执行。真实两个扫描实例均取得同一过滤，旧 `INTEGER` 列提升为 `BIGINT` 后正确过滤，零行批次之后依然读到匹配记录。27 项定向和 4,345 项 Native 完整回归通过。

封存批次 67 的独立进程产物通过 7 项 CDC 和 6 项关系写入故障恢复，以及 11 项产物身份检查。该次命令中的 4 个共享测试环境因缺少 AI 容器镜像参数而初始化失败。批次 68 补齐参数后的隔离运行通过 Activation Smoke、真实双 Worker 和 22 条 TPC-H 派生 SQL 正确性用例。

本地独立进程对照使用封存的批次 67 与 71 工件。四个进程组反转执行顺序，常量查询和 3 个 Iceberg 文件的聚合各组预热 3 次后测量 9 次。72 个采样逐次通过结果 `42`、真实 Native 退出与零租户槽位检查。每次启动新 CLI，端到端组中位耗时约为常量 1.45–1.49 秒、聚合 1.54–1.57 秒，组间波动覆盖了部分差别。常量查询固定为 1 个任务与 3 次状态 HTTP，聚合固定为 4 个任务与 9 次状态 HTTP。这组测试覆盖实际进程、提交、结果和退出，生命周期微测的提升未外推为完整查询提速。

实施批次 69 合并 Worker 对同一 Native 失败回执的三次解析。原始失败分类、资源消耗和实际释放信息来自同一个已校验对象。CDC Worker 直接构建带真实退出状态的失败回执。对含 512 个算子的 28,921 字节回执，反转顺序的 Corretto 17 探针分配由 627 KB 降到 253 KB，组中位耗时由 240–242 µs 降到 132–139 µs。输出回执字节和 SHA-256 一致。

实施批次 68 让 Worker 监控等 Native 生命周期的终态信号。读取无等待时走标量状态。50 ms 作为运行中租户配额检查的最长等待，FINISHED、FAILED、CANCELLED 可立即唤醒。CDC 后端在发布真实回执后唤醒等待者。受控 C++ 探针按交错顺序各跑 64 次终态转移，从终态发布到观测的 P50 由 26.7–28.5 ms 降到 8–12 µs，P95 由 54.0–55.1 ms 降到 31–60 µs。该对照只计本地生命周期通知，真实查询延迟独立测量。

实施批次 67 将 AQE 建议表示为按源分区顺序排列的左闭右开区间。范围必须无空洞、无重叠，缺少分区统计与已发布空分区分别处理。8,192 个分区、每分区 32 KiB 的算法对照均得到 128 组和 256 MiB 的总字节证据。分配由约 464 KB 降到 136 KB，组中位耗时由 0.38–0.68 ms 降到 0.16–0.20 ms。消费端的范围绑定与按物理布局派发独立验收。

实施批次 66 将 Remote Shuffle 消费的 JNI 帧拆为控制信封和有界数据参数。1 MiB 数据帧的 Java 探针中，Protobuf 信封从 1,048,868 字节减到 283 字节，调用线程分配从约 3.15 MB 降到 1.05 MB，载荷和尝试身份一致。Native 继续校验序号、范围、chunk 与 batch 摘要，控制通道拒绝内联数据。实际 JNI、读取与资源释放由跨边界用例验证。

实施批次 65 在派发前为每条输入边封存生产者记录或已提交的外部 Shuffle 描述。外部输入共用端点范围、发布代次、读权限和生产者集合摘要，每个消费任务仍携带自己的分区发布证据。512 个消费分区、64 个服务端点和 2,048 条无关派发记录的本地构造对照中，线程分配由 5.59–5.70 MB 降到 0.45 MB，组中位耗时由 11.44–13.99 ms 降到 0.83–0.91 ms。对照反转执行顺序，512 个请求绑定的合并摘要一致。该数据覆盖签发前的绑定构造。

实施批次 64 移除没有范围映射的自动并行度与子计划改写。实际 Java 评估和派发计数探针复现 4 个非空发布分区被缩为 1 个消费任务，该任务只读取分区 0。当前 DAG 派发使用原图，并继续从已提交的外部 Shuffle 采集 AQE 建议。可执行 AQE 仍需对每条输入冻结读源坐标及范围。

实施批次 62–63 清理终态统计在 Native 和 JVM 各自内部的 Protobuf 往返。同一次 Velox 统计快照产生算子和列式读取指标，C++ 将类型化统计填入最终回执才编码。Worker 使用已解析的统计展示计数并在实际释放时替换为最终快照。45 项 Native 定向测试及全量 4,341 项通过，覆盖 Native 写出失败、真实 Shuffle/动态过滤、最终回执和缓存计数。

实施批次 61 在实时 Exchange 绑定中复用已派发请求的不可变 Substrait 字节。同一来源的每种精确字节内容进行一次解码与 schema、分布验证，生产分区的派发身份仍按原记录保留。192 个生产任务共享一份 57,640 字节的嵌套 schema 计划，Corretto 17 探针每组预热 8 次、测 7 个窗口，每窗口 4 次绑定。反转执行顺序后，调用线程分配由约 551 MB 降到 2.80 MB，窗口中位耗时由 116–124 ms 降到 2.40–3.28 ms。消费端计划 SHA-256 一致。该测量只覆盖接收生产计划字节至绑定消费计划。

实施批次 58–60 合并有界的小批 Shuffle 页、去除页选择范围的二次切片，将分区哈希器和选择数组交给产生数据的 Velox driver。128 条稀疏选中行使用与生产相同的累加器、序列化和解码，编码由 8 页、3,264 字节降到 1 页、1,304 字节，逐行结果相同。宽批次仍按实际字节预算分页，生产和消费继续受各自队列上限约束。重新构建并打包后，Native 全量 4,341 项通过。

实施批次 57 将预聚合扩展到已知快照匹配 NDV、单个直接分组键的 `COUNT` 和整数 `MIN/MAX`，仅在预计行数和字节都减少时启用。81 行小夹具因预计宽度增长保留既有计划。扩大到 161 行的真实 Iceberg 双 Worker 夹具，8 个 Native 扫描任务的本地聚合总输出不超过 40 行。校验 5 个分组的计数和极值、NULL 分组及两个 Worker 的实际释放。

实施批次 56 在生成值读取中复用已验证发布的派发规格及生成定义，移除生成列绑定第二次读取清单和规格的调用。真实 PostgreSQL 16 的缺失声明探针各跑 10 轮，同一受保护读取的清单和规格查询均从每轮 2 次降到 1 次，拒绝条件一致。空分片阶段可直接跳过生成值绑定入口。

实施批次 54–55 将纯并行度估算切换到编译中已经选定的快照工作量，分片提取在一次调用中复用同一 Calcite 表对象的校验证据。真实 PostgreSQL 16 与受控 Paimon 夹具各跑 10 轮，每轮申请 8 次并行度估算。表所有权和目录部署读取分别从 8 次降到 0 次，随后交接执行绑定仍各检查 1 次。一次同表自连接提取的两类读取分别从 3 次降到 2 次，结果仍是同一 8 行快照与一个扫描源。不同的 Calcite 表对象各自验证，冲突的读绑定仍被拒绝。

实施批次 53 清理 Native 接纳后无消费者的文件描述字节。普通 Iceberg 分片和非 Lance 的 Paimon LSM 文件已解析出所需映射，直接使用这些字段。Lance C FFI、带索引提示的分片和 Paimon 删除向量保留各自需要的读描述，Native 缺省路径无需再次解析空内容。重建后二次执行全量 Native CTest，4,340 项通过。

批次 50–56 联合定向回归通过 397 项，含 57 项 Java 与 Native 端到端用例，另有 1 项手动 golden 更新用例按原配置跳过。覆盖真实双 Worker、COPY 私有入口、Lance/Paimon、索引提示、生成值发布、物化、协议描述符握手和排空。

实施批次 52 将 Paimon Java 读取注册表的 TTL 清理限制在按登记时间排序的过期前缀。活跃 lease 继续持有原 SDK 对象及操作凭据。真实 SDK split 的 16,384 次登记探针使用相同 TTL 和容量，5 个测量窗口的中位耗时从 709.61 ms 降到 7.88 ms，登记阶段分配从 3.80 MB 增至 4.72 MB。两组均校验了领取的 SDK split 身份及查询结束后清空注册表。该瓶颈影响显式 Java 读取路径，Native 批次 48 已绕过该登记。

实施批次 51 将可合并的全局 `COUNT` 和整数 `MIN/MAX` 放在扫描任务本地，随后由单个阶段合并结果。仅当预计扫描行数超过并行生产任务数时启用。FILTER 归属原始输入的聚合，`COUNT` 用 `SUM0` 合并，整数极值沿用原类型和 NULL 语义。实际 Iceberg SQL 在两个独立 Worker JVM 上读取 80 行、8 个文件，8 个扫描任务各输出一条聚合记录给汇总阶段。结果的总数、条件计数、无匹配时的 NULL 和最大值与夹具一致。两端的 Native 排空回执也通过验证。

实施批次 50 将 `DataSplit.file_detail` 改为直接声明 `FileSplitDetail`，保留旧字段编号为 reserved。Java 绑定、索引提示和任务组装共用类型化对象。Native 从同一已解析对象校验 Lance 读取单元与 Paimon 删除向量的存储绑定，避免在接纳期间为这些检查再次解析分片。2,048 分片、每文件 48 列映射的 Java 探针从 3,336,148 字节降到 3,211,220 字节。交换先后顺序的两组复测中，分配由 73.17–73.37 MB 降到 39.83 MB，组中位耗时由 22.25–26.53 ms 降到 14.77–15.32 ms。物理身份与文件描述的合并摘要一致。重建的 Native 全量 4,340 项通过。生产节点部署使用同一描述符的 Java 和 Native 构建。

实施批次 48–49 移除 Paimon Native 分片为 Java 页读取器保留的登记。显式增量和预热的 Java 读取仍使用自身作用域。Native 追加扫描直接展开文件，同一次物理扫描按实际源 schema ID 复用字段映射和类型提升。真实 Paimon SDK 读取同一个 96 文件 Parquet 快照，各预热 5 轮、测量 11 轮。每轮待领取 Java 分片从 96 个降到 0。`FileIO` 的 schema 文件打开从 193 次降到 2 次，存在性检查从 198 次降到 7 次，包含 SDK 自身必要的目录和快照定位。在带计数、校验和分片编码的探针中，调用线程分配中位数从 10.48 MB 降到 2.71 MB，耗时中位数从 13.59 ms 降到 8.63 ms，96 个分片的 Protobuf 摘要一致。首次遇到每个文件 schema 时仍校验存在性并读取其确切版本。 [Paimon schema 规范](https://paimon.apache.org/docs/2.1/concepts/spec/schema/)

实施批次 47 将 Paimon 非空物理读绑定验证和分片枚举合并到同一次目录打开。使用真实 Paimon FileSystem Catalog 和 64 行 Parquet 提交，各重复 10 次。仅统计绑定完成后至分片耗尽的 SDK 调用，每轮 `invalidateTable` 和 `getTable` 均由 2 次降到 1 次，分片来源快照一致。当前 schema 与选项漂移检查、已选快照身份检查和普通扫描规划结束时的快照复核仍在。同一目录和分片作用域内才执行合并。该数据覆盖本地 SDK 调用次数。

批次 47–49 共同通过 146 项定向验证，含 50 项端到端用例，覆盖 Paimon 历史快照、真实文件 schema 演进、Native MOR/MOW、元数据表、Java 增量读取与查询作用域释放。

实施批次 46 将 Iceberg、Paimon 和 Lance 的执行分片直接交给目录绑定与调度。原路径在 Paimon 和 Lance 的授权绑定前后重复编码、复制并解析字节。2,048 个 Paimon 追加分片使用真实 Connector 代码和目录句柄，隔离测量编解码与调度交接。各五组每组 20 次。调用线程分配从 30.92 MB 降到 15.74 MB，组中位耗时从 9.05 ms 降到 5.89 ms，每个分片的目录授权匹配且物理分片指纹一致。绑定保留 Connector 的实际扫描字节估算。持久化分片在记录生成时编码一次。214 项定向测试通过，含 39 项端到端测试。

实施批次 44–45 让 Worker 的 HTTP 接纳、结果签票和提交登记共用原请求的结构校验证据，并跳过相邻且来源与快照都相同的分片身份分配。实际截止时间、当前控制代次、Worker 身份和执行所有者仍由接纳流程检查。重新绑定需要新的结构校验。8,192 分片、256 KiB 计划的本地纯校验探针各跑五组每组 80 次。调用线程分配由每任务约 608 KB 降到 6.2 KB，组中位耗时由 3.90 ms 降到 1.22 ms。分片身份上限仍按全任务去重后检查。251 项定向测试通过，含 20 项端到端用例，覆盖签票、接纳代次、精确排空、Native 握手与私有 COPY。

实施批次 43 消除 Java 任务构建与 C++ 任务验证中重建、序列化和哈希协议描述符的工作。JVM 固定一份版本和摘要，Native 能力握手与任务验证共用进程内身份。采用相同 262,160 字节 Bloom 过滤器和任务构建探针重测，各五组每组 256 次。Java 任务构建线程分配由每次约 1.02 MB 降到 6.5 KB，组中位耗时由 979 µs 降到 26 µs。过滤绑定指纹一致。该测量覆盖 Java 任务构建，C++ 接纳耗时与跨机执行独立计量。121 项定向 Java 测试通过，含 25 项端到端测试。重新编译后的 Native 全量 4,339 项通过。

实施批次 42 将初始动态过滤的任务构建改为复用已经绑定 Node ID 的 `RuntimeFilterBinding`。原路径在 Stage 侧把条件编码到 REST DTO，再在每个任务构建时克隆字节、解析并重建消息。262,160 字节 Bloom 过滤器的本地 Java 测量，各五组每组 256 次。任务构建线程分配由每次 1,545,283 字节降到 1,019,703 字节，组中位耗时由 962 µs 降到 893 µs，Protobuf 绑定指纹一致。76 项定向 Java 用例通过，其中 21 项为端到端，52 项 Native 动态过滤测试通过。生产自动跨 Stage 过滤仍需赢者收集和扫描剪裁证据。

实施批次 41 对有双侧 NDV 的 inner equi-join 使用 `1 / max(NDV)`，多键使用选择率最低的已知键。单键缺任一侧 NDV 时使用 Calcite 的估计。两侧各 10,000 行、NDV 分别为 100 和 1,000 的均衡键夹具实际匹配 100,000 行。两个等价写法的估算均从 1,000,000 行变为 100,000 行。分布未知和键相关时仍需真实全链路归因。 [Spark Join 估算实现](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/logical/statsEstimation/JoinEstimation.scala)

实施批次 40 将发布事务中的逐列掩码 SQL 合并为一次所需列集合的读取，标签只做一次稳定表身份投影。64 列、含 1 个实际掩码的受控 Paimon 权限复核重复 10 轮，掩码 SQL 从每轮 64 次降到 1 次，所有列的已观察策略保持等价。

批次 40–41 合并回归通过 253 项，含 54 项 SQL/Native 和启动端到端测试。真实测试覆盖列标签、行策略、提交前复核、Iceberg/Paimon/Lance、两 Worker Shuffle 和 CBO 关联查询。以本地对照说明 SQL 次数和估计误差，SF100/SF1000 跨引擎结论需专用环境出数后验收。

实施批次 39 将写入和发布复核的行策略读取也收敛到同一事务内的一条语句。受控 Paimon 和 PostgreSQL 中，各运行 10 轮，读取从每轮 2 次降到 1 次。发布复核继续比较编译时所用谓词。首次修改中名称和 ID 的两条旧读取仍会产生重复项，写入与 Lance 导出回归捕获了不一致。合并后 210 项定向测试通过，含 37 项端到端测试，其中覆盖受 RLS 和列掩码约束的写入、发布与表重命名。

实施批次 38 用 Calcite 的直接列来源计算 Join 和过滤选择率。三表 Join 中，原路径读取了与连接键无关的 `a` 表统计，另一个操作数顺序则触发默认估算。两个等价写法分别给出 0.5 和 0.15，修改后均读取 `b`、`c` 的实际键列统计，得到 0.001。派生列和多来源列使用 Calcite 的启发式估算。 [Calcite 列来源](https://calcite.apache.org/javadocAggregate/org/apache/calcite/rel/metadata/RelMetadataQuery.html#getColumnOrigin(org.apache.calcite.rel.RelNode,int))

实施批次 37 删除从 min/max/NDV 推导的等频直方图，保留已取得的行数、NDV 和边界。各 100,000 行的两种分布拥有相同的 min/max/NDV，谓词分别命中 99,999 和 9 行，分桶估算均为 100。移除后两种分布均使用 0.5 的通用估计，偏差仍需用当前快照的真实分布修正。该探针用于验证证据边界。

实施批次 35–36 同一编译内的自连接共用一个授权表的行策略值，以一个 PostgreSQL 快照读取稳定 ID 绑定，并检查当前表名的未绑定策略。64 列受控 Paimon 自连接的行策略阶段各运行 10 轮，每轮 SQL 从 10 次降到 5 次，再降到 4 次。两个扫描仍各自应用过滤，保留相同的计划指纹。

实施批次 34 将查询授权的角色范围下推到 PostgreSQL。用生产授权实现和 4,097 条授权的 PostgreSQL 16 夹具，各运行 10 轮。通过授权的请求执行 1 次授权读取，返回行数由 4,097 降到 1。无有效角色的请求继续拒绝，授权读取由 1 次降到 0 次。保留管理读取和既有授权顺序。239 项定向验证通过，含 56 项端到端测试。

实施批次 33 将用户直授、身份组映射和已注册角色的交集归并成单条 PostgreSQL 语句。64 列 Paimon 自连接的列掩码阶段，以相同角色、掩码和表重复测量 10 轮，角色 SQL 由每轮 4 次降到 1 次，阶段读取由 8 次降到 5 次，保留同一计划指纹。170 项定向验证通过，含 41 项端到端测试。

实施批次 32 将每列掩码读取改为按授权表读取一次并在规划中复用。64 列受控 Paimon 表自连接的掩码阶段，对照各跑 10 轮。每轮 PostgreSQL 读取由 1,024 次降到 8 次，含掩码表达式的计划指纹一致。每次编译重新读取当前策略。114 项定向验证通过，含 42 项端到端测试。

实施批次 31 删除同一次表身份校验内的重复目录部署查询。读、写和 DDL 继续验证当前部署版本，显式 allowlist 首次绑定格式身份的写操作继续单独校验。真实 PostgreSQL 16 与受控 Paimon 数据源各运行 10 轮。一次身份校验的目录部署 SELECT 从 2 次降到 1 次，完整表绑定从 6 次降到 4 次。所有权 SQL 和统计 SQL 单独计数。140 项定向验证通过，含 33 项端到端测试。

实施批次 30 将 Calcite 查表拿到的选择器交给同一编译绑定。绑定只接受来自当前 metadata 实例的选择器，已选定的表在同一编译复用。真实 Iceberg JdbcCatalog 与 PostgreSQL 16 分别执行 10 轮单表和自连接计划。单表从查找到行类型、统计和绑定交接累计指针 SELECT 从 3 次降到 2 次，自连接加分组从每轮 6 次降到 2 次，计划指纹一致。139 项定向验证通过，其中 51 项为真实 SQL/Native 与启动冒烟。另有 1 项用于更新计划快照的用例按默认设置跳过。受控 Paimon 表发现后被外部替换的用例继续拒绝绑定。

实施批次 29 将等待本地操作、必要报告发布和精确 Worker 排空回执统一为正常的物理清理等待。此前真实端到端中，逻辑终态早于 Worker 排空的正常顺序进入了异常分支并安排退避，首个等待窗口为 100 ms。报告发布结束和取消请求的精确排空回执均推进现有清理通知，真正失败继续退避。保留每个 attempt 的原始回执和发布约束，并删除同一报告在 Coordinator 的第二次排空登记。

相同 SQL/Native 回归选择中，缺少排空回执触发异常退避的记录从 118 条降到 0 条。固定时钟回归重放旧的等待分支时，直接取消响应的回执无法触发即时释放。候选在时钟不推进的情况下即可完成物理清理。双 Worker 的回归从临时任务表改为记录真实 Native 回执中的 Worker 准入域和执行实例，并校验所有已签发任务均排空以及查询物理清理已经完成。

117 项定向验证通过，其中 41 项为真实 SQL/Native 端到端及 Activation Smoke。双 Worker 用例在更新证据采集后两次独立重跑均通过。

实施批次 28 让 Calcite 行类型和行数共用同一编译内的已授权快照声明。实际提取连接器声明或读绑定时仍检查当前所有权。复用声明后，同一受控 Paimon 自连接与分组的规划探针在 10 轮中每轮所有权 SQL 从 9 次降到 5 次，物理计划指纹与前几轮一致。表解除管理并重新接管时，优化器仍持有原声明用于本次编译，连接器绑定的移交会因所有权变化被拒绝。

161 项定向测试通过，其中 41 项为真实 SQL/Native 端到端及 Activation Smoke。TPC-H Q9 SF1 验证预热 10 次、测量 50 次，中位数 540 ms，最长 699 ms。另有 1 项计划 golden 写入用例按默认设置跳过。

实施批次 27 移除了 Calcite 可选能力探测中的查库。调用栈显示 Calcite 在验证与优化时调用 `unwrap` 探测 `InitializerExpressionFactory`、谓词统计和列唯一性等接口，探测结果不依赖实际表绑定。新路径直接判断类型。相同受控 Paimon 自连接与分组的 10 轮规划对照每轮少 16 次所有权 SQL，总数从 25 次降到 9 次，输出计划指纹一致。实际读取连接器声明或绑定时继续校验表所有权。

152 项定向结果通过，其中 32 项为实际 SQL 端到端，另有 1 项需显式开启的计划 golden 写入用例跳过。

实施批次 26 把 Calcite 的 `getStatistic()` 改为读取已授权绑定中的快照行数。读取列声明和提交给后续物理规划的绑定仍校验当前所有权。真实 Paimon 表绑定完成后，连续 8 次 Calcite 统计请求的所有权 SQL 从 8 次降到 0 次，行数仍为 8。

使用同一受控 Paimon 表的两个别名执行自连接与分组 SQL 规划。两个版本各运行 10 轮，规划阶段的所有权 SQL 从每轮 31 次降到 25 次，物理计划指纹一致。剩余查询计入后续所有权读取的审查。

78 项定向测试通过，其中 32 项覆盖实际 SQL 执行，包括历史快照、Lance 与 Paimon 读取及双 Worker Shuffle。

等值连接代价估算的两侧均需要连接器统计时，在同一条 PostgreSQL 语句读取匹配快照版本的 `ANALYZE` 统计与当前目录部署。两张受管 Iceberg 表和一个重复别名的真实 SQL 中，统计阶段的 SQL 从 2 次统计读取与 2 次目录读取合并为 1 次。这批测试的点查和未运行 ANALYZE 的对照均为 0 次。其中一张表提交新快照后，只使用仍匹配的统计。六条 SQL 的 Flight 结果和物理归还均已核对。该连接另外记录 2 次初始读准入和 4 次 Iceberg 目录指针读取。49 项定向、65 项同源 macOS 端到端和 9 项 Linux 独立进程回归通过。macOS 首轮有三个独立进程用例因产物旁缺少构建缓存而未启动。补齐同哈希 Native 库的原始 Release 构建缓存后整组复测通过。macOS 和 Linux 的角色清单 SHA-256 分别为 `4c107b96916ae85611eed0684b9327911943c46e6a5deae8e2610be283f25d11` 和 `cdec1507bd4a788c90651bb1eff15c04e6c25c1df15b008ab9eeef223c709c8d`。

实施批次 25 删除了优化器按表名预查 `ANALYZE` 的中间层。优化估计复用规划源的授权表 ID 和连接器选定版本，单次编译内只加载一次。Iceberg 和 Paimon 的历史快照使用自身统计，增量范围保留 unknown。Lance 从同一次已校验发布取得声明和统计，消除内部再次查持久目录及验证 Native manifest。

真实 Paimon SDK 和 PostgreSQL 16 `pg_stat_statements` 探针对当前、历史和增量源分别运行 10 轮，每轮在同一源完成绑定后发出 8 次优化统计请求。重放旧入口时，每轮额外发出 8 次所有权查询和 8 次统计查询，三种输入都拿到当前发布统计的 999 行。候选路径当前源为 0 次所有权查询、1 次统计查询和 999 行。历史和增量源两类 SQL 均为 0 次，历史返回对应快照的 3 行，增量返回 unknown。这里计量优化统计请求的额外 SQL，表绑定、目录和物理读取各自的权限检查另计。

94 项定向测试通过，其中 33 项覆盖实际 SQL 执行，包括双 Worker Shuffle、Lance 发布、Paimon 增量物化与受控表的 `ANALYZE`。

实施批次 24 将普通 Iceberg 读绑定的统计输入限定为快照摘要和已加载的 Puffin 元数据。NDV 仅取单列、快照和序列均匹配的条目；已有完整统计采集路径继续读取文件级 min/max、NULL 计数与 sketch。Planner 保留缺失 NDV、NULL 计数和平均值宽度的 unknown 状态，缺失 NDV 时使用代价模型的默认选择性，避免将整表行数当成列的 distinct count。

同一个 20 manifest、10,000 文件描述的 Iceberg SDK 探针，各预热 5 次、测量 11 次。读绑定涉及的统计阶段 Avro 流开启从 41 次降到 0 次，统计 P50 从 27.041 ms 降到 0.018 ms。随后枚举的 Avro 流开启从 20 次变为 21 次，包含从枚举阶段首次打开的 manifest list，两阶段合计从 61 次降到 21 次。此处仅计量本地 SDK 文件流和统计阶段，远程 GET、规划总耗时及 Join 计划质量需单独验收。

104 项定向测试通过，其中 25 项为真实 SQL 端到端，覆盖 `ANALYZE`、历史快照和双 Worker。另有 58 项计划及统计用例通过，包含 TPC-H 21 个和 TPC-DS 31 个受支持查询的计划 golden。自动测试跳过了需显式开启的 golden 重写入口。

实施批次 23 从 Calcite 的表发现路径移除会被绑定覆盖的声明读取。发现表时校验存在性，Calcite 请求列或统计时使用同一查询的授权读绑定。单次 `unwrap` 只读取一次该绑定。以真实 Iceberg JdbcCatalog 和 PostgreSQL 16 `pg_stat_statements` 复核 10 次，单表查找、首次行类型、快照与统计读取累计目录指针 SQL 从 4 次降到 3 次。首个行类型仍会加载两次目录指针，表所有权与治理 SQL 另计。

同一条双 Worker 三表查询在 HadoopCatalog 的派发前加载从每张表 7 次降到 5 次，总数从 21 次降到 15 次。PostgreSQL 的精确指针计数另取自上面的单表探针。

`IcebergReadBindings.bind` 在首次规划统计时会从每个 data manifest 聚合全表列统计，分片枚举随后从同一个快照规划文件。用真实 Iceberg SDK、20 个 manifest、10,000 个文件描述的探针，11 次采样每次观测到统计阶段开启 41 个 Avro 读取流，枚举再开启 20 个，统计 P50 为 27.041 ms。读取流计数包含 SDK 重开，远程对象 GET 和带宽需另测。后续冷路径只编入有界的快照统计证据，并以实际 Join 计划和估计误差验证未知统计的处理。

71 项定向测试通过，覆盖真实 PostgreSQL 目录、Iceberg 和 Paimon 读绑定、行类型与派生关系，以及 21 项真实 SQL 端到端。后者包含双 Worker 和两种格式的系统表。

实施批次 22 移除 Coordinator 分片链路的 Base64。Connector 的二进制 payload 解析一次，stage 在绑定发布对象之前校验分片身份和 ID 唯一性。生成列、检索、索引提示、分区放置和任务构造复用同一 `DataSplit`。生成列使用已验证发布清单中的行数和字节预算，移除第二次 `findPublishedGeneration` 读取，并严格校验引用的租户、源表、快照与列。

10,000 个合成分片的实际 Java 阶段编解码探针，预热 12 次、测量 21 次。包括 connector payload 转换、stage 分片校验和生成列输入校验，P50 从 109.903 ms 降到 58.340 ms，当前线程分配量中位数从 261,612,264 B 降到 129,332,144 B。此处计量编码和校验，发布对象的 PostgreSQL I/O 与完整 SQL 延迟独立测量。

57 项定向测试通过，包含真实 PostgreSQL 发布绑定、索引提示传递、双 Worker Shuffle、检索与异步生成列 SQL，以及分片身份、文件详情和重复 ID 的拒绝用例。

SpotBugs 对两处共享不可变 Protobuf 消息的字段产生内部可变表示告警。过滤条件同时限定持有类、字段、消息类型和误报规则。 [Protocol Buffers Java 生成代码](https://protobuf.dev/reference/java/java-generated/#builders) · [SpotBugs 字段匹配](https://spotbugs.readthedocs.io/en/stable/filter.html#field)

实施批次 21 删除构造 `DataSplit` 时对同一个不可变 `FileSplitDetail` 的两次 protobuf 序列化和解析。分区值在输入边界复制一次并保留字段次序。该批次的线上表示使用 `Any.pack`，批次 50 已直接使用类型化字段。 [Protocol Buffers Java 生成代码](https://protobuf.dev/reference/java/java-generated/#builders)

32 列映射、10,000 个合成文件 split 的真实 Java codec 探针，预热 12 次、测量 21 次。构造 P50 从 95.561 ms 降到 19.417 ms，当前线程分配量中位数从 294,798,400 B 降到 55,998,400 B。总 wire 大小均为 14,877,780 B，末个 split 的 SHA-256 一致。该探针计量打包开销，查询端到端与网络发送独立测量。

实施批次 20 将 Iceberg 全量、历史和增量路径改为按需枚举。请求首批时才打开 SDK 文件计划，每批仅构造本批 split，增量扫描整次复用一次列映射。只有不带约束且完整耗尽的全量扫描可填充缓存，单次上限 4,096 个。Coordinator 所有表源共用 30 秒单调时钟期限和 100,000 个 split 上限。末批超时时拒绝整个 stage，超额批次在绑定和入列表前拒绝；取消和出错关闭实际 SDK 文件计划。Stage 仍需收齐 split 才进行全局放置。

实际 HadoopCatalog 的 10,000 文件描述探针，各预热 5 次、测量 11 次。请求一个 split 后关闭，P50 从 11.340 ms 降到 2.647 ms，发起线程分配量中位数从 11,686,024 B 降到 882,616 B。全量耗尽 P50 从 10.516 ms 变为 10.825 ms，发起线程分配量从 11,768,480 B 变为 11,532,160 B。该实验只读取 Iceberg 元数据，额外的 SDK 后台线程分配、数据文件读取和全阶段放置延迟需独立测量。

真实双 Worker 三表查询在派发前观测到每张表 7 次 `loadTable`，共 21 次。该用例使用 HadoopCatalog，计数来自目录加载日志。`LazyMetadataSchema` 先读取句柄与声明，`PlanningReadBindings` 随后再绑定快照。Calcite 请求声明、统计或 unwrap 时会重复调用 `PlanningReadBindings.resolve`，即使命中也会再次查询表所有权。该调用链需要用真实 JdbcCatalog 的 `pg_stat_statements` 分别确认目录指针和授权表的次数，再将规划请求复用到现有查询级只读绑定，验证 DDL 围栏与权限撤销。

63 项定向测试通过，覆盖目录实际 SDK 关闭、超时末批、多个表源共享上限和 Worker 引导关闭。随后 100 项定向回归通过，其中 41 项为真实 SQL 端到端，覆盖双 Worker Shuffle、历史快照、复合约束、增量及删除文件。

实施批次 19 将 Worker 结果任务的终态 HTTP 发送和重试移出物理清理线程。归还 Native 资源时立即保留精确的本地 admission seal，原始 FINISHED 回执和后续 released 回执由同一任务按 ACK 串联。HTTP 使用异步请求和有界退避，同时最多发起 64 个请求，排队计入回执轮次期限。每轮复用同一二进制请求体。失败重放保留原 envelope 和 sequence，收到 released ACK 后才移除任务注册项。结果保留扫描触发合格的释放或回执重试，无需在扫描线程等待 HTTP。

在两个真实 Native Flight 任务的探针中，基线刻意持有任务 A 的初始 FINISHED ACK 至少 300 ms，任务 B 从报告 FINISHED 到完成物理释放耗时 306.815 ms。候选在 5 次独立运行中均于 A 的 ACK 返回前完成 B 的内存归还，测得 0.66–1.77 ms。另经生产 HTTP reporter 分别扣住 A 的初始 FINISHED ACK 和 released ACK，两种情形均验证 B 先归还内存、发送真实 released 回执并退出注册表。探针时段只覆盖 Worker 结果释放，跨主机查询和租户槽位端到端延迟另测。

50 项定向 Java 测试通过，覆盖真实 Flight、清理重试中 envelope 对象一致、HTTP 回执丢失与额度饱和、Coordinator journal 路径、任务取消和双 Worker Shuffle。Worker 启用 `-Xcheck:jni` 验证。随后复核 Worker 引导关闭时，客户端仍等待任务所有者实际退出，再关闭回执传输。

实施批次 18 删除 Worker 每 100 ms 扫描消费结果的 `ConsumedResultSweep` 及常规 JNI drain evidence 查询。Velox 完成回调直接确认停产。Native 在停产、撤权、队列清空、Flight reader 和 Arrow buffer 的最后一个持有者退出后发送一次通知。Worker 用对应的 Native handle 注册回调。回调只排队，释放在现有 Worker 清理线程上执行。初始 FINISHED 的原始回执先按序保留，Native 内存可以在等待该回执的 ACK 时归还，物理退出回执继续按原顺序发布。每个仍有结果的任务持有一个到期清理，到期或收到退出通知后进入同一个确切释放路径。慢速保留扫描仍负责历史清理和失败补偿。

同批就绪的调度探针调用实际 Worker 注册表和清理实现，用可控信号代替 Native I/O。扫描基线中 512 个结果最晚 840 ms，2,048 个最晚 3,321 ms。事件路径在预热 3 次后的 5 次测试中，512 个结果的最后回调为 1.33–2.24 ms，2,048 个为 2.61–30.92 ms。该探针只计量从完成信号到释放回调的调度开销。真实 Native Flight 用例另行验证 EOF、初始 FINISHED 回执阻塞时的实际内存归还、精确 released receipt 及发送失败后的重放。

4,339 项 Native CTest 全量测试、58 项定向 Java 测试和 4 项隔离 Native 集成测试通过。重新打包当前 Native 后，19 项 Worker 测试启用 `-Xcheck:jni` 复核。单独用 ASan 编译结果所有权代码，运行 1,000 次 reader 与真实 Arrow slice 并发归还。完整 ASan 配置拒绝当前未插桩的 Velox archive，完整链路的 ASan 结果仍待验证。

实施批次 17 将查询终态后等待本地操作退出改为事件驱动。Coordinator 在同一内存临界区关闭 issuance 并检查操作是否结束，等待本地操作期间不创建定时重试任务。最后一个操作退出后立即向现有有界清理池调度。退出发生在检查与登记等待之间时也保留通知。Worker 的精确 drain receipt、连接器、结果资源和 Shuffle 回收仍由实际完成条件约束。数据库、RPC 等失败继续退避，清理工作池拒绝的任务仍可由现有队列接管。

对照使用同一套真实本地 Coordinator 发行与退出路径，预热 8 次，测量 51 次。以最后一个本地操作开始关闭为起点，以实际准入释放回调为终点。P50 从 101.789 ms 降至 0.083 ms，P95 从 104.465 ms 降至 0.168 ms。每次确认操作仍活跃时准入未释放，最终只回调一次，查询退出活跃集合。该对照使用进程内 journal，跨主机 Worker、慢查询和远端对象存储的回收尾延迟单独计量。

同一批次合并远端 Shuffle 终态持久化的 scope 查询与意图插入。事务先锁定活跃 execution anchor，第二条 statement 在锁后快照上校验 scope owner 并原子插入意图。这样在等待并发注册期间出现的新 scope 仍受 owner 校验。清理 gRPC 成功时，Service 已在事务中移除 scope 和 intent，Coordinator 直接消费该成功回执，移除成功后的 intent 读回。 [PostgreSQL 16 Read Committed](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED) · [PostgreSQL 16 数据修改 CTE](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-MODIFYING)

使用 PostgreSQL 16 的 `pg_stat_statements` 对照同一测试数据和单连接池。每种场景预热 10 次、测量 51 次，逐次确认意图已持久化。已有 Shuffle scope 时登记 SQL 从 4 条降到 2 条，P50 从 1.085 ms 降至 0.795 ms。尚无 scope 时，SQL 从 3 条降到 2 条，P50 从 1.133 ms 降至 0.963 ms。注册并发及 scope owner 不匹配均有真实 PostgreSQL 用例。计数只覆盖终态意图登记，服务端对象删除和其他终态持久化另计。

172 项定向测试和 4 项隔离 Native 集成测试通过，覆盖真实双 Worker Shuffle、JDBC 读完与取消、状态回执补发、PostgreSQL 锁下并发注册，以及清理池拒绝和退出竞态。真实并发注册用例从 `pg_stat_activity` 确认终态记录确实在等待 anchor，验证等待期间新出现的匹配 scope 被接受，错误 owner 被拒绝。

**批次 18 前的 superseded 基线：**Worker 的 `ConsumedResultSweep` 曾以 100 ms 间隔逐段扫描，每轮最多检查 64 个注册项。用实际 Java 调度器及注册表模拟同批已消费结果，512 个注册项从消费信号到释放回调的 P50 为 524 ms、最长 840 ms，2,048 个注册项的 P50 为 1.76 s、最长 3.32 s。这项单次测量以可控谓词模拟 Native 完成信号，覆盖轮询调度开销。该常规轮询已由批次 18 删除并改为事件驱动；当前 drain 仍必须保留 producer 已停、授权已撤、队列为空和 reader、导出 batch 已释放的联合证据，跨主机收齐真实 Worker receipt 的尾延迟归 B6。

实施批次 16 将 Remote Shuffle 的逐批预留与发布事务替换为有界写窗口。每窗口最多 128 个不同批次，写授权最长一秒，服务端提前 50 ms 停止发起 PUT。单个窗口仍为两笔事务，共执行 8 条 SQL。窗口发布检查当前 owner、终态围栏和确切 attempt，一次从 PostgreSQL 校验已有的批次、每分区的序号边界，并原子发布新的批次。Worker 复用每端点的写流，Service 在最后一个窗口持久化后，通过既有 gRPC 回执返回。`V10__write_window_reservations.sql` 删除闲置的预留状态列和相应部分索引。 [PostgreSQL 16 数据修改 CTE](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-MODIFYING) · [Amazon S3 条件写入](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

真实 gRPC 用例向 PostgreSQL 16 和 MinIO 写入并确认 32 批，`pg_stat_statements` 统计从逐批执行的 256 条降至 8 条。序号有空洞时整窗回滚。取消时先丢弃仍在内存中的不完整批次，已确认的未发布对象交给作用域清理。对未确认的 PUT 禁止 SDK 自动重试，保留精确写前缀的持久保护。终态回收以同一个数据库时钟截点判断所有预留，作用域内仍有活动写入时推迟记录 tombstone 和删除。批量窗口内每批仍对应一次对象 PUT，生产者提交还需核对对象完整性。

在相同本地 PostgreSQL 16 和 MinIO 环境中，使用同一份服务端写路径对比每窗口最多 1 批和 128 批。每批为 4 KiB，计时从接收完整批次到最后一个窗口提交。每轮校验持久批次行数、序号、大小和 SHA-256。1 批和 100 批各预热 6 次、测量 15 次。10,000 批各跑一次用于验证 SQL 扩展性。

| 批次数 | 单批窗口 SQL | 128 批窗口 SQL | 单批窗口耗时 | 128 批窗口耗时 |
|---|---:|---:|---:|---:|
| 1 | 8 | 8 | 6.05 ms P50 | 6.22 ms P50 |
| 100 | 800 | 8 | 372.02 ms P50 | 164.78 ms P50 |
| 10,000 | 80,000 | 632 | 41.05 s 单次 | 17.73 s 单次 |

测量包括对象 PUT，另行计量 gRPC 传输、生产者提交、消费者读取及查询端到端耗时。单批未观察到稳定收益，万批耗时需在远端存储、多生产者和混合负载下复测。

80 项定向测试和 4 项独立 Native 集成测试通过。真实 Worker 取消用例以对象实际落盘和持久预留作为同步点，验证取消后无批次发布或 producer commit，且已确认窗口的预留被回收。Activation Smoke 另观察到 5 次查询终态早于本地 dispatch 操作退出，当前 drain 通知经过至少 100 ms 的重试调度。实际任务、结果和租户槽位的释放延迟仍需独立测量和验收。

实施批次 15 将 Remote Shuffle 消费路径的批次清单移出 PostgreSQL 分页查询。finalize 按输出分区、producer 和源批次顺序遍历一次已胜出清单，页查询同时限定 owner 与 metadata version。提交在同一事务保存分区回执和索引根，删除 `shuffle_batch.output_sequence` 及逐行排序回写。已发布 finalize 重放直接返回保存的回执。每分区最多 256 批时，索引随建流回执一次读出。更大分区使用固定宽度、按序号寻址的对象页，读取前逐层核对 SHA-256。对象 GET 按回执中的长度接收，超长或损坏数据在发送批次前失败。清单页 PUT 复用持久写入预留和迟到对象清扫。生产 gRPC 与 Native 分区摘要格式保持一致。 [Apache Uniffle 数据和索引布局](https://uniffle.apache.org/docs/intro/#shuffle-file-format) · [Amazon S3 条件写入](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

真实 PostgreSQL 16 与 MinIO 用例覆盖 17 个输出分区、两处非空分区和 262 个批次。发布执行一次有界胜出清单查询，重放没有读取批次表。服务重启后的续读无需批次元数据 SQL，许可续期仍独立计量。单元用例覆盖跨 262,144 批的目录边界、随机定位和损坏页拒绝。

在隔离 PostgreSQL 16 和 MinIO 中回放同一批次清单，每规模预热 6 次，测量 21 次。使用旧版索引与分页 SQL 对照，逐批确认输出序号、长度与行数相同。初始 256 条/对象页方案在 10,000 批时执行 40 次额外 GET，清单扫描 P50 为 47.90 ms，高于同轮 SQL 对照的 9.18 ms。最终保留 256 条内联阈值，将对象页扩大为 4,096 条，每次最多解码 256 条。

| 分区批次 | 旧分页 SQL/流 | 候选分页 SQL/流 | 候选清单 GET/流 | 重测旧 P50 | 重测候选 P50 |
|---|---:|---:|---:|---:|---:|
| 1 | 1 | 0 | 0 | 162.8 µs | 12.5 µs |
| 100 | 1 | 0 | 0 | 246.4 µs | 48.8 µs |
| 10,000 | 10 | 0 | 3 | 10.58 ms | 9.20 ms |

计量窗口是建流后的索引扫描。建流、权限续期、批次 payload 下载以及 SQL 查询端到端耗时分别验证。读流打开时为索引及解码窗口取得预算，取消和结束均归还此预算。预算不足时先释放数据库 pin，再返回资源不足。单进程预算取 64 MiB 与 `max-inflight-bytes` 中较小者。远端存储及规模混合负载下的首批延迟与吞吐仍需对照。

76 项定向测试、4 项隔离 Native 集成测试和 1 项发布版本栅栏回归通过。真实 PostgreSQL 与 MinIO 覆盖服务重启后的 gRPC 续读、损坏和超长对象拒绝、索引额度拒绝及 pin 归还、重复发布、活跃清单上传保护和不确定 PUT 清扫。胜出版本变化后，旧快照既不能取得清单写入预留，也不能发布回执。

实施批次 14 处理复用规划表绑定时的两次表所有权读取。旧路径先按名称找到 reservation 指向的记录，再按 table ID 读取同一表。现在复用通过 reservation 一次读取活动记录，使用同一份数据校验 table ID、命名空间、物理身份和活动状态，并保留目录部署校验。无 owner 的可观测表仍按名称确认期间未发生接管。

使用真实 Paimon SDK、生产绑定实现及 PostgreSQL 16 日志做同源对照。先完成表绑定，再复用同一绑定 16 次。表所有权 SELECT 由 32 条降至 16 条，冻结的三行快照及绑定对象保持一致。77 项规划、目录与所有权回归通过，覆盖采用、重新采用、重命名、快照推进、Time Travel 及 DDL。

实施批次 13 将 Remote Shuffle 建流收敛到一个事务。服务先核验签名任务身份，再校验 anchor 与 scope，精确读取当前分区的终帧证据并签发 pin 和窗口。删除了建流前两次完整分配查询和整份 Shuffle 分区摘要读取。scope 使用共享锁，清理封口仍与活跃 I/O 的签发互斥。接管回归发现封口后旧的 `ACTIVE` 限定会阻碍新的 owner 清理。修正后，已取得 live anchor 的后继可以接管封口域，续期清理票据，等待旧写入排空后删除对象。读写准入保持关闭。

真实 PostgreSQL 16 建流用例按当前数据库的顶层 SQL 模板计数，成功建流为 5 条语句，历史路径按源码为 10 条。PostgreSQL 仍用于有界许可、持久对象保护和分区元数据分页。大型分区的不可变对象清单仍需补齐，以达到读流稳态零同步 SQL 的发布门槛。

批次 12 和 13 一起通过 57 项定向测试，其中 22 项是真实远端 Shuffle 字节路径，3 项是真实整链路启动。另有 4 项隔离的 Native 和远端 Shuffle 集成测试通过。服务用例使用真实 PostgreSQL 16、MinIO 和 mTLS。

实施批次 12 的 Remote Shuffle 先在本地核验任务 capability，再登记持久写入意图，在事务外执行对象 PUT，最后在带栅栏的事务内发布批次并消费意图。删除批前独立的分配及 owner 读取。写入持有共享 scope 锁，各 allocation 独立串行化自身写入，清理状态更新与共享锁互斥。发布检查序号、重复内容及 winner。只有 winner 变化或 finalize 推进 committed metadata version。真实 PostgreSQL 16 和 MinIO 的 32 批生产入口观察到 256 条 SQL，合计每批 8 条。批次 7 的旧实现对相同批次数量仅登记就执行 640 条 SQL，外层分配与 owner 读取另计。真实锁等待用例确认同一查询的两个阶段可并行写入。回归覆盖精确重放、同序号不同内容、序号空洞、已胜出 attempt 晚写和清理等待。 [PostgreSQL 16 数据修改 CTE](https://www.postgresql.org/docs/16/queries-with.html#QUERIES-WITH-MODIFYING)

gRPC 单块输入直接复用已校验的 Protobuf payload 和摘要。多块输入按已收到的字节分段组装，摘要遍历只读视图，S3 每次上传打开独立输入流。Corretto 17 本地对照包含组装、摘要和使用 8 KiB 缓冲读取整个上传流，每组预热 48 次，验证两端上传字节一致。1 MiB 单块的 192 个样本中，P50 从 9.46 ms 降至 4.64 ms，调用线程分配从 5.25 MB 降至约 5.4 KB。8 MiB 八块的 96 个样本中，P50 从 77.65 ms 降至 74.38 ms，调用线程分配从 49.29 MB 降至 11.98 MB。这组计量覆盖本地入站组装至上传流消费。对象网络、数据库与跨引擎查询另行计量。 [Protobuf ByteString](https://protobuf.dev/reference/java/api-docs/com/google/protobuf/ByteString.html)

实施批次 11 移除了 Remote Shuffle 每批读取的 PostgreSQL 权限查询。在建流和窗口续期事务中校验 anchor、scope、pin 和终态清理意图，签发最长一秒的读取窗口。读取服务在对象 GET 和提交每个 gRPC 帧前校验本地窗口，采用单调时间和墙钟经过时间中先到的截止，并预留 50 ms 时钟裕量。早退的 anchor 可以撤销新许可，旧窗口仍在数据库中约束直接取得和批量接管。清理遇到活跃 I/O 时先提交关闭准入的状态，再等待持久 pin 和写入排空。终帧在许可内提交，RPC 完成前归还 pin。数据顺序由 finalized output sequence 校验，每页以单条索引查询读取对象键、长度、行数和摘要。空分区和已达终点的读取直接生成终帧。

真实 PostgreSQL 16 与 MinIO 用例在建流前后用 `pg_stat_statements` 观察到一次签发，随后窗口内的 128 次权限检查增加 0 次签发调用。之前的 128 次逐批检查产生 384 条 SELECT。范围摘要由原始字节区间直接计算。使用 1 MiB 块的本地 JDK 探针实测每帧调用线程分配由约 2.10 MB 降至 1.05 MB，摘要一致。本组耗时未显示稳定下降。对象清单移出 PostgreSQL、写入侧逐批意图事务减少以及规模负载下的接管和回收尾延迟仍待完成。 [PostgreSQL 16 行锁](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)

67 项定向 Maven 用例通过，覆盖真实 PostgreSQL 16 和 MinIO 的撤销、让渡、续期、清理与损坏检测，以及远端 Shuffle 和整链路启动。最终单条自动提交分页查询再次通过 20 项 Shuffle 服务用例。使用实际 Native、mTLS、PostgreSQL 与 MinIO 的 4 项隔离集成用例通过，覆盖多块读取、终帧截断、分区摘要不匹配和读取取消。

实施批次 10 以 Coordinator 所有者的单份状态快照提供版本等待。检查版本与登记观察者在同一同步边界完成，状态、失败、结果端点及实际释放标记按各自事件推进。HTTP 挂起时归还请求线程，响应由 Jetty 调度。SDK 将等待与取消绑定到受理查询的配置端点，CLI、JDBC 和 TPC Java 客户端移除了固定间隔等待。跨所有者接管、负载均衡器后的路由恢复及跨节点持久幂等仍需验收。

两个真实 Coordinator 共享 PostgreSQL 16 journal 对照中，16 次从另一节点发起的诊断 GET 借用 16 次 journal 连接。随后 16 次所有者版本等待和 16 次发向另一节点的版本请求新增 0 次借用。另一节点对这组版本请求返回明确的所有者不可用。真实 Jetty 回环实验在初次状态读取后 40 ms 发布结果端点，分别测量旧 JDBC 首次 200 ms 轮询和所有者版本等待。各 16 个样本中，端点发布到客户端观察的中位数由 163.1 ms 降到 1.23 ms。该对照的指标是状态传播延迟。189 项 Maven 定向测试通过，其中真实 JDBC、打包 CLI、启动、生命周期及提交幂等 E2E 共 22 项。 [Jersey 异步处理](https://eclipse-ee4j.github.io/jersey.github.io/documentation/latest3x/user-guide.html#async)

实施批次 9 将 Java SDK、JDBC、CLI 和 TPC 客户端切换到同一个 Flight 游标，删除整流和逐批 IPC 回写接口。JDBC 在首次读取和 EOF 间持有查询身份，在 EOF 核验 Coordinator 终态，提前关闭时取消自己的未终态查询。CLI 每 128 行刷新表格，并在终态成功后打印最后一组及总行数。VECTOR 顶层声明按 schema 解码，坐标直接从浮点子向量构造 Java 数组。本地真实 Flight 回环对照使用相同的 48 批、98,304 行和 9 MiB 文本负载，确认消费内容一致。两个 JVM 分别预热 8 次，测量 16 次。客户端中位耗时由 81.1 ms 降到 26.6 ms，调用线程每查询的中位分配量由 81.9 MB 降到 25.9 MB。该测量覆盖本地 Flight 和调用线程分配，线上 P95、全链路混合负载及 SF100/SF1000 独立验收。Corretto 17 定向验证 269 项通过，其中真实 Coordinator 和 Native Worker 的 JDBC E2E 6 项。12 MiB 的多批结果在 8 MiB 的客户端 Arrow 分配上限内按序读取。真实 Flight 提前关闭取消了调用，HTTP 与 Flight 联合用例验证了 EOF 后终态失败和部分读取取消的查询身份。Native 结果、所有权和 Flight 启停测试 22 项通过。重新打包当前 CLI 后，7 项真实 E2E 通过，涵盖进程读取 Native TPC-H Q1、成功回执和鉴权失败。TPC Java 客户端以当前 CLI artifact 的类路径编译通过，基准工具 113 项测试通过。 [Arrow Flight 19](https://arrow.apache.org/docs/19.0/format/Flight.html)

实施批次 8 将 SQL 分派中的 AST 解析入口由两次收敛为一次。SHOW 元数据和注册的语句工厂使用同一 AST，删除 `StatementDispatcher` 的独立编译器引用和刷新入口。关系查询继续进入认证后的编译阶段。81 项定向测试通过，涵盖分派、授权、Feature View、Semantic View 和写入路由。重复同一 Join SQL 的本地稳态对照中，解析入口耗时组中位数从 5.6 µs 降到 2.9 µs，线程分配量从约 2,944 B 降到 1,472 B。该数据覆盖分派阶段的 AST 调用，后续关系编译内的解析单独验证。

最终源码通过 15 项端到端测试，涵盖 Iceberg SHOW 元数据与系统表等价性、Paimon Time Travel、真实双 Worker Shuffle 和 Activation Smoke。

实施批次 7 将 `POST /v1/task/status-report` 和回执改为二进制 Protobuf。运行中报告携带完整已签发 attempt。终态使用原始 canonical `TaskReport` 字节承载状态、覆盖、错误、资源及 Native 统计。Worker 和 Coordinator 复用已解析的终态，写入仅附加连接器提交证据。ACK 同时关联 task ID 和序号。补发保持相同字节和序号，同一序号修改统计或控制 epoch 会被拒绝，结果通知和物理释放回执保持独立判定。

基于带 512 条算子统计的终态报告，用原 JSON DTO 对比二进制编解码，各取五组每组 250 次。请求体从 178,479 B 降到 33,226 B，耗时组中位数从 493 µs 降到 72 µs，线程分配量从约 1.08 MB 降到 0.30 MB。两条路径得到相同的 canonical 终态与算子统计。该样本测量报告编解码。Lance FFI 和 Native 重建后，32 项 Native 定向测试通过。

128 项定向 Maven 用例通过，包含 12 项真实 HTTP 投递、丢 ACK 重发、同序号改写拒绝和授权验证。覆盖真实双 Worker Shuffle、SQL 写入、COPY、物理释放与 Activation Smoke。批次 7 的冻结运行产物另外验证了独立 Coordinator 与 Worker 进程中的 7 项 Kafka CDC 和故障恢复场景，以及 1 项 JDBC 与 CLI 取消和物理释放场景。这 8 项计入上述 128 项。

另在真实 PostgreSQL 16 用 `pg_stat_statements` 测量远端 Shuffle 已签发读 pin 的 `requirePinnedRead`。1 次检查执行 3 条 SELECT，128 次执行 384 条。每次包含 `execution_anchor FOR SHARE`、`shuffle_query_scope FOR UPDATE` 和 `shuffle_read_pin`。生产 `readPage` 与逐批 `readPayload` 均进入该检查。计数仅覆盖 pin 校验，分页读元数据和对象 GET 另计。

同一 PostgreSQL 版本验证了远端 Shuffle 写入的 `beginWriteIntent` 与 `completeWriteIntent`。每个正常新批次执行 14 条 SELECT 及 6 条 INSERT、UPDATE、DELETE。连续 32 批得到 448 条 SELECT 和 192 条写语句。该计数覆盖数据库批次登记，外层分配查询和对象 PUT 另计。

实施批次 6 将 `POST /v1/task` 的请求和 ACK 切换为二进制 Protobuf，移除了对应的 JSON 与 Base64 信封及旧 ACK DTO。普通任务直接嵌入 canonical `TaskRequest`。COPY 在 mTLS 下携带独立票据和原始任务字节，Native 描述符闭包保持独立。Shuffle ACK 复用二进制分区票据。Coordinator 一次验证分区顺序，消费侧按分区编号定位，移除了 JSON 票据 DTO 和逐次线性查找。

同一 256 KiB 计划的本地编解码对照各取五组每组 300 次。请求体从 349,565 B 降到 262,154 B，编解码耗时的组中位数从 1.03 ms 降到 22.1 µs，Java 分配量从约 3.84 MB 降到 0.53 MB。两种编码解出的任务一致。测量范围是请求编解码。131 项定向 Maven 用例和 12 项 Native Exchange CTest 通过，覆盖旧媒体类型拒绝、错身份或乱序分区票据拒绝、完全相同的 epoch 重发、COPY、实际取消释放和双 Worker 执行。任务报告二进制切换、ACK 就绪与排空事实分离，以及 SF100/SF1000 对标继续按照实施计划验收。

新增 HTTP IDL 后重新生成了 Lance FFI 构建来源凭据，并重新构建和打包 Native。打包前后 Native 文件逐字节一致，使用重建产物再次运行上述 Native Exchange 和 13 项端到端用例，全部通过。

实施批次 5 将常规 JNI 完成检查收敛为读取 Native 生命周期原子状态。轮询跳过 Velox 算子统计、split 覆盖聚合和 Protobuf 报告编解码。失败保留 Native 原错误，终态和释放保留完整执行证据。真实 Native 待启动任务的本地对照中，五组各 15,000 次调用的中位数由每次 3.71 µs 降到 33 ns。Java 线程分配量由每次 1,968 B 降到 0 B。该样本仅测 JNI 查询与状态读取。

重新构建并打包 Native 后，50 项定向 CTest 和 48 项 Maven 测试通过，涵盖提交与取消竞态、真实双 Worker Shuffle、COPY 和 Activation Smoke。竞态用例对 `202` 等待实际释放，结论仍以 `200` 携带的 canonical drain receipt 为准。Java 声明与 Native 导出的 22 个 JNI 方法一致。

实施批次 4 移除了 Coordinator 构造任务信封时的 Protobuf 编码、Base64 往返与重新解析。Worker 在 HTTP 入口解析一次 canonical task，JVM 执行器直接读取已解析对象，Native 执行器应用 Worker 本地绑定后在 JNI 边界编码一次。调度描述复用已有的 `ByteString` 和 `DataSplit`。COPY 继续校验票据与精确任务字节的绑定。

61 项定向测试通过，包含真实双 Worker Shuffle、COPY HTTP 私有票据、Native 创建回滚和取消并发。基于 Corretto 17 对修改前后的实际 JSON 封装做本地对照，各取五组每组 200 次的中位数。256 KiB 计划的封装耗时由约 1.25 ms 降到 0.57 ms，线程分配量由约 4.32 MB/次降到 1.61 MB/次，HTTP 输出仍逐字节相同。该测量覆盖 Coordinator 请求封装。二进制 HTTP 结果见批次 6。

实施批次 3 让生成列读取复用已准入的 schema 与快照，移除了 Stage 绑定时按表名重新读取目录的路径。规划继续校验当前 owner，并复用连接器提供的物理表标识。Iceberg JDBC 新加载表时读取一次当前指针。绑定数据扫描共享同一次目录观察完成快照验证和 split 枚举。

真实 PostgreSQL 16 的 `pg_stat_statements` 对照确认，跨连接更新 schema 后重新解析一张 JDBC 表的指针 SELECT 从 2 次降到 1 次，且仍得到新 schema。已绑定的非空快照在验证与枚举 split 时只产生 1 次指针 SELECT。真实双 Worker 三表 Join 与全局 Top-K 复测通过。SDK 表加载从 36 次降到 24 次，首个 Stage 派发前从 28 次降到 22 次。两组计数来自不同实验，端到端日志的 SDK 次数用于定位剩余调用，PostgreSQL 次数只覆盖定向 JDBC 阶段。

补充验证覆盖 SDK 缓存包装器中已过期的快照、当前 owner 变化、历史声明和快照 pin 生命周期。修正本地 Maven 进程的 UTF-8 locale 后，最终代码的 Iceberg 包内 301 项测试和独立双 Worker E2E 通过。

Iceberg 在读绑定阶段验证历史声明及其相邻版本文件，分片枚举继续复核当前表与已选快照，直接从快照的 schema ID 取物理列。含真实 Parquet 文件、列重命名和后续元数据提交的 SDK 对照中，绑定读取 5 份历史元数据，分片阶段的历史元数据读取从 5 次降到 0 次。分片仍携带原快照 ID。Iceberg 模块 416 项测试和 macOS arm64 角色包 43 项端到端测试通过，覆盖实际时间旅行、分支和标签、双 Worker Shuffle 与 PostgreSQL 查询预算。739 项角色清单 SHA-256 为 `3c31c4c2323e1d8383ded1f9c4b204a423ac6c4d0221d6f5da58c64b7571b9e3`。

精确命名的 Iceberg JDBC 分支和标签复用一次表加载来选定引用和绑定读快照。同一语句的重复别名复用该引用解析。以主分支 `86f403861` 为基线，两套角色包在实际 PostgreSQL 和 Flight 上执行同一标签查询并返回历史行 42。目录指针读取从 3 次降到 2 次，命名空间枚举从 2 次降到 0，表枚举从 1 次降到 0。两次指针读取分别服务于精确名称发现和分片前的快照核验。26 项定向测试与 29 项端到端测试通过，覆盖同表别名、大小写回退、缺失标签、Iceberg 分支和 Paimon 标签。在测试模块对齐 Commons IO 版本后，Testcontainers 能将 TLS 文件复制到测试容器。

实施批次 2 移除了 split pull HTTP 路由、Worker 客户端、配置和租约回收器。Worker 拓扑直接更新缓存亲和放置。真实 Coordinator 上两个旧 POST 路由都返回 404，放置、启动、取消和 Shuffle 回收定向测试通过。实时 Stage 在签发前校验整组 split，之后复用已解析的 Protobuf 与冻结的 Substrait 字节和摘要。持久任务使用从已认领分区解析出的同一份 split 绑定产物和构造请求。任务封装与绑定的 51 项定向用例、真实独立双 Worker 的 Join 和全局 Top-K E2E、基准工具的 113 项测试均通过。SF100/SF1000 仍需专用环境中的跨引擎测量。

双 Worker 用例的读查询涉及 3 张 Iceberg 表。首个 Stage 派发前，日志中有 28 次 Iceberg SDK 加载表调用，整个读查询窗口有 36 次，每表 12 次。调用日志暂未给出实际 PostgreSQL SELECT 次数，下一轮用连接级 SQL 计数确定哪些加载跨过了存储边界，并从绑定、快照、统计与 split 枚举逐项归因。

实施批次 1 已交付 journal 新事件及 lineage outbox 零读回、已提交写回执复用、按物理扫描字节放置 split，以及移除未接入生产的 Java Worker 结果缓冲和报告重试队列。真实 PostgreSQL 16 的计数用例验证新 query/task 事件零读回、两个重复事件两次精确读回。Java 17 定向验证 66 项通过，覆盖协议描述符、分片、写入幂等、Native 结果传输的 Java 生命周期和生产报告发送路径。刷新了当前 Native exporter 生成的能力目录后，写入幂等用例通过。

10 万 split 的本地对照中，线性和堆分配的结果逐分区完全一致。128 分区的单次分配中位数约为 50.25 ms 与 35.42 ms，1024 分区约为 387.10 ms 与 51.41 ms。该实验仅测分配算法。启动、客户端状态与结果、远端 Shuffle、动态过滤、物理计划和完整故障演练仍需按实施计划交付。SF100 和 SF1000 的跨引擎对标仍是首发硬门槛。

本次校准复核生产入口、配置、源码调用和依赖。使用真实 PostgreSQL 16 隔离容器复现新行 RETURNING、并发冲突的 CTE 快照与下一条 SQL 的可见性，以及同 scope 排他行锁等待与异 scope 并行。实验使用最小表模型，整条链路上的数据库耗时与计数仍由发布用例归因。此前隔离源码副本用 Corretto 17 和 Python 3.12 跑定向 Maven reactor，195 项通过，其中真实 PostgreSQL journal 19 项。首轮有 33 项因已提交的 Native 能力清单和当前工作树 proto 描述符不匹配而在测试初始化报错。仅在隔离副本使用本地 Native exporter 刷新清单后，全组选定测试通过；工作树中的 fixture 无改动。另跑 94 项客户端幂等、CLI/JDBC、SQL 规划、真实 PostgreSQL 行策略、Iceberg 分片、Shuffle 框架和控制面结果测试，全部通过。33 项本地 Native 二进制中的结果、Shuffle、所有权和取消相关测试通过。此前的 JDK 确定性探针与分片微测见 [既有方案](./performance-remediation-plan.md#本轮验证)。新实现的数据库计数、协议成本、完整多进程 E2E 与吞吐对比属于各阶段验收。

每个阶段使用两类证据签核。源码证据包含入口、运行所有者、调用方、持久化或解码位置。动态证据包含已执行的用例、实际输入与观测量。优先使用下面的验收夹具，替换已删除实验路径的测试前先在真实入口补齐原约束。

| 边界 | 用于签核的现有测试和观察 |
|---|---|
| 接入、准入、授权与规划 | `QuerySubmissionIdempotencyE2ETest`、`QueryAdmissionOwnershipTest`、`TimeTravelExtractorTest`、`DmlCopyOnWritePolicyE2ETest`。查看不同身份/快照的绑定、终态和取消事件 |
| 计划、放置、签发、入参 | `StageSplitFetcherTest`、`DefaultWorkerSelectorMemoryFitTest`、`SubmitTaskRequestSymmetryTest`、`CanonicalProtocolDescriptorsTest`、`TaskAdmissionRegistryTest`。验证 split 覆盖、通道字节、精确 attempt 与并发重复提交 |
| Worker、DAG 和终态 | `DAGDispatchEngineCancelFuturesTest`、`DAGDispatchEngineStageRetryTest`、`TaskStatusReceiverWriteCommitTest`。验证已成功、失败、迟到、取消和重启分别能达到的终态 |
| 数据交换、输出和释放 | `NativeExchangeTransportExternalIntegrationTest`、`FlightResultFetcherMutualTlsTest`、`ResultBatchStreamReaderTest`、`ResultOwnershipStateTest`、`WorkerTaskRetentionWiringTest`。真实数据、授权、EOF、租约、异常及进程实际释放 |
| 持久化写入与回放 | `JdbcQueryStateJournalIntegrationTest`、具体 connector 的 writer/commit E2E、`LiveTwoWorkerCdcAcceptanceTest`。测试 PostgreSQL 统计、快照一致、已保管但未 ACK、未知结果与源 offset |
