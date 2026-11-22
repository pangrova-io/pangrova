---
title: "Pangrova 执行链路一次性重构与清理计划"
description: "以同版本发布为前提，对 SQL 主链路逐段验证并收敛为唯一生产路径"
unlisted: true
---

# Pangrova 执行链路一次性重构与清理计划

项目可以使用同版本 Coordinator、Worker、Native bundle、Shuffle Service、CLI、JDBC 和 benchmark 一起切换。删除只为过渡服务的协议、开关、路由和后台任务。上一轮的兼容入口、混部、双格式和迁移安排由本计划替代。底线仍是授权、快照一致性、精确 attempt、可恢复提交与资源实际归还。读 SQL、写 SQL、DDL 和 Java 执行的 CDC 是不同业务路径，各自应有明确入口和共同的任务生命周期。

待实施工作围绕扫描输入、持久 Shuffle、回调隔离和真实负载验证。已验证的协议收敛、数据库去重和所有权修复集中记入[验证记录](#验证记录与覆盖边界)。 原始调用链和基线见[逐节点协议与数据库审查](./end-to-end-protocol-optimization.md)及[微测与线程分析](./performance-remediation-plan.md)。

查询性能对齐的对标引擎、扫描权重反例、跨 Stage 动态过滤、阶段聚合缺口和性能验收，见 [查询性能对齐与执行引擎修正计划](./query-performance-parity-plan.md)。

## 证据口径

- **已验证调用链**表示能从 HTTP/SQL 接入、生产组装到调用点闭合，或已检查引用、注册和配置并确认有一侧断开。删除前还要通过全仓编译及真实入口，防止遗漏 SPI、反射注册或外部部署脚本。
- **验证了机制**表示由实际代码、JDK/Native 测试或既有实验支持。事务数、编码次数和等待边是机制证据，生产占比和目标收益由同源负载实验评估。
- **待测再设计**表示证据仅证明存在某种执行方式，如单端点、内存 spill 或排序。目标状态先定正确的协议和度量，不引入新的存储格式、传输模式或调度分支。

## 待实施与待验收

| 环节 | 剩余工作 | 验收证据 |
|---|---|---|
| 客户端、准入和状态 | 持久客户端和固定到达率的混合压测，验证认证状态投影、短查询、长查询与实际归还租户槽位 | 每条路径的首批、P95、超时、拒绝、耗尽恢复和零正常状态 SELECT |
| 快照和 split 枚举 | 跨 Stage build 证明驱动组装前剪裁。大文件按合法范围拆分，小文件按工作量组包。有界枚举与实际读放大按连接器验证 | 冻结相同快照与授权。比较 split 数、实际对象 GET 和读取字节，覆盖缺失、晚到、失败、删除文件、快照演进和取消 |
| 计划、分配和控制面 | 量化下游准备隔离在固定到达率、受阻目录 I/O 与写入混合负载下的 P95。完成可执行 AQE 的源分区映射，冻结每个消费者绑定 | 基线和候选完成同一完整结果。检查覆盖、滞后、重试、投机获胜和 durable replay |
| 远端 Shuffle | 验证同总字节不同 batch 数与热点 scope 下的事务、锁等待、吞吐和尾时延。按证据决定段存储或进一步的复制优化 | PostgreSQL 和对象存储真实压测，覆盖权限撤销、owner 切换、背压、未知 PUT、丢 ACK 和 GC |
| Velox 与结果 | 对剩余 I/O、解码、spill、缓存和单出口成本做归因。阅读、断连和取消用真实 Flight 客户端验证 | 冷热、偏斜和宽/窄负载。若出口瓶颈成立，验证并行消费的顺序、EOF、资源窗口和真实归还 |
| 写入与提交 | 用高并发和故障窗口校准文件大小、写放大、提交时延及各格式恢复。原子发布和跨引擎结果独立验收 | Iceberg/Paimon/Lance/COPY/CDC 保留快照、offset、精确 fence 和资源归还证据 |
| 发布门槛 | 同硬件、同快照对比 Trino 和 Spark 的 SF100/SF1000，并执行 Linux Native 完整链路回归 | 同源产物、结果摘要、物理 Profile、全量可复核的吞吐和延迟，以及限定环境记录 |

## 固定三条稳态零读库门槛

1. **状态订阅和诊断读取。** 活跃 query 由 owner 的认证状态投影提供。正常状态读取与客户端重连在投影窗口内产生零 journal SELECT。只在接管、冷启动重放、明确的历史查询读取数据库。以两个 Coordinator 和大量同 query 的客户端断言次数，覆盖 Ready endpoint 和错误细节。
2. **持久 journal。** 正常新事件一次 INSERT/RETURNING 或等价的单次 SQL。独立 SELECT 为零。事务、lineage 原子性和最终 ACK 所代表的 durability 保持一致。对已提交写的状态判断只读取一次，未知提交仍从权威证据核对。
3. **Shuffle steady state。** 只在建流、续租、最终关闭与恢复触及读租约数据库。manifest 是由已提交 attempt 发布的不可变数据。每 batch 禁止同步数据库操作，同时本地核验 scope/epoch、租约有效期、撤销与 byte credit；已有在途读取保有精确保护时间。测试每 batch/每 chunk 的 SQL 计数和撤销后新数据是否可见。

## 校准后的设计决策

### 短查询和大查询采用各自的最短生产路径

**控制面先改等待方式。** 用现有 HTTP 提供基于状态版本的异步有界等待，每个客户端同一查询最多一个 waiter。状态版本与 waiter 登记必须原子衔接，再检查版本，防止状态恰在登记期间变化而丢唤醒。立即结果走同一响应，取消/超时注销等待，按 principal 和 tenant 限制并发与时长。首版由 owner 提供状态，peer 通过已有 owner 信息定向请求。跨节点流转只更新轻量视图，未证明有必要之前无需全局事件总线或常驻流连接。Ready、Terminal 和 Drained 可独立出现，快速任务和无结果集也有完整响应。Trino 的 `nextUri` 与服务端有限 `waitForResults` 支持这种请求形态。Pangrova 继续用 Flight 传数据。 [Trino 客户端协议](https://trino.io/docs/current/develop/client-protocol.html) · [Trino 实现](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/server/protocol/ExecutingStatementResource.java)

**跨 owner 幂等先做合同。** 当前 `IdempotencyKeyRegistry` 是进程内 map，key 由 principal 与请求幂等键组成。owner 路由可以降低重复访问，但响应已丢、owner 已退出时，路由不能证明该 SQL 从未受理。若支持跨节点重复提交，必须在一次受理提交中保留可恢复的授权作用域、键、请求摘要与 queryId。优先与已有 durable 受理事实一起持久化，由权威唯一约束选出原请求。正常只走受理写入，冲突/未知结果再读取验证。取到已受理回执也不触发第二次 DML。没有配置跨节点持久受理时，在该部署合同内提供受理 owner 范围的幂等。验收分别注入响应丢失、崩溃与接管。

**控制协议和调度分开验收。** 保留现有 HTTP 服务设施，以一个 canonical 二进制消息替换 task/report 的 JSON+Base64 镜像，可独立测 body bytes、解析次数和 CPU/attempt。避免把新 gRPC 服务、共享计划缓存、重新编排 ACK 同时设成依赖。销毁任务与取消不能靠 HTTP 是否返回判断。Worker 快速接纳和真正端点 Ready 若需分开，以精确 attempt 和当前 generation 表达。对于不产生端点的任务无需额外 Ready RPC。DAG callback 只做小型状态发布，用查询顺序和 admission 约束交接给有界工作池，阶段准备与 durable commit 各有容量。用阻塞目录、慢对象存储、终态报告压力和关闭观察无关查询 P95、队列和 resource drain。

**小查询保留流水直连。** 持久交换支持跨 Worker 重试或 durable replay，但消耗写入、发布、存储和再次读取。Trino 也明确指出 TASK 重试适合大批查询，可能增加高频短查询延迟。按明确的恢复要求或实测负载选择路径。在同一查询中按 stage 边的依赖语义选定读取方式，已签发边保持稳定。将两种执行策略作为真实能力管理。 [Trino Fault-tolerant execution](https://trino.io/docs/current/admin/fault-tolerant-execution.html#task)

### PostgreSQL 热路径与撤权边界

**journal 保持冲突可判定。** 用 PostgreSQL 16 隔离容器和两个连接复现了新行 `RETURNING` 可验证 JSONB 内容、并发冲突的单条 CTE 返回零行、后续语句能看到已提交赢家。由此选择 `INSERT ... ON CONFLICT DO NOTHING RETURNING` 或可证明等价的写入结果。新行一次往返，零独立 SELECT。冲突分支按当前事务隔离和提交证据精确读取并比较。同键异 payload 判定冲突。拒绝用同快照 CTE 的空结果证明事件不存在，也避免靠无语义 UPDATE 换取 RETURNING。涉及 lineage 的写入、事务提交和 ACK 次序独立保留。用真实 schema 验证 JSON/NULL/时间/序列化同值、同键竞争、回滚及已提交未 ACK。 [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html) · [隔离级别](https://www.postgresql.org/docs/16/transaction-iso.html)

**Remote Shuffle 消费端去掉逐 batch 锁定查询。** 源码执行 `requirePinnedRead` 的事务包含 anchor、scope `FOR UPDATE` 和 pin 查询。双连接试验中同 scope 的 `FOR UPDATE` 等待，另一 scope 可继续。试验确认行锁机制和隔离范围。整条 Shuffle lock-wait 的占比须由实际并发读测得。记录每分区的建流、pin、metadata page、GET、续租、关闭和 SQL 等待，分别汇报操作次数和锁持有时间。 [PostgreSQL 16 行锁](https://www.postgresql.org/docs/16/explicit-locking.html#LOCKING-ROWS)

**读取许可和对象保护设计成两个时钟。** 建流由权威事务取得绑定 producer winner、owner epoch、消费 attempt、分区、序号区间和 manifest digest 的读许可。许可截止取请求、capability、execution authority 与自己的有效窗口的最小边界，明确时钟误差与对象 API deadline。发布 EOF/ACK 前完成长度、序号、摘要和所有权验证。同一读者的状态机负责禁止发起新 GET/发帧、等待现有所有者退出和结束 pin。GC 继续使用持久保护记录，保护时间覆盖已经发出的 I/O，重启后根据持久记录安全恢复。取消/接管必须先阻止新许可，向仍有许可的服务完成带版本的撤销屏障。确认旧 epoch 无权再发新字节之后才宣告切换或执行删除。旧进程不可达时，等待证明其许可耗尽并留足 I/O 保护窗口，期间向调用方报告仍在收敛。若现有合同要求更早的完成通知，原型先证明可达。单靠进程内 TTL 或异步通知无法通过验收。

**按实测选择写入粒度。** 读租约和不可变清单可独立测量。首轮原型不要求改变对象打包方式，成功后数据面按清单取帧。只有固定字节下 PUT/GET 次数、索引体积或写锁成为显著成本，才决定改成有界 segment。发布时仍原子绑定连续序号、长度、digest 和 winner receipt。写入仍按原 proof、fence 与未知提交状态验证。每次实验只引入一个变量。目标部署只运行已选定的一种对象格式。 Uniffle 为同分区的块独立保存数据文件和索引，提供了合并对象与保留块级验证的设计对照。 [Apache Uniffle 文件格式](https://uniffle.apache.org/docs/intro/#shuffle-file-format)

### 扫描、AQE、结果和写入的适用门槛

- **扫描与 split。** 原有 10 万分片微测证明最小堆适合高扇出，小并行度可能更慢。先用实际 range bytes、对象数、delete/解码估计和 backlog 定任务尺寸，再从生产分布决定是否换分配算法。总 split 有界准备和已签发 task descriptor/重试状态使用各自的容量指标。不能为减少枚举内存而提前签发需要固定 cohort 的 stage。需要先证明完整覆盖的模式仍按固定快照生成清单。用Iceberg manifest 剪枝和现有 Velox pushdown 先减少实际输入字节。 [Iceberg 扫描规划](https://iceberg.apache.org/spec/#scan-planning) · [Trino task sizing](https://trino.io/docs/current/admin/fault-tolerant-execution.html#task-sizing)
- **真实 AQE。** 派发保持原计划的分区坐标，`ExchangeSourceBuilder` 按 consumer partition ID 取 producer 分区或 receipt。候选 AQE 决策只用于观测。先以已提交 producer 输出生成、冻结、校验读取映射。用不同 join 类型、NULL、重复键、倾斜、重试和 durable replay 证明完整覆盖、去重和成本上界，再比较关键路径。Spark AQE 的参考点是使用运行时输出统计与不同读取划分。它的默认阈值不直接作为 Pangrova 门槛。 [Spark SQL AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
- **结果出口。** 首先保持单 endpoint 的 SQL 行为，移除客户端 IPC 回转。Arrow Java 当前使用 19.x，依据该版本的 DoGet、batch 生命周期及顺序。限额按 bytes 同时覆盖服务端输出、客户端 Arrow allocator、消费解码和传输队列。先跑慢消费者、客户端提前关闭、读完后终态失败、分区数据齐全。只有单出口带宽或资源滞留占主要成本时，再选择并行 endpoints、range order merge 或持久结果段。全局 ORDER BY/LIMIT 必须证明完整顺序与完成结果。`FlightInfo.ordered` 只能描述已保证的端点拼接顺序。 [Arrow Flight 19](https://arrow.apache.org/docs/19.0/format/Flight.html) · [Trino 结果分段](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol)
- **DML/CDC。** 先移除 `isCommitted/getEntry` 的已知重复读，保留原 authority 和行状态判断。不要因为只有一个发布 API 就把 journal 或清理做成 catalogue commit 的隐式成功。连接器 publish 与 journal 分属不同事务，前后校验不能仅因相邻而合并。COW、write file sizing、碎片包装和 COPY 本地化各用自己的证据，含快照可见、旧文件保留、未知提交和 CDC offset。 [Iceberg 可靠性](https://iceberg.apache.org/docs/latest/reliability/)

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

1. 以真实分片、对象读和精确终态证据交付跨 Stage 动态过滤。先限定确定性的 Inner BIGINT 等值 Join，再由实际目标负载决定扩展范围。
2. 在已有完整连续范围建议的基础上，将提交的 Shuffle 源分区映射到实际消费任务。优先验证单输入、低字节无偏斜的合并，再评估 Join 与热点拆分。
3. 为读写路径运行独立过程与资源压力验收，记录普通路径、失败恢复与物理退出。先对现有行为归因，按明确阈值决定额外的存储和结果并行优化。
4. 用冻结的 Linux 配置完成同条件跨引擎基准及正式发布回归。

每份 PR 的最终运行入口保持唯一。严重依赖可以在隔离集成分支内合入和验证完整链路，达到对应测试后随新版本一起发布。生成器、源 proto 两端与 descriptor golden 同步更新，删除字段后保留原编号与名称，新增字段使用新编号。schema 改动落到 canonical SQL 迁移模块及新集群引导，候选用独立的数据库和对象前缀初始化新 schema，不安排旧库或旧结果格式迁移。基线 A/B 使用各自同源的隔离部署；候选出错暂停发布，在单独环境修正并重新验证。

每一个删除项验收同时看生产调用归零、注册/配置被移除、相邻行为经过真实入口，以及对应的性能或正确性证据。生产 rollout 只有整套新版本和全新运行环境，故障恢复用同版本状态机演练。目录现有的业务数据与历史实验保留为可重现输入。

## 验证记录与覆盖边界

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

Worker 的 `ConsumedResultSweep` 仍以 100 ms 间隔逐段扫描，每轮最多检查 64 个注册项。用实际 Java 调度器及注册表模拟同批已消费结果，512 个注册项从消费信号到释放回调的 P50 为 524 ms、最长 840 ms，2,048 个注册项的 P50 为 1.76 s、最长 3.32 s。这项单次测量以可控谓词模拟 Native 完成信号，覆盖轮询调度开销。生产 Native 还需保留 producer 已停、授权已撤、队列为空和 reader、导出 batch 已释放的联合证据。当前 `resultDrainEvidence()` 查询还负责根据 Velox 状态更新 producer 停止证据，事件路径须将该更新移到真实完成回调。Coordinator 的取消测试也观察到仍在等待实际 Worker drain receipt，须单独验收收齐回执后的释放延迟。

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
