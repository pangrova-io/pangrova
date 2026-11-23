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
| 快照和 split 枚举 | 扩展跨 Stage 动态过滤到多键、更多连接器和晚到覆盖。大文件按合法范围拆分，小文件按工作量组包。有界枚举与实际读放大按连接器验证 | 冻结相同快照与授权。比较 split 数、实际对象 GET 和读取字节，覆盖缺失、晚到、失败、删除文件、快照演进和取消 |
| 计划、分配和控制面 | 量化下游准备隔离在固定到达率、受阻目录 I/O 与写入混合负载下的 P95。以混合负载校准单输入 AQE 的阈值和尾时延，再验证多输入、偏斜拆分和持久重放 | 基线和候选完成同一完整结果。检查覆盖、滞后、重试、投机获胜和 durable replay |
| 远端 Shuffle | 核验更宽和变长输出的实际对象数、内存及跨主机吞吐。扩大热点并发，量化锁等待与尾时延，再校准是否需要段存储 | PostgreSQL 和对象存储真实压测，覆盖权限撤销、owner 切换、背压、未知 PUT、丢 ACK 和 GC |
| Velox 与结果 | 对剩余 I/O、解码、spill、缓存和单出口成本做归因。继续测跨主机出口、持续内存占用与链路异常下的释放 | 冷热、偏斜和宽/窄负载。若出口瓶颈成立，验证并行消费的顺序、EOF、资源窗口和真实归还 |
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
- **真实 AQE。** 派发保持原计划的分区坐标，`ExchangeSourceBuilder` 按 consumer partition ID 取 producer 分区或 receipt。单哈希输入的范围合并已按已提交 producer 输出生成、冻结并校验了实际读取映射。进一步校准阈值及真实负载尾时延。用不同 join 类型、NULL、重复键、倾斜、重试和 durable replay 证明完整覆盖、去重和成本上界，再比较关键路径。Spark AQE 的参考点是使用运行时输出统计与不同读取划分。它的默认阈值不直接作为 Pangrova 门槛。 [Spark SQL AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
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
2. 用完整 SQL 负载校准已启用的单输入范围合并，再验证 Join 两侧的对齐范围、热点拆分与持久重放。
3. 为读写路径运行独立过程与资源压力验收，记录普通路径、失败恢复与物理退出。先对现有行为归因，按明确阈值决定额外的存储和结果并行优化。
4. 用冻结的 Linux 配置完成同条件跨引擎基准及正式发布回归。

每份 PR 的最终运行入口保持唯一。严重依赖可以在隔离集成分支内合入和验证完整链路，达到对应测试后随新版本一起发布。生成器、源 proto 两端与 descriptor golden 同步更新，删除字段后保留原编号与名称，新增字段使用新编号。schema 改动落到 canonical SQL 迁移模块及新集群引导，候选用独立的数据库和对象前缀初始化新 schema，不安排旧库或旧结果格式迁移。基线 A/B 使用各自同源的隔离部署；候选出错暂停发布，在单独环境修正并重新验证。

每一个删除项验收同时看生产调用归零、注册/配置被移除、相邻行为经过真实入口，以及对应的性能或正确性证据。生产 rollout 只有整套新版本和全新运行环境，故障恢复用同版本状态机演练。目录现有的业务数据与历史实验保留为可重现输入。

## 验证记录与覆盖边界

字面值 INSERT 与 INSERT OVERWRITE 在预检内共用一次表元数据，列顺序、FILE MIME、托管文件与生成列校验复用同一观测。写入分配和 fenced commit 继续验证当前权威状态。定向对照中，显式列 INSERT 的句柄与元数据调用由 2 次和 3 次变为各 1 次，省略列名的写入由 2 次和 4 次变为各 1 次。真实并发 Iceberg 对照中，四个 INSERT 在受理到调度之间各自的目录表加载从 9 次降为 5 次，可见快照和读取结果一致。此处计量的是连接器加载调用。 88 项定向回归与 Checkstyle 通过，另有真实读写并发用例通过。

Coordinator 提交线程执行规划和启动分布式 DAG，查询运行期间由完成事件推进。同步接口等待同一个结果，DML 与 CTAS 在提交栅栏通过后继续发布控制面结果，取消、阶段期限、写入期限和物理资源归还仍由执行所有者负责。真实单 Worker、HTTP 与 Flight 的旧路径对照中，四条已读完数据的查询保留其终态报告，占满四条提交线程。第五条等待 538 ms，释放一份报告后 33 ms 读到结果。候选在暂留全部四份报告时完成独立读取。四个真实 Iceberg 写入等待提交时也完成独立读取，随后各表读回预期数据并确认物理清理。这项受控对照验证调度隔离。固定到达率端到端 P95 留在发布验收。 164 项定向测试、69 项打包端到端测试及 Checkstyle、SpotBugs 通过。真实回归覆盖双 Worker Shuffle、CTAS、并发写入、拒绝与取消、物化和 Native 资源释放。角色运行包清单 SHA-256 为 `a6e4ddd526faf8da10ee046eadb9a6dcd3bce97f8854b46a01db0303831817fe`。 [Trino 查询生命周期](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/execution/QueryExecution.java)

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

实施批次 122 将低基数局部归约扩到多个直接分组列。每列沿用同一选定快照的列统计，规划器按各列基数估算组合数并计入 NULL，同时校验预计交换行数和字节都下降。真实双 Worker 的 `COUNT/MIN/MAX` 查询读取 192 行，原来 192 行进入 Shuffle，候选启用后 16 行进入 Shuffle。未发布列统计时仍交换 192 行。四个结果组逐组核对，涵盖 NULL、区间边界和物理释放。54 项 Planner、真实 PostgreSQL 读绑定、JNI 命令及双 Worker Java 用例和 Checkstyle 通过。

实施批次 121 将已解析的初始 `RuntimeFilterBinding` 和运行时 JNI 命令的类型化过滤器直接送入同一 Velox 转换入口。以实际生成的 Protobuf 类型测量，262,160 字节 Bloom 的内部消息为 262,194 字节。旧路径的晚到命令序列化与解析每次约 8.86–9.20 µs，初始绑定还复制一次过滤器，每次约 14.81–15.10 µs。两条路径均移除内部往返。测量仅覆盖本机协议交接。48 项定向及 4,350 项完整 Native CTest 通过，14 项真实 SQL Java 端到端和 Checkstyle 通过。

实施批次 120 修复 Native Flight 客户端断连后 OUTPUT 未退出的问题。独立客户端消费 72 MiB 变长结果的首行后被强制终止，期间未调用 Coordinator 取消接口。基线在 15 秒内未释放；探针确认 gRPC 已销毁 reader 且清空队列，实际 Velox 生产者仍存活。修复复用已绑定的失败回执，终止对应的 SDK 任务，并保留真实读取者、缓冲区与生产者的释放条件。本地对照在 34 ms 内观测到 `FAILED` 和物理释放。完整读取与显式取消用例保持正确。独立复测的物理释放为 27 ms。31 项 Java/JNI、Checkstyle、模块边界检查与 4,349 项完整 Native CTest 通过。显式取消回归核对原生终态和相同尝试的真实释放回执。同一 macOS ARM64 正式角色包通过 Native 双 Worker 最终结果和重启，以及 mTLS Remote Shuffle、Flight 和实物清理。739 项清单摘要为 `2fb6a8b853eee3e74623ab12b74382b122deb03c55aa6d2ab4b0d70a1da0f046`。

实施批次 119 保留各获胜 Worker 的构建键区间，合并重叠和相邻区间，最多向 Iceberg 传入 16 段。超过上限使用覆盖所有键的外包区间。真实双 Worker 回归锁定同一份 Iceberg 夹具和等值 Join，两个构建任务分别报告 `[2,3]` 与 `[35,36]`。原来合并成单段时，probe 完成 35 个分片并读取 24,460 字节。修复后为 4 个分片与 2,795 字节。对照用不触发跨 Stage 过滤的等价表达式，均为 40 个分片与 28,883 字节。完整结果包含重复键和 NULL 输入，逐行对照并等待所有任务物理释放。46 项 Java 与 Checkstyle 通过，其中 11 项为真实 SQL 端到端用例。模块边界检查通过。

实施批次 118 将持久化阶段的角色检查和任务准备接到同一份事务内授权证据。任务准备继承已经验证的直接角色，并保留租户、表策略、模型权限和当前执行租约的检查。真实 PostgreSQL 的 20 分区调度夹具执行 3 个检查窗口，`pg_stat_statements` 中匹配直接角色查询的调用从 6 次降到 3 次。 107 项 Java、Checkstyle 与模块边界检查通过，涵盖撤权、取消、租约期限、持久 Shuffle 和目录导出。

实施批次 117 用 8,192 行 Iceberg 数据验证 Native Flight 窄结果和显式 `VARCHAR` 变长结果。两次读取均逐行核对。窄结果的有效文本为 31,658 字节，首行、EOF、物理释放分别为 170、186、189 ms。宽结果有效文本为 75,497,472 字节，客户端在首行后暂停 250 ms，三个观测值为 127、502、514 ms。另一次在首行后发出取消并提前关闭，实际收到 `CANCEL_ACCEPTED` 和 `CANCELLED`，Native 回执与 Coordinator cleanup 完成。数值是本地应用层样本。

同一持久客户端完成 150 次串行和 80 次每秒 8 次到达的混合读。混合首行 P95 依次为常量 23.62 ms、点查 98.18 ms、选择性扫描 58.84 ms、计数 58.19 ms、分组 69.20 ms，各 16 个样本。230 次都核对完整结果与物理释放，每次最多使用两次状态请求。正式 macOS ARM64 角色目录的 739 项清单摘要为 `caea25d7c508332d1ad6afb0b92b575f3b85983b85c37ba6d358559659be9140`。同包通过独立进程 Native 双 Worker 重启、mTLS REMOTE Shuffle、Flight 与对象和槽位实际释放。6 项真实端到端回归与 Checkstyle 通过。跨主机出口、无取消指令的断连和长时间驻留仍需验证。

实施批次 116 在 Profile 中记录被当前 Stage 代次接纳的物理尝试及对应逻辑任务。记录点在实际终态槽位接纳之后。已取代的任务和未被接纳的报告不产生记录。重试夹具验证了逻辑任务与获胜副本 ID 不同的情形。真实双 Worker SQL 将实际接纳记录与 Native task attempt 坐标对应。115 项 Java 和 Checkstyle 通过，涵盖投机、重试、持久赢家、写提交竞争、HTTP 和释放。后续继续区分 Exchange 阻塞、队列与物理释放时间。

实施批次 115 让状态读取、状态等待和取消共用本机查询的原始提交主体。用户显式读取历史状态时，单条 journal SQL 返回最新状态与原始受理证明。恢复时从已有 journal 批次恢复主体。74 项 Java 和 Checkstyle 通过。真实双 Coordinator PostgreSQL 对照核验本机状态等待及非 owner 的定向等待均为零读取，显式历史请求每次只借用一次连接。真实 HTTP、Native Flight 和 PostgreSQL 验证其他主体的状态与取消请求直接拒绝，未执行 journal SELECT，原提交者仍可完整消费并确认物理释放。

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

实施批次 100 用实际打包的 Coordinator、两个 Worker、CLI 和 C++ Release JNI 验证了单输入 AQE。公开 SQL 从四个分开提交的 Iceberg 文件读出 20 行，四个哈希分区经 mTLS 远端 Shuffle 合并为一个消费任务。Native Flight 返回有序的四组计数 4、8、4、4。两个 Worker 回报精确 attempt 的 Native 释放，已归还租户槽位与 PostgreSQL Shuffle allocation，MinIO 桶内的实际对象已清空。现有 NATIVE 进程回归还验证了双 Worker 查询和优雅重启。34 项定向 Java 和 Checkstyle、9 项打包工具测试通过。

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
