---
title: "Pangrova 查询性能对齐与执行引擎修正计划"
description: "对标同类查询引擎，修复湖仓扫描、Join、聚合、交换与结果返回"
unlisted: true
---

# Pangrova 查询性能对齐与执行引擎修正计划

分片放置已采用物理扫描字节。Remote Shuffle 消费窗口内无 PostgreSQL 同步读取，生产端按窗口预留并发布批次。Exchange 前的局部归约已覆盖全局聚合以及满足成本门槛的低基数组合分组。后续优先减少跨 Stage 动态过滤遗漏的扫描量。控制面、对象存储 I/O 和结果返回继续按各自的成本归因验收。实施进度见 [执行链路重构与清理记录](./execution-clean-cut-plan.md#验证记录与覆盖边界)。

## 同类对标及适用边界

| 对标 | 查询场景 | 已核查的做法 |
|---|---|---|
| Trino + Iceberg | 交互式 SQL、多租户、选择性扫描和对象存储 Join | Connector 统计驱动 join 顺序和 broadcast/repartition，动态过滤传至 Coordinator 的 split 发现和 Worker 扫描，Iceberg 为过滤提供有界等待。可重试的持久交换适合大批任务，短查询另测。 [CBO](https://trino.io/docs/current/optimizer/cost-based-optimizations.html) · [动态过滤](https://trino.io/docs/current/admin/dynamic-filtering.html) · [Iceberg connector](https://trino.io/docs/current/connector/iceberg.html) · [FTE](https://trino.io/docs/current/admin/fault-tolerant-execution.html) |
| Spark SQL + Iceberg | 大扫描、复杂 Join、聚合、倾斜和强扩展 | 把文件打开成本纳入任务大小。AQE 根据 map 输出决定读取合并、拆分和 join 变化。Storage Partition Join 需要数据源报告兼容分布。push-based shuffle 有部署和负载前提。 [SQL 调优](https://spark.apache.org/docs/latest/sql-performance-tuning.html) · [Shuffle 配置](https://spark.apache.org/docs/latest/configuration.html#push-based-shuffle-overview) |
| Spark + Apache Gluten/Velox | Native CPU、列式转换、Shuffle、内存专项归因 | 记录实际 offload、算子支持和 fallback，把后端收益与 Spark 原生分别报告。用同语义子计划和资源隔离对照。 [Gluten Velox](https://gluten.apache.org/docs/v1.5.0/get-started/Velox.html) · [Velox 内存](https://facebookincubator.github.io/velox/develop/memory.html) |
| Dremio | 诊断与可选加速的设计参照 | Profile 分开显示估计与实际行数、fragment/算子的 setup/process/wait。Reflections 需单列预计算、命中率和刷新成本。 [Query Profiles](https://docs.dremio.com/current/admin/monitoring/jobs/view-profiles/) · [Reflections](https://docs.dremio.com/current/acceleration/) |

查询性能声明使用同资源、同结果和同负载的实验。工程模板和跑法称为 TPC 派生对照，官方 QphH 汇总需要符合 TPC 的运行和披露条件。 [TPC-H 概览](https://www.tpc.org/tpch/) · [仓库 TPC 方法](../testing/tpc-benchmark-methodology.md)

## Pangrova 的缺口及证据

| 环节 | 生产源码或实验证据 | 影响及待确认项 |
|---|---|---|
| CBO 统计质量 | 已移除由 min/max/NDV 推导的均匀直方图，并保留缺失 NDV。Join 和过滤器按直接列来源读取统计。三表 Join 的两个等价写法在本地规划探针中均使用实际来源的列统计 | 范围过滤仍需实测分布。倾斜和相关性可能影响 Join 顺序、build 侧、broadcast 和并行度。用每 plan node 的估计偏差与实际物理计划验证 |
| Join 并行化 | `PlanFragmenter.resolveStageParallelism` 对缺少兼容 Exchange 或仍混有直接表扫描的 Join 回到单任务，保护分布式覆盖 | 记录哪些多表 SQL 触发此保护、部署中实际 Worker/driver 数与单任务耗时，再为高成本形态补齐双输入分布 |
| 跨 Stage 动态过滤 | 实时 INNER BIGINT 等值 Join 按完整获胜回执约束同一 Iceberg 快照。真实双 Worker 查询的连续键场景从 40 个分片、32,449 字节降到 2 个分片、1,398 字节。分散构建区间场景从 35 个分片、24,460 字节降到 4 个分片、2,795 字节 | 单构建任务保留有界稀疏区间，冷探测从 35 个分片、27,885 字节降到 4 个分片、2,795 字节。继续验证等待成本、其他键及连接器。删除名称型描述与测试专用收集器留下的断开路径 |
| 扫描发现与分片大小 | Iceberg 已按批枚举并关闭 SDK 文件计划。`SplitFetcher` 仍收齐分片用于全局放置，`IndexAwareSplitFetcher` 先全量 fetch 再过滤。放置使用 split 实际文件范围和物理字节估计 | 按字节分配将两个 512 MiB 和两个 512 KiB 的输入从 1 GiB 与 1 MiB 调整为两组 512.5 MiB。10,000 文件的单 split 探针 P50 从 11.340 ms 降到 2.647 ms，生产 SQL 仍须单独测量放置等待、I/O 和尾 task |
| 聚合与 Exchange | 并行扫描上的全局 `COUNT` 和整数 `MIN/MAX` 可在任务本地归约，具有快照匹配 NDV 的简单分组可按同一键归并。真实双 Worker 分别验证了读取 80 行、输出 8 条全局状态，以及两列分组的 Shuffle 行数从 192 降到 16 | 更广的函数覆盖以及中间状态跨网仍需准确的类型和成本证据。Native 转换器对缺少原始参数类型的状态输入 phase 会拒绝 |
| AQE 读取布局 | 符合条件的单哈希输入按已提交回执合并连续分区，保留生产坐标和各分区证明。真实 Native 与两端点对照覆盖了 4 → 1 个任务和 35 行逐项一致 | 完整 SQL 负载仍需归因，Join 多输入合并、偏斜拆分和 durable replay 各需覆盖证明 |
| 短查询、交换与结果 | JNI 完成轮询已读取标量状态。定时 HTTP、远端逐 batch SQL 与 Flight IPC 回转已有调用证据。现有局部 Top-K 和最终归并应作为基础 | 按查询类别量化固定开销、读放大、实际出口和客户端背压，优先缩短无数据依赖的等待 |
| 归因能力 | JSON Profile 保留实际派发的 Stage、Exchange、阶段转换、各 Native attempt 与被 Stage 接纳的物理成功尝试 | 已记录实时读准备队列的排队、运行和拒绝。继续补齐 Exchange 阻塞原因、依赖准入和物理释放时间 |

## 修正工作包

### Q0 建立可解释的查询基线

1. 对同源 Coordinator、Worker、Native bundle 关联 query、stage、partition、attempt、plan node 和 owner epoch。按 Stage 代次和逻辑槽位对应 Profile 中的物理尝试与已接纳记录。采样 trace 关联高基数标识，常驻指标保持低基数。记录对象 GET/字节、decode、Shuffle payload/wire bytes、CPU、blocked、spill、RSS/heap/direct、JNI/控制字节、SQL 次数和锁等待。
2. EXPLAIN 分别记录各叶/Join/Aggregate 的估计行数和字节、统计来源及版本、物理分布、并行度和降到 singleton 的理由。运行报告已有算子累计阻塞以及生产者、消费者、split 与连接器等待。继续把对象请求、解码、首批、尾 partition 和真实资源释放关联到物理关键路径。基数用对称误差比衡量，零行和未知各自保留。阶段时间基于因果关系，避免把并行段相加。 [Trino EXPLAIN ANALYZE](https://trino.io/docs/current/sql/explain-analyze.html) · [Dremio Profile](https://docs.dremio.com/current/admin/monitoring/jobs/view-profiles/)
3. 现有 `tpch-sf100-baseline.json` 的 22 项 P50/P99 均为零。历史 SF10 聚合和 macOS 样本只作排查。跨引擎基线需要绑定代码、Native/依赖、硬件、数据和客户端，先验证结果再测速。

2026 年 11 月 22 日核对 GitHub Actions 的基准 runner 和配置，当前无匹配的专用 Linux runner，`TPC_BENCHMARK_CONFIG_DIR` 未配置。最近两次计划运行均跳过。SF100/SF1000 保持待验收。 [最近一次受控基准运行](https://github.com/pangrova-io/pangrova/actions/runs/45005872612)

### Q1 缩短交互查询，隔离大查询

本地持久连接已有常量、点查、选择性扫描和聚合的首行、完整消费、终态与物理退出基线。状态等待按客户端目标合并。继续测独立进程与取消，并在足量混合负载下核验实时读准备隔离的 P95、排队与拒绝。分别量化 durable 和写入确认耗时，并验收真实的资源释放。已完成的协议、状态等待、回调隔离与 drain 改造记入实施记录。

### Q2 减少读取量并修正扫描发散

1. 在已有按物理扫描字节分配的基础上分开校准对象打开、解码和删除成本，记录估计缺失。需要 durable cohort 的阶段按固定快照和完整分片覆盖验证限流派发。 [Spark 文件分区](https://spark.apache.org/docs/latest/sql-performance-tuning.html#tuning-partitions)
2. 先从 manifest、分区/文件统计剪裁开始，基于 Native reader 指标证明 row group/page 的读取量。大文件按不重叠的合法范围拆分，小文件按工作量组包。测试快照演进、删除文件、冷/热缓存、宽表、稀疏与随机访问。索引按成本和实际跳过 bytes 评估。缓存亲和用实际命中与驻留证据，prefetch 按字节预算，避免无效读取把热数据挤出。 [Iceberg 扫描规划](https://iceberg.apache.org/spec/#scan-planning)
3. **扩大可证明的动态过滤覆盖。** 当前 Iceberg INNER BIGINT 单列路径使用选定快照、稳定 field ID、完整获胜集合和 200 ms 有界等待。已验证多个构建任务与单任务内的稀疏范围。执行图已分别测得单构建与多构建任务的等待和 Probe 读取量。接下来按实际数据分布测量多键、过载及超时成本。在并行探测、迟报、投机和大扫描夹具中验证尚未签发的工作可以安全接收新条件。统计等待、已发 I/O、文件与 row group 跳过及实际字节。 [Trino 动态过滤](https://trino.io/docs/current/admin/dynamic-filtering.html)

### Q3 让 CBO、聚合和 AQE 缩减跨节点工作

1. **统计与 Join。** 用表/列和快照版本绑定统计来源，保留采样信息和 unknown。根据真实误判优先补少量高频值、NDV 和选择性证据，之后再评估多列统计。广播门槛受每 Worker build 上界、并发内存和复制字节约束。运行反馈按可比较的谓词、query scope 和数据身份使用。为实际热点 Join 完成 partition/broadcast 的覆盖证明。 [Trino CBO](https://trino.io/docs/current/optimizer/cost-based-optimizations.html)
   - 普通 Iceberg 规划从选定快照的摘要读取行数、文件数和字节量。列统计复用版本匹配的 `ANALYZE` 发布结果或快照匹配的 Puffin 元数据，重型 manifest 聚合留在显式采集。已删除按名称预查统计的中间层。`PlanningReadBindings` 为同一编译中的合格表源按授权表 ID 读取一次统计，并同时匹配选定快照版本。历史 Iceberg 和 Paimon 快照用对应快照的统计，增量范围和缺少证据的统计保留 unknown。
   - 用同一查询分别测量目录指针 SQL、表所有权 SQL、统计读取 SQL 和 manifest I/O。验证提交新快照、并发 `ANALYZE`、历史快照及撤销访问后的估计和物理计划。基线已观测到 20 个 manifest、10,000 个文件描述时，统计阶段开启 41 个 Avro 读取流，分片规划又开启 20 个。
   - Calcite 的行类型和行数访问复用查询内的不可变绑定，可选表能力探测直接判断类型。受控 Paimon 自连接加分组的规划探针每轮所有权 SQL 从 31 次降到 5 次，物理计划指纹一致。连接器声明和读绑定的交接仍检查当前所有权。
2. **扩大可证明的局部聚合覆盖。** 已验证符合成本门槛的标量和多列分组 COUNT 与整数 MIN/MAX。剩余工作按实际 SQL 热点验证 SUM、AVG 的 Substrait phase 和 Velox 中间状态。覆盖 DECIMAL 溢出、DISTINCT、GROUPING SETS 和高 NDV，按输入行、状态字节、CPU 与内存决定是否启用。 [Trino 预聚合规则](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/sql/planner/iterative/rule/PushPartialAggregationThroughExchange.java)
3. **扩展 AQE。** 用端到端 SQL 测量已启用的单哈希输入范围合并，记录任务数、尾 task、spill、Shuffle 授权与实际网络。多输入优先验证 Join 两侧对齐范围，然后按 Join 类型拆热点并限制另一侧复制。覆盖 NULL、重复值、投机获胜、重试及 durable replay，按实际分布确定阈值。 [Spark AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
4. **先减少要交换的数据。** 预聚合、列裁剪、有效 dynamic filters 和正确的局部/广播 Join 优先。Storage Partition Join 仅在两端 partition spec、变换、键类型及哈希/NULL 语义可证兼容时开启。分段存储、codec 和 JNI payload 优化按各自占比验证。Remote Shuffle 消费窗口内保持零同步 SQL，写入元数据事务按窗口统计，分别验证撤权及实际回收。 [Spark Storage Partition Join](https://spark.apache.org/docs/latest/sql-performance-tuning.html#storage-partition-join)

### Q4 专项优化 Native 与结果返回

1. 分开测对象 I/O、解码、filter/project/Join/Aggregate CPU、memory arbitration、spill 和 cache 缩减。达到归因条件才改 Velox 配置、向量 batch、prefetch 与 allocator。相同算子用 Gluten/Velox 做参考，同时记录 offload 覆盖和跨边界开销。 [Velox Runtime Metrics](https://facebookincubator.github.io/velox/monitoring/metrics.html) · [Gluten Velox](https://gluten.apache.org/docs/v1.5.0/get-started/Velox.html)
2. 本机已核验 8,192 行窄结果、72 MiB 变长结果、慢读、显式取消和客户端进程中途退出。继续用有界 Arrow 批流测量跨主机吞吐、长期驻留与链路异常后的回收。出口饱和时对照多个端点或并行 range merge。慢消费者持有执行资源主导时评估 spooling，验证排序、EOF、终态、撤权、实际退出及存储成本。已有局部 Top-K 可独立验证 metadata 剪枝。物化视图与预计算的收益、维护成本和数据新鲜度单列。 [Arrow Flight 19](https://arrow.apache.org/docs/19.0/format/Flight.html) · [Trino Spooling](https://trino.io/docs/current/client/client-protocol.html#spooling-protocol) · [Dremio Top-K](https://docs.dremio.com/dremio-cloud/admin/performance/top-k-query-acceleration/)

## 同条件对标与发布门槛

2026-11-23 复核了发布环境。仓库未登记专用基准 runner，未设置 `TPC_BENCHMARK_CONFIG_DIR`，本机也没有共享 SF100/SF1000 快照。本机可复用的 Docker 镜像持有 macOS Mach-O 测试产物，无法作为 Linux Native 构建证据。现有 TPC 工具通过 114 项自测，正式跨引擎门槛仍待运行。

| 活动 | 数据与场景 | 必交证据 |
|---|---|---|
| 正确性 | SF1 全模板，另含空/NULL、Decimal/时区、重复、Join 类型、偏斜、取消和故障 | Typed digest、ORDER BY/LIMIT、plan/attempt 与已消费范围，不支持和失败单列 |
| 交互与混合 | 持久客户端短 SQL、点查、高选择性与分析查询混跑。分别做固定并发和固定到达率 | 首/末批和端到端 P50/P95，P99 需足量样本。报告最大符合 SLA 的成功吞吐、拒绝/超时和实际 drain |
| 大查询归因 | TPC-H Q6 扫描、Q1 聚合、Q9 多表 Join、Q3/Q18 排序或 Top-K，再跑 TPC-H/TPC-DS SF100 | 实际读取、GET、Shuffle、spill、CPU/bytes、并行度、估计误差和最长 partition |
| 扩展与恢复 | SF1000，独立 Worker 2/4/8。同一数据进行强扩展，冷/热、健康/故障分开 | 吞吐和每核效率、固定失败策略、恢复时间、资源使用率与尾延迟。可重试和直接路径分立 |
| 大结果 | 窄/宽 schema、固定 byte window、总结果大小增加一个数量级；加入慢读及断连 | 首批与消费吞吐、完整结果时间、heap/direct/RSS、背压与释放。全部引擎完成等价的结果读取 |

每个窗口只跑一个引擎。冻结 CPU、内存、磁盘、Worker 数、网络、存储、catalog、Iceberg 快照和 Parquet 布局。cache、ANALYZE、容错、TLS、物化和客户端设置一并记录。每项同时给出引擎可比时段与真实客户端完整消费时间。启动、连接和渲染单独计量。采用现有 `tpc_reference.py`、`run_tpc_suite.py`、`tpc_suite_compare.py`、`tpc_report.py`。按仓库方法跑独立窗口，固定到达率、冷缓存、大结果与故障另跑。拒绝把历史样本填成当前基线。 [仓库跑法](../testing/tpc-benchmark-methodology.md)

**对齐标准先于候选测量登记。** 交互主对标 Trino，批量分析主对标 Spark SQL，两组均保留另一引擎的结果。建议工程初始目标是在同样的资源和服务等级下，目标负载族典型查询 P50 不超过主对标的 1.05 倍，P95 不超过 1.10 倍，最大符合 SLA 的成功吞吐不少于 95%。这组值是待按基线噪声校准的目标。完成可比基线后冻结错误预算、噪声容限和不可退化的代表查询清单，才开始计入候选验收。报告每条 SQL 的比值、失败、最慢尾部与覆盖率。跨引擎比值要使用各自有效计量数据，同一负载的总体变化不能用各 SQL 的百分位相加。按工作负载验收性能，同时遵守零 SQL、结果正确、权限和所有权的硬门槛。

| 交付 | 主要模块 | 首个放行证据 |
|---|---|---|
| Q0 归因基线 | Observability、Testing、Planner、Worker Native | 每个查询能复核实际物理计划、行/字节和关键路径。版本、资源、快照和结果相同的窗口可以重放 |
| Q1 控制面 | Client、Transport、Query、Coordination、Worker | 短查询真实首批与完成改善。混合高负载下取消/终态和租户资源有界，前述清理计划的零读库验收保留 |
| Q2 扫描与动态过滤 | Connectors、Planner、Coordination、Worker Native | 物理扫描字节和最长分片实测下降，覆盖完整。真实 SQL 的跨 stage filter 在 probe 到达前产生可见剪裁，晚到/缺失仍返回等价结果 |
| Q3 物理计划 | Planner、Protocol、Native、Shuffle | 单输入 AQE 已通过真实 SQL 的四分区到单消费任务与完整结果验证。量化局部聚合、阈值和尾时延，并对多输入、偏斜与重放验证映射完整性 |
| Q4 Native 与结果 | Worker Native、Transport、Client、Testing | 对比同一运算和相同消费后，确认被改路径的 CPU/bytes、峰值内存或端到端耗时下降 |

Q0 先行，Q1 与 Q2 可独立验证。Q3 在有统计和 Exchange 覆盖证据后分别提交 Join 分布、预聚合和 AQE 改动。Q4 只修改已经归因的路径。性能对齐的发布证据包含报告、版本和数据身份、覆盖率和仍有差距的 SQL。

## 本轮验证范围

重新追踪生产规划器、DAG 组装、Iceberg split、Substrait 转换、Native phase 和 Profile。Corretto 17 编译当前 `StageSplitPartitions`，复现四个 split 的模拟负载反例。核对 SF100 入口 22 项 P50/P99 的零值。本地现有 Native 测试二进制通过 26 项聚合、动态过滤和 Shuffle 背压聚焦测试，这些用例验证本地机制与数据通道。深度对标基于公开一手文档和上游实现。当前仓库缺同条件的跨引擎实测报告，性能差距及收益以 Q0 和上述门槛确定。
