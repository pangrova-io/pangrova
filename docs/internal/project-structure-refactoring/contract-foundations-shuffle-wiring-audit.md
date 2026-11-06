# 新纳入门禁的 Shuffle 实现：生产调用审查

本批将 Shuffle 契约移入 `-api` 后，SPI 门禁自动扩大扫描范围，发现以下五项真实
装配缺口。门禁失败保留，不增加豁免、不给孤立实现增加 Experimental 注解、不让孤立对象
仅为“可达”而被创建。以下是源码证据与后续必须闭合的需求，不是实现完成声明。

| 类型 | 实际源码状态 | 必须闭合的边界 |
| --- | --- | --- |
| `InMemoryShuffleDataInventory` | 只有单元测试创建；生产没有 register/markConsumed/removeQuery 调用。key 仅 query/stage/worker，没有 generation/attempt。Operations 的 residency/drain 端口没有与它连接 | 对接实际 producer/consumer 生命周期、精确 attempt 和保留事实；缩容选择具体节点并阻止丢失仍被读取的数据。不得把空 inventory 或 `alwaysAllow` 当作真实安全判断 |
| `MedianSkewDetector` | 只有测试创建；实际 AdaptiveQueryEvaluator 使用 SkewJoinHandler，后者重复检测算法 | 用唯一 detector 完成实际 adaptive 调用；保留 bytes/rows 双条件、最小大小、阈值和分区映射，验证真实 stage 统计到决策链，而不是额外计算后丢弃结果 |
| `SpoolingExchangeManager` | 只有测试创建；register 返回 LOCAL sentinel，commit 无论是否注册都返回 EMPTY，capability 由 durability 推导 | 本地 spool 的实际读写、producer attempt 选择、seal/commit 可见性和清理必须共同实现。不能直接接入该 no-op commit 并宣称 FTE；旧实现若由完整新 Provider 替代，应连同专属测试和文档退役 |
| `LocalSpillLifecycleManager` | package-private，生产无构造调用；重复记录注册，commit 为 no-op，cleanup 吞异常 | 与真实 Local Provider 统一 lifecycle/receipt owner；保留空分区完成、取消、活跃读取与可重试清理。不能只移动文件作为 Local 支持证据 |
| `DefaultShuffleByteEgress` | 只有测试创建；接口是 onPartitionCommitted(query, stage, partition, map, attempt, buffer)，生产 Native 不调用此形状；未使用的 writer cache 字段仍在 | 提取真实 Native Adapter 回调边界，明确 buffer lease、背压、完成/取消/错误传播，再让实际 Provider 消费编码数据；禁止直接把这个对象注册给当前 JNI |

## 当前真实数据路径

WorkerTaskBootstrap 在 REMOTE 模式创建 `RemoteShuffleByteEgress` 并向 NativeRuntime 注册。
C++ `ShuffleEgressExecutor` 实际解析三个回调：

- `onPartitionBatch(queryId, taskId, partitionId, rowCount, ByteBuffer)`；
- `onTaskCompleted(queryId, taskId)`；
- `onTaskAborted(queryId, taskId, reason)`。

直接注册 `DefaultShuffleByteEgress` 会发生 JNI method lookup failure。需要沿现有真实
回调、task 绑定和持久化 remote producer commit 完成通用化，不能复制另一套关闭或
attempt 状态。Direct 仍须保留 C++ 数据通路，不强迫经 JVM。

`ShuffleByteEgress` Javadoc 关于“尚无 JNI 数据路径”的说法已经过时，但该旧接口本身
仍未接通；后续应以真实 Native Adapter 契约替代，不能仅改文档声称旧接口可用。

## 验收要求

这些能力是当前目标的未完成事项。后续实现需覆盖 SQL/配置入口、实际装配、运行调用、
状态与资源 owner、支持与拒绝组合，以及真实 Native/多 Worker 测试。
本批 1,530 个定向用例中 1,529 通过，唯一失败为发现这五个实现的 SPI 门禁；
该结果不能宣称全仓回归通过或已达到生产验收。

后续更正：上文 JNI long 参数按 C++ 实现为 rowCount，并非 batch sequence。
MedianSkewDetector 的实际接入与验证见 [skew-wiring-ownership.md](skew-wiring-ownership.md)。
本文件五项失败是前批历史结果，不是后续批次的验收结果。
