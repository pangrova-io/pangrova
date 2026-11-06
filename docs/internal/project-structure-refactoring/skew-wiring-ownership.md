# AQE 倾斜检测的真实接入与职责收敛

本批对应 P7/P8 的一条实际调用闭环，不代表整个 Shuffle 通用化完成。
基线为本批开始时工作区的 8,233 个文本文件；此前改动原样保留。

## 需求与实际调用

上批门禁发现 `MedianSkewDetector` 只有测试创建，但实际 AQE 已经在
`SkewJoinHandler` 中重复实现同一检测策略。本批消除重复，并接通生产消费者：

```text
Server QueryExecutionBootstrap 选择 MedianSkewDetector
  → QueryDispatcherContext 固定借用该实例
  → QueryDispatcher 创建实际 AdaptiveQueryEvaluator
  → 阶段完成报告转换为 ShufflePartitionStats
  → SkewJoinHandler 调用 SkewDetector
  → 检测结果进入 AdaptiveDecision
  → Stage 保存决策，AdaptiveStagePlanResolver 消费决策
```

`MedianSkewDetector` 及其原有测试由 Planner Core 移入 Shuffle Core 的
`org.pangrova.shuffle.core.skew`。它与现有 Shuffle 统计算法同属无 SDK、无资源生命周期
的 builtin 算法；本批不为单个算法创建空家族或额外发行边界。
Planner Core 生产代码只依赖 Shuffle API，测试范围才使用真实 builtin 实现。
Server 负责实现选择；Evaluator/Handler 不内部创建默认实现、不使用静态 setter。

## 行为与边界

- 保持实际 AQE 的判断规则：超过最小字节数，并且 bytes 或 rows 超过对应中位数乘阈值。
- 偶数样本中位数采用不溢出的计算，避免接近 Long.MAX_VALUE 的均匀分区被误判为倾斜。
- 阈值在两个入口均要求有限且大于 1；不接受 NaN、Infinity，不钳制为默认值。
- split 数使用整数向上取整并限制到 64；倾斜分区建议至少 2，行数估计目标不会变成 0。
- 当前 Planner 的 SkewResult 消费热分区身份、中位数和最大值；split 建议是建议值，
  不是已绑定的执行范围，不能证明后端支持范围拆分、推测 attempt 隔离或数据容错。
- 现有 AdaptiveStagePlanResolver 仍是并行度启发式；本批不改变其算法，仅把误导性的
  `aqe_skew_mitigated` 日志改成 `aqe_skew_parallelism_heuristic`。
- 缺少 detector 时在构造时失败。没有新增仅用于满足可达性检查的无用对象。
- Stage 仍独占 adaptive decision；没有新增持久化事实、事务、状态机、线程或资源关闭 owner。

## 测试策略

原有检测、Planner handler/property 和 AQE 测试均切换到显式装配。
新增有意义的回归覆盖中位数溢出、行数倾斜、小目标建议、精确阈值以及非法 ratio。
注入替代检测器的测试断言它收到真实 Worker report 的分区值，并控制 Stage 上实际保存的
决策；真实 builtin 的集成断言直接检查热分区和 maxBytes，不再只断言“不抛异常”。

本批不修改 JNI、Native、Protocol、Persistence、Schema 或 Client 源码；各边界文件 SHA
由 [源码证据](skew-wiring-backend-evidence.json) 核对。运行时集成使用匹配源码的既有 Native 制品。

验证调度记录：首次 integration 运行尚在退出阶段时误启动 artifact 构建，后者运行
约 16 秒即被中止。两次结果均保留为诊断记录，排除出最新验收集合；随后通过进程锁
串行重跑 integration 与 artifact。该重跑是验证调度纠正，不用于隐藏产品测试失败。

实际命令、逐测试结果和失败保留在：

- [验证记录](skew-wiring-validation.tsv)
- [逐测试结果](skew-wiring-tests.tsv)
- [目标模块测试源覆盖](skew-wiring-owner-suite-coverage.json)
- [FQN 处置](skew-wiring-fqn-dispositions.tsv)
- [逐类审查与 SHA](skew-wiring-semantic-evidence.json)
- [生产差异审查](skew-wiring-production-review.diff)
- [调用与装配证据](skew-wiring-callers.tsv)
- [发行物检查](skew-wiring-artifacts.json)
- [静态检查](skew-wiring-static-evidence.json)
- [剩余装配失败](skew-wiring-orphan-evidence.json)

## 未完成事项

本批没有接通旧 SpoolingExchangeManager、LocalSpillLifecycleManager、
InMemoryShuffleDataInventory 或 DefaultShuffleByteEgress。它们的实际缺口仍按
[前批接线审查](contract-foundations-shuffle-wiring-audit.md) 处理，不增加豁免。
本轮同时核对 C++：真实 onPartitionBatch 的 long 参数是 rowCount，不能误当 sequence。
后续 Native Adapter 必须保持这个 ABI、callback 范围内 buffer 所有权和任务完成/取消语义。

通用 Shuffle session/receipt/attempt、真实 Local Provider、Native Adapter、自研 Provider
拆分、Celeborn 扩展边界以及 Generation/DAG 包环仍需继续完成。当前批次不能作为生产验收结论。
