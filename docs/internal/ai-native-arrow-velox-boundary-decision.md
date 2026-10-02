# Arrow / Velox 边界决策与实证要求

调研复核日期：2026-10-02。本文是当前边界约束与验收设计，不是性能实测报告。
适用链路：Calcite → Substrait → Native Velox → Arrow，包含结果、shuffle、Lance、
Embedding 与多模态；不引入第二执行器、第二传输格式或运行时 fallback。

## 1. 结论

Arrow 是唯一互操作和数据面传输契约，Velox Vector 是任务内唯一执行表示。
同进程、布局相容的 buffer 通过 C Data Interface 共享并显式移交生命周期；
跨进程/机器采用标准 Arrow IPC/Flight。禁止为了形式上的“全程 Arrow”在每个
Velox 算子之间重复导入/导出，也禁止声称跨机器 IPC、TLS、压缩绝对无拷贝。

依据：Arrow 明确把 C Data Interface 的共享范围限定为同进程，不负责跨进程
持久化；IPC 才承担跨进程与跨机器交换。
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html#goals)。
Velox 文档明确其向量的编码以及字符串、ARRAY/MAP 布局与 Arrow 不同。
[Velox Vectors](https://facebookincubator.github.io/velox/develop/vectors.html)。

这是选择各框架原生职责，不是允许双路径：一个 Task 只在 Velox 执行；出入边界
只有一套适配器；失败不改用私有 serde、JVM 算子或另一引擎重新执行。

## 2. 实际固定版本与布局证据

当前工作区 Velox checkout 为 `3da0390118fa4b0addbfbdeb03f438ec1b1237a6`，
Java Arrow 版本为根 POM 的 `19.0.0`。上游最新文档用于解释契约，实际行为必须以
固定 SDK、真实导出数组与接收端共同复证，不能把最新 API 当成已安装能力。

| 对象/边界 | 合理策略 | 已识别的布局限制与需要测量的工作 |
|---|---|---|
| 任务内 filter/join/unnest | 保留 Velox dictionary/constant，沿用原生表达式求值 | 不为统一传输而在内部展开字典；共享索引和原 buffer 的生命周期仍计入 Task 内存 |
| 连续 fixed-width flat 向量 → C Data | 实际 SDK 允许时共享值 buffer | 有 selection/gather、编码展开或位图处理时可能分配；必须逐 buffer 检查，不能按类型全称零拷贝 |
| 字符串/BLOB → 标准 UTF8/Binary | 目前唯一边界采用相容的 Arrow storage 布局 | Velox 非连续 StringView 转连续 payload/offsets 会复制；按真实字节计账，不另写字符串编码器 |
| ARRAY/MAP/ROW 与 VECTOR | 使用标准嵌套 Arrow 类型和统一逻辑类型 authority | offset/stride、NULL parent、sliced child、固定维度布局需校验；必要重排不能丢维度、可空性或 FLOAT/DOUBLE 精度 |
| TIMESTAMP 与短 DECIMAL | 固定 SDK 的明确类型转换 | 两字段 Timestamp 到 Arrow 单 int64、短 decimal 到 decimal128 涉及转换；单位/时区/精度由同源类型契约裁决 |
| IPC/Flight/持久化 shuffle | 一个 typed schema 与完整多 batch IPC stream | 序列化、压缩、网络及持久化分开统计；禁止 BINARY 列包私有 page、四字节自建 frame 或只读第一个 batch |
| JNI/Java/客户端 | 共享或消费同一 typed Arrow 契约 | `byte[]` 全量聚合、Object[] 行化及重复编码都必须实测；不是 C Data 指针传递即可宣称全链零拷贝 |

固定源码依据：
[Bridge.h](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/vector/arrow/Bridge.h)、
[Bridge.cpp](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/vector/arrow/Bridge.cpp)。
`exportValues` 只在符合实际条件时共享；`exportStrings` 分配 offsets 并复制 payload；
`exportArrays/exportMaps` 和 `exportValidityBitmap` 必须同样纳入检查。

## 3. 不盲目启用 StringView 或动态字典 wire

Arrow 已有 Binary/UTF8 View 布局，但“格式存在”不代表所有已固定的 Java、Rust、
Lance、IPC/Flight 接收端以及 SDK 生命周期行为都已贯通。
[Arrow Binary View layout](https://arrow.apache.org/docs/format/Columnar.html#variable-size-binary-view-layout)。

固定 Velox `Bridge.cpp` 的 `exportFlat → exportValues → exportViews` 路径还需要
真实保源 oracle：前者可能共享原 values buffer，后者对非 inline 字符串的 view
描述符进行 `const_cast` 并把指针改成 buffer index/offset。当前源码有这一风险证据，
尚未执行到真实破坏结果；不得猜测已安全，也不得在没有复证时启用该选项。
验收必须在导出前后、释放导出后继续读取原向量，并验证长/短/NULL/sliced/dictionary
字符串、跨线程最后释放以及 ASan。若必须修改 SDK，采用仓库管理的固定补丁和同源
完整验证，不编辑机器缓存，不增加备用编码路径。

当前 wire 固定为逻辑 storage schema，任务内字典仍保留。边界展开字典是当前确定
schema 的设计取舍，**不是物理上不能避免**：标准 Arrow dictionary 可在统一 schema
下使用，但要求完整 dictionary ID/memo/delta 生命周期和所有接收端一致实现。
是否更换这一个 canonical encoding，必须比较低/高重复率、字典 churn、NULL/嵌套、
跨 batch、多 producer、内存峰值及网络字节；禁止加旧/新 wire 开关或 negotiation。
未实测前不能宣称当前展开方式性能最优。
[Arrow IPC DictionaryBatch](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoding)。

## 4. 生命周期与资源隔离

Arrow release callback 持有实际 vector/buffer owner，消费者最终释放后才能解除
Task pool、transit credit 和结果保留 grant。Task FINISHED、异步 Future done、从 Map
删除任务都不是实际资源已归还的证据。取消应先撤销准入与读写权限，再等待真实 I/O、
reader、buffer owner 终止；失败保留追踪和额度以供重试，不提前退款。

当前结果链已发现并在修复的根因包括：FINISHED 后删除 output authority、未消费
结果提前释放 memory grant、terminal retention 清理只删 Java 记录而不 release Native。
修复源码不等于验收，必须以真实 Task/Flight/allocator 验证 read-after-FINISHED、
慢消费者、EOF、显式释放、期限清理和并发取消。

## 5. 指标与可执行验收

同一 source/build/proto/capability cohort 记录每个边界的实际输入/输出字节、共享值
buffer 字节、布局转换字节、IPC 编码/解码字节与 CPU、压缩字节、网络字节、峰值
retained/transient bytes、grant 等待、drain 时间。浅复制 descriptor 不计 payload copy；
新增输出/offset/位图等分别计分配与复制，避免 double count 和把网络字节混成拷贝字节。

真实验收包含：固定宽度/sliced/NULL、全部复杂类型、VECTOR 两种元素精度、字典
变化、多 batch、零列非零行与零行 schema、原 buffer 先释放、最后 owner 跨线程释放；
真实 result/direct shuffle/durable spool/receiver 共用同一 codec 的值与生命周期。
在相同机器、数据和并发下比较吞吐、P50/P95/P99、copy bytes、CPU、峰值内存；再在
多 Worker、慢 consumer、丢连接与 Worker 故障下验收。没有这些数据，不承诺性能优势、
全链零拷贝或生产完成。

## 6. SQL 前端不是本次边界决策的豁免

当前 `PangrovaParserFactory` 实际仍通过 ANTLR AST 转 Calcite SqlNode，合法
`FROM (VALUES ...)` 专项已经失败。该问题不能用改 SQL、只修 ANTLR 一个分支或捕获
异常转到 Calcite 的第二路径关闭。应以固定 Calcite 的 `config.fmpp/parserImpls.ftl`
生成唯一方言 parser，将 DDL/DML/时间旅行/元数据操作及全部消费者原子迁移，保持
原合法和负例 oracle，随后移除旧 grammar、AST-to-SqlNode 旁路和无效 parser factory。
[Calcite parser extensions](https://calcite.apache.org/docs/adapter#extending-the-parser)。

尚未完成这一完整迁移、实际 Native 构建与完整 Step0–8，本文不能作为这些门槛的
PASS 证明。Arrow/Velox 边界清晰也不等于未来不会存在任何缺陷。
