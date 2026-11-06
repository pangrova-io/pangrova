# Indexing 编码缓冲区与裁剪提示的所有权

本批接续 Metadata durable recovery。实施前单独保存当前工作区文本文件与 SHA-256，
不以 Git HEAD 覆盖此前改动。范围是 Indexing API 的 9 项静态告警及实际评估消费者，
不是 Indexing 全域完成，也不代表全仓生产验收完成。

## 契约与调用闭环

| 类型/能力 | 修改及 owner | 调用方与资源 |
| --- | --- | --- |
| `BloomFilterIndexData`、`NGramBloomIndexData` | 构造复制输入，数组 accessor 返回独立快照；新增 `filterDataView()` 返回只读 ByteBuffer | builtin Bloom/N-gram 评估通过 view 解码。视图独立 cursor、无可写 backing array；持有视图延长原数组生命周期，无 close/native handle |
| `SplitBlockBloomFilter` | Common 仍拥有 SBBF 格式与算法；新增 `readBuffer`，不引入 Indexing 依赖 | 按输入 remaining 解码、保留原 position/limit/mark/order，固定 wire big-endian；只保留新 int[]，不保留输入。可写输入的调用方不得在解码中并发修改 |
| `BitmapIndexData` | 输入和快照复制嵌套 byte[]；新增 `containsKey` | builtin EQ/IN 不再调用整份 map accessor；serializer 只获取一次快照。partial 的 UNKNOWN 语义保持 |
| `BoundingBox` | 坐标数组只返回快照；新增维数及单坐标读取 | R-tree 构建/序列化按维读取，几何运算直接使用私有坐标；没有每次比较复制坐标数组 |
| `RowGroupSkipInfo` | 拷贝输入及输出 BitSet；拒绝负 total 和超出 total 的已跳过位 | `NONE` 无法被 accessor 污染；未知 group 仍保守返回可读，不将裁剪提示误当文件边界或授权 |
| `ColumnRangeSummary` | 可选 digest 构造和 accessor 都复制 | 当前 `canPrune` 只使用 min/max，不声称 digest 已接入查询运行链 |
| `BTreePage`、`GinIndexData` | 明确不可变容器赋值，保留嵌套 Integer 列表复制 | 解决分析器对分支/辅助函数赋值的识别问题；测试同时覆盖外层及内层修改拒绝，无豁免 |

没有新增 JAR、SDK、Provider、配置开关、数据库或迁移路径。API 新增签名仅使用 JDK
类型。原有 byte[] accessor 仍有真实用途：取得调用方拥有的可写副本；不是旧 FQN 转发壳。
Builtin Provider 工厂、Core 的策略选择与缓存入口保持不变。

`readBuffer` 在数组分配前用 long 校验块数对应字节数，拒绝负 inserted count、溢出和
截断。`readFrom(byte[])` 复用同一解码路径，避免 null 调用因重载变得歧义。有效 SBBF
编码未改变，未手改 Proto/JNI/Native 生成文件。仍按原约定允许 body 后额外字节。

## 验证与边界

原先断言“数组 accessor 返回相同实例”的测试已替换为修改隔离测试，没有删除对应
行为覆盖。新增真实 Provider 的 build → snapshot mutation → evaluate → serialize /
deserialize 回归，覆盖 Bloom cached/uncached 一致性、N-gram 不出现假阴性、Bitmap
partial 不错误 SKIP。只读视图测试覆盖 array/put/slice 写入拒绝及独立 cursor；Common
解码覆盖 heap/direct/read-only 切片、不同 byte order、输入释放后的独立数据、截断及溢出。
这里的“输入释放”是调用方可修改/不再持有输入，不是显式释放 direct buffer。

首次定向测试 75 项通过。首次正式 API 静态验证运行 762 项测试通过，但 BTree 可空
列表还剩 1 项告警，命令退出 1。改为显式 null/不可变赋值后，第二次正式验证退出 0，
762 项测试通过；Common 和 Indexing API 的 Checkstyle/SpotBugs findings 为 0。
分析器仍输出 missing-class（lambda/record 方法式名称）诊断，完整保留在机器证据中，
不能把零 findings 解释为所有字节码路径已被完整分析。

随后全 reactor `clean install` 的 758 项选定测试通过，包括相关 Native SQL E2E；
builtin 单模块正式静态 `verify` 的 81 项测试、Checkstyle 与 SpotBugs 通过。
这些数量包含重复执行，不得相加作为独立用例数。最新去重结果、最终 clean 打包及
发行核对见 `index-buffer-ownership-validation-summary.json` 和对应证据文件。

最终再次全 reactor `clean install` 退出 0，167 项补验通过；本批最新去重报告共
1,282 项，失败、错误、跳过均为 0。Common 与 Indexing 各模块的 97 个测试文件
均有本批执行证据；其他格式调整采用针对性消费者测试，未宣称其全部模块测试通过。
这仍是选定回归，不是全量生产回归。静态 API/builtin 的正式报告已在 clean 删除 XML
前保存，源文件哈希保持一致；分析器 missing-class 诊断也在证据中保留。

发行审查：110 个 JAR、110 个独立模块名、853 个编译包无冲突；14 个受影响制品的
1,761 个 class 与打包字节一致。Coordinator/Worker/Standalone 实际启动 classpath
分别为 262/183/325 个条目，受影响依赖与最新制品哈希一致。三个 Native 产物哈希
一致，本批没有 C++/JNI/Proto 源码输入变化，不新增 Native 构建或 ASan 通过声明。

## 依赖门禁清理与未通过项

Catalog Provider 的完整 `-am verify -Pstatic-analysis` 越过 Indexing API 后，依次
发现 Protocol、File API、Persistence File/Coordination/Security、Shuffle Local/Core、
Observability Core/SDK、Config、Testing Kit 的既有样式问题。本批只调整长声明、
Javadoc、无用导入、接口冗余 public、try/catch 排版及等价分支括号。没有改变权限、
fencing、事务、资源关闭顺序、异常抑制规则、配置值或故障注入条件。

诊断命令也遵守原有失败策略，遇到错误即非零退出；每次失败均保留，未使用跳过、
降低严重性或放宽阈值。第 11 次样式诊断完成整条依赖链，退出 0。

第三次正式 Catalog 静态验证仍退出 1：`pangrova-shuffle-local` 的三个可能空值路径
及一个 `createTempFile` 路径来源诊断尚未处理。它们来自受 owner 控制的路径构建，
需要进一步验证父目录边界、symlink/并发替换及故障语义；本批既不宣称是已确认漏洞，
也不将其压制为已证安全。具体方法、行号和分析器诊断已记录在
`index-buffer-ownership-open-static-findings.json`。本次命令没有执行到 Catalog Provider
测试，不能将它计为该 Provider 的完整静态验收。

Protocol 仅心跳构造器换行，未改变可执行 token、字段或 wire 格式。其余 Native/JNI/
Proto 输入没有修改。Java 源码目录中的 NativeMetricsPublisher 只补构造器 Javadoc，
不构成 JNI 或 C++ 行为变化。最终资源生命周期和契约测试覆盖这些改动的消费者。

## 仍需继续处理

- `Comparable<?>` 不等于不可变值。BTree/Sparse/Bitmap 的 key 及动态范围 min/max
  仍需建立实际类型与值语义契约，不能仅靠复制容器宣称深层不可变。
- builtin `CompositeKey` 被置入公共 BTree 数据的 Comparable 字段，而通用 codec
  尚无该类型的编码。后续应核对多列建索引的真实入口、序列化与查询匹配后补齐；
  不允许为通过类型校验而删除既有多列能力。
  后续处理见 [多列键契约与编码](btree-composite.md)，此项保留为本批发现时的记录。
- 含数组 record 的结构 equals/hashCode、BoundingBox 非有限/反向坐标、Hash evaluator
  的快照分配和 N-gram 反复解码仍需按真实需求审查。本批不宣称已完成这些问题。
- `IndexEvaluationService` 当前也仅构造而无实际业务调用，详见 Indexing README。
  builtin 生产实现的组件测试与 Native SQL E2E 是不同证据；后者不证明 Java
  builtin 评估已运行。
- `RowGroupPruner`、`DynamicFilterSummary` 本批源码调用审查未发现生产调用链；
  它们的组件测试不能证明 SQL → Worker 的运行时装配。真实 Planner/Worker 动态
  过滤另有链路，需独立验证并在后续决定这些契约的归属/实现。
- 未进行同环境基准比较。“评估不新增编码 byte[] 的整块复制”由路径和源码支持，
  不等于零分配、零拷贝或吞吐提升。解码仍分配 int[]，数组快照和序列化仍有复制。
- Indexing Persistence/Core 拆分、其他领域、Shuffle lifecycle/receipt/背压、真实
  Celeborn Provider、全平台及全量生产回归仍属于未完成的总体计划。
