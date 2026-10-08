# Lance 当前架构核实、根因修复与验收

日期：2026-10-07。范围为数据库内核的 Lance Dataset 和 Paimon Lance 物理文件。
本报告依据当前工作树与固定发布版本，不把历史日志、组件成功或测试源码当作完整生产验收。
保留数据库通过独立推理服务执行模型调用的边界；本单元不改变 AI 功能。

## 1. 官方能力与 connector 决策

| 对象 | 固定版本官方事实 | Pangrova 决策 |
|---|---|---|
| Iceberg 1.11.0 | `FileFormat` 只有 Puffin、ORC、Parquet、Avro 和 metadata；标准没有 Lance 数据文件。 | 不把 Lance 伪装成 Parquet 放入 Iceberg manifest。源表与检索 artifact 的所属权必须明确。 |
| Paimon 1.4.1 | 有 `paimon-lance` format factory。`LanceWriter` 调用 `LanceFileWriter.open`，写一个物理文件；Paimon 自己提交 snapshot。 | Lance 物理文件复用 Paimon connector、schema、split、CommitMessage 与 snapshot 权威，不依赖 Dataset connector。 |
| 独立 Lance Dataset | Dataset 有自己的 version、fragment 与 manifest；不是 Paimon 单文件。 | 保留 Dataset metadata connector，用来注册真实 Dataset SQL 表、固定版本与发布目录可见性。 |

官方依据：[Iceberg 规范](https://iceberg.apache.org/spec/)、
[Iceberg 1.11.0 FileFormat](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/api/src/main/java/org/apache/iceberg/FileFormat.java)、
[Paimon 1.4.1 LanceFileFormatFactory](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/LanceFileFormatFactory.java)、
[Paimon 1.4.1 LanceWriter](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/jni/LanceWriter.java)。

Paimon 官方插件固定 Lance Java SDK 0.39.0，见
[发布 POM](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/pom.xml)；
Pangrova Rust FFI 的依赖是 0.20，当前唯一 C ABI 为 12。
现有仓库已经包含 `paimon-lance` 依赖及 Native physical reader/writer，不能重复沿用
历史“Paimon 不支持 Lance／插件未接入”判断。版本兼容必须用真实双向互读证明。

两种 metadata authority 共享唯一 canonical v2 TaskRequest、Native 执行、Arrow C Data 和
Flight；binding 由源数据语义确定，运行失败不得切换 authority 或 reader。不同来源的
connector 扩展并不等于同一输入的两条执行路径。

## 2. 当前代码差距

| 接缝 | 源码事实 | 必须落实的修复／验收 |
|---|---|---|
| Dataset EMPTY snapshot | `LanceMetadata.bindRead` 固定 EMPTY，但原 `LanceSnapshotResolver.resolve` 在另一查询首次发布后拒绝该旧句柄。 | 保留已选空快照，同时保留租户、warehouse、table incarnation 校验；本次修复。 |
| Dataset 最终可见性 | `LanceCatalogStore.publish` 对 table/base/manifest 做 PostgreSQL CAS，没有在该事务中验证 execution owner。 | 目录可见性原子更新必须验证当前授权及精确 durable intent，不能用 RPC 前后的检查替代。 |
| Paimon 最终可见性 | `PaimonDistributedWriteFinisher.commitPrepared` 使用 `InnerTableCommit.filterAndCommit`。上游 `SnapshotCommit.commit(snapshot,branch,statistics)` 没有 Pangrova owner 条件。 | 在真实 catalog-managed snapshot 的原子发布点接入 execution fence 和 intent identity。 |
| physical write 范围 | `PaimonPhysicalWriteAllocations.requireAppendLayout` 仅允许无 PK、无分区、固定一个 bucket append；`PaimonNativePhysicalWriteProvider` 只接受 INSERT。 | 扩展同一 Native TableWrite 的分区、bucket、PK/RowKind/sequence、排序/merge/spill、rolling 与 mixed file groups，不能转交 JVM rows。 |
| DML 分配 | `PaimonWriteLifecycle.insert` 分配 Lance physical binding；overwrite/delete 没有相同分配。 | 逐项追踪实际 sink 与 worker 准入，在现有契约内接通，不能将 append 成功称作全部 DML 完成。 |
| physical 文件清理 | `abortUncommittedFiles` 仍因缺 durable non-publication disposition 抛错。 | UNKNOWN 保留对象；只有最终提交权威证明未发布／已终止才授权删除。清理失败必须可恢复且可观察。 |
| 元数据分页 | 原 `scan.rs::enumerate_page` 每页重新收集并排序全部 fragment；本次已改借用 SDK 有序 slice 与精确 ID 二分游标。SDK manifest 解码和逐页完整顺序检查仍为 O(N)。 | 不再重复排序或额外分配全部 fragment 引用；完整元数据加载与长期缓存预算仍需实测，不能由 bounded page 推导全部内存有界。 |
| Arrow 边界 | reader 使用 C Data 导入，但 layout normalization 有实际拷贝；writer flatten dictionary/constant。 | 记录实际 copy bytes 与 allocation，保留所有权；不能声称端到端全部零拷贝。 |
| 谓词 | `PangrovaVeloxConnector` 未将 Velox 表达式字符串交给 Lance SQL；精确 FilterNode 在 Velox 执行。 | 增加可证明等价的 typed predicate 下推能力及实测，禁止另建 parser 或失败降级。 |

Paimon 最终原子接口官方依据：
[SnapshotCommit 1.4.1](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/SnapshotCommit.java)。
该审计发现的是检查与可见性之间的竞态窗口，不声称已观测到旧 owner 实际发布。

## 3. 本次独立根因修复

具体触发：查询 A 在新建 Dataset 表绑定 `version=0 / EMPTY`，查询 B 经 public SQL
INSERT 发布 version 1，查询 A 随后调用 SplitManager。原代码把 A 的空快照当成要求
当前目录仍为空，抛出 `Unpublished Lance table handle is stale`；这违反绑定快照隔离。

`LanceSnapshotResolver.resolve` 在现有 catalog/warehouse、tenant 和 table incarnation
检查之后，对 version 0 返回原 selector。SplitSource 保持立即 finished、零分片，统计与
schema 仍来自同一个 selector。新查询通过当前目录绑定 version 1，读取新增数据。
没有向 Native 创建 version 0 manifest，没有刷新旧句柄、合成 rows、捕获错误或切换路径。
DROP／同名重建仍拒绝旧 incarnation，跨 tenant 仍拒绝。

修改范围：Dataset resolver、一个 connector integration test、public SQL publication test，
以及 test-scope 的官方 Lance Java SDK 0.39.0 oracle 依赖。没有修改共享协议或生产 SDK 版本。

## 4. 最近验证与证据边界

`LanceEmptySnapshotIntegrationTest` 使用真实 PostgreSQL catalog 和官方 Java SDK，创建
真实 version 1 manifest、读取实际字节、计算 digest、提交真实目录事务，再验证原空 binding
的 metadata 与零分片。它不提供伪 NativeDatasetControlPlane，不证明 Native 非空读取。
目录测试的 schema fence 从实际 Arrow schema message 生成；public SQL 回归另行覆盖真实
Native schema/control、写入、journal 和读取，不以目录 oracle 代替该链路。

`LanceSqlPublicationE2ETest.selectedEmptySnapshotStaysEmptyAfterFirstPublicSqlPublication`
使用真实 CREATE／INSERT／SELECT，断言新查询读到 7、当前版本为 1、旧绑定仍为空，并
验证 DROP／同名重建后旧 binding 拒绝。`authenticatedCatalogSession` 对应实际 HTTP actor
的 tenant/principal，query authority 为空，用于直接访问同一个 production catalog provider。
旧 EMPTY binding 的 SplitManager 验证是生产 connector 边界；它没有制造 Native binding，
也不需要对不存在的 version 0 manifest 发 RPC。这个测试不证明同一挂起的 public SQL 查询
完成调度；实际 public SQL 用于验证相邻 CREATE／INSERT／新查询链路。已有 publication、
manifest digest、journal 和 append/overwrite 断言全部保留。

本单元执行了 `git diff --check`，未启动 Maven 或 Native 构建；构建由根任务统一排队。
新增测试仍为待执行，不能记为 PASS。选择器：

```text
LanceEmptySnapshotIntegrationTest
LanceSqlPublicationE2ETest
LanceSqlDdlSessionE2ETest
PaimonLanceNativeReadE2ETest
PaimonLanceNativeWriteE2ETest
```

Lance Native／Rust／connector／SQL测试源码定向检索未发现 runtime-unavailable skip、
`@Disabled` 或 `GTEST_SKIP`。历史 SKIPPED 记录不能改写为当下通过证明。
之前 public SQL 报告的真实阻断为 quota=0 HTTP429 与默认 namespace 重复创建；当前共享
bootstrap 已有其它工作树修改，旧报告不能覆盖修改后的实际结果。

## 5. 完整提交与生产验收规格

最终 catalog authority 在同一次原子可见性更新中验证 current execution fence、精确 intent
digest、table incarnation、schema/generation 与 base snapshot，并持久保存可查询的结果。
恢复只能更换当前授权，不得改写原 winner provenance；超时／ACK 丢失为 UNKNOWN，按同一
fingerprint 查询真实结果，不能重新分配身份发布第二份写入。

Paimon physical write 的后续完整扩展必须采用真实 SDK schema/options/path/snapshot 的
metadata authority。Native 唯一 Velox sink 完成输入默认值／RowKind、partition/bucket、
sequence、排序／merge／spill；Arrow batch 进入唯一 physical encoder。扩展现有有界
allocation/receipt，实际 key bounds、level、delete count、writeCols、mixed file-set、stats
都由 SDK commit adapter 消费；每个 partition/bucket 只有一个 winning producer。

真实 SDK 差分覆盖 DEFAULT/MOD/HIVE hashing、PK deduplicate/partial-update/aggregation、
delete reset、sequence.field/tie、schema rename/name reuse/widening、32/64 位 DV、
多分区、多 bucket、mixed files、零输入与 empty overwrite。SDK 仅做测试 oracle，禁止生产
转交行数据让 SDK writer 代执行。

发布验收使用真实 PostgreSQL、MinIO、两个独立 Coordinator 和多个独立 Worker，覆盖检查
后接管、过期 RPC 迟到、ACK 丢失、同意图重放、不同 digest 冲突、ABORT 并发、重启恢复、
OOM、取消、upload/drain 与删除失败。逐项检查最终 snapshot、manifest digest、journal、
实际行内容、对象保留／删除、资源释放，不以最终行数或第二次成功替代提交不变量。
先执行最近专项，再完成当前同源 Step 0–8 和 ASan；历史 Release/组件 PASS 均不折算。

## 6. 第二个独立单元：按官方 manifest 顺序分页

此前全量排序是重复工作而非 SDK 要求。Lance 0.20 官方 `Manifest.fragments` 明确规定
按 fragment ID 排序并允许间隙，见
[固定版本 manifest 定义](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/format/manifest.rs)。
ID 是格式身份，不能当作 dense array index；删除或 compaction 后可以产生间隙。

`scan.rs` 现在直接借用已固定版本的 SDK fragment slice，先在本次请求中一次检查严格升序，
再对精确 cursor ID 做 binary search，取至多 pageSize 个原 slice 元素。重复或乱序 manifest
失败关闭，不用排序修复损坏元数据；不存在的 cursor 仍拒绝。该单元消除了每页 O(N) 引用
向量分配和排序，游标查找为 O(log N)，分页描述符为 O(pageSize)。它没有改变协议、引入
第二份 manifest cache 或 reader，也不声称消除了 SDK 的 O(N) manifest 加载及顺序检查。

`scan::tests` 新增两个真实 SDK oracle：写出六个单行 fragment，用 SDK delete 发布
真实间隙 `[0,2,4,5]`，按两页验证 coverage、游标、manifest/schema digest；新版本发布后
旧 version 1 的下一页仍是 `[2,3]`；真实删除全部后返回 empty page。另从 SDK 实际 metadata
复制并破坏 ID 顺序／唯一性，验证拒绝，不修改真实文件或替换 SDK writer。测试同时断言
返回 slice 指针来自原 manifest、非法 page size 拒绝、缺失 cursor 拒绝。

本单元源文件经已安装 pinned Rust 1.98.1 的 rustfmt 和 `git diff --check`，未运行构建。
根任务必须在这个输入上重新构建 Rust FFI、重新链接／打包 Native，再运行完整 `--lib`
及 Native/JNI／SQL链路；先前启动的旧输入构建不作为新单元证据。

## 7. 第三个独立单元：read-side copy 指标贯通

`LanceBatchReader` 累计 layout normalization 的 copied bytes／offset allocation，
但两个 getter 在 Native 生产源码中没有 consumer；`PangrovaVeloxDataSource` 不导出
`getRuntimeStats`，reader 在 EOF／换 split 时释放。MOR 的 Lance wrapper 也没有统计
传递。因此存在 getter 不等于 ResourceUsage 已计入读取成本。

固定 Velox `3da0390118fa4b0addbfbdeb03f438ec1b1237a6` 的 `Driver::CALL_OPERATOR`
在调用 TableScan 时设置 `RuntimeStatWriterScopeGuard`，见
[固定版本 Driver](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/exec/Driver.cpp) 和
[RuntimeMetrics 接口](https://github.com/facebookincubator/velox/blob/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/common/base/RuntimeMetrics.h)。
单纯 DataSource getter 不足：TableScan 只在 noMoreSplits 合并它，失败／取消可能不到该位置。

现实现从每次真实 `normalizeImportLayout` 得到实际 byte 增量，在检查 conversion status
之前发布到当前 Velox operator runtimeStats。普通／MOR reader 共用 importBatch，reader
释放不丢失已发生成本；生成 fixed-list offsets 用独立 runtime metric，不能算作 payload copy。
`NativeMetricsCollector.observeColumnarReadTaskStats` 先加总所有实际 operator 的读取成本，
再以 task 累计最大值防止重复快照计费。它与原物理写入累计计数分离，最终 snapshot 求和，
不能将两个独立 reader 或 read/write 的局部累计值取 max。

`PlanExecutionDriver.collectTaskStats` 对同一个实际 TaskStats 先归集读取成本再序列化；
release 在真正删除 Task 前保存 final stats。`StatusSerializer` 先刷新实际统计再采样
collector，将结果放入已有 `TaskReport.usage.columnar_conversion_copied_bytes`，没有新增
协议权威或 wire 字段。NativeTaskStats 没有同名字段，不能把它伪装成 per-operator proto 指标。

新增真实 oracle：两个实际 Dataset fragment、nullable BOOLEAN slices，从独立 C ABI Arrow
batch 归一化测出精确 expected copied bytes，必须大于零；real TaskExecutor/Velox scan 的
runtimeStats、collector、TaskReport 与之相等，跨 split 相加，重复序列化／release 不多计。
取消测试在实际输出背压且发生 copies 后取消，验证最终 report；后续 split 失败专项使用
真实 parallel Velox Task +正式 Consumer，保持 split queue 开放，真实输出全部 32 行后
才添加不存在 Dataset（固定 SDK serial next 要求 noMore，不能用于 phased add），
验证 FAILED TaskStats→collector 及 Task 删除后重复观察稳定。另独立最终失败报告专项
先经 blocking Sort 完成全部实际 scan，再让真实 BIGINT divide-by-zero 失败，证明实际
TableScan rawInputPositions=32，然后检查 TaskExecutor→TaskReport FAILED、重复与 release
后精确成本。physical SDK writer 和同一 physical file sliced reader 使用 16 行 fixed-list 加真实
BOOLEAN 列；NULL ARRAY 第7行不携带dimension slots，使官方Rust适配必须materialize
并concat（连续dimension offsets仅共享，copiedBytes=0正确）。独立write oracle先导入
同一真实Velox Arrow批，验证NULL零slots，再用正式Arrow Concatenate量实际输出buffer
lengths，与实际SDK写入成本精确相等且非零。直接检查 C ABI 导入的 BOOLEAN bit offset、真实归一化 copiedBytes、
生成的 fixed-list offset allocation，read+write 精确求和；offset allocation 不算 copies。
这些是 Native component/integration oracle，不声称是 JNI／public SQL 多进程证据。

专项选择器：

```text
LanceFilterExecutionTest.ActualCopies*
LancePhysicalCopyMetricsTest.ActualReadAndWriteCopiesSumInTaskReportAfterRelease
NativeMetricsCollectorConnectorTest.*
StatusSerializerTest.*
```

根任务首次 Release 全量 4067 项中 4065 PASS，两个新增 oracle 失败：并行 driver 在另一
fragment 扫描完前取得 missing split，取消了仍在读取的 driver，实际成本 12 而全量 oracle
为 24；三行 nullable fixed-list 物理文件的真实 decoder 重基 primitive/validity buffers，
生成 offset allocation 而 copiedBytes=0。两者是 oracle 先决条件错误，不能改生产计量、
把 allocation 当 copy 或降断言。上面的 phased split 与真实 BOOLEAN offset 测试修正了
先决条件，额外 final FAILED report 专项补齐链路。根任务最近专项5项中，success、cancel、
phased actual missing、blocking Sort final FAILED四项PASS；physical新对齐输入的write
copiedBytes=0再次正确拒绝了错误先决条件。已按官方physical_schema.rs:91–120修正为
缺slot的NULL ARRAY并增加独立正式Arrow buffer oracle，生产计量没有改动。此次仅执行
`git diff --check`。第二次physical专项在新增独立oracle释放阶段崩溃，匹配ASan堆栈
确认Arrow PoolBuffer析构通过borrowed MemoryPool访问已释放ColumnarMemoryPool；
是test中slots比allocator先声明导致逆序释放的fixture UAF。已将所有slots和concat
payload放入allocator之后的inner scope，销毁后验证allocator.bytes_allocated=0、
实际task pool恢复进入该scope前baseline（保留原input/export buffer，不误算释放）。
全部成本／offset／精确sum断言保留；最新physical输入须由根任务重新Release／ASan
构建执行，当前不标记PASS。

## 8. 最终发布 fence 的具体阻断与落实边界

Dataset 原子权威是 `LanceCatalogStore.publish` 的 PostgreSQL transaction（lockTable、
requireCurrentBase、updateTable、insertCommit、insertVersion）。其参数只有 output、
fingerprint、version、manifestDigest、rows；当前不能在同一事务中判断 execution owner。
必须让该实际 transaction 使用同一 durable authority 中的 owner/lease 与 commit intent
记录，锁定精确 intent，并在更新可见版本时验证当前 owner fencing token、expiry、ABORT
状态、table incarnation、base 和 digest。若 owner 记录来自另一个数据库，增加 RPC 检查
或长持本地锁仍不能使跨库发布原子，须先把最终授权放到该 catalog authority 的事务内。

Paimon 当前最终权威仍为 `InnerTableCommit.filterAndCommit` 到真实 SnapshotCommit。
固定 SDK SnapshotCommit 方法没有 execution fence 参数；仅修改 coordinator journal 或
在 SDK RPC 前后 validate 不能关掉 delayed-old-owner 窗口。必须让真实 catalog-managed
snapshot compare-and-publish 消费完整 fenced intent，同时所有合法 writer 经过同一
authority。SDK adapter 需要真实事务实现、注册／调用链和 PostgreSQL+object-store+
两个独立 Coordinator 的接管 oracle，不能先加空 SPI 声称完成。当前 append 产品子集
与该最终授权缺口是不同事实，不能以 physical round trip 成功证明 final commit 安全。

两者在 ACK 丢失时按原 fingerprint 查询实际发布结果；查询无法确定就保持 UNKNOWN，
保留对象并等待恢复。发布记录保留原 winner provenance，接管只替换恢复授权。只有同一
最终 authority 持久证明未发布且已终止，才发对象删除 disposition。验证必须暂停旧 owner
于最终 compare-and-publish 之前、让新 owner 接管、再放行旧调用，观察实际目录/snapshot
拒绝及 journal 恢复；普通调用前的权限拒绝测试不覆盖这一竞态。

## 9. 固定 SDK 原子接点与完整落实链

以下是只读研究规格，未修改生产源码、构建或声明 final fence 已完成。Paimon API 同时
以本机官方 Maven 1.4.1 sources.jar 和固定发布源码交叉核实。

| Paimon 1.4.1 接点／源码行号 | 实际行为 | 对 final fence 的含义 |
|---|---|---|
| SnapshotCommit 29–30 | `commit(Snapshot,String,List<PartitionStatistics>)` | 原始参数没有 owner／intent；实现必须取得完整、认证的 commit envelope。 |
| CatalogEnvironment 126–138 | supportsVersionManagement=true 选择 CatalogSnapshotCommit，否则 RenamingSnapshotCommit+CatalogLock/empty lock。 | 必须明确唯一实际 publication authority，不能仅装饰上层 connector。 |
| CatalogSnapshotCommit 42–46；Catalog 688–693 | `catalog.commitSnapshot(identifierWithBranch,uuid,snapshot,statistics)` | 正式扩展点保留 catalog-issued table UUID，但 server 必须真正执行授权与可见性同事务。 |
| RESTCatalog 404–423；AbstractCatalog 605–606 | REST 支持 version management，默认 AbstractCatalog 不支持。 | REST 是 transport，不能由配置 REST 推导 server 有 Pangrova fence。 |
| RenamingSnapshotCommit 49–72 | lock 内检查 snapshot 文件不存在，tryToWriteAtomic，成功后更新 latest hint。 | 文件版本互斥没有 current execution epoch 条件。 |
| JdbcCatalogLock 52–59；CatalogLockFactory 28 | 按 catalog/database/table 获取锁、执行 Callable、release；JdbcCatalog 没有覆盖 version management。 | 锁互斥不能撤销旧 owner，不能使 SQL owner 更新与 remote object-store 操作原子。 |
| FileStoreCommitImpl 1009–1013、1045–1048 | PreCallback 在 commitSnapshotImpl 前；PostCallback 在成功后。 | PreCallback 仍有检查后窗口；PostCallback 不能撤回已发布 snapshot。 |
| RESTApi 665–678；CommitTableRequest 36–57 | 请求只有 tableId、Snapshot、statistics。 | 没有标准 currentOwner 参数；不可只传原 winning attempt 或 commitUser 冒充恢复授权。 |

官方依据：
[CatalogEnvironment](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/table/CatalogEnvironment.java)、
[CatalogSnapshotCommit](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogSnapshotCommit.java)、
[RenamingSnapshotCommit](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/RenamingSnapshotCommit.java)、
[最终 commit/callbacks](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java)、
[REST request](https://github.com/apache/paimon/blob/release-1.4.1/paimon-api/src/main/java/org/apache/paimon/rest/requests/CommitTableRequest.java)。

实际 receipt→commit→query pin 链：

1. `PlanExecutionWriteSinks.cpp` 创建唯一 physical sink；实际 SDK encoder 产生 allocation、
   TaskAttempt、checksum、physical rows、encoding receipt。
   `PhysicalWriteTaskReportValidator.java:27–28` 对 submit binding 与真实 receipt 验证。
2. `NativeTaskWriteCompletion.java:65–111` 持久化 exact fingerprint intent，catalog 调用前后
   检查 owner；`commitCatalog:116–132` 调用实际 connector。这是 redo 保障，检查没有进入
   实际 catalog 的可见性事务。
3. `WriteCommitEntry.java:175–178` 生成 `DistributedWriteCommitContext`，丢掉了 entry 中
   的 ownerId／incarnation／token。context 20–27 仅有 executionId、tenant、base、binding、
   fingerprint、principal、authorization receipt。原 winner provenance 与当前 recovery
   authority 必须分开保留，不能用新 owner 改写 worker 的原始 provenance。
4. Paimon `PaimonWriteLifecycle.java:110` 调 physical provider；
   `PaimonNativePhysicalWriteProvider.java:57–76` 验证实际对象及 receipt，再生成 SDK
   CommitMessage。`PaimonDistributedWriteFinisher.java:183–199` 用 fingerprint 作为
   commitUser 查重，234–243 最终调用 filterAndCommit。
5. Dataset `LanceMetadata.java:140–157` 先 Native finalize 再 PG publish。
   `WorkerNativeDatasetControl.java:118–150` 在 RPC 前后检查 shared owner，166–187
   已分别携带 current owner 与 immutable intent owner。但
   `LanceDatasetPublication.cpp:366–390` 的 C ABI 调用只传 tenant／generation／base／
   schema／fragments／deadline，没有把 current owner 或 intent fingerprint 交给 Rust
   CommitHandler。C++ owner 形状／相对 token 检查不等于查询实际当前权威。
6. Rust `lib.rs:125–129` 使用 Rename／ConditionalPut handler，1453–1494 使用 CommitBuilder；
   ExactVersionCommitHandler 只固定 next version。SDK manifest 已发布和随后 PG
   visible_version 更新是两个可观测事实，不能以 PG 最后拒绝掩盖 SDK manifest 已可见。
7. Dataset `LanceMetadata.bindRead:83–92` 固定 tableId/version，分片固定 manifest digest。
   Paimon `PaimonReadBindings.java:25–46` 固定 SDK snapshot JSON、uuid、location；
   `PaimonSplitManager.java:77–90` 按 scan.snapshot-id 规划。最终 authority 必须贯通
   latest 和历史 snapshot membership，而非只在写端加条件。

可落实 Paimon managed catalog，未必需要先 fork SDK。官方
`TableCommitImpl.filterAndCommitMultiple(List<ManifestCommittable>)` 273–290 可处理真实
committable；`ManifestCommittable` 的带 properties 构造器55–64、addProperty70–71 可传
完整 envelope，FileStoreCommitImpl 将 properties 写入真实 Snapshot。当前
`InnerTableCommit.filterAndCommit(Map<...>)` 没有 properties setter，不能虚构 API。
采用官方 concrete implementation 需要固定版本适配、类型检查、真实测试；properties
只解决传输，server 仍须验证认证身份、durable exact intent、当前 owner/incarnation/token、
table UUID/schema/generation、branch、base、ABORT 状态，并在实际 snapshot CAS 中提交结果。
[TableCommitImpl](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java)、
[ManifestCommittable](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/manifest/ManifestCommittable.java)。

Paimon allocation 随机 generation 不是 catalog table incarnation；AbstractFileStoreTable.uuid
197–202 在无 catalog UUID 时使用 name+earliest schema time。managed authority 必须固定
catalog-issued table UUID。当前 validateReplayWindow 仅证明原 base 尚存在；SDK tryCommit
700–714 每次读取最新 snapshot，806–809 取 latest+1，因此 append 可能重基于新版本。
要求固定 base 的授权必须拒绝该 candidate；若正式语义允许 append rebase，必须证明最终
lineage 与不变文件集、保留原 intent。不能忽略 base 变化或暗中改 fingerprint；overwrite／
delete 不能沿用盲目 append rebase。

SDK 读取限制也要闭环：SnapshotManager 167–202 优先 catalog loader 取 latest，124–133
的 `snapshot(id)` 直接读文件；部分 listing 扫 filesystem，loader 的
UnsupportedOperationException 会退回 filesystem。managed catalog 完整实现须验证
accepted historical membership／retention，避免未批准候选被历史 pin 或 listing 接受。
只改 latest 指针、保留另一份 filesystem truth，仍不是唯一权威。
[SnapshotManager](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java)。

Dataset 可复用官方 Lance0.20 CommitHandler（402–479），同时控制 commit、全部 latest／
version resolver 与 list_manifests。先写不可变候选 manifest，由同一个真实 catalog
authority 原子验证 fenced intent，登记 accepted version/digest 并更新 SQL visible version；
读取 handler 仅解析被该权威接受的版本。应贯通现有 PG directory 这个实际 catalog，不能
新增 PG shadow 对照表却继续由 object-store latest 决定可见性。execution_anchor 若在
另一个数据库，需要先解决同一事务 authority 的归属，异步复制 anchor 不是 current truth。

标准 ExternalManifestCommitHandler 不能直接当作严格 authority：external_manifest.rs
190–255 在缺记录时读 filesystem，并 best-effort 导入 version；307–341 将 external store
任意错误映射 CommitConflict，并删除 staging manifest。远端成功但 ACK 丢失时不能按
这一模式替代 UNKNOWN 保留。自定义正式 handler 必须覆盖所有解析入口，区分确定拒绝
与未知结果，固定 URI/version/digest，禁止默认 lookup 或错误分类破坏 publication 权威。
[Lance CommitHandler](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/io/commit.rs)、
[ExternalManifestCommitHandler](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/io/commit/external_manifest.rs)。

完整实施单元包括实际 catalog transaction/server、所有合法 writer 的同权威准入、owner
与 intent transport、candidate commit、accepted snapshot loader、query historical pin、
UNKNOWN reconciliation/清理以及 PG+object-store+双 Coordinator 接管测试。仓库当前
没有这套完整 managed publication server；本研究形成具体规格，不添加半接入 SPI／检查
补丁或把配置一个 REST endpoint 当作实现。
