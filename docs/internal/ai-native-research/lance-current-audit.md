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

## 10. 可实施的 managed catalog 原子单元（设计，尚未实现）

此节是下一阶段完整实现规格，不是已落地能力；没有新增 SPI、Native handler、REST
server 或最终发布事务。第11节 execution lease 修复也不能关闭现有 source publication
在 RPC 前后检查 owner 的窗口。

### 10.1 实际 authority 与最少模块

LanceMetadata.createTable:120 为表分配 warehouse/tableId.lance；datasetUri 是稳定物理
根目录。LanceCatalogStore:41–52、140–161 保存 incarnation、SQL visible version、历史
metadata payload 与 digest，没有 manifestURI。Rust open_atomic_dataset:132–141、
CommitBuilder:1453–1494 另外使用 _versions 可见性。不能新增一份 PG shadow ledger
继续让 filesystem 决定可见版本。

最少新增 Maven 模块为零：统一泛型 publication store 放在 pangrova-metadata；Lance
adapter 在 connector-lance；Paimon SDK DTO／REST／FileIO adapter 在 connector-paimon；
Rust managed handler 在 lance-ffi。框架类型不进入泛型 metadata、SPI 或 Coordinator。

拟新增 SourcePublicationAuthority 的真实 JDBC 实现 JdbcSourcePublicationAuthority，
提供 publish、lookup、resolveSnapshot、listSnapshots、abortAndIssueDisposition。
端口只使用不可变 source/intent/candidate descriptors，不暴露 Connection。内部
JdbcPublicationTransaction 用同一个 Connection 调现有 anchor/journal 的事务内
helper，更新唯一实际 source catalog。LanceCatalogStore 改为 authority facade；
LanceMetadata 不再先 SDK publish 再独立 PG publish。ConnectorContext 提供实际控制
端口，Worker 没有 JDBC 发布入口。现有任意 connector jdbc.url 需迁移到同一 authority
数据源，并由启动检查证明；同一 pool 名称或另一个数据库的 shadow owner 无法替代。

accepted snapshot 至少保存 tenant、catalog deployment revision、table incarnation、
branch、SDK version、immutable manifestURI、size/digest、schema fence、exact intent
fingerprint、winner/write-set root、最终结果与历史 membership/retention。

### 10.2 同一个 Connection 的最终事务

网络、官方 SDK 编码和 immutable object verification 均在事务之前执行。锁序固定为
execution anchor → exact intent → source table head → candidate/accepted snapshot。
所有必要锁取得后 clock_timestamp 校验数据库 lease，而非 CURRENT_TIMESTAMP 的
事务起点。最终事务验证：认证 tenant/principal/catalog revision、current owner ID/
incarnation/token/expiry、durable exact fingerprint、不可变 winner provenance、非ABORTED、
table UUID/incarnation、write generation、schema、branch、exact base。

同一事务 insert accepted snapshot、CAS visible head、记录 publication version/digest
及 journal COMMITTED。接管和旧 owner publish 竞争同一 anchor 行锁，不能仅在外层
RPC 前后验证。table version、accepted manifest 和 durable intent result 成为一个事实。

### 10.3 正式 Rust CommitHandler 与 immutable 预写

复用当前 InsertBuilder.execute_uncommitted_stream，它已经先产生实际 immutable
数据和 SDK transaction sidecar；保留 receipt/schema/attempt/hash 校验。不能以
single-file PhysicalBatchWriter receipt 手造 Dataset Fragment。官方
FileFragment.create_from_file:549 需要 existing Dataset、data_dir 相对路径、兼容 schema
和相同 file version，不能作为首次 CREATE 的通用转换。

拟新增 lance-ffi/src/managed_catalog.rs 实现正式 CommitHandler。commit 通过 SDK
提供的 ManifestWriter 在 unique immutable candidatePath 写 SDK manifest，再计算
真实 size/digest、验证候选并调用 authority 最终事务；只有实际accepted返回成功路径。
必须覆写 resolve_latest_location、resolve_latest_version、resolve_latest_version_id、
resolve_version、resolve_version_location、list_manifests 和删除/retention接点。当前
ExactVersionCommitHandler 只覆写 commit，不能保留其默认 filesystem resolver。
ManifestLocation 支持实际相对path、version、size和SDK namingScheme，无需fork格式。
[官方 Lance v0.20 CommitHandler](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/io/commit.rs)。

FinalizeDistributedWriteRequest 已携带 exact fingerprint、current owner、intent owner、
catalog binding/deadline；这些要穿过C++/C ABI到真实handler。目前C ABI丢失字段需完整
贯通。candidate保留同一不可变bytes/digest，UNKNOWN恢复不能重新生成带新timestamp
的manifest或改winner。先lookup exact fingerprint；已accepted则返回其真实receipt，
不重新运行不同candidate。authority不确定时不映射成CommitConflict并删除对象。

### 10.4 SQL与SDK读取只接受冻结manifest

LanceSnapshotResolver → LancePartitionSplitSource → DatasetScanPartitionsRequest → scan.rs
→ LanceBatchReader 全链携带 accepted reference：table incarnation、version、manifestURI、
digest、schema、authenticated membership evidence。读取按其真实manifest打开SDK；
不能从 _versions listing 或SDK latest重选版本。所有历史版本也验证accepted membership。
版本0继续原EMPTY隔离及drop/recreate拒绝，不合成SDK version0。

immutable reference 的真实性来自 authority；仅task自报URI/digest不能证明accepted。
已授权frozen read/pin需有明确retention hold；authority不可用时不能退回filesystem发现。
DROP后旧读binding按当前政策拒绝，但publication evidence仍保留用于UNKNOWN恢复。

### 10.5 Paimon正式REST server与历史FileIO

Paimon1.4 RESTCatalog是client，官方要求服务方实现server。拟由connector-paimon拥有
PaimonManagedCatalogEndpoint，接收正式CommitTableRequest/Snapshot/PartitionStatistics，
调用同一个泛型publication authority。physical receipt → PhysicalWriteProvider → SDK
CommitMessage → TableCommitImpl.filterAndCommitMultiple(ManifestCommittable) → SDK
Snapshot → CatalogSnapshotCommit → RESTCatalog.commitSnapshot(uuid,snapshot,statistics)
→ managed endpoint → 同Connection事务。
[官方REST概念](https://paimon.apache.org/docs/1.4/concepts/rest/overview/)、
[CatalogSnapshotCommit](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogSnapshotCommit.java)。

Snapshot.properties只保存immutable intent/winner/base/generation/fingerprint。current
recovery authority 通过正式 AuthProvider.mergeAuthHeader(baseHeader,RESTAuthParameter)
传输；AuthProviderFactory.create(Options)由FactoryUtil服务发现。每intent独立scoped
catalog/auth实例，不能修改共享bearer token；server认证后再查当前PGanchor，不能只信
header。current owner不能写入candidate Snapshot，否则接管改变candidate hash。
[正式AuthProvider](https://github.com/apache/paimon/blob/release-1.4.1/paimon-api/src/main/java/org/apache/paimon/rest/auth/AuthProvider.java)。

REST latest CAS还不足：SnapshotLoader仅load latest；SnapshotManager:124–155的历史
snapshot(id)/exists直接走FileIO，167–202支持filesystem fallback；RESTCatalog的分区
分页NotImplemented也fallbackFS。需正式PaimonManagedFileIO/FileIOLoader把snapshot/
branch/tag/schema metadata的exists/read/status/list映射相同PGaccepted membership；
Snapshot JSON用官方序列化并在PG事务接受，data/manifest对象继续真实storage。拒绝
canonical snapshot输出绕过authority。实际所用REST分区/分页API必须实现，或入口
明确拒绝unsupported，不能返回NotImplemented后filesystem发现候选。
[SnapshotManager](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java)、
[正式FileIOLoader](https://github.com/apache/paimon/blob/release-1.4.1/paimon-common/src/main/java/org/apache/paimon/fs/FileIOLoader.java)。

启动及真实测试必须证明FileStoreTable.fileIO、CatalogEnvironment.snapshotCommit实际
选中managed实现，不能仅有配置/factory类。不能复制SDK或增加getter-only SPI声称完成。

### 10.6 UNKNOWN、abort、对象持有与验收

authority持久candidate reservation与bounded object receipt pages；冻结exact write-set
root后才允许accept。accepted manifest实际引用对象转为durable accepted references。
ACK丢失按fingerprint查询，UNKNOWN持有candidate和全部对象。新owner复用原bytes/digest
和winner provenance，只更新恢复授权；不可改fingerprint、猜未发布或按lease过期删文件。

abort同事务锁owner/intent/candidate，确认没有accepted publication后，持久ABORTED
barrier及deletion disposition。只清理由该write真实拥有、没有accepted reference且
实际producer已drain的对象；append base文件不能被误删。authority不可用、结果未知
或尚未drain都不能发删除许可。DROP不得删除解释UNKNOWN所需的publication truth。

完整验收使用真实PG/object store及两个独立Coordinator：暂停旧owner于最终authority
事务前，完成接管后放行；SDK latest、历史snapshot、manifest listing、SQL pin全部拒绝
未accepted候选。另测事务COMMIT后丢ACK、相同fingerprint恢复、abort与delayed commit
竞态、权限撤销、drop/recreate、retention holds及真实对象清理。

## 11. Execution anchor锁后数据库时钟修复（与source final fence分开）

在隔离worktree实施JdbcExecutionAnchorStore小单元，未修改Native/协议/其他代理源码。
PostgreSQL CURRENT_TIMESTAMP是事务起点；clock_timestamp是真实调用时间。
[PostgreSQL16正式说明](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)。
仅换函数仍会在row lock等待前采时间，因此修复包括全部路径：tryAcquire在最终row锁
或INSERT竞争结束后采clock，成功新insert在同事务refresh lease；renewOwned/yieldOwned
按execution_id顺序锁exact owner集合，锁后采clock，只UPDATE锁定ID数组；release/yield
先锁exactrow再采clock；claimExpired的SKIPLOCKED选择后单独采grant时间；current在读取
row后采clock。SPI和lease duration语义保持。

真实PG回归保持所有旧断言，并用两个竞争连接和独立observer验证pg_blocking_pids、
xact_start早于原expiry且实际clock仍早于expiry，再由数据库pg_sleep等过该deadline后
放锁。断言旧owner renew/yieldOwned=0、release/yield=false、expired row不变；新acquire
的token递增且acquiredAt不早于放锁时刻。另真实不可见INSERT rollback测试证明新grant
不沿用竞争等待前时间；新插入的owned row不属于旧锁集合，不被batch renew暗中更新。
没有fake clock/store/connection或Thread.sleep。

Selector：JdbcExecutionAnchorStoreTest（旧3项+新增7项）。只做diff检查，构建由根任务
统一执行；当前未声明PASS。这只修execution lease store，不代表source catalog publication
与anchor/journal共事务已实现；第10节仍是尚未实现的完整设计。

## 12. PostgreSQL artifact owner fence：LockRows前投影与发布后的最终复验

本节只涉及同一个PostgreSQL metadata事务中的artifact generation发布，不能解释为
第10节Lance/Paimon source SDK最终catalog publication已经实现。第11节AnchorStore
最近真实PG专项已由root在隔离worktree/JDK17运行：10 tests、0 failures/errors/skipped，
72.47s，日志 `/tmp/pangrova-isolated-lease-clock-nearest-20261007.log`。

### 12.1 官方执行顺序与两个根因

原ArtifactExecutionFence.requireCurrent的目标列
`expires_at > clock_timestamp() AS live ... FOR SHARE`不保证锁后取时钟。
PG16 planner.c:1753–1763在已经形成final_target的路径外加入LockRows；ExecLockRows
先ExecProcNode(outerPlan)，之后才table_tuple_lock。仅tuple version被遍历更新才做
EvalPlanQual；纯FOR UPDATE持锁然后COMMIT没有更新tuple，因此返回原先投影出的live。
clock_timestamp确为实际时钟，但表达式执行位置仍在锁等待之前。
[官方planner](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/optimizer/plan/planner.c#L1753)、
[官方LockRows执行器](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/executor/nodeLockRows.c#L53)、
[官方扫描投影](https://github.com/postgres/postgres/blob/REL_16_STABLE/src/backend/executor/execScan.c#L221)、
[PG16时钟定义](https://www.postgresql.org/docs/16/functions-datetime.html#FUNCTIONS-DATETIME-CURRENT)。
Read Committed的重新检查针对被更新的行，不能把锁-only等待等同于更新后的EPQ。
[官方事务隔离](https://www.postgresql.org/docs/16/transaction-iso.html#XACT-READ-COMMITTED)。

第二个窗口独立存在：Index publisher最初requirePublication后再锁job/descriptor；
GeneratedValues同样锁job/descriptor；TabularExport先锁job再修改generation表。
SQL更新、唯一约束和触发器也可能等待。原代码没有写入完成后的owner校验，初始
FOR SHARE防止接管但不能停止deadline向前推进。ArtifactCurrentAuthority先取得
AuthorizationTransactionGate事务级shared advisory lock，授权变更必须取得exclusive
同一gate；本修复保留该锁顺序和全部tenant/grants/model/policy校验。

### 12.2 已实施的最小闭环（最近测试待root运行）

仅隔离worktree的四个production文件：
- `ArtifactExecutionFence.java`：FOR SHARE仅读取原始owner/incarnation/token/时间字段；
  取得exact行后，用独立SQL `SELECT clock_timestamp()`比较严格`expiresAt > actualNow`。
  durable tenant/job route和exact owner仍必需；同一Connection的行锁保持到COMMIT/ROLLBACK。
- `JdbcIndexArtifactPublicationStore.java`、
  `JdbcGeneratedValueArtifactPublicationStore.java`、
  `JdbcTabularExportPublicationStore.java`：所有publication写入完成后、COMMIT之前，
  同连接再次requirePublication。此时job和anchor都已锁定；后续过期会抛LostOwnerException，
  原有catch整体ROLLBACK generation、descriptor pointer/epoch与PUBLISHED job。

接受点定义为所有可能阻塞的publication SQL结束后同事务的最终owner复验；复验后
直接COMMIT，锁阻止其他owner在接受和提交之间接管。不能承诺PG COMMIT/WAL/网络
耗时始终在租约截止之前；若要求严格commit完成时间deadline，需要独立明确的数据库
提交协议语义。本单元也不改变已有publication/page lease的调用者逻辑Instant语义。

### 12.3 真实PG回归与边界

新增两个test文件，不改旧测试断言，不加mock/fake clock/provider/store：
- `ArtifactExecutionFenceLockIntegrationTest`：生产requireCurrent在纯anchor FOR UPDATE
  等待跨期后拒绝；同样等锁但未过期返回exact anchor；旧incarnation/跨tenant route拒绝。
  独立旧SQL oracle真实证明PG16目标投影live=true而取得锁后的actual clock已超期。
- `ArtifactPublicationFenceLockIntegrationTest`：参数化运行三正式production publisher；
  实际artifact_build_jobs FOR UPDATE等待跨期后generation、job与index/generated指针
  全回滚；未跨期相同等待正式发布成功；Index/Generated另有真实descriptor锁等待跨期。

fixture使用SharedPostgresContainer postgres:16-alpine、完整Flyway、实际租户/授权、
production createJob/claimNextPage/completePage/claimReadyForPublication和backend发出的
execution token；source allocations/output角色与覆盖真实持久化到PG。该专项验证metadata
接受语义，不宣称执行Native artifact构建或SDK源表写入。观察连接用pg_blocking_pids及
pg_stat_activity.xact_start证明worker已在租约尚未过期时进入等待；阻塞连接用PG实际
clock_timestamp/pg_sleep等待截止，再确认actual clock超期并释放行锁。无Thread.sleep。

尚未扩展到其他初始-only调用：JdbcArtifactBuildCompletion(job/page)、JdbcArtifactBuildStore
(claim/renew/retry/fail/markPublished/deferPublication)、JdbcArtifactStageExecutionStore
(prepare/claim/issued lease)及JdbcArtifactStageExchangeStore(register/finalize/load)。它们
经共同helper获得锁后clock修复，但仍需分别在后续等待后最终复验，不能据此宣称整个
artifact DAG的所有接受点已经关闭。JdbcWriteCommitJournal是另一条普通source写入
journal；本单元不把该journal的独立事务变为第10节managed SDK authority。


### 12.4 最近专项暴露的canonical manifest URI根因

root首次运行12 cases得到10 PASS、2 FAIL、0 errors/skipped；两处正向失败仅是
GeneratedValues/Tabular publisher返回值URI与实际PUBLISHED job读出的URI不同。
这不是允许忽略字段的type view：ArtifactGenerationManifest声明为每个consumer共享
的canonical immutable publication view；PublishedArtifactBindingContract:134严格比较
URI，PublishedArtifactBindingResolver:77/186和同连接JdbcArtifactPublishedInputs读取
同一个publication identity。URI为metadata逻辑digest identity，真实对象经output
opaqueUri读取，但逻辑URI仍参与pinned binding的不可变校验。

因此在原两个publisher中统一mint `metadata://artifact-generation/<sha256>`，与Index、
历史job投影和binding消费者一致。Tabular正式generation表的manifest_uri同步写入此
canonical值；Generated正式表没有manifest_uri字段，以同一digest作为持久身份。
保持原正向contains(manifest)全record equality，不忽略URI或增加双namespace适配。
新增实际三种generation表正向source/digest/对象数/行数/字节数/publishedAt校验，Tabular
另验证persisted URI严格等于返回URI；过期rollback另直接验证generation表不存在残留行。
本轮新项目不迁旧typed表数据，待root重验同一12 cases，不把首轮2FAIL写成通过。


## 13. Managed Lance overlay implementation (2026-10-07, DRAFT / UNVERIFIED)

This section describes only the draft under `/tmp/pangrova-managed-lance-overlay-20261007/source`. It is **not applied**, **not built**, **not tested**, and **not a production closure claim**. The shared and isolated validated checkouts remain untouched.

### 13.1 Sole source authority and atomic operation

The format connector has no JDBC directory. `LanceCatalogStore` is a codec over the `ManagedSourceCatalog` SPI. Coordinator bootstrap constructs the actual `JdbcManagedSourceCatalog` with the production metadata DataSource and a late-bound `WriteCommitFencer.currentAnchor(executionId)` callback. Recovery's new owner is the real PG `claimExpired` result registered in `QueryOwnershipRegistry`; the original durable intent does not mint a new lease.

The source transaction locks the global authorization publication gate, the exact execution anchor, the immutable write intent, current tenant/role/policy/model authority, catalog deployment, SQL head and candidate in that order. It repeats the existing lock-after-`clock_timestamp()` anchor check after all potentially waiting statements, immediately before COMMIT. Acceptance changes candidate ACCEPTED, head version/revision/rows/payload, and journal COMMITTED/version/manifest SHA on this **one connection**. No RPC or filesystem publication occurs between the final fence and COMMIT.

V51 stores schemas, heads, publication candidates, bounded object pages and exact owned-object membership. Only fully sealed pages may be accepted. Pages have exact sequence/cursor/count/SHA; identical replay is idempotent, conflicting replay, holes, cross-page duplicates or count/digest mismatch are rejected. Page size is 512, maximum 4096 pages / 2,097,152 new owned objects, page proof bytes at most 8 MiB; manifests and aggregate transaction metadata are bounded at 256 MiB. APPEND points to one accepted base membership; base data is not retransmitted or re-registered. Historical publications retain these reference edges after DROP.

### 13.2 Official SDK preparation and frozen reads

The sole former command 18 route is replaced with PREPARE_DISTRIBUTED_WRITE. Native returns a private SDK candidate and one object page, never COMMITTED. The original FINALIZE request/result/COMMITTED outcome and Rust public commit export are removed from the draft production dispatch. The canonical catalog binding declaration moves, unchanged, from task.proto to connector.proto to avoid a connector/task import cycle; task LanceWriteBinding base_snapshot is field 12. Generated code is not edited. Lance C ABI becomes 13 to carry the one canonical accepted-base protobuf into the Dataset writer.

`CatalogCommitHandler` explicitly implements all six official resolver methods. It resolves only the exact supplied accepted manifest, does not delegate any method to SDK filesystem version/latest discovery, and rejects SDK deletion. APPEND/OVERWRITE open the exact accepted base with that handler before handing the Dataset to the official CommitBuilder, whose Dataset destination inherits the handler. CREATE supplies the same official handler over an empty accepted base. The official ManifestWriter writes a temporary private object; conditional create places the result at the durably reserved `_pangrova/prepared/<fingerprint>.manifest` without a live `_versions` or tag publication. Candidate retries inspect only that known path and compare the decoded official aggregate Transaction to the exact canonical winning transactions.

A second upstream issue matters: fixed SDK `write_transaction_file` uses unconditional `put` for `_transactions/<readVersion>-<uuid>.txn`. The physical-store adapter converts that SDK write into conditional create, accepts only an identical existing size/SHA, and rejects conflicting bytes. It does not copy the SDK serializer. Backend conditional-create capability is verified before preparing a manifest, preserving the ignored-conditional MinIO rejection requirement.

Accepted manifest body size and SHA are verified against PG evidence, then those exact bytes are served to the official DatasetBuilder through a frozen object-store wrapper. A custom object-reader location prevents the SDK local-file fast path from reopening unchecked manifest bytes after verification, including dictionary metadata. Scan pages and each ScanPartitionDetail retain the same exact canonical accepted ref; Dataset readers require it and never use filesystem latest/version checkout as their source authority. Actual schema, version, intent tag and partition ID/rows remain checked.

Primary fixed-version sources:
- https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-table/src/io/commit.rs (CommitHandler / ManifestWriter / resolver defaults)
- https://github.com/lance-format/lance/blob/v0.20.0/rust/lance/src/dataset/builder.rs (official object-store injection and exact-version load)
- https://github.com/lance-format/lance/blob/v0.20.0/rust/lance/src/dataset/write/commit.rs (Dataset destination handler inheritance)
- https://github.com/lance-format/lance/blob/v0.20.0/rust/lance/src/io/commit.rs (aggregate transaction unconditional put)

### 13.3 Recovery, abort and retained objects

A transport timeout, SDK preparation failure, or unconfirmed PG acknowledgement is UNKNOWN; private objects and durable intent/candidate evidence remain held. Recovery checks actual PG accepted membership and replays the exact intent under the newly acquired owner. Missing table or a TableNotFound exception is not an abort receipt. The draft contextful connector abort returns an exact `DistributedWriteAbortReceipt` only after actual same-authority candidate ABORTED plus journal ABORTED commits. The context-free Lance abort explicitly rejects missing durable context. Recovery reports ABORTED only after that receipt matches execution ID and fingerprint; unsupported/unconfirmed abort quarantines.

**Automatic object GC is not implemented by this draft stage.** A worker task FINISHED or a Dataset writer drain result does not prove a separate metadata prepare RPC has drained. UNKNOWN and ABORTED candidates, staged sidecars and owned objects are retained until a later unit persists issuance/drain evidence, reconciles exact candidate membership and recursively checks current/history/retention references. DROP alone does not authorize physical deletion. Retaining these objects is intentional pending that full cleanup closure; it must not be advertised as completed full-chain cleanup.

### 13.4 Validation work still required before application

Production RPC readers/admission/callers are being migrated; legacy public filesystem metadata getters and old SDK publication/discard test infrastructure still require removal/migration. All prior normal/cancel/drain/OOM/multiple-page/numeric tests must retain their substantive assertions. A PREPARED fingerprint is not a PG accepted receipt. Native leaf fixtures using the hard accepted-ref contract must obtain it by an actual production PG accept; a test-only authority broker process may invoke the real stores and return canonical accepted refs, with no production reader bypass. Lower SDK tests may exercise shared SDK candidate resolution directly but cannot fabricate AcceptedSourceSnapshotRef and claim PG acceptance. Public SQL tests must cover actual DDL/write/accepted head/history/read, competing CAS, abort barrier, lost acknowledgement / UNKNOWN reconciliation, tenant/incarnation rejection and lock waits crossing lease expiry.

No selector or build success is claimed for this unfinished overlay. Root will apply/rebase the final complete delta, regenerate canonical protocol, and run JDK17/Release/ASAN plus owning/consumer/public SQL checks before the subsequent issuance/drain/GC unit.

### 13.5 Draft fixture and historical deployment semantics (2026-10-07)

This subsection describes TMP source only; nothing here is applied, built or tested. V51 remains reserved for this unit; the separate lineage outbox unit owns V52.

Accepted origin and current execution deployment are different identities. `AcceptedSourceSnapshotRef.catalog` carries the complete current execution deployment (including fingerprint); new field 13, `accepted_origin_catalog_revision`, records the immutable accepting revision. The SPI reference has the same explicit separation. PG `managed_source_publications.catalog_revision` is never rewritten when a later revision reads history. Its manifest relative path, size, digest, dataset version, membership ID and table incarnation also remain unchanged. Native candidate preparation requires origin equal to its original current write deployment; later deployment reauthorization applies only to source reading.

Historical load checks the actual current PG catalog row under a shared row lock, including incarnation, revision, ACTIVE state and tenant membership. In the same transaction `JdbcSourceNamespace` checks its actual warehouse property: the historical dataset URI must be a strict descendant of the current local or S3 namespace, with the same scheme and authority. A warehouse switch cannot automatically grant access to old objects, and tenant revocation denies their historical references. URI checks reject path traversal and encoded aliases. The emitted reference combines the validated current execution binding and the stored accepting revision; it does not alter the original publication or journal facts.

The draft accepted-write replay API was tightened after inspecting its real consumer. `LanceMetadata.finishDistributedWrite` had returned success on an accepted fingerprint before checking its current execution owner. The sole replay lookup now takes exact fragments plus commit context. Its same-connection order is authorization gate → actual current anchor → durable intent → exact context/fragments and current write receipt/tenant/grants/models → current catalog → SQL table incarnation/namespace → accepted publication with original accepting revision equal to the writing deployment and exact journal COMMITTED version/digest → independent final anchor clock → COMMIT. It never treats a history read reauthorization as permission to replay a writer from a different deployment epoch.

The test-only authority fixture uses production `SchemaMigrationRunner`, `JdbcMetadataStore`, `JdbcExecutionAnchorStore`, `JdbcWriteCommitJournal`, `JdbcManagedSourceCatalog`, real persisted tenant/role/grants and actual Native-finish sidecar size/SHA checks. It issues actual PG leases, preserves original winner provenance, and can yield/reacquire a new actual PG owner for recovery. Its local maps retain issuance handles and owned leases; accepted visibility is always queried from PG. Its UNIX socket broker loads no JNI library and calls no Native method. Java test compilation and a persistent PG broker can therefore precede CTest without first requiring Java integration tests to pass.

`pangrova_lance_fixture_tool` is a test-only executable outside the recursive gtest source glob. It links `lance_c_ffi`, canonical protobuf and Arrow, rather than the Worker engine. Its commands invoke the formal ABI13 schema fingerprint, Dataset writer/finish/drain, PREPARE, scan-partition enumeration and accepted-ref reader. The TMP CMake insertion has an independent before SHA and local delta for the parent to merge against its current CMake input. No production reader bypass or synthetic PG accepted reference was introduced.

`ManagedLancePublicationIntegrationTest` now contains draft real PG + matching fixed-SDK oracles for CREATE/APPEND/OVERWRITE and exact history; current revision 2 reading origin revision 1 without modifying the accepted row; warehouse change and tenant revocation; an actual ignored PREPARED response followed by exact candidate replay; actual PG base-CAS competition between two SDK candidates; object page holes, exact receipt replay and conflicting replay rollback; official SDK transaction conditional-create identical/conflicting bytes; concurrent same-fingerprint SDK preparation; real PG ownership handoff preserving original winners; and positive-version APPEND codec roundtrip with full actual base binding/origin/version/digest/membership. Adversarial storage mutation tests deliberately modify actual files only in their fixture, force the SDK commit path by removing the private unaccepted candidate, and assert no overwrite or PG visibility.

Original connector catalog, EMPTY, allocation, factory and split fixtures are being migrated to that same actual authority. Their substantive tenant isolation, pinned EMPTY, drop/recreate refusal, row count, canonical encoding, malformed/truncated encoding, SQL INSERT/OVERWRITE mode and manifest digest assertions are retained. The old pure structural codec v7 APPEND without any accepted base is retained as an explicit rejection oracle; a legal v0 CREATE carries its original complete structural type roundtrip coverage, while the real PG+SDK positive-version test covers formal base preservation. The split regression produces actual fixed-SDK fragments through 43 real writers and seven actual PG accepted versions, preserving version 7, fragment 42 and its physical 9-row assertions with actual SDK scan evidence.

Still required: migrate the remaining Native JNI/control, Rust and public SQL fixtures; delete remaining private/public filesystem discovery and unsafe discard routes; implement and prove persisted metadata-RPC issuance/drain plus recursive membership/current/history/retention object GC; run the entire owning and consumer Java/Native chain after parent application. All existing cancellation, OOM, writer drain, paging and physical deletion assertions must remain. The source-visibility/acceptance stage and these draft fixtures do not constitute completed full-chain Lance integration.


### 13.6 Owning control fixtures and retired filesystem routes (draft, not executed)

The two POMs have separately recorded local-before SHA/insertion files in the TMP overlay. Connector Lance publishes a narrow `tests` classifier at `test-compile`, after the lifecycle's default compiler binding; Coordinator depends on that classifier only in test scope. The fixture classpath has no reactor cycle: connector test → metadata/test-common, neither → coordinator. Coordinator already directly supplies metadata/security/governance, Arrow, PostgreSQL and Testcontainers runtime requirements. Maven test-JARs do not propagate test-scope dependencies; the consumer dependencies were inspected explicitly ([Maven test-JAR documentation](https://maven.apache.org/plugins/maven-jar-plugin/examples/create-test-jar.html), [test-jar goal](https://maven.apache.org/plugins/maven-jar-plugin/test-jar-mojo.html)). Parent still must prove the closest `-am test-compile` lifecycle produces the attached fixture JAR; no Maven was started by this agent.

`LanceManagedSourceTestExecution` exposes only SPI/protocol/Path values. It writes actual Arrow IPC internally, invokes the matching formal ABI13 fixture tool, and provisions actual PG source authority, intent and leases. Test-only database connection handoff is a 0600 temporary Path, never console output or committed state. Bootstrap tests use the same actual PG datasource for QueryBootstrap/journal/source authority and actual JdbcDistributedLease/JdbcExecutionAnchorStore with the original real process incarnation. The HTTP positive PREPARE response calls the actual SDK tool; injected authentication/internal-error/wrong-type responses exercise uncertainty only. The old native COMMITTED assertion is now correctly at the actual same-PG acceptance/journal transition, after PREPARED remains PENDING. Service-start, missing durable intent, missing ownership service, transport authentication, UNKNOWN, malformed type, owner token/deadline, process close and closed-client assertions remain.

`WorkerNativeDatasetControl.validateOutput` now compares the entire accepted base against the durable canonical output binding, including absence and positive-version requirements. Owning tests use an actual PG-accepted APPEND base and reject missing/conflicting membership as well as the original URI/operation/base/write-id/schema/staging/generation/columns/table/catalog mismatches. The seven old owning cases are migrated to PREPARE; successful decoder evidence is actual SDK output, and recovery performs actual PG yield/reacquire rather than minting positive fake leases.

Rust removes the unused production filesystem latest/commit/independent recovery-identity helpers (`open_atomic_dataset`, old ConditionalPut/ExactVersion handlers, `commit_fragments_impl`, filesystem history/tag scan and committed-manifest getter). The low-level conditional candidate race and real MinIO safe/unsafe checks now call official SDK CommitBuilder with the common CatalogCommitHandler and official serializer, retaining exactly-one winner/conflict, winning SDK transaction identity, immutable bytes after a late writer and unsafe-backend no-candidate assertions. Three former history-scan tests now assert that filesystem versions, a recorded version hint and base/latest fallback cannot establish accepted membership. Actual PG/SDK history/revision tests cover the replacement positive authority contract. A new actual two-writer PG/SDK case preserves order-independent canonical fingerprint/candidate replay and generation-fencing assertions; there is one coordinator canonical fingerprint rather than a duplicated Rust identity algorithm. Dataset Arrow-owner rejection and actual drain/destructor negative assertions remain; the ABI assertion is 13.

These changes are TMP draft only, unbuilt and untested. The parent outbox-25 inventory does not overlap these 71 baseline inputs at the time of this inspection. Remaining Worker route/service/public SQL migrations, matching Java/native build and complete real test execution, persisted prepare-RPC issuance/drain proof and recursive retention/history/current object GC are still mandatory. Unsafe fragment-discard infrastructure has not yet been removed. No claim of full managed Lance closure or automatic GC is made.


### 13.7 Public SQL consumers and source acceptance final-clock oracles (draft, not executed)

The public SQL publication oracle reads the manifest only through an actual `JdbcManagedSourceCatalog.snapshot()` loaded from the shared PG datasource. It keeps the original one-manifest assertion at the accepted private path, verifies actual file size/SHA against PG/journal and checks that `_versions` does not exist. It retains numeric/EMPTY/drop-recreate assertions and adds SQL `FOR VERSION AS OF 1/2` after version-3 overwrite, plus immutable historical/current generation identity. DDL/bootstrap/Feature export/Worker catalog fixtures remove connector-local JDBC settings. The existing Feature historical-corruption negative mutates `managed_source_publications.manifest_sha256` at its ACCEPTED row instead of the removed shadow catalog, retaining the exact one-row mutation and actual export rejection checks.

Worker control/route tests use the sole PREPARE command. A scan without accepted membership now fails before any deployment fetch; the former test's tenant-denial/fetch assertions remain on the actual schema-control tenant-denial route. No accepted reference is fabricated to reach a later authorization branch. The recovery admission negative uses actual PG acquire/yield/reacquire owner tuples; its untrusted reservation/opaque negative input cannot establish source acceptance and is rejected by the real tenant allowlist before Native.

`ManagedLancePublicationFenceIntegrationTest` adds two actual PG/SDK final-acceptance cases. Real SDK bytes are prepared and PG object pages sealed. A second connection holds the actual head row FOR UPDATE; production accept obtains its initial anchor lock and waits. `pg_blocking_pids` and backend `xact_start` prove the waiting transaction began before the unchanged original 60-second lease deadline. The expired case uses PostgreSQL `pg_sleep_until` and independent `clock_timestamp` to cross the deadline without mutating expiry or client/fake clock. Releasing the head lock must reject the final fresh-clock fence and roll back head/ACCEPTED/journal publication together, retaining sealed objects and the exact original intent bytes. The live case releases the same real lock before expiry, then requires PG accepted head/COMMITTED journal and exact actual SDK rows. The expiration case intentionally takes approximately the original lease duration; no duration was extended to conceal a failure. This adds an owning `awaitility` test dependency only; Coordinator already supplies it as a direct test dependency for the fixture JAR consumer.

All new cases remain UNRUN. The visible draft remains incomplete until remaining real Native fixture migrations and the durable prepare issuance/drain/GC unit are implemented and parent-applied owning/consumer integration checks pass.

### 13.8 First durable preparation drain and bounded object cleanup unit (TMP, unbuilt/unrun)

This checkpoint supersedes the preceding draft's statement that unsafe discard infrastructure is still present. The draft now removes `lance_ffi_discard_fragments`, its borrowed-fragment ABI and unused filesystem cleanup/recovery helpers. The original Native committed-sidecar deletion, repeated deletion and recovery-after-deletion assertions remain and use actual PG cleanup reservations plus the common SDK deletion implementation. This is a first safe production unit, not complete automatic GC or all Lance/Paimon closure.

Official fixed-version ownership evidence: [Lance 0.20 ObjectWriter](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-io/src/object_writer.rs#L302-L311) starts an asynchronous multipart abort from Drop when still InProgress. A top-level SDK error therefore cannot prove physical drain. Its successful shutdown polls part tasks, waits for an empty JoinSet and completes the upload before Done ([shutdown](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance-io/src/object_writer.rs#L415-L460)). The official [ManifestWriter](https://github.com/lance-format/lance/blob/v0.20.0/rust/lance/src/dataset.rs#L1622-L1642) awaits that shutdown. Our inference is restricted to that normal success path; an execute error, including a conditional-create loser with an already existing exact candidate, remains UNKNOWN with unproved drain.

`PrepareDistributedWriteRequest.invocation_id=17` identifies one durably issued actual call. The result carries the same nonce at field 5 and `sdk_joined` at field 6. `managed_source_prepare_invocations` records the exact candidate/execution/page/cursor, request SHA, selected endpoint and issuing owner tuple before RPC. Only the matching observed actual SDK join response records DRAINED and its exact response SHA; replay with a different response is rejected. Neither lease expiry, task FINISHED, HTTP cancellation nor Writer-drain evidence substitutes for this receipt. Registration requires a matching DRAINED page invocation. Every accepted publication still uses the same-connection initial/final independent DB-clock execution fence and head/journal/membership transaction.

The FFI no longer drops preparation at a tokio timeout. Admission checks the deadline, then waits for the actual SDK operation to return. All SDK execute errors return UNKNOWN with `sdk_joined=false` and remain ISSUED; a later real read-only invocation can reconcile the fixed candidate and accept it, while the earlier unproved invocation still prevents cleanup. HTTP lost replies likewise retain ISSUED. No 64 MiB buffering adapter or synthesized resource grant has been introduced. A proposed buffered metadata adapter was rejected for this first unit because metadata-control currently lacks an execution/stage memory-grant path. Actual grant accounting plus multipart-owner tracking is required in the next unit.

Each object has the sole canonical `SourceObjectPurpose` at field 3: SOURCE or WINNER_SIDECAR. The SDK validates winner sidecars from genuine receipts and uses SOURCE for data, private candidate manifest and official aggregate transaction. Page and total proofs include the role as unsigned 32-bit big-endian after path/size; unknown roles fail closed. Keys share the actual SDK 2048-byte ASCII relative-key bound. The authority retains role, size and digest facts even after physical deletion. It cannot register an object already marked for cleanup or owned by accepted history.

`reserveCleanup` is on the same authority connection and execution/authorization lock order. It requires ACCEPTED or ABORTED, complete sealed object inventory, and every invocation DRAINED. ACCEPTED permits winner-sidecar cleanup only. ABORTED permits only objects with no other accepted/current/history/PREPARING/PREPARED ownership at the same immutable dataset namespace; accepted APPEND base memberships remain retained, including after overwrite/drop. Source objects of accepted history are never selected in this unit. Unknown or partial inventories remain held. A bounded 512-object exact batch is first committed as DELETING, with nonce/count/role-aware SHA and exact object assignments. Canonical command 23 `CLEANUP_SOURCE_OBJECTS` executes only that batch through official ObjectStore HEAD/DELETE awaits; changed size or uncertain delete fails without a success receipt. The same authority marks DELETED only after exact nonce/SHA/count and actual SDK join. Lost deletion replies reconcile the same marked batch and real idempotent SDK deletion. No directory discovery or recursive filesystem deletion exists.

Production wiring is `LanceMetadata` barrier → `WorkerNativeDatasetControl`/real source authority → authenticated Worker control → `NativeDatasetControl`/`LanceDatasetPublication` → formal ABI13 cleanup → authority completion. Once source acceptance or abort is already committed, a cleanup failure retains the durable ownership facts and reports the existing truthful source receipt; it cannot undo or pretend that source publication is unknown. Exact source replay retries cleanup. This is durable retained work, not a successful GC metric.

Production control HTTP is binary RuntimeCommandResponse/Any Protobuf, with no JSON/base64 expansion. The complete serialized envelope is capped at 16 MiB at Native send, Worker validation and client bounded buffering, separately from the 8 MiB semantic page proof, 512 objects/page and 4096-page source limit. Client tests retain over-limit cancellation and add exact full-envelope/chunk/+1-byte assertions. Only the test broker uses JSON/base64 with a separate 32 MiB frame limit. Transport cancellation cannot prove Native owner drain and leaves the issued call held.

New real regression selectors: `ManagedLanceCleanupIntegrationTest` has six actual PG/fixed-SDK cases: committed sidecars only with historical/current numeric reads; sealed unreferenced abort deletion and idempotence; false join/unknown nonce rejection; lost prepare reply retaining every actual object; lost delete reply exact-batch reconciliation; and earlier unobserved invocation retaining sidecars after later acceptance. `NativeDatasetControlBootstrapTest` additionally tests real binary HTTP join receipts → actual PG DRAINED → actual SDK cleanup, retaining service lifecycle, errors, owner/deadline and UNKNOWN checks. Concurrent SDK candidate tests use different real issued nonces for different calls; all immutable candidate/page fields remain exactly equal after only the per-call nonce is removed for comparison, and both join flags/nonces are independently checked. Conditional losers must explicitly return UNKNOWN before a new real read-only replay. No mocked successful store, fake clock or fabricated accepted reference is used.

The overlay now has 82 existing-file before-SHA inputs. The only live baseline drift observed was ROOT's exact `NativeFunctionDescriptor.default_null_behavior` field-6 insertion; it was preserved by local rebase with the original baseline archived, not overwritten. C NULL, ROOT V52 lineage outbox and ROOT lease-clock fixes remain intact. V51 remains A's sole migration. All changes and tests in this section are UNBUILT, UNRUN and not applied to main or isolated checkout. Worker invocation status/ack outbox, process-incarnation/restart drain proof, actual metadata-stage memory/read budgets, retention-driven accepted-history GC and Paimon managed authority remain required follow-up units. Seven existing ASAN leak failures are not treated as a passed full run.

The Coordinator recovery regression now uses the production Lance connector/codec with an actual PG accepted APPEND base and official SDK prepared bytes. It checks every decoded field recursively (including the contents of defensive digest arrays), exact canonical encoded bytes, actual COMMITTED version/digest, and rejects an allocation substitution even when its forged journal fingerprint is internally consistent. The original v37 APPEND without an accepted base remains an explicit rejection input; table-gone exception names without a committed exact abort receipt remain QUARANTINED. The production encoder additionally rejects URI/version/schema divergence before dispatch. These are source-authored tests, still UNRUN.

### 2026-10-08: managed object paths must retain directory segments

The actual PG/SDK actor regression returned UNKNOWN because source enumeration
issued HEAD for a sidecar whose slashes were encoded as `%2F` in one component.
The sidecar writer had correctly written separate directory components. The
same construction defect affected candidate manifests, frozen accepted reads
and cleanup; a missing encoded cleanup alias could incorrectly appear absent
while the real nested object remained. The reproduction is recorded in
`/tmp/pangrova-isolated-lance-unknown-diagnostic-nearest-r1-20261008.log`.

The direct dependency is `object_store` 0.10.2, pinned in Cargo.lock. Its
[official Path implementation](https://raw.githubusercontent.com/apache/arrow-rs/object_store_0.10.2/object_store/src/path/mod.rs)
defines `child` as appending one `PathPart`; supplying a relative path containing
slashes encodes those slashes. Managed prepare, resolution, frozen reads and
cleanup now use the existing `object_path` validator and segment-wise append,
shared with physical sidecar reads and writes. Raw canonical keys, accepted
membership identity and URI authorization are unchanged. Encoded or traversal
keys remain rejected, and SDK errors still retain UNKNOWN ownership.

The added SDK leaf regression creates an actual local Dataset candidate and
sidecar, checks the physical nested files, independently HEADs their canonical
paths, verifies SDK transaction and sidecar contents, and proves the encoded
single-component aliases are absent. Existing conditional-create winner and
filesystem-history rejection tests retain their assertions and now populate
real directory paths. Leaf tests prove SDK/storage path semantics, not PG
publication authority. The existing PG-issued actor, accepted-reader and
physical abort/cleanup tests cover that authority chain. This precise patch is
unbuilt and unrun when frozen; validation remains with the root build owner.

### 2026-10-08: separate genuine SDK leaf facts from accepted-source admission

The first ABI13 Rust test build exposed obsolete SDK-test calls to removed
filesystem-authority helpers (`open_atomic_dataset` and
`committed_manifest_digest`) and missing accepted-reference fields. Those tests
exercise official SDK DELETE, including fragment gaps, deletion-file row holes
and historical versions. The managed SQL preparation contract deliberately
rejects deletion-file and external row-id publication until their complete
object contract is supported; these SDK datasets cannot honestly be labelled
PG-accepted by constructing an `AcceptedSourceSnapshotRef`.

The accepted FFI reader still requires the actual accepted identity, catalog
storage binding and membership tag before selecting a partition. The common
physical selector, actual SDK scanner setup and Arrow buffer-slicing operation
are now shared by that accepted reader and the SDK leaf tests. The leaf tests
use official Dataset write/delete/checkout operations, the SDK's own fixed
manifest naming API and independent hashes of the actual manifest object. They
never create an accepted reference or admit a dataset through a test-only
production open route. Their bounded scanner reuses the same physical selector,
projection/address setup, batch slicing and position materializer, then the
actual Arrow C Data export/import.

All SDK fragment gap, duplicate/order rejection, historical manifest, deletion
hole, numeric row position and progression assertions remain. A prior manifest
proof is rejected by the same physical selector against the actual later
manifest hash; changing per-call row limits still consumes every row once. The
former leaf FFI-open/status checks move to the formal PG-issued Native FFI
coverage, where the real actor test has passed after the path correction. This
SDK leaf migration itself is frozen but unbuilt/unrun; root owns its actual
Cargo execution and the affected fresh Rust/Native regression.

The official SDK's version-specific path computation is reused from
[ManifestNamingScheme 0.20.0](https://raw.githubusercontent.com/lancedb/lance/v0.20.0/rust/lance-table/src/io/commit.rs),
not reproduced as another manifest path codec or discoverable production
resolver. Supported SQL DELETE semantics, accepted deletion-object publication,
resource grants and restart/retention GC remain separate unfinished units.
