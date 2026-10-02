# Paimon Native physical-file 写入实施规格

状态：设计冻结候选；不是完成声明。现有 public SQL 读向测试须重新实测，反向写入尚未实现。
本规格使用统一 v2 task/result/control 协议；不新建 Lance feature 协议，不把 Dataset
manifest/version 充当 Paimon snapshot。首个原子开发单元可以 append-only，但不能缩小原有
合法 SQL 回归，不能因此宣称所有 Paimon 表模式生产就绪。

## 1. 已核实基线与根因

- Paimon 1.4.1 `paimon-lance` 官方 POM 锁定 Lance Java SDK 0.39.0；Native
  `pangrova-worker-native/lance-ffi/Cargo.toml` 锁定 Lance/Rust 子组件 0.20。
- `PaimonLanceNativeReadE2ETest` 有 7 个真实 SDK 写入后 public SQL 读出的场景，不能由
  fixture 源码存在推导兼容已经通过。该类旧 ABI 7 注释应随实际 ABI 8 验收修订。
- `SqlE2ETestBase:467` 将框架绑定到 `executeViaRealPipeline`；它使用认证 HTTP
  `/v1/query/submit`、真实 Worker/JNI、结果 transport 消费到 EOF、真实 query FINISHED。
- `PaimonSplit.toSplitInfoProto` 发送 Paimon snapshot、精确 SDK 文件路径和 byte length、
  physical row interval、schema ID、DV location/range/cardinality，不扫描独立 Dataset。
- `physical.rs::PhysicalFileReader` 校验 URI authority、对象实际大小、footer magic/version、
  有界 metadata 和实际行数，再用 Lance SDK decoder 读取明确行区间。谓词仍是 canonical
  FilterRel/Velox FilterNode，不发送字符串给 DataFusion。
- `PlanExecutionDriver::addWriteFormatProperties` 和 Java
  `NativeWriteExecutor::requireConnectorManagedWriteFormat` 对通用 LANCE 写入 fail closed：
  当前缺 allocated physical URI 与 external-file commit metadata，不能删除 guard 放行。
- `drainOutputToLance` 仅消费 `WriteOutputBinding.lance` 的 Dataset 事务，
  `NativeWriteAllocation` 也仅描述 Dataset；Paimon output handle 没有 Native physical 分配。
- `PaimonDistributedWriteFinisher` 当前只接受 SDK CommitMessage，经 Paimon SDK发布 snapshot。
  这条单提交权威必须保留；Native 不得创建 Paimon manifest/snapshot，不得提交 `_versions`。

## 2. Caller、implementation、wiring 和 exports

| 层 | 现有入口 | 改动责任与验收 |
| --- | --- | --- |
| SQL admission | QuerySchedulingEngine.allocateReservedWrite/beginWrite | 保留当前 tenant、写权限/declaration receipt、allocation intent；beginInsert/Overwrite 单次分配 |
| Connector metadata | PaimonMetadata / PaimonTableHandleFactory.outputFromTable | 从真实 SDK table/schema/snapshot/options 构造不可变 physical-write handle |
| Handle codec | Paimon connector 的 ConnectorOutputHandleCodec | 新 codec epoch 明确编码全部新字段；未知版本/尾随字节/跨 connector 拒绝 |
| Protocol binding | ConnectorWriteBindingFactory / WriteTaskOptions.output_binding | generic physical-file binding 并入已有 task.proto；字段号由根统一确认，不猜测 |
| Worker authority | WorkerCatalogRuntime.bindTask / TaskConnectorSession | exact catalog incarnation/revision、tenant、attempt、handle/schema 验证，secret 只在 Worker 本地解析 |
| Native options | TaskExecutor WriteConfigInfo / request conversion | 同一 binding 唯一解析；未知 sink type/mode/encoding 必须拒绝 |
| Columnar producer | PlanExecutionDriver / Velox Task | 同一 TableWrite root RowVector，非结果 wire、非 Java row fallback |
| Physical writer | Lance C ABI / Rust lance_file::v2::writer::FileWriter | 新物理文件 writer，不复用 Dataset transaction；Arrow C Data 仅进程内互操作 |
| Receipt reporting | Native writer metadata channel / task report | 上报 bounded typed receipt，真实 terminal 和 winning attempt，不造成功终态 |
| Durable preparation | NativeWriteCommitTracker / JdbcWriteCommitJournal | 持久化 allocation、winners、typed receipts 与 publication 状态，恢复不重分配 |
| Catalog publication | PaimonDistributedWriteFinisher / InnerTableCommit | receipt 转真实 DataFileMeta/CommitMessage，SDK唯一 filterAndCommit 与 fingerprint replay |
| Post-commit metadata | PaimonCommitNotifier / IndexCommitHandler | 指向真实已提交 snapshot/files/write provenance；不借维护获取任意表数据权限 |

SPI 的 exporter 是既有 ConnectorFactory ServiceLoader 和 Connector 返回的 metadata/codec，
不是新增独立 provider 注册或 coordinator JNI。所有新增 contract 必须有上述真实调用者、
Paimon 实现、Worker wiring、最近真实测试；不能只加入默认空实现后宣称能力已接通。

## 3. Contract 最小集合

metadata SPI 新增 `NativePhysicalWriteAllocation`、`NativePhysicalFileAllocation`、
`NativePhysicalFileReceipt` 和 `NativePhysicalWriteProvider`；既有
`ConnectorOutputTableHandle` 提供明确 physical allocation，Connector 暴露 provider。
值对象/接口与统一 v2 字段已落地，Paimon provider、Worker writer 和 publication 尚待贯通，
不能据此宣称生产能力已实现。SPI 不依赖 JNI 或 generated protobuf；protocol
adapter 将值对象映射到统一 v2。Dataset allocation 与 physical allocation 必须互斥。

### 3.1 Table allocation

包含 catalog identity/fingerprint、tenant、schema table identity、exact output handle/codec epoch、
schema ID、ordered logical columns 与完整 physical schema fingerprint、base snapshot、write ID、
generation、write mode、overwrite partition selection、schema/options declaration fingerprint、
partition/PK/bucket definitions、merge policy、sequence policy、encoding capabilities、size limits。
resolved credentials不进入持久 handle、TaskRequest或日志。

### 3.2 File allocation

由真实 Paimon SDK path factory 生成 fileName/path，记录 canonical partition、bucket、schema ID、
physical encoding、file ordinal、attempt identity/control epoch/fencing token、lease generation、
attempt-private namespace、allocation digest、source restored-file-set/sequence fence。
Worker不能从 target_table 或任意字符串猜 URI。首个开发单元的 unpartitioned/fixed-one-bucket
也必须从真实 schema.options 和 SDK factory得到，不是测试恒定填 0 的替代实现。

分配必须先持久化，重试同 allocation key 返回同 identity，重试 attempt 使用不同对象 URI。
文件创建必须 create-only，不覆写别的 attempt 或已提交文件。滚动文件需要逐一 durable ordinal
分配；EOF 时只提交实际存在的非空文件，空 overwrite 仍保留真实操作语义。

### 3.3 Receipt

每项包含 allocation digest、write ID/generation、完整 TaskAttemptId、file ordinal/URI、size bytes、
physical row count、schema ID/schema fingerprint、actual footer encoding、对象 checksum/identity、
真实 per-column stats及其列/策略定义、min/max sequence、key bounds（PK）、row-kind计数、
level/file source、相关 index/DV sidecars 的精确位置/大小/checksum，必要的 writeCols/evolution。
receipt必须bounded且字段存在性清楚；unsupported stats是显式 unavailable，不能编造 min/max、
零 null count 或声称 EMPTY_STATS 表示实际收集完成。禁止把未知业务列值放入日志或用户诊断。

数据文件 object identity不能仅靠易变 multipart ETag当 SHA-256。无法实际确认的 checksum
不能填假值；格式与 checksum contract 在同一 receipt epoch内明确约定。

## 4. Schema 与物理编码

Paimon TableSchema 的 immutable field ID、column order、nullability、decimal precision/scale、
timestamp semantics、VECTOR维度/elementtype均来自真实 SDK schema，不根据列名猜类型。
typed columnar input先满足声明，再构造对应 physical Arrow/Lance schema；dictionary/constant
Velox vector必须经正确 export处理，不能 reinterpret为flat。

append-only写出正常值 schema；PK模式必须使用 SDK `KeyValue.schema` 对应实际 key/value及
sequence/value-kind布局，不能写普通业务 schema再附假PK metadata。SDK
`KeyValueFileWriterFactory` 的 thin-mode和per-level格式也必须映射实际声明；尚未支持的模式
在admission拒绝，不把其业务列错误当作append-only。

用真实 Paimon SDK writer导出 schema与文件语义进行差分oracle，不能人工改footer或把0.3/2.0
拼写当无证据兼容。Native0.20写出的V2_0/V2_1与SDK0.39读取能力必须分别实测；未验证编码不发布。

## 5. Sequence、partition、bucket 和 PK 的真实 SDK策略

本节基于已读取的官方 `paimon-core-1.4.1-sources.jar`，不是依靠记忆。

- `BaseAppendFileStoreWrite.createWriter` 将 restoredMaxSeqNumber和SDK
  `createDataFilePathFactory(partition,bucket)`传给 AppendOnlyWriter。
- `AppendOnlyWriter` 正常模式 counter从 `maxSequenceNumber + 1` 开始；data-evolution模式
  使用每writer `LongCounter(0)`。不能把两者混为统一常数策略。
- `RowDataFileWriter.result` 的append min/max分别为 counter-recordCount与counter-1，
  使用真实 row stats、schemaId、writeCols/externalPath等构造DataFileMeta.forAppend。
- `KeyValueFileStoreWrite.createWriter` 将恢复的最大sequence交给MergeTreeWriter。
  `MergeTreeWriter.write` 每记录分配sequence，buffer flush/spill不重新编号。
- `TableWriteImpl` 使用schema-derivedKeyAndBucketExtractor提取 partition、bucket、trimmed PK；
  FixedBucketRowKeyExtractor基于logical bucket key和配置BucketFunction(DEFAULT/MOD/HIVE)。

实施策略是将上述schema/options解析成canonical typed Native write operator策略，在Native
执行分区、Paimon兼容key编码/比较/bucket hashing、row-kind与sequence、排序/merge/spill。
绝不把rows传JVM让SDK TableWrite.write代执行。SDK只做schema/path/snapshot metadata与最终commit。
差分测试的SDK writer是实际oracle，不是生产回退。

sequence基线来自分配时真实snapshot的restored files，连同file-set digest持久化。
同一写入单元的partition/bucket必须单winning owner，以确定输入序列和滚动文件counter；
并发SQL/其他SDK writer的冲突和序列语义由Paimon真实commit策略证明，不虚构全局序列唯一性。
重试必须读原durable基线，不用最新snapshot重编号同一receipt。

PK必须保留输入RowKind与persisted sequence，按SDK comparator与merge-engine执行：
deduplicate、partial-update、aggregation、delete-reset、sequence.field及tie规则均需真实差分。
不能在delete之后恢复旧fields，不能把旧值min/max统计成新logical行。
固定bucket DEFAULT/MOD/HIVE分别做负数、NULL、UTF-8、decimal、timestamp与多列key差分；
dynamic/postpone bucket必须实现其真实index/ownership协议和recovery，不改表options强制bucket=1。
在对应策略未接通前明确拒绝新Native路径，不影响既有已合法SQL功能及其原验收。

### 5.1 当前单元的真实限制与后续完整扩展边界

当前 `PaimonPhysicalWriteAllocations.requireAppendLayout` 明确只接受无主键、无分区、
固定一个 bucket 的 Lance append 表；这不是上游 Paimon 的限制，也不是大规模写入已闭环。
`PaimonWriteLifecycle.insert` 只对实际 `file.format=lance` 分配 physical binding，
`WorkerCatalogRuntime.validateNativeOutput/taskResources` 对 Native binding 校验后禁止取得
PageSink，不能在失败时改走 JVM rows。现有 PK、DV、partial-update、aggregation、schema
evolution 七项 public SQL read oracle仍需完整保留；SDK 写出后 SQL 读取不证明 SQL PK 写入。

已复读固定 `paimon-core-1.4.1-sources.jar` 的以下真实生产语义：

- `TableWriteImpl.writeAndReturn` 先检查 NOT NULL、应用默认值、生成/过滤 RowKind，再由
  `KeyAndBucketExtractor` 提取 partition、bucket、trimmed primary key。Native 不能仅 hash
  SQL 原始输入而略去这些步骤；具体受支持配置必须进入同一 policy checksum。
- `FixedBucketRowKeyExtractor` 使用 schema 的 bucket key projection；`BucketFunction.create`
  实际区分 DEFAULT、MOD、HIVE。对键的序列化、NULL、负数、DECIMAL、时间与字符串必须
  与实际 SDK 的 `getPartition/getBucket` 差分，不以通用 HASH 的相似结果代替。
- `AppendOnlyWriter` 正常 writer 共用从 restored max+1 开始的 counter，但 data-evolution
  模式每个 writer 使用新 counter(0)。这两种策略不得共用当前 append 的行数公式。
- `MergeTreeWriter.write` 在放入 buffer 前分配 sequence，同一条记录在 flush 后重放到
  buffer 仍使用原 sequence。`forEach` 实际使用 key comparator 和 merge function 后输出，
  不是简单把收到的业务 rows 原顺序写到文件。
- `KeyValueFileWriterFactory` 的 physical schema 为 `KeyValue.schema(keyType,valueType)`，
  内含 sequence/value-kind；thin mode 会改变 key 布局，level/changelog 可改变 format、
  compression 和 stats policy。`KeyValueDataFileWriter` 要求输入已排序，真实 first/last
  key、min/max sequence、retract count、level、key/value statistics 决定 `DataFileMeta`。
  当前 SPI receipt 尚未承载 proto 已有的 min/max key、level、delete-row-count，不能把这些
  字段的存在当作 PK 提交接通，也不能用 append `SimpleStats.EMPTY_STATS` 冒充 PK 元数据。
- `DedicatedFormatRollingFileWriter` 将普通列与 vector/blob 列投影到不同真实文件；一个主文件
  可对应多个 vector 文件，关闭时核对行数。当前单文件 physical allocation/receipt 不足以
  表达 mixed-format 的 file-set、writeCols 和共享 sequence 覆盖范围。

后续必须作为完整原子单元扩展同一 Native TableWrite，而不是新增执行路径：

1. 元数据侧从真实 SDK 导出完整业务输入与输出 physical schema、field identity、上述策略和
   原 source snapshot/file-set。Native 唯一 RowVector sink 执行默认值/RowKind、partition/
   bucket、序列、排序/merge/spill，再交给唯一 physical encoder；Coordinator 不接收 rows。
2. partition/bucket/write-column-group/rolling ordinal 的文件集合不能提前假定为一个文件。
   在现有 metadata-only callback/任务权限边界中扩展 connector-owned allocation：按原 write
   intent、attempt、source snapshot 和规范化的 typed partition/bucket 请求真实 SDK path，
   持久保存 allocation 后才允许 Create。请求只带有限描述符，不带 rows；未知 bucket、
   越界分区、不同 schema/options、重复 ordinal 不同 identity、旧 owner 全部拒绝。
   这需要现有 experimental provider/统一控制契约的后续设计与 journal authority 接线，
   当前没有相应实现，不从 URI 字符串推导授权。
3. 同一 source partition/bucket 的一个写入单元必须有单 winning producer；使用已有 canonical
   hash exchange 与有界 Native 排序，不让多个任务各自从相同基线独立编号。重试只重用原
   allocation/sequence provenance，不用 latest snapshot 重写已有 receipt。
4. 扩展现有 typed physical receipt 的实际 SPI/codec 消费，完整保留 PK key/sequence/RowKind/
   level/delete count、mixed file group/writeCols 与实际统计；由 SDK adapter生成相同
   `DataIncrement/CompactIncrement`，经原 durable journal/唯一 SDK snapshot commit 发布。
   atomic winner/file-set 覆盖必须先验证，不能逐文件提交使 mixed rows 部分可见。
5. SQL 差分门槛包含 multi-partition/multi-bucket、三种 bucket function、重复 key 与并发
   attempts、tombstone、partial-update/delete-reset、aggregation、sequence.field/tie、
   thin/per-level/changelog、mixed VECTOR/BLOB/MAP、static overwrite 空输入，以及 MinIO
   ACK 丢失和重启重放。未完成任何一项，保留原 guard并明确未验收，不宣称全部生产写入。

## 6. 统计、索引与源定位

统计在Native按声明的SDK StatsCollectorFactories策略和physical schema收集：真实行数、
NULL数量、支持类型min/max、截断策略和index/DV元数据。Java metadata适配只编码统计，
不重新读取rows。只支持部分统计时显式不提供相应pruning能力，不能常量补齐。

receipt的physical rows是文件实际行数，logical rows由PK merge/snapshot语义决定，两者不能混淆。
SDK提交后的source object identity由真实snapshot和精确物理路径确定；scan position是原文件行号，
过滤/投影/merge输出不得重排为伪物理locator。涉及DV时必须精确object+offset+length+
checksum+cardinality，禁止以整文件或同bucket其他文件替代。

后台统计通知携带已提交snapshot/file集合与write receipt provenance；只能维护这批已提交对象。
持久索引read-refresh是另一种current-revocation授权，不从write receipt获取任意read权限。
缺provenance永久拒绝并保留拒绝状态，不能静默ACK或假maintenance subject。

## 7. Native 唯一输入、资源、spill 与取消

保持现有Velox callback→有界OutputQueue→Native sink。drain必须在执行/等待Task完成之前
并发启动，否则满队列形成producer等consumer、consumer等完成的死锁。
物理sink直接消费同一TableWrite root RowVector，不经过Presto/Arrow wire解码或JVMrows。
Arrow C Data仅用于Velox→Rust进程内接口，不建立第二计算路径。

producerOutcome是实际成功/失败/取消状态；queue closed不等于成功。
只有producer成功、所有写入flush/finish成功、对象确认和receipt一致后才允许PREPARED。
Task、QueryCtx/MemoryPool、converter poolholders、writerlease、Rust runtime/storagecontext必须
存活至drain join、Arrow release、writer close和队列释放完成。关闭顺序不能倒置。

Sort/merge/spill由Native query admission统一预算，文件writer buffer/encoding/IO并发计入实际
Native容量，不能给每bucket不受限独立池；spill容量、文件数、待分配文件数、receipt字节有界。
cancel/failed终态关闭消费者和writer，登记owned orphan；abort删除只能对durable未提交分配且
仍有winning fence的精确URI进行。UNKNOWN publication时禁止贸然删除。

## 8. 原子发布与恢复状态机

`ALLOCATED → WRITING → PREPARED → COMMITTING → COMMITTED`；取消/永久拒绝进入
`ABORTED/REFUSED`，RPC超时/leader丢失进入`UNKNOWN`，不得自动转成功或重新提交新内容。

1. admission验证current权限、tenant/catalog/schema/options；持久intent后分配。
2. Native只create未引用物理文件；flush/close/对象确认后产生immutable receipt。
3. coordinator接受currentwinningattempt的真实report，持久化winners与完整receipts。
4. connector核对全部receipt和实际对象metadata，与allocation/schema/file-set fence一致；
   所有文件验证成功后构造真实SDKDataIncrement/CommitMessage，不能边验证边发布snapshot。
5. Paimon SDK InnerTableCommit唯一filterAndCommit，以现有commitFingerprint作replay identity。
   记录真实published snapshot/fingerprint/file-set后才可对SQL返回COMMITTED/FINISHED。
6. coordinator在SDKcommit成功、journal未完成之间崩溃，恢复先查真实SDKsnapshotcommitUser/
   fingerprint与manifest引用。已发布则补journal；未确认则UNKNOWN，不能重复计费/提交。
7. overwrite使用真实selection语义，包含无surviving rows的partition；不能只替换新文件涉及分区。
8. snapshot过期导致原replay证据丢失，永久ambiguous拒绝，不能假定未提交再写。

SDK一个commit可能生成data/compaction多个snapshot。验收要求一次业务提交、无重复文件和
正确operation，不把所有模式都武断要求snapshotId恰好增加1。
Post-commit统计失败不能把已发布业务commit变成可重新执行的未提交；独立durable维护状态
必须清楚记录失败/拒绝，并保持SQL写入publication事实可查询。

## 9. 原断言保护及测试矩阵

| 阶段 | 真实输入/入口 | 必须保留或新增的oracle |
| --- | --- | --- |
| ExistingSDK→SQL | PaimonLanceNativeReadE2ETest 7项 | projection/NULL/VECTOR、PK persisted sequence、bitmap32/64 DV、partial-update reset、aggregation reset、field-ID rename/name reuse/widen全保留 |
| ExistingSDK组件 | PaimonLancePhysicalFileFixtureTest 3项 | SDK真实physicalfooter、nullableVECTOR+originalpositions、共享DV CRC/ranges、SDKsnapshot namespace保留 |
| ExistingNative组件 | LanceBatchReader21 + Commit8 + Filter4 | 33项真实writer/commit/reader/typedFilterTask，不能用其代替SQL版本互读 |
| Append publicSQL双向 | SQL CREATE/INSERT/SELECT +实际SDKreader | Nativephysicalreceipt、journalCOMMITTED、SDK读取全部值/NULL/VECTOR，公共SQL读回相同，文件不创建Dataset_versions |
| Partition/bucket | SQL批量多partition/multi-bucket+SDK差分 | 真实partitionpath/bucket分配，DEFAULT/MOD/HIVE边界，跨worker只有winningfile集 |
| PK/sequence/merge | SDK与SQL交错写update/delete/merge | SDK读与SQL读一致，旧/新sequence、tie、delete-reset、partial/aggregation、spill后全值比较 |
| Overwrite | SQL INSERT OVERWRITE含空输入/静态分区 | 正确移除旧选中数据，不误删其他partition/old pinned snapshot |
| Statistics | SQL写真实mixedNULL/extremevalues | receipt/SDKDataFileMeta的真实count/null/minmax/format/schema，不对unavailable统计填常量 |
| Authority | 真实租户/auth/catalogdeployment | cross-tenant、旧revision、错handle/schema/URI/attempt永久拒绝，无snapshot/文件可见性泄露 |
| Failure/recovery | 真实worker取消/进程故障+PGjournal | producer失败无commit、重复reports不重复publish、commit后崩溃恢复、UNKNOWN不删可能已提交文件 |
| MinIO publicSQL | 真实MinIO+PG+相同双向场景 | SDK和Native同endpoint/bucket/secret绑定、真实范围读取/对象大小/namespace隔离，不用本地warehouse冒充 |
| Multi-process | 实际coordinator与多workerprocess | 不以single-JVM多HTTPmanager代替，admission预算与routing/commit/recovery均实测 |

首单元append-only的green不能省略后续行，亦不能把尚未支持模式改名、禁测或删guard来收窄矩阵。
新增人工负协议输入仅证明validation，不证明真实执行；成功执行的report必须来自实际Native任务。

## 10. 环境与验证命令

现有基座真实Postgres16-alpine，但SharedTestContainers关闭fsync/synchronous_commit/
full_page_writes；它不是crashdurability证据。生产恢复验收使用开启这些设置的真实PGfixture。
现有PaimonE2Ewarehouse为local；未发现真实MinIOfixture。名IcebergMinioIntegrationTest
源码也明确localHadoopCatalog，不能计入MinIO通过。

新增MinIOcontainer与bucket初始化，PaimonSDK真实S3FileIO依赖/插件，统一`s3://`warehouse与
Native`native.storage.s3.*`scope/endpoint/region/path-style/TLS；credentials由实际secret路径
注入，无机器路径/secret入仓。当前paimon-lance在connector/E2EPOM为testscope，生产package
是否需要SDKwriter插件应由唯一Native写设计明确验证，不因测试classpath推导部署已具备插件。

已打包ABI8和Java全编译后的最近SQL读向验证：

```bash
mvn -Pfull-regression -pl pangrova-testing/pangrova-test-e2e -am \
  -Dtest=PaimonLanceNativeReadE2ETest,LanceSqlPublicationE2ETest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

新增反向/MinIO测试纳入最近命令后，仍必须按build-pipeline完整Step0–8复验，包括真实所有
CTest/ASan、重新打包、Javacompile/fullregression、完整E2E和ActivationSmoke。
记录实际发现tests数量、PASS/FAIL/SKIPPED/BLOCKED、命令/environment/日志与根因；
无Docker、未打包、缺runtime或零测试不得写PASS。运行由主线程统一，当前文档不代表已执行。

## 11. 原子实施顺序与冻结

1. 先确认上述SPIexports/callers、统一v2fields与Paimon真实metadataadapter，保存规格。
2. append-only首单元必须一起落地allocation+physicalwriter+receipt+SDKcommit+SQL双向测试，
   不留下无调用者SPI或只移除guard的中间完成声明。
3. partition/bucket/schema/stats实现及SDK差分闭环；保留所有已有合法SQL回归。
4. PKsequence/key physicalschema、Native排序/merge/spill与真实SDK混合写闭环。
5. durablecommit/recovery/currentrevocation、MinIO和多process验收。

共享task.proto与NativeCMake/Rust输入在主线程AB回归窗口内不修改。C线未来functionrequirements
字段18和本单元新字段由根统一分配/生成；不能手改generatedsources、兼容未知旧epoch或
并发覆盖其他agent改动。LanceC ABI如增加writer结构/符号，由双端一致版本与实际打包门槛控制。

## 12. 已复证检查点与尚缺边界

### 12.1 SDK schema authority：PASS（不是 Native 写入/E2E）

`PaimonSchemaBuilder` 原先仅保留 DECIMAL 参数，VECTOR/ARRAY/MAP/ROW 均走无参数 enum。
`PaimonToPangrovaTypeVisitors` 又将 SDK VECTOR 退化成 ARRAY(FLOAT)，维度、DOUBLE 与非浮点
元素丢失。`PaimonTableHandleFactory` 将完整 SDK schema 编成 `{id,name,typeName}`，回读再造
默认 precision/type；这些是三个真实的参数丢失点，不是 SDK 格式版本的兼容性结论。

当前 `PaimonDeclaredTypes` 递归保留完整参数，Lance 使用真实 SDK VectorType；既有
Parquet/ORC 顶层 VECTOR 保留 ARRAY 和实际 dimension property，并拒绝相互矛盾的声明。
浮点 SDK VECTOR 恢复 SPI VECTOR(dimension,element)，INT/BOOLEAN 保留实际 ARRAY 元素。
handle 使用真实 SDK DataType JSON 保留 field ID、全嵌套字段、nullability、DECIMAL 与 VECTOR，
output codec epoch 3 拒绝 epoch 2。`PaimonPhysicalSchemas` 直接调用 SDK ArrowUtils，序列化完整
schema-only IPC，并保留 SDK field ID 与 logical dimension/element；不构造 Java 业务 rows。

最近真实命令：

```bash
mvn -Pfull-regression -pl pangrova-connector/pangrova-connector-paimon -am \
  -Dtest=PaimonSchemaBuilderTest,PaimonSdkVectorTypeTest,PaimonHandleSchemaIntegrationTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

主线程 r2：9 tests，0 failure/error/skip，13.142 s，PASS。真实 SDK catalog fixture 保留
bucket=1，并使用真实 BIGINT `id` 作为 bucket-key；没有转无桶规避 SDK 校验。此检查点不执行
Native physical writer、SQL reverse write、MinIO 或多进程；不得计入这些能力的通过数。

### 12.2 List 与 FixedSizeList 适配：真实数组组件已复证

Native 输入 VECTOR 是真实任务 List，SDK VECTOR 物理 schema 是 FixedSizeList。
`physical_schema.rs` 逐行读取实际 offset/range，保留 sliced 输入、NULL vector、NULL elements、
DOUBLE 精度、全部 Struct/Map/List 字段；错误维度、类型、字段数、字段顺序 fail-closed。
不能用 Arrow 默认 safe cast 把非法维度变 NULL；也不能对有 offset 的 sliced list 取从零开始
的 values。连续布局使用实际 firstOffset 的 values slice，真实 buffer 地址证明可共享；
NULL parent 缺 slots 或非连续布局才拼接，并计量实际适配 payload copied bytes。空 schema
明确拒绝（physical allocation 本就要求非空 columns），真实 zero-column/3-row
RecordBatchOptions 验证不能静默丢行。最近实际命令与全部六条 schema 测试通过见 §12.4。
这仍是 Arrow 数组组件边界，不构成 SDK 版本互通或 public SQL 写入通过。

### 12.3 写入资源边界：payload 接入实际 Task pool，SDK heap 尚未闭环

必须区分三个真实消费点，不得把同一配置重复称为多个硬预算：

- 编码缓存 hint：显式正整数表配置 `lance.native.encoder-cache-bytes` → metadata allocation
  与 recovery identity → canonical `PhysicalWritePolicy.writer_buffer_hint_bytes` → Rust
  `FileWriterOptions.data_cache_bytes(Some(...))`。真实 lance-file 0.20
  `FileWriter::initialize` 将该 **TOTAL** 数除以物理列数；只有 None 才默认每列 8 MiB。
  当前 allocation 缺配置明确拒绝，不借用 `write.batch-memory` 或 Task memoryBytes，亦不
  承诺 hint 等于缓存实际峰值。`keep_original_array=false` 避免编码缓存保活整个输入批次。
- StageGrant：实际调度 `SpillBudget` → 已签名 Task resource grant → Native 的 task/node
  spill policy 与 object-write grant → `FileStage` 写前字节 ledger。每个私有目录的 PUT 与
  multipart part 在写入前累加并拒绝越界；这不是覆盖其他 sink/operator 的共享 task/global
  spill lease。原 SQL fixture 的空 SpillBudget、关闭的 runtime spill 和空 directory 产生
  实际零授权；新增 fixture 使用独立 `native-write-stage`、真实 usable-space 检查与显式
  16 MiB 测试 ceiling，不借用 shuffle 的独立目录或 grant。生产共享磁盘租约仍待实现。
- 上传 payload：真实 SDK `finish` 后取得封口 fileSize，唯一 drain 从 actual Task root
  `addLeafChild` 获取 pool；`LancePhysicalBatchWriter::finish` 通过这个 leaf 真正
  `allocate(fileSize)`。验收要求不足即拒绝，发生在最终 `PutMode::Create` 之前。`PoolPayload`
  持有 pool/ancestor 和实际 buffer，经 C ABI 唯一 ownership transfer → Rust
  `Bytes::from_owner` → 最后一个 HTTP clone 释放后回调归还同一 allocation；没有文件大小
  的 Rust Vec、空 reservation 或 mmap 假计账。是否 cap 拒绝与所有 HTTP clone 按期结束
  均仍须实际验证。

新 `ActualTaskPoolCapRejectsClosedSdkPayloadBeforePublication` 使用实际
`VeloxMemoryGovernor::createTaskPool` 的 1 MiB root/leaf；独立 producer pool 提供确定性的
8 行 VECTOR(65535,DOUBLE)。必须先由真实 SDK 完成 write，再从 staging 文件实测
fileSize > cap、真实 0.3/LANC footer，捕获真正 `MEM_CAP_EXCEEDED`，并断言无最终对象、
后续 finish 永久拒绝、writer 析构后目录清空、pool usedBytes 与 active task 数回到基线。
原 DOUBLE/两类 NULL、Create collision 不覆盖、1 byte staging 拒绝、zero input 和 root NULL
拒绝的全部 oracles 保留。此新增 ABI 9 Native 测试 **尚未构建/运行**；原 ABI 8 全量通过、
Java metadata 最近 27 PASS 或 Arrow 数组组件通过均不能代替该测试、双向 public SQL 或
MinIO 条件创建竞态的验收。

编写该真实 oracle 时发现源码根因：`PangrovaArbitrator::growCapacity` 的首次和重试 grant
成功分支忽略 `growPool` 的 boolean。pinned `MemoryPoolImpl::grow` 在超 maxCapacity 时
返回 false，而不会自动抛 cap exception；不得把这次未提交的 pool grow 报告为成功。
并行修复单元必须退还 listener 已授予的真实 capacity，再报告 `MEM_CAP_EXCEEDED`，
两分支均覆盖。此为源码证据，尚未通过新测试复证，不能写成已运行暴露或已解决。

SDK `max_page_bytes` 仅为提示，单 value 可超过它；SDK 的初始 upload buffers、编码临时
分配和 multipart futures 未接实际 scoped allocator/peak-memory hook。因此真实 Task pool
只硬约束其本身分配的输入/输出及上传 payload，不能覆盖 Rust heap。适配器实际
`copied_buffer_bytes` 只表示必要数组适配的 payload 拷贝量，尚未接入完整 canonical usage，
不能冒充 allocator 复制总量或峰值。大规模并发 admission、共享 spill/object-write lease、
SDK heap 硬计账和完整生命周期 telemetry 仍是强制生产门槛。

deadline/cancellation 亦未闭环：当前实际 SDK `FileWriter` 无公开 async abort/drain API；
其内部 `ObjectWriter` 的 multipart Drop 仅启动 abort，并不返回可等待的完整清理结果。
不得添加无法证明 cleanup 的 C ABI 假接口或将 canceled Create 当作 NOT_CREATED。后续
必须先取得可审计的 SDK async drain/abort 实现，绑定既有 canonical deadline/cancel，保持
task/pool/body owner 至真实 futures/cleanup 结束；Create 响应丢失按 UNKNOWN 进入 durable
attempt/fence journal，只有永久不发布 disposition 才允许最终文件清理，不竞态删除。

### 12.4 ABI 9 Rust 最近检查点：组件 PASS，不代表物理写发布完成

2026-10-02 在实际 aarch64-apple-darwin 环境执行相同命令，使用 hermetic protobuf 25.3
与 well-known types、`MACOSX_DEPLOYMENT_TARGET=13.0`，锁文件不变：

```bash
cargo test --locked --release --target aarch64-apple-darwin physical_
```

r1 编译 FAIL：18 个 E0425，测试尚未运行。根因是 `ffi_guard!` 的裸名称在新增
`physical_write_ffi` 调用点解析，子模块没有 root module 的 panic imports。修复宏自身，
使用 `::std::panic::catch_unwind/AssertUnwindSafe` 与 `$crate::set_last_error/panic_message`，
不移除 panic protection、不向每个调用点补重复 import。新增真正 child-module panic 的
回归，验证 failure sentinel 与实际 thread-local last-error 诊断。

r2 原命令 PASS：release 编译 2m21s，**11 passed / 0 failed / 0 ignored / 22 filtered**，
0 warning/error。实际选择 payload ownership 3 条（包括跨线程最后 clone drop、非法
payload exactly-once release、未发布 owner 清理）、schema 6 条、新增 panic guard 1 条、
scan_identity 实际 sparse offsets/buffers 1 条；不把 filtered 测试计为通过。
日志：`/tmp/pangrova-production-lance-physical-boundaries-nearest-r1.log`、
`/tmp/pangrova-production-lance-physical-boundaries-nearest-r2.log`。

本命令确实编译当前 Rust writer/ABI 9 源码，但仅执行所列组件测试；没有运行 Native
writer fixture、MinIO、SDK catalog publication 或 public SQL 两次 INSERT/zero input。
它不替代最终顺序执行的 Step 0 static-library build、后续 Native/package/JNI、E2E、
Smoke 与生产资源/并发验收。

### 12.5 完整 Rust 专项：33/33 PASS，包含实际 MinIO 条件提交

使用 §12.4 相同 protobuf/target/deployment 环境，冻结整个 canonical v2 与 proto-imports
生成输入，执行不含过滤或 ignored 参数的完整命令：

```bash
cargo test --locked --release --target aarch64-apple-darwin
```

实际结果：release 编译 **2m17s，0 warning/error**；测试 **33 passed / 0 failed /
0 ignored / 0 filtered**，执行 560.63s。原命令没有重启、缩 scope 或增加 timeout。
日志：`/tmp/pangrova-production-lance-full-rust-nearest-r1.log`。

全部发现的测试均执行，包含 SDK 真实物理 0.3 文件的投影/NULL/精确范围、真实 Dataset
deleted holes/历史 manifest、payload 跨线程 last-drop、严格 schema/storage/provenance
边界、runtime shutdown/reinit、transaction sidecar 声明范围与两项真实 MinIO：
`minio/minio:RELEASE.2024-01-16T16-07-38Z` 的不支持 conditional create 明确拒绝，
`tobi312/minio:RELEASE.2025-02-28T09-55-16Z` 的并发单胜者与迟到写不覆盖。CAS 测试
提交的是 manifest marker，不是新 physical writer 的对象或 SDK catalog publication，
不能借这些通过结果宣称该后续链路已完成。

运行中前 31 项完成、两项 MinIO 启动等待超过 60s。实际 guest 为 aarch64/2 CPU/
约 4 GiB，MemAvailable 约 55 MiB、无 swap；MinIO 进程在早期启动有上万 major faults。
VM 进程实测发现本项目依赖编译 `cc1plus` 约 1.6 GiB、k3s 约 643 MiB、dockerd 约
307 MiB 的并发占用，并非业务测试容器内存泄漏证据。主线程负责构建资源串行化；本线
不修改 guest 设置或无关容器，不取消原测试，最终两项 MinIO 均实际 PASS。执行时长应
作为该环境的真实耗时记录，不包装成低延迟/资源隔离性能数据。

完整 Rust 专项仍不替代最终同源 Step 0 static library、Native/ASan、重新打包/JNI、
public SQL 两次 INSERT/zero input、Paimon SDK 0.39 双向读取与生产分布式验收。

### 12.6 唯一 ABI 10：真实 copy usage 必须跨失败生命周期保留

上节完整 `cargo test` 的 0 warning 不能代替生产 `cargo build`。随后主线程实际执行
`build-lance-ffi.sh` 编译 3m16s、exit 0，但生产发现 `AdaptedBatch.copied_buffer_bytes`
never read 的 1 warning；按 zero-warning gate 为 FAIL，未进入 Step 1。

根因是原计量只由测试读取，生产 `PhysicalWriter` 不消费它。当前 adapter 将真实
concat 输出-buffer 长度直接累加至调用方持久 counter；不是在 append 成功后才记账。
后续字段类型/NULL 校验失败或 SDK 写入失败也不能清零。成功与失败路径使用唯一
canonical `ResourceUsage.columnar_conversion_copied_bytes=19`，不塞入 input_bytes、heap
peak 或 provider token。新 partial-adaptation 真实数组回归验证成功拷贝、后续字段失败
后的第二次实际拷贝累计、前置拒绝不抹除历史值；所有原 33 条测试均保留。

新增 required C symbol `lance_ffi_physical_writer_resource_usage` 返回同一个 canonical
ResourceUsage 的序列化快照，不创建私有统计 struct/协议；failed handle 在 close 前仍能
读取，invalid handle 明确拒绝，不伪造零 usage。新符号使唯一当前 C ABI epoch 升至
**10**，C header 与实际返回值同步；不保留 ABI 9 分支。Native writer 在成功/失败 append、
stage/PUT 失败、close/析构前读取；正常 close 的读取失败必须使 finish 失败，已有统计
在解析失败时保留，不以默认零替换。`PhysicalWriterConfig.resourceUsagePublisher` 收到
累计快照，Task owner 必须 assign 或只合并新增 delta，不得在每次读取时重复加总。

原 Native SDK 1 byte staging 拒绝测试新增真实 canonical usage getter/publisher 的正值
和一致性断言，round-trip 验证复制数在 finish 后不重复增加；这些 Native 断言尚未运行。
主线程随后同 Step 0 原命令完成 ABI 10 production build：2m06s、0 warning/error。
本线同 §12.5 原完整 Rust 命令重跑，实际发现并执行 **35 passed / 0 failed / 0 ignored /
0 filtered**，release 编译 2m08s、测试 1.99s、0 warning/error。原 33 条与新增的真实
partial-adaptation 累计/非法 getter handle 两条均 PASS，两项实际 MinIO 亦保留并通过。
日志：`/tmp/pangrova-production-lance-full-rust-nearest-r2.log`。这些结果不能替代新 Native
writer/Task-owned metrics → terminal ResourceUsage 的失败路径接线与 Native/后续全链。
共同执行单元仍需完成这些验证，不能因 getter 存在就宣称已完整报告。

## 13. SDK 取消根因：先保留可等待的工作所有权，再观察发布结果

### 13.1 实际调用与断点

pinned `lance-file 0.20.0` 的 `v2::FileWriter::encode_batch/finish` 原先使用
`collect<Result<Vec<_>>>` 收集各字段任务，再 await external-buffer I/O，最后才将任务交给
局部 `FuturesOrdered`。`lance-encoding 0.20.0` 的 legacy primitive、list offsets 和
structural primitive 已在构造 `EncodeTask` 时分别 eager `tokio::spawn/spawn_cpu`。后字段
构造失败，或 write/finish Future 在 I/O await 时被取消，会丢掉已派发任务的接收者；
实际编码仍可继续持有外部输入 buffer。只修 top-level queue 不足以覆盖 nested
struct/list 在前 child 已派发、后 child 报错时丢弃任务的问题。

`lance-io 0.20.0::ObjectWriter` 原 Drop 仅在 InProgress 时 spawn multipart abort，
既不 await JoinSet，也不等待真实 abort 返回；CreatingUpload、PuttingSingle、Completing
没有可等待的清理接口。`PhysicalWriter::finish_stage` 又在 await 之前 take writer，
使 SDK finish 错误或取消后失去 retained cleanup owner。最初 ABI 10 checkpoint 未接入
下述 SDK 扩展；现已在源码切换为唯一 ABI 12，新的 Step 0/Native 验收尚未完成。

### 13.2 repository-owned 唯一补丁，不改 Cargo registry

`patches/lance-writer-drain.patch` 精确覆盖 SDK 十七个文件、四个 crate：

- `lance-io/src/object_writer.rs`：state-owned Aborting Future、拒绝新写、真实
  `abort_all → join_next` 全部已派发 part，再 await `MultipartUpload.abort`。
- `lance-file/src/v2/writer.rs`：writer-owned pending encoding queue，逐字段立即接管
  任务；abort 等待所有剩余编码，继续记录错误，随后显式 object cleanup。abort Future
  自己被取消也不能丢掉 queue/cleanup Future。
- `lance-encoding/src/encodings/logical/{primitive,list}.rs`：仅将 write encoder dispatch
  延迟到 EncodeTask 首次 poll，防止未归属任务被派发；不修改 CPU pool 并发默认值。
  仍由 FuturesOrdered 同时驱动所有已拥有任务，不逐任务 spawn 后 await 导致串行化。
  trace-level `encode_page` span 位于实际编码 closure 内；没有生产 barrier 或 test Encoder。
- 完整 path dependency 编译暴露了原 registry dependency 被 Cargo cap-lints 隐藏的
  十条警告：io 两条借用生命周期，encoding 七条借用生命周期和一个重复 paste import。
  补丁明确返回值的 `'_` 借用关系并删除宏作用域之外的冗余导入，不禁用 lint；涉及
  object_store/scheduler、decoder、binary/blob/struct decoder 以及 fastlanes。
- `lance-file/src/writer.rs`：legacy writer 显式 await 原有 ObjectWriter 的清理。
- `lance/src/dataset{.rs,/write.rs,/updater.rs,/write/merge_insert.rs,/fragment/write.rs}`：
  GenericWriter 的 abort 是必需方法，legacy/V2 两个实现均消费真实 SDK drain。
  stream/progress/write/tell/finish/update/merge/fragment 错误保留 writer，await 清理后
  返回原错误及真实 cleanup 诊断；不先 take writer 再 await finish。捕获操作 panic 后
  同样显式 await 清理，不把错误转成成功。default/blob 的 futures::join 并发不变。

`bin/prepare-lance-sdk.sh` 校验四个原始 `.crate` archive SHA256、十七个源码 pre/post
SHA256、四个完整 patched crate tree SHA256，正向 apply-check 与反向检查；prepare 到
独立 owned source 目录，不修改 registry，也不接受其他文件漂移。已实际执行 shell
syntax、原始 archive prepare、重复 prepare、完整 verify 与 reverse-check，通过；这仅是
来源/补丁真实性检查，不是 SDK 编译或运行通过。

当前补丁 SHA256 为 `b0f2587f4bb1ae0ea31f13028d9a56da199f9c3e88a3553a0283a219bb206d0b`，
prepare 工具为 `e27d23491dc85c444bbf58f7dada0d964792e1c3e69f488764aa2ce5adc44fcc`。
完整 patched tree：io `4d37b8acef263e71a0bcbf2e376f7348c8874cdff64bf92a5cc880a19f54b421`；
file `58b5cfa12fbf3b84c6deee0162df110cd95cbbf9aa149bebf0538d74c9d59749`；
encoding `f545f27491068ce3d20107ccd894b6a467f7bd49f947bd8e21411ee52fc9d36c`；
lance `7c7ced7068ba8d4c51e9c453d05ff0757e618f29dda4966e47abd2e617c7c68d`。
已实际执行四 archive 验证、完整 patch apply-check/apply、prepare upgrade、verify/reverse-check、
shell syntax 与两个 `cargo generate-lockfile --offline`，均 exit 0。升级先验证此前三个
完整已审 tree，再将原目录保存为精确同父恢复备份，唯一 canonical owned 路径发布新树；
漂移拒绝，没有 registry 修改或生产兼容分支。新的编译/运行结果仍待统一复验。

### 13.3 发布观察与后续唯一调用点接线

清理结果区分 NOT_PUBLISHED/PUBLISHED/UNKNOWN，并保留真实错误：未派发对象或成功
abort 的未完成 multipart 可观察为 NOT_PUBLISHED；已发 single PUT / multipart complete
必须等待实际结果。成功为 PUBLISHED，响应/请求错误为 UNKNOWN；禁止尝试删除以使状态
“确定”，禁止将超时或 unavailable 环境写成有效 UNKNOWN 场景已验证。

主 Cargo 以唯一 `[patch.crates-io]` 消费三个 owned crate；build 工具先 prepare/verify 全部
来源。`finish_stage` 保留同一个 SDK writer 至 finish/abort 观察结束，不在 await 前 take。
唯一 ABI **11** 删除旧 `physical_writer_close`，改为显式 `drain / destroy`：drain 总是
保留 opaque handle/runtime，await 全部实际 SDK 工作后输出 joined、private-stage-cleaned、
仅私有 stage 的 publication 三项独立事实，错误仍返回真实诊断；destroy 必须同时具备
joined 与 private-stage-cleaned，拒绝未完成清理的 owner。C++ `close` 失败保留可重试 owner；
已观测任务失败且两项本机资源事实均成立时，`releaseDrained` 可释放 owner，但不得抹除
原错误或改变 UNKNOWN。Task-owned writer 必须由唯一 drain/driver release 消费，不能依靠
析构日志判成功。实际 filesystem NotADirectory 失败/恢复/同 handle 重试和过早 destroy
拒绝已有 Native 源码验收，尚未运行。Task 的 active JNI write/deadline 打断信号尚未复证，仍是后续强制闭环，不因 SDK
abort 存在就承诺即时 cancellation。最终 Create 的 UNKNOWN 仍由既有 durable write
attempt/fence journal 决策；SDK staging 清理不是最终对象删除授权。

### 13.4 独立真实验收，不与当前 Native archive 混用

`lance-ffi/sdk-tests` 为独立 Cargo target/lock，不覆盖主 FFI 最终 archive。测试范围保留
六个真实场景：local 未发布与晚写拒绝；local 已发布对象不删；实际 CPU slot 被占用时
取消 write Future/abort Future，释放 slot 后等真实编码 drain、外部 Arrow owner 最后一次
释放；独立双 CPU 子进程由实际 encode span 观察两个不同线程同时进入，最终产出真实
SDK 文件；MinIO multipart abort 并检查 incomplete uploads；MinIO 真 PUT 成功、Toxiproxy
丢失返回后的 UNKNOWN，独立直连验证 matching 对象原字节不变。

barrier 只位于测试 subscriber/真实 CPU runtime 调度控制，不代替编码器、I/O、输出或
终态。Toxiproxy downstream timeout 的固定语义已核官方 pinned
[2.9.0 源码](https://raw.githubusercontent.com/Shopify/toxiproxy/v2.9.0/toxics/timeout.go)，
实际对象必须从未受故障链路影响的 MinIO endpoint 读回，不能仅制造连接失败就声称
验证了未知发布。独立 test lock 首次 offline 准备因 test HTTP 客户端默认 TLS/charset
依赖缺失而 FAIL；收敛为仅所需 plain HTTP features 后原 lock 准备命令通过。

2026-10-02 独立 SDK r1 完整命令 `CARGO_BUILD_JOBS=2 LANCE_CPU_THREADS=1 cargo test
--locked --release --target aarch64-apple-darwin` 实际发现执行 6 项：4 PASS、2 FAIL、
0 ignored/filtered；构建 5m02s、测试 32.02s。日志
`/tmp/pangrova-production-lance-owned-sdk-drain-nearest-r1.log`。两项 MinIO 测试在真实
Docker 拉取官方 `ghcr.io/shopify/toxiproxy:2.9.0` 时 HEAD 超时，尚未进入 SDK I/O，
根因是 daemon 直连 GHCR 不通；环境阻塞不是有效 UNKNOWN oracle。该轮还存在上述
10 条 SDK warnings，整体 FAIL；修复源码与来源校验已完成，完整重跑尚未通过。
完整 r2 已实际修正 10 warnings（构建 1m31s、0 warnings），但 2 项远端 FAIL 是
fixture 在 `with_allow_http(true)` 后调用 `with_client_options(default)`，覆盖真实 HTTP
许可；pinned object_store builder 的 setter 语义已核对，HTTP 许可现绑定在最终 options。
原完整命令 r3 已 **6/6 PASS**，0 failed/ignored/filtered/warning，构建 1m15s、测试 2.42s，
日志 `/tmp/pangrova-production-lance-owned-sdk-drain-nearest-r3.log`。两个远端 oracle 均实际执行。
宿主机以已有代理下载同官方 GHCR arm64 manifest
`703202eaa8a59fb975175ab0c7796424e0a228d9121e124e4e04a476b047a5f9`，逐 config/layer
校验 SHA256 后 docker load，实际 RootFS diff IDs 与官方 config 一致；未修改 daemon/VM 设置。
这不等于 ABI 11 FFI/Native/public SQL 验收。新的 SDK 原子单元必须
切换唯一生产 dependency 后同源重跑 Step 0、完整 Rust 与 Native/后续全链，不能借
§12.6 的 35/35 通过复用为新补丁通过。

### 13.5 实际 Arrow / task-vector 布局与所有权断点

完整 Native 回归中的真实 DOUBLE/父 NULL/元素 NULL 文件 roundtrip 失败于 pinned
`velox/vector/arrow/Bridge.cpp::importFromArrowImpl`：仅支持 `+l` List schema，不支持
Lance 真实 `+w:3` FixedSizeList；同 importer 还拒绝任何非零 C Data offset。
`columnar/ColumnarImportLayout` 是统一 Arrow 布局边界，不解释 SQL 类型：FixedSizeList
转换为 List 时仅新增实际 task pool 的 int32 offsets，保持 DOUBLE payload 与完整 NULL
slot；切片值 buffer 使用真实起点共享，无法共享的位图对齐使用真实 CopyBitmap 并计量。
新 offsets 的实际 buffer capacity 单独记录为 allocation，不冒充 payload copied bytes。
schema 和 batch 使用同一转换，字段名、NULL 声明与元数据保留。四项源码 tests 校验
原始/切片/嵌套 sharing 地址、NULL、精确 copy/allocation、owner 最后释放、无列 N 行和
非法布局拒绝；原 roundtrip 全部业务 assertions 保留。尚未构建/运行，不宣称通过。

另一个根因是 upstream importer 只在成功之后才清空 caller release slots，但失败时
已由其 copied release-owner 执行回调。调用者 RAII 会再次释放，或持有悬空 callback。
准备的 sole SDK ownership hunk 使用一次 `make_shared` 分配 schema/array co-owner，再
立即移动两个 callback，以 aliasing shared_ptr 绑定实际 buffers；分配失败仍由 caller
持有，转换失败与最后 child buffer 释放均 each-once。hunk 尚未合入/重建 Velox archive，
必须由统一 SDK patch/provenance 升级完成；三项实际 C Data tests 保留正常最后子引用、
unsupported layout 失败、真实 capped task pool sizes allocation 失败的 release 计数。

### 13.6 独立 Dataset 不继承 physical drain 的完成证明

独立 Dataset 是合法的另一种 metadata authority，不是 Paimon 的降级路径；当前真实
SQL fixture保留 publication 两项、DDL/session 两项和 FeatureView export一项。Paimon
fixture保留 SDK→public SQL read七项及 public SQL→SDK/public SQL write两项，均无
assumption、runtime-unavailable skip 或假推理/存储 provider。新 zero-input 写用例在原
SQL empty、SDK scan empty 之外，增加 `Files.walk(actualTable.location)` 检查精确本地
目标内无 `.lance` 文件；不会删除文件，也不把本地验证写成 MinIO 验收。尚未执行新包 E2E。

此前 ABI11 的明确 drain/destroy仅适用于 physical writer，不自动补齐 Dataset producer。
`lib.rs::start_writer` 仍调用 Lance 0.20 `InsertBuilder.execute_uncommitted_stream`；
`ChannelBatchReader::next` 取消后返回实际 stream error。上游
`dataset/write.rs::do_write_fragments` 的 `batch_chunk?`、progress、write、tell 和
`writer.take().finish().await?` 错误路径直接丢弃 `Box<dyn GenericWriter>`；其 trait与
`V2WriterAdapter` 没有调用 owned FileWriter 的显式 abort。外层
`lance_ffi_writer_close` 的 thread join只证明这个 Rust 线程退出，不证明内部实际已派发
的 CPU JoinHandle/upload tasks已 joined；ObjectWriter Drop只 spawn best-effort abort。
这是本次修复前的源码根因，不能把旧 ABI11 的测试结果写成该路径已验收。

当前源码已把同 pin 的 Lance Dataset GenericWriter错误清理纳入唯一 owned SDK补丁：
实际持有 writer，停止新IO，在每个 stream/progress/write/tell/finish 失败分支显式 await
file encoder/upload drain后返回原错误；finish期间不能先 take丢失重试 owner。保留实际
default/blob并发；legacy writer若缺同等公开 cleanup契约必须先核实现与验收，不能用
默认成功的 trait 方法掩盖。ABI12 移除 Dataset void close，以共同 WriteDrainEvidence
承载实际 joined/private-stage-cleaned 事实，drain 保留 runtime/handle，destroy 仅在
SDK 和线程实际退出后可用。Dataset 没有私有 FileStage，不把这一事实冒充 final object
NOT_PUBLISHED；publication 字段为未观察。Dataset C++ writer 持有真实 shared task pool，
显式 close 失败保留完整 owner，releaseDrained 只依真实资源证据。B 的 task-owned sink
接线必须在 producer join 后重试 drain，并在失败 TaskReport 保留错误，不把退款当成功。

新 SDK 组件用例保留原六项，并新增四项实际 oracle：保留 GenericWriter 后取消真实 write
与 abort Future、真实 CPU queue 及外部数组 last-drop；InsertBuilder 消费真实 Arrow
构造失败 stream 并在返回时 owners 释放；已 finish 文件遇到真实本地 I/O 错误后 bytes
保持；legacy Dataset 对真实 MinIO multipart 的停止/归零。主 Rust 再增加真实 Arrow
数组失败移交每次 consume/reset/last-drop 和缺失 owner 拒绝用例。所有新用例尚未运行。
明确边界：GenericWriter Future-drop 后必须保留 writer 并显式 await abort；Insert 的
合作 stream error 保持外层 Future 持续 await。直接 drop 外层 Insert Future 不是清理
完成证明，当前 C ABI 使用 ChannelBatchReader 合作取消，不以 drop 冒充 drain。

当前 physical drain只删除私有 FileStage目录，不删除其 Create目标；provider
`abortUncommittedFiles` 缺 durable non-publication disposition 时明确拒绝。
Dataset `discard_fragments_impl`只删除精确 transaction sidecar，不删除 manifest/data files；
这不能代替恢复 journal对 UNKNOWN的判定，也不能宣称完成 orphan回收。
