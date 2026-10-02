# ADR: Lance 写入作为统一 v2 分布式写提交的一种物理 sink

状态：待架构复核；本文是实施规格，不代表 SQL Lance 已接通或通过 E2E。

## 2026-10-02 当前源码与验收边界

当前唯一 Lance C ABI 已为 **11**：physical writer 的 `drain` 保留真实
handle/runtime，返回 joined、private-stage cleanup 和 staging publication
证据；`destroy` 只接受已真实排空且清理完成的所有者。旧 ABI7 及以下是历史
检查点，不是并存接口。主 Rust Release 构建及完整36项测试已通过，零过滤/跳过；
Native、JNI、SQL和完整Step0–8尚未通过最新输入的验收。

Paimon 的物理文件分配、读取及 Native writer 已在同一 v2 `physical` binding
接线，但目前明确受限于 unpartitioned、bucket=1、append 的声明。不能将这些
源码或局部测试称作主键表、分区表、覆盖、删除及大规模写入的完整闭环。
独立 Dataset connector 服务其真实 Dataset catalog/version/manifest 权威，
不参与 Paimon physical-file 提交；删除它会移除已有的合法 SQL 能力，不是清理死代码。
两类 authority 共享 canonical TaskRequest、Native 执行及 Flight，按绑定语义排他，
不是失败后切换的 fallback。

### 固定 Paimon 1.4.1 的最终提交权威复证

已直接核对本机 `paimon-core-1.4.1-sources.jar` 与发布 tag，不用 master API
推定当前能力。`FileStoreCommitImpl.commitSnapshotImpl` 最终调用
`SnapshotCommit.commit(snapshot, branch, statistics)`；此签名不包含 Pangrova
execution owner 条件。[固定版本接口](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/SnapshotCommit.java)。
`CatalogSnapshotCommit` 只传 table identifier、table UUID、snapshot 和统计；
UUID 检查表身份，不能代替任务 execution fence。
[固定版本 catalog adapter](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogSnapshotCommit.java)。
`RenamingSnapshotCommit` 在 catalog lock 下执行 `tryToWriteAtomic`，但没有
执行 owner 检查；没有 lock factory 时 `CatalogEnvironment` 使用空 lock。
[固定版本 rename adapter](https://github.com/apache/paimon/blob/release-1.4.1/paimon-core/src/main/java/org/apache/paimon/catalog/RenamingSnapshotCommit.java)。

本仓实际调用为 `TaskStatusReceiver` 的 owner 检查 → 持久意图 → 再检查 owner
→ `PaimonDistributedWriteFinisher` → `InnerTableCommit.filterAndCommit`
→ 上述 snapshot commit → 最后检查 owner。这证明当前检查不在最终原子提交点，
不是证明已经发生旧 owner 写入；目前没有对应双进程故障实测。

修复必须先确定一个最终可见性权威，再编码，不在调用前追加一次检查：

1. 持久意图固定 winner 集、output binding、base snapshot、schema、generation
   和 fingerprint。恢复只更换当前执行授权，不改原 winner provenance。
2. 最终提交权威在同一次原子可见性更新中，验证当前 execution fence、精确意图
   digest、目标表身份和 snapshot 前置条件，并记录可查询的提交结果。Paimon
   必须在其 catalog-managed snapshot 提交点接入此条件；不能靠本地 JVM 锁，
   也不能把 `CommitPreCallback` 的前置检查称作原子保护。
3. Dataset 的 manifest 条件创建只裁决物理版本冲突。其目录可见版本更新必须
   同样原子验证执行授权与已持久化的 manifest identity；物理 manifest 存在
   不等于 SQL 已提交，GC 不得删除结果仍 UNKNOWN 的发布对象。
4. 超时和 ACK 丢失进入 UNKNOWN，查询相同 fingerprint 的真实提交结果；仅有
   无提交证明才能结束为 NOT_COMMITTED。跨网络调用时持有数据库行锁仍不能
   排除超时后旧远端请求迟到，不把该方案当作最终权威。
5. 缺乏这一原子条件的 catalog 不得声明支持 fenced distributed write；不是
   自动切换到另一提交方式。需先完成真实 catalog 的实现与能力声明，不能
   只新增一个 SPI 后宣称闭环。本节是待实施规格，不是已有生产能力。

验收保留原合法 SQL 和全部存储 oracle，并在真实双 Coordinator / 多 Worker / catalog
中覆盖：owner 接管发生在检查后和原子提交前、旧 RPC 超时后迟到、同意图重复提交、
不同 digest 冲突、ABORT 与提交并发、catalog 成功但 journal ACK 丢失及重启恢复。
须逐项断言最终可见 snapshot、提交 identity、journal 状态、对象保留及资源释放；
仅靠最终行数或“第二次重试成功”不能验收这一不变量。

## 2026-10-01 历史复证（不代表上述当前源码）

Paimon 依赖已包含 `paimon-lance`，split 已输出 `FILE_FORMAT_LANCE`，Rust 已通过
官方 physical-file decoder 读取其单文件；旧的“插件/物理 reader 未接通”判断撤销。
当前唯一 Lance C ABI 为 **7**；ABI4/5/6 文字仅记录以前修订，不能作为当前接口规格。
Paimon 表的 Lance 格式复用 Paimon snapshot/manifest/CommitMessage，不需要独立
Lance connector；直接注册独立 Dataset 时才需要其 metadata provider。不能交换两者
提交权威。当前 Paimon 写仍经过 IPC→JVM GenericRow→SDK，未达到 Native-only。
SDK Lance 0.39 与 Native 0.20 必须取得真实 public SQL 双向互读证据，不能推定兼容。
源码没有 Lance runtime-unavailable skip，但 Native/SDK 组件 PASS 不替代 SQL/多进程
验收。完整根因、清理范围和实施单元见
[单路径修复规格](ai-native-single-path-root-cause-design.md#5-lance-决策与互通验收)。

## 2026-09-30 架构更正：Lance 不等于一种新的目录服务

本轮按发布源码复证，而非从当前 classpath 推导上游能力：

| 层级 | 官方事实与工程现状 | 唯一所有权及实施约束 |
|---|---|---|
| Iceberg 表格式 | [Iceberg 规范](https://iceberg.apache.org/spec/) 的 data-file 格式没有 Lance；Puffin 的存在不等于 Lance 数据文件支持。 | Iceberg 仍负责源表 snapshot；Lance 向量/检索 artifact 可以旁挂，不能把 `.lance` 伪装成 Parquet 写进 Iceberg manifest。 |
| Paimon 1.4.1 表格式 | [发布 tag 的 paimon-lance 模块](https://github.com/apache/paimon/tree/release-1.4.1/paimon-lance) 已有 Lance FileFormatFactory；本仓 `paimon-core` / `paimon-format` 依赖未包含该插件，且现有 Native reader 未实现该物理文件适配。 | 使用现有 Paimon catalog、snapshot、manifest、CommitMessage 和恢复语义；不创建第二套表目录，不替换 Paimon commit 为 Lance Dataset commit。 |
| Paimon 的 Lance 物理文件 | [LanceWriter](https://github.com/apache/paimon/blob/release-1.4.1/paimon-lance/src/main/java/org/apache/paimon/format/lance/jni/LanceWriter.java) 调用 `LanceFileWriter.open(path, allocator, null, storageOptions)`，是文件 writer，而不是 Dataset manifest。 | 在同一 canonical v2 文件 split/write 契约中实现 Native physical-file reader/writer；保留 Paimon row kind、主键合并、删除向量与 schema evolution。 |
| 独立 Lance Dataset | 外部 Dataset 自己维护 fragment / manifest / dataset version。现有 Native FFI 接口属于此层，不是上面的 Paimon 物理文件接口。 | 只有直接把独立 Dataset 注册为 SQL 表时，才需要对应的 metadata provider；`pangrova-connector-lance` 不能成为 Paimon 表、Embedding 或向量索引的前置依赖。 |

原文“Paimon 1.4 不支持 Lance”和“所有 Lance 使用都必须独立 connector”的推导撤销。
当前产品缺口是插件及 Native 物理文件链未接通，不是上游 SDK 缺少格式能力。
不得通过放开后缀检查、默认 ORC/Parquet、Java 全行扫描或把 Dataset URI 塞进
Paimon data-file metadata 来掩盖缺口。

实施顺序：先完成通用 Worker-local storage binding 与 Native 有界分区扫描底座；
然后补齐 Paimon 单物理文件适配。独立 Dataset metadata provider 仅服务直接 Dataset 表，
两种存储语义共享 `TaskRequest`、资源准入、attempt/owner fence、Flight 和结果契约，
不共用错误的提交机制，也不引入历史 parser 或失败降级路径。

Paimon 格式验收必须由真实 `paimon-lance` 1.4.1 fixture 产生数据并由 Native 读回，
再由 Native 写入、Paimon SDK commit/读回；同时覆盖 Parquet/Blob/Lance 共存、VECTOR、
snapshot 隔离、schema evolution、主键合并和删除。Lance 文件编码版本与当前 Rust crate
的交叉兼容必须以互读结果证明；不能用独立 Dataset fixture 的成功替代。当前这些验收未运行，
状态为未接通，不是 PASS 或 SKIPPED。

## 2026-09-30 当前实施与验证边界

统一 `runtime.proto` 已增加 metadata-only schema fingerprint / finalization 命令，
不是 Lance 私有协议。Coordinator 通过已认证 Worker HTTP 调用同一 RuntimeCommand，
不加载 JNI、不接收数据行；Worker 复用真实 catalog deployment 与 TaskManager fences，
Native 转换实际存储 schema 并调用 Rust 条件发布。新版唯一 C ABI 为 5；旧签名已删除。

发布采用严格 `published_version = base_version + 1`：CREATE、APPEND、OVERWRITE 都不
自动重基。否则 Native 的第二个并发 APPEND 会产生更高的物理版本，而目录 CAS 拒绝，
导致不可见行累积并使之后的写入继续失败。冲突必须终止该 allocation；不得改变其 base、
write ID 或 winner 集重试。实际 manifest 文件 SHA-256 与精确版本须先写入 durable journal，
然后才能执行目录可见性 CAS。FFI deadline 包括存储提交与实际 manifest 读取；超时和 I/O
失败为 UNKNOWN，绝不能从错误字符串推断 NOT_COMMITTED。

恢复身份分为两个通用字段：`execution_owner` 是当前后端签发的授权，Worker 必须实时验证；
`intent_execution_owner` 是 PENDING journal 固定的原 winner provenance。所有获胜片段
必须匹配后一身份；当前 fencing token 不得小于原 token，同 token 的身份必须完全相同。
获胜任务的 control term 可以小于当前 term（查询期间控制 leader 可更替），但不得超前。
恢复不能改写获胜任务的旧身份，也不能把旧身份当作当前授权。

Rust Release 构建已通过；当前 Native / Java 新增控制链测试正在执行。真实 SQL、多独立
Worker、MinIO、ACK 丢失及完整 Step 0–8 尚未取得当前源码的通过证据，不能据此标为闭环。
无 hint 的恢复扫描已改为 lazy 全相关历史并受 deadline 约束，不再永久受 64-version 截断；
持久分页游标、经可见性证明的 orphan GC 和大规模恢复压测仍是后续验收项。

## 2026-09-27 源码核查：当前不可直接接通的边界

下表记录当前源码的实际调用边界。Rust FFI Release 构建、`cargo test --locked --lib`
14/14 和 Lance CTest 32/32 均通过且无 skip；这些结果只覆盖 FFI 和 Native 的局部读写。

| 边界 | 当前行为 | 接通前必须完成 |
|---|---|---|
| `task.proto` 的 `LanceWriteBinding` | 只有 URI、模式、读版本、schema fence、write ID、staging prefix、generation；没有 `CatalogExecutionBinding` 或有序写入列 | 在同一 v2 message 中增加这两项，并在 C++ parser、Java dispatch、Worker catalog lease 中验证；不得从 `target_table` 或当前位置的 catalog 推断 |
| `ConnectorWriteBindingFactory.create` | 总是输出 `WriteOutputBinding.connector`，只序列化 `ConnectorOutputTableHandle` | 由独立 Lance connector 的输出分配接口选择 `lance` oneof；handle 的版本化快照仍留在 durable journal 中供恢复与目录 CAS 使用 |
| `TaskLifecycleManager.bindWriteResources` → `WorkerCatalogRuntime.bindTask` | 所有 WRITE 在 Native 启动前进入此链，而 `bindTask` 只接受 `connector` | 同一入口按 oneof 分支：Lance 取得精确 catalog revision/tenant lease 并校验 URI、schema 和列，但不创建 PageSink；`NativeWriteExecutor` 对 PageSink Lance 的拒绝保持 |
| `TaskStatusReceiver.commitDistributedWriteIfNeeded` → `DefaultWriteCommitReconciler.decodeExactHandle` | PENDING journal 后直接调用 connector `finishDistributedWrite`；恢复只接受 `connector` | Lance 先验证获胜 attempt 与 fragment，再写入可恢复意图，经已认证且有 owner fence 的 Worker finalization 原子发布 manifest，持久记录发布 identity 后做 catalog CAS；恢复必须沿同一意图和 fingerprint 重驱 |
| Native `lance_ffi_commit_fragments` / `lance_ffi_discard_fragments` | C ABI 和局部测试存在，生产 C++/JNI 与 Worker 内部控制面无调用方 | 增加唯一 finalization 控制入口及受限执行器；不得让 Coordinator 加载 Rust/JNI 或让 Native task 自行发布 manifest |
| SQL 读目录 | Iceberg/Paimon split 明确拒绝 Lance；没有独立 Lance connector | 建立持久 `table → dataset URI + ordered schema + visible version` 目录，并由 connector 生成固定版本的 v2 Lance split |

Native 读取已改为消费 canonical `DataSplit.source_snapshot` 中的非零十进制
Lance version，并在 Rust FFI 打开该版本；旧的只读 latest FFI 签名已移除，
ABI 升为 4。这个改动修复了 split 已固定 snapshot、读者却打开 latest 的漂移，
但仍需 dedicated connector 提供持久目录、精确版本和真实 SQL 验收。

这些是同一生产功能的前置条件。只放开 `NativeWriteExecutor` 的拒绝、在现有
`ConnectorWriteBindingFactory` 中把 `connector` 换成 `lance`，或直接从
`finishDistributedWrite` 在 Coordinator JVM 调用 FFI，都会绕开当前的 catalog、
租户和恢复 fence。未完成下列协议与目录工作前，Lance SQL 写入保持 fail-closed。

### 最小单路径实施顺序与协议验收

1. **先固定目录所有权。** 新 `pangrova-connector-lance` 作为唯一 Lance catalog
   provider，持久化表 identity、受允许的 warehouse URI、列顺序与类型、可见版本、
   catalog revision。`CREATE`、`APPEND`、`OVERWRITE` 的输出分配都产生确定的
   handle、write ID、generation、读版本和 schema fence；不复用 Iceberg/Paimon
   的数据文件或 PageSink。`CREATE` 的版本为 0；其余模式固定非零版本。
2. **扩展同一 v2 binding。** `LanceWriteBinding` 增加
   `CatalogExecutionBinding catalog` 与 `repeated string write_column_names`，
   字段号只能追加，现有 1–7 不重用；必须拒绝空列名、重复列名、空 catalog
   身份或与部署不一致的 fingerprint。若 staging prefix 要按 dispatch attempt
   派生，先保留持久的 allocation prefix，再在最终 `TaskAttemptId` 确定后生成
   attempt 专属子路径；journal 保存派生前的 allocation 与每个获胜 attempt 的
   精确路径。Rust sidecar namespace、Worker 和 C++ parser 用相同规则核验。
3. **扩展通用 finalization 控制消息。** 在 v2 中定义
   `FinalizeDistributedWriteRequest/Result` 与 Worker 内部已认证 RPC；请求包含
   catalog/tenant/principal、write ID/generation、base version、schema fence、
   operation、获胜 `TaskAttemptId` 与 fragment 摘要、journal fingerprint、
   owner/control epoch 和 deadline。结果三态为 `COMMITTED`、
   `NOT_COMMITTED`、`UNKNOWN`，成功时返回 published version 和 manifest digest。
   字节与 fragment 个数均有硬上限，绝不携带数据行。RPC 重试以相同 fingerprint
   查询已发布 manifest tag；超时或不确定结果不得被解释为未提交。
4. **将 finalization 接在唯一 journal 状态机上。** `TaskStatusReceiver` 在所有
   预期 work unit 的获胜 attempt 终态核验后、任何外部 manifest 写入前记录
   PENDING；其持久内容需包含 connector handle 快照、完整 fragment digest、
   owner fence 与相同 commit fingerprint。Worker 验证 fenced grant 后调用
   FFI 的条件提交；Coordinator 先持久化 published identity，再由 Lance connector
   对目录可见版本执行 CAS，最后把 journal 标记 COMMITTED。旧 owner、不同
   digest、丢失 fragment 或冲突版本必须明确失败；ACK 丢失保留 UNKNOWN 并恢复。
5. **最后接通固定快照的 SQL 读链。** `getTableHandle` 固定目录版本，
   `getSplits` 携带同一版本和 URI；Native reader 必须按 split 版本打开数据集，
   不能各自读取 latest。`SELECT` 与并发提交重叠时，旧查询仍读旧版本。

验收须从真实 SQL 公共入口完成 `CREATE/INSERT/SELECT`，至少两个独立 Worker
进程，真实 MinIO 条件创建语义，并覆盖并发提交单赢家、ACK 丢失、重启重放、
catalog CAS 冲突、跨租户和过期 owner 拒绝。任一链路无法运行时结果为 BLOCKED
或 FAIL；不能以 FFI/CTest PASS 代替 SQL E2E，也不能用 SKIPPED 计入通过。

## 边界与唯一契约

- SQL、planner 与 Coordinator 只解析、授权、分配 connector-owned 输出、调度和记录持久提交意图，不读取或合并数据行，不在 Coordinator JVM 加载 Lance JNI。Worker Native 执行数据读写；Lance Rust FFI 只处理 Arrow batch、事务 sidecar 与 manifest 原子发布。
- 网络与 JNI 均使用唯一的 `pangrova.protocol.v2` epoch。沿用 `TaskRequest.execution_options.write.output_binding` 的互斥 `oneof`：`connector` 选择既有 Worker PageSink；`lance` 选择 Native Lance sink。任一 WRITE 任务恰有一种 binding，Native 和 Worker 都按 `file_format`、`write_mode` 与 oneof 交叉校验；不允许失败时改走另一种 sink。
- `LanceWriteBinding` 必须携带与 connector binding 相同的精确 `CatalogExecutionBinding`，以及 connector 分配的有序目标列名。`dataset_uri`、`read_version`、schema SHA-256、`write_id`、`generation` 与单次 task-attempt 专属 staging prefix 在同一 binding 内；不得从 SQL 表名或 Worker 本地配置反推。保留一个 v2 `LanceWriteFragment`，Rust 的 protobuf 类型由该 schema 构建时生成，不维护手写字段镜像。
- 新增通用 `FinalizeDistributedWriteRequest/Result`（v2，不另起 Lance 协议）。请求承载精确 tenant、principal、catalog deployment、query/job ID、`TaskAttemptId` 集、write ID、generation、读版本、schema fence、操作模式、已获胜 fragment 引用及其聚合 digest、Coordinator owner/control epoch、提交 deadline；响应承载 commit identity、published version、manifest/transaction digest 与明确 `COMMITTED / NOT_COMMITTED / UNKNOWN`。不得承载数据行。对其它 Native sink，使用同一控制消息和执行器；PageSink 写仍由 connector metadata commit，不额外复制行数据。

## 所权与时序

1. Connector `beginInsert/beginOverwrite/createTable` 分配稳定 `write_id`、generation、目标 URI、读版本与 schema fence，输出 handle 由 Coordinator 注册到 `NativeWriteCommitTracker`。`ConnectorWriteBindingFactory` 通过 SPI 的 Native output allocation 创建唯一 v2 Lance binding；`QuerySchedulingEngine` 从该 allocation 取得 `LANCE`，不再硬编码 Parquet。每个实际 dispatch attempt 在绑定完整 `TaskAttemptId` 后派生独立 staging prefix，并被 Worker 按相同规则验证。
2. Worker 通过现有已认证的 catalog deployment 获取精确 revision 和租户可见性；对 Lance 只租赁 catalog 身份、验证 URI 位于允许的 warehouse 前缀、验证 schema/列/fence，不创建 PageSink。TaskRequest 过期、owner/control epoch 落后、目标 catalog revision 不符或 binding 模式不符即拒绝，不能降级。
3. Native 每个获胜 attempt 将数据文件和带 SHA-256 的事务 sidecar 写入 attempt 私有 staging 区；只回报有限长度的 v2 `LanceWriteFragment`，不自行发布 manifest。Coordinator 校验所有期望分区都有成功且无重复 winner、核对 attempt/租户/write ID/generation/版本/列与 fragment digest；失败任务的片段不能参与提交。
4. 在发起任何最终提交 RPC 前，Coordinator 用 `WriteCommitJournal` 原子持久化 `PENDING` 意图，包括完整获胜 fragment 摘要、output-handle 快照、base snapshot、owner fence、commit fingerprint；PENDING 不可见为成功。只有持有当前写入租约的 owner 可发起或重驱。
5. Coordinator 向一个有能力的 Worker 发通用 finalization RPC。Worker 校验内部 mTLS/服务身份和与持久意图相符的 fenced grant，读取 sidecar 后在对象存储使用真正的 `PutMode::Create` manifest 条件提交；S3 必须启用 `aws_conditional_put=etag`。`AlreadyExists` 为冲突，`NotImplemented`/超时/认证错误必须失败关闭，绝不能调用 Lance 默认 `UnsafeCommitHandler` 或普通覆盖写。重试必须使用同一 commit fingerprint，并以 manifest transaction tag 查询既有提交，不生成第二版本。
6. Worker 回复 `COMMITTED` 后，Coordinator 先把 published version/digest 持久写入 journal，再由 connector 校验目标 manifest identity 并执行唯一的 catalog CAS/可见性确认，最后将 journal 标记 `COMMITTED`，对 SQL 客户端报告成功。若 catalog CAS 冲突，返回明确失败并隔离已发布但未被 catalog 引用的 manifest；后台清理仅处理经证明不可见的对象。
7. RPC ACK 丢失、Worker 或 Coordinator 崩溃时，从同一 PENDING 意图重驱。先根据 journal 的精确 version hint 查询；无 hint 时按 manifest transaction tag 从 `read_version+1` 到最新版本分批、可持久游标扫描，不以固定 64 版本窗口永久判失败。找到相同 digest 即恢复原提交；同 write ID 不同 digest 为不可重试冲突。网络不明结果保持 `UNKNOWN`，不得重新分配 write ID，也不得对客户端报告成功。

## 远端存储与资源边界

- 支持的写 store 必须证明目标 manifest 的条件创建语义。目前 local 文件系统使用 `rename_if_not_exists`；S3/兼容存储走 `If-None-Match: *` 条件 PUT，并在每次 manifest 提交前以唯一 probe 对象验证“首次 create 成功、第二次 create 拒绝、原内容不变”。未知 URI scheme、忽略条件头的旧版兼容存储、缺乏原子条件写的对象存储或返回 `NotImplemented` 均拒绝。probe 仅检查顺序条件语义，并不能替代并发单赢家验收；读取不能暗中启用默认不安全提交路径。
- 单 Worker 仅保留有界 Arrow batch channel；事务 sidecar、控制面 fragment、聚合元数据和 manifest 分别有硬上限。超限返回明确资源错误；不同 attempt 不能共用 staging prefix。提交端点限制并发数与 deadline，不能由一个表的海量 fragments 无界占用内存。
- 本地 Native 测试不代表远端验收：用真实 MinIO TestContainer、至少两个独立 Worker 进程、并发 append/overwrite、条件提交冲突、断联/ACK 丢失、重启恢复、重放幂等、跨租户与过期 owner 拒绝，最终经 SQL `CREATE/INSERT/SELECT` 读回。所有测试为 PASS/FAIL/BLOCKED；不能用 `SKIPPED` 代替生产路径。
- 已在 Rust FFI 层观察到 `minio/minio:RELEASE.2024-01-16T16-07-38Z` 对重复条件创建返回成功，故生产实现必须 fail-closed；测试使用 `tobi312/minio:RELEASE.2025-02-28T09-55-16Z` 容器中的 MinIO 原版服务端二进制，对两个独立 S3 客户端的并发同版本 manifest 提交取得单赢家/单冲突。该社区镜像仅用于测试，生产部署需单独审查镜像供应链；此验证仍不代表双 Worker、Coordinator journal、catalog CAS 或 SQL 闭环。
- 当前唯一 probe 与 staging manifest 在错误路径采用尽力删除；对象存储故障或缺失删除权限会留下孤儿对象。部署前需定义按 commit-id/租约与可见性证明的后台回收、保留期和告警，不能把一次测试成功视为资源清理闭环。

## SQL 读取接线与可复证缺口

- Native 已能解析 canonical v2 `SplitInfo.file_format=LANCE` 并调用 Lance FFI 读取，但 SQL 表名到 Lance dataset 的生产目录尚不存在。Dedicated Lance connector 必须从持久目录读取并固定 `table identity → dataset_uri + ordered schema + visible_version`；`getTableHandle` 固定版本，`getTableMetadata/getColumnHandles` 返回同一 schema，`getSplits` 只生成该版本的 canonical v2 `SplitInfo`。同一查询的多个分片不得各自读取“latest”，否则跨分片快照会漂移。
- Coordinator JVM 不加载 Lance JNI。现有 `lance_ffi_dataset_info/version/reader_schema` 只在 Native 层，不能据此让 Coordinator 从 SQL 表名发现 schema/版本；应由持久 catalog 承载这些元数据，或通过现有可信内部控制通道增加通用 metadata-only Worker 查询后再由 catalog 固定版本。静态 catalog properties 或 Worker 本地路径推断不得作为生产读链路的替代。
- 验收需要真实 SQL `SELECT` 经 connector Factory/Metadata/SplitManager → `SplitInfo(FILE_FORMAT_LANCE, pinned_version)` → Worker Native → Lance FFI → SQL 结果；同一测试中重叠提交并确认旧查询仍读旧版本、新查询读新版本。此前 Iceberg/Paimon Lance E2E 采用捕获异常即通过的假测试，现已删除；当前 Rust 及 CTest 通过仅证明 FFI/Native 局部能力，不证明 SQL 读闭环。

## 拟改动边界

`pangrova-protocol/src/main/proto/pangrova/protocol/v2/{task,result}.proto`（仅扩展 canonical v2）；`pangrova-spi` connector Native allocation 接口；新 `pangrova-connector-lance`（Factory、Metadata、Split、OutputHandle、Codec、finalization 客户端）；`QuerySchedulingEngine`、`ConnectorWriteBindingFactory` 与 task-attempt binding；`WorkerCatalogRuntime`、Worker 内部认证 finalization endpoint；`pangrova-worker-native/lance-ffi` 条件 CAS 与 JNI metadata command；`pangrova-testing/pangrova-test-e2e` 真 MinIO/多进程 SQL 验收。完整 Java/Native 构建与仓库 Step 0–8 仍是发布门槛。
