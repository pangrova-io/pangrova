# Native Result 读取失败、取消与关停所有权闭包

状态：SOURCE_READY，未构建、未验收。此规格与代码待 Root 在同源 Native/包/JNI cohort 中复证；此前 921cca cohort 的 PASS 不覆盖本变更。

## 实测根因（2026-10-07 r8）

日志 `/tmp/pangrova-production-20261007-ai-lance-smoke-real-r8.log` 的 Maven 汇总为 18 tests、6 FAIL、6 ERROR、0 SKIPPED，随后 fork SIGABRT。第一个 AI callback 失败为真实 `AI execution traceId is not available`，由独立 Trace 契约修复处理，本修复不改变其失败分类或伪造成功。

链路为：AI callback 失败 → SDK Task FAILED → Result route 删除 → 已进入 DoGet 的消费者仍等待 → Native release 发现真实 RPC reader owner → Java 保留 handle/budget → cleanup retry 尚不能完成 → 下一 class 的 drain 验收失败 → shutdown hook 等待 Flight server → Surefire 在 30 秒后强制 Halt。

macOS 原始现场 `java-2026-10-07-164428.ips`：pid 38162，faultingThread 49，SIGABRT，实际栈为 `ResultBatchReader::~ResultBatchReader +388 → RecordBatchStream 析构 → ServerTransport::DoGet → gRPC worker`。同时线程 89 停在 `GrpcServerTransport::Shutdown → FlightResultServer::stop → FlightServerRegistry::shutdownAll → nativeShutdownInternal`。加载 Native UUID `80BD6C8C-31BE-3B74-87E9-0A8836A97D5A` 与现场 `build/libpangrova_native.dylib` 完全一致。反汇编中的 +388 是 noexcept 异常 landing pad；该现场不能进一步确定是哪一个 mutex 地址失效，不能据此捏造 OOM 或特定 global mutex 原因。

### 已复证的生命周期缺陷

- SDK `velox/exec/Task.cpp` 的 `Task::terminate` 在完成通知后调用 `maybeRemoveFromOutputBufferManager`；后者调用 `OutputBufferManager::removeTask`。
- SDK `OutputBufferManager.cpp::removeTask` 先移除 route，再调用 `OutputBuffer::terminate`；`OutputBuffer.cpp::terminate` 只完成 producer backpressure promises，不触发消费者 DestinationBuffer 的 getData notify。
- SDK 真正 `OutputBuffer::deleteResults` 会触发消费者 notify；但 Pangrova runner 在 taskCompletionFuture 之后才调用它，此时 route 可能已不存在。不能把这个事实误写成 SDK deleteResults 本身不通知。
- 原 reader 自己仍持 getData Promise，故回调销毁也不会让消费者立刻获得 broken-promise。原读取最多等待 300 秒或有限 authority expiry；客户端 60 秒 idle timeout 不是 Native producer EOF。
- 原 DoGet 丢弃 ServerCallContext；客户端 Cancel 不进入阻塞读取。
- 原 stop 在清理/撤销 registrations 之前调用 Shutdown/join，因此先等正在阻塞的消费者退出，后执行能撤销消费者的清理。
- 原 reader 析构重新查询 SDK function-static manager，未保存实际 manager owner；原服务 registration 操作加载裸 instance 指针后再访问 mutex，未 pin 正在退出的服务。
- Java TaskCompletionMonitor 已先保存真实 FAILED report/stats，再执行 FINISHED-only 成功检查；失败检查成为原异常的 suppressed 信息。onTaskError 在真实资源释放后才发布 FAILED。release 失败后 owner seal 必须保留；普通 cancel admission 再遇 sealed 不等于可以 unseal 或删掉真实 handle。

## 最小实现闭包

1. 既有 `ResultOwnershipState` 保存唯一 actual pending-read 的 weak subscription；它仍是 producer、reader、batch 计数的单一 authority，不新增 task registry。
2. `ResultPendingRead` 是一次 SDK getData 的实际 waiter。数据 callback、撤销和 RPC cancel 原子争夺同一个 completion；迟到 callback 仅释放自己携带的实际 pages，不重复完成或替换首 callback。
3. authority revoke 立即通知 exact waiter，与真实 queue clear 分开。只有 SDK deleteResults 实际执行成功后才标记 queue cleared；reader/batch counters 仍由原 RAII 实际析构递减。
4. reader 与 runner 捕获真实 OutputBufferManager shared owner。reader 不在析构时查任何 SDK global manager 或服务裸 instance。退出异常被记录且不伪报 queue drain，保留实际 task retry。
5. ReadNext 从 admitted immutable authority、原 ownership revoke fact、实际 ServerCallContext 校验；完全组级 schema/null/sequence 校验完成后才发布 batches 或 ACK。不以取消、失败、producer disappeared 合成 EOF。
6. Arrow 18 仅暴露 `ServerCallContext::is_cancelled()`，没有 cancel subscription。数据与 Native revoke 通过 CV 即时唤醒；只有实际 RPC cancel 采用 20ms bounded poll。此数字不是端到端性能 SLO。
7. stop 先关闭服务 admission，再撤销所有实际 registrations/wake consumers、执行真实 queue cleanup，最后 Shutdown/join。失败 registration 保留在原 cache 供 exact owner 重试；已停止 transport 不重复 join。
8. 既有 registry 的 result service 增加 STARTING/READY/CLOSING/STOPPED 元数据，保存实际 shared service owner。网络 bind/Shutdown/join 不持 registry mutex；失败 owner 不擦除。getter 返回实际 owner snapshot，不是另一个缓存。
9. 服务 static registration 操作由一个短 shared lifecycle lock pin 实例，仅覆盖 metadata 操作；stop 独占清除 publication 后释放该锁，再进行 SDK callbacks/网络关停。

### 读取重放与锁序

destination 0 自来只有一个 canonical sequence/ACK owner：`TaskOutputInfo::claimed` 在 output mutex 内拒绝第二次 claim。第二个同 signed ticket 的实际 DoGet 必须返回 Arrow error，不能覆盖 SDK notify。直接 reader factory 维持同一约束；重复 pending attachment 返回 ArrowStatus，不将逻辑异常抛过 ReadNext/gRPC。

锁序：registry result metadata → 仅保存/复制 service snapshot（随即 unlock）；service lifecycle → 短 instance publication → output cache metadata；output admission 可读取 ownership evidence。ownership lock 仅提取 waiter，unlock 后触发 waiter cancel；waiter 从不反向调用 ownership/cache/registry。不得持 instance/cache/registry metadata lock 调 SDK deleteResults、I/O 或线程 join。服务自己的 lifecycle lock 只序列化一个实际 start/stop owner。

Java终态协议、NativeOwnerReleased receipt、execution drain evidence、内存/租户预算及 held→released ACK 顺序本次均不改变。真实 retained batch/reader 存在时释放继续失败并保留 exact owner；不得靠早发 receipt、加预算、缩短 authority、假 EOF 或取消测试求通过。

## 验收选择器

已有 nearest：`ResultBatchStreamReaderTest.*:ResultOwnershipStateTest.*:FlightResultServerStartTest.*:FlightResultServerSecurityTest.*:ResultReadAuthorityTest.*:FlightServerRegistryConfigTest.*`。

新增 actual Native regressions（尚未执行）：

- `ResultBatchStreamReaderTest.ActualSdkFailureAfterRouteRemovalWakesPendingReaderWithoutEof`：真实 paused SDK Task、真实 PartitionedOutput 初始化、已安装的 actual getData、Task::setError 删除实际 route 后，才启动真实 runner completion；非 EOF error，真实 reader 退出前 drain 不成立。
- `ResultBatchStreamReaderTest.ActualFlightCancellationWakesSdkFetchAndRetiresItsReaderLease`：真实签名 authority、真实 Flight client Cancel、实际 server context、真实 reader count 退出；producer 未停止前不伪报 drain。
- `ResultBatchStreamReaderTest.ActualFlightShutdownRevokesPendingFetchBeforeJoiningTransport`：真实 RPC pending 时 stop，验证网络 join、错误非 EOF、registration 与 reader 实际退出。
- `ResultBatchStreamReaderTest.ConcurrentSignedTicketReplayCannotReplaceTheActualPendingReader`：相同真实 signed ticket 二次 RPC 被拒，首读取仍 pending/count 1，可真实 Cancel 并排空。
- `ResultBatchStreamReaderTest.ReaderCanExitAfterItsActualServiceOwnerWithoutGlobalLookup`：持有真实 reader 时销毁已关停服务，验证捕获的 SDK manager owner 不变，reader 实际析构前不宣称 drain，退出后才完成。
- `ResultBatchStreamReaderTest.ActualSdkDataCallbackRacingRevocationCannotCompleteTwice`：真实 SDK enqueue callback 与真实 authority revoke 同时开始；无 double completion，任何已发布真值保持正确，buffer/reader 实际 owner 离开后才 drain。
- `FlightResultServerStartTest.ConcurrentCommandsUseOneActualResultServiceAndOneTransportJoin`：并发 canonical START 同一端口/owner，并发 STOP 一个真实 transport join，snapshot 不能复活已关闭服务。

Java最近链路保持原 oracle：`TaskCancellationResourceCleanupTest`、`TaskResourceDrainProbeTest`、`TaskTerminalEvidenceContractTest`、`NativeTaskExecutorReleaseRaceTest`、`WorkerResultPortIntegrationTest`。须重建并 fresh package/Worker refresh 后运行，不能使用旧 verified JNI 宣称新 closure PASS。

最终重跑真实 `OllamaAiSqlE2ETest`、Lance SQL E2E、`ActivationSmokeE2ETest` 的同源全 cohort，核验跨 class cleanup 与 fork 正常 exit；组件测试不计为分布式 SQL E2E。Release full CTest 与 ASan 仍由 Root 执行。未执行项目标记 NOT RUN，任何失败按实际日志分类，不作环境或组件通过替代。
