# 独立推理服务：当前调用链与完整替换设计

核查日期：2026-10-07。本文件只记录源码审计和待实现设计；本轮不修改推理源码，不启动构建，
不声称独立服务、RPC 接入或下列验收已经完成。与 `kernel-current-audit.md` 的边界一致：
数据库支持 SQL 模型调用，外部模型执行交给独立服务；不增加 Agent 或 UsageLedger。
资源准入、诊断计数和审计证据仍需保留。

## 1. 当前真实链路

```text
SQL / Calcite expression
  -> planner AiInvocationBinder（模型 literal、invocation id、plan hint）
  -> Coordinator ExactModelResolver / OperatorBinding / TaskRequest
  -> Worker NativeTaskExecutor（注册可信 task / operator 资源权威）
  -> Native SubstraitFunctionConversion（验证绑定，追加内部参数）
  -> JvmScalarFunction::apply（选择行，Object[][]，同步 JNI）
  -> NativeFunctionBridge.evaluateBatch（解码 canonical callback authority）
  -> GovernedAiScalarFunction（内部参数与已准入绑定逐项一致）
  -> AiInvocationService（精确模型/能力/版本/输出、授权、准入）
  -> 配置的 provider -> HTTP
  -> 结果数量 / embedding 类型与维度检查 -> JNI -> Velox -> Flight -> client
```

`NativeFunctionBootstrap` 通过 ServiceLoader 获取 `AiFunctionFactory`，构造并保留 factory
所有权，再向 Native 注册 JVM 回调函数。初始化失败关闭资源，Worker 不得进入接受任务状态。
因此当前 AI 执行和 provider 生命周期属于 Worker，不是独立推理进程。

`ConfiguredAiProviderRegistry` 的精确部署是 `openai-compatible`、`openai-responses`、
`openai-transcription`；embedding/reranking 限制为兼容 provider。凭据由唯一 SecretResolver
解析；versioned wrapper 将逻辑模型名、版本、签名与实际 provider 关联。没有自动选择其他模型的路径。

`GovernedAiBindingArguments` 从已注册 task 取真实 `OperatorBinding`，内部 VARCHAR 字段只是其投影，
不是授权来源。Native `appendGovernedAiArguments()` 检查 model、输出类型、完整覆盖、禁止 fallback、
provider idempotency policy、并发/条目/请求/响应预算；AI_EMBED 还验证 FLOAT32 VECTOR 的维度。
迁移不能改成相信客户端提交的模型名、tenant 或预算字符串。

`AiInvocationService` 再验证模型名/版本/signature、能力版本、execution mode、modality、批大小、
deadline、输出数量和维度。内置 `ConfigurableAiInvocationAuthorizer` 默认 deny；authenticated
模式要求显式 model/capability/permission allowlists。它不是独立服务网络鉴权实现，也不应把
`authorization.granted-permissions` 的全局配置解释为任意请求都具有用户角色权限。

`GovernedBlobStore` 有 tenant capability、不可变 checksum/size、受治理对象读取和有界 staging；
`GovernedBlobContent` 在加载后核验实际内容长度与 SHA256。HTTPS import 另有地址/host/MIME/
wire/decoded size 策略。多模态迁移需保留所有这些步骤，不能将任意 URL 直接交给 provider 下载。

## 2. 当前批处理与生命周期的实际限制

`OpenAiCompatibleInferenceProvider.completeBatch()` 对每个 prompt 启动一个真实 HTTP 请求，
传播每项 idempotency ordinal/deadline/admission，最终 `allOf().join()`；这属于 client fan-out。
它的 close 只 shutdown rate replenisher / batch executor，没有在该方法内证明所有任务已终止。
2026-10-09 核对：`OpenAiHttpRequester` 及 embedding、legacy inference、rerank 的
OpenAI-compatible POST 路径已改为每次调用只发送一次。服务端是否已处理但响应丢失时，
Worker 报告 `PROVIDER_OUTCOME_UNCERTAIN`，不自动重放；能力注册表同时声明
`RetryPolicy.noRetry()`。重试只有在模型服务提供可验证、覆盖恢复窗口的去重与结果查询契约后
才能重新设计，详见 `docs/architecture/ai-provider-post-outcomes.md`。

`AiHttpTransport.send()` 为一个 application entity send 开 physical attempt，并在有界 body subscriber
读完或失败后释放；计数明确不包括 TLS/TCP headers 与重传。`AiPhysicalInvocationAdmission` 只有
logical close 且 liveAttempts=0 才 finalize。`AiTaskBudget.revoke()` 中断 permit waiters 并拒绝新 send，
不等于向每个已发送 HTTP 请求或远端模型 backend 发出取消。`releaseTask()` 只在 idle 后移除本地预算。
这是本地资源机制，尚不证明服务端 GPU/请求 buffer 的退出。

## 3. 固定 SDK 与官方新接口必须分开验收

固定 Velox revision：`3da0390118fa4b0addbfbdeb03f438ec1b1237a6`。本地核查路径以该 SDK 的
`velox/` 为根；文档最新接口不作为固定 binary 的能力证据。

| 固定源码 | 具体事实 | 替换前必须完成 |
| --- | --- | --- |
| `common/rpc/RPCTypes.h` | payload/result 是 string，error 是 optional string；无 move-only typed payload | 选择完整升级后的固定 revision 或受管理 SDK patch；不要新旧两种协议同时长期运行 |
| `common/rpc/IRPCClient.h:60` | 默认 callBatch 循环 call，失败变 error response | 覆盖为真实一次 Flight batch；后端只有逐行 API 时如实计 n 个物理 send |
| `expression/rpc/AsyncRPCFunction.h:165` | 默认 output 是 VARCHAR，所有 error 变 NULL | typed output 的强校验；只将契约明确的 NULL input 转 SQL NULL，普通失败传播 |
| `exec/rpc/RPCOperator.cpp:127-145` | 创建全选 SelectivityVector，预加载并 flatten 全部输入列 | 显式 guarded selection/passthrough；测量实际 copy 和 lazy materialization |
| 同文件 per-row / batch dispatch | 先 dispatchPerRow/flushBatch，再 acquire limiter token | 请求发送前取得准入；容量计量按真实 send，不把 n-way fan-out 算作 1 个 send |
| 同文件 batch completion:223-237 | 只核验数量，再按位置覆盖 response.rowId | 在 transport/function 层先核验服务返回身份、唯一性和完整性，不能用重打序号掩盖错行 |
| 同文件 close:452-468 | reset state/function/retained input；异步 callback 仍可能持 state | 显式 revoke/cancel/transport drain/owner receipt；关闭算子不代表 remote owner 退出 |
| `exec/rpc/RPCPlanNodeTranslator.cpp:39-55` | BATCH 和纯常量 synthetic row 强制单 Driver | 保留或用完整真实 race oracle 修正；不能删除限制后宣称并行提高 |

最新版官方说明已经包含 typed payload、执行前 backend admission、批量单位、流控、框架错误拒绝。
这是升级方向，不能据此认定固定 SDK 已有这些接口。首选完整核对并固定新 RPC cohort，补齐本项目
选择行与 owner drain 契约后继续使用 Velox RPCNode/RPCOperator，不新建数据库执行器或集群调度器。
[Velox 官方异步 RPC 接口](https://facebookincubator.github.io/velox/develop/async-rpc-functions.html)

## 4. SQL/Substrait lowering：选行必须成为正式契约

当前 `plan_extension.proto` 已声明 `AiInvokeDetail`，含 invocation/output schema/batch 限额，
`SubstraitExtensionShape` 和 `CanonicalRelationSchema` 能读取其输出形状；未找到 Planner 生产者
或 Native 执行转换分支，`SubstraitRelConversion` 对该 detail 仍走 unsupported。
应补齐这一现有 typed detail，不能再创建平行 AI envelope 或把 schema reader 当成执行实现。

最终 IR 使用 Substrait `ExtensionSingleRel` 的 typed `Any detail` 表示一个明确的 governed RPC
关系，携带 invocation id、精确 binding 引用、输入/输出 schema、argument ordinal、选行/分支依赖、
passthrough ordinal 和输出 nullable 契约。Substrait 官方要求生产者/消费者约定扩展的输出 schema，
因此类型规则、未知字段拒绝与 semantic/build signature 必须跟 Java/Native cohort 同时发布。
不在 TaskRequest 放另一份任意表达式计划，也不运行 Java 二次 SQL 编译。
[Substrait 官方 ExtensionSingleRel 定义](https://github.com/substrait-io/substrait/blob/main/proto/substrait/algebra.proto)

固定 RPC 框架没有通用“任意 scalar -> RPCNode”的转换。以下是实施前必须证明的 lowering 规则：

- 先完成 Calcite 优化和同质函数绑定，再对最终表达式图作 guarded lowering；每个非确定性调用保留
  自己的 invocation，不做公共子表达式共享、近似缓存或重复执行消除。
- CASE/IF 按首个 TRUE 条件选择 then；FALSE 与 NULL 均进入剩余行。仅被选分支计算参数和发送请求，
  未选分支既不加载媒体，也不授权 provider send；前面分支含 RPC 时，后续 guard 依赖其真实结果。
- AND/OR 使用三值逻辑：AND 仅 FALSE 终止，NULL 行仍可能需要后续 FALSE；OR 仅 TRUE 终止，NULL
  行仍可能需要后续 TRUE。异常是否被其他 conjunct 掩蔽须保存实际表达式错误规则，不能全部提前抛出。
- NULL primary input 依实际函数 null contract 本地产生结果；不是把所有 nullable argument 都过滤掉。
  模型 literal、purpose、schema/options 的验证阶段和纯无输入分支也要与编译/运行语义一致。
- 调用结束按 invocation + batch + selected ordinal scatter；未选行按原表达式合并，不能丢行或通过
  JOIN/UNION 复制源扫描。不同调用/不同任务的相同 ordinal 没有共享身份。

Velox 本地 `expression/SwitchExpr.cpp:69-146` 按剩余/then SelectivityVector 计算分支；
`ConjunctExpr.cpp:93-176` 在 active rows 上计算并保留错误规则。官方还说明 AND/OR 可以自适应重排，
因此不能把 SQL 文本的左到右顺序当作现有保证。涉及非确定性 RPC 的重排政策需要单独固定并验证。
[Velox 官方表达式求值](https://facebookincubator.github.io/velox/develop/expression-evaluation.html)

单个 `active=true` 列不足以实现任意嵌套语义。完整方案需在同一 Velox RPC operator 路径中支持
typed guarded expression continuation：本地表达式求值达到 RPC 依赖时暂停该选择行，异步返回后续算，
仍复用原 special-form 的选行、错误合并和 passthrough 规则。这是要实现并验证的 SDK 扩展，不是当前
已存在的接口。不采用将所有调用无条件提升到 Project 前的“简化方案”；复杂语义未完成时不发布替换 cohort。

## 5. 服务及传输契约

建议单独 `pangrova-inference-service` 进程承载 provider 和媒体实际 I/O，独立部署、扩缩、健康与停止。
它依赖 shared SPI/protocol/security/config，不依赖 Worker/Coordinator 或 NativeRuntime。
模型 deployment authority/endpoint 发现属于控制面；provider 凭据只在服务端解析。
目前 `ExactModelResolver` 从 Worker capability inventory 中解析模型。迁移时 Worker 声明的是
可执行 RPC function cohort，模型可用性应来自有签章的服务 deployment manifest，再绑定路由和
lease；不能把 Worker 原来本地构造 provider 的 model inventory 直接保留为假可用性。

数据库到服务采用 Flight `DoExchange`，一个调用上传多行 typed RecordBatch 并回传 typed batch。
`app_metadata` 携带有版本的 protobuf envelope；Flight 本身不定义推理字段或执行权威。
输入保留 UTF8/binary/list/struct/FLOAT32 vector，FILE 使用受治理不可变 reference；不把向量、媒体
转为 JSON 字符串。服务到厂商 API 的编码仍由对应 provider 实现。跨进程 IPC/网络有复制，不承诺零复制。
[Arrow Flight 官方 DoExchange、metadata 与逐调用认证](https://arrow.apache.org/docs/format/Flight.html)

Envelope 至少覆盖并验证：协议/cohort、tenant/principal、完整 task attempt 和 execution owner
incarnation、invocation、model/capability version/signature、input/output schema digest、selected
row ids、batch sequence、request/input digest、idempotency identity、absolute deadline、request/response/
media/并发资源租约，以及授权 token 的 scope/audience/expiry。复用 canonical authority 类型，新增的是
独立服务的认证签章和 audience，不能把 JVM 进程内 trusted callback bytes 原样当成网络凭据。
SQL function 的 semantic identity/签名也需绑定：AI_FILTER/CLASSIFY/EXTRACT 等公共函数虽调用
同类 text-generation capability，prompt construction、schema parsing 和 boolean/result 规则不同，
不能仅凭一个 TEXT_GENERATION operation 让服务自行猜测。这些规则随函数 cohort 固定。
TLS 与逐调用 token 校验同时覆盖 reconnect/L7 routing；trace 字段与 token 都不能作为用户权限来源。

服务解码前检查 envelope/上限；decode 后再次检查实际 schema、长度、媒体身份、行唯一性和预算。
响应必须对应同一 attempt/binding/batch/digest，包含每个 selected row 的唯一成功或明确失败。
缺失、重复、越界、外来行、维度漂移、非有限向量、schema/nullable 漂移均 fail closed。
不能因“返回条数一样”而接受 shuffled/wrong-row result。普通 SQL 错误失败整次调用；若将来需要显式
try/error-result 函数，需另定 SQL 契约，不默认吞成 NULL。

Serving 动态组批按模型版本/参数、shape/模态、租约权限、deadline 和资源容量兼容性进行。
数据库 batch 是一次传输，服务 batch 可以合并多个传输或拆分大 batch；两者身份映射不可混淆。
token/media 长度须有 padding/ragged/分桶政策和实际字节计量；组批队列有界，并在排队、加载、执行前
检查 deadline。Triton 官方建议通过实际 latency/throughput 调整 batch 和 queue delay，并通常不要
预设 preferred sizes。该机制不证明当前已接入 Triton，也不要求厂商 HTTP API 原生支持批处理。
[Triton 官方 batching 与调参流程](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html)

## 6. 取消与资源所有权

数据库首先撤销本 attempt 的结果发布权威；后续迟到结果必须丢弃。之后发送服务 cancel，取消 Flight
stream，并等待 transport 回调和本地 buffer owner 退出。服务另持 serving lease，取消新 admission 和
排队请求，对已执行 backend 请求发真实 cancel，并仅在 backend/实体/buffer 所有者退出后关闭 lease。
同一物理资源只有一个释放者，两个层次的预算不能通过相互发送“已取消”重复释放。

Flight cancel 可能同时关闭获取收据的流，因此 serving drain evidence 需要独立、认证、可重放的
control/status 接口，固定 service owner incarnation + request/attempt/lease。终态证据、幂等 cancel、
reconnect 和失联 UNKNOWN 要纳入生命周期。未知 owner 状态不得被当作 drained；Worker 只可以证明
自己的 transport/local owners 已退出，不能代服务释放 GPU/权重预算。不可取消的厂商 API 请求仍由
服务持有至真实响应或最终失败，数据库拒绝其迟到发布。服务失联处理需要真实租约与资源回收权威，
不能用固定 TTL 字段自动宣告资源已释放。

Triton 的取消检测有边界，进入 backend 后是否早停取决于 backend 实现；Python backend 需模型代码
配合。它也明确 core 在部分 rate-limiter 阶段尚不检测取消。迁移验收必须观察 backend owner 退出，
不能把 client CANCELLED 当作 serving drained。
[Triton 官方取消语义](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/request_cancellation.html)

## 7. 原子替换的具体 inventory

下表是拟实施清单，尚未移动任何源码。新服务中的包名按新 module 改为 `org.pangrova.inference.*`，
杜绝为了复用而引入 Worker module 依赖。

| 现有模块/类 | 目标与处理 |
| --- | --- |
| Worker `ai.provider`: `ConfiguredAiProviderRegistry`, `VersionedAiProviders`, `AiModelConfigParser`, `AiCapabilityConfigParser`, `AiModelProperties`, `AiProviderDialectValidator`, `EnvironmentSecretResolver` | 移入 inference service，provider 配置/实例/secret/lifecycle 全部由服务持有 |
| Worker `ai.inference`: `OpenAiCompatibleInferenceProvider`, `OpenAiResponsesInferenceProvider`, `OpenAiTranscriptionInferenceProvider`, `OpenAiInferenceConfig`, `OpenAiChatRequestEncoder`, `OpenAiResponsesRequestEncoder`, `OpenAiTranscriptionRequestEncoder`, `OpenAiHttpRequester`, `GovernedBlobContent` | 整组迁移；改成服务 request context，增加真实取消/owner drain，保留实际 provider dialect |
| Worker `ai.embedding`: `EmbeddingProviderLookup`, `EmbeddingResultValidator`; `ai.embedding.provider`: `OpenAiEmbeddingConfig`, `OpenAiCompatibleEmbeddingProvider`; `ai.rerank`: `OpenAiCompatibleRerankingProvider` | Provider/validator 迁移服务，Native 仍做输出契约独立校验；lookup 改为服务 registry，不注册 JNI 模型 callback |
| Worker `ai.provider`: `GovernedInferenceProvider`, `GovernedEmbeddingProvider`, `GovernedRerankingProvider`, `AiInvocationRequests` | provider guard/request builder 改成 service dispatch context；删除依赖 Java SQL ScalarFunction 的 wrapper |
| Worker `ai.context`: `AiExecutionContext`, `CanonicalAiExecutionContexts`, `BoundAiInvocation`, `GovernedAiBindings`, `AiProviderCallContext`, `AiHttpTransport`, `AiBoundedBodySubscriber`, `AiSingleSendBodyPublisher` | 网络 authority 映射移到 shared protocol/service，HTTP context/实体 owner 移服务；ThreadLocal callback 权威不跨网络 |
| Worker `ai.invocation`: `AiInvocationService`, `AiEmbeddingInvocationSupport`, `AiInvocationResultSupport`, `AiInvocationFailures`, `AiInvocationTerminalReporter` | 移服务并以 explicit async request owner 替代 Worker static budget singleton；实际执行路径只有这里的 provider dispatch |
| Worker `ai.invocation`: `AiInvocationBudgetManager`, `AiTaskBudget`, `AiModelBudgetRegistry`, `AiInvocationBudgetValidator`, `AiPhysicalInvocationAdmission`, `AiInvocationReservation`, `AiInvocationPermits`, `AiResourceAmounts`, `AiResourceLedger` | 拆成 Native/Worker transport admission 与 service serving admission；资源计数继续保留，不引入计费 UsageLedger；协议统一，实体 owner 各自唯一 |
| Worker `ai.governance`: `AiInvocationAuthorizer`, `ConfigurableAiInvocationAuthorizer`, `AiInvocationObserver`, `CompositeAiInvocationObserver`, `LoggingAiInvocationObserver`, `MimeTypeValidator` | 必要 interfaces 移 shared SPI，服务实现鉴权/审计，Worker admission 保存实际任务权限与 lease；拒绝多 provider policy 实例隐式选择 |
| Worker `ai.blob`: `BlobStorage`, `GovernedBlobStore`, `S3BlobStorage`, `S3BlobStorageSettingsParser`, `FileSystemBlobStorage`, `TenantCapabilitySigner`, `HttpsBlobImporter`, `PinnedHttpsTransport`, `PublicNetworkAddressPolicy` | 受治理 FILE store/import 迁移服务，shared BlobReference 保留；SQL FILE functions 走同一受治理服务而非留 Worker 网络后门 |
| Worker `ai.function`: `AiCompletionFunction`, `AiEmbedFunction`, `AiSummarizeFunction`, `AiClassifyFunction`, `AiFilterFunction`, `AiExtractFunction`, `AiGuardFunction`, `AiPredictFunction`, `AiTranslateFunction`, `AiDescribeImageFunction`, `AiParseDocumentFunction`, `AiOcrFunction`, `AiTranscribeFunction`, `AiRerankScoreFunction` | 保留公共 SQL 名称/签名/NULL/prompt/schema 转换语义，在 Native 绑定或服务 operation 编码处重现；删除 Worker Java ScalarFunction 注册与运行实现，禁止 JNI 备用模型路径 |
| Worker `ai.function`: `GovernedAiScalarFunction`, `GovernedAiBindingArguments`, `AiBatchBuffer`, `AiFunctionFactory`, `AiFunctionCatalog`, `AiFunctionActivation`; `function.file`: `FileStructHelper`, `FileValidationContext`, `FileFromBytesFunction`, `FileFromUrlFunction`; `function.table`: `AiTableFunctionFactory`, `DirectoryListingTableFunction` | 拆出公共声明/本地纯类型处理，provider/FILE 网络函数切至服务；删除 AI hidden VARCHAR 注册。目录读取若仍属数据源 Native 路径，需按实际 caller 拆分，不移进推理调度器 |
| Worker `NativeFunctionBootstrap`, `NativeFunctionBridge`, `NativeTaskExecutor`, `NativeCreationOwners`, `WorkerComponentRefs` 及 ServiceLoader resources | 移除 AI factory/provider owner 和 AI JNI registration；一般非 AI JVM UDF 不在本次删除范围。任务生命周期改用唯一 inference channel owner，资源报告保留 |
| Planner `AiInvocationBinder`, `ExpressionConverter`, `PlanNodeConverter`, `FunctionArgumentNormalizer`; Coordinator `ExactModelResolver`、embedding model registry/准入 | 保留 SQL 编译与模型授权，新增最终 guarded RPC relation lowering；模型部署位置改为服务，不能把 provider secret 搬进 Coordinator |
| Native `SubstraitFunctionConversion`, `GovernedBindingArguments`, `RegisteredFunctionBinding`, capability catalog/registry、计划 converter/bootstrap | AI 改为唯一 RPC binding/translator；删除 AI hidden-JNI args append，保留其他 governed operators 的布局；capability signature/cohort 与新 SDK 一起固定 |
| `pangrova-protocol` canonical authority/type/function declarations、生成工具、runtime/task report，SPI AI/provider | 正式扩展 generated proto，改 producer+consumer+validation+identity 一次接入；不得手改 generated sources，旧 AI 注册协议在切换点不再执行 |

`ai.worker.text.Bm25NativeScorer` 是本地 Native 文本评分，不是模型 provider，不迁入 serving；M12 起随 AI native adapter 落在 ai-worker，只依赖 native-bridge。
现有声明/签名/版本文件的 hash 不能沿用旧实现而只改 endpoint；新部署须重新生成完整 cohort 并在调度前
验证所有 Worker 一致。启动配置、provider ServiceLoader、测试配置与打包入口均跟 module 迁移同步更改。
在新路径真实正常/故障验收通过后一次切换并删除旧 AI JVM path；没有兼容 fallback 或两套同时可执行路由。

## 8. 真实验收 oracle 与完成条件

已有 `OllamaAiSqlE2ETest`/`SharedAiProvider` 使用真实部署和 HTTP access logs：completion 非空，
每次 SQL 实际请求计数增加；unknown model/wrong capability/invalid purpose 不增加请求；embedding
与直接 provider FLOAT32 结果在固定容差内一致；QUERY/DOCUMENT/RAW purpose 与 batch cardinality
无关；NULL 不发 provider 请求；generated value 走实际 committed write。它们证明旧链测试规格，
不证明新 Flight 服务已存在或本轮已运行。

替换的最小完整 cohort 需同时通过以下真实场景，不能以假 provider 或 fabricated metrics 代替：

1. 多进程 Coordinator + Worker Native + 独立 inference service + 真实 provider，日志/路由确认
   每个 AI call 都经过 Flight；同一测试拒绝任何 JNI AI callback 注册/调用。常规 UDF 仍正常。
2. 多行 text/embedding/multimodal 的真实 batch；数据库 batch size 与 serving actual batch/send
   数分别观测。embedding 以同模型 direct provider 的逐行 oracle 比对；生成文本不要求随机输出相等。
3. 嵌套 CASE/IF、三值 AND/OR、NULL primary/optional、异常分支、empty batch、dictionary/lazy 输入，
   对比分支选择和真实 provider invocation ids；未选媒体/错误分支绝不触发 I/O。adaptive reorder 政策也验证。
4. 真实模型版本更新/owner restart/tenant 权限撤销、过期/错误 audience/token、旧 attempt 重试；拒绝
   模型或 schema 漂移、重复/缺失/乱序 row ids，且不向错误租户发布。协议反例测试使用真实 decoder，
   这些反例不是 provider 质量证据。
5. 请求在排队、传输、backend 执行、结果 decode、发布竞争的各阶段真实取消/超时；证明发布先 revoke，
   transport buffers 与 serving leases 各自实际 drain。慢但不可取消 provider 保留服务 lease 至实际结束。
   backend drain oracle 使用真实支持取消的模型后端及其活动请求计数/退出证据，不伪造 cancel ACK。
6. 最后媒体/向量 owner 仍被下游持有时不提早释放；超大 request/response、服务失联/重连、预算耗尽、
   shutdown、过载和有限 retry 的真实故障。Native ASan、Java lifecycle 和跨进程 E2E 一并通过。
7. generated embedding、rerank、持久化结果与常规查询复用同一 service/模型契约；固定输入/schema/auth/
   snapshot/model identity，非确定性默认不复用。无 Agent 或业务账单旁路。

batch/queue/concurrency 的吞吐与尾延迟必须在受控硬件与足量样本上验证；本地 comparator、源码审计
和协议 fixture 都不是性能数据。当前冻结期只形成该设计与证据清单，尚未获得上述新链任何 PASS。

## 2026-10-08 模型调用授权复核（仍待实施）

正式边界为数据库实际调用独立推理服务，并持有模型选择、授权、资源预算和取消责任。
函数权限、模型权限及服务身份必须分别核对；Worker inventory 只能证明部署可用性，
不能代替数据库中该租户/主体当前模型授权与修订。

[Snowflake 当前 Cortex 权限](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql-privileges-and-access)
区分 AI function 使用权限及模型 RBAC；其 blanket 与 per-function 权限存在 OR 关系，
因此不能把某个函数撤权误判为已抵消 blanket 授权。此处采用明确 privilege 组合和
当前模型访问核对的设计依据，不复制其产品默认开放角色或历史 allowlist 兼容路径。
[BigQuery AI 权限](https://docs.cloud.google.com/bigquery/docs/permissions-for-ai-functions)
与[remote model 定义](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-remote-model-https)
将模型对象、远端 endpoint 和连接身份关联，并要求该身份具备 endpoint 访问权限。
这些是数据库调用服务的可核对边界，不能推导为模型库应该装入数据库内核。

当前源码具体差距：embedding JDBC registry 已有 revision/role/current authority；
通用 text/multimodal 的 ExactModelResolver 仅依赖 active inventory 同名模型唯一性，
没有同等持久模型对象/角色修订 authority。C 正在冻结优化前实际 deployment envelope，
这解决选型时点与重试一致性，尚不补足通用模型授权。后续应扩展既有模型 authority，
避免新增平行 registry；保留完整 authenticated subject/resolved tenant，以及实际
服务凭证引用。Durable background submission 不能由 principal string 伪造 empty
roles、EPOCH 或属性来重建原主体。

### 2026-10-08: exact configuration and asynchronous completion research

The independent technical catalog draft now has an actual PostgreSQL credential-reference rotation oracle. Public model signature is intentionally unchanged by that rotation. The configuration revision advances, the exact operation binding digest changes, and a previously captured compilation snapshot remains immutable. Ten PostgreSQL cases passed without skips; this is catalog/permission evidence, not model execution.

A separate fourteen-source dispatch draft adds `ModelBinding.binding_contract_sha256` (field 7). A sole projection copies the captured ModelAvailability operation digest, output type and request limits. Rerank planning retains the complete immutable captured availability instead of reconstructing it from copied scalar fields. Scheduling matches that digest and request limits. Native rejects an absent digest; the configured production Worker resolves providers only after comparing its actual descriptor with the canonical admitted task binding, before resolving secrets or making HTTP requests. Normal Java protoc 4.35.1 generation and direct JDK17 compilation passed with zero compiler diagnostics, including the two production bootstrap sources. An earlier incorrect protoc 25.3 run produced generated-code deprecation warnings and remains unqualified. The draft is unapplied; changed constructors still require owning tests and complete runtime validation. It does not implement the independent serving process or asynchronous Native RPC.

The latest [Triton cancellation contract](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/request_cancellation.html) explicitly delegates cancellation after backend admission to that backend; client cancellation is therefore not evidence that remote CPU/GPU work has terminated. [Decoupled inference](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/decoupled_models.html) requires bidirectional streaming and an explicit final response, with an opt-in empty final response when no output exists. Ordinary unary ModelInfer/HTTP is insufficient for this completion contract. [Current client documentation](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/client/README.html) exposes C++ request cancellation from r26.05, while completion callbacks and per-request call ownership still require explicit lifetime management. These are source facts, not proof that Pangrova uses that release or backend.

For Pangrova, the derived requirement is to distinguish revoking admission, requesting transport cancellation, joining local callbacks/buffers, and observing the service's final serving receipt. A terminal transport status must not manufacture a remote serving receipt. No-output completion also requires an explicit terminal event. Bounded input/output queues and actual owner leases must remain charged until their corresponding owner exits. The service retains remote work ownership through any provider operation that cannot be cancelled. Exact publication fencing remains the database's responsibility. [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html) is the typed database result protocol; its query/result cancellation alone does not supply the inference backend's completion evidence.


### 2026-10-08: actual tokenizer context and truncation contract

A pinned independent Ollama 0.6.2 ARM64 process loaded the actual learned nomic-embed-text:v1.5 model and returned finite 768-dimensional vectors. The model manifest is `0a109f422b47e3a30ba2b10eca18548e944e8a23073ee3f3e947efcf3c45e59f`. This component probe does not prove database invocation or the target asynchronous Native serving path.

The deployed GGUF exposes nomic-bert.context_length=2048. Its model parameters contain num_ctx=8192, but the [pinned EmbedHandler](https://github.com/ollama/ollama/blob/v0.6.2/server/routes.go) limits input to the smaller of configured context and model context. The [pinned OpenAI middleware](https://github.com/ollama/ollama/blob/v0.6.2/openai/openai.go) constructs an embedding request without the strict truncation setting. A real overlong input confirmed the difference: native `/api/embed` with truncate=false returned HTTP 400; OpenAI-compatible `/v1/embeddings` returned HTTP 200. The [current native API contract](https://docs.ollama.com/api/embed) likewise exposes explicit truncate control. Private deployment admission was reduced to the actual 2048-token context; this configuration change alone does not enforce tokens in the database.

Derived target contract: the independent serving process owns the exact deployed tokenizer and model context. It tokenizes the complete input after purpose prefixes and multimodal preprocessing, rejects over-limit rows before backend execution, and returns exact input/preprocessing identity and token counts in the authenticated serving receipt. Token context is per input; bounded encoded bytes, batch cardinality and actual resource leases remain separate limits. A tokenizer estimate or an HTTP byte limit cannot substitute for this check. Truncation changes the semantic input and therefore requires an explicit admitted policy included in the binding digest; default execution must reject it. The nomic [model card](https://huggingface.co/nomic-ai/nomic-embed-text-v1.5) requires purpose prefixes, which must participate in token limits and exact preprocessing identity. Its Hugging Face maximum is not evidence of the separately packaged GGUF's effective context.

The current synchronous Java provider advertises max-input-tokens but does not prove exact tokenizer enforcement, and adding an Ollama-specific truncate parameter to every OpenAI request would not establish a portable protocol contract. The canonical service adapter must select an explicit backend dialect and strict input policy under the captured deployment binding; there must be no fallback to a route that silently truncates. Implementation, actual database/JNI/Native positive and over-limit negative execution, backend drain receipts and quality evaluation remain pending.


## 2026-10-08：模型调用、AI 优化和未知完成的一级依据复核

边界结论继续保持：数据库 SQL 内核实际调用受治理模型；独立 Serving 承担模型设备和执行，数据库承担精确模型绑定、查询准入、选择行、预算、取消和结果发布 fence。SQL 推理能力需要贯通唯一 Calcite→Substrait→Velox→Arrow 链。Agent 编排和累计 Usage Ledger 仍按用户范围排除。

Snowflake 当前 [Cortex AI Functions 文档](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql) 明确提供 SQL 模型函数、独立权限要求和面向吞吐的批处理使用方式。这支持数据库模型调用边界，不能证明 Pangrova 已实现同等能力或同等性能。

新核对 Snowflake 作者 [Cortex AISQL 论文](https://arxiv.org/html/2511.07663v1) §5.1/§5.2：AI 谓词位置会影响模型调用成本，作者结合成本/选择率优化执行位置与顺序；其模型级联允许预算耗尽时采用 proxy 结果。Pangrova 的裁决是把 AI 成本纳入同一个优化器，同时必须先证明选择行、条件执行、调用来源与固定模型语义保持。论文中的 proxy 替代路径不符合本项目零 fallback、精确模型契约，不能直接移植，也不能沿用其性能数字。

| 一级依据 | Pangrova 模块落点 | 本次核对与验收边界 |
| --- | --- | --- |
| Snowflake SQL 模型函数与批处理 | Planner binding、模型 catalog、Worker 服务调用 | SQL 调用、模型权限和服务身份分别核对；当前真实 CPU serving 正控只属于组件 |
| AISQL AI 谓词成本/选择率 | Calcite 优化规则、同一 Native plan | 禁止把昂贵/带调用效果的函数一律下推或普通 CSE；原 CASE/COALESCE/source occurrence 断言和真实未选行零请求 oracle 都保留 |
| [RFC9110 §9.2.2](https://www.rfc-editor.org/rfc/rfc9110.html#section-9.2.2) | AiHttpTransport、provider retry、Stage retry | 非幂等请求的自动重发需实际幂等语义或原请求未应用的证据；连接断开本身不能充当证据 |
| [gRPC retry 文档](https://grpc.io/docs/guides/retry/) | Worker task dispatch/control、独立服务 transport | transparent retry 的安全范围是服务应用未收到调用；错误状态不能被扩展解释为模型未执行 |

已实际复现真实 ONNX 计算完成后连接 EOF，且缓存 replay 可以出现 compute=1、POST=2。因此“不重复计算”不等于“不重发”；正式 Task 验收同时查实际 send 数、服务 invocation identity、未发布结果和最终 typed error。现有协议已有 PROVIDER_OUTCOME_UNCERTAIN 类；JNI 正在独立修复真实异常 cause 投影，不能以 toString/NATIVE_TASK_FAILED 抹去完成不确定性。

当前 exact r24 Release 全4,179 PASS，515.82秒；完整源码6,872项、8个二进制、15份证据冻结于 qualified-r24-release-archive，source/CP/binary无漂移。新 ASAN configure 已通过，匹配 ASAN 全新编译进行中。该 Release 资格不证明新异步 Flight inference 路径存在，不转移到后继 Java deployment owner、ASAN、打包或 SQL 模型 E2E。实际 Testcontainers1.21.3清理使用强杀路径的缺口另行修复；原 CLI SIGTERM/held-handler退出0只证明该组件，不借用为Java部署drain。
