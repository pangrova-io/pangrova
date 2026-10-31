# 模型调用边界：源码、固定 SDK 与目标架构核对

核查日期：2026-10-15。状态：**设计与源码审计，独立推理服务尚未接入**。本文区分已运行的 Worker HTTP 路径、固定依赖的能力和待验收的目标；不以组件测试、最新上游文档或架构图冒充生产端到端证明。

## 结论与用语

“内核不调用模型”容易误导。准确边界是：**数据库的 SQL/Native 执行算子发起并治理模型调用；独立 Serving 进程持有权重、设备、provider 凭据和实际推理执行**。Coordinator 只解析精确模型及权限、编译和绑定、准入、调度、撤销发布权；Worker 数据面按选中行异步发送、校验 typed 结果并继续查询。独立服务可以位于同一集群的独立资源池，也可以是远端部署；跨进程不承诺零拷贝。Agent 对话/工具循环与累计 Usage Ledger 不属于这个内核边界。

这与 [Snowflake 的 SQL AI 函数和批处理建议](https://docs.snowflake.com/en/user-guide/snowflake-cortex/aisql)、[Velox 的外部推理 Async RPC 扩展](https://facebookincubator.github.io/velox/develop/async-rpc-functions.html)方向一致；它们证明模型调用可以是 SQL 执行的一部分，不证明 Pangrova 已实现目标，也不规定所有数据库都必须采用同一种服务或传输。

## 当前真实调用链与证据边界

```text
SQL / Calcite 表达式
  → Planner AiInvocationPlanHint
  → Coordinator 精确 Worker 模型 inventory / OperatorBinding / TaskRequest
  → Native Substrait scalar conversion / Velox JvmScalarFunction
  → 同步 JNI Object[][] / Worker NativeFunctionBridge
  → GovernedAiScalarFunction / AiInvocationService
  → Worker 内 provider 和 AiHttpTransport → 外部 HTTP 模型端点
  → JNI / Velox / Flight 查询结果
```

- `pangrova-coordination/pangrova-coordination-core/.../AiInvocationOperatorBinder.java` 把 `serviceId` 写为 `worker-local-governed-provider`；`ExactModelResolver` 从活跃 Worker inventory 解析同名精确模型。Embedding 有持久 catalog revision/角色 fence；通用 text/multimodal 尚未因此自动获得等价的持久模型权限权威。
- Worker 的 `META-INF/services/org.pangrova.function.api.registry.FunctionFactory` 注册 `AiFunctionFactory`；`NativeFunctionBootstrap` 注册 Java scalar callbacks。`NativeFunctionBridge.evaluateBatch` 接受 Native 的选行数组并建立可信 task context；`AiInvocationService` 在 Worker 内验证、准入和调用 provider；`AiHttpTransport` 负责真正的 HTTP entity send。模型与 provider 的生命周期目前属于 Worker。
- `pangrova-protocol/.../plan_extension.proto` 已定义 `AiInvokeDetail`，Native `CanonicalRelationSchema` 能读它的输出 schema；但 Planner 没有生成该关系，Native `SubstraitRelConversion` 没有 `ai_invoke` 执行分支，且仓库没有独立 inference-service 模块或 Native RPCNode 注册。因此声明和 schema reader 不是执行闭环。
- `OllamaAiSqlE2ETest` 使用真实 SQL、Native/JNI 和真实 Ollama HTTP 请求计数，证明上述**旧调用链**；Worker 的三个缓存模型集成类 13/13 通过，只证明真实 Ollama 组件调用。两者均不证明独立 Serving、Native 异步 RPC、服务侧资源 drain 或整套生产验收。最新滚动结果及失效范围见 [validation-current.md](validation-current.md)。
- 当前 OpenAI-compatible text batch 会对每个 prompt 发一个 HTTP 请求并 `allOf().join()`；数据库逻辑批次不能因此记作一次物理 provider batch。现有 POST 传输失败保守报告 `PROVIDER_OUTCOME_UNCERTAIN`、不可自动重发，详见 [AI provider POST outcomes](../../architecture/ai-provider-post-outcomes.md)。

## 固定 Velox SDK 与上游现行接口

Pangrova 的固定 Velox commit 是 `3da0390118fa4b0addbfbdeb03f438ec1b1237a6`，由 `pangrova-worker-native/cmake/PangrovaFunctionProvenance.cmake` 校验。不可用[现行 Velox Async RPC 指南](https://facebookincubator.github.io/velox/develop/async-rpc-functions.html)的接口说明代替此版本的编译证据。

| 固定版本的一手源码事实 | 接入前必须证明或修复 |
| --- | --- |
| [`RPCTypes.h`](https://raw.githubusercontent.com/facebookincubator/velox/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/common/rpc/RPCTypes.h) 的 request payload / response result 是 `std::string`，error 是 optional string。 | 使用完整升级后的固定 cohort 或受管理 SDK patch 建立 typed payload 与输出契约；即便字符串承载 IPC，也要计量复制和所有权，不能把向量/媒体 JSON 化后称为列式。 |
| [`AsyncRPCFunction.h`](https://raw.githubusercontent.com/facebookincubator/velox/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/expression/rpc/AsyncRPCFunction.h) 默认 `buildOutput` 为 VARCHAR，任何 error 都转 SQL NULL。 | 默认模型错误传播 typed 失败；只有函数明确规定的 NULL 输入才产生 SQL NULL。 |
| [`RPCOperator.cpp`](https://raw.githubusercontent.com/facebookincubator/velox/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/exec/rpc/RPCOperator.cpp) 对全输入建立选行、预加载并 flatten 所有列；`dispatchPerRow`/`flushBatch` 在取 limiter token 之前调用；batch 只验数量再按位置覆盖响应 rowId。 | 仅对真实选中行加载/发送，发送前取得准入，物理 fan-out 按实际 send 计量；在覆盖或 scatter 前验证服务返回的 attempt/batch/row 身份、唯一性与完整性。 |
| 同一 `RPCOperator::close` 释放自身引用；尚存 callback 可继续持有 state。 | 明确 revoke、transport cancel、callback/buffer drain 与 Serving owner receipt；关闭算子不是远端资源已退出的证明。 |

固定 SDK 的 [`IRPCClient::callBatch`](https://raw.githubusercontent.com/facebookincubator/velox/3da0390118fa4b0addbfbdeb03f438ec1b1237a6/velox/common/rpc/IRPCClient.h) 默认逐个调用；单 Driver batch 限制和无通用“任意 scalar → RPCNode”转换也必须按固定源码和实际计划验证。上游当前提供更丰富的 typed payload、预发送 admission、fan-out 单位与流控，但**升级是实施任务，不是现状**。不要以新增平行执行器或长期双路由掩盖 SDK 缺口。

## 目标契约与取舍

1. **规划与选行。** 在最终 Calcite 表达式优化之后绑定每个非确定性 invocation；复用现有 Substrait `ExtensionSingleRel` 的 typed `AiInvokeDetail`，让 Java producer 与 Native consumer 同版本支持输入/输出 schema、调用身份、argument ordinal、选行、passthrough、NULL 和分支依赖。[Substrait 原始定义](https://raw.githubusercontent.com/substrait-io/substrait/main/proto/substrait/algebra.proto)明确扩展的输出 schema 由双方约定。CASE/IF/COALESCE 只执行命中分支；AND/OR 遵守三值逻辑与错误遮蔽。Velox [表达式文档](https://facebookincubator.github.io/velox/develop/expression-evaluation.html)还说明 conjunct 可能自适应重排、确定性公共子表达式可能复用，故不能无条件把 AI 调用提升为上游 Project，也不能以 SQL 文本顺序作为模型调用保证。复杂嵌套需要经过测试的 guarded continuation；无法表达的形状应明确拒绝。
2. **独立服务。** 通过受版本治理的 deployment manifest 发现实际 Serving，而非把旧 Worker 本地 provider inventory 当成服务可用性。服务独立部署、健康检查、扩缩容，持 provider/secret/受治理 FILE 读取和模型 runtime；Worker 仅持服务客户端及自己拥有的 transport lease。数据库仍验证模型对象、当前租户/主体权限、精确 deployment 与 function semantic identity；服务逐调用校验 audience、scope、过期、task attempt、schema/input digest 和资源租约。连接级 TLS 不替代逐调用授权。
3. **数据传输。** Pangrova 已采用 Arrow/Flight 数据面，故优先评估 Flight `DoExchange` 的 typed RecordBatch + 应用自定义 `app_metadata`；[Flight 规范](https://arrow.apache.org/docs/format/Flight.html)允许双向流和逐调用认证，但不定义 Pangrova 的推理 envelope、行身份或幂等语义。应与 typed gRPC 在真实 text/vector/媒体大小、吞吐、P99 和复制字节下比较；Flight 是适配当前列式系统的候选，不是普遍“业界最优”的证明。服务到厂商端可能仍是逐行 HTTP，必须按实际物理调用计量。
4. **批处理、重试与取消。** SQL batch 是选中行的一次传输，Serving 可以按模型版本/shape/deadline/容量做有界动态组批或拆批；后端不支持原生 batch 时如实计逐行 send。[Triton batching](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/batcher.html)要求用实际 latency/throughput 调参，不保证 preferred size 对本负载最优。未知 POST 结果禁止因有 Idempotency-Key 就重放；服务须先证明持久去重与可查询结果。取消先撤销数据库结果发布权，再取消 stream 并等待本地 owner；服务独立保留 backend lease，直到 backend 真正退出或不可取消请求实际完成。[Triton 取消文档](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/user_guide/request_cancellation.html)明确 backend 已开始执行后的早停依实现，client CANCELLED 不是 GPU/buffer drain 收据。

独立 Serving 的收益是权重/GPU/凭据隔离、独立容量和故障域；成本是网络、认证、部署、版本对齐和远端 owner 生命周期。当前 Worker 请求外部 HTTP 对小规模场景可以是可用的过渡实现，但不满足本项目已定的独立 Serving/Native 异步路径目标。是否以及何时迁移，应由这些明确目标和真实正确性/性能证据决定，不借“行业唯一最佳”作论据。

## 增量实施与可执行验收

| 顺序 | 实施单元 | 完成证据 |
| --- | --- | --- |
| 1 | 冻结模型对象/权限修订、完整 deployment 与凭据引用、精确 function signature；定义跨进程 authority、schema digest、attempt/row/batch 身份和终态。 | 当前角色撤销、模型升级、secret rotation、过期/错误 audience、旧 attempt 均 fail closed；冻结计划不被活动 inventory 偷换。 |
| 2 | 升级或受管理补丁化固定 Velox RPC cohort；typed output/error、预发送 admission、物理单位、真实 row identity、callback owner drain。 | 固定 SDK 的单元/Native ASan 与恶意重复、缺失、乱序、外来 row 响应反例通过；没有“错误转 NULL”或响应数相等即接受。 |
| 3 | 实现 Calcite 最终表达式 → `AiInvokeDetail` → Native RPCNode，包含 guarded continuation；构建一个可部署的独立 Serving + 真实 provider 适配器。 | 多进程 Coordinator + Native Worker + Serving + 真 Ollama：SQL 请求与服务 Flight 收件、provider 实际 send 逐一对应；AI JNI callback 调用计数为 0，普通非 AI UDF 仍可用。 |
| 4 | 一次切换全部 AI SQL、Embedding、rerank、生成值和 FILE 受治理路径，删除旧 AI Worker provider/JNI 注册；保持同一语义 cohort。 | 多行 typed embedding 与同模型 direct provider 向量按固定容差比对；completion 验证非空与请求数；CASE/COALESCE/三值 AND/OR/NULL 未选行零请求，empty/dictionary/lazy batch 不丢行；错误/维度/nullable/schema 漂移一律拒绝。 |
| 5 | 完成取消、失联、未知完成、限流、资源与结果发布 fence。 | 排队、发送、backend 执行、decode 和发布竞争各阶段真实故障：无迟到发布/越权结果、无盲重发；分别观察本地 buffer/transport drain 与服务 backend owner 收据，失联状态保留 UNKNOWN。 |
| 6 | 做同源码发布级全回归和规模测量。 | Native Release/ASan、Java reactor、跨进程 SQL E2E 与打包镜像均通过；真实服务在受控硬件及足量样本上报告吞吐、P99、队列/执行耗时、物理 send、复制字节和资源峰值。此前任何局部 PASS 不自动转移到新 cohort。 |

以上是目标验收清单，**截至核查日没有一项可据此宣称独立 Serving 链已经 PASS**。现有 Worker/Ollama 结果应继续保留为旧路径回归 oracle，迁移后改为验证新路径，不得删断言或靠假 provider 获取绿色报告。
