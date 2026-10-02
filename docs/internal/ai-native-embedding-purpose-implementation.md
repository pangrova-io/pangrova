# Embedding 输入用途与批处理不变性：完整实施规格

状态：根因已复证、生产契约正在原子迁移；必须与统一函数定义及模型绑定一起验收。
基线：2026-10-02 当前工作区。不是已通过的能力声明。

### 当前实施证据与未完成边界

- 已新增唯一 `EmbeddingBatchRequest(purpose, inputs)`，text/blob 都是非空、有序、
  同表示的 typed batch；长度为一不再切换语义。`EmbeddingProvider`、OpenAI-compatible、
  Versioned、Governed、invocation support 和 Worker SQL function 已迁移同契约。
- `AiInvocationRequest` 显式携带 embedding purpose，非 embedding 禁止携带；
  fingerprint 包含 purpose。生成列和生成值 artifact plan 显式 DOCUMENT。
  普通二参 SQL 的 RAW 默认归一化由唯一函数声明拥有，Planner/Native 全链仍待接线。
- 模型绑定维度在 Provider I/O 前检查，结果共用真实 cardinality/dimension/finite
  validator。响应为空或非 JSON object 归类为 PROVIDER_RESPONSE_INVALID，不泄漏 NPE。
- SPI 最近 `EmbeddingBatchRequestTest,EmbeddingBatchRequestPropertyTest` **7/7 PASS，
  0 skip；property 100 次、seed 20261002**。r2 5.045s，日志
  `/tmp/pangrova-production-embedding-purpose-spi-nearest-r2.log`。它只验证输入契约。
- Worker 及其 16-module reactor compile r1 **FAIL**：NativeCapabilityLoader 仍消费已
  删除字符串函数字段；迁移为完整 FunctionAvailability/manifest 后同命令 r2 **PASS，
  12.203s**。日志 `/tmp/pangrova-production-embedding-purpose-worker-compile-r{1,2}.log`。
  编译不等于 test-compile、Native 函数发布或 E2E。
- 原假 HTTP Provider 测试拆为真实 production request/response codec 验证，以及
  Ollama/Toxiproxy transport integration；原假向量 function 验证拆为调用前真实 provider
  检查、实际模型值/8+1尾批/NULL顺序/尾批 grant 拒绝，cardinality/finite 负例仍测
  production validator。没有把组件直连计为 SQL/JNI E2E。**新增 Worker 测试尚未执行**，
  旧 invocation service/Governed fake fixtures 已替换为真实 registry/admission/生产结果
  helper 验证；r2完整testCompile FAIL12 errors，r3七类 **63/63 PASS，0 skip，18.157s**。
  新真实 HTTP integration 已经同一 testCompile 编译，但**没有执行通过证据**。
  当前另一个资源/JNI原子迁移已开始，r3不代表最新所有源码仍可编译。
- Ollama public SQL E2E 已新增非空 QUERY/DOCUMENT 前缀、三种用途、NULL和尾批值 oracle，
  **尚未运行**。真实模型/网络失败与公开 SQL 链路分别验收，不能由纯 codec PASS 推导。
- 未完成：唯一函数默认和 constant argument 的 Planner/Native 归一化、查询 QUERY 消费者、
  模型 lifecycle/purpose admission、全部旧测试 caller 迁移、LLM usage 统计与内核资源预算
  的完整分离，以及同源完整 Step0–8。无外部 exactly-once 或生产性能通过声明。

合法派生 VALUES 用例在用途绑定前被旧 ANTLR 主入口拒绝；原仅要求 RuntimeException
的负例存在 parser failure 假绿。已保留原 SQL 并要求同形合法 RAW preflight 和精确
purpose validation cause，等待唯一 Calcite 前端完整 consumer 迁移，不修改成另一 SQL
绕过。详见 `ai-native-single-calcite-frontend-implementation.md`。

## 1. 根因和用户影响

真实入口不是两个不同的 SQL 函数：同一个 `AI_EMBED(model, input)` 会由
`AiEmbedFunction.invoke` 调用 `EmbeddingProvider.embed`，由 `invokeBatch`
调用 `embedBatch`。`OpenAiCompatibleEmbeddingProvider` 前者应用 query-prefix，
后者应用 document-prefix。`GovernedEmbeddingProvider` 转为统一请求后，
`AiEmbeddingInvocationSupport.invokeEmbeddingProvider` 又按输入数量选择这两个
方法，单例尾批因而切回 query-prefix。

当两种前缀不同，单行、批量、批次尾部、分片重划以及重放可能产生不同向量。
即使模型、输入、输出维度和 schema 全部相同，结果仍会改变；索引文档与查询
向量可能落入错误的输入语义。现有 Ollama SQL E2E 的配置前缀为空，不能复证
这项缺陷已消除。不能靠仅改一个调用为 embedBatch 或删除 query-prefix 修复。

## 2. 调研裁决

用途是语义，不是性能参数。Voyage 官方将 input_type 的 query/document 与
批量输入独立定义，并允许不指定用途；Cohere 官方分别定义 search_query 和
search_document。两者都没有按数组长度切换用途的规则。
[Voyage 输入类型](https://docs.voyageai.com/reference/embeddings-api)、
[Cohere Embeddings](https://docs.cohere.com/v2/docs/embeddings)。

Snowflake AI_EMBED 的公开接口接受模型与文本/FILE，返回 VECTOR；它不能证明
Pangrova 现有前缀配置应该按 cardinality 切换。
[Snowflake AI_EMBED](https://docs.snowflake.com/en/sql-reference/functions/ai_embed)。

建议规范支持 RAW、QUERY、DOCUMENT 三种用途。RAW 不添加检索前缀；QUERY 与
DOCUMENT 分别使用精确模型配置拥有的前缀。这是 Pangrova 的明确设计选择，
不是宣称上游接口完全相同。普通二参 SQL 默认 RAW，在验证时归一化一次；
显式第三个用途参数必须是合法常量。生成列及持久生成值计划固定 DOCUMENT，
检索查询表达式固定 QUERY。模型不支持某用途或模态时在绑定阶段明确拒绝。

## 3. 单一契约与实施顺序

1. 在唯一函数语义声明中定义 SQL arity、用途值、RAW 默认、常量要求、NULL
   政策以及 normalized argument 顺序。Calcite 校验/推导与 Native callback
   签名由该声明产生，不能分别追加默认参数或写第二清单。
2. Canonical Substrait 表达式必须显式携带已归一化用途。规范 enum 到 Native
   value 参数的转换由同声明生成；不得直接将当前 converter 不支持的 enum
   argument 当作已支持。复用统一协议/LogicalType，不新增 embedding task IDL。
3. Provider SPI 收敛为一份有序批输入契约，输入携带显式用途和文本/受治理 blob
   reference。单条只是长度为一的批，不存在单条/批量不同语义的 API。
   `AiInvocationRequest` 使用 typed embedding input，不从属性 Map、execution
   mode 或 cardinality 恢复用途。非 embedding 输入不能携带 embedding 用途。
4. 迁移所有实际实现与包装：OpenAI-compatible、Versioned、Governed、统一
   invocation service、AI SQL function。查询和生成列均使用相同调用链；
   provider 只应用已经绑定的用途，不自行选择默认值。
5. 迁移 `GeneratedColumnMutationExpressions`、
   `GeneratedValueArtifactBuildPlanBuilder`、durable generated-value hint 与其
   bind/replay 路径。`QueryTextEmbedRule` 当前是 directive，须核对实际 consumer
   并接通 QUERY，不能把存在一个规则类写成查询链已完成。
6. 精确模型 signature 已包含 canonical query/document-prefix 配置。继续保留
   该边界，并将支持的用途、输入模态与前缀应用规则纳入 binding contract。
   不能只按模型名字或维度认定两个执行部署等价，不能泄漏 Secret。
7. Invocation fingerprint 和任何结果复用键包含用途、模型精确身份与实际输入
   语义。物理请求 key 对该请求完整有序输入计算；行/物化值 identity 不依赖
   execution mode、batch 长度或 Worker 数。不能用一次 provider HTTP idempotency
   自动推导跨批、跨分片的外部 exactly-once；durable publication 仍使用原有
   owner/attempt/lease/fence，只有可信完整结果能够成为 winner。
8. 数量、顺序、维度、有限值与输出类型仍须原子校验。失败不向调用者发布前半批
   结果，NULL 不调用 provider、不替换为空字符串。实际模态不支持时拒绝，
   不逐行回落到另一能力或返回占位向量。
9. 迁移所有 caller、配置文档与原测试，再删除 embed/embedBatch/embedBlob 的
   语义分裂入口。不得保留兼容 forwarding、旧 epoch 或按输入数量选择分支。

统一函数 manifest 的规格见
[函数契约实施规格](ai-native-function-contract-implementation.md)。此处 RAW 的
规范默认值必须在该唯一声明中落地，不另建 Embedding 独立注册表。

## 4. 不得夹带 Usage Ledger

用户明确要求内核不做 Usage Ledger/LLM 用量统计。当前 `AiUsage` 将 token、
费用、币种与 bytes/latency/providerCalls 混在一个对象；`AiTaskBudget` 与
`AiInvocationBudgetManager` 还据其进行真实资源预留和返还。
`LoggingAiInvocationObserver` 输出 LLM token/费用，属于必须移除的统计残留。
因此不能机械删除整个对象或忽略 resource reconciliation 使调用失去约束。

需要独立原子迁移为内核执行资源约束：deadline、并发、实际输入/输出字节、
有界模型请求及 retry physical attempts、媒体大小、内存生命周期。Provider
max_tokens 等请求参数是生成行为/服务上界，不等于内核累计 LLM 消耗统计。
追完 protocol grant、operator binding、Native hidden layout、callback context、
provider request/response 与所有预算 caller 后替换，删除 token ledger/计费/币种
及统计 observer 扩展；保留权限决定、执行失败与必要资源可观测性。
目前这项删除尚未完成，不能以搜索不到 UsageLedger 类名声明清理通过。

## 5. 可执行验收

- 真实 Ollama TestContainer 与模型 digest，设置明显不同的 query/document 前缀。
  同一原始文本分别以 RAW/QUERY/DOCUMENT 直接调用真实 provider 作输出 oracle；
  三者之间确实不同，同用途在各执行方式中逐坐标一致。
- 保留原 public SQL 断言，新增一行、N 行、maxBatchSize+1 尾单例、重复值、
  NULL、多模型及重分片；验证结果位置、VECTOR 维度和实际输入前缀语义。
- SYNC DML 与 ASYNC durable artifact 使用 DOCUMENT；真实文件与 source snapshot
  保持物理 locator。检索查询使用 QUERY，比较最终排序而非仅验证函数名称。
- 真实 PostgreSQL 权限/模型库存；错用途、非常量、模型变更/撤权、错维度在
  provider 访问前拒绝。已发请求取消/超时必须有完整资源释放和终态证据。
- Toxiproxy 注入真实重试/断链，验证没有部分向量 publication。批处理不变性
  不等于保证第三方物理请求 exactly-once，应分别验证请求重试和 fenced 提交。
- 全 caller 检索零旧入口、无假 provider/HTTP、无 disabled/assumption；原范围
  回归及完整 Step0–8 全部通过后才记实现完成。组件直连不是 public SQL E2E。
