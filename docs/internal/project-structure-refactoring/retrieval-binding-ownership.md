# 检索绑定、精确模型解析与 AI 部署校验

本批以当前工作区 8,185 个文本文件建立基线。目标是按实际语义消除调度反向依赖，
不是通过合并包、扩大 Context 或加门禁豁免隐藏依赖环。

| 能力 | Owner 与输入 | 装配、状态与资源 |
| --- | --- | --- |
| CapabilityIdentities | Protocol；精确 ID/version/signature 与可选 required build pin 比较 | 纯 wire identity，无调度、授权、SDK 或资源；从 Scheduler 原 helper 提取，算法不变 |
| ExactModelResolver | Scheduler Core；已验证活跃的 CapabilityInventory 快照、名称、能力 | 不发现节点；完整 serving envelope 去重，不唯一或版本/signature/限制不完整即拒绝 |
| EmbeddingModelExecutionAdmission | AI Control `.model`；Catalog descriptor 与真实 Worker advertisement | 校验部署版本/签名、配置 binding digest、维度、批量限制；不创建 registry 或修改模型 |
| RetrievalIntentCollector | 现有 Query 调度绑定；明确 vector/reranker plan 与 tenant | 不接收 DispatchQueryContext；仍借用 WorkerManager，Cross-Encoder 委托唯一模型解析器 |
| RetrievalGenerationResolver | 现有 Query 制品绑定；明确 artifact intents、optional merge、fragment、splits | 不接收 DAGQueryContext；借用现有 publication resolver，未新增状态或资源 owner |

Server 的 CREATE/ALTER embedding 入口、DefaultWorkerSelector 与 generated build 测试调用方
明确将活跃 WorkerInfo 映射为库存快照。普通 Worker admission、缓存亲和、内存/并发调度继续
使用完整 WorkerInfo。Embedding 的当前 Catalog 权限和版本检查保持在匹配之前/之后的既有位置。

Cross-Encoder 删除重复的库存扫描，使用与普通模型选择相同的 exact envelope 拒绝规则；
无模型或歧义继续返回 PLANNER_VALIDATION_FAILURE，并保留原始失败原因。没有自动选择另一个
版本、降低能力、放宽资源限制或默认允许。此改动不等于证明所有 Cross-Encoder Provider 都已
完成端到端集成；真实支持仍以对应 Provider、装配与 E2E 证据为准。

检索 candidate budget、ANN 参数、fusion strategy、source identity 与 Native encoding 不变。
BM25 必须拥有当前 snapshot 的完整不可变制品；只有原先支持的向量路径可以显式回到精确查询。
重复/未消费/不属于 merge 的源必须拒绝。旧的 null DAG context 不触发制品解析的行为留在
DAGStagePlanBinder 这个实际调用 owner；解析器本身不再借用整个调度上下文。

证据：

- [FQN 处置](retrieval-binding-fqn-dispositions.tsv)、[逐生产类语义审查](retrieval-binding-semantic-evidence.json)、[差异](retrieval-binding-production-review.diff)、[提取的 identity 比较方法等价证据](retrieval-binding-identity-equivalence.json)。
- [调用方](retrieval-binding-callers.tsv)、[文件处置](retrieval-binding-file-dispositions.tsv)、[后端未变证据](retrieval-binding-backend-evidence.json)。
- [逐次验证](retrieval-binding-validation.tsv)、[测试明细](retrieval-binding-tests.tsv)、[最新定向结果](retrieval-binding-latest-selected-tests.tsv)。
- [JAR 审计](retrieval-binding-artifacts.json)、[包循环](retrieval-binding-package-cycles.txt)。

首次编译暴露三处普通 Worker 调度调用被迁移脚本过宽替换，已按实际调用恢复；不降低类型约束。
新增 Scheduler 测试后模块基线未更新导致 gate 失败，重新生成真实基线；不放宽 manifest。
新增精排绑定测试首次错误使用了非现有 Proto 字段，修复为既有 canonical inventory fixture；
未修改 wire schema 或放宽库存验证。所有首次失败、后续修复与实际测试结果分别记录。
