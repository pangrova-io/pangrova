# Pangrova AI Native 能力演进报告

> 基于 Databricks Data+AI Summit 2025 & Snowflake Summit 2025 深度调研
> 日期: 2026-06-23
> 状态: 历史调研输入，不代表当前实现状态；当前计划以
> `ai-native-gap-analysis-and-roadmap-2026.md` 为准。

---

## 一、行业趋势总览

2025年 Databricks 和 Snowflake 的 Summit 揭示了数据平台 AI Native 化的三大核心趋势：

| 趋势 | Databricks 代表产品 | Snowflake 代表产品 |
|------|--------------------|--------------------|
| **Agentic AI（智能体原生）** | Agent Bricks, MCP Support, AI Gateway | Cortex Agents, Snowflake Intelligence, Cortex Knowledge Extensions |
| **多模态数据统一处理** | AI Functions Multimodal, Unity Catalog FILE type, Daft | Cortex AI COMPLETE Multimodal, Cortex AISQL, Openflow |
| **AI基础设施生产化** | AI Runtime, High-Scale Model Serving, MLflow 3.0 | Arctic Inference, Adaptive Compute, AI Observability |

---

## 二、Databricks Data+AI Summit 2025 核心发布

### 2.1 Agent Bricks（Beta）

**定位**: 生产级 AI Agent 构建平台，自动化评估与优化

- 用户只需描述 Agent 任务，系统自动生成 Task-Specific Evaluators 和 LLM Judges
- 内置四种 Agent 模板: 信息提取 Agent、知识助手 Agent、多Agent 协调器、自定义 LLM Agent
- **核心创新**: 可编程评估输出质量与成本的关系，自动推荐最佳模型组合
- 支持 Domain-specific synthetic data generation

**对 Pangrova 的启示**:
- 需要提供 Agent 构建的 SQL 原语（如 CREATE AGENT, EVALUATE AGENT）
- 需要内建 Agent 编排/评估框架，而非仅提供底层 AI 函数

### 2.2 AI Functions Multimodal（GA）

**定位**: SQL 中直接处理图像、文档等多模态数据

- `ai_parse_document`: 从复杂文档中提取结构化信息
- AI Functions 性能提升 3x，成本降低 4x（对比竞品）
- 支持跨文本、图像、音频的多模态分析

**对 Pangrova 的启示**:
- 当前 Pangrova 仅有 `BlobEmbeddingResolver` 做 multimodal embedding
- 缺少 `AI_PARSE_DOCUMENT`、图像理解、音频转写等函数
- 需要扩展 AI Functions 到完整多模态处理能力

### 2.3 Storage-Optimized AI Search（Public Preview）

**定位**: 存算分离的向量搜索引擎

- 完全重写的向量搜索基础设施，基于存算分离架构
- 支持 billions 级别向量，成本降低 7x
- 针对 RAG 和语义搜索场景优化

**对 Pangrova 的启示**:
- 当前 Pangrova 有 HNSW + IVFPQ，但缺少存算分离的大规模向量存储层
- DiskANN 适配已在路线图中，需加速推进

### 2.4 Lakebase（Postgres 兼容事务数据库）

**定位**: 为 AI Agent 提供低延迟事务层

- Postgres 兼容的全托管事务数据库
- 独立存储和计算，持续自动扩缩
- 低延迟（<10ms）、高并发（>10K QPS）
- **Branch 特性**: Copy-on-Write 数据库克隆用于 AI 实验

**对 Pangrova 的启示**:
- Pangrova 已有 Branch DDL 能力（`BranchDdlExecutor`），可对标 Lakebase Branch
- 可增强为 Agent 会话提供隔离的数据沙箱

### 2.5 MCP Protocol 集成

- MCP Server 可通过 Databricks Apps 托管
- Agent 可直接连接 Unity Catalog Functions、Genie、AI Search
- Playground 环境直接测试 MCP 模型

### 2.6 MLflow 3.0

- 从零重新设计，面向 GenAI
- 跨平台 Agent 可观测性（支持非 Databricks 环境）
- Prompt Registry：注册、版本化、测试、部署 LLM Prompts

### 2.7 Unity Catalog 多模态支持

- 新增 `FILE` 类型：在开放格式中管理 PDF、图像、音频、视频等非结构化数据
- Iceberg Managed Tables：统一治理 Iceberg 表
- Metrics 层：在数据层定义可复用业务指标

---

## 三、Snowflake Summit 2025 核心发布

### 3.1 Snowflake Intelligence & Cortex Agents

**定位**: 企业级 Agentic AI 系统

**五大支柱架构**:

| 支柱 | 核心能力 |
|------|---------|
| 1. Agentic Orchestration | 规划、推理、工具编排（LRM + LLM 协调循环） |
| 2. Structured Data Intelligence | Cortex Analyst — 自然语言→精确SQL（Semantic Models） |
| 3. Unstructured Data Intelligence | Cortex Search — 多跳推理、消歧、多模态检索 |
| 4. Observability & Trust | OpenTelemetry traces、TruLens集成、全链路审计 |
| 5. Inference Optimization | Prefix Caching、SwiftKV (2x吞吐)、Speculative Decoding (3-4x加速) |

**关键技术细节**:
- **Dynamic Multi-hop Planning**: Agent 不是静态执行计划，而是根据中间结果动态调整策略
- **Verified Diversification**: 对模糊查询进行多义检索后验证，Groundedness 提升 1.8x
- **ComposeRAG**: 将复杂问题分解为子问题链，逐步检索与验证
- **Hybrid Multimodal Retrieval**: VM3多模态向量 + BM25关键词 + Neural Reranker

**对 Pangrova 的启示**:
- Pangrova 已有 `ConversationManager` 和 `SemanticCache`，但缺少完整的 Agentic Orchestration 层
- 需要增加 Text-to-SQL（类似 Cortex Analyst）能力
- 已有 `CascadeReranker` 和 `RrfReranker`，可对标 Cortex Search 的 Hybrid Retrieval

### 3.2 Cortex AI COMPLETE Multimodal（Public Preview）

**定位**: SQL 中的多模态 AI 分析

- 直接在 SQL 中分析图像、文档等非结构化数据
- 支持模型: Claude 3.5/3.7 Sonnet, Pixtral Large, GPT-4.1, Llama 4 Scout
- 跨 Snowflake 表、Iceberg 表、S3 Object Storage 无需移动数据
- 统一安全治理

**SQL 示例**:
```sql
SELECT
    c.ad_id,
    snowflake.cortex.complete('claude-3-5-sonnet',
        'Classify the prominent color in this image', ad_images) as color
FROM campaign_table c JOIN image_table i ON c.ad_id = i.ad_id
```

**多模态能力矩阵**:

| 能力 | 最佳模型 | Benchmark |
|------|---------|-----------|
| 文档理解 | Claude 3.5 Sonnet | DocVQA 90.3% |
| 图表分析 | Pixtral Large | ChartQA 88.1% |
| 图像理解 | GPT-4.1 | MMMU 74.8% |
| OCR | Snowflake PARSE_DOCUMENT | ANLS 0.974 |

### 3.3 Cortex AISQL

- 将 AI 处理能力嵌入 SQL 语法（不是函数调用，而是语法级集成）
- 处理文档、图像等多模态数据
- Document AI 增强：Schema-aware 表格提取

### 3.4 Snowflake Openflow（GA on AWS）

**定位**: 开放的多模态数据摄入服务

- 基于 Apache NiFi 的托管摄入服务
- 消除数据孤岛，简化多模态数据接入
- 数百个连接器，支持 CDC 复制
- 无缝 ETL 处理用于 AI

### 3.5 Semantic Views（Public Preview）

- 在 Snowflake 中定义和存储业务指标与实体关系
- 帮助 AI 助手和 BI 工具产出更精确、一致的结果
- 桥接原始数据与业务理解的语义层

### 3.6 AI Observability

- 无代码监控 GenAI 应用
- 基于 OpenTelemetry 的全链路追踪
- 集成 TruLens 开源工具
- 支持评估答案质量、步骤正确性、执行效率

---

## 四、多模态存储技术趋势深度分析

### 4.1 行业共识：统一多模态数据管理

| 维度 | Databricks 方案 | Snowflake 方案 | 行业趋势 |
|------|----------------|----------------|----------|
| **存储层** | Unity Catalog FILE type + Delta Lake | Stage + Iceberg + Object Storage 直连 | 开放格式存储非结构化数据 |
| **类型系统** | FILE type in UC | VARIANT + Directory Tables | 原生支持 blob/file 类型列 |
| **处理层** | Daft (AI-native DataFrame) + AI Functions | Cortex COMPLETE Multimodal + AISQL | SQL-level 多模态处理 |
| **索引层** | Storage-Optimized AI Search | Cortex Search (VM3 + BM25 + Reranker) | 多模态向量 + 混合检索 |
| **治理层** | Unity Catalog 统一治理 | Horizon Catalog | 结构化+非结构化统一治理 |

### 4.2 多模态存储架构模式

```
┌─────────────────────────────────────────────────────────┐
│                  SQL Query Layer                          │
│  AI_COMPLETE(model, image_col)                          │
│  AI_PARSE_DOCUMENT(pdf_col)                             │
│  AI_EMBED(model, blob_col)                              │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────┼────────────────────────────────┐
│              Multimodal Processing Engine                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
│  │  Vision  │  │  Audio   │  │ Document │             │
│  │  Models  │  │  Models  │  │  Parser  │             │
│  └──────────┘  └──────────┘  └──────────┘             │
└────────────────────────┬────────────────────────────────┘
                         │
┌────────────────────────┼────────────────────────────────┐
│         Unified Storage (Open Format)                    │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐          │
│  │  Tables   │  │   Files   │  │  Indexes  │          │
│  │(Iceberg/  │  │(PDF/Image/│  │(Vector/   │          │
│  │ Paimon)   │  │Audio/Video│  │ BM25/     │          │
│  │           │  │           │  │ Multimodal│          │
│  └───────────┘  └───────────┘  └───────────┘          │
└─────────────────────────────────────────────────────────┘
```

### 4.3 Claim-Check 模式 vs 内联存储

| 模式 | 适用场景 | 优势 | Pangrova 现状 |
|------|---------|------|-------------|
| **Claim-Check** (元数据引用) | 大文件(>1MB): 图像/视频/PDF | 不膨胀表存储，支持外部存储 | **已实现** (`BlobEmbeddingResolver`) |
| **Inline Storage** (内联存储) | 小数据(<1MB): 缩略图/音频片段 | 减少IO跳数，事务一致 | 未实现 |
| **Hybrid** (混合) | 自适应选择 | 最优性价比 | 未实现 |

---

## 五、Pangrova 现有 AI Native 能力基线

### 5.1 已实现能力清单

| 层级 | 能力 | 实现状态 | 对标竞品 |
|------|------|---------|---------|
| **AI SQL 函数** | AI_COMPLETE, AI_EMBED, AI_CLASSIFY, AI_SUMMARIZE, AI_FILTER, AI_EXTRACT, AI_GUARD, AI_PREDICT, AI_TRANSLATE | ✅ 已实现 | Snowflake Cortex AI Functions |
| **向量搜索** | HNSW, IVFPQ, 混合搜索(ANN+Filter), VectorSearchRewriter | ✅ 已实现 | Databricks AI Search |
| **Embedding 管理** | Model Registry, Auto-Embedding DDL, Backfill Coordinator, Queue管理 | ✅ 已实现 | 独特能力 |
| **多模态 Embedding** | BlobEmbeddingResolver (Claim-Check, CLIP等) | ✅ 基础实现 | Snowflake COMPLETE Multimodal |
| **Reranking** | CascadeReranker, CrossEncoder, RRF, Weighted | ✅ 已实现 | Cortex Search Reranker |
| **语义缓存** | HnswBackedSemanticCache, InMemorySemanticCache | ✅ 已实现 | 独特能力(领先) |
| **对话上下文** | ConversationManager, ConversationContextRewriter | ✅ 已实现 | Snowflake Intelligence |
| **Branch DDL** | BranchDdlExecutor（数据沙箱/分支实验） | ✅ 已实现 | Databricks Lakebase Branch |
| **AI 预算** | 未接通实现已清理，等待统一 invocation contract | ❌ 未实现 | Databricks AI Gateway |
| **向量索引生命周期** | VectorIndexHealthAssessor, IndexRebuildScheduler, IndexRecommendationEngine | ✅ 已实现 | 独特能力(领先) |
| **Training Data Server** | TrainingDataServer, TrainingDataProducer | ✅ 已实现 | — |
| **Data Layout Advisor** | DataLayoutAdvisor | ✅ 已实现 | Snowflake Adaptive Compute |

### 5.2 能力差距分析

| 能力 | 竞品状态 | Pangrova 现状 | 差距等级 |
|------|---------|------------|---------|
| **多模态SQL处理** (图像分析/文档解析) | Snowflake GA / Databricks GA | 仅有 Blob Embedding | 🔴 P0 关键差距 |
| **Agentic Orchestration** | Snowflake Cortex Agents / Databricks Agent Bricks | 无 Agent 编排层 | 🔴 P0 关键差距 |
| **Text-to-SQL / Natural Language Query** | Snowflake Cortex Analyst / Databricks Genie | 无 | 🔴 P0 关键差距 |
| **多模态向量检索** (图文混合) | Snowflake VM3 / Databricks multimodal search | 仅支持 text embedding | 🟠 P1 重要差距 |
| **MCP Protocol 支持** | Databricks 原生支持 | 无 | 🟠 P1 重要差距 |
| **AI Observability** (全链路追踪) | Snowflake OpenTelemetry / Databricks MLflow 3 | 当前无 AI 专属 telemetry | 🟠 P1 重要差距 |
| **Document Intelligence** (OCR/表格提取) | Snowflake Document AI / Databricks ai_parse | 有 Parser SPI 但无实现 | 🟠 P1 重要差距 |
| **Semantic Views / Metrics Layer** | Snowflake Semantic Views / Databricks UC Metrics | 无 | 🟡 P2 增强 |
| **Inference Optimization** (投机解码/KV缓存) | Snowflake Arctic Inference / Databricks AI Runtime | 依赖外部推理服务 | 🟡 P2 增强 |
| **Agent Evaluation Framework** | Databricks Agent Bricks Evaluator | 无 | 🟡 P2 增强 |

---

## 六、AI Native 能力演进方案

### Phase 1: 多模态数据统一处理（P0, Q3 2026）

#### 6.1.1 多模态 SQL 函数扩展

**目标**: 在 SQL 中直接处理图像、文档、音频

```sql
-- 图像分析
SELECT AI_COMPLETE('claude-sonnet', 'Describe this image', image_col)
FROM product_catalog;

-- 文档解析
SELECT AI_PARSE_DOCUMENT(pdf_col, format => 'table') AS structured_data
FROM contracts;

-- 音频转写
SELECT AI_TRANSCRIBE(audio_col, language => 'zh') AS transcript
FROM call_recordings;

-- 多模态 embedding
SELECT AI_EMBED('clip-v2', image_col) AS visual_embedding
FROM product_images;
```

**实现要点**:

| 组件 | 改动 |
|------|------|
| `pangrova-spi` | 新增 `MultimodalProvider` SPI: vision/audio/document 三个子接口 |
| `pangrova-worker/ai/function` | 新增 `AiCompleteMultimodalFunction`, `AiParseDocumentFunction`, `AiTranscribeFunction` |
| `pangrova-worker/ai/document` | 实现 `PdfDocumentParser`, `HtmlDocumentParser` (SPI 已定义) |
| `pangrova-parser` | 扩展 `AI_COMPLETE` 函数签名支持 BLOB/FILE 列参数 |
| `pangrova-common` | 新增 `MimeType` 枚举和 `ContentTypeRouter` |

#### 6.1.2 FILE 类型与多模态存储

**目标**: 原生管理非结构化数据

```sql
-- 创建包含 FILE 列的表
CREATE TABLE documents (
    id BIGINT,
    content FILE,             -- 非结构化数据列 (Claim-Check 自动管理)
    metadata MAP<VARCHAR, VARCHAR>,
    embedding VECTOR(1024, FLOAT)
) WITH (
    file_storage = 's3://bucket/files/',
    auto_embed = ('content', 'clip-v2', 'embedding')
);

-- 插入时自动管理存储
INSERT INTO documents (id, content)
VALUES (1, FILE_FROM_URL('s3://bucket/report.pdf'));
```

**架构设计**:

```
FILE Column (Claim-Check Struct)
├── uri: VARCHAR          -- 外部存储路径
├── mime_type: VARCHAR    -- 内容类型
├── size: BIGINT          -- 字节大小
├── checksum: VARCHAR     -- SHA-256
├── created_at: TIMESTAMP -- 创建时间
└── metadata: MAP         -- 自定义元数据
```

**实现要点**:
- 扩展 `pangrova-parser` 支持 `FILE` 类型语法
- `pangrova-connector-iceberg/paimon` 支持 File 列序列化（存 Claim-Check struct）
- `pangrova-worker/ai/blob` 扩展 `BlobContentReader` 支持流式读取大文件
- External Object Storage 生命周期管理（GC orphaned files）

#### 6.1.3 多模态向量检索

**目标**: 支持图像→文本、文本→图像等跨模态检索

```sql
-- 以图搜图
SELECT * FROM products
ORDER BY COSINE_DISTANCE(image_embedding, AI_EMBED('clip', FILE_FROM_URL(?)))
LIMIT 10;

-- 图文混合检索 + Reranking
SELECT * FROM knowledge_base
WHERE MATCH(text_content, 'query')
   OR COSINE_DISTANCE(visual_embedding, ?) < 0.3
ORDER BY RRF(text_score(text_content, 'query'), vector_score(visual_embedding, ?))
LIMIT 20
RERANK BY 'cross-encoder' USING (text_content, 'query');
```

---

### Phase 2: Agentic AI 基础设施（P0, Q4 2026）

#### 6.2.1 Agent Orchestration Layer

**目标**: 提供 SQL-native 的 Agent 定义与编排

```sql
-- 定义 Agent
CREATE AGENT sales_analyst
USING MODEL 'gpt-4o'
WITH TOOLS (
    cortex_sql AS sql_tool,       -- Text-to-SQL 工具
    cortex_search AS search_tool, -- 语义搜索工具
    cortex_chart AS viz_tool      -- 可视化工具
)
WITH OPTIONS (
    planning_model = 'reasoning-v1',
    max_steps = 10,
    token_budget = 100000
);

-- 调用 Agent
SELECT AGENT_ASK(
    'sales_analyst',
    'Why did Q2 revenue drop compared to Q1?',
    context => CURRENT_SESSION()
);
```

**架构设计**:

```
┌─────────────────────────────────────┐
│         Agent Orchestrator           │
│  ┌───────────┐  ┌───────────┐      │
│  │  Planner  │  │  Executor │      │
│  │  (LRM)    │──│  (Tools)  │      │
│  └───────────┘  └───────────┘      │
│         │              │             │
│  ┌──────┴──────┐  ┌──┴─────────┐  │
│  │ Conversation│  │ Tool Catalog│  │
│  │ Context     │  │ (SQL, Search│  │
│  │             │  │  External) │  │
│  └─────────────┘  └────────────┘  │
└─────────────────────────────────────┘
```

**实现要点**:

| 模块 | 新增组件 |
|------|---------|
| `pangrova-coordinator/ai/agent` | `AgentOrchestrator`, `AgentPlanExecutor`, `ToolCatalog` |
| `pangrova-spi/ai/agent` | `AgentProvider`, `ToolProvider`, `PlanningProvider` SPI |
| `pangrova-parser` | `CREATE AGENT`, `AGENT_ASK()`, `DROP AGENT` DDL/函数语法 |
| `pangrova-coordinator/ai/conversation` | 增强为 Agent session 级别（已有基础） |

#### 6.2.2 Text-to-SQL (Natural Language Query)

**目标**: 用户可用自然语言查询数据

```sql
-- 注册 Semantic Model
CREATE SEMANTIC MODEL sales_model ON schema sales
WITH (
    description = 'Sales analytics semantic layer',
    metrics = ['revenue', 'conversion_rate', 'churn'],
    auto_discover = TRUE
);

-- 自然语言查询
SELECT * FROM TABLE(NL_QUERY(
    'What were our top 5 customers by revenue last quarter?',
    semantic_model => 'sales_model'
));
```

**技术路径**: 利用已有 Calcite SQL AST 基础设施 + LLM 的 schema-aware prompting

#### 6.2.3 MCP Protocol 支持

**目标**: Pangrova 作为 MCP Server 为外部 AI Agent 提供数据工具

```
┌─────────────────────┐
│  External AI Agent   │
│  (Claude/GPT/etc)    │
└──────────┬──────────┘
           │ MCP Protocol
┌──────────┴──────────┐
│  Pangrova MCP Server   │
│  ├── query_tool      │  ← 执行 SQL 查询
│  ├── search_tool     │  ← 语义搜索
│  ├── catalog_tool    │  ← 表/列元数据发现
│  └── embed_tool      │  ← 生成 Embedding
└─────────────────────┘
```

**实现要点**:
- `pangrova-protocol` 模块新增 MCP transport (JSON-RPC over stdio/HTTP)
- 将现有 SQL 执行、向量搜索、元数据查询包装为 MCP Tools
- 支持 Resource 暴露（表 schema、统计信息等）

---

### Phase 3: AI Observability & Governance（P1, Q1 2027）

#### 6.3.1 AI 全链路可观测性

**目标**: 对每次 AI 交互提供完整审计和质量评估

```sql
-- 查看 AI 调用追踪
SELECT * FROM system.ai_traces
WHERE session_id = ?
ORDER BY timestamp;

-- 查看 Agent 执行步骤
SELECT step_id, tool_name, input, output, latency_ms, token_count
FROM system.agent_execution_log
WHERE agent_name = 'sales_analyst'
  AND execution_id = ?;
```

**实现架构**:
- 基于 OpenTelemetry 的 AI Span 模型
- 集成已有的 `pangrova-observability` 模块
- 新增 AI-specific metrics: token usage, model latency, quality scores, cost

#### 6.3.2 Semantic Layer (语义层)

**目标**: 统一业务指标定义，提升 AI 查询准确性

```sql
-- 定义语义视图
CREATE SEMANTIC VIEW revenue_metrics AS
SELECT
    date_trunc('month', order_date) AS month,
    SUM(amount) AS total_revenue,
    COUNT(DISTINCT customer_id) AS active_customers,
    SUM(amount) / COUNT(DISTINCT customer_id) AS arpu
FROM orders
GROUP BY 1
WITH DESCRIPTION 'Monthly revenue KPIs';

-- AI Agent 自动使用语义层
SELECT AGENT_ASK('analyst', 'Show me ARPU trend');
-- Agent 自动发现 revenue_metrics 语义视图，生成精确 SQL
```

#### 6.3.3 AI Gateway & Model Routing

**目标**: 统一 AI 模型治理入口

```sql
-- 注册模型端点
CREATE MODEL ENDPOINT openai_gpt4
PROVIDER 'openai'
WITH (api_key_secret = 'openai_key', model = 'gpt-4o');

CREATE MODEL ENDPOINT local_llama
PROVIDER 'ollama'
WITH (endpoint = 'http://localhost:11434', model = 'llama3.1');

-- 定义路由策略
CREATE MODEL ROUTE balanced_route
PRIMARY openai_gpt4
FALLBACK local_llama
WITH (
    rate_limit = 1000,  -- RPM
    cost_limit = 100.0, -- USD/day
    pii_guard = TRUE
);
```

---

### Phase 4: Advanced AI Capabilities（P2, Q2 2027）

#### 6.4.1 推理优化

| 技术 | 效果 | 实现路径 |
|------|------|---------|
| Prefix Caching | 重复 prompt 加速 | 在 InferenceProvider 层实现 prompt hash cache |
| Speculative Decoding | 3-4x 生成加速 | 集成 draft model 在 worker 侧 |
| Batch Inference | 大规模数据处理 | 扩展 `AiBatchBuffer` 支持异步批处理 |
| KV Cache Offload | 长上下文支持 | Worker Native 层利用 NVMe/SSD 存 KV cache |

#### 6.4.2 Agent Evaluation Framework

```sql
-- 创建评估数据集
CREATE EVALUATION DATASET sales_qa
FROM (
    SELECT question, expected_answer, context
    FROM evaluation_golden_set
);

-- 运行评估
SELECT * FROM TABLE(EVALUATE_AGENT(
    'sales_analyst',
    dataset => 'sales_qa',
    metrics => ['accuracy', 'groundedness', 'latency', 'cost']
));
```

#### 6.4.3 Fine-Tuning 管道

```sql
-- 基于查询历史微调模型
CREATE FINE_TUNED MODEL custom_sql_model
BASE MODEL 'codellama-7b'
TRAINING DATA (
    SELECT query_text AS input, validated_sql AS output
    FROM system.query_history
    WHERE execution_status = 'SUCCESS'
      AND feedback_score >= 4
)
WITH (epochs = 3, learning_rate = 2e-5);
```

---

## 七、优先级排序与里程碑

```
          Q3 2026          Q4 2026          Q1 2027          Q2 2027
    ┌────────────────┬────────────────┬────────────────┬────────────────┐
P0  │ Multimodal SQL │ Agent Orch.    │                │                │
    │ FILE Type      │ Text-to-SQL    │                │                │
    │ Doc Parse      │ MCP Support    │                │                │
    ├────────────────┼────────────────┼────────────────┼────────────────┤
P1  │                │ Multimodal Vec │ AI Observ.     │                │
    │                │                │ Semantic Layer │                │
    │                │                │ AI Gateway     │                │
    ├────────────────┼────────────────┼────────────────┼────────────────┤
P2  │                │                │                │ Inference Opt. │
    │                │                │                │ Agent Eval     │
    │                │                │                │ Fine-tuning    │
    └────────────────┴────────────────┴────────────────┴────────────────┘
```

---

## 八、技术决策与架构原则

### 8.1 核心架构决策

| 决策 | 选择 | 理由 |
|------|------|------|
| 多模态存储模式 | **Claim-Check + 外部对象存储** | 已有基础设施,不膨胀Iceberg/Paimon行存储 |
| Agent 运行位置 | **Coordinator 侧** | Agent 需要全局视图(catalog, session, tools) |
| 推理引擎策略 | **SPI 抽象 + 可插拔 Provider** | 支持云端(OpenAI/Anthropic)和本地(Ollama/vLLM) |
| MCP 实现方式 | **独立 Transport 层 in pangrova-protocol** | 复用已有 Flight/gRPC 基础设施 |
| 多模态索引 | **扩展 VectorIndex SPI 支持多模态** | 统一索引管理生命周期 |

### 8.2 设计原则

1. **SQL-First**: 所有 AI 能力必须有 SQL 表面，对用户透明
2. **SPI-Driven**: 所有外部依赖通过 SPI 抽象，避免硬编码模型/服务商
3. **存算分离**: AI 计算不绑定存储，支持弹性扩缩
4. **安全第一**: Token Budget + PII Guard + Audit Log 全链路安全
5. **渐进增强**: 每个 Phase 独立可交付，不阻塞后续 Phase

### 8.3 与现有架构的集成点

```
pangrova-spi        → 新增 MultimodalProvider, AgentProvider, MCPTransport SPI
pangrova-parser     → 扩展 FILE type, CREATE AGENT, SEMANTIC VIEW 语法
pangrova-planner    → 新增 AgentPlanNode, NaturalLanguageQueryNode
pangrova-coordinator → 新增 agent/ 子包 (Orchestrator, ToolCatalog)
pangrova-worker     → 扩展 ai/function (multimodal), ai/document (实现 parsers)
pangrova-protocol   → 新增 MCP transport layer
pangrova-observability → 新增 AI Trace/Span 模型
```

---

## 九、竞争力对标总结

| 能力维度 | Snowflake | Databricks | Pangrova (当前) | Pangrova (目标) |
|---------|-----------|------------|---------------|---------------|
| AI SQL 函数 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 向量搜索 | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 多模态处理 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| Agentic AI | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |
| AI 可观测性 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ | ⭐⭐⭐⭐ |
| 自优化能力 | ⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| MCP/工具协议 | ⭐⭐ | ⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |
| 语义层 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐ | ⭐⭐⭐⭐ |

---

## 十、Pangrova 差异化优势与战略定位

### 已有差异化优势（需保持领先）:

1. **自优化数据库**: Semantic Cache + Vector Index Lifecycle + Data Layout Advisor + Learned Cardinality — 竞品均无此深度
2. **读写分离架构**: C++ (Velox) 扫描 + Java DML 的独特架构，可在 Worker Native 层做推理加速
3. **Auto-Embedding DDL**: 写入时自动生成 embedding，竞品需手动管道
4. **Branch 数据沙箱**: 比 Lakebase Branch 更早实现，且集成到 Agent 会话

### 战略定位:

> **Pangrova = 自优化的 AI-Native Lakehouse Query Engine**
>
> - 不仅提供 AI 函数（WITH AI），更让数据库本身成为 AI（AS AI）
> - 不仅支持 Agent 调用（被动），更主动为 Agent 提供最优数据服务
> - 通过自优化减少人工运维，通过 Agent 协议成为 AI 应用的智能数据层

---

## 附录 A: 参考资料

1. [Databricks Data+AI Summit 2025 Announcements](https://www.databricks.com/events/dataaisummit-2025-announcements)
2. [Databricks Mosaic AI Announcements](https://www.databricks.com/blog/mosaic-ai-announcements-data-ai-summit-2025)
3. [Snowflake Summit 2025 Highlights](https://www.snowflake.com/en/blog/announcements-snowflake-summit-2025/)
4. [Inside Snowflake Intelligence: Five Pillars of Enterprise-Grade Agentic AI](https://www.snowflake.com/en/blog/engineering/inside-snowflake-intelligence-enterprise-agentic-ai/)
5. [Simplifying Multimodal Data Analysis with Snowflake Cortex AI](https://www.snowflake.com/en/blog/multimodal-data-analysis-cortex-ai/)
6. [Daft and Unity Catalog: A Multimodal/AI-Native Lakehouse](https://www.databricks.com/blog/whats-new-unity-catalog-data-ai-summit-2026)
