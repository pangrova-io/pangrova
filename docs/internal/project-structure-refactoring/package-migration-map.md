# 全部主源包的迁移台账

当前台账：2026-11-01，第52节Generation归并工作区。700 包 / 49 Java 叶子 / 3,624 主源文件 / 2,400 测试源文件；每个当前主源包只登记一次。统计包含 `package-info.java`，不含 `target` 生成源；测试源文件数不是测试用例数。位置与编译通过不代表逐文件语义验收完成。

初审为 649 包 / 3,540 文件，计划交付时为 649 包 / 3,542 文件，历史证据保留在审查记录。第 30 节新增 Query 事务 fence 包与 Artifact execution store 包；拆除 Metadata 原 fence 和 File 重复的租约端口。第 31 节迁入通用 build/exchange/context，新增 Security 当前 direct-role 事务读取端口；Metadata 保留领域 publication 和跨域读 adapter。下面按当前源码更新文件数和新增包，迁移状态以执行记录为准。

第 32 节新增纯资源制品 schema-migrations（不计入 Java 包台账），迁回 Table/Materialization 的 owner 测试；Kit 新增无 Metadata 依赖的迁移 fixture。

第 33 节将 AI 工厂迁入 assembly 并要求显式 authority，移除 AI Control 的 Metadata 生产/test-jar 依赖；跨域测试迁入 Integration，原断言保留。

第34节提取Coordination API，Worker session/resolver下沉后全图SCC由8降至7；包归属仍须继续语义验收。

第35节将模型当前权限实现迁入 AI Control，由 Security 事务端口显式接入 Metadata、Artifact 和生成写入；删除静态 authority 和单纯转发工厂。领域发布与激活装配仍待闭环。

第42节新增 exchange.codec 包，并归位 TaskState/TaskMemoryAccount；两组 Worker SCC 消失，剩五组成员不变。预算异常回归已取得仓库红4→绿13；第42节完整 Worker/static/Integration 验证已通过，不将台账“保留”视为完成验收。

[总体计划与批次定义](../project-structure-refactoring-plan.md)。目标包中的 `{a,b}` 表示按能力分包，不是字面 Java 包名；`<能力>` 由该行现有能力后缀确定；多目标项按总体计划中的类级规则迁移，禁止整包机械复制。短包名前缀与当前 owner 对齐；跨域目标均注明制品。

第47节将File拆成API/Core/JDBC三个真实叶子；6个存储实现与4组测试归JDBC，Security authority归Core，传输DTO归api.object；没有保留旧包兼容类。当批仍待迁移的Worker LOCAL/S3实现在第48节迁出；OSS FileSpace路径继续开放。

第48节将LOCAL/S3访问器、S3流与SDK工厂迁入各自Storage provider；File API新增真实provider端口，Worker仅选择并关闭provider。MinIO可变版本删除因不支持条件语义而显式拒绝；无Worker→SDK工厂签名泄漏。

动作含义：保留＝当前结构无充分理由移动，继续语义审查；迁移＝已有清晰 owner/落位；按类拆分＝当前包混合职责；改依赖＝位置可保留，必须反转调用边。各行列出目标动作；只有执行记录中具备迁移与验证证据的部分可以标记完成，其余仍为“待实施/验收”。

第49节按新的明确目标建立Filesystem api/local/s3/oss、Persistence api/jdbc和中央SPI。已迁入的包以实际源码为准；剩余所有store及领域通用API继续迁移，不能沿用旧台账的保留动作作为架构例外。见[持久化候选清单](persistence-migration-inventory.md)。

第49节按新的明确目标建立Filesystem api/local/s3/oss、Persistence api/jdbc和中央SPI。已迁入的包以实际源码为准；剩余所有store及领域通用API继续迁移，不能沿用旧台账的保留动作作为架构例外。见[持久化候选清单](persistence-migration-inventory.md)。

第49节按新的明确目标建立Filesystem api/local/s3/oss、Persistence api/jdbc和中央SPI。已迁入的包以实际源码为准；剩余所有store及领域通用API继续迁移，不能沿用旧台账的保留动作作为架构例外。见[持久化候选清单](persistence-migration-inventory.md)。

第49节按新的明确目标建立Filesystem api/local/s3/oss、Persistence api/jdbc和中央SPI。已迁入的包以实际源码为准；剩余所有store及领域通用API继续迁移，不能沿用旧台账的保留动作作为架构例外。见[持久化候选清单](persistence-migration-inventory.md)。

## pangrova-ai-control

路径：`pangrova-ai/pangrova-ai-control`。主源文件 40，主源包 9，本叶子测试源文件 7（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.ai.control](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control) | 1 | 保留 / P7 | `pangrova-ai-control`：`org.pangrova.ai.control` | 领域根说明；工厂已归 assembly，不承载运行实现 |
| [org.pangrova.ai.control.api](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/api) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 模型/evaluation/feature 权威和 Generation 同事务关联 |
| [org.pangrova.ai.control.assembly](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/assembly) | 2 | 保留 / P3 | `pangrova-ai-control`：`org.pangrova.ai.control.assembly` | 组合根显式传入真实事务 authority；无 Metadata 发现或缺省授权实现 |
| [org.pangrova.ai.control.embedding](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/embedding) | 11 | 按类拆分 / P7 | `pangrova-ai-control`：`org.pangrova.ai.control.api.model / core.model / store.memory.model` | registry 契约/值与 concurrent-map 实现分开 |
| [org.pangrova.ai.control.evaluation](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/evaluation) | 12 | 按类拆分 / P7 | `pangrova-ai-control`：`org.pangrova.ai.control.api.evaluation / core.evaluation` | 值和 store port 不依赖决策/校验实现 |
| [org.pangrova.ai.control.feature](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/feature) | 3 | 迁移 / P7 | `pangrova-ai-control`：`org.pangrova.ai.control.api.feature` | Feature/export 契约，与 JDBC 关联 adapter 分离 |
| [org.pangrova.ai.control.store.jdbc.embedding](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/embedding) | 3 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 模型/evaluation/feature 权威和 Generation 同事务关联；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.ai.control.store.jdbc.evaluation](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/evaluation) | 3 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 模型/evaluation/feature 权威和 Generation 同事务关联；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.ai.control.store.jdbc.feature](../../../pangrova-ai/pangrova-ai-control/src/main/java/org/pangrova/ai/control/store/jdbc/feature) | 3 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 模型/evaluation/feature 权威和 Generation 同事务关联；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-ai-worker

路径：`pangrova-ai/pangrova-ai-worker`。主源文件 93，主源包 17，本叶子测试源文件 53（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.ai.worker](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker) | 1 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.blob](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/blob) | 6 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.blob` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.config](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/config) | 2 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.config` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.context](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/context) | 10 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.context` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.embedding](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/embedding) | 3 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.embedding` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.embedding.provider.openai](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/embedding/provider/openai) | 2 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.embedding.provider.openai` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.function](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/function) | 20 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.function` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.function.file](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/function/file) | 4 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.function.file` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.governance](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/governance) | 4 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.governance` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.governance.observer](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/governance/observer) | 4 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.governance.observer` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.inference.provider.openai](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/inference/provider/openai) | 10 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.inference.provider.openai` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.invocation](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/invocation) | 15 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.invocation` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.provider](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/provider) | 4 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.provider` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.provider.env](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/provider/env) | 1 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.provider.env` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.provider.governed](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/provider/governed) | 4 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.provider.governed` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.rerank.provider.openai](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/rerank/provider/openai) | 1 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.rerank.provider.openai` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |
| [org.pangrova.ai.worker.text](../../../pangrova-ai/pangrova-ai-worker/src/main/java/org/pangrova/ai/worker/text) | 2 | 保留 / P8 | `pangrova-ai-worker`：`org.pangrova.ai.worker.text` | 受控 provider 调用、取消/超时、资源释放、ServiceLoader |

## pangrova-client-cli

路径：`pangrova-client/pangrova-client-cli`。主源文件 8，主源包 1，本叶子测试源文件 7（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.client.cli](../../../pangrova-client/pangrova-client-cli/src/main/java/org/pangrova/client/cli) | 8 | 按类拆分 / P8 | `pangrova-client-cli`：`org.pangrova.client.cli.command / input / render` | 按真实命令/输入/渲染职责拆；共享小入口保留 |

## pangrova-client-core

路径：`pangrova-client/pangrova-client-core`。主源文件 19，主源包 5，本叶子测试源文件 5（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.client.core](../../../pangrova-client/pangrova-client-core/src/main/java/org/pangrova/client/core) | 1 | 保留 / P8 | `pangrova-client-core`：`org.pangrova.client.core` | API-only classpath、错误/会话/结果契约 |
| [org.pangrova.client.core.api](../../../pangrova-client/pangrova-client-core/src/main/java/org/pangrova/client/core/api) | 4 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | API-only classpath、错误/会话/结果契约 |
| [org.pangrova.client.core.error](../../../pangrova-client/pangrova-client-core/src/main/java/org/pangrova/client/core/error) | 5 | 保留 / P8 | `pangrova-client-core`：`org.pangrova.client.core.error` | API-only classpath、错误/会话/结果契约 |
| [org.pangrova.client.core.protocol](../../../pangrova-client/pangrova-client-core/src/main/java/org/pangrova/client/core/protocol) | 7 | 保留 / P8 | `pangrova-client-core`：`org.pangrova.client.core.protocol` | API-only classpath、错误/会话/结果契约 |
| [org.pangrova.client.core.session](../../../pangrova-client/pangrova-client-core/src/main/java/org/pangrova/client/core/session) | 2 | 保留 / P8 | `pangrova-client-core`：`org.pangrova.client.core.session` | API-only classpath、错误/会话/结果契约 |

## pangrova-client-jdbc

路径：`pangrova-client/pangrova-client-jdbc`。主源文件 17，主源包 2，本叶子测试源文件 24（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.client.jdbc](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc) | 16 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | Driver 稳定入口保留；同包协作按状态所有权分，不机械逐类建包；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.client.jdbc.value](../../../pangrova-client/pangrova-client-jdbc/src/main/java/org/pangrova/client/jdbc/value) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | Driver discovery、JDBC 状态机、ResultSet 和资源关闭；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-common

路径：`pangrova-common`。主源文件 76，主源包 21，本叶子测试源文件 38（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.common](../../../pangrova-common/src/main/java/org/pangrova/common) | 2 | 保留 / P1 | `pangrova-common`：`org.pangrova.common` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.activation](../../../pangrova-common/src/main/java/org/pangrova/common/activation) | 2 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.activation` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.annotation](../../../pangrova-common/src/main/java/org/pangrova/common/annotation) | 2 | 按类拆分 / P1 | `pangrova-common`：`原能力包；注解统一 org.pangrova.common.api.annotation` | 核对 retention、参数和 gate 后统一同义注解；并发工具留原包 |
| [org.pangrova.common.api.annotation](../../../pangrova-common/src/main/java/org/pangrova/common/api/annotation) | 4 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 核对 retention、参数和 gate 后统一同义注解；并发工具留原包 |
| [org.pangrova.common.bloom](../../../pangrova-common/src/main/java/org/pangrova/common/bloom) | 1 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.bloom` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.concurrent](../../../pangrova-common/src/main/java/org/pangrova/common/concurrent) | 9 | 按类拆分 / P1 | `pangrova-common`：`原能力包；注解统一 org.pangrova.common.api.annotation` | 核对 retention、参数和 gate 后统一同义注解；并发工具留原包 |
| [org.pangrova.common.exception](../../../pangrova-common/src/main/java/org/pangrova/common/exception) | 12 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.exception` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.file](../../../pangrova-common/src/main/java/org/pangrova/common/file) | 5 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.file` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.http](../../../pangrova-common/src/main/java/org/pangrova/common/http) | 6 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.http` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.http.adapter.jetty](../../../pangrova-common/src/main/java/org/pangrova/common/http/adapter/jetty) | 1 | 迁移 / P1 | `pangrova-transport`：`org.pangrova.transport.server.http` | Jetty server adapter 迁出，共同 HTTP 值不强搬 |
| [org.pangrova.common.jdbc](../../../pangrova-common/src/main/java/org/pangrova/common/jdbc) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.common.json](../../../pangrova-common/src/main/java/org/pangrova/common/json) | 2 | 按类拆分 / P1 | `pangrova-serialization-json / pangrova-common`：`org.pangrova.serialization.json.jackson；JDK JsonEscaper 留原能力` | Jackson 实现与公开技术类型移出 Common；保持 canonical 配置 |
| [org.pangrova.common.json.provider.jackson](../../../pangrova-common/src/main/java/org/pangrova/common/json/provider/jackson) | 1 | 按类拆分 / P1 | `pangrova-serialization-json / pangrova-common`：`org.pangrova.serialization.json.jackson；JDK JsonEscaper 留原能力` | Jackson 实现与公开技术类型移出 Common；保持 canonical 配置 |
| [org.pangrova.common.lifecycle](../../../pangrova-common/src/main/java/org/pangrova/common/lifecycle) | 11 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.lifecycle` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.resilience](../../../pangrova-common/src/main/java/org/pangrova/common/resilience) | 2 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.resilience` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.resilience.api](../../../pangrova-common/src/main/java/org/pangrova/common/resilience/api) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | QueryCheckpoint/CheckpointStore 属 Query 恢复契约 |
| [org.pangrova.common.resilience.policy](../../../pangrova-common/src/main/java/org/pangrova/common/resilience/policy) | 4 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.resilience.policy` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.session](../../../pangrova-common/src/main/java/org/pangrova/common/session) | 1 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.session` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.storage](../../../pangrova-common/src/main/java/org/pangrova/common/storage) | 1 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.storage` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.util](../../../pangrova-common/src/main/java/org/pangrova/common/util) | 5 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.util` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |
| [org.pangrova.common.value](../../../pangrova-common/src/main/java/org/pangrova/common/value) | 2 | 保留 / P1 | `pangrova-common`：`org.pangrova.common.value` | 公共原语最小 classpath；第三方实现移出；共享值语义与 canonical bytes |

## pangrova-config

路径：`pangrova-config`。主源文件 15，主源包 1，本叶子测试源文件 12（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.config](../../../pangrova-config/src/main/java/org/pangrova/config) | 15 | 按类拆分 / P1 | `pangrova-config / pangrova-coordination-core / pangrova-worker-core`：`org.pangrova.config.api / validation / adapter.yaml；进程 assembly.persistence` | 配置模型/校验/加载分离；StateStoreFactory 注册归启动实例 |

## pangrova-connector-iceberg

路径：`pangrova-connector/pangrova-connector-iceberg`。主源文件 97，主源包 9，本叶子测试源文件 69（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.connector.iceberg](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg) | 53 | 按类拆分 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg；子能力 catalog/read/write/handle/schema` | 根入口保留，SDK 实现按生命周期分能力；先看最近调用者 |
| [org.pangrova.connector.iceberg.fs](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/fs) | 3 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.fs` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.handle](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/handle) | 16 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.handle` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.index](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/index) | 2 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.index` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.read](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/read) | 5 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.read` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.schema](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/schema) | 1 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.schema` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.system](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/system) | 4 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.system` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.type](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/type) | 1 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.type` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.iceberg.write](../../../pangrova-connector/pangrova-connector-iceberg/src/main/java/org/pangrova/connector/iceberg/write) | 12 | 保留 / P8 | `pangrova-connector-iceberg`：`org.pangrova.connector.iceberg.write` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |

## pangrova-connector-lance

路径：`pangrova-connector/pangrova-connector-lance`。主源文件 16，主源包 1，本叶子测试源文件 22（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.connector.lance](../../../pangrova-connector/pangrova-connector-lance/src/main/java/org/pangrova/connector/lance) | 16 | 按类拆分 / P8 | `pangrova-connector-lance`：`org.pangrova.connector.lance；子能力 catalog/read/write/handle/schema` | 根入口保留，SDK 实现按生命周期分能力；先看最近调用者 |

## pangrova-connector-paimon

路径：`pangrova-connector/pangrova-connector-paimon`。主源文件 86，主源包 10，本叶子测试源文件 50（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.connector.paimon](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon) | 30 | 按类拆分 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon；子能力 catalog/read/write/handle/schema` | 根入口保留，SDK 实现按生命周期分能力；先看最近调用者 |
| [org.pangrova.connector.paimon.fs](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/fs) | 2 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.fs` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.handle](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/handle) | 19 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.handle` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.index](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/index) | 2 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.index` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.read](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/read) | 2 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.read` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.scan](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/scan) | 5 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.scan` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.schema](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/schema) | 4 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.schema` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.system](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/system) | 8 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.system` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.type](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/type) | 3 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.type` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |
| [org.pangrova.connector.paimon.write](../../../pangrova-connector/pangrova-connector-paimon/src/main/java/org/pangrova/connector/paimon/write) | 11 | 保留 / P8 | `pangrova-connector-paimon`：`org.pangrova.connector.paimon.write` | 真实 SDK、snapshot/handle、读写提交、并发/失败恢复 |

## pangrova-connector-runtime

路径：`pangrova-connector/pangrova-connector-runtime`。主源文件 7，主源包 4，本叶子测试源文件 6（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.connector.runtime](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime) | 1 | 保留 / P8 | `pangrova-connector-runtime`：`org.pangrova.connector.runtime` | 类加载隔离、唯一 provider、发现/关闭 |
| [org.pangrova.connector.runtime.classloader](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime/classloader) | 4 | 保留 / P8 | `pangrova-connector-runtime`：`org.pangrova.connector.runtime.classloader` | 类加载隔离、唯一 provider、发现/关闭 |
| [org.pangrova.connector.runtime.discovery](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime/discovery) | 1 | 保留 / P8 | `pangrova-connector-runtime`：`org.pangrova.connector.runtime.discovery` | 类加载隔离、唯一 provider、发现/关闭 |
| [org.pangrova.connector.runtime.lifecycle](../../../pangrova-connector/pangrova-connector-runtime/src/main/java/org/pangrova/connector/runtime/lifecycle) | 1 | 保留 / P8 | `pangrova-connector-runtime`：`org.pangrova.connector.runtime.lifecycle` | 类加载隔离、唯一 provider、发现/关闭 |

## pangrova-coordination-core

路径：`pangrova-coordination/pangrova-coordination-core`。主源文件 579，主源包 96，本叶子测试源文件 577（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.coordinator](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator) | 2 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.adapter.indexing](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing) | 9 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.indexing.ai.lifecycle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing/ai/lifecycle) | 4 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.indexing.ddl](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing/ddl) | 11 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.indexing.filter](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing/filter) | 9 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.indexing.lifecycle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing/lifecycle) | 13 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.indexing.lifecycle.gc](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/indexing/lifecycle/gc) | 3 | 按类拆分 / P5 | `pangrova-indexing-core / pangrova-query / pangrova-coordination-core`：`org.pangrova.indexing.core.<能力>；query.adapter.indexing；coordinator.adapter.indexing` | 迁 build/lifecycle/GC/领域 DDL，保留必要进程组合；先消回调反向边 |
| [org.pangrova.coordinator.adapter.scheduler](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/adapter/scheduler) | 6 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.scheduler` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.ai.branch](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/ai/branch) | 4 | 按类拆分 / P5 | `pangrova-ai-control / pangrova-query`：`org.pangrova.ai.control.core.<能力> / adapter.generation；query.adapter.ai` | 模型/Feature 权威归 AI Control，SQL 编排归 Query adapter |
| [org.pangrova.coordinator.ai.embedding](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/ai/embedding) | 9 | 按类拆分 / P5 | `pangrova-ai-control / pangrova-query`：`org.pangrova.ai.control.core.<能力> / adapter.generation；query.adapter.ai` | 模型/Feature 权威归 AI Control，SQL 编排归 Query adapter |
| [org.pangrova.coordinator.ai.layout](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/ai/layout) | 1 | 按类拆分 / P5 | `pangrova-ai-control / pangrova-query`：`org.pangrova.ai.control.core.<能力> / adapter.generation；query.adapter.ai` | 模型/Feature 权威归 AI Control，SQL 编排归 Query adapter |
| [org.pangrova.coordinator.ai.model](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/ai/model) | 1 | 按类拆分 / P5 | `pangrova-ai-control / pangrova-query`：`org.pangrova.ai.control.core.<能力> / adapter.generation；query.adapter.ai` | 模型/Feature 权威归 AI Control，SQL 编排归 Query adapter |
| [org.pangrova.coordinator.bootstrap](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap) | 33 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.assembly` | 抽 assembly module contract；phase 返回 owned resources，去反向容器访问 |
| [org.pangrova.coordinator.bootstrap.lifecycle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/lifecycle) | 5 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.assembly.lifecycle` | 抽 assembly module contract；phase 返回 owned resources，去反向容器访问 |
| [org.pangrova.coordinator.bootstrap.module](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/bootstrap/module) | 7 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.assembly.module` | 抽 assembly module contract；phase 返回 owned resources，去反向容器访问 |
| [org.pangrova.coordinator.cache](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/cache) | 1 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.cache` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.clustering](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/clustering) | 9 | 按类拆分 / P5 | `pangrova-table / pangrova-query`：`org.pangrova.table.core.maintenance；query.adapter.maintenance` | 维护事实归 Table，任务执行编排由 Query adapter |
| [org.pangrova.coordinator.coordination](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/coordination) | 9 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.coordination` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.dynamictable](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/dynamictable) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.adapter.materialization` | 实现 Materialization port；kernel 不依赖 Query/Coordinator 实现 |
| [org.pangrova.coordinator.embedding](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/embedding) | 1 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.embedding` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.feature](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/feature) | 6 | 按类拆分 / P5 | `pangrova-ai-control / pangrova-query`：`org.pangrova.ai.control.core.<能力> / adapter.generation；query.adapter.ai` | 模型/Feature 权威归 AI Control，SQL 编排归 Query adapter |
| [org.pangrova.coordinator.file](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/file) | 1 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.file` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.generation](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/generation) | 15 | 按类拆分 / P5 | `pangrova-generation-core / pangrova-query`：`org.pangrova.generation.core.execution；query.adapter.generation` | 通用图/模板归 Generation；绑定与 Worker dispatch 归 Query adapter |
| [org.pangrova.coordinator.generation.runtime](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/generation/runtime) | 1 | 按类拆分 / P5 | `pangrova-generation-core / pangrova-query`：`org.pangrova.generation.core.execution；query.adapter.generation` | 通用图/模板归 Generation；绑定与 Worker dispatch 归 Query adapter |
| [org.pangrova.coordinator.generation.template](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/generation/template) | 4 | 按类拆分 / P5 | `pangrova-generation-core / pangrova-query`：`org.pangrova.generation.core.execution；query.adapter.generation` | 通用图/模板归 Generation；绑定与 Worker dispatch 归 Query adapter |
| [org.pangrova.coordinator.governance](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/governance) | 1 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.governance` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.http](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http) | 5 | 按类拆分 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.server.http / adapter.http` | server 只装配 endpoint；endpoint 不依赖完整 HttpServerContext |
| [org.pangrova.coordinator.http.endpoint](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/endpoint) | 16 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.endpoint` | 入口映射留进程，只消费能力 API 与 HTTP primitives |
| [org.pangrova.coordinator.http.endpoint.catalog](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/endpoint/catalog) | 11 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.endpoint.catalog` | 入口映射留进程，只消费能力 API 与 HTTP primitives |
| [org.pangrova.coordinator.http.endpoint.copy](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/endpoint/copy) | 3 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.endpoint.copy` | 入口映射留进程，只消费能力 API 与 HTTP primitives |
| [org.pangrova.coordinator.http.endpoint.file](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/endpoint/file) | 1 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.endpoint.file` | 入口映射留进程，只消费能力 API 与 HTTP primitives |
| [org.pangrova.coordinator.http.jdbc](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/http/jdbc) | 7 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | JDBC-over-HTTP 入口留应用；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.coordinator.maintenance](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/maintenance) | 2 | 按类拆分 / P5 | `pangrova-table / pangrova-query`：`org.pangrova.table.core.maintenance；query.adapter.maintenance` | 维护事实归 Table，任务执行编排由 Query adapter |
| [org.pangrova.coordinator.materialization](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/materialization) | 20 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.adapter.materialization` | 实现 Materialization port；kernel 不依赖 Query/Coordinator 实现 |
| [org.pangrova.coordinator.metadata](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/metadata) | 9 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.metadata` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.metadata.nativecontrol](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/metadata/nativecontrol) | 2 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.metadata.nativecontrol` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.metadata.read](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/metadata/read) | 9 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.metadata.read` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.query](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query) | 12 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.admission](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/admission) | 1 | 按类拆分 / P5 | `pangrova-scheduler / pangrova-query`：`org.pangrova.scheduler.core.admission；query.adapter.admission` | 通用资源准入和 Query identity adapter 分离 |
| [org.pangrova.coordinator.query.audit](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/audit) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.audit` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.cache](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/cache) | 4 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.cache` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution) | 20 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch) | 19 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.adaptive](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/adaptive) | 6 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.adaptive` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag) | 14 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.binding](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/binding) | 6 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.binding` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.control](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/control) | 7 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.control` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.handler](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/handler) | 15 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.handler` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.retrieval](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/retrieval) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.retrieval` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.runtime](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/runtime) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.runtime` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.dag.split](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/dag/split) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.dag.split` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.listener](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/listener) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.listener` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.shuffle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle) | 8 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.shuffle` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.shuffle.generation](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/generation) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.shuffle.generation` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.shuffle.lifecycle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/shuffle/lifecycle) | 2 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.shuffle.lifecycle` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task) | 29 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.branch](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/branch) | 4 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task.branch` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.catalog](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/catalog) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task.catalog` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.file](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/file) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task.file` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.semantic](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/semantic) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task.semantic` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.storage](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/storage) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.dispatch.task.storage` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.dispatch.task.stream](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/dispatch/task/stream) | 5 | 按类拆分 / P5 | `pangrova-query / pangrova-streaming`：`org.pangrova.query.adapter.streaming / core.execution` | Query 持有执行编排；stream 专属状态留 Streaming |
| [org.pangrova.coordinator.query.execution.phase](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/phase) | 8 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.phase` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.result](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/result) | 2 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.result` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.retrieval](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/retrieval) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.retrieval` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.security](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/security) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.security` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.execution.split](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/execution/split) | 13 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.execution.split` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.lifecycle](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/lifecycle) | 2 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.lifecycle` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.resourcegroup](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/resourcegroup) | 5 | 按类拆分 / P5 | `pangrova-scheduler / pangrova-query`：`org.pangrova.scheduler.core.admission；query.adapter.admission` | 通用资源准入和 Query identity adapter 分离 |
| [org.pangrova.coordinator.query.security](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/security) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.security` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.session](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/session) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.session` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.stage](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/stage) | 7 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.stage` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.state](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/state) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.state` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.statement](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/statement) | 13 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.statement` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.statement.dml](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/statement/dml) | 5 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.statement.dml` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.task](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/task) | 9 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.task` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.task.report](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/task/report) | 6 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.task.report` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.task.write](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/task/write) | 6 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.task.write` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.query.tenant](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/query/tenant) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.tenant` | 契约/值先归 Query API；Planner/Worker/领域调用经窄 SPI+adapter，不保留 Coordinator import |
| [org.pangrova.coordinator.resilience.breaker](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/breaker) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.breaker` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.checkpoint](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/checkpoint) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.checkpoint` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.config](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/config) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.config` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.degradation](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/degradation) | 3 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.degradation` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.idempotency](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/idempotency) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.idempotency` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.lineage](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/lineage) | 1 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.lineage` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.provider.resilience4j](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/provider/resilience4j) | 2 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.provider.resilience4j` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.resilience.retry](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/resilience/retry) | 2 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.core.resilience.retry` | Query 恢复/重试/幂等归 owner；Resilience4j 实现归 Query adapter |
| [org.pangrova.coordinator.result](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/result) | 4 | 迁移 / P5 | `pangrova-query`：`org.pangrova.query.adapter.result` | 结果路由/签发保持 Query 生命周期，网络类型由 adapter 隔离 |
| [org.pangrova.coordinator.scheduler](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/scheduler) | 6 | 迁移 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.scheduler` | 通用 placement/recovery/speculation；SQL DAG 语义留 Query |
| [org.pangrova.coordinator.scheduler.ft](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/scheduler/ft) | 1 | 迁移 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.scheduler.ft` | 通用 placement/recovery/speculation；SQL DAG 语义留 Query |
| [org.pangrova.coordinator.semantic](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/semantic) | 1 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.semantic` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.speculation](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/speculation) | 1 | 迁移 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.speculation` | 通用 placement/recovery/speculation；SQL DAG 语义留 Query |
| [org.pangrova.coordinator.storage](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/storage) | 1 | 迁移 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.adapter.storage` | 进程只保留调用 owner API 的入口/组合；发现权威逻辑归还对应 owner |
| [org.pangrova.coordinator.task](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/task) | 11 | 按类拆分 / P5 | `pangrova-query / pangrova-query-api`：`org.pangrova.query.core.task / api.execution` | issuance/报告归 Query；Worker transport 实现留进程 adapter |
| [org.pangrova.coordinator.transport](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/transport) | 2 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.transport` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |
| [org.pangrova.coordinator.worker](../../../pangrova-coordination/pangrova-coordination-core/src/main/java/org/pangrova/coordinator/worker) | 7 | 保留 / P5 | `pangrova-coordination-core`：`org.pangrova.coordinator.worker` | 按能力迁出；同一状态机/事务不拆开，核对真实装配和下游 |

## pangrova-coordination-zookeeper

路径：`pangrova-coordination/pangrova-coordination-zookeeper`。主源文件 4，主源包 1，本叶子测试源文件 3（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.coordination.zookeeper](../../../pangrova-coordination/pangrova-coordination-zookeeper/src/main/java/org/pangrova/coordination/zookeeper) | 4 | 保留 / P5 | `pangrova-coordination-zookeeper`：`org.pangrova.coordination.zookeeper` | 保留包；POM 改依赖 coordination-api，lease/registry 通过公开契约 |

## pangrova-file-core

路径：`pangrova-file/pangrova-file-core`。主源文件 6，主源包 4，本叶子测试源文件 0（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.file.adapter.worker](../../../pangrova-file/pangrova-file-core/src/main/java/org/pangrova/file/adapter/worker) | 2 | 已拆分 / 第47节，继续语义验收 | `pangrova-file-core`：`org.pangrova.file.adapter.worker` | 授权与URL服务、目录刷新编排、Worker HTTP adapter及依赖Security的Internal authority SPI；禁止依赖JDBC store。 |
| [org.pangrova.file.core](../../../pangrova-file/pangrova-file-core/src/main/java/org/pangrova/file/core) | 1 | 已拆分 / 第47节，继续语义验收 | `pangrova-file-core`：`org.pangrova.file.core` | 授权与URL服务、目录刷新编排、Worker HTTP adapter及依赖Security的Internal authority SPI；禁止依赖JDBC store。 |
| [org.pangrova.file.core.directory](../../../pangrova-file/pangrova-file-core/src/main/java/org/pangrova/file/core/directory) | 1 | 已拆分 / 第47节，继续语义验收 | `pangrova-file-core`：`org.pangrova.file.core.directory` | 授权与URL服务、目录刷新编排、Worker HTTP adapter及依赖Security的Internal authority SPI；禁止依赖JDBC store。 |
| [org.pangrova.file.core.space](../../../pangrova-file/pangrova-file-core/src/main/java/org/pangrova/file/core/space) | 2 | 已拆分 / 第47节，继续语义验收 | `pangrova-file-core`：`org.pangrova.file.core.space` | 授权与URL服务、目录刷新编排、Worker HTTP adapter及依赖Security的Internal authority SPI；禁止依赖JDBC store。 |

## pangrova-filesystem-api

路径：`pangrova-filesystem/pangrova-filesystem-api`。主源文件 15，主源包 4，本叶子测试源文件 4（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.filesystem.api](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-api`：`org.pangrova.filesystem.api` | 物理I/O、资源所有权与错误语义 |
| [org.pangrova.filesystem.api.blob](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api/blob) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-api`：`org.pangrova.filesystem.api.blob` | 物理I/O、资源所有权与错误语义 |
| [org.pangrova.filesystem.api.object](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api/object) | 8 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-api`：`org.pangrova.filesystem.api.object` | 物理I/O、资源所有权与错误语义 |
| [org.pangrova.filesystem.api.profile](../../../pangrova-filesystem/pangrova-filesystem-api/src/main/java/org/pangrova/filesystem/api/profile) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-api`：`org.pangrova.filesystem.api.profile` | 物理I/O、资源所有权与错误语义 |

## pangrova-filesystem-local

路径：`pangrova-filesystem/pangrova-filesystem-local`。主源文件 7，主源包 2，本叶子测试源文件 6（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.filesystem.local.blob](../../../pangrova-filesystem/pangrova-filesystem-local/src/main/java/org/pangrova/filesystem/local/blob) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-local`：`org.pangrova.filesystem.local.blob` | 物理I/O、资源所有权与错误语义 |
| [org.pangrova.filesystem.local.object](../../../pangrova-filesystem/pangrova-filesystem-local/src/main/java/org/pangrova/filesystem/local/object) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-local`：`org.pangrova.filesystem.local.object` | 物理I/O、资源所有权与错误语义 |

## pangrova-filesystem-oss

路径：`pangrova-filesystem/pangrova-filesystem-oss`。主源文件 6，主源包 1，本叶子测试源文件 3（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.filesystem.oss.blob](../../../pangrova-filesystem/pangrova-filesystem-oss/src/main/java/org/pangrova/filesystem/oss/blob) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-oss`：`org.pangrova.filesystem.oss.blob` | 物理I/O、资源所有权与错误语义 |

## pangrova-filesystem-s3

路径：`pangrova-filesystem/pangrova-filesystem-s3`。主源文件 8，主源包 2，本叶子测试源文件 11（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.filesystem.s3.blob](../../../pangrova-filesystem/pangrova-filesystem-s3/src/main/java/org/pangrova/filesystem/s3/blob) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-s3`：`org.pangrova.filesystem.s3.blob` | 物理I/O、资源所有权与错误语义 |
| [org.pangrova.filesystem.s3.object](../../../pangrova-filesystem/pangrova-filesystem-s3/src/main/java/org/pangrova/filesystem/s3/object) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-filesystem-s3`：`org.pangrova.filesystem.s3.object` | 物理I/O、资源所有权与错误语义 |

## pangrova-generation-core

路径：`pangrova-generation/pangrova-generation-core`。主源文件 10，主源包 3，本叶子测试源文件 6（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.generation.assembly](../../../pangrova-generation/pangrova-generation-core/src/main/java/org/pangrova/generation/assembly) | 4 | 保留 / P3 | `pangrova-generation-core`：`org.pangrova.generation.assembly` | authority→anchor→job 锁序；tenant/job、canonical bytes、原子发布 |
| [org.pangrova.generation.core.publication](../../../pangrova-generation/pangrova-generation-core/src/main/java/org/pangrova/generation/core/publication) | 2 | 保留 / P3 | `pangrova-generation-core`：`org.pangrova.generation.core.publication` | authority→anchor→job 锁序；tenant/job、canonical bytes、原子发布 |
| [org.pangrova.generation.protocol](../../../pangrova-generation/pangrova-generation-core/src/main/java/org/pangrova/generation/protocol) | 4 | 保留 / P3 | `pangrova-generation-core`：`org.pangrova.generation.protocol` | authority→anchor→job 锁序；tenant/job、canonical bytes、原子发布 |

## pangrova-governance

路径：`pangrova-governance`。主源文件 62，主源包 16，本叶子测试源文件 21（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.governance](../../../pangrova-governance/src/main/java/org/pangrova/governance) | 1 | 保留 / P7 | `pangrova-governance`：`org.pangrova.governance` | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.api.audit](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/audit) | 3 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.api.catalog](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/catalog) | 1 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.api.classification](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/classification) | 4 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.api.lineage](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/lineage) | 5 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.api.quality](../../../pangrova-governance/src/main/java/org/pangrova/governance/api/quality) | 15 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.core](../../../pangrova-governance/src/main/java/org/pangrova/governance/core) | 2 | 按类拆分 / P7 | `pangrova-governance`：`org.pangrova.governance.assembly；各 core 能力` | GovernanceDeps/Context 只在组合入口，防服务容器扩散 |
| [org.pangrova.governance.core.audit](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/audit) | 1 | 保留 / P7 | `pangrova-governance`：`org.pangrova.governance.core.audit` | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.core.classification](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/classification) | 7 | 保留 / P7 | `pangrova-governance`：`org.pangrova.governance.core.classification` | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.core.lineage](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/lineage) | 6 | 保留 / P7 | `pangrova-governance`：`org.pangrova.governance.core.lineage` | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.core.quality](../../../pangrova-governance/src/main/java/org/pangrova/governance/core/quality) | 9 | 保留 / P7 | `pangrova-governance`：`org.pangrova.governance.core.quality` | quality gate 与 lineage 发布分离；分类/审计 owner |
| [org.pangrova.governance.lineage.provider.log](../../../pangrova-governance/src/main/java/org/pangrova/governance/lineage/provider/log) | 1 | 迁移 / P7 | `pangrova-governance`：`org.pangrova.governance.adapter.lineage.provider.log` | provider/executor adapter 消费 API；不暗中补缺省执行能力 |
| [org.pangrova.governance.lineage.provider.openlineage](../../../pangrova-governance/src/main/java/org/pangrova/governance/lineage/provider/openlineage) | 1 | 迁移 / P7 | `pangrova-governance`：`org.pangrova.governance.adapter.lineage.provider.openlineage` | provider/executor adapter 消费 API；不暗中补缺省执行能力 |
| [org.pangrova.governance.quality.executor.sql](../../../pangrova-governance/src/main/java/org/pangrova/governance/quality/executor/sql) | 1 | 迁移 / P7 | `pangrova-governance`：`org.pangrova.governance.adapter.quality.executor.sql` | provider/executor adapter 消费 API；不暗中补缺省执行能力 |
| [org.pangrova.governance.quality.executor.unconfigured](../../../pangrova-governance/src/main/java/org/pangrova/governance/quality/executor/unconfigured) | 1 | 迁移 / P7 | `pangrova-governance`：`org.pangrova.governance.adapter.quality.executor.unconfigured` | provider/executor adapter 消费 API；不暗中补缺省执行能力 |
| [org.pangrova.governance.store.jdbc](../../../pangrova-governance/src/main/java/org/pangrova/governance/store/jdbc) | 4 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | store 与 factory 分开，各能力唯一 writer；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-indexing-core

路径：`pangrova-indexing/pangrova-indexing-core`。主源文件 103，主源包 36，本叶子测试源文件 45（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.indexing](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.adapter.fs](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/adapter/fs) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.adapter.fs` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.assembly.serde](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/assembly/serde) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.assembly.serde` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.build](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/build) | 8 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.build` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.config](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/config) | 9 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.config` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.eval](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/eval) | 5 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.eval` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.eval.defaults](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/eval/defaults) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.eval.defaults` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.lifecycle](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/lifecycle) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.lifecycle` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.metrics](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/metrics) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.metrics` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.normalize](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/normalize) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.normalize` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.planner](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/planner) | 3 | 迁移 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.adapter.planner` | Planner 扩展隔离；wire 在统一序列化出口 |
| [org.pangrova.indexing.core.planner.vector](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/planner/vector) | 6 | 迁移 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.adapter.planner.vector` | Planner 扩展隔离；wire 在统一序列化出口 |
| [org.pangrova.indexing.core.registry](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/registry) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.registry` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.serde](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/serde) | 4 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.serde` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.core.storage](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/core/storage) | 5 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.core.storage` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.bitmap.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/bitmap/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.bitmap.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.bitmap.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/bitmap/serializer) | 3 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.bitmap.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.bloom.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/bloom/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.bloom.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.bloom.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/bloom/serializer) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.bloom.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.btree.evaluator](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/btree/evaluator) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.btree.evaluator` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.btree.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/btree/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.btree.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.btree.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/btree/serializer) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.btree.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.gin.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/gin/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.gin.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.gin.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/gin/serializer) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.gin.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.hash.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/hash/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.hash.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.hash.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/hash/serializer) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.hash.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.ngram.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/ngram/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.ngram.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.ngram.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/ngram/serializer) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.ngram.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.prefix.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/prefix/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.prefix.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.prefix.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/prefix/serializer) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.prefix.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.rtree.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/rtree/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.rtree.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.rtree.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/rtree/serializer) | 2 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.rtree.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.sparse.provider](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/sparse/provider) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.sparse.provider` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.engine.sparse.serializer](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/engine/sparse/serializer) | 1 | 保留 / P7 | `pangrova-indexing-core`：`org.pangrova.indexing.engine.sparse.serializer` | generation/publication/GC、唯一注册与关闭索引的行为 |
| [org.pangrova.indexing.store](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store) | 14 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 按跨域消费者区分 API 与内部 store port，拒绝整棵 store 导出；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.indexing.store.jdbc](../../../pangrova-indexing/pangrova-indexing-core/src/main/java/org/pangrova/indexing/store/jdbc) | 13 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 领域唯一 writer；通用 build 状态通过 Generation 同事务口；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-materialization-core

路径：`pangrova-materialization/pangrova-materialization-core`。主源文件 114，主源包 6，本叶子测试源文件 25（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.materialization](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization) | 1 | 保留 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization` | definition/generation/attempt、pin、refresh、GC 状态机 |
| [org.pangrova.materialization.domain](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/domain) | 46 | 按类拆分 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization.api.{definition,generation,attempt,publication,schedule,pin,gc}` | 按定义/执行/发布/调度/回收能力分组；避免 value 类型反向引用 service |
| [org.pangrova.materialization.extension](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/extension) | 10 | 保留 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization.extension` | definition/generation/attempt、pin、refresh、GC 状态机 |
| [org.pangrova.materialization.port](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/port) | 28 | 按类拆分 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization.spi.{definition,generation,attempt,publication,schedule,pin,gc}` | 按定义/执行/发布/调度/回收能力分组；避免 value 类型反向引用 service |
| [org.pangrova.materialization.strategy](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/strategy) | 4 | 迁移 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization.core.refresh.strategy` | 刷新策略内核，不拥有执行 adapter |
| [org.pangrova.materialization.usecase](../../../pangrova-materialization/pangrova-materialization-core/src/main/java/org/pangrova/materialization/usecase) | 25 | 按类拆分 / P7 | `pangrova-materialization-core`：`org.pangrova.materialization.core.{definition,generation,attempt,publication,schedule,pin,gc}` | 按定义/执行/发布/调度/回收能力分组；避免 value 类型反向引用 service |

## pangrova-materialization-store-jdbc

路径：`pangrova-materialization/pangrova-materialization-store-jdbc`。主源文件 10，主源包 1，本叶子测试源文件 1（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.materialization.store.jdbc](../../../pangrova-materialization/pangrova-materialization-store-jdbc/src/main/java/org/pangrova/materialization/store/jdbc) | 10 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 仓储对应能力；迁移 owner 测试，资源制品 P2 先行；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-metadata

路径：`pangrova-metadata`。主源文件 132，主源包 34，本叶子测试源文件 87（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.metadata](../../../pangrova-metadata/src/main/java/org/pangrova/metadata) | 1 | 按类拆分 / P7 | 各真实 owner 的模块说明；当前 `pangrova-metadata` 逐批归零退役 | 旧模块说明随真实职责迁移重写，不保留“所有元数据”权威或永久转发层 |
| [org.pangrova.metadata.adapter](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter) | 7 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.adapter.federation` | 联邦 schema/table 枚举、handle/factory 转换与缓存；连接器生命周期归 Catalog |
| [org.pangrova.metadata.adapter.generation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/generation) | 2 | 迁移 / P7 | 进程 PostgreSQL 事务适配：`org.pangrova.coordinator.adapter.generation.postgresql` | 组合 tenant/table/model 当前授权与 Query publication-owner 读投影；依赖窄 transaction SPI，复用调用者 Connection，不复制领域 SQL |
| [org.pangrova.metadata.adapter.governance](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/governance) | 2 | 按类拆分 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.adapter.governance`；wiring 归进程组合根 | 按类拆 quality-rule adapter 与实例装配；仅将名称解析为 stable Table identity，规则权威仍在 Governance/Table |
| [org.pangrova.metadata.adapter.security](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/adapter/security) | 1 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.adapter.security` | 列掩码适配只负责名称到 stable Table identity；Security/Table 继续拥有策略与绑定权威 |
| [org.pangrova.metadata.api.cache](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/cache) | 6 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 缓存、失效进度与监听契约归 Catalog；接口不能绑定 PostgreSQL provider |
| [org.pangrova.metadata.api.catalog](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/catalog) | 8 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 定义/incarnation/revision/handle 归 Catalog；MetadataStore 拆 current/history、技术库存、sync ports，删除默认 history-empty 能力假象 |
| [org.pangrova.metadata.api.object](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/object) | 12 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 技术表/列/资产 read model；Model/MV/Function/Volume DTO 不构成第二套领域写入权威，按真实调用保留 |
| [org.pangrova.metadata.api.relation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/api/relation) | 7 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 技术对象依赖图契约；领域对象实际删除经各 owner adapter，不能把维护 hook 当作已接线级联事务 |
| [org.pangrova.metadata.assembly.generation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/generation) | 1 | 迁移 / P7 | `pangrova-coordination-core`：进程 `bootstrap.generation` / `adapter.generation.postgresql` | GenerationStores 的真实 provider 选择、创建和跨域注入归进程；公共任务工厂不反向创建 Metadata/AI/Index provider |
| [org.pangrova.metadata.assembly.security](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/assembly/security) | 2 | 迁移 / P7 | `pangrova-coordination-core`：进程 `bootstrap.security` | ColumnMaskStoreWiring 与包说明归组合根；Catalog 只保留名称解析 adapter，真实 Security/Table providers 显式注入 |
| [org.pangrova.metadata.checkpoint](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/checkpoint) | 1 | 迁移 / P7 | Coordinator resilience PostgreSQL provider：`org.pangrova.coordinator.store.postgresql.checkpoint` | 实现 Common CheckpointStore；当前无生产构造点，实际恢复流程在 Coordinator；先验证 durable wiring，不归 Catalog 或凭名称搬到 Query |
| [org.pangrova.metadata.core.cache](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/cache) | 4 | 按类拆分 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.core.cache / provider.memory.cache` | Catalog tiered cache/metrics；Memory 与 disabled 后端显式选择，受控表 cache hit 仍验证当前 Table identity |
| [org.pangrova.metadata.core.catalog.handle.connector](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/catalog/handle/connector) | 2 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.adapter.connector` | 连接器实例与 CatalogHandle 绑定；工厂接口保留注入方向，注册失败时清理实际 handle |
| [org.pangrova.metadata.core.catalog.handle.federation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/catalog/handle/federation) | 2 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.adapter.federation` | 联邦 catalog handle/factory 生命周期；不让 Catalog core 反向依赖旧 Metadata adapter |
| [org.pangrova.metadata.core.catalog.handle.internal](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/catalog/handle/internal) | 1 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.core.handle.internal` | Internal CatalogHandle 能力与生命周期归 Catalog，不能因此取得 Table/AI/Index 权威 |
| [org.pangrova.metadata.core.catalog.registry](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/catalog/registry) | 2 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.core.registry` | 注册/注销/对账/健康和部署身份；显式注入 Table providers，去除默认 Memory 构造；保持 incarnation/revision fencing 与失败补偿 |
| [org.pangrova.metadata.core.event](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/event) | 3 | 按类拆分 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.api.event / core.event` | 拆窄生命周期 port 或同批迁技术事件 DTO，避免 Catalog↔Metadata 环；观测事件不替代领域提交真值 |
| [org.pangrova.metadata.core.object](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/object) | 1 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.protocol.object` | TableMetadata 技术投影序列化归 Catalog；已有编码与字段兼容不变 |
| [org.pangrova.metadata.core.outbox](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/outbox) | 4 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.core.observation / core.sync` | 联邦同步、drift/外部版本观测和 metrics；名称不代表可靠通用 outbox，实际 invalidation 与 owner mutation 同事务 |
| [org.pangrova.metadata.core.relation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/relation) | 3 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.core.relation` | 依赖 key/order/service 与真实 provider 分离；当前 service 无生产构造证据，不宣称已完成领域级联删除 |
| [org.pangrova.metadata.core.schema](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/core/schema) | 4 | 按类拆分 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.api.schema / core.schema` | Schema change/result/strictness 合同与演进校验分开；物理表 publication 仍由 Table/Connector owner |
| [org.pangrova.metadata.exception](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/exception) | 7 | 按类拆分 / P7 | `pangrova-catalog-core` 的 `org.pangrova.catalog.api.error`；Indexing API；各 owner storage error | IndexAlreadyExistsException 归 Indexing；MetastoreException 按 owner 替换，解除 Indexing→Metadata；保持原因/错误语义 |
| [org.pangrova.metadata.exception.catalog](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/exception/catalog) | 6 | 迁移 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.api.error` | Catalog 公开错误归定义/生命周期能力，不依赖 storage implementation |
| [org.pangrova.metadata.generation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation) | 5 | 按类拆分 / P7 | `pangrova-ai-control`：`org.pangrova.ai.control.api.generated / spi.generated` | 5 个生成值 descriptor/submission/generation-reference 合同归 AI；调用者依赖无 SQL port，不引入 Metadata |
| [org.pangrova.metadata.generation.jdbc](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc) | 7 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 3 个公共 job/digest/output validator 按纯逻辑与 SQL 拆；Index/Generated 发布归领域；Tabular 不因 Feature caller 归 AI；领域指针与 job 终态同连接原子提交；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.generation.jdbc.submission](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/generation/jdbc/submission) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 生成值提交、全部任务/descriptor 写入与 COMMITTED redo acknowledgement 同事务；通过 borrowed-connection port 替代具体 JDBC static helper；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.provider.memory.relation](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/provider/memory/relation) | 1 | 迁移 / P7 | Catalog Memory dependency provider：`org.pangrova.catalog.provider.memory.relation` | 真实 InMemoryObjectDependencyStore 与 PG 共享契约测试；不据此宣称完整 durable Catalog Memory 后端 |
| [org.pangrova.metadata.service](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/service) | 16 | 按类拆分 / P7 | `pangrova-catalog-core`：`org.pangrova.catalog.api.resolution / core.resolution / core.lifecycle / adapter.{security,governance,connector}`；同事务 adapter 归 PG 基础设施 | 16 类按专项设计逐项归位；技术解析/query begin-cleanup 归 Catalog；名称→stable identity→策略适配不复制领域权威；同连接 catalog/table 校验保留 |
| [org.pangrova.metadata.source.jdbc](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/source/jdbc) | 5 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 5 个 managed-source head/schema/publication/object/cleanup 实现归 Table；先抽 Catalog current-deployment 与 Query journal 事务 port，禁止 Table 引入 Metadata/Query runtime；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.store.jdbc.cache](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/cache) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | JdbcInvalidationPoller 负责 durable log 读取/清理；定义/库存 writer 的 invalidation 必须保留共同事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.store.jdbc.catalog](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/catalog) | 5 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | JdbcMetadataStore 拆定义/历史、库存、同步 ports；pool/Flyway 归进程，EmbeddedDataSource 归测试，MetadataJsonMapper 按 owner 拆；SQL 迁移资源保持 canonical 位置/版本；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.store.jdbc.object](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/store/jdbc/object) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 真实 ObjectDependencyStore SQL provider；与 Memory 共用契约测试，不拥有各领域对象删除；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.metadata.tenant](../../../pangrova-metadata/src/main/java/org/pangrova/metadata/tenant) | 1 | 迁移 / P7 | Security PostgreSQL provider：`org.pangrova.security.store.postgresql.tenant` | 实际写 tenants/memberships；current tenant/membership 与 scope 验证归 Security，保留借用 Connection 的撤权一致性 |

## pangrova-observability-sdk

路径：`pangrova-observability/pangrova-observability-sdk`。主源文件 57，主源包 17，本叶子测试源文件 27（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.observability.sdk](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk) | 1 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.alerting](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/alerting) | 5 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.alerting` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.config](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/config) | 2 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.config` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.logging](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/logging) | 1 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.logging` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.logging.provider.mdc](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/logging/provider/mdc) | 5 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.logging.provider.mdc` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.logging.provider.otel](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/logging/provider/otel) | 1 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.logging.provider.otel` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.metrics](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/metrics) | 8 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.metrics` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.metrics.jvm](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/metrics/jvm) | 2 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.metrics.jvm` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.metrics.nativebridge](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/metrics/nativebridge) | 3 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.metrics.nativebridge` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.metrics.provider.otel](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/metrics/provider/otel) | 6 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.metrics.provider.otel` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.noop](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/noop) | 10 | 待验收 / P49 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.noop` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.observability.sdk.profiling](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/profiling) | 1 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.profiling` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.tracing](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/tracing) | 2 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.tracing` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.tracing.adapter.grpc](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/tracing/adapter/grpc) | 2 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.tracing.adapter.grpc` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.tracing.adapter.http](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/tracing/adapter/http) | 1 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.tracing.adapter.http` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.tracing.adapter.nativebridge](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/tracing/adapter/nativebridge) | 2 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.tracing.adapter.nativebridge` | provider 装配、上下文传播与关闭 |
| [org.pangrova.observability.sdk.tracing.provider.otel](../../../pangrova-observability/pangrova-observability-sdk/src/main/java/org/pangrova/observability/sdk/tracing/provider/otel) | 5 | 保留 / P1 | `pangrova-observability-sdk`：`org.pangrova.observability.sdk.tracing.provider.otel` | provider 装配、上下文传播与关闭 |

## pangrova-operations

路径：`pangrova-operations`。主源文件 44，主源包 8，本叶子测试源文件 11（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.operations](../../../pangrova-operations/src/main/java/org/pangrova/operations) | 2 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.config](../../../pangrova-operations/src/main/java/org/pangrova/operations/config) | 2 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.config` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.ddl](../../../pangrova-operations/src/main/java/org/pangrova/operations/ddl) | 3 | 按类拆分 / P8 | `pangrova-query-api / pangrova-query`：`org.pangrova.query.api.statement / core.statement；无真实连接项删除` | 先核对反射/注册/需求再迁或删；不能启用另一套 DDL 执行路径 |
| [org.pangrova.operations.diagnostics](../../../pangrova-operations/src/main/java/org/pangrova/operations/diagnostics) | 13 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.diagnostics` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.dr](../../../pangrova-operations/src/main/java/org/pangrova/operations/dr) | 2 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.dr` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.recovery](../../../pangrova-operations/src/main/java/org/pangrova/operations/recovery) | 3 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.recovery` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.scaling](../../../pangrova-operations/src/main/java/org/pangrova/operations/scaling) | 10 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.scaling` | 入口权限、实际装配、副作用、操作恢复 |
| [org.pangrova.operations.upgrade](../../../pangrova-operations/src/main/java/org/pangrova/operations/upgrade) | 9 | 保留 / P8 | `pangrova-operations`：`org.pangrova.operations.upgrade` | 入口权限、实际装配、副作用、操作恢复 |

## pangrova-parser

路径：`pangrova-parser`。主源文件 201，主源包 8，本叶子测试源文件 50（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.parser](../../../pangrova-parser/src/main/java/org/pangrova/parser) | 26 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast) | 46 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.expression](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/expression) | 25 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.expression` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.statement](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/statement) | 82 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.statement` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.statement.dml](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/statement/dml) | 5 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.statement.dml` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.statement.maintenance](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/statement/maintenance) | 5 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.statement.maintenance` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.statement.stream](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/statement/stream) | 6 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.statement.stream` | SQL AST 正反例、round-trip、内置类型归属 |
| [org.pangrova.parser.ast.type](../../../pangrova-parser/src/main/java/org/pangrova/parser/ast/type) | 6 | 保留 / P4 | `pangrova-parser`：`org.pangrova.parser.ast.type` | SQL AST 正反例、round-trip、内置类型归属 |

## pangrova-persistence-api

路径：`pangrova-persistence/pangrova-persistence-api`。主源文件 47，主源包 22，本叶子测试源文件 4（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.persistence.api](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.coordination](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/coordination) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.file.claim](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/claim) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.file.claim` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.file.directory](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/directory) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.file.directory` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.file.profile](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/profile) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.file.profile` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.file.space](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/file/space) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.file.space` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation.exchange](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/exchange) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation.exchange` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation.execution](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/execution) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation.execution` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation.job](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/job) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation.job` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation.publication](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/publication) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation.publication` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.generation.transaction.jdbc](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/generation/transaction/jdbc) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.generation.transaction.jdbc` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.query.audit](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/audit) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.query.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.query.coordination](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/coordination) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.query.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.query.lineage](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/lineage) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.query.lineage` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.query.state](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/state) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.query.state` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.query.transaction](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/query/transaction) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.query.transaction` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.security.audit](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/audit) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.security.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.security.mask](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/mask) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.security.mask` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.security.policy](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/policy) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.security.policy` | CurrentDirectRoleReader / CurrentModelAuthority 借用调用方事务；Security 实现当前角色读取，AI Control 实现模型当前态校验；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.persistence.api.security.rowfilter](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/rowfilter) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.security.rowfilter` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.api.security.tenant](../../../pangrova-persistence/pangrova-persistence-api/src/main/java/org/pangrova/persistence/api/security/tenant) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-api`：`org.pangrova.persistence.api.security.tenant` | 领域契约或持久化职责；事务与调用方装配边界 |

## pangrova-persistence-jdbc

路径：`pangrova-persistence/pangrova-persistence-jdbc`。主源文件 66，主源包 25，本叶子测试源文件 28（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.persistence.jdbc](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.coordination](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/coordination) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file.claim](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/claim) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file.claim` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file.copy](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/copy) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file.copy` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file.directory](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/directory) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file.directory` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file.profile](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/profile) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file.profile` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.file.space](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/file/space) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.file.space` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.generation.exchange](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/exchange) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.generation.exchange` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.generation.execution](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/execution) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.generation.execution` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.generation.job](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/job) | 14 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.generation.job` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.generation.query](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/generation/query) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.generation.query` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.query.coordination](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/coordination) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.query.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.query.event](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/event) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.query.event` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.query.lineage](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/lineage) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.query.lineage` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.query.state](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/state) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.query.state` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.query.transaction](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/query/transaction) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.query.transaction` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.audit](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/audit) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.codec](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/codec) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.codec` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.mask](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/mask) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.mask` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.policy](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/policy) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.policy` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.rowfilter](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/rowfilter) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.rowfilter` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.support](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/support) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.support` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.tenant](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/tenant) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.tenant` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.jdbc.security.transaction](../../../pangrova-persistence/pangrova-persistence-jdbc/src/main/java/org/pangrova/persistence/jdbc/security/transaction) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-jdbc`：`org.pangrova.persistence.jdbc.security.transaction` | 领域契约或持久化职责；事务与调用方装配边界 |

## pangrova-persistence-memory

路径：`pangrova-persistence/pangrova-persistence-memory`。主源文件 14，主源包 6，本叶子测试源文件 12（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.persistence.memory](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.memory.coordination](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/coordination) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.memory.query.audit](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/audit) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory.query.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.memory.query.coordination](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/coordination) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory.query.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.memory.query.state](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/state) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory.query.state` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.persistence.memory.query.transaction](../../../pangrova-persistence/pangrova-persistence-memory/src/main/java/org/pangrova/persistence/memory/query/transaction) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-persistence-memory`：`org.pangrova.persistence.memory.query.transaction` | 领域契约或持久化职责；事务与调用方装配边界 |

## pangrova-planner

路径：`pangrova-planner`。主源文件 281，主源包 35，本叶子测试源文件 179（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.planner](../../../pangrova-planner/src/main/java/org/pangrova/planner) | 40 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.api / compile / binding.table / plan` | 主计划 §4.2：PlanTable、PlannerException、PlanFragment、PlannedQuery 与编译驱动分别落位 |
| [org.pangrova.planner.adaptive](../../../pangrova-planner/src/main/java/org/pangrova/planner/adaptive) | 15 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.adaptive` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.advisor](../../../pangrova-planner/src/main/java/org/pangrova/planner/advisor) | 1 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.advisor` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.branch](../../../pangrova-planner/src/main/java/org/pangrova/planner/branch) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.branch` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.cache](../../../pangrova-planner/src/main/java/org/pangrova/planner/cache) | 12 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.cache` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.cost](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost) | 17 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.cost.model / plan.physical / optimization.cost` | cost config/估计与规则注册拆开，不把环整体迁包 |
| [org.pangrova.planner.cost.aggregate](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost/aggregate) | 1 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.cost.aggregate` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.cost.convention](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost/convention) | 2 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.plan.physical / plan.trait / plan.fragment；cost.model` | 节点/trait 不反向依赖 optimization；成本模型独立 |
| [org.pangrova.planner.cost.distribution](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost/distribution) | 6 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.plan.physical / plan.trait / plan.fragment；cost.model` | 节点/trait 不反向依赖 optimization；成本模型独立 |
| [org.pangrova.planner.cost.join](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost/join) | 11 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.plan.physical.join / optimization.rule.join` | 物理节点与生成它的优化规则分离 |
| [org.pangrova.planner.cost.sort](../../../pangrova-planner/src/main/java/org/pangrova/planner/cost/sort) | 1 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.plan.physical / plan.trait / plan.fragment；cost.model` | 节点/trait 不反向依赖 optimization；成本模型独立 |
| [org.pangrova.planner.declaration](../../../pangrova-planner/src/main/java/org/pangrova/planner/declaration) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.declaration` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.embedding](../../../pangrova-planner/src/main/java/org/pangrova/planner/embedding) | 7 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.embedding` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.exchange](../../../pangrova-planner/src/main/java/org/pangrova/planner/exchange) | 1 | 按类拆分 / P4 | `pangrova-planner`：`org.pangrova.planner.plan.physical / plan.trait / plan.fragment；cost.model` | 节点/trait 不反向依赖 optimization；成本模型独立 |
| [org.pangrova.planner.extension](../../../pangrova-planner/src/main/java/org/pangrova/planner/extension) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.extension` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.generation](../../../pangrova-planner/src/main/java/org/pangrova/planner/generation) | 7 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.generation` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.incremental](../../../pangrova-planner/src/main/java/org/pangrova/planner/incremental) | 4 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.incremental` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.lineage](../../../pangrova-planner/src/main/java/org/pangrova/planner/lineage) | 8 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.lineage` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.metadata](../../../pangrova-planner/src/main/java/org/pangrova/planner/metadata) | 2 | 迁移 / P4 | `pangrova-planner`：`org.pangrova.planner.binding.metadata` | 编译绑定和版本校验不引用门面根包 |
| [org.pangrova.planner.mv](../../../pangrova-planner/src/main/java/org/pangrova/planner/mv) | 6 | 迁移 / P4 | `pangrova-planner`：`org.pangrova.planner.materialization` | 现有物化重写能力；去 mv 模糊缩写，不迁入运行时 Materialization kernel |
| [org.pangrova.planner.optimization](../../../pangrova-planner/src/main/java/org/pangrova/planner/optimization) | 7 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.optimization` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.parse](../../../pangrova-planner/src/main/java/org/pangrova/planner/parse) | 8 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.parse` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.rerank](../../../pangrova-planner/src/main/java/org/pangrova/planner/rerank) | 5 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.rerank` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.retrieval](../../../pangrova-planner/src/main/java/org/pangrova/planner/retrieval) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.retrieval` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.rewrite](../../../pangrova-planner/src/main/java/org/pangrova/planner/rewrite) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.rewrite` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.rule](../../../pangrova-planner/src/main/java/org/pangrova/planner/rule) | 10 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.rule` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.runtimefilter](../../../pangrova-planner/src/main/java/org/pangrova/planner/runtimefilter) | 8 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.runtimefilter` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.security](../../../pangrova-planner/src/main/java/org/pangrova/planner/security) | 7 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.security` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.security.expression](../../../pangrova-planner/src/main/java/org/pangrova/planner/security/expression) | 2 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.security.expression` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.stability](../../../pangrova-planner/src/main/java/org/pangrova/planner/stability) | 6 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.stability` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.stats](../../../pangrova-planner/src/main/java/org/pangrova/planner/stats) | 11 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.stats` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.substrait](../../../pangrova-planner/src/main/java/org/pangrova/planner/substrait) | 33 | 迁移 / P4 | `pangrova-planner`：`org.pangrova.planner.adapter.substrait` | 唯一 wire 编码出口，core/plan 不引用 protocol DTO |
| [org.pangrova.planner.throttle](../../../pangrova-planner/src/main/java/org/pangrova/planner/throttle) | 9 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.throttle` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.type](../../../pangrova-planner/src/main/java/org/pangrova/planner/type) | 10 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.type` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |
| [org.pangrova.planner.validation](../../../pangrova-planner/src/main/java/org/pangrova/planner/validation) | 14 | 保留 / P4 | `pangrova-planner`：`org.pangrova.planner.validation` | 绑定身份、权限/重写次数、计划 golden 和 Calcite 不泄漏 |

## pangrova-protocol

路径：`pangrova-protocol`。主源文件 165，主源包 38，本叶子测试源文件 74（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.protocol](../../../pangrova-protocol/src/main/java/org/pangrova/protocol) | 1 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.batch](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/batch) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.batch` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.capability](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/capability) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.capability` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.capability.inventory](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/capability/inventory) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.capability.inventory` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.catalog](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/catalog) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.catalog` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.checksum](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/checksum) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.checksum` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.descriptor](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/descriptor) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.descriptor` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.execution](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/execution) | 10 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.execution` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.execution.identity](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/execution/identity) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.execution.identity` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.function](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/function) | 12 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.function` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.function.catalog](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/function/catalog) | 6 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.function.catalog` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.function.declaration](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/function/declaration) | 11 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.function.declaration` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.function.formula](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/function/formula) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.function.formula` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.function.selection](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/function/selection) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.function.selection` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.generation](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/generation) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.generation` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.index](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/index) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.index` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.metadata](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/metadata) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.metadata` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.resource](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/resource) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.resource` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.catalog](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/catalog) | 1 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.catalog` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.config](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/config) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.config` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.copy](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/copy) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.copy` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.data](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/data) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.data` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.filter](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/filter) | 1 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.filter` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.shuffle](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/shuffle) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.shuffle` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.task](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/task) | 14 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.task` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.task.convert](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/task/convert) | 1 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.task.convert` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.task.exchange](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/task/exchange) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.task.exchange` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.version](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/version) | 2 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.version` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.rest.worker](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/rest/worker) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.rest.worker` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.retrieval](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/retrieval) | 16 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.retrieval` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.source](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/source) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.source` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.substrait](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/substrait) | 10 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.substrait` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.substrait.generated](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/substrait/generated) | 1 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.substrait.generated` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.task](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/task) | 5 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.task` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.transport](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/transport) | 8 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.transport` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.type](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/type) | 3 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.type` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.validation](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/validation) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.validation` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.protocol.write](../../../pangrova-protocol/src/main/java/org/pangrova/protocol/write) | 4 | 保留 / P4 | `pangrova-protocol`：`org.pangrova.protocol.write` | canonical schema、N/N-1、生成输入与双端 codec |

## pangrova-query

路径：`pangrova-query`。主源文件 7，主源包 6，本叶子测试源文件 4（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.query.assembly](../../../pangrova-query/src/main/java/org/pangrova/query/assembly) | 1 | 待验收 / P49 | `pangrova-query`：`org.pangrova.query.assembly` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.query.assembly.audit](../../../pangrova-query/src/main/java/org/pangrova/query/assembly/audit) | 1 | 待验收 / P49 | `pangrova-query`：`org.pangrova.query.assembly.audit` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.query.assembly.state](../../../pangrova-query/src/main/java/org/pangrova/query/assembly/state) | 1 | 待验收 / P49 | `pangrova-query`：`org.pangrova.query.assembly.state` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.query.core](../../../pangrova-query/src/main/java/org/pangrova/query/core) | 1 | 保留 / P3 | `pangrova-query`：`org.pangrova.query.core` | 单写者、取消/终态、journal/fence、恢复及同事务回滚 |
| [org.pangrova.query.core.audit](../../../pangrova-query/src/main/java/org/pangrova/query/core/audit) | 2 | 保留 / P3 | `pangrova-query`：`org.pangrova.query.core.audit` | 单写者、取消/终态、journal/fence、恢复及同事务回滚 |
| [org.pangrova.query.core.profiling](../../../pangrova-query/src/main/java/org/pangrova/query/core/profiling) | 1 | 保留 / P3 | `pangrova-query`：`org.pangrova.query.core.profiling` | 单写者、取消/终态、journal/fence、恢复及同事务回滚 |

## pangrova-scheduler

路径：`pangrova-scheduler`。主源文件 55，主源包 14，本叶子测试源文件 16（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.scheduler](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler) | 1 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.api.placement](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/api/placement) | 15 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.api.quota](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/api/quota) | 1 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.api.resource](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/api/resource) | 5 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.admission](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/admission) | 7 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.admission` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.failure](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/failure) | 5 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.failure` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.placement](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/placement) | 2 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.placement` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.resource](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/resource) | 1 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.resource` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.scoring](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/scoring) | 2 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.scoring` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.speculation](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/speculation) | 12 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.speculation` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.strategy.fair](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/strategy/fair) | 1 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.strategy.fair` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.core.strategy.loadbalanced](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/core/strategy/loadbalanced) | 1 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.core.strategy.loadbalanced` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.resource](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/resource) | 1 | 保留 / P5 | `pangrova-scheduler`：`org.pangrova.scheduler.resource` | 资源准入、公平性、placement、speculation 与恰一次释放 |
| [org.pangrova.scheduler.store.jdbc.admission](../../../pangrova-scheduler/src/main/java/org/pangrova/scheduler/store/jdbc/admission) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 资源准入、公平性、placement、speculation 与恰一次释放；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-security

路径：`pangrova-security`。主源文件 63，主源包 21，本叶子测试源文件 41（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.security](../../../pangrova-security/src/main/java/org/pangrova/security) | 1 | 保留 / P7 | `pangrova-security`：`org.pangrova.security` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.adapter.kms](../../../pangrova-security/src/main/java/org/pangrova/security/adapter/kms) | 1 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.adapter.kms` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.assembly.audit](../../../pangrova-security/src/main/java/org/pangrova/security/assembly/audit) | 3 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.assembly.audit` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.audit](../../../pangrova-security/src/main/java/org/pangrova/security/core/audit) | 4 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.audit` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.authn](../../../pangrova-security/src/main/java/org/pangrova/security/core/authn) | 4 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.authn` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.authn.provider.bypass](../../../pangrova-security/src/main/java/org/pangrova/security/core/authn/provider/bypass) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.authn.provider.bypass` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.authn.provider.ldap](../../../pangrova-security/src/main/java/org/pangrova/security/core/authn/provider/ldap) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.authn.provider.ldap` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.authn.provider.oauth2](../../../pangrova-security/src/main/java/org/pangrova/security/core/authn/provider/oauth2) | 9 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.authn.provider.oauth2` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.authz](../../../pangrova-security/src/main/java/org/pangrova/security/core/authz) | 6 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.authz` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.authz.deferred](../../../pangrova-security/src/main/java/org/pangrova/security/core/authz/deferred) | 2 | 待验收 / P49 | `pangrova-security`：`org.pangrova.security.core.authz.deferred` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.security.core.crypto](../../../pangrova-security/src/main/java/org/pangrova/security/core/crypto) | 4 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.crypto` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.mask](../../../pangrova-security/src/main/java/org/pangrova/security/core/mask) | 2 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.mask` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.mask.provider.empty](../../../pangrova-security/src/main/java/org/pangrova/security/core/mask/provider/empty) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.mask.provider.empty` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.mask.provider.memory](../../../pangrova-security/src/main/java/org/pangrova/security/core/mask/provider/memory) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.mask.provider.memory` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.rowfilter](../../../pangrova-security/src/main/java/org/pangrova/security/core/rowfilter) | 2 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.rowfilter` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.secret](../../../pangrova-security/src/main/java/org/pangrova/security/core/secret) | 1 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.secret` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.secret.provider.env](../../../pangrova-security/src/main/java/org/pangrova/security/core/secret/provider/env) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.secret.provider.env` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.secret.provider.file](../../../pangrova-security/src/main/java/org/pangrova/security/core/secret/provider/file) | 1 | 迁移 / P7 | `pangrova-security`：`org.pangrova.security.adapter.secret.provider.file` | provider 实现移出核心；保持实际选择和拒绝语义 |
| [org.pangrova.security.core.session](../../../pangrova-security/src/main/java/org/pangrova/security/core/session) | 1 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.session` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.core.tenant](../../../pangrova-security/src/main/java/org/pangrova/security/core/tenant) | 15 | 保留 / P7 | `pangrova-security`：`org.pangrova.security.core.tenant` | 当前权限、租户/角色隔离、撤权、secret 与 adapter 最小依赖 |
| [org.pangrova.security.spi.jdbc](../../../pangrova-security/src/main/java/org/pangrova/security/spi/jdbc) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | CurrentDirectRoleReader / CurrentModelAuthority 借用调用方事务；Security 实现当前角色读取，AI Control 实现模型当前态校验；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-semantic

路径：`pangrova-semantic`。主源文件 11，主源包 4，本叶子测试源文件 3（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.semantic](../../../pangrova-semantic/src/main/java/org/pangrova/semantic) | 1 | 保留 / P7 | `pangrova-semantic`：`org.pangrova.semantic` | View 定义权威、编译和授权；不扩展新 ontology 功能 |
| [org.pangrova.semantic.api.view](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/api/view) | 8 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | View 定义权威、编译和授权；不扩展新 ontology 功能 |
| [org.pangrova.semantic.core.view](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/core/view) | 1 | 保留 / P7 | `pangrova-semantic`：`org.pangrova.semantic.core.view` | View 定义权威、编译和授权；不扩展新 ontology 功能 |
| [org.pangrova.semantic.store.jdbc](../../../pangrova-semantic/src/main/java/org/pangrova/semantic/store/jdbc) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | View 定义权威、编译和授权；不扩展新 ontology 功能；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-shuffle-core

路径：`pangrova-shuffle/pangrova-shuffle-core`。主源文件 61，主源包 8，本叶子测试源文件 13（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.shuffle.api](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/api) | 26 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.core](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/core) | 4 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.core` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.exchange](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/exchange) | 15 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.exchange` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.provider](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/provider) | 1 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.provider` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.provider.spool](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/provider/spool) | 2 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.provider.spool` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.scheduling](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/scheduling) | 2 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.scheduling` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.spool](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool) | 6 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.spool` | exchange/spool owner、ticket、spill 和失败清理 |
| [org.pangrova.shuffle.spool.localfs](../../../pangrova-shuffle/pangrova-shuffle-core/src/main/java/org/pangrova/shuffle/spool/localfs) | 5 | 保留 / P6 | `pangrova-shuffle-core`：`org.pangrova.shuffle.spool.localfs` | exchange/spool owner、ticket、spill 和失败清理 |

## pangrova-shuffle-service

路径：`pangrova-shuffle/pangrova-shuffle-service`。主源文件 34，主源包 9，本叶子测试源文件 11（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.shuffle.checksum](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/checksum) | 1 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.checksum` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.config](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/config) | 2 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.config` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.deployment](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/deployment) | 2 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.deployment` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.security](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/security) | 4 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.security` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.server](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server) | 16 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.server` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.server.authority](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/authority) | 1 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.server.authority` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.server.error](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/error) | 2 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.server.error` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.server.persistence](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/server/persistence) | 1 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.server.persistence` | 远端协议、durable spool、authority、重启和关闭 |
| [org.pangrova.shuffle.transport](../../../pangrova-shuffle/pangrova-shuffle-service/src/main/java/org/pangrova/shuffle/transport) | 5 | 保留 / P6 | `pangrova-shuffle-service`：`org.pangrova.shuffle.transport` | 远端协议、durable spool、authority、重启和关闭 |

## pangrova-spi

路径：`pangrova-spi`。主源文件 461，主源包 83，本叶子测试源文件 65（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.spi](../../../pangrova-spi/src/main/java/org/pangrova/spi) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.capability](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/capability) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.capability` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.embedding](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/embedding) | 7 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.embedding` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.error](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/error) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.error` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.invocation](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/invocation) | 11 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.invocation` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.model](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/model) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.model` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.ai.runtime](../../../pangrova-spi/src/main/java/org/pangrova/spi/ai/runtime) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.ai.runtime` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector) | 8 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.base](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/base) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.base` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.base.protocol](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/base/protocol) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.base.protocol` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.base.split](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/base/split) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.base.split` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.clustering](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/clustering) | 10 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.clustering` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.coordination](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/coordination) | 9 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.block](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/block) | 10 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.block` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.page](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/page) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.page` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.type](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/type) | 12 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.type` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.type.mapping](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/type/mapping) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.type.mapping` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.type.primitive](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/type/primitive) | 12 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.type.primitive` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.type.primitive.temporal](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/type/primitive/temporal) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.type.primitive.temporal` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.value](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/value) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.value` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.write](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/write) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.write` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.data.write.model](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/data/write/model) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.data.write.model` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.index](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/index) | 16 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.index` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.index.predicate](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/index/predicate) | 15 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.index.predicate` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.index.statistics](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/index/statistics) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.index.statistics` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.materialization](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/materialization) | 13 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.materialization` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.metadata](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata) | 60 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.metadata` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.metadata.table](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/metadata/table) | 18 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.metadata.table` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.connector.plugin](../../../pangrova-spi/src/main/java/org/pangrova/spi/connector/plugin) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.connector.plugin` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.coordination](../../../pangrova-spi/src/main/java/org/pangrova/spi/coordination) | 8 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file](../../../pangrova-spi/src/main/java/org/pangrova/spi/file) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.access](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/access) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.access` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.authority](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/authority) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.authority` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.copy](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/copy) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.copy` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.directory](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/directory) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.directory` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.space](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/space) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.space` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.file.storage](../../../pangrova-spi/src/main/java/org/pangrova/spi/file/storage) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.file.storage` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.aggregate](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/aggregate) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.aggregate` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.metadata](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/metadata) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.metadata` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.registry](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/registry) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.registry` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.runtime](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/runtime) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.runtime` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.scalar](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/scalar) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.scalar` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.function.table](../../../pangrova-spi/src/main/java/org/pangrova/spi/function/table) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.function.table` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation) | 8 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation.execution](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation/execution) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation.execution` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation.execution.template](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation/execution/template) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation.execution.template` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation.format](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation/format) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation.format` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation.job](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation/job) | 10 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation.job` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.generation.manifest](../../../pangrova-spi/src/main/java/org/pangrova/spi/generation/manifest) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.generation.manifest` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.data](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/data) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.data` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.data.bitmap](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/data/bitmap) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.data.bitmap` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.data.spatial](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/data/spatial) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.data.spatial` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.data.text](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/data/text) | 4 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.data.text` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.descriptor](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/descriptor) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.descriptor` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.eval](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/eval) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.eval` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.pruning](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/pruning) | 6 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.pruning` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.publish](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/publish) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.publish` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.state](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/state) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.state` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.statistics](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/statistics) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.statistics` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.indexing.vector](../../../pangrova-spi/src/main/java/org/pangrova/spi/indexing/vector) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.indexing.vector` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.observability](../../../pangrova-spi/src/main/java/org/pangrova/spi/observability) | 18 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.observability` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.observability.event](../../../pangrova-spi/src/main/java/org/pangrova/spi/observability/event) | 8 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.observability.event` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.observability.profiling](../../../pangrova-spi/src/main/java/org/pangrova/spi/observability/profiling) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.observability.profiling` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query](../../../pangrova-spi/src/main/java/org/pangrova/spi/query) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query.audit](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/audit) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query.coordination](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/coordination) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.coordination` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query.identity](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/identity) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.identity` | canonical schema、N/N-1、生成输入与双端 codec |
| [org.pangrova.spi.query.lineage](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/lineage) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.lineage` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query.state](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/state) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.state` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.query.transaction](../../../pangrova-spi/src/main/java/org/pangrova/spi/query/transaction) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.query.transaction` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security](../../../pangrova-spi/src/main/java/org/pangrova/spi/security) | 14 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.audit](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/audit) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.audit` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.authn](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/authn) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.authn` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.authz](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/authz) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.authz` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.event](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/event) | 2 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.event` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.mask](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/mask) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.mask` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.policy](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/policy) | 7 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.policy` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.rowfilter](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/rowfilter) | 3 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.rowfilter` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.secret](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/secret) | 5 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.secret` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.session](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/session) | 1 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.session` | 领域契约或持久化职责；事务与调用方装配边界 |
| [org.pangrova.spi.security.tenant](../../../pangrova-spi/src/main/java/org/pangrova/spi/security/tenant) | 7 | 已迁入 / P49-P52；语义验收继续 | `pangrova-spi`：`org.pangrova.spi.security.tenant` | 领域契约或持久化职责；事务与调用方装配边界 |

## pangrova-standalone

路径：`pangrova-standalone`。主源文件 2，主源包 1，本叶子测试源文件 0（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.standalone](../../../pangrova-standalone/src/main/java/org/pangrova/standalone) | 2 | 保留 / P8 | `pangrova-standalone`：`org.pangrova.standalone` | embedding 装配、启动失败回滚、完整关闭与 SQL |

## pangrova-streaming

路径：`pangrova-streaming`。主源文件 120，主源包 25，本叶子测试源文件 50（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.streaming](../../../pangrova-streaming/src/main/java/org/pangrova/streaming) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.adapter.coordinator](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/adapter/coordinator) | 9 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.adapter.coordinator` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.api.aggregation](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/aggregation) | 5 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.api.cdc](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/cdc) | 12 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.api.checkpoint](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/checkpoint) | 6 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.api.source](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/source) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.api.watermark](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/api/watermark) | 1 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.aggregation](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/aggregation) | 4 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.aggregation` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.cdc](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/cdc) | 27 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.cdc` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.commit](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/commit) | 9 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.commit` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.maintenance](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/maintenance) | 16 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.maintenance` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.pipeline](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/pipeline) | 4 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.pipeline` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.query](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/query) | 2 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.query` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.core.watermark](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/core/watermark) | 8 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.core.watermark` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.cdcformat.canal](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/cdcformat/canal) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.cdcformat.canal` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.cdcformat.debezium](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/cdcformat/debezium) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.cdcformat.debezium` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.cdcformat.maxwell](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/cdcformat/maxwell) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.cdcformat.maxwell` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.writer.incremental](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/writer/incremental) | 2 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.writer.incremental` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.writer.lake](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/writer/lake) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.writer.lake` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.provider.writer.mergetree](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/provider/writer/mergetree) | 1 | 保留 / P7 | `pangrova-streaming`：`org.pangrova.streaming.provider.writer.mergetree` | checkpoint/offset/watermark 恢复、CDC sink 确认与事务 |
| [org.pangrova.streaming.store.jdbc.cdc](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/cdc) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | checkpoint/offset/watermark 恢复、CDC sink 确认与事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.streaming.store.jdbc.checkpoint](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/checkpoint) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | checkpoint/offset/watermark 恢复、CDC sink 确认与事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.streaming.store.jdbc.offset](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/offset) | 1 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | checkpoint/offset/watermark 恢复、CDC sink 确认与事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.streaming.store.jdbc.watermark](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/jdbc/watermark) | 2 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | checkpoint/offset/watermark 恢复、CDC sink 确认与事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.streaming.store.memory](../../../pangrova-streaming/src/main/java/org/pangrova/streaming/store/memory) | 2 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | checkpoint/offset/watermark 恢复、CDC sink 确认与事务；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-table

路径：`pangrova-table`。主源文件 37，主源包 11，本叶子测试源文件 9（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.table](../../../pangrova-table/src/main/java/org/pangrova/table) | 1 | 保留 / P7 | `pangrova-table`：`org.pangrova.table` | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.api.control](../../../pangrova-table/src/main/java/org/pangrova/table/api/control) | 11 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.api.governancebinding](../../../pangrova-table/src/main/java/org/pangrova/table/api/governancebinding) | 3 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.api.maintenance](../../../pangrova-table/src/main/java/org/pangrova/table/api/maintenance) | 5 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.api.policy](../../../pangrova-table/src/main/java/org/pangrova/table/api/policy) | 1 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.api.statistics](../../../pangrova-table/src/main/java/org/pangrova/table/api/statistics) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.core.control](../../../pangrova-table/src/main/java/org/pangrova/table/core/control) | 4 | 保留 / P7 | `pangrova-table`：`org.pangrova.table.core.control` | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.core.maintenance](../../../pangrova-table/src/main/java/org/pangrova/table/core/maintenance) | 1 | 保留 / P7 | `pangrova-table`：`org.pangrova.table.core.maintenance` | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.core.statistics](../../../pangrova-table/src/main/java/org/pangrova/table/core/statistics) | 1 | 保留 / P7 | `pangrova-table`：`org.pangrova.table.core.statistics` | 控制身份、policy binding、维护/统计；迁回 owner 测试 |
| [org.pangrova.table.store.jdbc.control](../../../pangrova-table/src/main/java/org/pangrova/table/store/jdbc/control) | 4 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 按实现能力拆；同事务绑定与独立 owner 测试不变；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |
| [org.pangrova.table.store.memory.control](../../../pangrova-table/src/main/java/org/pangrova/table/store/memory/control) | 4 | 按类迁移 / P49-Persistence | `pangrova-persistence`，按领域/能力与实际后端拆分 | 按实现能力拆；同事务绑定与独立 owner 测试不变；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence；领域判断留调用方，SQL与存储编解码归Persistence |

## pangrova-testing-benchmark

路径：`pangrova-testing/pangrova-testing-benchmark`。主源文件 7，主源包 2，本叶子测试源文件 0（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.testing.benchmark](../../../pangrova-testing/pangrova-testing-benchmark/src/main/java/org/pangrova/testing/benchmark) | 2 | 保留 / P9 | `pangrova-testing-benchmark`：`org.pangrova.testing.benchmark` | JMH 可运行和版本化输入，不替代正确性 |
| [org.pangrova.testing.benchmark.jmh](../../../pangrova-testing/pangrova-testing-benchmark/src/main/java/org/pangrova/testing/benchmark/jmh) | 5 | 保留 / P9 | `pangrova-testing-benchmark`：`org.pangrova.testing.benchmark.jmh` | JMH 可运行和版本化输入，不替代正确性 |

## pangrova-testing-fuzzer

路径：`pangrova-testing/pangrova-testing-fuzzer`。主源文件 1，主源包 1，本叶子测试源文件 0（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.testing.fuzzer](../../../pangrova-testing/pangrova-testing-fuzzer/src/main/java/org/pangrova/testing/fuzzer) | 1 | 保留 / P9 | `pangrova-testing-fuzzer`：`org.pangrova.testing.fuzzer` | 可重放 seed、真实 native 链与最小反例 |

## pangrova-testing-integration-tests

路径：`pangrova-testing/pangrova-testing-integration-tests`。主源文件 22，主源包 7，本叶子测试源文件 403（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.testing.integration.chaos.fault](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/chaos/fault) | 4 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.chaos.fault` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.chaos.linearizability](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/chaos/linearizability) | 3 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.chaos.linearizability` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.e2e](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/e2e) | 3 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.e2e` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.e2e.config](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/e2e/config) | 1 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.e2e.config` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.e2e.generator](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/e2e/generator) | 5 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.e2e.generator` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.e2e.runner](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/e2e/runner) | 3 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.e2e.runner` | SQL 全链、进程分离、实际执行数和无假跳过 |
| [org.pangrova.testing.integration.e2e.setup](../../../pangrova-testing/pangrova-testing-integration-tests/src/main/java/org/pangrova/testing/integration/e2e/setup) | 3 | 保留 / P9 | `pangrova-testing-integration-tests`：`org.pangrova.testing.integration.e2e.setup` | SQL 全链、进程分离、实际执行数和无假跳过 |

## pangrova-testing-kit

路径：`pangrova-testing/pangrova-testing-kit`。主源文件 27，主源包 8，本叶子测试源文件 29（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.testing.testkit](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit) | 14 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.architecture](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/architecture) | 1 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.architecture` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.function](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/function) | 2 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.function` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.lineage](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/lineage) | 1 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.lineage` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.network](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/network) | 1 | 待验收 / P49 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.network` | 新增职责包；检查调用、事务、异常与资源所有权 |
| [org.pangrova.testing.testkit.postgres](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/postgres) | 3 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.postgres` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.result](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/result) | 2 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.result` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |
| [org.pangrova.testing.testkit.security](../../../pangrova-testing/pangrova-testing-kit/src/main/java/org/pangrova/testing/testkit/security) | 3 | 保留 / P9 | `pangrova-testing-kit`：`org.pangrova.testing.testkit.security` | fixture 唯一包根、真实依赖、deterministic discovery 与 scanner |

## pangrova-transport

路径：`pangrova-transport`。主源文件 27，主源包 5，本叶子测试源文件 28（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.client.transport](../../../pangrova-transport/src/main/java/org/pangrova/client/transport) | 1 | 保留 / P8 | `pangrova-transport`：`org.pangrova.client.transport` | wire/result 映射、TLS、背压和 stream/buffer 关闭 |
| [org.pangrova.client.transport.http](../../../pangrova-transport/src/main/java/org/pangrova/client/transport/http) | 10 | 保留 / P8 | `pangrova-transport`：`org.pangrova.client.transport.http` | wire/result 映射、TLS、背压和 stream/buffer 关闭 |
| [org.pangrova.client.transport.security](../../../pangrova-transport/src/main/java/org/pangrova/client/transport/security) | 2 | 保留 / P8 | `pangrova-transport`：`org.pangrova.client.transport.security` | wire/result 映射、TLS、背压和 stream/buffer 关闭 |
| [org.pangrova.transport](../../../pangrova-transport/src/main/java/org/pangrova/transport) | 12 | 按类拆分 / P8 | `pangrova-transport`：`org.pangrova.transport.api.result / adapter.arrow / adapter.flight / assembly` | result 契约和 Arrow/Flight 实现分离；单制品可保持 |
| [org.pangrova.transport.security](../../../pangrova-transport/src/main/java/org/pangrova/transport/security) | 2 | 保留 / P8 | `pangrova-transport`：`org.pangrova.transport.security` | wire/result 映射、TLS、背压和 stream/buffer 关闭 |

## pangrova-worker-core

路径：`pangrova-worker/pangrova-worker-core`。主源文件 246，主源包 41，本叶子测试源文件 236（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.worker](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker) | 3 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.bootstrap](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap) | 5 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap` | 第43节：进程入口、启动顺序与清理准入；phase 不反向依赖编排器 |
| [org.pangrova.worker.bootstrap.config](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/config) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap.config` | 第43节：不可变部署配置，供启动阶段与部署适配器消费 |
| [org.pangrova.worker.bootstrap.lifecycle](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/lifecycle) | 5 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap.lifecycle` | 第43节：readiness、drain 与共享资源释放；关闭参数只保留真实 owner |
| [org.pangrova.worker.bootstrap.ownership](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/ownership) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap.ownership` | 第43节：组件引用与部分初始化资源账本，不执行 phase；删除五组无消费者字段 |
| [org.pangrova.worker.bootstrap.phase](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/phase) | 7 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap.phase` | 第43节：按阶段执行装配与发布资源，不引用顶层编排器 |
| [org.pangrova.worker.bootstrap.registration](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/bootstrap/registration) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.bootstrap.registration` | 第43节：注册/注销 HTTP 通信，不持有装配器或资源容器 |
| [org.pangrova.worker.buffer](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/buffer) | 9 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.buffer` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.cache](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/cache) | 3 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.cache` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.clustering](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/clustering) | 1 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.clustering` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.copy](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/copy) | 9 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.copy` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.dataset](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/dataset) | 5 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.dataset` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.embedding](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/embedding) | 1 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.embedding` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.exchange](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange) | 7 | 按类拆分 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.api / core / codec / adapter.nativebridge` | 第42节 CanonicalBatchChunks 已归 codec，exchange SCC 消失；其他执行/lease 职责继续按真实消费审查 |
| [org.pangrova.worker.exchange.codec](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/codec) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.codec` | 第42节纯 canonical batch 编码能力与包说明落位；chunk/checksum/只读字节语义不变，第42节真实远程链已通过 |
| [org.pangrova.worker.exchange.remote](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/remote) | 15 | 改依赖 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.remote` | remote 实现消费 exchange/execution SPI，不能反向引用聚合实现包 |
| [org.pangrova.worker.exchange.remote.security](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/remote/security) | 3 | 改依赖 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.remote.security` | remote 实现消费 exchange/execution SPI，不能反向引用聚合实现包 |
| [org.pangrova.worker.exchange.source](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/source) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.source` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.exchange.transport](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/exchange/transport) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.exchange.transport` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.execution](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/execution) | 9 | 按类拆分 / P6 | `pangrova-worker-core`：`org.pangrova.worker.execution.spi / core / adapter.nativebridge` | 只抽真实执行/egress 能力；保留 native creation recovery owner |
| [org.pangrova.worker.execution.filter](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/execution/filter) | 5 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.execution.filter` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.file](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/file) | 9 | provider已迁出 / 第48节；其余继续梳理 | `pangrova-worker-core`：`org.pangrova.worker.file` | HTTP descriptor/expiry/byte-window与provider装配；JDBC directory execution adapter的端口化仍开放 |
| [org.pangrova.worker.heartbeat](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/heartbeat) | 6 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.heartbeat` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.http](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/http) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.http` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.http.endpoint](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/http/endpoint) | 8 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.http.endpoint` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.memory](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/memory) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.memory` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.resource](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource) | 3 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.resource.guard](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource/guard) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource.guard` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.resource.memory](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource/memory) | 27 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource.memory` | 第43节：内存预算、池与 shuffle 进程租约；不依赖启动阶段 |
| [org.pangrova.worker.resource.spill](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource/spill) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource.spill` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.resource.tenant](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource/tenant) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource.tenant` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.resource.thread](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/resource/thread) | 1 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.resource.thread` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.result](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/result) | 7 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.result` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.shuffle](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/shuffle) | 5 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.shuffle` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.split](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/split) | 1 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.split` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.task](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task) | 42 | 按类拆分 / P6 | `pangrova-worker-core`：`org.pangrova.worker.task.lifecycle / task.state / task.admission / task.report` | 第42节 TaskState/TaskMemoryAccount 已归位并解环；TaskInfo 保留创建/释放/重试/终态单一 owner；其余能力继续语义审查 |
| [org.pangrova.worker.task.index](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/index) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.task.index` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.task.memory](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/memory) | 4 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.task.memory` | 第42节每任务唯一 TaskMemoryAccount 拥有两份原子账本，requestor/allocator 不再反向依赖 TaskInfo；异常回滚红绿及第42节完整 owner 已通过 |
| [org.pangrova.worker.task.report](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/report) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.task.report` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |
| [org.pangrova.worker.task.state](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/task/state) | 2 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.task.state` | 第42节纯 TaskState + transition 已独立，不依赖 TaskManager；八状态映射及状态唯一性检查通过，第42节生命周期完整回归已通过 |
| [org.pangrova.worker.transport](../../../pangrova-worker/pangrova-worker-core/src/main/java/org/pangrova/worker/transport) | 8 | 保留 / P6 | `pangrova-worker-core`：`org.pangrova.worker.transport` | native handle、buffer、lease 单一 owner；取消与关闭竞争 |

## pangrova-worker-native-bridge

路径：`pangrova-worker/pangrova-worker-native-bridge`。主源文件 38，主源包 9，本叶子测试源文件 25（不是测试用例数）。

| 当前包 | 文件数 | 动作 / 批次 | 目标制品与包 | 拆分依据与验收重点 |
|---|---:|---|---|---|
| [org.pangrova.worker.nativebridge](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge) | 1 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge` | JNI symbol/descriptor、catalog pin、回调线程与资源关闭 |
| [org.pangrova.worker.nativebridge.connector.session](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/connector/session) | 2 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.connector.session` | 不可变任务会话，不依赖JNI write/catalog runtime |
| [org.pangrova.worker.nativebridge.connector.spi](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/connector/spi) | 2 | 按类拆分 / P49-SPI | 通用契约→`pangrova-spi`，store端口→Persistence API；协议特有契约保留Protocol | 精确任务资源端口，write和catalog共同消费；资源owner不变 |
| [org.pangrova.worker.nativebridge.jni](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni) | 10 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.jni` | JNI symbol/descriptor、catalog pin、回调线程与资源关闭 |
| [org.pangrova.worker.nativebridge.jni.write](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write) | 10 | 按类拆分 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.jni.write；nativebridge.connector.session` | TaskConnectorSession 迁低层能力；native 方法 FQN 非必要不动 |
| [org.pangrova.worker.nativebridge.jni.write.catalog](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write/catalog) | 10 | 按类拆分 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.connector.spi / connector.catalog` | resolver 与 runtime 分离，write→SPI←catalog；catalog pin/close 不变 |
| [org.pangrova.worker.nativebridge.jni.write.secret](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write/secret) | 1 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.jni.write.secret` | JNI symbol/descriptor、catalog pin、回调线程与资源关闭 |
| [org.pangrova.worker.nativebridge.jni.write.secret.env](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/jni/write/secret/env) | 1 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.jni.write.secret.env` | JNI symbol/descriptor、catalog pin、回调线程与资源关闭 |
| [org.pangrova.worker.nativebridge.memory](../../../pangrova-worker/pangrova-worker-native-bridge/src/main/java/org/pangrova/worker/nativebridge/memory) | 1 | 保留 / P6 | `pangrova-worker-native-bridge`：`org.pangrova.worker.nativebridge.memory` | JNI symbol/descriptor、catalog pin、回调线程与资源关闭 |
