# Feature Store：真实调用链与完整实现边界

日期：2026-10-07。检查基线为当前工作区；已有用户改动保留。
状态：源码核查和实施规格，不是完整 Feature Store 的通过证明。

## 官方机制及本项目选择

[Feast PIT](https://docs.feast.dev/getting-started/concepts/point-in-time-joins)
按 entity 行的时间向过去寻找特征，TTL 相对于该行，不能按当前时间计算。
默认 created timestamp 用于同 event time 去重；当前文档另有
`filter_by_created_timestamp=True`，要求 created time 不晚于 entity time，
NULL created time 被排除，未支持的 store 显式报错。
采用严格历史可见语义时，应记录特征实际 available time，不能把任意写入日期
当作在线可见时间。这个选择需要 source 的真实可见性定义和一致的 snapshot。

[Feast offline store](https://docs.feast.dev/reference/offline-stores/overview)
区分历史检索、最新值物化、保存数据集，并允许 SQL、Arrow 输出；各 store 的
能力不同。Pangrova 的选择是复用同一 Calcite/Substrait/Velox/Flight 和 durable
artifact 执行，不新增 Python 数据路径、在线专用执行器或第二套任务调度器。

## 当前入口和断点

| 接点 | 当前实际行为 | 尚未闭合的能力 |
|---|---|---|
| `QueryExecutionBootstrap` / `QueryMaterializationWiring` | 注入当前用户的 `FeatureSourceCompiler`，经受保护 SQL 编译 | 不等于已有历史检索或 CDC 服务 |
| `FeatureViewStatementService` | 创建、发布 Feature View 版本，提交物化；验证 source plan 和定义身份 | 未注册 Training Dataset 的实际 SQL 编译/执行入口 |
| `FeatureViewDefinitionValidator` | 只接受 `entity_keys,event_time,source_table,source_query,transform_version,schema,freshness_target_ms,lineage`；仅允许单源和受支持 scalar | 没有 TTL、availability、tie-break、跨源 snapshot map；`freshness_target_ms` 不是历史 TTL |
| `DurableFeatureExportService.submit` | 校验 actor/tenant/唯一 source，固定一个 snapshot，枚举真实 splits，生成 durable export job | 没有 PIT 关系改写、多源分配或增量 watermark |
| `FeatureExportStageGraph.prepare` | 持久化普通 stage graph 和 export sink，保存分片依赖与 exchanges | 模型/retrieval stage 显式拒绝；不能称为自动 Embedding 特征已接通 |
| `FeatureCatalogStore.TrainingDatasetVersion` / `JdbcFeatureCatalogStore` | 保存 source snapshots、feature versions、normalized plan、split kind/seed 等元数据 | 当前 Coordinator 没有生产调用 `createTrainingDataset`；元数据 record 不证明数据集构建能力 |
| Feature View publication | 激活元数据版本，export 另有 generation 和 artifact publication | 元数据激活不是相应数据已 ready 的证明；读取必须固定实际 published generation |

当前单源快照 export 应保留为实际已实现的能力，但不得标注为多源 PIT、在线特征、
CDC 或完整 Feature Store。拒绝未支持输入的事实也不计作对应能力已实现。

## 必须一起实现的链路

1. 通过唯一 SQL 前端定义 entity 输入、每个 Feature View 的输出契约、entity keys、
   event time、availability time、TTL、确定性 tie-break 和 transform version。
   schema 使用 canonical LogicalType，不能仅用 scalar 名称掩盖 precision/nullability。
2. 在授权后固定 entity 源和每个 feature 源的表 incarnation、snapshot、模型/索引
   generation 和完整 policy observation；同一 SQL 的 source map 写进最终 durable
   graph。分片按所属源分配，不能把一个源的 splits 复用给全部叶 stage。
3. 为每条 entity 输入保留稳定、唯一的行身份。使用常规 Calcite join/window 关系
   表达 keys 相等、feature event time <= entity time、event time >= entity time - TTL，
   严格模式再约束 availability time <= entity time。按 event/availability/tie-break
   选择一个候选；没有候选仍保留 entity 行，重复输入不能在去重中丢失。
4. 正常转换为 Substrait 后由 Velox join/window/filter/project 执行；禁止 Coordinator
   收集全数据自行排序/拼接。时区、microsecond precision、NULL keys/time、TTL 边界、
   多条同时间记录都必须在单一语义契约裁决。
5. 同一 durable artifact store 记录全部源 watermark vector、不可变 transform/model
   version、attempt 和 generation。只有全部 partition/checksum/schema/owner fence
   验证后原子发布；迟到旧 attempt 不得覆盖。CDC 没有共同物理时间时保留每源
   watermark，不能捏造一个全局已一致 snapshot。
6. 离线训练与在线读取固定同一个发布版本，通过相同 SQL/Flight 得到 typed Arrow。
   Temporal/entity split 必须先固定规则，再分配训练/验证/测试；相同 entity 跨集合
   泄漏不能由随机种子合法化。任何非确定性模型特征必须固定已物化 generation。

## 真实验收要求

使用实际 catalog、真实 lake 数据、packaged JNI 和 Flight。独立预期数据覆盖：
TTL 的前/等于/后边界、晚到修订、NULL availability、相同 event time 冲突、重复
entity 输入、找不到特征、多源不同 watermark、schema/model 更新、并发撤销、
Worker 失联和旧 attempt 迟到。精确校验每行与物化 version；重跑相同 frozen
输入必须得到相同集合。再以真实多进程规模测 spill、shuffle、内存、吞吐和 P99。
当前尚未运行这些完整 oracle，不记录为 PASS。
