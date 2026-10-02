# Pangrova 行业级资格认证与生产验证计划

**状态：** Proposed  
**日期：** 2026-09-12  
**范围：** 查询正确性、性能、扩展效率、资源安全、故障恢复、长稳、CI 门禁、生产试点、升级与回滚  
**目标版本：** 首个可被称为 production-qualified 的 Pangrova release  
**原则：** 任何结论必须由可复现原始证据支持；架构或测试代码存在不等于能力已经通过资格认证。

## 1. 目标和完成定义

本计划把以下六项目标转化为可执行、可审计的 release qualification program：

1. 取得 TPC-H/TPC-DS SF100 和 SF1000 的真实、可复现结果，并在相同硬件、数据和运行规则下与 Trino、Presto、Spark + Gluten 对比。
2. 证明 4、8、16、32 Worker 的扩展效率、并发吞吐、P50/P95/P99、spill 和 shuffle backpressure 行为。
3. 证明 10 万和 100 万 split、数据倾斜、慢消费者、Worker 故障、Coordinator 故障下的正确性和有界资源行为。
4. 通过 8 小时、24 小时和 72 小时 soak，证明没有 Native/JVM 泄漏、队列持续增长、死锁、饥饿或句柄残留。
5. 让 Native、JNI、E2E、容器和依赖服务在 CI 中 fail closed；任何跳过、空运行、陈旧二进制或缺失测试都使 required check 失败。
6. 形成至少一个真实生产 workload 的脱敏回放包、SLO、canary、升级/降级、自动回滚和事故记录。

只有同时满足以下条件，版本才可标记为 `production-qualified`：

- 所有必需功能和正确性 gate 通过，查询结果与 oracle 一致。
- SF100 和 SF1000 结果均包含完整 workload，不含缺失、跳过、超时后忽略或零基线。
- 所有比较对象使用相同资源上限和相同数据快照；差异完整披露。
- 32 Worker 达到本计划定义的扩展效率门槛，或有经批准且量化的饱和资源解释。
- 极端 split、倾斜、背压和故障矩阵全部通过。
- 72 小时 release soak 通过。
- Required CI 中 skipped、failure、error、missing suite 均为零。
- 生产 canary 达到 SLO，并至少成功执行一次升级和一次真实回滚演练。
- 发布包包含不可变的证据索引、原始数据、配置、日志、指标、trace 和构建 provenance。

## 2. 声明边界与 TPC 合规

### 2.1 默认采用 TPC-derived 内部/公开比较

TPC-H 和 TPC-DS 是有严格规范和商标政策的 benchmark。未经独立审计的运行：

- 不称为官方 TPC Result。
- 不使用官方 primary/optional metric 名称宣传结果。
- 所有公开图表带 TPC Fair Use 要求的派生测试免责声明。
- 披露所有偏差，包括 schema、数据格式、query rewrite、缓存、容错、加载过程和运行方法。
- 不把未经审计的派生结果与 TPC 官网的官方结果直接比较。

当前规格和政策来源：

- TPC 当前规格：<https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp>
- TPC-H 3.0.1：<https://www.tpc.org/TPC_Documents_Current_Versions/pdf/TPC-H_v3.0.1.pdf>
- TPC-DS 当前规格以 TPC Current Specs 页面为准；每次运行必须在 manifest 中固定版本和工具 checksum。
- Fair Use Quick Reference：<https://www.tpc.org/tpc_documents_current_versions/pdf/tpc_fair_use_quick_reference_v1.0.0.pdf>
- 官方结果需要独立审计及 Full Disclosure Report：<https://www.tpc.org/information/other/submit_results5.asp>

如未来决定发布正式 TPC Result，应单独建立审计项目，在性能运行前聘请 auditor；本计划生成的内部结果不能事后自动转为官方结果。

### 2.2 两类结果必须分开

| 结果类型 | 用途 | 允许表述 | 必需内容 |
|---|---|---|---|
| Engineering qualification | PR/nightly/release gate | TPC-H/TPC-DS-derived workload | 完整环境与偏差披露，不能使用官方 primary metric |
| Audited TPC result | 正式市场发布 | TPC 官方规定表述 | Auditor、FDR、Executive Summary、官方提交 |

## 3. 当前基线与已确认缺口

以下是 2026-09-12 当前工作树的事实盘点，后续变化必须更新本节：

| 领域 | 已有基础 | 当前阻断项 |
|---|---|---|
| Native 测试 | CTest 当前可枚举约 3,250 个测试实例 | 尚无本次工作树的完整 release + ASan + E2E 统一证明 |
| Java/E2E | 大量 unit/property/E2E 测试和 Testcontainers 基础 | E2E 基类在 Docker、Native library 或 Flight 不可用时允许 skip |
| TPC runner | 已有数据生成、查询执行、回归比较脚本和 nightly workflow | 查询目录仅有 TPC-H Q1；TPC-DS 查询/schema 缺失；workflow 引用的启动脚本缺失 |
| 测量方法 | 输出单查询 samples、P50、P99 | 默认仅 3 个 measured samples，所谓 P99 实际是 max；warmup 失败被忽略 |
| 完整性 | 回归比较器已开始拒绝零值和缺失字段 | runner 会跳过缺失 query，可能仍以成功退出；未强制 22/99 全量 |
| 基线 | 有 SF10 描述性 baseline 和 SF100 文件 | SF100 22 个 query 的值全部为 0，不能用于资格认证 |
| TPC-DS | workflow 有占位 job | 没有可执行 query pack、schema、正确性 oracle 或 baseline |
| 横向对比 | 尚无统一 comparator harness | 没有 Trino、Presto、Spark + Gluten 的版本/config/container adapter |
| 扩展验证 | 有 worker 和 metrics 基础 | 没有固定 worker unit 的 4→32 自动实验和效率 gate |
| 极限/故障 | 有少量 chaos、fuzzer 和 failover 测试 | 百万 split、慢消费者、磁盘满、网络分区和 retry race 未形成矩阵 |
| 长稳 | 有 ASan/UBSan workflow | 没有 8h/24h/72h soak、斜率分析和泄漏 gate |
| 生产证据 | 有架构和目标文档 | 没有脱敏真实 workload、SLO 数据、canary 和回滚证据包 |

Phase 0 未关闭前，禁止发布任何 SF100/SF1000 或“行业领先”结论。

## 4. 项目治理

### 4.1 角色

角色对应职责，不绑定具体人员：

| 角色 | 职责 |
|---|---|
| Qualification Owner | 对最终 go/no-go 和证据完整性负责 |
| Benchmark Lead | workload、统计方法、比较公平性、报告 |
| Native Lead | Velox、内存、spill、shuffle、JNI、sanitizer |
| Distributed Lead | split、scheduler、attempt、fencing、failover |
| Connector Lead | Iceberg/Paimon、数据生成、schema、snapshot、写入 |
| SRE/Infra Lead | 裸机/Kubernetes、网络、对象存储、监控、故障注入 |
| Correctness Lead | oracle、结果 digest、SQL 兼容、浮点容差 |
| Release/Security Lead | provenance、密钥、升级、回滚、证据签名 |
| Production Owner | workload 授权、SLO、canary、业务验收 |

每项 gate 至少需要实现者和独立 reviewer。Benchmark Lead 不能单独批准自己的 baseline promotion。

### 4.2 状态模型

每个 requirement 只能处于以下状态之一：

- `NOT_STARTED`
- `IMPLEMENTING`
- `RUNNABLE`
- `MEASURED`
- `QUALIFIED`
- `BLOCKED_EXTERNAL`
- `FAILED`

`RUNNABLE` 只表示 harness 可运行，不能对外宣称能力完成。只有证据通过审核后才能进入 `QUALIFIED`。

### 4.3 变更控制

- workload、query、dataset、engine version、compiler flag、hardware 或 gate 改变，都生成新的 qualification revision。
- baseline 不自动更新。必须有三次独立成功运行、噪声检查、变更原因和两人批准。
- 原始结果不可覆盖；修正分析代码时重新生成 derived report，并保留旧版。
- 失败运行不得删除。标记 invalid 的运行必须保存 invalid reason。
- Benchmark 运行期间禁止自动升级 OS、JDK、Velox、对象存储或固件。

## 5. 资格认证环境

### 5.1 环境分层

| 环境 | 频率 | 数据规模 | 用途 | 是否 release gate |
|---|---|---|---|---|
| PR deterministic | 每个 PR | SF0.01/SF1、小 split 合成集 | 正确性、harness、JNI、fail-closed | 是 |
| Nightly dedicated | 每夜 | SF10/SF100 | 回归、并发、短故障、2h soak | 是 |
| Weekly scale | 每周 | SF100 | 4/8/16/32、倾斜、慢消费者、8h soak | 是 |
| Monthly large | 每月 | SF1000 | 完整比较、百万 split、24h soak | release 前必须通过 |
| Release qualification | 每个候选版本 | SF1000 + 真实 workload | 72h soak、升级/回滚、完整报告 | 是 |
| Production canary | 发布前后 | 脱敏/真实流量 | SLO 和业务验证 | 是 |

### 5.2 推荐物理拓扑

可比较的 scale-out 需要固定 Worker unit，不能只在一台机器上多起进程后称为 32 Worker 扩展：

- 1 台独立 load-driver 节点。
- 3 台独立 Coordinator 节点，用于 HA；性能基准可固定 1 个 active，另外两个 standby。
- 最多 32 个相同 Worker unit；优先一个 Worker unit 独占一个 NUMA 分区或节点。
- 独立对象存储集群，不与 Worker 争抢 CPU、内存或本地盘。
- 独立 metadata PostgreSQL/ZooKeeper 等控制依赖。
- 至少 25/100 GbE；记录实际速率、拓扑、MTU、拥塞和丢包。
- Worker unit 推荐固定为相同 vCPU、Native memory、JVM heap、本地 NVMe spill 容量和网络 QoS。

如果成本限制导致多个 Worker 共享宿主机，必须额外发布 density series，并使用 CPU pinning、NUMA pinning、cgroup memory/io 限额；不得把 density series 与 one-worker-per-node series 混为同一扩展曲线。

### 5.3 公平比较规则

Pangrova、Trino、Presto、Spark + Gluten 必须满足：

- 同一批 Parquet/ORC 文件、同一压缩、同一 partition layout、同一 snapshot checksum。
- 相同 aggregate vCPU、RAM、spill disk、网络带宽和对象存储权限。
- Coordinator/driver CPU 不计入 Worker 配额时，对所有引擎采用同一规则并披露。
- 使用固定发布版本和镜像 digest，禁止 `latest`。
- 分别运行 cold-cache 和 warm-cache，不混合样本。
- 引擎执行顺序使用随机化或 Latin square，降低温度、邻居和存储漂移影响。
- 每个引擎运行前恢复相同数据快照和缓存状态。
- 所有非默认配置、query rewrite、unsupported query 和失败均披露。
- 不允许以失败 query 的缺失降低总耗时；完整率低于 100% 的 run 无效。

### 5.4 环境 manifest

每次运行必须自动记录：

- `run_id`、UTC 时间、git SHA、dirty flag、build artifact SHA-256、container digest。
- Pangrova/Velox/Arrow/Substrait/JDK/compiler/glibc/kernel 版本。
- CPU model、microcode、socket/core/NUMA、频率 governor、SMT、内存容量和速度。
- 磁盘型号、filesystem、mount options、容量、IOPS/吞吐基线。
- NIC、driver、MTU、拓扑、带宽和 RTT。
- Worker/Coordinator/driver 数量及每个 cgroup/resource limit。
- dataset generator 版本、seed、SF、文件数、总字节、row count、schema/query checksum。
- 所有 engine config、session property、catalog config 的脱敏副本和 checksum。
- 缓存状态、warmup 规则、并发、query permutation、超时和重试规则。
- NTP/PTP 时钟偏差检查。

manifest 缺任一必需字段，run 标记为 invalid。

## 6. Benchmark harness 目标架构

建立一个统一 qualification harness，而不是继续让每个 workflow 拼 shell：

```text
Qualification Spec
  ├─ dataset + query pack + oracle
  ├─ engine adapters
  │    ├─ Pangrova
  │    ├─ Trino
  │    ├─ Presto
  │    └─ Spark + Gluten
  ├─ load model
  │    ├─ power/single stream
  │    ├─ closed-loop concurrency
  │    └─ open-loop arrival rate
  ├─ fault schedule
  ├─ metrics/traces/log collectors
  └─ validators
       ├─ completeness
       ├─ correctness
       ├─ statistics
       └─ resource/failure invariants
```

推荐在 `pangrova-testing` 下增加独立的 `pangrova-test-qualification` 模块，并保留现有 `pangrova-test-benchmark` 作为微基准和兼容入口。核心 runner 使用结构化结果模型；shell 只负责环境启动，不负责拼 JSON 或计算 percentile。

### 6.1 Engine adapter 合同

每个 adapter 必须实现：

- `provision()`：部署固定版本。
- `loadCatalog(datasetManifest)`：注册同一数据快照。
- `healthCheck()`：验证所有节点和依赖。
- `clearCaches(mode)`：执行明确且可验证的 cache policy。
- `execute(query, timeout, traceToken)`：返回首字节、末字节、row count、digest、error class。
- `collectPlan()`：保存 logical/physical/native plan。
- `collectMetrics(window)`：保存引擎和系统指标。
- `cancel(queryId)`：验证取消传播。
- `upgrade(from, to)` / `rollback(to)`：生产资格阶段使用。
- `destroy()`：回收资源且验证无残留。

### 6.2 结果数据模型

不得只保存汇总值。每个 sample 保存：

- engine/run/query/stream/sample identity。
- submit、accepted、planning start/end、execution start/end、first byte、last byte 时间。
- success/failure/cancel/timeout 和标准化 error class。
- row count、canonical result digest、source snapshot。
- scanned/pruned rows and bytes、CPU time、wall time、peak memory。
- shuffle input/output、spill read/write、blocked/backpressure time。
- retry/attempt/worker placement 和 winning fencing token。
- metrics artifact、log artifact、trace artifact 的 URI 和 checksum。

汇总报告必须能由 raw samples 重新生成。

### 6.3 统计规则

- 延迟使用单调时钟，从客户端提交到最后一字节完整消费；不能只测 server acknowledgement。
- 区分 queue、planning、execution、result transfer。
- P99 至少需要 100 个有效观察值；样本不足时不输出 P99，只输出样本量和可支持的统计量。
- 吞吐 steady-state window 至少 30 分钟或 10,000 次完成请求，取先满足者中更严格者。
- warmup 在达到 JIT/cache 稳态后结束，规则预先定义；warmup 失败使 run 无效。
- 每个性能结论至少 3 次独立 run；报告 median、MAD/CV、95% bootstrap confidence interval。
- 运行噪声 CV > 5% 时结果不进入 baseline，先定位环境噪声。
- 比较同时给出绝对值和相对值，不能只给 speedup。
- 超时和错误计入完成率，不能从 latency 分布删除。
- Open-loop 测试必须校正 coordinated omission；同时保存 offered load 和 achieved load。

## 7. 核心指标与 gate

### 7.1 正确性

| 指标 | Gate |
|---|---|
| Query completeness | TPC-H 22/22，TPC-DS 99/99；0 missing/skip |
| Result correctness | 所有 query canonical digest 与 oracle 匹配 |
| Snapshot consistency | 所有 engine 使用同一 dataset manifest |
| Retry correctness | 最终结果无重复/丢失；只有一个 fenced winning attempt |
| Write correctness | atomic commit、无双写、失败后临时文件可回收 |

无 `ORDER BY` 的结果按规范化行编码后进行 multiset digest；有序结果按顺序 digest。浮点和 decimal 使用 query-specific tolerance，任何 tolerance 必须记录，不能全局放宽。

### 7.2 性能和扩展

定义：

```text
scale_efficiency(N) = throughput(N) / (throughput(4) * N / 4)
```

Release gate：

- 8 Worker：效率不低于 85%。
- 16 Worker：效率不低于 78%。
- 32 Worker：效率不低于 70%。
- 如果未达到，只有在 CPU、storage、network 或单点 coordinator 饱和被指标明确证明、并形成容量边界文档时，才能作为 known limit 审批；不能称为线性扩展。
- 同 offered load 下 error rate 为 0，timeout rate 为 0；压力上限测试另行报告饱和点。
- 性能 baseline regression：P50 > 10% 或 P99 > 15% 且 confidence interval 不重叠时失败；吞吐下降 > 10% 时失败。

### 7.3 资源与背压

| 指标 | Gate |
|---|---|
| Per-task/worker memory | 不超过 grant/cgroup hard limit；无 OOM kill |
| Shuffle queue bytes | 始终不超过配置容量和允许的小幅测量误差 |
| Backpressure propagation | 慢消费者出现后 producer 停止无界增长，并在消费者恢复后继续 |
| Spill | 数据正确；spill bytes、disk latency、blocked time 可解释 |
| Coordinator heap | split/candidate/row 数量增加时不出现与数据量线性无界增长 |
| Handle/task registry | workload quiescence 后返回基线 |

### 7.4 可用性与恢复

初始内部 gate，生产 SLO 确认后可收紧：

- 单 Worker SIGKILL：已提交 query 在配置 deadline 内成功重试或返回明确失败，不能 silent partial success。
- Coordinator active SIGKILL：新 leader 在 30 秒内可接收新查询，已接受查询按协议恢复或明确失败。
- late attempt：不能覆盖 winning attempt 的结果或 commit。
- 网络恢复：积压有界，恢复后无永久卡住的 task。
- 取消：客户端取消后 30 秒内 task、buffer、spill、临时文件和句柄回收。

## 8. 工作流与任务分解

### WS0：冻结声明与建立证据治理

**目标：** 在 harness 可信前阻止错误性能结论。

- `QG-001` 建立 qualification status 页面，明确 `design / runnable / measured / qualified`。
- `QG-002` 为报告增加 TPC-derived disclaimer 和 deviation section。
- `QG-003` 定义 baseline promotion、invalid run、异常豁免和双人审批流程。
- `QG-004` 定义 artifact retention：PR 30 天、nightly 180 天、release 永久或至少 3 年。
- `QG-005` 建立 evidence index schema 和 SHA-256 签名流程。
- `QG-006` 修正 README/website 中未经证据支持的性能或 PB-scale 表述。

**退出条件：** 每个公开能力声明可以链接到 evidence ID；没有 evidence 的声明显式标为 target。

### WS1：修复并重建 benchmark harness

**目标：** runner 对缺失、跳过、错误样本和无效基线严格失败。

- `BH-001` 增加 preflight，检查 query 数量、schema、driver、engine endpoint、dataset 和磁盘空间。
- `BH-002` TPC-H 必须发现且校验 22 个 query；TPC-DS 必须发现且校验 99 个 query。
- `BH-003` 缺少 query/schema/start script 时退出码为 2，不允许 warning + continue。
- `BH-004` warmup 任意失败使 run invalid，不允许 `|| true`。
- `BH-005` 用结构化 driver 替代 bash 字符串拼接 JSON。
- `BH-006` 正确测量 first byte、last byte、完整结果消费和客户端开销。
- `BH-007` 使用可信 histogram/quantile 实现；样本不足拒绝 P95/P99。
- `BH-008` 每次运行保存 raw sample，不只保存 percentile。
- `BH-009` 为 manifest、result、baseline、comparison 编写 JSON Schema 或 protobuf schema。
- `BH-010` 校验 NaN、Infinity、零/负值、重复 key、query 集合、sample 数、SF 和 engine mismatch。
- `BH-011` baseline promotion 工具要求三个独立 run 和双人 approval metadata。
- `BH-012` 增加 harness 自测：缺 query、假成功、timeout、错误 digest、clock rollback、部分结果、零 baseline。
- `BH-013` 实现 graceful teardown 和脏环境检测；残留进程/port/container 使下一 run 失败。
- `BH-014` 输出机器可读 verdict：`PASS / FAIL / INVALID / BLOCKED_EXTERNAL`。

**退出条件：** 故意删除任一 query、制造 warmup 失败或填入零 baseline 时，workflow 必须红；完整 synthetic run 可从 raw data 重建相同报告。

### WS2：数据、查询和正确性资格

**目标：** 在做性能比较前证明所有引擎执行的是同一 workload 且结果正确。

- `WC-001` 选择并固定 TPC-H/TPC-DS 工具版本、下载来源和 checksum。
- `WC-002` 评审工具和 query 的许可证；不允许提交的内容在运行时从授权来源生成。
- `WC-003` 建立 SF0.01、SF1、SF10、SF100、SF1000 dataset manifests。
- `WC-004` 验证每张表 row count、schema、partition、file count、总字节和全量 checksum。
- `WC-005` 生成 Parquet 和可选 ORC；固定 row group、compression、dictionary、file target size。
- `WC-006` 生成完整 TPC-H 22 和 TPC-DS 99 query pack，记录 substitution seed 和 checksum。
- `WC-007` 为每个引擎维护最小必要 dialect adapter；每个 rewrite 进入 deviation report。
- `WC-008` SF1 对照规范 reference answers；建立 canonical result encoder。
- `WC-009` SF10/SF100/SF1000 使用跨引擎 digest 和独立 oracle 交叉验证。
- `WC-010` 保存 explain logical/physical/native plan，检测意外 fallback 或 unsupported operator。
- `WC-011` query 失败、fallback、partial result、近似结果均不能作为正确成功。
- `WC-012` 建立 data refresh/update 测试；若不执行 TPC refresh，报告必须明确偏差。

**退出条件：** 四个引擎在 qualification scale 完整通过结果校验；所有 rewrite 和 unsupported feature 有清单。

### WS3：同硬件竞争引擎适配

**目标：** 建立可重复的 Pangrova、Trino、Presto、Spark + Gluten 对比。

- `CE-001` 固定各引擎 release、JDK、native backend 和 container digest。
- `CE-002` 为每个引擎实现 provision/health/catalog/query/metrics/teardown adapter。
- `CE-003` 统一 CPU、RAM、spill、本地盘、网络和对象存储资源限制。
- `CE-004` 统一 file format、catalog snapshot 和统计信息构建规则。
- `CE-005` 定义 cold/warm cache 清理验证，而不是只执行清理命令。
- `CE-006` 捕获所有配置和 session properties；敏感值脱敏但保留 checksum。
- `CE-007` 执行随机化 engine order，至少三轮独立复现实验。
- `CE-008` 生成逐 query 和整体报告：成功率、绝对延迟、吞吐、资源、成本估算。
- `CE-009` 独立 reviewer 检查是否存在对单一引擎有利的 rewrite、统计或缓存。
- `CE-010` 报告 query coverage；不生成只基于成功子集的 geometric mean。

**退出条件：** 任一 reviewer 可按 evidence bundle 在同一硬件重跑，并在噪声范围内得到相同结论。

### WS4：4→32 Worker 扩展与并发

**目标：** 定量证明扩展效率、并发容量和饱和点。

- `SC-001` 固定一个 Worker unit 和 Coordinator/load-driver topology。
- `SC-002` 对 4/8/16/32 Worker 分别运行 SF100 和 SF1000 power workload。
- `SC-003` 运行 closed-loop concurrency：1、4、8、16、32、64、128 clients。
- `SC-004` 运行 open-loop offered load 阶梯，直到 P99 或 error SLO 首次失守。
- `SC-005` 每个点至少 30 分钟 steady-state；随机化 worker-count 顺序并重复三轮。
- `SC-006` 分别报告冷缓存、暖缓存和强制 spill profile。
- `SC-007` 捕获 CPU、RSS、JVM heap/GC、network、disk、object-store、shuffle、spill 和 queue。
- `SC-008` 计算 scale efficiency、吞吐/核、吞吐/GB、每 TB 扫描成本。
- `SC-009` 使用 trace 分解 planning、queue、scan、exchange、operator、result transfer。
- `SC-010` 识别第一个饱和资源，建立容量模型和 admission-control 参数。
- `SC-011` 验证 Coordinator heap/CPU 随 Worker 和 split 数增长的斜率。
- `SC-012` 建立自动 regression gate，保存与最近 qualified baseline 的差异。

**退出条件：** 32 Worker 达到 70% 效率，或明确记录真实饱和边界；所有点完成率和结果正确性为 100%。

### WS5：极端 split、倾斜、背压与故障

**目标：** 证明资源有界和失败语义正确，而不是只证明 happy path。

#### Split 矩阵

- `FT-001` 100,000 split：1 KB、64 KB、1 MB 和混合大小。
- `FT-002` 1,000,000 split：空 split、重复 split ID、乱序、延迟 listing、listing 中途失败。
- `FT-003` 测量 Coordinator/Worker heap slope、task request size、调度延迟、split residency。
- `FT-004` 证明 split 流式发现/交付，不因固定 100k cap 或全量 materialization 失败。
- `FT-005` retry 后每个 split 在 winning attempt 中恰好一次；旧 attempt 被 fencing。

#### 倾斜矩阵

- `FT-006` Zipf alpha：0、0.8、1.2、1.5、2.0。
- `FT-007` 单热 partition：50%、90%、99% rows/bytes。
- `FT-008` 宽行和可变 payload：P50/P99 row size 差 10 倍和 100 倍。
- `FT-009` join key、group key、window key 和 shuffle partition 四类倾斜分别覆盖。
- `FT-010` 验证 skew metrics、spill、repartition 或明确 admission failure，无单 Worker 无界增长。

#### 慢消费者和背压矩阵

- `FT-011` consumer 带宽限制为正常值的 50%、10%、1%。
- `FT-012` consumer pause 10 秒、60 秒、5 分钟，然后恢复。
- `FT-013` consumer 断开、不读取、半关闭、读取后取消。
- `FT-014` 多 partition 中一个慢，其余正常，检测 head-of-line blocking。
- `FT-015` oversized batch、磁盘 spill 慢、disk full、inode full。
- `FT-016` 验证 queue hard bound、producer blocking、取消传播、恢复和资源回收。

#### 故障矩阵

- `FT-017` Worker：SIGTERM、SIGKILL、process hang、OOM kill、磁盘故障。
- `FT-018` Coordinator：active crash、standby crash、leader lease expire、GC pause、时钟偏移。
- `FT-019` 网络：delay、jitter、loss、partition、reset、带宽限制、半开连接。
- `FT-020` 依赖：metadata DB restart、object store 5xx/timeout、DNS failure、credential rotation。
- `FT-021` retry/hedge race：旧 attempt 晚到、双成功、commit 前后故障。
- `FT-022` 验证结果完整、错误分类、RTO、fencing、无重复 commit、临时文件 GC。
- `FT-023` 每种故障至少运行 30 次；并发 race 类至少 1,000 次或达到预定置信度。

**退出条件：** 故障矩阵无 silent partial success、无越权 late commit、无无界队列；恢复时间满足 gate。

### WS6：长稳、泄漏和死锁验证

**目标：** 证明系统在稳定和故障混合负载下可长期运行。

#### Workload mix

建立至少三个 profile：

1. `read-heavy`：短查询 60%、复杂 join/agg 30%、大 scan 10%。
2. `mixed-lakehouse`：SELECT 65%、INSERT/UPDATE/DELETE/MERGE 20%、DDL/metadata 5%、故障/取消 10%。
3. `stress-spill-shuffle`：强制内存压力，大 shuffle、倾斜和慢消费者。

#### Soak tiers

- `SK-001` PR lifecycle loop：30 分钟，ASan/LSan，反复 init/create/cancel/release/shutdown。
- `SK-002` Nightly：2 小时，4 Worker，mixed workload。
- `SK-003` Weekly：8 小时，16/32 Worker，包含周期性故障。
- `SK-004` Monthly：24 小时，SF1000，包含 connector refresh 和 spill。
- `SK-005` Release：72 小时，生产形状 workload、canary 配置和升级/回滚事件。

#### Leak 和 liveness gate

- 预热 60 分钟后计算 Native RSS、JVM old-gen、allocator retained bytes、task/handle count、queue bytes 的 Theil-Sen slope。
- Native RSS steady-state slope 同时满足 `<= 32 MiB/hour` 和 `<= native cap 的 0.25%/hour`。
- JVM old-gen GC 后 slope `<= heap 的 0.1%/hour`。
- workload 停止并完成 GC/cleanup 后，RSS、handle、task、queue 回到预热基线的 5% 范围；allocator 合理缓存需单列并证明有 hard bound。
- queue depth/bytes 不允许持续单调增长；用窗口趋势和 hard capacity 双重判断。
- 任何 sanitizer finding、core dump、uncaught exception、OOM kill、deadlock 或 stuck query 都使 run 失败。
- watchdog 每 30 秒检查 progress counter；连续 5 分钟无进展时抓取 Java thread dump、native stack、metrics 和 trace，再使 run 失败。

**退出条件：** 72 小时 run 满足全部 gate，且 teardown 后资源回到允许范围。

### WS7：CI fail-closed

**目标：** required CI 不允许“环境缺失所以绿”。

- `CI-001` 增加统一 `qualification-preflight`：Docker、Native ABI、Flight、PostgreSQL、端口、磁盘、CPU feature。
- `CI-002` 引入 `pangrova.test.requireDocker=true`、`requireNative=true`、`requireFlight=true` 等 required-profile 开关。
- `CI-003` local developer profile 可报告 `BLOCKED_EXTERNAL`，required CI profile 必须直接失败。
- `CI-004` 移除 required suite 中 `Assumptions`/skip hook 对基础设施缺失的容错。
- `CI-005` 外部能力测试使用固定 Testcontainer；镜像 pull 失败是基础设施失败，不是 skipped test。
- `CI-006` 测试后解析 Surefire/Failsafe/CTest XML：`skipped > 0`、`error > 0`、`failure > 0` 均失败。
- `CI-007` 建立 expected suite manifest 和最小 test count；0 tests、suite 消失、名称过滤错误均失败。
- `CI-008` 使用 `-DfailIfNoTests=true`，删除会掩盖空测试选择的参数。
- `CI-009` Native build 生成 build ID；Java E2E 启动时校验 JNI build ID/git SHA，拒绝 stale library。
- `CI-010` Native CTest、ASan、UBSan、package、Java、E2E、smoke 形成显式依赖 DAG。
- `CI-011` branch protection 将 native、java、E2E、sanitizer、protocol、qualification smoke 设为 required。
- `CI-012` flaky test 不允许自动重试后绿；保留所有 attempt 并以 flake budget gate。
- `CI-013` 定义 flake budget：required tests 最近 30 天 flaky rate 必须为 0；发现后立即隔离环境或修复根因，不能 disable。
- `CI-014` 上传 test XML、logs、cores、manifest、native symbols 和 coverage，即使 job 失败。
- `CI-015` 每周运行一次 clean-room build，禁用 dependency cache，验证构建可重现。

**退出条件：** 人为移除 Native library、停止 Docker、禁用 Flight、过滤掉测试、制造 skip，所有 required workflow 均失败且错误清晰。

### WS8：真实生产 workload、SLO、升级与回滚

**目标：** 从 synthetic benchmark 进入真实业务证明。

#### Workload capture

- `PR-001` 获得业务、隐私和安全授权；定义允许采集字段和保留期。
- `PR-002` 按 fingerprint 聚合 SQL；移除 literal、token、PII、路径、tenant secret。
- `PR-003` 保存 workload 分布：query class、arrival rate、并发、扫描量、结果量、失败率、时间段。
- `PR-004` 构造可共享的 synthetic data twin；保持 cardinality、skew、partition 和 file-size 分布。
- `PR-005` 选择覆盖累计 CPU 时间、P99、错误和业务关键性的 workload pack，不只选快查询。
- `PR-006` 使用现网引擎结果或业务 oracle 做 dual-run correctness comparison。

#### SLO 模板

最终数值由 Production Owner 批准，至少包含：

- Availability：按 query class 的成功率，排除规则明确。
- Latency：interactive P50/P95/P99、batch deadline completion。
- Correctness：silent wrong result 为 0；partial result 必须显式协议化。
- Freshness：metadata/snapshot/index 可见延迟。
- Recovery：Worker/Coordinator 故障 RTO，commit 恢复 RPO=0。
- Resource：每 tenant/query 的 memory、spill、queue 和并发边界。
- Upgrade：升级期间错误率、延迟预算和允许容量下降。

#### 推进阶段

- `PR-007` Offline replay：100% workload pack，结果和性能符合门槛。
- `PR-008` Shadow：复制请求但不返回用户，运行至少 7 天。
- `PR-009` Canary 1%：至少 24 小时且覆盖峰值窗口。
- `PR-010` Canary 5% / 25% / 50%：每级至少一个完整业务周期，无 SLO breach 才晋级。
- `PR-011` 100%：保留快速回滚至少两个发布周期。

#### 升级和回滚

- `PR-012` 验证 N-1→N rolling upgrade 的 protocol/capability compatibility。
- `PR-013` 验证混合版本 Worker pool 只调度兼容 task，descriptor mismatch fail closed。
- `PR-014` 验证 query in-flight、shuffle、spill、write commit 在升级中的行为。
- `PR-015` 执行 N→N-1 rollback；不能只验证升级成功。
- `PR-016` 数据/schema migration 必须 expand-contract，并有逆向路径或明确 stop-the-world gate。
- `PR-017` 自动 rollback trigger：错误率、P99、wrong result、OOM、crash loop、commit anomaly。
- `PR-018` rollback 目标：触发后 10 分钟内恢复上一 qualified artifact；具体 SLO 经生产批准。
- `PR-019` 至少执行一次故意注入失败的回滚演练并保存时间线。
- `PR-020` 建立 incident template：影响、检测、时间线、root cause、修复、回归测试。

**退出条件：** 至少一个真实 workload 完成 7 天 shadow、分级 canary、一次升级和一次回滚演练，全部证据进入 release bundle。

### WS9：报告、审计与发布

- `RP-001` 自动生成 run summary，但报告数字必须链接 raw sample。
- `RP-002` 生成环境、配置、数据、query、偏差和失败完整清单。
- `RP-003` 生成 per-query latency、throughput、resource、plan 和 correctness 表。
- `RP-004` 生成 4→32 efficiency、并发 saturation、spill/backpressure 图。
- `RP-005` 生成 failure recovery timeline 和 soak slope 图。
- `RP-006` 生成竞争比较，同时显示绝对值、资源和 confidence interval。
- `RP-007` 安全审查 artifact，避免 credentials、PII、内部 hostname 泄露。
- `RP-008` 独立复现至少一次 release run。
- `RP-009` Qualification Owner、Benchmark Lead、SRE 和 Production Owner 签署 verdict。
- `RP-010` 发布后锁定 evidence bundle 和 artifact digest，任何更改产生新 revision。

## 9. 精确实验矩阵

### 9.1 TPC-derived workload

| Scale | Pangrova | Trino | Presto | Spark + Gluten | 重复 | 用途 |
|---|---:|---:|---:|---:|---:|---|
| SF1 | 必须 | 必须 | 必须 | 必须 | 1+ | 正确性/reference answer |
| SF10 | 必须 | 必须 | 必须 | 必须 | 3 | harness 和配置校准 |
| SF100 | 必须 | 必须 | 必须 | 必须 | 3 | nightly/竞争基线 |
| SF1000 | 必须 | 必须 | 必须 | 必须 | 3 | release qualification |

每个 scale 至少包含：

- 完整 query set。
- 单 stream power-style run。
- 多 stream throughput-style run。
- 冷缓存和暖缓存。
- 正常内存和强制 spill。
- 结果 correctness digest。

### 9.2 Worker/concurrency matrix

| Workers | Clients | Cache | Memory profile | 数据 |
|---:|---|---|---|---|
| 4 | 1,4,8,16,32 | cold/warm | normal/spill | SF100/SF1000 |
| 8 | 1,8,16,32,64 | cold/warm | normal/spill | SF100/SF1000 |
| 16 | 1,16,32,64,128 | cold/warm | normal/spill | SF100/SF1000 |
| 32 | 1,32,64,128,256 | cold/warm | normal/spill | SF100/SF1000 |

高并发点如果超出 admission capacity，必须记录明确 reject，而不是 timeout 或 OOM。饱和点由 offered load、achieved throughput 和 P99 转折共同确定。

### 9.3 故障注入时点

每种故障至少覆盖：

- task admission 前。
- scan 进行中。
- shuffle producer 已输出 25%/75%。
- output task 合并中。
- write prepare 后、commit 前。
- commit 成功后 acknowledgement 前。
- cancel 与 retry 同时发生。

## 10. Roadmap、依赖和里程碑

日历时间取决于是否有独立性能集群和生产 workload 权限。下面假设 Benchmark、Native、Distributed、SRE、Correctness 五个角色可并行投入。

| Milestone | 目标周 | 内容 | 硬性退出条件 |
|---|---:|---|---|
| M0 Truthful baseline | W1 | WS0、现状清单、声明冻结 | 所有未证实声明降级；evidence schema 确定 |
| M1 Fail-fast harness | W3 | WS1、CI preflight 原型 | 缺 query/Native/Docker/Flight 必失败 |
| M2 Correct workload | W6 | WS2、完整 22/99、SF1/SF10 oracle | 四引擎 100% correctness |
| M3 CI closed | W8 | WS7 required gates | 0 skip、0 empty suite、stale JNI 被拒绝 |
| M4 SF100 comparison | W11 | WS3 + SF100 三轮 | 完整、可复现、独立 reviewer 通过 |
| M5 Scale/failure | W15 | WS4 + WS5 | 32 Worker efficiency 和故障矩阵通过 |
| M6 SF1000/24h | W18 | 大数据三轮 + monthly soak | SF1000 完整，24h 通过 |
| M7 Production pilot | W22+ | workload、shadow、canary | SLO、升级和回滚演练通过 |
| M8 Release qualified | W24+ | 72h、独立复现、签署 | 全部 gate 通过并封存证据 |

### 10.1 关键路径

```text
真实 workload 权限 ────────────────────────────────┐
                                                   ├─ Production pilot ─ Release
完整 query/data → 正确性 oracle → 可信 harness ────┤
                              ├─ Comparator ─ SF100/1000
                              ├─ 4→32 scale ─ Fault matrix
                              └─ CI fail-closed ─ Soak ──────────┘
```

最常见的错误顺序是先跑 SF1000，再发现 query 缺失、结果不正确或 percentile 无效。本计划明确要求 M1/M2 先完成。

### 10.2 粗略工作量

| Workstream | 人周估算 | 外部依赖 |
|---|---:|---|
| WS0 治理 | 1–2 | 产品/法务 review |
| WS1 harness | 4–6 | 无 |
| WS2 workload/correctness | 5–8 | TPC kit 使用条件、存储 |
| WS3 comparator | 5–7 | 四引擎镜像和专家配置 |
| WS4 scale | 4–6 | 32 Worker 性能环境 |
| WS5 fault | 6–10 | Toxiproxy/Kubernetes/裸机故障权限 |
| WS6 soak | 3–5 实现 + 运行时间 | 长时环境配额 |
| WS7 CI | 3–5 | Branch protection 管理权限 |
| WS8 production | 6–12+ | 生产授权和业务周期 |
| WS9 report | 2–4 | 独立 reviewer |

估算不包括修复测试揭示的引擎缺陷；缺陷修复进入独立 issue，但会阻止对应 gate。

## 11. 建议仓库产物

目标结构，实施时按模块边界调整：

```text
pangrova-testing/
  pangrova-test-qualification/
    src/main/java/.../driver/
    src/main/java/.../engine/
    src/main/java/.../oracle/
    src/main/java/.../metrics/
    src/main/java/.../report/
    src/test/java/.../
    specs/
      qualification.schema.json
      result.schema.json
      environments/
      workloads/
  pangrova-test-benchmark/
    queries/                 # 仅存许可允许的内容
    scripts/                 # legacy wrapper，调用统一 runner
    baseline/                # 仅存 qualified 非零 baseline
  pangrova-test-chaos/
    scenarios/
      split/
      skew/
      backpressure/
      worker/
      coordinator/
      storage/
  pangrova-test-soak/
    profiles/
    analyzers/
infra/qualification/
  kubernetes/
  terraform/
  monitoring/
docs/qualification/
  methodology.md
  deviations.md
  evidence-index.md
```

建议新增 workflows：

- `qualification-pr.yml`
- `qualification-nightly.yml`
- `qualification-weekly-scale.yml`
- `qualification-monthly-sf1000.yml`
- `qualification-release.yml`
- `qualification-clean-room-build.yml`

## 12. Evidence bundle 规范

每个有效 run 生成只读目录：

```text
evidence/<qualification-revision>/<run-id>/
  manifest.json
  verdict.json
  raw-samples.parquet
  query-results/
  result-digests.json
  plans/
  configs/
  metrics/
  traces/
  logs/
  system/
  faults/
  report.md
  checksums.sha256
```

`verdict.json` 必须列出每个 gate 的：

- threshold。
- actual value。
- pass/fail/invalid。
- supporting artifact checksum。
- evaluator version。
- reviewer 和时间。

不得把 GitHub summary 当作唯一证据存储。

## 13. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| TPC 商标/规则误用 | 结果不能公开，法律风险 | 默认 TPC-derived + disclaimer；正式结果提前引入 auditor |
| 不同引擎资源不公平 | 比较结论无效 | cgroup/CPU pinning、同数据、完整配置披露、独立 review |
| 对象存储成为瓶颈 | 错把 storage 饱和当 engine 性能 | 单独校准 storage roofline，保存吞吐/延迟指标 |
| 3 个样本伪装 P99 | 尾延迟结论错误 | P99 至少 100 observations；不足不输出 |
| 缺 query 仍成功 | 虚假全量成绩 | preflight + expected query manifest + fail closed |
| CI skip 掩盖 ABI/环境问题 | 绿色但未执行 | required profile 禁止 skip，XML 后置审计 |
| 缓存污染 | 引擎顺序影响结果 | cold/warm 分开，随机顺序，验证 cache 状态 |
| benchmark tuning 过拟合 | 生产无收益 | 保留真实 workload 和 holdout query pack |
| Native 长稳问题只在 release 出现 | 延迟修复 | 30m/2h/8h/24h 分层提前发现 |
| 生产数据泄漏 | 合规事故 | fingerprint、literal removal、synthetic twin、安全 review |
| rollback 只有文档没有执行 | 故障时无法恢复 | 每个 release 必须真实演练并计时 |
| 大量现有改动干扰 baseline | 无法归因 | qualification 从 clean signed commit 构建，dirty run 无效 |

## 14. Issue 模板与执行规则

每个任务 issue 必须包含：

```text
Requirement ID:
Owner / reviewer:
Dependencies:
Environment:
Dataset/query manifest:
Implementation scope:
Correctness oracle:
Metrics and thresholds:
Failure cases:
Artifacts produced:
Commands/workflow:
Rollback:
Exit evidence ID:
```

每次实验的执行 checklist：

1. 确认 clean commit、artifact digest 和环境 reservation。
2. 运行 preflight；失败立即结束并标记 invalid。
3. 验证 dataset/query/config checksums。
4. 启动监控和 trace，再启动引擎。
5. health/readiness 通过后执行 correctness qualification。
6. 执行预定义 warmup；任意失败使 run invalid。
7. 执行 measurement，不在中途修改配置。
8. 执行 fault schedule 或 soak schedule。
9. 完整消费结果并验证 digest。
10. 收集 teardown 后资源，生成 verdict 和 checksum。
11. 独立 reviewer 检查异常、缺失和比较公平性。
12. 只有通过审批的 run 才能 promotion 为 baseline。

## 15. Go/No-Go 总表

| Gate | 必须结果 | Evidence |
|---|---|---|
| Build provenance | clean、可重现、signed digest | manifest + build logs |
| Native/JNI | release/ASan/UBSan 全绿 | CTest XML + sanitizer logs |
| Java/E2E | 0 failed/error/skipped，suite 数匹配 | Surefire/Failsafe XML |
| TPC-H | SF100/SF1000 22/22 正确 | raw samples + digest |
| TPC-DS | SF100/SF1000 99/99 正确 | raw samples + digest |
| Comparator | 四引擎三轮、公平性 review | configs + reports |
| Scale | 32 Worker >= 70% 或批准的饱和边界 | metrics + traces |
| Split | 100k/1m 正确且内存有界 | heap/RSS/assignment proof |
| Skew/backpressure | queue 有界，无 HOL deadlock | metrics + fault timeline |
| Failover | 无 silent partial、无重复 commit | attempts/fencing/journal |
| Soak | 72h 无泄漏/死锁/OOM | slopes + dumps + metrics |
| Production | 7d shadow + canary SLO | production evidence pack |
| Upgrade/rollback | N-1→N→N-1 演练通过 | timeline + artifact IDs |
| Security/compliance | 无 secret/PII，TPC 声明合规 | review sign-off |

任何一项不通过，release verdict 为 `NO-GO`。不得通过修改报告、删除失败样本、增加资源上限、延长 timeout 或关闭测试来改变 verdict。

## 16. 第一批实施顺序

建议立即建立以下 12 个 issue，按顺序推进：

1. `BH-001/BH-002/BH-003`：完整性 preflight 和缺失 query fail closed。
2. `BH-004/BH-007/BH-008`：修复 warmup、samples 和 percentile。
3. `WC-001/WC-002`：固定合法的 TPC kit 来源、版本和 checksum。
4. `WC-006/WC-008`：完整 22/99 query pack 和 SF1 oracle。
5. `CI-001/CI-002/CI-004`：required CI 基础设施缺失直接失败。
6. `CI-006/CI-007/CI-008`：XML skip/空 suite 审计。
7. `CI-009`：Java/Native build identity 防止旧 dylib/so。
8. `CE-001/CE-002/CE-003`：四引擎 adapter 和资源合同。
9. `SC-001/SC-007`：固定 Worker unit 和指标采集。
10. `FT-001/FT-002/FT-011/FT-014`：split 与慢消费者最小阻断集。
11. `SK-001/SK-002`：30 分钟 lifecycle + 2 小时 nightly soak。
12. `QG-005/RP-001`：evidence bundle 和报告生成。

这 12 项完成后，才开始首次可信的 SF100 对比；SF100 通过后再申请 SF1000 和 32 Worker 的昂贵资源。
