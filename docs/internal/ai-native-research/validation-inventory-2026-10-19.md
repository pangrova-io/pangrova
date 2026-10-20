---
title: "AI Native 生产验收剩余清单与滚动批次（2026-10-19）"
description: "按当前主分支源码区分已通过、本机待验证、功能未闭合和外部发布环境；不复用旧源码的全绿结论。"
---

# AI Native 剩余验证清单

本清单以 `codex/ai-native-semantic-closure` 为唯一交付分支，沿用[27 项验收定义](validation-batches-2026-10-15.md)。提交 `04fadbcf` 的 macOS ARM64 Native Release 加真实 PostgreSQL/Lance broker **4,175/4,175 PASS**（`/tmp/pangrova-integrated-native-ctest-20261019.log`），匹配插桩 Velox/Folly/Protobuf 的 Native ASan 加真实 broker **4,175/4,175 PASS**（`/tmp/pangrova-integrated-native-asan-final-20261019.log`，macOS `detect_leaks=0`）；隔离 Native owner 三类独立 JVM **9/9 PASS**（`/tmp/pangrova-owner-{worker-bootstrap,native-exchange,shuffle-admission}-20261019.log`）。新增 `$entries` 的提交 `a1cdcb60` 已通过定向 Java、Release/ASan Native 各 10/10、4/4 真 SQL E2E、完整 Java reactor 无测试安装；后续夹具提交 `859c1380` 的协议全模块 **447/447 PASS**（`/tmp/pangrova-protocol-a1cdcb60-r2-20261019.log`）；该轮 TPC-H Q11–Q22 **12/12 PASS**、本地独立双 Worker 和独立 AI 进程分别 **1/1 PASS**。提交 `f87cfd1f` 的 Native Release 全量结果见下文，最终源码全量 ASan/Java 测试 reactor/E2E 仍待验证。仓库发现 **195 个 `*E2ETest.java` 类**；旧提交、其他工作树或镜像的结果只保留精确来源，不能转记为最终源码全绿。

本机独立进程追加验证：一台 Coordinator 加两个独立 Native Worker 的 8 批 Iceberg JDBC 写入、JOIN/shuffle、Worker 停机重启及资源释放通过（`/tmp/pangrova-independent-local-processes-r5-20261019.log`）；预置 Ollama 镜像的真实模型 HTTP/SQL、两名 Worker 分别单独调用模型、重启后双 Worker 查询、审计和资源释放通过（`/tmp/pangrova-independent-ai-processes-r3-20261019.log`）。Coordinator `-Pfull-regression` 根因修复后整模块退出 0；本轮 542 份 Surefire 报告合计 **3,213 项，0 失败、0 错误、0 跳过**（`/tmp/pangrova-coordinator-full-r3-20261019.log`）。上一轮 3,295 项的失败记录不再代表当前结果，完整 Java reactor 仍待验。Worker 405 映射已通过定向测试。上述是功能证据，不代表规模/P99 验收。

提交 `f87cfd1f` 的 Native Release 加真实 PostgreSQL/Lance broker 已完整执行 **4,178/4,178 PASS**（`/tmp/pangrova-native-file-sequence-full-20261019.log`，总用时 606 秒）。该提交的 Connector 绑定 4/4、Native reader 11/11 和 Iceberg v3 血缘真 SQL 4/4 也已通过；静态分析、Native ASan、完整 Java reactor 和共享 E2E 仍需分别取得同源码终态，不用 Release 结果代替。

冻结提交 `65c7dffa` 的新增滚动复验：`GenerateSeriesE2ETest` 经真实 SQL/JNI **6/6 PASS**（`/tmp/pangrova-generate-series-current-20261019.log`，Surefire XML）；`IcebergShowMetadataTest` **5/6 PASS**，唯一失败为 `SHOW PARTITIONS` 缺 Native reader（`/tmp/pangrova-show-metadata-current-r2-20261019.log`）。后一失败有真实 SQL、真实模型夹具和 Native 启动作为前置，不把它记成已闭环。[Iceberg 规范](https://iceberg.apache.org/spec/)和[官方查询文档](https://iceberg.apache.org/docs/1.11.0/spark-queries/)要求 `$partitions` 按当前快照、分区 spec 和 data/delete 文件汇总；现有 `$files` Native 投影没有分区值，不能用无分区键的简单 SQL 聚合代替。

后续 `$entries` 血缘切片把 Iceberg manifest entry 的 data sequence 与 file sequence 分开暴露并校验：Native reader 定向 **11/11 PASS**，其中明确验证两者不同和超前文件序号拒绝；真实 PostgreSQL/Lance broker 预检 **1/1 PASS**（`/tmp/pangrova-iceberg-file-sequence-native-focused-20261019.log`、`/tmp/pangrova-lance-broker-current-preflight-20261019.log`）；Connector 绑定 **4/4 PASS**（`/tmp/pangrova-iceberg-file-sequence-connector-r2-20261019.log`）；重封 JNI 后的 `IcebergV3LineageTest` 真 SQL **4/4 PASS**（`/tmp/pangrova-iceberg-file-sequence-e2e-20261019.log`）。同源码 Native Release 全量已通过，完整 SDK 嵌套投影和 ASan 仍需完成，不用定向成功替代。

后续提交 `859c1380` 从真 Native/FILE/模型部署重采集了规范 descriptor 夹具，Protocol **447/447 PASS**；提交 `cf767a46` 修复 Worker 关闭与非法任务 400 语义后，Worker 整模块 **1,279/1,279 PASS**、静态 `verify` 通过，且重新封存该提交的 Worker JAR 后，两项独立进程 E2E **2/2 PASS**（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）。前段的 `a1cdcb60` 进程结果仅作历史证据，不冒充最新源码。

| 批次 | 检查点 | 当前缺口和本机验收动作 |
| --- | --- | --- |
| A：同源码基础门禁 | 5 Native Release | 提交 `f87cfd1f` **通过** 4,178/4,178，含真实 PostgreSQL/Lance broker；后续源码变更须按影响范围再验。 |
| A | 6 Native ASan | 提交 `04fadbcf` **通过** 4,175/4,175；`$entries` 修改后的定向 ASan 10/10 通过（`/tmp/pangrova-iceberg-row-id-asan-focused-20261019.log`），最终源码还须全量重跑；macOS 不覆盖 Linux LeakSanitizer。 |
| A | 7 Java reactor | 提交 `04fadbcf` Coordinator 整模块 3,213/3,213 通过；提交 `a1cdcb60` 完整 Java reactor 无测试安装、Planner/Connector/Coordinator 定向及受影响静态 `verify` 通过；提交 `cf767a46` Worker 1,279/1,279 与静态 `verify` 通过。最终源码仍需完整 `-Pfull-regression verify`。 |
| A | 8 静态/协议/结构 | 提交 `a1cdcb60` 协议夹具从真 Native 重采集后 447/447，通过日志 `/tmp/pangrova-protocol-a1cdcb60-r2-20261019.log`；文档类引用 183 项、E2E 真实性、descriptor、SPI 注解 147 类型、JNI 22 方法对称性及 dead-stub 检查此前通过；最终同源码仍需全 reactor 静态。覆盖率百分比不是验收门禁，JaCoCo 只按需生成报告。 |
| A | 10 隔离 Native owner | 提交 `04fadbcf` 三类各自独立 JVM：5 + 3 + 1 = 9/9，零跳过；Native 协议修改后最终源码还须复验。 |
| B：真实 SQL 与分布式 | 1 ARM64 镜像 | 旧源码镜像已构建；本机功能验收可用独立 JVM，不要求先打包镜像。若要交付镜像，再按交付源码重建。 |
| B | 2 镜像内 JNI/SQL 冒烟 | 本机 JNI/SQL 先以同源码独立进程验证；镜像内资格仅随镜像交付检查。 |
| B | 9 共享 SQL E2E | 完整发现/执行尚未通过；当前提交的 Iceberg `$entries` 4/4、TPC-H Q11–Q22 12/12 已增量通过；195 个 E2E 类继续按首个根因滚动修复。 |
| B | 11 两独立 Worker | 真实 JOIN/shuffle/TopK **1/1 通过**，含不同 PID/端口、两端任务和 drain；提交 `cf767a46` 重封 JAR/Native 后 8 批 JDBC 写入、重启后 JOIN/资源释放 1/1 再通过（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）。同源码推测执行 10 场景仍待全组复验。 |
| B | 12 故障恢复矩阵 | 丢 ACK、未知提交、取消、慢读、进程退出、OOM 等组合仍未完整验收；按单一故障根因增量跑。 |
| C：SQL/存储/写入 | 13 Iceberg REST | 用户决定当前先验 JDBC；非空 REST 发布 CAS/竞态未闭合，列为**暂缓协议范围**。 |
| C | 14 Paimon REST | 生产对象是 **Paimon REST Catalog 协议**，不是 `paimon-rust`；用户决定先验 JDBC，跨服务 CAS/fencing 列为**暂缓协议范围**。 |
| C | 15 历史版本和写入所有权 | Lance/Paimon 旧 snapshot、并发 winner/loser、未知提交及跨 reader 故障矩阵未全闭合。 |
| C | 16 Native 大写入容量 | 已有小规模实值 E2E；容量、spill、失败后的资源归零未全覆盖。 |
| C | 17 唯一 Calcite 前端 | 全 DDL/DML/时间旅行生产路径迁移和可达旧 fallback 清理未完成。 |
| C | 18 `GENERATE_SERIES` | 提交 `65c7dffa` 当前源码真实 SQL/JNI E2E **6/6 PASS**；最终同源码全功能回归仍待纳入最终批次。 |
| C | 19 `COPY INTO` | stage-only 和 `FROM (SELECT …)` 局部通过；URI SQL→可信 split→Native Parquet→Iceberg marker/未知提交对账尚未接通。 |
| C | 20 系统表 | `$files`/`$manifests` 等局部实值通过；Iceberg `$entries` 已有固定 manifest、状态、data/file 序列、v3 row ID 继承及 4/4 真 SQL E2E，但完整 SDK 嵌套列尚未闭合；SHOW 元数据 5/6，`$partitions` 缺 Native reader，Paimon `$files` 和复杂列/跨文件系统仍未完整验收。 |
| C | 21 Dynamic Table | 手动 full-refresh generation 已有真实 E2E；逻辑授权、持久 attempt/source pin、失败恢复、GC、调度/CDC 未闭合。 |
| C | 22 DiskANN 生命周期 | 小规模真实构建/检索/GC 局部通过；多 split、无结果和目标规模质量/成本待验。 |
| D：AI/Feature/CDC | 23 独立模型服务 | 提交 `cf767a46` 重封 JAR/Native 后预置真实模型的独立双 Worker HTTP/SQL、分别单独调用、重启和审计 1/1 再通过（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）；异步批量、取消、乱序、超时、未知完成、fencing 未全闭合。 |
| D | 24 Feature/CDC/评估 | 单源 Feature View 与数值评估局部通过；多源 point-in-time、真实 Kafka→sink→重启→SQL 和发布闭环未完成。 |
| E：发布/规模环境 | 3 Linux x86_64 镜像 | 当前无 x86_64 runner；不阻断本机独立 Worker 功能验收，交付 Linux 镜像时必须验。 |
| E | 4 Linux ASan/LSan | 当前无 Linux runner；macOS `detect_leaks=0` 不能代替。 |
| E | 25 TPC-H/写入基准 | 缺指定 Linux SF100/SF1000 与并发环境；本机小样本不外推。 |
| E | 26 向量质量/成本 | 缺 1,000 万/1 亿向量目标规模资源；本机只做正确性和小样本 oracle。 |
| E | 27 多 Worker 扩展/稳定性 | 缺 2 Coordinator、4–8 Worker 的指定规模环境；本机独立进程验证功能与故障，不冒充吞吐、P99 或长稳验收。 |

**计数与执行次序：**27 项中 #5 已在提交 `f87cfd1f` 全量通过；#6/#10 在更早冻结提交全量通过，但最终源码仍须复验。当前同源码全量通过计数为 **1**；其余本机条目还需重验或实现，2 项 REST 协议按用户决定暂缓（#13/#14），5 项需要当前没有的发布/规模环境（#3/#4/#25–#27）。A 的轻量规则检查可与 B 的共享 E2E 并行；Native ASan、完整 E2E 和其他高内存容器批次不能同时挤占本机 16 GiB RAM/Docker VM。B 的首个失败修复后滚动扩面，C/D 按独立纵切进行，最后在**同一最终源码**做一次完整功能回归。每批记录实测数量、失败根因、日志、commit 和磁盘余量；不把未运行的测试记作通过。
