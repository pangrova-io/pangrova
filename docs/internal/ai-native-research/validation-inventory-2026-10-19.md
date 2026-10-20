---
title: "AI Native 生产验收剩余清单与滚动批次（2026-10-19）"
description: "按当前主分支源码区分已通过、本机待验证、功能未闭合和外部发布环境；不复用旧源码的全绿结论。"
---

# AI Native 剩余验证清单

**2026-10-20 最新增量：**为闭合 `$partitions` 的前置能力，`$files` 已增加按稳定 field ID 读取的真实分区 struct，支持 spec 演进后的字段改名和缺失维度 NULL；读取身份也包含元数据位置与规范化 spec 映射。Java 绑定最近检查 8/8、缓存身份场景 1/1、Iceberg 所属模块 `-Pfull-regression` **437/437**、从最终打包 JNI 重采集库存后的 Protocol **447/447** 均通过，零失败/错误/跳过。最终改动的 Release Native 和匹配插桩 ASan 各 **17/17**；新 SQL/JNI 场景按增量执行共 **3/3**，原 lineage **4/4**、manifest-list **2/2**，新场景物理 ownership 和 active query count 归零断言共 **3/3** 通过。完整 struct SQL 首次失败定位为重复 Java 列白名单，已改为从冻结 typed schema 校验，新增回归断言后重跑通过；只读标量列也不再额外拷贝分区 tuple。最终日志为 `/tmp/pangrova-partition-final-{iceberg-module,protocol,native-focused,asan-focused,java-static}-20261020.log`；SQL 见 `/tmp/pangrova-partition-projection-{e2e-drain,manifests-e2e,full-struct-e2e-r2}-20261020.log`。Java 受影响依赖链及最终叶模块静态检查通过。本增量不宣称 `$partitions` 聚合或全部系统表已完成。此前 4,179 项 Native 全覆盖是本次运行代码修改前的基线；后续只补影响范围，待全部功能闭合后统一做用户要求的最终全量。

**元数据授权增量：**修复 `$entries` 遗漏基表 SELECT 校验。真实 RBAC 最近测试 **56/56**；新增真 HTTP SQL/JNI E2E **1/1**，覆盖 Iceberg 五种元数据表的后缀授权拒绝、基表授权放行、撤销后立即拒绝，以及 query ownership/active count 归零。Coordinator `-Pfull-regression` 首轮 **3,305 次用例执行中 3,295 通过**，10 项均因启动命令遗漏 `pangrova.lance.fixture.tool` 参数而失败；按当前源码构建该工具后，只补跑这 10 项，**10/10 PASS**，静态 `verify` 通过。合并首轮和增量结果，**3,305/3,305 次执行记录全部通过，零失败/错误/跳过**；这不是一次连续全绿运行。证据 `/tmp/pangrova-metadata-authority-coordinator-rolling-union-20261020.json`，日志 `/tmp/pangrova-metadata-authority-{focused,e2e,coordinator-full,coordinator-recovery-static,e2e-static}-20261020.log`。该批只证明授权身份与撤销语义；基表行过滤/列掩码对元数据读的保护仍需单独闭合。

本清单以 `codex/ai-native-semantic-closure` 为唯一交付分支，沿用[27 项验收定义](validation-batches-2026-10-15.md)。提交 `04fadbcf` 的 macOS ARM64 Native Release 加真实 PostgreSQL/Lance broker **4,175/4,175 PASS**（`/tmp/pangrova-integrated-native-ctest-20261019.log`），匹配插桩 Velox/Folly/Protobuf 的 Native ASan 加真实 broker **4,175/4,175 PASS**（`/tmp/pangrova-integrated-native-asan-final-20261019.log`，macOS `detect_leaks=0`）；隔离 Native owner 三类独立 JVM **9/9 PASS**（`/tmp/pangrova-owner-{worker-bootstrap,native-exchange,shuffle-admission}-20261019.log`）。新增 `$entries` 的提交 `a1cdcb60` 已通过定向 Java、Release/ASan Native 各 10/10、4/4 真 SQL E2E、完整 Java reactor 无测试安装；后续夹具提交 `859c1380` 的协议全模块 **447/447 PASS**（`/tmp/pangrova-protocol-a1cdcb60-r2-20261019.log`）；该轮 TPC-H Q11–Q22 **12/12 PASS**、本地独立双 Worker 和独立 AI 进程分别 **1/1 PASS**。提交 `f87cfd1f` 的 Native Release 全量结果见下文，ASan 滚动结果见下文，最终全功能回归仍待全部缺口修复后执行。仓库发现 **196 个 `*E2ETest.java` 类**；旧提交、其他工作树或镜像的结果只保留精确来源，不能转记为最终源码全绿。

本机独立进程追加验证：一台 Coordinator 加两个独立 Native Worker 的 8 批 Iceberg JDBC 写入、JOIN/shuffle、Worker 停机重启及资源释放通过（`/tmp/pangrova-independent-local-processes-r5-20261019.log`）；预置 Ollama 镜像的真实模型 HTTP/SQL、两名 Worker 分别单独调用模型、重启后双 Worker 查询、审计和资源释放通过（`/tmp/pangrova-independent-ai-processes-r3-20261019.log`）。Coordinator `-Pfull-regression` 根因修复后整模块退出 0；本轮 542 份 Surefire 报告合计 **3,213 项，0 失败、0 错误、0 跳过**（`/tmp/pangrova-coordinator-full-r3-20261019.log`）。上一轮 3,295 项的失败记录不再代表当前结果，完整 Java reactor 仍待验。Worker 405 映射已通过定向测试。上述是功能证据，不代表规模/P99 验收。

提交 `f87cfd1f` 的 Native Release 加真实 PostgreSQL/Lance broker 已完整执行 **4,178/4,178 PASS**（`/tmp/pangrova-native-file-sequence-full-20261019.log`，总用时 606 秒）。该提交的 Connector 绑定 4/4、Native reader 11/11 和 Iceberg v3 血缘真 SQL 4/4 也已通过；E2E 依赖 reactor 的 `-DskipTests -Pstatic-analysis verify` 退出 0（`/tmp/pangrova-iceberg-file-sequence-static-r2-20261020.log`）。完整 Java reactor 测试和共享 E2E 仍需取得终态，不用 Native 或静态结果代替。

2026-10-20 的 ASan 按用例并集核对 **4,179 个唯一用例全部通过，0 遗漏**：首次运行在 959 项后中止，899 项通过、60 项因旧 broker 进程已退出而在 `SetUp()` 连接失败；重启真实 PG authority 后，首个失败项 1/1、失败及未执行的稳定名称项 3,262/3,262、修正命名后的 decimal 与新增生命周期项 17/17 依次通过。全部结果覆盖当前 CTest 发现清单（`/tmp/pangrova-asan-final-discovery-20261020.json`），并集证据为 `/tmp/pangrova-asan-rolling-union-20261020.json`，各批日志路径见该证据；**这是滚动全覆盖，不是一次连续全量运行**。运行代码 `pangrova-worker-native/src` 的 Git 树与 `f87cfd1f` 相同（`490aa5ffb13b510fff4b3d9f52bb599484c49f88`），只增加了测试命名和启动工具。使用 `detect_leaks=0:halt_on_error=1`，不证明 Linux LSan 或 UBSan 验收。

Native 测试统一入口 `pangrova-worker-native/bin/run-native-tests.py` 现在准备当前 Java 夹具、创建新 nonce/socket、认证真实 authority 就绪并管理 broker/CTest 进程组；broker JVM 退出显式停止其 PostgreSQL 容器。生命周期单元 **6/6**，真实 PG/Native 加启动入口检查 **2/2 PASS**（`/tmp/pangrova-native-fixture-runner-real-20261020.log`），前后 PostgreSQL 容器清单一致且没有残留 broker 进程；Java 依赖链与最终所属模块静态 `verify` 均退出 0（`/tmp/pangrova-native-fixture-lifecycle-java-static{,-final}-20261020.log`）。门禁策略 18/18、文档类引用 183/183 和工作流 YAML 语法通过；CI 调用已接线，但未声称远端 CI 已执行。

增量选测发现 16 个 decimal 参数用例的默认 CTest 名称包含对象中的进程地址，重新发现后无法按原名称准确选中。现已改为运算名和左右/输出 precision、scale 组成的稳定名称，Release 定向 **16/16 PASS**（`/tmp/pangrova-stable-decimal-test-names-release-focused-20261020.log`）；两次独立 CTest 发现（第二次强制重新运行发现进程）的 **4,178 个唯一名称完全一致**（`/tmp/pangrova-stable-ctest-discovery-{a,b}-20261020.json`）。这项仅改测试命名，不改变 Native 运行代码或 decimal oracle。

冻结提交 `65c7dffa` 的新增滚动复验：`GenerateSeriesE2ETest` 经真实 SQL/JNI **6/6 PASS**（`/tmp/pangrova-generate-series-current-20261019.log`，Surefire XML）；`IcebergShowMetadataTest` **5/6 PASS**，唯一失败为 `SHOW PARTITIONS` 缺 Native reader（`/tmp/pangrova-show-metadata-current-r2-20261019.log`）。后一失败有真实 SQL、真实模型夹具和 Native 启动作为前置，不把它记成已闭环。[Iceberg 规范](https://iceberg.apache.org/spec/)和[官方查询文档](https://iceberg.apache.org/docs/1.11.0/spark-queries/)要求 `$partitions` 按当前快照、分区 spec 和 data/delete 文件汇总；现有 `$files` Native 投影没有分区值，不能用无分区键的简单 SQL 聚合代替。

后续 `$entries` 血缘切片把 Iceberg manifest entry 的 data sequence 与 file sequence 分开暴露并校验：Native reader 定向 **11/11 PASS**，其中明确验证两者不同和超前文件序号拒绝；真实 PostgreSQL/Lance broker 预检 **1/1 PASS**（`/tmp/pangrova-iceberg-file-sequence-native-focused-20261019.log`、`/tmp/pangrova-lance-broker-current-preflight-20261019.log`）；Connector 绑定 **4/4 PASS**（`/tmp/pangrova-iceberg-file-sequence-connector-r2-20261019.log`）；重封 JNI 后的 `IcebergV3LineageTest` 真 SQL **4/4 PASS**（`/tmp/pangrova-iceberg-file-sequence-e2e-20261019.log`）。同源码 Native Release 全量和 ASan 滚动全覆盖已通过，完整 SDK 嵌套投影仍需实现和验证。

后续提交 `859c1380` 从真 Native/FILE/模型部署重采集了规范 descriptor 夹具，Protocol **447/447 PASS**；提交 `cf767a46` 修复 Worker 关闭与非法任务 400 语义后，Worker 整模块 **1,279/1,279 PASS**、静态 `verify` 通过，且重新封存该提交的 Worker JAR 后，两项独立进程 E2E **2/2 PASS**（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）。前段的 `a1cdcb60` 进程结果仅作历史证据，不冒充最新源码。

| 批次 | 检查点 | 当前缺口和本机验收动作 |
| --- | --- | --- |
| A：同源码基础门禁 | 5 Native Release | 提交 `f87cfd1f` 的连续全量 **4,178/4,178**，后续命名/生命周期增量通过；本轮分区读取涉及的 reader/parser/逻辑类型检查 **17/17**，新真实 SQL 3/3 与原 lineage 4/4 均通过。完整最终全量仍待全部功能闭合后统一执行。 |
| A | 6 Native ASan | 分区读取修改前的同源码基线滚动并集 **4,179/4,179 PASS、0 遗漏**（`/tmp/pangrova-asan-rolling-union-20261020.json`）；本轮受影响 ASan **17/17 PASS**。全部功能修复后仍须按用户要求最终全量；macOS 不覆盖 Linux LeakSanitizer。 |
| A | 7 Java reactor | 提交 `04fadbcf` Coordinator 整模块 3,213/3,213 通过；提交 `a1cdcb60` 完整 Java reactor 无测试安装、Planner/Connector/Coordinator 定向及受影响静态 `verify` 通过；提交 `cf767a46` Worker 1,279/1,279 与静态 `verify` 通过。最终源码仍需完整 `-Pfull-regression verify`。 |
| A | 8 静态/协议/结构 | 提交 `a1cdcb60` 协议夹具从真 Native 重采集后 447/447，通过日志 `/tmp/pangrova-protocol-a1cdcb60-r2-20261019.log`；文档类引用 183 项、E2E 真实性、descriptor、SPI 注解 147 类型、JNI 22 方法对称性及 dead-stub 检查此前通过；最终同源码仍需全 reactor 静态。覆盖率百分比不是验收门禁，JaCoCo 只按需生成报告。 |
| A | 10 隔离 Native owner | 提交 `04fadbcf` 三类各自独立 JVM：5 + 3 + 1 = 9/9，零跳过；Native 协议修改后最终源码还须复验。 |
| B：真实 SQL 与分布式 | 1 ARM64 镜像 | 旧源码镜像已构建；本机功能验收可用独立 JVM，不要求先打包镜像。若要交付镜像，再按交付源码重建。 |
| B | 2 镜像内 JNI/SQL 冒烟 | 本机 JNI/SQL 先以同源码独立进程验证；镜像内资格仅随镜像交付检查。 |
| B | 9 共享 SQL E2E | 完整发现/执行尚未通过；当前提交的 Iceberg `$entries` 4/4、TPC-H Q11–Q22 12/12 已增量通过；196 个 E2E 类继续按首个根因滚动修复。 |
| B | 11 两独立 Worker | 真实 JOIN/shuffle/TopK **1/1 通过**，含不同 PID/端口、两端任务和 drain；提交 `cf767a46` 重封 JAR/Native 后 8 批 JDBC 写入、重启后 JOIN/资源释放 1/1 再通过（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）。同源码推测执行 10 场景仍待全组复验。 |
| B | 12 故障恢复矩阵 | 丢 ACK、未知提交、取消、慢读、进程退出、OOM 等组合仍未完整验收；按单一故障根因增量跑。 |
| C：SQL/存储/写入 | 13 Iceberg REST | 用户决定当前先验 JDBC；非空 REST 发布 CAS/竞态未闭合，列为**暂缓协议范围**。 |
| C | 14 Paimon REST | 生产对象是 **Paimon REST Catalog 协议**，不是 `paimon-rust`；用户决定先验 JDBC，跨服务 CAS/fencing 列为**暂缓协议范围**。 |
| C | 15 历史版本和写入所有权 | Lance/Paimon 旧 snapshot、并发 winner/loser、未知提交及跨 reader 故障矩阵未全闭合。 |
| C | 16 Native 大写入容量 | 已有小规模实值 E2E；容量、spill、失败后的资源归零未全覆盖。 |
| C | 17 唯一 Calcite 前端 | 全 DDL/DML/时间旅行生产路径迁移和可达旧 fallback 清理未完成。 |
| C | 18 `GENERATE_SERIES` | 提交 `65c7dffa` 当前源码真实 SQL/JNI E2E **6/6 PASS**；最终同源码全功能回归仍待纳入最终批次。 |
| C | 19 `COPY INTO` | stage-only 和 `FROM (SELECT …)` 局部通过；URI SQL→可信 split→Native Parquet→Iceberg marker/未知提交对账尚未接通。 |
| C | 20 系统表 | `$files` 新增按 field ID 的真实分区 struct，含改名/新维度 NULL/完整 struct Flight 返回的 SQL/JNI 3/3；`$entries` 的 data/file 序列和 v3 row ID 真 SQL 4/4 通过。SHOW 元数据历史 5/6，`$partitions` 仍缺关系聚合路径；完整 SDK 嵌套列、Paimon `$files`、全逻辑类型 SQL 矩阵和跨文件系统仍待完成。 |
| C | 21 Dynamic Table | 手动 full-refresh generation 已有真实 E2E；逻辑授权、持久 attempt/source pin、失败恢复、GC、调度/CDC 未闭合。 |
| C | 22 DiskANN 生命周期 | 小规模真实构建/检索/GC 局部通过；多 split、无结果和目标规模质量/成本待验。 |
| D：AI/Feature/CDC | 23 独立模型服务 | 提交 `cf767a46` 重封 JAR/Native 后预置真实模型的独立双 Worker HTTP/SQL、分别单独调用、重启和审计 1/1 再通过（`/tmp/pangrova-independent-processes-cf767a46-20261019.log`）；异步批量、取消、乱序、超时、未知完成、fencing 未全闭合。 |
| D | 24 Feature/CDC/评估 | 单源 Feature View 与数值评估局部通过；多源 point-in-time、真实 Kafka→sink→重启→SQL 和发布闭环未完成。 |
| E：发布/规模环境 | 3 Linux x86_64 镜像 | 当前无 x86_64 runner；不阻断本机独立 Worker 功能验收，交付 Linux 镜像时必须验。 |
| E | 4 Linux ASan/LSan | 当前无 Linux runner；macOS `detect_leaks=0` 不能代替。 |
| E | 25 TPC-H/写入基准 | 缺指定 Linux SF100/SF1000 与并发环境；本机小样本不外推。 |
| E | 26 向量质量/成本 | 缺 1,000 万/1 亿向量目标规模资源；本机只做正确性和小样本 oracle。 |
| E | 27 多 Worker 扩展/稳定性 | 缺 2 Coordinator、4–8 Worker 的指定规模环境；本机独立进程验证功能与故障，不冒充吞吐、P99 或长稳验收。 |

**计数与执行次序：**27 项中 #5/#6 的分区读取修改前基线已完整验证（Release 连续全量、ASan 滚动并集），本轮改动 Release/ASan 各 17/17 增量通过；**16 项本机功能检查点**仍需实现、扩面或最终回归（#7–#12、#15–#24）。#1/#2 是 2 项条件性的镜像交付检查，按用户要求不阻断本机独立进程功能验收；2 项 REST 协议暂缓（#13/#14），5 项需要当前没有的发布/规模环境（#3/#4/#25–#27）。A 的轻量规则检查可与 B 的共享 E2E 并行；Native ASan、完整 E2E 和其他高内存容器批次不能同时挤占本机 16 GiB RAM/Docker VM。B 的首个失败修复后滚动扩面，C/D 按独立纵切进行，最后在**同一最终源码**做一次完整功能回归。每批记录实测数量、失败根因、日志、commit 和磁盘余量；不把未运行的测试记作通过。
