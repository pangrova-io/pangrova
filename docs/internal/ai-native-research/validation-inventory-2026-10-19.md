---
title: "AI Native 生产验收剩余清单与滚动批次（2026-10-19）"
description: "按当前主分支源码区分已通过、本机待验证、功能未闭合和外部发布环境；不复用旧源码的全绿结论。"
---

# AI Native 剩余验证清单

本清单以 `codex/ai-native-semantic-closure` 为唯一交付分支，沿用[27 项验收定义](validation-batches-2026-10-15.md)。当前源码的 macOS ARM64 Native Release 加真实 PostgreSQL/Lance broker **4,175/4,175 PASS**（`/tmp/pangrova-integrated-native-ctest-20261019.log`）；受影响 Native **272/272 PASS**，独立双 Worker SQL/JNI E2E **1/1 PASS**（`/tmp/pangrova-integrated-two-worker-e2e-r3-20261019.log`）。仓库当前发现 **195 个 `*E2ETest.java` 类**；完整共享 E2E、隔离 owner 类、全 Java reactor 和同源码 ASan 尚不能宣称通过。曾经通过的旧提交、其他工作树或其他镜像仅是历史证据。

| 批次 | 检查点 | 当前缺口和本机验收动作 |
| --- | --- | --- |
| A：同源码基础门禁 | 5 Native Release | **本机通过**，4,175/4,175；源码变化涉及 Native 时重跑受影响测试，最终功能封版后再做一次全量。 |
| A | 6 Native ASan | 当前集成源码未跑全量 ASan；使用匹配的 Velox、Folly、Protobuf 插桩依赖重建并运行。 |
| A | 7 Java reactor | 当前集成源码只完成编译安装和部分模块/定向测试；跑完整 `-Pfull-regression verify`，逐模块记录实际发现数。 |
| A | 8 静态/协议/结构 | 当前集成源码静态曾定向通过；最终同源码执行静态、descriptor、SPI/JNI、文档和 E2E 真实性门禁。覆盖率百分比不是验收门禁，JaCoCo 只按需生成报告。 |
| A | 10 隔离 Native owner | 三类 Native runtime owner 测试须各用独立 JVM 在当前源码重跑。 |
| B：真实 SQL 与分布式 | 1 ARM64 镜像 | 旧源码镜像已构建；本机功能验收可用独立 JVM，不要求先打包镜像。若要交付镜像，再按交付源码重建。 |
| B | 2 镜像内 JNI/SQL 冒烟 | 本机 JNI/SQL 先以同源码独立进程验证；镜像内资格仅随镜像交付检查。 |
| B | 9 共享 SQL E2E | 完整发现/执行尚未通过；当前 195 个 E2E 类正在滚动运行，按首个根因修复。 |
| B | 11 两独立 Worker | 真实 JOIN/shuffle/TopK **1/1 通过**，含不同 PID/端口、两端任务和 drain；同源码推测执行 10 场景仍待全组复验。 |
| B | 12 故障恢复矩阵 | 丢 ACK、未知提交、取消、慢读、进程退出、OOM 等组合仍未完整验收；按单一故障根因增量跑。 |
| C：SQL/存储/写入 | 13 Iceberg REST | 用户决定当前先验 JDBC；非空 REST 发布 CAS/竞态未闭合，列为**暂缓协议范围**。 |
| C | 14 Paimon REST | 生产对象是 **Paimon REST Catalog 协议**，不是 `paimon-rust`；用户决定先验 JDBC，跨服务 CAS/fencing 列为**暂缓协议范围**。 |
| C | 15 历史版本和写入所有权 | Lance/Paimon 旧 snapshot、并发 winner/loser、未知提交及跨 reader 故障矩阵未全闭合。 |
| C | 16 Native 大写入容量 | 已有小规模实值 E2E；容量、spill、失败后的资源归零未全覆盖。 |
| C | 17 唯一 Calcite 前端 | 全 DDL/DML/时间旅行生产路径迁移和可达旧 fallback 清理未完成。 |
| C | 18 `GENERATE_SERIES` | 本机真实 SQL/JNI 与 Native 定向曾通过；当前源码完整 E2E/回归待纳入最终批次。 |
| C | 19 `COPY INTO` | stage-only 和 `FROM (SELECT …)` 局部通过；URI SQL→可信 split→Native Parquet→Iceberg marker/未知提交对账尚未接通。 |
| C | 20 系统表 | `$files`/`$manifests` 等局部实值通过；Iceberg `$entries`/`$partitions`、Paimon `$files` 和复杂列/跨文件系统仍未完整验收。 |
| C | 21 Dynamic Table | 手动 full-refresh generation 已有真实 E2E；逻辑授权、持久 attempt/source pin、失败恢复、GC、调度/CDC 未闭合。 |
| C | 22 DiskANN 生命周期 | 小规模真实构建/检索/GC 局部通过；多 split、无结果和目标规模质量/成本待验。 |
| D：AI/Feature/CDC | 23 独立模型服务 | 真实模型 HTTP/SQL 局部通过；异步批量、取消、乱序、超时、未知完成、fencing 未全闭合。 |
| D | 24 Feature/CDC/评估 | 单源 Feature View 与数值评估局部通过；多源 point-in-time、真实 Kafka→sink→重启→SQL 和发布闭环未完成。 |
| E：发布/规模环境 | 3 Linux x86_64 镜像 | 当前无 x86_64 runner；不阻断本机独立 Worker 功能验收，交付 Linux 镜像时必须验。 |
| E | 4 Linux ASan/LSan | 当前无 Linux runner；macOS `detect_leaks=0` 不能代替。 |
| E | 25 TPC-H/写入基准 | 缺指定 Linux SF100/SF1000 与并发环境；本机小样本不外推。 |
| E | 26 向量质量/成本 | 缺 1,000 万/1 亿向量目标规模资源；本机只做正确性和小样本 oracle。 |
| E | 27 多 Worker 扩展/稳定性 | 缺 2 Coordinator、4–8 Worker 的指定规模环境；本机独立进程验证功能与故障，不冒充吞吐、P99 或长稳验收。 |

**计数与执行次序：**27 项中 1 项当前源码本机全量通过（#5），19 项本机仍需验证或实现（含镜像交付项 #1/#2 和已局部通过的 #11），2 项 REST 协议按用户决定暂缓（#13/#14），5 项需要当前没有的发布/规模环境（#3/#4/#25–#27）。A 的轻量规则检查可与 B 的共享 E2E 并行；Native ASan、完整 E2E 和其他高内存容器批次不能同时挤占本机 16 GiB RAM/Docker VM。B 的首个失败修复后滚动扩面，C/D 按独立纵切进行，最后在**同一最终源码**做一次完整功能回归。每批记录实测数量、失败根因、日志、commit 和磁盘余量；不把未运行的测试记作通过。
