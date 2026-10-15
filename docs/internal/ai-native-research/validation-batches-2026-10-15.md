# 2026-10-15 未闭合验收清单与滚动批次

本清单按**独立验收检查点**计数，共 6 批、27 项。它不把某个局部测试通过等同于整条链通过；“已知缺口”表示现有实现或安全前置条件尚不满足，须先修复再验收。每项通过证据需绑定提交、依赖/协议、Native 或镜像指纹、实际执行命令、测试发现数、失败/错误/跳过数及日志。历史结果见 [validation-current.md](validation-current.md)，原始目标见仓库外的当前计划；最终生产资格仍以 `.pangrova/rules/build-pipeline.md`、`ci-gates.md` 为准。

2026-10-16 盘点：按下表的限定验收范围，已完成 3 项（#5 Mac Native Release、#6 Mac ASan 地址检查、#10 三个隔离 Native runtime owner），仍开放 24 项：第一批 4、第二批 2、第三批 3、第四批 4、第五批 8、第六批 3。`785dd3f9` 干净归档的 ARM64 Bake 曾成功加载，但独立进程启动暴露 SLF4J 2 provider 缺失、Jersey 1/4 shaded ASM 冲突和 Coordinator 主线程过早退出。`1eb68802` 已提交/推送这三项根因修复，干净归档的新镜像已完整 Bake/加载：`pangrova/pangrova:verify-1eb68802`、image ID `sha256:c2702ed0a8ed882ebbc37084ceb4764efb81ab016f9754c6eb98fced774125b8`（ARM64，860,063,655 B；`/tmp/pangrova-arm64-final-bake-1eb68802.log`）。Coordinator 已保持前台，健康端点返回真实 JSON；独立 E2E 首轮发现测试容器先要求 Coordinator ready=200 才启动 Worker，而该状态必须等待 Worker/Catalog，形成启动门禁死锁；测试组件已修、最近测试 6/6，独立 E2E 正滚动复验。镜像仍需与本批后续功能修复同源重建，不能计作最终生产验收。用户将验收重心明确为功能覆盖与端到端通过；强制覆盖率和重复 CI 门禁已从 `785dd3f9` 移除，行为/集成/安全检查保留。各批可并行做互不争用的代码审查和研究；同一 Docker VM 的镜像构建与重型容器 E2E 顺序执行，避免资源竞争造成伪失败。磁盘按批检查，仅清理明确由本任务生成且已失效的产物。

当前已通过但不能替代以下开放检查点：Mac Native Release/ASan 地址检查各 4,138/4,138（ASan 设置 `detect_leaks=0`）；JDK 17 Worker 1,433/1,433、Coordinator 先前 3,098/3,098；全 reactor 静态从起点退出 0；Gravitino Iceberg REST 空表 DDL/读/删除 1/1；DynamicTable 直读重写 5/5；真实 Ollama Worker HTTP 13/13 与共享 SQL E2E 13/13。它们各有源码批次和作用范围，不能合并成“生产全绿”。

## 第一批：Linux 可部署产物（4 项，进行中）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 1 | ARM64 正式 Bake 镜像 | `1eb68802` 干净归档完整 Bake/加载已通过，image ID `sha256:c2702ed0a8ed882ebbc37084ceb4764efb81ab016f9754c6eb98fced774125b8`，镜像 860,063,655 B；运行内容已确认仅 Jersey 4 server、匹配的 Logback 1.4.14/SLF4J 2.0.9。后续功能修复合并后须以最终同源码再 Bake。 |
| 2 | 镜像内 Native/JNI 与 SQL 冒烟 | 新镜像中 Coordinator 已在 JRE17 前台持续运行，真实 `/v1/health` 返回 JSON HTTP 503（`http=true`、尚无 Worker/Catalog），证实 HTTP 路由已可用。独立测试组件原先先等 HTTP 200 再启 Worker 形成启动死锁；组件修复后正重跑 SQL→Native→Arrow/Flight→客户端，结果未出。 |
| 3 | Linux x86_64 生产镜像 | 在 x86_64 runner 按同一 Bake 配方构建、启动并运行 Native/JNI/SQL；本机 ARM64 通过不能替代。 |
| 4 | Linux ASan/LSan | 同源码、同依赖和真实夹具的 Linux 全量 sanitizer，地址错误与泄漏均为 0；Mac `detect_leaks=0` 的 4,153 项不证明 LSan。 |

## 第二批：当前源码模块和质量门禁（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 5 | 最终 Native Release 全量 | 当前 `91bc121c` 同源码 Mac Release 在真实 PostgreSQL Lance broker 下 4,138/4,138 PASS、0 失败/跳过，耗时 600.34 秒；日志 `/tmp/pangrova-native-release-current-full-r2.log`。这是 Mac Native 门禁，不替代 Linux 镜像。 |
| 6 | 最终 Native ASan 全量 | 当前源码 ASan 增量构建 PASS，真实 PostgreSQL Lance broker 下 4,138/4,138 PASS、0 失败/跳过，耗时 1,674.73 秒（`/tmp/pangrova-native-asan-current-full-r1.log`）；Mac `detect_leaks=0` 只检查地址问题，LSan 仍见第 4 项。 |
| 7 | JDK 17 全 reactor 完整回归 | 最终同源码执行 `-Pfull-regression verify`，记录各模块实际发现数与 0 失败/错误/非预期跳过；本轮 Worker 1,433 项已过，但之前全 reactor E2E 遇 Docker VM 级联失败。 |
| 8 | 静态、协议与结构门禁 | `9b85f68e` 源码全 reactor `-Pstatic-analysis -DskipTests verify` 从起点退出 0（`/tmp/pangrova-reactor-static-current-r2.log`）；首轮发现 Iceberg 跨行 `if` 缺大括号，最近模块检查通过后全量重跑。最终 descriptor、SPI/JNI、文档引用、E2E 真实性及适用脚本仍须同源码完成。 |

## 第三批：共享与独立进程 E2E（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 9 | 完整共享 JVM E2E | `pangrova-test-e2e` 全部适用类、零失败/错误/跳过并记录实际测试数；先前 1,089 项运行受 VM 崩溃连锁污染，不能计通过。 |
| 10 | 三个隔离 Native runtime owner | 当前源码三个类各独立 JVM 重跑：`WorkerBootstrapE2ETest` 5/5、`NativeExchangeTransportExternalIntegrationTest` 3/3、`WorkerShuffleChannelAdmissionIntegrationTest` 1/1，均 0 失败/错误/跳过；日志分别为 `/tmp/pangrova-worker-bootstrap-owner-current-r1.log`、`/tmp/pangrova-exchange-external-owner-current-r1.log`、`/tmp/pangrova-shuffle-admission-owner-current-r1.log`。仍需第 11 项的镜像双进程搬运。 |
| 11 | 两个独立 Worker 的推测执行/交换 | `1eb68802` 新 image ID 的首轮测试揭示测试容器 Coordinator readiness=200 与实际启动顺序闭环（`/tmp/pangrova-independent-final-1eb68802-jdk17.log`）；门禁已改为严格验证 Coordinator HTTP 服务可达，再启动 Worker，最近测试 6/6 通过。正在同一 image ID/JDK17 重跑 10 项真实搬运、winner fencing 与资源归还（`/tmp/pangrova-independent-final-1eb68802-jdk17-r2.log`）。 |
| 12 | 故障恢复矩阵 | 真实 PG、对象存储、Flight/Toxiproxy 和独立进程验证接管、迟到 RPC、丢 ACK、未知提交、取消、慢读、OOM 与最后 owner 退出；局部用例已过，组合矩阵未全闭合。 |

## 第四批：外部 Catalog 和真实写入（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 13 | Gravitino Iceberg REST CRUD | 已有空表 DDL/读/删除 1/1；非空 INSERT/查询/删除 2 项被声明栅栏拒绝。需服务端原子完整 metadata CAS 或等价并发前置条件，不能撤掉保护后算通过。 |
| 14 | 独立 Paimon REST 服务 | Apache `paimon-rust` v0.4.0 正式源码服务（固定提交 `29727bfe`）与 Paimon Java 1.4.1 的非空建表/INSERT/SELECT/DROP E2E 已 **2/2 通过、0 失败/错误/适用跳过**（`/tmp/pangrova-paimon-rest-v0.4.0-e2e-r4.log`）；同一类的 3 个 Iceberg 用例因本轮未配置独立 Iceberg 服务而未运行，不计入 Paimon 通过数。修复了 REST JSON Content-Type、测试端鉴权配置、SDK 可选 audit `owner=null` 的冻结句柄表示，以及当前 schema 选项比较；此前 E2E 的旧断言误把 `ColumnValue.LongValue` 与原始 `Long` 比较，已改用 `TypedRow` 类型访问器并增加全部列值断言。Paimon 模块 341/341 与静态检查通过。该 Rust 本地文件服务不校验提交 table UUID，缺生产并发栅栏；尚需具有真实 CAS/fencing 的独立服务、冲突/未知提交故障矩阵及最终同源回归，故本检查点未关闭。 |
| 15 | Lance/Paimon 历史版本与写入所有权 | 用真实旧 snapshot 内容、并发 winner/loser、未知提交恢复、取消/drain 和跨 reader 验证最终可见性；已有局部 Lance/Paimon 结果不构成完整矩阵。 |
| 16 | 大批次 Native 写入容量 | 原 300 行 VECTOR 与新增 1000×16 VECTOR 单语句写入的真实 SQL/JNI/Iceberg E2E 2/2 通过，后者确认实值读取与 Native 活跃池/当前用量归位；容量上限、spill、失败路径资源归零仍待独立故障验证，不能凭这两项推断生产规模。 |

## 第五批：已知核心功能缺口（8 项，先实现后验收）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 17 | 唯一 Calcite SQL 前端 | 生产 DDL/DML/时间旅行等全部迁移并删除可达 ANTLR/fallback；逐形式语义、计划与调用链证明。 |
| 18 | `GENERATE_SERIES` 真 Native 表函数 | 裸调用、二/三参及极值经 Substrait 表函数关系到 Velox operator；零输入 source 的 Velox 多 driver 重复生成已修复，Native 聚焦 191/191、真实 SQL/JNI E2E 6/6 通过。仍需最终同源 Linux 镜像/独立进程及全量回归，不能仅凭本机局部结果关闭生产检查点。 |
| 19 | `COPY INTO` 真文件加载 | 无文件选项的 `FROM (SELECT …)` 已接通现有 INSERT SELECT 路径，真实 SQL/JNI/Iceberg 5/5 E2E 通过；URI 仅 fail closed。仍需授权 URI/文件快照、格式读取、幂等文件身份与提交 journal、真实对象存储数据核对；旧空成功断言不算验收。 |
| 20 | 剩余系统表 | Iceberg `$files/$entries/$manifests/$partitions`、Paimon `$schemas/$branches/$files` 等冻结句柄、typed split、Native reader 与 SQL 实值；`$snapshots/$tags/$options` 的局部结果不能替代。 |
| 21 | Dynamic Table 真物化刷新 | 接通 RefreshCoordinator、增量 split dispatch、原子版本与物理扫描量；已过的 5/5 仅是直读重写，刷新前亦可见新行。 |
| 22 | 索引完整生命周期与 DiskANN | scalar 索引持久授权、Iceberg generation snapshot refs、READY/REBUILD/GC；向量 DiskANN 真 ANN operator、过滤与 Recall，不能以候选数据或旧算术性能断言替代。 |
| 23 | 独立模型服务异步执行 | 当前 `91bc121c` 用本机完整 Ollama 模型缓存重跑 Worker 三类真实 HTTP 集成测试 13/13 PASS（`/tmp/pangrova-worker-ai-cache-focused-r1.log`），另有 SQL→Coordinator→Worker/JNI/Native→真实 Ollama→结果的共享 E2E 13/13 PASS（`/tmp/pangrova-ollama-sql-e2e-current-r1.log`），两者均 0 失败/错误/跳过。独立服务进程、批量异步 RPC/Arrow Flight、超时/取消/乱序/未知完成和 fencing 矩阵仍未闭合。 |
| 24 | Feature Store、CDC 与评估闭环 | 同一物理计划的 point-in-time join、watermark、版本物化、训练导出和真实指标计算/发布；当前元数据或单路径测试不能证明全链。 |

## 第六批：性能和发布资格（3 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 25 | TPC-H 与写入基准 | Linux x86_64 的 SF100/SF1000、22 查询、并发 1/8/32 与 append/upsert/delete/overwrite；同资源冻结基线、原始样本和逐查询回归门槛。小规模 TPC-H 22/22 不可外推。 |
| 26 | 向量检索质量/成本 | 1,000 万/1 亿向量、128/768 维、过滤/Top-K/混合检索；Recall@10 ≥0.95 时比较延迟、吞吐、内存和成本。 |
| 27 | 多 Worker 扩展及稳定性 | 指定 2 Coordinator、4→8 Worker、32 vCPU/128 GiB/NVMe/10 Gb 环境；每条件 ≥1,000 完成请求、3 独立窗口，冷/热、前后台干扰、P99、扩展效率与长时故障恢复。当前本机规格不能代替。 |

执行次序：第一批已在跑，随后按第二至第六批做最小失败复现、根因修复、最近测试和所属模块扩面。某项因实现或环境缺口阻断时记录准确状态，继续同批独立项。第五批任何生产实现变化后，重新运行第二、第三、第四批受影响链；所有功能闭合后再做一次全量门禁和 E2E，不复用旧源码的“通过”结论。Docker VM 每轮检查可用空间；只清理已失效归档、临时容器和临时 swap，保留复用成本最高的已通过构建层及可复测模型缓存。
