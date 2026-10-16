# 2026-10-15 未闭合验收清单与滚动批次

本清单按**独立验收检查点**计数，共 6 批、27 项。它不把某个局部测试通过等同于整条链通过；“已知缺口”表示现有实现或安全前置条件尚不满足，须先修复再验收。每项通过证据需绑定提交、依赖/协议、Native 或镜像指纹、实际执行命令、测试发现数、失败/错误/跳过数及日志。历史结果见 [validation-current.md](validation-current.md)，原始目标见仓库外的当前计划；最终生产资格仍以 `.pangrova/rules/build-pipeline.md`、`ci-gates.md` 为准。

2026-10-16 盘点：历史批次曾完成 3 项（#5 Mac Native Release、#6 Mac ASan 地址检查、#10 三个隔离 Native runtime owner）；这些结果早于本轮 Native/协议/执行源码变化，最终同源码资格仍为 0/27，不能把历史 3 项算进最终通过数。`668aa77a` 干净归档的 ARM64 镜像 `pangrova/pangrova:verify-668aa77a` 已完整 Bake/加载，image ID `sha256:381696b0abc77b14212c910695f4d4b32ce5b382cc6d696fbeabb02258c32e47`（861,013,799 B；`/tmp/pangrova-arm64-final-bake-668aa77a.log`）；独立双 Worker 推测执行首轮 Surefire 10 run/0 failure/10 error/0 skip（`/tmp/pangrova-independent-spec-668aa77a-r1.log`）。实际运行暴露 Worker `/v1/health/live` 被鉴权拦截为 401，以及 8,053,064 行聚合的 Native Flight shuffle spill 容量错误。健康鉴权精确修复 `9ba739f9` 已推送，真 HTTP 最近测试 1/1、Worker 静态检查退出 0。Shuffle 多批共享匿名段与 byte/file 区分诊断修复 `2a752251` 已推送，最近 Native 聚焦 71/71（其中 Shuffle 29/29）通过。随后包含这两项和 Iceberg `$files` 六列的 `4095f352` 干净归档已完整构建 ARM64 镜像 `pangrova/pangrova:verify-4095f352`，image ID `sha256:5569cac9ef3bee35d5cc9390814e1599fca89bceef8f16b0703a65862d580334`（`/tmp/pangrova-arm64-bake-4095f352.log`）；独立双 Worker 单例首轮在测试 SQL 准备阶段因 Worker 已注册但首次资源心跳未到而报调度容量不足，1 run/0 failure/1 error/0 skip（`/tmp/pangrova-independent-spec-4095f352-single-r1.log`）。该失败尚未执行到 8,053,064 行 shuffle，不能解释为修复通过或回归。 首次心跳与 `$files` 九列增量进入 `0b9d582a` 干净归档，ARM64 正式 Bake/加载完成，镜像 `pangrova/pangrova:verify-0b9d582a`、ID `sha256:13bbb111f45c5915142b6779bfe44c731888175920b02005e66e3370e980c874`、JNI SHA-256 `fb27d826fb4bf45aa463e0114a0cd09c52c588b5da78c33d40147c381bf3b9ba`（`/tmp/pangrova-arm64-bake-0b9d582a.log`）；同源 JDK17 install 退出 0。双 Worker 先前失败单例已进入真实 Native 任务，仍 1 run/0 failure/1 error/0 skip（`/tmp/pangrova-independent-spec-0b9d582a-single-r1.log`）：4 个 split 被压为 1 个 source task，终态报告暂扣时同 stage 完成率为 0，达不到当前 ≥75% straggler 检测前置，90 秒内没有 speculative clone。查询取消/报表重试是后续连锁；不能把这次当做 shuffle 修复已通过。用户将验收重心明确为功能覆盖与端到端通过；强制覆盖率和重复 CI 门禁已移除，行为/集成/安全检查保留。互不争用的模块可分别滚动验证；同一 Docker VM 的镜像构建与重型容器 E2E 顺序执行。磁盘按批检查，仅清理本任务生成且已失效的产物。

2026-10-16 最新增量：`bed0b815` 已把冻结 Iceberg 数据文件数传至 source stage 并按 work unit 决定 task 数，JDK17 SPI 6/6、Planner 6/6、真实 Iceberg 5/5 与受影响静态检查通过；同源 ARM64 Bake 镜像 `pangrova/pangrova:verify-bed0b815`（ID `sha256:0fab5ca5723c566ac739824a37beaa6932564ee836d79459ff91ce79d6c8ee6b`）的双 Worker 单例实测 `splits=4 tasks=4`，但 1 run/0 failure/1 error/0 skip，90 秒内仍无 clone（`/tmp/pangrova-independent-spec-bed0b815-single-r1.log`）。进一步定位是原始任务 RUNNING 未记入推测调度计时，已完成 peer 的耗时又随当前时间增长；修复 `0352f5af` 已推送，在干净 `bed0b815` 归档叠加该 5 文件的 JDK17 定向测试 33/33、Coordinator 单模块静态检查退出 0，随后安排从 `0352f5af` 干净归档重建镜像并重跑单例。`ca168288` 修复 Dynamic Table 增量 merge 失败后错误地把 delta 当全量发布，JDK17 定向 20/20、受影响静态检查通过；真实物化刷新仍开放。`9c5ee599` 补充 HadoopCatalog 仅用于本地夹具、生产需要 metastore 指针 CAS 的准入记录。同期 `COPY INTO` URI 票据和 `$files.column_sizes` 仍有协议/Native 未提交修改，故以上局部结果均不计入最终同源码 27 项验收。

`0352f5af` 干净归档正式 ARM64 Bake/加载退出 0，镜像 `pangrova/pangrova:verify-0352f5af`、ID `sha256:9b074c9df3d573121001e615e3297b480e7528c9a69e28b69e22e889b07273b7`，镜像 Native `.so` SHA-256 `fb27d826fb4bf45aa463e0114a0cd09c52c588b5da78c33d40147c381bf3b9ba`，日志 `/tmp/pangrova-arm64-bake-0352f5af.log`；同源隔离 JDK17 `-am -DskipTests install` 退出 0。先前失败的独立双 Worker 推测单例终于 1 run/0 failure/0 error/0 skip（Surefire XML，`/tmp/pangrova-independent-spec-0352f5af-single-r1.log`）：真实 stage1 `splits=4 tasks=4`，`speculative_dispatch_success` 发往 `e2e-worker-1`，原任务在 `e2e-worker-2`，Native 终态与 SQL 结果均通过。它仅关闭该单例；同组剩余九例和最终源码回归仍开放。本轮只删除 6 个已失效的本任务 `/tmp` 源码归档/Maven 缓存，未动当前归档和日志，本机空间约从 86 GiB 回升到 90 GiB。

本机可执行的并行滚动批次：A 为 `COPY INTO` 权限/票据/MinIO/Worker 闭环，B 为 `$files.column_sizes` 的 Java、Native Release/ASan、真实 SQL，C 为独立双 Worker 镜像推测执行。A、B 的代码和定向测试可并行；C 的干净归档准备可并行，但 A 的容器测试与 C 的 Docker Bake/E2E 要依次占用同一 Docker VM。A/B 稳定后统一重采真实 Native/JNI capability fixture，随后跑受影响协议和跨语言链；最后才执行 #5–#10 的同源码全量回归。#3、#4、#25–#27 仍需 Linux x86_64 或规模环境；用户当前没有可接入环境，不能以 ARM64 本机结果代替。

当前已通过但不能替代以下开放检查点：Mac Native Release/ASan 地址检查各 4,138/4,138（ASan 设置 `detect_leaks=0`）；JDK 17 Worker 1,433/1,433、Coordinator 先前 3,098/3,098；全 reactor 静态从起点退出 0；Gravitino Iceberg REST 空表 DDL/读/删除 1/1；DynamicTable 直读重写 5/5；真实 Ollama Worker HTTP 13/13 与共享 SQL E2E 13/13。`$files` 九列增量当前已通过 Native 6/6 和 SQL E2E 1/1（包含真实 DATA/position/equality DELETE），Worker 首次心跳修复当前已通过最近 11/11 和 E2E 可调度谓词 2/2；二者均尚未进入同源码镜像独立验收。它们各有源码批次和作用范围，不能合并成“生产全绿”。

## 第一批：Linux 可部署产物（4 项，进行中）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 1 | ARM64 正式 Bake 镜像 | `0352f5af` 干净归档已正式 Bake/加载，image ID `sha256:9b074c9df3d573121001e615e3297b480e7528c9a69e28b69e22e889b07273b7`，同源 JDK17 install 与双 Worker 推测单例 1/1 通过；最终功能源码仍在变更，需在全部功能闭合后同源重建。 |
| 2 | 镜像内 Native/JNI 与 SQL 冒烟 | `0352f5af` 正式 ARM64 镜像的独立双 Worker SQL/Native 推测执行单例 1/1 通过，实际跨 Worker clone 与结果已验证；完整 activation smoke 和最终功能源码同源镜像仍待验收。 |
| 3 | Linux x86_64 生产镜像 | 在 x86_64 runner 按同一 Bake 配方构建、启动并运行 Native/JNI/SQL；本机 ARM64 通过不能替代。 |
| 4 | Linux ASan/LSan | 同源码、同依赖和真实夹具的 Linux 全量 sanitizer，地址错误与泄漏均为 0；Mac `detect_leaks=0` 的 4,153 项不证明 LSan。 |

## 第二批：当前源码模块和质量门禁（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 5 | 最终 Native Release 全量 | 历史 `91bc121c` 同源码 Mac Release 在真实 PostgreSQL Lance broker 下 4,138/4,138 PASS、0 失败/跳过，耗时 600.34 秒；日志 `/tmp/pangrova-native-release-current-full-r2.log`。这是 Mac Native 门禁，不替代 Linux 镜像。 |
| 6 | 最终 Native ASan 全量 | 较早源码 ASan 增量构建 PASS，真实 PostgreSQL Lance broker 下 4,138/4,138 PASS、0 失败/跳过，耗时 1,674.73 秒（`/tmp/pangrova-native-asan-current-full-r1.log`）；Mac `detect_leaks=0` 只检查地址问题，LSan 仍见第 4 项。 |
| 7 | JDK 17 全 reactor 完整回归 | 最终同源码执行 `-Pfull-regression verify`，记录各模块实际发现数与 0 失败/错误/非预期跳过；本轮 Worker 1,433 项已过，但之前全 reactor E2E 遇 Docker VM 级联失败。 |
| 8 | 静态、协议与结构门禁 | `9b85f68e` 源码全 reactor `-Pstatic-analysis -DskipTests verify` 从起点退出 0（`/tmp/pangrova-reactor-static-current-r2.log`）；首轮发现 Iceberg 跨行 `if` 缺大括号，最近模块检查通过后全量重跑。最终 descriptor、SPI/JNI、文档引用、E2E 真实性及适用脚本仍须同源码完成。 |

## 第三批：共享与独立进程 E2E（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 9 | 完整共享 JVM E2E | `pangrova-test-e2e` 全部适用类、零失败/错误/跳过并记录实际测试数；先前 1,089 项运行受 VM 崩溃连锁污染，不能计通过。 |
| 10 | 三个隔离 Native runtime owner | 较早源码三个类各独立 JVM 重跑：`WorkerBootstrapE2ETest` 5/5、`NativeExchangeTransportExternalIntegrationTest` 3/3、`WorkerShuffleChannelAdmissionIntegrationTest` 1/1，均 0 失败/错误/跳过；日志分别为 `/tmp/pangrova-worker-bootstrap-owner-current-r1.log`、`/tmp/pangrova-exchange-external-owner-current-r1.log`、`/tmp/pangrova-shuffle-admission-owner-current-r1.log`。仍需第 11 项的镜像双进程搬运。 |
| 11 | 两个独立 Worker 的推测执行/交换 | `668aa77a` 镜像首轮真双 Worker 10 run/0 failure/10 error/0 skip；9 项等待 clone issued 超时、1 项等待 source task issued 超时，链路日志显示 Worker live 探针 401 与真实 `MAX(payload)` Flight spill 容量错误。Worker HTTP 修复 1/1 与 Native 共享 spill 段 GTest 29/29 已通过。修复入镜像后的 `4095f352` 单例 1 run/0 failure/1 error/0 skip，SQL 准备阶段两 Worker 已注册，但第一次资源心跳未到，调度器观察到可用内存为 0；未触及大批次 shuffle。首次真实心跳改为注册后立即发送，测试 SQL 前等待两个 Worker 的真实资源心跳；最近 HeartbeatSender 11/11、可调度谓词 2/2 和受影响 Worker/E2E 静态检查已过（`/tmp/pangrova-worker-first-heartbeat-focused-r2.log`、`/tmp/pangrova-independent-readiness-focused-r1.log`、`/tmp/pangrova-worker-readiness-static-r1.log`）。新镜像 `0b9d582a` 的单例已执行到两个独立 Worker 的真 Native 任务，但 4 个 split 合成 1 个 source task，终态报告暂扣后 stage 完成率为 0，当前 ≥75% 门槛使推测副本永不发出，单例 1 run/0 failure/1 error/0 skip；`bed0b815` 源 work unit 并行度和 `0352f5af` 任务计时修复后，干净 ARM64 镜像双 Worker 单例 1/1 通过，跨 Worker clone、Native 终态和 SQL 结果已实证；该组剩余九例待跑（`/tmp/pangrova-independent-spec-0352f5af-single-r1.log`）。规划代码静态审查另发现该 MAX 的非本地 Aggregate 输入先强制 SINGLETON exchange，且 AggregatePushdownRule 未注册；通用 partial/final 聚合属于独立规模优化缺口，尚待实际 fragment 与性能验证。本项未通过。 |
| 12 | 故障恢复矩阵 | 真实 PG、对象存储、Flight/Toxiproxy 和独立进程验证接管、迟到 RPC、丢 ACK、未知提交、取消、慢读、OOM 与最后 owner 退出；局部用例已过，组合矩阵未全闭合。 |

## 第四批：外部 Catalog 和真实写入（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 13 | Gravitino Iceberg REST CRUD | 已有空表 DDL/读/删除 1/1；非空 INSERT/查询/删除 2 项被声明栅栏拒绝。Iceberg REST 标准断言不覆盖完整旧 metadata 指针、任意 declaration properties/location；客户端预读无法封闭 ALTER 竞态。需服务端在发布指针的同一原子操作内核验冻结声明或完整旧 metadata CAS，并做两客户端竞态/未知提交验收；不能撤掉保护后算通过。协议对照和验收合同见 [iceberg-rest-publication-fence-2026-10-16.md](iceberg-rest-publication-fence-2026-10-16.md)。 |
| 14 | 独立 Paimon REST 服务 | Apache `paimon-rust` v0.4.0 正式源码服务（固定提交 `29727bfe`）与 Paimon Java 1.4.1 的非空建表/INSERT/SELECT/DROP E2E 已 **2/2 通过、0 失败/错误/适用跳过**（`/tmp/pangrova-paimon-rest-v0.4.0-e2e-r4.log`）；同一类的 3 个 Iceberg 用例因本轮未配置独立 Iceberg 服务而未运行，不计入 Paimon 通过数。修复了 REST JSON Content-Type、测试端鉴权配置、SDK 可选 audit `owner=null` 的冻结句柄表示，以及当前 schema 选项比较；此前 E2E 的旧断言误把 `ColumnValue.LongValue` 与原始 `Long` 比较，已改用 `TypedRow` 类型访问器并增加全部列值断言。Paimon 模块 341/341 与静态检查通过。该 Rust 本地文件服务不校验提交 table UUID，缺生产并发栅栏；尚需具有真实 CAS/fencing 的独立服务、冲突/未知提交故障矩阵及最终同源回归，故本检查点未关闭。 |
| 15 | Lance/Paimon 历史版本与写入所有权 | 用真实旧 snapshot 内容、并发 winner/loser、未知提交恢复、取消/drain 和跨 reader 验证最终可见性；已有局部 Lance/Paimon 结果不构成完整矩阵。 |
| 16 | 大批次 Native 写入容量 | 原 300 行 VECTOR 与新增 1000×16 VECTOR 单语句写入的真实 SQL/JNI/Iceberg E2E 2/2 通过，后者确认实值读取与 Native 活跃池/当前用量归位；容量上限、spill、失败路径资源归零仍待独立故障验证，不能凭这两项推断生产规模。 |

## 第五批：已知核心功能缺口（8 项，先实现后验收）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 17 | 唯一 Calcite SQL 前端 | 生产 DDL/DML/时间旅行等全部迁移并删除可达 ANTLR/fallback；逐形式语义、计划与调用链证明。 |
| 18 | `GENERATE_SERIES` 真 Native 表函数 | 裸调用、二/三参及极值经 Substrait 表函数关系到 Velox operator；零输入 source 的 Velox 多 driver 重复生成已修复，Native 聚焦 191/191、真实 SQL/JNI E2E 6/6 通过。仍需最终同源 Linux 镜像/独立进程及全量回归，不能仅凭本机局部结果关闭生产检查点。 |
| 19 | `COPY INTO` 真文件加载 | 无文件选项的 `FROM (SELECT …)` 已接通现有 INSERT SELECT 路径，真实 SQL/JNI/Iceberg 5/5 E2E 通过。V56 持久 Storage Profile/READ 授权与 Worker 精确版本 S3→托管 staging 的真实 PostgreSQL registry 7 项、Coordinator 6 项、Storage Profile SQL 8 项、MinIO 6 项均通过；V57 跨查询不可变文件认领在真实 PostgreSQL 中 8/8 通过（`/tmp/pangrova-copy-claim-pg-r1.log`），0 失败/错误/跳过。V58 的真 PostgreSQL publication/migration 最近 6/6，以及真 PostgreSQL + HadoopCatalog/Iceberg 的 marker 后 PG proof 丢失恢复 1/1（`/tmp/pangrova-v58-pg-1.log`、`/tmp/pangrova-copy-recovery-integration-r1.log`）已通过。上述均是 URI 执行的前置能力；URI SQL→typed split→Native Parquet→Iceberg snapshot marker/未知提交对账尚未接通，仍 fail closed，#19 不关闭；旧空成功断言不算验收。 |
| 20 | 剩余系统表 | Paimon `$schemas` 已接通 Calcite lookup/基表 SELECT 授权、冻结历史 schema 对象、typed split、Native JSON reader 与 SQL 实值；JDK17 SQL→Native 2/2、Native 聚焦 11/11、Coordinator lookup 9/9、授权 46/46、JNI loader 1/1。新增 Paimon 本地文件系统 `$branches` 的 SQL→Planner/授权→typed protobuf split→Java FileIO 目录状态冻结→Native `lstat` 校验/读取→JNI 实值链：真实 SQL E2E 1/1、Java 绑定 9/9、Native 聚焦 5/5、Paimon 全模块 342/342、Protocol 全模块 441/441、Paimon 依赖链静态检查通过（JDK17；`osx/aarch64` 打包 JNI SHA-256 `eb5cf5d35d2dc9099c24795b226f9d55aa65ed5c5d3312b3176e49587badd4d1`）。协议描述符变化后由当前 Native exporter 和真 JNI `GET_CAPABILITIES` 重采两份完整原始 fixture，仅 descriptor/派生 manifest hash 改变，能力函数仍为 19/99。详见 `docs/internal/system-table-native-scan-implementation.md` 和 capability fixture README。本轮新增 Iceberg `$manifests` 的 SQL→Calcite lookup/基表授权→Java v2/v3 snapshot/manifest-list 长度与 SHA-256 冻结→typed protobuf split→Native 有界 Avro OCF/null+deflate 读取与 12 列 typed 输出→Flight/Arrow 客户端链；v1 无 manifest-list、v4 与不受支持的分区 bound 类型保持 fail closed。当前源码 JDK17 Iceberg 绑定 3/3、Planner 6/6、Coordinator 55/55、Native Avro 聚焦 4/4、真实 SQL E2E 2/2（空表、提交后 12 列、分区 `east`/`west` 嵌套界值），均 0 fail/error/skip；本机打包 JNI SHA-256 `85db890051b7cb92f5508c64cf2f93bc81e099deb4033059a5e92bdcb0254e40`。当前 Native exporter/JNI 真启动完整重采 cache-disabled/catalogue 781796 bytes SHA `7e4925c45904cb956e842244faaac6c127dbf3a19e009888346fde65d03ad3c8` 与 cache-enabled/inventory 793767 bytes SHA `e054612c8200373ebc6103d3703f5c32cdec051779233516b80e96c8349d9c13`，textproto 对照仅 descriptor 与派生 manifest hash 改变，仍 19/99 能力/函数。随后同源码 Worker `NativeCapabilityLoaderTest` 6/6 和 `NativeLibraryLoaderTest` 1/1 通过，0 fail/error/skip；这是本机打包 JNI 加载验证，不替代 Linux 镜像。非本地 `$branches` 因 Native 文件状态抽象不足仍 fail closed；Iceberg `$files` 先前六个标量列的 DATA+DELETE 真 SQL 1/1、相邻 `$manifests` 回归 2/2 已通过（`/tmp/pangrova-iceberg-files-manifests-sql-e2e-r3.log`）；本轮加入 `split_offsets`、`equality_ids`、`sort_order_id` 后，JDK17 Java 绑定 6/6、Planner 7/7、Coordinator 55/55、Native 聚焦 6/6、真实 SQL/JNI DATA 与两类 DELETE 的九列 E2E 1/1，均 0 失败/错误/跳过（`/tmp/pangrova-iceberg-files-fixed-sql-e2e-r1.log`）；JNI SHA-256 `f7d17bd20a7ab7b030684b675925cd37e71618e9dff3d7a12017df58a1ca603c`。剩余复杂列在 Calcite lookup 以 `PLANNER-0001` fail closed，`SELECT *` 目前仅声明九列，不宣称官方完整 `$files` schema。`$entries/$partitions`、Paimon `$files`、跨文件系统/分区类型全域验收、独立镜像和最终全量回归未验收，本项不关闭。 |
| 21 | Dynamic Table 真物化刷新 | `751f6461` 已使刷新只接受匹配 checkpoint 的终态 Native commit outcome，并在状态推进前重检 epoch/lease；`4ccefccc` 把不支持读取 commit property 从“空值”改为明确失败，防止无法核验时重复 APPEND。JDK17 受影响 7 类 48/48、0 失败/错误/跳过（`/tmp/pangrova-dt-spi-broader-r3.log`）；前一变更 24/24 与受影响静态门禁通过。该用例集仍采用历史测试替身，不能充当生产提交证据；需在真实 connector 上完成失败与恢复验收。仍需真实 Worker 完成 receipt、原子 generation 发布、增量 split dispatch 与物理扫描量；已过的 5/5 仅是直读重写，刷新前亦可见新行，本项不关闭。 |
| 22 | 索引完整生命周期与 DiskANN | scalar 索引持久授权、Iceberg generation snapshot refs、READY/REBUILD/GC；向量 DiskANN 真 ANN operator、过滤与 Recall，不能以候选数据或旧算术性能断言替代。 |
| 23 | 独立模型服务异步执行 | 本机 E2E 目录模型缓存下，当前源码 Worker 三类真实 HTTP 集成测试重新通过 13/13、0 fail/error/skip（`/tmp/pangrova-worker-ollama-shared-cache-r4.log`；JDK17；三份 Surefire XML 分别 4/4、6/6、3/3）；先前 SQL→Coordinator→Worker/JNI/Native→真实 Ollama→结果的共享 E2E 13/13 PASS（`/tmp/pangrova-ollama-sql-e2e-current-r1.log`）属于较早源码，不能充当最终同源码证明。独立服务进程、批量异步 RPC/Arrow Flight、超时/取消/乱序/未知完成和 fencing 矩阵仍未闭合。 |
| 24 | Feature Store、CDC 与评估闭环 | 同一物理计划的 point-in-time join、watermark、版本物化、训练导出和真实指标计算/发布；当前元数据或单路径测试不能证明全链。 |

`$files` 协议变更后的能力目录已从当前 Native exporter 和真 JNI 分别重采完整原字节：781,796 B / SHA-256 `aad551fd052232e7a9ea32967ab2e35275e427f3c9dbad6a9a65a072053e162e`，793,767 B / SHA-256 `142db04be66d60da60c1e5bf648e2f6ef8bac40af8d5b531a5c69c52e4ef930b`；打包 JNI SHA-256 `495ffe335405fbead2c124d4cce728440015dc07b0a4f7034ffa17be4dde8af6`。textproto 对照仅协议描述符、function catalogue 描述符和派生 manifest hash 改变；当前 Protocol 全模块 441/441、0 失败/错误/跳过（`/tmp/pangrova-iceberg-files-protocol-full-r1.log`）。

## 第六批：性能和发布资格（3 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 25 | TPC-H 与写入基准 | Linux x86_64 的 SF100/SF1000、22 查询、并发 1/8/32 与 append/upsert/delete/overwrite；同资源冻结基线、原始样本和逐查询回归门槛。小规模 TPC-H 22/22 不可外推。 |
| 26 | 向量检索质量/成本 | 1,000 万/1 亿向量、128/768 维、过滤/Top-K/混合检索；Recall@10 ≥0.95 时比较延迟、吞吐、内存和成本。 |
| 27 | 多 Worker 扩展及稳定性 | 指定 2 Coordinator、4→8 Worker、32 vCPU/128 GiB/NVMe/10 Gb 环境；每条件 ≥1,000 完成请求、3 独立窗口，冷/热、前后台干扰、P99、扩展效率与长时故障恢复。当前本机规格不能代替。 |

执行次序：第一批已在跑，随后按第二至第六批做最小失败复现、根因修复、最近测试和所属模块扩面。某项因实现或环境缺口阻断时记录准确状态，继续同批独立项。第五批任何生产实现变化后，重新运行第二、第三、第四批受影响链；所有功能闭合后再做一次全量门禁和 E2E，不复用旧源码的“通过”结论。Docker VM 每轮检查可用空间；只清理已失效归档、临时容器和临时 swap，保留复用成本最高的已通过构建层及可复测模型缓存。
