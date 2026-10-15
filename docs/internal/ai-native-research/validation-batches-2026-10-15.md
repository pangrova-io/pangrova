# 2026-10-15 未闭合验收清单与滚动批次

本清单按**独立验收检查点**计数，共 6 批、27 项。它不把某个局部测试通过等同于整条链通过；“已知缺口”表示现有实现或安全前置条件尚不满足，须先修复再验收。每项通过证据需绑定提交、依赖/协议、Native 或镜像指纹、实际执行命令、测试发现数、失败/错误/跳过数及日志。历史结果见 [validation-current.md](validation-current.md)，原始目标见仓库外的当前计划；最终生产资格仍以 `.pangrova/rules/build-pipeline.md`、`ci-gates.md` 为准。

当前已通过但不能替代以下检查点：Mac Native Release/ASan 地址检查各 4,153/4,153（ASan 设置 `detect_leaks=0`）；JDK 17 Worker 本轮 1,433/1,433、Coordinator 先前 3,098/3,098；全 reactor 静态先前退出 0；Gravitino Iceberg REST 空表 DDL/读/删除 1/1；DynamicTable 直读重写 5/5；真实 Ollama Worker 13/13。它们各有源码批次和作用范围，不能合并成“生产全绿”。

## 第一批：Linux 可部署产物（4 项，进行中）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 1 | ARM64 正式 Bake 镜像 | 从干净 Git archive 完整构建、加载并取得真实 image ID；`cd064b77` 的 r18 代理增量构建正在编译 Lance Rust，Velox/Flight 已缓存；此前各轮没有完整镜像。 |
| 2 | 镜像内 Native/JNI 与 SQL 冒烟 | 核对镜像 Native、Java 包、协议与函数目录指纹，启动真实 Coordinator/Worker，执行 SQL→Native→Arrow/Flight→客户端实值及关闭；必须使用第 1 项同一个 image ID。 |
| 3 | Linux x86_64 生产镜像 | 在 x86_64 runner 按同一 Bake 配方构建、启动并运行 Native/JNI/SQL；本机 ARM64 通过不能替代。 |
| 4 | Linux ASan/LSan | 同源码、同依赖和真实夹具的 Linux 全量 sanitizer，地址错误与泄漏均为 0；Mac `detect_leaks=0` 的 4,153 项不证明 LSan。 |

## 第二批：当前源码模块和质量门禁（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 5 | 最终 Native Release 全量 | 现有 4,153/4,153 属于进入 Linux 生产链接修复前的源码；最终实现稳定后用匹配 fixture 全跑，0 失败/跳过。 |
| 6 | 最终 Native ASan 全量 | 当前同类历史 4,153/4,153 仅地址检查；最后一次 Native 改动后重新全跑，并单列 LSan 第 4 项。 |
| 7 | JDK 17 全 reactor 完整回归与 coverage | 最终同源码执行 `-Pfull-regression,coverage verify`，记录各模块报告与 0 失败/错误/跳过；本轮 Worker 1,433 项已过，但之前全 reactor E2E 遇 Docker VM 级联失败。 |
| 8 | 静态、协议、结构与变更行覆盖率 | 最终同源码从起点跑 `static-analysis`、descriptor、SPI/JNI、文档引用、E2E 真实性及适用脚本；变更行覆盖率 ≥60%。历史 70.9% 和静态退出 0 仍需最终重核。 |

## 第三批：共享与独立进程 E2E（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 9 | 完整共享 JVM E2E | `pangrova-test-e2e` 全部适用类、零失败/错误/跳过并记录实际测试数；先前 1,089 项运行受 VM 崩溃连锁污染，不能计通过。 |
| 10 | 三个隔离 Native runtime owner | `WorkerBootstrapE2ETest`、`NativeExchangeTransportExternalIntegrationTest`、`WorkerShuffleChannelAdmissionIntegrationTest` 各独立 JVM；历史 5/5、3/3、1/1，最终镜像/JNI 变更后复跑。 |
| 11 | 两个独立 Worker 的推测执行/交换 | 使用第 1/3 项真实 image ID 跑 `IndependentSpeculativeExecutionE2ETest` 和对应远程 shuffle/故障 E2E，验证独立进程、实际搬运、winner fencing 和资源归还；目前无镜像 ID。 |
| 12 | 故障恢复矩阵 | 真实 PG、对象存储、Flight/Toxiproxy 和独立进程验证接管、迟到 RPC、丢 ACK、未知提交、取消、慢读、OOM 与最后 owner 退出；局部用例已过，组合矩阵未全闭合。 |

## 第四批：外部 Catalog 和真实写入（4 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 13 | Gravitino Iceberg REST CRUD | 已有空表 DDL/读/删除 1/1；非空 INSERT/查询/删除 2 项被声明栅栏拒绝。需服务端原子完整 metadata CAS 或等价并发前置条件，不能撤掉保护后算通过。 |
| 14 | 独立 Paimon REST 服务 | 配置真正的 Paimon REST URI/warehouse，跑非空 CRUD 和故障边界；Gravitino 的 Paimon catalog 不是此协议的服务端。 |
| 15 | Lance/Paimon 历史版本与写入所有权 | 用真实旧 snapshot 内容、并发 winner/loser、未知提交恢复、取消/drain 和跨 reader 验证最终可见性；已有局部 Lance/Paimon 结果不构成完整矩阵。 |
| 16 | 大批次 Native 写入容量 | 修复已观察到的首发 `MEM_CAP_EXCEEDED`，再验批量写入、spill/回收、失败后资源归零及非空实际读取；队列错误诊断修复尚未消除容量根因。 |

## 第五批：已知核心功能缺口（8 项，先实现后验收）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 17 | 唯一 Calcite SQL 前端 | 生产 DDL/DML/时间旅行等全部迁移并删除可达 ANTLR/fallback；逐形式语义、计划与调用链证明。 |
| 18 | `GENERATE_SERIES` 真 Native 表函数 | 裸调用、二/三参及极值经 Substrait 表函数关系到 Velox operator；现有 3/3 E2E 只证明 Java 展开后的行值。 |
| 19 | `COPY INTO` 真文件加载 | 授权 URI/文件快照、格式读取、幂等文件身份与提交 journal，真实 S3/对象存储数据核对；旧测试的空成功断言不算验收。 |
| 20 | 剩余系统表 | Iceberg `$files/$entries/$manifests/$partitions`、Paimon `$schemas/$branches/$files` 等冻结句柄、typed split、Native reader 与 SQL 实值；`$snapshots/$tags/$options` 的局部结果不能替代。 |
| 21 | Dynamic Table 真物化刷新 | 接通 RefreshCoordinator、增量 split dispatch、原子版本与物理扫描量；已过的 5/5 仅是直读重写，刷新前亦可见新行。 |
| 22 | 索引完整生命周期与 DiskANN | scalar 索引持久授权、Iceberg generation snapshot refs、READY/REBUILD/GC；向量 DiskANN 真 ANN operator、过滤与 Recall，不能以候选数据或旧算术性能断言替代。 |
| 23 | 独立模型服务异步执行 | SQL→Native 异步批量 RPC/Arrow Flight→真实模型服务→typed result，以及超时、取消、乱序、未知完成和 fencing；本轮真实 Ollama 13/13 仅覆盖 Worker HTTP 组件。 |
| 24 | Feature Store、CDC 与评估闭环 | 同一物理计划的 point-in-time join、watermark、版本物化、训练导出和真实指标计算/发布；当前元数据或单路径测试不能证明全链。 |

## 第六批：性能和发布资格（3 项）

| 编号 | 待闭合检查点 | 通过标准与当前边界 |
| --- | --- | --- |
| 25 | TPC-H 与写入基准 | Linux x86_64 的 SF100/SF1000、22 查询、并发 1/8/32 与 append/upsert/delete/overwrite；同资源冻结基线、原始样本和逐查询回归门槛。小规模 TPC-H 22/22 不可外推。 |
| 26 | 向量检索质量/成本 | 1,000 万/1 亿向量、128/768 维、过滤/Top-K/混合检索；Recall@10 ≥0.95 时比较延迟、吞吐、内存和成本。 |
| 27 | 多 Worker 扩展及稳定性 | 指定 2 Coordinator、4→8 Worker、32 vCPU/128 GiB/NVMe/10 Gb 环境；每条件 ≥1,000 完成请求、3 独立窗口，冷/热、前后台干扰、P99、扩展效率与长时故障恢复。当前本机规格不能代替。 |

执行次序：第一批已在跑，随后按第二至第六批做最小失败复现、根因修复、最近测试和所属模块扩面。某项因实现或环境缺口阻断时记录准确状态，继续同批独立项。第五批任何生产实现变化后，重新运行第二、第三、第四批受影响链；所有功能闭合后再做一次全量门禁和 E2E，不复用旧源码的“通过”结论。Docker VM 每轮检查可用空间；只清理已失效归档、临时容器和临时 swap，保留复用成本最高的已通过构建层及可复测模型缓存。
