---
title: "执行链路剩余工作与验收"
description: "Pangrova 查询、写入、Native 与发布的统一未完成清单"
---

# 执行链路剩余工作与验收

本清单以 2026-11-28 的 `main`（`904eeba54`）为基线。执行链路整合代码已通过 [PR #112](https://github.com/pangrova-io/pangrova/pull/112) 合入。对应 [main CI](https://github.com/pangrova-io/pangrova/actions/runs/46135256629) 在本次核对时尚无最终结论。已有本地证据包括 230 项专项定向回归，以及 A9 之后的 70 项 Java 和 12 项重新链接的 Native 定向回归。C1–C9 文档勘误已完成。上述结果只覆盖各自测试范围。

保留原有 23 个待关闭条目，包括 A1–A14 与 B1–B9。A 表列出实现缺口和单项验证，B 表是成套生产验收。同一组故障或负载证据可用于多个条目。表后的专项探针和资格条件并入相应条目，按证据触发的设计候选在测出瓶颈后才实施。旧方案、测量和已交付批次可从该基线的 Git 历史追溯。

## A 实现工作包与单项验证 {#remaining-work}

| ID | 当前证据 | 剩余工作和关闭条件 |
|---|---|---|
| A1 · WP1.1 HTTP 受理、幂等与准入 | 普通新受理单条 INSERT，已验证本地 PostgreSQL、无幂等键写入和双 Worker | 组合两个独立 Coordinator、Worker 故障、响应丢失、未知提交、并发重放和 principal 隔离。核对原回执、唯一执行、租户槽位与正常路径 SQL 计数。见 B1/B3 |
| A2 · WP1.2 查询所有权与物理排空 | 已有签名回执定向转发、精确派发和退出凭证、租约与槽位同事务退还、本地 Flight 排空与持久接管 | 验证跨主机独立进程退出、旧实例迟到报告、租约过期、同名实例替换、接管与客户端重连。分别证明 Native 任务与结果、Shuffle、快照 pin 和槽位退役，并检查对外 DRAINED 的发布时间。覆盖已发布写入的成功保留和无运行所有者的回收。见 B1/B2/B6/B7 |
| A3 · WP1.3 结果状态 | `PENDING`、`STREAM`、`NONE` 已贯通客户端与服务端，本地双 Worker 和独立 CLI 已有覆盖 | 在独立进程上验证空结果、无结果集、延迟 Ready、结果读完后写提交失败、取消恢复。不允许端点 Ready 与逻辑终态互相替代。见 B2/B6/B8 |
| A4 · WP2 关键路径归因与容量 | 已有准备排队/执行、Worker 回报、胜出超时和全局物理排空等观测，本地真实 PostgreSQL 已验证排空计数不重复 | 跑跨主机固定到达率矩阵，覆盖短长查询、租户过载、目录阻塞、终态报告压力和 durable commit。关联真实提交、规划、ACK、JNI 创建、首末批、终态和排空，分离连接池、锁和实际执行时间。按 B3 证明饱和时独立查询与取消仍能推进 |
| A5 · WP3.2 Paimon 枚举 | 普通读取、内部增量和直接追加共用阶段上限，已加入跨窗口删除合并与受阻读取用例 | 在最新候选统一复跑并加入对象存储流中断、超时、取消和 manifest 压力。核对固定快照、完整结果、I/O 实际关闭和元素/字节/并发额度，校准阶段预算 |
| A6 · WP3.3 分片工作量与 Worker 放置 | 窗口合并持久认领，已纳入冻结 split、交换字节和精确在途计数。Worker 心跳包含各 Coordinator 已进入 Worker 的工作 | 补多 Coordinator 样本传播和并发放置验证。核对 Worker incarnation、样本新鲜度、ACK、派发前回滚和实际释放，解决在途与心跳重叠。按格式、冷热、删除文件和并发校准打包/放置，测分配等待和最长任务。共享样本不可扩大查询授权 |
| A7 · WP3.4 物理读取覆盖 | Parquet/Iceberg 支持基于合法范围读取并共享对象长度，Lance/Paimon 已有各自的整文件或范围约束 | 验证真实 Apache ORC reader、受保护产物贯穿授权与读取的整对象身份、跨主机 GET/HEAD 次数，以及删除文件较大时的成本与位置正确性。范围不得重叠或缺失，需核对整文件对照、删除语义、投影、压缩和对象版本不匹配。见 A8 |
| A8 · WP3.1 物理 I/O 和权限 | Native 读取计数及 S3 body 字节已进入 attempt Profile，标准 Iceberg/Paimon 已有逐目录凭证和缓存隔离实现 | 复跑真实 MinIO 与独立 Native 进程夹具，再按标准 SQL 路径及其他 SDK 实测逻辑请求、实际请求/重试、返回字节、缓存与异常。验证同地址不同凭证、目录变更、撤权及缓存隔离。HTTP header/传输帧如未实测标为不可用 |
| A9 · WP4 跨 Stage 动态过滤 | Iceberg/Paimon 同宽 TINYINT、SMALLINT、INTEGER、BIGINT 及多独立组合已实现，摘要有界。A9 已有定向 Java 和 Native 交换回归 | 以独立双 Worker 同源部署包验证晚到过滤、被接纳 attempt、Native 确认、split/row group/读取字节、结果与实际释放。覆盖共享 Stage、其他键、连接器及并行组合。按负载校准等待成本和晚到对已发 I/O 的影响，保持缺证、NULL、版本不匹配时的结果正确 |
| A10 · WP5 AQE 与持久读取布局 | 单输入和对齐双输入已有真实 SQL 合并证据，持久单/双侧拆分、接管与最终发布有 PostgreSQL 回归 | 复跑同源部署的 Shuffle → JNI → Flight 混合范围和偏斜用例。验证更复杂输入、Join 类型、NULL、重复键、投机获胜、原有 epoch、重试、durable replay、读取覆盖、最终 producer cohort 和复制上界。用混合负载校准阈值与尾 task。见 B5 |
| A11 · WP6 Shuffle 撤权、producer 停止与回收 | 已有读写 lease/window、终态封口、精确收敛、晚到对象清扫和本地 PostgreSQL/MinIO 证据 | 补未出现对象的 PUT 与 producer window 的正向停止证明，覆盖丢 ACK、迟到 PUT、清扫跨页、旧 owner、替换实例和并发 reader。撤销后阻止新的读取/发帧，在途 I/O 与实际退出有证据才删除或交还。本机多实例复跑 SQL 计数与故障回归，跨主机热点留待独立主机验收。见 B7 |
| A12 · WP8 写入日志与 CDC 回执 | 二进制 journal、精确回执、CDC 领导任期和 PostgreSQL 变更订阅已接入，本地写入 journal 有定向验证 | 验证各来源重放窗口、回执回收、提交 ACK 丢失、CDC 断线重连与跨 Coordinator 暂停/恢复。复跑空闲数据库计数，覆盖并发/故障与锁等待。只有目录提交可见且精确发布证明成立才确认成功或前进 offset。见 B8 |
| A13 · WP8 暂存文件与 fenced commit | 已接入 Paimon SDK 已上报文件的同 Linux 存储命名空间清理及独立租约回收 | 补未上报文件、Native 物理写出、外部根/对象存储、跨内核重启的存储权威。跨进程注入部分删除、未知提交、租约变化和重复清理，验证匹配指纹、实际止写、回收与发布 fencing。覆盖成功快照及遗留孤儿对象，核验保留集合。见 B8 |
| A14 · WP9 发布证据 | 受控 nightly 和 Native CI 已有入口，合并与定向回归本身不盖发布章 | 完成同源 Linux Native/生产镜像与不可变 TPC 证据门槛，替换 `tpch-sf100-baseline.json` 中 22 条零值占位。运行前落实 B9 环境，CI 和基础设施变更需先取得明确批准。参见 [Native 资格](#native-qualification) |

## B 生产验收项 {#production-acceptance}

| ID | 现有本地证据 | 仍需交付 |
|---|---|---|
| B1 | 48 项 PostgreSQL journal 回归 | 双 Coordinator 并发、未知提交、旧实例与 Worker 故障组合，证明恢复与退役的原子性 |
| B2 | 4 项 HTTP/PostgreSQL 回归，正常状态及等待零次 journal 借用，显式历史单次 | 跨进程等待、认证转发、断连/重连、停机和取消压力，验证尾延迟、准确终态与实际排空 |
| B3 | 已覆盖阻塞提交时的容量占用与恢复 | 固定到达率跨主机测试，叠加阻塞目录、慢对象存储、JNI/大请求和终态报告压力。报告排队、拒绝、超时和实际归还 |
| B4 | 6 项 Planner 回归，已有局部归约实现和历史双 Worker 对照 | 在同源真实 SQL 量化 identity projection、局部聚合的 CPU、首批/完成时延、实际交换量和跨 Worker 影响 |
| B5 | 43 项 PostgreSQL 持久交换回归，已有单/双输入合并对照 | 结合 A10 验证更多输入、 Join/NULL/重复/重试/不同代次、倾斜、混合负载、阈值与尾时延 |
| B6 | 66 项 Java Profile 与 Native 回报合同回归，已有本地中途断连证据 | 在最新部署产物跑实际 Flight、慢读/断连、失败 EOF、跨主机吞吐和长稳。验证独立客户端阶段、各层内存上界与全资源实际释放。完成完整 ASan/UBSan，并保留平台适用的 LSan 证据 |
| B7 | 40 项本地 PostgreSQL/MinIO 回归 | 本机复跑慢消费者、并发读写、租约切换、物理清理和每流/每窗口 SQL，覆盖已关闭窗口及保护时间内在途 I/O。跨主机热点的吞吐、尾时延和恢复时间待独立主机实测。见 [B7 环境要求](#b7-topology) |
| B8 | 23 项 PostgreSQL 写入回归 | Iceberg/Paimon/Lance/COPY/CDC 的同表与异表高并发、未知提交、崩溃与接管、重放、跨引擎结果及实际资源归还 |
| B9 | 有基准工具和固定到达率样本。核对时仓库可用专用 runner 数为 0，仓库级 Actions 变量列表中无 `TPC_BENCHMARK_CONFIG_DIR`，最近一次 [受控 nightly](https://github.com/pangrova-io/pangrova/actions/runs/46131366631) 跳过 | 配置匹配的专用 Linux runner、同源部署配置和共享 SF100/SF1000 快照。取得 Trino、Spark SQL 与 Pangrova 的正确性摘要、真实 Profile、三个独立有效窗口和可复核对照。缺失或零值样本无法计入 |

### B7 环境要求 {#b7-topology}

同机多 JVM、容器或 Docker context 的结果按同机回归记录。跨主机验收需要把相关实例部署在独立主机，流量经过真实主机间网络。记录主机、部署产物、服务拓扑、热点负载、吞吐、P95 和故障恢复时间。只有本机可用时，先完成同机回归，将跨主机子项标为「待环境」。

## 仍需跑通的专项与设计门槛 {#focused-probes}

- **A1/A4/A6 控制面与目录。** 复查跨表初始读准入、目录指针和大小写回退。分别计量授权、命名空间/表枚举、统计 SQL 和 manifest I/O。覆盖别名、CTE、视图、歧义、其他连接器具名快照、移动引用、到期、同名表重建、撤权与目录变化。两个独立 Coordinator 对照物化对象发布、暂停、删除、名字复用、重连和缓存淘汰，核对租户、发布代次与快照 pin。短查询采样 singleton 退化、冷启动、早期 ACK 和 Native 创建，依据实际路径评估安全的阶段合并。测潜在的锁内 future 回调、全局任务身份锁、并发大计划/票据扇出和 JNI 编解码。仅在 JFR 或流量证据表明跨查询阻塞、重复传输或持锁成本显著后，才调整锁、回调或票据布局。
- **A5–A9 扫描。** 验证 split 枚举、索引过滤的位置、文件打开、字节布局、实际解码和读取放大。按固定快照、约束、schema、授权和读版本对照 row group/page、冷热、宽窄、随机和删除文件。只有并发重复发现相同 manifest 成本明确时，才评估按身份共享加载，保持请求各自的 pin 和取消语义。缓存亲和依据实际命中与驻留，预取受字节预算限制。
- **A10/B4 统计、Join 与聚合。** 给 EXPLAIN 和执行 Profile 对照每算子的实际行/字节、估计误差、统计来源、物理分布及 singleton 原因。核对高频值、NDV、相关性和选择性导致的真实误判，再扩展统计或 Join 选择。广播计入每 Worker 内存与复制成本。验证分组 DECIMAL、混合可分解函数、DISTINCT 和 GROUPING SETS，区分语义证明、成本门槛与实测收益。Storage Partition Join 只有在两侧分区 spec、变换、键类型、哈希和 NULL 语义有兼容证据时才评估。
- **A11/B6/B7 数据面。** 区分建流、首包、队列、每次 push、实际关闭与跨流饥饿。若慢 reader 持有线程成为主因，评估有界预留、分帧或就绪回调。若对象次数或锁等待主导，才以相同总字节对照有界 segment。若出口带宽饱和、消费持有资源主导或单出口是显著瓶颈，再评估并行 endpoint、范围有序合并或 spooling。验证 ORDER BY/LIMIT、重复/遗漏、EOF、取消、撤权、内存/苏醒和存储回收。现有代码先分别归因对象 I/O、解码、Velox CPU、缓存、reclaim 和 spill。
- **A12/A13/B8 写入和混合负载。** 测 COPY 固定版本暂存、COW 命中与幸存行、同表热点、实际文件尺寸/数、写放大、catalog/commit/journal 锁等待、CDC source lag/offset 确认。单列后台 compaction、索引、检索和物化刷新对前台 P95 的影响，检索需保留 recall/Top-K 正确性。
- **A14/B9 计量与对标。** 按 [TPC 基准方法](../testing/tpc-benchmark-methodology.md) 使用 SF1 做正确性，SF100 做单流与混合吞吐，SF1000 做同源独立 Worker 规模扩展。覆盖点查、扫描、Join、聚合、Sort/Top-K、宽结果、写入及故障。固定硬件、资源、外部服务、SQL、快照/布局、安全和缓存设置。逐引擎对照原始结果、客户端完整消费、P50/P95、足量样本的 P99、SLA 内成功吞吐、拒绝/超时、CPU、读写量、峰值内存和从 EOF 到物理退出。阶段百分位分开量测。交互主对标 Trino，批量主对标 Spark SQL，两组保留另一引擎的结果。按负载冻结噪声、结果口径和性能门槛后再测候选。早期相对目标 P50 ≤ 1.05 倍、P95 ≤ 1.10 倍、SLA 内成功吞吐 ≥ 95% 仅用于基线校准。TPC 派生实验按工程对照报告。

## 运行合同与关闭门槛 {#hard-gates}

1. 正常状态等待、重连和诊断投影零 journal SELECT。显式历史、未知提交和接管限在对应权威边界。正常持久 journal 新事件以单条 INSERT 完成，无独立读回。
2. Shuffle 建流、续租、关闭及恢复可访问权威库。数据面 batch/chunk 零同步授权 SQL，生产按有界窗口预留和发布。对象身份、序号、覆盖、摘要、owner epoch 和已接纳 attempt 必须可证明。
3. 从入口到结果区分 ACK、Ready、首末批、EOF、逻辑终态和物理排空。每项发布和实际清理都需要精确身份与证据。旧 owner 不得确认新代次状态，未知目录提交不能推定成功或提前删除。
4. 同一版本交付 Coordinator、Worker、Native、Shuffle Service、CLI、JDBC 和 benchmark。候选使用独立测试 schema 和对象前缀。每条生产路径保留唯一权威入口，删除退役调用、配置和注册后用实际入口复测。协议、JNI、descriptor golden 和模块规则按仓库要求验证。把复测命令、commit、运行环境、原始日志和结果摘要写入此文件或链接到可复核产物。按实际覆盖关闭并删除对应待办。

## Native 构建与平台资格 {#native-qualification}

一般构建、依赖固定、分层缓存、Lance/Java 夹具和清理按仓库构建规则执行。A14/B6 仍需在同一提交校验来源、测试报告、发布包身份、生产镜像及四平台适用测试。Release、ASan/UBSan 和 Linux LSan 使用各自合格产物，完整 CTest 加严格诊断扫描。重建时保留源、工具链、部署目标、库、patch 和二进制的 SHA，记录实际加载的运行时。冷构建仍需实测磁盘和内存容量。

### 条件性构建优化

Linux GHCR 前缀在 CI 中已有通过 `PANGROVA_NATIVE_DEPS_IMAGE=ghcr` 启用的分支。仓库变量列表在本次核对时无该设置。若另行批准启用，验证发布权限、与 compiler/arch/lock/patch/sanitizer 的匹配，以及冷构建节省和镜像完整性。该选项不作为执行链路关闭的前置条件。

### Darwin LLVM 功能检查与 Folly {#darwin-function}

历史 macOS arm64 LLVM 17.0.6 / Folly v2026.01.05.00 资格结果只适用于实测产物。重做该组合时，使用新输出目录生成私有 LLVM，保留已安装编译器和运行时。验证实际加载、Mach-O `.alt_entry` 和有效调用，要求错误函数调用及堆越界对照能被报告。记录生成的 `build-evidence.json` 和 `canary-evidence.json`。使用私有编译器构建带锁定源码修复的 Folly，仍启用 `FOLLY_LIBRARY_SANITIZE_ADDRESS=ON` 与 `AsanFollyCompat.cmake`。

```sh
python3 pangrova-worker-native/bin/build-darwin-llvm-function-prefix.py \
  --llvm-prefix "$PANGROVA_HOME/llvm" --output "$PANGROVA_HOME/llvm-function-prefix-17.0.6"
python3 pangrova-worker-native/bin/build-darwin-asan-folly.py \
  --folly-source "$VELOX_HOME/deps-download/folly" \
  --compiler-prefix "$PANGROVA_HOME/llvm-function-prefix-17.0.6/toolchain" \
  --prefix-path "$VELOX_HOME/deps-install;$PANGROVA_HOME/homebrew" \
  --openssl-root "$PANGROVA_HOME/homebrew/opt/openssl@3" \
  --output "$PANGROVA_HOME/qualified-asan-folly" --jobs 6
export PANGROVA_ASAN_FOLLY_PREFIX="$PANGROVA_HOME/qualified-asan-folly/install"
```

Native configure 显式传入该 Folly prefix 和匹配插桩的 Protobuf/Abseil。资格证据包括 `FollyIntegerIntentControls.cpp` 的算术对照及完整 Native CTest，严格检查地址、函数类型和整数诊断。现行 macOS 发布 gate 使用 `ASAN_OPTIONS=detect_leaks=0:halt_on_error=1`，Linux ASan/LSan 单独验收。

### Darwin arm64 TLS 和 LeakSanitizer {#darwin-tls}

若本轮需要资格化 Darwin LeakSanitizer，使用与 LLVM 17.0.6 匹配的 `CC`、`CXX`、`LLVM_CONFIG` 和 CMake 3.28.x，运行 `bash pangrova-worker-native/bin/build-darwin-asan-runtime.sh /new/private/runtime-prefix`。使用私有 runtime 和校验过的 release 源，核对 `otool -L` 与实际加载证据。涉及安装名或签名只处理私有测试产物。

- 在 `detect_leaks=1:halt_on_error=1` 下，用 `DarwinTlsRuntimeControls.cpp` 验证真实 TLS 根、清除根后可检出的 193 字节泄漏、use-after-free 和 heap overflow。模式 0/1 必须正常清理，模式 2–4 必须得到对应诊断，机器的 pthread 最末 key 与校准预期匹配。
- 用 `RuntimeErrorDiagnostic.cpp` 对照异常创建、复制、抛出、重抛和自然退出的线程，在 1、32、4096 次迭代运行模式 0、1、2、3、4、8。留存原 runtime 的对照结果。
- 先通过完整 23 项校准控制，再以该 runtime SHA 和相同源码/依赖完整跑 Native CTest。分别保留 macOS 常规 gate 和额外 LeakSanitizer 运行证据。源码、Lance、ANN 或函数组合变化后重新核验受影响的产物。

## 下一任务 Prompt {#continuation-prompt}

```text
在 Pangrova 仓库继续实施当前分支的 docs/development/execution-backlog.md。以最新 main 和此文件为准。PR #112 已合入，只作为已交付基线，后续提交归并到一个新 PR。若清单还在文档清理分支，从包含清单的提交接续并合入最新 main。保留其他工作区的未提交改动，先读 AGENTS.md 和相关 .pangrova/rules。

逐项核对 A1–A14 和 B1–B9 的现有实现、测试和运行证据，先交付确定缺失的代码，再做统一生产验收。并行推进彼此独立的批次，优先完成跨 Coordinator 放置、受保护整对象读取与真实 I/O 证据、动态过滤覆盖、Shuffle 正向止写证明、CDC 回执和写入暂存回收。性能候选先做归因，按清单的触发条件决定是否实施。阶段内跑最贴近的回归，代码收齐后构建同源部署包，统一跑跨主机、故障注入、Native/Flight、资源实际排空和 SF100/SF1000 对标。B7 同机多进程只计并发、租约、SQL 次数和清理回归。跨主机热点需要独立主机与真实主机间网络。只有本机时，完成可跑部分并将跨主机子项标为「待环境」。CI、依赖、模块所有权及基础设施变更按仓库规则先获批准。

每次提交保留精确授权、快照、owner/epoch、attempt、fenced commit 和物理退出语义，维持清单中的稳态零读库门槛。定期检查空间，只清理可再生的无用产物。批次通过后及时提交并推送到同一新 PR，记录 commit、命令、运行环境、结果、日志和真实阻塞。已验证的条目按覆盖范围关闭并移出本文件。没有可比基准或实际部署证据时保留相应未完成项，全部强制门槛通过后再合并。
```
