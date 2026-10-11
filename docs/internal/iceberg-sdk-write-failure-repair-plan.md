# Iceberg SDK 写失败：根因、隔离补丁与正式验收方案

日期：2026-10-07。状态：**隔离候选 SOURCE_READY；生产依赖未切换；正式发布验收 BLOCKED**。

本文件仅记录实际 SDK / 文件系统证据与后续实施门槛，不宣称全仓回归、分布式部署或生产验收通过。未修改仓库 POM、BOM、生产 SDK、`.m2`、协议、JNI 或公共 SQL；没有部署以下实验 JAR。

## 1. 结论与版本选择

直接升级到官方 Iceberg 1.12.0 **不能修复**当前写失败问题。实际替换 `api/common/bundled-guava/core/parquet/orc` 六个官方 1.12.0 JAR，排除旧 Iceberg 1.11.0 后，错误行仍被计入 footer，完整读取失败，失败 writer 仍产生 DataFile。1.12.0 的 Parquet 依赖仍为 1.17.1。官方依据：[Iceberg 发布页](https://iceberg.apache.org/releases/)、[1.12.0 依赖版本](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/gradle/libs.versions.toml)、[发布 POM](https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-parquet/1.12.0/iceberg-parquet-1.12.0.pom)。

建议正式评审以 **1.11.0 最小回补 + Parquet Hadoop 1.17.1 回补**为候选基线，不因版本较新引入无关升级。1.12.0 还会改变现有主源码 position-delete 组件的兼容性：`DeleteWriteBuilder.forTable(table)` 设置 `rowSchema`，而其 `buildPositionWriter()` 拒绝非空 rowSchema；现有 `IcebergDeleteWriter.createPositionWriter()` 恰好使用此链路。全仓 `src/main` 调用检索尚未证明该组件接入公开 SQL delete，直接调用均在 tests，因此这里只确认组件兼容缺口，不宣称公开生产 position-delete 已实际失败。1.11.0 的原构造签名和 row-data 行为必须保留，不能把 1.12 的完整类直接覆盖到 1.11。依据：[1.12 Parquet.java](https://github.com/apache/iceberg/blob/apache-iceberg-1.12.0/parquet/src/main/java/org/apache/iceberg/parquet/Parquet.java)、[1.11 PositionDeleteWriter](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/deletes/PositionDeleteWriter.java)。

## 2. 真实故障链及独立证据

生产调用链：`NativeWriteExecutor → NativeWriteSession → 实际 Iceberg PageSink → DataWriter → Iceberg ParquetWriter → ParquetFileWriter → 实际文件系统`。旧 E2E 的真实 decimal 写失败发生后，清理保留文件的完整读报 `Should not read definition, past page end`，日志为 `/tmp/pangrova-production-20261007-write-cleanup-real-nearest-r5.log`。当轮 5 项结果是 **3 PASS / 2 ERROR / 0 SKIPPED**，不能计为全通过。

脱离 Pangrova、Native、故障 barrier，仅使用同版官方 SDK 的最小复现也发生相同错误：先写 decimal(6,0) 的 7、1，再写 7.01；SDK 抛原始 wrong-scale 异常，但 `close()` 返回，footer 增加 `rows=1/valueCount=0` 行组；完整 reader 读到 7、1 后失败。多字段场景中 marker 先写、decimal 后失败；默认缓冲情况下 footer 的 scalar valueCounts 为 marker=3、decimal=2，单纯后移计数不能恢复行组原子性。

另一个独立真实证据是 stock Parquet Hadoop `abort(); close(); close()`：对精确所属文件使用 `lsof` 检查，FD 数为 **1 → 1 → 1**；正常 close 为 **1 → 0 → 0**。这不是根据异常推测资源泄漏。

官方源码定位：

- [Iceberg 1.11 ParquetWriter](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/parquet/src/main/java/org/apache/iceberg/parquet/ParquetWriter.java)：model 写入前增加记录数；close 在 flush 之前设置 closed；`ColumnWriteStore.close()` 会 flush，不能用于丢弃脏行组。
- [Iceberg DataWriter](https://github.com/apache/iceberg/blob/apache-iceberg-1.11.0/core/src/main/java/org/apache/iceberg/io/DataWriter.java)：没有失败锁存，close 后无条件从 metrics 发布 DataFile。
- [Parquet 1.17.1 ParquetFileWriter](https://github.com/apache/parquet-java/blob/apache-parquet-1.17.1/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileWriter.java)：aborted 路径不关闭 out；finally 无条件设置 closed；footer 错误可能被 close 错误替换。

## 3. 五源候选的精确边界

| 所有者 | 候选修复 | 不能混同的状态 |
| --- | --- | --- |
| Iceberg ParquetWriter | model/endRecord 成功后才计数；MODEL_FAILED 丢弃当前行组；IO_FAILED 不重刷/重写 footer；列、page store、文件、compressor 分阶段关闭并仅重试未成功阶段 | 停止写入、永久禁止发布、实际资源 closed 是三个不同事实 |
| DataWriter | write/close/metadata 构造异常永久锁存；清理成功也不生成 DataFile；toDataFile/result 永久拒绝 | 清理完成不等于写成功 |
| EqualityDeleteWriter | 同样锁存实际 write/close/发布失败，不生成 DeleteFile/result | 不因重试 close 恢复发布资格 |
| PositionDeleteWriter | 锁存包括 referencedDataFiles 添加之前/之中的真实异常；保留成功引用集合契约；失败永不发布 | pre-appender 错误也属于本次写失败 |
| ParquetFileWriter | abort 也关闭实际 out；out/CRC 所有权分别确认；原错误优先、后续清理错误 suppressed；stream 在 schema/encryption 初始化后才获取 | aborted 不等于资源已关闭；footer 不得重复 end |

`publicationFailed` 在 Parquet appender 上独立且永久：model、flush、close/列/page/compressor 失败都拒绝 metrics 和 splitOffsets，即使后续资源清理成功。cleanup-only 故障不自动把已完成行组判成物理损坏，但也绝不能恢复任何 writer 的发布资格。清理异常组合保留原异常对象与真实 suppressed 错误，不预校验行来伪装 SDK 异常。

MODEL_FAILED 只能保留**此前已完成的 row groups**。尚未刷盘的整个脏行组丢弃，不能承诺其中已接受的行仍然保留。实际中途 I/O 错误可能破坏文件物理状态；公共 PositionOutputStream 没有安全回滚/截断契约。因此 IO_FAILED 文件必须在所有真实 owner 关闭后由调用方删除整个未提交文件，不生成元数据，不通过后续重试 footer 冒充成功。

### 本轮明确未闭合的边界

- 主源码组件 `IcebergDeleteWriter.buildPositionDeleteFile()` / `buildEqualityDeleteFile()` 没有 finally/try-with-resources；write 的 unchecked 异常会绕过后面的 close。SDK 实验主动执行了 close，因此不能把实验的 FD0 结论推广为这两个组件调用方的异常路径已闭合。当前只有测试直接调用，公开 SQL delete 的 runtime closure/wiring 为 OPEN；后续必须同时复证接线与真实 cleanup、owned-file 删除链。
- Iceberg ParquetWriter 构造过程在 `createWriterFunc.apply` / `startRowGroup` 前获取 compressor；构造尚未返回时 model/allocator/store 初始化失败的回收边界仍未验收。ParquetFileWriter encryption 构造失败的 FD0 不覆盖此边界。
- 任意 Error、真实 close 与 abort 同时失败、column/page/compressor 实际清理失败后的阶段重试尚未取得合法真实故障证据。状态机源码已设计，不将未实测分支计为通过。
- 不宣称 ORC/Avro、其他 FileWriter、远端对象存储、加密读写或全部 allocator 边界已修复。

## 4. 保留的隔离证据与可复现命令

独立目录均保留，未删除：

- 原 1.11 最小复现：`/tmp/pangrova-parquet-sdk-repro.ezvDW8`。
- 计数补丁/脏组丢弃探索：`/tmp/pangrova-iceberg-sdk-evaluation.msmmAm`；首次缺 Commons Lang API 的 compile FAIL 产物明确不可用，未用于验证。
- 官方 1.12 / 五源 1.12 候选：`/tmp/pangrova-iceberg-112-evaluation.9R1D98`。
- **最小 1.11 五源候选**：`/tmp/pangrova-iceberg-111-repair.bDH9UC`；完整 source trees、patches、probes、JAR、日志、固定 classpath 与逐 JAR 哈希均在此目录。

实验使用 JDK17 的 `javac --release 17 -encoding UTF-8`。所有 SDK 生产源码均编译，不是单类 shadow：1.11 core=601、parquet=59、hadoop=152；1.12 core=622、parquet=59、hadoop=152。core/parquet 的原始与候选 `.class` 集合完全相同；Hadoop 的生产类齐全，三个无注解 package-info.class 未由 javac 产生。原同名 core/parquet/hadoop JAR 从实验运行 classpath 排除，实际 CodeSource 已打印验证。

1.11 实验仅额外需要 compile annotation processor Immutables 2.10.1、Commons Lang 3.20.0（原 3.12.0 不含完整 parquet 源码所需 Streams API）和未 relocation 的 Fastutil 8.5.15。**这是实验 classpath，不是发布依赖设计**；发布必须走官方完整构建/依赖与 shading 流程。

运行脚本打印的实际 classpath（无末尾换行）SHA-256：`c7a17725c92ae930e92250a36399393ceb30259e96b07973c24df4104c8f480f`。同一 classpath 加末尾换行后的 `runtime-classpath.txt` 文件 SHA-256：`656749678cd50d76430119fd80e37e1697d4e602b43be0dfc818c515f4e011b9`，不是另一 cohort。376 个实际 JAR 的哈希清单 `runtime-jar-sha256.txt` SHA-256：`5dc4b2a2fcd3c3eecb6304a6f253752ffd03bed53260d8d9724955f81cbb1f14`。路径字符串 hash 不替代内容 hash。继承的 E2E classpath 不是最小发布 classpath；正式版本必须独立收敛并验证其实际 API 加载边界。`compile-record.md` 保留完整实际编译命令；`verify-source-closure.sh` / `.log` 保留源码数量和 class 集合对照。这些是独立 javac 证明，不是 official build。

在上述保留目录复证（每次创建新的输出目录，不能覆盖原真实文件；runner 内 JDK 路径为本次环境的实际路径）：

```sh
task_sdk_dir=/tmp/pangrova-iceberg-111-repair.bDH9UC
task_out=$(mktemp -d /tmp/pangrova-sdk-confirm.XXXXXX)
set -o pipefail
zsh "$task_sdk_dir/run-matrix.sh" ActualWriteFailureAcceptance "$task_out"
task_delete_out=$(mktemp -d /tmp/pangrova-sdk-delete-confirm.XXXXXX)
zsh "$task_sdk_dir/run-matrix.sh" ActualDeleteFailureAcceptance "$task_delete_out"
task_io_out=$(mktemp -d /tmp/pangrova-sdk-io-confirm.XXXXXX)
zsh "$task_sdk_dir/run-matrix.sh" ActualWriteFailureAcceptance "$task_io_out" compile
zsh "$task_sdk_dir/run-matrix.sh" ActualWriteFailureAcceptance "$task_io_out" io-limit
task_delete_io_out=$(mktemp -d /tmp/pangrova-sdk-delete-io-confirm.XXXXXX)
zsh "$task_sdk_dir/run-matrix.sh" ActualDeleteFailureAcceptance "$task_delete_io_out" compile
zsh "$task_sdk_dir/run-matrix.sh" ActualDeleteFailureAcceptance "$task_delete_io_out" io-limit
task_footer_out=$(mktemp -d /tmp/pangrova-sdk-footer-confirm.XXXXXX)
zsh "$task_sdk_dir/run-matrix.sh" ParquetOwnerFailureAcceptance "$task_footer_out" compile
zsh "$task_sdk_dir/run-matrix.sh" ParquetOwnerFailureAcceptance "$task_footer_out" io-limit
```

`io-limit` 只在该独立 Java 子进程设置 `RLIMIT_FSIZE` 和忽略 SIGXFSZ；实际文件写入/写 footer 抛 IOException `File too large`。不修改机器/VM 配置，不用 fake FileIO 或人为协作者 throw。仅删除已确认关闭的本次精确失败文件；保留诊断目录。

## 5. 精确输入与候选哈希

SHA-256；官方输入来自本机已有固定 1.11 artifact 或 Maven Central 官方 1.12 artifact，未写回 `.m2`。

| 输入/候选 | SHA-256 |
| --- | --- |
| 官方 iceberg-core 1.11 sources | `999b9603a72b655f22bd19e6b84e193d5c12ba94adcd0fa0fae6c5c614338e8c` |
| 官方 iceberg-parquet 1.11 sources | `ab8041e593b22972f00b3efb66c9a3319a1ef606afaaef9a220e9df1193e428a` |
| 官方 parquet-hadoop 1.17.1 sources | `1d1c960583d41b56961e1fe16cb4586dd92a5dac1ad1ca0a5324abed7457dd7a` |
| 官方 iceberg-core 1.12 sources | `2b85f5cb63061e2d59cd99cbac062e52219f56f15bb92bb7d1ab1e865959c94d` |
| 官方 iceberg-parquet 1.12 sources | `c37467bd0a104fb1a54187faa63343038b398ae772049cab1942e310d47bb8f4` |
| 官方 iceberg-api 1.11 binary | `f00675a12ceb6d833bb9c22024823a3e7afdedf7abcefe34719f45b0d211995a` |
| 官方 iceberg-common 1.11 binary | `b2835d00db7c92bdfa728123ad7cdcb488e2f2805c1c51fb9ae94e8bd203bd7d` |
| 官方 iceberg-bundled-guava 1.11 binary | `396d48d93ef5735f2ddef0cf9b3d6968ebd67b64b5cfe753d41c429d3f46a2a1` |
| 官方 iceberg-core 1.11 binary | `53b9eb7a8c34fb9958ea7d9a4d19ba5bc96f592d0f05e8625b0778c70b1f0090` |
| 官方 iceberg-parquet 1.11 binary | `9abe486e0a37b4ace19edda9ec3260a339660a77360d4ea50ae71e05b3786a81` |
| 官方 iceberg-orc 1.11 binary | `1bc83e41be22828697ca103f17ca890670078091fbc0aa83eb7596238cf698ed` |
| 官方 iceberg-api 1.12 binary | `d476a6989edfe3882a47ce0eaa3e53df4d589ef5e7bd0a16ff113ae570bca8b6` |
| 官方 iceberg-common 1.12 binary | `18a43330e3c5eb712a9bf88e8f70441971851400efb4646f0b6512ac40c5f357` |
| 官方 iceberg-bundled-guava 1.12 binary | `8d27e319e3bc28f7f86e7fbd59f6758bfaa1f78d72d9f76aa93ad1f7cbd3ba34` |
| 官方 iceberg-core 1.12 binary | `d3bbb331b7a907e134958d21ecccd91a736049e00768e07579ff60855bf28352` |
| 官方 iceberg-parquet 1.12 binary | `fba0355f0d54bd68e21170e0e5f05632845d42ba346945017d2156e66297b85f` |
| 官方 iceberg-orc 1.12 binary | `4ade554710c4d715623beb09735d9c3ea241fae345ebe54667cc667f8db7c2f9` |
| 官方 parquet-hadoop 1.17.1 binary | `2aeb5e21fb12ddc51fc8c69050906446d9c261c28b0446abf2e40d80cfc8e0a0` |
| 1.11 实验 core JAR | `d772a79463f9136e8b58af02d51190a0599e6c7695db3c7b8abef07062d193f1` |
| 1.11 实验 parquet JAR | `68952010d6ae436e4010c66ddf934da6c719d19f709eba52668418e6730cf9bd` |
| 1.11 cohort 实验 hadoop JAR | `b88b393ced306e312bf9d3b3490372adb7fd80632e77e6f6127f109c82752754` |
| 1.12 实验 core JAR | `d6e40c62b234bf02afe88aef33539fc855cb89dabf572e14b9ac3fe4b99ac295` |
| 1.12 实验 parquet JAR | `bd1dc27362c3d70c670b76bc38d464630659bffeea33fbe693d9afbe2b4ed785` |
| 1.12 cohort 实验 hadoop JAR | `d601180d9e448e07f067d289c22b8482b3dd97bf807100748ba7c5e33c626da1` |

1.11 的五份可追溯 unified patch：

| patch | SHA-256 |
| --- | --- |
| DataWriter.java.patch | `91fed6afedb7926a87f6d9483bca061d47560180ce7acf838cf625fb8d4bc5f8` |
| EqualityDeleteWriter.java.patch | `890f5a4f68cf4ea0f3f29d891acdb214a77daf7cdbe403808c1c22b7f2c574c5` |
| PositionDeleteWriter.java.patch | `d5f45ca26dd75ff819c33579bb2a7b2e83c6a391870f331b837af91866a56295` |
| ParquetWriter.java.patch | `652538dd2e0c01fd89191642d79bb0bb3bb5682515fb374d8de905b6cade0091` |
| ParquetFileWriter.java.patch | `b0f1b7a90d84ab1e661f6b059426edcc425aba98a3a5f2d18abacc2201ee04c2` |

实验 JAR 带时间戳，当前哈希用于锁定本次证据，**不是可重现发布构建证明**。

## 6. 本轮实际结果

| 命令/证据 | 结果 | 实际 oracle 与限制 |
| --- | --- | --- |
| 官方 1.11 SDK-only 最小复现 | FAIL | wrong-scale 后额外坏行组；完整读失败 |
| 官方 1.12 `official-matrix.log` | FAIL | flushed/buffered 完整读失败；失败 writer 发布 recordCount=3；blocked-flush 重试后还可发布空 DataFile |
| stock 1.17.1 `official-owner.log` | FAIL | abort 后实际 FD 仍为 1，重复 close 未释放 |
| 1.11 三 artifact 全源 javac / class 集合对照 | PASS | 无原同名实现补洞；有原上游 deprecated/unchecked 警告；不是零 warning 正式 gate |
| 1.11 `acceptance-rows.log` | PASS：4 场景 | 小阈值/默认配置的正常写完整读；7、1 + 7.01 原 SDK 异常；失败 result/DataFile 双拒绝；实际 FD0 |
| 1.11 `acceptance-deletes-ordered.log` | PASS：10 场景 | 实际 Equality/Position writer；完整读；null-path pre-appender 原异常；重复 close 后仍不发布；另补 position 有序 1、7 正向契约。原 7、1 仅作为保留/低层读取证据，不作为有效有序 position-delete 规格 |
| 1.11 `acceptance-io.log` | PASS：1 场景 | OS 实际 mid-column 写失败；拒绝后续写/发布；实际 FD0；关闭后删除整个失败文件 |
| 1.11 `acceptance-delete-io.log` | PASS：1 场景 | 实际 position IO 失败；toDeleteFile/result 双拒绝；真实 FD0 |
| 1.11 `acceptance-footer.log` | PASS：2 场景 | 无效 SDK encryption 配置在获取文件之前拒绝；真实 footer IOException cause；原错误链保留；重复清理 FD0 |
| 1.12 候选早期 R1 | PASS：4 data + 1 data IO 历史行为 | R1 在 publicationFailed/额外 delete owner 增补前执行；早期 JAR 后被同路径 R2 替换且未留其独立 hash，不能把历史 PASS 归入最终五源 JAR 证据 |
| 1.12 最终五源 R2 | PASS：8 delete + 2 constructor/footer + 1 position IO | hash 为上表最终1.12实验三JAR；最终 R2 未重跑4 data/1 data IO，新增 ordered2亦未在1.12复证；不称完整同 cohort 验收 |
| 生产旧 decimal cleanup2 | FAIL | 未部署实验 SDK；原 7、1 完整读断言未削弱，不能被隔离通过替代 |
| 正式 SDK 完整上游/调用方/全仓验收 | BLOCKED | 发布级构建与完整验收未完成，且共享构建环境有其他活动写入者；本轮不运行共享构建。必要 SDK 升级/修复已获用户授权，不虚构额外本地验证审批 |

以上是 standalone 真实断言场景，不是 JUnit 全量测试数量；没有 assume/skip/runtime-unavailable 通过。1.11 的18场景均在同一最终五源 JAR 上执行，完整源码实际编译命令记录在 `compile-record.md`。其中 delete IO 使用增加 ordered2 之前已经编译的实际 helper class，class hash 与恢复的历史 source 版本单列记录，不能用最终 ordered10 helper hash 冒充此前编译输入；恢复的 source 不是当时已签名留存的 snapshot。1.12 早期 helper/JAR 未全部留存，因此早期 PASS 与最终 R2 明确分开。初次 probe 编译错误与 footer 顶层消息误判均保留历史：前者 List API 写错，后者真实 IOException 通过 cause 保留 `File too large`；修正 fixture 后才得到上述 PASS，未更改生产异常文案。

## 7. 正式可落地实施与发布策略

1. 固定两个官方源码基线的 release commit、源码归档 SHA-256、官方签名验证和五份 patch。由正式 SDK 构建独立 owner 提交审查，保留 ASF header、LICENSE、NOTICE、第三方 attribution；不得热补 `.m2`、反射改内部状态或同包单类 shadow。
2. 使用独立版本，例如 Iceberg `1.11.0-pangrova.writefailure.1`、Parquet `1.17.1-pangrova.writefailure.1`，不得冒充原官方版本。所有 Iceberg 模块固定同一个上游基线，唯一 runtime provider；dependencyManagement/exclusions 必须排除原 core/parquet/hadoop 的重复类，并在 Worker 最小真实 classpath 验证 ABI/API。没有公共接口变更也必须跑二进制 API 对照、全部 provider 加载及实际 delete consumer。
3. 按各自官方完整构建流程产生完整 JAR、source JAR、POM/metadata，固定工具链与 dependency verification、可重现时间戳、SBOM、provenance/签名和仓库 checksum。**当前 unshaded Hadoop + 独立 Fastutil 实验 JAR 缺官方 relocation/发布资源流程，不可发布**。必须恢复官方 shade/relocation、服务资源和模块 metadata，并检查逐类/逐资源闭包。
4. 在取得独占构建环境后，先 SDK closest，再 Connector/Worker，再公开 SQL E2E，再全仓。只有正式候选通过必要门槛后才切换生产依赖。SDK 修复、delete 组件 always-close/owned-file cleanup 与实际接线复证、持久化 schema fence 作为原子验收闭包；任何一个仍失败都不能宣称生产级完成。用户未授权对外发布 SDK 或修改另一任务；这两项范围边界不构成本地升级验证需要新增批准。
5. 不在正常路径引入每行复制、额外强制 flush、行预校验或协议改动。原成功写的 flush/压缩/metrics、partition/spec/sort/encryption 语义必须保留；性能与扩展性只能根据实测给结论。

## 8. 可执行正式验收矩阵（当前均 BLOCKED / 未运行）

- SDK 官方受影响 suites：成功 Data/Equality/Position 完整读、默认/小行组、tracked-uncompressed（若选择1.12）、null/required/nested/array/map、decimal 精度/scale、压缩/字典/加密；有序 position 与 referencedDataFiles 不漂移。
- 真故障矩阵：多个字段位置的 model 错误；buffered 与 completed-group 前缀；真实 create/flush/footer/close/abort 故障；清理失败仅重试未完成 phase；原异常 identity/suppressed tree；任何失败永不 DataFile/DeleteFile/result/metrics/splitOffsets；真实 FD/allocator/owner 归零。没有合法真实 Error/双 close 故障时报告 BLOCKED，不添 fake collaborator。
- 原 Connector `IcebergParquetConfigurationIntegrationTest` 8 项及原相关17项，共25原通过 oracle，另 canonical-ID Hadoop/JDBC 声明链。正常/partitioned/unpartitioned/write/read/abort 与默认 lazy 行为不得丢失。
- Worker `NativeWriteExecutorTest`、`WritePageProjectorTest` 及真实 `NativeWriteCleanupFailureIntegrationTest` 75 场景；旧假 sink oracle 必须逐项映射迁移，不能因 SDK 实验18场景通过删除未覆盖的 Error/row-only/双关闭义务。
- 原公开 Deployment2 的真实 INSERT7 → SELECT7L、有限 quota 拒绝/恢复；原公开 ASYNC embedding CREATE/INSERT/CDC/backfill 及原 AI/Lance/Smoke SQL。实验直接 SDK 测试不替代入口 → Planner → JNI → SDK → 文件 → scan → 结果链。
- 获得构建独占后执行规定完整 Java/Native 集成及全仓 CI/release gates；同源 Linux 发行 artifact、真实多进程 distributed/remote-storage 故障验证。macOS SDK 文件测试不算大规模分布式验收。
- 正常写 JMH/TPC-H 与分布式性能基线，记录数据量、worker 数/资源、并发、p95/p99、吞吐、内存/FD 与故障恢复；按仓库性能门槛验收，不用“不影响性能”推断替代数据。

本轮交付允许评审五源候选与补丁来源；尚不允许把“隔离根因已复证”改写成“生产级验收已完成”。
