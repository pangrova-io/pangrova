# Pangrova 远端 Shuffle 分区读取测量

2026-11-14 在 macOS 13.7.8、Apple M2、8 核、16 GiB 内存和 Corretto 17 环境测量。实验比较提交 `5d28b406a8b72522fec2025df5d2196dfb49141b` 中的 Java 读取器与工作区读取器。后者在每个 chunk 到达时执行已发布上界检查，在向消费者确认完成前核对 Coordinator 回执与内容摘要。

## 测量范围

每个 JVM 建立独立的 PostgreSQL 数据库、MinIO bucket 和 mTLS Shuffle 服务，注册一个 producer，用生产写入路径发送四个 Arrow IPC 批次，随后通过 Coordinator 的远端实现发布一个分区。每个批次含 137,000 个递增的 BIGINT。每次读取相同的 548,000 行和 4,453,760 字节。测试夹具的 mTLS relay 观察生产服务给出的帧。两种读取器共用连接池、已提交分区、消费者身份、任务授权和服务端配置。基线的读取器、分区请求和值校验类从 Git 对象中提取，为三种类型加前缀以供同一 JVM 加载。复现脚本另对历史异常类补充当前制品的 import，原读写和验证逻辑保持一致。

每个 JVM 预热六对，随后记录 32 对。每对轮流采用“基线、当前”及“当前、基线”顺序，共运行三个独立 JVM。计时使用单调时钟，从借用连接开始，涵盖打开读取器、读取并核对终态、关闭读取器和归还连接。所有样本逐条核对有效数据字节数和真实终止帧。记录的 protobuf 字节只包括 ReadPartition 响应体；TCP、TLS、HTTP/2 开销未计量。

## 实测结果

| JVM | 读取器 | 样本数 | 中位数 ms | 样本 P95 ms | 吞吐 MiB/s |
|---|---|---:|---:|---:|---:|
| 1 | 基线 | 32 | 166.68 | 194.64 | 24.82 |
| 1 | 当前 | 32 | 164.19 | 181.41 | 25.00 |
| 2 | 基线 | 32 | 189.25 | 279.88 | 20.99 |
| 2 | 当前 | 32 | 189.65 | 315.34 | 20.85 |
| 3 | 基线 | 32 | 155.07 | 176.46 | 26.59 |
| 3 | 当前 | 32 | 153.99 | 172.65 | 27.16 |
| 合并 | 基线 | 96 | 166.68 | 216.37 | 23.90 |
| 合并 | 当前 | 96 | 164.65 | 237.55 | 24.04 |

吞吐量用全部有效载荷除以该读取器所有采样持续时间之和，分位数采用 R7。每次读取都观察到八个数据帧和一个终止帧，合计 4,454,625 个 protobuf 响应字节。三个 JVM 的进程峰值 RSS 分别为 697,466,880、722,599,936、828,014,592 字节，包含两种读取器以及同进程的服务和夹具，无法归因于单种实现。原始 192 个样本见[逐次读取数据](../../pangrova-testing/pangrova-testing-benchmark/results/2026-11-14-shuffle-read.csv)。

本机同期有独立 Maven 构建。第二个 JVM 中两种实现的延迟均高于另外两个 JVM。合并后当前实现的中位数减少 2.03 ms，样本 P95 增加 21.18 ms。这组观测没有独占宿主、整条 SQL 查询延迟或生产尾延迟基线，不能据此设立性能门限。JNI 复制量、集群链路字节、读写背压和 Native 峰值需要独立指标及负载。

## 复现

准备 JDK 17、仓库要求的 SDK 和 Docker，以及本版本通过外部 Shuffle 集成测试的 Maven test classpath。该 classpath 需要包含集成测试的 test-classes、Worker 测试 fixture、所有相关生产类、测试资源和 Maven 依赖。可从刚完成的 `NativeExchangeTransportExternalIntegrationTest` 的 Surefire XML 提取 JVM 实际使用的路径。在源码与编译输出一致时运行下面的命令，输出目录应当尚未存在。

```sh
python3 - <<'PY'
from pathlib import Path
import xml.etree.ElementTree as ET
report = next(Path("pangrova-testing").glob("**/target/surefire-reports/TEST-*NativeExchangeTransportExternalIntegrationTest.xml"))
classpath = ET.parse(report).find(".//property[@name='java.class.path']").get("value")
Path("/tmp/pangrova-shuffle-classpath.txt").write_text(classpath)
PY
python3 pangrova-testing/pangrova-testing-benchmark/scripts/shuffle_read_comparison.py \
  --classpath-file /tmp/pangrova-shuffle-classpath.txt --output /tmp/pangrova-shuffle-read-results
```

本次使用从通过 JNI 握手的测试 JVM 捕获的类路径，在独立目录编译最新拆分的 Provider、Server、Worker 及真实夹具，以隔离并行模块重构。G1 堆大小为 512–1024 MiB。四个批次使用相同的 Arrow 字节，单批 SHA-256 为 `d3adaf21c8c463a2932cba39d17b7e03e78dcde654801980eac4b9b8865e28b2`。原始 CSV 的 SHA-256 为 `2760a5d93141b855e0cc7608c031a4dd542f45a14947c9051b24676e5b5a9267`。

| 测量时的工作区源码 | SHA-256 |
|---|---|
| RemoteShuffleExchangeSource.java | `cdeead8f1d4eae15100395b145dab066fc8b9b46e7237a058585402f278d9707` |
| PangrovaPartitionRead.java | `c08b7e3a6b7dcf1cc1befb27816db7c5a2d25a159bc6fffb68afb7606423a02e` |
| StreamingBatchChunkValidator.java | `fad5728ab9ff8976b35ede82779f35175bbb9efdb8b507e0446a5321f9549219` |
