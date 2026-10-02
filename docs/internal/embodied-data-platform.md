---
title: "具身数据平台目标架构"
description: "面向 Pangrova 的具身数据采集、入库、存储、查询、训练交付与治理完整方案"
---

# 具身数据平台目标架构

> 状态：目标架构与实施建议，不代表当前已经支持。
>
> 基线：2026-09-12 当前工作树（包含现有未提交改动）。当前能力判断以代码、 wiring
> 和端到端测试为准；仓库中的 draft、roadmap、SPI 或孤立实现不单独算作已支持能力。
>
> 受众：Pangrova 技术负责人、架构评审、机器人数据平台负责人和训练基础设施负责人。

## 1. 结论

Pangrova 已具备建设具身数据平台的重要底座：Iceberg/Paimon 湖表连接器、分布式写入、
CDC 与水位线、`TIMESTAMP_NS`、`FILE` 引用、向量/全文/空间索引、血缘、质量规则、
版本化扫描和 Arrow Flight 训练数据服务雏形。但它目前仍是“通用湖仓查询引擎 + AI
数据能力”，不是一个端到端具身数据系统。

缺口不在于再增加一种表格式，而在于补齐以下八个闭环：

1. **边缘采集闭环**：ROS 2/ROS 1、DDS、CAN、相机、力矩、触觉和自定义 SDK 的采集代理，
   包括断网续传、本地 WAL、背压、丢帧显式化和远程会话控制。
2. **时间与空间真值闭环**：设备时钟、发布时钟、接收时钟和入库时钟分离；PTP/NTP/仿真时钟、
   时钟不确定度、漂移、TF 坐标树和标定有效期都成为可查询数据。
3. **具身语义闭环**：dataset、collection、episode、stream、sample、step、observation、action、
   reward、task、annotation、robot embodiment、calibration 不是自由文本，而是稳定的数据模型。
4. **大对象闭环**：媒体/点云不穿过 Coordinator JVM；支持可恢复分片上传、校验和、范围读取、
   视频帧索引、生命周期和对象级授权。
5. **事务与版本闭环**：一次 episode 的多个流和对象要么整体可见，要么整体隔离；数据集版本必须
   固定所有湖表快照、对象清单、schema、标定和派生参数。
6. **查询闭环**：时间范围、ASOF/最近邻时间连接、重采样、插值、坐标变换、轨迹切片、媒体片段、
   3D 空间和语义混合检索需要成为分布式 Native 算子，而不是客户端拼接。
7. **训练交付闭环**：按 episode 的确定性 split、窗口采样、跨模态对齐、并行 shard、解码/预取、
   PyTorch/JAX 适配，以及 RLDS/LeRobot 导入导出。
8. **质量、安全和运营闭环**：采集前检查、在线健康、入库检疫、可用性评分、脸/车牌/语音隐私处理、
   数据许可、血缘、成本、保留和删除证明。

最关键的架构决定是：**保留 Pangrova 的控制面/数据面边界，不把机器人采集或媒体字节塞进
Coordinator。** Edge Agent 负责实时采集和本地持久化；对象存储保存不可变原始事实；Pangrova
Coordinator 负责身份、目录、计划、调度、提交与版本清单；Worker Native 负责解析、对齐、解码、
转换和查询。

## 2. 范围与非目标

### 2.1 范围

- 真实机器人与仿真环境产生的 RGB/深度/视频、音频、LiDAR/点云、IMU、关节状态、末端位姿、
  力/力矩、触觉、控制指令、动作、奖励、事件、日志和自然语言任务。
- 远程操作、示教、自动策略运行、仿真 rollout、故障快照和持续遥测。
- 原始回放、SQL 分析、数据筛选/标注、特征生成、策略训练和评测数据交付。
- 多机器人、多站点、多租户以及同一数据集的迭代版本。

### 2.2 非目标

- Pangrova 不成为硬实时控制器、DDS 实现、视频会议系统或 Flink 类通用流处理引擎。
- 不要求把所有原始数据解码成数据库行；原始容器是事实来源，规范化表是可查询投影。
- 不以单一物理格式同时优化顺序回放、OLAP 扫描、随机训练取样和在线特征服务。
- 不在第一阶段承诺任意 ROS 消息都能获得强类型 SQL；未知 schema 必须仍可无损保存和回放。

## 3. 外部研究结论

### 3.1 原始记录格式

MCAP 是面向异构、带时间戳 pub/sub 消息的模块化容器，能同时保存 schema、channel、message、
压缩 chunk、按 channel 的消息索引、附件、元数据和统计信息；它适合“先无损记录、后按 topic
和时间随机定位”的原始层。[^1] ROS 2 的 rosbag2 已支持 MCAP 存储、录制/回放、按大小或时长
切分、故障前环形缓冲快照以及远程控制，因此自研全新机器人日志格式没有收益。[^2]

采集端不能假定“订阅成功就没有丢数据”。ROS 2 QoS 可在 best-effort 与 reliable、不同 history/
depth/durability 之间组合；传感器 profile 为及时性通常接受 best-effort 和较小队列，QoS 不兼容还
可能直接导致不传输。平台必须记录实际 QoS、publisher 变化和丢消息统计，而不能只记录 payload。[^3]

### 3.2 时间、坐标和标定

ROS 的 ApproximateTimeSynchronizer 本身就说明多传感器输入通常只能按时间戳和容差近似同步，
而不是天然共享同一 frame index。[^4] IEEE 1588 PTP 为网络化测量与控制系统定义精密时钟同步；
即使启用 PTP，也仍需保存 clock domain、锁定状态、offset、drift 和 uncertainty，避免把“配置了
同步”误写成“每个样本绝对同步”。[^5]

tf2 将动态坐标系关系按时间保存在树中，并支持查询任意时间点两个 frame 的变换。[^6] 因此相机
外参、机器人基座、末端、地图和对象位姿不能压成 episode 级一个固定矩阵；它们需要带版本和有效
区间的 frame graph。DROID 后续专门重新发布数万 episode 的改进相机标定，也证明标定是可演进、
有质量差异的一等数据，而不是采集脚本里的常量。[^10]

### 3.3 训练语义与物理布局必须解耦

RLDS 把数据定义为 episode 的集合，每个 episode 包含 steps；step 具有 `is_first`、`is_last`，
可选 observation/action/reward/discount/terminal，且明确区分正常终止与截断。[^7] Open
X-Embodiment 以 RLDS episode 统一不同机构、机器人和任务，说明跨 embodiment 的关键是规范化
语义与映射，不是强迫底层传感器 schema 相同。[^8]

LeRobotDataset v3 将低维高频信号放入 Parquet、视觉数据放入按相机分片的 MP4，并用关系元数据
记录 episode 边界、文件 offset、schema、FPS、统计和任务；用户看到 episode API，但物理层把多个
episode 合并进较大的文件。这一“语义视图与存储布局解耦”应直接采用。[^9] DROID 的原始发布也
采用 episode 元数据 + HDF5 低维轨迹 + 多路 MP4/SVO，并保留相机标定和语言标注，进一步说明
具身数据不是一张宽表。[^10]

### 3.4 湖表、Blob 与训练读取

Iceberg 的 snapshot、time travel、schema/partition evolution、branch/tag 适合数据集版本与可复现
查询；Paimon 的主键合并、changelog、流式读写和 compaction 更适合热增量入库。[^11][^12]
两者都不应直接承担大视频逐帧随机读取。

Lance 的列页布局针对对象存储上的选择性和随机访问；Blob v2 支持完整读取、批量范围读取和懒加载
file-like handle，适合作为派生训练 shard、特征和媒体随机访问加速层。[^13] 它在本方案中是可重建
的加速/交付格式，不是取代 Iceberg/Paimon 目录真值的第二套独立控制面。

Arrow 已定义 fixed/variable-shape tensor 扩展表示，但不同语言实现覆盖度并不一致；因此第一阶段
应以 Arrow/Parquet 可稳定承载的 ROW/LIST/BINARY/FILE 物理表示为兼容底座，并通过语义 schema
描述 shape、dtype、layout、单位和 frame，Native 支持成熟后再升级物理编码。[^14] Arrow Flight
以 gRPC/Protobuf 管控制、以 Arrow RecordBatch 流传数据，并允许多个 endpoint 并行读取，适合作为
训练数据交付协议。[^15]

## 4. Pangrova 当前能力与缺口

### 4.1 已有地基

| 能力 | 当前证据 | 结论 |
|---|---|---|
| 湖表读写与版本 | Iceberg/Paimon connector、snapshot pin、增量 scan、分布式 write/commit | 可复用 |
| 高精度时间 | `TIMESTAMP_NS` parser/type mapping，Iceberg E2E | 可复用，但只有精度，没有 clock semantics |
| 流式入库 | `pangrova-streaming` 的 CDC、offset、水位线、mini-batch、维护 | 可复用骨架，不是传感器采集系统 |
| 文件引用 | `FILE`/`BlobReference`、外置写入、读取校验、预取、孤儿清理 | 部分可复用 |
| 检索 | 向量、全文、R-tree、bitmap/bloom/sparse 等索引 | 可复用 |
| 治理 | OpenLineage 事件、tag、列级 lineage、质量规则和写入 gate | 可扩展 |
| 训练传输 | `TrainingDataServer` + Flight endpoint | 仅雏形 |
| AI/多模态 | Blob embedding、multimodal inference SPI | 与具身采集/episode 语义不同，仅可复用 provider 层 |

### 4.2 代码审计发现

| 缺口 | 代码证据 | 风险 |
|---|---|---|
| 原始流记录只有毫秒时间戳 | `SourceRecord.timestamp` 明确为 epoch milliseconds；没有 capture/publish/receive/clock domain/uncertainty | 高频数据失去时间真值 |
| streaming 定位是 CDC 表刷新 | `docs/architecture/streaming.md` 明确不是通用流处理，状态只有 offset + watermark | 不能把 CDC API 当机器人采集 API |
| ASOF 尚未进入物理执行 | `PhysicalJoinConverter` 对 `ASOF`、`LEFT_ASOF` 直接拒绝 | 多传感器对齐只能落到客户端 |
| Blob 存储合同不完整 | `FileSystemAccessor` 的 multipart、presign、checksum 是 default fallback/empty；接口位于 index 包 | 无法证明生产级大对象上传、范围读和对象级授权 |
| 训练请求与扫描脱节 | `TrainingDataRequest` 接受 version/split；`TrainingDataScanner.openScan()` 只把 columns 与 samplingRate 带入 ScanConfig，仍以 `DynamicFilter.ALL` 获取 split | 版本 pin 和 train/val/test split 尚未真正约束读取 |
| sampling 非确定 | `TrainingDataScanner` 使用 `ThreadLocalRandom` 在 page 粒度跳过 | 同一版本重复训练不能复现，且不是 episode/row 均匀采样 |
| Feature Store 只有合同 | `FeatureStore` 标记 Experimental；仓库内只有测试 stub，没有生产实现 | 不能承诺 point-in-time feature serving |
| 目录没有具身资产 | `AssetType` 只有 TABLE/VIEW/MATERIALIZED_VIEW/FUNCTION/MODEL/VOLUME | DATASET/version 不是一等治理对象 |
| 缺少领域实体 | 全仓库没有 episode/trajectory/robot/calibration/MCAP/ROS 业务实现 | 无法表达完整性、质量、回放和跨 embodiment 映射 |
| 缺少点云/张量物理合同 | SPI 类型以标量、ROW/ARRAY/MAP/VARBINARY/VECTOR 为主 | 不能在 shape/layout/单位/frame 上做强校验 |

结论应谨慎表述为：**通用湖仓与 AI 数据底座已有较多可复用实现，但具身领域闭环目前只覆盖少数
基础点，尚不能对外承诺端到端支持。**

## 5. 目标架构

```text
 Robot / Simulator
 drivers -> ROS/DDS/CAN/custom SDK
        |
        v
+------------------------- EDGE DATA PLANE --------------------------+
| Edge Agent: topic discovery | QoS capture | clock monitor          |
| local WAL/MCAP | pre-trigger ring | segmenter | loss accounting     |
| schema/calibration snapshot | resumable uploader | remote control   |
+-------------------+----------------------+-------------------------+
                    | control              | direct data
                    v                      v
+---------------- JVM CONTROL PLANE --+  +------ IMMUTABLE STORAGE --+
| Ingest Gateway / Coordinator        |  | raw MCAP/SVO/video/audio  |
| identity, quota, lease, manifest    |  | checksummed content keys  |
| validation jobs, snapshot binding  |  | derived Lance/artifacts   |
| episode commit ledger, catalog/RBAC |  +-------------+-------------+
+-------------------+-----------------+                |
                    | typed tasks                       |
                    v                                   v
+---------------------- NATIVE DATA PLANE ---------------------------+
| MCAP indexed scan | decode | temporal align/resample | TF transform |
| canonicalize | quality metrics | media/point-cloud slice | indexes  |
| distributed write -> Worker fragments -> Coordinator atomic commit |
+--------------------------+-----------------------------------------+
                           v
+----------------------- LAKEHOUSE / SERVING ------------------------+
| Hot canonical tables (Paimon) | Curated/versioned tables (Iceberg) |
| SQL/JDBC/REST | Flight episode shards | RLDS/LeRobot export         |
+-------------------------------------------------------------------+
```

### 5.1 绝对边界

- **Edge Agent** 参与实时订阅和原始字节持久化；Pangrova Coordinator 不订阅 ROS topic。
- **Coordinator JVM** 只处理 descriptor、manifest、schema、授权、计划、任务状态和 commit metadata；
  不接收视频、点云、MCAP chunk 或训练 batch。
- **Worker JVM** 管生命周期、凭据和 JNI；不在 Java heap 中做媒体解码或时间对齐。
- **C++ Native Worker** 执行所有数据相关解析、对齐、变换、过滤、索引、转换和分布式写。
- **对象存储** 保存不可变原始对象；“上传成功”不等于“episode 可见”，可见性由提交清单决定。
- **最终湖表/目录提交** 仍由 Java Connector SPI 完成，保持现有分布式写入边界。

### 5.2 新组件

#### Edge Agent

建议独立发布 C++/Rust 边缘二进制，而不是把 ROS 依赖引入 Pangrova Java 模块。

- Adapter：ROS 2、ROS 1 bridge、DDS、CAN/CAN-FD、EtherCAT 只读 tap、RTSP/GStreamer、文件导入、
  simulator SDK；首期只承诺 ROS 2 + MCAP。
- Recorder：按 topic/QoS/schema 记录 MCAP，支持时间/大小切片、pre-trigger 环形缓冲、snapshot、
  pause/resume/seal。
- Local journal：episode 状态、每个 chunk 的序号范围、大小、checksum、上传 part、重试和确认回执。
- Clock monitor：记录 PTP/NTP/ROS `/clock` 状态、设备 boot id、offset/drift/uncertainty 和时间跳变。
- Health：topic 期望频率、实际频率、丢帧、队列深度、写盘延迟、磁盘剩余、CPU、热降频。
- Uploader：大对象直接 multipart 到对象存储；控制面只签发能力受限的 upload session。
- Safety：默认只读订阅；不能向控制 topic 发布；边缘凭据限定 dataset/session/prefix 和 TTL。

#### Ingest Gateway

- `CreateCollection`、`OpenEpisode`、`CreateUpload`、`ReportChunk`、`SealEpisode`、`ResumeEpisode`、
  `GetEpisodeStatus`。
- mTLS/设备身份、租户限额、并发/带宽令牌、策略版本和审计。
- 只接受 manifest 和小型统计；返回 presigned multipart 或短期 scoped credential。
- 将 `EPISODE_SEALED` 事件写入持久队列，触发验证、规范化和索引任务。

#### Episode Commit Coordinator

- 持久化状态机、租约、attempt、幂等键和 fencing token。
- 校验 expected object set、checksum、schema、时间范围、序号覆盖、标定引用和质量 gate。
- 调度 Native materialization，聚合各表 write fragments。
- 原子发布 dataset-version manifest；失败对象保持不可见并由生命周期任务清理。

#### MCAP Connector 与 Native Reader

- 新增只读 `pangrova-connector-mcap`，只包含 connector/SPI 层的 split、schema、统计和谓词下推。
- C++ Native Reader 使用 MCAP summary/chunk/message index 下推 `topic` 与 `log_time` 范围。
- 未知 schema 返回 envelope + raw bytes；已注册 ROS/Protobuf/JSON schema 可投影强类型列。
- 支持截断文件的受控恢复扫描，但必须标记 `recovered=true` 和 coverage，不得静默当完整文件。

#### Embodied Catalog

- `DATASET` 作为一等 Asset；episode、stream、calibration 是 dataset 下的领域记录，不扩张为大量
  顶层 catalog object。
- dataset version manifest 固定所有组件快照和参数，是训练、评测、导出和审计的唯一版本入口。

## 6. 采集与入库协议

### 6.1 Episode 状态机

```text
CREATED -> RECORDING -> SEALING -> SEALED -> VALIDATING -> COMMITTING -> COMMITTED
              |             |          |           |            |
              v             v          v           v            v
          INTERRUPTED    INCOMPLETE  QUARANTINED  FAILED      SUPERSEDED
```

- `INTERRUPTED` 可由同一 device/boot/episode/lease 续录。
- `INCOMPLETE` 表示明确缺少对象或流，不得伪装成完整数据；策略可允许进入失败样本集。
- `QUARANTINED` 保存证据和质量原因，仅授权用户可查询。
- `COMMITTED` 之后原始对象不可覆盖；修复通过新 dataset version 或 replacement relation 完成。
- `SUPERSEDED` 不物理删除，直到引用它的所有版本、训练 run 和保留策略解除。

### 6.2 幂等与覆盖

每条样本至少拥有：

```text
tenant_id, dataset_id, collection_id, episode_id,
robot_id, boot_id, stream_id, sequence_number,
schema_id, capture_time_ns, publish_time_ns, receive_time_ns,
clock_domain_id, payload_checksum
```

稳定幂等键：

```text
sample_key = hash(tenant_id, robot_id, boot_id, stream_id, sequence_number)
chunk_key  = hash(sample_key_range, payload_checksum)
```

不能只用 event time 去重：设备重启、时间回拨、多 publisher 或同步误差都会产生相同时间戳。写入必须
报告 `expected_seq_range`、`observed_seq_range`、重复、缺口和越序；即使源 QoS 允许丢帧，丢失也要
成为数据而不是日志。

### 6.3 大对象上传

1. Gateway 为已授权的 episode/stream 创建 upload session。
2. Edge Agent 分片上传并本地记录每个 part receipt/checksum。
3. 对象使用不可变 content key；不得依赖对象存储的 rename 模拟事务。
4. Complete 时服务端验证 full/composite checksum。S3 multipart 的 ETag 不等同于整个对象 MD5，
   因此不能把 ETag 当内容校验和。[^16]
5. Edge 上报 object descriptor；Gateway HEAD 验证 size、checksum、encryption/version id。
6. 只有 sealed manifest 引用的对象才进入 VALIDATING；未被引用的 part/object 由 TTL 清理。

### 6.4 背压与离线模式

- 控制面发放 credits：允许的 bytes、chunks、episodes、并发 multipart、到期时间。
- Edge 分级降载：保留控制/状态/诊断 > 降低预览码率 > 减少非关键 camera > 停止新 episode；
  不允许静默丢弃关键 action/state。
- 断网时先落本地不可变 segment；本地容量达到水位时按明确策略停止采集或仅覆盖 pre-trigger ring，
  不能删除已 sealed 未上传 episode。
- 数据与控制分离：高吞吐 payload 直达对象存储；Kafka/消息总线只传 manifest/event 或小型 batch。

## 7. 数据模型

### 7.1 四层模型

| 层 | 内容 | 可变性 | 主要用途 |
|---|---|---|---|
| Raw | MCAP/SVO/原视频/音频/设备日志/仿真资产 | 不可变 | 法证、重放、重新解码 |
| Canonical | stream/sample、低维状态、动作、TF、标定、质量 | append + 受控修正 | SQL、分析、派生 |
| Aligned | 特定 AlignmentSpec 产生的 step/transition/window | 可重建、版本化 | 训练、评测 |
| Feature | embedding、检测、分割、语言标注、统计、索引 | 可重建、模型绑定 | 检索、筛选、训练 |

Raw 永远先于 Aligned。不得因为当前训练策略只需要 10 Hz 而丢弃 200 Hz 原始状态；也不得把
nearest-neighbor 对齐结果写回覆盖原始时间戳。

### 7.2 核心实体

| 实体 | 关键字段 |
|---|---|
| Dataset | id、owner、purpose、license、policy、default branch、retention |
| DatasetVersion | version id、parent、component snapshots、object manifest hash、schema set、quality policy、created by |
| RobotEmbodiment | vendor/model/serial、URDF/SDF/USD ref、joint/action spaces、firmware/software |
| Sensor | stable id、type、serial、frame id、topic、schema、nominal rate、QoS、units |
| Calibration | calibration id、method、parameters、quality metrics、valid from/to、frame graph edges |
| Collection | site、operator/teleop policy、environment、campaign、consent/license |
| Episode | start/end、outcome、termination reason、task、robot、completeness、quality score、commit state |
| Stream | episode、sensor/channel、schema version、clock domain、codec、sample count、loss count |
| Chunk/Object | URI、version id、offset/length、min/max time、seq range、checksum、encryption、media type |
| Sample | stream、sequence、四类时间、frame id、inline value 或 payload ref、quality flags |
| Step | episode、step index、reference time、observation/action/reward/discount、terminal/truncated、alignment spec |
| Annotation | target episode/time range/object、taxonomy、value、annotator/model、confidence、status、provenance |

### 7.3 时间模型

必须区分：

- `device_time_ns`：设备单调或硬件时钟，配合 `boot_id` 才有意义。
- `capture_time_ns`：曝光/采样发生时刻；能拿到时优先作为物理事件时间。
- `publish_time_ns`：publisher 放入消息的时间。
- `receive_time_ns`：Edge Agent 收到消息的时间。
- `ingest_time`：云端接受 descriptor/commit 的时间。
- `clock_domain_id`：PTP domain、ROS time、system UTC、device monotonic、simulation 等。
- `clock_offset_ns`、`drift_ppb`、`uncertainty_ns`、`sync_state`：时间转换证据。

`TIMESTAMP_NS` 用于规范化的时间点；设备单调计数用 `BIGINT`，不能伪装成 UTC timestamp。所有对齐
结果都要带 `source_sample_key`、`delta_ns` 和 `alignment_quality`，使误差可审计。

### 7.4 规范化 step

```sql
CREATE TABLE embodied.aligned_steps (
    dataset_version_id VARCHAR,
    episode_id VARCHAR,
    step_index BIGINT,
    reference_time TIMESTAMP_NS,
    task_id VARCHAR,
    observation ROW<
        joint_position ARRAY<DOUBLE>,
        joint_velocity ARRAY<DOUBLE>,
        end_effector_pose ROW<x DOUBLE, y DOUBLE, z DOUBLE,
                              qx DOUBLE, qy DOUBLE, qz DOUBLE, qw DOUBLE>,
        camera_refs MAP<VARCHAR, FILE>
    >,
    action ROW<space VARCHAR, values ARRAY<DOUBLE>>,
    reward DOUBLE,
    discount DOUBLE,
    is_first BOOLEAN,
    is_last BOOLEAN,
    is_terminal BOOLEAN,
    is_truncated BOOLEAN,
    validity MAP<VARCHAR, BOOLEAN>,
    source_delta_ns MAP<VARCHAR, BIGINT>,
    alignment_spec_id VARCHAR,
    calibration_id VARCHAR
);
```

这是一种逻辑 schema，不要求第一阶段直接执行上述完整 DDL。关键约束是 observation 和 action space
必须由 schema registry 描述 shape、dtype、单位、上下界、坐标 frame 和语义；不能只存一个无名
`ARRAY<DOUBLE>`。

### 7.5 Tensor、图像、视频与点云

- 小型定长向量继续用 `VECTOR` 或定长数组；关节数依 embodiment 变化时用带 schema 的 ARRAY。
- 图片/视频/音频/点云大 payload 用 `FILE`/Blob reference；行内 bytes 仅允许低于策略阈值的小对象。
- point cloud descriptor 保存 fields、dtype、offset、endianness、width/height、point/row stride、frame id；
  ROS `PointCloud2` 本身也是“schema 描述 + binary blob”，可无损映射。[^17]
- Tensor 逻辑类型保存 element type、shape、layout、dim names、endianness 和 compression。第一阶段落为
  ROW + Blob；Arrow canonical extension 在 Java/C++/connector 全链支持后再引入 `TENSOR` SQL 类型。
- 视频对象旁保存 frame index：frame number、PTS/DTS、keyframe offset、capture time 和 episode time；
  查询某个时间窗只读取相关 byte ranges/GOP，不下载完整视频。

## 8. 物理存储设计

### 8.1 格式分工

| 数据 | 推荐格式 | 角色 |
|---|---|---|
| 原始 ROS/DDS 消息 | MCAP（zstd/lz4 chunk + index） | 权威回放事实 |
| 原始相机专有流 | 原生 SVO/厂商格式 + MCAP 引用 | 保留无损设备能力 |
| 低维 sample/action/TF/quality | Parquet through Paimon/Iceberg | SQL 与批量扫描 |
| 训练视觉 | MP4/fMP4 shard + frame index | 高压缩顺序/窗口读取 |
| 大图片/音频/点云/tensor | 对象 Blob 或 Lance Blob | 懒加载/范围读取 |
| Aligned steps/features | Iceberg/Paimon；可物化 Lance shard | 版本化训练与随机取样 |
| ANN/全文/时间/空间 index | Pangrova immutable artifact generation | 可重建加速 |

不要把 Paimon、Iceberg 和 Lance 都当独立真值。建议：

- 热 canonical table 默认 Paimon，适合持续写、主键合并和 changelog。
- 冻结/共享的数据集版本默认 Iceberg，利用 snapshot/tag/branch 和生态兼容。
- Lance 是从固定 dataset version 生成的训练/检索 artifact；manifest 记录 source snapshot 和生成参数。
- 团队若只运维一种湖表，也可全选 Iceberg 或全选 Paimon；数据模型与提交协议不能依赖双格式。

### 8.2 分区、排序与小文件

- episode 元数据按 `dataset_id` + collection date 分区；不要按 `episode_id` 建海量目录。
- 高频 sample 按 `dataset_id/day/bucket(episode_id)` 分区，文件内排序 `(episode_id, stream_id,
  event_time_ns, sequence_number)`。
- aligned step 按 `dataset_version_id/bucket(episode_id)` 分区，保证 episode 局部性。
- annotations/features 按 dataset version + artifact/model generation 分区。
- MCAP 按大小或时长 segment；阈值由设备吞吐和对象存储 request cost 基准确定，不写死在协议。
- ingestion job 只产中等 segment；compaction 异步执行且必须维护 episode/frame locator 映射。

### 8.3 数据集版本清单

```json
{
   "dataset_version_id": "dv_...",
  "parents": ["dv_..."],
   "tables": [
    {"name": "canonical_samples", "format": "iceberg", "snapshot": "..."},
    {"name": "aligned_steps", "format": "iceberg", "snapshot": "..."}
   ],
   "objects_manifest_sha256": "...",
   "schemas": ["schema_..."],
   "calibration_set": "calset_...",
   "alignment_spec": "align_...",
   "quality_policy": "quality_...",
   "annotation_snapshot": "...",
   "code_revision": "..."
}
```

多表无法依赖一次普通湖表 commit 获得跨表原子性。解决方式是：各组件先产生不可变 snapshot，最后
在 Metadata Store 中原子发布唯一 dataset-version manifest。读取端先解析 manifest，再按固定
snapshot 打开组件；没有被 COMMITTED manifest 引用的组件不可见，并由 orphan cleanup 回收。

## 9. 查询与计算能力

### 9.1 首期 SQL 表面

```sql
-- 原始容器按 topic 和时间索引读取
SELECT topic, log_time, publish_time, schema_name, payload
FROM TABLE(read_mcap(
    uri => 's3://.../episode.mcap',
    topics => ARRAY['/joint_states', '/camera/front/image'],
    start_time => TIMESTAMP_NS '2026-09-01 10:00:00.000000000',
    end_time => TIMESTAMP_NS '2026-09-01 10:00:05.000000000'));

-- 找到每个图像之前最近的机器人状态，并限制最大时间误差
SELECT c.episode_id, c.capture_time, c.image_ref, s.joint_position,
       c.capture_time - s.capture_time AS delta
FROM camera_frames c
ASOF LEFT JOIN joint_states s
   ON c.episode_id = s.episode_id
 AND c.capture_time >= s.capture_time
WITHIN INTERVAL '5' MILLISECOND;

-- 可复现重采样；不同字段使用不同策略
SELECT *
FROM TABLE(resample_episode(
    dataset_version => 'dv_...',
    episode_id => 'ep_...',
    frequency_hz => 30,
    numeric_method => 'LINEAR',
    quaternion_method => 'SLERP',
    discrete_method => 'PREVIOUS',
    max_gap => INTERVAL '20' MILLISECOND));
```

### 9.2 对齐规则

| 数据 | 允许策略 | 禁止行为 |
|---|---|---|
| joint position/velocity | nearest、linear、previous | 跨大 gap 插值 |
| quaternion/pose | nearest、SLERP + translation linear | 对 quaternion 分量直接线性插值且不归一化 |
| action/action mode | previous/nearest | 构造不存在的中间类别 |
| image/video frame | nearest/previous，返回 delta | 合成帧冒充原始观测 |
| force/tactile | nearest、window aggregate | 忽略采样带宽与接触峰值 |
| reward/terminal/event | exact/window semantic rule | 在时间轴上任意扩散 |

每个 AlignmentSpec 必须版本化，包含 reference stream、frequency、方向、tolerance、各字段策略、
gap policy、clock conversion 和 calibration set。输出必须保留每个源样本 locator 与 delta。

### 9.3 Native 算子

- `McapScanRel`：topic/time/channel/schema 下推和 coverage report。
- `TemporalAsOfJoinRel`：按 `(episode_id, time)` 排序的分区 merge-asof，支持 direction/tolerance。
- `ResampleRel`：窗口与插值策略；按 episode 分区，不能跨 episode 状态。
- `FrameTransformRel`：按时间解析 frame graph，应用 pose/point/twist 变换。
- `BlobRangeScanRel`：按 locator/byte range/GOP 读取，Worker credential scoped。
- `EpisodeWindowRel`：生成 history/action horizon，保证窗口不越 episode 与 terminal。
- `PointCloudFilterRel`：解码选定字段、坐标变换、3D box/radius/voxel filter。
- `CoverageRel` 或通用执行 envelope：返回缺流、时间 gap、过滤/恢复记录数和索引覆盖。

这些算子经 parser/planner -> typed Substrait extension -> protocol -> JNI -> Native -> Arrow result
全链实现。不得以 JSON hint、Java 侧 row loop 或“无索引时 Coordinator 下载文件”作为 fallback。

### 9.4 索引

- L0：对象/segment 的 episode、topic、min/max time、seq range、message count、schema、checksum。
- L1：MCAP 内置 chunk/message index，视频 keyframe/frame-time index，Blob range map。
- L2：湖表 partition/file/row-group min-max、bloom/bitmap/sparse index。
- L3：时间+episode 复合 lookup，3D R-tree/voxel，向量 ANN，全文任务/标注。
- 所有索引绑定 `source_snapshot + object_manifest_hash + schema/calibration generation`，过期时 fail
  closed 或走带明确 coverage 的 Native exact scan，不得返回混合世代结果。

## 10. 训练、评测与回放交付

### 10.1 Training Data Service v2

当前 Flight 服务可以保留，但 request contract 需要从 table scan 升级为 dataset/episode contract：

```json
{
   "dataset_version": "dv_...",
  "selection": "quality.valid AND task_id IN (...) ",
  "split": {"key": "episode_id", "seed": 42, "train": 0.8, "val": 0.1},
  "window": {"history": 4, "action_horizon": 16, "stride": 1},
  "modalities": ["camera.front", "robot.state", "action"],
  "decode": {"camera.front": "RGB_UINT8"},
  "shuffle": {"seed": 7, "epoch": 3, "buffer": 4096},
  "output": "ARROW_EPISODE_BATCH"
}
```

必要语义：

- version 必须真正进入 connector snapshot handle，而不是只校验字符串。
- split 在 episode 层确定性计算，避免同一 episode 泄漏到 train/val/test。
- sampling 使用稳定 hash/seed，不用 `ThreadLocalRandom`；同一 manifest + seed + epoch 可复现。
- Flight endpoint 按 episode shard 划分并返回真实 schema/estimated bytes/rows；客户端并行消费。
- Worker 做 predicate、window、decode、augmentation 和 prefetch；Coordinator 不 materialize batch。
- Blob 可返回 scoped URI + checksum，也可按策略在 Worker 解码成 tensor；结果记录 decode generation。
- backpressure、cancel、retry、endpoint expiration 和 worker attempt fencing 是协议一部分。

Arrow Flight 支持多个 endpoint 并行下载，但不会自动提供上述 episode、确定性 split 和版本语义；
这些必须由 Pangrova contract 实现。[^15]

### 10.2 兼容出口

- **LeRobot v3 exporter/importer**：Parquet 低维帧、按相机 MP4 shard、episode offsets、tasks、stats、
  schema/FPS；导出清单绑定 Pangrova dataset version。[^9]
- **RLDS exporter/importer**：episode/steps、terminal/truncated、observation/action/reward/discount 映射；
  保留不能无损映射的 extension metadata。[^7]
- **MCAP passthrough**：原始回放无需重编码；支持 topic remap、时间窗和 schema registry。
- **PyTorch/JAX client**：可迭代 episode/window dataset、worker-aware sharding、seeded shuffle、异步
  prefetch、zero-copy where possible。
- **仿真回放**：导出 URDF/SDF/USD、TF、calibration、clock mode 和 control commands；记录 simulator
  version/seed/asset hashes。2026 年 ROS 的 OpenUSD 互操作约定仍是 draft，采用时必须保存 profile
  版本并允许迁移。[^18]

## 11. 数据质量、治理与安全

### 11.1 三段质量门

**采集前**：设备/serial、schema、topic 数、QoS 兼容、相机曝光/FPS、标定新鲜度、磁盘/网络、
时间同步、机器人安全状态。

**采集中**：频率、jitter、drop/duplicate/out-of-order、时间跳变、image corruption、TF 可连通、
action-state lag、关节 limit、急停/故障、操作员可见性、磁盘和上传 backlog。

**入库后**：checksum、对象完整性、MCAP index/summary、episode 边界、必需流覆盖、跨流 skew、
标定重投影误差、NaN/Inf、单位/shape、terminal/truncated、语言/任务、隐私和 license。

质量结果不是一个 boolean，而是：

```text
quality_score, policy_version, hard_failures[], warnings[],
per_stream_metrics{}, usable_for{replay, training, evaluation, safety_analysis}
```

### 11.2 隐私与安全

- 人脸、车牌、屏幕、语音和精确位置为敏感 modality；支持原始受限区与脱敏派生区分离。
- 脱敏派生必须记录 model/version/threshold、覆盖范围、人工复核和 source object；不得覆盖原始对象。
- 访问控制同时约束 dataset、episode 行、字段和 Blob object；presigned URL TTL 短且绑定方法/范围。
- 加密 envelope 与 key reference 入清单；密钥不入表、不进 task proto 明文、不写日志。
- dataset 记录采集同意、用途、地域、license、保留、legal hold 和删除策略。
- 删除采用 tombstone -> version 引用检查 -> object/table purge -> deletion certificate；被训练 run pin 的
  数据先阻止删除或触发明确的模型治理流程。
- Edge Agent 是潜在供应链入口：签名发布、最小权限、远程配置审计、adapter allowlist、禁止执行
  数据里携带的脚本/指令。

### 11.3 血缘

血缘粒度至少包含：

```text
raw object/channel/range
 -> canonical rows
 -> aligned step/window
 -> annotation/feature/model generation
 -> dataset version/export/training run
```

每个派生任务记录 source snapshots、object hash、code revision、schema、calibration、alignment spec、
model and parameters。只有 table-level lineage 无法回答“这个错误动作来自哪个相机片段和哪版标定”。

## 12. API 与 SPI 建议

### 12.1 不复用 index FileSystemAccessor 作为大对象主合同

建议在 `pangrova-spi` 新建 provider-neutral `storage` 合同：

```java
interface BlobStore {
    BlobWriteSession beginWrite(BlobWriteRequest request);
    BlobWriteReceipt complete(BlobWriteSession session, List<PartReceipt> parts);
    BlobMetadata head(BlobLocator locator);
    BlobInput openRange(BlobLocator locator, long offset, long length);
    ScopedAccess grantRead(BlobLocator locator, Duration ttl, Optional<ByteRange> range);
    void abort(BlobWriteSession session);
}
```

`BlobLocator` 包含 storage namespace、immutable object key、version id、size、checksum algorithm/value、
media type 和 encryption key reference。S3/OSS/GCS/HDFS 细节只出现在实现模块，不能泄漏进 SPI。

### 12.2 Embodied ingestion contract

不要扩张现有毫秒级 `SourceRecord`。新增独立合同：

```java
record SensorBatch(
    DatasetId dataset,
    EpisodeId episode,
    StreamId stream,
    SchemaId schema,
    ClockDomain clockDomain,
    long firstSequence,
    long lastSequence,
    long minCaptureTimeNs,
    long maxCaptureTimeNs,
    BatchPayload payload,
    BatchIntegrity integrity) {}
```

`BatchPayload` 只能是小型 columnar batch 或 immutable blob/chunk locator。该合同承载 capture semantics，
不伪装为数据库 CDC envelope。

### 12.3 Schema registry

- schema identity 使用 canonical bytes + compatibility policy 的 hash，不用可变名称。
- 保存原始 ROS message definition/IDL/Protobuf descriptor/JSON Schema，以及 canonical field mapping。
- compatibility 分 `RAW_READABLE`、`CANONICAL_BACKWARD`、`TRAINING_COMPATIBLE`，不能只用一个
  backward-compatible boolean。
- action/observation space 记录 dtype、shape、单位、范围、frame、normalization 和缺失语义。
- 未知 schema 允许 raw ingest；强类型 canonicalization 进入 quarantine/pending decoder，不阻塞原始保全。

## 13. 模块落点

| 模块 | 变更 |
|---|---|
| 新 `pangrova-embodied` | 领域模型、episode 状态机、manifest、质量策略、schema/alignment registry；新增模块需同步更新架构依赖矩阵 |
| `pangrova-spi` | BlobStore、SensorBatch、dataset version、temporal/coverage contracts；保持 framework-neutral |
| 新 `pangrova-connector-mcap` | MCAP split、schema/channel metadata、topic/time predicate pushdown |
| `pangrova-streaming` | 复用 durable offsets、mini-batch、watermark、maintenance；不加入任意 stateful stream join |
| `pangrova-metadata` | `DATASET` asset、dataset version manifest、episode commit ledger、calibration/schema records |
| `pangrova-parser` | `ASOF ... WITHIN` 与具身 table function 语法 |
| `pangrova-planner` | 时间排序/分区要求、typed custom relations、cost、snapshot/artifact binding |
| `pangrova-protocol` | ClockDomain、McapScan、TemporalAsOfJoin、Resample、FrameTransform、BlobRange、coverage report |
| `pangrova-worker` | scoped credential、JNI lifecycle、task/resource accounting；不解析媒体 |
| `pangrova-worker-native` | MCAP reader、时间对齐、TF/pose、Blob range、视频/点云选择性解码、window/shard |
| `pangrova-connector-*` | 固定 snapshot read、幂等 batch/episode commit metadata、orphan cleanup |
| `pangrova-indexing` | episode/time/channel/frame/3D index generation 与一致性检查 |
| `pangrova-governance` | modality policy、episode-level lineage、质量 gate、隐私与删除证明 |
| `pangrova-observability` | edge/ingest/quality/training SLO metrics 和 trace correlation |
| `pangrova-coordinator` | Ingest control、commit/validation 调度、Training Data Service v2；不接触 payload |

依赖红线：connector 仍只依赖 SPI/common/protocol；Java/C++ 仅通过 protocol + JNI；新领域模块不能
反向依赖 coordinator/worker 内部类；ROS/MCAP 实现名不能泄漏进通用 SPI 类型。

## 14. 实施路线图

### M0：纠正已有合同

- 让 Training Data Service 的 version、split 真正进入 scan；替换非确定随机 page sampling。
- 明确 FILE/Blob 生产能力矩阵：multipart、checksum、range、presign、encryption、abort。
- 增加 timestamp 四元组和 clock domain 设计，不再扩展毫秒 `SourceRecord`。
- 决定首期单一湖表主路径（Iceberg 或 Paimon）并冻结 dataset version manifest schema。

退出条件：同一 snapshot + split seed 两次读取产生相同 episode 集；大对象失败重传不产生可见孤儿；
代码与文档不再把现有 Flight/FILE SPI 表述为完整生产能力。

### M1：最小无损闭环

- ROS 2 Edge Agent、MCAP recorder、local journal、direct multipart upload。
- collection/episode/stream/chunk 元数据和 commit ledger。
- MCAP connector + Native topic/time scan。
- checksum、seq gap、topic coverage、clock health 基础质量 gate。
- 原始 episode SQL 发现、下载与 rosbag2/Foxglove 回放。

退出条件：断网、进程崩溃、重复 seal、重复上传和 Coordinator failover 下，无 silent loss、无重复可见
episode、无部分 episode 冒充成功；原始 MCAP 可由标准工具回放。

### M2：规范化与对齐

- robot/sensor/schema/calibration/TF 模型。
- canonical state/action/TF/sample tables。
- Native ASOF、resample、SLERP、frame transform 和 coverage。
- dataset version manifest 跨对象/表 snapshot 发布。
- DROID/OXE 风格 episode importer 作为真实数据验证集。

退出条件：对齐 oracle、时间回拨、跨 boot、缺帧、标定切换和 schema evolution 的属性/E2E 测试通过；
任意 aligned step 能追溯到源 channel/range/sample。

### M3：训练与数据运营

- Training Data Service v2、episode/window shard、deterministic shuffle、Worker decode/prefetch。
- LeRobot v3/RLDS import/export、PyTorch/JAX client。
- annotation/taxonomy/review、embedding/feature generation、active sampling。
- 质量 dashboard、隐私派生、license/retention/delete workflow。

退出条件：固定 manifest 可复现实验；train/val/test 无 episode 泄漏；多 Worker 重试/扩缩容不改变样本
集合；导出能被官方 LeRobot/RLDS loader 读取。

### M4：规模与高级 modality

- Lance derived shards、Blob range/GOP、点云字段投影/3D 索引、Tensor Native 类型。
- edge fleet rollout、跨地域复制、冷热分层、灾备、成本归因。
- simulation asset/rollout/replay、在线 feature serving 与硬负样本闭环。

退出条件：PB 级 manifest/snapshot 规划、百万 episode 元数据、生产形态吞吐/随机取样、故障注入和长稳
测试达到经基准确认的 SLO。

## 15. 验证与验收

### 15.1 正确性不变量

1. COMMITTED episode 引用的每个必需对象存在、size/checksum 匹配。
2. 未被 COMMITTED dataset-version manifest 引用的 snapshot/object 不会被默认查询看到。
3. `(robot, boot, stream, sequence)` 在一个 dataset version 内至多可见一次。
4. 原始 capture/publish/receive 时间永不被对齐或时区转换覆盖。
5. 对齐结果不跨 episode/boot/clock discontinuity；gap 超限返回 missing，不伪造值。
6. 同一 manifest、selection、split seed、shuffle seed 和 epoch 产生相同逻辑样本序列。
7. 每个 step/window/feature 都可追到 source locator、schema、calibration 和 code/model generation。
8. Worker/Coordinator 重试只能提交同一个 fenced attempt；迟到 attempt 不能发布。
9. 权限撤销后新 URL/Flight ticket 不再签发；已有短期能力到期且可审计。
10. 索引缺失/过期只允许显式 Native exact scan 或 fail closed，不允许不完整结果。

### 15.2 测试矩阵

| 层 | 测试 |
|---|---|
| Format | 官方/真实 MCAP golden、unknown schema、compressed chunk、truncated/corrupt index、attachment |
| Property | sequence coverage、clock conversion、状态机、idempotency、alignment tolerance、split determinism |
| Connector | topic/time pushdown、snapshot pin、schema evolution、object version/checksum |
| Native | ASOF/SLERP/TF/point-cloud oracle、spill、range read、cancellation、memory ownership |
| Cross-language | proto/descriptor golden、Java/C++ type parity、JNI lifecycle、coverage report |
| E2E | ROS publisher -> Edge -> object store -> commit -> SQL -> Flight -> official loader |
| Chaos | 断网、磁盘满、Edge crash、part 丢失、重复消息、Coordinator failover、Worker crash、clock jump |
| Security | prefix escape、SSRF、expired URL、cross-tenant locator、malicious schema/MCAP、zip/decode bomb |
| Performance | sustained ingest、seal latency、manifest planning、time-range scan、random episode/window fetch、decode |

### 15.3 SLO 设定方法

不在没有硬件、codec、网络和对象存储基准时伪造吞吐数字。先按以下公式建立容量模型：

```text
raw_ingress_bytes_per_sec = sum(stream_rate * average_payload * protocol_overhead)
daily_storage = raw_ingress * duty_cycle * 86400 * replication / compression_ratio
edge_buffer_hours = usable_local_bytes / peak_ingress_bytes_per_sec / safety_factor
training_required_read = workers * samples_per_sec * bytes_per_sample / cache_hit_ratio
```

首轮基准后冻结四类服务目标：

- durability：zero silent loss；所有缺口可量化；checksum failure 必须 quarantine。
- freshness：seal-to-query-visible 与 seal-to-training-ready，按 raw/canonical/aligned 分开。
- serving：metadata query、time-range scan、random episode start、sustained training read 的 p50/p95/p99。
- efficiency：每 TB ingest/query/train 的 object requests、CPU/GPU、egress、compaction 和存储成本。

## 16. 决策记录

| 决策 | 采用 | 不采用 | 原因 |
|---|---|---|---|
| 原始格式 | MCAP 为首期标准容器 | 自研 log container | 已有 ROS/工具生态与索引/附件/schema 能力 |
| 写入路径 | payload 直传对象存储，manifest 走控制面 | 视频/点云经 Coordinator/Kafka | 避免 JVM heap、broker 和双倍网络成本 |
| 语义 | 原始多率流 + 版本化 aligned view | 入库即强制统一 FPS 宽表 | 保留真值，可为不同策略重对齐 |
| 事务 | 不可变组件 + 最终 dataset manifest 发布 | 假设跨表湖表 commit 原子 | 多对象/多表没有天然原子边界 |
| 湖表 | Paimon 热写或 Iceberg 版本化，按部署选主 | 双主真值 | 降低一致性与运维复杂度 |
| Lance | 派生训练/检索 artifact | 独立第二 catalog truth | 利用随机访问，同时保留统一治理 |
| 类型 | 先 semantic schema + 稳定物理类型 | 首期新增大量 SQL UDT | 降低 parser/connector/JNI 全链风险 |
| 对齐 | Native typed operators | Python 客户端临时 join | 可重复、可优化、可治理且不搬全量数据 |
| 流处理 | Edge 实时采集 + Pangrova mini-batch 入湖 | 把 Pangrova 变成 Flink | 遵守现有产品与模块边界 |

## 17. 主要风险

| 风险 | 后果 | 缓解 |
|---|---|---|
| 先做 UI/标注，后补时间真值 | 数据不可用于严肃控制学习 | M0/M1 先固化 clock/sequence/schema/manifest |
| 把媒体逐帧行内写 Parquet | 小文件、扫描与随机取样都差 | 原视频 shard + frame index + Blob range |
| 多表 commit 直接暴露 | 训练看到半个 episode | dataset manifest 最后发布，默认视图只读 committed |
| “PTP enabled” 当准确时间 | 错误对齐与标定 | 保存 offset/drift/uncertainty/state，按质量过滤 |
| episode split 在 row/page 层做 | train/val 泄漏 | stable hash episode_id + seed |
| 过早引入新 Tensor/PointCloud 类型 | Java/C++/connector 不一致 | semantic schema first，golden + E2E 后升级 |
| 原始与脱敏数据混用 | 隐私泄漏 | 分区/密钥/策略隔离，派生血缘与短期 URL |
| Connector/Index SPI 被滥作 Blob 服务 | 生产合同不清、fallback 掩盖失败 | 独立 BlobStore capability handshake，缺能力 fail admission |

## 18. 建议的第一批 backlog

按依赖顺序，第一批应只做十项：

1. `DatasetVersionManifest`、`EpisodeManifest`、状态机和幂等/覆盖不变量。
2. `ClockDomain`、四类时间、sync quality 和 `sequence_number`。
3. BlobStore SPI 与 S3-compatible multipart/checksum/range 生产实现。
4. ROS 2 Edge Agent：MCAP、本地 journal、split/snapshot、断网续传、health。
5. MCAP connector + Native indexed reader + corrupt/truncated coverage。
6. dataset/collection/episode/stream/chunk metadata 与默认 committed-only system tables。
7. canonical joint/action/TF/calibration schema registry 和 Native materialization。
8. ASOF + tolerance 的 parser/planner/protocol/JNI/Native 全链。
9. 修复 Training Data Service version/split/determinism，再增加 episode shard。
10. 一条真实 E2E：ROS 2 publisher -> MCAP -> object store -> commit -> SQL ASOF -> Flight ->
   LeRobot/RLDS fixture loader。

这十项完成前，不建议优先投入高级点云 SQL 类型、自动标注 UI、在线 feature store 或复杂 active
learning；它们会建立在不可靠的 episode、时间和版本语义之上。

## Sources

所有外部来源访问于 2026-09-12。优先采用标准组织、项目官方文档、官方仓库和原始论文。

[^1]: MCAP. “[MCAP Format Specification](https://mcap.dev/spec).” MCAP project；另见 “[Concepts](https://mcap.dev/guides/concepts).”
[^2]: Open Robotics. “[rosbag2](https://github.com/ros2/rosbag2).” ROS 2 官方仓库；“[rosbag2_storage_mcap](https://docs.ros.org/en/ros2_packages/kilted/api/rosbag2_storage_mcap/).”
[^3]: Open Robotics. “[Quality of Service settings](https://docs.ros.org/en/humble/Concepts/Intermediate/About-Quality-of-Service-Settings.html).” ROS 2 Documentation.
[^4]: Open Robotics. “[message_filters](https://docs.ros.org/en/ros2_packages/rolling/api/message_filters/message_filters.html).” ROS 2 Documentation.
[^5]: IEEE Standards Association. “[IEEE 1588-2019](https://standards.ieee.org/ieee/1588/6825/).” Precision Clock Synchronization Protocol for Networked Measurement and Control Systems.
[^6]: Open Robotics. “[About tf2](https://docs.ros.org/en/galactic/Concepts/About-Tf2.html).” ROS 2 Documentation.
[^7]: Google Research. “[RLDS Dataset Format](https://github.com/google-research/rlds/blob/main/README.md).” RLDS official repository.
[^8]: Google DeepMind. “[Open X-Embodiment](https://github.com/google-deepmind/open_x_embodiment).” Official repository；Open X-Embodiment Collaboration, “[Robotic Learning Datasets and RT-X Models](https://arxiv.org/abs/2310.08864),” 2023.
[^9]: Hugging Face. “[LeRobotDataset v3.0](https://huggingface.co/docs/lerobot/lerobot-dataset-v3).” LeRobot official documentation.
[^10]: Khazatsky et al. “[DROID: A Large-Scale In-the-Wild Robot Manipulation Dataset](https://droid-dataset.github.io/).” RSS 2024；另见 “[The DROID Dataset](https://droid-dataset.github.io/droid/the-droid-dataset).”
[^11]: Apache Software Foundation. “[Apache Iceberg](https://iceberg.apache.org/).” 特别参见 “[Branching and Tagging](https://iceberg.apache.org/docs/latest/branching/).”
[^12]: Apache Software Foundation. “[Apache Paimon](https://paimon.apache.org/docs/master/).” 特别参见 “[Streaming](https://paimon.apache.org/docs/master/append-table/streaming/).”
[^13]: Lance project. “[Blob API](https://lance.org/guide/blob/).” 另见 “[Lance File Format](https://github.com/lance-format/lance/blob/main/docs/src/format/file/index.md).”
[^14]: Apache Software Foundation. “[Canonical Extension Types](https://arrow.apache.org/docs/format/CanonicalExtensions.html).” Apache Arrow.
[^15]: Apache Software Foundation. “[Arrow Flight RPC](https://arrow.apache.org/docs/format/Flight.html).” Apache Arrow.
[^16]: Amazon Web Services. “[Checking object integrity for data uploads in Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html).”
[^17]: Open Robotics. “[sensor_msgs/PointCloud2](https://docs.ros.org/en/electric/api/sensor_msgs/html/PointCloud2_8h_source.html).” ROS message definition.
[^18]: Open Robotics. “[OpenUSD Conventions for Simulation Asset Interoperability in Open Source Robotics](https://reps.openrobotics.org/rep-0158-2006/).” Draft REP, 2026.
