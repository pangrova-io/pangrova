# 二进制值与编码批次的数组所有权

本批针对上一轮真实静态分析发现继续实现 P1/P9 的基础契约与资源边界。没有修改包归属、数据库、协议或 JNI 签名，也没有扩大 SpotBugs 豁免。

| 边界 | 实际保证 | 生产调用 |
|---|---|---|
| Types 的 BytesValue | 构造复制输入；value/toRaw 返回调用方自己的副本；空数组合法；SQL NULL 通过 NullValue 表达 | TypedRow、DML、FILE_FROM_BYTES、连接器与结果序列化 |
| ArrayBlock 二进制读取 | 使用 BytesValue 的返回副本；删除多余的第二次 clone | Page/Row 转换与连接器批数据 |
| Batch API 的 NativePage | 拥有编码堆数组快照，读取返回副本；close 幂等并拒绝后续 payload 读取；已经返回的副本仍有效 | BatchSerializer、SpoolBackedExchangeManager、ConnectorBinaryPageCodec |
| NativePage 编码长度 | 不复制 payload 的只读长度值；关闭后仍可用于统计 | TableWarmupExecutor |

NativePage 的 volatile closed 检查允许已经开始的读取与 close 并发完成；它不持有 Native/JNI handle，堆数组由 JVM 回收。没有将这一类包装成 Native 零拷贝 lease，也没有宣称修改了 C++ 所有权。每次 payload 读取会产生一次复制，后续完整性能工作必须衡量其成本。

修复前四个 Types 回归全部失败，证明输入、getter、TypedRow raw 边界与 null 表达存在问题。后续静态检查暴露 NativePage 的 getter 直接返回内部数组；现有属性测试只检查构造输入，补充返回数组修改测试后实际失败。修复后依赖闭包静态分析通过。过程中清理 RowPageConverter 的两个同包导入及删除导入留下的多余空行，失败记录保留。

新增回归包括二进制快照穿过 Page/region 后仍不可被调用方污染，以及真实 Arrow 序列化保持原始二进制内容。已有 Connector、Transport、Shuffle、预热和 Native activation 用例用于检查实际消费路径，不用单独的 getter 测试替代消费链。

本批不将整个 Types 值体系、Native buffer lease、通用 Shuffle、Metadata 原子事务或全仓生产验收标记为完成。Array/Map/Row 的嵌套元素所有权和数组值的结构相等语义仍应按真实调用单独审查，不能从本批标量快照结论推导全部复杂值不可变。

## 消费方装配测试修复

消费方首次验证 613 个用例中有 3 failure/1 error，均来自 AI 默认生产工厂 fixture：它们把 test:// 引用传给只支持 env:// 的真实默认工厂，尚未进入原本要验证的 Local/OSS 路径。保留生产环境密钥约束，把四个装配测试放入显式拥有环境变量的子 JVM；每次生成临时测试密钥，不修改主进程环境、不依赖开发者已有密钥。Local 仍验证真实落盘、长度与 SHA-256，并新增修改原始输入后内容保持不变的消费行为；OSS 仍实例化并关闭真实 SDK，不发对象请求；缺失 Local 根、未知后端和缺失 OSS 配置仍准确拒绝且不创建 staging。子进程有退出超时与强制清理。20 个相关直接测试通过后，再运行完整消费方选择集。

源码和配置可达性扫描确认 CoordinatorResultWriter.serializeFromNativePage 只有定义，没有生产调用、服务注册或反射引用。该方法仅重复 NativePage 的 payload getter，已经连同未使用的导入和虚假的透传路径说明删除，没有转发壳；真实 Arrow 回归通过 serializeTypedRows 路径；结果写入器原有“zero-copy/zero-overhead”的说明也改为准确的列向量到 IPC 序列化，不宣称没有复制。删除后重新执行结果序列化、NativePage、模块边界和 Native activation，并 clean 打包。
