# Worker File authority 依赖清理

基线为当前工作区 8,601 个文本文件。检查源码、测试、资源和装配后，确认
org.pangrova.worker.file.FileSpaceObjectStore 仅由旧测试调用，没有生产构造、服务注册
或反射引用。它重复包装 profile 查找、路径布局与 accessor 选择，令 Worker 不必要地
依赖 File Persistence API。

删除该类及旧包装器测试，不添加转发类。Worker POM 移除 File Persistence API；manifest
将其列为禁止依赖，架构测试递归检查 Worker 生产依赖闭包。File API/Filesystem API 的真实
控制和字节访问契约仍保留。File Core、FileSpaceLayout、Worker Factory、FileSpaceFileResource
和 WorkerHttpBootstrap 五个实际 owner 的源码字节未变：当前 profile/revision 在控制面
解析和签发，Worker 先验签再访问 Provider，endpoint 继续持有并关闭 factory。

旧五项测试逐项保留语义：布局三项进入 File API，实际签发增加显式/default profile 与
外部路径越界测试，Provider 生命周期测试增加真实 LOCAL/S3 类型选择并正确关闭资源。
增加了 sibling prefix 和 rename 保持 managed root 的检查。没有用删除测试掩盖缺陷。
历史旧测试只作为基线证据，不计入当前通过数。

基线 24 PASS；移除前新增架构门禁 1 FAIL，明确报出 Worker→File Persistence API 路径。
删除与依赖清理后近端 27 PASS。全 reactor clean package 选择范围 127 PASS，0 FAIL/ERROR/SKIP，
覆盖 Worker HTTP 启动、文件传输、S3 multipart、读取消/超时、File SQL 和 Native activation。
8 项静态检查通过，826 个源码包零循环。实际 106 JAR、106 唯一模块名、832 编译包审核零违规，
Worker JAR 无旧 class 或 File Persistence 符号；Worker classpath 从 187 降为 186 项，
File Persistence API/PostgreSQL 均不存在。Coordinator/Standalone 仍各有一个当前 API/Provider，
三个角色均无 Testing Kit。Native 三处 SHA 一致，相关输入未改变。

同步修正 File 架构文档中此前残留的通用 Persistence 制品和旧包路径。没有重跑完整生产
回归；已知 Shuffle 未接线问题仍开放。Worker 其他 Provider 选择、Server Worker 抽取、
Generation/Persistence、workflow 和通用 Shuffle 等仍需按完整计划推进。此处只验收 File
控制面依赖清理，不宣称所有运行角色已完全隔离、性能或全平台已验证。

证据：[FQN 处置](worker-file-boundary-fqn-dispositions.tsv)、
[原调用](worker-file-boundary-callers-before.tsv)、[清理后调用](worker-file-boundary-callers-after.tsv)、
[实际 owner 未变](worker-file-boundary-runtime-owner-proof.json)、
[测试覆盖映射](worker-file-boundary-test-coverage-map.tsv)、
[验证](worker-file-boundary-validation-summary.json)、[逐测试](worker-file-boundary-tests.tsv)、
[静态检查](worker-file-boundary-static-evidence.json)、[包图](worker-file-boundary-package-graph.txt)、
[JAR](worker-file-boundary-artifacts.json)、[角色 classpath](worker-file-boundary-runtime-classpath.json)、
[输入](worker-file-boundary-input-evidence.json)、[文件处置](worker-file-boundary-file-dispositions.tsv)。
