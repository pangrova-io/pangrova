# 开源公开前仓库检查

检查日期：2026-10-24。目标地址：`https://github.com/pangrova/pangrova`（维护者已确认）。

本次是目录、文档、脚本、配置和发布入口的结构检查与清理，不是全量引擎正确性、
安全审计或依赖许可认证。开始时已有大量物化相关功能改动；保留这些改动，未重置、
提交、推送或修改仓库公开状态。

## 结论

项目具备基本开源社区文件、模块边界、测试基础和发布工作流，但此前存在新用户构建步骤
缺失、官网构建失败、仓库身份混用和重复脚本入口。本次已修复这些可在本地核实的问题。
不能据此宣告正式二进制发布就绪；托管端设置、制品完整性和现有测试失败仍需关闭；联系邮箱已由维护者指定。

## 逐项检查

| 项目 | 检查结果与处置 | 当前结论 |
|---|---|---|
| 根目录 | 按 Maven 模块、原生模块、开发策略、运行配置、部署、文档、网站分类；没有按文件名盲删代码 | 保留结构，目录说明移到贡献文档 |
| README | 原先超过 400 行，含大段能力列表、目录、路线图；现约 100 行，保留定位、成熟度、架构、快速开始与社区入口 | 已精简 |
| 架构图 | 原 PNG 把现有、部分实现和未来能力放在同一图中；改为当前 Coordinator/Worker/JNI/Velox/Flight 路径，标出直接结果返回 | Mermaid 语法检查通过；旧 PNG/HTML 已删除 |
| Roadmap | 从 README 移出；开发目标不再与当前能力混写 | 独立 `docs/getting-started/roadmap.md` |
| 仓库身份 | 远端为 `pangrova/pangrova`，大量入口仍指向 `pangrova-project` | 仓库、网站、徽章、博客、社区链接及源码 issue 引用已统一 |
| 社区文件 | LICENSE、NOTICE、CONTRIBUTING、SECURITY、行为准则、问题/PR 模板均存在 | 保留；维护者已指定安全与行为准则统一联系邮箱 |
| 贡献指南 | Maven 3.8、旧 500/50 行阈值和不存在的规则文件引用与现行政策冲突 | 改为 Maven 3.9+，链接单一规则来源 |
| 源码构建入口 | 缺少定制 Iceberg SDK 构建；开发指南使用不存在的启动脚本；快速开始声称全量构建 3–5 分钟 | 补 JDK 17 / Gradle 8.14.4 / Python / SDK 前置步骤，使用现有脚本，删除无证据时间和容量承诺 |
| 文档重复 | 开发指南尾部再次复制过时构建步骤 | 合并到正文快速开始，保留独有 E2E 前置条件 |
| 配置目录 | 源码 catalog 指引使用旧 `config/catalog/`；启动脚本默认消费 `etc/catalog/` | 修正文档；Docker/Helm 的 `/opt/pangrova/config/` 是实际挂载路径，保留 |
| 官网 | 两个公开 Markdown 链接指向被排除的 `docs/internal/`，构建失败 | 改为仓库来源链接；仍保持断链失败策略 |
| 网站工具 | README 仍为 Yarn/本地 gh-pages 模板，实际为 pnpm 与 Actions | 统一 Node 22 / pnpm 10 / frozen lockfile / Actions |
| 网站 CI | 原来只在 main 推送构建，PR 缺少网站验证 | 新增 PR 构建和类型检查；仅 main 非 PR 可以上传 Pages 制品与部署 |
| 工作流权限 | 网站工作流顶层赋予 Pages/OIDC 写权限 | 收敛到部署 job；PR 构建仅 contents:read |
| CODEOWNERS | 旧 `pangrova-substrait` 和错误 connector 根路径 | 改为 protocol 与 connector 聚合目录；迁移组织名，团队实际存在和权限待确认 |
| 依赖维护 | Dependabot 仅 Maven/Actions | 补网站 npm 与主 Lance Cargo；SDK 测试锁与原生固定依赖仍需对应维护流程 |
| Git 忽略 | 未覆盖 `.env.production`、运行 PID/日志目录、发布暂存目录、部分本地 agent 配置 | 补规则，保留 `.env.example` 可提交 |
| Docker 上下文 | 未排除 node_modules、Docusaurus、Python 缓存等 | 补排除，保留模板与构建输入 |
| 图片 | README 与网站各存一份完全相同 logo/架构 PNG | 合并 logo；用 Mermaid 替代旧目标架构资源 |
| 占位文件 | `pangrova-config/src/main/resources/.gitkeep` 已有真实配置文件 | 删除；`.mvn/.gitkeep` 有 Maven 根定位作用，保留 |
| 空文件 | `website/static/.nojekyll` 是 Pages 标记 | 保留，不按空文件规则删除 |
| 生成物与基线 | 协议 descriptor、能力二进制夹具、TPC 浮点数据、Cargo/pnpm 锁文件有消费者 | 保留，不能当垃圾清理 |
| 本地缓存 | IDE、target、native build、依赖下载属于本地状态 | 已跟踪文件中无命中忽略规则的文件；未执行破坏性全盘 clean |
| 脚本 | 逐项分类全部 127 个已跟踪 `.sh`/`.py`，含测试和导入模块 | 4 个冗余入口删除，123 个保留，详见清单 |
| CMake 辅助模块 | Arrow/Lance/Velox 发现、Abseil/RE2/Protobuf、函数目录/来源和系统依赖模块被主构建或 FindVelox 引用；ASan Folly 适配用于专用构建 | 按依赖边界保留，未合并成大脚本 |
| 原生补丁 | Velox、Iceberg、Lance 等补丁和来源校验承担具体兼容/正确性契约 | 保留，未根据体积或“patch”后缀删除 |
| 敏感信息 | 当前树及本地可达历史做有限特征扫描，见下文 | 没有完整秘密扫描器，不能承诺无泄漏 |
| 授权与发行 | 根 LICENSE/NOTICE 存在；未完成全部依赖及链接后二进制的许可/通知清单 | 正式分发前待办，未擅自改许可 |

## 脚本必要性与合并

[完整脚本清单](../contributing/script-inventory.md) 逐项记录文件、作用、消费者/发现方式和结论。
[目录与入口指南](../contributing/repository-guide.md) 记录可执行替代命令。

已删除：

- `generate-tpc-data.sh`：无仓库调用方，只转发到 `generate_tpc_data.py`。
- `start-benchmark-cluster.sh`：无仓库调用方，只转发到 `tpc_cluster.py start`。
- `stop-benchmark-cluster.sh`：无仓库调用方，只转发到 `tpc_cluster.py stop`。
- `generate-monthly-report.sh`：无仓库调用方，重复设置比较器环境并按文件名字典序挑选结果；
  保留现有 `tpch-regression-check.sh` 的历史报告能力，要求显式选定结果与基线。

三个角色启动脚本有明显重复，是后续合并候选：保持公开脚本名，把公共逻辑抽到内部启动器，
验证环境覆盖顺序、角色 JVM 默认值、参数、classpath、daemon、信号和 PID 所有权后再合并。
本次没有改动进程启动行为。不能把“有重复”直接等同于“可安全删除”。

未删除没有字面调用方的测试：有的由 unittest/CTest 发现，有的是冷缓存或 SpotBugs 手动诊断。
另发现 function-contract Python 测试没有默认 CI discovery，且手动运行存在基线失败；
在修正预期与真实契约的一致性后，再决定接入门禁，不能通过删除测试清理问题。

原生 release/ASan 工具、交互健康检查/容器探针/恢复计时，以及 JMH/普通 TPC suite/严格 P99
实验的合同不同，不应为了减少文件数量强行合并。

## 工作流清单

| 文件 | 必要性与外部前置条件 |
|---|---|
| `ci.yml` | 保留。负责按改动选择日常和全链路验证，最终 `Validation result` 为聚合结果；分支保护需要托管端配置 |
| `deploy-website.yml` | 保留。新增 PR 验证与最小部署权限；需要 GitHub Pages 设为 Actions 来源 |
| `release.yml` | 保留。tag 触发并依赖 correctness validation；当前发布文件列表主要为 native JAR/裸库，不能当成完整服务发行包 |
| `nightly-tpc-benchmark.yml` | 保留专用实验所有权。需要 self-hosted benchmark runner、真实数据/参考、配置变量和令牌；不是 fork 即可跑通的通用任务 |
| `nightly-operator-fuzzer.yml` | 文件名沿用历史名称，实际仅手动触发 DuckDB reference self-test；本次修正本地脚本注释，不冒充 Pangrova 差分验证 |
| `actions/setup-iceberg-sdk/action.yml` | 保留。复用固定 Gradle/JDK 与 SDK 构建，避免各 workflow 重复依赖构建逻辑 |

## 验证记录

| 检查 | 结果 |
|---|---|
| Shell/Python 语法 | 初始 49/78、清理后 45/78 全部通过 |
| YAML/XML/JSON 结构 | 27 个 workflow/运行/部署 YAML、42 个 POM/静态分析 XML、2 个 package/preset JSON 通过；Helm 模板需 helm 渲染，未混作普通 YAML |
| `.pangrova/scripts/tests` | 37 项通过 |
| benchmark `scripts/tests` | 113 项通过；使用临时 Python 3.12 环境及仓库锁定的 pyarrow 19.0.1 / sqlparse 0.5.3 |
| Python 环境排障 | 系统 Python 3.9 首次运行缺少上述依赖；配置独立临时环境后通过，未修改系统 Python 或依赖锁 |
| 替代脚本 CLI | 数据生成、cluster start/stop 的 `--help` 通过 |
| 文档类引用 | 179 个通过 |
| 指令布局 | `.pangrova/setup.sh --check` 通过 |
| 个人路径检查 | 自测与全仓扫描通过 |
| README Mermaid | 使用网站 Mermaid 解析器，flowchart-v2 解析通过 |
| 网站 | 最终类型检查与生产构建通过；新增文件未提交导致的更新时间 warning 保留，没有关闭断链检查 |
| function-contract Python 测试 | 93 项中 2 项失败：`test_binding_declarations` / `test_binding_manifest` 预期 107，实际 111；已确认 HEAD 的数据也是 111 且该范围没有本次 diff，保留失败 |
| Java/native/容器全回归 | 本次未运行；没有修改 runtime 执行、JNI 或协议；不能替代当前功能改动合入前的完整 CI |

## 有限历史敏感信息检查

扫描本地可达的 253 个 commit、13,987 个唯一 blob，使用 GitHub token、AWS access key、
OpenAI key 和 PEM private-key 标记的有限匹配。未输出秘密原文，未修改或重写历史。

- GitHub/OpenAI token 特征无命中。
- 4 个 AWS 签名命中均核实为同一示例值，出现在历史配置模板/部署文档。
- PEM 标记出现在解析器字符串、测试 TLS 工具和 localhost 测试夹具；这些夹具有测试用途，保留。
- 本环境无 gitleaks/trufflehog；没有全面 entropy/所有厂商令牌/凭证有效性检查，也未读取远端隐藏引用。
  正式公开前应对将要发布的全部历史再做专用秘密扫描；发现实际秘密时，删除当前文件不等于清除历史。

## 公开和正式发行前仍需处理

1. **联系渠道（已处理）**：维护者于 2026-10-24 指定 `sunxiaojian926@gmail.com`，
   SECURITY 与行为准则均已更新为该邮箱及对应 mailto 链接；未发送测试邮件。
2. **GitHub 设置**：确认 `@pangrova/maintainers/core/native/connectors/docs` 对应团队存在且有权；
   配置 branch protection、Pages、漏洞私密报告、依赖告警及实际使用的 labels/Discussions。
   这些不是本地 YAML 能证明的状态，本次未改变远端权限或公开设置。部分 `@Experimental` 的
   issue URL 仍是非数字占位编号，需要对应真实问题记录，统一组织地址不代表链接目标已存在。
3. **现有测试失败**：调查 107→111 的业务契约变化并补齐断言；不要仅改数字掩盖缺失验证。
4. **干净环境复现**：用空 Maven/native 缓存验证修正后的安装文档；本次只核对脚本契约，未重新编译全部依赖。
5. **二进制发行**：目前 POM 为 SNAPSHOT，缺少面向 Maven 发布的项目 URL/license/SCM 元数据；
   release 文件列表与部署文档中的 tar.gz 发行包预期不一致。正式 release 前需确定版本、Java/native
   配套发行格式、校验和/签名、SBOM 和第三方通知，并验证镜像/JAR 中携带的许可材料。
6. **原生及依赖发布审查**：按实际打包结果核对 statically linked/transitive 依赖与来源通知；
   当前 NOTICE 列表不能代替最终分发清单。此项没有通过本次语法检查获得认证。
7. **文档政策漂移**：`code-standards.md` 引用的 `.pangrova/modules.yaml` 与部分目标模块在当前树不存在，
   其约束与目标模块化设计需要维护者区分实施现状和未来政策。本次未擅自修改架构约束。
8. **工作区功能改动**：现有未提交/新增物化模块需要按原功能任务完成验证和提交；开源整理没有包含它们的正确性结论。

## 清理边界

删除项都有明确的替代入口、重复字节证据或已被新图取代的用途。未删除内部研究与验证依据、
协议基线、锁文件、测试证书、原生补丁、机器上的构建缓存、IDE 设置或用户的功能改动。
