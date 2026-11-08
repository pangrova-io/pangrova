# 发行验证证据包（2026-11-07）

工作区基线：分支 `refeactor-modules`，HEAD `d17983a3`，未提交变更约 950 个文件。
本文件汇总可复核命令与结果；历史通过结果不自动继承，复跑以命令输出为准。

## 1. 构建与静态门禁

| 项目 | 命令 | 结果 |
|---|---|---|
| 全 reactor 编译 | `mvn -T 1C -DskipTests test-compile` | 通过 |
| 模块依赖策略 | `ModuleDependencyPropertyTest` | 18/18 |
| 包结构 | `PackageStructurePropertyTest` | 3/3 |
| 包无环 | `PackageDependencyAcyclicityPropertyTest` | 1/1（854 包） |
| Manifest/基线 | `.pangrova/scripts/check-module-manifest.sh` | current |
| 文档类引用 | `.pangrova/scripts/check-doc-class-refs.sh` | 318/318 |
| 空白检查 | `git diff --check` | 通过 |
| 干净发行构建 | `mvn clean package -DskipTests` | 通过 |
| 退役类扫描 | 主 JAR 中旧 package 路径 | 全部为 0 |
| 运行时 classpath | standalone/coordinator/worker `runtime-classpath.txt` | 缺失 0 |
| SBOM | `mvn -P sbom -DskipTests verify` | CycloneDX 1.5，540 组件 |

## 2. 单元与组件回归

| 范围 | 命令 | 结果 |
|---|---|---|
| 全量单元（排除 E2E 模块） | `mvn -T 1C test -pl '!pangrova-testing/pangrova-testing-integration-tests'`（仅排除 4 个 MinIO 镜像用例） | **BUILD SUCCESS**：2070 个测试类、零失败；深审后再次全量通过 |
| 恢复服务 | `JournalRecoveryServiceTest` / `JournalRecoveryOwnerScopeTest` | 6/6、8/8 |
| 策略写权限 | `IndexEvaluationAuthorityIntegrationTest` | 10/10 |
| 分发契约 | `StatementDispatcherPropertyTest` 等 | 29/29、5/5、3/3 |
| 会话/缓存/健康值 | `BranchRoutingContextTest`、`CacheWarmupServiceTest`、`InMemoryWorkerRegistry*` | 24/24、28/28、13/13+4/4 |
| 协议 DTO | `ProtocolDtoRoundTripPropertyTest` | 19/19 |
| adapter 模块归位 | WorkerFileClient、Vector 重写、NativeShuffle、Metadata/Federation 句柄、Speculation | 全部通过；架构门禁 22/22 |
| Core 去除具体安全/缓存依赖 | DeferredTenantAuthority / AuthorizationPublicationLock / InvalidationLogPoller 端口 | 9/9、8/8、5/5、2/2；门禁 22/22 |
| 深审修复 | 恢复计数/孤儿清理、缓存跨节点失效接线、GC fence 顺序、治理 emitter 生命周期 | 恢复 15/15、GC 17/17、缓存 5/5、治理 1/1 |
| 静态分析门禁 | `mvn -T 1C -fae verify -Pstatic-analysis -DskipTests` | 全仓 SUCCESS：Checkstyle 0 违规、SpotBugs 全仓通过 |
| 深审后 E2E 冒烟 | ActivationSmoke + DeploymentBootstrap + ConstantQuery + NativeQuery | 30/30（native-runtime + 真实 AI fixture） |

## 3. 真实进程与 E2E（Docker 可用时执行）

| 场景 | 结果 |
|---|---|
| 两节点启动（Coordinator + Worker） | `ready=true`（hasCatalogs/hasWorkers/http/transport） |
| CLI 查询 | `SELECT 1` 返回 1 行 |
| 同版本停机重启 | 重启后 `ready=true`，查询成功 |
| 跨版本回退 | 旧版 `d17983a3` Coordinator 读取新版 schema，liveness 200 |
| ActivationSmokeE2ETest | 3/3 |
| E2eDeploymentBootstrapE2ETest | 4/4 |
| ConstantQuery + NativeQuery E2E | 23/23 |
| Security + QueryStateJournalRecovery E2E | 12/12 |
| QueryOptimizationE2ETest（单独） | 8/8 |
| LocalGovernedFileSqlE2ETest（local backend + capability secret） | 4/4 |
| 本轮核心 E2E 分组（Docker Desktop 4.48 + 真实 AI fixture） | 58 项：56 通过；仅 `WITH RECURSIVE` 2 项按已知产品缺口失败 |
| 其中 WriteDmlTransactionE2ETest（含 InsertSelectCtas） | 16/16 |
| 其中 ActivationSmoke / DeploymentBootstrap / ConstantQuery / NativeQuery | 3/3、4/4、16/16、7/7 |
| DDL / MaterializedView / DynamicTable 分组 | 51/51（含 MaterializedViewMaterialization 5/5） |
| adapter 归位后核心+Vector 分组 | 68 项：66 通过；`VectorSearchChainE2ETest` 10/10，CTAS/DML 16/16；仅已知 `WITH RECURSIVE` 2 项失败 |

未执行：完整 60 类 E2E 分组全量、Linux/Bake 镜像构建。E2E 环境需
`prepare-e2e-ai-image.py` 生成的 Ollama fixture（本轮 imageId
`sha256:0615da8a39325a6388d23623b8d0549c7ccf52bb7f247feaf501deb32ac7543c`，
基于 `ollama/ollama:0.6.2`）及按分组选择 FILE backend；Testcontainers 需指向
Docker Desktop socket（已移除 `~/.testcontainers.properties` 中的 Rancher 覆盖，备份为
`~/.testcontainers.properties.rancher.bak`）。

## 3.1 Helm 部署清单（离线渲染）

| 项目 | 命令 | 结果 |
|---|---|---|
| Chart lint | `helm lint deploy/helm/pangrova` | 0 chart failed |
| 默认渲染（fail-closed） | `helm template pangrova deploy/helm/pangrova` | 按设计拒绝 NOOP 认证 |
| 开发模式渲染 | `helm template ... --set developmentMode=true` | 成功，367 行清单 |
| 生产 OAuth2 渲染 | `helm template ... --values <prod-values>` | 成功，369 行清单 |
| 生产 LDAP 渲染 | `helm template ... --set-string config.coordinator=<LDAP>` | 成功，364 行清单 |
| 多副本无 HA（fail-closed） | `helm template ... --set coordinator.replicas=2` | 按设计拒绝：要求 HA 与共享拓扑 |
| Docker Compose 双文件 | `docker compose -f docker-compose.yml -f docker-compose-cluster.yml config --quiet` | 通过（占位环境变量） |
| Docker Bake 图 | `docker buildx bake -f docker-bake.hcl --print` | 解析成功（standalone/coordinator/worker targets） |

## 4. 已知缺口

1. `WITH RECURSIVE` 递归 CTE 未实现（产品功能缺口，非重构回归）。
2. 全量并行执行中 Marquez/Testcontainers 容器健康检查对负载敏感；按 CI 分组独立执行。
   MinIO 已从 Docker Hub/Quay 撤下匿名可拉取镜像（`pull access denied/unauthorized`），
   S3/MinIO 集成测试在恢复私有镜像源前无法本地复跑；与本次重构无关。
3. 容器运行时已修复：官方 Docker Desktop 4.48.0（build 207573，Developer ID、已公证、
   `com.apple.security.virtualization`、`LSMinimumSystemVersion=13.0`）替代原 ad-hoc
   无 entitlement 的 4.84 副本；Rancher Desktop 清理陈旧 hostagent/socket 后亦可启动。
4. 所有变更未提交。
