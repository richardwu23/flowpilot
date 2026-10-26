---
title: FlowPilot 本地搭建与分阶段实施步骤
status: planned
created: 2026-10-26
updated: 2026-10-26
---

# FlowPilot 本地搭建与分阶段实施步骤

本文记录从初始仓库到 v0.1 的操作顺序。**第一轮完成阶段 0：启动基础环境，并证明一个真实 Flink 作业能够被提交、识别、查询和取消。** 完成后再进入支付数据链路及 API／Worker 开发。

本文是待执行指南。保存本文件不代表已创建 Compose、构建 JAR、安装依赖或完成任何实验。命令标明了执行前提；需要实现的文件尚不能作为可用入口调用。

## 1. 起点与阶段顺序

2026-10-26 只读检查时，仓库包含 README、Python 模板 `.gitignore` 和 Apache 2.0 LICENSE，工作区干净。本机工具检查记录如下；其他机器应重新核查。

| 项目 | 检查结果 |
| --- | --- |
| 本地仓库 | `/Users/richardwu/coding/flowpilot` |
| 远端 | `https://github.com/richardwu23/flowpilot.git` |
| 架构 | Apple Silicon／ARM64 |
| Java | 已安装 Java 17，Maven 当前使用 17.0.12 |
| Maven | 3.9.9 |
| uv | 0.8.8；系统 Python 为 3.9.6，后端计划使用 uv 管理的 Python 3.12 |
| Docker CLI | 29.4.1 |
| Compose | `docker-compose` 5.1.3 可用；`docker compose` 当前不能调用 |
| Colima | 默认实例停止；配置为 4 CPU、8 GiB 内存、60 GiB 磁盘 |

本机 `~/.docker/cli-plugins/docker-compose` 指向已不存在的 Docker Desktop 路径，Homebrew 的独立 `docker-compose` 命令可用。因此本文统一使用该命令，暂不要求修改全局插件配置。工具状态是检查时的快照，不能作为引擎、连接器或数据链路已通过验证的证据。

```text
0: 基础环境 + 提交身份探针
  -> 1A: 正常支付数据链路 + 独立校验
  -> 1B: 重复 / 乱序 / 迟到边界
  -> 1C: Checkpoint + TaskManager 故障恢复
  -> 2A: API / Worker 基本管理
  -> 2B: 可靠操作 + 人工核查
  -> 2C: 全新实验环境复现 -> v0.1
```

v0.1 交付可靠的启动、查询、取消、幂等、对账和控制进程重启接管。停止语义为取消，不承诺保留计算状态。Savepoint 同版本恢复放在 v0.2，受限兼容升级放在 v0.3；三个展示页面和扩展组件不阻塞首发。

## 2. 建立开发分支与目录

**前提：** 仓库已 clone，当前改动已经核对。以下命令供手动执行，保存文档本身不执行这些操作。

```bash
cd /Users/richardwu/coding/flowpilot
git status --short
git switch -c codex/bootstrap-local

mkdir -p \
  deploy/local/postgres \
  jobs/streaming/src/main/java/dev/flowpilot \
  tools/probe \
  docs \
  experiments \
  tests/fixtures \
  .local
```

如果分支已经存在，切换到已有分支，不重复创建。第一轮需要准备以下文件，其中本指南已经创建；其余文件待实现。

```text
flowpilot/
  .env.example
  deploy/local/
    compose.yaml
    postgres/init.sql
  jobs/streaming/
    pom.xml
    src/main/java/dev/flowpilot/IdentityProbeJob.java
  tools/probe/
    flink_submission.py
  docs/
    README.md
    local-development.md
    tested-versions.md
  experiments/
    phase-0-submission-identity.md
```

在现有 `.gitignore` 中补充：

```gitignore
.DS_Store
.env.*
!.env.example
/.local/
*.class
```

现有模板已经忽略 `.env`、`.venv` 和 `target/`，不用重复添加。后续 `uv.lock`、`pom.xml`、迁移脚本和测试样本应提交到仓库。README 和 LICENSE 已存在，不重新初始化或覆盖。

## 3. 启动容器运行环境

**前提：** 已安装 Docker CLI、Colima 和可用的 Compose。检查时本机已有这些工具，可先沿用默认 Colima 实例的配置。

```bash
colima start --runtime docker --cpus 4 --memory 8

docker --context colima version
docker --context colima ps
docker-compose version
```

**通过条件：** Docker 返回服务端信息，容器列表查询成功。后续命令通过 `DOCKER_CONTEXT=colima` 指定运行环境，避免依赖其他终端中的默认 context。[Colima 使用说明](https://github.com/abiosoft/colima#usage)

如果希望以后恢复 `docker compose` 的调用方式，可根据 Homebrew 说明单独处理插件搜索目录；这不是当前阶段的前置条件。[Homebrew Compose 说明](https://formulae.brew.sh/formula/docker-compose)

## 4. 固定组件组合并编写 Compose

第一轮只创建 PostgreSQL、Kafka、Flink JobManager 和 Flink TaskManager 四个常驻服务。API 与 Worker 到阶段 2A 再加入同一份 Compose，最终扩为六个容器。

### 4.1 版本候选与验证记录

可以使用 **Flink 2.2.1 + Java 17** 作为首轮候选。官方提供 `flink:2.2.1-java17` 的 ARM64 镜像；连接器发布列表列出 Kafka Connector 5.0.0、JDBC Connector 4.1.0 对 Flink 2.2.x 的支持。具体 Maven 构件版本按对应发布文档核对，不能把连接器发布编号直接当作已确认的完整 Maven 版本。

这些信息来自官方资料，不表示该组合已在本机通过构建和运行测试。[官方镜像清单](https://raw.githubusercontent.com/docker-library/official-images/master/library/flink)、[连接器发布与兼容列表](https://flink.apache.org/downloads/)、[Flink 2.2 Java 兼容说明](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/deployment/java_compatibility/)

创建版本记录文件，保存 Python、JDK、Maven、Flink、Kafka Connector、JDBC Connector、PostgreSQL 驱动、Kafka Broker 和 PostgreSQL 的确切版本。镜像记录完整 tag 及拉取后的摘要，避免用 `latest` 或浮动版本作为复现依据。未验证项先标记为“候选”；完成对应实验后再补充已测配置、日期和证据。

### 4.2 地址与持久化约定

编写 `deploy/local/compose.yaml`，采用以下地址约定：

| 服务 | 容器内地址 | Mac 上的地址 |
| --- | --- | --- |
| PostgreSQL | `postgres:5432` | `127.0.0.1:55432` |
| Kafka | `kafka:9092` | `127.0.0.1:19092` |
| Flink REST／UI | `flink-jobmanager:8081` | `127.0.0.1:18081` |
| 后续 API | `flowpilot-api:8000` | `127.0.0.1:18000` |

配置要求：

- Kafka 使用单节点 KRaft，分别配置容器内与宿主机 listener。初始 Topic 使用一个分区，并使内部 Topic 的副本配置适用于单节点。
- PostgreSQL 和 Kafka 使用项目专属持久卷。所有网络、卷、Topic 和 Consumer Group 均采用项目专属名称。
- JobManager 与 TaskManager 共享状态卷，挂载路径一致，确认 Flink 用户可写。状态文件持久化不能替代 JobManager HA 或自动恢复配置。
- Flink 初始并行度为 1，TaskManager 配置足够的 slot。
- 宿主端口绑定 `127.0.0.1`，容器之间使用服务名通信。
- `.env.example` 保存配置项与本地示例值，实际 `.env` 不提交。
- Compose 中相对 bind mount 的路径以 Compose 文件所在目录为基准，不能误当作始终相对于仓库根目录。
- PostgreSQL 初始化脚本只负责首次初始化；修改初始化 SQL 不会自动升级已有数据库。后续变化通过显式 SQL 或迁移执行。

初期 PostgreSQL 可先完成数据库连接验证；平台元数据及业务结果的 Schema、角色和权限在对应阶段补齐。共享 PostgreSQL 实例仍存在资源竞争。

## 5. 启动基础服务并检查就绪状态

**前提：** Compose 与配置样例已经实现，固定版本已选定，Colima 已启动。以下命令从仓库根目录执行。

首次准备配置：

```bash
cp .env.example .env
```

填写 `.env`。如果已有本地配置，保留原文件并核对配置项，不重复覆盖。

在当前终端定义快捷函数：

```bash
fpcompose() {
  DOCKER_CONTEXT=colima docker-compose \
    --project-name flowpilot \
    --env-file .env \
    --file deploy/local/compose.yaml \
    "$@"
}
```

该函数只在当前 shell 有效。后续可以将同一入口整理成脚本或 Makefile；在入口实际实现前不把它写成已有命令。

```bash
fpcompose config --quiet
fpcompose pull
fpcompose up -d
fpcompose ps
```

分别检查服务。PostgreSQL 示例假设数据库和初始化用户都命名为 `flowpilot`；如配置不同，替换为实际值。

```bash
fpcompose exec -T postgres \
  pg_isready -U flowpilot -d flowpilot

fpcompose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list

curl --fail http://127.0.0.1:18081/overview
curl --fail http://127.0.0.1:18081/taskmanagers
```

**通过条件：** 数据库可连接，Kafka 管理请求成功，Flink 能看到一个已注册的 TaskManager。`compose ps` 或 HTTP 200 只证明局部状态，不证明数据链路正确。

失败时先查看对应日志：

```bash
fpcompose logs --tail=100 postgres
fpcompose logs --tail=100 kafka
fpcompose logs --tail=100 flink-jobmanager flink-taskmanager
```

平时停止本项目服务：

```bash
fpcompose stop
```

再次使用时运行 `fpcompose up -d`，然后重新检查就绪状态。默认保留数据卷；删除项目实验数据使用单独步骤并说明影响范围。

## 6. 实现最小 JAR 与提交身份探针

### 6.1 构建持续运行的最小作业

实现 `IdentityProbeJob`，持续读取测试 Kafka Topic，并输出少量记录。阶段 0 暂不加入支付窗口、业务去重或 JDBC Sink，避免把提交管理与业务逻辑问题混在一起。

在 Maven 配置中明确：

- Java 编译目标与容器运行时一致。
- Flink 核心依赖使用 `provided`。
- 必要的连接器及运行依赖正确打入作业 JAR。
- 设置入口类 `dev.flowpilot.IdentityProbeJob`，并将产物名固定为 `flowpilot-streaming.jar`。

先创建探针使用的单分区 Topic，再构建 JAR：

```bash
fpcompose exec -T kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create --if-not-exists \
  --topic flowpilot.probe.v1 \
  --partitions 1 \
  --replication-factor 1

mvn -f jobs/streaming/pom.xml clean package

shasum -a 256 jobs/streaming/target/flowpilot-streaming.jar
```

作业参数至少能指定 Kafka 地址、Topic 和 Consumer Group。容器内运行的 Flink 使用 `kafka:9092`。保留实际参数和制品摘要。

### 6.2 正常提交、查询与取消

实现提交探针，按以下顺序执行：

| 顺序 | 动作 | 保存或验证的内容 |
| --- | --- | --- |
| 1 | `POST /jars/upload` | 本地 JAR 摘要、返回的 `jarId` |
| 2 | 生成新的 32 位十六进制 Job ID | 提交之前保存 Job ID、入口类和参数到本地实验记录 |
| 3 | `POST /jars/{jarId}/run` | 请求携带预存的 `jobId` |
| 4 | `GET /jobs/{jobId}` | 实际身份与预存 ID 一致，最终进入 `RUNNING` |
| 5 | `PATCH /jobs/{jobId}?mode=cancel` | 请求取消原 Job |
| 6 | 持续查询原 Job | 确认进入 `CANCELED`，不能只看取消请求被接受 |

本地 JAR 摘要、上传后的 `jarId`、运行时 Job ID 是三种身份。每次明确的新启动使用新的 Job ID；同一次未决提交核查保留原 ID。[Flink 2.2 REST API](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/rest_api/)

### 6.3 提交响应丢失

增加故障模式：Flink 已接受提交，探针在把成功结果交给调用方前丢弃返回结果。使用预存 Job ID 独立查询，确认找到原作业，且没有创建第二个 Job。短随机超时不能替代确定发生在提交生效之后的故障。

本阶段模拟调用方无法获知提交结果；完整 Worker 的重启与响应丢失实验在阶段 2B 再做，不能用探针实验替代。

实验记录保存请求、JAR 摘要、`jarId`、预存／实际 Job ID、故障位置、查询结果、取消终态和结论。

**第一轮停点：** 四个基础服务可用，JAR 可构建，原作业可追踪，响应丢失后仍能找到它，取消可确认。通过后再进入下一阶段。

## 7. 实现支付数据链路

新增事件契约、固定输入、支付作业、结果表和独立校验器：

```text
固定支付输入
    -> Kafka
    -> Flink 商家分钟金额聚合
    -> PostgreSQL results
    -> 独立校验报告
```

按以下顺序实施：

| 交付点 | 实现内容 | 验收 |
| --- | --- | --- |
| 1A | 正常输入、窗口聚合、结果表、独立校验器 | 结果行和金额符合预期 |
| 1B | 重复、乱序、迟到样本 | 每种边界符合固定的业务规则 |
| 1C | Checkpoint、TaskManager 故障脚本 | 恢复后完整输入范围的结果仍正确 |

固定输入包含 `event_id`、`order_id`、`merchant_id`、`amount_cents`、`event_time`；金额使用整数分。明确去重保留范围、窗口边界、Watermark 与迟到策略。样本必须推进 Watermark，不能认为发送完数据就意味着窗口关闭。

结果表使用稳定业务键写入完整聚合值，并定义迟到更新及旧值覆盖行为。普通 JDBC Sink 的写入语义与整条链路的结果正确性分别验证；不直接宣称端到端 exactly-once。

校验器独立计算期望值并查询实际结果，报告缺失行、重复键和金额差异。独立实验使用明确的输入范围及结果命名空间，不清空共用结果库来掩盖差异。取消后的新启动明确记录输入起点与结果范围，不能当作状态恢复。

TaskManager 故障实验在确认成功 Checkpoint 后进行，故障前后持续产生已知范围的事件：

```bash
fpcompose kill -s SIGKILL flink-taskmanager
fpcompose up -d flink-taskmanager
```

等待引擎恢复及积压处理完成后运行结果校验。此时可以用脚本管理作业；平台不重复提交的行为到阶段 2B／2C 再验证。

## 8. 建立 API 与 Worker

**前提：** 阶段 1 的链路和结果验证通过。到阶段 2A 再初始化 Python 后端：

```bash
uv python install 3.12

uv init \
  --app \
  --package \
  --name flowpilot \
  --python 3.12 \
  backend

uv add --project backend \
  fastapi \
  "uvicorn[standard]" \
  pydantic-settings \
  "sqlalchemy>=2,<3" \
  "psycopg[binary]" \
  alembic \
  httpx

uv add --project backend --dev pytest ruff
```

如果后端项目已存在，检查并增量维护，不重复初始化。提交 `backend/uv.lock`，保存实际解析的依赖版本。

逐步建立以下模块：

```text
backend/src/flowpilot/
  api/
  tasks/
  deployments/
  operations/
  worker/
  adapters/
  persistence/
```

开发顺序：数据模型与迁移 → Flink REST adapter → 创建任务和版本的 API → 保存 Operation 的 API → Worker 正常执行 → 状态查询与取消 → 异常路径。

先实现 `Task`、不可变 `TaskVersion`、`Deployment`、`Operation`。使用 Alembic 维护元数据结构，从首版表结构保留幂等键、部署互斥、目标修订号与外部身份。恢复点对象到 v0.2 再增加。

API 在一个事务中保存目标与操作，返回 `202`；Worker 在短事务中领取，再到事务外调用 Flink。查询失败保留最后有效观测，并显示未知或过期。

入口文件实现后，可以在两个终端分别调试：

```bash
uv run --project backend uvicorn \
  flowpilot.api.main:app \
  --host 127.0.0.1 \
  --port 18000 \
  --reload
```

```bash
uv run --project backend python -m flowpilot.worker.main
```

本机进程使用宿主地址访问依赖。随后为 API／Worker 提供镜像和不同启动入口，加入同一份 Compose，切换为容器内服务地址；阶段 2C 按六容器方式验证复现。阶段 2A 只完成正常管理，不称为已完成 v0.1。

## 9. 补齐可靠操作与人工处理

阶段 2B 按这个顺序推进：请求幂等 → 同部署互斥 → 旧修订写回保护 → 观测过期 → 提交响应丢失 → Worker 中途退出 → 待人工处理 → 人工重新核查与解除阻塞。

需要同时落实：

- 同键同内容返回原 Operation，同键不同内容拒绝。同一部署已有非终态操作时拒绝新的冲突变更。
- 原 Job 仍存活或尚未确认终止时，新幂等键也不能绕过重复启动保护。
- 只有显式 Operation 触发外部变更。对账核实并续接已有操作，不因目标仍为 `RUNNING` 自动创建启动操作。
- 外部动作生效后、数据库写回前终止 Worker；确认旧进程退出，新进程先查询原 Job，再续接步骤。
- 核查预算耗尽进入 `NEEDS_ATTENTION` 并阻塞新变更。重启不重置已消耗预算或丢失处理历史。
- 人工请求携带原操作标识、预期修订号、幂等键和原因。找到匹配原 Job 后续接；确认失败并排除晚到请求后关闭；证据不足时继续阻塞。
- 关闭操作与释放阻塞在有条件校验的事务中执行，保留处理人、时间、原因、证据和前后状态。超时、单次未找到或用户放弃都不直接构成解锁证据。

先提供 API 文档页或 CLI 的核查入口。只演示卡在待处理状态不算完成；至少走通原作业认领与确定失败关闭两种结果。

## 10. 复现与 v0.1 验收

阶段 2C 从新的项目实验环境，按公开文档重新执行构建、迁移、启动、正常流程、自动接管及人工处理。以下均为待执行标准，不代表已经通过。

| 编号 | 必过场景 | 最低证据 |
| --- | --- | --- |
| A01 | 正常创建、启动、查询、取消 | 版本、操作、原 Job ID 与业务结果可关联；取消有终态证据 |
| A02 | 幂等与重复启动 | 同键重放无新操作或 Job；不同内容及不安全的新启动被拒绝 |
| A03 | 并发变更与旧结果写回 | 只接受一个冲突变更；旧 owner／revision 不覆盖新记录 |
| A04 | 引擎接受提交后响应丢失 | 原 Job 被查到并接管，没有额外部署 |
| A05 | 外部调用后、写回前 Worker 退出 | 新 Worker 核查原 Job 后续接，不重置为首次提交 |
| A06 | API 重启 | 原计算继续；恢复后可查到原记录和实际状态 |
| A07 | Flink 暂时不可达或查询未找到 | 观测标记未知／过期，不伪造终态或刷新成功观测时间 |
| A08 | 确认失败但目标仍为 `RUNNING` | 多轮对账不自动创建重启；历史成功操作不因后来故障改写 |
| A09 | 人工核查与解除阻塞 | 认领、确定失败关闭、证据不足保留阻塞均可验证；重启保留历史 |
| A10 | 数据正确性 | 正常、重复、迟到及故障后的独立结果报告 |
| A11 | TaskManager 恢复 | Checkpoint、故障位置、引擎恢复、无平台替代提交及结果对账 |
| A12 | 全新实验环境复现 | 固定版本、完整步骤、无隐含本机依赖、默认停止保留数据 |

每次实验记录代码与组件版本、输入范围、前置配置、故障动作、预期、实际观察、时间线、恢复耗时和结论。失败或未做的必过项继续标记未完成；不预填通过率或性能数字。

首次交付包含：可复现的构建／启动／停止步骤、固定依赖与已测版本、确定性输入和独立校验、正常流程／自动接管／人工处理记录、API 使用说明及已知限制。校验入口失败时返回非零退出码。

默认停止保留元数据、Kafka 数据和状态文件。清理实验数据使用单独明确的步骤，不要求清空其他项目的数据。JobManager 或整个 Compose 环境重建后的自动恢复不属于 v0.1 的承诺；保留文件不等于自动恢复原作业。

API 文档页或 CLI 足以完成首发操作。三个展示页面、Savepoint 同版本恢复、兼容升级、Kubernetes、Spark、CDC、指标平台及多 Worker 按后续需要推进。
