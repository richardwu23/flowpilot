---
title: FlowPilot 项目目录结构
status: planned
created: 2026-10-26
updated: 2026-10-26
---

# FlowPilot 项目目录结构

使用一个 repo，按 Python 后端、Java Flink 作业、部署配置、工具、测试和实验文档划分。以下为建议的目标结构，不表示全部目录和文件已经创建。

```text
/Users/richardwu/coding/flowpilot/
├── README.md
├── LICENSE
├── .gitignore
├── .env.example
├── alembic.ini
│
├── backend/                         # Python 平台后端
│   ├── pyproject.toml
│   ├── uv.lock
│   ├── Dockerfile                   # API、Worker 共用镜像
│   └── src/
│       └── flowpilot/
│           ├── __init__.py
│           ├── config.py
│           ├── api/
│           │   ├── main.py          # API 启动入口
│           │   └── routes/
│           ├── tasks/               # 任务与不可变版本
│           ├── deployments/         # 目标状态、实际观测
│           ├── operations/          # 操作状态与人工处理
│           ├── worker/
│           │   ├── main.py          # Worker 启动入口
│           │   ├── executor.py      # 推进操作步骤
│           │   └── reconciler.py    # 查询与核对外部状态
│           ├── adapters/
│           │   └── flink.py         # Flink REST 客户端
│           └── persistence/
│               ├── database.py
│               ├── models.py
│               └── repositories.py
│
├── migrations/                      # 平台元数据迁移
│   ├── env.py
│   └── versions/
│
├── jobs/
│   └── streaming/                   # Java Flink 作业
│       ├── pom.xml
│       └── src/
│           ├── main/java/dev/flowpilot/
│           │   ├── IdentityProbeJob.java
│           │   └── PaymentAggregationJob.java
│           └── test/java/dev/flowpilot/
│
├── deploy/
│   └── local/
│       ├── compose.yaml             # 四个基础服务，后续加入 API/Worker
│       └── postgres/
│           └── init.sql             # 首次初始化数据库与角色
│
├── tools/
│   ├── probe/
│   │   └── flink_submission.py      # 提交身份与响应丢失探针
│   ├── generator/
│   │   └── generate_events.py       # 生成固定业务输入
│   └── validator/
│       └── validate_results.py      # 独立计算并核对结果
│
├── tests/
│   ├── unit/                        # Python 状态转换等单元测试
│   ├── integration/                 # API、Worker、引擎集成测试
│   └── fixtures/                    # 固定输入与期望结果
│
├── experiments/                     # 实验步骤及实际结果
│   ├── phase-0-submission-identity.md
│   ├── phase-1-data-correctness.md
│   └── phase-2-worker-recovery.md
│
├── docs/
│   ├── README.md                    # 文档索引
│   ├── project-structure.md         # 本文件：建议的项目目录结构
│   ├── local-development.md         # 搭建与实施指南
│   ├── tested-versions.md           # 候选／已测版本及证据
│   ├── event-contract.md            # 输入、窗口、去重与迟到规则
│   ├── architecture.md              # 服务职责与管理链路
│   └── api.md                       # API 使用说明
│
└── .local/                          # 本地运行产物，不提交 Git
    ├── artifacts/                   # 构建后的实验制品
    ├── probe/                       # 提交意图、身份与查询记录
    └── reports/                     # 本地校验报告
```

## 当前阶段先创建的目录

阶段 0 先创建以下目录即可：

```text
deploy/local/postgres/
jobs/streaming/src/main/java/dev/flowpilot/
tools/probe/
tests/fixtures/
experiments/
docs/
.local/
```

先实现 `compose.yaml`、`pom.xml`、`IdentityProbeJob.java` 和提交探针。`backend/`、`migrations/`、生成器与校验器在对应阶段再增加，空文件不必提前建齐。

详细步骤见[本地搭建与分阶段实施指南](local-development.md)。
