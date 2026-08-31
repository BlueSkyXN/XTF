# XTF 文档中心

> XTF 2.0 — flags-first、可 dry-run、可 JSON 自动化的 Excel/CSV/Bitable 到飞书同步 CLI。

## 📚 核心文档

### [ARCH.md](./ARCH.md) — 系统架构文档 ⭐

XTF 的整体架构设计、核心组件交互、数据处理流水线与扩展机制。

1. 系统概览与设计哲学
2. CLI → config resolver → planner/executor → API 分层
3. 数据处理流水线与结构化 outcome
4. 错误处理与三层上传保障
5. 扩展点与二次开发指南

### [CONFIG.md](./CONFIG.md) — 配置参数详解

严格 YAML schema v2、完整 CLI override、配置发现和退出码参考。

1. CLI / `XTF_APP_SECRET` / YAML / defaults 优先级
2. source、target、sync、conversion、control 嵌套 schema
3. `config init/validate/show` 与 flags-first 覆盖
4. dry-run、删除授权、JSON 和退出码

### [SYNC.md](./SYNC.md) — 同步模式与选择性同步

四种同步模式的深度解析，含 Bitable/Sheet 双平台差异、选择性列同步机制与性能优化。

1. 全量 / 增量 / 覆盖 / 克隆模式详解（含 Bitable/Sheet 分版本流程图）
2. 模式对比表（Bitable 与 Sheet 维度）
3. 选择性列同步（字段级 & 列范围级精确控制）
4. API 接口选择策略与性能优化（field_names 按需获取）

### [FIELD_TYPES.md](./FIELD_TYPES.md) — 智能字段类型策略

四种字段类型策略的检测算法、转换规则与置信度机制。

1. Raw / Base / Auto / Intelligence 策略对比
2. 数据类型检测算法（数字、日期、布尔、枚举等）
3. 强制类型转换与数据清洗规则
4. 转换统计报告与调优建议

### [SHEET.md](./SHEET.md) — 电子表格算法设计

面向 Sheet 目标的深度技术文档，含分块机制、公式保护与差异检测。

1. 三层大数据稳定上传保障机制
2. 智能分块策略（行优先二分 → 列二分）
3. 公式保护与双读验证
4. 列级差异检测报告

### [CONTROL.md](./CONTROL.md) — 高级重试与频控策略

企业级高级控制系统，含 3 种重试策略 × 3 种频控策略的组合配置。

1. 重试策略（指数退避 / 线性增长 / 固定等待）
2. 频控策略（固定等待 / 滑动窗口 / 固定窗口）
3. 飞书 API 官方频率限制参考与错误码处理
4. 预置配置方案（保守 / 渐进 / 激进 / 调试）
5. 参数调优与性能监控

---

## 📦 参考资料

### [feishu-openapi-doc/](./feishu-openapi-doc/) — 飞书 OpenAPI 文档

AI 友好型飞书 OpenAPI Markdown 文档库，便于查阅与扩展开发。

### [../CLAUDE.md](../CLAUDE.md) — Claude Code 开发指南

面向 AI 辅助开发的项目上下文文件，包含架构概览、开发命令与注意事项。

---

## 🚀 快速开始

1. 安装依赖：`pip install -r requirements.txt`
2. 查看命令：`python3 XTF.py --help`
3. 生成 v2 配置：`python3 XTF.py config init --target-type bitable`
4. 填入实际资源标识，并通过 `--app-secret` 或 `XTF_APP_SECRET` 提供 secret；模板占位值不可直接用于远端操作。
5. 本地验证：`python3 XTF.py config validate --config config.yaml`，再运行 `python3 XTF.py doctor --config config.yaml`。
6. 需要验证远端只读可达性时：`python3 XTF.py doctor --config config.yaml --network`。
7. 精确只读计划：`python3 XTF.py sync --config config.yaml --dry-run --json`。
8. 审阅 plan 后才正式同步：`python3 XTF.py sync --config config.yaml`；任何 destructive plan 都必须额外传 `--allow-delete`。

`--dry-run` 可进行远端只读调用，但不执行 Feishu mutation；它仍可能创建本地运行日志，且不构成真实业务 UAT。

---

## 📊 文档统计

| 文档 | 内容 | 面向读者 |
|------|------|----------|
| [ARCH.md](./ARCH.md) | 系统架构与组件设计 | 开发者、架构师 |
| [CONFIG.md](./CONFIG.md) | 配置参数完整参考 | 所有用户 |
| [SYNC.md](./SYNC.md) | 同步模式与选择性同步（含 Bitable/Sheet 分版本详解） | 所有用户 |
| [FIELD_TYPES.md](./FIELD_TYPES.md) | 字段类型策略与转换 | 进阶用户、开发者 |
| [SHEET.md](./SHEET.md) | 电子表格算法与公式保护 | 进阶用户、开发者 |
| [CONTROL.md](./CONTROL.md) | 高级重试与频控配置 | 进阶用户 |
