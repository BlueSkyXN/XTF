# XTF 文档中心

> XTF (Excel To Feishu) — 企业级 Excel/CSV 到飞书平台的智能同步工具。
> 支持多维表格 (Bitable) 与电子表格 (Sheet) 双目标，四种同步模式，智能字段管理。

## 📚 核心文档

### [ARCH.md](./ARCH.md) — 系统架构文档 ⭐

XTF 的整体架构设计、核心组件交互、数据处理流水线与扩展机制。

1. 系统概览与设计哲学
2. 四层架构（入口 → 配置 → 引擎 → API）
3. 数据处理流水线（6 步完整流程）
4. 错误处理与三层上传保障
5. 扩展点与二次开发指南

### [CONFIG.md](./CONFIG.md) — 配置参数详解

完整的配置文件参考手册，含每个字段的类型、默认值、代码位置与实际影响。

1. 配置优先级体系（CLI > YAML > 推断 > 默认）
2. 通用参数、目标平台参数、性能参数
3. 选择性同步、高级控制、逻辑检测等进阶配置
4. CLI 参数映射与常用配置场景

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
2. 复制配置：`cp config.example.yaml config.yaml`
3. 编辑 `config.yaml`，填入飞书应用凭证和目标表格信息
4. 多维表格同步：`python XTF.py --target-type bitable --config config.yaml`
5. 电子表格同步：`python XTF.py --target-type sheet --config config.yaml`
6. 查看日志：`logs/xtf_*.log`

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
