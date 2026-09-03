# XTF 2.0 — Excel To Feishu CLI

XTF 是一个 flags-first 的数据同步 CLI，将本地 Excel/CSV 或另一张多维表格的数据同步到飞书 Bitable 或 Sheet。它提供可审计的只读计划、结构化结果、稳定退出码、显式删除授权以及严格的 YAML v2 配置。

> 🧪 CSV 格式为实验性支持（测试阶段）。Excel (.xlsx/.xls) 为生产就绪的主要格式。

## 核心特性

- **正式 CLI** — `sync`、`config`、`doctor` 和 `--version` 使用同一个 parser 与退出码契约
- **双平台支持** — 多维表格 (Bitable) 与电子表格 (Sheet)，源码入口 `XTF.py`、发布二进制 `XTF`
- **四种同步模式** — 全量 / 增量 / 覆盖 / 克隆，覆盖全场景数据同步需求
- **远端差异同步** — 从另一张多维表格读取数据，只新增缺失记录或更新真实差异，不删除目标多余记录
- **精确 Dry-run** — 允许只读 Feishu API 调用并输出真实 action 计划，但不会执行任何 mutation
- **机器可读输出** — `--json` 输出稳定的 plan/outcome/error 结构，适合 shell、Cron 和 CI
- **智能字段类型** — Raw / Base / Auto / Intelligence 四种策略，从保守到智能逐级增强
- **选择性列同步** — 精确列级控制，只更新指定列，其他列完全不受影响
- **公式保护** — `full` 模式双读检测云端公式，无法确认公式状态时停止写入
- **Typed 安全门禁** — 统一分块、mutation receipt、snapshot freshness 和可选写后读回
- **高级频控** — 3 种重试策略 × 3 种频控策略，9 种组合灵活配置
- **Excel 引擎回退** — 优先使用可用的 Calamine 引擎，必要时回退 OpenPyXL

## 快速开始

### 环境要求

- **Python 3.10+**（支持 3.10, 3.11, 3.12, 3.13）

### 安装

```bash
pip install -r requirements.txt
```

### 查看 CLI

```bash
python3 XTF.py --version
python3 XTF.py --help
python3 XTF.py sync --help
```

源码运行使用 `python3 XTF.py`；发布包将相同命令暴露为 `XTF`。

### 配置

```bash
# 显式生成严格的 YAML schema v2；已存在时需额外传 --force 才会覆盖
python3 XTF.py config init --target-type bitable --output config.yaml

# 仅做本地 schema/组合检查
python3 XTF.py config validate --config config.yaml

# 查看最终配置和来源，秘密与资源 token 会脱敏
python3 XTF.py config show --config config.yaml
```

主程序不接受旧 flat YAML，也不自动迁移。`app_secret` 优先级为 **CLI > `XTF_APP_SECRET` > YAML**；其他值为 **CLI > YAML > target-specific defaults**。未传 `--config` 时会自动发现当前目录的 `config.yaml`，也可以完全使用 flags/ENV。

### 运行

```bash
# 多维表格同步
python3 XTF.py sync --config config.yaml

# 联网读取源/目标并生成精确计划；零 mutation
python3 XTF.py sync --config config.yaml --dry-run

# 单一 JSON 文档输出
python3 XTF.py sync --config config.yaml --dry-run --json

# flags-first：YAML 只是可选 preset
python3 XTF.py sync \
  --app-id cli_xxx \
  --source-type file --file data.xlsx \
  --target-type bitable --target-app-token app_xxx --target-table-id tbl_xxx \
  --mode full --match-strategy by_key --index-column ID

# destructive 配置必须显式授权删除；clone 配置中须省略 match_strategy
python3 XTF.py sync --config config-clone.yaml --allow-delete
```

### 推荐的安全操作顺序

`config init` 生成的是包含占位值的 v2 模板；请先填入实际资源标识，并通过
`--app-secret` 或 `XTF_APP_SECRET` 提供 secret。不要把真实 secret 写入示例、提交或日志。

```bash
# 仅验证 schema、组合和本地输入
python3 XTF.py config validate --config config.yaml
python3 XTF.py doctor --config config.yaml

# 可选：认证并读取字段/工作表 metadata；仍不执行 Feishu mutation
python3 XTF.py doctor --config config.yaml --network

# 先获取与正式执行使用同一 planner 生成的计划
python3 XTF.py sync --config config.yaml --dry-run --json > sync-plan.json

# 审核计划后再执行。只有计划或 mode 需要删除时才传 --allow-delete。
python3 XTF.py sync --config config.yaml
```

`--dry-run` 的“零写入”只指 Feishu mutation；运行时仍可能创建本地 `logs/` 文件。计划和
JSON 结果不会包含完整 token、secret、记录正文或 mutation payload，但 dry-run 也不替代真实业务验收。

### 公共契约边界

XTF 2.0 的稳定公共接口仅包括 CLI、YAML v2、JSON output 和退出码，不提供稳定 Python
SDK。`core.*`、`api.*`、`SyncService` 和 typed client constructor 都是仓库内部实现，可以随
内部架构演进而变化。需要自动化集成时使用 `XTF ... --json`；Bitable backend 可在 YAML
中显式选择 `base_v3` 或 `bitable_v1`，不会自动 fallback。

## 同步模式

| 模式 | `match_strategy` | 已存在记录 | 不存在记录 | 数据安全 |
|------|------------------|-----------|-----------|----------|
| `full` | `by_key` | 更新 | 新增 | ✅ 安全 |
| `incremental` | `by_key` | 跳过 | 新增 | ✅ 安全 |
| `incremental` | `append_only` | 不匹配 | 全部追加 | ✅ 显式追加 |
| `overwrite` | `by_key` | 删除后重建 | 新增 | ⚠️ 需要 `--allow-delete` |
| `clone` | 省略 | 全部清除 | 全部创建 | 🔴 需要 `--allow-delete` |

> `overwrite` / `clone`，以及 planner 发现的任何 `delete_records` / `clear_range` action，在正式执行时都要求 `--allow-delete`。`--dry-run` 永远不执行 mutation。

`source.type: bitable` 不会创建新 Base，也不会复制视图、权限或自动化。它只读取源表
记录，并按 `sync.index.column` 写入已有目标表：`full` 新增缺失记录且仅更新发生变化的字段，
`incremental` 只新增缺失记录；两种模式都不会删除目标表多余记录。源、目标字段需预先
存在且类型兼容，公式、附件、系统字段和需要跨表 ID 映射的关联字段不会作为普通数据复制。

`full` / `overwrite` 只支持 `by_key`；`incremental` 可显式选择 `by_key` 或
`append_only`；`clone` 固定使用 replace-all，不读取 `match_strategy`。`by_key`
必须配置非空索引列，不再因目标为空或 Sheet 缺少索引而隐式转成 clone。

日期时间索引默认统一到 UTC 后使用完整毫秒值。选择
`sync.index.datetime_granularity: day` 时还必须通过
`sync.index.timezone`（或 `--datetime-index-timezone`）提供 IANA 业务时区，例如
`Asia/Shanghai`；不再依赖宿主机本地时区。

**详细说明**：[docs/SYNC.md](docs/SYNC.md)（含 Bitable/Sheet 分版本详解）

## 字段类型策略

| 策略 | 支持类型 | 推荐场景 |
|------|----------|----------|
| `raw` | 文本 | 数据完整性要求极高 |
| `base` | 文本 / 数字 / 日期 | ⭐ 日常使用（默认） |
| `auto` | + 单选 / 多选（Excel 验证） | 标准化 Excel 模板 |
| `intelligence` | 全部 8 种类型 | 高质量数据、进阶用户 |

```bash
python3 XTF.py sync --field-type-strategy base          # 默认推荐
python3 XTF.py sync --field-type-strategy intelligence  # 全面智能
```

**详细说明**：[docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)

## 文件格式支持

| 格式 | 扩展名 | 状态 | 读取引擎 |
|------|--------|------|----------|
| Excel 2007+ | `.xlsx` | ✅ 生产就绪 | Calamine（可用时）→ 回退 OpenPyXL |
| Excel 97-2003 | `.xls` | ✅ 生产就绪 | Calamine → 回退 OpenPyXL |
| CSV | `.csv` | 🧪 实验性 | pandas（UTF-8/GBK 自动检测） |

## 项目结构

```
XTF/
├── XTF.py                    # 薄可执行入口
├── xtf_cli/                  # parser、config v2、命令、输出与退出码
├── core/
│   ├── config.py             # mode/source/target/strategy 枚举
│   ├── runtime_config.py     # 嵌套、不可变运行时配置
│   ├── plan.py               # ExecutionPlan / PlanDocument / SyncResult
│   ├── snapshot.py           # SourceTable / BitableSnapshot / SheetSnapshot
│   ├── compiler.py           # Bitable / Sheet typed action compiler
│   ├── bootstrap.py          # 显式装配 logger/controller/auth/transport/client
│   ├── service.py            # 单一 planner + executor 主干
│   ├── converter.py          # 数据转换（类型分析、转换、统计报告）
│   └── control.py            # 高级控制（重试策略、频控策略）
├── api/
│   ├── auth.py               # 飞书认证
│   ├── base.py               # 基础 HTTP 客户端（重试、频控）
│   ├── sdk.py                # typed error、分页与批处理契约
│   ├── bitable_backend.py    # canonical Bitable contracts
│   ├── bitable_v1.py         # Bitable v1 wire client
│   ├── bitable_v3.py         # Base v3 wire client
│   └── sheet.py              # 电子表格 API
├── utils/
│   └── excel_reader.py       # Excel/CSV 读取器
├── config.example.yaml       # 主程序 YAML schema v2 模板
├── QUICKSTART.md             # 安装、运行和 1.9→2.0 人工迁移
├── requirements.txt          # 依赖
├── docs/                     # 详细文档
└── logs/                     # 运行日志
```

## 详细文档

| 文档 | 内容 |
|------|------|
| **[QUICKSTART.md](QUICKSTART.md)** | 快速开始与 1.9→2.0 人工迁移 |
| **[docs/README.md](docs/README.md)** | 📚 文档中心，导航与快速入门 |
| **[docs/ARCH.md](docs/ARCH.md)** | 系统架构，四层设计，组件交互 |
| **[docs/CONFIG.md](docs/CONFIG.md)** | 配置参数完整参考，CLI 映射 |
| **[docs/SYNC.md](docs/SYNC.md)** | 同步模式详解（含 Bitable/Sheet 分版本）、选择性列同步 |
| **[docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)** | 字段类型策略，检测算法，转换规则 |
| **[docs/SHEET.md](docs/SHEET.md)** | 电子表格算法，分块机制，公式保护 |
| **[docs/CONTROL.md](docs/CONTROL.md)** | 高级重试与频控策略配置 |
| **[docs/RELEASE_NOTES_2_0.md](docs/RELEASE_NOTES_2_0.md)** | 2.0 breaking changes 与发布门禁 |

## 常见问题

**Q: 配置文件缺少参数？**
使用 `config validate` 查看严格 schema v2 和组合错误；使用 `config init` 显式创建模板。`sync` 不会生成或改写配置。

**Q: 同步失败如何排查？**
关注 stderr 日志与最终 outcome；自动化使用 `--json` 和进程退出码。`--log-level DEBUG` 可输出详细诊断。

**Q: 大数据集处理超时？**
在 `sync` 子命令降低 `--batch-size`（如 100-200），增大 `--max-retries`，或启用高级频控。详见 [docs/SHEET.md](docs/SHEET.md)。

**Q: 频繁触发 API 限流（错误码 1254290）？**
HTTP `429/5xx` 和网络异常由 transport 统一重试；HTTP 200 中的飞书限流业务码由 Bitable 层重试，不会叠加 HTTP 重试。如仍频繁限流，请启用高级频控：详见 [docs/CONTROL.md](docs/CONTROL.md)。

**Q: 公式保护可以和哪些同步模式组合？**
`target.sheet.protect_formulas: true` 仅支持 `full`，并且需要配置 `sync.index.column`。`incremental`、`overwrite`、`clone` 会在配置加载时被拒绝，避免误以为破坏性模式也会保留公式。

写后公式验证使用独立的 `target.sheet.verify_formulas: true`，只校验 typed receipt 能证明的
成功写入范围；它不会由公式保护自动开启，也不会为新增行生成或复制公式。append 缺少
实际落点、Sheet AI 返回 partial/errors 或仍有更多结果时，同步失败关闭。任何已发送
mutation 的结果未知时返回 `indeterminate`（exit `8`）；自动下拉和显示格式等
best-effort enrichment 失败只产生 warning，不覆盖已确认的数据成功状态。

**Q: 字段类型推荐不准确？**
降级到 `base` 策略确保稳定，或调整 Intelligence 策略的置信度阈值。详见 [docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)。

**Q: 如何只同步部分列？**
在 YAML v2 中启用选择性同步，或在 CLI 中重复使用 `--column`。详见 [docs/SYNC.md](docs/SYNC.md#7-选择性列同步)。

```yaml
sync:
  selective:
    enabled: true
    columns: ["salary", "department"]
```

## 日志

- **stdout**：最终 human 摘要或单一 JSON 文档
- **stderr**：进度、warning、诊断和错误
- **文件**：`logs/xtf_{target}_{YYYYMMDD_HHMMSS}.log`
- **级别**：`XTF sync --log-level DEBUG|INFO|WARNING|ERROR`
- **统计报告**：每次同步自动生成转换统计

## License

[MIT](LICENSE)
