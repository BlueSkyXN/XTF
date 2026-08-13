# XTF 配置参数详解

> 源码位置：[`core/config.py`](../core/config.py) · 配置模板：[`config.example.yaml`](../config.example.yaml)

---

## 目录

- [配置文件概述](#配置文件概述)
- [配置优先级体系](#配置优先级体系)
- [基础配置](#基础配置)
- [多维表格配置](#多维表格配置)
- [电子表格配置](#电子表格配置)
- [同步设置](#同步设置)
- [性能设置](#性能设置)
- [字段类型策略](#字段类型策略)
- [选择性同步配置](#选择性同步配置)
- [电子表格高级配置](#电子表格高级配置)
- [高级控制配置](#高级控制配置)
- [日志配置](#日志配置)
- [CLI 参数映射](#cli-参数映射)
- [常用配置场景](#常用配置场景)

---

## 配置文件概述

XTF 使用 YAML 格式配置文件。首次运行时，如果 `config.yaml` 不存在，系统会自动生成示例配置。

```bash
# 复制示例配置
cp config.example.yaml config.yaml

# 编辑配置文件
vim config.yaml
```

**配置加载流程**：

```
config.example.yaml (模板)
        ↓ 用户复制
config.yaml (用户配置)
        ↓ ConfigManager.load_config()
SyncConfig 数据类 (运行时)
        ↑ CLI 参数覆盖
--target-type, --batch-size, ...
```

---

## 配置优先级体系

XTF 支持四层配置来源，优先级从高到低：

| 优先级 | 来源 | 示例 | 说明 |
|--------|------|------|------|
| 1️⃣ 最高 | CLI 参数 | `--batch-size 100` | 临时覆盖，适合测试 |
| 2️⃣ | YAML 配置文件 | `batch_size: 500` | 持久化项目配置 |
| 3️⃣ | 智能推断 | 有 `app_token` → bitable | 基于已有配置自动判断 |
| 4️⃣ 最低 | 系统默认值 | `batch_size: 500` | 确保系统始终能运行 |

**CLI 支持级别标识**：

| 符号 | 含义 | 说明 |
|------|------|------|
| ✅ | 完全支持 | YAML 配置 + CLI 参数覆盖 |
| ⚠️ | 部分支持 | CLI 仅支持部分选项值 |
| ❌ | 仅 YAML | 只能通过配置文件设置 |

---

## 基础配置

> 源码：`core/config.py` → `SyncConfig` 基础字段

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `file_path` | `str` | — | ✅ `--file-path` | 数据文件路径（.xlsx / .xls / .csv） |
| `app_id` | `str` | — | ✅ `--app-id` | 飞书应用 ID |
| `app_secret` | `str` | — | ✅ `--app-secret` | 飞书应用密钥 |
| `target_type` | `str` | `bitable` | ✅ `--target-type` | 目标类型：`bitable` 或 `sheet` |

**文件格式支持**：

| 格式 | 扩展名 | 状态 | 说明 |
|------|--------|------|------|
| Excel 2007+ | `.xlsx` | ✅ 生产就绪 | Calamine 加速读取（4-20x） |
| Excel 97-2003 | `.xls` | ✅ 生产就绪 | 传统格式兼容 |
| CSV | `.csv` | 🧪 实验性 | UTF-8/GBK 自动编码检测 |

---

## 多维表格配置

> 源码：`core/config.py` → `SyncConfig` Bitable 字段
>
> 当 `target_type: bitable` 时使用

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `app_token` | `str` | — | ✅ `--app-token` | 多维表格应用 Token |
| `table_id` | `str` | — | ✅ `--table-id` | 数据表 ID |
| `create_missing_fields` | `bool` | `true` | ✅ `--create-missing-fields true/false` | 自动创建缺失字段 |
| `bitable_api_backend` | `str` | `base_v3` | ❌ | 明确选择 `base_v3` 或 `bitable_v1`；没有 `auto`，任何错误都不会切换 API family |
| `bitable_user_id_type` | `str` | `open_id` | ❌ | v1 人员字段 ID 类型：`open_id` / `union_id` / `user_id`；不按前缀猜测 |

**获取 Token 方式**：
- `app_token`：多维表格 URL 中 `base/` 后的字符串
- `table_id`：多维表格 URL 中 `table/` 后的字符串

---

## 电子表格配置

> 源码：`core/config.py` → `SyncConfig` Sheet 字段
>
> 当 `target_type: sheet` 时使用

### 基础参数

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `spreadsheet_token` | `str` | — | ✅ `--spreadsheet-token` | 电子表格 Token |
| `sheet_id` | `str` | — | ✅ `--sheet-id` | 工作表 ID |
| `start_row` | `int` | `1` | ✅ `--start-row` | 起始行号（1-based） |
| `start_column` | `str` | `A` | ✅ `--start-column` | 起始列号 |

### 读取渲染选项

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `sheet_value_render_option` | `str` | `null` | ❌ | 值渲染选项 |
| `sheet_datetime_render_option` | `str` | `null` | ❌ | 日期渲染选项 |

**值渲染选项说明**：

| 选项 | 说明 | 适用场景 |
|------|------|----------|
| `ToString` | 返回纯文本 | 展示用途 |
| `Formula` | 返回公式文本 | 校验公式是否被改动 |
| `FormattedValue` | 返回计算后的格式化值 | ⭐ 对比结果（推荐） |
| `UnformattedValue` | 返回计算后的原始值 | 精确数值对比 |
| `FormattedString` | 日期以格式化字符串返回 | 配合 FormattedValue 使用 |

### 分块控制

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `sheet_scan_max_rows` | `int` | `5000` | ❌ | 读取分块最大行数 |
| `sheet_scan_max_cols` | `int` | `100` | ❌ | 读取分块最大列数 |
| `sheet_write_max_rows` | `int` | `5000` | ❌ | 写入分块最大行数 |
| `sheet_write_max_cols` | `int` | `100` | ❌ | 写入分块最大列数 |

> 遇到 10MB/90227 限制时会自动行优先二分，必要时列二分。详见 [SHEET.md](./SHEET.md)

### 逻辑同步与结果检测

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `sheet_validate_results` | `bool` | `false` | ❌ | 启用双读结果检测 |
| `sheet_protect_formulas` | `bool` | `false` | ❌ | 仅 `full`：保护公式列不被覆盖 |
| `sheet_report_column_diff` | `bool` | `false` | ❌ | 输出列级差异报告 |
| `sheet_diff_tolerance` | `float` | `0.001` | ❌ | 数值比较容忍度 |

**配置组合**：

| 场景 | 配置 | 行为 |
|------|------|------|
| 检测差异 + 正常同步 | `validate_results=true` | 检测并报告差异，所有列正常同步 |
| 保护公式 + 检测差异 | `validate_results=true`, `protect_formulas=true` | 公式列只检测不覆盖，数据列正常同步 |
| 完整差异报告 | 以上 + `report_column_diff=true` | 额外输出列级差异统计 |

> ⚠️ 启用 `sheet_protect_formulas` 时会自动启用 `sheet_validate_results`，并要求
> `sync_mode: full` 和有效 `index_column`。其他模式在配置加载时直接拒绝，避免
> `overwrite` / `clone` 清空或重写公式。

> 详细机制说明：[SHEET.md](./SHEET.md)

---

## 同步设置

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `verify_remote_writes` | `bool` | `false` | ❌ | 对同步引擎产生的记录/单元格 mutation 做写后读回；默认关闭 |

写后读回不是事务或回滚。出现 partial、unknown outcome、incomplete read 或 mismatch 时，
引擎停止后续依赖阶段，但此前服务端已接受的批次仍可能保留。

> 源码：`core/config.py` → `SyncMode` 枚举

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `sync_mode` | `str` | `full` | ✅ `--sync-mode` | 同步模式 |
| `index_column` | `str` | — | ✅ `--index-column` | 索引列名（数据比对关键） |

**同步模式速览**：

| 模式 | 行为 | 数据安全 |
|------|------|----------|
| `full` | 已存在→更新，不存在→新增 | ✅ 安全 |
| `incremental` | 已存在→跳过，不存在→新增 | ✅ 安全 |
| `overwrite` | 删除已存在→重新创建 | ⚠️ 部分数据删除 |
| `clone` | 清空全部→重新创建 | 🔴 全部数据清除 |

> 详细说明：[SYNC.md](./SYNC.md)（含 Bitable/Sheet 分版本详解）

---

## 性能设置

| 参数名 | 类型 | Bitable 默认 | Sheet 默认 | CLI | 说明 |
|--------|------|-------------|------------|-----|------|
| `batch_size` | `int` | `500` | `1000` | ✅ `--batch-size` | 批处理大小 |
| `rate_limit_delay` | `float` | `0.01` | `0.1` | ✅ `--rate-limit-delay` | API 调用间隔（秒） |
| `max_retries` | `int` | `3` | `3` | ✅ `--max-retries` | 最大重试次数 |

**调优建议**：
- **大数据集**：降低 `batch_size`（如 100-200），避免请求超限
- **限流频繁**：增大 `rate_limit_delay`（如 1.0-2.0）
- **网络不稳定**：增大 `max_retries`（如 5-10）

> 飞书多维表格 API 官方频率限制：查询 20 次/秒，写入 50 次/秒。
> 程序直接使用官方限制作为内嵌上限，并对限流错误码自动重试。
> 详见 [CONTROL.md](./CONTROL.md#飞书-api-频率限制参考)

---

## 字段类型策略

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `field_type_strategy` | `str` | `base` | ✅ `--field-type-strategy` | 字段类型策略 |
| `intelligence_date_confidence` | `float` | `0.85` | ❌ | 日期类型置信度阈值 |
| `intelligence_choice_confidence` | `float` | `0.9` | ❌ | 选择类型置信度阈值 |
| `intelligence_boolean_confidence` | `float` | `0.95` | ❌ | 布尔类型置信度阈值 |

**策略对比**：

| 策略 | 支持类型 | 风险 | 推荐场景 |
|------|----------|------|----------|
| `raw` | 文本 | 最低 | 数据完整性要求极高 |
| `base` | 文本/数字/日期 | 低 | ⭐ 日常使用（默认） |
| `auto` | + 单选/多选（Excel 验证） | 中 | 有标准化 Excel 模板 |
| `intelligence` | 全部类型 | 较高 | 高质量数据 + 进阶用户 |

> 详细说明：[FIELD_TYPES.md](./FIELD_TYPES.md)

---

## 选择性同步配置

> 源码：`core/config.py` → `SelectiveSyncConfig`

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `selective_sync.enabled` | `bool` | `false` | ❌ | 启用选择性列同步 |
| `selective_sync.columns` | `list` | `[]` | ❌ | 要同步的列名列表 |
| `selective_sync.auto_include_index` | `bool` | `true` | ❌ | 自动包含索引列 |
| `selective_sync.optimize_ranges` | `bool` | `true` | ❌ | 优化合并相邻列范围（仅 Sheet） |
| `selective_sync.max_gap_for_merge` | `int` | `2` | ❌ | 兼容参数；`0` 禁用合并，正数仅启用相邻目标列合并（仅 Sheet） |
| `selective_sync.preserve_column_order` | `bool` | `true` | ❌ | 保持原始列顺序 |

**配置示例**：

```yaml
selective_sync:
  enabled: true
  columns: ["salary", "department", "last_updated"]
  auto_include_index: true
  optimize_ranges: true
  max_gap_for_merge: 2
  preserve_column_order: true
```

**约束**：
- ❌ 不支持 `clone` 模式（逻辑冲突：克隆需要完整数据）
- `columns` 不能包含空字符串或重复列名
- `max_gap_for_merge` 范围 0-50；`0` 禁用合并，正数启用安全的相邻列合并
- 非相邻目标列始终使用独立 range，避免写空中间列并覆盖未选择数据

> 详细说明：[SYNC.md](./SYNC.md#选择性列同步)

---

## 电子表格高级配置

本节配置仅在 `target_type: sheet` 时生效，均为 YAML-only 配置。

完整配置示例：

```yaml
target_type: sheet
spreadsheet_token: "your_token"
sheet_id: "your_sheet_id"
start_row: 1
start_column: "A"

# 读取渲染
sheet_value_render_option: "FormattedValue"
sheet_datetime_render_option: "FormattedString"

# 分块上限
sheet_scan_max_rows: 5000
sheet_scan_max_cols: 100
sheet_write_max_rows: 5000
sheet_write_max_cols: 100

# 逻辑同步与结果检测
sheet_validate_results: true
sheet_protect_formulas: true
sheet_report_column_diff: true
sheet_diff_tolerance: 0.001
```

---

## 高级控制配置

> 源码：`core/control.py`
>
> 仅在 `enable_advanced_control: true` 时生效

| 参数名 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `enable_advanced_control` | `bool` | `false` | 启用高级重试与频控 |
| `retry_strategy_type` | `str` | `exponential_backoff` | 重试策略类型 |
| `retry_initial_delay` | `float` | `0.5` | 初始重试延迟（秒） |
| `retry_max_wait_time` | `float` | `null` | 最大重试等待时间 |
| `retry_multiplier` | `float` | `2.0` | 指数退避乘数 |
| `retry_increment` | `float` | `0.5` | 线性增长步长 |
| `rate_limit_strategy_type` | `str` | `fixed_wait` | 频控策略类型 |
| `rate_limit_window_size` | `float` | `1.0` | 时间窗口大小（秒） |
| `rate_limit_max_requests` | `int` | `10` | 窗口内最大请求数 |

**配置示例**：

```yaml
enable_advanced_control: true
retry_strategy_type: "exponential_backoff"
retry_initial_delay: 0.5
retry_multiplier: 2.0
rate_limit_strategy_type: "sliding_window"
rate_limit_window_size: 1.0
rate_limit_max_requests: 10
```

> 详细说明：[CONTROL.md](./CONTROL.md)

---

## 日志配置

| 参数名 | 类型 | 默认值 | CLI | 说明 |
|--------|------|--------|-----|------|
| `log_level` | `str` | `INFO` | ✅ `--log-level` | 日志级别 |

**日志级别**：

| 级别 | 内容 |
|------|------|
| `DEBUG` | 完整请求/响应、数据转换细节、字段分析过程 |
| `INFO` | 同步进度、批处理状态、转换统计（默认） |
| `WARNING` | 类型转换失败、格式异常、性能告警 |
| `ERROR` | API 错误、配置异常、致命异常 |

**日志输出**：
- 控制台：实时显示同步进度
- 文件：`logs/xtf_{target_type}_{YYYYMMDD_HHMMSS}.log`

---

## CLI 参数映射

完整的 CLI 参数与 YAML 配置对照表：

| CLI 参数 | YAML 字段 | 类型 | 说明 |
|----------|-----------|------|------|
| `--config, -c` | — | `str` | 配置文件路径（默认 `config.yaml`） |
| `--file-path` | `file_path` | `str` | 数据文件路径 |
| `--app-id` | `app_id` | `str` | 飞书应用 ID |
| `--app-secret` | `app_secret` | `str` | 飞书应用密钥 |
| `--target-type` | `target_type` | `str` | `bitable` / `sheet` |
| `--app-token` | `app_token` | `str` | 多维表格 Token |
| `--table-id` | `table_id` | `str` | 数据表 ID |
| `--create-missing-fields` | `create_missing_fields` | `bool` | 自动创建字段 |
| `--no-create-fields` | `create_missing_fields` | — | 禁用字段创建 |
| `--field-type-strategy` | `field_type_strategy` | `str` | 字段类型策略 |
| `--spreadsheet-token` | `spreadsheet_token` | `str` | 电子表格 Token |
| `--sheet-id` | `sheet_id` | `str` | 工作表 ID |
| `--start-row` | `start_row` | `int` | 起始行号 |
| `--start-column` | `start_column` | `str` | 起始列号 |
| `--sync-mode` | `sync_mode` | `str` | 同步模式 |
| `--index-column` | `index_column` | `str` | 索引列名 |
| `--batch-size` | `batch_size` | `int` | 批处理大小 |
| `--rate-limit-delay` | `rate_limit_delay` | `float` | API 间隔（秒） |
| `--max-retries` | `max_retries` | `int` | 最大重试次数 |
| `--log-level` | `log_level` | `str` | 日志级别 |

---

## 常用配置场景

### 场景一：首次同步（Bitable）

```yaml
file_path: "data.xlsx"
app_id: "cli_xxx"
app_secret: "xxx"
target_type: bitable
app_token: "xxx"
table_id: "xxx"
sync_mode: full
index_column: "ID"
field_type_strategy: base
create_missing_fields: true
```

### 场景二：日常增量同步

```yaml
sync_mode: incremental
index_column: "ID"
batch_size: 500
```

### 场景三：大数据集电子表格同步

```yaml
target_type: sheet
batch_size: 200
rate_limit_delay: 0.3
sheet_scan_max_rows: 3000
sheet_write_max_rows: 3000
```

### 场景四：保护公式 + 差异报告

```yaml
target_type: sheet
sync_mode: full
sheet_validate_results: true
sheet_protect_formulas: true
sheet_report_column_diff: true
sheet_diff_tolerance: 0.001
```

### 场景五：选择性列同步

```yaml
sync_mode: full
index_column: "ID"
selective_sync:
  enabled: true
  columns: ["salary", "department"]
  auto_include_index: true
```

### 场景六：高级频控配置

```yaml
enable_advanced_control: true
retry_strategy_type: "exponential_backoff"
retry_initial_delay: 0.5
retry_multiplier: 2.0
rate_limit_strategy_type: "sliding_window"
rate_limit_window_size: 1.0
rate_limit_max_requests: 10
```
