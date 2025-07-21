# XTF - Excel To Feishu 智能同步工具

XTF (Excel To Feishu) 是一个企业级的本地 Excel 表格到飞书平台的智能同步工具。支持多维表格和电子表格两种目标平台，具备四种同步模式，智能字段管理、类型转换、频率控制、重试机制等企业级功能特性。

## 🎯 双平台自动适配

XTF 通过主入口 [`XTF.py`](XTF.py) 支持**多维表格**（bitable）和**电子表格**（sheet）两种目标平台。只需通过 `--target-type` 参数切换，无需更换脚本。

## ✨ 核心特性

- 🔄 四种智能同步模式（全量、增量、覆盖、克隆）
- 🎯 智能字段管理与类型推断（原值/基础/自动/智能四种策略）
- 📊 大数据批量处理与性能优化
- 🛡️ 频率控制与智能重试
- 📝 详细日志与转换统计报告
- 🔧 灵活 YAML 配置 + 命令行参数，支持多环境切换

---

## ⚙️ 配置文件参数说明

请将 `config.example.yaml` 复制为 `config.yaml` 并根据实际需求填写。大部分参数支持命令行覆盖，优先级为：**命令行参数 > 配置文件 > 智能推断 > 系统默认值**。

### 配置优先级说明

1. **命令行参数**（最高优先级）- 用于临时覆盖和测试
2. **配置文件** - 项目的持久化配置
3. **智能推断** - 基于配置自动判断（如有 `app_token` 推断为 bitable）
4. **系统默认值**（最低优先级）- 确保系统始终能运行

### CLI 支持说明

| 符号 | 含义 | 说明 |
|------|------|------|
| ✅ | 完全支持 | 支持 YAML 配置 + 命令行参数覆盖 |
| ⚠️ | 部分支持 | CLI 仅支持部分选项值，完整支持需使用 YAML |
| ❌ | 仅 YAML | 只能通过 YAML 配置文件设置，不支持命令行覆盖 |


### 通用参数

| 参数名            | 类型    | 默认值      | CLI 支持 | CLI 参数 | 说明                         |
|-------------------|---------|-------------|----------|----------|------------------------------|
| file_path         | str     | -           | ✅       | `--file-path` | Excel 文件路径               |
| app_id            | str     | -           | ✅       | `--app-id` | 飞书应用 ID                 |
| app_secret        | str     | -           | ✅       | `--app-secret` | 飞书应用密钥                |
| target_type       | str     | bitable     | ✅       | `--target-type` | 目标类型：bitable 或 sheet  |
| sync_mode         | str     | full        | ✅       | `--sync-mode` | 同步模式：full/incremental/overwrite/clone |
| index_column      | str     | -           | ✅       | `--index-column` | 索引列名，数据比对关键      |
| batch_size        | int     | 500/1000    | ✅       | `--batch-size` | 批处理大小               |
| rate_limit_delay  | float   | 0.5/0.1     | ✅       | `--rate-limit-delay` | API 调用间隔（秒）       |
| max_retries       | int     | 3           | ✅       | `--max-retries` | 最大重试次数                |
| log_level         | str     | INFO        | ✅       | `--log-level` | 日志级别：DEBUG/INFO/WARNING/ERROR |
| field_type_strategy | str   | base        | ✅       | `--field-type-strategy` | 字段类型策略：raw/base/auto/intelligence |

### 多维表格专用

| 参数名                | 类型    | 默认值 | CLI 支持 | CLI 参数 | 说明                         |
|-----------------------|---------|--------|----------|----------|------------------------------|
| app_token             | str     | -      | ✅       | `--app-token` | 多维表格应用 Token          |
| table_id              | str     | -      | ✅       | `--table-id` | 多维表格数据表 ID           |
| create_missing_fields | bool    | true   | ✅       | `--create-missing-fields true/false` 或 `--no-create-fields` | 自动创建缺失字段|

### 电子表格专用

| 参数名            | 类型    | 默认值 | CLI 支持 | CLI 参数 | 说明                         |
|-------------------|---------|--------|----------|----------|------------------------------|
| spreadsheet_token | str     | -      | ✅       | `--spreadsheet-token` | 电子表格 Token              |
| sheet_id          | str     | -      | ✅       | `--sheet-id` | 电子表格工作表 ID           |
| start_row         | int     | 1      | ✅       | `--start-row` | 起始行号（1-based）         |
| start_column      | str     | A      | ✅       | `--start-column` | 起始列号                    |

### Intelligence 策略专用配置（多平台通用）

| 参数名                | 类型    | 默认值 | CLI 支持 | CLI 参数 | 说明                         |
|-----------------------|---------|--------|----------|----------|------------------------------|
| intelligence_date_confidence | float | 0.85 | ❌ | 仅YAML | Intelligence策略日期类型置信度 |
| intelligence_choice_confidence | float | 0.9 | ❌ | 仅YAML | Intelligence策略选择类型置信度 |
| intelligence_boolean_confidence | float | 0.95 | ❌ | 仅YAML | Intelligence策略布尔类型置信度 |

---

## 🚀 快速开始

### 多维表格同步

```bash
python XTF.py --target-type bitable --config config.yaml
```

### 电子表格同步

```bash
python XTF.py --target-type sheet --config config.yaml
```

### 常用参数

- `--sync-mode` 同步模式（full/incremental/overwrite/clone）
- `--field-type-strategy` 字段类型策略（raw/base/auto/intelligence）
- `--index-column` 索引列名
- `--batch-size` 批处理大小
- `--log-level` 日志级别

---

## 📊 同步模式详解

| 模式         | 描述 | 适用场景 |
|--------------|------|----------|
| **全量同步 (full)** | 已存在索引值的记录执行**更新**，不存在的执行**新增** | 日常数据同步，既有更新又有新增 |
| **增量同步 (incremental)** | 已存在索引值的记录**跳过**，只新增不存在的记录 | 只添加新数据，保护已有数据 |
| **覆盖同步 (overwrite)** | 删除已存在索引值的远程记录，然后新增本地全部记录 | 本地数据为准，覆盖部分远程数据 |
| **克隆同步 (clone)** | 清空远程表格全部数据，然后新增本地全部记录 | 完全重建远程表格，数据迁移 |

**注意**：overwrite/clone 模式会删除远程数据，生产环境请务必备份！

---

## 🧠 智能字段类型选择机制

### 四种字段类型策略

- **原值策略 (raw)** - 完全保持原值 🛡️
  - **多维表格**: 所有字段创建为文本类型，不进行数据类型转换
  - **电子表格**: 不应用任何格式化，保持Excel原始数据和格式
  - 适合数据完整性要求极高的场景

- **基础策略 (base)** - 默认推荐 ⭐
  - **多维表格**: 仅创建文本(1)/数字(2)/日期(5)三种基础类型
  - **电子表格**: 自动设置日期/数字格式，不创建下拉列表
  - 最大化数据同步成功率，适合企业级应用

- **自动策略 (auto)**
  - **多维表格**: 在基础类型上增加Excel类型检测
  - **电子表格**: 基于Excel数据验证设置下拉列表
  - 仅在检测到Excel数据验证时推荐单选/多选
  - 适合标准化Excel模板和表单

- **智能策略 (intelligence)**
  - **多维表格**: 基于置信度算法积极推荐各种字段类型
  - **电子表格**: 基于数据分析智能创建下拉列表、格式化
  - 支持所有类型，仅支持配置文件调整参数
  - 适合高质量数据和高级用户

### 智能数据类型识别

| Excel 数据类型 | 飞书字段类型 | 转换规则 | 示例 |
|----------------|--------------|----------|------|
| **文本** | 文本(1) | 直接转换 | `"张三"` → `"张三"` |
| **整数/小数** | 数字(2) | 保持数值，清理格式 | `"$1,234.56"` → `1234.56` |
| **日期时间** | 日期(5) | 多格式解析，转为毫秒时间戳 | `"2024年1月1日"` → `1704067200000` |
| **布尔值** | 复选框(7) | 智能识别真假值 | `"是/否"` → `true/false` |
| **分隔文本** | 多选(4) | 按分隔符拆分 | `"选项1;选项2"` → `["选项1","选项2"]` |
| **枚举选项** | 单选(3) | Excel下拉列表或明显枚举 | `"状态"列` → 单选字段 |
| **用户ID** | 人员(11) | 转为用户对象 | `"ou_xxx"` → `[{"id":"ou_xxx"}]` |
| **URL链接** | 超链接(15) | 转为链接对象 | `"https://..."` → `{"text":"...","link":"..."}` |

### 核心特性

- **增强日期检测**：支持11+种日期格式，包括中文格式，基于置信度分级推荐
- **Excel验证感知**：自动检测Excel下拉列表等数据验证，精准推荐选择类型
- **强制类型转换**：即使数据类型不匹配也能智能转换，提高同步成功率
- **详细推荐理由**：每个字段类型推荐都有详细的分析原因和置信度
- **转换统计报告**：完整的数据转换成功率和问题分析报告

### 配置示例

```bash
# 使用原值策略（完全保持原值）
python XTF.py --field-type-strategy raw

# 使用基础策略（默认推荐）
python XTF.py --field-type-strategy base

# 使用自动策略（Excel感知）
python XTF.py --field-type-strategy auto

# 使用智能策略（高级功能）
python XTF.py --field-type-strategy intelligence
```

**详细机制说明**：请参阅 [智能字段类型选择机制文档](docs/智能字段类型选择机制.md)

**策略选择建议**：
- 🛡️ **数据完整性**: `raw` 策略完全保持原值
- 🔰 **首次使用**: `base` 策略确保稳定
- 📋 **标准Excel**: `auto` 策略保持一致性
- 🧠 **高级功能**: `intelligence` 策略最大化功能

---

## 📁 项目结构

```
XTF/
├── XTF.py                  # 主入口
├── api/                    # 飞书 API 封装
├── core/                   # 配置、同步引擎、数据转换
├── lite/                   # 旧版脚本
├── config.example.yaml     # 配置示例
├── requirements.txt        # 依赖
├── docs/feishu-openapi-doc # 飞书 OpenAPI 文档库
├── logs/                   # 日志
└── README.md
```

### 架构与流程设计
架构与流程设计图
![架构与流程设计图](docs/DESIGN.md)


---

## 📝 日志与监控

- **控制台输出**：实时显示同步进度和关键信息
- **文件日志**：完整记录到 `logs/xtf_YYYYMMDD_HHMMSS.log`
- **日志级别**：DEBUG/INFO/WARNING/ERROR 可选
- **转换统计报告**：每次同步后自动生成，便于数据质量分析与优化


## ❓ 常见问题与注意事项

- **索引列设置**：同步模式下索引列（index_column）为数据比对关键，部分模式必须设置。
- **数据安全警告**：克隆/覆盖同步会清空或删除远程数据，生产环境请务必备份！
- **API 限制与配额**：批量操作、频控、重试机制均已内置，符合飞书官方限制。

- **Q: 配置文件缺少参数怎么办？**
  A: 启动时会自动提示缺失参数及命令行补全方式，可用 `config.example.yaml` 参考。

- **Q: 同步失败如何排查？**
  A: 查看控制台与 logs/ 目录下日志，关注字段类型不匹配、API 限流、网络异常等提示。

- **Q: 字段类型推荐不准确怎么办？**
  A: 可调整字段类型策略和置信度阈值，或查看详细文档调优参数。

- **Q: 如何扩展支持自定义字段类型？**
  A: 修改 `core/converter.py` 中类型推断与转换逻辑，或新增自定义转换方法。

- **Q: 如何只同步部分数据？**
  A: 可在 Excel 预处理后再同步，或在代码中增加数据过滤逻辑。

---

## 🛡️ 错误处理与稳定性

### 网格限制自动处理

XTF 会自动处理电子表格的网格限制问题：

- **智能顺序**: 先写入数据扩展网格，再设置格式
- **范围验证**: 自动检查格式化范围是否超出网格限制
- **优雅降级**: 跳过无效范围，只处理有效范围
- **详细日志**: 记录处理结果便于调试

### 数据完整性保护

- **原值策略**: `raw` 策略确保数据不会因为类型转换而丢失
- **容错机制**: 即使格式化失败，数据同步仍会成功完成
- **批量重试**: 网络错误或临时故障时自动重试
- **增量恢复**: 支持中断后的增量同步

---

## 📚 API 文档与二次开发

- 内置 [`docs/feishu-openapi-doc`](docs/feishu-openapi-doc/README.md) 为 AI 友好型飞书 OpenAPI Markdown 文档，便于查阅与扩展开发。
- 所有核心功能均模块化封装，便于二次开发与集成。
- 关键类与方法：
  - [`core/engine.py`](core/engine.py): `XTFSyncEngine` 统一同步主流程，支持四种模式。
  - [`core/converter.py`](core/converter.py): `DataConverter` 字段类型推断与强制转换。
  - [`api/bitable.py`](api/bitable.py), [`api/sheet.py`](api/sheet.py): 飞书 API 封装，支持批量操作、重试、频控。
