# XTF — Excel To Feishu 智能同步工具

XTF 是一个企业级数据同步工具，将本地 Excel/CSV 文件智能同步到飞书平台。支持多维表格 (Bitable) 与电子表格 (Sheet) 双目标，四种同步模式，智能字段管理与类型转换。

> 🧪 CSV 格式为实验性支持（测试阶段）。Excel (.xlsx/.xls) 为生产就绪的主要格式。

## 核心特性

- **双平台支持** — 多维表格 (Bitable) 与电子表格 (Sheet)，统一入口 `XTF.py`
- **四种同步模式** — 全量 / 增量 / 覆盖 / 克隆，覆盖全场景数据同步需求
- **智能字段类型** — Raw / Base / Auto / Intelligence 四种策略，从保守到智能逐级增强
- **选择性列同步** — 精确列级控制，只更新指定列，其他列完全不受影响
- **公式保护** — 双读检测云端公式，保护公式列不被覆盖
- **三层上传保障** — 预分块 → 自动二分重试 → 智能频控，确保大数据稳定处理
- **高级频控** — 3 种重试策略 × 3 种频控策略，9 种组合灵活配置
- **高性能读取** — Calamine 引擎（Rust 实现），Excel 读取加速 4-20x

## 快速开始

### 环境要求

- **Python 3.10+**（支持 3.10, 3.11, 3.12, 3.13）

### 安装

```bash
pip install -r requirements.txt
```

### 配置

```bash
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入飞书应用凭证和目标表格信息
```

配置优先级：**CLI 参数 > YAML 配置文件 > 智能推断 > 系统默认值**

### 运行

```bash
# 多维表格同步
python XTF.py --target-type bitable --config config.yaml

# 电子表格同步
python XTF.py --target-type sheet --config config.yaml

# 常用参数
python XTF.py --sync-mode full --index-column "ID" --batch-size 500
python XTF.py --field-type-strategy intelligence --log-level DEBUG
```

## 同步模式

| 模式 | 已存在记录 | 不存在记录 | 数据安全 |
|------|-----------|-----------|----------|
| `full` | 更新 | 新增 | ✅ 安全 |
| `incremental` | 跳过 | 新增 | ✅ 安全 |
| `overwrite` | 删除后重建 | 新增 | ⚠️ 部分删除 |
| `clone` | 全部清除 | 全部创建 | 🔴 全部清除 |

> ⚠️ overwrite/clone 会删除远程数据，生产环境请先备份！

**详细说明**：[docs/SYNC.md](docs/SYNC.md)

## 字段类型策略

| 策略 | 支持类型 | 推荐场景 |
|------|----------|----------|
| `raw` | 文本 | 数据完整性要求极高 |
| `base` | 文本 / 数字 / 日期 | ⭐ 日常使用（默认） |
| `auto` | + 单选 / 多选（Excel 验证） | 标准化 Excel 模板 |
| `intelligence` | 全部 8 种类型 | 高质量数据、进阶用户 |

```bash
python XTF.py --field-type-strategy base          # 默认推荐
python XTF.py --field-type-strategy intelligence   # 全面智能
```

**详细说明**：[docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)

## 文件格式支持

| 格式 | 扩展名 | 状态 | 读取引擎 |
|------|--------|------|----------|
| Excel 2007+ | `.xlsx` | ✅ 生产就绪 | Calamine (Rust, 4-20x 加速) → 回退 OpenPyXL |
| Excel 97-2003 | `.xls` | ✅ 生产就绪 | Calamine → 回退 OpenPyXL |
| CSV | `.csv` | 🧪 实验性 | pandas（UTF-8/GBK 自动检测） |

## 项目结构

```
XTF/
├── XTF.py                    # 统一入口
├── core/
│   ├── config.py             # 配置管理
│   ├── engine.py             # 同步引擎（四种模式、选择性同步、公式保护）
│   ├── converter.py          # 数据转换（类型分析、转换、统计报告）
│   └── control.py            # 高级控制（重试策略、频控策略）
├── api/
│   ├── auth.py               # 飞书认证
│   ├── base.py               # 基础 HTTP 客户端（重试、频控）
│   ├── bitable.py            # 多维表格 API
│   └── sheet.py              # 电子表格 API
├── utils/
│   └── excel_reader.py       # Excel/CSV 读取器
├── lite/                     # 旧版独立脚本
├── config.example.yaml       # 配置模板
├── requirements.txt          # 依赖
├── docs/                     # 详细文档
└── logs/                     # 运行日志
```

## 详细文档

| 文档 | 内容 |
|------|------|
| **[docs/README.md](docs/README.md)** | 📚 文档中心，导航与快速入门 |
| **[docs/ARCH.md](docs/ARCH.md)** | 系统架构，四层设计，组件交互 |
| **[docs/CONFIG.md](docs/CONFIG.md)** | 配置参数完整参考，CLI 映射 |
| **[docs/SYNC.md](docs/SYNC.md)** | 同步模式详解，选择性列同步 |
| **[docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)** | 字段类型策略，检测算法，转换规则 |
| **[docs/SHEET.md](docs/SHEET.md)** | 电子表格算法，分块机制，公式保护 |
| **[docs/CONTROL.md](docs/CONTROL.md)** | 高级重试与频控策略配置 |

## 常见问题

**Q: 配置文件缺少参数？**
启动时会自动提示缺失参数和命令行补全方式。首次运行自动生成示例配置。

**Q: 同步失败如何排查？**
查看 `logs/` 目录日志，关注字段类型不匹配、API 限流、网络异常等提示。使用 `--log-level DEBUG` 获取详细日志。

**Q: 大数据集处理超时？**
降低 `--batch-size`（如 100-200），增大 `--max-retries`，或启用高级频控。详见 [docs/SHEET.md](docs/SHEET.md)。

**Q: 字段类型推荐不准确？**
降级到 `base` 策略确保稳定，或调整 Intelligence 策略的置信度阈值。详见 [docs/FIELD_TYPES.md](docs/FIELD_TYPES.md)。

**Q: 如何只同步部分列？**
在 YAML 中启用选择性同步。详见 [docs/SYNC.md](docs/SYNC.md#7-选择性列同步)。

```yaml
selective_sync:
  enabled: true
  columns: ["salary", "department"]
```

## 日志

- **控制台**：实时显示同步进度
- **文件**：`logs/xtf_{target}_{YYYYMMDD_HHMMSS}.log`
- **级别**：`--log-level DEBUG|INFO|WARNING|ERROR`
- **统计报告**：每次同步自动生成转换统计

## License

[MIT](LICENSE)
