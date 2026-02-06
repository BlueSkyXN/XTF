# XTF 系统架构文档

> 源码位置：[`XTF.py`](../XTF.py) · [`core/`](../core/) · [`api/`](../api/) · [`utils/`](../utils/)

---

## 目录

- [1. 系统概览](#1-系统概览)
  - [1.1 系统定位](#11-系统定位)
  - [1.2 设计哲学](#12-设计哲学)
  - [1.3 四层架构](#13-四层架构)
  - [1.4 核心组件交互](#14-核心组件交互)
- [2. 入口层](#2-入口层)
  - [2.1 统一入口 XTF.py](#21-统一入口-xtfpy)
  - [2.2 目标类型自动推断](#22-目标类型自动推断)
  - [2.3 CLI 参数体系](#23-cli-参数体系)
- [3. 配置层](#3-配置层)
  - [3.1 SyncConfig 数据类](#31-syncconfig-数据类)
  - [3.2 配置优先级](#32-配置优先级)
  - [3.3 配置验证](#33-配置验证)
- [4. 引擎层](#4-引擎层)
  - [4.1 XTFSyncEngine](#41-xtfsyncengine)
  - [4.2 DataConverter](#42-dataconverter)
  - [4.3 高级控制器](#43-高级控制器)
- [5. API 层](#5-api-层)
  - [5.1 认证模块](#51-认证模块)
  - [5.2 基础客户端](#52-基础客户端)
  - [5.3 Bitable API](#53-bitable-api)
  - [5.4 Sheet API](#54-sheet-api)
- [6. 数据处理流水线](#6-数据处理流水线)
- [7. 错误处理架构](#7-错误处理架构)
- [8. 扩展指南](#8-扩展指南)

---

## 1. 系统概览

### 1.1 系统定位

XTF (Excel To Feishu) 是一个企业级数据同步工具，将本地 Excel/CSV 文件智能同步到飞书平台。支持多维表格 (Bitable) 和电子表格 (Sheet) 两种目标，通过统一引擎实现四种同步模式。

### 1.2 设计哲学

| 原则 | 实现方式 |
|------|----------|
| **统一入口** | 单个 `XTF.py` 统一处理 Bitable 和 Sheet 两种目标 |
| **策略模式** | 同步模式、字段类型策略、重试策略均可配置切换 |
| **渐进增强** | 从基础功能（raw/base）到高级功能（intelligence/advanced_control）逐步开启 |
| **防御式编程** | 三层上传保障、自动分块、二分重试、频控保护 |
| **配置驱动** | YAML 配置 + CLI 覆盖，支持多环境灵活切换 |

### 1.3 四层架构

```
┌─────────────────────────────────────────────────────────┐
│                    入口层 (Entry)                         │
│   XTF.py — CLI 解析、配置加载、流程编排                     │
├─────────────────────────────────────────────────────────┤
│                    配置层 (Config)                        │
│   core/config.py — SyncConfig 数据类、验证、优先级合并       │
├─────────────────────────────────────────────────────────┤
│                    引擎层 (Engine)                        │
│   core/engine.py    — XTFSyncEngine 同步引擎              │
│   core/converter.py — DataConverter 数据转换器             │
│   core/control.py   — 高级重试与频控控制器                  │
├─────────────────────────────────────────────────────────┤
│                    API 层 (API)                           │
│   api/auth.py    — 飞书认证 (tenant_access_token)         │
│   api/base.py    — RetryableAPIClient 基础客户端           │
│   api/bitable.py — BitableAPI 多维表格操作                 │
│   api/sheet.py   — SheetAPI 电子表格操作                   │
└─────────────────────────────────────────────────────────┘
```

### 1.4 核心组件交互

```
XTF.py (入口)
  │
  ├─→ ConfigManager.load_config()
  │     └─→ SyncConfig (配置数据类)
  │
  ├─→ XTFSyncEngine(config)
  │     ├─→ FeishuAuth(app_id, app_secret)
  │     │     └─→ tenant_access_token (自动缓存 & 刷新)
  │     │
  │     ├─→ BitableAPI(auth) 或 SheetAPI(auth)
  │     │     └─→ RetryableAPIClient (重试、频控)
  │     │
  │     ├─→ DataConverter(strategy, config)
  │     │     └─→ 字段类型分析 → 数据转换
  │     │
  │     └─→ AdvancedController (可选)
  │           ├─→ RetryStrategy (指数/线性/固定)
  │           └─→ RateLimitStrategy (固定/滑动窗/固定窗)
  │
  └─→ engine.sync(DataFrame)
        ├─→ sync_full()
        ├─→ sync_incremental()
        ├─→ sync_overwrite()
        └─→ sync_clone()
```

---

## 2. 入口层

> 源码：[`XTF.py`](../XTF.py)

### 2.1 统一入口 XTF.py

`XTF.py` 是整个系统的唯一入口，负责：

1. **初始化日志**：创建控制台 + 文件双输出（`logs/xtf_{target}_{timestamp}.log`）
2. **打印横幅**：显示版本号、Excel 引擎信息、支持特性
3. **加载配置**：解析 CLI 参数 → 合并 YAML 配置 → 构建 `SyncConfig`
4. **读取数据**：使用 `ExcelReader` 读取 Excel/CSV 为 DataFrame
5. **执行同步**：调用 `XTFSyncEngine.sync(df)` 完成同步
6. **输出报告**：显示同步结果、耗时、飞书文档链接

### 2.2 目标类型自动推断

当用户未指定 `--target-type` 时，系统按以下顺序推断：

```
1. CLI 参数 --target-type          (最高优先级)
2. 配置文件 target_type 字段
3. 智能推断：
   ├─ 有 app_token + table_id      → bitable
   └─ 有 spreadsheet_token + sheet_id → sheet
4. 默认值：bitable                  (最低优先级)
```

> 源码：`core/config.py` → `parse_target_type()`

### 2.3 CLI 参数体系

XTF 支持 20+ CLI 参数，覆盖配置文件中的大部分选项：

| 分类 | 参数 | 说明 |
|------|------|------|
| **基础** | `--config`, `--file-path`, `--app-id`, `--app-secret`, `--target-type` | 必要连接信息 |
| **Bitable** | `--app-token`, `--table-id`, `--create-missing-fields`, `--no-create-fields` | 多维表格专用 |
| **Sheet** | `--spreadsheet-token`, `--sheet-id`, `--start-row`, `--start-column` | 电子表格专用 |
| **同步** | `--sync-mode`, `--index-column` | 同步行为控制 |
| **性能** | `--batch-size`, `--rate-limit-delay`, `--max-retries` | 性能调优 |
| **策略** | `--field-type-strategy` | 字段类型策略 |
| **日志** | `--log-level` | 日志级别 |

> 详细参数说明：[CONFIG.md](./CONFIG.md)

---

## 3. 配置层

> 源码：[`core/config.py`](../core/config.py)

### 3.1 SyncConfig 数据类

`SyncConfig` 是整个系统的配置核心，使用 Python `@dataclass` 实现：

```python
@dataclass
class SyncConfig:
    # 基础配置（必需）
    file_path: str
    app_id: str
    app_secret: str
    target_type: TargetType           # bitable | sheet

    # 多维表格配置
    app_token: Optional[str]          # 多维表格应用 Token
    table_id: Optional[str]           # 数据表 ID
    create_missing_fields: bool       # 自动创建缺失字段 (默认 True)

    # 电子表格配置
    spreadsheet_token: Optional[str]  # 电子表格 Token
    sheet_id: Optional[str]           # 工作表 ID
    start_row: int                    # 起始行 (默认 1)
    start_column: str                 # 起始列 (默认 "A")

    # 同步设置
    sync_mode: SyncMode               # full | incremental | overwrite | clone
    index_column: Optional[str]       # 索引列名

    # 字段类型策略
    field_type_strategy: FieldTypeStrategy  # raw | base | auto | intelligence

    # 性能设置
    batch_size: int                   # 批处理大小 (bitable=500, sheet=1000)
    rate_limit_delay: float           # API 间隔 (bitable=0.5s, sheet=0.1s)
    max_retries: int                  # 最大重试次数 (默认 3)

    # 高级控制
    enable_advanced_control: bool     # 是否启用高级重试/频控
    selective_sync: SelectiveSyncConfig  # 选择性同步配置

    # ... 更多字段见 CONFIG.md
```

### 3.2 配置优先级

```
CLI 参数 (最高)  →  YAML 配置文件  →  智能推断  →  系统默认值 (最低)
```

**智能推断示例**：
- Bitable 默认 `batch_size=500`，`rate_limit_delay=0.5`
- Sheet 默认 `batch_size=1000`，`rate_limit_delay=0.1`
- `sheet_protect_formulas=True` 时自动启用 `sheet_validate_results=True`

### 3.3 配置验证

`SyncConfig.__post_init__()` 执行全面验证：

| 验证项 | 规则 | 错误类型 |
|--------|------|----------|
| 目标类型 | 必须为 `bitable` 或 `sheet` | `ValueError` |
| Bitable 必填 | `app_token` + `table_id` 不能为空 | `ValueError` |
| Sheet 必填 | `spreadsheet_token` + `sheet_id` 不能为空 | `ValueError` |
| 选择性同步 | `columns` 非空列表；不含重复项；不支持 clone 模式 | `ValueError` |
| 分块参数 | `sheet_scan_max_rows/cols > 0`，`sheet_write_max_rows/cols > 0` | `ValueError` |
| 合并间隔 | `max_gap_for_merge` 范围 0-50 | `ValueError` |
| 渲染选项 | 映射到标准值（大小写不敏感） | 自动修正 |

---

## 4. 引擎层

### 4.1 XTFSyncEngine

> 源码：[`core/engine.py`](../core/engine.py)

`XTFSyncEngine` 是系统的核心调度器，统一管理 Bitable 和 Sheet 两种目标的同步逻辑。

**核心方法**：

| 方法 | 签名 | 说明 |
|------|------|------|
| `sync()` | `(df: DataFrame) → bool` | 主入口，根据 sync_mode 分发 |
| `sync_full()` | `(df: DataFrame) → bool` | 全量同步 |
| `sync_incremental()` | `(df: DataFrame) → bool` | 增量同步 |
| `sync_overwrite()` | `(df: DataFrame) → bool` | 覆盖同步 |
| `sync_clone()` | `(df: DataFrame) → bool` | 克隆同步 |
| `ensure_fields_exist()` | `(df: DataFrame) → Tuple[bool, Dict]` | 确保字段存在（Bitable） |
| `get_all_bitable_records()` | `() → List[Dict]` | 获取全部 Bitable 记录 |
| `get_current_sheet_data()` | `() → DataFrame` | 获取当前 Sheet 数据 |
| `process_in_batches()` | `(items, batch_size, func) → bool` | 通用批处理 |

**同步分发逻辑**：

```python
def sync(self, df):
    if self.config.target_type == TargetType.BITABLE:
        # Bitable 前置：确保字段存在 → 字段类型分析 → 数据转换
        self.ensure_fields_exist(df)
    # 按模式分发
    mode_map = {
        SyncMode.FULL: self.sync_full,
        SyncMode.INCREMENTAL: self.sync_incremental,
        SyncMode.OVERWRITE: self.sync_overwrite,
        SyncMode.CLONE: self.sync_clone,
    }
    return mode_map[self.config.sync_mode](df)
```

> 详细同步逻辑：[SYNC.md](./SYNC.md)

### 4.2 DataConverter

> 源码：[`core/converter.py`](../core/converter.py)

`DataConverter` 负责智能字段类型分析和数据转换，是 XTF 的数据处理核心。

**职责**：
1. **字段类型分析**：根据策略（raw/base/auto/intelligence）分析 DataFrame 列类型
2. **类型推荐**：为每个字段生成推荐类型、置信度、推荐理由
3. **数据转换**：将原始数据转换为目标类型所需的格式
4. **转换统计**：生成完整的转换成功率和问题分析报告

**支持的目标类型**（Bitable）：

| 类型 ID | 类型名称 | 适用策略 |
|---------|----------|----------|
| 1 | 文本 | raw, base, auto, intelligence |
| 2 | 数字 | base, auto, intelligence |
| 5 | 日期 | base, auto, intelligence |
| 3 | 单选 | auto, intelligence |
| 4 | 多选 | auto, intelligence |
| 7 | 复选框 | intelligence |
| 11 | 人员 | intelligence |
| 15 | 超链接 | intelligence |

> 详细策略说明：[FIELD_TYPES.md](./FIELD_TYPES.md)

### 4.3 高级控制器

> 源码：[`core/control.py`](../core/control.py)

当 `enable_advanced_control: true` 时，系统使用高级控制器替代默认的简单重试和固定延迟。

**组件架构**：

```
AdvancedController (线程安全单例)
  ├─→ RetryStrategy
  │     ├─ ExponentialBackoffStrategy  (指数退避)
  │     ├─ LinearGrowthStrategy        (线性增长)
  │     └─ FixedWaitStrategy           (固定等待)
  └─→ RateLimitStrategy
        ├─ FixedWaitStrategy           (固定等待)
        ├─ SlidingWindowStrategy       (滑动窗口)
        └─ FixedWindowStrategy         (固定窗口)
```

> 详细配置：[CONTROL.md](./CONTROL.md)

---

## 5. API 层

### 5.1 认证模块

> 源码：[`api/auth.py`](../api/auth.py)

`FeishuAuth` 管理飞书 API 的认证令牌：

- **认证方式**：使用 `app_id` + `app_secret` 获取 `tenant_access_token`
- **令牌缓存**：自动缓存令牌，过期前 5 分钟自动刷新
- **错误处理**：认证失败时抛出明确的异常信息

### 5.2 基础客户端

> 源码：[`api/base.py`](../api/base.py)

`RetryableAPIClient` 是所有 API 调用的基础层：

- **自动重试**：可配置重试次数和延迟
- **频率控制**：内置请求间隔控制
- **错误分类**：区分 429（限流）、5xx（服务器错误）等
- **日志记录**：详细的请求/响应日志

### 5.3 Bitable API

> 源码：[`api/bitable.py`](../api/bitable.py)

`BitableAPI` 封装飞书多维表格的全部操作：

| 方法 | 说明 |
|------|------|
| `list_fields()` | 获取表格字段列表 |
| `create_field()` | 创建新字段 |
| `search_records()` | 搜索/分页获取记录 |
| `batch_create_records()` | 批量创建记录 |
| `batch_update_records()` | 批量更新记录 |
| `batch_delete_records()` | 批量删除记录 |

**特性**：
- 分页获取支持循环检测（防止无限翻页）
- 批量操作自动按 `batch_size` 分片
- 富文本字段自动处理 `[{"text": "...", "type": "text"}]` 格式

### 5.4 Sheet API

> 源码：[`api/sheet.py`](../api/sheet.py)

`SheetAPI` 封装飞书电子表格的全部操作：

| 方法 | 说明 |
|------|------|
| `get_sheet_info()` | 获取工作表元信息（行列数） |
| `get_sheet_data()` | 读取指定范围数据 |
| `write_sheet_data()` | 写入指定范围数据 |
| `append_data()` | 追加数据到末尾 |
| `batch_update()` | 批量更新多个范围 |
| `set_cell_format()` | 设置单元格格式 |
| `create_data_validation()` | 创建数据验证（下拉列表） |

**特性**：
- 智能分块：超过行/列限制时自动拆分请求
- 二分重试：遇到 90227（请求过大）错误时自动减半重试
- 范围验证：自动检查和修正 A1 记法范围
- 公式识别：支持 `identify_formula_columns()` 检测公式列

---

## 6. 数据处理流水线

完整的数据同步流程（以 Full 模式为例）：

```
第1步：初始化
  XTF.py → 解析 CLI → 加载 YAML → 构建 SyncConfig → 初始化日志

第2步：读取数据
  ExcelReader → 读取 Excel/CSV → pandas DataFrame
  ├─ .xlsx/.xls: 优先 Calamine 引擎 (4-20x 加速)，失败回退 OpenPyXL
  └─ .csv: UTF-8 优先，失败自动尝试 GBK

第3步：初始化引擎
  XTFSyncEngine(config) → 初始化 FeishuAuth → 初始化 API 客户端

第4步：字段准备 (仅 Bitable)
  ├─ 获取远程字段列表
  ├─ DataConverter 分析 DataFrame 列类型
  ├─ 创建缺失字段 (如 create_missing_fields=True)
  └─ 生成字段类型映射

第5步：数据同步
  ├─ 获取远程现有数据
  ├─ 构建索引映射 (index_column → 记录ID/行号)
  ├─ 数据分类：更新 vs 新增
  ├─ 应用选择性同步过滤 (如启用)
  ├─ 应用字段类型转换
  └─ 批量执行 API 操作

第6步：结果报告
  ├─ 转换统计报告（成功率、问题字段）
  ├─ 同步统计（更新/新增/删除数量）
  ├─ 列差异报告（如启用公式保护）
  └─ 输出飞书文档链接
```

---

## 7. 错误处理架构

XTF 采用多层错误处理策略：

### 第一层：API 级别

| 错误类型 | 处理方式 |
|----------|----------|
| 认证失败 | 自动刷新 token，重试请求 |
| 429 限流 | 等待后重试，使用频控策略 |
| 5xx 服务器错误 | 指数退避重试 |
| 网络超时 | 重试至 max_retries |

### 第二层：数据级别

| 错误类型 | 处理方式 |
|----------|----------|
| 90227 请求过大 | 自动二分（行 → 列）减半重试 |
| 字段类型不匹配 | 强制转换，记录警告 |
| 数据格式异常 | 跳过并记录，不阻断整体同步 |

### 第三层：流程级别

| 错误类型 | 处理方式 |
|----------|----------|
| 配置缺失/非法 | 启动时即报错，生成示例配置 |
| 文件不存在 | 明确错误提示和路径建议 |
| 用户中断 (Ctrl+C) | 优雅退出，输出已完成部分 |

---

## 8. 扩展指南

### 添加新的同步模式

1. 在 `core/config.py` 中扩展 `SyncMode` 枚举
2. 在 `core/engine.py` 中实现 `sync_{mode_name}()` 方法
3. 分别实现 `_sync_{mode_name}_bitable()` 和 `_sync_{mode_name}_sheet()`
4. 在 `sync()` 分发逻辑中注册新模式

### 添加新的字段类型

1. 在 `core/converter.py` 中添加类型检测逻辑
2. 实现对应的数据转换方法
3. 在各策略（base/auto/intelligence）中注册

### 添加新的 API 操作

1. 在 `api/bitable.py` 或 `api/sheet.py` 中添加方法
2. 使用 `RetryableAPIClient` 基础设施（自动重试/频控）
3. 在 `core/engine.py` 中调用新 API

### 添加新的重试/频控策略

1. 在 `core/control.py` 中继承 `RetryStrategy` 或 `RateLimitStrategy`
2. 实现 `calculate_delay()` 或 `wait_if_needed()` 方法
3. 在策略工厂中注册新策略名称

---

## 文件结构总览

```
XTF/
├── XTF.py                    # 统一入口：CLI 解析、流程编排
├── core/
│   ├── config.py             # 配置管理：SyncConfig、验证、优先级
│   ├── engine.py             # 同步引擎：四种模式、选择性同步、公式保护
│   ├── converter.py          # 数据转换：类型分析、转换、统计报告
│   └── control.py            # 高级控制：重试策略、频控策略
├── api/
│   ├── auth.py               # 认证：tenant_access_token 管理
│   ├── base.py               # 基础客户端：重试、频控、日志
│   ├── bitable.py            # 多维表格 API：字段/记录 CRUD
│   └── sheet.py              # 电子表格 API：范围读写、格式化、分块
├── utils/
│   └── excel_reader.py       # Excel/CSV 读取器：Calamine 加速
├── lite/                     # 旧版独立脚本（兼容保留）
│   ├── XTF_Bitable.py
│   └── XTF_Sheet.py
├── config.example.yaml       # 配置模板
├── requirements.txt          # 生产依赖
├── requirements-dev.txt      # 开发依赖
├── docs/                     # 文档目录
│   └── feishu-openapi-doc/   # 飞书 OpenAPI 参考文档
└── logs/                     # 运行日志
```
