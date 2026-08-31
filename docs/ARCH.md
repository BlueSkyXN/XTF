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
  - [5.3 专用 SDK 契约](#53-专用-sdk-契约)
  - [5.4 Bitable API](#54-bitable-api)
  - [5.5 Sheet API](#55-sheet-api)
- [6. 数据处理流水线](#6-数据处理流水线)
- [7. 错误处理架构](#7-错误处理架构)
- [8. 扩展指南](#8-扩展指南)

---

## 1. 系统概览

### 1.1 系统定位

XTF 2.0 是一个 flags-first、可审计的同步 CLI，将本地 Excel/CSV 或另一张 Bitable 的数据同步到飞书 Bitable/Sheet。同步先形成进程内 `ExecutionPlan`，dry-run 只公开不可重放的 `PlanDocument`；executor 按 typed action 顺序执行并返回结构化 `SyncResult`。

### 1.2 设计哲学

| 原则 | 实现方式 |
|------|----------|
| **单一 CLI** | `XTF.py` 是薄入口，所有 parser/config/output 行为由 `xtf_cli/` 统一管理 |
| **策略模式** | 同步模式、字段类型策略、重试策略均可配置切换 |
| **渐进增强** | 从基础功能（raw/base）到高级功能（intelligence/advanced_control）逐步开启 |
| **防御式编程** | 三层上传保障、自动分块、二分重试、频控保护 |
| **Flags-first** | 全部配置都可由 CLI 覆盖，严格 YAML v2 作为可复用 preset |
| **计划与执行分离** | dry-run 与正式执行共享 planner；dry-run 可以读远端但绝不 mutation |
| **证据可见** | plan、applied prefix、verification 和 error 通过 human/JSON outcome 输出 |

### 1.3 五层架构

```
┌─────────────────────────────────────────────────────────┐
│                    入口层 (Entry)                         │
│   XTF.py — 薄入口，raise SystemExit(main())               │
├─────────────────────────────────────────────────────────┤
│                    CLI 层 (CLI)                           │
│   xtf_cli — parser、config v2、dispatch、human/JSON 输出   │
├─────────────────────────────────────────────────────────┤
│                    计划/引擎层 (Core)                     │
│   core/plan.py      — ExecutionPlan / PlanDocument / Result│
│   core/engine.py    — read-only planner + executor        │
│   core/converter.py — DataConverter 数据转换器             │
│   core/control.py   — 高级重试与频控控制器                  │
├─────────────────────────────────────────────────────────┤
│                    API 层 (API)                           │
│   api/auth.py    — 飞书认证 (tenant_access_token)         │
│   api/base.py    — RetryableAPIClient 基础客户端           │
│   api/sdk.py     — 响应、错误、分页、批处理统一契约         │
│   api/bitable.py — BitableAPI 多维表格操作                 │
│   api/sheet.py   — SheetAPI 电子表格操作                   │
└─────────────────────────────────────────────────────────┘
```

### 1.4 核心组件交互

```
XTF.py (薄入口)
  │
  └─→ xtf_cli.main(argv)
        ├─→ 单一 argparse parser
        ├─→ config v2 resolver
        │     ├─→ CLI
        │     ├─→ XTF_APP_SECRET
        │     ├─→ YAML v2
        │     └─→ target defaults
        ├─→ XTFSyncEngine.plan(...)
        │     └─→ ordered typed action（零 mutation）
        ├─→ --dry-run → PlanDocument → renderer
        └─→ execute_plan(plan)
              └─→ SyncResult → human / JSON renderer
  │
XTFSyncEngine(config)
  │     ├─→ RetryableAPIClient (共享重试、频控)
  │     ├─→ FeishuAuth(app_id, app_secret, api_client)
  │     │     └─→ tenant_access_token (自动缓存 & 刷新)
  │     │
  │     ├─→ BitableAPI(auth, api_client) 或 SheetAPI(auth, api_client)
  │     │
  │     ├─→ DataConverter(strategy, config)
  │     │     └─→ 字段类型分析 → 数据转换
  │     │
  │     └─→ AdvancedController (可选)
  │           ├─→ RetryStrategy (指数/线性/固定)
  │           └─→ RateLimitStrategy (固定/滑动窗/固定窗)
  │
  └─→ source_type
        ├─→ file → engine.sync(DataFrame)
        │            ├─→ sync_full()
        │            ├─→ sync_incremental()
        │            ├─→ sync_overwrite()
        │            └─→ sync_clone()
        └─→ bitable → engine.sync_bitable_source()
                       └─→ 按索引新增缺失记录 / 更新真实差异
```

---

## 2. CLI 与入口层

> 源码：[`XTF.py`](../XTF.py)

### 2.1 薄入口 XTF.py

`XTF.py` 不再承载配置、读取和同步分支，只负责调用 CLI main 并将返回值作为进程退出码。

1. `--help` / `--version` 无配置、网络或日志副作用。
2. `sync`、`config`、`doctor` 共用一个 parser。
3. CLI resolver 合并 flags、ENV、YAML v2 和 defaults，并记录每个值的来源。
4. planner 完成读取和分类，executor 才获得 mutation 权限。
5. renderer 分离 stdout 最终结果与 stderr 诊断。

### 2.2 配置发现与优先级

未传 `--config` 时可自动发现 `./config.yaml`；显式配置路径缺失必须失败。目标类型必须由 CLI 或 YAML 明确提供，不再静默默认到另一个远端资源。

```
app_secret: CLI > XTF_APP_SECRET > YAML
其他配置:   CLI > YAML > target-specific defaults
```

配置 resolver 是纯解析/验证逻辑，不调用 Feishu API。

### 2.3 CLI 参数体系

XTF 2.0 对每个有效 `SyncConfig` leaf 提供显式 override，并按功能分组：

| 分类 | 参数 | 说明 |
|------|------|------|
| **Auth** | `--app-id`, `--app-secret` | 应用身份；secret 也可来自 ENV |
| **Source** | `--source-type`, `--file`, `--source-app-token`, `--source-table-id` | 本地或远端数据源 |
| **源 Bitable** | `--source-app-token`, `--source-table-id` | 远端多维表格数据源 |
| **Bitable** | `--target-app-token`, `--target-table-id`, backend、字段创建 | Bitable 目标 |
| **Sheet** | `--spreadsheet-token`, `--sheet-id`, `--start-row`, `--start-column` | 电子表格专用 |
| **同步** | `--mode`, `--match-strategy`, `--index-column`, `--datetime-index-granularity`, `--datetime-index-timezone` | 同步行为控制 |
| **性能** | `--batch-size`, `--rate-limit-delay`, `--max-retries` | 性能调优 |
| **策略** | `--field-type-strategy` | 字段类型策略 |
| **Output** | `--dry-run`, `--allow-delete`, `--json`, `--quiet`, `--log-level` | 计划、安全和自动化输出 |

> 详细参数说明：[CONFIG.md](./CONFIG.md)

---

## 3. 配置层

> 源码：[`core/config.py`](../core/config.py)

### 3.1 SyncConfig 数据类

`SyncConfig` 是整个系统的运行时配置核心，使用 Python `@dataclass` 实现。它接收 CLI/ENV/YAML v2
解析后的扁平化字段；这不是用户应手写的 YAML 格式。用户配置路径以 [CONFIG.md](./CONFIG.md) 的嵌套
schema v2 为准，例如运行时 `selective_sync` 对应 YAML `sync.selective`。

```python
@dataclass
class SyncConfig:
    # 基础配置（必需）
    file_path: Optional[str]          # bitable 数据源时不需要本地文件
    app_id: str
    app_secret: str
    target_type: TargetType           # bitable | sheet

    source_type: SourceType            # file | bitable
    source_app_token: Optional[str]    # 源多维表格 Token
    source_table_id: Optional[str]     # 源数据表 ID

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
    match_strategy: MatchStrategy     # by_key | append_only；clone 省略
    index_column: Optional[str]       # 索引列名
    datetime_index_timezone: Optional[str]  # day 模式的 IANA 时区

    # 字段类型策略
    field_type_strategy: FieldTypeStrategy  # raw | base | auto | intelligence

    # 性能设置（默认值由目标类型决定；YAML v2 路径见 CONFIG.md）
    batch_size: int                   # 批处理大小
    rate_limit_delay: float           # API 调用间隔
    max_retries: int                  # 最大重试次数 (默认 3)

    # 高级控制
    enable_advanced_control: bool     # 是否启用高级重试/频控
    selective_sync: SelectiveSyncConfig  # 选择性同步配置

    # ... 更多字段见 CONFIG.md
```

### 3.2 配置优先级

```
app_secret: CLI → XTF_APP_SECRET → YAML → missing error
其他配置:   CLI → YAML → target-specific defaults
```

**目标默认示例**（以下均为运行时 `SyncConfig` 字段名，不是 YAML v2 路径）：
- Bitable 默认 `batch_size=500`，`rate_limit_delay=0.01`
- Sheet 默认 `batch_size=1000`，`rate_limit_delay=0.1`
- `sheet_protect_formulas=True` 时自动启用 `sheet_validate_results=True`
- `sheet_protect_formulas=True` 仅支持 `full`，并在配置加载时要求有效索引列

### 3.3 配置验证

`SyncConfig.__post_init__()` 执行全面验证：

| 验证项 | 规则 | 错误类型 |
|--------|------|----------|
| 目标类型 | 必须为 `bitable` 或 `sheet` | `ValueError` |
| Bitable 必填 | 目标 `app_token` + `table_id` 不能为空 | `ValueError` |
| Sheet 必填 | `spreadsheet_token` + `sheet_id` 不能为空 | `ValueError` |
| 远端源表限制 | 需要 `source_app_token` + `source_table_id` + `index_column`，只支持 `full` / `incremental` | `ValueError` |
| 匹配策略 | full/overwrite=`by_key`；incremental=`by_key` 或 `append_only`；clone 省略 | `ValueError` |
| DATETIME day | 必须配置有效 IANA timezone；exact 禁止 timezone | `ValueError` |
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
| `plan()` | `(df: Optional[DataFrame]) → ExecutionPlan` | 只读生成进程内 typed actions；禁止 mutation |
| `execute_plan()` | `(plan: ExecutionPlan) → SyncResult` | 按计划执行，首错停止并保留公开 applied prefix |
| `sync()` | `(df: DataFrame) → bool` | 主入口，根据 sync_mode 分发 |
| `sync_bitable_source()` | `() → bool` | 从源 Bitable 读取并差异写入既有目标表 |
| `sync_full()` | `(df: DataFrame) → bool` | 全量同步 |
| `sync_incremental()` | `(df: DataFrame) → bool` | 增量同步 |
| `sync_overwrite()` | `(df: DataFrame) → bool` | 覆盖同步 |
| `sync_clone()` | `(df: DataFrame) → bool` | 克隆同步 |
| `plan_fields()` | `(df: DataFrame) → List[ExecutionAction]` | 只读分析缺失字段和预测 schema |
| `ensure_fields_exist()` | `(df: DataFrame) → Tuple[bool, Dict]` | 兼容 wrapper；正式执行时创建字段 |
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

当 YAML v2 的 `control.advanced.enabled: true`（内部字段 `enable_advanced_control`）时，系统使用高级控制器替代默认的简单重试和固定延迟。

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

`FeishuAuth` 与 Bitable/Sheet 共用同一个 `RetryableAPIClient`，认证请求不会绕过
配置的重试和频控。HTTP 429/5xx 退避优先读取 `X-Ogw-Ratelimit-Reset` 或
`Retry-After`，没有服务端提示时再使用带少量 jitter 的指数退避。
transport 独占网络异常和 HTTP 429/5xx 重试；Bitable 只重试 HTTP 200 中的明确
业务错误码，避免嵌套重试放大非幂等 POST。高级 controller 耗尽后保留最终
HTTP response；完全没有 response 的网络失败会转为 typed transport error。

### 5.3 专用 SDK 契约

> 源码：[`api/sdk.py`](../api/sdk.py)

`api/sdk.py` 是 XTF 的 Python SDK facade，不依赖 `lark-cli` 进程或 Go SDK：

- `XTFFeishuClient`：以 additive facade 统一装配认证和 transport。`bitable()` 继续
  返回 Bitable v1 legacy `BitableAPI`；同步引擎改用
  `bitable_backend(backend="base_v3" | "bitable_v1")` 的 typed backend；`sheet()`
  保持现有 `SheetAPI`。既有类和构造方式继续可用。注入自定义 `api_client`
  时，`max_retries` / `rate_limit_delay` 由该 transport 自身负责。
- `FeishuResponseParser`：统一处理 Bitable/Sheet 业务响应的 HTTP 状态、飞书业务码、
  `log_id`、`retryable` 与 `retry_after`；transport 无 response 时由
  `FeishuAPIError(kind="transport")` 保留统一异常边界。认证令牌响应暂时保留既有
  异常文本契约。
- `Paginator` / `Page`：显式表达 `has_more` 与 `page_token`；缺失/重复游标、
  `data/items` 类型错误直接失败，不把不完整或畸形响应当成完整结果。
- `run_batches` / `PartialBatchError`：批次首个失败即停止，明确报告已应用前缀；
  不假设服务端会回滚成功批次。
- Bitable 的 `create/update/delete` 等布尔接口保留既有 `False` 失败契约；
  查询接口则暴露带诊断元数据的 typed exception。

`api/bitable_backend.py` 定义 canonical `FieldSchema` / `CanonicalRecord`、typed
`RecordReadResult` / `MutationReceipt` 和 Protocol；`api/bitable_v1.py` 与
`api/bitable_v3.py` 分别拥有自己的 wire schema、分页和批处理。Base v3 严格解码
matrix，并以单批 200 条为上限；任何 auth、权限、404、429、5xx、timeout、业务码或
schema 错误都不会回退到 v1。

同步模式、字段转换、公式保护和远程删除仍归 `core/engine.py`，不会下沉到 SDK。
现有 `BitableAPI` / `SheetAPI` 公共调用方式保持不变。

Sheet 元数据不可用时可以用配置化窗口做有界诊断读取，但该读取会标记为不完整；
`full` / `incremental` / `overwrite` 等依赖远端索引的路径会停止写入，避免把截断结果
误判为完整远端状态。`clone` 仍要求先取得网格属性并成功清空后才写入。
启用公式保护时只允许 `full`，且双读无法确认公式状态时停止写入；成功识别后改走
精确列写入，不再整表回写。选择性范围只合并真正相邻的目标列，不会跨过未选择列
填入空值。

### 5.4 Bitable API

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
- legacy facade 的 `bool` / `tuple` / `dict` 返回契约永久保留；typed receipt 的
  `accepted` 只表示请求被服务端接受，只有可选读回通过才表示一致

### 5.5 Sheet API

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
- `RangeChunker`：统一 A1 范围、矩阵和 `5000 × 100` 双向分块
- 顺序 batch update：logical range 拆块后首错停止，不假设服务端 range-count 上限
- wide append：anchor band 取得 actual range 后才固定写入剩余列；缺失落点返回 `indeterminate`
- 大范围 clear：每块惰性生成空矩阵，不分配整表空矩阵
- 二分重试：遇到 90227（请求过大）错误时自动减半重试
- 范围验证：自动检查和修正 A1 记法范围
- 公式识别：支持 `identify_formula_columns()` 检测公式列

---

## 6. 数据处理流水线

完整的数据同步流程（以 Full 模式为例）：

```
第1步：解析
  XTF.py → xtf_cli → CLI/ENV/YAML v2/defaults → SyncConfig + sources

第2步：读取数据
  ExcelReader → 读取 Excel/CSV → pandas DataFrame
  ├─ .xlsx/.xls: 优先使用可用的 Calamine 引擎，失败回退 OpenPyXL
  └─ .csv: UTF-8 优先，失败自动尝试 GBK

第3步：初始化引擎
  XTFSyncEngine(config) → 初始化 FeishuAuth → 初始化 API 客户端

第4步：只读规划
  ├─ 获取远程字段列表
  ├─ DataConverter 分析 DataFrame 列类型
  ├─ 生成缺失字段 action（不创建）
  ├─ 分类 create/update/delete/clear/write actions
  └─ 生成 ExecutionPlan；dry-run 仅输出 PlanDocument 后结束

第5步：计划执行
  ├─ destructive gate 检查 --allow-delete
  ├─ 按 action 顺序 mutation
  ├─ 首错停止并保留 applied prefix
  └─ 生成 SyncResult、applied prefix 与 verification

第6步：输出
  ├─ human：最终 stdout，诊断 stderr
  └─ JSON：stdout 单一文档，退出码表达结果
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
| 配置缺失/非法 | 启动时即报错；仅 `config init` 可显式生成 v2 模板 |
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
├── XTF.py                    # 薄入口：SystemExit(main())
├── xtf_cli/                  # parser、v2 resolver、dispatch、renderer、version
├── core/
│   ├── config.py             # SyncConfig 与业务组合验证
│   ├── plan.py               # ExecutionPlan / PlanDocument / SyncResult
│   ├── engine.py             # planner + executor
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
├── config.example.yaml       # 主程序严格 YAML schema v2
├── requirements.txt          # 生产依赖
├── requirements-dev.txt      # 开发依赖
├── docs/                     # 文档目录
│   └── feishu-openapi-doc/   # 飞书 OpenAPI 参考文档
└── logs/                     # 运行日志
```
