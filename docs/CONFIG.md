# XTF 2.0 配置与 CLI 覆盖

XTF 2.0 使用严格的嵌套 YAML schema v2。`sync` 暴露所有有效配置 leaf 的命令行覆盖；`doctor` 使用同一解析规则进行本地或只读远端预检。旧 flat YAML 不再兼容，也不会自动迁移。

## 1. 配置来源与优先级

`app_secret`：

```text
CLI --app-secret > XTF_APP_SECRET > YAML auth.app_secret > 缺失错误
```

其他配置：

```text
CLI > YAML > target-specific defaults
```

- 显式传 `--config PATH` 时，文件不存在直接失败，不回退。
- 未传 `--config` 时，存在 `./config.yaml` 就自动加载，并在 human/JSON 结果中披露实际路径。
- 当前目录没有 `config.yaml` 时，可以完全使用 flags 和 `XTF_APP_SECRET`。
- `config show` 输出最终值和来源；secret、app/spreadsheet token 始终脱敏。

## 2. 配置命令

```bash
python3 XTF.py config init --target-type bitable
python3 XTF.py config init --target-type sheet --output config.yaml --force
python3 XTF.py config validate --config config.yaml
python3 XTF.py config show --config config.yaml
python3 XTF.py config show --config config.yaml --json
```

`config init` 默认输出 `config.yaml`，存在时拒绝覆盖；`sync` 不生成、不迁移也不改写配置。仓库没有 `config migrate` 命令。

`config init` 只创建带占位值的 preset，不验证 Feishu 凭据或资源可达性。填写实际资源标识后，先运行
`config validate` 和默认离线的 `doctor`；需要认证和读取字段/工作表 metadata 时，再显式运行
`doctor --network`。所有下面单独列出的 flag 都是 `XTF sync`（或 `XTF doctor` 预检）选项片段，
不能作为根命令调用。

## 3. YAML schema v2

```yaml
schema_version: 2

auth:
  app_id: "cli_your_app_id"
  app_secret: null

source:
  type: file
  file:
    path: "data.xlsx"
    sheet_name: null

target:
  type: bitable
  bitable:
    app_token: "your_app_token"
    table_id: "your_table_id"
    api_backend: base_v3
    user_id_type: open_id
    create_missing_fields: true

sync:
  mode: full
  match_strategy: by_key
  index:
    column: ID
    datetime_granularity: exact
    timezone: null
  verify_remote_writes: false
  selective:
    enabled: false
    columns: []
    auto_include_index: true
    optimize_ranges: true
    max_gap_for_merge: 2
    preserve_column_order: true

conversion:
  strategy: base
  intelligence:
    date_confidence: 0.85
    choice_confidence: 0.90
    boolean_confidence: 0.95

control:
  batch_size: 500
  rate_limit_delay: 0.01
  max_retries: 3
  advanced:
    enabled: false
    retry:
      strategy: exponential_backoff
      initial_delay: 0.5
      max_wait_time: null
      multiplier: 2.0
      increment: 0.5
    rate_limit:
      strategy: fixed_wait
      window_size: 1.0
      max_requests: 10

output:
  log_level: INFO
```

规则：

- `schema_version` 必须等于整数 `2`。
- 未知顶层、section 或 leaf key 都会失败。
- inactive source/target 分支可以省略；若提供非空且与 `type` 不匹配则失败。
- `source.type: bitable` 仅支持 `target.type: bitable`，并要求 source/target table 和索引列。
- Bitable source 只允许 `full` / `incremental`，不允许 `overwrite` / `clone`。
- `full` / `overwrite` 只允许 `by_key`；`incremental` 允许 `by_key` 或
  `append_only`；`clone` 必须省略 `match_strategy`。
- `by_key` 必须配置 `sync.index.column`。`append_only` 禁止 index 和 selective，
  并且不会读取目标 key 或执行去重。
- `sync.selective.enabled` 与 clone 互斥；columns 不允许空名或重复值。
- 公式保护只允许 Sheet full 且要求非空索引列。

## 4. Source

### 本地文件

```yaml
source:
  type: file
  file:
    path: "data.xlsx"
    sheet_name: "Sheet1"
```

```bash
--source-type file --file data.xlsx --excel-sheet Sheet1
```

`.xlsx` / `.xls` 是稳定主格式，CSV 仍为实验性支持。

### 另一张 Bitable

```yaml
source:
  type: bitable
  bitable:
    app_token: "app_source"
    table_id: "tbl_source"
```

```bash
--source-type bitable --source-app-token app_source --source-table-id tbl_source
```

源、目标字段必须同名且 backend-aware 类型兼容；系统、公式、附件和关联字段不会作为普通值复制。

## 5. Target

### Bitable

```yaml
target:
  type: bitable
  bitable:
    app_token: "app_target"
    table_id: "tbl_target"
    api_backend: base_v3
    user_id_type: open_id
    create_missing_fields: true
```

```bash
--target-type bitable --target-app-token app_target --target-table-id tbl_target
--bitable-backend base_v3 --user-id-type open_id --create-missing-fields
```

`api_backend` 仅允许 `base_v3` / `bitable_v1`，不会自动 fallback。

### Sheet

```yaml
target:
  type: sheet
  sheet:
    spreadsheet_token: "sht_target"
    sheet_id: "sheet1"
    start_row: 1
    start_column: A
    value_render_option: null
    datetime_render_option: null
    scan_max_rows: 5000
    scan_max_columns: 100
    write_max_rows: 5000
    write_max_columns: 100
    validate_results: false
    protect_formulas: false
    verify_formulas: false
    formula_max_locations: 20
    report_column_diff: false
    diff_tolerance: 0.001
```

所有有效 leaf 都有显式 CLI override；多数使用 kebab-case，同步对象歧义较大的字段使用更明确的名称，例如 `source.file.path` → `--file`、`target.bitable.app_token` → `--target-app-token`。布尔配置提供成对 flags，例如 `--sheet-protect-formulas` / `--no-sheet-protect-formulas`。

## 6. Sync、索引与选择性同步

```yaml
sync:
  mode: full
  match_strategy: by_key
  index:
    column: ID
    datetime_granularity: exact
    timezone: null
  verify_remote_writes: false
  selective:
    enabled: true
    columns: [salary, department]
```

```bash
--mode full --match-strategy by_key --index-column ID
--datetime-index-granularity exact
--verify-remote-writes --selective --column salary --column department
```

- `exact`：日期时间统一到 UTC 后使用完整 epoch milliseconds；naive Python/pandas/string 值按同一 UTC 语义解释，同日不同时间不会误匹配。
- 数字型 DATETIME 索引接受 2000-01-01 至 2100-01-01 范围内的 epoch seconds 或 milliseconds；范围外数字会 fail closed，不猜测单位。
- `day`：必须同时设置 `sync.index.timezone` / `--datetime-index-timezone`
  为有效 IANA 时区，并按该业务时区的 `YYYY-MM-DD` 匹配；同日多条记录会触发重复索引保护。
- `verify_remote_writes: false` 只表示 mutation receipt 被接受，不代表已经写后读回。
- 命令行出现任何 `--column` 时，会替换 YAML columns 并自动启用 selective sync。
- 若显式关闭 `auto_include_index`，配置的 `index.column` 必须由 `columns` 明确包含，否则在规划前失败。

## 7. Control

```yaml
control:
  batch_size: 500
  rate_limit_delay: 0.01
  max_retries: 3
  advanced:
    enabled: true
    retry:
      strategy: exponential_backoff
      initial_delay: 0.5
      max_wait_time: 30
      multiplier: 2.0
      increment: 0.5
    rate_limit:
      strategy: sliding_window
      window_size: 1.0
      max_requests: 10
```

Transport 负责网络异常与 HTTP `429/5xx`；Bitable 业务重试只处理 HTTP 200 中明确可恢复的业务码，两层预算不能相乘。

## 8. Dry-run 与删除授权

```bash
python3 XTF.py sync -c config.yaml --dry-run
python3 XTF.py sync -c config.yaml --dry-run --json
python3 XTF.py sync -c config.yaml --mode clone --allow-delete
```

`--dry-run` 可以读取 Feishu 字段、记录和 Sheet metadata，但不会创建字段、写记录、清空范围、写单元格或设置样式/验证。它使用与正式执行相同的 planner；运行时初始化仍可能创建本地日志，这不属于 Feishu mutation。

正式执行满足任一条件时必须有 `--allow-delete`：

- requested mode 是 `overwrite` / `clone`；
- planner 产生 `delete_records` / `clear_range`；

普通 `full` / `incremental` 不会因为目标为空或缺少索引而退化为 clone；非法
mode/strategy/index 组合会在零 mutation 阶段直接失败。

## 9. Exit codes

| Code | 含义 |
|---:|---|
| `0` | `success` / `noop`，或成功生成 dry-run plan |
| `1` | 未分类内部异常 |
| `2` | CLI usage 错误 |
| `3` | 配置、本地输入或输出错误 |
| `4` | 认证或权限错误 |
| `5` | 远端资源、读取、计划不完整或 stale snapshot |
| `6` | 已知 mutation failure / known partial |
| `7` | verification failure / 写后读回不一致 |
| `8` | 已发送 mutation 的远端结果未知（`indeterminate`） |
| `130` | 用户中断 |

human 最终摘要写 stdout，progress/warning/error 写 stderr。`--json` 对成功和失败都只在 stdout 输出一个 JSON document，进程退出码仍按上表返回。结果状态的 wire value 固定为 `success`、`noop`、`failed`、`partial` 或 `indeterminate`；精确状态和稳定错误码以 JSON 为准。

dry-run 的 `plan` 是 `schema_version: 1` 的公开 `PlanDocument`。每个 action 只公开 `kind`、`count`、`unit`、`scope`、`destructive` 和 `clears_values`；mutation payload、凭据、snapshot precondition 和 verification policy 只存在于进程内 `ExecutionPlan`，公开 plan 不能直接重放。
