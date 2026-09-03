# XTF 2.0 架构

> 关键源码：[`XTF.py`](../XTF.py) · [`xtf_cli/`](../xtf_cli/) ·
> [`core/`](../core/) · [`api/`](../api/) · [`utils/`](../utils/)

## 1. 架构边界

XTF 2.0 是模块化单体 CLI。正式产品只有一个入口、一套配置和一条 typed 同步主干：

```text
XTF.py / XTF
    ↓
xtf_cli
    ↓
bootstrap_runtime
    ↓
SyncService
    ↓
ExecutionPlan → Executor + Verifier → SyncResult
```

稳定公共契约限定为：

```text
CLI 命令
YAML schema v2
JSON output envelope
exit code contract
```

`SyncService`、`core.*`、`api.*` 和 typed client constructor 是仓库内部接口，不构成
稳定 Python SDK。XTF 2.0 不保留旧 flat config、旧 bool facade 或旧独立二进制入口。

## 2. 模块职责

| 模块 | 职责 | 禁止事项 |
| --- | --- | --- |
| `XTF.py` | 调用 `xtf_cli.main()` 并返回进程退出码 | 不解析配置，不装配 client，不执行同步 |
| `xtf_cli/` | parser、YAML v2、precedence、source tracking、dispatch、human/JSON render、exit mapping | 不包含同步算法或 Feishu wire 细节 |
| `core/` | runtime config、key、snapshot、reconcile、mode、target compiler、plan、execute、verify | 不直接拼 Feishu HTTP 请求 |
| `api/` | auth、transport、typed error、pagination、Bitable v1/Base v3/Sheet wire contract | 不决定同步 mode 或业务 action 顺序 |
| `utils/` | side-effect-light 文件读取辅助 | 不装配远端 client，不触发网络 |

物理目录仍保持 `xtf_cli/`、`core/`、`api/`，不为形式拆微服务或建立第二条产品路径。

## 3. 启动与配置

### 3.1 CLI 解析

`xtf_cli.parser` 提供以下命令：

```text
XTF sync
XTF config init|validate|show
XTF doctor
XTF --version
```

同步参数只能出现在 `sync` 子命令下。旧根级 flat invocation 是 usage error。

### 3.2 配置解析

`xtf_cli.config` 负责：

1. 读取并严格验证 `schema_version: 2`。
2. 拒绝未知 key、错误 leaf type 和非空 inactive source/target branch。
3. 按 `CLI > YAML > target defaults` 合并普通字段。
4. 按 `CLI > XTF_APP_SECRET > YAML` 合并 `app_secret`。
5. 记录每个最终值的来源。
6. 构造 `core.runtime_config.RuntimeConfig`。

`RuntimeConfig` 是嵌套 frozen dataclass graph：

```text
RuntimeConfig
├── auth
├── source
├── target (RuntimeBitableTarget | RuntimeSheetTarget)
├── sync
│   ├── index
│   └── selective
├── conversion
├── control
│   ├── retry
│   └── rate_limit
└── output
```

secret/token 字段不出现在 dataclass repr。planner、bootstrap 和 executor 直接消费这个
不可变配置，不再通过 mutable `SyncConfig` adapter 或全局 singleton 传递运行时状态。

### 3.3 Bootstrap

`core.bootstrap.bootstrap_runtime()` 显式装配一次运行所需依赖：

```text
RuntimeConfig
  ├── Logger / handlers（每次 runtime 显式配置）
  ├── RequestController（advanced 启用时，每次 runtime 独立）
  ├── RetryableAPIClient
  ├── FeishuAuth
  └── BaseV3Backend | BitableV1Backend | SheetAPI
```

`api/` 不反向导入 `core.control`，也不读取进程全局 controller。

## 4. 单一 typed 数据流

```text
SourceReader → SourceTable → TargetInspector → TargetSnapshot
→ KeyPolicy + Reconciler → ModePolicy
→ BitablePlanCompiler / SheetPlanCompiler
→ ExecutionPlan → Executor + Verifier → SyncResult
```

### 4.1 SourceTable

本地 `DataFrame` 会先转换为不可变 `SourceTable`；后续对原 `DataFrame` 的修改不会改变
已捕获的 source。Bitable source 由 typed backend 完整读取并转换为 canonical records。

### 4.2 TargetSnapshot

`BitableSnapshot` 保留：

- backend、revision、timezone；
- canonical schema 和 records；
- `complete`、`ignored_fields`、inspected time；
- 内容 fingerprint。

`SheetSnapshot` 保留：

- 实际读取 range、grid、header；
- index key→row mapping；
- 公式/保护列；
- complete、inspected time 和内容 fingerprint。

无法证明读取完整时，不允许把已读前缀当作完整目标状态。

### 4.3 KeyPolicy 与 ModePolicy

`KeyPolicy` 集中处理空 key、重复 key、数字、文本和 DATETIME：

- 本地空 key、本地重复 key、远端重复 key在 mutation 前失败；
- by-key 模式保留远端空 key并产生 warning；
- `int`、`Decimal` 和数字文本做无损十进制规范化；
- 已经以高风险 `float` 进入程序的大整数 fail closed；
- DATETIME `exact` 使用 UTC 毫秒，naive 值按 UTC；
- DATETIME `day` 要求显式 IANA timezone。

`ModePolicy` 固定模式/策略矩阵：

| mode | strategy |
| --- | --- |
| `full` | `by_key` |
| `incremental` | `by_key` 或 `append_only` |
| `overwrite` | `by_key` |
| `clone` | 内部 `replace_all`，配置中省略 `match_strategy` |

planner 不根据目标为空、Sheet 无 index 或其他运行时条件隐式改成 clone。

## 5. ExecutionPlan 与公开 PlanDocument

内部 `ExecutionPlan` 包含：

- typed action union；
- mutation payload；
- snapshot precondition；
- verification policy；
- requested/effective mode 和配置来源。

`ExecutionPlan` 仅存在于当前进程，executor 只接受该对象。`to_public()` 生成不可重放的
`PlanDocument(schema_version=1)`；公开 action 只包含：

```text
kind
count
unit
scope
destructive
clears_values
```

公开 JSON 不包含凭据、记录正文、mutation payload、snapshot precondition 或 verification
policy。dry-run 与正式执行共享 planner，但 dry-run 永远不调用 executor。

## 6. Target-specific compiler 与执行

### 6.1 Bitable

`BitablePlanCompiler` 生成 field/record action。Base v3 和 Bitable v1 各自拥有 wire、分页、
schema decode、batch limit 和业务重试，不调用已删除的 legacy API 私有方法。

执行前的新鲜度检查：

- Base v3 比较当前 revision；
- create 再确认 key 仍不存在；
- update/delete 再确认 record ID→key 未漂移；
- schema action 比较 schema fingerprint。

每次确认成功的 mutation 用 receipt/readback 推进 expected snapshot/revision。无法推进时停止
后续 action。

### 6.2 Sheet

`SheetPlanCompiler` 生成 target-specific range/row/column action。`RangeChunker` 是 A1 range、
矩阵切片和 applied range 的唯一分块实现：

- write 最多 `5000` 行 × `100` 列；
- clear 逐块惰性创建空矩阵；
- batch update 将每个 logical range 拆成合规块并顺序提交；
- wide append 先追加不超过 `100` 列的 anchor band，再按服务端 actual range 固定写剩余列。

wide append 缺失可证明的 actual range 时，不猜测落点，返回 `indeterminate` 并停止。

Sheet action 执行前按风险重读 header、index key→row mapping 或关键 range fingerprint，防止
计划后插行、换表头或目标范围漂移。数据写入、公式保护、公式验证和配置要求的 readback
属于 required；样式和自动下拉等 enrichment 属于 best effort，只能产生 warning。

## 7. 结果与错误优先级

`SyncResult` 的 wire status 固定为：

```text
success | noop | failed | partial | indeterminate
```

判定规则：

- `failed`：没有确认成功的 mutation，发生确定性失败；
- `partial`：已有确认成功前缀，后续发生确定性 mutation 或 verification 失败；
- `indeterminate`：任一已发送 mutation 的远端结果未知，不论此前是否有成功前缀；
- best-effort warning 不覆盖已确认的数据成功状态。

每个 action receipt 保留 requested/accepted unit、actual ranges、failed batch index、readback
和 unknown scope。executor 首错停止，不把已应用前缀报告成完整成功。

CLI 将错误类别映射为 `0/1/2/3/4/5/6/7/8/130`。退出码用于故障类别；需要精确区分
`failed`、`partial`、`indeterminate` 时读取 `--json`。

## 8. Retry ownership

```text
RequestController / RateLimiter
    ↓
RetryableAPIClient：request exception、HTTP 429/5xx
    ↓
Bitable backend：HTTP 成功响应中的明确 retryable business code
```

同一个 HTTP 失败不能同时消耗 transport 和 Bitable 两套预算。缺少幂等依据且 response
丢失的 mutation 不盲目重放，而是返回 unknown outcome。

## 9. 验证边界

默认单元测试使用 mock，不依赖真实 Feishu、真实凭据、网络、`config.yaml` 或执行顺序。
本地测试、lint、typecheck 和 PyInstaller smoke 只证明代码/构建契约；不能替代：

```text
GitHub exact-head CI
artifact 下载回读
真实 Feishu mutation/readback
业务 UAT
Release/部署
```

正式版本必须使用有 checksum 的 RC artifact 在隔离 Feishu 资源上完成 UAT。生产同步、
生产 delete/clear、合并、tag 和 Release 都是独立授权层。
