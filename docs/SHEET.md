# XTF 电子表格算法设计

> 源码位置：[`core/service.py`](../core/service.py) · [`api/sheet.py`](../api/sheet.py)

---

## 目录

- [1. 概述](#1-概述)
- [2. Typed 分块与失败边界](#2-typed-分块与失败边界)
- [3. 智能分块策略](#3-智能分块策略)
- [4. API 接口选择与调用](#4-api-接口选择与调用)
- [5. 公式保护与双读验证](#5-公式保护与双读验证)
- [6. 网格限制处理](#6-网格限制处理)
- [7. 调优起点（非性能保证）](#7-调优起点非性能保证)
- [8. 物理布局与追加位置](#8-物理布局与追加位置)

---

## 1. 概述

电子表格 (Sheet) 同步与多维表格 (Bitable) 相比，面临独特的技术挑战：

| 挑战 | 原因 | XTF 解决方案 |
|------|------|-------------|
| **请求体积限制** | 单次 API 有体积上限 | RangeChunker 预分块 + 90227 有界拆分 |
| **行列限制** | 单次 5000 行 × 100 列 | 可配置分块参数 |
| **公式保护** | 覆盖会破坏公式 | 双读检测 + 列级保护 |
| **范围定位** | A1 记法、列号转换 | 自动范围计算与验证 |
| **网格边界** | 格式不能超出数据范围 | 先写数据 → 再设格式 |

---

## 2. Typed 分块与失败边界

### 第一层：预分块

在发送 API 请求之前，基于配置参数进行初始分块：

```
逻辑 A1 range + 矩阵
        ↓
RangeChunker 按 write_max_rows × write_max_columns 双向切片
        ↓
生成 typed RangeChunk → 顺序提交 → 累积 actual/applied ranges
```

**默认分块参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `target.sheet.write_max_rows` | `5000` | 写入分块最大行数 |
| `target.sheet.write_max_columns` | `100` | 写入分块最大列数 |

### 第二层：自动二分重试

当 API 返回错误码 `90227`（请求体过大）时，自动减半重试：

```
发送 1000 行数据 → 90227 错误
        ↓
行二分：500 行 × 2 次 → 90227 错误（仍然太大）
        ↓
继续行二分：250 行 × 4 次 → 90227 错误
        ↓
列二分：250 行 × 50 列 × 8 次 → 成功 ✓
```

**二分策略**：
1. **行优先**：先尝试减少行数
2. **列兜底**：行数减到最小仍失败时，开始减少列数
3. **最小单元**：单行单列是不可再分的最小单位

### 第三层：智能重试与频控

在确定性分块和有界拆分基础上，应用通用重试和频率控制机制：

- **默认模式**：固定延迟 + 固定重试次数
- **高级模式**：可配置指数退避/线性增长/滑动窗口等策略

> 详细配置：[CONTROL.md](./CONTROL.md)

---

## 3. 智能分块策略

### 读取分块

```
远程表格 (R 行 × C 列)
        ↓
按区域分块读取：
  ├─ 区域1: A1:CV5000 (列1-100, 行1-5000)
  ├─ 区域2: CW1:FR5000 (列101-200, 行1-5000)
  ├─ 区域3: A5001:CV10000 (列1-100, 行5001-10000)
  └─ ...
        ↓
合并为完整 DataFrame
```

**分块参数**：

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `target.sheet.scan_max_rows` | `5000` | 每块最大行数 |
| `target.sheet.scan_max_columns` | `100` | 每块最大列数 |

`target.sheet.scan_max_rows/scan_max_columns` 是单次读取分块上限，不是完整表的总扫描上限。若工作表
元数据不可用，XTF 只执行该大小的有界诊断读取并将结果标记为不完整；依赖远端
索引的 `full` / `incremental` / `overwrite` 会停止写入，而不会把截断数据当成完整表。

### 写入分块

typed 写入统一由 `RangeChunker` 计算 A1 范围、矩阵切片和行列偏移。每个请求最多
`5000` 行、`100` 列；`5001 × 101` 会被拆成四个合规块并按顺序提交。写入采用与读取类似的分块策略，但额外包含二分重试机制：

```
待写入数据 → 按(行, 列)预分块 → 逐块写入
                                   ├─ 成功 → 下一块
                                   └─ 90227 → 二分重试
```

### 清空分块

clone 模式下清空数据也需要分块（写入空值）：

```
清空范围 (R 行 × C 列)
        ↓
按 target.sheet.write_max_rows × target.sheet.write_max_columns 分块
        ↓
逐块写入空值 (batch_update)
```

空矩阵按块惰性生成；不会先为整个大范围分配一份同尺寸空矩阵。batch update 的每个
logical range 也先经过同一个 `RangeChunker`，随后逐块顺序提交并在首个失败处停止。
XTF 不把未经 OpenAPI 或真实 UAT 证明的“单请求可包含多少个 ranges”固化为公共上限。

### Wide append

超过 `100` 列的 append 先只追加第一组不超过 `100` 列的 anchor band。只有服务端返回
唯一且形状匹配的 actual range 后，XTF 才按该实际行区间对剩余列执行固定范围 write：

```text
append A:CV → actual A10:CV11
                 ↓
fixed write CW10:...11
```

actual range 缺失、重复或形状不匹配时不猜测落点，立即返回 `indeterminate` 并停止后续
列带。typed `MutationReceipt` 同时携带 `requested_count`、`accepted_count`、`unit`、
`actual_ranges`、`failed_batch_index`、`readback` 和 `unknown_scope`，因此 partial prefix
与未知落点不会被报告成完整成功。

---

## 4. API 接口选择与调用

> 源码：`api/sheet.py`

### 接口映射

| 操作 | HTTP 方法 | 端点 | 用途 |
|------|-----------|------|------|
| 获取表信息 | GET | `/sheets/{sheet_id}` | 获取行列数、标题 |
| 读取数据 | GET | `/values/{range}` | 按范围读取单元格 |
| 写入数据 | PUT | `/values` | 精确重写指定范围 |
| 追加数据 | POST | `/values_append` | 智能追加到末尾 |
| 批量更新 | POST | `/values_batch_update` | logical ranges 分块后顺序更新 |
| 设置格式 | PUT | `/styles` | 设置单元格格式 |
| 数据验证 | POST | `/dataValidation` | 创建下拉列表 |

### 各模式的接口调用流程

**Full 模式**：
```
read(索引列) → 匹配 → batch_update(更新行) + append(新行)
```

**Incremental 模式**：
```
read(索引列) → 过滤 → append(新行)
```

**Overwrite 模式**：
```
read(全部) → 过滤保留 → values PUT(重写)
```

**Clone 模式**：
```
get_info(范围) → batch_update(清空) → values PUT(全部写入)
```

---

## 5. 公式保护与双读验证

> 源码：`core/service.py` → `get_sheet_data_with_validation()`

### 双读策略

当 `target.sheet.validate_results: true` 或 `target.sheet.protect_formulas: true` 时，系统执行双读：

```
第一次读取：target.sheet.value_render_option = "Formula"
    → 获取公式文本，识别哪些列包含公式

第二次读取：target.sheet.value_render_option = "FormattedValue"
    → 获取计算后的结果值，用于差异对比
```

### 公式列识别

通过 `SheetAPI.identify_formula_columns()` 检测：
- 单元格值以 `=` 开头 → 公式列
- 整列采样判断（非单个单元格）

### 保护逻辑

```
所有列
  ├─ 公式列 → 只检测差异，不覆盖
  └─ 数据列 → 正常同步
```

当 `target.sheet.protect_formulas: true` 时，仅支持 `sync.mode: full`：
1. 自动启用 `target.sheet.validate_results: true`
2. 配置加载时要求有效索引列，用于不移动行地精确匹配
3. 双读任一步失败或无法确认公式列时停止写入
4. 从同步列表中移除公式列，数据列使用精确列 range 写入
5. 公式列仅在报告中显示差异，不通过整表写入回填计算结果

`incremental`、`overwrite`、`clone` 不支持公式保护，配置层会拒绝这种组合。

### 写后 Sheet AI 公式验证

`target.sheet.verify_formulas: true` 是独立的写后门禁，不由公式保护自动开启。
同步引擎使用 typed mutation receipt 中已成功写入的实际行区间，按
`start_column + 当前物理数据宽度（包括空表头列）` 构造不带 sheet prefix 的 A1 ranges，并调用：

```http
POST /open-apis/sheet_ai/v2/spreadsheets/{token}/tools/invoke_read
```

唯一通过条件是 `status == "success"` 且 `has_more == false`。`errors_found`、`partial`、
未知状态、非法 JSON、缺失/错误类型的 `has_more` 都会返回同步失败。append 响应缺少
actual range 时不会猜测落点或改扫全表，而是报告验证范围未知。XTF 只验证既有公式，
不会为新增行生成、复制或平移公式。

`target.sheet.validate_results` 仍保持写前 `Formula` / `FormattedValue` 双读与差异报告职责，
没有被改造成写后验证。

---

## 6. 网格限制处理

飞书电子表格的格式化操作不能超出当前网格范围（已有数据的区域）。

### 智能处理顺序

```
1. 先写入数据 → 扩展网格范围
2. 再设置格式 → 在扩展后的范围内操作
```

### 范围验证

每次格式化操作前，系统会自动：
1. 获取当前表格的行列范围
2. 检查目标范围是否在网格内
3. 超出范围的部分自动跳过（优雅降级）
4. 记录详细日志便于调试

---

## 7. 调优起点（非性能保证）

对 Sheet 目标，`config init --target-type sheet` 的初始值为 `control.batch_size: 1000`、
`control.rate_limit_delay: 0.1`，以及 `target.sheet` 的读写窗口 `5000` 行 × `100` 列。这些
是可修改的起点，不是性能基准、时延承诺或 Feishu 服务等级保证。实际吞吐会受数据形状、目标表状态、
网络、限流和 API 响应影响；应先用 dry-run 和运行结果调整。

**优化建议**：

| 场景 | 建议 |
|------|------|
| 频繁超时 | 降低 `target.sheet.write_max_rows`，增大 `control.max_retries` |
| 90227 错误频繁 | 降低 `control.batch_size` 和 Sheet 读写窗口 |
| 限流 429 | 增大 `control.rate_limit_delay`，或启用高级频控 |
| 选择性同步 | 使用 `sync.selective` 或 `--selective --column NAME` 减少同步列数 |
| 公式保护 | 启用 `target.sheet.protect_formulas` 或 `--sheet-protect-formulas` 避免不必要的覆盖 |

## 8. 物理布局与追加位置

空表头列仍占用物理列；中间空行仍占用物理行。逻辑匹配行相邻不能推出物理行相邻。
matched update 只合并真正连续的物理行，选择性更新只写实际选中的列。普通 full/incremental
新增、append_only 和只有表头的目标都按目标表头投影；完全空目标先建立表头。

append action 保存已读取目标最后占用物理行之后的起点。Sheet values_append 使用
`insertDataOption=INSERT_ROWS`；每个后续 anchor 块从上一块服务端实际结束行之后开始，
其余列带按对应实际行范围写入，不从表头或中间空位开始寻找写入位置。

overwrite 按原始物理矩阵重建，保留未匹配记录的空列及未命名单元格，不将逻辑紧凑列直接
当作真实列位置。此模式不提供公式保护，必须遵守现有模式约束。

完整原始内容进入 Sheet 指纹，空行插入也会改变快照。双读或必要的重新读取失败时停止；
写后读取不能静默接受本次实际写入范围以外的内容变化。多次远端请求并非原子事务。

这些路径已经过本地回归测试，真实 Sheet 行插入、公式和宽表行为仍需按
[隔离联调步骤](../.local/review-20260905/UAT_RUNBOOK.md) 实测。
