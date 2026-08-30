# XTF 同步模式与选择性同步

> 源码位置：[`core/engine.py`](../core/engine.py) · [`core/config.py`](../core/config.py)

---

## 目录

- [1. 同步模式概览](#1-同步模式概览)
- [1.1 多维表格到多维表格的差异同步](#11-多维表格到多维表格的差异同步)
- [2. Full 模式（全量同步）](#2-full-模式全量同步)
- [3. Incremental 模式（增量同步）](#3-incremental-模式增量同步)
- [4. Overwrite 模式（覆盖同步）](#4-overwrite-模式覆盖同步)
- [5. Clone 模式（克隆同步）](#5-clone-模式克隆同步)
- [6. 模式对比](#6-模式对比)
- [7. 索引列机制](#7-索引列机制)
- [8. 选择性列同步](#8-选择性列同步)
- [9. API 接口选择策略](#9-api-接口选择策略)
- [10. 性能优化](#10-性能优化)
- [11. 模式选择指南](#11-模式选择指南)

---

## 1. 同步模式概览

XTF 支持四种同步模式，通过 YAML v2 的 `sync.mode` 或 `XTF sync --mode` 指定：

| 模式 | 说明 | 已存在记录 | 不存在记录 | 数据安全 | 需要索引列 |
|------|------|-----------|-----------|----------|-----------|
| **full** | 全量同步 | 更新 | 新增 | ⭐⭐⭐⭐⭐ 最安全 | 推荐 |
| **incremental** | 增量同步 | 跳过 | 新增 | ⭐⭐⭐⭐⭐ 最安全 | 推荐 |
| **overwrite** | 覆盖同步 | 删除后重建 | 新增 | ⚠️ 需要 `--allow-delete` | 必需 |
| **clone** | 克隆同步 | 全部清除 | 全部创建 | 🔴 需要 `--allow-delete` | 不需要 |

> **索引列说明**：索引列（`sync.index.column` / `--index-column`）用于建立匹配关系，值必须唯一。日期时间默认统一到 UTC 后按完整毫秒匹配，只有显式选择 `day` 才按本地日期匹配。

```bash
# 使用方式
python3 XTF.py sync --mode full --index-column "ID"
python3 XTF.py sync --mode clone --dry-run
python3 XTF.py sync --mode clone --allow-delete
```

---

## 1.1 多维表格到多维表格的差异同步

设置 `source.type: bitable` 后，XTF 从源表读取记录并写入既有目标表。这个能力只复制
数据，不创建新 Base，不复制视图、权限或自动化，也不会删除目标表中多余的记录。

```yaml
schema_version: 2
source:
  type: bitable
  bitable:
    app_token: "app_source"
    table_id: "tbl_source"
target:
  type: bitable
  bitable:
    app_token: "app_target"
    table_id: "tbl_target"
    api_backend: base_v3
sync:
  mode: full
  index:
    column: ID
    datetime_granularity: exact
```

执行：

```bash
python3 XTF.py sync --source-type bitable --mode full --config config.yaml
python3 XTF.py sync --source-type bitable --mode full --config config.yaml --dry-run
```

模式语义：

- `full`：新增目标缺失记录，只更新已有记录中真正变化的字段；源表没有而目标表已有的
  记录保持不变。
- `incremental`：只新增目标缺失记录；目标已存在的索引全部跳过。

源、目标记录都按索引列唯一匹配；源表索引为空、任一侧索引重复、分页读取
不完整、字段缺失或类型不兼容时，会在写入前停止。目标字段必须预先存在；公式、附件、
系统字段以及需要跨表记录 ID 映射的关联字段不作为普通数据复制。`sync.selective` 可用于
限定字段范围。该数据源明确不支持 `overwrite` / `clone`，避免把差异同步误用为破坏性重建。

---

## 2. Full 模式（全量同步）

> **行为**：对比索引列，已存在的记录更新，不存在的记录新增。远程有而本地没有的记录保持不变。

### Bitable 版本

```
┌───────────┐     ┌──────────────┐     ┌──────────────┐
│  本地Excel │ ──→ │ 获取远程记录  │ ──→ │  按索引比对   │
│   数据     │     │ (仅需要的字段)│     │              │
└───────────┘     └──────────────┘     └───┬──────┬───┘
                                           │      │
                                     ┌─────┴──┐ ┌─┴──────┐
                                     │ 匹配的  │ │ 不匹配的│
                                     │ → 更新  │ │ → 新增  │
                                     └────────┘ └────────┘
```

**API 调用流程**：
1. `search_records`（带 `field_names` 优化，仅获取必要字段）
2. 构建索引映射：index_column 值 → record_id
3. 比对索引，分类为"更新"和"新增"
4. `batch_update_records`（更新已存在的记录）
5. `batch_create_records`（创建新记录）

**特点**：
- 远程表中未匹配的记录完全不受影响
- 支持选择性列同步（`selective_sync`），选择性模式下仅获取索引列+指定列
- 无索引列时退化为纯新增操作

### Sheet 版本

**API 调用流程**：
1. 获取远程表格数据（含表头行号映射）
2. 构建索引映射：index_column 值 → 行号
3. 匹配的行 → 批量更新 (`values_batch_update`)
4. 不匹配的行 → 追加到末尾 (`values_append`)

**特点**：
- 支持结果检测（`sheet_validate_results`）和公式保护（`sheet_protect_formulas`）
- 支持选择性列同步，只更新指定列的单元格
- 无索引列时退化为克隆模式

### 适用场景

- ✅ 日常数据同步，本地数据可能有新增和修改
- ✅ 需要保留远程表中的手动编辑内容
- ✅ 对数据安全性要求最高的场景

---

## 3. Incremental 模式（增量同步）

> **行为**：只新增本地有而远程没有的记录，**不更新**已存在的记录。

```
┌───────────┐     ┌──────────────┐     ┌──────────────┐
│  本地Excel │ ──→ │ 获取远程记录  │ ──→ │  按索引比对   │
│   数据     │     │ (仅索引列)   │     │              │
└───────────┘     └──────────────┘     └───┬──────┬───┘
                                           │      │
                                     ┌─────┴──┐ ┌─┴──────┐
                                     │ 匹配的  │ │ 不匹配的│
                                     │ → 跳过  │ │ → 新增  │
                                     └────────┘ └────────┘
```

### Bitable 版本

**API 调用流程**：
1. `search_records`（带 `field_names` 仅获取索引列，大幅减少传输量）
2. 构建已有索引值集合
3. 筛选出不存在的记录
4. `batch_create_records`（仅创建新记录）

**特点**：
- 远程数据查询极快（只获取索引列）
- 已存在的记录完全不会被修改
- 无索引列时将所有本地数据作为新记录添加

### Sheet 版本

**API 调用流程**：
1. 获取远程索引列数据
2. 构建已有索引值集合
3. 过滤本地 DataFrame：只保留新记录
4. 使用 `values_append` 追加新行

**特点**：
- 支持选择性列同步
- 无索引列时追加全部本地数据
- 电子表格为空时退化为克隆模式

### 适用场景

- ✅ 追加新数据，如日志、交易记录
- ✅ 远程表有人工修改，不想被覆盖
- ✅ 数据只增不改的场景

---

## 4. Overwrite 模式（覆盖同步）

> **行为**：先删除远程表中与本地数据索引匹配的记录，然后重新写入全部本地数据。

```
┌───────────┐     ┌──────────────┐     ┌──────────────┐
│  本地Excel │ ──→ │ 获取远程记录  │ ──→ │  按索引比对   │
│   数据     │     │ (仅索引列)   │     │              │
└───────────┘     └──────────────┘     └───┬──────┬───┘
                                           │      │
                                     ┌─────┴──┐ ┌─┴──────┐
                                     │ 匹配的  │ │ 不匹配的│
                                     │ → 删除  │ │ → 保留  │
                                     └────────┘ └────────┘
                                           │
                                     ┌─────┴──────┐
                                     │ 新增全部    │
                                     │ 本地记录    │
                                     └────────────┘
```

### Bitable 版本

**API 调用流程**：
1. `search_records`（带 `field_names` 仅获取索引列）
2. 找出索引匹配的远程记录 ID
3. `batch_delete_records`（删除匹配的记录）
4. `batch_create_records`（写入全部本地记录）

**特点**：
- 远程表中未匹配的记录保持不变
- 本地数据完全覆盖匹配的远程数据
- **必须指定索引列**
- record_id 会改变

### Sheet 版本

**API 调用流程**：
1. 获取远程表格数据
2. 保留不在本地数据中的远程行 + 全部本地数据行
3. 使用 `write_sheet_data` 重写整个表格

**特点**：
- 支持选择性列覆盖同步
- 选择性模式下只覆盖指定列，其他列不变

### 适用场景

- ✅ 本地数据绝对正确，需要完全替换远程对应记录
- ✅ 修复远程数据错误
- ⚠️ **数据安全**：此模式会删除远程中与本地索引匹配的记录，生产环境请务必备份

---

## 5. Clone 模式（克隆同步）

> **行为**：**清空**远程表的全部数据，然后**完整写入**全部本地数据。

```
┌───────────┐     ┌──────────────┐     ┌──────────────┐
│  本地Excel │     │ 获取全部远程  │     │              │
│   数据     │     │ record_id    │     │  删除全部     │
└───────────┘     └──────────────┘     └──────────────┘
      │                                        │
      │           ┌──────────────┐             │
      └─────────→ │ 新增全部     │ ←───────────┘
                  │ 本地记录     │
                  └──────────────┘
```

### Bitable 版本

**API 调用流程**：
1. `search_records`（获取所有 record_id，最小字段集）
2. `batch_delete_records`（删除全部记录）
3. `batch_create_records`（写入全部本地记录）

**特点**：
- 完全重建远程表数据
- 不需要索引列
- 所有 record_id 都会改变
- **不支持选择性列同步**（逻辑冲突）

### Sheet 版本

**API 调用流程**：
1. 获取工作表网格属性（行数/列数）
2. `clear_sheet_data`（清空整个范围）
3. `write_sheet_data`（写入全部本地数据含表头）
4. 可选：应用智能字段配置（日期/数字格式、下拉列表等）

**特点**：
- 会清空整个工作表范围
- 会重写表头
- 支持写入后自动配置字段格式（`field_type_strategy`）

### 适用场景

- ✅ 完全重建/迁移数据
- ✅ 初始化远程表格
- ✅ 远程数据已损坏需要完全替换
- 🔴 **危险操作**：会丢失所有远程数据！
- ❌ **不支持选择性同步**

---

## 6. 模式对比

### Bitable 对比

| 特性 | Full | Incremental | Overwrite | Clone |
|------|------|-------------|-----------|-------|
| 获取远程记录 | 是（优化字段） | 是（仅索引列） | 是（仅索引列） | 是（最小字段集） |
| 新增记录 | ✅ | ✅ | ✅ | ✅ |
| 更新记录 | ✅ | ❌ | ❌（删后重建） | ❌（删后重建） |
| 删除记录 | ❌ | ❌ | ✅（匹配的） | ✅（全部） |
| 保留未匹配记录 | ✅ | ✅ | ✅ | ❌ |
| 需要索引列 | 推荐 | 推荐 | 必需 | 不需要 |
| 选择性同步 | ✅ | ✅ | ✅ | ❌ |
| record_id 不变 | ✅（更新的） | ✅ | ❌ | ❌ |

### Sheet 对比

| 特性 | Full | Incremental | Overwrite | Clone |
|------|------|-------------|-----------|-------|
| 读取远程数据 | 是 | 是 | 是 | 否 |
| 写入策略 | 更新+追加 | 仅追加 | 重写整表 | 清空+重写 |
| 保留未匹配行 | ✅ | ✅ | ✅ | ❌ |
| 需要索引列 | 推荐 | 推荐 | 必需 | 不需要 |
| 选择性列同步 | ✅ | ✅ | ✅ | ❌ |
| 结果检测支持 | ✅ | ❌ | ❌ | ❌ |
| 公式保护支持 | ✅ | ❌ | ❌ | ❌ |
| 智能字段配置 | ❌ | ❌ | ❌ | ✅ |

`target.sheet.protect_formulas: true` 仅允许与 `sync.mode: full` 组合；配置层会拒绝其他模式。
`full` 双读失败、公式列状态不明、缺少索引列时均停止写入，不会退化成 clone。

---

## 7. 索引列机制

索引列 (`sync.index.column` / `--index-column`) 是数据比对的关键，决定了源记录与目标记录的匹配关系。

### 工作原理

```
本地数据              远程数据
┌──────┬──────┐    ┌──────┬──────┬───────────┐
│  ID  │ Name │    │  ID  │ Name │ record_id │
├──────┼──────┤    ├──────┼──────┼───────────┤
│  1   │ 张三 │ ←→ │  1   │ 张三 │ rec_xxx   │  匹配 → 更新
│  2   │ 李四 │ ←→ │  2   │ 李四 │ rec_yyy   │  匹配 → 更新
│  3   │ 王五 │    │      │      │           │  不匹配 → 新增
└──────┴──────┘    └──────┴──────┴───────────┘
```

### 注意事项

- **唯一性**：必须选择唯一性强的列（如 ID、编号）；重复或空索引在严格路径中会停止写入
- **日期时间**：默认 `exact` 统一到 UTC 后保留完整毫秒；`day` 必须显式配置，且同日多条会触发重复保护
- **数字时间戳边界**：epoch seconds / milliseconds 仅在 2000-01-01～2100-01-01 范围内自动识别，范围外不猜测单位
- **位置无关**：索引列可以是任意列，不要求在第一列
- **Bitable**：通过字段值精确匹配，支持循环检测防止无限翻页
- **Sheet**：通过列名匹配到对应列，再按值匹配到行号
- **clone 模式**：不需要索引列（因为直接清空全表）

---

## 8. 选择性列同步

> 源码：`core/engine.py` → `_get_effective_selective_columns()`, `_sync_selective_columns_sheet()`
>
> 配置：`core/config.py` → `SelectiveSyncConfig`

选择性同步允许只更新指定的列/字段，其他列/字段保持不变。

### 配置方式

```yaml
sync:
  selective:
    enabled: true
    columns: ["salary", "department", "last_updated"]
    auto_include_index: true         # 自动包含索引列
    optimize_ranges: true            # 合并相邻列范围（仅 Sheet）
    max_gap_for_merge: 2             # 0 禁用；正数仅合并相邻目标列（仅 Sheet）
    preserve_column_order: true      # 保持原始列顺序
```

### 双平台实现差异

| 特性 | Bitable | Sheet |
|------|---------|-------|
| **控制粒度** | 字段级 | 列范围级 |
| **实现方式** | 只在记录中包含指定字段 | 按列范围精确写入 |
| **范围优化** | 不需要 | 自动合并相邻列减少 API 调用 |
| **不连续列** | 原生支持 | 通过多范围批量更新实现 |

### 与同步模式的兼容性

| 同步模式 + 选择性 | 行为 | 状态 |
|-------------------|------|------|
| **full + selective** | 更新指定列、新增指定列 | ✅ 支持 |
| **incremental + selective** | 只新增记录的指定列 | ✅ 支持 |
| **overwrite + selective** | 覆盖指定列、新增指定列 | ✅ 支持 |
| **clone + selective** | — | ❌ 不支持 |

### Sheet 范围优化

电子表格模式下，系统会将相邻列合并为连续范围以减少 API 调用：

```
原始指定列：B, D, E, F, H
              ↓ max_gap_for_merge=2
合并后范围：B, D:F, H
批量请求包含：3 个 value range（而非 5 个）
```

**`max_gap_for_merge` 说明**：
- 值为 0：不合并，每列保持独立 range
- 正数（默认 2）：启用相邻目标列合并；为兼容现有配置，允许范围仍为 1-50
- 非相邻目标列不会跨列合并，避免空值覆盖未选择的中间列

### 验证规则

- `columns` 必须是非空列表
- 列名不能为空字符串
- 列名不能重复
- `max_gap_for_merge` 范围 0-50

---

## 9. API 接口选择策略

不同同步模式在 Bitable 和 Sheet 下使用不同的 API 接口：

### Bitable API 映射

以下是同步引擎使用的 canonical 操作；实际 wire endpoint 由
`bitable_api_backend` 明确选择。默认 `base_v3` 使用 offset/matrix 与 200 条 batch，
`bitable_v1` 使用既有 page-token/client-token 契约。两者不会自动 fallback。

| 模式 | 读取操作 | 更新操作 | 新增操作 | 删除操作 |
|------|----------|----------|----------|----------|
| full | `search_records`（优化字段） | `batch_update_records` | `batch_create_records` | — |
| incremental | `search_records`（仅索引列） | — | `batch_create_records` | — |
| overwrite | `search_records`（仅索引列） | — | `batch_create_records` | `batch_delete_records` |
| clone | `search_records`（最小字段集） | — | `batch_create_records` | `batch_delete_records` |

当 `verify_remote_writes: true` 时，依赖阶段按以下顺序执行：

- `full`：update → verify update → create → verify create
- `overwrite` / `clone`：delete → verify absence → create → verify create

读取不完整、mutation partial/unknown、`ignored_fields` 或读回 mismatch 会立即停止后续
阶段。已成功批次不会自动回滚；create 响应缺少可定位 record IDs 时不会用总行数差猜测。

### Sheet API 映射

| 模式 | 更新操作 | 新增操作 | 清空操作 |
|------|----------|----------|----------|
| full | `POST /values_batch_update` | `POST /values_append` | — |
| incremental | — | `POST /values_append` | — |
| overwrite | `PUT /values` | — | — |
| clone | `PUT /values` | — | `POST /values_batch_update`（空值） |

同步引擎的数据 mutation 使用 additive typed 方法：`write_values()`、`append_values()`、
`batch_update_values()` 和 `clear_values()`。固定 range 会做矩阵 shape 预检查，分块失败时
只保留成功叶子范围；append 没有 actual range 时保持 unknown，不推断行号。原有
`write_sheet_data()` / `append_sheet_data()` / `write_selective_columns()` /
`clear_sheet_data()` 的 `bool` 以及私有 tuple 返回契约继续兼容。

append 与 Base v3 create 没有已确认幂等键，因此 transport 无响应时只发送一次并返回
`unknown_outcome`；不会由通用 transport 盲目重放。固定 range write 和已知 range 的
batch update 仍使用有界 transport policy。

`verify_remote_writes: true` 时对可证明落点的数据 mutation 做 values 读回；mismatch、
incomplete 或范围未知会阻止后续阶段。`target.sheet.verify_formulas: true` 时进一步对成功范围
执行 Sheet AI `verify_formula`；只有 `success + has_more=false` 通过。两种验证都不是事务，
不会回滚已经被服务端接受的前缀。

clear 的读回按“范围内不得出现任何非空 cell”判断；OpenAPI 将尾部空行/空列裁剪为
空矩阵也视为已清空，不要求返回与请求 range 同形状的空字符串矩阵。

> 详细算法设计：[SHEET.md](./SHEET.md)

---

## 10. 性能优化

### field_names 查询优化（Bitable）

Bitable 模式下的远程记录获取支持 `field_names` 参数优化，仅返回指定字段，大幅减少传输量：

| 模式 | 获取的字段 | 优化效果 |
|------|-----------|---------|
| `full` | 仅索引列 | 大幅减少传输量（仅用于 record_id 映射） |
| `incremental` | 仅索引列 | 大幅减少传输量 |
| `overwrite` | 仅索引列 | 大幅减少传输量 |
| `clone` | 空 `field_names`（仅 record_id） | 大幅减少传输量 |

### 频率限制

程序内嵌飞书 API 官方频率限制：

| API | 官方限制 |
|-----|---------|
| 查询记录 | 20 次/秒 |
| 新增/更新/删除 | 50 次/秒 |

网络异常和 HTTP `429/5xx` 由 transport 重试；HTTP 200 中的可恢复业务码（如
`1254290`）由 Bitable 层重试。同一个 HTTP 失败不会被两层重复放大。

> 详细频控配置：[CONTROL.md](./CONTROL.md)

---

## 11. 模式选择指南

### 按场景选择

| 场景 | 推荐模式 | 理由 |
|------|----------|------|
| 日常数据同步 | `full` | 兼顾更新和新增 |
| 每日新增数据 | `incremental` | 保护已有数据 |
| 重置部分数据 | `overwrite` | 以本地为准覆盖 |
| 初始化/迁移 | `clone` | 完全重建 |
| 只更新指定列 | `full` + selective | 精确控制 |
| 保护公式列 | `full` + formula protection | 公式不被覆盖 |

### 按安全级别选择

```
安全 ←————————————————————————————→ 危险
incremental   full   overwrite   clone
 (只新增)    (更新+新增) (部分删除)  (全部清空)
```

### 使用示例

```yaml
# 最安全的日常同步配置
sync:
  mode: full
  index:
    column: ID
    datetime_granularity: exact
```

```yaml
# 只追加新数据（日志/交易记录）
sync:
  mode: incremental
  index:
    column: 交易编号
```

```yaml
# 本地数据优先覆盖
sync:
  mode: overwrite
  index:
    column: 工号
```

```yaml
# 完全重建（正式执行还需要 --allow-delete）
sync:
  mode: clone
```

```yaml
# 只更新薪资和部门列
sync:
  mode: full
  index:
    column: 工号
  selective:
    enabled: true
    columns: ["薪资", "部门", "更新时间"]
```

```bash
# 命令行快速使用
python3 XTF.py sync --mode full --index-column "ID"
python3 XTF.py sync --mode incremental --index-column "ID"
python3 XTF.py sync --mode clone --dry-run
python3 XTF.py sync --mode clone --allow-delete
```
