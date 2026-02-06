# XTF 同步模式与选择性同步

> 源码位置：[`core/engine.py`](../core/engine.py) · [`core/config.py`](../core/config.py)

---

## 目录

- [1. 同步模式概览](#1-同步模式概览)
- [2. 全量同步 (full)](#2-全量同步-full)
- [3. 增量同步 (incremental)](#3-增量同步-incremental)
- [4. 覆盖同步 (overwrite)](#4-覆盖同步-overwrite)
- [5. 克隆同步 (clone)](#5-克隆同步-clone)
- [6. 索引列机制](#6-索引列机制)
- [7. 选择性列同步](#7-选择性列同步)
- [8. API 接口选择策略](#8-api-接口选择策略)
- [9. 模式选择指南](#9-模式选择指南)

---

## 1. 同步模式概览

XTF 支持四种同步模式，通过 `sync_mode` 参数指定：

| 模式 | 已存在记录 | 不存在记录 | 数据安全 | 需要索引列 |
|------|-----------|-----------|----------|-----------|
| **full** | 更新 | 新增 | ✅ 安全 | ✅ 必需 |
| **incremental** | 跳过 | 新增 | ✅ 安全 | ✅ 必需 |
| **overwrite** | 删除后重建 | 新增 | ⚠️ 部分删除 | ✅ 必需 |
| **clone** | 全部清除 | 全部创建 | 🔴 全部清除 | ❌ 不需要 |

```bash
# 使用方式
python XTF.py --sync-mode full --index-column "ID"
python XTF.py --sync-mode clone  # 不需要索引列
```

---

## 2. 全量同步 (full)

> **行为**：已存在索引值的记录执行更新，不存在的执行新增。

### Bitable 流程

```
1. 获取远程全部记录
2. 构建索引映射：index_column 值 → record_id
3. 遍历本地 DataFrame：
   ├─ 索引存在 → 加入更新列表
   └─ 索引不存在 → 加入新增列表
4. 批量更新已有记录 (batch_update_records)
5. 批量新增新记录 (batch_create_records)
```

### Sheet 流程

```
1. 获取远程表格数据（含表头行号映射）
2. 构建索引映射：index_column 值 → 行号
3. 遍历本地 DataFrame：
   ├─ 索引存在 → 加入更新列表（记录行号）
   └─ 索引不存在 → 加入追加列表
4. 批量更新已有行 (values_batch_update)
5. 追加新行到末尾 (values_append)
```

**适用场景**：日常数据同步，既有数据更新又有新数据添加。

---

## 3. 增量同步 (incremental)

> **行为**：已存在索引值的记录跳过，只新增不存在的记录。

### Bitable 流程

```
1. 获取远程全部记录
2. 构建已有索引值集合
3. 过滤本地 DataFrame：只保留索引值不在远程的记录
4. 批量新增过滤后的记录 (batch_create_records)
```

### Sheet 流程

```
1. 获取远程索引列数据
2. 构建已有索引值集合
3. 过滤本地 DataFrame：只保留新记录
4. 追加新行到末尾 (values_append)
```

**适用场景**：只添加新数据，保护已有数据不被修改。

---

## 4. 覆盖同步 (overwrite)

> **行为**：删除远程中与本地索引匹配的记录，然后新增本地全部记录。

### Bitable 流程

```
1. 获取远程全部记录
2. 构建索引映射：index_column 值 → record_id
3. 找出本地索引值在远程中存在的记录 → 删除列表
4. 批量删除匹配记录 (batch_delete_records)
5. 批量新增本地全部记录 (batch_create_records)
```

### Sheet 流程

```
1. 获取远程表格数据
2. 构建索引映射
3. 保留不匹配的远程行 + 本地全部行
4. 重写表格范围 (values PUT)
```

**适用场景**：以本地数据为准，覆盖远程中对应的数据。

> ⚠️ **数据安全**：此模式会删除远程中与本地索引匹配的记录，生产环境请务必备份。

---

## 5. 克隆同步 (clone)

> **行为**：清空远程表格全部数据，然后新增本地全部记录。

### Bitable 流程

```
1. 获取远程全部记录
2. 批量删除全部记录 (batch_delete_records)
3. 批量新增本地全部记录 (batch_create_records)
```

### Sheet 流程

```
1. 获取远程表格信息（行列范围）
2. 清空表格数据 (values_batch_update 写入空值)
3. 写入本地全部数据 (values PUT)
```

**适用场景**：完全重建远程表格、数据迁移。

> 🔴 **数据安全**：此模式会清空远程表格的全部数据！
>
> ❌ **不支持选择性同步**：clone 与选择性同步存在逻辑冲突（克隆需要完整数据）。

---

## 6. 索引列机制

索引列 (`index_column`) 是数据比对的关键，决定了本地记录与远程记录的匹配关系。

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

- **唯一性**：建议选择唯一性强的列（如 ID、编号），重复索引值会影响匹配准确性
- **位置无关**：索引列可以是任意列，不要求在第一列
- **Bitable**：通过字段值精确匹配，支持循环检测防止无限翻页
- **Sheet**：通过列名匹配到对应列，再按值匹配到行号
- **clone 模式**：不需要索引列（因为直接清空全表）

---

## 7. 选择性列同步

> 源码：`core/engine.py` → `_get_effective_selective_columns()`, `_sync_selective_columns_sheet()`
>
> 配置：`core/config.py` → `SelectiveSyncConfig`

选择性同步允许只更新指定的列/字段，其他列/字段保持不变。

### 配置方式

```yaml
selective_sync:
  enabled: true
  columns: ["salary", "department", "last_updated"]
  auto_include_index: true         # 自动包含索引列
  optimize_ranges: true            # 合并相邻列范围（仅 Sheet）
  max_gap_for_merge: 2             # 最大合并间隔（仅 Sheet，0-50）
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
合并后范围：B:F (B 和 D 间隔 1，合并), H
API 调用数：2 次（而非 5 次）
```

**`max_gap_for_merge` 说明**：
- 值为 0：不合并，每列独立请求
- 值为 2（默认）：允许跳过 2 列的间隔进行合并
- 值越大：合并越激进，API 调用越少，但可能包含不需要的列

### 验证规则

- `columns` 必须是非空列表
- 列名不能为空字符串
- 列名不能重复
- `max_gap_for_merge` 范围 0-50

---

## 8. API 接口选择策略

不同同步模式在 Bitable 和 Sheet 下使用不同的 API 接口：

### Bitable API 映射

| 模式 | 更新操作 | 新增操作 | 删除操作 |
|------|----------|----------|----------|
| full | `batch_update_records` | `batch_create_records` | — |
| incremental | — | `batch_create_records` | — |
| overwrite | — | `batch_create_records` | `batch_delete_records` |
| clone | — | `batch_create_records` | `batch_delete_records` |

### Sheet API 映射

| 模式 | 更新操作 | 新增操作 | 清空操作 |
|------|----------|----------|----------|
| full | `POST /values_batch_update` | `POST /values_append` | — |
| incremental | — | `POST /values_append` | — |
| overwrite | `PUT /values` | — | — |
| clone | `PUT /values` | — | `POST /values_batch_update`（空值） |

> 详细算法设计：[SHEET.md](./SHEET.md)

---

## 9. 模式选择指南

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

### 组合使用建议

```bash
# 安全的日常同步
python XTF.py --sync-mode full --index-column "ID"

# 安全的增量追加
python XTF.py --sync-mode incremental --index-column "ID"

# 精确列更新
python XTF.py --sync-mode full --index-column "ID"
# + YAML: selective_sync.enabled: true, columns: ["salary"]

# 数据迁移（请先备份！）
python XTF.py --sync-mode clone
```
