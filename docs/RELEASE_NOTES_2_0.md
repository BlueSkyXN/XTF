# XTF 2.0 Release Notes（RC1 源码修订）

## 2026-09-11 收尾缺陷修复

版本仍为 `2.0.0-rc1`。下表记录本次源码修复的本地验证，不代表已发布的二进制。提交与合并状态见 [Pull requests](https://github.com/BlueSkyXN/XTF/pulls?q=is%3Apr)，对应提交的远端测试与四平台构建结果见 [GitHub Actions](https://github.com/BlueSkyXN/XTF/actions)。

- Sheet 固定写入、批量写入及清空会拒绝无效或不一致的实际范围，保留已成功批次，不误报全部成功。
- Sheet 索引规范化规则绑定到执行计划，避免数字编号在 freshness 检查时被重新推断成时间戳。
- Sheet clone 的清空范围包含工作表 ID，保持整张工作表清空；配置起点仅决定后续写入位置。
- Bitable clone 创建字段后验证原有 schema 和记录未被其他操作改变，再推进快照；覆盖 v1/v3、多字段和可见性延迟。
- Base v3 创建回执拒绝空白记录 ID，并停止后续批次。
- 配置模板先验证再原子替换，失败保留旧配置；YAML 工作表名称与整数序号不再混淆；Excel 读取失败保留输入错误退出码。
- 高级重试尊重 Retry-After 和剩余等待预算；发布脚本的 ShellCheck 引用提示已修正。
- Sheet UAT 在启动前检查整张专用工作表为空；普通更新/追加保留 A1 哨兵，clone 则要求清空该哨兵。

| 验证层 | 本地结果 |
|---|---|
| 完整非集成测试 | 859 passed，覆盖率 80% |
| Ruff、Black、MyPy、Python 编译、diff 检查 | 通过 |
| actionlint（含 ShellCheck） | 通过 |
| 真实飞书 UAT、tag、Release | 未执行 |

普通运行仍无跨 API 原子事务或成功前缀自动回滚。线上 revision 传播、租户能力及真实同步结果仍须通过隔离 UAT 验证。

## 2026-09-05 历史源码修订

以下条目与文末历史状态表保留当时的交接背景，不应作为当前工作区或 GitHub 交付状态。

- Sheet 按物理行列写入；空列、跨空行、表头乱序、起始偏移、普通新增和覆盖重建得到统一处理。
- 追加范围从已占用尾行之后开始，使用 INSERT_ROWS，后续分块跟随服务端实际范围。
- 统一严格索引的匹配值与写入类型；真实文件保留文本索引、拒绝含糊表头。
- v1/v3 解析实际响应与部分结果；发送后结果未知不伪报零写入，HTTP/业务重试共用总预算。
- 补齐双读、并发变化、认证响应和 YAML 输入的失败路径。
- 明确 pandas/Calamine 版本关系，添加 tzdata 依赖及打包收集声明；构建实际效果尚未验证。

## Breaking changes

- 正式程序收敛为 `XTF` / `XTF.exe`；停止提供 `XTF-Sheet` 和 `XTF-Bitable`。
- 所有同步参数移入 `XTF sync`；旧根级 flat invocation 删除。
- 配置只接受严格 YAML `schema_version: 2`；不提供 flat fallback 或自动迁移。
- 删除 `XTFFeishuClient`、legacy `BitableAPI`、`XTFSyncEngine`、`SyncConfig`、
  `ConfigManager` 和 `sync() -> bool` 等兼容入口。
- 不提供稳定 Python SDK。稳定公共契约只有 CLI、YAML v2、JSON output 和退出码。
- `full` / `overwrite` 必须使用 `by_key` 和显式 index；`incremental` 可选
  `by_key` / `append_only`；`clone` 必须省略 `match_strategy`。
- 不再把空目标、Sheet 无 index 或其他条件隐式转换为 clone。

## 数据正确性与安全语义

- `KeyPolicy` 在 mutation 前拒绝本地空 key、本地重复 key和远端重复 key。
- 数字 key 使用无损十进制规范化；疑似已经丢精度的大整数 `float` fail closed。
- DATETIME `exact` 使用 UTC 毫秒；`day` 要求显式 IANA timezone。
- 进程内 `ExecutionPlan` 与公开 `PlanDocument(schema_version=1)` 分离；公开 plan 不含
  mutation payload、凭据或 snapshot precondition。
- `SyncResult` 支持 `success`、`noop`、`failed`、`partial`、`indeterminate`；远端结果未知
  使用 exit `8`。
- Base revision、Bitable record ID→key 和 Sheet header/key→row/range 使用 snapshot
  freshness gate；读取不完整、状态漂移、unknown outcome 或 readback mismatch 后停止。

## Sheet

- 单一 `RangeChunker` 同时负责 A1 range、矩阵和 applied-range 计算。
- write/clear/batch update 支持行列双向分块；clear 空矩阵按块惰性生成。
- wide append 先提交 anchor band，再以服务端 actual range 写剩余列；actual range 不可证明时
  返回 `indeterminate`。
- 数据、公式保护、公式验证和配置要求的 readback 是 required；样式/自动下拉等 enrichment
  是 best effort warning。

## 配置与使用迁移

完整人工映射和命令见 [`QUICKSTART.md`](../QUICKSTART.md)。最小流程：

```bash
python3 XTF.py config init --target-type bitable --output config.yaml
python3 XTF.py config validate --config config.yaml
python3 XTF.py doctor --config config.yaml
python3 XTF.py sync --config config.yaml --dry-run --json
python3 XTF.py sync --config config.yaml
```

`overwrite` / `clone` 和任何 delete/clear plan 仍需 `--allow-delete`。真实生产 mutation 不因
Release 审批或本地测试通过而自动获得授权。

## 2026-09-05 历史完成状态

| 项目 | 本次状态 |
|---|---|
| 源码修改、原测试与新增回归 | 完成，552 passed |
| Python 语法/编译、CLI 本地演练 | 完成 |
| Ruff、Black、MyPy | 未执行，环境中未安装 |
| 当前代码的 CI 与四平台 artifact | 未执行 |
| 隔离飞书实测与 artifact 实际运行 | 未执行 |
| 正式版本、合并、tag、Release | 未执行 |

## Windows 1.9 回滚说明

沿用已确定的 best-effort 定位，不承诺旧 Windows 1.9 二进制可成功启动。不要将设置
`PYTHONIOENCODING` 写成已证实有效的恢复步骤。本次没有重新运行旧二进制；上传材料中对
历史构建或恢复实验的描述，只能按其对应的旧源码和环境理解，不能证明本次修订可发布。
