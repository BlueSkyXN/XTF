# XTF 2.0 Release Notes（草案）

> 状态：`2.0.0-dev`。本文件不是发布公告；跨平台 RC、1.9 回滚包、真实 Feishu UAT、
> exact-head CI、合并、tag 和 GitHub Release 尚需分别取得证据。

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

## 发布门禁

正式发布前必须完成：

- XTF 1.9 精确源码的四平台可执行回滚包、flat 模板、manifest、checksum、下载回读和恢复演练；
- 单一 XTF 的 Linux x64/ARM64、Windows x64、macOS ARM64 artifact 与 checksum；
- artifact 解压、`--version`、`sync --help`、`config init` smoke；
- checksum 已记录 RC artifact 的隔离 Feishu UAT 和独立 readback；
- stale snapshot、row drift、response-lost `indeterminate` 故障注入；
- 最终 exact-head CI、artifact 回读、合并、正式版本重建、最小远端 smoke、tag 和 Release。

上述层级必须分开记录，任一层通过都不能替代下一层。
