# XTF 2.0 快速开始

XTF 2.0 只提供一个正式程序：源码入口 `python3 XTF.py`，发布包入口 `XTF`
（Windows 为 `XTF.exe`）。稳定公共契约只有 CLI、YAML v2、JSON output 和退出码；
不提供旧 flat YAML、`XTF-Sheet` / `XTF-Bitable` 或稳定 Python SDK。

## 1. 查看程序

源码运行：

```bash
python3 XTF.py --version
python3 XTF.py sync --help
```

发布包运行：

```bash
./XTF --version
./XTF sync --help
```

## 2. 生成配置

发布包附带的 `config.example.yaml` 只是示例，不会被自动当作真实配置。使用命令生成
`config.yaml`：

```bash
python3 XTF.py config init --target-type bitable --output config.yaml
```

Sheet 目标：

```bash
python3 XTF.py config init --target-type sheet --output config.yaml
```

默认同步契约为 `full + by_key + ID + exact`：

```yaml
sync:
  mode: full
  match_strategy: by_key
  index:
    column: ID
    datetime_granularity: exact
    timezone: null
```

填写资源标识后，通过 `--app-secret` 或环境变量提供 secret；不要把真实凭据提交到 Git：

```bash
export XTF_APP_SECRET='your-secret'
```

## 3. 本地检查和只读预检

```bash
python3 XTF.py config validate --config config.yaml
python3 XTF.py doctor --config config.yaml
```

需要验证 Feishu 认证和目标 metadata 时，显式启用只读网络检查：

```bash
python3 XTF.py doctor --config config.yaml --network
```

`doctor --network` 只读取 metadata，不执行记录、字段、range、样式或数据验证 mutation。

## 4. 先生成计划

```bash
python3 XTF.py sync --config config.yaml --dry-run
python3 XTF.py sync --config config.yaml --dry-run --json
```

dry-run 与正式执行使用同一 planner，可以读取远端状态，但不会执行 Feishu mutation。
运行时仍可能在本机创建 `logs/`。公开 plan 是不可重放的 `PlanDocument`，不包含凭据、
记录正文、mutation payload 或 snapshot precondition。

## 5. 正式执行

非破坏性计划：

```bash
python3 XTF.py sync --config config.yaml
```

`overwrite`、`clone` 或任何包含 delete/clear action 的计划必须显式授权：

先在单独配置中设置 `sync.mode: clone` 并删除 `sync.match_strategy`，再执行：

```bash
python3 XTF.py sync --config config-clone.yaml --allow-delete
```

这只授权本次命令按计划执行，不代表生产数据操作、Release 或部署授权。

## 6. 模式与匹配策略

| `mode` | `match_strategy` | 行为 |
| --- | --- | --- |
| `full` | `by_key` | 更新同 key，创建缺失 key |
| `incremental` | `by_key` | 只创建目标中不存在的 key |
| `incremental` | `append_only` | 不读取 key、不去重，追加每条源记录 |
| `overwrite` | `by_key` | 删除匹配 key 后重新创建源记录 |
| `clone` | 省略 | replace-all；清空后重建 |

`by_key` 必须配置 `sync.index.column`。`append_only` 禁止 index 和 selective。
`clone` 必须省略 `match_strategy`，不会因为目标为空或缺少 index 被其他模式隐式触发。

## 7. XTF 1.9 到 2.0 人工迁移

| 1.9 使用方式 | 2.0 替代方式 |
| --- | --- |
| 根级 flat flags | `XTF sync` 子命令下的 flags |
| flat `config.yaml` | `XTF config init` 生成的严格 YAML v2 |
| `XTF-Sheet` | `XTF sync --target-type sheet` |
| `XTF-Bitable` | `XTF sync --target-type bitable` |
| `XTFFeishuClient` / `BitableAPI` / bool facade | 无稳定 Python 替代；调用 CLI/JSON contract |
| 无 index 的 Sheet full | 配置 `by_key + sync.index.column`，或明确选择 `clone` |

XTF 2.0 不自动迁移 1.9 配置。升级前保留旧二进制和旧配置；回滚 1.9 时恢复升级前
flat 配置，不能把 YAML v2 直接交给 1.9。

## 8. 自动化结果

`--json` 在 stdout 只输出一个 JSON document；progress、warning 和诊断写 stderr。
状态 wire value 为：

```text
success | noop | failed | partial | indeterminate
```

关键退出码：

| Code | 含义 |
| ---: | --- |
| `0` | success / noop / dry-run 成功 |
| `3` | 配置或本地输入错误 |
| `4` | 认证错误 |
| `5` | 远端资源、读取、计划或 stale snapshot |
| `6` | 已知 mutation failure / partial |
| `7` | verification failure |
| `8` | 已发送 mutation 的远端结果未知 |

自动化需要区分 `failed`、`partial` 和 `indeterminate` 时，必须同时读取 `--json` 结果，
不能只看退出码。
