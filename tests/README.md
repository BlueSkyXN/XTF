# XTF 测试套件

测试覆盖 XTF 2.0 的 CLI/YAML/JSON 契约、不可变 runtime config、key/mode 语义、typed
plan/result、snapshot freshness、Bitable/Sheet wire contract、分块、转换、读取、重试和频控。

## 运行

安装开发依赖：

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r requirements-dev.txt
```

focused 示例：

```bash
pytest tests/test_cli_config.py tests/test_runtime_core.py -v
pytest tests/test_plan.py tests/test_service.py -v
pytest tests/test_api_bitable_v1.py tests/test_api_bitable_v3.py -v
pytest tests/test_api_sheet.py tests/test_api_sheet_typed.py -v
```

完整非集成测试与 coverage：

```bash
pytest tests/ -v -m "not integration" --tb=short \
  --cov=core --cov=api --cov=utils --cov=xtf_cli --cov-report=term
```

本地开发检查：

```bash
ruff check . --ignore E501,F401
black --check .
mypy core/ api/ utils/ xtf_cli/ --ignore-missing-imports
python3 -m py_compile XTF.py core/*.py api/*.py utils/*.py xtf_cli/*.py
```

CI 固定 Ruff `0.15.13`。不要使用本机更新版本产生的全仓新规则结果替代 CI 基线。

## 主要测试文件

| 文件 | 责任 |
| --- | --- |
| `test_cli.py` | parser、help/version、config init、usage |
| `test_cli_config.py` | YAML v2、precedence、source tracking、strict combinations |
| `test_cli_runtime.py` | dry-run、JSON/human output、exit mapping、doctor |
| `test_runtime_core.py` | frozen runtime、snapshot、bootstrap |
| `test_key_policy.py` | 空/重复/数字/DATETIME key |
| `test_plan.py` | typed action、PlanDocument、SyncResult、mode/compiler/freshness |
| `test_service.py` | planner/executor 顺序、applied prefix、verification、formula safety |
| `test_api_bitable_backend.py` | canonical Bitable contract |
| `test_api_bitable_v1.py` | Bitable v1 wire、分页、retry、receipt |
| `test_api_bitable_v3.py` | Base v3 matrix/revision/schema/batch |
| `test_api_sheet.py` | Sheet 基础 API contract |
| `test_api_sheet_typed.py` | RangeChunker、wide append、partial/unknown receipts |
| `test_api_sdk.py` | typed error、pagination、generic batch contract |
| `test_converter.py` | 字段/key 转换与类型策略 |
| `test_reader.py` | Excel/CSV 读取；CSV 仍为实验性 |
| `test_control.py` | 显式 retry/rate-limit controller |

## 测试规则

- 单元测试不得读取真实 `config.yaml`、真实凭据或真实 Feishu 资源。
- HTTP、retry sleep、rate-limit timing 和 mutation 必须 mock。
- mutation failure 必须同时断言 applied prefix 和后续 action 未执行。
- incomplete pagination、stale snapshot、unknown outcome 和 verification mismatch 必须 fail closed。
- 真实 Feishu UAT 使用隔离资源和已记录 checksum 的 RC artifact，不属于默认 pytest 路径。

## 2026-09-05 新增回归

`test_review_regressions.py` 与 `test_review_api_contracts.py` 合计 101 个场景，覆盖物理行列、
常规新增表头、索引类型、真实 Excel/CSV、混合错误重试、接口真实回执、未知写入结果、
删除后并发变化及 CLI 真实规划器 dry-run。完整套件 552 passed。

本次运行环境未安装 Ruff/Black/MyPy；它们的实际状态为“未执行”。使用
`python tools/check_project.py --format` 先格式化并运行全部检查；使用
`python tools/check_project.py --runtime-only` 运行语法、CLI 和测试子集。
