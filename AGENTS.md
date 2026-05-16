# XTF root router

本仓库是 Python CLI 工具 XTF，用于把本地 Excel/CSV 数据同步到飞书 Bitable 或 Sheet。团队通常从仓库根目录启动 Codex，因此本文件是启动期规则、目录地图和按需读取路由器。

## Directory map

| Path | Responsibility | Local AGENTS.md | Read when |
| --- | --- | --- | --- |
| `XTF.py` | CLI 入口、日志初始化、配置加载、同步流程编排 | No | 修改命令行入口、运行流程、用户可见输出时 |
| `core/` | 配置模型、同步引擎、字段转换、读取和高级重试/频控 | Yes | 修改配置、同步模式、选择性同步、字段策略、分块、公式保护时 |
| `api/` | 飞书认证、HTTP 客户端、Bitable/Sheet API 封装 | Yes | 修改 token、分页、限流、重试、批量写入/删除、range 校验时 |
| `utils/` | 读取辅助工具和可选 Excel 引擎检测 | Yes | 修改 Excel/CSV 读取能力、引擎回退、文件格式支持时 |
| `tests/` | pytest 单元测试、fixtures、mock 约定 | Yes | 新增或调整测试、fixtures、coverage 范围、测试标记时 |
| `docs/` | 架构、配置、同步、字段、Sheet、频控文档 | Yes | 修改文档或需要根据文档确认行为时 |
| `docs/feishu-openapi-doc/` | 飞书 OpenAPI 外部参考资料，git submodule | Covered by `docs/AGENTS.md` | 查接口资料时；不要在其中直接写本仓库规则 |
| `lite/` | legacy 独立脚本和旧版发行入口 | Yes | 修改 `XTF_Bitable.py`、`XTF_Sheet.py` 或 legacy 构建产物时 |
| `.github/` | GitHub Actions 测试、质量检查、多平台 PyInstaller 构建 | Yes | 修改 CI、测试矩阵、artifact、release/build 行为时 |
| `logs/`, `htmlcov/`, caches | 运行日志、coverage HTML、pytest/ruff/cache 产物 | No | 默认不要修改、提交或依赖这些目录 |
| `config.example.yaml` | 可提交配置模板 | No | 修改配置字段、默认值、发行包默认配置时 |
| `config.yaml` | 本地真实配置，gitignored | No | 只用于本地手动运行；不要提交 |

## On-demand read protocol

Before editing files under a directory that has a local `AGENTS.md`, read that file first. If multiple nested `AGENTS.md` files exist on the path to the target file, read them from shallow to deep before making changes.

Subdirectory `AGENTS.md` files are navigation cards, not startup rules. They only contain local invariants, guardrails, and special validation notes. Do not assume they were loaded unless you explicitly read them in the current turn.

## Commands

All commands run from repository root.

Install runtime dependencies, requires network:

```bash
python -m pip install -r requirements.txt
```

Install test/dev dependencies, requires network:

```bash
python -m pip install -r requirements-dev.txt
```

Run unit tests, no external Feishu service required:

```bash
pytest tests/ -v
pytest tests/ -v -m "not integration" --tb=short --cov=core --cov=api --cov=utils --cov-report=term
```

CI quality checks:

```bash
ruff check . --ignore E501,F401
black --check .
mypy core/ api/ utils/ --ignore-missing-imports
python -m py_compile XTF.py core/*.py api/*.py utils/*.py
```

Legacy syntax check:

```bash
python -m py_compile lite/*.py
```

Run the app manually, requires valid `config.yaml`, Feishu credentials, network, and may write remote data:

```bash
python XTF.py --target-type bitable --config config.yaml
python XTF.py --target-type sheet --config config.yaml
```

Build is defined in `.github/workflows/multi-platform-build.yml`; there is no local build script. CI installs `pyinstaller` and builds `XTF.py`, `lite/XTF_Sheet.py`, and `lite/XTF_Bitable.py`. Local replication requires network for dependencies and platform-specific tooling. There are no migration or codegen commands in this repository.

## Global rules

- Keep architecture boundaries: `XTF.py` coordinates, `core/` owns configuration and sync logic, `api/` owns Feishu HTTP details, `utils/` owns reusable helpers, `tests/` owns validation.
- Configuration priority must remain CLI arguments > YAML config > intelligent inference > defaults.
- Treat `overwrite` and `clone` as destructive remote-data modes. Any change touching them must describe deletion risk and validation.
- Consider both `bitable` and `sheet` targets for shared behavior changes.
- `config.example.yaml` is the committed template; `config.yaml` is local and ignored.
- Prefer focused tests near the changed module. Use mocks for HTTP, retry timing, and Feishu responses.

## Do not

- Do not commit real `app_secret`, `app_token`, `spreadsheet_token`, `tenant_access_token`, local `config.yaml`, or logs.
- Do not hand-edit or commit `logs/`, `htmlcov/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, `dist/`, `build/`, or other generated/cache output.
- Do not edit `docs/feishu-openapi-doc/` as normal documentation; it is a submodule reference.
- Do not weaken validation, lint, typecheck, or test workflow settings without explaining the impact.

## Done criteria

A change is complete when the relevant local navigation card has been read, unrelated dirty files are excluded, the smallest relevant tests have run, broader CI-equivalent checks have run when the blast radius is shared, and any skipped validation is explicitly justified.
