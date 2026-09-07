# XTF：按 API 行为设计的 CI

2026-09-05 更新。本包已实现原方案中的执行等待、全计划编码检查、真实写入执行器和既有二进制测试后原样发布；真实服务和远端工作流尚未运行。

```text
PR ---------------------> Tests（离线、13 个 OS/Python 组合）

分支 push / 手动构建 ---> 同事件源码 Tests
                                  |
                        四个平台安装打包依赖并测试
                                  |
                          PyInstaller -> 二进制 XLSX smoke
                                  |
                         四平台 ZIP + FULL ZIP（build run ID）
                                  |
                       手动 Test Candidate and Publish
                                  |
                  准确 tag/SHA -> 下载原 ZIP -> 4 平台 × 3 live 套件
                                  |
                      全部结果、清理和文件对应关系通过
                                  |
                          发布原五个 ZIP，不重新构建

上游关键文件每周比较 ---> 差异待审阅，不自动修改实现/测试期望
```

## 1. 实施状态

| 层 | 本包文件 | 真实状态 |
|---|---|---|
| 转换/协议/服务路径 | `tests/test_upstream_contracts.py`、`test_completion.py`、原测试 | 本地 Linux / Python 3.13.5 通过 |
| CI 与 live 执行器分支 | `test_ci_contracts.py`、`test_uat_and_release.py` | 本地通过；没有实际平台/Feishu 调用 |
| 只读在线样例 | `api-live-read.yml`、`tools/probe_feishu_api.py` | 已实现，真实请求未运行 |
| 临时资源真实写入 | `api-live-write.yml`、`tools/feishu_uat.py` | 已实现，真实请求未运行 |
| 固定上游来源比较 | `api-upstream-watch.yml`、`tools/check_upstream_api.py` | 比较逻辑已测试；定时远端运行未执行 |
| 四平台构建 | `multi-platform-build.yml`、`tools/smoke_binary.py` | 源码 CLI 的 smoke 流程已运行；实际二进制未构建 |
| 同一二进制测试/发布 | `promote-release.yml`、`tools/release_artifacts.py` | 离线编排/文件测试通过；12 份真实结果及发布未执行 |

Ruff、Black、MyPy 本次不可用，不能把 YAML 中存在这些命令当成它们通过。未降低这些检查要求。

## 2. 文档如何变成测试

为关键规则准备独立有效/非法输入及异常响应；不要从被测函数生成期望。

| 规则 | 测试的关键断言 |
|---|---|
| v3 毫秒日期、单选数组、多选 multiple | 真实转换器接真实后端编码器，捕获最终请求而不只测辅助函数 |
| 独立的写入、ID 读取与字段投影上限 | 201 个 ID / 101 个字段拆分；v1 使用自身读取限制 |
| 更新 code=0 不证明 ID 存在 | confirmed_count 只有真正读到对应 ID 和值才增加 |
| 所有批次发送前预检查 | 最后一条非法，第一条写请求也不能发出；包括 clone 的前置 delete/clear |
| 写后暂时不可见 | 前两次旧值、第三次新值； mutation 次数不增加 |
| 多记录可见时间不同 | 只重读 pending ID，不能将成功前缀当作成功集合 |
| 日粒度匹配与精确时间 | 同一天不同时间可按业务规则匹配，但不能作为写入相同值 |
| Formula 与显示值不同 | 检查实际表达式与确定计算值，`=""` 不等于已清空 |
| Formula 扫描截断 | 自动二分，单格仍截断/达到有限探测数时失败，不假报通过 |
| CI 汇总 | success/failure/cancelled/skipped 的组合，必需项全部 success 才成功 |
| 发布对应关系 | 源 SHA、build run、版本、二进制摘要、完整套件和 FULL 内容都对应 |

所有人工构造协议样例都不是服务器录制记录。真实场景覆盖范围见 `EXECUTION_AND_UAT.md`。

## 3. 离线开发与 PR

```bash
python -m pip install -r requirements.txt -r requirements-dev.txt
python tools/check_project.py
python -m pytest tests/ -q -m "not integration"
```

默认只检查、不改写。`--format` 会对全项目执行 Black，不作为本次接手默认操作；必要的格式修改只处理明确文件。工具缺失退出 2，不算通过。`.local/` 是交接资料，不在测试扫描路径内。

Tests 提供 PR/manual/workflow_call 入口；push 构建通过 workflow_call 使用相同事件版本，取消独立 push Tests 与 PR build，避免重复执行完整矩阵。原 13 个 OS/Python 组合没有缩减。测试汇总要求所有必需作业 success，不接纳 skipped/cancelled。

## 4. 真实接口测试的配置

### 只读探测

`Feishu API Read Probes` 使用原 `XTF_PROBE_*` 配置：APP_ID、APP_SECRET；Base 使用 BASE、TABLE、RECORD；公式使用 SPREADSHEET、SHEET_ID、FORMULA_RANGE、MIN_FORMULAS。对应 Secret/Variable 定义以 `api-live-read.yml` 为准。它只证明样例的读取/响应可用，不能当作同步通过。

### 临时资源写入

在 GitHub 环境 `feishu-api-ci` 配置 Secret：

```text
XTF_UAT_APP_ID
XTF_UAT_APP_SECRET
XTF_UAT_BASE
XTF_UAT_SPREADSHEET
XTF_UAT_SHEET_ID
```

这组名称与 `XTF_PROBE_*` 不同。Base 临时表每次创建后删除；Sheet 预建并预留空的 `A1:T200`，标题以 `XTF_UAT_` 开头，至少 200×20。资源、命令、清理范围及故障恢复详见 `EXECUTION_AND_UAT.md`。首次应逐套手动运行 `Feishu Isolated Write UAT`。

live 写入与产物测试使用同一个 concurrency group `xtf-feishu-isolated-uat`，cancel-in-progress=false；四平台 live 矩阵 max-parallel=1。同一组资源不得另行并行使用。普通 PR 不运行带真实凭据的写入任务。

## 5. 构建与二进制 smoke

四个平台：Linux x64、Linux ARM64、Windows x64、macOS ARM64。各自按 `constraints-build.txt` 安装依赖，在打包环境先 pytest，后 PyInstaller；原打包工具固定版本保留。实际二进制 smoke 检查版本、CLI、配置生成和中文路径真实 XLSX；正常空计划必须成功，重复表头必须返回本地输入错误。

`BUILD_INFO.json` 增加源码 SHA、版本、build run ID、目标平台和二进制 SHA256，同时保留 Python、宿主平台及依赖列表。平台包包含文件摘要；缺少任一平台包不生成完整总包。不通过 `release: published` 重新构建或自动上传。

smoke 是无需 Feishu 的离线测试，不是 API 联调；本次只将这个流程跑过源码 CLI，没有实际编译产物。

## 6. 发布操作：测试后原样发布

将修改后的工作流放到默认分支，先完成 Tests 与 `Multi-Platform Build`。发布工作流 `Test Candidate and Publish` 只能从默认分支手动启动，填入：

- `build_run_id`：成功完成的指定构建 run，不是 PR run；
- `tag`：已经创建、指向该准确源码 SHA 的 `vVERSION`，例如当前源码为 `v2.0.0-rc1`。

脚本检查 run 的来源、结果、事件、workflow 路径、仓库，检查 tag/SHA/源码 VERSION 一致。下载那个 run 的原四个平台 ZIP，不编译新程序。4 个平台分别运行 base_v3、bitable_v1、sheet，共 12 份真实结果；任一缺失、失败、清理失败或二进制/SHA 不对应均不进入发布。

发布前再次读取远端 tag，确认仍指向原 SHA，读取原 FULL 包并确认它由四个已测平台包的相同内容组成。先建 draft、上传原五个 ZIP 和 `RELEASE_TESTED.json`，最后公开；RC/dev 标记为 prerelease，不设 latest。发布使用 `xtf-release` 环境，可在该环境配置人工批准。

若上传中断留下 draft，先检查该 draft 与原文件，再人工处理并重跑；脚本不使用 `--clobber` 或自动覆盖已有 Release。没有完整 12 份结果的历史包不能凭“曾经 tests passed”通过新发布流程。工作流写好不等于本次真的执行过发布。

## 7. 上游变化

`Upstream API Source Changes` 每周一 03:15 UTC，也可手动。先解析一个准确上游 SHA，再比较清单关键文件；抓取异常不是“未变化”。0=未变化，1=有差异待审阅，2=比较不完整。

清单固定官方 CLI 提交，不固定 Feishu 服务版本。仅 CLI 文件未变，不能证明服务端未变；有 diff 也不直接说明 XTF 有缺陷。不要自动更新测试期望来抹平来源变化。

## 8. 一手参考及边界

- GitHub workflow syntax：https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax
- GitHub contexts：https://docs.github.com/en/actions/reference/workflows-and-actions/contexts
- GitHub concurrency：https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency
- 跨 run artifacts：https://docs.github.com/en/actions/how-tos/writing-workflows/choosing-what-your-workflow-does/storing-and-sharing-data-from-a-workflow
- gh release create：https://cli.github.com/manual/gh_release_create
- gh release edit：https://cli.github.com/manual/gh_release_edit
- 飞书协议依据见 `API_CONTRACTS.md` 与 `contracts/larkcli-sources.json`。

当前仍需在真实开发环境安装工具并通过格式/类型检查，再运行真实服务和目标平台。无需另行设计一套笼统“验收体系”；上述脚本的真实运行结果才是对应环节的结论。
