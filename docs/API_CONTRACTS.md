# XTF：飞书 API 对照说明

研究日期：2026-09-05。首次 API 修订基于 `XTF-20260905-reviewed-full.zip`；随后执行算法与 CI 完善基于 `XTF-20260905-api-ci-full.zip`。本文已更新为本包状态，不代表 GitHub 上其他分支。

## 1. 结论与材料范围

XTF 不是只有一个飞书接口客户端，而是四组不同协议：Bitable v1、Base v3、Sheets v2/v3，以及 Sheet AI 工具调用。不能因为都叫“飞书表格”，就共用请求结构、批量上限、身份要求和成功判断。

本次逐文件读取官方 `larksuite/cli` 的固定提交：

```text
repository: larksuite/cli
commit:     7fd6ef3c07182257ce776cdc5a614e122d5bd4b3
commit_date: 2026-09-04T10:34:52Z
```

固定提交用于说明检查的材料，不表示固定了飞书服务端版本。详细文件列表见 `contracts/larkcli-sources.json`。

**来源限制：**本次定位并尝试读取飞书开放平台对应页面，但正文在浏览工具中未完整展开；未将第三方转载、Apifox 镜像或猜测的限制冒充本次已读的官方正文。下面 Base v3 和 Sheet AI 的细节主要来自官方 CLI 的 Go 源码及其随仓库发布的技能参考文档。没有真实飞书账号调用记录，没有确认所有租户都能使用相同扩展接口。

## 2. 接口分组

| 接口组 | XTF 文件 | 核心区别 | 本次材料程度 |
|---|---|---|---|
| Bitable v1 | `api/bitable_v1.py` | `/bitable/v1/apps/...`；`records/search` 分页；记录是字段字典；batch_get 使用 `record_ids` | 审查本地实现；开放平台文档页面已定位，正文读取受限 |
| Base v3 | `api/bitable_v3.py` | `/base/v3/bases/...`；记录列表使用 offset/limit；矩阵及字段数组；创建用 `create_records`，更新用 `update_records` | 官方 CLI 源码及说明已读取 |
| Sheets v2/v3 | `api/sheet.py` | v2 值范围读写，v3 子表元数据；A1 使用物理坐标 | 审查本地实现；公开文档正文读取受限 |
| Sheet AI | `api/sheet.py::verify_formulas` | `/sheet_ai/v2/.../tools/invoke_read`；input/output 是 JSON 字符串 | 官方 CLI 工具包装和公式诊断实现已读取 |

“CLI 中有实现、开放平台页面不易找到”应称为 **CLI 来源的扩展协议**，不能直接推断为绕过认证的私有接口，也不能推断它具有公开 API 同等的稳定性承诺。

## 3. Base v3 的关键规则

### 3.1 CellValue 与 v1 不同

官方 CLI `skills/lark-base/SKILL.md` 给出的示例包括：

```json
{
  "Name": "Task A",
  "Status": ["Todo"],
  "Tags": ["A", "B"],
  "When": 1774317600000,
  "Owner": [{"id": "ou_example"}],
  "Done": false,
  "Location": {"lng": 116.397428, "lat": 39.90923},
  "Clear": null
}
```

单选也是数组；`multiple=false` 的人员/群聊字段不能提交多个 ID；日期接受带时区字符串、按 Base 时区解释的无时区字符串，以及 Unix 毫秒数字。空数组可清空数组字段。只读字段可能通过 `ignored_fields` 被过滤，不能仅看外层 code=0。

本次修订保持共享转换器的现有模型，在 Base v3 编码器中将单选标量转成数组，接纳整数毫秒；没有将 v3 的数组要求反向传播到 Bitable v1。

### 3.2 各接口分别定义批量上限

官方 CLI `record_ops.go` 的记录选择上限为 200，batch_get 的 select_fields 上限为 100；写记录上限 200。SKILL 说明列表 limit 上限为 2000。**列表一页 2000、一次读取指定记录 200、选择字段 100，是三个不同概念。** XTF 列表使用更保守的 200，不是错误，无需为了对齐数字而改大。

XTF 的 Bitable v1 客户端独立定义 batch_get=100；原服务层没有遵循它，实际写批次和读批次不相同。新增 `_read_bitable_verification` 按后端读上限与字段投影分别拆分。

### 3.3 更新响应不是存在性证明

官方 `record_batch_update.go` 明确说明：响应只有可选 ignored_fields，且不检查 record ID 是否存在。因此：

```text
请求已接受 ≠ 记录肯定存在 ≠ 值已经可读 ≠ 全部业务条件都正确
```

XTF 的 `accepted_count/accepted_units` 应继续解释为接受数，只有完成预期记录和值的读取后才能填 verified_count。默认 `verify_remote_writes=false`，默认运行不能宣称已经读回确认。

### 3.4 新建 select 字段必须保留 multiple

`field-create` 文档与 `helpers.go` 分别说明/实现 `multiple=false/true`。旧 XTF 将类型 3、4 都映射成字符串 select，却没有传 multiple。本次在字段创建请求中显式保留这一差异。

### 3.5 一致性不是即时可见

官方 Base SKILL 提醒，Table 更新可能通过异步链路生效，立即读取可能暂时看不到最新状态。本包已用 `core/verification.py` 实现有界的只读等待，覆盖记录值、删除后缺失、新字段 schema，以及后续操作依赖的状态。

执行顺序为：发送写入→只重试读取→在有限调度窗口内比较预期值→超时返回“接受了写入，但未确认结果”。记录轮询只查询尚未确认的 ID；不因读到旧值重发 create。这个窗口不是整个任务或正在执行的 HTTP 请求的硬超时。详见 `EXECUTION_AND_UAT.md`。

## 4. Sheet AI 的关键规则

`shortcuts/sheets/sheet_ai_api.go` 中的包装为：

```text
POST /open-apis/sheet_ai/v2/spreadsheets/{token}/tools/invoke_read
POST /open-apis/sheet_ai/v2/spreadsheets/{token}/tools/invoke_write

body.tool_name: 工具名
body.input: JSON 编码后的字符串，而非对象
response.data.output: JSON 编码后的字符串，需要再次解码
```

官方源码的注释将读、写分开，分别列出 `sheets:spreadsheet:read` 和 `sheets:spreadsheet:write_only`；错误地选择读写入口会失败。XTF 当前公式调用为 read，参数包含 excel_id、sheet_ids、ranges，这一实现与检查到的 CLI 包装相符。

`lark_sheet_formula_verify.go` 中 `--exit-on-error` 对 **success 与 partial 都返回 0**。因此 CI 不应只执行命令后检查退出码。应检查结构化结果：

```text
status == success
has_more == false
total_errors == 0
对于预置公式样例，total_formulas 至少达到已知数量
```

最后一个条件是 XTF CI 的额外要求：避免“扫了空表，所以没错误”。公式没有错误也不等于公式业务上正确，仍要断言已知输入的计算结果及公式表达式。XTF 的 `FormulaVerificationResult.passed` 对 partial 不放行；本包进一步将截断范围按分组、行、列二分续读。仅汇总完整的叶子范围；单格仍截断、真实公式错误或达到 128 次客户端逻辑探测上限均停止。128 不是推测的飞书服务端单元格上限。

同一个 `sheet_ai` 工具批处理还可能部分成功，成功子操作不会自动回滚。不能把 `batch_update` 当数据库事务，失败后更不能重发整个批次。本次没有为了减少请求数而把现有 Sheets 写入迁移到这个接口。

## 5. 已实际修订的缺陷

| 编号 | 缺陷与影响 | 修订位置/行为 |
|---|---|---|
| API-01 | 共享转换器生成毫秒数字，v3 编码器拒绝，日期字段真实写入链路断开 | `_encode_value` 支持整数毫秒，保留原数值 |
| API-02 | 共享转换器生成单选字符串，v3 编码器只接受数组 | v3 边界将单选标量包装为数组 |
| API-03 | 创建单选/多选字段时丢失 multiple | `create_field` 显式区分 3 与 4 |
| API-04 | 创建/更新/删除后把全部 ID 一次读回；超过 v3=200 或本地 v1=100 时写后失败 | 服务层分别按记录数和投影字段数拆分；测试覆盖两个后端和三种操作 |
| API-05 | 同一 ID 多次更新在 dict 中被覆盖，计数仍按原条数 | 发送前拒绝重复更新 ID；get/delete 也拒绝重复选择 |
| API-06 | 矩阵重名字段、重复字段 ID、重复记录 ID 被字典或分页合并掩盖 | 逐页及跨页检测重复身份；字段读取同样处理 |
| API-07 | batch_get 的 has_more=true 或缺失记录没有解释，仍返回 complete=True | 必须覆盖本次所有请求 ID，返回与缺失集合不能重叠 |
| API-08 | 单值人员/群聊字段可编码多个 ID；对象中的空 ID 可进入请求 | 编码前拒绝这两类值 |

这里的“拒绝”是本地行为，不代表远端出现了实际失败。异常响应样例是按文档规则人工构造的测试数据，**不是伪称的服务器录制记录**。

## 6. 正确但不应被重构破坏的部分

保留 v1/v3 分离，不在 v3 出错后悄悄切到 v1；保留 Sheets 物理坐标、INSERT_ROWS 与服务端实际 updatedRange；保留未知写入结果与确定失败的区别；保留 HTTP/业务重试共享总预算；保留 Sheet AI 的 JSON 字符串包装和 partial 不算通过。

这次并未宣称已独立确认 Sheets 所有字符/单元格/总表大小上限。不要照搬第三方资料中可能混淆的“5000 单元格”和“5000 行”，应在拿到对应官方正文和隔离资源后补边界实验。

## 7. 本包完成与未实测边界

已完成：全计划本地转换/编码检查；按后端独立分块的未确认记录轮询；字段创建可见性等待；
精确值比较与索引匹配分离；公式截断自动二分；真实资源测试执行器；四平台既有二进制测试后原样发布。
新增回归见 `tests/test_completion.py`、`tests/test_uat_and_release.py`，不能将这些人工构造场景当成服务器录制响应。

尚未实测：真实 Base v3 矩阵可选字段、rev、timezone 和缺失记录形状；租户对扩展接口的可用性；
真实大范围公式截断边界；人员等未列入当前 live 套件的所有字段类型；Windows/macOS/ARM 产物；
Calamine/旧 `.xls`；Ruff、Black、MyPy。本包写好了这些范围中的核心执行/CI 路径，
但没有真实账号、远端运行和全部开发工具的通过记录。

XTF 不承诺跨请求原子性、自动恢复全部写入、完整工作簿样式/公式迁移或所有 API 边界已覆盖。
已接受、读回已确认、全部业务语义正确仍是不同结论。

## 8. 一手来源

以下是核对路径；固定提交的来源清单也可机器读取。

- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/skills/lark-base/SKILL.md
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/shortcuts/base/record_ops.go
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/shortcuts/base/record_batch_update.go
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/skills/lark-base/references/lark-base-field-create.md
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/shortcuts/base/helpers.go
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/shortcuts/sheets/sheet_ai_api.go
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/shortcuts/sheets/lark_sheet_formula_verify.go
- https://github.com/larksuite/cli/blob/7fd6ef3c07182257ce776cdc5a614e122d5bd4b3/skills/lark-sheets/references/lark-sheets-formula-verify.md

已定位但本次正文读取不完整的开放平台页面：

- https://open.feishu.cn/document/server-docs/docs/bitable-v1/bitable-overview
- https://open.feishu.cn/document/server-docs/docs/bitable-v1/app-table-record/batch_get
- https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/append-data
- https://open.feishu.cn/document/server-docs/docs/sheets-v3/data-operation/write-data-to-a-single-range
