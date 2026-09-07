# XTF 2.0 单轨架构切换送审方案

| 项目 | 内容 |
| --- | --- |
| 文档状态 | 待审；建议原则同意，按附加条件实施 |
| 方案版本 | 1.1 |
| 编制日期 | 2026-08-31 |
| 当前稳定版本 | XTF 1.9.0 |
| 当前研发基线 | PR #19，`codex/xtf-cli-v2`，`a55c1a6` |
| 目标版本 | XTF 2.0.0 |

## 一、审议结论建议

建议对以 PR #19 为基础实施 XTF 2.0 单轨架构切换作出“原则同意，按附加条件实施”的审议结论：不回滚现有 CLI v2 成果，不从 `16c1617` 重新开发，不建设长期双轨兼容层。XTF 2.0 统一为一个 CLI、一套 YAML v2 配置、一条 typed 同步主干、一套结构化结果协议和一个正式构建产物。

本方案所称“一次性切换”是指正式产品不提供 legacy 双轨，`main` 最终只保留一条 XTF 2.0 产品路径；研发过程仍按可独立验证、可独立审查的阶段逐步迁移，不采用一次性重写或单个巨型提交。简言之：**对外一次性切换，对内分阶段迁移。**

本方案属于明确的 breaking change。批准本方案即表示接受旧命令、旧 flat YAML、legacy 独立脚本、legacy 二进制和旧 Python 调用方式不再兼容；同时要求在正式发布前完成数据正确性、Sheet 边界、snapshot precondition、跨平台构建和真实 Feishu UAT。

建议审议结论为：

> **原则同意 XTF 2.0 单轨切换方案，按附加条件实施。以数据正确性和真实交付证据作为发布门槛，不以兼容旧入口作为实施约束。**

## 二、提请审议的事项

| 编号 | 审议事项 | 建议决定 |
| --- | --- | --- |
| D-01 | 是否以 PR #19 作为 XTF 2.0 集成基础 | 同意；不回滚，不从 `16c1617` 重做 |
| D-02 | 是否接受 XTF 2.0 breaking cutover | 同意；不保留旧命令、旧配置和旧 Python API 兼容承诺 |
| D-03 | 是否停止 legacy 独立分发 | 同意；停止发布 `XTF-Sheet`、`XTF-Bitable`，删除 `lite/` |
| D-04 | 是否只保留严格 YAML v2 | 同意；不提供 `config migrate`、flat YAML fallback 或双配置读取 |
| D-05 | 是否统一到 typed 同步主干 | 同意；新 CLI、planner、executor、Bitable 和 Sheet 均走 typed contract |
| D-06 | 是否删除旧 Engine/facade 路径 | 同意；新主干切换完成后删除 `XTFSyncEngine`、`sync() -> bool` 和 legacy shim |
| D-07 | 是否将真实 Feishu UAT 设为正式发布门槛 | 同意；自动测试和构建不能替代远端 mutation/readback |
| D-08 | 是否保持 `main` 为 1.9 稳定线直至最终切换 | 同意；XTF 2.0 在 PR #19 集成线完成后一次性进入 `main` |
| D-09 | XTF 2.0 是否提供稳定 Python SDK | 不提供；稳定公共接口限定为 CLI、YAML v2、JSON output 和退出码 |
| D-10 | 是否在删除 legacy 构建前归档 1.9 回滚包 | 同意；必须完成可执行回滚包、旧配置模板、checksum 和下载回读 |

## 三、背景与当前事实

### 3.1 当前代码与交付状态

- 当前稳定 Release 为 `1.9.0`。
- 当前研发分支为 `codex/xtf-cli-v2`，精确 head 为 `a55c1a6de5040b9379ce18897d767650acd6ee60`。
- PR #19 当前为 Open、非 Draft、`MERGEABLE/CLEAN`，尚无 review approval，尚未合并。
- PR #19 当前标题和正文仍承诺保留 legacy binary、flat 配置模板、直接 client 构造和 bool 兼容入口，与本方案拟议的新边界不一致；方案获批后必须重置 PR 送审口径。
- PR #19 相对 `main` 包含 6 个提交、36 个文件，约 `+6632/-1428`。
- 当前代码已经建立 XTF 2.0 CLI、严格 YAML v2、`SyncPlan`、`PlanAction`、`SyncOutcome`、`--dry-run`、`--allow-delete`、JSON 输出和稳定退出码基础。
- exact-head GitHub Tests 已通过；本地记录为 483 个非集成测试通过，总覆盖率 67%。
- exact-head Multi-Platform Build 已通过主 `XTF` 和两个 legacy binary 的现有构建矩阵；`publish-release` 未执行。
- Release `1.9.0` 当前没有二进制 assets，只有 tag 和源码归档，尚不具备可直接取用的可执行回滚包。
- 尚未执行真实 Feishu mutation、在线 readback、业务 UAT、正式 Release 或部署。

### 3.2 已确认的主要缺口

1. file source 的空 key 可能被当作新数据重复创建。
2. 数字 key 当前经过 `float()`，超过 `2^53` 的整数可能发生精度碰撞。
3. `UNKNOWN_OUTCOME` 与已知成功前缀共同归入 `PARTIAL`，自动化无法只看顶层状态区分远端结果未知。
4. `RESOURCE` 当前借用认证退出码，资源不存在与凭据错误无法通过 exit code 分类。
5. `PlanAction.count` 没有单位，字段数、记录数、行数、列数和 range 数可能混用。
6. typed Sheet 的 `clear`、wide append 和 batch update 尚未完整覆盖大范围分块。
7. 依赖目标快照定位的 action 尚未携带统一 snapshot precondition；破坏性操作和 Sheet 行定位更新都存在目标状态漂移风险。
8. `XTFSyncEngine` 仍同时负责日志、全局请求控制、SDK 装配、计划、执行和验证，构造期存在文件与全局状态副作用。

## 四、建设目标与非目标

### 4.1 建设目标

XTF 2.0 建成后应具备以下能力：

- 单一正式入口：`XTF.py` 源码入口和 `XTF` 二进制。
- 单一命令面：`sync`、`config`、`doctor`、`--version`。
- 单一配置协议：严格 YAML v2。
- 稳定公共接口限定为 CLI、YAML v2、JSON output contract 和退出码；不承诺稳定 Python SDK。
- 单一同步主干：source → snapshot → reconcile → target plan → execute → verify。
- Bitable、Sheet 分别编译目标操作，不强行统一为 CRUD。
- key、partial、indeterminate、verification 和 snapshot freshness 语义明确。
- dry-run 与正式执行共享 planner；dry-run 可以远端只读，但执行零 Feishu mutation。
- 正式执行支持明确的退出码、JSON contract、applied prefix 和错误分类。
- 主 `XTF` 完成跨平台构建、artifact 回读和真实 Feishu UAT。

### 4.2 明确不做

- 不换语言，不拆微服务，不全面异步化。
- 不新增数据源、目标类型或无关业务功能。
- 不引入旧命令 alias、弃用周期、双配置读取或 feature flag 双跑。
- 不开发 `config migrate`。
- 不继续维护 `XTF-Sheet`、`XTF-Bitable` 和 `lite/` 业务实现。
- 不为旧 `sync() -> bool`、旧 import 或 legacy 私有方法提供兼容 shim。
- 不把 `SyncService`、`core.*`、`api.*` 或 typed client constructor 声明为跨版本稳定的 Python 公共接口。
- 不先创建完整的新 package 空目录，也不为目录形式进行大范围机械搬迁。
- 不把自动测试、CI 或构建成功等同于真实 Feishu 同步完成。

## 五、目标架构

```text
XTF.py / XTF
    │
    ▼
xtf_cli
    │
    ▼
bootstrap
    │
    ▼
SyncService
    ├── SourceReader
    ├── KeyPolicy
    ├── TargetInspector
    ├── TargetSnapshot
    ├── Reconciler
    ├── ModePolicy
    ├── BitablePlanCompiler
    └── SheetPlanCompiler
          │
          ▼
    ExecutionPlan ──to_public()──→ PlanDocument / JSON
          │
          ▼
    Executor + Verifier
          │
          ▼
      SyncResult
```

目标架构继续使用现有 `xtf_cli/`、`core/` 和 `api/` 作为物理边界：

- `xtf_cli/`：命令解析、配置解析、对象装配、输出与退出码。
- `core/`：key、snapshot、reconcile、mode、ExecutionPlan、PlanDocument、execution 和 service。
- `api/`：auth、transport、typed Bitable/Sheet client 和 OpenAPI contract。

只有在真实模块已经形成、旧 Engine 明显变薄后，才评估是否统一 package 名称；目录调整不作为 XTF 2.0 的独立目标。

## 六、实施范围

### 6.1 保留

- `XTF.py` 薄入口。
- `xtf_cli/` 已实现的新 CLI、YAML v2、JSON 和 secret redaction 基础。
- typed auth、transport、Bitable v1、Base v3、Sheet contract 和 fail-closed 读取能力。
- `plan()` / `execute_plan()` 已建立的迁移接缝。
- 现有 key、分页、partial batch、公式保护、写后读回和 CLI contract 测试中仍适用于新方案的部分。
- GitHub Tests 和主 `XTF` Multi-Platform Build 基础。

### 6.2 替换

- `XTFSyncEngine` → `SyncService` 与显式模块协作。
- 巨型、可变 `SyncConfig` 使用方式 → YAML v2 解析后的明确配置对象与策略参数。
- 隐式 key 归一化 → `KeyPolicy`。
- 过渡 planner → source snapshot、reconciler、target-specific compiler、进程内 `ExecutionPlan` 和对外 `PlanDocument`。
- 当前结果归纳 → `SyncResult`、明确的 `PARTIAL` / `INDETERMINATE` 和稳定错误码。
- 全局 Controller 与构造期日志 → bootstrap 显式装配 transport、rate limiter、retry policy 和 logger。
- Sheet 大范围写入 → 共享 RangeChunker、anchor append 和 applied-prefix receipt。

### 6.3 删除

- `lite/XTF_Sheet.py`、`lite/XTF_Bitable.py` 及其 flat 配置模板。
- `.github/workflows/multi-platform-build.yml` 中两个 legacy binary 的构建、平台 bundle 和合并发布逻辑。
- 旧 flat config 解析、旧 parser、旧 `ConfigManager` 执行路径。
- `GlobalRequestController` 及 `api/` 对 `core.control` 的反向依赖。
- `XTFSyncEngine.sync() -> bool`、`sync_bitable_source() -> bool` 等兼容 facade。
- typed 实现对 legacy 私有方法的依赖。
- legacy 专属文档、测试和发布说明。

上述删除在新主干完成并通过门禁后统一执行；开发分支中的临时共存只用于迁移施工，不形成正式双轨产品。

## 七、XTF 2.0 公共契约

XTF 2.0 的稳定公共接口只包括：

```text
CLI 命令
YAML v2
JSON output contract
exit code contract
```

`SyncService`、`core.*`、`api.*` 和 typed client constructor 属于内部实现接口，可用于仓库测试和源码开发，但不承诺跨版本兼容。

### 7.1 CLI 与配置

- 仅支持 `XTF <subcommand>` 或 `python3 XTF.py <subcommand>`。
- 配置只接受 `schema_version: 2` 的嵌套 YAML。
- `app_secret` 优先级：CLI > `XTF_APP_SECRET` > YAML。
- 其他值优先级：CLI > YAML > target-specific defaults。
- 不接受 flat YAML，不自动迁移，不生成兼容配置。
- `sync.match_strategy` 必须显式为 `by_key` 或 `append_only`。
- `full`、`incremental`、`overwrite` 使用 `by_key` 时必须配置有效 key；`append_only` 只允许定义明确的追加行为。
- `clone` 使用 replace-all 语义，不依赖 `match_strategy`。
- 取消 Sheet full 无索引时隐式转为 clone 等模式转换；任何 destructive effective mode 必须在计划中明确展示并要求授权。
- DATETIME 默认 `exact`；选择 `day` 时必须显式配置业务时区，不依赖宿主机默认时区。

配置示例：

```yaml
sync:
  mode: full
  match_strategy: by_key
  index:
    column: ID
```

### 7.2 KeyPolicy

```text
by_key full / incremental / overwrite
├── 本地空 key      ERROR
├── 本地重复 key    ERROR
├── 远端重复 key    ERROR
├── 远端空 key      PRESERVE + WARNING
├── 数字 key        Decimal / 无损文本规范化
├── DATETIME exact  UTC 毫秒
└── DATETIME day    显式 ZoneInfo 业务日

clone / replace-all
└── key 不参与保留判断，按 replace-all 语义处理
```

### 7.3 结果与退出码

顶层结果状态：

```text
SUCCESS
NOOP
FAILED
PARTIAL
INDETERMINATE
```

建议退出码：

| 退出码 | 含义 |
| ---: | --- |
| 0 | success / noop |
| 1 | unexpected runtime |
| 2 | usage |
| 3 | config / local input |
| 4 | authentication |
| 5 | remote resource / read / plan |
| 6 | mutation failure / known partial |
| 7 | verification failure |
| 8 | indeterminate |
| 130 | interrupted |

退出码表示故障类别，不保证与 `SyncResult.status` 一一对应。需要精确区分 `FAILED` 与 `PARTIAL` 的自动化调用必须使用 `--json`；JSON 中继续使用顶层状态和稳定错误码区分 resource、read、mutation rejected、partial、indeterminate 和 verification mismatch。

### 7.4 ExecutionPlan 与 PlanDocument

内部执行计划与公开审阅文档使用不同对象：

```text
ExecutionPlan                    # 进程内
├── typed actions
├── mutation payload
├── snapshot preconditions
└── replay / verification policy
          │
          └── to_public()
                   ▼
PlanDocument                     # 对外
├── action kind
├── count + unit
├── scope 摘要
├── destructive
├── clears_values
└── warnings
```

`PlanDocument` 中每个 action 至少包含：

```text
kind
count
unit
scope
destructive
clears_values
```

`unit` 限定为 `field`、`record`、`row`、`column` 或 `range`。mutation payload 只存在于进程内 `ExecutionPlan`，不进入 `PlanDocument`。

公开 JSON 计划不可直接重放；dry-run 输出是 `PlanDocument`；executor 只接受内部 `ExecutionPlan`。本方案不建设 plan approval/hash 系统。

配置、CLI output envelope 和 `PlanDocument` 各自拥有独立的 `schema_version`；保持字段名不变，在协议文档中分别说明作用域和升级规则。

### 7.5 Sheet required / best effort

| 操作 | 默认要求 |
| --- | --- |
| 数据写入 | required |
| 公式保护 | required |
| 公式验证 | required |
| 配置要求的写后读回 | required |
| 自动推断下拉验证 | best effort |
| 日期显示格式 | best effort |
| 数字显示格式 | best effort |

best-effort enrichment 失败只能产生 warning，不覆盖已经确认成功的数据结果。

## 八、实施步骤：对内分阶段迁移、对外单轨切换

### 阶段 A：固定新协议

实施内容：

1. 声明 XTF 2.0 的稳定公共接口只包括 CLI、YAML v2、JSON output 和退出码。
2. 建立行为刻画测试，固定两个 target、四种 mode、key 和结果状态的当前真值。
3. 正式增加 `sync.match_strategy`，取消无索引时的隐式模式转换。
4. file source 空 key 在 planner preflight 阶段失败。
5. 数字 key 使用 `Decimal` 或无损文本规范化，彻底绕过 `float()`。
6. DataConverter 的 DATETIME granularity 改为构造时显式传递。
7. 增加 `INDETERMINATE`、独立退出码和稳定错误码。
8. 修正 `RESOURCE` 的退出映射。
9. 为公开 action 的 `count` 增加 `unit`。
10. 拆分进程内 `ExecutionPlan` 与对外 `PlanDocument`。
11. 冻结 config、PlanDocument 和 output 三个 schema contract。
12. 集成阶段版本改为 `2.0.0-dev`；只有 RC 构建使用 `2.0.0-rcN`，正式 tag 使用 `2.0.0`。

完成门槛：

- key、DATETIME、outcome、exit code 和 JSON contract 的 focused tests 全部通过。
- 两个 target 的 full/incremental/overwrite/clone 真值测试通过。
- 完整非集成测试、Ruff、Black、MyPy 和 syntax check 通过。

### 阶段 B：完成新执行主干

内部按四个可独立验证的切片实施：

1. B1：SourceReader、SourceTable、TargetInspector、TargetSnapshot、KeyPolicy 和 Reconciler。
2. B2：ModePolicy 和 BitablePlanCompiler。
3. B3：SheetPlanCompiler 和 RangeChunker。
4. B4：ExecutionPlan、Executor、Verifier 和 explicit bootstrap。

实施要求：

1. Bitable Snapshot 保留 backend、revision、timezone、complete、ignored fields 和 inspected time。
2. Sheet Snapshot 保留实际读取范围、grid properties、formula/protected columns 和 inspected time。
3. 建立共享 RangeChunker：
   - write 保留行列双向分块；
   - clear 惰性生成空块；
   - batch update 按 range 数、行和列分块；
   - wide append 使用 anchor append，取得实际行范围后固定写剩余列。
4. 每个 mutation 返回 accepted prefix、actual scope、readback 和 unknown outcome。
5. 所有依赖目标快照定位的 action 均携带 snapshot precondition：
   - Base v3 使用 revision 检查；
   - Bitable v1 重新读取待更新或删除的 record ID 与关键 key；
   - Sheet destructive action 重读即将 clear/rewrite 的关键范围；
   - Sheet row patch 重读 header 和 index column，确认 key → row 未漂移。
6. 移除全局 Controller，transport、retry、rate limiter、logger 和 clients 由 bootstrap 显式装配。

执行顺序：

```text
SourceReader
    ↓
SourceTable
    ↓
TargetInspector
    ↓
TargetSnapshot
    ↓
KeyPolicy + Reconciler
    ↓
ModePolicy
    ↓
TargetPlanCompiler
    ↓
ExecutionPlan
    ↓
Executor + Verifier
    ↓
SyncResult
```

边界策略：

> 某项能力在 RC 前不能可靠实现时，必须由 planner 在零 mutation 阶段明确拒绝，并在文档中声明上限；不得执行到中途后再依赖远端 API 报错。

完成门槛：

- planner 与 executor action 顺序一致。
- incomplete read、stale snapshot、unknown mutation 和 readback mismatch 全部 fail closed。
- `5001` 行、`101` 列、`5001 × 101`、wide append、大范围 clear 和 selective range 测试通过或在 planner 阶段安全拒绝。
- 已知成功前缀和结果未知可被 JSON/exit code 明确区分。

### 阶段 C：切换、删除 legacy、形成 RC

删除顺序：

```text
新主干已承接行为
    ↓
仍代表产品行为的测试迁移到新主干
    ↓
CLI 切换到 SyncService
    ↓
切换候选构建与 smoke
    ↓
完成 XTF 1.9 可执行回滚包归档与下载回读
    ↓
删除旧代码、legacy 测试和 legacy 构建
    ↓
再次运行完整测试和构建
    ↓
形成 2.0.0-rc1
```

实施内容：

1. 把仍代表产品行为的旧测试迁移到新主干；只验证旧入口或旧格式的测试随 legacy 删除。
2. CLI 正式切换到 `SyncService`。
3. 归档 XTF 1.9 回滚包：源码 commit、已验证多平台二进制、SHA-256、旧 flat 配置模板、构建环境和回滚步骤。回滚包可以作为内部归档，无需重新公开发布 1.9。
4. 删除旧 Engine、bool facade、legacy parser/config 和 shim。
5. 删除 `lite/`、两个 legacy binary 和相关构建逻辑。
6. 主发布包只保留 `XTF` / `XTF.exe`、`config.example.yaml`、README/QUICKSTART 和 `checksums.txt`；真实 `config.yaml` 由 `XTF config init` 生成。
7. 清理 legacy 文档和测试，更新 README、CONFIG、SYNC、ARCH、SHEET 和 Release Notes，并提供人工迁移映射和完整 v2 示例。
8. 生成 `2.0.0-rc1` artifact、SHA-256、源 commit 和 workflow run 记录。

完成门槛：

- 仓库中只有一条正式 CLI/config/sync 路径。
- XTF 1.9 回滚包已经归档、下载回读并可实际取用。
- 主 `XTF` 在 Linux x64/ARM64、Windows x64、macOS ARM64 构建和 CLI smoke 通过。
- artifact 下载回读、解压、`--version`、`sync --help`、`config.example.yaml` 和 checksum 检查通过。
- PR #19 已重写标题、正文、兼容边界和 Review focus；现有自动化结果只作为初始基线，最终 head 已重新运行完整测试、构建和 UAT。

## 九、真实 Feishu UAT 与发布

### 9.1 UAT 原则

- 使用隔离、可清理的测试 Bitable 和 Sheet，不使用生产数据。
- 所有 destructive case 先 dry-run，再显式授权执行。
- 每次写入由独立只读调用回读最终状态，不只相信程序自身输出。
- UAT 使用已经构建并记录 checksum 的 RC artifact。
- Release 审批不等同于生产数据写入授权；真实生产同步需另行明确。

### 9.2 最小 UAT 集

1. file → Base v3：full、incremental、overwrite、clone。
2. file → Bitable v1：full，直接验证 typed v1 目标写入。
3. Bitable v1 → Bitable v1：full。
4. Base v3 → Base v3：full，并覆盖字段清空和 schema 不兼容。
5. file → Sheet：5001 行、101 列、selective、公式保护、大范围 clear 和 wide append。
6. 使用 fault proxy 或 transport injector 在请求发送后主动断开响应链路，再通过独立 readback 判断远端是否应用，验证 `INDETERMINATE`。
7. append 未返回实际范围，验证停止和 unknown scope。
8. 写后读回 mismatch，验证 verification failure。
9. dry-run 后远端状态变化，验证 destructive action 被拒绝。
10. Sheet 计划生成后插入行或调整表头，验证 row patch 在 key → row 漂移时停止执行。

### 9.3 发布链路

```text
完成阻塞项
    ↓
构建 2.0.0-rc1 artifact + checksum
    ↓
使用 RC artifact 完成真实 UAT
    ├── 失败：修复 → rc2 → 重新 UAT
    └── 通过
          ↓
最终 commit 完整 CI 与构建
          ↓
artifact 下载回读与最小远端 smoke
          ↓
合并 main / tag / GitHub Release 2.0.0
```

如果正式版因版本号或 merge commit 需要重新构建，应证明源码 tree 等价或差异仅限版本元数据，并重新执行完整 CI、构建、artifact 回读和最小远端 smoke。正式目标是候选内容、构建输入和验证证据可追溯，不强求未经验证的字节级可重现性。

## 十、Git 与变更管理

- `main` 在最终切换前继续代表 1.9 稳定线。
- `codex/xtf-cli-v2` 作为 XTF 2.0 唯一集成线。
- PR #19 作为最终进入 `main` 的 umbrella PR；实施使用短期施工 PR，不建立长期产品双轨：
  - PR-A → `codex/xtf-cli-v2`：公共接口、match strategy、key 和 DATETIME。
  - PR-B → `codex/xtf-cli-v2`：result、exit/error、ExecutionPlan 和 PlanDocument。
  - PR-C → `codex/xtf-cli-v2`：Sheet typed parity。
  - PR-D → `codex/xtf-cli-v2`：Snapshot、Reconciler、Compiler、Executor 和新鲜度检查。
  - PR-E → `codex/xtf-cli-v2`：legacy 删除、1.9 回滚包和 RC 构建。
- 每个子 PR 独立审查、独立验证并合并到集成线；前一阶段未通过，不继续叠加后续改动。
- 建立统一的 `XTF 2.0` milestone，将 PR #19、短期施工 PR、RC、UAT 和 Release gate 纳入同一状态视图，并为各切片指定 reviewer。
- 方案获批后，PR #19 必须重写标题、正文、兼容边界和 Review focus，删除现有兼容性承诺；`a55c1a6` 的测试和构建结果只作为初始基线。
- 发现问题使用独立 revert commit，不重写已审查历史。
- 不把无关功能、依赖升级或代码清理混入 XTF 2.0。

## 十一、回滚方案

### 11.1 合并前

- `main` 与 Release `1.9.0` 保持不变。
- 删除 legacy workflow 前完成 XTF 1.9 可执行回滚包归档、checksum、下载回读和恢复演练。
- 任一阶段未通过门禁，不合并 PR #19，不创建正式 tag。
- 集成分支使用 revert 回退失败变更，不 force push，不覆盖现有验证证据。

### 11.2 正式发布后

- 保留已经验证的 XTF 1.9 可执行回滚包、`1.9.0` tag、源码和旧配置备份，紧急情况下重新使用 1.9 稳定版本。
- 2.0 YAML 与旧 flat config 不可互换；回滚 1.9 时恢复升级前配置，不提供自动反向转换。
- XTF 同步是非事务性的；已经发生的远端 mutation 不能仅靠回滚二进制撤销。
- 生产执行 `overwrite`、`clone` 或任何 delete/clear action 前，必须另行备份目标数据并获得明确执行授权。

## 十二、风险与控制措施

| 风险 | 影响 | 控制措施 |
| --- | --- | --- |
| 旧命令和配置失效 | 现有脚本无法直接升级 | Release Notes 提供新命令和完整 v2 示例，不提供双轨运行 |
| legacy binary 停止发布 | 旧下载和使用方式中断 | 统一由 `XTF sync --target-type` 覆盖 Bitable/Sheet，不再维护重复实现 |
| key 规则改变 | 旧数据可能暴露空值、重复值或大整数问题 | planner preflight fail closed，发布前完成边界测试与 UAT |
| Sheet 大范围操作失败 | 可能部分写入或未知落点 | RangeChunker、anchor append、applied prefix；不能实现时预检拒绝 |
| 依赖目标快照的 action 基于过期状态 | 可能删除、清空或更新错误行 | snapshot precondition；revision、record/key 或 header/index/range 重新确认 |
| 架构切换范围大 | 回归和合并风险增加 | 三阶段门禁、短期施工 PR、独立 review checkpoint、行为刻画测试和可 revert commit |
| 构建产物与候选不一致 | UAT 证据不能证明正式版 | exact source/tree、workflow、checksum、下载回读和最终 smoke |
| 缺少真实 UAT | 无法证明 Feishu 远端行为 | UAT 作为正式 Release 硬门槛 |

## 十三、交付物与完成标准

### 13.1 交付物

- XTF 2.0 新架构源码。
- 单一 `XTF` 多平台 artifact。
- `config.example.yaml`、QUICKSTART、人工迁移映射和完整 YAML v2 示例。
- CLI、YAML v2、JSON、exit code、ExecutionPlan、PlanDocument、result 和 error contract 文档。
- key、Bitable、Sheet、snapshot freshness 和 failure-path 测试。
- exact-head CI 与 Multi-Platform Build 证据。
- XTF 1.9 可执行回滚包、旧配置模板、checksum、构建说明和下载回读记录。
- RC artifact、checksum 和下载回读记录。
- 真实 Feishu UAT 报告与独立 readback 证据。
- XTF 2.0 Release Notes 和 breaking-change 清单。

### 13.2 完成标准

只有同时满足以下条件，才能宣布 XTF 2.0 完成：

1. XTF 2.0 稳定公共接口已经限定为 CLI、YAML v2、JSON output 和退出码。
2. 唯一 CLI/config/sync 主干已经切换，legacy 路径已经删除。
3. 删除 legacy 构建前，XTF 1.9 可执行回滚包已经归档、下载回读并完成恢复演练。
4. 两个 target 的关键行为测试、完整测试和质量检查通过。
5. 主 `XTF` 多平台构建、artifact 回读和 smoke 通过；发布包不附带会被自动发现的真实 `config.yaml`。
6. RC artifact 在隔离 Feishu 资源上完成 UAT。
7. snapshot freshness、partial、indeterminate 和 verification 结果均有可审查证据。
8. PR #19 的标题、正文、Review focus、最终 head 和验证证据与本方案一致。
9. 最终 Release 指向已经验证的源码和构建链路。
10. PR、合并、Release、远端 readback 和业务 UAT 分层记录，不相互替代。

## 十四、已知限制与待补证据

- 当前判断主要来自源码、测试、GitHub CI 和构建记录；尚无真实 Feishu UAT 结果。
- Feishu OpenAPI 的实时限额、响应范围和异常行为仍需在隔离测试资源中验证。
- XTF 2.0 的实际工作量不在本方案中承诺，以阶段门禁和交付证据决定是否进入下一阶段。
- 本方案批准后允许开展仓库内 breaking refactor 和 legacy 删除；不自动授权生产同步、生产删除、正式 Release 发布或任何外部可见变更。

## 十五、依据与证据

- PR #19：<https://github.com/BlueSkyXN/XTF/pull/19>
- exact-head Tests：<https://github.com/BlueSkyXN/XTF/actions/runs/33354583165>
- exact-head Multi-Platform Build：<https://github.com/BlueSkyXN/XTF/actions/runs/33354606914>
- 当前正式 Release：<https://github.com/BlueSkyXN/XTF/releases/tag/1.9.0>
- 关键源码：`XTF.py`、`xtf_cli/`、`core/engine.py`、`core/plan.py`、`core/converter.py`、`api/sheet.py`、`api/bitable_backend.py`
- 关键测试：`tests/test_cli*.py`、`tests/test_config.py`、`tests/test_converter.py`、`tests/test_engine.py`、`tests/test_plan.py`

## 十六、审议意见

请在以下意见中选择：

- [ ] 同意按本方案实施。
- [x] 原则同意，按附加条件实施。
- [ ] 退回修改后重新送审。
- [ ] 不同意实施。

附加条件或审议意见：

1. “一次性切换”限定为正式产品单轨切换；研发过程必须按阶段、按可独立审查的变更切片推进，不采用一次性重写。
2. XTF 2.0 不提供稳定 Python SDK；CLI、YAML v2、JSON 和退出码是唯一稳定兼容契约。
3. PR #19 按本方案重写标题、正文、兼容边界和验证证据；当前自动化结果只作为初始基线。
4. 删除 legacy 构建前，完成 XTF 1.9 可执行回滚包、旧配置模板、checksum、下载回读和恢复演练。
5. 正式 RC 前完成空 key、大整数 key、`INDETERMINATE`、typed Sheet 大范围能力以及所有依赖目标快照定位 action 的新鲜度检查。
6. 发布包只附带 `config.example.yaml`；真实 `config.yaml` 由 `XTF config init` 生成。
7. RC artifact 必须经过真实 Feishu UAT 和独立 readback；正式 Release 必须对应已经重新验证的最终源码与构建链路。

审批说明：本方案获批后，项目进入 XTF 2.0 breaking refactor 实施阶段；正式 Release 和真实生产数据操作仍按各自门禁单独审批。
