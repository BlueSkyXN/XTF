# XTF - 表格同步工具

XTF (Excel To Feishu) 是一个强大的本地 Excel 表格到飞书多维表格的同步工具，支持四种智能同步模式，具备企业级功能特性。

## ✨ 核心功能

### 🔄 四种同步模式

1. **全量同步 (full)** - 默认模式
   - 检查索引列，已存在索引值的记录执行**更新**
   - 不存在索引值的记录执行**新增**
   - 适用于：数据更新和增量添加并存的场景

2. **增量同步 (incremental)**
   - 检查索引列，已存在索引值的记录**跳过**
   - 只对不存在索引值的记录执行**新增**
   - 适用于：只添加新数据，不修改已有数据

3. **覆盖同步 (overwrite)**
   - 先删除本地数据中已存在索引值对应的远程记录
   - 然后新增本地的全部记录
   - 适用于：本地数据为准，覆盖远程对应记录

4. **克隆同步 (clone)**
   - 清空远程表格的全部记录
   - 然后新增本地的全部记录
   - 适用于：完全重建远程表格数据

### 🚀 企业级特性

- **🎯 智能字段管理**: 自动创建目标表缺少的字段
- **🔄 智能字段类型转换**: 数字、日期、布尔值、多选等类型自动识别
- **📊 分页处理**: 支持大数据量，自动处理分页
- **🛡️ 频率控制**: 内置rate limiter，防止API调用过频
- **🔁 智能重试**: 指数退避算法，自动处理网络异常
- **⚡ 性能优化**: 批处理 + 一致性检查优化
- **📝 详细日志**: 双输出(文件+控制台)，支持中文
- **🔧 灵活配置**: 支持config.json + 命令行参数

## 🛠️ 安装要求

### Python 依赖
```bash
pip install pandas requests uuid datetime pathlib
```

### 系统要求
- Python 3.7+
- 网络连接
- 飞书应用凭证

## ⚙️ 配置设置

### 1. 飞书应用配置

在飞书开放平台创建应用并获取：
- `app_id`: 应用ID
- `app_secret`: 应用密钥
- `app_token`: 多维表格应用Token
- `table_id`: 数据表ID

### 2. 配置文件

#### 默认配置文件
复制 `config.example.json` 为 `config.json`：
```bash
cp config.example.json config.json
```

#### 自定义配置文件
也可以创建自定义配置文件并通过 `--config` 参数指定：
```bash
python XTF.py --config my_config.json
```

编辑配置文件，填入实际参数：
```json
{
  "file_path": "你的Excel文件.xlsx",
  "app_id": "cli_你的应用ID",
  "app_secret": "你的应用密钥",
  "app_token": "你的应用Token",
  "table_id": "你的表格ID",
  "sync_mode": "full",
  "index_column": "ID",
  "batch_size": 500,
  "rate_limit_delay": 0.5,
  "max_retries": 3,
  "create_missing_fields": true,
  "log_level": "INFO"
}
```

### 3. 参数优先级

XTF 使用以下参数优先级规则（**从低到高**）：
1. **默认值** - 代码中的内置默认值
2. **配置文件** - 从配置文件读取的参数
3. **命令行参数** - 通过命令行指定的参数（最高优先级）

这意味着：
- 配置文件参数会覆盖默认值
- 命令行参数会覆盖配置文件参数
- 只有明确指定的命令行参数才会覆盖配置文件，未指定的保持配置文件值

## 🚀 使用方法

### 基础用法
```bash
# 使用默认配置文件 (config.json)
python XTF.py

# 使用自定义配置文件
python XTF.py --config my_config.json
```

### 命令行参数
```bash
# 指定Excel文件和同步模式
python XTF.py --file-path data.xlsx --sync-mode incremental

# 指定索引列
python XTF.py --index-column "员工ID" --sync-mode full

# 指定自定义配置文件
python XTF.py --config production.json --sync-mode full

### 配置文件与命令行混合使用示例

```bash
# 使用配置文件设置基础参数，命令行覆盖特定参数
python XTF.py --config prod.json --sync-mode incremental --batch-size 1000

# 使用不同配置文件进行测试
python XTF.py --config test.json --log-level DEBUG --batch-size 10

# 临时覆盖配置文件中的性能参数
python XTF.py --rate-limit-delay 0.1 --max-retries 5
```

### 自动创建配置文件

如果指定的配置文件不存在，XTF会自动创建示例配置文件：
```bash
# 自动创建 my_config.json 示例配置文件
python XTF.py --config my_config.json
```

### 完整参数列表
```bash
python XTF.py --help
```

**主要参数说明:**

**配置文件参数** (可在配置文件中设置，也可通过命令行覆盖)：
- `--file-path`: Excel文件路径
- `--sync-mode`: 同步模式 (full/incremental/overwrite/clone)
- `--index-column`: 索引列名称
- `--batch-size`: 批处理大小 (默认500)
- `--rate-limit-delay`: API调用间隔秒数 (默认0.5)
- `--max-retries`: 最大重试次数 (默认3)
- `--create-missing-fields`: 是否自动创建缺失字段 (默认true)
- `--log-level`: 日志级别 (DEBUG/INFO/WARNING/ERROR，默认INFO)

**命令行专用参数**：
- `--config`: 指定配置文件路径 (默认config.json)

**参数优先级说明**：
- 默认值 < 配置文件 < 命令行参数
- 只有明确指定的命令行参数才会覆盖配置文件值
- 未指定的命令行参数保持配置文件或默认值

## 📊 同步模式详解

### 🔄 全量同步 (推荐)
```bash
python XTF.py --sync-mode full --index-column "ID"
# 或在配置文件中设置 "sync_mode": "full"
```
- 根据索引列比对数据
- 已存在的记录：更新内容
- 不存在的记录：新增记录
- **适用场景**: 日常数据同步，既有更新又有新增

### ➕ 增量同步
```bash
python XTF.py --sync-mode incremental --index-column "ID"
# 或在配置文件中设置 "sync_mode": "incremental"
```
- 根据索引列过滤数据
- 已存在的记录：跳过
- 不存在的记录：新增记录
- **适用场景**: 只添加新数据，保护已有数据

### 🔄 覆盖同步
```bash
python XTF.py --sync-mode overwrite --index-column "ID"
# 或在配置文件中设置 "sync_mode": "overwrite"
```
- 删除远程表中索引值匹配的记录
- 新增本地的全部记录
- **适用场景**: 本地数据为准，覆盖部分远程数据

### 🔄 克隆同步
```bash
python XTF.py --sync-mode clone
# 或在配置文件中设置 "sync_mode": "clone"
```
- 清空远程表格全部数据
- 新增本地的全部记录
- **适用场景**: 完全重建远程表格，数据迁移

## 📁 文件结构

```
XTF/
├── XTF.py                 # 主程序
├── config.json            # 配置文件
├── config.example.json    # 配置示例
├── README.md             # 说明文档
├── logs/                 # 日志目录
│   └── xtf_YYYYMMDD_HHMMSS.log
└── docs/                 # API文档
    └── feishu-openapi-doc/
```

## 🔍 字段类型支持

XTF 智能识别并转换以下字段类型：

| Excel类型 | 飞书类型 | 转换规则 |
|-----------|----------|----------|
| 文本 | 文本 | 直接转换 |
| 数字 | 数字 | 保持数值类型 |
| 日期 | 日期 | 转为毫秒级时间戳 |
| true/false | 复选框 | 转为布尔值 |
| 选项1,选项2 | 多选 | 按分隔符拆分 |
| 选项1 | 单选 | 直接转换 |

## 📝 日志系统

### 日志输出
- **控制台**: 实时显示同步进度
- **文件**: 保存在 `logs/` 目录，按时间戳命名

### 日志级别
- **DEBUG**: 详细调试信息
- **INFO**: 一般信息 (默认)
- **WARNING**: 警告信息
- **ERROR**: 错误信息


## 🚨 注意事项

### 索引列要求
- **全量/增量/覆盖同步**: 必须指定索引列
- **克隆同步**: 不需要索引列
- 索引列值应该唯一且稳定

### 索引列缺失情况处理
当索引列在表格中不存在时，各同步模式的行为：

| 同步模式 | 本地表格无索引列 | 飞书表格无索引列 | 处理结果 |
|----------|----------------|----------------|----------|
| **全量同步** | 所有记录视为新增 | 所有记录视为新增 | 退化为纯新增操作，可能产生重复记录 |
| **增量同步** | 所有记录视为新增 | 所有记录视为新增 | 所有记录都会被新增，容易产生重复 |
| **覆盖同步** | 同步失败 | 同步失败 | 返回错误，要求必须指定有效索引列 |
| **克隆同步** | 正常工作 | 正常工作 | 不受影响，清空后重新添加全部数据 |

⚠️ **重要提示**: 
- 使用全量/增量模式时，如果索引列不存在，建议先检查列名是否正确
- 覆盖模式对索引列要求严格，缺失时会直接失败
- 克隆模式是唯一不依赖索引列的模式，适合数据完全重建场景

### API限制
- 批量创建：最多1000条记录/次
- 批量更新：最多1000条记录/次
- 批量删除：最多500条记录/次
- API频率：参考飞书官方限制

### 数据安全
- 克隆模式会清空目标表格，请谨慎使用
- 建议先在测试环境验证
- 重要数据请提前备份

## 🔍 故障排查流程

1. **检查配置文件**
   ```bash
   # 验证配置文件格式正确
   python -c "import json; print(json.load(open('config.json')))"
   ```

2. **验证网络连接**
   ```bash
   # 测试与飞书API的连接
   curl -v https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal
   ```

3. **检查API权限**
   ```bash
   # 程序会输出权限错误信息
   python XTF.py --log-level DEBUG
   ```

4. **故障隔离**
   ```bash
   # 使用最小数据集测试
   head -n 10 data.xlsx > test.xlsx
   python XTF.py --file-path test.xlsx --batch-size 2
   ```

5. **查看详细日志**
   ```bash
   # 查找错误
   grep -i error logs/xtf_*.log
   
   # 查找警告
   grep -i warning logs/xtf_*.log
   
   # 查看API响应
   grep -A 10 "API响应" logs/xtf_*.log
   ```

6. **调试类型转换问题**
   ```bash
   # 开启DEBUG级别日志查看数据转换过程
   python XTF.py --log-level DEBUG | grep "转换"
   ```

### 调试方法
```bash
# 开启详细日志
python XTF.py --log-level DEBUG

# 小批量测试
python XTF.py --batch-size 10

# 使用测试配置文件
python XTF.py --config test_config.json --log-level DEBUG --batch-size 10

# 查看完整错误信息
cat logs/xtf_*.log | grep ERROR
```

## 🔧 配置文件管理

### 多环境配置
```bash
# 开发环境
python XTF.py --config dev.json

# 测试环境
python XTF.py --config test.json

# 生产环境
python XTF.py --config prod.json
```

### 配置文件自动生成
```bash
# 生成默认配置文件
python XTF.py --config my_config.json
# 程序会自动创建 my_config.json 示例文件并退出

# 然后编辑配置文件
nano my_config.json

# 使用配置文件运行
python XTF.py --config my_config.json
```