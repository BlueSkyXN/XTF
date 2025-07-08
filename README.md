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

复制 `config.example.json` 为 `config.json`：
```bash
cp config.example.json config.json
```

编辑 `config.json`，填入实际参数：
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

## 🚀 使用方法

### 基础用法
```bash
python XTF.py
```

### 命令行参数
```bash
# 指定Excel文件和同步模式
python XTF.py --file-path data.xlsx --sync-mode incremental

# 指定索引列
python XTF.py --index-column "员工ID" --sync-mode full

# 性能调优
python XTF.py --batch-size 1000 --rate-limit-delay 0.3

# 调试模式
python XTF.py --log-level DEBUG
```

### 完整参数列表
```bash
python XTF.py --help
```

**主要参数说明:**
- `--file-path`: Excel文件路径
- `--sync-mode`: 同步模式 (full/incremental/overwrite/clone)
- `--index-column`: 索引列名称
- `--batch-size`: 批处理大小 (默认500)
- `--rate-limit-delay`: API调用间隔秒数 (默认0.5)
- `--max-retries`: 最大重试次数 (默认3)
- `--log-level`: 日志级别 (DEBUG/INFO/WARNING/ERROR)

## 📊 同步模式详解

### 🔄 全量同步 (推荐)
```bash
python XTF.py --sync-mode full --index-column "ID"
```
- 根据索引列比对数据
- 已存在的记录：更新内容
- 不存在的记录：新增记录
- **适用场景**: 日常数据同步，既有更新又有新增

### ➕ 增量同步
```bash
python XTF.py --sync-mode incremental --index-column "ID"
```
- 根据索引列过滤数据
- 已存在的记录：跳过
- 不存在的记录：新增记录
- **适用场景**: 只添加新数据，保护已有数据

### 🔄 覆盖同步
```bash
python XTF.py --sync-mode overwrite --index-column "ID"
```
- 删除远程表中索引值匹配的记录
- 新增本地的全部记录
- **适用场景**: 本地数据为准，覆盖部分远程数据

### 🔄 克隆同步
```bash
python XTF.py --sync-mode clone
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

### 查看日志
```bash
# 查看最新日志
ls -la logs/
tail -f logs/xtf_20241208_143022.log
```

## ⚡ 性能优化建议

### 大数据量处理
```bash
# 增加批处理大小
python XTF.py --batch-size 1000

# 减少API调用间隔
python XTF.py --rate-limit-delay 0.3
```

### 网络不稳定环境
```bash
# 增加重试次数
python XTF.py --max-retries 5

# 增加API调用间隔
python XTF.py --rate-limit-delay 1.0
```

## 🚨 注意事项

### 索引列要求
- **全量/增量/覆盖同步**: 必须指定索引列
- **克隆同步**: 不需要索引列
- 索引列值应该唯一且稳定

### API限制
- 批量创建：最多1000条记录/次
- 批量更新：最多1000条记录/次
- 批量删除：最多500条记录/次
- API频率：参考飞书官方限制

### 数据安全
- 克隆模式会清空目标表格，请谨慎使用
- 建议先在测试环境验证
- 重要数据请提前备份

## 🔧 故障排除

### 常见错误

**1. 认证失败**
```
错误: 获取访问令牌失败
解决: 检查app_id和app_secret是否正确
```

**2. 表格不存在**
```
错误: app_token错误
解决: 确认app_token和table_id是否正确
```

**3. 权限不足**
```
错误: Permission denied
解决: 确保应用有多维表格的管理权限
```

**4. 字段类型错误**
```
错误: 字段类型转换失败
解决: 检查Excel数据格式，确保与目标字段类型匹配
```

### 调试方法
```bash
# 开启详细日志
python XTF.py --log-level DEBUG

# 小批量测试
python XTF.py --batch-size 10

# 查看完整错误信息
cat logs/xtf_*.log | grep ERROR
```

## 🔄 版本历史

### v1.0.0
- ✨ 支持四种同步模式
- ✨ 智能字段类型转换
- ✨ 企业级错误处理和重试机制
- ✨ 性能优化和批处理
- ✨ 详细日志和进度跟踪

---

**XTF** - 让Excel与飞书多维表格同步更智能、更高效！ 🚀