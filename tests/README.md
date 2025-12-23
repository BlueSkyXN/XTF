# XTF 测试套件

本目录包含 XTF 项目的自动化测试套件，参考了 SuperBatchVideoCompressor 的测试架构。

## 测试结构

```
tests/
├── __init__.py              # 测试模块标记
├── conftest.py              # Pytest fixtures 和公共配置
├── test_config.py           # 配置模块测试 (48 tests)
├── test_converter.py        # 数据转换模块测试 (60 tests)
├── test_reader.py           # 文件读取模块测试 (25 tests)
├── test_control.py          # 重试和频控策略测试 (29 tests)
└── test_api_base.py         # HTTP 客户端测试 (13 tests)
```

**总计: 152 个测试用例**

## 快速开始

### 安装测试依赖

```bash
pip install -r requirements.txt
```

测试依赖包括:
- `pytest>=7.0.0` - 测试框架
- `pytest-cov>=4.0.0` - 代码覆盖率
- `pytest-mock>=3.10.0` - Mock 工具

### 运行所有测试

```bash
# 运行所有测试
pytest tests/ -v

# 运行特定测试文件
pytest tests/test_config.py -v

# 运行特定测试类
pytest tests/test_config.py::TestSyncConfig -v

# 运行特定测试方法
pytest tests/test_config.py::TestSyncConfig::test_bitable_config_creation -v
```

### 代码覆盖率

```bash
# 生成覆盖率报告
pytest tests/ --cov=core --cov=api --cov-report=term --cov-report=html

# 查看 HTML 报告
open htmlcov/index.html
```

**当前覆盖率:**
- **core/control.py**: 82% (重试和频控策略)
- **core/reader.py**: 74% (文件读取)
- **api/base.py**: 69% (HTTP 客户端)
- **core/config.py**: 47% (配置管理)
- **core/converter.py**: 47% (数据转换)
- **总体**: 32% (2771 行中的 873 行)

## 测试类别

### 1. 配置测试 (test_config.py)

测试配置管理功能，包括:
- ✅ 枚举类型验证
- ✅ 选择性同步配置
- ✅ 配置对象创建和验证
- ✅ 配置文件加载和保存
- ✅ 字符串到枚举的自动转换
- ✅ 配置验证和错误处理

**关键测试场景:**
- 多维表格和电子表格配置
- 必需参数验证
- Selective sync 配置验证
- 重复列名检测
- 最大间隙参数范围验证

### 2. 数据转换测试 (test_converter.py)

测试数据转换和字段类型推断，包括:
- ✅ 索引值哈希计算
- ✅ 记录索引构建
- ✅ 类型检测（数字、日期、时间戳）
- ✅ Excel 列数据分析
- ✅ 字段类型策略 (RAW/BASE/AUTO/INTELLIGENCE)
- ✅ 强制类型转换
- ✅ DataFrame 和值列表转换

**关键测试场景:**
- 数字字符串检测（支持千分位）
- 日期格式检测（多种格式）
- 时间戳检测（秒级、毫秒级）
- 富文本格式解析
- 列号和字母转换

### 3. 文件读取测试 (test_reader.py)

测试多格式文件读取，包括:
- ✅ Excel 文件读取 (.xlsx, .xls)
- ✅ CSV 文件读取 (UTF-8, GBK)
- ✅ 格式自动检测
- ✅ 错误处理
- ✅ 边界情况处理

**关键测试场景:**
- 空文件处理
- 特殊字符处理
- 大文件处理（1000+ 行）
- 不同分隔符的 CSV
- 文件不存在错误

### 4. 重试和频控测试 (test_control.py)

测试高级重试和频控策略，包括:
- ✅ 指数退避重试
- ✅ 线性增长重试
- ✅ 固定等待重试
- ✅ 固定等待频控
- ✅ 滑动时间窗频控
- ✅ 固定时间窗频控
- ✅ 全局控制器单例

**关键测试场景:**
- 延迟计算验证
- 最大等待时间限制
- 时间窗清理机制
- 请求计数管理
- 从配置创建策略

### 5. HTTP 客户端测试 (test_api_base.py)

测试 HTTP 请求和重试机制，包括:
- ✅ 频率限制器
- ✅ API 客户端初始化
- ✅ HTTP 方法 (GET/POST/PUT/DELETE)
- ✅ 重试机制
- ✅ 错误处理

**关键测试场景:**
- 服务器错误重试
- 频率限制重试
- 请求异常重试
- 指数退避验证
- Mock 响应处理

## Fixtures

### 配置 Fixtures

```python
sample_bitable_config    # 多维表格配置
sample_sheet_config      # 电子表格配置
sample_selective_sync_config  # 选择性同步配置
sample_config_dict       # 配置字典
```

### 数据 Fixtures

```python
sample_dataframe         # 标准测试数据
sample_dataframe_with_types  # 包含多种类型的数据
sample_records          # 飞书记录列表
```

### 文件 Fixtures

```python
temp_excel_file         # 临时 Excel 文件
temp_csv_file           # 临时 CSV 文件
temp_config_file        # 临时配置文件
```

## CI/CD 集成

测试在 GitHub Actions 中自动运行，配置文件: `.github/workflows/test.yml`

### 测试矩阵

**平台覆盖:**
- Ubuntu 22.04, 24.04
- Windows Server 2022, 2025
- macOS 13 (Intel), macOS Latest (ARM)

**Python 版本:**
- 3.10, 3.11, 3.12, 3.13

**任务:**
1. **代码质量检查**
   - Ruff 代码检查
   - Black 格式检查
   - MyPy 类型检查
   - Python 语法检查

2. **单元测试**
   - 152 个测试用例
   - 代码覆盖率报告
   - 多平台验证

### 触发条件

- Push 到 main/master/dev 分支
- Pull Request 到这些分支
- 手动触发 (workflow_dispatch)

## 最佳实践

### 编写新测试

1. **使用清晰的测试类名**
   ```python
   class TestFeatureName:
       """功能名称测试"""
   ```

2. **使用描述性的测试方法名**
   ```python
   def test_specific_behavior_under_condition(self):
       """测试特定条件下的特定行为"""
   ```

3. **使用 fixtures 减少重复**
   ```python
   def test_something(self, sample_bitable_config):
       # 使用 fixture 提供的配置
   ```

4. **使用 parametrize 测试多个场景**
   ```python
   @pytest.mark.parametrize("input,expected", [
       ("2024-01-01", True),
       ("invalid", False),
   ])
   def test_date_detection(self, input, expected):
       assert is_date(input) == expected
   ```

### Mock 最佳实践

```python
from unittest.mock import Mock, patch

# Mock HTTP 请求
@patch('requests.request')
def test_api_call(self, mock_request):
    mock_response = Mock()
    mock_response.status_code = 200
    mock_request.return_value = mock_response
    # 测试代码

# Mock 时间以加速测试
@patch('time.sleep')
def test_retry(self, mock_sleep):
    # 测试重试逻辑
```

## 测试标记

虽然目前未使用，但测试框架支持以下标记:

```python
@pytest.mark.integration  # 集成测试
@pytest.mark.slow         # 慢速测试
@pytest.mark.unit         # 单元测试
```

运行特定标记的测试:
```bash
pytest -m "not integration"  # 排除集成测试
```

## 未来改进

### 短期目标

1. **提高覆盖率到 70%+**
   - 添加 API 模块测试 (bitable.py, sheet.py, auth.py)
   - 添加 engine.py 测试
   - 添加更多边界情况测试

2. **添加集成测试**
   - 端到端同步流程测试
   - Mock Feishu API 响应
   - 不同同步模式测试

3. **性能测试**
   - 大数据集测试
   - 并发请求测试
   - 内存使用测试

### 长期目标

1. **测试数据生成**
   - 使用 Faker 生成测试数据
   - 自动生成各种字段类型

2. **快照测试**
   - 验证转换结果的一致性
   - 回归测试支持

3. **并行测试**
   - 使用 pytest-xdist 加速测试
   - 优化测试执行时间

## 故障排查

### 常见问题

**Q: 测试失败 "ModuleNotFoundError"**

A: 确保从项目根目录运行测试:
```bash
cd /path/to/XTF
pytest tests/
```

**Q: CSV 测试失败 "EmptyDataError"**

A: pandas 不支持完全空的 CSV，需要至少有表头:
```python
empty_file.write_text("col1,col2\n")
```

**Q: Mock 不工作**

A: 检查 mock 路径是否正确，应该 mock 被测试代码导入的位置:
```python
# 如果 api/base.py 导入 requests
@patch('requests.request')  # 正确
# 不是
@patch('api.base.requests.request')  # 错误
```

## 参考资源

- [Pytest 官方文档](https://docs.pytest.org/)
- [pytest-cov 文档](https://pytest-cov.readthedocs.io/)
- [unittest.mock 文档](https://docs.python.org/3/library/unittest.mock.html)
- [SuperBatchVideoCompressor 测试示例](https://github.com/user/SuperBatchVideoCompressor/tree/main/tests)

## 贡献指南

添加新测试时:
1. 遵循现有的测试结构和命名约定
2. 为新功能添加相应的测试
3. 确保所有测试通过: `pytest tests/ -v`
4. 检查覆盖率: `pytest tests/ --cov=core --cov=api`
5. 更新此 README 如果添加新的测试类别

---

**测试统计:**
- 测试文件: 5
- 测试类: 54
- 测试方法: 152
- 代码覆盖率: 32% (目标: 70%+)
- 支持平台: 6 (Linux/Windows/macOS)
- Python 版本: 4 (3.10-3.13)
