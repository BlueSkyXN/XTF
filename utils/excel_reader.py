"""
Excel 智能读取模块

优先使用高性能 Calamine 引擎（Rust实现，速度提升4-20倍）
失败时自动回退到 OpenPyXL 引擎（Python实现，稳定可靠）

作者: XTF Team
版本: 1.7.3+
"""
import pandas as pd
from pathlib import Path
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)


def smart_read_excel(
    file_path: Union[str, Path],
    sheet_name: Union[str, int] = 0,
    **kwargs
) -> pd.DataFrame:
    """
    智能读取 Excel 文件，自动选择最优引擎

    引擎优先级:
    1. Calamine (python-calamine) - Rust实现，性能优异
       - 读取速度: 4-20倍于 OpenPyXL
       - 支持格式: .xlsx, .xlsm, .xls, .xlsb, .ods
       - 限制: 仅支持读取，不支持写入

    2. OpenPyXL - Python实现，功能完整
       - 读取速度: 标准性能
       - 支持格式: .xlsx, .xlsm
       - 优势: 稳定可靠，社区成熟

    Args:
        file_path: Excel 文件路径
        sheet_name: 工作表名称或索引，默认为 0（第一个工作表）
        **kwargs: 传递给 pd.read_excel 的其他参数

    Returns:
        pd.DataFrame: 读取的数据框

    Raises:
        Exception: 当所有引擎都无法读取文件时抛出异常

    Examples:
        >>> df = smart_read_excel('data.xlsx')
        >>> df = smart_read_excel('data.xlsx', sheet_name='Sheet1')
        >>> df = smart_read_excel('data.xlsx', header=0, dtype={'col': str})
    """
    file_path = Path(file_path)

    # 尝试 1: Calamine 引擎 (高性能)
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            engine='calamine',
            **kwargs
        )
        logger.debug(f"✅ Calamine 引擎读取成功: {file_path.name}")
        return df

    except ImportError:
        # python-calamine 未安装
        logger.debug("⚠️ python-calamine 未安装，使用 OpenPyXL 引擎")

    except Exception as e:
        # Calamine 引擎读取失败（可能是文件格式问题）
        logger.warning(f"⚠️ Calamine 引擎失败，切换到 OpenPyXL: {e}")

    # 尝试 2: OpenPyXL 引擎 (备用)
    try:
        df = pd.read_excel(
            file_path,
            sheet_name=sheet_name,
            engine='openpyxl',
            **kwargs
        )
        logger.debug(f"✅ OpenPyXL 引擎读取成功: {file_path.name}")
        return df

    except Exception as e:
        # 所有引擎都失败
        error_msg = f"❌ 无法读取 Excel 文件 {file_path.name}: {e}"
        logger.error(error_msg)
        raise Exception(error_msg) from e


def get_available_engines() -> dict:
    """
    检测当前环境可用的 Excel 读取引擎

    Returns:
        dict: 引擎可用性信息
            {
                'calamine': bool,
                'openpyxl': bool,
                'primary': str,  # 主引擎名称
                'fallback': str  # 备用引擎名称
            }

    Examples:
        >>> engines = get_available_engines()
        >>> print(f"主引擎: {engines['primary']}")
    """
    engines = {
        'calamine': False,
        'openpyxl': False,
        'primary': None,
        'fallback': None
    }

    # 检测 Calamine
    try:
        import python_calamine
        engines['calamine'] = True
        engines['primary'] = 'calamine'
    except ImportError:
        pass

    # 检测 OpenPyXL
    try:
        import openpyxl
        engines['openpyxl'] = True
        if engines['primary'] is None:
            engines['primary'] = 'openpyxl'
        else:
            engines['fallback'] = 'openpyxl'
    except ImportError:
        pass

    # 如果 Calamine 可用，OpenPyXL 作为备用
    if engines['calamine'] and engines['openpyxl']:
        engines['fallback'] = 'openpyxl'

    return engines


def print_engine_info(verbose: bool = True) -> Optional[str]:
    """
    打印当前可用的 Excel 引擎信息

    Args:
        verbose: 是否打印详细信息，默认为 True

    Returns:
        str: 引擎信息字符串（当 verbose=False 时返回）

    Examples:
        >>> print_engine_info()
        🚀 Excel引擎: Calamine (高性能模式) + OpenPyXL (备用)
    """
    engines = get_available_engines()

    # 构建信息字符串
    if engines['calamine'] and engines['openpyxl']:
        info = "🚀 Excel引擎: Calamine (高性能模式) + OpenPyXL (备用)"
    elif engines['calamine']:
        info = "🚀 Excel引擎: Calamine (高性能模式)"
    elif engines['openpyxl']:
        info = "📖 Excel引擎: OpenPyXL (标准模式)"
    else:
        info = "⚠️ 警告: 未安装 Excel 引擎，请运行: pip install python-calamine openpyxl"

    if verbose:
        print(info)
    else:
        return info
