"""数据模型定义"""

from pydantic import BaseModel
from typing import Optional


# ─── 解析相关 ────────────────────────────────────────────────

class ColumnInfo(BaseModel):
    name: str                    # 字段名
    dtype: str                   # 数据类型
    sample_values: list          # 采样值（前 N 个非空值）
    null_count: int              # 空值数量
    unique_count: int            # 唯一值数量
    min_value: Optional[float]   # 数值字段最小值
    max_value: Optional[float]   # 数值字段最大值
    mean_value: Optional[float]  # 数值字段均值


class SheetInfo(BaseModel):
    columns: list[ColumnInfo]    # 字段信息列表
    row_count: int               # 总行数
    sample_data: str             # 采样数据文本（供 LLM 阅读）


class ParseResult(BaseModel):
    sheets: dict[str, SheetInfo]  # key 是 sheet 名
    summary: str                  # 自然语言摘要（供 LLM 快速理解数据结构）


# ─── 分析执行相关 ────────────────────────────────────────────

class AnalysisRequest(BaseModel):
    code: str                            # LLM 生成的 Python 分析代码
    file_url: str                        # Excel 文件下载链接（支持 JSON 数组字符串，多文件）
    timeout: int = 30                    # 执行超时秒数


class AnalysisResult(BaseModel):
    success: bool                        # 是否执行成功
    output: str                          # 标准输出（分析结果）
    error: Optional[str]                 # 错误信息
    code_used: str                       # 实际执行的代码
    execution_time: float                # 执行耗时（秒）
