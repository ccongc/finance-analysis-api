"""Excel 解析模块：提取表头、字段统计、采样数据"""

import io
from typing import Optional

import pandas as pd

from models import ColumnInfo, SheetInfo, ParseResult


def _analyze_column(series: pd.Series, sample_rows: int) -> ColumnInfo:
    """分析单个字段，提取统计信息"""
    non_null = series.dropna()

    info = ColumnInfo(
        name=str(series.name),
        dtype=str(series.dtype),
        sample_values=non_null.head(sample_rows).tolist(),
        null_count=int(series.isnull().sum()),
        unique_count=int(series.nunique()),
        min_value=None,
        max_value=None,
        mean_value=None,
    )

    # 数值型字段补充统计
    if pd.api.types.is_numeric_dtype(series):
        info.min_value = float(series.min()) if not series.isnull().all() else None
        info.max_value = float(series.max()) if not series.isnull().all() else None
        info.mean_value = float(series.mean()) if not series.isnull().all() else None

    return info


def _build_summary(sheets: dict[str, SheetInfo]) -> str:
    """生成数据结构的自然语言摘要，供 LLM 快速理解"""
    lines = []
    for name, sheet in sheets.items():
        lines.append(f"工作表「{name}」: {sheet.row_count} 行")
        for col in sheet.columns:
            desc = f"  - {col.name} ({col.dtype})"
            if col.unique_count <= 10 and col.unique_count > 0:
                desc += f", 取值: {col.sample_values[:10]}"
            elif col.min_value is not None:
                desc += f", 范围: {col.min_value:.2f} ~ {col.max_value:.2f}, 均值: {col.mean_value:.2f}"
            if col.null_count > 0:
                desc += f", 空值: {col.null_count}"
            lines.append(desc)
    return "\n".join(lines)


def parse_excel_bytes(content: bytes, sample_rows: int = 5) -> ParseResult:
    """从字节数据解析 Excel"""
    xl = pd.ExcelFile(io.BytesIO(content))
    return _parse(xl, sample_rows)


async def parse_excel_from_url(url: str, sample_rows: int = 5) -> ParseResult:
    """从 URL 下载并解析 Excel"""
    import httpx

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    xl = pd.ExcelFile(io.BytesIO(resp.content))
    return _parse(xl, sample_rows)


def _parse(xl: pd.ExcelFile, sample_rows: int) -> ParseResult:
    """核心解析逻辑"""
    sheets: dict[str, SheetInfo] = {}

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name)

        # 跳过完全空的 sheet
        if df.empty:
            continue

        # 去除完全为空的列
        df = df.dropna(axis=1, how="all")

        columns = [_analyze_column(df[col], sample_rows) for col in df.columns]

        sheets[sheet_name] = SheetInfo(
            columns=columns,
            row_count=len(df),
            # 采样数据转为对齐的文本表格，LLM 更容易阅读
            sample_data=df.head(sample_rows).to_string(index=True),
        )

    summary = _build_summary(sheets)

    return ParseResult(sheets=sheets, summary=summary)
