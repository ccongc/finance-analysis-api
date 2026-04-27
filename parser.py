"""Excel 解析模块：提取表头、字段统计、采样数据，支持多文件"""

import io
import json
from urllib.parse import unquote
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
        if sheet.row_count == 0 and sheet.columns:
            # 空报表：只有表头没有数据，标记为目标输出模板
            lines.append(f"工作表「{name}」: ⚠️ 空报表模板（0 行数据，仅有表头），这是用户期望的输出格式")
            lines.append(f"  目标输出字段: {[col.name for col in sheet.columns]}")
        else:
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


def _extract_filename(url: str) -> str:
    """从 URL 中提取文件名（去除查询参数和解码中文）"""
    path = url.split("?")[0]
    filename = path.rsplit("/", 1)[-1]
    return unquote(filename)


def parse_excel_bytes(content: bytes, sample_rows: int = 5) -> ParseResult:
    """从字节数据解析 Excel"""
    xl = pd.ExcelFile(io.BytesIO(content))
    return _parse(xl, sample_rows)


async def parse_excel_from_url(url: str, sample_rows: int = 5) -> ParseResult:
    """从单个 URL 下载并解析 Excel"""
    import httpx

    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        resp = await client.get(url)
        resp.raise_for_status()

    xl = pd.ExcelFile(io.BytesIO(resp.content))
    return _parse(xl, sample_rows)


async def parse_multiple_from_urls(file_url: str, sample_rows: int = 5) -> ParseResult:
    """
    从多个 URL 下载并解析 Excel，合并结果

    file_url 支持以下格式：
    1. JSON 数组字符串：'["url1", "url2"]'
    2. 单个 URL 字符串：'https://...'
    """
    urls = _parse_file_urls(file_url)

    all_sheets: dict[str, SheetInfo] = {}

    for i, url in enumerate(urls):
        filename = _extract_filename(url)
        prefix = filename.replace(".xlsx", "").replace(".xls", "")

        try:
            result = await parse_excel_from_url(url, sample_rows)

            if not result.sheets:
                # 文件下载成功但无有效 sheet（如纯空文件）
                all_sheets[f"_空文件_{prefix}"] = SheetInfo(
                    columns=[],
                    row_count=0,
                    sample_data=f"文件 {filename} 无有效工作表",
                )
                continue

            for sheet_name, sheet_info in result.sheets.items():
                if len(urls) == 1:
                    final_name = sheet_name
                else:
                    final_name = f"{prefix}_{sheet_name}"
                all_sheets[final_name] = sheet_info
        except Exception as e:
            all_sheets[f"_错误_{prefix}"] = SheetInfo(
                columns=[],
                row_count=0,
                sample_data=f"文件解析失败: {e}",
            )

    summary = _build_summary(all_sheets)
    return ParseResult(sheets=all_sheets, summary=summary)


def _parse_file_urls(file_url: str) -> list[str]:
    """解析 file_url 参数，兼容 JSON 数组和单个 URL"""
    file_url = file_url.strip()

    # 尝试解析为 JSON 数组
    if file_url.startswith("["):
        try:
            urls = json.loads(file_url)
            if isinstance(urls, list):
                return [u.strip() for u in urls if u.strip()]
        except json.JSONDecodeError:
            pass

    # 单个 URL
    return [file_url]


def _parse(xl: pd.ExcelFile, sample_rows: int) -> ParseResult:
    """核心解析逻辑"""
    sheets: dict[str, SheetInfo] = {}

    for sheet_name in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sheet_name)

        # 空报表模板：有列（表头）但 0 行数据
        # 必须在 dropna 之前检测，否则空 DataFrame 的列会被全部删掉
        if len(df) == 0 and len(df.columns) > 0:
            columns = [ColumnInfo(
                name=str(col),
                dtype="object",
                sample_values=[],
                null_count=0,
                unique_count=0,
                min_value=None,
                max_value=None,
                mean_value=None,
            ) for col in df.columns]
            sheets[sheet_name] = SheetInfo(
                columns=columns,
                row_count=0,
                sample_data="（空报表模板，仅有表头）",
            )
            continue

        # 去除完全为空的列
        df = df.dropna(axis=1, how="all")

        # 跳过没有任何有效列和数据的 sheet
        if df.empty:
            continue

        columns = [_analyze_column(df[col], sample_rows) for col in df.columns]

        sheets[sheet_name] = SheetInfo(
            columns=columns,
            row_count=len(df),
            sample_data=df.head(sample_rows).to_string(index=True),
        )

    summary = _build_summary(sheets)

    return ParseResult(sheets=sheets, summary=summary)
