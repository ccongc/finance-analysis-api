"""
财务数据分析 API 服务
提供 Excel 解析、动态分析代码执行、健康检查三个端点
供 FastGPT 工作流调用
"""

from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from typing import Optional
import uvicorn
import hashlib
import time
import os

from parser import parse_excel_bytes, parse_excel_from_url, parse_multiple_from_urls
from sandbox import execute_analysis_code, execute_report_code
from models import AnalysisRequest, ParseResult, AnalysisResult, ReportRequest, ReportResult


# ─── 解析结果缓存 ───────────────────────────────────────────
# 按 file_url 哈希缓存，同一文件不重复解析，追问时直接返回
_cache: dict[str, dict] = {}
_CACHE_TTL = 1800  # 缓存有效期 30 分钟


def _cache_key(file_url: str) -> str:
    return hashlib.md5(file_url.encode()).hexdigest()


def _get_cached(key: str) -> Optional[dict]:
    entry = _cache.get(key)
    if entry and time.time() - entry["time"] < _CACHE_TTL:
        return entry["data"]
    if entry:
        del _cache[key]
    return None


def _set_cached(key: str, data: dict):
    _cache[key] = {"data": data, "time": time.time()}
    # 限制缓存大小
    if len(_cache) > 100:
        oldest = min(_cache.items(), key=lambda x: x[1]["time"])
        del _cache[oldest[0]]

app = FastAPI(
    title="财务数据分析 API",
    description="供 FastGPT 智能问数工作流调用，支持 Excel 解析和动态代码执行",
    version="1.0.0",
)

# 允许跨域，FastGPT 云版需要
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── 健康检查 ───────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "finance-analysis-api"}


# ─── Excel 解析 ─────────────────────────────────────────────

@app.post("/parse", response_model=ParseResult)
async def parse_upload(
    file: UploadFile = File(..., description="Excel 文件"),
    sample_rows: int = 5,
):
    """解析上传的 Excel 文件，返回表头、字段统计、采样数据"""
    content = await file.read()
    if len(content) > 10 * 1024 * 1024:  # 10MB 上限
        raise HTTPException(400, "文件超过 10MB 限制")
    try:
        result = parse_excel_bytes(content, sample_rows)
        return result
    except Exception as e:
        raise HTTPException(400, f"Excel 解析失败: {e}")


class ParseByUrlBody(BaseModel):
    file_url: str
    sample_rows: int = 5
    timeout: int = 30


@app.post("/parseByUrl", response_model=ParseResult)
async def parse_by_url(body: ParseByUrlBody):
    """通过 URL 下载并解析 Excel（适配 FastGPT 传入 userFileUrl，支持多文件 JSON 数组，带缓存）"""
    try:
        # 查缓存
        key = _cache_key(body.file_url)
        cached = _get_cached(key)
        if cached:
            return ParseResult(**cached)

        result = await parse_multiple_from_urls(body.file_url, body.sample_rows, timeout=body.timeout)

        # 写缓存
        _set_cached(key, result.dict())

        return result
    except Exception as e:
        raise HTTPException(400, f"Excel 解析失败: {e}")


# ─── 代码执行 ───────────────────────────────────────────────

@app.post("/analyze", response_model=AnalysisResult)
async def analyze(body: AnalysisRequest):
    """
    在安全沙箱中执行分析代码

    - code: LLM 生成的 Pandas 分析代码
    - file_url: Excel 文件下载链接
    - timeout: 执行超时秒数，默认 30
    """
    if len(body.code) > 5000:
        raise HTTPException(400, "代码超过 5000 字符限制")
    try:
        result = await execute_analysis_code(
            code=body.code,
            file_url=body.file_url,
            timeout=body.timeout,
        )
        return result
    except Exception as e:
        raise HTTPException(500, f"代码执行失败: {e}")


# ─── 报表生成 ───────────────────────────────────────────────

@app.post("/generateReport", response_model=ReportResult)
async def generate_report(body: ReportRequest):
    """
    生成 Excel 报表并返回下载链接

    - code: LLM 生成的报表构建代码（使用 pandas，最终 result 为 DataFrame）
    - file_url: 源数据 Excel 下载链接
    - report_name: 报表文件名
    - timeout: 执行超时秒数，默认 30
    """
    if len(body.code) > 5000:
        raise HTTPException(400, "代码超过 5000 字符限制")
    try:
        result = await execute_report_code(
            code=body.code,
            file_url=body.file_url,
            report_name=body.report_name,
            timeout=body.timeout,
        )
        # 把相对路径转为完整 URL
        if result.success and result.file_url:
            host = os.environ.get("RAILWAY_PUBLIC_DOMAIN", "")
            if host:
                result.file_url = f"https://{host}{result.file_url}"
            else:
                # 本地开发
                result.file_url = f"http://localhost:{os.environ.get('PORT', '8000')}{result.file_url}"
        return result
    except Exception as e:
        raise HTTPException(500, f"报表生成失败: {e}")


# ─── 静态文件（报表下载） ──────────────────────────────────

# 确保静态目录存在
_reports_dir = os.path.join(os.path.dirname(__file__), "static", "reports")
os.makedirs(_reports_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=os.path.join(os.path.dirname(__file__), "static")), name="static")


# ─── 启动入口 ───────────────────────────────────────────────

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
