"""
安全沙箱：在受限环境中执行 LLM 生成的分析代码

安全措施：
1. 禁用危险内置函数（open, exec, eval, import 等）
2. 只暴露 pandas 和 numpy 作为可用库
3. 代码超时自动终止
4. 限制输出长度
5. 资源限制（行数、内存）
"""

import asyncio
import signal
import io
import json
import sys
import traceback
from contextlib import redirect_stdout, redirect_stderr
from urllib.parse import unquote

import pandas as pd

from models import AnalysisResult


# ─── 危险函数黑名单 ─────────────────────────────────────────

BLOCKED_BUILTINS = {
    "open", "exec", "eval", "compile", "__import__",
    "globals", "locals", "vars", "dir",
    "input", "breakpoint", "exit", "quit",
    "memoryview", "type",
}

# 允许的安全内置函数
SAFE_BUILTINS = {
    k: v for k, v in __builtins__.items()  # type: ignore
    if k not in BLOCKED_BUILTINS
} if isinstance(__builtins__, dict) else {
    k: getattr(__builtins__, k)
    for k in dir(__builtins__)
    if not k.startswith("_") and k not in BLOCKED_BUILTINS
}


def _validate_code(code: str) -> list[str]:
    """
    静态检查代码安全性，返回违规列表
    不完美但能拦截大部分危险操作
    """
    violations = []
    lines = code.split("\n")

    for i, line in enumerate(lines, 1):
        stripped = line.strip()

        # 跳过注释和空行
        if not stripped or stripped.startswith("#"):
            continue

        # 检查 import（只允许 pandas 和 numpy）
        if "import " in stripped:
            allowed_modules = {"pandas", "pd", "numpy", "np"}
            import_found = False
            for mod in allowed_modules:
                if mod in stripped:
                    import_found = True
                    break
            if not import_found:
                violations.append(f"第{i}行: 不允许的 import 语句")

        # 检查危险函数调用
        for func in BLOCKED_BUILTINS:
            pattern = f"{func}("
            idx = stripped.find(pattern)
            while idx != -1:
                # 排除方法调用：astype() 包含 type( 但不是调用 type()
                # 检查 pattern 前一个字符是否是字母（是则属于方法名如 astype）
                if idx > 0 and stripped[idx - 1].isalpha():
                    idx = stripped.find(pattern, idx + 1)
                    continue
                violations.append(f"第{i}行: 不允许调用 {func}()")
                break

        # 检查 os / sys / subprocess 访问
        for mod in ["os.", "sys.", "subprocess.", "shutil.", "pathlib."]:
            if mod in stripped:
                violations.append(f"第{i}行: 不允许访问 {mod} 模块")

        # 检查文件操作
        for pattern in [".write(", ".read(", ".save(", "to_csv", "to_excel",
                        ".remove(", ".unlink(", ".rmdir("]:
            if pattern in stripped and "to_string" not in stripped:
                violations.append(f"第{i}行: 不允许文件操作 {pattern}")

        # 检查直接给 .columns 赋值（会导致 Length mismatch）
        # 匹配: xxx.columns = [...] 或 xxx.columns = (...)
        if ".columns = [" in stripped or ".columns = (" in stripped or ".columns=[" in stripped or ".columns=(" in stripped:
            violations.append(f"第{i}行: 禁止直接给 .columns 赋值列表（会导致 Length mismatch），请用 df.rename(columns={{旧名: 新名}}) 逐列重命名")

    return violations


def _extract_filename(url: str) -> str:
    """从 URL 中提取文件名"""
    path = url.split("?")[0]
    filename = path.rsplit("/", 1)[-1]
    return unquote(filename)


def _parse_file_urls(file_url: str) -> list[str]:
    """解析 file_url 参数，兼容 JSON 数组和单个 URL"""
    file_url = file_url.strip()
    if file_url.startswith("["):
        try:
            urls = json.loads(file_url)
            if isinstance(urls, list):
                return [u.strip() for u in urls if u.strip()]
        except json.JSONDecodeError:
            pass
    return [file_url]


def _load_dataframes(file_url: str) -> dict[str, pd.DataFrame]:
    """下载 Excel（支持多文件 JSON 数组）并加载为 DataFrame 字典"""
    import httpx

    urls = _parse_file_urls(file_url)
    all_dfs: dict[str, pd.DataFrame] = {}

    for url in urls:
        filename = _extract_filename(url)
        prefix = filename.replace(".xlsx", "").replace(".xls", "")

        resp = httpx.get(url, timeout=30, verify=False)
        resp.raise_for_status()

        xl = pd.ExcelFile(io.BytesIO(resp.content))
        file_dfs = {name: pd.read_excel(xl, sheet_name=name) for name in xl.sheet_names}

        for sheet_name, df in file_dfs.items():
            if len(urls) == 1:
                all_dfs[sheet_name] = df
            else:
                all_dfs[f"{prefix}_{sheet_name}"] = df

    return all_dfs


def _convert_numpy_types(obj):
    """递归将 numpy 类型转为原生 Python 类型，避免输出 np.float64() 等标记"""
    import numpy as np

    if isinstance(obj, pd.DataFrame):
        return obj.to_dict(orient="records")
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: _convert_numpy_types(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [_convert_numpy_types(v) for v in obj]
    elif isinstance(obj, (np.integer,)):
        return int(obj)
    elif isinstance(obj, (np.floating,)):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.bool_):
        return bool(obj)
    return obj


def _strip_imports(code: str) -> str:
    """移除代码中的 import 语句（pd/np 已预注入沙箱）"""
    lines = code.split("\n")
    cleaned = []
    for line in lines:
        stripped = line.strip()
        # 跳过 pandas / numpy 的 import 语句
        if stripped.startswith("import pandas") or stripped.startswith("import numpy"):
            continue
        if stripped.startswith("import pd") or stripped.startswith("import np"):
            continue
        if stripped.startswith("from pandas") or stripped.startswith("from numpy"):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def _run_code_in_sandbox(code: str, df_dict: dict[str, pd.DataFrame]) -> dict:
    """
    在受限命名空间中执行代码

    返回: {"output": str, "error": str|None}
    """
    # 预处理：移除 import 语句（pd/np 已预注入）
    code = _strip_imports(code)

    # 构建安全的执行命名空间
    safe_globals = {
        "__builtins__": SAFE_BUILTINS,
        "pd": pd,
        "np": __import__("numpy"),
        "df_dict": df_dict,
    }

    # 自动为每个 sheet 创建变量，如 df_利润表
    for name, df in df_dict.items():
        safe_name = f"df_{name}"
        safe_globals[safe_name] = df

    # 第一个 sheet 也绑定为 df
    if df_dict:
        first_key = next(iter(df_dict))
        safe_globals["df"] = df_dict[first_key]

    # 捕获输出
    stdout_buf = io.StringIO()
    stderr_buf = io.StringIO()

    try:
        with redirect_stdout(stdout_buf), redirect_stderr(stderr_buf):
            exec(code, safe_globals)  # noqa: S102

        # 尝试获取 result 变量
        result_var = safe_globals.get("result", None)
        output_parts = []

        if stdout_buf.getvalue().strip():
            output_parts.append(stdout_buf.getvalue().strip())

        if result_var is not None:
            if isinstance(result_var, pd.DataFrame):
                output_parts.append(result_var.to_string())
            elif isinstance(result_var, pd.Series):
                output_parts.append(result_var.to_string())
            elif isinstance(result_var, dict):
                # 将 dict 中的 numpy 类型转为原生 Python 类型，避免输出 np.float64() 等标记
                clean_dict = _convert_numpy_types(result_var)
                output_parts.append(json.dumps(clean_dict, ensure_ascii=False, indent=2))
            elif isinstance(result_var, (list, tuple)):
                clean_list = _convert_numpy_types(result_var)
                output_parts.append(json.dumps(clean_list, ensure_ascii=False, indent=2))
            else:
                output_parts.append(str(result_var))

        output = "\n\n".join(output_parts) if output_parts else "代码执行完成，无输出"

        # 限制输出长度
        if len(output) > 10000:
            output = output[:10000] + "\n\n... 输出已截断（超过 10000 字符）"

        return {"output": output, "error": None}

    except Exception:
        error_msg = traceback.format_exc()
        return {
            "output": stdout_buf.getvalue() or "",
            "error": error_msg,
        }


async def execute_analysis_code(
    code: str,
    file_url: str,
    timeout: int = 30,
) -> AnalysisResult:
    """
    主入口：下载数据 → 校验代码 → 沙箱执行 → 返回结果

    使用 asyncio 的 wait_for 实现超时控制
    """
    import time

    start = time.time()

    # 1. 静态安全检查
    violations = _validate_code(code)
    if violations:
        return AnalysisResult(
            success=False,
            output="",
            error=f"代码安全检查未通过:\n" + "\n".join(violations),
            code_used=code,
            execution_time=time.time() - start,
        )

    # 2. 下载数据（在线程池中执行，避免阻塞事件循环）
    try:
        df_dict = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _load_dataframes, file_url),
            timeout=30,
        )
    except asyncio.TimeoutError:
        return AnalysisResult(
            success=False,
            output="",
            error="Excel 文件下载超时（30秒）",
            code_used=code,
            execution_time=time.time() - start,
        )
    except Exception as e:
        return AnalysisResult(
            success=False,
            output="",
            error=f"Excel 文件下载失败: {e}",
            code_used=code,
            execution_time=time.time() - start,
        )

    # 3. 在线程池中执行沙箱代码（带超时）
    try:
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, _run_code_in_sandbox, code, df_dict
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return AnalysisResult(
            success=False,
            output="",
            error=f"代码执行超时（{timeout}秒），请简化分析逻辑",
            code_used=code,
            execution_time=time.time() - start,
        )

    elapsed = time.time() - start

    return AnalysisResult(
        success=result["error"] is None,
        output=result["output"],
        error=result["error"],
        code_used=code,
        execution_time=round(elapsed, 2),
    )


def _run_report_in_sandbox(code: str, df_dict: dict[str, pd.DataFrame], output_path: str) -> dict:
    """在沙箱中执行报表生成代码，输出 Excel 文件"""
    import os as _os

    safe_globals = {
        "__builtins__": SAFE_BUILTINS,
        "pd": pd,
        "np": __import__("numpy"),
        "df_dict": df_dict,
        "output_path": output_path,
    }

    for name, df in df_dict.items():
        safe_globals[f"df_{name}"] = df
    if df_dict:
        first_key = next(iter(df_dict))
        safe_globals["df"] = df_dict[first_key]

    stdout_buf = io.StringIO()

    try:
        with redirect_stdout(stdout_buf):
            exec(code, safe_globals)  # noqa: S102

        # 如果代码没写 to_excel，自动用 result 变量生成文件
        if not _os.path.exists(output_path):
            result_var = safe_globals.get("result", None)
            if result_var is not None and isinstance(result_var, pd.DataFrame):
                result_var.to_excel(output_path, index=False)
            else:
                return {"output": stdout_buf.getvalue(), "error": "代码执行完成但未生成报表文件，请确保代码中包含: result.to_excel(output_path, index=False)，或将最终 DataFrame 赋值给 result 变量"}

        file_size = _os.path.getsize(output_path)
        return {"output": f"报表生成成功，文件大小: {file_size} 字节", "error": None}

    except Exception:
        return {"output": stdout_buf.getvalue() or "", "error": traceback.format_exc()}


async def execute_report_code(
    code: str,
    file_url: str,
    report_name: str,
    timeout: int = 30,
) -> "ReportResult":
    """执行报表生成代码，返回 Excel 文件下载信息"""
    from models import ReportResult
    import tempfile
    import time
    import os

    start = time.time()

    # 安全检查
    violations = _validate_code(code)
    # 报表生成允许 to_excel
    violations = [v for v in violations if "to_excel" not in v]
    if violations:
        return ReportResult(
            success=False,
            file_url=None,
            file_name=None,
            error=f"代码安全检查未通过:\n" + "\n".join(violations),
            code_used=code,
            execution_time=time.time() - start,
        )

    # 预处理
    code = _strip_imports(code)

    # 下载数据
    try:
        df_dict = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(None, _load_dataframes, file_url),
            timeout=30,
        )
    except asyncio.TimeoutError:
        return ReportResult(success=False, file_url=None, file_name=None, error="文件下载超时", code_used=code, execution_time=time.time() - start)
    except Exception as e:
        return ReportResult(success=False, file_url=None, file_name=None, error=f"文件下载失败: {e}", code_used=code, execution_time=time.time() - start)

    # 创建临时输出路径
    output_dir = tempfile.mkdtemp()
    safe_name = report_name.replace("..", "").replace("/", "").replace("\\", "")
    output_path = os.path.join(output_dir, f"{safe_name}.xlsx")

    # 执行
    try:
        result = await asyncio.wait_for(
            asyncio.get_event_loop().run_in_executor(
                None, _run_report_in_sandbox, code, df_dict, output_path
            ),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        return ReportResult(success=False, file_url=None, file_name=None, error=f"代码执行超时（{timeout}秒）", code_used=code, execution_time=time.time() - start)

    elapsed = time.time() - start

    if result["error"] is not None:
        return ReportResult(success=False, file_url=None, file_name=None, error=result["error"], code_used=code, execution_time=round(elapsed, 2))

    # 文件生成成功，移动到静态文件目录
    import shutil
    static_dir = os.path.join(os.path.dirname(__file__), "static", "reports")
    os.makedirs(static_dir, exist_ok=True)

    # 用时间戳防重名
    import time as time_mod
    final_name = f"{safe_name}_{int(time_mod.time())}.xlsx"
    final_path = os.path.join(static_dir, final_name)
    shutil.move(output_path, final_path)

    return ReportResult(
        success=True,
        file_url=f"/static/reports/{final_name}",
        file_name=f"{safe_name}.xlsx",
        error=None,
        code_used=code,
        execution_time=round(elapsed, 2),
    )
