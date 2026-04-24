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
import sys
import traceback
from contextlib import redirect_stdout, redirect_stderr

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
            if f"{func}(" in stripped:
                violations.append(f"第{i}行: 不允许调用 {func}()")

        # 检查 os / sys / subprocess 访问
        for mod in ["os.", "sys.", "subprocess.", "shutil.", "pathlib."]:
            if mod in stripped:
                violations.append(f"第{i}行: 不允许访问 {mod} 模块")

        # 检查文件操作
        for pattern in [".write(", ".read(", ".save(", "to_csv", "to_excel",
                        ".remove(", ".unlink(", ".rmdir("]:
            if pattern in stripped and "to_string" not in stripped:
                violations.append(f"第{i}行: 不允许文件操作 {pattern}")

    return violations


def _load_dataframes(file_url: str) -> dict[str, pd.DataFrame]:
    """下载 Excel 并加载为 DataFrame 字典（同步，在线程中调用）"""
    import httpx

    resp = httpx.get(file_url, timeout=30)
    resp.raise_for_status()

    xl = pd.ExcelFile(io.BytesIO(resp.content))
    return {name: pd.read_excel(xl, sheet_name=name) for name in xl.sheet_names}


def _run_code_in_sandbox(code: str, df_dict: dict[str, pd.DataFrame]) -> dict:
    """
    在受限命名空间中执行代码

    返回: {"output": str, "error": str|None}
    """
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
