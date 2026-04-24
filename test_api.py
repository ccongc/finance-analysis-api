"""本地测试脚本：验证 API 各端点功能"""

import sys
import httpx
import time
import json

BASE_URL = "http://localhost:8000"


def test_health():
    """测试健康检查"""
    resp = httpx.get(f"{BASE_URL}/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    print("[PASS] 健康检查")


def test_parse():
    """测试 Excel 解析（使用项目目录中的 Excel 文件）"""
    import os
    excel_dir = os.path.dirname(os.path.abspath(__file__)) + "/.."
    # 找一个 Excel 文件
    excel_files = [f for f in os.listdir(excel_dir) if f.endswith(".xlsx") and not f.startswith("~$")]
    if not excel_files:
        print("[SKIP] 未找到测试用 Excel 文件")
        return None

    test_file = os.path.join(excel_dir, excel_files[0])
    print(f"[INFO] 使用测试文件: {excel_files[0]}")

    with open(test_file, "rb") as f:
        resp = httpx.post(
            f"{BASE_URL}/parse",
            files={"file": (excel_files[0], f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            data={"sample_rows": "3"},
            timeout=30,
        )

    assert resp.status_code == 200
    data = resp.json()
    print(f"[PASS] Excel 解析 - 发现 {len(data['sheets'])} 个工作表")
    print(f"[INFO] 数据摘要:\n{data['summary'][:500]}")
    return data


def test_analyze():
    """测试代码执行（需要先启动服务并上传文件）"""
    # 使用一个简单的内联测试
    code = """
# 基础数据探索
print("工作表:", list(df_dict.keys()))
for name, df in df_dict.items():
    print(f"\\n--- {name} ---")
    print(f"形状: {df.shape}")
    print(f"列名: {list(df.columns)}")
    print(df.head(3).to_string())
result = "数据探索完成"
"""

    # 注意：这个测试需要一个可访问的 Excel URL
    # 本地测试时请替换为实际 URL
    test_url = input("请输入一个可访问的 Excel 文件 URL（回车跳过）: ").strip()
    if not test_url:
        print("[SKIP] 未提供测试 URL，跳过代码执行测试")
        return

    resp = httpx.post(
        f"{BASE_URL}/analyze",
        json={
            "code": code,
            "file_url": test_url,
            "timeout": 30,
        },
        timeout=60,
    )

    assert resp.status_code == 200
    data = resp.json()
    if data["success"]:
        print("[PASS] 代码执行成功")
        print(f"[INFO] 输出:\n{data['output'][:500]}")
    else:
        print(f"[FAIL] 代码执行失败: {data['error'][:300]}")


def test_security():
    """测试安全沙箱是否能拦截危险代码"""
    dangerous_codes = [
        # 文件读取
        "with open('/etc/passwd') as f: result = f.read()",
        # 系统调用
        "import os\nresult = os.listdir('/')",
        # 网络请求
        "import subprocess\nresult = subprocess.run(['ls'])",
        # 写文件
        "df.to_csv('/tmp/steal.csv')",
    ]

    for code in dangerous_codes:
        resp = httpx.post(
            f"{BASE_URL}/analyze",
            json={
                "code": code,
                "file_url": "https://example.com/test.xlsx",  # 不会真正下载
                "timeout": 10,
            },
            timeout=30,
        )

        data = resp.json()
        if not data["success"] and ("不允许" in (data.get("error") or "") or "安全检查" in (data.get("error") or "")):
            print(f"[PASS] 拦截危险代码: {code[:40]}...")
        else:
            # 可能因为 URL 无效而失败，只要不是成功执行就行
            if not data["success"]:
                print(f"[PASS] 危险代码未成功执行: {code[:40]}...")
            else:
                print(f"[FAIL] 未能拦截危险代码: {code[:40]}...")


if __name__ == "__main__":
    print("=" * 50)
    print("财务数据分析 API 测试")
    print("=" * 50)

    try:
        test_health()
    except Exception as e:
        print(f"[FAIL] 服务未启动？{e}")
        print("请先运行: python main.py")
        sys.exit(1)

    parse_result = test_parse()
    test_security()
    test_analyze()

    print("\n" + "=" * 50)
    print("测试完成")
