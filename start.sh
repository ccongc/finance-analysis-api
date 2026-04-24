#!/bin/bash
# 本地启动脚本

echo "安装依赖..."
pip install -r requirements.txt

echo "启动服务..."
python main.py
