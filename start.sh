#!/bin/bash

# 检查配置文件
if [ ! -f "config.yaml" ]; then
    echo "Generating default config..."
    python run.py init-config
fi

# 检查日志目录
mkdir -p logs

# 启动服务
if [ "$1" == "dev" ]; then
    echo "Starting in development mode..."
    python run.py dev --config config.yaml
else
    echo "Starting in production mode..."
    python run.py start --config config.yaml
fi
