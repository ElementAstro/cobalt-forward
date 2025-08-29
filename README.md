# Cobalt Forward

高性能TCP-WebSocket转发服务，支持热重载和实时监控。

## 特性

- 🚀 **高性能转发** - TCP到WebSocket的双向数据转发
- 📊 **实时监控** - 连接状态、性能指标、健康检查
- 🔄 **热重载** - 配置文件变化自动重载，无需重启
- 🎯 **命令调度** - 灵活的命令处理和路由系统
- 🔌 **插件系统** - 支持动态加载和管理插件
- 🌐 **HTTP API** - RESTful API管理接口
- 📝 **配置管理** - 支持YAML/JSON格式配置
- 📋 **完整测试** - 高测试覆盖率，保证代码质量

## 快速开始

### 1. 安装依赖
```bash
# 使用 uv（推荐）
uv install

# 或使用 pip
pip install -r requirements.txt
```

### 2. 生成配置文件
```bash
python -m cobalt_forward init-config --output config.yaml
```

### 3. 启动服务
```bash
# 生产模式
python -m cobalt_forward start --config config.yaml

# 开发模式（支持热重载）
python -m cobalt_forward dev --port 8000
```

### 4. 验证服务
```bash
python -m cobalt_forward health-check
```

## 测试覆盖

项目具备完整的测试覆盖，包括：

- ✅ **主入口测试** - CLI命令和应用启动
- ✅ **API层测试** - FastAPI应用和路由
- ✅ **日志系统测试** - 日志设置和管理器
- ✅ **命令调度测试** - 命令处理和调度
- ✅ **插件系统测试** - 插件管理和生命周期
- ✅ **配置监控测试** - 文件监控和热重载
- ✅ **核心服务测试** - 所有核心组件和接口

### 运行测试
```bash
# 运行所有测试
pytest

# 带覆盖率报告
pytest --cov=cobalt_forward --cov-report=html

# 运行特定测试
pytest tests/test_main.py -v
```

## 项目文档

详细文档位于 `doc/` 目录：

- 📋 **[项目文档](doc/项目文档.md)** - 完整的功能说明和使用指南
- 🔍 **[分析结果](doc/分析结果.md)** - 代码架构和质量分析
- 📚 **[函数文档](doc/函数文档.md)** - 详细的函数调用链和API文档

## 开发指南

### 项目结构
```
cobalt_forward/
├── application/          # 应用层
├── core/                # 核心层
├── infrastructure/      # 基础设施层
├── presentation/        # 表现层
├── plugins/             # 插件系统
└── main.py              # 主入口
```

### 代码质量
```bash
# 类型检查
mypy cobalt_forward

# 代码格式化
black cobalt_forward tests
isort cobalt_forward tests

# 代码质量检查
ruff cobalt_forward tests
```
