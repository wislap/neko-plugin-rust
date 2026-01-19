# neko_plugin_cli

N.E.K.O 插件管理命令行工具,支持插件打包、安装和管理。

## 功能特性

- 插件打包和压缩
- 插件安装和卸载
- 终端 UI 界面 (基于 ratatui)
- 支持 Python 绑定 (可选)

## 编译

### 前置要求

- Rust 工具链 (推荐使用 rustup)
- Python 3.8+ (如需 Python 绑定)
- maturin (如需构建 wheel)

### 编译可执行文件

```bash
# Debug 模式
cargo build

# Release 模式 (推荐)
cargo build --release
```

编译后的可执行文件位于:
- Debug: `target/debug/neko_plugin_cli`
- Release: `target/release/neko_plugin_cli`

### 编译 Python 绑定

本项目支持同时编译为可执行文件和 Python 库。

#### 安装 maturin

```bash
pip install maturin
# 或使用 uv
uv pip install maturin
```

#### 构建开发版本

```bash
# 开发模式安装 (启用 Python 功能)
maturin develop --features python

# 或使用 uv
uv run maturin develop --features python --release
```

#### 构建 wheel 包

```bash
# 构建 wheel 包 (启用 Python 功能)
maturin build --release --features python

# 构建后的 wheel 文件位于 target/wheels/ 目录
```

### 导出 wheel 给 uv 使用

#### 方法 1: 直接安装到虚拟环境

```bash
# 构建并安装
maturin build --release --features python
uv pip install target/wheels/neko_plugin_cli-*.whl
```

#### 方法 2: 使用 maturin develop

```bash
# 在开发环境中直接安装 (推荐用于开发)
uv run maturin develop --release --features python
```

#### 方法 3: 发布到本地目录

```bash
# 构建 wheel
maturin build --release --features python -o dist/

# 从本地目录安装
uv pip install dist/neko_plugin_cli-*.whl
```

## 运行

### 作为命令行工具

```bash
# 直接运行
cargo run --release

# 或运行编译后的二进制文件
./target/release/neko_plugin_cli --help
```

### 作为 Python 库

```python
import neko_plugin_cli

# 使用库的功能
# TODO: 添加具体的使用示例
```

## 项目结构

- `src/main.rs` - 命令行入口
- `src/lib.rs` - Python 模块入口
- `Cargo.toml` - Rust 项目配置

## Features 说明

- `default` - 默认功能,仅编译命令行工具
- `python` - 启用 Python 绑定,支持构建为 Python 库

## 开发提示

- 命令行工具开发: 使用 `cargo build` 和 `cargo run`
- Python 库开发: 使用 `maturin develop --features python`
- 发布前使用 `maturin build --release --features python` 构建优化版本
