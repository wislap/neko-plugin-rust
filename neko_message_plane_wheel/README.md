# neko_message_plane_wheel

N.E.K.O 消息平面的 Python 绑定库,使用 PyO3 构建。

## 功能特性

- 提供 Python 接口访问消息平面功能
- 使用 PyO3 实现高性能 Rust-Python 互操作
- 支持导出为 Python wheel 包

## 编译

### 前置要求

- Rust 工具链 (推荐使用 rustup)
- Python 3.8+
- maturin (用于构建 Python wheel)

### 安装 maturin

```bash
pip install maturin
# 或使用 uv
uv pip install maturin
```

### 构建开发版本

```bash
# 开发模式安装 (可编辑安装)
maturin develop

# 或使用 uv
uv run maturin develop
```

### 构建 wheel 包

```bash
# 构建 wheel 包
maturin build --release

# 构建后的 wheel 文件位于 target/wheels/ 目录
```

### 导出 wheel 给 uv 使用

#### 方法 1: 直接安装到虚拟环境

```bash
# 构建并安装
maturin build --release
uv pip install target/wheels/neko_message_plane_wheel-*.whl
```

#### 方法 2: 使用 maturin develop

```bash
# 在开发环境中直接安装 (推荐用于开发)
uv run maturin develop --release
```

#### 方法 3: 发布到本地目录

```bash
# 构建 wheel
maturin build --release -o dist/

# 从本地目录安装
uv pip install dist/neko_message_plane_wheel-*.whl
```

## 使用示例

```python
import neko_message_plane_wheel

# 使用库的功能
# TODO: 添加具体的使用示例
```

## 项目结构

- `src/lib.rs` - Python 模块入口
- `Cargo.toml` - Rust 项目配置

## 开发提示

- 修改代码后使用 `maturin develop` 快速测试
- 发布前使用 `maturin build --release` 构建优化版本
- wheel 文件命名格式: `neko_message_plane_wheel-{version}-{python_tag}-{abi_tag}-{platform_tag}.whl`
