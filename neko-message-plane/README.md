# neko-message-plane

基于 ZeroMQ 的消息平面服务,用于 N.E.K.O 插件系统的进程间通信。

## 功能特性

- 基于 ZeroMQ 的高性能消息传递
- 支持 MessagePack 序列化
- 多线程并发处理
- 消息路由和转发

## 编译

### 前置要求

- Rust 工具链 (推荐使用 rustup)
- ZeroMQ 库

### 编译可执行文件

```bash
# Debug 模式
cargo build

# Release 模式 (推荐)
cargo build --release
```

编译后的可执行文件位于:
- Debug: `target/debug/neko-message-plane`
- Release: `target/release/neko-message-plane`

## 运行

```bash
# 直接运行
cargo run --release

# 或运行编译后的二进制文件
./target/release/neko-message-plane
```

## 项目结构

- `src/main.rs` - 主入口
- `src/config.rs` - 配置管理
- `src/types.rs` - 类型定义
- `src/store.rs` - 消息存储
- `src/handlers.rs` - 消息处理器
- `src/utils.rs` - 工具函数

## 注意事项

本项目是独立的可执行程序,不生成 Python wheel 包。如需 Python 绑定,请使用 `neko_message_plane_wheel` 项目。
