# 贡献者指南

项目主要面向中国用户，用户可见文案、文档和代码注释优先使用中文；协议字段、配置项、日志 `event`、依赖 feature 和代码标识保持英文原文

## 改动边界

- `src/` 包含完整 Rust 应用：订阅 API、WebSocket 监听、订阅匹配、Bark 推送和 Web 页面路由
- `web/index.html` 是唯一 Web 界面源文件，通过 `include_str!` 编译进二进制
- 仓库不维护特定平台的反向代理、进程守护或静态托管配置

服务端行为和 Web 交互尽量分开改，跨层改动需要说明数据流如何变化

## 本地检查

开发入口：

```bash
cp .env.example .env
set -a; . ./.env; set +a
cargo run
```

提交前至少跑：

```bash
cargo fmt --check
cargo check
cargo test
```

如果改动涉及依赖、并发、错误处理、HTTP/WebSocket 或共享模型，也跑：

```bash
cargo clippy --all-targets --all-features
```

## Rust 和依赖

`Cargo.toml` 已启用严格 lint，新增代码不要使用 `unwrap()`、`expect()`、`dbg!()`、`println!()`、`todo!()`、`unimplemented!()`，也不要引入 `unsafe`

新增或升级依赖时，默认保持 `default-features = false`，只开启实际用到的 feature，不要启用 `tokio/full`、TLS 双栈或框架默认全量功能来省配置，依赖变更后检查：

```bash
cargo tree -e features
cargo check
cargo test
```

## 日志和注释

后端统一使用 `tracing`，日志面向排障，动态值放字段里；用户可见文案继续中文

```rust
tracing::info!(
    event = "subscription.stored",
    device_key = %mask_device_key(&device_key),
    "subscription.stored"
);
```

约定：

- `event` 使用稳定英文标识，格式为 `domain.action`
- Bark ID、token、URL 中的敏感部分必须脱敏
- 错误日志保留 `error = ?error`，不要只写字符串
- 高频心跳、pong、重复事件使用 `debug`

注释只解释代码本身看不出的内容，例如上游字段拼写、时区、算法边界、平台限制和业务规则来源，不要写「创建变量」「保存数据」这类逐行复述，也不要写没有指标支撑的「高并发」「百万级」「优化版」

## 安全和隐私确认

这个项目会保存 Bark Key、监测地点和通知级别，任何相关改动都要先确认下面几条约束：

- 只允许通过 `POST /api/subscribe` 创建或覆盖订阅，通过 `DELETE /api/unsubscribe` 删除订阅
- 不提供「输入 Bark Key 查询订阅详情」的接口，Bark Key 不能作为反查用户位置、地点名称、通知级别或订阅时间的凭据
- 退订接口只返回操作结果，不回显订阅内容
- 统计接口只返回聚合数量，不返回 Bark Key、位置或通知规则
- 日志中只输出 `mask_device_key` 处理后的 Bark Key，不输出完整 Bark Key、精确位置和原始订阅请求体
- 示例、测试、截图和 issue 不使用真实 Bark Key 或真实用户位置
- 不提交真实 `.env`、数据库文件、Bark key、访问 token 或生产私密配置
- 修改 CORS、反代或静态托管规则时，确认不会新增订阅详情读取面

涉及隐私边界的 PR 或提交说明里，需要明确写出是否新增了读取接口、是否回显订阅数据、日志里是否可能出现完整 Bark Key 或位置
