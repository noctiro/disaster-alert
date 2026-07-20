# 灾害预警 Bark 订阅系统

通过 Bark 接收地震、气象、海啸和台风信息。服务提供网页订阅界面，也可以直接调用 HTTP API。

示例：<https://alert.noctiro.moe>

## 功能

- 接收 Wolfx、FAN Studio 和 Huania 提供的灾害信息
- 支持地震预警、地震速报、气象预警、海啸预警和台风信息
- 每个 Bark 订阅可以配置最多 3 个监测地点
- 可按灾种、信息来源、预计烈度、震级、严重度和距离设置通知条件
- 地震通知显示监测点预计烈度、距离以及 P 波和 S 波到达时间
- 地震预警会按监测点的实际 S 波剩余时间每秒更新，直到震波到达
- 不同灾种使用独立的 Bark 标题和正文排版，不显示内部渠道、事件 ID 等开发字段
- 通知可打开详情页查看灾害信息和本次命中的订阅条件
- 服务重启后会继续处理尚未完成的订阅确认和通知

地震波到达时间由起震时间、距离、深度和配置的波速估算。震级不改变传播时间，但会影响监测点的预计烈度；预计烈度未命中订阅规则时不会发送通知。

## 部署

### Docker Compose（推荐）

克隆仓库并准备配置：

```bash
git clone https://github.com/noctiro/disaster-alert.git
cd disaster-alert
cp .env.example .env
```

生成签名私钥：

```bash
openssl rand 32 | base64 | tr '+/' '-_' | tr -d '=\n'
```

编辑 `.env`，填写通知详情页访问地址和上一步生成的私钥：

```dotenv
ALERT_DETAIL_BASE_URL=https://alerts.example.com
ALERT_SIGNING_KEY=生成的私钥
```

阅读[使用与部署责任](#使用与部署责任)后，如确认接受实例运营责任，再设置：

```dotenv
INSTANCE_TERMS_ACCEPTED=true
```

启动服务：

```bash
docker compose up -d --build
```

检查状态和日志：

```bash
docker compose ps
docker compose logs -f disaster-alert
```

可选应用配置见[配置](#配置)。

### 手动部署

不使用 Docker 时，需要 Rust `1.97` 或更高版本。先准备配置：

```bash
cp .env.example .env
```

在 `.env` 中填写 `ALERT_DETAIL_BASE_URL`、`ALERT_SIGNING_KEY` 和其他需要的配置，然后构建并启动：

```bash
cargo build --release
./target/release/disaster-alert
```

生产环境建议监听 `127.0.0.1`，再通过反向代理提供 HTTPS。

## 维护与迁移

### 更新 Docker Compose 部署

```bash
git pull --ff-only
docker compose up -d --build
```

数据库保存在 Docker 命名卷中。`docker compose down` 不会删除数据库；`docker compose down -v` 会永久删除数据库。

数据库目录只能由一个应用实例使用，不要增加 `disaster-alert` 服务的副本数。Compose 会等待服务优雅退出并完成数据库刷盘。

### 迁移旧版 sled 数据

以下步骤适用于手动部署。只迁移旧版订阅时，先编译迁移工具：

```bash
cargo build --release --features migration --bin disaster-alert-migrate
```

停止旧服务后执行：

```bash
./target/release/disaster-alert-migrate \
  ./data/disaster-alert.db/ \
  ./data/disaster-alert.fjall
```

迁移完成后，将 `DB_PATH` 指向新目录。迁移工具只迁移订阅，不迁移旧通知任务和历史记录。迁移期间不要同时运行新旧服务。

## 配置

应用会读取当前工作目录下的 `.env`。进程环境变量优先于 `.env`；完整示例见 [.env.example](.env.example)。

### 应用服务

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `INSTANCE_TERMS_ACCEPTED` | `false` | 为 `false` 时拒绝新增和覆盖订阅，已有任务与取消订阅不受影响。设为 `true` 前须阅读“使用与部署责任” |
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `30010` | 服务端口 |
| `SERVER_PUBLISH_HOST` | `127.0.0.1` | Docker Compose 发布端口时使用的宿主机地址；不使用 Compose 时忽略 |
| `ALLOWED_ORIGINS` | 空 | 允许访问 API 的前端 Origin，多个值用逗号分隔 |
| `DB_PATH` | `./data/disaster-alert.fjall` | 数据库目录；同一目录只能由一个应用实例使用 |
| `SHUTDOWN_TIMEOUT_SECONDS` | `15` | 服务关闭时的最长等待时间，范围 `1..=300` 秒 |

### Bark

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `BARK_URL_ALLOWLIST` | `https://api.day.app` | 网页端可以选择的 Bark 服务地址，多个值用逗号分隔 |
| `BARK_SOUND` | 空 | Bark 铃声名称，空表示使用默认铃声 |
| `BARK_VOLUME` | `10` | 通知音量，范围 `0..=10` |
| `BARK_GROUP` | `灾害预警` | Bark 通知分组名 |
| `BARK_CALL` | `true` | 是否为非静默灾害通知启用 Bark 通话级提醒 |
| `ALERT_DETAIL_BASE_URL` | 必填 | Bark 客户端能够访问的通知详情页根地址，部署时使用 HTTPS |
| `ALERT_SIGNING_KEY` | 必填 | 32 字节、无填充的 URL-safe Base64 私钥 |

`BARK_URL_ALLOWLIST` 支持域名、IP、端口和反向代理子路径，例如：

```dotenv
BARK_URL_ALLOWLIST=https://api.day.app,http://192.168.1.10:8080,https://example.com/bark
```

### 灾害数据

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `RECONNECT_MIN_SECONDS` | `1` | 数据源断开后的最小重连间隔 |
| `RECONNECT_MAX_SECONDS` | `30` | 数据源断开后的最大重连间隔 |
| `PUSH_UPDATES` | `false` | 是否推送同一事件的后续报告 |
| `UPDATE_MIN_REPORT_GAP` | `1` | 后续报告至少间隔多少个报告编号才再次推送 |
| `IGNORE_TRAINING` | `true` | 是否忽略演练信息 |
| `IGNORE_CANCEL` | `false` | 是否忽略取消或解除信息，通常应保持 `false` |
| `STALE_ORIGIN_SECONDS` | `600` | 忽略起震时间超过该秒数的地震预警 |
| `P_WAVE_KM_S` | `6.0` | P 波估算速度，单位 km/s |
| `S_WAVE_KM_S` | `3.5` | S 波估算速度，单位 km/s |

其余环境变量用于数据保留、Bark 并发和反向地理编码，默认值见 [.env.example](.env.example)。

## 安全与隐私

服务会保存 Bark Key、监测地点和通知规则。通知详情 URL 包含访问凭据，反向代理、CDN、WAF、APM 和分析系统不得记录 `/incidents/` 路径的完整 URL。

- 不要提交真实 `.env`、数据库、Bark Key 或签名私钥
- 不要在日志、截图、Issue 或测试数据中使用真实 Bark Key、用户位置或通知详情 URL
- 修改 `ALERT_SIGNING_KEY` 后，之前发送的详情链接会失效
- 统计接口只返回聚合数量，系统不提供通过 Bark Key 查询订阅内容的接口

## 使用与部署责任

本仓库提供可独立部署的软件源代码。项目维护者不运营、控制或认可第三方使用本项目搭建的实时灾害信息、订阅、通知或预警服务。

将 `INSTANCE_TERMS_ACCEPTED=true` 写入部署环境，表示实例运营者明确确认：

- 启用实时数据或向他人提供服务前，应自行核查部署地、服务对象所在地及数据来源所在地适用的法律法规，并取得主管部门和数据提供方要求的许可或授权
- 实例运营者对数据接入、内容展示、通知发送、个人信息处理、数据保存和服务对象范围承担责任；自部署不等于获准向社会发布预警
- 本项目及其处理的信息可能延迟、缺失或误报，不属于官方预警，也不应作为唯一的灾害预警、安全决策或应急行动依据

该环境变量只记录部署者的明确确认，不能替代法律评估、行政许可、数据授权或个人信息处理依据，也不能证明某项部署当然合法。若部署者不能确认上述事项，应保持默认值 `false`，并停止对外提供实时功能。

## API

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| `POST` | `/api/subscribe` | 创建或覆盖订阅 |
| `DELETE` | `/api/unsubscribe` | 删除订阅 |
| `GET` | `/api/bark-urls` | 获取可用的 Bark 服务地址 |
| `GET` | `/api/subscription-options` | 获取灾种、来源和默认规则 |
| `GET` | `/api/reverse-geocode` | 根据坐标查询行政区 |
| `GET` | `/api/status` | 获取订阅总数、数据源和后台任务状态 |
| `GET` | `/health` | 健康检查 |

机器可读的接口规范见 [OpenAPI 3.1](docs/openapi.yaml)。大多数用户可以直接使用内置的网页。

## 开发

```bash
cargo fmt --check
cargo check --all-targets
cargo test --all-targets
```

更多开发约定见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 致谢

- 数据源：[wolfx.jp](https://ws-api.wolfx.jp)
- 数据源：[FAN Studio](https://api.fanstudio.tech/doc/ws-api/#home)
- 数据源：[成都高新减灾研究所](http://www.365icl.com/) / [成都市美幻科技有限公司](http://www.huania.com/)
- 推送服务：[Bark](https://github.com/Finb/Bark)
