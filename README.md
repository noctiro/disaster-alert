# 地震预警 Bark 订阅系统

基于 Rust 长驻后端的地震预警实时推送服务，后端监听 Wolfx WebSocket，按订阅者位置匹配后通过 Bark 推送，并内嵌前端页面

示例：<http://eew.noctiro.moe>

## 数据流

1. Wolfx WebSocket 推送 JMA / 四川 / CENC / 福建 / 重庆地震预警消息到后端
2. 后端解析消息，转换为通用格式，过滤演练 / 取消 / 过期 / 重复事件
3. 从内存快照流式遍历全部订阅，按各监测点的震源距估算 JMA 震度并匹配通知级别
4. 同一事件的待处理更新自动合并为最新报告，再通过 Bark 并发推送；命中推送失败时自动清理无效 Bark Key

## 技术栈

- **后端**：Rust、Axum、sled、tokio-tungstenite、reqwest（rustls）
- **前端**：单文件 `web/index.html`，原生 JS、Leaflet + CartoCDN 地图
- **部署**：后端单二进制内嵌前端；前端可由后端、Cloudflare Worker、Vercel 或任意静态平台托管

## 项目结构

```text
backend/              Rust 后端，编译产物为单一二进制
web/index.html         前端源文件，后端通过 include_str! 内嵌
deploy/
  cloudflare-worker/   托管前端 + 反代 API
  vercel/              rewrite 反代
  caddy/               反向代理示例
  nginx/               反向代理示例
  systemd/             后端守护进程示例
```

## 部署

后端必须运行，地震监听和推送只发生在后端进程；前端可以由后端内嵌、由边缘平台托管，或两者分离部署

### 后端

```bash
cd backend
cp .env.example .env
set -a; . ./.env; set +a
cargo build --release
./target/release/earthquake-alert-backend
```

默认监听 `0.0.0.0:30010`，前端页面已内嵌在二进制里，浏览器访问 `http://your-server:30010` 即可使用

如果需要常驻，参考 `deploy/systemd/earthquake-alert.service`：

```bash
sudo cp deploy/systemd/earthquake-alert.service /etc/systemd/system/
sudo systemctl enable --now earthquake-alert
```

## 前端托管

### 方式一：后端内嵌（默认）

上面的后端启动方式已经内嵌前端，无需额外配置，所以方式二到方式四只在需要分离托管前端时使用

### 方式二：Cloudflare Worker

`deploy/cloudflare-worker` 托管 `web/` 静态前端，并把 `/api/*`、`/health` 转发到后端

```bash
cd deploy/cloudflare-worker
# 编辑 wrangler.toml，把 BACKEND_URL 改为后端地址
wrangler deploy --env production
```

### 方式三：Vercel

把 `web/` 作为 Vercel 静态站点根目录，`deploy/vercel/vercel.json` 把 `/api/*` 和 `/health` rewrite 到后端，部署前把 `vercel.json` 里的 `YOUR_BACKEND_HOST` 改成后端地址

### 方式四：Caddy 或 Nginx 反代

- `deploy/caddy/Caddyfile`：整站反代到本机 `127.0.0.1:30010`，自动申请 HTTPS
- `deploy/caddy/Caddyfile.static`：Caddy 托管 `web/` 静态文件，只反代 `/api/*` 和 `/health`
- `deploy/nginx/earthquake-alert.conf`：把 `/` 反代到本机 `127.0.0.1:30010`

### 任意静态平台 + 跨域后端

把 `web/index.html` 托管到 GitHub Pages、对象存储等任意静态平台，在页面加载前设置后端地址：

```html
<script>window.EEW_API_BASE = "https://api.example.com"</script>
```

这样前端可以跨域访问独立后端；需要在后端 `ALLOWED_ORIGINS` 中配置前端 Origin 才能跨域访问

## 配置

后端配置通过环境变量，在 `backend/` 下创建 `.env`，运行前导入或通过系统环境变量配置：

```bash
cd backend
cp .env.example .env
# 编辑 .env
set -a; . ./.env; set +a
```

配置值会在启动时校验；数值格式错误、重连下限大于上限、非正波速、无效并发上限等会直接导致服务启动失败。

`BARK_URL_ALLOWLIST` 支持 HTTP/HTTPS、域名或 IP、显式端口和反向代理子路径，例如 `https://api.day.app`、`http://192.168.1.10:8080`、`https://example.com/bark`。不允许凭据、查询参数或 fragment；末尾 `/` 会被移除，推送时统一追加 `/push`。配置顺序会原样提供给网页端；网页端首次使用时选择第一项。服务端不会把第一项当作发送失败或历史地址失效时的回退目标。

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `SERVER_HOST` | `0.0.0.0` | 监听地址 |
| `SERVER_PORT` | `30010` | 服务端口 |
| `ALLOWED_ORIGINS` | (空) | 允许跨域访问 API 的前端 Origin，多个值用逗号分隔；空表示不额外开放跨域 |
| `DB_PATH` | `./data/earthquake.db` | 数据库路径 |
| `BARK_URL_ALLOWLIST` | `https://api.day.app` | 前端可选的 Bark 基础 URL 有序白名单，支持 HTTP/HTTPS、端口、IP 和反代子路径；多个值用逗号分隔 |
| `BARK_SOUND` | (空) | Bark 铃声名称，空表示使用默认 |
| `BARK_VOLUME` | `10` | Bark 推送音量 (0-10) |
| `BARK_GROUP` | `地震预警` | Bark 推送分组名 |
| `BARK_CALL` | `true` | 是否触发 Bark 通话级别推送；默认重复播放通知铃声 |
| `EEW_WEBSOCKET_URL` | `wss://ws-api.wolfx.jp/all_eew` | 地震预警 WebSocket 地址 |
| `RECONNECT_MIN_SECONDS` | `1` | 重连最小间隔秒数 |
| `RECONNECT_MAX_SECONDS` | `30` | 指数退避重连最大间隔秒数 |
| `PUSH_UPDATES` | `false` | 是否推送同一事件的后续报告 |
| `UPDATE_MIN_REPORT_GAP` | `1` | 同事件两次推送之间至少间隔的报告数 |
| `IGNORE_TRAINING` | `true` | 是否跳过演练事件 |
| `IGNORE_CANCEL` | `true` | 是否跳过取消事件 |
| `P_WAVE_KM_S` | `6.0` | P 波传播速度 (km/s) |
| `S_WAVE_KM_S` | `3.5` | S 波传播速度 (km/s) |
| `STALE_ORIGIN_SECONDS` | `600` | 发震时间超过该秒数视为过期 |
| `DEDUP_KEEP_MINUTES` | `120` | 事件去重窗口分钟数 |
| `MAX_CONCURRENT_NOTIFICATIONS` | `200` | 并发推送上限；实际值不会超过 `HTTP_POOL_SIZE` |
| `HTTP_POOL_SIZE` | `200` | HTTP 连接池大小 |

## API

所有接口返回统一 JSON：

```json
{
  "success": true,
  "message": "订阅成功",
  "data": {}
}
```

失败时 `success` 为 `false`，`data` 字段省略，`message` 返回可展示的错误原因

| 方法 | 路径 | 用途 | 成功响应 |
| --- | --- | --- | --- |
| `POST` | `/api/subscribe` | 发送 Bark 确认提醒成功后创建或覆盖订阅 | `200` |
| `GET` | `/api/bark-urls` | 返回网页端可选择的 Bark URL 白名单 | `200` |
| `DELETE` | `/api/unsubscribe` | 按 Bark ID 删除订阅 | `200` |
| `GET` | `/api/stats` | 返回订阅总数 | `200` |
| `GET` | `/health` | 健康检查 | `200` |

### `POST /api/subscribe`

请求体：

```json
{
  "bark_id": "key",
  "bark_url": "https://api.day.app",
  "location_name": "东京",
  "latitude": 35.6,
  "longitude": 139.6,
  "locations": [
    { "name": "东京", "latitude": 35.6, "longitude": 139.6 }
  ],
  "notify_bands": [
    { "min": 1, "max": 1, "level": "passive", "label": "低烈度" },
    { "min": 2, "max": 2, "level": "active", "label": "中等烈度" },
    { "min": 3, "max": 99, "level": "critical", "label": "高烈度" }
  ]
}
```

字段说明：

| 字段 | 当前请求要求 | 说明 |
| --- | --- | --- |
| `bark_id` | 是 | Bark Key，只允许字母和数字，最长 64 字符 |
| `bark_url` | 是 | 必须精确匹配后端 `BARK_URL_ALLOWLIST` 中规范化后的 URL |
| `location_name` | 是 | 单地点订阅名称，`locations` 为空时使用 |
| `latitude` / `longitude` | 是 | 单地点坐标，`locations` 为空时使用；当前反序列化仍要求传入这两个字段 |
| `locations` | 建议传入 | 监测地点列表，最多 3 个，有效坐标范围为纬度 `-90..90`、经度 `-180..180` |
| `locations[].name` | 否 | 地点名称，最多 80 个字符 |
| `notify_bands` | 是 | 通知规则列表，最多 3 条，烈度范围不能重叠 |
| `notify_bands[].min` / `max` | 是 | 匹配的预估 JMA 烈度范围，取值 `0..99` |
| `notify_bands[].level` | 是 | Bark 中断级别，只允许 `passive`、`active`、`critical` |
| `notify_bands[].label` | 否 | 前端展示标签，最多 32 个字符 |

兼容说明：

- 新版前端使用 `locations`，旧版单地点字段 `location_name`、`latitude`、`longitude` 仍会保留在请求体中
- 如果 `locations` 非空，后端以 `locations[0]` 作为主地点，并忽略旧版单地点字段
- `critical` 规则的 `max` 小于 `7` 时，后端会扩展为 `99`

成功响应：

```json
{
  "success": true,
  "message": "订阅成功",
  "data": { "saved": true }
}
```

常见失败：

| 状态码 | 原因 |
| --- | --- |
| `400` | Bark ID 为空、过长或包含非字母数字字符 |
| `400` | Bark URL 无效或不在白名单中 |
| `400` | 没有有效监测地点 |
| `400` | 通知规则为空、超过 3 条、级别非法或烈度范围重叠 |
| `502` | Bark 确认提醒发送失败，订阅未保存 |
| `500` | 数据库存储失败 |

### `DELETE /api/unsubscribe`

请求体：

```json
{ "bark_id": "key" }
```

成功响应：

```json
{
  "success": true,
  "message": "已取消订阅"
}
```

常见失败：

| 状态码 | 原因 |
| --- | --- |
| `400` | Bark ID 为空、过长或包含非字母数字字符 |
| `404` | 删除失败，通常表示没有对应订阅或数据库删除失败 |

### `GET /api/stats`

返回订阅总数，不返回 Bark ID、位置、通知规则或订阅时间：

```json
{
  "success": true,
  "message": "统计成功",
  "data": { "total_subscriptions": 12 }
}
```

### `GET /health`

健康检查只表示 HTTP 服务可响应：

```json
{
  "success": true,
  "message": "OK"
}
```

### 隐私边界

系统不提供「输入 Bark Key 查询订阅内容」的接口，Bark Key 不能反查用户位置、通知级别或订阅时间，详见 [CONTRIBUTING.md](CONTRIBUTING.md) 中的隐私确认

## 致谢

- 数据源：[wolfx.jp](https://ws-api.wolfx.jp)
- 推送服务：[Bark](https://github.com/Finb/Bark)
