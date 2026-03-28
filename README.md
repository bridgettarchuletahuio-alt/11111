# 11111
短链生成器

## Cloudflare Workers 方案

仓库里已经新增一套独立的 Workers 版本，目录在 [workers/src/index.js](workers/src/index.js)、[workers/schema.sql](workers/schema.sql)、[workers/wrangler.toml](workers/wrangler.toml)。

这个方案的核心区别：

- 短链直接走 Worker 的 [r/:id](workers/src/index.js) 路由，用户访问时由边缘节点直接返回 302 跳转，不再先打开 [redirect.html](redirect.html)
- 后台创建、历史、统计统一走 [api](workers/src/index.js) 接口
- 数据存到 Cloudflare D1，延迟会明显低于 GitHub Pages + Apps Script + Google Sheets

### 一次性部署

1. 安装 Wrangler
2. 创建 D1 数据库：`npx wrangler d1 create link-dispatch-db`
3. 把返回的 `database_id` 填进 [workers/wrangler.toml](workers/wrangler.toml)
4. 初始化表结构：`npx wrangler d1 execute link-dispatch-db --file=workers/schema.sql`
5. 如果你要保护后台接口，设置管理员密钥：`npx wrangler secret put ADMIN_TOKEN`
6. 发布 Worker：`cd workers && npx wrangler deploy`

### 前端切换

如果你准备把现有 GitHub Pages 管理页接到 Worker：

1. 把 [index.html](index.html) 里的 `SCRIPT_URL` 改成你的 Worker 地址加 `/api`
2. 把 [index.html](index.html) 里的 `PUBLIC_BASE_URL` 改成你的 Worker 域名，比如 `https://xxx.workers.dev/`
3. 把 [index.html](index.html) 里的 `SHORT_LINK_MODE` 从 `legacy` 改成 `worker`

改完后，前台生成的短链会从旧的 `redirect.html#abc123` 变成新的 `https://你的域名/r/abc123`。

### 已兼容的接口动作

- `createSet`
- `nextUrl`
- `listSets`
- `getStats`

这意味着你的现有前端逻辑基本不用重写，只要把接口地址切过去即可；真正提升速度的关键，是用户访问短链时不再经过 [redirect.html](redirect.html)，而是直接命中 Worker 的 302 跳转。

### 当前限制

- 这版 Worker 已经适合中低到中等流量
- 如果你后面并发非常高，而且你特别在意严格轮询顺序，可以再把 `current_index` 那段状态更新从 D1 升级成 Durable Object
- 现在这版已经能先把体感速度大幅拉起来，通常比 Apps Script 方案更稳
