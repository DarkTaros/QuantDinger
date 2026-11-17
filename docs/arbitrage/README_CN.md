# 套利专区

保留原有 QuantDinger 策略和回测系统，新增 Binance 现货＋OKX 现货的
BTC/USDT、ETH/USDT、SOL/USDT 双边吃单回测与实时模拟。每次选择一个交易对，两所分别
预置现金与基础币；不借币、不加杠杆、不转账，不注册真实交易执行客户端。
三角套利、maker-taker 和真实下单不在本期范围。

## 本机启动

Flask 继续使用 Python 3.12。引擎独立使用 Python **3.14** 和
**nautilus_trader==2.0.0rc9**：

```sh
scripts/arbitrage.sh install
(cd backend_api_python && SKIP_AUTO_MIGRATE=false .venv/bin/python -m app.commands.migrate)
scripts/arbitrage.sh supervisor
scripts/arbitrage.sh import-worker
scripts/arbitrage.sh backtest-worker
```

后三个命令分别运行在独立终端。提前启动 PostgreSQL 和持久任务 Redis。
在项目根目录的 `.env` 中配置，勿提交密钥：

```dotenv
ARBITRAGE_ENABLED=true
CRYPTOHFTDATA_API_KEY=
# 留空时不设置此变量，默认使用项目 data/arbitrage
ARB_DATA_DIR=/绝对路径/data/arbitrage
ARB_QUOTA_BYTES=107374182400
ARB_HTTP_PROXY=
```

修改功能开关后重启 API。相邻 `QuantDinger-Vue` 项目的 `/arbitrage` 页面提供数据管理、
回测配置、完整报告和实时模拟。关闭开关时菜单隐藏、套利 API 返回 404，原回测继续工作。

## 根据用户要求调整为 CCXT 公共行情

rc9 官方 Binance 适配器无法解析当前公共 REST SBE 的 exchangeInfo 响应，实际错误为
`Invalid block length: expected 19, was 22`。根据用户后续要求，实时行情改用
**ccxt==4.5.92** (CCXT Pro)，通过 rc9 正式提供的 Python `DataClient` / `DataClientFactory`
接口送入 `LiveNode.run_async()`。执行侧仍然只注册原生 Sandbox 工厂，仍使用双所独立
多币种 CASH 账户；没有更换或修改引擎版本。

Binance 使用官方明确提供的公共行情专用端点：
`https://data-api.binance.vision/api/v3`、`wss://data-stream.binance.vision:443/ws`。
这些端点不提供用户数据和下单功能，见
[官方说明](https://developers.binance.com/en/docs/products/spot/faqs/market_data_only)。
OKX 使用公开现货 `books` 频道。不需要交易所密钥，也不读取真实交易凭据。

CCXT 负责网络与初始快照同步；自定义 Decimal 解析及原始消息钩子保留价格数量精度。
共享校验器再次验证原生序列，每个消息作为一个原子 L2 批次发布。
初始同步的缓存增量合并为可靠快照后才恢复交易，后续不会每次重置整个订单簿。
零数量删除价位，空心跳不补充流动性；断线、序列异常和过期报价禁止新周期。

## 进程、任务和存储

- API 仅做认证、契约校验和用户隔离，不导入 Nautilus、不启动长期循环。
- Supervisor 将 PostgreSQL 持久投递记录发送到专用 Celery 队列，最多管理一个模拟子进程。
- 导入 Worker 每次执行一个导入，内部下载并发为 2。
- 回测 Worker 每次运行独立子进程；PostgreSQL 会话锁确保跨 Worker 实例也最多并发 1。
- 复用 `qd_agent_jobs`，新增 `arb_import`、`arb_backtest`、`arb_paper` 类型；新增套利数据集、
  授权、运行、租约、周期和事件表。Redis 只负责投递，数据库是状态依据。

任务领取有唯一 fencing token 和 90 秒租约；父进程续租。重复投递不能重复执行。
取消运行任务进入 `stopping`，晚到的成功只能提交为 `cancelled`。
失去租约或模拟子进程异常退出会标记 `interrupted`，保留最后确认余额，不自动恢复或重置资金。
幂等键按用户和任务类型隔离；同键不同配置拒绝。没有新增积分收费或 Agent/MCP 接口。

API 前缀为 `/api/arbitrage`，沿用用户 JWT 和 `code: 1` 响应。请求结构见
[OpenAPI](../api/openapi.yaml)，共享契约代码位于 `backend_api_python/qd_arbitrage/`。
报告图表通过 `GET /runs/{run_id}/series` 按 `equity`、`drawdown`、`spread`、`fees`、
`exposure` 获取服务端降采样序列；已有 CryptoHFTData 文件可通过
`POST /datasets/upload` 导入，上传文件仍会进入同一套严格序列、时间和精度校验。
金额数量都是十进制字符串；时间必须带时区，统一使用 UTC `[start,end)`，不扩展到整天。
页面默认 24 小时，后端最长 7 天。随机种子被记录，但当前成交模型不使用随机滑点或随机成交。

原文件、规范化压缩 JSONL 和完整事件分片保存在 `data/arbitrage/`。
SHA-256 内容地址、manifest 和完成标记关联原文件路径、下载时间、转换版本、行情分片及交易规则。
供应商同路径文件修订会产生新版本，旧运行继续引用旧版本。数据库与数据目录应一起备份；
不要删除仍被历史运行引用的文件。默认 100 GiB 软配额超限后停止新增下载，不自动清理。

逐批读取 Parquet；按文件头区分原生 Parquet 与外包 Zstandard。有限重试并尊重 429 的
Retry-After。缺小时、断链、无法解释的时间倒退和缺可靠快照严格失败，初始簿最多回溯 24 小时。
同接收时间按 Binance、OKX，再按订单簿、成交和文件消息顺序固定回放。
历史时间代表供应商采集环境。交易规则在导入时读取并冻结；不声称已还原历史规则变更。

## 策略与账目

按目标基础币数量遍历买卖深度，扣除双所费用并验证库存、现金、最小数量、最小金额与价格保护。
两所数量步长使用最小公倍数，订单数量相同。使用价格保护范围内取整的 IOC 限价单，启用原生
L2 流动性消耗约束，不叠加 K 线固定滑点。只模拟订单入站延迟，成交回报附加延迟为零。
默认敞口上限为 200 USDT，可编辑；100 USDT 为单腿名义金额，允许短暂的受保护单腿敞口。

仅按确认成交量推进状态。两个原始订单终态后存在差额，才在原超额成交场所反向补偿，最多一次。
未确定终态先撤单等待；补偿失败、行情不可用或不可交易 dust 会暂停新周期并保留完整敞口。
停止操作不会清空预置库存。

报告分别展示完整周期已实现净利润、初始库存持有损益、未完成敞口和分所估值后的总权益；
另外记录库存分布变化和未完成交易的重估差额以便对账。报价过期时估值显示不可用。
PostgreSQL 周期账本与不可变事件才是收益依据，前端分页不会参与收益计算。
排除非经济 UUID 后的事件摘要可用于重复回放比对。

## Linux Docker

```sh
docker compose -f docker-compose.yml -f docker-compose.arbitrage.yml \
  --profile arbitrage config --quiet
docker compose -f docker-compose.yml -f docker-compose.arbitrage.yml \
  --profile arbitrage up -d --build arbitrage-supervisor arbitrage-import arbitrage-backtest
```

使用 Python 3.14 独立镜像、依赖锁和非 root 用户。三个服务共享 `arbitrage-data` 卷；
后端只访问数据库，无须挂载引擎文件。该服务组不依赖 Kafka 交易执行链。
前端需从相邻 Vue 仓库构建部署，已有上游预构建镜像不会自动包含本次新增页面。

## 检查与排障

```sh
scripts/arbitrage.sh test
cd backend_api_python
python -m pytest tests/test_arbitrage.py -q
# 设置 ARB_TEST_DATABASE_URL 后，会在临时 schema 内测试 PostgreSQL 生命周期。
```

[验收记录](ACCEPTANCE.md)只记录实际完成的测试，不把待跑检查算作通过。
子进程日志在 `data/arbitrage/logs/<jobId>.log`；任务 API 返回脱敏错误。
不要记录带 `api_key` 的供应商请求 URL。

受控断线演练可向模拟子进程发送 `SIGUSR1`：关闭 CCXT WebSocket、使报价失效，再等待新快照。
正常结束请用 API 停止。异常退出会保留最后确认账目并标记中断；再次启动是独立、显式的新会话。
