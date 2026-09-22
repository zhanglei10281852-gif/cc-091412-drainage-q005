# 连续降雨污水抢修调度服务

接收公众报修、物联网液位与现场回执，合并同一影响范围的事件，按危险等级、队伍资质、
备件与可通行路线生成工单；高危事件先隔离再派工，两个调度员并发处理同一工单时只有一个
派工结果生效。

- 仅使用 Python 3.11 标准库，无外部依赖。
- 所有状态变更以事件形式追加落盘（JSONL + fsync），重启重放，未签收/超时提醒不丢失。
- 时间统一使用带时区的 ISO 8601（Asia/Shanghai），事件同时保留来源时间与接收时间。

## 运行

```bash
python src/index.py            # 默认 0.0.0.0:8000
PORT=8000 DATA_DIR=.data python src/index.py
python -m unittest discover    # 全部测试
docker compose up --build
```

## 参考数据（reference/drainage.json）

| 数据 | 说明 |
| --- | --- |
| wells / pipes | 井位与管段，管段连通的井位属于同一水力分量（同一影响范围） |
| roadNodes / roadEdges | 道路图；边可临时封闭，可设置 `maxClass` 车型限制 |
| teams | 队伍班次、资质、车辆 |
| vehicles / parts | 车型（小型/中型/大型）与车载备件数量 |
| rules | 危险等级阈值、所需资质/备件、高危隔离要求、ETA 与超时时限 |

回执样例见 `reference/receipt_sample.json`。

## 核心规则

1. **影响范围合并**：同一水力分量上的在处事件自动并入；跨分量可由调度员显式并单
   （`POST /incidents/merge`，如泵站顶水波及多个小区）。重复来电按 `reportId` 幂等。
2. **危险分级**：取影响范围内最大积水深度，low < 25cm、medium < 50cm、high ≥ 50cm。
   IoT 液位上涨自动升级；升级到高危自动派隔离班。
3. **高危先隔离**：高危工单在隔离确认回执到达前派工一律返回阻塞原因。
4. **派工选择**：班次内 → 资质满足 → 备件充足 → 车型在未封闭道路可达；
   优先资质刚好匹配的队伍（高危班留给高危事件），路程最短者胜出。
5. **派工互斥**：工单带版本号，`expectedVersion` 过期或工单已有派工结果返回 409；
   服务端全局锁 + CAS，并发派工只有一个成功。
6. **合并不取消到场工单**：已到场（arrived/resolved/recovered）工单一律保留；
   若已有到场力量，到场前的重复工单取消，否则保留进度最靠前的一条，取消原因留痕。
7. **道路变化**：封闭/解封边后对在途工单重算路线；无替代路线则尝试改派，仍不可行挂阻塞
   原因；全局队列顺序变化时记录变化前后的排序依据（`queue_reordered` 事件）。
8. **离线回执**：按 `receiptId` 幂等；时间线按发生时间（occurredAt）插入，状态只升不降。
9. **超时提醒**：创建后 15 分钟未签收、签收后 30 分钟未到场；提醒事件落盘，
   重启不丢、不重复。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/health` | 进程存活检查（不代表业务完成） |
| GET | `/board` | 调度看板：影响范围、危险等级、当前队伍、ETA、阻塞原因、队列序 |
| GET | `/teams` | 队伍班次/资质/占用/备件库存 |
| POST | `/reports` | 公众报修（`wellIds` 或 `compound`，`depthCm`，可选 `reportId`） |
| POST | `/measurements` | IoT 液位（`wellId`，`depthCm`，`measurementId`） |
| POST | `/incidents/merge` | 显式并单 `{incidentId, sourceIds, reason}` |
| POST | `/incidents/{id}/isolation` | 派出隔离警戒班 |
| GET | `/incidents/{id}` | 事件详情（影响范围、隔离、工单） |
| POST | `/work-orders/{id}/dispatch` | 派工 `{dispatcher, expectedVersion}` |
| GET | `/work-orders/{id}` | 工单详情（队伍、ETA 窗口、阻塞、版本、提醒） |
| GET | `/work-orders/{id}/timeline` | 从报修到恢复的完整事件记录 |
| POST | `/receipts` | 现场回执（signed/enroute/arrived/resolved/recovered/isolation_confirmed/parts_used） |
| POST | `/roads/{edgeId}/close` `/reopen` | 道路临时封闭/恢复 |
| POST | `/admin/scan-reminders` | 立即扫描超时提醒 |

错误：400 请求不合法，404 资源不存在，409 派工冲突/重复派工。

## 持久化

`DATA_DIR/events.log` 为只增事件日志（每行一个 JSON，写入后 fsync）。进程启动时重放重建
工单、库存、封闭道路、提醒记录；测试必须传入独立临时目录。
