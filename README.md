# 排水抢修调度服务

面向连续降雨期间的城市排水抢修调度：接收**公众报修、物联网液位、现场回执**三类输入，
合并同一影响范围的事件，按**危险等级 → 隔离 → 队伍资质/班次 → 车载备件 → 可通行路线**
安排工单，并记录从报修到恢复的全过程。

## 运行

需要 Python 3.11+，仅使用标准库：

```bash
python src/index.py            # 默认 0.0.0.0:8000
python -m unittest discover    # 测试
docker compose up --build      # 容器
```

环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DISPATCH_DATA_DIR` | `.runtime/data` | journal 落盘目录（必须可写） |
| `DISPATCH_REFERENCE_DIR` | `reference/dispatch` | 井位/队伍/规则等参考数据 |
| `DISPATCH_SCHEDULER` | `1` | 是否启动后台节拍（自动派工/超时提醒/路线巡检） |
| `DISPATCH_TICK_SECONDS` | `15` | 节拍间隔 |
| `DISPATCH_ACK_TIMEOUT_SECONDS` | `300` | 派工后未签收提醒阈值 |
| `DISPATCH_RESTORE_TIMEOUT_SECONDS` | `5400` | 到场后未恢复提醒阈值 |
| `DISPATCH_SEED_SAMPLE` | `0` | 启动时把 `sample_receipts.jsonl` 离线回执补录进既有工单 |

健康接口只表示进程存活，不代表任何业务已完成。

## 核心保证

- **影响范围合并**：拓扑中的合并组（同一干管/小区）内的重复来电与液位读数归入同一事件，
  只有一张在途工单；重复提交按确定性 id 幂等去重。
- **高危先隔离**：命中高危规则（如水深 ≥ 0.5m、泵站带电环境）时，未记录隔离不可派工。
- **资质/备件/路线闸门**：不具备受限空间、气体检测等资质的班组、车载备件被占用、
  车辆超过限高/总重/涉水限制或无路可达，都会在响应中给出结构化阻塞原因。
- **已到场保护**：工单进入 onsite/working 后，普通合并不得取消、不得改派。
- **路线留痕**：道路封闭/限行后在途工单自动重算路线，记录旧/新路段、旧/新 ETA 与
  旧/新队列位次（`reroutes`）；完全阻断时记录 `route_blocked`。
- **离线回执**：按 `occurredAt`（发生时间）而非接收时间接入时间线；同一 `receiptId`
  重传幂等，同号异状态返回 409。
- **双调度员一致性**：派工携带 `expectedVersion`（乐观锁），并发派工只有一个成功，
  其余得到 `409 version_conflict`。
- **重启不丢**：所有处置事实只追加到 `journal.jsonl`（fsync），重启按发生时间全量重放；
  未签收工单、超时提醒、改线依据均保留，重放结果与到达顺序无关。

## 接口（节选）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/reports` | 公众报修（reportId 幂等，可带 nodeId/community/depthM） |
| POST | `/v1/sensor-readings` | 物联网液位（低于预警值不建工单） |
| POST | `/v1/events/<id>/isolation` | 高危事件隔离登记 |
| POST | `/v1/events/<id>/merge` | 手工合并事件（已到场源工单 409） |
| POST | `/v1/route-closures` | 道路封闭/恢复（restriction 支持限高/限重/涉水） |
| POST | `/v1/work-orders/<id>/dispatch` | 派工/改派（body 可带 crewId、expectedVersion） |
| POST | `/v1/work-orders/<id>/ack` | 班组签收 |
| POST | `/v1/receipts` | 现场回执（dispatched/isolated/onsite/working/restored） |
| POST | `/v1/scheduler/tick` | 手动触发一次调度节拍 |
| GET | `/v1/work-orders/<id>` | 影响范围、当前队伍、预计到达窗口、阻塞原因、完整事件记录 |
| GET | `/v1/events/<id>` | 事件视图与从报修到恢复的时间线 |
| GET | `/v1/crews` | 队伍班次、资质、车辆、可用备件、当前队列 |
| GET | `/v1/situation` | 态势总览（事件/工单/队伍/封闭路段/版本号） |

工单视图中的关键字段：`impactArea`（影响范围）、`currentCrew`（当前队伍与路线）、
`expectedArrivalEarly/Late`（到达窗口）、`blockers`（阻塞原因）、`reroutes`（改线与重排依据）、
`escalations`（超时提醒）、`timeline`（报修→隔离→派工→到场→恢复记录）。

## 参考数据

`reference/dispatch/` 下为可替换的业务数据：`topology.json`（井位/管段/合并组）、
`crews.json`（班次/资质/车辆装载）、`hazard_rules.json`（危险作业规则）、
`parts.json`（备件与库存）、`roads.json`（路网与行驶参数）、
`sample_receipts.jsonl`（离线回执样例）。时间字段统一为带时区的 ISO 8601。
