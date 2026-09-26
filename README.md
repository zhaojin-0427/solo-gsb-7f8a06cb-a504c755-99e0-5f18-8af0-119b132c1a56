# 本地水质采样事件 API

使用 FastAPI + SQLite 实现。客户端以批量方式提交设备浊度采样；服务端按 `(device_id, sequence)` 去重，允许乱序和迟到记录，通过“告警阈值 + 恢复阈值”的滞回规则维护连续浊度告警事件，并按最大采样间隔识别观测中断。已入库的误测记录可通过更正接口在校验旧值后修改采样时间与浊度，并同步重算该设备的告警事件。每次成功的写入、重放或更正还会在同一事务内向**增量事件变更流**追加按事件比对出的差异（`upsert` / `delete`），供下游以“快照 + 变更流”方式复制事件。运维人员可对当前版本的事件做**人工确认**；确认随事件版本失效，且不影响变更流。此外还提供多设备**共同告警查询**，找出指定时间窗内同时处于告警状态的设备数达到阈值的连续区段；**事件证据查询**，按采样列出当前告警事件由哪些观测形成；以及单设备**超阈浊度累积查询**，按时间窗对原始采样做线性插值，累计浊度超过指定阈值的时长与面积。

## 一条命令启动

```bash
docker compose up --build
```

服务监听本机：

- API：<http://127.0.0.1:8000>
- Swagger：<http://127.0.0.1:8000/docs>
- 健康检查：<http://127.0.0.1:8000/health>

SQLite 数据文件保存在 Docker volume `water_data` 中，容器重建后数据仍保留。旧版本创建的数据库在启动时自动迁移，无需额外命令，详见[数据库升级与迁移](#数据库升级与迁移)。

停止：

```bash
docker compose down
```

如需同时删除数据库卷：

```bash
docker compose down -v
```

## 配置

在 `docker-compose.yml` 中配置：

| 环境变量 | 默认值 | 含义 |
|---|---:|---|
| `WATER_ALARM_THRESHOLD` | `10.0` | 浊度 `>= 10.0` 进入告警 |
| `WATER_RECOVERY_THRESHOLD` | `5.0` | 已告警时，浊度 `<= 5.0` 恢复 |
| `WATER_MAX_SAMPLE_GAP_SECONDS` | `600` | 同一设备相邻采样允许的最大间隔（秒），正整数 |
| `WATER_DATABASE_PATH` | `/data/water.db` | SQLite 文件路径 |

要求 `WATER_ALARM_THRESHOLD > WATER_RECOVERY_THRESHOLD`，且 `WATER_MAX_SAMPLE_GAP_SECONDS` 为正整数，否则服务启动时拒绝配置。启动时如果阈值或采样间隔与数据库中已持久化的值不同，服务会自动读取全部原始采样并重算所有事件。

## 数据模型

提交记录：

```json
{
  "records": [
    {
      "device_id": "sensor-01",
      "sequence": 1,
      "sampled_at": "2026-09-23T08:00:00Z",
      "turbidity": 12.5
    }
  ]
}
```

- `device_id`：设备 ID。
- `sequence`：设备本地单调递增序列号，非负整数。
- `sampled_at`：采样时间，必须带时区；服务端统一转为 UTC。
- `turbidity`：浊度值，非负有限数。

去重键为 `(device_id, sequence)`：

- 重复提交且 `sampled_at`、`turbidity` 完全一致：幂等忽略。
- 相同键但数据不一致：返回 `409 Conflict`，整个批次回滚。
- 不同设备可使用相同 `sequence`。
- 单批最多 10,000 条。

## 事件语义

事件从某个浊度值 `>= alarm_threshold` 的采样开始。设备已在告警中时，按采样时间顺序处理每个后续采样：

- 与上一采样的间隔 `> max_sample_gap_seconds`：观测连续性中断，当前告警在**上一个采样处**暂时结束并标记为 `interrupted`，不虚构恢复采样；当前采样随后按“未在告警”重新判定（例如它本身 `>= alarm_threshold` 时会开启新事件）；
- `> recovery_threshold`：仍属于当前事件，包括位于恢复阈值和告警阈值之间的滞回区；
- `<= recovery_threshold`：事件在该采样点恢复，该采样也计入事件并提供 `end_at`。

间隔恰好等于 `max_sample_gap_seconds` 时仍视为连续。

每个事件包含：

- 稳定 `event_id`：`evt:{URL 编码后的 device_id}:{start_sequence}`；
- 开始和结束序列号/时间；
- 峰值浊度及峰值序列号；
- 事件内采样数；
- `active` / `interrupted` / `recovered` 状态：
  - `active`：告警仍在持续，尚无结束采样（`end_at` 为空）；
  - `interrupted`：因相邻采样间隔超限，在最后一个已观测采样处暂时结束，`end_at` 即该采样时间；
  - `recovered`：由观测到的 `<= recovery_threshold` 采样结束，该采样提供 `end_at`。

查询接口（`GET /devices/{device_id}/events`）为每条事件额外附带 `acknowledged` 布尔字段，表示该事件当前版本是否已被人工确认（见[人工确认告警事件](#7-人工确认告警事件)）。`acknowledged` 只出现在查询响应中，不属于事件本身，写入/重放/更正响应与变更流载荷均不包含它。

### 迟到数据如何重算和合并

写入新原始记录后，服务不会增量修补旧事件，而是在一个 SQLite 事务中：

1. 插入该设备的新事实；
2. 删除该设备所有派生事件；
3. 按 `sampled_at ASC, sequence ASC` 重新读取该设备全部原始采样；
4. 重新运行同一个确定性滞回状态机（含采样间隔判断）；
5. 按 `event_id` 比对该设备重算前后的事件，把新增/字段变化记为 `upsert`、消失记为 `delete`，追加到变更流（见[消费增量事件变更流](#6-消费增量事件变更流)）。

事件与变更在同一事务提交；若重算后事件集合没有任何变化（例如相同记录重放），第 5 步不写入任何变更。

因此，迟到记录可以向前连接原本分段的高浊度区间、改变事件起点、补入峰值，或暴露一个真实恢复点并拆分区间。例如（默认 600 秒间隔上限）：

- 先收到 00:00 和 00:30 的两个高值、中间没有任何观测：立即得到两个事件——00:00 采样所在事件在该采样处 `interrupted`，00:30 采样开启一个 `active` 事件；
- 随后补入 00:10、00:20 的采样，把所有相邻间隔补齐到上限以内：暂时分段被重算合并为一个事件，峰值、`sample_count` 和起止时间随之更新；
- 先收到 08:00 和 08:02 的两个高值（间隔未超限），它们不会仅因中间记录迟到而被拆成两个事件；
- 迟到的 `<= recovery_threshold` 观测如果是新发现的恢复边界，会把区间拆分；后续高值形成新事件。

已经观测到的恢复点（`recovered`）是硬边界：之后再补入更高浊度，不会跨越该恢复点合并事件；但 `interrupted` 不是硬边界——如果迟到记录把所有大间隔补齐，暂时分段会合并。这正是滞回规则与间隔规则的确定性结果。重复调用重放接口或按不同 HTTP 批次重放同一批数据，得到相同事件。

## API

### 1. 写入记录

```bash
curl -s -X POST http://127.0.0.1:8000/records \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"device_id":"sensor-01","sequence":1,"sampled_at":"2026-09-23T08:00:00Z","turbidity":4.0},
      {"device_id":"sensor-01","sequence":2,"sampled_at":"2026-09-23T08:01:00Z","turbidity":12.0},
      {"device_id":"sensor-01","sequence":3,"sampled_at":"2026-09-23T08:02:00Z","turbidity":8.0},
      {"device_id":"sensor-01","sequence":4,"sampled_at":"2026-09-23T08:03:00Z","turbidity":5.0}
    ]
  }'
```

返回 `201 Created`，内容包括插入数量、相同重复数量、请求涉及设备（即使全是重复项也会列出）和重算后的事件。

### 2. 重放同一批记录

```bash
curl -s -X POST http://127.0.0.1:8000/records/replay \
  -H 'Content-Type: application/json' \
  -d '{
    "records": [
      {"device_id":"sensor-01","sequence":2,"sampled_at":"2026-09-23T08:01:00Z","turbidity":12.0}
    ]
  }'
```

该接口与写入使用相同的幂等核心逻辑，但返回 `200 OK`。相同记录会被计为 `duplicate_identical`，事件仍会重算并返回。

### 3. 按设备查询事件

```bash
curl -s 'http://127.0.0.1:8000/devices/sensor-01/events'
```

可选参数：

- `status=active|interrupted|recovered`
- `limit=1..1000`，默认 `100`
- `offset>=0`，默认 `0`

示例：

```bash
curl -s 'http://127.0.0.1:8000/devices/sensor-01/events?status=interrupted&limit=50'
```

排序固定为 `start_sequence ASC, start_at ASC, event_id ASC`，保证分页和重复读取可重复。每条事件额外带 `acknowledged` 字段（默认 `false`，详见[第 7 节](#7-人工确认告警事件)）。

### 4. 查询当前配置

```bash
curl -s http://127.0.0.1:8000/thresholds
```

返回当前生效的 `alarm_threshold`、`recovery_threshold` 和 `max_sample_gap_seconds`。

### 5. 更正已入库的误测记录

```bash
curl -s -X POST http://127.0.0.1:8000/records/correction \
  -H 'Content-Type: application/json' \
  -d '{
    "device_id": "sensor-01",
    "sequence": 2,
    "old_sampled_at": "2026-09-23T08:01:00Z",
    "old_turbidity": 12.0,
    "new_sampled_at": "2026-09-23T08:01:30Z",
    "new_turbidity": 11.5
  }'
```

按 `(device_id, sequence)` 定位唯一原始记录，**只允许修改 `sampled_at` 与 `turbidity`**；`device_id`、`sequence` 是记录身份，不可变更。请求必须同时提供旧值与新值：

- 所有时间字段必须带时区，与写入接口一样统一按 UTC 规范比较——`2026-09-23T10:00:00+02:00` 与 `2026-09-23T08:00:00Z` 视为同一时刻；`turbidity` 必须为非负有限数；
- 记录不存在：返回 `404`，数据不变；
- 旧值与库中当前记录不符（记录已被更正过，或请求基于过期数据）：返回 `409`，数据不变；
- 校验通过：返回 `200`、更正后的记录以及该设备重算后的全部事件。

更正与该设备告警事件的重算在同一事务中提交：时间前移或浊度跨越告警/恢复阈值时，事件的分段、峰值与计数立即按新值重算，其他设备的事件不受影响；任一步失败则整体回滚，原始记录保持不变。

更正不是写入也不是重放，不经过 `(device_id, sequence)` 去重逻辑。更正生效后，库中的新值成为后续去重比较的基准：再提交旧值会按冲突返回 `409`，重复提交新值则按 `duplicate_identical` 幂等忽略。

### 6. 消费增量事件变更流

```bash
curl -s 'http://127.0.0.1:8000/event-changes?after=0&limit=100'
```

`POST /records`、`POST /records/replay`、`POST /records/correction` 成功时，服务在原来的写事务内按 `event_id` 比较该设备重算前后的事件集合：

- **新增或任一字段变化**：追加一条 `upsert`，携带完整的新事件（`event` 字段）；
- **消失**：追加一条 `delete`，只携带 `event_id`（`event` 为 `null`）。

一个写事务内的全部变更先按 `device_id`、再按 `event_id` 排序，依次写入，因此 `change_id` 是全局递增、无空洞的游标。变更行与原始记录、派生事件在同一 SQLite 事务中提交或回滚：

- 相同记录重放（事件没有任何变化）、冲突失败批次（`409`）、记录不存在（`404`）或旧值不符（`409`）的被拒更正都**不产生任何变更**；
- 启动时的数据库迁移与阈值变更重算不走写入接口，也不产生变更（升级后变更流为空，`head_change_id` 为 `0`）。

`GET /event-changes` 的参数：

| 参数 | 范围 | 默认 | 含义 |
|---|---|---|---|
| `after` | 非负整数 | `0` | 游标：只返回 `change_id > after` 的变更 |
| `limit` | `1..1000` | `100` | 单页最多返回的变更条数 |

返回体按 `change_id` 升序排列：

```json
{
  "after": 0,
  "limit": 100,
  "next_cursor": 2,
  "head_change_id": 5,
  "changes": [
    {
      "change_id": 1,
      "device_id": "sensor-01",
      "event_id": "evt:sensor-01:2",
      "change_type": "upsert",
      "event": { "event_id": "evt:sensor-01:2", "device_id": "sensor-01", "status": "recovered", "...": "完整事件字段" }
    },
    {
      "change_id": 2,
      "device_id": "sensor-01",
      "event_id": "evt:sensor-01:9",
      "change_type": "delete",
      "event": null
    }
  ]
}
```

- `next_cursor` 是下一次请求应使用的游标：取到数据时等于本页最后一条的 `change_id`；**空页时原样回显 `after`**（只是暂时没有新提交）。
- `head_change_id` 是当前已提交的最大 `change_id`；变更流尚为空时为 `0`。
- 同一 `event_id` 可以反复出现（例如迟到记录使事件合并消失、之后又因更正重新出现）；变更流只追加，从不变更或删除历史行。

#### 推荐的复制协议：先取水位，再取快照，随后追变更

新接入的下游应按以下顺序初始化，避免漏掉与快照并发的写入：

1. **先读 `head_change_id`**：请求一次 `GET /event-changes?after=<一个很大的游标>`（空页也会返回 `head_change_id`），记下水位 `H`；
2. **再取事件快照**：通过 `GET /devices/{device_id}/events` 读取各设备在该时刻的完整事件集合（分页读全）；
3. **随后从该水位消费变更**：用 `after=H` 循环拉取 `/event-changes`，把每页的 `upsert` 按 `event_id` 写入本地副本、`delete` 按 `event_id` 删除，并把 `next_cursor` 作为下一页的 `after`，直到追平 `head_change_id`。

为什么不会丢：第 1 步之后提交的写事务，其变更的 `change_id` 必然 `> H`，一定会在第 3 步中到达；第 1 步之前或期间提交的事件则已包含在第 2 步的快照里。重叠的部分只是被相同内容的 `upsert` 重放一遍，幂等应用即可。若只需要增量而不需要快照，直接从 `after=0`（或上次保存的 `next_cursor`）开始消费即可。

### 7. 人工确认告警事件

```bash
curl -s -X POST \
  'http://127.0.0.1:8000/devices/sensor-01/events/evt:sensor-01:2/ack' \
  -H 'Content-Type: application/json' \
  -d '{
    "expected_event": {
      "event_id": "evt:sensor-01:2",
      "device_id": "sensor-01",
      "status": "recovered",
      "start_sequence": 2,
      "end_sequence": 5,
      "start_at": "2026-09-23T08:01:00Z",
      "end_at": "2026-09-23T08:04:00Z",
      "peak_turbidity": 12.0,
      "peak_sequence": 3,
      "sample_count": 4
    }
  }'
```

请求体中的 `expected_event` 必须是 `GET /devices/{device_id}/events` 查询结果里该事件**除 `acknowledged` 外的完整字段**（时间戳同样允许用不同时区表示同一时刻）：

- **确认成功**：返回 `200` 和该事件（带 `"acknowledged": true`）；
- **重复确认同一版本**：幂等，仍返回 `200` 且状态保持已确认；
- **事件不存在**（`event_id` 不存在或不属于路径中的设备）：返回 `404`，不写入任何状态；
- **版本过期**：`expected_event` 与当前事件任一字段不一致（包括请求体内的 `event_id`/`device_id` 与路径不一致）：返回 `409`，不写入任何状态；客户端应重新查询事件后再确认；
- 请求缺少字段、多出字段或时间戳不带时区：返回 `422`。

#### 确认何时失效

确认状态存入 SQLite（`event_acks` 表），仅以 `event_id` 为键，重启后保留。设备事件只可能在三种事务中被替换：迟到写入/重放、更正、启动时阈值或采样间隔配置变更引发的全量重算。每次重算都在**同一事务内**对该设备重算前后的事件按 `event_id` 逐字段比较：

- 事件的**任一字段发生变化**：其确认随字段变化在该事务内撤销（查询立即得到 `acknowledged: false`）；
- 事件**消失**（被迟到数据合并、被更正消除等）：其确认在同一事务内删除；
- 事件字段逐字段保持一致（例如相同记录重放、未受影响的事件、其他设备的事件）：确认保留；
- 同一 `event_id` **日后重新出现**（例如暂时分段先合并、之后又因更正在新位置出现）：保持**未确认**，不会自动恢复旧确认，需要重新确认。

失效判定与事件替换、变更流追加在同一个 SQLite 事务内提交或回滚，因此不存在“事件已是新版本、确认仍挂在旧版本上”的中间状态；一个设备的失效不影响其他设备，也不影响本设备字段未变的事件。确认与失效本身**不产生** `/event-changes` 变更流记录，变更流载荷也从不包含 `acknowledged` 字段。

### 8. 查询多设备共同告警

```bash
curl -s -X POST http://127.0.0.1:8000/coincident-alarms \
  -H 'Content-Type: application/json' \
  -d '{
    "device_ids": ["sensor-01", "sensor-02", "sensor-03"],
    "quorum": 2,
    "from": "2026-09-23T08:00:00Z",
    "to": "2026-09-23T09:00:00Z"
  }'
```

请求体：

| 字段 | 约束 |
|---|---|
| `device_ids` | 2～50 个**互异**设备 ID（与写入接口一样去除首尾空白，去空白后仍须非空且两两不同） |
| `quorum` | 整数，范围 `2..设备数`：同一时刻至少多少台设备在告警才输出该区段 |
| `from` / `to` | 必须带时区，服务端统一转为 UTC；要求 `from < to`，查询窗口为半开区间 `[from, to)` |

任一约束不满足（缺字段、多字段、时间不带时区、设备数越界、ID 重复、quorum 越界、`from >= to`）返回 `422`。未知设备视为该设备没有任何事件，不报错。

事件如何映射为时间区间（统一为半开区间）：

- `recovered` / `interrupted`：取 `[start_at, end_at)`，即恢复/中断采样提供结束端点，结束端点本身不再算告警；
- `active`：没有结束采样，取 `[start_at, to)`；
- 所有区间裁切到查询窗口 `[from, to)`，裁切后零长度的区间丢弃（例如事件恰好在 `from` 结束、在 `to` 才开始，或单采样即 `interrupted` 的零长事件）。

同一设备在任一时刻最多计一次（同一设备的多个事件区间不会叠加计数）；同一时刻的所有起止端点统一处理：先同时移除在该点结束的设备、再加入在该点开始的设备，因此恰好在同一端点“交接”的两台设备不计为共同告警。

只输出设备数达到 `quorum` 的连续区段：参与设备集合发生变化即分段，只有相邻且集合完全相同的区段才合并；区段按起点排序。`segments` 为空表示窗口内没有任何时刻达到 quorum。响应示例：

```json
{
  "from": "2026-09-23T08:00:00Z",
  "to": "2026-09-23T09:00:00Z",
  "quorum": 2,
  "device_ids": ["sensor-01", "sensor-02", "sensor-03"],
  "segments": [
    {
      "start": "2026-09-23T08:05:00Z",
      "end": "2026-09-23T08:42:00Z",
      "device_ids": ["sensor-01", "sensor-02"]
    },
    {
      "start": "2026-09-23T08:42:00Z",
      "end": "2026-09-23T08:50:00Z",
      "device_ids": ["sensor-01", "sensor-02", "sensor-03"]
    }
  ]
}
```

- `start` / `end` 始终为 UTC（`Z`）时间，区段是半开区间 `[start, end)`；`device_ids` 按字典序排列。
- 查询基于当前已提交派生事件的一次一致读取（只读），**忽略人工确认状态**：已确认的事件照常计入。
- 迟到写入或更正生效后，下一次查询立即反映新的事件结果；查询本身不改动任何采样、事件、确认或变更流（不产生 `/event-changes` 记录）。

### 9. 查询事件证据（告警由哪些采样形成）

```bash
curl -s 'http://127.0.0.1:8000/devices/sensor-01/events/evt:sensor-01:1/evidence'
```

查明一条当前事件具体由哪些采样形成。事件不存在（`event_id` 不存在，或不属于路径中的设备）返回 `404`。当前事件、当前生效的告警/恢复阈值与最大采样间隔、该设备的全部原始采样在**同一个只读快照**中读取，并用与写入路径完全相同的确定性滞回状态机重新定位事件成员：

- `event`：当前事件的完整字段，与 `GET /devices/{device_id}/events` 返回的同一事件一致，但**不包含** `acknowledged`（证据查询同样忽略确认状态）；
- `alarm_threshold` / `recovery_threshold` / `max_sample_gap_seconds`：快照中生效的阈值与最大采样间隔；
- `samples`：事件内采样，严格按 `(sampled_at, sequence)` 升序排列——同刻采样按序列号确定边界，**不会只按时间截取**；条数与事件的 `sample_count` 一致；每条含：
  - `sequence` / `sampled_at` / `turbidity`：原始采样；
  - `role`：该采样与告警的关系：
    - `trigger`：事件首条（触发告警）的采样；
    - `continuing`：事件内的其余采样（含滞回区与高值持续）；
    - `recovery`：仅 `recovered` 事件的末条，即观测到的 `<= recovery_threshold` 采样——它必须计入事件；
  - `peak`：该采样是否为事件峰值（事件的 `peak_sequence`）。峰值并列时沿用状态机的**首次命中规则**：只有 `(sampled_at, sequence)` 顺序上第一个达到最大浊度的采样为 `true`；
- `interruption`：仅 `interrupted` 事件附带，说明观测是如何中断的：
  - `sample`：事件末条成员**之后**按 `(sampled_at, sequence)` 顺序的下一条设备采样——正是它的到来暴露了超间隔，它不属于本事件；
  - `gap_seconds`：事件末条采样与该下一条采样之间的间隔秒数（必然严格大于 `max_sample_gap_seconds`，间隔恰好等于上限时不会中断）；
  - `recovered` 事件该字段为 `null`；`active` 事件没有结束依据，该字段同样为 `null`。

示例（默认 600 秒间隔上限；08:00、08:02 为高值，下一条观测在 08:20，间隔超限）：

```json
{
  "event": {
    "event_id": "evt:sensor-01:1",
    "device_id": "sensor-01",
    "status": "interrupted",
    "start_sequence": 1,
    "end_sequence": 2,
    "start_at": "2026-09-23T08:00:00Z",
    "end_at": "2026-09-23T08:02:00Z",
    "peak_turbidity": 15.0,
    "peak_sequence": 2,
    "sample_count": 2
  },
  "alarm_threshold": 10.0,
  "recovery_threshold": 5.0,
  "max_sample_gap_seconds": 600,
  "samples": [
    {"sequence": 1, "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 12.5, "role": "trigger", "peak": false},
    {"sequence": 2, "sampled_at": "2026-09-23T08:02:00Z", "turbidity": 15.0, "role": "continuing", "peak": true}
  ],
  "interruption": {
    "sample": {"sequence": 3, "sampled_at": "2026-09-23T08:20:00Z", "turbidity": 13.0},
    "gap_seconds": 1080
  }
}
```

迟到写入、更正或启动时配置（阈值/采样间隔）变动导致事件重算后，下一次证据查询立即基于最新采样与事件回答；若事件因此消失，其 `event_id` 再查询返回 `404`。证据查询是**只读**的：不写入事件、确认，也不追加变更流（不产生 `/event-changes` 记录）。

### 10. 查询超阈浊度累积量

```bash
curl -s -X POST http://127.0.0.1:8000/devices/sensor-01/turbidity-exposure \
  -H 'Content-Type: application/json' \
  -d '{
    "from": "2026-09-23T08:00:00Z",
    "to": "2026-09-23T08:03:00Z",
    "threshold": 10.0
  }'
```

按时间窗累计单设备浊度超过指定阈值的程度。请求体：

| 字段 | 约束 |
|---|---|
| `from` / `to` | 必须带时区，服务端统一转为 UTC；要求 `from < to`，线段裁切到闭区间 `[from, to]` |
| `threshold` | 非负有限数：累计所对照的浊度阈值 |

任一约束不满足（缺字段、多字段、时间不带时区、`from >= to`、阈值缺失/为负/非有限值）返回 `422`。未知设备视为没有任何采样，不报错。

该设备的全部原始采样在**一次只读快照**中按 `(sampled_at, sequence)` 读取；同一时刻存在多条采样时只取 `sequence` 最大者参与计算。随后把采样连成折线：

- 仅对**间隔不超过当前 `max_sample_gap_seconds` 的相邻点**做线性插值（间隔恰好等于上限时仍插值，与事件连续性规则一致）；间隔超限的两点之间不插值、不计覆盖；
- 首条采样之前与末条采样之后**不外推**，窗口的其余部分不计覆盖；
- 每段线段裁到 `[from, to]`，与 `threshold` 相交时按交点精确分段。

响应示例（使用[第 1 节](#1-写入记录)写入的四条采样，阈值 10.0）：

```json
{
  "device_id": "sensor-01",
  "from": "2026-09-23T08:00:00Z",
  "to": "2026-09-23T08:03:00Z",
  "threshold": 10.0,
  "covered_seconds": 180.0,
  "exceedance_seconds": 45.0,
  "excess_integral": 45.0
}
```

- `covered_seconds`：窗口内被插值线段覆盖的总时长（秒）；
- `exceedance_seconds`：其中插值浊度严格高于 `threshold` 的时长（秒）；
- `excess_integral`：浊度超出 `threshold` 部分对时间的积分，即折线与阈值之间的面积（单位：浊度·秒）。

三者均保留小数秒不截断；窗口内没有任何覆盖段时三者均为 `0`。迟到写入或更正生效后，下一次查询立即按最新采样计算；查询本身是**只读**的，不写入采样、事件、确认，也不追加变更流（不产生 `/event-changes` 记录）。

## 数据库升级与迁移

`events` 表是 `samples` 原始采样的纯派生数据。服务启动时比对 `schema_meta` 中持久化的 `schema_version` 与代码版本：

- **版本不一致（旧库升级）**：仅当旧版本的 `events` 表结构与当前不同时，丢弃旧的派生 `events` 表并按新结构重建，随后从 `samples` 表重算全部事件；v3/v4 升级到 v5 时事件表结构已一致，不重建事件。原始采样记录逐行保留，迁移只影响派生数据。
- **阈值或 `WATER_MAX_SAMPLE_GAP_SECONDS` 变更**：同样只重建派生事件，不触碰原始记录；重建过程中按字段变化撤销相应的人工确认。

迁移在启动流程内自动完成，`docker compose up --build` 一条命令即可带着旧数据卷升级，无需手工干预。重建是确定性的，重复写入与重放在迁移前后均保持幂等。

### 升级中断恢复

涉及旧 `events` 表结构的升级分两步提交：先按新结构重建空的派生 `events` 表，随后在同一个事务里从 `samples` 重算全部事件并写入新的 `schema_version`——**只有重算成功提交，升级才视为完成**。若进程在两次提交之间退出（断电、容器被 kill 等），数据库中保留的仍是旧版本号，下次启动会重新检测到版本不一致，再次执行完整迁移：丢弃（可能为空的）派生事件表，并从保留的原始采样完整重算，告警事件不会丢失。v3/v4 升级到 v5 不改变 `events` 结构，无需重建，`schema_version` 在新表（`event_changes`、`event_acks`）建立后直接写入。升级成功后重复重启不会再次清空或重算已恢复的事件，已有的确认状态也继续保留。

当前结构版本为 5。各版本变更：

- 版本 5：新增人工确认状态表 `event_acks`。它与 `event_changes` 一样不是可由 `samples` 重建的派生数据，因此升级时只以空表建立、**不会被丢弃或重建**；`events` 表自版本 3 起结构未变，所以 v4 升级到 v5 时事件表**不重建**，已有的确认状态原样保留。若升级同时伴随阈值或采样间隔配置变更，启动重算仍按[确认失效规则](#确认何时失效)逐字段撤销受影响事件的确认。
- 版本 4：新增只追加的 `event_changes` 增量变更流。该表不是可由 `samples` 重建的派生缓存（下游可能停留在任意水位），因此升级时**不会被丢弃或重建**：旧库升级只重建 `events`，`event_changes` 以空表建立，随后由成功的写入、重放和更正产生变更；升级过程中的事件重算本身不写入变更流。
- 版本 3：事件表不再以序列号大小约束事件起点、终点与峰值的先后——序列号仅用于设备内去重和事件标识（`event_id`），迟到/乱序采样按采样时间重算出的终点、峰值与计数现在可以正确持久化；时间维度的一致性约束（如 `end_at >= start_at`）保持不变。

## 本地非容器开发

要求 Python 3.11+：

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements-dev.txt
WATER_DATABASE_PATH=./water.db uvicorn app.main:app --reload
```

运行测试：

```bash
pytest -q
```

## 事务与一致性说明

- SQLite 使用 WAL 模式和 `BEGIN IMMEDIATE`。
- 同一 FastAPI 进程内使用写锁，保证批次内“查重、插入、删事件、重算事件”原子完成。
- 查询只读取已提交的派生事件。
- 派生事件完全可由 `samples` 表重建；重放和配置变更（阈值或 `WATER_MAX_SAMPLE_GAP_SECONDS`）不需要额外修复逻辑。
- 更正接口的记录更新与该设备事件重算在同一事务提交；失败整体回滚，且更正不影响其他设备的派生事件。
- 增量变更行与原始记录、派生事件在同一个写事务内提交或回滚；事务内的变更按 `(device_id, event_id)` 排序后依次写入，`change_id` 全局递增、无空洞。相同记录重放、冲突批次、被拒更正（`404`/`409`）不产生变更。
- 人工确认只在事件逐字段匹配 `expected_event` 时写入；重复确认同一版本幂等。确认的撤销（事件字段变化或消失）与该设备的事件重算、变更流追加在同一事务内提交或回滚，同一 `event_id` 日后重现仍为未确认，其他设备和字段未变的事件不受影响。确认与撤销不产生变更流记录，重启后确认状态保留。
- `POST /coincident-alarms` 是只读查询：在一次一致读取中扫描所请求设备的当前派生事件，忽略确认状态，不写入采样、事件、确认，也不追加变更流。迟到写入或更正提交后，下一次查询即反映新结果。
- `GET /devices/{device_id}/events/{event_id}/evidence` 是只读查询：在一次只读事务快照中同时读取事件、生效阈值与该设备原始采样，并用写入路径同一个状态机定位成员，不写入事件、确认，也不追加变更流。迟到写入、更正或配置变更后下一次查询即反映最新证据，事件消失则返回 `404`。
- `POST /devices/{device_id}/turbidity-exposure` 是只读查询：在一次只读快照中读取该设备原始采样与当前最大采样间隔，按线性插值累计超阈时长与面积，不写入采样、事件、确认，也不追加变更流。迟到写入或更正提交后，下一次查询即反映新结果。
- 升级时 `schema_version` 与事件重算在同一事务提交；升级任一阶段中断后重启会自动重新迁移，从原始采样恢复全部派生事件。`event_changes` 与 `event_acks` 表都不是派生缓存，升级时不会被丢弃。
