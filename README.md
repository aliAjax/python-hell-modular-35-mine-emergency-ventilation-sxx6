# 矿井应急避险与通风协调

这是一个只使用Python标准库和SQLite的模块化原型项目，默认端口为`8335`。领域对象包括矿井人员、气体传感、通风设备、逃生通道、避险硐室、事件和处置任务。`app.py`只负责参数解析、依赖组装和服务生命周期，业务状态机与约束集中在`src/rules.py`。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、领域异常、身份解析和实体数据结构。
- `src/rules.py`：状态机、角色权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务、乐观锁、审计和幂等键。
- `src/service.py`：用例编排、离线记录合并、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、JSON解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8335
```

服务启动时自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。初始化服务不需要单独命令，首次启动即可访问：

```bash
curl http://127.0.0.1:8335/health
```

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。

身份通过`X-User-Id`和`X-Role`请求头传入，角色和动作权限由规则引擎校验。## 核心流程

创建矿井事件、人员和设备记录后，依次执行撤离、搜救、通风恢复和事件关闭。`POST /api/offline-records` 用于整批合并现场离线记录，`source_id + record_id` 相同会幂等返回原记录（已入账的记录在重试时跳过，现场可原样重发）。

离线记录按 `recorded_at` 顺序在一个事务内整批入账，任何一条冲突或超容量都会整批拒绝、不产生任何写入，返回 409 和 `rejected` 清单，逐条指出 `index/source_id/record_id/reason/message`。支持的 `payload.type`：

- `gas`：`sensor_id` 指向的传感器按 `gas_ppm`/`threshold_ppm` 重算严重度并更新状态；中心已标记 `faulty` 的传感器整批冲突。不带 `sensor_id` 的读数仅归档。
- `refuge_enter` / `refuge_leave`：硐室进出，合并后按全部进出记录时间线重算 `occupants` 和硐室状态；超过 `capacity`、硐室维保中、人员已在中心撤离、重复进入/无对应撤出都退回。
- `ventilation_restore`：风机恢复，恢复前确认同区域没有报警传感器和未撤出人员（硐室内人员也算未撤出）。
- `incident_close`：事件关闭，关闭前再用重算后的硐室占用（必须全部清空）和风机状态拦一次。

## 规则重点

- 活跃任务按 `dedupe_key` 防止重复派工。
- 气体读数按阈值计算`severity`。
- 风机恢复（在线或离线）要求同区域无报警传感器、无未撤出人员。
- 事件关闭前必须没有失联或已定位人员、没有活跃任务、所有通风设备恢复运行，并且所有避险硐室已经清空（无人员占用）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

项目使用请求头模拟身份、SQLite单机持久化和简化状态机，适合原型演示和流程验证，不替代行业正式系统、设备控制系统或现场安全规程。
