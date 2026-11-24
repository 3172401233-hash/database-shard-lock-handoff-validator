# database-shard-lock-handoff-validator

验证分片迁移中的锁所有权、epoch 和 fencing。

## 背景

分片迁移把某个会话移动到新协调器时，旧协调器可能仍持有 advisory lock。
若新协调器直接重新授予同名锁，两个事务可能同时修改互斥资源。
本项目在本地模型中重放授予 / 释放 / 迁移 / 写入事件，验证每一步是否有效：

- 同名锁不能因迁移产生两个有效 owner（旧 epoch 的授予在 CUTOVER 后失效）。
- 旧 epoch 的写入必须被 fencing（epoch 检查 + 单调递增 fencing token）。
- 迁移活跃阶段（PREPARE / COPY / CUTOVER）阻断源协调器上的新授予。
- CUTOVER 时旧协调器仍持有的锁记录为迁移阻断项。

## 本地模型说明

**所有连接与锁均为本地模型**：协调器、会话、事务、锁表、租约和 fencing
token 都是单进程内存中的 Python 对象，不建立任何真实网络连接，不访问
真实数据库。时间由事件自带的 `now` 参数驱动，租约到期按时间惰性清理。

## 模型要素

- 会话（session）/ 事务（txn）：锁的申请与持有主体。
- 锁：按名字区分，模式为 `SHARED` / `UPDATE` / `EXCLUSIVE`，附带持锁租约；
  `UPDATE` 与 `SHARED` 兼容、与 `UPDATE` 互斥，可在无其他持有者时升级为
  `EXCLUSIVE`。
- 协调器 epoch：CUTOVER 时权威 epoch 提升，旧 epoch 授予变为僵尸。
- 迁移阶段：`PREPARE -> COPY -> CUTOVER -> COMPLETE`（或 `ABORTED`）。
- 资源写入：携带 `(epoch, fencing_token)`，二者均校验通过才接受。

## 输出

`LockHandoffValidator.report(now)` 返回：

- `holders`：每把锁当前的有效持有者（会话、事务、模式、epoch、token）。
- `conflicting_writes`：被 fencing 的冲突写入及原因。
- `migration_blockers`：迁移阻断项（如 CUTOVER 时源协调器未释放的锁）。
- `safe_retry_steps`：安全重试步骤（等待租约到期、在新 epoch 重新获锁、
  用新 fencing token 重试写入等）。
- `dual_owner_violations`：同名锁多有效 owner 的违规检测（应为空）。

## 运行测试

```bash
python -m unittest
```

测试覆盖：共享锁共存与互斥、更新锁升级、迁移中断与恢复、旧会话晚写
fencing、旧协调器残留锁下的交接。代码兼容 Python 3.8.2+。
