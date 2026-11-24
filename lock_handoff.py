"""分片迁移场景下的 advisory lock 交接验证器（纯本地模型）。

模型要素：
- 协调器（Coordinator）：持有锁表，带单调递增的 epoch。
- 会话（Session）/ 事务（Txn）：锁的申请者与持有者。
- 锁：按名字区分，模式为 SHARED / UPDATE / EXCLUSIVE，附带持锁租约。
- 迁移：PREPARE -> COPY -> CUTOVER -> COMPLETE（或 ABORTED）。
- 资源写入：携带 (epoch, fencing token)，旧 epoch 的写入必须被 fencing。

所有协调器、连接与锁均为单进程内的本地模拟，不涉及真实网络。
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple


class LockMode(enum.Enum):
    SHARED = "shared"
    UPDATE = "update"
    EXCLUSIVE = "exclusive"


_COMPATIBLE = {
    LockMode.SHARED: frozenset({LockMode.SHARED, LockMode.UPDATE}),
    LockMode.UPDATE: frozenset({LockMode.SHARED}),
    LockMode.EXCLUSIVE: frozenset(),
}


class MigrationPhase(enum.Enum):
    IDLE = "idle"
    PREPARE = "prepare"
    COPY = "copy"
    CUTOVER = "cutover"
    COMPLETE = "complete"
    ABORTED = "aborted"


_PHASE_ORDER = {
    MigrationPhase.PREPARE: (MigrationPhase.COPY, MigrationPhase.ABORTED),
    MigrationPhase.COPY: (MigrationPhase.CUTOVER, MigrationPhase.ABORTED),
    MigrationPhase.CUTOVER: (MigrationPhase.COMPLETE, MigrationPhase.ABORTED),
}

_ACTIVE_PHASES = (MigrationPhase.PREPARE, MigrationPhase.COPY, MigrationPhase.CUTOVER)


@dataclass
class Grant:
    lock_name: str
    session: str
    txn: str
    mode: LockMode
    epoch: int
    fencing_token: int
    lease_expiry: float
    coordinator: str
    released: bool = False

    def alive(self, now: float) -> bool:
        return not self.released and now <= self.lease_expiry


@dataclass
class EventResult:
    ok: bool
    kind: str
    reason: str = ""

    def __bool__(self) -> bool:
        return self.ok


@dataclass
class Migration:
    migration_id: str
    shard: str
    source: str
    target: str
    new_epoch: int
    phase: MigrationPhase = MigrationPhase.PREPARE


@dataclass
class Coordinator:
    name: str
    epoch: int
    grants: Dict[str, List[Grant]] = field(default_factory=dict)


class LockHandoffValidator:
    """按事件顺序验证授予 / 释放 / 写入 / 迁移是否有效。"""

    def __init__(self) -> None:
        self.coordinators: Dict[str, Coordinator] = {}
        self.authoritative_epoch = 0
        self.migration: Optional[Migration] = None
        self._token_counter = 0
        self._resource_max_token: Dict[str, int] = {}
        self.accepted_writes: List[dict] = []
        self.conflicting_writes: List[dict] = []
        self.migration_blockers: List[str] = []
        self.safe_retry_steps: List[str] = []

    def add_coordinator(self, name: str, epoch: int) -> None:
        self.coordinators[name] = Coordinator(name=name, epoch=epoch)
        self.authoritative_epoch = max(self.authoritative_epoch, epoch)

    def _next_token(self) -> int:
        self._token_counter += 1
        return self._token_counter

    def _purge_expired(self, now: float) -> None:
        for coord in self.coordinators.values():
            for lock_name in list(coord.grants):
                coord.grants[lock_name] = [
                    g for g in coord.grants[lock_name] if g.alive(now)
                ]
                if not coord.grants[lock_name]:
                    del coord.grants[lock_name]

    def _live_grants(self, lock_name: str, now: float) -> List[Grant]:
        self._purge_expired(now)
        live = []
        for coord in self.coordinators.values():
            live.extend(coord.grants.get(lock_name, []))
        return live

    def _valid_grants(self, lock_name: str, now: float) -> List[Grant]:
        """有效 owner：存活且 epoch 等于当前权威 epoch。"""
        return [
            g for g in self._live_grants(lock_name, now)
            if g.epoch == self.authoritative_epoch
        ]

    def grant(self, coordinator, session, txn, lock_name,
              mode, lease, now):
        # type: (...) -> EventResult
        coord = self.coordinators.get(coordinator)
        if coord is None:
            return EventResult(False, "grant", "未知协调器 %s" % coordinator)
        _, lease_expiry = lease
        if lease_expiry < now:
            return EventResult(False, "grant", "租约已过期")

        mig = self.migration
        if mig and mig.phase in _ACTIVE_PHASES and coordinator == mig.source:
            reason = ("迁移 %s 处于 %s 阶段，源协调器 %s 上的新授予被阻断"
                      % (mig.migration_id, mig.phase.value, coordinator))
            self.migration_blockers.append(reason)
            self.safe_retry_steps.append(
                "等待迁移 %s 到达 CUTOVER 后，在协调器 %s (epoch %d) 上重新申请锁 %s"
                % (mig.migration_id, mig.target, mig.new_epoch, lock_name))
            return EventResult(False, "grant", reason)

        if coord.epoch < self.authoritative_epoch:
            reason = ("协调器 %s 的 epoch %d 落后于权威 epoch %d，授予无效"
                      % (coordinator, coord.epoch, self.authoritative_epoch))
            self.safe_retry_steps.append(
                "在权威协调器 (epoch %d) 上为会话 %s 重新申请锁 %s"
                % (self.authoritative_epoch, session, lock_name))
            return EventResult(False, "grant", reason)

        # 只有当前权威 epoch 的授予参与兼容性判断；
        # 旧 epoch 的残留授予是僵尸，不阻断新授予。
        live = [g for g in self._live_grants(lock_name, now)
                if g.epoch == self.authoritative_epoch]
        others = [g for g in live if g.txn != txn]
        own = [g for g in live if g.txn == txn]

        if own:
            current = own[0]
            if current.mode == LockMode.UPDATE and mode == LockMode.EXCLUSIVE \
                    and not others:
                current.mode = LockMode.EXCLUSIVE
                return EventResult(True, "grant",
                                   "事务 %s 的 UPDATE 锁升级为 EXCLUSIVE" % txn)
            if current.mode == mode:
                return EventResult(True, "grant", "同事务重入，保持已有授予")
            reason = "事务 %s 已持有 %s 锁，无法升级为 %s（存在其他持有者）" % (
                txn, current.mode.value, mode.value)
            return EventResult(False, "grant", reason)

        for other in others:
            if other.mode not in _COMPATIBLE[mode]:
                reason = ("锁 %s 与事务 %s 持有的 %s 锁不兼容"
                          % (lock_name, other.txn, other.mode.value))
                return EventResult(False, "grant", reason)

        grant = Grant(lock_name=lock_name, session=session, txn=txn, mode=mode,
                      epoch=coord.epoch, fencing_token=self._next_token(),
                      lease_expiry=lease_expiry, coordinator=coordinator)
        coord.grants.setdefault(lock_name, []).append(grant)
        return EventResult(True, "grant",
                           "授予锁 %s (%s) fencing_token=%d"
                           % (lock_name, mode.value, grant.fencing_token))

    def release(self, coordinator, session, txn, lock_name, now):
        # type: (...) -> EventResult
        coord = self.coordinators.get(coordinator)
        if coord is None:
            return EventResult(False, "release", "未知协调器 %s" % coordinator)
        self._purge_expired(now)
        for grant in coord.grants.get(lock_name, []):
            if grant.txn == txn and grant.session == session:
                grant.released = True
                return EventResult(True, "release",
                                   "释放锁 %s (事务 %s)" % (lock_name, txn))
        holders = ", ".join(
            "%s/%s" % (g.session, g.txn)
            for c in self.coordinators.values()
            for g in c.grants.get(lock_name, [])) or "无"
        return EventResult(False, "release",
                           "会话 %s/事务 %s 不持有锁 %s（当前持有者：%s）"
                           % (session, txn, lock_name, holders))

    def begin_migration(self, migration_id, shard, source, target, new_epoch, now):
        # type: (...) -> EventResult
        if self.migration and self.migration.phase in _ACTIVE_PHASES:
            return EventResult(False, "migrate", "已有进行中的迁移 %s"
                               % self.migration.migration_id)
        if new_epoch <= self.authoritative_epoch:
            return EventResult(False, "migrate",
                               "新 epoch %d 必须大于权威 epoch %d"
                               % (new_epoch, self.authoritative_epoch))
        self.migration = Migration(migration_id=migration_id, shard=shard,
                                   source=source, target=target,
                                   new_epoch=new_epoch)
        return EventResult(True, "migrate",
                           "迁移 %s 开始：%s -> %s (shard %s)"
                           % (migration_id, source, target, shard))

    def advance_migration(self, phase, now):
        # type: (MigrationPhase, float) -> EventResult
        mig = self.migration
        if mig is None:
            return EventResult(False, "migrate", "没有进行中的迁移")
        allowed = _PHASE_ORDER.get(mig.phase, ())
        if phase not in allowed:
            return EventResult(False, "migrate",
                               "非法阶段跳转 %s -> %s" % (mig.phase.value, phase.value))

        if phase == MigrationPhase.CUTOVER:
            self._purge_expired(now)
            leftover = [g for c in self.coordinators.values()
                        if c.name == mig.source
                        for grants in c.grants.values() for g in grants]
            if leftover:
                names = ", ".join(sorted({g.lock_name for g in leftover}))
                self.migration_blockers.append(
                    "CUTOVER 时源协调器 %s 仍持有 advisory lock：%s；"
                    "这些授予将随 epoch 提升失效" % (mig.source, names))
                self.safe_retry_steps.append(
                    "等待源协调器 %s 上的租约到期或显式释放锁：%s"
                    % (mig.source, names))
            self.authoritative_epoch = mig.new_epoch
            self.coordinators[mig.target].epoch = mig.new_epoch
            self.safe_retry_steps.append(
                "迁移 %s 完成 CUTOVER：权威 epoch 提升为 %d，"
                "旧 epoch 的写入一律 fencing" % (mig.migration_id, mig.new_epoch))

        if phase in (MigrationPhase.COMPLETE, MigrationPhase.ABORTED):
            if phase == MigrationPhase.ABORTED:
                self.safe_retry_steps.append(
                    "迁移 %s 已中止：源协调器 %s 的授予在 epoch %d 下继续有效，"
                    "被阻断的申请可安全重试"
                    % (mig.migration_id, mig.source, self.authoritative_epoch))
            self.migration = None
        else:
            mig.phase = phase
        return EventResult(True, "migrate", "迁移进入 %s 阶段" % phase.value)

    def write(self, session, txn, lock_name, resource, value, epoch,
              fencing_token, now):
        # type: (...) -> EventResult
        record = {"session": session, "txn": txn, "lock": lock_name,
                  "resource": resource, "value": value,
                  "epoch": epoch, "fencing_token": fencing_token}

        def fence(reason):
            record["reason"] = reason
            self.conflicting_writes.append(dict(record))
            return EventResult(False, "write", reason)

        if epoch < self.authoritative_epoch:
            self.safe_retry_steps.append(
                "会话 %s 的写入携带旧 epoch %d：在 epoch %d 的权威协调器上"
                "重新获取锁 %s 并以新 fencing token 重试写入资源 %s"
                % (session, epoch, self.authoritative_epoch, lock_name, resource))
            return fence("旧 epoch %d 的写入被 fencing（权威 epoch %d）"
                         % (epoch, self.authoritative_epoch))

        valid = [g for g in self._valid_grants(lock_name, now)
                 if g.session == session and g.txn == txn]
        if not valid:
            self.safe_retry_steps.append(
                "会话 %s 不持有锁 %s 的有效授予：重新申请后再写入资源 %s"
                % (session, lock_name, resource))
            return fence("会话 %s/事务 %s 不持有锁 %s 的有效授予"
                         % (session, txn, lock_name))

        grant = valid[0]
        if grant.mode != LockMode.EXCLUSIVE:
            return fence("写入需要 EXCLUSIVE 锁，当前为 %s" % grant.mode.value)
        if fencing_token != grant.fencing_token:
            return fence("fencing token %d 与授予 token %d 不符"
                         % (fencing_token, grant.fencing_token))
        highest = self._resource_max_token.get(resource, 0)
        if fencing_token <= highest:
            return fence("fencing token %d 不大于资源 %s 已接受的 token %d"
                         % (fencing_token, resource, highest))

        self._resource_max_token[resource] = fencing_token
        self.accepted_writes.append(dict(record))
        return EventResult(True, "write", "写入资源 %s 成功" % resource)

    def holders(self, now):
        # type: (float) -> Dict[str, List[dict]]
        result = {}
        names = {g.lock_name for c in self.coordinators.values()
                 for grants in c.grants.values() for g in grants}
        for name in sorted(names):
            valid = self._valid_grants(name, now)
            if valid:
                result[name] = [{
                    "session": g.session, "txn": g.txn, "mode": g.mode.value,
                    "epoch": g.epoch, "fencing_token": g.fencing_token,
                    "coordinator": g.coordinator,
                } for g in valid]
        return result

    def dual_owner_violations(self, now):
        # type: (float) -> List[str]
        """检查同名锁是否存在两个有效 owner（迁移后绝不允许）。"""
        violations = []
        names = {g.lock_name for c in self.coordinators.values()
                 for grants in c.grants.values() for g in grants}
        for name in sorted(names):
            valid = self._valid_grants(name, now)
            txns = {g.txn for g in valid}
            exclusive = [g for g in valid if g.mode == LockMode.EXCLUSIVE]
            if len(txns) > 1 and exclusive:
                violations.append(
                    "锁 %s 存在多个有效 owner：%s"
                    % (name, ", ".join(sorted(txns))))
        return violations

    def report(self, now):
        # type: (float) -> dict
        return {
            "authoritative_epoch": self.authoritative_epoch,
            "holders": self.holders(now),
            "conflicting_writes": list(self.conflicting_writes),
            "migration_blockers": list(self.migration_blockers),
            "safe_retry_steps": list(dict.fromkeys(self.safe_retry_steps)),
            "dual_owner_violations": self.dual_owner_violations(now),
        }
