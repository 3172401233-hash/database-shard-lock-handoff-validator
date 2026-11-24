# -*- coding: utf-8 -*-
"""锁交接验证器测试：共享锁、更新锁升级、迁移中断、旧会话晚写。"""

import unittest

from lock_handoff import (
    LockHandoffValidator,
    LockMode,
    MigrationPhase,
)

LEASE = (0.0, 100.0)


def make_validator():
    v = LockHandoffValidator()
    v.add_coordinator("c-old", epoch=1)
    v.add_coordinator("c-new", epoch=1)
    return v


class SharedLockTest(unittest.TestCase):
    def test_shared_locks_coexist_and_block_exclusive(self):
        v = make_validator()
        r1 = v.grant("c-old", "s1", "t1", "orders", LockMode.SHARED, LEASE, now=0)
        r2 = v.grant("c-old", "s2", "t2", "orders", LockMode.SHARED, LEASE, now=1)
        self.assertTrue(r1)
        self.assertTrue(r2)

        r3 = v.grant("c-old", "s3", "t3", "orders", LockMode.EXCLUSIVE, LEASE, now=2)
        self.assertFalse(r3)
        self.assertIn("不兼容", r3.reason)

        holders = v.holders(now=3)["orders"]
        self.assertEqual({h["txn"] for h in holders}, {"t1", "t2"})
        self.assertEqual(v.dual_owner_violations(now=3), [])

    def test_shared_lock_release_allows_exclusive(self):
        v = make_validator()
        v.grant("c-old", "s1", "t1", "orders", LockMode.SHARED, LEASE, now=0)
        bad = v.release("c-old", "s9", "t9", "orders", now=1)
        self.assertFalse(bad)

        ok = v.release("c-old", "s1", "t1", "orders", now=1)
        self.assertTrue(ok)
        r = v.grant("c-old", "s2", "t2", "orders", LockMode.EXCLUSIVE, LEASE, now=2)
        self.assertTrue(r)

    def test_lease_expiry_frees_lock(self):
        v = make_validator()
        v.grant("c-old", "s1", "t1", "orders", LockMode.EXCLUSIVE, (0, 10), now=0)
        blocked = v.grant("c-old", "s2", "t2", "orders", LockMode.EXCLUSIVE,
                          LEASE, now=5)
        self.assertFalse(blocked)
        granted = v.grant("c-old", "s2", "t2", "orders", LockMode.EXCLUSIVE,
                          LEASE, now=11)
        self.assertTrue(granted)


class UpdateLockUpgradeTest(unittest.TestCase):
    def test_update_lock_upgrades_to_exclusive_then_writes(self):
        v = make_validator()
        r = v.grant("c-old", "s1", "t1", "stock", LockMode.UPDATE, LEASE, now=0)
        self.assertTrue(r)

        # UPDATE 与 UPDATE 不兼容
        conflict = v.grant("c-old", "s2", "t2", "stock", LockMode.UPDATE,
                           LEASE, now=1)
        self.assertFalse(conflict)

        # UPDATE 不能直接写，需要升级
        token = 1
        denied = v.write("s1", "t1", "stock", "res/stock", "v1",
                         epoch=1, fencing_token=token, now=2)
        self.assertFalse(denied)
        self.assertIn("EXCLUSIVE", denied.reason)

        up = v.grant("c-old", "s1", "t1", "stock", LockMode.EXCLUSIVE,
                     LEASE, now=3)
        self.assertTrue(up)
        self.assertIn("升级", up.reason)

        written = v.write("s1", "t1", "stock", "res/stock", "v1",
                          epoch=1, fencing_token=token, now=4)
        self.assertTrue(written)
        self.assertEqual(len(v.accepted_writes), 1)

    def test_upgrade_blocked_by_other_holder(self):
        v = make_validator()
        v.grant("c-old", "s1", "t1", "stock", LockMode.UPDATE, LEASE, now=0)
        v.grant("c-old", "s2", "t2", "stock", LockMode.SHARED, LEASE, now=1)
        up = v.grant("c-old", "s1", "t1", "stock", LockMode.EXCLUSIVE,
                     LEASE, now=2)
        self.assertFalse(up)
        self.assertIn("其他持有者", up.reason)


class MigrationInterruptTest(unittest.TestCase):
    def test_grants_blocked_during_migration_and_restored_after_abort(self):
        v = make_validator()
        v.grant("c-old", "s1", "t1", "orders", LockMode.EXCLUSIVE, LEASE, now=0)

        self.assertTrue(v.begin_migration("m1", "shardA", "c-old", "c-new",
                                          new_epoch=2, now=1))
        self.assertTrue(v.advance_migration(MigrationPhase.COPY, now=2))

        # 迁移期间源协调器上的新授予被阻断
        blocked = v.grant("c-old", "s2", "t2", "orders", LockMode.SHARED,
                          LEASE, now=3)
        self.assertFalse(blocked)
        self.assertTrue(any("阻断" in b for b in v.migration_blockers))

        # 非法阶段跳转被拒绝
        illegal = v.advance_migration(MigrationPhase.COMPLETE, now=4)
        self.assertFalse(illegal)

        # 迁移中断：中止后源协调器恢复授予
        self.assertTrue(v.advance_migration(MigrationPhase.ABORTED, now=5))
        retry = v.grant("c-old", "s2", "t2", "orders", LockMode.SHARED,
                        LEASE, now=6)
        self.assertFalse(retry)  # t1 的 EXCLUSIVE 仍持有，模式不兼容
        retry2 = v.grant("c-old", "s2", "t2", "items", LockMode.SHARED,
                         LEASE, now=6)
        self.assertTrue(retry2)
        self.assertEqual(v.authoritative_epoch, 1)
        steps = v.report(now=7)["safe_retry_steps"]
        self.assertTrue(any("已中止" in s for s in steps))


class LateWriteFencingTest(unittest.TestCase):
    def _migrate_with_leftover_lock(self):
        v = make_validator()
        old = v.grant("c-old", "s1", "t1", "orders", LockMode.EXCLUSIVE,
                      LEASE, now=0)
        self.assertTrue(old)
        old_token = 1

        self.assertTrue(v.begin_migration("m1", "shardA", "c-old", "c-new",
                                          new_epoch=2, now=1))
        self.assertTrue(v.advance_migration(MigrationPhase.COPY, now=2))
        # 旧协调器仍持有 advisory lock 就进入 CUTOVER
        self.assertTrue(v.advance_migration(MigrationPhase.CUTOVER, now=3))
        self.assertTrue(v.advance_migration(MigrationPhase.COMPLETE, now=4))
        return v, old_token

    def test_cutover_with_leftover_lock_is_flagged_and_fenced(self):
        v, old_token = self._migrate_with_leftover_lock()

        # 迁移阻断项记录了旧协调器未释放的锁
        self.assertTrue(any("仍持有 advisory lock" in b
                            for b in v.migration_blockers))

        # 新协调器在 epoch 2 重新授予同名锁
        new_grant = v.grant("c-new", "s2", "t2", "orders",
                            LockMode.EXCLUSIVE, LEASE, now=5)
        self.assertTrue(new_grant)

        # 同名锁只有一个有效 owner（旧授予随 epoch 失效）
        holders = v.holders(now=6)["orders"]
        self.assertEqual(len(holders), 1)
        self.assertEqual(holders[0]["txn"], "t2")
        self.assertEqual(holders[0]["epoch"], 2)
        self.assertEqual(v.dual_owner_violations(now=6), [])

        # 旧会话晚写：携带旧 epoch，必须被 fencing
        late = v.write("s1", "t1", "orders", "res/orders", "stale",
                       epoch=1, fencing_token=old_token, now=7)
        self.assertFalse(late)
        self.assertIn("fencing", late.reason)
        self.assertEqual(len(v.conflicting_writes), 1)
        self.assertEqual(v.conflicting_writes[0]["session"], "s1")

        # 新 owner 的写入被接受
        new_token = 2
        accepted = v.write("s2", "t2", "orders", "res/orders", "fresh",
                           epoch=2, fencing_token=new_token, now=8)
        self.assertTrue(accepted)

        # 旧会话用旧 token 伪装新 epoch 也被拒绝（token 不属于其授予）
        fake = v.write("s1", "t1", "orders", "res/orders", "stale2",
                       epoch=2, fencing_token=old_token, now=9)
        self.assertFalse(fake)

        report = v.report(now=10)
        self.assertEqual(report["authoritative_epoch"], 2)
        self.assertTrue(any("重新获取锁" in s
                            for s in report["safe_retry_steps"]))

    def test_stale_coordinator_grant_rejected(self):
        v, _ = self._migrate_with_leftover_lock()
        stale = v.grant("c-old", "s3", "t3", "orders", LockMode.SHARED,
                        LEASE, now=5)
        self.assertFalse(stale)
        self.assertIn("落后", stale.reason)


if __name__ == "__main__":
    unittest.main()
