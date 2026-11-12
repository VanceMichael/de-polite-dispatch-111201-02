import threading
import unittest

from collector.dispatch import (
    Dispatcher,
    ManualClock,
    Outcome,
    QuotaConfig,
    SystemClock,
)
from collector.domain import Task
from collector.simulate import ScriptedExecutor


def task(identifier, provider='p1', account='a', priority=0, cost=1, deadline=100):
    return Task(identifier, provider, account, priority, cost, deadline)


def make(config=None, now=0.0, executor=None):
    clock = ManualClock(now)
    dispatcher = Dispatcher(config or QuotaConfig(global_concurrency=5), clock,
                            executor=executor or ScriptedExecutor())
    return dispatcher, clock


class QuotaConfigTests(unittest.TestCase):
    def test_alias_resolution(self):
        config = QuotaConfig.from_dict({
            'global_concurrency': 3,
            'aliases': {'api.one.example': 'p1', 'download.one.example': 'p1'},
        })
        self.assertEqual(config.resolve_provider('download.one.example'), 'p1')
        self.assertEqual(config.resolve_provider('p2'), 'p2')

    def test_invalid_config_rejected(self):
        with self.assertRaises(ValueError):
            QuotaConfig.from_dict({'global_concurrency': 0})
        with self.assertRaises(ValueError):
            QuotaConfig.from_dict({'provider_limits': {'p1': 0}})
        with self.assertRaises(ValueError):
            QuotaConfig.from_dict(
                {'account_buckets': {'p1:a': {'capacity': 0, 'refill_per_second': 1}}})


class ConcurrencyTests(unittest.TestCase):
    def test_global_and_provider_limits_hold_together(self):
        config = QuotaConfig(global_concurrency=3,
                             provider_limits={'p1': 2, 'p2': 2})
        dispatcher, _ = make(config)
        dispatcher.submit_all([
            task('A', 'p1'), task('B', 'p1'), task('C', 'p1'),
            task('D', 'p2'), task('E', 'p2'),
        ])
        claimed = [dispatcher.claim(f'w{i}') for i in range(4)]
        self.assertEqual([lease.task_id for lease in claimed if lease],
                         ['A', 'B', 'D'])
        self.assertIsNone(claimed[3])  # 全局已满, E 不能派发
        reasons = {}
        for record in dispatcher.records():
            if record['kind'] == 'skip':
                reasons.setdefault(record['task'], set()).add(record['why'])
        self.assertTrue(any('供应方p1并发已满' in why for why in reasons['C']))
        self.assertTrue(any('全局并发已满' in why for why in reasons['E']))

    def test_aliased_domains_share_provider_limit_and_bucket(self):
        config = QuotaConfig(
            global_concurrency=5,
            provider_limits={'p1': 2},
            aliases={'api.one.example': 'p1', 'download.one.example': 'p1'},
            account_buckets={'p1:a': (4.0, 1.0)})
        dispatcher, _ = make(config)
        dispatcher.submit_all([
            task('A', 'api.one.example', cost=1),
            task('B', 'download.one.example', cost=1),
            task('C', 'api.one.example', cost=1),
        ])
        self.assertIsNotNone(dispatcher.claim('w1'))
        self.assertIsNotNone(dispatcher.claim('w2'))
        self.assertIsNone(dispatcher.claim('w3'))  # 两个域名共用 p1=2
        metrics = dispatcher.metrics()
        self.assertEqual(metrics['concurrency']['providers']['p1']['inflight'], 2)
        self.assertEqual(metrics['buckets']['p1:a']['reserved'], 2)

    def test_token_budget_blocks_without_serializing(self):
        config = QuotaConfig(global_concurrency=5,
                             provider_limits={'p1': 5},
                             account_buckets={'p1:a': (4.0, 1.0)})
        dispatcher, _ = make(config)
        dispatcher.submit_all([task('A', cost=2), task('B', cost=2), task('C', cost=1)])
        self.assertIsNotNone(dispatcher.claim('w1'))
        self.assertIsNotNone(dispatcher.claim('w2'))
        self.assertIsNone(dispatcher.claim('w3'))  # 4 个令牌已预留完
        why = dispatcher.explain('C')[-1]['why']
        self.assertIn('令牌不足', why)


class TokenLifecycleTests(unittest.TestCase):
    def test_reserved_tokens_consumed_only_when_sent(self):
        config = QuotaConfig(global_concurrency=5,
                             account_buckets={'p1:a': (4.0, 1.0)})
        dispatcher, _ = make(config)
        dispatcher.submit(task('A', cost=2))
        lease = dispatcher.claim('w1')
        bucket = dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual(bucket['reserved'], 2)
        self.assertEqual(bucket['consumed'], 0)
        dispatcher.worker_exit('w1')  # 未发出即交还: 释放预留, 不消费
        bucket = dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual(bucket['reserved'], 0)
        self.assertEqual(bucket['consumed'], 0)
        lease = dispatcher.claim('w2')
        self.assertTrue(dispatcher.mark_sent(lease))
        bucket = dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual(bucket['consumed'], 2)
        self.assertEqual(bucket['reserved'], 0)

    def test_mark_sent_is_idempotent(self):
        config = QuotaConfig(global_concurrency=5,
                             account_buckets={'p1:a': (4.0, 1.0)})
        dispatcher, _ = make(config)
        dispatcher.submit(task('A', cost=2))
        lease = dispatcher.claim('w1')
        self.assertTrue(dispatcher.mark_sent(lease))
        self.assertTrue(dispatcher.mark_sent(lease))
        self.assertEqual(dispatcher.metrics()['buckets']['p1:a']['consumed'], 2)


class FairnessTests(unittest.TestCase):
    def test_live_first_but_backlog_ages_in(self):
        config = QuotaConfig(global_concurrency=8, aging_rate=1.0)
        dispatcher, clock = make(config)
        dispatcher.submit(task('backlog', priority=0, deadline=1000))
        claimed = []
        for tick in range(4):
            clock.set(tick)
            dispatcher.submit(task(f'live-{tick}', priority=2, deadline=30 + tick))
            lease = dispatcher.claim('w')
            claimed.append(lease.task_id)
            dispatcher.execute_lease(lease)
        self.assertEqual(claimed[:3], ['live-0', 'live-1', 'live-2'])
        self.assertEqual(claimed[3], 'backlog')  # 等待 3 秒后老化超过实时任务
        waits = dispatcher.metrics()['wait_seconds']
        self.assertEqual(waits['backlog'], 3)

    def test_expired_task_leaves_queue_with_reason(self):
        dispatcher, clock = make()
        dispatcher.submit(task('A', deadline=5))
        clock.set(5)
        self.assertIsNone(dispatcher.claim('w'))
        self.assertEqual(dispatcher.pending(), [])
        expired = [record for record in dispatcher.records()
                   if record['kind'] == 'expired']
        self.assertEqual(len(expired), 1)
        self.assertIn('超过最晚执行时间', expired[0]['why'])
        self.assertEqual(dispatcher.metrics()['tasks']['expired'], 1)


class LeaseTests(unittest.TestCase):
    def test_lease_expiry_reclaims_and_fencing_rejects_stale_worker(self):
        config = QuotaConfig(global_concurrency=5, lease_seconds=10)
        dispatcher, clock = make(config)
        dispatcher.submit(task('A'))
        old = dispatcher.claim('w1')
        clock.set(11)  # 租约到期
        new = dispatcher.claim('w2')
        self.assertIsNotNone(new)
        self.assertGreater(new.generation, old.generation)
        self.assertFalse(dispatcher.mark_sent(old))  # 旧 worker 禁止发送
        result = dispatcher.complete(old, Outcome.ok())
        self.assertFalse(result.accepted)
        self.assertTrue(dispatcher.mark_sent(new))
        self.assertTrue(dispatcher.complete(new, Outcome.ok()).accepted)
        self.assertEqual(dispatcher.metrics()['stale_completions_rejected'], 1)
        kinds = [record['kind'] for record in dispatcher.records()]
        self.assertIn('lease_reclaimed', kinds)
        self.assertIn('stale_complete', kinds)

    def test_worker_exit_returns_leases_and_stale_complete_rejected(self):
        config = QuotaConfig(global_concurrency=5, lease_seconds=100)
        dispatcher, _ = make(config)
        dispatcher.submit_all([task('A'), task('B')])
        leases = [dispatcher.claim('w1'), dispatcher.claim('w1')]
        returned = dispatcher.worker_exit('w1')
        self.assertEqual(returned, ['A', 'B'])
        lease = dispatcher.claim('w2')
        self.assertEqual(lease.task_id, 'A')
        dispatcher.execute_lease(lease)
        result = dispatcher.complete(leases[0], Outcome.ok())
        self.assertFalse(result.accepted)  # 晚到的旧 worker 不得完成已转交任务
        self.assertEqual(dispatcher.metrics()['stale_completions_rejected'], 1)
        self.assertEqual(dispatcher.metrics()['tasks']['done'], 1)
        kinds = [record['kind'] for record in dispatcher.records()]
        self.assertIn('returned', kinds)

    def test_duplicate_send_detected_after_reclaim(self):
        config = QuotaConfig(global_concurrency=5, lease_seconds=10)
        dispatcher, clock = make(config)
        dispatcher.submit(task('A'))
        first = dispatcher.claim('w1')
        dispatcher.mark_sent(first)  # 请求已发出但 worker 失联
        clock.set(11)
        second = dispatcher.claim('w2')
        self.assertIsNotNone(second)
        dispatcher.mark_sent(second)  # 同一任务第二次发出: 计为重复
        duplicates = dispatcher.metrics()['duplicates']
        self.assertEqual(duplicates['extra_sends'], 1)
        self.assertEqual(duplicates['sends']['A'], 2)


class OutcomeTests(unittest.TestCase):
    def test_429_freezes_only_matching_bucket(self):
        config = QuotaConfig(global_concurrency=5,
                             provider_limits={'p1': 2, 'p2': 2},
                             account_buckets={'p1:a': (4.0, 1.0)})
        dispatcher, clock = make(config)
        dispatcher.submit_all([
            task('A', 'p1', 'a'), task('B', 'p1', 'a'), task('C', 'p2', 'b'),
        ])
        lease_a = dispatcher.claim('w1')
        clock.set(1)
        dispatcher.mark_sent(lease_a)
        dispatcher.complete(lease_a, Outcome.throttled(5))
        bucket = dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual(bucket['frozen_until'], 6.0)
        self.assertEqual(bucket['consumed'], 1)
        clock.set(2)
        lease_c = dispatcher.claim('w2')  # p2 不受 p1:a 冻结影响
        self.assertEqual(lease_c.task_id, 'C')
        self.assertIsNone(dispatcher.claim('w3'))
        reasons = {record['task']: record['why']
                   for record in dispatcher.records() if record['kind'] == 'skip'}
        self.assertIn('冻结', reasons['B'])
        clock.set(6)  # 解冻后恢复
        self.assertEqual(dispatcher.claim('w3').task_id, 'A')
        self.assertEqual(dispatcher.claim('w3').task_id, 'B')

    def test_retryable_requeues_with_backoff(self):
        config = QuotaConfig(global_concurrency=5, backoff_base=2.0)
        dispatcher, clock = make(config)
        dispatcher.submit(task('A'))
        lease = dispatcher.claim('w1')
        dispatcher.mark_sent(lease)
        dispatcher.complete(lease, Outcome.retryable('连接重置'))
        self.assertIsNone(dispatcher.claim('w2'))  # 退避中
        self.assertIn('退避', dispatcher.explain('A')[-1]['why'])
        clock.set(2)
        self.assertIsNotNone(dispatcher.claim('w2'))

    def test_permanent_goes_dead_letter(self):
        dispatcher, _ = make()
        dispatcher.submit(task('A'))
        lease = dispatcher.claim('w1')
        dispatcher.mark_sent(lease)
        dispatcher.complete(lease, Outcome.permanent('403 拒绝'))
        self.assertIsNone(dispatcher.claim('w2'))
        dead = [record for record in dispatcher.records()
                if record['kind'] == 'dead_letter']
        self.assertEqual(len(dead), 1)
        self.assertIn('永久拒绝', dead[0]['why'])
        self.assertEqual(dispatcher.metrics()['tasks']['dead'], 1)

    def test_retry_exhaustion_goes_dead_letter(self):
        config = QuotaConfig(global_concurrency=5, max_attempts=2, backoff_base=0.0)
        dispatcher, clock = make(config)
        dispatcher.submit(task('A'))
        for _ in range(2):
            lease = dispatcher.claim('w1')
            dispatcher.mark_sent(lease)
            dispatcher.complete(lease, Outcome.retryable('超时'))
        self.assertIsNone(dispatcher.claim('w2'))
        dead = [record for record in dispatcher.records()
                if record['kind'] == 'dead_letter']
        self.assertIn('超过最大重试次数', dead[0]['why'])


class ExplainabilityTests(unittest.TestCase):
    def test_pending_and_explain_report_blocking_reason(self):
        config = QuotaConfig(global_concurrency=5,
                             account_buckets={'p1:a': (1.0, 0.0)})
        dispatcher, _ = make(config)
        dispatcher.submit(task('A', cost=2))  # 成本超过容量, 永远不够
        self.assertIsNone(dispatcher.claim('w1'))
        pending = dispatcher.pending()
        self.assertEqual(pending[0]['task'], 'A')
        self.assertIn('令牌不足', pending[0]['why'])
        self.assertTrue(any(record['kind'] == 'skip'
                            for record in dispatcher.explain('A')))

    def test_claim_specific_task(self):
        dispatcher, _ = make()
        dispatcher.submit_all([task('A', priority=5), task('B', priority=0)])
        lease = dispatcher.claim('w1', task_id='B')
        self.assertEqual(lease.task_id, 'B')


class ParallelismTests(unittest.TestCase):
    def test_execution_not_serialized(self):
        barrier = threading.Barrier(3)

        class BarrierExecutor:
            def execute(self, task):
                barrier.wait(timeout=10)  # 串行执行会在此超时
                return Outcome.ok()

        config = QuotaConfig(global_concurrency=3)
        dispatcher = Dispatcher(config, SystemClock(), BarrierExecutor())
        for index in range(3):
            dispatcher.submit(task(f'T{index}', provider=f'p{index}',
                                   deadline=10 ** 9))
        leases = [dispatcher.claim(f'w{index}') for index in range(3)]
        self.assertTrue(all(leases))
        threads = [threading.Thread(target=dispatcher.execute_lease, args=(lease,))
                   for lease in leases]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)
        self.assertFalse(any(thread.is_alive() for thread in threads))
        self.assertEqual(dispatcher.metrics()['tasks']['done'], 3)
        sends = dispatcher.metrics()['duplicates']['sends']
        self.assertEqual(sorted(sends.values()), [1, 1, 1])


if __name__ == '__main__':
    unittest.main()
