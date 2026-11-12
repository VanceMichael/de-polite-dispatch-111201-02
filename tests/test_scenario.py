import unittest

from collector.domain import Task
from collector.simulate import Scenario, load_quotas, run_quotas_demo


def backlog_tasks():
    return [
        Task('history-1', 'api.one.example', 'a', 1, 2, 100),
        Task('history-2', 'download.one.example', 'a', 1, 1, 100),
    ]


class FixtureScenarioTests(unittest.TestCase):
    """逐事件回放 fixtures/quotas.json 的共享预算与调度事件。"""

    def setUp(self):
        self.config, self.events = load_quotas('fixtures/quotas.json')
        self.scenario = Scenario(self.config, backlog_tasks())
        self.dispatcher = self.scenario.dispatcher

    def test_fixture_events_step_by_step(self):
        claim, throttled, exit_, stale = self.events

        # t=0: w1 循环领取, 两个域名共用 p1=2, 各领一个, 令牌仅预留
        granted = self.scenario.apply(claim)
        self.assertEqual([lease.task_id for lease in granted],
                         ['history-1', 'history-2'])
        bucket = self.dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual((bucket['reserved'], bucket['consumed']), (3, 0))

        # t=1: 429 落在 p1:a 最近领取的 history-2 上, 请求发出消费 1 令牌, 桶冻结至 t=6
        self.scenario.apply(throttled)
        bucket = self.dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual(bucket['frozen_until'], 6.0)
        self.assertEqual(bucket['consumed'], 1)
        held = self.scenario.leases['w1']
        self.assertEqual([lease.task_id for lease in held], ['history-1'])

        # t=3: w1 停机交还 history-1, 预留释放且从未消费
        returned = self.scenario.apply(exit_)
        self.assertEqual(returned, ['history-1'])
        bucket = self.dispatcher.metrics()['buckets']['p1:a']
        self.assertEqual((bucket['reserved'], bucket['consumed']), (0, 1))
        pending = {item['task']: item['why'] for item in self.dispatcher.pending()}
        self.assertIn('冻结', pending['history-1'])
        self.assertIn('退避', pending['history-2'])

        # t=8: 晚到的旧 worker 不得完成已交还的任务
        results = self.scenario.apply(stale)
        self.assertTrue(results)
        self.assertFalse(any(result.accepted for result in results))
        self.assertEqual(self.dispatcher.metrics()['stale_completions_rejected'], 1)

        # 解冻后其他 worker 接手完成, 无重复请求
        for expected in ('history-1', 'history-2'):
            lease = self.dispatcher.claim('w2')
            self.assertEqual(lease.task_id, expected)
            self.assertTrue(self.dispatcher.execute_lease(lease).accepted)
        metrics = self.dispatcher.metrics()
        self.assertEqual(metrics['tasks']['done'], 2)
        self.assertEqual(metrics['buckets']['p1:a']['consumed'], 4)
        self.assertEqual(metrics['duplicates']['extra_sends'], 0)

    def test_quotas_demo_tells_full_story(self):
        result = run_quotas_demo('fixtures/quotas.json')
        metrics = result['metrics']
        bucket = metrics['buckets']['p1:a']
        self.assertEqual(bucket['consumed'], 4)
        self.assertEqual(bucket['freezes'], 1)
        self.assertEqual(bucket['frozen_seconds'], 5)
        self.assertAlmostEqual(bucket['utilization'], 0.5)
        self.assertEqual(metrics['tasks']['done'], 3)
        self.assertEqual(metrics['stale_completions_rejected'], 1)
        self.assertEqual(metrics['duplicates']['extra_sends'], 0)
        kinds = [record['kind'] for record in result['records']]
        for expected in ('submit', 'dispatch', 'send', 'freeze', 'requeue',
                         'returned', 'worker_exit', 'stale_complete', 'complete'):
            self.assertIn(expected, kinds)
        # t=4 的领取被冻结挡住, 运行记录给出明确原因
        frozen_skips = [record for record in result['records']
                        if record['kind'] == 'skip' and '冻结' in record['why']]
        self.assertTrue(frozen_skips)


if __name__ == '__main__':
    unittest.main()
