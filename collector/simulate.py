"""配额配置与调度事件的本地回放。

读取 fixtures/quotas.json 一类的配置(共享预算、域名别名、调度事件),
用注入的手动时钟与脚本执行器回放整个调度过程, 不访问网络。
事件动作: submit / claim / send / execute / 429 / worker_exit / stale_complete / reap。
claim 表示 worker 循环领取直到没有可派发任务; 429 落在该配额桶最近领取的租约上。
"""

from __future__ import annotations

import json
from pathlib import Path

from .dispatch import Dispatcher, ManualClock, Outcome, QuotaConfig
from .domain import Task


class ScriptedExecutor:
    """按任务标识依次弹出预置结果的执行器, 缺省成功; 仅用于本地回放与测试。"""

    def __init__(self, script=None):
        self.script = {key: list(value) for key, value in (script or {}).items()}
        self.calls = []

    def execute(self, task):
        self.calls.append(task.identifier)
        outcomes = self.script.get(task.identifier)
        if outcomes:
            return outcomes.pop(0)
        return Outcome.ok()


class Scenario:
    """按时间顺序回放调度事件, 驱动 Dispatcher 并记住各 worker 持有的租约。"""

    def __init__(self, config: QuotaConfig, tasks=(), executor=None):
        self.clock = ManualClock(0.0)
        self.executor = executor or ScriptedExecutor()
        self.dispatcher = Dispatcher(config, self.clock, executor=self.executor)
        self.leases = {}        # worker -> [Lease] 当前持有
        self.invalidated = {}   # worker -> [Lease] 已交还或回收, 用于重演晚到完成
        self._order = {}        # Lease -> 领取序号
        self._seq = 0
        for task in tasks:
            self.dispatcher.submit(task)

    def run(self, events):
        for event in events:
            self.apply(event)
        return self.dispatcher

    def apply(self, event):
        self.clock.set(event['time'])
        action = event['action']
        if action == 'submit':
            self.dispatcher.submit(Task.parse(event['task']))
            return None
        if action == 'claim':
            return self._claim(event)
        if action == 'send':
            lease = self._current(event['worker'], event.get('task'))
            if lease is None:
                raise RuntimeError(f'worker 没有持有租约: {event}')
            return self.dispatcher.mark_sent(lease)
        if action == 'execute':
            lease = self._current(event['worker'], event.get('task'))
            if lease is None:
                raise RuntimeError(f'worker 没有可执行的租约: {event}')
            result = self.dispatcher.execute_lease(lease)
            self._forget(lease)
            return result
        if action == '429':
            lease = self._match_bucket(event['bucket'])
            if lease is None:
                raise RuntimeError(f"429 事件没有匹配的租约: {event['bucket']}")
            self.dispatcher.mark_sent(lease)
            result = self.dispatcher.complete(
                lease, Outcome.throttled(event.get('retry_after', 0)))
            self._forget(lease)
            return result
        if action == 'worker_exit':
            worker = event['worker']
            self.invalidated.setdefault(worker, []).extend(self.leases.pop(worker, []))
            return self.dispatcher.worker_exit(worker)
        if action == 'stale_complete':
            worker = event['worker']
            return [self.dispatcher.complete(lease, Outcome.ok())
                    for lease in self.invalidated.get(worker, [])]
        if action == 'reap':
            self.dispatcher.reap()
            return None
        raise ValueError(f'未知事件: {action}')

    def _claim(self, event):
        worker = event['worker']
        granted = []
        while True:
            lease = self.dispatcher.claim(worker, task_id=event.get('task'))
            if lease is None:
                break
            granted.append(lease)
            self.leases.setdefault(worker, []).append(lease)
            self._order[lease] = self._seq
            self._seq += 1
            if event.get('task') or len(granted) >= event.get('max', 99):
                break
        return granted

    def _current(self, worker, task_id=None):
        held = self.leases.get(worker, [])
        if task_id is None:
            return held[-1] if held else None
        for lease in reversed(held):
            if lease.task_id == task_id:
                return lease
        return None

    def _forget(self, lease):
        for held in self.leases.values():
            if lease in held:
                held.remove(lease)
                return

    def _match_bucket(self, bucket_key):
        candidates = []
        for held in self.leases.values():
            for lease in held:
                view = self.dispatcher.lease_view(lease)
                if view and view['valid'] and view['bucket'] == bucket_key:
                    candidates.append((view, lease))
        if not candidates:
            return None
        unsent = [(view, lease) for view, lease in candidates if not view['sent']]
        pool = unsent or candidates
        return max(pool, key=lambda item: self._order[item[1]])[1]


def load_quotas(path):
    """读取共享预算配置与调度事件; events 原样返回, 不参与配额解析。"""
    data = json.loads(Path(path).read_text(encoding='utf-8'))
    return QuotaConfig.from_dict(data), data.get('events', [])


def run_quotas_demo(quotas_path='fixtures/quotas.json'):
    """回放 fixtures/quotas.json 的共享预算与调度事件。

    两个域名经别名共用 p1 的并发与 p1:a 令牌桶; 演示 429 冻结只影响该桶、
    停机交还未发出的预留不消费、晚到的旧 worker 完成被拒绝, 解冻后积压恢复。
    """
    config, fixture_events = load_quotas(quotas_path)
    scenario = Scenario(config)
    driver = [
        {'time': 0, 'action': 'submit', 'task': dict(
            id='history-1', provider='api.one.example', account='a',
            priority=1, cost=2, deadline=100)},
        {'time': 0, 'action': 'submit', 'task': dict(
            id='history-2', provider='download.one.example', account='a',
            priority=1, cost=1, deadline=100)},
        {'time': 2, 'action': 'submit', 'task': dict(
            id='live-1', provider='p2', account='b',
            priority=2, cost=1, deadline=30)},
        {'time': 2, 'action': 'claim', 'worker': 'w2'},
        {'time': 2, 'action': 'execute', 'worker': 'w2', 'task': 'live-1'},
        {'time': 4, 'action': 'claim', 'worker': 'w3'},
        {'time': 6, 'action': 'claim', 'worker': 'w3'},
        {'time': 6, 'action': 'execute', 'worker': 'w3', 'task': 'history-1'},
        {'time': 6, 'action': 'execute', 'worker': 'w3', 'task': 'history-2'},
    ]
    timeline = sorted(
        [dict(event, origin='fixture') for event in fixture_events]
        + [dict(event, origin='driver') for event in driver],
        key=lambda event: (event['time'], event['origin'] == 'fixture'))
    scenario.run(timeline)
    dispatcher = scenario.dispatcher
    return {
        'records': dispatcher.records(),
        'pending': dispatcher.pending(),
        'metrics': dispatcher.metrics(),
    }
