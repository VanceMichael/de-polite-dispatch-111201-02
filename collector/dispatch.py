"""共享出口配额派发器。

在任务登记器(domain)之上提供真正的派发能力:

- 全局并发、供应方并发与账号令牌预算同时生效, 域名别名归并到同一供应方;
- 令牌在领取时仅预留, 请求真正发出时才消费, 未发出即交还;
- 实时任务优先, 积压任务随等待老化, 持续有新任务时仍能获得服务;
- 过期任务带明确原因移出队列, 不再占用队列位置;
- 多 worker 领取租约, 租约到期回收, 停机交还, 围栏序号拒绝晚到的旧 worker;
- 429 的 Retry-After 只冻结对应配额桶, 不阻塞无关供应方;
- 可重试错误退避重排, 永久拒绝进入死信, 两种结局不同;
- 时钟与请求执行器均可注入, 运行记录解释每次未派发的原因。

未配置限额的供应方不限制并发, 未配置配额桶的账号不限制令牌。
执行器在锁外调用, 多个 worker 的请求真正并行, 不靠串行化换正确性。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

OK = 'ok'
THROTTLED = 'throttled'
RETRYABLE = 'retryable'
PERMANENT = 'permanent'

QUEUED = 'queued'
LEASED = 'leased'
DONE = 'done'
EXPIRED = 'expired'
DEAD = 'dead'


class Clock(Protocol):
    def now(self) -> float:
        ...


class SystemClock:
    def now(self) -> float:
        return time.monotonic()


class ManualClock:
    """测试与回放用的手动时钟。"""

    def __init__(self, now: float = 0.0):
        self._now = float(now)

    def now(self) -> float:
        return self._now

    def set(self, now: float) -> None:
        self._now = float(now)

    def advance(self, seconds: float) -> float:
        self._now += seconds
        return self._now


@dataclass(frozen=True)
class Outcome:
    """一次请求执行的结果。kind 决定任务结局。"""

    kind: str
    reason: str = ''
    retry_after: float = 0.0

    @classmethod
    def ok(cls) -> 'Outcome':
        return cls(OK)

    @classmethod
    def throttled(cls, retry_after: float, reason: str = '供应方限流 429') -> 'Outcome':
        return cls(THROTTLED, reason, float(retry_after))

    @classmethod
    def retryable(cls, reason: str = '可重试错误') -> 'Outcome':
        return cls(RETRYABLE, reason)

    @classmethod
    def permanent(cls, reason: str = '永久拒绝') -> 'Outcome':
        return cls(PERMANENT, reason)


class Executor(Protocol):
    """请求执行器, 由调用方注入; 在派发器锁外被调用。"""

    def execute(self, task) -> Outcome:
        ...


@dataclass(frozen=True)
class Lease:
    task_id: str
    worker: str
    generation: int
    claimed_at: float
    expires_at: float


@dataclass(frozen=True)
class Completion:
    accepted: bool
    why: str
    task_id: str


@dataclass(frozen=True)
class QuotaConfig:
    global_concurrency: int = 1
    provider_limits: dict = field(default_factory=dict)
    account_buckets: dict = field(default_factory=dict)  # '供应方:账号' -> (容量, 每秒补充)
    aliases: dict = field(default_factory=dict)          # 域名 -> 供应方
    lease_seconds: float = 30.0
    aging_rate: float = 1.0 / 60.0   # 每秒等待折算的优先级, 保证积压任务终能获得服务
    max_attempts: int = 5
    backoff_base: float = 1.0
    backoff_cap: float = 60.0

    @classmethod
    def from_dict(cls, data: dict) -> 'QuotaConfig':
        buckets = {}
        for key, spec in (data.get('account_buckets') or {}).items():
            capacity = float(spec['capacity'])
            refill = float(spec.get('refill_per_second', 0.0))
            if capacity < 1 or refill < 0:
                raise ValueError('配额桶容量与速率不合法')
            buckets[key] = (capacity, refill)
        limits = {name: int(limit) for name, limit in (data.get('provider_limits') or {}).items()}
        if any(limit < 1 for limit in limits.values()):
            raise ValueError('供应方并发上限不合法')
        config = cls(
            global_concurrency=int(data.get('global_concurrency', 1)),
            provider_limits=limits,
            account_buckets=buckets,
            aliases=dict(data.get('aliases') or {}),
            lease_seconds=float(data.get('lease_seconds', 30.0)),
            aging_rate=float(data.get('aging_rate', 1.0 / 60.0)),
            max_attempts=int(data.get('max_attempts', 5)),
            backoff_base=float(data.get('backoff_base', 1.0)),
            backoff_cap=float(data.get('backoff_cap', 60.0)),
        )
        if config.global_concurrency < 1:
            raise ValueError('全局并发上限不合法')
        if config.lease_seconds <= 0 or config.max_attempts < 1:
            raise ValueError('租约时长与最大尝试次数不合法')
        return config

    def resolve_provider(self, name: str) -> str:
        return self.aliases.get(name, name)


@dataclass
class TokenBucket:
    capacity: float
    refill_per_second: float
    tokens: float = -1.0
    reserved: float = 0.0
    frozen_until: float = 0.0
    consumed: float = 0.0
    freezes: int = 0
    frozen_seconds: float = 0.0
    _refilled_at: float = 0.0

    def __post_init__(self):
        if self.tokens < 0:
            self.tokens = float(self.capacity)

    def _refill(self, now: float) -> None:
        if now > self._refilled_at:
            self.tokens = min(self.capacity, self.tokens + self.refill_per_second * (now - self._refilled_at))
            self._refilled_at = now

    def available(self, now: float) -> float:
        if now < self.frozen_until:
            return 0.0
        self._refill(now)
        return max(0.0, self.tokens - self.reserved)

    def reserve(self, cost: float, now: float) -> None:
        self._refill(now)
        self.reserved += cost

    def release(self, cost: float) -> None:
        self.reserved = max(0.0, self.reserved - cost)

    def consume(self, cost: float, now: float) -> None:
        self._refill(now)
        self.tokens -= cost
        self.reserved = max(0.0, self.reserved - cost)
        self.consumed += cost

    def freeze(self, until: float, now: float) -> None:
        start = max(now, self.frozen_until)
        if until > start:
            self.frozen_seconds += until - start
        if until > self.frozen_until:
            self.frozen_until = until
        self.freezes += 1

    def snapshot(self, now: float) -> dict:
        self._refill(now)
        return {
            'capacity': self.capacity,
            'refill_per_second': self.refill_per_second,
            'tokens': self.tokens,
            'reserved': self.reserved,
            'consumed': self.consumed,
            'freezes': self.freezes,
            'frozen_seconds': self.frozen_seconds,
            'frozen_until': self.frozen_until,
            'frozen': self.frozen_until > now,
        }


@dataclass
class _Entry:
    task: object
    provider: str
    bucket_key: str
    submitted_at: float
    state: str = QUEUED
    generation: int = 0
    leased_by: str = None
    lease_expires: float = 0.0
    claimed_at: float = 0.0
    sent: bool = False
    sends: int = 0
    retried_sends: int = 0
    not_before: float = 0.0
    first_dispatch_at: float = None
    finished_at: float = None
    outcome_reason: str = ''


class Dispatcher:
    """多 worker 配额派发器。所有状态变更在锁内完成, 执行器在锁外调用。"""

    def __init__(self, config: QuotaConfig, clock: Clock, executor: Executor = None):
        self.config = config
        self.clock = clock
        self.executor = executor
        self._lock = threading.Lock()
        self._entries = {}
        self._buckets = {key: TokenBucket(capacity, refill)
                         for key, (capacity, refill) in config.account_buckets.items()}
        self._provider_inflight = {}
        self._provider_peak = {}
        self._global_inflight = 0
        self._global_peak = 0
        self._records = []
        self._last_skip = {}
        self._stale_rejected = 0
        self._duplicate_sends = 0
        self._started_at = clock.now()

    # ---- 登记与查询 ----

    def submit(self, task) -> None:
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            if task.identifier in self._entries:
                raise ValueError('任务标识重复')
            provider = self.config.resolve_provider(task.provider)
            self._entries[task.identifier] = _Entry(
                task=task, provider=provider,
                bucket_key=f'{provider}:{task.account}', submitted_at=now)
            self._record('submit', now, task=task.identifier,
                         provider=provider, account=task.account, why='登记任务')

    def submit_all(self, tasks) -> None:
        for task in tasks:
            self.submit(task)

    # ---- 领取 ----

    def claim(self, worker: str, task_id: str = None):
        """为 worker 领取一个当前可派发的任务; 无可派发时返回 None 并记录原因。"""
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            candidates = [entry for entry in self._entries.values() if entry.state == QUEUED]
            if task_id is not None:
                candidates = [entry for entry in candidates if entry.task.identifier == task_id]
                if not candidates:
                    return None
            candidates.sort(key=lambda entry: (
                -(entry.task.priority + self.config.aging_rate * max(0.0, now - entry.submitted_at)),
                entry.task.deadline, entry.task.identifier))
            blocked = {}
            for entry in candidates:
                reason = self._block_reason(entry, now)
                if reason is None:
                    return self._dispatch(entry, worker, now)
                blocked[entry.task.identifier] = reason
                self._skip(entry, now, reason)
            if blocked:
                self._record('idle', now, worker=worker, why='本次未派发', reasons=blocked)
            return None

    def _block_reason(self, entry: _Entry, now: float):
        task = entry.task
        if entry.not_before > now:
            return f'退避等待至{entry.not_before:g}'
        if self._global_inflight >= self.config.global_concurrency:
            return f'全局并发已满({self._global_inflight}/{self.config.global_concurrency})'
        limit = self.config.provider_limits.get(entry.provider)
        if limit is not None and self._provider_inflight.get(entry.provider, 0) >= limit:
            return f'供应方{entry.provider}并发已满({self._provider_inflight.get(entry.provider, 0)}/{limit})'
        bucket = self._buckets.get(entry.bucket_key)
        if bucket is not None:
            if bucket.frozen_until > now:
                return f'配额桶{entry.bucket_key}冻结至{bucket.frozen_until:g}'
            if bucket.available(now) < task.cost:
                return f'配额桶{entry.bucket_key}令牌不足(需要{task.cost})'
        return None

    def _skip(self, entry: _Entry, now: float, reason: str) -> None:
        if self._last_skip.get(entry.task.identifier) != reason:
            self._last_skip[entry.task.identifier] = reason
            self._record('skip', now, task=entry.task.identifier, why=reason)

    def _dispatch(self, entry: _Entry, worker: str, now: float) -> Lease:
        task = entry.task
        bucket = self._buckets.get(entry.bucket_key)
        if bucket is not None:
            bucket.reserve(task.cost, now)
        entry.state = LEASED
        entry.generation += 1
        entry.leased_by = worker
        entry.claimed_at = now
        entry.lease_expires = now + self.config.lease_seconds
        entry.sent = False
        self._inflight_inc(entry)
        if entry.first_dispatch_at is None:
            entry.first_dispatch_at = now
        self._last_skip.pop(task.identifier, None)
        self._record('dispatch', now, task=task.identifier, worker=worker,
                     provider=entry.provider, bucket=entry.bucket_key,
                     generation=entry.generation, lease_expires=entry.lease_expires,
                     why='获得租约, 令牌已预留待发出时消费')
        return Lease(task.identifier, worker, entry.generation, now, entry.lease_expires)

    # ---- 发出与完成 ----

    def mark_sent(self, lease: Lease) -> bool:
        """请求真正发出时调用, 此刻才消费预留令牌。租约失效时禁止发送。"""
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            entry = self._entries.get(lease.task_id)
            if not self._valid(entry, lease):
                self._record('send_rejected', now, task=lease.task_id,
                             worker=lease.worker, why='租约已失效, 禁止发送')
                return False
            if entry.sent:
                return True
            bucket = self._buckets.get(entry.bucket_key)
            if bucket is not None:
                bucket.consume(entry.task.cost, now)
            duplicate = entry.sends > entry.retried_sends
            if duplicate:
                self._duplicate_sends += 1
            entry.sent = True
            entry.sends += 1
            self._record('send', now, task=entry.task.identifier, worker=lease.worker,
                         duplicate=duplicate, why='请求发出, 消费预留令牌')
            return True

    def complete(self, lease: Lease, outcome: Outcome) -> Completion:
        if outcome.kind not in (OK, THROTTLED, RETRYABLE, PERMANENT):
            raise ValueError('未知执行结果')
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            entry = self._entries.get(lease.task_id)
            if not self._valid(entry, lease):
                self._stale_rejected += 1
                self._record('stale_complete', now, task=lease.task_id, worker=lease.worker,
                             why='拒绝晚到的完成: 租约已回收或转交')
                return Completion(False, '租约已失效', lease.task_id)
            self._inflight_dec(entry)
            if not entry.sent:
                bucket = self._buckets.get(entry.bucket_key)
                if bucket is not None:
                    bucket.release(entry.task.cost)
            entry.leased_by = None
            entry.sent = False
            task_id = entry.task.identifier
            if outcome.kind == OK:
                entry.state = DONE
                entry.finished_at = now
                self._record('complete', now, task=task_id, worker=lease.worker, why='执行成功')
            elif outcome.kind == THROTTLED:
                until = now + max(0.0, outcome.retry_after)
                bucket = self._buckets.get(entry.bucket_key)
                if bucket is not None:
                    bucket.freeze(until, now)
                    self._record('freeze', now, task=task_id, bucket=entry.bucket_key,
                                 until=until,
                                 why=f'收到429, Retry-After={outcome.retry_after:g}, 冻结对应配额桶')
                self._retry(entry, now, until, f'限流退避至{until:g}')
            elif outcome.kind == RETRYABLE:
                delay = min(self.config.backoff_cap,
                            self.config.backoff_base * (2 ** max(0, entry.sends - 1)))
                until = now + delay
                self._retry(entry, now, until, f'可重试错误, 退避至{until:g}: {outcome.reason}')
            else:
                entry.state = DEAD
                entry.finished_at = now
                entry.outcome_reason = outcome.reason or '永久拒绝'
                self._record('dead_letter', now, task=task_id,
                             why=f'永久拒绝: {entry.outcome_reason}')
            return Completion(True, '', task_id)

    def execute_lease(self, lease: Lease) -> Completion:
        """标记发出并在锁外调用执行器, 然后按结果收尾; 多个租约可真正并行。"""
        with self._lock:
            entry = self._entries.get(lease.task_id)
            task = entry.task if entry is not None else None
        if task is None:
            return Completion(False, '任务不存在', lease.task_id)
        if not self.mark_sent(lease):
            return Completion(False, '租约已失效', lease.task_id)
        outcome = self.executor.execute(task)
        return self.complete(lease, outcome)

    def _retry(self, entry: _Entry, now: float, not_before: float, why: str) -> None:
        entry.retried_sends = entry.sends
        task_id = entry.task.identifier
        if entry.sends >= self.config.max_attempts:
            entry.state = DEAD
            entry.finished_at = now
            entry.outcome_reason = f'超过最大重试次数({self.config.max_attempts})'
            self._record('dead_letter', now, task=task_id, why=entry.outcome_reason)
        elif entry.task.deadline <= now:
            entry.state = EXPIRED
            entry.finished_at = now
            entry.outcome_reason = f'超过最晚执行时间({entry.task.deadline:g})'
            self._record('expired', now, task=task_id, why=entry.outcome_reason)
        else:
            entry.state = QUEUED
            entry.not_before = not_before
            self._record('requeue', now, task=task_id, not_before=not_before,
                         attempts=entry.sends, why=why)

    # ---- 回收与交还 ----

    def worker_exit(self, worker: str) -> list:
        """worker 停机, 立即交还持有的全部租约; 未发出的预留令牌不消费。"""
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            held = [entry for entry in self._entries.values()
                    if entry.state == LEASED and entry.leased_by == worker]
            for entry in held:
                self._reclaim(entry, now, 'returned', 'worker停机交还')
            self._record('worker_exit', now, worker=worker,
                         returned=len(held), why='worker退出, 交还持有租约')
            return sorted(entry.task.identifier for entry in held)

    def reap(self) -> None:
        """主动回收到期租约与过期任务; 其他入口调用时也会自动执行。"""
        with self._lock:
            self._refresh(self.clock.now())

    def _refresh(self, now: float) -> None:
        for entry in self._entries.values():
            if entry.state == LEASED and entry.lease_expires <= now:
                self._reclaim(entry, now, 'lease_reclaimed',
                              f'租约到期({entry.lease_expires:g})回收')
        for entry in self._entries.values():
            if entry.state == QUEUED and entry.task.deadline <= now:
                entry.state = EXPIRED
                entry.finished_at = now
                entry.outcome_reason = f'超过最晚执行时间({entry.task.deadline:g})'
                self._record('expired', now, task=entry.task.identifier,
                             why=entry.outcome_reason)

    def _reclaim(self, entry: _Entry, now: float, kind: str, why: str) -> None:
        worker = entry.leased_by
        was_sent = entry.sent
        self._inflight_dec(entry)
        if not was_sent:
            bucket = self._buckets.get(entry.bucket_key)
            if bucket is not None:
                bucket.release(entry.task.cost)
        entry.generation += 1
        entry.leased_by = None
        entry.sent = False
        if entry.task.deadline <= now:
            entry.state = EXPIRED
            entry.finished_at = now
            entry.outcome_reason = f'超过最晚执行时间({entry.task.deadline:g})'
            self._record('expired', now, task=entry.task.identifier, why=entry.outcome_reason)
        else:
            entry.state = QUEUED
        self._record(kind, now, task=entry.task.identifier, worker=worker,
                     sent=was_sent, why=why)

    # ---- 观察 ----

    def lease_view(self, lease: Lease):
        with self._lock:
            entry = self._entries.get(lease.task_id)
            if entry is None:
                return None
            return {
                'task': lease.task_id,
                'provider': entry.provider,
                'bucket': entry.bucket_key,
                'sent': entry.sent,
                'held_by': entry.leased_by,
                'valid': self._valid(entry, lease),
            }

    def pending(self) -> list:
        """当前队列快照, 每个任务附带此刻未派发的原因。"""
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            result = []
            for entry in self._entries.values():
                if entry.state != QUEUED:
                    continue
                result.append({
                    'task': entry.task.identifier,
                    'provider': entry.provider,
                    'bucket': entry.bucket_key,
                    'queued_for': now - entry.submitted_at,
                    'why': self._block_reason(entry, now) or '可派发',
                })
            result.sort(key=lambda item: item['task'])
            return result

    def records(self) -> list:
        with self._lock:
            return [dict(record) for record in self._records]

    def explain(self, task_id: str) -> list:
        """某个任务相关的全部运行记录, 解释它为何被派发或未被派发。"""
        with self._lock:
            return [dict(record) for record in self._records
                    if record.get('task') == task_id]

    def metrics(self) -> dict:
        with self._lock:
            now = self.clock.now()
            self._refresh(now)
            elapsed = max(now - self._started_at, 1e-9)
            buckets = {}
            for key, bucket in self._buckets.items():
                snap = bucket.snapshot(now)
                if bucket.refill_per_second > 0:
                    snap['utilization'] = bucket.consumed / (bucket.refill_per_second * elapsed)
                buckets[key] = snap
            states = {}
            waits = {}
            queued = {}
            sends = {}
            providers = set(self.config.provider_limits) | set(self._provider_inflight)
            for task_id, entry in self._entries.items():
                states[entry.state] = states.get(entry.state, 0) + 1
                providers.add(entry.provider)
                if entry.first_dispatch_at is not None:
                    waits[task_id] = entry.first_dispatch_at - entry.submitted_at
                if entry.state == QUEUED:
                    queued[task_id] = now - entry.submitted_at
                if entry.sends:
                    sends[task_id] = entry.sends
            return {
                'time': now,
                'concurrency': {
                    'global': {'limit': self.config.global_concurrency,
                               'inflight': self._global_inflight, 'peak': self._global_peak},
                    'providers': {name: {'limit': self.config.provider_limits.get(name),
                                         'inflight': self._provider_inflight.get(name, 0),
                                         'peak': self._provider_peak.get(name, 0)}
                                  for name in sorted(providers)},
                },
                'buckets': buckets,
                'tasks': states,
                'wait_seconds': waits,
                'queued_seconds': queued,
                'duplicates': {'extra_sends': self._duplicate_sends, 'sends': sends},
                'stale_completions_rejected': self._stale_rejected,
            }

    # ---- 内部 ----

    @staticmethod
    def _valid(entry: _Entry, lease: Lease) -> bool:
        return (entry is not None and entry.state == LEASED
                and entry.leased_by == lease.worker
                and entry.generation == lease.generation)

    def _inflight_inc(self, entry: _Entry) -> None:
        self._global_inflight += 1
        self._global_peak = max(self._global_peak, self._global_inflight)
        count = self._provider_inflight.get(entry.provider, 0) + 1
        self._provider_inflight[entry.provider] = count
        self._provider_peak[entry.provider] = max(
            self._provider_peak.get(entry.provider, 0), count)

    def _inflight_dec(self, entry: _Entry) -> None:
        self._global_inflight = max(0, self._global_inflight - 1)
        self._provider_inflight[entry.provider] = max(
            0, self._provider_inflight.get(entry.provider, 0) - 1)

    def _record(self, kind: str, now: float, **fields) -> None:
        self._records.append({'time': now, 'kind': kind, **fields})
