from dataclasses import dataclass


@dataclass(frozen=True)
class Task:
    identifier: str
    provider: str
    account: str
    priority: int
    cost: int
    deadline: int

    @classmethod
    def parse(cls, value):
        if value['cost'] < 1 or value['deadline'] < 0:
            raise ValueError('成本与截止时间不合法')
        if not value['provider'] or not value['account']:
            raise ValueError('供应方和账号不能为空')
        return cls(value['id'], value['provider'], value['account'], value['priority'], value['cost'], value['deadline'])


def eligible(tasks, clock):
    return sorted((task for task in tasks if task.deadline > clock), key=lambda task: (-task.priority, task.deadline, task.identifier))


def collect(payload):
    tasks = [Task.parse(item) for item in payload['tasks']]
    if len({task.identifier for task in tasks}) != len(tasks):
        raise ValueError('任务标识重复')
    return [dict(key=task.identifier, provider=task.provider, account=task.account,
                 priority=task.priority, cost=task.cost, deadline=task.deadline)
            for task in eligible(tasks, payload['clock'])]
