"""Demo: high-priority burst + low-priority borrowing, printed per slice.

Run: python3 demo.py
"""
from apf import (
    FlowSchema,
    MatchingRule,
    PriorityLevelConfiguration,
    Request,
    Scheduler,
)

levels = [
    PriorityLevelConfiguration(
        name="high", assured_concurrency_shares=2,
        lendable_percent=100, queue_capacity=8,
    ),
    PriorityLevelConfiguration(
        name="low", assured_concurrency_shares=1,
        borrowing_limit_percent=200, queue_capacity=8,
    ),
]
schemas = [
    FlowSchema("leader-election", "high", [MatchingRule(user="leader")]),
    FlowSchema("controllers", "high", [MatchingRule(user="controller")]),
    FlowSchema("watchers", "low", [MatchingRule()]),
]

requests = []
# Low-priority watchers: long list calls, present from the start.
for i in range(6):
    requests.append(Request(rid="watch%d" % i, user="watcher", verb="list",
                            resource="pods", arrive=0, work=4, timeout=15))
# High-priority burst arrives at t=3.
for i in range(4):
    requests.append(Request(rid="lease%d" % i, user="leader", verb="update",
                            resource="leases", arrive=3, work=2, timeout=15))
# A controller flow bursts late and must drain gradually.
for i in range(4):
    requests.append(Request(rid="ctrl%d" % i, user="controller", verb="get",
                            resource="nodes", arrive=6, work=1, timeout=15))

sched = Scheduler(flow_schemas=schemas, priority_levels=levels)
result = sched.run(requests, slices=16)
print(result.format())
print()
print("completed:", result.completed)
print("rejected:", result.rejected)
print("expired:", result.expired)
