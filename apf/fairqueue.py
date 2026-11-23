"""Virtual-time fair queuing within one priority level.

Implements start-time fair queuing across the flows of a single
PriorityLevelConfiguration. Each flow keeps a virtual finish time ``r``
for its last dispatched request; the scheduler always dispatches from the
flow with the smallest ``r``. This is deliberately *not* a plain FIFO or
priority sort: dispatch order depends on accumulated virtual service, so
every active flow gets a fair share of seat-time.

Anti-burst property: when a flow goes idle and later resumes, its ``r``
is lifted to the queue set's current virtual time. A resumed flow
therefore cannot bank credit while idle and flush its entire backlog
ahead of flows that stayed active; its backlog drains interleaved with
the other flows' traffic.
"""

from collections import deque
from typing import List, Optional

from .models import Request


class FlowQueue:
    __slots__ = ("flow_id", "requests", "r")

    def __init__(self, flow_id: str) -> None:
        self.flow_id = flow_id
        self.requests = deque()  # type: deque
        self.r = 0.0  # virtual finish time of the last dispatched request


class FairQueueSet:
    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.queues = {}  # type: dict
        self.virtual_time = 0.0
        self.queued = 0

    def _queue(self, flow_id: str) -> FlowQueue:
        queue = self.queues.get(flow_id)
        if queue is None:
            queue = FlowQueue(flow_id)
            self.queues[flow_id] = queue
        return queue

    def enqueue(self, req: Request) -> bool:
        """Add a request; return False (caller rejects it) if at capacity."""
        if self.queued >= self.capacity:
            return False
        queue = self._queue(req.flow)
        if not queue.requests:
            # Flow is (re)starting: no credit for time spent idle.
            queue.r = max(queue.r, self.virtual_time)
        queue.requests.append(req)
        self.queued += 1
        return True

    def expire(self, now: int) -> List[Request]:
        """Drop requests that waited in queue past their timeout."""
        expired = []
        for queue in self.queues.values():
            kept = deque()
            while queue.requests:
                req = queue.requests.popleft()
                if now - req.arrive >= req.timeout:
                    expired.append(req)
                    self.queued -= 1
                else:
                    kept.append(req)
            queue.requests = kept
        return expired

    def _next_queue(self) -> Optional[FlowQueue]:
        best = None
        for queue in self.queues.values():
            if not queue.requests:
                continue
            if (
                best is None
                or queue.r < best.r
                or (queue.r == best.r and queue.flow_id < best.flow_id)
            ):
                best = queue
        return best

    def dispatch(self, width: int) -> Optional[Request]:
        """Pop the head request of the fairest flow.

        ``width`` is the level's current concurrency limit; a request's
        virtual cost is its work estimate divided by the width, so flows
        with heavier requests consume virtual time faster.
        """
        queue = self._next_queue()
        if queue is None:
            return None
        req = queue.requests.popleft()
        self.queued -= 1
        self.virtual_time = max(self.virtual_time, queue.r)
        queue.r += req.work / max(1, width)
        return req
