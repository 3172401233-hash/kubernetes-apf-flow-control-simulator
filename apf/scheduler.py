"""Time-sliced APF scheduler.

Each time slice the scheduler:

1. admits arrivals (classify via FlowSchemas, enqueue or reject when the
   priority level's queue capacity is full),
2. expires requests that waited in queue past their timeout,
3. recomputes per-level concurrency limits: every level keeps its assured
   shares (minimum guarantee), unused lendable shares form a pool, and
   levels with unmet demand borrow from it up to their borrowing limit,
4. dispatches queued requests up to each level's limit using virtual-time
   fair queuing,
5. advances running requests by one slice of work and frees seats of
   completed requests.

Running requests are never preempted; if a level's limit shrinks below
its running count, dispatching simply pauses until seats free up.
"""

from collections import defaultdict
from typing import Dict, List, Tuple

from .fairqueue import FairQueueSet
from .models import (
    FlowSchema,
    PriorityLevelConfiguration,
    Request,
    RoundLog,
)


class SimulationResult:
    def __init__(self, rounds):
        self.rounds = rounds

    def _collect(self, attr):
        out = []
        for rnd in self.rounds:
            out.extend(getattr(rnd, attr))
        return out

    @property
    def dispatched(self):
        return self._collect("dispatched")

    @property
    def completed(self):
        return self._collect("completed")

    @property
    def rejected(self):
        return self._collect("rejected")

    @property
    def expired(self):
        return self._collect("expired")

    def completed_at(self):
        out = {}
        for rnd in self.rounds:
            for rid in rnd.completed:
                out[rid] = rnd.t
        return out

    def format(self):
        lines = []
        for rnd in self.rounds:
            lines.append(
                "t=%02d dispatched=%s completed=%s running=%s queued=%s "
                "limits=%s borrowed=%s rejected=%s expired=%s"
                % (
                    rnd.t,
                    rnd.dispatched or "-",
                    rnd.completed or "-",
                    rnd.running or "-",
                    rnd.queued,
                    rnd.limits,
                    {k: v for k, v in rnd.borrowed.items() if v},
                    rnd.rejected or "-",
                    rnd.expired or "-",
                )
            )
        return "\n".join(lines)


class Scheduler:
    def __init__(self, flow_schemas, priority_levels):
        self.flow_schemas = list(flow_schemas)
        self.levels = {pl.name: pl for pl in priority_levels}
        self.queues = {
            pl.name: FairQueueSet(pl.queue_capacity) for pl in priority_levels
        }
        self.running = []  # list of [request, remaining work]
        self.now = 0

    def classify(self, req):
        for schema in self.flow_schemas:
            if schema.matches(req):
                req.flow_schema = schema.name
                req.priority_level = schema.priority_level
                req.flow = schema.flow_id(req)
                return req.priority_level in self.levels
        return False

    def _running_in(self, level):
        return sum(1 for entry in self.running if entry[0].priority_level == level)

    def _compute_limits(self):
        demands = {
            name: self._running_in(name) + self.queues[name].queued
            for name in self.levels
        }
        limits = {}
        unmet = {}
        borrow_caps = {}
        pool = 0.0
        for name, pl in self.levels.items():
            assured = pl.assured_concurrency_shares
            base = float(min(assured, demands[name]))
            limits[name] = base
            unmet[name] = float(demands[name]) - base
            lendable = assured * pl.lendable_percent / 100.0
            pool += min(lendable, float(assured) - base)
            borrow_caps[name] = assured * pl.borrowing_limit_percent / 100.0
        # Distribute the lendable pool proportionally to unmet demand,
        # respecting per-level borrowing caps; iterate so shares refused
        # by capped levels are redistributed to the rest.
        for _ in range(len(self.levels)):
            if pool <= 1e-9:
                break
            active = [
                n for n in self.levels if unmet[n] > 1e-9 and borrow_caps[n] > 1e-9
            ]
            if not active:
                break
            total_unmet = sum(unmet[n] for n in active)
            spent = 0.0
            for n in active:
                grant = min(pool * unmet[n] / total_unmet, unmet[n], borrow_caps[n])
                limits[n] += grant
                unmet[n] -= grant
                borrow_caps[n] -= grant
                spent += grant
            pool -= spent
            if spent <= 1e-9:
                break
        borrowed = {}
        int_limits = {}
        for name, pl in self.levels.items():
            base = min(pl.assured_concurrency_shares, demands[name])
            extra = limits[name] - base
            borrowed[name] = int(round(extra)) if extra > 1e-9 else 0
            int_limits[name] = int(limits[name])
        return int_limits, borrowed

    def step(self, arrivals):
        log = RoundLog(t=self.now)
        for req in arrivals:
            if not self.classify(req):
                log.rejected.append(req.rid)
                continue
            if not self.queues[req.priority_level].enqueue(req):
                log.rejected.append(req.rid)
        for name, queue_set in self.queues.items():
            for req in queue_set.expire(self.now):
                log.expired.append(req.rid)

        limits, borrowed = self._compute_limits()
        for name, queue_set in self.queues.items():
            width = limits[name]
            while self._running_in(name) < width:
                req = queue_set.dispatch(width=max(1, width))
                if req is None:
                    break
                self.running.append([req, req.work])
                log.dispatched.append(req.rid)

        still_running = []
        for entry in self.running:
            entry[1] -= 1
            if entry[1] <= 0:
                log.completed.append(entry[0].rid)
            else:
                still_running.append(entry)
        self.running = still_running

        log.running = [entry[0].rid for entry in self.running]
        log.queued = {name: qs.queued for name, qs in self.queues.items()}
        log.limits = limits
        log.borrowed = borrowed
        return log

    def run(self, requests, slices):
        arrivals = defaultdict(list)
        for req in sorted(requests, key=lambda r: (r.arrive, r.rid)):
            arrivals[req.arrive].append(req)
        rounds = []
        for t in range(slices):
            self.now = t
            rounds.append(self.step(arrivals.get(t, [])))
        return SimulationResult(rounds)
