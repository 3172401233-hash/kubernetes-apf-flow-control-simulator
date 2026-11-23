import unittest

from apf import (
    FlowSchema,
    MatchingRule,
    PriorityLevelConfiguration,
    Request,
    Scheduler,
)


def make_scheduler(levels, schemas=None):
    if schemas is None:
        schemas = [
            FlowSchema(
                name="catch-all",
                priority_level=levels[0].name,
                rules=[MatchingRule()],
            )
        ]
    return Scheduler(flow_schemas=schemas, priority_levels=levels)


def req(rid, user="alice", arrive=0, work=1.0, timeout=50, verb="get"):
    return Request(
        rid=rid, user=user, verb=verb, resource="pods",
        arrive=arrive, work=work, timeout=timeout,
    )


class DualFlowContentionTest(unittest.TestCase):
    # Two flows share one seat: the late, light flow must not starve
    # behind the heavy flow's backlog, and service must interleave.

    def test_fair_interleaving(self):
        level = PriorityLevelConfiguration(
            name="normal", assured_concurrency_shares=1, queue_capacity=64
        )
        sched = make_scheduler([level])
        requests = [req("a%d" % i, user="alice") for i in range(10)]
        requests.append(req("b0", user="bob", arrive=1))
        result = sched.run(requests, slices=20)

        completed_at = result.completed_at()
        # Bob's single request completes quickly even though Alice's
        # backlog arrived first: no FIFO head-of-line blocking.
        self.assertIn("b0", completed_at)
        self.assertLessEqual(completed_at["b0"], 3)
        self.assertLess(completed_at["b0"], completed_at["a9"])
        # Alice's backlog is not cleared before Bob is served.
        alice_done_early = [
            rid for rid, t in completed_at.items()
            if rid.startswith("a") and t <= completed_at["b0"]
        ]
        self.assertLess(len(alice_done_early), 10)


class CapacityBorrowingTest(unittest.TestCase):
    # Unused lendable shares of an idle level are borrowed by a busy
    # level, and reclaimed when the lender becomes active.

    def build(self):
        levels = [
            PriorityLevelConfiguration(
                name="high", assured_concurrency_shares=2,
                lendable_percent=100, queue_capacity=64,
            ),
            PriorityLevelConfiguration(
                name="low", assured_concurrency_shares=2,
                borrowing_limit_percent=100, queue_capacity=64,
            ),
        ]
        schemas = [
            FlowSchema("fs-high", "high", [MatchingRule(user="leader")]),
            FlowSchema("fs-low", "low", [MatchingRule()]),
        ]
        return Scheduler(flow_schemas=schemas, priority_levels=levels)

    def test_borrow_and_reclaim(self):
        sched = self.build()
        requests = [req("low%d" % i, user="watcher", work=6) for i in range(4)]
        requests += [req("high%d" % i, user="leader", arrive=2, work=6)
                     for i in range(2)]
        result = sched.run(requests, slices=8)

        round0 = result.rounds[0]
        # High level is idle: low borrows its 2 lendable shares.
        self.assertEqual(round0.limits["low"], 4)
        self.assertEqual(round0.borrowed["low"], 2)
        self.assertEqual(len(round0.dispatched), 4)

        round3 = result.rounds[3]
        # High level is active again: borrow is reclaimed, no preemption.
        self.assertEqual(round3.limits["high"], 2)
        self.assertEqual(round3.limits["low"], 2)
        self.assertEqual(round3.borrowed["low"], 0)


class BurstTrafficTest(unittest.TestCase):
    # A flow that was idle and bursts must not flush its whole backlog
    # at once; the continuously active flow keeps its fair share.

    def test_burst_drains_gradually(self):
        level = PriorityLevelConfiguration(
            name="normal", assured_concurrency_shares=1, queue_capacity=64
        )
        sched = make_scheduler([level])
        # Bob is active the whole time, one request per slice.
        requests = [req("b%d" % i, user="bob", arrive=i) for i in range(16)]
        # Alice idles, then bursts 6 requests at t=5.
        requests += [req("a%d" % i, user="alice", arrive=5) for i in range(6)]
        result = sched.run(requests, slices=16)

        completed_at = result.completed_at()
        alice_done = sorted(t for rid, t in completed_at.items()
                            if rid.startswith("a"))
        # The burst cannot drain within its own 6 slices: interleaving
        # with Bob stretches it to roughly twice as long.
        self.assertGreater(alice_done[-1], 5 + 6)
        # Bob keeps completing requests while Alice's backlog drains.
        bob_done_mid = [t for rid, t in completed_at.items()
                        if rid.startswith("b") and 6 <= t <= alice_done[-1]]
        self.assertGreaterEqual(len(bob_done_mid), 3)


class WorkEstimateSkewTest(unittest.TestCase):
    # Fairness is in seat-time, not request count: a flow with heavy
    # requests (large work estimate) gets proportionally fewer dispatches.

    def test_heavy_requests_consume_more_virtual_time(self):
        level = PriorityLevelConfiguration(
            name="normal", assured_concurrency_shares=1, queue_capacity=64
        )
        sched = make_scheduler([level])
        requests = [req("light%d" % i, user="alice", work=1) for i in range(10)]
        requests += [req("heavy%d" % i, user="bob", work=4) for i in range(10)]
        result = sched.run(requests, slices=60)

        completed_at = result.completed_at()
        window = 25
        light_done = sum(1 for rid, t in completed_at.items()
                         if rid.startswith("light") and t < window)
        heavy_done = sum(1 for rid, t in completed_at.items()
                         if rid.startswith("heavy") and t < window)
        # With 4x the work estimate, Bob's flow completes far fewer
        # requests in the same window of seat-time.
        self.assertGreaterEqual(light_done, 3 * max(1, heavy_done))
        # Everything eventually completes: total seat-time is 10*1+10*4=50.
        self.assertEqual(len(result.completed), 20)


class RejectionAndExpiryTest(unittest.TestCase):
    def test_queue_capacity_rejects(self):
        level = PriorityLevelConfiguration(
            name="normal", assured_concurrency_shares=1, queue_capacity=2
        )
        sched = make_scheduler([level])
        requests = [req("r%d" % i, work=5) for i in range(5)]
        result = sched.run(requests, slices=1)
        # Capacity 2: 2 enqueued (1 then dispatched), 3 rejected.
        self.assertEqual(len(result.rounds[0].dispatched), 1)
        self.assertEqual(result.rounds[0].queued["normal"], 1)
        self.assertEqual(len(result.rejected), 3)

    def test_queued_requests_expire(self):
        level = PriorityLevelConfiguration(
            name="normal", assured_concurrency_shares=1, queue_capacity=8
        )
        sched = make_scheduler([level])
        requests = [req("long", work=10)]
        requests += [req("stale%d" % i, arrive=1, timeout=3) for i in range(2)]
        result = sched.run(requests, slices=6)
        # The seat is held by "long"; the two queued requests expire at t=4.
        self.assertEqual(sorted(result.expired), ["stale0", "stale1"])
        self.assertNotIn("stale0", result.completed)


if __name__ == "__main__":
    unittest.main()
