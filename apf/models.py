"""Data models for the APF simulator (Python 3.8 compatible)."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class Request:
    """One incoming API request.

    ``work`` is the work estimate in seat-slices: how many time slices the
    request occupies one concurrency seat once dispatched. Different request
    types (list vs. get vs. write) can carry very different estimates.
    ``timeout`` is the maximum number of slices the request may wait in a
    queue before it expires.
    """

    rid: str
    user: str
    verb: str
    resource: str
    namespace: str = ""
    work: float = 1.0
    arrive: int = 0
    timeout: int = 10
    # Filled in by classification.
    flow_schema: str = ""
    priority_level: str = ""
    flow: str = ""


@dataclass
class MatchingRule:
    """A simple AND matcher; ``None`` fields are wildcards."""

    user: Optional[str] = None
    verb: Optional[str] = None
    resource: Optional[str] = None
    namespace: Optional[str] = None

    def matches(self, req: Request) -> bool:
        return (
            (self.user is None or self.user == req.user)
            and (self.verb is None or self.verb == req.verb)
            and (self.resource is None or self.resource == req.resource)
            and (self.namespace is None or self.namespace == req.namespace)
        )


@dataclass
class FlowSchema:
    """Classifies requests into a priority level and a flow.

    ``flow_distinguisher`` is ``"user"`` or ``"user+namespace"``.
    """

    name: str
    priority_level: str
    rules: List[MatchingRule] = field(default_factory=list)
    flow_distinguisher: str = "user"

    def matches(self, req: Request) -> bool:
        return any(rule.matches(req) for rule in self.rules)

    def flow_id(self, req: Request) -> str:
        if self.flow_distinguisher == "user+namespace":
            return "%s/%s@%s" % (req.user, req.namespace, self.name)
        return "%s@%s" % (req.user, self.name)


@dataclass
class PriorityLevelConfiguration:
    """One priority level.

    ``assured_concurrency_shares`` is the minimum guaranteed concurrency.
    ``lendable_percent`` of the assured shares may be borrowed by other
    levels while unused here. ``borrowing_limit_percent`` caps how much
    this level may borrow on top of its assured shares.
    ``queue_capacity`` bounds the total queued requests in this level;
    arrivals beyond it are rejected.
    """

    name: str
    assured_concurrency_shares: int
    lendable_percent: float = 0.0
    borrowing_limit_percent: float = 0.0
    queue_capacity: int = 64


@dataclass
class RoundLog:
    """What happened in one scheduling time slice."""

    t: int
    dispatched: List[str] = field(default_factory=list)
    completed: List[str] = field(default_factory=list)
    running: List[str] = field(default_factory=list)
    queued: Dict[str, int] = field(default_factory=dict)
    limits: Dict[str, int] = field(default_factory=dict)
    borrowed: Dict[str, int] = field(default_factory=dict)
    rejected: List[str] = field(default_factory=list)
    expired: List[str] = field(default_factory=list)
