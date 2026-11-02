"""Lightweight per-request phase timer used to log where a download/preview request spends its time."""

import contextlib
from time import monotonic
from typing import Any, Dict, Iterator, Optional


class PhaseTimer:
    """Accumulates elapsed seconds per named phase plus a few request attributes.

    Phases can be entered several times (e.g. the XLSX pre-check and the final query both connect), in which case the
    elapsed times are added up and ``counts`` records how many times each phase ran.
    """

    def __init__(self) -> None:
        self.started_at = monotonic()
        self.times: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}
        self.attrs: Dict[str, Any] = {}
        self.emitted = False

    def add(self, name: str, seconds: float) -> None:
        """Add ``seconds`` to phase ``name``."""
        self.times[name] = self.times.get(name, 0.0) + seconds
        self.counts[name] = self.counts.get(name, 0) + 1

    @contextlib.contextmanager
    def phase(self, name: str) -> Iterator[None]:
        """Time the enclosed block into phase ``name``, also when it raises."""
        started = monotonic()
        try:
            yield
        finally:
            self.add(name, monotonic() - started)


def phase(timer: Optional[PhaseTimer], name: str):
    """Return ``timer.phase(name)``, or a no-op context manager when ``timer`` is None."""
    if timer is None:
        return contextlib.nullcontext()
    return timer.phase(name)
