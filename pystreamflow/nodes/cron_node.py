from ..core.node import BaseNode
import asyncio
from datetime import datetime, timezone


def parse_cron_part(part: str, lo: int, hi: int) -> set:
    """Expand one cron field into the set of integer values it matches,
    supporting the common subset of cron syntax: ``*``, a bare number,
    ``a-b``, ``*/n``, ``a-b/n``, and comma-separated lists of any of the
    above (e.g. ``0,15,30,45`` or ``9-17/2``).

    No cron-parsing library (``croniter`` or similar) is a dependency of
    this project (see pyproject.toml) - this is a small, self-contained
    implementation rather than adding one just for a single node type.
    """
    values: set = set()
    for token in part.split(','):
        token = token.strip()
        if not token:
            continue
        step = 1
        if '/' in token:
            token, step_s = token.split('/', 1)
            step = int(step_s)
        # A step applies relative to wherever its own range starts - '*/15'
        # steps from the field's global minimum (`lo`), but 'a-b/n' steps
        # from `a`, not from `lo` (e.g. '9-17/2' is 9,11,13,15,17 - not
        # filtered against "even offsets from 0").
        if token == '*':
            start = lo
            rng = range(lo, hi + 1)
        elif '-' in token:
            a, b = token.split('-', 1)
            start = int(a)
            rng = range(int(a), int(b) + 1)
        else:
            v = int(token)
            start = v
            rng = range(v, v + 1)
        for v in rng:
            if (v - start) % step == 0:
                values.add(v)
    return values


class CronNode(BaseNode):
    """Emits a tick whenever the current wall-clock time (UTC) matches a
    real 5-field cron expression (``minute hour day-of-month month
    day-of-week``, using the same syntax `crontab` uses) - e.g. ``*/15 * *
    * *`` for "every 15 minutes" or ``0 9 * * 1-5`` for "9am on weekdays".

    Distinct from the two scheduling primitives this codebase already has:
    ``ClockNode`` is a fixed-interval ticker with no notion of wall-clock
    alignment (it just counts seconds from whenever it happened to start),
    and ``TimerTriggerNode``/``TimerNode`` are periodic-interval triggers
    for the same reason - neither can express "run at 9am" or "run on the
    hour", only "run every N seconds".
    """

    async def init(self):
        self.cron = str(self.config.get('cron', '* * * * *'))
        self.payload = self.config.get('payload')
        # How often to actually check the wall clock against the cron
        # expression. Real cron granularity is one minute, so 1.0s is
        # already far finer-grained than needed for correctness -
        # configurable (and dropped much lower in tests) purely so a test
        # doesn't have to wait up to a real minute boundary to observe a
        # fire.
        self.check_interval = float(self.config.get('check_interval', 1.0))
        parts = self.cron.split()
        if len(parts) != 5:
            raise ValueError(
                f"CronNode requires a 5-field cron expression "
                f"(minute hour day-of-month month day-of-week), got {self.cron!r}"
            )
        minute, hour, dom, month, dow = parts
        self._minute = parse_cron_part(minute, 0, 59)
        self._hour = parse_cron_part(hour, 0, 23)
        self._dom = parse_cron_part(dom, 1, 31)
        self._month = parse_cron_part(month, 1, 12)
        # Cron day-of-week is conventionally 0-6 with BOTH 0 and 7 meaning
        # Sunday - accept either spelling and normalize 7 -> 0 so either
        # one matches Python's own Sunday value below.
        dow_values = parse_cron_part(dow, 0, 7)
        self._dow = {(v % 7) for v in dow_values}
        self._dom_restricted = dom.strip() != '*'
        self._dow_restricted = dow.strip() != '*'
        self._last_fired_key = None
        self._tick = 0

    def _matches(self, dt: datetime) -> bool:
        if dt.minute not in self._minute or dt.hour not in self._hour or dt.month not in self._month:
            return False
        dom_ok = dt.day in self._dom
        # Python's isoweekday() is 1 (Monday) .. 7 (Sunday); `% 7` maps
        # Sunday (7) -> 0 and every other day to cron's own numbering
        # (Monday=1, ..., Saturday=6), matching self._dow above.
        dow_ok = (dt.isoweekday() % 7) in self._dow
        if self._dom_restricted and self._dow_restricted:
            # Standard (if famously surprising) cron rule: when *both*
            # day-of-month and day-of-week are restricted (neither is
            # '*'), a match on *either* is enough to fire - they are not
            # ANDed together the way every other field pair is.
            return dom_ok or dow_ok
        return dom_ok and dow_ok

    async def process(self):
        while self._running:
            now = datetime.now(timezone.utc)
            # Keyed to the minute (not a raw timestamp) so this fires
            # exactly once per matching minute no matter how often
            # check_interval wakes this loop up within that minute.
            key = (now.year, now.month, now.day, now.hour, now.minute)
            if key != self._last_fired_key and self._matches(now):
                self._last_fired_key = key
                self._tick += 1
                item = {
                    'cron': self.cron,
                    'tick': self._tick,
                    'timestamp': now.isoformat(),
                }
                if self.payload is not None:
                    item['payload'] = self.payload
                self.emit('out', item)
            await asyncio.sleep(self.check_interval)
