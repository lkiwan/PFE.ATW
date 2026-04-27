"""ATW autorun — single long-running scheduler for all data refresh jobs.

Windows has no native cron, so this script IS the cron: one Python process
that wakes every TICK_SECONDS, checks which buckets are due, and runs them
as subprocesses. Three independent cadences:

  - NEWS     : every 1 hour       → every news_crawler/ATW_*_news.py
  - REALTIME : every 15 minutes   → scrapers/atw_realtime_scraper.py snapshot
  - MONTHLY  : 1st of month 02:00 → scrapers/fondamental_scraper.py
                                    scrapers/atw_macro_collector.py

Usage:
    python autorun/scheduler.py                 # run forever
    python autorun/scheduler.py --once NEWS     # run one bucket and exit
    python autorun/scheduler.py --once REALTIME
    python autorun/scheduler.py --once MONTHLY
    python autorun/scheduler.py --status        # show next-fire times and exit

Logs to autorun/autorun.log + stdout. Stop with Ctrl-C.
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Literal

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


# =============================================================================
# CONFIG — paths, cadences, timeouts
# =============================================================================

PROJECT_ROOT = Path(__file__).resolve().parents[1]
NEWS_DIR = PROJECT_ROOT / "news_crawler"
SCRAPERS_DIR = PROJECT_ROOT / "scrapers"
LOG_FILE = Path(__file__).resolve().parent / "autorun.log"

PY = sys.executable

NEWS_INTERVAL = timedelta(hours=1)
REALTIME_INTERVAL = timedelta(minutes=15)
MONTHLY_DAY_OF_MONTH = 1
MONTHLY_HOUR = 2
MONTHLY_MINUTE = 0

NEWS_JOB_TIMEOUT_S = 30 * 60      # 30 min per news source
REALTIME_JOB_TIMEOUT_S = 5 * 60   # 5 min for one snapshot
MONTHLY_JOB_TIMEOUT_S = 60 * 60   # 1 hour each for fundamentals/macro

# Casablanca Bourse trading window (Mon–Fri, local time).
# Continuous session is ~09:30–15:30; window is widened to cover pre-open
# and closing auctions. Outside this window, REALTIME is parked.
REALTIME_MARKET_OPEN_HOUR = 9
REALTIME_MARKET_CLOSE_HOUR = 16
REALTIME_SKIP_WEEKENDS = True

TICK_SECONDS = 30


# =============================================================================
# LOGGING — file + stdout, both UTF-8
# =============================================================================

def setup_logging() -> logging.Logger:
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger = logging.getLogger("autorun")
    logger.setLevel(logging.INFO)
    logger.handlers = [fh, sh]
    logger.propagate = False
    return logger


log = setup_logging()


# =============================================================================
# JOB DEFINITIONS — what each bucket runs (rebuilt at fire time)
# =============================================================================

def news_commands() -> list[list[str]]:
    files = sorted(NEWS_DIR.glob("ATW_*_news.py"))
    return [[PY, str(f)] for f in files]


def realtime_commands() -> list[list[str]]:
    return [[PY, str(SCRAPERS_DIR / "atw_realtime_scraper.py"), "snapshot"]]


def monthly_commands() -> list[list[str]]:
    return [
        [PY, str(SCRAPERS_DIR / "fondamental_scraper.py")],
        [PY, str(SCRAPERS_DIR / "atw_macro_collector.py")],
    ]


# =============================================================================
# JOB EXECUTION — subprocess wrapper with timeout + structured logging
# =============================================================================

def run_command(label: str, cmd: list[str], timeout_s: int) -> int:
    name = Path(cmd[1]).name if len(cmd) > 1 else cmd[0]
    log.info("[%s] starting %s", label, name)
    started = time.monotonic()
    try:
        result = subprocess.run(
            cmd,
            cwd=str(PROJECT_ROOT),
            timeout=timeout_s,
            capture_output=False,
        )
        rc = result.returncode
    except subprocess.TimeoutExpired:
        log.error("[%s] %s TIMEOUT after %ss", label, name, timeout_s)
        return -1
    except Exception as e:
        log.exception("[%s] %s crashed: %s", label, name, e)
        return -2
    elapsed = time.monotonic() - started
    level = logging.INFO if rc == 0 else logging.WARNING
    log.log(level, "[%s] %s exit=%s elapsed=%.1fs", label, name, rc, elapsed)
    return rc


def run_bucket(label: str, commands: list[list[str]], timeout_s: int) -> None:
    if not commands:
        log.warning("[%s] no commands to run", label)
        return
    log.info("[%s] bucket start (%d commands)", label, len(commands))
    for cmd in commands:
        run_command(label, cmd, timeout_s)
    log.info("[%s] bucket done", label)


# =============================================================================
# SCHEDULER — interval + monthly cadence tracking
# =============================================================================

ScheduleKind = Literal["interval", "monthly"]


@dataclass
class Job:
    label: str
    kind: ScheduleKind
    build_commands: Callable[[], list[list[str]]]
    timeout_s: int
    interval: timedelta | None = None
    next_fire: datetime = field(default_factory=datetime.now)


def is_market_open(now: datetime) -> bool:
    """Casablanca Bourse open: Mon–Fri inside [OPEN_HOUR, CLOSE_HOUR)."""
    if REALTIME_SKIP_WEEKENDS and now.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return REALTIME_MARKET_OPEN_HOUR <= now.hour < REALTIME_MARKET_CLOSE_HOUR


def next_market_open(after: datetime) -> datetime:
    """Next weekday at OPEN_HOUR:00 strictly after `after`."""
    candidate = after.replace(
        hour=REALTIME_MARKET_OPEN_HOUR, minute=0, second=0, microsecond=0
    )
    if candidate <= after:
        candidate += timedelta(days=1)
    while REALTIME_SKIP_WEEKENDS and candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return candidate


def next_monthly(after: datetime) -> datetime:
    """Next 1st-of-month at MONTHLY_HOUR:MONTHLY_MINUTE strictly after `after`."""
    candidate = after.replace(
        day=MONTHLY_DAY_OF_MONTH,
        hour=MONTHLY_HOUR,
        minute=MONTHLY_MINUTE,
        second=0,
        microsecond=0,
    )
    if candidate <= after:
        if candidate.month == 12:
            candidate = candidate.replace(year=candidate.year + 1, month=1)
        else:
            candidate = candidate.replace(month=candidate.month + 1)
    return candidate


def build_jobs() -> list[Job]:
    now = datetime.now()
    realtime_next = now if is_market_open(now) else next_market_open(now)
    return [
        Job(
            label="NEWS",
            kind="interval",
            build_commands=news_commands,
            timeout_s=NEWS_JOB_TIMEOUT_S,
            interval=NEWS_INTERVAL,
            next_fire=now,
        ),
        Job(
            label="REALTIME",
            kind="interval",
            build_commands=realtime_commands,
            timeout_s=REALTIME_JOB_TIMEOUT_S,
            interval=REALTIME_INTERVAL,
            next_fire=realtime_next,
        ),
        Job(
            label="MONTHLY",
            kind="monthly",
            build_commands=monthly_commands,
            timeout_s=MONTHLY_JOB_TIMEOUT_S,
            next_fire=next_monthly(now),
        ),
    ]


def reschedule(job: Job, fired_at: datetime) -> None:
    if job.kind == "interval":
        assert job.interval is not None
        job.next_fire = fired_at + job.interval
    else:
        job.next_fire = next_monthly(fired_at)


def scheduler_loop(jobs: list[Job]) -> None:
    log.info("autorun scheduler started — %d jobs", len(jobs))
    for j in jobs:
        log.info(
            "  [%s] kind=%s next_fire=%s",
            j.label, j.kind, j.next_fire.isoformat(timespec="seconds"),
        )
    while True:
        now = datetime.now()
        for j in jobs:
            if now < j.next_fire:
                continue
            if j.label == "REALTIME" and not is_market_open(now):
                j.next_fire = next_market_open(now)
                log.info(
                    "[REALTIME] market closed (weekday=%s hour=%02d) — "
                    "parked until %s",
                    now.strftime("%A"), now.hour,
                    j.next_fire.isoformat(timespec="seconds"),
                )
                continue
            run_bucket(j.label, j.build_commands(), j.timeout_s)
            reschedule(j, datetime.now())
            log.info(
                "[%s] next_fire=%s",
                j.label, j.next_fire.isoformat(timespec="seconds"),
            )
        time.sleep(TICK_SECONDS)


# =============================================================================
# CLI / MAIN
# =============================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ATW autorun scheduler")
    p.add_argument(
        "--once",
        choices=["NEWS", "REALTIME", "MONTHLY"],
        help="Run one bucket once and exit (no scheduling).",
    )
    p.add_argument(
        "--status",
        action="store_true",
        help="Print next-fire times for all jobs and exit.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    jobs = build_jobs()

    if args.status:
        for j in jobs:
            print(
                f"[{j.label:8}] kind={j.kind:9} "
                f"next_fire={j.next_fire.isoformat(timespec='seconds')}"
            )
        return 0

    if args.once:
        target = next(j for j in jobs if j.label == args.once)
        run_bucket(target.label, target.build_commands(), target.timeout_s)
        return 0

    try:
        scheduler_loop(jobs)
    except KeyboardInterrupt:
        log.info("autorun stopped by user")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
