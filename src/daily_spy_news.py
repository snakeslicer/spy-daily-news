from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import feedparser
import requests
from dateutil import parser as dateparser
from rapidfuzz import fuzz


@dataclass(frozen=True)
class FeedConfig:
    name: str
    url: str
    weight: float = 1.0


@dataclass(frozen=True)
class Item:
    title: str
    link: str
    source: str
    published_utc: Optional[datetime]
    summary: str


SECTION_ORDER = [
    "Macro_Fed",
    "Rates_Credit",
    "Equities",
    "Commodities",
    "FX",
    "Geopolitics",
    "Volatility",
    "Other",
]


SECTION_RULES: list[tuple[str, list[str]]] = [
    (
        "Macro_Fed",
        [
            "fed",
            "fomc",
            "powell",
            "inflation",
            "cpi",
            "ppi",
            "pce",
            "jobs",
            "payroll",
            "unemployment",
            "gdp",
            "ism",
            "retail sales",
            "consumer confidence",
            "treasury auction",
            "budget",
            "shutdown",
        ],
    ),
    (
        "Rates_Credit",
        [
            "yield",
            "yields",
            "10-year",
            "2-year",
            "bond",
            "bonds",
            "treasury",
            "spreads",
            "credit",
            "junk",
            "high-yield",
            "investment grade",
            "mortgage",
            "mbs",
            "curve",
            "inversion",
        ],
    ),
    (
        "Volatility",
        [
            "vix",
            "volatility",
            "options",
            "gamma",
            "skew",
            "put",
            "call",
        ],
    ),
    (
        "Commodities",
        [
            "oil",
            "wti",
            "brent",
            "gas",
            "opec",
            "gold",
            "silver",
            "copper",
            "commodit",
            "energy",
        ],
    ),
    (
        "FX",
        [
            "dollar",
            "usd",
            "eur",
            "yen",
            "jpy",
            "cny",
            "yuan",
            "fx",
            "currency",
        ],
    ),
    (
        "Geopolitics",
        [
            "ukraine",
            "russia",
            "china",
            "taiwan",
            "middle east",
            "israel",
            "iran",
            "gaza",
            "red sea",
            "sanction",
            "tariff",
            "trade war",
            "election",
        ],
    ),
    (
        "Equities",
        [
            "stocks",
            "equities",
            "s&p",
            "sp 500",
            "nasdaq",
            "dow",
            "earnings",
            "guidance",
            "buyback",
            "dividend",
            "ipo",
            "ai",
            "semiconductor",
            "bank",
        ],
    ),
]


def _clean_whitespace(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _strip_html(s: str) -> str:
    # Cheap/robust: remove tags; RSS summaries are typically short.
    return _clean_whitespace(re.sub(r"<[^>]+>", " ", s or ""))


def _parse_published_utc(entry: dict) -> Optional[datetime]:
    # feedparser may provide structured time or strings.
    for key in ("published", "updated", "created"):
        if key in entry and entry.get(key):
            try:
                dt = dateparser.parse(str(entry.get(key)))
                if not dt:
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt.astimezone(UTC)
            except Exception:
                continue

    for key in ("published_parsed", "updated_parsed"):
        t = entry.get(key)
        if t:
            try:
                dt = datetime(*t[:6], tzinfo=UTC)
                return dt
            except Exception:
                continue

    return None


def load_feeds(path: Path) -> tuple[str, list[FeedConfig]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    tz = str(data.get("timezone") or "UTC")
    feeds_raw = data.get("feeds") or []
    feeds: list[FeedConfig] = []
    for f in feeds_raw:
        if not isinstance(f, dict):
            continue
        name = str(f.get("name") or "").strip()
        url = str(f.get("url") or "").strip()
        if not name or not url:
            continue
        weight = float(f.get("weight") or 1.0)
        feeds.append(FeedConfig(name=name, url=url, weight=weight))
    if not feeds:
        raise ValueError("feeds.json contains no valid feeds.")
    return tz, feeds


def fetch_feed(url: str, *, timeout_s: int = 20) -> feedparser.FeedParserDict:
    headers = {
        "User-Agent": "spy-daily-news/1.0 (+https://github.com/)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    resp = requests.get(url, headers=headers, timeout=timeout_s)
    resp.raise_for_status()
    return feedparser.parse(resp.content)


def iter_items(
    feed_name: str, parsed: feedparser.FeedParserDict, *, max_items: int
) -> Iterable[Item]:
    entries = list(parsed.get("entries") or [])[:max_items]
    for e in entries:
        title = _clean_whitespace(str(e.get("title") or ""))
        link = _clean_whitespace(str(e.get("link") or ""))
        if not title or not link:
            continue
        published_utc = _parse_published_utc(e)
        summary = _strip_html(str(e.get("summary") or e.get("description") or ""))
        yield Item(
            title=title,
            link=link,
            source=feed_name,
            published_utc=published_utc,
            summary=summary,
        )


def _normalize_for_dedupe(s: str) -> str:
    s = s.lower()
    s = re.sub(r"https?://\S+", " ", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return _clean_whitespace(s)


def dedupe_items(items: list[Item], *, threshold: int = 92) -> list[Item]:
    kept: list[Item] = []
    kept_norm: list[str] = []
    for it in items:
        n = _normalize_for_dedupe(it.title)
        is_dup = False
        for existing in kept_norm:
            if fuzz.token_set_ratio(n, existing) >= threshold:
                is_dup = True
                break
        if not is_dup:
            kept.append(it)
            kept_norm.append(n)
    return kept


def assign_section(title: str) -> str:
    t = title.lower()
    for section, keywords in SECTION_RULES:
        for kw in keywords:
            if kw in t:
                return section
    return "Other"


def _date_window_utc(day: datetime) -> tuple[datetime, datetime]:
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)
    return start, end


def filter_to_date(items: list[Item], day_utc: datetime) -> list[Item]:
    start, end = _date_window_utc(day_utc)
    kept: list[Item] = []
    for it in items:
        if it.published_utc is None:
            kept.append(it)
            continue
        if start <= it.published_utc < end:
            kept.append(it)
    return kept


def format_output(day_utc: datetime, items: list[Item]) -> str:
    day_str = day_utc.strftime("%Y-%m-%d")
    now_str = datetime.now(tz=UTC).strftime("%Y-%m-%d %H:%M:%SZ")

    buckets: dict[str, list[Item]] = {s: [] for s in SECTION_ORDER}
    for it in items:
        sec = assign_section(it.title)
        buckets.setdefault(sec, []).append(it)

    # Deterministic ordering within sections: published desc, then title/link.
    def key(it: Item):
        ts = it.published_utc or datetime(1970, 1, 1, tzinfo=UTC)
        return (-int(ts.timestamp()), it.title.lower(), it.link)

    lines: list[str] = []
    lines.append(f"$SPY daily news summary — {day_str} (UTC)")
    lines.append(f"Generated: {now_str}")
    lines.append("")

    total = 0
    for sec in SECTION_ORDER:
        sec_items = sorted(buckets.get(sec, []), key=key)
        if not sec_items:
            continue
        lines.append(f"== {sec.replace('_', ' ')} ==")
        for it in sec_items:
            total += 1
            ts = it.published_utc.strftime("%Y-%m-%d %H:%MZ") if it.published_utc else "unknown"
            lines.append(f"- [{it.source}] {ts} — {it.title}")
            lines.append(f"  {it.link}")
        lines.append("")

    if total == 0:
        lines.append("No items found for the selected date window.")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate daily SPY news summary from RSS feeds.")
    p.add_argument(
        "--date",
        default=None,
        help="UTC date to generate (YYYY-MM-DD). Defaults to today's UTC date.",
    )
    p.add_argument(
        "--feeds",
        default=str(Path(__file__).with_name("feeds.json")),
        help="Path to feeds.json.",
    )
    p.add_argument(
        "--out",
        default=None,
        help="Output path. Default: summaries/YYYY-MM-DD.txt at repo root.",
    )
    p.add_argument("--max-per-feed", type=int, default=30, help="Max items to read per feed.")
    p.add_argument("--dedupe-threshold", type=int, default=92, help="0-100 similarity cutoff.")
    p.add_argument("--timeout", type=int, default=20, help="HTTP timeout seconds per feed.")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if args.date:
        day = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=UTC)
    else:
        day = datetime.now(tz=UTC)
        day = datetime(day.year, day.month, day.day, tzinfo=UTC)

    feeds_path = Path(args.feeds)
    _, feeds = load_feeds(feeds_path)

    # Default output to this project's summaries/ regardless of where it's run from.
    project_root = Path(__file__).resolve().parents[1]
    out_path = Path(args.out) if args.out else (project_root / "summaries" / f"{day.strftime('%Y-%m-%d')}.txt")
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_items: list[Item] = []
    failures: list[str] = []
    for f in feeds:
        try:
            parsed = fetch_feed(f.url, timeout_s=int(args.timeout))
            all_items.extend(iter_items(f.name, parsed, max_items=int(args.max_per_feed)))
        except Exception as e:
            failures.append(f"{f.name}: {e}")

    # Prefer dated filtering when timestamps exist; keep unknown timestamps.
    all_items = filter_to_date(all_items, day)

    # Sort before dedupe so we keep the most recent headline when duplicates exist.
    all_items.sort(
        key=lambda it: (it.published_utc or datetime(1970, 1, 1, tzinfo=UTC)),
        reverse=True,
    )
    all_items = dedupe_items(all_items, threshold=int(args.dedupe_threshold))

    content = format_output(day, all_items)
    if failures:
        content += "\n"
        content += "== Feed fetch failures ==\n"
        for f in failures:
            content += f"- {f}\n"

    out_path.write_text(content, encoding="utf-8")
    print(f"Wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

