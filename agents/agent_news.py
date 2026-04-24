"""ATW-NEWS — standalone live web-search agent.

Runs DuckDuckGo searches directly in Python (same `ddgs` library agno's
WebSearchTools wraps), then hands the raw hits to a single Groq LLM call
that fills a fixed Pydantic schema. This avoids Groq's flaky model-side
tool-calling entirely. Terminal output only. No DB writes, no file writes.
  python agents/agent_news.py              # default 5 angles
  python agents/agent_news.py --raw        # JSON dump
  python agents/agent_news.py --per-query 6  # bigger search depth
  python agents/agent_news.py --query "Only ATW Q1 2026 earnings"
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Literal

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from dotenv import load_dotenv
from pydantic import BaseModel, Field

from agno.agent import Agent
from agno.models.groq import Groq
from ddgs import DDGS


# --- Schema ------------------------------------------------------------------

class NewsItem(BaseModel):
    date: str = Field(description="Publication date ISO (YYYY-MM-DD) if stated, else today.")
    title: str = Field(description="Article headline.")
    source: str = Field(description="Publisher name (Reuters, Médias24, Boursenews, L'Économiste, etc.).")
    url: str = Field(description="Direct link.")
    summary: str = Field(description="1-2 sentence summary.")
    category: Literal[
        "EARNINGS", "DIVIDEND", "ANALYST", "REGULATORY",
        "MA", "MACRO", "AFRICA", "GEOPOLITICS", "COMMODITY", "OTHER",
    ] = Field(description="GEOPOLITICS=wars/conflicts/sanctions. COMMODITY=oil/gold/wheat shocks. MACRO=rates/inflation/FX.")
    signal_score: int = Field(
        ge=0, le=100,
        description="0-100. 75+ price-moving, 60-74 meaningful, 30-59 contextual, <30 noise.",
    )
    is_atw_core: bool = Field(description="True if primarily about ATW itself.")
    bucket: Literal["HIGH", "MEDIUM", "CONTEXT", "NOISE"] = Field(
        description="HIGH >=75, MEDIUM 60-74, CONTEXT 30-59, NOISE <30."
    )


class NewsBrief(BaseModel):
    as_of_date: str
    items: list[NewsItem] = Field(description="Relevant articles, excluding NOISE.")
    sector_pulse: str = Field(description="2-3 sentences tying global backdrop to Moroccan/ATW outlook.")
    sentiment_verdict: Literal["POSITIVE", "NEUTRAL", "NEGATIVE"]
    sentiment_reasoning: str


# --- Search queries ----------------------------------------------------------

ATW_SEARCH_HINTS = [
    "Attijariwafa Bank résultats earnings PNB 2026",
    "Attijariwafa Bank dividend analyst rating M&A",
    "Attijariwafa Bank Afrique CBAO Wafa Assurance",
    "Maroc inflation BAM taux directeur dirham MASI",
    "global crisis war Fed ECB Brent markets 2026",
]


def run_searches(queries: list[str], per_query: int = 4) -> list[dict]:
    """Run DDG text searches directly. Returns deduped hits."""
    hits: list[dict] = []
    seen_urls: set[str] = set()
    with DDGS() as ddg:
        for q in queries:
            try:
                results = list(ddg.text(q, max_results=per_query))
            except Exception as e:
                print(f"[search] {q!r} failed: {e}", flush=True)
                continue
            for r in results:
                url = r.get("href") or r.get("url") or ""
                if not url or url in seen_urls:
                    continue
                seen_urls.add(url)
                hits.append({
                    "query": q,
                    "title": r.get("title", ""),
                    "url": url,
                    "snippet": r.get("body", "") or r.get("snippet", ""),
                })
    return hits


# --- LLM synthesis -----------------------------------------------------------

SYNTH_INSTRUCTIONS = [
    "You are ATW-NEWS — track Attijariwafa Bank (ticker ATW, Casablanca Bourse) and global context that moves Moroccan equities.",
    "You will receive a list of web search hits (title, url, snippet, query).",
    "Convert them into a NewsBrief using ONLY information in the hits. Never invent URLs, dates, or sources.",
    "For each hit you include, fill every NewsItem field.",
    "Score signal_score 0-100 based on impact: 75+ price-moving (earnings, guidance, rating changes, M&A), 60-74 meaningful (analyst notes, regulatory), 30-59 contextual (macro, sector, global backdrop), <30 noise.",
    "Derive bucket from signal_score: >=75 HIGH, 60-74 MEDIUM, 30-59 CONTEXT, <30 NOISE.",
    "Exclude NOISE from items.",
    "Include global items (wars, Fed/ECB, oil, crises) ONLY if they plausibly affect Moroccan markets or ATW.",
    "sector_pulse: 2-3 sentences linking global backdrop to the Moroccan/ATW outlook.",
    "sentiment_verdict + sentiment_reasoning: overall read of the hits.",
    "If no hit qualifies, return items=[] with a sector_pulse explaining the quiet tape.",
    "Keep each summary to ONE sentence. Return at most 8 items total (prioritize highest signal_score).",
    "Always fill sector_pulse, sentiment_verdict, and sentiment_reasoning — never omit them.",
]


def _build_synth_agent() -> Agent:
    model_id = os.getenv("GROQ_MODEL", "openai/gpt-oss-120b")
    return Agent(
        model=Groq(id=model_id, max_tokens=4096, temperature=0.2),
        output_schema=NewsBrief,
        instructions=SYNTH_INSTRUCTIONS,
    )


def _format_hits_for_llm(hits: list[dict], today: str) -> str:
    if not hits:
        return f"No hits. Today is {today}. Return items=[] with a sector_pulse noting a quiet tape."
    blocks = [f"Today: {today}", f"Hits ({len(hits)}):", ""]
    for i, h in enumerate(hits, 1):
        snippet = (h["snippet"] or "").strip().replace("\n", " ")
        if len(snippet) > 300:
            snippet = snippet[:300] + "…"
        blocks.append(
            f"[{i}] query={h['query']}\n"
            f"    title={h['title']}\n"
            f"    url={h['url']}\n"
            f"    snippet={snippet}"
        )
    return "\n".join(blocks)


# --- Output ------------------------------------------------------------------

def _print_brief(brief: NewsBrief) -> None:
    print(f"\n═══ ATW NEWS BRIEF — {brief.as_of_date} ═══\n")
    for bucket, marker in (("HIGH", "🔴"), ("MEDIUM", "🟡"), ("CONTEXT", "🟢")):
        items = [i for i in brief.items if i.bucket == bucket]
        if not items:
            continue
        print(f"{marker} {bucket}")
        for it in items:
            print(f"  • [{it.source}] {it.title}")
            print(f"    {it.summary}")
            print(f"    category={it.category} score={it.signal_score} core={it.is_atw_core}")
            print(f"    {it.url}\n")
    print(f"🧭 SECTOR PULSE\n  {brief.sector_pulse}\n")
    print(f"📊 VERDICT: {brief.sentiment_verdict}\n   {brief.sentiment_reasoning}\n")


def main() -> int:
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")
    parser = argparse.ArgumentParser(description="ATW standalone news intelligence agent")
    parser.add_argument("--query", type=str, default=None,
                        help="Override: run ONE custom query instead of the default angles.")
    parser.add_argument("--raw", action="store_true",
                        help="Print the raw NewsBrief JSON instead of the formatted brief.")
    parser.add_argument("--per-query", type=int, default=4,
                        help="Max results per search query (default 4).")
    args = parser.parse_args()

    if not os.getenv("GROQ_API_KEY"):
        print("ERROR: GROQ_API_KEY not set in .env", flush=True)
        return 2

    today = datetime.now().date().isoformat()
    queries = [args.query] if args.query else ATW_SEARCH_HINTS

    print(f"[search] running {len(queries)} queries...", flush=True)
    hits = run_searches(queries, per_query=args.per_query)
    print(f"[search] {len(hits)} unique hits collected.", flush=True)

    agent = _build_synth_agent()
    prompt = _format_hits_for_llm(hits, today)
    resp = agent.run(prompt)
    brief = resp.content
    if not isinstance(brief, NewsBrief):
        print("ERROR: LLM did not return a NewsBrief. Raw content:\n")
        print(brief)
        return 3

    if args.raw:
        print(brief.model_dump_json(indent=2))
    else:
        _print_brief(brief)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
