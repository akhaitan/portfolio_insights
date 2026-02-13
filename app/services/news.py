from __future__ import annotations

import asyncio
import logging
from app.sources import massive, yahoo_rss
from app.sources.massive import RateLimitError
from app.db import (
    upsert_articles, update_refresh_log, get_articles, get_all_recent_articles,
    purge_old_articles, get_all_watched_tickers, get_user_tickers, get_stock_name,
    delete_articles_for_ticker,
)

logger = logging.getLogger(__name__)

SUMMARY_PREFIX_LEN = 200


def _is_relevant(article: dict, ticker: str, company_name: str | None) -> bool:
    """Article must mention the company in the title, and ticker/name must appear in title or summary prefix."""
    title = (article.get("title") or "").lower()
    summary_prefix = (article.get("summary") or "")[:SUMMARY_PREFIX_LEN].lower()

    # Title must mention the ticker or company name
    title_match = ticker.lower() in title or (company_name and company_name.lower() in title)
    if not title_match:
        return False

    # Also check broader text for reinforcement
    text = title + " " + summary_prefix
    if ticker.lower() in text:
        return True
    if company_name and company_name.lower() in text:
        return True
    return False


async def refresh_stock(ticker: str) -> int:
    articles = []

    try:
        articles = await massive.fetch_news(ticker)
    except RateLimitError:
        logger.info(f"Falling back to Yahoo RSS for {ticker}")

    yahoo_articles = await yahoo_rss.fetch_news(ticker)
    existing_urls = {a["url"] for a in articles}
    for a in yahoo_articles:
        if a["url"] not in existing_urls:
            articles.append(a)

    company_name = get_stock_name(ticker)
    articles = [a for a in articles if _is_relevant(a, ticker, company_name)]

    delete_articles_for_ticker(ticker)
    if articles:
        upsert_articles(articles)
    update_refresh_log(ticker)

    return len(articles)


async def refresh_all_stocks() -> dict[str, int]:
    tickers = get_all_watched_tickers()
    results = {}
    for ticker in tickers:
        count = await refresh_stock(ticker)
        results[ticker] = count
        await asyncio.sleep(0.2)

    purge_old_articles()
    return results


async def refresh_user_stocks(username: str) -> dict[str, int]:
    tickers = get_user_tickers(username)
    results = {}
    for ticker in tickers:
        count = await refresh_stock(ticker)
        results[ticker] = count
        await asyncio.sleep(0.2)
    return results


def get_digest(ticker: str | None = None, hours: int = 24) -> list[dict]:
    if ticker:
        return get_articles(ticker, hours)
    return get_all_recent_articles(hours)
