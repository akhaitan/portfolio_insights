import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Request, Body
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import asyncio
import json
from datetime import datetime, timezone
from app.config import DEFAULT_STOCKS, REFRESH_HOUR
from app.db import (
    init_db, get_user_stock_summary, get_articles, get_last_refresh,
    add_user_ticker, remove_user_ticker, get_user_tickers, get_stock_name,
    search_known_stocks, resolve_ticker, get_known_stock_count,
    upsert_known_stocks, get_known_stock_name,
    mark_article_read, mark_article_unread, get_read_article_ids,
    get_user_refresh_timestamps, get_stale_tickers,
)
from app.services.news import refresh_all_stocks, refresh_stock, refresh_user_stocks, refresh_stock_with_polygon
from app.services.events import refresh_events_for_user, refresh_all_events, get_user_events, refresh_events_for_ticker
from app.sources.polygon_tickers import fetch_all_us_tickers
from app.sources.massive import fetch_news_batch, RateLimitError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    scheduler.add_job(
        refresh_all_stocks,
        trigger=CronTrigger(hour=REFRESH_HOUR),
        id="nightly_refresh",
        replace_existing=True,
    )
    scheduler.add_job(
        refresh_all_events,
        trigger=CronTrigger(hour=REFRESH_HOUR, minute=30),
        id="nightly_events_refresh",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Scheduler started — nightly refresh at {REFRESH_HOUR}:00")

    # Populate known stocks if table has fewer than 100 entries
    count = get_known_stock_count()
    if count < 100:
        logger.info(f"Known stocks table has {count} entries — fetching US tickers from Polygon.io in background...")
        asyncio.create_task(_populate_known_stocks())
    else:
        logger.info(f"Known stocks table has {count} entries — skipping fetch")

    yield
    scheduler.shutdown()


async def _populate_known_stocks():
    """Background task to populate known stocks from Polygon.io."""
    try:
        tickers = await fetch_all_us_tickers()
        if not tickers:
            # Fallback: seed with DEFAULT_STOCKS if API fails
            fallback = [{"ticker": t, "name": n, "exchange": ""} for t, n in DEFAULT_STOCKS.items()]
            upsert_known_stocks(fallback)
            logger.warning(f"API fetch failed, seeded {len(fallback)} default stocks")
        else:
            logger.info(f"Finished populating {len(tickers)} known stocks")
    except Exception as e:
        logger.error(f"Failed to populate known stocks: {e}")


app = FastAPI(title="Stock News Digest", lifespan=lifespan)
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/api/stocks/search")
async def api_stock_search(q: str = "", username: str = ""):
    """Autocomplete endpoint — search known stocks by ticker or company name.
    
    If username is provided, results include an 'in_watchlist' flag.
    """
    results = search_known_stocks(q.strip(), limit=15)
    if username:
        user_tickers = set(get_user_tickers(username))
        for r in results:
            r["in_watchlist"] = r["ticker"] in user_tickers
    return JSONResponse(results)


@app.post("/api/stocks/refresh-list")
async def api_refresh_stock_list():
    """Manually re-fetch the known stocks list from Polygon.io."""
    tickers = await fetch_all_us_tickers()
    if tickers:
        upsert_known_stocks(tickers)
        return JSONResponse({"status": "ok", "count": len(tickers)})
    return JSONResponse({"status": "error", "message": "No tickers fetched"}, status_code=500)


def _format_ago(minutes: float) -> str:
    """Human-readable time-ago string."""
    if minutes < 1:
        return "just now"
    if minutes < 60:
        return f"{int(minutes)}m ago"
    hours = minutes / 60
    if hours < 24:
        return f"{int(hours)}h ago"
    return f"{int(hours / 24)}d ago"


@app.get("/{username}", response_class=HTMLResponse)
async def dashboard(request: Request, username: str):
    stocks = get_user_stock_summary(username, hours=24)
    tickers = get_user_tickers(username)
    all_articles = []
    for t in tickers:
        all_articles.extend(get_articles(t, hours=24))
    # Sort: unread first (newest first), then read (newest first)
    read_ids = get_read_article_ids(username)
    all_articles.sort(key=lambda a: a.get("published_at") or "", reverse=True)
    all_articles.sort(key=lambda a: a["id"] in read_ids)

    # Compute per-stock freshness indicators
    refresh_ts = get_user_refresh_timestamps(username)
    now = datetime.now(timezone.utc)
    for stock in stocks:
        ts = refresh_ts.get(stock["ticker"], {})
        nr = ts.get("news_refresh")
        er = ts.get("events_refresh")

        if nr:
            diff = (now - datetime.fromisoformat(nr)).total_seconds() / 60
            stock["news_freshness"] = "green" if diff < 5 else ("yellow" if diff < 60 else "red")
            stock["news_ago"] = _format_ago(diff)
        else:
            stock["news_freshness"] = "gray"
            stock["news_ago"] = "Never"

        if er:
            diff = (now - datetime.fromisoformat(er)).total_seconds() / 60
            stock["events_freshness"] = "green" if diff < 60 else ("yellow" if diff < 360 else "red")
            stock["events_ago"] = _format_ago(diff)
        else:
            stock["events_freshness"] = "gray"
            stock["events_ago"] = "Never"

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "username": username,
            "stocks": stocks,
            "articles": all_articles,
            "read_ids": read_ids,
        },
    )


@app.get("/{username}/stock/{ticker}", response_class=HTMLResponse)
async def stock_detail(request: Request, username: str, ticker: str):
    ticker = ticker.upper()
    user_tickers = get_user_tickers(username)
    if ticker not in user_tickers:
        return HTMLResponse("Ticker not in your watchlist", status_code=404)

    articles = get_articles(ticker, hours=24)
    last_refresh = get_last_refresh(ticker)
    name = get_stock_name(ticker) or ticker
    return templates.TemplateResponse(
        "stock.html",
        {
            "request": request,
            "username": username,
            "ticker": ticker,
            "name": name,
            "articles": articles,
            "last_refresh": last_refresh,
        },
    )


@app.post("/{username}/add")
async def add_ticker(username: str, ticker: str = Form(...)):
    raw_input = ticker.strip()
    # Resolve input to a known ticker (handles both ticker symbols and company names)
    resolved = resolve_ticker(raw_input)
    if not resolved:
        # User entered something we don't recognize
        return RedirectResponse(f"/{username}?error=unknown_ticker&q={raw_input}", status_code=303)

    # Use the resolved ticker and update the stocks table with the proper name
    known_name = get_known_stock_name(resolved)
    if known_name:
        from app.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO stocks (ticker, name) VALUES (?, ?) ON CONFLICT(ticker) DO UPDATE SET name = ?",
                (resolved, known_name, known_name),
            )
    add_user_ticker(username, resolved)
    # Auto-fetch if no cached articles for this ticker
    if not get_articles(resolved, hours=24):
        _count, _errors = await refresh_stock(resolved)
    return RedirectResponse(f"/{username}", status_code=303)


@app.post("/api/{username}/add-multiple")
async def add_multiple_tickers(username: str, tickers: list[str] = Body(...)):
    """Add multiple tickers at once. Expects a JSON body: ["AAPL", "Microsoft", ...]
    
    Each entry can be a ticker symbol or company name — they're resolved against the
    known stocks database. Returns per-line results so the UI can show what matched.
    """
    line_results = []
    added = []
    existing_tickers = set(get_user_tickers(username))

    for raw in tickers:
        raw = raw.strip()
        if not raw:
            continue
        resolved = resolve_ticker(raw)
        if not resolved:
            line_results.append({"input": raw, "status": "not_found", "ticker": None, "name": None})
            continue

        known_name = get_known_stock_name(resolved) or resolved

        if resolved in existing_tickers:
            line_results.append({"input": raw, "status": "already_exists", "ticker": resolved, "name": known_name})
            continue

        # Update stocks table with proper name
        from app.db import get_db
        with get_db() as conn:
            conn.execute(
                "INSERT INTO stocks (ticker, name) VALUES (?, ?) ON CONFLICT(ticker) DO UPDATE SET name = ?",
                (resolved, known_name, known_name),
            )
        add_user_ticker(username, resolved)
        added.append(resolved)
        existing_tickers.add(resolved)
        line_results.append({"input": raw, "status": "added", "ticker": resolved, "name": known_name})

    # Fetch news for newly added tickers in parallel
    async def fetch_if_needed(t):
        if not get_articles(t, hours=24):
            _count, _errors = await refresh_stock(t)

    if added:
        await asyncio.gather(*[fetch_if_needed(t) for t in added])

    return JSONResponse({
        "status": "ok",
        "results": line_results,
        "added_count": len(added),
    })


@app.post("/{username}/remove/{ticker}")
async def remove_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    remove_user_ticker(username, ticker)
    return RedirectResponse(f"/{username}", status_code=303)


@app.get("/{username}/calendar", response_class=HTMLResponse)
async def calendar_page(request: Request, username: str):
    tickers = get_user_tickers(username)
    return templates.TemplateResponse(
        "calendar.html",
        {
            "request": request,
            "username": username,
            "tickers": tickers,
        },
    )


@app.get("/api/events/{username}")
async def api_get_events(username: str, start: str = "", end: str = ""):
    events = get_user_events(username, start_date=start or None, end_date=end or None)
    # Format for FullCalendar
    fc_events = []
    color_map = {
        "dividend_ex": "#10b981",    # green
        "dividend_pay": "#6366f1",   # indigo
        "split": "#f59e0b",          # amber
    }
    for e in events:
        fc_events.append({
            "id": e["id"],
            "title": e["title"],
            "start": e["event_date"],
            "allDay": True,
            "color": color_map.get(e["event_type"], "#6b7280"),
            "extendedProps": {
                "ticker": e["ticker"],
                "event_type": e["event_type"],
                "description": e.get("description") or "",
                "metadata": e.get("metadata") or "",
            },
        })
    return JSONResponse(fc_events)


@app.post("/api/events/refresh/{username}")
async def api_refresh_events(username: str):
    results, errors, skipped = await refresh_events_for_user(username, stale_minutes=60)
    total = sum(results.values())
    response = {
        "status": "partial" if errors else "ok",
        "total_events": total,
        "per_ticker": results,
        "skipped": skipped,
    }
    if errors:
        response["errors"] = errors
    return JSONResponse(response)


@app.post("/api/articles/{article_id}/read")
async def api_mark_read(article_id: int, username: str = Form(...)):
    mark_article_read(username, article_id)
    return JSONResponse({"status": "ok", "article_id": article_id, "is_read": True})


@app.post("/api/articles/{article_id}/unread")
async def api_mark_unread(article_id: int, username: str = Form(...)):
    mark_article_unread(username, article_id)
    return JSONResponse({"status": "ok", "article_id": article_id, "is_read": False})


@app.post("/api/refresh/{username}")
async def api_refresh_user(username: str):
    results, errors, skipped = await refresh_user_stocks(username, stale_minutes=5)
    total = sum(results.values())
    response = {
        "status": "partial" if errors else "ok",
        "total_articles": total,
        "per_ticker": results,
        "skipped": skipped,
    }
    if errors:
        response["errors"] = errors
    return JSONResponse(response)


# --- Streaming refresh endpoints (SSE) ---
# NOTE: These must be registered BEFORE the /{ticker} catch-all route
# so that "/stream" is matched literally instead of as a ticker name.

@app.post("/api/refresh/{username}/stream")
async def api_refresh_user_stream(username: str):
    """Stream progress updates as news is refreshed for each ticker.

    Phase 1: Single batch Polygon.io call for all stale tickers.
    Phase 2: Per-ticker Yahoo RSS + merge + cache with progress updates.
    """
    async def generate():
        all_tickers = get_user_tickers(username)
        stale = get_stale_tickers(username, stale_minutes=5, log_table="refresh_log")
        stale_set = set(stale)
        skipped = [t for t in all_tickers if t not in stale_set]

        yield f"data: {json.dumps({'type': 'init', 'queue': stale, 'skipped': skipped})}\n\n"

        if not stale:
            yield f"data: {json.dumps({'type': 'complete', 'errors': []})}\n\n"
            return

        # Phase 1: Batch fetch from Polygon.io (single API call for all tickers)
        yield f"data: {json.dumps({'type': 'batch_start', 'source': 'Polygon.io', 'count': len(stale)})}\n\n"
        polygon_batch: dict[str, list] = {}
        polygon_ok = False
        all_errors: list[str] = []
        try:
            polygon_batch = await fetch_news_batch(stale)
            polygon_ok = True
        except RateLimitError:
            all_errors.append("Polygon.io rate limited (batch)")
        except Exception as e:
            all_errors.append(f"Polygon.io batch error: {str(e)[:120]}")

        yield f"data: {json.dumps({'type': 'batch_done', 'success': polygon_ok})}\n\n"

        # Phase 2: Per-ticker Yahoo RSS + merge + cache
        for ticker in stale:
            yield f"data: {json.dumps({'type': 'processing', 'ticker': ticker})}\n\n"
            count, errors = await refresh_stock_with_polygon(
                ticker, polygon_batch.get(ticker, []), polygon_ok
            )
            all_errors.extend(errors)
            yield f"data: {json.dumps({'type': 'ticker_done', 'ticker': ticker, 'count': count, 'errors': errors})}\n\n"

        yield f"data: {json.dumps({'type': 'complete', 'errors': all_errors})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/refresh/{username}/{ticker}")
async def api_refresh_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    user_tickers = get_user_tickers(username)
    if ticker not in user_tickers:
        return JSONResponse({"error": "Ticker not in watchlist"}, status_code=404)

    count, errors = await refresh_stock(ticker)
    response = {
        "status": "partial" if errors else "ok",
        "ticker": ticker,
        "articles_fetched": count,
    }
    if errors:
        response["errors"] = errors
    return JSONResponse(response)


@app.post("/api/events/refresh/{username}/stream")
async def api_refresh_events_stream(username: str):
    """Stream progress updates as events are refreshed for each ticker."""
    async def generate():
        all_tickers = get_user_tickers(username)
        stale = get_stale_tickers(username, stale_minutes=60, log_table="events_refresh_log")
        stale_set = set(stale)
        skipped = [t for t in all_tickers if t not in stale_set]

        yield f"data: {json.dumps({'type': 'init', 'queue': stale, 'skipped': skipped})}\n\n"

        all_errors: list[str] = []
        for ticker in stale:
            yield f"data: {json.dumps({'type': 'processing', 'ticker': ticker})}\n\n"
            count, errors = await refresh_events_for_ticker(ticker)
            all_errors.extend(errors)
            yield f"data: {json.dumps({'type': 'ticker_done', 'ticker': ticker, 'count': count, 'errors': errors})}\n\n"

        yield f"data: {json.dumps({'type': 'complete', 'errors': all_errors})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
