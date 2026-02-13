import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import DEFAULT_STOCKS, REFRESH_HOUR
from app.db import (
    init_db, get_user_stock_summary, get_articles, get_last_refresh,
    add_user_ticker, remove_user_ticker, get_user_tickers, get_stock_name,
)
from app.services.news import refresh_all_stocks, refresh_stock, refresh_user_stocks

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
    scheduler.start()
    logger.info(f"Scheduler started — nightly refresh at {REFRESH_HOUR}:00")
    yield
    scheduler.shutdown()


app = FastAPI(title="Stock News Digest", lifespan=lifespan)
templates = Jinja2Templates(directory="app/templates")


@app.get("/", response_class=HTMLResponse)
async def landing(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})


@app.get("/{username}", response_class=HTMLResponse)
async def dashboard(request: Request, username: str):
    stocks = get_user_stock_summary(username, hours=24)
    tickers = get_user_tickers(username)
    all_articles = []
    for t in tickers:
        all_articles.extend(get_articles(t, hours=24))
    all_articles.sort(key=lambda a: a.get("published_at") or "", reverse=True)
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "username": username,
            "stocks": stocks,
            "default_stocks": DEFAULT_STOCKS,
            "articles": all_articles,
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
    ticker = ticker.strip().upper()
    if ticker:
        add_user_ticker(username, ticker)
        # Auto-fetch if no cached articles for this ticker
        if not get_articles(ticker, hours=24):
            await refresh_stock(ticker)
    return RedirectResponse(f"/{username}", status_code=303)


@app.post("/{username}/remove/{ticker}")
async def remove_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    remove_user_ticker(username, ticker)
    return RedirectResponse(f"/{username}", status_code=303)


@app.post("/api/refresh/{username}")
async def api_refresh_user(username: str):
    results = await refresh_user_stocks(username)
    total = sum(results.values())
    return JSONResponse({"status": "ok", "total_articles": total, "per_ticker": results})


@app.post("/api/refresh/{username}/{ticker}")
async def api_refresh_ticker(username: str, ticker: str):
    ticker = ticker.upper()
    user_tickers = get_user_tickers(username)
    if ticker not in user_tickers:
        return JSONResponse({"error": "Ticker not in watchlist"}, status_code=404)

    count = await refresh_stock(ticker)
    return JSONResponse({"status": "ok", "ticker": ticker, "articles_fetched": count})
