from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from app.config import DB_PATH, DEFAULT_STOCKS, ARTICLE_RETENTION_DAYS


@contextmanager
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS stocks (
                ticker TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                title TEXT NOT NULL,
                summary TEXT,
                url TEXT NOT NULL UNIQUE,
                source TEXT,
                published_at TEXT,
                sentiment TEXT,
                sentiment_reasoning TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            );

            CREATE INDEX IF NOT EXISTS idx_articles_ticker ON articles(ticker);
            CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);

            CREATE TABLE IF NOT EXISTS refresh_log (
                ticker TEXT PRIMARY KEY,
                last_refresh TEXT NOT NULL,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
            );

            CREATE TABLE IF NOT EXISTS user_watchlists (
                username TEXT NOT NULL,
                ticker TEXT NOT NULL,
                added_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (username, ticker)
            );
        """)

        for ticker, name in DEFAULT_STOCKS.items():
            conn.execute(
                "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)",
                (ticker, name),
            )


def ensure_stock_exists(ticker: str):
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO stocks (ticker, name) VALUES (?, ?)",
            (ticker, ticker),
        )


def delete_articles_for_ticker(ticker: str):
    with get_db() as conn:
        conn.execute("DELETE FROM articles WHERE ticker = ?", (ticker,))


def upsert_articles(articles: list[dict]):
    with get_db() as conn:
        for a in articles:
            conn.execute(
                """INSERT OR IGNORE INTO articles
                   (ticker, title, summary, url, source, published_at, sentiment, sentiment_reasoning)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    a["ticker"],
                    a["title"],
                    a.get("summary"),
                    a["url"],
                    a.get("source"),
                    a.get("published_at"),
                    a.get("sentiment"),
                    a.get("sentiment_reasoning"),
                ),
            )


def get_articles(ticker: str, hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM articles
               WHERE ticker = ? AND published_at >= ?
               ORDER BY published_at DESC""",
            (ticker, since),
        ).fetchall()
        return [dict(r) for r in rows]


def get_all_recent_articles(hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM articles
               WHERE published_at >= ?
               ORDER BY published_at DESC""",
            (since,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_stock_summary(hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT s.ticker, s.name,
                      COUNT(a.id) as article_count,
                      SUM(CASE WHEN a.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
                      SUM(CASE WHEN a.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
                      SUM(CASE WHEN a.sentiment = 'neutral' OR a.sentiment IS NULL THEN 1 ELSE 0 END) as neutral
               FROM stocks s
               LEFT JOIN articles a ON s.ticker = a.ticker AND a.published_at >= ?
               GROUP BY s.ticker, s.name
               ORDER BY s.ticker""",
            (since,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_user_stock_summary(username: str, hours: int = 24) -> list[dict]:
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    with get_db() as conn:
        rows = conn.execute(
            """SELECT s.ticker, s.name,
                      COUNT(a.id) as article_count,
                      SUM(CASE WHEN a.sentiment = 'positive' THEN 1 ELSE 0 END) as positive,
                      SUM(CASE WHEN a.sentiment = 'negative' THEN 1 ELSE 0 END) as negative,
                      SUM(CASE WHEN a.sentiment = 'neutral' OR a.sentiment IS NULL THEN 1 ELSE 0 END) as neutral
               FROM user_watchlists w
               JOIN stocks s ON w.ticker = s.ticker
               LEFT JOIN articles a ON s.ticker = a.ticker AND a.published_at >= ?
               WHERE w.username = ?
               GROUP BY s.ticker, s.name
               ORDER BY s.ticker""",
            (since, username),
        ).fetchall()
        return [dict(r) for r in rows]


def get_user_tickers(username: str) -> list[str]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT ticker FROM user_watchlists WHERE username = ? ORDER BY ticker",
            (username,),
        ).fetchall()
        return [r["ticker"] for r in rows]


def add_user_ticker(username: str, ticker: str):
    ensure_stock_exists(ticker)
    with get_db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO user_watchlists (username, ticker) VALUES (?, ?)",
            (username, ticker),
        )


def remove_user_ticker(username: str, ticker: str):
    with get_db() as conn:
        conn.execute(
            "DELETE FROM user_watchlists WHERE username = ? AND ticker = ?",
            (username, ticker),
        )


def get_all_watched_tickers() -> list[str]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM user_watchlists ORDER BY ticker"
        ).fetchall()
        return [r["ticker"] for r in rows]


def get_last_refresh(ticker: str) -> str | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT last_refresh FROM refresh_log WHERE ticker = ?", (ticker,)
        ).fetchone()
        return row["last_refresh"] if row else None


def update_refresh_log(ticker: str):
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO refresh_log (ticker, last_refresh) VALUES (?, ?)
               ON CONFLICT(ticker) DO UPDATE SET last_refresh = ?""",
            (ticker, now, now),
        )


def purge_old_articles():
    cutoff = (
        datetime.now(timezone.utc) - timedelta(days=ARTICLE_RETENTION_DAYS)
    ).isoformat()
    with get_db() as conn:
        conn.execute("DELETE FROM articles WHERE published_at < ?", (cutoff,))


def get_stock_name(ticker: str) -> str | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT name FROM stocks WHERE ticker = ?", (ticker,)
        ).fetchone()
        return row["name"] if row else None
