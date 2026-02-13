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

            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticker TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_date TEXT NOT NULL,
                title TEXT NOT NULL,
                description TEXT,
                metadata TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                FOREIGN KEY (ticker) REFERENCES stocks(ticker),
                UNIQUE(ticker, event_type, event_date)
            );

            CREATE INDEX IF NOT EXISTS idx_events_ticker ON events(ticker);
            CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);

            CREATE TABLE IF NOT EXISTS known_stocks (
                ticker TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                exchange TEXT,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            );

            CREATE INDEX IF NOT EXISTS idx_known_stocks_name ON known_stocks(name COLLATE NOCASE);

            CREATE TABLE IF NOT EXISTS read_articles (
                username TEXT NOT NULL,
                article_id INTEGER NOT NULL,
                read_at TEXT NOT NULL DEFAULT (datetime('now')),
                PRIMARY KEY (username, article_id),
                FOREIGN KEY (article_id) REFERENCES articles(id)
            );

            CREATE TABLE IF NOT EXISTS events_refresh_log (
                ticker TEXT PRIMARY KEY,
                last_refresh TEXT NOT NULL,
                FOREIGN KEY (ticker) REFERENCES stocks(ticker)
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


def upsert_events(events: list[dict]):
    with get_db() as conn:
        for e in events:
            conn.execute(
                """INSERT OR REPLACE INTO events
                   (ticker, event_type, event_date, title, description, metadata)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    e["ticker"],
                    e["event_type"],
                    e["event_date"],
                    e["title"],
                    e.get("description"),
                    e.get("metadata"),
                ),
            )


def get_events_for_ticker(ticker: str) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM events
               WHERE ticker = ?
               ORDER BY event_date ASC""",
            (ticker,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_events_for_tickers(tickers: list[str], start_date: str | None = None, end_date: str | None = None) -> list[dict]:
    if not tickers:
        return []
    placeholders = ",".join("?" * len(tickers))
    query = f"SELECT * FROM events WHERE ticker IN ({placeholders})"
    params: list = list(tickers)
    if start_date:
        query += " AND event_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND event_date <= ?"
        params.append(end_date)
    query += " ORDER BY event_date ASC"
    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def delete_events_for_ticker(ticker: str):
    with get_db() as conn:
        conn.execute("DELETE FROM events WHERE ticker = ?", (ticker,))


def purge_old_events():
    cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    with get_db() as conn:
        conn.execute("DELETE FROM events WHERE event_date < ?", (cutoff,))


# --- Known stocks (autocomplete & normalization) ---

def upsert_known_stocks(stocks: list[dict]):
    """Bulk insert/update known stocks."""
    with get_db() as conn:
        for s in stocks:
            conn.execute(
                """INSERT INTO known_stocks (ticker, name, exchange, updated_at)
                   VALUES (?, ?, ?, datetime('now'))
                   ON CONFLICT(ticker) DO UPDATE SET
                       name = excluded.name,
                       exchange = excluded.exchange,
                       updated_at = datetime('now')""",
                (s["ticker"], s["name"], s.get("exchange")),
            )


def search_known_stocks(query: str, limit: int = 15) -> list[dict]:
    """Search known stocks by ticker or company name (case-insensitive).
    
    Results are ordered: exact ticker match → ticker prefix → name contains.
    """
    if not query or len(query) < 1:
        return []
    with get_db() as conn:
        rows = conn.execute(
            """SELECT ticker, name, exchange FROM (
                   SELECT ticker, name, exchange, 1 as priority FROM known_stocks
                   WHERE ticker = ? COLLATE NOCASE
                   UNION ALL
                   SELECT ticker, name, exchange, 2 as priority FROM known_stocks
                   WHERE ticker LIKE ? COLLATE NOCASE AND ticker != ? COLLATE NOCASE
                   UNION ALL
                   SELECT ticker, name, exchange, 3 as priority FROM known_stocks
                   WHERE name LIKE ? COLLATE NOCASE AND ticker NOT LIKE ? COLLATE NOCASE
               )
               ORDER BY priority, ticker
               LIMIT ?""",
            (query, f"{query}%", query, f"%{query}%", f"{query}%", limit),
        ).fetchall()
        return [dict(r) for r in rows]


def resolve_ticker(input_text: str) -> str | None:
    """Resolve user input to a valid ticker. Handles ticker or company name."""
    input_text = input_text.strip()
    if not input_text:
        return None
    with get_db() as conn:
        # Try exact ticker match
        row = conn.execute(
            "SELECT ticker FROM known_stocks WHERE ticker = ? COLLATE NOCASE",
            (input_text,),
        ).fetchone()
        if row:
            return row["ticker"]
        # Try exact name match
        row = conn.execute(
            "SELECT ticker FROM known_stocks WHERE name = ? COLLATE NOCASE",
            (input_text,),
        ).fetchone()
        if row:
            return row["ticker"]
        return None


def get_known_stock_count() -> int:
    with get_db() as conn:
        row = conn.execute("SELECT COUNT(*) as cnt FROM known_stocks").fetchone()
        return row["cnt"] if row else 0


def get_known_stock_name(ticker: str) -> str | None:
    """Get company name from known_stocks table."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT name FROM known_stocks WHERE ticker = ? COLLATE NOCASE",
            (ticker,),
        ).fetchone()
        return row["name"] if row else None


# --- Read/unread article tracking ---

def mark_article_read(username: str, article_id: int):
    """Mark an article as read for a user."""
    with get_db() as conn:
        conn.execute(
            """INSERT OR IGNORE INTO read_articles (username, article_id)
               VALUES (?, ?)""",
            (username, article_id),
        )


def mark_article_unread(username: str, article_id: int):
    """Mark an article as unread (remove read marker) for a user."""
    with get_db() as conn:
        conn.execute(
            "DELETE FROM read_articles WHERE username = ? AND article_id = ?",
            (username, article_id),
        )


def get_read_article_ids(username: str) -> set[int]:
    """Get all article IDs that a user has read."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT article_id FROM read_articles WHERE username = ?",
            (username,),
        ).fetchall()
        return {r["article_id"] for r in rows}


# --- Events refresh tracking ---

def update_events_refresh_log(ticker: str):
    """Record that events for a ticker were just refreshed."""
    now = datetime.now(timezone.utc).isoformat()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO events_refresh_log (ticker, last_refresh) VALUES (?, ?)
               ON CONFLICT(ticker) DO UPDATE SET last_refresh = ?""",
            (ticker, now, now),
        )


def get_user_refresh_timestamps(username: str) -> dict[str, dict]:
    """Get news and events refresh timestamps for all of a user's tickers."""
    tickers = get_user_tickers(username)
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    result: dict[str, dict] = {t: {"news_refresh": None, "events_refresh": None} for t in tickers}
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT ticker, last_refresh FROM refresh_log WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchall()
        for r in rows:
            result[r["ticker"]]["news_refresh"] = r["last_refresh"]
        rows = conn.execute(
            f"SELECT ticker, last_refresh FROM events_refresh_log WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchall()
        for r in rows:
            result[r["ticker"]]["events_refresh"] = r["last_refresh"]
    return result


def get_stale_tickers(username: str, stale_minutes: int, log_table: str = "refresh_log") -> list[str]:
    """Return user's tickers whose last refresh is older than stale_minutes (or never refreshed).

    Results are sorted most-stale first: never-refreshed tickers come first,
    then by oldest last_refresh ascending.
    """
    assert log_table in ("refresh_log", "events_refresh_log"), "invalid table"
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=stale_minutes)).isoformat()
    tickers = get_user_tickers(username)
    if not tickers:
        return []
    placeholders = ",".join("?" * len(tickers))
    with get_db() as conn:
        # Get refresh timestamps for all tickers
        rows = conn.execute(
            f"SELECT ticker, last_refresh FROM {log_table} WHERE ticker IN ({placeholders})",
            tickers,
        ).fetchall()
        refresh_map = {r["ticker"]: r["last_refresh"] for r in rows}

    # Filter to stale only (refresh older than cutoff, or never refreshed)
    stale = [t for t in tickers if refresh_map.get(t, "") <= cutoff]

    # Sort: never-refreshed first, then oldest refresh first
    stale.sort(key=lambda t: refresh_map.get(t, ""))
    return stale
