import os
from dotenv import load_dotenv

load_dotenv()

MASSIVE_API_KEY = os.getenv("MASSIVE_API_KEY", "")
MASSIVE_BASE_URL = "https://api.polygon.io"

DB_PATH = os.getenv("DB_PATH", "news_digest.db")

DEFAULT_STOCKS = {
    "AAPL": "Apple",
    "MSFT": "Microsoft",
    "GOOGL": "Alphabet",
    "AMZN": "Amazon",
    "NVDA": "NVIDIA",
    "META": "Meta Platforms",
    "TSLA": "Tesla",
    "JPM": "JPMorgan Chase",
    "V": "Visa",
    "JNJ": "Johnson & Johnson",
}

REFRESH_HOUR = 6  # Nightly refresh at 6 AM
ARTICLE_RETENTION_DAYS = 7
