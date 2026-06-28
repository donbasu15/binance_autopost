"""
Binance Square Auto-Poster
==========================
Generates 40-50 varied short crypto posts per day using Gemini API
and publishes them to Binance Square with irregular human-like intervals.

Requirements:
    pip install google-genai requests python-dotenv Flask gunicorn

Setup:
    Create a .env file with:
        GEMINI_API_KEY=your_gemini_api_key
        BINANCE_SQUARE_KEY=your_binance_square_openapi_key

Get your Binance Square API key at:
    https://www.binance.com/square/creator-center/home
"""

import os
import time
import random
import logging
from logging.handlers import RotatingFileHandler
import requests
import json
import threading
import sys
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string, request

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
load_dotenv()

GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY")
GEMINI_API_KEYS_STR   = os.getenv("GEMINI_API_KEYS", "")

# Parse multiple keys; fall back to GEMINI_API_KEY, GEMINI_API_KEY_2, GEMINI_API_KEY_3 etc.
GEMINI_API_KEYS = [k.strip() for k in GEMINI_API_KEYS_STR.split(",") if k.strip()]
if not GEMINI_API_KEYS:
    for suffix in ["", "_2", "_3"]:
        key = os.getenv(f"GEMINI_API_KEY{suffix}")
        if key:
            GEMINI_API_KEYS.append(key.strip())

BINANCE_SQUARE_KEY    = os.getenv("BINANCE_SQUARE_KEY")
STATE_SERVER_URL      = os.getenv("STATE_SERVER_URL")
STATE_ID              = os.getenv("STATE_ID")
BINANCE_POST_URL      = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
BINANCE_UPLOAD_URL    = "https://www.binance.com/bapi/composite/v2/public/pgc/openApi/image/presignedUrl"
BINANCE_STATUS_URL    = "https://www.binance.com/bapi/composite/v2/public/pgc/openApi/image/imageStatus"
BINANCE_KLINES_URL    = "https://api.binance.com/api/v3/klines"
BINANCE_FUTURES_OI    = "https://fapi.binance.com/fapi/v1/openInterest"
BINANCE_FUNDING_URL   = "https://fapi.binance.com/fapi/v1/fundingRate"
COINGECKO_URL         = "https://api.coingecko.com/api/v3"
FEAR_GREED_URL        = "https://api.alternative.me/fng/"

BINANCE_POST_ENDPOINT = BINANCE_POST_URL

DATA_REFRESH_EVERY = 2

POSTS_PER_DAY_MIN = 40
POSTS_PER_DAY_MAX = 60

# Irregular interval ranges between posts (in seconds).
# Mimics human posting patterns: short bursts + longer gaps.
INTERVAL_BANDS = [
    (60,   300),    # 1–5 min  — "just replied and posted again"
    (300,  900),    # 5–15 min — quick follow-up
    (900,  2700),   # 15–45 min — normal browsing gap
    (2700, 5400),   # 45–90 min — stepped away
    (5400, 10800),  # 90 min–3 hr — long break
]

# Weights: short gaps are common, very long gaps are rare
INTERVAL_WEIGHTS = [15, 30, 30, 15, 10]

# ─────────────────────────────────────────────
# THREAD-SAFE STATE & LOGGING
# ─────────────────────────────────────────────
state_lock = threading.Lock()
last_log_messages = []

def get_ist_now():
    return datetime.utcnow() + timedelta(hours=5, minutes=30)

bot_state = {
    "status": "Starting up",
    "start_time": get_ist_now().strftime("%Y-%m-%d %I:%M:%S %p IST"),
    "posts_published": 0,
    "posts_failed": 0,
    "n_posts_scheduled": 0,
    "schedule": [],  # list of {"time": str, "status": str, "coin": str, "type": str}
    "recent_posts": [],  # list of {"time": str, "content": str, "status": str, "url": str}
    "last_log_messages": last_log_messages,
    "recent_coins": [],
    "recent_types": [],
    "is_running": False,
    "error_message": None
}

STATE_FILE = "bot_state.json"
IDS_CACHE_FILE = ".state_id.json"

class StateServerManager:
    def __init__(self, server_url: str, state_id: str):
        # Normalize server url to end with /v1/json/
        if server_url:
            self.server_url = server_url.rstrip("/")
            if not self.server_url.endswith("/v1/json"):
                self.server_url = self.server_url + "/v1/json/"
            else:
                self.server_url = self.server_url + "/"
        else:
            self.server_url = None
        self.state_id = state_id

    def get_url(self, item_id: str) -> str:
        return f"{self.server_url}{item_id}"

    def initialize(self):
        if not self.server_url:
            log.warning("No STATE_SERVER_URL configured. State server features are disabled.")
            return False

        # Load ID from local cache if not set in env
        cache = {}
        if os.path.exists(IDS_CACHE_FILE) and os.path.getsize(IDS_CACHE_FILE) > 0:
            try:
                import json
                with open(IDS_CACHE_FILE, "r", encoding="utf-8") as f:
                    cache = json.load(f)
            except Exception:
                pass

        if not self.state_id:
            self.state_id = cache.get("state_id")

        try:
            # 1. Initialize bot_state storage
            if not self.state_id:
                log.info("Creating new bot_state item on custom State Server...")
                initial_state = {}
                if os.path.exists(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
                    try:
                        import json
                        with open(STATE_FILE, "r", encoding="utf-8") as f:
                            initial_state = json.load(f)
                    except Exception:
                        pass
                self.state_id = self.create_data(initial_state)
                if self.state_id:
                    log.warning(f"⚠️ Created bot_state item on State Server. ID: {self.state_id}")
            
            # Save generated ID to local cache
            if self.state_id:
                try:
                    import json
                    with open(IDS_CACHE_FILE, "w", encoding="utf-8") as f:
                        json.dump({"state_id": self.state_id}, f, indent=2)
                except Exception:
                    pass

            if self.state_id:
                log.info(f"State Server initialized. State ID: {self.state_id}")
                return True
            return False

        except Exception as e:
            log.error(f"Failed to initialize State Server: {e}.")
            return False

    def load_data(self, item_id: str):
        if not item_id or not self.server_url:
            return None
        try:
            url = self.get_url(item_id)
            res = requests.get(url, timeout=10)
            res.raise_for_status()
            return res.json()
        except Exception as e:
            log.error(f"Error reading item {item_id} from State Server: {e}")
            return None

    def save_data(self, item_id: str, data):
        if not item_id or not self.server_url:
            return False
        try:
            url = self.get_url(item_id)
            headers = {"Content-Type": "application/json; charset=utf-8"}
            res = requests.put(url, headers=headers, json=data, timeout=10)
            res.raise_for_status()
            return True
        except Exception as e:
            log.error(f"Error writing to item {item_id} on State Server: {e}")
            return False

    def create_data(self, data):
        if not self.server_url:
            return None
        try:
            url = self.server_url.rstrip("/")
            headers = {"Content-Type": "application/json; charset=utf-8"}
            res = requests.post(url, headers=headers, json=data, timeout=10)
            res.raise_for_status()
            uri = res.json().get("uri", "")
            item_id = uri.replace(self.server_url, "")
            return item_id
        except Exception as e:
            log.error(f"Error creating new item on State Server: {e}")
            return None

state_server_manager = StateServerManager(STATE_SERVER_URL, STATE_ID)

def save_bot_state():
    with state_lock:
        state_to_save = {
            "start_time": bot_state["start_time"],
            "posts_published": bot_state["posts_published"],
            "posts_failed": bot_state["posts_failed"],
            "n_posts_scheduled": bot_state["n_posts_scheduled"],
            "schedule": bot_state["schedule"],
            "recent_posts": bot_state["recent_posts"],
            "recent_coins": bot_state["recent_coins"],
            "recent_types": bot_state["recent_types"],
            "error_message": bot_state["error_message"],
            "status": bot_state["status"]
        }
    try:
        import json
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state_to_save, f, indent=2)
    except Exception as e:
        log.error(f"Failed to save bot state locally: {e}")

    if state_server_manager.state_id:
        def push_state():
            state_server_manager.save_data(state_server_manager.state_id, state_to_save)
        threading.Thread(target=push_state, daemon=True).start()

def load_bot_state():
    global bot_state
    
    if state_server_manager.initialize():
        log.info("Syncing state from State Server to local files...")
        
        # Sync bot state
        cloud_state = state_server_manager.load_data(state_server_manager.state_id)
        if cloud_state:
            try:
                import json
                with open(STATE_FILE, "w", encoding="utf-8") as f:
                    json.dump(cloud_state, f, indent=2)
                log.info("Synced bot state from cloud to local bot_state.json")
            except Exception as e:
                log.error(f"Failed to write downloaded bot state locally: {e}")

    if os.path.exists(STATE_FILE) and os.path.getsize(STATE_FILE) > 0:
        try:
            import json
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                saved_state = json.load(f)
            with state_lock:
                for k, v in saved_state.items():
                    if k in bot_state:
                        bot_state[k] = v
            log.info("Successfully loaded bot state from persistent file.")
        except Exception as e:
            log.error(f"Failed to load bot state: {e}")

class MemoryLogHandler(logging.Handler):
    def __init__(self, target_list, max_items=50):
        super().__init__()
        self.target_list = target_list
        self.max_items = max_items

    def emit(self, record):
        try:
            log_entry = self.format(record)
            with state_lock:
                self.target_list.append(log_entry)
                if len(self.target_list) > self.max_items:
                    self.target_list.pop(0)
        except Exception:
            self.handleError(record)

# Configure log rotation (max 5MB per file, keeping 3 backups)
rotating_handler = RotatingFileHandler(
    "autoposter.log", 
    maxBytes=5 * 1024 * 1024, 
    backupCount=3,
    encoding="utf-8"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        rotating_handler,
        logging.StreamHandler()
    ]
)

# Silence the extremely noisy Flask (werkzeug) and HTTP client (httpx) polling logs
logging.getLogger('werkzeug').setLevel(logging.WARNING)
logging.getLogger('httpx').setLevel(logging.WARNING)

log = logging.getLogger(__name__)

# Add MemoryLogHandler to display console logs in real time on the web UI
memory_handler = MemoryLogHandler(last_log_messages)
memory_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(memory_handler)


# ─────────────────────────────────────────────
# CRYPTOPANIC NEWS FETCHING
# ─────────────────────────────────────────────

def fetch_cryptopanic_news() -> list[dict]:
    """Fetches cryptopanic news directly from GitHub JSON endpoint."""
    url = "https://raw.githubusercontent.com/donbasu15/news_bot/refs/heads/main/cryptopanic_news.json"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            # Save a local cache copy as backup
            try:
                with open("newsdata.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
            except Exception as le:
                log.warning(f"Failed to save cryptopanic news cache: {le}")
            return data
    except Exception as e:
        log.error(f"Failed to fetch cryptopanic news from GitHub: {e}")
    
    # Fallback to local cache if GitHub is down/rate-limited
    try:
        if os.path.exists("newsdata.json") and os.path.getsize("newsdata.json") > 0:
            with open("newsdata.json", "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        log.error(f"Failed to read cached newsdata.json: {e}")
    return []


load_bot_state()

class GeminiClientRotator:
    """Manages rotation and failover between multiple Gemini API keys."""
    def __init__(self, api_keys: list[str]):
        self.api_keys = api_keys
        self.current_index = 0
        self.clients = [genai.Client(api_key=key) for key in api_keys]

    def get_client(self) -> genai.Client:
        if not self.clients:
            raise ValueError("No Gemini API keys configured.")
        return self.clients[self.current_index]

    def rotate(self):
        if len(self.clients) > 1:
            self.current_index = (self.current_index + 1) % len(self.clients)
            masked = f"...{self.api_keys[self.current_index][-6:]}" if len(self.api_keys[self.current_index]) > 6 else "..."
            log.info(f"🔄 Switched to Gemini API key index {self.current_index} (Key ending in {masked})")

    def has_multiple_keys(self) -> bool:
        return len(self.clients) > 1

POST_TYPES = [

    {
        "name": "price_target",
        "description": (
            "Bold price target with a price ladder. Ground it in the LIVE DATA above "
            "— reference the actual current price and use realistic next targets. "
            "Do not invent numbers not supported by the data."
        ),
        "example": (
            "$ETH sitting at $3,200 right now.\n"
            "Based on current momentum: 3200 ➡️ 4500 ➡️ 6000 ➡️ 8000\n"
            "24h vol is $18B. Institutions are still buying.\n"
            "DYOR"
        )
    },
    {
        "name": "entry_signal",
        "description": (
            "Entry zone post with SL and TP targets. Use the LIVE DATA for the "
            "current price to set a realistic entry zone just below it, "
            "SL ~5-8% below entry, TPs at logical resistances above."
        ),
        "example": (
            "$SOL\n"
            "Position: Long\n"
            "Entry Zone: 148 - 155\n"
            "SL: 138\n"
            "Targets:\n"
            "TP1: 170\n"
            "TP2: 195\n"
            "TP3: 220"
        )
    },
    {
        "name": "dip_entry",
        "description": (
            "Short punchy post about entering on a dip. Use the 24h low from "
            "LIVE DATA to anchor the dip level. Casual tone, 1-2 emojis max."
        ),
        "example": (
            "$BNB testing the 600 zone after that -4% candle 🙃\n"
            "I'm loading here. 24h vol still at $2.3B — not dead.\n"
            "S/L at 565, watching for a bounce."
        )
    },
    {
        "name": "bearish_warning",
        "description": (
            "Bearish short take. Use the 7d change or current price structure "
            "from LIVE DATA to justify the bearish view. No links, no signals."
        ),
        "example": (
            "$XRP down 8% this week and volume is drying up.\n"
            "Bearish structure forming. 0.75 is the next level to watch.\n"
            "Don't chase pumps. Patience."
        )
    },
    {
        "name": "news_reaction",
        "description": (
            "React to ONE of the real news headlines from LIVE DATA above. "
            "Pick the most interesting one. Write your genuine take on it. "
            "Do NOT include any URLs. Reference the headline topic, not the source name."
        ),
        "example": (
            "Senate just advanced the stablecoin bill. 15-9 vote.\n"
            "$BTC barely moved but this changes everything long term.\n"
            "Regulatory clarity = institutional green light.\n"
            "Watch Q3."
        )
    },
    {
        "name": "fear_greed_take",
        "description": (
            "Short take based on the Fear & Greed index from LIVE DATA. "
            "Explain what it means right now and what historically happens next. "
            "Use the actual index value in the post."
        ),
        "example": (
            "Fear & Greed index just hit 82 — Extreme Greed.\n"
            "Last time we saw this: $BTC was at its cycle peak.\n"
            "Not saying sell. Just saying be careful up here.\n"
            "Greed kills portfolios."
        )
    },
    {
        "name": "trending_coin_take",
        "description": (
            "Short take on one of the currently trending coins from LIVE DATA. "
            "Use the actual price data for that coin if available. "
            "Keep it punchy — why is it trending and what does it mean?"
        ),
        "example": (
            "$PEPE is trending again.\n"
            "Up 22% in 7 days while everything else bleeds.\n"
            "No fundamentals needed when the memes are this strong 🐸\n"
            "Watch the volume — this one moves fast."
        )
    },
    {
        "name": "community_hold",
        "description": (
            "Encouraging hold post for long-term believers. "
            "Reference the current price and volume from LIVE DATA to reinforce confidence."
        ),
        "example": (
            "Still holding $LINK.\n"
            "Volume up 40% this week. Oracle dominance untouched.\n"
            "The quiet ones always run when the market wakes up.\n"
            "Patience is the trade."
        )
    },
    {
        "name": "dark_humor_take",
        "description": (
            "Funny/sarcastic take on the market or a specific coin. "
            "Use actual price or % change data from LIVE DATA to make it feel real."
        ),
        "example": (
            "$AVAX down 12% this week and everyone's still waiting for $100.\n"
            "The hopium is real 😭\n"
            "Meanwhile I'll be watching my $BTC quietly doing its thing."
        )
    },
]

# ─────────────────────────────────────────────
# COINS POOL — Maps cashtag → CoinGecko ID → Symbol
# ─────────────────────────────────────────────
COINS = [
    {"tag": "$BTC",   "cg_id": "bitcoin",        "symbol": "BTC"},
    {"tag": "$ETH",   "cg_id": "ethereum",        "symbol": "ETH"},
    {"tag": "$BNB",   "cg_id": "binancecoin",     "symbol": "BNB"},
    {"tag": "$SOL",   "cg_id": "solana",          "symbol": "SOL"},
    {"tag": "$XRP",   "cg_id": "ripple",          "symbol": "XRP"},
    {"tag": "$AVAX",  "cg_id": "avalanche-2",     "symbol": "AVAX"},
    {"tag": "$LINK",  "cg_id": "chainlink",       "symbol": "LINK"},
    {"tag": "$ARB",   "cg_id": "arbitrum",        "symbol": "ARB"},
    {"tag": "$OP",    "cg_id": "optimism",        "symbol": "OP"},
    {"tag": "$MATIC", "cg_id": "matic-network",   "symbol": "MATIC"},
    {"tag": "$DOGE",  "cg_id": "dogecoin",        "symbol": "DOGE"},
    {"tag": "$DOT",   "cg_id": "polkadot",        "symbol": "DOT"},
    {"tag": "$ADA",   "cg_id": "cardano",         "symbol": "ADA"},
    {"tag": "$SUI",   "cg_id": "sui",             "symbol": "SUI"},
    {"tag": "$APT",   "cg_id": "aptos",           "symbol": "APT"},
    {"tag": "$INJ",   "cg_id": "injective-protocol", "symbol": "INJ"},
    {"tag": "$TIA",   "cg_id": "celestia",        "symbol": "TIA"},
    {"tag": "$JUP",   "cg_id": "jupiter-exchange-solana", "symbol": "JUP"},
    {"tag": "$WIF",   "cg_id": "dogwifcoin",      "symbol": "WIF"},
    {"tag": "$PEPE",  "cg_id": "pepe",            "symbol": "PEPE"},
    {"tag": "$NEAR",  "cg_id": "near",            "symbol": "NEAR"},
    {"tag": "$FTM",   "cg_id": "fantom",          "symbol": "FTM"},
    {"tag": "$ATOM",  "cg_id": "cosmos",          "symbol": "ATOM"},
]

HASHTAG_POOL = [
    "#crypto", "#BinanceSquare",
    "#Bitcoin", "#Ethereum", "#DeFi", "#Altcoins",
    "#CryptoTrading", "#BullRun", "#DYOR",
    "#cryptonews", "#Web3", "#blockchain",
    "#BTC", "#ETH", "#BNB", "#SOL",
    "#CryptoSignals", "#TechnicalAnalysis",
    "#CryptoInvesting", "#hodl", "#cryptomarket",
]

# ─────────────────────────────────────────────
# LIVE DATA FETCHER
# ─────────────────────────────────────────────

class LiveDataFetcher:
    """Fetches and caches live market data from free APIs."""

    def __init__(self):
        self._cache: dict = {}
        self._cache_time: dict = {}
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    def _get(self, url: str, params: dict = None, timeout: int = 10) -> dict | list | None:
        try:
            r = self.session.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"Fetch failed [{url}]: {e}")
            return None

    # ── CoinGecko: bulk market data for all tracked coins ──
    def fetch_market_data(self) -> dict:
        """Returns dict keyed by CoinGecko ID with price/change/volume/mcap."""
        ids = ",".join(c["cg_id"] for c in COINS)
        data = self._get(
            f"{COINGECKO_URL}/coins/markets",
            params={
                "vs_currency": "usd",
                "ids": ids,
                "order": "market_cap_desc",
                "per_page": 50,
                "page": 1,
                "price_change_percentage": "24h,7d",
            }
        )
        if not data:
            return {}
        result = {}
        for coin in data:
            result[coin["id"]] = {
                "price":      coin.get("current_price"),
                "change_24h": coin.get("price_change_percentage_24h"),
                "change_7d":  coin.get("price_change_percentage_7d_in_currency"),
                "volume":     coin.get("total_volume"),
                "mcap":       coin.get("market_cap"),
                "high_24h":   coin.get("high_24h"),
                "low_24h":    coin.get("low_24h"),
                "symbol":     coin.get("symbol", "").upper(),
            }
        log.info(f"  📊 Market data refreshed for {len(result)} coins")
        return result

    # ── CoinGecko: trending coins (no key needed) ──
    def fetch_trending(self) -> list[str]:
        data = self._get(f"{COINGECKO_URL}/search/trending")
        if not data:
            return []
        coins = data.get("coins", [])[:7]
        return [c["item"]["symbol"].upper() for c in coins]

    # ── cryptocurrency.cv: latest news headlines (no key needed) ──
    def fetch_news_headlines(self, limit: int = 15) -> list[str]:
        # Using the existing cryptonews URL since it's the specific news source
        data = self._get("https://cryptocurrency.cv/api/news", params={"limit": limit})
        if not data or "articles" not in data:
            return []
        headlines = []
        for a in data["articles"]:
            title = a.get("title", "").strip()
            if title:
                headlines.append(title)
        log.info(f"  📰 Fetched {len(headlines)} news headlines")
        return headlines

    # ── alternative.me: Fear & Greed Index ──
    def fetch_fear_greed(self) -> dict:
        data = self._get(FEAR_GREED_URL)
        if not data or "data" not in data or not data["data"]:
            return {"value": "N/A", "classification": "Unknown"}
        curr = data["data"][0]
        return {
            "value":          curr.get("value", "N/A"),
            "classification": curr.get("value_classification", "Unknown"),
        }

    # ── Binance Spot: Klines ──
    def fetch_klines(self, symbol: str, interval: str = "4h", limit: int = 100) -> list | None:
        """Fetches spot klines for a given symbol."""
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"
        return self._get(BINANCE_KLINES_URL, params={"symbol": symbol, "interval": interval, "limit": limit})

    # ── Binance Futures: Open Interest ──
    def fetch_futures_oi(self, symbol: str) -> dict | None:
        """Fetches current open interest for a futures symbol."""
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"
        return self._get(BINANCE_FUTURES_OI, params={"symbol": symbol})

    # ── Binance Futures: Funding Rate ──
    def fetch_futures_funding(self, symbol: str) -> list | None:
        """Fetches historical/current funding rate for a futures symbol."""
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"
        return self._get(BINANCE_FUNDING_URL, params={"symbol": symbol, "limit": 1})

    # ── Master refresh: called every N posts ──
    def refresh_all(self) -> dict:
        log.info("🔄 Refreshing live market data...")
        time.sleep(1)  # polite delay for free APIs
        return {
            "market":   self.fetch_market_data(),
            "trending": self.fetch_trending(),
            "news":     self.fetch_news_headlines(),
            "fg":       self.fetch_fear_greed(),
            "fetched_at": get_ist_now().strftime("%I:%M %p IST"),
        }


def calculate_indicators(klines_data: list) -> dict:
    """Calculates RSI, MACD, Bollinger Bands, EMAs, Support, and Resistance from klines."""
    if not klines_data or len(klines_data) < 20:
        return {}
    
    try:
        # Klines format: [open_time, open, high, low, close, volume, ...]
        closes = [float(k[4]) for k in klines_data]
        highs = [float(k[2]) for k in klines_data]
        lows = [float(k[3]) for k in klines_data]
        
        import pandas as pd
        df = pd.DataFrame({"close": closes, "high": highs, "low": lows})
        
        # EMAs
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        
        # Bollinger Bands
        df["bb_mid"] = df["close"].rolling(20).mean()
        df["bb_std"] = df["close"].rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]
        
        # RSI 14
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        # Handle divide by zero
        loss = loss.replace(0, 1e-9)
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        # MACD
        exp1 = df["close"].ewm(span=12, adjust=False).mean()
        exp2 = df["close"].ewm(span=26, adjust=False).mean()
        df["macd"] = exp1 - exp2
        df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
        df["macd_hist"] = df["macd"] - df["macd_signal"]
        
        # Support / Resistance (local min/max over last 50 candles)
        support = float(df["low"].iloc[-50:].min())
        resistance = float(df["high"].iloc[-50:].max())
        
        last = df.iloc[-1]
        prev_macd_hist = df["macd_hist"].iloc[-2] if len(df) > 1 else 0
        
        # Determine classification text
        rsi_val = last["rsi"]
        if rsi_val is None or pd.isna(rsi_val):
            rsi_desc = "N/A"
        elif rsi_val >= 70:
            rsi_desc = f"{rsi_val:.2f} (Overbought)"
        elif rsi_val <= 30:
            rsi_desc = f"{rsi_val:.2f} (Oversold)"
        else:
            rsi_desc = f"{rsi_val:.2f} (Neutral)"
            
        macd_val = last["macd"]
        macd_sig = last["macd_signal"]
        macd_hist = last["macd_hist"]
        if pd.isna(macd_val) or pd.isna(macd_sig):
            macd_desc = "N/A"
        elif macd_hist > 0 and prev_macd_hist <= 0:
            macd_desc = f"Line={macd_val:.2f}, Signal={macd_sig:.2f} (Bullish Crossover)"
        elif macd_hist < 0 and prev_macd_hist >= 0:
            macd_desc = f"Line={macd_val:.2f}, Signal={macd_sig:.2f} (Bearish Crossover)"
        else:
            macd_desc = f"Line={macd_val:.2f}, Signal={macd_sig:.2f}, Hist={macd_hist:.2f}"
            
        bb_upper = last["bb_upper"]
        bb_lower = last["bb_lower"]
        bb_mid = last["bb_mid"]
        close_val = last["close"]
        if pd.isna(bb_upper) or pd.isna(bb_lower):
            bb_desc = "N/A"
        elif close_val >= bb_upper:
            bb_desc = f"Upper={bb_upper:.2f}, Lower={bb_lower:.2f} (Price above Upper Band - Overextended)"
        elif close_val <= bb_lower:
            bb_desc = f"Upper={bb_upper:.2f}, Lower={bb_lower:.2f} (Price below Lower Band - Oversold)"
        else:
            bb_desc = f"Upper={bb_upper:.2f}, Mid={bb_mid:.2f}, Lower={bb_lower:.2f} (Neutral zone)"
            
        ema20 = last["ema20"]
        ema50 = last["ema50"]
        if pd.isna(ema20) or pd.isna(ema50):
            ema_desc = "N/A"
        elif close_val > ema20 and ema20 > ema50:
            ema_desc = f"Price is above EMA20 (${ema20:.2f}) and EMA50 (${ema50:.2f}) -> Bullish Trend"
        elif close_val < ema20 and ema20 < ema50:
            ema_desc = f"Price is below EMA20 (${ema20:.2f}) and EMA50 (${ema50:.2f}) -> Bearish Trend"
        else:
            ema_desc = f"Price = ${close_val:.2f}, EMA20 = ${ema20:.2f}, EMA50 = ${ema50:.2f} -> Consolidating"
            
        return {
            "rsi": float(rsi_val) if not pd.isna(rsi_val) else None,
            "rsi_desc": rsi_desc,
            "macd": float(macd_val) if not pd.isna(macd_val) else None,
            "macd_signal": float(macd_sig) if not pd.isna(macd_sig) else None,
            "macd_hist": float(macd_hist) if not pd.isna(macd_hist) else None,
            "macd_desc": macd_desc,
            "bb_upper": float(bb_upper) if not pd.isna(bb_upper) else None,
            "bb_lower": float(bb_lower) if not pd.isna(bb_lower) else None,
            "bb_mid": float(bb_mid) if not pd.isna(bb_mid) else None,
            "bb_desc": bb_desc,
            "ema20": float(ema20) if not pd.isna(ema20) else None,
            "ema50": float(ema50) if not pd.isna(ema50) else None,
            "ema_desc": ema_desc,
            "support": float(support),
            "resistance": float(resistance)
        }
    except Exception as e:
        log.warning(f"Error calculating indicators: {e}")
        return {}


# ─────────────────────────────────────────────
# DATA FORMATTER → turns raw data into a
# compact text block injected into Gemini prompt
# ─────────────────────────────────────────────

def format_coin_data(coin: dict, market: dict, fetcher: LiveDataFetcher,
                     global_data: dict) -> str:
    """Returns a compact real-time data block for the selected coin."""
    cg_id  = coin["cg_id"]
    tag    = coin["tag"]
    symbol = coin["symbol"]

    # Fetch extra coin-specific data (klines and derivatives)
    klines = fetcher.fetch_klines(symbol)
    oi_data = fetcher.fetch_futures_oi(symbol)
    funding_data = fetcher.fetch_futures_funding(symbol)

    lines = [f"=== LIVE MARKET DATA (as of {global_data.get('fetched_at', 'now')}) ==="]

    # Price data
    m = global_data["market"].get(cg_id, {})
    
    # Robust fallback to Binance Klines if CoinGecko is empty or rate-limited
    if not m and klines:
        try:
            price = float(klines[-1][4])
            ch24 = 0.0
            if len(klines) >= 6: # 6 * 4h candles = 24h
                prev_price = float(klines[-6][4])
                if prev_price > 0:
                    ch24 = ((price - prev_price) / prev_price) * 100
            
            h24 = max(float(k[2]) for k in klines[-6:]) if len(klines) >= 6 else price
            l24 = min(float(k[3]) for k in klines[-6:]) if len(klines) >= 6 else price
            vol = sum(float(k[5]) for k in klines[-6:]) * price if len(klines) >= 6 else 0.0
            
            m = {
                "price": price,
                "change_24h": ch24,
                "change_7d": 0.0,
                "volume": vol,
                "mcap": 0.0,
                "high_24h": h24,
                "low_24h": l24,
                "symbol": symbol
            }
        except Exception as e:
            log.warning(f"Error in price fallback calculation for {symbol}: {e}")

    if m:
        price    = m.get("price")
        ch24     = m.get("change_24h")
        ch7d     = m.get("change_7d")
        vol      = m.get("volume")
        mcap     = m.get("mcap")
        h24      = m.get("high_24h")
        l24      = m.get("low_24h")

        def fmt_price(p):
            if p is None: return "N/A"
            if p >= 1:    return f"${p:,.2f}"
            return f"${p:.6f}"

        def fmt_large(n):
            if n is None or n == 0.0: return "N/A"
            if n >= 1e9: return f"${n/1e9:.2f}B"
            if n >= 1e6: return f"${n/1e6:.1f}M"
            return f"${n:,.0f}"

        def fmt_pct(p):
            if p is None: return "N/A"
            sign = "+" if p > 0 else ""
            return f"{sign}{p:.2f}%"

        lines.append(f"Coin: {tag}")
        lines.append(f"Price: {fmt_price(price)}")
        lines.append(f"24h Change: {fmt_pct(ch24)}")
        lines.append(f"7d Change: {fmt_pct(ch7d)}")
        lines.append(f"24h High: {fmt_price(h24)}  |  24h Low: {fmt_price(l24)}")
        lines.append(f"Volume (24h): {fmt_large(vol)}")
        if mcap and mcap > 0:
            lines.append(f"Market Cap: {fmt_large(mcap)}")
    else:
        lines.append(f"Coin: {tag}  |  Price data unavailable")

    # Technical analysis indicators
    if klines:
        indicators = calculate_indicators(klines)
        if indicators:
            lines.append("\n--- TECHNICAL INDICATORS ---")
            lines.append(f"RSI (14): {indicators['rsi_desc']}")
            lines.append(f"MACD: {indicators['macd_desc']}")
            lines.append(f"Bollinger Bands: {indicators['bb_desc']}")
            lines.append(f"EMA Trend: {indicators['ema_desc']}")
            lines.append(f"Support: ${indicators['support']:,.2f}  |  Resistance: ${indicators['resistance']:,.2f}")

    # Futures / Derivatives data
    futures_lines = []
    if oi_data:
        oi_val = oi_data.get("openInterest")
        if oi_val:
            try:
                oi_num = float(oi_val)
                current_price = m.get("price") if m else None
                if not current_price and klines:
                    current_price = float(klines[-1][4])
                
                if current_price:
                    oi_usd = oi_num * current_price
                    if oi_usd >= 1e9:
                        oi_str = f"${oi_usd/1e9:.2f}B USD"
                    elif oi_usd >= 1e6:
                        oi_str = f"${oi_usd/1e6:.1f}M USD"
                    else:
                        oi_str = f"${oi_usd:,.0f} USD"
                    futures_lines.append(f"Futures Open Interest: {oi_num:,.2f} contracts ({oi_str})")
                else:
                    futures_lines.append(f"Futures Open Interest: {oi_num:,.2f} contracts")
            except Exception:
                futures_lines.append(f"Futures Open Interest: {oi_val} contracts")
                
    if funding_data and len(funding_data) > 0:
        fr = funding_data[0].get("fundingRate")
        if fr:
            try:
                fr_pct = float(fr) * 100
                futures_lines.append(f"Futures Funding Rate: {fr_pct:.4f}%")
            except Exception:
                futures_lines.append(f"Futures Funding Rate: {fr}")

    if futures_lines:
        lines.append("\n--- DERIVATIVES / FUTURES DATA ---")
        lines.extend(futures_lines)

    # Fear & Greed
    fg = global_data.get("fg", {})
    lines.append(f"\nMarket Sentiment: Fear & Greed = {fg.get('value', 'N/A')} "
                 f"({fg.get('classification', 'Unknown')})")

    # Trending
    trending = global_data.get("trending", [])
    if trending:
        lines.append(f"Currently Trending: {', '.join(trending[:5])}")

    # Load news from cryptopanic_news.json URL (directly from GitHub)
    local_news = []
    try:
        news_list = fetch_cryptopanic_news()
        if news_list and isinstance(news_list, list):
            for item in news_list:
                title = item.get("title")
                source = item.get("source", "unknown")
                sentiment = item.get("sentiment", "neutral")
                if title:
                    local_news.append(f"{title} (Source: {source}, Sentiment: {sentiment})")
    except Exception as e:
        log.warning(f"Failed to fetch cryptopanic news inside format_coin_data: {e}")

    # General news headlines
    news = local_news + global_data.get("news", [])
    if news:
        lines.append("\nTop Crypto Headlines Right Now:")
        for h in news[:10]:
            lines.append(f"  • {h}")

    lines.append("=" * 50)
    return "\n".join(lines)


# ─────────────────────────────────────────────
# GEMINI PROMPT BUILDER
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are a crypto KOL (Key Opinion Leader) posting on Binance Square.
Your posts must feel 100% human — written by a real trader with real data, not AI.
You follow real market sentiment and write with personality, confidence, and edge.

You will be given LIVE market data (prices, % changes, news headlines, Fear & Greed index).
USE THIS DATA to write the post. Do not use guessed or invented numbers.
If data says ETH is at $3,412 — use $3,412, not some random price.

STRICT RULES:
1. Each post MUST feel different from previous ones in tone, structure, coin, and format.
2. Keep posts SHORT: 40–120 words maximum. This is a short post platform.
3. Use cashtags ($BTC, $ETH etc.) naturally in the body — at least 1 per post.
4. Hashtags go at the very bottom only. Use 2–4 max.
5. NEVER use bullet formatting like "•" for entry/target posts — use line breaks only.
6. NO external URLs or website links of any kind.
7. NO social media handles (no @username, no Telegram, no Discord, no WhatsApp).
8. NO guaranteed returns or "100% sure" language.
9. NO financial advice disclaimers — short posts on Binance Square don't need them.
10. No data sources mentioned by name (don't say "CoinGecko says..." — just state the fact).
11. Do NOT repeat the same coin or format as recent posts.
12. Use emojis sparingly — 1–3 per post max, never mid-sentence.
13. Vary your sentence length. Mix punchy 3-word lines with longer ones.
14. Occasional typos or casual grammar are fine — makes it feel human.
15. NEVER start a post with "I think" or "In my opinion".
16. Output ONLY the post text. No preamble, no explanation, no quotes around it."""


def build_user_prompt(post_type: dict, coin: dict, live_data_block: str,
                      recent_coins: list, recent_types: list) -> str:
    recent_coins_str = ", ".join(recent_coins[-5:]) if recent_coins else "none"
    recent_types_str = ", ".join(recent_types[-3:]) if recent_types else "none"

    # Pick 2-4 hashtags randomly
    tags = random.sample(HASHTAG_POOL, random.randint(2, 4))
    tags_str = " ".join(tags)

    return f"""{live_data_block}

Write a Binance Square short post in this format: [{post_type['name']}]

FORMAT DESCRIPTION: {post_type['description']}

EXAMPLE (use as style guide ONLY, do NOT copy):
{post_type['example']}

PRIMARY COIN: {coin['tag']}
You may include 1-2 other related coins for context.

AVOID these coins (used recently): {recent_coins_str}
AVOID these post types (used recently): {recent_types_str}

End the post with these hashtags on a new line: {tags_str}

Write the post now. Output ONLY the post text, nothing else."""


# ─────────────────────────────────────────────
# GEMINI CONTENT GENERATOR
# ─────────────────────────────────────────────

def generate_post(client: genai.Client, post_type: dict, coin: dict,
                  live_data_block: str, recent_coins: list, recent_types: list) -> str:
    prompt = build_user_prompt(post_type, coin, live_data_block, recent_coins, recent_types)

    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=1.1,        # Higher = more creative/varied
            top_p=0.95,
            max_output_tokens=3000,
        )
    )
    return response.text.strip()


def generate_post_with_gemma(client: genai.Client, post_type: dict, coin: dict,
                             live_data_block: str, recent_coins: list, recent_types: list) -> str:
    """Fallback generator using gemma-4-26b-a4b-it if Gemini keys are exhausted."""
    prompt = build_user_prompt(post_type, coin, live_data_block, recent_coins, recent_types)
    response = client.models.generate_content(
        model="gemma-4-26b-a4b-it",
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=SYSTEM_PROMPT,
            temperature=1.1,
            top_p=0.95,
            max_output_tokens=3000,
        )
    )
    return response.text.strip()


def generate_advanced_chart(symbol: str, klines: list) -> bytes | None:
    """Generates an advanced dark-themed technical analysis chart using Matplotlib and returns image bytes."""
    if not klines or len(klines) < 20:
        log.warning(f"Not enough klines data ({len(klines) if klines else 0}) to generate chart for {symbol}")
        return None
    
    fig = None
    try:
        import pandas as pd
        import numpy as np
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        from io import BytesIO
        
        # Prepare dataframe
        df = pd.DataFrame(klines, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_asset_volume", "taker_buy_quote_asset_volume", "ignore"
        ])
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = pd.to_numeric(df[col])
        df["time"] = pd.to_datetime(df["open_time"], unit="ms")
        
        # Keep last 60 candles for neat plotting
        df = df.iloc[-60:].reset_index(drop=True)
        
        # Calculate indicators
        df["ema20"] = df["close"].ewm(span=20, adjust=False).mean()
        df["ema50"] = df["close"].ewm(span=50, adjust=False).mean()
        
        df["bb_mid"] = df["close"].rolling(20).mean()
        df["bb_std"] = df["close"].rolling(20).std()
        df["bb_upper"] = df["bb_mid"] + 2 * df["bb_std"]
        df["bb_lower"] = df["bb_mid"] - 2 * df["bb_std"]
        
        delta = df["close"].diff()
        gain = delta.where(delta > 0, 0).rolling(14).mean()
        loss = -delta.where(delta < 0, 0).rolling(14).mean()
        loss = loss.replace(0, 1e-9)
        rs = gain / loss
        df["rsi"] = 100 - (100 / (1 + rs))
        
        support = float(df["low"].min())
        resistance = float(df["high"].max())
        
        # Set up styles
        plt.style.use('dark_background')
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), gridspec_kw={'height_ratios': [2.5, 1]}, sharex=False)
        fig.patch.set_facecolor('#181A20')
        
        # Axis 1: Candlesticks
        ax1.set_facecolor('#181A20')
        up = df[df['close'] >= df['open']]
        down = df[df['close'] < df['open']]
        green = '#0ECB81'
        red = '#F6465D'
        
        ax1.bar(up.index, up['close'] - up['open'], bottom=up['open'], color=green, width=0.6)
        ax1.bar(down.index, down['open'] - down['close'], bottom=down['close'], color=red, width=0.6)
        ax1.vlines(up.index, up['low'], up['high'], color=green, linewidth=1)
        ax1.vlines(down.index, down['low'], down['high'], color=red, linewidth=1)
        
        # EMA
        ax1.plot(df.index, df['ema20'], color='#F0B90B', linewidth=1.2, label='EMA 20', linestyle='--')
        ax1.plot(df.index, df['ema50'], color='#4B9CD3', linewidth=1.2, label='EMA 50', linestyle='-.')
        
        # Bollinger Bands
        ax1.plot(df.index, df['bb_upper'], color='#8A9098', linewidth=0.8, alpha=0.4, linestyle=':')
        ax1.plot(df.index, df['bb_lower'], color='#8A9098', linewidth=0.8, alpha=0.4, linestyle=':')
        ax1.fill_between(df.index, df['bb_lower'], df['bb_upper'], color='#8A9098', alpha=0.03)
        
        # Support/Resistance lines
        ax1.axhline(support, color=red, alpha=0.3, linestyle='--', linewidth=1)
        ax1.text(0, support, f" Support: ${support:,.2f}", color=red, alpha=0.6, va='bottom', fontsize=8)
        
        ax1.axhline(resistance, color=green, alpha=0.3, linestyle='--', linewidth=1)
        ax1.text(0, resistance, f" Resistance: ${resistance:,.2f}", color=green, alpha=0.6, va='bottom', fontsize=8)
        
        symbol_usdt = symbol if symbol.endswith("USDT") else f"{symbol}USDT"
        ax1.set_title(f"{symbol_usdt} Technical Analysis (4h Chart)", color='#FCD535', fontsize=14, fontweight='bold', pad=15)
        ax1.set_ylabel("Price (USD)", color='#EAECEF', fontsize=10)
        ax1.tick_params(colors='#8A9098', labelsize=8)
        ax1.grid(True, color='#8A9098', alpha=0.1, linestyle=':')
        ax1.legend(loc='upper left', framealpha=0.1, labelcolor='#EAECEF', fontsize=9)
        
        for spine in ax1.spines.values():
            spine.set_color('#2B3139')
            
        # Axis 2: RSI
        ax2.set_facecolor('#181A20')
        ax2.plot(df.index, df['rsi'], color='#E8A317', linewidth=1.5, label='RSI (14)')
        ax2.axhline(70, color=red, alpha=0.3, linestyle=':', linewidth=1)
        ax2.axhline(30, color=green, alpha=0.3, linestyle=':', linewidth=1)
        ax2.fill_between(df.index, 30, 70, color='#E8A317', alpha=0.02)
        
        ax2.set_ylabel("RSI", color='#EAECEF', fontsize=10)
        ax2.set_ylim(10, 90)
        ax2.tick_params(colors='#8A9098', labelsize=8)
        ax2.grid(True, color='#8A9098', alpha=0.1, linestyle=':')
        for spine in ax2.spines.values():
            spine.set_color('#2B3139')
            
        # X Axis Formatting
        tick_indices = np.linspace(0, len(df)-1, 6, dtype=int)
        tick_labels = [df['time'].iloc[i].strftime('%m-%d %H:%M') for i in tick_indices]
        ax2.set_xticks(tick_indices)
        ax2.set_xticklabels(tick_labels, rotation=15, color='#8A9098')
        ax1.set_xticks([])
        
        fig.text(0.98, 0.02, "Powered by BiPass AutoPoster AI", color='#8A9098', 
                 fontsize=8, alpha=0.5, ha='right', va='bottom')
        
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format='png', dpi=120)
        buf.seek(0)
        return buf.getvalue()
    except Exception as e:
        log.warning(f"Failed to generate matplotlib chart for {symbol}: {e}")
        return None
    finally:
        if fig is not None:
            plt.close(fig)


def retrieve_search_image(rotator: GeminiClientRotator, symbol: str, topic: str) -> bytes | None:
    """Uses LLM with Google Search grounding to retrieve a public direct image URL for the coin/topic, validates and downloads it."""
    prompt = (
        f"Search the web using Google Search grounding. Find a valid, high-quality, public direct image URL "
        f"related to {symbol} (like a price chart, news visual, or logo) for the topic: '{topic}'. "
        f"The URL must end with a common image extension like .png, .jpg, or .jpeg. "
        f"Return ONLY the raw direct image URL on a single line, with absolutely no markdown formatting, quotes, or other text."
    )
    
    img_url = None
    failed_keys_count = 0
    max_attempts = len(rotator.clients)
    
    # Try Gemini clients first
    while failed_keys_count < max_attempts:
        try:
            client = rotator.get_client()
            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[{"google_search": {}}],
                    temperature=0.2
                )
            )
            text = response.text.strip()
            if text.startswith("http"):
                img_url = text
                log.info(f"   Image search URL found (Gemini): {img_url}")
                break
            else:
                failed_keys_count += 1
                rotator.rotate()
        except Exception as e:
            log.warning(f"   Gemini search failed on key index {rotator.current_index}: {e}")
            failed_keys_count += 1
            if failed_keys_count < max_attempts:
                rotator.rotate()
                time.sleep(1)
            else:
                break
                
    # If Gemini failed or is exhausted, try fallback model Gemma 4
    if not img_url:
        try:
            log.info("   All Gemini keys exhausted. Falling back to Gemma 4 for image search...")
            client = rotator.get_client()
            response = client.models.generate_content(
                model="gemma-4-26b-a4b-it",
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[{"google_search": {}}],
                    temperature=0.2
                )
            )
            text = response.text.strip()
            if text.startswith("http"):
                img_url = text
                log.info(f"   Image search URL found (Gemma 4): {img_url}")
        except Exception as e:
            log.warning(f"   Gemma 4 search failed: {e}")
            
    if not img_url:
        return None
        
    if " " in img_url:
        img_url = img_url.split()[0]
    img_url = img_url.strip("`*\"'")
    
    # Validate and download
    try:
        r = requests.head(img_url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        content_type = r.headers.get("content-type", "")
        if r.status_code == 200 and "image" in content_type:
            r_get = requests.get(img_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
            if r_get.status_code == 200:
                log.info(f"   Successfully downloaded search image ({len(r_get.content)} bytes)")
                return r_get.content
        else:
            log.warning(f"   URL HEAD check failed for {img_url}: Status={r.status_code}, Type={content_type}")
            
        r_get = requests.get(img_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        content_type = r_get.headers.get("content-type", "")
        if r_get.status_code == 200 and "image" in content_type:
            log.info(f"   Successfully downloaded search image via GET ({len(r_get.content)} bytes)")
            return r_get.content
            
    except Exception as e:
        log.warning(f"   Failed to validate/download image from {img_url}: {e}")
        
    return None


class ImageUploader:
    """Handles uploading images to Binance Square via their presigned URL flow."""
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "X-Square-OpenAPI-Key": self.api_key,
            "Content-Type": "application/json",
            "clienttype": "web"
        })

    def upload(self, image_bytes: bytes) -> str | None:
        if not self.api_key:
            log.warning("Missing Binance Square Key, skipping image upload.")
            return None
            
        log.info("📤 Requesting presigned URL from Binance Square image upload API...")
        try:
            r = self.session.post(
                BINANCE_UPLOAD_URL,
                json={"imageName": "chart.png"},
                timeout=15
            )
            if r.status_code != 200:
                log.warning(f"   Presigned URL request failed with status code {r.status_code}: {r.text}")
                return None
                
            data = r.json()
            if data.get("code") != "000000":
                log.warning(f"   Presigned URL response error: {data.get('message')}")
                return None
                
            res_data = data.get("data", {})
            put_url = res_data.get("presignedUrl")
            file_ticket = res_data.get("fileTicket")
        except Exception as e:
            log.warning(f"   Error fetching upload presigned URL: {e}")
            return None

        if not put_url or not file_ticket:
            log.warning(f"   Missing presignedUrl or fileTicket in response data: {res_data}")
            return None

        log.info("   PUT uploading image bytes to presigned URL...")
        try:
            put_r = requests.put(
                put_url,
                data=image_bytes,
                headers={"Content-Type": "image/png"},
                timeout=30
            )
            if put_r.status_code not in (200, 204):
                log.warning(f"   Image bytes PUT failed with status code {put_r.status_code}")
                return None
        except Exception as e:
            log.warning(f"   Error PUT uploading image: {e}")
            return None

        log.info("   Polling image status to wait for public CDN URL...")
        cdn_url = None
        try:
            for attempt in range(1, 12):
                time.sleep(1.5)
                r_st = self.session.post(
                    BINANCE_STATUS_URL,
                    json={"fileTicket": file_ticket},
                    timeout=10
                )
                if r_st.status_code != 200:
                    continue
                data_st = r_st.json().get("data", {})
                if data_st and data_st.get("imageUrl"):
                    cdn_url = data_st.get("imageUrl")
                    break
                if data_st and data_st.get("status") == 2:
                    log.warning(f"   Image processing failed on backend: {data_st.get('failedReason')}")
                    return None
        except Exception as e:
            log.warning(f"   Error polling image status: {e}")
            return None

        if not cdn_url:
            log.warning("   Timeout waiting for public image URL from Binance status endpoint")
            return None

        log.info(f"   🖼 Image successfully uploaded to Binance CDN: {cdn_url}")
        return cdn_url


# ─────────────────────────────────────────────
# BINANCE SQUARE POSTER
# ─────────────────────────────────────────────

def post_to_binance_square(content: str, image_urls: list = None) -> dict:
    headers = {
        "X-Square-OpenAPI-Key": BINANCE_SQUARE_KEY,
        "Content-Type": "application/json",
        "clienttype": "web",
    }
    payload = {"bodyTextOnly": content}
    if image_urls:
        payload["imageList"] = image_urls
        payload["contentType"] = 1
    else:
        payload["contentType"] = 1

    response = requests.post(
        BINANCE_POST_ENDPOINT,
        headers=headers,
        json=payload,
        timeout=15
    )
    return response.json()


# ─────────────────────────────────────────────
# INTERVAL PICKER — irregular, human-like
# ─────────────────────────────────────────────

def pick_interval() -> int:
    """
    Returns a random interval in seconds using weighted bands.
    Occasionally adds a 'micro-burst' (very short gap) to mimic
    someone posting 2-3 things quickly then going quiet.
    """
    # 12% chance of a micro-burst (10–45 sec gap)
    if random.random() < 0.12:
        return random.randint(10, 45)

    band = random.choices(INTERVAL_BANDS, weights=INTERVAL_WEIGHTS, k=1)[0]
    base = random.randint(band[0], band[1])

    # Add small random jitter (±20%) so even same-band posts differ
    jitter = int(base * random.uniform(-0.2, 0.2))
    return max(10, base + jitter)


def format_interval(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s"
    elif seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    else:
        h = seconds // 3600
        m = (seconds % 3600) // 60
        return f"{h}h {m}m"


# ─────────────────────────────────────────────
# DAILY SESSION SCHEDULER
# Posts are spread across a natural active window:
# 06:00–23:00 UTC (avoids posting at 3am like a bot)
# ─────────────────────────────────────────────

def reset_daily_cycle():
    global bot_state
    log.info("🔄 Resetting daily cycle posts, schedule, and stats...")
    with state_lock:
        bot_state["posts_published"] = 0
        bot_state["posts_failed"] = 0
        bot_state["n_posts_scheduled"] = 0
        bot_state["schedule"] = []
        bot_state["status"] = "Reset: Waiting for next cycle"
        bot_state["error_message"] = None
    save_bot_state()

def responsive_sleep(seconds, status_prefix):
    remaining = seconds
    while remaining > 0:
        now_ist = get_ist_now()
        with state_lock:
            bot_state["status"] = f"{status_prefix} | IST: {now_ist.strftime('%I:%M:%S %p')} (in {format_interval(int(remaining))})"
        sleep_chunk = min(10, remaining)
        time.sleep(sleep_chunk)
        remaining -= sleep_chunk

def run_daily_session():
    global bot_state
    if not GEMINI_API_KEYS or not BINANCE_SQUARE_KEY:
        raise ValueError(
            "Missing API keys. Set GEMINI_API_KEYS (or GEMINI_API_KEY) and BINANCE_SQUARE_KEY in your .env file."
        )

    # 1. Check if we have an active schedule with at least one "Pending" item
    has_active_schedule = False
    with state_lock:
        if bot_state["schedule"]:
            has_active_schedule = any(item["status"] in ("Pending", "Generating", "Posting") for item in bot_state["schedule"])

    if has_active_schedule:
        log.info("📅 Resuming existing daily schedule from persistent state.")
        execute_active_schedule()
        return

    # 2. Build a fresh schedule for the remaining active window
    fetcher = LiveDataFetcher()
    
    # Initial data fetch
    try:
        live_data = fetcher.refresh_all()
    except Exception as e:
        log.warning(f"Initial live data fetch failed: {e}. Will retry during postings.")
        live_data = {"market": {}, "trending": [], "news": [], "fg": {}, "fetched_at": "N/A"}

    now_ist = get_ist_now()
    cycle_end = get_cycle_end(now_ist)
    remaining_seconds = (cycle_end - now_ist).total_seconds()
    
    if remaining_seconds <= 0:
        log.warning("No remaining time in today's active window. Skipping scheduling.")
        return

    # Calculate proportion of posts to schedule
    total_window_duration = (cycle_end - get_cycle_start(now_ist)).total_seconds() # 8 hours (28800 seconds)
    proportion = max(0.1, min(1.0, remaining_seconds / total_window_duration))
    
    base_n = random.randint(POSTS_PER_DAY_MIN, POSTS_PER_DAY_MAX)
    n_posts = max(1, int(base_n * proportion))
    
    # Generate schedule times within the remaining window
    timestamps_ist = sorted([
        now_ist + timedelta(seconds=random.randint(0, int(remaining_seconds)))
        for _ in range(n_posts)
    ])
    
    log.info(f"📅 Daily session: {n_posts} posts scheduled across remaining active window")
    log.info(f"   First post: {timestamps_ist[0].strftime('%I:%M %p IST')}")
    log.info(f"   Last post:  {timestamps_ist[-1].strftime('%I:%M %p IST')}")

    with state_lock:
        bot_state["n_posts_scheduled"] = n_posts
        bot_state["schedule"] = [
            {
                "time": s.strftime("%I:%M %p IST"),
                "time_iso": s.isoformat(),
                "status": "Pending",
                "coin": "Pending",
                "type": "Pending"
            } for s in timestamps_ist
        ]
    save_bot_state()
    
    execute_active_schedule()

def get_cycle_start(dt_ist):
    return dt_ist.replace(hour=11, minute=30, second=0, microsecond=0)

def get_cycle_end(dt_ist):
    return dt_ist.replace(hour=19, minute=30, second=0, microsecond=0)

def execute_active_schedule():
    global bot_state
    
    rotator = GeminiClientRotator(GEMINI_API_KEYS)
    fetcher = LiveDataFetcher()
    
    try:
        live_data = fetcher.refresh_all()
    except Exception as e:
        log.warning(f"Initial live data fetch in execute_active_schedule failed: {e}")
        live_data = {"market": {}, "trending": [], "news": [], "fg": {}, "fetched_at": "N/A"}

    recent_coins = bot_state["recent_coins"]
    recent_types = bot_state["recent_types"]
    fail_streak = 0
    
    while True:
        # Check if the cycle has ended in the meantime
        now_ist = get_ist_now()
        if now_ist >= get_cycle_end(now_ist):
            log.info("⏰ Posting cycle end reached. Terminating daily schedule execution.")
            break
            
        # Find next pending item
        next_item_idx = -1
        with state_lock:
            for idx, item in enumerate(bot_state["schedule"]):
                if item["status"] == "Pending":
                    next_item_idx = idx
                    break
        
        if next_item_idx == -1:
            log.info("🏁 No pending items left in schedule. Daily schedule execution finished.")
            break
            
        item = bot_state["schedule"][next_item_idx]
        scheduled_time_ist = datetime.fromisoformat(item["time_iso"])
        
        # Wait until scheduled time
        wait_sec = (scheduled_time_ist - now_ist).total_seconds()
        if wait_sec > 0:
            log.info(f"⏳ Next scheduled post (index {next_item_idx + 1}) in {format_interval(int(wait_sec))} at {item['time']}")
            responsive_sleep(wait_sec, f"Waiting for post {next_item_idx + 1}/{len(bot_state['schedule'])} at {item['time']}")
            
        # Re-check time after sleep
        now_ist = get_ist_now()
        if now_ist >= get_cycle_end(now_ist):
            log.info("⏰ Posting cycle end reached after sleep. Terminating daily schedule execution.")
            break

        # Refresh live data periodically
        if next_item_idx > 0 and next_item_idx % DATA_REFRESH_EVERY == 0:
            try:
                live_data = fetcher.refresh_all()
            except Exception as e:
                log.warning(f"Live data refresh failed: {e}")

        # Pick post type and coin
        is_news_post = random.random() < 0.5
        with state_lock:
            if is_news_post:
                post_type = next((t for t in POST_TYPES if t["name"] == "news_reaction"), POST_TYPES[4])
                coin = None
                try:
                    news_list = fetch_cryptopanic_news()
                    if news_list and isinstance(news_list, list):
                        all_titles = " ".join([it.get("title", "").upper() for it in news_list])
                        mentioned_coins = []
                        for c in COINS:
                            if c["symbol"].upper() in all_titles or c["cg_id"].upper() in all_titles or c["tag"].upper() in all_titles:
                                mentioned_coins.append(c)
                        if mentioned_coins:
                            coin = random.choice(mentioned_coins)
                except Exception as e:
                    log.warning(f"Error scanning news headlines for coin: {e}")
                
                if not coin:
                    available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
                    coin = random.choice(available_coins if available_coins else COINS)
            else:
                other_types = [t for t in POST_TYPES if t["name"] != "news_reaction" and t["name"] not in recent_types[-2:]]
                post_type = random.choice(other_types if other_types else POST_TYPES)
                
                if post_type["name"] == "trending_coin_take" and live_data.get("trending"):
                    trending_syms = live_data["trending"]
                    matching_coins = [c for c in COINS if c["symbol"] in trending_syms and c["tag"] not in recent_coins[-4:]]
                    coin = random.choice(matching_coins if matching_coins else COINS)
                else:
                    available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
                    coin = random.choice(available_coins if available_coins else COINS)

            bot_state["schedule"][next_item_idx]["coin"] = coin["tag"]
            bot_state["schedule"][next_item_idx]["type"] = post_type["name"]
            bot_state["schedule"][next_item_idx]["status"] = "Generating"
        save_bot_state()

        # Build live data block
        live_data_block = format_coin_data(coin, live_data.get("market", {}), fetcher, live_data)

        # Generate content with Gemini
        content = None
        failed_keys_count = 0
        max_attempts = len(GEMINI_API_KEYS)
        while failed_keys_count < max_attempts:
            try:
                current_client = rotator.get_client()
                log.info(f"🤖 Generating [{post_type['name']}] post about {coin['tag']} (Key {rotator.current_index + 1}/{len(GEMINI_API_KEYS)}, attempt {failed_keys_count + 1}/{max_attempts})...")
                content = generate_post(current_client, post_type, coin, live_data_block, recent_coins, recent_types)

                word_count = len(content.split())
                if word_count < 8:
                    log.warning(f"   Post too short ({word_count} words), skipping.")
                    with state_lock:
                        bot_state["schedule"][next_item_idx]["status"] = "Skipped (Too Short)"
                    save_bot_state()
                    content = None
                    break
                if word_count > 180:
                    log.warning(f"   Post too long ({word_count} words), truncating.")
                    content = " ".join(content.split()[:160]) + "..."
                
                fail_streak = 0
                break

            except Exception as e:
                log.error(f"   Gemini error on key index {rotator.current_index}: {e}")
                failed_keys_count += 1
                if failed_keys_count < max_attempts:
                    rotator.rotate()
                    time.sleep(1)
                else:
                    try:
                        log.info(f"   ⚠️ All Gemini keys exhausted. Falling back to Gemma 4 for final content generation...")
                        content = generate_post_with_gemma(current_client, post_type, coin, live_data_block, recent_coins, recent_types)
                        fail_streak = 0
                        break
                    except Exception as g_err:
                        log.error(f"   Gemma 4 fallback final generation failed: {g_err}")
                        with state_lock:
                            bot_state["schedule"][next_item_idx]["status"] = f"Gemini Error"
                            bot_state["posts_failed"] += 1
                        save_bot_state()
                        fail_streak += 1
                        if fail_streak >= 3:
                            log.error("   3 consecutive Gemini failures — pausing 5 minutes.")
                            time.sleep(300)
                            fail_streak = 0
                        break

        if not content:
            continue

        content = content.strip() + " "
        klines = fetcher.fetch_klines(coin["symbol"])

        # Image generation/upload
        image_urls = []
        try:
            image_bytes = None
            uploader = ImageUploader(BINANCE_SQUARE_KEY)
            technical_types = {"price_target", "entry_signal", "dip_entry", "bearish_warning"}
            if post_type["name"] in technical_types:
                log.info(f"   📊 Technical post type detected. Generating advanced chart for {coin['symbol']}...")
                image_bytes = generate_advanced_chart(coin["symbol"], klines)
            else:
                log.info(f"   🔍 News/trending post type detected. Searching web for {coin['symbol']} image...")
                search_topic = f"{coin['tag']} price action and news"
                image_bytes = retrieve_search_image(rotator, coin["symbol"], search_topic)
                if not image_bytes:
                    log.info("   ⚠️ Web search image failed or returned no result. Falling back to generating a chart...")
                    image_bytes = generate_advanced_chart(coin["symbol"], klines)
            
            if image_bytes:
                cdn_url = uploader.upload(image_bytes)
                if cdn_url:
                    image_urls.append(cdn_url)
        except Exception as img_err:
            log.warning(f"   ⚠️ Image upload workflow failed: {img_err}. Posting as text-only.")

        # Post to Binance Square
        try:
            with state_lock:
                bot_state["status"] = f"Posting post {next_item_idx + 1}/{len(bot_state['schedule'])} to Binance..."
                bot_state["schedule"][next_item_idx]["status"] = "Posting"
            save_bot_state()
                
            log.info(f"📤 Posting to Binance Square...")
            result = post_to_binance_square(content, image_urls)

            if result.get("code") == "000000":
                post_id = result.get("data", {}).get("id", "unknown")
                post_url = f"https://www.binance.com/square/post/{post_id}"
                fail_streak = 0
                log.info(f"   ✅ Success! Post #{bot_state['posts_published'] + 1} → {post_url}")
                log.info(f"   Content preview: {content[:80]}...")

                with state_lock:
                    recent_coins.append(coin["tag"])
                    recent_types.append(post_type["name"])
                    if len(recent_coins) > 10:
                        recent_coins.pop(0)
                    if len(recent_types) > 6:
                        recent_types.pop(0)
                        
                    bot_state["schedule"][next_item_idx]["status"] = "Published"
                    bot_state["posts_published"] += 1
                    bot_state["recent_posts"].insert(0, {
                        "time": get_ist_now().strftime("%I:%M:%S %p IST"),
                        "type": post_type["name"],
                        "content": content,
                        "status": "Success",
                        "url": post_url
                    })
                    if len(bot_state["recent_posts"]) > 10:
                        bot_state["recent_posts"].pop()
            else:
                error_code = result.get("code", "unknown")
                error_msg  = result.get("message", "no message")
                log.warning(f"   ⚠️ Binance rejected post. Code: {error_code} | {error_msg}")

                with state_lock:
                    bot_state["schedule"][next_item_idx]["status"] = f"Rejected"
                    bot_state["posts_failed"] += 1
                    bot_state["recent_posts"].insert(0, {
                        "time": get_ist_now().strftime("%I:%M:%S %p IST"),
                        "type": post_type["name"],
                        "content": content[:80] + "...",
                        "status": f"Rejected ({error_code})",
                        "url": "#"
                    })
                    if len(bot_state["recent_posts"]) > 10:
                        bot_state["recent_posts"].pop()

                if error_code in ("10001", "20001"):
                    log.error("   Invalid or missing API key. Check BINANCE_SQUARE_KEY.")
                    return
                elif error_code == "40003":
                    log.warning("   Daily post limit hit. Stopping for today.")
                    break
                elif error_code == "50001":
                    log.warning("   Sensitive content detected. Skipping this post.")
                elif error_code == "30001":
                    log.error("   Account banned. Contact Binance support.")
                    return

        except requests.exceptions.RequestException as e:
            log.error(f"   Network error posting to Binance Square: {e}")
            with state_lock:
                bot_state["schedule"][next_item_idx]["status"] = "Network Error"
                bot_state["posts_failed"] += 1
            fail_streak += 1
            
        save_bot_state()


# ─────────────────────────────────────────────
# FLASK WEB APP DEFINITIONS
# ─────────────────────────────────────────────
app = Flask(__name__)

# Start background thread on the first request to avoid Gunicorn master/worker fork issues
first_request_done = False
first_request_lock = threading.Lock()

@app.before_request
def initialize_background_thread():
    global first_request_done
    if not first_request_done:
        with first_request_lock:
            if not first_request_done:
                start_background_thread()
                first_request_done = True

# Premium Web Dashboard (Glassmorphism dark theme)
DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Binance Square Auto-Poster Control Center</title>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=Outfit:wght@400;500;600;700;800&family=JetBrains+Mono&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg-color: #060814;
            --card-bg: rgba(13, 17, 38, 0.7);
            --border-color: rgba(255, 255, 255, 0.08);
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
            --accent-gold: #F0B90B;
            --accent-glow: rgba(240, 185, 11, 0.15);
            --success: #10B981;
            --success-glow: rgba(16, 185, 129, 0.15);
            --error: #EF4444;
            --error-glow: rgba(239, 68, 68, 0.15);
            --pending: #F59E0B;
            --pending-glow: rgba(245, 158, 11, 0.15);
        }
        
        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }
        
        body {
            font-family: 'Inter', sans-serif;
            background-color: var(--bg-color);
            background-image: 
                radial-gradient(circle at 10% 20%, rgba(240, 185, 11, 0.05) 0%, transparent 40%),
                radial-gradient(circle at 90% 80%, rgba(138, 58, 255, 0.05) 0%, transparent 40%);
            background-attachment: fixed;
            color: var(--text-primary);
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            padding: 2rem;
        }

        h1, h2, h3, h4 {
            font-family: 'Outfit', sans-serif;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }

        .logo-section {
            display: flex;
            align-items: center;
            gap: 1rem;
        }

        .logo-icon {
            width: 45px;
            height: 45px;
            background: linear-gradient(135deg, var(--accent-gold), #ff8c00);
            border-radius: 12px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 0 20px rgba(240, 185, 11, 0.3);
        }

        .logo-icon svg {
            width: 25px;
            height: 25px;
            fill: #060814;
        }

        .logo-text h1 {
            font-size: 1.5rem;
            font-weight: 800;
            letter-spacing: -0.5px;
            background: linear-gradient(to right, #ffffff, #e5e7eb);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .logo-text p {
            font-size: 0.85rem;
            color: var(--text-secondary);
        }

        .uptime-badge {
            background: rgba(255, 255, 255, 0.05);
            border: 1px solid var(--border-color);
            padding: 0.5rem 1rem;
            border-radius: 50px;
            font-size: 0.85rem;
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-family: 'JetBrains Mono', monospace;
        }

        .pulse-dot {
            width: 8px;
            height: 8px;
            background-color: var(--success);
            border-radius: 50%;
            display: inline-block;
            box-shadow: 0 0 10px var(--success);
            animation: pulse 2s infinite;
        }

        @keyframes pulse {
            0% { transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
            70% { transform: scale(1); opacity: 1; box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
            100% { transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
        }

        .pulse-dot.sleeping {
            background-color: var(--pending);
            box-shadow: 0 0 10px var(--pending);
            animation: pulse-sleeping 2s infinite;
        }

        @keyframes pulse-sleeping {
            0% { transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.7); }
            70% { transform: scale(1); opacity: 1; box-shadow: 0 0 0 8px rgba(245, 158, 11, 0); }
            100% { transform: scale(0.9); opacity: 0.5; box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }
        }

        .dashboard-grid {
            display: grid;
            grid-template-columns: 1fr 1fr 1fr;
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        @media (max-width: 1024px) {
            .dashboard-grid {
                grid-template-columns: 1fr;
            }
        }

        .card {
            background: var(--card-bg);
            backdrop-filter: blur(12px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            transition: all 0.3s ease;
            position: relative;
            overflow: hidden;
        }

        .card:hover {
            border-color: rgba(240, 185, 11, 0.2);
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.3);
            transform: translateY(-2px);
        }

        .card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 3px;
            background: linear-gradient(90deg, transparent, rgba(240, 185, 11, 0.3), transparent);
        }

        .card-title {
            font-size: 1rem;
            color: var(--text-secondary);
            font-weight: 500;
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .stat-value {
            font-size: 2.25rem;
            font-weight: 700;
            font-family: 'Outfit', sans-serif;
            color: #ffffff;
            line-height: 1.2;
        }

        .stat-sub {
            font-size: 0.85rem;
            color: var(--text-secondary);
            margin-top: 0.5rem;
        }

        .config-status {
            display: flex;
            flex-direction: column;
            gap: 0.75rem;
        }

        .config-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            background: rgba(255, 255, 255, 0.02);
            padding: 0.75rem 1rem;
            border-radius: 8px;
            border: 1px solid rgba(255, 255, 255, 0.04);
        }

        .config-name {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
        }

        .status-badge {
            padding: 0.25rem 0.75rem;
            border-radius: 50px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
        }

        .status-badge.configured {
            background-color: var(--success-glow);
            color: var(--success);
            border: 1px solid rgba(16, 185, 129, 0.3);
        }

        .status-badge.missing {
            background-color: var(--error-glow);
            color: var(--error);
            border: 1px solid rgba(239, 68, 68, 0.3);
        }

        .btn {
            background: linear-gradient(135deg, var(--accent-gold), #cca00a);
            color: #060814;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 8px;
            font-weight: 600;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s ease;
            font-family: 'Inter', sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 0.5rem;
            box-shadow: 0 4px 15px rgba(240, 185, 11, 0.25);
        }

        .btn:hover {
            opacity: 0.95;
            box-shadow: 0 6px 20px rgba(240, 185, 11, 0.4);
            transform: translateY(-1px);
        }

        .btn:active {
            transform: translateY(0);
        }

        .btn:disabled {
            background: #4b5563;
            color: #9ca3af;
            cursor: not-allowed;
            box-shadow: none;
            transform: none;
        }

        .btn-spinner {
            width: 16px;
            height: 16px;
            border: 2px solid rgba(6, 8, 20, 0.3);
            border-top: 2px solid #060814;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            display: none;
        }

        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }

        .layout-cols {
            display: grid;
            grid-template-columns: 1.2fr 1.8fr;
            gap: 1.5rem;
            margin-bottom: 2rem;
        }

        @media (max-width: 1024px) {
            .layout-cols {
                grid-template-columns: 1fr;
            }
        }

        .scroll-container {
            max-height: 400px;
            overflow-y: auto;
            padding-right: 0.5rem;
        }

        .scroll-container::-webkit-scrollbar {
            width: 6px;
        }

        .scroll-container::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.01);
            border-radius: 10px;
        }

        .scroll-container::-webkit-scrollbar-thumb {
            background: rgba(255, 255, 255, 0.1);
            border-radius: 10px;
        }

        .scroll-container::-webkit-scrollbar-thumb:hover {
            background: rgba(255, 255, 255, 0.2);
        }

        /* Timeline schedule styling */
        .timeline {
            position: relative;
            padding-left: 1.5rem;
        }

        .timeline::before {
            content: '';
            position: absolute;
            left: 5px;
            top: 0;
            height: 100%;
            width: 2px;
            background: var(--border-color);
        }

        .timeline-item {
            position: relative;
            margin-bottom: 1.25rem;
        }

        .timeline-item::before {
            content: '';
            position: absolute;
            left: -1.5rem;
            top: 5px;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            background: #4b5563;
            border: 2px solid var(--bg-color);
            z-index: 2;
        }

        .timeline-item.pending::before {
            background: var(--pending);
        }

        .timeline-item.generating::before {
            background: var(--accent-gold);
            box-shadow: 0 0 8px var(--accent-gold);
        }

        .timeline-item.published::before {
            background: var(--success);
        }

        .timeline-item.failed::before {
            background: var(--error);
        }

        .timeline-content {
            background: rgba(255, 255, 255, 0.01);
            border: 1px solid var(--border-color);
            padding: 0.75rem 1rem;
            border-radius: 8px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .timeline-time {
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            font-weight: 600;
        }

        .timeline-details {
            font-size: 0.85rem;
            color: var(--text-secondary);
            text-align: right;
        }

        .timeline-coin {
            font-weight: 600;
            color: #ffffff;
        }

        /* Recent Posts list */
        .post-card {
            background: rgba(255, 255, 255, 0.01);
            border: 1px solid var(--border-color);
            border-radius: 10px;
            padding: 1rem;
            margin-bottom: 1rem;
        }

        .post-type-badge {
            background: rgba(240, 185, 11, 0.1);
            color: var(--accent-gold);
            border: 1px solid rgba(240, 185, 11, 0.25);
            padding: 2px 6px;
            border-radius: 4px;
            font-size: 0.7rem;
            margin-left: 0.5rem;
            font-family: 'Inter', sans-serif;
            text-transform: uppercase;
            font-weight: 600;
            display: inline-block;
        }

        .post-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 0.5rem;
            font-size: 0.8rem;
        }

        .post-time {
            color: var(--text-secondary);
            font-family: 'JetBrains Mono', monospace;
        }

        .post-status {
            font-weight: 600;
        }

        .post-status.success {
            color: var(--success);
        }

        .post-status.failed {
            color: var(--error);
        }

        .post-body {
            font-size: 0.9rem;
            line-height: 1.4;
            color: #ffffff;
            white-space: pre-wrap;
            margin-bottom: 0.75rem;
        }

        .post-link {
            font-size: 0.8rem;
            color: var(--accent-gold);
            text-decoration: none;
            display: inline-flex;
            align-items: center;
            gap: 0.25rem;
            font-weight: 500;
        }

        .post-link:hover {
            text-decoration: underline;
        }

        /* Console / Terminal output */
        .terminal {
            background: #02040a;
            border: 1px solid var(--border-color);
            border-radius: 12px;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            color: #a9b1d6;
            padding: 1rem;
            height: 350px;
            overflow-y: auto;
            display: flex;
            flex-direction: column;
            gap: 0.25rem;
            box-shadow: inset 0 0 10px rgba(0, 0, 0, 0.8);
        }

        .terminal-line {
            line-height: 1.4;
            word-break: break-all;
        }

        .terminal-line.error { color: #f7768e; }
        .terminal-line.warning { color: #e0af68; }
        .terminal-line.info { color: #7aa2f7; }
        .terminal-line.success { color: #9ece6a; }

        footer {
            margin-top: auto;
            text-align: center;
            padding-top: 2rem;
            font-size: 0.8rem;
            color: var(--text-secondary);
            border-top: 1px solid var(--border-color);
        }

        footer a {
            color: var(--accent-gold);
            text-decoration: none;
        }
    </style>
</head>
<body>

    <header>
        <div class="logo-section">
            <div class="logo-icon">
                <svg viewBox="0 0 24 24">
                    <path d="M19 3H5c-1.1 0-2 .9-2 2v14c0 1.1.9 2 2 2h14c1.1 0 2-.9 2-2V5c0-1.1-.9-2-2-2zm-2 10H7v-2h10v2zm0-4H7V7h10v2zm0 8H7v-2h10v2z"/>
                </svg>
            </div>
            <div class="logo-text">
                <h1>Binance Auto-Poster</h1>
                <p>Gemini AI Powered Content Engine</p>
            </div>
        </div>
        <div class="uptime-badge">
            <span class="pulse-dot" id="pulse-indicator"></span>
            Status: <span id="status-text-badge">Active</span>
        </div>
    </header>

    <div class="dashboard-grid">
        <!-- Status Card -->
        <div class="card">
            <div class="card-title">System Status</div>
            <div class="stat-value" id="bot-status" style="font-size: 1.25rem; word-break: break-word; min-height: 48px;">Loading...</div>
            <div class="stat-sub">Current activity state of the auto-poster</div>
        </div>

        <!-- Publishing Metrics -->
        <div class="card">
            <div class="card-title">Daily Progress</div>
            <div class="stat-value" id="stats-published">0 / 0</div>
            <div class="stat-sub">Posts published successfully today. Failed: <span id="stats-failed" style="color: var(--error)">0</span></div>
        </div>

        <!-- API Config Check -->
        <div class="card">
            <div class="card-title">Manual Action</div>
            <button class="btn" id="post-now-btn" onclick="triggerManualPost()">
                <div class="btn-spinner" id="btn-spinner"></div>
                <span id="btn-text">🚀 Trigger Post Now</span>
            </button>
            <div class="stat-sub" style="margin-top: 0.75rem;">Generate and publish a post immediately</div>
        </div>
    </div>

    <div class="layout-cols">
        <!-- Daily Schedule Column -->
        <div class="card">
            <div class="card-title">Daily Posting Schedule</div>
            <div class="scroll-container timeline" id="schedule-container">
                <div class="timeline-item pending">
                    <div class="timeline-content">
                        <span class="timeline-time">--:-- IST</span>
                        <div class="timeline-details">
                            <span class="timeline-coin">Waiting</span><br>
                            <span>Initialize scheduler</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Terminal Logs & History -->
        <div style="display: flex; flex-direction: column; gap: 1.5rem;">
            <!-- Console Logs -->
            <div class="card" style="padding-bottom: 1rem;">
                <div class="card-title">Live Service Logs</div>
                <div class="terminal" id="terminal-logs">
                    <div class="terminal-line info">Connecting to service logs...</div>
                </div>
            </div>
            
            <!-- Recent Posts -->
            <div class="card">
                <div class="card-title">Recently Published Content</div>
                <div class="scroll-container" id="recent-posts-container">
                    <div style="text-align: center; color: var(--text-secondary); padding: 2rem 0;">
                        No posts published yet in this session.
                    </div>
                </div>
            </div>
        </div>
    </div>

    <footer>
        <p>Binance Auto-Poster Bot Control Center • Configured for Render Hosting</p>
    </footer>

    <script>
        // Poll status every 4 seconds
        async function fetchStatus() {
            try {
                const response = await fetch('/api/status');
                const data = await response.json();
                
                // Update bot status description
                document.getElementById('bot-status').innerText = data.status || 'Running';
                
                // Update pulse indicator class based on status
                const pulse = document.getElementById('pulse-indicator');
                const badgeText = document.getElementById('status-text-badge');
                if (data.status && data.status.toLowerCase().includes('sleep')) {
                    pulse.className = 'pulse-dot sleeping';
                    badgeText.innerText = 'Sleeping';
                } else {
                    pulse.className = 'pulse-dot';
                    badgeText.innerText = 'Active';
                }
                
                // Update statistics
                document.getElementById('stats-published').innerText = `${data.posts_published} / ${data.n_posts_scheduled}`;
                document.getElementById('stats-failed').innerText = data.posts_failed;
                
                // Render schedule
                const scheduleContainer = document.getElementById('schedule-container');
                if (data.schedule && data.schedule.length > 0) {
                    scheduleContainer.innerHTML = '';
                    data.schedule.forEach(item => {
                        const itemClass = item.status.toLowerCase().replace(/[^a-z]/g, '');
                        let displayStatus = item.status;
                        if (item.status === 'Pending') {
                            displayStatus = 'Scheduled';
                        }
                        
                        const el = document.createElement('div');
                        el.className = `timeline-item ${itemClass}`;
                        el.innerHTML = `
                            <div class="timeline-content">
                                <span class="timeline-time">${item.time}</span>
                                <div class="timeline-details">
                                    <span class="timeline-coin">${item.coin !== 'Pending' ? item.coin : 'TBD'}</span><br>
                                    <span style="font-size: 0.75rem;">${item.type !== 'Pending' ? item.type : displayStatus}</span>
                                </div>
                            </div>
                        `;
                        scheduleContainer.appendChild(el);
                    });
                }
                
                // Render recent posts
                const recentContainer = document.getElementById('recent-posts-container');
                if (data.recent_posts && data.recent_posts.length > 0) {
                    recentContainer.innerHTML = '';
                    data.recent_posts.forEach(post => {
                        const isSuccess = post.status.toLowerCase().includes('success');
                        const statusClass = isSuccess ? 'success' : 'failed';
                        
                        const el = document.createElement('div');
                        el.className = 'post-card';
                        
                        let actionLink = '';
                        if (isSuccess && post.url && post.url !== '#') {
                            actionLink = `<a href="${post.url}" target="_blank" class="post-link">
                                View on Binance Square 
                                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                    <polyline points="15 3 21 3 21 9"></polyline>
                                    <line x1="10" y1="14" x2="21" y2="3"></line>
                                </svg>
                            </a>`;
                        }
                        
                        const postTypeBadge = post.type ? `<span class="post-type-badge">${post.type}</span>` : '';
                        el.innerHTML = `
                            <div class="post-header" style="align-items: center;">
                                <span class="post-time">${post.time}${postTypeBadge}</span>
                                <span class="post-status ${statusClass}">${post.status}</span>
                            </div>
                            <div class="post-body">${escapeHTML(post.content)}</div>
                            ${actionLink}
                        `;
                        recentContainer.appendChild(el);
                    });
                } else {
                    recentContainer.innerHTML = `
                        <div style="text-align: center; color: var(--text-secondary); padding: 2rem 0;">
                            No posts published yet in this session.
                        </div>
                    `;
                }
                
                // Render console logs
                const terminal = document.getElementById('terminal-logs');
                if (data.last_log_messages && data.last_log_messages.length > 0) {
                    const wasScrolledToBottom = terminal.scrollHeight - terminal.clientHeight <= terminal.scrollTop + 20;
                    
                    terminal.innerHTML = '';
                    data.last_log_messages.forEach(msg => {
                        const el = document.createElement('div');
                        let typeClass = 'info';
                        
                        if (msg.includes('[ERROR]')) typeClass = 'error';
                        else if (msg.includes('[WARNING]')) typeClass = 'warning';
                        else if (msg.includes('✅') || msg.includes('Success')) typeClass = 'success';
                        
                        el.className = `terminal-line ${typeClass}`;
                        el.innerText = msg;
                        terminal.appendChild(el);
                    });
                    
                    // Auto-scroll terminal if it was already at the bottom
                    if (wasScrolledToBottom) {
                        terminal.scrollTop = terminal.scrollHeight;
                    }
                }
                
            } catch (err) {
                console.error("Error fetching status:", err);
            }
        }

        function escapeHTML(str) {
            return str
                .replace(/&/g, "&amp;")
                .replace(/</g, "&lt;")
                .replace(/>/g, "&gt;")
                .replace(/"/g, "&quot;")
                .replace(/'/g, "&#039;");
        }

        async function triggerManualPost() {
            const btn = document.getElementById('post-now-btn');
            const spinner = document.getElementById('btn-spinner');
            const text = document.getElementById('btn-text');
            
            btn.disabled = true;
            spinner.style.display = 'block';
            text.innerText = 'Generating & Publishing...';
            
            try {
                const response = await fetch('/api/post-now', {
                    method: 'POST'
                });
                const result = await response.json();
                
                if (result.success) {
                    alert(`✅ Post successfully published!\n\nLink: ${result.url}`);
                } else {
                    alert(`❌ Failed to publish post:\n\n${result.error}`);
                }
            } catch (err) {
                alert(`❌ Network error triggering post:\n\n${err.message}`);
            } finally {
                btn.disabled = false;
                spinner.style.display = 'none';
                text.innerText = '🚀 Trigger Post Now';
                fetchStatus();
            }
        }

        // Initial fetch and start interval
        fetchStatus();
        setInterval(fetchStatus, 4000);
    </script>
</body>
</html>
"""

@app.route("/")
def index():
    return render_template_string(DASHBOARD_HTML)

@app.route("/health")
def health():
    return jsonify({
        "status": "healthy",
        "is_running": bot_state["is_running"],
        "posts_published": bot_state["posts_published"],
        "posts_failed": bot_state["posts_failed"],
        "timestamp": datetime.utcnow().isoformat()
    })

@app.route("/api/status")
def api_status():
    with state_lock:
        serializable_schedule = []
        for item in bot_state["schedule"]:
            serializable_schedule.append({
                "time": item["time"],
                "status": item["status"],
                "coin": item["coin"],
                "type": item["type"]
            })
        return jsonify({
            "status": bot_state["status"],
            "start_time": bot_state["start_time"],
            "posts_published": bot_state["posts_published"],
            "posts_failed": bot_state["posts_failed"],
            "n_posts_scheduled": bot_state["n_posts_scheduled"],
            "schedule": serializable_schedule,
            "recent_posts": bot_state["recent_posts"],
            "last_log_messages": bot_state["last_log_messages"],
            "error_message": bot_state["error_message"]
        })

@app.route("/api/post-now", methods=["POST"])
def api_post_now():
    if not GEMINI_API_KEYS or not BINANCE_SQUARE_KEY:
        return jsonify({"success": False, "error": "API keys not configured"}), 400

    try:
        rotator       = GeminiClientRotator(GEMINI_API_KEYS)
        fetcher       = LiveDataFetcher()
        
        # Fresh live data fetch for the manual post
        try:
            live_data = fetcher.refresh_all()
        except Exception as e:
            log.warning(f"Manual post live data fetch failed: {e}")
            live_data = {"market": {}, "trending": [], "news": [], "fg": {}, "fetched_at": "N/A"}
        
        # Pick coin and type using shared recency history
        recent_coins = bot_state["recent_coins"]
        recent_types = bot_state["recent_types"]
        
        # Decide if this is a news post (50% probability) or other post
        is_news_post = random.random() < 0.5
        
        if is_news_post:
            # Use standard news_reaction type
            post_type = next((t for t in POST_TYPES if t["name"] == "news_reaction"), POST_TYPES[4])
            
            # Scan cryptopanic news for any coin mentions to set as primary coin
            coin = None
            try:
                news_list = fetch_cryptopanic_news()
                if news_list and isinstance(news_list, list):
                    all_titles = " ".join([item.get("title", "").upper() for item in news_list])
                    mentioned_coins = []
                    for c in COINS:
                        if c["symbol"].upper() in all_titles or c["cg_id"].upper() in all_titles or c["tag"].upper() in all_titles:
                            mentioned_coins.append(c)
                    if mentioned_coins:
                        coin = random.choice(mentioned_coins)
            except Exception as e:
                log.warning(f"Error scanning news headlines for coin: {e}")
            
            if not coin:
                available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
                coin = random.choice(available_coins if available_coins else COINS)
        else:
            # Pick other standard post types (excluding news_reaction)
            other_types = [t for t in POST_TYPES if t["name"] != "news_reaction" and t["name"] not in recent_types[-2:]]
            post_type = random.choice(other_types if other_types else POST_TYPES)
            
            # Pick coin — for trending/news types, try to use a trending coin
            if post_type["name"] == "trending_coin_take" and live_data.get("trending"):
                trending_syms = live_data["trending"]
                matching_coins = [c for c in COINS if c["symbol"] in trending_syms
                                  and c["tag"] not in recent_coins[-4:]]
                coin = random.choice(matching_coins if matching_coins else COINS)
            else:
                available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
                coin = random.choice(available_coins if available_coins else COINS)

        log.info(f"🤖 Manual trigger: Generating [{post_type['name']}] post about {coin['tag']}...")
        
        # Build live data block for this specific coin
        live_data_block = format_coin_data(coin, live_data.get("market", {}), fetcher, live_data)
        
        content = None
        failed_keys_count = 0
        max_attempts = len(GEMINI_API_KEYS)
        while failed_keys_count < max_attempts:
            try:
                current_client = rotator.get_client()
                content = generate_post(current_client, post_type, coin, live_data_block, recent_coins, recent_types)
                break
            except Exception as e:
                log.error(f"   Manual trigger Gemini error on key index {rotator.current_index}: {e}")
                failed_keys_count += 1
                if failed_keys_count < max_attempts:
                    rotator.rotate()
                else:
                    # Fallback to Gemma 4
                    try:
                        log.info("   Manual trigger falling back to Gemma 4 for content generation...")
                        content = generate_post_with_gemma(current_client, post_type, coin, live_data_block, recent_coins, recent_types)
                    except Exception as g_err:
                        log.error(f"   Manual trigger Gemma 4 fallback failed: {g_err}")
                        raise e

        if not content:
            raise ValueError("Failed to generate content after all API key attempts.")

        # Add a space at the end of post to prevent hashtag rendering issues
        content = content.strip() + " "

        # Fetch klines for image generation
        klines = fetcher.fetch_klines(coin["symbol"])

        # Determine image to generate/retrieve and upload to Binance Square
        image_urls = []
        try:
            image_bytes = None
            uploader = ImageUploader(BINANCE_SQUARE_KEY)
            
            # Technical post types: generate Matplotlib chart
            technical_types = {"price_target", "entry_signal", "dip_entry", "bearish_warning"}
            if post_type["name"] in technical_types:
                log.info(f"   📊 Technical post type detected. Generating advanced chart for {coin['symbol']}...")
                image_bytes = generate_advanced_chart(coin["symbol"], klines)
            else:
                # News / trending post types: try to search the web first
                log.info(f"   🔍 News/trending post type detected. Searching web for {coin['symbol']} image...")
                search_topic = f"{coin['tag']} price action and news"
                image_bytes = retrieve_search_image(rotator, coin["symbol"], search_topic)
                
                if not image_bytes:
                    log.info("   ⚠️ Web search image failed or returned no result. Falling back to generating a chart...")
                    image_bytes = generate_advanced_chart(coin["symbol"], klines)
            
            if image_bytes:
                cdn_url = uploader.upload(image_bytes)
                if cdn_url:
                    image_urls.append(cdn_url)
        except Exception as img_err:
            log.warning(f"   ⚠️ Manual trigger image upload workflow failed: {img_err}. Posting as text-only.")

        log.info(f"📤 Manual trigger: Posting to Binance Square...")
        result = post_to_binance_square(content, image_urls)

        if result.get("code") == "000000":
            post_id = result.get("data", {}).get("id", "unknown")
            post_url = f"https://www.binance.com/square/post/{post_id}"
            
            with state_lock:
                recent_coins.append(coin["tag"])
                recent_types.append(post_type["name"])
                if len(recent_coins) > 10:
                    recent_coins.pop(0)
                if len(recent_types) > 6:
                    recent_types.pop(0)
                    
                bot_state["posts_published"] += 1
                bot_state["recent_posts"].insert(0, {
                    "time": get_ist_now().strftime("%I:%M:%S %p IST"),
                    "type": post_type["name"],
                    "content": content,
                    "status": "Success",
                    "url": post_url
                })
                if len(bot_state["recent_posts"]) > 10:
                    bot_state["recent_posts"].pop()
            save_bot_state()
            
            log.info(f"   ✅ Manual success! Post → {post_url}")
            return jsonify({"success": True, "url": post_url, "content": content})
        else:
            error_code = result.get("code", "unknown")
            error_msg = result.get("message", "no message")
            log.warning(f"   ⚠️ Manual trigger rejected. Code: {error_code} | {error_msg}")
            
            with state_lock:
                bot_state["posts_failed"] += 1
                bot_state["recent_posts"].insert(0, {
                    "time": get_ist_now().strftime("%I:%M:%S %p IST"),
                    "type": post_type["name"],
                    "content": content[:80] + "...",
                    "status": f"Rejected ({error_code})",
                    "url": "#"
                })
                if len(bot_state["recent_posts"]) > 10:
                    bot_state["recent_posts"].pop()
            save_bot_state()
                    
            return jsonify({"success": False, "error": f"Binance error {error_code}: {error_msg}"}), 400
            
    except Exception as e:
        log.error(f"Error in manual post trigger: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/news", methods=["POST"])
def api_update_news():
    log.warning("POST /api/news is deprecated. News data is now fetched directly from GitHub.")
    return jsonify({"success": True, "message": "Deprecated. News data is now fetched directly from GitHub."})


@app.route("/api/newsdata", methods=["GET"])
def api_get_newsdata():
    try:
        data = fetch_cryptopanic_news()
        return jsonify(data)
    except Exception as e:
        log.error(f"Failed to fetch cryptopanic news for endpoint: {e}")
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────
# BACKGROUND WORKER & THREAD CONTROL
# ─────────────────────────────────────────────
bg_started = False
bg_lock = threading.Lock()

def background_worker():
    global bot_state
    with state_lock:
        bot_state["is_running"] = True
    
    # Reload state at thread startup
    load_bot_state()

    while True:
        try:
            now_ist = get_ist_now()
            cycle_start = get_cycle_start(now_ist)
            cycle_end = get_cycle_end(now_ist)

            if now_ist < cycle_start:
                wait_seconds = (cycle_start - now_ist).total_seconds()
                log.info(f"💤 Outside active window. Sleeping until start of next cycle at 11:30 AM IST (in {format_interval(int(wait_seconds))})...")
                
                # Reset previous stats if not already reset
                if bot_state["posts_published"] > 0 or bot_state["posts_failed"] > 0 or bot_state["schedule"]:
                    reset_daily_cycle()
                
                responsive_sleep(wait_seconds, "Sleeping until cycle start (11:30 AM IST)")
                
            elif now_ist >= cycle_end:
                tomorrow_start = get_cycle_start(now_ist + timedelta(days=1))
                wait_seconds = (tomorrow_start - now_ist).total_seconds()
                log.info(f"💤 Cycle ended for today. Resetting stats and sleeping until tomorrow's cycle at 11:30 AM IST (in {format_interval(int(wait_seconds))})...")
                
                # Reset cycle
                reset_daily_cycle()
                
                responsive_sleep(wait_seconds, "Sleeping until tomorrow's cycle (11:30 AM IST)")
                
            else:
                # We are inside the active window! Run/Resume daily session
                run_daily_session()
                time.sleep(10)
                    
        except Exception as e:
            log.error(f"Error in background worker loop: {e}")
            with state_lock:
                bot_state["error_message"] = str(e)
                bot_state["status"] = f"Error: {str(e)[:40]}"
            time.sleep(60)


def start_background_thread():
    global bg_started
    with bg_lock:
        if not bg_started:
            thread = threading.Thread(target=background_worker, daemon=True)
            thread.start()
            bg_started = True
            log.info("Background posting thread started successfully.")


# Note: Background thread is started dynamically via app.before_request when run under Gunicorn


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    if "--cli" in sys.argv:
        log.info("Starting in standalone CLI mode (one daily session)...")
        try:
            run_daily_session()
        except KeyboardInterrupt:
            log.info("Execution interrupted by user.")
    else:
        # Start background posting thread
        start_background_thread()
        
        # Start Flask web server (useful for local development/testing)
        port = int(os.environ.get("PORT", 10000))
        log.info(f"Starting local web server on port {port}...")
        app.run(host="0.0.0.0", port=port, debug=False)