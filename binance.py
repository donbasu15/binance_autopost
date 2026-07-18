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
import faulthandler
import signal
faulthandler.enable()
try:
    faulthandler.register(signal.SIGUSR1)
except Exception:
    pass
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

POSTS_PER_DAY_MIN = 60
POSTS_PER_DAY_MAX = 70

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

# Thread-safe state & logging (using RLock to prevent self-deadlock on log emit)
state_lock = threading.RLock()
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
    "recent_posts": [],  # Kept empty here to avoid large storage in autoposter
    "last_log_messages": last_log_messages,
    "recent_coins": [],
    "recent_types": [],
    "is_running": False,
    "error_message": None
}

recent_posts_cache = []  # In-memory cache for recent posts, synced with state server


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
            "recent_posts": [],  # Avoid storing recent posts locally to keep file size small
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
    global bot_state, recent_posts_cache
    
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

        # Sync todays_posts from state server to in-memory cache
        cloud_posts = state_server_manager.load_data("todays_posts")
        if isinstance(cloud_posts, list):
            recent_posts_cache = cloud_posts
            log.info(f"Loaded {len(recent_posts_cache)} posts from state server into recent_posts_cache")

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

def add_recent_post(post_type_name, content, status, url):
    global recent_posts_cache
    post_item = {
        "time": get_ist_now().strftime("%I:%M:%S %p IST"),
        "time_iso": get_ist_now().isoformat(),
        "type": post_type_name,
        "content": content,
        "status": status,
        "url": url
    }
    with state_lock:
        recent_posts_cache.insert(0, post_item)
        # Keep only the last 24 hours
        cutoff = get_ist_now() - timedelta(hours=24)
        recent_posts_cache = [
            p for p in recent_posts_cache
            if p.get("time_iso") and datetime.fromisoformat(p["time_iso"]) >= cutoff
        ][:50]
    
    if state_server_manager.server_url:
        def push_posts():
            state_server_manager.save_data("todays_posts", recent_posts_cache)
        threading.Thread(target=push_posts, daemon=True).start()


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

# ─────────────────────────────────────────────
# POST INSTRUCTIONS — 5 templates from top Binance Square earner analysis
# ─────────────────────────────────────────────

POST_INSTRUCTIONS = {

"quick_narrative": """
Write a SHORT narrative post (Template A). Structure:
- Line 1: [emoji] + one sharp take on the coin (use actual price from data)
- Lines 2-4: 2-3 lines of analysis referencing real price action, candle structure, or momentum from the data
- Line 5: Your personal stance ("I'm taking the long" / "I'm staying bearish" / "I'm watching for...")
- Line 6: Optional casual closing question or comment (😉 optional)

FORMATTING RULES:
- NO entry/SL/TP numbers in this template.
- Max 200 words.
- Absolutely NO hashtags.
- DO NOT write {future} or {spot} tags.
""",

"long_signal": """
Write a LONG SIGNAL post (Template B). Structure:
- Line 1: 🚀 [verb phrase] $COIN — Nx Leverage  (use 10x or 20x)
- Blank line
- "Entry: [price derived from live data — use current price ±1-2%]"
- Blank line
- "SL: [price — 4-6% below entry]"
- Blank line
- "TP1: [+3-5% from entry]"
- "TP2: [+7-10% from entry]"
- "TP3: [+15-20% from entry]"
- Blank line
- [1-2 lines: RSI reading, MACD direction, trend — use ACTUAL computed values from data]
- Blank line
- "‼️ Risk Management is Important."
- Blank line
- "Trade Here 👇🏻"

FORMATTING RULES:
- Each key component (Entry, SL, TP, ‼️, Trade Here) MUST start on a new line.
- Absolutely NO hashtags.
- DO NOT write {future} or {spot} tags.
""",

"short_signal": """
Write a SHORT SIGNAL post (Template C). Structure:
- Line 1: 🩸 [verb phrase] $COIN — Nx Leverage  (use 10x or 20x)
- Blank line
- "Entry: [price — at or slightly above current price]"
- "SL: [price — 3-5% above entry]"
- Blank line
- "TP1: [price — 3-5% below entry]"
- "TP2: [price — 7-10% below entry]"
- "TP3: [price — 12-18% below entry]"
- Blank line
- [1-2 lines: overbought RSI, rejection, exhaustion — use ACTUAL computed values]
- Blank line
- "‼️ Risk Management is Important."
- Blank line
- "Trade Here 👇🏻"

FORMATTING RULES:
- Each key component (Entry, SL, TP, ‼️, Trade Here) MUST start on a new line.
- Absolutely NO hashtags.
- DO NOT write {future} or {spot} tags.
""",

"thuchoang_style": """
Write a post in thuchoang90's style (Template D). Structure:
- Line 1: One-liner metaphor or personality observation about the coin's price action (witty, trader voice)
  Examples: "SOL is trapped in a boring range; fading the local resistance until the trend decides."
            "Gold is testing the MA99 ceiling; one clean breakout and we're printing green candles."
- Blank line
- "[Long/Short] play on $COIN." or "Interesting [long/short] on $COIN."
- Blank line
- "Entry: [price] - [price]"  (or "Entry point:" or "Execution:")
- "TP [price] | [price]"  or "Target: [price] / [price]"
- "SL [price]"
- Blank line
- [One-liner TA reason referencing MA/RSI from actual data]
- Blank line
- "Be careful: [one specific risk scenario with a price level]"
- "Never all-in, fam. Use a size that fits your own account."
- Blank line
- "Trade Here 👇🏻"

FORMATTING RULES:
- Each key component (Entry, TP, SL, Be careful, Never all-in, Trade Here) MUST start on a new line.
- Absolutely NO hashtags.
- DO NOT write {future} or {spot} tags.
""",

"scenario_analysis": """
Write a SCENARIO ANALYSIS post (Template E). Structure:
- Line 1: ‼️$COIN is [situation description at current price]
- Blank line
- "🟢 Bullish Scenario:"
- [2 lines of bullish case — what needs to hold, where it goes]
- Blank line
- "🔴 Bearish Scenario:"
- [2 lines of bearish case — what fails, where it goes]
- Blank line
- [1 line personal stance — "I'm waiting for confirmation rather than chasing"]
- Blank line
- "Trade Here 👇🏻"

FORMATTING RULES:
- Each key component (🟢, 🔴, Trade Here) MUST start on a new line.
- Absolutely NO hashtags.
- DO NOT write {future} or {spot} tags.
""",
}

# Weighted post type mix matching top earner ratios
POST_MIX = {
    "quick_narrative":   40,   # most common — 40% of posts
    "long_signal":       20,   # full signal long
    "short_signal":      15,   # full signal short
    "thuchoang_style":   15,   # dual-widget casual style
    "scenario_analysis": 10,   # bull/bear layout
}

# Secondary coins rotation for thuchoang_style dual-widget posts
SECONDARY_COINS = ["TRADOORUSDT", "AKEUSDT", "ESPORTSUSDT", "SKHYUSDT"]

# Legacy POST_TYPES list kept for non-signal post_type lookups (will not be used for content generation)
POST_TYPES = []

def _select_signal(rsi, macd, boll, vol) -> str:
    """Map technical indicators to the best post template."""
    if rsi is not None and rsi < 30:
        return "long_signal"        # oversold → long signal
    if rsi is not None and rsi > 70:
        return "short_signal"       # overbought → short signal
    if macd and macd.get("crossover") == "bullish":
        return "long_signal"
    if macd and macd.get("crossover") == "bearish":
        return "short_signal"
    if boll and boll.get("squeeze"):
        return "scenario_analysis"  # squeeze → both scenarios possible
    if vol and vol.get("trend") == "spike":
        return "quick_narrative"    # volume spike → quick take
    return "quick_narrative"        # default → quick narrative

def is_low_sentiment_regime(live_data) -> bool:
    """
    Checks if the Fear & Greed index is under 25.
    """
    if not live_data:
        return False
    fg = live_data.get("fg", {})
    val = fg.get("value")
    if val is None:
        return False
    try:
        val_str = str(val).strip()
        if val_str.isdigit():
            return int(val_str) < 25
    except Exception:
        pass
    return False

def get_macro_regulatory_headline(news_list: list) -> str | None:
    """
    Scans news items for regulatory updates or institutional catalysts.
    """
    keywords = [
        "MICA", "SEC", "ETF", "FED", "REGULATION", "COMPLIANCE", "CLARITY ACT", 
        "BLACKROCK", "JPMORGAN", "BNY MELLON", "FIDELITY", "INSTITUTIONAL", 
        "TREASURY", "CENTRAL BANK", "LEGISLATION", "CONGRESS"
    ]
    if not news_list or not isinstance(news_list, list):
        return None
    for item in news_list:
        title = item.get("title", "")
        if not title:
            continue
        title_upper = title.upper()
        cleaned_title = sanitize_text_token(title)
        if any(kw in title_upper for kw in keywords):
            return cleaned_title
    # Fallback to first headline
    first_title = news_list[0].get("title", "")
    return sanitize_text_token(first_title) if first_title else None

def generate_schedule_portfolio(n_posts: int, low_sentiment: bool = True) -> list[str]:
    """
    Structures the programmatic daily posting output based on POST_MIX weights
    derived from analysis of top Binance Square commission-earning profiles:
      - quick_narrative:   40%
      - long_signal:       20%
      - short_signal:      15%
      - thuchoang_style:   15%
      - scenario_analysis: 10%
    The low_sentiment parameter is accepted for compatibility but no longer changes the mix.
    """
    total_weight = sum(POST_MIX.values())
    ratios = [(name, weight / total_weight) for name, weight in POST_MIX.items()]
    portfolio = []
    temp_counts = {}
    allocated = 0
    for name, ratio in ratios:
        count = int(round(n_posts * ratio))
        temp_counts[name] = count
        allocated += count

    difference = n_posts - allocated
    if difference != 0:
        temp_counts["quick_narrative"] += difference
        if temp_counts["quick_narrative"] < 0:
            temp_counts["quick_narrative"] = 0

    for name, count in temp_counts.items():
        portfolio.extend([name] * count)

    random.shuffle(portfolio)
    return portfolio


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

# Note: HASHTAG_POOL removed — top earners use ZERO hashtags.
# Discovery is handled by the {future}(SYMBOLUSDT) widget appended to every post.

# ─────────────────────────────────────────────
# SANITIZATION, LAYOUT & OUTREACH HELPERS
# ─────────────────────────────────────────────

def restrict_math_operators(text: str) -> str:
    """
    Restrict mathematical rendering operators to text descriptions to avoid XML parser failures.
    """
    if not text:
        return ""
    text = text.replace("<=", " less than or equal to ")
    text = text.replace(">=", " greater than or equal to ")
    text = text.replace("<", " less than ")
    text = text.replace(">", " greater than ")
    import re
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def sanitize_text_token(text: str) -> str:
    """
    Enforce strict sanitization on incomplete or truncated text tokens from scraping.
    Strip trailing arrows/symbols, or drop malformed data points.
    """
    if not text:
        return ""
    text = text.strip()
    import re
    # Strip trailing arrows or broken transitions
    cleaned = re.sub(r'[\s➡️→\->=>\+\|]+$', '', text)
    if not cleaned or cleaned.strip() in ("$", "➡️", "→", "-", "->", "=>", "+", "*", "/", "\\", "|"):
        return ""
    return cleaned

def sanitize_metric(val) -> int:
    """
    Non-Numeric Metric Safeguards: If the 'views' or 'likes' parameters contain
    missing, non-numeric, or placeholder variables (e.g., "Error", "N/A"),
    automatically map them to an absolute numerical integer value of 0
    while maintaining the log execution stream status flag.
    """
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val)
    val_str = str(val).strip()
    if val_str.lower() in ("error", "n/a", "null", "none", ""):
        return 0
    try:
        return int(float(val_str))
    except ValueError:
        log.warning(f"⚠️ Non-numeric metric value encountered: '{val_str}'. Defaulted to 0.")
        return 0

def fetch_whale_flow_metrics() -> dict:
    """
    Provides institutional on-chain metrics, ETF outflow trends, large-scale token burns,
    and company balance sheet shifts for whale_flow_alerts posts.
    Uses a randomized pool to ensure content feels fresh across different post cycles.
    """
    burn_rates = {
        "BNB": random.choice([
            "125,000 BNB burned in the latest quarterly auto-burn cycle",
            "BNB auto-burn reduced supply by over 2 million BNB this quarter",
            "Quarterly BNB burn completes — total supply now below 150M tokens"
        ]),
        "ETH": random.choice([
            "2,450 ETH burned in the last 24h as network activity surges",
            "ETH deflationary pressure: over 3,100 ETH destroyed in 24h",
            "Post-merge ETH burn rate accelerates — net issuance goes negative"
        ]),
        "SOL": random.choice([
            "Increased fee burns on-chain matching DEX volume surge on Solana",
            "Solana validator fee pressure rising as transaction throughput spikes",
            "SOL on-chain activity hits 30-day high — fee burn rate climbing"
        ])
    }
    etf_flows = {
        "BTC": random.choice([
            "-$145M ETF net outflow today (Grayscale leading outflows, BlackRock inflows slowing)",
            "Bitcoin spot ETF records +$220M net inflow — BlackRock leads institutional buying",
            "US spot BTC ETFs see largest single-day outflow this quarter at -$310M",
            "ETF inflows reverse: +$180M net positive as institutional desks reenter"
        ]),
        "ETH": random.choice([
            "-$35M ETH ETF net outflow as sentiment turns cautious",
            "Ethereum ETF sees +$95M inflow week — Fidelity and BlackRock dominate flows",
            "ETH spot ETF net negative for 3rd consecutive day: -$55M outflow"
        ])
    }
    wallet_actions = [
        "Dormant Satoshi-era wallet (1,000 BTC) activated after 12 years",
        "JPMorgan internal ledger transferred 50,000 ETH to institutional custody",
        "BNY Mellon wallet added 5,200 BTC to balance sheet",
        "CryptoQuant reports whale exchange inflow reaches a 3-month low (holding pressure)",
        "Large unknown wallet moved 8,500 BTC off exchanges — cold storage signal",
        "Grayscale GBTC discount narrows to near-zero as institutional demand builds",
        "MicroStrategy added 2,500 BTC this week — total holdings now above 215,000 BTC",
        "On-chain data: exchange BTC reserves fall to a 5-year low — supply squeeze building",
        "CryptoQuant Exchange Whale Ratio drops sharply — whales are not selling",
        "Binance order book depth increases 40% — institutional accumulation pattern detected"
    ]
    return {
        "burn_rates": burn_rates,
        "etf_flows": etf_flows,
        "wallet_actions": wallet_actions
    }

def hoist_preview_keywords(content: str) -> str:
    """
    Extract key institutional entity keywords and position them within the first 150 characters
    of the layout string to optimize click-through rates within mobile feed preview windows.
    """
    import re
    entities = [
        "Fed", "MiCA", "BlackRock", "Vitalik", "Clarity Act", "JPMorgan", "BNY Mellon",
        "Ethereum", "Bitcoin", "SEC", "Fidelity", "Grayscale", "Binance", "Coinbase",
        "ETF", "CFTC", "Treasury", "Congress", "Solana", "Ripple"
    ]
    found_entities = []
    for ent in entities:
        if re.search(r'\b' + re.escape(ent) + r'\b', content, re.IGNORECASE):
            found_entities.append(ent)

    if found_entities:
        first_150 = content[:150]
        entity_in_preview = any(ent.lower() in first_150.lower() for ent in found_entities)
        if not entity_in_preview:
            # Hoist up to 2 most important entities
            hoist_prefix = f"🔥 {', '.join(found_entities[:2])}: "
            content = hoist_prefix + content
    return content

def format_layout_scannability(content: str) -> str:
    """
    Ban long dense prose blocks. Refactor output to inject single line breaks between punchy sentences.
    Preserves list formatting and metrics blockquotes.
    """
    lines = content.split('\n')
    scannable_lines = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        if line.startswith(('•', '-', '*', '>', '$')) or any(char.isdigit() for char in line[:3]):
            scannable_lines.append(line)
        else:
            import re
            sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z$#])', line)
            for s in sentences:
                s = s.strip()
                if s:
                    scannable_lines.append(s)
    return "\n".join(scannable_lines)

def tag_metadata(content: str, coin_tag: str) -> str:
    """
    Strips any hashtags that Gemini may have injected into the post body.
    Top earners use ZERO hashtags — discovery comes from the {future} widget, not tags.
    """
    import re
    # Strip any existing hashtags from the content body
    content_clean = re.sub(r'#\w+', '', content).strip()
    return content_clean


def format_post(content: str, coin: dict, post_type: str) -> tuple[str, list[dict]]:
    import re

    # ── 1. Strip hashtags ──
    content = re.sub(r'#\w+', '', content)

    # ── 2. Remove any {future}/{spot} Gemini wrote (we add them ourselves) ──
    content = re.sub(r'\{future\}\s*\(?\w*\)?', '', content)
    content = re.sub(r'\{spot\}\s*\(?\w*\)?', '', content)

    # ── 3. Force line breaks before key signal keywords ──
    # These should ALWAYS start on their own line
    force_newline_before = [
        r'(Entry\s*:)',
        r'(Be careful\s*:)',
        r'(Never all-in)',
        r'(Target\s)',
        r'(SL\s:)',
        r'(TP1\s:)',
        r'(TP2\s:)',
        r'(TP3\s:)',
        r'(TP4\s:)',
        r'(RSI\s:)',
        r'(EMA\s:)',
        r'(🟢\s*Bull)',
        r'(🔴\s*Bear)',
        r'(‼️\s*Risk)'
    ]
    for pattern in force_newline_before:
        content = re.sub(r'[ \t]*' + pattern, r'\n\n\1', content)

    # ── 4. Strip trailing spaces from every line ──
    lines = [line.rstrip() for line in content.splitlines()]

    # ── 5. Collapse 3+ blank lines into 1 blank line ──
    cleaned = []
    blank_streak = 0
    for line in lines:
        if line == '':
            blank_streak += 1
            if blank_streak == 1:
                cleaned.append('')
        else:
            blank_streak = 0
            cleaned.append(line)

    text = '\n'.join(cleaned).strip()

    # ── 6. Build widget block (updated for widgets API) ──
    main_sym = coin["symbol"] + "USDT"  # e.g. "DOTUSDT"
    widgets = []

    secondary_sym = random.choice(SECONDARY_COINS)
    secondary_tag = "$" + secondary_sym.replace("USDT", "")
    
    # Append secondary tag text to the content as requested
    text = text.rstrip() + f"\n\n${coin["symbol"]}" +f"\n\n{secondary_tag}\n"
    
    widgets.append({
        "type": "candle_chart",
        "symbol": main_sym,
        "interval": "1h"
    })


    # ── 7. Final pass — strip trailing spaces on every line ──
    final = '\n'.join(line.rstrip() for line in text.splitlines())

    return final, widgets


def process_post_layout(content: str, coin_tag: str) -> str:
    """
    Complete pipeline processing for scannability, math operators sanitization, and hashtag removal.
    Widget appending is handled separately by append_widget().
    """
    if not content:
        return ""
    # Restrict math operators
    content = restrict_math_operators(content)
    # Strip any hashtags Gemini may have injected
    content = tag_metadata(content, coin_tag)
    return content


def append_widget(content: str, coin: dict, post_type: str) -> str:
    """
    Appends the {future}(SYMBOLUSDT) widget as a post-processing step AFTER Gemini generates text.
    For thuchoang_style posts, adds a secondary coin widget (dual-widget format).
    This is the single most important structural element for Binance Square discovery.
    """
    symbol = coin["symbol"] + "USDT"  # e.g., "BTC" -> "BTCUSDT"
    widget = f"{{future}}({symbol})"

    # Only add widget(s) if not already present (safety check)
    if "{future}" not in content and "{spot}" not in content:
        if post_type == "thuchoang_style":
            secondary = random.choice(SECONDARY_COINS)
            secondary_tag = f"${secondary.replace('USDT', '')}"
            content = content.rstrip() + f"\n\n{secondary_tag}\n\ud83d\udc47\ud83d\udc47\ud83d\udc47\n{widget}\n{{future}}({secondary})"
        else:
            content = content.rstrip() + f"\n{widget}"
    return content

def find_trending_coin_anomaly(live_data, fetcher) -> dict:
    """
    Programmatically isolate assets exhibiting relative strength divergence against market baseline.
    """
    market = live_data.get("market", {})
    baseline_changes = []
    for symbol in ["bitcoin", "ethereum", "solana"]:
        c_data = market.get(symbol, {})
        ch = c_data.get("change_24h")
        if ch is not None:
            baseline_changes.append(ch)
    avg_baseline = sum(baseline_changes) / len(baseline_changes) if baseline_changes else 0.0
    
    candidates = []
    for coin in COINS:
        cg_id = coin["cg_id"]
        c_data = market.get(cg_id, {})
        if not c_data:
            continue
        ch24 = c_data.get("change_24h")
        price = c_data.get("price")
        if ch24 is None or price is None:
            continue
            
        holding_ema = False
        try:
            klines = fetcher.fetch_klines(coin["symbol"])
            if klines:
                # Need to use calculate_indicators helper
                indicators = calculate_indicators(klines)
                if indicators:
                    ema20 = indicators.get("ema20")
                    ema50 = indicators.get("ema50")
                    if ema20 and price > ema20:
                        holding_ema = True
                    elif ema50 and price > ema50:
                        holding_ema = True
        except Exception as e:
            log.warning(f"Error checking indicators for {coin['symbol']} anomaly: {e}")
            
        if (ch24 > 0 and avg_baseline < 0) or holding_ema:
            divergence_score = ch24 - avg_baseline
            candidates.append((coin, divergence_score))
            
    if candidates:
        candidates.sort(key=lambda x: x[1], reverse=True)
        chosen_coin = candidates[0][0]
        log.info(f"🔥 Isolated relative strength anomaly: {chosen_coin['tag']} with score {candidates[0][1]:.2f}")
        return chosen_coin
        
    return random.choice(COINS)


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

    # ── cryptocurrency.cv was removed because it was non-functional/blocked. Fetching from working GitHub raw JSON source ──
    def fetch_news_headlines(self, limit: int = 15) -> list[str]:
        try:
            news_list = fetch_cryptopanic_news()
            if not news_list or not isinstance(news_list, list):
                return []
            headlines = []
            for item in news_list[:limit]:
                title = item.get("title", "").strip()
                cleaned_title = sanitize_text_token(title)
                if not cleaned_title:
                    continue
                source = item.get("source", "unknown")
                sentiment = item.get("sentiment", "neutral")
                headlines.append(f"{cleaned_title} (Source: {source}, Sentiment: {sentiment})")
            log.info(f"  📰 Fetched {len(headlines)} news headlines (cleaned) from GitHub source")
            return headlines
        except Exception as e:
            log.warning(f"Failed to fetch news headlines from GitHub: {e}")
            return []


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

    # General news headlines from live data block
    news = global_data.get("news", [])
    if news:
        lines.append("\nTop Crypto Headlines Right Now:")
        for h in news[:10]:
            lines.append(f"  • {h}")

    lines.append("=" * 50)
    return "\n".join(lines)


# ─────────────────────────────────────────────
# GEMINI PROMPT BUILDER
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """You are a professional crypto trader posting on Binance Square.
You write like the top commission-earning KOLs: confident, specific, personal, short.

VOICE: First person. "I'm watching", "I'm taking", "I'm staying bearish".
Real trader — not an AI, not a newsletter, not a bot.

STRUCTURE RULES:
1. Follow the template given EXACTLY — spacing, line breaks, emoji placement.
2. Use REAL numbers from the live data provided — never invent prices.
3. For entry zones: set 1-2% range around current price. For SL: 4-6% away. For TPs: realistic increments.
4. Keep it SHORT — 150-350 chars for narratives, up to 500 for full signals.
5. ZERO hashtags — none at all.
6. Do NOT add {future} or {spot} widgets — those are added automatically after you write.
7. End signals with "Trade Here \ud83d\udc47\ud83c\udffb" on its own line.
8. Emojis: use 1-3 max. Only opening emoji + maybe one closing. Never mid-sentence.
9. NEVER use mathematical comparison symbols like '<' or '>'. Write them out (e.g. 'less than', 'greater than').
10. Output ONLY the post text. No explanation, no preamble."""


def build_user_prompt(post_type_name: str, coin: dict, live_data_block: str,
                      recent_coins: list, recent_types: list, macro_headline: str = None) -> str:
    """Builds the Gemini user prompt using the new POST_INSTRUCTIONS templates."""
    recent_coins_str = ", ".join(recent_coins[-5:]) if recent_coins else "none"
    recent_types_str = ", ".join(recent_types[-3:]) if recent_types else "none"

    template_instructions = POST_INSTRUCTIONS.get(post_type_name, POST_INSTRUCTIONS["quick_narrative"])

    return f"""{live_data_block}

PRIMARY COIN: {coin['tag']}
AVOID these coins (used recently): {recent_coins_str}
AVOID these post types (used recently): {recent_types_str}

POST TEMPLATE TO FOLLOW:
{template_instructions}

Write the post now. Output ONLY the post body text, nothing else.
ZERO hashtags. Do NOT write {{future}} or {{spot}} — those are auto-appended."""


# ─────────────────────────────────────────────
# GEMINI CONTENT GENERATOR
# ─────────────────────────────────────────────

def generate_post(client: genai.Client, post_type_name: str, coin: dict,
                  live_data_block: str, recent_coins: list, recent_types: list, macro_headline: str = None) -> str:
    prompt = build_user_prompt(post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline)

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
    if not response.text:
        raise ValueError(f"Generation failed: response.text is empty or blocked by safety filters. Full response: {response}")
    return response.text.strip()


def generate_post_with_gemma(client: genai.Client, post_type_name: str, coin: dict,
                             live_data_block: str, recent_coins: list, recent_types: list, macro_headline: str = None) -> str:
    """Fallback generator using gemma-4-26b-a4b-it if Gemini keys are exhausted."""
    prompt = build_user_prompt(post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline)
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
    if not response.text:
        raise ValueError(f"Gemma 4 generation failed: response.text is empty or blocked. Full response: {response}")
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

def post_to_binance_square(content: str, image_urls: list = None, widgets: list = None) -> dict:
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

    if widgets:
        payload["widgets"] = widgets

    response = requests.post(
        BINANCE_POST_ENDPOINT,
        headers=headers,
        json=payload,
        timeout=15
    )
    resp_json = response.json()
    print(f"📨 DEBUG: API RESPONSE: {json.dumps(resp_json, indent=2, ensure_ascii=False)}")
    return resp_json


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
        sleep_chunk = min(1, remaining)
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
    morning_start = cycle_end.replace(hour=6, minute=0, second=0, microsecond=0)
    morning_end = cycle_end.replace(hour=9, minute=0, second=0, microsecond=0)

    timestamps_ist = []
    
    if now_ist < morning_end:
        n_morning = int(n_posts * 0.30)
        n_other = n_posts - n_morning
        
        actual_morning_start = max(now_ist, morning_start)
        morning_duration = (morning_end - actual_morning_start).total_seconds()
        
        for _ in range(n_morning):
            if morning_duration > 0:
                timestamps_ist.append(actual_morning_start + timedelta(seconds=random.randint(0, int(morning_duration))))
            else:
                timestamps_ist.append(now_ist)
                
        # Distribute remaining 70% outside the morning window
        valid_ranges = []
        if now_ist < morning_start:
            valid_ranges.append((now_ist, morning_start))
        if morning_end < cycle_end:
            valid_ranges.append((max(now_ist, morning_end), cycle_end))
            
        for _ in range(n_other):
            if valid_ranges:
                total_dur = sum((r[1] - r[0]).total_seconds() for r in valid_ranges)
                if total_dur > 0:
                    r = random.uniform(0, total_dur)
                    for start_dt, end_dt in valid_ranges:
                        dur = (end_dt - start_dt).total_seconds()
                        if r <= dur:
                            timestamps_ist.append(start_dt + timedelta(seconds=random.randint(0, int(dur))))
                            break
                        r -= dur
                else:
                    timestamps_ist.append(now_ist)
            else:
                timestamps_ist.append(now_ist)
    else:
        # Past the morning window, distribute uniformly
        for _ in range(n_posts):
            timestamps_ist.append(now_ist + timedelta(seconds=random.randint(0, int(remaining_seconds))))
            
    timestamps_ist.sort()
    
    log.info(f"📅 Daily session: {n_posts} posts scheduled across remaining active window")
    log.info(f"   First post: {timestamps_ist[0].strftime('%I:%M %p IST')}")
    log.info(f"   Last post:  {timestamps_ist[-1].strftime('%I:%M %p IST')}")

    low_sentiment = is_low_sentiment_regime(live_data)
    types_portfolio = generate_schedule_portfolio(n_posts, low_sentiment=low_sentiment)

    with state_lock:
        bot_state["n_posts_scheduled"] = n_posts
        bot_state["schedule"] = [
            {
                "time": s.strftime("%I:%M %p IST"),
                "time_iso": s.isoformat(),
                "status": "Pending",
                "coin": "Pending",
                "type": types_portfolio[i]
            } for i, s in enumerate(timestamps_ist)
        ]
    save_bot_state()
    
    execute_active_schedule()

def get_cycle_start(dt_ist):
    # If the time is before 10:00 AM, the cycle started yesterday at 2:00 PM
    limit_time = dt_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    if dt_ist < limit_time:
        yesterday = dt_ist - timedelta(days=1)
        return yesterday.replace(hour=14, minute=0, second=0, microsecond=0)
    else:
        return dt_ist.replace(hour=14, minute=0, second=0, microsecond=0)

def get_cycle_end(dt_ist):
    # If the time is before 10:00 AM, the cycle ends today at 10:00 AM
    limit_time = dt_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    if dt_ist < limit_time:
        return dt_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    else:
        tomorrow = dt_ist + timedelta(days=1)
        return tomorrow.replace(hour=10, minute=0, second=0, microsecond=0)


def execute_active_schedule():
    global bot_state
    
    session_cycle_end = get_cycle_end(get_ist_now())
    
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
        if now_ist >= session_cycle_end:
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
        if now_ist >= session_cycle_end:
            log.info("⏰ Posting cycle end reached after sleep. Terminating daily schedule execution.")
            break


        # Refresh live data periodically
        if next_item_idx > 0 and next_item_idx % DATA_REFRESH_EVERY == 0:
            try:
                live_data = fetcher.refresh_all()
            except Exception as e:
                log.warning(f"Live data refresh failed: {e}")

        # Pick post type name from schedule or derive from indicators
        assigned_type_name = item.get("type", "Pending")
        if assigned_type_name and assigned_type_name != "Pending" and assigned_type_name in POST_INSTRUCTIONS:
            post_type_name = assigned_type_name
        else:
            # 15% random chance to force thuchoang_style for variety
            if random.random() < 0.15:
                post_type_name = "thuchoang_style"
            else:
                # Try to derive from indicators for the coin
                try:
                    klines_pre = fetcher.fetch_klines(next((c for c in COINS if c["tag"] not in recent_coins[-4:]), COINS[0])["symbol"])
                    indicators_pre = calculate_indicators(klines_pre) if klines_pre else {}
                    rsi_pre = indicators_pre.get("rsi")
                    macd_pre = {"crossover": "bullish" if (indicators_pre.get("macd_hist", 0) or 0) > 0 else "bearish"}
                    post_type_name = _select_signal(rsi_pre, macd_pre, None, None)
                except Exception:
                    post_type_name = random.choices(
                        list(POST_MIX.keys()), weights=list(POST_MIX.values()), k=1
                    )[0]

        # Pick coin (avoid recently used)
        coin = None
        with state_lock:
            available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
        coin = random.choice(available_coins if available_coins else COINS)

        with state_lock:
            bot_state["schedule"][next_item_idx]["coin"] = coin["tag"]
            bot_state["schedule"][next_item_idx]["type"] = post_type_name
        with state_lock:
            bot_state["schedule"][next_item_idx]["status"] = "Generating"
        save_bot_state()

        macro_headline = None  # kept for API compat; not used in new templates

        # Build live data block
        live_data_block = format_coin_data(coin, live_data.get("market", {}), fetcher, live_data)

        # Generate content with Gemini
        content = None
        failed_keys_count = 0
        max_attempts = len(GEMINI_API_KEYS)
        while failed_keys_count < max_attempts:
            try:
                current_client = rotator.get_client()
                log.info(f"🤖 Generating [{post_type_name}] post about {coin['tag']} (Key {rotator.current_index + 1}/{len(GEMINI_API_KEYS)}, attempt {failed_keys_count + 1}/{max_attempts})...")
                content = generate_post(current_client, post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline=macro_headline)

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
                        content = generate_post_with_gemma(current_client, post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline=macro_headline)
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

        content = restrict_math_operators(content)
        if not content:
            continue

        content, widgets = format_post(content, coin, post_type_name)
        klines = fetcher.fetch_klines(coin["symbol"])

        # Image generation/upload
        image_urls = []
        try:
            image_bytes = None
            uploader = ImageUploader(BINANCE_SQUARE_KEY)
            log.info(f"   📊 Generating advanced chart for {coin['symbol']}...")
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
            result = post_to_binance_square(content, image_urls, widgets=widgets)

            if result.get("code") == "000000":
                post_id = result.get("data", {}).get("id", "unknown")
                post_url = f"https://www.binance.com/square/post/{post_id}"
                fail_streak = 0
                log.info(f"   ✅ Success! Post #{bot_state['posts_published'] + 1} → {post_url}")
                log.info(f"   Content preview: {content[:80]}...")

                with state_lock:
                    recent_coins.append(coin["tag"])
                    recent_types.append(post_type_name)
                    if len(recent_coins) > 10:
                        recent_coins.pop(0)
                    if len(recent_types) > 6:
                        recent_types.pop(0)
                        
                    bot_state["schedule"][next_item_idx]["status"] = "Published"
                    bot_state["posts_published"] += 1
                add_recent_post(post_type_name, content, "Success", post_url)
            else:
                error_code = result.get("code", "unknown")
                error_msg  = result.get("message", "no message")
                log.warning(f"   ⚠️ Binance rejected post. Code: {error_code} | {error_msg}")

                with state_lock:
                    bot_state["schedule"][next_item_idx]["status"] = f"Rejected"
                    bot_state["posts_failed"] += 1
                add_recent_post(post_type_name, content[:80] + "...", f"Rejected ({error_code})", "#")


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
        setInterval(fetchStatus, 1000);
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
            "recent_posts": recent_posts_cache,
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
        
        # Pick post type using POST_MIX weights (15% chance of thuchoang_style)
        recent_coins = bot_state["recent_coins"]
        recent_types = bot_state["recent_types"]

        if random.random() < 0.15:
            post_type_name = "thuchoang_style"
        else:
            post_type_name = random.choices(
                list(POST_MIX.keys()), weights=list(POST_MIX.values()), k=1
            )[0]

        # Pick coin (avoid recently used)
        available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
        coin = random.choice(available_coins if available_coins else COINS)

        macro_headline = None  # not used in new templates

        log.info(f"🤖 Manual trigger: Generating [{post_type_name}] post about {coin['tag']}...")

        # Build live data block for this specific coin
        live_data_block = format_coin_data(coin, live_data.get("market", {}), fetcher, live_data)

        content = None
        failed_keys_count = 0
        max_attempts = len(GEMINI_API_KEYS)
        while failed_keys_count < max_attempts:
            try:
                current_client = rotator.get_client()
                content = generate_post(current_client, post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline=macro_headline)
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
                        content = generate_post_with_gemma(current_client, post_type_name, coin, live_data_block, recent_coins, recent_types, macro_headline=macro_headline)
                    except Exception as g_err:
                        log.error(f"   Manual trigger Gemma 4 fallback failed: {g_err}")
                        raise e

        if not content:
            raise ValueError("Failed to generate content after all API key attempts.")

        # Sanitize and format post layout
        content = restrict_math_operators(content)
        if not content:
            raise ValueError("Sanitized post content is empty.")

        content, widgets = format_post(content, coin, post_type_name)

        # Fetch klines for image generation
        klines = fetcher.fetch_klines(coin["symbol"])

        # Determine image to generate/retrieve and upload to Binance Square
        image_urls = []
        try:
            image_bytes = None
            uploader = ImageUploader(BINANCE_SQUARE_KEY)

            log.info(f"   📊 Generating advanced chart for {coin['symbol']}...")
            image_bytes = generate_advanced_chart(coin["symbol"], klines)

            if image_bytes:
                cdn_url = uploader.upload(image_bytes)
                if cdn_url:
                    image_urls.append(cdn_url)
        except Exception as img_err:
            log.warning(f"   ⚠️ Manual trigger image upload workflow failed: {img_err}. Posting as text-only.")

        log.info(f"📤 Manual trigger: Posting to Binance Square...")
        result = post_to_binance_square(content, image_urls, widgets=widgets)

        if result.get("code") == "000000":
            post_id = result.get("data", {}).get("id", "unknown")
            post_url = f"https://www.binance.com/square/post/{post_id}"

            with state_lock:
                recent_coins.append(coin["tag"])
                recent_types.append(post_type_name)
                if len(recent_coins) > 10:
                    recent_coins.pop(0)
                if len(recent_types) > 6:
                    recent_types.pop(0)

                bot_state["posts_published"] += 1
            add_recent_post(post_type_name, content, "Success", post_url)
            save_bot_state()

            log.info(f"   ✅ Manual success! Post → {post_url}")
            return jsonify({"success": True, "url": post_url, "content": content})
        else:
            error_code = result.get("code", "unknown")
            error_msg = result.get("message", "no message")
            log.warning(f"   ⚠️ Manual trigger rejected. Code: {error_code} | {error_msg}")

            with state_lock:
                bot_state["posts_failed"] += 1
            add_recent_post(post_type_name, content[:80] + "...", f"Rejected ({error_code})", "#")
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