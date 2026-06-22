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
import threading
import sys
from datetime import datetime, timedelta
from google import genai
from google.genai import types
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template_string

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
load_dotenv()

GEMINI_API_KEY        = os.getenv("GEMINI_API_KEY")
BINANCE_SQUARE_KEY    = os.getenv("BINANCE_SQUARE_KEY")
BINANCE_POST_ENDPOINT = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"

DATA_REFRESH_EVERY = 6

POSTS_PER_DAY_MIN = 80
POSTS_PER_DAY_MAX = 90

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

bot_state = {
    "status": "Starting up",
    "start_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"),
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
# POST TYPE TEMPLATES
# These define the 9 rotating post formats.
# Gemini picks content — the format varies the structure.
# ─────────────────────────────────────────────
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
    "#crypto", "#BinanceSquare", "#Write2Earn",
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

    COINGECKO_BASE   = "https://api.coingecko.com/api/v3"
    CRYPTONEWS_BASE  = "https://cryptocurrency.cv/api"

    def __init__(self):
        self._cache: dict = {}
        self._cache_time: dict = {}
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        })

    def _get(self, url: str, params: dict = None, timeout: int = 10) -> dict | None:
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
            f"{self.COINGECKO_BASE}/coins/markets",
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
        data = self._get(f"{self.COINGECKO_BASE}/search/trending")
        if not data:
            return []
        coins = data.get("coins", [])[:7]
        return [c["item"]["symbol"].upper() for c in coins]

    # ── cryptocurrency.cv: latest news headlines (no key needed) ──
    def fetch_news_headlines(self, limit: int = 15) -> list[str]:
        data = self._get(f"{self.CRYPTONEWS_BASE}/news", params={"limit": limit})
        if not data or "articles" not in data:
            return []
        headlines = []
        for a in data["articles"]:
            title = a.get("title", "").strip()
            if title:
                headlines.append(title)
        log.info(f"  📰 Fetched {len(headlines)} news headlines")
        return headlines

    # ── cryptocurrency.cv: Fear & Greed Index (no key needed) ──
    def fetch_fear_greed(self) -> dict:
        data = self._get(f"{self.CRYPTONEWS_BASE}/fear-greed")
        if not data or "current" not in data:
            return {"value": "N/A", "classification": "Unknown"}
        curr = data["current"]
        return {
            "value":          curr.get("value", "N/A"),
            "classification": curr.get("valueClassification", "Unknown"),
        }

    # ── Master refresh: called every N posts ──
    def refresh_all(self) -> dict:
        log.info("🔄 Refreshing live market data...")
        time.sleep(1)  # polite delay for free APIs
        return {
            "market":   self.fetch_market_data(),
            "trending": self.fetch_trending(),
            "news":     self.fetch_news_headlines(),
            "fg":       self.fetch_fear_greed(),
            "fetched_at": datetime.utcnow().strftime("%H:%M UTC"),
        }


# ─────────────────────────────────────────────
# DATA FORMATTER → turns raw data into a
# compact text block injected into Gemini prompt
# ─────────────────────────────────────────────

def format_coin_data(coin: dict, market: dict, fetcher: LiveDataFetcher,
                     global_data: dict) -> str:
    """Returns a compact real-time data block for the selected coin."""
    cg_id  = coin["cg_id"]
    tag    = coin["tag"]

    lines = [f"=== LIVE MARKET DATA (as of {global_data.get('fetched_at', 'now')}) ==="]

    # Price data
    m = global_data["market"].get(cg_id, {})
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
            if n is None: return "N/A"
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
        lines.append(f"Market Cap: {fmt_large(mcap)}")
    else:
        lines.append(f"Coin: {tag}  |  Price data unavailable")

    # Fear & Greed
    fg = global_data.get("fg", {})
    lines.append(f"\nMarket Sentiment: Fear & Greed = {fg.get('value', 'N/A')} "
                 f"({fg.get('classification', 'Unknown')})")

    # Trending
    trending = global_data.get("trending", [])
    if trending:
        lines.append(f"Currently Trending: {', '.join(trending[:5])}")

    # General news headlines
    news = global_data.get("news", [])
    if news:
        lines.append("\nTop Crypto Headlines Right Now:")
        for h in news[:6]:
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
            max_output_tokens=5000,
        )
    )
    return response.text.strip()


# ─────────────────────────────────────────────
# BINANCE SQUARE POSTER
# ─────────────────────────────────────────────

def post_to_binance_square(content: str) -> dict:
    headers = {
        "X-Square-OpenAPI-Key": BINANCE_SQUARE_KEY,
        "Content-Type": "application/json",
        "clienttype": "web",
    }
    payload = {"bodyTextOnly": content}

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

def build_daily_schedule(n_posts: int) -> list:
    """
    Returns list of datetime objects for when to post today,
    spread irregularly across the active window.
    """
    now = datetime.utcnow()
    window_start = now.replace(hour=6, minute=0, second=0, microsecond=0)
    window_end   = now.replace(hour=23, minute=0, second=0, microsecond=0)

    if now > window_start:
        window_start = now + timedelta(seconds=30)

    total_seconds = int((window_end - window_start).total_seconds())
    if total_seconds <= 0:
        log.warning("Active window already passed. Will post immediately.")
        return [now + timedelta(seconds=i * 60) for i in range(n_posts)]

    # Pick n_posts random timestamps within window, then sort
    timestamps = sorted([
        window_start + timedelta(seconds=random.randint(0, total_seconds))
        for _ in range(n_posts)
    ])
    return timestamps


# ─────────────────────────────────────────────
# MAIN RUNNER
# ─────────────────────────────────────────────

def run_daily_session():
    global bot_state
    if not GEMINI_API_KEY or not BINANCE_SQUARE_KEY:
        raise ValueError(
            "Missing API keys. Set GEMINI_API_KEY and BINANCE_SQUARE_KEY in your .env file."
        )

    gemini_client = genai.Client(api_key=GEMINI_API_KEY)
    fetcher       = LiveDataFetcher()
    
    # Initial data fetch
    try:
        live_data = fetcher.refresh_all()
    except Exception as e:
        log.warning(f"Initial live data fetch failed: {e}. Will retry during postings.")
        live_data = {"market": {}, "trending": [], "news": [], "fg": {}, "fetched_at": "N/A"}

    n_posts = random.randint(POSTS_PER_DAY_MIN, POSTS_PER_DAY_MAX)
    schedule = build_daily_schedule(n_posts)

    log.info(f"📅 Daily session: {n_posts} posts scheduled across today's active window")
    log.info(f"   First post: {schedule[0].strftime('%H:%M UTC')}")
    log.info(f"   Last post:  {schedule[-1].strftime('%H:%M UTC')}")

    with state_lock:
        bot_state["n_posts_scheduled"] = n_posts
        bot_state["schedule"] = [
            {
                "time": s.strftime("%H:%M UTC"),
                "status": "Pending",
                "coin": "Pending",
                "type": "Pending"
            } for s in schedule
        ]

    recent_coins = bot_state["recent_coins"]
    recent_types = bot_state["recent_types"]
    post_count   = 0
    fail_streak  = 0

    for idx, scheduled_time in enumerate(schedule):
        # Refresh live data every N posts
        if idx > 0 and idx % DATA_REFRESH_EVERY == 0:
            try:
                live_data = fetcher.refresh_all()
            except Exception as e:
                log.warning(f"Live data refresh failed: {e}")

        # Wait until scheduled time
        now = datetime.utcnow()
        wait_sec = (scheduled_time - now).total_seconds()
        
        if wait_sec > 0:
            log.info(f"⏳ Next post in {format_interval(int(wait_sec))} "
                     f"(post {idx + 1}/{n_posts})")
            
            # Responsive wait loop
            remaining = wait_sec
            while remaining > 0:
                with state_lock:
                    bot_state["status"] = f"Waiting for post {idx + 1}/{n_posts} at {scheduled_time.strftime('%H:%M UTC')} (in {format_interval(int(remaining))})"
                sleep_chunk = min(10, remaining)
                time.sleep(sleep_chunk)
                remaining -= sleep_chunk

        # Pick post type and coin — avoid recent repeats
        with state_lock:
            bot_state["status"] = f"Generating post {idx + 1}/{n_posts}"
            available_types = [t for t in POST_TYPES if t["name"] not in recent_types[-2:]]
            post_type = random.choice(available_types if available_types else POST_TYPES)

            # Pick coin — for trending/news types, try to use a trending coin
            if post_type["name"] == "trending_coin_take" and live_data.get("trending"):
                trending_syms = live_data["trending"]
                matching_coins = [c for c in COINS if c["symbol"] in trending_syms
                                  and c["tag"] not in recent_coins[-4:]]
                coin = random.choice(matching_coins if matching_coins else COINS)
            else:
                available_coins = [c for c in COINS if c["tag"] not in recent_coins[-4:]]
                coin = random.choice(available_coins if available_coins else COINS)
            
            bot_state["schedule"][idx]["coin"] = coin["tag"]
            bot_state["schedule"][idx]["type"] = post_type["name"]
            bot_state["schedule"][idx]["status"] = "Generating"

        # Build live data block for this specific coin
        live_data_block = format_coin_data(coin, live_data.get("market", {}), fetcher, live_data)

        # Generate content with Gemini
        content = None
        try:
            log.info(f"🤖 Generating [{post_type['name']}] post about {coin['tag']}...")
            content = generate_post(gemini_client, post_type, coin, live_data_block, recent_coins, recent_types)

            # Basic sanity check — reject if too long or too short
            word_count = len(content.split())
            if word_count < 8:
                log.warning(f"   Post too short ({word_count} words), skipping.")
                with state_lock:
                    bot_state["schedule"][idx]["status"] = "Skipped (Too Short)"
                continue
            if word_count > 180:
                log.warning(f"   Post too long ({word_count} words), truncating.")
                content = " ".join(content.split()[:160]) + "..."

        except Exception as e:
            log.error(f"   Gemini error: {e}")
            with state_lock:
                bot_state["schedule"][idx]["status"] = f"Gemini Error"
                bot_state["posts_failed"] += 1
            fail_streak += 1
            if fail_streak >= 3:
                log.error("   3 consecutive Gemini failures — pausing 5 minutes.")
                time.sleep(300)
                fail_streak = 0
            continue

        # Post to Binance Square
        try:
            with state_lock:
                bot_state["status"] = f"Posting post {idx + 1}/{n_posts} to Binance..."
                bot_state["schedule"][idx]["status"] = "Posting"
                
            log.info(f"📤 Posting to Binance Square...")
            result = post_to_binance_square(content)

            if result.get("code") == "000000":
                post_id = result.get("data", {}).get("id", "unknown")
                post_url = f"https://www.binance.com/square/post/{post_id}"
                post_count += 1
                fail_streak = 0
                log.info(f"   ✅ Success! Post #{post_count} → {post_url}")
                log.info(f"   Content preview: {content[:80]}...")

                # Track recency
                with state_lock:
                    recent_coins.append(coin["tag"])
                    recent_types.append(post_type["name"])
                    if len(recent_coins) > 10:
                        recent_coins.pop(0)
                    if len(recent_types) > 6:
                        recent_types.pop(0)
                        
                    bot_state["schedule"][idx]["status"] = "Published"
                    bot_state["posts_published"] += 1
                    bot_state["recent_posts"].insert(0, {
                        "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
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
                    bot_state["schedule"][idx]["status"] = f"Rejected"
                    bot_state["posts_failed"] += 1
                    bot_state["recent_posts"].insert(0, {
                        "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
                        "content": content[:80] + "...",
                        "status": f"Rejected ({error_code})",
                        "url": "#"
                    })
                    if len(bot_state["recent_posts"]) > 10:
                        bot_state["recent_posts"].pop()

                # Handle known error codes
                if error_code in ("10001", "20001"):
                    log.error("   Invalid or missing API key. Check BINANCE_SQUARE_KEY.")
                    return  # Fatal — stop session
                elif error_code == "40003":
                    log.warning("   Daily post limit hit. Stopping for today.")
                    break
                elif error_code == "50001":
                    log.warning("   Sensitive content detected. Skipping this post.")
                elif error_code == "30001":
                    log.error("   Account banned. Contact Binance support.")
                    return  # Fatal

        except requests.exceptions.RequestException as e:
            log.error(f"   Network error posting to Binance Square: {e}")
            with state_lock:
                bot_state["schedule"][idx]["status"] = "Network Error"
                bot_state["posts_failed"] += 1
            fail_streak += 1

    with state_lock:
        bot_state["status"] = "Daily session complete"
    log.info(f"\n🏁 Session complete. {post_count}/{n_posts} posts published successfully.")


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
                        <span class="timeline-time">--:-- UTC</span>
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
                        
                        el.innerHTML = `
                            <div class="post-header">
                                <span class="post-time">${post.time}</span>
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
    if not GEMINI_API_KEY or not BINANCE_SQUARE_KEY:
        return jsonify({"success": False, "error": "API keys not configured"}), 400

    try:
        gemini_client = genai.Client(api_key=GEMINI_API_KEY)
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
        
        available_types = [t for t in POST_TYPES if t["name"] not in recent_types[-2:]]
        post_type = random.choice(available_types if available_types else POST_TYPES)

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
        
        content = generate_post(gemini_client, post_type, coin, live_data_block, recent_coins, recent_types)
        
        log.info(f"📤 Manual trigger: Posting to Binance Square...")
        result = post_to_binance_square(content)

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
                    "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
                    "content": content,
                    "status": "Success",
                    "url": post_url
                })
                if len(bot_state["recent_posts"]) > 10:
                    bot_state["recent_posts"].pop()
            
            log.info(f"   ✅ Manual success! Post → {post_url}")
            return jsonify({"success": True, "url": post_url, "content": content})
        else:
            error_code = result.get("code", "unknown")
            error_msg = result.get("message", "no message")
            log.warning(f"   ⚠️ Manual trigger rejected. Code: {error_code} | {error_msg}")
            
            with state_lock:
                bot_state["posts_failed"] += 1
                bot_state["recent_posts"].insert(0, {
                    "time": datetime.utcnow().strftime("%H:%M:%S UTC"),
                    "content": content[:80] + "...",
                    "status": f"Rejected ({error_code})",
                    "url": "#"
                })
                if len(bot_state["recent_posts"]) > 10:
                    bot_state["recent_posts"].pop()
                    
            return jsonify({"success": False, "error": f"Binance error {error_code}: {error_msg}"}), 400
            
    except Exception as e:
        log.error(f"Error in manual post trigger: {e}")
        return jsonify({"success": False, "error": str(e)}), 500


# ─────────────────────────────────────────────
# BACKGROUND WORKER & THREAD CONTROL
# ─────────────────────────────────────────────
bg_started = False
bg_lock = threading.Lock()

def background_worker():
    global bot_state
    with state_lock:
        bot_state["is_running"] = True
    
    while True:
        try:
            run_daily_session()
        except Exception as e:
            log.error(f"Fatal error in daily session: {e}")
            with state_lock:
                bot_state["error_message"] = str(e)
                bot_state["status"] = "Error encountered"
        
        with state_lock:
            bot_state["status"] = "Sleeping until next day's active window"
        log.info("💤 Daily session complete. Sleeping 8 hours before building next schedule...")
        time.sleep(28800)


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