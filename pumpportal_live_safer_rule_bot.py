"""
pumpportal_live_safer_rule_bot.py

Standalone PumpPortal live bot with:
- PAPER mode by default
- Optional REAL mode using PumpPortal Lightning Transaction API
- NO MySQL
- NO external collector needed
- Live websocket tracking in memory

It listens to PumpPortal live websocket:
  wss://pumpportal.fun/api/data?api-key=...

It subscribes to:
  - subscribeNewToken
  - subscribeTokenTrade for each newly detected token

Then it builds in-memory snapshots:
  - 5:00 after launch
  - 5:15 after launch
  - 5:30 after launch

Safer Rule:
  5:15:
    market_cap >= $10,000

  5:30:
    market_cap_5_30 >= 97% of market_cap_5_15
    new_buyers_5_00_to_5_30 >= 10
    new_buyers - new_sellers >= 5
    buyer/seller ratio at 5:30 >= 1.5
    creator_sell_share <= 15%
    optional top_buyer_share <= 20%

Exit:
  - Take profit at +15%
  - Stop loss at -15%
  - Final exit at 6:45 after launch
  - Stop the whole bot when collective estimated/paper profit reaches +30%

Install:
  pip install websockets requests python-dotenv

Run paper mode:
  python pumpportal_live_safer_rule_bot.py

Create .env:
  copy pumpportal_live_bot.env.example to .env and edit it.

IMPORTANT:
- Start in PAPER mode first.
- Real mode can lose money.
- Websocket token-trade subscriptions may cost SOL according to PumpPortal.
- Keep your API key private.
"""

from __future__ import annotations

import asyncio
import csv
import json
import math
import os
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import requests
except ImportError:
    print("Missing package: requests")
    print("Install with: pip install requests")
    sys.exit(1)

try:
    import websockets
except ImportError:
    print("Missing package: websockets")
    print("Install with: pip install websockets")
    sys.exit(1)

if load_dotenv:
    load_dotenv()


# ============================================================
# CONFIG
# ============================================================

TRADE_MODE = os.getenv("TRADE_MODE", "paper").strip().lower()
PAPER_TRADING = TRADE_MODE != "real"

PUMPPORTAL_API_KEY = os.getenv("PUMPPORTAL_API_KEY", "c94k6wb58d372j9fatqmmpjc6d574rup6rr4cckq8mr52wthet136gjtcnb32d315duk0kv389970rk1dt8n2gbp9hr6mwbb6x9mahju85v78k3ed9546ta8chw50y9b6da5jvbda4ykudhp4pkkfe1c4pthnad7q6j3heg8t448k3g95q70pkcexb68vkd6nr78gkg6xvkuf8").strip()
PUMPPORTAL_WS_URL = f"wss://pumpportal.fun/api/data?api-key={PUMPPORTAL_API_KEY}"
PUMPPORTAL_TRADE_URL = f"https://pumpportal.fun/api/trade?api-key={PUMPPORTAL_API_KEY}"

# Real trading safety gates
REAL_TRADING_ENABLED = os.getenv("REAL_TRADING_ENABLED", "false").strip().lower() == "true"
LIVE_TRADING_ACK = os.getenv("LIVE_TRADING_ACK", "")
REQUIRED_LIVE_ACK = "I_UNDERSTAND_REAL_TRADING_CAN_LOSE_MONEY"

# Bot settings
BOT_COUNT = int(os.getenv("BOT_COUNT", "5"))
STARTING_BALANCE_PER_BOT_USD = float(os.getenv("STARTING_BALANCE_PER_BOT_USD", "100"))
POSITION_SIZE_USD = float(os.getenv("POSITION_SIZE_USD", "10"))
COLLECTIVE_PROFIT_TARGET_PCT = float(os.getenv("COLLECTIVE_PROFIT_TARGET_PCT", "30"))
ALLOW_DUPLICATE_TOKEN_ENTRIES = os.getenv("ALLOW_DUPLICATE_TOKEN_ENTRIES", "false").lower() == "true"

# Strategy thresholds
SOL_TO_USD = float(os.getenv("SOL_TO_USD", "180"))
MIN_MARKET_CAP_USD_515 = float(os.getenv("MIN_MARKET_CAP_USD_515", "10000"))
MIN_MARKET_CAP_530_RATIO_VS_515 = float(os.getenv("MIN_MARKET_CAP_530_RATIO_VS_515", "0.97"))
MIN_NEW_BUYERS_500_TO_530 = int(os.getenv("MIN_NEW_BUYERS_500_TO_530", "10"))
MIN_BUYER_GROWTH_ADVANTAGE = int(os.getenv("MIN_BUYER_GROWTH_ADVANTAGE", "5"))
MIN_BUYER_SELLER_RATIO_530 = float(os.getenv("MIN_BUYER_SELLER_RATIO_530", "1.5"))
MAX_CREATOR_SELL_SHARE_PCT = float(os.getenv("MAX_CREATOR_SELL_SHARE_PCT", "15"))

USE_TOP_BUYER_FILTER = os.getenv("USE_TOP_BUYER_FILTER", "true").lower() == "true"
MAX_TOP_BUYER_SHARE_PCT = float(os.getenv("MAX_TOP_BUYER_SHARE_PCT", "20"))

# Timings after token launch
AGE_500 = float(os.getenv("AGE_500", "300"))
AGE_515 = float(os.getenv("AGE_515", "315"))
AGE_530 = float(os.getenv("AGE_530", "330"))
FINAL_EXIT_AGE_SECONDS = float(os.getenv("FINAL_EXIT_AGE_SECONDS", "405"))

# Exits
TAKE_PROFIT_PCT = float(os.getenv("TAKE_PROFIT_PCT", "15"))
STOP_LOSS_PCT = float(os.getenv("STOP_LOSS_PCT", "-15"))

# Real order settings
REAL_BUY_AMOUNT_SOL = float(os.getenv("REAL_BUY_AMOUNT_SOL", "0.01"))
MAX_REAL_BUY_AMOUNT_SOL = float(os.getenv("MAX_REAL_BUY_AMOUNT_SOL", "0.02"))
PUMPPORTAL_SLIPPAGE = float(os.getenv("PUMPPORTAL_SLIPPAGE", "10"))
PUMPPORTAL_PRIORITY_FEE = float(os.getenv("PUMPPORTAL_PRIORITY_FEE", "0.00005"))
PUMPPORTAL_POOL = os.getenv("PUMPPORTAL_POOL", "pump")
PUMPPORTAL_SKIP_PREFLIGHT = os.getenv("PUMPPORTAL_SKIP_PREFLIGHT", "false").lower()
PUMPPORTAL_JITO_ONLY = os.getenv("PUMPPORTAL_JITO_ONLY", "false").lower()

# Tracking limits and cleanup
MAX_ACTIVE_TRACKED_TOKENS = int(os.getenv("MAX_ACTIVE_TRACKED_TOKENS", "300"))
TOKEN_CLEANUP_AGE_SECONDS = float(os.getenv("TOKEN_CLEANUP_AGE_SECONDS", "900"))  # 15 min
STATUS_EVERY_SECONDS = float(os.getenv("STATUS_EVERY_SECONDS", "30"))
MAINTENANCE_EVERY_SECONDS = float(os.getenv("MAINTENANCE_EVERY_SECONDS", "1"))

# Optional entry-hour filter, local computer time.
# Example: ACTIVE_ENTRY_HOURS=10,13,17
ACTIVE_ENTRY_HOURS = [
    int(x.strip())
    for x in os.getenv("ACTIVE_ENTRY_HOURS", "").split(",")
    if x.strip().isdigit()
]

# Logs
LOG_DIR = Path(os.getenv("LOG_DIR", "pumpportal_live_bot_logs"))
TRADES_CSV = LOG_DIR / "trades.csv"
EVENTS_CSV = LOG_DIR / "events.csv"
RAW_EVENTS_CSV = LOG_DIR / "raw_sample_events.csv"

# Save a few raw events so you can inspect PumpPortal field names if needed
SAVE_RAW_EVENT_SAMPLES = os.getenv("SAVE_RAW_EVENT_SAMPLES", "true").lower() == "true"
MAX_RAW_EVENT_SAMPLES = int(os.getenv("MAX_RAW_EVENT_SAMPLES", "50"))


# ============================================================
# DATA MODELS
# ============================================================

@dataclass
class MetricSnapshot:
    age: float
    market_cap_usd: float
    unique_buyers: int
    unique_sellers: int
    total_buy_sol: float
    total_sell_sol: float
    creator_sell_sol: float
    creator_sell_share_pct: float
    top_buyer_share_pct: float


@dataclass
class TokenState:
    mint: str
    symbol: str = ""
    name: str = ""
    creator: str = ""
    launch_monotonic: float = field(default_factory=time.monotonic)
    launch_utc: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    latest_market_cap_sol: Optional[float] = None
    latest_market_cap_usd: Optional[float] = None
    latest_price_sol: Optional[float] = None

    buyers: Set[str] = field(default_factory=set)
    sellers: Set[str] = field(default_factory=set)
    buyer_sol_volume: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    total_buy_sol: float = 0.0
    total_sell_sol: float = 0.0
    creator_sell_sol: float = 0.0

    snap_500: Optional[MetricSnapshot] = None
    snap_515: Optional[MetricSnapshot] = None
    snap_530: Optional[MetricSnapshot] = None

    subscribed: bool = False
    evaluated: bool = False
    entered: bool = False
    unsubscribed: bool = False

    raw_new_token_event: Dict[str, Any] = field(default_factory=dict)

    def age_seconds(self) -> float:
        return time.monotonic() - self.launch_monotonic

    def current_snapshot(self, forced_age: Optional[float] = None) -> Optional[MetricSnapshot]:
        if self.latest_market_cap_usd is None:
            return None

        creator_share = 0.0
        if self.total_sell_sol > 0:
            creator_share = (self.creator_sell_sol / self.total_sell_sol) * 100.0

        top_buyer_share = 0.0
        if self.total_buy_sol > 0 and self.buyer_sol_volume:
            top_buyer_share = (max(self.buyer_sol_volume.values()) / self.total_buy_sol) * 100.0

        return MetricSnapshot(
            age=forced_age if forced_age is not None else self.age_seconds(),
            market_cap_usd=self.latest_market_cap_usd,
            unique_buyers=len(self.buyers),
            unique_sellers=len(self.sellers),
            total_buy_sol=self.total_buy_sol,
            total_sell_sol=self.total_sell_sol,
            creator_sell_sol=self.creator_sell_sol,
            creator_sell_share_pct=creator_share,
            top_buyer_share_pct=top_buyer_share,
        )


@dataclass
class Position:
    bot_id: int
    mint: str
    symbol: str
    entry_market_cap_usd: float
    entry_time_utc: datetime
    entry_monotonic: float
    stake_usd: float
    mode: str
    buy_response: str = ""
    highest_return_pct: float = 0.0
    lowest_return_pct: float = 0.0


@dataclass
class BotAccount:
    bot_id: int
    balance_usd: float
    open_position: Optional[Position] = None
    trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl_usd: float = 0.0


@dataclass
class StrategyDecision:
    passed: bool
    reason: str
    metrics: Dict[str, Any] = field(default_factory=dict)


# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        if isinstance(value, str) and not value.strip():
            return default
        return float(value)
    except Exception:
        return default


def safe_str(value: Any) -> str:
    return "" if value is None else str(value).strip()


def percent_return(entry: float, current: float) -> float:
    if entry <= 0:
        return 0.0
    return ((current / entry) - 1.0) * 100.0


def buyer_seller_ratio(buyers: int, sellers: int) -> float:
    if sellers <= 0:
        return math.inf if buyers > 0 else 0.0
    return buyers / sellers


def get_first(data: Dict[str, Any], keys: List[str], default: Any = None) -> Any:
    for k in keys:
        if k in data and data[k] is not None:
            return data[k]
    return default


def extract_mint(data: Dict[str, Any]) -> str:
    return safe_str(get_first(data, [
        "mint", "token", "tokenMint", "token_mint", "contractAddress", "ca"
    ]))


def extract_symbol(data: Dict[str, Any]) -> str:
    return safe_str(get_first(data, ["symbol", "ticker", "tokenSymbol"]))


def extract_name(data: Dict[str, Any]) -> str:
    return safe_str(get_first(data, ["name", "tokenName"]))


def extract_trader(data: Dict[str, Any]) -> str:
    return safe_str(get_first(data, [
        "traderPublicKey", "trader", "user", "wallet", "maker", "account"
    ]))


def extract_creator(data: Dict[str, Any]) -> str:
    return safe_str(get_first(data, [
        "creator", "creatorPublicKey", "dev", "deployer",
        "traderPublicKey", "trader", "user"
    ]))


def extract_tx_type(data: Dict[str, Any]) -> str:
    tx = safe_str(get_first(data, ["txType", "type", "side", "action", "event"])).lower()
    if "buy" in tx:
        return "buy"
    if "sell" in tx:
        return "sell"
    if "create" in tx or "new" in tx:
        return "create"
    if "migration" in tx:
        return "migration"
    return tx


def extract_market_cap_sol(data: Dict[str, Any]) -> Optional[float]:
    # PumpPortal commonly uses marketCapSol; this stays flexible.
    return safe_float(get_first(data, [
        "marketCapSol", "market_cap_sol", "currentMarketCapSol",
        "current_market_cap_sol", "marketcapSol", "marketCapSOL"
    ]))


def extract_sol_amount(data: Dict[str, Any]) -> float:
    value = safe_float(get_first(data, [
        "solAmount", "sol_amount", "amountSol", "amount_sol",
        "vSol", "sol", "nativeAmount"
    ]), default=0.0)
    return float(value or 0.0)


def event_is_new_token(data: Dict[str, Any]) -> bool:
    tx = extract_tx_type(data)
    if tx == "create":
        return True
    # New token events may not always set txType consistently.
    # If it has a mint + bonding curve + initialBuy/name/symbol, treat it as a create event.
    has_mint = bool(extract_mint(data))
    has_creation_like = any(k in data for k in ["initialBuy", "bondingCurveKey", "name", "symbol"])
    return has_mint and has_creation_like and tx not in {"buy", "sell"}


def ensure_log_files() -> None:
    LOG_DIR.mkdir(exist_ok=True)

    if not TRADES_CSV.exists():
        with TRADES_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "mode", "bot_id", "mint", "symbol", "entry_mc_usd", "exit_mc_usd",
                "return_pct", "pnl_usd", "reason",
                "opened_utc", "closed_utc", "duration_seconds",
                "buy_response", "sell_response",
            ])

    if not EVENTS_CSV.exists():
        with EVENTS_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_utc", "mode", "event_type", "mint", "symbol", "message"])

    if SAVE_RAW_EVENT_SAMPLES and not RAW_EVENTS_CSV.exists():
        with RAW_EVENTS_CSV.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp_utc", "raw_json"])


def log_event(mode: str, event_type: str, mint: str, symbol: str, message: str) -> None:
    with EVENTS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([now_utc().isoformat(timespec="seconds"), mode, event_type, mint, symbol, message])
    print(f"[{mode.upper()}][{event_type}] {symbol or mint}: {message}")


def log_trade(
    mode: str,
    position: Position,
    exit_mc: float,
    ret_pct: float,
    pnl_usd: float,
    reason: str,
    sell_response: str,
) -> None:
    duration = int((now_utc() - position.entry_time_utc).total_seconds())
    with TRADES_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            mode,
            position.bot_id,
            position.mint,
            position.symbol,
            f"{position.entry_market_cap_usd:.8f}",
            f"{exit_mc:.8f}",
            f"{ret_pct:.4f}",
            f"{pnl_usd:.4f}",
            reason,
            position.entry_time_utc.isoformat(timespec="seconds"),
            now_utc().isoformat(timespec="seconds"),
            duration,
            position.buy_response,
            sell_response,
        ])


def save_raw_event_sample(data: Dict[str, Any], count: int) -> None:
    if not SAVE_RAW_EVENT_SAMPLES or count >= MAX_RAW_EVENT_SAMPLES:
        return
    with RAW_EVENTS_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([now_utc().isoformat(timespec="seconds"), json.dumps(data, ensure_ascii=False)])


# ============================================================
# PUMPPORTAL TRADER
# ============================================================

class PumpPortalTrader:
    def __init__(self) -> None:
        if not PUMPPORTAL_API_KEY:
            raise RuntimeError("PUMPPORTAL_API_KEY is missing.")
        if REAL_BUY_AMOUNT_SOL <= 0:
            raise RuntimeError("REAL_BUY_AMOUNT_SOL must be greater than 0.")
        if REAL_BUY_AMOUNT_SOL > MAX_REAL_BUY_AMOUNT_SOL:
            raise RuntimeError(
                f"REAL_BUY_AMOUNT_SOL={REAL_BUY_AMOUNT_SOL} exceeds MAX_REAL_BUY_AMOUNT_SOL={MAX_REAL_BUY_AMOUNT_SOL}."
            )

    def _request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            PUMPPORTAL_TRADE_URL,
            json=payload,
            timeout=30,
            headers={"Content-Type": "application/json"},
        )

        try:
            data = response.json()
        except Exception:
            data = {"raw_text": response.text}

        if response.status_code >= 400:
            raise RuntimeError(f"PumpPortal HTTP {response.status_code}: {data}")

        if isinstance(data, dict) and (data.get("error") or data.get("errors")):
            raise RuntimeError(f"PumpPortal returned error: {data}")

        return data

    def buy(self, mint: str) -> Dict[str, Any]:
        payload = {
            "action": "buy",
            "mint": mint,
            "amount": REAL_BUY_AMOUNT_SOL,
            "denominatedInSol": "true",
            "slippage": PUMPPORTAL_SLIPPAGE,
            "priorityFee": PUMPPORTAL_PRIORITY_FEE,
            "pool": PUMPPORTAL_POOL,
            "skipPreflight": PUMPPORTAL_SKIP_PREFLIGHT,
            "jitoOnly": PUMPPORTAL_JITO_ONLY,
        }
        return self._request(payload)

    def sell_all(self, mint: str) -> Dict[str, Any]:
        payload = {
            "action": "sell",
            "mint": mint,
            "amount": "100%",
            "denominatedInSol": "false",
            "slippage": PUMPPORTAL_SLIPPAGE,
            "priorityFee": PUMPPORTAL_PRIORITY_FEE,
            "pool": PUMPPORTAL_POOL,
            "skipPreflight": PUMPPORTAL_SKIP_PREFLIGHT,
            "jitoOnly": PUMPPORTAL_JITO_ONLY,
        }
        return self._request(payload)


# ============================================================
# BOT ENGINE
# ============================================================

class LiveSaferRuleBot:
    def __init__(self) -> None:
        self.validate_config()

        self.mode = "paper" if PAPER_TRADING else "real"
        self.trader = None if PAPER_TRADING else PumpPortalTrader()

        self.tokens: Dict[str, TokenState] = {}
        self.traded_tokens: Set[str] = set()
        self.closed_tokens: Set[str] = set()

        self.accounts: List[BotAccount] = [
            BotAccount(bot_id=i + 1, balance_usd=STARTING_BALANCE_PER_BOT_USD)
            for i in range(BOT_COUNT)
        ]

        self.running = True
        self.started_utc = now_utc()
        self.raw_sample_count = 0
        self.websocket = None

        self.last_status_ts = 0.0

    def validate_config(self) -> None:
        if TRADE_MODE not in {"paper", "real"}:
            raise RuntimeError("TRADE_MODE must be 'paper' or 'real'.")

        if not PUMPPORTAL_API_KEY:
            raise RuntimeError("PUMPPORTAL_API_KEY is required even in paper mode because the live websocket uses it.")

        if not PAPER_TRADING:
            if not REAL_TRADING_ENABLED:
                raise RuntimeError("REAL mode blocked. Set REAL_TRADING_ENABLED=true.")
            if LIVE_TRADING_ACK != REQUIRED_LIVE_ACK:
                raise RuntimeError(
                    "REAL mode blocked. Add this exact line to .env:\n"
                    f"LIVE_TRADING_ACK={REQUIRED_LIVE_ACK}"
                )

        if BOT_COUNT <= 0:
            raise RuntimeError("BOT_COUNT must be greater than 0.")

    @property
    def start_balance_total(self) -> float:
        return STARTING_BALANCE_PER_BOT_USD * BOT_COUNT

    @property
    def current_balance_total(self) -> float:
        return sum(a.balance_usd for a in self.accounts)

    def collective_profit_pct(self) -> float:
        return ((self.current_balance_total / self.start_balance_total) - 1.0) * 100.0

    def get_available_account(self) -> Optional[BotAccount]:
        for account in self.accounts:
            if account.open_position is None and account.balance_usd >= POSITION_SIZE_USD:
                return account
        return None

    def has_open_token(self, mint: str) -> bool:
        return any(a.open_position and a.open_position.mint == mint for a in self.accounts)

    def hour_allowed(self) -> bool:
        if not ACTIVE_ENTRY_HOURS:
            return True
        return datetime.now().hour in ACTIVE_ENTRY_HOURS

    def update_token_from_event(self, token: TokenState, data: Dict[str, Any]) -> None:
        mc_sol = extract_market_cap_sol(data)
        if mc_sol is not None and mc_sol > 0:
            token.latest_market_cap_sol = mc_sol
            token.latest_market_cap_usd = mc_sol * SOL_TO_USD

        tx_type = extract_tx_type(data)
        trader = extract_trader(data)
        sol_amount = extract_sol_amount(data)

        if tx_type == "buy":
            if trader:
                token.buyers.add(trader)
                token.buyer_sol_volume[trader] += max(sol_amount, 0.0)
            token.total_buy_sol += max(sol_amount, 0.0)

        elif tx_type == "sell":
            if trader:
                token.sellers.add(trader)
            token.total_sell_sol += max(sol_amount, 0.0)

            if token.creator and trader and trader == token.creator:
                token.creator_sell_sol += max(sol_amount, 0.0)

    async def subscribe_token_trade(self, mint: str) -> None:
        if not self.websocket:
            return
        payload = {"method": "subscribeTokenTrade", "keys": [mint]}
        await self.websocket.send(json.dumps(payload))

    async def unsubscribe_token_trade(self, mint: str) -> None:
        if not self.websocket:
            return
        payload = {"method": "unsubscribeTokenTrade", "keys": [mint]}
        await self.websocket.send(json.dumps(payload))

    async def handle_new_token(self, data: Dict[str, Any]) -> None:
        mint = extract_mint(data)
        if not mint:
            return

        if mint in self.tokens:
            return

        if len(self.tokens) >= MAX_ACTIVE_TRACKED_TOKENS:
            log_event(self.mode, "TRACK_LIMIT", mint, "", f"Max active token tracking reached: {MAX_ACTIVE_TRACKED_TOKENS}")
            return

        creator = extract_creator(data)

        token = TokenState(
            mint=mint,
            symbol=extract_symbol(data),
            name=extract_name(data),
            creator=creator,
            raw_new_token_event=data,
        )

        mc_sol = extract_market_cap_sol(data)
        if mc_sol is not None and mc_sol > 0:
            token.latest_market_cap_sol = mc_sol
            token.latest_market_cap_usd = mc_sol * SOL_TO_USD

        self.tokens[mint] = token

        await self.subscribe_token_trade(mint)
        token.subscribed = True

        log_event(
            self.mode,
            "NEW_TOKEN",
            mint,
            token.symbol,
            f"Tracking new token | creator={creator or '-'} | active_tracked={len(self.tokens)}",
        )

    async def handle_trade_event(self, data: Dict[str, Any]) -> None:
        mint = extract_mint(data)
        if not mint:
            return

        token = self.tokens.get(mint)
        if token is None:
            # This bot trades only tokens it saw from launch/new-token event.
            return

        self.update_token_from_event(token, data)

    async def handle_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except Exception:
            return

        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    await self.handle_message(json.dumps(item))
            return

        if not isinstance(data, dict):
            return

        save_raw_event_sample(data, self.raw_sample_count)
        if self.raw_sample_count < MAX_RAW_EVENT_SAMPLES:
            self.raw_sample_count += 1

        if event_is_new_token(data):
            await self.handle_new_token(data)
            return

        tx_type = extract_tx_type(data)
        if tx_type in {"buy", "sell"}:
            await self.handle_trade_event(data)
            return

    def capture_snapshots(self, token: TokenState) -> None:
        age = token.age_seconds()

        if token.snap_500 is None and age >= AGE_500:
            token.snap_500 = token.current_snapshot(forced_age=AGE_500)
            if token.snap_500:
                log_event(self.mode, "SNAP_5_00", token.mint, token.symbol, f"mc=${token.snap_500.market_cap_usd:,.2f}")

        if token.snap_515 is None and age >= AGE_515:
            token.snap_515 = token.current_snapshot(forced_age=AGE_515)
            if token.snap_515:
                log_event(self.mode, "SNAP_5_15", token.mint, token.symbol, f"mc=${token.snap_515.market_cap_usd:,.2f}")

        if token.snap_530 is None and age >= AGE_530:
            token.snap_530 = token.current_snapshot(forced_age=AGE_530)
            if token.snap_530:
                log_event(self.mode, "SNAP_5_30", token.mint, token.symbol, f"mc=${token.snap_530.market_cap_usd:,.2f}")

    def evaluate_safer_rule(self, token: TokenState) -> StrategyDecision:
        s500 = token.snap_500
        s515 = token.snap_515
        s530 = token.snap_530

        if not s500 or not s515 or not s530:
            return StrategyDecision(False, "missing required snapshots")

        if s515.market_cap_usd < MIN_MARKET_CAP_USD_515:
            return StrategyDecision(False, f"5:15 market cap below ${MIN_MARKET_CAP_USD_515:,.0f}")

        if s530.market_cap_usd < s515.market_cap_usd * MIN_MARKET_CAP_530_RATIO_VS_515:
            return StrategyDecision(False, "5:30 market cap dropped more than 3% from 5:15")

        new_buyers = s530.unique_buyers - s500.unique_buyers
        new_sellers = s530.unique_sellers - s500.unique_sellers
        buyer_advantage = new_buyers - new_sellers
        ratio = buyer_seller_ratio(s530.unique_buyers, s530.unique_sellers)

        if new_buyers < MIN_NEW_BUYERS_500_TO_530:
            return StrategyDecision(False, f"new buyers too low: {new_buyers}")

        if buyer_advantage < MIN_BUYER_GROWTH_ADVANTAGE:
            return StrategyDecision(False, f"buyer growth advantage too low: {buyer_advantage}")

        if ratio < MIN_BUYER_SELLER_RATIO_530:
            return StrategyDecision(False, f"buyer/seller ratio too low: {ratio:.2f}")

        if s530.creator_sell_share_pct > MAX_CREATOR_SELL_SHARE_PCT:
            return StrategyDecision(False, f"creator sell share too high: {s530.creator_sell_share_pct:.2f}%")

        if USE_TOP_BUYER_FILTER and s530.top_buyer_share_pct > MAX_TOP_BUYER_SHARE_PCT:
            return StrategyDecision(False, f"top buyer share too high: {s530.top_buyer_share_pct:.2f}%")

        return StrategyDecision(
            True,
            "PASSED safer rule",
            {
                "new_buyers": new_buyers,
                "new_sellers": new_sellers,
                "buyer_advantage": buyer_advantage,
                "buyer_seller_ratio": ratio,
                "creator_sell_share_pct": s530.creator_sell_share_pct,
                "top_buyer_share_pct": s530.top_buyer_share_pct,
                "market_cap_515": s515.market_cap_usd,
                "market_cap_530": s530.market_cap_usd,
            },
        )

    async def open_position(self, account: BotAccount, token: TokenState, decision: StrategyDecision) -> None:
        if token.latest_market_cap_usd is None:
            return

        buy_response = ""

        if not PAPER_TRADING:
            assert self.trader is not None
            log_event(self.mode, "REAL_BUY_ATTEMPT", token.mint, token.symbol, f"Buying {REAL_BUY_AMOUNT_SOL} SOL")
            data = self.trader.buy(token.mint)
            buy_response = str(data)
            log_event(self.mode, "REAL_BUY_RESPONSE", token.mint, token.symbol, buy_response)

        stake = POSITION_SIZE_USD if PAPER_TRADING else REAL_BUY_AMOUNT_SOL * SOL_TO_USD

        account.open_position = Position(
            bot_id=account.bot_id,
            mint=token.mint,
            symbol=token.symbol,
            entry_market_cap_usd=token.latest_market_cap_usd,
            entry_time_utc=now_utc(),
            entry_monotonic=time.monotonic(),
            stake_usd=stake,
            mode=self.mode,
            buy_response=buy_response,
        )

        token.entered = True
        self.traded_tokens.add(token.mint)

        log_event(
            self.mode,
            "OPEN",
            token.mint,
            token.symbol,
            (
                f"bot={account.bot_id} | entry_mc=${token.latest_market_cap_usd:,.2f} | "
                f"new_buyers={decision.metrics.get('new_buyers')} | "
                f"buyer_advantage={decision.metrics.get('buyer_advantage')} | "
                f"ratio={decision.metrics.get('buyer_seller_ratio'):.2f} | "
                f"creator_sell_share={decision.metrics.get('creator_sell_share_pct'):.2f}% | "
                f"top_buyer_share={decision.metrics.get('top_buyer_share_pct'):.2f}%"
            ),
        )

    async def close_position(self, account: BotAccount, token: Optional[TokenState], reason: str) -> None:
        pos = account.open_position
        if not pos:
            return

        exit_mc = pos.entry_market_cap_usd
        if token and token.latest_market_cap_usd:
            exit_mc = token.latest_market_cap_usd

        ret_pct = percent_return(pos.entry_market_cap_usd, exit_mc)
        pnl_usd = pos.stake_usd * (ret_pct / 100.0)
        sell_response = ""

        if not PAPER_TRADING:
            assert self.trader is not None
            try:
                log_event(self.mode, "REAL_SELL_ATTEMPT", pos.mint, pos.symbol, f"Sell 100% | reason={reason}")
                data = self.trader.sell_all(pos.mint)
                sell_response = str(data)
                log_event(self.mode, "REAL_SELL_RESPONSE", pos.mint, pos.symbol, sell_response)
            except Exception as e:
                # Do not mark as closed if sell failed.
                log_event(self.mode, "REAL_SELL_FAILED", pos.mint, pos.symbol, str(e))
                return

        account.balance_usd += pnl_usd
        account.trades += 1
        account.total_pnl_usd += pnl_usd
        if pnl_usd > 0:
            account.wins += 1
        else:
            account.losses += 1

        log_trade(self.mode, pos, exit_mc, ret_pct, pnl_usd, reason, sell_response)

        log_event(
            self.mode,
            "CLOSE",
            pos.mint,
            pos.symbol,
            (
                f"bot={account.bot_id} | reason={reason} | estimated_return={ret_pct:.2f}% | "
                f"estimated_pnl=${pnl_usd:.4f} | collective_profit={self.collective_profit_pct():.2f}%"
            ),
        )

        self.closed_tokens.add(pos.mint)
        account.open_position = None

    async def check_entries(self) -> None:
        if not self.hour_allowed():
            return

        for token in list(self.tokens.values()):
            if token.evaluated or token.entered:
                continue

            self.capture_snapshots(token)

            if token.snap_530 is None:
                continue

            token.evaluated = True
            decision = self.evaluate_safer_rule(token)

            if not decision.passed:
                log_event(self.mode, "REJECT", token.mint, token.symbol, decision.reason)
                continue

            if not ALLOW_DUPLICATE_TOKEN_ENTRIES and (token.mint in self.traded_tokens or self.has_open_token(token.mint)):
                continue

            account = self.get_available_account()
            if not account:
                log_event(self.mode, "NO_SLOT", token.mint, token.symbol, "Signal passed but no bot slot available")
                continue

            try:
                await self.open_position(account, token, decision)
            except Exception as e:
                log_event(self.mode, "OPEN_FAILED", token.mint, token.symbol, str(e))

    async def check_exits(self) -> None:
        for account in self.accounts:
            pos = account.open_position
            if not pos:
                continue

            token = self.tokens.get(pos.mint)
            if token is None or token.latest_market_cap_usd is None:
                held = time.monotonic() - pos.entry_monotonic
                if held >= 120:
                    await self.close_position(account, token, "WALL_CLOCK_NO_MARKET_CAP_EXIT")
                continue

            ret_pct = percent_return(pos.entry_market_cap_usd, token.latest_market_cap_usd)
            pos.highest_return_pct = max(pos.highest_return_pct, ret_pct)
            pos.lowest_return_pct = min(pos.lowest_return_pct, ret_pct)

            if ret_pct >= TAKE_PROFIT_PCT:
                await self.close_position(account, token, f"TAKE_PROFIT_{TAKE_PROFIT_PCT:.0f}%")
                continue

            if ret_pct <= STOP_LOSS_PCT:
                await self.close_position(account, token, f"STOP_LOSS_{STOP_LOSS_PCT:.0f}%")
                continue

            if token.age_seconds() >= FINAL_EXIT_AGE_SECONDS:
                await self.close_position(account, token, "FINAL_EXIT_6_45")
                continue

    async def cleanup_tokens(self) -> None:
        for mint, token in list(self.tokens.items()):
            if token.age_seconds() < TOKEN_CLEANUP_AGE_SECONDS:
                continue

            if self.has_open_token(mint):
                continue

            if token.subscribed and not token.unsubscribed:
                try:
                    await self.unsubscribe_token_trade(mint)
                    token.unsubscribed = True
                    log_event(self.mode, "UNSUB", mint, token.symbol, "Stopped token trade stream after cleanup age")
                except Exception as e:
                    log_event(self.mode, "UNSUB_FAILED", mint, token.symbol, str(e))

            # Remove from memory after unsubscribe
            self.tokens.pop(mint, None)

    def print_status(self) -> None:
        open_count = sum(1 for a in self.accounts if a.open_position)
        trades = sum(a.trades for a in self.accounts)
        wins = sum(a.wins for a in self.accounts)
        losses = sum(a.losses for a in self.accounts)
        success = (wins / trades * 100.0) if trades else 0.0

        print(
            f"[STATUS][{self.mode.upper()}] tracked={len(self.tokens)} | open={open_count}/{BOT_COUNT} | "
            f"closed_trades={trades} | wins={wins} | losses={losses} | success={success:.2f}% | "
            f"est_balance=${self.current_balance_total:.2f} | profit={self.collective_profit_pct():.2f}%/"
            f"{COLLECTIVE_PROFIT_TARGET_PCT:.2f}%"
        )

    async def close_all_open_positions(self, reason: str) -> None:
        for account in self.accounts:
            if account.open_position:
                token = self.tokens.get(account.open_position.mint)
                await self.close_position(account, token, reason)

    def final_summary(self) -> None:
        ended = now_utc()
        runtime = ended - self.started_utc

        trades = sum(a.trades for a in self.accounts)
        wins = sum(a.wins for a in self.accounts)
        losses = sum(a.losses for a in self.accounts)
        pnl = sum(a.total_pnl_usd for a in self.accounts)
        success = (wins / trades * 100.0) if trades else 0.0
        profit_pct = self.collective_profit_pct()

        print("\n" + "=" * 76)
        print(f"FINAL {self.mode.upper()} TRADING SUMMARY")
        print("=" * 76)
        print(f"Started UTC:             {self.started_utc.isoformat(timespec='seconds')}")
        print(f"Ended UTC:               {ended.isoformat(timespec='seconds')}")
        print(f"Runtime:                 {runtime}")
        print(f"Bot slots:               {BOT_COUNT}")
        print(f"Start balance estimate:  ${self.start_balance_total:.2f}")
        print(f"End balance estimate:    ${self.current_balance_total:.2f}")
        print(f"Total estimated PnL:     ${pnl:.4f}")
        print(f"Profit percent estimate: {profit_pct:.2f}%")
        print(f"Target profit:           {COLLECTIVE_PROFIT_TARGET_PCT:.2f}%")
        print(f"Total closed trades:     {trades}")
        print(f"Wins:                    {wins}")
        print(f"Losses:                  {losses}")
        print(f"Success rate:            {success:.2f}%")
        print(f"Trades CSV:              {TRADES_CSV.resolve()}")
        print(f"Events CSV:              {EVENTS_CSV.resolve()}")

        if not PAPER_TRADING:
            print("\nREAL MODE NOTE:")
            print("The printed PnL is estimated from market-cap movement.")
            print("Actual wallet PnL can differ because of fees, slippage, partial fills, failed sells, and price impact.")

        print("\nPer-bot summary:")
        for a in self.accounts:
            sr = (a.wins / a.trades * 100.0) if a.trades else 0.0
            open_mint = a.open_position.mint if a.open_position else "-"
            print(
                f"  Bot {a.bot_id}: est_balance=${a.balance_usd:.2f}, "
                f"trades={a.trades}, wins={a.wins}, losses={a.losses}, "
                f"success={sr:.2f}%, est_pnl=${a.total_pnl_usd:.4f}, open={open_mint}"
            )
        print("=" * 76 + "\n")

    async def maintenance_loop(self) -> None:
        while self.running:
            try:
                await self.check_entries()
                await self.check_exits()
                await self.cleanup_tokens()

                if self.collective_profit_pct() >= COLLECTIVE_PROFIT_TARGET_PCT:
                    log_event(self.mode, "TARGET_REACHED", "", "", f"Collective profit target reached: {self.collective_profit_pct():.2f}%")
                    await self.close_all_open_positions("BOT_STOP_TARGET_REACHED")
                    self.running = False
                    break

                if time.time() - self.last_status_ts >= STATUS_EVERY_SECONDS:
                    self.print_status()
                    self.last_status_ts = time.time()

                await asyncio.sleep(MAINTENANCE_EVERY_SECONDS)

            except Exception as e:
                log_event(self.mode, "MAINTENANCE_ERROR", "", "", str(e))
                await asyncio.sleep(3)

    async def websocket_loop(self) -> None:
        reconnect_delay = 5

        while self.running:
            try:
                async with websockets.connect(PUMPPORTAL_WS_URL, ping_interval=20, ping_timeout=20) as ws:
                    self.websocket = ws

                    await ws.send(json.dumps({"method": "subscribeNewToken"}))
                    log_event(self.mode, "WS_CONNECTED", "", "", "Subscribed to new token stream")

                    async for message in ws:
                        if not self.running:
                            break
                        await self.handle_message(message)

            except Exception as e:
                log_event(self.mode, "WS_ERROR", "", "", f"{e}. Reconnecting in {reconnect_delay}s")
                await asyncio.sleep(reconnect_delay)

            finally:
                self.websocket = None

    async def run(self) -> None:
        ensure_log_files()

        print("\nStarting PumpPortal Live Safer Rule Bot")
        print(f"MODE: {self.mode.upper()}")
        print(f"Bot slots: {BOT_COUNT}")
        print(f"Target profit: {COLLECTIVE_PROFIT_TARGET_PCT:.2f}%")
        print(f"TP={TAKE_PROFIT_PCT:.2f}% | SL={STOP_LOSS_PCT:.2f}% | Final exit age={FINAL_EXIT_AGE_SECONDS:.0f}s")
        print(f"Active entry hours: {ACTIVE_ENTRY_HOURS if ACTIVE_ENTRY_HOURS else 'all'}")
        print("Press CTRL+C to stop and print summary.\n")

        loop = asyncio.get_running_loop()

        def request_stop() -> None:
            print("\nStop requested. Closing open positions and printing summary...")
            self.running = False

        try:
            loop.add_signal_handler(signal.SIGINT, request_stop)
            loop.add_signal_handler(signal.SIGTERM, request_stop)
        except NotImplementedError:
            # Windows event loop may not support add_signal_handler
            pass

        ws_task = asyncio.create_task(self.websocket_loop())
        maint_task = asyncio.create_task(self.maintenance_loop())

        try:
            while self.running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            self.running = False

        await self.close_all_open_positions("BOT_STOPPED_BY_USER")

        for task in [ws_task, maint_task]:
            task.cancel()

        await asyncio.gather(ws_task, maint_task, return_exceptions=True)
        self.final_summary()


if __name__ == "__main__":
    bot = LiveSaferRuleBot()
    asyncio.run(bot.run())
