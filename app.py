import os
import json
import time
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template, request

# ── Redis persistence (Vercel) or local JSON fallback ─────────────────────────
_redis = None
if os.environ.get('UPSTASH_REDIS_REST_URL'):
    from upstash_redis import Redis
    _redis = Redis(url=os.environ['UPSTASH_REDIS_REST_URL'],
                   token=os.environ['UPSTASH_REDIS_REST_TOKEN'])

_root = os.path.dirname(os.path.abspath(__file__))
# Writable data dir — overridden by DATA_DIR env var on hosts with a persistent
# disk (e.g. Render). Defaults to the app directory for local development.
_data_dir = os.environ.get("DATA_DIR", _root)
os.makedirs(_data_dir, exist_ok=True)

app = Flask(__name__,
            template_folder=os.path.join(_root, "templates"),
            static_folder=os.path.join(_root, "static"))

DATA_FILE = os.path.join(_data_dir, "portfolio.json")
EQUITY_LOG_FILE = os.path.join(_data_dir, "equity_log.json")

# Demo portfolio shown on a fresh install. Users replace this by adding their
# own holdings through the UI — the UI overwrites this on the first save.
INITIAL_DATA = {
    "holdings": [
        {"ticker": "VOO", "shares": 10, "avg_cost": 500.00, "signal": "HOLD", "sector": "S&P 500 Index", "notes": "Core equity allocation (demo)"},
        {"ticker": "QQQ", "shares": 5, "avg_cost": 450.00, "signal": "HOLD", "sector": "Nasdaq-100", "notes": "Tech tilt (demo)"},
        {"ticker": "BND", "shares": 20, "avg_cost": 75.00, "signal": "HOLD", "sector": "US Aggregate Bonds", "notes": "Fixed income (demo)"}
    ],
    "limit_orders": [],
    "watchlist": [
        {"ticker": "SCHD", "notes": "Dividend growth ETF — example watchlist entry"}
    ],
    "events": [],
    "cash": 1000.0,
    "last_updated": None
}

# ── Price cache ────────────────────────────────────────────────────────────────
_price_cache: dict = {}
_prev_close_cache: dict = {}
_last_fetched: float = 0
CACHE_TTL = 60

# ── Risk metrics cache ─────────────────────────────────────────────────────────
_risk_cache: dict = {}
_risk_last_fetched: float = 0
RISK_TTL = 86400


def load_data() -> dict:
    if _redis:
        raw = _redis.get("portfolio")
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        save_data(INITIAL_DATA)
        return INITIAL_DATA
    if not os.path.exists(DATA_FILE):
        save_data(INITIAL_DATA)
        return INITIAL_DATA
    with open(DATA_FILE, "r") as f:
        return json.load(f)


def save_data(data: dict) -> None:
    if _redis:
        _redis.set("portfolio", json.dumps(data))
        return
    tmp = DATA_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, DATA_FILE)


def load_equity_log() -> list:
    if _redis:
        raw = _redis.get("equity_log")
        if raw:
            return json.loads(raw) if isinstance(raw, str) else raw
        return []
    if not os.path.exists(EQUITY_LOG_FILE):
        return []
    with open(EQUITY_LOG_FILE, "r") as f:
        return json.load(f)


def save_equity_log(log: list) -> None:
    if _redis:
        _redis.set("equity_log", json.dumps(log))
        return
    tmp = EQUITY_LOG_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(log, f, indent=2)
    os.replace(tmp, EQUITY_LOG_FILE)


def take_snapshot() -> dict | None:
    """Append (or update) today's portfolio value in equity_log.json. Idempotent per day."""
    if not _price_cache:
        return None
    today = datetime.now().date().isoformat()
    data = load_data()
    total = sum(
        h["shares"] * _price_cache.get(h["ticker"], h["avg_cost"])
        for h in data["holdings"]
    )
    total += data.get("cash", 0)
    total = round(total, 2)
    log = load_equity_log()
    for entry in log:
        if entry["date"] == today:
            entry["total_value"] = total
            save_equity_log(log)
            return entry
    log.append({"date": today, "total_value": total})
    log.sort(key=lambda x: x["date"])
    save_equity_log(log)
    return {"date": today, "total_value": total}


def get_all_tickers(data: dict) -> list:
    tickers = set()
    for h in data.get("holdings", []):
        tickers.add(h["ticker"])
    for lo in data.get("limit_orders", []):
        tickers.add(lo["ticker"])
    for w in data.get("watchlist", []):
        tickers.add(w["ticker"])
    return list(tickers)


def refresh_prices(tickers: list) -> dict:
    global _price_cache, _prev_close_cache, _last_fetched
    if time.time() - _last_fetched < CACHE_TTL:
        return _price_cache

    if not tickers:
        return _price_cache

    try:
        import yfinance as yf
        raw = yf.download(
            tickers if len(tickers) > 1 else tickers[0],
            period="2d",
            interval="1m",
            auto_adjust=True,
            progress=False,
            threads=False,
        )

        if raw.empty:
            return _price_cache

        close = raw["Close"] if "Close" in raw.columns else raw

        if len(tickers) == 1:
            t = tickers[0]
            series = close.dropna()
            if not series.empty:
                _price_cache[t] = float(series.iloc[-1])
                # prev close: last value before today
                today = datetime.now().date()
                prev = series[series.index.date < today]
                if not prev.empty:
                    _prev_close_cache[t] = float(prev.iloc[-1])
        else:
            today = datetime.now().date()
            for t in tickers:
                try:
                    series = close[t].dropna()
                    if not series.empty:
                        _price_cache[t] = float(series.iloc[-1])
                        prev = series[series.index.date < today]
                        if not prev.empty:
                            _prev_close_cache[t] = float(prev.iloc[-1])
                except Exception:
                    pass

        _last_fetched = time.time()
        # Auto-snapshot after a successful live price fetch
        try:
            take_snapshot()
        except Exception:
            pass
    except Exception:
        pass

    return _price_cache


def compute_alerts(data: dict, prices: dict) -> list:
    alerts = []
    for order in data.get("limit_orders", []):
        if order.get("status") != "active":
            continue
        ticker = order["ticker"]
        current = prices.get(ticker)
        if current is None:
            continue
        lp = order["limit_price"]
        gap_pct = round((current - lp) / lp * 100, 2)
        if order["action"] == "BUY" and current <= lp:
            alerts.append({
                "order_id": order["id"],
                "ticker": ticker,
                "action": "BUY",
                "limit_price": lp,
                "current_price": current,
                "gap_pct": gap_pct,
                "type": "BUY_TRIGGERED"
            })
        elif order["action"] == "SELL" and current >= lp:
            alerts.append({
                "order_id": order["id"],
                "ticker": ticker,
                "action": "SELL",
                "limit_price": lp,
                "current_price": current,
                "gap_pct": gap_pct,
                "type": "SELL_TRIGGERED"
            })
    return alerts


# ── Routes ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/portfolio")
def get_portfolio():
    return jsonify(load_data())


@app.route("/api/prices")
def get_prices():
    data = load_data()
    tickers = get_all_tickers(data)
    prices = refresh_prices(tickers)
    alerts = compute_alerts(data, prices)
    return jsonify({
        "prices": prices,
        "prev_closes": _prev_close_cache,
        "alerts": alerts,
        "last_updated": datetime.now(timezone.utc).isoformat()
    })


# ── Holdings ───────────────────────────────────────────────────────────────────

@app.route("/api/holdings", methods=["POST"])
def add_holding():
    body = request.get_json()
    ticker = body.get("ticker", "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    data = load_data()
    if any(h["ticker"] == ticker for h in data["holdings"]):
        return jsonify({"error": f"{ticker} already in holdings"}), 409
    data["holdings"].append({
        "ticker": ticker,
        "shares": float(body.get("shares", 0)),
        "avg_cost": float(body.get("avg_cost", 0)),
        "signal": body.get("signal", "HOLD"),
        "sector": body.get("sector", ""),
        "notes": body.get("notes", "")
    })
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/holdings/<ticker>", methods=["PUT"])
def update_holding(ticker):
    ticker = ticker.upper()
    body = request.get_json()
    data = load_data()
    for h in data["holdings"]:
        if h["ticker"] == ticker:
            for field in ("shares", "avg_cost", "signal", "sector", "notes", "thesis"):
                if field in body:
                    h[field] = float(body[field]) if field in ("shares", "avg_cost") else body[field]
            save_data(data)
            return jsonify({"ok": True})
    return jsonify({"error": "not found"}), 404


@app.route("/api/holdings/<ticker>", methods=["DELETE"])
def delete_holding(ticker):
    ticker = ticker.upper()
    data = load_data()
    before = len(data["holdings"])
    data["holdings"] = [h for h in data["holdings"] if h["ticker"] != ticker]
    if len(data["holdings"]) == before:
        return jsonify({"error": "not found"}), 404
    save_data(data)
    return jsonify({"ok": True})


# ── Limit orders ───────────────────────────────────────────────────────────────

@app.route("/api/limit_orders", methods=["POST"])
def add_limit_order():
    body = request.get_json()
    ticker = body.get("ticker", "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    data = load_data()
    data["limit_orders"].append({
        "id": "lo_" + uuid.uuid4().hex[:8],
        "ticker": ticker,
        "action": body.get("action", "BUY").upper(),
        "shares": float(body.get("shares", 0)),
        "limit_price": float(body.get("limit_price", 0)),
        "status": "active",
        "notes": body.get("notes", "")
    })
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/limit_orders/<order_id>", methods=["PUT"])
def update_limit_order(order_id):
    body = request.get_json()
    data = load_data()
    for lo in data["limit_orders"]:
        if lo["id"] == order_id:
            for field in ("ticker", "action", "notes", "status"):
                if field in body:
                    lo[field] = body[field].upper() if field in ("ticker", "action") else body[field]
            for field in ("shares", "limit_price"):
                if field in body:
                    lo[field] = float(body[field])
            save_data(data)
            return jsonify({"ok": True})
    return jsonify({"error": "not found"}), 404


@app.route("/api/limit_orders/<order_id>", methods=["DELETE"])
def delete_limit_order(order_id):
    data = load_data()
    before = len(data["limit_orders"])
    data["limit_orders"] = [lo for lo in data["limit_orders"] if lo["id"] != order_id]
    if len(data["limit_orders"]) == before:
        return jsonify({"error": "not found"}), 404
    save_data(data)
    return jsonify({"ok": True})


# ── Watchlist ──────────────────────────────────────────────────────────────────

@app.route("/api/watchlist", methods=["POST"])
def add_watchlist():
    body = request.get_json()
    ticker = body.get("ticker", "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    data = load_data()
    if any(w["ticker"] == ticker for w in data["watchlist"]):
        return jsonify({"error": f"{ticker} already in watchlist"}), 409
    data["watchlist"].append({"ticker": ticker, "notes": body.get("notes", "")})
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/watchlist/<ticker>", methods=["DELETE"])
def delete_watchlist(ticker):
    ticker = ticker.upper()
    data = load_data()
    before = len(data["watchlist"])
    data["watchlist"] = [w for w in data["watchlist"] if w["ticker"] != ticker]
    if len(data["watchlist"]) == before:
        return jsonify({"error": "not found"}), 404
    save_data(data)
    return jsonify({"ok": True})


# ── Events ─────────────────────────────────────────────────────────────────────

@app.route("/api/events", methods=["POST"])
def add_event():
    body = request.get_json()
    data = load_data()
    data.setdefault("events", []).append({
        "id": "ev_" + uuid.uuid4().hex[:8],
        "ticker": body.get("ticker", "").upper().strip(),
        "date": body.get("date") or None,
        "description": body.get("description", ""),
        "type": body.get("type", "catalyst")
    })
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/events/<event_id>", methods=["PUT"])
def update_event(event_id):
    body = request.get_json()
    data = load_data()
    for ev in data.get("events", []):
        if ev["id"] == event_id:
            for field in ("ticker", "date", "description", "type"):
                if field in body:
                    ev[field] = body[field].upper() if field == "ticker" else body[field]
            save_data(data)
            return jsonify({"ok": True})
    return jsonify({"error": "not found"}), 404


@app.route("/api/events/<event_id>", methods=["DELETE"])
def delete_event(event_id):
    data = load_data()
    before = len(data.get("events", []))
    data["events"] = [ev for ev in data.get("events", []) if ev["id"] != event_id]
    if len(data["events"]) == before:
        return jsonify({"error": "not found"}), 404
    save_data(data)
    return jsonify({"ok": True})


# ── Cash ───────────────────────────────────────────────────────────────────────

@app.route("/api/cash", methods=["PUT"])
def update_cash():
    body = request.get_json()
    data = load_data()
    data["cash"] = float(body.get("cash", data["cash"]))
    save_data(data)
    return jsonify({"ok": True})


# ── Valuation ──────────────────────────────────────────────────────────────────

@app.route("/api/valuation")
def get_valuation():
    data = load_data()
    return jsonify(data.get("valuation", []))


@app.route("/api/valuation/<ticker>", methods=["PUT"])
def update_valuation(ticker):
    ticker = ticker.upper()
    body = request.get_json()
    data = load_data()
    data.setdefault("valuation", [])
    num_fields = ("fwd_pe", "peg", "ev_ebitda", "price_sales", "trail_pe",
                  "gross_margin", "op_margin", "net_margin", "roe", "market_cap", "fcf_margin")
    for v in data["valuation"]:
        if v["ticker"] == ticker:
            for field in num_fields:
                if field in body and body[field] not in (None, ""):
                    v[field] = float(body[field])
            if "notes" in body:
                v["notes"] = body["notes"]
            if "name" in body:
                v["name"] = body["name"]
            save_data(data)
            return jsonify({"ok": True})
    # ticker not found — add it
    new_entry = {"ticker": ticker, "name": body.get("name", ticker), "notes": body.get("notes", "")}
    for field in num_fields:
        new_entry[field] = float(body[field]) if field in body and body[field] not in (None, "") else None
    data["valuation"].append(new_entry)
    save_data(data)
    return jsonify({"ok": True})


def _info_to_valuation(ticker: str, info: dict) -> dict:
    """Map yfinance .info dict to our valuation schema."""
    mc = info.get("marketCap")
    fcf = info.get("freeCashflow")
    rev = info.get("totalRevenue")
    fcf_margin = round(fcf / rev, 4) if fcf and rev and rev != 0 else None

    def _r(v):
        return round(v, 4) if isinstance(v, float) else v

    return {
        "ticker": ticker,
        "name": info.get("shortName") or info.get("longName") or ticker,
        "fwd_pe":      _r(info.get("forwardPE")),
        "trail_pe":    _r(info.get("trailingPE")),
        "peg":         _r(info.get("pegRatio")),
        "ev_ebitda":   _r(info.get("enterpriseToEbitda")),
        "price_sales": _r(info.get("priceToSalesTrailing12Months")),
        "gross_margin":_r(info.get("grossMargins")),
        "op_margin":   _r(info.get("operatingMargins")),
        "net_margin":  _r(info.get("profitMargins")),
        "roe":         _r(info.get("returnOnEquity")),
        "market_cap":  round(mc / 1e9, 1) if mc else None,
        "fcf_margin":  fcf_margin,
    }


@app.route("/api/valuation/fetch/<ticker>")
def fetch_valuation_ticker(ticker):
    """Fetch live valuation metrics, current price, and 52-week high from yfinance."""
    ticker = ticker.upper()
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.info
        result = _info_to_valuation(ticker, info)

        # Current price
        hist_1d = t.history(period="2d", interval="1m")
        if not hist_1d.empty:
            result["price"] = round(float(hist_1d["Close"].dropna().iloc[-1]), 2)

        # 52-week high
        hist_1y = t.history(period="1y", interval="1d")
        if not hist_1y.empty:
            result["high_52wk"] = round(float(hist_1y["Close"].dropna().max()), 2)

        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/valuation/refresh", methods=["POST"])
def refresh_valuation_metrics():
    """Refresh all holdings' valuation metrics from yfinance and persist."""
    data = load_data()
    valuation = data.get("valuation", [])
    if not valuation:
        return jsonify({"ok": True, "updated": 0})

    try:
        import yfinance as yf
        updated = 0
        for v in valuation:
            ticker = v["ticker"]
            try:
                info = yf.Ticker(ticker).info
                if not info:
                    continue
                fresh = _info_to_valuation(ticker, info)
                for field in ("fwd_pe", "trail_pe", "peg", "ev_ebitda", "price_sales",
                              "gross_margin", "op_margin", "net_margin", "roe", "market_cap", "fcf_margin"):
                    if fresh.get(field) is not None:
                        v[field] = fresh[field]
                # Preserve name only if yfinance returns a non-trivial value
                if fresh.get("name") and fresh["name"] != ticker:
                    v["name"] = fresh["name"]
                updated += 1
            except Exception:
                pass

        data["val_last_refreshed"] = datetime.now(timezone.utc).isoformat()
        save_data(data)
        return jsonify({"ok": True, "updated": updated, "val_last_refreshed": data["val_last_refreshed"]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/valuation/<ticker>", methods=["DELETE"])
def delete_valuation(ticker):
    ticker = ticker.upper()
    data = load_data()
    before = len(data.get("valuation", []))
    data["valuation"] = [v for v in data.get("valuation", []) if v["ticker"] != ticker]
    if len(data["valuation"]) == before:
        return jsonify({"error": "not found"}), 404
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/valuation/sync", methods=["POST"])
def sync_valuation():
    """Fetch valuation entries for any holdings that don't have one yet."""
    data = load_data()
    holdings = data.get("holdings", [])
    val_tickers = {v["ticker"] for v in data.get("valuation", [])}
    missing = [h["ticker"] for h in holdings if h["ticker"] not in val_tickers]

    if not missing:
        return jsonify({"ok": True, "fetched": []})

    fetched = []
    try:
        import yfinance as yf
        for ticker in missing:
            try:
                info = yf.Ticker(ticker).info
                if not info:
                    continue
                entry = _info_to_valuation(ticker, info)
                # Guard against a concurrent write adding it between our check and now
                if not any(v["ticker"] == ticker for v in data.get("valuation", [])):
                    data.setdefault("valuation", []).append(entry)
                    fetched.append(ticker)
            except Exception:
                pass
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})

    if fetched:
        save_data(data)

    return jsonify({"ok": True, "fetched": fetched})


# ── Macro ──────────────────────────────────────────────────────────────────────

MACRO_TICKERS = {
    "^GSPC":    {"label": "S&P 500",       "category": "equity",   "unit": "pts"},
    "^IXIC":    {"label": "NASDAQ",        "category": "equity",   "unit": "pts"},
    "^DJI":     {"label": "Dow Jones",     "category": "equity",   "unit": "pts"},
    "^VIX":     {"label": "VIX",           "category": "volatility","unit": ""},
    "CL=F":     {"label": "WTI Oil",       "category": "commodity","unit": "$/bbl"},
    "BZ=F":     {"label": "Brent Oil",     "category": "commodity","unit": "$/bbl"},
    "GC=F":     {"label": "Gold",          "category": "commodity","unit": "$/oz"},
    "^TNX":     {"label": "10Y Treasury",  "category": "rates",    "unit": "%"},
    "^IRX":     {"label": "3M T-Bill",     "category": "rates",    "unit": "%"},
    "DX-Y.NYB": {"label": "USD Index",     "category": "fx",       "unit": ""},
    "CNY=X":    {"label": "USD/CNY",       "category": "fx",       "unit": ""},
    "EURUSD=X": {"label": "EUR/USD",       "category": "fx",       "unit": ""},
}

_macro_cache: dict = {}
_macro_prev_cache: dict = {}
_macro_last_fetched: float = 0
MACRO_TTL = 120  # macro data refreshes every 2 min (less critical than stock prices)


def refresh_macro() -> dict:
    global _macro_cache, _macro_prev_cache, _macro_last_fetched
    if time.time() - _macro_last_fetched < MACRO_TTL:
        return _macro_cache

    tickers = list(MACRO_TICKERS.keys())
    try:
        import yfinance as yf
        raw = yf.download(tickers, period="2d", interval="1m",
                          auto_adjust=True, progress=False, threads=False)
        if raw.empty:
            return _macro_cache

        close = raw["Close"] if "Close" in raw.columns else raw
        today = datetime.now().date()

        for t in tickers:
            try:
                series = close[t].dropna()
                if not series.empty:
                    _macro_cache[t] = float(series.iloc[-1])
                    prev = series[series.index.date < today]
                    if not prev.empty:
                        _macro_prev_cache[t] = float(prev.iloc[-1])
            except Exception:
                pass

        _macro_last_fetched = time.time()
    except Exception:
        pass

    return _macro_cache


@app.route("/api/macro")
def get_macro():
    data = load_data()
    prices = refresh_macro()
    result = []
    for ticker, meta in MACRO_TICKERS.items():
        cur = prices.get(ticker)
        prev = _macro_prev_cache.get(ticker)
        chg = round(cur - prev, 4) if cur and prev else None
        chg_pct = round((cur - prev) / prev * 100, 2) if cur and prev and prev != 0 else None
        result.append({
            "ticker": ticker,
            "label": meta["label"],
            "category": meta["category"],
            "unit": meta["unit"],
            "price": round(cur, 4) if cur else None,
            "prev_close": round(prev, 4) if prev else None,
            "change": chg,
            "change_pct": chg_pct,
        })
    manual = data.get("macro_manual", {})
    return jsonify({
        "indicators": result,
        "manual": manual,
        "last_updated": datetime.now(timezone.utc).isoformat()
    })


@app.route("/api/52wk")
def get_52wk_high():
    tickers = request.args.get("tickers", "").split(",")
    tickers = [t.strip().upper() for t in tickers if t.strip()]
    if not tickers:
        return jsonify({})
    result = {}
    try:
        import yfinance as yf
        raw = yf.download(
            tickers if len(tickers) > 1 else tickers[0],
            period="1y", interval="1d",
            auto_adjust=True, progress=False, threads=False
        )
        close = raw["Close"] if "Close" in raw.columns else raw
        if len(tickers) == 1:
            hi = close.dropna().max()
            if hi: result[tickers[0]] = float(hi)
        else:
            for t in tickers:
                try:
                    hi = close[t].dropna().max()
                    if hi: result[t] = float(hi)
                except Exception:
                    pass
    except Exception:
        pass
    return jsonify(result)


@app.route("/api/macro/manual", methods=["PUT"])
def update_macro_manual():
    body = request.get_json()
    data = load_data()
    data.setdefault("macro_manual", {})
    for field in ("fed_rate", "cpi_yoy", "notes"):
        if field in body:
            data["macro_manual"][field] = body[field]
    save_data(data)
    return jsonify({"ok": True})


# ── Equity curve ───────────────────────────────────────────────────────────────

_equity_curve_cache: dict = {}
_equity_curve_last_fetched: float = 0
EQUITY_CURVE_TTL = 3600  # rebuild at most once per hour


@app.route("/api/equity_curve")
def get_equity_curve():
    global _equity_curve_cache, _equity_curve_last_fetched
    if time.time() - _equity_curve_last_fetched < EQUITY_CURVE_TTL and _equity_curve_cache:
        return jsonify(_equity_curve_cache)

    data = load_data()
    holdings = data.get("holdings", [])
    if not holdings:
        return jsonify({"dates": [], "portfolio_raw": [], "spy_raw": []})

    cash = data.get("cash", 0.0)
    holding_tickers = [h["ticker"] for h in holdings]
    all_tickers = holding_tickers + ["SPY"]

    try:
        import yfinance as yf
        import pandas as pd

        raw = yf.download(
            all_tickers,
            period="1y", interval="1d",
            auto_adjust=True, progress=False, threads=False,
        )
        if raw.empty:
            return jsonify({"dates": [], "portfolio_raw": [], "spy_raw": []})

        close = (raw["Close"] if "Close" in raw.columns else raw).copy()
        close = close.ffill()  # forward-fill weekends/holidays

        # Build daily portfolio value from current holdings
        port = pd.Series(0.0, index=close.index)
        for h in holdings:
            t = h["ticker"]
            if t in close.columns:
                port = port + h["shares"] * close[t].fillna(0)
        port = port + cash

        spy = close["SPY"] if "SPY" in close.columns else pd.Series(dtype=float, index=close.index)

        valid_idx = port.dropna().index
        dates = [d.date().isoformat() for d in valid_idx]
        portfolio_raw = [round(float(port[d]), 2) for d in valid_idx]
        spy_raw = [round(float(spy[d]), 4) if d in spy.index and not pd.isna(spy[d]) else None
                   for d in valid_idx]

        result = {"dates": dates, "portfolio_raw": portfolio_raw, "spy_raw": spy_raw}
        _equity_curve_cache = result
        _equity_curve_last_fetched = time.time()
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e), "dates": [], "portfolio_raw": [], "spy_raw": []})


@app.route("/api/snapshot", methods=["POST"])
def manual_snapshot():
    """Invalidate equity curve cache and force a fresh rebuild on next fetch."""
    global _equity_curve_last_fetched, _last_fetched
    _equity_curve_last_fetched = 0
    _last_fetched = 0
    return jsonify({"ok": True})


def _get_api_key() -> str:
    """Return ANTHROPIC_API_KEY from env or a .env file next to this script."""
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if key:
        return key
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line.startswith("ANTHROPIC_API_KEY="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _quarter_label(dt):
    """Convert a pandas Timestamp to a readable quarter like '2026 Q1'."""
    try:
        month = dt.month
        year = dt.year
        quarter = (month - 1) // 3 + 1
        return f"{year} Q{quarter}"
    except Exception:
        return str(dt)[:10]


def _fmt_num(n):
    """Format a raw number as $1.23B, $456M, etc. Returns None on invalid input."""
    try:
        import math
        if n is None or (isinstance(n, float) and (math.isnan(n) or math.isinf(n))):
            return None
        n = float(n)
        if abs(n) >= 1e9:
            return f"${n/1e9:.2f}B"
        if abs(n) >= 1e6:
            return f"${n/1e6:.0f}M"
        return f"${n:.0f}"
    except Exception:
        return None


def _structured_bullets(q_data: dict, news_bullets: list) -> str:
    """Build plain bullet points from financial data + news headlines (no API key needed)."""
    lines = []
    rev     = q_data.get("revenue")
    ni      = q_data.get("net_income")
    rev_yoy = q_data.get("rev_yoy")
    gm      = q_data.get("gross_margin")
    om      = q_data.get("op_margin")
    fcf     = q_data.get("fcf")

    if rev:
        rev_str = _fmt_num(rev) or ""
        if rev_yoy is not None:
            sign = "+" if rev_yoy >= 0 else ""
            lines.append(f"• Revenue {rev_str} ({sign}{rev_yoy * 100:.1f}% YoY)")
        else:
            lines.append(f"• Revenue {rev_str}")
    if ni:
        nm = (q_data.get("net_margin") or 0)
        lines.append(f"• Net income {_fmt_num(ni)} ({nm * 100:.1f}% net margin)")
    margins = []
    if gm:
        margins.append(f"Gross {gm * 100:.1f}%")
    if om:
        margins.append(f"Operating {om * 100:.1f}%")
    if margins:
        lines.append("• Margins: " + ", ".join(margins))
    if fcf:
        lines.append(f"• Free Cash Flow {_fmt_num(fcf)}")
    for b in news_bullets[:4]:
        lines.append(b)
    return "\n".join(lines)


def _fetch_external_data(ticker: str) -> str:
    """Search DuckDuckGo + scrape pages for non-Yahoo financial verification data."""
    try:
        import requests as req, urllib.parse
        from bs4 import BeautifulSoup

        corpus = []
        query = f"{ticker} quarterly earnings revenue free cash flow results 2025 2026"
        ddg_url = (
            "https://html.duckduckgo.com/html/?q="
            + urllib.parse.quote_plus(query)
            + "&df=y"
        )
        r = req.get(
            ddg_url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=10,
        )
        soup = BeautifulSoup(r.content, "html.parser")

        # Collect snippets from search results
        snippets = []
        for el in soup.select(".result__snippet, .result__title"):
            t = el.get_text().strip()
            if t and len(t) > 20:
                snippets.append(t)
        if snippets:
            corpus.append("[DuckDuckGo search snippets]\n" + "\n".join(snippets[:12]))

        # Fetch top 2 non-Yahoo result pages
        skip_domains = {"yahoo", "twitter", "facebook", "instagram", "reddit", "tiktok"}
        links = [a.get("href", "") for a in soup.select(".result__title a")]
        fetched = 0
        for href in links:
            if fetched >= 2:
                break
            if not href or any(d in href for d in skip_domains):
                continue
            try:
                pr = req.get(
                    href,
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=6,
                )
                ps = BeautifulSoup(pr.content, "html.parser")
                for tag in ps(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                text = " ".join(ps.get_text().split())[:2500]
                if any(kw in text.lower() for kw in ["revenue", "earnings", "cash flow", "income", "billion"]):
                    corpus.append(f"[{href[:100]}]\n{text}")
                    fetched += 1
            except Exception:
                pass

        return "\n\n".join(corpus)[:7000]
    except Exception:
        return ""


def _ai_summarize(api_key: str, ticker: str, q_data: dict, news_corpus: str, external_corpus: str = "") -> str:
    """Call Claude to synthesize financial data + web sources into bullet points."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        q_label  = q_data.get("quarter", "")
        rev      = _fmt_num(q_data.get("revenue")) or "—"
        ni       = _fmt_num(q_data.get("net_income")) or "—"
        rev_yoy  = q_data.get("rev_yoy") or 0
        gm       = (q_data.get("gross_margin") or 0) * 100
        om       = (q_data.get("op_margin")    or 0) * 100
        nm       = (q_data.get("net_margin")   or 0) * 100
        fcf_yf   = _fmt_num(q_data.get("fcf")) or "—"
        sign     = "+" if rev_yoy >= 0 else ""

        prompt = (
            f"You are a senior equity analyst. Write a concise earnings summary for {ticker} {q_label}.\n\n"
            f"YAHOO FINANCE DATA (FCF is often wrong — verify it from external sources below):\n"
            f"Revenue: {rev} ({sign}{rev_yoy * 100:.1f}% YoY) | Net Income: {ni}\n"
            f"Gross Margin: {gm:.1f}% | Op Margin: {om:.1f}% | Net Margin: {nm:.1f}% | FCF (yfinance, may be wrong): {fcf_yf}\n\n"
            f"EXTERNAL VERIFICATION DATA (non-Yahoo sources — use these to correct wrong numbers):\n"
            f"{external_corpus[:3500]}\n\n"
            f"RECENT NEWS & COMMENTARY:\n"
            f"{news_corpus[:2000]}\n\n"
            f"CRITICAL FORMATTING RULES:\n"
            f"1. Output ONLY bullet points — NO section headings, NO intro text, NO ## markers.\n"
            f"2. Every line MUST start with '• ' (bullet + space).\n"
            f"3. Wrap key figures and important phrases in **double asterisks** for bold: e.g. **Revenue $26.9B**.\n"
            f"4. Verify FCF from external sources; cite the corrected figure if it differs from yfinance.\n"
            f"5. Write exactly 6–8 bullets covering: (a) revenue vs expectations, (b) key business updates "
            f"(hyperscaler contracts, AI wins, product launches), (c) margins + verified FCF, "
            f"(d) management guidance, (e) notable risks.\n"
            f"6. Be specific with numbers. No vague sentences.\n\n"
            f"OUTPUT (start immediately with first bullet, nothing before it):"
        )
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=900,
            messages=[{"role": "user", "content": prompt}],
        )
        return msg.content[0].text.strip()
    except Exception:
        return _structured_bullets(q_data, [])


def _maybe_add_web_quarter(ticker: str, existing_quarters: list, api_key: str) -> list:
    """
    If yfinance is missing the most-recently-completed quarter (e.g. reported today),
    fetch financials + summary from public web and prepend a web-sourced entry.
    """
    from datetime import date
    import json as _json

    if not api_key:
        return existing_quarters

    today = date.today()
    year, month = today.year, today.month

    # Determine most recently completed quarter-end date
    if month >= 10:
        q_end   = date(year, 9, 30);   q_label = f"{year} Q3"
    elif month >= 7:
        q_end   = date(year, 6, 30);   q_label = f"{year} Q2"
    elif month >= 4:
        q_end   = date(year, 3, 31);   q_label = f"{year} Q1"
    else:
        q_end   = date(year - 1, 12, 31); q_label = f"{year - 1} Q4"

    # Only proceed if earnings have had time to be published (≥7 days after quarter-end)
    if (today - q_end).days < 7:
        return existing_quarters

    # Check if yfinance already covers this quarter
    if existing_quarters:
        try:
            latest_yf = date.fromisoformat(existing_quarters[0].get("date", ""))
            if latest_yf >= q_end:
                return existing_quarters
        except Exception:
            pass

    # Search the web for the missing quarter's earnings
    try:
        import requests as req, urllib.parse
        from bs4 import BeautifulSoup

        query = f"{ticker} {q_label} earnings revenue net income results"
        ddg_url = (
            "https://html.duckduckgo.com/html/?q="
            + urllib.parse.quote_plus(query)
            + "&df=m"
        )
        r = req.get(
            ddg_url,
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            timeout=10,
        )
        soup = BeautifulSoup(r.content, "html.parser")

        web_text = ""
        for el in soup.select(".result__snippet, .result__title"):
            t = el.get_text().strip()
            if t and len(t) > 20:
                web_text += t + "\n"

        skip_domains = {"yahoo", "twitter", "facebook", "instagram", "reddit"}
        # DuckDuckGo wraps links as //duckduckgo.com/l/?uddg=<encoded_url>
        raw_links = [a.get("href", "") for a in soup.select(".result__title a")]
        decoded_links = []
        for href in raw_links:
            if "uddg=" in href:
                try:
                    real = urllib.parse.unquote(href.split("uddg=")[1].split("&")[0])
                    decoded_links.append(real)
                except Exception:
                    pass
            elif href.startswith("http"):
                decoded_links.append(href)
        fetched = 0
        for href in decoded_links:
            if fetched >= 3:
                break
            if not href or any(d in href for d in skip_domains):
                continue
            try:
                pr = req.get(href, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
                ps = BeautifulSoup(pr.content, "html.parser")
                for tag in ps(["script", "style", "nav", "footer", "header", "aside"]):
                    tag.decompose()
                text = " ".join(ps.get_text().split())[:2500]
                if any(kw in text.lower() for kw in ["revenue", "earnings", "billion", "income"]):
                    web_text += f"\n[{href[:80]}]\n{text}\n"
                    fetched += 1
            except Exception:
                pass

        if len(web_text.strip()) < 100:
            return existing_quarters

        # Ask Claude to extract financials + generate bullets
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        prompt = (
            f"You are a financial analyst. From the web sources below about {ticker} {q_label} earnings, "
            f"do two things:\n"
            f"1. Extract financial metrics. CRITICAL: return ALL dollar values as full integers in USD "
            f"(e.g. $35.4B = 35400000000, NOT 35.4 or 35400). Convert currencies first: TWD÷32, CNY÷7.3, HKD÷7.8.\n"
            f"   Margins must be 0–1 decimals (e.g. 58.8% = 0.588). rev_yoy is 0–1 (e.g. 41.6% = 0.416).\n"
            f"2. If earnings have been reported, write 6–8 bullet points each starting with '• ' on separate lines, "
            f"wrapping key numbers in **double asterisks**. "
            f"If earnings have NOT been reported yet, set bullets to null.\n\n"
            f"WEB SOURCES:\n{web_text[:5500]}\n\n"
            f"Return ONLY valid JSON — bullets must be a newline-separated STRING, not an array:\n"
            f'{{"revenue":null,"net_income":null,"gross_margin":null,"op_margin":null,'
            f'"net_margin":null,"fcf":null,"rev_yoy":null,"bullets":null}}'
        )
        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if "```" in raw:
            raw = raw.split("```")[1].lstrip("json").strip()
        data = _json.loads(raw)

        # bullets may be returned as list or string — normalize to newline-separated string
        raw_bullets = data.get("bullets")
        if isinstance(raw_bullets, list):
            bullets = "\n".join(str(b) for b in raw_bullets).strip()
        else:
            bullets = (raw_bullets or "").strip()

        # Skip if earnings haven't been reported yet (no useful bullets)
        if not bullets or len(bullets) < 50:
            return existing_quarters

        # Safety: scale up revenue/income if Claude returned billions instead of full USD
        def _scale_to_dollars(val):
            if val is None:
                return None
            fv = float(val)
            # If value looks like billions (< 10000) or millions (< 1e7), scale up
            if 0 < abs(fv) < 1e4:
                return fv * 1e9   # assume billions
            if 0 < abs(fv) < 1e7:
                return fv * 1e6   # assume millions
            return fv

        # Safety: margins might be returned as percentages (58.8) instead of decimals (0.588)
        def _to_margin(val):
            if val is None:
                return None
            fv = float(val)
            return round(fv / 100.0, 4) if fv > 1.5 else round(fv, 4)

        q_entry = {
            "quarter":      q_label,
            "date":         q_end.isoformat(),
            "revenue":      _scale_to_dollars(data.get("revenue")),
            "net_income":   _scale_to_dollars(data.get("net_income")),
            "gross_margin": _to_margin(data.get("gross_margin")),
            "op_margin":    _to_margin(data.get("op_margin")),
            "net_margin":   _to_margin(data.get("net_margin")),
            "fcf":          _scale_to_dollars(data.get("fcf")),
            "rev_yoy":      data.get("rev_yoy"),
            "summary":      "",
            "bullets":      bullets,
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "web_sourced":  True,
        }
        return [q_entry] + existing_quarters
    except Exception:
        return existing_quarters


@app.route("/api/holdings/<ticker>/summaries/refresh", methods=["POST"])
def refresh_summaries(ticker):
    ticker = ticker.upper()
    try:
        import yfinance as yf, math, requests as req
        from bs4 import BeautifulSoup

        tk = yf.Ticker(ticker)

        # ── Step 1: Quarterly financials ──────────────────────────────────────
        income   = tk.quarterly_income_stmt
        cashflow = tk.quarterly_cashflow
        quarters = []

        # If yfinance has no income data at all, skip to web-only path (handled in Step 4b)
        if income is None or income.empty:
            quarters = []
            # Skip to Steps 2–4b which will build bullets from external + web sources
            news_corpus = ""
            news_bullets = []
            try:
                raw_news = tk.news or []
                for item in raw_news[:6]:
                    cnt   = item.get("content", item)
                    title = (cnt.get("title") or item.get("title") or "").strip()
                    if title:
                        news_corpus += f"\n{title}"
                        news_bullets.append(f"• {title}")
            except Exception:
                pass
            external_corpus = _fetch_external_data(ticker)
            api_key = _get_api_key()
            quarters = _maybe_add_web_quarter(ticker, [], api_key or "")
            if not quarters:
                return jsonify({"error": "No financial data available from any source", "summaries": []})
            data = load_data()
            for h in data.get("holdings", []):
                if h["ticker"].upper() == ticker:
                    h["summaries"]           = quarters
                    h["summaries_refreshed"] = datetime.now(timezone.utc).isoformat()
                    break
            save_data(data)
            return jsonify({
                "summaries":  quarters,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "ai_used":    bool(api_key),
            })

        cols = list(income.columns)

        for i, col in enumerate(cols[:8]):
            try:
                def _get(df, row, c=col):
                    try:
                        val = df.loc[row, c] if row in df.index else None
                        if val is None:
                            return None
                        fv = float(val)
                        return None if math.isnan(fv) or math.isinf(fv) else fv
                    except Exception:
                        return None

                rev = _get(income, "Total Revenue")
                ni  = _get(income, "Net Income")
                gp  = _get(income, "Gross Profit")
                oi  = _get(income, "Operating Income")

                # yfinance returns TSM (TSMC ADR) financials in TWD, not USD.
                # Detect by quarterly revenue > 100 billion (impossible in USD for any single quarter).
                # 1 USD ≈ 32 TWD (2024-2026 avg). Margins (ratios) are unaffected.
                _TWD_USD = 1 / 32.0
                _needs_fx = (ticker == "TSM" and rev is not None and rev > 1e11)

                if _needs_fx:
                    rev = rev * _TWD_USD if rev else rev
                    ni  = ni  * _TWD_USD if ni  else ni
                    gp  = gp  * _TWD_USD if gp  else gp
                    oi  = oi  * _TWD_USD if oi  else oi

                yoy_col = cols[i + 4] if i + 4 < len(cols) else None
                rev_yoy = None
                if rev and yoy_col is not None:
                    rev_prev_raw = _get(income, "Total Revenue", yoy_col)
                    if rev_prev_raw is not None and _needs_fx:
                        rev_prev_raw = rev_prev_raw * _TWD_USD
                    rev_prev = rev_prev_raw
                    if rev_prev and rev_prev != 0:
                        rev_yoy = round((rev - rev_prev) / abs(rev_prev), 4)

                fcf = None
                if cashflow is not None and not cashflow.empty and col in cashflow.columns:
                    ocf   = _get(cashflow, "Operating Cash Flow")
                    capex = _get(cashflow, "Capital Expenditure")
                    if ocf is not None:
                        raw_fcf = ocf + (capex or 0)
                        # Apply same TWD→USD fix if income data needed it,
                        # OR if cashflow alone looks like TWD (>10× revenue in USD)
                        if _needs_fx:
                            fcf = raw_fcf * _TWD_USD
                        elif rev and abs(raw_fcf) > 10 * abs(rev):
                            fcf = raw_fcf * _TWD_USD
                        else:
                            fcf = raw_fcf

                gm = round(gp / rev, 4) if (gp and rev and rev != 0) else None
                om = round(oi / rev, 4) if (oi and rev and rev != 0) else None
                nm = round(ni / rev, 4) if (ni and rev and rev != 0) else None

                parts = []
                if rev:
                    rev_str = _fmt_num(rev)
                    if rev_yoy is not None:
                        parts.append(f"Revenue {rev_str} ({'+' if rev_yoy >= 0 else ''}{rev_yoy * 100:.1f}% YoY)")
                    else:
                        parts.append(f"Revenue {rev_str}")
                if ni:
                    parts.append(f"Net income {_fmt_num(ni)}")
                if gm:
                    parts.append(f"Gross margin {gm * 100:.1f}%")
                if om:
                    parts.append(f"Op margin {om * 100:.1f}%")
                if fcf:
                    parts.append(f"FCF {_fmt_num(fcf)}")

                try:
                    q_label = _quarter_label(col)
                    q_date  = col.strftime("%Y-%m-%d")
                except Exception:
                    q_label = str(col)[:10]
                    q_date  = str(col)[:10]

                quarters.append({
                    "quarter":      q_label,
                    "date":         q_date,
                    "revenue":      rev,
                    "net_income":   ni,
                    "gross_margin": gm,
                    "op_margin":    om,
                    "net_margin":   nm,
                    "fcf":          fcf,
                    "rev_yoy":      rev_yoy,
                    "summary":      ". ".join(parts) + "." if parts else "No data.",
                })
            except Exception:
                continue

        # ── Step 2: Scrape recent news ────────────────────────────────────────
        news_corpus  = ""
        news_bullets = []
        try:
            raw_news = tk.news or []
            for item in raw_news[:8]:
                cnt      = item.get("content", item)
                title    = (cnt.get("title") or item.get("title") or "").strip()
                snippet  = (cnt.get("summary") or item.get("summary") or "").strip()
                pub_date = cnt.get("pubDate") or ""
                # URL
                url = None
                canonical = cnt.get("canonicalUrl", {})
                if isinstance(canonical, dict):
                    url = canonical.get("url")
                if not url:
                    url = item.get("link") or item.get("url") or ""

                if title:
                    news_corpus  += f"\n[{pub_date[:10]}] {title}"
                    if snippet:
                        news_corpus += f"\n  {snippet}"
                    news_bullets.append(f"• {title}")

                if url:
                    try:
                        r = req.get(
                            url,
                            headers={"User-Agent": "Mozilla/5.0"},
                            timeout=6,
                        )
                        soup = BeautifulSoup(r.content, "html.parser")
                        for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
                            tag.decompose()
                        text = " ".join(soup.get_text().split())[:2500]
                        if text:
                            news_corpus += f"\n  {text}"
                    except Exception:
                        pass
        except Exception:
            pass

        # ── Step 3: Fetch external verification data (non-Yahoo) ─────────────
        external_corpus = _fetch_external_data(ticker)

        # ── Step 4: Generate AI bullets (or fallback) ─────────────────────────
        api_key = _get_api_key()
        for q in quarters:
            if api_key:
                q["bullets"] = _ai_summarize(api_key, ticker, q, news_corpus, external_corpus)
            else:
                q["bullets"] = _structured_bullets(q, news_bullets)
            q["fetched_at"] = datetime.now(timezone.utc).isoformat()

        # ── Step 4b: Prepend web-sourced entry if most recent quarter missing ──
        quarters = _maybe_add_web_quarter(ticker, quarters, api_key or "")

        # ── Step 5: Persist ───────────────────────────────────────────────────
        data = load_data()
        for h in data.get("holdings", []):
            if h["ticker"].upper() == ticker:
                h["summaries"]           = quarters
                h["summaries_refreshed"] = datetime.now(timezone.utc).isoformat()
                break
        save_data(data)

        return jsonify({
            "summaries":  quarters,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "ai_used":    bool(api_key),
        })

    except Exception as e:
        return jsonify({"error": str(e), "summaries": []}), 500


@app.route("/api/watchlist/reorder", methods=["PUT"])
def reorder_watchlist():
    tickers = (request.get_json() or {}).get("tickers", [])
    data = load_data()
    lookup = {w["ticker"]: w for w in data.get("watchlist", [])}
    data["watchlist"] = [lookup[t] for t in tickers if t in lookup]
    save_data(data)
    return jsonify({"ok": True})


@app.route("/api/risk-metrics")
def get_risk_metrics():
    global _risk_cache, _risk_last_fetched
    if time.time() - _risk_last_fetched < RISK_TTL and _risk_cache:
        return jsonify(_risk_cache)

    data = load_data()
    holdings = data.get("holdings", [])
    if not holdings:
        return jsonify({"beta": None, "volatility": None, "sharpe": None, "warning": "no holdings"})

    try:
        import numpy as np
        import yfinance as yf

        # Risk-free rate from macro cache (^IRX is quoted as %, e.g. 4.8 = 4.8%)
        rf_raw = _macro_cache.get("^IRX")
        rf = (rf_raw / 100) if rf_raw and rf_raw > 0 else 0.045

        # SPY 1-year daily history
        spy_raw = yf.download("SPY", period="1y", interval="1d",
                              auto_adjust=True, progress=False, threads=False)
        if spy_raw.empty:
            return jsonify({"beta": None, "volatility": None, "sharpe": None,
                            "warning": "SPY data unavailable"})
        spy_close = spy_raw["Close"].squeeze().dropna()
        spy_returns = spy_close.pct_change().dropna()

        # Value-weighted portfolio returns
        prices_snap = dict(_price_cache)
        total_val = sum(h["shares"] * prices_snap.get(h["ticker"], h["avg_cost"])
                        for h in holdings)
        if total_val == 0:
            return jsonify({"beta": None, "volatility": None, "sharpe": None,
                            "warning": "zero portfolio value"})

        ticker_list = list({h["ticker"] for h in holdings})
        hist_raw = yf.download(
            ticker_list if len(ticker_list) > 1 else ticker_list[0],
            period="1y", interval="1d", auto_adjust=True, progress=False, threads=False
        )

        port_returns = None
        for h in holdings:
            t = h["ticker"]
            price = prices_snap.get(t, h["avg_cost"])
            weight = (h["shares"] * price) / total_val
            try:
                close_col = (hist_raw["Close"][t] if len(ticker_list) > 1
                             else hist_raw["Close"].squeeze())
                close_col = close_col.dropna()
                if len(close_col) < 30:
                    continue
                ret = close_col.pct_change().dropna() * weight
                port_returns = ret if port_returns is None else port_returns.add(ret, fill_value=0)
            except Exception:
                continue

        if port_returns is None or len(port_returns) < 30:
            return jsonify({"beta": None, "volatility": None, "sharpe": None,
                            "warning": "insufficient history"})

        p, s = port_returns.align(spy_returns, join="inner")
        if len(p) < 30:
            return jsonify({"beta": None, "volatility": None, "sharpe": None,
                            "warning": "insufficient overlapping dates"})

        cov = np.cov(p.values, s.values)
        beta = round(float(cov[0, 1] / cov[1, 1]), 3)
        vol = round(float(p.std() * np.sqrt(252)), 4)
        ann_ret = float(p.mean() * 252)
        sharpe = round((ann_ret - rf) / vol, 3) if vol > 0 else None

        result = {
            "beta": beta,
            "volatility": vol,
            "sharpe": sharpe,
            "risk_free_rate": round(rf, 4),
            "trading_days": len(p),
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "warning": None,
        }
        _risk_cache = result
        _risk_last_fetched = time.time()
        return jsonify(result)

    except Exception as e:
        return jsonify({"beta": None, "volatility": None, "sharpe": None,
                        "warning": str(e)})


if __name__ == "__main__":
    app.run(debug=True, port=5001)
