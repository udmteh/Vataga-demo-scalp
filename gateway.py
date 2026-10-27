import asyncio
import json
import time
import uuid
import os
import sqlite3
import datetime
import ssl
import sys
import aiohttp
from aiohttp import web, WSMsgType
import urllib.request
import random

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Ports
PROXY_PORT = 8989
MOCK_PORT = 8990
INITIAL_BALANCE = 50.0 # $50.00 virtual deposit

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
STATS_FILE = os.path.join(DATA_DIR, "vataga_demo_trades.json")

CRT_PATH = os.path.join(os.path.dirname(__file__), "gateway.crt")
KEY_PATH = os.path.join(os.path.dirname(__file__), "gateway.key")

# Targets to intercept
MOCK_HOSTS = {
    "api.bybit.com",
    "api-testnet.bybit.com",
    "stream.bybit.com",
    "stream-testnet.bybit.com",
    "stream.bybit-tr.com",
    "stream.bybit.id",
    "stream.bybit.kz",
    "stream.bybitgeorgia.ge",
    "fapi.binance.com",
    "dapi.binance.com",
    "api.binance.com",
    "fstream.binance.com",
    "stream.binance.com"
}

# ----------------- VIRTUAL WALLET -----------------

class VirtualWallet:
    def __init__(self, initial_balance=50.0):
        self.balance = initial_balance
        self.initial_balance = initial_balance
        self.binance_positions = {}
        self.bybit_positions = {}
        self.open_orders = {}
        self.trades_history = []
        self.binance_subscribers = []
        self.bybit_subscribers = []
        self.subscribers = self.binance_subscribers
        self.cached_quotes = {}
        self._http_session = None
        self.symbol_leverage = {}
        self.default_leverage = 50
        self.load_history()

    @property
    def positions(self):
        res = dict(self.binance_positions)
        res.update(self.bybit_positions)
        return res

    @positions.setter
    def positions(self, val):
        if isinstance(val, dict):
            if "binance" in val or "bybit" in val:
                self.binance_positions = dict(val.get("binance", {}))
                self.bybit_positions = dict(val.get("bybit", {}))
            else:
                self.binance_positions = dict(val)
                self.bybit_positions = {}
        else:
            self.binance_positions = {}
            self.bybit_positions = {}

    def load_history(self):
        if os.path.exists(STATS_FILE):
            try:
                with open(STATS_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.balance = float(data.get("balance", self.initial_balance))
                    pos_data = data.get("positions", {})
                    if isinstance(pos_data, dict) and ("binance" in pos_data or "bybit" in pos_data):
                        self.binance_positions = dict(pos_data.get("binance", {}))
                        self.bybit_positions = dict(pos_data.get("bybit", {}))
                    elif isinstance(pos_data, dict):
                        self.binance_positions = dict(pos_data)
                        self.bybit_positions = {}
                    self.trades_history = data.get("trades", [])
                    self.symbol_leverage = data.get("symbol_leverage", {})
            except Exception as e:
                print(f"[WALLET] Error loading history: {e}")

    def save_history(self):
        try:
            with open(STATS_FILE, "w", encoding="utf-8") as f:
                json.dump({
                    "balance": self.balance,
                    "initial_balance": self.initial_balance,
                    "positions": {
                        "binance": self.binance_positions,
                        "bybit": self.bybit_positions
                    },
                    "symbol_leverage": self.symbol_leverage,
                    "trades": self.trades_history[-100:]
                }, f, indent=2)
        except Exception as e:
            print(f"[WALLET] Error saving history: {e}")

    async def broadcast_binance(self, payload):
        dead = []
        for ws in self.binance_subscribers:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            if d in self.binance_subscribers:
                self.binance_subscribers.remove(d)

    async def broadcast_bybit(self, payload):
        dead = []
        for ws in self.bybit_subscribers:
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for d in dead:
            if d in self.bybit_subscribers:
                self.bybit_subscribers.remove(d)

    async def broadcast(self, payload):
        await self.broadcast_binance(payload)

    def get_bybit_wallet_ws_payload(self, now_ms=None):
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        bal_str = f"{self.balance:.4f}"
        def make_entry(acc_type):
            return {
                "accountType": acc_type,
                "accountIMRate": "0",
                "accountMMRate": "0",
                "totalEquity": bal_str,
                "totalWalletBalance": bal_str,
                "totalMarginBalance": bal_str,
                "totalAvailableBalance": bal_str,
                "totalPerpUPL": "0",
                "totalInitialMargin": "0",
                "totalMaintenanceMargin": "0",
                "coin": [{
                    "coin": "USDT",
                    "equity": bal_str,
                    "usdValue": bal_str,
                    "walletBalance": bal_str,
                    "availableToWithdraw": bal_str,
                    "availableToBorrow": "",
                    "borrowAmount": "0",
                    "accruedInterest": "0",
                    "totalOrderIM": "0",
                    "totalPositionIM": "0",
                    "totalPositionMM": "0",
                    "unrealisedPnl": "0",
                    "cumRealisedPnl": "0"
                }]
            }
        return {
            "topic": "wallet",
            "id": f"wal_{now_ms}",
            "creationTime": now_ms,
            "data": [
                make_entry("SPOT"),
                make_entry("CONTRACT"),
                make_entry("UNIFIED")
            ]
        }

    def get_bybit_position_ws_payload(self, now_ms=None, symbol_filter=None):
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        data = []
        for sym, p in self.bybit_positions.items():
            if symbol_filter and sym != symbol_filter:
                continue
            size = p.get("size", 0.0)
            side = p.get("side", "None")
            lev = self.symbol_leverage.get(sym, self.default_leverage)
            entry_price = p.get("entry_price", 0.0)
            pos_val = size * entry_price
            im = (pos_val / lev) if lev > 0 else 0.0
            data.append({
                "category": "linear",
                "positionIdx": 0,
                "tradeMode": 0,
                "riskId": 1,
                "riskLimitValue": "2000000",
                "symbol": sym,
                "side": side if size > 0 else "None",
                "size": str(size),
                "entryPrice": str(entry_price),
                "leverage": str(lev),
                "positionValue": f"{pos_val:.4f}",
                "positionBalance": "0",
                "markPrice": str(entry_price),
                "positionIM": f"{im:.4f}",
                "positionMM": "0",
                "unrealisedPnl": "0",
                "cumRealisedPnl": "0",
                "positionStatus": "Normal",
                "autoAddMargin": 0,
                "updatedTime": str(now_ms)
            })
        return {
            "topic": "position",
            "id": f"pos_{now_ms}",
            "creationTime": now_ms,
            "data": data
        }

    def get_bybit_order_ws_payload(self, now_ms=None, order_items=None):
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        data = []
        if order_items is None:
            order_items = [o for o in self.open_orders.values() if o.get("exchange") == "bybit"]
        for o in order_items:
            oid = str(o.get("orderId", ""))
            link_id = str(o.get("clientOrderId", o.get("orderLinkId", oid)))
            sym = o.get("symbol", "BTCUSDT")
            side = "Buy" if o.get("side", "BUY").upper() in ["BUY", "B"] else "Sell"
            price = str(o.get("price", 0.0))
            qty = str(o.get("qty", 0.0))
            status = o.get("orderStatus", "New")
            data.append({
                "category": "linear",
                "orderId": oid,
                "orderLinkId": link_id,
                "symbol": sym,
                "price": price,
                "qty": qty,
                "side": side,
                "orderType": o.get("type", "Limit"),
                "orderStatus": status,
                "avgPrice": str(o.get("avgPrice", price)),
                "cumExecQty": str(o.get("cumExecQty", 0.0)),
                "cumExecValue": str(o.get("cumExecValue", 0.0)),
                "cumExecFee": "0.0001",
                "timeInForce": "GTC",
                "createType": "CreateByUser",
                "cancelType": "CancelByUser" if status == "Cancelled" else "UNKNOWN",
                "rejectReason": "EC_NoError",
                "positionIdx": 0,
                "reduceOnly": o.get("reduceOnly", False),
                "closeOnTrigger": False,
                "leavesQty": qty if status == "New" else "0",
                "leavesValue": str(round(float(qty) * float(price), 4)) if status == "New" else "0",
                "createdTime": str(o.get("time", now_ms)),
                "updatedTime": str(now_ms)
            })
        return {
            "topic": "order",
            "id": f"ord_{now_ms}",
            "creationTime": now_ms,
            "data": data
        }

    def get_bybit_execution_ws_payload(self, symbol, side, qty, price, order_id, client_order_id, now_ms=None):
        if now_ms is None:
            now_ms = int(time.time() * 1000)
        val = round(qty * price, 4)
        fee = round(val * 0.00055, 4)
        side_norm = "Buy" if side.upper() in ["BUY", "B"] else "Sell"
        return {
            "topic": "execution",
            "id": f"exec_{now_ms}",
            "creationTime": now_ms,
            "data": [{
                "category": "linear",
                "symbol": symbol,
                "isLeverage": "0",
                "orderId": str(order_id),
                "orderLinkId": str(client_order_id),
                "side": side_norm,
                "orderPrice": str(price),
                "orderQty": str(qty),
                "orderType": "Limit",
                "execId": f"exec_{uuid.uuid4().hex[:10]}",
                "execPrice": str(price),
                "execQty": str(qty),
                "execFee": str(fee),
                "execType": "Trade",
                "execValue": str(val),
                "execTime": str(now_ms),
                "isMaker": False,
                "feeRate": "0.00055"
            }]
        }

    async def broadcast_binance_order_update(self, order_data, status="NEW"):
        now_ms = int(time.time() * 1000)
        oid_raw = order_data.get("orderId", 0)
        oid = int(oid_raw) if str(oid_raw).isdigit() else int(time.time() * 1000)
        sym = order_data.get("symbol", "")
        side = order_data.get("side", "BUY").upper()
        price = str(order_data.get("price", 0.0))
        qty = str(order_data.get("qty", 0.0))
        cid = order_data.get("clientOrderId", order_data.get("orderLinkId", f"ord_{oid}"))
        await self.broadcast_binance({
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "o": {
                "s": sym,
                "c": cid,
                "S": side,
                "o": "LIMIT",
                "f": "GTC",
                "q": qty,
                "p": price,
                "ap": "0",
                "sp": "0",
                "x": status,
                "X": status,
                "i": oid,
                "l": "0",
                "z": "0",
                "L": "0",
                "n": "0",
                "N": "USDT",
                "T": now_ms,
                "t": 0,
                "b": "0",
                "a": "0",
                "m": False,
                "R": False,
                "wt": "CONTRACT_PRICE",
                "ot": "LIMIT",
                "ps": "BOTH",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0
            }
        })

    async def get_http_session(self):
        if self._http_session is None or self._http_session.closed:
            self._http_session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2.5))
        return self._http_session

    async def get_market_quote(self, symbol):
        clean_sym = symbol.replace("USDT", "") + "USDT"
        now = time.time()
        cached = self.cached_quotes.get(clean_sym)
        if cached and (now - cached["ts"] < 0.25):
            return cached

        session = await self.get_http_session()

        # 1. Primary: Bybit Linear Tickers (fastest in RU, ~0.2s, never 451)
        try:
            url_bybit = f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={clean_sym}"
            async with session.get(url_bybit) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        items = data.get("result", {}).get("list", [])
                        if items:
                            it = items[0]
                            last = float(it.get("lastPrice", 0.0))
                            bid = float(it.get("bid1Price") or last)
                            ask = float(it.get("ask1Price") or last)
                            if bid > 0 and ask > 0:
                                q = {"bid": bid, "ask": ask, "last": last, "ts": now}
                                self.cached_quotes[clean_sym] = q
                                return q
        except Exception:
            pass

        # 2. Fallback: Binance Vision bookTicker
        try:
            url_vis = f"https://data-api.binance.vision/api/v3/ticker/bookTicker?symbol={clean_sym}"
            async with session.get(url_vis) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bid = float(data.get("bidPrice", 0.0))
                    ask = float(data.get("askPrice", 0.0))
                    if bid > 0 and ask > 0:
                        q = {"bid": bid, "ask": ask, "last": (bid + ask) / 2, "ts": now}
                        self.cached_quotes[clean_sym] = q
                        return q
        except Exception:
            pass

        return cached

    def get_market_price(self, symbol):
        clean_sym = symbol.replace("USDT", "") + "USDT"
        if clean_sym in self.cached_quotes:
            return self.cached_quotes[clean_sym]["last"]
        try:
            url = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={clean_sym}"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                data = json.loads(resp.read().decode())
                return float(data.get("price", 0.0))
        except Exception:
            return 0.0

    def can_place_order(self, symbol, side, qty, price, is_binance=True):
        side_norm = "Buy" if side.upper() in ["BUY", "B"] else "Sell"
        positions = self.binance_positions if is_binance else self.bybit_positions
        pos = positions.get(symbol, {})
        # If reducing/closing position, always allowed!
        if pos.get("size", 0.0) > 1e-6 and pos.get("side") != side_norm:
            return True, ""

        leverage = self.symbol_leverage.get(symbol, pos.get("leverage", self.default_leverage))
        used_margin = sum(
            (p["size"] * p["entry_price"]) / p.get("leverage", self.default_leverage)
            for p in positions.values() if p.get("size", 0.0) > 1e-6
        )
        target_exch = "binance" if is_binance else "bybit"
        orders_margin = sum(
            (o["qty"] * o["price"]) / self.symbol_leverage.get(o.get("symbol", symbol), self.default_leverage)
            for o in self.open_orders.values() if o.get("exchange") == target_exch
        )
        order_price = price or self.get_market_price(symbol) or 0.0
        if order_price <= 0:
            cached = self.cached_quotes.get(symbol)
            order_price = cached["last"] if cached else 1.0

        new_order_margin = (qty * order_price) / leverage

        # Margin check: total margin must not exceed virtual deposit balance * 1.05
        if (used_margin + orders_margin + new_order_margin) > (self.balance * 1.05):
            return False, f"Margin is insufficient. Balance: ${self.balance:.2f}, Used: ${used_margin + orders_margin:.2f}, Order: ${new_order_margin:.2f}"

        return True, ""

    async def execute_fill(self, symbol, side, qty, exec_price, client_order_id, order_type="LIMIT", is_binance=True, order_id_num=None):
        now_ms = int(time.time() * 1000)
        if order_id_num is None:
            order_id_num = int(time.time() * 1000) * 1000 + (random.randint(100, 999) % 1000000000)
        order_id = f"ord_{order_id_num}"
        side_norm = "Buy" if side.upper() in ["BUY", "B"] else "Sell"
        exch_label = "BINANCE" if is_binance else "BYBIT"

        positions = self.binance_positions if is_binance else self.bybit_positions
        pos = positions.get(symbol, {"side": "None", "size": 0.0, "entry_price": 0.0, "leverage": 50})
        pnl = 0.0
        if pos.get("size", 0.0) > 1e-6 and pos.get("side") != side_norm:
            # Closing / reducing
            close_qty = min(qty, pos["size"])
            if pos["side"] == "Buy":
                pnl = (exec_price - pos["entry_price"]) * close_qty
            else:
                pnl = (pos["entry_price"] - exec_price) * close_qty

            fee = (exec_price * close_qty) * 0.0005
            net_pnl = pnl - fee
            self.balance += net_pnl
            new_size = max(0.0, round(pos["size"] - close_qty, 8))
            if new_size < 1e-6:
                positions[symbol] = {"side": "None", "size": 0.0, "entry_price": 0.0, "leverage": pos.get("leverage", 50)}
                pos = positions[symbol]
            else:
                pos["size"] = new_size

            self.trades_history.append({
                "timestamp": now_ms,
                "exchange": "binance" if is_binance else "bybit",
                "symbol": symbol,
                "side": pos["side"],
                "entry_price": pos["entry_price"],
                "exit_price": exec_price,
                "qty": close_qty,
                "pnl_usd": net_pnl,
                "balance_after": self.balance
            })
            self.save_history()
            print(f"💰 [{exch_label} POSITION CLOSED] {symbol} | Net PnL: ${net_pnl:.2f} | Bal: ${self.balance:.2f}", flush=True)
        else:
            # Opening / adding
            if pos.get("size", 0.0) <= 1e-6:
                positions[symbol] = {"side": side_norm, "size": qty, "entry_price": exec_price, "leverage": pos.get("leverage", 50)}
                pos = positions[symbol]
            else:
                old_val = pos["size"] * pos["entry_price"]
                new_val = qty * exec_price
                pos["entry_price"] = (old_val + new_val) / (pos["size"] + qty)
                pos["size"] = round(pos["size"] + qty, 8)
            self.save_history()
            print(f"📦 [{exch_label} POSITION OPENED] {symbol} | {side_norm} {pos['size']} @ {pos['entry_price']:.4f}", flush=True)

        if is_binance:
            # 1. Broadcast ONLY to Binance Futures WebSocket subscribers
            pos_amt = pos["size"] if pos["side"].upper() in ["BUY", "B"] else (-pos["size"] if pos["side"].upper() in ["SELL", "S"] else 0.0)
            bal_str = f"{self.balance:.4f}"
            await self.broadcast_binance({
                "e": "ORDER_TRADE_UPDATE",
                "E": now_ms,
                "T": now_ms,
                "o": {
                    "s": symbol,
                    "c": client_order_id,
                    "S": side_norm.upper(),
                    "o": order_type.upper(),
                    "f": "GTC",
                    "q": str(qty),
                    "p": str(exec_price),
                    "ap": str(exec_price),
                    "sp": "0",
                    "x": "TRADE",
                    "X": "FILLED",
                    "i": order_id_num,
                    "l": str(qty),
                    "z": str(qty),
                    "L": str(exec_price),
                    "n": "0.0001",
                    "N": "USDT",
                    "T": now_ms,
                    "t": order_id_num,
                    "b": "0",
                    "a": "0",
                    "m": False,
                    "R": False,
                    "wt": "CONTRACT_PRICE",
                    "ot": order_type.upper(),
                    "ps": "BOTH",
                    "cp": False,
                    "rp": "0",
                    "pP": False,
                    "si": 0,
                    "st": 0
                }
            })

            await self.broadcast_binance({
                "e": "ACCOUNT_UPDATE",
                "E": now_ms,
                "T": now_ms,
                "a": {
                    "m": "ORDER",
                    "B": [{"a": "USDT", "wb": bal_str, "cw": bal_str, "bc": "0"}],
                    "P": [{
                        "s": symbol,
                        "pa": str(pos_amt),
                        "ep": str(pos["entry_price"]),
                        "cr": "0",
                        "up": "0.0000",
                        "mt": "cross",
                        "iw": "0",
                        "ps": "BOTH",
                        "ma": "USDT"
                    }]
                }
            })
        else:
            # 2. Broadcast ONLY to Bybit V5 Private WebSocket subscribers
            bybit_side = "Buy" if side_norm.upper() in ["BUY", "B"] else "Sell"
            await self.broadcast_bybit(self.get_bybit_execution_ws_payload(symbol, bybit_side, qty, exec_price, order_id_num, client_order_id, now_ms))
            await self.broadcast_bybit(self.get_bybit_order_ws_payload(now_ms, [{
                "orderId": order_id_num,
                "clientOrderId": client_order_id,
                "symbol": symbol,
                "price": exec_price,
                "qty": qty,
                "side": bybit_side,
                "type": order_type,
                "orderStatus": "Filled",
                "cumExecQty": qty,
                "cumExecValue": round(qty * exec_price, 4),
                "avgPrice": exec_price,
                "time": now_ms
            }]))
            await self.broadcast_bybit(self.get_bybit_position_ws_payload(now_ms, symbol))
            await self.broadcast_bybit(self.get_bybit_wallet_ws_payload(now_ms))

        if is_binance:
            return {
                "orderId": order_id_num,
                "symbol": symbol,
                "status": "FILLED",
                "clientOrderId": client_order_id,
                "price": str(exec_price),
                "avgPrice": str(exec_price),
                "origQty": str(qty),
                "executedQty": str(qty),
                "cumQty": str(qty),
                "cumQuote": str(round(qty * exec_price, 4)),
                "timeInForce": "GTC",
                "type": order_type.upper(),
                "reduceOnly": False,
                "closePosition": False,
                "side": side_norm.upper(),
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": order_type.upper(),
                "updateTime": now_ms
            }
        else:
            return {
                "orderId": f"ord_{order_id_num}",
                "orderLinkId": client_order_id
            }

wallet = VirtualWallet(INITIAL_BALANCE)

# ----------------- BYBIT V5 HANDLERS -----------------

async def bybit_server_time(request):
    now_ms = int(time.time() * 1000)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "timeSecond": str(now_ms // 1000),
            "timeNano": str(now_ms * 1_000_000)
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_risk_limit(request):
    now_ms = int(time.time() * 1000)
    category = request.query.get("category", "linear")
    symbol = request.query.get("symbol", "BTCUSDT").upper()
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": category,
            "list": [{
                "id": 1,
                "symbol": symbol,
                "riskLimitValue": "2000000",
                "maintenanceMargin": "0.005",
                "initialMargin": "0.01",
                "isLowestRisk": 1,
                "maxLeverage": "50"
            }]
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_query_api(request):
    now_ms = int(time.time() * 1000)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "id": "cgt_vataga_demo",
            "note": "Vataga Paper Gateway by CGTehnik",
            "apiKey": "cgt_demo_key",
            "readOnly": 0,
            "vipLevel": "VIP-1",
            "permissions": {
                "ContractTrade": ["Order", "Position"],
                "Spot": ["SpotTrade"],
                "Wallet": ["AccountBalance"],
                "Options": []
            }
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_get_member_type(request):
    now_ms = int(time.time() * 1000)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "memberType": 0,
            "accounts": [{
                "uid": "12345678",
                "vipLevel": "No VIP",
                "tradeType": 0,
                "memberType": 0
            }]
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_account_info(request):
    now_ms = int(time.time() * 1000)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "marginMode": "REGULAR_MARGIN",
            "updatedTime": str(now_ms),
            "unifiedMarginStatus": 1,
            "dcpStatus": "DEACTIVATE",
            "timeWindow": 10,
            "smpGroup": 0,
            "isMasterTrader": False,
            "spotHedgingStatus": "OFF"
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_account_config(request):
    now_ms = int(time.time() * 1000)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "marginMode": "REGULAR_MARGIN",
            "updatedTime": str(now_ms)
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_fee_rate(request):
    now_ms = int(time.time() * 1000)
    symbol = request.query.get("symbol", "")
    items = []
    if symbol:
        items.append({
            "symbol": symbol.upper(),
            "takerFeeRate": "0.00055",
            "makerFeeRate": "0.0002"
        })
    else:
        info = await get_fapi_exchange_info()
        symbols_list = [s["symbol"] for s in info.get("symbols", [])][:50]
        if not symbols_list:
            symbols_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        for sym in symbols_list:
            items.append({
                "symbol": sym,
                "takerFeeRate": "0.00055",
                "makerFeeRate": "0.0002"
            })
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": items
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_wallet_balance(request):
    now_ms = int(time.time() * 1000)
    bal_str = f"{wallet.balance:.4f}"
    req_type = request.query.get("accountType", "").upper()

    def make_entry(acc_type):
        return {
            "totalEquity": bal_str,
            "accountIMRate": "0",
            "totalMarginBalance": bal_str,
            "totalInitialMargin": "0",
            "accountType": acc_type,
            "totalAvailableBalance": bal_str,
            "accountMMRate": "0",
            "totalPerpUPL": "0",
            "totalWalletBalance": bal_str,
            "accountLTV": "0",
            "totalMaintenanceMargin": "0",
            "coin": [{
                "availableToBorrow": "",
                "bonus": "0",
                "accruedInterest": "0",
                "availableToWithdraw": bal_str,
                "totalOrderIM": "0",
                "equity": bal_str,
                "totalPositionMM": "0",
                "usdValue": bal_str,
                "unrealisedPnl": "0",
                "borrowAmount": "0",
                "totalPositionIM": "0",
                "walletBalance": bal_str,
                "cumRealisedPnl": "0",
                "locked": "0",
                "marginCollateral": True,
                "coin": "USDT"
            }]
        }

    order = ["SPOT", "CONTRACT", "UNIFIED"]
    if req_type and req_type in order:
        order.remove(req_type)
        order.insert(0, req_type)
    elif req_type:
        order.insert(0, req_type)

    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [make_entry(t) for t in order]
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_positions(request):
    now_ms = int(time.time() * 1000)
    symbol = request.query.get("symbol")
    pos_list = []
    
    if symbol:
        symbols_to_check = [symbol.upper()]
    else:
        symbols_to_check = [sym for sym, p in wallet.bybit_positions.items() if p.get("size", 0.0) > 0]

    for sym in symbols_to_check:
        p = wallet.bybit_positions.get(sym, {"size": 0.0, "entry_price": 0.0, "side": "None"})
        size = p.get("size", 0.0)
        if not symbol and size <= 0:
            continue
        side = p.get("side", "None")
        lev = wallet.symbol_leverage.get(sym, wallet.default_leverage)
        entry_price = p.get("entry_price", 0.0)
        pos_val = size * entry_price
        im = (pos_val / lev) if lev > 0 else 0.0
        pos_list.append({
            "category": "linear",
            "positionIdx": 0,
            "riskId": 1,
            "riskLimitValue": "2000000",
            "symbol": sym,
            "side": side if size > 0 else "None",
            "size": str(size),
            "avgPrice": str(entry_price),
            "positionValue": f"{pos_val:.4f}",
            "tradeMode": 0,
            "positionStatus": "Normal",
            "autoAddMargin": 0,
            "adlRankIndicator": 2,
            "leverage": str(lev),
            "positionBalance": "0",
            "markPrice": str(entry_price),
            "liqPrice": "",
            "bustPrice": "",
            "positionMM": "0",
            "positionIM": f"{im:.4f}",
            "tpslMode": "Full",
            "takeProfit": p.get("take_profit", ""),
            "stopLoss": p.get("stop_loss", ""),
            "trailingStop": "0",
            "unrealisedPnl": str(p.get("unrealized_pnl", 0.0)),
            "cumRealisedPnl": "0",
            "seq": 1,
            "isReduceOnly": False,
            "mmrSysUpdatedTime": "",
            "leverageSysUpdatedTime": "",
            "createdTime": str(now_ms),
            "updatedTime": str(now_ms)
        })
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": pos_list,
            "category": "linear",
            "nextPageCursor": ""
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_leverage_info(request):
    now_ms = int(time.time() * 1000)
    symbol = request.query.get("symbol", "BTCUSDT").upper()
    lev = wallet.symbol_leverage.get(symbol, wallet.default_leverage)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "symbol": symbol,
            "leverage": str(lev),
            "maxLeverage": "50",
            "minLeverage": "1"
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_set_leverage(request):
    params = await parse_request_params(request)
    symbol = params.get("symbol", "BTCUSDT").upper()
    buy_lev = params.get("buyLeverage", params.get("leverage", "50"))
    try:
        lev = int(float(buy_lev))
    except Exception:
        lev = 50
    wallet.symbol_leverage[symbol] = lev
    if symbol in wallet.bybit_positions:
        wallet.bybit_positions[symbol]["leverage"] = lev
    wallet.save_history()
    print(f"⚙️ [BYBIT LEVERAGE CHANGED] {symbol} leverage set to {lev}x", flush=True)
    now_ms = int(time.time() * 1000)
    if symbol in wallet.bybit_positions:
        await wallet.broadcast_bybit(wallet.get_bybit_position_ws_payload(now_ms, symbol))
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {},
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_switch_mode(request):
    return web.json_response({"retCode": 0, "retMsg": "OK", "result": {}, "time": int(time.time() * 1000)})

async def bybit_trading_stop(request):
    now_ms = int(time.time() * 1000)
    params = await parse_request_params(request)
    symbol = params.get("symbol", "").upper()
    tp = params.get("takeProfit", "")
    sl = params.get("stopLoss", "")
    if symbol in wallet.bybit_positions:
        if tp:
            wallet.bybit_positions[symbol]["take_profit"] = tp
        if sl:
            wallet.bybit_positions[symbol]["stop_loss"] = sl
    print(f"🛑 [BYBIT TRADING STOP] {symbol} TP: {tp}, SL: {sl}", flush=True)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {},
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_open_orders(request):
    now_ms = int(time.time() * 1000)
    symbol = request.query.get("symbol", "").upper()
    order_list = []
    for oid, o in wallet.open_orders.items():
        if o.get("exchange") != "bybit":
            continue
        if symbol and o.get("symbol") != symbol:
            continue
        order_list.append({
            "orderId": str(oid),
            "orderLinkId": str(o.get("clientOrderId", o.get("orderLinkId", oid))),
            "symbol": o["symbol"],
            "price": str(o["price"]),
            "qty": str(o["qty"]),
            "side": "Buy" if o.get("side", "BUY").upper() in ["BUY", "B"] else "Sell",
            "isLeverage": "",
            "positionIdx": 0,
            "orderStatus": o.get("orderStatus", "New"),
            "cancelType": "UNKNOWN",
            "rejectReason": "EC_NoError",
            "avgPrice": "0",
            "leavesQty": str(o["qty"]),
            "leavesValue": str(round(float(o["qty"]) * float(o["price"]), 4)),
            "cumExecQty": "0",
            "cumExecValue": "0",
            "cumExecFee": "0",
            "timeInForce": "GTC",
            "orderType": o.get("type", "Limit"),
            "stopOrderType": "UNKNOWN",
            "orderIv": "",
            "triggerPrice": "",
            "takeProfit": "",
            "stopLoss": "",
            "tpTriggerBy": "UNKNOWN",
            "slTriggerBy": "UNKNOWN",
            "triggerDirection": 0,
            "triggerBy": "UNKNOWN",
            "lastPriceOnCreated": "",
            "reduceOnly": False,
            "closeOnTrigger": False,
            "smpType": "None",
            "smpGroup": 0,
            "smpOrderId": "",
            "tpslMode": "Full",
            "tpLimitPrice": "",
            "slLimitPrice": "",
            "placeType": "",
            "createdTime": str(o.get("time", now_ms)),
            "updatedTime": str(now_ms)
        })
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": order_list,
            "nextPageCursor": "",
            "category": "linear"
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def parse_request_params(request):
    params = {}
    for k, v in request.query.items():
        params[k] = v
    if request.can_read_body:
        try:
            body = await request.json()
            if isinstance(body, dict):
                params.update(body)
        except Exception:
            try:
                post_data = await request.post()
                params.update(dict(post_data))
            except Exception:
                pass
    return params

async def handle_order_execution(params, is_binance=False):
    now_ms = int(time.time() * 1000)
    symbol = params.get("symbol", "BTCUSDT").upper()
    side_raw = params.get("side", "Buy")
    side = "Buy" if side_raw.upper() in ["BUY", "B"] else "Sell"
    order_type = params.get("orderType", params.get("type", "LIMIT")).upper()
    
    qty = float(params.get("qty", params.get("quantity", 0.0)))
    price_val = params.get("price")
    price = float(price_val) if price_val and float(price_val) > 0 else None
    
    client_order_id = params.get("orderLinkId", params.get("newClientOrderId", f"cgt_{uuid.uuid4().hex[:8]}"))

    # 1. Margin limit check
    can_trade, err_msg = wallet.can_place_order(symbol, side, qty, price, is_binance=is_binance)
    if not can_trade:
        print(f"⚠️ [DEMO ORDER REJECTED] {err_msg}", flush=True)
        if is_binance:
            return {"code": -2019, "msg": "Margin is insufficient."}
        else:
            return {"retCode": 140007, "retMsg": "insufficient available balance"}

    # 2. Check market quote and fill condition
    quote = await wallet.get_market_quote(symbol)

    is_market = (order_type == "MARKET" or price is None)
    crosses_spread = False

    if not is_market and price is not None and quote is not None:
        best_bid = quote.get("bid", 0.0)
        best_ask = quote.get("ask", 0.0)
        if best_bid > 0 and best_ask > 0 and (0.3 * best_ask <= price <= 3.0 * best_ask):
            if side == "Buy" and price >= best_ask:
                crosses_spread = True
            elif side == "Sell" and price <= best_bid:
                crosses_spread = True

    if is_market or crosses_spread:
        fallback_price = quote.get("last", 100.0) if quote else 100.0
        exec_price = price if price else (quote.get("ask", fallback_price) if side == "Buy" else quote.get("bid", fallback_price))
        print(f"⚡ [DEMO MARKET EXEC] {side} {qty} {symbol} @ {exec_price}", flush=True)
        return await wallet.execute_fill(
            symbol=symbol,
            side=side,
            qty=qty,
            exec_price=exec_price,
            client_order_id=client_order_id,
            order_type=order_type,
            is_binance=is_binance
        )
    else:
        # Maker / Passive Limit Order -> goes into order book as NEW
        order_id_num = int(time.time() * 1000) * 1000 + (random.randint(100, 999) % 1000000000)
        order_data = {
            "orderId": order_id_num,
            "clientOrderId": client_order_id,
            "orderLinkId": client_order_id,
            "symbol": symbol,
            "side": side,
            "type": "LIMIT",
            "price": price,
            "qty": qty,
            "time": now_ms,
            "orderStatus": "New",
            "exchange": "binance" if is_binance else "bybit",
            "is_binance": is_binance
        }
        wallet.open_orders[order_id_num] = order_data
        bid_str = f"{quote.get('bid', 0.0):.4f}" if quote else "N/A"
        ask_str = f"{quote.get('ask', 0.0):.4f}" if quote else "N/A"
        exch_label = "BINANCE" if is_binance else "BYBIT"
        print(f"📋 [{exch_label} LIMIT PLACED] {side} {qty} {symbol} @ {price} (Market Spread: {bid_str} - {ask_str})", flush=True)

        # Broadcast NEW event ONLY to the originating exchange
        if is_binance:
            await wallet.broadcast_binance_order_update(order_data, "NEW")
        else:
            await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, [order_data]))

        if is_binance:
            return {
                "orderId": order_id_num,
                "symbol": symbol,
                "status": "NEW",
                "clientOrderId": client_order_id,
                "price": str(price),
                "avgPrice": "0",
                "origQty": str(qty),
                "executedQty": "0",
                "cumQty": "0",
                "cumQuote": "0",
                "timeInForce": "GTC",
                "type": "LIMIT",
                "reduceOnly": False,
                "closePosition": False,
                "side": side.upper(),
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": "LIMIT",
                "updateTime": now_ms
            }
        else:
            return {
                "orderId": str(order_id_num),
                "orderLinkId": client_order_id
            }

async def bybit_create_order(request):
    params = await parse_request_params(request)
    res = await handle_order_execution(params, is_binance=False)
    now_ms = int(time.time() * 1000)
    if isinstance(res, dict) and "retCode" in res:
        return web.json_response(res)
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": res,
        "retExtInfo": {},
        "time": now_ms
    })

def find_open_order(order_id="", order_link_id="", symbol="", exchange="bybit"):
    clean_id = str(order_id or "").strip()
    clean_raw_id = clean_id[4:] if clean_id.startswith("ord_") else clean_id
    clean_link = str(order_link_id or "").strip()

    # Filter to only the requested exchange if specified
    orders_to_search = {
        oid: o for oid, o in wallet.open_orders.items()
        if not exchange or o.get("exchange") == exchange
    }

    # 1. Match by orderId
    if clean_id:
        for oid, o in orders_to_search.items():
            s_oid = str(oid).strip()
            s_o_order_id = str(o.get("orderId", "")).strip()
            if (s_oid == clean_id or s_oid == clean_raw_id or 
                s_o_order_id == clean_id or s_o_order_id == clean_raw_id or
                f"ord_{s_oid}" == clean_id or f"ord_{s_o_order_id}" == clean_id):
                return oid, o

    # 2. Match by orderLinkId / clientOrderId
    if clean_link:
        for oid, o in orders_to_search.items():
            s_client = str(o.get("clientOrderId", "")).strip()
            s_order_link = str(o.get("orderLinkId", "")).strip()
            if s_client == clean_link or s_order_link == clean_link:
                return oid, o

    # 3. Match cross-fields (if terminal sent orderLinkId in orderId or vice versa)
    if clean_id:
        for oid, o in orders_to_search.items():
            s_client = str(o.get("clientOrderId", "")).strip()
            s_order_link = str(o.get("orderLinkId", "")).strip()
            if s_client == clean_id or s_order_link == clean_id:
                return oid, o
    if clean_link:
        for oid, o in orders_to_search.items():
            s_oid = str(oid).strip()
            s_o_order_id = str(o.get("orderId", "")).strip()
            if s_oid == clean_link or s_o_order_id == clean_link:
                return oid, o

    # 4. Fallback if symbol provided and only one order exists for that symbol on that exchange
    if symbol:
        sym_upper = symbol.strip().upper()
        matching = [oid for oid, o in orders_to_search.items() if o.get("symbol", "").upper() == sym_upper]
        if len(matching) == 1:
            return matching[0], orders_to_search[matching[0]]

    return None, None

async def bybit_order_amend(request):
    params = await parse_request_params(request)
    order_id = params.get("orderId", "")
    order_link_id = params.get("orderLinkId", "")
    symbol = params.get("symbol", "").upper()
    now_ms = int(time.time() * 1000)
    target_oid, o = find_open_order(order_id, order_link_id, symbol, exchange="bybit")
    if target_oid and o:
        if "price" in params:
            o["price"] = float(params["price"])
        if "qty" in params:
            o["qty"] = float(params["qty"])
        o["time"] = now_ms
        print(f"✏️ [BYBIT ORDER AMENDED] {o.get('symbol')} Order #{target_oid} Price: {o.get('price')}, Qty: {o.get('qty')}", flush=True)
        await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, [o]))
        return web.json_response({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": str(target_oid),
                "orderLinkId": str(order_link_id or target_oid)
            },
            "retExtInfo": {},
            "time": now_ms
        })
    else:
        return web.json_response({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": str(order_id or "0"),
                "orderLinkId": str(order_link_id or "0")
            },
            "retExtInfo": {},
            "time": now_ms
        })

async def bybit_order_amend_batch(request):
    params = await parse_request_params(request)
    now_ms = int(time.time() * 1000)
    req_list = params.get("request", [])
    res_list = []
    for item in req_list:
        oid_val = item.get("orderId", "")
        link_id_val = item.get("orderLinkId", "")
        sym_val = item.get("symbol", "").upper()
        target_oid, o = find_open_order(oid_val, link_id_val, sym_val, exchange="bybit")
        if target_oid and o:
            if "price" in item:
                o["price"] = float(item["price"])
            if "qty" in item:
                o["qty"] = float(item["qty"])
            o["time"] = now_ms
            await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, [o]))
        res_list.append({"orderId": str(target_oid or oid_val or ""), "orderLinkId": str(link_id_val or "")})
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {"list": res_list},
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_cancel_order(request):
    now_ms = int(time.time() * 1000)
    params = await parse_request_params(request)
    print(f"👉 [BYBIT CANCEL REQ] {params}", flush=True)
    order_id = params.get("orderId", "")
    order_link_id = params.get("orderLinkId", "")
    symbol = params.get("symbol", "").upper()
    target_oid, o = find_open_order(order_id, order_link_id, symbol, exchange="bybit")
    if target_oid and target_oid in wallet.open_orders:
        cancelled_order = wallet.open_orders.pop(target_oid)
        cancelled_order["orderStatus"] = "Cancelled"
        print(f"❌ [BYBIT ORDER CANCELED] {cancelled_order.get('symbol')} Order #{target_oid} linkId={cancelled_order.get('clientOrderId')}", flush=True)
        await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, [cancelled_order]))
        return web.json_response({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": str(target_oid),
                "orderLinkId": str(order_link_id or cancelled_order.get("clientOrderId", ""))
            },
            "retExtInfo": {},
            "time": now_ms
        })
    else:
        # Ghost / out-of-sync order in terminal DOM: still broadcast Cancelled so DOM cleans it!
        clean_id = str(order_id or "")
        clean_link = str(order_link_id or "")
        print(f"⚠️ [BYBIT CANCEL FALLBACK SYNTHETIC] orderId={clean_id} orderLinkId={clean_link} symbol={symbol}", flush=True)
        synth_cancelled = {
            "orderId": clean_id or str(now_ms),
            "clientOrderId": clean_link or clean_id or str(now_ms),
            "orderLinkId": clean_link or clean_id or str(now_ms),
            "symbol": symbol or "BTCUSDT",
            "side": "Buy",
            "type": "Limit",
            "price": "0",
            "qty": "0",
            "time": now_ms,
            "orderStatus": "Cancelled"
        }
        await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, [synth_cancelled]))
        return web.json_response({
            "retCode": 0,
            "retMsg": "OK",
            "result": {
                "orderId": clean_id,
                "orderLinkId": clean_link
            },
            "retExtInfo": {},
            "time": now_ms
        })

async def bybit_cancel_all(request):
    now_ms = int(time.time() * 1000)
    params = await parse_request_params(request)
    print(f"👉 [BYBIT CANCEL ALL REQ] {params}", flush=True)
    symbol = params.get("symbol", "").upper()
    cancelled = []
    for oid in list(wallet.open_orders.keys()):
        o = wallet.open_orders[oid]
        if o.get("exchange") != "bybit":
            continue
        if not symbol or o.get("symbol") == symbol:
            c = wallet.open_orders.pop(oid)
            c["orderStatus"] = "Cancelled"
            cancelled.append(c)
    if cancelled:
        print(f"❌ [BYBIT CANCEL ALL] Cancelled {len(cancelled)} orders for {symbol or 'ALL'}", flush=True)
        await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, cancelled))
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [{"orderId": str(o.get("orderId", "")), "orderLinkId": str(o.get("clientOrderId", ""))} for o in cancelled],
            "success": "1"
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_cancel_batch(request):
    params = await parse_request_params(request)
    now_ms = int(time.time() * 1000)
    print(f"👉 [BYBIT CANCEL BATCH REQ] {params}", flush=True)
    req_list = params.get("request", [])
    res_list = []
    cancelled = []
    for item in req_list:
        oid_val = item.get("orderId", "")
        link_id_val = item.get("orderLinkId", "")
        sym_val = item.get("symbol", "").upper()
        target_oid, o = find_open_order(oid_val, link_id_val, sym_val, exchange="bybit")
        if target_oid and target_oid in wallet.open_orders:
            c = wallet.open_orders.pop(target_oid)
            c["orderStatus"] = "Cancelled"
            cancelled.append(c)
            res_list.append({"orderId": str(target_oid), "orderLinkId": str(link_id_val or c.get("clientOrderId", ""))})
        else:
            synth_c = {
                "orderId": str(oid_val or ""),
                "clientOrderId": str(link_id_val or oid_val or ""),
                "orderLinkId": str(link_id_val or oid_val or ""),
                "symbol": sym_val or "BTCUSDT",
                "side": "Buy",
                "type": "Limit",
                "price": "0",
                "qty": "0",
                "time": now_ms,
                "orderStatus": "Cancelled"
            }
            cancelled.append(synth_c)
            res_list.append({"orderId": str(oid_val or ""), "orderLinkId": str(link_id_val or "")})
    if cancelled:
        await wallet.broadcast_bybit(wallet.get_bybit_order_ws_payload(now_ms, cancelled))
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {"list": res_list},
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_execution_list(request):
    now_ms = int(time.time() * 1000)
    category = request.query.get("category", "linear")
    return web.json_response({
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "list": [],
            "nextPageCursor": "",
            "category": category
        },
        "retExtInfo": {},
        "time": now_ms
    })

async def bybit_private_ws(request):
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)
    if ws not in wallet.bybit_subscribers:
        wallet.bybit_subscribers.append(ws)
    conn_id = f"bybit_conn_{uuid.uuid4().hex[:8]}"
    print("🔌 [BYBIT WS CONNECTED] Private Bybit stream connected!", flush=True)

    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                op = data.get("op", "")
                req_id = data.get("req_id")
                if op == "auth":
                    print(f"🔑 [BYBIT WS AUTH] Client authenticated: {data.get('args', [])[:1]}", flush=True)
                    auth_resp = {
                        "success": True,
                        "ret_msg": "",
                        "op": "auth",
                        "conn_id": conn_id
                    }
                    if req_id is not None:
                        auth_resp["req_id"] = req_id
                    await ws.send_json(auth_resp)
                elif op == "subscribe":
                    args = data.get("args", [])
                    print(f"📡 [BYBIT WS SUBSCRIBE] Subscribed to {args}", flush=True)
                    sub_resp = {
                        "success": True,
                        "ret_msg": "",
                        "op": "subscribe",
                        "args": args,
                        "conn_id": conn_id
                    }
                    if req_id is not None:
                        sub_resp["req_id"] = req_id
                    await ws.send_json(sub_resp)
                elif op == "ping":
                    resp = {"op": "pong", "args": [str(int(time.time() * 1000))], "conn_id": conn_id}
                    if req_id is not None:
                        resp["req_id"] = req_id
                    await ws.send_json(resp)
            elif msg.type == WSMsgType.PING:
                await ws.pong()
    finally:
        if ws in wallet.bybit_subscribers:
            wallet.bybit_subscribers.remove(ws)
        print("🔌 [BYBIT WS DISCONNECTED] Private Bybit stream closed", flush=True)
    return ws

async def handle_bybit_public_ws(request):
    client_ws = web.WebSocketResponse(heartbeat=20, max_msg_size=16*1024*1024)
    await client_ws.prepare(request)
    target_url = f"wss://stream.bybit.com{request.rel_url}"
    print(f"🔌 [BYBIT PUBLIC WS PROXY] Forwarding to {target_url}", flush=True)

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.ws_connect(target_url, heartbeat=20, max_msg_size=16*1024*1024) as upstream_ws:
                async def upstream_to_client():
                    try:
                        async for msg in upstream_ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await client_ws.send_str(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await client_ws.send_bytes(msg.data)
                            elif msg.type == aiohttp.WSMsgType.PING:
                                await client_ws.ping(msg.data)
                            elif msg.type == aiohttp.WSMsgType.PONG:
                                await client_ws.pong(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    except Exception:
                        pass

                async def client_to_upstream():
                    try:
                        async for msg in client_ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await upstream_ws.send_str(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await upstream_ws.send_bytes(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    except Exception:
                        pass

                t1 = asyncio.create_task(upstream_to_client())
                t2 = asyncio.create_task(client_to_upstream())
                done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
                for p in pending:
                    p.cancel()
        except Exception as e:
            print(f"[BYBIT PUBLIC WS PROXY ERROR] {e}", flush=True)
    return client_ws

# ----------------- BINANCE FUTURES HANDLERS -----------------

async def binance_ping(request):
    return web.json_response({})

async def binance_time(request):
    return web.json_response({"serverTime": int(time.time() * 1000)})

async def binance_account(request):
    now_ms = int(time.time() * 1000)
    bal_str = f"{wallet.balance:.4f}"
    pos_list = []
    info = await get_fapi_exchange_info()
    symbols_list = [s["symbol"] for s in info.get("symbols", [])]
    if not symbols_list:
        symbols_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "0GUSDT", "PARTIUSDT", "WLDUSDT", "XRPUSDT"]
    all_syms = set(symbols_list) | set(wallet.binance_positions.keys()) | set(wallet.symbol_leverage.keys())
    for sym in all_syms:
        p = wallet.binance_positions.get(sym, {"size": 0.0, "entry_price": 0.0, "side": "None"})
        lev = wallet.symbol_leverage.get(sym, wallet.default_leverage)
        qty = p["size"] if p.get("side") == "Buy" else -p.get("size", 0.0)
        entry_price = p.get("entry_price", 0.0)
        pos_list.append({
            "symbol": sym,
            "initialMargin": str((p.get("size", 0.0) * entry_price) / lev) if lev > 0 else "0",
            "maintMargin": "0",
            "unrealizedProfit": "0.0000",
            "positionInitialMargin": str((p.get("size", 0.0) * entry_price) / lev) if lev > 0 else "0",
            "openOrderInitialMargin": "0",
            "leverage": str(lev),
            "isolated": False,
            "entryPrice": str(entry_price),
            "maxNotional": "1000000",
            "positionSide": "BOTH",
            "positionAmt": str(qty),
            "notional": str(p.get("size", 0.0) * entry_price),
            "isolatedWallet": "0",
            "updateTime": now_ms
        })
    return web.json_response({
        "feeTier": 0,
        "canTrade": True,
        "canDeposit": True,
        "canWithdraw": True,
        "updateTime": now_ms,
        "totalInitialMargin": "0.0000",
        "totalMaintMargin": "0.0000",
        "totalWalletBalance": bal_str,
        "totalUnrealizedProfit": "0.0000",
        "totalMarginBalance": bal_str,
        "totalPositionInitialMargin": "0.0000",
        "totalOpenOrderInitialMargin": "0.0000",
        "totalCrossWalletBalance": bal_str,
        "totalCrossUnPnl": "0.0000",
        "availableBalance": bal_str,
        "maxWithdrawAmount": bal_str,
        "assets": [{
            "asset": "USDT",
            "walletBalance": bal_str,
            "unrealizedProfit": "0.0000",
            "marginBalance": bal_str,
            "maintMargin": "0.0000",
            "initialMargin": "0.0000",
            "positionInitialMargin": "0.0000",
            "openOrderInitialMargin": "0.0000",
            "crossWalletBalance": bal_str,
            "crossUnPnl": "0.0000",
            "availableBalance": bal_str,
            "maxWithdrawAmount": bal_str,
            "marginAvailable": True,
            "updateTime": now_ms
        }],
        "positions": pos_list
    })

async def binance_balance(request):
    bal_str = f"{wallet.balance:.8f}"
    return web.json_response([{
        "accountAlias": "cgt_paper",
        "asset": "USDT",
        "balance": bal_str,
        "crossWalletBalance": bal_str,
        "crossUnPnl": "0.00000000",
        "availableBalance": bal_str,
        "maxWithdrawAmount": bal_str,
        "marginAvailable": True,
        "updateTime": int(time.time() * 1000)
    }])

async def binance_position_risk(request):
    now_ms = int(time.time() * 1000)
    symbol = request.query.get("symbol")
    pos_list = []

    if symbol:
        symbols_to_check = [symbol.upper()]
    else:
        info = await get_fapi_exchange_info()
        symbols_list = [s["symbol"] for s in info.get("symbols", [])]
        if not symbols_list:
            symbols_list = ["BTCUSDT", "ETHUSDT", "0GUSDT", "SOLUSDT", "PARTIUSDT", "WLDUSDT", "XRPUSDT"]
        symbols_to_check = list(set(symbols_list) | set(wallet.binance_positions.keys()) | set(wallet.symbol_leverage.keys()))

    for sym in symbols_to_check:
        p = wallet.binance_positions.get(sym, {"size": 0.0, "entry_price": 0.0, "side": "None"})
        lev = wallet.symbol_leverage.get(sym, wallet.default_leverage)
        qty = p.get("size", 0.0) if p.get("side") == "Buy" else -p.get("size", 0.0)
        entry_price = p.get("entry_price", 0.0)
        pos_list.append({
            "symbol": sym,
            "positionAmt": str(qty),
            "entryPrice": str(entry_price),
            "markPrice": str(entry_price),
            "unRealizedProfit": "0.00000000",
            "liquidationPrice": "0",
            "leverage": str(lev),
            "maxNotionalValue": "1000000",
            "marginType": "cross",
            "isolatedMargin": "0.00000000",
            "isAutoAddMargin": "false",
            "positionSide": "BOTH",
            "notional": str(p.get("size", 0.0) * entry_price),
            "isolatedWallet": "0",
            "updateTime": now_ms
        })
    return web.json_response(pos_list)

async def binance_listen_key(request):
    return web.json_response({"listenKey": "cgt_binance_stream_key"})

async def binance_commission_rate(request):
    symbol = request.query.get("symbol", "BTCUSDT")
    return web.json_response({
        "symbol": symbol,
        "makerCommissionRate": "0.000200",
        "takerCommissionRate": "0.000500"
    })

async def binance_open_orders(request):
    symbol = request.query.get("symbol", "").upper()
    now_ms = int(time.time() * 1000)
    orders = []
    for oid, o in wallet.open_orders.items():
        if o.get("exchange") != "binance":
            continue
        if not symbol or o.get("symbol") == symbol:
            orders.append({
                "orderId": int(oid),
                "symbol": o.get("symbol"),
                "status": "NEW",
                "clientOrderId": o.get("clientOrderId", f"ord_{oid}"),
                "price": str(o.get("price")),
                "avgPrice": "0",
                "origQty": str(o.get("qty")),
                "executedQty": "0",
                "cumQty": "0",
                "cumQuote": "0",
                "timeInForce": "GTC",
                "type": o.get("type", "LIMIT"),
                "reduceOnly": False,
                "closePosition": False,
                "side": o.get("side", "BUY").upper(),
                "positionSide": "BOTH",
                "stopPrice": "0",
                "workingType": "CONTRACT_PRICE",
                "priceProtect": False,
                "origType": o.get("type", "LIMIT"),
                "time": o.get("time", now_ms),
                "updateTime": o.get("time", now_ms)
            })
    return web.json_response(orders)

async def binance_all_orders(request):
    return await binance_open_orders(request)

async def binance_create_order(request):
    params = await parse_request_params(request)
    res = await handle_order_execution(params, is_binance=True)
    if "code" in res and res["code"] != 200:
        return web.json_response(res, status=400)
    return web.json_response(res)

async def binance_cancel_order(request):
    params = await parse_request_params(request)
    order_id_raw = params.get("orderId")
    client_order_id = params.get("origClientOrderId") or params.get("clientOrderId")
    symbol = params.get("symbol", "BTCUSDT").upper()

    target_order = None
    target_id = None
    for oid, o in list(wallet.open_orders.items()):
        if o.get("exchange") != "binance":
            continue
        if (order_id_raw and str(oid) == str(order_id_raw)) or (client_order_id and o.get("clientOrderId") == client_order_id):
            target_order = o
            target_id = oid
            break

    now_ms = int(time.time() * 1000)
    if target_id is not None:
        del wallet.open_orders[target_id]
        print(f"❌ [BINANCE ORDER CANCELED] {symbol} Order #{target_id}", flush=True)
        await wallet.broadcast_binance({
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "o": {
                "s": symbol,
                "c": target_order.get("clientOrderId", client_order_id or ""),
                "S": target_order.get("side", "BUY").upper(),
                "o": target_order.get("type", "LIMIT"),
                "f": "GTC",
                "q": str(target_order.get("qty", 0.0)),
                "p": str(target_order.get("price", 0.0)),
                "ap": "0",
                "sp": "0",
                "x": "CANCELED",
                "X": "CANCELED",
                "i": int(target_id),
                "l": "0",
                "z": "0",
                "L": "0",
                "n": "0",
                "N": "USDT",
                "T": now_ms,
                "t": 0,
                "b": "0",
                "a": "0",
                "m": False,
                "R": False,
                "wt": "CONTRACT_PRICE",
                "ot": target_order.get("type", "LIMIT"),
                "ps": "BOTH",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0
            }
        })
        return web.json_response({
            "symbol": symbol,
            "status": "CANCELED",
            "clientOrderId": target_order.get("clientOrderId", client_order_id or ""),
            "orderId": int(target_id),
            "origQty": str(target_order.get("qty", 0.0)),
            "price": str(target_order.get("price", 0.0)),
            "side": target_order.get("side", "BUY").upper(),
            "type": target_order.get("type", "LIMIT"),
            "updateTime": now_ms
        })
    else:
        # Fallback synthetic cancel to clear orphan order from Binance DOM
        synth_oid = int(order_id_raw) if order_id_raw and str(order_id_raw).isdigit() else now_ms
        await wallet.broadcast_binance({
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "o": {
                "s": symbol,
                "c": client_order_id or f"ord_{synth_oid}",
                "S": "BUY",
                "o": "LIMIT",
                "f": "GTC",
                "q": "0",
                "p": "0",
                "ap": "0",
                "sp": "0",
                "x": "CANCELED",
                "X": "CANCELED",
                "i": synth_oid,
                "l": "0",
                "z": "0",
                "L": "0",
                "n": "0",
                "N": "USDT",
                "T": now_ms,
                "t": 0,
                "b": "0",
                "a": "0",
                "m": False,
                "R": False,
                "wt": "CONTRACT_PRICE",
                "ot": "LIMIT",
                "ps": "BOTH",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0
            }
        })
        return web.json_response({
            "symbol": symbol,
            "status": "CANCELED",
            "clientOrderId": client_order_id or "",
            "orderId": int(synth_oid)
        })

async def binance_user_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    if ws not in wallet.binance_subscribers:
        wallet.binance_subscribers.append(ws)
    print(f"🔌 [BINANCE WS] User stream connected: {request.path}", flush=True)

    # Send initial ACCOUNT_UPDATE to sync wallet state in Vataga
    now_ms = int(time.time() * 1000)
    bal_str = f"{wallet.balance:.4f}"
    pos_items = []
    for s, p in wallet.binance_positions.items():
        if p.get("size", 0.0) > 0:
            amt = p["size"] if p["side"].upper() in ["BUY", "B"] else -p["size"]
            pos_items.append({
                "s": s,
                "pa": str(amt),
                "ep": str(p["entry_price"]),
                "cr": "0",
                "up": "0.0000",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
                "ma": "USDT"
            })
    await ws.send_json({
        "e": "ACCOUNT_UPDATE",
        "E": now_ms,
        "T": now_ms,
        "a": {
            "m": "ORDER",
            "B": [{"a": "USDT", "wb": bal_str, "cw": bal_str, "bc": "0"}],
            "P": pos_items
        }
    })

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    if "id" in data:
                        await ws.send_json({"id": data["id"], "result": "pong"})
                except Exception:
                    pass
            elif msg.type == web.WSMsgType.PING:
                await ws.pong()
    finally:
        if ws in wallet.binance_subscribers:
            wallet.binance_subscribers.remove(ws)
    return ws

async def handle_binance_ws(request):
    path = request.path
    tail = request.match_info.get("tail", "")
    
    # Private user stream is /private/ws/... or /ws/{listenKey} without '@'
    is_private_user = (
        path.startswith("/private/ws/")
        or (path.startswith("/ws/") and "@" not in path and not path.startswith("/ws/stream"))
    )
    if is_private_user:
        return await binance_user_ws(request)

    # Otherwise it's a public market data stream (e.g. /stream?streams=... or /ws/btcusdt@trade)
    client_ws = web.WebSocketResponse(heartbeat=20, max_msg_size=16*1024*1024)
    await client_ws.prepare(request)
    host = request.headers.get("Host", "fstream.binance.com")
    if ":" in host:
        host = host.split(":")[0]
    target_url = f"wss://{host}{request.rel_url}"
    
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.ws_connect(target_url, heartbeat=20, max_msg_size=16*1024*1024) as upstream_ws:
                async def upstream_to_client():
                    try:
                        async for msg in upstream_ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await client_ws.send_str(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await client_ws.send_bytes(msg.data)
                            elif msg.type == aiohttp.WSMsgType.PING:
                                await client_ws.ping(msg.data)
                            elif msg.type == aiohttp.WSMsgType.PONG:
                                await client_ws.pong(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    except Exception as e:
                        print(f"[WS UPSTREAM ERROR] {e}", flush=True)

                async def client_to_upstream():
                    try:
                        async for msg in client_ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                await upstream_ws.send_str(msg.data)
                            elif msg.type == aiohttp.WSMsgType.BINARY:
                                await upstream_ws.send_bytes(msg.data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
                    except Exception as e:
                        print(f"[WS CLIENT ERROR] {e}", flush=True)

                t1 = asyncio.create_task(upstream_to_client())
                t2 = asyncio.create_task(client_to_upstream())
                done, pending = await asyncio.wait([t1, t2], return_when=asyncio.FIRST_COMPLETED)
                for d in done:
                    if d.exception():
                        print(f"[WS TASK EXCEPTION] {d.exception()}", flush=True)
                for p in pending:
                    p.cancel()
        except Exception as e:
            print(f"[WS PROXY TOP ERROR] {request.path}: {e}", flush=True)
    return client_ws

async def binance_change_leverage(request):
    params = await parse_request_params(request)
    symbol = params.get("symbol", "BTCUSDT").upper()
    lev = int(params.get("leverage", 1))
    wallet.symbol_leverage[symbol] = lev
    if symbol in wallet.binance_positions:
        wallet.binance_positions[symbol]["leverage"] = lev
    print(f"⚙️ [BINANCE LEVERAGE CHANGED] {symbol} leverage set to {lev}x", flush=True)

    now_ms = int(time.time() * 1000)
    await wallet.broadcast_binance({
        "e": "ACCOUNT_CONFIG_UPDATE",
        "E": now_ms,
        "T": now_ms,
        "ac": {
            "s": symbol,
            "l": lev
        }
    })

    bal_str = f"{wallet.balance:.4f}"
    pos_items = []
    for s, p in wallet.binance_positions.items():
        if p.get("size", 0.0) > 0:
            amt = p["size"] if p["side"].upper() in ["BUY", "B"] else -p["size"]
            pos_items.append({
                "s": s,
                "pa": str(amt),
                "ep": str(p["entry_price"]),
                "cr": "0",
                "up": "0.0000",
                "mt": "cross",
                "iw": "0",
                "ps": "BOTH",
                "ma": "USDT"
            })
    await wallet.broadcast_binance({
        "e": "ACCOUNT_UPDATE",
        "E": now_ms,
        "T": now_ms,
        "a": {
            "m": "CONFIG_CHANGE",
            "B": [{"a": "USDT", "wb": bal_str, "cw": bal_str, "bc": "0"}],
            "P": pos_items
        }
    })

    wallet.save_history()
    return web.json_response({
        "leverage": lev,
        "maxNotionalValue": "1000000",
        "symbol": symbol
    })

async def binance_margin_type(request):
    return web.json_response({"code": 200, "msg": "success"})

async def binance_spot_account(request):
    now_ms = int(time.time() * 1000)
    bal_str = f"{wallet.balance:.4f}"
    return web.json_response({
        "makerCommission": 15,
        "takerCommission": 15,
        "buyerCommission": 0,
        "sellerCommission": 0,
        "canTrade": True,
        "canWithdraw": True,
        "canDeposit": True,
        "updateTime": now_ms,
        "accountType": "SPOT",
        "balances": [
            {"asset": "USDT", "free": bal_str, "locked": "0.00000000"}
        ],
        "permissions": ["SPOT"]
    })

async def binance_spot_listen_key(request):
    return web.json_response({"listenKey": "cgt_binance_spot_stream_key"})

async def binance_api_restrictions(request):
    return web.json_response({
        "ipRestrict": False,
        "createTime": int((time.time() - 86400 * 30) * 1000),
        "enableWithdrawals": False,
        "enableInternalTransfer": False,
        "permitsUniversalTransfer": False,
        "enableVanillaOptions": False,
        "enableReading": True,
        "enableFutures": True,
        "enableMargin": False,
        "enableSpotAndMarginTrading": True,
        "enableFastWithdrawSwitch": False
    })

async def binance_account_status(request):
    return web.json_response({"data": "Normal"})

async def binance_capital_config(request):
    bal_str = f"{wallet.balance:.4f}"
    return web.json_response([
        {
            "coin": "USDT",
            "depositAllEnable": True,
            "free": bal_str,
            "freeze": "0.00000000",
            "ipoable": "0.00000000",
            "isLegalMoney": False,
            "locked": "0.00000000",
            "name": "Tether",
            "networkList": [],
            "storage": "0.00000000",
            "trading": True,
            "withdrawAllEnable": True,
            "withdrawing": "0.00000000"
        }
    ])

async def binance_leverage_bracket(request):
    symbol = request.query.get("symbol", "")
    bracket = {
        "bracket": 1,
        "initialLeverage": 50,
        "notionalCap": 1000000,
        "notionalFloor": 0,
        "maintMarginRatio": 0.005,
        "cum": 0.0
    }
    if symbol:
        return web.json_response([{"symbol": symbol.upper(), "brackets": [bracket]}])

    info = await get_fapi_exchange_info()
    symbols_list = [s["symbol"] for s in info.get("symbols", [])]
    if not symbols_list:
        symbols_list = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "0GUSDT", "PARTIUSDT", "WLDUSDT", "XRPUSDT"]
    all_syms = set(symbols_list) | set(wallet.positions.keys()) | set(wallet.symbol_leverage.keys())
    return web.json_response([{"symbol": sym, "brackets": [bracket]} for sym in all_syms])

async def binance_dual_side(request):
    return web.json_response({"dualSidePosition": False})

async def binance_multi_assets(request):
    return web.json_response({"multiAssetsMargin": False})

async def binance_empty_list(request):
    return web.json_response([])

async def binance_adl_quantile(request):
    symbol = request.query.get("symbol")
    res = []
    if symbol:
        res.append({
            "symbol": symbol,
            "adlQuantile": {
                "LONG": 1,
                "SHORT": 1,
                "HEDGE": 1
            }
        })
    else:
        for sym in wallet.positions:
            if wallet.positions[sym].get("size", 0.0) > 0:
                res.append({
                    "symbol": sym,
                    "adlQuantile": {
                        "LONG": 1,
                        "SHORT": 1,
                        "HEDGE": 1
                    }
                })
    return web.json_response(res)

async def binance_cancel_all_orders(request):
    params = await parse_request_params(request)
    symbol = params.get("symbol", "").upper()
    now_ms = int(time.time() * 1000)

    to_delete = []
    for oid, o in list(wallet.open_orders.items()):
        if o.get("exchange") != "binance":
            continue
        if not symbol or o.get("symbol") == symbol:
            to_delete.append((oid, o))

    for oid, o in to_delete:
        del wallet.open_orders[oid]
        print(f"❌ [BINANCE ALL CANCELED] {o.get('symbol')} Order #{oid}", flush=True)
        await wallet.broadcast_binance({
            "e": "ORDER_TRADE_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "o": {
                "s": o.get("symbol", symbol),
                "c": o.get("clientOrderId", ""),
                "S": o.get("side", "BUY").upper(),
                "o": o.get("type", "LIMIT"),
                "f": "GTC",
                "q": str(o.get("qty", 0.0)),
                "p": str(o.get("price", 0.0)),
                "ap": "0",
                "sp": "0",
                "x": "CANCELED",
                "X": "CANCELED",
                "i": int(oid),
                "l": "0",
                "z": "0",
                "L": "0",
                "n": "0",
                "N": "USDT",
                "T": now_ms,
                "t": 0,
                "b": "0",
                "a": "0",
                "m": False,
                "R": False,
                "wt": "CONTRACT_PRICE",
                "ot": o.get("type", "LIMIT"),
                "ps": "BOTH",
                "cp": False,
                "rp": "0",
                "pP": False,
                "si": 0,
                "st": 0
            }
        })
    return web.json_response({"code": 200, "msg": "The operation of cancel all open order is done."})

async def binance_countdown_cancel(request):
    symbol = request.query.get("symbol", "BTCUSDT")
    return web.json_response({"symbol": symbol, "countdownTime": "10000"})

# ----------------- MARKET DATA & EXCHANGE INFO HANDLERS -----------------

CACHED_FAPI_EXCHANGE_INFO = None

async def get_fapi_exchange_info():
    global CACHED_FAPI_EXCHANGE_INFO
    if CACHED_FAPI_EXCHANGE_INFO:
        return CACHED_FAPI_EXCHANGE_INFO
    try:
        url = "https://api.bybit.com/v5/market/instruments-info?category=linear"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    items = data.get("result", {}).get("list", [])
                    symbols_out = []
                    for inst in items:
                        sym = inst.get("symbol", "")
                        base = inst.get("baseCoin", "")
                        quote = inst.get("quoteCoin", "USDT")
                        price_filter = inst.get("priceFilter", {})
                        lot_filter = inst.get("lotSizeFilter", {})
                        tick_size = price_filter.get("tickSize", "0.01")
                        qty_step = lot_filter.get("qtyStep", "0.001")
                        min_price = price_filter.get("minPrice", "0.0001")
                        max_price = price_filter.get("maxPrice", "1000000")
                        min_qty = lot_filter.get("minOrderQty", "0.001")
                        max_qty = lot_filter.get("maxOrderQty", "1000000")
                        p_prec = len(tick_size.split(".")[1]) if "." in tick_size else 0
                        q_prec = len(qty_step.split(".")[1]) if "." in qty_step else 0

                        symbols_out.append({
                            "symbol": sym,
                            "pair": sym,
                            "contractType": "PERPETUAL",
                            "deliveryDate": 4133404800000,
                            "onboardDate": 1569398400000,
                            "status": "TRADING",
                            "maintMarginPercent": "2.5000",
                            "requiredMarginPercent": "5.0000",
                            "baseAsset": base,
                            "quoteAsset": quote,
                            "marginAsset": "USDT",
                            "pricePrecision": p_prec,
                            "quantityPrecision": q_prec,
                            "baseAssetPrecision": 8,
                            "quotePrecision": 8,
                            "underlyingType": "COIN",
                            "settlePlan": 0,
                            "triggerProtect": "0.0500",
                            "filters": [
                                {
                                    "filterType": "PRICE_FILTER",
                                    "minPrice": min_price,
                                    "maxPrice": max_price,
                                    "tickSize": tick_size
                                },
                                {
                                    "filterType": "LOT_SIZE",
                                    "minQty": min_qty,
                                    "maxQty": max_qty,
                                    "stepSize": qty_step
                                },
                                {
                                    "filterType": "MARKET_LOT_SIZE",
                                    "minQty": min_qty,
                                    "maxQty": max_qty,
                                    "stepSize": qty_step
                                }
                            ],
                            "orderTypes": ["LIMIT", "MARKET", "STOP", "STOP_MARKET", "TAKE_PROFIT", "TAKE_PROFIT_MARKET"],
                            "timeInForce": ["GTC", "IOC", "FOK", "GTX"]
                        })

                    now_ms = int(time.time() * 1000)
                    CACHED_FAPI_EXCHANGE_INFO = {
                        "timezone": "UTC",
                        "serverTime": now_ms,
                        "futuresType": "U_MARGINED",
                        "rateLimits": [],
                        "exchangeFilters": [],
                        "symbols": symbols_out
                    }
                    print(f"[MARKET] Cached {len(symbols_out)} Binance Futures perpetual contracts", flush=True)
                    return CACHED_FAPI_EXCHANGE_INFO
    except Exception as e:
        print(f"[EXCHANGE_INFO ERROR] {e}", flush=True)

    return {"timezone": "UTC", "serverTime": int(time.time() * 1000), "futuresType": "U_MARGINED", "rateLimits": [], "exchangeFilters": [], "symbols": []}

async def binance_futures_exchange_info(request):
    info = await get_fapi_exchange_info()
    return web.json_response(info)

async def binance_spot_exchange_info(request):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get("https://data-api.binance.vision/api/v3/exchangeInfo", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return web.Response(body=data, status=200, content_type="application/json")
    except Exception:
        pass
    return web.json_response({"symbols": []})

async def binance_depth(request):
    symbol = request.query.get("symbol", "BTCUSDT").upper()
    limit = request.query.get("limit", "100")
    # 1. Try Binance Vision Spot depth
    try:
        url = f"https://data-api.binance.vision/api/v3/depth?symbol={symbol}&limit={limit}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    if len(bids) > 0:
                        now_ms = int(time.time() * 1000)
                        return web.json_response({
                            "lastUpdateId": data.get("lastUpdateId", now_ms),
                            "E": now_ms,
                            "T": now_ms,
                            "bids": bids,
                            "asks": asks
                        })
    except Exception:
        pass

    # 2. Try Bybit Linear orderbook
    try:
        url_bybit = f"https://api.bybit.com/v5/market/orderbook?category=linear&symbol={symbol}&limit={limit}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url_bybit, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    res = data.get("result", {})
                    bids = res.get("b", [])
                    asks = res.get("a", [])
                    if len(bids) > 0:
                        now_ms = int(time.time() * 1000)
                        return web.json_response({
                            "lastUpdateId": now_ms,
                            "E": now_ms,
                            "T": now_ms,
                            "bids": bids,
                            "asks": asks
                        })
    except Exception:
        pass

    # 3. Synthetic orderbook ladder around 100
    now_ms = int(time.time() * 1000)
    p = 100.0
    bids = [[f"{p * (1 - 0.0005 * i):.4f}", "1.0"] for i in range(1, 25)]
    asks = [[f"{p * (1 + 0.0005 * i):.4f}", "1.0"] for i in range(1, 25)]
    return web.json_response({
        "lastUpdateId": now_ms,
        "E": now_ms,
        "T": now_ms,
        "bids": bids,
        "asks": asks
    })

async def binance_klines(request):
    symbol = request.query.get("symbol", "BTCUSDT").upper()
    interval = request.query.get("interval", "1m")
    limit = request.query.get("limit", "100")
    start = request.query.get("startTime", "")
    end = request.query.get("endTime", "")

    # 1. Try Binance Vision
    try:
        url = f"https://data-api.binance.vision/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        if start:
            url += f"&startTime={start}"
        if end:
            url += f"&endTime={end}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if len(data) > 0:
                        return web.json_response(data)
    except Exception:
        pass

    # 2. Try Bybit Linear klines
    bybit_iv = "1"
    if "m" in interval:
        bybit_iv = interval.replace("m", "")
    elif "h" in interval:
        bybit_iv = str(int(interval.replace("h", "")) * 60)
    elif "d" in interval:
        bybit_iv = "D"
    try:
        url_bybit = f"https://api.bybit.com/v5/market/kline?category=linear&symbol={symbol}&interval={bybit_iv}&limit={limit}"
        if start:
            url_bybit += f"&start={start}"
        if end:
            url_bybit += f"&end={end}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url_bybit, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    raw_list = data.get("result", {}).get("list", [])
                    if len(raw_list) > 0:
                        binance_klines_list = []
                        for item in reversed(raw_list):
                            t = int(item[0])
                            binance_klines_list.append([
                                t, item[1], item[2], item[3], item[4], item[5],
                                t + 59999, item[6], 100, item[5], item[6], "0"
                            ])
                        return web.json_response(binance_klines_list)
    except Exception:
        pass

    return web.json_response([])

async def binance_ticker_price(request):
    symbol = request.query.get("symbol", "")
    try:
        url = "https://data-api.binance.vision/api/v3/ticker/price"
        if symbol:
            url += f"?symbol={symbol}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return web.Response(body=data, status=200, content_type="application/json")
    except Exception:
        pass
    return web.json_response({"symbol": symbol, "price": "100.00"} if symbol else [])

async def binance_ticker_24hr(request):
    symbol = request.query.get("symbol", "")
    try:
        url = "https://data-api.binance.vision/api/v3/ticker/24hr"
        if symbol:
            url += f"?symbol={symbol}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=2.5)) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return web.Response(body=data, status=200, content_type="application/json")
    except Exception:
        pass
    return web.json_response({"symbol": symbol, "priceChange": "0", "priceChangePercent": "0", "lastPrice": "100"} if symbol else [])

async def bybit_market_forward(request):
    path = request.path
    target_url = f"https://api.bybit.com{path}"
    if request.query_string:
        target_url += f"?{request.query_string}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(target_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                body = await resp.read()
                return web.Response(body=body, status=resp.status, content_type=resp.content_type)
        except Exception as e:
            return web.json_response({"retCode": 0, "retMsg": "OK", "result": {}})

@web.middleware
async def fallback_middleware(request, handler):
    print(f"👉 [REQ] {request.method} {request.path}?{request.query_string}", flush=True)
    try:
        return await handler(request)
    except web.HTTPNotFound:
        path = request.path
        print(f"[MOCK FALLBACK] Unhandled endpoint: {request.method} {path}?{request.query_string}", flush=True)
        # Safety net: If it's a Bybit private endpoint (/v5/user, /v5/account, /v5/order, /v5/position, /v5/trade),
        # NEVER forward to real Bybit with mock keys, as real Bybit returns 10003 API key error!
        if path.startswith("/v5/") and not path.startswith("/v5/market/"):
            now_ms = int(time.time() * 1000)
            return web.json_response({
                "retCode": 0,
                "retMsg": "OK",
                "result": {"list": []},
                "retExtInfo": {},
                "time": now_ms
            })

        host = request.headers.get("Host", "fapi.binance.com")
        if ":" in host:
            host = host.split(":")[0]
        if "binance" in host:
            target_url = f"https://data-api.binance.vision{request.rel_url}"
        elif "bybit" in host:
            target_url = f"https://api.bybit.com{request.rel_url}"
        else:
            target_url = f"https://{host}{request.rel_url}"

        try:
            headers = {k: v for k, v in request.headers.items() if k.lower() not in ("host", "content-length")}
            async with aiohttp.ClientSession() as session:
                async with session.request(request.method, target_url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
                    if resp.status == 200:
                        body = await resp.read()
                        return web.Response(body=body, status=200, content_type=resp.content_type)
        except Exception:
            pass
        if any(x in path for x in ["klines", "Trades", "orders", "Bracket", "adlQuantile", "position", "risk"]):
            return web.json_response([])
        return web.json_response({})

# ----------------- APP INITIALIZATION -----------------

def create_app():
    app = web.Application(middlewares=[fallback_middleware])
    # Bybit V5 Trading & Account
    app.router.add_get("/v5/market/time", bybit_server_time)
    app.router.add_get("/v5/market/risk-limit", bybit_risk_limit)
    app.router.add_get("/v5/user/query-api", bybit_query_api)
    app.router.add_get("/v5/user/get-member-type", bybit_get_member_type)
    app.router.add_get("/v5/account/info", bybit_account_info)
    app.router.add_get("/v5/account/config", bybit_account_config)
    app.router.add_get("/v5/account/fee-rate", bybit_fee_rate)
    app.router.add_get("/v5/account/trade-fee", bybit_fee_rate)
    app.router.add_get("/v5/account/wallet-balance", bybit_wallet_balance)
    app.router.add_get("/v5/account/balance", bybit_wallet_balance)
    app.router.add_get("/v5/account/positions", bybit_positions)
    app.router.add_get("/v5/account/leverage-info", bybit_leverage_info)
    app.router.add_post("/v5/account/set-leverage", bybit_set_leverage)
    app.router.add_get("/v5/position/list", bybit_positions)
    app.router.add_post("/v5/position/set-leverage", bybit_set_leverage)
    app.router.add_post("/v5/position/switch-mode", bybit_switch_mode)
    app.router.add_post("/v5/position/switch-isolated", bybit_switch_mode)
    app.router.add_post("/v5/position/trading-stop", bybit_trading_stop)
    app.router.add_get("/v5/order/realtime", bybit_open_orders)
    app.router.add_post("/v5/order/create", bybit_create_order)
    app.router.add_post("/v5/order/amend", bybit_order_amend)
    app.router.add_post("/v5/order/amend-batch", bybit_order_amend_batch)
    app.router.add_post("/v5/order/cancel", bybit_cancel_order)
    app.router.add_post("/v5/order/cancel-all", bybit_cancel_all)
    app.router.add_post("/v5/order/cancel-batch", bybit_cancel_batch)
    app.router.add_get("/v5/execution/list", bybit_execution_list)

    # Legacy Bybit / Trade aliases
    app.router.add_post("/v5/trade/order", bybit_create_order)
    app.router.add_post("/v5/trade/cancel-order", bybit_cancel_order)
    app.router.add_post("/v5/trade/cancel-all", bybit_cancel_all)
    app.router.add_post("/v5/trade/cancel-all-orders", bybit_cancel_all)
    app.router.add_post("/v5/trade/cancel-batch-orders", bybit_cancel_batch)
    app.router.add_post("/v5/trade/amend-batch-orders", bybit_order_amend_batch)
    app.router.add_get("/v5/trade/fills-history", bybit_execution_list)
    app.router.add_get("/v5/trade/orders-pending", bybit_open_orders)

    # Bybit WebSockets
    app.router.add_get("/v5/private", bybit_private_ws)
    app.router.add_get("/v5/public/{tail:.*}", handle_bybit_public_ws)
    app.router.add_get("/contract/private/v3", bybit_private_ws)
    app.router.add_get("/spot/private/v3", bybit_private_ws)

    # Bybit V5 Market Data
    app.router.add_get("/v5/market/{tail:.*}", bybit_market_forward)

    # Binance Futures Market Data & Tickers
    app.router.add_get("/fapi/v1/exchangeInfo", binance_futures_exchange_info)
    app.router.add_get("/fapi/v1/depth", binance_depth)
    app.router.add_get("/fapi/v2/depth", binance_depth)
    app.router.add_get("/fapi/v1/klines", binance_klines)
    app.router.add_get("/fapi/v1/ticker/price", binance_ticker_price)
    app.router.add_get("/fapi/v1/ticker/24hr", binance_ticker_24hr)

    # Binance Futures Trading & Account
    app.router.add_get("/fapi/v1/ping", binance_ping)
    app.router.add_get("/fapi/v1/time", binance_time)
    app.router.add_get("/fapi/v1/account", binance_account)
    app.router.add_get("/fapi/v2/account", binance_account)
    app.router.add_get("/fapi/v3/account", binance_account)
    app.router.add_get("/fapi/v1/balance", binance_balance)
    app.router.add_get("/fapi/v2/balance", binance_balance)
    app.router.add_get("/fapi/v1/positionRisk", binance_position_risk)
    app.router.add_get("/fapi/v2/positionRisk", binance_position_risk)
    app.router.add_get("/fapi/v1/positionSide/dual", binance_dual_side)
    app.router.add_post("/fapi/v1/positionSide/dual", binance_dual_side)
    app.router.add_get("/fapi/v1/multiAssetsMargin", binance_multi_assets)
    app.router.add_post("/fapi/v1/multiAssetsMargin", binance_multi_assets)
    app.router.add_get("/fapi/v1/leverageBracket", binance_leverage_bracket)
    app.router.add_get("/fapi/v2/leverageBracket", binance_leverage_bracket)
    app.router.add_post("/fapi/v1/leverage", binance_change_leverage)
    app.router.add_post("/fapi/v1/marginType", binance_margin_type)
    app.router.add_get("/fapi/v1/commissionRate", binance_commission_rate)
    app.router.add_get("/fapi/v1/allOrders", binance_all_orders)
    app.router.add_get("/fapi/v1/openOrders", binance_open_orders)
    app.router.add_get("/fapi/v1/allOpenOrders", binance_all_orders)
    app.router.add_delete("/fapi/v1/allOpenOrders", binance_cancel_all_orders)
    app.router.add_get("/fapi/v1/openAlgoOrders", binance_empty_list)
    app.router.add_get("/fapi/v1/algoOpenOrders", binance_empty_list)
    app.router.add_get("/fapi/v1/userTrades", binance_empty_list)
    app.router.add_get("/fapi/v1/income", binance_empty_list)
    app.router.add_get("/fapi/v1/adlQuantile", binance_adl_quantile)
    app.router.add_get("/fapi/v2/adlQuantile", binance_adl_quantile)
    app.router.add_post("/fapi/v1/countdownCancelAll", binance_countdown_cancel)
    app.router.add_post("/fapi/v1/listenKey", binance_listen_key)
    app.router.add_put("/fapi/v1/listenKey", binance_ping)
    app.router.add_delete("/fapi/v1/listenKey", binance_ping)
    app.router.add_post("/fapi/v1/order", binance_create_order)
    app.router.add_delete("/fapi/v1/order", binance_cancel_order)
    app.router.add_post("/fapi/v1/batchOrders", binance_create_order)
    app.router.add_delete("/fapi/v1/batchOrders", binance_cancel_order)
    app.router.add_get("/stream", handle_binance_ws)
    app.router.add_get("/ws/{tail:.*}", handle_binance_ws)
    app.router.add_get("/private/ws/{tail:.*}", handle_binance_ws)
    app.router.add_get("/public/{tail:.*}", handle_binance_ws)


    # Binance Spot & SAPI
    app.router.add_get("/api/v3/ping", binance_ping)
    app.router.add_get("/api/v3/time", binance_time)
    app.router.add_get("/api/v3/exchangeInfo", binance_spot_exchange_info)
    app.router.add_get("/api/v3/depth", binance_depth)
    app.router.add_get("/api/v3/klines", binance_klines)
    app.router.add_get("/api/v3/ticker/price", binance_ticker_price)
    app.router.add_get("/api/v3/ticker/24hr", binance_ticker_24hr)
    app.router.add_get("/api/v3/account", binance_spot_account)
    app.router.add_get("/api/v3/openOrders", binance_open_orders)
    app.router.add_post("/api/v3/order", binance_create_order)
    app.router.add_delete("/api/v3/order", binance_cancel_order)
    app.router.add_post("/api/v3/userDataStream", binance_spot_listen_key)
    app.router.add_put("/api/v3/userDataStream", binance_ping)
    app.router.add_delete("/api/v3/userDataStream", binance_ping)
    app.router.add_get("/sapi/v1/account/apiRestrictions", binance_api_restrictions)
    app.router.add_get("/sapi/v1/account/status", binance_account_status)
    app.router.add_get("/sapi/v1/capital/config/getall", binance_capital_config)

    async def demo_reset(req):
        wallet.balance = 50.0
        wallet.initial_balance = 50.0
        wallet.positions = {}
        wallet.trades_history = []
        wallet.save_history()
        now_ms = int(time.time() * 1000)
        await wallet.broadcast({
            "e": "ACCOUNT_UPDATE",
            "E": now_ms,
            "T": now_ms,
            "a": {
                "m": "ORDER",
                "B": [{"a": "USDT", "wb": "50.0000", "cw": "50.0000", "bc": "0"}],
                "P": []
            }
        })
        return web.json_response({"status": "ok", "balance": 50.0})

    # Vataga Paper Gateway Stats API by CGTehnik
    app.router.add_get("/api/demo/stats", lambda req: web.json_response({
        "balance": wallet.balance,
        "initial_balance": wallet.initial_balance,
        "positions": {
            "binance": wallet.binance_positions,
            "bybit": wallet.bybit_positions
        },
        "trades": wallet.trades_history[-50:]
    }))
    app.router.add_post("/api/demo/reset", demo_reset)
    app.router.add_get("/api/demo/reset", demo_reset)

    return app

# ----------------- TCP CONNECT TUNNEL PROXY -----------------

async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            data = await reader.read(8192)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def handle_proxy_conn(client_reader, client_writer):
    try:
        initial_data = await client_reader.read(4096)
        if not initial_data:
            client_writer.close()
            return

        lines = initial_data.split(b"\r\n")
        first_line = lines[0].decode('latin1', errors='replace')
        parts = first_line.split()

        if len(parts) >= 2 and parts[0].upper() == "CONNECT":
            target = parts[1]
            if ":" in target:
                host, port_str = target.split(":", 1)
                port = int(port_str)
            else:
                host = target
                port = 443

            if host in MOCK_HOSTS:
                dest_host = "127.0.0.1"
                dest_port = MOCK_PORT
                print(f"[TUNNEL] Intercepting {host}:{port} -> Mock Engine 127.0.0.1:{MOCK_PORT}", flush=True)
            else:
                dest_host = host
                dest_port = port
                print(f"[TUNNEL] Passthrough to real internet: {dest_host}:{dest_port}", flush=True)

            try:
                remote_reader, remote_writer = await asyncio.open_connection(dest_host, dest_port)
            except Exception as e:
                print(f"[TUNNEL] Failed connect to {dest_host}:{dest_port}: {e}", flush=True)
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\n\r\n")
                await client_writer.drain()
                client_writer.close()
                return

            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await client_writer.drain()

            await asyncio.gather(
                pipe(client_reader, remote_writer),
                pipe(remote_reader, client_writer),
                return_exceptions=True
            )
        else:
            # Plain HTTP forward
            try:
                remote_reader, remote_writer = await asyncio.open_connection("127.0.0.1", MOCK_PORT)
                remote_writer.write(initial_data)
                await remote_writer.drain()
                await asyncio.gather(
                    pipe(client_reader, remote_writer),
                    pipe(remote_reader, client_writer),
                    return_exceptions=True
                )
            except Exception as e:
                client_writer.close()

    except Exception as e:
        print(f"[TUNNEL ERROR] {e}", flush=True)
    finally:
        try:
            client_writer.close()
        except Exception:
            pass

async def order_matching_loop():
    print("🎯 [MATCHING ENGINE] Real-time limit order matcher active", flush=True)
    while True:
        try:
            if not wallet.open_orders:
                await asyncio.sleep(0.12)
                continue

            orders_by_symbol = {}
            for oid, o in list(wallet.open_orders.items()):
                sym = o.get("symbol")
                if sym:
                    orders_by_symbol.setdefault(sym, []).append((oid, o))

            for sym, orders in orders_by_symbol.items():
                quote = await wallet.get_market_quote(sym)
                if not quote:
                    continue
                best_bid = quote.get("bid", 0.0)
                best_ask = quote.get("ask", 0.0)
                last_price = quote.get("last", 0.0)
                if best_bid <= 0 or best_ask <= 0:
                    continue

                for oid, o in orders:
                    if oid not in wallet.open_orders:
                        continue

                    side = o["side"].upper()
                    limit_price = float(o["price"])
                    qty = float(o["qty"])
                    should_fill = False
                    exec_price = limit_price

                    if side in ["BUY", "B"]:
                        # Limit BUY executes when market ask drops to/below limit price, or last price drops to/below limit price
                        if best_ask <= limit_price or (last_price > 0 and last_price <= limit_price):
                            should_fill = True
                            exec_price = min(limit_price, best_ask if best_ask > 0 else limit_price)
                    else:
                        # Limit SELL executes when market bid rises to/above limit price, or last price rises to/above limit price
                        if best_bid >= limit_price or (last_price > 0 and last_price >= limit_price):
                            should_fill = True
                            exec_price = max(limit_price, best_bid if best_bid > 0 else limit_price)

                    if should_fill:
                        del wallet.open_orders[oid]
                        is_bin = (o.get("exchange") == "binance")
                        exch_tag = "BINANCE" if is_bin else "BYBIT"
                        print(f"🎯 [{exch_tag} LIMIT HIT & EXECUTED] {sym} | {side} {qty} @ {exec_price:.4f} (Limit: {limit_price:.4f}, Best Ask: {best_ask:.4f}, Best Bid: {best_bid:.4f})", flush=True)
                        await wallet.execute_fill(
                            symbol=sym,
                            side=side,
                            qty=qty,
                            exec_price=exec_price,
                            client_order_id=o.get("clientOrderId", f"ord_{oid}"),
                            order_type="LIMIT",
                            is_binance=is_bin,
                            order_id_num=int(oid)
                        )
        except Exception as e:
            print(f"[MATCHING ENGINE ERROR] {e}", flush=True)
        await asyncio.sleep(0.12)

async def start_services():
    # 0. Pre-cache exchange info
    try:
        await get_fapi_exchange_info()
    except Exception as e:
        print(f"[PRELOAD ERROR] {e}", flush=True)

    # 1. Load SSL Certificate
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_ctx.load_cert_chain(CRT_PATH, KEY_PATH)

    # 2. Start internal HTTPS Mock API on 8990
    app = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '127.0.0.1', MOCK_PORT, ssl_context=ssl_ctx)
    await site.start()

    # 3. Start background limit order matching engine
    asyncio.create_task(order_matching_loop())

    # 4. Start Proxy Tunnel on 8989
    proxy_server = await asyncio.start_server(handle_proxy_conn, '127.0.0.1', PROXY_PORT)

    print(f"===========================================================", flush=True)
    print(f"  🚀 VATAGA PAPER GATEWAY by CGTehnik ACTIVE!", flush=True)
    print(f"  • Proxy Port (Vataga connects here): 127.0.0.1:{PROXY_PORT}", flush=True)
    print(f"  • Mock SSL Engine: 127.0.0.1:{MOCK_PORT}", flush=True)
    print(f"  • Virtual Balance: ${wallet.balance:.2f} USDT", flush=True)
    print(f"  • Supported Exchanges: Binance Futures & Bybit V5 Linear", flush=True)
    print(f"===========================================================", flush=True)

    async with proxy_server:
        await proxy_server.serve_forever()

if __name__ == "__main__":
    asyncio.run(start_services())
