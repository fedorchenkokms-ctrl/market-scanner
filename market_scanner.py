#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
market_scanner.py — фоновый поиск линейных неэффективностей по всему рынку
с самообучением и выдачей сигналов.

КАК УСТРОЕНО
============
Торгуем на Bybit (спот YS:, перпы YF:): сигналы выдаются только по этим инструментам.
Остальные площадки — источники информации о том, «кто опережает»: Binance (S:, F:),
сами акции США (A:, Alpaca), Мосбиржа через брокера Финам (FN:). Акции США учитываются
только в часы сессии NYSE/Nasdaq, Мосбиржа — в часы, когда по ней идут сделки.
Все минутные бары (OHLCV) сохраняются в локальную базу SQLite для будущих бэктестов.

Каждую минуту приходит новый бар по всем инструментам, и движок:

1. ИЩЕТ НОВЫЕ ЗАКОНОМЕРНОСТИ (раз в scan_every баров, по последним window барам).
   Перебираются тысячи гипотез трёх семейств:
     LAG — движение инструмента A за L баров предсказывает движение B на h баров вперёд.
           A = B  → моментум / возврат к среднему (автокорреляция);
           A ≠ B  → lead-lag (A опережает B); A = MKT → рынок в целом опережает B.
     RES — отклонение инструмента от рынка (остаток после вычета β·рынок) продолжается
           или схлопывается.
     SPR — спред двух инструментов (log A − β·log B) стационарен и возвращается к среднему
           (коинтеграция, тест Энгла–Грейнджера). Сюда попадает базис спот–фьючерс.
   Гипотеза проходит два фильтра: сильная статистика на первой части окна (по умолчанию
   70%) И подтверждение на последних 30%, причём с учётом комиссии (прибыль после
   издержек > 0). Это защищает от «нашёл шум».

2. ПРОВЕРЯЕТ ИХ ВЖИВУЮ. Каждая новая гипотеза получает собственную мини-модель
   (рекурсивный МНК с забыванием — коэффициенты подстраиваются после каждого бара)
   и торгует «в тени» (бумажные сделки с комиссией). Жизненный цикл:
     testing  → active   : набрала ≥ promote_min_trades сделок и t-статистика ≥ promote_t
     active   → testing  : последние demote_window сделок в минусе
     testing  → retired  : много сделок без результата, или поиск давно её не подтверждает
   СИГНАЛЫ выдают только активные гипотезы — те, что доказали себя на новых данных.

3. ПОМНИТ. Состояние (все гипотезы, их коэффициенты и статистика) сохраняется на диск,
   после перезапуска обучение продолжается. Сигналы и события пишутся в SQLite и JSONL.

РЕЖИМЫ
======
  python market_scanner.py init-config              # создать config.json
  python market_scanner.py live                     # работа в реальном времени
  python market_scanner.py status                   # активные закономерности и сигналы
  python market_scanner.py backfill --days 90       # скачать глубокую историю в базу
  python market_scanner.py db-info                  # что лежит в базе
  python market_scanner.py export --out data --from 2026-11-01 --symbols YF:BTCUSDT,F:BTCUSDT
  python market_scanner.py replay --from-db --from 2026-11-01   # бэктест по базе
  python market_scanner.py replay --csv-dir data    # бэктест по CSV
  python market_scanner.py replay --demo            # проверка на синтетическом рынке
  python market_scanner.py finam-search BTC         # найти тикер Мосбиржи в Финаме

Нужны: Python 3.9+, numpy, pandas (на Windows ещё tzdata). Публичные данные бирж;
для самих акций США — бесплатный ключ Alpaca (необязательно).
Это исследовательский инструмент. Сигналы — не гарантия прибыли.
"""
import argparse
import json
import logging
import math
import os
import pickle
import sqlite3
import sys
import time
import threading
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

# ============================================================================
# Настройки
# ============================================================================
DEFAULT_CONFIG = {
    # Торгуем на Bybit: сигналы выдаются только по инструментам с этими префиксами.
    # Остальные площадки — источники информации («кто опережает»).
    "trade_prefixes": ["YS", "YF"],

    # --- Bybit (торговая площадка): YS: — спот, YF: — линейные перпы
    "bybit_spot":    {"enabled": True, "top_n": 30},
    "bybit_linear":  {"enabled": True, "top_n": 40},

    # --- Binance (только данные): S: — спот, F: — перпы
    "binance_spot":    {"enabled": True, "top_n": 20},
    "binance_futures": {"enabled": True, "top_n": 30},

    # --- MOEX через брокера Финам (Finam Trade API). FN: — инструменты Мосбиржи.
    # Ключ (secret) создаётся в личном кабинете Финама → Trade API.
    # Тикеры в формате Финама ТИКЕР@MIC: акции — @MISX, фьючерсы — @RTSX (пример: SiZ6@RTSX).
    # Найти точный тикер: python market_scanner.py finam-search BTC
    "finam": {"enabled": False, "secret": "", "symbols": []},
    "moex": {"enabled": False, "tickers": []},  # запасной вариант: бесплатный ISS (задержка ~15 мин)
    "exclude_bases": ["USDC", "FDUSD", "TUSD", "BUSD", "USDP", "DAI", "EUR", "AEUR",
                      "USD1", "USDE", "XUSD", "PAXG", "WBTC", "BFUSD", "RLUSD"],

    # --- Американский рынок: сами акции + их «двойники» на криптобиржах
    "us_market": {
        "enabled": True,
        "tickers": ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "AMD", "NFLX",
                    "PLTR", "COIN", "MSTR", "HOOD", "CRCL", "ORCL", "INTC", "MU", "SPY", "QQQ"],
        "bybit_xstocks": {"enabled": True, "top_n": 30},       # YS: токенизированные акции
        "bybit_stock_perps": {"enabled": True, "top_n": 30},   # YF: перпы Bybit на акции
        "binance_stock_perps": {"enabled": True, "top_n": 30},  # F: перпы Binance на акции (данные)
        # Сами акции. Бесплатный ключ Alpaca: котировки IEX в реальном времени.
        "alpaca": {"enabled": False, "key_id": "", "secret_key": "", "feed": "iex", "max_symbols": 60},
    },
    # Инструменты с ограниченными часами торгов: "us" — сессия NYSE/Nasdaq по часам,
    # "data" — часы определяются по фактическим сделкам (Мосбиржа: вечерние, выходные сессии)
    "sessions": {"A": "us", "FN": "data", "M": "data"},
    "max_stale_bars": 3,         # не торговать инструмент, по которому сделок не было дольше N минут
    "session_min_rows": 600,     # минимум минут торговой сессии в окне для поиска

    # --- Локальная база истории (OHLCV) для бэктестов
    "history_db": "scanner_data/history.db",

    "window": 6000,           # баров истории для поиска (~4 суток: нужно, чтобы попало 2–3 сессии США)
    "scan_every": 60,         # искать новые закономерности каждые N баров
    "lookbacks": [1, 3, 10, 30],  # L: за сколько баров берём движение-«причину»
    "horizons": [1, 5, 15],       # h: на сколько баров вперёд предсказываем
    "val_frac": 0.3,          # доля окна для подтверждения
    "z_discover": 4.5,        # порог значимости при поиске (высокий — тестов тысячи)
    "t_validate": 2.0,        # порог подтверждения на отложенной части
    "min_val_trades": 30,
    "pair_min_corr": 0.6,     # кандидаты в спреды: корреляция доходностей выше
    "pairs_max": 200,
    "max_new_per_scan": 40,
    "z_incremental": 3.0,     # новая причина для того же инструмента должна давать НОВУЮ информацию
    "max_per_target": 3,      # не больше стольких причин на один инструмент и горизонт
    "max_hypotheses": 400,

    # издержки на ОДНУ сторону сделки (доля): комиссия тейкера + запас
    # Комиссия тейкера на одну сторону (доля). Bybit VIP0: спот 0.1%, перпы 0.055%.
    # Поставьте свои, если у вас VIP-уровень или вы торгуете лимитками (мейкер 0.02% на перпах).
    "cost_per_side": {"YS": 0.001, "YF": 0.00055, "S": 0.001, "F": 0.0005, "A": 0.0003,
                      "FN": 0.0002, "M": 0.0003, "X": 0.0005},
    "slippage": 0.0002,
    "edge_mult": 1.2,         # входить, если ожидаемое движение > edge_mult × издержки

    "rls_lambda": 0.995,      # забывание: память ≈ 1/(1-λ) ≈ 200 наблюдений
    "promote_min_trades": 20,
    "promote_t": 2.0,
    "demote_window": 50,
    "retire_min_trades": 60,
    "retire_t": 0.5,
    "retire_unconfirmed_scans": 24,
    "retire_cooldown_scans": 48,  # списанную гипотезу не берём обратно столько циклов поиска

    "data_dir": "scanner_data",
    "universe_refresh_hours": 24,
    "display_utc_offset": 10,  # часовой пояс для вывода (Владивосток = +10)
}

log = logging.getLogger("scanner")


class _WarnCounter(logging.Handler):
    """Считает предупреждения (ошибки сети, пустые ответы бирж) для отчёта о здоровье данных."""
    def __init__(self):
        super().__init__(logging.WARNING)
        self.n = 0

    def emit(self, record):
        self.n += 1

    def take(self):
        n, self.n = self.n, 0
        return n


WARN = _WarnCounter()


def setup_logging(data_dir):
    os.makedirs(data_dir, exist_ok=True)
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fmt = logging.Formatter("%(asctime)s  %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(os.path.join(data_dir, "scanner.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    log.addHandler(fh)
    if sys.stdout is not None:  # в оконной программе консоли нет
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        log.addHandler(sh)
    log.addHandler(WARN)


def load_config(path):
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))
    if path and os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            user = json.load(f)
        for k, v in user.items():
            if isinstance(v, dict) and isinstance(cfg.get(k), dict):
                cfg[k].update(v)
            else:
                cfg[k] = v
    return cfg


try:
    from zoneinfo import ZoneInfo
    _NY = ZoneInfo("America/New_York")
except Exception:  # на Windows без пакета tzdata — считаем летнее время вручную
    _NY = None


def _ny_time(ts_ms):
    if _NY is not None:
        return datetime.fromtimestamp(ts_ms / 1000, _NY)
    u = datetime.fromtimestamp(ts_ms / 1000, timezone.utc)
    y = u.year
    mar = datetime(y, 3, 8, 7, tzinfo=timezone.utc)
    mar += timedelta(days=(6 - mar.weekday()) % 7)       # 2-е воскресенье марта, 2:00 EST
    nov = datetime(y, 11, 1, 6, tzinfo=timezone.utc)
    nov += timedelta(days=(6 - nov.weekday()) % 7)       # 1-е воскресенье ноября, 2:00 EDT
    return u + timedelta(hours=-4 if mar <= u < nov else -5)


def us_session(ts_ms):
    """Минутный бар, начинающийся в ts_ms, внутри основной сессии NYSE/Nasdaq (9:30–16:00 NY)."""
    d = _ny_time(ts_ms)
    if d.weekday() >= 5:
        return False
    m = d.hour * 60 + d.minute
    return 570 <= m < 960


def fmt_ts(ts_ms, cfg):
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc) + timedelta(hours=cfg["display_utc_offset"])
    return dt.strftime("%Y-%m-%d %H:%M")


# ============================================================================
# Буфер цен (лог-цены всех инструментов + индекс рынка MKT)
# ============================================================================
class Buffer:
    def __init__(self, symbols, window, sessions=None, max_stale=3, trade_prefixes=()):
        sessions = sessions or {}
        self.syms = list(symbols)
        self.N = len(self.syms)
        self.col = {s: i for i, s in enumerate(self.syms)}
        self.col["MKT"] = self.N
        pref = [s.split(":")[0] for s in self.syms]
        self.kind = np.array([sessions.get(p, "") for p in pref], dtype=object)
        self.is_sess = self.kind != ""
        self.tradable = np.array([(not trade_prefixes) or p in trade_prefixes for p in pref], dtype=bool)
        self.max_stale = max_stale
        self.stale = np.zeros(self.N, dtype=np.int64)  # сколько баров подряд не было сделок
        self.keep = window + 500
        self.cap = 2 * window + 1000
        self.P = np.full((self.cap, self.N + 1), np.nan)
        self.T = np.zeros(self.cap, dtype=np.int64)
        self.S = np.zeros(self.cap, dtype=bool)  # шла ли сессия США в этом баре
        self.Z = np.zeros((self.cap, self.N), dtype=bool)  # были ли свежие сделки
        self.n = 0
        self.base = 0  # абсолютный номер бара в строке 0

    def append(self, ts, closes):
        if self.n == self.cap:
            k = self.keep
            self.P[:k] = self.P[self.n - k:self.n]
            self.T[:k] = self.T[self.n - k:self.n]
            self.S[:k] = self.S[self.n - k:self.n]
            self.Z[:k] = self.Z[self.n - k:self.n]
            self.P[k:] = np.nan
            self.base += self.n - k
            self.n = k
        row = np.full(self.N + 1, np.nan)
        for s, v in closes.items():
            i = self.col.get(s)
            if i is not None and i < self.N and v and v > 0:
                row[i] = math.log(v)
        got = np.isfinite(row[:self.N])
        self.stale = np.where(got, 0, self.stale + 1)
        if self.n:
            prev = self.P[self.n - 1]
            miss = np.isnan(row[:self.N])
            row[:self.N][miss] = prev[:self.N][miss]  # нет нового бара — цена прежняя
            r = (row[:self.N] - prev[:self.N])[~self.is_sess]  # индекс рынка — по круглосуточным
            r = r[np.isfinite(r)]
            row[self.N] = (prev[self.N] if np.isfinite(prev[self.N]) else 0.0) + (r.mean() if len(r) else 0.0)
        else:
            row[self.N] = 0.0
        self.P[self.n] = row
        self.T[self.n] = ts
        self.S[self.n] = us_session(ts)
        self.Z[self.n] = self.stale <= self.max_stale
        self.n += 1
        return self.base + self.n - 1

    def window(self, W):
        return self.P[max(0, self.n - W):self.n]

    def sess_window(self, W):
        return self.S[max(0, self.n - W):self.n]

    def fresh_window(self, W):
        return self.Z[max(0, self.n - W):self.n]


# ============================================================================
# Гипотеза = одна найденная закономерность + её самообучающаяся мини-модель
# ============================================================================
class Hyp:
    def __init__(self, kind, tgt, src, L, h, cost, b_disc, fscale=1.0, beta=0.0, mu=0.0, sd=1.0, hl=0.0):
        self.kind, self.tgt, self.src, self.L, self.h = kind, tgt, src, L, h
        self.cost, self.b_disc, self.fscale = cost, b_disc, fscale
        self.beta, self.mu, self.sd, self.hl = beta, mu, sd, hl
        self.state = "testing"
        self.th0 = self.th1 = 0.0
        self.P00 = self.P01 = self.P11 = 1.0
        self.pnls = deque(maxlen=200)
        self.n_trades = 0
        self.sig_n = 0
        self.sig_sum = 0.0
        self.sig_wins = 0
        self.created = 0
        self.confirmed_scan = 0
        self.val_net = 0.0
        self.pending = deque()
        self.open = None
        self.ct = self.cs = -1
        self.session = False

    @property
    def key(self):
        if self.kind == "LAG":
            return f"LAG|{self.src}|{self.L}|{self.tgt}|{self.h}"
        if self.kind == "RES":
            return f"RES|{self.tgt}|{self.L}|{self.h}"
        return f"SPR|{self.tgt}|{self.src}|{self.h}"

    def describe(self):
        same = "в ту же сторону" if self.b_disc > 0 else "в обратную сторону"
        if self.kind == "LAG" and self.src == self.tgt:
            what = "моментум" if self.b_disc > 0 else "возврат к среднему"
            return f"{self.tgt}: {what} — движение за {self.L} бар. → следующие {self.h} бар."
        if self.kind == "LAG":
            who = "Рынок в целом" if self.src == "MKT" else self.src
            return f"{who} опережает {self.tgt}: движение за {self.L} бар. → {self.tgt} на {self.h} бар. {same}"
        if self.kind == "RES":
            what = "продолжается" if self.b_disc > 0 else "схлопывается"
            return f"{self.tgt}: отклонение от рынка за {self.L} бар. {what} в следующие {self.h} бар."
        return (f"Спред {self.tgt} − {self.beta:.3f}·{self.src} возвращается к среднему "
                f"(полураспад ≈{self.hl:.0f} бар., горизонт {self.h})")

    def symbols(self):
        return [s for s in (self.tgt, self.src) if s and s != "MKT"]

    def bind(self, buf):
        self.ct = buf.col[self.tgt]
        self.cs = buf.col[self.src] if self.src else -1
        self.legs = [buf.col[s] for s in self.symbols()]
        self.session = any(buf.kind[i] == "us" for i in self.legs)
        self.pending.clear()
        self.open = None

    def level(self, P, r):
        if self.kind == "SPR":
            return P[r, self.ct] - self.beta * P[r, self.cs]
        return P[r, self.ct]

    def feature(self, P, r):
        if self.kind == "LAG":
            return (P[r, self.cs] - P[r - self.L, self.cs]) / self.fscale
        if self.kind == "RES":
            m = P[r, -1] - P[r - self.L, -1]
            return ((P[r, self.ct] - P[r - self.L, self.ct]) - self.beta * m) / self.fscale
        return (self.level(P, r) - self.mu) / self.sd

    def init_rls(self, x, y, lam):
        n = len(x)
        sx, sxx, sy, sxy = x.sum(), (x * x).sum(), y.sum(), (x * y).sum()
        A = np.array([[n, sx], [sx, sxx]]) + np.eye(2)
        th = np.linalg.solve(A, np.array([sy, sxy]))
        shrink = min(1.0, (1.0 / (1.0 - lam)) / max(n, 1))
        Pm = np.linalg.inv(A * shrink + np.eye(2) * 1e-6)
        self.th0, self.th1 = float(th[0]), float(th[1])
        self.P00, self.P01, self.P11 = float(Pm[0, 0]), float(Pm[0, 1]), float(Pm[1, 1])

    def rls_update(self, x, y, lam):
        # рекурсивный МНК: коэффициенты подстраиваются под каждое новое наблюдение
        p0 = self.P00 + self.P01 * x
        p1 = self.P01 + self.P11 * x
        den = lam + p0 + x * p1
        k0, k1 = p0 / den, p1 / den
        err = y - (self.th0 + self.th1 * x)
        self.th0 += k0 * err
        self.th1 += k1 * err
        self.P00 = (self.P00 - k0 * p0) / lam
        self.P01 = (self.P01 - k0 * p1) / lam
        self.P11 = (self.P11 - k1 * p1) / lam

    def tstat(self, last=100):
        p = list(self.pnls)[-last:]
        if len(p) < 5:
            return 0.0
        a = np.array(p)
        sd = a.std(ddof=1)
        return float(a.mean() / sd * math.sqrt(len(a))) if sd > 0 else 0.0

    def avg_bps(self, last=100):
        p = list(self.pnls)[-last:]
        return float(np.mean(p) * 1e4) if p else 0.0

    def winrate(self, last=100):
        p = list(self.pnls)[-last:]
        return float(np.mean(np.array(p) > 0)) if p else 0.0


# ============================================================================
# Статистика
# ============================================================================
EG_CRIT = -3.90  # Энгл–Грейнджер, 2 ряда, 1%


def adf_tstat(s):
    ds = np.diff(s)
    y = ds[1:]
    X = np.column_stack([np.ones_like(y), s[1:-1], ds[:-1]])
    b, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ b
    s2 = resid @ resid / max(len(y) - 3, 1)
    se = math.sqrt(s2 * np.linalg.pinv(X.T @ X)[1, 1])
    g = b[1]
    hl = -math.log(2) / math.log(1 + g) if -1 < g < 0 else float("inf")
    return g / se if se > 0 else 0.0, hl


def colcorr(X, Y):
    Xc = X - X.mean(0)
    Yc = Y - Y.mean(0)
    den = np.sqrt((Xc ** 2).sum(0) * (Yc ** 2).sum(0))
    with np.errstate(invalid="ignore", divide="ignore"):
        return np.where(den > 0, (Xc * Yc).sum(0) / den, 0.0)


# ============================================================================
# Движок
# ============================================================================
class Engine:
    def __init__(self, cfg, symbols, store=None):
        self.cfg = cfg
        self.store = store
        self.hyps = {}
        self.scan_no = 0
        self.retired_total = 0
        self.cooldown = {}
        self.last_ts = -1  # последний обработанный бар: повторно на старых данных не учимся
        self.last_scan_abs = -10 ** 9
        self.set_universe(symbols)

    # --- вселенная инструментов -------------------------------------------
    def set_universe(self, symbols):
        self.buf = Buffer(symbols, self.cfg["window"], self.cfg["sessions"], self.cfg["max_stale_bars"],
                          tuple(self.cfg.get("trade_prefixes") or ()))
        self.last_scan_abs = -10 ** 9
        for k in list(self.hyps):
            h = self.hyps[k]
            if all(s in self.buf.col for s in h.symbols()):
                h.bind(self.buf)
            else:
                self._retire(h, 0, "инструмент выпал из списка")

    def leg_cost(self, sym):
        pref = sym.split(":")[0] if ":" in sym else "X"
        c = self.cfg["cost_per_side"]
        return c.get(pref, c.get("X", 0.0005)) + self.cfg["slippage"]

    # --- обработка нового бара ---------------------------------------------
    def on_bar(self, ts, closes, emit=True):
        a = self.buf.append(ts, closes)
        if ts <= self.last_ts:
            return []  # бар уже был обработан до перезапуска — только заполняем историю
        self.last_ts = ts
        r = a - self.buf.base
        P = self.buf.P
        lam = self.cfg["rls_lambda"]
        edge = self.cfg["edge_mult"]
        stale_ok = self.buf.stale <= self.cfg["max_stale_bars"]
        sess_now = self.buf.S[r]
        sess_exit = {hz: us_session(ts + hz * 60000) for hz in self.cfg["horizons"]}
        raw = []
        for h in list(self.hyps.values()):
            lvl = h.level(P, r)
            while h.pending and h.pending[0][0] <= a:
                due, x, l0, okp = h.pending.popleft()
                y = lvl - l0
                if okp and due == a and math.isfinite(y):
                    h.rls_update(x, y, lam)  # самообучение на свершившемся факте
            if h.open and h.open[0] <= a:
                _, d, l0, was_active, ts0, pred0 = h.open
                h.open = None
                pnl = d * (lvl - l0) - h.cost
                if math.isfinite(pnl) and self.store:
                    self.store.trade((int(ts0), int(ts), h.key, h.kind, h.tgt, h.h, d, pred0 * 1e4,
                                      h.cost * 1e4, (pnl + h.cost) * 1e4, pnl * 1e4, int(was_active),
                                      self.scan_no - getattr(h, "created_scan", 0), h.val_net * 1e4))
                if math.isfinite(pnl):
                    h.pnls.append(pnl)
                    h.n_trades += 1
                    if was_active:
                        h.sig_n += 1
                        h.sig_sum += pnl
                        h.sig_wins += pnl > 0
                    self._lifecycle(h, ts)
                    if h.state == "retired":
                        continue
            if r - h.L < 0:
                continue
            x = h.feature(P, r)
            if not (math.isfinite(x) and math.isfinite(lvl)):
                continue
            # торговать можно, только если по всем ногам идут сделки и (для акций) открыта сессия
            ok = all(stale_ok[i] for i in h.legs) and (
                not h.session or (sess_now and sess_exit.get(h.h, us_session(ts + h.h * 60000))))
            h.pending.append((a + h.h, x, lvl, ok))
            pred = h.th0 + h.th1 * x
            if ok and h.open is None and abs(pred) > edge * h.cost:
                d = 1 if pred > 0 else -1
                h.open = (a + h.h, d, lvl, h.state == "active", ts, pred)
                if h.state == "active" and emit:
                    raw.append((h, pred))
        signals = self._merge_signals(raw, ts) if raw else []
        if self.store:
            self.store.health((int(ts), self.buf.N, int((self.buf.stale == 0).sum()), WARN.take()))
        if self.buf.n >= self.cfg["window"] and a - self.last_scan_abs >= self.cfg["scan_every"]:
            self.discover(ts)
            self.last_scan_abs = a
        return signals

    # --- жизненный цикл гипотезы -------------------------------------------
    def _lifecycle(self, h, ts):
        c = self.cfg
        t = h.tstat()
        if h.state == "testing" and h.n_trades >= c["promote_min_trades"] and t >= c["promote_t"]:
            h.state = "active"
            self._event(ts, h, "АКТИВИРОВАНА",
                        f"{h.n_trades} бумажных сделок, в среднем {h.avg_bps():+.1f} б.п., t={t:.1f}")
        elif h.state == "active":
            recent = list(h.pnls)[-c["demote_window"]:]
            if len(recent) >= c["demote_window"] and np.mean(recent) < 0:
                h.state = "testing"
                self._event(ts, h, "ПРИОСТАНОВЛЕНА", f"последние {len(recent)} сделок в минусе")
        elif h.state == "testing" and h.n_trades >= c["retire_min_trades"] and t < c["retire_t"]:
            self._retire(h, ts, f"не подтвердилась вживую ({h.n_trades} сделок, t={t:.1f})")

    def _retire(self, h, ts, why):
        h.state = "retired"
        self.hyps.pop(h.key, None)
        self.cooldown[h.key] = self.scan_no + self.cfg["retire_cooldown_scans"]
        self.retired_total += 1
        self._event(ts, h, "СПИСАНА", why)

    def _event(self, ts, h, ev, details):
        msg = f"[{ev}] {h.describe()} — {details}"
        if ev != "НОВАЯ":
            log.info(msg)
        if self.store:
            self.store.event(ts, h.key, ev, msg)

    def _merge_signals(self, raw, ts):
        """Несколько активных гипотез про один инструмент и горизонт → один сигнал."""
        groups = {}
        for h, pred in raw:
            k = (h.tgt, h.src if h.kind == "SPR" else "", h.h)
            groups.setdefault(k, []).append((h, pred))
        out = []
        for (tgt, leg2, hz), items in groups.items():
            net = sum(p for _, p in items)
            best = max(items, key=lambda z: z[0].tstat())[0]
            if net == 0 or abs(net) <= self.cfg["edge_mult"] * best.cost:
                continue  # причины противоречат друг другу — молчим
            out.append(self._signal(best, ts, 1 if net > 0 else -1, net, items))
        return out

    def _signal(self, h, ts, d, pred, items):
        side = "LONG" if d > 0 else "SHORT"
        if h.kind == "SPR":
            legs = [{"symbol": h.tgt, "side": "BUY" if d > 0 else "SELL", "ratio": 1.0},
                    {"symbol": h.src, "side": "SELL" if d > 0 else "BUY", "ratio": round(h.beta, 4)}]
            action = f"{'КУПИТЬ' if d > 0 else 'ПРОДАТЬ'} {h.tgt} / {'ПРОДАТЬ' if d > 0 else 'КУПИТЬ'} {h.beta:.3f}×{h.src}"
        else:
            legs = [{"symbol": h.tgt, "side": "BUY" if d > 0 else "SELL", "ratio": 1.0}]
            action = f"{'ЛОНГ' if d > 0 else 'ШОРТ'} {h.tgt}"
        reasons = [{"key": x.key, "why": x.describe(), "expected_bps": round(p * 1e4, 1),
                    "live_trades": x.n_trades, "live_winrate": round(x.winrate(), 3),
                    "live_avg_net_bps": round(x.avg_bps(), 1), "t_stat": round(x.tstat(), 2)} for x, p in items]
        s = {
            "ts": int(ts), "time": fmt_ts(ts, self.cfg), "key": h.key, "kind": h.kind, "side": side,
            "legs": legs, "horizon_bars": h.h, "expected_bps": round(abs(pred) * 1e4, 1),
            "cost_bps": round(h.cost * 1e4, 1), "live_trades": h.n_trades,
            "live_winrate": round(h.winrate(), 3), "live_avg_net_bps": round(h.avg_bps(), 1),
            "t_stat": round(h.tstat(), 2), "why": " + ".join(r["why"] for r in reasons), "reasons": reasons,
        }
        s["text"] = (f"{s['time']}  {action} на {h.h} бар. | ожид. {s['expected_bps']:+.1f} б.п. "
                     f"(издержки {s['cost_bps']:.1f}) | лучшая причина вживую: {h.n_trades} сд., "
                     f"{s['live_winrate'] * 100:.0f}% прибыльных, ср. {s['live_avg_net_bps']:+.1f} б.п. | {s['why']}")
        log.info("СИГНАЛ  " + s["text"])
        if self.store:
            self.store.signal(s)
        return s

    # --- поиск новых закономерностей ---------------------------------------
    def _validate(self, Xd, Yd, Xv, Yv, cost, Lh):
        c = self.cfg
        with np.errstate(invalid="ignore", divide="ignore"):
            mx, my = Xd.mean(0), Yd.mean(0)
            vx = Xd.var(0)
            b = ((Xd - mx) * (Yd - my)).mean(0) / vx
            a = my - b * mx
            pred = a + b * Xv
            trade = np.abs(pred) > c["edge_mult"] * cost
            net = np.where(trade, np.sign(pred) * Yv - cost, np.nan)
            cnt = trade.sum(0)
            mean_net = np.where(cnt > 0, np.nansum(net, 0) / np.maximum(cnt, 1), -1.0)
            sd_net = np.sqrt(np.where(cnt > 1, np.nansum((net - mean_net) ** 2, 0) / np.maximum(cnt - 1, 1), np.inf))
            t_net = mean_net / sd_net * np.sqrt(np.maximum(cnt, 1) / Lh)
            r_v = colcorr(Xv, Yv)
            t_v = r_v * np.sqrt(len(Xv) / Lh)
            ok = ((cnt >= c["min_val_trades"]) & (mean_net > 0) & (np.sign(r_v) == np.sign(b))
                  & (np.abs(t_v) >= c["t_validate"]) & np.isfinite(b))
        return ok, b, np.sqrt(vx), mean_net, t_net

    def discover(self, ts):
        c = self.cfg
        self.scan_no += 1
        W = c["window"]
        Pw = self.buf.window(W).copy()
        Sw = self.buf.sess_window(W)
        N = self.buf.N
        nan_frac = np.isnan(Pw[:, :N]).mean(0)
        for j in np.where(np.isnan(Pw[0, :N]) & (nan_frac < 0.5))[0]:  # история короче окна
            first = np.argmax(~np.isnan(Pw[:, j]))
            Pw[:first, j] = Pw[first, j]
        ok_col = ~np.isnan(Pw[:, :N]).any(0) & (np.ptp(np.nan_to_num(Pw[:, :N]), axis=0) > 0)
        Lmax, hmax = max(c["lookbacks"]), max(c["horizons"])
        allT = np.arange(Lmax, len(Pw) - hmax)
        cands = []
        # проход 1: круглосуточные инструменты (крипта, перпы, токены), все минуты
        colsA = np.where(ok_col & ~self.buf.is_sess)[0]
        if len(colsA) >= 2:
            cands += self._discover_block(Pw, allT, colsA, None)
        # проход 2+: связи с инструментами, у которых есть часы торгов (акции США, Мосбиржа) —
        # только минуты, когда этот рынок работал
        Zw = self.buf.fresh_window(W)
        for kind in sorted(set(self.buf.kind[ok_col & self.buf.is_sess])):
            grp = (self.buf.kind == kind) & ok_col
            if kind == "us":
                live = Sw
            else:
                live = Zw[:, grp].any(1)
            if not len(allT):
                break
            Tb = allT[live[allT - Lmax] & live[allT + hmax]]
            if len(Tb) >= c["session_min_rows"]:
                cands += self._discover_block(Pw, Tb, np.where(ok_col)[0], grp)
        self._accept(cands, ts)

    def _discover_block(self, Pw, T, valid, need):
        """Поиск на строках T по столбцам valid. need — если задан, оставлять только связи,
        где участвует хотя бы один такой инструмент (акции США в сессионном проходе)."""
        c = self.cfg
        N = self.buf.N
        names = self.buf.syms + ["MKT"]
        s = int(len(T) * (1 - c["val_frac"]))
        if s < 200 or len(T) - s < 100:
            return []
        involve = (lambda *cols: True) if need is None else \
            (lambda *cols: any(cc < N and need[cc] for cc in cols))
        need_ext = None if need is None else np.append(need, False)
        Lmax, hmax = max(c["lookbacks"]), max(c["horizons"])
        src_cols = np.append(valid, N)
        costs_t = np.array([2 * self.leg_cost(names[i]) for i in valid])
        cands = []

        # ---- LAG: автокорреляция, lead-lag, рынок → инструмент
        for L in c["lookbacks"]:
            F = (Pw[T] - Pw[T - L])[:, src_cols]
            for h in c["horizons"]:
                Y = (Pw[T + h] - Pw[T])[:, valid]
                Fd, Yd = F[:s], Y[:s]
                Fz = (Fd - Fd.mean(0)) / (Fd.std(0) + 1e-12)
                Yz = (Yd - Yd.mean(0)) / (Yd.std(0) + 1e-12)
                tst = (Fz.T @ Yz / s) * math.sqrt(s / max(L, h))
                si, ti = np.where(np.abs(tst) > c["z_discover"])
                keep = self.buf.tradable[valid[ti]]  # сигнал — только по инструменту, которым торгуем
                if need_ext is not None:
                    keep &= need_ext[src_cols[si]] | need_ext[valid[ti]]
                si, ti = si[keep], ti[keep]
                if len(si) == 0:
                    continue
                order = np.argsort(-np.abs(tst[si, ti]))[:600]
                si, ti = si[order], ti[order]
                ok, b, sx, mnet, tnet = self._validate(Fd[:, si], Yd[:, ti], F[s:, si], Y[s:, ti],
                                                       costs_t[ti], max(L, h))
                for k in np.where(ok)[0]:
                    if not involve(src_cols[si[k]], valid[ti[k]]):
                        continue
                    src, tgt = names[src_cols[si[k]]], names[valid[ti[k]]]
                    hy = Hyp("LAG", tgt, src, L, h, costs_t[ti[k]], b[k], fscale=sx[k])
                    cands.append((tnet[k], mnet[k], hy, F[:, si[k]] / sx[k], Y[:, ti[k]], s))

        # ---- RES: отклонение от рынка
        R1 = Pw[T[:s]] - Pw[T[:s] - 1]
        rm = R1[:, N] - R1[:, N].mean()
        betas = ((R1[:, valid] - R1[:, valid].mean(0)) * rm[:, None]).mean(0) / (rm.var() + 1e-18)
        for L in c["lookbacks"]:
            Fall = Pw[T] - Pw[T - L]
            F = Fall[:, valid] - betas * Fall[:, [N]]
            for h in c["horizons"]:
                Y = (Pw[T + h] - Pw[T])[:, valid]
                t_d = colcorr(F[:s], Y[:s]) * math.sqrt(s / max(L, h))
                idx = np.where(np.abs(t_d) > c["z_discover"])[0]
                idx = idx[self.buf.tradable[valid[idx]]]
                if need_ext is not None:
                    idx = idx[need_ext[valid[idx]]]
                if len(idx) == 0:
                    continue
                ok, b, sx, mnet, tnet = self._validate(F[:s, idx], Y[:s, idx], F[s:, idx], Y[s:, idx],
                                                       costs_t[idx], max(L, h))
                for k in np.where(ok)[0]:
                    j = idx[k]
                    if not involve(valid[j]):
                        continue
                    hy = Hyp("RES", names[valid[j]], "MKT", L, h, costs_t[j], b[k], fscale=sx[k], beta=betas[j])
                    cands.append((tnet[k], mnet[k], hy, F[:, j] / sx[k], Y[:, j], s))

        # ---- SPR: коинтеграция пар
        R = Pw[T][:, valid] - Pw[T - 1][:, valid]
        with np.errstate(invalid="ignore", divide="ignore"):
            CR = np.corrcoef(R.T)
        iu = np.triu_indices(len(valid), 1)
        pc = CR[iu]
        sel = np.where(pc > c["pair_min_corr"])[0]
        sel = sel[np.argsort(-pc[sel])][:c["pairs_max"]]
        hz = np.array(c["horizons"])
        for k in sel:
            i, j = valid[iu[0][k]], valid[iu[1][k]]
            if not involve(i, j) or not (self.buf.tradable[i] and self.buf.tradable[j]):
                continue
            pa, pb = Pw[T[:s], i], Pw[T[:s], j]
            beta, c0 = np.polyfit(pb, pa, 1)
            if not (0.2 < beta < 5):
                continue
            sd_ = pa - beta * pb
            t_adf, hl = adf_tstat(sd_)
            if t_adf > EG_CRIT or not (1 <= hl <= 3 * hmax):
                continue
            h = int(hz[np.argmin(np.abs(hz - hl))])
            S = Pw[:, i] - beta * Pw[:, j]
            mu, sd = sd_.mean(), sd_.std()
            X = ((S[T] - mu) / sd)[:, None]
            Y = (S[T + h] - S[T])[:, None]
            cost = 2 * (self.leg_cost(names[i]) + self.leg_cost(names[j]))
            ok, b, _, mnet, tnet = self._validate(X[:s], Y[:s], X[s:], Y[s:], np.array([cost]), h)
            if ok[0]:
                hy = Hyp("SPR", names[i], names[j], 1, h, cost, b[0], beta=beta, mu=mu, sd=sd, hl=hl)
                cands.append((tnet[0], mnet[0], hy, X[:, 0], Y[:, 0], s))
        return cands

    def _accept(self, cands, ts):
        c = self.cfg
        # ---- убрать «двойников»: для одного инструмента и горизонта оставляем только причины,
        #      которые добавляют новую информацию (иначе 20 монет «опережают» одну и ту же)
        cands.sort(key=lambda z: (z[2].key not in self.hyps, -z[0]))  # уже известные — в приоритете
        groups, kept = {}, []
        for cnd in cands:
            s = cnd[5]
            hy, x, y = cnd[2], cnd[3][:s], cnd[4][:s]
            g = (s, hy.kind == "SPR", hy.tgt, hy.src if hy.kind == "SPR" else "", hy.h)
            acc = groups.setdefault(g, [])
            if len(acc) >= c["max_per_target"]:
                continue
            if acc:
                X = np.column_stack([np.ones(s)] + acc + [x])
                b, *_ = np.linalg.lstsq(X, y, rcond=None)
                res = y - X @ b
                s2 = res @ res / max(s - X.shape[1], 1)
                se = math.sqrt(max(s2 * np.linalg.pinv(X.T @ X)[-1, -1], 1e-30))
                if abs(b[-1] / se) / math.sqrt(max(hy.L, hy.h)) < c["z_incremental"]:
                    continue
            acc.append(x)
            kept.append(cnd)

        # ---- обновить известные, добавить новые
        new = confirmed = 0
        for tnet, mnet, hy, x, y, _s in kept:
            old = self.hyps.get(hy.key)
            if old is not None:
                old.confirmed_scan = self.scan_no
                old.beta, old.mu, old.sd, old.fscale, old.hl = hy.beta, hy.mu, hy.sd, hy.fscale, hy.hl
                confirmed += 1
                continue
            if new >= c["max_new_per_scan"] or self.cooldown.get(hy.key, -1) > self.scan_no:
                continue
            hy.created = int(ts)
            hy.created_scan = self.scan_no
            hy.confirmed_scan = self.scan_no
            hy.val_net = float(mnet)
            hy.init_rls(x, y, c["rls_lambda"])
            hy.bind(self.buf)
            self.hyps[hy.key] = hy
            new += 1
            self._event(ts, hy, "НОВАЯ", f"на отложенной части {mnet * 1e4:+.1f} б.п. после издержек")

        # ---- забыть то, что давно не подтверждается и вживую не работает
        for h in list(self.hyps.values()):
            if h.state == "testing" and self.scan_no - h.confirmed_scan > c["retire_unconfirmed_scans"]:
                self._retire(h, ts, "закономерность больше не находится в данных")
        if len(self.hyps) > c["max_hypotheses"]:
            weak = sorted((h for h in self.hyps.values() if h.state == "testing"), key=lambda h: h.tstat())
            for h in weak[:len(self.hyps) - c["max_hypotheses"]]:
                self._retire(h, ts, "вытеснена более сильными")

        act = sum(h.state == "active" for h in self.hyps.values())
        log.info(f"Поиск #{self.scan_no}: кандидатов {len(cands)} (без двойников {len(kept)}), новых {new}, подтверждено {confirmed} | "
                 f"активных {act}, на проверке {len(self.hyps) - act}, списано всего {self.retired_total}")
        if self.store:
            self.store.scan((int(ts), self.scan_no, len(cands), len(kept), new, confirmed, act,
                             len(self.hyps) - act, self.retired_total))
            self.store.snapshot(self.hyps.values(), ts)
            self.store.commit()

    # --- сохранение ---------------------------------------------------------
    def save(self, path):
        state = {"hyps": self.hyps, "scan_no": self.scan_no, "retired_total": self.retired_total,
                 "cooldown": self.cooldown, "last_ts": self.last_ts}
        tmp = path + ".tmp"
        with open(tmp, "wb") as f:
            pickle.dump(state, f)
        os.replace(tmp, path)

    def load(self, path):
        if not os.path.exists(path):
            return False
        with open(path, "rb") as f:
            st = pickle.load(f)
        self.hyps, self.scan_no, self.retired_total = st["hyps"], st["scan_no"], st["retired_total"]
        self.cooldown = st.get("cooldown", {})
        self.last_ts = st.get("last_ts", -1)
        self.set_universe(self.buf.syms)
        log.info(f"Загружено состояние: {len(self.hyps)} гипотез")
        return True


# ============================================================================
# Хранилище сигналов и событий
# ============================================================================
class Store:
    def __init__(self, data_dir):
        self.db = sqlite3.connect(os.path.join(data_dir, "scanner.db"))
        self.jsonl = open(os.path.join(data_dir, "signals.jsonl"), "a", encoding="utf-8")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS signals(ts INTEGER, time TEXT, key TEXT, kind TEXT, side TEXT,
                legs TEXT, horizon INTEGER, expected_bps REAL, cost_bps REAL, live_trades INTEGER,
                live_winrate REAL, live_avg_net_bps REAL, t_stat REAL, text TEXT);
            CREATE TABLE IF NOT EXISTS events(ts INTEGER, key TEXT, event TEXT, text TEXT);
            CREATE TABLE IF NOT EXISTS hypotheses(key TEXT PRIMARY KEY, kind TEXT, state TEXT,
                description TEXT, trades INTEGER, winrate REAL, avg_net_bps REAL, t_stat REAL,
                signal_trades INTEGER, signal_net_bps REAL, created INTEGER, updated INTEGER);
            -- каждая бумажная сделка: ожидание vs факт, издержки, был ли это сигнал
            CREATE TABLE IF NOT EXISTS trades(ts_open INTEGER, ts_close INTEGER, key TEXT, kind TEXT,
                target TEXT, horizon INTEGER, dir INTEGER, expected_bps REAL, cost_bps REAL,
                gross_bps REAL, net_bps REAL, was_signal INTEGER, age_scans INTEGER, val_net_bps REAL);
            CREATE INDEX IF NOT EXISTS trades_ts ON trades(ts_close);
            CREATE TABLE IF NOT EXISTS scans(ts INTEGER, scan_no INTEGER, candidates INTEGER, kept INTEGER,
                new INTEGER, confirmed INTEGER, active INTEGER, testing INTEGER, retired_total INTEGER);
            CREATE TABLE IF NOT EXISTS health(ts INTEGER, n_symbols INTEGER, n_fresh INTEGER, warnings INTEGER);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
        """)

    def signal(self, s):
        self.db.execute("INSERT INTO signals VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        (s["ts"], s["time"], s["key"], s["kind"], s["side"], json.dumps(s["legs"]),
                         s["horizon_bars"], s["expected_bps"], s["cost_bps"], s["live_trades"],
                         s["live_winrate"], s["live_avg_net_bps"], s["t_stat"], s["text"]))
        self.jsonl.write(json.dumps(s, ensure_ascii=False) + "\n")
        self.jsonl.flush()

    def trade(self, row):
        self.db.execute("INSERT INTO trades VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row)

    def scan(self, row):
        self.db.execute("INSERT INTO scans VALUES(?,?,?,?,?,?,?,?,?)", row)

    def health(self, row):
        self.db.execute("INSERT INTO health VALUES(?,?,?,?)", row)

    def event(self, ts, key, ev, text):
        self.db.execute("INSERT INTO events VALUES(?,?,?,?)", (int(ts), key, ev, text))

    def snapshot(self, hyps, ts):
        self.db.execute("DELETE FROM hypotheses")
        self.db.executemany("INSERT INTO hypotheses VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", [
            (h.key, h.kind, h.state, h.describe(), h.n_trades, h.winrate(), h.avg_bps(), h.tstat(),
             h.sig_n, h.sig_sum * 1e4, h.created, int(ts)) for h in hyps])

    def commit(self):
        self.db.commit()


# ============================================================================
# Источники данных
# ============================================================================
_rl_lock = threading.Lock()
_rl_last = {}
RATE = {"binance": 0.06, "bybit": 0.05, "alpaca": 0.31, "finam": 0.25, "moex": 0.1}


def _throttle(url):
    host = urllib.parse.urlparse(url).netloc
    key = next((k for k in RATE if k in host), None)
    if not key:
        return
    with _rl_lock:
        wait = _rl_last.get(key, 0) + RATE[key] - time.time()
        _rl_last[key] = max(time.time(), _rl_last.get(key, 0) + RATE[key])
    if wait > 0:
        time.sleep(wait)


def http_json(url, params=None, retries=3, timeout=20, headers=None, body=None):
    if params:
        url += "?" + urllib.parse.urlencode(params)
    data = json.dumps(body).encode() if body is not None else None
    hdr = {"User-Agent": "market-scanner/1.0", **(headers or {})}
    if data is not None:
        hdr["Content-Type"] = "application/json"
    for attempt in range(retries):
        try:
            _throttle(url)
            req = urllib.request.Request(url, data=data, headers=hdr, method="POST" if data else "GET")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode())
        except urllib.error.HTTPError as e:
            if e.code in (400, 401, 403, 404) or attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
        except Exception as e:
            if attempt == retries - 1:
                raise
            log.warning(f"Сеть: {e}; повтор через {2 ** attempt} с")
            time.sleep(2 ** attempt)


def _now_ms():
    return int(time.time() * 1000)


def _last_closed_min():
    return (_now_ms() // 60000 - 1) * 60000


def _iso(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# Все источники отдают бары в одном виде: (время_ms, open, high, low, close, volume),
# только закрытые минуты и только минуты, в которые были сделки.

class Binance:
    """S: — спот, F: — USDT-перпы. На Binance не торгуем — используем как источник информации."""
    BASE = {"S": "https://api.binance.com/api/v3", "F": "https://fapi.binance.com/fapi/v1"}
    BAD_SUFFIX = ("UPUSDT", "DOWNUSDT", "BULLUSDT", "BEARUSDT")

    def spot_universe(self, top_n, exclude):
        rows = []
        for d in http_json(self.BASE["S"] + "/ticker/24hr"):
            sym = d["symbol"]
            if sym.endswith("USDT") and "_" not in sym and not sym.endswith(self.BAD_SUFFIX) \
                    and sym[:-4] not in exclude:
                rows.append((float(d.get("quoteVolume", 0)), sym))
        rows.sort(reverse=True)
        return [f"S:{s}" for _, s in rows[:top_n]]

    def futures_split(self, exclude, stock_tickers):
        info = http_json(self.BASE["F"] + "/exchangeInfo")
        vol = {d["symbol"]: float(d.get("quoteVolume", 0)) for d in http_json(self.BASE["F"] + "/ticker/24hr")}
        crypto, tradfi = [], []
        for s in info.get("symbols", []):
            if s.get("contractType") != "PERPETUAL" or s.get("status") != "TRADING" \
                    or s.get("quoteAsset") != "USDT":
                continue
            base = s.get("baseAsset", "")
            ut = str(s.get("underlyingType", "COIN")).upper()
            sub = " ".join(str(x).upper() for x in s.get("underlyingSubType", []))
            is_tf = ut not in ("COIN", "INDEX") or base in stock_tickers or \
                any(k in sub for k in ("TRADFI", "STOCK", "EQUITY", "ETF"))
            row = (vol.get(s["symbol"], 0.0), s["symbol"], base)
            if is_tf:
                tradfi.append(row)
            elif base not in exclude:
                crypto.append(row)
        crypto.sort(reverse=True)
        tradfi.sort(reverse=True)
        return crypto, tradfi

    def _page(self, name, limit, end_ms):
        mkt, sym = name.split(":")
        p = {"symbol": sym, "interval": "1m", "limit": limit}
        if end_ms:
            p["endTime"] = end_ms
        data = http_json(self.BASE[mkt] + "/klines", p)
        now = _now_ms()
        bars = [(int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
                for k in data if int(k[6]) < now and float(k[5]) > 0]
        return bars, (int(data[0][0]) if data else None)

    def history(self, name, minutes, end_ms=None):
        out, end = [], end_ms
        oldest = (end_ms or _now_ms()) - minutes * 60000
        while True:
            chunk, first = self._page(name, 1000, end)
            out = chunk + out
            if first is None or first <= oldest:
                break
            end = first - 1
        return [b for b in out if b[0] >= oldest]

    def latest(self, name):
        return self._page(name, 5, None)[0]


class Bybit:
    """Торговая площадка. YS: — спот (крипта и токенизированные акции xStocks), YF: — линейные перпы."""
    BASE = "https://api.bybit.com/v5/market"

    def _get(self, path, params):
        d = http_json(self.BASE + path, params)
        if d.get("retCode", 0) != 0:
            raise RuntimeError(f"Bybit {path}: {d.get('retMsg')}")
        return d["result"]

    def _instruments(self, category):
        out, cursor = [], None
        while True:
            p = {"category": category, "limit": 1000}
            if cursor:
                p["cursor"] = cursor
            r = self._get("/instruments-info", p)
            out += r.get("list", [])
            cursor = r.get("nextPageCursor")
            if not cursor:
                return out

    def _turnover(self, category):
        return {d["symbol"]: float(d.get("turnover24h") or 0)
                for d in self._get("/tickers", {"category": category}).get("list", [])}

    @staticmethod
    def _tags(d):
        return " ".join(str(d.get(k, "")) for k in ("symbolType", "innovation", "tagType", "contractType")).lower()

    def universe(self, cfg, stock_tickers):
        """→ (крипто-спот, xStocks, крипто-перпы, перпы на акции): списки (оборот, символ, базовый актив)."""
        ex = set(cfg["exclude_bases"])
        sp_t, li_t = self._turnover("spot"), self._turnover("linear")
        spot_c, xst, lin_c, lin_s = [], [], [], []
        for d in self._instruments("spot"):
            if d.get("quoteCoin") != "USDT" or d.get("status", "Trading") != "Trading":
                continue
            base, sym, tags = d.get("baseCoin", ""), d["symbol"], self._tags(d)
            if base.endswith("X") and (base[:-1] in stock_tickers or "stock" in tags):
                xst.append((sp_t.get(sym, 0.0), sym, base[:-1]))
            elif base not in ex:
                spot_c.append((sp_t.get(sym, 0.0), sym, base))
        for d in self._instruments("linear"):
            if d.get("quoteCoin") != "USDT" or d.get("status", "Trading") != "Trading" \
                    or "perpetual" not in str(d.get("contractType", "")).lower():
                continue
            base, sym, tags = d.get("baseCoin", ""), d["symbol"], self._tags(d)
            if base in stock_tickers or any(k in tags for k in ("stock", "equity", "tradfi")):
                lin_s.append((li_t.get(sym, 0.0), sym, base))
            elif base not in ex:
                lin_c.append((li_t.get(sym, 0.0), sym, base))
        for x in (spot_c, xst, lin_c, lin_s):
            x.sort(reverse=True)
        return spot_c, xst, lin_c, lin_s

    def _page(self, name, limit, end_ms):
        pref, sym = name.split(":")
        p = {"category": "spot" if pref == "YS" else "linear", "symbol": sym, "interval": "1",
             "limit": min(limit, 1000)}
        if end_ms:
            p["end"] = end_ms
        lst = self._get("/kline", p).get("list", [])
        now = _now_ms()
        bars = sorted((int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5]))
                      for k in lst if int(k[0]) + 60000 <= now and float(k[5]) > 0)
        return bars, min((int(k[0]) for k in lst), default=None)

    def history(self, name, minutes, end_ms=None):
        out, end = [], end_ms
        oldest = (end_ms or _now_ms()) - minutes * 60000
        while True:
            chunk, first = self._page(name, 1000, end)
            out = chunk + out
            if first is None or first <= oldest:
                break
            end = first - 1
        return sorted(set(b for b in out if b[0] >= oldest))

    def latest(self, name):
        return self._page(name, 5, None)[0]


class Alpaca:
    """A: — акции США. Бесплатный ключ: https://alpaca.markets (feed=iex — реальное время по бирже IEX)."""
    URL = "https://data.alpaca.markets/v2/stocks/bars"

    def __init__(self, acfg):
        self.h = {"APCA-API-KEY-ID": acfg["key_id"], "APCA-API-SECRET-KEY": acfg["secret_key"]}
        self.feed = acfg.get("feed", "iex")

    def bars(self, names, since_ms, until_ms=None):
        tick = [n.split(":")[1] for n in names]
        out = {n: [] for n in names}
        now = _now_ms()
        for i in range(0, len(tick), 50):
            token = None
            while True:
                p = {"symbols": ",".join(tick[i:i + 50]), "timeframe": "1Min", "start": _iso(since_ms),
                     "limit": 10000, "feed": self.feed}
                if until_ms:
                    p["end"] = _iso(until_ms)
                if token:
                    p["page_token"] = token
                d = http_json(self.URL, p, headers=self.h)
                for t, lst in (d.get("bars") or {}).items():
                    for b in lst:
                        ts = int(datetime.fromisoformat(b["t"].replace("Z", "+00:00")).timestamp() * 1000)
                        if ts + 60000 <= now:
                            out[f"A:{t}"].append((ts, float(b["o"]), float(b["h"]), float(b["l"]),
                                                  float(b["c"]), float(b.get("v", 0))))
                token = d.get("next_page_token")
                if not token:
                    break
        return out


class Finam:
    """FN: — Мосбиржа через брокера Финам (Finam Trade API). Символ: ТИКЕР@MIC, напр. SBER@MISX, SiZ6@RTSX."""
    BASE = "https://api.finam.ru/v1"

    def __init__(self, secret):
        self.secret = secret
        self.token, self.t_tok = None, 0.0
        self.bearer = True
        self.lock = threading.Lock()

    def _auth(self, force=False):
        with self.lock:
            if force or not self.token or time.time() - self.t_tok > 600:
                self.token = http_json(self.BASE + "/sessions", body={"secret": self.secret})["token"]
                self.t_tok = time.time()
            return self.token

    def _get(self, path, params=None):
        for attempt in range(3):
            tok = self._auth(force=attempt > 0)
            try:
                return http_json(self.BASE + path, params,
                                 headers={"Authorization": ("Bearer " if self.bearer else "") + tok})
            except urllib.error.HTTPError as e:
                if e.code != 401 or attempt == 2:
                    raise
                if attempt == 1:
                    self.bearer = not self.bearer  # в документации встречаются оба формата заголовка

    @staticmethod
    def _num(x):
        if isinstance(x, dict):
            x = x.get("value", x.get("num", 0))
        return float(x or 0)

    @staticmethod
    def _ts(x):
        if isinstance(x, dict):
            return int(x.get("seconds", 0)) * 1000
        return int(datetime.fromisoformat(str(x).replace("Z", "+00:00")).timestamp() * 1000)

    def bars(self, name, since_ms, until_ms=None):
        sym = name.split(":", 1)[1]
        until_ms = until_ms or _now_ms()
        out, t = [], since_ms
        now = _now_ms()
        while t < until_ms:
            t2 = min(t + 86400000, until_ms)
            d = self._get(f"/instruments/{urllib.parse.quote(sym)}/bars", {
                "timeframe": "TIME_FRAME_M1", "interval.start_time": _iso(t), "interval.end_time": _iso(t2)})
            for b in d.get("bars", []):
                ts = self._ts(b["timestamp"])
                v = self._num(b.get("volume"))
                if ts + 60000 <= now and v > 0:
                    out.append((ts, self._num(b["open"]), self._num(b["high"]), self._num(b["low"]),
                                self._num(b["close"]), v))
            t = t2
        return sorted(set(out))

    def search(self, query):
        d = self._get("/assets")
        q = query.upper()
        return [a for a in d.get("assets", []) if q in str(a.get("symbol", "")).upper()
                or q in str(a.get("name", "")).upper()]


class Moex:
    """M: — бесплатный ISS Мосбиржи (с задержкой ~15 мин). Формат: futures:BTZ6, shares:SBER."""
    PATHS = {"futures": "engines/futures/markets/forts", "shares": "engines/stock/markets/shares",
             "currency": "engines/currency/markets/selt"}
    MSK = timedelta(hours=3)

    def candles(self, name, since_ms):
        market, sec = name.split(":", 1)[1].split(":")
        url = f"https://iss.moex.com/iss/{self.PATHS[market]}/securities/{sec}/candles.json"
        frm = (datetime.fromtimestamp(since_ms / 1000, timezone.utc) + self.MSK).strftime("%Y-%m-%d %H:%M:%S")
        out, start = [], 0
        while True:
            d = http_json(url, {"interval": 1, "from": frm, "start": start, "iss.meta": "off"})["candles"]
            cols, rows = d["columns"], d["data"]
            if not rows:
                break
            ix = {k: cols.index(k) for k in ("open", "high", "low", "close", "volume", "begin")}
            for r in rows:
                if not r[ix["volume"]]:
                    continue
                t = datetime.strptime(r[ix["begin"]], "%Y-%m-%d %H:%M:%S") - self.MSK
                out.append((int(t.replace(tzinfo=timezone.utc).timestamp() * 1000), float(r[ix["open"]]),
                            float(r[ix["high"]]), float(r[ix["low"]]), float(r[ix["close"]]),
                            float(r[ix["volume"]])))
            start += len(rows)
        return out


# ============================================================================
# Локальная база истории (SQLite): все минутные бары OHLCV, которые видел сканер
# ============================================================================
class BarStore:
    def __init__(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self.path = path
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.Lock()
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS symbols(id INTEGER PRIMARY KEY, name TEXT UNIQUE NOT NULL);
            CREATE TABLE IF NOT EXISTS bars(sid INTEGER NOT NULL, ts INTEGER NOT NULL,
                o REAL, h REAL, l REAL, c REAL, v REAL, PRIMARY KEY(sid, ts)) WITHOUT ROWID;
        """)
        self.ids = dict((n, i) for i, n in self.db.execute("SELECT id, name FROM symbols"))

    def sid(self, name):
        if name not in self.ids:
            with self.lock:
                cur = self.db.execute("INSERT OR IGNORE INTO symbols(name) VALUES(?)", (name,))
                self.ids[name] = cur.lastrowid or self.db.execute(
                    "SELECT id FROM symbols WHERE name=?", (name,)).fetchone()[0]
        return self.ids[name]

    def save(self, name, bars):
        if not bars:
            return
        i = self.sid(name)
        with self.lock:
            self.db.executemany("INSERT OR REPLACE INTO bars VALUES(?,?,?,?,?,?,?)", [(i, *b) for b in bars])

    def commit(self):
        with self.lock:
            self.db.commit()

    def span(self, name):
        if name not in self.ids:
            return None, None
        return self.db.execute("SELECT MIN(ts), MAX(ts) FROM bars WHERE sid=?", (self.ids[name],)).fetchone()

    def symbols(self):
        return sorted(self.ids)

    def closes(self, names, since_ms=None, until_ms=None):
        """Сетка минут × инструменты (close). Минуты без сделок — NaN."""
        q = "SELECT ts, c FROM bars WHERE sid=? AND ts>=? AND ts<=? ORDER BY ts"
        lo, hi = since_ms or 0, until_ms or 2 ** 62
        series = {}
        for n in names:
            if n in self.ids:
                rows = self.db.execute(q, (self.ids[n], lo, hi)).fetchall()
                if rows:
                    a = np.array(rows)
                    series[n] = pd.Series(a[:, 1], index=a[:, 0].astype(np.int64))
        if not series:
            return pd.DataFrame()
        df = pd.DataFrame(series)
        grid = np.arange(int(df.index.min()), int(df.index.max()) + 1, 60000, dtype=np.int64)
        return df.reindex(grid).dropna(how="all")

    def ohlcv(self, name, since_ms=None, until_ms=None):
        rows = self.db.execute("SELECT ts,o,h,l,c,v FROM bars WHERE sid=? AND ts>=? AND ts<=? ORDER BY ts",
                               (self.ids.get(name, -1), since_ms or 0, until_ms or 2 ** 62)).fetchall()
        return pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])

    def info(self):
        n, lo, hi = self.db.execute("SELECT COUNT(*), MIN(ts), MAX(ts) FROM bars").fetchone()
        return {"symbols": len(self.ids), "bars": n, "from": lo, "to": hi,
                "size_mb": os.path.getsize(self.path) / 2 ** 20}


# ============================================================================
# Источники и режим реального времени
# ============================================================================
class Sources:
    def __init__(self, cfg):
        self.cfg = cfg
        self.bn, self.by, self.mx = Binance(), Bybit(), Moex()
        al = cfg["us_market"]["alpaca"]
        self.al = Alpaca(al) if cfg["us_market"]["enabled"] and al["enabled"] and al["key_id"] else None
        fc = cfg["finam"]
        self.fn = Finam(fc["secret"]) if fc["enabled"] and fc["secret"] else None

    def universe(self):
        cfg, um = self.cfg, self.cfg["us_market"]
        ex = set(cfg["exclude_bases"])
        tickers = set(um["tickers"]) if um["enabled"] else set()
        syms, report = [], []

        def safe(label, fn):
            try:
                return fn()
            except Exception as e:
                log.warning(f"{label}: не удалось получить список ({e})")
                return None

        # Binance (данные): нужен раньше Bybit, чтобы узнать, какие акции вообще торгуются как перпы
        fs = safe("Binance фьючерсы", lambda: self.bn.futures_split(ex, tickers))
        bn_tf = []
        if fs:
            crypto, tradfi = fs
            if um["enabled"] and um["binance_stock_perps"]["enabled"]:
                bn_tf = tradfi[:um["binance_stock_perps"]["top_n"]]
                tickers |= {b for _, _, b in bn_tf}
        # Bybit (торговля)
        bu = safe("Bybit", lambda: self.by.universe(cfg, tickers))
        if bu:
            spot_c, xst, lin_c, lin_s = bu
            if cfg["bybit_spot"]["enabled"]:
                syms += [f"YS:{s}" for _, s, _ in spot_c[:cfg["bybit_spot"]["top_n"]]]
                report.append(f"Bybit спот {min(len(spot_c), cfg['bybit_spot']['top_n'])}")
            if cfg["bybit_linear"]["enabled"]:
                syms += [f"YF:{s}" for _, s, _ in lin_c[:cfg["bybit_linear"]["top_n"]]]
                report.append(f"Bybit перпы {min(len(lin_c), cfg['bybit_linear']['top_n'])}")
            if um["enabled"] and um["bybit_xstocks"]["enabled"]:
                x = xst[:um["bybit_xstocks"]["top_n"]]
                syms += [f"YS:{s}" for _, s, _ in x]
                tickers |= {b for _, _, b in x}
                report.append(f"Bybit xStocks {len(x)}")
            if um["enabled"] and um["bybit_stock_perps"]["enabled"]:
                x = lin_s[:um["bybit_stock_perps"]["top_n"]]
                syms += [f"YF:{s}" for _, s, _ in x]
                tickers |= {b for _, _, b in x}
                report.append(f"Bybit перпы на акции {len(x)}")
        if cfg["binance_spot"]["enabled"]:
            r = safe("Binance спот", lambda: self.bn.spot_universe(cfg["binance_spot"]["top_n"], ex)) or []
            syms += r
            report.append(f"Binance спот {len(r)}")
        if fs and cfg["binance_futures"]["enabled"]:
            syms += [f"F:{s}" for _, s, _ in fs[0][:cfg["binance_futures"]["top_n"]]]
            report.append(f"Binance перпы {min(len(fs[0]), cfg['binance_futures']['top_n'])}")
        if bn_tf:
            syms += [f"F:{s}" for _, s, _ in bn_tf]
            report.append(f"Binance перпы на акции/TradFi {len(bn_tf)}")
        if self.al:
            stocks = sorted(t for t in tickers if t.isalpha())[:um["alpaca"]["max_symbols"]]
            syms += [f"A:{t}" for t in stocks]
            report.append(f"акции США (Alpaca) {len(stocks)}")
        if self.fn:
            syms += [f"FN:{t}" for t in cfg["finam"]["symbols"]]
            report.append(f"Мосбиржа (Финам) {len(cfg['finam']['symbols'])}")
        if cfg["moex"]["enabled"]:
            syms += [f"M:{t}" for t in cfg["moex"]["tickers"]]
            report.append(f"MOEX ISS {len(cfg['moex']['tickers'])}")
        log.info("Источники: " + ", ".join(report))
        return list(dict.fromkeys(syms))

    def fetch(self, syms, since_ms, until_ms=None, threads=16):
        """Бары по всем инструментам за [since, until]. → {символ: [(ts,o,h,l,c,v), ...]}"""
        until_ms = until_ms or _last_closed_min()
        minutes = max(1, int((until_ms - since_ms) // 60000) + 1)
        end = None if until_ms >= _last_closed_min() else until_ms

        def one(s):
            try:
                p = s.split(":")[0]
                if p in ("S", "F"):
                    return s, self.bn.history(s, minutes, end)
                if p in ("YS", "YF"):
                    return s, self.by.history(s, minutes, end)
                if p == "FN" and self.fn:
                    return s, self.fn.bars(s, since_ms, until_ms)
                if p == "M":
                    return s, [b for b in self.mx.candles(s, since_ms) if b[0] <= until_ms]
                return s, []
            except Exception as e:
                log.warning(f"{s}: {e}")
                return s, []

        with ThreadPoolExecutor(threads) as ex:
            res = dict(ex.map(one, [s for s in syms if not s.startswith("A:")]))
        a = [s for s in syms if s.startswith("A:")]
        if a and self.al:
            try:
                res.update(self.al.bars(a, since_ms, None if end is None else until_ms))
            except Exception as e:
                log.warning(f"Alpaca: {e}")
        return res

    def latest(self, syms):
        def one(s):
            try:
                p = s.split(":")[0]
                if p in ("S", "F"):
                    return s, self.bn.latest(s)
                if p in ("YS", "YF"):
                    return s, self.by.latest(s)
                if p == "FN" and self.fn:
                    return s, self.fn.bars(s, _now_ms() - 10 * 60000)
                if p == "M":
                    return s, self.mx.candles(s, _now_ms() - 10 * 60000)
                return s, []
            except Exception as e:
                log.warning(f"{s}: {e}")
                return s, []

        with ThreadPoolExecutor(16) as ex:
            res = dict(ex.map(one, [s for s in syms if not s.startswith("A:")]))
        a = [s for s in syms if s.startswith("A:")]
        if a and self.al:
            try:
                res.update(self.al.bars(a, _now_ms() - 10 * 60000))
            except Exception as e:
                log.warning(f"Alpaca: {e}")
        return res

    def sync_history(self, syms, minutes, bars_db):
        """Догружает в базу только недостающее (с последней сохранённой минуты), возвращает сетку close."""
        now = _last_closed_min()
        want = now - (minutes - 1) * 60000
        groups = {}
        for s in syms:
            _, hi = bars_db.span(s)
            start = want if hi is None or hi < want else hi + 60000
            if start <= now:
                groups.setdefault(start // 3600000, []).append((s, start))
        n_new = 0
        for _, items in sorted(groups.items()):
            start = min(st for _, st in items)
            for s, bars in self.fetch([s for s, _ in items], start).items():
                bars_db.save(s, bars)
                n_new += len(bars)
        bars_db.commit()
        log.info(f"База истории: догружено {n_new} баров")
        return bars_db.closes(syms, want, now)


def _wait(stop, seconds):
    """Пауза, которую можно прервать кнопкой «Стоп». True — пора останавливаться."""
    if stop is None:
        time.sleep(seconds)
        return False
    return stop.wait(seconds)


def run_live(cfg, stop=None, on_signal=None, on_status=None):
    """Основной цикл. stop — threading.Event для остановки из программы с окном,
    on_signal(сигнал) — колбэк на каждый новый сигнал, on_status(текст) — состояние."""
    status = on_status or (lambda s: None)
    dd = cfg["data_dir"]
    src = Sources(cfg)
    store = Store(dd)
    bars_db = BarStore(cfg["history_db"])
    state_path = os.path.join(dd, "state.pkl")
    engine = None
    while not (stop and stop.is_set()):  # цикл «обновить список инструментов → работать сутки»
        status("Получаю список инструментов…")
        syms = src.universe()
        warm = cfg["window"] + 1000
        log.info(f"Инструментов: {len(syms)}. Синхронизирую историю…")
        status(f"Загружаю историю по {len(syms)} инструментам…")
        hist = src.sync_history(syms, warm, bars_db)
        if hist.empty:
            raise RuntimeError("История не загружена ни по одному инструменту — проверьте интернет")
        hist = hist[[c for c in hist.columns if hist[c].notna().sum() >= 30]]
        syms = list(hist.columns)
        if engine is None:
            engine = Engine(cfg, syms, store)
            engine.load(state_path)
        else:
            engine.set_universe(syms)
        log.info(f"История: {len(hist)} минут × {len(syms)} инструментов. Прогрев (без сигналов)…")
        status("Прогрев на истории…")
        cols = list(hist.columns)
        for k, (ts, row) in enumerate(zip(hist.index, hist.values)):
            engine.on_bar(int(ts), dict(zip(cols, row)), emit=False)
            if stop is not None and k % 200 == 0 and stop.is_set():
                engine.save(state_path)
                return
        engine.save(state_path)
        last_ts = int(hist.index[-1])
        refresh_at = time.time() + cfg["universe_refresh_hours"] * 3600
        log.info("Работаю в реальном времени.")
        status(f"Работает: {len(syms)} инструментов")

        next_report = local_midnight_ms(_now_ms(), cfg) + 86400000
        while time.time() < refresh_at:
            if _wait(stop, 60 - time.time() % 60 + 3):  # 3 с после закрытия минуты
                engine.save(state_path)
                store.commit()
                log.info("Сканер остановлен")
                return
            if _now_ms() >= next_report:  # раз в сутки, в полночь по вашему времени
                try:
                    write_report(cfg, next_report - 86400000, next_report)
                except Exception as e:
                    log.warning(f"Отчёт не построен: {e}")
                next_report += 86400000
            got = src.latest(syms)
            cur = _last_closed_min()
            if cur - last_ts > 30 * 60000:  # компьютер «спал» — догружаем пропуск из истории
                log.info(f"Пропуск {(cur - last_ts) // 60000} мин — догружаю")
                got = src.fetch(syms, last_ts + 60000)
            bars = {t: {} for t in range(last_ts + 60000, cur + 1, 60000)}
            for s, kl in got.items():
                bars_db.save(s, kl)
                for b in kl:
                    if b[0] in bars:
                        bars[b[0]][s] = b[4]
            bars_db.commit()
            for t in sorted(bars):
                for sgn in engine.on_bar(t, bars[t], emit=True):
                    if on_signal:
                        on_signal(sgn)
                last_ts = t
            store.commit()
            if engine.buf.n and engine.last_scan_abs == engine.buf.base + engine.buf.n - 1:
                engine.save(state_path)


def run_backfill(cfg, days, stop=None):
    """Загрузить глубокую историю по текущему списку инструментов в локальную базу."""
    src = Sources(cfg)
    bars_db = BarStore(cfg["history_db"])
    syms = src.universe()
    now = _last_closed_min()
    target = now - days * 1440 * 60000
    log.info(f"Загрузка истории за {days} дн. по {len(syms)} инструментам → {cfg['history_db']}")
    for k, s in enumerate(syms, 1):
        if stop is not None and stop.is_set():
            log.info("Загрузка истории прервана")
            break
        lo, hi = bars_db.span(s)
        ranges = [(target, now)] if lo is None else \
            ([(target, lo - 60000)] if lo > target else []) + ([(hi + 60000, now)] if hi < now else [])
        got = 0
        for a, b in ranges:
            if b > a:
                bars = src.fetch([s], a, b, threads=1).get(s, [])
                bars_db.save(s, bars)
                got += len(bars)
        bars_db.commit()
        log.info(f"[{k}/{len(syms)}] {s}: +{got} баров")
    print_db_info(cfg)


def print_db_info(cfg):
    if not os.path.exists(cfg["history_db"]):
        print("База истории ещё не создана")
        return
    b = BarStore(cfg["history_db"])
    i = b.info()
    f = lambda t: fmt_ts(t, cfg) if t else "—"
    print(f"База: {cfg['history_db']}\n  инструментов {i['symbols']}, баров {i['bars']:,}, "
          f"с {f(i['from'])} по {f(i['to'])}, размер {i['size_mb']:.1f} МБ")


def run_export(cfg, out, since, until, symbols):
    b = BarStore(cfg["history_db"])
    os.makedirs(out, exist_ok=True)
    names = symbols or b.symbols()
    for n in names:
        df = b.ohlcv(n, since, until)
        if df.empty:
            continue
        df.insert(0, "time", pd.to_datetime(df.pop("ts"), unit="ms", utc=True).dt.strftime("%Y-%m-%d %H:%M:%S"))
        fn = n.replace(":", "_").replace("/", "-") + ".csv"
        df.to_csv(os.path.join(out, fn), index=False)
        print(f"  {fn}: {len(df)} баров")
    print(f"Готово: {out}  (прогон: python market_scanner.py replay --csv-dir {out})")


def run_finam_search(cfg, query):
    if not cfg["finam"]["secret"]:
        sys.exit("Укажите finam.secret в config.json")
    for a in Finam(cfg["finam"]["secret"]).search(query)[:50]:
        print(f"  {a.get('symbol', '?'):24s} {a.get('type', ''):12s} {a.get('name', '')}")


# ============================================================================
# Прогон по истории (CSV или синтетика)
# ============================================================================
def to_ms(idx):
    return np.asarray((idx - pd.Timestamp("1970-01-01", tz="UTC")) // pd.Timedelta(milliseconds=1), dtype=np.int64)


def make_demo_market(n=30000, seed=3):
    """Синтетический рынок: 26 монет на споте + 3 фьючерса, общий рыночный фактор.
    Заложенные закономерности (модель о них не знает):
      1) F:BTCUSDT опережает S:BTCUSDT, базис возвращается к среднему — всё время;
      2) S:LINKUSDT реагирует на рынок с опозданием на 1 бар — всё время;
      3) S:PEPEUSDT возврат к среднему (автокорреляция −0.25) — ТОЛЬКО первая половина;
      4) S:SOLUSDT опережает S:ARBUSDT на 1 бар — ТОЛЬКО вторая половина (новая связь);
      Американский рынок (акции торгуются только в сессию NY, токены и перпы — круглосуточно):
      5) токен YS:NVDAXUSDT (Bybit) с опозданием догоняет перп F:NVDAUSDT (Binance), сделки редкие;
      6) в часы сессии акция A:TSLA опережает перп F:TSLAUSDT на 1 бар;
      7) A:AAPL, F:AAPLUSDT, YS:AAPLXUSDT — без заложенных связей (проверка на ложные находки)."""
    rng = np.random.default_rng(seed)
    coins = ["BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "TRX", "TON", "LTC", "BCH",
             "NEAR", "APT", "ARB", "OP", "SUI", "PEPE", "WIF", "SHIB", "UNI", "ATOM", "FIL", "INJ", "TIA"]
    f = rng.normal(0, 7e-4, n)
    R = {}
    for c in coins:
        R[c] = rng.uniform(0.6, 1.4) * f + rng.normal(0, 1e-3, n)
    half = n // 2
    R["LINK"][1:] += 0.5 * f[:-1]
    R["ARB"][half + 1:] += 0.45 * R["SOL"][half:-1]
    pepe = R["PEPE"]
    for t in range(1, half):
        pepe[t] += -0.25 * pepe[t - 1]
    out = {f"S:{c}USDT": np.cumsum(R[c]) for c in coins}
    for c in ["ETH", "SOL"]:
        out[f"F:{c}USDT"] = out[f"S:{c}USDT"] + np.cumsum(rng.normal(0, 1e-4, n)) * 0.1 + rng.normal(0, 2e-4, n)
    lf = np.cumsum(R["BTC"])
    ls = np.empty(n)
    ls[0] = lf[0]
    for t in range(1, n):
        ls[t] = ls[t - 1] + 0.6 * (lf[t] - lf[t - 1]) + 0.25 * (lf[t - 1] - ls[t - 1]) + rng.normal(0, 3e-4)
    out["F:BTCUSDT"], out["S:BTCUSDT"] = lf, ls
    idx = pd.date_range("2026-01-05", periods=n, freq="1min", tz="UTC")
    ms_idx = to_ms(idx)
    sess = np.array([us_session(int(t)) for t in ms_idx])
    g = rng.normal(0, 4e-4, n)  # фактор фондового рынка
    for tk in ["NVDA", "TSLA", "AAPL"]:
        V = np.cumsum(0.3 * f + g + rng.normal(0, 6e-4, n))      # «справедливая» цена
        stock = V + rng.normal(0, 5e-5, n)
        perp = np.empty(n)
        perp[0] = V[0]
        k = 0.5 if tk == "TSLA" else 0.95
        for t in range(1, n):
            perp[t] = perp[t - 1] + k * (V[t] - perp[t - 1]) + rng.normal(0, 1e-4)
        tok = np.empty(n)
        tok[0] = perp[0]
        kt = 0.3 if tk == "NVDA" else 0.9
        for t in range(1, n):
            tok[t] = tok[t - 1] + kt * (perp[t - 1 if tk == "NVDA" else t] - tok[t - 1]) + rng.normal(0, 2e-4)
        out[f"A:{tk}"] = np.where(sess, stock, np.nan)  # вне сессии сделок по акции нет
        out[f"F:{tk}USDT"] = perp
        out[f"YS:{tk}XUSDT"] = np.where(rng.random(n) < 0.3, np.nan, tok)  # 30% минут без сделок
    df = pd.DataFrame({k: 100 * np.exp(v) for k, v in out.items()}, index=idx)
    df.index = ms_idx
    return df


def load_csv_dir(path):
    series = {}
    for fn in sorted(os.listdir(path)):
        if not fn.lower().endswith(".csv"):
            continue
        df = pd.read_csv(os.path.join(path, fn))
        cols = {c.lower(): c for c in df.columns}
        tcol = next((cols[c] for c in cols if any(k in c for k in ("time", "date", "begin"))), df.columns[0])
        ccol = cols.get("close") or df.select_dtypes("number").columns[-1]
        t = df[tcol]
        if pd.api.types.is_numeric_dtype(t):
            idx = pd.to_datetime(t, unit="ms" if t.iloc[0] > 1e11 else "s", utc=True)
        else:
            idx = pd.to_datetime(t, utc=True)
        name = os.path.splitext(fn)[0]
        pref = name.split("_")[0]
        if pref in ("S", "F", "YS", "YF", "A", "FN", "M") and "_" in name:  # A_AAPL.csv → A:AAPL
            name = pref + ":" + name.split("_", 1)[1]
        series[name] = pd.Series(df[ccol].astype(float).values, index=idx).groupby(level=0).last()
    df = pd.DataFrame(series).sort_index()
    df = df.reindex(pd.date_range(df.index[0], df.index[-1], freq="1min")).dropna(how="all")
    df.index = to_ms(df.index)
    return df


def run_replay(cfg, df, label):
    dd = cfg["data_dir"]
    store = Store(dd)
    engine = Engine(cfg, list(df.columns), store)
    log.info(f"Прогон «{label}»: {len(df)} баров × {df.shape[1]} инструментов")
    t0 = time.time()
    vals = df.values
    cols = list(df.columns)
    n_sig = 0
    for i, ts in enumerate(df.index):
        n_sig += len(engine.on_bar(int(ts), dict(zip(cols, vals[i])), emit=True))
    store.commit()
    engine.save(os.path.join(dd, "state.pkl"))
    log.info(f"Готово за {time.time() - t0:.0f} с")
    summarize(engine, n_sig)
    return engine


def summarize(engine, n_sig=None):
    hs = sorted(engine.hyps.values(), key=lambda h: (h.state != "active", -h.tstat()))
    act = [h for h in hs if h.state == "active"]
    print(f"\n=== Гипотезы: активных {len(act)}, на проверке {len(hs) - len(act)}, "
          f"списано за время работы {engine.retired_total} ===")
    for h in hs[:25]:
        print(f"  [{h.state:7s}] {h.describe()}\n"
              f"            сделок {h.n_trades}, прибыльных {h.winrate() * 100:.0f}%, "
              f"ср. {h.avg_bps():+.1f} б.п. net, t={h.tstat():.1f}")
    all_h = list(engine.hyps.values())
    sn = sum(h.sig_n for h in all_h)
    if sn:
        ss = sum(h.sig_sum for h in all_h)
        sw = sum(h.sig_wins for h in all_h)
        print(f"\nЕсли исполнять все сигналы (по живым гипотезам): {sn} сделок, "
              f"прибыльных {sw / sn * 100:.0f}%, в среднем {ss / sn * 1e4:+.1f} б.п. после издержек")
    if n_sig is not None:
        print(f"Всего выдано сигналов: {n_sig}")


def run_status(cfg):
    path = os.path.join(cfg["data_dir"], "scanner.db")
    if not os.path.exists(path):
        print(f"Нет данных в {path}: сканер ещё не запускался")
        return
    db = sqlite3.connect(path)
    print("=== Активные гипотезы ===")
    for r in db.execute("SELECT description, trades, winrate, avg_net_bps, t_stat FROM hypotheses "
                        "WHERE state='active' ORDER BY t_stat DESC"):
        print(f"  {r[0]}\n      сделок {r[1]}, прибыльных {r[2] * 100:.0f}%, ср. {r[3]:+.1f} б.п., t={r[4]:.1f}")
    print("\n=== Последние сигналы ===")
    for (t,) in db.execute("SELECT text FROM signals ORDER BY ts DESC LIMIT 20"):
        print("  " + t)


# ============================================================================
# ============================================================================
# Диагностический отчёт (для анализа и предложений по настройке; сам ничего не меняет)
# ============================================================================
def build_report(cfg, since_ms, until_ms):
    db = sqlite3.connect(os.path.join(cfg["data_dir"], "scanner.db"))
    q = lambda sql, *p: pd.read_sql_query(sql, db, params=p)
    f = lambda t: fmt_ts(int(t), cfg)
    R = {"period": [f(since_ms), f(until_ms)], "tz": f"UTC{cfg['display_utc_offset']:+d}"}
    L = [f"# Отчёт сканера: {R['period'][0]} — {R['period'][1]} ({R['tz']})", ""]

    tr = q("SELECT * FROM trades WHERE ts_close>=? AND ts_close<?", since_ms, until_ms)
    sg = tr[tr.was_signal == 1]

    def agg(df):
        if df.empty:
            return {"n": 0}
        return {"n": int(len(df)), "win_%": round(float((df.net_bps > 0).mean() * 100), 1),
                "avg_net_bps": round(float(df.net_bps.mean()), 2), "avg_gross_bps": round(float(df.gross_bps.mean()), 2),
                "avg_cost_bps": round(float(df.cost_bps.mean()), 2), "sum_net_%": round(float(df.net_bps.sum() / 100), 2),
                "t": round(float(df.net_bps.mean() / (df.net_bps.std(ddof=1) or np.inf) * math.sqrt(len(df))), 2)
                if len(df) > 2 else 0.0}

    def table(df, by, title, top=None, sort="sum_net_%", order=None):
        if df.empty:
            return
        rows = []
        for k, g in df.groupby(by):
            a = agg(g)
            a[by if isinstance(by, str) else "key"] = k
            rows.append(a)
        t = pd.DataFrame(rows)
        if order:
            t = t.sort_values(by, key=lambda s: s.map(order))
        elif sort == by:
            t = t.sort_values(by)
        else:
            t = t.sort_values(sort, ascending=False)
        if top:
            t = pd.concat([t.head(top), t.tail(top)]).drop_duplicates()
        R.setdefault("tables", {})[title] = t.to_dict("records")
        L.append(f"### {title}")
        cols = [by if isinstance(by, str) else "key", "n", "win_%", "avg_net_bps", "avg_gross_bps", "avg_cost_bps",
                "sum_net_%", "t"]
        L.append("| " + " | ".join(cols) + " |")
        L.append("|" + "---|" * len(cols))
        for r in t[cols].itertuples(index=False):
            L.append("| " + " | ".join(str(x) for x in r) + " |")
        L.append("")

    # 1. Итог
    R["signals"] = agg(sg)
    R["shadow_all"] = agg(tr)
    s = R["signals"]
    L += ["## 1. Итог по сигналам (бумажное исполнение)", ""]
    if s["n"]:
        eaten = s["avg_cost_bps"] / s["avg_gross_bps"] * 100 if s["avg_gross_bps"] > 0 else float("inf")
        R["cost_share_of_gross_%"] = round(eaten, 1)
        L += [f"- Сделок по сигналам: **{s['n']}**, прибыльных {s['win_%']}%, в среднем **{s['avg_net_bps']:+.2f} б.п.** "
              f"после издержек (валовая {s['avg_gross_bps']:+.2f}, издержки {s['avg_cost_bps']:.2f}), t={s['t']}",
              f"- Сумма: {s['sum_net_%']:+.2f}% (если каждую сделку делать на одинаковую сумму)",
              f"- Издержки съели {eaten:.0f}% валовой прибыли",
              f"- Все бумажные сделки, включая гипотезы на проверке: {R['shadow_all']['n']}, "
              f"в среднем {R['shadow_all'].get('avg_net_bps', 0):+.2f} б.п.", ""]
    else:
        L += ["- Сигналов за период не было", ""]

    # 2. Калибровка: ожидание vs факт
    L += ["## 2. Ожидание против факта", ""]
    if len(sg) > 20:
        ratio = sg.expected_bps.abs() / sg.cost_bps
        bins = [0, 1.5, 2, 3, 5, np.inf]
        sg = sg.assign(edge_x=pd.cut(ratio, bins, labels=["<1.5×", "1.5–2×", "2–3×", "3–5×", ">5×"]).astype(str))
        table(sg, "edge_x", "Результат по силе сигнала (ожидаемое движение / издержки)",
              order={"<1.5×": 0, "1.5–2×": 1, "2–3×": 2, "3–5×": 3, ">5×": 4})
        realized = sg.gross_bps
        exp_dir = sg.expected_bps.abs()
        k = float((realized * exp_dir).sum() / (exp_dir ** 2).sum()) if (exp_dir ** 2).sum() > 0 else 0
        R["calibration_k"] = round(k, 3)
        L += [f"Калибровка: реальное валовое движение ≈ **{k:.2f} ×** ожидаемого "
              f"(1.0 — модель честная, <0.5 — модель переоценивает силу сигналов)", ""]
    # 3. Разрезы
    if len(sg):
        L += ["## 3. Разрезы сигналов", ""]
        table(sg, "kind", "По типу закономерности")
        table(sg, "horizon", "По горизонту, мин")
        table(sg.assign(venue=sg.target.str.split(":").str[0]), "venue", "По площадке")
        table(sg.assign(hour=((sg.ts_open // 3600000 + cfg["display_utc_offset"]) % 24).astype(int)),
              "hour", f"По часу открытия ({R['tz']})", sort="hour")
        table(sg, "target", "Инструменты: лучшие и худшие", top=8)
    # 4. Переобучение и старение
    L += ["## 4. Переобучение и старение закономерностей", ""]
    if len(tr) > 20:
        per = tr.groupby("key").agg(n=("net_bps", "size"), live=("net_bps", "mean"), val=("val_net_bps", "first"))
        per = per[per.n >= 10]
        if len(per):
            med_val, med_live = float(per.val.median()), float(per.live.median())
            R["overfit"] = {"median_validation_bps": round(med_val, 2), "median_live_bps": round(med_live, 2),
                            "hypotheses": int(len(per))}
            L += [f"- На отложенной части при поиске: медиана {med_val:+.2f} б.п.; вживую: {med_live:+.2f} б.п. "
                  f"({len(per)} гипотез с ≥10 сделками)", ""]
        bins = [-1, 24, 96, 288, 10 ** 9]
        tr2 = tr.assign(age=pd.cut(tr.age_scans, bins, labels=["<1 сут", "1–4 сут", "4–12 сут", ">12 сут"]).astype(str))
        table(tr2, "age", "Все бумажные сделки по возрасту гипотезы", order={"<1 сут": 0, "1–4 сут": 1,
                                                                             "4–12 сут": 2, ">12 сут": 3})
    # 5. Жизненный цикл
    ev = q("SELECT key, event FROM events WHERE ts>=? AND ts<?", since_ms, until_ms)
    cnt = ev.event.value_counts().to_dict()
    flaps = ev[ev.event == "АКТИВИРОВАНА"].key.value_counts()
    R["lifecycle"] = {"events": cnt, "flapping_hypotheses": int((flaps >= 3).sum())}
    hyp = q("SELECT kind, state, COUNT(*) n FROM hypotheses GROUP BY kind, state")
    R["hypotheses_now"] = hyp.to_dict("records")
    L += ["## 5. Жизненный цикл", "",
          "- События: " + (", ".join(f"{k} {v}" for k, v in cnt.items()) or "нет"),
          f"- «Мигающих» гипотез (активировались ≥3 раз за период): {R['lifecycle']['flapping_hypotheses']}",
          "- Сейчас: " + (", ".join(f"{r.kind}/{r.state} {r.n}" for r in hyp.itertuples()) or "нет"), ""]
    # 6. Поиск
    sc = q("SELECT * FROM scans WHERE ts>=? AND ts<?", since_ms, until_ms)
    if len(sc):
        R["discovery"] = {k: round(float(sc[k].mean()), 1) for k in ("candidates", "kept", "new", "confirmed")}
        R["discovery"]["scans"] = int(len(sc))
        d = R["discovery"]
        L += ["## 6. Поиск закономерностей", "",
              f"- Циклов поиска {d['scans']}; в среднем за цикл: кандидатов {d['candidates']}, после удаления "
              f"двойников {d['kept']}, новых {d['new']}, подтверждено старых {d['confirmed']}", ""]
    # 7. Данные
    he = q("SELECT * FROM health WHERE ts>=? AND ts<?", since_ms, until_ms)
    if len(he):
        exp_min = (until_ms - since_ms) / 60000
        R["data"] = {"minutes_processed": int(len(he)), "minutes_expected": int(exp_min),
                     "avg_symbols": round(float(he.n_symbols.mean()), 1),
                     "avg_fresh_share_%": round(float((he.n_fresh / he.n_symbols).mean() * 100), 1),
                     "warnings": int(he.warnings.sum())}
        d = R["data"]
        L += ["## 7. Данные", "",
              f"- Обработано минут: {d['minutes_processed']} из {d['minutes_expected']}; инструментов в среднем {d['avg_symbols']}",
              f"- В среднем со свежей сделкой в минуте: {d['avg_fresh_share_%']}% инструментов",
              f"- Предупреждений (сеть, пустые ответы): {d['warnings']}", ""]
    if os.path.exists(cfg["history_db"]):
        hb = sqlite3.connect(cfg["history_db"])
        liq = pd.read_sql_query("SELECT s.name, COUNT(*) n FROM bars b JOIN symbols s ON s.id=b.sid "
                                "WHERE b.ts>=? AND b.ts<? GROUP BY s.name", hb, params=(since_ms, until_ms))
        if len(liq):
            liq["share_%"] = (liq.n / ((until_ms - since_ms) / 60000) * 100).round(1)
            low = liq.sort_values("share_%").head(10)
            R["least_liquid"] = low[["name", "share_%"]].to_dict("records")
            L += ["- Самые неликвидные (доля минут со сделками): " +
                  ", ".join(f"{r.name} {r._3}%" for r in low.itertuples()), ""]
    # 8. Параметры
    keys = ["window", "scan_every", "lookbacks", "horizons", "z_discover", "t_validate", "z_incremental",
            "max_per_target", "edge_mult", "rls_lambda", "promote_min_trades", "promote_t", "demote_window",
            "retire_min_trades", "retire_t", "max_stale_bars", "cost_per_side", "slippage", "trade_prefixes"]
    R["params"] = {k: cfg.get(k) for k in keys}
    L += ["## 8. Текущие параметры", "", "```json", json.dumps(R["params"], ensure_ascii=False), "```", ""]
    # 9. Автоматические наблюдения (подсказки для разбора, не решения)
    obs = []
    if s["n"] and s.get("avg_net_bps", 0) < 0:
        obs.append("Сигналы в сумме убыточны после издержек.")
    t_edge = R.get("tables", {}).get("Результат по силе сигнала (ожидаемое движение / издержки)", [])
    weak = [r for r in t_edge if r["edge_x"] in ("<1.5×", "1.5–2×") and r["n"] >= 20 and r["avg_net_bps"] < 0]
    if weak:
        obs.append("Слабые сигналы (ожидание < 2× издержек) в минусе — кандидат на повышение edge_mult.")
    if R.get("calibration_k", 1) < 0.5 and len(sg) > 50:
        obs.append("Модель заметно переоценивает силу сигналов (калибровка < 0.5).")
    ov = R.get("overfit")
    if ov and ov["median_validation_bps"] > 0 and ov["median_live_bps"] < ov["median_validation_bps"] / 3:
        obs.append("Вживую закономерности дают в 3+ раза меньше, чем при отборе — признак переобучения "
                   "(ужесточить z_discover / t_validate или удлинить window).")
    if R["lifecycle"]["flapping_hypotheses"] >= 5:
        obs.append("Много «мигающих» гипотез — правила включения/выключения слишком чувствительны.")
    if R.get("cost_share_of_gross_%", 0) > 70:
        obs.append("Издержки съедают больше 70% валовой прибыли — проверить спреды и вход лимитками.")
    if R.get("data", {}).get("warnings", 0) > 100:
        obs.append("Много сетевых предупреждений — проверить связь и лимиты API.")
    R["observations"] = obs
    L += ["## 9. Наблюдения (автоматически)", ""] + [f"- {o}" for o in obs or ["Явных проблем не найдено."]] + [""]
    return "\n".join(L), R


def write_report(cfg, since_ms, until_ms):
    md, data = build_report(cfg, since_ms, until_ms)
    d = os.path.join(cfg["data_dir"], "reports")
    os.makedirs(d, exist_ok=True)
    name = "report_" + fmt_ts(until_ms - 60000, cfg)[:10]
    if until_ms - since_ms > 86400000 + 60000:
        name += f"_{(until_ms - since_ms) // 86400000}d"
    with open(os.path.join(d, name + ".md"), "w", encoding="utf-8") as fh:
        fh.write(md)
    with open(os.path.join(d, name + ".json"), "w", encoding="utf-8") as fh:
        json.dump(data, fh, ensure_ascii=False, indent=1, default=str)
    log.info(f"Отчёт сохранён: {os.path.join(d, name + '.md')}")
    return os.path.join(d, name + ".md")


def local_midnight_ms(ts_ms, cfg):
    off = cfg["display_utc_offset"] * 3600000
    return (ts_ms + off) // 86400000 * 86400000 - off


def _date_ms(s, end=False):
    if not s:
        return None
    d = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int((d + timedelta(days=1 if end else 0)).timestamp() * 1000) - (60000 if end else 0)


def main():
    ap = argparse.ArgumentParser(description="Фоновый поиск линейных неэффективностей по рынку")
    ap.add_argument("mode", choices=["live", "replay", "status", "init-config", "backfill", "export",
                                     "db-info", "finam-search", "report"])
    ap.add_argument("query", nargs="?", help="для finam-search: часть тикера или названия")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--demo", action="store_true", help="replay на синтетическом рынке")
    ap.add_argument("--csv-dir", help="replay по папке CSV")
    ap.add_argument("--from-db", action="store_true", help="replay по локальной базе истории")
    ap.add_argument("--days", type=int, default=None, help="backfill: глубина истории (30); report: за сколько суток (1)")
    ap.add_argument("--replay", action="store_true", help="report: по результатам последнего replay")
    ap.add_argument("--demo-data", action="store_true", help="report: по результатам replay --demo")
    ap.add_argument("--out", default="export", help="export: папка для CSV")
    ap.add_argument("--from", dest="date_from", help="ГГГГ-ММ-ДД (UTC)")
    ap.add_argument("--to", dest="date_to", help="ГГГГ-ММ-ДД (UTC), включительно")
    ap.add_argument("--symbols", help="через запятую, напр. YF:BTCUSDT,F:BTCUSDT")
    a = ap.parse_args()

    if a.mode == "init-config":
        with open(a.config, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        print(f"Создан {a.config}")
        return
    cfg = load_config(a.config)
    syms = [s.strip() for s in a.symbols.split(",")] if a.symbols else None
    d_from, d_to = _date_ms(a.date_from), _date_ms(a.date_to, end=True)
    if a.mode == "replay":
        # прогон не должен затирать память и сигналы живого сканера
        cfg["data_dir"] = cfg["data_dir"] + ("_demo" if a.demo else "_replay")
    if a.mode == "replay" and a.demo:
        cfg["cost_per_side"] = {p: 0.0001 for p in ("S", "F", "YS", "YF", "A", "FN", "M", "X")}
        cfg["slippage"] = 0.0
        cfg["trade_prefixes"] = []  # в демо торгуем всем
    setup_logging(cfg["data_dir"])
    if a.mode == "status":
        run_status(cfg)
    elif a.mode == "db-info":
        print_db_info(cfg)
    elif a.mode == "export":
        run_export(cfg, a.out, d_from, d_to, syms)
    elif a.mode == "finam-search":
        run_finam_search(cfg, a.query or "")
    elif a.mode == "backfill":
        run_backfill(cfg, a.days or 30)
    elif a.mode == "report":
        if a.replay or a.demo_data:
            cfg["data_dir"] += "_demo" if a.demo_data else "_replay"
        if a.demo_data:  # те же условия, что и в replay --demo
            cfg["cost_per_side"] = {p: 0.0001 for p in ("S", "F", "YS", "YF", "A", "FN", "M", "X")}
            cfg["slippage"], cfg["trade_prefixes"] = 0.0, []
        db = sqlite3.connect(os.path.join(cfg["data_dir"], "scanner.db"))
        hi = db.execute("SELECT MAX(ts) FROM health").fetchone()[0]
        if not hi:
            sys.exit("Нет данных для отчёта")
        until = d_to + 60000 if d_to else hi + 60000
        since = d_from or until - (a.days or 1) * 86400000
        print(open(write_report(cfg, since, until), encoding="utf-8").read())
    elif a.mode == "live":
        try:
            run_live(cfg)
        except KeyboardInterrupt:
            log.info("Остановлено пользователем")
    elif a.demo:
        run_replay(cfg, make_demo_market(), "синтетический рынок")
    elif a.csv_dir:
        run_replay(cfg, load_csv_dir(a.csv_dir), a.csv_dir)
    elif a.from_db:
        b = BarStore(cfg["history_db"])
        df = b.closes(syms or b.symbols(), d_from, d_to)
        if df.empty:
            sys.exit("В базе нет данных за этот период")
        run_replay(cfg, df, "локальная база истории")
    else:
        ap.error("для replay укажите --demo, --csv-dir или --from-db")


if __name__ == "__main__":
    main()
