# -*- coding: utf-8 -*-
"""
MarketScanner — оконная программа для сканера линейных неэффективностей.

Окно: старт/стоп сканера, лента сигналов, таблица закономерностей, отчёты, база истории,
журнал, настройки. Значок в трее, уведомления о сигналах, автозапуск вместе с Windows.

Все файлы (config.json, scanner_data/) лежат рядом с MarketScanner.exe.
Запуск из исходников: python app.py   (нужны numpy, pandas; для трея — pystray, pillow)
"""
import json
import logging
import os
import queue
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import traceback

FROZEN = getattr(sys, "frozen", False)
BASE = os.path.dirname(sys.executable) if FROZEN else os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE)  # config.json и scanner_data — рядом с программой

import market_scanner as ms  # noqa: E402

try:
    import tkinter as tk
    from tkinter import ttk, messagebox, filedialog, scrolledtext
except ImportError:  # для проверки логики без окна
    tk = None

try:
    import pystray
    from PIL import Image, ImageDraw
except Exception:
    pystray = None

APP = "Market Scanner"
CFG = "config.json"
GUI_CFG = "gui_settings.json"
LOCK_PORT = 47321
GUI_DEFAULTS = {"autostart_scanner": False, "tray_on_close": True, "notify_signals": True,
                "minimized_start": False}


# ============================================================================
# Логика без окна (её можно проверить отдельно)
# ============================================================================
class _QueueHandler(logging.Handler):
    def __init__(self, q):
        super().__init__(logging.INFO)
        self.q = q
        self.setFormatter(logging.Formatter("%(asctime)s  %(message)s", "%H:%M:%S"))

    def emit(self, record):
        try:
            self.q.put_nowait(self.format(record))
        except Exception:
            pass


class Controller:
    def __init__(self):
        if not os.path.exists(CFG):
            with open(CFG, "w", encoding="utf-8") as f:
                json.dump(ms.DEFAULT_CONFIG, f, ensure_ascii=False, indent=2)
        self.cfg = ms.load_config(CFG)
        self.gui = dict(GUI_DEFAULTS)
        if os.path.exists(GUI_CFG):
            try:
                self.gui.update(json.load(open(GUI_CFG, encoding="utf-8")))
            except Exception:
                pass
        ms.setup_logging(self.cfg["data_dir"])
        self.logq, self.sigq = queue.Queue(), queue.Queue()
        ms.log.addHandler(_QueueHandler(self.logq))
        self.stop = threading.Event()
        self.thread = None
        self.status = "Остановлен"
        self.job = None  # фоновая задача (отчёт, загрузка истории…)
        self.job_stop = threading.Event()

    # --- сканер ---------------------------------------------------------------
    @property
    def running(self):
        return self.thread is not None and self.thread.is_alive()

    def start(self):
        if self.running:
            return
        self.cfg = ms.load_config(CFG)  # подхватить изменённые настройки
        self.stop.clear()

        def work():
            try:
                ms.run_live(self.cfg, stop=self.stop, on_signal=self.sigq.put, on_status=self._set_status)
            except Exception as e:
                ms.log.error(f"Сканер остановился с ошибкой: {e}\n{traceback.format_exc()}")
                self._set_status(f"Ошибка: {e}")
                return
            self._set_status("Остановлен")

        self._set_status("Запуск…")
        self.thread = threading.Thread(target=work, daemon=True, name="scanner")
        self.thread.start()

    def stop_scanner(self):
        if self.running:
            self._set_status("Останавливаю…")
            self.stop.set()

    def _set_status(self, s):
        self.status = s

    # --- фоновые задачи ------------------------------------------------------
    def run_job(self, name, fn, done=None):
        if self.job and self.job.is_alive():
            return False
        self.job_stop.clear()

        def work():
            ms.log.info(f"▶ {name}")
            res = None
            try:
                res = fn()
                ms.log.info(f"✔ {name}: готово")
            except SystemExit as e:
                ms.log.warning(f"{name}: {e}")
            except Exception as e:
                ms.log.error(f"{name}: ошибка {e}\n{traceback.format_exc()}")
            if done:
                done(res)

        self.job = threading.Thread(target=work, daemon=True, name="job")
        self.job.start()
        return True

    def report(self, days):
        db = os.path.join(self.cfg["data_dir"], "scanner.db")
        if not os.path.exists(db):
            raise SystemExit("Нет данных: сканер ещё не работал")
        hi = sqlite3.connect(db).execute("SELECT MAX(ts) FROM health").fetchone()[0]
        if not hi:
            raise SystemExit("Нет данных для отчёта")
        until = hi + 60000
        return ms.write_report(self.cfg, until - days * 86400000, until)

    def backfill(self, days):
        ms.run_backfill(ms.load_config(CFG), days, stop=self.job_stop)

    def export(self, out, date_from, date_to, symbols):
        ms.run_export(self.cfg, out, ms._date_ms(date_from), ms._date_ms(date_to, end=True), symbols)

    def replay_db(self, date_from, date_to):
        cfg = ms.load_config(CFG)
        cfg["data_dir"] += "_replay"
        b = ms.BarStore(cfg["history_db"])
        df = b.closes(b.symbols(), ms._date_ms(date_from), ms._date_ms(date_to, end=True))
        if df.empty:
            raise SystemExit("В базе нет данных за этот период")
        store_dir = cfg["data_dir"]
        os.makedirs(store_dir, exist_ok=True)
        ms.run_replay(cfg, df, "локальная база истории")
        hi = sqlite3.connect(os.path.join(store_dir, "scanner.db")).execute("SELECT MAX(ts) FROM health").fetchone()[0]
        return ms.write_report(cfg, int(df.index[0]), hi + 60000)

    # --- чтение данных для окна ----------------------------------------------
    def _q(self, sql, *p):
        db = os.path.join(self.cfg["data_dir"], "scanner.db")
        if not os.path.exists(db):
            return []
        con = sqlite3.connect(db, timeout=2)
        try:
            return con.execute(sql, p).fetchall()
        except sqlite3.Error:
            return []
        finally:
            con.close()

    def hypotheses(self, only_active=False):
        where = "WHERE state='active'" if only_active else ""
        return self._q(f"SELECT state, kind, description, trades, winrate, avg_net_bps, t_stat, signal_trades, "
                       f"signal_net_bps FROM hypotheses {where} ORDER BY state='active' DESC, t_stat DESC")

    def signals(self, n=300):
        return self._q("SELECT ts, time, side, legs, horizon, expected_bps, cost_bps, live_winrate, "
                       "live_avg_net_bps, text FROM signals ORDER BY ts DESC LIMIT ?", n)

    def stats(self):
        act = self._q("SELECT COUNT(*) FROM hypotheses WHERE state='active'")
        tot = self._q("SELECT COUNT(*) FROM hypotheses")
        day = self._q("SELECT COUNT(*) FROM signals WHERE ts >= ?", int(time.time() * 1000) - 86400000)
        return {"active": act[0][0] if act else 0, "total": tot[0][0] if tot else 0,
                "signals_24h": day[0][0] if day else 0}

    def db_info_text(self):
        p = self.cfg["history_db"]
        if not os.path.exists(p):
            return "База истории ещё не создана — появится после первого запуска сканера."
        i = ms.BarStore(p).info()
        f = lambda t: ms.fmt_ts(t, self.cfg) if t else "—"
        return (f"Инструментов: {i['symbols']}   Баров: {i['bars']:,}   Размер: {i['size_mb']:.1f} МБ\n"
                f"Период: {f(i['from'])} — {f(i['to'])}").replace(",", " ")

    # --- настройки -------------------------------------------------------------
    def save_cfg_fields(self, upd):
        raw = json.load(open(CFG, encoding="utf-8"))
        for path, val in upd.items():
            node = raw
            keys = path.split(".")
            for k in keys[:-1]:
                node = node.setdefault(k, {})
            node[keys[-1]] = val
        tmp = CFG + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(raw, f, ensure_ascii=False, indent=2)
        os.replace(tmp, CFG)
        self.cfg = ms.load_config(CFG)

    def save_gui(self):
        with open(GUI_CFG, "w", encoding="utf-8") as f:
            json.dump(self.gui, f, ensure_ascii=False, indent=2)


# --- автозапуск с Windows ------------------------------------------------------
RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"


def autostart_get():
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY) as k:
            winreg.QueryValueEx(k, "MarketScanner")
            return True
    except Exception:
        return False


def autostart_set(on):
    import winreg
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY, 0, winreg.KEY_SET_VALUE) as k:
        if on:
            cmd = f'"{sys.executable}" --autostart' if FROZEN else \
                f'"{sys.executable}" "{os.path.abspath(__file__)}" --autostart'
            winreg.SetValueEx(k, "MarketScanner", 0, winreg.REG_SZ, cmd)
        else:
            try:
                winreg.DeleteValue(k, "MarketScanner")
            except FileNotFoundError:
                pass


def open_path(p):
    if sys.platform.startswith("win"):
        os.startfile(p)  # noqa
    elif sys.platform == "darwin":
        subprocess.Popen(["open", p])
    else:
        subprocess.Popen(["xdg-open", p])


def _legs_action(legs_json, side):
    try:
        legs = json.loads(legs_json)
    except Exception:
        return side
    ru = {"BUY": "КУПИТЬ", "SELL": "ПРОДАТЬ"}
    if len(legs) == 1:
        return f"{'ЛОНГ' if legs[0]['side'] == 'BUY' else 'ШОРТ'} {legs[0]['symbol']}"
    return " / ".join(f"{ru.get(l['side'], l['side'])} {'' if l['ratio'] == 1 else str(l['ratio']) + '×'}{l['symbol']}"
                      for l in legs)


# ============================================================================
# Окно
# ============================================================================
if tk is not None:
    class App:
        def __init__(self, ctl, autostart=False, selftest=False):
            self.ctl = ctl
            self.selftest = selftest
            self.root = tk.Tk()
            self.root.title(APP)
            self.root.geometry("1180x720")
            self.root.minsize(900, 560)
            try:
                self.root.iconbitmap(default=os.path.join(getattr(sys, "_MEIPASS", BASE), "icon.ico"))
            except Exception:
                pass
            st = ttk.Style()
            try:
                st.theme_use("vista" if sys.platform.startswith("win") else "clam")
            except Exception:
                pass
            st.configure("Big.TButton", font=("Segoe UI", 11, "bold"), padding=(18, 8))
            st.configure("Status.TLabel", font=("Segoe UI", 11))
            st.configure("Treeview", rowheight=24)
            self.tray = None
            self._last_sig_ts = 0
            self._build()
            self.root.protocol("WM_DELETE_WINDOW", self.on_close)
            self.root.after(300, self.tick)
            self.root.after(1000, self.refresh_tables)
            if pystray is not None and not selftest:
                self._start_tray()
            if autostart or ctl.gui.get("autostart_scanner"):
                self.root.after(500, self.toggle)
            if autostart and ctl.gui.get("minimized_start") and self.tray:
                self.root.after(800, self.root.withdraw)

        # --- построение ----------------------------------------------------------
        def _build(self):
            top = ttk.Frame(self.root, padding=(10, 8))
            top.pack(fill="x")
            self.btn = ttk.Button(top, text="▶  Старт", style="Big.TButton", command=self.toggle)
            self.btn.pack(side="left")
            self.lamp = tk.Canvas(top, width=18, height=18, highlightthickness=0)
            self.lamp.pack(side="left", padx=(14, 6))
            self.lamp_id = self.lamp.create_oval(2, 2, 16, 16, fill="#9aa0a6", outline="")
            self.status_var = tk.StringVar(value="Остановлен")
            ttk.Label(top, textvariable=self.status_var, style="Status.TLabel").pack(side="left")
            self.stats_var = tk.StringVar(value="")
            ttk.Label(top, textvariable=self.stats_var, style="Status.TLabel").pack(side="right")

            nb = ttk.Notebook(self.root)
            nb.pack(fill="both", expand=True, padx=8, pady=(0, 8))
            self.nb = nb
            self._tab_signals(nb)
            self._tab_hyps(nb)
            self._tab_reports(nb)
            self._tab_history(nb)
            self._tab_log(nb)
            self._tab_settings(nb)

        def _tree(self, parent, cols, widths, stretch_col):
            fr = ttk.Frame(parent)
            fr.pack(fill="both", expand=True)
            tv = ttk.Treeview(fr, columns=[c for c, _ in cols], show="headings")
            for (c, title), w in zip(cols, widths):
                tv.heading(c, text=title)
                tv.column(c, width=w, stretch=(c == stretch_col), anchor="w" if c == stretch_col else "center")
            sb = ttk.Scrollbar(fr, orient="vertical", command=tv.yview)
            tv.configure(yscrollcommand=sb.set)
            tv.pack(side="left", fill="both", expand=True)
            sb.pack(side="right", fill="y")
            tv.tag_configure("long", foreground="#0b7a2a")
            tv.tag_configure("short", foreground="#b3261e")
            tv.tag_configure("testing", foreground="#6b7280")
            return tv

        def _tab_signals(self, nb):
            f = ttk.Frame(nb, padding=6)
            nb.add(f, text="Сигналы")
            self.sig_tv = self._tree(f, [("time", "Время"), ("act", "Действие"), ("h", "Горизонт, мин"),
                                         ("exp", "Ожид., б.п."), ("cost", "Издержки, б.п."), ("win", "Прибыльных"),
                                         ("avg", "Ср. результат, б.п."), ("why", "Причина")],
                                     [120, 260, 90, 90, 100, 90, 120, 500], "why")
            self.sig_tv.bind("<Double-1>", lambda e: self._show_row_text(self.sig_tv, self._sig_text))
            self._sig_text = {}

        def _tab_hyps(self, nb):
            f = ttk.Frame(nb, padding=6)
            nb.add(f, text="Закономерности")
            bar = ttk.Frame(f)
            bar.pack(fill="x", pady=(0, 6))
            self.only_active = tk.BooleanVar(value=False)
            ttk.Checkbutton(bar, text="Только активные (дают сигналы)", variable=self.only_active,
                            command=self.refresh_tables).pack(side="left")
            ttk.Button(bar, text="Обновить", command=self.refresh_tables).pack(side="right")
            self.hyp_tv = self._tree(f, [("state", "Статус"), ("kind", "Тип"), ("desc", "Закономерность"),
                                         ("n", "Сделок"), ("win", "Прибыльных"), ("avg", "Ср., б.п."), ("t", "t"),
                                         ("sn", "По сигналам"), ("ssum", "Итог сигналов, б.п.")],
                                     [80, 50, 560, 70, 85, 70, 50, 90, 120], "desc")

        def _tab_reports(self, nb):
            f = ttk.Frame(nb, padding=6)
            nb.add(f, text="Отчёты")
            bar = ttk.Frame(f)
            bar.pack(fill="x", pady=(0, 6))
            ttk.Button(bar, text="Отчёт за сутки", command=lambda: self.make_report(1)).pack(side="left")
            ttk.Button(bar, text="За неделю", command=lambda: self.make_report(7)).pack(side="left", padx=6)
            ttk.Button(bar, text="За месяц", command=lambda: self.make_report(30)).pack(side="left")
            ttk.Button(bar, text="Открыть папку отчётов",
                       command=lambda: self._open_dir(os.path.join(self.ctl.cfg["data_dir"], "reports"))) \
                .pack(side="right")
            ttk.Button(bar, text="Последний отчёт", command=self.show_last_report).pack(side="right", padx=6)
            self.rep_txt = scrolledtext.ScrolledText(f, wrap="word", font=("Consolas", 10))
            self.rep_txt.pack(fill="both", expand=True)
            self.rep_txt.insert("1.0", "Отчёт строится автоматически каждый день в полночь.\n"
                                       "Здесь можно построить его вручную за нужный период.")

        def _tab_history(self, nb):
            f = ttk.Frame(nb, padding=10)
            nb.add(f, text="База истории")
            self.db_var = tk.StringVar(value="")
            ttk.Label(f, textvariable=self.db_var, font=("Segoe UI", 10)).pack(anchor="w", pady=(0, 10))

            g1 = ttk.LabelFrame(f, text="Загрузить глубокую историю (по текущему списку инструментов)", padding=8)
            g1.pack(fill="x", pady=4)
            self.bf_days = tk.IntVar(value=30)
            ttk.Label(g1, text="Дней:").pack(side="left")
            ttk.Spinbox(g1, from_=1, to=1500, textvariable=self.bf_days, width=7).pack(side="left", padx=6)
            ttk.Button(g1, text="Загрузить", command=self.do_backfill).pack(side="left", padx=6)
            ttk.Button(g1, text="Прервать", command=self.ctl.job_stop.set).pack(side="left")

            g2 = ttk.LabelFrame(f, text="Выгрузить в CSV (дата ГГГГ-ММ-ДД, пусто — всё)", padding=8)
            g2.pack(fill="x", pady=4)
            self.ex_from, self.ex_to, self.ex_syms = tk.StringVar(), tk.StringVar(), tk.StringVar()
            for lbl, var, w in (("С:", self.ex_from, 12), ("По:", self.ex_to, 12),
                                ("Инструменты (через запятую):", self.ex_syms, 40)):
                ttk.Label(g2, text=lbl).pack(side="left", padx=(6, 2))
                ttk.Entry(g2, textvariable=var, width=w).pack(side="left")
            ttk.Button(g2, text="Выгрузить…", command=self.do_export).pack(side="left", padx=8)

            g3 = ttk.LabelFrame(f, text="Бэктест: прогнать сканер по базе (память живого сканера не трогается)",
                                padding=8)
            g3.pack(fill="x", pady=4)
            self.rp_from, self.rp_to = tk.StringVar(), tk.StringVar()
            for lbl, var in (("С:", self.rp_from), ("По:", self.rp_to)):
                ttk.Label(g3, text=lbl).pack(side="left", padx=(6, 2))
                ttk.Entry(g3, textvariable=var, width=12).pack(side="left")
            ttk.Button(g3, text="Запустить бэктест", command=self.do_replay).pack(side="left", padx=8)
            ttk.Label(f, text="Ход выполнения — на вкладке «Журнал».", foreground="#6b7280").pack(anchor="w", pady=8)

        def _tab_log(self, nb):
            f = ttk.Frame(nb, padding=6)
            nb.add(f, text="Журнал")
            self.log_txt = scrolledtext.ScrolledText(f, wrap="none", font=("Consolas", 9), state="disabled")
            self.log_txt.pack(fill="both", expand=True)
            bar = ttk.Frame(f)
            bar.pack(fill="x", pady=(6, 0))
            ttk.Button(bar, text="Открыть папку данных",
                       command=lambda: self._open_dir(self.ctl.cfg["data_dir"])).pack(side="right")

        def _tab_settings(self, nb):
            f = ttk.Frame(nb, padding=10)
            nb.add(f, text="Настройки")
            c = self.ctl.cfg
            self.fields = {}

            def row(parent, label, path, value, width=50, show=None):
                r = ttk.Frame(parent)
                r.pack(fill="x", pady=2)
                ttk.Label(r, text=label, width=34).pack(side="left")
                v = tk.StringVar(value=value)
                ttk.Entry(r, textvariable=v, width=width, show=show).pack(side="left")
                self.fields[path] = v

            def check(parent, label, path, value):
                v = tk.BooleanVar(value=value)
                ttk.Checkbutton(parent, text=label, variable=v).pack(anchor="w", pady=2)
                self.fields[path] = v

            g = ttk.LabelFrame(f, text="Мосбиржа через Финам", padding=8)
            g.pack(fill="x", pady=4)
            check(g, "Включить", "finam.enabled", c["finam"]["enabled"])
            row(g, "Ключ Trade API (secret)", "finam.secret", c["finam"]["secret"], show="•")
            row(g, "Тикеры через запятую (SBER@MISX, SiZ6@RTSX)", "finam.symbols", ", ".join(c["finam"]["symbols"]))

            g = ttk.LabelFrame(f, text="Акции США через Alpaca", padding=8)
            g.pack(fill="x", pady=4)
            al = c["us_market"]["alpaca"]
            check(g, "Включить", "us_market.alpaca.enabled", al["enabled"])
            row(g, "API Key ID", "us_market.alpaca.key_id", al["key_id"])
            row(g, "Secret Key", "us_market.alpaca.secret_key", al["secret_key"], show="•")

            g = ttk.LabelFrame(f, text="Комиссии Bybit на одну сторону, %", padding=8)
            g.pack(fill="x", pady=4)
            row(g, "Спот (YS)", "cost_per_side.YS", f"{c['cost_per_side']['YS'] * 100:g}", width=10)
            row(g, "Перпы (YF)", "cost_per_side.YF", f"{c['cost_per_side']['YF'] * 100:g}", width=10)
            row(g, "Часовой пояс отчётов (UTC+…)", "display_utc_offset", str(c["display_utc_offset"]), width=10)

            g = ttk.LabelFrame(f, text="Программа", padding=8)
            g.pack(fill="x", pady=4)
            self.v_auto_scanner = tk.BooleanVar(value=self.ctl.gui["autostart_scanner"])
            self.v_tray = tk.BooleanVar(value=self.ctl.gui["tray_on_close"])
            self.v_notify = tk.BooleanVar(value=self.ctl.gui["notify_signals"])
            self.v_min = tk.BooleanVar(value=self.ctl.gui["minimized_start"])
            self.v_win = tk.BooleanVar(value=autostart_get())
            ttk.Checkbutton(g, text="Запускать сканер сразу при открытии программы",
                            variable=self.v_auto_scanner).pack(anchor="w")
            win_cb = ttk.Checkbutton(g, text="Запускать программу вместе с Windows", variable=self.v_win)
            win_cb.pack(anchor="w")
            if not sys.platform.startswith("win"):
                win_cb.state(["disabled"])
            ttk.Checkbutton(g, text="При автозапуске — сразу свернуть в трей", variable=self.v_min).pack(anchor="w")
            ttk.Checkbutton(g, text="Крестик сворачивает в трей (сканер продолжает работать)",
                            variable=self.v_tray).pack(anchor="w")
            ttk.Checkbutton(g, text="Всплывающие уведомления о сигналах", variable=self.v_notify).pack(anchor="w")

            bar = ttk.Frame(f)
            bar.pack(fill="x", pady=10)
            ttk.Button(bar, text="Сохранить", style="Big.TButton", command=self.save_settings).pack(side="left")
            ttk.Button(bar, text="Все настройки (config.json)…", command=lambda: open_path(os.path.abspath(CFG))) \
                .pack(side="left", padx=10)
            ttk.Label(f, text="Новые настройки сканера применяются после «Стоп» → «Старт».",
                      foreground="#6b7280").pack(anchor="w")

        # --- действия ----------------------------------------------------------
        def toggle(self):
            if self.ctl.running:
                self.ctl.stop_scanner()
            else:
                self.ctl.start()

        def save_settings(self):
            try:
                upd = {}
                for path, v in self.fields.items():
                    val = v.get()
                    if path == "finam.symbols":
                        val = [s.strip() for s in val.split(",") if s.strip()]
                    elif path.startswith("cost_per_side."):
                        val = float(str(val).replace(",", ".")) / 100
                    elif path == "display_utc_offset":
                        val = int(val)
                    upd[path] = val
                self.ctl.save_cfg_fields(upd)
                self.ctl.gui.update(autostart_scanner=self.v_auto_scanner.get(), tray_on_close=self.v_tray.get(),
                                    notify_signals=self.v_notify.get(), minimized_start=self.v_min.get())
                self.ctl.save_gui()
                if sys.platform.startswith("win") and self.v_win.get() != autostart_get():
                    autostart_set(self.v_win.get())
                messagebox.showinfo(APP, "Сохранено. Настройки сканера применятся после «Стоп» → «Старт».")
            except Exception as e:
                messagebox.showerror(APP, f"Не удалось сохранить: {e}")

        def make_report(self, days):
            def done(path):
                if path:
                    self.root.after(0, lambda: self._show_file(path))
            if not self.ctl.run_job(f"Отчёт за {days} сут.", lambda: self.ctl.report(days), done):
                messagebox.showinfo(APP, "Дождитесь окончания предыдущей задачи (см. «Журнал»).")

        def show_last_report(self):
            d = os.path.join(self.ctl.cfg["data_dir"], "reports")
            files = sorted((os.path.join(d, x) for x in os.listdir(d) if x.endswith(".md")),
                           key=os.path.getmtime) if os.path.isdir(d) else []
            if files:
                self._show_file(files[-1])
            else:
                messagebox.showinfo(APP, "Отчётов пока нет.")

        def _show_file(self, path):
            self.rep_txt.delete("1.0", "end")
            self.rep_txt.insert("1.0", open(path, encoding="utf-8").read())
            self.nb.select(2)

        def do_backfill(self):
            days = int(self.bf_days.get())
            if not self.ctl.run_job(f"Загрузка истории за {days} дн.", lambda: self.ctl.backfill(days),
                                    lambda r: self.root.after(0, self.refresh_db)):
                messagebox.showinfo(APP, "Дождитесь окончания предыдущей задачи.")
            else:
                self.nb.select(4)

        def do_export(self):
            out = filedialog.askdirectory(title="Куда сохранить CSV")
            if not out:
                return
            syms = [s.strip() for s in self.ex_syms.get().split(",") if s.strip()] or None
            self.ctl.run_job("Выгрузка в CSV",
                             lambda: self.ctl.export(out, self.ex_from.get().strip() or None,
                                                     self.ex_to.get().strip() or None, syms),
                             lambda r: self.root.after(0, lambda: open_path(out)))

        def do_replay(self):
            def done(path):
                if path:
                    self.root.after(0, lambda: self._show_file(path))
            if self.ctl.run_job("Бэктест по базе",
                                lambda: self.ctl.replay_db(self.rp_from.get().strip() or None,
                                                           self.rp_to.get().strip() or None), done):
                self.nb.select(4)

        def _open_dir(self, p):
            os.makedirs(p, exist_ok=True)
            open_path(os.path.abspath(p))

        def _show_row_text(self, tv, store):
            sel = tv.selection()
            if sel and sel[0] in store:
                messagebox.showinfo(APP, store[sel[0]])

        # --- периодическое обновление -----------------------------------------
        def tick(self):
            run = self.ctl.running
            self.btn.configure(text="■  Стоп" if run else "▶  Старт")
            st = self.ctl.status if run or self.ctl.status.startswith("Ошибка") else "Остановлен"
            self.status_var.set(st)
            color = "#1e8e3e" if run and st.startswith("Работает") else "#f9ab00" if run else \
                "#d93025" if st.startswith("Ошибка") else "#9aa0a6"
            self.lamp.itemconfigure(self.lamp_id, fill=color)
            # журнал
            lines = []
            while not self.ctl.logq.empty() and len(lines) < 500:
                lines.append(self.ctl.logq.get_nowait())
            if lines:
                self.log_txt.configure(state="normal")
                self.log_txt.insert("end", "\n".join(lines) + "\n")
                n = int(self.log_txt.index("end-1c").split(".")[0])
                if n > 5000:
                    self.log_txt.delete("1.0", f"{n - 5000}.0")
                self.log_txt.see("end")
                self.log_txt.configure(state="disabled")
            # новые сигналы
            new = []
            while not self.ctl.sigq.empty():
                new.append(self.ctl.sigq.get_nowait())
            if new:
                self.refresh_signals()
                if self.tray and self.ctl.gui.get("notify_signals"):
                    s = new[-1]
                    more = f" (+ещё {len(new) - 1})" if len(new) > 1 else ""
                    try:
                        self.tray.notify(s["text"].split("|")[0].strip()[18:] + more, "Новый сигнал")
                    except Exception:
                        pass
            self.root.after(500, self.tick)

        def refresh_signals(self):
            tv = self.sig_tv
            tv.delete(*tv.get_children())
            self._sig_text = {}
            for ts, tm, side, legs, h, exp, cost, win, avg, text in self.ctl.signals():
                why = text.split("|")[-1].strip()
                iid = tv.insert("", "end", values=(tm, _legs_action(legs, side), h, f"{exp:+.1f}", f"{cost:.1f}",
                                                    f"{(win or 0) * 100:.0f}%", f"{avg:+.1f}", why),
                                tags=("long" if side == "LONG" else "short",))
                self._sig_text[iid] = text

        def refresh_tables(self):
            self.refresh_signals()
            tv = self.hyp_tv
            tv.delete(*tv.get_children())
            ru = {"active": "активна", "testing": "проверка"}
            for st, kind, desc, n, win, avg, t, sn, ssum in self.ctl.hypotheses(self.only_active.get()):
                tv.insert("", "end", values=(ru.get(st, st), kind, desc, n, f"{(win or 0) * 100:.0f}%",
                                             f"{avg:+.1f}", f"{t:.1f}", sn, f"{ssum:+.0f}"),
                          tags=() if st == "active" else ("testing",))
            s = self.ctl.stats()
            self.stats_var.set(f"Активных закономерностей: {s['active']} из {s['total']}   "
                               f"Сигналов за 24 ч: {s['signals_24h']}")
            self.refresh_db()
            self.root.after(30000, self.refresh_tables)

        def refresh_db(self):
            try:
                self.db_var.set(self.ctl.db_info_text())
            except Exception as e:
                self.db_var.set(f"База истории: {e}")

        # --- трей и выход ------------------------------------------------------
        def _start_tray(self):
            img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
            d = ImageDraw.Draw(img)
            d.ellipse((4, 4, 60, 60), fill=(26, 115, 232, 255))
            d.line((14, 44, 26, 30, 36, 38, 50, 18), fill="white", width=6)
            menu = pystray.Menu(
                pystray.MenuItem("Открыть", lambda: self.root.after(0, self.show), default=True),
                pystray.MenuItem("Старт / Стоп", lambda: self.root.after(0, self.toggle)),
                pystray.MenuItem("Выход", lambda: self.root.after(0, self.quit)))
            self.tray = pystray.Icon("MarketScanner", img, APP, menu)
            threading.Thread(target=self.tray.run, daemon=True).start()

        def show(self):
            self.root.deiconify()
            self.root.lift()
            self.root.focus_force()

        def on_close(self):
            if self.tray and self.ctl.gui.get("tray_on_close"):
                self.root.withdraw()
                try:
                    self.tray.notify("Сканер продолжает работать. Открыть — значок в трее.", APP)
                except Exception:
                    pass
            else:
                self.quit()

        def quit(self):
            if self.ctl.running and not self.selftest:
                if not messagebox.askyesno(APP, "Сканер работает. Остановить и выйти?"):
                    return
            self.ctl.stop_scanner()
            self.ctl.job_stop.set()
            if self.ctl.thread:
                self.ctl.thread.join(timeout=15)
            if self.tray:
                try:
                    self.tray.stop()
                except Exception:
                    pass
            self.root.destroy()

        def run(self):
            self.root.mainloop()


# ============================================================================
# Самопроверка (запускается при сборке на GitHub): окно + короткий прогон движка
# ============================================================================
def selftest():
    import tempfile
    tmp = tempfile.mkdtemp(prefix="msc_selftest_")
    os.chdir(tmp)
    result = {"ok": False, "err": ""}
    ctl = Controller()
    app = App(ctl, selftest=True) if tk is not None else None

    def work():
        try:
            cfg = ms.load_config(CFG)
            cfg.update(window=2000, scan_every=60, trade_prefixes=[], data_dir=os.path.join(tmp, "demo"))
            cfg["cost_per_side"] = {p: 0.0001 for p in ("S", "F", "YS", "YF", "A", "FN", "M", "X")}
            cfg["slippage"] = 0.0
            os.makedirs(cfg["data_dir"], exist_ok=True)
            ms.run_replay(cfg, ms.make_demo_market(n=6000), "самопроверка")
            db = sqlite3.connect(os.path.join(cfg["data_dir"], "scanner.db"))
            hi = db.execute("SELECT MAX(ts) FROM health").fetchone()[0]
            ms.write_report(cfg, hi - 86400000, hi + 60000)
            result["ok"] = True
        except Exception:
            result["err"] = traceback.format_exc()

    th = threading.Thread(target=work, daemon=True)
    th.start()
    if app:
        def poll():
            if th.is_alive():
                app.root.after(500, poll)
            else:
                app.refresh_tables()
                app.root.after(1500, app.quit)
        app.root.after(500, poll)
        app.run()
    th.join(600)
    with open(os.path.join(BASE, "selftest_result.txt"), "w", encoding="utf-8") as f:
        f.write("OK" if result["ok"] else "FAIL\n" + result["err"])
    sys.exit(0 if result["ok"] else 1)


def main():
    if "--selftest" in sys.argv:
        selftest()
    if tk is None:
        sys.exit("Нет tkinter: запустите сканер из консоли — python market_scanner.py live")
    # одна копия программы: две копии писали бы в одну базу
    lock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        lock.bind(("127.0.0.1", LOCK_PORT))
    except OSError:
        r = tk.Tk()
        r.withdraw()
        messagebox.showinfo(APP, "Программа уже запущена — посмотрите значок в трее возле часов.")
        sys.exit(0)
    ctl = Controller()
    App(ctl, autostart="--autostart" in sys.argv).run()
    lock.close()


if __name__ == "__main__":
    main()
