# -*- coding: utf-8 -*-
"""
نظام البصمة - تطبيق ويندوز مستقل (واجهة + خدمة خلفية + أيقونة بجانب الساعة).
يضم وسيط fp_bridge كاملًا بداخله: لا يحتاج بايثون ولا أي تثبيت.
"""
import contextlib
import ctypes
import io
import json
import os
import re
import sys
import threading
import time
import tkinter as tk
from http.server import ThreadingHTTPServer
from tkinter import messagebox, ttk

import fp_bridge as fb

APP = "نظام البصمة - الوسيط"
RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"
RUN_NAME = "FPBridge"
CFG_FILE = os.path.join(fb.HERE, "fp_config.json")
KEY_FILE = os.path.join(fb.HERE, "fp_key.txt")
FONT = ("Segoe UI", 10)

fb.setup_log()
log = fb.log


# ---------------------------------------------------------------- إعدادات
def read_cfg_file():
    try:
        with open(CFG_FILE, encoding="utf-8-sig") as f:
            return json.load(f)
    except Exception:
        return {}


def save_cfg_file(d):
    fb._write_json(CFG_FILE, d)
    fb.CFG.update(d)


def interval_min():
    return max(1, int(read_cfg_file().get("interval", 15) or 15))


def has_key():
    try:
        return bool(fb.get_key())
    except Exception:
        return False


# ------------------------------------------------------- التشغيل مع ويندوز
def autostart_get():
    if os.name != "nt":
        return False
    import winreg
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY) as k:
            winreg.QueryValueEx(k, RUN_NAME)
            return True
    except OSError:
        return False


def autostart_set(on):
    if os.name != "nt":
        return
    import winreg
    with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY, 0, winreg.KEY_SET_VALUE) as k:
        if on:
            winreg.SetValueEx(k, RUN_NAME, 0, winreg.REG_SZ, '"%s" --minimized' % sys.executable)
        else:
            try:
                winreg.DeleteValue(k, RUN_NAME)
            except OSError:
                pass


def single_instance():
    if os.name != "nt":
        return True
    ctypes.windll.kernel32.CreateMutexW(None, False, "Global\\FPBridgeApp_v1")
    return ctypes.windll.kernel32.GetLastError() != 183  # ERROR_ALREADY_EXISTS


# ------------------------------------------------------------- الخلفية
class Worker:
    """يسحب من الأجهزة كل N دقيقة، ويخدم زر «سحب من الأجهزة» في الصفحة عبر 127.0.0.1"""

    def __init__(self):
        self.wake = threading.Event()
        self.stop = threading.Event()
        self.busy = threading.Lock()
        self.state = "idle"  # idle | running
        self.msg = ""
        self.srv = None

    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        try:
            self.srv = ThreadingHTTPServer(("127.0.0.1", int(fb.CFG["port"])), fb.Handler)
            log.info("الخادم المحلي للصفحة يعمل على 127.0.0.1:%s", fb.CFG["port"])
            self.srv.serve_forever()
        except OSError as e:
            log.error("تعذر تشغيل الخادم المحلي على المنفذ %s: %s", fb.CFG["port"], e)

    def _loop(self):
        time.sleep(3)
        while not self.stop.is_set():
            self.sync()
            self.wake.wait(interval_min() * 60)
            self.wake.clear()

    def sync(self, force=False):
        if not self.busy.acquire(blocking=False):
            return
        self.state, self.msg = "running", ""
        try:
            fb.run_sync(force=force, wait=0)
        except Exception as e:
            self.msg = str(e)
            log.error("خطأ في المزامنة: %s", e)
            try:
                fb.write_status([], False, str(e))
            except Exception:
                pass
        finally:
            self.state = "idle"
            self.busy.release()

    def now(self):
        self.wake.set()

    def selftest(self):
        buf = io.StringIO()
        with self.busy:
            with contextlib.redirect_stdout(buf):
                try:
                    fb.run_test()
                except Exception as e:
                    print("خطأ:", e)
        return buf.getvalue()


# ------------------------------------------------------------- الواجهة
class UI(tk.Tk):
    def __init__(self, worker, minimized):
        super().__init__()
        self.w = worker
        self.tray = None
        self.title(APP)
        self.geometry("760x540")
        self.minsize(640, 460)
        self.option_add("*Font", FONT)
        st = ttk.Style(self)
        try:
            st.theme_use("vista" if os.name == "nt" else "clam")
        except tk.TclError:
            pass
        st.configure("Treeview", rowheight=26, font=FONT)
        st.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        self.protocol("WM_DELETE_WINDOW", self.hide)

        top = ttk.Frame(self, padding=(14, 12))
        top.pack(fill="x")
        self.lbl_state = tk.Label(top, text="…", font=("Segoe UI", 16, "bold"), anchor="e")
        self.lbl_state.pack(fill="x")
        self.lbl_info = tk.Label(top, text="", fg="#555", anchor="e", justify="right")
        self.lbl_info.pack(fill="x", pady=(2, 0))

        mid = ttk.Frame(self, padding=(14, 0))
        mid.pack(fill="both", expand=True)
        cols = ("last", "read", "state", "ip", "name")
        self.tv = ttk.Treeview(mid, columns=cols, show="headings", height=8)
        for c, t, wd in (("name", "الجهاز", 130), ("ip", "العنوان", 130), ("state", "الحالة", 150), ("read", "المقروء", 70), ("last", "آخر حركة", 150)):
            self.tv.heading(c, text=t, anchor="e")
            self.tv.column(c, width=wd, anchor="e")
        self.tv.pack(fill="both", expand=True)
        self.lbl_err = tk.Label(mid, text="", fg="#b00020", wraplength=720, anchor="e", justify="right")
        self.lbl_err.pack(fill="x", pady=(6, 0))

        bar = ttk.Frame(self, padding=14)
        bar.pack(fill="x")
        for txt, cmd in (("الإعدادات", self.settings), ("السجل", self.show_log), ("فحص شامل", self.test), ("سحب الآن", self.pull)):
            ttk.Button(bar, text=txt, command=cmd).pack(side="right", padx=4)
        tk.Label(bar, text="الأجهزة تُعرَّف من الموقع (تبويب أجهزة البصمة، طريقة «وسيط بايثون»)", fg="#777").pack(side="left")

        self.make_tray()
        self.refresh()
        if minimized and self.tray:
            self.withdraw()
        if not has_key():
            self.after(400, lambda: self.settings(first=True))

    # ----- tray
    def make_tray(self):
        try:
            import pystray
            from PIL import Image, ImageDraw
        except Exception:
            return
        img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
        d = ImageDraw.Draw(img)
        d.ellipse((2, 2, 62, 62), fill=(24, 90, 157, 255))
        for r in (10, 17, 24):
            d.arc((32 - r, 32 - r, 32 + r, 32 + r), 200, 340, fill="white", width=3)
            d.arc((32 - r, 32 - r, 32 + r, 32 + r), 20, 160, fill="white", width=3)
        menu = pystray.Menu(
            pystray.MenuItem("فتح", lambda: self.after(0, self.show), default=True),
            pystray.MenuItem("سحب الآن", lambda: self.after(0, self.pull)),
            pystray.MenuItem("خروج", lambda: self.after(0, self.quit_app)),
        )
        self.tray = pystray.Icon("FPBridge", img, APP, menu)
        self.tray.run_detached()

    def show(self):
        self.deiconify()
        self.lift()
        self.focus_force()

    def hide(self):
        if self.tray:
            self.withdraw()
        else:
            self.iconify()

    def quit_app(self):
        self.w.stop.set()
        if self.tray:
            self.tray.stop()
        self.destroy()
        os._exit(0)

    # ----- حالة
    def refresh(self):
        try:
            s = fb.read_status()
            pend = len(fb.outbox_load())
            self.tv.delete(*self.tv.get_children())
            seen = set()
            for i in s.get("items", []):
                seen.add(i["name"])
                dev = next((d for d in fb._read_json(fb.DEV_CACHE, []) if str(d.get("name")) == i["name"]), {})
                ok = i.get("ok")
                self.tv.insert("", "end", values=(i.get("last") or "—", i.get("read", 0), "متصل ✓" if ok else "فشل: %s" % (i.get("kind") or ""), dev.get("ip", ""), i["name"]))
            for d in fb._read_json(fb.DEV_CACHE, []):
                if str(d.get("name")) not in seen:
                    self.tv.insert("", "end", values=("—", 0, "بانتظار أول سحب", d.get("ip", ""), d.get("name")))
            if not has_key():
                self.lbl_state.config(text="⚠ لم يُضبط مفتاح الجسر بعد", fg="#b45f06")
            elif self.w.state == "running":
                self.lbl_state.config(text="⏳ جارٍ السحب من الأجهزة…", fg="#185a9d")
            elif int(s.get("fails", 0)) > 0 and s.get("last_error"):
                self.lbl_state.config(text="⚠ آخر محاولة فشلت (ستُعاد تلقائيًا)", fg="#b45f06")
            elif s.get("last_success"):
                self.lbl_state.config(text="● يعمل بشكل سليم", fg="#1b7f3b")
            else:
                self.lbl_state.config(text="● يعمل - بانتظار أول سحب", fg="#185a9d")
            self.lbl_info.config(text="آخر نجاح: %s   |   حركات بانتظار الإرسال: %d   |   السحب كل %d دقيقة" % (s.get("last_success", "—"), pend, interval_min()))
            self.lbl_err.config(text=(s.get("last_error") or self.w.msg or "")[:400])
        except Exception as e:
            self.lbl_err.config(text=str(e)[:300])
        self.after(3000, self.refresh)

    def pull(self):
        if not has_key():
            return self.settings(first=True)
        self.w.now()
        self.lbl_state.config(text="⏳ جارٍ السحب من الأجهزة…", fg="#185a9d")

    # ----- نوافذ
    def settings(self, first=False):
        win = tk.Toplevel(self)
        win.title("الإعدادات")
        win.geometry("560x330")
        win.transient(self)
        win.grab_set()
        cfg = read_cfg_file()
        f = ttk.Frame(win, padding=16)
        f.pack(fill="both", expand=True)
        if first:
            tk.Label(f, text="خطوة واحدة فقط: الصق مفتاح الجسر (من Apps Script ← تشغيل showKey ← السجل).", fg="#b45f06", wraplength=500, justify="right", anchor="e").pack(fill="x", pady=(0, 8))

        def row(label, init, show=None):
            tk.Label(f, text=label, anchor="e").pack(fill="x")
            e = ttk.Entry(f, justify="left", show=show)
            e.insert(0, init)
            e.pack(fill="x", pady=(0, 8))
            return e

        try:
            cur = fb.get_key()
        except Exception:
            cur = ""
        e_key = row("مفتاح الجسر (64 حرفًا)", cur)
        e_url = row("رابط النظام (Apps Script)", str(fb.CFG.get("url", fb.DEFAULT_URL)))
        e_int = row("السحب التلقائي كل (دقيقة)", str(cfg.get("interval", 15)))
        v = tk.BooleanVar(value=autostart_get() if os.path.exists(CFG_FILE) else True)
        ttk.Checkbutton(f, text="تشغيل التطبيق تلقائيًا عند تشغيل ويندوز", variable=v).pack(anchor="e")

        def save():
            k = e_key.get().strip()
            if not re.fullmatch(r"[0-9a-fA-F]{64}", k):
                return messagebox.showerror(APP, "المفتاح يجب أن يكون 64 حرفًا (أرقام وحروف a-f).", parent=win)
            u = e_url.get().strip()
            if not u.startswith("https://"):
                return messagebox.showerror(APP, "رابط النظام يجب أن يبدأ بـ https://", parent=win)
            try:
                m = max(1, int(e_int.get()))
            except ValueError:
                return messagebox.showerror(APP, "الفاصل الزمني رقم صحيح بالدقائق.", parent=win)
            with open(KEY_FILE, "w", encoding="utf-8") as fh:
                fh.write(k)
            c = read_cfg_file()
            c.update({"url": u, "interval": m})
            save_cfg_file(c)
            try:
                autostart_set(v.get())
            except Exception as ex:
                log.warning("تعذر ضبط التشغيل التلقائي: %s", ex)
            win.destroy()
            self.pull()

        ttk.Button(f, text="حفظ", command=save).pack(side="left", pady=8)

    def show_log(self):
        win = tk.Toplevel(self)
        win.title("السجل")
        win.geometry("760x420")
        t = tk.Text(win, wrap="word", font=("Consolas", 9))
        t.pack(fill="both", expand=True)
        try:
            with open(os.path.join(fb.HERE, "fp_bridge.log"), encoding="utf-8") as fh:
                t.insert("1.0", "".join(fh.readlines()[-300:]))
        except Exception:
            t.insert("1.0", "لا يوجد سجل بعد.")
        t.see("end")

    def test(self):
        win = tk.Toplevel(self)
        win.title("الفحص الشامل")
        win.geometry("760x420")
        t = tk.Text(win, wrap="word", font=("Segoe UI", 10))
        t.pack(fill="both", expand=True)
        t.insert("1.0", "جارٍ الفحص… (قد يستغرق دقيقة)")

        def go():
            out = self.w.selftest()
            self.after(0, lambda: (t.delete("1.0", "end"), t.insert("1.0", out)))

        threading.Thread(target=go, daemon=True).start()


def main():
    if not single_instance():
        try:
            ctypes.windll.user32.MessageBoxW(0, "التطبيق يعمل مسبقًا (ابحث عن أيقونته بجانب الساعة).", APP, 0x40)
        except Exception:
            pass
        return 0
    w = Worker()
    ui = UI(w, "--minimized" in sys.argv)
    w.start()
    ui.mainloop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
