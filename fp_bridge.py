#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fp_bridge.py v6.1 : وسيط بايثون بين أجهزة البصمة (FingerTec / ZK) عبر الشبكة المحلية LAN ونظام البصمة (Google Apps Script)

طريقة الاتصال الوحيدة: وسيط بايثون + كيبل/شبكة LAN. (أُلغيت ملفات USB ومجلد الاستيراد.)

يسحب من كل الأجهزة المفعّلة (FaceID-1 ثم 2 ثم 3 ...) ويرسل كل حركة مع اسم جهازها؛ والنظام يدمج بصمات الأجهزة
في سطر واحد لكل موظف في كل يوم (اتحاد/تقاطع) ويربط الرقم بالموظف حسب (الجهاز + الرقم) من «ربط البصمات».
القراءة من الجهاز (LAN) منفصلة عن الإرسال للخادم: الحركات تُحفظ أولًا في fp_outbox.json ثم تُرسل عند توفر الإنترنت
(لا تضيع حركة ولا تتكرر).

التثبيت (مرة واحدة):
    pip install pyzk requests
    ضع مفتاح الجسر (من الدالة showKey في السكربت) في ملف fp_key.txt بجانب هذا الملف (64 حرفًا).

الأوامر:
    python fp_bridge.py --test               فحص كل شيء (الخادم، المفتاح، الأجهزة، ساعة الجهاز) — شغّله أولًا
    python fp_bridge.py                      سحب من كل الأجهزة ثم إرسال (للجدولة كل 15-30 دقيقة)
    python fp_bridge.py --collect            قراءة الأجهزة وحفظها محليًا فقط (بلا إنترنت)
    python fp_bridge.py --flush [--wait N]   إرسال المحفوظ محليًا (ينتظر الإنترنت حتى N دقيقة)
    python fp_bridge.py --sync --wait 10     اسحب الآن ثم انتظر حتى 10 دقائق لتفصل الكيبل ويعود الإنترنت
    python fp_bridge.py --serve              زر «سحب من الأجهزة» في الصفحة (يتحقق من توكن الجلسة)
    python fp_bridge.py --loop [دقائق]       خدمة مستمرة (الافتراضي 15 دقيقة) وتكتب fp_status.json
    python fp_bridge.py --install-task [د]   (ويندوز) تشغيل عند الدخول + حارس كل 30 دقيقة
    python fp_bridge.py --remove-task        إزالة المهمتين
    python fp_bridge.py --status             آخر تشغيل/نجاح/خطأ وحالة كل جهاز والحركات المنتظرة

الأجهزة: تُقرأ من تبويب «أجهزة البصمة» في النظام (طريقة «وسيط بايثون») وتُحفظ نسخة محلية (fp_devices_cache.json)،
وإلا فمن "devices" في fp_config.json. الاسم يجب أن يطابق اسم الجهاز في النظام تمامًا.

fp_config.json (اختياري):
{
  "devices": [{"name": "FaceID-1", "ip": "192.168.1.201", "port": 4370},
              {"name": "FaceID-2", "ip": "192.168.1.202", "port": 4370},
              {"name": "FaceID-3", "ip": "192.168.1.203", "port": 4370}],
  "comm_keys": {"FaceID-1": 0, "FaceID-2": 0, "FaceID-3": 0},
  "allowed_origins": [],
  "port": 8765
}

رمز الخروج 0 = نجاح، 1 = فشل قراءة جهاز أو إرسال. السجل الكامل في fp_bridge.log
"""
import argparse
import hashlib
import json
import logging
import os
import sys
import threading
import socket
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from types import SimpleNamespace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import RotatingFileHandler

import requests

try:
    from zk import ZK
except ImportError:  # يسمح باستيراد الملفات حتى بدون pyzk
    ZK = None

VERSION = "6.1"
MODE = "sync"
def _data_dir():
    """عند التشغيل كملف exe تُحفظ كل الملفات في %APPDATA%\\FPBridge (مجلد ثابت يعمل مهما كان مكان الـexe)"""
    if getattr(sys, "frozen", False):
        base = os.environ.get("APPDATA") or os.path.expanduser("~")
        d = os.path.join(base, "FPBridge")
        os.makedirs(d, exist_ok=True)
        return d
    return os.path.dirname(os.path.abspath(__file__))


HERE = _data_dir()
STATUS = os.path.join(HERE, "fp_status.json")
DEFAULT_URL = "https://script.google.com/macros/s/AKfycbyP3vOjqG_Pj4_Q6lkYTJpa6eSP8JvYhNdpe5R6f-FtIR7m1lJzaTidTPMtIq4Sm9g0ZQ/exec"
STATE = os.path.join(HERE, "fp_state.json")
OUTBOX = os.path.join(HERE, "fp_outbox.json")
DEV_CACHE = os.path.join(HERE, "fp_devices_cache.json")
PROTO_FILE = os.path.join(HERE, "fp_proto.json")  # البروتوكول الذي نجح مع كل جهاز (TCP/UDP) لتفادي انتظار المهلة
LOG_CACHE = os.path.join(HERE, "fp_logs_%s.json")  # نسخة محلية من سجل كل جهاز؛ تُستعمل إن لم يتغير عدد السجلات
log = logging.getLogger("fp")


# ====================== الإعدادات والسجل ======================
def load_cfg():
    cfg = {
        "url": DEFAULT_URL,
        "port": 8765,
        "devices": [{"name": "FaceID-1", "ip": "192.168.1.201", "port": 4370}],
        "comm_keys": {},
        "allowed_origins": [],
        "min_year": 2020,
    }
    p = os.path.join(HERE, "fp_config.json")
    if os.path.exists(p):
        with open(p, encoding="utf-8-sig") as f:
            cfg.update(json.load(f))
    if not str(cfg["url"]).startswith("https://"):
        print("تحذير أمني: رابط الخادم ليس https")
    if os.environ.get("FP_URL"):
        cfg["url"] = os.environ["FP_URL"]
    if os.environ.get("FP_PORT"):
        cfg["port"] = int(os.environ["FP_PORT"])
    return cfg


CFG = load_cfg()


def setup_log():
    if log.handlers:
        return
    log.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(os.path.join(HERE, "fp_bridge.log"), maxBytes=1000000, backupCount=5, encoding="utf-8")
    hs = [fh]
    if sys.stderr is not None:
        hs.append(logging.StreamHandler())
    for h in hs:
        h.setFormatter(fmt)
        log.addHandler(h)


def get_key():
    k = os.environ.get("FP_KEY")
    if k:
        return k.strip()
    p = os.path.join(HERE, "fp_key.txt")
    if os.path.exists(p):
        with open(p, encoding="utf-8-sig") as f:
            k = f.read().strip()
        if k:
            return k
    try:
        from embedded_key import KEY
        if KEY:
            return KEY
    except ImportError:
        pass
    raise RuntimeError("مفتاح الجسر غير مضبوط: افتح «الإعدادات» والصق المفتاح (يظهر عند تشغيل showKey في Apps Script)")


def load_state():
    try:
        with open(STATE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    tmp = STATE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False)
    os.replace(tmp, STATE)  # كتابة ذرّية: لا يتلف الملف إن انقطعت الكهرباء


def _read_json(path, default):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _write_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False)
    os.replace(tmp, path)


def online(timeout=6):
    """فحص سريع: هل يصل هذا الكمبيوتر لخادم النظام الآن؟ (يتغير عند توصيل/فصل كيبل الجهاز)"""
    try:
        requests.get(CFG["url"], timeout=timeout)
        return True
    except Exception:
        return False


def wait_online(minutes):
    """ينتظر عودة الإنترنت (مثلًا بعد فصل كيبل الجهاز) حتى minutes دقيقة. يرجع True إن توفر."""
    if online():
        return True
    end = time.time() + max(0, minutes) * 60
    if minutes > 0:
        log.info("لا يوجد إنترنت الآن. أنتظر عودته حتى %d دقيقة (افصل كيبل الجهاز أو أعد توصيل الإنترنت)…", minutes)
    while time.time() < end:
        time.sleep(5)
        if online(4):
            log.info("عاد الإنترنت")
            return True
    return False


def outbox_load():
    return _read_json(OUTBOX, {"rows": []}).get("rows", [])


def outbox_save(rows):
    _write_json(OUTBOX, {"rows": rows})


def outbox_add(rows):
    """يضيف حركات لصندوق الإرسال المحلي دون تكرار (الرقم+الوقت+الجهاز). يرجع عدد المضاف فعلًا"""
    cur = outbox_load()
    have = set((r[0], r[1], r[2]) for r in cur)
    n = 0
    for r in rows:
        k = (r[0], r[1], r[2])
        if k not in have:
            have.add(k)
            cur.append([r[0], r[1], r[2]])
            n += 1
    if n:
        outbox_save(cur)
    return n


# ====================== الاتصال بالخادم ======================
def call(action, retries=3, **data):
    body = json.dumps(dict(data, action=action)).encode("utf-8")
    last = None
    for i in range(retries):
        try:
            j = requests.post(CFG["url"], data=body, timeout=120).json()
        except Exception as e:
            last = e
            time.sleep(3 * (i + 1))
            continue
        if j.get("ok"):
            return j
        last = RuntimeError(j.get("err") or "فشل")
        if j.get("err") in ("unauthorized", "forbidden"):
            if j.get("err") == "unauthorized":
                last = RuntimeError("unauthorized: مفتاح fp_key.txt لا يطابق مفتاح السكريبت الحالي؛ شغّل showKey في Apps Script وانسخ المفتاح إلى fp_key.txt")
            break  # لا فائدة من التكرار
        time.sleep(3 * (i + 1))
    raise last


def post_rows(rows, key):
    """يرجع إحصاءات الإرسال: added جديدة، dups مكررة مريبة سُجّلت بالخادم، resent معادة، bad مرفوضة"""
    t = {"added": 0, "dups": 0, "resent": 0, "bad": 0}
    for i in range(0, len(rows), 2000):
        j = call("punches", k=key, rows=rows[i:i + 2000])
        t["added"] += j.get("added", 0)
        t["dups"] += j.get("dups", 0)
        t["resent"] += j.get("resent", 0)
        t["bad"] += j.get("skipped", 0)
    return t


def classify(e):
    """تصنيف الخطأ لتحديد سياسة إعادة المحاولة وتسهيل التشخيص في سجل الأجهزة"""
    s = str(e).lower()
    if isinstance(e, requests.exceptions.Timeout) or "timed out" in s or "timeout" in s:
        return "timeout"
    if "unauthorized" in s or "forbidden" in s:
        return "auth"
    if "pyzk" in s or "مكتبة" in str(e):
        return "lib"
    if "تعذر الاتصال" in str(e) or "refused" in s or "unreachable" in s or "no route" in s:
        return "unreachable"
    if isinstance(e, requests.exceptions.RequestException):
        return "server_net"
    if "تاريخ" in str(e) or "غير صالح" in str(e):
        return "bad_data"
    return "other"


def cooldown(fails):
    """بعد 3 إخفاقات متتالية لجهاز: ننتظر 5 ثم 10 ثم 20… حتى 120 دقيقة بدل إغراق جهاز/شبكة معطلة"""
    return 0 if fails < 3 else min(120, 5 * (2 ** (fails - 3)))


def read_status():
    try:
        with open(STATUS, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def write_status(items, ok, err=""):
    s = read_status()
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    s.update({"version": VERSION, "mode": MODE, "pid": os.getpid(), "host": socket.gethostname(), "last_run": now, "items": items})
    if ok:
        s["last_success"] = now
        s["fails"] = 0
        s["last_error"] = ""
    else:
        s["fails"] = int(s.get("fails", 0)) + 1
        s["last_error"] = err or "; ".join("%s: %s" % (i["name"], i.get("err", "")) for i in items if not i.get("ok"))
    tmp = STATUS + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(s, f, ensure_ascii=False)
    os.replace(tmp, STATUS)
    return s


def report(items, key, hb=None):  # حالة آخر سحب لكل جهاز + نبضة الخدمة تظهر في تبويب الأجهزة بالنظام
    try:
        s = read_status()
        beat = hb or {"v": VERSION, "host": socket.gethostname(), "mode": MODE, "err": s.get("last_error", ""), "okts": s.get("last_success", ""), "fails": s.get("fails", 0)}
        call("bridge", retries=1, k=key, items=items, hb=beat)
    except Exception as e:
        log.debug("تعذر إرسال الحالة: %s", e)


# ====================== الطريقة 1: الأجهزة عبر الشبكة (ZK) ======================
DEV_LOCKS = {}
_LK = threading.Lock()


def dev_lock(name):  # قفل لكل جهاز: تُقرأ الأجهزة الثلاثة بالتوازي دون تداخل على الجهاز نفسه
    with _LK:
        return DEV_LOCKS.setdefault(name, threading.Lock())


def get_devices(auth):
    """auth = {"k": مفتاح} للمجدول، أو {"t": توكن} لزر الصفحة. بدون إنترنت: النسخة المحفوظة ثم fp_config.json"""
    devs = None
    try:
        r = call("devices", retries=1, **auth)
        devs = [d for d in r.get("devices", []) if "بايثون" in str(d.get("method", ""))]
        if devs:
            _write_json(DEV_CACHE, devs)
    except Exception as e:
        log.warning("تعذر جلب قائمة الأجهزة من الخادم (%s): أستخدم النسخة المحفوظة/fp_config.json", str(e)[:80])
    if not devs:
        devs = _read_json(DEV_CACHE, []) or [dict(d) for d in CFG["devices"]]
    return [{
        "name": str(d.get("name")),
        "ip": str(d.get("ip")),
        "port": int(d.get("port") or 4370),
        "password": int(CFG["comm_keys"].get(str(d.get("name")), 0) or 0),
        "driver": str(d.get("driver") or "zk"),
    } for d in devs]


def connect(dev):
    if ZK is None:
        raise RuntimeError("مكتبة pyzk غير مثبتة: pip install pyzk")
    last = None
    proto = _read_json(PROTO_FILE, {})
    first = bool(proto.get(dev["name"], False))  # آخر بروتوكول نجح (False = TCP)
    for udp in (first, not first):  # بعض الأجهزة تردّ على UDP فقط
        for attempt in range(3):  # الجهاز يقبل اتصالًا واحدًا: إن كان مشغولًا أو بقيت جلسة قديمة يقطع الاتصال (WinError 10054) فنعيد بعد مهلة
            try:
                c = ZK(dev["ip"], port=dev["port"], timeout=12, password=dev["password"],
                       force_udp=udp, ommit_ping=True).connect()
                if proto.get(dev["name"]) != udp:
                    proto[dev["name"]] = udp
                    try:
                        _write_json(PROTO_FILE, proto)
                    except Exception:
                        pass
                return c
            except Exception as e:
                last = e
                m = str(e).lower()
                if attempt < 2 and ("10054" in m or "forcibly" in m or "reset" in m or "busy" in m or "unauthenticated" in m):
                    time.sleep(3 + attempt * 3)
                    continue
                break
    raise RuntimeError("تعذر الاتصال بـ %s:%s (%s). تحقق من IP والشبكة والمنفذ وكلمة الاتصال Comm Key، وأن لا برنامج آخر (ZKTime/نسخة ثانية من الوسيط) متصل بالجهاز في الوقت نفسه" % (dev["ip"], dev["port"], last))


DRIVERS = {}  # اسم المشغّل -> دالة (dev) تعيد قائمة حركات بحقول user_id و timestamp. أضف مشغّلًا لأي نوع جهاز آخر هنا


LAST_READ = {}  # آخر قراءة لكل جهاز: {"cache": هل من النسخة المحلية؟, "n": عدد السجلات}


def read_device(dev, full=False):
    drv = DRIVERS.get(str(dev.get("driver") or "zk"))
    if drv:
        return drv(dev)
    return read_device_zk(dev, full)


def _cache_rows(name):
    c = _read_json(LOG_CACHE % "".join(ch if ch.isalnum() else "_" for ch in name), None)
    return c if isinstance(c, dict) and isinstance(c.get("rows"), list) else None


def read_device_zk(dev, full=False):
    """الجهاز لا يدعم التصفية بالتاريخ فيُنزَّل كل سجله. لذلك نقرأ عدد السجلات أولًا (سريع)، فإن لم يتغير منذ
    آخر قراءة (وخلال 24 ساعة) نستعمل النسخة المحلية فورًا بدل إعادة التنزيل. المحاولة الأولى أو بعد بصمات جديدة تنزّل كل شيء."""
    with dev_lock(dev["name"]):
        conn = connect(dev)
        try:
            try:
                conn.disable_device()
            except Exception:
                pass
            n = None
            try:
                conn.read_sizes()
                n = int(conn.records)
            except Exception:
                pass
            path = LOG_CACHE % "".join(ch if ch.isalnum() else "_" for ch in dev["name"])
            c = None if full else _cache_rows(dev["name"])
            if c and n is not None and c.get("n") == n and time.time() - float(c.get("at", 0)) < 86400:
                LAST_READ[dev["name"]] = {"cache": True, "n": n}
                return [SimpleNamespace(user_id=r[0], timestamp=datetime.fromisoformat(r[1])) for r in c["rows"]]
            att = conn.get_attendance() or []
            LAST_READ[dev["name"]] = {"cache": False, "n": len(att)}
            if n is not None:
                try:
                    _write_json(path, {"n": n, "at": time.time(),
                                       "rows": [[str(a.user_id), a.timestamp.isoformat()] for a in att if isinstance(a.timestamp, datetime)]})
                except Exception:
                    pass
            return att
        finally:  # لا نمسح سجلات الجهاز أبدًا؛ نعيد تفعيله دائمًا
            for fn in (conn.enable_device, conn.disconnect):
                try:
                    fn()
                except Exception:
                    pass


def row_of(a, dev):
    ts = a.timestamp
    if not isinstance(ts, datetime) or ts.year < CFG["min_year"] or ts > datetime.now() + timedelta(days=1):
        return None  # تاريخ غير منطقي (ساعة الجهاز خاطئة)
    uid = str(a.user_id).strip().lstrip("0") or "0"
    return [uid, ts.strftime("%Y-%m-%d %H:%M"), dev["name"]]


def day_before(last):
    if not last:
        return ""
    return (datetime.strptime(last[:10], "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")


def collect_devices(key, force=False):
    """يقرأ كل جهاز عبر الشبكة المحلية فقط، ويحفظ حركاته الجديدة في صندوق الإرسال. لا يحتاج إنترنت."""
    state = load_state()
    # عند تغيير رابط السكريبت (جدول/نظام جديد) تُصفَّر مؤشرات السحب وكاش الأجهزة تلقائيًا
    tgt = hashlib.sha1(CFG["url"].encode("utf-8")).hexdigest()[:12]
    if state.get("_target") != tgt:
        stale = [k for k in state if k not in ("_meta", "_target")]
        if stale:
            log.warning("تغيّر رابط السكريبت: إعادة سحب كامل سجل الأجهزة للنظام الجديد")
        for k in stale:
            del state[k]
        state["_meta"] = {}
        state["_target"] = tgt
        try:
            os.remove(DEV_CACHE)
        except OSError:
            pass
        save_state(state)
    meta = state.setdefault("_meta", {})
    items, failed = [], False
    devs = get_devices({"k": key})
    if not devs:
        log.warning("لا توجد أجهزة مفعّلة")
    for dev in devs:
        name = dev["name"]
        m = meta.setdefault(name, {"fails": 0, "next": 0})
        if not force and m.get("next", 0) > time.time():
            log.info("%s: تخطٍّ مؤقت بعد إخفاقات متكررة (إعادة المحاولة بعد %d دقيقة)", name, int((m["next"] - time.time()) / 60) + 1)
            continue
        t0 = time.time()
        try:
            since = day_before(state.get(name, ""))  # تداخل يوم واحد للأمان، والخادم يزيل المكرر
            rows, bad, rd = [], 0, 0
            for a in read_device(dev):
                rd += 1
                r = row_of(a, dev)
                if r is None:
                    bad += 1
                elif r[1][:10] >= since:
                    rows.append(r)
            queued = outbox_add(rows)  # نقطة التقدم تُحفظ بعد أن صارت الحركات في الملف المحلي الآمن
            if rows:
                state[name] = max(state.get(name, ""), max(r[1] for r in rows))
            m["fails"], m["next"] = 0, 0
            save_state(state)
            ms = int((time.time() - t0) * 1000)
            log.info("%s: قُرئت %d، حُفظت محليًا %d (بانتظار الإرسال)، تاريخ غير صالح %d (%d ms)", name, rd, queued, bad, ms)
            items.append({"name": name, "ok": True, "n": 0, "dups": 0, "bad": bad, "read": rd, "ms": ms, "last": state.get(name, "")})
        except Exception as e:
            failed = True
            kind = classify(e)
            m["fails"] = int(m.get("fails", 0)) + 1
            m["next"] = time.time() + cooldown(m["fails"]) * 60
            save_state(state)
            log.error("%s: فشل [%s]: %s", name, kind, e)
            items.append({"name": name, "ok": False, "n": 0, "kind": kind, "err": str(e), "ms": int((time.time() - t0) * 1000), "last": state.get(name, "")})
    return items, failed


def flush_outbox(key, items=None, wait=0):
    """يرسل الحركات المحفوظة محليًا للخادم (مجمّعة حسب الجهاز). عند عدم توفر إنترنت تبقى محفوظة. يرجع (تمّ؟, عدد المتبقي)"""
    rows = outbox_load()
    if not rows:
        return True, 0
    if not wait_online(wait):
        log.warning("لا يوجد إنترنت: %d حركة محفوظة محليًا وستُرسل تلقائيًا عند عودته (fp_outbox.json)", len(rows))
        return False, len(rows)
    groups = {}
    for r in rows:
        groups.setdefault(r[2], []).append(r)
    names = list(groups)
    left, ok_all = [], True
    for idx, dev in enumerate(names):
        lst = groups[dev]
        t = {"added": 0, "dups": 0, "resent": 0, "bad": 0}
        sent = 0
        try:
            for i in range(0, len(lst), 2000):
                chunk = lst[i:i + 2000]
                s = post_rows(chunk, key)
                for k in t:
                    t[k] += s[k]
                sent += len(chunk)
        except Exception as e:
            ok_all = False
            log.error("%s: تعذر الإرسال (%s). أُبقيت %d حركة محليًا", dev, e, len(lst) - sent)
            left.extend(lst[sent:])
            for it in (items or []):
                if it["name"] == dev:
                    it["err"] = ((it.get("err") or "") + " تعذر الإرسال: %s" % e).strip()
        else:
            log.info("%s: أُرسلت %d، جديدة %d، موجودة مسبقًا %d، مكررة مريبة %d، مرفوضة %d", dev, len(lst), t["added"], t["resent"], t["dups"], t["bad"])
        for it in (items or []):
            if it["name"] == dev:
                it["n"], it["dups"], it["bad"] = t["added"], t["dups"], it.get("bad", 0) + t["bad"]
        rest = [r for d2 in names[idx + 1:] for r in groups[d2]]
        outbox_save(left + rest)  # حفظ تقدم آمن بعد كل جهاز
    return ok_all, len(left)


def run_sync(force=False, collect=True, flush=True, wait=0):
    key = get_key()
    items, failed = [], False
    if collect:
        items, failed = collect_devices(key, force)
    sent_ok, left = True, len(outbox_load())
    if flush:
        sent_ok, left = flush_outbox(key, items, wait)
    s = write_status(items, not failed)
    s2 = read_status()
    s2["pending"] = left
    _write_json(STATUS, s2)
    if items and online(4):
        report(items, key)
    return 1 if failed else 0


def run_loop(minutes):
    global MODE
    MODE = "loop"
    minutes = max(1, int(minutes or 15))
    log.info("خدمة مستمرة: سحب كل %d دقيقة (Ctrl+C للإيقاف). حالتها في %s", minutes, STATUS)
    while True:
        try:
            run_sync()
        except Exception as e:  # أي خطأ غير متوقع لا يوقف الخدمة
            log.error("خطأ في الدورة: %s", e)
            try:
                write_status([], False, str(e))
                report([], get_key(), {"v": VERSION, "host": socket.gethostname(), "mode": MODE, "err": str(e), "okts": read_status().get("last_success", ""), "fails": read_status().get("fails", 0)})
            except Exception:
                pass
        time.sleep(minutes * 60)


def pid_alive(pid):
    try:
        pid = int(pid)
        if os.name == "nt":
            out = subprocess.run(["tasklist", "/FI", "PID eq %d" % pid], capture_output=True, text=True).stdout
            return str(pid) in out
        os.kill(pid, 0)
        return True
    except Exception:
        return False


def run_ensure(minutes):
    """حارس: إن لم تُحدَّث الحالة منذ 3 دورات ولم تكن العملية حيّة يعيد تشغيل الخدمة"""
    s = read_status()
    try:
        age = time.time() - time.mktime(time.strptime(s.get("last_run", ""), "%Y-%m-%d %H:%M:%S"))
    except Exception:
        age = 1e9
    if age < minutes * 60 * 3 or pid_alive(s.get("pid")):
        return 0
    py = sys.executable
    args = [py, "--minimized"] if getattr(sys, "frozen", False) else [py, os.path.abspath(__file__), "--loop", str(minutes)]
    kw = {"creationflags": 0x00000008 | 0x00000200} if os.name == "nt" else {"start_new_session": True}
    subprocess.Popen(args, cwd=HERE, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, **kw)
    log.warning("الخدمة متوقفة (آخر تحديث قبل %d دقيقة): أُعيد تشغيلها", age // 60)
    return 0


def install_task(minutes):
    if os.name != "nt":
        print("التثبيت التلقائي لويندوز فقط. على لينكس/ماك استخدم systemd أو cron مع: python fp_bridge.py --loop %d" % minutes)
        return 1
    py = sys.executable
    pw = py.replace("python.exe", "pythonw.exe")
    py = pw if os.path.exists(pw) else py
    me = os.path.abspath(__file__)
    cmds = [["schtasks", "/Create", "/SC", "ONLOGON", "/TN", "FPBridge", "/TR", '"%s" "%s" --loop %d' % (py, me, minutes), "/RL", "LIMITED", "/F"],
            ["schtasks", "/Create", "/SC", "MINUTE", "/MO", "30", "/TN", "FPBridgeWatchdog", "/TR", '"%s" "%s" --ensure %d' % (py, me, minutes), "/RL", "LIMITED", "/F"]]
    rc = 0
    for c in cmds:
        r = subprocess.run(c, capture_output=True, text=True)
        print((r.stdout or r.stderr).strip())
        rc |= r.returncode
    if rc == 0:
        print("تم. الخدمة تبدأ عند دخول ويندوز، والحارس يعيد تشغيلها إن توقفت. للتشغيل الآن: python fp_bridge.py --loop %d" % minutes)
    return rc


def remove_task():
    rc = 0
    for n in ("FPBridge", "FPBridgeWatchdog"):
        r = subprocess.run(["schtasks", "/Delete", "/TN", n, "/F"], capture_output=True, text=True)
        print((r.stdout or r.stderr).strip())
        rc |= r.returncode
    return rc


def show_status():
    s = read_status()
    if not s:
        print("لا توجد حالة بعد (لم يعمل الوسيط على هذا الكمبيوتر).")
        return 1
    print("الإصدار %s | الوضع %s | الجهاز %s | PID %s (%s)" % (s.get("version"), s.get("mode"), s.get("host"), s.get("pid"), "حيّة" if pid_alive(s.get("pid")) else "غير نشطة"))
    print("آخر تشغيل: %s | آخر نجاح: %s | إخفاقات متتالية: %s" % (s.get("last_run"), s.get("last_success", "—"), s.get("fails", 0)))
    print("حركات محفوظة محليًا بانتظار الإرسال (fp_outbox.json): %d" % len(outbox_load()))
    if s.get("last_error"):
        print("آخر خطأ:", s["last_error"])
    for i in s.get("items", []):
        print(" -", i["name"], "نجاح" if i.get("ok") else "فشل [%s] %s" % (i.get("kind"), i.get("err")), "| مقروء", i.get("read", 0), "جديد", i.get("n", 0), "مكرر", i.get("dups", 0), "رفض", i.get("bad", 0), "|", i.get("ms", 0), "ms")
    return 0


# ====================== الطريقة 3: خادم محلي لزر «سحب من الجهاز» ======================
TOK = {}  # توكن مقبول -> وقت انتهاء التخزين المؤقت (60 ث)


def token_ok(t):
    t = str(t or "")
    if not t:
        return False
    now = time.time()
    if TOK.get(t, 0) > now:
        return True
    for k in [k for k, v in TOK.items() if v <= now]:
        TOK.pop(k, None)
    try:
        r = call("check", retries=1, t=t)
    except Exception:
        return False
    if r.get("role") in ("admin", "hr", "fp"):
        TOK[t] = now + 60
        return True
    return False


class Handler(BaseHTTPRequestHandler):
    def _cors(self):
        allowed = CFG.get("allowed_origins") or []
        org = self.headers.get("Origin", "")
        self.send_header("Access-Control-Allow-Origin", "*" if not allowed else (org if org in allowed else "null"))
        self.send_header("Vary", "Origin")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Private-Network", "true")

    def _send(self, code, obj):
        b = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self._cors()
        self.end_headers()
        self.wfile.write(b)

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Max-Age", "600")
        self._cors()
        self.end_headers()

    def do_GET(self):
        self._send(200, {"ok": True, "v": VERSION, "msg": "fp bridge ready", "devices": [d["name"] for d in CFG["devices"]]})

    def do_POST(self):
        try:
            d = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
        except Exception:
            return self._send(400, {"ok": False, "err": "bad request"})
        if not token_ok(d.get("t")):
            return self._send(403, {"ok": False, "err": "unauthorized"})
        frm, to = str(d.get("from") or ""), str(d.get("to") or "")  # YYYY-MM-DD
        dates = {str(x)[:10] for x in (d.get("dates") or []) if x}
        full = bool(d.get("full"))

        def keep(day):
            if dates:
                return day in dates
            return (not frm or day >= frm) and (not to or day <= to)

        def one(dev):
            t0 = time.time()
            out = []
            try:
                for a in read_device(dev, full):
                    r = row_of(a, dev)
                    if r and keep(r[1][:10]):
                        out.append(r)
                lr = LAST_READ.get(dev["name"], {})
                return dev["name"], out, None, {"name": dev["name"], "ms": int((time.time() - t0) * 1000), "n": len(out), "read": lr.get("n"), "cache": bool(lr.get("cache"))}
            except Exception as e:
                return dev["name"], out, "%s: %s" % (dev["name"], e), {"name": dev["name"], "ms": int((time.time() - t0) * 1000), "n": 0, "err": str(e)}

        devs = get_devices({"t": d["t"]})
        rows, errs, info = [], [], []
        if devs:
            with ThreadPoolExecutor(max_workers=len(devs)) as ex:  # الأجهزة بالتوازي: الزمن = أبطأ جهاز لا مجموعها
                for name, r, er, inf in ex.map(one, devs):
                    rows += r
                    info.append(inf)
                    if er:
                        errs.append(er)
        rows.sort(key=lambda r: (r[1], r[2]))
        log.info("سحب من الأجهزة بطلب من الصفحة: %d حركة %s %s", len(rows), errs or "", [(i["name"], i["ms"]) for i in info])
        self._send(200, {"ok": bool(rows) or not errs, "rows": rows, "errors": errs, "info": info})

    def log_message(self, *a):
        pass


def run_serve():
    port = int(CFG["port"])
    try:
        srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    except OSError as e:
        log.error("تعذر تشغيل الخادم على المنفذ %d (%s). غيّر port في fp_config.json", port, e)
        return 1
    log.info("الوسيط يعمل على http://127.0.0.1:%d (اتركه مفتوحًا، Ctrl+C للإيقاف)", port)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


# ====================== فحص شامل ======================
def run_test():
    ok = True

    def line(good, msg):
        print(("[ نجح ] " if good else "[ فشل ] ") + msg)

    key = None
    try:
        key = get_key()
        line(True, "ملف المفتاح موجود")
    except Exception as e:
        line(False, str(e))
        ok = False
    server_ok = True
    try:
        call("info", retries=1)
        line(True, "الخادم (Apps Script) يردّ")
    except Exception as e:
        server_ok = False
        line(False, "الخادم لا يردّ: %s. إن كان الكيبل موصولًا فالإنترنت مقطوع عادةً؛ سأكمل فحص الأجهزة محليًا" % e)
    pend = len(outbox_load())
    print("          حركات محفوظة محليًا بانتظار الإرسال: %d" % pend)
    if key and server_ok:
        try:
            call("devices", retries=1, k=key)
            line(True, "مفتاح الجسر صحيح")
        except Exception as e:
            line(False, "مفتاح الجسر مرفوض (%s). انسخه من showKey وضعه في fp_key.txt" % e)
            ok = False
    devs = get_devices({"k": key}) if (key and server_ok) else (_read_json(DEV_CACHE, []) or [dict(d) for d in CFG["devices"]])
    devs = [dict(d, name=str(d.get("name")), ip=str(d.get("ip")), port=int(d.get("port") or 4370), password=int(CFG["comm_keys"].get(str(d.get("name")), 0) or 0)) for d in devs]
    if not devs:
        line(False, "لا توجد أجهزة مفعّلة: فعّل جهازًا بطريقة «وسيط بايثون» من تبويب الأجهزة")
        ok = False
    for dev in devs:
        tag = "%s (%s:%s)" % (dev["name"], dev["ip"], dev["port"])
        try:
            conn = connect(dev)
        except Exception as e:
            line(False, "%s: %s" % (tag, e))
            ok = False
            continue
        try:
            def safe(fn):
                try:
                    return fn()
                except Exception:
                    return "?"
            n = len(conn.get_attendance() or [])
            clock = safe(conn.get_time)
            line(True, "%s: اتصال ناجح | الطراز %s | الإصدار %s | الرقم التسلسلي %s | حركات مخزنة %d" % (
                tag, safe(conn.get_device_name), safe(conn.get_firmware_version), safe(conn.get_serialnumber), n))
            if isinstance(clock, datetime):
                drift = abs((datetime.now() - clock).total_seconds()) / 60
                line(drift < 5, "ساعة الجهاز %s (فرق %.0f دقيقة عن هذا الكمبيوتر)%s" % (
                    clock.strftime("%Y-%m-%d %H:%M"), drift, "" if drift < 5 else "، اضبط الساعة من قائمة الجهاز" ))
                ok = ok and drift < 5
        finally:
            try:
                conn.disconnect()
            except Exception:
                pass
    print("\nالنتيجة: " + ("كل شيء جاهز ✓" if ok else "هناك ما يحتاج معالجة (انظر السطور الفاشلة)"))
    return 0 if ok else 1


def main():
    ap = argparse.ArgumentParser(description="fp_bridge v" + VERSION)
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--test", action="store_true", help="فحص الاتصال والإعدادات")
    g.add_argument("--serve", action="store_true", help="خادم محلي لزر السحب من الصفحة")
    g.add_argument("--loop", nargs="?", const=15, type=int, metavar="MIN", help="خدمة مستمرة: سحب كل MIN دقيقة")
    g.add_argument("--ensure", nargs="?", const=15, type=int, metavar="MIN", help="حارس يعيد تشغيل الخدمة إن توقفت")
    g.add_argument("--install-task", dest="inst", nargs="?", const=15, type=int, metavar="MIN", help="تثبيت المهام على ويندوز")
    g.add_argument("--remove-task", dest="rmtask", action="store_true", help="إزالة المهام")
    g.add_argument("--status", action="store_true", help="عرض حالة الوسيط")
    g.add_argument("--collect", action="store_true", help="قراءة الأجهزة وحفظها محليًا فقط (بلا إنترنت)")
    g.add_argument("--flush", action="store_true", help="إرسال الحركات المحفوظة محليًا للخادم")
    g.add_argument("--sync", action="store_true", help="قراءة الأجهزة ثم الإرسال (الافتراضي)")
    ap.add_argument("--wait", type=int, default=0, metavar="MIN", help="انتظار عودة الإنترنت حتى MIN دقيقة قبل الإرسال")
    a = ap.parse_args()
    if a.test:
        return run_test()
    if a.status:
        return show_status()
    setup_log()
    global MODE
    MODE = "loop" if a.loop is not None else "serve" if a.serve else "sync"
    try:
        if a.inst is not None:
            return install_task(a.inst)
        if a.rmtask:
            return remove_task()
        if a.ensure is not None:
            return run_ensure(a.ensure)
        if a.loop is not None:
            return run_loop(a.loop)
        if a.serve:
            return run_serve()
        if a.collect:
            return run_sync(collect=True, flush=False)
        if a.flush:
            return run_sync(collect=False, flush=True, wait=a.wait)
        return run_sync(wait=a.wait)
    except Exception as e:
        log.error("خطأ: %s", e)
        return 1


if __name__ == "__main__":
    sys.exit(main())
