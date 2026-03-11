import os
import time
import threading
import pyodbc
import datetime as dt
from flask import Flask, jsonify, render_template, send_from_directory, request, redirect
from dotenv import load_dotenv

# =================================================
# Flask App & Environment
# =================================================
load_dotenv()
app = Flask(__name__)


@app.get("/assets/<path:filename>")
def assets(filename):
    return send_from_directory("assets", filename, as_attachment=False)


# -------------------------------------------------
# 서버 내부 캐시
# -------------------------------------------------
_CACHE = {
    "ts": 0.0,
    "data": None,
    "error": None,
    "running": False
}

# DB 조회 목표 주기 (초)
REFRESH_SEC = 2.0

# Emulator replay 기본 간격(초)
REPLAY_STEP_SEC = 5


# =================================================
# 환경 변수 헬퍼
# =================================================
def env(name: str, default=None, required=True):
    v = os.getenv(name, default)
    if required and (v is None or str(v).strip() == ""):
        raise RuntimeError(f"Missing env: {name}")
    return v


# =================================================
# MSSQL 연결
# =================================================
def get_conn():
    return pyodbc.connect(
        f"DRIVER={{{env('MSSQL_DRIVER')}}};"
        f"SERVER={env('MSSQL_HOST')},{env('MSSQL_PORT')};"
        f"DATABASE={env('MSSQL_DB')};"
        f"UID={env('MSSQL_USER')};"
        f"PWD={env('MSSQL_PASS')};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=yes;"
        f"Connection Timeout=10;"
    )


# =================================================
# 공통 유틸
# =================================================
def parse_local_datetime(value: str):
    if not value:
        return None

    value = value.strip()

    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in fmts:
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            pass

    try:
        return dt.datetime.fromisoformat(value)
    except Exception:
        return None


# =================================================
# 당일 생산량 계산
# =================================================
def calc_today_output_by_ip(conn) -> dict:
    out = {}
    cur = conn.cursor()

    today = dt.date.today()
    start = dt.datetime.combine(today, dt.time(0, 0, 0))
    now = dt.datetime.now()

    cur.execute("""
        SELECT ip, MIN([timestamp])
        FROM dbo.machine_status
        WHERE [timestamp] BETWEEN ? AND ?
        GROUP BY ip
    """, (start, now))
    first_ts = {str(ip): ts for ip, ts in cur.fetchall() if ip}

    first_cnt = {}
    name_map = {}

    if first_ts:
        q = ",".join(["?"] * len(first_ts))
        cur.execute(f"""
            SELECT ip, name, part_count
            FROM dbo.machine_status
            WHERE [timestamp] IN ({q})
        """, tuple(first_ts.values()))
        for ip, name, cnt in cur.fetchall():
            first_cnt[str(ip)] = int(cnt or 0)
            if name:
                name_map[str(ip)] = str(name)

    cur.execute("""
        SELECT ip, MAX([timestamp])
        FROM dbo.machine_status
        WHERE [timestamp] BETWEEN ? AND ?
        GROUP BY ip
    """, (start, now))
    last_ts = {str(ip): ts for ip, ts in cur.fetchall() if ip}

    last_cnt = {}

    if last_ts:
        q = ",".join(["?"] * len(last_ts))
        cur.execute(f"""
            SELECT ip, name, part_count
            FROM dbo.machine_status
            WHERE [timestamp] IN ({q})
        """, tuple(last_ts.values()))
        for ip, name, cnt in cur.fetchall():
            last_cnt[str(ip)] = int(cnt or 0)
            if name:
                name_map[str(ip)] = str(name)

    for ip in set(first_cnt) | set(last_cnt):
        first = first_cnt.get(ip, 0)
        last = last_cnt.get(ip, 0)
        qty = last - first if last >= first else last
        out[ip] = {
            "qty": max(0, qty),
            "name": name_map.get(ip, ip)
        }

    return out


# =================================================
# 대시보드 스냅샷 생성
# =================================================
def fetch_machine_snapshot():
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ip
                           ORDER BY [timestamp] DESC
                       ) AS rn
                FROM dbo.machine_status
                WHERE [timestamp] >= DATEADD(MINUTE, -10, GETDATE())
            ) t
            WHERE rn = 1
            ORDER BY name
        """)

        columns = [c[0] for c in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]

        today_map = calc_today_output_by_ip(conn)

    for r in rows:
        ip = str(r.get("ip") or "")
        qty = today_map.get(ip, {}).get("qty", 0)

        util = r.get("utilization_rate")
        opm = r.get("total_operating_min") or r.get("operating_min")

        try:
            if int(float(util)) == 0 or int(float(opm)) == 0:
                qty = 0
        except Exception:
            pass

        r["part_count_today"] = qty
        r["part_count"] = qty

        ts = r.get("timestamp")
        if hasattr(ts, "strftime"):
            r["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "count": len(rows),
        "rows": rows,
        "refreshed_at": time.time()
    }


# =================================================
# Replay helpers
# =================================================
def fetch_replay_range_by_date(date_str: str):
    target_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    start_dt = dt.datetime.combine(target_date, dt.time.min)
    end_dt = dt.datetime.combine(target_date, dt.time.max)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT MIN([timestamp]), MAX([timestamp])
            FROM dbo.machine_status
            WHERE [timestamp] BETWEEN ? AND ?
        """, (start_dt, end_dt))
        row = cur.fetchone()

    if not row or not row[0] or not row[1]:
        return None

    min_ts, max_ts = row
    return {
        "ok": True,
        "date": date_str,
        "min": min_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "max": max_ts.strftime("%Y-%m-%d %H:%M:%S"),
        "step_sec": REPLAY_STEP_SEC,
    }


def fetch_replay_snapshot(at_time: dt.datetime):
    day_start = dt.datetime.combine(at_time.date(), dt.time.min)
    day_end = dt.datetime.combine(at_time.date(), dt.time.max)

    with get_conn() as conn:
        cur = conn.cursor()

        # 1) 먼저 선택 시각 이하에서 ip별 최신값 조회
        cur.execute("""
            SELECT *
            FROM (
                SELECT *,
                       ROW_NUMBER() OVER (
                           PARTITION BY ip
                           ORDER BY [timestamp] DESC
                       ) AS rn
                FROM dbo.machine_status
                WHERE [timestamp] BETWEEN ? AND ?
                  AND [timestamp] <= ?
            ) t
            WHERE rn = 1
            ORDER BY name
        """, (day_start, day_end, at_time))

        columns = [c[0] for c in cur.description]
        rows = [dict(zip(columns, r)) for r in cur.fetchall()]

        # 2) 없으면 해당 날짜의 가장 이른 시각으로 fallback
        actual_target = at_time

        if not rows:
            cur.execute("""
                SELECT MIN([timestamp])
                FROM dbo.machine_status
                WHERE [timestamp] BETWEEN ? AND ?
            """, (day_start, day_end))
            first_row = cur.fetchone()
            first_ts = first_row[0] if first_row else None

            if first_ts:
                actual_target = first_ts

                cur.execute("""
                    SELECT *
                    FROM (
                        SELECT *,
                               ROW_NUMBER() OVER (
                                   PARTITION BY ip
                                   ORDER BY [timestamp] DESC
                               ) AS rn
                        FROM dbo.machine_status
                        WHERE [timestamp] BETWEEN ? AND ?
                          AND [timestamp] <= ?
                    ) t
                    WHERE rn = 1
                    ORDER BY name
                """, (day_start, day_end, actual_target))

                columns = [c[0] for c in cur.description]
                rows = [dict(zip(columns, r)) for r in cur.fetchall()]

    for r in rows:
        ts = r.get("timestamp")
        if hasattr(ts, "strftime"):
            r["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

    return {
        "ok": True,
        "target_at": actual_target.strftime("%Y-%m-%d %H:%M:%S"),
        "count": len(rows),
        "rows": rows,
        "step_sec": REPLAY_STEP_SEC,
    }


# =================================================
# Cache helper
# =================================================
def get_cached_machine_row(ip: str):
    data = _CACHE.get("data") or {}
    rows = data.get("rows") or []
    for row in rows:
        if str(row.get("ip") or "") == str(ip):
            return dict(row)
    return None


def safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def machine_current_payload(ip: str, row: dict):
    return {
        "ok": True,
        "ip": ip,
        "name": row.get("name") or ip,
        "status": row.get("status") or "UNKNOWN",
        "timestamp": row.get("timestamp"),
        "currentToolNo": safe_int(row.get("tool_no"), 0),
        "loadPct": safe_float(row.get("utilization_rate"), 0),
        "rpm": safe_float(row.get("rpm"), 0),
        "prodMin": safe_int(row.get("total_operating_min") or row.get("operating_min"), 0),
        "program": row.get("onum"),
        "alarm": row.get("alarm", 0),
        "partCount": safe_int(row.get("part_count_today") or row.get("part_count"), 0),
    }


def fetch_machine_row_at(ip: str, at_time: dt.datetime):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1 *
            FROM dbo.machine_status
            WHERE ip = ?
              AND [timestamp] <= ?
            ORDER BY [timestamp] DESC
        """, (ip, at_time))
        row = cur.fetchone()
        if not row:
            return None

        cols = [c[0] for c in cur.description]
        result = dict(zip(cols, row))

    ts = result.get("timestamp")
    if hasattr(ts, "strftime"):
        result["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

    return result


def fetch_machine_tool_row_at(ip: str, tool_no: int, at_time: dt.datetime):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1 *
            FROM dbo.machine_status
            WHERE ip = ?
              AND tool_no = ?
              AND [timestamp] <= ?
            ORDER BY [timestamp] DESC
        """, (ip, tool_no, at_time))
        row = cur.fetchone()
        if not row:
            return None

        cols = [c[0] for c in cur.description]
        result = dict(zip(cols, row))

    ts = result.get("timestamp")
    if hasattr(ts, "strftime"):
        result["timestamp"] = ts.strftime("%Y-%m-%d %H:%M:%S")

    return result


def fetch_machine_tool_chart_at(ip: str, tool_no: int, at_time: dt.datetime, limit: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TOP {limit} [timestamp], utilization_rate, rpm
            FROM dbo.machine_status
            WHERE ip = ?
              AND tool_no = ?
              AND [timestamp] <= ?
            ORDER BY [timestamp] DESC
        """, (ip, tool_no, at_time))
        rows = cur.fetchall()

    return list(reversed(rows))


# =================================================
# Background Worker
# =================================================
def refresh_loop():
    while True:
        t0 = time.time()
        try:
            _CACHE["data"] = fetch_machine_snapshot()
            _CACHE["ts"] = time.time()
            _CACHE["error"] = None
        except Exception as e:
            _CACHE["error"] = f"{type(e).__name__}: {e}"
        time.sleep(max(0.1, REFRESH_SEC - (time.time() - t0)))


def start_worker_once():
    if _CACHE["running"]:
        return

    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    _CACHE["running"] = True
    threading.Thread(target=refresh_loop, daemon=True).start()


@app.before_request
def _ensure_worker():
    start_worker_once()


# =================================================
# Routes
# =================================================
@app.get("/health")
def health():
    return jsonify({
        "ok": True,
        "cache_ready": _CACHE["data"] is not None,
        "cache_age_sec": None if not _CACHE["data"]
        else round(time.time() - _CACHE["ts"], 3),
        "error": _CACHE["error"]
    })


@app.get("/api/machines")
def api_machines():
    if not _CACHE["data"]:
        return jsonify({"error": "warming up"}), 503
    return jsonify(_CACHE["data"])


@app.get("/")
def home():
    return redirect("/dashboard")
    

@app.get("/dashboard")
def dashboard():
    return render_template("index.html")


@app.get("/emulator")
def emulator_page():
    return render_template("emulator.html", replay_step_sec=REPLAY_STEP_SEC)


@app.get("/machine/<path:ip>")
def machine_detail(ip):
    return render_template("machine_detail.html", ip=ip)


@app.get("/api/replay/range")
def api_replay_range():
    date_str = request.args.get("date", "").strip()
    if not date_str:
        return jsonify({"ok": False, "message": "date is required"}), 400

    try:
        data = fetch_replay_range_by_date(date_str)
    except Exception as e:
        return jsonify({"ok": False, "message": f"range query failed: {e}"}), 500

    if not data:
        return jsonify({"ok": False, "message": "해당 날짜의 기록이 없습니다."}), 404

    return jsonify(data)


@app.get("/api/replay/snapshot")
def api_replay_snapshot():
    at_str = request.args.get("at", "").strip()
    if not at_str:
        return jsonify({"ok": False, "message": "at is required"}), 400

    at_time = parse_local_datetime(at_str)
    if not at_time:
        return jsonify({"ok": False, "message": "invalid datetime format"}), 400

    try:
        data = fetch_replay_snapshot(at_time)
    except Exception as e:
        return jsonify({"ok": False, "message": f"snapshot query failed: {e}"}), 500

    return jsonify(data)


@app.get("/api/machine/<path:ip>/current")
def api_machine_current(ip):
    at_str = request.args.get("at", "").strip()

    if at_str:
        at_time = parse_local_datetime(at_str)
        if not at_time:
            return jsonify({"ok": False, "message": "invalid datetime format"}), 400

        row = fetch_machine_row_at(ip, at_time)
        if not row:
            return jsonify({"ok": False, "message": "machine not found at replay time"}), 404

        payload = machine_current_payload(ip, row)
        payload["replayAt"] = at_time.strftime("%Y-%m-%d %H:%M:%S")
        payload["mode"] = "replay"
        return jsonify(payload)

    row = get_cached_machine_row(ip)
    if not row:
        return jsonify({"ok": False, "message": "machine not found"}), 404

    payload = machine_current_payload(ip, row)
    payload["mode"] = "live"
    return jsonify(payload)


@app.get("/api/machine/<path:ip>/tools")
def api_machine_tools(ip):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT tool_no
            FROM dbo.machine_status
            WHERE ip = ?
              AND tool_no IS NOT NULL
              AND tool_no <> 0
            ORDER BY tool_no
        """, (ip,))
        tools = [int(r[0]) for r in cur.fetchall()]
    return jsonify({"ip": ip, "tools": tools})


@app.get("/api/machine/<path:ip>/tool/<int:tool_no>")
def api_machine_tool_detail(ip, tool_no):
    at_str = request.args.get("at", "").strip()

    if at_str:
        at_time = parse_local_datetime(at_str)
        if not at_time:
            return jsonify({"ok": False, "message": "invalid datetime format"}), 400

        current = fetch_machine_row_at(ip, at_time)
        if not current:
            return jsonify({"ok": False, "message": "machine not found at replay time"}), 404

        current_tool_no = safe_int(current.get("tool_no"), 0)
        selected_tool_row = fetch_machine_tool_row_at(ip, tool_no, at_time)
        if not selected_tool_row:
            return jsonify({"ok": False, "message": "tool not found at replay time"}), 404

        is_current_tool = (tool_no == current_tool_no)

        return jsonify({
            "ok": True,
            "ip": ip,
            "name": current.get("name") or ip,
            "status": current.get("status") or "UNKNOWN",
            "timestamp": current.get("timestamp"),
            "replayAt": at_time.strftime("%Y-%m-%d %H:%M:%S"),
            "mode": "replay",
            "toolNo": tool_no,
            "currentToolNo": current_tool_no,
            "isCurrentTool": is_current_tool,
            "loadPct": safe_float(
                current.get("utilization_rate") if is_current_tool else selected_tool_row.get("utilization_rate"),
                0
            ),
            "rpm": safe_float(current.get("rpm"), 0) if is_current_tool else 0,
            "prodMin": safe_int(current.get("total_operating_min") or current.get("operating_min"), 0),
            "program": current.get("onum"),
            "alarm": current.get("alarm", 0),
            "machineToolNo": current_tool_no,
        })

    current = get_cached_machine_row(ip)
    if not current:
        return jsonify({"ok": False, "message": "machine not found"}), 404

    current_tool_no = safe_int(current.get("tool_no"), 0)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1 *
            FROM dbo.machine_status
            WHERE ip = ?
              AND tool_no = ?
            ORDER BY [timestamp] DESC
        """, (ip, tool_no))
        row = cur.fetchone()

        if not row:
            return jsonify({"ok": False, "message": "tool not found"}), 404

        cols = [c[0] for c in cur.description]
        selected_tool_row = dict(zip(cols, row))

    is_current_tool = (tool_no == current_tool_no)

    return jsonify({
        "ok": True,
        "ip": ip,
        "name": current.get("name") or ip,
        "status": current.get("status") or "UNKNOWN",
        "timestamp": current.get("timestamp"),
        "mode": "live",
        "toolNo": tool_no,
        "currentToolNo": current_tool_no,
        "isCurrentTool": is_current_tool,
        "loadPct": safe_float(
            current.get("utilization_rate") if is_current_tool else selected_tool_row.get("utilization_rate"),
            0
        ),
        "rpm": safe_float(current.get("rpm"), 0) if is_current_tool else 0,
        "prodMin": safe_int(current.get("total_operating_min") or current.get("operating_min"), 0),
        "program": current.get("onum"),
        "alarm": current.get("alarm", 0),
        "machineToolNo": current_tool_no,
    })


@app.get("/api/machine/<path:ip>/tool/<int:tool_no>/chart")
def api_machine_tool_chart(ip, tool_no):
    at_str = request.args.get("at", "").strip()

    try:
        limit = int(request.args.get("limit", 60))
    except Exception:
        limit = 60
    limit = max(10, min(limit, 200))

    if at_str:
        at_time = parse_local_datetime(at_str)
        if not at_time:
            return jsonify({"ok": False, "message": "invalid datetime format"}), 400

        current = fetch_machine_row_at(ip, at_time)
        if not current:
            return jsonify({"ok": False, "message": "machine not found at replay time"}), 404

        current_tool_no = safe_int(current.get("tool_no"), 0)
        is_current_tool = (tool_no == current_tool_no)

        rows = fetch_machine_tool_chart_at(ip, tool_no, at_time, limit)

        labels = []
        load = []
        rpm = []

        for ts, util, row_rpm in rows:
            if hasattr(ts, "strftime"):
                labels.append(ts.strftime("%H:%M:%S"))
            else:
                labels.append(str(ts))

            load.append(round(safe_float(util, 0), 2))
            rpm.append(round(safe_float(row_rpm, 0), 2) if is_current_tool else 0)

        return jsonify({
            "ok": True,
            "ip": ip,
            "toolNo": tool_no,
            "currentToolNo": current_tool_no,
            "isCurrentTool": is_current_tool,
            "mode": "replay",
            "replayAt": at_time.strftime("%Y-%m-%d %H:%M:%S"),
            "labels": labels,
            "load": load,
            "rpm": rpm,
        })

    current = get_cached_machine_row(ip)
    if not current:
        return jsonify({"ok": False, "message": "machine not found"}), 404

    current_tool_no = safe_int(current.get("tool_no"), 0)
    is_current_tool = (tool_no == current_tool_no)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT TOP {limit} [timestamp], utilization_rate, rpm
            FROM dbo.machine_status
            WHERE ip = ?
              AND tool_no = ?
            ORDER BY [timestamp] DESC
        """, (ip, tool_no))
        rows = cur.fetchall()

    rows = list(reversed(rows))

    labels = []
    load = []
    rpm = []

    for ts, util, row_rpm in rows:
        if hasattr(ts, "strftime"):
            labels.append(ts.strftime("%H:%M:%S"))
        else:
            labels.append(str(ts))

        load.append(round(safe_float(util, 0), 2))
        rpm.append(round(safe_float(row_rpm, 0), 2) if is_current_tool else 0)

    return jsonify({
        "ok": True,
        "ip": ip,
        "toolNo": tool_no,
        "currentToolNo": current_tool_no,
        "isCurrentTool": is_current_tool,
        "mode": "live",
        "labels": labels,
        "load": load,
        "rpm": rpm,
    })


# =================================================
# Run
# =================================================
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8000, debug=True)
