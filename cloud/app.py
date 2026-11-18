"""火焰报警装置 · 云端服务（演示用模拟器 + 真机双模式）

两种数据来源，同一套看板与告警链路：
  MOCK=1（默认）  内置模拟器按真实节拍出数：温湿度缓慢漂移 + 噪声，
                  火焰按可配概率偶发，或手动 POST /api/ignite 点火。
  MOCK=0          设备经 MQTT 上报（固件见 firmware/），入库路径完全一致。

演示口径：模拟器只负责产生"设备在跑"的数据流，
所有阈值、节流、告警联动逻辑与真机固件同一份实现。
"""
import asyncio
import json
import math
import os
import random
import sqlite3
import time
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.environ.get("FIRE_DB", os.path.join(HERE, "data", "fire.db"))
MOCK = os.environ.get("MOCK", "1") == "1"
PORT = int(os.environ.get("PORT", "8100"))

# 告警节流：一次点火 60s 内只算一条告警（与固件同一口径）
ALARM_THROTTLE_S = 60
# 火焰有效电平：KY-026 实测为「见火输出高」
FIRE_ACTIVE = 1


# ---------------------------------------------------------------------------
# 数据库
# ---------------------------------------------------------------------------
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = db()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            temp_c REAL, humi_pct REAL,
            fire INTEGER NOT NULL DEFAULT 0,
            source TEXT NOT NULL DEFAULT 'device'
        );
        CREATE TABLE IF NOT EXISTS alarms (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL, fire INTEGER,
            note TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_tel_ts ON telemetry(ts);
        """
    )
    conn.commit()
    conn.close()


def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# 模拟器：按真实节拍出数
# ---------------------------------------------------------------------------
class Simulator:
    """产生与环境一致的读数。

    温度在室温基线上做缓慢漂移（正弦 + 随机游走），湿度反向微动，
    两者都带 ±0.2 / ±1 的读数噪声——真传感器就是这样，恒定值反而假。
    火焰默认熄灭，按概率偶发或由 /api/ignite 手动触发。
    """

    def __init__(self, base_temp=28.6, base_humi=36.9, fire_prob=0.012):
        self.t0 = time.time()
        self.base_temp = base_temp
        self.base_humi = base_humi
        self.fire_prob = fire_prob
        self.fire = False
        self.fire_until = 0.0
        self.tick = 0

    def next(self):
        self.tick += 1
        t = time.time() - self.t0
        drift = 0.4 * math.sin(t / 180.0) + 0.15 * math.sin(t / 37.0)
        temp = self.base_temp + drift + random.uniform(-0.2, 0.2)
        humi = self.base_humi - 1.2 * math.sin(t / 210.0) + random.uniform(-1.0, 1.0)

        if self.fire_until and time.time() < self.fire_until:
            self.fire = True
            # 有火时温度被抬一点，和真实现场一致
            temp += random.uniform(1.5, 4.0)
        elif random.random() < self.fire_prob:
            self.fire = True
            self.fire_until = time.time() + random.uniform(2, 6)
            temp += random.uniform(1.5, 4.0)
        else:
            self.fire = False

        return {
            "temp_c": round(temp, 1),
            "humi_pct": round(max(5.0, min(95.0, humi)), 1),
            "fire": 1 if self.fire else 0,
        }


SIM = Simulator()
LAST_ALARM_TS = [0.0]


def ingest(sample, source):
    """入库 + 告警节流。真机与模拟器共用这一条路径。"""
    conn = db()
    conn.execute(
        "INSERT INTO telemetry(ts, temp_c, humi_pct, fire, source) VALUES (?,?,?,?,?)",
        (now_str(), sample.get("temp_c"), sample.get("humi_pct"),
         int(sample.get("fire") == FIRE_ACTIVE), source),
    )
    alarm = None
    if int(sample.get("fire") == FIRE_ACTIVE):
        t = time.time()
        if t - LAST_ALARM_TS[0] >= ALARM_THROTTLE_S:
            LAST_ALARM_TS[0] = t
            conn.execute(
                "INSERT INTO alarms(ts, fire, note) VALUES (?,?,?)",
                (now_str(), 1, f"{ALARM_THROTTLE_S}s 节流内仅记一条"),
            )
            alarm = {"ts": now_str(), "msg": "检测到火焰，请立即处理！"}
    conn.commit()
    conn.close()
    return alarm


async def sim_loop():
    """模拟器节拍：2s 一帧，与真机 sendTsl 节拍照近。"""
    while True:
        sample = SIM.next()
        ingest(sample, "simulator")
        await asyncio.sleep(2)


@asynccontextmanager
async def lifespan(app):
    init_db()
    task = asyncio.create_task(sim_loop()) if MOCK else None
    yield
    if task:
        task.cancel()


app = FastAPI(title="4G 火焰报警装置 · 演示看板", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"],
                   allow_headers=["*"])


@app.get("/")
async def index():
    return FileResponse(os.path.join(HERE, "static", "index.html"))


@app.get("/api/now")
async def now():
    conn = db()
    row = conn.execute(
        "SELECT ts, temp_c, humi_pct, fire, source FROM telemetry "
        "ORDER BY id DESC LIMIT 1").fetchone()
    cnt = conn.execute("SELECT COUNT(*) c FROM telemetry").fetchone()["c"]
    fires = conn.execute("SELECT COUNT(*) c FROM telemetry WHERE fire=1").fetchone()["c"]
    alarms = conn.execute("SELECT COUNT(*) c FROM alarms").fetchone()["c"]
    recent = conn.execute(
        "SELECT ts, temp_c, humi_pct, fire FROM telemetry "
        "ORDER BY id DESC LIMIT 40").fetchall()
    conn.close()
    return JSONResponse({
        "mode": "simulator" if MOCK else "device",
        "now": dict(row) if row else None,
        "samples": cnt, "fire_frames": fires, "alarms": alarms,
        "throttle_s": ALARM_THROTTLE_S,
        "fire_active_level": FIRE_ACTIVE,
        "series": [dict(r) for r in reversed(recent)],
    })


@app.get("/api/alarms")
async def alarms():
    conn = db()
    rows = conn.execute(
        "SELECT ts, note FROM alarms ORDER BY id DESC LIMIT 30").fetchall()
    conn.close()
    return JSONResponse([dict(r) for r in rows])


@app.post("/api/ignite")
async def ignite(seconds: float = 4.0):
    """手动点火：录屏时用，效果与真机打火机一致。"""
    SIM.fire_until = time.time() + max(1.0, seconds)
    return JSONResponse({"ok": True, "note": f"火焰已置位 {seconds:.0f}s，节流后最多产生一条告警"})


@app.post("/api/ingest")
async def ingest_from_device(payload: dict):
    """真机上报入口（MQTT 桥或直连均可复用）。"""
    alarm = ingest(payload, "device")
    return JSONResponse({"ok": True, "alarm": alarm})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
