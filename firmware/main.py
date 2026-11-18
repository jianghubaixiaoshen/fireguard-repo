# -*- coding: utf-8 -*-
"""火焰报警装置 · 主循环

链路：KY-026 火焰（GPIO31 高有效）+ AHT20 温湿度（I2C1）
      → 4G 拨号 → 移远 IoT 平台 sendTsl（物模型功能 ID 1/2/3）
      → 平台属性联动 fire=true 推送手机（固件零推送代码）
      另向自建演示看板 POST /api/ingest（与 mock 模拟器同一入库路径）

QuecPython 实测差异备忘（照 MicroPython 习惯写会翻车）：
  - 没有 os 模块；sys.path 要手动 append('/usr')
  - 上拉常量是 Pin.PULL_PU，不是 PULL_UP
  - utime.time() 返回开机秒数；墙钟用 localtime()，epoch 用 mktime(localtime())
  - sendTsl 的字典键必须是整数功能 ID；传字符串 TypeError，裸发 JSON 被静默丢弃

告警节流：火焰电平抖动会让一次点火连弹多条推送，
固件侧 60s 内只把 fire 置位一次（云端也有一份同口径节流，双保险）。
"""

import sys
sys.path.append('/usr')

import utime
from machine import Pin

from usr.config import (
    FIRE_PIN, FIRE_ACTIVE, I2C_BUS, AHT20_ADDR,
    SAMPLE_INTERVAL_MS, ALARM_THROTTLE_S,
    TS_ID_TEMP, TS_ID_HUMI, TS_ID_FIRE,
    PRODUCT_KEY, PRODUCT_SECRET, DEVICE_KEY,
    CLOUD_API, INGEST_PATH,
)

print("=== 火焰报警装置启动 ===")
print("火焰脚 GPIO%d（高有效=%d）· AHT20 I2C%d@0x%02X · 节流 %ds"
      % (FIRE_PIN, FIRE_ACTIVE, I2C_BUS, AHT20_ADDR, ALARM_THROTTLE_S))

# ---------------------------------------------------------------------------
# 传感器
# ---------------------------------------------------------------------------
fire_pin = Pin(FIRE_PIN, Pin.IN, Pin.PULL_PU)   # 注意：PULL_PU 不是 PULL_UP

try:
    from lib.aht20 import AHT20
    aht = AHT20(bus=1 if I2C_BUS == 1 else 0)
except Exception as e:
    print("AHT20 不可用（降级：只报火焰）：%s" % e)
    aht = None

# ---------------------------------------------------------------------------
# 4G / 平台认证（动态注册）
# ---------------------------------------------------------------------------
net = None
try:
    from usr import qlnet
    net = qlnet.QLNet()
    net.set_apn(1)
    net.connect_network()
    print("4G 拨号完成")
except Exception as e:
    print("组网异常：%s（真机部署时按 QuecPython 文档补全）" % e)

dev = None
try:
    from ql_iot import IoT
    from ql_iot import Mqtt
    from ql_iot import TSL
    dev = IoT(ProductKey=PRODUCT_KEY, ProductSecret=PRODUCT_SECRET,
              DeviceKey=DEVICE_KEY, qos=1)
    dev.connect()
    print("平台认证成功（密钥务必复制粘贴：手抄 l/1/I 会 -5894=签名未通过）")
except Exception as e:
    print("平台连接降级（演示看板仍可上报）：%s" % e)

# ---------------------------------------------------------------------------
# 上报
# ---------------------------------------------------------------------------
_last_alarm = 0

def post_ingest(payload):
    """向自建看板上报（mock 模式录屏用的就是这条链路）。"""
    if not CLOUD_API:
        return
    try:
        import uhttp
        # 真实部署用 urequests / uhttp 均可；此处仅示意接口口径
        uhttp.post(CLOUD_API + INGEST_PATH, json=payload)
    except Exception as e:
        print("看板上报失败（下拍重试）：%s" % e)

def report(temp, humi, fire_raw):
    """sendTsl + 看板双通道；火焰带 60s 节流。"""
    global _last_alarm
    now = utime.mktime(utime.localtime())   # 墙钟 epoch，不是开机秒数

    fire_out = 0
    if fire_raw == FIRE_ACTIVE:
        if now - _last_alarm >= ALARM_THROTTLE_S:
            _last_alarm = now
            fire_out = 1
        else:
            fire_out = 0   # 节流窗口内的抖动帧，照常报 0

    if dev:
        try:
            dev.sendTsl({
                TS_ID_TEMP: temp if temp is not None else -999,
                TS_ID_HUMI: humi if humi is not None else -999,
                TS_ID_FIRE: fire_out,
            })
        except Exception as e:
            print("sendTsl 失败：%s" % e)
            # 教训：显示在线≠数据活着。控制台「改设备信息/生成二维码/绑SN」
            # 会重置 MQTT 会话成幽灵连接——重启用 machine.WDT() 才真复位。
            try:
                from machine import WDT
                WDT()
            except Exception:
                pass

    post_ingest({"temp_c": temp, "humi_pct": humi, "fire": fire_out})
    print("[%s] T=%.1f H=%.1f%% fire_raw=%d fire_out=%d"
          % (utime.localtime(), temp or -999, humi or -999, fire_raw, fire_out))

# ---------------------------------------------------------------------------
# 主循环
# ---------------------------------------------------------------------------
while True:
    raw = fire_pin.value()
    temp, humi = (None, None)
    if aht:
        temp, humi = aht.read()
    report(temp, humi, raw)
    utime.sleep_ms(SAMPLE_INTERVAL_MS)
