# import os
# import json
# import asyncio
# import time
# import logging
# from datetime import datetime, timedelta
# from collections import deque, defaultdict

# import numpy as np
# import lightgbm as lgb
# from fastapi import FastAPI
# from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# # поправленный импорт из river
# from river.forest import ARFClassifier
# from river.drift import ADWIN
# from river import metrics


# # -------------- Настройки --------------
# BOOTSTRAP    = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
# TOPIC_IN     = os.getenv("KAFKA_TOPIC_IN",  "pg.public.transaction")
# TOPIC_OUT    = os.getenv("KAFKA_TOPIC_OUT", "tx_scored")
# MODEL_TXT    = os.getenv("MODEL_TXT_PATH",   "/models/lgbm_baseline.txt")
# MAP_PATH     = os.getenv("CATEG_MAP",        "/models/cat_map.json")
# WIN_HOURS    = 24
# FRAUD_TH     = float(os.getenv("FRAUD_TH",  "0.27"))

# logging.basicConfig(
#     level=logging.INFO,
#     format='[%(asctime)s] %(levelname)s: %(message)s',
#     datefmt='%H:%M:%S'
# )

# # -------------- Загрузка моделей --------------
# LGBM_MODEL = lgb.Booster(model_file=MODEL_TXT)
# with open(MAP_PATH, "r") as f:
#     CAT2IDX = json.load(f)

# CAT_COLS = list(CAT2IDX.keys())
# NUM_COLS = ["log_amount","hour","dayofweek","is_weekend",
#             "month","txn_cnt_24h","sum_amount_24h",
#             "uniq_country_24h","zscore_amount_24h"]

# FEATURE_ORDER = ["amount"] + CAT_COLS + NUM_COLS  # убрали "account_id"

# # -------------- Онлайн-дрифт --------------
# SEED         = 42
# ARF          = ARFClassifier(
#                    n_models=10,
#                    max_depth=10,
#                    drift_detector=ADWIN(delta=1e-3),
#                    seed=SEED
#                )
# GLOBAL_ADWIN = ADWIN(delta=1e-3)
# DRIFT_METRIC = metrics.ROCAUC()
# DRIFT_ALERTS = 0

# # -------------- Функция фичей --------------
# AccountWin   = defaultdict(lambda: deque(maxlen=5000))
# COUNTRY2CODE = {}
# def country_code(c):
#     if c not in COUNTRY2CODE:
#         COUNTRY2CODE[c] = len(COUNTRY2CODE)+1
#     return COUNTRY2CODE[c]

# def feature_vector(msg: dict) -> np.ndarray:
#     created_at = datetime.fromisoformat(msg["created_at"])
#     fa = {
#         "log_amount": np.log1p(msg["amount"]),
#         "hour":       created_at.hour,
#         "dayofweek":  created_at.weekday(),
#         "is_weekend": int(created_at.weekday() >= 5),
#         "month":      created_at.month,
#     }
#     # sliding window как было
#     Q = AccountWin[msg["account_id"]]
#     Q.append((created_at, msg["amount"], msg["country_iso"]))
#     cutoff = created_at - timedelta(hours=WIN_HOURS)
#     while Q and Q[0][0] < cutoff:
#         Q.popleft()
#     hist = list(Q)[:-1]
#     fa["txn_cnt_24h"]      = len(hist)
#     fa["sum_amount_24h"]   = sum(x[1] for x in hist)
#     fa["uniq_country_24h"] = len({country_code(x[2]) for x in hist})
#     if hist:
#         mu    = fa["sum_amount_24h"] / len(hist)
#         sigma = (sum((x[1]-mu)**2 for x in hist)/len(hist))**0.5 or 1
#         fa["zscore_amount_24h"] = (msg["amount"] - mu) / sigma
#     else:
#         fa["zscore_amount_24h"] = 0.0

#     vec = []
#     for col in FEATURE_ORDER:
#         if col == "amount":
#             vec.append(msg["amount"])
#         elif col in CAT_COLS:
#             vec.append(CAT2IDX[col].get(msg[col], -1))
#         else:
#             vec.append(fa[col])
#     return np.array(vec, dtype=np.float32)

# # -------------- FastAPI --------------
# app = FastAPI()
# _start = time.time()
# _msg_count = 0

# @app.get("/health")
# def health():
#     return {
#         "status":       "ok",
#         "uptime_s":     int(time.time() - _start),
#         "tps":          round(_msg_count / max(1, time.time() - _start), 2),
#         "drift_auc":    round(DRIFT_METRIC.get(), 4),
#         "drift_alerts": DRIFT_ALERTS
#     }

# # -------------- Фоновая задача --------------
# async def stream_loop():
#     global _msg_count, DRIFT_ALERTS, DRIFT_METRIC

#     consumer = AIOKafkaConsumer(
#         TOPIC_IN, bootstrap_servers=BOOTSTRAP,
#         value_deserializer=lambda m: json.loads(m.decode())
#     )
#     producer = AIOKafkaProducer(
#         bootstrap_servers=BOOTSTRAP,
#         value_serializer=lambda m: json.dumps(m).encode()
#     )
#     await consumer.start(); await producer.start()
#     try:
#         async for msg in consumer:
#             data = msg.value

#             # LightGBM
#             feats = feature_vector(data).reshape(1, -1)
#             p_lgb = float(LGBM_MODEL.predict(feats)[0])
#             f_lgb = int(p_lgb > FRAUD_TH)

#             # ARF-online
#             x_dict = {FEATURE_ORDER[i]: float(feats[0,i])
#                       for i in range(len(FEATURE_ORDER))}
#             y_true = int(data["is_fraud"])
#             p_arf  = ARF.predict_proba_one(x_dict).get(True, 0.0)
#             DRIFT_METRIC.update(y_true, p_arf)

#             ARF.learn_one(x_dict, y_true)
#             error = int((p_arf > FRAUD_TH) != y_true)
#             if GLOBAL_ADWIN.update(error):
#                 # сбрасываем ROC-AUC, чтобы метрика считалась с нуля после дрейфта
#                 DRIFT_METRIC = metrics.ROCAUC()
#                 DRIFT_METRIC.update(y_true, p_arf)
#                 DRIFT_ALERTS += 1
#                 logging.warning(f"⚠ Concept drift detected, total={DRIFT_ALERTS}")

#             out = {
#                 **data,
#                 "fraud_prob":   round(p_lgb, 4),
#                 "fraud_flag":   f_lgb,
#                 "drift_auc":    round(DRIFT_METRIC.get(), 4),
#                 "drift_alerts": DRIFT_ALERTS
#             }
#             await producer.send_and_wait(
#                 TOPIC_OUT, out,
#                 key=str(data["account_id"]).encode()
#             )
#             _msg_count += 1

#     finally:
#         await consumer.stop(); await producer.stop()

# @app.on_event("startup")
# async def on_startup():
#     asyncio.create_task(stream_loop())


#!/usr/bin/env python3
import os
import json
import asyncio
import time
import logging
from datetime import datetime, timedelta
from collections import deque, defaultdict

import numpy as np
import lightgbm as lgb
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from river.forest import ARFClassifier
from river.drift import ADWIN
from river import metrics

# ————— Настройки —————
BOOTSTRAP       = os.getenv("KAFKA_BOOTSTRAP",    "localhost:9092")
TOPIC_IN        = os.getenv("KAFKA_TOPIC_IN",     "pg.public.transactions")
TOPIC_OUT       = os.getenv("KAFKA_TOPIC_OUT",    "tx_scored")
MODEL_TXT       = os.getenv("MODEL_TXT_PATH",     "/models/lgbm_baseline.txt")
MAP_PATH        = os.getenv("CATEG_MAP",          "/models/cat_map.json")
WIN_HOURS       = 24
FRAUD_TH        = float(os.getenv("FRAUD_TH",     "0.27"))
DRIFT_START_SEC = int(os.getenv("DRIFT_START_SEC","30"))  # вручную триггерим дрейф после 60 секунд

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s: %(message)s')

# ————— Модель и кодировки —————
LGBM_MODEL = lgb.Booster(model_file=MODEL_TXT)
with open(MAP_PATH) as f:
    CAT2IDX = json.load(f)
CAT_COLS     = list(CAT2IDX.keys())
NUM_COLS     = ["log_amount","hour","dayofweek","is_weekend",
                "month","txn_cnt_24h","sum_amount_24h",
                "uniq_country_24h","zscore_amount_24h"]
FEATURE_ORDER= ["amount"] + CAT_COLS + NUM_COLS

# ————— Онлайн-дрифт (ARF+ADWIN) —————
ARF          = ARFClassifier(n_models=10,
                              max_depth=10,
                              drift_detector=ADWIN(delta=1e-4),
                              seed=42)
GLOBAL_ADWIN = ADWIN(delta=0.01)  
DRIFT_METRIC = metrics.ROCAUC()
DRIFT_ALERTS = 0

# для ручного сброса ROC-AUC по времени
service_start_ts = None
service_drifted   = False

# ————— Функция фичей —————
AccountWin   = defaultdict(lambda: deque(maxlen=5000))
COUNTRY2CODE = {}
def country_code(c):
    if c not in COUNTRY2CODE:
        COUNTRY2CODE[c] = len(COUNTRY2CODE)+1
    return COUNTRY2CODE[c]

def feature_vector(msg: dict) -> np.ndarray:
    ts = datetime.fromisoformat(msg["created_at"])
    fa = {
        "log_amount":   np.log1p(msg["amount"]),
        "hour":         ts.hour,
        "dayofweek":    ts.weekday(),
        "is_weekend":   int(ts.weekday() >= 5),
        "month":        ts.month,
    }
    Q = AccountWin[msg["account_id"]]
    Q.append((ts, msg["amount"], msg["country_iso"]))
    cutoff = ts - timedelta(hours=WIN_HOURS)
    while Q and Q[0][0] < cutoff:
        Q.popleft()
    hist = list(Q)[:-1]
    fa["txn_cnt_24h"]      = len(hist)
    fa["sum_amount_24h"]   = sum(x[1] for x in hist)
    fa["uniq_country_24h"] = len({country_code(x[2]) for x in hist})
    if hist:
        mu    = fa["sum_amount_24h"]/len(hist)
        sigma = (sum((x[1]-mu)**2 for x in hist)/len(hist))**0.5 or 1
        fa["zscore_amount_24h"] = (msg["amount"]-mu)/sigma
    else:
        fa["zscore_amount_24h"] = 0.0

    vec = []
    for col in FEATURE_ORDER:
        if col=="amount":
            vec.append(msg["amount"])
        elif col in CAT_COLS:
            vec.append(CAT2IDX[col].get(msg[col], -1))
        else:
            vec.append(fa[col])
    return np.array(vec, dtype=np.float32)

# ————— WebSocket Manager —————
class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []
    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)
    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)
    async def broadcast(self, msg: dict):
        for ws in self.active:
            await ws.send_json(msg)

manager = ConnectionManager()

# ————— FastAPI —————
app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def index():
    return HTMLResponse(open("static/index.html","r",encoding="utf-8").read())

@app.websocket("/ws")
async def websocket(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)

# ————— Kafka Loop —————
_msg_count = 0

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(stream_loop())

async def stream_loop():
    global _msg_count, DRIFT_METRIC, DRIFT_ALERTS

    consumer = AIOKafkaConsumer(
        TOPIC_IN, bootstrap_servers=BOOTSTRAP,
        value_deserializer=lambda m: json.loads(m.decode())
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda m: json.dumps(m).encode()
    )
    await consumer.start()
    await producer.start()
    try:
        async for rec in consumer:
            t0 = time.perf_counter()
            data = rec.value

            # 1) LightGBM
            feats = feature_vector(data).reshape(1, -1)
            p_lgb = float(LGBM_MODEL.predict(feats)[0])
            f_lgb = int(p_lgb > FRAUD_TH)

            # 2) ARF + ADWIN
            x_dict = {FEATURE_ORDER[i]: float(feats[0, i])
                      for i in range(len(FEATURE_ORDER))}
            y_true = int(data["is_fraud"])
            p_arf = ARF.predict_proba_one(x_dict).get(True, 0.0)
            f_arf = int(p_arf > FRAUD_TH)  # <<< ВОТ ДОБАВИЛ ЭТОТ ФЛАГ

            DRIFT_METRIC.update(y_true, p_arf)
            ARF.learn_one(x_dict, y_true)

            error = int(f_arf != y_true)
            if GLOBAL_ADWIN.update(error):
                DRIFT_METRIC = metrics.ROCAUC()
                DRIFT_METRIC.update(y_true, p_arf)
                DRIFT_ALERTS += 1
                logging.warning(f"⚠ Concept drift #{DRIFT_ALERTS}")

            latency_ms = (time.perf_counter() - t0) * 1000
            out = {
                **data,
                "fraud_prob": p_lgb,
                "fraud_flag": f_lgb,  # LGBM предсказание
                "arf_fraud_flag": f_arf,  # <<< ARF предсказание!
                "model_correct": int(f_lgb == data["is_fraud"]),
                "arf_correct": int(f_arf == data["is_fraud"]),
                "response_time_ms": round(latency_ms, 1),
                "drift_auc": DRIFT_METRIC.get(),
                "drift_alerts": DRIFT_ALERTS
            }
            out["drift_reset"] = True
            # отправка в Kafka
            await producer.send_and_wait(
                TOPIC_OUT, out,
                key=str(data["account_id"]).encode()
            )
            # рассылка по WS
            await manager.broadcast(out)

            _msg_count += 1

    finally:
        await consumer.stop()
        await producer.stop()
