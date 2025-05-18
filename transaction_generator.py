# import argparse, random, time, csv, sys, math, os
# from datetime import datetime, timedelta, timezone
# from typing import Optional
# from pathlib import Path
# import psycopg2, psycopg2.extras
# import numpy as np
# import pandas as pd
# from faker import Faker

# # ---------- константы ----------
# CURRENCIES = ["EUR", "USD", "GBP", "CAD"]
# MCC_NORMAL = ["5411", "5732", "5812", "5999", "4111"]
# MCC_FRAUD_1 = ["4829", "6536", "6012"]
# MCC_FRAUD_2 = ["7922", "7995", "4899"]
# COUNTRIES_EU = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
# COUNTRIES_OFFSHORE = ["RU", "UA", "NG", "VN", "PH", "BR"]
# CHANNELS = ["POS", "WEB", "APP"]
# DEVICES = ["mobile", "desktop", "pos"]
# TZ = timezone.utc

# faker = Faker()
# Faker.seed(42)
# np.random.seed(42)
# random.seed(42)


# # class TransactionGenerator:
# #     """
# #     Более реалистичный синтетический генератор транзакций.
# #     • Каждый account_id имеет смесь честных и мошеннических операций.
# #     • Признаки классов частично перекрываются.
# #     • Есть опциональный drift (смена схем мошенничества).
# #     """

# #     def __init__(self, fraud_rate: float, drift: bool = False, flip_noise=0.05):
# #         self.fraud_rate = fraud_rate          # общая доля фрода
# #         self.drift = drift
# #         self.flip_noise = flip_noise          # label noise 5 %
# #         # --- пул 50 k аккаунтов ---
# #         self.accounts = np.arange(1_000_000, 1_050_000)
# #         self._account_country = {
# #             aid: random.choice(COUNTRIES_EU) for aid in self.accounts
# #         }

# #     # ---------- helpers ----------
# #     def _sample_account(self) -> int:
# #         return int(np.random.choice(self.accounts))

# #     def _home_country(self, aid: int) -> str:
# #         return self._account_country[aid]

# #     # ---------- main ----------
# #     def make_row(self, ts: datetime) -> dict:
# #         aid = self._sample_account()
# #         base_country = self._home_country(aid)

# #         # 90 % честных, 10 % фрода на каждом аккаунте,
# #         # а глобальная fraud_rate выравнивает общий баланс
# #         is_fraud = random.random() < self.fraud_rate
# #         if random.random() < self.flip_noise:        # label noise
# #             is_fraud = not is_fraud

# #         row = {
# #             "created_at": ts.isoformat(),
# #             "account_id": aid,
# #             "currency": random.choices(CURRENCIES, weights=[50, 30, 15, 5])[0],
# #             "channel": random.choices(CHANNELS, weights=[60, 30, 10])[0],
# #             "device_type": random.choices(DEVICES, weights=[60, 30, 10])[0],
# #         }

# #         # ---- суммы с перекрытием ----
# #         if is_fraud:
# #             mu, sigma = (7.7, 0.9) if not self.drift else (3.2, 0.5)
# #         else:
# #             mu, sigma = (7.2, 0.8)
# #         row["amount"] = round(float(np.random.lognormal(mu, sigma)), 2)

# #         # ---- MCC ----
# #         if is_fraud:
# #             row["merchant_mcc"] = (
# #                 random.choice(MCC_FRAUD_1) if random.random() < 0.7
# #                 else random.choice(MCC_NORMAL)
# #             )
# #         else:
# #             row["merchant_mcc"] = (
# #                 random.choice(MCC_FRAUD_1) if random.random() < 0.15
# #                 else random.choice(MCC_NORMAL)
# #             )

# #         # ---- Country ----
# #         if is_fraud:
# #             if random.random() < 0.6:      # 60 % офшоры
# #                 row["country_iso"] = random.choice(COUNTRIES_OFFSHORE)
# #             else:
# #                 row["country_iso"] = random.choice(COUNTRIES_EU)
# #         else:
# #             # 10 % честных поедут в офшоры
# #             row["country_iso"] = (
# #                 random.choice(COUNTRIES_OFFSHORE)
# #                 if random.random() < 0.10 else base_country
# #             )

# #         row["is_fraud"] = is_fraud
# #         return row




# class TransactionGeneratorLite:
#     def __init__(self, fraud_rate: float):
#         self.fraud_rate = fraud_rate
#         self.accounts   = np.arange(1_000_000, 1_050_000)
#         self._account_home_country = {
#             aid: random.choice(COUNTRIES_EU) for aid in self.accounts
#         }
#         self.drift = False

#     def make_row(self, ts: datetime) -> dict:
#         aid      = self._sample_account()
#         is_fraud = random.random() < self.fraud_rate
#         row = {
#             "created_at": ts.isoformat(),
#             "account_id": aid,
#             "currency"   : "EUR",
#             "channel"    : "WEB",
#             "device_type": "desktop",
#         }
#         if is_fraud:
#             # до дрифта: MCC_FRAUD_1, lognormal(mu=8.0,σ=0.8)
#             # после дрифта — переключаем на MCC_FRAUD_2 и меняем μ/σ
#             if not self.drift:
#                 row["amount"]       = round(float(np.random.lognormal(8.0, 0.8)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_1)
#             else:
#                 row["amount"]       = round(float(np.random.lognormal(6.0, 1.2)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_2)
#             row["country_iso"]  = random.choice(COUNTRIES_OFFSHORE)
#         else:
#             # честные до/после дрифта остаются теми же
#             row["amount"]       = round(float(np.random.lognormal(6.0, 0.6)), 2)
#             row["merchant_mcc"] = random.choice(MCC_NORMAL)
#             row["country_iso"]  = self._home_country(aid)

#         row["is_fraud"] = is_fraud
#         return row





# def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size=1_000):
#     """
#     Быстрая вставка пачкой через psycopg2.extras.execute_values
#     """
#     cols = (
#         "created_at",
#         "account_id",
#         "amount",
#         "currency",
#         "merchant_mcc",
#         "country_iso",
#         "channel",
#         "device_type",
#         "is_fraud",
#     )
#     tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
#     with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
#         buf = []
#         for r in rows:
#             buf.append(tuple(r[c] for c in cols))
#             if len(buf) >= batch_size:
#                 psycopg2.extras.execute_values(
#                     cur,
#                     f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                     buf,
#                     tpl,
#                 )
#                 buf.clear()
#         if buf:
#             psycopg2.extras.execute_values(
#                 cur,
#                 f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                 buf,
#                 tpl,
#             )
#         conn.commit()


# def duration_to_seconds(txt: str) -> int | None:
#     if not txt:
#         return None
#     units = {"s": 1, "m": 60, "h": 3600}
#     return int(txt[:-1]) * units[txt[-1]]


# ROWS        = 2_000_000          # -1 → бесконечный стрим
# FRAUD_RATE  = 0.015
# CSV_PATH    = "/content/tx_2M.csv"   # None → не писать CSV
# PG_URI      = None             # пример: 'postgresql://fraud:secret@localhost:5432/frauddb'
# TPS         = 2_000            # для live-потока
# DRIFT_START = None           # None → без дрейфа "30m"




# def run_generator(rows: int, fraud_rate: float,
#                   csv_path: str | None,
#                   pg_uri: str | None,
#                   tps: int,
#                   drift_start: str | None):
#     drift_sec = duration_to_seconds(drift_start)
#     gen = TransactionGeneratorLite(fraud_rate=fraud_rate)

#     # CSV writer
#     if csv_path:
#         Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
#         f_csv = open(csv_path, "w", newline="")
#         csv_writer = csv.DictWriter(
#             f_csv,
#             fieldnames=[
#                 "created_at", "account_id", "amount", "currency",
#                 "merchant_mcc", "country_iso",
#                 "channel", "device_type", "is_fraud",
#             ],
#         )
#         csv_writer.writeheader()
#     else:
#         csv_writer = None

#     buffer = []
#     start_ts = datetime.now(tz=TZ)
#     last_flush = time.time()
#     i = 0
#     while True:
#         now_ts = start_ts + timedelta(seconds=i / max(1, tps))
#         # drift toggle
#         if drift_sec and (now_ts - start_ts).total_seconds() >= drift_sec:
#             gen.drift = True
#         row = gen.make_row(now_ts)
#         i += 1

#         if csv_writer:
#             csv_writer.writerow(row)
#         if pg_uri:
#             buffer.append(row)
#             if len(buffer) >= 1000 or (time.time() - last_flush) > 0.5:
#                 bulk_insert_pg(pg_uri, buffer)
#                 buffer.clear()
#                 last_flush = time.time()

#         if rows > 0 and i >= rows:
#             break
#         if rows < 0:
#             time.sleep(1 / tps)

#     if csv_writer:
#         f_csv.close()
#     if buffer and pg_uri:
#         bulk_insert_pg(pg_uri, buffer)

#     print(f"✅ Done — generated {i:,} rows ",
#           "with drift" if gen.drift else "")


# # run_generator(ROWS, FRAUD_RATE, CSV_PATH, PG_URI, TPS, DRIFT_START)

# # 2. Добавьте в конец файла
# if __name__ == "__main__":
#     import argparse
#     ap = argparse.ArgumentParser()
#     ap.add_argument("--rows", type=int, default=-1,
#                     help="-1 = бесконечный поток")
#     ap.add_argument("--fraud-rate", type=float, default=0.015)
#     ap.add_argument("--csv", type=str, default=None)
#     ap.add_argument("--pg", type=str, default=None,
#                     help="postgresql://fraud:secret@localhost:5432/frauddb")
#     ap.add_argument("--tps", type=int, default=2000)
#     ap.add_argument("--drift-start", type=str, default=None,
#                     help="например '30m' или '2h'")
#     args = ap.parse_args()

#     run_generator(
#         rows=args.rows,
#         fraud_rate=args.fraud_rate,
#         csv_path=args.csv,
#         pg_uri=args.pg,
#         tps=args.tps,
#         drift_start=args.drift_start,
#     )







#  рабочая 



# #!/usr/bin/env python3
# import argparse
# import random
# import time
# import csv
# import os
# from datetime import datetime, timedelta, timezone
# from pathlib import Path

# import psycopg2
# import psycopg2.extras
# import numpy as np
# from faker import Faker

# # ---------- константы ----------
# CURRENCIES        = ["EUR", "USD", "GBP", "CAD"]
# MCC_NORMAL        = ["5411", "5732", "5812", "5999", "4111"]
# MCC_FRAUD_1       = ["4829", "6536", "6012"]
# MCC_FRAUD_2       = ["7922", "7995", "4899"]
# COUNTRIES_EU      = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
# COUNTRIES_OFFSHORE= ["RU", "UA", "NG", "VN", "PH", "BR"]
# CHANNELS          = ["POS", "WEB", "APP"]
# DEVICES           = ["mobile", "desktop", "pos"]
# TZ                = timezone.utc

# faker = Faker()
# Faker.seed(42)
# np.random.seed(42)
# random.seed(42)

# class TransactionGeneratorLite:
#     """
#     «Лёгкий» синтетический генератор с опциональным дрейфом:
#     • До дрейфа — MCC_FRAUD_1 и lognormal(mu=8.0,σ=0.8)
#     • После дрейфа — MCC_FRAUD_2 и lognormal(mu=6.0,σ=1.2)
#     """
#     def __init__(self, fraud_rate: float):
#         self.fraud_rate = fraud_rate
#         self.accounts   = np.arange(1_000_000, 1_050_000)
#         self._account_home_country = {
#             aid: random.choice(COUNTRIES_EU) for aid in self.accounts
#         }
#         self.drift = False

#     def _sample_account(self) -> int:
#         return int(np.random.choice(self.accounts))

#     def _home_country(self, aid: int) -> str:
#         return self._account_home_country[aid]

#     def make_row(self, ts: datetime) -> dict:
#         aid      = self._sample_account()
#         is_fraud = random.random() < self.fraud_rate

#         row = {
#             "created_at": ts.isoformat(),
#             "account_id": aid,
#             "currency"   : "EUR",
#             "channel"    : "WEB",
#             "device_type": "desktop",
#         }

#         if is_fraud:
#             if not self.drift:
#                 row["amount"]       = round(float(np.random.lognormal(8.0, 0.8)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_1)
#             else:
#                 row["amount"]       = round(float(np.random.lognormal(6.0, 1.2)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_2)
#             row["country_iso"] = random.choice(COUNTRIES_OFFSHORE)
#         else:
#             row["amount"]       = round(float(np.random.lognormal(6.0, 0.6)), 2)
#             row["merchant_mcc"] = random.choice(MCC_NORMAL)
#             row["country_iso"]  = self._home_country(aid)

#         row["is_fraud"] = is_fraud
#         return row

# def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size: int = 1000):
#     """
#     Быстрая вставка пачкой через psycopg2.extras.execute_values
#     """
#     cols = (
#         "created_at",
#         "account_id",
#         "amount",
#         "currency",
#         "merchant_mcc",
#         "country_iso",
#         "channel",
#         "device_type",
#         "is_fraud",
#     )
#     tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
#     with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
#         buf = []
#         for r in rows:
#             buf.append(tuple(r[c] for c in cols))
#             if len(buf) >= batch_size:
#                 psycopg2.extras.execute_values(
#                     cur,
#                     f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                     buf, tpl
#                 )
#                 buf.clear()
#         if buf:
#             psycopg2.extras.execute_values(
#                 cur,
#                 f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                 buf, tpl
#             )
#         conn.commit()

# def duration_to_seconds(txt: str) -> int | None:
#     """
#     Конвертирует '30m' -> 1800, '2h' -> 7200, '15s' -> 15
#     """
#     if not txt:
#         return None
#     unit = txt[-1]
#     val  = int(txt[:-1])
#     if unit == 's':
#         return val
#     if unit == 'm':
#         return val * 60
#     if unit == 'h':
#         return val * 3600
#     return None

# def run_generator(
#     rows: int,
#     fraud_rate: float,
#     csv_path: str | None,
#     pg_uri:   str | None,
#     tps:      int,
#     drift_start: str | None
# ):
#     drift_sec = duration_to_seconds(drift_start)
#     gen       = TransactionGeneratorLite(fraud_rate=fraud_rate)

#     # CSV writer
#     if csv_path:
#         Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
#         f_csv = open(csv_path, "w", newline="")
#         csv_writer = csv.DictWriter(
#             f_csv,
#             fieldnames=[
#                 "created_at","account_id","amount","currency",
#                 "merchant_mcc","country_iso","channel",
#                 "device_type","is_fraud"
#             ]
#         )
#         csv_writer.writeheader()
#     else:
#         csv_writer = None

#     buffer     = []
#     start_ts   = datetime.now(tz=TZ)
#     last_flush = time.time()
#     i = 0

#     while True:
#         now_ts = start_ts + timedelta(seconds=i / max(1, tps))
#         # включаем дрейф по временному порогу
#         if drift_sec and (now_ts - start_ts).total_seconds() >= drift_sec:
#             gen.drift = True

#         row = gen.make_row(now_ts)
#         i += 1

#         if csv_writer:
#             csv_writer.writerow(row)
#         if pg_uri:
#             buffer.append(row)
#             if len(buffer) >= 1000 or (time.time() - last_flush) > 0.5:
#                 bulk_insert_pg(pg_uri, buffer)
#                 buffer.clear()
#                 last_flush = time.time()

#         if rows > 0 and i >= rows:
#             break
#         if rows < 0:
#             time.sleep(1 / tps)

#     if csv_writer:
#         f_csv.close()
#     if buffer and pg_uri:
#         bulk_insert_pg(pg_uri, buffer)

#     print(f"✅ Done — generated {i:,} rows "
#           f"{'(with drift)' if gen.drift else ''}")

# if __name__ == "__main__":
#     ap = argparse.ArgumentParser(description="Synthetic transaction generator with drift")
#     ap.add_argument("--rows",       type=int,   default=-1, help="-1 = infinite stream")
#     ap.add_argument("--fraud-rate", type=float, default=0.015)
#     ap.add_argument("--csv",        type=str,   default=None, help="Path to output CSV")
#     ap.add_argument("--pg",         type=str,   default=None,
#                     help="Postgres URI, e.g. postgresql://user:pass@host:5432/db")
#     ap.add_argument("--tps",        type=int,   default=2000, help="Transactions per second")
#     ap.add_argument("--drift-start",type=str,   default=None,
#                     help="When to start drift, e.g. '30m', '2h'")
#     args = ap.parse_args()

#     run_generator(
#         rows        = args.rows,
#         fraud_rate  = args.fraud_rate,
#         csv_path    = args.csv,
#         pg_uri      = args.pg,
#         tps         = args.tps,
#         drift_start = args.drift_start
#     )





# с дрейфом - проблема - слабый дрейф



# #!/usr/bin/env python3
# import argparse
# import random
# import time
# import csv
# import os
# from datetime import datetime, timedelta, timezone
# from pathlib import Path

# import psycopg2
# import psycopg2.extras
# import numpy as np
# from faker import Faker

# # ---------- константы ----------
# CURRENCIES         = ["EUR", "USD", "GBP", "CAD"]
# MCC_NORMAL         = ["5411", "5732", "5812", "5999", "4111"]
# MCC_FRAUD_1        = ["4829", "6536", "6012"]
# MCC_FRAUD_2        = ["7922", "7995", "4899"]
# COUNTRIES_EU       = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
# COUNTRIES_OFFSHORE = ["RU", "UA", "NG", "VN", "PH", "BR"]
# CHANNELS           = ["POS", "WEB", "APP"]
# DEVICES            = ["mobile", "desktop", "pos"]
# TZ                 = timezone.utc

# faker = Faker()
# Faker.seed(42)
# np.random.seed(42)
# random.seed(42)

# class TransactionGeneratorLite:
#     """
#     «Лёгкий» синтетический генератор с опциональным дрейфом:
#     – До дрейфа: MCC_FRAUD_1 и lognormal(mu=8.0,σ=0.8)
#     – После дрейфа: MCC_FRAUD_2 и lognormal(mu=6.0,σ=1.2)
#     """
#     def __init__(self, fraud_rate: float):
#         self.fraud_rate = fraud_rate
#         self.accounts   = np.arange(1_000_000, 1_050_000)
#         self._account_home_country = {
#             aid: random.choice(COUNTRIES_EU) for aid in self.accounts
#         }
#         self.drift = False

#     def _sample_account(self) -> int:
#         return int(np.random.choice(self.accounts))

#     def _home_country(self, aid: int) -> str:
#         return self._account_home_country[aid]

#     def make_row(self, ts: datetime) -> dict:
#         aid      = self._sample_account()
#         is_fraud = random.random() < self.fraud_rate

#         row = {
#             "created_at": ts.isoformat(),
#             "account_id": aid,
#             "currency"   : "EUR",
#             "channel"    : "WEB",
#             "device_type": "desktop",
#         }

#         if is_fraud:
#             # схема до/после дрейфа
#             if not self.drift:
#                 # до дрейфа
#                 row["amount"]       = round(float(np.random.lognormal(8.0, 0.8)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_1)
#             else:
#                 # после дрейфа
#                 row["amount"]       = round(float(np.random.lognormal(6.0, 1.2)), 2)
#                 row["merchant_mcc"] = random.choice(MCC_FRAUD_2)
#             row["country_iso"] = random.choice(COUNTRIES_OFFSHORE)
#         else:
#             # честные операции — без изменений после дрейфа
#             row["amount"]       = round(float(np.random.lognormal(6.0, 0.6)), 2)
#             row["merchant_mcc"] = random.choice(MCC_NORMAL)
#             row["country_iso"]  = self._home_country(aid)

#         row["is_fraud"] = is_fraud
#         return row

# def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size: int = 1000):
#     """
#     Быстрая вставка пачкой через psycopg2.extras.execute_values
#     """
#     cols = (
#         "created_at",
#         "account_id",
#         "amount",
#         "currency",
#         "merchant_mcc",
#         "country_iso",
#         "channel",
#         "device_type",
#         "is_fraud",
#     )
#     tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
#     with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
#         buf = []
#         for r in rows:
#             buf.append(tuple(r[c] for c in cols))
#             if len(buf) >= batch_size:
#                 psycopg2.extras.execute_values(
#                     cur,
#                     f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                     buf, tpl
#                 )
#                 buf.clear()
#         if buf:
#             psycopg2.extras.execute_values(
#                 cur,
#                 f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                 buf, tpl
#             )
#         conn.commit()

# def duration_to_seconds(txt: str) -> int | None:
#     """
#     Конвертирует '30m' -> 1800, '2h' -> 7200, '15s' -> 15
#     """
#     if not txt:
#         return None
#     unit = txt[-1]
#     val  = int(txt[:-1])
#     return {"s":1, "m":60, "h":3600}.get(unit, None) * val

# def run_generator(
#     rows:        int,
#     fraud_rate:  float,
#     csv_path:    str | None,
#     pg_uri:      str | None,
#     tps:         int,
#     drift_start: str | None
# ):
#     drift_sec = duration_to_seconds(drift_start)
#     gen       = TransactionGeneratorLite(fraud_rate=fraud_rate)

#     # CSV writer
#     if csv_path:
#         Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
#         f_csv = open(csv_path, "w", newline="")
#         csv_writer = csv.DictWriter(
#             f_csv,
#             fieldnames=[
#                 "created_at","account_id","amount","currency",
#                 "merchant_mcc","country_iso","channel",
#                 "device_type","is_fraud"
#             ]
#         )
#         csv_writer.writeheader()
#     else:
#         csv_writer = None

#     buffer     = []
#     start_ts   = datetime.now(tz=TZ)
#     last_flush = time.time()
#     i = 0

#     while True:
#         now_ts = start_ts + timedelta(seconds=i / max(1, tps))
#         # через drift_sec секунд — включаем дрейф
#         if drift_sec and (now_ts - start_ts).total_seconds() >= drift_sec:
#             gen.drift = True

#         row = gen.make_row(now_ts)
#         i += 1

#         if csv_writer:
#             csv_writer.writerow(row)
#         if pg_uri:
#             buffer.append(row)
#             if len(buffer) >= 1000 or (time.time() - last_flush) > 0.5:
#                 bulk_insert_pg(pg_uri, buffer)
#                 buffer.clear()
#                 last_flush = time.time()

#         if rows > 0 and i >= rows:
#             break
#         if rows < 0:
#             time.sleep(1 / tps)

#     if csv_writer:
#         f_csv.close()
#     if buffer and pg_uri:
#         bulk_insert_pg(pg_uri, buffer)

#     print(
#         f"✅ Done — generated {i:,} rows "
#         f"{'(with drift)' if gen.drift else ''}"
#     )

# if __name__ == "__main__":
#     ap = argparse.ArgumentParser(
#         description="Synthetic transaction generator with optional concept drift"
#     )
#     ap.add_argument("--rows",        type=int,   default=-1,
#                     help="-1 = infinite stream")
#     ap.add_argument("--fraud-rate",  type=float, default=0.015)
#     ap.add_argument("--csv",         type=str,   default=None,
#                     help="Path to output CSV")
#     ap.add_argument("--pg",          type=str,   default=None,
#                     help="Postgres URI, e.g. postgresql://user:pass@host:5432/db")
#     ap.add_argument("--tps",         type=int,   default=2000,
#                     help="Transactions per second")
#     ap.add_argument("--drift-start", type=str,   default=None,
#                     help="When to start drift, e.g. '30m', '2h', '45s'")
#     args = ap.parse_args()

#     run_generator(
#         rows        = args.rows,
#         fraud_rate  = args.fraud_rate,
#         csv_path    = args.csv,
#         pg_uri      = args.pg,
#         tps         = args.tps,
#         drift_start = args.drift_start
#     )


# жесткий концептуальный дрейф раскрывает адвин и адаптивные деревья

# #!/usr/bin/env python3
# import argparse
# import random
# import time
# import csv
# import os
# from datetime import datetime, timedelta, timezone
# from pathlib import Path

# import psycopg2
# import psycopg2.extras
# import numpy as np
# from faker import Faker

# # ---------- константы ----------
# CURRENCIES        = ["EUR", "USD", "GBP", "CAD"]
# NEW_CURRENCIES    = ["BRL", "INR", "CNY"]
# MCC_NORMAL        = ["5411", "5732", "5812", "5999", "4111"]
# MCC_FRAUD_1       = ["4829", "6536", "6012"]
# MCC_FRAUD_2       = ["7922", "7995", "4899"]
# COUNTRIES_EU      = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
# COUNTRIES_OFFSHORE= ["RU", "UA", "NG", "VN", "PH", "BR"]
# CHANNELS          = ["POS", "WEB", "APP"]
# NEW_CHANNELS      = ["API", "BOT"]
# DEVICES           = ["mobile", "desktop", "pos"]
# NEW_DEVICES       = ["tablet", "iot"]
# TZ                = timezone.utc

# faker = Faker()
# Faker.seed(42)
# np.random.seed(42)
# random.seed(42)

# class TransactionGeneratorLite:
#     """
#     Лёгкий генератор с РАДИКАЛЬНЫМ дрейфом:
#     - До дрейфа: обычные значения
#     - После дрейфа: всё другое: суммы, валюты, каналы, устройства
#     """
#     def __init__(self, fraud_rate: float):
#         self.fraud_rate = fraud_rate
#         self.accounts   = np.arange(1_000_000, 1_050_000)
#         self._account_home_country = {
#             aid: random.choice(COUNTRIES_EU) for aid in self.accounts
#         }
#         self.drift = False

#     def _sample_account(self) -> int:
#         return int(np.random.choice(self.accounts))

#     def _home_country(self, aid: int) -> str:
#         return self._account_home_country[aid]

#     def make_row(self, ts: datetime) -> dict:
#         aid      = self._sample_account()
#         is_fraud = random.random() < self.fraud_rate

#         row = {
#             "created_at": ts.isoformat(),
#             "account_id": aid,
#         }

#         if is_fraud:
#             if not self.drift:
#                 row.update({
#                     "amount": round(float(np.random.lognormal(8.0, 0.8)), 2),
#                     "merchant_mcc": random.choice(MCC_FRAUD_1),
#                     "currency": random.choice(CURRENCIES),
#                     "channel": random.choice(CHANNELS),
#                     "device_type": random.choice(DEVICES),
#                     "country_iso": random.choice(COUNTRIES_OFFSHORE),
#                 })
#             else:
#                 row.update({
#                     "amount": round(float(np.random.lognormal(10.0, 1.5)), 2) * 10,
#                     "merchant_mcc": random.choice(MCC_FRAUD_2),
#                     "currency": random.choice(NEW_CURRENCIES),
#                     "channel": random.choice(NEW_CHANNELS),
#                     "device_type": random.choice(NEW_DEVICES),
#                     "country_iso": random.choice(["BR", "NG", "VN"]),  # офшорки
#                 })
#         else:
#             if not self.drift:
#                 row.update({
#                     "amount": round(float(np.random.lognormal(6.0, 0.6)), 2),
#                     "merchant_mcc": random.choice(MCC_NORMAL),
#                     "currency": random.choice(CURRENCIES),
#                     "channel": random.choice(CHANNELS),
#                     "device_type": random.choice(DEVICES),
#                     "country_iso": self._home_country(aid),
#                 })
#             else:
#                 row.update({
#                     "amount": round(float(np.random.lognormal(8.0, 1.2)), 2) * 5,
#                     "merchant_mcc": random.choice(MCC_FRAUD_2),
#                     "currency": random.choice(NEW_CURRENCIES),
#                     "channel": random.choice(NEW_CHANNELS),
#                     "device_type": random.choice(NEW_DEVICES),
#                     "country_iso": random.choice(["BR", "NG", "VN"]),
#                 })

#         row["is_fraud"] = is_fraud
#         return row

# def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size: int = 1000):
#     cols = (
#         "created_at",
#         "account_id",
#         "amount",
#         "currency",
#         "merchant_mcc",
#         "country_iso",
#         "channel",
#         "device_type",
#         "is_fraud",
#     )
#     tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
#     with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
#         buf = []
#         for r in rows:
#             buf.append(tuple(r[c] for c in cols))
#             if len(buf) >= batch_size:
#                 psycopg2.extras.execute_values(
#                     cur,
#                     f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                     buf, tpl
#                 )
#                 buf.clear()
#         if buf:
#             psycopg2.extras.execute_values(
#                 cur,
#                 f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
#                 buf, tpl
#             )
#         conn.commit()

# def duration_to_seconds(txt: str) -> int | None:
#     if not txt:
#         return None
#     unit = txt[-1]
#     val  = int(txt[:-1])
#     if unit == 's':
#         return val
#     if unit == 'm':
#         return val * 60
#     if unit == 'h':
#         return val * 3600
#     return None

# def run_generator(
#     rows: int,
#     fraud_rate: float,
#     csv_path: str | None,
#     pg_uri:   str | None,
#     tps:      int,
#     drift_start: str | None
# ):
#     drift_sec = duration_to_seconds(drift_start)
#     gen       = TransactionGeneratorLite(fraud_rate=fraud_rate)

#     if csv_path:
#         Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
#         f_csv = open(csv_path, "w", newline="")
#         csv_writer = csv.DictWriter(
#             f_csv,
#             fieldnames=[
#                 "created_at","account_id","amount","currency",
#                 "merchant_mcc","country_iso","channel",
#                 "device_type","is_fraud"
#             ]
#         )
#         csv_writer.writeheader()
#     else:
#         csv_writer = None

#     buffer     = []
#     start_ts   = datetime.now(tz=TZ)
#     last_flush = time.time()
#     i = 0

#     while True:
#         now_ts = start_ts + timedelta(seconds=i / max(1, tps))
#         if drift_sec and (now_ts - start_ts).total_seconds() >= drift_sec:
#             gen.drift = True

#         row = gen.make_row(now_ts)
#         i += 1

#         if csv_writer:
#             csv_writer.writerow(row)
#         if pg_uri:
#             buffer.append(row)
#             if len(buffer) >= 1000 or (time.time() - last_flush) > 0.5:
#                 bulk_insert_pg(pg_uri, buffer)
#                 buffer.clear()
#                 last_flush = time.time()

#         if rows > 0 and i >= rows:
#             break
#         if rows < 0:
#             time.sleep(1 / tps)

#     if csv_writer:
#         f_csv.close()
#     if buffer and pg_uri:
#         bulk_insert_pg(pg_uri, buffer)

#     print(f"✅ Done — generated {i:,} rows "
#           f"{'(with drift)' if gen.drift else ''}")

# if __name__ == "__main__":
#     ap = argparse.ArgumentParser(description="Synthetic transaction generator with strong drift")
#     ap.add_argument("--rows",       type=int,   default=-1, help="-1 = infinite stream")
#     ap.add_argument("--fraud-rate", type=float, default=0.015)
#     ap.add_argument("--csv",        type=str,   default=None, help="Path to output CSV")
#     ap.add_argument("--pg",         type=str,   default=None,
#                     help="Postgres URI, e.g. postgresql://user:pass@host:5432/db")
#     ap.add_argument("--tps",        type=int,   default=2000, help="Transactions per second")
#     ap.add_argument("--drift-start",type=str,   default=None,
#                     help="When to start drift, e.g. '30m', '2h'")
#     args = ap.parse_args()

#     run_generator(
#         rows        = args.rows,
#         fraud_rate  = args.fraud_rate,
#         csv_path    = args.csv,
#         pg_uri      = args.pg,
#         tps         = args.tps,
#         drift_start = args.drift_start
#     )



# более мягкий концепт дрифт 

#!/usr/bin/env python3
import argparse
import random
import time
import csv
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
import numpy as np
from faker import Faker

# ---------- Константы ----------
CURRENCIES_NORMAL = ["EUR", "USD", "GBP", "CAD"]
CURRENCIES_DRIFT  = ["BRL", "CNY", "INR", "PHP", "NGN"]

MCC_NORMAL        = ["5411", "5732", "5812", "5999", "4111"]
MCC_DRIFT         = ["7995", "4899", "4829", "6536"]

COUNTRIES_EU      = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
COUNTRIES_DRIFT   = ["BR", "CN", "IN", "NG", "PH", "VN"]

CHANNELS_NORMAL   = ["POS", "WEB", "APP"]
CHANNELS_DRIFT    = ["BOT", "EMBEDDED", "SMARTWATCH"]

DEVICES_NORMAL    = ["mobile", "desktop", "pos"]
DEVICES_DRIFT     = ["iot", "smart-tv", "kiosk"]

TZ = timezone.utc

faker = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class TransactionGeneratorV2:
    """
    Генератор со стабильным fraud_rate, но с изменением признаков при дрейфе.
    """
    def __init__(self, fraud_rate: float):
        self.fraud_rate = fraud_rate
        self.accounts = np.arange(1_000_000, 1_050_000)
        self.account_home_country = {aid: random.choice(COUNTRIES_EU) for aid in self.accounts}
        self.drift = False

    def _sample_account(self) -> int:
        return int(np.random.choice(self.accounts))

    def _home_country(self, aid: int) -> str:
        return self.account_home_country[aid]

    def make_row(self, ts: datetime) -> dict:
        aid = self._sample_account()
        is_fraud = random.random() < self.fraud_rate

        if not self.drift:
            # До дрейфа: нормальные признаки
            row = {
                "created_at": ts.isoformat(),
                "account_id": aid,
                "amount": round(float(np.random.lognormal(6.0, 0.6)), 2),
                "currency": random.choice(CURRENCIES_NORMAL),
                "merchant_mcc": random.choice(MCC_NORMAL),
                "country_iso": self._home_country(aid),
                "channel": random.choice(CHANNELS_NORMAL),
                "device_type": random.choice(DEVICES_NORMAL),
                "is_fraud": is_fraud
            }
        else:
            # После дрейфа: новые странные признаки
            row = {
                "created_at": ts.isoformat(),
                "account_id": aid,
                "amount": round(float(np.random.lognormal(10.0, 1.5) * 10), 2),
                "currency": random.choice(CURRENCIES_DRIFT),
                "merchant_mcc": random.choice(MCC_DRIFT),
                "country_iso": random.choice(COUNTRIES_DRIFT),
                "channel": random.choice(CHANNELS_DRIFT),
                "device_type": random.choice(DEVICES_DRIFT),
                "is_fraud": is_fraud  # <<<<< ключевое отличие: метка фрода остается как была
            }

        return row

def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size: int = 1000):
    """
    Быстрая вставка пачкой через psycopg2.extras.execute_values
    """
    cols = (
        "created_at", "account_id", "amount", "currency",
        "merchant_mcc", "country_iso", "channel", "device_type", "is_fraud"
    )
    tpl = "(" + ",".join(["%s"] * len(cols)) + ")"
    with psycopg2.connect(pg_uri) as conn, conn.cursor() as cur:
        buf = []
        for r in rows:
            buf.append(tuple(r[c] for c in cols))
            if len(buf) >= batch_size:
                psycopg2.extras.execute_values(
                    cur,
                    f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
                    buf, tpl
                )
                buf.clear()
        if buf:
            psycopg2.extras.execute_values(
                cur,
                f"INSERT INTO public.transactions ({','.join(cols)}) VALUES %s",
                buf, tpl
            )
        conn.commit()

def duration_to_seconds(txt: str) -> int | None:
    if not txt:
        return None
    unit = txt[-1]
    val  = int(txt[:-1])
    if unit == 's':
        return val
    if unit == 'm':
        return val * 60
    if unit == 'h':
        return val * 3600
    return None

def run_generator(rows: int, fraud_rate: float, csv_path: str | None, pg_uri: str | None, tps: int, drift_start: str | None):
    drift_sec = duration_to_seconds(drift_start)
    gen = TransactionGeneratorV2(fraud_rate=fraud_rate)

    if csv_path:
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        f_csv = open(csv_path, "w", newline="")
        csv_writer = csv.DictWriter(f_csv, fieldnames=[
            "created_at","account_id","amount","currency",
            "merchant_mcc","country_iso","channel",
            "device_type","is_fraud"
        ])
        csv_writer.writeheader()
    else:
        csv_writer = None

    buffer = []
    start_ts = datetime.now(tz=TZ)
    last_flush = time.time()
    i = 0

    while True:
        now_ts = start_ts + timedelta(seconds=i / max(1, tps))
        if drift_sec and (now_ts - start_ts).total_seconds() >= drift_sec:
            gen.drift = True

        row = gen.make_row(now_ts)
        i += 1

        if csv_writer:
            csv_writer.writerow(row)
        if pg_uri:
            buffer.append(row)
            if len(buffer) >= 1000 or (time.time() - last_flush) > 0.5:
                bulk_insert_pg(pg_uri, buffer)
                buffer.clear()
                last_flush = time.time()

        if rows > 0 and i >= rows:
            break
        if rows < 0:
            time.sleep(1 / tps)

    if csv_writer:
        f_csv.close()
    if buffer and pg_uri:
        bulk_insert_pg(pg_uri, buffer)

    print(f"✅ Done — generated {i:,} rows {'(drifted)' if gen.drift else ''}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Synthetic transaction generator with controlled drift")
    ap.add_argument("--rows",       type=int,   default=-1, help="-1 = infinite stream")
    ap.add_argument("--fraud-rate", type=float, default=0.03)
    ap.add_argument("--csv",        type=str,   default=None, help="Path to output CSV")
    ap.add_argument("--pg",         type=str,   default=None, help="Postgres URI, e.g. postgresql://user:pass@host:5432/db")
    ap.add_argument("--tps",        type=int,   default=10, help="Transactions per second")
    ap.add_argument("--drift-start",type=str,   default="30s", help="When to start drift, e.g. '30s', '2m', '1h'")
    args = ap.parse_args()

    run_generator(
        rows        = args.rows,
        fraud_rate  = args.fraud_rate,
        csv_path    = args.csv,
        pg_uri      = args.pg,
        tps         = args.tps,
        drift_start = args.drift_start
    )
