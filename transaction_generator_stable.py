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

# ---------- константы ----------
CURRENCIES         = ["EUR", "USD", "GBP", "CAD"]
MCC_NORMAL         = ["5411", "5732", "5812", "5999", "4111"]
MCC_FRAUD_1        = ["4829", "6536", "6012"]
MCC_FRAUD_2        = ["7922", "7995", "4899"]
COUNTRIES_EU       = ["NL", "DE", "FR", "ES", "IT", "BE", "PL"]
COUNTRIES_OFFSHORE = ["RU", "UA", "NG", "VN", "PH", "BR"]
CHANNELS           = ["POS", "WEB", "APP"]
DEVICES            = ["mobile", "desktop", "pos"]
TZ                 = timezone.utc

faker = Faker()
Faker.seed(42)
np.random.seed(42)
random.seed(42)

class TransactionGeneratorLite:
    """
    «Лёгкий» синтетический генератор с опциональным дрейфом:
    – До дрейфа: MCC_FRAUD_1 и lognormal(mu=8.0,σ=0.8)
    – После дрейфа: MCC_FRAUD_2 и lognormal(mu=6.0,σ=1.2)
    """
    def __init__(self, fraud_rate: float):
        self.fraud_rate = fraud_rate
        self.accounts   = np.arange(1_000_000, 1_050_000)
        self._account_home_country = {
            aid: random.choice(COUNTRIES_EU) for aid in self.accounts
        }
        self.drift = False

    def _sample_account(self) -> int:
        return int(np.random.choice(self.accounts))

    def _home_country(self, aid: int) -> str:
        return self._account_home_country[aid]

    def make_row(self, ts: datetime) -> dict:
        aid      = self._sample_account()
        is_fraud = random.random() < self.fraud_rate

        row = {
            "created_at": ts.isoformat(),
            "account_id": aid,
            "currency"   : "EUR",
            "channel"    : "WEB",
            "device_type": "desktop",
        }

        if is_fraud:
            # схема до/после дрейфа
            if not self.drift:
                # до дрейфа
                row["amount"]       = round(float(np.random.lognormal(8.0, 0.8)), 2)
                row["merchant_mcc"] = random.choice(MCC_FRAUD_1)
            else:
                # после дрейфа
                row["amount"]       = round(float(np.random.lognormal(6.0, 1.2)), 2)
                row["merchant_mcc"] = random.choice(MCC_FRAUD_2)
            row["country_iso"] = random.choice(COUNTRIES_OFFSHORE)
        else:
            # честные операции — без изменений после дрейфа
            row["amount"]       = round(float(np.random.lognormal(6.0, 0.6)), 2)
            row["merchant_mcc"] = random.choice(MCC_NORMAL)
            row["country_iso"]  = self._home_country(aid)

        row["is_fraud"] = is_fraud
        return row

def bulk_insert_pg(pg_uri: str, rows: list[dict], batch_size: int = 1000):
    """
    Быстрая вставка пачкой через psycopg2.extras.execute_values
    """
    cols = (
        "created_at",
        "account_id",
        "amount",
        "currency",
        "merchant_mcc",
        "country_iso",
        "channel",
        "device_type",
        "is_fraud",
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
    """
    Конвертирует '30m' -> 1800, '2h' -> 7200, '15s' -> 15
    """
    if not txt:
        return None
    unit = txt[-1]
    val  = int(txt[:-1])
    return {"s":1, "m":60, "h":3600}.get(unit, None) * val

def run_generator(
    rows:        int,
    fraud_rate:  float,
    csv_path:    str | None,
    pg_uri:      str | None,
    tps:         int,
    drift_start: str | None
):
    drift_sec = duration_to_seconds(drift_start)
    gen       = TransactionGeneratorLite(fraud_rate=fraud_rate)

    # CSV writer
    if csv_path:
        Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
        f_csv = open(csv_path, "w", newline="")
        csv_writer = csv.DictWriter(
            f_csv,
            fieldnames=[
                "created_at","account_id","amount","currency",
                "merchant_mcc","country_iso","channel",
                "device_type","is_fraud"
            ]
        )
        csv_writer.writeheader()
    else:
        csv_writer = None

    buffer     = []
    start_ts   = datetime.now(tz=TZ)
    last_flush = time.time()
    i = 0

    while True:
        now_ts = start_ts + timedelta(seconds=i / max(1, tps))
        # через drift_sec секунд — включаем дрейф
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

    print(
        f"✅ Done — generated {i:,} rows "
        f"{'(with drift)' if gen.drift else ''}"
    )

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Synthetic transaction generator with optional concept drift"
    )
    ap.add_argument("--rows",        type=int,   default=-1,
                    help="-1 = infinite stream")
    ap.add_argument("--fraud-rate",  type=float, default=0.015)
    ap.add_argument("--csv",         type=str,   default=None,
                    help="Path to output CSV")
    ap.add_argument("--pg",          type=str,   default=None,
                    help="Postgres URI, e.g. postgresql://user:pass@host:5432/db")
    ap.add_argument("--tps",         type=int,   default=2000,
                    help="Transactions per second")
    ap.add_argument("--drift-start", type=str,   default=None,
                    help="When to start drift, e.g. '30m', '2h', '45s'")
    args = ap.parse_args()

    run_generator(
        rows        = args.rows,
        fraud_rate  = args.fraud_rate,
        csv_path    = args.csv,
        pg_uri      = args.pg,
        tps         = args.tps,
        drift_start = args.drift_start
    )