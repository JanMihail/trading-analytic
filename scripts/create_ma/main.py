import os
import glob
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import clickhouse_connect

# ---------- Конфигурация ----------
CH_HOST, CH_PORT, CH_USER, CH_PASSWORD = "localhost", 8123, "admin", "admin"
SRC_DB, SRC_TABLE = "source", "tick_data_GBPUSD"
DST_DB, DST_TABLE = "analytic", "tick_data_GBPUSD_with_ma"

WORK_DIR = "/data/tmp_ma"     # желательно на SSD, с запасом места (см. расчёт диска ниже)
RAW_DIR = os.path.join(WORK_DIR, "raw_chunks")
CUMSUM_PATH = os.path.join(WORK_DIR, "cum_sum.bin")
os.makedirs(RAW_DIR, exist_ok=True)

CHUNK_ROWS = 1_000_000

MA_WINDOWS = [
    100_000, 200_000, 400_000, 600_000, 800_000,
    1_000_000, 2_000_000, 4_000_000, 6_000_000, 8_000_000,
    10_000_000, 20_000_000, 40_000_000, 60_000_000,
    80_000_000, 100_000_000,
]

client = clickhouse_connect.get_client(host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD)

client.command(f"""
CREATE TABLE IF NOT EXISTS `{DST_DB}`.`{DST_TABLE}`
(
    timestamp DateTime64(3),
    ask Decimal32(5),
    bid Decimal32(5),
    {", ".join(f"ma_{n} Float64" for n in MA_WINDOWS)}
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp)
""")

# ---------- Проход A: сырые чанки + cum_sum на диск ----------
total_rows = client.query(
    f"SELECT count() FROM `{SRC_DB}`.`{SRC_TABLE}`"
).result_rows[0][0]
print(f"Всего строк: {total_rows:,}")

cumsum_mm = np.memmap(CUMSUM_PATH, dtype=np.float64, mode="w+", shape=(total_rows,))

running_total = 0.0
offset = 0
chunk_id = 0
last_ts = None

while offset < total_rows:
    if last_ts is None:
        q = f"""
            SELECT timestamp, ask, bid FROM `{SRC_DB}`.`{SRC_TABLE}`
            ORDER BY timestamp ASC LIMIT {CHUNK_ROWS}
        """
    else:
        q = f"""
            SELECT timestamp, ask, bid FROM `{SRC_DB}`.`{SRC_TABLE}`
            WHERE timestamp > toDateTime64('{last_ts}', 3)
            ORDER BY timestamp ASC LIMIT {CHUNK_ROWS}
        """
    rows = client.query(q).result_rows
    if not rows:
        break

    n = len(rows)
    ts = [r[0] for r in rows]
    ask = np.array([float(r[1]) for r in rows], dtype=np.float32)
    bid = np.array([float(r[2]) for r in rows], dtype=np.float32)

    batch_cumsum = np.cumsum(bid.astype(np.float64)) + running_total
    running_total = batch_cumsum[-1]
    cumsum_mm[offset: offset + n] = batch_cumsum

    pq.write_table(
        pa.table({"timestamp": ts, "ask": ask, "bid": bid}),
        os.path.join(RAW_DIR, f"chunk_{chunk_id:06d}.parquet"),
    )

    offset += n
    chunk_id += 1
    last_ts = ts[-1]
    print(f"[Проход A] {offset:,}/{total_rows:,}")

cumsum_mm.flush()
del cumsum_mm
print("Проход A завершён.")

# ---------- Проход B: расчёт MA через последовательные смещённые срезы ----------
cumsum_mm = np.memmap(CUMSUM_PATH, dtype=np.float64, mode="r", shape=(total_rows,))
chunk_files = sorted(glob.glob(os.path.join(RAW_DIR, "chunk_*.parquet")))

offset = 0
for path in chunk_files:
    table = pq.read_table(path)
    n = table.num_rows
    ts = table.column("timestamp").to_pylist()
    ask = table.column("ask").to_numpy()
    bid = table.column("bid").to_numpy()

    cur_cumsum = np.asarray(cumsum_mm[offset: offset + n])  # копия чанка в RAM (~8 МБ на млн строк)

    ma_cols = {}
    denom_base = np.arange(offset + 1, offset + n + 1, dtype=np.float64)

    for N in MA_WINDOWS:
        lag_start = offset - N
        lag_vals = np.zeros(n, dtype=np.float64)
        if lag_start >= 0:
            lag_vals[:] = cumsum_mm[lag_start: lag_start + n]        # последовательный срез!
        elif lag_start + n > 0:
            valid_from = -lag_start
            lag_vals[valid_from:] = cumsum_mm[0: lag_start + n]
        denom = np.minimum(denom_base, N)
        ma_cols[f"ma_{N}"] = (cur_cumsum - lag_vals) / denom

    out_table = pa.table({"timestamp": ts, "ask": ask, "bid": bid, **ma_cols})

    # Конвертация в список строк и вставка в ClickHouse
    rows_out = list(zip(*[out_table.column(c).to_pylist() for c in out_table.column_names]))
    client.insert(f"{DST_DB}.{DST_TABLE}", rows_out, column_names=out_table.column_names)

    offset += n
    print(f"[Проход B] {offset:,}/{total_rows:,}")

print("Готово.")