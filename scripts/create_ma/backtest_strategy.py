"""
Бэктест стратегии "bid - ma_200000" по тиковым данным из ClickHouse.

Правила:
  Вход:
    - bid - ma_200000 >  500  -> Short по bid
    - bid - ma_200000 < -500  -> Long по ask
  Выход:
    - Short закрывается, когда bid - ma_200000 < 0, по цене ask
    - Long закрывается,  когда bid - ma_200000 > 0, по цене bid

Архитектура:
  - Данные читаются из ClickHouse ПАРТИЦИЯМИ (toYYYYMM(timestamp)) — это
    естественные чанки, заданные структурой таблицы (PARTITION BY toYYYYMM(timestamp)).
  - Внутри партиции конечный автомат считается в numba-функции (быстро, как C).
  - Состояние стратегии (позиция, цена/время входа, накопленный PnL) хранится
    в обычных python-переменных и передаётся из партиции в партицию.
  - Чекпоинт в JSON-файле позволяет дозагружать новые партиции без пересчёта
    уже обработанных -> решение НЕ зависит от общего объёма данных в таблице.

Требования:
  pip install clickhouse-connect numba numpy pandas
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd
import clickhouse_connect
from numba import njit

# ----------------------------------------------------------------------------
# КОНФИГУРАЦИЯ
# ----------------------------------------------------------------------------

CH_HOST = "localhost"
CH_PORT = 8123
CH_USER = "admin"
CH_PASSWORD = "admin"
CH_DATABASE = "analytic"
CH_TABLE = "tick_data_GBPUSD_with_ma"

MA_COLUMN = "ma_400000"
ENTRY_THRESHOLD = 0.01000

OUTPUT_TRADES_CSV = Path("trades.csv")
CHECKPOINT_FILE = Path("backtest_checkpoint.json")

# ----------------------------------------------------------------------------
# КОНЕЧНЫЙ АВТОМАТ (numba, векторизованно быстро проходит по чанку)
# ----------------------------------------------------------------------------

@njit(cache=True)
def run_fsm(ts_ns, bid, ask, diff, state, entry_price, entry_ts):
    """
    Проходит по одному чанку тиков (уже отсортированному по времени) и
    возвращает закрытые внутри чанка сделки + итоговое состояние на конец чанка.

    state: 0 = нет позиции, -1 = short, 1 = long
    """
    n = diff.shape[0]
    out_entry_ts = np.empty(n, dtype=np.int64)
    out_exit_ts = np.empty(n, dtype=np.int64)
    out_dir = np.empty(n, dtype=np.int8)          # -1 short, 1 long
    out_entry_price = np.empty(n, dtype=np.float64)
    out_exit_price = np.empty(n, dtype=np.float64)
    cnt = 0

    for i in range(n):
        d = diff[i]

        if state == 0:
            if d > ENTRY_THRESHOLD:
                state = -1
                entry_price = bid[i]
                entry_ts = ts_ns[i]
            elif d < -ENTRY_THRESHOLD:
                state = 1
                entry_price = ask[i]
                entry_ts = ts_ns[i]

        elif state == -1:  # в шорте
            if d < 0.0:
                out_entry_ts[cnt] = entry_ts
                out_exit_ts[cnt] = ts_ns[i]
                out_dir[cnt] = -1
                out_entry_price[cnt] = entry_price
                out_exit_price[cnt] = ask[i]
                cnt += 1
                state = 0

        else:  # state == 1, в лонге
            if d > 0.0:
                out_entry_ts[cnt] = entry_ts
                out_exit_ts[cnt] = ts_ns[i]
                out_dir[cnt] = 1
                out_entry_price[cnt] = entry_price
                out_exit_price[cnt] = bid[i]
                cnt += 1
                state = 0

    return (
        out_entry_ts[:cnt], out_exit_ts[:cnt], out_dir[:cnt],
        out_entry_price[:cnt], out_exit_price[:cnt],
        state, entry_price, entry_ts,
    )


# ----------------------------------------------------------------------------
# РАБОТА С CLICKHOUSE
# ----------------------------------------------------------------------------

def get_client():
    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE,
    )


def get_partitions(client):
    """Список партиций таблицы (быстро — читается только метаданные)."""
    query = f"""
        SELECT partition
        FROM system.parts
        WHERE database = '{CH_DATABASE}'
          AND table = '{CH_TABLE}'
          AND active
        GROUP BY partition
        ORDER BY partition
    """
    result = client.query(query)
    return [row[0] for row in result.result_rows]


def load_partition(client, partition):
    """Загружает одну партицию, отсортированную по времени, в numpy-массивы."""
    query = f"""
        SELECT
            toUnixTimestamp64Nano(timestamp) AS ts_ns,
            toFloat64(bid) AS bid,
            toFloat64(ask) AS ask,
            toFloat64(bid) - {MA_COLUMN} AS diff
        FROM {CH_DATABASE}.{CH_TABLE}
        WHERE toYYYYMM(timestamp) = {partition}
        ORDER BY timestamp
    """
    df = client.query_df(query)
    return (
        df["ts_ns"].to_numpy(dtype=np.int64),
        df["bid"].to_numpy(dtype=np.float64),
        df["ask"].to_numpy(dtype=np.float64),
        df["diff"].to_numpy(dtype=np.float64),
    )


# ----------------------------------------------------------------------------
# ЧЕКПОИНТ (для инкрементальной дозагрузки новых данных без пересчёта всего)
# ----------------------------------------------------------------------------

def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            data = json.load(f)
        return data
    return {
        "last_partition": None,
        "state": 0,
        "entry_price": 0.0,
        "entry_ts": 0,
        "cumulative_pnl": 0.0,
        "trades_count": 0,
    }


def save_checkpoint(cp):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f, indent=2)


# ----------------------------------------------------------------------------
# ОСНОВНОЙ ЦИКЛ
# ----------------------------------------------------------------------------

def main():
    client = get_client()
    cp = load_checkpoint()

    partitions = get_partitions(client)

    if cp["last_partition"] is not None:
        partitions = [p for p in partitions if p > cp["last_partition"]]

    if not partitions:
        print("Нет новых партиций для обработки.")
        return

    state = cp["state"]
    entry_price = cp["entry_price"]
    entry_ts = cp["entry_ts"]
    cumulative_pnl = cp["cumulative_pnl"]

    write_header = not OUTPUT_TRADES_CSV.exists()

    for partition in partitions:
        print(f"Обработка партиции {partition} ...")
        ts_ns, bid, ask, diff = load_partition(client, partition)

        if ts_ns.shape[0] == 0:
            continue

        (entry_ts_arr, exit_ts_arr, dir_arr,
         entry_price_arr, exit_price_arr,
         state, entry_price, entry_ts) = run_fsm(
            ts_ns, bid, ask, diff, state, entry_price, entry_ts
        )

        n_trades = entry_ts_arr.shape[0]
        if n_trades > 0:
            pnl_arr = np.where(
                dir_arr == -1,
                entry_price_arr - exit_price_arr,   # short: profit = entry - exit
                exit_price_arr - entry_price_arr,   # long:  profit = exit - entry
            )
            cumulative_pnl_arr = cumulative_pnl + np.cumsum(pnl_arr)
            cumulative_pnl = cumulative_pnl_arr[-1]

            trades_df = pd.DataFrame({
                "entry_time": pd.to_datetime(entry_ts_arr, unit="ns"),
                "exit_time": pd.to_datetime(exit_ts_arr, unit="ns"),
                "direction": np.where(dir_arr == -1, "short", "long"),
                "entry_price": entry_price_arr,
                "exit_price": exit_price_arr,
                "pnl": pnl_arr,
                "cumulative_pnl": cumulative_pnl_arr,
            })

            trades_df.to_csv(
                OUTPUT_TRADES_CSV, mode="a", index=False, header=write_header, decimal=','
            )
            write_header = False
            cp["trades_count"] += n_trades

        # Сохраняем чекпоинт после каждой партиции — можно безопасно
        # прервать выполнение и продолжить позже.
        cp.update({
            "last_partition": partition,
            "state": int(state),
            "entry_price": float(entry_price),
            "entry_ts": int(entry_ts),
            "cumulative_pnl": float(cumulative_pnl),
        })
        save_checkpoint(cp)

        print(
            f"  тиков: {ts_ns.shape[0]:>10}  "
            f"сделок закрыто: {n_trades:>6}  "
            f"текущее состояние: {state:>2}  "
            f"cum_pnl: {cumulative_pnl:.5f}"
        )

    print("\nГотово.")
    print(f"Всего сделок: {cp['trades_count']}")
    print(f"Итоговый cumulative PnL: {cp['cumulative_pnl']:.5f}")
    if cp["state"] != 0:
        print(
            f"Внимание: на конец данных осталась открытая позиция "
            f"({'short' if cp['state'] == -1 else 'long'}), "
            f"она не учтена в PnL (не закрыта)."
        )
    print(f"Сделки записаны в: {OUTPUT_TRADES_CSV.resolve()}")


if __name__ == "__main__":
    main()
