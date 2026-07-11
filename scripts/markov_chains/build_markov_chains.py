#!/usr/bin/env python3
"""
build_markov_chains.py

Нарезает тиковые данные bid из ClickHouse на цепи Маркова с фиксированным
шагом (Renko-подобная нарезка): каждое звено цепи -- это движение цены
на >= threshold от последнего "якоря" в одну сторону. В отличие от
Directional Change / ZigZag, звенья одного направления могут идти подряд
(UP, UP, UP, ...), если цена монотонно растёт.

Алгоритм:
    anchor = первая цена потока
    для каждого следующего тика p:
        если p - anchor >= threshold:      emit(UP,   anchor -> p); anchor = p
        иначе если anchor - p >= threshold: emit(DOWN, anchor -> p); anchor = p
        иначе: ничего не делаем, ждём следующий тик

Свойства решения:
  - Потоковое чтение через execute_iter: память не зависит от объёма
    исходной таблицы (протестировано на паттерне до 1 млрд+ строк).
  - Таблица tick_data_* уже ORDER BY (timestamp), поэтому
    "SELECT ... ORDER BY timestamp" не требует пересортировки -- это
    дешёвый потоковый merge уже упорядоченных кусков.
  - Резюмируемость: состояние (anchor_price, anchor_time, last_processed)
    сохраняется в source.tick_moves_state после каждого обработанного
    блока. Повторный запуск скрипта продолжит с того места, где
    остановился (по timestamp > last_processed), не блокируя.
  - Численное ядро нарезки написано на numba (@njit) и работает с
    целочисленными представлениями цены (price * 10^5) и времени
    (unix-миллисекунды), что даёт скорость на уровне 10^8-10^9
    элементов/сек на одном ядре CPU и отсутствие проблем с плавающей
    точкой.

Использование:
    python3 build_markov_chains.py --config config.yaml
    python3 build_markov_chains.py --config config.yaml --reset   # начать с нуля
    python3 build_markov_chains.py --config config.yaml --dry-run # без записи в CH
"""

import argparse
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Optional, Tuple

import numpy as np
import yaml
from clickhouse_driver import Client
from numba import njit

# ----------------------------------------------------------------------------
# Константы
# ----------------------------------------------------------------------------

# Масштаб для перевода Decimal(9,5) в целые числа (5 знаков после запятой).
PRICE_SCALE = 100_000

EPOCH = datetime(1970, 1, 1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("build_markov_chains")


# ----------------------------------------------------------------------------
# Численное ядро нарезки (numba, работает с int64 массивами)
# ----------------------------------------------------------------------------

@njit(cache=True)
def renko_kernel(ts, price, threshold, anchor_price, anchor_time,
                  out_dir, out_st, out_et, out_sp, out_ep):
    """
    Прогоняет один блок тиков через state-machine нарезки.

    ts, price       -- int64 массивы (unix ms, price*PRICE_SCALE) одного блока
    threshold       -- int64, порог в тех же единицах, что и price
    anchor_price/time -- текущее состояние (переносится между вызовами!)
    out_*           -- предвыделенные выходные буферы, длина >= len(ts)
                        (в худшем случае каждый тик даёт одно звено)

    Возвращает: (m, anchor_price, anchor_time)
        m -- сколько звеньев записано в out_* (первые m элементов валидны)
        anchor_price/time -- новое состояние для следующего вызова
    """
    n = ts.shape[0]
    m = 0
    for i in range(n):
        p = price[i]
        t = ts[i]
        diff = p - anchor_price
        if diff >= threshold:
            out_dir[m] = 1
            out_st[m] = anchor_time
            out_et[m] = t
            out_sp[m] = anchor_price
            out_ep[m] = p
            m += 1
            anchor_price = p
            anchor_time = t
        elif -diff >= threshold:
            out_dir[m] = -1
            out_st[m] = anchor_time
            out_et[m] = t
            out_sp[m] = anchor_price
            out_ep[m] = p
            m += 1
            anchor_price = p
            anchor_time = t
        # иначе: движение внутри threshold, якорь не меняется
    return m, anchor_price, anchor_time


# ----------------------------------------------------------------------------
# Вспомогательные функции
# ----------------------------------------------------------------------------

def ms_to_dt(ms: int) -> datetime:
    """Целые unix-миллисекунды -> datetime без потери точности (без float)."""
    return EPOCH + timedelta(milliseconds=int(ms))


def dt_to_ms(dt: datetime) -> int:
    delta = dt - EPOCH
    return int(delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000)


def validate_symbol(symbol: str) -> str:
    """Простая защита от инъекции в имя таблицы (identifier нельзя параметризовать)."""
    if not symbol.replace("_", "").isalnum():
        raise ValueError(f"Недопустимое имя символа: {symbol!r}")
    return symbol


@dataclass
class Config:
    host: str
    port: int
    user: str
    password: str
    database: str
    symbol: str
    threshold: Decimal
    block_size: int
    flush_rows: int
    log_every_rows: int

    @staticmethod
    def load(path: str) -> "Config":
        with open(path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        ch = raw["clickhouse"]
        return Config(
            host=ch.get("host", "localhost"),
            port=int(ch.get("port", 9000)),
            user=ch.get("user", "default"),
            password=ch.get("password", ""),
            database=ch.get("database", "source"),
            symbol=validate_symbol(raw["symbol"]),
            threshold=Decimal(str(raw["threshold"])),
            block_size=int(raw.get("block_size", 200_000)),
            flush_rows=int(raw.get("flush_rows", 200_000)),
            log_every_rows=int(raw.get("log_every_rows", 5_000_000)),
        )


# ----------------------------------------------------------------------------
# Работа с чекпоинтом
# ----------------------------------------------------------------------------

def load_checkpoint(client: Client, cfg: Config) -> Optional[Tuple[Decimal, datetime, datetime]]:
    rows = client.execute(
        f"""
        SELECT anchor_price, anchor_time, last_processed
        FROM {cfg.database}.tick_moves_state FINAL
        WHERE symbol = %(symbol)s AND threshold = %(threshold)s
        """,
        {"symbol": cfg.symbol, "threshold": cfg.threshold},
    )
    if rows:
        return rows[0]  # (anchor_price: Decimal, anchor_time: datetime, last_processed: datetime)
    return None


def save_checkpoint(client: Client, cfg: Config, anchor_price_i: int,
                     anchor_time_ms: int, last_processed_ms: int) -> None:
    client.execute(
        f"""INSERT INTO {cfg.database}.tick_moves_state
            (symbol, threshold, anchor_price, anchor_time, last_processed)
            VALUES""",
        [(
            cfg.symbol,
            cfg.threshold,
            Decimal(anchor_price_i) / PRICE_SCALE,
            ms_to_dt(anchor_time_ms),
            ms_to_dt(last_processed_ms),
        )],
    )


def reset_checkpoint(client: Client, cfg: Config) -> None:
    client.execute(
        f"ALTER TABLE {cfg.database}.tick_moves_state "
        f"DELETE WHERE symbol = %(symbol)s AND threshold = %(threshold)s",
        {"symbol": cfg.symbol, "threshold": cfg.threshold},
    )
    client.execute(
        f"ALTER TABLE {cfg.database}.tick_moves "
        f"DELETE WHERE symbol = %(symbol)s AND threshold = %(threshold)s",
        {"symbol": cfg.symbol, "threshold": cfg.threshold},
    )
    log.info("Чекпоинт и ранее посчитанные движения для symbol=%s threshold=%s удалены",
              cfg.symbol, cfg.threshold)


# ----------------------------------------------------------------------------
# Основной цикл обработки
# ----------------------------------------------------------------------------

def process_block(chunk_ts: List[int], chunk_price: List[int], threshold_i: int,
                   anchor_price_i: int, anchor_time_ms: int
                   ) -> Tuple[List[tuple], int, int]:
    """Прогоняет один накопленный блок через numba-ядро, возвращает
    список сырых кортежей движений (direction, start_ms, end_ms, start_price_i, end_price_i)
    и новое состояние якоря."""
    ts_arr = np.asarray(chunk_ts, dtype=np.int64)
    pr_arr = np.asarray(chunk_price, dtype=np.int64)
    n = len(ts_arr)

    out_dir = np.zeros(n, dtype=np.int8)
    out_st = np.zeros(n, dtype=np.int64)
    out_et = np.zeros(n, dtype=np.int64)
    out_sp = np.zeros(n, dtype=np.int64)
    out_ep = np.zeros(n, dtype=np.int64)

    m, anchor_price_i, anchor_time_ms = renko_kernel(
        ts_arr, pr_arr, threshold_i, anchor_price_i, anchor_time_ms,
        out_dir, out_st, out_et, out_sp, out_ep,
    )

    moves = []
    for i in range(m):
        moves.append((
            int(out_dir[i]), int(out_st[i]), int(out_et[i]),
            int(out_sp[i]), int(out_ep[i]),
        ))
    return moves, anchor_price_i, anchor_time_ms


def moves_to_rows(cfg: Config, moves: List[tuple]) -> List[tuple]:
    rows = []
    for d, st_ms, et_ms, sp_i, ep_i in moves:
        rows.append((
            cfg.symbol,
            cfg.threshold,
            "U" if d == 1 else "D",
            d,
            ms_to_dt(st_ms),
            ms_to_dt(et_ms),
            Decimal(sp_i) / PRICE_SCALE,
            Decimal(ep_i) / PRICE_SCALE,
        ))
    return rows


def insert_moves(client: Client, cfg: Config, rows: List[tuple]) -> None:
    if not rows:
        return
    client.execute(
        f"""INSERT INTO {cfg.database}.tick_moves
            (symbol, threshold, direction_name, direction,
             start_time, end_time, start_price, end_price)
            VALUES""",
        rows,
    )


def run(cfg: Config, dry_run: bool = False, reset: bool = False) -> None:
    # ВАЖНО: clickhouse-driver не позволяет выполнить второй запрос на том же
    # соединении, пока предыдущий потоковый execute_iter() на этом соединении
    # не вычитан до конца (PartiallyConsumedQueryError: "Simultaneous queries
    # on single connection detected"). Поэтому используем два отдельных
    # соединения: одно только для чтения тиков (execute_iter),
    # второе -- только для записи движений и чекпоинтов.
    client_read = Client(
        host=cfg.host, port=cfg.port, user=cfg.user,
        password=cfg.password, database=cfg.database,
    )
    client_write = Client(
        host=cfg.host, port=cfg.port, user=cfg.user,
        password=cfg.password, database=cfg.database,
    )

    try:
        threshold_i = int((cfg.threshold * PRICE_SCALE).to_integral_value())

        if reset:
            reset_checkpoint(client_write, cfg)

        checkpoint = load_checkpoint(client_write, cfg)

        table = f"{cfg.database}.tick_data_{cfg.symbol}"

        if checkpoint:
            anchor_price, anchor_time, last_processed = checkpoint
            anchor_price_i = int((anchor_price * PRICE_SCALE).to_integral_value())
            anchor_time_ms = dt_to_ms(anchor_time)
            where_sql = "timestamp > %(from_ts)s"
            params = {"from_ts": last_processed}
            log.info("Найден чекпоинт: anchor=%s@%s, продолжаем с timestamp > %s",
                      anchor_price, anchor_time, last_processed)
            need_bootstrap_anchor = False
        else:
            where_sql = "1"
            params = {}
            anchor_price_i = None
            anchor_time_ms = None
            need_bootstrap_anchor = True
            log.info("Чекпоинт не найден, начинаем с первой строки таблицы %s", table)

        # ВАЖНО: bid имеет тип Decimal(9,5) -- всего 4 знака до запятой capacity.
        # "bid * 100000" в ClickHouse НЕ расширяет тип результата автоматически
        # и переполняется (DECIMAL_OVERFLOW) практически для любого ненулевого
        # значения, т.к. результат нужно хранить как Decimal(9,5), а он занимает
        # 6 знаков до запятой. Поэтому явно расширяем точность до Decimal64(5)
        # (макс. 13 знаков до запятой) перед умножением -- проверено на chdb.
        query = (
            f"SELECT toUnixTimestamp64Milli(timestamp) AS ts_ms, "
            f"toInt64(CAST(bid AS Decimal64(5)) * {PRICE_SCALE}) AS bid_i "
            f"FROM {table} "
            f"WHERE {where_sql} "
            f"ORDER BY timestamp"
        )

        rows_iter = client_read.execute_iter(
            query, params, settings={"max_block_size": cfg.block_size}
        )

        if need_bootstrap_anchor:
            try:
                first_ts_ms, first_price_i = next(rows_iter)
            except StopIteration:
                log.warning("Таблица %s пуста, нечего обрабатывать", table)
                return
            anchor_price_i = first_price_i
            anchor_time_ms = first_ts_ms
            last_processed_ms = first_ts_ms
            log.info("Bootstrap anchor: price=%s time=%s",
                      Decimal(anchor_price_i) / PRICE_SCALE, ms_to_dt(anchor_time_ms))
        else:
            last_processed_ms = anchor_time_ms

        buf_ts: List[int] = []
        buf_price: List[int] = []
        out_buffer: List[tuple] = []

        total_processed = 0
        total_moves = 0
        next_log_threshold = cfg.log_every_rows

        def flush_block():
            nonlocal anchor_price_i, anchor_time_ms, last_processed_ms, total_moves
            if not buf_ts:
                return
            moves, anchor_price_i, anchor_time_ms = process_block(
                buf_ts, buf_price, threshold_i, anchor_price_i, anchor_time_ms
            )
            last_processed_ms = buf_ts[-1]
            total_moves += len(moves)
            out_buffer.extend(moves_to_rows(cfg, moves))
            buf_ts.clear()
            buf_price.clear()

            if not dry_run:
                if len(out_buffer) >= cfg.flush_rows:
                    insert_moves(client_write, cfg, out_buffer)
                    out_buffer.clear()
                save_checkpoint(client_write, cfg, anchor_price_i, anchor_time_ms, last_processed_ms)

        for ts_ms, price_i in rows_iter:
            buf_ts.append(ts_ms)
            buf_price.append(price_i)
            total_processed += 1

            if len(buf_ts) >= cfg.block_size:
                flush_block()

            if total_processed >= next_log_threshold:
                log.info("Обработано строк: %d, найдено движений: %d, текущий anchor=%s@%s",
                          total_processed, total_moves,
                          Decimal(anchor_price_i) / PRICE_SCALE, ms_to_dt(anchor_time_ms))
                next_log_threshold += cfg.log_every_rows

        # финальный неполный блок
        flush_block()

        # финальный сброс накопленных, но ещё не вставленных движений
        if not dry_run and out_buffer:
            insert_moves(client_write, cfg, out_buffer)
            out_buffer.clear()

        log.info(
            "Готово. Всего обработано исходных строк: %d, найдено движений: %d. "
            "Финальный anchor=%s@%s (сохранён в чекпоинт%s).",
            total_processed, total_moves,
            Decimal(anchor_price_i) / PRICE_SCALE if anchor_price_i is not None else None,
            ms_to_dt(anchor_time_ms) if anchor_time_ms is not None else None,
            "" if not dry_run else ", но dry-run -- ничего не записано в ClickHouse",
        )
    finally:
        client_read.disconnect()
        client_write.disconnect()


# ----------------------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default="config.yaml", help="Путь к config.yaml")
    parser.add_argument("--dry-run", action="store_true",
                         help="Прогнать алгоритм и посчитать статистику, "
                              "но ничего не писать в ClickHouse (чекпоинт тоже не сохраняется)")
    parser.add_argument("--reset", action="store_true",
                         help="Удалить существующий чекпоинт и ранее посчитанные "
                              "движения для этого symbol+threshold и начать заново")
    args = parser.parse_args()

    cfg = Config.load(args.config)
    log.info("Конфигурация: symbol=%s threshold=%s block_size=%d flush_rows=%d",
              cfg.symbol, cfg.threshold, cfg.block_size, cfg.flush_rows)

    run(cfg, dry_run=args.dry_run, reset=args.reset)


if __name__ == "__main__":
    main()