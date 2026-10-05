-- ============================================================================
-- schema.sql
-- Схема для хранения нарезанных цепей Маркова (fixed-step / Renko-подобная
-- нарезка по цене bid) и служебной таблицы чекпоинтов для резюмируемой
-- потоковой обработки.
-- ============================================================================

-- Исходная таблица (для справки, уже существует у вас):
--
-- CREATE TABLE source.tick_data_GBPUSD
-- (
--     `timestamp` DateTime64(3),
--     `ask` Decimal(9,5),
--     `bid` Decimal(9,5),
--     `flags` UInt32
-- )
-- ENGINE = MergeTree
-- PARTITION BY toYYYYMM(timestamp)
-- ORDER BY (timestamp);


-- ----------------------------------------------------------------------------
-- Результирующая таблица движений (цепь Маркова).
-- Партиционирование по символу и месяцу start_time, сортировка по
-- (symbol, threshold, start_time) — удобно для последующей группировки
-- по инструменту/порогу и для оконных функций по времени (расчёт матрицы
-- переходов).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source.tick_moves
(
    symbol         LowCardinality(String),
    threshold      Decimal(9,5),
    direction_name LowCardinality(String),   -- 'UP' / 'DOWN'
    direction      Int8,                     -- +1 / -1
    start_time     DateTime64(3),
    end_time       DateTime64(3),
    start_price    Decimal(9,5),
    end_price      Decimal(9,5),
    time_delta     UInt32
)
ENGINE = MergeTree
PARTITION BY (symbol, toYYYYMM(start_time))
ORDER BY (symbol, threshold, start_time);


-- ----------------------------------------------------------------------------
-- Таблица чекпоинтов состояния алгоритма нарезки.
-- Хранит текущий "якорь" (anchor) — последнюю цену и время, от которых
-- отсчитывается следующее движение, а также timestamp последней
-- обработанной строки исходных тиков (last_processed) для возобновления
-- обработки без повторного прохода по всей таблице.
--
-- ReplacingMergeTree(updated_at) + запрос с FINAL: таблица крошечная
-- (одна актуальная строка на пару symbol+threshold), FINAL на ней дешёвый.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS source.tick_moves_state
(
    symbol         LowCardinality(String),
    threshold      Decimal(9,5),
    anchor_price   Decimal(9,5),
    anchor_time    DateTime64(3),
    last_processed DateTime64(3),
    updated_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (symbol, threshold);


-- ----------------------------------------------------------------------------
-- Пример запроса для построения матрицы переходов Маркова первого порядка
-- (по направлению + бакету амплитуды хода — чистый direction чередуется
-- не всегда, но добавление амплитуды делает состояния содержательными).
-- Раскомментируйте и адаптируйте под конкретный анализ, когда понадобится.
-- ----------------------------------------------------------------------------
-- WITH moves AS (
--     SELECT
--         direction_name,
--         multiIf(abs(end_price - start_price) < threshold * 2, 'small',
--                 abs(end_price - start_price) < threshold * 4, 'medium', 'large') AS size_bucket,
--         start_time
--     FROM source.tick_moves
--     WHERE symbol = 'GBPUSD' AND threshold = 0.00100
-- )
-- SELECT
--     concat(direction_name, '_', size_bucket) AS from_state,
--     leadInFrame(concat(direction_name, '_', size_bucket))
--         OVER (ORDER BY start_time ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING) AS to_state,
--     count() AS n
-- FROM moves
-- GROUP BY from_state, to_state
-- ORDER BY from_state, to_state;
