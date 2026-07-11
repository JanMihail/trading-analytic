--
-- Матрица распределения вероятностей по цепям Маркова
--
WITH seq AS (
    SELECT
        symbol,
        threshold,
        start_time,
        direction_name AS d0,
        lagInFrame(direction_name, 1, '') OVER w AS d1,
        lagInFrame(direction_name, 2, '') OVER w AS d2,
        lagInFrame(direction_name, 3, '') OVER w AS d3,
        lagInFrame(direction_name, 4, '') OVER w AS d4,
        lagInFrame(direction_name, 5, '') OVER w AS d5,
        lagInFrame(direction_name, 6, '') OVER w AS d6,
        lagInFrame(direction_name, 7, '') OVER w AS d7,
        lagInFrame(direction_name, 8, '') OVER w AS d8,
        lagInFrame(direction_name, 9, '') OVER w AS d9
    FROM source.tick_moves
    WHERE threshold = 0.00200
    AND start_time >= fromUnixTimestamp64Milli(1752257330935) AND start_time <= fromUnixTimestamp64Milli(1783793330935)
    WINDOW w AS (PARTITION BY symbol, threshold ORDER BY start_time)
),
by_len AS (
    SELECT
        2 AS seq_len,
        concat(d1, d0) AS sequence,
        concat(d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d1 != ''
    GROUP BY d1, d0
    UNION ALL
    SELECT
        3 AS seq_len,
        concat(d2, d1, d0) AS sequence,
        concat(d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d2 != ''
    GROUP BY d2, d1, d0
    UNION ALL
    SELECT
        4 AS seq_len,
        concat(d3, d2, d1, d0) AS sequence,
        concat(d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d3 != ''
    GROUP BY d3, d2, d1, d0
    UNION ALL
    SELECT
        5 AS seq_len,
        concat(d4, d3, d2, d1, d0) AS sequence,
        concat(d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d4 != ''
    GROUP BY d4, d3, d2, d1, d0
    UNION ALL
    SELECT
        6 AS seq_len,
        concat(d5, d4, d3, d2, d1, d0) AS sequence,
        concat(d5, d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d5 != ''
    GROUP BY d5, d4, d3, d2, d1, d0
    UNION ALL
    SELECT
        7 AS seq_len,
        concat(d6, d5, d4, d3, d2, d1, d0) AS sequence,
        concat(d6, d5, d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d6 != ''
    GROUP BY d6, d5, d4, d3, d2, d1, d0
    UNION ALL
    SELECT
        8 AS seq_len,
        concat(d7, d6, d5, d4, d3, d2, d1, d0) AS sequence,
        concat(d7, d6, d5, d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d7 != ''
    GROUP BY d7, d6, d5, d4, d3, d2, d1, d0
    UNION ALL
    SELECT
        9 AS seq_len,
        concat(d8, d7, d6, d5, d4, d3, d2, d1, d0) AS sequence,
        concat(d8, d7, d6, d5, d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d8 != ''
    GROUP BY d8, d7, d6, d5, d4, d3, d2, d1, d0
    UNION ALL
    SELECT
        10 AS seq_len,
        concat(d9, d8, d7, d6, d5, d4, d3, d2, d1, d0) AS sequence,
        concat(d9, d8, d7, d6, d5, d4, d3, d2, d1) AS prefix,
        count() AS cnt
    FROM seq WHERE d9 != ''
    GROUP BY d9, d8, d7, d6, d5, d4, d3, d2, d1, d0
)
SELECT
    seq_len,
    sequence,
    cnt,
    cnt * 100 / sum(cnt) OVER (PARTITION BY seq_len, prefix) AS probability
FROM by_len
ORDER BY probability DESC