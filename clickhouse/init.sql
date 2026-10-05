--
-- Справочники
--
DROP DATABASE IF EXISTS `catalog`;
CREATE DATABASE `catalog`;

DROP TABLE IF EXISTS `catalog`.instrument;
CREATE TABLE `catalog`.instrument
(
    name String NOT NULL COMMENT 'Название инструмента',
    tick_size Decimal32(5) NOT NULL COMMENT 'Размер тика'
)
ENGINE = MergeTree()
ORDER BY (name)
COMMENT 'Справочник инструментов';

INSERT INTO `catalog`.instrument (name, tick_size) VALUES
	('AUDUSD', 0.00001),
	('EURUSD', 0.00001),
	('EURUSD_real', 0.00001),
	('GBPUSD', 0.00001),
	('USDJPY', 0.00100),
	('XAUUSD', 0.01000),
	('XAUUSD_real', 0.01000);


--
-- Шаблоны таблиц
--
DROP DATABASE IF EXISTS `templates`;
CREATE DATABASE `templates`;

DROP TABLE IF EXISTS `templates`.tick_data;
CREATE TABLE `templates`.tick_data
(
    timestamp DateTime64(3) NOT NULL COMMENT 'Временная метка котировки',
    ask Decimal32(5) NOT NULL COMMENT 'Цена покупки',
    bid Decimal32(5) NOT NULL COMMENT 'Цена продажи',
    flags UInt32 NULL COMMENT 'Флаги изменения'
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp)
COMMENT 'Шаблон для таблицы с сырыми тиковыми данными';



--
-- Сырые даные
--
DROP DATABASE IF EXISTS `source`;
CREATE DATABASE `source`;

CREATE TABLE `source`.tick_data_AUDUSD AS `templates`.tick_data COMMENT 'Тиковые данные AUDUSD';
CREATE TABLE `source`.tick_data_EURUSD AS `templates`.tick_data COMMENT 'Тиковые данные EURUSD';
CREATE TABLE `source`.tick_data_EURUSD_real AS `templates`.tick_data COMMENT 'Тиковые данные EURUSD_real';
CREATE TABLE `source`.tick_data_GBPUSD AS `templates`.tick_data COMMENT 'Тиковые данные GBPUSD';
CREATE TABLE `source`.tick_data_USDJPY AS `templates`.tick_data COMMENT 'Тиковые данные USDJPY';
CREATE TABLE `source`.tick_data_XAUUSD AS `templates`.tick_data COMMENT 'Тиковые данные XAUUSD';
CREATE TABLE `source`.tick_data_XAUUSD_real AS `templates`.tick_data COMMENT 'Тиковые данные XAUUSD_real';


--
-- Аналитика
--
DROP DATABASE IF EXISTS `analytic`;
CREATE DATABASE `analytic`;

DROP TABLE IF EXISTS `analytic`.tick_data_GBPUSD_with_ma;
CREATE TABLE `analytic`.tick_data_GBPUSD_with_ma
(
    timestamp DateTime64(3) NOT NULL,
    ask Decimal32(5) NOT NULL,
    bid Decimal32(5) NOT NULL,
    ma_100000 Float64,
    ma_200000 Float64,
    ma_400000 Float64,
    ma_600000 Float64,
    ma_800000 Float64,
    ma_1000000 Float64,
    ma_2000000 Float64,
    ma_4000000 Float64,
    ma_6000000 Float64,
    ma_8000000 Float64,
    ma_10000000 Float64,
    ma_20000000 Float64,
    ma_40000000 Float64,
    ma_60000000 Float64,
    ma_80000000 Float64,
    ma_100000000 Float64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY timestamp;