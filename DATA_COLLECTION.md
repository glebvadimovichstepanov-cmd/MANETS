# 📁 DATA_COLLECTION.md

**Версия:** 2.1 (Foundation Spec)  
**Дата:** 2026-04-25  
**Статус:** ✅ Архитектурный фундамент проекта (не подлежит пересмотру без архитектурного совета)  
**Назначение:** Единый концептуальный стандарт слоя данных для MANETS. Фиксирует архитектуру, структуру хранения, контракты объектов, стратегию кэширования, инкрементной синхронизации и глубину сбора исторических данных.

---

## 1. Архитектурные принципы

| Принцип | Описание |
|---------|----------|
| **Производительность по умолчанию** | Асинхронная I/O-модель, пакетная обработка, пулинг соединений, отсутствие блокирующих операций в критическом пути |
| **Абстракция источников** | Единый контракт данных независимо от провайдера. Поддержка мульти-вендорной архитектуры с автоматическим фолбэком |
| **Инкрементность** | Загрузка только дельты данных на основе контрольных точек (checkpoint/timestamp). Полные перезагрузки запрещены в production |
| **Cache-First** | Memcached как слой горячих данных (<1ч) для inference и streaming. Persistent storage как холодный источник для обучения и бэктестов |
| **Строгая каузальность** | Нулевой допуск lookahead bias. Все признаки, макро-данные и агрегации сдвинуты относительно момента принятия решения |
| **Верифицируемость** | Каждый снэпшот сопровождается checksum, версией схемы, цепочкой источников и отчётом валидации |
| **Конфигурируемая глубина** | Гибкая настройка глубины истории по таймфреймам: 3 года для внутридневных TF, 10 лет для дневных и выше |

---

## 2. Концептуальная схема потоков данных

```
[Источники: T-Tech, MoexAlgo, CBR, Stubs] 
          ↓ (Async gRPC/REST + Stream)
[Provider Router + Rate Limiter + Circuit Breaker]
          ↓
[Memcached Layer] ←→ (Hit/Miss routing)
          ↓
[Validator & Normalizer] → (Schema check, Adjusted Prices, Causality Guard)
          ↓
[Persistent Storage] → (Tickers, Macro, LOB, Events, Metadata)
          ↓
[Data Assembly & Feature Pipeline] → (MANETS TickerEnv, Specialists, Execution GNN)
```

---

## 3. Файловая структура хранилища

```
data/
├── tickers/
│   └── {TICKER}/                    # SNGS, GAZP, SBER, LKOH, YNDX
│       ├── {timeframe}/             # 1m, 5m, 10m, 15m, 1h, 4h, 1d, 1w, 1M
│       │   ├── ohlcv.json           # Основные свечи
│       │   ├── metadata.json        # Checkpoint, source_chain, quality_score
│       │   └── validation_report.json
│       ├── lob/                     # Стаканы (Level 2)
│       │   └── snapshots_{TF}.json
│       └── fundamentals.json        # Квартальные мультипликаторы
│
├── macro/
│   └── {INSTRUMENT}/                # USD_RUB, BRENT, MOEX_INDEX, OFZ_*, CBR_RATE...
│       ├── {timeframe}/
│       │   ├── candles.json
│       │   └── metadata.json
│       └── interpolation_config.json # Метод заполнения пропусков (ffill/linear)
│
├── events/
│   └── corporate_actions.json       # Дивиденды, сплиты, отчётность, M&A
│
├── cache/                           # Локальный fallback при недоступности Memcached
│   └── hot_data/                    # LRU-дамп последних 24ч
│
├── backtest/
│   └── snapshots/                   # Версионированные копии для воспроизводимости
│       └── {snapshot_id}/
│           ├── manifest.json
│           └── checksums.sha256
│
└── metadata/
    ├── registry.json                # Глобальный каталог инструментов и статусов
    └── version_log.json             # История миграций схем
```

---

## 4. Основные объекты хранения (контракты схем)

| Объект | Назначение | Ключевые поля | Частота обновления |
|--------|-----------|---------------|-------------------|
| **OHLCV** | Базовые ценовые данные для всех таймфреймов | `timestamp, open, high, low, close, volume, adj_close, adj_factor, is_complete` | Real-time / по закрытию бара |
| **L2_OrderBook** | Микроструктурный анализ, вход для Execution GNN | `timestamp, bids[{price, qty, orders}], asks[{price, qty, orders}], mid_price, spread_bps, limit_up/down` | 100ms → агрегация по TF |
| **Trades** | Тиковый поток сделок (опционально для MVP) | `timestamp, price, volume, side, aggressor_flag` | Real-time |
| **Macro_Candles** | Макро-факторы, режимы, кросс-корреляции | `timestamp, open, high, low, close, volume, source, interpolation_method` | Зависит от инструмента (FX/Commodities: high, Rates: low) |
| **Fundamentals** | Квартальные финансовые показатели | `report_date, pe, pb, roe, ev_ebitda, debt_ebitda, dividend_yield, free_cash_flow` | Квартально |
| **Corporate_Events** | Корпоративные действия для adjusted pricing | `event_type, ex_date, record_date, payout_date, ratio/amount, status` | Event-driven |
| **Metadata** | Управление целостностью и версионированием | `snapshot_id, schema_version, checksum_sha256, source_chain, last_sync_ts, quality_score, gaps_detected` | При каждой записи |

---

## 5. Поддерживаемые таймфреймы и инструменты

### 5.1. Таймфреймы (универсальная сетка)
`1m` • `5m` • `10m` • `15m` • `1h` • `4h` • `1d` • `1w` • `1M`

*Примечание:* Для ставок и облигаций агрегация доступна только с `1d`. Для LOB и Trades доступна базовая частота с последующей агрегацией до целевых TF.

### 5.2. Глубина сбора исторических данных

Конфигурация глубины определяется в `config/data_collector.yaml`:

```yaml
history_depth:
  intraday_years: 3          # Для TF < 1d
  daily_and_above_years: 10  # Для TF >= 1d
  by_timeframe:
    1m: 3, 5m: 3, 10m: 3, 15m: 3, 1h: 3, 4h: 3
    1d: 10, 1w: 10, 1M: 10
```

| Категория | Таймфреймы | Глубина | Обоснование |
|-----------|------------|---------|-------------|
| **Внутридневные** | 1m, 5m, 10m, 15m, 1h, 4h | 3 года | Баланс между детализацией и объёмом хранения. Достаточно для ML-моделей |
| **Дневные и выше** | 1d, 1w, 1M | 10 лет | Долгосрочные циклы, макро-режимы, стресс-тестирование |

*Программный интерфейс:*
```python
from src.infrastructure.data.config import load_config

config = load_config('config/data_collector.yaml')
depth_1h = config.history_depth.get_depth_for_timeframe('1h')    # 3 года
depth_1d = config.history_depth.get_depth_for_timeframe('1d')    # 10 лет
```

### 5.3. Тикеры (MVP + расширяемость)
- **Ядро:** `SNGS, GAZP, SBER, LKOH, YNDX`
- **Расширение:** Конфигурируемый список first-tier MOEX. Модуль поддерживает динамическое добавление новых инструментов без изменения кода.

### 5.4. Макро-инструменты
| Категория | Инструменты | Примечание |
|-----------|-------------|------------|
| Валюты | `USD_RUB, EUR_RUB, CNY_RUB` | Высокая ликвидность, основной хедж-фактор |
| Сырьё | `BRENT, NATURAL_GAS` | Критично для SNGS/газового сектора |
| Индексы | `MOEX_INDEX, RTS_INDEX` | Режимы рынка, волатильность (VIX proxy) |
| Ставки/Облигации | `CBR_KEY_RATE, OFZ_26238, OFZ_26244, RUONIA` | Yield curve spread, макро-режимы |

---

## 6. Стратегия источников и фолбэк-цепочка

| Приоритет | Источник | Тип | Ответственность | Фолбэк |
|-----------|----------|-----|-----------------|--------|
| 1 | **T-Tech (Tinkoff Invest API)** | Primary | OHLCV, LOB, Trades, Fundamentals, Corporate Events, Real-time streams | → MoexAlgo |
| 2 | **MoexAlgo** | Secondary | Super Candles, OBStats, TradeStats, HI2, исторические агрегаты | → CBR/SmartLab Stub |
| 3 | **CBR / MOEX Official** | Tertiary | Ключевая ставка, RUONIA, официальные индикаторы | → Static Stub |
| 4 | **Stub Provider** | Degraded | Возвращает исторические срезы + фоллбэк-данные с пониженным `quality_score` | Блокировка новых сделок |

*Принцип:* Прозрачная замена источника без изменения контрактов данных. Все ответы нормализуются к единой схеме до попадания в валидатор.

---

## 7. Механизмы максимальной производительности

- **Асинхронный конвейер:** Полная поддержка `async/await`, неблокирующие HTTP/gRPC вызовы, конкурентный сбор по множеству тикеров/таймфреймов
- **Пул соединений:** Переиспользование сессий, HTTP/2 multiplexing, TCP keep-alive
- **Пакетная загрузка:** Объединение запросов к `GetCandles`/`GetLastPrices` для нескольких инструментов в один вызов
- **Rate Limiting & Circuit Breaker:** Token-bucket лимитирование по каждому сервису. При достижении порога ошибок ≥5 → временная изоляция источника, переключение на фолбэк
- **Приоритизация очередей:** `Execution/Realtime` > `Feature Assembly` > `Backtest/Historical`
- **Параллельная агрегация:** Сбор данных по всем 9 таймфреймам одновременно с независимыми контрольными точками

---

## 8. Кэширование и инкрементальное обновление

### 8.1. Memcached (Hot Layer)
- Назначение: Real-time inference, streaming subscriptions, частые запросы `< 1h`
- Стратегия: TTL по таймфрейму (`1m`=60s, `1h`=3600s, `1d`=86400s)
- Ключи: `{provider}:{instrument}:{tf}:{timestamp_bucket}`
- Invalidations: При успешной записи в persistent storage или по TTL

### 8.2. Инкрементальный синхронизатор
1. Чтение `metadata.json` → получение `last_checkpoint_ts`
2. Формирование запроса только за `[last_checkpoint_ts, now)`
3. Получение дельты, валидация, конкатенация с существующим массивом
4. Атомарная запись (write to `.tmp` → rename) + обновление `checkpoint_ts`
5. Логирование статистики: `new_bars`, `replaced_bars`, `gaps_filled`, `source`

*Гарантия:* Отсутствие дубликатов, полная идемпотентность повторных запусков, сохранение порядка временных меток.

---

## 9. Валидация, качество и версионирование

| Проверка | Логика | Критичность |
|----------|--------|-------------|
| **Schema Enforcement** | Соответствие контракту, типы, обязательные поля | 🔴 Critical |
| **OHLC Consistency** | `low ≤ open,close ≤ high`, `high ≥ low` | 🔴 Critical |
| **Causality Guard** | Все макро/кросс-фичи сдвинуты минимум на 1 период | 🔴 Critical |
| **Adjusted Prices** | Backward-adjustment через `adj_factor`, монотонность фактора | 🔴 Critical |
| **Gap/Volume Check** | Пропуски ≤24ч, нулевые объёмы ≤5 баров, скачки ≤15% | 🟡 High |
| **Checksum & Snapshot** | SHA256 от данных + версионирование схемы | 🟡 High |
| **Quality Scoring** | Интегральный скор [0.0, 1.0] на основе весов проверок | 🟡 High |

*Версионирование:* Каждая загрузка фиксирует `schema_version`, `snapshot_id`, `source_chain`. Старые версии архивируются в `backtest/snapshots/` согласно retention policy (90 дней для чекпоинтов, 365 дней для отчётов).

---

## 10. Интеграция с MANETS

| Компонент MANETS | Потребляемые данные | Требования к слою |
|------------------|-------------------|-------------------|
| **TickerEnv** | OHLCV (все TF), Macro (1d/1w), Fundamentals | Быстрый `get_latest()`, каузальная нормализация, state vector ~80 dims |
| **Specialists Pool** | OHLCV, Technical Indicators, Macro Regimes | Rolling windows без lookahead, консистентные метаданные |
| **Execution GNN** | L2 OrderBook, Trades, Spread/Imbalance | Глубина ≥20, бипартитная структура графа, предсказание impact |
| **Risk Controller** | Portfolio Metrics, Volatility Regimes, Circuit Breakers | Real-time доступ к `current_dd`, `vol_percentile`, `sector_corr` |
| **Drift Detector** | Historical Snapshots, Synthetic Paths | Верифицируемые снэпшоты, checksum match, metadata registry |

*Интерфейсы доступа:*
- `get_latest(instrument, tf)` → Memcached → Storage (fallback)
- `get_historical(instrument, tf, start, end)` → Indexed Storage
- `subscribe_stream(instrument, tf)` → Real-time pipeline + Cache push
- `incremental_sync()` → Delta fetch + Validation + Checkpoint update

---

## 📜 История версий

| Версия | Дата | Статус | Изменения |
|--------|------|--------|-----------|
| **2.1** | 2026-04-25 | ✅ Foundation | Добавлена секция 5.2 "Глубина сбора исторических данных" с конфигурацией по таймфреймам (3 года для <1d, 10 лет для >=1d) |
| **2.0** | 2026-04-25 | ✅ Foundation | Архитектурный фундамент: убран код, зафиксированы принципы, структура хранения, контракты объектов, Memcached, инкрементность, мульти-источники, все TF, LOB/Trades. Заглушки для недостающих источников. |
| 1.0 | 2025-01 | 🗑 Deprecated | Предыдущая спецификация с деталями реализации, заменена архитектурным стандартом |

> ⚠️ **Архитектурное правило:** Данный документ является единственным источником истины для слоя данных. Любые изменения структуры хранения, контрактов или стратегии кэширования требуют архитектурного согласования и фиксации новой версии. Возврат к деталям реализации запрещён до завершения Фазы 4.
