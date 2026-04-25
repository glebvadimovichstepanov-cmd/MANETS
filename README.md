# MANETS Data Collector

**Версия:** 2.0 | **Дата:** 2026-04-25 | **Статус:** Foundation Spec

Система сбора, нормализации и хранения рыночных данных для MANETS Trading System.

## 📋 Возможности

- **Мульти-провайдер архитектура**: T-Tech Investments (приоритет), MoexAlgo, CBR, Stub fallback
- **Асинхронный конвейер**: Высокая производительность с rate limiting и circuit breaker
- **Инкрементальная синхронизация**: Загрузка только дельты данных по контрольным точкам
- **Двухуровневое кэширование**: Memcached (hot layer) + Local LRU fallback
- **Строгая валидация**: OHLC консистентность, каузальность, adjusted prices
- **Глубина истории**: 
  - Внутридневные TF (< 1d): **3 года**
  - Дневные и выше (>= 1d): **10 лет**
- **Поддерживаемые таймфреймы**: 1m, 5m, 10m, 15m, 1h, 4h, 1d, 1w, 1M

## 📁 Структура проекта

```
/workspace/
├── config/
│   └── data_collector.yaml      # Конфигурация системы
├── src/infrastructure/data/     # Ядро Data Collector
│   ├── collector.py             # Главный класс DataCollector
│   ├── config.py                # Загрузка конфигурации
│   ├── models.py                # Pydantic модели данных
│   ├── validator.py             # Валидация данных
│   ├── cache/                   # Memcached + Local fallback
│   ├── providers/               # Провайдеры данных
│   ├── storage/                 # Постоянное хранилище
│   └── sync/                    # Инкрементальная синхронизация
├── data/
│   ├── storage/                 # Постоянное хранилище данных
│   ├── metadata/                # Контрольные точки и метаданные
│   └── cache/                   # Локальный кэш
├── tests/                       # Тесты
├── run_collector.py             # Точка входа
├── run_test.py                  # Тестирование
├── INSTALL.md                   # Инструкция по установке
├── DATA_COLLECTION.md           # Архитектурная спецификация
└── README.md                    # Этот файл
```

## 🚀 Быстрый старт

```python
import asyncio
from src.infrastructure.data.collector import DataCollector
from src.infrastructure.data.config import load_config

async def main():
    config = load_config('config/data_collector.yaml')
    collector = DataCollector(config)
    
    # Получение последних 10 свечей SNGS 1h
    candles = await collector.get_latest('SNGS', '1h', count=10)
    
    for candle in candles:
        print(f"{candle.timestamp}: O={candle.open} C={candle.close}")

asyncio.run(main())
```

## 📖 Документация

| Документ | Описание |
|----------|----------|
| [INSTALL.md](INSTALL.md) | Установка зависимостей, настройка токенов, запуск тестов |
| [DATA_COLLECTION.md](DATA_COLLECTION.md) | Архитектурная спецификация слоя данных (Foundation Spec) |

## ⚙️ Конфигурация глубины данных

В `config/data_collector.yaml` настроена глубина сбора исторических данных:

```yaml
history_depth:
  intraday_years: 3          # Для TF < 1d (1m, 5m, 10m, 15m, 1h, 4h)
  daily_and_above_years: 10  # Для TF >= 1d (1d, 1w, 1M)
  by_timeframe:
    1m: 3, 5m: 3, 10m: 3, 15m: 3, 1h: 3, 4h: 3
    1d: 10, 1w: 10, 1M: 10
```

## 🔑 Лицензия

MANETS Trading System © 2026
