# 📦 Установка и настройка MANETS Data Collector

## 1. Установка зависимостей

### Базовые зависимости
```bash
pip install -r requirements.txt
```

### Приоритетный провайдер: T-Tech Investments
Пакет `t-tech-investments` устанавливается из частного репозитория Т-Теха:

```bash
pip install t-tech-investments --index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple
```

**Примечание:** Если у вас нет доступа к репозиторию, система автоматически переключится на fallback (StubProvider).

## 2. Настройка переменных окружения

Скопируйте пример файла `.env.example`:
```bash
cp .env.example .env
```

Заполните файл `.env`:
```bash
# T-Tech Investments API Token (обязательно для работы с реальными данными)
INVEST_TOKEN=t.your_actual_token_here

# MoexAlgo API Token (опционально, вторичный провайдер)
MOEXALGO_TOKEN=your_moex_token_here

# Memcached (опционально, если не используется — будет LRU fallback)
MEMCACHED_HOST=localhost
MEMCACHED_PORT=11211
```

### Как получить INVEST_TOKEN:
1. Зайдите в [Т-Тех Инвестиции](https://tinkoffinvest.ru/)
2. Перейдите в настройки → API доступ
3. Создайте новый токен с правами на чтение данных
4. Скопируйте токен в `.env`

## 3. Запуск тестов

Проверка работоспособности всех компонентов:
```bash
pytest run_test.py -v
```

Или через прямой запуск:
```bash
python run_test.py
```

## 4. Быстрый старт

Пример использования DataCollector:
```python
import asyncio
import os
from src.infrastructure.data.collector import DataCollector
from src.infrastructure.data.config import load_config

async def main():
    # Убедитесь, что токен установлен
    os.environ['INVEST_TOKEN'] = 'your_token_here'
    
    # Загрузка конфигурации
    config = load_config('config/data_collector.yaml')
    
    # Инициализация коллектора
    collector = DataCollector(config)
    
    # Получение данных
    candles = await collector.get_latest('SNGS', '1h', count=10)
    
    for candle in candles:
        print(f"{candle.timestamp}: O={candle.open} C={candle.close}")

asyncio.run(main())
```

## 5. Структура данных

После запуска данные сохраняются в:
```
data/
├── storage/
│   └── tickers/
│       ├── SNGS/
│       │   ├── 1h/
│       │   │   ├── candles.json
│       │   │   └── checkpoint.json
│       │   └── 1d/
│       └── GAZP/
├── cache/
│   └── hot_data/
└── metadata/
    ├── registry.json
    └── version_log.json
```

## 6. Troubleshooting

### Ошибка: "Auth token not found"
- Проверьте, что переменная окружения `INVEST_TOKEN` установлена
- Убедитесь, что токен действителен (не истек срок действия)

### Ошибка: "Memcached unavailable"
- Это не критично. Система автоматически использует встроенный LRU cache
- Для продакшена установите Memcached: `docker run -d -p 11211:11211 memcached`

### Ошибка: "ModuleNotFoundError: No module named 't_tech_investments'"
- Установите пакет командой выше
- Или временно используйте StubProvider (данные будут синтетическими)

## 7. Приоритеты провайдеров

Система автоматически выбирает провайдер в порядке приоритета:
1. **T-Tech Investments** (приоритет 1) — основной источник
2. **MoexAlgo** (приоритет 2) — fallback для OHLCV
3. **CBR** (приоритет 3) — макро-данные (курсы, ставки)
4. **Stub** (приоритет 4) — синтетические данные для тестов

При недоступности основного провайдера происходит автоматическое переключение на следующий.
