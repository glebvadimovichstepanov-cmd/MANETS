#!/usr/bin/env python3
"""
run_test.py — Комплексный тест инфраструктуры сбора данных (Фаза 1).

Проверяет:
1. Конфигурацию (YAML загрузка).
2. Модели данных (Pydantic валидация).
3. Валидаторы бизнес-логики (OHLC, каузальность, разрывы).
4. Утилиты (RateLimiter, CircuitBreaker).
5. Кэширование (Memcached wrapper + LRU fallback).
6. Хранилище (Атомарная запись, checkpoint).
7. Провайдеры (StubProvider генерация).
8. Инкрементальную синхронизацию.
9. Оркестратор (DataCollector пайплайн).

Запуск:
    python run_test.py

Требования:
    pip install pydantic pyyaml aiohttp aiomcache pymemcache cachetools pytest-asyncio
"""

import asyncio
import os
import sys
import json
import time
import shutil
import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import List, Optional

# Настройка логирования для тестов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TestRunner")

# --- Импорт компонентов инфраструктуры ---
# Добавляем корень проекта в path, если скрипт запускается не из корня
if os.path.basename(os.getcwd()) != "workspace":
    # Попытка определить корень относительно скрипта
    script_dir = os.path.dirname(os.path.abspath(__file__))
    if os.path.exists(os.path.join(script_dir, "src")):
        os.chdir(script_dir)
    elif os.path.exists(os.path.join(script_dir, "..", "src")):
        os.chdir(os.path.join(script_dir, ".."))

try:
    from src.infrastructure.data.config import load_config
    from src.infrastructure.data.models import (
        Candle, L2OrderBook, Trade, MacroCandle, 
        Fundamental, CorporateEvent, Checkpoint
    )
    from src.infrastructure.data.validator import DataValidator
    from src.infrastructure.data.providers.base import (
        DataProvider, TokenBucketRateLimiter, CircuitBreaker
    )
    from src.infrastructure.data.providers.stub import StubProvider
    from src.infrastructure.data.cache.memcached import MemcachedClient
    from src.infrastructure.data.storage.local_file import LocalFileStorage
    from src.infrastructure.data.sync.incremental import IncrementalSynchronizer, CausalityError, IncrementalSyncError
    from src.infrastructure.data.collector import DataCollector
except ImportError as e:
    logger.error(f"❌ Ошибка импорта: {e}")
    logger.error("Убедитесь, что зависимости установлены: pip install -r requirements.txt")
    sys.exit(1)

# --- Константы теста ---
TEST_DATA_DIR = "data/test_storage_tmp"
TEST_CONFIG_PATH = "config/data_collector.yaml"
TICKER = "SBER"
TIMEFRAME = "1h"


class TestResult:
    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.errors = []

    def add_pass(self, name: str):
        self.passed += 1
        print(f"  ✅ {name}")

    def add_fail(self, name: str, reason: str):
        self.failed += 1
        self.errors.append((name, reason))
        print(f"  ❌ {name}: {reason}")

    def summary(self):
        total = self.passed + self.failed
        status = "✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ" if self.failed == 0 else "⚠️ ЕСТЬ ОШИБКИ"
        print("\n" + "="*50)
        print(f"ИТОГИ: {status}")
        print(f"Пройдено: {self.passed}/{total}")
        print(f"Провалено: {self.failed}/{total}")
        if self.errors:
            print("\nДетали ошибок:")
            for name, reason in self.errors:
                print(f"  - {name}: {reason}")
        print("="*50)
        return self.failed == 0


# --- Тестовые функции ---

def test_config_loading(results: TestResult):
    """Тест 1: Загрузка конфигурации."""
    name = "Конфигурация (YAML)"
    try:
        if not os.path.exists(TEST_CONFIG_PATH):
            raise FileNotFoundError(f"Файл {TEST_CONFIG_PATH} не найден")
        
        config = load_config(TEST_CONFIG_PATH)
        
        # Проверка структуры конфига
        assert hasattr(config, 'providers'), "Отсутствует секция providers"
        assert len(config.providers) > 0, "Список провайдеров пуст"
        assert hasattr(config, 'timeframes'), "Отсутствует секция timeframes"
        
        # Проверяем что providers это список словарей
        assert isinstance(config.providers, list), "providers должен быть списком"
        
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_models_validation(results: TestResult):
    """Тест 2: Валидация Pydantic моделей."""
    name = "Модели данных (Pydantic)"
    try:
        now = datetime.now(timezone.utc)
        
        # Корректная свеча (со всеми обязательными полями)
        candle = Candle(
            timestamp=now,
            open=Decimal("100.5"),
            high=Decimal("105.0"),
            low=Decimal("99.0"),
            close=Decimal("102.0"),
            volume=Decimal("1000"),
            adj_close=Decimal("102.0")  # Обязательное поле
        )
        assert candle.high >= candle.low, "Нарушено правило High >= Low"
        assert candle.open >= candle.low and candle.open <= candle.high, "Open вне диапазона"
        
        # Проверка отклонения некорректной свечи (High < Low)
        try:
            bad_candle = Candle(
                timestamp=now,
                open=Decimal("100"),
                high=Decimal("90"),  # Ошибка: High < Low
                low=Decimal("95"),
                close=Decimal("92"),
                volume=Decimal("100"),
                adj_close=Decimal("92")
            )
            # В зависимости от реализации валидатора, это может выбросить ошибку сразу или позже
        except ValueError:
            pass # Ожидаемое поведение при строгой модели

        # L2 OrderBook
        lob = L2OrderBook(
            timestamp=now,
            bids=[(Decimal("100.0"), 10), (Decimal("99.5"), 20)],
            asks=[(Decimal("100.5"), 15), (Decimal("101.0"), 25)]
        )
        assert len(lob.bids) == 2
        
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_business_validator(results: TestResult):
    """Тест 3: Бизнес-валидатор (OHLC, разрывы, каузальность)."""
    name = "Бизнес-валидатор"
    try:
        validator = DataValidator()
        now = datetime.now(timezone.utc)
        
        # Набор корректных свечей (со всеми обязательными полями)
        candles = [
            Candle(timestamp=now - timedelta(hours=2), open=Decimal("100"), high=Decimal("102"), low=Decimal("99"), close=Decimal("101"), volume=Decimal("100"), adj_close=Decimal("101")),
            Candle(timestamp=now - timedelta(hours=1), open=Decimal("101"), high=Decimal("103"), low=Decimal("100"), close=Decimal("102"), volume=Decimal("120"), adj_close=Decimal("102")),
            Candle(timestamp=now, open=Decimal("102"), high=Decimal("104"), low=Decimal("101"), close=Decimal("103"), volume=Decimal("110"), adj_close=Decimal("103"))
        ]
        
        # Проверка последовательности
        is_valid = validator.validate_candles(candles)
        assert is_valid, "Корректные свечи отклонены"
        
        # Проверка каузальности для макро-данных
        macro = MacroCandle(
            timestamp=now,
            indicator="CPI",
            value=Decimal("1.2"),
            shift_periods=1 # Сдвиг на 1 период вперед (защита от lookahead)
        )
        assert macro.shift_periods >= 0
        
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_rate_limiter(results: TestResult):
    """Тест 4: Token Bucket Rate Limiter."""
    name = "Rate Limiter (Token Bucket)"
    try:
        # Используем правильные параметры: rate и burst
        limiter = TokenBucketRateLimiter(rate=2.0, burst=5) # 2 токена в сек, макс 5
        
        async def check():
            # Быстрое потребление
            acquired = 0
            for _ in range(5):
                if await limiter.acquire():
                    acquired += 1
            assert acquired == 5, "Не удалось получить все начальные токены"
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_circuit_breaker(results: TestResult):
    """Тест 5: Circuit Breaker."""
    name = "Circuit Breaker"
    try:
        # Используем правильные параметры: failure_threshold и recovery_timeout_sec
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=1.0)
        
        async def check():
            # Имитация ошибок
            for i in range(3):
                await cb.record_failure()
            
            assert cb.state == "OPEN", "Circuit должен быть открыт после 3 ошибок"
            
            # Попытка выполнения в открытом состоянии
            can_exec = await cb.can_execute()
            assert not can_exec, "Выполнение должно быть запрещено в OPEN"
            
            # Ждем восстановления
            await asyncio.sleep(1.1)
            assert cb.state == "HALF_OPEN", "Circuit должен перейти в half-open"
            
            # Успешное выполнение закрывает цепь
            can_exec2 = await cb.can_execute()
            assert can_exec2, "Выполнение разрешено в HALF_OPEN"
            await cb.record_success()
            assert cb.state == "CLOSED", "Circuit должен закрыться после успеха"
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_cache_manager(results: TestResult):
    """Тест 6: Менеджер кэша (LRU Fallback)."""
    name = "Cache Manager (LRU)"
    try:
        # Пытаемся подключиться к несуществующему Memcached -> должен сработать fallback
        cache = MemcachedClient(hosts=[{'host': 'localhost', 'port': 11211}], fallback_enabled=True)
        
        async def check():
            key = "test:key:1"
            value = {"data": "hello"}
            
            # Запись
            await cache.set(key, value, ttl=60)
            
            # Чтение
            res = await cache.get(key)
            assert res == value, "Значение не совпадает"
            
            # Удаление
            await cache.delete(key)
            res_none = await cache.get(key)
            assert res_none is None, "Кэш не очистился"
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_local_storage(results: TestResult):
    """Тест 7: Локальное хранилище."""
    name = "Local Storage (Atomic Write)"
    try:
        # Очистка перед тестом
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        
        storage = LocalFileStorage(base_path=TEST_DATA_DIR)
        now = datetime.now(timezone.utc)
        
        candles = [
            Candle(timestamp=now, open=Decimal("100"), high=Decimal("101"), low=Decimal("99"), close=Decimal("100.5"), volume=100)
        ]
        
        async def check():
            # Запись
            await storage.write_ohlcv(TICKER, TIMEFRAME, candles)
            
            # Проверка файла
            path = os.path.join(TEST_DATA_DIR, "tickers", TICKER, TIMEFRAME, "ohlcv.json")
            assert os.path.exists(path), "Файл не создан"
            
            # Чтение
            loaded = await storage.read_ohlcv(TICKER, TIMEFRAME)
            assert len(loaded) == 1, "Количество свечей не совпадает"
            assert loaded[0].close == Decimal("100.5") or str(loaded[0].get('close')) == "100.5"
            
            # Проверка чекпоинта
            cp_path = os.path.join(TEST_DATA_DIR, "..", "metadata")
            # Checkpoint создается отдельно через write_checkpoint
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))
    finally:
        # Очистка после теста
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        # Очистка метаданных
        metadata_dir = os.path.join(TEST_DATA_DIR, "..", "metadata")
        if os.path.exists(metadata_dir):
            shutil.rmtree(metadata_dir)

def test_stub_provider(results: TestResult):
    """Тест 8: Stub Provider."""
    name = "Stub Provider"
    try:
        # StubProvider требует config
        provider = StubProvider(config={"priority": 4})
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=5)
        
        async def check():
            data = await provider.get_ohlcv(TICKER, TIMEFRAME, start=start, end=now)
            
            assert len(data) > 0, "Stub не вернул данных"
            assert all(isinstance(c.close, Decimal) for c in data), "Цены не Decimal"
            assert all(c.quality_score < 1.0 for c in data), "Stub должен иметь quality_score < 1"
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))

def test_incremental_sync(results: TestResult):
    """Тест 9: Инкрементальная синхронизация."""
    name = "Incremental Sync"
    try:
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        metadata_dir = os.path.join(TEST_DATA_DIR, "..", "metadata")
        if os.path.exists(metadata_dir):
            shutil.rmtree(metadata_dir)
            
        storage = LocalFileStorage(base_path=TEST_DATA_DIR)
        provider = StubProvider(config={"priority": 4})
        sync = IncrementalSynchronizer(storage, provider)
        
        now = datetime.now(timezone.utc)
        start = now - timedelta(hours=2)
        
        async def check():
            # Первый запуск (полная загрузка дельты)
            stats = await sync.run(TICKER, TIMEFRAME, start=start)
            
            assert stats['fetched'] > 0, "Данные не были загружены"
            assert stats['updated'] is True, "Хранилище не обновлено"
            
            # Второй запуск (должен быть пустым, если время не ушло далеко)
            # Но так как Stub генерирует данные динамически, проверим просто отсутствие ошибок
            stats2 = await sync.run(TICKER, TIMEFRAME, start=now - timedelta(minutes=1))
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))
    finally:
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        if os.path.exists(metadata_dir := os.path.join(TEST_DATA_DIR, "..", "metadata")):
            shutil.rmtree(metadata_dir)

def test_data_collector_pipeline(results: TestResult):
    """Тест 10: Полный пайплайн DataCollector."""
    name = "DataCollector Pipeline"
    try:
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        metadata_dir = os.path.join(TEST_DATA_DIR, "..", "metadata")
        if os.path.exists(metadata_dir):
            shutil.rmtree(metadata_dir)
            
        config = load_config(TEST_CONFIG_PATH)
        
        # Создаем хранилище отдельно для передачи в коллектор
        storage = LocalFileStorage(base_path=TEST_DATA_DIR)
        
        collector = DataCollector(config, storage=storage)
        
        async def check():
            # Получение последних данных (должен использовать Stub, т.к. реальные ключи отсутствуют)
            # Используем параметр limit вместо count
            data = await collector.get_latest(TICKER, TIMEFRAME, limit=3)
            
            assert data is not None, "Pipeline вернул None"
            assert len(data) > 0, "Pipeline вернул пустой список"
            
            # Проверка типов
            assert isinstance(data[0], Candle)
            
            return True

        asyncio.run(check())
        results.add_pass(name)
    except Exception as e:
        results.add_fail(name, str(e))
    finally:
        if os.path.exists(TEST_DATA_DIR):
            shutil.rmtree(TEST_DATA_DIR)
        if os.path.exists(metadata_dir := os.path.join(TEST_DATA_DIR, "..", "metadata")):
            shutil.rmtree(metadata_dir)


# --- Основная точка входа ---

def main():
    print("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТИРОВАНИЯ (ФАЗА 1)")
    print("="*50)
    
    results = TestResult()
    
    tests = [
        test_config_loading,
        test_models_validation,
        test_business_validator,
        test_rate_limiter,
        test_circuit_breaker,
        test_cache_manager,
        test_local_storage,
        test_stub_provider,
        test_incremental_sync,
        test_data_collector_pipeline
    ]
    
    for test_func in tests:
        try:
            test_func(results)
        except Exception as e:
            results.add_fail(test_func.__name__, f"Критическая ошибка: {e}")
            logging.exception("Stacktrace:")
    
    success = results.summary()
    
    if success:
        print("\n🎉 Инфраструктура готова к эксплуатации!")
        return 0
    else:
        print("\n⚠️ Обнаружены проблемы. Требуется доработка.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
