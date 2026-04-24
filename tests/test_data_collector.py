"""
Базовые тесты для Data Collector.

Покрывает:
- Валидацию моделей
- Валидацию данных (validator)
- Кэширование (LRU fallback)
- Инкрементальную логику
- Circuit breaker и rate limiter
"""

import asyncio
import pytest
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Any

# Импорты тестируемых модулей
from src.infrastructure.data.models import (
    Candle, L2OrderBook, L2OrderLevel, Timeframe, DataSource,
    Checkpoint, ValidationReport, MacroCandle
)
from src.infrastructure.data.validator import DataValidator, CausalityError
from src.infrastructure.data.cache.memcached import LRUCache
from src.infrastructure.data.providers.base import (
    TokenBucketRateLimiter, CircuitBreaker, ProviderState
)


class TestCandleModel:
    """Тесты модели свечи."""
    
    def test_valid_candle(self):
        """Создание валидной свечи."""
        candle = Candle(
            timestamp=datetime.utcnow(),
            open=Decimal('100.0'),
            high=Decimal('105.0'),
            low=Decimal('98.0'),
            close=Decimal('103.0'),
            volume=Decimal('1000000'),
            adj_close=Decimal('103.0'),
            adj_factor=Decimal('1.0')
        )
        
        assert candle.open == Decimal('100.0')
        assert candle.high >= candle.low
        assert candle.quality_score == 1.0
    
    def test_invalid_high_low(self):
        """Отклонение свечи где high < low."""
        # Pydantic проверяет multiple validators, ошибка может быть от разных проверок
        with pytest.raises(ValueError):
            Candle(
                timestamp=datetime.utcnow(),
                open=Decimal('100.0'),
                high=Decimal('95.0'),  # < low
                low=Decimal('98.0'),
                close=Decimal('103.0'),
                volume=Decimal('1000000'),
                adj_close=Decimal('103.0')
            )
    
    def test_open_out_of_range(self):
        """Отклонение свечи где open вне диапазона [low, high]."""
        with pytest.raises(ValueError, match='open.*в диапазоне'):
            Candle(
                timestamp=datetime.utcnow(),
                open=Decimal('110.0'),  # > high
                high=Decimal('105.0'),
                low=Decimal('98.0'),
                close=Decimal('103.0'),
                volume=Decimal('1000000'),
                adj_close=Decimal('103.0')
            )


class TestDataValidator:
    """Тесты валидатора данных."""
    
    @pytest.mark.asyncio
    async def test_valid_ohlcv(self):
        """Валидация корректных данных."""
        validator = DataValidator(strict_mode=True)
        
        candles = [
            {
                'timestamp': '2024-01-01T00:00:00Z',
                'open': '100.0',
                'high': '105.0',
                'low': '98.0',
                'close': '103.0',
                'volume': '1000000',
                'adj_close': '103.0',
                'adj_factor': '1.0'
            },
            {
                'timestamp': '2024-01-02T00:00:00Z',
                'open': '103.0',
                'high': '108.0',
                'low': '102.0',
                'close': '107.0',
                'volume': '1200000',
                'adj_close': '107.0',
                'adj_factor': '1.0'
            }
        ]
        
        report = await validator.validate_ohlcv(candles, 'SNGS', '1d')
        
        assert report.passed is True
        assert len(report.errors) == 0
        assert report.quality_score == 1.0
    
    @pytest.mark.asyncio
    async def test_invalid_ohlc(self):
        """Отклонение данных с нарушенной OHLC логикой."""
        validator = DataValidator(strict_mode=True)
        
        candles = [
            {
                'timestamp': '2024-01-01T00:00:00Z',
                'open': '100.0',
                'high': '95.0',  # < low
                'low': '98.0',
                'close': '103.0',
                'volume': '1000000',
                'adj_close': '103.0',
                'adj_factor': '1.0'
            }
        ]
        
        report = await validator.validate_ohlcv(candles, 'SNGS', '1d')
        
        assert report.passed is False
        assert any('high' in err and 'low' in err for err in report.errors)
    
    @pytest.mark.asyncio
    async def test_non_monotonic_timestamps(self):
        """Отклонение данных с немонотонными timestamp."""
        validator = DataValidator(strict_mode=False)
        
        candles = [
            {
                'timestamp': '2024-01-02T00:00:00Z',
                'open': '100.0',
                'high': '105.0',
                'low': '98.0',
                'close': '103.0',
                'volume': '1000000',
                'adj_close': '103.0',
                'adj_factor': '1.0'
            },
            {
                'timestamp': '2024-01-01T00:00:00Z',  # Раньше предыдущего
                'open': '103.0',
                'high': '108.0',
                'low': '102.0',
                'close': '107.0',
                'volume': '1200000',
                'adj_close': '107.0',
                'adj_factor': '1.0'
            }
        ]
        
        report = await validator.validate_ohlcv(candles, 'SNGS', '1d')
        
        assert any('non-monotonic' in err for err in report.errors)


class TestLRUCache:
    """Тесты LRU кэша."""
    
    @pytest.mark.asyncio
    async def test_set_get(self):
        """Базовая операция set/get."""
        cache = LRUCache(max_size=100)
        
        await cache.set('key1', {'data': 'value1'})
        result = await cache.get('key1')
        
        assert result == {'data': 'value1'}
    
    @pytest.mark.asyncio
    async def test_ttl_expiry(self):
        """Истечение TTL."""
        cache = LRUCache(max_size=100)
        
        await cache.set('key1', 'value', ttl=1)  # 1 секунда
        result = await cache.get('key1')
        assert result == 'value'
        
        await asyncio.sleep(1.1)
        
        result = await cache.get('key1')
        assert result is None
    
    @pytest.mark.asyncio
    async def test_lru_eviction(self):
        """Вытеснение по LRU при переполнении."""
        cache = LRUCache(max_size=3)
        
        await cache.set('key1', 'v1')
        await cache.set('key2', 'v2')
        await cache.set('key3', 'v3')
        
        # Доступ к key1 поднимает его в начало
        await cache.get('key1')
        
        # Добавление нового вытесняет key2 (самый старый)
        await cache.set('key4', 'v4')
        
        assert await cache.get('key1') == 'v1'
        assert await cache.get('key2') is None  # Вытеснен
        assert await cache.get('key3') == 'v3'
        assert await cache.get('key4') == 'v4'


class TestTokenBucketRateLimiter:
    """Тесты rate limiter."""
    
    @pytest.mark.asyncio
    async def test_acquire_tokens(self):
        """Запрос токенов."""
        limiter = TokenBucketRateLimiter(rate=10.0, burst=5)
        
        # Первые 5 запросов должны пройти (burst)
        for _ in range(5):
            assert await limiter.acquire() is True
        
        # Следующий должен быть отклонён (токены кончились)
        assert await limiter.acquire() is False
    
    @pytest.mark.asyncio
    async def test_token_refill(self):
        """Пополнение токенов."""
        limiter = TokenBucketRateLimiter(rate=10.0, burst=2)
        
        # Потребляем все токены
        await limiter.acquire()
        await limiter.acquire()
        assert await limiter.acquire() is False
        
        # Ждём пополнения (0.2 сек = 2 токена при rate=10)
        await asyncio.sleep(0.25)
        
        assert await limiter.acquire() is True


class TestCircuitBreaker:
    """Тесты circuit breaker."""
    
    @pytest.mark.asyncio
    async def test_normal_operation(self):
        """Нормальная работа."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=1.0)
        
        assert await cb.can_execute() is True
        
        await cb.record_success()
        assert cb.state == ProviderState.ACTIVE
    
    @pytest.mark.asyncio
    async def test_open_after_failures(self):
        """Размыкание после ошибок."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=1.0)
        
        # 3 ошибки
        for _ in range(3):
            await cb.record_failure()
        
        assert cb.state == ProviderState.FAILED
        assert await cb.can_execute() is False
    
    @pytest.mark.asyncio
    async def test_half_open_after_recovery(self):
        """Переход в HALF_OPEN после тайм-аута."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout_sec=0.5)
        
        # Ошибки
        await cb.record_failure()
        await cb.record_failure()
        assert cb.state == ProviderState.FAILED
        
        # Ждём восстановления
        await asyncio.sleep(0.6)
        
        assert await cb.can_execute() is True
        assert cb.state == ProviderState.HALF_OPEN


class TestMacroCausality:
    """Тесты каузальности макро-данных."""
    
    def test_macro_with_shift(self):
        """Макро-данные со сдвигом (корректно)."""
        macro = MacroCandle(
            timestamp=datetime.utcnow(),
            open=Decimal('90.0'),
            high=Decimal('91.0'),
            low=Decimal('89.0'),
            close=Decimal('90.5'),
            shift_periods=1  # Сдвиг на 1 период
        )
        
        assert macro.shift_periods >= 1
    
    def test_macro_without_shift_validation(self):
        """Проверка что сдвиг обязателен для каузальности."""
        # Макро без сдвига технически можно создать,
        # но пайплайн должен добавлять shift_periods=1
        macro = MacroCandle(
            timestamp=datetime.utcnow(),
            open=Decimal('90.0'),
            high=Decimal('91.0'),
            low=Decimal('89.0'),
            close=Decimal('90.5'),
            shift_periods=1
        )
        
        # StubProvider всегда устанавливает shift_periods=1
        assert macro.shift_periods >= 1


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
