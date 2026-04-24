"""
Базовый абстрактный класс провайдера данных.

Определяет контракт для всех источников данных:
- T-Tech (Tinkoff Invest API)
- MoexAlgo
- CBR (Центральный Банк РФ)
- Stub (заглушка для fallback)

Включает реализации:
- TokenBucketRateLimiter для контроля частоты запросов
- CircuitBreaker для обработки сбоев источников
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from ..models import (
    Candle, L2OrderBook, Trade, MacroCandle, 
    Fundamental, CorporateEvent, DataSource, Timeframe
)


class ProviderState(Enum):
    """Состояние провайдера."""
    ACTIVE = "active"
    DEGRADED = "degraded"
    FAILED = "failed"
    HALF_OPEN = "half_open"


@dataclass
class RateLimitState:
    """Состояние rate limiter."""
    tokens: float = 0.0
    last_update: float = field(default_factory=time.monotonic)
    burst_size: int = 10
    refill_rate: float = 10.0  # tokens per second


class TokenBucketRateLimiter:
    """
    Rate limiter на основе token bucket алгоритма.
    
    Позволяет bursts запросов до burst_size, затем ограничивает
    до refill_rate запросов в секунду.
    
    Attributes:
        rate: Количество запросов в секунду (refill rate).
        burst: Максимальный размер burst.
    """
    
    def __init__(self, rate: float = 10.0, burst: int = 20):
        """
        Инициализация rate limiter.
        
        Args:
            rate: Количество запросов в секунду.
            burst: Максимальное количество запросов в burst.
        """
        self._state = RateLimitState(
            tokens=float(burst),
            burst_size=burst,
            refill_rate=rate
        )
        self._lock = asyncio.Lock()
    
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Запрос токенов.
        
        Args:
            tokens: Количество требуемых токенов.
            
        Returns:
            True если токены получены, False если превышен лимит.
        """
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._state.last_update
            
            # Пополнение токенов
            self._state.tokens = min(
                self._state.burst_size,
                self._state.tokens + elapsed * self._state.refill_rate
            )
            self._state.last_update = now
            
            if self._state.tokens >= tokens:
                self._state.tokens -= tokens
                return True
            return False
    
    async def wait_for_token(self, tokens: int = 1) -> None:
        """
        Асинхронное ожидание доступности токенов.
        
        Args:
            tokens: Количество требуемых токенов.
        """
        while not await self.acquire(tokens):
            # Вычисление времени ожидания
            tokens_needed = tokens - self._state.tokens
            wait_time = tokens_needed / self._state.refill_rate
            await asyncio.sleep(min(wait_time, 1.0))


@dataclass
class CircuitBreakerState:
    """Состояние circuit breaker."""
    failures: int = 0
    last_failure_time: Optional[float] = None
    state: ProviderState = ProviderState.ACTIVE
    half_open_requests: int = 0


class CircuitBreaker:
    """
    Circuit breaker для обработки сбоев источников.
    
    Состояния:
    - ACTIVE: Нормальная работа, запросы проходят
    - FAILED: Цепь разомкнута, запросы блокируются
    - HALF_OPEN: Проверка восстановления, ограниченное число запросов
    
    Attributes:
        failure_threshold: Порог ошибок для размыкания цепи.
        recovery_timeout: Время до попытки восстановления (сек).
        half_open_requests: Количество тестовых запросов в HALF_OPEN.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout_sec: float = 30.0,
        half_open_requests: int = 3
    ):
        """
        Инициализация circuit breaker.
        
        Args:
            failure_threshold: Количество ошибок для размыкания.
            recovery_timeout_sec: Время восстановления в секундах.
            half_open_requests: Тестовых запросов в HALF_OPEN.
        """
        self._state = CircuitBreakerState()
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout_sec
        self._half_open_requests = half_open_requests
        self._lock = asyncio.Lock()
    
    async def can_execute(self) -> bool:
        """
        Проверка возможности выполнения запроса.
        
        Returns:
            True если запрос разрешён, False если цепь разомкнута.
        """
        async with self._lock:
            if self._state.state == ProviderState.ACTIVE:
                return True
            
            if self._state.state == ProviderState.FAILED:
                # Проверка тайм-аута восстановления
                if self._state.last_failure_time is None:
                    return True
                
                elapsed = time.monotonic() - self._state.last_failure_time
                if elapsed >= self._recovery_timeout:
                    self._state.state = ProviderState.HALF_OPEN
                    self._state.half_open_requests = self._half_open_requests
                    return True
                return False
            
            if self._state.state == ProviderState.HALF_OPEN:
                if self._state.half_open_requests > 0:
                    self._state.half_open_requests -= 1
                    return True
                return False
            
            return False
    
    async def record_success(self) -> None:
        """Запись успешного выполнения."""
        async with self._lock:
            if self._state.state == ProviderState.HALF_OPEN:
                self._state.state = ProviderState.ACTIVE
            self._state.failures = 0
    
    async def record_failure(self) -> None:
        """Запись ошибки выполнения."""
        async with self._lock:
            self._state.failures += 1
            self._state.last_failure_time = time.monotonic()
            
            if self._state.failures >= self._failure_threshold:
                self._state.state = ProviderState.FAILED
            
            if self._state.state == ProviderState.HALF_OPEN:
                # Ошибка в HALF_OPEN → возврат в FAILED
                self._state.state = ProviderState.FAILED
    
    @property
    def state(self) -> ProviderState:
        """Текущее состояние."""
        return self._state.state


class DataProvider(ABC):
    """
    Абстрактный базовый класс провайдера данных.
    
    Определяет контракт для всех источников данных.
    Все методы асинхронные.
    
    Example:
        >>> provider = TtechProvider(config)
        >>> candles = await provider.get_ohlcv("SNGS", Timeframe.D1, from_dt, to_dt)
    """
    
    def __init__(
        self,
        name: str,
        priority: int = 1,
        rate_limit_config: Optional[Dict[str, Any]] = None,
        circuit_breaker_config: Optional[Dict[str, Any]] = None
    ):
        """
        Инициализация провайдера.
        
        Args:
            name: Название провайдера.
            priority: Приоритет (меньше = выше приоритет).
            rate_limit_config: Конфигурация rate limiter.
            circuit_breaker_config: Конфигурация circuit breaker.
        """
        self.name = name
        self.priority = priority
        self._state = ProviderState.ACTIVE
        
        # Rate limiter
        rl_config = rate_limit_config or {}
        self._rate_limiter = TokenBucketRateLimiter(
            rate=rl_config.get('requests_per_second', 10.0),
            burst=rl_config.get('burst_size', 20)
        )
        
        # Circuit breaker
        cb_config = circuit_breaker_config or {}
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=cb_config.get('failure_threshold', 5),
            recovery_timeout_sec=cb_config.get('recovery_timeout_sec', 30.0),
            half_open_requests=cb_config.get('half_open_requests', 3)
        )
    
    @abstractmethod
    async def get_ohlcv(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Candle]:
        """
        Получение OHLCV свечей.
        
        Args:
            instrument: Тикер инструмента.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список свечей.
        """
        pass
    
    @abstractmethod
    async def get_orderbook(
        self,
        instrument: str,
        depth: int = 10
    ) -> L2OrderBook:
        """
        Получение стакана заявок L2.
        
        Args:
            instrument: Тикер инструмента.
            depth: Глубина стакана.
            
        Returns:
            Стакан заявок.
        """
        pass
    
    @abstractmethod
    async def get_trades(
        self,
        instrument: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Trade]:
        """
        Получение тиковых сделок.
        
        Args:
            instrument: Тикер инструмента.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список сделок.
        """
        pass
    
    @abstractmethod
    async def get_macro(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[MacroCandle]:
        """
        Получение макро-данных.
        
        Args:
            instrument: Макро-инструмент (USD_RUB, BRENT, etc).
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список макро-свечей.
        """
        pass
    
    @abstractmethod
    async def get_fundamentals(
        self,
        ticker: str
    ) -> List[Fundamental]:
        """
        Получение фундаментальных показателей.
        
        Args:
            ticker: Тикер компании.
            
        Returns:
            Список фундаментальных данных.
        """
        pass
    
    @abstractmethod
    async def get_corporate_events(
        self,
        ticker: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[CorporateEvent]:
        """
        Получение корпоративных событий.
        
        Args:
            ticker: Тикер компании.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список событий.
        """
        pass
    
    @abstractmethod
    def supports(self, data_type: str, instrument: str) -> bool:
        """
        Проверка поддержки типа данных и инструмента.
        
        Args:
            data_type: Тип данных (ohlcv, lob, macro, etc).
            instrument: Инструмент.
            
        Returns:
            True если поддерживается.
        """
        pass
    
    @abstractmethod
    def get_rate_limits(self) -> Dict[str, Any]:
        """
        Получение текущих лимитов запросов.
        
        Returns:
            Информация о лимитах.
        """
        pass
    
    async def _execute_with_protection(self, coro):
        """
        Выполнение запроса с защитой (rate limit + circuit breaker).
        
        Args:
            coro: Асинхронная корутина для выполнения.
            
        Returns:
            Результат выполнения.
            
        Raises:
            Exception: Если circuit breaker разомкнут или ошибка выполнения.
        """
        # Проверка circuit breaker
        if not await self._circuit_breaker.can_execute():
            raise RuntimeError(
                f"Circuit breaker OPEN for {self.name}. "
                f"State: {self._circuit_breaker.state}"
            )
        
        # Ожидание rate limit
        await self._rate_limiter.wait_for_token()
        
        try:
            result = await coro
            await self._circuit_breaker.record_success()
            return result
        except Exception as e:
            await self._circuit_breaker.record_failure()
            raise
    
    @property
    def is_available(self) -> bool:
        """Проверка доступности провайдера."""
        return self._circuit_breaker.state != ProviderState.FAILED
    
    @property
    def circuit_breaker_state(self) -> ProviderState:
        """Текущее состояние circuit breaker."""
        return self._circuit_breaker.state


class ProviderRouter:
    """
    Маршрутизатор запросов к провайдерам.
    
    Автоматически выбирает доступный провайдер с наивысшим приоритетом.
    Поддерживает fallback цепочку при сбоях.
    
    Example:
        >>> router = ProviderRouter([ttech, moexalgo, stub])
        >>> provider = router.get_provider('ohlcv', 'SNGS')
        >>> candles = await provider.get_ohlcv(...)
    """
    
    def __init__(self, providers: List[DataProvider]):
        """
        Инициализация маршрутизатора.
        
        Args:
            providers: Список провайдеров.
        """
        self._providers = sorted(providers, key=lambda p: p.priority)
    
    def get_provider(
        self,
        data_type: str,
        instrument: str
    ) -> Optional[DataProvider]:
        """
        Получение доступного провайдера для типа данных.
        
        Args:
            data_type: Тип данных.
            instrument: Инструмент.
            
        Returns:
            Провайдер или None если все недоступны.
        """
        for provider in self._providers:
            if provider.is_available and provider.supports(data_type, instrument):
                return provider
        return None
    
    def get_all_providers(self) -> List[DataProvider]:
        """Получение всех провайдеров."""
        return self._providers.copy()
    
    async def execute_with_fallback(
        self,
        data_type: str,
        instrument: str,
        method_name: str,
        *args,
        **kwargs
    ) -> Any:
        """
        Выполнение метода с автоматическим fallback.
        
        Args:
            data_type: Тип данных.
            instrument: Инструмент.
            method_name: Имя метода для вызова.
            *args: Позиционные аргументы метода.
            **kwargs: Именованные аргументы метода.
            
        Returns:
            Результат выполнения.
            
        Raises:
            RuntimeError: Если все провайдеры недоступны.
        """
        for provider in self._providers:
            if not provider.is_available:
                continue
            if not provider.supports(data_type, instrument):
                continue
            
            try:
                method = getattr(provider, method_name)
                return await method(*args, **kwargs)
            except Exception as e:
                # Логирование ошибки и переход к следующему провайдеру
                continue
        
        raise RuntimeError(
            f"All providers failed for {data_type}:{instrument}"
        )
