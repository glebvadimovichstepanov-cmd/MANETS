"""
T-Tech (Tinkoff Invest API) провайдер данных.

Асинхронный клиент для Tinkoff Invest API gRPC/REST.
Поддержка:
- OHLCV свечи
- Стаканы L2
- Трейды
- Фундаментальные данные
- Корпоративные события

Особенности:
- Async gRPC клиент
- Конвертация Quotation → Decimal
- Retry с exponential backoff
- Rate limiting и circuit breaker
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..models import (
    Candle, L2OrderBook, L2OrderLevel, Trade, TradeSide,
    MacroCandle, Fundamental, CorporateEvent,
    EventType, EventStatus, DataSource, Timeframe
)
from .base import DataProvider, ProviderState

logger = logging.getLogger(__name__)


class TtechProvider(DataProvider):
    """
    Провайдер для Tinkoff Invest API.
    
    Поддерживает работу через gRPC (основной) и REST (fallback).
    Автоматически конвертирует Quotation в Decimal.
    
    Example:
        >>> config = {...}
        >>> provider = TtechProvider(config)
        >>> candles = await provider.get_ohlcv("SNGS", Timeframe.D1, from_dt, to_dt)
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        rate_limit_config: Optional[Dict[str, Any]] = None,
        circuit_breaker_config: Optional[Dict[str, Any]] = None
    ):
        """
        Инициализация T-Tech провайдера.
        
        Args:
            config: Конфигурация провайдера (endpoints, auth_token_env).
            rate_limit_config: Конфигурация rate limiter.
            circuit_breaker_config: Конфигурация circuit breaker.
        """
        super().__init__(
            name="ttech",
            priority=config.get('priority', 1),
            rate_limit_config=rate_limit_config,
            circuit_breaker_config=circuit_breaker_config
        )
        
        self.endpoints = config.get('endpoints', {})
        self.grpc_endpoint = self.endpoints.get('grpc', 'invest-public-api.tinkoff.ru:443')
        self.rest_endpoint = self.endpoints.get('rest', 'https://invest-public-api.tinkoff.ru/rest')
        
        # Получение токена из env (приоритет: INVEST_TOKEN для t-tech-investments)
        token_env = config.get('auth_token_env', 'INVEST_TOKEN')
        self.token = os.environ.get(token_env, '')
        
        if not self.token:
            logger.warning(
                f"Auth token not found in env variable '{token_env}'. "
                "Set INVEST_TOKEN environment variable for T-Tech Investments API access."
            )
        
        # Клиенты (ленивая инициализация)
        self._grpc_client = None
        self._session = None
        
        # Retry конфигурация
        self._retry_max_attempts = 3
        self._retry_base_delay = 1.0
        self._retry_max_delay = 60.0
        
        # Поддерживаемые инструменты (будет заполнено из конфига)
        self._supported_tickers = set()
        self._supported_macro = set()
    
    def _quotation_to_decimal(self, quotation: Any) -> Decimal:
        """
        Конвертация Tinkoff Quotation в Decimal.
        
        Quotation имеет поля units (целая часть) и nano (наночастицы).
        
        Args:
            quotation: Quotation объект от API.
            
        Returns:
            Decimal значение.
        """
        if quotation is None:
            return Decimal('0')
        
        units = getattr(quotation, 'units', 0) or 0
        nano = getattr(quotation, 'nano', 0) or 0
        
        # Конвертация: units + nano / 1e9
        value = Decimal(str(units)) + Decimal(str(nano)) / Decimal('1e9')
        return value
    
    def _timestamp_to_datetime(self, ts: Any) -> datetime:
        """
        Конвертация timestamp в datetime.
        
        Args:
            ts: Timestamp объект или int.
            
        Returns:
            datetime объект (UTC).
        """
        if ts is None:
            return datetime.utcnow()
        
        if hasattr(ts, 'seconds'):
            # Tinkoff Timestamp
            return datetime.fromtimestamp(ts.seconds, tz=None)
        
        if isinstance(ts, int):
            return datetime.fromtimestamp(ts, tz=None)
        
        if isinstance(ts, str):
            # ISO формат
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        
        return datetime.utcnow()
    
    async def _get_grpc_client(self):
        """Ленивая инициализация gRPC клиента."""
        if self._grpc_client is None:
            # Здесь будет реальная инициализация grpc клиента
            # Для MVP оставляем заглушку
            logger.debug("gRPC client initialization (stub)")
        return self._grpc_client
    
    async def _get_http_session(self):
        """Ленивая инициализация HTTP сессии."""
        if self._session is None:
            import aiohttp
            self._session = aiohttp.ClientSession(
                headers={
                    'Authorization': f'Bearer {self.token}',
                    'Content-Type': 'application/json'
                },
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session
    
    async def close(self):
        """Закрытие соединений."""
        if self._session:
            await self._session.close()
            self._session = None
    
    async def _retry_with_backoff(self, coro, operation: str = "operation"):
        """
        Выполнение с retry и exponential backoff.
        
        Args:
            coro: Корутина для выполнения.
            operation: Название операции для логирования.
            
        Returns:
            Результат выполнения.
            
        Raises:
            Exception: Если все попытки исчерпаны.
        """
        last_exception = None
        
        for attempt in range(1, self._retry_max_attempts + 1):
            try:
                return await coro
            except Exception as e:
                last_exception = e
                
                if attempt == self._retry_max_attempts:
                    break
                
                # Exponential backoff
                delay = min(
                    self._retry_base_delay * (2 ** (attempt - 1)),
                    self._retry_max_delay
                )
                
                logger.warning(
                    f"{operation} failed (attempt {attempt}/{self._retry_max_attempts}): {e}. "
                    f"Retrying in {delay:.2f}s"
                )
                
                await asyncio.sleep(delay)
        
        raise last_exception
    
    async def get_ohlcv(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Candle]:
        """
        Получение OHLCV свечей от T-Tech.
        
        Args:
            instrument: Тикер инструмента (SNGS, GAZP, etc).
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список свечей.
        """
        async def _fetch():
            # Реальная реализация будет использовать gRPC GetCandles
            # Для MVP возвращаем пустой список
            logger.debug(
                f"T-Tech: Fetching OHLCV for {instrument} {timeframe.value} "
                f"from {from_dt} to {to_dt}"
            )
            
            # Здесь будет вызов API
            # candles_response = await client.get_candles(...)
            
            return []
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_ohlcv:{instrument}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_ohlcv failed for {instrument}: {e}")
            raise
    
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
        async def _fetch():
            logger.debug(f"T-Tech: Fetching orderbook for {instrument} depth={depth}")
            
            # Реальная реализация: gRPC GetOrderBook
            # Для MVP заглушка
            return L2OrderBook(
                timestamp=datetime.utcnow(),
                bids=[],
                asks=[],
                source=DataSource.TTECH
            )
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_orderbook:{instrument}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_orderbook failed for {instrument}: {e}")
            raise
    
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
        async def _fetch():
            logger.debug(f"T-Tech: Fetching trades for {instrument}")
            
            # Реальная реализация: gRPC GetTrades
            return []
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_trades:{instrument}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_trades failed for {instrument}: {e}")
            raise
    
    async def get_macro(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[MacroCandle]:
        """
        Получение макро-данных.
        
        T-Tech не предоставляет макро-данные напрямую,
        поэтому этот метод возвращает пустой список.
        Используйте CBR provider для макро.
        
        Args:
            instrument: Макро-инструмент.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Пустой список (T-Tech не поддерживает макро).
        """
        logger.debug(f"T-Tech: Macro data not supported for {instrument}")
        return []
    
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
        async def _fetch():
            logger.debug(f"T-Tech: Fetching fundamentals for {ticker}")
            
            # Реальная реализация: gRPC GetAcademy
            return []
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_fundamentals:{ticker}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_fundamentals failed for {ticker}: {e}")
            raise
    
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
        async def _fetch():
            logger.debug(f"T-Tech: Fetching corporate events for {ticker}")
            
            # Реальная реализация: gRPC GetEvents
            return []
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_corporate_events:{ticker}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_corporate_events failed for {ticker}: {e}")
            raise
    
    def supports(self, data_type: str, instrument: str) -> bool:
        """
        Проверка поддержки типа данных.
        
        T-Tech поддерживает:
        - ohlcv: все тикеры
        - lob: все тикеры
        - trades: все тикеры
        - fundamentals: акции
        - events: акции
        - macro: НЕ поддерживается
        
        Args:
            data_type: Тип данных.
            instrument: Инструмент.
            
        Returns:
            True если поддерживается.
        """
        unsupported_macro = ['macro']
        if data_type in unsupported_macro and instrument in ['USD_RUB', 'BRENT', 'CBR_KEY_RATE']:
            return False
        
        supported_types = ['ohlcv', 'lob', 'trades', 'fundamentals', 'events']
        return data_type in supported_types
    
    def get_rate_limits(self) -> Dict[str, Any]:
        """Получение текущих лимитов запросов."""
        return {
            'provider': self.name,
            'requests_per_second': self._rate_limiter._state.refill_rate,
            'burst_size': self._rate_limiter._state.burst_size,
            'available_tokens': self._rate_limiter._state.tokens,
            'circuit_breaker_state': self.circuit_breaker_state.value
        }
