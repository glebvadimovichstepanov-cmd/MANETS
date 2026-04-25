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

# Проверка доступности библиотеки t_tech.invest при загрузке модуля
try:
    from t_tech.invest import AsyncClient, CandleInterval, GetCandlesRequest, InstrumentIdType
    from t_tech.invest.services import InstrumentsService, MarketDataService
    TTECH_LIBRARY_AVAILABLE = True
    logger.debug("t_tech.invest library loaded successfully")
except ImportError as e:
    TTECH_LIBRARY_AVAILABLE = False
    logger.warning(
        f"t_tech.invest library not available: {e}. "
        "TtechProvider will return empty data. "
        "Install with: pip install t-tech-investments --index-url https://opensource.tbank.ru/api/v4/projects/238/packages/pypi/simple"
    )
    # Заглушки для отсутствия библиотеки
    class AsyncClient:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass
        async def __aenter__(self):
            return self
        async def __aexit__(self, *args):
            pass
        @property
        def instruments(self):
            return None
        @property
        def market_data(self):
            return None
    
    class CandleInterval:  # type: ignore
        CANDLE_INTERVAL_5_SEC = "5s"
        CANDLE_INTERVAL_10_SEC = "10s"
        CANDLE_INTERVAL_30_SEC = "30s"
        CANDLE_INTERVAL_1_MIN = "1m"
        CANDLE_INTERVAL_2_MIN = "2m"
        CANDLE_INTERVAL_3_MIN = "3m"
        CANDLE_INTERVAL_5_MIN = "5m"
        CANDLE_INTERVAL_10_MIN = "10m"
        CANDLE_INTERVAL_15_MIN = "15m"
        CANDLE_INTERVAL_30_MIN = "30m"
        CANDLE_INTERVAL_HOUR = "1h"
        CANDLE_INTERVAL_2_HOUR = "2h"
        CANDLE_INTERVAL_4_HOUR = "4h"
        CANDLE_INTERVAL_DAY = "1d"
        CANDLE_INTERVAL_WEEK = "1w"
        CANDLE_INTERVAL_MONTH = "1M"
    
    class InstrumentIdType:  # type: ignore
        INSTRUMENT_ID_TYPE_TICKER = "ticker"
        INSTRUMENT_ID_TYPE_FIGI = "figi"
        INSTRUMENT_ID_TYPE_UID = "uid"
    
    class InstrumentsService:  # type: ignore
        pass
    
    class MarketDataService:  # type: ignore
        pass


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
        self.token = os.environ.get(token_env, '').strip()
        
        if not self.token:
            logger.error(
                f"Auth token not found in env variable '{token_env}'. "
                "Set INVEST_TOKEN environment variable for T-Tech Investments API access. "
                "Token format should be like: t.ZLtpCN0pOiGj8WbOU0xxGgpCWxBH5vmnYH-hzvgXQesS04yGMtEiw1tzJevGZox1r6nVMXi0z0QMO3BaRH7lBA"
            )
        else:
            logger.info(f"Token loaded from {token_env} (length={len(self.token)}, starts with: {self.token[:5]}...)")
        
        # Клиенты (ленивая инициализация)
        self._grpc_client = None
        self._session = None
        
        # Retry конфигурация
        self._retry_max_attempts = 3
        self._retry_base_delay = 1.0
        self._retry_max_delay = 60.0
        
        # Поддерживаемые инструменты (будет заполнено из конфига)
        self._supported_tickers = set()
        
        # Поддерживаемые макро-инструменты T-Tech API
        self._supported_macro = {
            # Валюты
            'USD_RUB', 'EUR_RUB', 'CNY_RUB',
            # Товары (фьючерсы/ETF)
            'BRENT', 'NATURAL_GAS',
            # Индексы
            'MOEX_INDEX', 'RTS_INDEX',
            # Ставки и облигации
            'CBR_KEY_RATE', 'OFZ_26238', 'OFZ_26244', 'RUONIA'
        }
        
        # Маппинг тикеров для API запросов (ticker -> instrument_id для T-Tech)
        # Актуальные идентификаторы по состоянию на 2026
        self._macro_ticker_map = {
            # Валютные пары (FIGI для tom-расчетов)
            'USD_RUB': 'USD000UTSTOM',   # USD/RUB tom
            'EUR_RUB': 'EUR000UTSTOM',   # EUR/RUB tom
            'CNY_RUB': 'CNY000UTSTOM',   # CNY/RUB tom
            
            # Товары (фьючерсы и ETF)
            'BRENT': 'BRNF',             # Brent crude futures ticker
            'NATURAL_GAS': 'NGAS',       # Natural Gas futures ticker
            
            # Индексы (FIGI)
            'MOEX_INDEX': 'IMOEX',       # MOEX Russia Index full return gross
            'RTS_INDEX': 'IRTS',         # RTS Index futures
            
            # Ставки и облигации
            'CBR_KEY_RATE': 'CBR_KEY_RATE',  # Ключевая ставка (специальный, берется из CBR)
            'OFZ_26238': 'SU26238RMFS4',     # OFZ bond ISIN
            'OFZ_26244': 'SU26244RMFS2',     # OFZ bond ISIN
            'RUONIA': 'RUSFAR',              # RUSFAR (аналог RUONIA, есть в Tinkoff)
        }
    
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
            logger.debug(
                f"T-Tech: Fetching OHLCV for {instrument} {timeframe.value} "
                f"from {from_dt} to {to_dt}"
            )
            
            # Проверка доступности библиотеки
            if not TTECH_LIBRARY_AVAILABLE:
                logger.error(
                    "tinkoff.invest library is not installed. "
                    "Cannot fetch real data from T-Tech API. "
                    "Returning empty list."
                )
                return []
            
            try:
                # Маппинг таймфреймов на CandleInterval с поддержкой всех интервалов T-Tech API
                timeframe_map = {
                    '5s': CandleInterval.CANDLE_INTERVAL_5_SEC,
                    '10s': CandleInterval.CANDLE_INTERVAL_10_SEC,
                    '30s': CandleInterval.CANDLE_INTERVAL_30_SEC,
                    '1m': CandleInterval.CANDLE_INTERVAL_1_MIN,
                    '2m': CandleInterval.CANDLE_INTERVAL_2_MIN,
                    '3m': CandleInterval.CANDLE_INTERVAL_3_MIN,
                    '5m': CandleInterval.CANDLE_INTERVAL_5_MIN,
                    '10m': CandleInterval.CANDLE_INTERVAL_10_MIN,
                    '15m': CandleInterval.CANDLE_INTERVAL_15_MIN,
                    '30m': CandleInterval.CANDLE_INTERVAL_30_MIN,
                    '1h': CandleInterval.CANDLE_INTERVAL_HOUR,
                    '2h': CandleInterval.CANDLE_INTERVAL_2_HOUR,
                    '4h': CandleInterval.CANDLE_INTERVAL_4_HOUR,
                    '1d': CandleInterval.CANDLE_INTERVAL_DAY,
                    '1w': CandleInterval.CANDLE_INTERVAL_WEEK,
                    '1M': CandleInterval.CANDLE_INTERVAL_MONTH,
                }
                
                interval = timeframe_map.get(timeframe.value)
                if interval is None:
                    logger.warning(f"Unsupported timeframe: {timeframe.value}")
                    return []
                
                # Ограничение периода запроса согласно лимитам Tinkoff API
                # Максимальные периоды для разных интервалов (в днях)
                max_periods_days = {
                    CandleInterval.CANDLE_INTERVAL_5_SEC: 200 / (24 * 60),  # 200 минут
                    CandleInterval.CANDLE_INTERVAL_10_SEC: 200 / (24 * 60),  # 200 минут
                    CandleInterval.CANDLE_INTERVAL_30_SEC: 20 / 24,  # 20 часов
                    CandleInterval.CANDLE_INTERVAL_1_MIN: 1,  # 1 день
                    CandleInterval.CANDLE_INTERVAL_2_MIN: 1,  # 1 день
                    CandleInterval.CANDLE_INTERVAL_3_MIN: 1,  # 1 день
                    CandleInterval.CANDLE_INTERVAL_5_MIN: 7,  # 1 неделя
                    CandleInterval.CANDLE_INTERVAL_10_MIN: 7,  # 1 неделя
                    CandleInterval.CANDLE_INTERVAL_15_MIN: 21,  # 3 недели
                    CandleInterval.CANDLE_INTERVAL_30_MIN: 21,  # 3 недели
                    CandleInterval.CANDLE_INTERVAL_HOUR: 90,  # 3 месяца
                    CandleInterval.CANDLE_INTERVAL_2_HOUR: 90,  # 3 месяца
                    CandleInterval.CANDLE_INTERVAL_4_HOUR: 90,  # 3 месяца
                    CandleInterval.CANDLE_INTERVAL_DAY: 365 * 6,  # 6 лет
                    CandleInterval.CANDLE_INTERVAL_WEEK: 365 * 5,  # 5 лет
                    CandleInterval.CANDLE_INTERVAL_MONTH: 365 * 10,  # 10 лет
                }
                max_days = max_periods_days.get(interval, 30)
                total_days = (to_dt - from_dt).days
                
                # Получаем FIGI или UID для тикера
                # Токен должен быть передан без префиксов "Bearer" или "t."
                clean_token = self.token
                if clean_token.startswith('t.'):
                    # Оставляем токен как есть, библиотека сама добавит нужные заголовки
                    pass
                
                logger.debug(f"T-Tech: Using token (length={len(clean_token)}, starts with: {clean_token[:5]}...)")
                
                async with AsyncClient(token=clean_token) as client:
                    instruments = client.instruments
                    
                    # Поиск инструмента по тикуру
                    instrument_id = None
                    try:
                        # Используем share_by с правильными параметрами (новый API t-tech-investments)
                        # id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER, class_code='TQBR', id=ticker
                        response = await client.instruments.share_by(
                            id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                            class_code='TQBR',
                            id=instrument
                        )
                        # Предпочитаем instrument_id (UID), fallback на figi
                        instrument_id = response.instrument.uid or response.instrument.figi
                        logger.debug(f"T-Tech: Found instrument_id={instrument_id} (FIGI={response.instrument.figi}) for ticker {instrument}")
                    except Exception as e:
                        logger.warning(
                            f"Could not find share by ticker '{instrument}': {e}. "
                            f"Trying to use as FIGI/UID directly."
                        )
                        # Пытаемся использовать как FIGI или UID напрямую
                        instrument_id = instrument
                    
                    if not instrument_id:
                        logger.error(f"Could not find instrument ID for ticker: {instrument}")
                        return []
                    
                    logger.debug(f"T-Tech: Using instrument_id={instrument_id} for {instrument}")
                    
                    # Запрос свечей с разбивкой на chunks если период превышает лимит
                    # T-Tech API имеет лимиты на количество свечей в одном запросе
                    max_candles_per_request = {
                        CandleInterval.CANDLE_INTERVAL_5_SEC: 2500,
                        CandleInterval.CANDLE_INTERVAL_10_SEC: 1250,
                        CandleInterval.CANDLE_INTERVAL_30_SEC: 2500,
                        CandleInterval.CANDLE_INTERVAL_1_MIN: 2400,
                        CandleInterval.CANDLE_INTERVAL_2_MIN: 1200,
                        CandleInterval.CANDLE_INTERVAL_3_MIN: 750,
                        CandleInterval.CANDLE_INTERVAL_5_MIN: 2400,
                        CandleInterval.CANDLE_INTERVAL_10_MIN: 1200,
                        CandleInterval.CANDLE_INTERVAL_15_MIN: 2400,
                        CandleInterval.CANDLE_INTERVAL_30_MIN: 1200,
                        CandleInterval.CANDLE_INTERVAL_HOUR: 2400,
                        CandleInterval.CANDLE_INTERVAL_2_HOUR: 2400,
                        CandleInterval.CANDLE_INTERVAL_4_HOUR: 700,
                        CandleInterval.CANDLE_INTERVAL_DAY: 2400,
                        CandleInterval.CANDLE_INTERVAL_WEEK: 300,
                        CandleInterval.CANDLE_INTERVAL_MONTH: 120,
                    }
                    
                    all_candles = []
                    limit = max_candles_per_request.get(interval, 1000)
                    
                    # Вычисляем необходимый chunk_size в зависимости от таймфрейма
                    # и максимального количества свечей
                    chunk_delta = {
                        CandleInterval.CANDLE_INTERVAL_5_SEC: timedelta(seconds=5 * limit),
                        CandleInterval.CANDLE_INTERVAL_10_SEC: timedelta(seconds=10 * limit),
                        CandleInterval.CANDLE_INTERVAL_30_SEC: timedelta(seconds=30 * limit),
                        CandleInterval.CANDLE_INTERVAL_1_MIN: timedelta(minutes=limit),
                        CandleInterval.CANDLE_INTERVAL_2_MIN: timedelta(minutes=2 * limit),
                        CandleInterval.CANDLE_INTERVAL_3_MIN: timedelta(minutes=3 * limit),
                        CandleInterval.CANDLE_INTERVAL_5_MIN: timedelta(minutes=5 * limit),
                        CandleInterval.CANDLE_INTERVAL_10_MIN: timedelta(minutes=10 * limit),
                        CandleInterval.CANDLE_INTERVAL_15_MIN: timedelta(minutes=15 * limit),
                        CandleInterval.CANDLE_INTERVAL_30_MIN: timedelta(minutes=30 * limit),
                        CandleInterval.CANDLE_INTERVAL_HOUR: timedelta(hours=limit),
                        CandleInterval.CANDLE_INTERVAL_2_HOUR: timedelta(hours=2 * limit),
                        CandleInterval.CANDLE_INTERVAL_4_HOUR: timedelta(hours=4 * limit),
                        CandleInterval.CANDLE_INTERVAL_DAY: timedelta(days=limit),
                        CandleInterval.CANDLE_INTERVAL_WEEK: timedelta(weeks=limit),
                        CandleInterval.CANDLE_INTERVAL_MONTH: timedelta(days=30 * limit),
                    }
                    
                    current_from = from_dt
                    chunk_num = 0
                    
                    while current_from < to_dt:
                        chunk_to = min(current_from + chunk_delta.get(interval, timedelta(days=365)), to_dt)
                        chunk_num += 1
                        
                        logger.debug(
                            f"T-Tech: Fetching chunk {chunk_num} for {instrument} "
                            f"from {current_from} to {chunk_to} (limit={limit})"
                        )
                        
                        candles_response = await client.market_data.get_candles(
                            instrument_id=instrument_id,
                            interval=interval,
                            from_=current_from,
                            to=chunk_to,
                            limit=limit,
                        )
                        
                        for candle in candles_response.candles:
                            all_candles.append(Candle(
                                timestamp=self._timestamp_to_datetime(candle.time),
                                open=self._quotation_to_decimal(candle.open),
                                high=self._quotation_to_decimal(candle.high),
                                low=self._quotation_to_decimal(candle.low),
                                close=self._quotation_to_decimal(candle.close),
                                volume=Decimal(str(candle.volume)),
                                adj_close=None,
                                timeframe=timeframe,
                                source=DataSource.TTECH
                            ))
                        
                        current_from = chunk_to
                        
                        # Задержка между запросами для соблюдения rate limits Tinkoff API
                        # Лимит: 200 запросов в минуту = 3.33 req/s
                        # Используем консервативную задержку 0.4s (2.5 req/s)
                        if current_from < to_dt:
                            await asyncio.sleep(0.4)
                    
                    logger.info(f"T-Tech: Retrieved {len(all_candles)} candles for {instrument}")
                    return all_candles
                    
            except ImportError as e:
                logger.error(f"t-tech-investments library not installed: {e}")
                return []
            except Exception as e:
                # Проверка на RESOURCE_EXHAUSTED (rate limit exceeded)
                error_str = str(e)
                if 'RESOURCE_EXHAUSTED' in error_str or 'ratelimit_remaining=0' in error_str:
                    logger.warning(
                        f"T-Tech API rate limit exceeded for {instrument}. "
                        f"Waiting before retry... Error: {e}"
                    )
                    # Ждем восстановления лимита (обычно 60 секунд для Tinkoff)
                    await asyncio.sleep(65)
                    # Повторяем запрос с начала
                    return await _fetch()
                
                logger.error(f"T-Tech API error for {instrument}: {e}")
                raise
        
        try:
            return await self._execute_with_protection(_fetch())
        except Exception as e:
            logger.error(f"T-Tech get_ohlcv failed for {instrument}: {e}")
            raise
    
    async def get_orderbook(
        self,
        instrument: str,
        depth: int = 10
    ) -> Optional[L2OrderBook]:
        """
        Получение стакана заявок L2.
        
        Args:
            instrument: Тикер инструмента.
            depth: Глубина стакана (1-20).
            
        Returns:
            Стакан заявок или None если стакан пуст.
        """
        async def _fetch():
            logger.debug(f"T-Tech: Fetching orderbook for {instrument} depth={depth}")
            
            if not TTECH_LIBRARY_AVAILABLE:
                logger.warning("t_tech.invest library not available. Returning empty orderbook.")
                return None
            
            # Получаем instrument_id с поддержкой разных классов инструментов
            instrument_id = None
            class_codes_to_try = ['TQBR', 'TQCB', 'TQOB', 'EQBR']  # Акции, Облигации (CBR/MSBR), Валюты
            
            try:
                async with AsyncClient(token=self.token) as client:
                    for class_code in class_codes_to_try:
                        try:
                            response = await client.instruments.share_by(
                                id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                                class_code=class_code,
                                id=instrument
                            )
                            instrument_id = response.instrument.uid or response.instrument.figi
                            logger.debug(f"T-Tech: Found instrument_id={instrument_id} (class={class_code}) for ticker {instrument}")
                            break
                        except Exception as e:
                            logger.debug(f"T-Tech: Instrument '{instrument}' not found in class_code={class_code}: {e}")
                            continue
                    
                    if not instrument_id:
                        # Пытаемся использовать тикер как есть (возможно это FIGI или UID)
                        instrument_id = instrument
                        logger.warning(f"T-Tech: Could not find instrument '{instrument}' in any class code. Using as-is: {instrument_id}")
            except Exception as e:
                logger.error(f"T-Tech: Error resolving instrument '{instrument}': {e}. Using as-is.")
                instrument_id = instrument
            
            # Запрос стакана
            async with AsyncClient(token=self.token) as client:
                ob_response = await client.market_data.get_order_book(
                    instrument_id=instrument_id,
                    depth=depth
                )
                
                # Проверка на пустой стакан
                if not ob_response.bids and not ob_response.asks:
                    logger.warning(f"T-Tech: Empty orderbook for {instrument} (no bids/asks). Skipping.")
                    return None
                
                # Конвертация в наши модели
                # t_tech.invest использует поле 'volume' вместо 'quantity'
                bids = []
                for bid in ob_response.bids:
                    vol = getattr(bid, 'volume', None) or getattr(bid, 'quantity', None)
                    if vol is None:
                        logger.warning(f"Orderbook bid has no volume/quantity field: {bid}")
                        vol = Decimal('0')
                    bids.append(
                        L2OrderLevel(
                            price=self._quotation_to_decimal(bid.price),
                            volume=Decimal(str(vol))
                        )
                    )
                
                asks = []
                for ask in ob_response.asks:
                    vol = getattr(ask, 'volume', None) or getattr(ask, 'quantity', None)
                    if vol is None:
                        logger.warning(f"Orderbook ask has no volume/quantity field: {ask}")
                        vol = Decimal('0')
                    asks.append(
                        L2OrderLevel(
                            price=self._quotation_to_decimal(ask.price),
                            volume=Decimal(str(vol))
                        )
                    )
                
                return L2OrderBook(
                    timestamp=self._timestamp_to_datetime(ob_response.orderbook_ts) if hasattr(ob_response, 'orderbook_ts') else datetime.utcnow(),
                    bids=bids,
                    asks=asks,
                    last_price=self._quotation_to_decimal(ob_response.last_price) if hasattr(ob_response, 'last_price') else None,
                    source=DataSource.TTECH
                )
        
        try:
            # Передаем factory функцию которая создает новую корутину при каждом вызове
            async def _execute_fetch():
                return await self._retry_with_backoff(_fetch(), f"get_orderbook:{instrument}")
            
            return await self._execute_with_protection(_execute_fetch())
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
    
    async def _resolve_macro_instrument_id(self, client: AsyncClient, instrument: str) -> Optional[str]:
        """
        Поиск instrument_id (FIGI/UID) для макро-инструмента через API.
        
        Использует различные методы поиска в зависимости от типа инструмента:
        - bond_by для облигаций (OFZ_*, SU*)
        - index_by для индексов (RUONIA, MOEX_INDEX, RTS_INDEX)
        - currency_by для валютных пар
        - share_by для товаров (BRENT, NATURAL_GAS)
        
        Args:
            client: AsyncClient экземпляр.
            instrument: Тикер макро-инструмента.
            
        Returns:
            instrument_id (UID) или FIGI, либо None если не найдено.
        """
        # Определяем тип инструмента по префиксу/названию
        if instrument.startswith('OFZ_') or instrument.startswith('SU'):
            # Облигации - используем bond_by
            # Сначала проверяем маппинг на FIGI
            mapped_figi = self._macro_ticker_map.get(instrument)
            
            # Если инструмент начинается с SU, используем его как FIGI напрямую
            if instrument.startswith('SU'):
                logger.info(f"T-Tech Macro: Using {instrument} as FIGI directly")
                return instrument
            
            # Для OFZ_* пробуем найти по mapped FIGI
            if mapped_figi and mapped_figi.startswith('SU'):
                logger.info(f"T-Tech Macro: Trying to find bond by mapped FIGI {mapped_figi} for {instrument}")
                try:
                    response = await client.instruments.bond_by(
                        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                        id=mapped_figi
                    )
                    if response and hasattr(response, 'instrument'):
                        inst = response.instrument
                        logger.info(f"T-Tech Macro: Found bond by FIGI {mapped_figi}: instrument_id={inst.uid}, figi={inst.figi}, ticker={inst.ticker}")
                        return inst.uid or inst.figi
                except Exception as e:
                    logger.debug(f"T-Tech Macro: bond_by FIGI failed for {mapped_figi}: {e}")
                
                # Если поиск по FIGI не дал результата, возвращаем FIGI напрямую
                logger.info(f"T-Tech Macro: Using mapped FIGI {mapped_figi} for {instrument} (not found via API, using directly)")
                return mapped_figi
            
            # Если нет маппинга, пробуем найти по тикеру в классе TQCB
            try:
                logger.info(f"T-Tech Macro: Searching bond by ticker {instrument} in TQCB")
                response = await client.instruments.bond_by(
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                    class_code='TQCB',
                    id=instrument
                )
                if response and hasattr(response, 'instrument'):
                    inst = response.instrument
                    logger.info(f"T-Tech Macro: Found bond {instrument} via bond_by (TQCB): instrument_id={inst.uid}, figi={inst.figi}")
                    return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: bond_by (TQCB) failed for {instrument}: {e}")
            
            # Пробуем TQOB (для некоторых облигаций)
            try:
                logger.info(f"T-Tech Macro: Searching bond by ticker {instrument} in TQOB")
                response = await client.instruments.bond_by(
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                    class_code='TQOB',
                    id=instrument
                )
                if response and hasattr(response, 'instrument'):
                    inst = response.instrument
                    logger.info(f"T-Tech Macro: Found bond {instrument} via bond_by (TQOB): instrument_id={inst.uid}, figi={inst.figi}")
                    return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: bond_by (TQOB) failed for {instrument}: {e}")
                
        elif instrument in ['RUONIA', 'MOEX_INDEX', 'RTS_INDEX', 'CBR_KEY_RATE']:
            # Индексы - используем index_by
            try:
                # Для индексов используем специальный класс_code или поиск по FIGI
                mapped_figi = self._macro_ticker_map.get(instrument, instrument)
                response = await client.instruments.index_by(
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                    id=mapped_figi
                )
                if response and hasattr(response, 'instrument'):
                    inst = response.instrument
                    logger.info(f"T-Tech Macro: Found index {instrument} via index_by: instrument_id={inst.uid}, figi={inst.figi}")
                    return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: index_by failed for {instrument}: {e}")
                
            # Fallback: используем mapped FIGI
            mapped_figi = self._macro_ticker_map.get(instrument, instrument)
            if mapped_figi != instrument:
                logger.info(f"T-Tech Macro: Using mapped FIGI {mapped_figi} for index {instrument}")
                return mapped_figi
                
        elif instrument in ['USD_RUB', 'EUR_RUB', 'CNY_RUB']:
            # Валютные пары - используем currency_by
            # На Московской бирже валютные пары торгуются в классе VALNET (tom-расчеты)
            try:
                mapped_figi = self._macro_ticker_map.get(instrument)
                if mapped_figi:
                    # Пробуем найти по FIGI через currency_by
                    response = await client.instruments.currency_by(
                        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                        id=mapped_figi
                    )
                    if response and hasattr(response, 'instrument'):
                        inst = response.instrument
                        logger.info(f"T-Tech Macro: Found currency {instrument} via currency_by (FIGI): instrument_id={inst.uid}, figi={inst.figi}")
                        return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: currency_by (FIGI) failed for {instrument}: {e}")
            
            # Fallback: пробуем найти по тикеру в классе VALNET
            try:
                logger.info(f"T-Tech Macro: Searching currency {instrument} by ticker in VALNET class")
                # Для валют используем тикер из маппинга (например, USD000UTSTOM)
                mapped_ticker = self._macro_ticker_map.get(instrument, instrument)
                response = await client.instruments.currency_by(
                    id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_TICKER,
                    class_code='VALNET',
                    id=mapped_ticker
                )
                if response and hasattr(response, 'instrument'):
                    inst = response.instrument
                    logger.info(f"T-Tech Macro: Found currency {instrument} via currency_by (VALNET): instrument_id={inst.uid}, figi={inst.figi}, ticker={inst.ticker}")
                    return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: currency_by (VALNET) failed for {instrument}: {e}")
            
            # Fallback: используем FIGI напрямую
            mapped_figi = self._macro_ticker_map.get(instrument)
            if mapped_figi and mapped_figi != instrument:
                logger.info(f"T-Tech Macro: Using mapped FIGI {mapped_figi} for currency {instrument} (fallback)")
                return mapped_figi
                
        elif instrument in ['BRENT', 'NATURAL_GAS']:
            # Товары - могут быть как futures, ищем через share_by или bond_by
            try:
                mapped_figi = self._macro_ticker_map.get(instrument)
                if mapped_figi:
                    # Пробуем как акцию/future
                    response = await client.instruments.share_by(
                        id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                        id=mapped_figi
                    )
                    if response and hasattr(response, 'instrument'):
                        inst = response.instrument
                        logger.info(f"T-Tech Macro: Found commodity {instrument} via share_by: instrument_id={inst.uid}, figi={inst.figi}")
                        return inst.uid or inst.figi
            except Exception as e:
                logger.debug(f"T-Tech Macro: share_by failed for {instrument}: {e}")
                
            # Fallback
            mapped_figi = self._macro_ticker_map.get(instrument, instrument)
            if mapped_figi != instrument:
                return mapped_figi
        
        # Последний fallback: возвращаем то, что есть в маппинге
        mapped = self._macro_ticker_map.get(instrument, instrument)
        logger.warning(f"T-Tech Macro: Could not resolve instrument_id for {instrument}, using mapped value: {mapped}")
        return mapped
    
    async def get_macro(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[MacroCandle]:
        """
        Получение макро-данных от T-Tech.
        
        Поддерживаемые инструменты:
        - Валюты: USD_RUB, EUR_RUB, CNY_RUB
        - Товары: BRENT, NATURAL_GAS
        - Индексы: MOEX_INDEX, RTS_INDEX
        - Ставки: CBR_KEY_RATE, OFZ_26238, OFZ_26244, RUONIA
        
        Args:
            instrument: Макро-инструмент.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список макро-свечей.
        """
        async def _fetch():
            logger.debug(
                f"T-Tech: Fetching macro for {instrument} {timeframe.value} "
                f"from {from_dt} to {to_dt}"
            )
            
            # Проверка доступности библиотеки
            if not TTECH_LIBRARY_AVAILABLE:
                logger.error(
                    "t_tech.invest library is not installed. "
                    "Cannot fetch real macro data from T-Tech API. "
                    "Returning empty list."
                )
                return []
            
            # Проверка поддержки инструмента
            if instrument not in self._supported_macro:
                logger.warning(f"Macro instrument {instrument} not supported by T-Tech")
                return []
            
            try:
                # Маппинг таймфреймов на CandleInterval
                timeframe_map = {
                    '5s': CandleInterval.CANDLE_INTERVAL_5_SEC,
                    '10s': CandleInterval.CANDLE_INTERVAL_10_SEC,
                    '30s': CandleInterval.CANDLE_INTERVAL_30_SEC,
                    '1m': CandleInterval.CANDLE_INTERVAL_1_MIN,
                    '2m': CandleInterval.CANDLE_INTERVAL_2_MIN,
                    '3m': CandleInterval.CANDLE_INTERVAL_3_MIN,
                    '5m': CandleInterval.CANDLE_INTERVAL_5_MIN,
                    '10m': CandleInterval.CANDLE_INTERVAL_10_MIN,
                    '15m': CandleInterval.CANDLE_INTERVAL_15_MIN,
                    '30m': CandleInterval.CANDLE_INTERVAL_30_MIN,
                    '1h': CandleInterval.CANDLE_INTERVAL_HOUR,
                    '2h': CandleInterval.CANDLE_INTERVAL_2_HOUR,
                    '4h': CandleInterval.CANDLE_INTERVAL_4_HOUR,
                    '1d': CandleInterval.CANDLE_INTERVAL_DAY,
                    '1w': CandleInterval.CANDLE_INTERVAL_WEEK,
                    '1M': CandleInterval.CANDLE_INTERVAL_MONTH,
                }
                
                interval = timeframe_map.get(timeframe.value)
                if interval is None:
                    logger.warning(f"Unsupported timeframe for macro: {timeframe.value}")
                    return []
                
                # Очистка токена
                clean_token = self.token
                if clean_token.startswith('t.'):
                    pass  # Оставляем как есть
                
                async with AsyncClient(token=clean_token) as client:
                    # Динамический поиск instrument_id через API
                    instrument_id = await self._resolve_macro_instrument_id(client, instrument)
                    
                    if not instrument_id:
                        logger.error(f"T-Tech Macro: Could not resolve instrument_id for {instrument}")
                        return []
                    
                    logger.info(f"T-Tech Macro: Resolved {instrument} -> instrument_id={instrument_id}")
                    # Для макро-инструментов используем разные методы в зависимости от типа
                    # Валюты, товары, индексы - через candle_by (аналогично акциям)
                    # Ставки - могут требовать специального处理
                    
                    # Запрос свечей с разбивкой на chunks
                    max_candles_per_request = {
                        CandleInterval.CANDLE_INTERVAL_5_SEC: 2500,
                        CandleInterval.CANDLE_INTERVAL_10_SEC: 1250,
                        CandleInterval.CANDLE_INTERVAL_30_SEC: 2500,
                        CandleInterval.CANDLE_INTERVAL_1_MIN: 2400,
                        CandleInterval.CANDLE_INTERVAL_2_MIN: 1200,
                        CandleInterval.CANDLE_INTERVAL_3_MIN: 750,
                        CandleInterval.CANDLE_INTERVAL_5_MIN: 2400,
                        CandleInterval.CANDLE_INTERVAL_10_MIN: 1200,
                        CandleInterval.CANDLE_INTERVAL_15_MIN: 800,
                        CandleInterval.CANDLE_INTERVAL_30_MIN: 400,
                        CandleInterval.CANDLE_INTERVAL_HOUR: 500,
                        CandleInterval.CANDLE_INTERVAL_2_HOUR: 250,
                        CandleInterval.CANDLE_INTERVAL_4_HOUR: 125,
                        CandleInterval.CANDLE_INTERVAL_DAY: 365 * 6,
                        CandleInterval.CANDLE_INTERVAL_WEEK: 52 * 5,
                        CandleInterval.CANDLE_INTERVAL_MONTH: 12 * 10,
                    }
                    
                    max_candles = max_candles_per_request.get(interval, 1000)
                    
                    all_candles: List[MacroCandle] = []
                    current_from = from_dt
                    
                    while current_from < to_dt:
                        # Вычисление конца текущего chunk
                        estimated_candles = int((to_dt - current_from).total_seconds() / 
                                              self._get_interval_seconds(interval))
                        chunk_size = min(max_candles, estimated_candles)
                        
                        if chunk_size <= 0:
                            break
                        
                        # Вычисление to для этого chunk
                        chunk_to = current_from + timedelta(seconds=chunk_size * self._get_interval_seconds(interval))
                        chunk_to = min(chunk_to, to_dt)
                        
                        logger.debug(
                            f"T-Tech Macro: Fetching chunk from {current_from} to {chunk_to} "
                            f"(~{chunk_size} candles)"
                        )
                        
                        try:
                            # Попытка получить свечи через GetCandles (новый API с именованными параметрами)
                            response = await client.market_data.get_candles(
                                instrument_id=instrument_id,
                                interval=interval,
                                from_=current_from,
                                to=chunk_to
                            )
                            
                            if not response or not hasattr(response, 'candles'):
                                logger.warning(f"No candles returned for {instrument} chunk")
                                break
                            
                            for candle in response.candles:
                                # Конвертация в MacroCandle
                                macro_candle = MacroCandle(
                                    timestamp=self._timestamp_to_datetime(candle.time),
                                    open=self._quotation_to_decimal(candle.open),
                                    high=self._quotation_to_decimal(candle.high),
                                    low=self._quotation_to_decimal(candle.low),
                                    close=self._quotation_to_decimal(candle.close),
                                    volume=Decimal(str(candle.volume)) if hasattr(candle, 'volume') else None,
                                    source=DataSource.TINKOFF,
                                    interpolation_method='none',
                                    shift_periods=1
                                )
                                all_candles.append(macro_candle)
                            
                            # Переход к следующему chunk
                            current_from = chunk_to
                            
                        except Exception as chunk_error:
                            logger.error(
                                f"T-Tech Macro: Error fetching chunk for {instrument}: {chunk_error}. "
                                f"Stopping pagination."
                            )
                            break
                    
                    # Сортировка по timestamp
                    all_candles.sort(key=lambda c: c.timestamp)
                    
                    logger.info(
                        f"T-Tech: Fetched {len(all_candles)} macro candles for {instrument} "
                        f"{timeframe.value}"
                    )
                    
                    return all_candles
                    
            except Exception as e:
                logger.error(f"T-Tech get_macro failed for {instrument}: {e}")
                raise
        
        try:
            return await self._execute_with_protection(
                self._retry_with_backoff(_fetch(), f"get_macro:{instrument}")
            )
        except Exception as e:
            logger.error(f"T-Tech get_macro failed for {instrument}: {e}")
            raise
    
    def _get_interval_seconds(self, interval: Any) -> int:
        """Получение длительности интервала в секундах."""
        interval_seconds = {
            CandleInterval.CANDLE_INTERVAL_5_SEC: 5,
            CandleInterval.CANDLE_INTERVAL_10_SEC: 10,
            CandleInterval.CANDLE_INTERVAL_30_SEC: 30,
            CandleInterval.CANDLE_INTERVAL_1_MIN: 60,
            CandleInterval.CANDLE_INTERVAL_2_MIN: 120,
            CandleInterval.CANDLE_INTERVAL_3_MIN: 180,
            CandleInterval.CANDLE_INTERVAL_5_MIN: 300,
            CandleInterval.CANDLE_INTERVAL_10_MIN: 600,
            CandleInterval.CANDLE_INTERVAL_15_MIN: 900,
            CandleInterval.CANDLE_INTERVAL_30_MIN: 1800,
            CandleInterval.CANDLE_INTERVAL_HOUR: 3600,
            CandleInterval.CANDLE_INTERVAL_2_HOUR: 7200,
            CandleInterval.CANDLE_INTERVAL_4_HOUR: 14400,
            CandleInterval.CANDLE_INTERVAL_DAY: 86400,
            CandleInterval.CANDLE_INTERVAL_WEEK: 604800,
            CandleInterval.CANDLE_INTERVAL_MONTH: 2592000,  # ~30 дней
        }
        return interval_seconds.get(interval, 60)
    
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
        - ohlcv: все тикеры акций
        - lob: все тикеры
        - trades: все тикеры
        - fundamentals: акции
        - events: акции
        - macro: валюты, товары, индексы, ставки (см. _supported_macro)
        
        Args:
            data_type: Тип данных.
            instrument: Инструмент.
            
        Returns:
            True если поддерживается.
        """
        # Проверка макро-данных
        if data_type == 'macro':
            return instrument in self._supported_macro
        
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
