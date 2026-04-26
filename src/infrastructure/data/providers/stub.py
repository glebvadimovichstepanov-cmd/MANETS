"""
Stub Provider - заглушка для fallback.

Возвращает синтетические/исторические данные с пониженным quality_score.
Используется когда все основные источники недоступны.

Особенности:
- Генерация синтетических данных на основе исторических паттернов
- Пониженный quality_score (0.3 по умолчанию)
- Логирование предупреждений об использовании fallback
- Поддержка всех типов данных
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from ..models import (
    Candle, L2OrderBook, L2OrderLevel, Trade, TradeSide,
    MacroCandle, Fundamental, CorporateEvent,
    EventType, EventStatus, DataSource, Timeframe
)
from .base import DataProvider

logger = logging.getLogger(__name__)


class StubProvider(DataProvider):
    """
    Провайдер-заглушка для fallback сценариев.
    
    Генерирует синтетические данные на основе:
    - Исторических паттернов (если доступны в кэше)
    - Случайного блуждания с волатильностью
    
    Quality score всегда понижен (0.3) для标识ции fallback данных.
    
    Example:
        >>> provider = StubProvider(config)
        >>> candles = await provider.get_ohlcv("SNGS", Timeframe.D1, from_dt, to_dt)
        >>> print(candles[0].quality_score)  # 0.3
    """
    
    # Базовые цены для генерации (можно переопределить в конфиге)
    BASE_PRICES = {
        'SNGS': Decimal('150.0'),
        'GAZP': Decimal('160.0'),
        'SBER': Decimal('270.0'),
        'LKOH': Decimal('6500.0'),
        'YNDX': Decimal('3500.0'),
        'USD_RUB': Decimal('90.0'),
        'EUR_RUB': Decimal('98.0'),
        'BRENT': Decimal('80.0'),
        'MOEX_INDEX': Decimal('3200.0'),
    }
    
    # Волатильность по инструментам (дневная, %)
    VOLATILITY = {
        'SNGS': 0.03,
        'GAZP': 0.025,
        'SBER': 0.02,
        'LKOH': 0.02,
        'YNDX': 0.035,
        'USD_RUB': 0.01,
        'EUR_RUB': 0.012,
        'BRENT': 0.025,
        'MOEX_INDEX': 0.015,
    }
    
    def __init__(
        self,
        config: Dict[str, Any],
        rate_limit_config: Optional[Dict[str, Any]] = None,
        circuit_breaker_config: Optional[Dict[str, Any]] = None
    ):
        """
        Инициализация Stub провайдера.
        
        Args:
            config: Конфигурация провайдера.
            rate_limit_config: Конфигурация rate limiter.
            circuit_breaker_config: Конфигурация circuit breaker.
        """
        super().__init__(
            name="stub",
            priority=config.get('priority', 4),
            rate_limit_config=rate_limit_config or {'requests_per_second': 100.0, 'burst_size': 200},
            circuit_breaker_config=circuit_breaker_config
        )
        
        self.quality_score_override = config.get('quality_score_override', 0.3)
        self.warning_on_use = config.get('warning_on_use', True)
        
        # Кэш сгенерированных данных для консистентности
        self._generated_cache: Dict[str, List[Candle]] = {}
        
        if self.warning_on_use:
            logger.warning(
                "StubProvider initialized. All data will be synthetic with "
                f"quality_score={self.quality_score_override}. Use only as fallback."
            )
    
    def _get_base_price(self, instrument: str) -> Decimal:
        """Получение базовой цены для инструмента."""
        return self.BASE_PRICES.get(instrument, Decimal('100.0'))
    
    def _get_volatility(self, instrument: str) -> float:
        """Получение волатильности для инструмента."""
        return self.VOLATILITY.get(instrument, 0.02)
    
    def _generate_random_walk(
        self,
        start_price: Decimal,
        volatility: float,
        num_steps: int,
        timeframe: Timeframe
    ) -> List[Decimal]:
        """
        Генерация случайного блуждания цен.
        
        Args:
            start_price: Начальная цена.
            volatility: Волатильность (дневная).
            num_steps: Количество шагов.
            timeframe: Таймфрейм для масштабирования волатильности.
            
        Returns:
            Список цен закрытия.
        """
        # Масштабирование волатильности по таймфрейму
        tf_hours = {
            Timeframe.M1: 1/60,
            Timeframe.M5: 5/60,
            Timeframe.M10: 10/60,
            Timeframe.M15: 15/60,
            Timeframe.H1: 1,
            Timeframe.H4: 4,
            Timeframe.D1: 24,
            Timeframe.W1: 24*7,
            Timeframe.MN: 24*30,
        }
        
        hours = tf_hours.get(timeframe, 1)
        scaled_vol = volatility * (hours / 24) ** 0.5
        
        prices = [start_price]
        for _ in range(num_steps - 1):
            change = Decimal(str(random.gauss(0, scaled_vol)))
            new_price = prices[-1] * (1 + change)
            prices.append(max(new_price, Decimal('0.01')))  # Защита от отрицательных цен
        
        return prices
    
    def _generate_candle(
        self,
        timestamp: datetime,
        open_price: Decimal,
        close_price: Decimal,
        volatility: float
    ) -> Candle:
        """
        Генерация одной свечи OHLCV.
        
        Args:
            timestamp: Временная метка.
            open_price: Цена открытия.
            close_price: Цена закрытия.
            volatility: Волатильность для генерации high/low.
            
        Returns:
            Свеча.
        """
        # Генерация high и low
        price_range = abs(close_price - open_price)
        extra_range = Decimal(str(volatility)) * open_price * Decimal('0.5')
        
        if close_price >= open_price:
            high = max(open_price, close_price) + extra_range
            low = min(open_price, close_price) - extra_range * Decimal('0.5')
        else:
            high = max(open_price, close_price) + extra_range * Decimal('0.5')
            low = min(open_price, close_price) - extra_range
        
        high = max(high, open_price, close_price)
        low = min(low, open_price, close_price)
        
        # Генерация объёма
        base_volume = Decimal(str(random.uniform(1000, 100000)))
        
        return Candle(
            timestamp=timestamp,
            open=open_price,
            high=high,
            low=low,
            close=close_price,
            volume=base_volume,
            adj_close=close_price,  # Для stub не корректируем
            adj_factor=Decimal('1.0'),
            is_complete=True,
            source=DataSource.STUB,
            quality_score=self.quality_score_override
        )
    
    async def get_ohlcv(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Candle]:
        """
        Получение синтетических OHLCV свечей.
        
        Args:
            instrument: Тикер инструмента.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список синтетических свечей.
        """
        if self.warning_on_use:
            logger.warning(
                f"StubProvider: Generating synthetic OHLCV for {instrument} "
                f"{timeframe.value}. Data is NOT real!"
            )
        
        # Вычисление количества баров
        duration = to_dt - from_dt
        tf_deltas = {
            Timeframe.M1: timedelta(minutes=1),
            Timeframe.M5: timedelta(minutes=5),
            Timeframe.M10: timedelta(minutes=10),
            Timeframe.M15: timedelta(minutes=15),
            Timeframe.H1: timedelta(hours=1),
            Timeframe.H4: timedelta(hours=4),
            Timeframe.D1: timedelta(days=1),
            Timeframe.W1: timedelta(weeks=1),
            Timeframe.MN: timedelta(days=30),
        }
        
        tf_delta = tf_deltas.get(timeframe, timedelta(hours=1))
        num_bars = max(1, int(duration.total_seconds() / tf_delta.total_seconds()))
        
        # Генерация цен
        base_price = self._get_base_price(instrument)
        volatility = self._get_volatility(instrument)
        
        close_prices = self._generate_random_walk(
            base_price, volatility, num_bars + 1, timeframe
        )
        
        # Генерация свечей
        candles = []
        current_ts = from_dt
        
        for i in range(min(num_bars, len(close_prices) - 1)):
            open_price = close_prices[i]
            close_price = close_prices[i + 1]
            
            candle = self._generate_candle(current_ts, open_price, close_price, volatility)
            candles.append(candle)
            
            current_ts += tf_delta
        
        return candles
    
    async def get_orderbook(
        self,
        instrument: str,
        depth: int = 10
    ) -> L2OrderBook:
        """
        Получение синтетического стакана.
        
        Args:
            instrument: Тикер инструмента.
            depth: Глубина стакана.
            
        Returns:
            Синтетический стакан.
        """
        if self.warning_on_use:
            logger.warning(f"StubProvider: Generating synthetic orderbook for {instrument}")
        
        base_price = self._get_base_price(instrument)
        
        # Генерация уровней
        bids = []
        asks = []
        
        spread = base_price * Decimal('0.001')  # 0.1% спред
        
        for i in range(depth):
            bid_price = base_price - spread * (i + 1)
            ask_price = base_price + spread * (i + 1)
            
            bid_qty = Decimal(str(random.uniform(100, 10000)))
            ask_qty = Decimal(str(random.uniform(100, 10000)))
            
            bids.append(L2OrderLevel(price=bid_price, qty=bid_qty, orders=random.randint(1, 50)))
            asks.append(L2OrderLevel(price=ask_price, qty=ask_qty, orders=random.randint(1, 50)))
        
        return L2OrderBook(
            timestamp=datetime.utcnow(),
            bids=bids,
            asks=asks,
            source=DataSource.STUB,
            quality_score=self.quality_score_override
        )
    
    async def get_trades(
        self,
        instrument: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Trade]:
        """
        Получение синтетических трейдов.
        
        Args:
            instrument: Тикер инструмента.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список синтетических сделок.
        """
        if self.warning_on_use:
            logger.warning(f"StubProvider: Generating synthetic trades for {instrument}")
        
        base_price = self._get_base_price(instrument)
        
        # Генерация случайных трейдов
        num_trades = random.randint(10, 100)
        trades = []
        
        for i in range(num_trades):
            ts = from_dt + timedelta(seconds=random.randint(0, int((to_dt - from_dt).total_seconds())))
            price = base_price * Decimal(str(1 + random.uniform(-0.01, 0.01)))
            volume = Decimal(str(random.uniform(1, 1000)))
            side = random.choice([TradeSide.BUY, TradeSide.SELL])
            
            trades.append(Trade(
                timestamp=ts,
                price=price,
                volume=volume,
                side=side,
                aggressor_flag=random.choice([True, False]),
                source=DataSource.STUB
            ))
        
        return sorted(trades, key=lambda t: t.timestamp)
    
    async def get_macro(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[MacroCandle]:
        """
        Получение синтетических макро-данных.
        
        Args:
            instrument: Макро-инструмент.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список синтетических макро-свечей.
        """
        # Для макро используем те же механизмы генерации
        candles = await self.get_ohlcv(instrument, timeframe, from_dt, to_dt)
        
        return [
            MacroCandle(
                timestamp=c.timestamp,
                open=c.open,
                high=c.high,
                low=c.low,
                close=c.close,
                volume=c.volume,
                source=DataSource.STUB,
                interpolation_method='ffill',
                shift_periods=1  # Каузальность!
            )
            for c in candles
        ]
    
    async def get_fundamentals(
        self,
        ticker: str
    ) -> List[Fundamental]:
        """
        Получение синтетических фундаментальных данных.
        
        Args:
            ticker: Тикер компании.
            
        Returns:
            Список синтетических показателей.
        """
        if self.warning_on_use:
            logger.warning(f"StubProvider: Generating synthetic fundamentals for {ticker}")
        
        # Генерация случайных квартальных данных
        fundamentals = []
        
        for quarters_ago in range(4):
            report_date = datetime.utcnow().date() - timedelta(days=quarters_ago * 90)
            
            fundamentals.append(Fundamental(
                report_date=report_date,
                ticker=ticker,
                pe=Decimal(str(random.uniform(5, 20))),
                pb=Decimal(str(random.uniform(0.5, 3))),
                roe=Decimal(str(random.uniform(0.1, 0.3))),
                ev_ebitda=Decimal(str(random.uniform(3, 10))),
                debt_ebitda=Decimal(str(random.uniform(0.5, 2))),
                dividend_yield=Decimal(str(random.uniform(0.02, 0.1))),
                free_cash_flow=Decimal(str(random.uniform(1e9, 1e11))),
                source=DataSource.STUB
            ))
        
        return fundamentals
    
    async def get_corporate_events(
        self,
        ticker: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[CorporateEvent]:
        """
        Получение синтетических корпоративных событий.
        
        Args:
            ticker: Тикер компании.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список синтетических событий.
        """
        if self.warning_on_use:
            logger.warning(f"StubProvider: Generating synthetic corporate events for {ticker}")
        
        # Возвращаем пустой список или минимальные события
        return []
    
    def supports(self, data_type: str, instrument: str) -> bool:
        """
        Stub поддерживает все типы данных для всех инструментов.
        
        Args:
            data_type: Тип данных.
            instrument: Инструмент.
            
        Returns:
            True (всегда).
        """
        return True
    
    def get_rate_limits(self) -> Dict[str, Any]:
        """Получение текущих лимитов запросов."""
        return {
            'provider': self.name,
            'requests_per_second': self._rate_limiter._state.refill_rate,
            'burst_size': self._rate_limiter._state.burst_size,
            'available_tokens': self._rate_limiter._state.tokens,
            'circuit_breaker_state': self.circuit_breaker_state.value,
            'is_stub': True,
            'quality_score': self.quality_score_override
        }
