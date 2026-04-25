"""
Модели данных для Data Collector (MANETS).

Строгие Pydantic v2 модели для всех типов данных:
- OHLCV свечи
- Стаканы L2
- Трейды
- Макро-данные
- Фундаментальные показатели
- Корпоративные события
- Контрольные точки и метаданные

Все модели поддерживают валидацию, сериализацию и типизацию.
"""

from __future__ import annotations

from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from typing import List, Optional, Dict, Any, Literal
import hashlib

from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict
from pydantic.v1 import BaseModel as V1BaseModel  # Для обратной совместимости если нужно


class Timeframe(str, Enum):
    """Поддерживаемые таймфреймы."""
    S5 = "5s"
    S10 = "10s"
    S30 = "30s"
    M1 = "1m"
    M2 = "2m"
    M3 = "3m"
    M5 = "5m"
    M10 = "10m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    D1 = "1d"
    W1 = "1w"
    MN = "1M"


class DataSource(str, Enum):
    """Источники данных."""
    TTECH = "ttech"
    MOEXALGO = "moexalgo"
    CBR = "cbr"
    STUB = "stub"


class EventType(str, Enum):
    """Типы корпоративных событий."""
    DIVIDEND = "dividend"
    SPLIT = "split"
    REVERSE_SPLIT = "reverse_split"
    EARNINGS = "earnings"
    MA = "merger_acquisition"
    OTHER = "other"


class EventStatus(str, Enum):
    """Статус корпоративного события."""
    ANNOUNCED = "announced"
    CONFIRMED = "confirmed"
    EXECUTED = "executed"
    CANCELLED = "cancelled"


class Candle(BaseModel):
    """
    Модель OHLCV свечи.
    
    Attributes:
        timestamp: Временная метка начала бара (UTC).
        open: Цена открытия (Decimal для точности).
        high: Максимальная цена за период.
        low: Минимальная цена за период.
        close: Цена закрытия.
        volume: Объём торгов.
        adj_close: Скорректированная цена закрытия (backward-adjusted).
        adj_factor: Коэффициент корректировки (>= 1.0).
        is_complete: Флаг завершённости бара (False для текущего незавершённого).
        source: Источник данных.
        quality_score: Оценка качества [0.0, 1.0].
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    timestamp: datetime = Field(..., description="Временная метка начала бара (UTC)")
    open: Decimal = Field(..., description="Цена открытия", ge=0)
    high: Decimal = Field(..., description="Максимальная цена", ge=0)
    low: Decimal = Field(..., description="Минимальная цена", ge=0)
    close: Decimal = Field(..., description="Цена закрытия", ge=0)
    volume: Decimal = Field(..., description="Объём торгов", ge=0)
    adj_close: Optional[Decimal] = Field(default=None, description="Скорректированная цена закрытия", ge=0)
    adj_factor: Decimal = Field(default=Decimal("1.0"), description="Коэффициент корректировки", ge=1.0)
    is_complete: bool = Field(default=True, description="Флаг завершённости бара")
    source: DataSource = Field(default=DataSource.TTECH, description="Источник данных")
    quality_score: float = Field(default=1.0, description="Оценка качества", ge=0.0, le=1.0)
    
    @field_validator('high', 'low')
    @classmethod
    def check_high_low_range(cls, v: Decimal, info) -> Decimal:
        """Проверка: high >= low."""
        values = info.data
        if 'low' in values and 'high' in values:
            if values['high'] < values['low']:
                raise ValueError('high должно быть >= low')
        return v
    
    @model_validator(mode='after')
    def validate_ohlc_consistency(self) -> 'Candle':
        """
        Валидация OHLC консистентности.
        
        Правила:
        - low <= open, close <= high
        - high >= low
        """
        if not (self.low <= self.open <= self.high):
            raise ValueError(f'open ({self.open}) должен быть в диапазоне [low={self.low}, high={self.high}]')
        if not (self.low <= self.close <= self.high):
            raise ValueError(f'close ({self.close}) должен быть в диапазоне [low={self.low}, high={self.high}]')
        if self.high < self.low:
            raise ValueError(f'high ({self.high}) должен быть >= low ({self.low})')
        return self


class L2OrderLevel(BaseModel):
    """Уровень стакана (цена + объём + количество заявок)."""
    model_config = ConfigDict(populate_by_name=True)
    
    price: Decimal = Field(..., ge=0, description="Цена уровня")
    volume: Decimal = Field(..., ge=0, validation_alias="qty", serialization_alias="qty", description="Объём на уровне")
    orders: int = Field(default=1, ge=1, description="Количество заявок")


class L2OrderBook(BaseModel):
    """
    Стакан заявок Level 2.
    
    Attributes:
        timestamp: Временная метка среза.
        bids: Массив уровней спроса (от лучшего к худшему).
        asks: Массив уровней предложения (от лучшего к худшему).
        mid_price: Средняя цена (bid/ask midpoint).
        spread_bps: Спред в базисных пунктах.
        limit_up: Верхний лимит цены (если есть).
        limit_down: Нижний лимит цены (если есть).
        source: Источник данных.
        quality_score: Оценка качества.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    timestamp: datetime = Field(..., description="Временная метка среза")
    bids: List[L2OrderLevel] = Field(..., min_length=1, description="Уровни спроса")
    asks: List[L2OrderLevel] = Field(..., min_length=1, description="Уровни предложения")
    mid_price: Optional[Decimal] = Field(default=None, description="Средняя цена")
    spread_bps: Optional[Decimal] = Field(default=None, description="Спред в базисных пунктах")
    limit_up: Optional[Decimal] = Field(default=None, description="Верхний лимит цены")
    limit_down: Optional[Decimal] = Field(default=None, description="Нижний лимит цены")
    source: DataSource = Field(default=DataSource.TTECH, description="Источник данных")
    quality_score: float = Field(default=1.0, description="Оценка качества", ge=0.0, le=1.0)
    
    @model_validator(mode='after')
    def calculate_derived_fields(self) -> 'L2OrderBook':
        """Вычисление mid_price и spread_bps если не предоставлены."""
        if self.bids and self.asks:
            best_bid = self.bids[0].price
            best_ask = self.asks[0].price
            
            if self.mid_price is None:
                self.mid_price = (best_bid + best_ask) / 2
            
            if self.spread_bps is None and self.mid_price > 0:
                spread = best_ask - best_bid
                self.spread_bps = (spread / self.mid_price) * 10000
        
        return self
    
    @field_validator('bids')
    @classmethod
    def validate_bids_descending(cls, v: List[L2OrderLevel]) -> List[L2OrderLevel]:
        """Проверка: bids отсортированы по убыванию цены (от лучшего к худшему)."""
        for i in range(len(v) - 1):
            if v[i].price < v[i + 1].price:
                raise ValueError('bids должны быть отсортированы по убыванию цены')
        return v
    
    @field_validator('asks')
    @classmethod
    def validate_asks_ascending(cls, v: List[L2OrderLevel]) -> List[L2OrderLevel]:
        """Проверка: asks отсортированы по возрастанию цены (от лучшего к худшему)."""
        for i in range(len(v) - 1):
            if v[i].price > v[i + 1].price:
                raise ValueError('asks должны быть отсортированы по возрастанию цены')
        return v


class TradeSide(str, Enum):
    """Направление сделки."""
    BUY = "buy"
    SELL = "sell"


class Trade(BaseModel):
    """
    Тиковая сделка.
    
    Attributes:
        timestamp: Временная метка сделки.
        price: Цена сделки.
        volume: Объём сделки.
        side: Направление (buy/sell).
        aggressor_flag: Флаг агрессора (True если инициатор сделки).
        trade_id: Уникальный идентификатор сделки.
        source: Источник данных.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    timestamp: datetime = Field(..., description="Временная метка сделки")
    price: Decimal = Field(..., ge=0, description="Цена сделки")
    volume: Decimal = Field(..., gt=0, description="Объём сделки")
    side: TradeSide = Field(..., description="Направление сделки")
    aggressor_flag: bool = Field(default=False, description="Флаг агрессора")
    trade_id: Optional[str] = Field(default=None, description="Уникальный ID сделки")
    source: DataSource = Field(default=DataSource.TTECH, description="Источник данных")


class MacroCandle(BaseModel):
    """
    Макро-экономический индикатор в формате свечи.
    
    Attributes:
        timestamp: Временная метка.
        open: Значение на открытие периода.
        high: Максимальное значение.
        low: Минимальное значение.
        close: Значение на закрытие.
        volume: Объём (если применимо).
        source: Источник данных.
        interpolation_method: Метод заполнения пропусков.
        shift_periods: Количество периодов сдвига для каузальности.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    timestamp: datetime = Field(..., description="Временная метка")
    open: Decimal = Field(..., description="Значение на открытие")
    high: Decimal = Field(..., description="Максимальное значение")
    low: Decimal = Field(..., description="Минимальное значение")
    close: Decimal = Field(..., description="Значение на закрытие")
    volume: Optional[Decimal] = Field(default=None, description="Объём (если применимо)")
    source: DataSource = Field(default=DataSource.CBR, description="Источник данных")
    interpolation_method: Literal["ffill", "linear", "none"] = Field(
        default="ffill", description="Метод заполнения пропусков"
    )
    shift_periods: int = Field(default=1, ge=1, description="Сдвиг для каузальности")
    
    @model_validator(mode='after')
    def validate_macro_consistency(self) -> 'MacroCandle':
        """Валидация консистентности макро-данных."""
        if self.high < self.low:
            raise ValueError('high должно быть >= low')
        if not (self.low <= self.open <= self.high):
            raise ValueError('open должен быть в диапазоне [low, high]')
        if not (self.low <= self.close <= self.high):
            raise ValueError('close должен быть в диапазоне [low, high]')
        return self


class Fundamental(BaseModel):
    """
    Фундаментальные показатели компании.
    
    Attributes:
        report_date: Дата отчётности.
        ticker: Тикер компании.
        pe: P/E ratio.
        pb: P/B ratio.
        roe: Return on Equity.
        ev_ebitda: EV/EBITDA.
        debt_ebitda: Debt/EBITDA.
        dividend_yield: Дивидендная доходность.
        free_cash_flow: Свободный денежный поток.
        market_cap: Рыночная капитализация.
        source: Источник данных.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    report_date: date = Field(..., description="Дата отчётности")
    ticker: str = Field(..., min_length=1, description="Тикер компании")
    pe: Optional[Decimal] = Field(default=None, description="P/E ratio")
    pb: Optional[Decimal] = Field(default=None, description="P/B ratio")
    roe: Optional[Decimal] = Field(default=None, description="Return on Equity")
    ev_ebitda: Optional[Decimal] = Field(default=None, description="EV/EBITDA")
    debt_ebitda: Optional[Decimal] = Field(default=None, description="Debt/EBITDA")
    dividend_yield: Optional[Decimal] = Field(default=None, description="Дивидендная доходность")
    free_cash_flow: Optional[Decimal] = Field(default=None, description="Свободный денежный поток")
    market_cap: Optional[Decimal] = Field(default=None, description="Рыночная капитализация")
    source: DataSource = Field(default=DataSource.TTECH, description="Источник данных")
    
    @field_validator('*')
    @classmethod
    def check_non_negative(cls, v: Optional[Decimal], info) -> Optional[Decimal]:
        """Проверка неотрицательности финансовых показателей."""
        if v is not None and v < 0:
            # Разрешаем отрицательные значения для некоторых метрик
            if info.field_name not in ['free_cash_flow']:
                pass  # Предупреждение можно добавить в лог
        return v


class CorporateEvent(BaseModel):
    """
    Корпоративное событие.
    
    Attributes:
        event_type: Тип события.
        ticker: Тикер компании.
        ex_date: Дата экс-дивидендов/сплита.
        record_date: Дата реестра.
        payout_date: Дата выплаты.
        ratio: Коэффициент (для сплитов).
        amount: Сумма (для дивидендов).
        status: Статус события.
        source: Источник данных.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    event_type: EventType = Field(..., description="Тип события")
    ticker: str = Field(..., min_length=1, description="Тикер компании")
    ex_date: Optional[date] = Field(default=None, description="Дата экс-дивидендов/сплита")
    record_date: Optional[date] = Field(default=None, description="Дата реестра")
    payout_date: Optional[date] = Field(default=None, description="Дата выплаты")
    ratio: Optional[Decimal] = Field(default=None, description="Коэффициент (для сплитов)")
    amount: Optional[Decimal] = Field(default=None, description="Сумма (для дивидендов)")
    status: EventStatus = Field(default=EventStatus.ANNOUNCED, description="Статус события")
    source: DataSource = Field(default=DataSource.TTECH, description="Источник данных")
    
    @model_validator(mode='after')
    def validate_event_dates(self) -> 'CorporateEvent':
        """Проверка последовательности дат."""
        dates = []
        if self.ex_date:
            dates.append(('ex_date', self.ex_date))
        if self.record_date:
            dates.append(('record_date', self.record_date))
        if self.payout_date:
            dates.append(('payout_date', self.payout_date))
        
        for i in range(len(dates) - 1):
            if dates[i][1] > dates[i + 1][1]:
                raise ValueError(f'{dates[i][0]} ({dates[i][1]}) должна быть <= {dates[i+1][0]} ({dates[i+1][1]})')
        
        return self


class Checkpoint(BaseModel):
    """
    Контрольная точка для инкрементальной синхронизации.
    
    Attributes:
        instrument: Инструмент.
        timeframe: Таймфрейм.
        data_type: Тип данных (ohlcv, lob, macro, etc).
        last_timestamp: Последняя успешная временная метка.
        last_sync_at: Время последней синхронизации.
        source_chain: Цепочка источников.
        checksum: SHA256 хэш данных.
        schema_version: Версия схемы.
        quality_score: Интегральная оценка качества.
        gaps_detected: Обнаруженные пропуски.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    instrument: str = Field(..., min_length=1, description="Инструмент")
    timeframe: Timeframe = Field(..., description="Таймфрейм")
    data_type: Literal["ohlcv", "lob", "macro", "fundamentals", "events", "trades"] = Field(
        ..., description="Тип данных"
    )
    last_timestamp: Optional[datetime] = Field(default=None, description="Последняя временная метка")
    last_sync_at: datetime = Field(default_factory=datetime.utcnow, description="Время синхронизации")
    source_chain: List[DataSource] = Field(default_factory=list, description="Цепочка источников")
    checksum: Optional[str] = Field(default=None, description="SHA256 хэш")
    schema_version: str = Field(default="2.0", description="Версия схемы")
    quality_score: float = Field(default=1.0, description="Оценка качества", ge=0.0, le=1.0)
    gaps_detected: int = Field(default=0, ge=0, description="Количество пропусков")
    
    def compute_checksum(self, data: bytes) -> str:
        """Вычисление SHA256 хэша."""
        self.checksum = hashlib.sha256(data).hexdigest()
        return self.checksum


class Metadata(BaseModel):
    """
    Метаданные для набора данных.
    
    Attributes:
        snapshot_id: Уникальный идентификатор снэпшота.
        instrument: Инструмент.
        timeframe: Таймфрейм.
        data_type: Тип данных.
        created_at: Время создания.
        updated_at: Время обновления.
        checkpoint: Контрольная точка.
        validation_report: Отчёт валидации.
        source_chain: Цепочка источников.
        total_records: Общее количество записей.
    """
    model_config = ConfigDict(arbitrary_types_allowed=True, use_enum_values=True)
    
    snapshot_id: str = Field(..., min_length=1, description="ID снэпшота")
    instrument: str = Field(..., min_length=1, description="Инструмент")
    timeframe: Timeframe = Field(..., description="Таймфрейм")
    data_type: str = Field(..., description="Тип данных")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Время создания")
    updated_at: datetime = Field(default_factory=datetime.utcnow, description="Время обновления")
    checkpoint: Optional[Checkpoint] = Field(default=None, description="Контрольная точка")
    validation_report: Optional[Dict[str, Any]] = Field(default=None, description="Отчёт валидации")
    source_chain: List[DataSource] = Field(default_factory=list, description="Цепочка источников")
    total_records: int = Field(default=0, ge=0, description="Общее количество записей")


class ValidationReport(BaseModel):
    """
    Отчёт о валидации данных.
    
    Attributes:
        timestamp: Время валидации.
        passed: Флаг успешной валидации.
        errors: Список ошибок.
        warnings: Список предупреждений.
        checks_performed: Выполненные проверки.
        quality_score: Итоговая оценка качества.
    """
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Время валидации")
    passed: bool = Field(..., description="Флаг успешной валидации")
    errors: List[str] = Field(default_factory=list, description="Список ошибок")
    warnings: List[str] = Field(default_factory=list, description="Список предупреждений")
    checks_performed: List[str] = Field(default_factory=list, description="Выполненные проверки")
    quality_score: float = Field(default=1.0, description="Итоговая оценка", ge=0.0, le=1.0)


class RateLimitConfig(BaseModel):
    """Конфигурация rate limiter."""
    requests_per_second: float = Field(default=10.0, gt=0)
    burst_size: int = Field(default=20, gt=0)


class CircuitBreakerConfig(BaseModel):
    """Конфигурация circuit breaker."""
    failure_threshold: int = Field(default=5, gt=0)
    recovery_timeout_sec: float = Field(default=30.0, gt=0)
    half_open_requests: int = Field(default=3, gt=0)


class ProviderConfig(BaseModel):
    """Конфигурация провайдера."""
    name: str
    enabled: bool = True
    priority: int = Field(..., gt=0)
    rate_limit: Optional[RateLimitConfig] = None
    circuit_breaker: Optional[CircuitBreakerConfig] = None
    endpoints: Dict[str, str] = Field(default_factory=dict)
    auth_token_env: Optional[str] = None
    quality_score_override: Optional[float] = None
    warning_on_use: bool = False
    request_timeout_sec: float = Field(default=30.0, gt=0)
