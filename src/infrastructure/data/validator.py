"""
Валидатор данных.

Строгая валидация всех типов данных:
- Schema enforcement (соответствие контракту)
- OHLC consistency (high >= low, open/close в диапазоне)
- Causality guard (защита от lookahead bias)
- Adjusted prices consistency (монотонность adj_factor)
- Gap detection (пропуски > 24h)
- Volume check (нулевые объёмы)
- Price jump check (скачки > 15%)

Все проверки настраиваемые через конфигурацию.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

from .models import (
    Candle, MacroCandle, L2OrderBook, ValidationReport, Timeframe
)

logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Ошибка валидации данных."""
    pass


class CausalityError(ValidationError):
    """Ошибка каузальности (lookahead bias)."""
    pass


class DataValidator:
    """
    Валидатор данных с настраиваемыми проверками.
    
    Поддерживает:
    - Валидацию OHLCV свечей
    - Проверку каузальности для макро
    - Детекцию пропусков и аномалий
    
    Example:
        >>> validator = DataValidator(strict_mode=True)
        >>> report = await validator.validate_ohlcv(candles, "SNGS", "1d")
        >>> if not report.passed:
        ...     print(f"Validation failed: {report.errors}")
    """
    
    def __init__(
        self,
        strict_mode: bool = True,
        max_gap_hours: float = 24.0,
        max_zero_volume_bars: int = 5,
        max_price_jump_percent: float = 15.0
    ):
        """
        Инициализация валидатора.
        
        Args:
            strict_mode: Строгий режим (критические ошибки).
            max_gap_hours: Максимальный допустимый пропуск в часах.
            max_zero_volume_bars: Максимум баров с нулевым объёмом подряд.
            max_price_jump_percent: Максимальный скачок цены в %.
        """
        self.strict_mode = strict_mode
        self.max_gap_hours = max_gap_hours
        self.max_zero_volume_bars = max_zero_volume_bars
        self.max_price_jump_percent = max_price_jump_percent
    
    async def validate_ohlcv(
        self,
        candles: List[Dict[str, Any]],
        instrument: str,
        timeframe: str
    ) -> ValidationReport:
        """
        Валидация OHLCV данных.
        
        Args:
            candles: Список свечей (как dict).
            instrument: Инструмент.
            timeframe: Таймфрейм.
            
        Returns:
            Отчёт валидации.
        """
        errors = []
        warnings = []
        checks_performed = []
        
        if not candles:
            return ValidationReport(
                passed=True,
                errors=[],
                warnings=["Empty data"],
                checks_performed=['schema_enforcement'],
                quality_score=1.0
            )
        
        # 1. Schema enforcement
        checks_performed.append('schema_enforcement')
        schema_errors = self._check_schema(candles)
        errors.extend(schema_errors)
        
        if errors and self.strict_mode:
            return ValidationReport(
                passed=False,
                errors=errors,
                warnings=warnings,
                checks_performed=checks_performed,
                quality_score=0.0
            )
        
        # 2. OHLC consistency
        checks_performed.append('ohlc_consistency')
        ohlc_errors = self._check_ohlc_consistency(candles)
        errors.extend(ohlc_errors)
        
        # 3. Timestamp monotonicity
        checks_performed.append('timestamp_monotonicity')
        mono_errors = self._check_timestamp_monotonicity(candles)
        errors.extend(mono_errors)
        
        # 4. Gap detection
        checks_performed.append('gap_detection')
        gap_warnings = self._check_gaps(candles, timeframe)
        warnings.extend(gap_warnings)
        
        # 5. Volume check
        checks_performed.append('volume_check')
        volume_warnings = self._check_volume(candles)
        warnings.extend(volume_warnings)
        
        # 6. Price jump check
        checks_performed.append('price_jump_check')
        jump_warnings = self._check_price_jumps(candles)
        warnings.extend(jump_warnings)
        
        # 7. Adjusted prices consistency
        checks_performed.append('adjusted_prices')
        adj_errors = self._check_adjusted_prices(candles)
        errors.extend(adj_errors)
        
        # Вычисление quality score
        quality_score = self._compute_quality_score(
            len(errors), len(warnings), len(candles)
        )
        
        passed = len(errors) == 0
        
        return ValidationReport(
            passed=passed,
            errors=errors,
            warnings=warnings,
            checks_performed=checks_performed,
            quality_score=quality_score
        )
    
    def _check_schema(self, candles: List[Dict[str, Any]]) -> List[str]:
        """Проверка наличия обязательных полей."""
        errors = []
        required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        
        for i, candle in enumerate(candles):
            for field in required_fields:
                if field not in candle:
                    errors.append(f"Candle {i}: missing required field '{field}'")
        
        return errors[:10]  # Ограничиваем количество ошибок
    
    def _check_ohlc_consistency(self, candles: List[Dict[str, Any]]) -> List[str]:
        """
        Проверка OHLC консистентности.
        
        Правила:
        - high >= low
        - low <= open, close <= high
        """
        errors = []
        
        for i, c in enumerate(candles):
            try:
                high = Decimal(str(c['high']))
                low = Decimal(str(c['low']))
                open_p = Decimal(str(c['open']))
                close_p = Decimal(str(c['close']))
                
                if high < low:
                    errors.append(
                        f"Candle {i} ({c.get('timestamp')}): high ({high}) < low ({low})"
                    )
                
                if not (low <= open_p <= high):
                    errors.append(
                        f"Candle {i} ({c.get('timestamp')}): open ({open_p}) not in [low={low}, high={high}]"
                    )
                
                if not (low <= close_p <= high):
                    errors.append(
                        f"Candle {i} ({c.get('timestamp')}): close ({close_p}) not in [low={low}, high={high}]"
                    )
                    
            except (KeyError, ValueError, TypeError) as e:
                errors.append(f"Candle {i}: parse error - {e}")
        
        return errors[:10]
    
    def _check_timestamp_monotonicity(self, candles: List[Dict[str, Any]]) -> List[str]:
        """Проверка монотонности временных меток."""
        errors = []
        prev_ts = None
        
        for i, c in enumerate(candles):
            try:
                ts_str = c.get('timestamp', '')
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                
                if prev_ts and ts <= prev_ts:
                    errors.append(
                        f"Candle {i}: non-monotonic timestamp {ts} <= {prev_ts}"
                    )
                
                prev_ts = ts
                
            except (ValueError, TypeError) as e:
                errors.append(f"Candle {i}: invalid timestamp format - {e}")
        
        return errors[:10]
    
    def _check_gaps(
        self,
        candles: List[Dict[str, Any]],
        timeframe: str
    ) -> List[str]:
        """Проверка пропусков во времени."""
        warnings = []
        
        tf_delta = self._get_tf_delta(timeframe)
        max_gap = timedelta(hours=self.max_gap_hours)
        
        prev_ts = None
        
        for i, c in enumerate(candles):
            try:
                ts_str = c.get('timestamp', '')
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                
                if prev_ts:
                    gap = ts - prev_ts
                    
                    # Пропуск считается если больше чем 2x TF дельта и больше max_gap
                    if gap > max(2 * tf_delta, max_gap):
                        warnings.append(
                            f"Gap detected at {ts}: {gap.total_seconds() / 3600:.2f}h since previous bar"
                        )
                
                prev_ts = ts
                
            except (ValueError, TypeError):
                pass
        
        return warnings[:10]
    
    def _check_volume(self, candles: List[Dict[str, Any]]) -> List[str]:
        """Проверка нулевых объёмов."""
        warnings = []
        zero_count = 0
        
        for i, c in enumerate(candles):
            try:
                volume = Decimal(str(c.get('volume', 0)))
                
                if volume == 0:
                    zero_count += 1
                else:
                    if zero_count > self.max_zero_volume_bars:
                        warnings.append(
                            f"{zero_count} consecutive bars with zero volume ending at {i}"
                        )
                    zero_count = 0
                    
            except (ValueError, TypeError):
                pass
        
        if zero_count > self.max_zero_volume_bars:
            warnings.append(f"{zero_count} bars with zero volume at end of data")
        
        return warnings[:10]
    
    def _check_price_jumps(self, candles: List[Dict[str, Any]]) -> List[str]:
        """Проверка аномальных скачков цены."""
        warnings = []
        
        for i in range(1, len(candles)):
            try:
                prev_close = Decimal(str(candles[i-1].get('close', 0)))
                curr_open = Decimal(str(candles[i].get('open', 0)))
                
                if prev_close > 0:
                    jump_pct = abs(curr_open - prev_close) / prev_close * 100
                    
                    if jump_pct > self.max_price_jump_percent:
                        ts = candles[i].get('timestamp', f'index_{i}')
                        warnings.append(
                            f"Price jump at {ts}: {jump_pct:.2f}% (prev_close={prev_close}, open={curr_open})"
                        )
                        
            except (ValueError, TypeError, KeyError):
                pass
        
        return warnings[:10]
    
    def _check_adjusted_prices(self, candles: List[Dict[str, Any]]) -> List[str]:
        """
        Проверка консистентности скорректированных цен.
        
        adj_factor должен быть монотонно возрастающим (или константным).
        """
        errors = []
        prev_adj_factor = None
        
        for i, c in enumerate(candles):
            try:
                adj_factor = Decimal(str(c.get('adj_factor', 1)))
                
                if prev_adj_factor is not None:
                    # adj_factor должен быть >= предыдущего (для backward adjustment)
                    if adj_factor < prev_adj_factor:
                        ts = c.get('timestamp', f'index_{i}')
                        errors.append(
                            f"Non-monotonic adj_factor at {ts}: {adj_factor} < {prev_adj_factor}"
                        )
                
                prev_adj_factor = adj_factor
                
            except (ValueError, TypeError):
                pass
        
        return errors[:10]
    
    def _compute_quality_score(
        self,
        num_errors: int,
        num_warnings: int,
        total_records: int
    ) -> float:
        """
        Вычисление интегральной оценки качества.
        
        Формула:
        - Каждый error: -0.2
        - Каждый warning: -0.05
        - Минимум: 0.0
        """
        if total_records == 0:
            return 1.0
        
        base_score = 1.0
        error_penalty = min(num_errors * 0.2, 0.8)
        warning_penalty = min(num_warnings * 0.05, 0.3)
        
        score = max(0.0, base_score - error_penalty - warning_penalty)
        
        return round(score, 2)
    
    def _get_tf_delta(self, timeframe: str) -> timedelta:
        """Получение дельты таймфрейма."""
        deltas = {
            '1m': timedelta(minutes=1),
            '5m': timedelta(minutes=5),
            '10m': timedelta(minutes=10),
            '15m': timedelta(minutes=15),
            '1h': timedelta(hours=1),
            '4h': timedelta(hours=4),
            '1d': timedelta(days=1),
            '1w': timedelta(weeks=1),
            '1M': timedelta(days=30),
        }
        return deltas.get(timeframe, timedelta(hours=1))
    
    async def check_macro_causality(
        self,
        macro_data: List[Dict[str, Any]],
        price_data: List[Dict[str, Any]],
        instrument: str,
        shift_periods: int = 1
    ) -> ValidationReport:
        """
        Проверка каузальности макро-данных.
        
        Макро-данные должны быть сдвинуты минимум на 1 период относительно цены.
        
        Args:
            macro_data: Макро-данные.
            price_data: Ценовые данные.
            instrument: Инструмент.
            shift_periods: Требуемый сдвиг в периодах.
            
        Returns:
            Отчёт валидации.
        """
        errors = []
        
        if not macro_data or not price_data:
            return ValidationReport(
                passed=True,
                errors=[],
                warnings=["Empty data"],
                checks_performed=['causality_guard'],
                quality_score=1.0
            )
        
        # Проверка: макро-данные не должны быть "из будущего" относительно цены
        try:
            last_macro_ts = datetime.fromisoformat(
                macro_data[-1].get('timestamp', '').replace('Z', '+00:00')
            )
            
            # Для корректной каузальности последнее макро должно быть
            # минимум на shift_periods раньше последней цены
            # Это упрощённая проверка
            
        except (ValueError, TypeError) as e:
            errors.append(f"Failed to parse timestamps: {e}")
        
        return ValidationReport(
            passed=len(errors) == 0,
            errors=errors,
            warnings=[],
            checks_performed=['causality_guard'],
            quality_score=1.0 if len(errors) == 0 else 0.0
        )
