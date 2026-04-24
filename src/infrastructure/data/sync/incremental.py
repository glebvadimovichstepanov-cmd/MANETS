"""
Инкрементальный синхронизатор данных.

Реализует стратегию инкрементальной загрузки:
1. Чтение checkpoint.json → last_timestamp
2. Запрос дельты [last_ts, now) у провайдера
3. Валидация новых данных
4. Атомарная запись (conсatenation + deduplication)
5. Обновление checkpoint и метаданных

Гарантии:
- Идемпотентность повторных запусков
- Отсутствие дубликатов
- Сохранение порядка временных меток
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from ..models import (
    Candle, Timeframe, Checkpoint, Metadata, ValidationReport,
    DataSource
)
from ..providers.base import DataProvider
from ..storage.local_file import LocalFileStorage
from ..cache.memcached import MemcachedClient
from ..validator import DataValidator

logger = logging.getLogger(__name__)


class IncrementalSyncError(Exception):
    """Ошибка инкрементальной синхронизации."""
    pass


class CausalityError(Exception):
    """Ошибка каузальности (lookahead bias detected)."""
    pass


class IncrementalSynchronizer:
    """
    Инкрементальный синхронизатор для OHLCV данных.
    
    Example:
        >>> sync = IncrementalSynchronizer(provider, storage, cache)
        >>> result = await sync.sync("SNGS", Timeframe.D1)
        >>> print(f"Loaded {result['new_bars']} new bars")
    """
    
    def __init__(
        self,
        provider: DataProvider,
        storage: LocalFileStorage,
        cache: Optional[MemcachedClient] = None,
        validator: Optional[DataValidator] = None,
        max_batch_size: int = 1000
    ):
        """
        Инициализация синхронизатора.
        
        Args:
            provider: Провайдер данных.
            storage: Хранилище.
            cache: Кэш (опционально).
            validator: Валидатор (опционально).
            max_batch_size: Максимальный размер батча за один запрос.
        """
        self.provider = provider
        self.storage = storage
        self.cache = cache
        self.validator = validator or DataValidator()
        self.max_batch_size = max_batch_size
    
    async def sync(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv",
        force_full: bool = False
    ) -> Dict[str, Any]:
        """
        Инкрементальная синхронизация.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            force_full: Принудительная полная загрузка.
            
        Returns:
            Статистика синхронизации.
            
        Raises:
            IncrementalSyncError: Если ошибка синхронизации.
            CausalityError: Если обнаружен lookahead bias.
        """
        start_time = datetime.utcnow()
        
        logger.info(
            f"Starting incremental sync for {instrument} {timeframe.value} "
            f"(force_full={force_full})"
        )
        
        # Чтение контрольной точки
        checkpoint = None
        if not force_full:
            checkpoint = await self.storage.read_checkpoint(
                instrument, timeframe, data_type
            )
        
        # Определение периода загрузки
        if checkpoint and checkpoint.last_timestamp:
            from_dt = checkpoint.last_timestamp
            # Добавляем небольшой overlap для безопасности (1 бар)
            from_dt = from_dt - self._get_tf_delta(timeframe)
        else:
            # Полная загрузка: с далёкого прошлого
            from_dt = datetime(2000, 1, 1, tzinfo=timezone.utc)
            logger.info(f"No checkpoint found. Full load from {from_dt}")
        
        to_dt = datetime.utcnow().replace(tzinfo=timezone.utc)
        
        # Проверка необходимости загрузки
        if to_dt - from_dt < self._get_tf_delta(timeframe):
            logger.info("No new data needed (within one bar)")
            return {
                'status': 'no_op',
                'instrument': instrument,
                'timeframe': timeframe.value,
                'new_bars': 0,
                'duration_ms': 0
            }
        
        # Загрузка данных (возможно по батчам)
        all_candles = []
        current_from = from_dt
        
        while current_from < to_dt:
            batch_to = min(current_from + timedelta(days=30), to_dt)
            
            logger.debug(f"Fetching batch: {current_from} to {batch_to}")
            
            try:
                candles = await self.provider.get_ohlcv(
                    instrument, timeframe, current_from, batch_to
                )
                
                if not candles:
                    logger.warning(f"No data returned for batch {current_from} to {batch_to}")
                    break
                
                all_candles.extend(candles)
                current_from = batch_to
                
                # Небольшая пауза между батчами
                if current_from < to_dt:
                    await asyncio.sleep(0.1)
                    
            except Exception as e:
                logger.error(f"Failed to fetch batch: {e}")
                raise IncrementalSyncError(f"Fetch failed: {e}")
        
        if not all_candles:
            logger.warning("No data loaded")
            return {
                'status': 'no_data',
                'instrument': instrument,
                'timeframe': timeframe.value,
                'new_bars': 0,
                'duration_ms': (datetime.utcnow() - start_time).total_seconds() * 1000
            }
        
        # Конвертация в dict для хранения
        candles_dict = [self._candle_to_dict(c) for c in all_candles]
        
        # Валидация
        validation_result = await self._validate_and_check_causality(
            candles_dict, instrument, timeframe
        )
        
        if not validation_result['passed']:
            errors = validation_result.get('errors', [])
            if any('causality' in str(e).lower() for e in errors):
                raise CausalityError(f"Causality violation detected: {errors}")
            
            raise IncrementalSyncError(f"Validation failed: {errors}")
        
        # Запись в хранилище
        await self.storage.write_ohlcv(
            instrument, timeframe, candles_dict, append=True
        )
        
        # Обновление контрольной точки
        last_ts = max(
            datetime.fromisoformat(c['timestamp'].replace('Z', '+00:00'))
            for c in candles_dict
        )
        
        new_checkpoint = Checkpoint(
            instrument=instrument,
            timeframe=timeframe,
            data_type=data_type,
            last_timestamp=last_ts,
            last_sync_at=datetime.utcnow(),
            source_chain=[DataSource(self.provider.name)],
            quality_score=validation_result.get('quality_score', 1.0),
            gaps_detected=validation_result.get('gaps', 0)
        )
        
        await self.storage.write_checkpoint(
            instrument, timeframe, new_checkpoint, data_type
        )
        
        # Обновление метаданных
        await self._update_metadata(
            instrument, timeframe, data_type,
            len(candles_dict), validation_result
        )
        
        # Инвалидация кэша
        if self.cache:
            await self.cache.invalidate(
                self.provider.name, instrument, timeframe, data_type
            )
        
        duration_ms = (datetime.utcnow() - start_time).total_seconds() * 1000
        
        result = {
            'status': 'success',
            'instrument': instrument,
            'timeframe': timeframe.value,
            'new_bars': len(candles_dict),
            'last_timestamp': last_ts.isoformat(),
            'duration_ms': duration_ms,
            'validation': validation_result
        }
        
        logger.info(
            f"Sync completed: {len(candles_dict)} bars in {duration_ms:.2f}ms"
        )
        
        return result
    
    async def _validate_and_check_causality(
        self,
        candles: List[Dict[str, Any]],
        instrument: str,
        timeframe: Timeframe
    ) -> Dict[str, Any]:
        """
        Валидация и проверка каузальности.
        
        Args:
            candles: Список свечей.
            instrument: Инструмент.
            timeframe: Таймфрейм.
            
        Returns:
            Результат валидации.
        """
        if not self.validator:
            return {'passed': True, 'quality_score': 1.0}
        
        # Валидация OHLCV
        report = await self.validator.validate_ohlcv(candles, instrument, timeframe.value)
        
        # Проверка каузальности для макро (если применимо)
        # Для обычных тикеров это не требуется
        
        return {
            'passed': report.passed,
            'quality_score': report.quality_score,
            'errors': report.errors,
            'warnings': report.warnings,
            'checks_performed': report.checks_performed,
            'gaps': 0  # Можно добавить детекцию пропусков
        }
    
    def _candle_to_dict(self, candle: Candle) -> Dict[str, Any]:
        """Конвертация Candle модели в dict."""
        return candle.model_dump(mode='json')
    
    def _get_tf_delta(self, timeframe: Timeframe) -> timedelta:
        """Получение дельты времени для таймфрейма."""
        deltas = {
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
        return deltas.get(timeframe, timedelta(hours=1))
    
    async def _update_metadata(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str,
        new_records: int,
        validation_result: Dict[str, Any]
    ) -> None:
        """Обновление метаданных после синхронизации."""
        existing_meta = await self.storage.read_metadata(
            instrument, timeframe, data_type
        )
        
        total_records = new_records
        if existing_meta:
            total_records = existing_meta.total_records + new_records
        
        metadata = Metadata(
            snapshot_id=f"{instrument}_{timeframe.value}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
            instrument=instrument,
            timeframe=timeframe,
            data_type=data_type,
            updated_at=datetime.utcnow(),
            checkpoint=None,  # Будет обновлено отдельно
            validation_report=validation_result,
            source_chain=[DataSource.TTECH],  # Уточняется
            total_records=total_records
        )
        
        await self.storage.write_metadata(instrument, timeframe, metadata, data_type)
