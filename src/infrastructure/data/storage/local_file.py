"""
Локальное файловое хранилище данных.

Поддержка:
- JSON/Parquet форматы
- Атомарная запись (tmp → rename)
- Структура по спецификации: data/storage/tickers/{TICKER}/{tf}/
- Чтение/запись метаданных и checkpoint

Структура хранения:
    data/storage/
    ├── tickers/{TICKER}/{timeframe}/ohlcv.json
    ├── macro/{INSTRUMENT}/{timeframe}/candles.json
    └── metadata/{instrument}_{tf}_metadata.json
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import tempfile
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..models import Timeframe, Checkpoint, Metadata, ValidationReport, DataSource

logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder для Decimal."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Timeframe):
            return obj.value
        if isinstance(obj, DataSource):
            return obj.value
        return super().default(obj)


class LocalFileStorage:
    """
    Локальное файловое хранилище с атомарной записью.
    
    Поддерживает:
    - Запись в JSON (по умолчанию) или Parquet
    - Атомарность через временные файлы + rename
    - Чтение/запись метаданных
    - Управление контрольными точками
    
    Example:
        >>> storage = LocalFileStorage(base_path="data/storage")
        >>> await storage.write_ohlcv("SNGS", Timeframe.D1, candles)
        >>> data = await storage.read_ohlcv("SNGS", Timeframe.D1)
    """
    
    def __init__(
        self,
        base_path: str = "data/storage",
        format: str = "json",
        atomic_writes: bool = True,
        compression: bool = False,
        backup_enabled: bool = True,
        backup_path: str = "data/backtest/snapshots"
    ):
        """
        Инициализация хранилища.
        
        Args:
            base_path: Базовый путь к хранилищу.
            format: Формат файлов (json/parquet).
            atomic_writes: Использовать атомарную запись.
            compression: Включить сжатие.
            backup_enabled: Включить резервное копирование.
            backup_path: Путь для бэкапов.
        """
        self.base_path = Path(base_path)
        self.format = format.lower()
        self.atomic_writes = atomic_writes
        self.compression = compression
        self.backup_enabled = backup_enabled
        self.backup_path = Path(backup_path)
        
        # Создание директорий
        self._ensure_directories()
        
        # Блокировки для потокобезопасности
        self._locks: Dict[str, asyncio.Lock] = {}
    
    def _ensure_directories(self) -> None:
        """Создание необходимых директорий."""
        dirs = [
            self.base_path / "tickers",
            self.base_path / "macro",
            self.base_path / "events",
            self.base_path.parent / "metadata",
        ]
        
        if self.backup_enabled:
            dirs.append(self.backup_path)
        
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)
    
    def _get_lock(self, key: str) -> asyncio.Lock:
        """Получение блокировки для ключа."""
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]
    
    def _get_ticker_path(
        self,
        ticker: str,
        timeframe: Union[Timeframe, str],
        data_type: str = "ohlcv"
    ) -> Path:
        """
        Получение пути для тикера.
        
        Args:
            ticker: Тикер.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Путь к файлу.
        """
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        
        if data_type == "ohlcv":
            return self.base_path / "tickers" / ticker / tf_value / "ohlcv.json"
        elif data_type == "lob":
            return self.base_path / "tickers" / ticker / "lob" / f"snapshots_{tf_value}.json"
        elif data_type == "fundamentals":
            return self.base_path / "tickers" / ticker / "fundamentals.json"
        else:
            return self.base_path / "tickers" / ticker / tf_value / f"{data_type}.json"
    
    def _get_macro_path(
        self,
        instrument: str,
        timeframe: Union[Timeframe, str]
    ) -> Path:
        """
        Получение пути для макро-данных.
        
        Args:
            instrument: Макро-инструмент.
            timeframe: Таймфрейм.
            
        Returns:
            Путь к файлу.
        """
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        return self.base_path / "macro" / instrument / tf_value / "candles.json"
    
    def _get_metadata_path(
        self,
        instrument: str,
        timeframe: Union[Timeframe, str],
        data_type: str = "ohlcv"
    ) -> Path:
        """
        Получение пути для метаданных.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Путь к файлу метаданных.
        """
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        safe_name = f"{instrument}_{tf_value}_{data_type}".replace('/', '_')
        return self.base_path.parent / "metadata" / f"{safe_name}_metadata.json"
    
    def _get_checkpoint_path(
        self,
        instrument: str,
        timeframe: Union[Timeframe, str],
        data_type: str = "ohlcv"
    ) -> Path:
        """
        Получение пути для checkpoint.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Путь к файлу checkpoint.
        """
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        safe_name = f"{instrument}_{tf_value}_{data_type}".replace('/', '_')
        return self.base_path.parent / "metadata" / f"{safe_name}_checkpoint.json"
    
    async def _atomic_write(self, path: Path, data: Any) -> None:
        """
        Атомарная запись данных.
        
        Пишет во временный файл, затем переименовывает.
        
        Args:
            path: Целевой путь.
            data: Данные для записи.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        if self.atomic_writes:
            # Создание временного файла в той же директории
            fd, tmp_path = tempfile.mkstemp(
                suffix='.tmp',
                dir=path.parent
            )
            
            try:
                with os.fdopen(fd, 'w', encoding='utf-8') as f:
                    json.dump(data, f, cls=DecimalEncoder, indent=2, ensure_ascii=False)
                
                # Атомарное переименование
                shutil.move(tmp_path, path)
                
            except Exception as e:
                # Очистка временного файла при ошибке
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        else:
            # Прямая запись (не атомарно)
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(data, f, cls=DecimalEncoder, indent=2, ensure_ascii=False)
    
    async def read_json(self, path: Path) -> Optional[Any]:
        """
        Чтение JSON файла.
        
        Args:
            path: Путь к файлу.
            
        Returns:
            Данные или None если файл не существует.
        """
        if not path.exists():
            return None
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {path}: {e}")
            return None
    
    async def write_ohlcv(
        self,
        ticker: str,
        timeframe: Union[Timeframe, str],
        candles: List[Dict[str, Any]],
        append: bool = True,
        data_type: str = "ohlcv"
    ) -> bool:
        """
        Запись OHLCV или макро данных.
        
        Args:
            ticker: Тикер/инструмент.
            timeframe: Таймфрейм.
            candles: Список свечей (как dict).
            append: Добавить к существующим (True) или заменить (False).
            data_type: Тип данных ("ohlcv" или "macro").
            
        Returns:
            True если успешно.
        """
        # Конвертация timeframe в Timeframe enum если это строка
        if isinstance(timeframe, str):
            timeframe = Timeframe(timeframe)
        
        lock_key = f"{data_type}:{ticker}:{timeframe.value}"
        async with self._get_lock(lock_key):
            path = self._get_ticker_path(ticker, timeframe, data_type)
            
            existing = []
            if append:
                existing_data = await self.read_json(path)
                if existing_data and isinstance(existing_data, list):
                    existing = existing_data
            
            # Конкатенация и удаление дубликатов
            combined = existing + candles
            combined = self._deduplicate_candles(combined)
            
            await self._atomic_write(path, combined)
            
            logger.debug(f"Written {len(candles)} {data_type} to {path} (total: {len(combined)})")
            return True
    
    def _deduplicate_candles(self, candles: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Удаление дубликатов свечей по timestamp.
        
        Args:
            candles: Список свечей (dict или Candle объекты).
            
        Returns:
            Список без дубликатов.
        """
        seen = {}
        for candle in candles:
            # Обработка как dict так и Candle объектов
            if hasattr(candle, 'timestamp'):
                # Это Candle объект
                ts = candle.timestamp.isoformat() if hasattr(candle.timestamp, 'isoformat') else str(candle.timestamp)
                is_complete = getattr(candle, 'is_complete', False)
                candle_data = candle.model_dump(mode='json') if hasattr(candle, 'model_dump') else candle
            else:
                # Это dict
                ts = candle.get('timestamp')
                is_complete = candle.get('is_complete', False)
                candle_data = candle
            
            if ts not in seen:
                seen[ts] = candle_data
            else:
                # Обновление если новая версия (например, is_complete изменился)
                if is_complete and not seen[ts].get('is_complete', False):
                    seen[ts] = candle_data
        
        # Сортировка по timestamp
        return sorted(seen.values(), key=lambda c: c.get('timestamp', ''))
    
    async def read_ohlcv(
        self,
        ticker: str,
        timeframe: Timeframe,
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """
        Чтение OHLCV данных.
        
        Args:
            ticker: Тикер.
            timeframe: Таймфрейм.
            from_dt: Начало периода (опционально).
            to_dt: Конец периода (опционально).
            
        Returns:
            Список свечей.
        """
        path = self._get_ticker_path(ticker, timeframe, "ohlcv")
        data = await self.read_json(path)
        
        if not data or not isinstance(data, list):
            return []
        
        # Фильтрация по периоду
        if from_dt or to_dt:
            filtered = []
            for candle in data:
                ts_str = candle.get('timestamp', '')
                try:
                    ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    if from_dt and ts < from_dt:
                        continue
                    if to_dt and ts > to_dt:
                        continue
                    filtered.append(candle)
                except (ValueError, TypeError):
                    filtered.append(candle)  # Оставляем если не можем распарсить
            
            return filtered
        
        return data
    
    async def write_metadata(
        self,
        instrument: str,
        timeframe: Timeframe,
        metadata: Metadata,
        data_type: str = "ohlcv"
    ) -> bool:
        """
        Запись метаданных.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            metadata: Объект метаданных.
            data_type: Тип данных.
            
        Returns:
            True если успешно.
        """
        path = self._get_metadata_path(instrument, timeframe, data_type)
        
        # Преобразование модели в dict
        data = metadata.model_dump(mode='json')
        
        await self._atomic_write(path, data)
        return True
    
    async def read_metadata(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> Optional[Metadata]:
        """
        Чтение метаданных.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Metadata объект или None.
        """
        path = self._get_metadata_path(instrument, timeframe, data_type)
        data = await self.read_json(path)
        
        if not data:
            return None
        
        try:
            return Metadata(**data)
        except Exception as e:
            logger.error(f"Failed to parse metadata: {e}")
            return None
    
    async def write_checkpoint(
        self,
        instrument: str,
        timeframe: Timeframe,
        checkpoint: Checkpoint,
        data_type: str = "ohlcv"
    ) -> bool:
        """
        Запись контрольной точки.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            checkpoint: Checkpoint объект.
            data_type: Тип данных.
            
        Returns:
            True если успешно.
        """
        path = self._get_checkpoint_path(instrument, timeframe, data_type)
        
        data = checkpoint.model_dump(mode='json')
        
        await self._atomic_write(path, data)
        return True
    
    async def read_checkpoint(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> Optional[Checkpoint]:
        """
        Чтение контрольной точки.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Checkpoint объект или None.
        """
        path = self._get_checkpoint_path(instrument, timeframe, data_type)
        data = await self.read_json(path)
        
        if not data:
            return None
        
        try:
            return Checkpoint(**data)
        except Exception as e:
            logger.error(f"Failed to parse checkpoint: {e}")
            return None
    
    async def write_validation_report(
        self,
        instrument: str,
        timeframe: Timeframe,
        report: ValidationReport,
        data_type: str = "ohlcv"
    ) -> bool:
        """Запись отчёта валидации."""
        path = self._get_ticker_path(instrument, timeframe, data_type).parent / "validation_report.json"
        
        data = report.model_dump(mode='json')
        
        await self._atomic_write(path, data)
        return True
    
    async def exists(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> bool:
        """
        Проверка существования данных.
        
        Args:
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            True если файл существует.
        """
        if data_type == "ohlcv":
            path = self._get_ticker_path(instrument, timeframe, data_type)
        else:
            path = self._get_macro_path(instrument, timeframe)
        
        return path.exists()
    
    async def get_file_size(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> int:
        """Получение размера файла в байтах."""
        if data_type == "ohlcv":
            path = self._get_ticker_path(instrument, timeframe, data_type)
        else:
            path = self._get_macro_path(instrument, timeframe)
        
        if path.exists():
            return path.stat().st_size
        return 0
    
    async def backup_snapshot(
        self,
        snapshot_id: str,
        instruments: List[str],
        timeframes: List[Timeframe]
    ) -> Path:
        """
        Создание бэкапа для backtest.
        
        Args:
            snapshot_id: ID снэпшота.
            instruments: Список инструментов.
            timeframes: Список таймфреймов.
            
        Returns:
            Путь к бэкапу.
        """
        if not self.backup_enabled:
            raise RuntimeError("Backup is disabled")
        
        snapshot_path = self.backup_path / snapshot_id
        snapshot_path.mkdir(parents=True, exist_ok=True)
        
        # Копирование данных
        for instrument in instruments:
            for tf in timeframes:
                src = self._get_ticker_path(instrument, tf)
                if src.exists():
                    dst = snapshot_path / f"{instrument}_{tf.value}.json"
                    shutil.copy2(src, dst)
        
        # Создание манифеста
        manifest = {
            'snapshot_id': snapshot_id,
            'created_at': datetime.utcnow().isoformat(),
            'instruments': instruments,
            'timeframes': [tf.value for tf in timeframes]
        }
        
        with open(snapshot_path / "manifest.json", 'w') as f:
            json.dump(manifest, f, indent=2)
        
        logger.info(f"Created backup snapshot: {snapshot_path}")
        return snapshot_path
