"""
Оркестратор Data Collector.

Центральный компонент для управления сбором данных:
- Маршрутизация через ProviderRouter
- Параллельная загрузка с asyncio.Semaphore
- Инкрементальная синхронизация
- Кэширование и валидация
- Структурированное логирование

Example:
    >>> collector = DataCollector(config)
    >>> await collector.collect("SNGS", Timeframe.D1)
    >>> result = await collector.incremental_sync(["SNGS", "GAZP"], Timeframe.D1)
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Union

from .models import (
    Candle, L2OrderBook, Trade, MacroCandle,
    Fundamental, CorporateEvent, Timeframe, DataSource,
    ValidationReport, Checkpoint, Metadata
)
from .config import DataCollectorConfig, load_config
from .providers.base import DataProvider, ProviderRouter
from .providers.ttech import TtechProvider
from .providers.moexalgo import MoexAlgoProvider
from .cache.memcached import MemcachedClient
from .storage.local_file import LocalFileStorage
from .sync.incremental import IncrementalSynchronizer, CausalityError, IncrementalSyncError
from .validator import DataValidator

logger = logging.getLogger(__name__)


class DataCollector:
    """
    Центральный оркестратор сбора данных.
    
    Координирует работу провайдеров, кэша, хранилища и валидатора.
    Поддерживает параллельную загрузку и инкрементальную синхронизацию.
    
    Example:
        >>> collector = DataCollector.from_config()
        >>> await collector.start()
        >>> candles = await collector.get_latest("SNGS", Timeframe.D1)
        >>> await collector.stop()
    """
    
    def __init__(
        self,
        config: DataCollectorConfig,
        providers: Optional[List[DataProvider]] = None,
        cache: Optional[MemcachedClient] = None,
        storage: Optional[LocalFileStorage] = None,
        validator: Optional[DataValidator] = None
    ):
        """
        Инициализация коллектора.
        
        Args:
            config: Конфигурация.
            providers: Список провайдеров (создаются из конфига если None).
            cache: Клиент кэша (создаётся из конфига если None).
            storage: Хранилище (создаётся из конфига если None).
            validator: Валидатор (создаётся по умолчанию если None).
        """
        self.config = config
        
        # Инициализация компонентов
        self._providers = providers or self._create_providers()
        self._router = ProviderRouter(self._providers)
        self._cache = cache or self._create_cache()
        self._storage = storage or self._create_storage()
        self._validator = validator or DataValidator(
            strict_mode=self.config.validation.strict_mode
        )
        
        # Semaphore для ограничения параллелизма
        self._semaphore = asyncio.Semaphore(
            self.config.general.max_concurrent_fetches
        )
        
        # Синхронизаторы (ленивое создание)
        self._syncs: Dict[str, IncrementalSynchronizer] = {}
        
        # Статистика
        self._stats = {
            'collections': 0,
            'errors': 0,
            'cache_hits': 0,
            'cache_misses': 0
        }
        
        self._started = False
    
    def _create_providers(self) -> List[DataProvider]:
        """Создание провайдеров из конфигурации."""
        providers = []
        
        prov_configs = self.config.providers
        
        # T-Tech (primary) - только для USD_RUB и EUR_RUB в macro
        if 'primary' in prov_configs:
            cfg = prov_configs['primary']
            try:
                logger.info(f"Initializing T-Tech provider with config: priority={cfg.priority}, endpoints={cfg.endpoints}")
                logger.debug(f"Auth token env var: {cfg.auth_token_env}")
                
                # Проверка наличия токена
                import os
                token = os.environ.get(cfg.auth_token_env, '')
                if not token:
                    logger.error(f"INVEST_TOKEN is empty or not set. T-Tech provider will not be initialized.")
                else:
                    logger.info(f"INVEST_TOKEN found (length={len(token)}), starts with: {token[:5]}...")
                
                provider_instance = TtechProvider(
                    config={
                        'priority': cfg.priority,
                        'endpoints': cfg.endpoints,
                        'auth_token_env': cfg.auth_token_env
                    },
                    rate_limit_config=cfg.rate_limit.model_dump() if cfg.rate_limit else None,
                    circuit_breaker_config=cfg.circuit_breaker.model_dump() if cfg.circuit_breaker else None
                )
                providers.append(provider_instance)
                logger.info("T-Tech provider initialized successfully")
            except ImportError as ie:
                logger.error(f"Failed to import T-Tech dependencies: {ie}. Please install t-tech-investments package.", exc_info=True)
            except Exception as e:
                logger.error(f"Failed to initialize T-Tech provider: {e}", exc_info=True)
        
        # MoexAlgo (secondary) - для всех остальных macro инструментов
        if 'secondary' in prov_configs:
            cfg = prov_configs['secondary']
            try:
                logger.info(f"Initializing MoexAlgo provider with config: priority={cfg.priority}, base_url={cfg.endpoints.get('base_url', 'N/A')}")
                
                # Подготовка конфигурации макро-инструментов для MoexAlgo
                macro_instruments_config = None
                if hasattr(self.config, 'instruments') and self.config.instruments:
                    macro_instruments_config = {
                        'currencies': [instr.model_dump() for instr in self.config.instruments.macro.currencies],
                        'commodities': [instr.model_dump() for instr in self.config.instruments.macro.commodities],
                        'indices': [instr.model_dump() for instr in self.config.instruments.macro.indices],
                        'rates': [instr.model_dump() for instr in self.config.instruments.macro.rates]
                    }
                
                provider_instance = MoexAlgoProvider(
                    config={
                        'priority': cfg.priority,
                        'endpoints': cfg.endpoints,
                        'request_timeout': cfg.request_timeout_sec
                    },
                    rate_limit_config=cfg.rate_limit.model_dump() if cfg.rate_limit else None,
                    circuit_breaker_config=cfg.circuit_breaker.model_dump() if cfg.circuit_breaker else None,
                    macro_instruments_config=macro_instruments_config
                )
                providers.append(provider_instance)
                logger.info("MoexAlgo provider initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize MoexAlgo provider: {e}", exc_info=True)
        
        if not providers:
            logger.critical("No providers were initialized! Data collection will fail.")
        
        return providers
    
    def _create_cache(self) -> MemcachedClient:
        """Создание клиента кэша."""
        cache_cfg = self.config.cache.memcached
        local_cfg = self.config.cache.local_fallback
        
        return MemcachedClient(
            hosts=cache_cfg.hosts,
            key_prefix=cache_cfg.key_prefix,
            fallback_enabled=cache_cfg.fallback_to_local,
            fallback_max_size=local_cfg.lru_max_entries
        )
    
    def _create_storage(self) -> LocalFileStorage:
        """Создание хранилища."""
        storage_cfg = self.config.storage
        
        return LocalFileStorage(
            base_path=storage_cfg.base_path,
            format=storage_cfg.format,
            atomic_writes=storage_cfg.atomic_writes,
            compression=storage_cfg.compression,
            backup_enabled=storage_cfg.backup_enabled,
            backup_path=storage_cfg.backup_path
        )
    
    async def start(self) -> None:
        """Запуск коллектора."""
        logger.info("Starting DataCollector...")
        self._started = True
    
    async def stop(self) -> None:
        """Остановка коллектора."""
        logger.info("Stopping DataCollector...")
        self._started = False
        
        # Закрытие соединений
        for provider in self._providers:
            if hasattr(provider, 'close'):
                await provider.close()
        
        if self._cache:
            await self._cache.close()
    
    async def collect(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv",
        from_dt: Optional[datetime] = None,
        to_dt: Optional[datetime] = None
    ) -> List[Candle]:
        """
        Сбор данных для инструмента.
        
        Args:
            instrument: Тикер/инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список свечей.
        """
        if not self._started:
            raise RuntimeError("DataCollector not started. Call start() first.")
        
        start_time = time.monotonic()
        
        async with self._semaphore:
            try:
                # Попытка получения из кэша
                cached = await self._get_from_cache(
                    instrument, timeframe, data_type, from_dt, to_dt
                )
                
                if cached:
                    self._stats['cache_hits'] += 1
                    logger.debug(f"Cache hit for {instrument} {timeframe.value}")
                    return cached
                
                self._stats['cache_misses'] += 1
                
                # Запрос у провайдера
                provider = self._router.get_provider(data_type, instrument)
                
                if not provider:
                    raise RuntimeError(f"No available provider for {data_type}:{instrument}")
                
                if from_dt is None:
                    # Вычисляем глубину сбора на основе таймфрейма из конфига
                    depth_years = self.config.history_depth.get_depth_for_timeframe(timeframe.value)
                    from_dt = datetime.now(timezone.utc) - timedelta(days=depth_years * 365)
                    logger.info(f"Using history depth of {depth_years} years for {instrument} {timeframe.value}")
                
                if to_dt is None:
                    to_dt = datetime.now(timezone.utc)
                
                candles = await provider.get_ohlcv(instrument, timeframe, from_dt, to_dt)
                
                # Валидация
                candles_dict = [c.model_dump(mode='json') for c in candles]
                report = await self._validator.validate_ohlcv(
                    candles_dict, instrument, timeframe.value
                )
                
                if not report.passed:
                    logger.warning(f"Validation warnings for {instrument}: {report.warnings}")
                
                # Кэширование
                await self._save_to_cache(
                    instrument, timeframe, data_type, candles_dict, from_dt, to_dt
                )
                
                self._stats['collections'] += 1
                
                duration_ms = (time.monotonic() - start_time) * 1000
                logger.info(
                    f"Collected {len(candles)} candles for {instrument} {timeframe.value} "
                    f"in {duration_ms:.2f}ms"
                )
                
                return candles
                
            except Exception as e:
                self._stats['errors'] += 1
                logger.error(f"Collection failed for {instrument}: {e}")
                raise
    
    async def collect_batch(
        self,
        instruments: List[str],
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> Dict[str, List[Candle]]:
        """
        Параллельный сбор данных для нескольких инструментов.
        
        Args:
            instruments: Список тикеров.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Словарь {instrument: candles}.
        """
        tasks = [
            self.collect(inst, timeframe, data_type)
            for inst in instruments
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        for inst, result in zip(instruments, results):
            if isinstance(result, Exception):
                logger.error(f"Batch collection failed for {inst}: {result}")
                output[inst] = []
            else:
                output[inst] = result
        
        return output
    
    async def incremental_sync(
        self,
        instruments: List[str],
        timeframe: Timeframe,
        data_type: str = "ohlcv",
        force_full: bool = False
    ) -> Dict[str, Dict[str, Any]]:
        """
        Инкрементальная синхронизация для списка инструментов.
        
        Args:
            instruments: Список тикеров.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            force_full: Принудительная полная загрузка.
            
        Returns:
            Результаты синхронизации {instrument: stats}.
        """
        results = {}
        
        for instrument in instruments:
            try:
                sync = self._get_synchronizer(instrument, timeframe, data_type)
                result = await sync.sync(
                    instrument, timeframe,
                    data_type=data_type,
                    force_full=force_full
                )
                results[instrument] = result
                
            except CausalityError as e:
                logger.error(f"Causality error for {instrument}: {e}")
                results[instrument] = {'status': 'error', 'error': str(e)}
                
            except IncrementalSyncError as e:
                logger.error(f"Sync error for {instrument}: {e}")
                results[instrument] = {'status': 'error', 'error': str(e)}
                
            except Exception as e:
                logger.error(f"Unexpected error for {instrument}: {e}")
                results[instrument] = {'status': 'error', 'error': str(e)}
        
        return results
    
    def _get_synchronizer(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str = "ohlcv"
    ) -> IncrementalSynchronizer:
        """Получение или создание синхронизатора."""
        key = f"{instrument}:{timeframe.value}:{data_type}"
        
        if key not in self._syncs:
            provider = self._router.get_provider(data_type, instrument)
            self._syncs[key] = IncrementalSynchronizer(
                provider=provider,
                storage=self._storage,
                cache=self._cache,
                validator=self._validator
            )
        
        return self._syncs[key]
    
    async def get_latest(
        self,
        instrument: str,
        timeframe: Timeframe,
        limit: int = 100
    ) -> List[Candle]:
        """
        Получение последних N свечей.
        
        Args:
            instrument: Тикер.
            timeframe: Таймфрейм.
            limit: Количество свечей.
            
        Returns:
            Последние N свечей.
        """
        # Чтение из хранилища
        all_data = await self._storage.read_ohlcv(instrument, timeframe)
        
        if not all_data:
            # Если нет данных,尝试 загрузить
            try:
                return await self.collect(instrument, timeframe)
            except Exception:
                return []
        
        # Возврат последних N
        latest = all_data[-limit:]
        
        # Конвертация обратно в модели
        return [Candle(**c) for c in latest]
    
    async def _get_from_cache(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str,
        from_dt: Optional[datetime],
        to_dt: Optional[datetime]
    ) -> List[Candle]:
        """Получение из кэша."""
        if not self._cache:
            return []
        
        provider = self._router.get_provider(data_type, instrument)
        if not provider:
            return []
        
        cached = await self._cache.get_cached_ohlcv(
            provider.name, instrument, timeframe,
            from_dt or datetime(2000, 1, 1),
            to_dt or datetime.utcnow()
        )
        
        if cached:
            return [Candle(**c) for c in cached]
        
        return []
    
    async def _save_to_cache(
        self,
        instrument: str,
        timeframe: Timeframe,
        data_type: str,
        candles: List[Dict[str, Any]],
        from_dt: datetime,
        to_dt: datetime
    ) -> None:
        """Сохранение в кэш."""
        if not self._cache:
            return
        
        provider = self._router.get_provider(data_type, instrument)
        if not provider:
            return
        
        await self._cache.cache_ohlcv(
            provider.name, instrument, timeframe,
            candles, from_dt, to_dt
        )
    
    def get_stats(self) -> Dict[str, Any]:
        """Получение статистики."""
        cache_stats = self._cache.get_stats() if self._cache else {}
        
        return {
            **self._stats,
            'cache': cache_stats,
            'providers': {
                p.name: {
                    'available': p.is_available,
                    'state': p.circuit_breaker_state.value
                }
                for p in self._providers
            }
        }
    
    @property
    def primary_provider(self) -> Optional[DataProvider]:
        """Получение основного провайдера (первый доступный для ohlcv)."""
        if not self._providers:
            return None
        return self._router.get_provider('ohlcv', 'ANY')
    
    def get_provider_for(self, data_type: str, instrument: str) -> Optional[DataProvider]:
        """Получение провайдера для конкретного типа данных и инструмента."""
        return self._router.get_provider(data_type, instrument)
    
    @property
    def storage(self) -> LocalFileStorage:
        """Получение хранилища."""
        return self._storage
    
    @property
    def cache(self) -> Optional[MemcachedClient]:
        """Получение кэша."""
        return self._cache
    
    @property
    def validator(self) -> DataValidator:
        """Получение валидатора."""
        return self._validator
    
    @classmethod
    def from_config(
        cls,
        config_path: Optional[str] = None
    ) -> 'DataCollector':
        """
        Создание коллектора из конфигурационного файла.
        
        Args:
            config_path: Путь к YAML файлу.
            
        Returns:
            Настроенный DataCollector.
        """
        config = load_config(config_path)
        return cls(config)
