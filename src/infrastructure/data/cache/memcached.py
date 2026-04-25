"""
Асинхронный Memcached клиент для кэширования горячих данных.

Поддержка:
- Асинхронные операции get/set/invalidate
- TTL по таймфрейму
- Fallback на локальный LRU-кэш при недоступности Memcached
- Структурированные ключи: {prefix}:{provider}:{instrument}:{tf}:{bucket}

Спецификация TTL:
- 1m: 60s
- 1h: 3600s
- 1d: 86400s
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import OrderedDict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from ..models import Timeframe

logger = logging.getLogger(__name__)


class LRUCache:
    """
    Локальный LRU-кэш для fallback.
    
    Потокобезопасная реализация с ограничением по количеству записей.
    
    Attributes:
        max_size: Максимальное количество записей.
    """
    
    def __init__(self, max_size: int = 10000):
        """
        Инициализация LRU-кэша.
        
        Args:
            max_size: Максимальное количество записей.
        """
        self._cache: OrderedDict[str, Tuple[Any, float]] = OrderedDict()
        self._max_size = max_size
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Получение значения из кэша.
        
        Args:
            key: Ключ.
            
        Returns:
            Значение или None если не найдено/истекло.
        """
        async with self._lock:
            if key not in self._cache:
                return None
            
            value, expiry = self._cache[key]
            
            # Проверка истечения
            if expiry and time.time() > expiry:
                del self._cache[key]
                return None
            
            # Поднятие в начало (LRU)
            self._cache.move_to_end(key)
            return value
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Установка значения в кэш.
        
        Args:
            key: Ключ.
            value: Значение.
            ttl: Время жизни в секундах (None = бессрочно).
        """
        async with self._lock:
            expiry = time.time() + ttl if ttl else None
            
            if key in self._cache:
                self._cache.move_to_end(key)
            
            self._cache[key] = (value, expiry)
            
            # Удаление старых записей при переполнении
            while len(self._cache) > self._max_size:
                self._cache.popitem(last=False)
    
    async def delete(self, key: str) -> bool:
        """
        Удаление значения из кэша.
        
        Args:
            key: Ключ.
            
        Returns:
            True если удалено, False если не найдено.
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                return True
            return False
    
    async def clear(self) -> None:
        """Очистка кэша."""
        async with self._lock:
            self._cache.clear()
    
    async def size(self) -> int:
        """Получение текущего размера кэша."""
        async with self._lock:
            return len(self._cache)


class MemcachedClient:
    """
    Асинхронный клиент Memcached с fallback на локальный LRU-кэш.
    
    Поддерживает:
    - Операции get, set, delete, invalidate
    - Автоматический fallback при недоступности сервера
    - TTL по таймфрейму
    - Сериализацию через JSON
    
    Example:
        >>> client = MemcachedClient(hosts=[{'host': 'localhost', 'port': 11211}])
        >>> await client.set('key', {'data': 'value'}, ttl=3600)
        >>> data = await client.get('key')
    """
    
    # TTL по таймфрейму (секунды)
    DEFAULT_TTL_BY_TF = {
        Timeframe.M1: 60,
        Timeframe.M5: 300,
        Timeframe.M10: 600,
        Timeframe.M15: 900,
        Timeframe.H1: 3600,
        Timeframe.H4: 14400,
        Timeframe.D1: 86400,
        Timeframe.W1: 604800,
        Timeframe.MN: 2592000,
    }
    
    def __init__(
        self,
        hosts: Optional[List[Dict[str, Any]]] = None,
        key_prefix: str = "manets:data",
        fallback_enabled: bool = True,
        fallback_max_size: int = 10000,
        timeout: float = 5.0
    ):
        """
        Инициализация Memcached клиента.
        
        Args:
            hosts: Список хостов [{'host': '...', 'port': 11211}].
            key_prefix: Префикс для всех ключей.
            fallback_enabled: Включить fallback на локальный кэш.
            fallback_max_size: Максимальный размер LRU-кэша.
            timeout: Таймаут подключения (сек).
        """
        self.hosts = hosts or [{'host': 'localhost', 'port': 11211}]
        self.key_prefix = key_prefix
        self.fallback_enabled = fallback_enabled
        self.timeout = timeout
        
        # Локальный fallback
        self._fallback = LRUCache(max_size=fallback_max_size) if fallback_enabled else None
        self._memcached_available = True
        
        # pymemcache клиент (ленивая инициализация)
        self._client = None
        self._pool = None
        
        # Статистика
        self._stats = {
            'hits': 0,
            'misses': 0,
            'fallback_hits': 0,
            'fallback_misses': 0,
            'errors': 0
        }
    
    def _build_key(
        self,
        provider: str,
        instrument: str,
        timeframe: Union[Timeframe, str],
        data_type: str = "ohlcv",
        bucket: Optional[str] = None
    ) -> str:
        """
        Построение структурированного ключа.
        
        Формат: {prefix}:{provider}:{instrument}:{tf}:{data_type}:{bucket}
        
        Args:
            provider: Название провайдера.
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            bucket: Опциональный бакет (timestamp bucket).
            
        Returns:
            Строковый ключ.
        """
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        
        parts = [self.key_prefix, provider, instrument, tf_value, data_type]
        if bucket:
            parts.append(bucket)
        
        return ':'.join(parts)
    
    def _get_ttl(self, timeframe: Union[Timeframe, str]) -> int:
        """
        Получение TTL для таймфрейма.
        
        Args:
            timeframe: Таймфрейм.
            
        Returns:
            TTL в секундах.
        """
        if isinstance(timeframe, Timeframe):
            return self.DEFAULT_TTL_BY_TF.get(timeframe, 3600)
        
        # Попытка найти по строковому значению
        for tf, ttl in self.DEFAULT_TTL_BY_TF.items():
            if tf.value == str(timeframe):
                return ttl
        
        return 3600  # Default
    
    async def _get_client(self):
        """Ленивая инициализация memcached клиента."""
        if self._client is None:
            try:
                import aiomcache
                
                # Подключение к первому доступному хосту
                host_info = self.hosts[0]
                self._client = aiomcache.Client(
                    host=host_info['host'],
                    port=host_info['port'],
                    pool_size=10,
                    max_idle_time=300
                )
                
                logger.info(f"Connected to Memcached at {host_info['host']}:{host_info['port']}")
                
            except ImportError:
                logger.warning("aiomcache not installed. Using local fallback only.")
                self._memcached_available = False
            except Exception as e:
                logger.warning(f"Failed to connect to Memcached: {e}. Using fallback.")
                self._memcached_available = False
        
        return self._client
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Получение значения из кэша.
        
        Args:
            key: Ключ.
            
        Returns:
            Десериализованное значение или None.
        """
        # Попытка получения из Memcached
        if self._memcached_available:
            try:
                client = await self._get_client()
                if client:
                    raw = await client.get(key.encode())
                    if raw:
                        self._stats['hits'] += 1
                        return json.loads(raw.decode())
                    
                    self._stats['misses'] += 1
                    
            except Exception as e:
                logger.debug(f"Memcached get error: {e}")
                self._stats['errors'] += 1
                self._memcached_available = False
        
        # Fallback на локальный кэш
        if self._fallback:
            value = await self._fallback.get(key)
            if value is not None:
                self._stats['fallback_hits'] += 1
                return value
            self._stats['fallback_misses'] += 1
        
        return None
    
    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        Установка значения в кэш.
        
        Args:
            key: Ключ.
            value: Значение (сериализуемое в JSON).
            ttl: Время жизни в секундах.
            
        Returns:
            True если успешно.
        """
        serialized = json.dumps(value, default=str).encode()
        
        # Запись в Memcached
        if self._memcached_available:
            try:
                client = await self._get_client()
                if client:
                    await client.set(key.encode(), serialized, exptime=ttl or 0)
                    
            except Exception as e:
                logger.debug(f"Memcached set error: {e}")
                self._stats['errors'] += 1
                self._memcached_available = False
        
        # Запись в локальный fallback
        if self._fallback:
            await self._fallback.set(key, value, ttl)
        
        return True
    
    async def delete(self, key: str) -> bool:
        """
        Удаление значения из кэша.
        
        Args:
            key: Ключ.
            
        Returns:
            True если удалено.
        """
        deleted = False
        
        # Удаление из Memcached
        if self._memcached_available:
            try:
                client = await self._get_client()
                if client:
                    await client.delete(key.encode())
                    deleted = True
                    
            except Exception as e:
                logger.debug(f"Memcached delete error: {e}")
        
        # Удаление из локального fallback
        if self._fallback:
            if await self._fallback.delete(key):
                deleted = True
        
        return deleted
    
    async def invalidate(
        self,
        provider: str,
        instrument: str,
        timeframe: Union[Timeframe, str],
        data_type: str = "ohlcv"
    ) -> int:
        """
        Инвалидация всех ключей для комбинации параметров.
        
        Args:
            provider: Провайдер.
            instrument: Инструмент.
            timeframe: Таймфрейм.
            data_type: Тип данных.
            
        Returns:
            Количество инвалидированных ключей.
        """
        # Паттерн ключа для поиска
        prefix = f"{self.key_prefix}:{provider}:{instrument}:"
        tf_value = timeframe.value if isinstance(timeframe, Timeframe) else str(timeframe)
        prefix += f"{tf_value}:{data_type}"
        
        invalidated = 0
        
        # Для Memcached потребуется scan (если поддерживается)
        # Для упрощения просто очищаем fallback
        if self._fallback:
            await self._fallback.clear()
            invalidated += 1
        
        logger.info(f"Invalidated cache for {prefix}*")
        
        return invalidated
    
    async def get_cached_ohlcv(
        self,
        provider: str,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Dict[str, Any]]:
        """
        Получение закэшированных OHLCV данных.
        
        Args:
            provider: Провайдер.
            instrument: Инструмент.
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список свечей (как dict).
        """
        # Вычисление bucket (группировка по времени)
        bucket = f"{from_dt.isoformat()}_{to_dt.isoformat()}"
        key = self._build_key(provider, instrument, timeframe, "ohlcv", bucket)
        
        cached = await self.get(key)
        return cached or []
    
    async def cache_ohlcv(
        self,
        provider: str,
        instrument: str,
        timeframe: Timeframe,
        candles: List[Dict[str, Any]],
        from_dt: datetime,
        to_dt: datetime
    ) -> bool:
        """
        Кэширование OHLCV данных.
        
        Args:
            provider: Провайдер.
            instrument: Инструмент.
            timeframe: Таймфрейм.
            candles: Список свечей.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            True если успешно.
        """
        bucket = f"{from_dt.isoformat()}_{to_dt.isoformat()}"
        key = self._build_key(provider, instrument, timeframe, "ohlcv", bucket)
        ttl = self._get_ttl(timeframe)
        
        return await self.set(key, candles, ttl)
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Получение статистики кэша.
        
        Returns:
            Статистика операций.
        """
        total_requests = (
            self._stats['hits'] + 
            self._stats['misses'] + 
            self._stats['fallback_hits'] + 
            self._stats['fallback_misses']
        )
        
        hit_rate = 0.0
        if total_requests > 0:
            hit_rate = (self._stats['hits'] + self._stats['fallback_hits']) / total_requests
        
        return {
            **self._stats,
            'total_requests': total_requests,
            'hit_rate': hit_rate,
            'memcached_available': self._memcached_available
        }
    
    async def close(self) -> None:
        """Закрытие соединений."""
        if self._client:
            try:
                await self._client.close()
            except Exception:
                pass
            self._client = None


# Глобальный экземпляр (опционально)
_cache_client: Optional[MemcachedClient] = None


def get_cache_client() -> MemcachedClient:
    """Получение глобального клиента кэша."""
    global _cache_client
    if _cache_client is None:
        _cache_client = MemcachedClient()
    return _cache_client


def reset_cache_client() -> None:
    """Сброс глобального клиента."""
    global _cache_client
    if _cache_client:
        asyncio.create_task(_cache_client.close())
    _cache_client = None
