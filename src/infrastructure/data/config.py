"""
Конфигурация Data Collector.

Загрузка и валидация конфигурации из YAML файла.
Поддержка переопределения через переменные окружения.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, ValidationError

from .models import ProviderConfig, Timeframe, RateLimitConfig, CircuitBreakerConfig


class GeneralConfig(BaseModel):
    """Общие настройки."""
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="structured_json")
    timezone: str = Field(default="Europe/Moscow")
    max_concurrent_fetches: int = Field(default=10, gt=0)
    request_timeout_sec: float = Field(default=30.0, gt=0)
    retry_max_attempts: int = Field(default=3, gt=0)
    retry_base_delay_sec: float = Field(default=1.0, gt=0)
    retry_max_delay_sec: float = Field(default=60.0, gt=0)


class MemcachedConfig(BaseModel):
    """Настройки Memcached."""
    enabled: bool = True
    hosts: List[Dict[str, Any]] = Field(default_factory=lambda: [{"host": "localhost", "port": 11211}])
    ttl_by_timeframe: Dict[str, int] = Field(default_factory=dict)
    key_prefix: str = Field(default="manets:data")
    fallback_to_local: bool = True


class LocalCacheConfig(BaseModel):
    """Настройки локального кэша (fallback)."""
    enabled: bool = True
    path: str = Field(default="data/cache/hot_data")
    max_size_mb: int = Field(default=500, gt=0)
    lru_max_entries: int = Field(default=10000, gt=0)


class CacheConfig(BaseModel):
    """Конфигурация кэширования."""
    memcached: MemcachedConfig = Field(default_factory=MemcachedConfig)
    local_fallback: LocalCacheConfig = Field(default_factory=LocalCacheConfig)


class StorageConfig(BaseModel):
    """Конфигурация хранилища."""
    base_path: str = Field(default="data/storage")
    format: str = Field(default="json")
    atomic_writes: bool = True
    compression: bool = False
    backup_enabled: bool = True
    backup_path: str = Field(default="data/backtest/snapshots")
    
    class MetadataConfig(BaseModel):
        path: str = Field(default="data/metadata")
        registry_file: str = Field(default="registry.json")
        version_log_file: str = Field(default="version_log.json")
    
    metadata: MetadataConfig = Field(default_factory=MetadataConfig)


class ValidationCheckConfig(BaseModel):
    """Настройки отдельной проверки валидации."""
    enabled: bool = True
    critical: bool = False
    rules: Optional[Dict[str, Any]] = None


class ValidationConfig(BaseModel):
    """Конфигурация валидации."""
    strict_mode: bool = True
    
    class ChecksConfig(BaseModel):
        schema_enforcement: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=True)
        )
        ohlc_consistency: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=True)
        )
        causality_guard: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=True)
        )
        adjusted_prices: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=True)
        )
        gap_detection: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=False)
        )
        volume_check: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=False)
        )
        price_jump_check: ValidationCheckConfig = Field(
            default_factory=lambda: ValidationCheckConfig(enabled=True, critical=False)
        )
    
    checks: ChecksConfig = Field(default_factory=ChecksConfig)


class SyncConfig(BaseModel):
    """Конфигурация синхронизации."""
    class IncrementalConfig(BaseModel):
        enabled: bool = True
        checkpoint_file: str = Field(default="checkpoint.json")
        metadata_file: str = Field(default="metadata.json")
        deduplication: bool = True
        idempotent: bool = True
    
    incremental: IncrementalConfig = Field(default_factory=IncrementalConfig)


class InstrumentsConfig(BaseModel):
    """Конфигурация инструментов."""
    tickers: List[str] = Field(default_factory=list)
    
    class MacroInstrument(BaseModel):
        """Макро-инструмент с FIGI и кодом MOEX."""
        ticker: str
        figi: Optional[str] = None
        moex_code: Optional[str] = None
        moex_board: Optional[str] = None
    
    class MacroConfig(BaseModel):
        currencies: List[InstrumentsConfig.MacroInstrument] = Field(default_factory=list)
        commodities: List[InstrumentsConfig.MacroInstrument] = Field(default_factory=list)
        indices: List[InstrumentsConfig.MacroInstrument] = Field(default_factory=list)
        rates: List[InstrumentsConfig.MacroInstrument] = Field(default_factory=list)
    
    macro: MacroConfig = Field(default_factory=MacroConfig)


class DataCollectorConfig(BaseModel):
    """
    Корневая конфигурация Data Collector.
    
    Attributes:
        general: Общие настройки.
        providers: Конфигурация провайдеров.
        timeframes: Поддерживаемые таймфреймы.
        instruments: Конфигурация инструментов.
        cache: Настройки кэширования.
        storage: Настройки хранилища.
        validation: Настройки валидации.
        sync: Настройки синхронизации.
        logging: Настройки логирования.
        metrics: Настройки метрик.
    """
    general: GeneralConfig = Field(default_factory=GeneralConfig)
    providers: Dict[str, ProviderConfig] = Field(default_factory=dict)
    timeframes: List[Timeframe] = Field(default_factory=list)
    instruments: InstrumentsConfig = Field(default_factory=InstrumentsConfig)
    cache: CacheConfig = Field(default_factory=CacheConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    validation: ValidationConfig = Field(default_factory=ValidationConfig)
    sync: SyncConfig = Field(default_factory=SyncConfig)
    logging: Dict[str, Any] = Field(default_factory=dict)
    metrics: Dict[str, Any] = Field(default_factory=dict)


class ConfigLoader:
    """
    Загрузчик конфигурации из YAML файлов.
    
    Поддерживает:
    - Загрузку из файла
    - Переопределение через переменные окружения
    - Валидацию через Pydantic
    - Fallback на значения по умолчанию
    
    Example:
        >>> loader = ConfigLoader("config/data_collector.yaml")
        >>> config = loader.load()
        >>> print(config.general.log_level)
    """
    
    DEFAULT_CONFIG_PATH = "config/data_collector.yaml"
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Инициализация загрузчика конфигурации.
        
        Args:
            config_path: Путь к YAML файлу конфигурации.
                        Если None, используется путь по умолчанию.
        """
        self.config_path = Path(config_path) if config_path else Path(self.DEFAULT_CONFIG_PATH)
    
    def load(self) -> DataCollectorConfig:
        """
        Загрузка и валидация конфигурации.
        
        Returns:
            DataCollectorConfig: Валидированная конфигурация.
        
        Raises:
            FileNotFoundError: Если файл конфигурации не найден.
            ValidationError: Если конфигурация не проходит валидацию.
            yaml.YAMLError: Если ошибка парсинга YAML.
        """
        if not self.config_path.exists():
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        with open(self.config_path, 'r', encoding='utf-8') as f:
            raw_config = yaml.safe_load(f)
        
        # Применение переменных окружения
        raw_config = self._apply_env_overrides(raw_config)
        
        # Валидация через Pydantic
        return DataCollectorConfig(**raw_config)
    
    def _apply_env_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Применение переопределений из переменных окружения.
        
        Формат переменных: MANETS_<SECTION>_<KEY>
        Пример: MANETS_GENERAL_LOG_LEVEL=DEBUG
        
        Args:
            config: Исходная конфигурация.
            
        Returns:
            Конфигурация с применёнными переопределениями.
        """
        env_prefix = "MANETS_"
        
        for env_key, env_value in os.environ.items():
            if not env_key.startswith(env_prefix):
                continue
            
            parts = env_key[len(env_prefix):].split('_', 1)
            if len(parts) != 2:
                continue
            
            section, key = parts
            section_lower = section.lower()
            key_lower = key.lower()
            
            if section_lower in config and isinstance(config[section_lower], dict):
                # Преобразование типов
                config[section_lower][key_lower] = self._convert_env_value(
                    config[section_lower].get(key_lower), env_value
                )
        
        return config
    
    def _convert_env_value(self, original_value: Any, env_value: str) -> Any:
        """
        Преобразование строкового значения из env в нужный тип.
        
        Args:
            original_value: Оригинальное значение для определения типа.
            env_value: Строковое значение из переменной окружения.
            
        Returns:
            Преобразованное значение.
        """
        if original_value is None:
            # Попытка угадать тип
            if env_value.lower() in ('true', 'false'):
                return env_value.lower() == 'true'
            try:
                return int(env_value)
            except ValueError:
                try:
                    return float(env_value)
                except ValueError:
                    return env_value
        
        if isinstance(original_value, bool):
            return env_value.lower() in ('true', 'yes', '1')
        elif isinstance(original_value, int):
            return int(env_value)
        elif isinstance(original_value, float):
            return float(env_value)
        
        return env_value
    
    @classmethod
    def get_default_config(cls) -> DataCollectorConfig:
        """
        Получение конфигурации по умолчанию.
        
        Returns:
            DataCollectorConfig: Конфигурация со значениями по умолчанию.
        """
        return DataCollectorConfig()


def load_config(config_path: Optional[str] = None) -> DataCollectorConfig:
    """
    Утилита для загрузки конфигурации.
    
    Args:
        config_path: Опциональный путь к файлу конфигурации.
        
    Returns:
        DataCollectorConfig: Загруженная и валидированная конфигурация.
    """
    loader = ConfigLoader(config_path)
    return loader.load()


# Глобальный экземпляр конфигурации (ленивая инициализация)
_config: Optional[DataCollectorConfig] = None


def get_config() -> DataCollectorConfig:
    """
    Получение глобальной конфигурации.
    
    Returns:
        DataCollectorConfig: Глобальная конфигурация.
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reload_config(config_path: Optional[str] = None) -> DataCollectorConfig:
    """
    Перезагрузка конфигурации.
    
    Args:
        config_path: Опциональный новый путь к конфигурации.
        
    Returns:
        DataCollectorConfig: Новая конфигурация.
    """
    global _config
    _config = load_config(config_path)
    return _config
