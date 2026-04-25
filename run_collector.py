#!/usr/bin/env python3
"""
Скрипт запуска сбора данных для проекта MANETS.

Использование:
    # Запуск с конфигом по умолчанию
    python run_collector.py

    # Запуск с кастомным конфигом
    python run_collector.py --config config/custom.yaml

    # Запуск для конкретных тикеров
    python run_collector.py --tickers SBER,GAZP,LKOH

    # Запуск для конкретных таймфреймов
    python run_collector.py --timeframes M1,H1,D1

    # Только стаканы (L2)
    python run_collector.py --data-type lob --depth 10

    # Только макро-данные
    python run_collector.py --data-type macro

    # Только OHLCV свечи (по умолчанию)
    python run_collector.py --data-type ohlcv

    # Инкрементальная загрузка (проверяет checkpoint)
    python run_collector.py --incremental

    # Полная перезагрузка (игнорирует checkpoint)
    python run_collector.py --full-reload
"""

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

if TYPE_CHECKING:
    from src.infrastructure.data.collector import DataCollector

# Добавляем корень проекта в путь
sys.path.insert(0, str(Path(__file__).parent))

from src.infrastructure.data.config import ConfigLoader
from src.infrastructure.data.collector import DataCollector
from src.infrastructure.data.models import Timeframe


def setup_logging(level: int = logging.INFO) -> None:
    """Базовая настройка логирования."""
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Запуск сбора данных для проекта MANETS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument(
        "--config",
        type=str,
        default="config/data_collector.yaml",
        help="Путь к YAML конфигурационному файлу (по умолчанию: config/data_collector.yaml)"
    )
    parser.add_argument(
        "--tickers",
        type=str,
        default=None,
        help="Список тикеров через запятую (переопределяет конфиг)"
    )
    parser.add_argument(
        "--timeframes",
        type=str,
        default=None,
        help="Список таймфреймов через запятую (переопределяет конфиг)"
    )
    parser.add_argument(
        "--data-type",
        type=str,
        choices=["ohlcv", "lob", "macro", "all"],
        default="ohlcv",
        help="Тип данных для сбора: ohlcv (свечи), lob (стаканы), macro (макро), all (все)"
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=10,
        help="Глубина стакана для L2 данных (1-20, по умолчанию 10)"
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        default=True,
        help="Инкрементальная загрузка (проверяет checkpoint, по умолчанию включено)"
    )
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Полная перезагрузка (игнорирует checkpoint)"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Режим проверки без реального сбора данных"
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Включить подробное логирование (DEBUG уровень)"
    )
    return parser.parse_args()


async def main():
    args = parse_args()
    
    # Настройка логирования
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=log_level)
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("MANETS Data Collector - Запуск сбора данных")
    logger.info("=" * 60)
    
    try:
        # Загрузка конфигурации
        logger.info(f"Загрузка конфигурации из: {args.config}")
        config_path = Path(args.config)
        
        if not config_path.exists():
            logger.error(f"Конфигурационный файл не найден: {config_path}")
            logger.info("Создайте файл конфигурации или укажите правильный путь через --config")
            sys.exit(1)
        
        config_loader = ConfigLoader(str(config_path))
        config = config_loader.load()
        
        logger.info(f"Конфигурация успешно загружена")
        logger.info(f"  Log Level: {config.general.log_level}")
        logger.info(f"  Max Concurrent Fetches: {config.general.max_concurrent_fetches}")
        logger.info(f"  Request Timeout: {config.general.request_timeout_sec}s")
        
        # Переопределение параметров из командной строки
        if args.tickers:
            tickers = [t.strip().upper() for t in args.tickers.split(',')]
            config.instruments.tickers = tickers
            logger.info(f"Тикеры переопределены: {tickers}")
        else:
            tickers = config.instruments.tickers
            
        if args.timeframes:
            tf_map = {
                '5S': Timeframe.M1, '10S': Timeframe.M1, '30S': Timeframe.M1,
                '1M': Timeframe.M1, 'M1': Timeframe.M1, 
                '2M': Timeframe.M1, 'M2': Timeframe.M1,
                '3M': Timeframe.M1, 'M3': Timeframe.M1,
                '5M': Timeframe.M5, 'M5': Timeframe.M5,
                '10M': Timeframe.M10, 'M10': Timeframe.M10,
                '15M': Timeframe.M15, 'M15': Timeframe.M15,
                '30M': Timeframe.M30, 'M30': Timeframe.M30,
                '1H': Timeframe.H1, 'H1': Timeframe.H1,
                '2H': Timeframe.H1, 'H2': Timeframe.H1,
                '4H': Timeframe.H4, 'H4': Timeframe.H4,
                '1D': Timeframe.D1, 'D1': Timeframe.D1,
                '1W': Timeframe.W1, 'W1': Timeframe.W1,
                '1MN': Timeframe.MN, 'MN': Timeframe.MN
            }
            timeframes = []
            for tf in args.timeframes.split(','):
                tf_clean = tf.strip().upper()
                # Нормализация форматов: 4h -> 4H, 4H -> 4H
                if tf_clean.endswith('H') and len(tf_clean) > 1 and tf_clean[:-1].isdigit():
                    tf_clean = 'H' + tf_clean[:-1]  # 4H -> H4
                elif tf_clean.endswith('M') and len(tf_clean) > 1 and tf_clean[:-1].isdigit():
                    tf_clean = 'M' + tf_clean[:-1]  # 10M -> M10
                elif tf_clean.endswith('D') and len(tf_clean) > 1 and tf_clean[:-1].isdigit():
                    tf_clean = 'D' + tf_clean[:-1]  # 1D -> D1
                elif tf_clean.endswith('W') and len(tf_clean) > 1 and tf_clean[:-1].isdigit():
                    tf_clean = 'W' + tf_clean[:-1]  # 1W -> W1
                
                if tf_clean in tf_map:
                    timeframes.append(tf_map[tf_clean])
                else:
                    logger.warning(f"Неизвестный таймфрейм: {tf}, пропускаем")
            
            if timeframes:
                config.timeframes = timeframes
                logger.info(f"Таймфреймы переопределены: {[tf.value for tf in timeframes]}")
        else:
            timeframes = config.timeframes
        
        # Определение типа данных и режима загрузки
        data_types = []
        if args.data_type == "all":
            data_types = ["ohlcv", "lob", "macro"]
        else:
            data_types = [args.data_type]
        
        force_full = args.full_reload
        incremental = not force_full
        
        logger.info(f"Параметры сбора:")
        logger.info(f"  Тикеры: {tickers}")
        logger.info(f"  Таймфреймы: {[tf.value for tf in timeframes]}")
        logger.info(f"  Типы данных: {data_types}")
        logger.info(f"  Режим: {'incremental' if incremental else 'full_reload'}")
        if "lob" in data_types:
            logger.info(f"  Глубина стакана: {args.depth}")
        
        # Создание коллектора данных
        logger.info("Инициализация DataCollector...")
        collector: Optional["DataCollector"] = None
        try:
            collector = DataCollector(config=config)
            
            if args.dry_run:
                logger.info("РЕЖИМ ПРОВЕРКИ (dry-run) - данные не будут сохраняться")
                # Проверка доступности провайдеров
                await collector.start()
                logger.info("Проверка провайдеров завершена успешно")
                await collector.stop()
                logger.info("Проверка завершена. Выход.")
                return
            
            # Запуск сбора данных
            logger.info("Запуск сбора данных...")
            logger.info("-" * 60)
            
            await collector.start()
            
            total_records = 0
            
            # Сбор OHLCV свечей
            if "ohlcv" in data_types:
                for ticker in tickers:
                    for timeframe in timeframes:
                        try:
                            logger.info(f"Сбор OHLCV: {ticker} [{timeframe.value}]")
                            
                            # Инкрементальная загрузка через synchronizer
                            if incremental:
                                from src.infrastructure.data.sync.incremental import IncrementalSynchronizer
                                
                                synchronizer = IncrementalSynchronizer(
                                    provider=collector.primary_provider,
                                    storage=collector.storage,
                                    cache=collector.cache,
                                    validator=collector.validator,
                                    config=collector.config
                                )
                                
                                result = await synchronizer.sync(
                                    instrument=ticker,
                                    timeframe=timeframe,
                                    data_type="ohlcv",
                                    force_full=force_full
                                )
                                
                                count = result.get('new_bars', 0)
                                total_records += count
                                
                                if result['status'] == 'success':
                                    logger.info(f"  ✓ Загружено {count} новых свечей")
                                elif result['status'] == 'no_op':
                                    logger.info(f"  ✓ Данные актуальны (загрузка не требуется)")
                                else:
                                    logger.warning(f"  ⚠ Статус: {result['status']}")
                            else:
                                # Полная загрузка (старый метод)
                                candles = await collector.collect(
                                    instrument=ticker,
                                    timeframe=timeframe
                                )
                                
                                if candles:
                                    count = len(candles)
                                    total_records += count
                                    logger.info(f"  ✓ Получено {count} свечей")
                                    
                                    # Сохранение данных через storage
                                    candles_dict = [c.model_dump(mode='json') for c in candles]
                                    await collector.storage.write_ohlcv(
                                        ticker=ticker,
                                        timeframe=timeframe.value,
                                        candles=candles_dict
                                    )
                                    logger.info(f"  ✓ Данные сохранены")
                                else:
                                    logger.warning(f"  ⚠ Нет данных для {ticker} [{timeframe.value}]")
                                    
                        except Exception as e:
                            logger.error(f"  ✗ Ошибка при сборе {ticker} [{timeframe.value}]: {e}", exc_info=args.verbose)
            
            # Сбор стаканов (L2)
            if "lob" in data_types:
                for ticker in tickers:
                    try:
                        logger.info(f"Сбор стакана L2: {ticker} (depth={args.depth})")
                        
                        orderbook = await collector.primary_provider.get_orderbook(
                            instrument=ticker,
                            depth=args.depth
                        )
                        
                        if orderbook and (orderbook.bids or orderbook.asks):
                            total_records += 1
                            logger.info(f"  ✓ Получен стакан: {len(orderbook.bids)} bids, {len(orderbook.asks)} asks")
                            
                            # Сохранение стакана
                            await collector.storage.write_orderbook(
                                ticker=ticker,
                                orderbook=orderbook
                            )
                            logger.info(f"  ✓ Данные сохранены")
                        else:
                            logger.warning(f"  ⚠ Пустой стакан для {ticker}")
                            
                    except Exception as e:
                        logger.error(f"  ✗ Ошибка при сборе стакана {ticker}: {e}", exc_info=args.verbose)
            
            # Сбор макро-данных
            if "macro" in data_types:
                # Получаем список макро-инструментов (теперь это объекты MacroInstrument)
                macro_instruments = (
                    config.instruments.macro.currencies +
                    config.instruments.macro.commodities +
                    config.instruments.macro.indices +
                    config.instruments.macro.rates
                )
                
                logger.info(f"Сбор макро-данных: {[inst.ticker for inst in macro_instruments]}")
                
                for macro_inst_obj in macro_instruments:
                    # Извлекаем ticker из объекта MacroInstrument
                    macro_inst = macro_inst_obj.ticker
                    
                    for timeframe in timeframes:
                        try:
                            logger.info(f"Сбор макро: {macro_inst} [{timeframe.value}]")
                            
                            # Инкрементальная загрузка
                            if incremental:
                                from src.infrastructure.data.sync.incremental import IncrementalSynchronizer
                                
                                # Получаем провайдер для макро-данных
                                macro_provider = collector.get_provider_for("macro", macro_inst)
                                if not macro_provider:
                                    logger.warning(f"  ⚠ Нет доступного провайдера для {macro_inst}")
                                    continue
                                
                                synchronizer = IncrementalSynchronizer(
                                    provider=macro_provider,
                                    storage=collector.storage,
                                    cache=collector.cache,
                                    validator=collector.validator,
                                    config=collector.config
                                )
                                
                                result = await synchronizer.sync(
                                    instrument=macro_inst,
                                    timeframe=timeframe,
                                    data_type="macro",
                                    force_full=force_full
                                )
                                
                                count = result.get('new_bars', 0)
                                total_records += count
                                
                                if result['status'] == 'success':
                                    logger.info(f"  ✓ Загружено {count} макро-баров")
                                elif result['status'] == 'no_op':
                                    logger.info(f"  ✓ Данные актуальны")
                                else:
                                    logger.warning(f"  ⚠ Статус: {result['status']}")
                            else:
                                # Полная загрузка
                                macro_provider = collector.get_provider_for("macro", macro_inst)
                                if not macro_provider:
                                    logger.warning(f"  ⚠ Нет доступного провайдера для {macro_inst}")
                                    continue
                                    
                                macro_candles = await macro_provider.get_macro(
                                    instrument=macro_inst,
                                    timeframe=timeframe,
                                    from_dt=None,
                                    to_dt=None
                                )
                                
                                if macro_candles:
                                    count = len(macro_candles)
                                    total_records += count
                                    logger.info(f"  ✓ Получено {count} макро-баров")
                                else:
                                    logger.warning(f"  ⚠ Нет макро-данных для {macro_inst}")
                                    
                        except Exception as e:
                            logger.error(f"  ✗ Ошибка при сборе макро {macro_inst}: {e}", exc_info=args.verbose)
            
            await collector.stop()
            
            logger.info("-" * 60)
            logger.info(f"Сбор данных завершен. Всего получено записей: {total_records}")
            logger.info(f"Данные сохранены в: {config.storage.base_path}")
            
        except KeyboardInterrupt:
            logger.info("\nСбор данных прерван пользователем")
            if collector:
                await collector.stop()
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
            if collector:
                await collector.stop()
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Ошибка на верхнем уровне: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
