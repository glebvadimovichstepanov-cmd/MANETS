#!/usr/bin/env python3
"""
Скрипт запуска сбора данных для проекта MANETS.

Использование:
    python run_collector.py [--config path/to/config.yaml] [--tickers TICKER1,TICKER2,...] [--timeframes M1,H1,D1]

Примеры:
    # Запуск с конфигом по умолчанию
    python run_collector.py

    # Запуск с кастомным конфигом
    python run_collector.py --config config/custom.yaml

    # Запуск для конкретных тикеров
    python run_collector.py --tickers SBER,GAZP,LKOH

    # Запуск для конкретных таймфреймов
    python run_collector.py --timeframes M1,H1,D1
"""

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Optional

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
        "--start-date",
        type=str,
        default=None,
        help="Дата начала сбора в формате YYYY-MM-DD (переопределяет конфиг)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Дата окончания сбора в формате YYYY-MM-DD (переопределяет конфиг)"
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
                'M1': Timeframe.M1, 'M5': Timeframe.M5, 'M10': Timeframe.M10,
                'M15': Timeframe.M15, 'H1': Timeframe.H1, 'H4': Timeframe.H4,
                'D1': Timeframe.D1, 'W1': Timeframe.W1, 'MN': Timeframe.MN
            }
            timeframes = [tf_map[tf.strip().upper()] for tf in args.timeframes.split(',')]
            config.timeframes = timeframes
            logger.info(f"Таймфреймы переопределены: {[tf.value for tf in timeframes]}")
        else:
            timeframes = config.timeframes
        
        logger.info(f"Параметры сбора:")
        logger.info(f"  Тикеры: {tickers}")
        logger.info(f"  Таймфреймы: {[tf.value for tf in timeframes]}")
        
        # Создание коллектора данных
        logger.info("Инициализация DataCollector...")
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
        
        total_candles = 0
        for ticker in tickers:
            for timeframe in timeframes:
                try:
                    logger.info(f"Сбор данных: {ticker} [{timeframe.value}]")
                    
                    candles = await collector.collect(
                        instrument=ticker,
                        timeframe=timeframe
                    )
                    
                    if candles:
                        count = len(candles)
                        total_candles += count
                        logger.info(f"  ✓ Получено {count} свечей")
                        
                        # Сохранение данных через storage
                        candles_dict = [c.model_dump(mode='json') for c in candles]
                        await collector._storage.write_ohlcv(
                            ticker=ticker,
                            timeframe=timeframe.value,
                            candles=candles_dict
                        )
                        logger.info(f"  ✓ Данные сохранены")
                    else:
                        logger.warning(f"  ⚠ Нет данных для {ticker} [{timeframe.value}]")
                        
                except Exception as e:
                    logger.error(f"  ✗ Ошибка при сборе {ticker} [{timeframe.value}]: {e}", exc_info=args.verbose)
        
        await collector.stop()
        
        logger.info("-" * 60)
        logger.info(f"Сбор данных завершен. Всего получено свечей: {total_candles}")
        logger.info(f"Данные сохранены в: {config.storage.base_path}")
        
    except KeyboardInterrupt:
        logger.info("\nСбор данных прерван пользователем")
        if 'collector' in locals():
            await collector.stop()
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
