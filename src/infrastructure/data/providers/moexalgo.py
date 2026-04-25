"""
MoexAlgo провайдер данных.

Асинхронный клиент для MOEX Algo API (Московская Биржа).
Поддержка:
- OHLCV свечи
- Стаканы L2
- Трейды
- Макро-данные (валюты, товары, индексы)

Особенности:
- REST API клиент
- Поддержка всех инструментов Мосбиржи
- Retry с exponential backoff
- Rate limiting и circuit breaker
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

from ..models import (
    Candle, L2OrderBook, L2OrderLevel, Trade, TradeSide,
    MacroCandle, Fundamental, CorporateEvent,
    EventType, EventStatus, DataSource, Timeframe
)
from .base import DataProvider, ProviderState

logger = logging.getLogger(__name__)


class MoexAlgoProvider(DataProvider):
    """
    Провайдер для MOEX Algo API (Московская Биржа).
    
    Поддерживает работу через REST API.
    Автоматически конвертирует данные в стандартный формат.
    
    Example:
        >>> config = {'endpoints': {'base_url': 'https://apim.moex.com'}}
        >>> provider = MoexAlgoProvider(config)
        >>> candles = await provider.get_ohlcv("SNGS", Timeframe.D1, from_dt, to_dt)
    """
    
    def __init__(
        self,
        config: Dict[str, Any],
        rate_limit_config: Optional[Dict[str, Any]] = None,
        circuit_breaker_config: Optional[Dict[str, Any]] = None
    ):
        """
        Инициализация MoexAlgo провайдера.
        
        Args:
            config: Конфигурация провайдера (endpoints, auth_token_env).
            rate_limit_config: Конфигурация rate limiter.
            circuit_breaker_config: Конфигурация circuit breaker.
        """
        super().__init__(
            name="moexalgo",
            priority=config.get('priority', 2),
            rate_limit_config=rate_limit_config,
            circuit_breaker_config=circuit_breaker_config
        )
        
        self.endpoints = config.get('endpoints', {})
        # MOEX API доступен только через публичный ISS API (apim.moex.com требует авторизации)
        self.base_url = 'https://iss.moex.com/iss'
        
        # Получение токена из env (приоритет: MOEX_TOKEN)
        token_env = config.get('auth_token_env', 'MOEX_TOKEN')
        self.token = os.environ.get(token_env, '').strip()
        
        if not self.token:
            logger.info(
                f"Auth token not found in env variable '{token_env}'. "
                "Using public ISS API (some endpoints may be rate-limited)."
            )
        else:
            logger.info(f"MOEX token loaded from {token_env} (length={len(self.token)})")
        
        # HTTP сессия (ленивая инициализация)
        self._session: Optional[aiohttp.ClientSession] = None
        
        # Retry конфигурация
        self._retry_max_attempts = 3
        self._retry_base_delay = 1.0
        self._retry_max_delay = 60.0
        
        # Поддерживаемые инструменты MOEX
        self._supported_tickers = {
            # Голубые фишки
            'SNGS', 'GAZP', 'SBER', 'LKOH', 'YDEX', 'GMKN', 'NVTK',
            'ROSN', 'TATN', 'SIBN', 'ALRS', 'MTSS', 'MGNT', 'PLZL',
            'POLY', 'VTBR', 'BSPB', 'CBOM', 'FEES', 'MOEX', 'AFKS',
            'PIKK', 'RUAL', 'UPRO', 'TRNFP', 'IRAO', 'LSRG', 'ETLN',
            'CHMF', 'KRKNP', 'MVID', 'OZON', 'SGZH', 'VKCO'
        }
        
        # Поддерживаемые макро-инструменты MOEX
        self._supported_macro = {
            # Валюты
            'USD_RUB', 'EUR_RUB', 'CNY_RUB', 'GBP_RUB', 'KZT_RUB',
            # Товары (фьючерсы)
            'BRENT', 'NATURAL_GAS', 'GOLD', 'SILVER',
            # Индексы
            'MOEX_INDEX', 'RTS_INDEX', 'MOEX_OG', 'MOEX_BC',
            # Ставки и облигации
            'RUONIA', 'RUSFAR', 'CBR_KEY_RATE',
            # OFZ облигации
            'OFZ_26238', 'OFZ_26244'
        }
        
        # Маппинг тикеров для API запросов
        self._macro_ticker_map = {
            # Валютные пары (коды инструментов MOEX)
            'USD_RUB': 'USDRUB_TOM',
            'EUR_RUB': 'EURRUB_TOM',
            'CNY_RUB': 'CNYRUB_TOM',
            'GBP_RUB': 'GBPRUB_TOM',
            'KZT_RUB': 'KZTRUB_TOM',
            
            # Товары - фьючерсы
            'BRENT': 'BR',           # Фьючерс на Brent
            'NATURAL_GAS': 'NG',     # Фьючерс на Natural Gas
            'GOLD': 'GLD',           # Фьючерс на золото
            'SILVER': 'SLV',         # Фьючерс на серебро
            
            # Индексы
            'MOEX_INDEX': 'IMOEX',   # Индекс Мосбиржи
            'RTS_INDEX': 'RTSI',     # Индекс РТС
            
            # Ставки
            'RUONIA': 'RUONIA',
            'RUSFAR': 'RUSFAR',
            'CBR_KEY_RATE': 'CBR_KEY_RATE',  # Специальный, берется из CBR
            
            # OFZ облигации (ISIN коды)
            'OFZ_26238': 'SU26238RMFS4',
            'OFZ_26244': 'SU26244RMFS2'
        }
        
        # Инструменты, которые требуют специального处理方式
        self._special_macro_instruments = {
            'CBR_KEY_RATE',  # Требуется внешний источник (ЦБ РФ)
        }
        
        # Таймфреймы поддерживаемые MOEX API
        self._timeframe_map = {
            Timeframe.S5: '1',       # 1 минута (минимальный)
            Timeframe.S10: '1',
            Timeframe.S30: '1',
            Timeframe.M1: '1',       # 1 минута
            Timeframe.M2: '1',
            Timeframe.M3: '1',
            Timeframe.M5: '5',       # 5 минут
            Timeframe.M10: '10',     # 10 минут
            Timeframe.M15: '15',     # 15 минут
            Timeframe.M30: '30',     # 30 минут
            Timeframe.H1: '60',      # 1 час
            Timeframe.H2: '120',     # 2 часа
            Timeframe.H4: '240',     # 4 часа
            Timeframe.D1: 'D',       # День
            Timeframe.W1: 'W',       # Неделя
            Timeframe.MN: 'M',       # Месяц
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Ленивая инициализация HTTP сессии."""
        if self._session is None or self._session.closed:
            headers = {}
            if self.token:
                headers['Authorization'] = f'Bearer {self.token}'
            
            self._session = aiohttp.ClientSession(
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            )
        return self._session
    
    async def close(self):
        """Закрытие соединений."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
    
    async def _retry_with_backoff(self, coro, operation: str = "operation"):
        """
        Выполнение с retry и exponential backoff.
        
        Args:
            coro: Корутина для выполнения.
            operation: Название операции для логирования.
            
        Returns:
            Результат выполнения.
            
        Raises:
            Exception: Если все попытки исчерпаны.
        """
        last_exception = None
        
        for attempt in range(1, self._retry_max_attempts + 1):
            try:
                return await coro
            except Exception as e:
                last_exception = e
                
                if attempt == self._retry_max_attempts:
                    break
                
                # Exponential backoff
                delay = min(
                    self._retry_base_delay * (2 ** (attempt - 1)),
                    self._retry_max_delay
                )
                
                logger.warning(
                    f"{operation} failed (attempt {attempt}/{self._retry_max_attempts}): {e}. "
                    f"Retrying in {delay:.2f}s"
                )
                
                await asyncio.sleep(delay)
        
        raise last_exception
    
    async def _fetch_json(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        GET запрос с возвратом JSON (MOEX ISS API).
        
        Args:
            url: URL endpoint.
            params: Query параметры.
            
        Returns:
            JSON ответ.
        """
        session = await self._get_session()
        
        # Добавляем json=1 для получения JSON вместо XML (MOEX ISS API)
        if params is None:
            params = {}
        params['json'] = 1
        
        try:
            async with session.get(url, params=params, timeout=self.config.request_timeout) as response:
                content_type = response.headers.get("Content-Type", "")
                
                if response.status == 200:
                    # MOEX может вернуть XML даже при json=1 в некоторых случаях
                    if "xml" in content_type:
                        text = await response.text()
                        return self._parse_iss_xml(text)
                    else:
                        try:
                            return await response.json()
                        except aiohttp.ContentTypeError:
                            # Fallback: пытаемся распарсить текст как JSON
                            text = await response.text()
                            import json
                            return json.loads(text)
                            
                elif response.status == 401:
                    raise RuntimeError(f"Unauthorized: {url}")
                elif response.status == 403:
                    raise RuntimeError(f"Forbidden: {url}")
                elif response.status == 404:
                    raise RuntimeError(f"Not found: {url}")
                elif response.status == 429:
                    raise RuntimeError(f"Rate limited: {url}")
                else:
                    raise RuntimeError(f"HTTP {response.status}: {url}")
                    
        except asyncio.TimeoutError:
            raise RuntimeError(f"Timeout fetching {url}")
    
    def _parse_iss_xml(self, xml_text: str) -> Dict[str, Any]:
        """
        Парсинг XML ответа от MOEX ISS API.
        
        Args:
            xml_text: XML строка.
            
        Returns:
            Словарь с данными в формате, совместимом с JSON-ответом.
        """
        import xml.etree.ElementTree as ET
        
        try:
            root = ET.fromstring(xml_text)
            result = {}
            
            # Ищем все блоки <data> с атрибутом name или id
            for block in root.findall(".//data"):
                block_name = block.get("name") or block.get("id")
                if not block_name:
                    continue
                
                columns = []
                rows = []
                
                # Извлекаем названия колонок
                cols_elem = block.find("columns")
                if cols_elem is not None:
                    columns = [c.get("name") for c in cols_elem.findall("column")]
                
                # Извлекаем строки данных
                rows_elem = block.find("rows")
                if rows_elem is not None:
                    for row in rows_elem.findall("row"):
                        row_data = []
                        cells = row.findall("cell")
                        for i, cell in enumerate(cells):
                            val = cell.text
                            # Попытка конвертации типов
                            if val is not None and i < len(columns):
                                try:
                                    if "." in val:
                                        row_data.append(float(val))
                                    else:
                                        row_data.append(int(val))
                                except ValueError:
                                    row_data.append(val)
                            else:
                                row_data.append(val)
                        rows.append(row_data)
                
                if columns and rows:
                    result[block_name] = {"columns": columns, "data": rows}
            
            return result
            
        except ET.ParseError as e:
            logger.error(f"MOEX ISS XML parse error: {e}")
            return {}
        except Exception as e:
            logger.error(f"MOEX ISS XML error: {e}")
            return {}
    
    def _parse_moex_datetime(self, dt_str: str) -> datetime:
        """
        Парсинг даты/времени от MOEX.
        
        Args:
            dt_str: Строка даты/времени.
            
        Returns:
            datetime объект.
        """
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S.%f',
            '%Y-%m-%dT%H:%M:%S.%f',
            '%Y-%m-%d',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(dt_str, fmt)
            except ValueError:
                continue
        
        # Fallback
        return datetime.utcnow()
    
    async def get_ohlcv(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Candle]:
        """
        Получение OHLCV свечей от MOEX.
        
        Args:
            instrument: Тикер инструмента (SNGS, GAZP, etc).
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список свечей.
        """
        async def _fetch():
            logger.debug(
                f"MOEX: Fetching OHLCV for {instrument} {timeframe.value} "
                f"from {from_dt} to {to_dt}"
            )
            
            # Определение типа инструмента
            is_macro = instrument in self._supported_macro
            is_currency = instrument in ['USD_RUB', 'EUR_RUB', 'CNY_RUB', 'GBP_RUB', 'KZT_RUB']
            is_index = instrument in ['MOEX_INDEX', 'RTS_INDEX', 'MOEX_OG', 'MOEX_BC']
            
            # Построение URL для ISS API
            if is_macro:
                # Макро-инструменты (валюты, индексы, товары)
                # Валюты используют engine=currency, market=selt
                # Индексы используют engine=stock, market=index
                # Товары используют engine=stock, market=main
                
                ticker = self._macro_ticker_map.get(instrument, instrument)
                
                if is_currency:
                    engine = 'currency'
                    market = 'selt'
                elif is_index:
                    engine = 'stock'
                    market = 'index'
                else:
                    # Товары
                    engine = 'stock'
                    market = 'main'
                
                url = f"{self.base_url}/engines/{engine}/markets/{market}/securities/{ticker}/candles"
            else:
                # Акции
                url = f"{self.base_url}/engines/stock/markets/shares/securities/{instrument}/candles"
            
            # Параметры запроса
            params = {
                'from': from_dt.strftime('%Y-%m-%d'),
                'to': to_dt.strftime('%Y-%m-%d'),
                'interval': self._timeframe_map.get(timeframe, '1'),
            }
            
            try:
                data = await self._fetch_json(url, params)
                
                # Парсинг ответа MOEX
                candles = []
                
                # MOEX возвращает данные в формате:
                # { 'candles': { 'columns': [...], 'data': [[...], ...] } }
                if 'candles' in data and 'data' in data['candles']:
                    columns = data['candles'].get('columns', [])
                    rows = data['candles']['data']
                    
                    # Поиск индексов колонок
                    col_idx = {name: idx for idx, name in enumerate(columns)}
                    
                    begin_idx = col_idx.get('begin')
                    open_idx = col_idx.get('open')
                    high_idx = col_idx.get('high')
                    low_idx = col_idx.get('low')
                    close_idx = col_idx.get('close')
                    volume_idx = col_idx.get('volume')
                    end_idx = col_idx.get('end')
                    
                    for row in rows:
                        try:
                            timestamp = self._parse_moex_datetime(row[begin_idx]) if begin_idx is not None else datetime.utcnow()
                            open_price = Decimal(str(row[open_idx])) if open_idx is not None and row[open_idx] else Decimal('0')
                            high_price = Decimal(str(row[high_idx])) if high_idx is not None and row[high_idx] else Decimal('0')
                            low_price = Decimal(str(row[low_idx])) if low_idx is not None and row[low_idx] else Decimal('0')
                            close_price = Decimal(str(row[close_idx])) if close_idx is not None and row[close_idx] else Decimal('0')
                            volume = Decimal(str(row[volume_idx])) if volume_idx is not None and row[volume_idx] else Decimal('0')
                            
                            candle = Candle(
                                timestamp=timestamp,
                                open=open_price,
                                high=high_price,
                                low=low_price,
                                close=close_price,
                                volume=volume,
                                adj_close=close_price,
                                adj_factor=Decimal('1.0'),
                                is_complete=True,
                                source=DataSource.MOEXALGO,
                                quality_score=0.95
                            )
                            candles.append(candle)
                        except (IndexError, ValueError, TypeError) as e:
                            logger.warning(f"Failed to parse candle row: {row}, error: {e}")
                            continue
                
                logger.info(f"MOEX: Retrieved {len(candles)} candles for {instrument}")
                return candles
                
            except Exception as e:
                logger.error(f"MOEX: Failed to fetch OHLCV for {instrument}: {e}")
                return []
        
        return await self._execute_with_protection(_fetch())
    
    async def get_orderbook(
        self,
        instrument: str,
        depth: int = 10
    ) -> L2OrderBook:
        """
        Получение стакана заявок L2 от MOEX.
        
        Args:
            instrument: Тикер инструмента.
            depth: Глубина стакана.
            
        Returns:
            Стакан заявок.
        """
        async def _fetch():
            logger.debug(f"MOEX: Fetching orderbook for {instrument} depth={depth}")
            
            # Определение типа инструмента
            is_macro = instrument in self._supported_macro
            is_currency = instrument in ['USD_RUB', 'EUR_RUB', 'CNY_RUB', 'GBP_RUB', 'KZT_RUB']
            is_index = instrument in ['MOEX_INDEX', 'RTS_INDEX']
            
            # Построение URL
            if is_macro:
                ticker = self._macro_ticker_map.get(instrument, instrument)
                
                if is_currency:
                    engine = 'currency'
                    market = 'selt'
                    board = 'ALL'
                elif is_index:
                    engine = 'stock'
                    market = 'index'
                    board = 'INDEX'
                else:
                    engine = 'stock'
                    market = 'main'
                    board = 'MAIN'
                
                url = f"{self.base_url}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/orderbook"
            else:
                url = f"{self.base_url}/engines/stock/markets/shares/boards/TQBR/securities/{instrument}/orderbook"
            
            params = {'depth': depth}
            
            try:
                data = await self._fetch_json(url, params)
                
                # Парсинг ответа MOEX
                # MOEX возвращает: { 'orderbook': { 'bids': [...], 'asks': [...], ... } }
                bids_data = data.get('orderbook', {}).get('bids', [])
                asks_data = data.get('orderbook', {}).get('asks', [])
                
                bids = []
                asks = []
                
                for bid in bids_data[:depth]:
                    price = Decimal(str(bid[0])) if len(bid) > 0 else Decimal('0')
                    qty = Decimal(str(bid[1])) if len(bid) > 1 else Decimal('0')
                    orders = int(bid[2]) if len(bid) > 2 else 1
                    
                    bids.append(L2OrderLevel(price=price, qty=qty, orders=orders))
                
                for ask in asks_data[:depth]:
                    price = Decimal(str(ask[0])) if len(ask) > 0 else Decimal('0')
                    qty = Decimal(str(ask[1])) if len(ask) > 1 else Decimal('0')
                    orders = int(ask[2]) if len(ask) > 2 else 1
                    
                    asks.append(L2OrderLevel(price=price, qty=qty, orders=orders))
                
                if not bids or not asks:
                    logger.warning(f"MOEX: Empty orderbook for {instrument}")
                    # Возвращаем пустой стакан с текущим временем
                    return L2OrderBook(
                        timestamp=datetime.utcnow(),
                        bids=[],
                        asks=[],
                        source=DataSource.MOEXALGO,
                        quality_score=0.5
                    )
                
                return L2OrderBook(
                    timestamp=datetime.utcnow(),
                    bids=bids,
                    asks=asks,
                    source=DataSource.MOEXALGO,
                    quality_score=0.95
                )
                
            except Exception as e:
                logger.error(f"MOEX: Failed to fetch orderbook for {instrument}: {e}")
                # Возвращаем пустой стакан при ошибке
                return L2OrderBook(
                    timestamp=datetime.utcnow(),
                    bids=[],
                    asks=[],
                    source=DataSource.MOEXALGO,
                    quality_score=0.3
                )
        
        return await self._execute_with_protection(_fetch())
    
    async def get_trades(
        self,
        instrument: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[Trade]:
        """
        Получение тиковых сделок от MOEX.
        
        Args:
            instrument: Тикер инструмента.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список сделок.
        """
        async def _fetch():
            logger.debug(
                f"MOEX: Fetching trades for {instrument} "
                f"from {from_dt} to {to_dt}"
            )
            
            # Определение типа инструмента
            is_macro = instrument in self._supported_macro
            is_currency = instrument in ['USD_RUB', 'EUR_RUB', 'CNY_RUB', 'GBP_RUB', 'KZT_RUB']
            is_index = instrument in ['MOEX_INDEX', 'RTS_INDEX']
            
            # Построение URL
            if is_macro:
                ticker = self._macro_ticker_map.get(instrument, instrument)
                
                if is_currency:
                    engine = 'currency'
                    market = 'selt'
                    board = 'ALL'
                elif is_index:
                    engine = 'stock'
                    market = 'index'
                    board = 'INDEX'
                else:
                    engine = 'stock'
                    market = 'main'
                    board = 'MAIN'
                
                url = f"{self.base_url}/engines/{engine}/markets/{market}/boards/{board}/securities/{ticker}/trades"
            else:
                url = f"{self.base_url}/engines/stock/markets/shares/boards/TQBR/securities/{instrument}/trades"
            
            params = {
                'from': from_dt.strftime('%Y-%m-%d'),
                'to': to_dt.strftime('%Y-%m-%d'),
            }
            
            try:
                data = await self._fetch_json(url, params)
                
                # Парсинг ответа MOEX
                # MOEX возвращает: { 'trades': { 'columns': [...], 'data': [[...], ...] } }
                trades = []
                
                if 'trades' in data and 'data' in data['trades']:
                    columns = data['trades'].get('columns', [])
                    rows = data['trades']['data']
                    
                    # Поиск индексов колонок
                    col_idx = {name: idx for idx, name in enumerate(columns)}
                    
                    tradeid_idx = col_idx.get('tradeid')
                    start_idx = col_idx.get('start')
                    price_idx = col_idx.get('price')
                    quantity_idx = col_idx.get('quantity')
                    buyback_idx = col_idx.get('buyback')  # SELL если true
                    value_idx = col_idx.get('value')
                    
                    for row in rows:
                        try:
                            trade_id = str(row[tradeid_idx]) if tradeid_idx is not None else None
                            timestamp = self._parse_moex_datetime(row[start_idx]) if start_idx is not None else datetime.utcnow()
                            price = Decimal(str(row[price_idx])) if price_idx is not None and row[price_idx] else Decimal('0')
                            volume = Decimal(str(row[quantity_idx])) if quantity_idx is not None and row[quantity_idx] else Decimal('0')
                            
                            # Определение стороны сделки
                            is_buyback = row[buyback_idx] if buyback_idx is not None else False
                            side = TradeSide.SELL if is_buyback else TradeSide.BUY
                            
                            trade = Trade(
                                timestamp=timestamp,
                                price=price,
                                volume=volume,
                                side=side,
                                aggressor_flag=not is_buyback,
                                trade_id=trade_id,
                                source=DataSource.MOEXALGO
                            )
                            trades.append(trade)
                        except (IndexError, ValueError, TypeError) as e:
                            logger.warning(f"Failed to parse trade row: {row}, error: {e}")
                            continue
                
                logger.info(f"MOEX: Retrieved {len(trades)} trades for {instrument}")
                return sorted(trades, key=lambda t: t.timestamp)
                
            except Exception as e:
                logger.error(f"MOEX: Failed to fetch trades for {instrument}: {e}")
                return []
        
        return await self._execute_with_protection(_fetch())
    
    async def get_macro(
        self,
        instrument: str,
        timeframe: Timeframe,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[MacroCandle]:
        """
        Получение макро-данных от MOEX.
        
        Args:
            instrument: Макро-инструмент (USD_RUB, BRENT, etc).
            timeframe: Таймфрейм.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список макро-свечей.
        """
        # Для макро-инструментов используем тот же механизм что и для OHLCV
        candles = await self.get_ohlcv(instrument, timeframe, from_dt, to_dt)
        
        return [
            MacroCandle(
                timestamp=c.timestamp,
                open=c.open,
                high=c.high,
                low=c.low,
                close=c.close,
                volume=c.volume,
                source=DataSource.MOEXALGO,
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
        Получение фундаментальных показателей от MOEX.
        
        Args:
            ticker: Тикер компании.
            
        Returns:
            Список фундаментальных данных.
        """
        async def _fetch():
            logger.debug(f"MOEX: Fetching fundamentals for {ticker}")
            
            # MOEX предоставляет ограниченные фундаментальные данные
            # Основной источник - T-Tech или другие специализированные API
            url = f"{self.base_url}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/analytics"
            
            try:
                data = await self._fetch_json(url)
                
                fundamentals = []
                
                # Парсинг ответа (формат зависит от endpoint)
                # Это заглушка - реальный парсинг зависит от API MOEX
                if 'analytics' in data:
                    analytics = data['analytics']
                    
                    # Извлечение доступных метрик
                    pe = Decimal(str(analytics.get('pe', 0))) if analytics.get('pe') else None
                    pb = Decimal(str(analytics.get('pb', 0))) if analytics.get('pb') else None
                    dividend_yield = Decimal(str(analytics.get('dividend_yield', 0))) if analytics.get('dividend_yield') else None
                    market_cap = Decimal(str(analytics.get('market_cap', 0))) if analytics.get('market_cap') else None
                    
                    fundamentals.append(Fundamental(
                        report_date=datetime.utcnow().date(),
                        ticker=ticker,
                        pe=pe,
                        pb=pb,
                        dividend_yield=dividend_yield,
                        market_cap=market_cap,
                        source=DataSource.MOEXALGO
                    ))
                
                return fundamentals
                
            except Exception as e:
                logger.warning(f"MOEX: Failed to fetch fundamentals for {ticker}: {e}")
                return []
        
        return await self._execute_with_protection(_fetch())
    
    async def get_corporate_events(
        self,
        ticker: str,
        from_dt: datetime,
        to_dt: datetime
    ) -> List[CorporateEvent]:
        """
        Получение корпоративных событий от MOEX.
        
        Args:
            ticker: Тикер компании.
            from_dt: Начало периода.
            to_dt: Конец периода.
            
        Returns:
            Список событий.
        """
        async def _fetch():
            logger.debug(
                f"MOEX: Fetching corporate events for {ticker} "
                f"from {from_dt} to {to_dt}"
            )
            
            url = f"{self.base_url}/engines/stock/markets/shares/boards/TQBR/securities/{ticker}/events"
            
            params = {
                'from': from_dt.strftime('%Y-%m-%d'),
                'to': to_dt.strftime('%Y-%m-%d'),
            }
            
            try:
                data = await self._fetch_json(url, params)
                
                events = []
                
                # Парсинг ответа MOEX
                # MOEX возвращает: { 'events': { 'columns': [...], 'data': [[...], ...] } }
                if 'events' in data and 'data' in data['events']:
                    columns = data['events'].get('columns', [])
                    rows = data['events']['data']
                    
                    # Поиск индексов колонок
                    col_idx = {name: idx for idx, name in enumerate(columns)}
                    
                    eventtype_idx = col_idx.get('eventtype')
                    recorddate_idx = col_idx.get('recorddate')
                    paymentdate_idx = col_idx.get('paymentdate')
                    value_idx = col_idx.get('value')
                    
                    for row in rows:
                        try:
                            event_type_str = row[eventtype_idx] if eventtype_idx is not None else 'other'
                            
                            # Маппинг типов событий
                            event_type_map = {
                                'dividend': EventType.DIVIDEND,
                                'split': EventType.SPLIT,
                                'consolidation': EventType.REVERSE_SPLIT,
                                'earnings': EventType.EARNINGS,
                            }
                            event_type = event_type_map.get(event_type_str.lower(), EventType.OTHER)
                            
                            record_date_str = row[recorddate_idx] if recorddate_idx is not None else None
                            payment_date_str = row[paymentdate_idx] if paymentdate_idx is not None else None
                            amount = Decimal(str(row[value_idx])) if value_idx is not None and row[value_idx] else None
                            
                            record_date = datetime.strptime(record_date_str, '%Y-%m-%d').date() if record_date_str else None
                            payout_date = datetime.strptime(payment_date_str, '%Y-%m-%d').date() if payment_date_str else None
                            
                            event = CorporateEvent(
                                event_type=event_type,
                                ticker=ticker,
                                ex_date=record_date,
                                record_date=record_date,
                                payout_date=payout_date,
                                ratio=None,
                                amount=amount,
                                status=EventStatus.ANNOUNCED,
                                source=DataSource.MOEXALGO
                            )
                            events.append(event)
                        except (IndexError, ValueError, TypeError) as e:
                            logger.warning(f"Failed to parse event row: {row}, error: {e}")
                            continue
                
                logger.info(f"MOEX: Retrieved {len(events)} corporate events for {ticker}")
                return events
                
            except Exception as e:
                logger.error(f"MOEX: Failed to fetch corporate events for {ticker}: {e}")
                return []
        
        return await self._execute_with_protection(_fetch())
    
    def supports(self, data_type: str, instrument: str) -> bool:
        """
        Проверка поддержки типа данных и инструмента.
        
        Args:
            data_type: Тип данных (ohlcv, lob, macro, etc).
            instrument: Инструмент.
            
        Returns:
            True если поддерживается.
        """
        # Поддерживаемые типы данных
        supported_types = {'ohlcv', 'lob', 'trades', 'macro'}
        
        if data_type not in supported_types:
            return False
        
        # Проверка инструмента
        if instrument in self._supported_tickers:
            return True
        
        if instrument in self._supported_macro:
            return True
        
        return False
    
    def get_rate_limits(self) -> Dict[str, Any]:
        """Получение текущих лимитов запросов."""
        return {
            'provider': self.name,
            'requests_per_second': self._rate_limiter._state.refill_rate,
            'burst_size': self._rate_limiter._state.burst_size,
            'available_tokens': self._rate_limiter._state.tokens,
            'circuit_breaker_state': self.circuit_breaker_state.value,
            'base_url': self.base_url,
        }
