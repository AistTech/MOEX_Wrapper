"""
MOEX (Московская Биржа) API Обёртка

Производственно-готовая обёртка для API Московской Биржи с корректной обработкой ошибок,
логированием, логикой повторных попыток и ограничением частоты запросов.
"""

import logging
import time
import asyncio
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import aiohttp
import math
import sys

LOG_COLORS = {
    'DEBUG': '\033[36m',    # Cyan
    'INFO': '\033[32m',     # Green
    'WARNING': '\033[33m',  # Yellow
    'ERROR': '\033[31m',    # Red
    'CRITICAL': '\033[41m', # Red background
    'RESET': '\033[0m'
}

class ColoredFormatter(logging.Formatter):
    """Пользовательский форматтер для добавления цветов и красивого форматирования логов."""
    def format(self, record):
        levelname = record.levelname
        color = LOG_COLORS.get(levelname, '')
        reset = LOG_COLORS['RESET']
        # Pretty time format
        asctime = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        # Pretty log message
        message = f"{color}{asctime} | {levelname:<8} | {record.name:<20} | {record.getMessage()}{reset}"
        if record.exc_info:
            # Add exception info in red
            message += f"\n{LOG_COLORS['ERROR']}{self.formatException(record.exc_info)}{reset}"
        return message

@dataclass
class MOEXConfig:
    """Конфигурация для обёртки MOEX API."""
    base_url: str = "https://iss.moex.com/iss"
    timeout: int = 30
    max_retries: int = 3
    backoff_factor: float = 0.3
    rate_limit_delay: float = 0.05  # Minimum delay between requests (20 requests per second)
    user_agent: str = "MOEX-Wrapper/1.0"


class MOEXError(Exception):
    """Базовое исключение для ошибок MOEX API."""
    pass


class MOEXAPIError(MOEXError):
    """Исключение, вызываемое для ошибок, связанных с API."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        self.status_code = status_code
        self.response_text = response_text
        super().__init__(message)


class MOEXWrapper:
    """
    Производственно-готовая обёртка MOEX API.
    
    Предоставляет методы для взаимодействия с API Московской Биржи с корректной обработкой ошибок,
    логикой повторных попыток и ограничением частоты запросов.
    """
    
    def __init__(self, config: Optional[MOEXConfig] = None, verbose: int = 1):
        """
        Инициализация обёртки MOEX.
        
        Args:
            config: Объект конфигурации. Если None, используется конфигурация по умолчанию.
            verbose: Уровень детализации (0=без логгера, 1=логгер, 2=отладочный логгер)
        """
        self.config = config or MOEXConfig()
        self.verbose = verbose
        if self.verbose > 0:
            self.logger = self._setup_logger()
        self.session = self._setup_session()
        self._last_request_time = 0.0
        self._security_info_cache: Dict[str, Dict[str, str]] = {}  
        
    def _setup_logger(self) -> logging.Logger:
        """Настройка логирования для обёртки с цветным и красивым выводом."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = ColoredFormatter()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            # Set log level based on verbosity
            if self.verbose == 0:
                logger.setLevel(logging.INFO)
            elif self.verbose == 2:
                logger.setLevel(logging.DEBUG)
            else:
                logger.setLevel(logging.INFO)
        return logger
    
    def _setup_session(self) -> requests.Session:
        """Настройка сессии requests со стратегией повторных попыток."""
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set default headers
        session.headers.update({
            'User-Agent': self.config.user_agent,
            'Accept': 'application/json',
        })
        
        return session
    def _rate_limit(self) -> None:
        """Реализация ограничения частоты запросов между запросами."""
        current_time = time.time()
        time_since_last = current_time - self._last_request_time
        
        if time_since_last < self.config.rate_limit_delay:
            sleep_time = self.config.rate_limit_delay - time_since_last
            time.sleep(sleep_time)
        
        self._last_request_time = time.time()
    
    def _number_of_paginations(self, from_date: str, till_date: str, interval: int) -> int:
        """
        Рассчитать количество пагинаций, необходимых для получения всех данных.
        """
        number_returned = 500

        # Calculate number of days between from_date and till_date (inclusive)
        day_array = pd.date_range(from_date, till_date, freq='D')
        num_days = len(day_array)
        weekdays = day_array[day_array.weekday < 5]
        num_weekdays = len(weekdays)
        num_weekends = num_days - num_weekdays

        # Estimate number of candles
        if interval in [1, 10, 60]:
            # Market open 10:00-23:50 (14 hours, 840 minutes), so 840/interval candles per weekday
            candles_per_weekday = int(14 * 60 // interval)
            # For weekends, assume no trading (0 candles)
            total_candles = num_weekdays * candles_per_weekday
        elif interval == 24:
            # One candle per day (daily)
            total_candles = num_weekdays
        elif interval == 7:
            # One candle per week (weekly)
            # Count number of unique weeks in the range
            total_candles = len(set(day_array.to_period('W')))
        elif interval == 31:
            # One candle per month (monthly)
            total_candles = len(set(day_array.to_period('M')))
        else:
            # Fallback: estimate by minutes between dates divided by interval
            total_minutes = (datetime.strptime(till_date, '%Y-%m-%d') - datetime.strptime(from_date, '%Y-%m-%d')).total_seconds() / 60
            total_candles = int(total_minutes // interval)

        # Calculate number of paginations needed
        number_of_paginations = math.ceil(total_candles / number_returned)
        return max(1, number_of_paginations)
        
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Выполнить запрос к MOEX API.
        
        Args:
            endpoint: Конечная точка API (без базового URL)
            params: Параметры запроса
            
        Returns:
            JSON ответ в виде словаря
            
        Raises:
            MOEXAPIError: Если запрос к API не удался
        """
        self._rate_limit()
        
        url = f"{self.config.base_url}/{endpoint.lstrip('/')}"

        try:
            if self.verbose == 2:
                self.logger.debug(f"Making request to: {url} with params: {params}")
            
            response = self.session.get(
                url,
                params=params,
                timeout=self.config.timeout
            )
            
            response.raise_for_status()
            
            # Log successful request
            if self.verbose == 2:
                self.logger.debug(f"Request successful: {response.status_code}")
            
            return response.json()
            
        except requests.exceptions.HTTPError as e:
            error_msg = f"HTTP error occurred: {e}"
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(error_msg)
            raise MOEXAPIError(
                error_msg,
                status_code=response.status_code if 'response' in locals() else None,
                response_text=response.text if 'response' in locals() else None
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Request failed: {e}"
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(error_msg)
            raise MOEXAPIError(error_msg)
            
        except ValueError as e:
            error_msg = f"Failed to parse JSON response: {e}"
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(error_msg)
            raise MOEXAPIError(error_msg)
    
    def _process_moex_data(self, data_block: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Обработка формата данных MOEX API (столбцы + массивы данных) в список словарей.
        
        Args:
            data_block: Словарь, содержащий ключи 'columns' и 'data'
            
        Returns:
            Список словарей с именами столбцов в качестве ключей
        """
        if not data_block or 'columns' not in data_block or 'data' not in data_block:
            return []
        
        columns = data_block['columns']
        rows = data_block['data']
        
        return [
            {columns[i]: row[i] for i in range(len(columns))}
            for row in rows
        ]
    
    def _response_to_blocks(self, response: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Преобразование ответа MOEX API в блоки с обработанными данными.
        
        Args:
            response: Сырой ответ API
            
        Returns:
            Словарь с именами блоков в качестве ключей и обработанными данными в качестве значений
        """
        return {
            block_name: self._process_moex_data(block_data)
            for block_name, block_data in response.items()
        }
    
    def _add_custom_fields(self, securities: List[Dict[str, Any]], request_params: Optional[Dict[str, Any]] = None) -> None:
        """
        Добавление пользовательских полей к данным ценных бумаг (аналогично JavaScript _securitiesCustomFields).
        
        Args:
            securities: Список данных ценных бумаг
            request_params: Параметры запроса для контекста
        """
        request_params = request_params or {}
        
        for security in securities:
            # Add node with processed fields
            security['node'] = {
                'last': security.get('LAST') or security.get('CURRENTVALUE'),
                'volume': (security.get('VALTODAY_RUR') or 
                          security.get('VALTODAY') or 
                          security.get('VALTODAY_USD')),
                'id': security.get('SECID'),
                'friendly_title': self._get_friendly_title(security, request_params)
            }
            
            # Use previous price if no current price available
            if not security['node']['last'] and security.get('security_info'):
                security['node']['last'] = security['security_info'].get('PREVPRICE')
    
    def _get_friendly_title(self, security: Dict[str, Any], request_params: Dict[str, Any]) -> str:
        """
        Получение дружественного названия для ценной бумаги на основе типа рынка.
        
        Args:
            security: Данные ценной бумаги
            request_params: Параметры запроса, содержащие информацию о рынке
            
        Returns:
            Строка дружественного названия
        """
        security_info = security.get('security_info', {})
        if not security_info:
            return security.get('SECID', '')
        
        market = request_params.get('market', '')
        
        if market == 'index':
            return security_info.get('NAME') or security_info.get('SHORTNAME', '')
        elif market == 'forts':
            return security_info.get('SECNAME') or security_info.get('SHORTNAME', '')
        else:
            return security_info.get('SHORTNAME', '')
    
    # Оригинальные методы из первой реализации
    def search_securities(self, query: str, **kwargs) -> pd.DataFrame:
        """
        Поиск ценных бумаг по строке запроса.
        
        Это реализует шаблон запроса, показанный в вашем notebook:
        https://iss.moex.com/iss/securities.json?q=Yandex
        
        Args:
            query: Строка поискового запроса
            **kwargs: Дополнительные параметры запроса
            
        Returns:
            DataFrame, содержащий данные ценных бумаг
            
        Raises:
            MOEXAPIError: Если запрос к API не удался
            ValueError: Если запрос пустой или недопустимый
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")
        
        # Prepare parameters
        params = {'q': query.strip()}
        params.update(kwargs)
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Searching securities for query: '{query}'")
        
        try:
            # Make the API request
            response_data = self._make_request('securities.json', params)
            
            # Process the securities data
            if 'securities' not in response_data:
                if self.verbose == 1 or self.verbose == 2:
                    self.logger.warning("No 'securities' block in response")
                return pd.DataFrame()
            
            securities_data = self._process_moex_data(response_data['securities'])
            
            if not securities_data:
                if self.verbose == 1 or self.verbose == 2:
                    self.logger.info(f"No securities found for query: '{query}'")
                return pd.DataFrame()
            
            # Create DataFrame
            df = pd.DataFrame(securities_data)
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Found {len(df)} securities for query: '{query}'")
            
            return df
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error searching securities: {e}")
            raise
    
    def get_common_shares(self, query: str, **kwargs) -> pd.DataFrame:
        """
        Поиск ценных бумаг и фильтрация только обыкновенных акций.
        
        Это реализует полный шаблон из вашего notebook, включая фильтрацию.
        
        Args:
            query: Строка поискового запроса
            **kwargs: Дополнительные параметры запроса
            
        Returns:
            DataFrame, содержащий только обыкновенные акции
        """
        df = self.search_securities(query, **kwargs)
        
        if df.empty:
            return df
        
        # Filter for common shares
        if 'type' in df.columns:
            common_shares = df[df['type'] == 'common_share']
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Filtered to {len(common_shares)} common shares")
            return common_shares
        else:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.warning("No 'type' column found in securities data")
            return df
    
    # Новые методы, портированные из JavaScript API
    def engines(self) -> List[Dict[str, Any]]:
        """
        Получить все доступные движки.
        
        Returns:
            Список словарей с информацией о движках
        """
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info("Fetching available engines")
        
        try:
            response_data = self._make_request('engines.json')
            engines_data = self._process_moex_data(response_data.get('engines', {}))
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Found {len(engines_data)} engines")
            return engines_data
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching engines: {e}")
            raise
    
    def markets(self, engine: str) -> List[Dict[str, Any]]:
        """
        Получить все рынки для определенного движка.
        
        Args:
            engine: Название движка (например, 'stock', 'currency')
            
        Returns:
            Список словарей с информацией о рынках
        """
        if not engine:
            raise ValueError("Engine parameter is required")
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching markets for engine: {engine}")
        
        try:
            endpoint = f'engines/{engine}/markets.json'
            response_data = self._make_request(endpoint)
            markets_data = self._process_moex_data(response_data.get('markets', {}))
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Found {len(markets_data)} markets for engine {engine}")
            return markets_data
            
        except Exception as e:
            self.logger.error(f"Error fetching markets for engine {engine}: {e}")
            raise
    
    def boards(self, engine: str, market: str) -> List[Dict[str, Any]]:
        """
        Получить все режимы торгов для определенного движка и рынка.
        
        Args:
            engine: Название движка (например, 'stock')
            market: Название рынка (например, 'shares')
            
        Returns:
            Список словарей с информацией о режимах торгов
        """
        if not engine or not market:
            raise ValueError("Both engine and market parameters are required")
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching boards for engine: {engine}, market: {market}")
        
        try:
            endpoint = f'engines/{engine}/markets/{market}/boards.json'
            response_data = self._make_request(endpoint)
            boards_data = self._process_moex_data(response_data.get('boards', {}))
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Found {len(boards_data)} boards for {engine}/{market}")
            return boards_data
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching boards for {engine}/{market}: {e}")
            raise
    
    def index(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Получить информацию об индексе (глобальные константы) из MOEX API.
        
        Returns:
            Словарь с информацией о движках, рынках и режимах торгов
        """
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info("Fetching MOEX index information")
        
        try:
            response_data = self._make_request('.json')  # Empty endpoint for index
            blocks = self._response_to_blocks(response_data)
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info("Successfully fetched MOEX index information")
            return blocks
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching MOEX index: {e}")
            raise
    
    def securities_definitions(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Получить определения для всех ценных бумаг.
        
        Args:
            **kwargs: Параметры запроса (например, start, first для пагинации)
            
        Returns:
            Список словарей с определениями ценных бумаг
        """
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info("Fetching securities definitions")
        
        try:
            response_data = self._make_request('securities.json', kwargs)
            securities_data = self._process_moex_data(response_data.get('securities', {}))
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Found {len(securities_data)} securities definitions")
            return securities_data
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching securities definitions: {e}")
            raise
    
    def security_definition(self, security: str) -> Dict[str, Any]:
        """
        Получить определение для конкретной ценной бумаги.
        
        Args:
            security: ID ценной бумаги
            
        Returns:
            Словарь с определением ценной бумаги, включая описание и режимы торгов
        """
        if not security:
            raise ValueError("Security parameter is required")
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching definition for security: {security}")
        
        try:
            endpoint = f'securities/{security}.json'
            response_data = self._make_request(endpoint)
            blocks = self._response_to_blocks(response_data)
            
            # Convert to indexed format similar to JavaScript version
            result: Dict[str, Any] = {}
            for block_name, block_data in blocks.items():
                if block_name == 'description' and isinstance(block_data, list):
                    result[block_name] = {
                        item['name']: item for item in block_data
                    }
                elif block_name == 'boards' and isinstance(block_data, list):
                    result[block_name] = {
                        item['boardid']: item for item in block_data
                    }
                else:
                    result[block_name] = block_data
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Successfully fetched definition for security: {security}")
            return result
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching definition for security {security}: {e}")
            raise
    
    def get_security_info(self, security: str) -> Dict[str, str]:
        """
        Получить и кэшировать информацию о движке/рынке ценной бумаги.
        
        Args:
            security: ID ценной бумаги
            
        Returns:
            Словарь с информацией о движке и рынке
        """
        if security in self._security_info_cache:
            return self._security_info_cache[security]
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching security info for: {security}")
        
        try:
            definition = self.security_definition(security)
            boards = list(definition.get('boards', {}).values())
            
            if not boards:
                raise MOEXError(f"Security {security} doesn't have any board in definition")
            
            # Use first board for engine/market info
            board = boards[0]
            info = {
                'engine': board.get('engine', ''),
                'market': board.get('market', '')
            }
            
            # Cache the result
            self._security_info_cache[security] = info
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Cached security info for {security}: {info}")
            return info
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error getting security info for {security}: {e}")
            raise
    
    def securities_data_raw(self, engine: str, market: str, **kwargs) -> Dict[str, Any]:
        """
        Получить сырые данные ценных бумаг для движка/рынка.
        
        Args:
            engine: Название движка
            market: Название рынка
            **kwargs: Дополнительные параметры запроса
            
        Returns:
            Словарь с сырым ответом API
        """
        if not engine or not market:
            raise ValueError("Both engine and market parameters are required")
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching raw securities data for {engine}/{market}")
        
        try:
            endpoint = f'engines/{engine}/markets/{market}/securities.json'
            response_data = self._make_request(endpoint, kwargs)
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Successfully fetched raw securities data for {engine}/{market}")
            return response_data
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching raw securities data for {engine}/{market}: {e}")
            raise
    
    def security_data_raw_explicit(self, engine: str, market: str, security: str) -> Dict[str, Any]:
        """
        Получить сырые данные для конкретной ценной бумаги в определенном движке/рынке.
        
        Args:
            engine: Название движка
            market: Название рынка
            security: ID ценной бумаги
            
        Returns:
            Словарь с сырым ответом API
        """
        if not all([engine, market, security]):
            raise ValueError("Engine, market, and security parameters are all required")
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching raw data for security {security} in {engine}/{market}")
        
        try:
            endpoint = f'engines/{engine}/markets/{market}/securities/{security}.json'
            response_data = self._make_request(endpoint)
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Successfully fetched raw data for {security}")
            return response_data
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching raw data for {security} in {engine}/{market}: {e}")
            raise
    
    def security_market_data_explicit(self, engine: str, market: str, security: str, 
                                    currency: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Получить рыночные данные для конкретной ценной бумаги с явным указанием движка/рынка.
        
        Args:
            engine: Название движка
            market: Название рынка
            security: ID ценной бумаги
            currency: Необязательный фильтр по валюте
            
        Returns:
            Словарь с рыночными данными или None, если данные не найдены
        """
        try:
            response_data = self.security_data_raw_explicit(engine, market, security)
            securities_data = self._response_to_securities(response_data, {'engine': engine, 'market': market})
            
            # Filter for securities with last price and currency
            filtered_securities = [
                sec for sec in securities_data 
                if sec['node']['last'] and self._filter_by_currency(sec, currency)
            ]
            
            if not filtered_securities:
                return None
            
            # Sort by VALTODAY descending and return first
            sorted_securities = sorted(
                filtered_securities, 
                key=lambda x: x.get('VALTODAY', 0) or 0, 
                reverse=True
            )
            
            return sorted_securities[0]
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching market data for {security}: {e}")
            raise
    
    def security_market_data(self, security: str, currency: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Получить рыночные данные для ценной бумаги (автоматически определяет движок/рынок).
        
        Args:
            security: ID ценной бумаги
            currency: Необязательный фильтр по валюте
            
        Returns:
            Словарь с рыночными данными или None, если данные не найдены
        """
        try:
            security_info = self.get_security_info(security)
            return self.security_market_data_explicit(
                security_info['engine'], 
                security_info['market'], 
                security, 
                currency
            )
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching market data for {security}: {e}")
            raise
    
    def _response_to_securities(self, response: Dict[str, Any], request_params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Обработка ответа API в формат ценных бумаг с рыночными данными.
        
        Args:
            response: Сырой ответ API
            request_params: Параметры запроса для контекста
            
        Returns:
            Список обработанных ценных бумаг с рыночными данными
        """
        blocks = self._response_to_blocks(response)
        
        # Index securities info by SECID_BOARDID
        securities_info = {}
        if 'securities' in blocks:
            for sec in blocks['securities']:
                key = f"{sec.get('SECID', '')}_{sec.get('BOARDID', '')}"
                securities_info[key] = sec
        
        # Process market data
        securities = blocks.get('marketdata', [])
        
        # Attach security info to each security
        for security in securities:
            key = f"{security.get('SECID', '')}_{security.get('BOARDID', '')}"
            security['security_info'] = securities_info.get(key, {})
        
        # Add custom fields
        self._add_custom_fields(securities, request_params)
        
        return securities
    
    def _filter_by_currency(self, security: Dict[str, Any], currency: Optional[str]) -> bool:
        """
        Фильтрация ценной бумаги по валюте.
        
        Args:
            security: Данные ценной бумаги
            currency: Валюта для фильтрации (None означает отсутствие фильтра)
            
        Returns:
            True, если ценная бумага соответствует фильтру по валюте
        """
        if currency is None:
            return True
        
        security_info = security.get('security_info', {})
        return security_info.get('CURRENCYID') == currency
    
    def securities_market_data(self, engine: str, market: str, first: Optional[int] = None, 
                             **kwargs) -> Dict[str, Dict[str, Any]]:
        """
        Получить рыночные данные для нескольких ценных бумаг (сгруппированные по SECID с наибольшим объемом режима торгов).
        
        Args:
            engine: Название движка
            market: Название рынка
            first: Ограничение количества результатов
            **kwargs: Дополнительные параметры запроса
            
        Returns:
            Словарь с SECID в качестве ключей и рыночными данными в качестве значений
        """
        # Set default sorting
        if 'sort_column' not in kwargs:
            kwargs['sort_order'] = 'desc'
            kwargs['sort_column'] = 'VALTODAY'
        
        try:
            response_data = self.securities_data_raw(engine, market, **kwargs)
            securities = self._response_to_securities(response_data, {'engine': engine, 'market': market})
            # Group by SECID, keeping highest VALTODAY for each
            grouped_data: Dict[str, Dict[str, Any]] = {}
            for security in securities:
                sec_id = security.get('SECID')
                if not sec_id or not security['node']['last']:
                    continue
                
                valtoday = security.get('VALTODAY', 0) or 0
                
                if sec_id not in grouped_data or grouped_data[sec_id].get('VALTODAY', 0) < valtoday:
                    grouped_data[sec_id] = security
            
            # Apply first limit if specified
            if first:
                sorted_securities = sorted(
                    grouped_data.values(), 
                    key=lambda x: x.get('VALTODAY', 0) or 0, 
                    reverse=True
                )[:first]
                grouped_data = {sec['SECID']: sec for sec in sorted_securities}
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Returned {len(grouped_data)} securities with market data")
            return grouped_data

        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching securities market data: {e}")
            raise
    
    def candles_data_explicit(self, engine: str, market: str, security: str, 
                            from_date: str, till_date: str, interval: int = 24, start: int = 0) -> pd.DataFrame:
        """
        Получить данные свечей (OHLCV) для конкретной ценной бумаги с явным указанием движка/рынка.
        
        Args:
            engine: Название движка (например, 'stock')
            market: Название рынка (например, 'shares')
            security: ID ценной бумаги (например, 'SBER')
            from_date: Дата начала в формате YYYY-MM-DD
            till_date: Дата окончания в формате YYYY-MM-DD
            interval: Интервал свечей в минутах (1, 10, 60, 24=дневной, 7=недельный, 31=месячный)
            start: Начальный индекс для пагинации
            
        Returns:
            DataFrame с данными свечей (open, close, high, low, volume и т.д.)
            
        Raises:
            MOEXAPIError: Если запрос к API не удался
            ValueError: Если параметры недопустимы
        """
        if not all([engine, market, security, from_date, till_date]):
            raise ValueError("All parameters (engine, market, security, from_date, till_date) are required")
        
        # Validate date format (basic check)
        try:
            from datetime import datetime
            datetime.strptime(from_date, '%Y-%m-%d')
            datetime.strptime(till_date, '%Y-%m-%d')
        except ValueError:
            raise ValueError("Dates must be in YYYY-MM-DD format")
        
        # Validate interval
        valid_intervals = [1, 10, 60, 24, 7, 31]  # minutes, daily, weekly, monthly
        if interval not in valid_intervals:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.warning(f"Interval {interval} might not be valid. Valid intervals: {valid_intervals}")
        
        if self.verbose == 1 or self.verbose == 2:
            self.logger.info(f"Fetching candles for {security} from {from_date} to {till_date} (interval: {interval})")
        
        try:
            endpoint = f'engines/{engine}/markets/{market}/securities/{security}/candles.json'
            params = {
                'from': from_date,
                'till': till_date,
                'interval': interval,
                'start': start,
            }
            
            response_data = self._make_request(endpoint, params)
            
            # Process candles data
            if 'candles' not in response_data:
                if self.verbose == 1 or self.verbose == 2:
                    self.logger.warning("No 'candles' block in response")
                return pd.DataFrame()
            
            candles_data = self._process_moex_data(response_data['candles'])
            
            if not candles_data:
                if self.verbose == 1 or self.verbose == 2:
                    self.logger.info(f"No candles data found for {security}")
                return pd.DataFrame()
            
            # Create DataFrame
            df = pd.DataFrame(candles_data)
            
            # Convert datetime column if present
            if 'begin' in df.columns:
                try:
                    df['begin'] = pd.to_datetime(df['begin'])
                except Exception as e:
                    if self.verbose == 1 or self.verbose == 2:
                        self.logger.warning(f"Could not parse datetime column: {e}")
            
            # Set datetime as index if available
            if 'begin' in df.columns:
                df.set_index('begin', inplace=True)
            
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info(f"Retrieved {len(df)} candles for {security}")
            return df
            
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching candles for {security}: {e}")
            raise
    
    def candles_data(self, security: str, from_date: str, till_date: str, 
                    interval: int = 24, start: int = 0) -> pd.DataFrame:
        """
        Получить данные свечей (OHLCV) для ценной бумаги (автоматически определяет движок/рынок).
        
        Args:
            security: ID ценной бумаги (например, 'SBER')
            from_date: Дата начала в формате YYYY-MM-DD
            till_date: Дата окончания в формате YYYY-MM-DD
            interval: Интервал свечей в минутах (1, 10, 60, 24=дневной, 7=недельный, 31=месячный)
            
        Returns:
            DataFrame с данными свечей
            
        Examples:
            # Получить дневные свечи для SBER
            candles = moex.candles_data('SBER', '2023-01-01', '2023-12-31')
            
            # Получить часовые свечи для Яндекса
            candles = moex.candles_data('YNDX', '2023-05-25', '2023-09-01', interval=60)
        """
        try:
            security_info = self.get_security_info(security)
            number_of_paginations = self._number_of_paginations(from_date, till_date, interval)
            self.logger.info(f"Number of paginations: {number_of_paginations}")
            candles = pd.DataFrame()
            for i in range(number_of_paginations):
                candles = pd.concat([candles, self.candles_data_explicit(
                security_info['engine'], 
                security_info['market'], 
                security, 
                from_date, 
                till_date, 
                interval,
                start + i * 500
                )])
            return candles
        except Exception as e:
            if self.verbose == 1 or self.verbose == 2:
                self.logger.error(f"Error fetching candles for {security}: {e}")
            raise
    
    def close(self) -> None:
        """Закрыть сессию и очистить ресурсы."""
        if self.session:
            self.session.close()
            if self.verbose == 1 or self.verbose == 2:
                self.logger.info("MOEX wrapper session closed")
    
    def __enter__(self):
        """Вход в контекстный менеджер."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Выход из контекстного менеджера."""
        self.close()

