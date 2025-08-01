# MOEX Wrapper

🚀 **Производственно-готовая обёртка для API Московской Биржи**

Современная Python библиотека для работы с API Московской Биржи (MOEX) с поддержкой корректной обработки ошибок, логирования, повторных попыток и ограничения частоты запросов.

## ✨ Особенности

- 🛡️ **Надёжность**: Встроенная обработка ошибок и логика повторных попыток
- ⚡ **Производительность**: Автоматическое ограничение частоты запросов и кэширование
- 📊 **Удобство**: Прямая интеграция с pandas DataFrame
- 🎨 **Логирование**: Красивые цветные логи с настраиваемым уровнем детализации
- 🔄 **Пагинация**: Автоматическая обработка больших наборов данных
- 🐍 **Современный Python**: Типизация, dataclasses, контекстные менеджеры

## 📦 Установка

```bash
pip install pandas requests aiohttp
```

Затем скопируйте файлы из репозитория в ваш проект.

## 🚀 Быстрый старт

```python
from moex.moex_wrapper import MOEXWrapper

# Создание экземпляра обёртки
moex = MOEXWrapper(verbose=1)

# Поиск ценных бумаг
securities = moex.search_securities("Сбербанк")
print(securities.head())

# Получение рыночных данных
market_data = moex.security_market_data("SBER")
print(f"Последняя цена SBER: {market_data['node']['last']}")

# Получение исторических данных (свечи)
candles = moex.candles_data("SBER", "2023-01-01", "2023-12-31")
print(candles.head())
```

## 📖 Подробные примеры

### Поиск ценных бумаг

```python
# Поиск по названию компании
yandex_securities = moex.search_securities("Яндекс")

# Фильтрация только обыкновенных акций
yandex_common = moex.get_common_shares("Яндекс")

# Поиск с дополнительными параметрами
securities = moex.search_securities("Газпром", limit=10)
```

### Работа с рыночными данными

```python
# Получение данных для конкретной ценной бумаги
sber_data = moex.security_market_data("SBER")
print(f"Цена: {sber_data['LAST']} RUB")
print(f"Объём торгов: {sber_data['VALTODAY']} RUB")

# Получение данных для всех ценных бумаг рынка
market_data = moex.securities_market_data("stock", "shares", first=50)
for secid, data in market_data.items():
    print(f"{secid}: {data['node']['last']} RUB")
```

### Исторические данные (свечи)

```python
# Дневные свечи за год
daily_candles = moex.candles_data("SBER", "2023-01-01", "2023-12-31", interval=24)

# Часовые свечи за месяц
hourly_candles = moex.candles_data("YNDX", "2023-11-01", "2023-11-30", interval=60)

# Минутные свечи за день
minute_candles = moex.candles_data("GAZP", "2023-12-01", "2023-12-01", interval=1)

print(f"Получено {len(daily_candles)} дневных свечей")
print(daily_candles[['open', 'close', 'high', 'low', 'volume']].head())
```

### Информация о структуре биржи

```python
# Получение всех движков
engines = moex.engines()
for engine in engines:
    print(f"Движок: {engine['name']} - {engine['title']}")

# Получение рынков для движка акций
stock_markets = moex.markets("stock")
for market in stock_markets:
    print(f"Рынок: {market['name']} - {market['title']}")

# Получение режимов торгов
boards = moex.boards("stock", "shares")
for board in boards:
    print(f"Режим: {board['boardid']} - {board['title']}")
```

## ⚙️ Конфигурация

```python
from moex.moex_wrapper import MOEXWrapper, MOEXConfig

# Пользовательская конфигурация
config = MOEXConfig(
    timeout=60,  # Таймаут запросов в секундах
    max_retries=5,  # Максимальное количество повторных попыток
    rate_limit_delay=0.1,  # Задержка между запросами (10 запросов в секунду)
    user_agent="МойПроект/1.0"
)

moex = MOEXWrapper(config=config, verbose=2)  # verbose=2 для отладочных логов
```

## 🎯 Уровни логирования

- `verbose=0`: Без логирования
- `verbose=1`: Основные операции и ошибки (по умолчанию)
- `verbose=2`: Подробная отладочная информация

## 🔄 Контекстный менеджер

```python
# Автоматическое закрытие сессии
with MOEXWrapper() as moex:
    data = moex.search_securities("Сбербанк")
    # Сессия автоматически закроется при выходе из блока
```

## 📊 Интервалы для свечей

- `1` - Минутные свечи
- `10` - 10-минутные свечи  
- `60` - Часовые свечи
- `24` - Дневные свечи (по умолчанию)
- `7` - Недельные свечи
- `31` - Месячные свечи

## 🛠️ API Reference

### Основные методы

#### `search_securities(query: str, **kwargs) -> pd.DataFrame`
Поиск ценных бумаг по строке запроса.

**Параметры:**
- `query`: Строка поиска
- `**kwargs`: Дополнительные параметры API

**Возвращает:** DataFrame с найденными ценными бумагами

#### `security_market_data(security: str, currency: Optional[str] = None) -> Optional[Dict]`
Получение рыночных данных для ценной бумаги.

**Параметры:**
- `security`: ID ценной бумаги (например, "SBER")
- `currency`: Фильтр по валюте (опционально)

**Возвращает:** Словарь с рыночными данными или None

#### `candles_data(security: str, from_date: str, till_date: str, interval: int = 24) -> pd.DataFrame`
Получение исторических данных (свечей).

**Параметры:**
- `security`: ID ценной бумаги
- `from_date`: Дата начала (YYYY-MM-DD)
- `till_date`: Дата окончания (YYYY-MM-DD)  
- `interval`: Интервал свечей в минутах

**Возвращает:** DataFrame с данными свечей

### Служебные методы

- `engines()` - Получить все движки
- `markets(engine)` - Получить рынки для движка
- `boards(engine, market)` - Получить режимы торгов
- `security_definition(security)` - Получить определение ценной бумаги

## ⚠️ Обработка ошибок

```python
from moex.moex_wrapper import MOEXAPIError, MOEXError

try:
    data = moex.security_market_data("INVALID_SECID")
except MOEXAPIError as e:
    print(f"Ошибка API: {e}")
    print(f"Код ответа: {e.status_code}")
except MOEXError as e:
    print(f"Общая ошибка MOEX: {e}")
```

## 🔍 Примеры анализа данных

```python
import matplotlib.pyplot as plt

# Получение и визуализация дневных свечей
candles = moex.candles_data("SBER", "2023-01-01", "2023-12-31")

# Построение графика цены закрытия
plt.figure(figsize=(12, 6))
plt.plot(candles.index, candles['close'])
plt.title('Динамика цены акций Сбербанка в 2023 году')
plt.xlabel('Дата')
plt.ylabel('Цена закрытия (RUB)')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Расчёт простой скользящей средней
candles['sma_20'] = candles['close'].rolling(window=20).mean()
candles['sma_50'] = candles['close'].rolling(window=50).mean()
```

## 📝 Требования

- Python 3.7+
- pandas
- requests  
- aiohttp (для асинхронной версии)

## 🤝 Вклад в проект

Мы приветствуем вклад в развитие проекта! Пожалуйста:

1. Форкните репозиторий
2. Создайте ветку для вашей функции (`git checkout -b feature/amazing-feature`)
3. Закоммитьте изменения (`git commit -m 'Добавить amazing-feature'`)
4. Запушьте в ветку (`git push origin feature/amazing-feature`)
5. Откройте Pull Request

## 📄 Лицензия

Этот проект лицензирован под лицензией MIT - см. файл [LICENSE](LICENSE) для подробностей.

## 📞 Поддержка

Если у вас есть вопросы или предложения, пожалуйста:
- Создайте issue в репозитории
- Ознакомьтесь с документацией API MOEX: https://iss.moex.com/iss/reference/

---

**Сделано с ❤️ для российского финтех-сообщества**