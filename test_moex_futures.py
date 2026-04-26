import asyncio
import aiohttp

async def test():
    # Тестируем разные варианты URL для фьючерса BR с правильными контрактами
    # Сейчас 25 апреля 2026, следующий квартальный контракт - Июнь 2026 (BRM6)
    urls = [
        # Неправильный - stock engine
        'https://iss.moex.com/iss/engines/stock/markets/main/boards/SPBFUT/securities/BR/candles',
        # Правильный - futures engine с конкретным контрактом BRM6 (Июнь 2026)
        'https://iss.moex.com/iss/engines/futures/markets/forts/securities/BRM6/candles',
        # Контракт BRU6 (Сентябрь 2026)
        'https://iss.moex.com/iss/engines/futures/markets/forts/securities/BRU6/candles',
    ]
    
    params = {'from': '2026-04-01', 'to': '2026-07-31', 'interval': '7'}
    
    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    text = await r.text()
                    # Проверяем есть ли данные в ответе
                    has_rows = '<row ' in text
                    print(f"URL: {url}")
                    print(f"  Status: {r.status}, Has data rows: {has_rows}")
                    if has_rows:
                        # Извлекаем первую строку данных
                        start = text.find('<row ')
                        end = text.find('/>', start) + 2
                        print(f"  First row: {text[start:end]}")
            except Exception as e:
                print(f"URL: {url}")
                print(f"  Error: {e}")
            print()

if __name__ == '__main__':
    asyncio.run(test())
