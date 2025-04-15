import aiohttp
import asyncio

BINANCE_API_URL = "https://api.binance.com/api/v3/ticker/24hr"

async def fetch_top_100_symbols():
    async with aiohttp.ClientSession() as session:
        async with session.get(BINANCE_API_URL) as response:
            if response.status == 200:
                data = await response.json()
                
                # Filter symbols that have USDT pairs
                symbols = [item['symbol'] for item in data if item['symbol'].endswith('USDT')]
                
                # Sort by market capitalization (volume * price is an approximation)
                symbols.sort(key=lambda symbol: float([item['quoteVolume'] for item in data if item['symbol'] == symbol][0]), reverse=True)
                
                # Take the top 100
                top_100_symbols = symbols[:100]
                
                return top_100_symbols
            else:
                print(f"Error fetching data: {response.status}")
                return []

async def main():
    top_100_symbols = await fetch_top_100_symbols()
    if top_100_symbols:
        print("Top 100 symbols by market cap (USDT pairs):")
        
        # Store the top 100 symbols in a list for later use
        all_symbols = top_100_symbols
        
        # Print all symbols
        for symbol in all_symbols:
            print(symbol)
        
        # You can also return or manipulate the symbols list as needed
        return all_symbols
    else:
        print("No data found")

# Run the async function
all_symbols = asyncio.run(main())

# Store all symbols outside the async function if you want to use them later
if all_symbols:
    print("\nAll stored symbols:")
    print(all_symbols)
