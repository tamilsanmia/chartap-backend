# import asyncio
# import aiohttp
# from symbols import SYMBOLS
# import redis.asyncio as redis
# from fastapi import FastAPI, WebSocket, WebSocketDisconnect
# from fastapi.middleware.cors import CORSMiddleware
# import json

# # FastAPI App Initialization
# app = FastAPI()

# # Global Redis Connection
# redis_client = None

# # Binance API Endpoints
# BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
# WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"


# CANDLE_COUNT = 250
# INTERVALS = {"5m": CANDLE_COUNT, "15m": CANDLE_COUNT, "1h": CANDLE_COUNT, "4h": CANDLE_COUNT, "1d": CANDLE_COUNT}

# websocket_clients = set()


# recent_closes = {symbol: {interval: [] for interval in INTERVALS} for symbol in SYMBOLS} 

# # Semaphore for API Rate Limiting
# semaphore = asyncio.Semaphore(50)  # Limits concurrent requests to 50


# def format_symbol(symbol: str) -> str:
#     return symbol.upper() + "USDT" if not symbol.endswith("USDT") else symbol.upper()


# async def fetch_historical_data(symbol: str, interval: str, limit: int = CANDLE_COUNT):
#     """
#     Fetch historical OHLCV data from Binance API and store it in Redis.
#     """
#     async with semaphore:
#         symbol = format_symbol(symbol)
#         params = {"symbol": symbol, "interval": interval, "limit": limit}
#         recent_closes_symbol = recent_closes[symbol][interval]

#         async with aiohttp.ClientSession() as session:
#             async with session.get(BINANCE_API_URL, params=params) as response:
#                 if response.status == 200:
#                     data = await response.json()
#                     ohlc_data = []

#                     for entry in data:
#                         time, open_price, high, low, close_price, volume = entry[:6]
#                         open_price, high, low, close_price, volume = map(float, [open_price, high, low, close_price, volume])

#                         recent_closes_symbol.append(close_price)
#                         if len(recent_closes_symbol) > 9:
#                             recent_closes_symbol.pop(0)

#                         rolling_average_current = rolling_average_sixth = rolling_average_fourth = waterfall = None
#                         if len(recent_closes_symbol) == 9:
#                             rolling_average_current = sum(recent_closes_symbol[-4:]) / 4
#                             rolling_average_sixth = sum(recent_closes_symbol[-9:-5]) / 4
#                             rolling_average_fourth = sum(recent_closes_symbol[-7:-3]) / 4
#                             if rolling_average_fourth != 0:
#                                 waterfall = -((rolling_average_sixth - rolling_average_current) / rolling_average_fourth) * 100

#                         ohlc_data.append({
#                             "time": time,
#                             "open": open_price,
#                             "high": high,
#                             "low": low,
#                             "close": close_price,
#                             "volume": volume,
#                             "rolling_average_current": rolling_average_current,
#                             "waterfall": waterfall,
#                         })

#                     key = f"{symbol}_{interval}"
#                     await redis_client.set(key, json.dumps(ohlc_data))
#                     print(f"{key} historical data saved to Redis")


# async def binance_websocket():
#     """
#     Connect to Binance WebSocket and stream live kline data.
#     """
#     async with aiohttp.ClientSession() as session:
#         async with session.ws_connect(WEBSOCKET_URL) as ws:
#             chunk_size = 100  # Avoids rate limit
#             symbol_chunks = [SYMBOLS[i:i + chunk_size] for i in range(0, len(SYMBOLS), chunk_size)]

#             for chunk in symbol_chunks:
#                 subscribe_msg = {
#                     "method": "SUBSCRIBE",
#                     "params": [f"{symbol.lower()}@kline_{interval}" for symbol in chunk for interval in INTERVALS],
#                     "id": 1
#                 }
#                 await ws.send_json(subscribe_msg)
#                 await asyncio.sleep(0.5)

#             async for msg in ws:
#                 if msg.type == aiohttp.WSMsgType.TEXT:
#                     data = json.loads(msg.data)
#                     if 'k' in data:
#                         kline = data['k']
#                         symbol = kline['s']
#                         interval = kline['i']

#                         recent_closes_symbol = recent_closes[symbol][interval]
#                         recent_closes_symbol.append(float(kline['c']))
#                         if len(recent_closes_symbol) > 9:
#                             recent_closes_symbol.pop(0)

#                         rolling_average_current = rolling_average_sixth = rolling_average_fourth = waterfall = None
#                         if len(recent_closes_symbol) == 9:
#                             rolling_average_current = sum(recent_closes_symbol[-4:]) / 4
#                             rolling_average_sixth = sum(recent_closes_symbol[-9:-5]) / 4
#                             rolling_average_fourth = sum(recent_closes_symbol[-7:-3]) / 4
#                             if rolling_average_fourth != 0:
#                                 waterfall = -((rolling_average_sixth - rolling_average_current) / rolling_average_fourth) * 100

#                         update = {
#                             "time": kline['t'],
#                             "open": float(kline['o']),
#                             "high": float(kline['h']),
#                             "low": float(kline['l']),
#                             "close": float(kline['c']),
#                             "volume": float(kline['v']),
#                             "rolling_average_current": rolling_average_current,
#                             "waterfall": format(waterfall, '.14f')
#                         }

#                         for client in websocket_clients:
#                             try:
#                                 await client.send_json({"symbol": symbol, "interval": interval, "data": update})
#                             except Exception as e:
#                                 print(f"Error sending to client: {e}")


# @app.get("/historical/{symbol}/{interval}")
# async def get_historical_data(symbol: str, interval: str):
#     """
#     API Endpoint to get historical data from Redis.
#     """
#     symbol = format_symbol(symbol)
#     key = f"{symbol}_{interval}"
#     data = await redis_client.get(key)

#     if not data:
#         await fetch_historical_data(symbol, interval, INTERVALS.get(interval, 250))
#         data = await redis_client.get(key)

#     if data:
#         return {"symbol": symbol, "interval": interval, "data": json.loads(data)}
#     return {"error": "Data not found"}


# @app.websocket("/ws")
# async def websocket_endpoint(websocket: WebSocket):
#     await websocket.accept()
#     websocket_clients.add(websocket)
#     print("Client connected to WebSocket")

#     try:
#         while True:
#             await websocket.receive_text()  # Ensure this is receiving only valid WebSocket messages
#     except WebSocketDisconnect:
#         websocket_clients.remove(websocket)
#         print("Client disconnected from WebSocket")



# async def periodic_fetch_all_symbols_data():
#     """
#     Periodically fetch historical data for all symbols every 2 seconds.
#     """
#     while True:
#         tasks = [fetch_historical_data(symbol, interval) for symbol in SYMBOLS for interval in INTERVALS]
#         await asyncio.gather(*tasks)
#         await asyncio.sleep(2)  # Run every 2 seconds



# @app.on_event("startup")
# async def startup_event():
#     global redis_client
#     redis_client = await redis.Redis(host="localhost", port=6379, db=0)
#     asyncio.create_task(periodic_fetch_all_symbols_data())
#     asyncio.create_task(binance_websocket())


# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


import asyncio
import aiohttp
from symbols import SYMBOLS
import redis.asyncio as redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import json
import logging
from typing import Dict, Set, Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
redis_client: Optional[redis.Redis] = None
websocket_clients: Set[WebSocket] = set()

# Constants
BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
WEBSOCKET_URL = "wss://stream.binance.com:9443/ws"
CANDLE_COUNT = 250
INTERVALS = {"5m": CANDLE_COUNT, "15m": CANDLE_COUNT, "1h": CANDLE_COUNT, "4h": CANDLE_COUNT,"1w": CANDLE_COUNT, "1d": CANDLE_COUNT,}
semaphore = asyncio.Semaphore(50)  # API rate limiting

# Initialize recent_closes with valid symbols only
recent_closes: Dict = {}
for symbol in SYMBOLS:
    formatted_symbol = None
    try:
        formatted_symbol = symbol.upper() + "USDT" if not symbol.upper().endswith("USDT") else symbol.upper()
        if formatted_symbol == "USDT":
            logger.warning(f"Skipping invalid symbol: {symbol}. USDT alone is not a valid trading pair.")
            continue
        recent_closes[formatted_symbol] = {interval: [] for interval in INTERVALS}
    except Exception as e:
        logger.error(f"Error formatting symbol {symbol}: {str(e)}")

def format_symbol(symbol: str) -> Optional[str]:
    """Format symbol and skip invalid ones like USDT."""
    symbol = symbol.upper()
    if symbol == "USDT":
        logger.warning(f"Skipping invalid symbol: {symbol}. USDT alone is not a valid trading pair.")
        return None
    return symbol + "USDT" if not symbol.endswith("USDT") else symbol

async def fetch_historical_data(symbol: str, interval: str, limit: int = CANDLE_COUNT) -> None:
    """Fetch historical OHLCV data from Binance API and store in Redis."""
    async with semaphore:
        formatted_symbol = format_symbol(symbol)
        if not formatted_symbol:
            return  # Skip invalid symbols
        
        key = f"{formatted_symbol}_{interval}"
        try:
            params = {"symbol": formatted_symbol, "interval": interval, "limit": limit}
            async with aiohttp.ClientSession() as session:
                async with session.get(BINANCE_API_URL, params=params) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        logger.error(f"API error for {key}: {response.status} - {error_text}")
                        return
                    
                    data = await response.json()
                    ohlc_data = []
                    recent_closes_symbol = recent_closes[formatted_symbol][interval]
                  

                    for entry in data:
                        time, open_p, high, low, close_p, volume = map(float, entry[:6])
                        recent_closes_symbol.append(close_p)
                        if len(recent_closes_symbol) > 9:
                            recent_closes_symbol.pop(0)

                        rolling_avg_current = rolling_avg_sixth = rolling_avg_fourth = waterfall = None
                        if len(recent_closes_symbol) == 9:
                            rolling_avg_current = sum(recent_closes_symbol[-4:]) / 4
                            rolling_avg_sixth = sum(recent_closes_symbol[-9:-5]) / 4
                            rolling_avg_fourth = sum(recent_closes_symbol[-7:-3]) / 4
                            waterfall = (-((rolling_avg_sixth - rolling_avg_current) / rolling_avg_fourth) * 100 
                                       if rolling_avg_fourth else None)

                        ohlc_data.append({
                            "time": int(time),
                            "open": open_p,
                            "high": high,
                            "low": low,
                            "close": close_p,
                            "volume": volume,
                            "rolling_average_current": rolling_avg_current,
                            "waterfall": waterfall
                        })

                    await redis_client.setex(key, 3600, json.dumps(ohlc_data))  # Expire after 1 hour
                    logger.info(f"Updated historical data for {key}")
        except Exception as e:
            logger.error(f"Error fetching historical data for {key}: {str(e)}")

async def binance_websocket() -> None:
    """Stream live kline data from Binance WebSocket."""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(WEBSOCKET_URL) as ws:
                    chunk_size = 100
                    valid_symbols = [s for s in SYMBOLS if format_symbol(s) is not None]
                    symbol_chunks = [valid_symbols[i:i + chunk_size] for i in range(0, len(valid_symbols), chunk_size)]
                    
                    for chunk in symbol_chunks:
                        formatted_chunk = [format_symbol(symbol) for symbol in chunk if format_symbol(symbol) is not None]
                        await ws.send_json({
                            "method": "SUBSCRIBE",
                            "params": [f"{symbol.lower()}@kline_{interval}" for symbol in formatted_chunk for interval in INTERVALS],
                            "id": 1
                        })
                        await asyncio.sleep(0.5)

                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        
                        data = json.loads(msg.data)
                        if 'k' not in data:
                            continue

                        kline = data['k']
                        symbol, interval = kline['s'], kline['i']
                        if symbol not in recent_closes:  # Skip if symbol wasn't initialized
                            continue
                        
                        recent_closes_symbol = recent_closes[symbol][interval]
                        close_price = float(kline['c'])
                        
                        recent_closes_symbol.append(close_price)
                        if len(recent_closes_symbol) > 9:
                            recent_closes_symbol.pop(0)

                        rolling_avg_current = rolling_avg_sixth = rolling_avg_fourth = waterfall = None
                        if len(recent_closes_symbol) == 9:
                            rolling_avg_current = sum(recent_closes_symbol[-4:]) / 4
                            rolling_avg_sixth = sum(recent_closes_symbol[-9:-5]) / 4
                            rolling_avg_fourth = sum(recent_closes_symbol[-7:-3]) / 4
                            waterfall = (-((rolling_avg_sixth - rolling_avg_current) / rolling_avg_fourth) * 100 )
                                       

                        update = {
                            "time": kline['t'],
                            "open": float(kline['o']),
                            "high": float(kline['h']),
                            "low": float(kline['l']),
                            "close": close_price,
                            "volume": float(kline['v']),
                            "rolling_average_current": rolling_avg_current,
                            "waterfall": format(waterfall, '.14f')
                        }

                        disconnected_clients = set()
                        for client in websocket_clients:
                            try:
                                await client.send_json({"symbol": symbol, "interval": interval, "data": update})
                            except Exception:
                                disconnected_clients.add(client)
                        websocket_clients.difference_update(disconnected_clients)

        except Exception as e:
            logger.error(f"WebSocket error: {str(e)}. Reconnecting in 5 seconds...")
            await asyncio.sleep(5)

@app.get("/historical/{symbol}/{interval}")
async def get_historical_data(symbol: str, interval: str):
    """Retrieve historical data from Redis."""
    formatted_symbol = format_symbol(symbol)
    if not formatted_symbol:
        return {"error": f"Invalid symbol: {symbol}"}
    
    key = f"{formatted_symbol}_{interval}"
    try:
        data = await redis_client.get(key)
        if not data:
            await fetch_historical_data(symbol, interval, INTERVALS.get(interval, CANDLE_COUNT))
            data = await redis_client.get(key)
        
        return {"symbol": formatted_symbol, "interval": interval, "data": json.loads(data)} if data else {"error": "Data not found"}
    except Exception as e:
        logger.error(f"Error retrieving historical data for {key}: {str(e)}")
        return {"error": "Internal server error"}

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle WebSocket connections."""
    await websocket.accept()
    websocket_clients.add(websocket)
    logger.info(f"New WebSocket client connected. Total: {len(websocket_clients)}")
    
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        websocket_clients.remove(websocket)
        logger.info(f"WebSocket client disconnected. Total: {len(websocket_clients)}")
    except Exception as e:
        logger.error(f"WebSocket client error: {str(e)}")
        websocket_clients.remove(websocket)

async def periodic_fetch_all_symbols_data() -> None:
    """Periodically fetch historical data for all symbols."""
    while True:
        try:
            valid_symbols = [s for s in SYMBOLS if format_symbol(s) is not None]
            tasks = [fetch_historical_data(symbol, interval) for symbol in valid_symbols for interval in INTERVALS]
            await asyncio.gather(*tasks)
            logger.info("Completed periodic data fetch cycle")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Periodic fetch error: {str(e)}")
            await asyncio.sleep(5)  # Back off on error

@app.on_event("startup")
async def startup_event() -> None:
    """Initialize application resources."""
    global redis_client
    try:
        redis_client = await redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        await redis_client.ping()
        logger.info("Redis connection established")
        
        asyncio.create_task(periodic_fetch_all_symbols_data())
        asyncio.create_task(binance_websocket())
        logger.info("Background tasks started")
    except Exception as e:
        logger.error(f"Startup error: {str(e)}")
        raise

@app.on_event("shutdown")
async def shutdown_event() -> None:
    """Clean up resources on shutdown."""
    global redis_client
    if redis_client:
        await redis_client.close()
        logger.info("Redis connection closed")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)