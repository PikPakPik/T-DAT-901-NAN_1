import os
import json
import asyncio
import websockets

async def listen_stream(url, stream_type):
    async with websockets.connect(url) as websocket:
        print(f"✅ Connected to {stream_type} stream: {url}")
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)
                print(f"[{stream_type}] Received: {data}")
            except Exception as e:
                print(f"⚠️ Error in {stream_type} stream: {e}")
                break

async def main():
    wsServer = os.environ.get('BINANCE_WEBSOCKET_URL')

    aggTrade = os.environ.get('BINANCE_AGGREGATE_TRADE_STEAMS').split(',')
    kline = os.environ.get('BINANCE_KLINE_STREAMS').split(',')

    aggTradeFormattedUrl = wsServer + "/".join(aggTrade)
    klineFormattedUrl = wsServer + "/".join(kline)

    tasks = []
    
    tasks.append(listen_stream(aggTradeFormattedUrl, "aggTrade"))
    tasks.append(listen_stream(klineFormattedUrl, "kline"))

    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())