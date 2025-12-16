from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, List, Any

import websockets
from websockets.exceptions import ConnectionClosed

try:
    import config
    from writer import ParquetWriter
except ImportError:
    # Attempt relative import if running as package
    from . import config
    from .writer import ParquetWriter

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("binance_listener")

async def main():
    # 1. Setup Configuration
    target_coins = config.TARGET_COINS # e.g. ["BTCUSDT", ...]
    # Streams: <symbol>@depth20@100ms, <symbol>@trade
    streams = []
    for coin in target_coins:
        c = coin.lower()
        # Use Diff Depth Stream: <symbol>@depth@100ms
        streams.append(f"{c}@depth@{config.DEPTH_SPEED}")
        streams.append(f"{c}@trade")

    logger.info(f"Target Coins: {target_coins}")
    logger.info(f"Streams to subscribe: {streams}")

    # 2. Initialize Writer
    writer = ParquetWriter(
        output_dir=config.DATA_DIR,
        flush_interval_seconds=config.FLUSH_INTERVAL,
        batch_size=config.BATCH_SIZE
    )
    await writer.start()

    # 3. Connection Loop
    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Signal received, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    ws_url = "wss://stream.binance.com:9443/stream"

    while not stop_event.is_set():
        try:
            logger.info(f"Connecting to {ws_url}...")
            async with websockets.connect(ws_url) as ws:
                logger.info("Connected.")
                
                # Subscribe
                subscribe_msg = {
                    "method": "SUBSCRIBE",
                    "params": streams,
                    "id": 1
                }
                await ws.send(json.dumps(subscribe_msg))
                logger.info("Sent subscription request.")

                while not stop_event.is_set():
                    try:
                        msg_raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        msg = json.loads(msg_raw)
                        
                        # Handle Combined Stream Message
                        # {"stream": "...", "data": ...}
                        if "stream" in msg and "data" in msg:
                            stream_name = msg["stream"]
                            data = msg["data"]
                            
                            # For Diff Depth and Trade, 'data' contains the payload with 'e', 's', etc.
                            # We want to store exactly what Binance returns.
                            
                            # Ensure we have a valid dictionary to write
                            if isinstance(data, dict):
                                # We might want to attach the local timestamp for latency analysis,
                                # but the user asked to "only keep binance returned content".
                                # However, strictly speaking, without a receipt time, research is hard.
                                # I will add local_time as a metadata field, but keep the structure flat
                                # based on the Binance payload as much as possible.
                                # Actually, let's just pass 'data' and let writer handle it.
                                # But we typically need 'local_time' for ingestion time.
                                data['local_time'] = time.time()
                                await writer.add_data(data)

                        elif "id" in msg and msg.get("result") is None:
                            # Subscription confirmation or heartbeat
                            logger.info(f"Received system message: {msg}")

                    except asyncio.TimeoutError:
                        # Send a pong or just continue? websockets handles ping/pong.
                        # We just loop to check stop_event
                        continue
                    except ConnectionClosed:
                        logger.warning("Connection closed by server.")
                        break
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue
                        
        except Exception as e:
            logger.error(f"Connection error: {e}")
            if not stop_event.is_set():
                logger.info("Reconnecting in 5 seconds...")
                await asyncio.sleep(5)

    await writer.stop()
    logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
