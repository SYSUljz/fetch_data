from __future__ import annotations

import asyncio
import json
import logging
import signal
import sys
import time
from typing import Dict, List, Any

import requests

try:
    import hyperliquid.config as config
    from hyperliquid.utils.writer import ParquetWriter
except ImportError:
    # Attempt relative import if running as package
    from . import config
    from .utils.writer import ParquetWriter

from hyperliquid.utils.types import Subscription, WsMsg
from hyperliquid.websocket_manager import WebsocketManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("lob_research")

def fetch_spot_coins(target_base_names: List[str]) -> Dict[str, str]:
    """
    Fetches spot metadata and returns a map of Base -> SpotCoinName
    e.g. "BTC" -> "BTC" (if that's the spot name) or "HIFBTC" etc.
    """
    try:
        logger.info("Fetching spot metadata...")
        resp = requests.post(config.API_URL, json={"type": "spotMeta"}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        
        # universe is a list of {name: str, tokens: [int, int], ...}
        universe = data.get("universe", [])
        tokens = data.get("tokens", [])
        
        found_coins = {}
        universe_names = {u["name"] for u in universe}
        
        for base in target_base_names:
            if base in universe_names:
                found_coins[base] = base
            else:
                logger.warning(f"Could not find exact spot match for {base}")
        
        return found_coins
    except Exception as e:
        logger.error(f"Failed to fetch spot metadata: {e}")
        return {}

async def main():
    # 1. Identify Coins to Subscribe
    perp_coins = config.TARGET_COINS
    spot_map = fetch_spot_coins(config.TARGET_COINS)
    spot_coins = list(spot_map.values())
    
    logger.info(f"Perp Coins: {perp_coins}")
    logger.info(f"Spot Coins: {spot_coins}")
    
    all_subscriptions: List[Subscription] = []
    
    # Add Perp Subscriptions
    for coin in perp_coins:
        # Trades Subscription
        all_subscriptions.append({"type": "trades", "coin": coin})

        base_sub = {"type": "l2Book", "coin": coin}
        # Apply config if exists
        if coin in config.COIN_CONFIG:
            conf = config.COIN_CONFIG[coin]
            n_sig_figs = conf.get("nSigFigs")
            
            if isinstance(n_sig_figs, list):
                # Create multiple subscriptions for list of nSigFigs
                for n in n_sig_figs:
                    sub = base_sub.copy()
                    sub.update(conf)
                    sub["nSigFigs"] = n
                    all_subscriptions.append(sub)
            else:
                # Single subscription
                sub = base_sub.copy()
                sub.update(conf)
                all_subscriptions.append(sub)
        else:
            all_subscriptions.append(base_sub)

    if not all_subscriptions:
        logger.error("No subscriptions generated. Exiting.")
        return

    # Group subscriptions by nSigFigs
    subs_by_sigfigs = {}
    for sub in all_subscriptions:
        n = sub.get("nSigFigs")
        if n not in subs_by_sigfigs:
            subs_by_sigfigs[n] = []
        subs_by_sigfigs[n].append(sub)

    # 2. Initialize Writer
    writer = ParquetWriter(
        output_dir=config.DATA_DIR,
        flush_interval_seconds=config.FLUSH_INTERVAL,
        batch_size=config.BATCH_SIZE
    )
    await writer.start()

    # 3. Define Handler Factory
    loop = asyncio.get_running_loop()

    def create_message_handler(writer: ParquetWriter, n_sig_figs_context: int | None):
        async def async_handler(msg: WsMsg):
            # specific handling for l2Book
            channel = msg.get("channel")
            if channel == "l2Book":
                data = msg.get("data")
                if not data:
                    return
                
                coin = data.get("coin")
                levels = data.get("levels")
                timestamp = data.get("time")
                
                if not (coin and levels and timestamp):
                    return

                # levels is [[bids...], [asks...]]
                bids_data = levels[0]
                asks_data = levels[1]
                
                record = {
                    "coin": coin,
                    "channel": "l2Book",
                    "nSigFigs": n_sig_figs_context,
                    "exchange_time": timestamp,
                    "local_time": time.time(),
                    # Decompose bids/asks into separate arrays for efficiency (columnar friendly)
                    "bids_px": [l["px"] for l in bids_data],
                    "bids_sz": [l["sz"] for l in bids_data],
                    "bids_n": [l["n"] for l in bids_data],
                    "asks_px": [l["px"] for l in asks_data],
                    "asks_sz": [l["sz"] for l in asks_data],
                    "asks_n": [l["n"] for l in asks_data],
                }
                
                # Async add to writer
                await writer.add_data(record)
            elif channel == "trades":
                data = msg.get("data")
                if data:
                    for trade in data:
                        record = {
                            "coin": trade["coin"],
                            "channel": "trades",
                            "exchange_time": trade["time"],
                            "local_time": time.time(),
                            "side": trade["side"],
                            "px": trade["px"],
                            "sz": trade["sz"],
                            "hash": trade["hash"],
                            "tid": trade.get("tid"),
                            "users": json.dumps(trade.get("users", [])),
                        }
                        await writer.add_data(record)
            elif channel == "pong":
                pass
            else:
                pass
        
        def thread_callback(msg: Any):
            asyncio.run_coroutine_threadsafe(async_handler(msg), loop)
            
        return thread_callback

    # 4. Initialize WebSocket Managers
    managers = []
    
    # Clean base_url from config.API_URL which is .../info
    base_url = config.API_URL
    if base_url.endswith("/info"):
        base_url = base_url[:-5]

    for n_sig, group_subs in subs_by_sigfigs.items():
        logger.info(f"Initializing manager for nSigFigs={n_sig} with {len(group_subs)} subscriptions")
        
        ws_manager = WebsocketManager(base_url=base_url)
        callback = create_message_handler(writer, n_sig)
        
        ws_manager.start()
        managers.append(ws_manager)
        
        # Subscribe
        for sub in group_subs:
            ws_manager.subscribe(sub, callback)

    # 5. Run
    stop_event = asyncio.Event()

    def handle_signal(sig, frame):
        logger.info("Signal received, stopping...")
        stop_event.set()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    # We don't have async connection tasks anymore as managers are threads
    logger.info(f"Collector started with {len(managers)} connection(s). Press Ctrl+C to stop.")
    
    try:
        await stop_event.wait()
    finally:
        logger.info("Shutting down...")
        # Stop all managers
        for manager in managers:
            manager.stop()
            
        await writer.stop()
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass