import os

# Binance Spot WebSocket URL
WS_URL = "wss://stream.binance.com:9443/ws"

# Target symbols (should be uppercase for internal logic, converted to lowercase for stream)
# Using USDT pairs as proxies for the "BTC", "ETH", "SOL" interest
TARGET_COINS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

DATA_DIR = os.getenv("DATA_DIR", "./data")
FLUSH_INTERVAL = 120
BATCH_SIZE = 5000

# Configuration for subscription
# Binance depth update speed (100ms or 1000ms)
DEPTH_SPEED = "100ms"
# Diff Depth stream does not use levels (it pushes all updates)
# DEPTH_LEVELS = 20
