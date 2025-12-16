# Hyperliquid LOB Data Collector

This project collects Level 2 Order Book (L2Book) data for BTC, ETH, and SOL (both Perpetual and Spot markets) from Hyperliquid's WebSocket API and saves it to Parquet files.

## Features
- Real-time L2Book monitoring.
- Robust WebSocket connection with automatic reconnection and heartbeats.
- Efficient data storage using Parquet (batched writes, snappy compression).
- Handles network jitter/disconnections gracefully.

## Installation

1. Ensure you have Python 3.9+ installed.
2. Install dependencies:

```bash
cd fetch_data
pip install -e .
```

## Usage

Run the collector:

```bash
python -m lob_research.main
```

## Configuration

Configuration variables are available in `src/lob_research/config.py`.
- `TARGET_COINS`: List of coins to monitor (default: BTC, ETH, SOL).
- `DATA_DIR`: Output directory for Parquet files.
- `FLUSH_INTERVAL`: Time in seconds between disk writes.

## Output

Data is saved to `data/{YYYY-MM-DD}/l2book_{timestamp}.parquet`.
Columns:
- `coin`: Token symbol (e.g., "BTC").
- `channel`: "l2Book".
- `exchange_time`: Timestamp from the exchange.
- `local_time`: Local receipt timestamp.
- `bids`: List of bid levels `[{px, sz, n}, ...]`.
- `asks`: List of ask levels.
