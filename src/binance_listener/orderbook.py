from typing import Dict, List, Any, Optional
import logging

logger = logging.getLogger("binance_listener.orderbook")

class GapDetectedError(Exception):
    """Raised when a sequence gap is detected in the order book stream."""
    pass

class LocalOrderBook:
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.bids: Dict[float, float] = {}
        self.asks: Dict[float, float] = {}
        self.last_update_id = 0
        self.initialized = False
        self.synced = False

    def set_snapshot(self, snapshot_data: Dict[str, Any]):
        """
        Initialize the order book with a REST snapshot.
        snapshot_data: The JSON response from GET /api/v3/depth
        """
        self.last_update_id = snapshot_data["lastUpdateId"]
        
        # Clear existing
        self.bids.clear()
        self.asks.clear()

        # Populate (Price, Qty)
        for price_str, qty_str in snapshot_data.get("bids", []):
            self.bids[float(price_str)] = float(qty_str)
            
        for price_str, qty_str in snapshot_data.get("asks", []):
            self.asks[float(price_str)] = float(qty_str)
            
        self.initialized = True
        self.synced = False
        logger.info(f"[{self.symbol}] Order book initialized. LastUpdateId: {self.last_update_id}")

    def apply_update(self, update_data: Dict[str, Any]) -> bool:
        """
        Apply a depthUpdate event.
        Returns True if applied, False if ignored (stale).
        Raises GapDetectedError if a sequence gap is detected.
        """
        if not self.initialized:
            # Buffer or ignore if handled externally
            return False

        u = update_data["u"] # Final update ID
        U = update_data["U"] # First update ID
        pu = update_data.get("pu") # Previous update ID

        # 1. Drop any event where u is <= lastUpdateId
        if u <= self.last_update_id:
            return False

        # 2. Sequence Check
        if not self.synced:
            # First event after snapshot: expect overlap
            # "The first processed event should have U <= lastUpdateId+1 AND u >= lastUpdateId+1"
            if U <= self.last_update_id + 1 <= u:
                self.synced = True
            else:
                if U > self.last_update_id + 1:
                    raise GapDetectedError(f"Initial sync gap: U={U} > last={self.last_update_id}+1")
                # If U <= last_update_id but u < last_update_id, it's stale (caught by step 1).
                # But if U > last + 1, it's a gap.
                return False
        else:
            # Steady state: Check continuity
            # If 'pu' is available, check it strictly against last_update_id
            if pu is not None:
                if pu != self.last_update_id:
                    raise GapDetectedError(f"Gap detected: pu={pu} != last_update_id={self.last_update_id}")
            else:
                # Fallback if pu is missing (though it should be there for depth streams)
                if U != self.last_update_id + 1:
                    raise GapDetectedError(f"Gap detected (no pu): U={U} != last_update_id+1 ({self.last_update_id + 1})")

        # Apply updates
        for price_str, qty_str in update_data.get("b", []):
            self._update_level(self.bids, float(price_str), float(qty_str))
            
        for price_str, qty_str in update_data.get("a", []):
            self._update_level(self.asks, float(price_str), float(qty_str))

        self.last_update_id = u
        return True

    def _update_level(self, side_dict: Dict[float, float], price: float, qty: float):
        if qty == 0.0:
            if price in side_dict:
                del side_dict[price]
        else:
            side_dict[price] = qty

    def get_snapshot_dict(self) -> Dict[str, Any]:
        """Returns the current state in a format similar to the REST snapshot"""
        # Sort bids descending, asks ascending
        sorted_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)
        sorted_asks = sorted(self.asks.items(), key=lambda x: x[0])
        
        return {
            "lastUpdateId": self.last_update_id,
            "bids": [[str(p), str(q)] for p, q in sorted_bids],
            "asks": [[str(p), str(q)] for p, q in sorted_asks]
        }

    def get_best_prices(self) -> Dict[str, float | None]:
        """Returns the best bid and ask prices."""
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        return {"best_bid": best_bid, "best_ask": best_ask, "lastUpdateId": self.last_update_id}
