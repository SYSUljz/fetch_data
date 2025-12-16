import json
import time
import logging
import threading
from collections import defaultdict

import websocket
from websocket import WebSocketConnectionClosedException

from hyperliquid.utils.types import Any, Callable, Dict, List, NamedTuple, Optional, Subscription, Tuple, WsMsg

ActiveSubscription = NamedTuple("ActiveSubscription", [("callback", Callable[[Any], None]), ("subscription_id", int)])

# --- 保持原有的 identifier 转换函数不变 ---
def subscription_to_identifier(subscription: Subscription) -> str:
    # ... (与原代码一致，为了节省篇幅省略) ...
    if subscription["type"] == "allMids":
        return "allMids"
    elif subscription["type"] == "l2Book":
        return f'l2Book:{subscription["coin"].lower()}'
    elif subscription["type"] == "trades":
        return f'trades:{subscription["coin"].lower()}'
    elif subscription["type"] == "userEvents":
        return "userEvents"
    elif subscription["type"] == "userFills":
        return f'userFills:{subscription["user"].lower()}'
    elif subscription["type"] == "candle":
        return f'candle:{subscription["coin"].lower()},{subscription["interval"]}'
    elif subscription["type"] == "orderUpdates":
        return "orderUpdates"
    elif subscription["type"] == "userFundings":
        return f'userFundings:{subscription["user"].lower()}'
    elif subscription["type"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{subscription["user"].lower()}'
    elif subscription["type"] == "webData2":
        return f'webData2:{subscription["user"].lower()}'
    elif subscription["type"] == "bbo":
        return f'bbo:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetCtx":
        return f'activeAssetCtx:{subscription["coin"].lower()}'
    elif subscription["type"] == "activeAssetData":
        return f'activeAssetData:{subscription["coin"].lower()},{subscription["user"].lower()}'

def ws_msg_to_identifier(ws_msg: WsMsg) -> Optional[str]:
    # ... (与原代码一致，为了节省篇幅省略) ...
    if ws_msg["channel"] == "pong":
        return "pong"
    elif ws_msg["channel"] == "allMids":
        return "allMids"
    elif ws_msg["channel"] == "l2Book":
        return f'l2Book:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "trades":
        trades = ws_msg["data"]
        if len(trades) == 0:
            return None
        else:
            return f'trades:{trades[0]["coin"].lower()}'
    elif ws_msg["channel"] == "user":
        return "userEvents"
    elif ws_msg["channel"] == "userFills":
        return f'userFills:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "candle":
        return f'candle:{ws_msg["data"]["s"].lower()},{ws_msg["data"]["i"]}'
    elif ws_msg["channel"] == "orderUpdates":
        return "orderUpdates"
    elif ws_msg["channel"] == "userFundings":
        return f'userFundings:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "userNonFundingLedgerUpdates":
        return f'userNonFundingLedgerUpdates:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "webData2":
        return f'webData2:{ws_msg["data"]["user"].lower()}'
    elif ws_msg["channel"] == "bbo":
        return f'bbo:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetCtx" or ws_msg["channel"] == "activeSpotAssetCtx":
        return f'activeAssetCtx:{ws_msg["data"]["coin"].lower()}'
    elif ws_msg["channel"] == "activeAssetData":
        return f'activeAssetData:{ws_msg["data"]["coin"].lower()},{ws_msg["data"]["user"].lower()}'

class WebsocketManager(threading.Thread):
    def __init__(self, base_url):
        super().__init__()
        self.subscription_id_counter = 0
        self.ws_ready = False
        self.subscriptions: List[Tuple[Subscription, int]] = []
        self.active_subscriptions: Dict[str, List[ActiveSubscription]] = defaultdict(list)
        
        # 增加锁，保证多线程操作 active_subscriptions 时的安全
        self.lock = threading.Lock()
        
        self.base_url = base_url
        self.ws_url = "ws" + base_url[len("http") :] + "/ws"
        self.ws = None # 延迟到 run 中初始化
        
        self.ping_sender = None
        self.stop_event = threading.Event()
        
        # 指数退避参数
        self.initial_reconnect_delay = 1
        self.max_reconnect_delay = 60
        self.current_reconnect_delay = self.initial_reconnect_delay

    def run(self):
        while not self.stop_event.is_set():
            # 1. 启动 Ping 线程 (如果还没启动)
            if self.ping_sender is None or not self.ping_sender.is_alive():
                self.ping_sender = threading.Thread(target=self.send_ping)
                self.ping_sender.daemon = True # 设置为守护线程，主程序退出时自动结束
                self.ping_sender.start()
            
            logging.info(f"Connecting to websocket: {self.ws_url}...")
            
            # 2. 每次重连创建新的 WebSocketApp 实例，防止脏状态
            self.ws = websocket.WebSocketApp(
                self.ws_url, 
                on_message=self.on_message, 
                on_open=self.on_open,
                on_error=self.on_error, # 建议加上 error 处理
                on_close=self.on_close
            )
            
            # 3. 阻塞运行
            # ping_interval=0 禁用库自带的ping，因为我们有自定义的send_ping线程
            self.ws.run_forever()
            
            # 4. 连接断开后的处理
            self.ws_ready = False
            
            if not self.stop_event.is_set():
                with self.lock:
                    subs = [s[0] for s in self.subscriptions]
                logging.warning(f"Websocket connection lost. Reconnecting in {self.current_reconnect_delay}s... subscriptions: {subs}")
                time.sleep(self.current_reconnect_delay)
                
                # 5. 指数退避计算 (1 -> 2 -> 4 -> ... -> 60)
                self.current_reconnect_delay = min(self.max_reconnect_delay, self.current_reconnect_delay * 2)

    def send_ping(self):
        """修复后的 Ping 逻辑：增加异常捕获"""
        while not self.stop_event.wait(50): # 等待50秒，如果 stop_event 被 set 则立即退出
            if not self.ws or not self.ws.keep_running:
                continue # 连接没建立好，跳过
            
            try:
                logging.debug("Websocket sending ping")
                # 增加 sock 检查
                if self.ws.sock and self.ws.sock.connected:
                    self.ws.send(json.dumps({"method": "ping"}))
                else:
                    logging.debug("Skipping ping: Socket not connected")
            except (WebSocketConnectionClosedException, BrokenPipeError, ConnectionResetError) as e:
                logging.warning(f"Failed to send ping (connection likely closed): {e}")
                # 不需要在这里重连，run() 循环会处理
            except Exception as e:
                logging.error(f"Unexpected error in send_ping: {e}")

        logging.debug("Websocket ping sender stopped")

    def stop(self):
        self.stop_event.set()
        if self.ws:
            self.ws.close()
        if self.ping_sender and self.ping_sender.is_alive():
            self.ping_sender.join(timeout=5) # 加上 timeout 防止死锁

    def on_open(self, _ws):
        logging.debug("on_open: Connection established")
        self.ws_ready = True
        
        # 重置重连延迟时间，因为连接成功了
        self.current_reconnect_delay = self.initial_reconnect_delay
        
        with self.lock: # 加锁读取
            for subscription, _ in self.subscriptions:
                logging.debug(f"Resubscribing: {subscription}")
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))

    def on_message(self, _ws, message):
        if message == "Websocket connection established.":
            logging.debug(message)
            return
        
        # logging.debug(f"on_message {message}") # 过于啰嗦，建议注释掉或仅在 DEBUG 极深层级开启
        
        try:
            ws_msg: WsMsg = json.loads(message)
        except json.JSONDecodeError:
            logging.error(f"Failed to decode JSON: {message}")
            return

        identifier = ws_msg_to_identifier(ws_msg)
        if identifier == "pong":
            logging.debug("Websocket received pong")
            return
        if identifier is None:
            logging.debug("Websocket not handling empty message")
            return
        
        with self.lock: # 加锁读取
            active_subscriptions = self.active_subscriptions[identifier]
            # 为了线程安全，复制一份列表进行回调，防止回调执行期间列表被修改
            callbacks_to_run = list(active_subscriptions)

        if len(callbacks_to_run) == 0:
            logging.warning(f"Websocket message from an unexpected subscription: {identifier}")
        else:
            for active_subscription in callbacks_to_run:
                try:
                    active_subscription.callback(ws_msg)
                except Exception as e:
                    logging.error(f"Error in subscription callback: {e}", exc_info=True)

    def on_error(self, _ws, error):
        with self.lock:
            subs = [s[0] for s in self.subscriptions]
        logging.error(f"Websocket error: {error}. subscriptions: {subs}")

    def on_close(self, _ws, close_status_code, close_msg):
        logging.info(f"Websocket closed. Code: {close_status_code}, Msg: {close_msg}")

    def _safe_send(self, data):
        """辅助函数：安全发送数据"""
        try:
            if self.ws and self.ws.sock and self.ws.sock.connected:
                self.ws.send(data)
            else:
                logging.warning("Attempted to send message while socket is closed.")
        except Exception as e:
            logging.error(f"Failed to send message: {e}")

    def subscribe(
        self, subscription: Subscription, callback: Callable[[Any], None], subscription_id: Optional[int] = None
    ) -> int:
        with self.lock: # 加锁修改
            if subscription_id is None:
                self.subscription_id_counter += 1
                subscription_id = self.subscription_id_counter
            
            self.subscriptions.append((subscription, subscription_id))
            
            identifier = subscription_to_identifier(subscription)
            if identifier == "userEvents" or identifier == "orderUpdates":
                if len(self.active_subscriptions[identifier]) != 0:
                    raise NotImplementedError(f"Cannot subscribe to {identifier} multiple times")
            
            self.active_subscriptions[identifier].append(ActiveSubscription(callback, subscription_id))
            
            if self.ws_ready:
                logging.debug("subscribing")
                self._safe_send(json.dumps({"method": "subscribe", "subscription": subscription}))
            else:
                logging.debug("queueing subscription (ws not ready)")
                
            return subscription_id

    def unsubscribe(self, subscription: Subscription, subscription_id: int) -> bool:
        with self.lock: # 加锁修改
            self.subscriptions = [s for s in self.subscriptions if s[1] != subscription_id]

            identifier = subscription_to_identifier(subscription)
            active_subscriptions = self.active_subscriptions[identifier]
            new_active_subscriptions = [x for x in active_subscriptions if x.subscription_id != subscription_id]
            
            if len(new_active_subscriptions) == 0 and self.ws_ready:
                self._safe_send(json.dumps({"method": "unsubscribe", "subscription": subscription}))
            
            self.active_subscriptions[identifier] = new_active_subscriptions
            return len(active_subscriptions) != len(new_active_subscriptions)