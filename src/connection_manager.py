from typing import Dict, Set, List, Any
from fastapi import WebSocket
import json
from decimal import Decimal
from logger import LoggerSetup

class ConnectionManager:
    """
    Manages WebSocket connections and topic subscriptions.
    Singleton pattern usage recommended via Dependency Injection.
    """
    def __init__(self):
        # socket -> set of subscribed topics
        self.active_connections: Dict[WebSocket, Set[str]] = {}
        # topic -> list of sockets
        self.topic_subscribers: Dict[str, List[WebSocket]] = {}
        self.logger = LoggerSetup.get_logger("ConnectionManager")

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections[websocket] = set()
        self.logger.info(f"New WebSocket connection accepted. Total active: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.logger.info("WebSocket disconnected.")
            # Clean up topic subscriptions
            user_topics = self.active_connections[websocket]
            for topic in user_topics:
                if topic in self.topic_subscribers:
                    if websocket in self.topic_subscribers[topic]:
                        self.topic_subscribers[topic].remove(websocket)
            del self.active_connections[websocket]

    async def subscribe(self, websocket: WebSocket, topic: str):
        if websocket not in self.active_connections:
            return
            
        self.active_connections[websocket].add(topic)
        if topic not in self.topic_subscribers:
            self.topic_subscribers[topic] = []
        if websocket not in self.topic_subscribers[topic]:
            self.topic_subscribers[topic].append(websocket)
            self.logger.info(f"Subscribed connection to topic: {topic}")

    async def unsubscribe(self, websocket: WebSocket, topic: str):
        if websocket in self.active_connections:
            if topic in self.active_connections[websocket]:
                self.active_connections[websocket].remove(topic)
        
        if topic in self.topic_subscribers:
            if websocket in self.topic_subscribers[topic]:
                self.topic_subscribers[topic].remove(websocket)
                self.logger.info(f"Unsubscribed connection from topic: {topic}")

    def _json_encoder(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

    async def broadcast(self, topic: str, message: dict, msg_type: str = "update"):
        """
        Sends a message to all clients subscribed to a specific topic.
        Also broadcast to 'wildcard' subscribers if we implement them later.
        """
        if topic in self.topic_subscribers:
            # Wrap data in standard envelope
            envelope = {
                "type": msg_type,
                "topic": topic,
                "data": message
            }
            # Serialize once
            try:
                payload = json.dumps(envelope, default=self._json_encoder)
            except Exception as e:
                self.logger.error(f"WS Serialization Error: {e}")
                return

            subscriber_count = len(self.topic_subscribers[topic])
            self.logger.debug(f"Broadcast to {subscriber_count} subscribers on topic '{topic}': {message}")

            # Snapshot list to avoid modification during iteration issues
            for connection in list(self.topic_subscribers[topic]):
                try:
                    await connection.send_text(payload)
                except Exception:
                    # Connection probably dead, remove it
                    self.disconnect(connection)

    async def broadcast_to_all(self, message: dict):
        subscriber_count = len(self.active_connections)
        self.logger.info(f"Broadcast to all {subscriber_count} connections: {message}")
        for connection in list(self.active_connections.keys()):
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)
