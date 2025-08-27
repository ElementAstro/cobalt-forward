"""
WebSocket API endpoints and connection management.

This module provides WebSocket endpoints for real-time communication
and message forwarding capabilities.
"""

import json
import logging
from typing import Any, Dict, Set, List, Optional, Union

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from ....application.container import IContainer
from ....core.interfaces.messaging import IEventBus, IMessageBus
from ....core.domain.events import Event, EventPriority
from ....core.domain.messages import Message, MessageType
from ..dependencies import get_container

router = APIRouter()
logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages WebSocket connections and message broadcasting."""

    def __init__(self) -> None:
        self.active_connections: Set[WebSocket] = set()
        self.connection_metadata: Dict[WebSocket, Dict[str, Any]] = {}

    async def connect(self, websocket: WebSocket, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Accept a WebSocket connection."""
        await websocket.accept()
        self.active_connections.add(websocket)
        self.connection_metadata[websocket] = metadata or {}
        logger.info(
            f"WebSocket connection established. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection."""
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            self.connection_metadata.pop(websocket, None)
            logger.info(
                f"WebSocket connection closed. Total connections: {len(self.active_connections)}")

    async def send_personal_message(self, message: str, websocket: WebSocket) -> None:
        """Send a message to a specific WebSocket connection."""
        try:
            await websocket.send_text(message)
        except Exception as e:
            logger.error(f"Failed to send message to WebSocket: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: str) -> None:
        """Broadcast a message to all connected WebSocket clients."""
        disconnected = set()

        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                logger.error(f"Failed to broadcast to WebSocket: {e}")
                disconnected.add(connection)

        # Clean up disconnected connections
        for connection in disconnected:
            self.disconnect(connection)

    async def broadcast_json(self, data: Dict[str, Any]) -> None:
        """Broadcast JSON data to all connected clients."""
        message = json.dumps(data)
        await self.broadcast(message)

    def get_connection_count(self) -> int:
        """Get the number of active connections."""
        return len(self.active_connections)


# Global connection manager instance
connection_manager = ConnectionManager()


@router.websocket("/connect")
async def websocket_endpoint(
    websocket: WebSocket,
    container: IContainer = Depends(get_container)
) -> None:
    """
    Main WebSocket endpoint for client connections.

    Handles WebSocket connections and message routing.
    """
    # Get services from container
    try:
        event_bus = container.resolve(IEventBus)  # type: ignore[type-abstract]
        # type: ignore[type-abstract]
        message_bus = container.resolve(IMessageBus)
    except Exception as e:
        logger.error(f"Failed to resolve services: {e}")
        await websocket.close(code=1011, reason="Service unavailable")
        return

    # Accept connection
    client_info = {
        "client_ip": websocket.client.host if websocket.client else "unknown",
        "user_agent": websocket.headers.get("user-agent", "unknown")
    }

    await connection_manager.connect(websocket, client_info)

    # Publish connection event
    await event_bus.publish(
        Event(
            name="websocket.client.connected",
            data=client_info,
            priority=EventPriority.NORMAL
        )
    )

    try:
        while True:
            # Receive message from client
            data = await websocket.receive_text()

            try:
                # Parse JSON message
                message_data = json.loads(data)
                await handle_client_message(message_data, websocket, event_bus, message_bus)

            except json.JSONDecodeError:
                # Handle plain text message
                await handle_text_message(data, websocket, event_bus)

            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}")
                await websocket.send_json({
                    "type": "error",
                    "message": "Failed to process message",
                    "error": str(e)
                })

    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)

        # Publish disconnection event
        await event_bus.publish(
            Event(
                name="websocket.client.disconnected",
                data=client_info,
                priority=EventPriority.NORMAL
            )
        )

    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        connection_manager.disconnect(websocket)


async def handle_client_message(
    message_data: Dict[str, Any],
    websocket: WebSocket,
    event_bus: IEventBus,
    message_bus: IMessageBus
) -> None:
    """
    Handle structured JSON messages from clients.

    Args:
        message_data: Parsed JSON message data
        websocket: WebSocket connection
        event_bus: Event bus instance
        message_bus: Message bus instance
    """
    message_type = message_data.get("type", "unknown")

    if message_type == "ping":
        # Handle ping/pong for keepalive
        await websocket.send_json({"type": "pong", "timestamp": message_data.get("timestamp")})

    elif message_type == "subscribe":
        # Handle event subscription
        topic = message_data.get("topic")
        if topic:
            # In a real implementation, you would manage subscriptions per connection
            await websocket.send_json({
                "type": "subscribed",
                "topic": topic,
                "status": "success"
            })

    elif message_type == "message":
        # Handle message forwarding
        topic = message_data.get("topic", "websocket.inbound")
        payload = message_data.get("data", {})

        # Create message for message bus
        message = Message(
            topic=topic,
            data=payload,
            message_type=MessageType.DATA,
            source="websocket",
            headers={"websocket_id": id(websocket)}
        )

        # Send to message bus
        await message_bus.send(message)

        # Acknowledge receipt
        await websocket.send_json({
            "type": "ack",
            "message_id": message.message_id,
            "status": "sent"
        })

    elif message_type == "command":
        # Handle command execution
        command_data = message_data.get("command", {})

        # Publish command event
        await event_bus.publish(
            Event(
                name="websocket.command.received",
                data={
                    "command": command_data,
                    "websocket_id": id(websocket)
                },
                priority=EventPriority.HIGH
            )
        )

        # For now, just acknowledge
        await websocket.send_json({
            "type": "command_ack",
            "command": command_data.get("name", "unknown"),
            "status": "received"
        })

    else:
        # Unknown message type
        await websocket.send_json({
            "type": "error",
            "message": f"Unknown message type: {message_type}"
        })


async def handle_text_message(
    text: str,
    websocket: WebSocket,
    event_bus: IEventBus
) -> None:
    """
    Handle plain text messages from clients.

    Args:
        text: Plain text message
        websocket: WebSocket connection
        event_bus: Event bus instance
    """
    # Publish text message event
    await event_bus.publish(
        Event(
            name="websocket.text.received",
            data={
                "text": text,
                "websocket_id": id(websocket)
            },
            priority=EventPriority.NORMAL
        )
    )

    # Echo the message back (for testing)
    await websocket.send_json({
        "type": "echo",
        "message": text,
        "timestamp": "2024-01-01T00:00:00Z"  # Placeholder
    })


@router.get("/connections")
async def get_connection_info() -> Dict[str, Any]:
    """
    Get information about active WebSocket connections.

    Returns connection count and basic statistics.
    """
    return {
        "active_connections": connection_manager.get_connection_count(),
        "connection_details": [
            {
                "id": id(conn),
                "metadata": connection_manager.connection_metadata.get(conn, {})
            }
            for conn in connection_manager.active_connections
        ]
    }


@router.post("/broadcast")
async def broadcast_message(message: Dict[str, Any]) -> Dict[str, Union[str, int]]:
    """
    Broadcast a message to all connected WebSocket clients.

    Args:
        message: Message data to broadcast
    """
    await connection_manager.broadcast_json({
        "type": "broadcast",
        "data": message,
        "timestamp": "2024-01-01T00:00:00Z"  # Placeholder
    })

    return {
        "status": "sent",
        "recipients": connection_manager.get_connection_count()
    }
