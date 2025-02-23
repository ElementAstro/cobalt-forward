from fastapi import APIRouter, WebSocket, HTTPException, status
from fastapi.websockets import WebSocketDisconnect
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
import time
import json
import asyncio
from uuid import uuid4
from loguru import logger
from app.client.ssh.client import SSHConfig


class WebSocketStats(BaseModel):
    """WebSocket connection statistics"""
    connection_id: str = Field(..., description="Connection identifier")
    connected_at: float = Field(..., description="Connection timestamp")
    messages_sent: int = Field(default=0, description="Total messages sent")
    messages_received: int = Field(
        default=0, description="Total messages received")
    last_activity: float = Field(..., description="Last activity timestamp")


router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """Handle general WebSocket connection"""
    connection_id = str(uuid4())
    logger.info(
        f"WebSocket connection request [connection_id={connection_id}]")
    app = websocket.app

    try:
        # Set connection timeout
        await asyncio.wait_for(
            websocket.accept(),
            timeout=10.0
        )
        await app.forwarder.manager.connect(websocket)
        logger.success(
            f"WebSocket connection established [connection_id={connection_id}]")

        # Main message loop
        while True:
            try:
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=60.0
                )
                await app.forwarder.manager.handle_client_message(websocket, message)
            except asyncio.TimeoutError:
                # Send keepalive
                await websocket.send_text("")
                continue

    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected [connection_id={connection_id}]")
    except asyncio.TimeoutError:
        logger.warning(f"WebSocket timeout [connection_id={connection_id}]")
        await websocket.close(code=4000, reason="Connection timeout")
    except Exception as e:
        logger.error(
            f"WebSocket error [connection_id={connection_id}]: {str(e)}", exc_info=True)
        await websocket.close(code=4001, reason="Internal error")
    finally:
        await app.forwarder.manager.disconnect(websocket)


@router.websocket("/ws/ssh")
async def websocket_ssh_endpoint(websocket: WebSocket):
    """Handle SSH WebSocket terminal connection"""
    connection_id = str(uuid4())
    logger.info(
        f"SSH WebSocket connection request [connection_id={connection_id}]")
    session_id: Optional[str] = None

    try:
        # Accept connection with timeout
        await asyncio.wait_for(
            websocket.accept(),
            timeout=10.0
        )
        app = websocket.app

        # Wait for SSH configuration
        try:
            config_data = await asyncio.wait_for(
                websocket.receive_json(),
                timeout=15.0
            )
            ssh_config = SSHConfig(**config_data)
        except (json.JSONDecodeError, ValueError) as e:
            logger.error(f"Invalid SSH configuration: {str(e)}")
            await websocket.close(code=4002, reason="Invalid configuration")
            return

        # Create SSH session
        try:
            session_id = await app.ssh_forwarder.create_session(websocket, ssh_config)
            logger.success(
                f"SSH session created [connection_id={connection_id}] [session_id={session_id}]")
        except Exception as e:
            logger.error(f"Failed to create SSH session: {str(e)}")
            await websocket.close(code=4003, reason="Connection failed")
            return

        # Main message loop with keepalive
        while True:
            try:
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=60.0
                )
                await app.ssh_forwarder.handle_message(session_id, message)
            except asyncio.TimeoutError:
                # Send keepalive
                await websocket.send_text("")
                continue

    except WebSocketDisconnect:
        logger.info(
            f"SSH WebSocket disconnected [connection_id={connection_id}]")
    except asyncio.TimeoutError:
        logger.warning(
            f"SSH WebSocket timeout [connection_id={connection_id}]")
        await websocket.close(code=4004, reason="Connection timeout")
    except Exception as e:
        logger.error(
            f"SSH WebSocket error [connection_id={connection_id}]: {str(e)}", exc_info=True)
        await websocket.close(code=4005, reason="Internal error")
    finally:
        if session_id:
            try:
                await app.ssh_forwarder.close_session(session_id)
                logger.info(f"SSH session closed [session_id={session_id}]")
            except Exception as e:
                logger.error(f"Error closing SSH session: {str(e)}")
