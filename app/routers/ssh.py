from fastapi import APIRouter, WebSocket, HTTPException, status
from fastapi.websockets import WebSocketDisconnect
from app.client.ssh.client import SSHConfig
from app.core.exceptions import SSHError
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field
from loguru import logger
import asyncio
from uuid import uuid4


class SSHSessionStats(BaseModel):
    """SSH session statistics model"""
    session_id: str = Field(..., description="Session identifier")
    connected_at: float = Field(..., description="Connection timestamp")
    bytes_sent: int = Field(default=0, description="Total bytes sent")
    bytes_received: int = Field(default=0, description="Total bytes received")
    last_activity: float = Field(..., description="Last activity timestamp")
    client_info: Dict[str, Any] = Field(default_factory=dict)


router = APIRouter(
    prefix="/api/ssh",
    tags=["ssh"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Session not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.websocket("/terminal")
async def websocket_ssh_endpoint(websocket: WebSocket):
    """Handle SSH WebSocket terminal connection"""
    connection_id = str(uuid4())
    logger.info(
        f"New SSH WebSocket connection request [connection_id={connection_id}]")

    try:
        await websocket.accept()
        app = websocket.app
        session_id: Optional[str] = None

        # Set connection timeout
        await asyncio.wait_for(
            websocket.receive_json(),
            timeout=10.0
        )

        # Validate and create SSH config
        try:
            config_data = await websocket.receive_json()
            ssh_config = SSHConfig(**config_data)
        except Exception as e:
            logger.error(f"Invalid SSH configuration: {str(e)}")
            await websocket.close(code=4000, reason="Invalid configuration")
            return

        # Create SSH session
        try:
            session_id = await app.ssh_forwarder.create_session(websocket, ssh_config)
            logger.info(f"SSH session created [session_id={session_id}]")
        except SSHError as e:
            logger.error(f"Failed to create SSH session: {str(e)}")
            await websocket.close(code=4001, reason="Connection failed")
            return

        # Main message loop
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
        await websocket.close(code=4002, reason="Connection timeout")
    except Exception as e:
        logger.exception(
            f"SSH WebSocket error [connection_id={connection_id}]: {str(e)}")
        await websocket.close(code=4003, reason="Internal error")
    finally:
        if session_id:
            try:
                await app.ssh_forwarder.close_session(session_id)
                logger.info(f"SSH session closed [session_id={session_id}]")
            except Exception as e:
                logger.error(f"Error closing SSH session: {str(e)}")


@router.get("/sessions/{session_id}/stats", response_model=SSHSessionStats)
async def get_session_stats(session_id: str):
    """Get SSH session statistics"""
    try:
        app = router.app
        stats = app.ssh_forwarder.get_session_stats(session_id)

        if not stats:
            logger.warning(f"Session not found [session_id={session_id}]")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Session not found"
            )

        return SSHSessionStats(**stats)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get session stats: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve session statistics"
        )


@router.post("/sessions/{session_id}/resize")
async def resize_terminal(
    session_id: str,
    rows: int = Field(..., gt=0, lt=1000),
    cols: int = Field(..., gt=0, lt=1000)
):
    """Resize SSH terminal window"""
    try:
        app = router.app
        await app.ssh_forwarder.resize_terminal(session_id, rows, cols)
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Failed to resize terminal: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to resize terminal"
        )
