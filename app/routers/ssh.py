from fastapi import APIRouter, WebSocket, HTTPException, status, Depends, Body
from fastapi.websockets import WebSocketDisconnect
from app.client.ssh.client import SSHConfig
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


class TerminalResizeRequest(BaseModel):
    """Terminal resize request model"""
    rows: int = Field(..., gt=0, lt=1000, description="Terminal rows")
    cols: int = Field(..., gt=0, lt=1000, description="Terminal columns")


# Dependency injection functions
def get_app():
    """Get the FastAPI application instance"""
    from fastapi import Request

    def _get_app(request: Request):
        return request.app
    return Depends(_get_app)


def get_ssh_forwarder():
    """Get SSH forwarder from app state"""
    def _get_ssh_forwarder(app=get_app()):
        return app.state.ssh_forwarder
    return Depends(_get_ssh_forwarder)


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
    session_id: Optional[str] = None
    logger.info(
        f"New SSH WebSocket connection request [connection_id={connection_id}]")

    try:
        await websocket.accept()
        ssh_forwarder = websocket.app.state.ssh_forwarder

        # Wait for initial configuration with timeout
        try:
            config_data = await asyncio.wait_for(
                websocket.receive_json(),
                timeout=10.0
            )
            ssh_config = SSHConfig(**config_data)
            logger.info(
                f"SSH configuration received [connection_id={connection_id}]")
        except asyncio.TimeoutError:
            logger.error(
                f"Configuration timeout [connection_id={connection_id}]")
            await websocket.close(code=4000, reason="Configuration timeout")
            return
        except Exception as e:
            logger.error(
                f"Invalid SSH configuration [connection_id={connection_id}]: {str(e)}")
            await websocket.close(code=4000, reason="Invalid configuration")
            return

        # Create SSH session
        try:
            session_id = await ssh_forwarder.create_session(websocket, ssh_config)
            logger.info(
                f"SSH session created [session_id={session_id}] [connection_id={connection_id}]")
        except Exception as e:
            logger.error(
                f"Failed to create SSH session [connection_id={connection_id}]: {str(e)}")
            await websocket.close(code=4001, reason="Connection failed")
            return

        # Main message loop
        while True:
            try:
                message = await asyncio.wait_for(
                    websocket.receive_text(),
                    timeout=60.0
                )
                await ssh_forwarder.handle_message(session_id, message)
            except asyncio.TimeoutError:
                # Send keepalive
                try:
                    await websocket.send_text("")
                except:
                    break
                continue
            except WebSocketDisconnect:
                logger.info(
                    f"WebSocket disconnected [session_id={session_id}]")
                break
            except Exception as e:
                logger.error(
                    f"Error handling message [session_id={session_id}]: {str(e)}")
                break

    except WebSocketDisconnect:
        logger.info(
            f"SSH WebSocket disconnected [connection_id={connection_id}]")
    except Exception as e:
        logger.exception(
            f"SSH WebSocket error [connection_id={connection_id}]: {str(e)}")
        try:
            await websocket.close(code=4003, reason="Internal error")
        except:
            pass
    finally:
        # Cleanup session if it was created
        if session_id:
            try:
                ssh_forwarder = websocket.app.state.ssh_forwarder
                await ssh_forwarder.close_session(session_id)
                logger.info(f"SSH session closed [session_id={session_id}]")
            except Exception as e:
                logger.error(
                    f"Error closing SSH session [session_id={session_id}]: {str(e)}")


@router.get("/sessions/{session_id}/stats", response_model=SSHSessionStats)
async def get_session_stats(
    session_id: str,
    ssh_forwarder=get_ssh_forwarder()
):
    """Get SSH session statistics"""
    try:
        stats = ssh_forwarder.get_session_stats(session_id)

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
        logger.error(
            f"Failed to get session stats [session_id={session_id}]: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve session statistics"
        )


@router.post("/sessions/{session_id}/resize")
async def resize_terminal(
    session_id: str,
    resize_request: TerminalResizeRequest,
    ssh_forwarder=get_ssh_forwarder()
):
    """Resize SSH terminal window"""
    try:
        await ssh_forwarder.resize_terminal(
            session_id,
            resize_request.rows,
            resize_request.cols
        )

        logger.info(
            f"Terminal resized [session_id={session_id}] [rows={resize_request.rows}] [cols={resize_request.cols}]")
        return {
            "status": "success",
            "session_id": session_id,
            "rows": resize_request.rows,
            "cols": resize_request.cols
        }
    except Exception as e:
        logger.error(
            f"Failed to resize terminal [session_id={session_id}]: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to resize terminal"
        )


@router.get("/sessions")
async def list_ssh_sessions(
    ssh_forwarder=get_ssh_forwarder()
):
    """List all active SSH sessions"""
    try:
        if hasattr(ssh_forwarder, 'list_sessions'):
            sessions = ssh_forwarder.list_sessions()
        else:
            # Fallback if method doesn't exist
            sessions = []

        return {
            "sessions": sessions,
            "count": len(sessions)
        }
    except Exception as e:
        logger.error(f"Failed to list SSH sessions: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to list SSH sessions"
        )


@router.delete("/sessions/{session_id}")
async def close_ssh_session(
    session_id: str,
    ssh_forwarder=get_ssh_forwarder()
):
    """Close SSH session"""
    try:
        await ssh_forwarder.close_session(session_id)
        logger.info(f"SSH session closed via API [session_id={session_id}]")
        return {
            "status": "success",
            "message": f"Session {session_id} closed successfully"
        }
    except Exception as e:
        logger.error(
            f"Failed to close SSH session [session_id={session_id}]: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to close SSH session"
        )


@router.post("/sessions/{session_id}/command")
async def send_command(
    session_id: str,
    command: str = Body(..., embed=True),
    ssh_forwarder=get_ssh_forwarder()
):
    """Send command to SSH session"""
    try:
        if hasattr(ssh_forwarder, 'send_command'):
            result = await ssh_forwarder.send_command(session_id, command)
        else:
            # Fallback using handle_message
            result = await ssh_forwarder.handle_message(session_id, command)

        logger.info(f"Command sent to SSH session [session_id={session_id}]")
        return {
            "status": "success",
            "command": command,
            "result": result
        }
    except Exception as e:
        logger.error(
            f"Failed to send command [session_id={session_id}]: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to send command"
        )
