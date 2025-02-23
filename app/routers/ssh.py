from fastapi import APIRouter, WebSocket, HTTPException
from fastapi.websockets import WebSocketDisconnect
from app.client.ssh.client import SSHConfig
from loguru import logger

router = APIRouter(prefix="/api/ssh", tags=["ssh"])

@router.websocket("/terminal")
async def websocket_ssh_endpoint(websocket: WebSocket):
    """SSH WebSocket终端连接处理"""
    connection_id = id(websocket)
    logger.info(f"SSH WebSocket连接请求 [connection_id={connection_id}]")

    await websocket.accept()
    app = websocket.app
    session_id = None

    try:
        config_data = await websocket.receive_json()
        ssh_config = SSHConfig(**config_data)
        session_id = await app.ssh_forwarder.create_session(websocket, ssh_config)
        
        while True:
            message = await websocket.receive_text()
            await app.ssh_forwarder.handle_message(session_id, message)

    except WebSocketDisconnect:
        logger.info(f"SSH WebSocket连接断开 [connection_id={connection_id}]")
        if session_id:
            await app.ssh_forwarder.close_session(session_id)
    except Exception as e:
        logger.exception(f"SSH WebSocket错误 [connection_id={connection_id}]: {str(e)}")
        raise
    finally:
        if session_id:
            await app.ssh_forwarder.close_session(session_id)

@router.get("/sessions/{session_id}/stats")
async def get_session_stats(session_id: str):
    """获取SSH会话状态"""
    app = router.app
    stats = app.ssh_forwarder.get_session_stats(session_id)
    if not stats:
        raise HTTPException(status_code=404, detail="Session not found")
    return stats
