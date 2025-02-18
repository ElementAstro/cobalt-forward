from fastapi import APIRouter, WebSocket
from fastapi.websockets import WebSocketDisconnect
import time
import json
from loguru import logger
from app.client.ssh_client import SSHConfig

router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    connection_id = id(websocket)
    logger.info(f"WebSocket连接请求 [connection_id={connection_id}]")
    start_time = time.time()
    app = websocket.app

    await app.forwarder.manager.connect(websocket)
    logger.success(f"WebSocket连接建立成功 [connection_id={connection_id}]")

    try:
        while True:
            message = await websocket.receive_text()
            process_start = time.time()
            logger.debug(
                f"收到WebSocket消息 [connection_id={connection_id}] [message={message}]")

            await app.forwarder.manager.handle_client_message(websocket, message)

            latency = (time.time() - process_start) * 1000
            app.forwarder.performance_monitor.record_message(latency)
            logger.debug(
                f"WebSocket消息处理完成 [connection_id={connection_id}] [latency={latency:.2f}ms]")

    except WebSocketDisconnect:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(
            f"WebSocket连接断开 [connection_id={connection_id}] [duration={duration:.2f}s]")
        await app.forwarder.manager.disconnect(websocket)
    except Exception as e:
        logger.exception(
            f"WebSocket错误 [connection_id={connection_id}]: {str(e)}")
        raise


@router.websocket("/ws/ssh")
async def websocket_ssh_endpoint(websocket: WebSocket):
    """SSH WebSocket终端连接处理"""
    connection_id = id(websocket)
    logger.info(f"SSH WebSocket连接请求 [connection_id={connection_id}]")

    await websocket.accept()
    app = websocket.app
    session_id = None

    try:
        # 等待客户端发送SSH连接配置
        config_data = await websocket.receive_json()
        ssh_config = SSHConfig(**config_data)

        # 创建SSH会话
        session_id = await app.ssh_forwarder.create_session(websocket, ssh_config)
        logger.success(
            f"SSH会话创建成功 [connection_id={connection_id}] [session_id={session_id}]")

        # 处理客户端消息
        while True:
            message = await websocket.receive_text()
            await app.ssh_forwarder.handle_message(session_id, message)

    except WebSocketDisconnect:
        logger.info(f"SSH WebSocket连接断开 [connection_id={connection_id}]")
        if session_id:
            await app.ssh_forwarder.close_session(session_id)
    except Exception as e:
        logger.exception(
            f"SSH WebSocket错误 [connection_id={connection_id}]: {str(e)}")
        raise
    finally:
        if session_id:
            await app.ssh_forwarder.close_session(session_id)
