from fastapi import APIRouter, WebSocket
from fastapi.websockets import WebSocketDisconnect
import time
from loguru import logger

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
            logger.debug(f"收到WebSocket消息 [connection_id={connection_id}] [message={message}]")
            
            await app.forwarder.manager.handle_client_message(websocket, message)
            
            latency = (time.time() - process_start) * 1000
            app.forwarder.performance_monitor.record_message(latency)
            logger.debug(f"WebSocket消息处理完成 [connection_id={connection_id}] [latency={latency:.2f}ms]")

    except WebSocketDisconnect:
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"WebSocket连接断开 [connection_id={connection_id}] [duration={duration:.2f}s]")
        await app.forwarder.manager.disconnect(websocket)
    except Exception as e:
        logger.exception(f"WebSocket错误 [connection_id={connection_id}]: {str(e)}")
        raise
