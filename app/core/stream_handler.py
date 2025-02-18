from typing import AsyncGenerator, Dict, Any, Optional
import asyncio
from loguru import logger
import json
import base64
import os

class RateLimit:
    def __init__(self, max_rate: int):
        self.max_rate = max_rate
        self._tokens = max_rate
        self._last = asyncio.get_event_loop().time()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def acquire(self, tokens: int):
        while self._tokens < tokens:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last
            self._tokens += elapsed * self.max_rate
            self._tokens = min(self._tokens, self.max_rate)
            self._last = now
            await asyncio.sleep(0.01)
        self._tokens -= tokens

class StreamHandler:
    def __init__(self):
        self.CHUNK_SIZE = 1024 * 16
        self._progress_callbacks = {}
        self._flow_controller = self._setup_flow_controller()

    def _setup_flow_controller(self):
        return RateLimit(max_rate=1024 * 1024)  # 1MB/s

    async def stream_file_to_websocket(self, websocket, file_path: str):
        progress = {'total': os.path.getsize(file_path), 'current': 0}
        
        async with self._flow_controller:
            try:
                with open(file_path, 'rb') as f:
                    while chunk := f.read(self.CHUNK_SIZE):
                        await self._send_chunk(websocket, chunk, progress)
                        
            except Exception as e:
                logger.error(f"Stream error: {e}")
                await self._handle_stream_error(websocket, e)

    async def _send_chunk(self, websocket, chunk: bytes, progress: dict):
        encoded = base64.b64encode(chunk).decode()
        progress['current'] += len(chunk)
        
        await websocket.send_json({
            'type': 'file_chunk',
            'data': encoded,
            'progress': progress['current'] / progress['total']
        })

    async def _handle_stream_error(self, websocket, error: Exception):
        await websocket.send_json({
            "type": "error",
            "error": str(error)
        })

    async def stream_command_output(self, websocket, command_stream: AsyncGenerator[Dict[str, Any], None]):
        """流式发送命令输出"""
        try:
            async for output in command_stream:
                await websocket.send_json({
                    "type": "command_output",
                    "data": output,
                    "timestamp": str(asyncio.get_event_loop().time())
                })
        except Exception as e:
            logger.error(f"命令输出流传输错误: {str(e)}")
            await websocket.send_json({
                "type": "error",
                "error": str(e)
            })

    async def receive_file_upload(self, websocket) -> AsyncGenerator[bytes, None]:
        """接收文件上传流"""
        buffer = b""
        try:
            while True:
                message = await websocket.receive_json()
                if message["type"] != "file_chunk":
                    continue
                
                if not message.get("more", False) and not message.get("data"):
                    break
                
                chunk = base64.b64decode(message["data"])
                buffer += chunk
                
                if len(buffer) >= self.CHUNK_SIZE:
                    yield buffer
                    buffer = b""
            
            if buffer:  # 发送剩余数据
                yield buffer
                
        except Exception as e:
            logger.error(f"文件上传流接收错误: {str(e)}")
            raise
