import base64
from dataclasses import dataclass
import asyncio
from typing import AsyncGenerator, Dict, Optional, Set, List
from datetime import datetime
import uuid
from loguru import logger
from app.client.ssh.client import SSHClient, SSHConfig
from fastapi import WebSocket
import json
from app.core.stream_handler import StreamHandler
import os
from pathlib import Path


@dataclass
class SSHSessionStats:
    created_at: datetime
    bytes_sent: int = 0
    bytes_received: int = 0
    commands_executed: int = 0
    last_activity: datetime = None


@dataclass
class SSHSession:
    client: SSHClient
    websocket: WebSocket
    channel: any  # paramiko.Channel
    active: bool = True
    stats: SSHSessionStats
    terminal_size: Dict[str, int] = None
    command_history: List[str] = None


class SSHForwarder:
    """
    SSHForwarder manages SSH sessions and facilitates communication between SSH channels and WebSocket clients.
    It supports operations such as session creation, command execution, terminal resizing, file transfers,
    and streaming of file data or command output.
    Attributes:
        sessions (Dict[str, SSHSession]): Active SSH sessions mapped by their session IDs.
        _running_tasks (Set[asyncio.Task]): Set of asynchronous tasks managing session activities.
        file_transfers (Dict[str, asyncio.Task]): Ongoing file transfer tasks indexed by session or transfer IDs.
        session_logs (Dict[str, List[Dict]]): Logs of command executions and other events per session.
        stream_handler (StreamHandler): Handler for managing streamed file uploads and command output.
        upload_dir (Path): Directory for temporary storage of files during uploads.
    Methods:
        __init__():
            Initializes the SSHForwarder instance, sets up session management, and ensures the upload directory exists.
        create_session(websocket: WebSocket, ssh_config: SSHConfig) -> str:
            Creates a new SSH session using the provided WebSocket and SSH configuration.
            Establishes an SSH connection and shell channel, stores session details, and starts asynchronous monitoring tasks.
        close_session(session_id: str):
            Closes the active SSH session identified by session_id.
            Shuts down the connection gracefully and removes session data.
        send_to_ssh(session_id: str, data: str):
            Sends data from the WebSocket client to the SSH channel for the specified session.
        handle_message(session_id: str, message: str):
            Processes an incoming WebSocket message.
            Determines the message type and delegates to the appropriate handler for terminal input, resizing, file transfers,
            executing commands, or streaming operations.
        resize_terminal(session_id: str, rows: int, cols: int):
            Adjusts the terminal window size for the given SSH session both locally and on the remote channel.
        start_file_upload(session_id: str, filename: str, data: str):
            Initiates a file upload operation by creating an asynchronous task to upload the file data
            to a designated temporary path.
        start_file_download(session_id: str, path: str):
            Initiates a file download operation from the remote SSH session.
            On success, sends the downloaded file data to the client; on failure, returns an error status.
        execute_command(session_id: str, command: str):
            Executes a command on the remote SSH session.
            Handles both synchronous and asynchronous command execution, logs the command along with its result,
            and sends the command output or error back to the client.
        get_session_stats(session_id: str) -> Dict:
            Retrieves statistics for a specific SSH session including creation time, uptime, data transfer details,
            command execution count, and last activity timestamp.
        _update_command_history(session_id: str, command: str):
            Updates the command history for the session and records the last activity time.
        _log_command(session_id: str, command: str, result: Dict):
            Logs details of a command execution, including timestamp, command string, exit code, and command output.
        _send_status(session_id: str, status: str, data: Dict = None):
            Sends a status message to the WebSocket client.
            The message includes the status type and additional optional data.
        _send_error(session_id: str, error: str):
            Sends an error message to the client by wrapping the error details in a status message.
        _send_command_result(session_id: str, result: Dict):
            Sends the result of a command execution to the client via the WebSocket connection.
        _send_file_data(session_id: str, filename: str, data: bytes):
            Sends file data encoded in base64 to the client as part of a file download operation.
        _start_session_tasks(session_id: str):
            Launches asynchronous tasks to forward SSH output to the WebSocket and monitor session activity.
        _monitor_session(session_id: str):
            Periodically monitors the SSH session for inactivity.
            Closes the session if it remains idle beyond a specified timeout period.
        _forward_ssh_to_ws(session_id: str):
            Continuously reads SSH channel output and forwards any available data to the WebSocket client.
        _handle_file_transfer(session_id: str, payload: Dict):
            Manages file transfer operations including initiating transfers, handling file chunks,
            and completing file transfers with appropriate notifications to the client.
        _handle_stream_file(session_id: str, payload: Dict):
            Handles streaming file transfers for both downloads (streaming data from remote file)
            and uploads (receiving streamed data and transferring it to the remote destination).
        stream_remote_file(session_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
            Provides an asynchronous generator that streams remote file data in chunks for processing.
        _handle_command(session_id: str, payload: Dict):
            Processes a command execution request.
            Supports both streaming and non-streaming command execution, sending real-time output or a final result
            back to the client, and increments the executed command count.
    """

    def __init__(self):
        self.sessions: Dict[str, SSHSession] = {}
        self._running_tasks: Set[asyncio.Task] = set()
        self.file_transfers: Dict[str, asyncio.Task] = {}
        self.session_logs: Dict[str, List[Dict]] = {}
        self.stream_handler = StreamHandler()
        self.upload_dir = Path("/tmp/ssh_uploads")
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        logger.info("SSH转发器初始化完成")

    async def create_session(self, websocket: WebSocket, ssh_config: SSHConfig) -> str:
        """创建新的SSH会话"""
        session_id = str(id(websocket))
        try:
            client = SSHClient(ssh_config)
            client.connect()
            channel = client.ssh.invoke_shell()

            session = SSHSession(
                client=client,
                websocket=websocket,
                channel=channel,
                active=True,
                stats=SSHSessionStats(created_at=datetime.now()),
                terminal_size={"rows": 24, "cols": 80},
                command_history=[]
            )

            self.sessions[session_id] = session
            self.session_logs[session_id] = []

            # 启动监控任务
            self._start_session_tasks(session_id)
            logger.info(f"SSH会话创建成功 [session_id={session_id}]")
            return session_id

        except Exception as e:
            logger.error(f"创建SSH会话失败: {str(e)}")
            raise

    async def close_session(self, session_id: str):
        """关闭SSH会话"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            session.active = False
            session.client.close()
            del self.sessions[session_id]
            logger.info(f"SSH会话已关闭 [session_id={session_id}]")

    async def send_to_ssh(self, session_id: str, data: str):
        """发送数据到SSH"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            session.channel.send(data)
            logger.debug(
                f"数据已发送到SSH [session_id={session_id}] [length={len(data)}]")

    async def handle_message(self, session_id: str, message: str):
        """处理WebSocket消息"""
        try:
            data = json.loads(message)
            message_type = data.get("type")
            payload = data.get("payload", {})

            handlers = {
                "terminal_input": self._handle_terminal_input,
                "resize": self._handle_resize,
                "file_upload": self._handle_file_upload,
                "file_download": self._handle_file_download,
                "execute_command": self._handle_command,
                "file_transfer": self._handle_file_transfer,
                "stream_file": self._handle_stream_file
            }

            handler = handlers.get(message_type)
            if handler:
                await handler(session_id, payload)
            else:
                await self._send_error(session_id, f"Unknown message type: {message_type}")

        except Exception as e:
            logger.error(f"处理消息失败 [session_id={session_id}]: {str(e)}")
            await self._send_error(session_id, str(e))

    async def resize_terminal(self, session_id: str, rows: int, cols: int):
        """调整终端大小"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            session.terminal_size = {"rows": rows, "cols": cols}
            session.channel.resize_pty(width=cols, height=rows)
            logger.debug(
                f"终端大小已调整 [session_id={session_id}] [rows={rows}] [cols={cols}]")

    async def start_file_upload(self, session_id: str, filename: str, data: str):
        """开始文件上传"""
        session = self.sessions[session_id]
        transfer_task = asyncio.create_task(
            session.client.upload_file(data, f"/tmp/{filename}")
        )
        self.file_transfers[session_id] = transfer_task
        await self._send_status(session_id, "upload_started", {"filename": filename})

    async def start_file_download(self, session_id: str, path: str):
        """开始文件下载"""
        session = self.sessions[session_id]
        try:
            data = await session.client.download_file(path)
            await self._send_file_data(session_id, path, data)
        except Exception as e:
            await self._send_error(session_id, f"文件下载失败: {str(e)}")

    async def execute_command(self, session_id: str, command: str):
        """执行命令并返回结果"""
        session = self.sessions[session_id]
        try:
            result = await session.client.execute_command_async(command)
            self._log_command(session_id, command, result)
            await self._send_command_result(session_id, result)
            session.stats.commands_executed += 1
        except Exception as e:
            await self._send_error(session_id, f"命令执行失败: {str(e)}")

    def get_session_stats(self, session_id: str) -> Dict:
        """获取会话统计信息"""
        if session_id in self.sessions:
            session = self.sessions[session_id]
            return {
                "created_at": session.stats.created_at.isoformat(),
                "uptime": (datetime.now() - session.stats.created_at).total_seconds(),
                "bytes_sent": session.stats.bytes_sent,
                "bytes_received": session.stats.bytes_received,
                "commands_executed": session.stats.commands_executed,
                "last_activity": session.stats.last_activity.isoformat() if session.stats.last_activity else None
            }
        return None

    def _update_command_history(self, session_id: str, command: str):
        """更新命令历史"""
        session = self.sessions[session_id]
        session.command_history.append(command)
        session.stats.last_activity = datetime.now()

    def _log_command(self, session_id: str, command: str, result: Dict):
        """记录命令执行"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "command": command,
            "exit_code": result.get("status"),
            "output": result.get("stdout")
        }
        self.session_logs[session_id].append(log_entry)

    async def _send_status(self, session_id: str, status: str, data: Dict = None):
        """发送状态消息到客户端"""
        session = self.sessions[session_id]
        message = {
            "type": "status",
            "status": status,
            "data": data or {}
        }
        await session.websocket.send_json(message)

    async def _send_error(self, session_id: str, error: str):
        """发送错误消息到客户端"""
        await self._send_status(session_id, "error", {"message": error})

    async def _send_command_result(self, session_id: str, result: Dict):
        """发送命令执行结果"""
        await self._send_status(session_id, "command_result", result)

    async def _send_file_data(self, session_id: str, filename: str, data: bytes):
        """发送文件数据"""
        await self._send_status(session_id, "file_data", {
            "filename": filename,
            "data": data.decode('base64')
        })

    def _start_session_tasks(self, session_id: str):
        """启动会话相关的异步任务"""
        # 数据转发任务
        forward_task = asyncio.create_task(self._forward_ssh_to_ws(session_id))
        self._running_tasks.add(forward_task)
        forward_task.add_done_callback(self._running_tasks.discard)

        # 会话监控任务
        monitor_task = asyncio.create_task(self._monitor_session(session_id))
        self._running_tasks.add(monitor_task)
        monitor_task.add_done_callback(self._running_tasks.discard)

    async def _monitor_session(self, session_id: str):
        """监控会话活动"""
        while session_id in self.sessions:
            session = self.sessions[session_id]
            if session.active:
                # 检查空闲超时
                if session.stats.last_activity:
                    idle_time = (datetime.now() -
                                 session.stats.last_activity).total_seconds()
                    if idle_time > 3600:  # 1小时超时
                        logger.warning(f"会话空闲超时 [session_id={session_id}]")
                        await self.close_session(session_id)
                        break
            await asyncio.sleep(60)  # 每分钟检查一次

    async def _forward_ssh_to_ws(self, session_id: str):
        """将SSH输出转发到WebSocket"""
        session = self.sessions[session_id]
        try:
            while session.active:
                if session.channel.recv_ready():
                    data = session.channel.recv(1024).decode(
                        'utf-8', errors='ignore')
                    if data:
                        await session.websocket.send_text(data)
                        logger.debug(
                            f"SSH数据已转发到WebSocket [session_id={session_id}] [length={len(data)}]")
                await asyncio.sleep(0.1)
        except Exception as e:
            logger.error(f"SSH转发错误 [session_id={session_id}]: {str(e)}")
            await self.close_session(session_id)

    async def _handle_file_transfer(self, session_id: str, payload: Dict):
        """处理文件传输请求"""
        session = self.sessions[session_id]
        operation = payload.get("operation")

        if operation == "start":
            # 开始新的文件传输
            transfer_id = str(uuid.uuid4())
            self.file_transfers[transfer_id] = {
                "filename": payload["filename"],
                "size": payload["size"],
                "received": 0,
                "path": self.upload_dir / payload["filename"]
            }
            await self._send_status(session_id, "transfer_ready", {"transfer_id": transfer_id})

        elif operation == "chunk":
            # 处理文件块
            transfer_id = payload["transfer_id"]
            if transfer_id in self.file_transfers:
                transfer = self.file_transfers[transfer_id]
                chunk = base64.b64decode(payload["data"])

                with open(transfer["path"], 'ab') as f:
                    f.write(chunk)

                transfer["received"] += len(chunk)
                await self._send_status(session_id, "transfer_progress", {
                    "transfer_id": transfer_id,
                    "received": transfer["received"],
                    "total": transfer["size"]
                })

        elif operation == "complete":
            # 完成文件传输
            transfer_id = payload["transfer_id"]
            if transfer_id in self.file_transfers:
                transfer = self.file_transfers[transfer_id]
                await self._send_status(session_id, "transfer_complete", {
                    "transfer_id": transfer_id,
                    "path": str(transfer["path"])
                })
                del self.file_transfers[transfer_id]

    async def _handle_stream_file(self, session_id: str, payload: Dict):
        """处理文件流式传输"""
        session = self.sessions[session_id]
        remote_path = payload["path"]
        operation = payload["operation"]

        if operation == "download":
            # 流式下载远程文件
            async for chunk in session.client.stream_download(remote_path):
                await session.websocket.send_json({
                    "type": "file_stream",
                    "data": base64.b64encode(chunk).decode(),
                    "more": True
                })
            await session.websocket.send_json({
                "type": "file_stream",
                "data": "",
                "more": False
            })

        elif operation == "upload":
            # 准备接收流式上传
            file_path = self.upload_dir / payload["filename"]
            async for chunk in self.stream_handler.receive_file_upload(session.websocket):
                with open(file_path, 'ab') as f:
                    f.write(chunk)

            # 上传完成后传输到远程
            await session.client.upload_file(str(file_path), payload["remote_path"])
            os.unlink(file_path)  # 清理临时文件

    async def stream_remote_file(self, session_id: str, remote_path: str) -> AsyncGenerator[bytes, None]:
        """流式读取远程文件"""
        session = self.sessions[session_id]
        try:
            async for chunk in session.client.stream_download(remote_path):
                yield chunk
        except Exception as e:
            logger.error(f"远程文件流读取错误 [session_id={session_id}]: {str(e)}")
            raise

    async def _handle_command(self, session_id: str, payload: Dict):
        """处理命令执行"""
        session = self.sessions[session_id]
        command = payload["command"]
        stream = payload.get("stream", False)

        try:
            if stream:
                # 流式执行命令
                async for output in session.client.stream_command(command):
                    await self.stream_handler.stream_command_output(
                        session.websocket,
                        output
                    )
            else:
                # 普通命令执行
                result = await session.client.execute_command_async(command)
                await self._send_command_result(session_id, result)

            session.stats.commands_executed += 1

        except Exception as e:
            await self._send_error(session_id, f"命令执行失败: {str(e)}")
