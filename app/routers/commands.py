import time
from fastapi import APIRouter, HTTPException
from typing import Dict, Any, List
import uuid
import asyncio
from loguru import logger

router = APIRouter(prefix="/api", tags=["commands"])

@router.post("/schedule_command")
async def schedule_command(command_data: dict):
    command_id = str(uuid.uuid4())
    logger.info(f"开始调度命令 [command_id={command_id}] [data={command_data}]")
    try:
        app = router.app
        command = app.forwarder.ScheduledCommand(command_data)
        task = asyncio.create_task(app.forwarder.command_transmitter.send(command))
        app.forwarder.scheduled_tasks[command_id] = task
        logger.success(f"命令调度成功 [command_id={command_id}]")
        return {"task_id": command_id}
    except Exception as e:
        logger.error(f"命令调度失败 [command_id={command_id}]: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/batch_command")
async def batch_command(commands: List[dict]):
    logger.info(f"开始执行批量命令 [commands_count={len(commands)}]")
    try:
        app = router.app
        command = app.forwarder.BatchCommand(commands)
        start_time = time.time()
        result = await app.forwarder.command_transmitter.send(command)
        execution_time = (time.time() - start_time) * 1000
        logger.success(f"批量命令执行完成 [execution_time={execution_time:.2f}ms]")
        return result.result
    except Exception as e:
        logger.error(f"批量命令执行失败: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/device/control")
async def device_control(command_data: Dict[str, Any]):
    try:
        app = router.app
        command = app.forwarder.DeviceControlCommand(command_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/data/query")
async def data_query(query_data: Dict[str, Any]):
    try:
        app = router.app
        command = app.forwarder.DataQueryCommand(query_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
