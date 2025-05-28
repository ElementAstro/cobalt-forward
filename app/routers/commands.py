from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from typing import Dict, Any, List
from uuid import uuid4
import asyncio
import time
from loguru import logger

# Input validation models


class CommandData(BaseModel):
    type: str = Field(..., description="Command type")
    parameters: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=0, le=10)
    timeout: float = Field(default=30.0, ge=0)

    @field_validator('type')
    def validate_type(cls, v):
        allowed_types = {'control', 'query', 'config', 'reset'}
        if v not in allowed_types:
            raise ValueError(
                f"Invalid command type. Must be one of: {allowed_types}")
        return v


class BatchCommandRequest(BaseModel):
    commands: List[CommandData] = Field(..., max_length=100)
    parallel: bool = Field(default=False)


router = APIRouter(prefix="/api", tags=["commands"])


async def global_exception_handler(_request: Request, exc: Exception):
    """Global exception handler for all routes"""
    error_id = str(uuid4())
    logger.error(f"Error ID: {error_id} - {str(exc)}", exc_info=True)
    return JSONResponse(
        status_code=500,
        content={
            "error_id": error_id,
            "detail": "Internal server error. Please contact support with the error ID."
        }
    )

router.add_exception_handler(Exception, global_exception_handler)


@router.post("/schedule_command")
async def schedule_command(command: CommandData, request: Request):
    """Schedule a command for execution"""
    command_id = str(uuid4())
    logger.info(f"Scheduling command [id={command_id}] [type={command.type}]")

    try:
        app = request.app 
        # Assuming app.forwarder has ScheduledCommand, command_transmitter, and scheduled_tasks
        # If router.app() was intended, it implies a custom APIRouter or setup.
        # Using request.app is the standard FastAPI way to get the app instance.
        scheduled_command = app.forwarder.ScheduledCommand(**command.model_dump())

        # Create task with timeout
        task = asyncio.create_task(
            asyncio.wait_for(
                app.forwarder.command_transmitter.send(scheduled_command),
                timeout=command.timeout
            )
        )

        app.forwarder.scheduled_tasks[command_id] = task
        logger.success(f"Command scheduled successfully [id={command_id}]")

        return {
            "task_id": command_id,
            "estimated_completion": time.time() + command.timeout
        }

    except asyncio.TimeoutError:
        logger.error(f"Command scheduling timeout [id={command_id}]")
        raise HTTPException(
            status_code=408,
            detail="Command scheduling timed out"
        )
    except ValueError as e:
        logger.error(f"Invalid command data [id={command_id}]: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Command scheduling failed [id={command_id}]: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Internal command processing error")


@router.post("/batch_command")
async def batch_command(batch_request: BatchCommandRequest, request: Request):
    """Execute multiple commands in batch"""
    batch_id = str(uuid4())
    logger.info(
        f"Starting batch execution [id={batch_id}] [count={len(batch_request.commands)}]")

    try:
        app = request.app
        start_time = time.perf_counter()

        if batch_request.parallel:
            # Execute commands in parallel
            tasks = [
                app.forwarder.command_transmitter.send(
                    app.forwarder.BatchCommand([cmd.model_dump()])
                )
                for cmd in batch_request.commands
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            # Execute commands sequentially
            command_payload = app.forwarder.BatchCommand(
                [cmd.model_dump() for cmd in batch_request.commands])
            results = await app.forwarder.command_transmitter.send(command_payload)

        execution_time = (time.perf_counter() - start_time) * 1000
        logger.success(
            f"Batch execution completed [id={batch_id}] "
            f"[time={execution_time:.2f}ms]"
        ) # Removed success={len(results)} as results structure varies

        return {
            "batch_id": batch_id,
            "execution_time_ms": execution_time,
            "results": results # Ensure results are serializable
        }

    except Exception as e:
        logger.error(f"Batch execution failed [id={batch_id}]: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Batch execution failed: {str(e)}"
        )


@router.post("/device/control")
async def device_control(command: CommandData, request: Request):
    """Control device with specified command"""
    operation_id = str(uuid4())
    logger.info(
        f"Device control operation [id={operation_id}] [type={command.type}]")

    try:
        app = request.app
        control_command_payload = app.forwarder.DeviceControlCommand(**command.model_dump())

        # Execute with timeout
        result = await asyncio.wait_for(
            app.forwarder.command_transmitter.send(control_command_payload),
            timeout=command.timeout
        )

        logger.success(f"Device control successful [id={operation_id}]")
        return {
            "operation_id": operation_id,
            "result": result.result # Assuming result has a 'result' attribute
        }

    except asyncio.TimeoutError:
        logger.error(f"Device control timeout [id={operation_id}]")
        raise HTTPException(status_code=408, detail="Operation timed out")
    except Exception as e:
        logger.error(f"Device control failed [id={operation_id}]: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Device control operation failed")


@router.post("/data/query")
async def data_query(query: CommandData, request: Request):
    """Query device data"""
    query_id = str(uuid4())
    logger.info(f"Data query operation [id={query_id}] [type={query.type}]")

    try:
        app = request.app
        query_command_payload = app.forwarder.DataQueryCommand(**query.model_dump())

        # Execute with timeout
        result = await asyncio.wait_for(
            app.forwarder.command_transmitter.send(query_command_payload),
            timeout=query.timeout
        )

        logger.success(f"Data query successful [id={query_id}]")
        return {
            "query_id": query_id,
            "data": result.result # Assuming result has a 'result' attribute
        }

    except asyncio.TimeoutError:
        logger.error(f"Data query timeout [id={query_id}]")
        raise HTTPException(status_code=408, detail="Query timed out")
    except Exception as e:
        logger.error(f"Data query failed [id={query_id}]: {str(e)}")
        raise HTTPException(
            status_code=500, detail="Data query operation failed")
