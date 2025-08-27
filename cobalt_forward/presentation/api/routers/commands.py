"""
Command management API router for the Cobalt Forward application.

This module provides REST API endpoints for executing and managing
system commands through the command dispatcher.
"""

from fastapi import APIRouter, HTTPException, Depends, status
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field, field_validator
import asyncio
import logging
import time
from uuid import uuid4

from ....core.interfaces.commands import ICommandDispatcher
from ....application.container import IContainer

logger = logging.getLogger(__name__)


class CommandRequest(BaseModel):
    """Command execution request model."""
    command_type: str = Field(..., description="Type of command to execute")
    parameters: Dict[str, Any] = Field(
        default_factory=dict, description="Command parameters")
    priority: int = Field(default=0, ge=0, le=10,
                          description="Command priority (0-10)")
    timeout: float = Field(default=30.0, ge=0, le=3600,
                           description="Command timeout in seconds")
    async_execution: bool = Field(
        default=False, description="Execute command asynchronously")

    @field_validator('command_type')
    @classmethod
    def validate_command_type(cls, v: str) -> str:
        """Validate command type."""
        allowed_types = {
            'system.status', 'system.restart', 'system.shutdown',
            'config.reload', 'config.backup', 'config.restore',
            'ssh.connect', 'ssh.disconnect', 'ssh.execute',
            'upload.start', 'upload.pause', 'upload.cancel',
            'plugin.load', 'plugin.unload', 'plugin.reload'
        }
        if v not in allowed_types:
            raise ValueError(
                f"Invalid command type. Allowed types: {sorted(allowed_types)}")
        return v


class BatchCommandRequest(BaseModel):
    """Batch command execution request model."""
    commands: List[CommandRequest] = Field(
        ..., max_length=50, description="List of commands to execute")
    parallel: bool = Field(
        default=False, description="Execute commands in parallel")
    stop_on_error: bool = Field(
        default=True, description="Stop execution on first error")


class CommandResponse(BaseModel):
    """Command execution response model."""
    command_id: str = Field(..., description="Command identifier")
    command_type: str = Field(..., description="Command type")
    status: str = Field(..., description="Execution status")
    result: Optional[Dict[str, Any]] = Field(
        None, description="Command result")
    error: Optional[str] = Field(None, description="Error message if failed")
    execution_time: float = Field(..., description="Execution time in seconds")
    started_at: float = Field(..., description="Start timestamp")
    completed_at: Optional[float] = Field(
        None, description="Completion timestamp")


class BatchCommandResponse(BaseModel):
    """Batch command execution response model."""
    batch_id: str = Field(..., description="Batch identifier")
    total_commands: int = Field(..., description="Total number of commands")
    successful_commands: int = Field(...,
                                     description="Number of successful commands")
    failed_commands: int = Field(..., description="Number of failed commands")
    execution_time: float = Field(..., description="Total execution time")
    commands: List[CommandResponse] = Field(...,
                                            description="Individual command results")


# Dependency injection
def get_container() -> IContainer:
    """Get the dependency injection container."""
    from fastapi import Request

    def _get_container(request: Request) -> IContainer:
        return request.app.state.container  # type: ignore[no-any-return]

    return Depends(_get_container)  # type: ignore[no-any-return]


def get_command_dispatcher(container: IContainer = get_container()) -> ICommandDispatcher:
    """Get command dispatcher from container."""
    return container.resolve(ICommandDispatcher)  # type: ignore[type-abstract]


# Router definition
router = APIRouter(
    prefix="/api/commands",
    tags=["commands"],
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid command"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.post("/execute", response_model=CommandResponse, status_code=status.HTTP_201_CREATED)
async def execute_command(
    request: CommandRequest,
    command_dispatcher: ICommandDispatcher = Depends(get_command_dispatcher)
) -> CommandResponse:
    """Execute a single command."""
    command_id = str(uuid4())
    started_at = time.time()

    try:
        logger.info(f"Executing command {command_id}: {request.command_type}")

        # Execute command through dispatcher
        # Note: This is a simplified implementation. In a real system, you would
        # create proper Command objects based on the command_type and parameters
        from ....core.domain.commands import Command

        # Create a generic command object (this would need proper implementation)
        command = Command(
            name=request.command_type,
            data=request.parameters,
            metadata={"timeout": request.timeout}
        )

        if request.async_execution:
            # For async execution, we would typically queue the command
            # and return immediately with a pending status
            # Note: dispatch_async doesn't exist in interface, using dispatch
            result = await command_dispatcher.dispatch(command)
            status_value = "pending"
        else:
            # Synchronous execution
            result = await command_dispatcher.dispatch(command)
            status_value = "completed" if result.is_success else "failed"

        completed_at = time.time()
        execution_time = completed_at - started_at

        return CommandResponse(
            command_id=command_id,
            command_type=request.command_type,
            status=status_value,
            result=result.data if hasattr(result, 'data') else None,
            error=result.error if hasattr(
                result, 'error') and not result.is_success else None,
            execution_time=execution_time,
            started_at=started_at,
            completed_at=completed_at if status_value != "pending" else None
        )

    except Exception as e:
        completed_at = time.time()
        execution_time = completed_at - started_at
        error_msg = str(e)

        logger.error(f"Command {command_id} failed: {error_msg}")

        return CommandResponse(
            command_id=command_id,
            command_type=request.command_type,
            status="failed",
            result=None,
            error=error_msg,
            execution_time=execution_time,
            started_at=started_at,
            completed_at=completed_at
        )


@router.post("/batch", response_model=BatchCommandResponse, status_code=status.HTTP_201_CREATED)
async def execute_batch_commands(
    request: BatchCommandRequest,
    command_dispatcher: ICommandDispatcher = Depends(get_command_dispatcher)
) -> BatchCommandResponse:
    """Execute multiple commands in batch."""
    batch_id = str(uuid4())
    started_at = time.time()

    logger.info(
        f"Executing batch {batch_id} with {len(request.commands)} commands")

    command_results = []
    successful_count = 0
    failed_count = 0

    try:
        if request.parallel:
            # Execute commands in parallel
            tasks = []
            for cmd in request.commands:
                task = _execute_single_command(cmd, command_dispatcher)
                tasks.append(task)

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    cmd_response = CommandResponse(
                        command_id=str(uuid4()),
                        command_type=request.commands[i].command_type,
                        status="failed",
                        result=None,
                        error=str(result),
                        execution_time=0.0,
                        started_at=started_at,
                        completed_at=time.time()
                    )
                    failed_count += 1
                else:
                    cmd_response = result  # type: ignore[assignment]
                    if cmd_response.status == "completed":
                        successful_count += 1
                    else:
                        failed_count += 1

                command_results.append(cmd_response)
        else:
            # Execute commands sequentially
            for cmd in request.commands:
                try:
                    cmd_response = await _execute_single_command(cmd, command_dispatcher)
                    command_results.append(cmd_response)

                    if cmd_response.status == "completed":
                        successful_count += 1
                    else:
                        failed_count += 1
                        if request.stop_on_error:
                            logger.warning(
                                f"Stopping batch {batch_id} due to command failure")
                            break

                except Exception as e:
                    cmd_response = CommandResponse(
                        command_id=str(uuid4()),
                        command_type=cmd.command_type,
                        status="failed",
                        result=None,
                        error=str(e),
                        execution_time=0.0,
                        started_at=started_at,
                        completed_at=time.time()
                    )
                    command_results.append(cmd_response)
                    failed_count += 1

                    if request.stop_on_error:
                        logger.warning(
                            f"Stopping batch {batch_id} due to command failure")
                        break

        completed_at = time.time()
        total_execution_time = completed_at - started_at

        return BatchCommandResponse(
            batch_id=batch_id,
            total_commands=len(request.commands),
            successful_commands=successful_count,
            failed_commands=failed_count,
            execution_time=total_execution_time,
            commands=command_results
        )

    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Batch execution failed: {str(e)}"
        )


@router.get("/status/{command_id}", response_model=CommandResponse)
async def get_command_status(
    command_id: str,
    command_dispatcher: ICommandDispatcher = Depends(get_command_dispatcher)
) -> CommandResponse:
    """Get the status of a command execution."""
    try:
        # This would typically query a command status store
        # For now, we'll return a not implemented response
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Command status tracking not yet implemented"
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get command status {command_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get command status: {str(e)}"
        )


async def _execute_single_command(
    cmd: CommandRequest,
    command_dispatcher: ICommandDispatcher
) -> CommandResponse:
    """Execute a single command and return response."""
    command_id = str(uuid4())
    started_at = time.time()

    try:
        # Create a generic command object
        from ....core.domain.commands import Command
        command = Command(
            name=cmd.command_type,
            data=cmd.parameters,
            metadata={"timeout": cmd.timeout}
        )

        result = await command_dispatcher.dispatch(command)

        completed_at = time.time()
        execution_time = completed_at - started_at

        return CommandResponse(
            command_id=command_id,
            command_type=cmd.command_type,
            status="completed" if result.is_success else "failed",
            result=result.data if hasattr(result, 'data') else None,
            error=result.error if hasattr(
                result, 'error') and not result.is_success else None,
            execution_time=execution_time,
            started_at=started_at,
            completed_at=completed_at
        )

    except Exception as e:
        completed_at = time.time()
        execution_time = completed_at - started_at

        return CommandResponse(
            command_id=command_id,
            command_type=cmd.command_type,
            status="failed",
            result=None,
            error=str(e),
            execution_time=execution_time,
            started_at=started_at,
            completed_at=completed_at
        )
