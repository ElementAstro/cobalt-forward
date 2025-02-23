from fastapi import APIRouter, HTTPException, status, Body, Query, Path
from typing import Dict, Any, Optional, Annotated
from pydantic import BaseModel, Field, field_validator, StringConstraints
from app.core.upload_manager import UploadManager
from app.core.message_bus import MessageBus
from app.core.event_bus import Event, EventBus, EventPriority
from loguru import logger
import asyncio
from uuid import uuid4
import re
from datetime import datetime, timezone


class UploadRequest(BaseModel):
    """Upload request validation model"""
    filename: Annotated[str, StringConstraints(min_length=1, max_length=255)]
    total_size: int = Field(..., gt=0, lt=1024**4)  # Max 1TB

    @field_validator('filename')
    def validate_filename(cls, v):
        pattern = r'^[a-zA-Z0-9\-_\.]+$'
        if not re.match(pattern, v):
            raise ValueError(
                "Filename must contain only letters, numbers, dash, underscore and dot")
        return v


class MessageData(BaseModel):
    """Message data validation model"""
    topic: Annotated[str, StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r'^[a-zA-Z0-9\-_\.\/]+$'
    )]
    data: Dict[str, Any] = Field(..., description="Message payload")
    priority: Optional[int] = Field(default=0, ge=0, le=10)
    # Message expiration in seconds
    expiration: Optional[int] = Field(default=3600, ge=0)


class EventData(BaseModel):
    """Event data validation model"""
    name: Annotated[str, StringConstraints(
        min_length=1,
        max_length=255,
        pattern=r'^[a-zA-Z0-9\-_\.]+$'
    )]
    data: Dict[str, Any] = Field(..., description="Event payload")
    priority: str = Field(
        default="NORMAL",
        pattern="^(LOW|NORMAL|HIGH)$"
    )


router = APIRouter(
    prefix="/api/core",
    tags=["core"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Resource not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {
            "description": "Internal server error"}
    }
)


@router.post("/upload/create", status_code=status.HTTP_201_CREATED)
async def create_upload(upload_request: UploadRequest):
    """Create file upload task"""
    request_id = str(uuid4())
    logger.info(
        f"Creating upload task [request_id={request_id}] [file={upload_request.filename}]")

    try:
        app = router.app
        async with asyncio.timeout(30):
            upload_id = await app.upload_manager.create_upload(
                upload_request.filename,
                upload_request.total_size
            )

        logger.success(
            f"Upload task created [request_id={request_id}] [upload_id={upload_id}]")
        return {
            "upload_id": upload_id,
            "request_id": request_id,
            "status": "created",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    except asyncio.TimeoutError:
        logger.error(f"Upload creation timeout [request_id={request_id}]")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Upload creation timed out"
        )
    except ValueError as e:
        logger.error(
            f"Invalid upload parameters [request_id={request_id}]: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(
            f"Upload creation failed [request_id={request_id}]: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create upload task"
        )


@router.post("/message/publish", status_code=status.HTTP_200_OK)
async def publish_message(message: MessageData):
    """Publish message to message bus"""
    message_id = str(uuid4())
    logger.info(
        f"Publishing message [message_id={message_id}] [topic={message.topic}]")

    try:
        app = router.app
        async with asyncio.timeout(10):
            await app.message_bus.publish(
                message.topic,
                message.data,
                priority=message.priority
            )

        logger.success(
            f"Message published successfully [message_id={message_id}]")
        return {
            "message_id": message_id,
            "status": "published",
            "timestamp": app.message_bus.get_last_publish_time(message.topic)
        }

    except asyncio.TimeoutError:
        logger.error(f"Message publish timeout [message_id={message_id}]")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Message publish timed out"
        )
    except Exception as e:
        logger.error(
            f"Message publish failed [message_id={message_id}]: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish message"
        )


@router.post("/event/publish", status_code=status.HTTP_200_OK)
async def publish_event(
    event_name: str = Body(..., embed=True),
    data: Dict[str, Any] = Body(...),
    priority: str = Body(default="NORMAL", pattern="^(LOW|NORMAL|HIGH)$")
):
    """发布事件到事件总线"""
    event_id = str(uuid4())
    logger.info(f"Publishing event [event_id={event_id}] [name={event_name}]")

    try:
        app = router.app
        event = Event(
            name=event_name,
            data=data,
            priority=EventPriority[priority]
        )

        async with asyncio.timeout(10):
            await app.event_bus.publish(event)

        return {
            "event_id": event_id,
            "status": "published",
            "timestamp": event.timestamp
        }

    except asyncio.TimeoutError:
        logger.error(f"Event publish timeout [event_id={event_id}]")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Event publish timed out"
        )
    except Exception as e:
        logger.error(
            f"Event publish failed [event_id={event_id}]: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to publish event"
        )


@router.get("/upload/{upload_id}/status")
async def get_upload_status(upload_id: str):
    """获取上传任务状态"""
    try:
        app = router.app
        status = await app.upload_manager.get_upload_status(upload_id)
        if not status:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Upload task not found"
            )
        return status
    except Exception as e:
        logger.error(f"Failed to get upload status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to get upload status"
        )


@router.delete("/upload/{upload_id}")
async def cancel_upload(upload_id: str):
    """取消上传任务"""
    try:
        app = router.app
        await app.upload_manager.cancel_upload(upload_id)
        return {"status": "cancelled", "upload_id": upload_id}
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Upload task not found"
        )
    except Exception as e:
        logger.error(f"Failed to cancel upload: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to cancel upload"
        )


@router.get("/stats")
async def get_stats(
    include_history: bool = Query(default=False),
    timeframe: str = Query(default="1h", pattern="^[0-9]+[hmds]$"),
    cache_ttl: int = Query(default=60, ge=0, le=3600)
):
    """Get core component statistics with caching"""
    cache_key = f"core_stats_{include_history}_{timeframe}"

    try:
        app = router.app

        # Try to get from cache first
        if cached_stats := await app.cache.get(cache_key):
            return cached_stats

        async with asyncio.timeout(5):
            stats = {
                "message_bus": app.message_bus.metrics,
                "command_stats": app.command_dispatcher.metrics,
                "upload_manager": app.upload_manager.stats,
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            if include_history:
                stats["history"] = {
                    "message_bus": await app.message_bus.get_history(timeframe),
                    "commands": await app.command_dispatcher.get_history(timeframe)
                }

            # Cache the results
            await app.cache.set(cache_key, stats, expire=cache_ttl)
            return stats

    except asyncio.TimeoutError:
        logger.error("Stats retrieval timeout")
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Stats retrieval timed out"
        )
    except Exception as e:
        logger.error(f"Failed to retrieve stats: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system statistics"
        )
