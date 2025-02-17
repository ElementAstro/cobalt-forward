from fastapi import APIRouter, HTTPException, Body, Query
from typing import Dict, Any, Optional
import time
from fastapi.responses import JSONResponse
from dataclasses import asdict
from loguru import logger

router = APIRouter(prefix="/api/system", tags=["system"])

@router.get("/status")
async def get_system_status():
    logger.debug("开始获取系统状态")
    try:
        app = router.app
        status_data = {
            "server": {
                "status": "running",
                "uptime": time.time() - app.state.start_time,
                "version": "1.0.0"
            },
            "clients": {
                "websocket": len(app.forwarder.manager.active_connections),
                "tcp": app.forwarder.tcp_client.state.value if app.forwarder.tcp_client else "Not Connected",
                "mqtt": "Connected" if app.forwarder.mqtt_client and app.forwarder.mqtt_client.connected else "Not Connected"
            },
            "plugins": {
                "loaded": len(app.forwarder.plugin_manager.plugins),
                "active": sum(1 for p in app.forwarder.plugin_manager.plugins.values() if p.state == "ACTIVE")
            },
            "performance": app.forwarder.performance_monitor.get_current_metrics().__dict__,
            "cache": await app.forwarder.cache.raw.stats()
        }
        logger.info(f"系统状态获取成功 [uptime={status_data['server']['uptime']:.2f}s]")
        return status_data
    except Exception as e:
        logger.error(f"获取系统状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/maintenance")
async def trigger_maintenance(
    operation: str = Body(..., embed=True),
    params: Dict[str, Any] = Body(default={}, embed=True)
):
    logger.info(f"开始执行维护操作 [operation={operation}] [params={params}]")
    try:
        app = router.app
        if operation == "clear_cache":
            logger.debug("开始清理缓存")
            await app.forwarder.cache.clear()
            logger.success("缓存清理完成")
            return {"status": "success", "message": "Cache cleared"}
        elif operation == "reload_plugins":
            for plugin_name in app.forwarder.plugin_manager.plugins:
                await app.forwarder.plugin_manager.reload_plugin(plugin_name)
            return {"status": "success", "message": "All plugins reloaded"}
        elif operation == "reset_metrics":
            app.forwarder.performance_monitor.reset()
            return {"status": "success", "message": "Metrics reset"}
        else:
            raise HTTPException(status_code=400, detail="Unsupported operation")
    except Exception as e:
        logger.error(f"维护操作失败 [operation={operation}]: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/config")
async def update_config(config_data: Dict[str, Any]):
    try:
        app = router.app
        command = app.forwarder.SystemConfigCommand(config_data)
        result = await app.forwarder.command_transmitter.send(command)
        return result.result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
