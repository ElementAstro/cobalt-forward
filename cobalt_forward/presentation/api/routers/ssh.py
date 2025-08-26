"""
SSH management API router for the Cobalt Forward application.

This module provides REST API endpoints for managing SSH connections,
tunnels, and file transfers.
"""

from fastapi import APIRouter, HTTPException, status, Depends, Body
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
import logging
import time
from uuid import uuid4

from ....core.interfaces.ssh import ISSHForwarder, ForwardType, ForwardConfig
from ....application.container import IContainer

logger = logging.getLogger(__name__)


class SSHConnectionRequest(BaseModel):
    """SSH connection request model."""
    host: str = Field(..., description="SSH server hostname or IP")
    port: int = Field(default=22, ge=1, le=65535, description="SSH server port")
    username: str = Field(..., description="Username for authentication")
    password: Optional[str] = Field(None, description="Password for authentication")
    key_file: Optional[str] = Field(None, description="Private key file path")
    key_passphrase: Optional[str] = Field(None, description="Private key passphrase")
    auto_connect: bool = Field(default=True, description="Auto-connect after creation")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Additional metadata")


class SSHForwardRequest(BaseModel):
    """SSH port forward request model."""
    forward_type: ForwardType = Field(..., description="Type of port forwarding")
    listen_host: str = Field(default="127.0.0.1", description="Listen host")
    listen_port: int = Field(..., ge=1, le=65535, description="Listen port")
    dest_host: str = Field(..., description="Destination host")
    dest_port: int = Field(..., ge=1, le=65535, description="Destination port")
    description: Optional[str] = Field(None, description="Forward description")


class SSHCommandRequest(BaseModel):
    """SSH command execution request model."""
    command: str = Field(..., description="Command to execute")
    timeout: Optional[float] = Field(None, ge=1, le=3600, description="Command timeout in seconds")


class SSHFileTransferRequest(BaseModel):
    """SSH file transfer request model."""
    local_path: str = Field(..., description="Local file path")
    remote_path: str = Field(..., description="Remote file path")


class SSHConnectionResponse(BaseModel):
    """SSH connection response model."""
    tunnel_id: str = Field(..., description="Tunnel identifier")
    host: str = Field(..., description="SSH server host")
    port: int = Field(..., description="SSH server port")
    username: str = Field(..., description="Username")
    status: str = Field(..., description="Connection status")
    created_at: float = Field(..., description="Creation timestamp")
    forwards: List[Dict[str, Any]] = Field(default_factory=list, description="Port forwards")


class SSHCommandResponse(BaseModel):
    """SSH command execution response model."""
    success: bool = Field(..., description="Command success status")
    command: str = Field(..., description="Executed command")
    exit_status: Optional[int] = Field(None, description="Command exit status")
    stdout: Optional[str] = Field(None, description="Command stdout")
    stderr: Optional[str] = Field(None, description="Command stderr")
    execution_time: Optional[float] = Field(None, description="Execution time in seconds")
    error: Optional[str] = Field(None, description="Error message if failed")


# Dependency injection
def get_container() -> IContainer:
    """Get the dependency injection container."""
    from fastapi import Request
    
    def _get_container(request: Request) -> IContainer:
        return request.app.state.container
    
    return Depends(_get_container)


def get_ssh_forwarder(container: IContainer = get_container()) -> ISSHForwarder:
    """Get SSH forwarder service from container."""
    return container.resolve(ISSHForwarder)


# Router definition
router = APIRouter(
    prefix="/api/ssh",
    tags=["ssh"],
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Tunnel not found"},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error"}
    }
)


@router.post("/tunnels", response_model=SSHConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_ssh_tunnel(
    request: SSHConnectionRequest,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> SSHConnectionResponse:
    """Create a new SSH tunnel."""
    try:
        # Convert forwards if provided
        forwards = []
        if "forwards" in request.metadata:
            for forward_data in request.metadata["forwards"]:
                forward = ForwardConfig(
                    forward_type=ForwardType(forward_data["forward_type"]),
                    listen_host=forward_data["listen_host"],
                    listen_port=forward_data["listen_port"],
                    dest_host=forward_data["dest_host"],
                    dest_port=forward_data["dest_port"],
                    description=forward_data.get("description")
                )
                forwards.append(forward)
        
        # Create tunnel
        tunnel_id = await ssh_forwarder.create_tunnel(
            host=request.host,
            port=request.port,
            username=request.username,
            password=request.password,
            key_file=request.key_file,
            key_passphrase=request.key_passphrase,
            forwards=forwards,
            auto_connect=request.auto_connect,
            metadata=request.metadata
        )
        
        # Get tunnel info
        tunnel_info = ssh_forwarder.get_tunnel_info(tunnel_id)
        if not tunnel_info:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to retrieve tunnel information"
            )
        
        return SSHConnectionResponse(
            tunnel_id=tunnel_info.tunnel_id,
            host=tunnel_info.host,
            port=tunnel_info.port,
            username=tunnel_info.username,
            status=tunnel_info.status.value,
            created_at=tunnel_info.created_at,
            forwards=[
                {
                    "forward_type": forward.forward_type.value,
                    "listen_host": forward.listen_host,
                    "listen_port": forward.listen_port,
                    "dest_host": forward.dest_host,
                    "dest_port": forward.dest_port,
                    "description": forward.description
                }
                for forward in tunnel_info.forwards
            ]
        )
        
    except Exception as e:
        logger.error(f"Failed to create SSH tunnel: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create SSH tunnel: {str(e)}"
        )


@router.get("/tunnels", response_model=List[SSHConnectionResponse])
async def list_ssh_tunnels(
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> List[SSHConnectionResponse]:
    """List all SSH tunnels."""
    try:
        tunnels = ssh_forwarder.list_tunnels()
        
        return [
            SSHConnectionResponse(
                tunnel_id=tunnel.tunnel_id,
                host=tunnel.host,
                port=tunnel.port,
                username=tunnel.username,
                status=tunnel.status.value,
                created_at=tunnel.created_at,
                forwards=[
                    {
                        "forward_type": forward.forward_type.value,
                        "listen_host": forward.listen_host,
                        "listen_port": forward.listen_port,
                        "dest_host": forward.dest_host,
                        "dest_port": forward.dest_port,
                        "description": forward.description
                    }
                    for forward in tunnel.forwards
                ]
            )
            for tunnel in tunnels
        ]
        
    except Exception as e:
        logger.error(f"Failed to list SSH tunnels: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list SSH tunnels: {str(e)}"
        )


@router.get("/tunnels/{tunnel_id}", response_model=SSHConnectionResponse)
async def get_ssh_tunnel(
    tunnel_id: str,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> SSHConnectionResponse:
    """Get SSH tunnel information."""
    try:
        tunnel_info = ssh_forwarder.get_tunnel_info(tunnel_id)
        if not tunnel_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"SSH tunnel not found: {tunnel_id}"
            )
        
        return SSHConnectionResponse(
            tunnel_id=tunnel_info.tunnel_id,
            host=tunnel_info.host,
            port=tunnel_info.port,
            username=tunnel_info.username,
            status=tunnel_info.status.value,
            created_at=tunnel_info.created_at,
            forwards=[
                {
                    "forward_type": forward.forward_type.value,
                    "listen_host": forward.listen_host,
                    "listen_port": forward.listen_port,
                    "dest_host": forward.dest_host,
                    "dest_port": forward.dest_port,
                    "description": forward.description
                }
                for forward in tunnel_info.forwards
            ]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get SSH tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get SSH tunnel: {str(e)}"
        )


@router.post("/tunnels/{tunnel_id}/connect")
async def connect_ssh_tunnel(
    tunnel_id: str,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Connect an SSH tunnel."""
    try:
        success = await ssh_forwarder.connect_tunnel(tunnel_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to connect SSH tunnel: {tunnel_id}"
            )

        return {"success": True, "message": f"SSH tunnel {tunnel_id} connected"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to connect SSH tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to connect SSH tunnel: {str(e)}"
        )


@router.post("/tunnels/{tunnel_id}/disconnect")
async def disconnect_ssh_tunnel(
    tunnel_id: str,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Disconnect an SSH tunnel."""
    try:
        success = await ssh_forwarder.disconnect_tunnel(tunnel_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to disconnect SSH tunnel: {tunnel_id}"
            )

        return {"success": True, "message": f"SSH tunnel {tunnel_id} disconnected"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to disconnect SSH tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to disconnect SSH tunnel: {str(e)}"
        )


@router.delete("/tunnels/{tunnel_id}")
async def delete_ssh_tunnel(
    tunnel_id: str,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Delete an SSH tunnel."""
    try:
        success = await ssh_forwarder.remove_tunnel(tunnel_id)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"SSH tunnel not found: {tunnel_id}"
            )

        return {"success": True, "message": f"SSH tunnel {tunnel_id} deleted"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete SSH tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete SSH tunnel: {str(e)}"
        )


@router.post("/tunnels/{tunnel_id}/forwards", status_code=status.HTTP_201_CREATED)
async def add_port_forward(
    tunnel_id: str,
    request: SSHForwardRequest,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Add a port forward to an SSH tunnel."""
    try:
        forward_config = ForwardConfig(
            forward_type=request.forward_type,
            listen_host=request.listen_host,
            listen_port=request.listen_port,
            dest_host=request.dest_host,
            dest_port=request.dest_port,
            description=request.description
        )

        forward_index = await ssh_forwarder.add_forward(tunnel_id, forward_config)
        if forward_index < 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"SSH tunnel not found: {tunnel_id}"
            )

        return {
            "success": True,
            "forward_index": forward_index,
            "message": f"Port forward added to tunnel {tunnel_id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add port forward to tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add port forward: {str(e)}"
        )


@router.delete("/tunnels/{tunnel_id}/forwards/{forward_index}")
async def remove_port_forward(
    tunnel_id: str,
    forward_index: int,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Remove a port forward from an SSH tunnel."""
    try:
        success = await ssh_forwarder.remove_forward(tunnel_id, forward_index)
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Port forward not found: tunnel {tunnel_id}, index {forward_index}"
            )

        return {
            "success": True,
            "message": f"Port forward {forward_index} removed from tunnel {tunnel_id}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to remove port forward from tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove port forward: {str(e)}"
        )


@router.post("/tunnels/{tunnel_id}/execute", response_model=SSHCommandResponse)
async def execute_command(
    tunnel_id: str,
    request: SSHCommandRequest,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> SSHCommandResponse:
    """Execute a command through an SSH tunnel."""
    try:
        result = await ssh_forwarder.execute_command(
            tunnel_id=tunnel_id,
            command=request.command,
            timeout=request.timeout
        )

        return SSHCommandResponse(**result)

    except Exception as e:
        logger.error(f"Failed to execute command on tunnel {tunnel_id}: {e}")
        return SSHCommandResponse(
            success=False,
            command=request.command,
            error=str(e)
        )


@router.post("/tunnels/{tunnel_id}/upload")
async def upload_file(
    tunnel_id: str,
    request: SSHFileTransferRequest,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Upload a file through an SSH tunnel."""
    try:
        result = await ssh_forwarder.upload_file(
            tunnel_id=tunnel_id,
            local_path=request.local_path,
            remote_path=request.remote_path
        )

        return result

    except Exception as e:
        logger.error(f"Failed to upload file through tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to upload file: {str(e)}"
        )


@router.post("/tunnels/{tunnel_id}/download")
async def download_file(
    tunnel_id: str,
    request: SSHFileTransferRequest,
    ssh_forwarder: ISSHForwarder = Depends(get_ssh_forwarder)
) -> Dict[str, Any]:
    """Download a file through an SSH tunnel."""
    try:
        result = await ssh_forwarder.download_file(
            tunnel_id=tunnel_id,
            remote_path=request.remote_path,
            local_path=request.local_path
        )

        return result

    except Exception as e:
        logger.error(f"Failed to download file through tunnel {tunnel_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file: {str(e)}"
        )
