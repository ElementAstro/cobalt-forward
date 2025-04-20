import time
import logging
import paramiko
from threading import Lock
from typing import List, Optional, Tuple, Dict, Any
from .config import SSHConfig
from .exceptions import SSHException, SSHPoolError, SSHConnectionError


class SSHConnectionPool:
    """SSH connection pool for managing multiple connections"""

    def __init__(self, config: SSHConfig):
        self.config = config
        self.lock = Lock()
        self.pool_config = config.pool
        self._last_check = time.time()
        self._check_interval = self.pool_config.check_interval if self.pool_config else 60
        self._idle_timeout = self.pool_config.idle_timeout if self.pool_config else 300
        # Tuple of (client, last_used_time)
        self._active_connections: List[Tuple[paramiko.SSHClient, float]] = []
        self._available_connections: List[paramiko.SSHClient] = []
        self._logger = logging.getLogger("SSHConnectionPool")

        # Initialize connection pool
        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the connection pool with minimum connections"""
        if not self.pool_config:
            return

        with self.lock:
            for _ in range(self.pool_config.min_size):
                try:
                    client = self._create_client()
                    self._connect_client(client)
                    self._available_connections.append(client)
                except Exception as e:
                    self._logger.error(f"Failed to initialize connection: {str(e)}")

    def get_client(self) -> paramiko.SSHClient:
        """
        Get a connection from the pool
        
        Returns:
            paramiko.SSHClient: An SSH client connection
            
        Raises:
            SSHPoolError: If pool is full and no connections are available
        """
        with self.lock:
            # Check connection status
            self._check_connections()

            # If available connections exist, return the first one
            if self._available_connections:
                client = self._available_connections.pop(0)
                # Validate connection is active, create new one if not
                if not self._is_connection_alive(client):
                    try:
                        client.close()
                    except:
                        pass
                    client = self._create_and_connect_client()
                self._active_connections.append((client, time.time()))
                return client

            # If active connections are less than max size, create a new one
            if len(self._active_connections) < self.pool_config.max_size:
                client = self._create_and_connect_client()
                self._active_connections.append((client, time.time()))
                return client

            # Pool is full, wait and retry
            raise SSHPoolError(
                f"Connection pool is full. Active connections: {len(self._active_connections)}, Max size: {self.pool_config.max_size}")

    def release_client(self, client: paramiko.SSHClient) -> None:
        """
        Release a client back to the pool
        
        Args:
            client: The SSH client to release
        """
        with self.lock:
            # Remove from active connections
            self._active_connections = [
                (c, t) for c, t in self._active_connections if c != client]

            # If connection is still alive, add to available list
            if self._is_connection_alive(client):
                self._available_connections.append(client)
            else:
                # Otherwise close the invalid connection
                try:
                    client.close()
                except:
                    pass

    def _create_client(self) -> paramiko.SSHClient:
        """
        Create a new SSH client
        
        Returns:
            paramiko.SSHClient: A new SSH client instance
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        return client

    def _create_and_connect_client(self) -> paramiko.SSHClient:
        """
        Create and connect a new client
        
        Returns:
            paramiko.SSHClient: A connected SSH client
        """
        client = self._create_client()
        self._connect_client(client)
        return client

    def _connect_client(self, client: paramiko.SSHClient) -> None:
        """
        Connect an SSH client
        
        Args:
            client: The SSH client to connect
            
        Raises:
            SSHConnectionError: If connection fails
        """
        try:
            connect_kwargs = {
                'hostname': self.config.hostname,
                'port': self.config.port,
                'username': self.config.username,
                'timeout': self.config.timeout,
                'compress': self.config.compress,
                'banner_timeout': self.config.banner_timeout,
                'auth_timeout': self.config.auth_timeout,
            }

            if self.config.password:
                connect_kwargs['password'] = self.config.password
            elif self.config.private_key_path:
                connect_kwargs['key_filename'] = self.config.private_key_path

            client.connect(**connect_kwargs)

            # Set keepalive
            if self.config.keep_alive:
                transport = client.get_transport()
                if transport:
                    transport.set_keepalive(self.config.keep_alive_interval)

        except Exception as e:
            self._logger.error(f"SSH connection failed: {str(e)}")
            raise SSHConnectionError(f"Failed to create SSH connection: {str(e)}") from e

    def _check_connections(self) -> None:
        """Check connection status and clean up invalid connections"""
        now = time.time()
        if now - self._last_check < self._check_interval:
            return

        self._last_check = now

        with self.lock:
            # Check active connections
            active_to_keep = []
            for client, last_used in self._active_connections:
                if now - last_used > self._idle_timeout:
                    # Close idle connections
                    try:
                        client.close()
                    except:
                        pass
                elif self._is_connection_alive(client):
                    active_to_keep.append((client, last_used))
                else:
                    try:
                        client.close()
                    except:
                        pass

            self._active_connections = active_to_keep

            # Check available connections
            available_to_keep = []
            for client in self._available_connections:
                if self._is_connection_alive(client):
                    available_to_keep.append(client)
                else:
                    try:
                        client.close()
                    except:
                        pass

            self._available_connections = available_to_keep

            # Ensure minimum pool size
            while len(self._available_connections) < self.pool_config.min_size:
                try:
                    client = self._create_and_connect_client()
                    self._available_connections.append(client)
                except Exception as e:
                    self._logger.error(f"Failed to replenish connection pool: {str(e)}")
                    break

    def _is_connection_alive(self, client: paramiko.SSHClient) -> bool:
        """
        Check if a connection is alive
        
        Args:
            client: The SSH client to check
            
        Returns:
            bool: True if connection is alive, False otherwise
        """
        try:
            transport = client.get_transport()
            if transport is None or not transport.is_active():
                return False

            # Send empty command to verify connection
            transport.send_ignore()
            return True
        except Exception:
            return False

    def close_all(self) -> None:
        """Close all connections in the pool"""
        with self.lock:
            # Close all active connections
            for client, _ in self._active_connections:
                try:
                    client.close()
                except:
                    pass

            # Close all available connections
            for client in self._available_connections:
                try:
                    client.close()
                except:
                    pass

            self._active_connections = []
            self._available_connections = []
    
    def stats(self) -> Dict[str, int]:
        """
        Get pool statistics
        
        Returns:
            Dict[str, int]: Statistics about the pool
        """
        with self.lock:
            return {
                "active_connections": len(self._active_connections),
                "available_connections": len(self._available_connections),
                "total": len(self._active_connections) + len(self._available_connections),
                "max_size": self.pool_config.max_size if self.pool_config else 0,
                "min_size": self.pool_config.min_size if self.pool_config else 0
            }

    def __del__(self):
        """Destructor to ensure all connections are closed"""
        self.close_all()
