import asyncio
import json
from typing import Optional, Callable, Any, List, Dict, Union, Tuple, Set
import socket
import time
import functools
import random
import struct
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

from ..base import BaseClient
from .config import ClientConfig, UDPSocketOptions
from .state import ClientState, ConnectionStats
from .protocol import PacketProtocol, PacketType
from .buffer import PacketBuffer
from .plugin import UDPPluginManager
from ...utils.error_handler import CircuitBreaker
from ...utils.performance import RateLimiter, async_performance_monitor


class ReliabilityManager:
    """UDP Reliability Manager
    
    Handles reliability, ordered delivery, and fragment reassembly in UDP communication.
    """
    
    def __init__(self, max_retry: int = 3, ack_timeout: float = 0.2):
        """Initialize the reliability manager
        
        Args:
            max_retry: Maximum number of retries
            ack_timeout: Acknowledgment timeout (seconds)
        """
        self.pending_acks: Dict[int, asyncio.Future] = {}  # Messages awaiting acknowledgment
        self.received_seqs: Set[int] = set()  # Received sequence numbers (for deduplication)
        self.max_retry = max_retry
        self.ack_timeout = ack_timeout
        self.next_expected = 0  # Next expected sequence number
        
        # Fragment reassembly
        self.fragments: Dict[int, Dict[int, bytes]] = {}  # original_seq -> (fragment_id -> fragment_data)
        self.fragments_info: Dict[int, int] = {}  # original_seq -> total_fragments
        
        # Ordered delivery
        self.out_of_order_buffer: Dict[int, bytes] = {}  # Out-of-order data buffer
        
        # Statistics
        self.stats = {
            'packets_sent': 0,
            'packets_acked': 0,
            'packets_lost': 0,
            'packets_received': 0,
            'packets_duplicated': 0,
            'packets_out_of_order': 0,
            'fragments_received': 0,
            'fragments_reassembled': 0,
        }
        
    def reset(self):
        """Reset the reliability manager state"""
        for future in self.pending_acks.values():
            if not future.done():
                future.cancel()
                
        self.pending_acks.clear()
        self.received_seqs.clear()
        self.next_expected = 0
        self.fragments.clear()
        self.fragments_info.clear()
        self.out_of_order_buffer.clear()
        
    def create_ack_future(self, sequence: int) -> asyncio.Future:
        """Create a Future to wait for acknowledgment
        
        Args:
            sequence: Packet sequence number
            
        Returns:
            asyncio.Future: Future waiting for acknowledgment
        """
        future = asyncio.Future()
        self.pending_acks[sequence] = future
        return future
        
    def receive_ack(self, sequence: int) -> bool:
        """Handle received acknowledgment packet
        
        Args:
            sequence: Acknowledged sequence number
            
        Returns:
            bool: True if successfully processed
        """
        if sequence in self.pending_acks:
            future = self.pending_acks.pop(sequence)
            if not future.done():
                future.set_result(True)
                self.stats['packets_acked'] += 1
                return True
        return False
        
    def is_duplicate(self, sequence: int) -> bool:
        """Check if it is a duplicate packet
        
        Args:
            sequence: Packet sequence number
            
        Returns:
            bool: True if it is a duplicate packet
        """
        return sequence in self.received_seqs
        
    def mark_received(self, sequence: int) -> None:
        """Mark packet as received
        
        Args:
            sequence: Packet sequence number
        """
        self.received_seqs.add(sequence)
        self.stats['packets_received'] += 1
        
        # Clean up old sequence numbers to prevent infinite growth of the set
        if len(self.received_seqs) > 10000:
            min_seq = min(self.received_seqs)
            self.received_seqs = {seq for seq in self.received_seqs if seq > min_seq - 5000}
            
    def check_and_update_order(self, sequence: int) -> Tuple[bool, Optional[List[bytes]]]:
        """Check packet order and return processable packets
        
        Args:
            sequence: Received packet sequence number
            
        Returns:
            Tuple[bool, Optional[List[bytes]]]: 
                - Whether it's out of order
                - List of packets that can be processed in order (if any)
        """
        # Check if it is the next expected sequence number
        if sequence == self.next_expected:
            self.next_expected = (sequence + 1) & 0xFFFFFFFF
            
            # Check if any buffered out-of-order packets can now be processed
            ready_packets = []
            next_seq = self.next_expected
            
            while next_seq in self.out_of_order_buffer:
                data = self.out_of_order_buffer.pop(next_seq)
                ready_packets.append(data)
                self.next_expected = (next_seq + 1) & 0xFFFFFFFF
                next_seq = self.next_expected
                
            return False, ready_packets if ready_packets else None
        
        # Out-of-order packet
        self.stats['packets_out_of_order'] += 1
        return True, None
        
    def add_fragment(self, original_seq: int, fragment_id: int, total_fragments: int, data: bytes) -> Optional[bytes]:
        """Add packet fragment and attempt reassembly
        
        Args:
            original_seq: Original packet sequence number
            fragment_id: Fragment ID
            total_fragments: Total number of fragments
            data: Fragment data
            
        Returns:
            Optional[bytes]: Reassembled complete data if all fragments are received, otherwise None
        """
        self.stats['fragments_received'] += 1
        
        # Create fragment set (if it doesn't exist)
        if original_seq not in self.fragments:
            self.fragments[original_seq] = {}
            self.fragments_info[original_seq] = total_fragments
            
        # Store fragment
        self.fragments[original_seq][fragment_id] = data
        
        # Check if all fragments have been received
        if len(self.fragments[original_seq]) == total_fragments:
            # Merge all fragments in order
            fragments = []
            for i in range(total_fragments):
                if i not in self.fragments[original_seq]:
                    # Missing fragment, should not happen, but add defensive programming
                    logger.error(f"Missing fragment ID {i} when merging fragments")
                    del self.fragments[original_seq]
                    del self.fragments_info[original_seq]
                    return None
                    
                fragments.append(self.fragments[original_seq][i])
                
            # Clean up resources
            del self.fragments[original_seq]
            del self.fragments_info[original_seq]
            
            # Merge all fragments
            self.stats['fragments_reassembled'] += 1
            return b''.join(fragments)
            
        return None


class UDPClient(BaseClient):
    """Enhanced UDP client implementation
    
    Features:
    - Optional reliable transmission
    - Packet serialization and ordering guarantee
    - Heartbeat mechanism
    - Large data fragmentation transmission
    - Plugin system
    - Compression support
    - Multicast and broadcast
    """

    def __init__(self, config: ClientConfig):
        """Initialize UDP client
        
        Args:
            config: Client configuration
        """
        super().__init__(config)
        self._socket = None
        self._buffer = PacketBuffer(config.read_buffer_size)
        self._protocol = PacketProtocol(
            compression_threshold=4096 if config.compression_enabled else 0, 
            compression_level=config.compression_level
        )
        self._plugin_manager = UDPPluginManager()
        self._rate_limiter = RateLimiter(rate_limit=1000)
        self._circuit_breaker = CircuitBreaker()
        self._heartbeat_task = None
        self._write_task = None
        self._read_task = None
        self._write_queue = asyncio.Queue()
        self._read_queue = asyncio.Queue()
        self._last_activity = time.time()
        self._write_lock = asyncio.Lock()
        self._state = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
        self._remote_addr = None  # Remote address cache
        
        # Reliable transmission support
        self._reliability = ReliabilityManager(
            max_retry=config.retry_policy.max_retries,
            ack_timeout=config.ack_timeout
        )
        self._reliable_delivery = config.reliable_delivery
        self._ordered_delivery = config.ordered_delivery
        
        # Multicast support
        self._multicast_groups = []
        
        # Event handlers
        self._callbacks: Dict[str, List[Callable]] = {
            'connected': [],
            'disconnected': [],
            'message': [],
            'error': [],
            'heartbeat': [],
            'packet_loss': [],
            'out_of_order': []
        }
        
        logger.info(f"UDP client initialized: {config.host}:{config.port}")

    def _create_socket(self) -> socket.socket:
        """Create UDP socket
        
        Returns:
            socket.socket: Configured UDP socket
        """
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        
        # Apply socket options
        if hasattr(self.config, 'socket_options'):
            socket_opts = self.config.socket_options
            
            # Set buffer sizes
            if socket_opts.recvbuf_size > 0:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, socket_opts.recvbuf_size)
            if socket_opts.sendbuf_size > 0:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, socket_opts.sendbuf_size)
                
            # Set address reuse
            if socket_opts.reuseaddr:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                
            # Allow port reuse
            if socket_opts.reuseport and hasattr(socket, 'SO_REUSEPORT'):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                
            # Broadcast support
            if socket_opts.broadcast:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
                
            # Multicast settings
            if hasattr(self.config, 'multicast_support') and self.config.multicast_support:
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 
                               socket_opts.ip_multicast_ttl)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 
                               int(socket_opts.ip_multicast_loop))
                
        sock.setblocking(False)
        return sock

    @property
    def connected(self) -> bool:
        """Check if the client is connected
        
        Returns:
            bool: True if connected
        """
        return self._state == ClientState.CONNECTED

    async def connect(self) -> bool:
        """Establish UDP "connection"
        
        Note: UDP is connectionless, but we set internal state and verify remote host reachability
        
        Returns:
            bool: True if connection is successful
        """
        if self._state == ClientState.CONNECTED:
            return True
            
        if self._state == ClientState.CONNECTING:
            # Wait for existing connection attempt to complete
            for _ in range(10):
                await asyncio.sleep(0.1)
                if self._state == ClientState.CONNECTED:
                    return True
                elif self._state != ClientState.CONNECTING:
                    break
                    
        self._state = ClientState.CONNECTING
        
        try:
            # Create socket
            self._socket = self._create_socket()
            
            # Save remote address
            self._remote_addr = (self.config.host, self.config.port)
            
            # For multicast, join groups
            if hasattr(self.config, 'multicast_support') and self.config.multicast_support:
                multicast_groups = self.config.multicast_groups
                for group in multicast_groups:
                    mreq = struct.pack('4sl', socket.inet_aton(group), socket.INADDR_ANY)
                    self._socket.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                    self._multicast_groups.append(group)
                    logger.info(f"Joined multicast group: {group}")
                    
            # Verify remote host reachability (for connection-oriented virtual connection)
            if not self._verify_remote_reachable():
                raise ConnectionError(f"Cannot connect to {self.config.host}:{self.config.port}")
                
            self._state = ClientState.CONNECTED
            self._stats.connection_time = time.time()
            self._last_activity = time.time()
            
            # Start background tasks
            self._start_background_tasks()
            
            # Initialize plugins
            await self._plugin_manager.initialize(self)
            
            # Call connection callbacks
            await self._trigger_callbacks('connected')
            
            logger.info(f"Connected to UDP endpoint: {self.config.host}:{self.config.port}")
            return True
            
        except Exception as e:
            self._state = ClientState.ERROR
            self._stats.errors += 1
            logger.error(f"UDP connection failed: {str(e)}")
            
            # Call error callbacks
            await self._trigger_callbacks('error', e)
            
            # If auto-reconnect is enabled, schedule reconnection
            if getattr(self.config, 'auto_reconnect', True):
                asyncio.create_task(self._reconnect_loop())
                
            return False

    def _verify_remote_reachable(self) -> bool:
        """Verify remote UDP endpoint reachability
        
        Due to UDP's connectionless nature, this is a best-effort verification
        
        Returns:
            bool: True if the remote host is reachable
        """
        try:
            # Send a small verification packet
            verification_packet = self._protocol.create_packet(
                PacketType.CONTROL, 
                {'command': 'verify', 'timestamp': int(time.time())}
            )
            self._socket.sendto(verification_packet, self._remote_addr)
            return True
        except Exception as e:
            logger.error(f"Failed to verify remote host reachability: {e}")
            return False

    def _start_background_tasks(self):
        """Start background processing tasks"""
        if not self._write_task or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_loop())
            
        if not self._read_task or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())
            
        if hasattr(self.config, 'heartbeat_enabled') and self.config.heartbeat_enabled:
            if not self._heartbeat_task or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        """Disconnect and clean up resources"""
        # Cancel background tasks
        tasks = []
        
        if self._write_task and not self._write_task.done():
            self._write_task.cancel()
            tasks.append(self._write_task)
            
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            tasks.append(self._read_task)
            
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            tasks.append(self._heartbeat_task)
            
        # Wait for tasks to finish cancellation
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        # Close connection
        if self._socket:
            try:
                # Leave multicast groups
                if self._multicast_groups:
                    for group in self._multicast_groups:
                        try:
                            mreq = struct.pack('4sl', socket.inet_aton(group), socket.INADDR_ANY)
                            self._socket.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
                        except Exception as e:
                            logger.warning(f"Failed to leave multicast group {group}: {e}")
                
                # Close socket
                self._socket.close()
            except Exception as e:
                logger.warning(f"Error closing socket: {e}")
            finally:
                self._socket = None
                
        # Reset reliability manager
        self._reliability.reset()
        
        # Update state
        self._state = ClientState.DISCONNECTED
        
        # Call disconnection callbacks
        await self._trigger_callbacks('disconnected')
        
        logger.info("UDP client disconnected")

    @async_performance_monitor
    async def _write_loop(self):
        """Send loop"""
        while self._state == ClientState.CONNECTED:
            try:
                # Get data to send
                data = await self._write_queue.get()
                
                # If socket is unavailable, skip
                if not self._socket:
                    continue
                    
                # Send data
                try:
                    self._socket.sendto(data, self._remote_addr)
                    self._stats.bytes_sent += len(data)
                    self._stats.packets_sent += 1
                    self._last_activity = time.time()
                except Exception as e:
                    logger.error(f"Failed to send data: {e}")
                    self._stats.errors += 1
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Write loop error: {e}")
                await asyncio.sleep(0.1)  # Short pause on error
                
        logger.debug("Write loop exited")

    @async_performance_monitor
    async def _read_loop(self):
        """Receive loop"""
        while self._state == ClientState.CONNECTED:
            try:
                # If socket is unavailable, wait
                if not self._socket:
                    await asyncio.sleep(0.1)
                    continue
                    
                # Non-blocking receive data
                try:
                    # Create receive event
                    loop = asyncio.get_event_loop()
                    future = loop.create_future()
                    loop.add_reader(self._socket.fileno(), self._socket_ready, future)
                    
                    try:
                        # Wait for data to be available
                        await asyncio.wait_for(future, timeout=1.0)
                        data, addr = self._socket.recvfrom(self.config.max_datagram_size)
                        
                        # Update statistics
                        self._stats.bytes_received += len(data)
                        self._last_activity = time.time()
                        
                        # Process packet
                        await self._process_received_data(data, addr)
                        
                    except asyncio.TimeoutError:
                        # Normal timeout, continue loop
                        pass
                    finally:
                        # Remove reader
                        loop.remove_reader(self._socket.fileno())
                        
                except Exception as e:
                    logger.error(f"Failed to receive data: {e}")
                    self._stats.errors += 1
                    await asyncio.sleep(0.1)  # Short pause on error
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Read loop error: {e}")
                await asyncio.sleep(0.1)
                
        logger.debug("Read loop exited")

    def _socket_ready(self, future):
        """Socket readable event callback"""
        if not future.done():
            future.set_result(None)

    async def _process_received_data(self, data: bytes, addr: Tuple[str, int]) -> None:
        """Process received UDP packet
        
        Args:
            data: Received data
            addr: Sender address (ip, port)
        """
        # Process through plugins
        data = await self._plugin_manager.run_pre_receive(data)
        if data is None:
            return
            
        try:
            # Parse packet
            packet_type, body, sequence, flags, _ = self._protocol.unpack(data)
            
            # Handle different packet types
            if packet_type == PacketType.ACK:
                # Handle acknowledgment packet
                ack_data = self._protocol.try_parse_json(body)
                if isinstance(ack_data, dict) and 'ack_sequence' in ack_data:
                    self._reliability.receive_ack(ack_data['ack_sequence'])
                    
            elif packet_type == PacketType.HEARTBEAT:
                # Handle heartbeat packet - automatically reply with an ACK packet
                ack_packet = self._protocol.create_ack(sequence)
                await self._write_queue.put(ack_packet)
                
            elif packet_type == PacketType.FRAGMENT:
                # Handle fragment packet
                # Separate header info and data (using null byte)
                parts = body.split(b'\0', 1)
                if len(parts) == 2:
                    try:
                        header = json.loads(parts[0].decode('utf-8'))
                        fragment_data = parts[1]
                        
                        # Add fragment and attempt reassembly
                        complete_data = self._reliability.add_fragment(
                            header['original_seq'],
                            header['fragment_id'],
                            header['total_fragments'],
                            fragment_data
                        )
                        
                        # If all fragments have been received
                        if complete_data:
                            # Process reassembled complete data
                            await self._process_data_packet(
                                PacketType.DATA,
                                complete_data,
                                header['original_seq']
                            )
                            
                    except json.JSONDecodeError:
                        logger.error("Failed to parse fragment header")
                    except KeyError as e:
                        logger.error(f"Fragment header missing required field: {e}")
                        
            else:
                # Handle normal data packet
                await self._process_data_packet(packet_type, body, sequence)
                
            # Plugin post-processing
            await self._plugin_manager.run_post_receive(body)
            
        except ValueError as e:
            logger.error(f"Invalid packet: {e}")
        except Exception as e:
            logger.error(f"Error processing packet: {e}")
            self._stats.errors += 1

    async def _process_data_packet(self, packet_type: PacketType, body: bytes, sequence: int) -> None:
        """Process data and control packets
        
        Args:
            packet_type: Packet type
            body: Data content
            sequence: Sequence number
        """
        # If reliable and ordered transmission is enabled, perform reliability checks
        if self._reliable_delivery:
            # Check if it is a duplicate packet
            if self._reliability.is_duplicate(sequence):
                self._stats.duplicate_packets += 1
                logger.debug(f"Received duplicate packet, sequence: {sequence}")
                
                # Still need to reply with ACK in case the original ACK was lost
                ack_packet = self._protocol.create_ack(sequence)
                await self._write_queue.put(ack_packet)
                return
                
            # Send acknowledgment packet
            ack_packet = self._protocol.create_ack(sequence)
            await self._write_queue.put(ack_packet)
            
            # Mark as received
            self._reliability.mark_received(sequence)
            
            # When ordered delivery is enabled, check sequence order
            if self._ordered_delivery:
                is_out_of_order, ready_packets = self._reliability.check_and_update_order(sequence)
                
                if is_out_of_order:
                    # Out-of-order packet, buffer and wait
                    logger.debug(f"Received out-of-order packet, sequence: {sequence}, expected: {self._reliability.next_expected}")
                    await self._trigger_callbacks('out_of_order', self._reliability.next_expected, sequence)
                    return
                
                # Process current packet
                await self._trigger_callbacks('message', packet_type, body)
                
                # Process packets in the out-of-order buffer that can now be processed
                if ready_packets:
                    for packet_data in ready_packets:
                        await self._trigger_callbacks('message', packet_type, packet_data)
                    
                return
                
        # Call message callback (for unreliable or reliable but unordered transmission)
        await self._trigger_callbacks('message', packet_type, body)

    async def _heartbeat_loop(self):
        """Heartbeat sending loop"""
        while self._state == ClientState.CONNECTED:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                
                # Check if sending heartbeat is allowed
                allow_heartbeat = await self._plugin_manager.run_pre_heartbeat()
                if not allow_heartbeat:
                    continue
                    
                # Send heartbeat packet
                success = await self.send_heartbeat()
                
                # Call heartbeat callback and plugin post-processing
                if success:
                    await self._trigger_callbacks('heartbeat')
                await self._plugin_manager.run_post_heartbeat(success)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}")
                
        logger.debug("Heartbeat loop exited")

    async def send_heartbeat(self) -> bool:
        """Send heartbeat packet
        
        Returns:
            bool: True if sending is successful
        """
        if self._state == ClientState.CONNECTED:
            try:
                heartbeat_data = self._protocol.create_heartbeat()
                await self._write_queue.put(heartbeat_data)
                return True
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
                self._stats.errors += 1
                return False
        return False

    async def _reconnect_loop(self):
        """Reconnection loop (exponential backoff)"""
        retry = 0
        while self._state not in [ClientState.CONNECTED, ClientState.CLOSED]:
            try:
                # Calculate backoff delay
                if hasattr(self.config, 'retry_policy'):
                    delay = self.config.retry_policy.get_retry_delay(retry)
                else:
                    # Default backoff
                    delay = min(30, (2 ** retry) * 0.1 + random.uniform(0, 0.1))
                    
                retry += 1
                logger.info(f"Reconnect attempt {retry} scheduled in {delay:.2f} seconds")
                await asyncio.sleep(delay)
                
                logger.info(f"Attempting reconnect (attempt {retry})")
                success = await self.connect()
                
                if success:
                    logger.info(f"Reconnect successful after {retry} attempts")
                    self._stats.reconnects += 1
                    break
                    
                # Check if maximum retry count is reached
                if hasattr(self.config, 'retry_policy') and retry >= self.config.retry_policy.max_retries:
                    logger.error(f"Reconnect failed, maximum attempts {retry} reached, giving up")
                    break
                    
            except asyncio.CancelledError:
                logger.debug("Reconnect loop cancelled")
                break
            except Exception as e:
                logger.error(f"Reconnect loop error: {e}")
                self._stats.errors += 1
                
        logger.debug("Reconnect loop exited")

    async def send(self, data: Union[bytes, dict], packet_type: PacketType = PacketType.DATA) -> bool:
        """Send packet
        
        Args:
            data: Data to send (bytes or dict)
            packet_type: Packet type
            
        Returns:
            bool: True if sending is successful
            
        Raises:
            ConnectionError: If the client is not connected
        """
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")
            
        # Data size check
        if isinstance(data, bytes) and len(data) > self.config.max_packet_size:
            return await self._send_large_data(data, packet_type)
            
        # Process through plugins
        if isinstance(data, bytes):
            processed_data = await self._plugin_manager.run_pre_send(data)
            if processed_data is None:
                return False  # Plugin rejected sending
            data = processed_data
            
        # Create packet
        sequence = self._protocol.get_next_sequence()
        packet = self._protocol.create_packet(packet_type, data, sequence)
        
        success = False
        try:
            # Check if reliable transmission is needed
            if self._reliable_delivery and packet_type != PacketType.ACK:
                success = await self._send_reliable(packet, sequence)
            else:
                # Normal send
                await self._write_queue.put(packet)
                success = True
                
            # Plugin post-processing
            if isinstance(data, bytes):
                await self._plugin_manager.run_post_send(data, success)
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to send data: {e}")
            self._stats.errors += 1
            return False

    async def _send_large_data(self, data: bytes, packet_type: PacketType) -> bool:
        """Send large data, automatically fragmenting
        
        Args:
            data: Large data to send
            packet_type: Packet type
            
        Returns:
            bool: True if sending is successful
        """
        try:
            # Fragment size considering header overhead
            max_fragment_size = self.config.max_datagram_size - 100
            
            # Calculate fragmentation info
            data_len = len(data)
            total_fragments = (data_len + max_fragment_size - 1) // max_fragment_size
            original_sequence = self._protocol.get_next_sequence()
            
            logger.debug(f"Fragmenting {data_len} bytes of data into {total_fragments} fragments")
            
            # Create and send each fragment
            all_success = True
            for i in range(total_fragments):
                start = i * max_fragment_size
                end = min(start + max_fragment_size, data_len)
                fragment_data = data[start:end]
                
                # Create fragment packet
                fragment_packet = self._protocol.create_fragment_packet(
                    fragment_data, i, total_fragments, original_sequence
                )
                
                # Send fragment (using reliable transmission if enabled)
                if self._reliable_delivery:
                    fragment_sequence = ((i << 24) | (original_sequence & 0x00FFFFFF))
                    success = await self._send_reliable(fragment_packet, fragment_sequence)
                else:
                    await self._write_queue.put(fragment_packet)
                    success = True
                    
                if not success:
                    all_success = False
                    
                # Avoid sending too fast causing network congestion
                await asyncio.sleep(0.001)
                
            return all_success
            
        except Exception as e:
            logger.error(f"Failed to send large data: {e}")
            return False

    async def _send_reliable(self, packet: bytes, sequence: int) -> bool:
        """Reliably send packet, waiting for acknowledgment
        
        Args:
            packet: Packet to send
            sequence: Packet sequence number
            
        Returns:
            bool: True if sent and acknowledged successfully
        """
        future = self._reliability.create_ack_future(sequence)
        
        for attempt in range(self._reliability.max_retry + 1):
            if attempt > 0:
                logger.debug(f"Retrying send packet, sequence: {sequence}, attempt: {attempt}")
                
            try:
                # Send packet
                await self._write_queue.put(packet)
                
                # Wait for acknowledgment
                try:
                    await asyncio.wait_for(future, timeout=self._reliability.ack_timeout)
                    return True  # Successfully received acknowledgment
                except asyncio.TimeoutError:
                    # Timeout waiting for acknowledgment, need to retry
                    if attempt == self._reliability.max_retry:
                        logger.warning(f"Maximum retry attempts exhausted, packet lost, sequence: {sequence}")
                        self._reliability.stats['packets_lost'] += 1
                        await self._trigger_callbacks('packet_loss', sequence)
                        await self._plugin_manager.run_on_packet_loss(sequence)
                        return False
                        
            except Exception as e:
                logger.error(f"Reliable send error: {e}")
                return False
                
        return False

    async def receive(self) -> Optional[Tuple[PacketType, bytes]]:
        """Receive data
        
        Returns:
            Optional tuple (packet_type, data), None if queue is empty
            
        Raises:
            ConnectionError: If the client is not connected
        """
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")
            
        try:
            # Wait for data to be available
            packet_type, data = await asyncio.wait_for(self._read_queue.get(), timeout=0.1)
            return packet_type, data
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Failed to receive data: {e}")
            self._stats.errors += 1
            return None

    async def send_to_group(self, group: str, data: Union[bytes, dict], 
                           packet_type: PacketType = PacketType.DATA) -> bool:
        """Send data to multicast group
        
        Args:
            group: Multicast group address
            data: Data to send
            packet_type: Packet type
            
        Returns:
            bool: True if sending is successful
        """
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")
            
        # Create packet
        packet = self._protocol.create_packet(packet_type, data)
        
        try:
            # Save original remote address
            original_addr = self._remote_addr
            
            # Set multicast group address
            self._remote_addr = (group, self.config.port)
            
            # Send packet
            await self._write_queue.put(packet)
            
            # Restore original remote address
            self._remote_addr = original_addr
            
            return True
        except Exception as e:
            logger.error(f"Failed to send to multicast group: {e}")
            return False

    async def broadcast(self, port: int, data: Union[bytes, dict], 
                       packet_type: PacketType = PacketType.DATA) -> bool:
        """Broadcast data to local network
        
        Args:
            port: Target port
            data: Data to send
            packet_type: Packet type
            
        Returns:
            bool: True if sending is successful
        """
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")
            
        # Check if socket supports broadcast
        if not hasattr(self.config, 'allow_broadcast') or not self.config.allow_broadcast:
            logger.error("Broadcast not enabled, please set allow_broadcast=True in config")
            return False
            
        # Create packet
        packet = self._protocol.create_packet(packet_type, data)
        
        try:
            # Save original remote address
            original_addr = self._remote_addr
            
            # Set broadcast address
            self._remote_addr = ('<broadcast>', port)
            
            # Send packet
            await self._write_queue.put(packet)
            
            # Restore original remote address
            self._remote_addr = original_addr
            
            return True
        except Exception as e:
            logger.error(f"Broadcast failed: {e}")
            return False

    def on(self, event: str, callback: Callable) -> None:
        """Register event callback
        
        Args:
            event: Event name
            callback: Callback function
            
        Raises:
            ValueError: If event is unknown
        """
        if event in self._callbacks:
            self._callbacks[event].append(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def off(self, event: str, callback: Callable) -> None:
        """Unregister event callback
        
        Args:
            event: Event name
            callback: Callback function
            
        Raises:
            ValueError: If event is unknown
        """
        if event in self._callbacks:
            if callback in self._callbacks[event]:
                self._callbacks[event].remove(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    async def _trigger_callbacks(self, event: str, *args) -> None:
        """Trigger event callbacks
        
        Args:
            event: Event name
            *args: Arguments to pass to the callback
        """
        if event not in self._callbacks:
            return
            
        for callback in self._callbacks[event]:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(self, *args)
                else:
                    callback(self, *args)
            except Exception as e:
                logger.error(f"Error in callback for event {event}: {e}")

    def register_plugin(self, plugin_class) -> bool:
        """Register plugin
        
        Args:
            plugin_class: Plugin class
            
        Returns:
            bool: True if registration is successful
        """
        return self._plugin_manager.register(plugin_class)

    def unregister_plugin(self, plugin_name: str) -> bool:
        """Unregister plugin
        
        Args:
            plugin_name: Plugin name
            
        Returns:
            bool: True if unregistration is successful
        """
        return self._plugin_manager.unregister(plugin_name)

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics
        
        Returns:
            Dictionary containing statistics
        """
        return {
            "state": self._state.name,
            "connected": self.connected,
            "host": self.config.host,
            "port": self.config.port,
            "last_activity": self._last_activity,
            "idle_time": time.time() - self._last_activity,
            "reliability": {
                "reliable_delivery": self._reliable_delivery,
                "ordered_delivery": self._ordered_delivery,
                **self._reliability.stats
            },
            "stats": self._stats.to_dict()
        }

    async def __aenter__(self):
        """Async context manager entry"""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.disconnect()