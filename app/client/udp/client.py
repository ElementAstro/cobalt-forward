import asyncio
import json
from typing import Optional, Callable, Any, List, Dict, Union, Tuple, Set, Awaitable, Type, TypeVar, Coroutine
import socket
import time
# import functools # Unused
import random
import struct
# import uuid # Unused
# from collections import deque # Unused
# from concurrent.futures import ThreadPoolExecutor # Unused
from loguru import logger
from types import TracebackType


from ..base import BaseClient
# Assuming ClientConfig is correctly defined with all necessary fields
from .config import ClientConfig
from .state import ClientState, ConnectionStats
from .protocol import PacketProtocol, PacketType
from .buffer import PacketBuffer
# Assuming UDPPlugin is the base for plugins
from .plugin import UDPPluginManager, UDPPlugin
from ...utils.error_handler import CircuitBreaker
from ...utils.performance import RateLimiter, async_performance_monitor

_T = TypeVar('_T')


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
        self.pending_acks: Dict[int, asyncio.Future[bool]
                                ] = {}  # Messages awaiting acknowledgment
        # Received sequence numbers (for deduplication)
        self.received_seqs: Set[int] = set()
        self.max_retry = max_retry
        self.ack_timeout = ack_timeout
        self.next_expected: int = 0  # Next expected sequence number

        # Fragment reassembly
        # original_seq -> (fragment_id -> fragment_data)
        self.fragments: Dict[int, Dict[int, bytes]] = {}
        # original_seq -> total_fragments
        self.fragments_info: Dict[int, int] = {}

        # Ordered delivery
        # Out-of-order data buffer
        self.out_of_order_buffer: Dict[int, bytes] = {}

        # Statistics
        self.stats: Dict[str, int] = {
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
        future: asyncio.Future[bool]
        for future in self.pending_acks.values():
            if not future.done():
                future.cancel()

        self.pending_acks.clear()
        self.received_seqs.clear()
        self.next_expected = 0
        self.fragments.clear()
        self.fragments_info.clear()
        self.out_of_order_buffer.clear()

    def create_ack_future(self, sequence: int) -> asyncio.Future[bool]:
        """Create a Future to wait for acknowledgment

        Args:
            sequence: Packet sequence number

        Returns:
            asyncio.Future[bool]: Future waiting for acknowledgment
        """
        future: asyncio.Future[bool] = asyncio.Future()
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
        if len(self.received_seqs) > 10000:  # Configurable threshold
            min_seq_val = min(self.received_seqs) if self.received_seqs else 0
            # Keep a window of sequence numbers
            # Configurable window
            self.received_seqs = {
                seq for seq in self.received_seqs if seq > min_seq_val - 5000}

    def check_and_update_order(self, sequence: int, data: bytes) -> Tuple[bool, Optional[List[bytes]]]:
        """Check packet order and return processable packets.

        Args:
            sequence: Received packet sequence number.
            data: The data of the current packet with `sequence`.

        Returns:
            Tuple[bool, Optional[List[bytes]]]: 
                - is_out_of_order_and_buffered: True if current packet is out of order and has been buffered.
                - processable_packets: List of packets (bytes) that can be processed now in order.
                                       Includes the current packet if it's in order, plus any from the buffer.
                                       None if the current packet is out of order and buffered, or too old.
        """
        processable_packets: List[bytes] = []

        if sequence == self.next_expected:
            processable_packets.append(data)
            self.next_expected = (self.next_expected +
                                  1) & 0xFFFFFFFF  # Wrap around

            # Check buffer for subsequent packets
            current_seq = self.next_expected
            while current_seq in self.out_of_order_buffer:
                processable_packets.append(
                    self.out_of_order_buffer.pop(current_seq))
                self.next_expected = (current_seq + 1) & 0xFFFFFFFF
                current_seq = self.next_expected
            return False, processable_packets
        elif sequence > self.next_expected:
            # Future packet, buffer it
            self.out_of_order_buffer[sequence] = data
            self.stats['packets_out_of_order'] += 1
            return True, None  # Buffered, no packets to process now
        else:  # sequence < self.next_expected
            # This is an old packet (already processed or part of a past sequence).
            # Typically, is_duplicate should catch this. If it gets here, log it.
            logger.debug(
                f"Received old sequence packet {sequence}, expected {self.next_expected}. Likely duplicate.")
            # Or a different counter like 'old_packets_ignored'
            self.stats['packets_out_of_order'] += 1
            return True, None  # Out of order (old), no packets to process now

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

        if original_seq not in self.fragments_info:
            self.fragments_info[original_seq] = total_fragments
            self.fragments[original_seq] = {}

        # Store fragment, only if total_fragments matches what we expect for this original_seq
        if self.fragments_info[original_seq] != total_fragments:
            logger.warning(
                f"Fragment for original_seq {original_seq} has mismatched total_fragments. Expected {self.fragments_info[original_seq]}, got {total_fragments}. Discarding.")
            # Clean up to avoid inconsistent state
            if original_seq in self.fragments:
                del self.fragments[original_seq]
            if original_seq in self.fragments_info:
                del self.fragments_info[original_seq]
            return None

        self.fragments[original_seq][fragment_id] = data

        # Check if all fragments have been received
        if len(self.fragments[original_seq]) == total_fragments:
            reassembled_frags: List[bytes] = []
            for i in range(total_fragments):
                if i not in self.fragments[original_seq]:
                    logger.error(
                        f"Missing fragment ID {i} for original_seq {original_seq} when reassembling. Expected {total_fragments} fragments.")
                    del self.fragments[original_seq]
                    del self.fragments_info[original_seq]
                    return None

                reassembled_frags.append(self.fragments[original_seq][i])

            del self.fragments[original_seq]
            del self.fragments_info[original_seq]

            self.stats['fragments_reassembled'] += 1
            return b''.join(reassembled_frags)

        return None


# Define a more specific type for callbacks if possible
# SelfType = TypeVar('SelfType', bound='UDPClient') # For self argument in callbacks
# EventCallbackType = Callable[[SelfType, Any], Awaitable[None]] # For async
# General async callback
EventCallbackType = Callable[..., Coroutine[Any, Any, None]]
SyncEventCallbackType = Callable[..., None]  # General sync callback
AnyEventCallbackType = Union[EventCallbackType, SyncEventCallbackType]


class UDPClient(BaseClient):
    """Enhanced UDP client implementation"""

    def __init__(self, config: ClientConfig):
        super().__init__(config)
        self.config: ClientConfig = config
        self._socket: Optional[socket.socket] = None
        self._buffer = PacketBuffer(config.read_buffer_size)
        self._protocol = PacketProtocol(
            compression_threshold=config.compression_threshold,  # Assuming on ClientConfig
            compression_level=config.compression_level
        )
        self._plugin_manager = UDPPluginManager()
        self._rate_limiter = RateLimiter(
            rate_limit=config.rate_limit if hasattr(config, 'rate_limit') else 1000)
        self._circuit_breaker = CircuitBreaker(
            max_failures=config.circuit_breaker_max_failures if hasattr(
                config, 'circuit_breaker_max_failures') else 5,
            reset_timeout=config.circuit_breaker_reset_timeout if hasattr(
                config, 'circuit_breaker_reset_timeout') else 30
        )
        self._heartbeat_task: Optional[asyncio.Task[None]] = None
        self._write_task: Optional[asyncio.Task[None]] = None
        self._read_task: Optional[asyncio.Task[None]] = None
        self._write_queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._read_queue: asyncio.Queue[Tuple[PacketType, bytes]] = asyncio.Queue(
        )
        self._last_activity: float = time.time()
        self._write_lock = asyncio.Lock()  # Consider if still needed with Queue
        self._state: ClientState = ClientState.DISCONNECTED
        self._stats = ConnectionStats()
        self._remote_addr: Optional[Tuple[str, int]] = None

        self._reliability = ReliabilityManager(
            max_retry=config.retry_policy.max_retries if config.retry_policy else 3,
            ack_timeout=config.ack_timeout
        )
        self._reliable_delivery: bool = config.reliable_delivery
        self._ordered_delivery: bool = config.ordered_delivery

        self._multicast_groups: List[str] = []

        self._callbacks: Dict[str, List[AnyEventCallbackType]] = {
            'connected': [], 'disconnected': [], 'message': [], 'error': [],
            'heartbeat': [], 'packet_loss': [], 'out_of_order': []
        }

        logger.info(f"UDP client initialized: {config.host}:{config.port}")

    def _create_socket(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        if self.config.socket_options:
            opts = self.config.socket_options
            if opts.recvbuf_size > 0:
                sock.setsockopt(socket.SOL_SOCKET,
                                socket.SO_RCVBUF, opts.recvbuf_size)
            if opts.sendbuf_size > 0:
                sock.setsockopt(socket.SOL_SOCKET,
                                socket.SO_SNDBUF, opts.sendbuf_size)
            if opts.reuseaddr:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if opts.reuseport and hasattr(socket, 'SO_REUSEPORT'):
                # type: ignore[attr-defined]
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            if opts.broadcast:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

            if self.config.multicast_support:
                sock.setsockopt(socket.IPPROTO_IP,
                                socket.IP_MULTICAST_TTL, opts.ip_multicast_ttl)
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, int(
                    opts.ip_multicast_loop))

        sock.setblocking(False)
        return sock

    async def connect(self) -> bool:
        if self._state == ClientState.CONNECTED:
            self.connected = True
            return True

        if self._state == ClientState.CONNECTING:
            # Use config.timeout
            for _ in range(int(self.config.timeout / 0.1) if self.config.timeout > 0 else 10):
                await asyncio.sleep(0.1)
                if self._state == ClientState.CONNECTED:
                    self.connected = True
                    return True
                if self._state != ClientState.CONNECTING:
                    break

        self._state = ClientState.CONNECTING
        self.connected = False

        try:
            self._socket = self._create_socket()
            self._remote_addr = (self.config.host, self.config.port)

            if self.config.multicast_support and self.config.multicast_groups:
                for group_addr in self.config.multicast_groups:
                    if self._socket:  # Ensure socket exists
                        mreq = struct.pack('4sl', socket.inet_aton(
                            group_addr), socket.INADDR_ANY)
                        self._socket.setsockopt(
                            socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
                        self._multicast_groups.append(group_addr)
                        logger.info(f"Joined multicast group: {group_addr}")

            if not self._verify_remote_reachable():
                raise ConnectionError(
                    f"Cannot connect to {self.config.host}:{self.config.port} (verification failed)")

            self._state = ClientState.CONNECTED
            self.connected = True
            self._stats.connection_time = time.time()
            self._last_activity = time.time()

            self._start_background_tasks()
            await self._plugin_manager.initialize(self)
            await self._trigger_callbacks('connected')
            logger.info(
                f"Connected to UDP endpoint: {self.config.host}:{self.config.port}")
            return True

        except Exception as e:
            self._state = ClientState.ERROR
            self.connected = False
            self._stats.errors += 1
            logger.error(f"UDP connection failed: {str(e)}", exc_info=True)
            await self._trigger_callbacks('error', e)
            if self.config.auto_reconnect:
                asyncio.create_task(self._reconnect_loop())
            return False

    def _verify_remote_reachable(self) -> bool:
        if not self._socket or not self._remote_addr:
            return False
        try:
            verification_packet = self._protocol.create_packet(
                PacketType.CONTROL, {'command': 'verify'})
            self._socket.sendto(verification_packet, self._remote_addr)
            return True
        except Exception as e:
            logger.warning(f"Failed to send verification packet: {e}")
            return False

    def _start_background_tasks(self):
        if not self._write_task or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_loop())
        if not self._read_task or self._read_task.done():
            self._read_task = asyncio.create_task(self._read_loop())
        if self.config.heartbeat_enabled and (not self._heartbeat_task or self._heartbeat_task.done()):
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def disconnect(self) -> None:
        logger.info("UDP client disconnecting...")
        current_state = self._state
        self._state = ClientState.DISCONNECTING  # Indicate disconnecting
        self.connected = False

        tasks_to_await: List[asyncio.Task[Any]] = []
        if self._write_task and not self._write_task.done():
            self._write_task.cancel()
            tasks_to_await.append(self._write_task)
        if self._read_task and not self._read_task.done():
            self._read_task.cancel()
            tasks_to_await.append(self._read_task)
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            tasks_to_await.append(self._heartbeat_task)

        if tasks_to_await:
            await asyncio.gather(*tasks_to_await, return_exceptions=True)

        if self._socket:
            try:
                if self.config.multicast_support and self.config.multicast_groups:
                    for group_addr in self._multicast_groups:
                        try:
                            mreq = struct.pack('4sl', socket.inet_aton(
                                group_addr), socket.INADDR_ANY)
                            self._socket.setsockopt(
                                socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
                        except Exception as e_mc:
                            logger.warning(
                                f"Failed to leave multicast group {group_addr}: {e_mc}")
                self._socket.close()
            except Exception as e_sock:
                logger.warning(f"Error closing socket: {e_sock}")
            finally:
                self._socket = None

        self._reliability.reset()
        self._state = ClientState.DISCONNECTED

        # Only trigger 'disconnected' if it was previously connected or connecting
        if current_state in [ClientState.CONNECTED, ClientState.CONNECTING, ClientState.ERROR]:
            await self._trigger_callbacks('disconnected')
        logger.info("UDP client disconnected.")

    @async_performance_monitor
    async def _write_loop(self):
        while self._state not in [ClientState.DISCONNECTED, ClientState.CLOSED, ClientState.DISCONNECTING]:
            try:
                data_to_send = await self._write_queue.get()
                if not self._socket or not self._remote_addr:
                    logger.warning(
                        "Write loop: Socket/address gone, packet dropped.")
                    self._write_queue.task_done()
                    continue
                try:
                    self._socket.sendto(data_to_send, self._remote_addr)
                    self._stats.bytes_sent += len(data_to_send)
                    self._stats.packets_sent += 1
                    self._last_activity = time.time()
                except Exception as e_send:
                    logger.error(f"Socket sendto error: {e_send}")
                    self._stats.errors += 1
                    # Consider circuit breaker or state change on persistent send errors
                finally:
                    self._write_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e_loop:
                logger.error(f"Write loop error: {e_loop}")
                await asyncio.sleep(0.01)
        logger.debug("Write loop exited")

    @async_performance_monitor
    async def _read_loop(self):
        while self._state == ClientState.CONNECTED:
            try:
                if not self._socket:
                    await asyncio.sleep(0.1)
                    continue
                loop = asyncio.get_event_loop()
                can_read_future: asyncio.Future[None] = loop.create_future()
                fd = self._socket.fileno()
                if fd == -1:  # Socket closed
                    logger.warning(
                        "Read loop: socket file descriptor is -1 (closed).")
                    await self._handle_disconnect_internally()
                    break
                loop.add_reader(fd, self._socket_ready, can_read_future)
                try:
                    await asyncio.wait_for(can_read_future, timeout=1.0)
                    if self._socket:  # Re-check after await
                        received_data, sender_addr = self._socket.recvfrom(
                            self.config.max_datagram_size)
                        self._stats.bytes_received += len(received_data)
                        self._last_activity = time.time()
                        await self._process_received_data(received_data, sender_addr)
                except asyncio.TimeoutError:
                    pass  # Normal read timeout
                except BlockingIOError:
                    pass  # Can happen if not truly ready
                except ConnectionResetError as e_conn_reset:  # Windows specific
                    logger.warning(
                        f"Read loop: ConnectionResetError: {e_conn_reset}. Disconnecting.")
                    await self._handle_disconnect_internally()
                    break
                except OSError as e_os:  # Catch other OS errors like "Bad file descriptor"
                    logger.error(
                        f"Read loop: OSError: {e_os}. Disconnecting if socket related.")
                    # EBADF (Bad file descriptor)
                    if fd == -1 or (e_os.errno == socket.EBADF):
                        await self._handle_disconnect_internally()
                        break
                    await asyncio.sleep(0.1)  # Pause on other OS errors
                finally:
                    if fd != -1:
                        loop.remove_reader(fd)
            except asyncio.CancelledError:
                break
            except Exception as e_outer:
                logger.error(f"Outer Read loop error: {e_outer}")
                await asyncio.sleep(0.1)
        logger.debug("Read loop exited")

    async def _handle_disconnect_internally(self):
        """Internal handler for when a read/write error indicates disconnection."""
        if self._state == ClientState.CONNECTED:  # Avoid multiple disconnect calls
            logger.warning("Internal error detected, initiating disconnect.")
            await self.disconnect()  # Trigger full disconnect logic
            if self.config.auto_reconnect:
                asyncio.create_task(self._reconnect_loop())

    def _socket_ready(self, future: asyncio.Future[None]):
        if not future.done():
            future.set_result(None)

    async def _process_received_data(self, data: bytes, _sender_addr: Tuple[str, int]) -> None:
        processed_data = await self._plugin_manager.run_pre_receive(data)
        if processed_data is None:
            return

        try:
            packet_type, body, sequence, _flags, _pkt_len = self._protocol.unpack(
                processed_data)

            if packet_type == PacketType.ACK:
                ack_content = self._protocol.try_parse_json(body)
                if isinstance(ack_content, dict) and 'ack_sequence' in ack_content:
                    self._reliability.receive_ack(ack_content['ack_sequence'])
            elif packet_type == PacketType.HEARTBEAT:
                ack_pkt = self._protocol.create_ack(sequence)
                await self._write_queue.put(ack_pkt)
            elif packet_type == PacketType.FRAGMENT:
                parts = body.split(b'\0', 1)
                if len(parts) == 2:
                    try:
                        header_json = json.loads(parts[0].decode('utf-8'))
                        frag_data = parts[1]
                        reassembled = self._reliability.add_fragment(
                            header_json['original_seq'], header_json['fragment_id'],
                            header_json['total_fragments'], frag_data
                        )
                        if reassembled:
                            await self._process_data_packet(PacketType.DATA, reassembled, header_json['original_seq'])
                    except (json.JSONDecodeError, KeyError) as e_frag:
                        logger.error(f"Fragment processing error: {e_frag}")
            else:  # DATA, CONTROL, ERROR
                await self._process_data_packet(packet_type, body, sequence)
            await self._plugin_manager.run_post_receive(body)
        except ValueError as e_unpack:
            logger.error(f"Invalid packet: {e_unpack}")
        except Exception as e_proc:
            logger.error(f"Error processing packet: {e_proc}", exc_info=True)
            self._stats.errors += 1

    async def _process_data_packet(self, packet_type: PacketType, body: bytes, sequence: int) -> None:
        if self._reliable_delivery:
            if self._reliability.is_duplicate(sequence):
                self._stats.packets_duplicated += 1
                logger.debug(f"Duplicate packet seq: {sequence}")
                # Re-ACK
                await self._write_queue.put(self._protocol.create_ack(sequence))
                return
            await self._write_queue.put(self._protocol.create_ack(sequence))
            self._reliability.mark_received(sequence)

            if self._ordered_delivery:
                is_buffered, processable_now = self._reliability.check_and_update_order(
                    sequence, body)
                if is_buffered and not processable_now:  # Buffered, not yet ready
                    await self._trigger_callbacks('out_of_order', self._reliability.next_expected, sequence)
                    return  # Wait for it to be unbuffered
                if processable_now:
                    for pkt_data in processable_now:
                        # Assuming type is same for all
                        await self._read_queue.put((packet_type, pkt_data))
                        await self._trigger_callbacks('message', packet_type, pkt_data)
                    return  # Processed via ordered delivery
                # If not buffered and not processable_now (e.g. old packet), it's already logged by check_and_update_order
                return

        # Unreliable, or reliable but not ordered (or ordered and processed above)
        await self._read_queue.put((packet_type, body))
        await self._trigger_callbacks('message', packet_type, body)

    async def _heartbeat_loop(self):
        while self._state == ClientState.CONNECTED:
            try:
                await asyncio.sleep(self.config.heartbeat_interval)
                if not await self._plugin_manager.run_pre_heartbeat():
                    continue
                success = await self.send_heartbeat()
                if success:
                    await self._trigger_callbacks('heartbeat')
                await self._plugin_manager.run_post_heartbeat(success)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat loop error: {e}", exc_info=True)
        logger.debug("Heartbeat loop exited")

    async def send_heartbeat(self) -> bool:
        if self._state == ClientState.CONNECTED:
            try:
                await self._write_queue.put(self._protocol.create_heartbeat())
                return True
            except Exception as e:
                logger.error(f"Failed to send heartbeat: {e}")
                self._stats.errors += 1
        return False

    async def _reconnect_loop(self):
        attempt = 0
        while self._state not in [ClientState.CONNECTED, ClientState.CLOSED, ClientState.DISCONNECTING]:
            attempt += 1
            delay = self.config.retry_policy.get_retry_delay(
                attempt - 1) if self.config.retry_policy else min(30.0, (2**(attempt-1))*0.1 + random.uniform(0, 0.1))
            logger.info(f"Reconnect attempt {attempt} in {delay:.2f}s...")
            try:
                await asyncio.sleep(delay)
                if self._state in [ClientState.CONNECTED, ClientState.CLOSED, ClientState.DISCONNECTING]:
                    break  # State changed while sleeping
                if await self.connect():
                    logger.info(f"Reconnect successful (attempt {attempt})")
                    self._stats.reconnects += 1
                    break
                if self.config.retry_policy and attempt >= self.config.retry_policy.max_retries:
                    logger.error(
                        f"Max reconnect attempts ({attempt}) reached. Giving up.")
                    self._state = ClientState.CLOSED
                    self.connected = False
                    break
            except asyncio.CancelledError:
                logger.debug("Reconnect loop cancelled")
                break
            except Exception as e:
                logger.error(
                    f"Error in reconnect attempt {attempt}: {e}", exc_info=True)
                self._stats.errors += 1
        logger.debug("Reconnect loop exited")

    async def send(self, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA) -> bool:
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")

        if isinstance(data, bytes) and len(data) > self.config.max_packet_size:
            return await self._send_large_data(data)

        current_payload: Union[bytes, Dict[str, Any]] = data
        if isinstance(current_payload, bytes):  # Apply pre-send plugin only to bytes for now
            plugin_modified_payload = await self._plugin_manager.run_pre_send(current_payload)
            if plugin_modified_payload is None:
                return False
            current_payload = plugin_modified_payload

        seq = self._protocol.get_next_sequence()
        pkt_bytes = self._protocol.create_packet(
            packet_type, current_payload, seq)

        sent_successfully = False
        try:
            # Don't make ACKs/heartbeats reliable themselves
            if self._reliable_delivery and packet_type not in [PacketType.ACK, PacketType.HEARTBEAT]:
                sent_successfully = await self._send_reliable(pkt_bytes, seq)
            else:
                await self._write_queue.put(pkt_bytes)
                sent_successfully = True

            # Post-send plugin based on original data form
            original_data_form = data if isinstance(
                data, bytes) else pkt_bytes  # Or serialize dict for plugin?
            await self._plugin_manager.run_post_send(original_data_form, sent_successfully)
            return sent_successfully
        except Exception as e:
            logger.error(f"Send error: {e}", exc_info=True)
            self._stats.errors += 1
            return False

    async def _send_large_data(self, data_payload: bytes) -> bool:
        try:
            # Max payload per fragment, accounting for protocol header and fragment JSON header
            json_header_approx_size = 100  # Estimate for fragment metadata JSON
            max_frag_payload = self.config.max_datagram_size - \
                self._protocol.HEADER_SIZE - json_header_approx_size
            if max_frag_payload <= 0:
                logger.error("max_datagram_size too small for fragmentation")
                return False

            data_len = len(data_payload)
            num_fragments = (data_len + max_frag_payload -
                             1) // max_frag_payload
            orig_seq = self._protocol.get_next_sequence()
            logger.debug(
                f"Fragmenting {data_len}B into {num_fragments} for orig_seq {orig_seq}")

            all_ok = True
            for i in range(num_fragments):
                start, end = i * \
                    max_frag_payload, min((i + 1) * max_frag_payload, data_len)
                frag_content = data_payload[start:end]
                frag_pkt_bytes = self._protocol.create_fragment_packet(
                    frag_content, i, num_fragments, orig_seq)

                # Extract the actual sequence number from the packed fragment for ACK tracking
                _m, _pt, _c, frag_actual_seq, _f, _l = struct.unpack(
                    self._protocol.HEADER_FORMAT, frag_pkt_bytes[:self._protocol.HEADER_SIZE])

                success_frag: bool
                if self._reliable_delivery:  # Fragments should be reliable if base delivery is
                    success_frag = await self._send_reliable(frag_pkt_bytes, frag_actual_seq)
                else:
                    await self._write_queue.put(frag_pkt_bytes)
                    success_frag = True
                if not success_frag:
                    all_ok = False
                    logger.warning(
                        f"Failed sending fragment {i} of orig_seq {orig_seq}")
                await asyncio.sleep(0.001)  # Inter-fragment delay
            return all_ok
        except Exception as e:
            logger.error(f"Send large data error: {e}", exc_info=True)
            return False

    async def _send_reliable(self, packet_to_send: bytes, sequence_to_ack: int) -> bool:
        future_ack = self._reliability.create_ack_future(sequence_to_ack)
        for attempt in range(self._reliability.max_retry + 1):
            if attempt > 0:
                logger.debug(
                    f"Retry send seq: {sequence_to_ack}, attempt: {attempt+1}")
            try:
                await self._write_queue.put(packet_to_send)
                await asyncio.wait_for(future_ack, timeout=self._reliability.ack_timeout)
                return True  # ACKed
            except asyncio.TimeoutError:
                if attempt == self._reliability.max_retry:
                    logger.warning(
                        f"Max retries for seq: {sequence_to_ack}, packet lost.")
                    self._reliability.stats['packets_lost'] += 1
                    await self._trigger_callbacks('packet_loss', sequence_to_ack)
                    await self._plugin_manager.run_on_packet_loss(sequence_to_ack)
                    if not future_ack.done():
                        future_ack.cancel()
                    self._reliability.pending_acks.pop(sequence_to_ack, None)
                    return False
            except Exception as e:
                logger.error(
                    f"Send reliable (seq {sequence_to_ack}) error: {e}", exc_info=True)
                if not future_ack.done():
                    future_ack.set_exception(e)
                self._reliability.pending_acks.pop(sequence_to_ack, None)
                return False
        return False  # Should be covered by loop logic

    async def receive(self) -> Optional[Tuple[PacketType, bytes]]:
        if self._state != ClientState.CONNECTED:
            raise ConnectionError("Client not connected")
        try:
            return await asyncio.wait_for(self._read_queue.get(), timeout=self.config.timeout if self.config.timeout > 0 else 0.1)
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            logger.error(f"Receive error: {e}", exc_info=True)
            self._stats.errors += 1
            return None

    async def send_to_group(self, group: str, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA) -> bool:
        if self._state != ClientState.CONNECTED or not self._socket:
            raise ConnectionError("Client not connected or socket unavailable")
        pkt = self._protocol.create_packet(packet_type, data)
        try:
            self._socket.sendto(pkt, (group, self.config.port))
            self._stats.bytes_sent += len(pkt)
            self._stats.packets_sent += 1
            return True
        except Exception as e:
            logger.error(f"Send to group {group} error: {e}", exc_info=True)
            return False

    async def broadcast(self, port: int, data: Union[bytes, Dict[str, Any]], packet_type: PacketType = PacketType.DATA) -> bool:
        if self._state != ClientState.CONNECTED or not self._socket:
            raise ConnectionError("Client not connected or socket unavailable")
        if not (self.config.socket_options and self.config.socket_options.broadcast):
            logger.error("Broadcast not configured/enabled in socket options")
            return False
        pkt = self._protocol.create_packet(packet_type, data)
        try:
            self._socket.sendto(pkt, ('<broadcast>', port))
            self._stats.bytes_sent += len(pkt)
            self._stats.packets_sent += 1
            return True
        except Exception as e:
            logger.error(f"Broadcast error: {e}", exc_info=True)
            return False

    def on(self, event: str, callback: AnyEventCallbackType) -> None:
        if event in self._callbacks:
            self._callbacks[event].append(callback)
        else:
            raise ValueError(f"Unknown event: {event}")

    def off(self, event: str, callback: AnyEventCallbackType) -> None:
        if event in self._callbacks and callback in self._callbacks[event]:
            self._callbacks[event].remove(callback)
        elif event not in self._callbacks:
            raise ValueError(f"Unknown event: {event}")

    async def _trigger_callbacks(self, event: str, *args: Any) -> None:
        if event not in self._callbacks:
            return
        for cb in self._callbacks[event]:
            try:
                if asyncio.iscoroutinefunction(cb):
                    await cb(self, *args)
                else:
                    cb(self, *args)  # Consider run_in_executor for sync blocking
            except Exception as e:
                logger.error(
                    f"Callback error event {event} ({cb.__name__}): {e}", exc_info=True)

    # Ensure UDPPlugin is the correct base
    def register_plugin(self, plugin_class: Type[UDPPlugin]) -> bool:
        return self._plugin_manager.register(plugin_class)

    def unregister_plugin(self, plugin_name: str) -> bool:
        return self._plugin_manager.unregister(plugin_name)

    def get_stats(self) -> Dict[str, Any]:
        reliability_stats = self._reliability.stats.copy()  # Make a copy
        return {
            "state": self._state.name, "connected": self.connected,
            "host": self.config.host, "port": self.config.port,
            "last_activity": self._last_activity, "idle_time": time.time() - self._last_activity,
            "reliability": {"reliable_delivery": self._reliable_delivery, "ordered_delivery": self._ordered_delivery, **reliability_stats},
            "connection_stats": self._stats.to_dict()
        }

    async def __aenter__(self) -> "UDPClient":
        await self.connect()
        return self

    async def __aexit__(self, _exc_type: Optional[Type[BaseException]], _exc_val: Optional[BaseException], _exc_tb: Optional[TracebackType]) -> Optional[bool]:
        await self.disconnect()
        return None
