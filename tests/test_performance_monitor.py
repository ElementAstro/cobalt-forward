import pytest
from unittest.mock import AsyncMock, Mock, patch, MagicMock
import asyncio
import time
from app.performance_monitor import PerformanceMonitor, PerformanceMetrics

# filepath: /d:/Project/cobalt-forward-1/app/test_performance_monitor.py


@pytest.fixture
def mock_psutil():
    with patch('app.performance_monitor.psutil') as mock:
        mock.cpu_percent.return_value = 50.0
        mock.virtual_memory.return_value = Mock(
            total=17179869184,  # 16GB
            available=8589934592,  # 8GB
            used=8589934592,  # 8GB
            percent=50.0
        )
        mock.net_connections.return_value = ['conn1', 'conn2']
        mock.net_io_counters.return_value = Mock(
            bytes_sent=1000,
            bytes_recv=2000,
            packets_sent=10,
            packets_recv=20
        )
        mock.cpu_count.return_value = 8
        mock.getloadavg.return_value = [1.0, 1.5, 2.0]
        yield mock


@pytest.fixture
def monitor(mock_psutil):
    return PerformanceMonitor(history_size=10)


@pytest.mark.asyncio
async def test_monitor_initialization(monitor):
    assert monitor.start_time > 0
    assert len(monitor.message_timestamps) == 0
    assert len(monitor.latencies) == 0
    assert monitor._running is False
    assert monitor._monitor_task is None


@pytest.mark.asyncio
async def test_start_stop_monitor(monitor):
    with patch('app.performance_monitor.logger') as mock_logger:
        await monitor.start()
        assert monitor._running is True
        assert monitor._monitor_task is not None

        await monitor.stop()
        assert monitor._running is False
        mock_logger.info.assert_called_with("Performance monitoring stopped")


def test_record_message(monitor):
    monitor.record_message(10.5)
    assert len(monitor.message_timestamps) == 1
    assert len(monitor.latencies) == 1
    assert monitor.latencies[0] == 10.5


@pytest.mark.asyncio
async def test_monitor_loop(monitor, mock_psutil):
    with patch('app.performance_monitor.logger') as mock_logger:
        monitor._running = True
        # Create a task that runs for a short time
        task = asyncio.create_task(monitor._monitor_loop())
        await asyncio.sleep(0.1)
        monitor._running = False
        await task

        mock_logger.error.assert_not_called()


def test_get_current_metrics(monitor, mock_psutil):
    # Record some test messages
    current_time = time.time()
    with patch('time.time', return_value=current_time):
        for _ in range(5):
            monitor.record_message(10.0)

    metrics = monitor.get_current_metrics()
    assert isinstance(metrics, PerformanceMetrics)
    assert metrics.cpu_percent == 50.0
    assert metrics.memory_percent == 50.0
    assert metrics.connection_count == 2
    assert metrics.average_latency == 10.0


def test_get_detailed_metrics(monitor, mock_psutil):
    metrics = monitor.get_detailed_metrics()

    assert "memory_details" in metrics
    assert "network_stats" in metrics
    assert "system_load" in metrics

    memory_details = metrics["memory_details"]
    assert memory_details["total"] == 16.0  # 16GB
    assert memory_details["percent"] == 50.0

    network_stats = metrics["network_stats"]
    assert network_stats["bytes_sent"] == 1000
    assert network_stats["bytes_recv"] == 2000

    system_load = metrics["system_load"]
    assert len(system_load) == 3
    assert system_load[0] == 12.5  # 1.0/8*100


def test_get_memory_details(monitor, mock_psutil):
    memory_details = monitor._get_memory_details()
    assert isinstance(memory_details, dict)
    assert memory_details["total"] == 16.0
    assert memory_details["available"] == 8.0
    assert memory_details["used"] == 8.0
    assert memory_details["percent"] == 50.0


def test_get_network_stats(monitor, mock_psutil):
    network_stats = monitor._get_network_stats()
    assert isinstance(network_stats, dict)
    assert network_stats["bytes_sent"] == 1000
    assert network_stats["bytes_recv"] == 2000
    assert network_stats["packets_sent"] == 10
    assert network_stats["packets_recv"] == 20


def test_get_system_load(monitor, mock_psutil):
    system_load = monitor._get_system_load()
    assert isinstance(system_load, list)
    assert len(system_load) == 3
    # Values divided by CPU count * 100
    assert system_load == [12.5, 18.75, 25.0]


@pytest.mark.asyncio
async def test_monitor_error_handling(monitor):
    with patch('app.performance_monitor.logger') as mock_logger:
        monitor._running = True
        with patch.object(monitor, 'get_current_metrics', side_effect=Exception("Test error")):
            task = asyncio.create_task(monitor._monitor_loop())
            await asyncio.sleep(0.1)
            monitor._running = False
            await task

            mock_logger.error.assert_called()


def test_log_metrics(monitor):
    with patch('app.performance_monitor.logger') as mock_logger:
        metrics = PerformanceMetrics(
            cpu_percent=50.0,
            memory_percent=60.0,
            connection_count=2,
            message_rate=100.0,
            average_latency=5.0
        )
        monitor._log_metrics(metrics)
        mock_logger.info.assert_called_once()
