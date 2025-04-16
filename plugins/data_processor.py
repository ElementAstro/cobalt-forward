"""
Data Processor Plugin - A plugin for processing data in different formats

This is an example plugin for the Cobalt Forward system.
"""
import logging
import time
import json
import asyncio
from typing import Dict, List, Any, Optional
from app.plugin.base import Plugin
from app.plugin.models import PluginMetadata, PluginState
from app.plugin.permissions import PluginPermission, require_permission

logger = logging.getLogger(__name__)


class DataProcessorPlugin(Plugin):
    """
    A plugin for processing data in different formats.
    Demonstrates various plugin capabilities.
    """
    
    def __init__(self):
        """Initialize plugin"""
        super().__init__()
        
        # Define plugin metadata
        self.metadata = PluginMetadata(
            name="data_processor",
            version="1.0.0",
            author="Cobalt Forward Team",
            description="Process data in different formats",
            dependencies=[],
            load_priority=50  # Lower values load first
        )
        
        # Processed items counter
        self.processed_count = 0
        self.last_processed = None
        
    async def initialize(self) -> None:
        """Called when plugin is loaded"""
        logger.info("Initializing Data Processor plugin")
        
        # Register event handlers
        self.register_event_handler("data.process", self.handle_process_event)
        self.register_event_handler("system.ping", self.handle_ping)
        
        # Register lifecycle hooks
        self.add_lifecycle_hook("pre_shutdown", self.cleanup_resources)
        
        logger.info("Data Processor plugin initialized successfully")
        
    async def shutdown(self) -> None:
        """Called when plugin is unloaded"""
        logger.info("Shutting down Data Processor plugin")
        await self.cleanup_resources()
        
    async def cleanup_resources(self):
        """Cleanup any resources used by the plugin"""
        logger.info(f"Cleaned up resources, processed {self.processed_count} items")
    
    @require_permission(PluginPermission.EVENT)
    async def handle_ping(self, event_data: Any) -> Dict[str, Any]:
        """Handle ping event"""
        return {
            "plugin": "data_processor",
            "version": self.metadata.version,
            "timestamp": time.time(),
            "status": "active",
            "processed_count": self.processed_count
        }
    
    @require_permission(PluginPermission.FILE_IO)
    async def handle_process_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process data from an event"""
        try:
            if not isinstance(event_data, dict):
                return {"error": "Invalid data format, expected dictionary"}
            
            data_format = event_data.get("format", "json")
            data = event_data.get("data")
            
            if not data:
                return {"error": "No data provided"}
                
            # Process based on format
            result = await self.process_data(data, data_format)
            
            # Update stats
            self.processed_count += 1
            self.last_processed = time.time()
            
            # Record execution metrics
            self._metrics.record_execution(time.time() - self.last_processed)
            
            return {
                "success": True,
                "result": result,
                "format": data_format,
                "processed_count": self.processed_count
            }
            
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            self._metrics.record_error()
            return {"error": str(e)}
    
    async def process_data(self, data: Any, format_type: str) -> Any:
        """Process data based on the specified format"""
        if format_type == "json":
            return self._process_json(data)
        elif format_type == "csv":
            return self._process_csv(data)
        elif format_type == "text":
            return self._process_text(data)
        else:
            raise ValueError(f"Unsupported format: {format_type}")
    
    def _process_json(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process JSON data"""
        if isinstance(data, str):
            # Parse JSON string
            data = json.loads(data)
        
        # Example processing: count keys at each level
        def count_keys(obj, counts=None):
            if counts is None:
                counts = {}
                
            if isinstance(obj, dict):
                for key, value in obj.items():
                    counts[key] = counts.get(key, 0) + 1
                    if isinstance(value, (dict, list)):
                        count_keys(value, counts)
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, (dict, list)):
                        count_keys(item, counts)
            
            return counts
        
        key_counts = count_keys(data)
        
        return {
            "key_counts": key_counts,
            "total_keys": sum(key_counts.values()),
            "timestamp": time.time()
        }
    
    def _process_csv(self, data: str) -> Dict[str, Any]:
        """Process CSV data"""
        lines = data.strip().split('\n')
        if not lines:
            return {"error": "Empty CSV data"}
            
        headers = lines[0].split(',')
        rows = []
        
        for i in range(1, len(lines)):
            values = lines[i].split(',')
            if len(values) == len(headers):
                row = {headers[j]: values[j] for j in range(len(headers))}
                rows.append(row)
        
        return {
            "headers": headers,
            "row_count": len(rows),
            "rows": rows,
            "timestamp": time.time()
        }
    
    def _process_text(self, data: str) -> Dict[str, Any]:
        """Process text data"""
        # Count words, lines, characters
        lines = data.split('\n')
        words = data.split()
        chars = len(data)
        
        # Get most common words
        word_counts = {}
        for word in words:
            word = word.lower().strip('.,!?;:"\'()[]{}')
            if word:
                word_counts[word] = word_counts.get(word, 0) + 1
                
        # Sort by count
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            "line_count": len(lines),
            "word_count": len(words),
            "char_count": chars,
            "top_words": {word: count for word, count in top_words},
            "timestamp": time.time()
        }
    
    def get_plugin_stats(self) -> Dict[str, Any]:
        """Return plugin statistics"""
        return {
            "name": self.metadata.name,
            "version": self.metadata.version,
            "processed_count": self.processed_count,
            "last_processed": self.last_processed,
            "uptime": time.time() - self.start_time if self.start_time else 0,
            "metrics": self._metrics.get_metrics_summary() if hasattr(self._metrics, 'get_metrics_summary') else {}
        }
