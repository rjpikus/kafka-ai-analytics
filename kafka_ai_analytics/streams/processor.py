"""
Kafka Streams processor for data transformation and feature engineering.
"""
import json
import logging
import time
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict
import numpy as np

from ..config import config

logger = logging.getLogger(__name__)

class WindowedAggregator:
    """
    A class for performing windowed aggregations on streaming data.
    Implements 5-second sliding windows for time-series aggregations.
    """
    
    def __init__(self, window_size_seconds: int = 5):
        """
        Initialize the windowed aggregator.
        
        Args:
            window_size_seconds: Size of the sliding window in seconds.
        """
        self.window_size = window_size_seconds
        self.windows = defaultdict(lambda: [])  # key -> list of values in window
        self.timestamp_index = defaultdict(lambda: [])  # key -> list of timestamps
    
    def add_value(self, key: str, value: float, timestamp: Optional[datetime] = None) -> None:
        """
        Add a value to the window for a given key.
        
        Args:
            key: The key for aggregation (e.g., "device_id:temperature").
            value: The numerical value to add.
            timestamp: Optional timestamp (defaults to now).
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        # Add to windows
        self.windows[key].append(value)
        self.timestamp_index[key].append(timestamp)
        
        # Remove old values outside the window
        self._prune_window(key, timestamp)
    
    def _prune_window(self, key: str, current_time: datetime) -> None:
        """
        Remove values outside the sliding window.
        
        Args:
            key: The aggregation key.
            current_time: The current timestamp.
        """
        if not self.timestamp_index[key]:
            return
        
        window_start = current_time - timedelta(seconds=self.window_size)
        
        # Find index of first value within the window
        cutoff_idx = 0
        for idx, ts in enumerate(self.timestamp_index[key]):
            if ts >= window_start:
                cutoff_idx = idx
                break
        
        # Prune values outside the window
        if cutoff_idx > 0:
            self.windows[key] = self.windows[key][cutoff_idx:]
            self.timestamp_index[key] = self.timestamp_index[key][cutoff_idx:]
    
    def get_aggregates(self, key: str) -> Dict[str, float]:
        """
        Get aggregations for the current window.
        
        Args:
            key: The aggregation key.
            
        Returns:
            Dictionary with sum, avg, min, max, p95 aggregations.
        """
        values = self.windows.get(key, [])
        
        if not values:
            return {
                "sum": 0.0,
                "avg": 0.0,
                "min": 0.0,
                "max": 0.0,
                "p95": 0.0,
                "count": 0
            }
        
        return {
            "sum": sum(values),
            "avg": np.mean(values),
            "min": min(values),
            "max": max(values),
            "p95": np.percentile(values, 95) if len(values) >= 5 else max(values),
            "count": len(values)
        }
    
    def get_all_aggregates(self) -> Dict[str, Dict[str, float]]:
        """
        Get aggregations for all keys in the current windows.
        
        Returns:
            Dictionary mapping keys to their aggregation dictionaries.
        """
        return {key: self.get_aggregates(key) for key in self.windows.keys()}


class SessionProcessor:
    """
    Processor for identifying and aggregating user sessions.
    A session is defined as a sequence of events from the same user within
    a certain time window.
    """
    
    def __init__(self, session_timeout_seconds: int = 1800):
        """
        Initialize the session processor.
        
        Args:
            session_timeout_seconds: Session timeout in seconds (default: 30 minutes).
        """
        self.session_timeout = session_timeout_seconds
        self.user_sessions = {}  # user_id -> {session_id, last_activity, events}
    
    def process_event(self, user_id: str, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a user event, assigning it to a session.
        
        Args:
            user_id: The user identifier.
            event: The event data.
            
        Returns:
            The event with session information added.
        """
        event_time = self._get_event_time(event)
        
        # Check if user has an active session
        if user_id in self.user_sessions:
            session = self.user_sessions[user_id]
            last_activity = session["last_activity"]
            
            # If session has timed out, create a new one
            if (event_time - last_activity).total_seconds() > self.session_timeout:
                session_id = f"{user_id}_{event_time.timestamp()}"
                session = {
                    "session_id": session_id,
                    "last_activity": event_time,
                    "start_time": event_time,
                    "events": [],
                    "event_count": 0
                }
                self.user_sessions[user_id] = session
            else:
                # Update existing session
                session["last_activity"] = event_time
        else:
            # Create new session for new user
            session_id = f"{user_id}_{event_time.timestamp()}"
            session = {
                "session_id": session_id,
                "last_activity": event_time,
                "start_time": event_time,
                "events": [],
                "event_count": 0
            }
            self.user_sessions[user_id] = session
        
        # Add event to session
        session["events"].append(event)
        session["event_count"] += 1
        
        # Augment event with session info
        event_with_session = event.copy()
        event_with_session["session_id"] = session["session_id"]
        event_with_session["session_event_count"] = session["event_count"]
        event_with_session["session_duration_seconds"] = (
            event_time - session["start_time"]
        ).total_seconds()
        
        return event_with_session
    
    def _get_event_time(self, event: Dict[str, Any]) -> datetime:
        """
        Extract timestamp from event.
        
        Args:
            event: The event data.
            
        Returns:
            Event timestamp as datetime.
        """
        if "timestamp" in event:
            timestamp = event["timestamp"]
            if isinstance(timestamp, str):
                try:
                    return datetime.fromisoformat(timestamp)
                except ValueError:
                    pass
            elif isinstance(timestamp, datetime):
                return timestamp
        
        # Default to current time if no valid timestamp found
        return datetime.now()
    
    def get_session_summary(self, user_id: str) -> Dict[str, Any]:
        """
        Get a summary of the user's current session.
        
        Args:
            user_id: The user identifier.
            
        Returns:
            Session summary or None if no active session.
        """
        if user_id not in self.user_sessions:
            return None
        
        session = self.user_sessions[user_id]
        duration = (session["last_activity"] - session["start_time"]).total_seconds()
        
        return {
            "session_id": session["session_id"],
            "start_time": session["start_time"].isoformat(),
            "last_activity": session["last_activity"].isoformat(),
            "duration_seconds": duration,
            "event_count": session["event_count"]
        }
    
    def get_all_active_sessions(self) -> List[Dict[str, Any]]:
        """
        Get summaries for all active sessions.
        
        Returns:
            List of session summaries.
        """
        active_sessions = []
        now = datetime.now()
        
        for user_id, session in self.user_sessions.items():
            # Check if session is still active
            if (now - session["last_activity"]).total_seconds() <= self.session_timeout:
                summary = self.get_session_summary(user_id)
                if summary:
                    summary["user_id"] = user_id
                    active_sessions.append(summary)
        
        return active_sessions


class StreamProcessor:
    """
    Main Kafka Streams processor for real-time feature engineering.
    Handles windowed aggregations and session processing.
    """
    
    def __init__(self):
        """Initialize the stream processor."""
        self.config = config
        self.raw_topic = self.config.kafka.topics["raw_data"]
        self.processed_topic = self.config.kafka.topics["processed_features"]
        
        # Initialize processors
        self.window_aggregator = WindowedAggregator(window_size_seconds=5)
        self.session_processor = SessionProcessor()
        
        # Metrics
        self.metrics = {
            "processed_count": 0,
            "processing_time_ms_sum": 0,
            "errors": 0
        }
    
    def process_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a message from the raw data topic.
        
        Args:
            message: The raw message to process.
            
        Returns:
            Processed features dictionary.
        """
        start_time = time.time()
        try:
            source = message.get("source", "unknown")
            
            if source == "iot_device":
                return self._process_iot_message(message)
            elif source == "user_activity":
                return self._process_user_message(message)
            else:
                return self._process_generic_message(message)
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.metrics["errors"] += 1
            # Return the original message with an error flag
            result = message.copy()
            result["processing_error"] = str(e)
            return result
        finally:
            # Update metrics
            end_time = time.time()
            self.metrics["processed_count"] += 1
            self.metrics["processing_time_ms_sum"] += (end_time - start_time) * 1000
    
    def _process_iot_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process an IoT device message."""
        device_id = message.get("device_id", "unknown")
        readings = message.get("readings", {})
        timestamp_str = message.get("timestamp")
        
        try:
            timestamp = datetime.fromisoformat(timestamp_str)
        except (ValueError, TypeError):
            timestamp = datetime.now()
        
        # Process each reading type through windowed aggregation
        aggregations = {}
        for reading_type, value in readings.items():
            if isinstance(value, (int, float)):
                # Create an aggregation key for this device and reading type
                agg_key = f"{device_id}:{reading_type}"
                
                # Add to windowed aggregator
                self.window_aggregator.add_value(agg_key, value, timestamp)
                
                # Get aggregations for this window
                aggregations[reading_type] = self.window_aggregator.get_aggregates(agg_key)
        
        # Create a processed feature record
        processed = {
            "device_id": device_id,
            "source": "iot_device",
            "timestamp": timestamp.isoformat(),
            "raw_readings": readings,
            "aggregations": aggregations,
            "processing_timestamp": datetime.now().isoformat()
        }
        
        return processed
    
    def _process_user_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process a user activity message."""
        user_id = message.get("user_id", "unknown")
        
        # Process through session processor
        processed = self.session_processor.process_event(user_id, message)
        
        # Add processing metadata
        processed["processing_timestamp"] = datetime.now().isoformat()
        processed["source"] = "user_activity"
        
        return processed
    
    def _process_generic_message(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process a generic message with basic enrichment."""
        processed = message.copy()
        processed["processing_timestamp"] = datetime.now().isoformat()
        
        # Add a UUID if not present
        if "id" not in processed:
            import uuid
            processed["id"] = str(uuid.uuid4())
        
        return processed
    
    def get_average_processing_time(self) -> float:
        """
        Get the average processing time in milliseconds.
        
        Returns:
            Average processing time or 0 if no messages processed.
        """
        if self.metrics["processed_count"] == 0:
            return 0
        return self.metrics["processing_time_ms_sum"] / self.metrics["processed_count"]
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get processor metrics.
        
        Returns:
            Dictionary of metrics.
        """
        metrics = dict(self.metrics)
        if self.metrics["processed_count"] > 0:
            metrics["average_processing_time_ms"] = self.get_average_processing_time()
        return metrics 