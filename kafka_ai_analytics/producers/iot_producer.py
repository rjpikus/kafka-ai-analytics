"""
IoT-specific Kafka producer.
"""
import json
import uuid
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from .base_producer import BaseKafkaProducer
from ..config import config

logger = logging.getLogger(__name__)

class IoTProducer(BaseKafkaProducer):
    """Kafka producer for IoT device data."""
    
    def __init__(self, use_confluent: bool = True):
        """
        Initialize the IoT producer.
        
        Args:
            use_confluent: Whether to use the Confluent Kafka client.
        """
        super().__init__(use_confluent=use_confluent)
        self.raw_data_topic = config.kafka.topics["raw_data"]
    
    def send_device_telemetry(self, device_id: str, readings: Dict[str, Any],
                              timestamp: Optional[datetime] = None) -> None:
        """
        Send device telemetry data to Kafka.
        
        Args:
            device_id: Unique identifier for the device.
            readings: Dictionary of sensor readings (e.g., {"temperature": 22.5}).
            timestamp: Optional timestamp for the reading (defaults to now).
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        message = {
            "message_id": str(uuid.uuid4()),
            "device_id": device_id,
            "timestamp": timestamp.isoformat(),
            "readings": readings,
            "source": "iot_device"
        }
        
        # Use the device_id as the key for consistent partitioning
        self.send(self.raw_data_topic, message, key=device_id)
    
    def send_batch_telemetry(self, device_data: List[Dict[str, Any]]) -> None:
        """
        Send a batch of device telemetry data to Kafka.
        
        Args:
            device_data: List of device data dictionaries, each containing:
                - device_id: Unique identifier for the device
                - readings: Dictionary of sensor readings
                - timestamp: Optional timestamp (ISO format or datetime object)
        """
        for device in device_data:
            device_id = device["device_id"]
            readings = device["readings"]
            
            # Handle timestamp if provided, otherwise use current time
            timestamp = device.get("timestamp")
            if isinstance(timestamp, str):
                try:
                    timestamp = datetime.fromisoformat(timestamp)
                except ValueError:
                    logger.warning(f"Invalid timestamp format for device {device_id}. Using current time.")
                    timestamp = None
            
            self.send_device_telemetry(device_id, readings, timestamp)
    
    def simulate_device_stream(self, device_count: int, readings_per_device: int, 
                              interval_ms: int = 100) -> None:
        """
        Simulate a stream of IoT device data for testing.
        
        Args:
            device_count: Number of devices to simulate.
            readings_per_device: Number of readings per device.
            interval_ms: Interval between readings in milliseconds.
        """
        import random
        
        for i in range(readings_per_device):
            batch = []
            for device_num in range(device_count):
                device_id = f"sim-device-{device_num}"
                
                # Simulate various sensor readings
                readings = {
                    "temperature": round(random.uniform(15.0, 35.0), 2),
                    "humidity": round(random.uniform(30.0, 80.0), 2),
                    "pressure": round(random.uniform(980.0, 1020.0), 2),
                    "battery": round(random.uniform(3.0, 4.2), 2)
                }
                
                batch.append({
                    "device_id": device_id,
                    "readings": readings
                })
            
            self.send_batch_telemetry(batch)
            
            # Sleep for the specified interval
            if i < readings_per_device - 1:  # Don't sleep after the last batch
                time.sleep(interval_ms / 1000.0)
            
            # Log progress every 10 batches
            if i % 10 == 0:
                logger.info(f"Sent {i * device_count} simulated device readings")
        
        total_messages = device_count * readings_per_device
        logger.info(f"Finished sending {total_messages} simulated device readings")
        
        # Report metrics
        metrics = self.get_metrics()
        avg_latency = metrics.get("average_latency_ms", 0)
        logger.info(f"Average message latency: {avg_latency:.2f}ms") 