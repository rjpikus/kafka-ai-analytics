"""
Base Kafka producer implementation.
"""
import json
import time
import logging
from typing import Dict, Any, Callable, Optional
from kafka.producer import KafkaProducer
from confluent_kafka import Producer as ConfluentProducer
from ..config import config

logger = logging.getLogger(__name__)

class BaseKafkaProducer:
    """Base Kafka producer class."""
    
    def __init__(self, use_confluent: bool = False):
        """
        Initialize the Kafka producer.
        
        Args:
            use_confluent: Whether to use the Confluent Kafka client (better performance)
                           or the kafka-python client.
        """
        self.config = config.kafka
        self.use_confluent = use_confluent
        self.producer = self._create_producer()
        self.metrics = {
            "messages_sent": 0,
            "bytes_sent": 0,
            "errors": 0,
            "latency_ms_sum": 0,
            "latency_ms_count": 0
        }
    
    def _create_producer(self) -> Any:
        """Create and return a Kafka producer."""
        if self.use_confluent:
            return self._create_confluent_producer()
        return self._create_kafka_python_producer()
    
    def _create_kafka_python_producer(self) -> KafkaProducer:
        """Create a kafka-python producer."""
        return KafkaProducer(
            bootstrap_servers=self.config.brokers,
            acks='all',  # Wait for all replicas to acknowledge
            retries=5,
            batch_size=16384,
            linger_ms=5,
            buffer_memory=33554432,  # 32MB
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            value_serializer=lambda v: json.dumps(v).encode('utf-8') if v else None,
            request_timeout_ms=30000,  # 30 seconds
            compression_type='gzip'
        )
    
    def _create_confluent_producer(self) -> ConfluentProducer:
        """Create a confluent-kafka producer."""
        return ConfluentProducer({
            'bootstrap.servers': ','.join(self.config.brokers),
            'acks': 'all',
            'retries': 5,
            'linger.ms': 5,
            'batch.size': 16384,
            'compression.type': 'gzip',
            'queue.buffering.max.messages': 100000,
            'queue.buffering.max.ms': 5,
            'request.timeout.ms': 30000
        })
    
    def send(self, topic: str, value: Dict[str, Any], key: Optional[str] = None, 
             callback: Optional[Callable] = None) -> None:
        """
        Send a message to a Kafka topic.
        
        Args:
            topic: The topic to send the message to.
            value: The message value.
            key: Optional message key for partitioning.
            callback: Optional callback function to execute after sending.
        """
        start_time = time.time()
        
        if self.use_confluent:
            self._send_confluent(topic, value, key, callback, start_time)
        else:
            self._send_kafka_python(topic, value, key, callback, start_time)
    
    def _send_kafka_python(self, topic: str, value: Dict[str, Any], key: Optional[str], 
                           callback: Optional[Callable], start_time: float) -> None:
        """Send a message using kafka-python."""
        try:
            future = self.producer.send(topic, value=value, key=key)
            
            if callback:
                future.add_callback(lambda _: self._record_metrics(value, start_time))
                future.add_errback(lambda exc: self._handle_error(exc))
            else:
                future.add_callback(lambda _: self._record_metrics(value, start_time))
                future.add_errback(lambda exc: self._handle_error(exc))
        except Exception as e:
            self._handle_error(e)
    
    def _send_confluent(self, topic: str, value: Dict[str, Any], key: Optional[str], 
                        callback: Optional[Callable], start_time: float) -> None:
        """Send a message using confluent-kafka."""
        try:
            # Serialize the value
            value_bytes = json.dumps(value).encode('utf-8') if value else None
            key_bytes = key.encode('utf-8') if key else None
            
            def delivery_callback(err, msg):
                if err:
                    self._handle_error(err)
                else:
                    self._record_metrics(value, start_time)
                    if callback:
                        callback(msg)
            
            self.producer.produce(topic, value=value_bytes, key=key_bytes, 
                                  callback=delivery_callback)
            # Trigger any callbacks for messages that have already been delivered
            self.producer.poll(0)
        except Exception as e:
            self._handle_error(e)
    
    def _record_metrics(self, value: Dict[str, Any], start_time: float) -> None:
        """Record metrics for the sent message."""
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000
        
        self.metrics["messages_sent"] += 1
        self.metrics["bytes_sent"] += len(json.dumps(value))
        self.metrics["latency_ms_sum"] += latency_ms
        self.metrics["latency_ms_count"] += 1
        
        # Log if latency is above threshold (50ms)
        if latency_ms > 50:
            logger.warning(f"High message latency: {latency_ms:.2f}ms")
    
    def _handle_error(self, exception: Exception) -> None:
        """Handle producer errors."""
        self.metrics["errors"] += 1
        logger.error(f"Error sending message to Kafka: {exception}")
    
    def flush(self, timeout: int = 30) -> None:
        """
        Flush the producer to ensure all messages are sent.
        
        Args:
            timeout: Maximum time to block (in seconds).
        """
        if self.use_confluent:
            self.producer.flush(timeout)
        else:
            self.producer.flush(timeout=timeout)
    
    def close(self) -> None:
        """Close the producer."""
        if self.use_confluent:
            # Confluent producer only needs flush
            self.producer.flush()
        else:
            self.producer.close()
    
    def get_average_latency(self) -> float:
        """Get the average latency in milliseconds."""
        if self.metrics["latency_ms_count"] == 0:
            return 0
        return self.metrics["latency_ms_sum"] / self.metrics["latency_ms_count"]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get the producer metrics."""
        metrics = dict(self.metrics)  # Copy the metrics
        if self.metrics["latency_ms_count"] > 0:
            metrics["average_latency_ms"] = self.get_average_latency()
        return metrics 