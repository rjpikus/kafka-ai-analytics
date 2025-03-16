"""
Main module for the Kafka AI Analytics pipeline.
"""
import os
import sys
import argparse
import logging
import time
import json
from typing import Dict, Any, Optional
from kafka import KafkaConsumer
import threading
import signal

from .config import config, get_config
from .producers.iot_producer import IoTProducer
from .streams.processor import StreamProcessor
from .storage.s3_sink import S3Sink
from .ml_pipeline.sagemaker_client import SageMakerClient
from .monitoring.cloudwatch import CloudWatchMonitor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

class KafkaAnalyticsPipeline:
    """
    Main class for the Kafka AI Analytics pipeline.
    Integrates all components: producers, processors, storage, ML, monitoring.
    """
    
    def __init__(self, env: Optional[str] = None):
        """
        Initialize the pipeline.
        
        Args:
            env: Environment (dev, test, prod).
        """
        # Load environment-specific config
        self.config = get_config(env)
        logger.info(f"Initializing Kafka AI Analytics pipeline in {self.config.env} environment")
        
        # Initialize components
        self.stream_processor = StreamProcessor()
        self.s3_sink = S3Sink()
        self.sagemaker_client = SageMakerClient()
        self.monitor = CloudWatchMonitor()
        
        # Pipeline state
        self.running = False
        self.consumer_thread = None
        self.monitoring_thread = None
    
    def start_iot_simulation(self, device_count: int, readings_per_second: int, 
                           duration_seconds: Optional[int] = None) -> None:
        """
        Start a simulation of IoT devices sending data to Kafka.
        
        Args:
            device_count: Number of devices to simulate.
            readings_per_second: Readings per second (per device).
            duration_seconds: Duration of simulation (None for indefinite).
        """
        logger.info(f"Starting IoT simulation with {device_count} devices " +
                   f"at {readings_per_second} readings/sec")
        
        producer = IoTProducer()
        
        # Calculate interval in milliseconds
        interval_ms = 1000 // readings_per_second
        
        start_time = time.time()
        iteration = 0
        
        try:
            while duration_seconds is None or time.time() - start_time < duration_seconds:
                # Simulate one batch of readings
                producer.simulate_device_stream(
                    device_count=device_count,
                    readings_per_device=1,
                    interval_ms=interval_ms
                )
                
                iteration += 1
                if iteration % 10 == 0:
                    logger.info(f"Simulation running for {time.time() - start_time:.1f} seconds")
                
                # Sleep to maintain the rate
                time.sleep(1.0 / readings_per_second)
        
        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        
        finally:
            # Cleanup
            producer.flush()
            producer.close()
            
            # Report metrics
            metrics = producer.get_metrics()
            logger.info(f"Simulation complete. Metrics: {json.dumps(metrics)}")
            
            # Record metrics in CloudWatch
            self.monitor.monitor_component("IoTProducer", metrics)
    
    def start_processing(self) -> None:
        """Start the Kafka processing pipeline."""
        if self.running:
            logger.warning("Pipeline is already running")
            return
        
        self.running = True
        
        # Start the consumer thread
        self.consumer_thread = threading.Thread(
            target=self._run_consumer,
            daemon=True
        )
        self.consumer_thread.start()
        
        # Start the monitoring thread
        self.monitoring_thread = threading.Thread(
            target=self._run_monitoring,
            daemon=True
        )
        self.monitoring_thread.start()
        
        logger.info("Processing pipeline started")
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _run_consumer(self) -> None:
        """Run the Kafka consumer processing loop."""
        try:
            # Create consumer
            consumer = KafkaConsumer(
                self.config.kafka.topics["raw_data"],
                bootstrap_servers=self.config.kafka.brokers,
                group_id=self.config.kafka.consumer_group,
                auto_offset_reset='latest',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                enable_auto_commit=True,
                auto_commit_interval_ms=5000
            )
            
            logger.info(f"Consumer started, listening to {self.config.kafka.topics['raw_data']}")
            
            # Process messages
            for message in consumer:
                if not self.running:
                    break
                
                try:
                    # Extract value
                    value = message.value
                    
                    # Process the message
                    processed = self.stream_processor.process_message(value)
                    
                    # Write to S3
                    self.s3_sink.write(processed)
                    
                    # Invoke ML inference if needed
                    if "aggregations" in processed:
                        inference_result = self.sagemaker_client.invoke_endpoint(processed)
                        logger.debug(f"Inference result: {inference_result}")
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
            
            # Cleanup
            consumer.close()
            
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.running = False
    
    def _run_monitoring(self) -> None:
        """Run the monitoring thread to collect and publish metrics."""
        try:
            while self.running:
                # Collect metrics from all components
                processor_metrics = self.stream_processor.get_metrics()
                storage_metrics = self.s3_sink.get_metrics()
                ml_metrics = self.sagemaker_client.get_metrics()
                
                # Publish to CloudWatch
                self.monitor.monitor_component("StreamProcessor", processor_metrics)
                self.monitor.monitor_component("S3Sink", storage_metrics)
                self.monitor.monitor_component("SageMakerClient", ml_metrics)
                
                # Force publication of all metrics
                self.monitor.publish_metrics()
                
                # Check SLA compliance if we have latency metrics
                if "latency_ms_sum" in processor_metrics and processor_metrics["latency_ms_count"] > 0:
                    avg_latency = processor_metrics["latency_ms_sum"] / processor_metrics["latency_ms_count"]
                    sla_result = self.monitor.check_sla_compliance([avg_latency])
                    logger.info(f"SLA compliance: {sla_result}")
                
                # Sleep for a bit
                time.sleep(30)
                
        except Exception as e:
            logger.error(f"Monitoring error: {e}")
            self.running = False
    
    def _signal_handler(self, sig, frame) -> None:
        """Handle termination signals."""
        logger.info(f"Received signal {sig}, shutting down...")
        self.stop()
    
    def stop(self) -> None:
        """Stop the processing pipeline."""
        if not self.running:
            return
        
        logger.info("Stopping pipeline...")
        self.running = False
        
        # Wait for threads to terminate
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5)
        
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        
        # Flush any buffered data
        self.s3_sink.flush()
        
        # Final metrics publication
        self.monitor.publish_metrics()
        
        logger.info("Pipeline stopped")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Kafka AI Analytics Pipeline')
    parser.add_argument('--env', choices=['dev', 'test', 'prod'], default='dev',
                        help='Environment (dev, test, prod)')
    parser.add_argument('--action', choices=['simulate', 'process'], required=True,
                        help='Action to perform')
    parser.add_argument('--device-count', type=int, default=10,
                        help='Number of devices to simulate')
    parser.add_argument('--readings-per-second', type=int, default=1,
                        help='Readings per second per device')
    parser.add_argument('--duration', type=int, 
                        help='Duration of simulation in seconds')
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = KafkaAnalyticsPipeline(env=args.env)
    
    try:
        if args.action == 'simulate':
            # Start IoT simulation
            pipeline.start_iot_simulation(
                device_count=args.device_count,
                readings_per_second=args.readings_per_second,
                duration_seconds=args.duration
            )
        elif args.action == 'process':
            # Start processing pipeline
            pipeline.start_processing()
            
            # Keep the main thread alive
            while pipeline.running:
                time.sleep(1)
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    finally:
        pipeline.stop()


if __name__ == "__main__":
    main() 