"""
CloudWatch metrics for monitoring the Kafka AI analytics pipeline.
"""
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
import boto3

from ..config import config

logger = logging.getLogger(__name__)

class CloudWatchMonitor:
    """
    CloudWatch monitoring for Kafka AI analytics pipeline.
    Collects and publishes metrics for latency, throughput, and SLA compliance.
    """
    
    def __init__(self, namespace: Optional[str] = None):
        """
        Initialize the CloudWatch monitor.
        
        Args:
            namespace: CloudWatch namespace (defaults to config value).
        """
        self.config = config.cloudwatch
        self.namespace = namespace or self.config.namespace
        
        # Initialize CloudWatch client
        self.cloudwatch = boto3.client('cloudwatch')
        
        # Last time the metrics were published
        self.last_publish_time = time.time()
        self.publish_interval_seconds = 60  # Publish metrics every minute
        
        # Metrics buffer
        self.metrics_buffer = {}
    
    def record_metric(self, metric_name: str, value: float, 
                     unit: str = 'Count', dimensions: Optional[Dict[str, str]] = None) -> None:
        """
        Record a metric to be published to CloudWatch.
        
        Args:
            metric_name: The name of the metric.
            value: The value of the metric.
            unit: The unit of the metric.
            dimensions: Optional dimensions for the metric.
        """
        if dimensions is None:
            dimensions = {}
        
        # Create a key for this metric and dimensions
        dimension_key = ','.join([f"{k}={v}" for k, v in sorted(dimensions.items())])
        key = f"{metric_name}:{dimension_key}"
        
        # Add to buffer
        if key not in self.metrics_buffer:
            self.metrics_buffer[key] = {
                'MetricName': metric_name,
                'Dimensions': [{'Name': k, 'Value': v} for k, v in dimensions.items()],
                'Unit': unit,
                'Values': [],
                'Timestamps': []
            }
        
        self.metrics_buffer[key]['Values'].append(value)
        self.metrics_buffer[key]['Timestamps'].append(datetime.now())
        
        # Check if we should publish
        if time.time() - self.last_publish_time >= self.publish_interval_seconds:
            self.publish_metrics()
    
    def publish_metrics(self) -> bool:
        """
        Publish metrics to CloudWatch.
        
        Returns:
            True if successful, False otherwise.
        """
        if not self.metrics_buffer:
            return True
        
        try:
            # Prepare metrics data
            metrics_data = []
            
            for metric_key, metric_data in self.metrics_buffer.items():
                # For each buffered metric, create statistics
                values = metric_data['Values']
                if not values:
                    continue
                
                # Create a data point with statistics
                data_point = {
                    'MetricName': metric_data['MetricName'],
                    'Dimensions': metric_data['Dimensions'],
                    'Unit': metric_data['Unit'],
                    'StatisticValues': {
                        'SampleCount': len(values),
                        'Sum': sum(values),
                        'Minimum': min(values),
                        'Maximum': max(values)
                    },
                    'Timestamp': datetime.now()
                }
                
                metrics_data.append(data_point)
            
            if metrics_data:
                # Publish to CloudWatch
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=metrics_data
                )
                
                logger.info(f"Published {len(metrics_data)} metrics to CloudWatch namespace '{self.namespace}'")
            
            # Clear buffer and update last publish time
            self.metrics_buffer = {}
            self.last_publish_time = time.time()
            
            return True
            
        except Exception as e:
            logger.error(f"Error publishing metrics to CloudWatch: {e}")
            return False
    
    def record_latency(self, component: str, latency_ms: float) -> None:
        """
        Record latency metric for a component.
        
        Args:
            component: The component name.
            latency_ms: The latency in milliseconds.
        """
        self.record_metric(
            metric_name='Latency',
            value=latency_ms,
            unit='Milliseconds',
            dimensions={'Component': component}
        )
    
    def record_throughput(self, component: str, count: int) -> None:
        """
        Record throughput metric for a component.
        
        Args:
            component: The component name.
            count: The number of items processed.
        """
        self.record_metric(
            metric_name='Throughput',
            value=count,
            unit='Count',
            dimensions={'Component': component}
        )
    
    def record_error_rate(self, component: str, error_count: int, total_count: int) -> None:
        """
        Record error rate metric for a component.
        
        Args:
            component: The component name.
            error_count: The number of errors.
            total_count: The total number of operations.
        """
        if total_count > 0:
            error_rate = (error_count / total_count) * 100
        else:
            error_rate = 0
        
        self.record_metric(
            metric_name='ErrorRate',
            value=error_rate,
            unit='Percent',
            dimensions={'Component': component}
        )
    
    def record_data_loss(self, component: str, lost_count: int, total_count: int) -> None:
        """
        Record data loss metric for a component.
        
        Args:
            component: The component name.
            lost_count: The number of lost items.
            total_count: The total number of items.
        """
        if total_count > 0:
            loss_rate = (lost_count / total_count) * 100
        else:
            loss_rate = 0
        
        self.record_metric(
            metric_name='DataLoss',
            value=loss_rate,
            unit='Percent',
            dimensions={'Component': component}
        )
    
    def check_sla_compliance(self, latencies: List[float], threshold_ms: float = 100) -> Dict[str, Any]:
        """
        Check if latencies comply with SLA (99th percentile < threshold_ms).
        
        Args:
            latencies: List of latency values in milliseconds.
            threshold_ms: The latency threshold for SLA compliance.
            
        Returns:
            Dictionary with SLA compliance information.
        """
        if not latencies:
            return {
                "compliant": True,
                "p99_latency": 0,
                "threshold_ms": threshold_ms
            }
        
        import numpy as np
        
        # Calculate 99th percentile
        p99_latency = np.percentile(latencies, 99)
        
        # Check compliance
        compliant = p99_latency < threshold_ms
        
        # Record as metric
        self.record_metric(
            metric_name='P99Latency',
            value=p99_latency,
            unit='Milliseconds',
            dimensions={}
        )
        
        self.record_metric(
            metric_name='SLACompliance',
            value=1 if compliant else 0,
            unit='None',
            dimensions={}
        )
        
        return {
            "compliant": compliant,
            "p99_latency": p99_latency,
            "threshold_ms": threshold_ms
        }
    
    def monitor_component(self, component_name: str, metrics: Dict[str, Any]) -> None:
        """
        Record all metrics for a component.
        
        Args:
            component_name: The component name.
            metrics: Dictionary of component metrics.
        """
        # Record latency if available
        if "average_latency_ms" in metrics:
            self.record_latency(component_name, metrics["average_latency_ms"])
        elif "latency_ms_sum" in metrics and "latency_ms_count" in metrics and metrics["latency_ms_count"] > 0:
            avg_latency = metrics["latency_ms_sum"] / metrics["latency_ms_count"]
            self.record_latency(component_name, avg_latency)
        
        # Record throughput if available
        if "messages_sent" in metrics:
            self.record_throughput(component_name, metrics["messages_sent"])
        elif "processed_count" in metrics:
            self.record_throughput(component_name, metrics["processed_count"])
        elif "writes" in metrics:
            self.record_throughput(component_name, metrics["writes"])
        
        # Record error rate if available
        if "errors" in metrics and any(count_key in metrics for count_key in 
                                      ["messages_sent", "processed_count", "writes"]):
            total = (metrics.get("messages_sent", 0) + 
                    metrics.get("processed_count", 0) + 
                    metrics.get("writes", 0))
            
            if total > 0:
                self.record_error_rate(component_name, metrics["errors"], total) 