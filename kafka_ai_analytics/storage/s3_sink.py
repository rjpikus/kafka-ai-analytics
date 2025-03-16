"""
S3 sink for storing processed data in Parquet format.
"""
import io
import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import config

logger = logging.getLogger(__name__)

class S3Sink:
    """
    S3 sink for storing processed data from Kafka in Parquet format.
    Implements the partitioning by date/hour as mentioned in the README.
    """
    
    def __init__(self, bucket_name: Optional[str] = None, prefix: Optional[str] = None):
        """
        Initialize the S3 sink.
        
        Args:
            bucket_name: S3 bucket name (defaults to config value).
            prefix: S3 key prefix (defaults to config value).
        """
        self.config = config.s3
        self.bucket_name = bucket_name or self.config.bucket_name
        self.prefix = prefix or self.config.prefix
        self.partition_format = self.config.partition_format
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3')
        
        # Buffering for batch writes
        self.buffer = []
        self.buffer_size_limit = 1000  # Messages to buffer before writing
        self.last_flush_time = time.time()
        self.flush_interval_seconds = 60  # Force flush after this many seconds
        
        # Metrics
        self.metrics = {
            "writes": 0,
            "batches": 0,
            "write_errors": 0,
            "bytes_written": 0
        }
    
    def write(self, data: Dict[str, Any]) -> None:
        """
        Write a data record to S3 (buffers until batch size is reached).
        
        Args:
            data: The data record to write.
        """
        # Add to buffer
        self.buffer.append(data)
        
        # Check if we should flush
        current_time = time.time()
        if (len(self.buffer) >= self.buffer_size_limit or
                current_time - self.last_flush_time >= self.flush_interval_seconds):
            self.flush()
    
    def write_batch(self, batch: List[Dict[str, Any]]) -> None:
        """
        Write a batch of data records to S3.
        
        Args:
            batch: List of data records to write.
        """
        self.buffer.extend(batch)
        self.flush()
    
    def flush(self) -> bool:
        """
        Flush the buffer to S3 in Parquet format.
        
        Returns:
            True if successful, False otherwise.
        """
        if not self.buffer:
            return True
        
        try:
            # Group records by partition (date/hour)
            partitioned_data = self._partition_data(self.buffer)
            
            # Write each partition
            for partition_key, records in partitioned_data.items():
                self._write_partition(partition_key, records)
            
            # Update metrics
            self.metrics["writes"] += len(self.buffer)
            self.metrics["batches"] += 1
            
            # Clear buffer
            self.buffer = []
            self.last_flush_time = time.time()
            
            return True
        
        except Exception as e:
            logger.error(f"Error flushing data to S3: {e}")
            self.metrics["write_errors"] += 1
            return False
    
    def _partition_data(self, records: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Partition data by date/hour.
        
        Args:
            records: List of data records.
            
        Returns:
            Dictionary of partitioned data: partition_key -> list of records.
        """
        partitioned = {}
        
        for record in records:
            # Extract timestamp
            timestamp_str = record.get("timestamp") or record.get("processing_timestamp")
            
            if timestamp_str:
                try:
                    # Parse the timestamp
                    timestamp = datetime.fromisoformat(timestamp_str)
                    
                    # Create partition key
                    partition_key = self.partition_format.format(
                        year=timestamp.year,
                        month=f"{timestamp.month:02d}",
                        day=f"{timestamp.day:02d}",
                        hour=f"{timestamp.hour:02d}"
                    )
                    
                    # Add to partition
                    if partition_key not in partitioned:
                        partitioned[partition_key] = []
                    partitioned[partition_key].append(record)
                    
                except (ValueError, TypeError):
                    # Fall back to generic partition if timestamp is invalid
                    generic_key = "unknown_partition"
                    if generic_key not in partitioned:
                        partitioned[generic_key] = []
                    partitioned[generic_key].append(record)
            else:
                # Fall back to generic partition if no timestamp
                generic_key = "unknown_partition"
                if generic_key not in partitioned:
                    partitioned[generic_key] = []
                partitioned[generic_key].append(record)
        
        return partitioned
    
    def _write_partition(self, partition_key: str, records: List[Dict[str, Any]]) -> None:
        """
        Write a partition of records to S3 in Parquet format.
        
        Args:
            partition_key: The partition key (e.g., "year=2023/month=09/day=01/hour=15").
            records: List of records for this partition.
        """
        # Create DataFrame from records
        df = pd.DataFrame(records)
        
        # Create a unique filename with timestamp
        timestamp = int(time.time())
        filename = f"data_{timestamp}_{len(records)}.parquet"
        
        # Create the complete S3 key
        s3_key = f"{self.prefix}/{partition_key}/{filename}"
        
        # Convert to Parquet and write to S3
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
        
        # Get buffer size for metrics
        parquet_buffer.seek(0)
        buffer_size = len(parquet_buffer.getvalue())
        
        # Upload to S3
        parquet_buffer.seek(0)
        self.s3_client.upload_fileobj(
            parquet_buffer, 
            self.bucket_name, 
            s3_key
        )
        
        # Update metrics
        self.metrics["bytes_written"] += buffer_size
        
        logger.info(f"Wrote {len(records)} records to s3://{self.bucket_name}/{s3_key} ({buffer_size} bytes)")
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get sink metrics.
        
        Returns:
            Dictionary of metrics.
        """
        return dict(self.metrics) 