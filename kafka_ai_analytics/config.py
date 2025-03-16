"""
Configuration module for Kafka AI Analytics.
"""
import os
from pydantic import BaseSettings
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

class KafkaConfig(BaseSettings):
    """Kafka configuration settings."""
    brokers: List[str] = ["localhost:9092"]
    replication_factor: int = 3
    partitions: int = 12
    topics: Dict[str, str] = {
        "raw_data": "raw-data",
        "processed_features": "processed-features"
    }
    consumer_group: str = "ai-analytics-group"
    
    class Config:
        env_prefix = "KAFKA_"

class MSKConfig(BaseSettings):
    """AWS MSK configuration settings."""
    node_config: Dict[str, Any] = {
        "vcpu": 4,
        "memory_gb": 16
    }
    enabled: bool = False
    
    class Config:
        env_prefix = "MSK_"

class S3Config(BaseSettings):
    """S3 configuration settings."""
    bucket_name: str = "ai-analytics-data"
    prefix: str = "data"
    partition_format: str = "year={}/month={}/day={}/hour={}"
    
    class Config:
        env_prefix = "S3_"

class LambdaConfig(BaseSettings):
    """Lambda configuration settings."""
    memory_mb: int = 128
    max_concurrent: int = 10000
    timeout_seconds: int = 60
    
    class Config:
        env_prefix = "LAMBDA_"

class SageMakerConfig(BaseSettings):
    """SageMaker configuration settings."""
    endpoint_name: str = "ai-analytics-endpoint"
    training_job_name: str = "ai-analytics-training"
    
    class Config:
        env_prefix = "SAGEMAKER_"

class KinesisConfig(BaseSettings):
    """Kinesis configuration settings."""
    stream_name: str = "ai-analytics-overflow"
    shard_count: int = 100
    
    class Config:
        env_prefix = "KINESIS_"

class CloudWatchConfig(BaseSettings):
    """CloudWatch configuration settings."""
    namespace: str = "AI/Analytics"
    metrics: List[str] = ["Latency", "Throughput", "ErrorRate", "DataLoss"]
    
    class Config:
        env_prefix = "CW_"

class AppConfig(BaseSettings):
    """Main application configuration."""
    env: str = os.getenv("APP_ENV", "dev")
    kafka: KafkaConfig = KafkaConfig()
    msk: MSKConfig = MSKConfig()
    s3: S3Config = S3Config()
    lambda_: LambdaConfig = LambdaConfig()
    sagemaker: SageMakerConfig = SageMakerConfig()
    kinesis: KinesisConfig = KinesisConfig()
    cloudwatch: CloudWatchConfig = CloudWatchConfig()
    
    class Config:
        env_prefix = "APP_"

# Create config instances for different environments
def get_config(env: Optional[str] = None) -> AppConfig:
    """Get configuration for the specified environment."""
    environment = env or os.getenv("APP_ENV", "dev")
    
    # Default config
    config = AppConfig()
    
    # Override with environment-specific settings
    if environment == "dev":
        # Development environment settings
        pass
    elif environment == "test":
        # Test environment settings
        config.kafka.topics = {
            "raw_data": "raw-data-test",
            "processed_features": "processed-features-test"
        }
        config.s3.bucket_name = "ai-analytics-data-test"
    elif environment == "prod":
        # Production environment settings
        config.msk.enabled = True
        config.s3.bucket_name = "ai-analytics-data-prod"
    
    return config

# Default config instance
config = get_config() 