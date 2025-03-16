"""
SageMaker client for ML model inference and training.
"""
import json
import logging
import time
from typing import Dict, Any, List, Optional, Union
import boto3
import numpy as np
import pandas as pd

from ..config import config

logger = logging.getLogger(__name__)

class SageMakerClient:
    """
    Client for interacting with Amazon SageMaker for model training and inference.
    """
    
    def __init__(self, endpoint_name: Optional[str] = None):
        """
        Initialize the SageMaker client.
        
        Args:
            endpoint_name: SageMaker endpoint name (defaults to config value).
        """
        self.config = config.sagemaker
        self.endpoint_name = endpoint_name or self.config.endpoint_name
        self.training_job_name = self.config.training_job_name
        
        # Initialize AWS clients
        self.sagemaker_runtime = boto3.client('sagemaker-runtime')
        self.sagemaker = boto3.client('sagemaker')
        
        # Metrics
        self.metrics = {
            "inference_count": 0,
            "inference_errors": 0,
            "inference_latency_ms_sum": 0,
            "training_jobs_started": 0
        }
    
    def invoke_endpoint(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Invoke a SageMaker endpoint for inference.
        
        Args:
            features: Dictionary of feature values for inference.
            
        Returns:
            Dictionary with inference results.
        """
        start_time = time.time()
        try:
            # Convert features to the format expected by the model
            payload = self._prepare_payload(features)
            
            # Invoke the endpoint
            response = self.sagemaker_runtime.invoke_endpoint(
                EndpointName=self.endpoint_name,
                ContentType='application/json',
                Body=json.dumps(payload)
            )
            
            # Parse the response
            result = json.loads(response['Body'].read().decode())
            
            # Update metrics
            self.metrics["inference_count"] += 1
            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000
            self.metrics["inference_latency_ms_sum"] += latency_ms
            
            # Return the result with latency info
            return {
                "prediction": result,
                "latency_ms": latency_ms
            }
            
        except Exception as e:
            logger.error(f"Error invoking SageMaker endpoint: {e}")
            self.metrics["inference_errors"] += 1
            
            # Return error information
            return {
                "error": str(e),
                "latency_ms": (time.time() - start_time) * 1000
            }
    
    def batch_inference(self, feature_batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Perform inference on a batch of features.
        
        Args:
            feature_batch: List of feature dictionaries.
            
        Returns:
            List of inference results.
        """
        results = []
        for features in feature_batch:
            result = self.invoke_endpoint(features)
            results.append(result)
        return results
    
    def start_training_job(self, 
                          s3_input_path: str, 
                          s3_output_path: str,
                          instance_type: str = 'ml.m5.xlarge',
                          instance_count: int = 1,
                          max_runtime_seconds: int = 3600) -> Dict[str, Any]:
        """
        Start a SageMaker training job.
        
        Args:
            s3_input_path: S3 URI for training data.
            s3_output_path: S3 URI for model output.
            instance_type: SageMaker instance type.
            instance_count: Number of instances.
            max_runtime_seconds: Maximum runtime in seconds.
            
        Returns:
            Dictionary with training job information.
        """
        try:
            # Generate a unique job name
            timestamp = int(time.time())
            job_name = f"{self.training_job_name}-{timestamp}"
            
            # Start the training job
            response = self.sagemaker.create_training_job(
                TrainingJobName=job_name,
                AlgorithmSpecification={
                    'TrainingImage': self._get_algorithm_image(),
                    'TrainingInputMode': 'File'
                },
                RoleArn=self._get_role_arn(),
                InputDataConfig=[
                    {
                        'ChannelName': 'train',
                        'DataSource': {
                            'S3DataSource': {
                                'S3DataType': 'S3Prefix',
                                'S3Uri': s3_input_path,
                                'S3DataDistributionType': 'FullyReplicated'
                            }
                        },
                        'ContentType': 'application/x-parquet',
                        'CompressionType': 'None'
                    }
                ],
                OutputDataConfig={
                    'S3OutputPath': s3_output_path
                },
                ResourceConfig={
                    'InstanceType': instance_type,
                    'InstanceCount': instance_count,
                    'VolumeSizeInGB': 30
                },
                StoppingCondition={
                    'MaxRuntimeInSeconds': max_runtime_seconds
                },
                HyperParameters={
                    'feature_dim': '10',
                    'epochs': '10',
                    'learning_rate': '0.01'
                }
            )
            
            # Update metrics
            self.metrics["training_jobs_started"] += 1
            
            return {
                "job_name": job_name,
                "status": "InProgress",
                "response": response
            }
            
        except Exception as e:
            logger.error(f"Error starting SageMaker training job: {e}")
            return {
                "error": str(e),
                "status": "Failed"
            }
    
    def deploy_model(self, model_s3_uri: str, instance_type: str = 'ml.t2.medium', 
                     instance_count: int = 1) -> Dict[str, Any]:
        """
        Deploy a model to a SageMaker endpoint.
        
        Args:
            model_s3_uri: S3 URI for the model artifacts.
            instance_type: Instance type for deployment.
            instance_count: Number of instances.
            
        Returns:
            Dictionary with deployment information.
        """
        try:
            # Create model
            timestamp = int(time.time())
            model_name = f"{self.endpoint_name}-model-{timestamp}"
            
            self.sagemaker.create_model(
                ModelName=model_name,
                PrimaryContainer={
                    'Image': self._get_inference_image(),
                    'ModelDataUrl': model_s3_uri
                },
                ExecutionRoleArn=self._get_role_arn()
            )
            
            # Create endpoint config
            endpoint_config_name = f"{self.endpoint_name}-config-{timestamp}"
            
            self.sagemaker.create_endpoint_config(
                EndpointConfigName=endpoint_config_name,
                ProductionVariants=[
                    {
                        'VariantName': 'default',
                        'ModelName': model_name,
                        'InitialInstanceCount': instance_count,
                        'InstanceType': instance_type
                    }
                ]
            )
            
            # Create or update endpoint
            if self._endpoint_exists():
                response = self.sagemaker.update_endpoint(
                    EndpointName=self.endpoint_name,
                    EndpointConfigName=endpoint_config_name
                )
                status = "Updating"
            else:
                response = self.sagemaker.create_endpoint(
                    EndpointName=self.endpoint_name,
                    EndpointConfigName=endpoint_config_name
                )
                status = "Creating"
            
            return {
                "endpoint_name": self.endpoint_name,
                "model_name": model_name,
                "endpoint_config": endpoint_config_name,
                "status": status,
                "response": response
            }
            
        except Exception as e:
            logger.error(f"Error deploying model to SageMaker: {e}")
            return {
                "error": str(e),
                "status": "Failed"
            }
    
    def _endpoint_exists(self) -> bool:
        """Check if the endpoint already exists."""
        try:
            self.sagemaker.describe_endpoint(EndpointName=self.endpoint_name)
            return True
        except self.sagemaker.exceptions.ClientError:
            return False
    
    def _get_algorithm_image(self) -> str:
        """Get the algorithm container image URI."""
        # This would typically be a pre-built SageMaker algorithm or custom container
        # For simplicity, using a placeholder for a TensorFlow image
        return "763104351884.dkr.ecr.us-east-1.amazonaws.com/tensorflow-training:2.6.0-cpu-py38-ubuntu20.04"
    
    def _get_inference_image(self) -> str:
        """Get the inference container image URI."""
        # This would typically be a pre-built SageMaker algorithm or custom container
        # For simplicity, using a placeholder for a TensorFlow image
        return "763104351884.dkr.ecr.us-east-1.amazonaws.com/tensorflow-inference:2.6.0-cpu"
    
    def _get_role_arn(self) -> str:
        """Get the IAM role ARN for SageMaker."""
        # In a real implementation, this would be provided via config
        return "arn:aws:iam::123456789012:role/service-role/AmazonSageMaker-ExecutionRole"
    
    def _prepare_payload(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare the payload for SageMaker inference.
        
        Args:
            features: Raw feature dictionary.
            
        Returns:
            Formatted payload for the model.
        """
        # Extract numeric and categorical features
        numeric_features = {}
        categorical_features = {}
        
        for key, value in features.items():
            if isinstance(value, (int, float)):
                numeric_features[key] = value
            elif isinstance(value, str):
                categorical_features[key] = value
        
        # Create a payload with the expected format
        payload = {
            "numeric_features": numeric_features,
            "categorical_features": categorical_features
        }
        
        # Include any aggregations if present
        if "aggregations" in features:
            payload["aggregations"] = features["aggregations"]
        
        return payload
    
    def get_average_inference_latency(self) -> float:
        """
        Get the average inference latency in milliseconds.
        
        Returns:
            Average latency or 0 if no inferences performed.
        """
        if self.metrics["inference_count"] == 0:
            return 0
        return self.metrics["inference_latency_ms_sum"] / self.metrics["inference_count"]
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get client metrics.
        
        Returns:
            Dictionary of metrics.
        """
        metrics = dict(self.metrics)
        if self.metrics["inference_count"] > 0:
            metrics["average_inference_latency_ms"] = self.get_average_inference_latency()
        return metrics 