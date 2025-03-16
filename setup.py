"""
Setup script for the kafka_ai_analytics package.
"""
from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = fh.read().splitlines()

setup(
    name="kafka_ai_analytics",
    version="0.1.0",
    description="Kafka Integration for AI Analytics",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/kafka-ai-analytics",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "kafka-ai-analytics=kafka_ai_analytics.main:main",
        ],
    },
) 