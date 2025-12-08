from setuptools import setup, find_packages

setup(
    name="medical-o1-sft-grpo",
    version="0.1.0",
    description="SFT + GRPO training pipeline for medical reasoning",
    author="Medical AI Research",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "torch>=2.0.0",
        "transformers>=4.35.0",
        "datasets>=2.14.0",
        "trl>=0.7.0",
        "peft>=0.7.0",
    ],
    extras_require={
        "dev": ["pytest", "black", "isort"],
    },
)
