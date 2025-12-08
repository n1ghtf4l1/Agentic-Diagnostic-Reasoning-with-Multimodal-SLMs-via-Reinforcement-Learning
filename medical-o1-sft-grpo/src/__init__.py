"""
Medical O1 Reasoning: SFT + GRPO Training Pipeline

A comprehensive implementation of Supervised Fine-Tuning (SFT) followed by 
Group Relative Policy Optimization (GRPO) for medical reasoning using the 
FreedomIntelligence medical-o1-reasoning-SFT dataset.
"""

__version__ = "0.1.0"
__author__ = "Medical AI Research"
__license__ = "Apache 2.0"

# Keep top-level imports lightweight to avoid pulling optional heavy deps (e.g., PEFT, Transformers)
from src.data import MedicalDataset, DataCollatorForSFT, DataCollatorForGRPO

__all__ = [
    "MedicalDataset",
    "DataCollatorForSFT",
    "DataCollatorForGRPO",
]
