"""
Data utilities for loading and preprocessing medical reasoning dataset.
"""

import json
import os
from typing import Dict, List, Optional, Tuple
import logging

import torch
from datasets import load_dataset, Dataset
from torch.utils.data import DataLoader, IterableDataset
import numpy as np

logger = logging.getLogger(__name__)


class MedicalDataset:
    """Load and preprocess medical reasoning dataset."""
    
    @staticmethod
    def from_huggingface(
        dataset_name: str = "FreedomIntelligence/medical-o1-reasoning-SFT",
        config: str = "en",
        split: str = "train",
        cache_dir: Optional[str] = None,
        max_samples: Optional[int] = None,
    ) -> Dataset:
        """
        Load dataset from HuggingFace.
        
        Args:
            dataset_name: HuggingFace dataset identifier
            config: Dataset config ('en', 'zh', 'en_mix', 'zh_mix' for medical-o1-reasoning-SFT)
            split: Dataset split ('train', 'validation', etc.)
            cache_dir: Cache directory for downloaded data
            max_samples: Maximum number of samples to load (for testing)
            
        Returns:
            HuggingFace Dataset object
        """
        logger.info(f"Loading {dataset_name} ({config}) from HuggingFace...")
        
        try:
            dataset = load_dataset(
                dataset_name,
                config,
                cache_dir=cache_dir,
                split=split,
            )
        except Exception as e:
            logger.error(f"Failed to load dataset: {e}")
            raise
        
        # Normalize column names for consistency
        # The medical-o1-reasoning-SFT dataset uses: Question, Complex_CoT, Response
        # Rename to standard: question, response
        original_columns = dataset.column_names
        if 'Question' in original_columns:
            dataset = dataset.rename_column('Question', 'question')
        if 'Complex_CoT' in original_columns:
            dataset = dataset.rename_column('Complex_CoT', 'response')
        elif 'Response' in original_columns and 'response' not in dataset.column_names:
            dataset = dataset.rename_column('Response', 'response')
        
        # Select only question and response columns if there are extras
        if 'question' in dataset.column_names and 'response' in dataset.column_names:
            dataset = dataset.select_columns(['question', 'response'])
        
        if max_samples and max_samples < len(dataset):
            logger.info(f"Limiting dataset to {max_samples} samples")
            dataset = dataset.select(range(max_samples))
        
        logger.info(f"Loaded {len(dataset)} samples from {split} split")
        logger.info(f"Dataset columns: {dataset.column_names}")
        
        return dataset
    
    @staticmethod
    def from_json(
        json_path: str,
        max_samples: Optional[int] = None,
    ) -> Dataset:
        """
        Load dataset from JSON file.
        
        Args:
            json_path: Path to JSON file
            max_samples: Maximum number of samples to load
            
        Returns:
            HuggingFace Dataset object
        """
        logger.info(f"Loading dataset from {json_path}...")
        
        with open(json_path, 'r') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            data = [data]
        
        if max_samples:
            data = data[:max_samples]
        
        # Convert to HuggingFace Dataset
        dataset = Dataset.from_dict({
            'question': [item.get('question', '') for item in data],
            'response': [item.get('response', '') for item in data],
        })
        
        logger.info(f"Loaded {len(dataset)} samples from JSON")
        
        return dataset
    
    @staticmethod
    def preprocess_for_sft(
        dataset: Dataset,
        tokenizer,
        max_length: int = 2048,
        num_proc: int = 4,
    ) -> Dataset:
        """
        Preprocess dataset for SFT training.
        
        Args:
            dataset: Input dataset
            tokenizer: Tokenizer to use
            max_length: Maximum sequence length
            num_proc: Number of processes for mapping
            
        Returns:
            Preprocessed dataset
        """
        def preprocess_function(examples):
            # Combine question and response
            texts = []
            for q, r in zip(examples['question'], examples['response']):
                # Format: Q: [question]\nA: [response]
                text = f"Q: {q}\nA: {r}"
                texts.append(text)
            
            # Tokenize
            tokenized = tokenizer(
                texts,
                truncation=True,
                max_length=max_length,
                padding=False,
                return_tensors=None,
            )
            
            # Create labels (for language modeling, labels = input_ids)
            tokenized['labels'] = tokenized['input_ids'].copy()
            
            return tokenized
        
        logger.info("Preprocessing dataset for SFT...")
        processed_dataset = dataset.map(
            preprocess_function,
            batched=True,
            num_proc=num_proc,
            remove_columns=dataset.column_names,
        )
        
        return processed_dataset
    
    @staticmethod
    def preprocess_for_grpo(
        dataset: Dataset,
        tokenizer,
        max_length: int = 2048,
        num_proc: int = 4,
    ) -> Dataset:
        """
        Preprocess dataset for GRPO training.
        
        Args:
            dataset: Input dataset
            tokenizer: Tokenizer to use
            max_length: Maximum sequence length
            num_proc: Number of processes for mapping
            
        Returns:
            Preprocessed dataset with questions only
        """
        def preprocess_function(examples):
            # For GRPO, we only need the questions as prompts
            prompts = examples['question']
            
            # Tokenize prompts
            tokenized = tokenizer(
                prompts,
                truncation=True,
                max_length=max_length // 2,  # Leave room for generation
                padding=False,
                return_tensors=None,
            )
            
            return tokenized
        
        logger.info("Preprocessing dataset for GRPO...")
        processed_dataset = dataset.map(
            preprocess_function,
            batched=True,
            num_proc=num_proc,
            remove_columns=dataset.column_names,
        )
        
        return processed_dataset


class DataCollatorForSFT:
    """Data collator for SFT training with dynamic padding."""
    
    def __init__(self, tokenizer, pad_to_multiple_of: int = 8):
        self.tokenizer = tokenizer
        self.pad_to_multiple_of = pad_to_multiple_of
    
    def __call__(self, examples: List[Dict]) -> Dict:
        """
        Collate examples into a batch with dynamic padding.
        
        Args:
            examples: List of tokenized examples
            
        Returns:
            Padded batch
        """
        # Pad input_ids and attention_mask
        input_ids = [ex['input_ids'] for ex in examples]
        attention_mask = [ex['attention_mask'] for ex in examples]
        labels = [ex['labels'] for ex in examples]
        
        # Find max length in batch
        max_length = max(len(x) for x in input_ids)
        
        # Pad to multiple of 8
        if self.pad_to_multiple_of:
            max_length = (
                (max_length + self.pad_to_multiple_of - 1) 
                // self.pad_to_multiple_of 
                * self.pad_to_multiple_of
            )
        
        batch = {
            'input_ids': [],
            'attention_mask': [],
            'labels': [],
        }
        
        for input_id, mask, label in zip(input_ids, attention_mask, labels):
            # Pad
            pad_length = max_length - len(input_id)
            padded_input_id = input_id + [self.tokenizer.pad_token_id] * pad_length
            padded_mask = mask + [0] * pad_length
            padded_label = label + [-100] * pad_length  # -100 ignored in loss
            
            batch['input_ids'].append(padded_input_id)
            batch['attention_mask'].append(padded_mask)
            batch['labels'].append(padded_label)
        
        # Convert to tensors
        batch = {
            k: torch.tensor(v, dtype=torch.long)
            for k, v in batch.items()
        }
        
        return batch


class DataCollatorForGRPO:
    """Data collator for GRPO training."""
    
    def __init__(self, tokenizer, pad_to_multiple_of: int = 8):
        self.tokenizer = tokenizer
        self.pad_to_multiple_of = pad_to_multiple_of
    
    def __call__(self, examples: List[Dict]) -> Dict:
        """
        Collate examples for GRPO (prompt-only).
        
        Args:
            examples: List of tokenized examples
            
        Returns:
            Padded batch of prompts
        """
        input_ids = [ex['input_ids'] for ex in examples]
        attention_mask = [ex['attention_mask'] for ex in examples]
        
        max_length = max(len(x) for x in input_ids)
        
        if self.pad_to_multiple_of:
            max_length = (
                (max_length + self.pad_to_multiple_of - 1) 
                // self.pad_to_multiple_of 
                * self.pad_to_multiple_of
            )
        
        batch = {
            'input_ids': [],
            'attention_mask': [],
        }
        
        for input_id, mask in zip(input_ids, attention_mask):
            pad_length = max_length - len(input_id)
            padded_input_id = input_id + [self.tokenizer.pad_token_id] * pad_length
            padded_mask = mask + [0] * pad_length
            
            batch['input_ids'].append(padded_input_id)
            batch['attention_mask'].append(padded_mask)
        
        batch = {
            k: torch.tensor(v, dtype=torch.long)
            for k, v in batch.items()
        }
        
        return batch
