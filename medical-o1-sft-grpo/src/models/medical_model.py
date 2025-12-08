"""
Medical model utilities and reward functions.
"""

import logging
from typing import Optional, Dict, List, Tuple
import torch
import torch.nn.functional as F
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import LoraConfig, get_peft_model

logger = logging.getLogger(__name__)


class MedicalModel:
    """Utility class for loading and configuring medical reasoning models."""
    
    @staticmethod
    def load_model_and_tokenizer(
        model_name_or_path: str,
        device: str = "cuda",
        use_lora: bool = True,
        lora_rank: int = 16,
        lora_alpha: int = 32,
        lora_target_modules: Optional[List[str]] = None,
        torch_dtype: Optional[torch.dtype] = None,
    ) -> Tuple:
        """
        Load model and tokenizer with optional LoRA.
        
        Args:
            model_name_or_path: Model identifier from HuggingFace
            device: Device to load model on
            use_lora: Whether to apply LoRA adapters
            lora_rank: LoRA rank
            lora_alpha: LoRA scaling factor
            lora_target_modules: Modules to apply LoRA to
            torch_dtype: Data type for model (default: auto)
            
        Returns:
            Tuple of (model, tokenizer)
        """
        logger.info(f"Loading model from {model_name_or_path}...")
        
        # Default LoRA target modules for common models
        if lora_target_modules is None:
            lora_target_modules = [
                "q_proj", "k_proj", "v_proj", "o_proj",
                "gate_proj", "up_proj", "down_proj",
            ]
        
        # Load tokenizer
        tokenizer = AutoTokenizer.from_pretrained(
            model_name_or_path,
            trust_remote_code=True,
        )
        
        # Ensure padding works for decoder-only models
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
        # Left padding avoids generation warnings for causal LM
        tokenizer.padding_side = "left"
        
        # Load model
        model = AutoModelForCausalLM.from_pretrained(
            model_name_or_path,
            device_map=device,
            torch_dtype=torch_dtype or torch.float32,
            trust_remote_code=True,
        )
        
        # Apply LoRA if requested
        if use_lora:
            logger.info(f"Applying LoRA (rank={lora_rank}) to model...")
            
            lora_config = LoraConfig(
                r=lora_rank,
                lora_alpha=lora_alpha,
                target_modules=lora_target_modules,
                lora_dropout=0.05,
                bias="none",
                task_type="CAUSAL_LM",
            )
            
            model = get_peft_model(model, lora_config)
            model.print_trainable_parameters()
        
        logger.info(f" Model loaded successfully")
        
        return model, tokenizer
    
    @staticmethod
    def generate_responses(
        model,
        tokenizer,
        prompts: List[str],
        max_length: int = 1024,
        num_return_sequences: int = 1,
        temperature: float = 0.7,
        top_p: float = 0.9,
        do_sample: bool = True,
        device: str = "cuda",
    ) -> List[str]:
        """
        Generate responses for medical questions.
        
        Args:
            model: Model for generation
            tokenizer: Tokenizer
            prompts: Input prompts (questions)
            max_length: Maximum generation length
            num_return_sequences: Number of sequences to generate per prompt
            temperature: Sampling temperature
            top_p: Top-p sampling parameter
            do_sample: Whether to use sampling
            device: Device to use
            
        Returns:
            List of generated responses
        """
        model.eval()
        
        with torch.no_grad():
            # Tokenize prompts
            inputs = tokenizer(
                prompts,
                return_tensors="pt",
                padding=True,
                truncation=True,
                max_length=512,
            ).to(device)
            
            # Generate
            outputs = model.generate(
                **inputs,
                max_length=max_length,
                num_return_sequences=num_return_sequences,
                temperature=temperature,
                top_p=top_p,
                do_sample=do_sample,
                pad_token_id=tokenizer.pad_token_id,
                eos_token_id=tokenizer.eos_token_id,
            )
            
            # Decode
            responses = tokenizer.batch_decode(
                outputs,
                skip_special_tokens=True,
            )
        
        return responses


class RewardModel:
    """Compute rewards for medical reasoning responses."""
    
    @staticmethod
    def compute_semantic_similarity(
        generated: str,
        reference: str,
        model,
        tokenizer,
        device: str = "cuda",
    ) -> float:
        """
        Compute semantic similarity between generated and reference texts.
        Uses embeddings-based similarity (cosine).
        
        Args:
            generated: Generated response
            reference: Reference response
            model: Model with embedding capability
            tokenizer: Tokenizer
            device: Device to use
            
        Returns:
            Similarity score between 0 and 1
        """
        def get_embedding(text):
            inputs = tokenizer(
                text,
                return_tensors="pt",
                truncation=True,
                max_length=512,
            ).to(device)
            
            with torch.no_grad():
                outputs = model(**inputs, output_hidden_states=True)
                # Use mean pooling of last hidden state
                embedding = outputs.hidden_states[-1].mean(dim=1)
            
            return F.normalize(embedding, p=2, dim=-1)
        
        emb_generated = get_embedding(generated)
        emb_reference = get_embedding(reference)
        
        similarity = F.cosine_similarity(emb_generated, emb_reference).item()
        
        return (similarity + 1) / 2  # Normalize to [0, 1]
    
    @staticmethod
    def compute_length_reward(
        text: str,
        min_length: int = 100,
        max_length: int = 2000,
        penalty_factor: float = 0.1,
    ) -> float:
        """
        Reward based on response length.
        
        Encourages responses within a reasonable range.
        
        Args:
            text: Response text
            min_length: Minimum desired length
            max_length: Maximum desired length
            penalty_factor: Penalty for being outside range
            
        Returns:
            Length reward between 0 and 1
        """
        text_length = len(text.split())
        
        if min_length <= text_length <= max_length:
            return 1.0
        elif text_length < min_length:
            penalty = (min_length - text_length) / min_length
            return max(0, 1.0 - penalty_factor * penalty)
        else:  # text_length > max_length
            penalty = (text_length - max_length) / max_length
            return max(0, 1.0 - penalty_factor * penalty)
    
    @staticmethod
    def compute_medical_accuracy_reward(
        generated: str,
        reference: str,
        keyword_coverage: float = 0.6,
    ) -> float:
        """
        Estimate medical accuracy based on keyword coverage.
        
        Args:
            generated: Generated response
            reference: Reference response
            keyword_coverage: Threshold for keyword coverage
            
        Returns:
            Accuracy reward between 0 and 1
        """
        # Extract key medical terms (simplified: assume words with 5+ chars)
        ref_keywords = set(
            word.lower() 
            for word in reference.split() 
            if len(word) >= 5
        )
        gen_keywords = set(
            word.lower() 
            for word in generated.split() 
            if len(word) >= 5
        )
        
        if not ref_keywords:
            return 0.5  # Neutral if no keywords found
        
        overlap = len(ref_keywords & gen_keywords)
        coverage = overlap / len(ref_keywords)
        
        return min(1.0, coverage / keyword_coverage)
    
    @staticmethod
    def compute_reasoning_quality_reward(
        text: str,
        reasoning_keywords: Optional[List[str]] = None,
    ) -> float:
        """
        Score reasoning quality based on presence of reasoning patterns.
        
        Args:
            text: Response text
            reasoning_keywords: Keywords indicating good reasoning
            
        Returns:
            Reasoning quality reward between 0 and 1
        """
        if reasoning_keywords is None:
            reasoning_keywords = [
                "think", "reason", "because", "therefore", "thus",
                "consider", "analyze", "diagnose", "findings",
                "likely", "probably", "suggests", "indicates",
            ]
        
        text_lower = text.lower()
        keyword_count = sum(
            text_lower.count(kw) 
            for kw in reasoning_keywords
        )
        
        # Normalize by text length
        text_words = len(text.split())
        if text_words == 0:
            return 0.0
        
        keyword_density = keyword_count / (text_words / 100)
        
        # Optimal density is around 2-5 keywords per 100 words
        if keyword_density < 1:
            return keyword_density
        elif keyword_density > 5:
            return max(0, 1.0 - (keyword_density - 5) / 10)
        else:
            return 1.0
    
    @staticmethod
    def compute_combined_reward(
        generated: str,
        reference: str,
        model = None,
        tokenizer = None,
        device: str = "cuda",
        weights: Optional[Dict[str, float]] = None,
    ) -> float:
        """
        Compute combined reward from multiple signals.
        
        Args:
            generated: Generated response
            reference: Reference response
            model: Optional model for semantic similarity
            tokenizer: Optional tokenizer for semantic similarity
            device: Device to use
            weights: Dictionary of reward weights
            
        Returns:
            Combined reward between 0 and 1
        """
        if weights is None:
            weights = {
                'similarity': 0.3,
                'length': 0.2,
                'accuracy': 0.3,
                'reasoning': 0.2,
            }
        
        rewards = {}
        
        # Semantic similarity (if model provided)
        if model is not None and tokenizer is not None:
            try:
                rewards['similarity'] = RewardModel.compute_semantic_similarity(
                    generated, reference, model, tokenizer, device
                )
            except Exception:
                rewards['similarity'] = 0.5  # Fallback
        else:
            rewards['similarity'] = 0.5
        
        # Length reward
        rewards['length'] = RewardModel.compute_length_reward(generated)
        
        # Medical accuracy
        rewards['accuracy'] = RewardModel.compute_medical_accuracy_reward(
            generated, reference
        )
        
        # Reasoning quality
        rewards['reasoning'] = RewardModel.compute_reasoning_quality_reward(generated)
        
        # Compute weighted average
        total_weight = sum(weights.values())
        combined = sum(
            rewards.get(k, 0.5) * v 
            for k, v in weights.items()
        ) / total_weight
        
        return combined
