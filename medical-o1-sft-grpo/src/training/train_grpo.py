"""
Main GRPO training script.
"""

import argparse
import os
import logging
from typing import Optional

import torch
from torch.utils.data import DataLoader
from transformers import get_linear_schedule_with_warmup

from src.data.dataset import MedicalDataset, DataCollatorForGRPO
from src.models.medical_model import MedicalModel, RewardModel
from src.training.grpo_trainer import GRPOTrainer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train a medical reasoning model with GRPO"
    )
    
    # Model arguments
    parser.add_argument(
        "--sft_model_path",
        type=str,
        required=True,
        help="Path to SFT model checkpoint",
    )
    parser.add_argument(
        "--ref_model_path",
        type=str,
        default=None,
        help="Path to reference model (default: same as sft_model_path)",
    )
    
    # Data arguments
    parser.add_argument(
        "--data_path",
        type=str,
        default="./data",
        help="Path to training data",
    )
    parser.add_argument(
        "--use_huggingface_dataset",
        action="store_true",
        help="Load dataset from HuggingFace Hub",
    )
    parser.add_argument(
        "--max_seq_length",
        type=int,
        default=2048,
        help="Maximum sequence length",
    )
    parser.add_argument(
        "--max_samples",
        type=int,
        default=None,
        help="Maximum number of samples to use",
    )
    
    # Training arguments
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./checkpoints/grpo",
        help="Output directory for checkpoints",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=16,
        help="Training batch size",
    )
    parser.add_argument(
        "--num_steps",
        type=int,
        default=10000,
        help="Total training steps",
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        default=1e-5,
        help="Learning rate",
    )
    parser.add_argument(
        "--group_size",
        type=int,
        default=4,
        help="Number of sequences per group",
    )
    parser.add_argument(
        "--generation_max_length",
        type=int,
        default=1024,
        help="Maximum generation length",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.8,
        help="Sampling temperature",
    )
    parser.add_argument(
        "--entropy_weight",
        type=float,
        default=0.01,
        help="Entropy regularization weight",
    )
    parser.add_argument(
        "--beta_kl",
        type=float,
        default=0.1,
        help="KL divergence penalty weight",
    )
    parser.add_argument(
        "--save_steps",
        type=int,
        default=500,
        help="Save checkpoint every N steps",
    )
    parser.add_argument(
        "--eval_steps",
        type=int,
        default=500,
        help="Evaluate every N steps",
    )
    
    # Other arguments
    parser.add_argument(
        "--use_wandb",
        action="store_true",
        help="Use Weights & Biases for logging",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed",
    )
    parser.add_argument(
        "--device",
        type=str,
        default="cuda",
        help="Device to use (cuda or cpu)",
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Set seed
    torch.manual_seed(args.seed)
    
    # Set reference model path
    if args.ref_model_path is None:
        args.ref_model_path = args.sft_model_path
    
    # Load dataset
    logger.info("Loading dataset...")
    if args.use_huggingface_dataset:
        dataset = MedicalDataset.from_huggingface(
            max_samples=args.max_samples,
        )
    else:
        json_path = os.path.join(args.data_path, "medical_o1_sft.json")
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Dataset not found at {json_path}")
        dataset = MedicalDataset.from_json(
            json_path,
            max_samples=args.max_samples,
        )
    
    # Split into train/eval
    eval_size = int(0.1 * len(dataset))
    train_dataset = dataset.select(range(eval_size, len(dataset)))
    eval_dataset = dataset.select(range(eval_size))
    
    # Load policy model and tokenizer
    logger.info("Loading policy model...")
    policy_model, tokenizer = MedicalModel.load_model_and_tokenizer(
        args.sft_model_path,
        device=args.device,
        use_lora=False,
    )
    
    # Load reference model
    logger.info("Loading reference model...")
    ref_model, _ = MedicalModel.load_model_and_tokenizer(
        args.ref_model_path,
        device=args.device,
        use_lora=False,
    )
    ref_model.eval()
    
    # Preprocess datasets (GRPO uses prompts only)
    logger.info("Preprocessing datasets...")
    train_dataset = MedicalDataset.preprocess_for_grpo(
        train_dataset,
        tokenizer,
        max_length=args.max_seq_length,
    )
    eval_dataset = MedicalDataset.preprocess_for_grpo(
        eval_dataset,
        tokenizer,
        max_length=args.max_seq_length,
    )
    
    # Create data loaders
    data_collator = DataCollatorForGRPO(tokenizer)
    
    train_dataloader = DataLoader(
        train_dataset,
        batch_size=args.batch_size,
        collate_fn=data_collator,
        shuffle=True,
    )
    
    eval_dataloader = DataLoader(
        eval_dataset,
        batch_size=args.batch_size,
        collate_fn=data_collator,
    )
    
    # Setup optimizer and scheduler
    optimizer = torch.optim.AdamW(
        policy_model.parameters(),
        lr=args.learning_rate,
    )
    
    num_warmup_steps = int(0.1 * args.num_steps)
    
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=num_warmup_steps,
        num_training_steps=args.num_steps,
    )
    
    # Initialize reward model
    reward_model = RewardModel()
    
    # Create trainer
    trainer = GRPOTrainer(
        model=policy_model,
        ref_model=ref_model,
        tokenizer=tokenizer,
        reward_model=reward_model,
        optimizer=optimizer,
        device=args.device,
        use_wandb=args.use_wandb,
    )
    
    # Train
    results = trainer.train(
        train_dataloader=train_dataloader,
        eval_dataloader=eval_dataloader,
        num_steps=args.num_steps,
        group_size=args.group_size,
        generation_max_length=args.generation_max_length,
        temperature=args.temperature,
        entropy_weight=args.entropy_weight,
        beta_kl=args.beta_kl,
        output_dir=args.output_dir,
        save_steps=args.save_steps,
        eval_steps=args.eval_steps,
    )
    
    logger.info(f" Training completed!")
    logger.info(f"Checkpoints saved to {args.output_dir}")


if __name__ == "__main__":
    main()
