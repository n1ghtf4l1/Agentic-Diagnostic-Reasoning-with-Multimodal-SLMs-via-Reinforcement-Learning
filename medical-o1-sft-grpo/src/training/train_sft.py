"""
Main SFT training script.
"""

import argparse
import os
import logging
import yaml
from typing import Dict, Any

import torch
from torch.utils.data import DataLoader
from transformers import get_linear_schedule_with_warmup

from src.data.dataset import MedicalDataset, DataCollatorForSFT
from src.models.medical_model import MedicalModel
from src.training.sft_trainer import SFTTrainer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Train a medical reasoning model with SFT"
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to YAML config file for overriding defaults",
    )
    
    # Model arguments
    parser.add_argument(
        "--model_name_or_path",
        type=str,
        default="meta-llama/Llama-2-7b",
        help="Model name or path",
    )
    parser.add_argument(
        "--use_lora",
        action="store_true",
        help="Use LoRA for parameter-efficient training",
    )
    parser.add_argument(
        "--lora_rank",
        type=int,
        default=16,
        help="LoRA rank",
    )
    parser.add_argument(
        "--lora_alpha",
        type=int,
        default=32,
        help="LoRA alpha",
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
        "--dataset_name",
        type=str,
        default="FreedomIntelligence/medical-o1-reasoning-SFT",
        help="HuggingFace dataset name when using hub loading",
    )
    parser.add_argument(
        "--dataset_config",
        type=str,
        default="en",
        help="Dataset config (en, zh, en_mix, zh_mix) when using hub loading",
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
        default="./checkpoints/sft",
        help="Output directory for checkpoints",
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=32,
        help="Training batch size",
    )
    parser.add_argument(
        "--train_val_split",
        type=float,
        default=0.9,
        help="Fraction of data to use for training (rest for eval)",
    )
    parser.add_argument(
        "--num_epochs",
        type=int,
        default=3,
        help="Number of training epochs",
    )
    parser.add_argument(
        "--learning_rate",
        type=float,
        default=2e-5,
        help="Learning rate",
    )
    parser.add_argument(
        "--warmup_ratio",
        type=float,
        default=0.1,
        help="Warmup ratio",
    )
    parser.add_argument(
        "--gradient_accumulation_steps",
        type=int,
        default=4,
        help="Gradient accumulation steps",
    )
    parser.add_argument(
        "--max_grad_norm",
        type=float,
        default=1.0,
        help="Maximum gradient norm",
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
    
    args, _ = parser.parse_known_args()

    if args.config:
        with open(args.config, "r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}
        if not isinstance(config_data, dict):
            raise ValueError("YAML config must be a mapping at the top level")

        nesting = {
            ("model", "name"): "model_name_or_path",
            ("model", "use_lora"): "use_lora",
            ("model", "lora_rank"): "lora_rank",
            ("model", "lora_alpha"): "lora_alpha",
            ("data", "dataset_name"): "dataset_name",
            ("data", "dataset_config"): "dataset_config",
            ("data", "max_seq_length"): "max_seq_length",
            ("data", "max_samples"): "max_samples",
            ("data", "train_val_split"): "train_val_split",
            ("training", "output_dir"): "output_dir",
            ("training", "num_epochs"): "num_epochs",
            ("training", "batch_size"): "batch_size",
            ("training", "learning_rate"): "learning_rate",
            ("training", "warmup_ratio"): "warmup_ratio",
            ("training", "gradient_accumulation_steps"): "gradient_accumulation_steps",
            ("training", "max_grad_norm"): "max_grad_norm",
            ("training", "seed"): "seed",
            ("checkpoint", "save_steps"): "save_steps",
            ("checkpoint", "eval_steps"): "eval_steps",
            ("checkpoint", "use_wandb"): "use_wandb",
            ("device",): "device",
        }

        defaults: Dict[str, Any] = {}
        for key_path, dest in nesting.items():
            cursor: Any = config_data
            try:
                for key in key_path:
                    cursor = cursor[key]
                defaults[dest] = cursor
            except (KeyError, TypeError):
                continue

        # Allow top-level keys that directly match destinations
        valid_dests = {action.dest for action in parser._actions}
        for key, value in config_data.items():
            if key in valid_dests and key not in defaults:
                defaults[key] = value

        parser.set_defaults(**defaults)

    return parser.parse_args()


def main():
    args = parse_args()
    
    # Set seed
    torch.manual_seed(args.seed)
    
    # Load dataset
    logger.info("Loading dataset...")
    if args.use_huggingface_dataset:
        dataset = MedicalDataset.from_huggingface(
            dataset_name=args.dataset_name,
            config=args.dataset_config,
            max_samples=args.max_samples,
        )
    else:
        # Assume JSON file
        json_path = os.path.join(args.data_path, "medical_o1_sft.json")
        if not os.path.exists(json_path):
            raise FileNotFoundError(f"Dataset not found at {json_path}")
        dataset = MedicalDataset.from_json(
            json_path,
            max_samples=args.max_samples,
        )
    
    # Split into train/eval
    train_size = int(args.train_val_split * len(dataset))
    train_dataset = dataset.select(range(train_size))
    eval_dataset = dataset.select(range(train_size, len(dataset)))
    
    # Load model and tokenizer
    logger.info("Loading model and tokenizer...")
    model, tokenizer = MedicalModel.load_model_and_tokenizer(
        args.model_name_or_path,
        device=args.device,
        use_lora=args.use_lora,
        lora_rank=args.lora_rank,
        lora_alpha=args.lora_alpha,
    )
    
    # Preprocess datasets
    logger.info("Preprocessing datasets...")
    train_dataset = MedicalDataset.preprocess_for_sft(
        train_dataset,
        tokenizer,
        max_length=args.max_seq_length,
    )
    eval_dataset = MedicalDataset.preprocess_for_sft(
        eval_dataset,
        tokenizer,
        max_length=args.max_seq_length,
    )
    
    # Create data loaders
    data_collator = DataCollatorForSFT(tokenizer)
    
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
        model.parameters(),
        lr=args.learning_rate,
    )
    
    num_training_steps = (
        len(train_dataloader) * args.num_epochs
        // args.gradient_accumulation_steps
    )
    num_warmup_steps = int(args.warmup_ratio * num_training_steps)
    
    lr_scheduler = get_linear_schedule_with_warmup(
        optimizer,
        num_warmup_steps=num_warmup_steps,
        num_training_steps=num_training_steps,
    )
    
    # Create trainer
    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        optimizer=optimizer,
        lr_scheduler=lr_scheduler,
        device=args.device,
        use_wandb=args.use_wandb,
    )
    
    # Train
    results = trainer.train(
        train_dataloader=train_dataloader,
        eval_dataloader=eval_dataloader,
        num_epochs=args.num_epochs,
        gradient_accumulation_steps=args.gradient_accumulation_steps,
        max_grad_norm=args.max_grad_norm,
        output_dir=args.output_dir,
        save_steps=args.save_steps,
        eval_steps=args.eval_steps,
        warmup_steps=num_warmup_steps,
    )
    
    logger.info(f" Training completed!")
    logger.info(f"Checkpoints saved to {args.output_dir}")


if __name__ == "__main__":
    main()
