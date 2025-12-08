"""
Download dataset from HuggingFace Hub.
"""

import argparse
import os
from src.data.dataset import MedicalDataset


def main():
    parser = argparse.ArgumentParser(
        description="Download medical-o1-reasoning-SFT dataset"
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default="./data",
        help="Output directory for the dataset",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="en",
        choices=["en", "zh", "en_mix", "zh_mix"],
        help="Dataset config (language variant)",
    )
    parser.add_argument(
        "--split",
        type=str,
        default="train",
        choices=["train", "validation", "test"],
        help="Dataset split to download",
    )
    parser.add_argument(
        "--max_samples",
        type=int,
        default=None,
        help="Maximum number of samples to download (for testing)",
    )
    
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output_dir, exist_ok=True)
    
    # Download dataset
    dataset = MedicalDataset.from_huggingface(
        config=args.config,
        split=args.split,
        cache_dir=args.output_dir,
        max_samples=args.max_samples,
    )
    
    print(f" Successfully downloaded {len(dataset)} samples to {args.output_dir}")
    print(f"  Dataset config: {args.config}")
    print(f"  Dataset split: {args.split}")
    print(f"  Dataset columns: {dataset.column_names}")


if __name__ == "__main__":
    main()
