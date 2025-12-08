"""
SFT (Supervised Fine-Tuning) trainer implementation.
"""

import os
import logging
from typing import Optional, Dict
import torch
from torch.utils.data import DataLoader
from transformers import AutoTokenizer, get_linear_schedule_with_warmup
import numpy as np
from tqdm import tqdm

try:
    import wandb
    WANDB_AVAILABLE = True
except ImportError:
    WANDB_AVAILABLE = False

logger = logging.getLogger(__name__)


class SFTTrainer:
    """Supervised Fine-Tuning trainer for medical reasoning."""
    
    def __init__(
        self,
        model,
        tokenizer,
        optimizer,
        lr_scheduler,
        device: str = "cuda",
        use_wandb: bool = False,
        project_name: str = "medical-o1-sft",
    ):
        """
        Initialize SFT trainer.
        
        Args:
            model: Model to train
            tokenizer: Tokenizer
            optimizer: Optimizer
            lr_scheduler: Learning rate scheduler
            device: Device to use
            use_wandb: Whether to use W&B logging
            project_name: W&B project name
        """
        self.model = model
        self.tokenizer = tokenizer
        self.optimizer = optimizer
        self.lr_scheduler = lr_scheduler
        self.device = device
        self.use_wandb = use_wandb and WANDB_AVAILABLE
        self.project_name = project_name
        self.global_step = 0
    
    def train(
        self,
        train_dataloader: DataLoader,
        eval_dataloader: Optional[DataLoader] = None,
        num_epochs: int = 3,
        gradient_accumulation_steps: int = 4,
        max_grad_norm: float = 1.0,
        output_dir: str = "./checkpoints",
        save_steps: int = 500,
        eval_steps: int = 500,
        warmup_steps: int = 0,
    ) -> Dict:
        """
        Train the model.
        
        Args:
            train_dataloader: Training data loader
            eval_dataloader: Validation data loader
            num_epochs: Number of training epochs
            gradient_accumulation_steps: Gradient accumulation steps
            max_grad_norm: Maximum gradient norm for clipping
            output_dir: Directory to save checkpoints
            save_steps: Save checkpoint every N steps
            eval_steps: Evaluate every N steps
            warmup_steps: Number of warmup steps
            
        Returns:
            Dictionary with training results
        """
        # Setup
        os.makedirs(output_dir, exist_ok=True)
        
        # Initialize W&B if requested
        if self.use_wandb:
            wandb.init(
                project=self.project_name,
                config={
                    "learning_rate": self.optimizer.defaults["lr"],
                    "num_epochs": num_epochs,
                    "gradient_accumulation_steps": gradient_accumulation_steps,
                },
            )
        
        # Training loop
        total_steps = len(train_dataloader) * num_epochs // gradient_accumulation_steps
        
        if warmup_steps == 0:
            warmup_steps = int(0.1 * total_steps)
        
        logger.info(f"Starting SFT training for {num_epochs} epochs")
        logger.info(f"Total training steps: {total_steps}")
        logger.info(f"Warmup steps: {warmup_steps}")
        
        best_eval_loss = float('inf')
        training_losses = []
        training_perplexities = []
        eval_losses = []
        eval_perplexities = []
        step_history = []
        
        for epoch in range(num_epochs):
            logger.info(f"\n--- Epoch {epoch + 1}/{num_epochs} ---")
            
            epoch_loss = 0.0
            num_batches = 0
            
            pbar = tqdm(
                train_dataloader,
                desc=f"Training Epoch {epoch + 1}",
                total=len(train_dataloader),
            )
            
            for batch_idx, batch in enumerate(pbar):
                # Move batch to device
                batch = {k: v.to(self.device) for k, v in batch.items()}
                
                # Forward pass
                outputs = self.model(**batch)
                loss = outputs.loss
                
                # Normalize loss for gradient accumulation
                loss = loss / gradient_accumulation_steps
                
                # Backward pass
                loss.backward()
                
                # Accumulate loss
                epoch_loss += loss.item()
                num_batches += 1
                
                # Gradient accumulation
                if (batch_idx + 1) % gradient_accumulation_steps == 0:
                    # Gradient clipping
                    torch.nn.utils.clip_grad_norm_(
                        self.model.parameters(),
                        max_grad_norm,
                    )
                    
                    # Optimizer step
                    self.optimizer.step()
                    self.lr_scheduler.step()
                    self.optimizer.zero_grad()
                    
                    self.global_step += 1
                    
                    # Update progress bar
                    avg_loss = epoch_loss / num_batches
                    pbar.set_postfix({"loss": f"{avg_loss:.4f}"})
                    
                    # Logging
                    if self.use_wandb and self.global_step % 10 == 0:
                        wandb.log({
                            "train/loss": avg_loss,
                            "train/learning_rate": self.lr_scheduler.get_last_lr()[0],
                            "train/global_step": self.global_step,
                        })
                    
                    # Evaluation
                    if eval_dataloader and self.global_step % eval_steps == 0:
                        eval_metrics = self._evaluate(eval_dataloader)
                        eval_loss = eval_metrics['loss']
                        eval_perp = eval_metrics['perplexity']
                        
                        logger.info(f"Step {self.global_step}: eval_loss={eval_loss:.4f}, perplexity={eval_perp:.2f}")
                        
                        eval_losses.append(eval_loss)
                        eval_perplexities.append(eval_perp)
                        step_history.append(self.global_step)
                        
                        if self.use_wandb:
                            wandb.log({
                                "eval/loss": eval_loss,
                                "eval/perplexity": eval_perp,
                                "eval/global_step": self.global_step,
                            })
                        
                        # Save best model
                        if eval_loss < best_eval_loss:
                            best_eval_loss = eval_loss
                            self._save_checkpoint(
                                output_dir,
                                checkpoint_name="best_model",
                            )
                    
                    # Periodic checkpoint save
                    if self.global_step % save_steps == 0:
                        self._save_checkpoint(
                            output_dir,
                            checkpoint_name=f"checkpoint-{self.global_step}",
                        )
            
            avg_epoch_loss = epoch_loss / num_batches
            training_losses.append(avg_epoch_loss)
            training_perplexities.append(np.exp(avg_epoch_loss))
            logger.info(f"Epoch {epoch + 1} average loss: {avg_epoch_loss:.4f}, perplexity: {np.exp(avg_epoch_loss):.2f}")
        
        # Save final model
        self._save_checkpoint(output_dir, checkpoint_name="final_model")
        
        logger.info(" Training completed!")
        
        results = {
            "training_losses": training_losses,
            "training_perplexities": training_perplexities,
            "eval_losses": eval_losses,
            "eval_perplexities": eval_perplexities,
            "step_history": step_history,
            "best_eval_loss": best_eval_loss,
            "final_checkpoint": os.path.join(output_dir, "final_model"),
        }
        
        if self.use_wandb:
            wandb.finish()
        
        return results
    
    def _evaluate(self, eval_dataloader: DataLoader) -> Dict:
        """
        Evaluate model on validation set.
        
        Args:
            eval_dataloader: Validation data loader
            
        Returns:
            Dictionary with evaluation metrics (loss, perplexity, token_accuracy)
        """
        self.model.eval()
        total_loss = 0.0
        total_correct = 0
        total_tokens = 0
        num_batches = 0
        
        with torch.no_grad():
            for batch in eval_dataloader:
                batch = {k: v.to(self.device) for k, v in batch.items()}
                
                outputs = self.model(**batch)
                loss = outputs.loss
                logits = outputs.logits
                
                total_loss += loss.item()
                num_batches += 1
                
                # Calculate token accuracy (top-1 exact match)
                predictions = logits.argmax(dim=-1)
                labels = batch['labels']
                
                # Mask padding/ignore tokens
                mask = labels != -100
                
                correct = (predictions[mask] == labels[mask]).sum().item()
                total_correct += correct
                total_tokens += mask.sum().item()
        
        self.model.train()
        
        avg_loss = total_loss / num_batches
        token_accuracy = total_correct / total_tokens if total_tokens > 0 else 0.0
        perplexity = np.exp(avg_loss) if avg_loss < 100 else float('inf')
        
        return {
            'loss': avg_loss,
            'token_accuracy': token_accuracy,
            'perplexity': perplexity,
        }
    
    def _save_checkpoint(self, output_dir: str, checkpoint_name: str):
        """
        Save model checkpoint.
        
        Args:
            output_dir: Output directory
            checkpoint_name: Name for checkpoint
        """
        checkpoint_path = os.path.join(output_dir, checkpoint_name)
        os.makedirs(checkpoint_path, exist_ok=True)
        
        # Save model and tokenizer
        self.model.save_pretrained(checkpoint_path)
        self.tokenizer.save_pretrained(checkpoint_path)
        
        logger.info(f" Checkpoint saved to {checkpoint_path}")
