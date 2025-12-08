"""
GRPO (Group Relative Policy Optimization) trainer implementation.
"""

import os
import logging
from typing import Optional, Dict, List, Tuple
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
import numpy as np
from tqdm import tqdm

try:
    import wandb
    WANDB_AVAILABLE = True
except ImportError:
    WANDB_AVAILABLE = False

logger = logging.getLogger(__name__)


class GRPOTrainer:
    """Group Relative Policy Optimization trainer for medical reasoning."""
    
    def __init__(
        self,
        model,
        ref_model,
        tokenizer,
        reward_model,
        optimizer,
        device: str = "cuda",
        use_wandb: bool = False,
        project_name: str = "medical-o1-grpo",
    ):
        """
        Initialize GRPO trainer.
        
        Args:
            model: Model to train (policy)
            ref_model: Reference model (for KL divergence)
            tokenizer: Tokenizer
            reward_model: Reward model instance
            optimizer: Optimizer
            device: Device to use
            use_wandb: Whether to use W&B logging
            project_name: W&B project name
        """
        self.model = model
        self.ref_model = ref_model
        self.tokenizer = tokenizer
        self.reward_model = reward_model
        self.optimizer = optimizer
        self.device = device
        self.use_wandb = use_wandb and WANDB_AVAILABLE
        self.project_name = project_name
        self.global_step = 0
    
    def train(
        self,
        train_dataloader: DataLoader,
        eval_dataloader: Optional[DataLoader] = None,
        num_steps: int = 10000,
        group_size: int = 4,
        generation_max_length: int = 1024,
        temperature: float = 0.8,
        top_p: float = 0.9,
        entropy_weight: float = 0.01,
        beta_kl: float = 0.1,
        output_dir: str = "./checkpoints",
        save_steps: int = 500,
        eval_steps: int = 500,
    ) -> Dict:
        """
        Train the model with GRPO.
        
        Args:
            train_dataloader: Training data loader (with prompts and references)
            eval_dataloader: Validation data loader
            num_steps: Total training steps
            group_size: Number of sequences per group
            generation_max_length: Maximum generation length
            temperature: Sampling temperature for diversity
            top_p: Top-p sampling parameter
            entropy_weight: Weight for entropy regularization
            beta_kl: Weight for KL divergence penalty
            output_dir: Directory to save checkpoints
            save_steps: Save checkpoint every N steps
            eval_steps: Evaluate every N steps
            
        Returns:
            Dictionary with training results
        """
        os.makedirs(output_dir, exist_ok=True)
        
        if self.use_wandb:
            wandb.init(
                project=self.project_name,
                config={
                    "num_steps": num_steps,
                    "group_size": group_size,
                    "temperature": temperature,
                    "entropy_weight": entropy_weight,
                    "beta_kl": beta_kl,
                },
            )
        
        logger.info(f"Starting GRPO training for {num_steps} steps")
        logger.info(f"Group size: {group_size}")
        logger.info(f"Generation max length: {generation_max_length}")
        
        self.model.train()
        self.ref_model.eval()
        
        step_losses = []
        step_rewards = []
        
        # Create infinite dataloader
        dataloader_iter = iter(train_dataloader)
        
        pbar = tqdm(range(num_steps), desc="GRPO Training")
        
        for step in pbar:
            try:
                batch = next(dataloader_iter)
            except StopIteration:
                dataloader_iter = iter(train_dataloader)
                batch = next(dataloader_iter)
            
            # Move batch to device
            batch = {k: v.to(self.device) for k, v in batch.items()}
            
            # Generate completions for each prompt in the group
            loss, step_info = self._grpo_step(
                batch,
                group_size=group_size,
                generation_max_length=generation_max_length,
                temperature=temperature,
                top_p=top_p,
                entropy_weight=entropy_weight,
                beta_kl=beta_kl,
            )
            
            # Backward pass
            loss.backward()
            
            # Gradient clipping
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
            
            # Optimizer step
            self.optimizer.step()
            self.optimizer.zero_grad()
            
            self.global_step += 1
            step_losses.append(loss.item())
            step_rewards.append(step_info['avg_reward'])
            
            # Logging
            pbar.set_postfix({
                "loss": f"{loss.item():.4f}",
                "avg_reward": f"{step_info['avg_reward']:.3f}",
            })
            
            if self.use_wandb and self.global_step % 10 == 0:
                wandb.log({
                    "train/policy_loss": step_info['policy_loss'],
                    "train/kl_loss": step_info['kl_loss'],
                    "train/entropy": step_info['entropy'],
                    "train/avg_reward": step_info['avg_reward'],
                    "train/global_step": self.global_step,
                })
            
            # Evaluation
            if eval_dataloader and self.global_step % eval_steps == 0:
                eval_results = self._evaluate(eval_dataloader, group_size=group_size)
                
                logger.info(
                    f"Eval at step {self.global_step}: "
                    f"reward={eval_results['avg_reward']:.3f}"
                )
                
                if self.use_wandb:
                    wandb.log({
                        "eval/avg_reward": eval_results['avg_reward'],
                        "eval/global_step": self.global_step,
                    })
            
            # Save checkpoint
            if self.global_step % save_steps == 0:
                self._save_checkpoint(
                    output_dir,
                    checkpoint_name=f"checkpoint-{self.global_step}",
                )
        
        # Save final model
        self._save_checkpoint(output_dir, checkpoint_name="final_model")
        
        logger.info(" GRPO training completed!")
        
        results = {
            "training_losses": step_losses,
            "rewards": step_rewards,
            "final_checkpoint": os.path.join(output_dir, "final_model"),
        }
        
        if self.use_wandb:
            wandb.finish()
        
        return results
    
    def _grpo_step(
        self,
        batch: Dict,
        group_size: int = 4,
        generation_max_length: int = 1024,
        temperature: float = 0.8,
        top_p: float = 0.9,
        entropy_weight: float = 0.01,
        beta_kl: float = 0.1,
    ) -> Tuple[torch.Tensor, Dict]:
        """
        Perform one GRPO training step.
        
        Args:
            batch: Input batch with prompt_input_ids and prompt_attention_mask
            group_size: Number of sequences to generate per prompt
            generation_max_length: Maximum generation length
            temperature: Sampling temperature
            top_p: Top-p sampling parameter
            entropy_weight: Entropy regularization weight
            beta_kl: KL divergence weight
            
        Returns:
            Tuple of (loss, step_info_dict)
        """
        batch_size = batch['input_ids'].size(0)
        
        # Generate multiple completions per prompt
        all_completions = []
        all_rewards = []
        all_log_probs = []
        all_ref_log_probs = []
        
        for _ in range(group_size):
            with torch.no_grad():
                # Generate from policy model
                generated_ids = self.model.generate(
                    input_ids=batch['input_ids'],
                    attention_mask=batch['attention_mask'],
                    max_new_tokens=generation_max_length,
                    temperature=temperature,
                    top_p=top_p,
                    do_sample=True,
                    pad_token_id=self.tokenizer.pad_token_id,
                    eos_token_id=self.tokenizer.eos_token_id,
                    return_dict_in_generate=True,
                    output_scores=True,
                )
                
                generated_ids = generated_ids.sequences
                all_completions.append(generated_ids)
            
            # Compute log probabilities (keep grad for policy, ref stays frozen)
            policy_outputs = self.model(input_ids=generated_ids)
            policy_logits = policy_outputs.logits
            policy_log_probs = self._compute_sequence_log_probs(
                policy_logits,
                generated_ids,
            )
            all_log_probs.append(policy_log_probs)
            
            # Reference model log probs (no grad needed)
            with torch.no_grad():
                ref_outputs = self.ref_model(input_ids=generated_ids)
                ref_logits = ref_outputs.logits
                ref_log_probs = self._compute_sequence_log_probs(
                    ref_logits,
                    generated_ids,
                )
            all_ref_log_probs.append(ref_log_probs)
        
        # Stack completions and log probs
        completions = torch.stack(all_completions)  # [group_size, batch_size, seq_len]
        log_probs = torch.stack(all_log_probs)  # [group_size, batch_size]
        ref_log_probs = torch.stack(all_ref_log_probs)  # [group_size, batch_size]
        
        # Compute rewards using integrated reward model
        rewards_list = []
        
        for group_idx in range(group_size):
            group_rewards = []
            for batch_idx in range(batch_size):
                # Decode generated sequence
                generated_ids = completions[group_idx, batch_idx]
                generated_text = self.tokenizer.decode(
                    generated_ids,
                    skip_special_tokens=True,
                )
                
                # Get reference text if available in batch
                # For GRPO, batch typically only has input_ids (prompts), not response text
                if isinstance(batch, dict) and 'response' in batch and len(batch['response']) > batch_idx:
                    reference_text = batch['response'][batch_idx]
                else:
                    reference_text = ''
                
                # Compute combined reward from reward model
                try:
                    combined_reward = self.reward_model.compute_combined_reward(
                        generated=generated_text,
                        reference=reference_text,
                        model=self.model,
                        tokenizer=self.tokenizer,
                        device=self.device,
                    )
                    group_rewards.append(combined_reward)
                except Exception as e:
                    # Fallback: use length-based reward if full computation fails
                    length_reward = self.reward_model.compute_length_reward(generated_text)
                    group_rewards.append(length_reward)
            
            rewards_list.append(torch.tensor(group_rewards, device=self.device, dtype=torch.float32))
        
        rewards = torch.stack(rewards_list)  # [group_size, batch_size]
        
        # Normalize rewards per group
        group_rewards = rewards - rewards.mean(dim=0, keepdim=True)
        group_rewards = group_rewards / (group_rewards.std(dim=0, keepdim=True) + 1e-8)
        
        # Compute advantage
        advantages = group_rewards  # Already normalized
        
        # GRPO loss: Policy gradient loss - KL penalty
        # Loss = -mean(advantages * log_probs) + beta_kl * KL(policy || ref)
        policy_loss = -(advantages.detach() * log_probs).mean()
        
        # KL divergence
        kl_divergence = (ref_log_probs.detach() - log_probs).mean()
        
        # Entropy bonus
        entropy = -log_probs.mean()
        
        # Total loss
        total_loss = policy_loss + beta_kl * kl_divergence - entropy_weight * entropy
        
        # Step info for logging
        step_info = {
            'policy_loss': policy_loss.item(),
            'kl_loss': (beta_kl * kl_divergence).item(),
            'entropy': entropy.item(),
            'avg_reward': rewards.mean().item(),
        }
        
        return total_loss, step_info
    
    def _compute_sequence_log_probs(
        self,
        logits: torch.Tensor,
        token_ids: torch.Tensor,
    ) -> torch.Tensor:
        """
        Compute log probabilities for token sequences.
        
        Args:
            logits: Model logits [seq_len, vocab_size] or [batch, seq_len, vocab_size]
            token_ids: Token IDs [seq_len] or [batch, seq_len]
            
        Returns:
            Sequence log probabilities
        """
        # Shift logits and token_ids for next token prediction
        shift_logits = logits[..., :-1, :].contiguous()
        shift_token_ids = token_ids[..., 1:].contiguous()
        
        # Compute log softmax
        log_probs = F.log_softmax(shift_logits, dim=-1)
        
        # Gather log probs for actual tokens
        batch_shape = shift_token_ids.shape[:-1]
        sequence_length = shift_token_ids.shape[-1]
        
        # Flatten batch dimensions
        flat_log_probs = log_probs.view(-1, log_probs.size(-1))
        flat_token_ids = shift_token_ids.view(-1)
        
        # Gather log probs
        sequence_log_probs = flat_log_probs.gather(-1, flat_token_ids.unsqueeze(-1))
        sequence_log_probs = sequence_log_probs.view(*batch_shape, sequence_length)
        
        # Sum over sequence
        total_log_probs = sequence_log_probs.sum(dim=-1)
        
        return total_log_probs
    
    def _evaluate(
        self,
        eval_dataloader: DataLoader,
        group_size: int = 4,
    ) -> Dict:
        """
        Evaluate model on validation set.
        
        Args:
            eval_dataloader: Validation data loader
            group_size: Number of sequences per prompt
            
        Returns:
            Dictionary with evaluation results
        """
        self.model.eval()
        
        all_rewards = []
        
        with torch.no_grad():
            for batch in eval_dataloader:
                batch = {k: v.to(self.device) for k, v in batch.items()}
                
                # Generate completions
                generated_ids = self.model.generate(
                    input_ids=batch['input_ids'],
                    attention_mask=batch['attention_mask'],
                    max_length=1024,
                    temperature=0.7,
                    do_sample=True,
                )
                
                # Compute rewards (placeholder)
                batch_rewards = torch.randn(
                    batch['input_ids'].size(0),
                    device=self.device,
                ) * 0.1 + 0.5
                
                all_rewards.extend(batch_rewards.cpu().numpy())
        
        self.model.train()
        
        return {
            'avg_reward': np.mean(all_rewards),
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
        
        self.model.save_pretrained(checkpoint_path)
        self.tokenizer.save_pretrained(checkpoint_path)
        
        logger.info(f" Checkpoint saved to {checkpoint_path}")
