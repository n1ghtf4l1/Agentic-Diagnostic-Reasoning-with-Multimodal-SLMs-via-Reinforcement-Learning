"""
Configuration utilities.
"""

import yaml
from typing import Dict, Any


class ConfigManager:
    """Manage configuration files."""
    
    @staticmethod
    def load_config(config_path: str) -> Dict[str, Any]:
        """
        Load configuration from YAML file.
        
        Args:
            config_path: Path to configuration file
            
        Returns:
            Configuration dictionary
        """
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        return config
    
    @staticmethod
    def save_config(config: Dict[str, Any], output_path: str):
        """
        Save configuration to YAML file.
        
        Args:
            config: Configuration dictionary
            output_path: Path to save configuration
        """
        with open(output_path, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
