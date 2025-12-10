import yaml
import os

class ConfigManager:

    def __init__(self, config_path : str = None):

        if config_path is None:
            current_dir = os.getcwd()
            config_path = os.path.join(current_dir, "..", "config.yaml")
            if not os.path.exists(config_path):
                config_path = "/Workspace/nasa-lakehouse-databricks/config.yaml"
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"Config file not found at {config_path}")

        self.config_path = config_path
        
        self.config = self._load_config()
    
    def _load_config(self):
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get(self, *keys):
        """Acessa valores aninhados"""
        value = self.config
        for key in keys:
            value = value.get(key, {})
        return value