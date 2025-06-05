import yaml
from pathlib import Path

class Settings:
    def __init__(self):
        config_path = Path(__file__).parent / "config.yaml"
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)

    def get(self, key_path, default=None):
        """Получить значение по пути типа 'imap.host'"""
        keys = key_path.split('.')
        value = self.config
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key, {})
            else:
                return default
        return value if value != {} else default

settings = Settings()
