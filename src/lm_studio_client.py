import requests
import json
import re
from .utils import retry_with_backoff

class LMStudioClient:
    def __init__(self, api_url, model_name, logger, timeout=90, max_tokens=512, temperature=0.0, retry_attempts=2):
        self.api_url = api_url
        self.model_name = model_name
        self.logger = logger
        self.timeout = timeout
        self.max_tokens = max_tokens
        self.temperature = temperature
        self.retry_attempts = retry_attempts

    @retry_with_backoff()
    def analyze_email(self, email_body, target_fields, retries=None):
        """
        Анализ письма через LM Studio.
        Возвращает: Dict c извлечёнными данными или None при ошибке.
        """
        prompt = self._create_prompt(email_body, target_fields)
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "max_tokens": self.max_tokens,
            "temperature": self.temperature,
            "stop": None,
            "stream": False
        }

        try:
            response = requests.post(
                self.api_url,
                json=payload,
                timeout=self.timeout
            )
            response.raise_for_status()
            result = response.json()
            text = result.get("choices", [{}])[0].get("text", "")
            parsed = self._parse_response(text, target_fields)
            if parsed is None:
                self.logger.warning("LM Studio: Не удалось извлечь JSON из ответа.")
            return parsed
        except Exception as e:
            self.logger.error(f"LM Studio API error: {e}")
            return None

    def _create_prompt(self, email_body, target_fields):
        """
        Создание промпта для LM Studio.
        На русском языке, строго требуем только JSON, без пояснений.
        """
        fields_list = "\n".join(f'- {field}' for field in target_fields)
        prompt = (
            f"Проанализируй текст письма и извлеки следующую информацию в формате JSON.\n"
            f"Если информация отсутствует, оставь поле пустым (\"\").\n\n"
            f"Поля для извлечения:\n"
            f"{fields_list}\n\n"
            f"Верни только JSON без дополнительных комментариев.\n\n"
            f"Текст письма:\n"
            f"{email_body}\n"
        )
        return prompt

    def _parse_response(self, response_text, target_fields):
        """
        Парсинг ответа LM Studio:
        1. Найти JSON в тексте (даже если есть “шум”)
        2. Удалить комментарии //
        3. Валидировать структуру, вернуть словарь с нужными полями
        """
        if not response_text:
            return None
        # 1. Найти JSON-блок с помощью регулярки
        json_match = re.search(r'{.*}', response_text, flags=re.DOTALL)
        if not json_match:
            return None
        json_text = json_match.group(0)
        # 2. Удалить комментарии //...
        json_text = re.sub(r'//.*', '', json_text)
        # 3. Попробовать распарсить
        try:
            data = json.loads(json_text)
        except Exception:
            return None
        # 4. Оставить только нужные поля
        clean = {}
        for field in target_fields:
            clean[field] = data.get(field, "")
        return clean
