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
            
            # Логируем сырой ответ для отладки
            self.logger.debug(f"LM Studio raw response: {repr(text)}")
            
            parsed = self._parse_response(text, target_fields)
            if parsed is None:
                self.logger.warning(f"LM Studio: Не удалось извлечь JSON из ответа: {repr(text[:200])}")
            return parsed
        except Exception as e:
            self.logger.error(f"LM Studio API error: {e}")
            return None

    def _create_prompt(self, email_body, target_fields):
        """
        Создание промпта для LM Studio.
        Улучшенный промпт с четкими инструкциями.
        """
        fields_example = {field: "" for field in target_fields}
        example_json = json.dumps(fields_example, ensure_ascii=False, indent=2)

        prompt = (
            f"Извлеки информацию из письма и верни ТОЛЬКО JSON в указанном формате.\n"
            f"Не добавляй пояснения, комментарии или другой текст.\n"
            f"Не добавляй длинных ссылок, не включай вложения и списки. "
            f"Включай их только если автор письма явно просит об этом.\n\n"
            f"Требуемый формат JSON:\n"
            f"{example_json}\n\n"
            f"Правила:\n"
            f"- Если информация отсутствует, используй пустую строку \"\"\n"
            f"- Не добавляй поля, которых нет в примере\n"
            f"- Верни только валидный JSON\n\n"
            f"Текст письма:\n"
            f"{email_body}\n\n"
            f"JSON:"
        )
        return prompt

    def _parse_response(self, response_text, target_fields):
        """
        Улучшенный парсинг ответа LM Studio с множественными стратегиями.
        """
        if not response_text or not response_text.strip():
            self.logger.debug("Пустой ответ от LM Studio")
            return None

        # Стратегия 1: Поиск JSON между фигурными скобками (самый надежный)
        json_objects = self._extract_json_objects(response_text)
        
        for json_obj in json_objects:
            parsed = self._try_parse_json(json_obj, target_fields)
            if parsed is not None:
                return parsed

        # Стратегия 2: Попробовать весь текст как JSON (после очистки)
        cleaned_text = self._clean_response_text(response_text)
        parsed = self._try_parse_json(cleaned_text, target_fields)
        if parsed is not None:
            return parsed

        # Стратегия 3: Поиск JSON после ключевых слов
        json_after_keywords = self._extract_json_after_keywords(response_text)
        for json_text in json_after_keywords:
            parsed = self._try_parse_json(json_text, target_fields)
            if parsed is not None:
                return parsed

        self.logger.debug(f"Все стратегии парсинга не сработали для: {repr(response_text[:200])}")
        return None

    def _extract_json_objects(self, text):
        """
        Извлекает все JSON-объекты из текста, учитывая вложенность и обрезку в конце.
        Если находит явно обрезанный JSON — пытается его аккуратно «закрыть» и валидировать.
        """
        json_objects = []

        # Поиск всех валидных JSON-объектов по вложенным скобкам
        brace_count = 0
        start_pos = None
        for i, char in enumerate(text):
            if char == '{':
                if brace_count == 0:
                    start_pos = i
                brace_count += 1
            elif char == '}':
                brace_count -= 1
                if brace_count == 0 and start_pos is not None:
                    json_candidate = text[start_pos:i+1]
                    json_objects.append(json_candidate)
                    start_pos = None

        # Если валидных JSON не найдено, ищем последний незакрытый объект и пробуем его починить
        if not json_objects:
            last_open = text.rfind('{')
            if last_open != -1:
                candidate = text[last_open:].strip()
                fixed_candidate = self._try_fix_truncated_json(candidate)
                if fixed_candidate:
                    self.logger.warning("LM Studio: Auto-fix applied to truncated JSON.")
                    json_objects.append(fixed_candidate)
        return json_objects
        
        # Дополнительно: простой regex для случаев, где алгоритм выше не сработал
        regex_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text, re.DOTALL)
        json_objects.extend(regex_matches)
        
        return json_objects
    def _try_fix_truncated_json(self, candidate):
        """
        Пробует аккуратно «закрыть» обрезанный JSON-объект, добавляя кавычки/скобки.
        Не гарантирует исправление, просто пытается минимально восстановить структуру.
        """
        candidate = candidate.rstrip()
        # Если последняя строка внутри объекта оборвана — пытаемся закрыть кавычки
        if candidate.count('"') % 2 != 0:
            candidate += '"'
        # Если не хватает закрывающей скобки
        if candidate.count('{') > candidate.count('}'):
            candidate += '}'
        # Пробуем загрузить как JSON
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            return None

    def _extract_json_after_keywords(self, text):
        """
        Ищет JSON после ключевых слов типа "JSON:", "Результат:", и т.д.
        """
        keywords = [
            r'JSON\s*:',
            r'Результат\s*:',
            r'Ответ\s*:',
            r'Извлеченные данные\s*:',
            r'\{',  # Просто первая открывающая скобка
        ]
        
        json_candidates = []
        
        for keyword in keywords:
            pattern = rf'{keyword}\s*(\{{.*?\}})'
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            json_candidates.extend(matches)
            
            # Также ищем все от ключевого слова до конца
            match = re.search(rf'{keyword}\s*(.*)', text, re.DOTALL | re.IGNORECASE)
            if match:
                remaining_text = match.group(1).strip()
                # Извлекаем первый JSON-объект из оставшегося текста
                json_from_remaining = self._extract_json_objects(remaining_text)
                json_candidates.extend(json_from_remaining)
        
        return json_candidates

    def _clean_response_text(self, text):
        """
        Очищает текст от комментариев и лишних символов.
        """
        # Удаляем комментарии в стиле //
        text = re.sub(r'//.*?$', '', text, flags=re.MULTILINE)
        
        # Удаляем комментарии в стиле /* */
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        
        # Удаляем лишние пробелы и переносы строк
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def _try_parse_json(self, json_text, target_fields):
        """
        Пытается распарсить JSON и вернуть только нужные поля.
        """
        if not json_text or not json_text.strip():
            return None
            
        # Очищаем текст
        cleaned_json = self._clean_response_text(json_text)
        
        try:
            data = json.loads(cleaned_json)
            
            # Проверяем, что это словарь
            if not isinstance(data, dict):
                self.logger.debug(f"JSON не является объектом: {type(data)}")
                return None
            
            # Извлекаем только нужные поля
            result = {}
            for field in target_fields:
                value = data.get(field, "")
                # Убеждаемся, что значение - строка
                if not isinstance(value, str):
                    value = str(value) if value is not None else ""
                result[field] = value
            
            # Проверяем, что хотя бы одно поле не пустое
            if all(not v.strip() for v in result.values()):
                self.logger.debug("Все поля пустые")
                return None
                
            return result
            
        except json.JSONDecodeError as e:
            self.logger.debug(f"JSON decode error: {e} для текста: {repr(cleaned_json[:100])}")
            return None
        except Exception as e:
            self.logger.debug(f"Unexpected error parsing JSON: {e}")
            return None
