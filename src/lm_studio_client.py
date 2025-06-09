import requests
import json
import re
from .utils import retry_with_backoff, strip_html_tags

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
        # ОЧИСТКА ПИСЬМА: убрать HTML, цитаты, подписи и т.п.
        clean_body = self._preprocess_body(email_body)
        prompt = self._create_prompt(clean_body, target_fields)

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

    def _preprocess_body(self, email_body):
        """
        Очищает тело письма:
        - убирает HTML
        - удаляет цитаты (строки, начинающиеся с '>')
        - удаляет подписи (простая эвристика для 'Best regards', 'On ... wrote:')
        """
        if not email_body:
            return ""

        # Убираем HTML, если есть
        body = strip_html_tags(email_body)

        # Убираем цитаты (строки, начинающиеся с '>')
        lines = body.splitlines()
        cleaned_lines = []
        for line in lines:
            if line.strip().startswith(">"):
                continue
            # Убираем всё, что идет после стандартного "On <date> <name> wrote:"
            if re.match(r"^On .+ wrote:", line):
                break
            if "Best regards" in line or "Regards," in line or "Sent from" in line:
                break
            cleaned_lines.append(line)
        cleaned = "\n".join(cleaned_lines).strip()
        return cleaned

    def _create_prompt(self, email_body, target_fields):
        """
        Создание очень строгого промпта для LM Studio.
        """
        fields_example = {field: "" for field in target_fields}
        example_json = json.dumps(fields_example, ensure_ascii=False, indent=2)

        prompt = (
            "Извлеки информацию из письма и верни ТОЛЬКО один валидный JSON в указанном формате.\n"
            "Не добавляй пояснения, комментарии, markdown, вложения, списки, ссылки или другой текст, размышления.\n"
            "Не добавляй никаких заголовков, только JSON!\n"
            "Если информации нет — оставь поле пустым (\"\").\n"
            "Нельзя добавлять поля, которых нет в примере.\n\n"
            f"Формат:\n{example_json}\n\n"
            "Текст письма:\n"
            f"{email_body}\n\n"
            "JSON:"
        )
        return prompt

    def _parse_response(self, response_text, target_fields):
        """
        Усиленный парсинг ответа LM Studio: 
        - поиск JSON, попытка починить, fallback-стратегии.
        """
        if not response_text or not response_text.strip():
            self.logger.debug("Пустой ответ от LM Studio")
            return None

        # ШАГ 1: Поиск валидного JSON-объекта
        json_objects = self._extract_json_objects(response_text)
        for json_obj in json_objects:
            parsed = self._try_parse_json(json_obj, target_fields)
            if parsed is not None:
                return parsed

        # ШАГ 2: Fallback — пробуем весь текст как JSON (после чистки)
        cleaned_text = self._clean_response_text(response_text)
        parsed = self._try_parse_json(cleaned_text, target_fields)
        if parsed is not None:
            return parsed

        # ШАГ 3: Fallback — поиск JSON после ключевых слов
        json_after_keywords = self._extract_json_after_keywords(response_text)
        for json_text in json_after_keywords:
            parsed = self._try_parse_json(json_text, target_fields)
            if parsed is not None:
                return parsed

        self.logger.debug(f"Все стратегии парсинга не сработали для: {repr(response_text[:200])}")
        return None

    def _extract_json_objects(self, text):
        """
        Извлекает все JSON-объекты из текста, учитывая вложенность и обрезку.
        """
        json_objects = []
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

        # Если не нашли ни одного объекта — ищем последний незакрытый и пытаемся починить
        if not json_objects:
            last_open = text.rfind('{')
            if last_open != -1:
                candidate = text[last_open:].strip()
                fixed_candidate = self._try_fix_truncated_json(candidate)
                if fixed_candidate:
                    self.logger.warning("LM Studio: Auto-fix applied to truncated JSON.")
                    json_objects.append(fixed_candidate)
        return json_objects

    def _try_fix_truncated_json(self, candidate):
        """
        Пробует аккуратно «закрыть» обрезанный JSON-объект.
        """
        candidate = candidate.rstrip()
        # Закрываем кавычки, если нужно
        if candidate.count('"') % 2 != 0:
            candidate += '"'
        # Закрываем скобки
        if candidate.count('{') > candidate.count('}'):
            candidate += '}'
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            return None

    def _extract_json_after_keywords(self, text):
        """
        Ищет JSON после ключевых слов типа "JSON:", "Результат:" и т.д.
        """
        keywords = [
            r'JSON\s*:',
            r'Результат\s*:',
            r'Ответ\s*:',
            r'Извлеченные данные\s*:',
        ]
        json_candidates = []
        for keyword in keywords:
            pattern = rf'{keyword}\s*(\{{.*?\}})'
            matches = re.findall(pattern, text, re.DOTALL | re.IGNORECASE)
            json_candidates.extend(matches)
            match = re.search(rf'{keyword}\s*(.*)', text, re.DOTALL | re.IGNORECASE)
            if match:
                remaining_text = match.group(1).strip()
                json_from_remaining = self._extract_json_objects(remaining_text)
                json_candidates.extend(json_from_remaining)
        return json_candidates

    def _clean_response_text(self, text):
        """
        Очищает текст от комментариев и лишних символов.
        """
        text = re.sub(r'//.*?$', '', text, flags=re.MULTILINE)
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _try_parse_json(self, json_text, target_fields):
        """
        Пробует распарсить JSON и вернуть только нужные поля.
        """
        if not json_text or not json_text.strip():
            return None
        cleaned_json = self._clean_response_text(json_text)
        try:
            data = json.loads(cleaned_json)
            if not isinstance(data, dict):
                self.logger.debug(f"JSON не является объектом: {type(data)}")
                return None
            # Только нужные поля, все к строке
            result = {}
            for field in target_fields:
                value = data.get(field, "")
                if not isinstance(value, str):
                    value = str(value) if value is not None else ""
                result[field] = value
            # Хотя бы одно непустое поле
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
