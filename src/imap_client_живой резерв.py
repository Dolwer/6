import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta
import logging
import re
import time
from email.utils import parsedate_to_datetime

class IMAPClient:
    """
    IMAP-клиент для обработки отправленных писем и поиска ответов.
    """

    def __init__(
        self,
        host,
        port,
        email_address,
        password,
        logger,
        sent_folders,
        timeout=30,
        retry_attempts=3,
    ): 
        self.host = host
        self.port = port
        self.email_address = email_address
        self.password = password
        self.logger = logger
        self.timeout = timeout
        self.retry_attempts = retry_attempts
        self.sent_folders = sent_folders
        self.conn = None
        self.selected_folder = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.conn:
                self.conn.logout()
        except Exception as e:
            self.logger.warning(f"IMAP logout error: {e}")

    def connect(self):
        for attempt in range(self.retry_attempts):
            try:
                self.conn = imaplib.IMAP4_SSL(self.host, self.port)
                self.conn.login(self.email_address, self.password)
                self.conn.sock.settimeout(self.timeout)
                self.logger.info(f"Connected to IMAP server {self.host}")
                return
            except Exception as e:
                self.logger.warning(f"IMAP connection failed (attempt {attempt+1}): {e}")
                if attempt == self.retry_attempts - 1:
                    raise
                time.sleep(2 ** attempt)

    def _decode_header(self, value):
        if not value:
            return ""
        if isinstance(value, bytes):
            value = value.decode("utf-8", errors="replace")
        try:
            decoded_parts = decode_header(value)
            return "".join([
                part.decode(enc or "utf-8") if isinstance(part, bytes) else part
                for part, enc in decoded_parts
            ]).strip()
        except Exception:
            return value

    def _decode_utf7(self, folder_name):
        """Декодирует UTF-7 названия папок IMAP"""
        if not folder_name:
            return folder_name
        try:
            # IMAP использует модифицированный UTF-7
            # Заменяем & на + для стандартного UTF-7, затем декодируем
            if '&' in folder_name and folder_name != 'INBOX':
                # Преобразуем modified UTF-7 в обычный UTF-7
                utf7_name = folder_name.replace('&', '+').replace(',', '/')
                # Декодируем UTF-7
                decoded = utf7_name.encode('ascii').decode('utf-7')
                return decoded
        except Exception as e:
            self.logger.debug(f"UTF-7 decode failed for '{folder_name}': {e}")
        return folder_name

    def _list_folders(self):
        typ, folders = self.conn.list()
        if typ != "OK":
            raise RuntimeError("Cannot list IMAP folders")
        
        folder_info = []
        for f in folders:
            decoded = f.decode()
            # IMAP папки могут возвращаться в разных форматах, ищем имя в конце строки
            match = re.search(r' (?:"([^"]+)"|([^\s]+))$', decoded)
            if match:
                raw_name = match.group(1) or match.group(2)
                decoded_name = self._decode_utf7(raw_name)
                folder_info.append({
                    'raw': raw_name,
                    'decoded': decoded_name
                })
        
        self.logger.debug(f"Available IMAP folders: {[f['decoded'] for f in folder_info]}")
        return folder_info

    def _select_folder(self, possible_names):
        folder_info = self._list_folders()
        
        # Создаем словари для поиска по декодированным и сырым именам
        decoded_map = {f['decoded'].strip().lower(): f['raw'] for f in folder_info}
        raw_map = {f['raw'].strip().lower(): f['raw'] for f in folder_info}
        
        # Логируем доступные папки для отладки
        decoded_names = [f['decoded'] for f in folder_info]
        self.logger.info(f"Decoded folder names: {decoded_names}")
        
        for name in possible_names:
            norm = name.strip().lower()
            real_name = None
            
            # Сначала ищем среди декодированных имен
            if norm in decoded_map:
                real_name = decoded_map[norm]
            # Затем среди сырых имен
            elif norm in raw_map:
                real_name = raw_map[norm]
            
            if real_name:
                try:
                    # Пробуем выбрать папку
                    folder_name = f'"{real_name}"' if " " in real_name else real_name
                    typ, _ = self.conn.select(folder_name)
                    if typ == "OK":
                        self.selected_folder = real_name
                        decoded_folder = self._decode_utf7(real_name)
                        self.logger.info(f"Selected IMAP folder: {decoded_folder} (raw: {real_name})")
                        return True
                    else:
                        self.logger.warning(f"Failed to select folder {real_name} (IMAP returned {typ})")
                except Exception as e:
                    self.logger.warning(f"Error selecting folder {real_name}: {e}")
        
        available_names = [f['decoded'] for f in folder_info]
        self.logger.warning(f"None of the folders {possible_names} found. Available: {available_names}")
        return False

    def get_sent_emails(self, days_back=14, limit=10000, batch_size=50):
        """
        Получает отправленные письма за указанное количество дней. 
        Обрабатывает письма батчами, автоматически переподключается при разрыве соединения.
        """
        if not self._select_folder(self.sent_folders):
            return []

        since_date = (datetime.now() - timedelta(days=days_back)).strftime("%d-%b-%Y")
        search_criteria = f'(SINCE "{since_date}")'
        typ, data = self.conn.search(None, search_criteria)
        if typ != "OK":
            self.logger.warning("IMAP search failed for sent emails.")
            return []

        ids = data[0].split()
        self.logger.info(f"DEBUG: Всего найдено id писем: {len(ids)}")

        emails = []
        total_ids = len(ids)
        for batch_start in range(0, total_ids, batch_size):
            batch = ids[batch_start:batch_start+batch_size]
            for eid in batch:
                tries = 0
                while tries < self.retry_attempts:
                    try:
                        typ, msg_data = self.conn.fetch(eid, "(RFC822)")
                        if typ != "OK":
                            raise imaplib.IMAP4.abort("FETCH failed for id {}".format(eid))
                        msg = email.message_from_bytes(msg_data[0][1])
                        to_addr = self._decode_header(msg.get("To"))
                        subject = self._decode_header(msg.get("Subject"))
                        date = msg.get("Date")
                        message_id = msg.get("Message-ID")
                        body = self._get_email_body(msg)
                        emails.append({
                            "to": to_addr,
                            "subject": subject,
                            "date": date,
                            "message_id": message_id,
                            "body": body,
                        })
                        break  # успешно обработали письмо, выходим из цикла попыток
                    except imaplib.IMAP4.abort as e:
                        self.logger.warning(f"IMAP connection lost during fetch ({e}). Reconnecting (try {tries+1})...")
                        tries += 1
                        time.sleep(2 ** tries)
                        try:
                            self.connect()
                            self._select_folder(self.sent_folders)
                        except Exception as err:
                            self.logger.error(f"Reconnect failed: {err}")
                            if tries == self.retry_attempts:
                                self.logger.error(f"Giving up on eid={eid} after {self.retry_attempts} attempts.")
                    except Exception as e:
                        self.logger.warning(f"Error fetching message {eid}: {e}")
                        break  # не IMAP abort, не будем пытаться ещё раз
        self.logger.info(f"Fetched {len(emails)} sent emails from '{self.selected_folder}'")
        return emails

    def _get_email_body(self, msg):
        try:
            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        payload = part.get_payload(decode=True)
                        if payload is not None:
                            return payload.decode(errors="replace")
                # fallback to first available if no plain
                for part in msg.walk():
                    if "attachment" not in str(part.get("Content-Disposition", "")):
                        payload = part.get_payload(decode=True)
                        if payload is not None:
                            return payload.decode(errors="replace")
            else:
                payload = msg.get_payload(decode=True)
                if payload is not None:
                    return payload.decode(errors="replace")
            return ""  # если тело письма не найдено
        except Exception as e:
            self.logger.warning(f"Failed to extract email body: {e}")
            return ""

    def find_reply(self, sent_email_data):
        """
        Ищет ответы на отправленное письмо во всех папках.
        """
        if not sent_email_data.get('message_id'):
            return None
        
        # Извлекаем данные отправленного письма
        sent_message_id = sent_email_data['message_id'].strip('<>')
        sent_to = sent_email_data.get('to', '').lower()
        sent_subject = sent_email_data.get('subject', '')
        sent_date = sent_email_data.get('date')
        
        # Парсим дату отправки
        try:
            sent_datetime = parsedate_to_datetime(sent_date) if sent_date else datetime.now()
        except Exception:
            sent_datetime = datetime.now()
        
        # Ищем ответы в течение 30 дней после отправки
        end_date = sent_datetime + timedelta(days=30)
        
        # Получаем список всех папок для поиска
        folder_info = self._list_folders()
        search_folders = ['INBOX']  # Основная папка для поиска
        
        # Добавляем другие папки, исключая отправленные, спам и корзину
        exclude_patterns = ['sent', 'отправ', 'spam', 'junk', 'trash', 'корзин', 'удален']
        for folder in folder_info:
            folder_name = folder['decoded'].lower()
            if not any(pattern in folder_name for pattern in exclude_patterns):
                if folder['raw'] not in search_folders:
                    search_folders.append(folder['raw'])
        
        # Поиск ответов
        for folder_name in search_folders:
            try:
                # Выбираем папку
                folder_select = f'"{folder_name}"' if " " in folder_name else folder_name
                typ, _ = self.conn.select(folder_select)
                if typ != "OK":
                    continue
                
                # Формируем критерии поиска
                since_date = sent_datetime.strftime("%d-%b-%Y")
                before_date = end_date.strftime("%d-%b-%Y")
                
                # Поиск по разным критериям
                search_criteria = [
                    f'(SINCE "{since_date}" BEFORE "{before_date}" HEADER "In-Reply-To" "{sent_message_id}")',
                    f'(SINCE "{since_date}" BEFORE "{before_date}" HEADER "References" "{sent_message_id}")',
                    f'(SINCE "{since_date}" BEFORE "{before_date}" FROM "{sent_to}")',
                ]
                
                # Если есть тема, добавляем поиск по Re:
                if sent_subject:
                    clean_subject = re.sub(r'^(Re:|RE:|re:)\s*', '', sent_subject).strip()
                    if clean_subject:
                        search_criteria.append(f'(SINCE "{since_date}" BEFORE "{before_date}" SUBJECT "Re: {clean_subject}")')
                        search_criteria.append(f'(SINCE "{since_date}" BEFORE "{before_date}" SUBJECT "RE: {clean_subject}")')
                
                found_replies = []
                
                for criteria in search_criteria:
                    try:
                        typ, data = self.conn.search(None, criteria)
                        if typ == "OK" and data[0]:
                            email_ids = data[0].split()
                            for eid in email_ids:
                                reply_data = self._fetch_and_validate_reply(eid, sent_message_id, sent_to, sent_subject, sent_datetime)
                                if reply_data:
                                    found_replies.append(reply_data)
                    except Exception as e:
                        self.logger.debug(f"Search failed for criteria {criteria}: {e}")
                        continue
                
                # Если нашли ответы, возвращаем лучший
                if found_replies:
                    return self._select_best_reply(found_replies, sent_datetime)
                    
            except Exception as e:
                self.logger.debug(f"Error searching in folder {folder_name}: {e}")
                continue
        
        return None

    def _fetch_and_validate_reply(self, email_id, sent_message_id, sent_to, sent_subject, sent_datetime):
        """
        Получает письмо и проверяет, является ли оно ответом.
        """
        try:
            typ, msg_data = self.conn.fetch(email_id, "(RFC822)")
            if typ != "OK":
                return None
                
            msg = email.message_from_bytes(msg_data[0][1])
            
            # Извлекаем заголовки
            in_reply_to = msg.get("In-Reply-To", "").strip('<>')
            references = msg.get("References", "")
            from_addr = self._decode_header(msg.get("From", "")).lower()
            subject = self._decode_header(msg.get("Subject", ""))
            date_str = msg.get("Date")
            
            # Парсим дату ответа
            try:
                reply_date = parsedate_to_datetime(date_str) if date_str else None
            except Exception:
                reply_date = None
            
            # Проверяем, что это ответ после отправленного письма
            if reply_date and reply_date <= sent_datetime:
                return None
            
            # Проверяем критерии ответа
            is_reply = False
            confidence = 0
            
            # 1. Прямая ссылка через In-Reply-To
            if in_reply_to == sent_message_id:
                is_reply = True
                confidence += 50
            
            # 2. Ссылка через References
            if sent_message_id in references:
                is_reply = True
                confidence += 40
            
            # 3. От получателя оригинального письма
            if sent_to in from_addr:
                confidence += 30
            
            # 4. Тема начинается с Re:
            if subject.lower().startswith(('re:', 'ре:')):
                confidence += 20
            
            # 5. Проверяем схожесть темы
            if self._subjects_match(subject, sent_subject):
                confidence += 15
            
            if is_reply or confidence >= 40:
                body = self._get_email_body(msg)
                return {
                    'message_id': msg.get("Message-ID", ""),
                    'from': from_addr,
                    'subject': subject,
                    'date': date_str,
                    'body': body,
                    'confidence': confidence,
                    'reply_date': reply_date,
                    'in_reply_to': in_reply_to,
                    'references': references
                }
        
        except Exception as e:
            self.logger.debug(f"Error fetching email {email_id}: {e}")
        
        return None

    def _subjects_match(self, reply_subject, original_subject):
        """
        Проверяет соответствие тем писем.
        """
        if not reply_subject or not original_subject:
            return False
        
        # Очищаем темы от Re:, Fwd: и т.д.
        clean_reply = re.sub(r'^(Re:|RE:|re:|Fwd:|FWD:|fwd:)\s*', '', reply_subject).strip()
        clean_original = re.sub(r'^(Re:|RE:|re:|Fwd:|FWD:|fwd:)\s*', '', original_subject).strip()
        
        # Проверяем точное совпадение
        if clean_reply.lower() == clean_original.lower():
            return True
        
        # Проверяем частичное совпадение (70% слов)
        reply_words = set(clean_reply.lower().split())
        original_words = set(clean_original.lower().split())
        
        if len(original_words) > 0:
            intersection = len(reply_words & original_words)
            similarity = intersection / len(original_words)
            return similarity >= 0.7
        
        return False

    def _select_best_reply(self, replies, sent_datetime):
        """
        Выбирает лучший ответ из найденных.
        """
        if not replies:
            return None
        
        # Сортируем по уверенности, затем по дате (ближайший к отправке)
        replies.sort(key=lambda x: (
            -x['confidence'],  # Высокая уверенность
            abs((x['reply_date'] - sent_datetime).total_seconds()) if x['reply_date'] else float('inf')  # Близкая дата
        ))
        
        return replies[0]
