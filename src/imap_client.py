import imaplib
import email
from email.header import decode_header
from datetime import datetime, timedelta
from contextlib import contextmanager
from .utils import retry_with_backoff, normalize_subject, normalize_email, strip_html_tags

class IMAPClient:
    def __init__(self, host, port, email_address, password, logger, sent_folders, timeout=30, retry_attempts=3):
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

    @retry_with_backoff()
    def connect(self):
        self.conn = imaplib.IMAP4_SSL(self.host, self.port)
        self.conn.login(self.email_address, self.password)
        self.conn.sock.settimeout(self.timeout)
        self.logger.info(f"Connected to IMAP server {self.host}")

    def _decode_header(self, value):
        """Декодирует заголовки писем (Subject, From и т.д.)."""
        if not value:
            return ""
        if isinstance(value, bytes):
            value = value.decode('utf-8', errors='replace')
        try:
            decoded_parts = decode_header(value)
            return "".join([
                part.decode(enc or 'utf-8') if isinstance(part, bytes) else part
                for part, enc in decoded_parts
            ]).strip()
        except Exception:
            return value

    def _list_folders(self):
        typ, folders = self.conn.list()
        if typ != 'OK':
            raise RuntimeError("Cannot list IMAP folders")
        folder_names = []
        for f in folders:
            parts = f.decode().split(' "/" ')
            if len(parts) == 2:
                folder_names.append(parts[1].strip('"'))
        return folder_names

    def _select_folder(self, possible_names):
        available = self._list_folders()
        for name in possible_names:
            if name in available:
                self.conn.select(f'"{name}"')
                self.selected_folder = name
                self.logger.info(f"Selected IMAP folder: {name}")
                return True
        self.logger.warning(f"None of the folders {possible_names} found. Available: {available}")
        return False

    @retry_with_backoff()
    def get_sent_emails(self, days_back=14, limit=100):
        if not self._select_folder(self.sent_folders):
            return []

        since_date = (datetime.now() - timedelta(days=days_back)).strftime('%d-%b-%Y')
        search_criteria = f'(SINCE "{since_date}")'
        typ, data = self.conn.search(None, search_criteria)
        if typ != 'OK':
            self.logger.warning("IMAP search failed for sent emails.")
            return []

        ids = data[0].split()[-limit:]
        emails = []
        for eid in ids:
            typ, msg_data = self.conn.fetch(eid, '(RFC822)')
            if typ != "OK":
                continue
            msg = email.message_from_bytes(msg_data[0][1])
            to_addr = self._decode_header(msg.get('To'))
            subject = self._decode_header(msg.get('Subject'))
            msg_id = msg.get('Message-ID')
            date = msg.get('Date')
            norm_subj = normalize_subject(subject)
            references = msg.get('References', '')
            body = self._get_body(msg)
            emails.append({
                "to": to_addr,
                "message_id": msg_id,
                "subject": subject,
                "normalized_subject": norm_subj,
                "date": date,
                "body": body,
                "references": references
            })
        self.logger.info(f"Loaded {len(emails)} sent emails.")
        return emails

    def _get_body(self, msg):
        """Извлечение текста письма (text/plain приоритет, иначе html->text)."""
        if msg.is_multipart():
            for part in msg.walk():
                ctype = part.get_content_type()
                disp = str(part.get('Content-Disposition'))
                if ctype == 'text/plain' and 'attachment' not in disp:
                    return part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='replace')
            for part in msg.walk():
                if part.get_content_type() == 'text/html':
                    html = part.get_payload(decode=True).decode(part.get_content_charset() or 'utf-8', errors='replace')
                    return strip_html_tags(html)
        else:
            ctype = msg.get_content_type()
            if ctype == 'text/plain':
                return msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='replace')
            elif ctype == 'text/html':
                html = msg.get_payload(decode=True).decode(msg.get_content_charset() or 'utf-8', errors='replace')
                return strip_html_tags(html)
        return ""

    @retry_with_backoff()
    def find_reply(self, sent_email_data):
        """
        Поиск ответа на отправленное письмо.
        Стратегии по приоритету:
        1. In-Reply-To
        2. References
        3. FROM + SUBJECT + дата
        4. FROM + частичный SUBJECT
        5. FROM + дата (последнее письмо)
        """
        if not self._select_folder(["INBOX"]):
            return None

        msg_id = sent_email_data.get('message_id')
        references = sent_email_data.get('references')
        to_addr = sent_email_data.get('to')
        subject = sent_email_data.get('subject')
        norm_subject = sent_email_data.get('normalized_subject')
        sent_date = sent_email_data.get('date')
        from_addr = normalize_email(to_addr)
        found = None

        # 1. Поиск по In-Reply-To
        if msg_id:
            typ, data = self.conn.search(None, f'(HEADER In-Reply-To "{msg_id}")')
            if typ == "OK" and data[0]:
                found = self._fetch_first_email(data[0])
        # 2. Поиск по References
        if not found and references:
            typ, data = self.conn.search(None, f'(HEADER References "{references.split()[-1]}")')
            if typ == "OK" and data[0]:
                found = self._fetch_first_email(data[0])
        # 3. FROM + SUBJECT + дата
        if not found:
            search_subject = f'"{subject}"' if " " in subject else subject
            typ, data = self.conn.search(None, f'(FROM "{from_addr}" SUBJECT {search_subject})')
            if typ == "OK" and data[0]:
                found = self._fetch_first_email(data[0])
        # 4. FROM + частичный SUBJECT
        if not found and len(norm_subject) > 5:
            typ, data = self.conn.search(None, f'(FROM "{from_addr}" SUBJECT "{norm_subject[:6]}")')
            if typ == "OK" and data[0]:
                found = self._fetch_first_email(data[0])
        # 5. FROM + дата (берём последнее письмо)
        if not found and sent_date:
            typ, data = self.conn.search(None, f'(FROM "{from_addr}" SINCE "{sent_date[:16]}")')
            if typ == "OK" and data[0]:
                found = self._fetch_first_email(data[0])

        return found

    def _fetch_first_email(self, ids_bytes):
        ids = ids_bytes.split()
        if not ids:
            return None
        eid = ids[-1]
        typ, msg_data = self.conn.fetch(eid, '(RFC822)')
        if typ != "OK":
            return None
        msg = email.message_from_bytes(msg_data[0][1])
        from_addr = self._decode_header(msg.get('From'))
        subject = self._decode_header(msg.get('Subject'))
        date = msg.get('Date')
        body = self._get_body(msg)
        return {
            "from": from_addr,
            "subject": subject,
            "body": body,
            "date": date
        }
