import os
import sys
import time
import csv
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path
from config.settings import settings
from src.utils import (
    filter_rows_with_valid_mail,
    validate_excel_structure,
    normalize_email,
    extract_emails,
    setup_logging,
    mark_mail_column_multiple_emails,
)
from src.imap_client import IMAPClient
from src.lm_studio_client import LMStudioClient
from src.excel_processor import ExcelProcessor

class ProcessingStats:
    """
    Класс для сбора и отображения статистики обработки
    """
    def __init__(self):
        self.start_time = time.time()
        self.total_sent = 0
        self.replies_found = 0
        self.lm_analysis_success = 0
        self.excel_updates = 0
        self.errors = {'imap': 0, 'lm_studio': 0, 'excel': 0}
        self.bad_processing = []

    def log_summary(self, logger):
        elapsed = time.time() - self.start_time
        logger.info("=== Processing summary ===")
        logger.info(f"Total sent emails processed: {self.total_sent}")
        logger.info(f"Replies found: {self.replies_found}")
        logger.info(f"LM Studio analysis successful: {self.lm_analysis_success}")
        logger.info(f"Excel updates performed: {self.excel_updates}")
        logger.info(f"Errors: {self.errors}")
        logger.info(f"Bad processing count: {len(self.bad_processing)}")
        logger.info(f"Elapsed time: {elapsed:.2f} seconds")
        print("\n=== Processing summary ===")
        print(f"Total sent emails processed: {self.total_sent}")
        print(f"Replies found: {self.replies_found}")
        print(f"LM Studio analysis successful: {self.lm_analysis_success}")
        print(f"Excel updates performed: {self.excel_updates}")
        print(f"Errors: {self.errors}")
        print(f"Bad processing count: {len(self.bad_processing)}")
        print(f"Elapsed time: {elapsed:.2f} seconds")

    def add_bad_processing(self, email, body, reason):
        self.bad_processing.append({"email": email, "body": body, "reason": reason})

    def save_bad_processing(self, filename="bad_processing.csv"):
        if not self.bad_processing:
            return
        with open(filename, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["email", "body", "reason"])
            writer.writeheader()
            for row in self.bad_processing:
                writer.writerow(row)

def main():
    # 1. Загрузка конфигурации и настройка логирования
    load_dotenv()
    log_level = os.getenv("LOG_LEVEL") or settings.get("logging.level", "INFO")
    log_file = settings.get("logging.file", "bot.log")
    logger = setup_logging(log_level=log_level, log_file=log_file)
    logger.info("Starting email processing bot...")

    # 2. Проверка переменных окружения
    ZOHO_EMAIL = os.getenv("ZOHO_EMAIL")
    ZOHO_PASS = os.getenv("ZOHO_APP_PASSWORD")
    if not ZOHO_EMAIL or not ZOHO_PASS:
        logger.error("Missing Zoho credentials in environment (.env)")
        sys.exit(1)

    LM_API_URL = os.getenv("LMSTUDIO_API_URL") or settings.get("lm_studio.api_url")
    LM_MODEL_NAME = os.getenv("LM_MODEL_NAME") or settings.get("lm_studio.model_name")
    if not LM_API_URL or not LM_MODEL_NAME:
        logger.error("Missing LM Studio config.")
        sys.exit(1)

    excel_file = settings.get("excel.file_name")
    mail_col = settings.get("excel.columns.mail", "Mail")
    target_cols = [
        settings.get("excel.columns.price_usd", "Price usd"),
        settings.get("excel.columns.price_usd_casino", "Price usd casino"),
        settings.get("excel.columns.payment", "Payment"),
        settings.get("excel.columns.special", "Q"),
        settings.get("excel.columns.comments", "Comments"),
    ]

    # === 1. Загрузка Excel и фильтрация строк без email ===
    try:
        df = pd.read_excel(excel_file)
    except Exception as e:
        logger.error(f"Failed to read Excel file '{excel_file}': {e}")
        sys.exit(1)

    df_valid, idx_multi = filter_rows_with_valid_mail(df, mail_column=mail_col)
    if len(idx_multi) > 0:
        mark_mail_column_multiple_emails(excel_file, mail_column=mail_col, color="FFFF0000")
        logger.warning(f"Found {len(idx_multi)} rows with multiple emails in '{mail_col}' column. Marked in red.")

    # === 2. Валидация структуры Excel (дубликаты, обязательные столбцы) ===
    try:
        validate_excel_structure(df_valid, [mail_col] + target_cols)
    except Exception as e:
        logger.error(f"Excel validation failed: {e}")
        sys.exit(1)

    # 3. Инициализация клиентов
    imap_host = settings.get("imap.host")
    imap_port = settings.get("imap.port")
    imap_sent_folders = settings.get("imap.folders.sent", ["Sent Items", "Sent", "Send", "Отправленные"])
    imap_timeout = settings.get("imap.timeout", 30)
    imap_retry = settings.get("imap.retry_attempts", 3)

    excel = ExcelProcessor(
        file_path=excel_file,
        logger=logger,
        mail_column=mail_col,
        target_columns=target_cols,
        backup=settings.get("excel.backup", True)
    )
    lm_client = LMStudioClient(
        api_url=LM_API_URL,
        model_name=LM_MODEL_NAME,
        logger=logger,
        timeout=settings.get("lm_studio.timeout", 90),
        max_tokens=settings.get("lm_studio.max_tokens", 512),
        temperature=settings.get("lm_studio.temperature", 0.0),
        retry_attempts=settings.get("lm_studio.retry_attempts", 2)
    )

    stats = ProcessingStats()

    # 4. Получение отправленных писем
    with IMAPClient(
        host=imap_host,
        port=imap_port,
        email_address=ZOHO_EMAIL,
        password=ZOHO_PASS,
        logger=logger,
        sent_folders=imap_sent_folders,
        timeout=imap_timeout,
        retry_attempts=imap_retry
    ) as imap:
        sent_emails = imap.get_sent_emails(
            days_back=settings.get("search.days_back", 14),
            limit=settings.get("search.max_emails_per_batch", 10000)
        )
        stats.total_sent = len(sent_emails)

        for sent in sent_emails:
            email_addr = sent.get("to")
            body = sent.get("body", "")
            reply = None
            try:
                reply = imap.find_reply(sent)
            except Exception as e:
                logger.warning(f"IMAP search for reply failed: {e}")
                stats.errors['imap'] += 1
                continue

            if not reply:
                logger.info(f"No reply found for sent email to {email_addr}")
                continue

            stats.replies_found += 1
            reply_body = reply.get("body", "")
            try:
                # 5. Анализ через LM Studio
                extracted = lm_client.analyze_email(
                    email_body=reply_body,
                    target_fields=target_cols
                )
                if not extracted:
                    stats.add_bad_processing(email_addr, reply_body, "LM Studio parsing error")
                    stats.errors['lm_studio'] += 1
                    continue
                stats.lm_analysis_success += 1
            except Exception as e:
                logger.error(f"LM Studio analyze failed: {e}")
                stats.add_bad_processing(email_addr, reply_body, str(e))
                stats.errors['lm_studio'] += 1
                continue

            # 6. Обновление Excel (поиск строк по email с нормализацией)
            try:
                updates = excel.update_rows(normalize_email(email_addr), extracted)
                stats.excel_updates += updates
            except Exception as e:
                logger.error(f"Excel update failed for {email_addr}: {e}")
                stats.errors['excel'] += 1

    # 7. Сохранение результатов
    try:
        excel.save_with_backup()
    except Exception as e:
        logger.error(f"Failed to save Excel: {e}")
        stats.errors['excel'] += 1

    stats.save_bad_processing()
    # 8. Вывод статистики
    stats.log_summary(logger)

if __name__ == "__main__":
    main()
