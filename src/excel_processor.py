import pandas as pd
import openpyxl
from openpyxl.styles import PatternFill
from pathlib import Path
import shutil
import datetime
from .utils import normalize_email, validate_excel_structure, safe_filename

class ExcelProcessor:
    def __init__(self, file_path, logger, mail_column="Mail", target_columns=None, backup=True):
        self.file_path = Path(file_path)
        self.logger = logger
        self.mail_column = mail_column
        self.target_columns = target_columns or []
        self.backup = backup
        self.df = None
        self.email_index = {}
        self.changes = set()  # (row_idx, col_name)
        self.load_data()

    def load_data(self):
        """
        Загрузка Excel файла.
        Проверка существования файла и необходимых колонок.
        """
        if not self.file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.file_path}")
        self.df = pd.read_excel(self.file_path, engine="openpyxl")
        validate_excel_structure(self.df, self.target_columns + [self.mail_column])
        self.create_email_index()
        self.logger.info(f"Loaded Excel: {self.file_path}, rows: {len(self.df)}")

    def create_email_index(self):
        """
        Создание индекса email -> [список индексов строк]
        Нормализация email адресов (приведение к нижнему регистру, trim)
        """
        self.email_index = {}
        for idx, row in self.df.iterrows():
            email_val = normalize_email(str(row[self.mail_column]))
            if email_val not in self.email_index:
                self.email_index[email_val] = []
            self.email_index[email_val].append(idx)

    def update_rows(self, email, extracted_data):
        """
        Обновление строк в DataFrame для указанного email.
        Отслеживание изменений для подсветки.
        """
        email = normalize_email(email)
        if email not in self.email_index:
            self.logger.warning(f"Email not found in Excel: {email}")
            return 0
        update_count = 0
        for idx in self.email_index[email]:
            for col in self.target_columns:
                old_val = str(self.df.at[idx, col]) if pd.notna(self.df.at[idx, col]) else ""
                new_val = extracted_data.get(col, "")
                if new_val and old_val != new_val:
                    self.df.at[idx, col] = new_val
                    self.changes.add((idx, col))
                    update_count += 1
        if update_count:
            self.logger.info(f"Excel updated for {email}: {update_count} changes")
        return update_count

    def save_with_backup(self):
        """
        Сохранение файла с созданием резервной копии. Подсветка измененных ячеек желтым цветом.
        """
        if self.backup:
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.file_path.parent / f"{self.file_path.stem}_backup_{ts}{self.file_path.suffix}"
            shutil.copy2(self.file_path, backup_path)
            self.logger.info(f"Backup Excel created: {backup_path}")

        # Сохраняем DataFrame
        self.df.to_excel(self.file_path, index=False, engine="openpyxl")
        if self.changes:
            self.highlight_changes()
            self.logger.info("Excel changes highlighted.")

    def highlight_changes(self):
        """
        Подсветка изменённых ячеек в Excel файле (использует openpyxl).
        """
        wb = openpyxl.load_workbook(self.file_path)
        ws = wb.active
        header = [cell.value for cell in ws[1]]
        fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

        for idx, col in self.changes:
            row_num = idx + 2  # pandas 0-based, Excel 1-based (+1 for header)
            if col in header:
                col_num = header.index(col) + 1
                ws.cell(row=row_num, column=col_num).fill = fill

        wb.save(self.file_path)
        self.logger.info(f"Highlighted {len(self.changes)} changed cells in Excel.")
        self.changes.clear()
