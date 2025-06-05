# Email Processing Bot

## Описание

Python-приложение для автоматизированной обработки email (Zoho Mail, IMAP), извлечения ответов, анализа содержимого через LM Studio (qwen3-8b), и обновления Excel-файла (`Mail_USD.xlsx`).

**Основные функции:**
- Поиск отправленных писем за 14 дней
- Поиск ответов (с учётом цепочек)
- Анализ текстов через LM Studio (JSON-ответ)
- Обновление Excel (строго столбцы: C, D, E, F, Q, R)
- Логирование, резервные копии, подсветка изменений
- Сохранение ошибок парсинга в `bad_processing.csv`

## Требования

- Python 3.13
- LM Studio 0.3.16 (локально)
- Zoho Mail (IMAP)
- Excel-файл: Mail_USD.xlsx

## Установка

```bash
pip install -r requirements.txt
```

## Конфигурация

- Все настройки в `config/config.yaml`
- Переменные окружения — см. `.env.example`
- Рабочий Excel: `Mail_USD.xlsx` в корне проекта

## Запуск

```bash
python main.py
```

## Краткая структура

```
email_bot/
├── main.py
├── config/
│   ├── __init__.py
│   ├── settings.py
│   └── config.yaml
├── src/
│   ├── __init__.py
│   ├── imap_client.py
│   ├── excel_processor.py
│   ├── lm_studio_client.py
│   └── utils.py
├── logs/
├── requirements.txt
├── .env.example
└── README.md
```

## Прочее

- Для ошибок парсинга LM Studio создаётся `bad_processing.csv` (email, исходное письмо, причина).
- Все остальные детали — см. техническое задание.
