# Кадровый Навигатор (Telegram Bot + Web Admin)

## Быстрый старт

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Заполните `.env` и запустите:

```bash
python bot.py
python admin.py
```

## Модули
- `database.py` — инициализация SQLite и CRUD-операции.
- `bot.py` — aiogram long polling, ответы по триггерам сценариев.
- `admin.py` — FastAPI + Jinja2 admin-панель для сценариев, пользователей и рассылок.
