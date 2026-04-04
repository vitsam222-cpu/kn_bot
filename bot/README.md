# Telegram Bot with Reminders

Бот для выдачи документов и отложенных напоминаний на Python + Aiogram 3.

## 📋 Требования

- Python 3.10+
- Ubuntu 22.04 (рекомендуется для VDS)
- Токен Telegram бота (получить у [@BotFather](https://t.me/BotFather))

## 🚀 Установка

### 1. Клонирование репозитория

```bash
cd /opt
git clone <your-repo-url> bot
cd bot
```

### 2. Создание виртуального окружения

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Настройка конфигурации

```bash
cp .env.example .env
nano .env
```

В файле `.env` укажите ваш токен:

```env
BOT_TOKEN=1234567890:ABCdefGHIjklMNOpqrsTUVwxyz
DATABASE_PATH=./bot_database.db
LOG_LEVEL=INFO
```

## ▶️ Запуск бота

### Ручной запуск

```bash
source venv/bin/activate
python3 main.py
```

### Автозапуск через systemd

1. Создайте файл сервиса:

```bash
sudo nano /etc/systemd/system/telegram-bot.service
```

2. Добавьте содержимое (замените пути на актуальные):

```ini
[Unit]
Description=Telegram Bot Service
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/opt/bot
Environment="PATH=/opt/bot/venv/bin"
ExecStart=/opt/bot/venv/bin/python3 /opt/bot/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

3. Включите и запустите сервис:

```bash
sudo systemctl daemon-reload
sudo systemctl enable telegram-bot
sudo systemctl start telegram-bot
```

4. Проверка статуса:

```bash
sudo systemctl status telegram-bot
```

5. Просмотр логов:

```bash
sudo journalctl -u telegram-bot -f
```

## 📁 Структура проекта

```
bot/
├── main.py              # Точка входа, запуск polling + scheduler
├── config.py            # Загрузка переменных из .env
├── database.py          # CRUD операции с SQLite (async)
├── handlers/
│   ├── __init__.py
│   ├── start.py         # /start + главное меню
│   ├── documents.py     # Обработка кнопки "Документы" + триггер
│   └── fsm_steps.py     # Пошаговые сценарии (FSM)
├── scheduler.py         # Фоновая проверка напоминаний
├── requirements.txt
├── .env.example
└── README.md
```

## 🔧 Функционал

### Команды

- `/start` — Приветствие и главное меню
- `/cancel` — Отмена текущего FSM-сценария

### Возможности

1. **Главное меню** — Инлайн-кнопки для навигации
2. **Документы** — Выдача списка документов с ссылками
3. **Напоминания** — Автоматическая отправка напоминаний через 3 дня после триггера
4. **FSM** — Готовый шаблон для пошаговых сценариев

## 🗄 База данных

### Таблица `users`

| Поле | Тип | Описание |
|------|-----|----------|
| `user_id` | INTEGER PK | Telegram ID |
| `username` | TEXT | Username (nullable) |
| `created_at` | TIMESTAMP | Дата регистрации |

### Таблица `reminders`

| Поле | Тип | Описание |
|------|-----|----------|
| `id` | INTEGER PK | Автоинкремент |
| `user_id` | INTEGER FK | Связь с users |
| `trigger_type` | TEXT | Тип триггера |
| `triggered_at` | TIMESTAMP | Время нажатия |
| `delay_days` | INTEGER | Задержка в днях |
| `sent` | BOOLEAN | Отправлено ли напоминание |

## ⚙️ Планировщик

- Проверка напоминаний каждые 60 минут
- Graceful shutdown при SIGINT/SIGTERM
- Повторная попытка отправки при ошибке

## 🛠 Расширение функционала

### Добавление новых обработчиков

1. Создайте файл в `handlers/`:

```python
# handlers/new_feature.py
from aiogram import Router, F
from aiogram.types import Message

router = Router(name="new_feature")

@router.message(F.text == "test")
async def handle_test(message: Message) -> None:
    await message.answer("Test handler works!")
```

2. Зарегистрируйте роутер в `main.py`:

```python
from handlers import new_feature
dp.include_routers(new_feature.router)
```

### Изменение интервала проверки напоминаний

В `main.py` измените параметр `check_interval`:

```python
scheduler = ReminderScheduler(bot=bot, db=db, check_interval=1800)  # 30 минут
```

## 📝 Лицензия

MIT
