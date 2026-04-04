from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import sqlite3
import os

app = FastAPI()

# Путь к директории с шаблонами относительно этого файла
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# Путь к базе данных бота (на один уровень выше, в папке bot)
DB_PATH = os.path.join(BASE_DIR, "..", "bot", "bot.db")

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/", response_class=HTMLResponse)
async def admin_panel(request: Request):
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Статистика
        cur.execute("SELECT COUNT(*) FROM users")
        total_users = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reminders WHERE sent = 0")
        pending_reminders = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM reminders WHERE sent = 1")
        sent_reminders = cur.fetchone()[0]
        
        # Список пользователей
        cur.execute("SELECT user_id, username, created_at FROM users ORDER BY created_at DESC LIMIT 50")
        users = cur.fetchall()
        
        conn.close()
        
        return templates.TemplateResponse("index.html", {
            "request": request,
            "total_users": total_users,
            "pending_reminders": pending_reminders,
            "sent_reminders": sent_reminders,
            "users": users
        })
    except Exception as e:
        return HTMLResponse(f"<h1>Ошибка базы данных</h1><p>{str(e)}</p><p>Убедитесь, что бот запущен и создал таблицы.</p>", status_code=500)

@app.post("/send_message")
async def send_message(user_id: int = Form(...), text: str = Form(...)):
    # Заглушка для будущей функциональности
    return HTMLResponse("<script>alert('Функция отправки сообщений будет добавлена в следующей версии'); window.location.href='/';</script>")

if __name__ == "__main__":
    import uvicorn
    # Запуск на всех интерфейсах, порт 8000
    uvicorn.run(app, host="0.0.0.0", port=8000)
