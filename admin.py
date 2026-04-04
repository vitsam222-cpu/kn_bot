import csv
import io
import json
from typing import Any

import aiohttp
import uvicorn
from fastapi import FastAPI, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from config import settings
from database import Database

app = FastAPI(title="Кадровый Навигатор Admin")
app.add_middleware(SessionMiddleware, secret_key=settings.secret_key)
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
db = Database()


def is_auth(request: Request) -> bool:
    return request.session.get("auth") is True


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_auth(request):
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": None})


@app.post("/", response_class=HTMLResponse)
async def login(request: Request, password: str = Form(...)):
    if password == settings.admin_password:
        request.session["auth"] = True
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "error": "Неверный пароль"})


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    stats = db.get_stats()
    return templates.TemplateResponse("dashboard.html", {"request": request, **stats})


async def send_broadcast(text: str, user_ids: list[int], buttons_json: str | None, photo: bytes | None) -> dict[str, int]:
    sent, failed = 0, 0
    markup: dict[str, Any] | None = None
    if buttons_json:
        markup = {"inline_keyboard": json.loads(buttons_json)}

    api_url = f"https://api.telegram.org/bot{settings.bot_token}"
    async with aiohttp.ClientSession() as session:
        for user_id in user_ids:
            try:
                if photo:
                    data = aiohttp.FormData()
                    data.add_field("chat_id", str(user_id))
                    data.add_field("caption", text)
                    data.add_field("photo", photo, filename="broadcast.jpg", content_type="image/jpeg")
                    if markup:
                        data.add_field("reply_markup", json.dumps(markup, ensure_ascii=False))
                    async with session.post(f"{api_url}/sendPhoto", data=data) as resp:
                        ok = resp.status == 200 and (await resp.json()).get("ok")
                else:
                    payload = {"chat_id": user_id, "text": text}
                    if markup:
                        payload["reply_markup"] = markup
                    async with session.post(f"{api_url}/sendMessage", json=payload) as resp:
                        ok = resp.status == 200 and (await resp.json()).get("ok")
                if ok:
                    sent += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

    return {"sent": sent, "failed": failed}


@app.post("/broadcast")
async def broadcast(
    request: Request,
    text: str = Form(...),
    buttons_json: str = Form(""),
    photo: UploadFile | None = None,
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)

    image_data = await photo.read() if photo and photo.filename else None
    user_ids = db.get_active_user_ids()
    await send_broadcast(text, user_ids, buttons_json or None, image_data)
    return RedirectResponse("/dashboard", status_code=302)


@app.get("/scenarios", response_class=HTMLResponse)
async def scenarios_page(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    scenarios = db.get_all_scenarios()
    return templates.TemplateResponse("scenarios.html", {"request": request, "scenarios": scenarios})


@app.post("/scenarios/save")
async def save_scenario(
    request: Request,
    scenario_id: int | None = Form(None),
    trigger_text: str = Form(...),
    bot_reply_text: str = Form(...),
    buttons_json: str = Form(""),
    next_step: int | None = Form(None),
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.upsert_scenario(
        trigger_text=trigger_text.strip(),
        bot_reply_text=bot_reply_text.strip(),
        buttons_json=buttons_json.strip() or None,
        next_step=next_step,
        scenario_id=scenario_id,
    )
    return RedirectResponse("/scenarios", status_code=302)


@app.post("/scenarios/delete")
async def delete_scenario(request: Request, scenario_id: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.delete_scenario(scenario_id)
    return RedirectResponse("/scenarios", status_code=302)


@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    users = db.get_users_with_status()
    return templates.TemplateResponse("users.html", {"request": request, "users": users})


@app.post("/users/toggle-ban")
async def toggle_ban(request: Request, user_id: int = Form(...), banned: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.set_blacklist(user_id=user_id, banned=bool(banned))
    return RedirectResponse("/users", status_code=302)


@app.get("/users/export")
async def export_users(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)

    users = db.get_users_with_status()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["user_id", "username", "created_at", "status"])
    for user in users:
        writer.writerow(
            [user["user_id"], user["username"] or "", user["created_at"], "banned" if user["is_banned"] else "active"]
        )

    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=users.csv"},
    )


if __name__ == "__main__":
    uvicorn.run(app, host=settings.admin_host, port=settings.admin_port)
