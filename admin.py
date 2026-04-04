import csv
import io
import json
import asyncio
from datetime import datetime
from pathlib import Path
from uuid import uuid4
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import uvicorn
from fastapi import FastAPI, File, Form, Request, UploadFile
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
SCENARIO_UPLOAD_DIR = Path("uploads/scenarios")
SCENARIO_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
BROADCAST_UPLOAD_DIR = Path("uploads/broadcasts")
BROADCAST_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
TIMEZONE_OPTIONS = ["UTC", "Europe/Moscow", "Asia/Almaty", "Asia/Tashkent"]


def is_auth(request: Request) -> bool:
    return request.session.get("auth") is True


def extract_transitions(scenario: dict[str, Any]) -> list[int]:
    transitions: set[int] = set()
    next_step = scenario.get("next_step")
    if isinstance(next_step, int):
        transitions.add(next_step)

    buttons_json = scenario.get("buttons_json")
    if buttons_json:
        try:
            rows = json.loads(buttons_json)
            for row in rows:
                for button in row:
                    step_id = button.get("step_id")
                    if isinstance(step_id, int):
                        transitions.add(step_id)
                    elif isinstance(step_id, str) and step_id.isdigit():
                        transitions.add(int(step_id))
        except (json.JSONDecodeError, TypeError, AttributeError):
            pass

    return sorted(transitions)


@app.get("/", response_class=HTMLResponse)
async def login_page(request: Request):
    if is_auth(request):
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={"error": None})


@app.post("/", response_class=HTMLResponse)
async def login(request: Request, password: str = Form(...)):
    if password == settings.admin_password:
        request.session["auth"] = True
        return RedirectResponse("/dashboard", status_code=302)
    return templates.TemplateResponse(request=request, name="login.html", context={"error": "Неверный пароль"})


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/", status_code=302)


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    stats = db.get_stats()
    history = db.get_broadcast_history()
    return templates.TemplateResponse(
        request=request,
        name="dashboard.html",
        context={**stats, "broadcast_history": history, "timezone_options": TIMEZONE_OPTIONS},
    )


async def send_broadcast(
    text: str, user_ids: list[int], buttons_json: str | None, photo: bytes | None, photo_path: str | None = None
) -> dict[str, int]:
    sent, failed = 0, 0
    markup: dict[str, Any] | None = None
    if buttons_json:
        markup = {"inline_keyboard": json.loads(buttons_json)}

    api_url = f"https://api.telegram.org/bot{settings.bot_token}"
    async with aiohttp.ClientSession() as session:
        for user_id in user_ids:
            try:
                image_payload = photo
                if not image_payload and photo_path and Path(photo_path).exists():
                    image_payload = Path(photo_path).read_bytes()
                if image_payload:
                    data = aiohttp.FormData()
                    data.add_field("chat_id", str(user_id))
                    data.add_field("caption", text)
                    data.add_field("photo", image_payload, filename="broadcast.jpg", content_type="image/jpeg")
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
    photo: UploadFile | None = File(None),
    scheduled_at: str = Form(""),
    timezone: str = Form("UTC"),
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)

    image_data = await photo.read() if photo and photo.filename else None
    photo_path = None
    if image_data:
        ext = Path(photo.filename or "").suffix or ".jpg"
        filename = f"{uuid4().hex}{ext}"
        saved = BROADCAST_UPLOAD_DIR / filename
        saved.write_bytes(image_data)
        photo_path = str(saved)

    schedule_utc = None
    if scheduled_at.strip():
        tz = ZoneInfo(timezone if timezone in TIMEZONE_OPTIONS else "UTC")
        dt_local = datetime.strptime(scheduled_at, "%Y-%m-%dT%H:%M")
        schedule_utc = dt_local.replace(tzinfo=tz).astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%d %H:%M:%S")

    if schedule_utc:
        db.log_broadcast(
            message_text=text,
            buttons_json=buttons_json or None,
            photo_path=photo_path,
            timezone=timezone,
            scheduled_at=schedule_utc,
            status="pending",
        )
    else:
        user_ids = db.get_active_user_ids()
        broadcast_id = db.log_broadcast(
            message_text=text,
            buttons_json=buttons_json or None,
            photo_path=photo_path,
            timezone=timezone,
            scheduled_at=None,
            status="running",
        )
        result = await send_broadcast(text, user_ids, buttons_json or None, image_data, photo_path=photo_path)
        db.update_broadcast_status(
            broadcast_id, status="done", sent_count=result["sent"], failed_count=result["failed"], error_text=None
        )
    return RedirectResponse("/dashboard", status_code=302)


@app.on_event("startup")
async def start_scheduler() -> None:
    async def scheduler_loop():
        while True:
            try:
                pending = db.get_pending_broadcasts()
                for item in pending:
                    db.update_broadcast_status(item["id"], status="running")
                    user_ids = db.get_active_user_ids()
                    result = await send_broadcast(
                        item["message_text"],
                        user_ids,
                        item.get("buttons_json"),
                        photo=None,
                        photo_path=item.get("photo_path"),
                    )
                    db.update_broadcast_status(
                        item["id"], "done", sent_count=result["sent"], failed_count=result["failed"], error_text=None
                    )
            except Exception as exc:
                # keep background loop alive
                print(f"Broadcast scheduler error: {exc}")
            await asyncio.sleep(20)

    asyncio.create_task(scheduler_loop())


@app.get("/scenarios", response_class=HTMLResponse)
async def scenarios_page(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    scenarios = db.get_all_scenarios()
    start_scenario = db.get_scenario_by_trigger("/start")
    metrics = db.get_scenario_metrics()
    scenario_ids = {s["id"] for s in scenarios}
    scenario_branches = [
        {
            "id": scenario["id"],
            "trigger_text": scenario["trigger_text"],
            "transitions": extract_transitions(scenario),
            "visits_count": metrics.get(scenario["id"], 0),
        }
        for scenario in scenarios
    ]
    incoming_refs: dict[int, int] = {sid: 0 for sid in scenario_ids}
    broken_links: list[dict[str, int]] = []
    for branch in scenario_branches:
        for target in branch["transitions"]:
            if target in incoming_refs:
                incoming_refs[target] += 1
            else:
                broken_links.append({"source": branch["id"], "target": target})
    orphan_ids = sorted([sid for sid, refs in incoming_refs.items() if refs == 0 and sid != (start_scenario or {}).get("id")])

    return templates.TemplateResponse(
        request=request,
        name="scenarios.html",
        context={
            "scenarios": scenarios,
            "scenario_branches": scenario_branches,
            "start_scenario": start_scenario,
            "broken_links": broken_links,
            "orphan_ids": orphan_ids,
        },
    )


@app.post("/scenarios/save")
async def save_scenario(
    request: Request,
    scenario_id: int | None = Form(None),
    trigger_text: str = Form(...),
    bot_reply_text: str = Form(...),
    buttons_json: str = Form(""),
    next_step: int | None = Form(None),
    scenario_image: UploadFile | None = File(None),
    existing_image_path: str = Form(""),
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)

    scenario_image_path = existing_image_path.strip() or None
    if scenario_image and scenario_image.filename:
        ext = Path(scenario_image.filename).suffix or ".jpg"
        filename = f"{uuid4().hex}{ext}"
        image_path = SCENARIO_UPLOAD_DIR / filename
        image_path.write_bytes(await scenario_image.read())
        scenario_image_path = str(image_path)

    safe_buttons_json = buttons_json.strip() or None
    if safe_buttons_json:
        try:
            json.loads(safe_buttons_json)
        except json.JSONDecodeError:
            safe_buttons_json = None

    db.upsert_scenario(
        trigger_text=trigger_text.strip(),
        bot_reply_text=bot_reply_text.strip(),
        buttons_json=safe_buttons_json,
        next_step=next_step,
        scenario_image_path=scenario_image_path,
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
    stats = db.get_stats()
    return templates.TemplateResponse(request=request, name="users.html", context={"users": users, **stats})


@app.post("/users/toggle-ban")
async def toggle_ban(request: Request, user_id: int = Form(...), banned: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.set_blacklist(user_id=user_id, banned=bool(banned))
    db.add_user_event(user_id, "ban_toggle", "banned" if banned else "unbanned")
    return RedirectResponse("/users", status_code=302)


@app.get("/users/{user_id}", response_class=HTMLResponse)
async def user_profile(request: Request, user_id: int):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    users = [u for u in db.get_users_with_status() if u["user_id"] == user_id]
    if not users:
        return RedirectResponse("/users", status_code=302)
    events = db.get_user_events(user_id)
    return templates.TemplateResponse(
        request=request,
        name="user_profile.html",
        context={"user": users[0], "events": events},
    )


@app.post("/users/import-list")
async def import_list(request: Request, mode: str = Form(...), file: UploadFile | None = None):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    if not file or not file.filename:
        return RedirectResponse("/users", status_code=302)
    raw = (await file.read()).decode("utf-8", errors="ignore")
    user_ids = []
    for token in raw.replace(",", "\n").splitlines():
        token = token.strip()
        if token.isdigit():
            user_ids.append(int(token))
    if mode == "blacklist":
        db.import_blacklist(user_ids)
    else:
        db.import_whitelist(user_ids)
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
