import csv
import io
import json
import asyncio
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus
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
TIMEZONE_OPTIONS = [
    ("Europe/Kaliningrad", "Калининград (UTC+2)"),
    ("Europe/Moscow", "Москва (UTC+3)"),
    ("Europe/Simferopol", "Симферополь (UTC+3)"),
    ("Europe/Volgograd", "Волгоград (UTC+3)"),
    ("Europe/Samara", "Самара (UTC+4)"),
    ("Asia/Yekaterinburg", "Екатеринбург (UTC+5)"),
    ("Asia/Omsk", "Омск (UTC+6)"),
    ("Asia/Novosibirsk", "Новосибирск (UTC+7)"),
    ("Asia/Barnaul", "Барнаул (UTC+7)"),
    ("Asia/Tomsk", "Томск (UTC+7)"),
    ("Asia/Novokuznetsk", "Новокузнецк (UTC+7)"),
    ("Asia/Krasnoyarsk", "Красноярск (UTC+7)"),
    ("Asia/Irkutsk", "Иркутск (UTC+8)"),
    ("Asia/Chita", "Чита (UTC+9)"),
    ("Asia/Yakutsk", "Якутск (UTC+9)"),
    ("Asia/Khandyga", "Хандыга (UTC+9)"),
    ("Asia/Vladivostok", "Владивосток (UTC+10)"),
    ("Asia/Ust-Nera", "Усть-Нера (UTC+10)"),
    ("Asia/Sakhalin", "Сахалин (UTC+11)"),
    ("Asia/Magadan", "Магадан (UTC+11)"),
    ("Asia/Srednekolymsk", "Среднеколымск (UTC+11)"),
    ("Asia/Kamchatka", "Камчатка (UTC+12)"),
    ("Asia/Anadyr", "Анадырь (UTC+12)"),
]


def is_auth(request: Request) -> bool:
    return request.session.get("auth") is True


def render_template(request: Request, name: str, context: dict[str, Any]):
    shared = {"tasks_pending_count": db.get_pending_tasks_count()}
    merged = {**context, **shared}
    return templates.TemplateResponse(request=request, name=name, context=merged)


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
    return render_template(request=request, name="login.html", context={"error": None})


@app.post("/", response_class=HTMLResponse)
async def login(request: Request, password: str = Form(...)):
    if password == settings.admin_password:
        request.session["auth"] = True
        return RedirectResponse("/dashboard", status_code=302)
    return render_template(request=request, name="login.html", context={"error": "Неверный пароль"})


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
    raw_drip_rules = db.get_step_broadcast_rules(active_only=False)
    drip_rules: list[dict[str, Any]] = []
    for rule in raw_drip_rules:
        scenario_id = db.resolve_scenario_ref(str(rule.get("scenario_ref", "")).strip())
        if scenario_id and int(rule.get("is_active") or 0) == 1:
            due_users = db.get_due_users_for_step_rule_detailed(
                rule_id=int(rule["id"]),
                scenario_id=scenario_id,
                delay_days=int(rule.get("delay_days") or 0),
                weekly_limit=int(rule.get("weekly_limit") or 1),
                send_time=str(rule.get("send_time") or "00:00"),
                required_tag=(rule.get("required_tag") or None),
                limit=30,
            )
            next_trigger_at = db.get_rule_next_trigger_at(
                scenario_id=scenario_id, delay_days=int(rule.get("delay_days") or 0)
            )
        else:
            due_users = []
            next_trigger_at = None
        rule_view = dict(rule)
        rule_view["due_users"] = due_users
        rule_view["due_count"] = len(due_users)
        rule_view["next_trigger_at"] = next_trigger_at
        drip_rules.append(rule_view)
    scenarios = db.get_all_scenarios()
    all_tags = db.get_all_tags()
    flash_msg = request.query_params.get("msg")
    return render_template(
        request=request,
        name="dashboard.html",
        context={
            **stats,
            "broadcast_history": history,
            "timezone_options": TIMEZONE_OPTIONS,
            "flash_msg": flash_msg,
            "drip_rules": drip_rules,
            "scenarios": scenarios,
            "all_tags": all_tags,
        },
    )


async def send_broadcast(
    text: str,
    user_ids: list[int],
    buttons_json: str | None,
    photo: bytes | None,
    photo_path: str | None = None,
    broadcast_id: int | None = None,
    rule_id: int | None = None,
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
                    data.add_field("parse_mode", "Markdown")
                    data.add_field("photo", image_payload, filename="broadcast.jpg", content_type="image/jpeg")
                    if markup:
                        data.add_field("reply_markup", json.dumps(markup, ensure_ascii=False))
                    async with session.post(f"{api_url}/sendPhoto", data=data) as resp:
                        ok = resp.status == 200 and (await resp.json()).get("ok")
                else:
                    payload = {"chat_id": user_id, "text": text, "parse_mode": "Markdown"}
                    if markup:
                        payload["reply_markup"] = markup
                    async with session.post(f"{api_url}/sendMessage", json=payload) as resp:
                        ok = resp.status == 200 and (await resp.json()).get("ok")
                if ok:
                    sent += 1
                    db.log_broadcast_delivery(
                        user_id=user_id,
                        status="sent",
                        message_text=text,
                        broadcast_id=broadcast_id,
                        rule_id=rule_id,
                        error_text=None,
                    )
                else:
                    failed += 1
                    db.log_broadcast_delivery(
                        user_id=user_id,
                        status="failed",
                        message_text=text,
                        broadcast_id=broadcast_id,
                        rule_id=rule_id,
                        error_text="Telegram API response not ok",
                    )
            except Exception:
                failed += 1
                db.log_broadcast_delivery(
                    user_id=user_id,
                    status="failed",
                    message_text=text,
                    broadcast_id=broadcast_id,
                    rule_id=rule_id,
                    error_text="Exception during send",
                )

    return {"sent": sent, "failed": failed}


@app.post("/broadcast")
async def broadcast(
    request: Request,
    text: str = Form(...),
    buttons_json: str = Form(""),
    photo: UploadFile | None = File(None),
    scheduled_at: str = Form(""),
    timezone: str = Form("UTC"),
    segment_type: str = Form("all"),
    segment_value: str = Form(""),
    segment_step_ref: str = Form(""),
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
        photo_path = str(saved.resolve())

    schedule_utc = None
    if scheduled_at.strip():
        valid_timezones = {tz for tz, _ in TIMEZONE_OPTIONS}
        tz = ZoneInfo(timezone if timezone in valid_timezones else "Europe/Moscow")
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
        msg = "Рассылка запланирована"
    else:
        user_ids = db.get_segment_user_ids(
            segment_type=segment_type,
            segment_value=segment_value.strip() or None,
            scenario_ref=segment_step_ref.strip() or None,
        )
        broadcast_id = db.log_broadcast(
            message_text=text,
            buttons_json=buttons_json or None,
            photo_path=photo_path,
            timezone=timezone,
            scheduled_at=None,
            status="running",
        )
        db.create_task(
            task_type="broadcast_send",
            payload={
                "broadcast_id": broadcast_id,
                "text": text,
                "user_ids": user_ids,
                "buttons_json": buttons_json or None,
                "photo_path": photo_path,
            },
            message=f"Рассылка #{broadcast_id} поставлена в очередь",
        )
        msg = f"Рассылка #{broadcast_id} добавлена в очередь"
    return RedirectResponse(f"/dashboard?msg={quote_plus(msg)}", status_code=302)


@app.post("/broadcast/rule")
async def create_step_rule(
    request: Request,
    rule_id: int | None = Form(None),
    scenario_ref: str = Form(...),
    delay_days: int = Form(3),
    weekly_limit: int = Form(1),
    send_time: str = Form("10:00"),
    required_tag: str = Form(""),
    text: str = Form(...),
    buttons_json: str = Form(""),
    photo: UploadFile | None = File(None),
    existing_photo_path: str = Form(""),
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)

    image_data = await photo.read() if photo and photo.filename else None
    photo_path = existing_photo_path.strip() or None
    if image_data:
        ext = Path(photo.filename or "").suffix or ".jpg"
        filename = f"{uuid4().hex}{ext}"
        saved = BROADCAST_UPLOAD_DIR / filename
        saved.write_bytes(image_data)
        photo_path = str(saved.resolve())

    db.upsert_step_broadcast_rule(
        scenario_ref=scenario_ref,
        delay_days=max(delay_days, 0),
        weekly_limit=max(weekly_limit, 1),
        send_time=send_time.strip() or "10:00",
        required_tag=required_tag.strip() or None,
        message_text=text.strip(),
        buttons_json=buttons_json.strip() or None,
        photo_path=photo_path,
        rule_id=rule_id,
    )
    msg = "Правило автодожима обновлено" if rule_id else "Правило автодожима сохранено"
    return RedirectResponse(f"/dashboard?msg={quote_plus(msg)}", status_code=302)


@app.post("/broadcast/rule/delete")
async def delete_step_rule(request: Request, rule_id: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.deactivate_step_broadcast_rule(rule_id)
    return RedirectResponse(f"/dashboard?msg={quote_plus('Правило автодожима удалено')}", status_code=302)


@app.post("/broadcast/rule/toggle")
async def toggle_step_rule(request: Request, rule_id: int = Form(...), is_active: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.set_step_broadcast_rule_active(rule_id, bool(is_active))
    msg = "Правило включено" if is_active else "Правило поставлено на паузу"
    return RedirectResponse(f"/dashboard?msg={quote_plus(msg)}", status_code=302)


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

                rules = db.get_step_broadcast_rules()
                for rule in rules:
                    scenario_id = db.resolve_scenario_ref(str(rule.get("scenario_ref", "")).strip())
                    if not scenario_id:
                        continue
                    due_user_ids = db.get_users_due_for_step_rule(
                        rule_id=int(rule["id"]),
                        scenario_id=scenario_id,
                        delay_days=int(rule.get("delay_days") or 0),
                        weekly_limit=int(rule.get("weekly_limit") or 1),
                        send_time=str(rule.get("send_time") or "00:00"),
                        required_tag=(rule.get("required_tag") or None),
                    )
                    for user_id in due_user_ids:
                        result = await send_broadcast(
                            text=rule["message_text"],
                            user_ids=[user_id],
                            buttons_json=rule.get("buttons_json"),
                            photo=None,
                            photo_path=rule.get("photo_path"),
                            rule_id=int(rule["id"]),
                        )
                        if result["sent"] > 0:
                            db.log_step_rule_delivery(int(rule["id"]), int(user_id))

                queued = db.get_queued_tasks(limit=20)
                for task in queued:
                    db.set_task_status(int(task["id"]), "running")
                    payload = json.loads(task.get("payload_json") or "{}")
                    if task["task_type"] == "broadcast_send":
                        result = await send_broadcast(
                            text=str(payload.get("text") or ""),
                            user_ids=[int(uid) for uid in payload.get("user_ids") or []],
                            buttons_json=payload.get("buttons_json"),
                            photo=None,
                            photo_path=payload.get("photo_path"),
                            broadcast_id=payload.get("broadcast_id"),
                        )
                        db.update_broadcast_status(
                            int(payload.get("broadcast_id")),
                            status="done",
                            sent_count=result["sent"],
                            failed_count=result["failed"],
                            error_text=None,
                        )
                        db.set_task_status(int(task["id"]), "done", message="Задача выполнена")
                    else:
                        db.set_task_status(int(task["id"]), "failed", message="Неизвестный тип задачи")
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

    return render_template(
        request=request,
        name="scenarios.html",
        context={
            "scenarios": scenarios,
            "scenario_branches": scenario_branches,
            "start_scenario": start_scenario,
            "broken_links": broken_links,
            "orphan_ids": orphan_ids,
            "flash_msg": request.query_params.get("msg"),
        },
    )


@app.post("/scenarios/save")
async def save_scenario(
    request: Request,
    scenario_id: str = Form(""),
    trigger_text: str = Form(...),
    bot_reply_text: str = Form(...),
    buttons_json: str = Form(""),
    next_step: str = Form(""),
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
        scenario_image_path = str(image_path.resolve())

    parsed_scenario_id = db.resolve_scenario_ref(scenario_id)
    parsed_next_step = db.resolve_scenario_ref(next_step)
    scenario_ref_raw = scenario_id.strip()
    next_step_raw = next_step.strip()
    if scenario_ref_raw and parsed_scenario_id is None and scenario_ref_raw.isdigit():
        parsed_scenario_id = int(scenario_ref_raw)

    safe_buttons_json = buttons_json.strip() or None
    if safe_buttons_json:
        try:
            payload = json.loads(safe_buttons_json)
            canonical_rows: list[list[dict[str, Any]]] = []
            if isinstance(payload, dict):
                payload = [[payload]]
            if isinstance(payload, list):
                for row in payload:
                    if isinstance(row, dict):
                        row = [row]
                    if not isinstance(row, list):
                        continue
                    canonical_row: list[dict[str, Any]] = []
                    for button in row:
                        if not isinstance(button, dict):
                            continue
                        canonical_row.append(button)
                    if canonical_row:
                        canonical_rows.append(canonical_row)

            for row in canonical_rows:
                for button in row:
                    step_trigger = button.get("step_trigger")
                    if step_trigger and not button.get("step_id"):
                        resolved_id = db.resolve_scenario_ref(str(step_trigger))
                        if resolved_id:
                            button["step_id"] = resolved_id
            safe_buttons_json = json.dumps(canonical_rows, ensure_ascii=False) if canonical_rows else None
        except json.JSONDecodeError:
            safe_buttons_json = None

    if next_step_raw and parsed_next_step is None and next_step_raw.isdigit():
        parsed_next_step = int(next_step_raw)

    try:
        db.upsert_scenario(
            trigger_text=trigger_text.strip(),
            bot_reply_text=bot_reply_text.strip(),
            buttons_json=safe_buttons_json,
            next_step=parsed_next_step,
            scenario_image_path=scenario_image_path,
            scenario_id=parsed_scenario_id,
        )
    except (sqlite3.Error, ValueError):
        return RedirectResponse(f"/scenarios?msg={quote_plus('Ошибка сохранения шага')}", status_code=302)
    return RedirectResponse(f"/scenarios?msg={quote_plus('Шаг сохранен')}", status_code=302)


@app.post("/scenarios/delete")
async def delete_scenario(request: Request, scenario_id: int = Form(...)):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    db.delete_scenario(scenario_id)
    return RedirectResponse(f"/scenarios?msg={quote_plus('Шаг удален')}", status_code=302)


@app.get("/users", response_class=HTMLResponse)
async def users_page(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    filter_tag = request.query_params.get("tag") or None
    filter_activity = request.query_params.get("activity") or None
    step_ref = request.query_params.get("step_ref") or None
    scenario_id = db.resolve_scenario_ref(step_ref) if step_ref else None
    users = db.get_users_filtered(tag=filter_tag, activity=filter_activity, scenario_id=scenario_id)
    stats = db.get_stats()
    return render_template(
        request=request,
        name="users.html",
        context={
            "users": users,
            **stats,
            "all_tags": db.get_all_tags(),
            "scenarios": db.get_all_scenarios(),
            "filter_tag": filter_tag or "",
            "filter_activity": filter_activity or "",
            "filter_step_ref": step_ref or "",
            "flash_msg": request.query_params.get("msg"),
        },
    )


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
    visits = db.get_user_step_visits(user_id)
    deliveries = db.get_user_delivery_logs(user_id)
    return render_template(
        request=request,
        name="user_profile.html",
        context={"user": users[0], "events": events, "visits": visits, "deliveries": deliveries, "tags": db.get_user_tags(user_id)},
    )


@app.get("/tasks", response_class=HTMLResponse)
async def tasks_page(request: Request):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    tasks = db.get_task_history()
    return render_template(request=request, name="tasks.html", context={"tasks": tasks})


@app.post("/users/tag")
async def tag_user(request: Request, user_id: int = Form(...), tag: str = Form(...), action: str = Form("add")):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    normalized = tag.strip()
    if normalized:
        if action == "remove":
            db.remove_user_tag(user_id, normalized)
        else:
            db.add_user_tag(user_id, normalized)
    return RedirectResponse("/users", status_code=302)


@app.post("/users/tags/bulk")
async def bulk_tag_users(
    request: Request,
    tag: str = Form(...),
    activity: str = Form(""),
    step_ref: str = Form(""),
):
    if not is_auth(request):
        return RedirectResponse("/", status_code=302)
    scenario_id = db.resolve_scenario_ref(step_ref) if step_ref.strip() else None
    tagged = db.add_tag_to_filtered_users(
        tag=tag.strip(),
        activity=activity.strip() or None,
        scenario_id=scenario_id,
    )
    return RedirectResponse(f"/users?msg={quote_plus(f'Присвоено тегов: {tagged}')}", status_code=302)


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

    filter_tag = request.query_params.get("tag") or None
    filter_activity = request.query_params.get("activity") or None
    step_ref = request.query_params.get("step_ref") or None
    scenario_id = db.resolve_scenario_ref(step_ref) if step_ref else None
    users = db.get_users_filtered(tag=filter_tag, activity=filter_activity, scenario_id=scenario_id)
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
