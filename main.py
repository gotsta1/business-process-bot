import asyncio
import logging
import os
from datetime import datetime, time, timedelta
from typing import Dict, Optional

import httpx
from dotenv import load_dotenv

from db import (
    get_all_processes,
    get_all_users,
    get_connection,
    get_processes_for_owner,
    get_user,
    init_db,
    record_reminder_sent,
    register_user,
    seed_default_processes,
)

# Load variables from .env if present
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("business-bot")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_TOKEN:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN environment variable.")

TELEGRAM_API = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}"

# Single global connection is OK for this simple demo app.
CONN = get_connection()
init_db(CONN)
seed_default_processes(CONN)

REMINDER_MINUTES = [120, 60]  # 2ч, 1ч до дедлайна


async def send_message(chat_id: int, text: str) -> None:
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{TELEGRAM_API}/sendMessage",
                json={"chat_id": chat_id, "text": text},
                timeout=10,
            )
    except Exception as exc:
        logger.error("Failed to send Telegram message: %s", exc)


def parse_check_datetime(text: str) -> Optional[datetime]:
    parts = text.strip().split(maxsplit=1)
    if len(parts) < 2:
        return None
    raw = parts[1]
    normalized = (
        raw.replace(".", "-").replace("/", "-").replace(",", " ").replace("T", " ")
    )
    try:
        date_str, time_str = normalized.split()
        dt = datetime.strptime(f"{date_str} {time_str}", "%d-%m-%Y %H:%M")
        return dt
    except Exception:
        return None


def format_process_list(owner_name: str) -> str:
    processes = get_processes_for_owner(CONN, owner_name)
    if not processes:
        return "За вами пока не закреплены процессы."

    reminders_text = ", ".join([f"за {m} мин" for m in REMINDER_MINUTES])
    lines = [
        f"Ваши процессы ({owner_name}):",
        f"Напоминания: {reminders_text}",
    ]
    for p in processes:
        lines.append(
            f"• {p['name']} — {p['periodicity']}, дедлайн {p['deadline_time']}."
        )
    return "\n".join(lines)


def _deadline_datetime(reference: datetime, hhmm: str) -> datetime:
    h, m = hhmm.split(":")
    deadline_time = time(int(h), int(m))
    return datetime.combine(reference.date(), deadline_time)


def humanize_delta(delta: timedelta) -> str:
    minutes = int(delta.total_seconds() // 60)
    if minutes < 0:
        minutes = abs(minutes)
        return f"просрочено на {minutes} мин"
    if minutes < 60:
        return f"через {minutes} мин"
    hours, mins = divmod(minutes, 60)
    return f"через {hours} ч {mins} мин"


def build_check_response(now: datetime, owner_name: str) -> str:
    processes = get_processes_for_owner(CONN, owner_name)
    if not processes:
        return "Нет процессов для расчета."

    lines = [f"Проверка на {now.strftime('%d-%m-%Y %H:%M')}:"]
    for p in processes:
        deadline_dt = _deadline_datetime(now, p["deadline_time"])
        delta = deadline_dt - now
        if delta.total_seconds() >= 0:
            # humanize_delta уже формирует "через X"; уберем лишнее слово.
            status = f"✅ успеваете, осталось {humanize_delta(delta).replace('через ', '')}"
        else:
            status = f"⚠️ дедлайн прошел, просрочено на {humanize_delta(delta)}"
        lines.append(f"• {p['name']} — дедлайн {p['deadline_time']} — {status}")
    return "\n".join(lines)


def build_help(is_registered: bool) -> str:
    base = [
        "Команды:",
        "/start — регистрация (введите имя, как в процессах).",
        "/my — ваши процессы.",
        "/check DD-MM-YYYY HH:MM — проверка дедлайнов и напоминаний.",
    ]
    if not is_registered:
        base.append("Сначала отправьте ваше имя для регистрации.")
    return "\n".join(base)


async def handle_message(message: Dict) -> None:
    chat_id = message["chat"]["id"]
    user = message["from"]
    text = message.get("text", "").strip()
    tg_id = user["id"]
    tg_username = user.get("username")
    registered = get_user(CONN, tg_id)

    # Registration flow: after /start ask name; next plain text registers.
    if text.startswith("/start"):
        await send_message(
            chat_id,
            "Привет! Напиши своё имя так, как оно указано в бизнес-процессах (например: Кирилл).",
        )
        return

    # If not registered yet, treat any non-command text as the name.
    if not registered and text and not text.startswith("/"):
        register_user(CONN, tg_id, text, tg_username)
        await send_message(
            chat_id,
            f"Записал: {text}. Теперь доступна команда /my и /check DD-MM-YYYY HH:MM",
        )
        return

    # Help for non-registered command usage.
    if not registered:
        await send_message(chat_id, build_help(False))
        return

    owner_name = registered["name"]

    if text.startswith("/my"):
        await send_message(chat_id, format_process_list(owner_name))
        return

    if text.startswith("/check"):
        check_dt = parse_check_datetime(text)
        if not check_dt:
            await send_message(
                chat_id, "Используйте формат: /check 15-12-2025 09:00"
            )
            return
        await send_message(chat_id, build_check_response(check_dt, owner_name))
        return

    await send_message(chat_id, build_help(True))


async def polling_loop() -> None:
    offset: Optional[int] = None
    logger.info("Starting long polling...")
    async with httpx.AsyncClient(timeout=35) as client:
        while True:
            try:
                resp = await client.get(
                    f"{TELEGRAM_API}/getUpdates",
                    params={"timeout": 25, "offset": offset},
                )
                data = resp.json()
                if not data.get("ok"):
                    logger.error("getUpdates error: %s", data)
                    await asyncio.sleep(2)
                    continue

                for update in data.get("result", []):
                    offset = update["update_id"] + 1
                    message = update.get("message") or update.get("edited_message")
                    if not message:
                        continue
                    logger.info("Update received: %s", update)
                    await handle_message(message)
            except Exception as exc:
                logger.error("Polling error: %s", exc)
                await asyncio.sleep(3)


async def reminders_loop(poll_seconds: int = 60) -> None:
    """
    Periodically checks reminders for all registered users and sends messages when due.
    Uses local server time.
    """
    while True:
        now = datetime.now()
        try:
            users = get_all_users(CONN)
            for user in users:
                processes = get_processes_for_owner(CONN, user["name"])
                if not processes:
                    continue

                for p in processes:
                    target_date = now.date()
                    deadline_dt = _deadline_datetime(
                        datetime.combine(target_date, time()), p["deadline_time"]
                    )

                    # Не шлем после дедлайна.
                    if now >= deadline_dt:
                        continue

                    for idx, minutes_before in enumerate(REMINDER_MINUTES, start=1):
                        reminder_time = deadline_dt - timedelta(minutes=minutes_before)
                        if now >= reminder_time:
                            inserted = record_reminder_sent(
                                CONN,
                                user["id"],
                                p["id"],
                                target_date.isoformat(),
                                idx,
                            )
                            if inserted:
                                to_deadline = deadline_dt - now
                                text = (
                                    f"Напоминание: {p['name']} (дедлайн {p['deadline_time']}, {p['periodicity']}). "
                                    f"Осталось {humanize_delta(to_deadline)}."
                                )
                                await send_message(user["telegram_id"], text)
        except Exception as exc:
            logger.error("Reminder loop error: %s", exc)

        await asyncio.sleep(poll_seconds)


if __name__ == "__main__":
    async def main():
        await asyncio.gather(polling_loop(), reminders_loop())

    asyncio.run(main())
