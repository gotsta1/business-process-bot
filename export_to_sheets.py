import os
import sys
from typing import List

import gspread
from dotenv import load_dotenv

from db import get_all_processes, get_connection


load_dotenv()


def export():
    credentials_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not credentials_path or not sheet_id:
        raise RuntimeError(
            "Set GOOGLE_SERVICE_ACCOUNT_JSON (path to service account json) "
            "and GOOGLE_SHEET_ID (spreadsheet id)."
        )

    conn = get_connection()
    processes = get_all_processes(conn)

    gc = gspread.service_account(filename=credentials_path)
    sh = gc.open_by_key(sheet_id)

    try:
        ws = sh.worksheet("Processes")
        ws.clear()
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title="Processes", rows="50", cols="6")

    rows: List[List[str]] = [
        [
            "Название",
            "Ответственный",
            "Периодичность",
            "Дедлайн",
            "Напоминание 1 (мин)",
            "Напоминание 2 (мин)",
        ]
    ]
    for p in processes:
        rows.append(
            [
                p["name"],
                p["owner_name"],
                p["periodicity"],
                p["deadline_time"],
                str(p["reminder_minutes_before_1"] or ""),
                str(p["reminder_minutes_before_2"] or ""),
            ]
        )

    ws.update(rows)
    print("Exported", len(processes), "rows to worksheet Processes.")


if __name__ == "__main__":
    try:
        export()
    except Exception as exc:
        print("Export failed:", exc, file=sys.stderr)
        sys.exit(1)
