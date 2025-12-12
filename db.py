import sqlite3
from pathlib import Path
from typing import Iterable, List, Optional

DB_PATH = Path(__file__).parent / "bot.db"

# Predefined processes to seed the database on first run.
DEFAULT_PROCESSES: List[dict] = [
    {
        "name": "Заполнить таблицу показателей",
        "owner_name": "Кирилл",
        "periodicity": "ежедневно (конец дня)",
        "deadline_time": "23:59",
        "reminders": (24 * 60, 2 * 60),
    },
    {
        "name": "Посмотреть просмотры конкурентов",
        "owner_name": "Кирилл",
        "periodicity": "ежедневно (конец дня)",
        "deadline_time": "23:59",
        "reminders": (24 * 60, 2 * 60),
    },
    {
        "name": "Заполнить КОПы",
        "owner_name": "Иван",
        "periodicity": "ежедневно до 10:30",
        "deadline_time": "10:30",
        "reminders": (12 * 60, 2 * 60),
    },
    {
        "name": "Проверить рекламные кампании",
        "owner_name": "Иван",
        "periodicity": "ежедневно до 12:00",
        "deadline_time": "12:00",
        "reminders": (12 * 60, 2 * 60),
    },
]


def get_connection() -> sqlite3.Connection:
    # check_same_thread=False allows reuse from FastAPI event loop threads.
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            tg_username TEXT,
            name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS processes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            owner_name TEXT NOT NULL,
            periodicity TEXT NOT NULL,
            deadline_time TEXT NOT NULL,
            reminder_minutes_before_1 INTEGER,
            reminder_minutes_before_2 INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS reminder_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            process_id INTEGER NOT NULL,
            deadline_date TEXT NOT NULL,
            reminder_idx INTEGER NOT NULL,
            sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(user_id, process_id, deadline_date, reminder_idx)
        );
        """
    )
    conn.commit()


def seed_default_processes(conn: sqlite3.Connection) -> None:
    """Populate base business processes if table is empty."""
    count = conn.execute("SELECT COUNT(*) FROM processes;").fetchone()[0]
    if count:
        return

    conn.executemany(
        """
        INSERT INTO processes (
            name,
            owner_name,
            periodicity,
            deadline_time,
            reminder_minutes_before_1,
            reminder_minutes_before_2
        ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            (
                item["name"],
                item["owner_name"],
                item["periodicity"],
                item["deadline_time"],
                item["reminders"][0] if item["reminders"] else None,
                item["reminders"][1] if item["reminders"] else None,
            )
            for item in DEFAULT_PROCESSES
        ),
    )
    conn.commit()


def get_user(conn: sqlite3.Connection, telegram_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM users WHERE telegram_id = ?;", (telegram_id,)
    ).fetchone()


def register_user(
    conn: sqlite3.Connection, telegram_id: int, name: str, username: Optional[str]
) -> None:
    conn.execute(
        """
        INSERT INTO users (telegram_id, tg_username, name)
        VALUES (?, ?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET
            name=excluded.name,
            tg_username=excluded.tg_username,
            updated_at=CURRENT_TIMESTAMP;
        """,
        (telegram_id, username, name),
    )
    conn.commit()


def get_processes_for_owner(
    conn: sqlite3.Connection, owner_name: str
) -> List[sqlite3.Row]:
    cur = conn.execute(
        "SELECT * FROM processes WHERE owner_name = ? ORDER BY id;", (owner_name,)
    )
    return cur.fetchall()


def get_all_processes(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    cur = conn.execute("SELECT * FROM processes ORDER BY owner_name, id;")
    return cur.fetchall()


def get_all_users(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    cur = conn.execute("SELECT * FROM users ORDER BY id;")
    return cur.fetchall()


def record_reminder_sent(
    conn: sqlite3.Connection,
    user_id: int,
    process_id: int,
    deadline_date: str,
    reminder_idx: int,
) -> bool:
    """
    Persist the fact we sent this reminder. Returns True if inserted, False if already present.
    """
    cur = conn.execute(
        """
        INSERT INTO reminder_logs (user_id, process_id, deadline_date, reminder_idx)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id, process_id, deadline_date, reminder_idx) DO NOTHING;
        """,
        (user_id, process_id, deadline_date, reminder_idx),
    )
    conn.commit()
    return cur.rowcount > 0


def any_reminder_sent(
    conn: sqlite3.Connection, user_id: int, process_id: int, deadline_date: str
) -> bool:
    cur = conn.execute(
        """
        SELECT 1 FROM reminder_logs
        WHERE user_id = ? AND process_id = ? AND deadline_date = ?
        LIMIT 1;
        """,
        (user_id, process_id, deadline_date),
    )
    return cur.fetchone() is not None


def upsert_processes(conn: sqlite3.Connection, processes: Iterable[dict]) -> None:
    """Allow adding extra processes beyond the defaults."""
    conn.executemany(
        """
        INSERT INTO processes (
            name,
            owner_name,
            periodicity,
            deadline_time,
            reminder_minutes_before_1,
            reminder_minutes_before_2
        ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        (
            (
                item["name"],
                item["owner_name"],
                item["periodicity"],
                item["deadline_time"],
                item.get("reminders", (None, None))[0],
                item.get("reminders", (None, None))[1],
            )
            for item in processes
        ),
    )
    conn.commit()
