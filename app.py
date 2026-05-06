import csv
import io
import os
import re
import secrets
import sqlite3
import string
from functools import wraps
from pathlib import Path
from typing import Iterable, Optional

from flask import (
    Flask,
    Response,
    abort,
    flash,
    redirect,
    render_template,
    request,
    session,
    url_for,
)
from werkzeug.security import check_password_hash, generate_password_hash

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("DB_PATH", BASE_DIR / "cdk_center.db"))

CODE_STATUSES = {"created", "distributed", "redeemed", "disabled"}
TASK_STATUSES = {"pending", "assigned", "processing", "success", "failed"}
ACTIVE_TASK_STATUSES = {"pending", "assigned", "processing"}
USER_ROLES = {"owner", "lead", "staff"}

PUBLIC_STATUS_TEXT = {
    "created": "兑换码尚未上架。",
    "distributed": "待提交。",
    "pending": "排队中。",
    "assigned": "排队中。",
    "processing": "排队中。",
    "success": "充值成功。",
    "failed": "请检查Token后重新提交。",
    "redeemed": "充值成功，兑换码已核销。",
    "disabled": "兑换码不可用，请联系购买渠道管理员。",
}

CODE_STATUS_TEXT = {
    "created": "刚生成 / 未上架",
    "distributed": "可兑换",
    "redeemed": "已核销",
    "disabled": "已禁用",
}

TASK_STATUS_TEXT = {
    "pending": "Pending",
    "assigned": "Assigned",
    "processing": "Processing",
    "success": "Success",
    "failed": "Failed",
}

FAIL_REASONS = [
    "Invalid token",
    "Expired token",
    "Account already subscribed",
    "Supplier failed",
    "Other",
]

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

OWNER_USERNAME = os.environ.get("OWNER_USERNAME", "owner")
OWNER_PASSWORD = os.environ.get("OWNER_PASSWORD") or os.environ.get("ADMIN_PASSWORD", "admin123")

# User-facing guide buttons shown on /redeem.
# Replace these with your real fixed websites, or set them via environment variables.
STEP2_LOGIN_URL = os.environ.get("STEP2_LOGIN_URL", "https://example.com/login")
STEP2_COPY_URL = os.environ.get("STEP2_COPY_URL", "https://example.com/copy")
STEP2_LOGIN_LABEL = os.environ.get("STEP2_LOGIN_LABEL", "打开网页 1")
STEP2_COPY_LABEL = os.environ.get("STEP2_COPY_LABEL", "打开网页 2")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, definition: str) -> None:
    if column_name not in table_columns(conn, table_name):
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}")


def create_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS codes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            proxy_code TEXT UNIQUE NOT NULL,
            partner TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'created',
            partner_price REAL DEFAULT 0,
            supplier_cost REAL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            redeemed_at TEXT,
            redeemed_task_id INTEGER,
            remark TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS redemption_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code_id INTEGER NOT NULL,
            proxy_code TEXT NOT NULL,
            token TEXT NOT NULL,
            contact TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            assigned_to INTEGER,
            assigned_by INTEGER,
            assigned_at TEXT,
            started_at TEXT,
            completed_by INTEGER,
            completed_at TEXT,
            fail_reason TEXT,
            worker_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(code_id) REFERENCES codes(id),
            FOREIGN KEY(assigned_to) REFERENCES users(id),
            FOREIGN KEY(assigned_by) REFERENCES users(id),
            FOREIGN KEY(completed_by) REFERENCES users(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            role TEXT NOT NULL,
            display_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            deleted_at TEXT,
            created_by INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_login_at TEXT,
            FOREIGN KEY(created_by) REFERENCES users(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operation_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            actor_role TEXT,
            action TEXT NOT NULL,
            task_id INTEGER,
            code_id INTEGER,
            old_value TEXT,
            new_value TEXT,
            note TEXT,
            ip_address TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(actor_id) REFERENCES users(id),
            FOREIGN KEY(task_id) REFERENCES redemption_tasks(id),
            FOREIGN KEY(code_id) REFERENCES codes(id)
        )
        """
    )

    # Existing V1 databases already have a codes table. Add V2 columns without dropping data.
    add_column_if_missing(conn, "codes", "redeemed_at", "TEXT")
    add_column_if_missing(conn, "codes", "redeemed_task_id", "INTEGER")
    add_column_if_missing(conn, "codes", "remark", "TEXT")
    add_column_if_missing(conn, "users", "is_deleted", "INTEGER NOT NULL DEFAULT 0")
    add_column_if_missing(conn, "users", "deleted_at", "TEXT")

    conn.execute("CREATE INDEX IF NOT EXISTS idx_codes_status ON codes(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_code_id ON redemption_tasks(code_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON redemption_tasks(status, created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_assigned_to ON redemption_tasks(assigned_to)")


def create_initial_owner(conn: sqlite3.Connection) -> None:
    owner_exists = conn.execute("SELECT 1 FROM users WHERE role = 'owner' AND COALESCE(is_deleted, 0) = 0 LIMIT 1").fetchone()
    if not owner_exists:
        conn.execute(
            """
            INSERT INTO users (username, password_hash, role, display_name, is_active)
            VALUES (?, ?, 'owner', 'Owner', 1)
            """,
            (OWNER_USERNAME, generate_password_hash(OWNER_PASSWORD)),
        )


def migrate_legacy_rows(conn: sqlite3.Connection) -> None:
    """Move old single-table submitted/success/failed rows into redemption_tasks.

    V1 stored token and task status directly inside codes. V2 keeps codes as CDK inventory
    and stores every user submission as one task. This migration is intentionally conservative:
    it only creates one task for old rows that have a token and do not yet have any tasks.
    """
    columns = table_columns(conn, "codes")
    if "student_id" not in columns:
        return

    old_rows = conn.execute(
        """
        SELECT * FROM codes
        WHERE COALESCE(student_id, '') != ''
          AND status IN ('submitted', 'processing', 'success', 'failed', 'refunded')
        ORDER BY id ASC
        """
    ).fetchall()

    for row in old_rows:
        task_exists = conn.execute(
            "SELECT 1 FROM redemption_tasks WHERE code_id = ? LIMIT 1", (row["id"],)
        ).fetchone()
        if task_exists:
            continue

        old_status = row["status"]
        task_status = {
            "submitted": "pending",
            "processing": "processing",
            "success": "success",
            "failed": "failed",
            "refunded": "failed",
        }.get(old_status, "pending")

        completed_at = None
        if task_status in {"success", "failed"} and "processed_at" in columns:
            completed_at = row["processed_at"]

        cur = conn.execute(
            """
            INSERT INTO redemption_tasks (
                code_id, proxy_code, token, contact, status,
                completed_at, fail_reason, worker_note, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, CURRENT_TIMESTAMP))
            """,
            (
                row["id"],
                row["proxy_code"],
                row["student_id"],
                row["contact"] if "contact" in columns else "",
                task_status,
                completed_at,
                "Legacy failed/refunded" if task_status == "failed" else None,
                row["remark"] if "remark" in columns else None,
                row["submitted_at"] if "submitted_at" in columns else None,
            ),
        )
        if task_status == "success":
            conn.execute(
                "UPDATE codes SET redeemed_task_id = COALESCE(redeemed_task_id, ?) WHERE id = ?",
                (cur.lastrowid, row["id"]),
            )

    # Convert V1 code statuses into V2 inventory statuses.
    conn.execute("UPDATE codes SET status = 'redeemed', redeemed_at = COALESCE(redeemed_at, processed_at) WHERE status = 'success'")
    conn.execute("UPDATE codes SET status = 'distributed' WHERE status IN ('submitted', 'processing', 'failed')")
    conn.execute("UPDATE codes SET status = 'disabled' WHERE status = 'refunded'")


def init_db() -> None:
    with get_db() as conn:
        create_tables(conn)
        create_initial_owner(conn)
        migrate_legacy_rows(conn)
        conn.commit()


@app.before_request
def ensure_db_and_csrf() -> None:
    init_db()
    if request.method == "POST":
        session_token = session.get("_csrf_token")
        form_token = request.form.get("_csrf_token", "")
        if not session_token or not secrets.compare_digest(session_token, form_token):
            abort(400, "Invalid CSRF token")


def csrf_token() -> str:
    token = session.get("_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["_csrf_token"] = token
    return token


def format_queue_age(minutes: Optional[int], *, lang: str = "en") -> str:
    try:
        value = max(0, int(minutes or 0))
    except (TypeError, ValueError):
        value = 0

    if lang == "zh":
        if value < 1:
            return "刚刚"
        if value < 60:
            return f"{value} 分钟"
        hours, mins = divmod(value, 60)
        if mins == 0:
            return f"{hours} 小时"
        return f"{hours} 小时 {mins} 分钟"

    if value < 1:
        return "just now"
    if value < 60:
        unit = "minute" if value == 1 else "minutes"
        return f"{value} {unit}"
    hours, mins = divmod(value, 60)
    hour_unit = "hour" if hours == 1 else "hours"
    if mins == 0:
        return f"{hours} {hour_unit}"
    minute_unit = "minute" if mins == 1 else "minutes"
    return f"{hours} {hour_unit} {mins} {minute_unit}"


@app.context_processor
def inject_globals():
    return {
        "csrf_token": csrf_token,
        "current_user": current_user(),
        "code_status_text": CODE_STATUS_TEXT,
        "task_status_text": TASK_STATUS_TEXT,
        "fail_reasons": FAIL_REASONS,
        "step2_login_url": STEP2_LOGIN_URL,
        "step2_copy_url": STEP2_COPY_URL,
        "step2_login_label": STEP2_LOGIN_LABEL,
        "step2_copy_label": STEP2_COPY_LABEL,
        "format_queue_age": format_queue_age,
    }


def normalize_code(code: str) -> str:
    return "".join(code.strip().upper().split())


def parse_status_query_codes(raw_text: str) -> tuple[list[str], int]:
    """Parse up to 1000 CDKs separated by commas, Chinese commas, spaces, tabs, or new lines.

    Returns a de-duplicated list that preserves input order, plus the raw parsed count
    before de-duplication so the UI can enforce the advertised 1000-code limit.
    """
    raw_items = [item for item in re.split(r"[\s,，]+", raw_text or "") if item.strip()]
    codes: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        code = normalize_code(item)
        if code and code not in seen:
            seen.add(code)
            codes.append(code)
    return codes, len(raw_items)


def chunked(items: list, size: int = 400):
    for start in range(0, len(items), size):
        yield items[start : start + size]


def public_status_message(code_row: sqlite3.Row, latest_task: Optional[sqlite3.Row]) -> str:
    if code_row["status"] == "redeemed":
        return PUBLIC_STATUS_TEXT["redeemed"]
    if code_row["status"] in {"created", "disabled"}:
        return PUBLIC_STATUS_TEXT[code_row["status"]]
    if latest_task:
        return PUBLIC_STATUS_TEXT.get(latest_task["status"], "状态未知。")
    return PUBLIC_STATUS_TEXT["distributed"]


def batch_public_status_results(conn: sqlite3.Connection, codes: list[str]) -> list[dict]:
    if not codes:
        return []

    code_map = {}
    for group in chunked(codes):
        placeholders = ",".join("?" for _ in group)
        rows = conn.execute(f"SELECT * FROM codes WHERE proxy_code IN ({placeholders})", group).fetchall()
        code_map.update({row["proxy_code"]: row for row in rows})

    code_ids = [row["id"] for row in code_map.values()]
    latest_task_map = {}
    for group in chunked(code_ids):
        placeholders = ",".join("?" for _ in group)
        rows = conn.execute(
            f"""
            SELECT t.*
            FROM redemption_tasks t
            JOIN (
                SELECT code_id, MAX(id) AS latest_id
                FROM redemption_tasks
                WHERE code_id IN ({placeholders})
                GROUP BY code_id
            ) latest ON latest.latest_id = t.id
            """,
            group,
        ).fetchall()
        latest_task_map.update({row["code_id"]: row for row in rows})

    results = []
    for code in codes:
        code_row = code_map.get(code)
        if not code_row:
            results.append({
                "proxy_code": code,
                "code_status": "-",
                "task_status": "-",
                "message": "兑换码无效。",
                "type": "error",
            })
            continue

        latest_task = latest_task_map.get(code_row["id"])
        results.append({
            "proxy_code": code_row["proxy_code"],
            "code_status": code_row["status"],
            "task_status": latest_task["status"] if latest_task else "",
            "message": public_status_message(code_row, latest_task),
            "type": "success" if code_row["status"] == "redeemed" or (latest_task and latest_task["status"] == "success") else "info",
        })
    return results


def make_code(prefix: str = "CNVIP") -> str:
    alphabet = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(alphabet) for _ in range(4))
    part2 = "".join(secrets.choice(alphabet) for _ in range(4))
    return f"{prefix}-{part1}-{part2}"


def generate_unique_code(conn: sqlite3.Connection, prefix: str = "CNVIP") -> str:
    for _ in range(50):
        code = make_code(prefix)
        exists = conn.execute("SELECT 1 FROM codes WHERE proxy_code = ?", (code,)).fetchone()
        if not exists:
            return code
    raise RuntimeError("生成兑换码失败，请重试")


def current_user() -> Optional[sqlite3.Row]:
    user_id = session.get("user_id")
    if not user_id:
        return None
    with get_db() as conn:
        return conn.execute("SELECT * FROM users WHERE id = ? AND is_active = 1 AND COALESCE(is_deleted, 0) = 0", (user_id,)).fetchone()


def login_user(user: sqlite3.Row) -> None:
    session.clear()
    session["user_id"] = user["id"]
    session["role"] = user["role"]
    session["display_name"] = user["display_name"]
    csrf_token()
    with get_db() as conn:
        conn.execute("UPDATE users SET last_login_at = CURRENT_TIMESTAMP WHERE id = ?", (user["id"],))
        conn.commit()


def role_required(*roles: str):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            user = current_user()
            if not user:
                if "owner" in roles and len(roles) == 1:
                    return redirect(url_for("owner_login", next=request.path))
                return redirect(url_for("ops_login", next=request.path))
            if user["role"] not in roles:
                abort(403)
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def owner_required(fn):
    return role_required("owner")(fn)


def ops_required(fn):
    return role_required("owner", "lead", "staff")(fn)


def lead_or_owner_required(fn):
    return role_required("owner", "lead")(fn)


def write_log(
    conn: sqlite3.Connection,
    action: str,
    *,
    task_id: Optional[int] = None,
    code_id: Optional[int] = None,
    old_value: Optional[str] = None,
    new_value: Optional[str] = None,
    note: Optional[str] = None,
) -> None:
    user_id = session.get("user_id")
    role = session.get("role")
    conn.execute(
        """
        INSERT INTO operation_logs (
            actor_id, actor_role, action, task_id, code_id,
            old_value, new_value, note, ip_address
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (user_id, role, action, task_id, code_id, old_value, new_value, note, request.remote_addr),
    )


def authenticate(username: str, password: str) -> Optional[sqlite3.Row]:
    with get_db() as conn:
        user = conn.execute("SELECT * FROM users WHERE username = ? AND is_active = 1 AND COALESCE(is_deleted, 0) = 0", (username,)).fetchone()
    if user and check_password_hash(user["password_hash"], password):
        return user
    return None


def latest_task_for_code(conn: sqlite3.Connection, code_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM redemption_tasks WHERE code_id = ? ORDER BY id DESC LIMIT 1", (code_id,)
    ).fetchone()


def active_task_for_code(conn: sqlite3.Connection, code_id: int) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM redemption_tasks
        WHERE code_id = ? AND status IN ('pending', 'assigned', 'processing')
        ORDER BY id DESC LIMIT 1
        """,
        (code_id,),
    ).fetchone()


def task_query(where_sql: str = "", params: Iterable = (), limit: int = 500) -> list[sqlite3.Row]:
    sql = """
        SELECT
            t.*,
            c.status AS code_status,
            assigned.display_name AS assigned_name,
            assigned.username AS assigned_username,
            completed.display_name AS completed_name,
            MAX(0, CAST((julianday('now') - julianday(t.created_at)) * 24 * 60 AS INTEGER)) AS queue_minutes
        FROM redemption_tasks t
        JOIN codes c ON c.id = t.code_id
        LEFT JOIN users assigned ON assigned.id = t.assigned_to
        LEFT JOIN users completed ON completed.id = t.completed_by
    """
    if where_sql:
        sql += " WHERE " + where_sql
    sql += """
        ORDER BY
            CASE t.status
                WHEN 'pending' THEN 0
                WHEN 'assigned' THEN 1
                WHEN 'processing' THEN 2
                WHEN 'failed' THEN 3
                WHEN 'success' THEN 4
                ELSE 5
            END,
            CASE WHEN t.status IN ('pending', 'assigned', 'processing') THEN t.created_at END ASC,
            CASE WHEN t.status IN ('success', 'failed') THEN t.completed_at END DESC,
            t.id ASC
        LIMIT ?
    """
    with get_db() as conn:
        return conn.execute(sql, (*params, limit)).fetchall()


def visible_task_counts(conn: sqlite3.Connection, user: sqlite3.Row) -> dict[str, int]:
    params: list = []
    active_where = "status IN ('pending', 'assigned', 'processing')"
    today_done_where = "DATE(completed_at) = DATE('now')"
    if user["role"] == "staff":
        active_where += " AND assigned_to = ?"
        today_done_where += " AND completed_by = ?"
        params.append(user["id"])

    counts = {
        "pending": 0,
        "assigned": 0,
        "processing": 0,
        "active": 0,
        "success_today": 0,
        "failed_today": 0,
        "completed_today": 0,
    }
    active_rows = conn.execute(
        f"SELECT status, COUNT(*) AS count FROM redemption_tasks WHERE {active_where} GROUP BY status",
        params,
    ).fetchall()
    for row in active_rows:
        counts[row["status"]] = row["count"]
        counts["active"] += row["count"]

    done_params = params[:]
    done_rows = conn.execute(
        f"""
        SELECT status, COUNT(*) AS count
        FROM redemption_tasks
        WHERE status IN ('success', 'failed') AND {today_done_where}
        GROUP BY status
        """,
        done_params,
    ).fetchall()
    for row in done_rows:
        key = "success_today" if row["status"] == "success" else "failed_today"
        counts[key] = row["count"]
        counts["completed_today"] += row["count"]
    return counts


def staff_workload_rows(conn: sqlite3.Connection, viewer: sqlite3.Row) -> list[dict]:
    if viewer["role"] == "lead":
        users = conn.execute(
            "SELECT * FROM users WHERE role = 'staff' AND COALESCE(is_deleted, 0) = 0 ORDER BY is_active DESC, display_name COLLATE NOCASE"
        ).fetchall()
    else:
        users = conn.execute(
            "SELECT * FROM users WHERE role IN ('lead', 'staff') AND COALESCE(is_deleted, 0) = 0 ORDER BY role, is_active DESC, display_name COLLATE NOCASE"
        ).fetchall()

    rows = []
    for u in users:
        active = conn.execute(
            """
            SELECT status, COUNT(*) AS count
            FROM redemption_tasks
            WHERE assigned_to = ? AND status IN ('pending', 'assigned', 'processing')
            GROUP BY status
            """,
            (u["id"],),
        ).fetchall()
        active_counts = {row["status"]: row["count"] for row in active}
        success_today = conn.execute(
            """
            SELECT COUNT(*) AS count FROM redemption_tasks
            WHERE completed_by = ? AND status = 'success' AND DATE(completed_at) = DATE('now')
            """,
            (u["id"],),
        ).fetchone()["count"]
        failed_today = conn.execute(
            """
            SELECT COUNT(*) AS count FROM redemption_tasks
            WHERE completed_by = ? AND status = 'failed' AND DATE(completed_at) = DATE('now')
            """,
            (u["id"],),
        ).fetchone()["count"]
        rows.append({
            "user": u,
            "current_queue": sum(active_counts.values()),
            "waiting_queue": active_counts.get("pending", 0) + active_counts.get("assigned", 0),
            "processing": active_counts.get("processing", 0),
            "success_today": success_today,
            "failed_today": failed_today,
            "completed_today": success_today + failed_today,
        })
    return rows


def member_workload_counts(conn: sqlite3.Connection, member_id: int) -> dict[str, int]:
    active = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM redemption_tasks
        WHERE assigned_to = ? AND status IN ('pending', 'assigned', 'processing')
        GROUP BY status
        """,
        (member_id,),
    ).fetchall()
    counts = {"pending": 0, "assigned": 0, "processing": 0, "current_queue": 0}
    for row in active:
        counts[row["status"]] = row["count"]
        counts["current_queue"] += row["count"]

    done = conn.execute(
        """
        SELECT status, COUNT(*) AS count
        FROM redemption_tasks
        WHERE completed_by = ? AND status IN ('success', 'failed') AND DATE(completed_at) = DATE('now')
        GROUP BY status
        """,
        (member_id,),
    ).fetchall()
    counts["success_today"] = 0
    counts["failed_today"] = 0
    for row in done:
        if row["status"] == "success":
            counts["success_today"] = row["count"]
        elif row["status"] == "failed":
            counts["failed_today"] = row["count"]
    counts["completed_today"] = counts["success_today"] + counts["failed_today"]
    return counts


def active_staff_users(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT * FROM users
        WHERE is_active = 1 AND COALESCE(is_deleted, 0) = 0 AND role IN ('lead', 'staff')
        ORDER BY role, display_name COLLATE NOCASE
        """
    ).fetchall()


def can_operate_task(user: sqlite3.Row, task: sqlite3.Row) -> bool:
    if user["role"] in {"owner", "lead"}:
        return True
    return task["assigned_to"] == user["id"]


@app.route("/")
def index():
    return redirect(url_for("redeem"))


@app.route("/redeem", methods=["GET", "POST"])
def redeem():
    result = None
    code_row = None
    latest_task = None
    status_results = []
    status_query_input = ""

    if request.method == "POST":
        action = request.form.get("action", "redeem")

        if action == "check":
            status_query_input = request.form.get("proxy_codes", "") or request.form.get("proxy_code", "")
            query_codes, raw_count = parse_status_query_codes(status_query_input)
            if not query_codes:
                result = {"type": "error", "message": "请输入至少一个兑换码。"}
            elif raw_count > 1000:
                result = {"type": "error", "message": "一次最多查询 1000 个兑换码，请减少数量后重试。"}
            else:
                with get_db() as conn:
                    status_results = batch_public_status_results(conn, query_codes)
                result = {
                    "type": "info",
                    "message": f"已查询 {len(status_results)} 个兑换码。"
                    + (f" 输入中有重复项，已自动去重。" if raw_count != len(query_codes) else ""),
                }
        else:
            proxy_code = normalize_code(request.form.get("proxy_code", ""))

            if not proxy_code:
                result = {"type": "error", "message": "请输入兑换码。"}
            else:
                with get_db() as conn:
                    code_row = conn.execute("SELECT * FROM codes WHERE proxy_code = ?", (proxy_code,)).fetchone()
                    if not code_row:
                        result = {"type": "error", "message": "兑换码无效。"}
                    else:
                        token = request.form.get("token", "").strip() or request.form.get("student_id", "").strip()
                        contact = request.form.get("contact", "").strip()

                        if code_row["status"] == "redeemed":
                            result = {"type": "info", "message": PUBLIC_STATUS_TEXT["redeemed"]}
                        elif code_row["status"] != "distributed":
                            result = {
                                "type": "info",
                                "message": PUBLIC_STATUS_TEXT.get(code_row["status"], "兑换码暂不可用。"),
                            }
                        elif not token:
                            result = {"type": "error", "message": "请输入Token。"}
                        else:
                            active_task = active_task_for_code(conn, code_row["id"])
                            if active_task:
                                latest_task = active_task
                                result = {
                                    "type": "info",
                                    "message": "当前兑换码已有未完成订单，正在排队中，请勿重复提交。",
                                }
                            else:
                                conn.execute(
                                    """
                                    INSERT INTO redemption_tasks (code_id, proxy_code, token, contact, status)
                                    VALUES (?, ?, ?, ?, 'pending')
                                    """,
                                    (code_row["id"], code_row["proxy_code"], token, contact),
                                )
                                conn.commit()
                                latest_task = latest_task_for_code(conn, code_row["id"])
                                result = {
                                    "type": "success",
                                    "message": "提交成功，当前状态：排队中。请保存兑换码，稍后可在本页查询状态。",
                                }

    return render_template(
        "redeem.html",
        result=result,
        code_row=code_row,
        latest_task=latest_task,
        status_results=status_results,
        status_query_input=status_query_input,
        status_text=PUBLIC_STATUS_TEXT,
    )


@app.route("/FenYi/login", methods=["GET", "POST"])
def owner_login():
    if request.method == "POST":
        username = request.form.get("username", OWNER_USERNAME).strip() or OWNER_USERNAME
        password = request.form.get("password", "")
        user = authenticate(username, password)
        if user and user["role"] == "owner":
            login_user(user)
            return redirect(request.args.get("next") or url_for("owner_dashboard"))
        flash("账号或密码不正确。", "error")
    return render_template("admin_login.html", default_username=OWNER_USERNAME)


@app.route("/ops/login", methods=["GET", "POST"])
def ops_login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = authenticate(username, password)
        if user and user["role"] in {"owner", "lead", "staff"}:
            login_user(user)
            return redirect(request.args.get("next") or url_for("ops_tasks"))
        flash("Invalid username or password.", "error")
    return render_template("ops_login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("redeem"))


@app.route("/FenYi/logout")
def owner_logout():
    session.clear()
    return redirect(url_for("owner_login"))


@app.route("/ops/logout")
def ops_logout():
    session.clear()
    return redirect(url_for("ops_login"))


@app.route("/FenYi")
@owner_required
def owner_dashboard():
    active_tasks = task_query("t.status IN ('pending', 'assigned', 'processing')", limit=200)
    with get_db() as conn:
        task_counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM redemption_tasks GROUP BY status").fetchall()
        }
        code_counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM codes GROUP BY status").fetchall()
        }
        staff = active_staff_users(conn)
    return render_template(
        "admin.html",
        tasks=active_tasks,
        task_counts=task_counts,
        code_counts=code_counts,
        staff=staff,
    )


@app.route("/FenYi/tasks")
@owner_required
def owner_tasks():
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    where = []
    params = []
    if status in {"pending", "assigned", "processing"}:
        where.append("t.status = ?")
        params.append(status)
    else:
        where.append("t.status IN ('pending', 'assigned', 'processing')")
    if q:
        where.append("(t.proxy_code LIKE ? OR t.token LIKE ? OR t.contact LIKE ?)")
        params.extend([f"%{q}%"] * 3)
    tasks = task_query(" AND ".join(where), params, limit=500)
    with get_db() as conn:
        staff = active_staff_users(conn)
        queue_counts = visible_task_counts(conn, current_user())
    return render_template(
        "owner_tasks.html",
        tasks=tasks,
        staff=staff,
        selected_status=status,
        q=q,
        queue_counts=queue_counts,
        archive_mode=False,
    )


@app.route("/FenYi/tasks/archive")
@owner_required
def owner_tasks_archive():
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    where = []
    params = []
    if status in {"success", "failed"}:
        where.append("t.status = ?")
        params.append(status)
    else:
        where.append("t.status IN ('success', 'failed')")
    if q:
        where.append("(t.proxy_code LIKE ? OR t.token LIKE ? OR t.contact LIKE ?)")
        params.extend([f"%{q}%"] * 3)
    tasks = task_query(" AND ".join(where), params, limit=500)
    with get_db() as conn:
        staff = active_staff_users(conn)
        queue_counts = visible_task_counts(conn, current_user())
    return render_template(
        "owner_tasks.html",
        tasks=tasks,
        staff=staff,
        selected_status=status,
        q=q,
        queue_counts=queue_counts,
        archive_mode=True,
    )


@app.route("/FenYi/codes/list")
@owner_required
def owner_codes():
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    params = []
    where = []
    if status in CODE_STATUSES:
        where.append("status = ?")
        params.append(status)
    if q:
        where.append("(proxy_code LIKE ? OR partner LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])
    sql = "SELECT * FROM codes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT 500"
    with get_db() as conn:
        codes = conn.execute(sql, params).fetchall()
    return render_template("codes.html", codes=codes, selected_status=status, q=q)


@app.route("/FenYi/codes/update/<int:code_id>", methods=["POST"])
@owner_required
def owner_update_code(code_id: int):
    new_status = request.form.get("status", "").strip()
    remark = request.form.get("remark", "").strip()
    if new_status not in {"created", "distributed", "disabled"}:
        flash("只能手动设置为未上架、可兑换或禁用；已核销状态由成功任务自动产生。", "error")
        return redirect(request.referrer or url_for("owner_codes"))
    with get_db() as conn:
        code = conn.execute("SELECT * FROM codes WHERE id = ?", (code_id,)).fetchone()
        if not code:
            abort(404)
        conn.execute("UPDATE codes SET status = ?, remark = ? WHERE id = ?", (new_status, remark, code_id))
        write_log(
            conn,
            "owner_update_code",
            code_id=code_id,
            old_value=code["status"],
            new_value=new_status,
            note=remark,
        )
        conn.commit()
    flash("卡密状态已更新。", "success")
    return redirect(request.referrer or url_for("owner_codes"))


@app.route("/FenYi/codes", methods=["GET", "POST"])
@owner_required
def generate_codes():
    generated = []
    if request.method == "POST":
        partner = request.form.get("partner", "白先生").strip()
        prefix = normalize_code(request.form.get("prefix", "CNVIP")) or "CNVIP"
        status = request.form.get("status", "distributed")
        if status not in {"created", "distributed"}:
            status = "distributed"
        try:
            quantity = max(1, min(500, int(request.form.get("quantity", "20"))))
        except ValueError:
            quantity = 20
        try:
            partner_price = float(request.form.get("partner_price", "32") or 0)
        except ValueError:
            partner_price = 0
        try:
            supplier_cost = float(request.form.get("supplier_cost", "25") or 0)
        except ValueError:
            supplier_cost = 0

        with get_db() as conn:
            for _ in range(quantity):
                code = generate_unique_code(conn, prefix)
                conn.execute(
                    """
                    INSERT INTO codes (proxy_code, partner, status, partner_price, supplier_cost)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (code, partner, status, partner_price, supplier_cost),
                )
                generated.append(code)
            write_log(conn, "generate_codes", new_value=str(len(generated)), note=f"partner={partner}, prefix={prefix}")
            conn.commit()
        flash(f"已生成 {len(generated)} 个兑换码。", "success")

    return render_template("generate.html", generated=generated)


@app.route("/FenYi/export")
@owner_required
def export_codes():
    partner = request.args.get("partner", "").strip()
    status = request.args.get("status", "distributed").strip()
    params = []
    where = []
    if partner:
        where.append("partner = ?")
        params.append(partner)
    if status in CODE_STATUSES:
        where.append("status = ?")
        params.append(status)
    sql = "SELECT proxy_code FROM codes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id ASC"

    output = io.StringIO()
    writer = csv.writer(output)
    redeem_url = request.url_root.rstrip("/") + url_for("redeem")
    writer.writerow(["proxy_code", "redeem_url"])
    with get_db() as conn:
        for row in conn.execute(sql, params).fetchall():
            writer.writerow([row["proxy_code"], redeem_url])
        write_log(conn, "export_codes", new_value=status, note=f"partner={partner or 'ALL'}")
        conn.commit()

    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=proxy_codes.csv"},
    )


@app.route("/FenYi/stats")
@owner_required
def stats():
    with get_db() as conn:
        code_counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM codes GROUP BY status").fetchall()
        }
        task_counts = {
            row["status"]: row["count"]
            for row in conn.execute("SELECT status, COUNT(*) AS count FROM redemption_tasks GROUP BY status").fetchall()
        }
        totals = conn.execute(
            """
            SELECT
                COALESCE(SUM(partner_price), 0) AS revenue,
                COALESCE(SUM(supplier_cost), 0) AS cost,
                COALESCE(SUM(partner_price - supplier_cost), 0) AS profit
            FROM codes WHERE status = 'redeemed'
            """
        ).fetchone()
    return render_template("stats.html", code_counts=code_counts, task_counts=task_counts, totals=totals)


@app.route("/FenYi/team", methods=["GET", "POST"])
@owner_required
def owner_team():
    return manage_team(owner_view=True)


@app.route("/ops")
@ops_required
def ops_index():
    return redirect(url_for("ops_tasks"))


@app.route("/ops/tasks")
@ops_required
def ops_tasks():
    user = current_user()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    where = []
    params = []

    if user["role"] == "staff":
        where.append("t.assigned_to = ?")
        params.append(user["id"])
    if status in {"pending", "assigned", "processing"}:
        where.append("t.status = ?")
        params.append(status)
    else:
        where.append("t.status IN ('pending', 'assigned', 'processing')")
    if q and user["role"] in {"owner", "lead"}:
        where.append("(t.proxy_code LIKE ? OR t.token LIKE ? OR t.contact LIKE ?)")
        params.extend([f"%{q}%"] * 3)

    tasks = task_query(" AND ".join(where), params, limit=500)
    with get_db() as conn:
        staff = active_staff_users(conn) if user["role"] in {"owner", "lead"} else []
        queue_counts = visible_task_counts(conn, user)
    return render_template(
        "ops_tasks.html",
        tasks=tasks,
        staff=staff,
        selected_status=status,
        q=q,
        queue_counts=queue_counts,
        archive_mode=False,
    )


@app.route("/ops/tasks/archive")
@ops_required
def ops_tasks_archive():
    user = current_user()
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    where = []
    params = []

    if user["role"] == "staff":
        where.append("t.completed_by = ?")
        params.append(user["id"])
    if status in {"success", "failed"}:
        where.append("t.status = ?")
        params.append(status)
    else:
        where.append("t.status IN ('success', 'failed')")
    if q and user["role"] in {"owner", "lead"}:
        where.append("(t.proxy_code LIKE ? OR t.token LIKE ? OR t.contact LIKE ?)")
        params.extend([f"%{q}%"] * 3)

    tasks = task_query(" AND ".join(where), params, limit=500)
    with get_db() as conn:
        staff = active_staff_users(conn) if user["role"] in {"owner", "lead"} else []
        queue_counts = visible_task_counts(conn, user)
    return render_template(
        "ops_tasks.html",
        tasks=tasks,
        staff=staff,
        selected_status=status,
        q=q,
        queue_counts=queue_counts,
        archive_mode=True,
    )


@app.route("/ops/team", methods=["GET", "POST"])
@lead_or_owner_required
def ops_team():
    return manage_team(owner_view=False)


@app.route("/ops/team/<int:member_id>")
@lead_or_owner_required
def ops_team_member(member_id: int):
    viewer = current_user()
    with get_db() as conn:
        if viewer["role"] == "lead":
            member = conn.execute(
                "SELECT * FROM users WHERE id = ? AND role = 'staff' AND COALESCE(is_deleted, 0) = 0", (member_id,)
            ).fetchone()
        else:
            member = conn.execute(
                "SELECT * FROM users WHERE id = ? AND role IN ('lead', 'staff') AND COALESCE(is_deleted, 0) = 0", (member_id,)
            ).fetchone()
        if not member:
            abort(404)
        counts = member_workload_counts(conn, member_id)
        staff = active_staff_users(conn)

    current_tasks = task_query(
        "t.assigned_to = ? AND t.status IN ('pending', 'assigned', 'processing')",
        (member_id,),
        limit=200,
    )
    completed_today = task_query(
        "t.completed_by = ? AND t.status IN ('success', 'failed') AND DATE(t.completed_at) = DATE('now')",
        (member_id,),
        limit=200,
    )
    return render_template(
        "ops_member.html",
        member=member,
        counts=counts,
        current_tasks=current_tasks,
        completed_today=completed_today,
        staff=staff,
    )


def manage_team(owner_view: bool):
    user = current_user()
    if request.method == "POST":
        action = request.form.get("action", "create")
        with get_db() as conn:
            if action == "create":
                username = request.form.get("username", "").strip()
                display_name = request.form.get("display_name", "").strip() or username
                password = request.form.get("password", "")
                role = request.form.get("role", "staff")
                if user["role"] == "lead":
                    role = "staff"
                if role not in {"lead", "staff"}:
                    flash("Invalid role." if not owner_view else "角色不合法。", "error")
                elif len(username) < 3 or len(password) < 6:
                    flash(
                        "Username must be at least 3 characters and password at least 6 characters."
                        if not owner_view
                        else "用户名至少 3 位，密码至少 6 位。",
                        "error",
                    )
                else:
                    try:
                        cur = conn.execute(
                            """
                            INSERT INTO users (username, password_hash, role, display_name, is_active, created_by)
                            VALUES (?, ?, ?, ?, 1, ?)
                            """,
                            (username, generate_password_hash(password), role, display_name, user["id"]),
                        )
                        write_log(
                            conn,
                            "create_user",
                            new_value=f"{username}:{role}",
                            note=f"display_name={display_name}",
                        )
                        conn.commit()
                        flash("User created." if not owner_view else "员工账号已创建。", "success")
                    except sqlite3.IntegrityError:
                        flash("Username already exists." if not owner_view else "用户名已存在。", "error")

            elif action in {"disable", "enable"}:
                target_id = int(request.form.get("user_id", "0"))
                target = conn.execute("SELECT * FROM users WHERE id = ? AND COALESCE(is_deleted, 0) = 0", (target_id,)).fetchone()
                if not target or target["role"] == "owner" or (user["role"] == "lead" and target["role"] != "staff"):
                    abort(403)
                new_active = 0 if action == "disable" else 1
                conn.execute("UPDATE users SET is_active = ? WHERE id = ?", (new_active, target_id))
                write_log(conn, action + "_user", new_value=target["username"])
                conn.commit()
                flash("User updated." if not owner_view else "员工状态已更新。", "success")

            elif action == "delete":
                target_id = int(request.form.get("user_id", "0"))
                target = conn.execute("SELECT * FROM users WHERE id = ? AND COALESCE(is_deleted, 0) = 0", (target_id,)).fetchone()
                if not target or target["role"] == "owner" or (user["role"] == "lead" and target["role"] != "staff"):
                    abort(403)

                # Soft-delete the account so historical tasks/logs keep a valid user reference.
                # Any unfinished tasks assigned to this account return to the unassigned queue.
                reassigned = conn.execute(
                    """
                    UPDATE redemption_tasks
                    SET status = 'pending', assigned_to = NULL, assigned_by = NULL,
                        assigned_at = NULL, started_at = NULL
                    WHERE assigned_to = ? AND status IN ('pending', 'assigned', 'processing')
                    """,
                    (target_id,),
                ).rowcount
                deleted_username = f"{target['username']}__deleted_{target_id}"
                conn.execute(
                    """
                    UPDATE users
                    SET is_active = 0, is_deleted = 1, deleted_at = CURRENT_TIMESTAMP,
                        username = ?
                    WHERE id = ?
                    """,
                    (deleted_username, target_id),
                )
                write_log(
                    conn,
                    "delete_user",
                    old_value=target["username"],
                    new_value=deleted_username,
                    note=f"display_name={target['display_name']}; active_tasks_returned_to_queue={reassigned}",
                )
                conn.commit()
                flash("User deleted. Active tasks were returned to the queue." if not owner_view else "员工账号已删除；未完成任务已回到待分配队列。", "success")

            elif action == "reset_password":
                target_id = int(request.form.get("user_id", "0"))
                new_password = request.form.get("new_password", "")
                target = conn.execute("SELECT * FROM users WHERE id = ? AND COALESCE(is_deleted, 0) = 0", (target_id,)).fetchone()
                if not target or target["role"] == "owner" or (user["role"] == "lead" and target["role"] != "staff"):
                    abort(403)
                if len(new_password) < 6:
                    flash("Password must be at least 6 characters." if not owner_view else "密码至少 6 位。", "error")
                else:
                    conn.execute(
                        "UPDATE users SET password_hash = ? WHERE id = ?",
                        (generate_password_hash(new_password), target_id),
                    )
                    write_log(conn, "reset_password", new_value=target["username"])
                    conn.commit()
                    flash("Password reset." if not owner_view else "密码已重置。", "success")

    with get_db() as conn:
        if user["role"] == "owner":
            users = conn.execute("SELECT * FROM users WHERE COALESCE(is_deleted, 0) = 0 ORDER BY role, is_active DESC, id DESC").fetchall()
        else:
            users = conn.execute("SELECT * FROM users WHERE role = 'staff' AND COALESCE(is_deleted, 0) = 0 ORDER BY is_active DESC, id DESC").fetchall()
        workloads = staff_workload_rows(conn, user) if not owner_view else []

    template = "team.html" if owner_view else "ops_team.html"
    return render_template(template, users=users, workloads=workloads)


@app.route("/ops/tasks/<int:task_id>/assign", methods=["POST"])
@lead_or_owner_required
def assign_task(task_id: int):
    assignee_id = int(request.form.get("assigned_to", "0"))
    user = current_user()
    with get_db() as conn:
        task = conn.execute("SELECT * FROM redemption_tasks WHERE id = ?", (task_id,)).fetchone()
        assignee = conn.execute(
            "SELECT * FROM users WHERE id = ? AND is_active = 1 AND COALESCE(is_deleted, 0) = 0 AND role IN ('lead', 'staff')",
            (assignee_id,),
        ).fetchone()
        if not task or not assignee:
            abort(404)
        if task["status"] in {"success", "failed"}:
            flash("Completed tasks cannot be reassigned.", "error")
            return redirect(request.referrer or url_for("ops_tasks"))
        old = f"{task['status']}:{task['assigned_to'] or ''}"
        conn.execute(
            """
            UPDATE redemption_tasks
            SET status = 'assigned', assigned_to = ?, assigned_by = ?, assigned_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (assignee_id, user["id"], task_id),
        )
        write_log(
            conn,
            "assign_task",
            task_id=task_id,
            code_id=task["code_id"],
            old_value=old,
            new_value=f"assigned:{assignee_id}",
        )
        conn.commit()
    flash("Task assigned.", "success")
    return redirect(request.referrer or url_for("ops_tasks"))


@app.route("/ops/tasks/<int:task_id>/start", methods=["POST"])
@ops_required
def start_task(task_id: int):
    user = current_user()
    with get_db() as conn:
        task = conn.execute("SELECT * FROM redemption_tasks WHERE id = ?", (task_id,)).fetchone()
        if not task:
            abort(404)
        if not can_operate_task(user, task):
            abort(403)
        if task["status"] not in {"pending", "assigned"}:
            flash("Only pending or assigned tasks can be started.", "error")
            return redirect(request.referrer or url_for("ops_tasks"))
        assigned_to = task["assigned_to"] or user["id"]
        conn.execute(
            """
            UPDATE redemption_tasks
            SET status = 'processing', assigned_to = ?, started_at = COALESCE(started_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            """,
            (assigned_to, task_id),
        )
        write_log(conn, "start_task", task_id=task_id, code_id=task["code_id"], old_value=task["status"], new_value="processing")
        conn.commit()
    flash("Task started.", "success")
    return redirect(request.referrer or url_for("ops_tasks"))


@app.route("/ops/tasks/<int:task_id>/success", methods=["POST"])
@ops_required
def mark_task_success(task_id: int):
    user = current_user()
    note = request.form.get("worker_note", "").strip()
    with get_db() as conn:
        task = conn.execute("SELECT * FROM redemption_tasks WHERE id = ?", (task_id,)).fetchone()
        if not task:
            abort(404)
        if not can_operate_task(user, task):
            abort(403)
        if task["status"] in {"success", "failed"}:
            flash("Task is already completed.", "error")
            return redirect(request.referrer or url_for("ops_tasks"))
        code = conn.execute("SELECT * FROM codes WHERE id = ?", (task["code_id"],)).fetchone()
        if not code or code["status"] != "distributed":
            flash("This CDK is no longer redeemable.", "error")
            return redirect(request.referrer or url_for("ops_tasks"))

        conn.execute(
            """
            UPDATE redemption_tasks
            SET status = 'success', completed_by = ?, completed_at = CURRENT_TIMESTAMP,
                worker_note = ?
            WHERE id = ?
            """,
            (user["id"], note, task_id),
        )
        conn.execute(
            """
            UPDATE codes
            SET status = 'redeemed', redeemed_at = CURRENT_TIMESTAMP, redeemed_task_id = ?
            WHERE id = ? AND status = 'distributed'
            """,
            (task_id, task["code_id"]),
        )
        write_log(
            conn,
            "mark_success",
            task_id=task_id,
            code_id=task["code_id"],
            old_value=task["status"],
            new_value="success/redeemed",
            note=note,
        )
        conn.commit()
    flash("Marked as Success. CDK has been redeemed.", "success")
    return redirect(request.referrer or url_for("ops_tasks"))


@app.route("/ops/tasks/<int:task_id>/failed", methods=["POST"])
@ops_required
def mark_task_failed(task_id: int):
    user = current_user()
    fail_reason = request.form.get("fail_reason", "").strip()
    other_reason = request.form.get("other_reason", "").strip()
    note = request.form.get("worker_note", "").strip()
    if fail_reason == "Other":
        fail_reason = f"Other: {other_reason}" if other_reason else "Other"
    if not fail_reason:
        flash("Failure reason is required.", "error")
        return redirect(request.referrer or url_for("ops_tasks"))

    with get_db() as conn:
        task = conn.execute("SELECT * FROM redemption_tasks WHERE id = ?", (task_id,)).fetchone()
        if not task:
            abort(404)
        if not can_operate_task(user, task):
            abort(403)
        if task["status"] in {"success", "failed"}:
            flash("Task is already completed.", "error")
            return redirect(request.referrer or url_for("ops_tasks"))
        conn.execute(
            """
            UPDATE redemption_tasks
            SET status = 'failed', completed_by = ?, completed_at = CURRENT_TIMESTAMP,
                fail_reason = ?, worker_note = ?
            WHERE id = ?
            """,
            (user["id"], fail_reason, note, task_id),
        )
        # Important business rule: Failed tasks do NOT redeem the CDK.
        conn.execute("UPDATE codes SET status = 'distributed' WHERE id = ? AND status = 'distributed'", (task["code_id"],))
        write_log(
            conn,
            "mark_failed",
            task_id=task_id,
            code_id=task["code_id"],
            old_value=task["status"],
            new_value="failed/code_still_distributed",
            note=f"{fail_reason}; {note}",
        )
        conn.commit()
    flash("Marked as Failed. CDK remains redeemable.", "success")
    return redirect(request.referrer or url_for("ops_tasks"))


@app.route("/FenYi/logs")
@owner_required
def owner_logs():
    with get_db() as conn:
        logs = conn.execute(
            """
            SELECT l.*, u.username, u.display_name
            FROM operation_logs l
            LEFT JOIN users u ON u.id = l.actor_id
            ORDER BY l.id DESC LIMIT 500
            """
        ).fetchall()
    return render_template("logs.html", logs=logs)


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
