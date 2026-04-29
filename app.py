import csv
import io
import os
import secrets
import sqlite3
import string
from functools import wraps
from pathlib import Path
from typing import Optional

from flask import (
    Flask, Response, flash, redirect, render_template, request,
    session, url_for
)

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "cdk_center.db"
VALID_STATUSES = {"created", "distributed", "submitted", "processing", "success", "failed", "refunded"}
PUBLIC_STATUS_TEXT = {
    "created": "兑换码尚未上架",
    "distributed": "待提交",
    "submitted": "排队中",
    "processing": "排队中",
    "success": "充值成功",
    "failed": "充值失败，请凭兑换码联系购买渠道管理员退款",
    "refunded": "已退款",
}
ADMIN_STATUS_TEXT = {
    "created": "刚生成",
    "distributed": "已发给合作方/可兑换",
    "submitted": "用户已提交/排队中",
    "processing": "处理中",
    "success": "充值成功",
    "failed": "充值失败",
    "refunded": "已退款",
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD", "admin123")


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                proxy_code TEXT UNIQUE NOT NULL,
                partner TEXT DEFAULT '',
                status TEXT NOT NULL DEFAULT 'created',
                student_id TEXT,
                contact TEXT,
                partner_price REAL DEFAULT 0,
                supplier_cost REAL DEFAULT 0,
                submitted_at TEXT,
                processed_at TEXT,
                created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                remark TEXT
            )
            """
        )
        conn.commit()


@app.before_request
def ensure_db_exists():
    init_db()


def normalize_code(code: str) -> str:
    return "".join(code.strip().upper().split())


def make_code(prefix: str = "CNVIP") -> str:
    alphabet = string.ascii_uppercase + string.digits
    part1 = "".join(secrets.choice(alphabet) for _ in range(4))
    part2 = "".join(secrets.choice(alphabet) for _ in range(4))
    return f"{prefix}-{part1}-{part2}"


def generate_unique_code(conn: sqlite3.Connection, prefix: str = "CNVIP") -> str:
    for _ in range(20):
        code = make_code(prefix)
        exists = conn.execute("SELECT 1 FROM codes WHERE proxy_code = ?", (code,)).fetchone()
        if not exists:
            return code
    raise RuntimeError("生成兑换码失败，请重试")


def admin_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged_in"):
            return redirect(url_for("admin_login", next=request.path))
        return fn(*args, **kwargs)
    return wrapper


@app.route("/")
def index():
    return redirect(url_for("redeem"))


@app.route("/redeem", methods=["GET", "POST"])
def redeem():
    result = None
    code_row = None

    if request.method == "POST":
        action = request.form.get("action", "redeem")
        proxy_code = normalize_code(request.form.get("proxy_code", ""))

        if not proxy_code:
            result = {"type": "error", "message": "请输入兑换码。"}
        else:
            with get_db() as conn:
                row = conn.execute("SELECT * FROM codes WHERE proxy_code = ?", (proxy_code,)).fetchone()
                if not row:
                    result = {"type": "error", "message": "兑换码无效。"}
                elif action == "check":
                    code_row = row
                    result = {
                        "type": "info",
                        "message": PUBLIC_STATUS_TEXT.get(row["status"], "状态未知"),
                        "status": row["status"],
                    }
                else:
                    student_id = request.form.get("student_id", "").strip()
                    contact = request.form.get("contact", "").strip()
                    if row["status"] != "distributed":
                        code_row = row
                        result = {
                            "type": "info",
                            "message": PUBLIC_STATUS_TEXT.get(row["status"], "兑换码已使用或不可兑换。"),
                            "status": row["status"],
                        }
                    elif not student_id:
                        result = {"type": "error", "message": "请输入学号。"}
                    else:
                        cur = conn.execute(
                            """
                            UPDATE codes
                            SET status = 'submitted', student_id = ?, contact = ?, submitted_at = CURRENT_TIMESTAMP
                            WHERE proxy_code = ? AND status = 'distributed'
                            """,
                            (student_id, contact, proxy_code),
                        )
                        conn.commit()
                        if cur.rowcount == 1:
                            code_row = conn.execute("SELECT * FROM codes WHERE proxy_code = ?", (proxy_code,)).fetchone()
                            result = {
                                "type": "success",
                                "message": "提交成功，当前状态：排队中。请保存兑换码，稍后可在本页查询状态。",
                                "status": "submitted",
                            }
                        else:
                            result = {"type": "error", "message": "提交失败，请刷新后重试。"}

    return render_template("redeem.html", result=result, code_row=code_row, status_text=PUBLIC_STATUS_TEXT)



@app.route("/FenYi/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if secrets.compare_digest(password, ADMIN_PASSWORD):
            session["admin_logged_in"] = True
            return redirect(request.args.get("next") or url_for("admin"))
        flash("后台密码不正确。", "error")
    return render_template("admin_login.html")



@app.route("/FenYi/logout")

def admin_logout():
    session.clear()
    return redirect(url_for("admin_login"))



@app.route("/FenYi")

@admin_required
def admin():
    status = request.args.get("status", "").strip()
    q = request.args.get("q", "").strip()
    params = []
    where = []
    if status in VALID_STATUSES:
        where.append("status = ?")
        params.append(status)
    if q:
        where.append("(proxy_code LIKE ? OR student_id LIKE ? OR contact LIKE ? OR partner LIKE ?)")
        params.extend([f"%{q}%"] * 4)
    sql = "SELECT * FROM codes"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT 500"
    with get_db() as conn:
        codes = conn.execute(sql, params).fetchall()
    return render_template("admin.html", codes=codes, statuses=VALID_STATUSES, status_text=ADMIN_STATUS_TEXT)



@app.route("/FenYi/update/<int:code_id>", methods=["POST"])
@admin_required
def update_code(code_id: int):
    new_status = request.form.get("status", "")
    remark = request.form.get("remark", "").strip()
    if new_status not in VALID_STATUSES:
        flash("状态不合法。", "error")
        return redirect(url_for("admin"))

    processed_clause = ", processed_at = CURRENT_TIMESTAMP" if new_status in {"success", "failed", "refunded"} else ""
    with get_db() as conn:
        conn.execute(
            f"UPDATE codes SET status = ?, remark = ? {processed_clause} WHERE id = ?",
            (new_status, remark, code_id),
        )
        conn.commit()
    flash("状态已更新。", "success")
    return redirect(request.referrer or url_for("admin"))



@app.route("/FenYi/codes", methods=["GET", "POST"])

@admin_required
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
            conn.commit()
        flash(f"已生成 {len(generated)} 个兑换码。", "success")

    return render_template("generate.html", generated=generated)



@app.route("/FenYi/export")

@admin_required
def export_codes():
    partner = request.args.get("partner", "").strip()
    status = request.args.get("status", "distributed").strip()
    params = []
    where = []
    if partner:
        where.append("partner = ?")
        params.append(partner)
    if status in VALID_STATUSES:
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

    return Response(
        output.getvalue(),
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=proxy_codes.csv"},
    )



@app.route("/FenYi/stats")

@admin_required
def stats():
    with get_db() as conn:
        counts = {row["status"]: row["count"] for row in conn.execute("SELECT status, COUNT(*) AS count FROM codes GROUP BY status").fetchall()}
        totals = conn.execute(
            """
            SELECT
                COALESCE(SUM(partner_price), 0) AS revenue,
                COALESCE(SUM(supplier_cost), 0) AS cost,
                COALESCE(SUM(partner_price - supplier_cost), 0) AS profit
            FROM codes WHERE status = 'success'
            """
        ).fetchone()
    return render_template("stats.html", counts=counts, totals=totals, statuses=VALID_STATUSES, status_text=ADMIN_STATUS_TEXT)


if __name__ == "__main__":
    init_db()
    app.run(debug=True)
