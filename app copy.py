import os
import sqlite3
from functools import wraps
import json
import uuid
from datetime import datetime, timezone

from flask import Flask, render_template, request, redirect, url_for, session, flash, send_file
from flask_wtf import FlaskForm
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from wtforms import StringField, PasswordField
from wtforms.validators import DataRequired


PRICING_STORE_PATH = os.path.join("data", "pricing_entries.json")
CUSTOMERS_STORE_PATH = os.path.join("data", "customers.json")
COMPANY_STORE_PATH = os.path.join("data", "company_info.json")
LOGO_UPLOAD_FOLDER = os.path.join("static", "uploads", "logos")
PRICE_LETTER_HISTORY_PATH = os.path.join("data", "price_letter_history.json")
PRICE_LETTER_FOLDER = os.path.join("static", "price_letters")


# -------------------------
# Config
# -------------------------
app = Flask(__name__)

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", "dev-only-change-me")
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = (os.environ.get("FLASK_ENV") == "production")
app.config["LOGO_UPLOAD_FOLDER"] = LOGO_UPLOAD_FOLDER

os.makedirs(app.config["LOGO_UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(PRICE_LETTER_FOLDER, exist_ok=True)

DB_PATH = os.path.join(os.path.dirname(__file__), "app.db")


# -------------------------
# Helpers
# -------------------------
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            full_name TEXT NOT NULL DEFAULT '',
            phone TEXT NOT NULL DEFAULT '',
            is_admin INTEGER NOT NULL DEFAULT 0,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()

    existing_cols = [row["name"] for row in cur.execute("PRAGMA table_info(users)").fetchall()]

    if "full_name" not in existing_cols:
        cur.execute("ALTER TABLE users ADD COLUMN full_name TEXT NOT NULL DEFAULT ''")
    if "phone" not in existing_cols:
        cur.execute("ALTER TABLE users ADD COLUMN phone TEXT NOT NULL DEFAULT ''")
    if "is_admin" not in existing_cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_admin INTEGER NOT NULL DEFAULT 0")

    conn.commit()
    conn.close()


def create_user(email: str, password: str, full_name: str = "", phone: str = "", is_admin: int = 0):
    conn = get_db()
    cur = conn.cursor()
    pw_hash = generate_password_hash(password)
    cur.execute("""
        INSERT INTO users (email, password_hash, full_name, phone, is_admin)
        VALUES (?, ?, ?, ?, ?)
    """, (
        email.lower().strip(),
        pw_hash,
        full_name.strip(),
        phone.strip(),
        int(is_admin)
    ))
    conn.commit()
    conn.close()


def find_user_by_email(email: str):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE email = ?", (email.lower().strip(),))
    row = cur.fetchone()
    conn.close()
    return row


def find_user_by_id(user_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def get_all_users():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users ORDER BY full_name, email")
    rows = cur.fetchall()
    conn.close()
    return rows


def update_user_profile(user_id: int, full_name: str, email: str, phone: str, password: str = ""):
    conn = get_db()
    cur = conn.cursor()

    if password.strip():
        pw_hash = generate_password_hash(password.strip())
        cur.execute("""
            UPDATE users
            SET full_name = ?, email = ?, phone = ?, password_hash = ?
            WHERE id = ?
        """, (
            full_name.strip(),
            email.lower().strip(),
            phone.strip(),
            pw_hash,
            user_id
        ))
    else:
        cur.execute("""
            UPDATE users
            SET full_name = ?, email = ?, phone = ?
            WHERE id = ?
        """, (
            full_name.strip(),
            email.lower().strip(),
            phone.strip(),
            user_id
        ))

    conn.commit()
    conn.close()


def delete_user(user_id: int):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()


def _ensure_data_dir():
    os.makedirs(os.path.dirname(PRICING_STORE_PATH), exist_ok=True)


def load_pricing_store():
    _ensure_data_dir()
    if not os.path.exists(PRICING_STORE_PATH):
        return {"last_reset_key": None, "by_month": {}}
    try:
        with open(PRICING_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"last_reset_key": None, "by_month": {}}


def save_pricing_store(store: dict):
    _ensure_data_dir()
    with open(PRICING_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2)


def month_key_from(year: str, month: str) -> str:
    m = (month or "").strip().upper()
    month_map = {
        "1": "JAN", "01": "JAN", "JANUARY": "JAN", "JAN": "JAN",
        "2": "FEB", "02": "FEB", "FEBRUARY": "FEB", "FEB": "FEB",
        "3": "MAR", "03": "MAR", "MARCH": "MAR", "MAR": "MAR",
        "4": "APR", "04": "APR", "APRIL": "APR", "APR": "APR",
        "5": "MAY", "05": "MAY", "MAY": "MAY",
        "6": "JUN", "06": "JUN", "JUNE": "JUN", "JUN": "JUN",
        "7": "JUL", "07": "JUL", "JULY": "JUL", "JUL": "JUL",
        "8": "AUG", "08": "AUG", "AUGUST": "AUG", "AUG": "AUG",
        "9": "SEP", "09": "SEP", "SEPTEMBER": "SEP", "SEP": "SEP", "SEPT": "SEP",
        "10": "OCT", "OCTOBER": "OCT", "OCT": "OCT",
        "11": "NOV", "NOVEMBER": "NOV", "NOV": "NOV",
        "12": "DEC", "DECEMBER": "DEC", "DEC": "DEC",
    }
    m3 = month_map.get(m, m[:3] if len(m) >= 3 else m)
    y = (year or "").strip()
    return f"{y}-{m3}"


def current_month_key_central():
    now = datetime.now()
    y = str(now.year)
    m = now.strftime("%b").upper()
    return f"{y}-{m}"


def maybe_auto_reset_month(store: dict):
    now = datetime.now()
    if now.day != 1:
        return store

    cur_key = current_month_key_central()
    if store.get("last_reset_key") == cur_key:
        return store

    store.setdefault("by_month", {})
    store["by_month"][cur_key] = []
    store["last_reset_key"] = cur_key
    save_pricing_store(store)
    return store


def load_customers():
    os.makedirs(os.path.dirname(CUSTOMERS_STORE_PATH), exist_ok=True)
    if not os.path.exists(CUSTOMERS_STORE_PATH):
        return []
    try:
        with open(CUSTOMERS_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_customers(customers: list):
    os.makedirs(os.path.dirname(CUSTOMERS_STORE_PATH), exist_ok=True)
    with open(CUSTOMERS_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(customers, f, indent=2)


def get_current_month_product_costs():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    cur_key = current_month_key_central()
    rows = (store.get("by_month") or {}).get(cur_key, []) or []

    latest = {}
    for r in rows:
        product = (r.get("product") or "").strip()
        um = (r.get("um") or "").strip().upper()
        if not product or not um:
            continue
        latest[(product.lower(), um.lower())] = r

    products = []
    for (_, _), r in latest.items():
        product = (r.get("product") or "").strip()
        um = (r.get("um") or "").strip().upper()
        cost = float(r.get("final_price") or 0.0)
        products.append({
            "key": f"{product}||{um}",
            "product": product,
            "um": um,
            "cost": cost
        })

    products.sort(key=lambda x: (x["product"].lower(), x["um"].lower()))
    return cur_key, products


def load_company_info():
    os.makedirs(os.path.dirname(COMPANY_STORE_PATH), exist_ok=True)
    if not os.path.exists(COMPANY_STORE_PATH):
        return {
            "company_name": "",
            "website_url": "",
            "address": "",
            "logo_path": ""
        }
    try:
        with open(COMPANY_STORE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {
            "company_name": "",
            "website_url": "",
            "address": "",
            "logo_path": ""
        }


def save_company_info(company_info: dict):
    os.makedirs(os.path.dirname(COMPANY_STORE_PATH), exist_ok=True)
    with open(COMPANY_STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(company_info, f, indent=2)


def allowed_logo_file(filename):
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in {"png", "jpg", "jpeg", "webp"}


def load_price_letter_history():
    os.makedirs(os.path.dirname(PRICE_LETTER_HISTORY_PATH), exist_ok=True)
    if not os.path.exists(PRICE_LETTER_HISTORY_PATH):
        return []
    try:
        with open(PRICE_LETTER_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def save_price_letter_history(rows: list):
    os.makedirs(os.path.dirname(PRICE_LETTER_HISTORY_PATH), exist_ok=True)
    with open(PRICE_LETTER_HISTORY_PATH, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def make_safe_filename_part(value: str) -> str:
    value = (value or "").strip()
    cleaned = "".join(ch for ch in value if ch.isalnum() or ch in (" ", "-", "_"))
    cleaned = " ".join(cleaned.split())
    return cleaned or "PriceLetter"


def month_label_from_key(month_key: str) -> str:
    parts = (month_key or "").split("-")
    if len(parts) != 2:
        return month_key or ""
    year, mon = parts
    month_names = {
        "JAN": "January", "FEB": "February", "MAR": "March", "APR": "April",
        "MAY": "May", "JUN": "June", "JUL": "July", "AUG": "August",
        "SEP": "September", "OCT": "October", "NOV": "November", "DEC": "December",
    }
    return f"{month_names.get(mon, mon)} {year}"


def add_price_letter_history(customer_name, month_key, file_name, pdf_path, payload):
    history = load_price_letter_history()

    entry = {
        "id": f"pl_{uuid.uuid4().hex[:8]}",
        "customer_name": customer_name,
        "month_key": month_key,
        "file_name": file_name,
        "pdf_path": pdf_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload
    }

    history.insert(0, entry)
    save_price_letter_history(history)
    return entry

def ensure_folder(path):
    os.makedirs(path, exist_ok=True)


def load_json_file(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except Exception:
        return default


def save_json_file(path, data):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_company_info():
    data = load_json_file(COMPANY_STORE_PATH, {})
    if not isinstance(data, dict):
        return {}

    return {
        "company_name": data.get("company_name", "Robertsons Team"),
        "phone": data.get("phone", ""),
        "email": data.get("email", ""),
        "address": data.get("address", ""),
        "website": data.get("website", ""),
        "logo_path": data.get("logo_path", "")
    }


def get_company_logo_path():
    company_info = load_company_info()
    return company_info.get("logo_path", "")


def load_price_letter_history():
    data = load_json_file(PRICE_LETTER_HISTORY_PATH, [])
    if isinstance(data, list):
        return data
    return []


def save_price_letter_history(history):
    save_json_file(PRICE_LETTER_HISTORY_PATH, history)


def add_price_letter_history(customer_name, month_key, file_name, pdf_path, payload):
    history = load_price_letter_history()

    entry = {
        "id": f"pl_{uuid.uuid4().hex[:10]}",
        "customer_name": customer_name,
        "month_key": month_key,
        "file_name": file_name,
        "pdf_path": pdf_path,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "payload": payload,
    }

    history.insert(0, entry)
    save_price_letter_history(history)
    return entry

def build_price_letter_payload(form_data):
    """
    Builds the exact data needed to render the preview page.
    This same payload can be:
      - shown in preview
      - used to generate PDF
      - stored in history
      - reopened later
    """

    customer_name = (form_data.get("customer_name") or "").strip()
    attention = (form_data.get("attention") or "").strip()
    month_key = (form_data.get("month_key") or "").strip()
    letter_date = (form_data.get("letter_date") or "").strip()
    notes = (form_data.get("notes") or "").strip()

    company_info = load_company_info()
    logo_path = get_company_logo_path()

    # These come from repeated form rows:
    # product_name[], uom[], price[], del_fob[]
    product_names = form_data.getlist("product_name[]")
    uoms = form_data.getlist("uom[]")
    prices = form_data.getlist("price[]")
    del_fobs = form_data.getlist("del_fob[]")

    products = []
    max_len = max(len(product_names), len(uoms), len(prices), len(del_fobs), 0)

    for i in range(max_len):
        product_name = product_names[i].strip() if i < len(product_names) and product_names[i] else ""
        uom = uoms[i].strip() if i < len(uoms) and uoms[i] else ""
        price = prices[i].strip() if i < len(prices) and prices[i] else ""
        del_fob = del_fobs[i].strip() if i < len(del_fobs) and del_fobs[i] else ""

        # only keep rows that actually have something in them
        if product_name or uom or price or del_fob:
            products.append({
                "product_name": product_name,
                "uom": uom,
                "price": price,
                "del_fob": del_fob,
            })

    payload = {
        "customer_name": customer_name,
        "attention": attention,
        "month_key": month_key,
        "letter_date": letter_date,
        "notes": notes,
        "products": products,
        "company_info": company_info,
        "logo_path": logo_path,
    }

    return payload

def get_price_letter_history_item(letter_id):
    history = load_price_letter_history()
    return next((item for item in history if item.get("id") == letter_id), None)

# -------------------------
# Forms
# -------------------------
class LoginForm(FlaskForm):
    email = StringField("Email", validators=[DataRequired()])
    password = PasswordField("Password", validators=[DataRequired()])


# -------------------------
# Auth helpers
# -------------------------
def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)
    return wrapper


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login"))
        if not session.get("is_admin"):
            flash("Admin access required.", "error")
            return redirect(url_for("dashboard"))
        return view_func(*args, **kwargs)
    return wrapper


# -------------------------
# Routes
# -------------------------
@app.route("/")
@login_required
def dashboard():
    return render_template("dashboard.html", page="dashboard", page_title="Price Letter Printer")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))

    form = LoginForm()

    if form.validate_on_submit():
        email = form.email.data
        password = form.password.data

        user = find_user_by_email(email)
        if not user:
            flash("Invalid email or password.", "error")
            return render_template("login.html", form=form)

        if user["is_active"] != 1:
            flash("Your account is inactive. Contact an admin.", "error")
            return render_template("login.html", form=form)

        if not check_password_hash(user["password_hash"], password):
            flash("Invalid email or password.", "error")
            return render_template("login.html", form=form)

        session.clear()
        session["user_id"] = user["id"]
        session["email"] = user["email"]
        session["full_name"] = user["full_name"]
        session["phone"] = user["phone"]
        session["is_admin"] = user["is_admin"]
        return redirect(url_for("dashboard"))

    return render_template("login.html", form=form)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/pricing", methods=["GET", "POST"])
def pricing_page():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    cur_key = current_month_key_central()
    store.setdefault("by_month", {})
    store["by_month"].setdefault(cur_key, [])
    rows = store["by_month"][cur_key]

    errors = []
    paste_text = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "clear":
            store["by_month"][cur_key] = []
            store["last_reset_key"] = cur_key
            save_pricing_store(store)
            session.pop("pricing_pending", None)
            flash("Cleared current month list.", "success")
            return redirect(url_for("pricing_page"))

        paste_text = request.form.get("paste_data") or ""
        lines = [ln.strip() for ln in paste_text.splitlines() if ln.strip()]

        if not lines:
            errors.append("Paste area is empty.")
            return render_template(
                "pricing.html",
                month_key=cur_key,
                rows=rows,
                errors=errors,
                paste_text=paste_text,
                page="app",
                page_title="Pricing"
            )

        new_entries = []
        for i, line in enumerate(lines, start=1):
            raw = line

            if "|" in line:
                parts = [p.strip() for p in line.split("|")]
                parts = [p for p in parts if p != ""]
            elif "\t" in line:
                parts = [p.strip() for p in line.split("\t")]
            else:
                parts = [p.strip() for p in line.split(",")]

            if len(parts) != 7:
                errors.append(f"Line {i}: expected 7 columns, got {len(parts)} → {raw}")
                continue

            year, month, product, um, price, freight_tax, final_price = parts

            year = year.strip()
            month = month.strip().upper()
            product = product.strip()
            um = um.strip().upper()

            def to_float(val, field):
                try:
                    return float(str(val).replace("$", "").replace(",", "").strip())
                except Exception:
                    errors.append(f"Line {i}: '{field}' must be a number → {val}")
                    return None

            p = to_float(price, "Price")
            f = to_float(freight_tax, "Freight/Tax")
            fp = to_float(final_price, "Final Price")

            if not year or not month or not product or not um:
                errors.append(f"Line {i}: Year, Month, Product, and U/M are required → {raw}")
                continue
            if p is None or f is None or fp is None:
                continue

            entry_key = month_key_from(year, month)
            if entry_key != cur_key:
                errors.append(f"Line {i}: Month/Year ({entry_key}) does not match current list ({cur_key}).")
                continue

            new_entries.append({
                "id": uuid.uuid4().hex[:10],
                "year": year,
                "month": month,
                "product": product,
                "um": um,
                "price": p,
                "freight_tax": f,
                "final_price": fp,
                "created_at": datetime.now(timezone.utc).isoformat()
            })

        if errors:
            return render_template(
                "pricing.html",
                month_key=cur_key,
                rows=rows,
                errors=errors,
                paste_text=paste_text,
                page="app",
                page_title="Pricing"
            )

        existing = store["by_month"][cur_key]

        def _pkey(e):
            return (
                str(e.get("product", "")).strip().lower(),
                str(e.get("um", "")).strip().lower(),
            )

        def _fullsig(e):
            return (
                _pkey(e),
                float(e.get("price") or 0.0),
                float(e.get("freight_tax") or 0.0),
                float(e.get("final_price") or 0.0),
            )

        existing_full = set(_fullsig(e) for e in existing)

        existing_by_pkey = {}
        for e in existing:
            existing_by_pkey[_pkey(e)] = e

        brand_new = []
        mods = []
        ignored_exact = 0

        for e in new_entries:
            if _fullsig(e) in existing_full:
                ignored_exact += 1
                continue

            pk = _pkey(e)
            if pk in existing_by_pkey:
                mods.append(e)
            else:
                brand_new.append(e)

        if mods:
            session["pricing_pending"] = {
                "month_key": cur_key,
                "brand_new": brand_new,
                "mods": mods,
                "ignored_exact": ignored_exact
            }

            rows = store["by_month"][cur_key]
            return render_template(
                "pricing.html",
                month_key=cur_key,
                rows=rows,
                errors=[],
                paste_text=paste_text,
                show_mod_modal=True,
                new_count=len(brand_new),
                mod_count=len(mods),
                ignored_exact=ignored_exact,
                mod_products=sorted({m["product"] for m in mods}),
                page="app",
                page_title="Pricing"
            )

        if brand_new:
            store["by_month"][cur_key].extend(brand_new)
            save_pricing_store(store)

        if brand_new or ignored_exact:
            flash(f"Saved {len(brand_new)} new row(s). Ignored {ignored_exact} exact duplicate(s).", "success")
        else:
            flash("No new rows were added (all rows were exact duplicates).", "info")

        return redirect(url_for("pricing_page"))

    return render_template(
        "pricing.html",
        month_key=cur_key,
        rows=rows,
        errors=[],
        paste_text="",
        page="app",
        page_title="Pricing"
    )


@app.route("/pricing/mods/apply", methods=["POST"])
def pricing_apply_mods():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    pending = session.get("pricing_pending") or {}
    month_key = pending.get("month_key")
    brand_new = pending.get("brand_new") or []
    mods = pending.get("mods") or []

    if not month_key:
        flash("No pending pricing changes found.", "warning")
        return redirect(url_for("pricing_page"))

    store.setdefault("by_month", {})
    store["by_month"].setdefault(month_key, [])
    existing = store["by_month"][month_key]

    def _pkey(e):
        return (
            str(e.get("product", "")).strip().lower(),
            str(e.get("um", "")).strip().lower(),
        )

    decision = (request.form.get("decision") or "").strip().lower()

    if decision == "cancel":
        session.pop("pricing_pending", None)
        flash("Changes cancelled.", "info")
        return redirect(url_for("pricing_page"))

    if decision == "append_anyway":
        store["by_month"][month_key].extend(brand_new + mods)
        save_pricing_store(store)
        session.pop("pricing_pending", None)
        flash("Appended new rows + modified rows as additional entries.", "success")
        return redirect(url_for("pricing_page"))

    if decision == "replace_mods":
        mod_keys = set(_pkey(m) for m in mods)
        filtered = [e for e in existing if _pkey(e) not in mod_keys]
        filtered.extend(brand_new)
        filtered.extend(mods)
        store["by_month"][month_key] = filtered

        save_pricing_store(store)
        session.pop("pricing_pending", None)
        flash("Replaced modified products and saved changes.", "success")
        return redirect(url_for("pricing_page"))

    flash("Unknown decision.", "danger")
    return redirect(url_for("pricing_page"))


@app.route("/customers", methods=["GET", "POST"])
@login_required
def customers_page():
    customers = load_customers()
    errors = []
    name = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add":
            name = (request.form.get("customer_name") or "").strip()
            if not name:
                errors.append("Customer name is required.")
            else:
                if any((c.get("name", "").strip().lower() == name.lower()) for c in customers):
                    errors.append("That customer already exists.")

            if not errors:
                customers.append({
                    "id": uuid.uuid4().hex[:10],
                    "name": name,
                    "created_at": datetime.now(timezone.utc).isoformat()
                })
                save_customers(customers)
                flash("Customer added.", "success")
                return redirect(url_for("customers_page"))

    return render_template(
        "customers.html",
        customers=customers,
        errors=errors,
        form={"customer_name": name},
        page="app",
        page_title="Customers"
    )


@app.route("/printer", methods=["GET", "POST"])
@login_required
def printer_page():
    customers = load_customers()
    month_key, products = get_current_month_product_costs()

    errors = []
    quote_rows = []
    form = {
        "customer_id": "",
        "default_margin": "15",
        "package_type": "",
    }

    prod_map = {p["key"]: p for p in products}

    if request.method == "POST":
        action = (request.form.get("action") or "build").strip().lower()

        if action == "build":
            form["customer_id"] = (request.form.get("customer_id") or "").strip()
            form["default_margin"] = (request.form.get("default_margin") or "0").strip()

            selected_keys = request.form.getlist("product_key")
            if not form["customer_id"]:
                errors.append("Select a customer.")
            if not selected_keys:
                errors.append("Select at least one product.")
            if not products:
                errors.append("No pricing products found for the current month.")

            try:
                default_margin = float(form["default_margin"])
            except Exception:
                errors.append("Margin must be a number.")
                default_margin = 0

            if not errors:
                for k in selected_keys:
                    p = prod_map.get(k)
                    if not p:
                        continue

                    cost = float(p["cost"] or 0.0)
                    margin = float(default_margin)
                    price = cost * (1.0 + margin / 100.0)
                    shipping = 0.0
                    packaging = 0.0
                    final_price = price + shipping + packaging

                    quote_rows.append({
                        "product": p["product"],
                        "um": p["um"],
                        "package_type": "",
                        "cost": cost,
                        "margin": margin,
                        "price": price,
                        "shipping": shipping,
                        "packaging": packaging,
                        "final_price": final_price
                    })

                session["printer_draft"] = {
                    "month_key": month_key,
                    "customer_id": form["customer_id"],
                    "default_margin": default_margin,
                    "rows": quote_rows
                }

        elif action == "clear":
            session.pop("printer_draft", None)
            session.pop("print_quote", None)

            customer_id = (request.form.get("customer_id") or "").strip()

            form = {
                "customer_id": customer_id,
                "default_margin": "15",
                "package_type": "",
            }
            quote_rows = []
            customer_name = ""

            if customer_id:
                for c in customers:
                    if str(c["id"]) == customer_id:
                        customer_name = c["name"]
                        break

            return render_template(
                "printer.html",
                month_key=month_key,
                customers=customers,
                products=products,
                errors=errors,
                form=form,
                quote_rows=quote_rows,
                customer_name=customer_name,
                page="app",
                page_title="Build Letter"
            )

        elif action == "new":
            session.pop("printer_draft", None)
            session.pop("print_quote", None)

            form = {
                "customer_id": "",
                "default_margin": "15",
                "package_type": "",
            }
            quote_rows = []
            customer_name = ""

            return render_template(
                "printer.html",
                month_key=month_key,
                customers=customers,
                products=products,
                errors=errors,
                form=form,
                quote_rows=quote_rows,
                customer_name=customer_name,
                page="app",
                page_title="Build Letter"
            )

        elif action == "print":
            customer_name = (request.form.get("customer_name") or "").strip()

            row_products = request.form.getlist("row_product")
            row_finals = request.form.getlist("row_final")
            row_package_types = request.form.getlist("row_package_type")

            print_rows = []
            for i, prod in enumerate(row_products):
                prod = (prod or "").strip()

                fin = row_finals[i] if i < len(row_finals) else ""
                package_type = row_package_types[i] if i < len(row_package_types) else ""

                try:
                    fin_f = float(str(fin).replace("$", "").replace(",", "").strip())
                except Exception:
                    fin_f = 0.0

                if prod:
                    print_rows.append({
                        "product": prod,
                        "final_price": fin_f,
                        "package_type": (package_type or "").strip()
                    })

            session["print_quote"] = {
                "customer_name": customer_name,
                "month_key": month_key,
                "rows": print_rows,
                "created_at": datetime.now(timezone.utc).isoformat()
            }
            return redirect(url_for("printer_print"))

    if request.method == "GET":
        draft = session.get("printer_draft")
        if draft and draft.get("month_key") == month_key:
            form["customer_id"] = str(draft.get("customer_id") or "")
            form["default_margin"] = str(draft.get("default_margin") or "15")
            quote_rows = draft.get("rows") or []

    customer_name = ""
    if form.get("customer_id"):
        c = next((x for x in customers if str(x.get("id")) == str(form["customer_id"])), None)
        if c:
            customer_name = c.get("name", "")

    return render_template(
        "printer.html",
        month_key=month_key,
        customers=customers,
        products=products,
        quote_rows=quote_rows,
        errors=errors,
        form=form,
        customer_name=customer_name,
        page="app",
        page_title="Build Letter"
    )


@app.route("/users", methods=["GET", "POST"])
@login_required
def users_page():
    user = find_user_by_id(session["user_id"])
    errors = []

    if not user:
        session.clear()
        return redirect(url_for("login"))

    form = {
        "full_name": user["full_name"] or "",
        "email": user["email"] or "",
        "phone": user["phone"] or "",
        "password": ""
    }

    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        email = (request.form.get("email") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        password = (request.form.get("password") or "").strip()

        if not full_name:
            errors.append("Name is required.")
        if not email:
            errors.append("Email is required.")

        form = {
            "full_name": full_name,
            "email": email,
            "phone": phone,
            "password": ""
        }

        if not errors:
            try:
                update_user_profile(
                    user_id=session["user_id"],
                    full_name=full_name,
                    email=email,
                    phone=phone,
                    password=password
                )

                updated_user = find_user_by_id(session["user_id"])
                session["email"] = updated_user["email"]
                session["full_name"] = updated_user["full_name"]
                session["phone"] = updated_user["phone"]

                flash("User profile updated.", "success")
                return redirect(url_for("users_page"))
            except sqlite3.IntegrityError:
                errors.append("That email is already in use.")

    return render_template(
        "users.html",
        form=form,
        errors=errors,
        page="app",
        page_title="Users"
    )


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@admin_required
def admin_users_page():
    errors = []
    company_info = load_company_info()

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "create":
            full_name = (request.form.get("full_name") or "").strip()
            email = (request.form.get("email") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            password = (request.form.get("password") or "").strip()

            if not full_name:
                errors.append("Name is required.")
            if not email:
                errors.append("Email is required.")
            if not password:
                errors.append("Password is required.")

            if not errors:
                try:
                    create_user(
                        email=email,
                        password=password,
                        full_name=full_name,
                        phone=phone,
                        is_admin=0
                    )
                    flash("User created.", "success")
                    return redirect(url_for("admin_users_page"))
                except sqlite3.IntegrityError:
                    errors.append("That email already exists.")

        elif action == "delete":
            user_id = request.form.get("user_id")
            try:
                user_id = int(user_id)
            except Exception:
                user_id = 0

            if user_id == session["user_id"]:
                errors.append("You cannot delete your own admin account.")
            elif user_id:
                delete_user(user_id)
                flash("User removed.", "success")
                return redirect(url_for("admin_users_page"))

        elif action == "save_company":
            company_name = (request.form.get("company_name") or "").strip()
            website_url = (request.form.get("website_url") or "").strip()
            address = (request.form.get("address") or "").strip()

            company_info["company_name"] = company_name
            company_info["website_url"] = website_url
            company_info["address"] = address

            logo_file = request.files.get("logo_file")
            if logo_file and logo_file.filename:
                if allowed_logo_file(logo_file.filename):
                    filename = secure_filename(logo_file.filename)
                    ext = filename.rsplit(".", 1)[1].lower()
                    saved_name = f"company_logo.{ext}"
                    save_path = os.path.join(app.config["LOGO_UPLOAD_FOLDER"], saved_name)
                    logo_file.save(save_path)
                    company_info["logo_path"] = f"uploads/logos/{saved_name}"
                else:
                    errors.append("Logo must be a PNG, JPG, JPEG, or WEBP file.")

            if not errors:
                save_company_info(company_info)
                flash("Company information updated.", "success")
                return redirect(url_for("admin_users_page"))

    users = get_all_users()
    return render_template(
        "admin_users.html",
        users=users,
        errors=errors,
        company_info=company_info,
        page="app",
        page_title="Admin"
    )


@app.route("/printer/print")
@login_required
def printer_print():
    quote = session.get("print_quote") or {}
    company_info = load_company_info()
    user = find_user_by_id(session.get("user_id"))

    display_date = ""
    created_at = quote.get("created_at")
    if created_at:
        try:
            dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            display_date = f"{dt.month}/{dt.day}/{dt.year}"
        except Exception:
            display_date = ""

    return render_template(
        "printer_print.html",
        quote=quote,
        company_info=company_info,
        user=user,
        display_date=display_date,
        page="app",
        page_title="Print Letter"
    )


@app.route("/history/price-letter/save", methods=["POST"])
@login_required
def save_price_letter_history_route():
    data = request.get_json(silent=True) or {}

    customer_name = (data.get("customer_name") or "").strip()
    month_key = (data.get("month_key") or "").strip()
    file_name = (data.get("file_name") or "").strip()

    if not customer_name or not month_key or not file_name:
        return {"ok": False, "error": "Missing required fields."}, 400

    user = find_user_by_id(session.get("user_id"))
    created_by = ""
    created_by_email = ""

    if user:
        created_by = user["full_name"] or ""
        created_by_email = user["email"] or ""

    add_price_letter_history(
        customer_name=customer_name,
        month_key=month_key,
        created_by=created_by,
        created_by_email=created_by_email,
        file_name=file_name
    )

    return {"ok": True}


@app.route("/history")
@login_required
def history_page():
    letter_history = load_price_letter_history()

    pricing_store = load_pricing_store()
    by_month = (pricing_store.get("by_month") or {})

    product_months = []
    for key, rows in by_month.items():
        product_months.append({
            "month_key": key,
            "label": month_label_from_key(key),
            "count": len(rows or [])
        })

    product_months.sort(key=lambda x: x["month_key"], reverse=True)

    return render_template(
        "history.html",
        letter_history=letter_history,
        product_months=product_months,
        page="app",
        page_title="History"
    )


@app.route("/pricing/view/<month_key>", methods=["GET", "POST"])
@login_required
def pricing_view_page(month_key):
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    store.setdefault("by_month", {})
    rows = (store["by_month"].get(month_key) or []).copy()
    errors = []

    if request.method == "POST":
        row_ids = request.form.getlist("row_id")
        row_years = request.form.getlist("row_year")
        row_months = request.form.getlist("row_month")
        row_products = request.form.getlist("row_product")
        row_ums = request.form.getlist("row_um")
        row_prices = request.form.getlist("row_price")
        row_freights = request.form.getlist("row_freight_tax")
        row_finals = request.form.getlist("row_final_price")

        updated_rows = []

        for i, row_id in enumerate(row_ids):
            year = (row_years[i] if i < len(row_years) else "").strip()
            month = (row_months[i] if i < len(row_months) else "").strip().upper()
            product = (row_products[i] if i < len(row_products) else "").strip()
            um = (row_ums[i] if i < len(row_ums) else "").strip().upper()

            def to_float(v, label):
                try:
                    return float(str(v).replace("$", "").replace(",", "").strip())
                except Exception:
                    errors.append(f"Row {i+1}: {label} must be a number.")
                    return 0.0

            price = to_float(row_prices[i] if i < len(row_prices) else 0, "Price")
            freight_tax = to_float(row_freights[i] if i < len(row_freights) else 0, "Freight/Tax")
            final_price = to_float(row_finals[i] if i < len(row_finals) else 0, "Final Price")

            if not year or not month or not product or not um:
                errors.append(f"Row {i+1}: Year, Month, Product, and U/M are required.")

            updated_rows.append({
                "id": row_id,
                "year": year,
                "month": month,
                "product": product,
                "um": um,
                "price": price,
                "freight_tax": freight_tax,
                "final_price": final_price,
                "created_at": rows[i].get("created_at") if i < len(rows) else datetime.now(timezone.utc).isoformat()
            })

        if not errors:
            store["by_month"][month_key] = updated_rows
            save_pricing_store(store)
            flash(f"Updated pricing for {month_key}.", "success")
            return redirect(url_for("pricing_view_page", month_key=month_key))

        rows = updated_rows

    return render_template(
        "view_prices.html",
        month_key=month_key,
        rows=rows,
        errors=errors,
        page="app",
        page_title="View Pricing"
    )


@app.route("/price-letters/preview", methods=["POST"])
def price_letter_preview():
    payload = build_price_letter_payload(request.form)

    if not payload["customer_name"]:
        flash("Customer name is required.", "error")
        return redirect(url_for("printer_page"))

    if not payload["products"]:
        flash("Add at least one product row.", "error")
        return redirect(url_for("printer_page"))

    return render_template(
        "price_letter_preview.html",
        payload=payload
    )
@app.route("/price-letters/export", methods=["POST"])
def export_price_letter():
    payload = build_price_letter_payload(request.form)

    if not payload["customer_name"]:
        flash("Customer name is required.", "error")
        return redirect(url_for("printer_page"))

    if not payload["products"]:
        flash("Add at least one product row.", "error")
        return redirect(url_for("printer_page"))

    ensure_folder(PRICE_LETTER_FOLDER)

    safe_customer = "".join(c for c in payload["customer_name"] if c.isalnum() or c in (" ", "_", "-")).strip()
    safe_customer = safe_customer.replace(" ", "_") or "Customer"

    safe_month = payload["month_key"].replace(" ", "_") if payload["month_key"] else "NO_MONTH"
    unique_suffix = uuid.uuid4().hex[:6]

    file_name = f"{safe_customer}_{safe_month}_{unique_suffix}.pdf"
    pdf_path = os.path.join(PRICE_LETTER_FOLDER, file_name)

    # --------------------------------------------------
    # REPLACE THIS SECTION with your real PDF generator
    # --------------------------------------------------
    html = render_template("price_letter_pdf.html", payload=payload)

    # Example placeholder write so the route works even before PDF engine is wired:
    with open(pdf_path, "w", encoding="utf-8") as f:
        f.write(html)
    # --------------------------------------------------

    entry = add_price_letter_history(
        customer_name=payload["customer_name"],
        month_key=payload["month_key"],
        file_name=file_name,
        pdf_path=pdf_path,
        payload=payload
    )

    flash("Price letter saved successfully.", "success")
    return redirect(url_for("open_saved_price_letter", letter_id=entry["id"]))

@app.route("/price-letters/<letter_id>")
@login_required
def open_saved_price_letter(letter_id):
    history = load_price_letter_history()

    entry = next((x for x in history if x.get("id") == letter_id), None)
    if not entry:
        flash("Price letter not found.", "error")
        return redirect(url_for("history_page"))

    payload = entry.get("payload") or {
        "customer_name": entry.get("customer_name", ""),
        "attention": "",
        "month_key": entry.get("month_key", ""),
        "letter_date": "",
        "notes": "",
        "products": [],
        "company_info": {},
        "logo_path": "",
    }

    return render_template(
        "price_letter_preview.html",
        payload=payload,
        saved_letter=entry
    )

@app.route("/price-letters/<letter_id>/download")
def download_saved_price_letter(letter_id):
    entry = get_price_letter_history_item(letter_id)

    if not entry:
        flash("Saved price letter not found.", "error")
        return redirect(url_for("price_letter_history_page"))

    pdf_path = entry.get("pdf_path", "")
    file_name = entry.get("file_name", "price_letter.pdf")

    if not pdf_path or not os.path.exists(pdf_path):
        flash("PDF file is missing.", "error")
        return redirect(url_for("price_letter_history_page"))

    return send_file(pdf_path, as_attachment=True, download_name=file_name)

@app.route("/price-letters/history")
def price_letter_history_page():
    history = load_price_letter_history()

    return render_template(
        "price_letter_history.html",
        history=history
    )
# -------------------------
# First-run setup
# -------------------------
if __name__ == "__main__":
    init_db()

    try:
        if not find_user_by_email("csalas@robertsonsteam.com"):
            create_user(
                email="csalas@robertsonsteam.com",
                password="password",
                full_name="Cesar Salas",
                phone="",
                is_admin=1
            )
            print("Created initial admin user: csalas@robertsonsteam.com")
    except sqlite3.IntegrityError:
        pass

    app.run(debug=True)