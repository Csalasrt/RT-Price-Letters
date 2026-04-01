import os
import sqlite3
from functools import wraps
import json
import uuid
import re
from datetime import datetime, timezone, timedelta


from flask import Flask, render_template, request, redirect, url_for, session, flash, send_file
from flask_wtf import FlaskForm
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from werkzeug.datastructures import MultiDict
from wtforms import StringField, PasswordField
from wtforms.validators import DataRequired
from flask import flash
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate



PRICING_STORE_PATH = os.path.join("data", "pricing_entries.json")
CUSTOMERS_STORE_PATH = os.path.join("data", "customers.json")
COMPANY_STORE_PATH = os.path.join("data", "company_info.json")
LOGO_UPLOAD_FOLDER = os.path.join("static", "uploads", "logos")
PRICE_LETTER_HISTORY_PATH = os.path.join("data", "price_letter_history.json")
PRICE_LETTER_FOLDER = os.path.join("static", "price_letters")
COMPANY_PRODUCTS_PATH = os.path.join("data", "company_products.json")
SALES_PEOPLE_PATH = os.path.join("data", "sales_people.json")
TODO_LIST_PATH = os.path.join("data", "monthly_todos.json")
TODO_CONFIG_PATH = os.path.join("data", "todo_config.json")
MARGIN_HISTORY_PATH = os.path.join("data", "margin_history.json")


# -------------------------
# Config
# -------------------------
app = Flask(__name__)

IS_PROD = os.environ.get("RENDER") == "true" or os.environ.get("FLASK_ENV") == "production"

secret_key = os.environ.get("SECRET_KEY")
database_url = os.environ.get("DATABASE_URL")

if IS_PROD and not secret_key:
    raise RuntimeError("SECRET_KEY is required in production.")

if IS_PROD and not database_url:
    raise RuntimeError("DATABASE_URL is required in production.")

app.config["SECRET_KEY"] = secret_key or "dev-only-change-me"
app.config["SESSION_COOKIE_HTTPONLY"] = True
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
app.config["SESSION_COOKIE_SECURE"] = IS_PROD
app.config["LOGO_UPLOAD_FOLDER"] = LOGO_UPLOAD_FOLDER

app.config["SQLALCHEMY_DATABASE_URI"] = database_url or "sqlite:///local_dev.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

trusted_hosts = os.environ.get("TRUSTED_HOSTS", "").strip()
if trusted_hosts:
    app.config["TRUSTED_HOSTS"] = [h.strip() for h in trusted_hosts.split(",") if h.strip()]

db = SQLAlchemy(app)
migrate = Migrate(app, db)

os.makedirs(app.config["LOGO_UPLOAD_FOLDER"], exist_ok=True)
os.makedirs(PRICE_LETTER_FOLDER, exist_ok=True)
# -------------------------
# Database Models
# -------------------------
class User(db.Model):
    __tablename__ = "users"

    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.Text, nullable=False)
    full_name = db.Column(db.String(255), nullable=False, default="")
    phone = db.Column(db.String(50), nullable=False, default="")
    is_admin = db.Column(db.Boolean, nullable=False, default=False)
    is_active = db.Column(db.Boolean, nullable=False, default=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)


class CompanyInfo(db.Model):
    __tablename__ = "company_info"

    id = db.Column(db.Integer, primary_key=True)
    company_name = db.Column(db.String(255), default="")
    website_url = db.Column(db.String(500), default="")
    address = db.Column(db.Text, default="")
    logo_path = db.Column(db.String(500), default="")


class CompanyProduct(db.Model):
    __tablename__ = "company_products"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    product = db.Column(db.String(255), nullable=False)
    lb_per_gal = db.Column(db.Float, default=0.0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


class SalesPerson(db.Model):
    __tablename__ = "sales_people"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    name = db.Column(db.String(255), default="")
    email = db.Column(db.String(255), default="")
    phone = db.Column(db.String(50), default="")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Customer(db.Model):
    __tablename__ = "customers"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    name = db.Column(db.String(255), nullable=False, unique=True, index=True)
    notes = db.Column(db.Text, nullable=False, default="")
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

    default_rows = db.relationship(
        "CustomerDefaultRow",
        backref="customer",
        cascade="all, delete-orphan",
        lazy=True,
        order_by="CustomerDefaultRow.sort_order.asc()"
    )


class CustomerDefaultRow(db.Model):
    __tablename__ = "customer_default_rows"

    id = db.Column(db.Integer, primary_key=True)
    customer_id = db.Column(db.String(32), db.ForeignKey("customers.id"), nullable=False, index=True)
    sort_order = db.Column(db.Integer, nullable=False, default=0)

    product = db.Column(db.String(255), nullable=False, default="")
    package_type = db.Column(db.String(100), nullable=False, default="")
    um = db.Column(db.String(20), nullable=False, default="")
    margin = db.Column(db.Float, nullable=False, default=15.0)
    shipping = db.Column(db.Float, nullable=False, default=0.0)
    packaging = db.Column(db.Float, nullable=False, default=0.0)

class PricingEntry(db.Model):
    __tablename__ = "pricing_entries"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    month_key = db.Column(db.String(20), nullable=False, index=True)

    product = db.Column(db.String(255), nullable=False, index=True)
    um = db.Column(db.String(20), nullable=False, default="")

    price = db.Column(db.Float, nullable=False, default=0.0)
    freight_tax = db.Column(db.Float, nullable=False, default=0.0)
    final_price = db.Column(db.Float, nullable=False, default=0.0)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class PriceLetterHistory(db.Model):
    __tablename__ = "price_letter_history"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex)
    customer_name = db.Column(db.String(255), nullable=False, default="")
    customer_id = db.Column(db.String(32), nullable=False, default="", index=True)
    month_key = db.Column(db.String(20), nullable=False, default="", index=True)

    file_name = db.Column(db.String(500), nullable=False, default="")
    file_path = db.Column(db.String(1000), nullable=False, default="")

    created_by = db.Column(db.String(255), nullable=False, default="")
    created_by_email = db.Column(db.String(255), nullable=False, default="", index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)

    sales_person_name = db.Column(db.String(255), nullable=False, default="")
    sales_person_phone = db.Column(db.String(50), nullable=False, default="")
    sales_person_email = db.Column(db.String(255), nullable=False, default="")

    quote_json = db.Column(db.Text, nullable=False, default="{}")

class TodoRecurringCustomer(db.Model):
    __tablename__ = "todo_recurring_customers"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    user_id = db.Column(db.Integer, nullable=False, index=True)
    customer_id = db.Column(db.String(32), nullable=False, index=True)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class TodoItem(db.Model):
    __tablename__ = "todo_items"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex[:10])
    user_id = db.Column(db.Integer, nullable=False, index=True)
    month_key = db.Column(db.String(20), nullable=False, index=True)
    customer_id = db.Column(db.String(32), nullable=False, index=True)

    done = db.Column(db.Boolean, nullable=False, default=False)
    done_at = db.Column(db.DateTime, nullable=True)

    history_id = db.Column(db.String(32), nullable=False, default="", index=True)
    file_name = db.Column(db.String(500), nullable=False, default="")

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)

class MarginHistoryRecord(db.Model):
    __tablename__ = "margin_history_records"

    id = db.Column(db.String(32), primary_key=True, default=lambda: uuid.uuid4().hex)
    product = db.Column(db.String(255), nullable=False, default="", index=True)
    pricing_date = db.Column(db.String(20), nullable=False, default="", index=True)
    entry_seq = db.Column(db.Integer, nullable=False, default=1)

    source = db.Column(db.String(100), nullable=False, default="")
    customer = db.Column(db.String(255), nullable=False, default="")
    um = db.Column(db.String(20), nullable=False, default="")
    package_type = db.Column(db.String(100), nullable=False, default="")

    cost = db.Column(db.Float, nullable=False, default=0.0)
    margin_pct = db.Column(db.Float, nullable=False, default=0.0)
    price = db.Column(db.Float, nullable=False, default=0.0)
    shipping = db.Column(db.Float, nullable=False, default=0.0)
    packaging = db.Column(db.Float, nullable=False, default=0.0)
    final_price = db.Column(db.Float, nullable=False, default=0.0)

    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, index=True)
    created_by = db.Column(db.String(255), nullable=False, default="")
    created_by_name = db.Column(db.String(255), nullable=False, default="")

# -------------------------
# Helpers
# -------------------------
def init_db():
    with app.app_context():
        db.create_all()


def create_user(email: str, password: str, full_name: str = "", phone: str = "", is_admin: int = 0):
    user = User(
        email=email.lower().strip(),
        password_hash=generate_password_hash(password),
        full_name=full_name.strip(),
        phone=phone.strip(),
        is_admin=bool(is_admin),
        is_active=True,
    )
    db.session.add(user)
    db.session.commit()


def find_user_by_email(email: str):
    return User.query.filter_by(email=email.lower().strip()).first()


def find_user_by_id(user_id: int):
    return db.session.get(User, user_id)


def get_all_users():
    return User.query.order_by(User.full_name.asc(), User.email.asc()).all()


def update_user_profile(user_id: int, full_name: str, email: str, phone: str, password: str = ""):
    user = db.session.get(User, user_id)
    if not user:
        return

    user.full_name = full_name.strip()
    user.email = email.lower().strip()
    user.phone = phone.strip()

    if password.strip():
        user.password_hash = generate_password_hash(password.strip())

    db.session.commit()


def delete_user(user_id: int):
    user = db.session.get(User, user_id)
    if not user:
        return
    db.session.delete(user)
    db.session.commit()

def _ensure_data_dir():
    os.makedirs(os.path.dirname(PRICING_STORE_PATH), exist_ok=True)


def load_pricing_store():
    rows = PricingEntry.query.order_by(
        PricingEntry.month_key.asc(),
        PricingEntry.created_at.asc(),
        PricingEntry.product.asc(),
        PricingEntry.um.asc()
    ).all()

    by_month = {}

    for r in rows:
        mk = (r.month_key or "").strip()
        by_month.setdefault(mk, []).append({
            "id": r.id,
            "product": r.product or "",
            "um": r.um or "",
            "price": float(r.price or 0.0),
            "freight_tax": float(r.freight_tax or 0.0),
            "final_price": float(r.final_price or 0.0),
            "created_at": r.created_at.isoformat() if r.created_at else datetime.now(timezone.utc).isoformat(),
        })

    return {
        "last_reset_key": None,
        "by_month": by_month
    }


def save_pricing_store(store: dict):
    PricingEntry.query.delete()

    by_month = (store or {}).get("by_month") or {}

    for month_key, rows in by_month.items():
        for r in rows or []:
            product = (r.get("product") or "").strip()
            um = (r.get("um") or "").strip().upper()

            if not product:
                continue

            created_at_raw = r.get("created_at")
            created_at = datetime.utcnow()
            if created_at_raw:
                try:
                    created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
                    if created_at.tzinfo is not None:
                        created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
                except Exception:
                    created_at = datetime.utcnow()

            db.session.add(PricingEntry(
                id=r.get("id") or uuid.uuid4().hex[:10],
                month_key=str(month_key or "").strip(),
                product=product,
                um=um,
                price=to_float(r.get("price", 0.0), 0.0),
                freight_tax=to_float(r.get("freight_tax", 0.0), 0.0),
                final_price=to_float(r.get("final_price", 0.0), 0.0),
                created_at=created_at,
            ))

    db.session.commit()


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
    return store

def load_customers():
    customers = Customer.query.order_by(Customer.name.asc()).all()

    result = []
    for c in customers:
        default_letter_rows = []
        default_products = []

        for row in c.default_rows:
            row_dict = {
                "product": row.product or "",
                "package_type": row.package_type or "",
                "um": row.um or "",
                "margin": round(float(row.margin or 0.0), 4),
                "shipping": round(float(row.shipping or 0.0), 4),
                "packaging": round(float(row.packaging or 0.0), 4),
            }
            default_letter_rows.append(row_dict)

            product_name = (row.product or "").strip()
            if product_name:
                default_products.append(product_name)

        default_products = _clean_default_products(default_products)

        result.append({
            "id": c.id,
            "name": c.name or "",
            "notes": c.notes or "",
            "default_products": default_products,
            "default_letter_rows": default_letter_rows,
            "created_at": c.created_at.isoformat() if c.created_at else datetime.now(timezone.utc).isoformat(),
        })

    return result


def save_customers(customers: list):
    CustomerDefaultRow.query.delete()
    Customer.query.delete()

    for c in customers or []:
        name = (c.get("name") or "").strip()
        if not name:
            continue

        created_at_raw = c.get("created_at")
        created_at = datetime.utcnow()
        if created_at_raw:
            try:
                created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
                if created_at.tzinfo is not None:
                    created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                created_at = datetime.utcnow()

        customer = Customer(
            id=c.get("id") or uuid.uuid4().hex[:10],
            name=name,
            notes=(c.get("notes") or "").strip(),
            created_at=created_at,
        )
        db.session.add(customer)
        db.session.flush()

        default_rows = _clean_default_letter_rows(c.get("default_letter_rows") or [])

        if not default_rows:
            fallback_products = _clean_default_products(c.get("default_products") or [])
            default_rows = [
                {
                    "product": p,
                    "package_type": "",
                    "um": "",
                    "margin": 15.0,
                    "shipping": 0.0,
                    "packaging": 0.0,
                }
                for p in fallback_products
            ]

        for i, row in enumerate(default_rows):
            db.session.add(CustomerDefaultRow(
                customer_id=customer.id,
                sort_order=i,
                product=(row.get("product") or "").strip(),
                package_type=(row.get("package_type") or "").strip(),
                um=normalize_um(row.get("um", "")),
                margin=to_float(row.get("margin", 15.0), 15.0),
                shipping=to_float(row.get("shipping", 0.0), 0.0),
                packaging=to_float(row.get("packaging", 0.0), 0.0),
            ))

    db.session.commit()


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
    info = CompanyInfo.query.first()

    if not info:
        info = CompanyInfo()
        db.session.add(info)
        db.session.commit()

    return {
        "company_name": info.company_name or "",
        "website_url": info.website_url or "",
        "address": info.address or "",
        "logo_path": info.logo_path or "",
    }


def save_company_info(company_info: dict):
    info = CompanyInfo.query.first()

    if not info:
        info = CompanyInfo()
        db.session.add(info)

    info.company_name = company_info.get("company_name", "")
    info.website_url = company_info.get("website_url", "")
    info.address = company_info.get("address", "")
    info.logo_path = company_info.get("logo_path", "")

    db.session.commit()

def allowed_logo_file(filename):
    if "." not in filename:
        return False
    ext = filename.rsplit(".", 1)[1].lower()
    return ext in {"png", "jpg", "jpeg", "webp"}


def _price_letter_history_row_to_dict(row):
    if not row:
        return {}

    try:
        quote = json.loads(row.quote_json or "{}")
        if not isinstance(quote, dict):
            quote = {}
    except Exception:
        quote = {}

    entry = dict(quote)

    entry["id"] = row.id
    entry["customer_name"] = row.customer_name or entry.get("customer_name", "")
    entry["customer_id"] = row.customer_id or entry.get("customer_id", "")
    entry["month_key"] = row.month_key or entry.get("month_key", "")
    entry["file_name"] = row.file_name or entry.get("file_name", "")
    entry["file_path"] = row.file_path or entry.get("file_path", "")
    entry["created_by"] = row.created_by or entry.get("created_by", "")
    entry["created_by_email"] = row.created_by_email or entry.get("created_by_email", "")
    entry["created_at"] = row.created_at.isoformat() if row.created_at else entry.get("created_at", "")
    entry["sales_person_name"] = row.sales_person_name or entry.get("sales_person_name", "")
    entry["sales_person_phone"] = row.sales_person_phone or entry.get("sales_person_phone", "")
    entry["sales_person_email"] = row.sales_person_email or entry.get("sales_person_email", "")

    return entry


def load_price_letter_history(limit=100):
    rows = (
        PriceLetterHistory.query
        .order_by(PriceLetterHistory.created_at.desc())
        .limit(limit)
        .all()
    )
    return [_price_letter_history_row_to_dict(r) for r in rows]


def get_price_letter_history_entry(entry_id):
    row = db.session.get(PriceLetterHistory, str(entry_id))
    return _price_letter_history_row_to_dict(row) if row else None


def add_price_letter_history(entry: dict):
    entry = dict(entry or {})

    created_at = datetime.utcnow()
    created_at_raw = entry.get("created_at")
    if created_at_raw:
        try:
            created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
            if created_at.tzinfo is not None:
                created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            created_at = datetime.utcnow()

    row = PriceLetterHistory(
        id=str(entry.get("id") or uuid.uuid4().hex),
        customer_name=str(entry.get("customer_name") or "").strip(),
        customer_id=str(entry.get("customer_id") or "").strip(),
        month_key=str(entry.get("month_key") or "").strip().upper(),
        file_name=str(entry.get("file_name") or "").strip(),
        file_path=str(entry.get("file_path") or "").strip(),
        created_by=str(entry.get("created_by") or "").strip(),
        created_by_email=str(entry.get("created_by_email") or "").strip(),
        created_at=created_at,
        sales_person_name=str(entry.get("sales_person_name") or "").strip(),
        sales_person_phone=str(entry.get("sales_person_phone") or "").strip(),
        sales_person_email=str(entry.get("sales_person_email") or "").strip(),
        quote_json=json.dumps(entry),
    )

    db.session.add(row)
    db.session.commit()
    return _price_letter_history_row_to_dict(row)


def migrate_price_letter_history_json_to_db():
    if not os.path.exists(PRICE_LETTER_HISTORY_PATH):
        return 0

    try:
        with open(PRICE_LETTER_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    if not isinstance(data, list):
        return 0

    inserted = 0

    for entry in data:
        entry = dict(entry or {})
        entry_id = str(entry.get("id") or "").strip()

        if not entry_id:
            entry["id"] = uuid.uuid4().hex
            entry_id = entry["id"]

        existing = db.session.get(PriceLetterHistory, entry_id)
        if existing:
            continue

        add_price_letter_history(entry)
        inserted += 1

    return inserted

def _margin_history_row_to_dict(row):
    if not row:
        return {}

    return {
        "id": row.id,
        "product": row.product or "",
        "pricing_date": row.pricing_date or "",
        "entry_seq": int(row.entry_seq or 0),
        "source": row.source or "",
        "customer": row.customer or "",
        "um": row.um or "",
        "package_type": row.package_type or "",
        "cost": round(float(row.cost or 0.0), 4),
        "margin_pct": round(float(row.margin_pct or 0.0), 4),
        "price": round(float(row.price or 0.0), 4),
        "shipping": round(float(row.shipping or 0.0), 4),
        "packaging": round(float(row.packaging or 0.0), 4),
        "final_price": round(float(row.final_price or 0.0), 4),
        "created_at": row.created_at.isoformat() if row.created_at else "",
        "created_by": row.created_by or "",
        "created_by_name": row.created_by_name or "",
    }


def load_margin_history():
    rows = (
        MarginHistoryRecord.query
        .order_by(
            MarginHistoryRecord.product.asc(),
            MarginHistoryRecord.pricing_date.asc(),
            MarginHistoryRecord.entry_seq.asc(),
            MarginHistoryRecord.created_at.asc()
        )
        .all()
    )

    return {
        "records": [_margin_history_row_to_dict(r) for r in rows]
    }


def save_margin_history(store):
    MarginHistoryRecord.query.delete()

    records = (store or {}).get("records") or []
    for record in records:
        created_at = datetime.utcnow()
        created_at_raw = record.get("created_at")
        if created_at_raw:
            try:
                created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
                if created_at.tzinfo is not None:
                    created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                created_at = datetime.utcnow()

        db.session.add(MarginHistoryRecord(
            id=str(record.get("id") or uuid.uuid4().hex),
            product=str(record.get("product") or "").strip(),
            pricing_date=normalize_pricing_date(record.get("pricing_date")),
            entry_seq=int(record.get("entry_seq") or 1),
            source=str(record.get("source") or "").strip(),
            customer=str(record.get("customer") or "").strip(),
            um=normalize_um(record.get("um", "")),
            package_type=str(record.get("package_type") or "").strip(),
            cost=to_float(record.get("cost", 0.0), 0.0),
            margin_pct=to_float(record.get("margin_pct", 0.0), 0.0),
            price=to_float(record.get("price", 0.0), 0.0),
            shipping=to_float(record.get("shipping", 0.0), 0.0),
            packaging=to_float(record.get("packaging", 0.0), 0.0),
            final_price=to_float(record.get("final_price", 0.0), 0.0),
            created_at=created_at,
            created_by=str(record.get("created_by") or "").strip(),
            created_by_name=str(record.get("created_by_name") or "").strip(),
        ))

    db.session.commit()


def normalize_pricing_date(value):
    """
    Accepts:
      - YYYY-MM-DD
      - M/D/YYYY
      - MM/DD/YYYY
    Returns YYYY-MM-DD or "".
    """
    raw = str(value or "").strip()
    if not raw:
        return ""

    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass

    return ""


def pricing_date_from_month_key(month_key):
    """
    Uses the same business logic as your print page:
    - if not current month, use first day of selected month
    - if current month, use today's date
    """
    display_date = display_date_from_month_key(month_key)
    return normalize_pricing_date(display_date)


def next_margin_entry_seq(product, pricing_date):
    product_key = normalize_product_name(product)
    pricing_date = normalize_pricing_date(pricing_date)

    rows = (
        MarginHistoryRecord.query
        .filter(MarginHistoryRecord.pricing_date == pricing_date)
        .order_by(MarginHistoryRecord.entry_seq.asc())
        .all()
    )

    same_day_rows = [
        r for r in rows
        if normalize_product_name(r.product) == product_key
    ]

    if not same_day_rows:
        return 1

    max_seq = 0
    for r in same_day_rows:
        try:
            seq = int(r.entry_seq or 0)
        except Exception:
            seq = 0
        if seq > max_seq:
            max_seq = seq

    return max_seq + 1


def build_margin_history_record(
    *,
    product,
    pricing_date,
    source,
    customer="",
    um="",
    package_type="",
    cost=0.0,
    margin_pct=0.0,
    price=0.0,
    shipping=0.0,
    packaging=0.0,
    final_price=0.0,
    created_by="",
    created_by_name=""
):
    pricing_date = normalize_pricing_date(pricing_date)

    return {
        "id": uuid.uuid4().hex,
        "product": str(product or "").strip(),
        "pricing_date": pricing_date,
        "entry_seq": next_margin_entry_seq(product, pricing_date),
        "source": str(source or "").strip(),
        "customer": str(customer or "").strip(),
        "um": normalize_um(um),
        "package_type": str(package_type or "").strip(),
        "cost": round(to_float(cost, 0.0), 4),
        "margin_pct": round(to_float(margin_pct, 0.0), 4),
        "price": round(to_float(price, 0.0), 4),
        "shipping": round(to_float(shipping, 0.0), 4),
        "packaging": round(to_float(packaging, 0.0), 4),
        "final_price": round(to_float(final_price, 0.0), 4),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "created_by": str(created_by or "").strip(),
        "created_by_name": str(created_by_name or "").strip(),
    }


def append_margin_history_record(record):
    if not isinstance(record, dict):
        return

    created_at = datetime.utcnow()
    created_at_raw = record.get("created_at")
    if created_at_raw:
        try:
            created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
            if created_at.tzinfo is not None:
                created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception:
            created_at = datetime.utcnow()

    row = MarginHistoryRecord(
        id=str(record.get("id") or uuid.uuid4().hex),
        product=str(record.get("product") or "").strip(),
        pricing_date=normalize_pricing_date(record.get("pricing_date")),
        entry_seq=int(record.get("entry_seq") or 1),
        source=str(record.get("source") or "").strip(),
        customer=str(record.get("customer") or "").strip(),
        um=normalize_um(record.get("um", "")),
        package_type=str(record.get("package_type") or "").strip(),
        cost=to_float(record.get("cost", 0.0), 0.0),
        margin_pct=to_float(record.get("margin_pct", 0.0), 0.0),
        price=to_float(record.get("price", 0.0), 0.0),
        shipping=to_float(record.get("shipping", 0.0), 0.0),
        packaging=to_float(record.get("packaging", 0.0), 0.0),
        final_price=to_float(record.get("final_price", 0.0), 0.0),
        created_at=created_at,
        created_by=str(record.get("created_by") or "").strip(),
        created_by_name=str(record.get("created_by_name") or "").strip(),
    )

    db.session.add(row)
    db.session.commit()

def migrate_margin_history_json_to_db():
    if not os.path.exists(MARGIN_HISTORY_PATH):
        return 0

    try:
        with open(MARGIN_HISTORY_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    records = data.get("records", []) if isinstance(data, dict) else []
    inserted = 0

    for record in records:
        record = dict(record or {})
        record_id = str(record.get("id") or "").strip()

        if not record_id:
            record_id = uuid.uuid4().hex
            record["id"] = record_id

        existing = db.session.get(MarginHistoryRecord, record_id)
        if existing:
            continue

        created_at = datetime.utcnow()
        created_at_raw = record.get("created_at")
        if created_at_raw:
            try:
                created_at = datetime.fromisoformat(str(created_at_raw).replace("Z", "+00:00"))
                if created_at.tzinfo is not None:
                    created_at = created_at.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                created_at = datetime.utcnow()

        db.session.add(MarginHistoryRecord(
            id=record_id,
            product=str(record.get("product") or "").strip(),
            pricing_date=normalize_pricing_date(record.get("pricing_date")),
            entry_seq=int(record.get("entry_seq") or 1),
            source=str(record.get("source") or "").strip(),
            customer=str(record.get("customer") or "").strip(),
            um=normalize_um(record.get("um", "")),
            package_type=str(record.get("package_type") or "").strip(),
            cost=to_float(record.get("cost", 0.0), 0.0),
            margin_pct=to_float(record.get("margin_pct", 0.0), 0.0),
            price=to_float(record.get("price", 0.0), 0.0),
            shipping=to_float(record.get("shipping", 0.0), 0.0),
            packaging=to_float(record.get("packaging", 0.0), 0.0),
            final_price=to_float(record.get("final_price", 0.0), 0.0),
            created_at=created_at,
            created_by=str(record.get("created_by") or "").strip(),
            created_by_name=str(record.get("created_by_name") or "").strip(),
        ))
        inserted += 1

    db.session.commit()
    return inserted

def is_meaningful_margin_record(record):
    """
    Use this later on the charts page.
    """
    return (
        to_float(record.get("cost"), 0.0) > 0
        and to_float(record.get("final_price"), 0.0) > 0
    )

def _user_to_snapshot(user_row):
    if not user_row:
        return {}

    if isinstance(user_row, dict):
        return {
            "email": str(user_row.get("email") or "").strip(),
            "full_name": str(user_row.get("full_name") or "").strip(),
        }

    return {
        "email": str(getattr(user_row, "email", "") or "").strip(),
        "full_name": str(getattr(user_row, "full_name", "") or "").strip(),
    }

def save_price_letter_rows_to_margin_history(quote, user_row=None):
    if not quote:
        return

    user_data = _user_to_snapshot(user_row)

    pricing_date = pricing_date_from_month_key(quote.get("month_key"))
    customer_name = str(quote.get("customer_name") or "").strip()
    rows = quote.get("rows") or []

    created_by = str(user_data.get("email") or "").strip()
    created_by_name = str(user_data.get("full_name") or "").strip()

    for row in rows:
        product = str(row.get("product") or "").strip()
        um = normalize_um(row.get("um", ""))
        cost = to_float(row.get("cost", 0.0), 0.0)
        final_price = to_float(row.get("final_price", 0.0), 0.0)

        if not product:
            continue
        if not pricing_date:
            continue
        if not um:
            continue
        if cost <= 0:
            continue
        if final_price <= 0:
            continue

        record = build_margin_history_record(
            product=product,
            pricing_date=pricing_date,
            source="price_letter_final",
            customer=customer_name,
            um=um,
            package_type=row.get("package_type", ""),
            cost=cost,
            margin_pct=to_float(row.get("margin", 0.0), 0.0),
            price=to_float(row.get("price", 0.0), 0.0),
            shipping=to_float(row.get("shipping", 0.0), 0.0),
            packaging=to_float(row.get("packaging", 0.0), 0.0),
            final_price=final_price,
            created_by=created_by,
            created_by_name=created_by_name,
        )
        append_margin_history_record(record)


def save_reverse_margin_rows_to_history(
    *,
    customer_name,
    pricing_date,
    rows,
    user_row=None
):
    user_data = _user_to_snapshot(user_row)
    created_by = str(user_data.get("email") or "").strip()
    created_by_name = str(user_data.get("full_name") or "").strip()

    for row in rows or []:
        product = str(row.get("product") or "").strip()
        um = normalize_um(row.get("um", ""))
        cost = to_float(row.get("historical_cost", 0.0), 0.0)
        final_price = to_float(row.get("final_price", 0.0), 0.0)

        if not product:
            continue
        if not customer_name:
            continue
        if not pricing_date:
            continue
        if not um:
            continue
        if cost <= 0:
            continue
        if final_price <= 0:
            continue

        margin_pct = to_float(
            row.get("reverse_margin_pct", row.get("margin", 0.0)),
            0.0
        )
        price = to_float(row.get("price", 0.0), 0.0)
        shipping = to_float(row.get("shipping", 0.0), 0.0)
        packaging = to_float(row.get("packaging", 0.0), 0.0)

        record = build_margin_history_record(
            product=product,
            pricing_date=pricing_date,
            source="reverse_margin_saved",
            customer=customer_name,
            um=um,
            package_type=row.get("package_type", ""),
            cost=cost,
            margin_pct=margin_pct,
            price=price,
            shipping=shipping,
            packaging=packaging,
            final_price=final_price,
            created_by=created_by,
            created_by_name=created_by_name,
        )
        append_margin_history_record(record)

def user_snapshot_from_id(user_id):
    row = find_user_by_id(user_id)
    if not row:
        return {}
    return {
        "id": row.id,
        "full_name": row.full_name or "",
        "email": row.email or "",
        "phone": row.phone or "",
    }


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

def display_date_from_month_key(month_key: str) -> str:
    month_key = str(month_key or "").strip().upper()

    month_num_map = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }

    today = datetime.now()
    current_month_key = f"{today.year}-{today.strftime('%b').upper()}"

    if month_key and month_key != current_month_key:
        try:
            year_part, mon_part = month_key.split("-", 1)
            month_num = month_num_map.get(mon_part)
            year_num = int(year_part)

            if month_num:
                return f"{month_num}/1/{year_num}"
        except Exception:
            pass

    return f"{today.month}/{today.day}/{today.year}"

def _month_sort_value(month_abbr: str) -> int:
    month_order = {
        "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
        "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
        "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
    }
    return month_order.get((month_abbr or "").strip().upper(), 0)


def get_available_pricing_periods():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    by_month = store.get("by_month") or {}

    month_order = [
        ("JAN", "January"),
        ("FEB", "February"),
        ("MAR", "March"),
        ("APR", "April"),
        ("MAY", "May"),
        ("JUN", "June"),
        ("JUL", "July"),
        ("AUG", "August"),
        ("SEP", "September"),
        ("OCT", "October"),
        ("NOV", "November"),
        ("DEC", "December"),
    ]

    years = set()

    # Always include the current year so all 12 months show up
    current_year = str(datetime.now().year)
    years.add(current_year)

    # Also include any years that already exist in pricing history
    for month_key in by_month.keys():
        if "-" in str(month_key):
            years.add(str(month_key).split("-")[0])

    periods = []
    for year in years:
        for month_abbr, month_name in month_order:
            periods.append({
                "value": f"{year}-{month_abbr}",
                "label": f"{month_name} {year}"
            })

    periods.sort(
        key=lambda x: (
            int(str(x["value"]).split("-")[0]) if "-" in str(x["value"]) else 0,
            _month_sort_value(str(x["value"]).split("-")[1] if "-" in str(x["value"]) else "")
        )
    )

    return periods


def get_product_costs_for_month(month_key: str):
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    rows = (store.get("by_month") or {}).get(month_key, []) or []

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
    return month_key, products


def get_printer_product_options(month_key: str):
    month_key, priced_products = get_product_costs_for_month(month_key)
    company_products = load_company_products() or []

    options = []
    seen = set()

    for item in company_products:
        product_name = (item.get("product") or "").strip()
        if not product_name:
            continue

        key = normalize_product_name(product_name)
        if key in seen:
            continue
        seen.add(key)

        try:
            lb_per_gal = float(item.get("lb_per_gal") or 0.0)
        except Exception:
            lb_per_gal = 0.0

        options.append({
            "key": f"name::{product_name}",
            "product": product_name,
            "um": "",
            "cost": 0.0,
            "lb_per_gal": lb_per_gal,
            "is_priced": False
        })

    for p in priced_products:
        product_name = (p.get("product") or "").strip()
        if not product_name:
            continue

        key = normalize_product_name(product_name)

        replaced = False
        for i, existing in enumerate(options):
            if normalize_product_name(existing.get("product")) == key:
                options[i] = {
                    "key": p["key"],
                    "product": p.get("product", ""),
                    "um": p.get("um", ""),
                    "cost": float(p.get("cost") or 0.0),
                    "lb_per_gal": float(existing.get("lb_per_gal") or 0.0),
                    "is_priced": True
                }
                replaced = True
                break

        if not replaced:
            if key in seen:
                continue
            seen.add(key)
            options.append({
                "key": p["key"],
                "product": p.get("product", ""),
                "um": p.get("um", ""),
                "cost": float(p.get("cost") or 0.0),
                "lb_per_gal": 0.0,
                "is_priced": True
            })

    options.sort(key=lambda x: normalize_product_name(x.get("product")))
    return month_key, options, priced_products
def normalize_product_name(name: str) -> str:
    return " ".join(str(name or "").strip().lower().split())


def load_company_products():
    products = CompanyProduct.query.order_by(CompanyProduct.product.asc()).all()

    return [
        {
            "id": p.id,
            "product": p.product or "",
            "lb_per_gal": float(p.lb_per_gal or 0.0),
            "created_at": p.created_at.isoformat() if p.created_at else datetime.now(timezone.utc).isoformat()
        }
        for p in products
    ]


def save_company_products(products):
    CompanyProduct.query.delete()

    cleaned = []
    seen = set()

    for item in products or []:
        if isinstance(item, str):
            product_name = item.strip()
            if not product_name:
                continue
            key = normalize_product_name(product_name)
            if key in seen:
                continue
            seen.add(key)

            cleaned.append(
                CompanyProduct(
                    id=uuid.uuid4().hex[:10],
                    product=product_name,
                    lb_per_gal=0.0
                )
            )
            continue

        if isinstance(item, dict):
            product_name = (item.get("product") or item.get("name") or "").strip()
            if not product_name:
                continue

            key = normalize_product_name(product_name)
            if key in seen:
                continue
            seen.add(key)

            try:
                lb_per_gal = float(item.get("lb_per_gal") or 0.0)
            except Exception:
                lb_per_gal = 0.0

            cleaned.append(
                CompanyProduct(
                    id=item.get("id") or uuid.uuid4().hex[:10],
                    product=product_name,
                    lb_per_gal=lb_per_gal
                )
            )

    for row in cleaned:
        db.session.add(row)

    db.session.commit()




def find_customer_by_id(customer_id):
    c = db.session.get(Customer, str(customer_id))
    if not c:
        return None

    default_letter_rows = []
    default_products = []

    for row in sorted(c.default_rows, key=lambda r: r.sort_order or 0):
        row_dict = {
            "product": row.product or "",
            "package_type": row.package_type or "",
            "um": row.um or "",
            "margin": round(float(row.margin or 0.0), 4),
            "shipping": round(float(row.shipping or 0.0), 4),
            "packaging": round(float(row.packaging or 0.0), 4),
        }
        default_letter_rows.append(row_dict)

        product_name = (row.product or "").strip()
        if product_name:
            default_products.append(product_name)

    return {
        "id": c.id,
        "name": c.name or "",
        "notes": c.notes or "",
        "default_products": _clean_default_products(default_products),
        "default_letter_rows": default_letter_rows,
        "created_at": c.created_at.isoformat() if c.created_at else datetime.now(timezone.utc).isoformat(),
    }

def _clean_default_products(values):
    submitted = [
        str(v or "").strip()
        for v in (values or [])
        if str(v or "").strip()
    ]

    deduped = []
    seen = set()
    for p in submitted:
        key = p.lower()
        if key not in seen:
            seen.add(key)
            deduped.append(p)

    return deduped

def _blank_default_letter_row():
    return {
        "product": "",
        "package_type": "",
        "um": "",
        "margin": 15.0,
        "shipping": 0.0,
        "packaging": 0.0,
    }


def _normalize_default_letter_row(row):
    row = row or {}

    return {
        "product": (row.get("product") or "").strip(),
        "package_type": (row.get("package_type") or "").strip(),
        "um": normalize_um(row.get("um", "")),
        "margin": round(to_float(row.get("margin", 15.0), 15.0), 4),
        "shipping": round(to_float(row.get("shipping", 0.0), 0.0), 4),
        "packaging": round(to_float(row.get("packaging", 0.0), 0.0), 4),
    }


def _clean_default_letter_rows(rows):
    cleaned = []
    seen = set()

    for raw in rows or []:
        row = _normalize_default_letter_row(raw)

        if not row["product"]:
            continue

        row_key = (
            normalize_product_name(row["product"]),
            normalize_um(row["um"]),
            (row["package_type"] or "").strip().lower(),
        )

        if row_key in seen:
            continue

        seen.add(row_key)
        cleaned.append(row)

    return cleaned

def _find_customer_by_id(customers, customer_id):
    for c in customers:
        if str(c.get("id")) == str(customer_id):
            return c
    return None


def _build_printer_row_from_product(product_name, priced_by_name, default_margin):
    company_products = load_company_products() or []
    product_map = {
        normalize_product_name(x.get("product")): x
        for x in company_products
        if (x.get("product") or "").strip()
    }

    product_info = product_map.get(normalize_product_name(product_name), {})
    try:
        lb_per_gal = float(product_info.get("lb_per_gal") or 0.0)
    except Exception:
        lb_per_gal = 0.0

    matches = priced_by_name.get(normalize_product_name(product_name), [])

    if matches:
        p = matches[0]

        try:
            cost = float(p.get("cost") or 0.0)
        except Exception:
            cost = 0.0

        margin = float(default_margin or 0.0)
        price = cost * (1.0 + margin / 100.0)
        shipping = 0.0
        packaging = 0.0
        final_price = price + shipping + packaging

        return {
            "product": p.get("product", product_name),
            "um": p.get("um", ""),
            "lb_per_gal": lb_per_gal,
            "package_type": "",
            "cost": cost,
            "margin": margin,
            "price": price,
            "shipping": shipping,
            "packaging": packaging,
            "final_price": final_price
        }

    return {
        "product": product_name,
        "um": "",
        "lb_per_gal": lb_per_gal,
        "package_type": "",
        "cost": 0.0,
        "margin": float(default_margin or 0.0),
        "price": 0.0,
        "shipping": 0.0,
        "packaging": 0.0,
        "final_price": 0.0
    }

def _build_printer_row_from_default_letter_row(default_row, priced_by_name, fallback_margin):
    default_row = _normalize_default_letter_row(default_row)

    product_name = default_row.get("product", "")
    package_type = default_row.get("package_type", "")
    saved_um = normalize_um(default_row.get("um", ""))
    saved_margin = to_float(default_row.get("margin", fallback_margin), fallback_margin)
    saved_shipping = to_float(default_row.get("shipping", 0.0), 0.0)
    saved_packaging = to_float(default_row.get("packaging", 0.0), 0.0)

    matches = priced_by_name.get(normalize_product_name(product_name), []) or []

    matched = None
    if saved_um:
        for p in matches:
            if normalize_um(p.get("um", "")) == saved_um:
                matched = p
                break

    if matched is None and matches:
        matched = matches[0]

    if matched:
        built = build_printer_row_from_priced_product(
            matched,
            default_margin=saved_margin
        )
    else:
        built = build_printer_row_from_name_only(
            product_name,
            default_margin=saved_margin
        )

    built["package_type"] = package_type
    built["shipping"] = round(saved_shipping, 4)
    built["packaging"] = round(saved_packaging, 4)
    built["margin"] = round(saved_margin, 4)

    cost = to_float(built.get("cost", 0.0), 0.0)
    price = cost * (1.0 + saved_margin / 100.0)
    final_price = price + saved_shipping + saved_packaging

    built["price"] = round(price, 4)
    built["source_price"] = round(price, 4)
    built["final_price"] = round(final_price, 4)

    return normalize_printer_row(built)

def get_month_options():
    return ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
            "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def get_year_options():
    now = datetime.now()
    start = now.year
    return [str(y) for y in range(start, start + 6)]

def normalize_um(value):
    return (value or "").strip().upper()


def to_float(value, default=0.0):
    try:
        if value is None:
            return default
        s = str(value).replace("$", "").replace(",", "").strip()
        if s == "":
            return default
        return float(s)
    except Exception:
        return default


def get_product_weight(product_name):
    """
    Looks up lb_per_gal from company products.
    Returns None if not found or invalid.
    """
    products = load_company_products()
    target = normalize_product_name(product_name)

    for p in products:
        name = normalize_product_name(p.get("product") or p.get("name") or "")
        if name == target:
            val = p.get("lb_per_gal")
            if val in (None, ""):
                return None
            try:
                return float(str(val).replace(",", "").strip())
            except Exception:
                return None

    return None

def build_printer_row_from_pricing_entry(entry):
    """
    Converts a pricing entry into the row structure used by the printer page.
    Keeps source pricing values so U/M conversion can always be based on pricing data.
    """
    product_name = entry.get("product", "")
    source_price = to_float(entry.get("price", 0))
    freight = to_float(entry.get("freight", 0))
    source_um = normalize_um(entry.get("um", ""))

    weight = get_product_weight(product_name)

    return {
        "product": product_name,
        "vendor": entry.get("vendor", ""),
        "del_fob": entry.get("del_fob", ""),
        "um": source_um,                 # current displayed U/M starts as source U/M
        "source_um": source_um,          # original pricing-data U/M
        "price": round(source_price, 4), # current displayed price starts as source price
        "source_price": round(source_price, 4),  # original pricing-data price
        "freight": round(freight, 4),
        "final_price": round(source_price + freight, 4),
        "weight": weight,                # lbs/gal or None
        "date": entry.get("date", "")
    }

def normalize_printer_row(row):
    """
    Ensures every printer row has the fields needed for U/M conversion.
    Also keeps older session rows from breaking.
    """
    product_name = row.get("product", "")

    current_um = normalize_um(row.get("um", ""))
    source_um = normalize_um(row.get("source_um", current_um))

    price = round(to_float(row.get("price", 0.0), 0.0), 4)
    source_price = round(to_float(row.get("source_price", price), price), 4)
    shipping = round(to_float(row.get("shipping", 0.0), 0.0), 4)
    packaging = round(to_float(row.get("packaging", 0.0), 0.0), 4)
    final_price = round(
        to_float(row.get("final_price", price + shipping + packaging), price + shipping + packaging),
        4
    )

    weight = row.get("weight")
    if weight in (None, ""):
        weight = get_product_weight(product_name)
    else:
        try:
            weight = float(str(weight).replace(",", "").strip())
        except Exception:
            weight = get_product_weight(product_name)

    return {
        "product": product_name,
        "um": current_um,
        "source_um": source_um,
        "package_type": row.get("package_type", ""),
        "cost": round(to_float(row.get("cost", 0.0), 0.0), 4),
        "margin": round(to_float(row.get("margin", 0.0), 0.0), 4),
        "price": price,
        "source_price": source_price,
        "shipping": shipping,
        "packaging": packaging,
        "final_price": final_price,
        "weight": weight
    }

def build_printer_row_from_priced_product(p, default_margin=0.0):
    product_name = p.get("product", "")
    source_um = normalize_um(p.get("um", ""))
    cost = to_float(p.get("cost"), 0.0)
    margin = to_float(default_margin, 0.0)

    source_price = cost * (1.0 + margin / 100.0)
    shipping = 0.0
    packaging = 0.0
    final_price = source_price + shipping + packaging
    weight = get_product_weight(product_name)

    return {
        "product": product_name,
        "um": source_um,
        "source_um": source_um,
        "package_type": "",
        "cost": round(cost, 4),
        "margin": round(margin, 4),
        "price": round(source_price, 4),
        "source_price": round(source_price, 4),
        "shipping": round(shipping, 4),
        "packaging": round(packaging, 4),
        "final_price": round(final_price, 4),
        "weight": weight
    }


def build_printer_row_from_name_only(product_name, default_margin=0.0):
    return {
        "product": product_name,
        "um": "",
        "source_um": "",
        "package_type": "",
        "cost": 0.0,
        "margin": round(to_float(default_margin, 0.0), 4),
        "price": 0.0,
        "source_price": 0.0,
        "shipping": 0.0,
        "packaging": 0.0,
        "final_price": 0.0,
        "weight": get_product_weight(product_name)
    }

def ensure_parent_dir(path):
    folder = os.path.dirname(path)
    if folder:
        os.makedirs(folder, exist_ok=True)


def load_sales_people():
    rows = SalesPerson.query.order_by(SalesPerson.name.asc()).all()

    return [
        {
            "id": r.id,
            "name": r.name or "",
            "email": r.email or "",
            "phone": r.phone or "",
            "created_at": r.created_at.isoformat() if r.created_at else datetime.now(timezone.utc).isoformat(),
        }
        for r in rows
    ]


def save_sales_people(sales_people):
    SalesPerson.query.delete()

    seen = set()
    cleaned = []

    for item in sales_people or []:
        if not isinstance(item, dict):
            continue

        name = str(item.get("name") or "").strip()
        email = str(item.get("email") or "").strip()
        phone = str(item.get("phone") or "").strip()

        if not name and not email and not phone:
            continue

        key = (name.lower(), email.lower(), phone)
        if key in seen:
            continue
        seen.add(key)

        cleaned.append(
            SalesPerson(
                id=item.get("id") or uuid.uuid4().hex[:10],
                name=name,
                email=email,
                phone=phone,
            )
        )

    for row in cleaned:
        db.session.add(row)

    db.session.commit()

def normalize_phone(phone):
    digits = re.sub(r"\D", "", str(phone or ""))

    if len(digits) == 10:
        return f"({digits[:3]})-{digits[3:6]}-{digits[6:]}"
    
    return phone


def get_sales_person_by_id(sales_person_id):
    row = db.session.get(SalesPerson, str(sales_person_id))

    if not row:
        return None

    return {
        "id": row.id,
        "name": row.name or "",
        "email": row.email or "",
        "phone": row.phone or "",
    }


def add_sales_person(name, phone, email):
    rows = load_sales_people()
    now = datetime.now(timezone.utc).isoformat()

    row = {
        "id": str(uuid.uuid4()),
        "name": (name or "").strip(),
        "phone": normalize_phone(phone),
        "email": (email or "").strip(),
        "created_at": now,
        "updated_at": now,
    }

    rows.append(row)
    rows.sort(key=lambda x: (x.get("name") or "").lower())
    save_sales_people(rows)
    return row


def update_sales_person(sales_person_id, name, phone, email):
    rows = load_sales_people()
    now = datetime.now(timezone.utc).isoformat()

    updated = None
    for r in rows:
        if str(r.get("id")) == str(sales_person_id):
            r["name"] = (name or "").strip()
            r["phone"] = normalize_phone(phone)
            r["email"] = (email or "").strip()
            r["updated_at"] = now
            updated = r
            break

    rows.sort(key=lambda x: (x.get("name") or "").lower())
    save_sales_people(rows)
    return updated


def delete_sales_person(sales_person_id):
    rows = load_sales_people()
    new_rows = [r for r in rows if str(r.get("id")) != str(sales_person_id)]
    deleted = len(new_rows) != len(rows)
    save_sales_people(new_rows)
    return deleted



def get_todo_month_key():
    now = datetime.now()
    year = now.year
    month = now.month

    if now.day > 15:
        month += 1
        if month == 13:
            month = 1
            year += 1

    month_abbr = ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"][month - 1]
    return f"{year}-{month_abbr}"

def _todo_item_row_to_dict(row):
    if not row:
        return {}

    return {
        "id": row.id,
        "user_id": str(row.user_id),
        "month_key": row.month_key or "",
        "customer_id": row.customer_id or "",
        "done": bool(row.done),
        "done_at": row.done_at.isoformat() if row.done_at else "",
        "history_id": row.history_id or "",
        "file_name": row.file_name or "",
    }


def load_todo_store():
    rows = TodoItem.query.order_by(
        TodoItem.month_key.asc(),
        TodoItem.customer_id.asc(),
        TodoItem.created_at.asc()
    ).all()

    return {
        "items": [_todo_item_row_to_dict(r) for r in rows]
    }


def save_todo_store(store):
    TodoItem.query.delete()

    items = (store or {}).get("items") or []
    for item in items:
        done_at = None
        done_at_raw = item.get("done_at")
        if done_at_raw:
            try:
                done_at = datetime.fromisoformat(str(done_at_raw).replace("Z", "+00:00"))
                if done_at.tzinfo is not None:
                    done_at = done_at.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                done_at = None

        db.session.add(TodoItem(
            id=str(item.get("id") or uuid.uuid4().hex[:10]),
            user_id=int(item.get("user_id") or 0),
            month_key=str(item.get("month_key") or "").strip().upper(),
            customer_id=str(item.get("customer_id") or "").strip(),
            done=bool(item.get("done")),
            done_at=done_at,
            history_id=str(item.get("history_id") or "").strip(),
            file_name=str(item.get("file_name") or "").strip(),
        ))

    db.session.commit()


def load_todo_config():
    rows = TodoRecurringCustomer.query.order_by(
        TodoRecurringCustomer.customer_id.asc(),
        TodoRecurringCustomer.created_at.asc()
    ).all()

    recurring_customer_ids = [str(r.customer_id) for r in rows]
    return {"recurring_customer_ids": recurring_customer_ids}


def save_todo_config(data):
    TodoRecurringCustomer.query.delete()

    recurring_customer_ids = [
        str(cid).strip()
        for cid in (data or {}).get("recurring_customer_ids", [])
        if str(cid).strip()
    ]

    seen = set()
    for customer_id in recurring_customer_ids:
        key = customer_id
        if key in seen:
            continue
        seen.add(key)

        db.session.add(TodoRecurringCustomer(
            user_id=int(session.get("user_id") or 0),
            customer_id=customer_id,
        ))

    db.session.commit()


def get_user_month_todos(user_id, month_key):
    rows = (
        TodoItem.query
        .filter(
            TodoItem.user_id == int(user_id),
            TodoItem.month_key == str(month_key).strip().upper()
        )
        .order_by(TodoItem.customer_id.asc(), TodoItem.created_at.asc())
        .all()
    )
    return [_todo_item_row_to_dict(r) for r in rows]


def add_customer_to_todo(user_id, month_key, customer_id):
    existing = (
        TodoItem.query
        .filter(
            TodoItem.user_id == int(user_id),
            TodoItem.month_key == str(month_key).strip().upper(),
            TodoItem.customer_id == str(customer_id).strip()
        )
        .first()
    )
    if existing:
        return False

    row = TodoItem(
        id=uuid.uuid4().hex[:10],
        user_id=int(user_id),
        month_key=str(month_key).strip().upper(),
        customer_id=str(customer_id).strip(),
        done=False,
        done_at=None,
        history_id="",
        file_name="",
    )
    db.session.add(row)
    db.session.commit()
    return True


def set_todo_done(todo_id, done=True):
    row = db.session.get(TodoItem, str(todo_id))
    if not row:
        return False

    row.done = bool(done)
    row.done_at = datetime.utcnow() if done else None
    if not done:
        row.history_id = ""
        row.file_name = ""

    db.session.commit()
    return True


def remove_todo_item(todo_id):
    row = db.session.get(TodoItem, str(todo_id))
    if not row:
        return False

    db.session.delete(row)
    db.session.commit()
    return True

def migrate_todo_items_json_to_db():
    if not os.path.exists(TODO_LIST_PATH):
        return 0

    try:
        with open(TODO_LIST_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    items = data.get("items", []) if isinstance(data, dict) else []
    inserted = 0

    for item in items:
        item_id = str((item or {}).get("id") or "").strip()
        if not item_id:
            item_id = uuid.uuid4().hex[:10]

        existing = db.session.get(TodoItem, item_id)
        if existing:
            continue

        done_at = None
        done_at_raw = item.get("done_at")
        if done_at_raw:
            try:
                done_at = datetime.fromisoformat(str(done_at_raw).replace("Z", "+00:00"))
                if done_at.tzinfo is not None:
                    done_at = done_at.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                done_at = None

        db.session.add(TodoItem(
            id=item_id,
            user_id=int(item.get("user_id") or 0),
            month_key=str(item.get("month_key") or "").strip().upper(),
            customer_id=str(item.get("customer_id") or "").strip(),
            done=bool(item.get("done")),
            done_at=done_at,
            history_id=str(item.get("history_id") or "").strip(),
            file_name=str(item.get("file_name") or "").strip(),
        ))
        inserted += 1

    db.session.commit()
    return inserted


def migrate_todo_config_json_to_db(default_user_id=None):
    if not os.path.exists(TODO_CONFIG_PATH):
        return 0

    try:
        with open(TODO_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return 0

    recurring_customer_ids = data.get("recurring_customer_ids", []) if isinstance(data, dict) else []
    inserted = 0
    user_id = int(default_user_id or 0)

    for customer_id in recurring_customer_ids:
        customer_id = str(customer_id).strip()
        if not customer_id:
            continue

        existing = (
            TodoRecurringCustomer.query
            .filter(
                TodoRecurringCustomer.user_id == user_id,
                TodoRecurringCustomer.customer_id == customer_id
            )
            .first()
        )
        if existing:
            continue

        db.session.add(TodoRecurringCustomer(
            user_id=user_id,
            customer_id=customer_id,
        ))
        inserted += 1

    db.session.commit()
    return inserted

def get_posted_printer_rows(form):
    posted_products = form.getlist("row_product")
    posted_ums = form.getlist("row_um")
    posted_package_types = form.getlist("row_package_type")
    posted_costs = form.getlist("row_cost")
    posted_margins = form.getlist("row_margin")
    posted_prices = form.getlist("row_price")
    posted_shipping = form.getlist("row_shipping")
    posted_packaging = form.getlist("row_packaging")
    posted_finals = form.getlist("row_final")

    row_count = len(posted_products)
    rows = []

    for i in range(row_count):
        product = (posted_products[i] if i < len(posted_products) else "").strip()
        if not product:
            continue

        row = {
            "product": product,
            "um": posted_ums[i] if i < len(posted_ums) else "",
            "package_type": posted_package_types[i] if i < len(posted_package_types) else "",
            "cost": posted_costs[i] if i < len(posted_costs) else "0",
            "margin": posted_margins[i] if i < len(posted_margins) else "0",
            "price": posted_prices[i] if i < len(posted_prices) else "0",
            "shipping": posted_shipping[i] if i < len(posted_shipping) else "0",
            "packaging": posted_packaging[i] if i < len(posted_packaging) else "0",
            "final_price": posted_finals[i] if i < len(posted_finals) else "0",
        }
        rows.append(normalize_printer_row(row))

    return rows

def get_posted_reverse_margin_rows(form):
    posted_products = form.getlist("row_product")
    posted_ums = form.getlist("row_um")
    posted_finals = form.getlist("row_final")
    posted_shipping = form.getlist("row_shipping")
    posted_packaging = form.getlist("row_packaging")
    posted_historical_costs = form.getlist("row_historical_cost")

    row_count = max(
        len(posted_products),
        len(posted_ums),
        len(posted_finals),
        len(posted_shipping),
        len(posted_packaging),
        len(posted_historical_costs),
    )

    rows = []

    for i in range(row_count):
        product = (posted_products[i] if i < len(posted_products) else "").strip()
        um = (posted_ums[i] if i < len(posted_ums) else "").strip()
        final_price = posted_finals[i] if i < len(posted_finals) else "0"
        shipping = posted_shipping[i] if i < len(posted_shipping) else "0"
        packaging = posted_packaging[i] if i < len(posted_packaging) else "0"
        historical_cost = posted_historical_costs[i] if i < len(posted_historical_costs) else "0"

        # Skip only rows that are completely empty
        if not product and not um and not str(final_price).strip() and not str(shipping).strip() and not str(packaging).strip() and not str(historical_cost).strip():
            continue

        rows.append({
            "product": product,
            "um": um,
            "final_price": final_price,
            "shipping": shipping,
            "packaging": packaging,
            "historical_cost": historical_cost,
        })

    return rows


def calculate_reverse_margin(final_price, shipping, packaging, cost):
    final_price = to_float(final_price, 0.0)
    shipping = to_float(shipping, 0.0)
    packaging = to_float(packaging, 0.0)
    cost = to_float(cost, 0.0)

    net_price = final_price - shipping - packaging

    if cost <= 0:
        return {
            "net_price": round(net_price, 4),
            "margin_pct": None,
            "margin_ratio": None,
            "status": "missing_cost"
        }

    margin_ratio = (net_price - cost) / cost

    return {
        "net_price": round(net_price, 4),
        "margin_pct": round(margin_ratio * 100.0, 4),
        "margin_ratio": round(margin_ratio, 6),
        "status": "ok"
    }


def get_historical_cost_candidates(month_key: str, product_name: str):
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    rows = (store.get("by_month") or {}).get(str(month_key or "").strip().upper(), []) or []
    target_name = normalize_product_name(product_name)
    matches = []

    for r in rows:
        if normalize_product_name(r.get("product")) != target_name:
            continue

        matches.append({
            "product": (r.get("product") or "").strip(),
            "vendor": (r.get("vendor") or "").strip(),
            "um": normalize_um(r.get("um", "")),
            "price": round(to_float(r.get("price", 0.0), 0.0), 4),
            "freight": round(to_float(r.get("freight", 0.0), 0.0), 4),
            "final_price": round(to_float(r.get("final_price", 0.0), 0.0), 4),
            "date": (r.get("date") or "").strip(),
        })

    matches.sort(key=lambda x: (
        0 if x.get("final_price", 0.0) > 0 else 1,
        x.get("final_price", 0.0),
        x.get("vendor", "").lower(),
        x.get("um", "")
    ))
    return matches


def convert_cost_between_ums(cost, from_um, to_um, product_name):
    cost = to_float(cost, 0.0)
    from_um = normalize_um(from_um)
    to_um = normalize_um(to_um)

    if cost <= 0:
        return None

    if not from_um or not to_um or from_um == to_um:
        return round(cost, 4)

    if from_um == "LB" and to_um == "GAL":
        weight = get_product_weight(product_name)
        if not weight or weight <= 0:
            return None
        return round(cost * weight, 4)

    if from_um == "GAL" and to_um == "LB":
        weight = get_product_weight(product_name)
        if not weight or weight <= 0:
            return None
        return round(cost / weight, 4)

    if from_um == "EACH" or to_um == "EACH":
        return None

    return None


def pick_best_historical_cost_candidate(month_key: str, product_name: str, requested_um: str = ""):
    requested_um = normalize_um(requested_um)
    candidates = get_historical_cost_candidates(month_key, product_name)

    if not candidates:
        return {
            "matched": False,
            "match_type": "none",
            "cost": 0.0,
            "picked": None,
            "candidate_count": 0,
        }

    # First try exact U/M
    if requested_um:
        exact_matches = [
            c for c in candidates
            if normalize_um(c.get("um", "")) == requested_um
        ]
        if exact_matches:
            picked = exact_matches[0]
            return {
                "matched": True,
                "match_type": "exact_um",
                "cost": round(to_float(picked.get("final_price", 0.0), 0.0), 4),
                "picked": picked,
                "candidate_count": len(exact_matches),
            }

    # Then try convertible U/M
    if requested_um:
        convertible = []
        for c in candidates:
            converted_cost = convert_cost_between_ums(
                cost=c.get("final_price", 0.0),
                from_um=c.get("um", ""),
                to_um=requested_um,
                product_name=product_name
            )
            if converted_cost is not None:
                candidate_copy = dict(c)
                candidate_copy["converted_cost"] = converted_cost
                convertible.append(candidate_copy)

        if convertible:
            convertible.sort(key=lambda x: x.get("converted_cost", 0.0))
            picked = convertible[0]
            return {
                "matched": True,
                "match_type": "converted_um",
                "cost": round(to_float(picked.get("converted_cost", 0.0), 0.0), 4),
                "picked": picked,
                "candidate_count": len(convertible),
            }

    # Finally fall back to product only
    picked = candidates[0]
    return {
        "matched": True,
        "match_type": "product_only",
        "cost": round(to_float(picked.get("final_price", 0.0), 0.0), 4),
        "picked": picked,
        "candidate_count": len(candidates),
    }


def match_historical_cost(month_key: str, product_name: str, um: str = ""):
    return pick_best_historical_cost_candidate(
        month_key=month_key,
        product_name=product_name,
        requested_um=um
    )

def enrich_reverse_margin_rows(rows, month_key: str):
    enriched = []

    for row in (rows or []):
        product_name = (row.get("product") or "").strip()
        requested_um = normalize_um(row.get("um", ""))

        match = match_historical_cost(
            month_key=month_key,
            product_name=product_name,
            um=requested_um
        )

        manual_cost = round(to_float(row.get("historical_cost", 0.0), 0.0), 4)
        matched_cost = manual_cost if manual_cost > 0 else round(to_float(match.get("cost", 0.0), 0.0), 4)

        reverse = calculate_reverse_margin(
            final_price=row.get("final_price", 0.0),
            shipping=row.get("shipping", 0.0),
            packaging=row.get("packaging", 0.0),
            cost=matched_cost
        )

        picked = match.get("picked") or {}
        source_cost = round(to_float(picked.get("final_price", 0.0), 0.0), 4)
        source_cost_um = normalize_um(picked.get("um", ""))
        product_weight = get_product_weight(product_name)

        enriched.append({
            "product": product_name,
            "um": requested_um,
            "final_price": round(to_float(row.get("final_price", 0.0), 0.0), 4),
            "shipping": round(to_float(row.get("shipping", 0.0), 0.0), 4),
            "packaging": round(to_float(row.get("packaging", 0.0), 0.0), 4),
            "historical_cost": matched_cost,
            "historical_cost_found": bool(match.get("matched")),
            "historical_cost_match_type": match.get("match_type", "none"),
            "historical_cost_candidate_count": int(match.get("candidate_count") or 0),
            "historical_cost_date": ((match.get("picked") or {}).get("date") or ""),
            "historical_source_cost": source_cost,
            "historical_source_um": source_cost_um,
            "product_weight": product_weight,
            "reverse_net_price": reverse.get("net_price"),
            "reverse_margin_pct": reverse.get("margin_pct"),
            "reverse_margin_ratio": reverse.get("margin_ratio"),
            "reverse_margin_status": reverse.get("status"),
        })

    return enriched


def build_reverse_margin_summary(rows):
    valid = [r for r in (rows or []) if r.get("reverse_margin_pct") is not None]

    if not valid:
        return {
            "row_count": len(rows or []),
            "matched_count": 0,
            "avg_margin_pct": None,
            "avg_net_price": None,
            "avg_historical_cost": None,
        }

    avg_margin_pct = sum(to_float(r.get("reverse_margin_pct"), 0.0) for r in valid) / len(valid)
    avg_net_price = sum(to_float(r.get("reverse_net_price"), 0.0) for r in valid) / len(valid)
    avg_historical_cost = sum(to_float(r.get("historical_cost"), 0.0) for r in valid) / len(valid)

    return {
        "row_count": len(rows or []),
        "matched_count": len(valid),
        "avg_margin_pct": round(avg_margin_pct, 4),
        "avg_net_price": round(avg_net_price, 4),
        "avg_historical_cost": round(avg_historical_cost, 4),
    }


def build_reverse_margin_rows_from_customer_products(customer):
    default_products = _clean_default_products(
        (customer or {}).get("default_products") or []
    )

    rows = []
    seen = set()

    for product_name in default_products:
        key = normalize_product_name(product_name)
        if not key or key in seen:
            continue
        seen.add(key)

        rows.append({
            "product": product_name,
            "um": "",
            "final_price": 0.0,
            "shipping": 0.0,
            "packaging": 0.0,
        })

    return rows

def append_margin_history_record(record):
    store = load_margin_history()
    store["records"].append(record)
    store["records"].sort(
        key=lambda r: (
            str(r.get("product", "")).upper(),
            str(r.get("pricing_date", "")),
            int(r.get("entry_seq", 0) or 0),
            str(r.get("created_at", ""))
        )
    )
    save_margin_history(store)

def _safe_iso_date(value):
    raw = normalize_pricing_date(value)
    return raw or ""


def _margin_record_sort_key(record):
    return (
        _safe_iso_date(record.get("pricing_date")),
        int(record.get("entry_seq") or 0),
        str(record.get("created_at") or "")
    )


def get_margin_history_records(filters=None):
    filters = filters or {}
    store = load_margin_history()
    records = store.get("records", []) or []

    product_filter = normalize_product_name(filters.get("product", ""))
    customer_filter = str(filters.get("customer", "") or "").strip().lower()
    source_filter = str(filters.get("source", "") or "").strip().lower()
    um_filter = normalize_um(filters.get("um", ""))
    start_date = _safe_iso_date(filters.get("start_date", ""))
    end_date = _safe_iso_date(filters.get("end_date", ""))

    filtered = []

    for r in records:
        if not is_meaningful_margin_record(r):
            continue

        product_name = normalize_product_name(r.get("product"))
        customer_name = str(r.get("customer") or "").strip().lower()
        source_name = str(r.get("source") or "").strip().lower()
        um_name = normalize_um(r.get("um", ""))
        pricing_date = _safe_iso_date(r.get("pricing_date"))

        if product_filter and product_name != product_filter:
            continue
        if customer_filter and customer_name != customer_filter:
            continue
        if source_filter and source_name != source_filter:
            continue
        if um_filter and um_name != um_filter:
            continue
        if start_date and pricing_date and pricing_date < start_date:
            continue
        if end_date and pricing_date and pricing_date > end_date:
            continue

        filtered.append(r)

    filtered.sort(key=_margin_record_sort_key)
    return filtered


def build_margin_analytics_summary(records):
    summary = {
        "count": 0,
        "avg_margin_pct": 0.0,
        "avg_cost": 0.0,
        "avg_final_price": 0.0,
        "best_margin_pct": None,
        "worst_margin_pct": None,
        "latest_pricing_date": "",
    }

    if not records:
        return summary

    margins = [to_float(r.get("margin_pct"), 0.0) for r in records]
    costs = [to_float(r.get("cost"), 0.0) for r in records]
    finals = [to_float(r.get("final_price"), 0.0) for r in records]
    dates = [_safe_iso_date(r.get("pricing_date")) for r in records if _safe_iso_date(r.get("pricing_date"))]

    summary["count"] = len(records)
    summary["avg_margin_pct"] = round(sum(margins) / len(margins), 2) if margins else 0.0
    summary["avg_cost"] = round(sum(costs) / len(costs), 4) if costs else 0.0
    summary["avg_final_price"] = round(sum(finals) / len(finals), 4) if finals else 0.0
    summary["best_margin_pct"] = round(max(margins), 2) if margins else None
    summary["worst_margin_pct"] = round(min(margins), 2) if margins else None
    summary["latest_pricing_date"] = max(dates) if dates else ""

    return summary


def build_margin_product_rollup(records):
    grouped = {}

    for r in records:
        key = normalize_product_name(r.get("product"))
        if not key:
            continue

        if key not in grouped:
            grouped[key] = {
                "product": str(r.get("product") or "").strip(),
                "count": 0,
                "avg_margin_pct": 0.0,
                "avg_cost": 0.0,
                "avg_final_price": 0.0,
                "_margin_sum": 0.0,
                "_cost_sum": 0.0,
                "_final_sum": 0.0,
                "latest_pricing_date": "",
            }

        row = grouped[key]
        row["count"] += 1
        row["_margin_sum"] += to_float(r.get("margin_pct"), 0.0)
        row["_cost_sum"] += to_float(r.get("cost"), 0.0)
        row["_final_sum"] += to_float(r.get("final_price"), 0.0)

        pricing_date = _safe_iso_date(r.get("pricing_date"))
        if pricing_date and pricing_date > (row["latest_pricing_date"] or ""):
            row["latest_pricing_date"] = pricing_date

    output = []
    for _, row in grouped.items():
        count = row["count"] or 1
        output.append({
            "product": row["product"],
            "count": row["count"],
            "avg_margin_pct": round(row["_margin_sum"] / count, 2),
            "avg_cost": round(row["_cost_sum"] / count, 4),
            "avg_final_price": round(row["_final_sum"] / count, 4),
            "latest_pricing_date": row["latest_pricing_date"],
        })

    output.sort(key=lambda x: (-x["count"], x["product"].lower()))
    return output


def build_margin_chart_points(records):
    points = []

    for r in records:
        pricing_date = _safe_iso_date(r.get("pricing_date"))
        if not pricing_date:
            continue

        points.append({
            "pricing_date": pricing_date,
            "product": str(r.get("product") or "").strip(),
            "margin_pct": round(to_float(r.get("margin_pct"), 0.0), 2),
            "cost": round(to_float(r.get("cost"), 0.0), 4),
            "final_price": round(to_float(r.get("final_price"), 0.0), 4),
            "customer": str(r.get("customer") or "").strip(),
            "source": str(r.get("source") or "").strip(),
        })

    points.sort(key=lambda x: (x["pricing_date"], x["product"].lower()))
    return points


def get_margin_filter_options():
    records = get_margin_history_records()

    products = sorted({
        str(r.get("product") or "").strip()
        for r in records
        if str(r.get("product") or "").strip()
    }, key=lambda x: x.lower())

    customers = sorted({
        str(r.get("customer") or "").strip()
        for r in records
        if str(r.get("customer") or "").strip()
    }, key=lambda x: x.lower())

    sources = sorted({
        str(r.get("source") or "").strip()
        for r in records
        if str(r.get("source") or "").strip()
    }, key=lambda x: x.lower())

    return {
        "products": products,
        "customers": customers,
        "sources": sources,
    }


def get_product_default_um(product_name: str) -> str:
    """
    Picks the default analytics U/M for a product.

    Rule:
    - If current pricing exists for the product, use that U/M
    - Otherwise fall back to GAL when lb_per_gal exists
    - Otherwise blank
    """
    product_key = normalize_product_name(product_name)
    if not product_key:
        return ""

    # First try current pricing rows
    store = load_pricing_store()
    by_month = store.get("by_month") or {}

    latest_found = None
    latest_key = ""

    for month_key, rows in by_month.items():
        if month_key > latest_key:
            for row in rows or []:
                if normalize_product_name(row.get("product")) == product_key:
                    um = normalize_um(row.get("um", ""))
                    if um:
                        latest_found = um
                        latest_key = month_key

    if latest_found:
        return latest_found

    # Then try product setup
    for p in load_company_products() or []:
        if normalize_product_name(p.get("product")) == product_key:
            try:
                lb_per_gal = float(p.get("lb_per_gal") or 0.0)
            except Exception:
                lb_per_gal = 0.0

            if lb_per_gal > 0:
                return "GAL"

    return ""


def convert_value_to_default_um(value, from_um, product_name, default_um):
    """
    Convert a saved numeric value into the product's default analytics U/M.
    Uses the same LB/GAL logic already used elsewhere in the app.
    """
    value = to_float(value, 0.0)
    from_um = normalize_um(from_um)
    default_um = normalize_um(default_um)

    if value <= 0:
        return 0.0

    if not from_um or not default_um or from_um == default_um:
        return round(value, 4)

    converted = convert_cost_between_ums(
        cost=value,
        from_um=from_um,
        to_um=default_um,
        product_name=product_name
    )

    if converted is None:
        return round(value, 4)

    return round(converted, 4)


def normalize_margin_record_for_analytics(record):
    """
    Returns a copy of a saved margin-history record normalized to the
    product's default analytics U/M for charting.
    """
    r = dict(record or {})
    product_name = str(r.get("product") or "").strip()
    saved_um = normalize_um(r.get("um", ""))

    default_um = get_product_default_um(product_name) or saved_um

    normalized_cost = convert_value_to_default_um(
        r.get("cost", 0.0),
        saved_um,
        product_name,
        default_um
    )

    normalized_final_price = convert_value_to_default_um(
        r.get("final_price", 0.0),
        saved_um,
        product_name,
        default_um
    )

    r["saved_um"] = saved_um
    r["default_um"] = default_um
    r["normalized_cost"] = normalized_cost
    r["normalized_final_price"] = normalized_final_price

    return r


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

def mark_customer_todo_done(user_id, month_key, customer_id, history_id="", file_name=""):
    row = (
        TodoItem.query
        .filter(
            TodoItem.user_id == int(user_id),
            TodoItem.month_key == str(month_key or "").strip().upper(),
            TodoItem.customer_id == str(customer_id or "").strip()
        )
        .first()
    )

    if not row:
        return False

    row.done = True
    row.done_at = datetime.utcnow()
    row.history_id = str(history_id or "")
    row.file_name = str(file_name or "")
    db.session.commit()
    return True


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
        email = (form.email.data or "").strip()
        password = form.password.data or ""

        user = find_user_by_email(email)

        if not user:
            flash("Invalid email or password.", "error")
            return render_template("login.html", form=form)

        if not user.is_active:
            flash("Your account is inactive. Contact an admin.", "error")
            return render_template("login.html", form=form)

        if not check_password_hash(user.password_hash, password):
            flash("Invalid email or password.", "error")
            return render_template("login.html", form=form)

        session.clear()
        session["user_id"] = user.id
        session["email"] = user.email or ""
        session["full_name"] = user.full_name or ""
        session["phone"] = user.phone or ""
        session["is_admin"] = bool(user.is_admin)
        return redirect(url_for("dashboard"))

    return render_template("login.html", form=form)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/pricing", methods=["GET", "POST"])
@login_required
def pricing_page():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)
    store.setdefault("by_month", {})

    now = datetime.now()
    default_month = now.strftime("%b").upper()
    default_year = str(now.year)

    if request.method == "POST":
        selected_month = (request.form.get("selected_month") or default_month).strip().upper()
        selected_year = (request.form.get("selected_year") or default_year).strip()
    else:
        selected_month = (request.args.get("selected_month") or default_month).strip().upper()
        selected_year = (request.args.get("selected_year") or default_year).strip()

    month_options = get_month_options()
    year_options = get_year_options()

    if selected_month not in month_options:
        selected_month = default_month
    if selected_year not in year_options:
        selected_year = default_year

    selected_key = month_key_from(selected_year, selected_month)

    store["by_month"].setdefault(selected_key, [])
    rows = store["by_month"][selected_key]

    errors = []
    paste_text = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "save_rows":
            row_ids = request.form.getlist("row_id")
            row_products = request.form.getlist("row_product")
            row_ums = request.form.getlist("row_um")
            row_prices = request.form.getlist("row_price")
            row_freights = request.form.getlist("row_freight_tax")
            row_finals = request.form.getlist("row_final_price")

            updated_rows = []

            def to_float(v, label, row_num):
                try:
                    return float(str(v).replace("$", "").replace(",", "").strip())
                except Exception:
                    errors.append(f"Row {row_num}: {label} must be a number.")
                    return 0.0

            for i, row_id in enumerate(row_ids):
                product = (row_products[i] if i < len(row_products) else "").strip()
                um = (row_ums[i] if i < len(row_ums) else "").strip().upper()

                price = to_float(row_prices[i] if i < len(row_prices) else 0, "Price", i + 1)
                freight_tax = to_float(row_freights[i] if i < len(row_freights) else 0, "Freight/Tax", i + 1)
                final_price = to_float(row_finals[i] if i < len(row_finals) else 0, "Final Price", i + 1)

                if not product or not um:
                    errors.append(f"Row {i+1}: Product and U/M are required.")

                updated_rows.append({
                    "id": row_id,
                    "year": selected_year,
                    "month": selected_month,
                    "product": product,
                    "um": um,
                    "price": price,
                    "freight_tax": freight_tax,
                    "final_price": final_price,
                    "created_at": rows[i].get("created_at") if i < len(rows) else datetime.now(timezone.utc).isoformat()
                })

            if errors:
                rows = updated_rows
                return render_template(
                    "pricing.html",
                    month_key=selected_key,
                    rows=rows,
                    errors=errors,
                    paste_text="",
                    month_options=month_options,
                    year_options=year_options,
                    selected_month=selected_month,
                    selected_year=selected_year,
                    page="pricing",
                    page_title="Pricing"
                )

            store["by_month"][selected_key] = updated_rows
            save_pricing_store(store)
            flash(f"Updated {len(updated_rows)} row(s) for {selected_key}.", "success")
            return redirect(
                url_for(
                    "pricing_page",
                    selected_month=selected_month,
                    selected_year=selected_year
                )
            )

        if action == "delete_selected":
            delete_ids = set(request.form.getlist("delete_row"))

            if not delete_ids:
                flash("No rows were selected.", "info")
                return redirect(
                    url_for(
                        "pricing_page",
                        selected_month=selected_month,
                        selected_year=selected_year
                    )
                )

            remaining_rows = [
                r for r in rows
                if str(r.get("id")) not in delete_ids
            ]

            deleted_count = len(rows) - len(remaining_rows)
            store["by_month"][selected_key] = remaining_rows
            save_pricing_store(store)

            flash(f"Deleted {deleted_count} row(s) from {selected_key}.", "success")
            return redirect(
                url_for(
                    "pricing_page",
                    selected_month=selected_month,
                    selected_year=selected_year
                )
            )
        
        if action == "clear":
            store["by_month"][selected_key] = []
            save_pricing_store(store)
            session.pop("pricing_pending", None)
            flash(f"Cleared {selected_key} list.", "success")
            return redirect(
                url_for(
                    "pricing_page",
                    selected_month=selected_month,
                    selected_year=selected_year
                )
            )

        paste_text = request.form.get("paste_data") or ""
        lines = [ln.strip() for ln in paste_text.splitlines() if ln.strip()]

        if not lines:
            errors.append("Paste area is empty.")
            return render_template(
                "pricing.html",
                month_key=selected_key,
                rows=rows,
                errors=errors,
                paste_text=paste_text,
                month_options=month_options,
                year_options=year_options,
                selected_month=selected_month,
                selected_year=selected_year,
                page="pricing",
                page_title="Pricing"
            )

        new_entries = []
        for i, line in enumerate(lines, start=1):
            raw = line

            if "\t" in line:
                parts = [p.strip() for p in line.split("\t")]
            else:
                parts = [p.strip() for p in line.split(",")]

            parts = [p for p in parts if p != ""]

            if len(parts) != 5:
                errors.append(
                    f"Line {i}: expected 5 columns "
                    f"(Product, U/M, Price, Freight/Tax, Final Price), got {len(parts)} → {raw}"
                )
                continue

            product, um, price, freight_tax, final_price = parts

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

            if not product or not um:
                errors.append(f"Line {i}: Product and U/M are required → {raw}")
                continue

            if p is None or f is None or fp is None:
                continue

            new_entries.append({
                "id": uuid.uuid4().hex[:10],
                "year": selected_year,
                "month": selected_month,
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
                month_key=selected_key,
                rows=rows,
                errors=errors,
                paste_text=paste_text,
                month_options=month_options,
                year_options=year_options,
                selected_month=selected_month,
                selected_year=selected_year,
                page="pricing",
                page_title="Pricing"
            )

        existing = store["by_month"][selected_key]

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
                "month_key": selected_key,
                "brand_new": brand_new,
                "mods": mods,
                "ignored_exact": ignored_exact
            }

            rows = store["by_month"][selected_key]
            return render_template(
                "pricing.html",
                month_key=selected_key,
                rows=rows,
                errors=[],
                paste_text=paste_text,
                show_mod_modal=True,
                new_count=len(brand_new),
                mod_count=len(mods),
                ignored_exact=ignored_exact,
                mod_products=sorted({m["product"] for m in mods}),
                month_options=month_options,
                year_options=year_options,
                selected_month=selected_month,
                selected_year=selected_year,
                page="pricing",
                page_title="Pricing"
            )

        if brand_new:
            store["by_month"][selected_key].extend(brand_new)
            save_pricing_store(store)

        if brand_new or ignored_exact:
            flash(f"Saved {len(brand_new)} new row(s). Ignored {ignored_exact} exact duplicate(s).", "success")
        else:
            flash("No new rows were added (all rows were exact duplicates).", "info")

        return redirect(
            url_for(
                "pricing_page",
                selected_month=selected_month,
                selected_year=selected_year
            )
        )

    return render_template(
        "pricing.html",
        month_key=selected_key,
        rows=rows,
        errors=[],
        paste_text="",
        month_options=month_options,
        year_options=year_options,
        selected_month=selected_month,
        selected_year=selected_year,
        page="pricing",
        page_title="Pricing"
    )


@app.route("/pricing/mods/apply", methods=["POST"])
@login_required
def pricing_apply_mods():
    store = load_pricing_store()
    store = maybe_auto_reset_month(store)

    pending = session.get("pricing_pending") or {}
    month_key = pending.get("month_key")
    brand_new = pending.get("brand_new") or []
    mods = pending.get("mods") or []

    selected_year = ""
    selected_month = ""

    if "-" in str(month_key):
        selected_year, selected_month = str(month_key).split("-", 1)

    if not month_key:
        flash("No pending pricing changes found.", "warning")
        return redirect(url_for(
            "pricing_page",
            selected_month=selected_month,
            selected_year=selected_year
        ))

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
        return redirect(url_for(
            "pricing_page",
            selected_month=selected_month,
            selected_year=selected_year
        ))

    if decision == "append_anyway":
        store["by_month"][month_key].extend(brand_new + mods)
        save_pricing_store(store)
        session.pop("pricing_pending", None)
        flash("Appended new rows + modified rows as additional entries.", "success")
        return redirect(url_for(
            "pricing_page",
            selected_month=selected_month,
            selected_year=selected_year
        ))

    if decision == "replace_mods":
        mod_keys = set(_pkey(m) for m in mods)
        filtered = [e for e in existing if _pkey(e) not in mod_keys]
        filtered.extend(brand_new)
        filtered.extend(mods)
        store["by_month"][month_key] = filtered

        save_pricing_store(store)
        session.pop("pricing_pending", None)
        flash("Replaced modified products and saved changes.", "success")
        return redirect(url_for(
            "pricing_page",
            selected_month=selected_month,
            selected_year=selected_year
        ))

    flash("Unknown decision.", "danger")
    return redirect(url_for(
        "pricing_page",
        selected_month=selected_month,
        selected_year=selected_year
    ))


@app.route("/customers", methods=["GET", "POST"])
@login_required
def customers_page():
    customers = load_customers()
    company_products = load_company_products()
    errors = []
    form = MultiDict()
    form["customer_name"] = ""
    form.setlist("default_products", [])
    form["default_letter_rows"] = []
    form["add_to_recurring_todo"] = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add":
            name = (request.form.get("customer_name") or "").strip()
            add_to_recurring_todo = (request.form.get("add_to_recurring_todo") or "").strip() == "1"

            row_products = request.form.getlist("default_row_product")
            row_package_types = request.form.getlist("default_row_package_type")
            row_ums = request.form.getlist("default_row_um")
            row_margins = request.form.getlist("default_row_margin")
            row_shippings = request.form.getlist("default_row_shipping")
            row_packagings = request.form.getlist("default_row_packaging")

            row_count = max(
                len(row_products),
                len(row_package_types),
                len(row_ums),
                len(row_margins),
                len(row_shippings),
                len(row_packagings),
            )

            submitted_rows = []
            for i in range(row_count):
                submitted_rows.append({
                    "product": row_products[i] if i < len(row_products) else "",
                    "package_type": row_package_types[i] if i < len(row_package_types) else "",
                    "um": row_ums[i] if i < len(row_ums) else "",
                    "margin": row_margins[i] if i < len(row_margins) else "",
                    "shipping": row_shippings[i] if i < len(row_shippings) else "",
                    "packaging": row_packagings[i] if i < len(row_packagings) else "",
                })

            default_letter_rows = _clean_default_letter_rows(submitted_rows)

            default_products = _clean_default_products([
                (row.get("product") or "").strip()
                for row in default_letter_rows
                if (row.get("product") or "").strip()
            ])

            form["customer_name"] = name
            form.setlist("default_products", default_products)
            form["default_letter_rows"] = default_letter_rows
            form["add_to_recurring_todo"] = "1" if add_to_recurring_todo else ""

            if not name:
                errors.append("Customer name is required.")
            elif any((c.get("name", "").strip().lower() == name.lower()) for c in customers):
                errors.append("That customer already exists.")

            if not errors:
                new_customer = {
                    "id": uuid.uuid4().hex[:10],
                    "name": name,
                    "notes": "",
                    "default_products": default_products,
                    "default_letter_rows": default_letter_rows,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }

                customers.append(new_customer)
                save_customers(customers)

                if add_to_recurring_todo:
                    todo_config = load_todo_config()
                    recurring_customer_ids = [
                        str(cid).strip()
                        for cid in todo_config.get("recurring_customer_ids", [])
                        if str(cid).strip()
                    ]

                    if str(new_customer["id"]) not in recurring_customer_ids:
                        recurring_customer_ids.append(str(new_customer["id"]))
                        todo_config["recurring_customer_ids"] = recurring_customer_ids
                        save_todo_config(todo_config)

                if add_to_recurring_todo:
                    flash("Customer added and included in the recurring monthly to-do list.", "success")
                else:
                    flash("Customer added.", "success")

                return redirect(url_for("customers_page"))

    return render_template(
        "customers.html",
        customers=customers,
        company_products=company_products,
        errors=errors,
        form=form,
        page="customers",
        page_title="Customers"
    )

@app.route("/customers/<customer_id>")
@login_required
def customer_profile_page(customer_id):
    customer = find_customer_by_id(customer_id)
    if not customer:
        flash("Customer not found.", "error")
        return redirect(url_for("customers_page"))

    company_products = load_company_products()

    default_letter_rows = _clean_default_letter_rows(
        customer.get("default_letter_rows") or []
    )

    if not default_letter_rows:
        fallback_products = _clean_default_products(customer.get("default_products") or [])
        default_letter_rows = [
            {
                "product": p,
                "package_type": "",
                "um": "",
                "margin": 15.0,
                "shipping": 0.0,
                "packaging": 0.0,
            }
            for p in fallback_products
        ]

    customer["default_letter_rows"] = default_letter_rows

    return render_template(
        "customer_profile.html",
        customer=customer,
        company_products=company_products,
        page="customers_profile",
        page_title="Customer Profile"
    )


@app.route("/customers/<customer_id>/save", methods=["POST"])
@login_required
def customer_profile_save(customer_id):
    customers = load_customers()

    target = None
    for c in customers:
        if str(c.get("id")) == str(customer_id):
            target = c
            break

    if not target:
        flash("Customer not found.", "error")
        return redirect(url_for("customers_page"))

    name = (request.form.get("customer_name") or "").strip()
    notes = (request.form.get("notes") or "").strip()

    row_products = request.form.getlist("default_row_product")
    row_package_types = request.form.getlist("default_row_package_type")
    row_ums = request.form.getlist("default_row_um")
    row_margins = request.form.getlist("default_row_margin")
    row_shippings = request.form.getlist("default_row_shipping")
    row_packagings = request.form.getlist("default_row_packaging")

    row_count = max(
        len(row_products),
        len(row_package_types),
        len(row_ums),
        len(row_margins),
        len(row_shippings),
        len(row_packagings),
    )

    submitted_rows = []
    for i in range(row_count):
        submitted_rows.append({
            "product": row_products[i] if i < len(row_products) else "",
            "package_type": row_package_types[i] if i < len(row_package_types) else "",
            "um": row_ums[i] if i < len(row_ums) else "",
            "margin": row_margins[i] if i < len(row_margins) else "",
            "shipping": row_shippings[i] if i < len(row_shippings) else "",
            "packaging": row_packagings[i] if i < len(row_packagings) else "",
        })

    default_letter_rows = _clean_default_letter_rows(submitted_rows)

    submitted_products = [
        p.strip()
        for p in request.form.getlist("default_products")
        if p.strip()
    ]
    default_products = _clean_default_products(submitted_products)

    if not name:
        flash("Customer name is required.", "error")
        return redirect(url_for("customer_profile_page", customer_id=customer_id))

    for c in customers:
        if str(c.get("id")) != str(customer_id) and (c.get("name", "").strip().lower() == name.lower()):
            flash("That customer already exists.", "error")
            return redirect(url_for("customer_profile_page", customer_id=customer_id))

    target["name"] = name
    target["notes"] = notes
    target["default_products"] = default_products
    target["default_letter_rows"] = default_letter_rows

    save_customers(customers)
    flash("Customer profile updated.", "success")
    return redirect(url_for("customer_profile_page", customer_id=customer_id))

@app.route("/printer", methods=["GET", "POST"])
@login_required
def printer_page():
    customers = load_customers()
    sales_people = load_sales_people()
    available_periods = get_available_pricing_periods()

    current_key = current_month_key_central()
    available_values = [p["value"] for p in available_periods]

    selected_period = (
        (request.form.get("pricing_period") or "").strip()
        if request.method == "POST"
        else (request.args.get("pricing_period") or "").strip()
    )

    draft = session.get("printer_draft") or {}

    if not selected_period and draft.get("month_key") in available_values:
        selected_period = draft.get("month_key")

    if not selected_period:
        if current_key in available_values:
            selected_period = current_key
        elif available_values:
            selected_period = available_values[0]
        else:
            selected_period = current_key

    month_key, products, priced_products = get_printer_product_options(selected_period)

    errors = []
    quote_rows = []

    form = {
        "customer_id": "",
        "sales_person_id": "",
        "default_margin": "15",
        "package_type": "",
        "pricing_period": month_key,
    }

    prod_map = {p["key"]: p for p in priced_products}

    priced_by_name = {}
    for p in priced_products:
        name_key = normalize_product_name(p.get("product"))
        if not name_key:
            continue
        priced_by_name.setdefault(name_key, []).append(p)

    if request.method == "POST":
        if "duplicate_row" in request.form:
            action = "duplicate_row"
        elif "delete_row" in request.form and (request.form.get("action") or "").strip().lower() == "delete_selected":
            action = "delete_selected"
        else:
            action = (request.form.get("action") or "build").strip().lower()

        form["customer_id"] = (request.form.get("customer_id") or "").strip()
        form["sales_person_id"] = (request.form.get("sales_person_id") or "").strip()
        form["default_margin"] = (request.form.get("default_margin") or "").strip() or "15"
        form["pricing_period"] = month_key

        try:
            default_margin = float(form["default_margin"])
        except Exception:
            default_margin = 15.0
            form["default_margin"] = "15"

        existing_draft = session.get("printer_draft") or {}

        if existing_draft and existing_draft.get("month_key") == month_key:
            existing_rows = [
                normalize_printer_row(r)
                for r in (existing_draft.get("rows") or [])
            ]
        else:
            existing_rows = []

        if action == "select_customer":
            selected_customer = _find_customer_by_id(customers, form["customer_id"])

            if not selected_customer:
                errors.append("Select a customer.")
                quote_rows = existing_rows
            else:
                default_letter_rows = _clean_default_letter_rows(
                    selected_customer.get("default_letter_rows") or []
                )

                new_rows = []
                existing_keys = set()

                if default_letter_rows:
                    for saved_row in default_letter_rows:
                        new_row = _build_printer_row_from_default_letter_row(
                            default_row=saved_row,
                            priced_by_name=priced_by_name,
                            fallback_margin=default_margin
                        )
                        new_row = normalize_printer_row(new_row)

                        row_key = (
                            normalize_product_name(new_row.get("product")),
                            normalize_um(new_row.get("um")),
                            (new_row.get("package_type") or "").strip().lower(),
                        )

                        if row_key in existing_keys:
                            continue

                        new_rows.append(new_row)
                        existing_keys.add(row_key)

                    quote_rows = new_rows

                    session["printer_draft"] = {
                        "month_key": month_key,
                        "customer_id": form["customer_id"],
                        "sales_person_id": form["sales_person_id"],
                        "default_margin": default_margin,
                        "rows": quote_rows,
                    }

                    flash(f"Loaded {len(new_rows)} default row(s).", "success")

                else:
                    default_products = _clean_default_products(
                        selected_customer.get("default_products") or []
                    )

                    for product_name in default_products:
                        new_row = _build_printer_row_from_product(
                            product_name=product_name,
                            priced_by_name=priced_by_name,
                            default_margin=default_margin
                        )
                        new_row = normalize_printer_row(new_row)

                        row_key = (
                            normalize_product_name(new_row.get("product")),
                            normalize_um(new_row.get("um")),
                            (new_row.get("package_type") or "").strip().lower(),
                        )

                        if row_key in existing_keys:
                            continue

                        new_rows.append(new_row)
                        existing_keys.add(row_key)

                    quote_rows = new_rows

                    session["printer_draft"] = {
                        "month_key": month_key,
                        "customer_id": form["customer_id"],
                        "sales_person_id": form["sales_person_id"],
                        "default_margin": default_margin,
                        "rows": quote_rows,
                    }

                    if new_rows:
                        flash(f"Loaded {len(new_rows)} default product(s).", "success")
                    else:
                        flash("This customer has no default price letter yet. Build the list normally.", "info")

        elif action == "build":
            if existing_draft and existing_draft.get("month_key") == month_key:
                if not form["customer_id"]:
                    form["customer_id"] = str(existing_draft.get("customer_id") or "")
                if not form["sales_person_id"]:
                    form["sales_person_id"] = str(existing_draft.get("sales_person_id") or "")
                if not form["default_margin"] or form["default_margin"] == "15":
                    stored_margin = existing_draft.get("default_margin")
                    if stored_margin not in (None, ""):
                        form["default_margin"] = str(stored_margin)
                        try:
                            default_margin = float(form["default_margin"])
                        except Exception:
                            default_margin = 15.0
                            form["default_margin"] = "15"

            selected_keys = request.form.getlist("product_key")

            posted_rows = get_posted_printer_rows(request.form)
            if posted_rows:
                existing_rows = posted_rows
            elif existing_draft and existing_draft.get("month_key") == month_key:
                existing_rows = [
                    normalize_printer_row(r)
                    for r in (existing_draft.get("rows") or [])
                ]
            else:
                existing_rows = []

            if not form["customer_id"]:
                errors.append("Select a customer.")
            if not form["sales_person_id"]:
                errors.append("Select a salesperson.")

            selected_sales_person = None
            if form["sales_person_id"]:
                selected_sales_person = get_sales_person_by_id(form["sales_person_id"])
                if not selected_sales_person:
                    errors.append("Selected salesperson was not found.")

            if not errors:
                # Build an ordered list of selected product names from the picker.
                selected_product_names = []
                selected_product_name_keys = set()

                for k in selected_keys:
                    product_name = ""

                    if k in prod_map:
                        p = prod_map.get(k) or {}
                        product_name = (p.get("product") or "").strip()
                    elif str(k).startswith("name::"):
                        product_name = str(k)[6:].strip()

                    normalized_name = normalize_product_name(product_name)
                    if not normalized_name or normalized_name in selected_product_name_keys:
                        continue

                    selected_product_name_keys.add(normalized_name)
                    selected_product_names.append(product_name)

                # Preserve all existing rows for products that are still selected.
                preserved_rows = []
                preserved_product_keys = set()

                for row in existing_rows:
                    normalized_row = normalize_printer_row(row)
                    product_key = normalize_product_name(normalized_row.get("product"))

                    if product_key and product_key in selected_product_name_keys:
                        preserved_rows.append(normalized_row)
                        preserved_product_keys.add(product_key)

                # Add only brand-new products that are selected but not already present.
                added_rows = []

                for product_name in selected_product_names:
                    product_key = normalize_product_name(product_name)
                    if not product_key or product_key in preserved_product_keys:
                        continue

                    built_row = _build_printer_row_from_product(
                        product_name=product_name,
                        priced_by_name=priced_by_name,
                        default_margin=default_margin
                    )
                    built_row = normalize_printer_row(built_row)

                    added_rows.append(built_row)
                    preserved_product_keys.add(product_key)

                rebuilt_rows = preserved_rows + added_rows

                kept_count = len(preserved_rows)
                added_count = len(added_rows)
                removed_count = max(len(existing_rows) - kept_count, 0)

                quote_rows = rebuilt_rows

                session["printer_draft"] = {
                    "month_key": month_key,
                    "customer_id": form["customer_id"],
                    "sales_person_id": form["sales_person_id"],
                    "default_margin": default_margin,
                    "rows": quote_rows,
                }

                if added_count and removed_count:
                    flash(
                        f"Updated list: added {added_count} product(s) and removed {removed_count} product(s).",
                        "success"
                    )
                elif added_count:
                    flash(f"Added {added_count} product(s) to the list.", "success")
                elif removed_count:
                    flash(f"Removed {removed_count} product(s) from the list.", "success")
                else:
                    flash("List updated.", "info")
            else:
                quote_rows = existing_rows

        elif action == "delete_selected":
            draft = session.get("printer_draft") or {}

            if draft and draft.get("month_key") == month_key:
                existing_rows = [
                    normalize_printer_row(r)
                    for r in (draft.get("rows") or [])
                ]

                selected_indexes = request.form.getlist("delete_row")

                try:
                    selected_indexes = {int(x) for x in selected_indexes}
                except Exception:
                    selected_indexes = set()

                updated_rows = [
                    row for i, row in enumerate(existing_rows)
                    if i not in selected_indexes
                ]

                quote_rows = updated_rows

                session["printer_draft"] = {
                    "month_key": month_key,
                    "customer_id": form["customer_id"] or str(draft.get("customer_id") or ""),
                    "sales_person_id": form["sales_person_id"] or str(draft.get("sales_person_id") or ""),
                    "default_margin": default_margin,
                    "rows": updated_rows,
                }

                if selected_indexes:
                    flash(f"Deleted {len(selected_indexes)} product(s).", "success")
                else:
                    flash("No products were selected.", "info")
            else:
                quote_rows = []

        elif action == "duplicate_row":
            draft = session.get("printer_draft") or {}

            if draft and draft.get("month_key") == month_key:
                existing_rows = [
                    normalize_printer_row(r)
                    for r in (draft.get("rows") or [])
                ]
            else:
                existing_rows = []

            try:
                idx = int(request.form.get("duplicate_row"))
            except Exception:
                idx = None

            rebuilt_rows = get_posted_printer_rows(request.form)
            if not rebuilt_rows:
                rebuilt_rows = existing_rows[:]

            if idx is not None and 0 <= idx < len(rebuilt_rows):
                row_copy = dict(rebuilt_rows[idx])
                rebuilt_rows.insert(idx + 1, normalize_printer_row(row_copy))
                flash("Row duplicated.", "success")
            else:
                flash("Could not duplicate that row.", "error")

            quote_rows = rebuilt_rows

            session["printer_draft"] = {
                "month_key": month_key,
                "customer_id": form["customer_id"] or str(draft.get("customer_id") or ""),
                "sales_person_id": form["sales_person_id"] or str(draft.get("sales_person_id") or ""),
                "default_margin": default_margin,
                "rows": quote_rows,
            }

        elif action == "clear":
            session.pop("printer_draft", None)
            session.pop("print_quote", None)

            form = {
                "customer_id": form["customer_id"],
                "sales_person_id": form["sales_person_id"],
                "default_margin": "15",
                "package_type": "",
                "pricing_period": month_key,
            }
            quote_rows = []

        elif action == "new":
            session.pop("printer_draft", None)
            session.pop("print_quote", None)

            form = {
                "customer_id": "",
                "sales_person_id": "",
                "default_margin": "15",
                "package_type": "",
                "pricing_period": month_key,
            }
            quote_rows = []

        elif action == "print":
            customer_name = (request.form.get("customer_name") or "").strip()
            customer_id = (request.form.get("customer_id") or "").strip()
            sales_person_id = (request.form.get("sales_person_id") or "").strip()
            sales_person = get_sales_person_by_id(sales_person_id)

            if not sales_person_id:
                errors.append("Select a salesperson.")
            elif not sales_person:
                errors.append("Selected salesperson was not found.")

            print_rows = get_posted_printer_rows(request.form)

            form["customer_id"] = customer_id
            form["sales_person_id"] = sales_person_id
            form["pricing_period"] = month_key

            posted_margin = (request.form.get("default_margin") or "").strip()
            form["default_margin"] = posted_margin or form.get("default_margin", "15")

            if errors:
                quote_rows = print_rows

                session["printer_draft"] = {
                    "month_key": month_key,
                    "customer_id": customer_id,
                    "sales_person_id": sales_person_id,
                    "default_margin": form["default_margin"],
                    "rows": quote_rows,
                }

            else:
                quote_data = {
                    "customer_name": customer_name,
                    "customer_id": customer_id,
                    "month_key": month_key,
                    "sales_person_id": sales_person.get("id", ""),
                    "sales_person_name": sales_person.get("name", ""),
                    "sales_person_phone": sales_person.get("phone", ""),
                    "sales_person_email": sales_person.get("email", ""),
                    "rows": print_rows,
                    "created_at": datetime.now(timezone.utc).isoformat()
                }

                session["print_quote"] = quote_data

                session["printer_draft"] = {
                    "month_key": month_key,
                    "customer_id": customer_id,
                    "sales_person_id": sales_person_id,
                    "default_margin": form["default_margin"],
                    "rows": print_rows,
                }

                return redirect(url_for("printer_print"))

        else:
            quote_rows = existing_rows

    prefill_customer_id = (request.args.get("customer_id") or "").strip()

    if prefill_customer_id:
        form["customer_id"] = prefill_customer_id
        form["pricing_period"] = month_key

        selected_customer = _find_customer_by_id(customers, prefill_customer_id)

        if selected_customer:
            default_margin = float(form.get("default_margin") or 15)

            default_letter_rows = _clean_default_letter_rows(
                selected_customer.get("default_letter_rows") or []
            )

            if default_letter_rows:
                quote_rows = [
                    _build_printer_row_from_default_letter_row(
                        default_row=row,
                        priced_by_name=priced_by_name,
                        fallback_margin=default_margin
                    )
                    for row in default_letter_rows
                ]
            else:
                default_products = _clean_default_products(
                    selected_customer.get("default_products") or []
                )

                quote_rows = [
                    normalize_printer_row(
                        _build_printer_row_from_product(
                            product_name=product_name,
                            priced_by_name=priced_by_name,
                            default_margin=default_margin
                        )
                    )
                    for product_name in default_products
                ]

    else:
        draft = session.get("printer_draft") or {}
        if draft and draft.get("month_key") == month_key:
            form["customer_id"] = str(draft.get("customer_id") or "")
            form["sales_person_id"] = str(draft.get("sales_person_id") or "")
            form["default_margin"] = str(draft.get("default_margin") or "15")
            form["pricing_period"] = month_key
            quote_rows = [
                normalize_printer_row(r)
                for r in (draft.get("rows") or [])
            ]

    customer_name = ""
    if form.get("customer_id"):
        c = next(
            (x for x in customers if str(x.get("id")) == str(form["customer_id"])),
            None
        )
        if c:
            customer_name = c.get("name", "")

    quote_rows = sorted(
        quote_rows,
        key=lambda x: (x.get("product") or "").lower()
    )

    return render_template(
        "printer.html",
        month_key=month_key,
        customers=customers,
        sales_people=sales_people,
        products=products,
        quote_rows=quote_rows,
        errors=errors,
        form=form,
        customer_name=customer_name,
        available_periods=available_periods,
        selected_pricing_period=form.get("pricing_period", month_key),
        page="printer",
        page_title="Build Letter"
    )


@app.route("/reverse-margin", methods=["GET", "POST"])
@login_required
def reverse_margin_page():
    customers = load_customers()
    company_products = load_company_products() or []
    available_periods = get_available_pricing_periods()

    today = datetime.now()
    month_key = (request.values.get("pricing_period") or current_month_key_central()).strip().upper()

    form = {
        "customer_id": (request.values.get("customer_id") or "").strip(),
        "pricing_period": month_key,
        "pricing_year": (request.values.get("pricing_year") or str(today.year)).strip(),
        "pricing_month": (request.values.get("pricing_month") or str(today.month)).strip(),
        "pricing_day": (request.values.get("pricing_day") or str(today.day)).strip(),
    }

    quote_rows = []
    errors = []

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        form["customer_id"] = (request.form.get("customer_id") or "").strip()
        form["pricing_year"] = (request.form.get("pricing_year") or "").strip()
        form["pricing_month"] = (request.form.get("pricing_month") or "").strip()
        form["pricing_day"] = (request.form.get("pricing_day") or "").strip()
        form["pricing_period"] = month_key_from(
            form["pricing_year"],
            form["pricing_month"]
        ).strip().upper()
        month_key = form["pricing_period"]

        selected_customer = _find_customer_by_id(customers, form["customer_id"]) if form["customer_id"] else None
        posted_rows = get_posted_reverse_margin_rows(request.form)

        if action == "load_customer_products":
            if selected_customer:
                quote_rows = build_reverse_margin_rows_from_customer_products(selected_customer)
            else:
                quote_rows = []

        elif action == "calculate":
            quote_rows = posted_rows if posted_rows else []

        elif action == "add_row":
            quote_rows = posted_rows[:] if posted_rows else []
            quote_rows.append({
                "product": "",
                "um": "",
                "final_price": 0.0,
                "shipping": 0.0,
                "packaging": 0.0,
            })

        elif action == "clear":
            today = datetime.now()
            form = {
                "customer_id": "",
                "pricing_period": month_key_from(str(today.year), str(today.month)),
                "pricing_year": str(today.year),
                "pricing_month": str(today.month),
                "pricing_day": str(today.day),
            }
            month_key = form["pricing_period"]
            quote_rows = []

        elif action == "save_history":
            quote_rows = posted_rows if posted_rows else []

            customer_name = ""
            if selected_customer:
                customer_name = (selected_customer.get("name") or "").strip()

            pricing_date = ""
            try:
                y = int(form.get("pricing_year") or 0)
                m = int(form.get("pricing_month") or 0)
                d = int(form.get("pricing_day") or 0)
                pricing_date = datetime(y, m, d).strftime("%Y-%m-%d")
            except Exception:
                pricing_date = ""

            if not customer_name:
                errors.append("Customer is required before saving margin history.")

            if not pricing_date:
                errors.append("A valid pricing date is required before saving margin history.")

            valid_rows = []
            enriched_rows = enrich_reverse_margin_rows(quote_rows, month_key)

            for i, row in enumerate(enriched_rows, start=1):
                product = (row.get("product") or "").strip()
                um = normalize_um(row.get("um", ""))
                cost = to_float(row.get("historical_cost", 0.0), 0.0)
                final_price = to_float(row.get("final_price", 0.0), 0.0)

                if not product:
                    continue

                if not um:
                    errors.append(f"Row {i}: U/M is required to save history.")
                    continue

                if cost <= 0:
                    errors.append(f"Row {i}: cost must be greater than 0 to save history.")
                    continue

                if final_price <= 0:
                    errors.append(f"Row {i}: final price must be greater than 0 to save history.")
                    continue

                valid_rows.append(row)

            if not errors and valid_rows:
                user = find_user_by_id(session["user_id"])

                save_reverse_margin_rows_to_history(
                    customer_name=customer_name,
                    pricing_date=pricing_date,
                    rows=valid_rows,
                    user_row=user
                )

                flash("Reverse margin history saved.", "success")
                quote_rows = valid_rows
            elif not errors and not valid_rows:
                errors.append("No valid rows were available to save.")
                quote_rows = posted_rows if posted_rows else []
            else:
                quote_rows = posted_rows if posted_rows else []

        else:
            # Default POST behavior:
            # preserve existing posted rows only.
            # Do NOT auto-load customer defaults here.
            # Do NOT auto-create blank rows here.
            quote_rows = posted_rows if posted_rows else []

    else:
        # Initial GET:
        # keep empty unless you explicitly want a customer's defaults loaded.
        if form.get("customer_id"):
            selected_customer = _find_customer_by_id(customers, form["customer_id"])
            if selected_customer:
                quote_rows = build_reverse_margin_rows_from_customer_products(selected_customer)

    enriched_rows = enrich_reverse_margin_rows(quote_rows, month_key) if quote_rows else []
    reverse_margin_summary = build_reverse_margin_summary(enriched_rows)

    return render_template(
        "reverse_margin.html",
        customers=customers,
        company_products=company_products,
        available_periods=available_periods,
        selected_pricing_period=month_key,
        form=form,
        quote_rows=enriched_rows,
        reverse_margin_summary=reverse_margin_summary,
        errors=errors,
        page="reverse_margin",
        page_title="Reverse Margin Calculator"
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
        "full_name": user.full_name or "",
        "email": user.email or "",
        "phone": user.phone or "",
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
                session["email"] = updated_user.email or ""
                session["full_name"] = updated_user.full_name or ""
                session["phone"] = updated_user.phone or ""

                flash("User profile updated.", "success")
                return redirect(url_for("users_page"))
            except sqlite3.IntegrityError:
                errors.append("That email is already in use.")

    return render_template(
        "users.html",
        form=form,
        errors=errors,
        page="users",
        page_title="Users"
    )


@app.route("/admin/users", methods=["GET", "POST"])
@login_required
@admin_required
def admin_users_page():
    errors = []
    company_info = load_company_info()
    company_products = load_company_products()
    

    if request.method == "POST":
        if "duplicate_row" in request.form:
            action = "duplicate_row"
        elif "delete_row" in request.form and (request.form.get("action") or "").strip().lower() == "delete_selected":
            action = "delete_selected"
        else:
            action = (request.form.get("action") or "build").strip().lower()

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
        company_products=company_products,
        page="admin",
        page_title="Admin"
    )

@app.route("/printer/print")
@login_required
def printer_print():
    stored = session.get("print_quote") or {}
    company_info = load_company_info()
    user = find_user_by_id(session.get("user_id"))

    # unwrap the actual quote object
    if isinstance(stored.get("quote"), dict):
        quote = stored.get("quote") or {}
    else:
        quote = stored

    # force rows onto the object the template reads
    quote_rows = quote.get("rows") or []

    customer_id = (
        quote.get("customer_id")
        or stored.get("customer_id")
        or ""
    )
    month_key = (
        quote.get("month_key")
        or stored.get("month_key")
        or ""
    )

    if customer_id and month_key:
        mark_customer_todo_done(
            user_id=session.get("user_id"),
            month_key=str(month_key).strip().upper(),
            customer_id=str(customer_id).strip(),
            history_id=stored.get("id", ""),
            file_name=stored.get("file_name", "")
        )

    display_date = ""

    selected_month_key = str(
        quote.get("month_key")
        or stored.get("month_key")
        or ""
    ).strip().upper()

    today = datetime.now()
    current_month_key = f"{today.year}-{today.strftime('%b').upper()}"

    if selected_month_key and selected_month_key != current_month_key:
        try:
            parts = selected_month_key.split("-")
            selected_year = int(parts[0])
            selected_mon = parts[1]

            month_num_map = {
                "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4,
                "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8,
                "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
            }

            selected_month_num = month_num_map.get(selected_mon)
            if selected_month_num:
                display_date = f"{selected_month_num}/1/{selected_year}"
        except Exception:
            display_date = ""
    else:
        display_date = f"{today.month}/{today.day}/{today.year}"

    sales_person = {
        "name": quote.get("sales_person_name", "") or stored.get("sales_person_name", ""),
        "phone": quote.get("sales_person_phone", "") or stored.get("sales_person_phone", ""),
        "email": quote.get("sales_person_email", "") or stored.get("sales_person_email", ""),
    }

    return render_template(
        "printer_print.html",
        quote=quote,
        quote_rows=quote_rows,
        company_info=company_info,
        user=user,
        sales_person=sales_person,
        display_date=display_date,
        page="print",
        page_title="Print Letter"
    )


@app.route("/history/price-letter/<entry_id>")
@login_required
def open_price_letter_history(entry_id):
    entry = get_price_letter_history_entry(entry_id)

    if not entry:
        flash("Saved price letter not found.", "error")
        return redirect(url_for("history_page"))

    session["print_quote"] = entry
    return redirect(url_for("printer_print", from_history=1))


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
        page="history",
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
        row_products = request.form.getlist("row_product")
        row_ums = request.form.getlist("row_um")
        row_prices = request.form.getlist("row_price")
        row_freights = request.form.getlist("row_freight_tax")
        row_finals = request.form.getlist("row_final_price")

        updated_rows = []

        for i, row_id in enumerate(row_ids):
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

            if not product:
                errors.append(f"Row {i+1}: Product is required.")

            updated_rows.append({
                "id": row_id,
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

@app.route("/products", methods=["GET", "POST"])
@login_required
def products_page():
    errors = []
    products = load_company_products()

    single_product_name = ""
    single_lb_per_gal = ""
    mass_product_data = ""

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add_single_product":
            product_name = (request.form.get("product_name") or "").strip()
            lb_raw = (request.form.get("lb_per_gal") or "").strip()

            single_product_name = product_name
            single_lb_per_gal = lb_raw

            if not product_name:
                errors.append("Product name is required.")

            try:
                lb_per_gal = float(lb_raw.replace(",", ""))
            except Exception:
                lb_per_gal = None
                errors.append("LB/GAL must be a number.")

            if not errors:
                existing_map = {
                    normalize_product_name(p.get("product")): p
                    for p in products
                }

                key = normalize_product_name(product_name)

                if key in existing_map:
                    existing_map[key]["product"] = product_name
                    existing_map[key]["lb_per_gal"] = lb_per_gal
                    flash("Product updated.", "success")
                else:
                    products.append({
                        "id": uuid.uuid4().hex[:10],
                        "product": product_name,
                        "lb_per_gal": lb_per_gal,
                        "created_at": datetime.now(timezone.utc).isoformat()
                    })
                    flash("Product added.", "success")

                products.sort(key=lambda x: normalize_product_name(x.get("product")))
                save_company_products(products)
                return redirect(url_for("products_page"))

        elif action == "mass_add_products":
            mass_product_data = (request.form.get("mass_product_data") or "").strip()

            if not mass_product_data:
                errors.append("Paste list is empty.")
            else:
                existing_map = {
                    normalize_product_name(p.get("product")): p
                    for p in products
                }

                lines = [ln.strip() for ln in mass_product_data.splitlines() if ln.strip()]
                added_count = 0
                updated_count = 0

                for i, line in enumerate(lines, start=1):
                    if "\t" in line:
                        parts = [p.strip() for p in line.split("\t")]
                    else:
                        parts = [p.strip() for p in line.split(",")]

                    if len(parts) != 2:
                        errors.append(f"Line {i}: expected 2 columns (Product, LB/GAL).")
                        continue

                    product_name, lb_raw = parts

                    if not product_name:
                        errors.append(f"Line {i}: product name is required.")
                        continue

                    try:
                        lb_per_gal = float(lb_raw.replace(",", ""))
                    except Exception:
                        errors.append(f"Line {i}: LB/GAL must be numeric.")
                        continue

                    key = normalize_product_name(product_name)
                    if key in existing_map:
                        existing_map[key]["product"] = product_name
                        existing_map[key]["lb_per_gal"] = lb_per_gal
                        updated_count += 1
                    else:
                        row = {
                            "id": uuid.uuid4().hex[:10],
                            "product": product_name,
                            "lb_per_gal": lb_per_gal,
                            "created_at": datetime.now(timezone.utc).isoformat()
                        }
                        products.append(row)
                        existing_map[key] = row
                        added_count += 1

                if not errors:
                    products.sort(key=lambda x: normalize_product_name(x.get("product")))
                    save_company_products(products)
                    flash(f"Saved {added_count} new product(s) and updated {updated_count} existing product(s).", "success")
                    return redirect(url_for("products_page"))

        elif action == "save_products_table":
            product_ids = request.form.getlist("product_id")
            row_product_names = request.form.getlist("row_product_name")
            row_lb_values = request.form.getlist("row_lb_per_gal")

            updated_rows = []

            for i, product_id in enumerate(product_ids):
                product_name = (row_product_names[i] if i < len(row_product_names) else "").strip()
                lb_raw = (row_lb_values[i] if i < len(row_lb_values) else "").strip()

                if not product_name:
                    errors.append(f"Row {i+1}: product name is required.")
                    continue

                try:
                    lb_per_gal = float(lb_raw.replace(",", ""))
                except Exception:
                    errors.append(f"Row {i+1}: LB/GAL must be numeric.")
                    continue

                existing = next((p for p in products if str(p.get("id")) == str(product_id)), {})
                updated_rows.append({
                    "id": product_id,
                    "product": product_name,
                    "lb_per_gal": lb_per_gal,
                    "created_at": existing.get("created_at") or datetime.now(timezone.utc).isoformat()
                })

            if not errors:
                updated_rows.sort(key=lambda x: normalize_product_name(x.get("product")))
                save_company_products(updated_rows)
                flash("Products updated.", "success")
                return redirect(url_for("products_page"))

            products = updated_rows

        elif action == "delete_selected_products":
            delete_ids = set(request.form.getlist("delete_product_id"))

            if not delete_ids:
                flash("No products were selected.", "info")
                return redirect(url_for("products_page"))

            products = [p for p in products if str(p.get("id")) not in delete_ids]
            save_company_products(products)
            flash("Selected products deleted.", "success")
            return redirect(url_for("products_page"))

    return render_template(
        "products.html",
        products=products,
        errors=errors,
        single_product_name=single_product_name,
        single_lb_per_gal=single_lb_per_gal,
        mass_product_data=mass_product_data,
        page="app",
        page_title="Products"
    )

@app.route("/printer/mark_printed", methods=["POST"])
@login_required
def printer_mark_printed():
    stored = session.get("print_quote") or {}

    if not stored:
        return {"ok": False, "error": "No active print quote found."}, 400

    if isinstance(stored.get("quote"), dict):
        quote = stored.get("quote") or {}
    else:
        quote = stored

    customer_name = str(
        stored.get("customer_name")
        or quote.get("customer_name")
        or ""
    ).strip()

    customer_id = str(
        stored.get("customer_id")
        or quote.get("customer_id")
        or ""
    ).strip()

    month_key = str(
        stored.get("month_key")
        or quote.get("month_key")
        or ""
    ).strip().upper()

    created_at = (
        stored.get("created_at")
        or quote.get("created_at")
        or datetime.now(timezone.utc).isoformat()
    )

    history_id = str(stored.get("id") or uuid.uuid4().hex[:10]).strip()

    file_name = str(stored.get("file_name") or "").strip()
    if not file_name:
        safe_customer = make_safe_filename_part(customer_name or "PriceLetter")
        file_display_date = display_date_from_month_key(month_key)
        safe_file_date = file_display_date.replace("/", "-")
        file_name = f"{safe_customer}-{safe_file_date}.pdf"

    user_snapshot = user_snapshot_from_id(session.get("user_id"))

    history_entry = {
        "id": history_id,
        "file_name": file_name,
        "customer_name": customer_name,
        "customer_id": customer_id,
        "month_key": month_key,
        "created_at": created_at,
        "created_by": user_snapshot.get("full_name", ""),
        "quote": quote,
        "company_info": load_company_info(),
        "user": user_snapshot,
        "sales_person_id": str(
            stored.get("sales_person_id")
            or quote.get("sales_person_id")
            or ""
        ),
        "sales_person_name": str(
            stored.get("sales_person_name")
            or quote.get("sales_person_name")
            or ""
        ),
        "sales_person_phone": str(
            stored.get("sales_person_phone")
            or quote.get("sales_person_phone")
            or ""
        ),
        "sales_person_email": str(
            stored.get("sales_person_email")
            or quote.get("sales_person_email")
            or ""
        ),
    }

    history = load_price_letter_history()
    already_exists = any(str(x.get("id")) == history_id for x in history)

    if not already_exists:
        add_price_letter_history(history_entry)

    # Automatically save final downloaded price-letter rows
    # into product margin history.
    save_price_letter_rows_to_margin_history(
        quote=quote,
        user_row=user_snapshot
    )

    if customer_id and month_key:
        mark_customer_todo_done(
            user_id=session.get("user_id"),
            month_key=month_key,
            customer_id=customer_id,
            history_id=history_id,
            file_name=file_name
        )

    session.pop("print_quote", None)
    session.pop("printer_draft", None)

    return {"ok": True}

@app.route("/sales-people", methods=["GET", "POST"])
@login_required
def sales_people_page():
    errors = []

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "add":
            name = (request.form.get("name") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            email = (request.form.get("email") or "").strip()

            if not name:
                errors.append("Name is required.")
            if not phone:
                errors.append("Phone is required.")
            if not email:
                errors.append("Email is required.")

            if not errors:
                add_sales_person(name, phone, email)
                flash("Salesperson added successfully.", "success")
                return redirect(url_for("sales_people_page"))

    sales_people = load_sales_people()

    return render_template(
        "sales_people.html",
        sales_people=sales_people,
        errors=errors,
        page="app",
        page_title="Salespeople",
    )


@app.route("/sales-people/<sales_person_id>", methods=["GET", "POST"])
@login_required
def view_sales_account_page(sales_person_id):
    sales_person = get_sales_person_by_id(sales_person_id)
    if not sales_person:
        flash("Salesperson not found.", "error")
        return redirect(url_for("sales_people_page"))

    errors = []

    if request.method == "POST":
        action = (request.form.get("action") or "").strip()

        if action == "save":
            name = (request.form.get("name") or "").strip()
            phone = (request.form.get("phone") or "").strip()
            email = (request.form.get("email") or "").strip()

            if not name:
                errors.append("Name is required.")
            if not phone:
                errors.append("Phone is required.")
            if not email:
                errors.append("Email is required.")

            if not errors:
                update_sales_person(sales_person_id, name, phone, email)
                flash("Salesperson updated successfully.", "success")
                return redirect(url_for("view_sales_account_page", sales_person_id=sales_person_id))

        elif action == "delete":
            delete_sales_person(sales_person_id)
            flash("Salesperson deleted.", "success")
            return redirect(url_for("sales_people_page"))

    sales_person = get_sales_person_by_id(sales_person_id)

    return render_template(
        "view_sales_account.html",
        sales_person=sales_person,
        errors=errors,
        page="app",
        page_title="View Sales Account",
    )

@app.route("/todos", methods=["GET", "POST"])
@login_required
def todo_page():
    customers = load_customers()
    customer_map = {str(c.get("id")): c for c in customers}
    month_key = get_todo_month_key()

    todo_config = load_todo_config()
    recurring_customer_ids = [
        str(cid).strip()
        for cid in todo_config.get("recurring_customer_ids", [])
        if str(cid).strip()
    ]

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add":
            customer_id = (request.form.get("customer_id") or "").strip()

            if not customer_id:
                flash("Select a customer.", "error")
            elif customer_id not in customer_map:
                flash("Selected customer was not found.", "error")
            elif customer_id in recurring_customer_ids:
                flash("That customer is already on the monthly to-do list.", "info")
            else:
                recurring_customer_ids.append(customer_id)
                todo_config["recurring_customer_ids"] = recurring_customer_ids
                save_todo_config(todo_config)
                flash("Customer added to monthly to-do list.", "success")

            return redirect(url_for("todo_page"))

        if action == "remove_selected":
            remove_ids = {
                str(cid).strip()
                for cid in request.form.getlist("remove_customer_ids")
                if str(cid).strip()
            }

            if remove_ids:
                recurring_customer_ids = [
                    cid for cid in recurring_customer_ids
                    if cid not in remove_ids
                ]
                todo_config["recurring_customer_ids"] = recurring_customer_ids
                save_todo_config(todo_config)
                flash("Selected customers removed from the monthly to-do list.", "success")
            else:
                flash("No customers were selected.", "info")

            return redirect(url_for("todo_page"))

    letter_history = load_price_letter_history()

    enriched = []
    for customer_id in recurring_customer_ids:
        customer = customer_map.get(str(customer_id))
        if not customer:
            continue

        customer_name = (customer.get("name") or "").strip()
        customer_name_key = customer_name.lower()
        target_month_key = str(month_key).strip().upper()

        matched_history = next(
            (
                h for h in letter_history
                if (str(h.get("customer_name") or "").strip().lower() == customer_name_key)
                and (str(h.get("month_key") or "").strip().upper() == target_month_key)
            ),
            None
        )

        enriched.append({
            "customer_id": customer_id,
            "customer_name": customer_name,
            "printer_url": url_for(
                "printer_page",
                customer_id=customer.get("id"),
                pricing_period=month_key
            ),
            "done": bool(matched_history),
            "history_url": (
                url_for("open_price_letter_history", entry_id=matched_history.get("id"))
                if matched_history else ""
            ),
            "file_name": (
                matched_history.get("file_name", "")
                if matched_history else ""
            ),
        })

    enriched.sort(key=lambda x: (x.get("customer_name") or "").lower())

    return render_template(
        "todos.html",
        customers=sorted(customers, key=lambda x: (x.get("name") or "").lower()),
        todo_items=enriched,
        month_key=month_key,
        month_label=month_label_from_key(month_key),
        page="todos",
        page_title="Monthly To-Do List"
    )

@app.route("/todo-setup", methods=["GET", "POST"])
@login_required
def todo_setup_page():
    customers = load_customers()
    customer_map = {str(c.get("id")): c for c in customers}

    todo_config = load_todo_config()
    recurring_customer_ids = [
        str(cid).strip()
        for cid in todo_config.get("recurring_customer_ids", [])
        if str(cid).strip()
    ]

    if request.method == "POST":
        action = (request.form.get("action") or "").strip().lower()

        if action == "add":
            customer_id = (request.form.get("customer_id") or "").strip()

            if not customer_id:
                flash("Select a customer.", "error")
            elif customer_id not in customer_map:
                flash("Selected customer was not found.", "error")
            elif customer_id in recurring_customer_ids:
                flash("That customer is already in the saved selection.", "info")
            else:
                recurring_customer_ids.append(customer_id)
                todo_config["recurring_customer_ids"] = recurring_customer_ids
                save_todo_config(todo_config)
                flash("Customer added to saved selection.", "success")

            return redirect(url_for("todo_setup_page"))

        if action == "remove_selected":
            remove_ids = {
                str(cid).strip()
                for cid in request.form.getlist("remove_customer_ids")
                if str(cid).strip()
            }

            if remove_ids:
                recurring_customer_ids = [
                    cid for cid in recurring_customer_ids
                    if cid not in remove_ids
                ]
                todo_config["recurring_customer_ids"] = recurring_customer_ids
                save_todo_config(todo_config)
                flash("Selected customers removed.", "success")
            else:
                flash("No customers were selected.", "info")

            return redirect(url_for("todo_setup_page"))

    selected_customers = []
    for customer_id in recurring_customer_ids:
        customer = customer_map.get(customer_id)
        if customer:
            selected_customers.append({
                "customer_id": customer_id,
                "customer_name": (customer.get("name") or "").strip()
            })

    selected_customers.sort(key=lambda x: x["customer_name"].lower())

    return render_template(
        "todo_setup.html",
        customers=sorted(customers, key=lambda x: (x.get("name") or "").lower()),
        selected_customers=selected_customers,
        page="todo_setup",
        page_title="To-Do Setup"
    )


@app.route("/analytics", methods=["GET"])
@login_required
def analytics_page():
    filters = {
        "product": (request.args.get("product") or "").strip(),
        "customer": (request.args.get("customer") or "").strip(),
        "source": (request.args.get("source") or "").strip(),
        "um": (request.args.get("um") or "").strip(),
        "start_date": (request.args.get("start_date") or "").strip(),
        "end_date": (request.args.get("end_date") or "").strip(),
    }

    records = get_margin_history_records(filters)
    summary = build_margin_analytics_summary(records)
    product_rollup = build_margin_product_rollup(records)
    chart_points = build_margin_chart_points(records)
    filter_options = get_margin_filter_options()

    return render_template(
        "analytics.html",
        filters=filters,
        summary=summary,
        product_rollup=product_rollup,
        records=records,
        chart_points=chart_points,
        filter_options=filter_options,
        page="analytics",
        page_title="Analytics"
    )

@app.route("/margin-analytics", methods=["GET"])
@login_required
def margin_analytics_page():
    product = (request.args.get("product") or "").strip()

    store = load_margin_history()
    records = store.get("records", []) or []

    product_options = sorted({
        str(r.get("product") or "").strip()
        for r in records
        if str(r.get("product") or "").strip()
    }, key=lambda x: x.lower())

    filtered = []
    for r in records:
        if not is_meaningful_margin_record(r):
            continue

        if product and normalize_product_name(r.get("product")) != normalize_product_name(product):
            continue

        filtered.append(r)

    filtered.sort(key=lambda r: (
        normalize_pricing_date(r.get("pricing_date")),
        int(r.get("entry_seq") or 0)
    ))

    normalized_records = []
    for r in filtered:
        normalized = normalize_margin_record_for_analytics(r)
        if isinstance(normalized, dict):
            normalized_records.append(normalized)

    chart_data = []
    for r in normalized_records:
        chart_data.append({
            "date": normalize_pricing_date(r.get("pricing_date")),
            "cost": round(to_float(r.get("normalized_cost"), 0.0), 4),
            "final_price": round(to_float(r.get("normalized_final_price"), 0.0), 4),
            "margin": round(to_float(r.get("margin_pct"), 0.0), 2),
            "default_um": r.get("default_um", ""),
            "saved_um": r.get("saved_um", ""),
        })

    analytics_um = ""
    if normalized_records:
        analytics_um = normalized_records[0].get("default_um", "")

    return render_template(
        "margin_analytics.html",
        page="analytics",
        product=product,
        product_options=product_options,
        chart_data=chart_data,
        records=normalized_records,
        analytics_um=analytics_um
    )


@app.route("/health")
def health():
    return "ok", 200


# -------------------------
# Local dev only
# -------------------------
if __name__ == "__main__":
    with app.app_context():
        init_db()
    app.run(debug=True)