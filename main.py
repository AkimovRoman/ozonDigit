from __future__ import annotations

from decimal import Decimal
from typing import Any
from datetime import date

from fastapi import FastAPI, Request, Form
from fastapi.responses import RedirectResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from sqlalchemy import select

from app.db import SessionLocal
from app.models import OzonAccount, UserOzonAccount, Product

from app.settings_router import router as settings_router
from app.auth import router as auth_router
from app.products_router import router as products_router
from app.performance_router import router as performance_router
from app.rnp_import import router as rnp_import_router
from app.rnp_service_big import build_rnp_big_view
from app.campaigns_router import router as campaigns_router
import app.rnp_import_reports_only as rnp_import_reports_only


app = FastAPI()

app.add_middleware(
    SessionMiddleware,
    secret_key="CHANGE_ME_TO_RANDOM_SECRET",
)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")
app.state.templates = templates


def _to_decimal(v: Any) -> Decimal | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return v
    try:
        return Decimal(str(v))
    except Exception:
        return None


def fmt_int(v: Any) -> str:
    if v is None:
        return "-"
    try:
        n = int(v)
        return f"{n:,}".replace(",", " ")
    except Exception:
        return str(v)


def fmt_money(v: Any, digits: int = 0) -> str:
    d = _to_decimal(v)
    if d is None:
        return "-"
    q = Decimal("1") if digits == 0 else Decimal("1." + "0" * digits)
    d = d.quantize(q)
    s = f"{d:,}".replace(",", " ")
    s = s.replace(".", ",")
    return f"{s} ₽"


def fmt_num(v: Any, digits: int = 2) -> str:
    d = _to_decimal(v)
    if d is None:
        return "-"
    q = Decimal("1." + "0" * digits)
    d = d.quantize(q)
    return f"{d}".replace(".", ",")


def fmt_pct(v: Any, digits: int = 2, with_sign: bool = False) -> str:
    d = _to_decimal(v)
    if d is None:
        return "-"
    q = Decimal("1." + "0" * digits)
    d = d.quantize(q)
    s = f"{d}".replace(".", ",")
    return f"{s}%" if with_sign else s


templates.env.filters["fmt_int"] = fmt_int
templates.env.filters["fmt_money"] = fmt_money
templates.env.filters["fmt_num"] = fmt_num
templates.env.filters["fmt_pct"] = fmt_pct


def get_user_id(request: Request) -> int | None:
    return request.session.get("user_id")


def login_required(request: Request):
    if not get_user_id(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


async def get_active_ozon_context(request: Request):
    user_id = get_user_id(request)
    if not user_id:
        return None, []

    active_id = request.session.get("active_ozon_account_id")

    async with SessionLocal() as session:
        accounts = (await session.execute(
            select(OzonAccount)
            .join(UserOzonAccount, UserOzonAccount.ozon_account_id == OzonAccount.id)
            .where(UserOzonAccount.user_id == user_id)
            .order_by(OzonAccount.id.asc())
        )).scalars().all()

        if not accounts:
            request.session.pop("active_ozon_account_id", None)
            return None, []

        if (not active_id) or (not any(a.id == active_id for a in accounts)):
            active_id = accounts[0].id
            request.session["active_ozon_account_id"] = active_id

        active_account = next(a for a in accounts if a.id == active_id)
        return active_account, accounts


async def get_active_product_context(request: Request):
    active_account, _ = await get_active_ozon_context(request)
    if not active_account:
        request.session.pop("active_product_id", None)
        return None, []

    active_product_id = request.session.get("active_product_id")

    async with SessionLocal() as session:
        products = (await session.execute(
            select(Product)
            .where(
                Product.ozon_account_id == active_account.id,
                Product.is_active == True,  # noqa: E712
            )
            .order_by(Product.product_id.asc())
        )).scalars().all()

        if not products:
            request.session.pop("active_product_id", None)
            return None, []

        if (not active_product_id) or (not any(p.product_id == active_product_id for p in products)):
            active_product_id = products[0].product_id
            request.session["active_product_id"] = active_product_id

        active_product = next(p for p in products if p.product_id == active_product_id)
        return active_product, products


app.state.get_active_ozon_context = get_active_ozon_context

app.include_router(auth_router)
app.include_router(settings_router)
app.include_router(products_router)
app.include_router(performance_router)
app.include_router(rnp_import_router)
app.include_router(campaigns_router)
app.include_router(rnp_import_reports_only.router)


@app.post("/switch-ozon-account")
async def switch_ozon_account(request: Request, ozon_account_id: int = Form(...)):
    redirect = login_required(request)
    if redirect:
        return redirect

    request.session["active_ozon_account_id"] = ozon_account_id
    referer = request.headers.get("referer") or "/"
    return RedirectResponse(url=referer, status_code=303)


# ✅ сохраняем настройки РНП в session (ширина, шрифт, жирность)
@app.post("/rnp/prefs")
async def rnp_prefs(
    request: Request,
    metric_width: int | None = Form(None),
    font_size: int | None = Form(None),
    font_weight: int | None = Form(None),
):
    redirect = login_required(request)
    if redirect:
        return redirect

    if metric_width is not None:
        metric_width = max(160, min(520, metric_width))
        request.session["rnp_metric_width"] = metric_width

    if font_size is not None:
        font_size = max(8, min(16, font_size))
        request.session["rnp_font_size"] = font_size

    if font_weight is not None:
        if font_weight not in (300, 400, 600):
            font_weight = 400
        request.session["rnp_font_weight"] = font_weight

    return PlainTextResponse("OK")


def _parse_iso_date(s: str | None) -> str | None:
    if not s:
        return None
    try:
        date.fromisoformat(s)
        return s
    except Exception:
        return None


# ✅ выбор диапазона дат (пока только сохраняем и отображаем сверху)
@app.post("/rnp/date-range")
async def rnp_date_range(
    request: Request,
    date_from: str = Form(...),
    date_to: str = Form(...),
):
    redirect = login_required(request)
    if redirect:
        return redirect

    df = _parse_iso_date(date_from)
    dt = _parse_iso_date(date_to)

    # ❗Если кто-то всё же попытался очистить дату (удалить) — просто не применяем
    if not df or not dt:
        return RedirectResponse(url="/rnp", status_code=303)

    # нормализуем порядок
    if df > dt:
        df, dt = dt, df

    request.session["rnp_view_date_from"] = df
    request.session["rnp_view_date_to"] = dt

    return RedirectResponse(url="/rnp", status_code=303)


@app.get("/")
async def dashboard(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, accounts = await get_active_ozon_context(request)

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "active_page": "dashboard",
            "active_ozon_account": active_account,
            "ozon_accounts": accounts,
        },
    )


@app.get("/rnp")
async def rnp(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, accounts = await get_active_ozon_context(request)
    active_product, products = await get_active_product_context(request)

    rnp_view = None
    if active_account and active_product:
        async with SessionLocal() as session:
            rnp_view = await build_rnp_big_view(
                session=session,
                ozon_account_id=active_account.id,
                product_id=active_product.product_id,
            )

    metric_width = request.session.get("rnp_metric_width", 260)
    font_size = request.session.get("rnp_font_size", 10)
    font_weight = request.session.get("rnp_font_weight", 400)

    # --- диапазон дат ---
    sess_df = request.session.get("rnp_view_date_from")  # ISO YYYY-MM-DD
    sess_dt = request.session.get("rnp_view_date_to")    # ISO YYYY-MM-DD

    # если в сессии не задано — берём из rnp_view
    eff_df = sess_df or (rnp_view.get("date_from") if rnp_view else None)
    eff_dt = sess_dt or (rnp_view.get("date_to") if rnp_view else None)

    # для flatpickr нужны ISO-значения или пусто
    date_from_iso = eff_df or ""
    date_to_iso = eff_dt or ""

    # для кнопки сверху — красивая строка
    def _fmt_ru(iso: str) -> str:
        # iso: YYYY-MM-DD
        try:
            y, m, d = iso.split("-")
            return f"{d}.{m}.{y}"
        except Exception:
            return iso

    if eff_df and eff_dt:
        date_range_label = f"{_fmt_ru(eff_df)} — {_fmt_ru(eff_dt)}"
    else:
        date_range_label = "Выбрать даты"

    # min/max (если есть данные) — чтобы нельзя было выбрать вне диапазона данных по товару
    min_iso = rnp_view.get("date_from") if rnp_view else ""
    max_iso = rnp_view.get("date_to") if rnp_view else ""

    return templates.TemplateResponse(
        "rnp.html",
        {
            "request": request,
            "active_page": "rnp",
            "active_ozon_account": active_account,
            "ozon_accounts": accounts,
            "active_product": active_product,
            "products": products,
            "rnp_view": rnp_view,
            "metric_width": metric_width,
            "font_size": font_size,
            "font_weight": font_weight,

            # ✅ для UI
            "date_range_label": date_range_label,

            # ✅ для календаря (ISO)
            "date_from_iso": date_from_iso,
            "date_to_iso": date_to_iso,

            # ✅ ограничения по данным (ISO)
            "min_iso": min_iso,
            "max_iso": max_iso,
        },
    )


@app.get("/forecast")
async def forecast(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, accounts = await get_active_ozon_context(request)

    return templates.TemplateResponse(
        "forecast.html",
        {
            "request": request,
            "active_page": "forecast",
            "active_ozon_account": active_account,
            "ozon_accounts": accounts,
        },
    )


@app.get("/__ping__", response_class=PlainTextResponse)
async def __ping__():
    return "MAIN.PY IS RUNNING"


@app.get("/__routes__", response_class=PlainTextResponse)
async def __routes__():
    lines = []
    for r in app.router.routes:
        methods = getattr(r, "methods", None)
        path = getattr(r, "path", None)
        name = getattr(r, "name", None)
        if path:
            lines.append(f"{sorted(list(methods)) if methods else ''} {path} -> {name}")
    return "\n".join(lines)


@app.post("/switch-product")
async def switch_product(request: Request, product_id: int = Form(...)):
    redirect = login_required(request)
    if redirect:
        return redirect

    request.session["active_product_id"] = product_id
    referer = request.headers.get("referer") or "/rnp:**"
    return RedirectResponse(url="/rnp", status_code=303)
