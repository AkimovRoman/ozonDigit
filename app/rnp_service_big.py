from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    AdsTopDaily,
    AdsStencilDaily,
    AdsTotalDaily,
    ConversionsDaily,
    PricingDaily,
    TrackedQuery,
    TrackedQueryDaily,
)


def _as_key(d: date) -> str:
    return d.isoformat()


def _get_attr(obj: Any, name: str) -> Any:
    return getattr(obj, name, None)


def _json_safe(v: Any) -> Any:
    """
    Делает структуру данных пригодной для JSON (для Jinja |tojson):
    - Decimal -> float
    - date/datetime -> isoformat
    - list/tuple/dict -> рекурсивно
    """
    if v is None:
        return None

    if isinstance(v, Decimal):
        return float(v)

    if isinstance(v, (date, datetime)):
        return v.isoformat()

    if isinstance(v, dict):
        return {k: _json_safe(val) for k, val in v.items()}

    if isinstance(v, list):
        return [_json_safe(x) for x in v]

    if isinstance(v, tuple):
        return [_json_safe(x) for x in v]

    return v


async def _fetch_all_by_day(
    session: AsyncSession,
    model: Any,
    ozon_account_id: int,
    product_id: int,
) -> dict[str, Any]:
    q = (
        select(model)
        .where(
            model.ozon_account_id == ozon_account_id,
            model.product_id == product_id,
        )
        .order_by(model.day.asc())
    )
    rows = (await session.execute(q)).scalars().all()
    return {_as_key(r.day): r for r in rows}


def _values_by_day(dates: list[date], by_day: dict[str, Any], field: str) -> list[Any]:
    out: list[Any] = []
    for d in dates:
        row = by_day.get(_as_key(d))
        out.append(_get_attr(row, field) if row is not None else None)
    return out


def _fmt_query_pos_cpc(pos: Any, cpc: Any) -> str:
    left = "-" if pos is None else str(pos)
    right = "-"
    if cpc is not None:
        right = str(Decimal(str(cpc)).quantize(Decimal("1.00"))).replace(".", ",")
    return f"{left} / {right}"


async def build_rnp_big_view(
    session: AsyncSession,
    ozon_account_id: int,
    product_id: int,
) -> dict[str, Any] | None:
    """
    Одна большая таблица:
    - sticky top: даты
    - sticky left: (блок, показатель)
    """

    top_by_day = await _fetch_all_by_day(session, AdsTopDaily, ozon_account_id, product_id)
    traf_by_day = await _fetch_all_by_day(session, AdsStencilDaily, ozon_account_id, product_id)
    total_by_day = await _fetch_all_by_day(session, AdsTotalDaily, ozon_account_id, product_id)
    conv_by_day = await _fetch_all_by_day(session, ConversionsDaily, ozon_account_id, product_id)
    price_by_day = await _fetch_all_by_day(session, PricingDaily, ozon_account_id, product_id)

    # queries
    queries = (await session.execute(
        select(TrackedQuery)
        .where(
            TrackedQuery.ozon_account_id == ozon_account_id,
            TrackedQuery.product_id == product_id,
            TrackedQuery.is_active == True,  # noqa: E712
        )
        .order_by(TrackedQuery.sort_order.asc(), TrackedQuery.id.asc())
    )).scalars().all()

    query_ids = [q.id for q in queries]
    qdaily_map: dict[tuple[int, str], TrackedQueryDaily] = {}
    qdaily_days: set[date] = set()

    if query_ids:
        qd = (await session.execute(
            select(TrackedQueryDaily)
            .where(
                TrackedQueryDaily.ozon_account_id == ozon_account_id,
                TrackedQueryDaily.product_id == product_id,
                TrackedQueryDaily.query_id.in_(query_ids),
            )
            .order_by(TrackedQueryDaily.day.asc())
        )).scalars().all()

        for row in qd:
            qdaily_map[(row.query_id, _as_key(row.day))] = row
            qdaily_days.add(row.day)

    # union dates
    all_days: set[date] = set()
    for k in top_by_day.keys():
        all_days.add(date.fromisoformat(k))
    for k in traf_by_day.keys():
        all_days.add(date.fromisoformat(k))
    for k in total_by_day.keys():
        all_days.add(date.fromisoformat(k))
    for k in conv_by_day.keys():
        all_days.add(date.fromisoformat(k))
    for k in price_by_day.keys():
        all_days.add(date.fromisoformat(k))
    all_days |= qdaily_days

    if not all_days:
        return None

    dates = sorted(all_days)
    date_from = dates[0]
    date_to = dates[-1]

    rows: list[dict[str, Any]] = []

    def add_block(block: str, css: str, items: list[tuple[str, list[Any], str]]):
        """
        items: (label, values, fmt)
        fmt: money | int | num | pct | raw
        """
        nonlocal rows
        first = True
        for label, values, fmt in items:
            rows.append({
                "block": block,
                "block_css": css,
                "block_first": first,
                "block_span": len(items),  # для rowspan (если вдруг понадобится в другом шаблоне)
                "label": label,
                "fmt": fmt,
                "values": values,
            })
            first = False

    # TOP
    add_block("ТОП", "blk-top", [
        ("Расход", _values_by_day(dates, top_by_day, "spend_rub"), "money"),
        ("Заказы, руб", _values_by_day(dates, top_by_day, "orders_amount_rub"), "money"),
        ("Заказы, шт", _values_by_day(dates, top_by_day, "orders_count"), "int"),
        ("ДРР", _values_by_day(dates, top_by_day, "drr"), "pct"),
        ("CTR", _values_by_day(dates, top_by_day, "ctr"), "pct"),
        ("Показы", _values_by_day(dates, top_by_day, "impressions"), "int"),
        ("Клики", _values_by_day(dates, top_by_day, "clicks"), "int"),
        ("Корзин", _values_by_day(dates, top_by_day, "carts"), "int"),
        ("ставка кон.", _values_by_day(dates, top_by_day, "bid_competitor"), "money"),
        ("ставка наша", _values_by_day(dates, top_by_day, "bid_ours"), "money"),
        ("траты на заказ", _values_by_day(dates, top_by_day, "spend_per_order"), "money"),
        ("Конверсия рекламная", _values_by_day(dates, top_by_day, "ad_conversion"), "pct"),
    ])

    # TRAF
    add_block("ТРАФ", "blk-traf", [
        ("Расход", _values_by_day(dates, traf_by_day, "spend_rub"), "money"),
        ("Заказы, руб", _values_by_day(dates, traf_by_day, "orders_amount_rub"), "money"),
        ("Заказы, шт", _values_by_day(dates, traf_by_day, "orders_count"), "int"),
        ("ДРР", _values_by_day(dates, traf_by_day, "drr"), "pct"),
        ("CTR", _values_by_day(dates, traf_by_day, "ctr"), "pct"),
        ("Показы", _values_by_day(dates, traf_by_day, "impressions"), "int"),
        ("Клики", _values_by_day(dates, traf_by_day, "clicks"), "int"),
        ("Корзин", _values_by_day(dates, traf_by_day, "carts"), "int"),
        ("ставка кон.", _values_by_day(dates, traf_by_day, "bid_competitor"), "money"),
        ("ставка наша", _values_by_day(dates, traf_by_day, "bid_ours"), "money"),
        ("траты на заказ", _values_by_day(dates, traf_by_day, "spend_per_order"), "money"),
        ("Конверсия рекламная", _values_by_day(dates, traf_by_day, "ad_conversion"), "pct"),
    ])

    # TOTAL
    add_block("ИТОГО", "blk-total", [
        ("Расход", _values_by_day(dates, total_by_day, "spend_rub"), "money"),
        ("Все заказы, руб", _values_by_day(dates, total_by_day, "all_orders_amount_rub"), "money"),
        ("Все заказы, шт", _values_by_day(dates, total_by_day, "all_orders_count"), "int"),
        ("Рекламные заказы, руб", _values_by_day(dates, total_by_day, "ad_orders_amount_rub"), "money"),
        ("Рекламные заказы, шт", _values_by_day(dates, total_by_day, "ad_orders_count"), "int"),
        ("ДРР", _values_by_day(dates, total_by_day, "drr"), "pct"),
        ("ДРР общий", _values_by_day(dates, total_by_day, "drr_total"), "pct"),
        ("CTR", _values_by_day(dates, total_by_day, "ctr"), "pct"),
        ("Показы", _values_by_day(dates, total_by_day, "impressions"), "int"),
        ("Клики", _values_by_day(dates, total_by_day, "clicks"), "int"),
        ("Корзин", _values_by_day(dates, total_by_day, "carts"), "int"),
        ("Конверсия рекламная", _values_by_day(dates, total_by_day, "ad_conversion"), "pct"),
    ])

    # CONV
    add_block("КОНВЕРСИИ", "blk-conv", [
        ("из показа в заказ", _values_by_day(dates, conv_by_day, "impression_to_order"), "pct"),
        ("из поиск и кат. в корзину", _values_by_day(dates, conv_by_day, "search_cat_to_cart"), "pct"),
        ("из поиск и кат. в карточку", _values_by_day(dates, conv_by_day, "search_cat_to_card"), "pct"),
        ("из карточки в корзину", _values_by_day(dates, conv_by_day, "card_to_cart"), "pct"),
        ("в корзину общая", _values_by_day(dates, conv_by_day, "cart_total"), "pct"),
        ("из корзины в заказ", _values_by_day(dates, conv_by_day, "cart_to_order"), "pct"),
        ("из заказа в выкуп", _values_by_day(dates, conv_by_day, "order_to_purchase"), "pct"),
    ])

    # PRICES
    add_block("ЦЕНЫ", "blk-price", [
        ("Наша цена", _values_by_day(dates, price_by_day, "our_price_rub"), "money"),
        ("Цена покупателя", _values_by_day(dates, price_by_day, "buyer_price_rub"), "money"),
        ("Цена по карте Ozon", _values_by_day(dates, price_by_day, "ozon_card_price_rub"), "money"),
        ("Соинвест, %", _values_by_day(dates, price_by_day, "spp_percent"), "pct"),
    ])

    # QUERIES
    if queries:
        q_items: list[tuple[str, list[Any], str]] = []
        for q in queries:
            vals: list[str | None] = []
            for d in dates:
                row = qdaily_map.get((q.id, _as_key(d)))
                if not row:
                    vals.append(None)
                    continue
                vals.append(_fmt_query_pos_cpc(row.position, row.cpc_rub))
            q_items.append((q.query_text, vals, "raw"))

        add_block("ЗАПРОСЫ (МЕСТА / CPC)", "blk-queries", q_items)

    payload = {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "dates": [{"iso": d.isoformat(), "label": d.strftime("%d.%m.%Y")} for d in dates],
        "rows": rows,
    }

    # ✅ критично для Jinja |tojson (Decimal -> float)
    return _json_safe(payload)
