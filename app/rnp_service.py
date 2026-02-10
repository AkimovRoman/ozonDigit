from __future__ import annotations

from dataclasses import dataclass
from datetime import date
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


def _values_by_day(dates: list[date], by_day: dict[str, Any], field: str) -> list[Any]:
    out: list[Any] = []
    for d in dates:
        row = by_day.get(_as_key(d))
        out.append(_get_attr(row, field) if row is not None else None)
    return out


@dataclass
class MetricRow:
    label: str
    values: list[Any]
    fmt: str  # money | int | num | pct | raw


@dataclass
class MetricTable:
    title: str
    css_class: str
    rows: list[MetricRow]


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


async def build_rnp_view(
    session: AsyncSession,
    ozon_account_id: int,
    product_id: int,
) -> dict[str, Any] | None:
    """
    Без фильтрации по датам: выводим всё что есть в БД по товару.
    Даты = union всех таблиц.
    """

    # ---------- daily blocks (ALL)
    top_by_day = await _fetch_all_by_day(session, AdsTopDaily, ozon_account_id, product_id)
    traf_by_day = await _fetch_all_by_day(session, AdsStencilDaily, ozon_account_id, product_id)
    total_by_day = await _fetch_all_by_day(session, AdsTotalDaily, ozon_account_id, product_id)
    conv_by_day = await _fetch_all_by_day(session, ConversionsDaily, ozon_account_id, product_id)
    price_by_day = await _fetch_all_by_day(session, PricingDaily, ozon_account_id, product_id)

    # ---------- tracked queries
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

    # ---------- union dates from ALL sources
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

    def mk_table(title: str, css_class: str, rows: list[MetricRow]) -> MetricTable:
        return MetricTable(title=title, css_class=css_class, rows=rows)

    tables: list[MetricTable] = []

    # TOP
    tables.append(mk_table(
        title="ТОП",
        css_class="tbl-top",
        rows=[
            MetricRow("Расход", _values_by_day(dates, top_by_day, "spend_rub"), "money"),
            MetricRow("Заказы, руб", _values_by_day(dates, top_by_day, "orders_amount_rub"), "money"),
            MetricRow("Заказы, шт", _values_by_day(dates, top_by_day, "orders_count"), "int"),
            MetricRow("ДРР", _values_by_day(dates, top_by_day, "drr"), "pct"),
            MetricRow("CTR", _values_by_day(dates, top_by_day, "ctr"), "pct"),
            MetricRow("Показы", _values_by_day(dates, top_by_day, "impressions"), "int"),
            MetricRow("Клики", _values_by_day(dates, top_by_day, "clicks"), "int"),
            MetricRow("Корзин", _values_by_day(dates, top_by_day, "carts"), "int"),
            MetricRow("ставка кон.", _values_by_day(dates, top_by_day, "bid_competitor"), "money"),
            MetricRow("ставка наша", _values_by_day(dates, top_by_day, "bid_ours"), "money"),
            MetricRow("траты на заказ", _values_by_day(dates, top_by_day, "spend_per_order"), "money"),
            MetricRow("Конверсия рекламная", _values_by_day(dates, top_by_day, "ad_conversion"), "pct"),
        ],
    ))

    # TRAF
    tables.append(mk_table(
        title="ТРАФ",
        css_class="tbl-traf",
        rows=[
            MetricRow("Расход", _values_by_day(dates, traf_by_day, "spend_rub"), "money"),
            MetricRow("Заказы, руб", _values_by_day(dates, traf_by_day, "orders_amount_rub"), "money"),
            MetricRow("Заказы, шт", _values_by_day(dates, traf_by_day, "orders_count"), "int"),
            MetricRow("ДРР", _values_by_day(dates, traf_by_day, "drr"), "pct"),
            MetricRow("CTR", _values_by_day(dates, traf_by_day, "ctr"), "pct"),
            MetricRow("Показы", _values_by_day(dates, traf_by_day, "impressions"), "int"),
            MetricRow("Клики", _values_by_day(dates, traf_by_day, "clicks"), "int"),
            MetricRow("Корзин", _values_by_day(dates, traf_by_day, "carts"), "int"),
            MetricRow("ставка кон.", _values_by_day(dates, traf_by_day, "bid_competitor"), "money"),
            MetricRow("ставка наша", _values_by_day(dates, traf_by_day, "bid_ours"), "money"),
            MetricRow("траты на заказ", _values_by_day(dates, traf_by_day, "spend_per_order"), "money"),
            MetricRow("Конверсия рекламная", _values_by_day(dates, traf_by_day, "ad_conversion"), "pct"),
        ],
    ))

    # TOTAL
    tables.append(mk_table(
        title="ИТОГО",
        css_class="tbl-total",
        rows=[
            MetricRow("Расход", _values_by_day(dates, total_by_day, "spend_rub"), "money"),
            MetricRow("Все заказы, руб", _values_by_day(dates, total_by_day, "all_orders_amount_rub"), "money"),
            MetricRow("Все заказы, шт", _values_by_day(dates, total_by_day, "all_orders_count"), "int"),
            MetricRow("Рекламные заказы, руб", _values_by_day(dates, total_by_day, "ad_orders_amount_rub"), "money"),
            MetricRow("Рекламные заказы, шт", _values_by_day(dates, total_by_day, "ad_orders_count"), "int"),
            MetricRow("ДРР", _values_by_day(dates, total_by_day, "drr"), "pct"),
            MetricRow("ДРР общий", _values_by_day(dates, total_by_day, "drr_total"), "pct"),
            MetricRow("CTR", _values_by_day(dates, total_by_day, "ctr"), "pct"),
            MetricRow("Показы", _values_by_day(dates, total_by_day, "impressions"), "int"),
            MetricRow("Клики", _values_by_day(dates, total_by_day, "clicks"), "int"),
            MetricRow("Корзин", _values_by_day(dates, total_by_day, "carts"), "int"),
            MetricRow("Конверсия рекламная", _values_by_day(dates, total_by_day, "ad_conversion"), "pct"),
        ],
    ))

    # CONVERSIONS
    tables.append(mk_table(
        title="КОНВЕРСИИ",
        css_class="tbl-conv",
        rows=[
            MetricRow("из показа в заказ", _values_by_day(dates, conv_by_day, "impression_to_order"), "pct"),
            MetricRow("из поиск и кат. в корзину", _values_by_day(dates, conv_by_day, "search_cat_to_cart"), "pct"),
            MetricRow("из поиск и кат. в карточку", _values_by_day(dates, conv_by_day, "search_cat_to_card"), "pct"),
            MetricRow("из карточки в корзину", _values_by_day(dates, conv_by_day, "card_to_cart"), "pct"),
            MetricRow("в корзину общая", _values_by_day(dates, conv_by_day, "cart_total"), "pct"),
            MetricRow("из корзины в заказ", _values_by_day(dates, conv_by_day, "cart_to_order"), "pct"),
            MetricRow("из заказа в выкуп", _values_by_day(dates, conv_by_day, "order_to_purchase"), "pct"),
        ],
    ))

    # PRICES
    tables.append(mk_table(
        title="ЦЕНЫ",
        css_class="tbl-price",
        rows=[
            MetricRow("Наша цена", _values_by_day(dates, price_by_day, "our_price_rub"), "money"),
            MetricRow("Цена покупателя", _values_by_day(dates, price_by_day, "buyer_price_rub"), "money"),
            MetricRow("Цена по карте Ozon", _values_by_day(dates, price_by_day, "ozon_card_price_rub"), "money"),
            MetricRow("Соинвест, %", _values_by_day(dates, price_by_day, "spp_percent"), "pct"),
        ],
    ))

    # QUERIES
    query_rows: list[MetricRow] = []
    for q in queries:
        vals: list[str | None] = []
        for d in dates:
            row = qdaily_map.get((q.id, _as_key(d)))
            if not row:
                vals.append(None)
                continue
            pos = row.position
            cpc = row.cpc_rub
            left = "-" if pos is None else str(pos)
            right = "-"
            if cpc is not None:
                right = str(Decimal(cpc).quantize(Decimal("1.00"))).replace(".", ",")
            vals.append(f"{left} / {right}")
        query_rows.append(MetricRow(q.query_text, vals, "raw"))

    if query_rows:
        tables.append(mk_table(
            title="ЗАПРОСЫ МЕСТА / CPC",
            css_class="tbl-queries",
            rows=query_rows,
        ))

    return {
        "date_from": date_from.isoformat(),
        "date_to": date_to.isoformat(),
        "dates": [{"iso": d.isoformat(), "label": d.strftime("%d.%m.%Y")} for d in dates],
        "tables": [
            {
                "title": t.title,
                "css_class": t.css_class,
                "rows": [{"label": r.label, "fmt": r.fmt, "values": r.values} for r in t.rows],
            }
            for t in tables
        ],
    }
