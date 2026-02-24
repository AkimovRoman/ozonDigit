from __future__ import annotations

from typing import Any, Optional
from datetime import date, datetime, timedelta
import httpx

BASE_URL = "https://api-seller.ozon.ru"


class OzonApiError(Exception):
    pass


async def _post(client_id: str, api_key: str, path: str, payload: dict) -> dict:
    headers = {
        "Client-Id": str(client_id),
        "Api-Key": str(api_key),
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(base_url=BASE_URL, timeout=30.0) as client:
        r = await client.post(path, headers=headers, json=payload)

    if r.status_code >= 400:
        raise OzonApiError(f"Ozon API error {r.status_code}: {r.text}")

    return r.json()


async def product_list(client_id: str, api_key: str, limit: int = 1000) -> dict[str, Any]:
    """
    Возвращаем СЫРОЙ ответ Ozon:
    POST /v3/product/list
    с пагинацией limit + last_id
    """
    items: list[dict[str, Any]] = []
    last_id = ""

    while True:
        payload = {
            "filter": {"visibility": "ALL"},
            "last_id": last_id,
            "limit": limit,
        }

        data = await _post(client_id, api_key, "/v3/product/list", payload)
        result = data.get("result") or {}

        part = result.get("items") or []
        items.extend(part)

        new_last_id = result.get("last_id") or ""
        if not new_last_id:
            break
        if new_last_id == last_id:
            break

        last_id = new_last_id

        if len(part) < limit:
            break

    return {"items": items, "total": len(items)}


async def product_info_list_v3(client_id: str, api_key: str, product_ids: list[int]) -> dict[str, Any]:
    """
    POST /v3/product/info/list
    Получаем name и sku (и много чего ещё) по product_id.
    В одном запросе можно до 1000 суммарно (offer_id/product_id/sku).
    """
    # Ozon в схеме пишет Array of strings<int64>, безопаснее отправлять как строки
    payload = {"product_id": [str(pid) for pid in product_ids]}
    data = await _post(client_id, api_key, "/v3/product/info/list", payload)

    # В этом методе обычно корень ответа: {"items": [...]}
    # (без "result"), поэтому берём items сверху.
    items = data.get("items") or []
    return {"items": items, "total": len(items)}


def _date_to_ozon_str(d: date | str) -> str:
    if isinstance(d, str):
        # ожидаем YYYY-MM-DD
        return d
    return d.isoformat()


async def finance_transaction_list_v3(
    client_id: str,
    api_key: str,
    date_from: date | str,
    date_to: date | str,
    page: int = 1,
    page_size: int = 1000,
    transaction_type: str = "all",
    operation_type: Optional[list[str]] = None,
    posting_number: Optional[str] = None,
) -> dict[str, Any]:
    """
    POST /v3/finance/transaction/list
    Возвращает страницу транзакций.

    Фильтр по датам: filter.date.from / filter.date.to
    Максимальный период запроса — 30 дней (1 месяц). :contentReference[oaicite:1]{index=1}
    """
    if page_size > 1000:
        raise ValueError("page_size must be <= 1000")

    flt: dict[str, Any] = {
        "date": {"from": _date_to_ozon_str(date_from), "to": _date_to_ozon_str(date_to)},
        "transaction_type": transaction_type,
    }

    # опционально
    if operation_type is not None:
        flt["operation_type"] = operation_type
    if posting_number:
        flt["posting_number"] = posting_number

    payload = {
        "filter": flt,
        "page": int(page),
        "page_size": int(page_size),
    }

    data = await _post(client_id, api_key, "/v3/finance/transaction/list", payload)
    # в ответе обычно data["result"]["operations"], page_count, row_count
    return data


async def finance_transaction_list_v3_all_pages(
    client_id: str,
    api_key: str,
    date_from: date | str,
    date_to: date | str,
    page_size: int = 1000,
    transaction_type: str = "all",
    operation_type: Optional[list[str]] = None,
    posting_number: Optional[str] = None,
) -> dict[str, Any]:
    """
    Забирает ВСЕ страницы за период.
    """
    all_ops: list[dict[str, Any]] = []
    page = 1

    while True:
        data = await finance_transaction_list_v3(
            client_id=client_id,
            api_key=api_key,
            date_from=date_from,
            date_to=date_to,
            page=page,
            page_size=page_size,
            transaction_type=transaction_type,
            operation_type=operation_type,
            posting_number=posting_number,
        )

        result = data.get("result") or {}
        ops = result.get("operations") or []
        all_ops.extend(ops)

        page_count = int(result.get("page_count") or 0)
        if page_count == 0:
            break

        page += 1
        if page > page_count:
            break

    return {"items": all_ops, "total": len(all_ops), "date_from": str(date_from), "date_to": str(date_to)}