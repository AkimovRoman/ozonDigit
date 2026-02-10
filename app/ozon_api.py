from __future__ import annotations

from typing import Any
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
