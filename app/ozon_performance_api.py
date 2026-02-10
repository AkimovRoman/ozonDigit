from __future__ import annotations

from typing import Any, Optional
import time
import httpx

from fastapi import Request

from app.db import SessionLocal
from app.models import OzonAccount

# Performance API
PERF_BASE_URL = "https://api-performance.ozon.ru"
PERF_TOKEN_PATH = "/api/client/token"


class OzonPerformanceApiError(Exception):
    pass


class OzonPerformanceClient:
    def __init__(self, client_id: str, client_secret: str, timeout: float = 30.0):
        self.client_id = client_id
        self.client_secret = client_secret
        self.timeout = timeout

        self._access_token: Optional[str] = None
        self._expires_at: float = 0.0

    # =========================
    # Internal helpers
    # =========================
    @staticmethod
    def _ensure_json_response(r: httpx.Response, path: str) -> None:
        if 300 <= r.status_code < 400:
            raise OzonPerformanceApiError(
                f"HTTP {r.status_code} {path}: redirect. Body={r.text[:300]}"
            )

        ctype = (r.headers.get("content-type") or "").lower()
        if "application/json" not in ctype:
            raise OzonPerformanceApiError(
                f"Non-JSON response from {path}. "
                f"Status={r.status_code}, Content-Type={ctype}, Body={r.text[:300]}"
            )

    async def _post_json(self, path: str, payload: dict) -> Any:
        async with httpx.AsyncClient(
            base_url=PERF_BASE_URL,
            timeout=self.timeout,
            follow_redirects=True,
            headers={"Accept": "application/json"},
        ) as client:
            r = await client.post(path, json=payload)

        if r.status_code >= 400:
            raise OzonPerformanceApiError(f"HTTP {r.status_code} {path}: {r.text}")

        self._ensure_json_response(r, path)
        return r.json()

    async def _get_json(
        self,
        path: str,
        headers: dict[str, str] | None = None,
        params: dict | None = None,
    ) -> Any:
        base_headers = {"Accept": "application/json"}
        if headers:
            base_headers.update(headers)

        async with httpx.AsyncClient(
            base_url=PERF_BASE_URL,
            timeout=self.timeout,
            follow_redirects=True,
        ) as client:
            r = await client.get(path, headers=base_headers, params=params)

        if r.status_code >= 400:
            raise OzonPerformanceApiError(f"HTTP {r.status_code} {path}: {r.text}")

        self._ensure_json_response(r, path)
        return r.json()

    # =========================
    # Auth
    # =========================
    async def get_access_token(self, force_refresh: bool = False) -> str:
        now = time.time()
        if (not force_refresh) and self._access_token and now < (self._expires_at - 30):
            return self._access_token

        data = await self._post_json(
            PERF_TOKEN_PATH,
            {
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
        )

        token = (
            data.get("access_token")
            or data.get("token")
            or (data.get("result") or {}).get("access_token")
        )
        if not token:
            raise OzonPerformanceApiError(f"No access_token in response: {data}")

        expires_in = (
            data.get("expires_in")
            or (data.get("result") or {}).get("expires_in")
            or 3600
        )

        self._access_token = str(token)
        self._expires_at = now + int(expires_in)
        return self._access_token

    # =========================
    # API methods
    # =========================
    async def campaign_list(self) -> Any:
        token = await self.get_access_token()
        return await self._get_json(
            "/api/client/campaign",
            headers={"Authorization": f"Bearer {token}"},
        )

    async def statistics_campaign_product(
        self,
        campaign_ids: list[str] | None,
        date_from: str,
        date_to: str,
    ) -> Any:
        token = await self.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        params: dict[str, Any] = {
            "dateFrom": date_from,
            "dateTo": date_to,
        }
        if campaign_ids:
            params["campaignIds"] = campaign_ids

        return await self._get_json(
            "/api/client/statistics/campaign/product/json",
            headers=headers,
            params=params,
        )

    async def campaign_products_v2(
        self,
        campaign_id: str,
        page: int = 1,
        page_size: int = 200,
    ) -> Any:
        token = await self.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        params = {"page": page, "pageSize": page_size}

        return await self._get_json(
            f"/api/client/campaign/{campaign_id}/v2/products",
            headers=headers,
            params=params,
        )

    async def campaign_products_v2_all(
        self,
        campaign_id: str,
        page_size: int = 200,
        max_pages: int = 2000,
    ) -> list[dict[str, Any]]:
        all_products: list[dict[str, Any]] = []
        page = 1

        while page <= max_pages:
            data = await self.campaign_products_v2(
                campaign_id=campaign_id, page=page, page_size=page_size
            )
            products = data.get("products") or []
            if not products:
                break

            all_products.extend(products)

            if len(products) < page_size:
                break

            page += 1

        return all_products

    async def campaign_products_bids_competitive(
        self,
        campaign_id: str,
        skus: list[str],
    ) -> Any:
        if not skus:
            return {"campaignId": str(campaign_id), "bids": []}

        if len(skus) > 200:
            raise OzonPerformanceApiError(
                "Too many skus for competitive bids: максимум 200 за запрос"
            )

        token = await self.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}

        params = {"skus": skus}

        return await self._get_json(
            f"/api/client/campaign/{campaign_id}/products/bids/competitive",
            headers=headers,
            params=params,
        )

    async def campaign_products_bids_competitive_all(
        self,
        campaign_id: str,
        skus: list[str],
        chunk_size: int = 200,
    ) -> dict[str, Any]:
        bids_all: list[Any] = []
        campaign_id_str = str(campaign_id)

        for i in range(0, len(skus), chunk_size):
            chunk = skus[i : i + chunk_size]
            data = await self.campaign_products_bids_competitive(campaign_id, chunk)
            bids = data.get("bids") or []
            bids_all.extend(bids)

        return {"campaignId": campaign_id_str, "bids": bids_all}


# ==========================================================
# Factory: берем креды из БД по текущему выбранному кабинету
# ==========================================================

async def get_perf_client(request: Request) -> OzonPerformanceClient:
    """
    Берем active_ozon_account_id из session, достаем из БД perf_* поля
    и возвращаем клиент Performance API.
    """
    ozon_account_id = request.session.get("active_ozon_account_id")
    if not ozon_account_id:
        raise OzonPerformanceApiError("Не выбран Ozon-кабинет (active_ozon_account_id отсутствует)")

    async with SessionLocal() as session:
        acc = await session.get(OzonAccount, int(ozon_account_id))

    if not acc:
        raise OzonPerformanceApiError(f"Ozon-кабинет id={ozon_account_id} не найден в БД")

    perf_client_id = getattr(acc, "perf_client_id", None)
    perf_client_secret = getattr(acc, "perf_client_secret", None)

    if not perf_client_id or not perf_client_secret:
        raise OzonPerformanceApiError(
            "Для выбранного кабинета не заданы perf_client_id / perf_client_secret"
        )

    return OzonPerformanceClient(str(perf_client_id), str(perf_client_secret))
