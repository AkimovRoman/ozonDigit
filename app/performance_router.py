from __future__ import annotations

from datetime import date, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from app.ozon_performance_api import (
    get_perf_client,
    OzonPerformanceApiError,
)

router = APIRouter()


@router.get("/api/test/performance/campaigns")
async def test_performance_campaigns(request: Request):
    """
    Проверка: список кампаний (по активному кабинету)
    """
    try:
        api = await get_perf_client(request)
        data = await api.campaign_list()
        return JSONResponse(content=data)
    except OzonPerformanceApiError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {e}"})


@router.get("/api/test/performance/campaigns/stats-yesterday")
async def test_performance_campaigns_stats_yesterday(request: Request):
    """
    Проверка: статистика по ВСЕМ кампаниям за ВЧЕРА (по активному кабинету)
    """
    try:
        api = await get_perf_client(request)

        # 1) кампании
        campaigns_data = await api.campaign_list()
        campaigns = campaigns_data.get("list", [])
        campaign_ids = [c["id"] for c in campaigns if "id" in c]

        # 2) вчера
        yesterday = date.today() - timedelta(days=1)
        day = yesterday.strftime("%Y-%m-%d")

        # 3) статистика
        stats = await api.statistics_campaign_product(
            campaign_ids=campaign_ids,
            date_from=day,
            date_to=day,
        )

        return JSONResponse(
            content={
                "date": day,
                "campaigns_count": len(campaign_ids),
                "stats": stats,
            }
        )

    except OzonPerformanceApiError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {e}"})


@router.get("/api/test/performance/campaign/{campaign_id}/products")
async def test_campaign_products(request: Request, campaign_id: str):
    """
    Тест 1: товары кампании (по активному кабинету)
    """
    try:
        api = await get_perf_client(request)
        products = await api.campaign_products_v2_all(campaign_id=campaign_id, page_size=200)

        return JSONResponse(
            content={
                "campaignId": str(campaign_id),
                "products_count": len(products),
                "products": products,
            }
        )

    except OzonPerformanceApiError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {e}"})


@router.get("/api/test/performance/campaign/{campaign_id}/products/competitive-bids")
async def test_campaign_competitive_bids(request: Request, campaign_id: str):
    """
    Тест 2:
    1) товары кампании
    2) sku
    3) competitive bids чанками
    (по активному кабинету)
    """
    try:
        api = await get_perf_client(request)

        products = await api.campaign_products_v2_all(campaign_id=campaign_id, page_size=200)

        skus: list[str] = []
        for p in products:
            sku = p.get("sku")
            if sku is None:
                continue
            skus.append(str(sku))

        competitive = await api.campaign_products_bids_competitive_all(
            campaign_id=campaign_id,
            skus=skus,
            chunk_size=200,
        )

        return JSONResponse(
            content={
                "campaignId": str(campaign_id),
                "products_count": len(products),
                "skus_count": len(skus),
                "products": products,
                "competitive_bids": competitive,
            }
        )

    except OzonPerformanceApiError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": f"Unexpected error: {e}"})
