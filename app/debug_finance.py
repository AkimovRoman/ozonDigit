from datetime import date
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

from app.db import get_db
from app.models import OzonStore  # подставь своё имя модели
from app.ozon_api import finance_transaction_list_v3_all_pages, OzonApiError

router = APIRouter(prefix="/debug", tags=["debug"])


@router.get("/finance/transactions")
async def debug_finance_transactions(
    store_id: int = Query(...),
    day: date = Query(..., description="YYYY-MM-DD"),
    page_size: int = Query(1000, le=1000),
    db: Session = Depends(get_db),
):
    store = db.query(OzonStore).filter(OzonStore.id == store_id).first()
    if not store:
        raise HTTPException(status_code=404, detail="Store not found")

    # подставь реальные поля из твоей модели/таблицы
    client_id = store.client_id
    api_key = store.api_key

    try:
        data = await finance_transaction_list_v3_all_pages(
            client_id=client_id,
            api_key=api_key,
            date_from=day,
            date_to=day,
            page_size=page_size,
        )
        return data
    except OzonApiError as e:
        raise HTTPException(status_code=502, detail=str(e))