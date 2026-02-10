from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.db import SessionLocal
from app.models import OzonAccount, TrackedCampaign
from app.ozon_performance_api import get_perf_client, OzonPerformanceApiError

router = APIRouter()


def get_user_id(request: Request) -> int | None:
    return request.session.get("user_id")


def login_required(request: Request):
    if not get_user_id(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


def get_active_ozon_account_id(request: Request) -> int | None:
    return request.session.get("active_ozon_account_id")


def _parse_ozon_dt(v: Any) -> datetime | None:
    """
    createdAt приходит как "2026-01-06T15:49:19.233802Z"
    datetime.fromisoformat не любит 'Z', заменяем на '+00:00'
    """
    if not v:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return datetime.fromisoformat(s)
    except Exception:
        return None


@router.get("/campaigns")
async def campaigns_page(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    async with SessionLocal() as session:
        campaigns = (
            await session.execute(
                select(TrackedCampaign)
                .where(TrackedCampaign.ozon_account_id == active_ozon_account_id)
                .order_by(TrackedCampaign.campaign_id.asc())
            )
        ).scalars().all()

    return request.app.state.templates.TemplateResponse(
        "campaigns.html",
        {
            "request": request,
            "active_page": "campaigns",
            "campaigns": campaigns,
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,
        },
    )


@router.post("/campaigns/{campaign_id}/delete")
async def delete_campaign(request: Request, campaign_id: str):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    async with SessionLocal() as session:
        c = await session.scalar(
            select(TrackedCampaign).where(
                TrackedCampaign.campaign_id == str(campaign_id),
                TrackedCampaign.ozon_account_id == active_ozon_account_id,
            )
        )
        if c:
            await session.delete(c)
            await session.commit()

    return RedirectResponse(url="/campaigns?ok=deleted", status_code=303)


@router.get("/campaigns/import")
async def campaigns_import_page(request: Request):
    """
    Страница выбора кампаний:
    - дергаем Performance API campaign_list()
    - показываем только те, которых еще нет в tracked_campaigns
    """
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    # какие кампании уже отслеживаем (глобально в БД они уникальны)
    async with SessionLocal() as session:
        existing_ids = set(
            (await session.execute(select(TrackedCampaign.campaign_id))).scalars().all()
        )

    # получаем список из Performance API
    try:
        api = await get_perf_client(request)
        data = await api.campaign_list()
        items = data.get("list") or []
        if not isinstance(items, list):
            items = []
    except OzonPerformanceApiError:
        return RedirectResponse(url="/campaigns?err=ozon_api", status_code=303)
    except Exception:
        return RedirectResponse(url="/campaigns?err=ozon_api", status_code=303)

    # кандидаты = те, которых нет в БД
    candidates: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = it.get("id")
        if cid is None:
            continue
        cid_s = str(cid)
        if cid_s in existing_ids:
            continue
        candidates.append(it)

    return request.app.state.templates.TemplateResponse(
        "campaigns_import.html",
        {
            "request": request,
            "active_page": "campaigns",
            "items": candidates,
            "total": len(candidates),
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,
        },
    )


@router.post("/campaigns/import/commit")
async def campaigns_import_commit(request: Request):
    """
    Сохраняем выбранные campaign_id в tracked_campaigns (для активного кабинета).
    Важно: campaign_id уникален глобально -> если уже есть в другом кабинете,
    БД не даст вставить (UNIQUE). Мы это аккуратно обработаем (просто пропустим).
    """
    redirect = login_required(request)
    if redirect:
        return redirect

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    form = await request.form()
    selected_raw = form.getlist("campaign_id")
    selected_ids = [str(x).strip() for x in selected_raw if str(x).strip()]

    if not selected_ids:
        return RedirectResponse(url="/campaigns/import?err=select_none", status_code=303)

    # ещё раз берём список кампаний из API, чтобы подтянуть title/createdAt по выбранным
    try:
        api = await get_perf_client(request)
        data = await api.campaign_list()
        items = data.get("list") or []
        if not isinstance(items, list):
            items = []
    except OzonPerformanceApiError:
        return RedirectResponse(url="/campaigns?err=ozon_api", status_code=303)
    except Exception:
        return RedirectResponse(url="/campaigns?err=ozon_api", status_code=303)

    by_id: dict[str, dict[str, Any]] = {}
    for it in items:
        if not isinstance(it, dict):
            continue
        cid = it.get("id")
        if cid is None:
            continue
        by_id[str(cid)] = it

    created = 0
    skipped_exists = 0

    async with SessionLocal() as session:
        # проверим кабинет существует
        acc = await session.scalar(select(OzonAccount).where(OzonAccount.id == active_ozon_account_id))
        if not acc:
            return RedirectResponse(url="/campaigns?err=ozon_api", status_code=303)

        for cid in selected_ids:
            it = by_id.get(cid)
            if not it:
                continue

            # если уже есть — пропускаем
            exists = await session.scalar(
                select(TrackedCampaign).where(TrackedCampaign.campaign_id == cid)
            )
            if exists:
                skipped_exists += 1
                continue

            c = TrackedCampaign(
                ozon_account_id=active_ozon_account_id,
                campaign_id=cid,
                title=it.get("title"),
                ozon_created_at=_parse_ozon_dt(it.get("createdAt")),
            )
            session.add(c)
            created += 1

        await session.commit()

    return RedirectResponse(url=f"/campaigns?ok=imported&count={created}&skipped={skipped_exists}", status_code=303)
