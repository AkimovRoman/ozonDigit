from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.db import SessionLocal
from app.models import Product, OzonAccount
from app.ozon_api import product_list, product_info_list_v3, OzonApiError

router = APIRouter()


def get_user_id(request: Request) -> int | None:
    return request.session.get("user_id")


def login_required(request: Request):
    if not get_user_id(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


def get_active_ozon_account_id(request: Request) -> int | None:
    return request.session.get("active_ozon_account_id")


@router.get("/products")
async def products_page(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    async with SessionLocal() as session:
        products = (
            await session.execute(
                select(Product)
                .where(Product.ozon_account_id == active_ozon_account_id)
                .order_by(Product.product_id.asc())
            )
        ).scalars().all()

    return request.app.state.templates.TemplateResponse(
        "products.html",
        {
            "request": request,
            "active_page": "products",
            "products": products,
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,
        },
    )


@router.post("/products/{product_id}/delete")
async def delete_product(request: Request, product_id: int):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    async with SessionLocal() as session:
        p = await session.scalar(
            select(Product).where(
                Product.product_id == product_id,
                Product.ozon_account_id == active_ozon_account_id,
            )
        )
        if p:
            await session.delete(p)
            await session.commit()

    return RedirectResponse(url="/products?ok=deleted", status_code=303)


@router.get("/products/import")
async def products_import_page(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    async with SessionLocal() as session:
        acc = await session.scalar(select(OzonAccount).where(OzonAccount.id == active_ozon_account_id))
        if not acc:
            return RedirectResponse(url="/products?err=ozon_api", status_code=303)

        client_id = acc.client_id
        api_key = acc.api_key

        existing_ids = set(
            (await session.execute(
                select(Product.product_id).where(Product.ozon_account_id == active_ozon_account_id)
            )).scalars().all()
        )

    try:
        data = await product_list(client_id=client_id, api_key=api_key, limit=1000)
        items = data.get("items") or []
    except OzonApiError:
        return RedirectResponse(url="/products?err=ozon_api", status_code=303)

    candidates = []
    for it in items:
        pid = it.get("product_id")
        if pid is None:
            continue
        pid_int = int(pid)
        if pid_int in existing_ids:
            continue
        if it.get("archived") is True:
            continue
        candidates.append(it)

    return request.app.state.templates.TemplateResponse(
        "products_import.html",
        {
            "request": request,
            "active_page": "products",
            "items": candidates,
            "total": len(candidates),
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,
        },
    )


@router.post("/products/import/step2")
async def products_import_step2(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    form = await request.form()
    selected_raw = form.getlist("product_id")
    selected_ids = [int(x) for x in selected_raw if str(x).strip().isdigit()]

    if not selected_ids:
        return RedirectResponse(url="/products/import?err=select_none", status_code=303)

    async with SessionLocal() as session:
        acc = await session.scalar(select(OzonAccount).where(OzonAccount.id == active_ozon_account_id))
        if not acc:
            return RedirectResponse(url="/products/import?err=ozon_api", status_code=303)

        client_id = acc.client_id
        api_key = acc.api_key

    try:
        data = await product_list(client_id=client_id, api_key=api_key, limit=1000)
        items = data.get("items") or []
    except OzonApiError:
        return RedirectResponse(url="/products/import?err=ozon_api", status_code=303)

    selected_map = {
        int(it.get("product_id")): it
        for it in items
        if it.get("product_id") is not None
    }
    selected_items = [selected_map[i] for i in selected_ids if i in selected_map]

    return request.app.state.templates.TemplateResponse(
        "products_import_costs.html",
        {
            "request": request,
            "active_page": "products",
            "items": selected_items,
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,
        },
    )


@router.post("/products/import/commit")
async def products_import_commit(request: Request):
    redirect = login_required(request)
    if redirect:
        return redirect

    active_ozon_account_id = get_active_ozon_account_id(request)
    if not active_ozon_account_id:
        return RedirectResponse(url="/settings", status_code=303)

    form = await request.form()
    selected_raw = form.getlist("product_id")
    selected_ids = [int(x) for x in selected_raw if str(x).strip().isdigit()]

    if not selected_ids:
        return RedirectResponse(url="/products?err=select_none", status_code=303)

    async with SessionLocal() as session:
        acc = await session.scalar(select(OzonAccount).where(OzonAccount.id == active_ozon_account_id))
        if not acc:
            return RedirectResponse(url="/products?err=ozon_api", status_code=303)

        client_id = acc.client_id
        api_key = acc.api_key

        # 1) list — чтобы иметь offer_id на всякий случай (и фильтры/архивность)
        try:
            data = await product_list(client_id=client_id, api_key=api_key, limit=1000)
            items = data.get("items") or []
        except OzonApiError:
            return RedirectResponse(url="/products?err=ozon_api", status_code=303)

        by_id = {
            int(it.get("product_id")): it
            for it in items
            if it.get("product_id") is not None
        }

        # 2) info/list — вот тут берём name и sku (актуальный метод)
        try:
            info = await product_info_list_v3(client_id=client_id, api_key=api_key, product_ids=selected_ids)
            info_items = info.get("items") or []
        except OzonApiError:
            return RedirectResponse(url="/products?err=ozon_api", status_code=303)

        info_by_pid = {}
        for it in info_items:
            # в ответе /v3/product/info/list id = product_id
            pid = it.get("id")
            if pid is None:
                continue
            info_by_pid[int(pid)] = it

        created = 0

        for pid in selected_ids:
            base_it = by_id.get(pid)
            info_it = info_by_pid.get(pid)

            # если вдруг товара нет в list — пропустим
            if not base_it:
                continue

            cost_key = f"cost_{pid}"
            raw_cost = (form.get(cost_key) or "").strip()
            if raw_cost == "":
                raw_cost = "0"

            if not raw_cost.isdigit():
                return RedirectResponse(url="/products/import?err=bad_cost", status_code=303)

            cost_price = int(raw_cost)
            if cost_price < 0:
                return RedirectResponse(url="/products/import?err=cost_negative", status_code=303)

            exists = await session.scalar(
                select(Product).where(
                    Product.product_id == pid,
                    Product.ozon_account_id == active_ozon_account_id,
                )
            )
            if exists:
                continue

            p = Product(
                product_id=pid,
                ozon_account_id=active_ozon_account_id,

                # ✅ берём из info/list
                sku=info_it.get("sku") if info_it else None,
                name=info_it.get("name") if info_it else None,

                # ✅ offer_id берём из list
                offer_id=base_it.get("offer_id"),

                cost_price_rub=cost_price,
                is_active=True,
            )
            session.add(p)
            created += 1

        await session.commit()

    return RedirectResponse(url=f"/products?ok=imported&count={created}", status_code=303)
