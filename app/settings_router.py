from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from sqlalchemy import select, delete, func

from app.db import SessionLocal
from app.models import User, OzonAccount, UserOzonAccount
from app.security import verify_password, hash_password

router = APIRouter()


def get_user_id(request: Request) -> int | None:
    return request.session.get("user_id")


def require_login(request: Request):
    if not get_user_id(request):
        return RedirectResponse(url="/login", status_code=303)
    return None


@router.get("/settings")
async def settings_page(request: Request):
    redirect = require_login(request)
    if redirect:
        return redirect

    user_id = get_user_id(request)

    # для шапки (dropdown кабинетов)
    active_account, header_accounts = await request.app.state.get_active_ozon_context(request)

    async with SessionLocal() as session:
        user = await session.scalar(select(User).where(User.id == user_id))

        rows = (await session.execute(
            select(OzonAccount.id, OzonAccount.client_id, OzonAccount.name, UserOzonAccount.role)
            .join(UserOzonAccount, UserOzonAccount.ozon_account_id == OzonAccount.id)
            .where(UserOzonAccount.user_id == user_id)
            .order_by(OzonAccount.id.desc())
        )).all()

    # список для страницы настроек (с role)
    ozon_accounts_page = [
        {"id": r.id, "client_id": r.client_id, "name": r.name, "role": r.role}
        for r in rows
    ]

    return request.app.state.templates.TemplateResponse(
        "settings.html",
        {
            "request": request,
            "active_page": "settings",
            "user": user,

            # 👇 для navbar (base.html)
            "active_ozon_account": active_account,
            "ozon_accounts": header_accounts,

            # 👇 для списка на самой странице settings
            "ozon_accounts_page": ozon_accounts_page,

            "error": None,
            "success": None,
        },
    )


@router.post("/settings/change-password")
async def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password: str = Form(...),
    new_password2: str = Form(...),
):
    redirect = require_login(request)
    if redirect:
        return redirect

    user_id = get_user_id(request)

    # bcrypt ограничение (чтобы не падало)
    if len(new_password.encode("utf-8")) > 72:
        return RedirectResponse(url="/settings?err=pw_too_long", status_code=303)

    if new_password != new_password2:
        return RedirectResponse(url="/settings?err=pw_mismatch", status_code=303)

    async with SessionLocal() as session:
        user = await session.scalar(select(User).where(User.id == user_id))
        if not user:
            return RedirectResponse(url="/login", status_code=303)

        if not verify_password(current_password, user.password_hash):
            return RedirectResponse(url="/settings?err=pw_current_wrong", status_code=303)

        user.password_hash = hash_password(new_password)
        await session.commit()

    return RedirectResponse(url="/settings?ok=pw_changed", status_code=303)


@router.post("/settings/ozon/add")
async def add_ozon_account(
    request: Request,
    client_id: str = Form(...),
    api_key: str = Form(...),
    name: str = Form(...),
    perf_client_id: str = Form(...),
    perf_client_secret: str = Form(...),
):
    redirect = require_login(request)
    if redirect:
        return redirect

    user_id = get_user_id(request)

    client_id_norm = client_id.strip()
    api_key_norm = api_key.strip()
    name_norm = name.strip()

    perf_client_id_norm = perf_client_id.strip()
    perf_client_secret_norm = perf_client_secret.strip()

    if not all([client_id_norm, api_key_norm, name_norm, perf_client_id_norm, perf_client_secret_norm]):
        return RedirectResponse(url="/settings?err=ozon_fields", status_code=303)

    async with SessionLocal() as session:
        acc = await session.scalar(
            select(OzonAccount).where(OzonAccount.client_id == client_id_norm)
        )

        # если кабинет существует — просто привязываем пользователя
        if acc:
            link = await session.scalar(
                select(UserOzonAccount).where(
                    UserOzonAccount.user_id == user_id,
                    UserOzonAccount.ozon_account_id == acc.id
                )
            )
            if link:
                return RedirectResponse(url="/settings?err=ozon_already_linked", status_code=303)

            session.add(UserOzonAccount(user_id=user_id, ozon_account_id=acc.id, role="member"))
            await session.commit()

            # ВАЖНО: здесь мы НЕ перезаписываем perf_* и api_key существующего кабинета
            # (чтобы "member" не мог менять секреты владельца).
            return RedirectResponse(url="/settings?ok=ozon_added", status_code=303)

        # если кабинета нет — создаём (сюда сохраняем perf_* если они введены)
        acc = OzonAccount(
            client_id=client_id_norm,
            api_key=api_key_norm,
            name=name_norm,
            perf_client_id=perf_client_id_norm,
            perf_client_secret=perf_client_secret_norm,
        )
        session.add(acc)
        await session.commit()
        await session.refresh(acc)

        session.add(UserOzonAccount(user_id=user_id, ozon_account_id=acc.id, role="owner"))
        await session.commit()

    return RedirectResponse(url="/settings?ok=ozon_added", status_code=303)


@router.post("/settings/ozon/{account_id}/unlink")
async def unlink_ozon_account(request: Request, account_id: int):
    redirect = require_login(request)
    if redirect:
        return redirect

    user_id = get_user_id(request)

    async with SessionLocal() as session:
        # удаляем ТОЛЬКО связь user <-> account
        await session.execute(
            delete(UserOzonAccount).where(
                UserOzonAccount.user_id == user_id,
                UserOzonAccount.ozon_account_id == account_id
            )
        )
        await session.commit()

    return RedirectResponse(url="/settings?ok=ozon_unlinked", status_code=303)
