from fastapi import APIRouter, Request, Form
from fastapi.responses import RedirectResponse
from sqlalchemy import select

from app.db import SessionLocal
from app.models import User
from app.security import hash_password, verify_password

router = APIRouter()


@router.get("/register")
async def register_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "register.html",
        {"request": request, "error": None}
    )


@router.post("/register")
async def register(
    request: Request,
    last_name: str = Form(...),
    first_name: str = Form(...),
    middle_name: str = Form(""),
    email: str = Form(...),
    password: str = Form(...),
):
    email_norm = email.strip().lower()

    # bcrypt ограничение: максимум 72 байта
    if len(password.encode("utf-8")) > 72:
        return request.app.state.templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "Пароль слишком длинный. Используй до ~50 символов."}
        )

    async with SessionLocal() as session:
        exists = await session.scalar(select(User).where(User.email == email_norm))
        if exists:
            return request.app.state.templates.TemplateResponse(
                "register.html",
                {"request": request, "error": "Пользователь с такой почтой уже существует."}
            )

        user = User(
            last_name=last_name.strip(),
            first_name=first_name.strip(),
            middle_name=middle_name.strip() or None,
            email=email_norm,
            password_hash=hash_password(password),
        )
        session.add(user)
        await session.commit()
        await session.refresh(user)

    request.session["user_id"] = int(user.id)
    return RedirectResponse(url="/", status_code=303)


@router.get("/login")
async def login_page(request: Request):
    return request.app.state.templates.TemplateResponse(
        "login.html",
        {"request": request, "error": None}
    )


@router.post("/login")
async def login(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
):
    email_norm = email.strip().lower()

    # защита от падений на bcrypt
    if len(password.encode("utf-8")) > 72:
        return request.app.state.templates.TemplateResponse(
            "login.html",
            {"request": request, "error": "Пароль слишком длинный."}
        )

    async with SessionLocal() as session:
        user = await session.scalar(select(User).where(User.email == email_norm))

        if (not user) or (not verify_password(password, user.password_hash)):
            return request.app.state.templates.TemplateResponse(
                "login.html",
                {"request": request, "error": "Неверная почта или пароль."}
            )

    request.session["user_id"] = int(user.id)
    return RedirectResponse(url="/", status_code=303)


@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)
