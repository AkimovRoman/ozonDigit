from __future__ import annotations

from decimal import Decimal
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy import (
    String,
    Text,
    DateTime,
    Date,
    BigInteger,
    Integer,
    Boolean,
    Numeric,
    ForeignKey,
    func,
)


class Base(DeclarativeBase):
    pass


# =========================
# USERS
# =========================
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    last_name: Mapped[str] = mapped_column(String(100), nullable=False)
    first_name: Mapped[str] = mapped_column(String(100), nullable=False)
    middle_name: Mapped[str | None] = mapped_column(String(100), nullable=True)

    email: Mapped[str] = mapped_column(String(320), nullable=False, unique=True)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)

    created_at: Mapped[DateTime] = mapped_column(
        DateTime, server_default=func.now(), nullable=False
    )

    ozon_links: Mapped[list["UserOzonAccount"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
    )


# =========================
# OZON ACCOUNTS
# =========================
class OzonAccount(Base):
    __tablename__ = "ozon_accounts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    # ===== Ozon Seller API (обычный кабинет)
    client_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    api_key: Mapped[str] = mapped_column(Text, nullable=False)

    name: Mapped[str | None] = mapped_column(String(150), nullable=True)

    # ===== Ozon Performance API
    perf_client_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    perf_client_secret: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[DateTime] = mapped_column(
        DateTime,
        server_default=func.now(),
        nullable=False,
    )

    # ===== relations
    user_links: Mapped[list["UserOzonAccount"]] = relationship(
        back_populates="ozon_account",
        cascade="all, delete-orphan",
    )

    products: Mapped[list["Product"]] = relationship(
        back_populates="ozon_account",
        cascade="all, delete-orphan",
    )

    tracked_campaigns: Mapped[list["TrackedCampaign"]] = relationship(
        back_populates="ozon_account",
        cascade="all, delete-orphan",
    )


# =========================
# USER <-> OZON ACCOUNT
# =========================
class UserOzonAccount(Base):
    __tablename__ = "user_ozon_accounts"

    user_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
    )

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("ozon_accounts.id", ondelete="CASCADE"),
        primary_key=True,
    )

    role: Mapped[str] = mapped_column(
        String(30),
        nullable=False,
        server_default="member",
    )

    added_at: Mapped[DateTime] = mapped_column(
        DateTime,
        server_default=func.now(),
        nullable=False,
    )

    user: Mapped["User"] = relationship(back_populates="ozon_links")
    ozon_account: Mapped["OzonAccount"] = relationship(back_populates="user_links")


# =========================
# PRODUCTS
# =========================
class Product(Base):
    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("ozon_accounts.id", ondelete="CASCADE"),
        nullable=False,
    )

    sku: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    offer_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)

    cost_price_rub: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    ozon_account: Mapped["OzonAccount"] = relationship(back_populates="products")

    ads_top_daily: Mapped[list["AdsTopDaily"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    ads_stencil_daily: Mapped[list["AdsStencilDaily"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    ads_total_daily: Mapped[list["AdsTotalDaily"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    conversions_daily: Mapped[list["ConversionsDaily"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    pricing_daily: Mapped[list["PricingDaily"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )
    tracked_queries: Mapped[list["TrackedQuery"]] = relationship(
        back_populates="product", cascade="all, delete-orphan"
    )


# =========================
# ADS TOP DAILY
# =========================
class AdsTopDaily(Base):
    __tablename__ = "ads_top_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[Date] = mapped_column(Date, nullable=False)

    spend_rub: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, server_default="0")
    orders_amount_rub: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False, server_default="0")
    orders_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    drr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    impressions: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    clicks: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    carts: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")

    bid_competitor: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    bid_ours: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    spend_per_order: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    ad_conversion: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="ads_top_daily")


# =========================
# ADS STENCIL DAILY
# =========================
class AdsStencilDaily(Base):
    __tablename__ = "ads_stencil_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[Date] = mapped_column(Date, nullable=False)

    spend_rub: Mapped[Decimal | None] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    orders_amount_rub: Mapped[Decimal | None] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    orders_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    drr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    ctr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    impressions: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    clicks: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    carts: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    bid_competitor: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    bid_ours: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    spend_per_order: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    ad_conversion: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="ads_stencil_daily")


# =========================
# ADS TOTAL DAILY
# =========================
class AdsTotalDaily(Base):
    __tablename__ = "ads_total_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[Date] = mapped_column(Date, nullable=False)

    spend_rub: Mapped[Decimal | None] = mapped_column(
        Numeric(14, 2), nullable=True
    )

    all_orders_amount_rub: Mapped[Decimal | None] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    all_orders_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    ad_orders_amount_rub: Mapped[Decimal | None] = mapped_column(
        Numeric(14, 2), nullable=True
    )
    ad_orders_count: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    drr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    drr_total: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    ctr: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    impressions: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    clicks: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )
    carts: Mapped[int | None] = mapped_column(
        BigInteger, nullable=True
    )

    ad_conversion: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="ads_total_daily")


# =========================
# CONVERSIONS DAILY
# =========================
class ConversionsDaily(Base):
    __tablename__ = "conversions_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[Date] = mapped_column(Date, nullable=False)

    impression_to_order: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    search_cat_to_cart: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    search_cat_to_card: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    card_to_cart: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    cart_total: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    cart_to_order: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)
    order_to_purchase: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="conversions_daily")


# =========================
# PRICING DAILY
# =========================
class PricingDaily(Base):
    __tablename__ = "pricing_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    day: Mapped[Date] = mapped_column(Date, nullable=False)

    our_price_rub: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    buyer_price_rub: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    ozon_card_price_rub: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)
    spp_percent: Mapped[Decimal | None] = mapped_column(Numeric(10, 4), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="pricing_daily")


# =========================
# TRACKED QUERIES
# =========================
class TrackedQuery(Base):
    __tablename__ = "tracked_queries"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )

    query_text: Mapped[str] = mapped_column(Text, nullable=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="true")
    created_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    product: Mapped["Product"] = relationship(back_populates="tracked_queries")
    daily: Mapped[list["TrackedQueryDaily"]] = relationship(
        back_populates="query", cascade="all, delete-orphan"
    )


# =========================
# TRACKED QUERY DAILY
# =========================
class TrackedQueryDaily(Base):
    __tablename__ = "tracked_query_daily"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("ozon_accounts.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("products.product_id", ondelete="CASCADE"), nullable=False
    )
    query_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("tracked_queries.id", ondelete="CASCADE"), nullable=False
    )

    day: Mapped[Date] = mapped_column(Date, nullable=False)

    position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cpc_rub: Mapped[Decimal | None] = mapped_column(Numeric(14, 2), nullable=True)

    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    query: Mapped["TrackedQuery"] = relationship(back_populates="daily")


# =========================
# TRACKED CAMPAIGNS
# =========================
class TrackedCampaign(Base):
    __tablename__ = "tracked_campaigns"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)

    ozon_account_id: Mapped[int] = mapped_column(
        BigInteger,
        ForeignKey("ozon_accounts.id", ondelete="CASCADE"),
        nullable=False,
    )

    # id кампании в Ozon (в API приходит строкой)
    campaign_id: Mapped[str] = mapped_column(String(32), nullable=False, unique=True)

    title: Mapped[str | None] = mapped_column(String(255), nullable=True)

    # createdAt из API (может быть null)
    ozon_created_at: Mapped[DateTime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)
    updated_at: Mapped[DateTime] = mapped_column(DateTime, server_default=func.now(), nullable=False)

    ozon_account: Mapped["OzonAccount"] = relationship(back_populates="tracked_campaigns")
