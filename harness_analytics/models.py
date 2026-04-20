"""SQLAlchemy ORM models matching harness_analytics schema."""

from datetime import date, datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Computed,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


class Application(Base):
    __tablename__ = "applications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_number: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    filing_date: Mapped[Optional[date]] = mapped_column(Date)
    issue_date: Mapped[Optional[date]] = mapped_column(Date)
    patent_number: Mapped[Optional[str]] = mapped_column(Text)
    application_status_code: Mapped[Optional[str]] = mapped_column(Text)
    application_status_text: Mapped[Optional[str]] = mapped_column(Text)
    application_status_date: Mapped[Optional[date]] = mapped_column(Date)
    invention_title: Mapped[Optional[str]] = mapped_column(Text)
    customer_number: Mapped[Optional[str]] = mapped_column(Text)
    hdp_customer_number: Mapped[Optional[str]] = mapped_column(Text)
    attorney_docket_number: Mapped[Optional[str]] = mapped_column(Text)
    confirmation_number: Mapped[Optional[str]] = mapped_column(Text)
    group_art_unit: Mapped[Optional[str]] = mapped_column(Text)
    patent_class: Mapped[Optional[str]] = mapped_column(Text)
    patent_subclass: Mapped[Optional[str]] = mapped_column(Text)
    examiner_first_name: Mapped[Optional[str]] = mapped_column(Text)
    examiner_last_name: Mapped[Optional[str]] = mapped_column(Text)
    examiner_phone: Mapped[Optional[str]] = mapped_column(Text)
    assignee_name: Mapped[Optional[str]] = mapped_column(Text)
    applicant_name: Mapped[Optional[str]] = mapped_column(Text)
    issue_year: Mapped[Optional[int]] = mapped_column(
        Integer,
        Computed("EXTRACT(YEAR FROM issue_date)::integer", persisted=True),
    )
    xml_raw: Mapped[Optional[str]] = mapped_column(Text)
    continuity_child_of_prior_us: Mapped[bool] = mapped_column(Boolean, default=False)
    has_child_continuation: Mapped[Optional[bool]] = mapped_column(Boolean)
    earliest_priority_date: Mapped[Optional[date]] = mapped_column(Date)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    imported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    attorneys: Mapped[list["ApplicationAttorney"]] = relationship(
        back_populates="application", cascade="all, delete-orphan"
    )
    inventors: Mapped[list["Inventor"]] = relationship(
        back_populates="application", cascade="all, delete-orphan"
    )
    prosecution_events: Mapped[list["ProsecutionEvent"]] = relationship(
        back_populates="application", cascade="all, delete-orphan"
    )
    file_wrapper_documents: Mapped[list["FileWrapperDocument"]] = relationship(
        back_populates="application", cascade="all, delete-orphan"
    )
    analytics: Mapped[Optional["ApplicationAnalytics"]] = relationship(
        back_populates="application", uselist=False, cascade="all, delete-orphan"
    )


class ApplicationAttorney(Base):
    __tablename__ = "application_attorneys"
    __table_args__ = (
        UniqueConstraint(
            "application_id", "registration_number", "attorney_role",
            name="uq_app_atty_reg_role",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False
    )
    registration_number: Mapped[Optional[str]] = mapped_column(Text)
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    phone: Mapped[Optional[str]] = mapped_column(Text)
    agent_status: Mapped[Optional[str]] = mapped_column(Text)
    attorney_role: Mapped[Optional[str]] = mapped_column(Text)
    is_first_attorney: Mapped[bool] = mapped_column(Boolean, default=False)

    application: Mapped["Application"] = relationship(back_populates="attorneys")


class Inventor(Base):
    __tablename__ = "inventors"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False
    )
    first_name: Mapped[Optional[str]] = mapped_column(Text)
    last_name: Mapped[Optional[str]] = mapped_column(Text)
    city: Mapped[Optional[str]] = mapped_column(Text)
    country_code: Mapped[Optional[str]] = mapped_column(Text)

    application: Mapped["Application"] = relationship(back_populates="inventors")


class ProsecutionEvent(Base):
    __tablename__ = "prosecution_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False
    )
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False)
    transaction_description: Mapped[str] = mapped_column(Text, nullable=False)
    status_number: Mapped[Optional[str]] = mapped_column(Text)
    status_description: Mapped[Optional[str]] = mapped_column(Text)
    event_type: Mapped[Optional[str]] = mapped_column(Text)
    seq_order: Mapped[Optional[int]] = mapped_column(Integer)

    application: Mapped["Application"] = relationship(back_populates="prosecution_events")


class FileWrapperDocument(Base):
    __tablename__ = "file_wrapper_documents"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False
    )
    document_code: Mapped[Optional[str]] = mapped_column(Text)
    document_description: Mapped[Optional[str]] = mapped_column(Text)
    mail_room_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    page_quantity: Mapped[Optional[int]] = mapped_column(Integer)
    document_category: Mapped[Optional[str]] = mapped_column(Text)

    application: Mapped["Application"] = relationship(
        back_populates="file_wrapper_documents"
    )


class ApplicationAnalytics(Base):
    __tablename__ = "application_analytics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False, unique=True
    )
    nonfinal_oa_count: Mapped[int] = mapped_column(Integer, default=0)
    final_oa_count: Mapped[int] = mapped_column(Integer, default=0)
    total_substantive_oas: Mapped[int] = mapped_column(Integer, default=0)
    first_oa_date: Mapped[Optional[date]] = mapped_column(Date)
    first_nonfinal_oa_date: Mapped[Optional[date]] = mapped_column(Date)
    first_final_oa_date: Mapped[Optional[date]] = mapped_column(Date)
    first_noa_date: Mapped[Optional[date]] = mapped_column(Date)
    had_examiner_interview: Mapped[bool] = mapped_column(Boolean, default=False)
    interview_count: Mapped[int] = mapped_column(Integer, default=0)
    interview_before_noa: Mapped[bool] = mapped_column(Boolean, default=False)
    interview_led_to_noa: Mapped[bool] = mapped_column(Boolean, default=False)
    days_interview_to_noa: Mapped[Optional[int]] = mapped_column(Integer)
    rce_count: Mapped[int] = mapped_column(Integer, default=0)
    first_rce_date: Mapped[Optional[date]] = mapped_column(Date)
    days_filing_to_first_oa: Mapped[Optional[int]] = mapped_column(Integer)
    days_filing_to_noa: Mapped[Optional[int]] = mapped_column(Integer)
    days_filing_to_issue: Mapped[Optional[int]] = mapped_column(Integer)
    is_jac: Mapped[bool] = mapped_column(Boolean, default=False)
    office_name: Mapped[Optional[str]] = mapped_column(Text)
    ifw_a_ne_count: Mapped[int] = mapped_column(Integer, default=0)
    ifw_ctrs_count: Mapped[int] = mapped_column(Integer, default=0)
    # Heuristic extension-of-time counts (see extension_metrics); >90 days late not stored.
    ctnf_ext_1mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctnf_ext_2mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctnf_ext_3mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctfr_ext_1mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctfr_ext_2mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctfr_ext_3mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctrs_ext_1mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctrs_ext_2mo_count: Mapped[int] = mapped_column(Integer, default=0)
    ctrs_ext_3mo_count: Mapped[int] = mapped_column(Integer, default=0)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    application: Mapped["Application"] = relationship(back_populates="analytics")


# ---------------------------------------------------------------------------
# Auth (M0.2)
# ---------------------------------------------------------------------------


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    email: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    name: Mapped[Optional[str]] = mapped_column(Text)
    password_hash: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str] = mapped_column(Text, nullable=False, default="VIEWER")
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))


class UserSession(Base):
    __tablename__ = "user_sessions"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    user_agent: Mapped[Optional[str]] = mapped_column(Text)
    ip: Mapped[Optional[str]] = mapped_column(Text)
