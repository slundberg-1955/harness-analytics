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
from sqlalchemy import BigInteger
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
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
    # M9: per-user opaque token used to sign the personal ICS feed URL.
    ics_token: Mapped[Optional[str]] = mapped_column(Text, unique=True)
    # M11: supervising user. Used by /actions/inbox?assignee=team to roll up a
    # supervisor's direct reports' open deadlines.
    manager_user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )


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


# ---------------------------------------------------------------------------
# Timeline (M2/M3)
# ---------------------------------------------------------------------------


class IfwRule(Base):
    __tablename__ = "ifw_rules"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_ifw_rules_tenant_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    code: Mapped[str] = mapped_column(Text, nullable=False)
    aliases: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    description: Mapped[str] = mapped_column(Text, nullable=False)
    kind: Mapped[str] = mapped_column(Text, nullable=False)
    ssp_months: Mapped[Optional[int]] = mapped_column(Integer)
    max_months: Mapped[Optional[int]] = mapped_column(Integer)
    due_months_from_grant: Mapped[Optional[int]] = mapped_column(Integer)
    grace_months_from_grant: Mapped[Optional[int]] = mapped_column(Integer)
    from_filing_months: Mapped[Optional[int]] = mapped_column(Integer)
    from_priority_months: Mapped[Optional[int]] = mapped_column(Integer)
    base_months_from_priority: Mapped[Optional[int]] = mapped_column(Integer)
    late_months_from_priority: Mapped[Optional[int]] = mapped_column(Integer)
    extendable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    trigger_label: Mapped[str] = mapped_column(Text, nullable=False)
    user_note: Mapped[str] = mapped_column(Text, nullable=False, default="")
    authority: Mapped[str] = mapped_column(Text, nullable=False)
    warnings: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    priority_tier: Mapped[Optional[str]] = mapped_column(Text)
    patent_type_applicability: Mapped[list[str]] = mapped_column(
        ARRAY(Text),
        nullable=False,
        default=lambda: ["UTILITY", "DESIGN", "PLANT", "REISSUE", "REEXAM"],
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class IfwRuleVersion(Base):
    """Snapshot of the pre-edit state of an ``ifw_rules`` row (M15).

    Written by ``timeline_api.admin_update_rule`` before applying the patch
    so the admin UI can show a per-rule history and revert to any version.
    Revert just enqueues a normal ``PUT`` against the editable fields, which
    in turn writes a new history row — so revert is itself audited.
    """

    __tablename__ = "ifw_rule_versions"
    __table_args__ = (
        UniqueConstraint(
            "rule_id", "version", name="uq_ifw_rule_versions_rule_version"
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    rule_id: Mapped[int] = mapped_column(
        ForeignKey("ifw_rules.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    snapshot_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    edited_by_user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    edited_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class UnmappedIfwCode(Base):
    __tablename__ = "unmapped_ifw_codes"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_unmapped_ifw_tenant_code"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    code: Mapped[str] = mapped_column(Text, nullable=False)
    count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    first_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    last_seen: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ComputedDeadline(Base):
    __tablename__ = "computed_deadlines"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    application_id: Mapped[int] = mapped_column(
        ForeignKey("applications.id", ondelete="CASCADE"), nullable=False
    )
    rule_id: Mapped[int] = mapped_column(ForeignKey("ifw_rules.id"), nullable=False)
    trigger_event_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("prosecution_events.id", ondelete="SET NULL")
    )
    trigger_document_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("file_wrapper_documents.id", ondelete="SET NULL")
    )
    trigger_date: Mapped[date] = mapped_column(Date, nullable=False)
    trigger_source: Mapped[str] = mapped_column(Text, nullable=False)
    ssp_date: Mapped[Optional[date]] = mapped_column(Date)
    statutory_bar_date: Mapped[Optional[date]] = mapped_column(Date)
    primary_date: Mapped[date] = mapped_column(Date, nullable=False)
    primary_label: Mapped[str] = mapped_column(Text, nullable=False)
    rows_json: Mapped[list] = mapped_column(JSONB, nullable=False, default=list)
    window_open_date: Mapped[Optional[date]] = mapped_column(Date)
    grace_end_date: Mapped[Optional[date]] = mapped_column(Date)
    ids_phases_json: Mapped[Optional[list]] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="OPEN")
    completed_event_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("prosecution_events.id", ondelete="SET NULL")
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    superseded_by: Mapped[Optional[int]] = mapped_column(
        ForeignKey("computed_deadlines.id", ondelete="SET NULL")
    )
    assigned_user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    snoozed_until: Mapped[Optional[date]] = mapped_column(Date)
    notes: Mapped[Optional[str]] = mapped_column(Text)
    warnings: Mapped[Optional[list[str]]] = mapped_column(ARRAY(Text))
    severity: Mapped[Optional[str]] = mapped_column(Text)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
    computed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class VerifiedDeadline(Base):
    """Attorney-verified deadline (M9).

    A separate row keeps the materializer free to recompute ``computed_deadlines``
    aggressively without losing the human verification. We only store the
    fields we need to render a "Verified" badge + audit trail; the underlying
    deadline row remains the source of truth for the date itself.
    """
    __tablename__ = "verified_deadlines"
    __table_args__ = (
        UniqueConstraint("deadline_id", name="uq_verified_deadline"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    deadline_id: Mapped[int] = mapped_column(
        ForeignKey("computed_deadlines.id", ondelete="CASCADE"), nullable=False
    )
    verified_by_user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    verified_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    verified_date: Mapped[date] = mapped_column(Date, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False, default="manual")
    note: Mapped[Optional[str]] = mapped_column(Text)


class DeadlineEvent(Base):
    __tablename__ = "deadline_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    deadline_id: Mapped[int] = mapped_column(
        ForeignKey("computed_deadlines.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(Text, nullable=False)
    payload_json: Mapped[Optional[dict]] = mapped_column(JSONB)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class SavedView(Base):
    """User-defined named filter snapshot (M12).

    Surface is a free-form namespace (``inbox``, future ``portfolio``, etc.)
    so we can ship saved views for new pages without a schema change. Only
    one row per (user, surface) may carry ``is_default = TRUE`` — enforced
    in the API write path, not via a partial-unique index, so the JSON write
    path stays straightforward.
    """

    __tablename__ = "saved_views"
    __table_args__ = (
        UniqueConstraint(
            "user_id", "surface", "name", name="uq_saved_views_user_surface_name"
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    surface: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    params_json: Mapped[dict] = mapped_column(JSONB, nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )


class SupersessionMap(Base):
    __tablename__ = "supersession_map"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "prev_kind", "new_kind", name="uq_supersession"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    prev_kind: Mapped[str] = mapped_column(Text, nullable=False)
    new_kind: Mapped[str] = mapped_column(Text, nullable=False)
    tenant_id: Mapped[str] = mapped_column(Text, nullable=False, default="global")
