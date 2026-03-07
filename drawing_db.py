"""
drawing_db.py — SQLAlchemy models and DrawingDatabase class.

Used by drawing_scanner.py and any other program needing to look up
the latest PDF/STEP/SolidWorks file for a given part number.

File identity columns (pdf_file_id, step_file_id, etc.) store Box file IDs.
Use the Box API to generate download/preview URLs from those IDs.

Supports SQLite (local) and PostgreSQL (Render.com) via DATABASE_URL.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
    create_engine,
    or_,
)
from sqlalchemy.orm import DeclarativeBase, Session, relationship


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class Part(Base):
    __tablename__ = "parts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    part_num = Column(Text, nullable=False, unique=True)   # 6-digit string
    description = Column(Text, nullable=True)
    box_folder_id = Column(Text, nullable=True)            # Box folder ID
    latest_revision = Column(Text, nullable=True)          # denormalized
    scanned_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)

    drawings = relationship(
        "Drawing", back_populates="part", cascade="all, delete-orphan"
    )

    def __repr__(self) -> str:
        return f"<Part {self.part_num!r} rev={self.latest_revision!r}>"


class Drawing(Base):
    __tablename__ = "drawings"
    __table_args__ = (
        UniqueConstraint("part_id", "revision", name="uq_drawing_part_rev"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    part_id = Column(Integer, ForeignKey("parts.id", ondelete="CASCADE"), nullable=False)
    revision = Column(Text, nullable=True)      # NULL = no revision in filename
    is_latest = Column(Boolean, nullable=False, default=False)
    is_released = Column(Boolean, nullable=False, default=False)  # True = letter rev
    pdf_file_id = Column(Text, nullable=True)      # Box file ID
    step_file_id = Column(Text, nullable=True)     # Box file ID
    slddrw_file_id = Column(Text, nullable=True)   # Box file ID
    sldprt_file_id = Column(Text, nullable=True)   # Box file ID
    sldasm_file_id = Column(Text, nullable=True)   # Box file ID
    scanned_at = Column(DateTime(timezone=True), nullable=True)

    part = relationship("Part", back_populates="drawings")

    def __repr__(self) -> str:
        return (
            f"<Drawing part_id={self.part_id} rev={self.revision!r} "
            f"latest={self.is_latest}>"
        )


# ---------------------------------------------------------------------------
# Database wrapper
# ---------------------------------------------------------------------------

class DrawingDatabase:
    """
    Thin wrapper around SQLAlchemy engine/session.

    Usage
    -----
    db = DrawingDatabase("sqlite:///drawings.db")
    db.init_schema()
    part = db.get_part("100100")
    drawing = db.get_latest_drawing("100100")
    # drawing.pdf_file_id is a Box file ID; use Box API to get a download URL
    """

    def __init__(self, database_url: str) -> None:
        connect_args = {}
        if database_url.startswith("sqlite"):
            connect_args["check_same_thread"] = False
        self._engine = create_engine(
            database_url,
            connect_args=connect_args,
            pool_pre_ping=True,  # reconnect silently if the server closed the connection
        )

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def init_schema(self) -> None:
        """Create tables if they don't exist (idempotent)."""
        Base.metadata.create_all(self._engine)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_part(self, part_num: str) -> Optional[Part]:
        """Return the Part row for *part_num*, or None."""
        with Session(self._engine) as session:
            part = session.query(Part).filter_by(part_num=part_num).first()
            if part:
                session.expunge(part)
            return part

    def get_latest_drawing(self, part_num: str) -> Optional[Drawing]:
        """Return the Drawing row marked is_latest=True for *part_num*, or None."""
        with Session(self._engine) as session:
            part = session.query(Part).filter_by(part_num=part_num).first()
            if part is None:
                return None
            drawing = (
                session.query(Drawing)
                .filter_by(part_id=part.id, is_latest=True)
                .first()
            )
            if drawing:
                session.expunge(drawing)
            return drawing

    def get_all_drawings_for_part(self, part_num: str) -> list[Drawing]:
        """Return all Drawing rows for *part_num*, ordered by revision."""
        with Session(self._engine) as session:
            part = session.query(Part).filter_by(part_num=part_num).first()
            if part is None:
                return []
            drawings = (
                session.query(Drawing)
                .filter_by(part_id=part.id)
                .order_by(Drawing.revision)
                .all()
            )
            for d in drawings:
                session.expunge(d)
            return drawings

    def get_all_parts(self) -> list[Part]:
        """Return all Part rows, ordered by part_num."""
        with Session(self._engine) as session:
            parts = session.query(Part).order_by(Part.part_num).all()
            for p in parts:
                session.expunge(p)
            return parts

    def search_parts(self, query: str) -> list[Part]:
        """Return Parts whose part_num or description match *query* (LIKE)."""
        pattern = f"%{query}%"
        with Session(self._engine) as session:
            parts = (
                session.query(Part)
                .filter(
                    or_(
                        Part.part_num.like(pattern),
                        Part.description.like(pattern),
                    )
                )
                .order_by(Part.part_num)
                .all()
            )
            for p in parts:
                session.expunge(p)
            return parts

    # ------------------------------------------------------------------
    # Write operations (used by scanner)
    # ------------------------------------------------------------------

    def upsert_part(
        self,
        *,
        part_num: str,
        description: Optional[str],
        box_folder_id: str,
        latest_revision: Optional[str],
    ) -> int:
        """
        Insert or update a Part row.  Returns the part's database id.
        """
        now = _now()
        with Session(self._engine) as session:
            part = session.query(Part).filter_by(part_num=part_num).first()
            if part is None:
                part = Part(
                    part_num=part_num,
                    description=description,
                    box_folder_id=box_folder_id,
                    latest_revision=latest_revision,
                    scanned_at=now,
                    updated_at=now,
                )
                session.add(part)
            else:
                part.description = description
                part.box_folder_id = box_folder_id
                part.latest_revision = latest_revision
                part.updated_at = now
                part.scanned_at = now
            session.commit()
            return part.id

    def upsert_drawing(
        self,
        *,
        part_id: int,
        revision: Optional[str],
        is_latest: bool,
        is_released: bool,
        pdf_file_id: Optional[str] = None,
        step_file_id: Optional[str] = None,
        slddrw_file_id: Optional[str] = None,
        sldprt_file_id: Optional[str] = None,
        sldasm_file_id: Optional[str] = None,
    ) -> None:
        """
        Insert or update a Drawing row identified by (part_id, revision).
        """
        now = _now()
        with Session(self._engine) as session:
            drawing = (
                session.query(Drawing)
                .filter_by(part_id=part_id, revision=revision)
                .first()
            )
            if drawing is None:
                drawing = Drawing(
                    part_id=part_id,
                    revision=revision,
                    is_latest=is_latest,
                    is_released=is_released,
                    pdf_file_id=pdf_file_id,
                    step_file_id=step_file_id,
                    slddrw_file_id=slddrw_file_id,
                    sldprt_file_id=sldprt_file_id,
                    sldasm_file_id=sldasm_file_id,
                    scanned_at=now,
                )
                session.add(drawing)
            else:
                drawing.is_latest = is_latest
                drawing.is_released = is_released
                drawing.pdf_file_id = pdf_file_id
                drawing.step_file_id = step_file_id
                drawing.slddrw_file_id = slddrw_file_id
                drawing.sldprt_file_id = sldprt_file_id
                drawing.sldasm_file_id = sldasm_file_id
                drawing.scanned_at = now
            session.commit()

    def get_stats(self) -> dict:
        """Return summary counts."""
        with Session(self._engine) as session:
            total_parts = session.query(Part).count()
            parts_with_pdf = (
                session.query(Drawing)
                .filter(Drawing.pdf_file_id.isnot(None), Drawing.is_latest == True)
                .count()
            )
            parts_with_step = (
                session.query(Drawing)
                .filter(Drawing.step_file_id.isnot(None), Drawing.is_latest == True)
                .count()
            )
            total_drawings = session.query(Drawing).count()
            released = session.query(Drawing).filter_by(is_released=True).count()
        return {
            "total_parts": total_parts,
            "total_drawing_rows": total_drawings,
            "latest_with_pdf": parts_with_pdf,
            "latest_with_step": parts_with_step,
            "released_revisions": released,
        }
