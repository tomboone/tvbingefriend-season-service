"""SQLAlchemy model for a season."""
from sqlalchemy import String, Integer, Text, Index
from sqlalchemy.orm import mapped_column, Mapped
from sqlalchemy.dialects.mysql import JSON

from tvbingefriend_season_service.models.base import Base


class Season(Base):
    """SQLAlchemy model for a season."""
    __tablename__ = "seasons"

    # Attributes
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=False)
    show_id: Mapped[int] = mapped_column(Integer, nullable=False)
    url: Mapped[str] = mapped_column(Text)
    number: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str | None] = mapped_column(Text)
    episodeOrder: Mapped[int | None] = mapped_column(Integer)
    premiereDate: Mapped[str | None] = mapped_column(String(255))
    endDate: Mapped[str | None] = mapped_column(String(255))
    network: Mapped[dict | None] = mapped_column(JSON)
    webChannel: Mapped[dict | None] = mapped_column(JSON)
    image: Mapped[dict | None] = mapped_column(JSON)
    summary: Mapped[str | None] = mapped_column(Text)
    _links: Mapped[dict | None] = mapped_column(JSON)

    # Indexes for query optimization
    __table_args__ = (
        Index('idx_seasons_show_number', 'show_id', 'number'),
        Index('idx_seasons_show_id', 'show_id'),
    )
