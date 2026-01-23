import uuid

from sqlalchemy import (
    BIGINT,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from .database import Base


class Frame(Base):
    __tablename__ = "frames"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    timestamp = Column(DateTime(timezone=True), nullable=True)
    ingest_timestamp = Column(DateTime(timezone=True), nullable=True)
    update_timestamp = Column(DateTime(timezone=True), nullable=True)
    stream_id = Column(String, nullable=True)
    fps = Column(Integer, nullable=True)
    scale = Column(Integer, nullable=True)


class Detection(Base):
    __tablename__ = "detections"

    id = Column(BIGINT, primary_key=True, autoincrement=True)
    frame_id = Column(PG_UUID(as_uuid=True), ForeignKey("frames.id"), nullable=False)
    detection_timestamp = Column(DateTime(timezone=True), nullable=False)
    label = Column(String, nullable=False)
    class_id = Column(Integer, nullable=False)
    confidence = Column(Float, nullable=False)
    x1 = Column(Float, nullable=False)
    y1 = Column(Float, nullable=False)
    x2 = Column(Float, nullable=False)
    y2 = Column(Float, nullable=False)
