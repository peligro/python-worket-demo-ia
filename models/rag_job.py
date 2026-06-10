# worker/models/rag_job.py
from typing import Optional
from datetime import datetime, timezone
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import Text, String, Integer, DateTime, Index, Enum as SQLEnum
import enum

class JobStatus(str, enum.Enum):
    QUEUED = "queued"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class RAGJob(SQLModel, table=True):
    __tablename__ = "rag_jobs"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    filename: str = Field(..., max_length=255)
    s3_key: Optional[str] = Field(default=None, max_length=500)
    file_size: Optional[int] = Field(default=None)
    status: JobStatus = Field(
        default=JobStatus.QUEUED,
        sa_column=Column(SQLEnum(JobStatus, name="jobstatus", create_type=True))
    )
    error_message: Optional[str] = Field(default=None, sa_column=Column(Text))
    chunks_created: Optional[int] = Field(default=None)
    processing_time_ms: Optional[int] = Field(default=None)
    user_id: Optional[int] = Field(default=None, index=True)
    
    # ✅ Campos de timestamp agregados
    """
    started_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True))
    )
    completed_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True))
    )
    failed_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True))
    )
    """
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False)
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    )
    
    __table_args__ = (
        Index('idx_rag_jobs_status', 'status'),
        Index('idx_rag_jobs_created', 'created_at'),
        Index('idx_rag_jobs_user', 'user_id'),
        {"extend_existing": True}
    )