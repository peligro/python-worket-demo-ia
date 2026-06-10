# worker/models/rag_chunk.py
from typing import Optional, List
from datetime import datetime, timezone
from sqlmodel import SQLModel, Field, Column
from sqlalchemy import Text, String, Integer, Boolean, DateTime, Index, ARRAY
from pgvector.sqlalchemy import Vector
import enum


class RAGChunk(SQLModel, table=True):
    """
    Chunk extraído de PDF para RAG con embeddings vectoriales.
    Cada chunk representa un par P/R del manual de atención al cliente.
    
    Copia local para el worker (sin dependencias de api/).
    """
    __tablename__ = "rag_chunks"
    
    id: Optional[int] = Field(default=None, primary_key=True)
    
    # Metadata del chunk
    section: str = Field(..., max_length=50, description="Sección del manual")
    question: str = Field(sa_column=Column(Text), description="Pregunta del manual")
    answer: str = Field(sa_column=Column(Text), description="Respuesta del manual")
    
    # Keywords para búsqueda híbrida - ARRAY para índice GIN
    keywords: List[str] = Field(default=[], sa_column=Column(ARRAY(String)))
    
    # Embedding vectorial para búsqueda semántica (pgvector)
    # dim=384 corresponde al modelo all-MiniLM-L6-v2
    embedding: Optional[List[float]] = Field(
        default=None,
        sa_column=Column(Vector(384))
    )
    
    # Metadata de origen
    source_pdf: str = Field(..., max_length=255, description="Nombre del archivo PDF")
    page_number: Optional[int] = Field(default=None, description="Número de página")
    
    # Estado y auditoría
    is_active: bool = Field(default=True, description="¿Chunk activo para búsqueda?")
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        sa_column=Column(DateTime(timezone=True), nullable=False)
    )
    updated_at: Optional[datetime] = Field(
        default=None,
        sa_column=Column(DateTime(timezone=True), onupdate=lambda: datetime.now(timezone.utc))
    )
    
    # Índices para búsqueda eficiente
    __table_args__ = (
        Index('idx_rag_chunks_section', 'section'),
        Index('idx_rag_chunks_keywords', 'keywords', postgresql_using='gin'),
        Index('idx_rag_chunks_created', 'created_at'),
        {"extend_existing": True}  # ← Para evitar "Table already defined"
    )
