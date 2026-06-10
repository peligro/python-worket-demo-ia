# worker/services/rag/pdf_processor.py
"""
Servicio de procesamiento de PDF para RAG - Versión worker.
Extrae pares P/R, genera embeddings y guarda en la base de datos.

IMPORTANTE: Todos los imports son relativos al worker, NO a api/.
"""
import re
import logging
from typing import List
import PyPDF2
from sentence_transformers import SentenceTransformer
from sqlmodel import Session

# ✅ Import local del worker
from worker.models.rag_chunk import RAGChunk

logger = logging.getLogger(__name__)

# Modelo de embeddings (singleton)
EMBEDDING_MODEL = SentenceTransformer('all-MiniLM-L6-v2')


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extrae texto plano de un archivo PDF."""
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ""
        for page in pdf_reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text.strip()


def parse_sections(text: str) -> List[dict]:
    """Parsea el texto del manual y extrae secciones con pares P/R."""
    chunks = []
    
    # Primero, dividir por secciones
    section_pattern = r'SECCIÓN\s*\d+:\s*([^\n]+)\n(.*?)(?=\nSECCIÓN\s*\d+:|\Z)'
    sections = re.findall(section_pattern, text, re.DOTALL | re.IGNORECASE)
    
    for section_title, section_content in sections:
        section_title = section_title.strip()
        
        # Regex MÁS FLEXIBLE para pares P: / R:
        # Busca "P:" o "P:" seguido de pregunta, luego "R:" o "R:" seguido de respuesta
        pr_pattern = r'[Pp]:\s*([^\n]+(?:\n(?![Rr]:).*)*?)\s*[Rr]:\s*([^\n]+(?:\n(?:(?![Pp]:|[Ss]ECCIÓN).)*)*)'
        
        pr_pairs = re.findall(pr_pattern, section_content, re.DOTALL)
        
        for question, answer in pr_pairs:
            question = question.strip()
            answer = answer.strip()
            
            # Saltar si están vacíos
            if not question or not answer:
                continue
            
            keywords = extract_keywords(f"{question} {answer}")
            
            chunks.append({
                "section": section_title.upper().replace(" ", "_").replace(":", ""),
                "question": question,
                "answer": answer,
                "keywords": keywords,
            })
    
    return chunks


def extract_keywords(text: str, max_keywords: int = 10) -> List[str]:
    """Extrae keywords relevantes de un texto."""
    stopwords = {
        'el', 'la', 'los', 'las', 'un', 'una', 'unos', 'unas',
        'de', 'del', 'al', 'en', 'con', 'por', 'para', 'sin', 'sobre',
        'y', 'o', 'pero', 'que', 'qué', 'cuál', 'cómo', 'cuándo', 'dónde',
        'se', 'es', 'son', 'ser', 'estar', 'está', 'están', 'fue', 'fueron',
        'ha', 'han', 'he', 'hemos', 'lo', 'le', 'les', 'me', 'te', 'nos', 'os'
    }
    
    tokens = re.findall(r'\b[a-zA-ZáéíóúÁÉÍÓÚñÑ]+\b', text.lower())
    
    seen = set()
    keywords = []
    for token in tokens:
        if token not in stopwords and token not in seen and len(token) > 2:
            keywords.append(token)
            seen.add(token)
            if len(keywords) >= max_keywords:
                break
    
    return keywords


def generate_embedding(text: str) -> List[float]:
    """Genera embedding vectorial para un texto."""
    embedding = EMBEDDING_MODEL.encode(text, convert_to_numpy=True)
    return embedding.tolist()


def process_pdf_to_chunks(pdf_path: str, session: Session, source_pdf: str) -> int:
    """
    Procesa un PDF completo y guarda chunks en la base de datos.
    
    Args:
        pdf_path: Ruta local al archivo PDF
        session: Sesión de SQLModel activa
        source_pdf: Nombre del archivo para metadata
    
    Returns:
        Número de chunks creados
    """
    logger.info(f"📄 Procesando PDF: {pdf_path}")
    
    # 1. Extraer texto del PDF
    text = extract_text_from_pdf(pdf_path)
    logger.info(f"✅ Texto extraído: {len(text)} caracteres")
    
    # 2. Parsear secciones y pares P/R
    chunks_data = parse_sections(text)
    logger.info(f"✅ Pares P/R encontrados: {len(chunks_data)}")
    
    if not chunks_data:
        logger.warning("⚠️ No se encontraron pares P/R válidos en el PDF")
        return 0
    
    # 3. Generar embeddings y guardar en DB
    created_count = 0
    for chunk_data in chunks_data:
        embedding_text = f"{chunk_data['question']} {chunk_data['answer']}"
        embedding = generate_embedding(embedding_text)
        
        chunk = RAGChunk(
            section=chunk_data["section"],
            question=chunk_data["question"],
            answer=chunk_data["answer"],
            keywords=chunk_data["keywords"],
            embedding=embedding,
            source_pdf=source_pdf,
            is_active=True,
        )
        
        session.add(chunk)
        created_count += 1
    
    session.commit()
    logger.info(f"✅ {created_count} chunks guardados en rag_chunks")
    
    return created_count
