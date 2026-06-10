# worker/database/database.py
from sqlmodel import create_engine, Session
from dotenv import load_dotenv
import os

load_dotenv()

is_local = os.getenv("ENVIRONMENT", "production") == "local"
DATABASE_URL = os.getenv('DATABASE_URL')

connect_args = {}
if is_local:
    connect_args["sslmode"] = "disable"
else:
    connect_args["sslmode"] = "require"

engine = create_engine(
    DATABASE_URL,
    echo=False,
    pool_pre_ping=True,
    connect_args=connect_args
)


def get_engine():
    """Retorna el engine para uso fuera de FastAPI (workers, scripts)"""
    return engine


def get_session():
    """
    Retorna una sesión sincrónica para uso fuera de FastAPI.
    Uso: with get_session() as session: ...
    """
    return Session(engine)