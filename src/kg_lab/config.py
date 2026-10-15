from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    neo4j_uri: str = "neo4j://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "change-me-now"
    neo4j_database: str = "neo4j"
    openai_api_key: str | None = None
    openai_model: str = "gpt-5"
    document_embedding_dimensions: int = 128
    kafka_bootstrap_servers: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:18081"
    kafka_consumer_group: str = "kg-projection-v2"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


settings = Settings()
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
