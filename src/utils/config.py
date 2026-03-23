"""Configuration management for Agentic Ledger."""

from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import PostgresDsn, field_validator


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"  # ← ADD THIS to ignore extra fields like gemini_api_key
    )
    
    # PostgreSQL
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_DB: str = "agentic_ledger"
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_POOL_MIN_SIZE: int = 5
    POSTGRES_POOL_MAX_SIZE: int = 20
    
    @property
    def POSTGRES_DSN(self) -> str:
        """Build PostgreSQL DSN from components."""
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # MCP Server
    MCP_HOST: str = "127.0.0.1"
    MCP_PORT: int = 8000
    MCP_DEBUG: bool = False
    MCP_WORKERS: int = 4
    
    # Projection Daemon
    PROJECTION_POLL_INTERVAL_MS: int = 100
    PROJECTION_BATCH_SIZE: int = 500
    PROJECTION_MAX_RETRIES: int = 3
    PROJECTION_RETRY_DELAY_MS: int = 1000
    
    # Note: max_retry_attempts is NOT in the config - it's from .env
    # We'll let it be ignored via extra="ignore"
    
    # Security
    ENCRYPTION_KEY: str = "please-change-this-in-production"
    JWT_SECRET: str = "please-change-this-in-production"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"
    
    # Environment
    ENVIRONMENT: str = "development"
    DEBUG: bool = True
    
    @field_validator("LOG_LEVEL")
    def validate_log_level(cls, v: str) -> str:
        """Validate log level."""
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {allowed}")
        return v.upper()
    
    @field_validator("ENVIRONMENT")
    def validate_environment(cls, v: str) -> str:
        """Validate environment."""
        allowed = {"development", "staging", "production"}
        if v.lower() not in allowed:
            raise ValueError(f"ENVIRONMENT must be one of {allowed}")
        return v.lower()


# Global settings instance
settings = Settings()


def get_settings() -> Settings:
    """Get application settings."""
    return settings