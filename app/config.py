from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "postgresql+asyncpg://payments:payments@postgres:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@rabbitmq:5672/"
    api_key: str = "dev-api-key-change-me"
    outbox_poll_seconds: float = 1.0
    webhook_timeout_seconds: float = 5.0

settings = Settings()
