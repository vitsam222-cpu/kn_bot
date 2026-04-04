import os
from dataclasses import dataclass

from dotenv import load_dotenv


load_dotenv()


@dataclass(frozen=True)
class Settings:
    bot_token: str = os.getenv("BOT_TOKEN", "")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "admin")
    database_path: str = os.getenv("DATABASE_PATH", "bot.db")
    admin_host: str = os.getenv("ADMIN_HOST", "0.0.0.0")
    admin_port: int = int(os.getenv("ADMIN_PORT", "8000"))
    secret_key: str = os.getenv("SECRET_KEY", "change-me")


settings = Settings()
