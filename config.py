from os import getenv
from dotenv import find_dotenv, load_dotenv

from dataclasses import dataclass

load_dotenv(find_dotenv())

@dataclass
class TgBot:
    bot_token: str
    admin_id: int


@dataclass
class Database:
    host: str
    port: int
    user: str
    password: str
    name: str

@dataclass
class Config:
    tg_bot: TgBot
    database: Database


def load_config() -> Config:
    return Config(
        tg_bot=TgBot(
            bot_token=getenv('BOT_TOKEN'),
            admin_id=int(getenv('TG_CHAT_ADMIN'))
        ),
        database=Database(
            host=getenv('DB_HOST'),
            port=int(getenv('DB_PORT')),
            user=getenv('DB_USER'),
            password=getenv('DB_PASSWORD'),
            name=getenv('DB_NAME')
        )
    )
