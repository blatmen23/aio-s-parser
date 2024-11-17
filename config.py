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
    database: str

@dataclass
class Parser:
    connection_timeout: float
    max_pool_size: int
    time_delta: float

@dataclass
class Config:
    tg_bot: TgBot
    database: Database
    parser: Parser


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
            database=getenv('DB_NAME')
        ),
        parser=Parser(
            connection_timeout=float(getenv('CONNECTION_TIMEOUT')),
            max_pool_size=int(getenv('MAX_POOL_SIZE')),
            time_delta=float(getenv('TIME_DELTA'))
        )
    )
