import asyncio
import time
import data_parser
import database
from config import load_config
config = load_config()

def main():
    start_time = time.time()
    data_scrapper = data_parser.DataScrapper(
        connection_timeout=config.parser.connection_timeout,
        max_pool_size=config.parser.max_pool_size,
        time_delta=config.parser.time_delta
    )
    data = asyncio.run(data_scrapper.parse_data())
    print(f"Total time: {time.time() - start_time}")

    db_manager = database.DatabaseManager(
        # mysql+pymysql://user:password@host:port/dbname[?key=value&key=value...]
        url=f"mysql+pymysql://{config.database.user}:{config.database.password}@{config.database.host}:{config.database.port}/{config.database.database}",
        echo=True
    )
    db_manager.create_tables()
    db_manager.insert_data(institutes_data=data[0],
                           groups_data=data[1],
                           students_data=data[2])


if __name__ == "__main__":
    main()