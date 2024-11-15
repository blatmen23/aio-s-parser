import asyncio
import time
import data_parser
from config import load_config

config = load_config()

async def main():
    scrapper = data_parser.DataScrapper(
        connection_timeout=config.parser.connection_timeout,
        max_pool_size=config.parser.max_pool_size,
        time_delta=config.parser.time_delta
    )
    start_time = time.time()
    students = await scrapper.parse_data()
    print(f"Total time: {time.time() - start_time}")

if __name__ == "__main__":
    asyncio.run(main())