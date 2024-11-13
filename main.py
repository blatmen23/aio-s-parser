import asyncio

import data_parser
from config import load_config
config = load_config()

async def main():
    scrapper = data_parser.DataScrapper()
    await scrapper.parse_data()

if __name__ == "__main__":
    asyncio.run(main())