# import asyncio
#
# import aiohttp
# import fake_useragent
# from bs4 import BeautifulSoup
#
# url = "https://request.urih.com/"
# params = {}
# user_agent = fake_useragent.UserAgent()
#
# async def main():
#     async with aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=5)) as session:
#         headers = {
#             'User-Agent': user_agent.random,
#             'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
#             'Accept-Encoding': 'gzip, deflate, br'
#         }
#         response = await session.get(url=url, headers=headers, params=params)
#         group_page = await response.text()
#         soup = BeautifulSoup(group_page, "html.parser")
#         print(soup.find("table"))
#
#         headers = {
#             'User-Agent': user_agent.random,
#             'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
#             'Accept-Encoding': 'gzip, deflate, br'
#         }
#         response = await session.get(url=url, headers=headers, params=params)
#         group_page = await response.text()
#         soup = BeautifulSoup(group_page, "html.parser")
#         print(soup.find("table"))
#
#         # table = soup.find("tbody")
#         # trows = table.find_all("tr")
#         # print(table)
#
#         # alert = soup.find("div", class_="alert alert-info")
#         # print(alert.text)
#
# asyncio.run(main())

import datetime

print(type(str(datetime.date.today())))
print(type(datetime.date.today().ctime()))
print(type(datetime.date.today().strftime("%Y-%m-%d")))
print(datetime.date.today().strftime("%Y-%m-%d"))
