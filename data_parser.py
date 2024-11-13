import asyncio

import aiohttp
import json
from bs4 import BeautifulSoup
from dataclasses import dataclass

@dataclass
class Group:
    institute: str
    institute_num: int
    course: str
    group_id: int
    group: str

@dataclass
class Student:
    group: Group
    student: str
    leader: bool

    def __str__(self):
        return f'Группа: {self.group.group} Имя: {self.student} Староста: {self.leader}'


class DataScrapper:
    institutes = {
        1: "Институт авиации, наземного транспорта и энергетики", # отделение СПО ТК 8
        2: "Факультет физико-математический",
        3: "Институт автоматики и электронного приборостроения",
        4: "Институт компьютерных технологий и защиты информации",  # отделение СПО КИТ 4
        5: "Институт радиоэлектроники, фотоники и цифровых технологий",
        6: "Институт инженерной экономики и предпринимательства"
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br'
    }

    def __init__(self):
        ...

    async def _get_groups(self, session) -> Group:
        url = "https://kai.ru/raspisanie"
        params = {
            "p_p_id": "pubStudentSchedule_WAR_publicStudentSchedule10",
            "p_p_lifecycle": 2,
            "p_p_state": "normal",
            "p_p_mode": "view",
            "p_p_resource_id": "getGroupsURL",
            "p_p_cacheability": "cacheLevelPage",
            "p_p_col_id": "column-1",
            "p_p_col_count": 1
        }

        async with session.get(url=url, headers=self.headers, timeout=7, params=params) as response:  # type: aiohttp.ClientResponse
            groups_data_str = await response.text()
            groups_data = json.loads(groups_data_str)

            groups = []
            for group_data in groups_data:
                institute_num = 1 if group_data['group'].startswith('8') else group_data['group'][0]
                institute_num = int(institute_num)
                groups.append(
                    Group(
                        institute=self.institutes[institute_num],
                        institute_num=institute_num,
                        course=group_data['group'][1],
                        group_id=group_data['id'],
                        group=group_data['group']
                    )
                )
            return groups

    async def _get_students(self, session, group) -> Student:
        url = f"https://kai.ru/infoClick/-/info/group"
        params = {
            "id": group.group_id,
            "name": group.group
        }

        async with session.get(url=url, headers=self.headers, timeout=7, params=params) as response:  # type: aiohttp.ClientResponse
            # response.encoding = 'utf-8'
            group_page = await response.text()
            soup = BeautifulSoup(group_page, "lxml")
            table = soup.find("tbody")
            trows = table.find_all("tr")

            students = []
            for trow in trows:
                tcolumns = trow.find_all("td")
                student_column = tcolumns[1]
                leader = True if student_column.find("span") is not None else False
                student = student_column.find(text=True, recursive=False).get_text(strip=True)

                students.append(
                    Student(
                        group=group,
                        student=student,
                        leader=leader
                    )
                )
            for student in students:
                print(student.__str__())
            return students

    async def parse_data(self):
        async with aiohttp.ClientSession(trust_env=True) as session:
            groups = await self._get_groups(session)

            tasks = []
            for group in groups:
                tasks.append(asyncio.create_task(self._get_students(session, group)))
                await asyncio.sleep(0.01)
                if len(tasks) >= 10:
                    break
            print("start completing tasks")
            await asyncio.gather(*tasks)
