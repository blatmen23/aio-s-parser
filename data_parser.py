import asyncio
import fake_useragent
import aiohttp
import json
from bs4 import BeautifulSoup
from dataclasses import dataclass

@dataclass
class Institute:
    institute: str
    institute_num: int

@dataclass
class Group:
    institute: Institute
    course: str
    group: str
    group_id: str

@dataclass
class Student:
    group: Group
    student: str
    leader: bool

    def __str__(self):
        return f'Группа: {self.group.group} Имя: {self.student} Староста: {self.leader}'


class DataScrapper:
    kai_institutes = {
        1: "Институт авиации, наземного транспорта и энергетики",  # отделение СПО ТК 8
        2: "Факультет физико-математический",
        3: "Институт автоматики и электронного приборостроения",
        4: "Институт компьютерных технологий и защиты информации",  # отделение СПО КИТ 4
        5: "Институт радиоэлектроники, фотоники и цифровых технологий",
        6: "Институт инженерной экономики и предпринимательства"
    }
    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Cache-Control:': 'max-age=0'
    }
    students: list[Student] = []
    groups: list[Group] = []
    institutes: list[Institute] = [Institute(institute=value, institute_num=key) for key, value in kai_institutes.items()]

    exception_groups = []

    def __init__(self, connection_timeout, max_pool_size, time_delta):
        self.connection_timeout = connection_timeout
        self.max_pool_size = max_pool_size
        self.time_delta = time_delta

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

        try:
            response = await session.get(url=url, headers=self.headers, params=params)
        except asyncio.TimeoutError:
            print("TimeoutError in get_groups query")
            raise "Exit from aio-s-parser"

        groups_data_str = await response.text()
        groups_data = json.loads(groups_data_str)  # [0:5:1]

        for group_data in groups_data:
            institute_num = "1" if group_data['group'].startswith('8') else group_data['group'][0]
            institute_num = int(institute_num)
            self.groups.append(
                Group(
                    institute=Institute(
                        institute=self.kai_institutes[institute_num],
                        institute_num=institute_num
                    ),
                    course=group_data['group'][1],
                    group=group_data['group'],
                    group_id=group_data['id']
                )
            )

    def _get_group_chunks(self, groups):
        group_chunks = []
        group_chunk = []
        for group in groups:
            group_chunk.append(group)
            if (len(group_chunk) == self.max_pool_size) or (groups[-1] == group):
                group_chunks.append(tuple(group_chunk))
                group_chunk = []
        return group_chunks

    async def _get_students(self, session, group) -> Student:
        url = "https://kai.ru/infoClick/-/info/group"
        params = {
            "id": group.group_id,
            "name": group.group
        }
        headers = self.headers
        user_agent = fake_useragent.UserAgent()
        headers['User-Agent'] = user_agent.random

        try:
            response = await session.get(url=url, headers=headers, params=params)
            group_page = await response.text()
            soup = BeautifulSoup(group_page, "html.parser")

            table = soup.find("tbody")
            if table is None:
                alert = soup.find("div", class_="alert alert-info")
                if alert is None:
                    print("table tag is NoneType, why?", group.group)
                    self.exception_groups.append(group)
                    # await self._student_parsing(session, )
                    return
                else:
                    print(group.group, "is", alert.text)
                    return

            trows = table.find_all("tr")
            for trow in trows:
                tcolumns = trow.find_all("td")
                student_column = tcolumns[1]
                leader = True if student_column.find("span") is not None else False
                student = student_column.find(text=True, recursive=False).get_text(strip=True)

                self.students.append(
                    Student(
                        group=group,
                        student=student,
                        leader=leader
                    )
                )
        except asyncio.TimeoutError:
            self.exception_groups.append(group)
            print(f"TimeoutError in group: id({group.group_id}) group({group.group})")
            print("TimeoutError count:", len(self.exception_groups))
            await asyncio.sleep(3)
            print("3 seconds sleeping because TimeoutError")
            print()

    async def _student_parsing(self, session, groups):
        self.exception_groups = []

        group_chunks = self._get_group_chunks(groups)
        tasks = []
        for chunk in group_chunks:
            for group in chunk:
                task = self._get_students(session, group)
                tasks.append(asyncio.create_task(task))
            print("Starting group_chunks: ", [group.group for group in chunk])
            await asyncio.gather(*tasks)
            print("Current students: ", len(self.students))
            await asyncio.sleep(self.time_delta)
            print()

        if self.exception_groups:
            print("Recursion parsing:")
            await asyncio.sleep(3)
            await self._student_parsing(session, self.exception_groups)

    async def parse_data(self):
        """
        :return: (institutes[Institute], groups[Group], students[Student])
        """
        async with aiohttp.ClientSession(trust_env=True, timeout=aiohttp.ClientTimeout(total=self.connection_timeout)) as session:
            print("Start students parsing")
            await self._get_groups(session)
            await self._student_parsing(session, self.groups)
            print("End students parsing")
            return self.institutes, self.groups, self.students