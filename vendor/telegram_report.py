import os
import json
import datetime
import asyncio
import aiohttp
import aiofiles

class Rapporteur:
    def __init__(self, connection_timeout, tg_bot_token, chat_id):
        self.connection_timeout = connection_timeout
        self.tg_bot_token = tg_bot_token
        self.chat_id = chat_id

    async def _write_file(self, file_path, data):
        async with aiofiles.open(file_path, "w", encoding="utf-8") as file:
            if file_path.endswith(".json"):
                await file.write(json.dumps(data, ensure_ascii=False, indent=4))
            else:
                await file.write(data)

    async def _save_reports(self, report_json, report_txt):
        os.makedirs('reports/json', exist_ok=True)
        os.makedirs('reports/txt', exist_ok=True)

        json_file_name = f"{datetime.date.today()}.json"
        txt_file_name = f"{datetime.date.today()}.txt"

        json_file_path = f"reports/json/{json_file_name}"
        txt_file_path = f"reports/txt/{txt_file_name}"

        await asyncio.gather(
            asyncio.create_task(self._write_file(json_file_path, report_json)),
            asyncio.create_task(self._write_file(txt_file_path, report_txt))
        )
        return json_file_path, txt_file_path

    async def send_reports(self, report_json: dict, report_txt: str):
        report_json_path, report_txt_path = await self._save_reports(report_json, report_txt)
        async with aiohttp.ClientSession(trust_env=True,
                                         timeout=aiohttp.ClientTimeout(total=self.connection_timeout)) as session:
            with open(report_txt_path, "rb") as file_txt:
                data_txt = {'chat_id': self.chat_id,
                            'document': file_txt,
                            'disable_notification': 'true'}
                url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendDocument'
                response_report_txt = await session.post(url=url, data=data_txt)

                if response_report_txt.ok:
                    print(".txt отправлен в телеграм")

            with open(report_json_path, "rb") as file_json:
                data_json = {'chat_id': self.chat_id,
                             'document': file_json,
                             'disable_notification': 'true'}
                url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendDocument'
                response_report_json = await session.post(url=url, data=data_json)

            if response_report_json.ok:
                print(".json отправлен в телеграм")

    async def send_info_message(self):
        try:
            async with aiohttp.ClientSession(trust_env=True,
                                             timeout=aiohttp.ClientTimeout(total=self.connection_timeout)) as session:
                data = {'chat_id': self.chat_id,
                        'text': '<b>🚀 Запускаю сбор и анализ данных</b>',
                        'parse_mode': 'html',
                        'disable_notification': 'true'}
                url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendMessage'
                await session.get(url=url, data=data)
        except asyncio.TimeoutError:
            print("TimeoutError in send_info_message")
        except Exception as ex:
            print(f"Не удалось отправить информационное сообщение: {ex}")

    async def send_error_message(self):
        try:
            async with aiohttp.ClientSession(trust_env=True,
                                             timeout=aiohttp.ClientTimeout(total=self.connection_timeout)) as session:
                data = {'chat_id': self.chat_id,
                        'text': '<b>⚠️ Возникла непредвиденная ошибка, отчёта сегодня не будет 😔</b>',
                        'parse_mode': 'html',
                        'disable_notification': 'true'}
                url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendMessage'
                await session.get(url=url, data=data)
        except asyncio.TimeoutError:
            print("TimeoutError in send_error_message")
        except Exception as ex:
            print(f"Не удалось отправить сообщение об ошибке: {ex}")
