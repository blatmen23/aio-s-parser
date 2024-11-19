import datetime
import json
import aiohttp
import aiofiles

class Rapporteur:
    def __init__(self, connection_timeout, tg_bot_token):
        self.connection_timeout = connection_timeout
        self.tg_bot_token = tg_bot_token

    async def _save_reports(self, report_json, report_txt):
        async with aiofiles.open(f"reports/json/report_{datetime.date.today()}.json", "w", encoding="utf-8") as file:
            await file.write(report_json)

        async with aiofiles.open(f"reports/txt/report_{datetime.date.today()}.txt", "w", encoding="utf-8") as file:
            await file.write(report_txt)

        return f"reports/json/report_{datetime.date.today()}.json", f"reports/txt/report_{datetime.date.today()}.txt"

    async def send_reports(self, chat_id: str, report_json: str, report_txt: str, caption: str):
        report_json_path, report_txt_path = await self._save_reports(report_json, report_txt)
        async with aiohttp.ClientSession(trust_env=True,
                                         timeout=aiohttp.ClientTimeout(total=self.connection_timeout)) as session:
            with open(report_txt_path, "rb") as report:
                data = {'chat_id': chat_id,
                        'document': report,
                        'caption': caption,
                        'parse_mode': 'html',
                        'disable_notification': 'true'}
                url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendDocument'
                response = await session.post(url=url, data=data)

                if response.ok:
                    print("Отчёт отправлен в телеграм")
    # except Exception as ex:
    #     print(f"Не удалось отправить {file_path}: {ex}")

    # def send_error_message(self, chat_id, text):
    #     try:
    #         params = {'chat_id': chat_id,
    #                   'text': text,
    #                   'parse_mode': 'html',
    #                   'disable_notification': True}
    #         url = f'https://api.telegram.org/bot{self.tg_bot_token}/sendMessage'
    #         response = requests.post(url, data, timeout=10)
    #         if response:
    #             print(f"Сообщение об ошибке отправлено в телеграм")
    #     except Exception as ex:
    #         print(f"Не удалось отправить сообщение об ошибке: {ex}")
