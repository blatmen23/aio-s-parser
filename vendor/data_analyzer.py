import datetime


class DataAnalyzer(object):
    def __init__(self):
        ...

    def _get_json_report(self, differences: dict, elapsed_time: float, total_groups: int, total_students: int):
        report_content = dict(differences)
        report_content["today"] = str(datetime.date.today())
        report_content["time_difference"] = datetime.datetime.utcfromtimestamp(elapsed_time).strftime("%H:%M:%S")
        report_content["total_groups"] = total_groups
        report_content["total_new_groups"] = len(differences["new_groups"])
        report_content["total_deleted_groups"] = len(differences["deleted_groups"])
        report_content["total_students"] = total_students
        report_content["total_new_students"] = len(differences["entered_students"])
        report_content["total_deleted_students"] = len(differences["left_students"])
        report_content["total_leader_status"] = len(differences["leader_status"])
        report_content["total_group_changes"] = len(differences["group_changes"])

        print("The .json report is ready")
        return report_content

    def _get_txt_report(self, differences: dict, elapsed_time: float, total_groups: int, total_students: int):
        all_new_groups = "\n".join([str(group["group_"]) for group in differences["new_groups"]])
        all_deleted_groups = "\n".join([str(group["group_"]) for group in differences["deleted_groups"]])

        all_entered_students = "\n".join(
            [str(student["student"]) + " - " + str(student["group_"]) for student in
             differences["entered_students"]])
        all_left_students = "\n".join([str(student["student"]) + " - " + str(student["group_"]) for student in
                                       differences["left_students"]])

        all_leaders_status_changes = "\n".join([str(
            student["student"] + " - " + str(student["group_"]) + " - " + (
                "повышение" if str(student["status"]) == "promotion" else (
                    "понижение" if str(student["status"]) == "demotion" else ""))) for student in
                                                differences["leader_status"]])
        all_group_changes = "\n".join([str(
            student["student"] + " - " + str(student["old_group_"])) + " -> " + str(student["new_group_"])
                                       for student in differences["group_changes"]])

        report_content = (f'База студентов обновлена: {datetime.date.today()}\n'
                          f'Затраченное время: {datetime.datetime.utcfromtimestamp(elapsed_time).strftime("%H:%M:%S")}\n'
                          f'Найдено групп: {total_groups}\n'
                          f'Новые группы: {len(differences["new_groups"])}\n'
                          f'{all_new_groups}\n\n'
                          f'Не найденные группы: {len(differences["deleted_groups"])}\n'
                          f'{all_deleted_groups}\n\n'
                          f'Найдено студентов: {total_students}\n'
                          f'Новые студенты: {len(differences["entered_students"])}\n'
                          f'{all_entered_students}\n\n'
                          f'Не найденные студенты: {len(differences["left_students"])}\n'
                          f'{all_left_students}\n\n'
                          f'Изменения статусов старост: {len(differences["leader_status"])}\n'
                          f'{all_leaders_status_changes}\n\n'
                          f'Студенты изменившие группу: {len(differences["group_changes"])}\n'
                          f'{all_group_changes}\n\n')
        print("The .txt report is ready")
        return report_content

    def get_reports(self, tables_difference: dict, elapsed_time: float, total_groups: int, total_students: int):
        return (self._get_json_report(differences=tables_difference,
                                      elapsed_time=elapsed_time,
                                      total_groups=total_groups,
                                      total_students=total_students),
                self._get_txt_report(differences=tables_difference,
                                     elapsed_time=elapsed_time,
                                     total_groups=total_groups,
                                     total_students=total_students))