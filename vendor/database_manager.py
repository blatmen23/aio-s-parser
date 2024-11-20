import os

from sqlalchemy import create_engine, MetaData, Connection, URL
from sqlalchemy import Table, Column, Integer, String, SmallInteger, Boolean, Date, JSON, Text, ForeignKey
from sqlalchemy import select, insert, update, delete, case, and_
from sqlalchemy.dialects import mysql
from vendor.data_parser import Institute, Group, Student
from datetime import date

from os import mkdir
import json
class DatabaseManager:
    reports_archive_table: Table
    students_archive_table: Table

    institutes_table: Table
    groups_table: Table
    students_table: Table

    old_institutes_table: Table
    old_groups_table: Table
    old_students_table: Table

    def __init__(self, db_echo: bool, driver: str, username: str, password: str, host: str, port: int, db_name: str, echo: bool):
        self.engine = create_engine(
            url=URL.create(drivername=driver,
                           username=username,
                           password=password,
                           host=host,
                           port=port,
                           database=db_name),
            echo=db_echo
        )
        self.metadata = MetaData()

    def create_tables(self):
        self.reports_archive_table = Table(
            "reports_archive",
            self.metadata,
            Column("report_id", Integer, primary_key=True, autoincrement=True),
            Column("report_content", Text),
            Column("report_json", JSON),
            Column("report_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.students_archive_table = Table(
            "students_archive",
            self.metadata,
            Column("record_id", Integer, primary_key=True, autoincrement=True),
            Column("institute", String(256)),
            Column("course", SmallInteger),
            Column("group_", String(16)),
            Column("student", String(256)),
            Column("record_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.institutes_table = Table(
            "institutes",
            self.metadata,
            Column("institute_id", Integer, primary_key=True),
            Column("institute", String(256)),
            Column("institute_num", SmallInteger),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.groups_table = Table(
            "groups_",
            self.metadata,
            Column("group_id", Integer, primary_key=True),
            Column("group_", String(16)),
            Column("course", SmallInteger),
            Column("institute", ForeignKey("institutes.institute_id")),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.students_table = Table(
            "students",
            self.metadata,
            Column("student_id", Integer, primary_key=True, autoincrement=True),
            Column("student", String(128)),
            Column("student_group", ForeignKey("groups_.group_id")),
            Column("leader", Boolean, default=False),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.old_institutes_table = Table(
            "old_institutes",
            self.metadata,
            Column("institute_id", Integer, primary_key=True),
            Column("institute", String(256)),
            Column("institute_num", SmallInteger),
            Column("parse_date", Date)
        )

        self.old_groups_table = Table(
            "old_groups_",
            self.metadata,
            Column("group_id", Integer, primary_key=True),
            Column("group_", String(16)),
            Column("course", SmallInteger),
            Column("institute", ForeignKey("old_institutes.institute_id")),
            Column("parse_date", Date)
        )

        self.old_students_table = Table(
            "old_students",
            self.metadata,
            Column("student_id", Integer, primary_key=True),
            Column("student", String(128)),
            Column("student_group", ForeignKey("old_groups_.group_id")),
            Column("leader", Boolean, default=False),
            Column("parse_date", Date)
        )

        self.metadata.create_all(self.engine)

    def prepare_tables(self):
        with self.engine.begin() as conn:  # type: Connection
            # clear old tables
            conn.execute(delete(self.old_students_table))
            conn.execute(delete(self.old_groups_table))
            conn.execute(delete(self.old_institutes_table))

            # take data from tables
            institutes_table = conn.execute(select(self.institutes_table)).mappings().all()
            student_groups_table = conn.execute(select(self.groups_table)).mappings().all()
            students_table = conn.execute(select(self.students_table)).mappings().all()

            # insert data in old tables
            if institutes_table:
                conn.execute(insert(self.old_institutes_table).values(
                    [{"institute_id": institute['institute_id'],
                      "institute": institute['institute'],
                      "institute_num": institute['institute_num'],
                      "parse_date": institute['parse_date']}
                     for institute in institutes_table]
                ).compile(self.engine, mysql.dialect()))
            if student_groups_table:
                conn.execute(insert(self.old_groups_table).values(
                    [{"group_id": student_group['group_id'],
                      "group_": student_group['group_'],
                      "course": student_group['course'],
                      "institute": student_group['institute'],
                      "parse_date": student_group['parse_date']}
                     for student_group in student_groups_table]
                ).compile(self.engine, mysql.dialect()))
            if students_table:
                conn.execute(insert(self.old_students_table).values(
                    [{"student_id": student['student_id'],
                      "student": student['student'],
                      "student_group": student['student_group'],
                      "leader": student['leader'],
                      "parse_date": student['parse_date']}
                     for student in students_table]
                ).compile(self.engine, mysql.dialect()))

            # clear tables
            conn.execute(delete(self.students_table))
            conn.execute(delete(self.groups_table))
            conn.execute(delete(self.institutes_table))

    def insert_data(self, institutes_data: list[Institute], groups_data: list[Group], students_data: list[Student]):
        with self.engine.begin() as conn:  # type: Connection
            institutes_data = [{"institute_id": institute.institute_num,
                                "institute": institute.institute,
                                "institute_num": institute.institute_num}
                               for institute in institutes_data]
            institutes_stmt = insert(self.institutes_table).values(institutes_data).compile(self.engine,
                                                                                            mysql.dialect())
            conn.execute(statement=institutes_stmt)

            groups_data = [{"group_id": group.group_id,
                            "group_": group.group,
                            "course": group.course,
                            "institute": group.institute.institute_num}
                           for group in groups_data]
            groups_stmt = insert(self.groups_table).values(groups_data).compile(self.engine, mysql.dialect())
            conn.execute(statement=groups_stmt)

            students_data = [{"student": student.student,
                              "student_group": student.group.group_id,
                              "leader": student.leader}
                             for student in students_data]
            students_stmt = insert(self.students_table).values(students_data).compile(self.engine, mysql.dialect())
            conn.execute(statement=students_stmt)

    def get_tables_difference(self):
        with self.engine.connect() as conn:
            new_groups = conn.execute(
                select(self.groups_table).select_from(
                    self.groups_table).join(
                    self.old_groups_table, isouter=True,
                    onclause=self.groups_table.c.group_id == self.old_groups_table.c.group_id).where(
                    self.old_groups_table.c.group_id == None).compile(self.engine, mysql.dialect())).mappings().all()
            print(f"-- new groups")

            deleted_groups = conn.execute(
                select(self.old_groups_table).select_from(
                    self.old_groups_table).join(
                    self.groups_table, isouter=True,
                    onclause=self.groups_table.c.group_id == self.old_groups_table.c.group_id).where(
                    self.groups_table.c.group_id == None).compile(self.engine, mysql.dialect())).mappings().all()
            print(f"-- deleted groups")

            group_changes = conn.execute(
                select(self.students_table.c.student_id,
                       self.students_table.c.student,
                       self.students_table.c.student_group.label("new_group_id"),
                       self.old_students_table.c.student_group.label("old_group_id"),
                       self.groups_table.c.group_.label("new_group_"),
                       self.old_groups_table.c.group_.label("old_group_")
                       ).select_from(self.students_table
                                     ).join(self.old_students_table, onclause=and_(
                    self.students_table.c.student == self.old_students_table.c.student,
                    self.students_table.c.student_group != self.old_students_table.c.student_group)
                                            ).join(self.groups_table,
                                                   onclause=self.students_table.c.student_group == self.groups_table.c.group_id
                                                   ).join(self.old_groups_table,
                                                          onclause=self.old_students_table.c.student_group == self.old_groups_table.c.group_id
                                                          ).compile(self.engine, mysql.dialect())).mappings().all()
            print(f"-- group changes")

            entered_students = conn.execute(
                select(self.students_table.c.student_id,
                       self.students_table.c.student,
                       self.students_table.c.student_group.label("group_id"),
                       self.groups_table.c.group_,
                       self.students_table.c.leader).select_from(self.students_table
                                                                 ).join(
                    self.old_students_table, isouter=True,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group
                                  )
                ).join(self.groups_table,
                       onclause=self.students_table.c.student_group == self.groups_table.c.group_id).where(
                    self.old_students_table.c.student_id == None).compile(self.engine,
                                                                          mysql.dialect())).mappings().all()
            print(f"-- entered students")

            left_students = conn.execute(
                select(self.old_students_table, self.old_groups_table, self.old_institutes_table).select_from(
                    self.old_students_table).join(
                    self.students_table, isouter=True,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group)
                ).join(self.old_groups_table,
                       onclause=self.old_groups_table.c.group_id == self.old_students_table.c.student_group
                       ).join(self.old_institutes_table,
                              onclause=self.old_groups_table.c.institute == self.old_institutes_table.c.institute_id
                              ).where(self.students_table.c.student_id == None
                                      ).compile(self.engine, mysql.dialect())).mappings().all()
            print(f"-- left students")

            leader_status = conn.execute(
                select(self.students_table, case(
                    (self.students_table.c.leader != 0, 'promotion'),
                    (self.students_table.c.leader == 0, 'demotion'),
                ).label("status"), self.groups_table).select_from(self.students_table).
                join(
                    self.old_students_table,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group,
                                  self.students_table.c.leader != self.old_students_table.c.leader)
                ).join(self.groups_table,
                       onclause=self.students_table.c.student_group == self.groups_table.c.group_id).compile(
                    self.engine, mysql.dialect())).mappings().all()
            print(f"-- leader status")

            tables_difference = {
                "new_groups": new_groups,
                "deleted_groups": deleted_groups,
                "group_changes": group_changes,
                "entered_students": entered_students,
                "left_students": left_students,
                "leader_status": leader_status
            }
            return tables_difference

    def archive_data(self, entered_students, left_students):
        with self.engine.begin() as conn:  # type: Connection
            if left_students:
                conn.execute(insert(self.students_archive_table).values(
                    [{"institute": student["institute"],
                      "course": student["course"],
                      "group_": student["group_"],
                      "student": student["student"]}
                     for student in left_students]
                ).compile(self.engine, mysql.dialect()))

            conn.execute(delete(self.students_archive_table).where(self.students_archive_table.c.student.in_([
                student["student"] for student in entered_students
            ])))

    def save_reports(self, report_json: dict, report_txt: str):
        with open("tmp.json", "w", encoding='utf-8') as file:
            json.dump(report_json, file, ensure_ascii=False)

        with open("tmp.json", "r", encoding='utf-8') as file:
            report_json_str = json.load(file)

        if os.path.exists("tmp.json"):
            os.remove("tmp.json")

        with self.engine.begin() as conn:  # type: Connection
            conn.execute(insert(self.reports_archive_table
                                ).values(report_content=report_txt, report_json=report_json_str,
                                         report_date=date.today().strftime("%Y-%m-%d")).compile(self.engine, mysql.dialect()))