from sqlalchemy import create_engine, MetaData, Connection, text
from sqlalchemy import Table, Column, Integer, String, SmallInteger, Boolean, Date, ForeignKey, func, cast
from sqlalchemy import select, insert, update, delete
from sqlalchemy.dialects import mysql
from data_parser import Institute, Group, Student
from datetime import date
class DatabaseManager:
    institutes_table: Table
    student_groups_table: Table
    students_table: Table

    def __init__(self, url: str, echo: bool):
        self.engine = create_engine(
            url=url,
            echo=echo
        )
        self.metadata = MetaData()

    def create_tables(self):
        self.institutes_table = Table(
            "institutes",
            self.metadata,
            Column("institute_id", Integer, primary_key=True),
            Column("institute", String(256)),
            Column("institute_num", SmallInteger),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.student_groups_table = Table(
            "groups_",
            self.metadata,
            Column("group_id", Integer, primary_key=True),
            Column("group_", String(32)),
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
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.old_student_groups_table = Table(
            "old_groups_",
            self.metadata,
            Column("group_id", Integer, primary_key=True),
            Column("group_", String(32)),
            Column("course", SmallInteger),
            Column("institute", ForeignKey("institutes.institute_id")),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.old_students_table = Table(
            "old_students",
            self.metadata,
            Column("student_id", Integer, primary_key=True, autoincrement=True),
            Column("student", String(128)),
            Column("student_group", ForeignKey("groups_.group_id")),
            Column("leader", Boolean, default=False),
            Column("parse_date", Date, default=date.today().strftime("%Y-%m-%d"))
        )

        self.metadata.create_all(self.engine)

    def prepare_tables(self):
        ...

    def insert_data(self, institutes_data: list[Institute], groups_data: list[Group], students_data: list[Student]):
        # with self.engine.connect() as conn:

        with self.engine.begin() as conn:  # type: Connection
            institutes_data = [{"institute_id": institute.institute_num,
                                "institute": institute.institute,
                                "institute_num": institute.institute_num}
                               for institute in institutes_data]
            institutes_stmt = insert(self.institutes_table).values(institutes_data).compile(self.engine, mysql.dialect())
            conn.execute(statement=institutes_stmt)

            groups_data = [{"group_id": group.group_id,
                            "group_": group.group,
                            "course": group.course,
                            "institute": group.institute.institute_num}
                           for group in groups_data]
            groups_stmt = insert(self.student_groups_table).values(groups_data).compile(self.engine, mysql.dialect())
            conn.execute(statement=groups_stmt)

            students_data = [{"student": student.student,
                              "student_group": student.group.group_id,
                              "leader": student.leader}
                             for student in students_data]
            students_stmt = insert(self.students_table).values(students_data).compile(self.engine, mysql.dialect())
            conn.execute(statement=students_stmt)