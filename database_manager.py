from sqlalchemy import create_engine, MetaData, Connection, URL
from sqlalchemy import Table, Column, Integer, String, SmallInteger, Boolean, Date, ForeignKey
from sqlalchemy import select, insert, update, delete, case, and_
from sqlalchemy.dialects import mysql
from data_parser import Institute, Group, Student
from datetime import date


class DatabaseManager:
    institutes_table: Table
    student_groups_table: Table
    students_table: Table

    old_institutes_table: Table
    old_student_groups_table: Table
    old_students_table: Table

    def __init__(self, driver: str, username: str, password: str, host: str, port: int, db_name: str, echo: bool):
        self.engine = create_engine(
            url=URL.create(drivername=driver,
                           username=username,
                           password=password,
                           host=host,
                           port=port,
                           database=db_name),
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
            Column("parse_date", Date)
        )

        self.old_student_groups_table = Table(
            "old_groups_",
            self.metadata,
            Column("group_id", Integer, primary_key=True),
            Column("group_", String(32)),
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
            conn.execute(delete(self.old_student_groups_table))
            conn.execute(delete(self.old_institutes_table))

            # take data from tables
            institutes_table = conn.execute(select(self.institutes_table)).mappings().all()
            student_groups_table = conn.execute(select(self.student_groups_table)).mappings().all()
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
                conn.execute(insert(self.old_student_groups_table).values(
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
            conn.execute(delete(self.student_groups_table))
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
            groups_stmt = insert(self.student_groups_table).values(groups_data).compile(self.engine, mysql.dialect())
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
                select(self.student_groups_table, self.old_student_groups_table).select_from(
                    self.student_groups_table).join(
                    self.old_student_groups_table, isouter=True,
                    onclause=self.student_groups_table.c.group_id == self.old_student_groups_table.c.group_id).where(
                    self.old_student_groups_table.c.group_id == None).compile(self.engine, mysql.dialect()))
            print(f"-- new groups{new_groups.fetchall()}\n\n")

            deleted_groups = conn.execute(
                select(self.student_groups_table, self.old_student_groups_table).select_from(
                    self.old_student_groups_table).join(
                    self.student_groups_table, isouter=True,
                    onclause=self.student_groups_table.c.group_id == self.old_student_groups_table.c.group_id).where(
                    self.student_groups_table.c.group_id == None).compile(self.engine, mysql.dialect()))
            print(f"-- deleted groups{deleted_groups.fetchall()}\n\n")

            #             SELECT Students.student_id,
            #                 Students.student_name,
            #                 Students.student_group AS 'new_group_id',
            #                 Students_tmp.student_group AS 'last_group_id',
            #                 StudentGroups.group_name AS 'new_group_name',
            #                 StudentGroups_tmp.group_name AS 'last_group_name'
            #             FROM Students
            #                 JOIN Students_tmp ON Students.student_id = Students_tmp.student_id
            #                     AND Students.student_group <> Students_tmp.student_group
            #                 JOIN StudentGroups ON Students.student_group = StudentGroups.group_id
            #                 JOIN StudentGroups_tmp ON Students_tmp.student_group = StudentGroups_tmp.group_id;
            group_changes = conn.execute(select(self.students_table, self.))
            print(f"-- group changes {group_changes}")

            entered_students = conn.execute(
                select(self.students_table, self.old_students_table).select_from(self.students_table).join(
                    self.old_students_table, isouter=True,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group)
                ).where(self.old_students_table.c.student_id == None).compile(self.engine, mysql.dialect()))
            print(f"-- entered students{entered_students.fetchall()}\n\n")

            left_students = conn.execute(
                select(self.students_table, self.old_students_table).select_from(self.old_students_table).join(
                    self.students_table, isouter=True,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group)
                ).where(self.students_table.c.student_id == None).compile(self.engine, mysql.dialect()))
            print(f"-- left students {left_students.fetchall()}\n\n")

            leader_status = conn.execute(
                select(self.students_table, case(
                    (self.students_table.c.leader != 0, 'promotion'),
                    (self.students_table.c.leader == 0, 'demotion'),
                ).label("status"), self.student_groups_table).select_from(self.students_table).
                join(
                    self.old_students_table,
                    onclause=and_(self.students_table.c.student == self.old_students_table.c.student,
                                  self.students_table.c.student_group == self.old_students_table.c.student_group,
                                  self.students_table.c.leader != self.old_students_table.c.leader)
                ).join(self.student_groups_table,
                       onclause=self.students_table.c.student_group == self.student_groups_table.c.group_id).compile(
                    self.engine, mysql.dialect())
            )
            print(f"-- leader status {leader_status.fetchall()}\n\n")
