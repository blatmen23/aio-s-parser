from sqlalchemy import DateTime, Date, Float, String, Text, SmallInteger, func, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

class Base(DeclarativeBase):
    created: Mapped[DateTime] = mapped_column(DateTime, default=func.now())
    updated: Mapped[DateTime] = mapped_column(DateTime, default=func.now(), onupdate=func.now())

class Student(Base):
    __tablename__ = "students"
    student_id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)
    student: Mapped[str] = mapped_column()
    student_group: Mapped[int] = mapped_column(ForeignKey("student_groups.group_id"))
    leader: Mapped[bool] = mapped_column()
    parse_data: Mapped[Date] = mapped_column(Date, default=func.now)

class StudentGroup(Base):
    __tablename__ = "student_groups"
    group_id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)
    group: Mapped[str] = mapped_column()
    course: Mapped[int] = mapped_column(SmallInteger)
    institute: Mapped[int] = mapped_column(SmallInteger, ForeignKey("institutes.institute_id"))
    parse_data: Mapped[Date] = mapped_column(Date, default=func.now)

class Institute(Base):
    __tablename__ = "institute"
    institute_id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)
    institute: Mapped[str] = mapped_column()
    institute_num: Mapped[int] = mapped_column()
    parse_data: Mapped[Date] = mapped_column(Date, default=func.now)