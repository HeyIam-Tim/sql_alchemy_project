from sqlalchemy import create_engine, ForeignKey, select, MetaData, Table, Integer, BigInteger, String, insert, Column, \
    Connection, and_, or_, desc, update, bindparam, delete, inspect
from sqlalchemy.dialects import postgresql, sqlite
from sqlalchemy.orm import as_declarative, mapped_column, Mapped, Session, DeclarativeBase, relationship

# engine = create_engine('sqlite+pysqlite:///:memory:', echo=True)


engine = create_engine('postgresql://postgres_test:postgres_test@0.0.0.0:5434/postgres_test', echo=True)

# metadata = MetaData()
#
# user = Table(
#     'user',
#     metadata,
#     Column('id', Integer, primary_key=True, autoincrement=True, unique=True),
#     # Column('user_id', BigInteger, unique=True),
#     Column('name', String(30)),
#     Column('surname', String)
# )
#
# address = Table(
#     'address',
#     metadata,
#     Column('id', Integer, primary_key=True),
#     # Column('user_id', ForeignKey('user.user_id'), unique=True),
#     # Column('user_id', ForeignKey('user.id'), unique=True),
#     Column('user_id', Integer, unique=True),
#     Column('location', String, nullable=False),
# )
#
# metadata.drop_all(engine)
# metadata.create_all(engine)

# stmt = insert(user).values(name='John', surname='Black', user_id='13412408519274129384')
# stmt_wo_values = insert(user)
#
# stmt_postgres = select(user.c.id, user.c.name).where(
#     # or_(user.c.name.startswith('J'),
#     #     user.c.surname.contains('ee'), )
#     user.c.id.in_((1, 2))
# )
# print('POSTGRES: ', stmt_postgres.compile(engine, postgresql.dialect()))

# print(stmt.compile(engine, sqlite.dialect()))
# print(stmt.compile(engine, postgresql.dialect()))
#

# with engine.begin() as connection:  # type: Connection
#     connection.execute(
#         insert(user),
#         parameters=[
#             {
#                 'name': 'Jack',
#                 'surname': 'White',
#                 # 'user_id': 3412345245236456
#             }, {
#                 'name': 'Rachel',
#                 'surname': 'Green',
#                 # 'user_id': 3412345245236455
#             }, {
#                 'name': 'Jhandler',
#                 'surname': 'Bing',
#                 # 'user_id': 3412345245236454
#             },
#         ]
#     )
#
#     connection.execute(
#         insert(address),
#         parameters=[
#             {'user_id': 2, 'location': 'Orenburg'},
#             {'user_id': 3, 'location': 'Kazan'},
#         ]
#     )

# with engine.begin() as connection:
# result = connection.execute(
#     select(user.c.id, user.c.name).where(
#         # or_(user.c.name.startswith('J'),
#         #     user.c.surname.contains('ee'), )
#         user.c.id.in_((1, 2))
#     )
# )

# result = connection.execute(
#     select(
#         user.c.id,
#         (user.c.name + ' ' + user.c.surname).label('fullname'),
#     ).where(
#         user.c.id.in_((1, 2))
#     )
# )
# for res in result:
#     print(res.id, res, type(res), res.fullname)
# print('RESULT: ', result.all())
# print('RESULT: ', result.fetchall())
# print('RESULT: ', result.mappings().all())

# result = connection.execute(
#     select(
#         address.c.location.label('address'),
#         user.c.id,
#         (user.c.name + ' ' + user.c.surname).label('fullname'),
#     ).where(
#         user.c.id > 1,
#     # ).join_from(user, address, onclause=user.c.id == address.c.user_id)
#     ).join(address, isouter=True)
# )

# result = connection.execute(
#     select(
#         user.c.id,
#         (user.c.name + ' ' + user.c.surname).label('fullname'),
#         address.c.location.label('address'),
#     ).join(address, isouter=True)
#     .order_by(
#         # desc(user.c.name),
#         # user.c.name.desc(),
#         desc('address'),
#     )
# )
#
# print('ORDERED: ', result.mappings().all())

class Base(DeclarativeBase):
    pass


class AbstractModel:
    id: Mapped[int] = mapped_column(autoincrement=True, primary_key=True)


class User(AbstractModel, Base):
    __tablename__ = 'user'

    # addresses: Mapped[list['Address']] = relationship(back_populates='user', uselist=True, secondary='user_address')
    addresses: Mapped[list['Address']] = relationship(back_populates='user', uselist=True, lazy='selectin')

    name: Mapped[str] = mapped_column()
    surname: Mapped[str]
    age: Mapped[int]
    height: Mapped[int]

    def __repr__(self):
        return f'User:{self.id=}:{self.name=}:{self.age=}'


class Address(AbstractModel, Base):
    __tablename__ = 'address'

    user: Mapped['User'] = relationship(back_populates='addresses', uselist=False)

    user_fk: Mapped[int] = mapped_column(ForeignKey('user.id'))
    location: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self):
        return f'Address:{self.id=}:{self.location=}:{self.user_fk=}'


# class UserAddress(AbstractModel, Base):
#     __tablename__ = 'user_address'
#
#     user_fk: Mapped[int] = mapped_column(ForeignKey('user.id'))
#     address_fk: Mapped[int] = mapped_column(ForeignKey('address.id'))
#
#     def __repr__(self):
#         return f'UserAddress:{self.id=}:{self.user_fk=}:{self.address_fk=}'


# Base.metadata.drop_all(engine)
#
# session = Session(engine, expire_on_commit=True)
# Base.metadata.create_all(engine)
#
# peter = User(name='Peter', surname='Parker', age=25)
# kate = User(name='Kate', surname='Dou', age=22)
#
# address1 = Address(location='Yarovoy')
# address2 = Address(location='ORENBURG')
# address3 = Address(location='Svetly')
# address4 = Address(location='Moscow')
#
# peter.addresses.append(address1)
# peter.addresses.append(address2)
#
# kate.addresses.append(address3)
# kate.addresses.append(address4)
#
# session.add(peter)
# session.add(kate)
#
# session.commit()
# users = session.scalars(select(User)).all()
# addresses = session.execute(select(Address)).scalars().all()
# print('USERS: ', users)
# print('ADDRESSES: ', addresses)

# users = session.scalars(select(User))
# for user in users:
#     print('USER INFO: ', user, type(user), user.addresses)

# user_address = session.scalars(select(UserAddress)).all()
# print('USER: ', users)
# print('ADDRESSES: ', users.addresses)
# print('SECONDARY: ', user_address)

# Base.metadata.drop_all(engine)

# mark = User(name='Mark', surname='Rufalo', age=25)
# session.add(mark)
# insp = inspect(peter)
# session.add(peter)
# session.flush()
# mark.age = 26
# print(mark in session.dirty)
# session.flush()
#

# # session.expire_all()
# session.expunge_all()
# mark_session = session.get(User, 1)
# print(mark_session == mark)
# print(mark_session is mark)
#
# mark_session.age = 1
# print(mark.age)
#
# user_from_db = session.scalar(select(User).where(User.id == 1))
# print(user_from_db == user_from_db)
# print(user_from_db is user_from_db)
# user_from_db.age = 2
# session.commit()

# print('IS TRANSIENT: ', insp.transient)
# print('IS PENDING: ', insp.pending)
# print('IS PERSISTENT: ', insp.persistent)
#
# session.delete(peter)
# session.flush()
# print('IS DELETED: ', insp.deleted)
# session.commit()

# print('NEW: ', session.new)

#

#
#
# with Session(engine) as session:
#     with session.begin():
#         AbstractModel.metadata.create_all(engine)
#         user = UserModel(name='John', surname='Black')
#         session.add(user)
#         result = session.execute(select(UserModel).where(UserModel.id == 1))
#         print(result.scalar())


#
#
# print(user_table.c.keys(), type(user_table.c.keys()))
# print(user_table.c, type(user_table.c))
#
#
# metadata.create_all(engine)
# metadata.drop_all(engine)


# with engine.connect() as connection:
#     result = connection.execute(text('select "hello"'))
#     print('RESULT: ', result.scalar_one_or_none())
#     print('RESULT: ', result.scalar())
#     print('RESULT: ', result.scalars().all())


# with engine.begin() as connection:
# connection.execute(
#     update(user).where(user.c.id == 1).values(name='Mark')
# )
# result = connection.execute(
#     select(user).where(user.c.id == 1)
# )
# print(result.mappings().all())

# stmt = update(user).where(user.c.name == bindparam('oldname')).values(name=bindparam('newname'))
# connection.execute(
#     stmt,
#     parameters=[
#         {'oldname': 'Jack', 'newname': 'Ross'},
#         {'oldname': 'Rachel', 'newname': 'Monica'},
#         {'oldname': 'Jhandler', 'newname': 'Fibbe'},
#     ]
# )
#
# print(connection.execute(select(user)).mappings().all())

# connection.execute(delete(user).where(user.c.id == 1))
# print('RESULT: ', connection.execute(select(user)).mappings().all())
#
#     delete_stmt = (
#         delete(user)
#         .where(user.c.id.in_((1, 2)))
#         # .where(user.c.id == address.c.user_id)
#         # .where(address.c.location == 'Orenburg')
#         .returning(user.c.id, user.c.name, user.c.surname)
#     )
#
#     print('DELETED: ', connection.execute(delete_stmt).mappings().all())
#     # print('RESULT: ', connection.execute(select(address)).mappings().all())
#     # print('RESULT: ', connection.execute(select(user)).mappings().all())
#
# metadata.drop_all(engine)
