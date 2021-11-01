from flask import Flask, request
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, Float, String
from sqlalchemy.orm.session import Session 
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

convention = {
  "ix": 'ix_%(column_0_label)s',
  "uq": "uq_%(table_name)s_%(column_0_name)s",
  "ck": "ck_%(table_name)s_%(constraint_name)s",
  "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
  "pk": "pk_%(table_name)s"
}

app = Flask(__name__)
engine = create_engine('mysql://root:root@db/dbname')
Session = sessionmaker(engine)
Base = declarative_base(engine)
metadata = Base.metadata
metadata.naming_convention = convention


class Values(Base):
    __tablename__ = 'values'
    id = Column(Integer, primary_key=True)
    v_name = Column('v_name', String(10))
    t = Column('t', Integer)
    value = Column('value', Float)


class Aggregation(Base):
    __tablename__ = 'aggregation'
    id = Column(Integer, primary_key=True)
    v_name = Column('v_name', String(10))
    t_start = Column('t_start', Integer)
    t_end = Column('t_end', Integer)
    avg = Column('avg', Float)
    sum = Column('sum', Float)


if not database_exists(engine.url):
    engine.execute("CREATE DATABASE dbname")
    engine.execute("USE dbname")
    metadata.create_all()


@app.route('/request_aggregation', methods=['POST'])
def request_aggregation():
    data = request.get_json()
    with Session() as s:
        row = s.query(Aggregation).filter(Aggregation.id == data['id']).first()
        q = s.query(func.avg(Values.value).\
        label('average'), func.sum(Values.value).\
        label('sum')).\
        filter(Values.v_name==row.v_name).\
        filter(Values.t >= row.t_start).\
        filter(Values.t <= row.t_end)
        result = q.first()
        row.avg = result.average
        row.sum = result.sum
        s.commit()
    return 'ok', 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
