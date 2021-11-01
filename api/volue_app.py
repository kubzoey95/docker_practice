from flask import Flask, request
from sqlalchemy import create_engine, Table, Column, MetaData, Integer, Float, String
from sqlalchemy.orm.session import Session 
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import requests
import threading


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


@app.route('/post_data', methods=['POST'])
def post_data():
    data = request.get_json()
    with Session() as s:
        s.bulk_save_objects([Values(v_name=datum['name'], t=datum['t'], value=datum['v']) for datum in data])
        s.commit()
    return 'ok', 201


def request_task(url, json):
    requests.post(url, json=json)


def fire_and_forget(url, json):
    threading.Thread(target=request_task, args=(url, json)).start()


@app.route("/get_aggregation", methods=['GET'])
def get_aggregation():
    v_name = request.args.get('v_name')
    t_start = request.args.get('from')
    t_end = request.args.get('to')
    with Session() as s:
        q = s.query(Aggregation).filter(Aggregation.v_name == v_name). \
            filter(Aggregation.t_start == t_start). \
            filter(Aggregation.t_end == t_end)
        row = q.first()
        if row is None:
            row = Aggregation(v_name=v_name, t_start=t_start, t_end=t_end)
            s.add(row)
            s.commit()
        if not (row.sum and row.avg):
            fire_and_forget('http://calc:5002/request_aggregation', json={'id': row.id})
    return {'avg': row.avg, 'sum': row.sum}, 200

@app.route("/get_value", methods=['GET'])
def get_value():
    v_name = request.args.get('v_name')
    t = request.args.get('t')
    with Session() as s:
        q = s.query(Values). \
            filter(Values.v_name == v_name). \
            filter(Values.t == t)
        row = q.first()
    return {'value': row.value}, 200

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5001, debug=True)
