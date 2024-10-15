from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from pymongo import MongoClient
from time import sleep
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random
import sys
import os
import threading
from celery import Celery

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://keti_root:madcoder@keties.iptime.org:55401/timescaledb_sensor'
db = SQLAlchemy(app)

# Flask와 Celery 설정
# def make_celery(app):
#     celery = Celery(
#         app.import_name,
#         broker="redis://keties.iptimes.org:55419/0", 
#         backend="redis://keties.iptimes.org:55419/1"
#     )
#     celery.conf.update(app.config)
#     return celery

# Celery 인스턴스 생성
# celery = make_celery(app)
celery = Celery('FlaskServer_main', 
             broker='redis://keties.iptimes.org:55419/0', 
             backend='redis://keties.iptimes.org:55419/1'
            )

# TimescaleDB 연결 설정 (커넥션 풀 사용 권장)
def get_ts_conn():
    return psycopg2.connect(
        host="keties.iptime.org",
        database="timescaledb_sensor",
        user="keti_root",
        password="madcoder",
        port="55401"
    )

# MongoDB 연결 설정
# mongo_client = MongoClient("mongodb://keti_root:madcoder@keties.iptime.org:55402/")
# mongo_db = mongo_client["overflow_data"]
# mongo_collection = mongo_db["sensor_data"]

def get_mongo_client():
    """각 워커에서 MongoClient를 생성하는 함수"""
    return MongoClient("mongodb://keti_root:madcoder@keties.iptime.org:55402/")

class SensorLog(db.Model):
    __tablename__ = 'sensor_logs'
    time = db.Column(db.DateTime, primary_key=True, nullable=False)
    sensor_type = db.Column(db.String, nullable=False)
    sensor_name = db.Column(db.String, nullable=False)
    sensor_value = db.Column(db.String, nullable=False)
    user_id = db.Column(db.String, nullable=False)
    location = db.Column(db.String, nullable=False)
    json = db.Column(JSONB, nullable=False) 
    
# 서버 시작 시 테이블 자동 생성
with app.app_context():
    db.create_all()  # 이 줄을 추가하여 테이블 자동 생성

@app.route('/add_sensor_log', methods=['POST'])
def add_sensor_log():
    # await asyncio.sleep(1)  # 비동기 작업 예시
    # 워커가 MongoDB 클라이언트를 생성
    client = get_mongo_client()
    mongo_db = client["overflow_data"]
    mongo_collection = mongo_db["sensor_data"]

    data = request.json
    # MongoDB에 먼저 데이터 저장
    try:
        mongo_collection.insert_one(data)
         # Celery 비동기 작업을 호출
        transfer_data.delay()  # 백그라운드 작업으로 데이터를 처리

        return jsonify({"message": "Sensor log saved to MongoDB and processing"}), 201
    except Exception as e:
        print(f"MongoDB 저장 오류: {e}")
        return jsonify({"message": "Error storing data in MongoDB"}), 500
    finally:
        client.close()
    
# Celery 작업 정의
@celery.task
def transfer_data():
    # 워커가 MongoDB 클라이언트를 생성
    client = get_mongo_client()
    mongo_db = client["overflow_data"]
    mongo_collection = mongo_db["sensor_data"]

    cursor = mongo_collection.find({}).limit(100)
    for document in cursor:
        save_data_to_timescaledb(document)
        mongo_collection.delete_one({'_id': document['_id']})  # 데이터 삭제
    client.close()

def save_data_to_timescaledb(sensor_data):
    conn = get_ts_conn()  # 각 작업마다 새 TimescaleDB 커넥션 생성
    try:
        with conn.cursor() as cur:
            query = """
            INSERT INTO sensor_logs (time, sensor_type, sensor_name, sensor_value, user_id, location, json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT ("time") DO NOTHING
            """
            cur.execute(query, (
                sensor_data['time'],
                sensor_data['sensor_type'],
                sensor_data['sensor_name'],
                sensor_data['sensor_value'],
                sensor_data['user_id'],
                sensor_data['location'],
                sensor_data['json']
            ))
        conn.commit()
        print("TimescaleDB에 데이터 저장 성공")
    except Exception as e:
        conn.rollback()
        print(f"TimescaleDB 저장 중 오류 발생: {e}")
    finally:
        conn.close()


if __name__ == '__main__':
    # 비동기 작업 실행
    app.run(host="0.0.0.0", port=5000, debug=True)