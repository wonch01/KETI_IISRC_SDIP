from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler
from time import sleep
import psycopg2
from faker import Faker
from datetime import datetime, timedelta
import random
import sys
import os
import threading

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://keti_root:madcoder@keties.iptime.org:55401/timescaledb_sensor'
db = SQLAlchemy(app)

# TimescaleDB 연결 설정
ts_conn = psycopg2.connect(
    host="keties.iptime.org",
    database="timescaledb_sensor",
    user="keti_root",
    password="madcoder",
    port="55401"
)

# MongoDB 연결 설정
mongo_client = MongoClient("mongodb://keti_root:madcoder@keties.iptime.org:55402/")
mongo_db = mongo_client["overflow_data"]
mongo_collection = mongo_db["sensor_data"]

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
    data = request.json
    # MongoDB에 먼저 데이터 저장
    try:
        mongo_collection.insert_one(data)
        return jsonify({"message": "Sensor log added successfully to MongoDB"}), 201
    except Exception as e:
        print(f"MongoDB 저장 오류: {e}")
        return jsonify({"message": "Error storing data in MongoDB"}), 500
    

# TimescaleDB 모델
def save_data_to_timescaledb(sensor_data):
    try:
        with ts_conn.cursor() as cur:
            insert_query = """
            INSERT INTO sensor_logs (time, sensor_type, sensor_name, sensor_value, user_id, location, json)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                sensor_data['time'],
                sensor_data['sensor_type'],
                sensor_data['sensor_name'],
                sensor_data['sensor_value'],
                sensor_data['user_id'],
                sensor_data['location'],
                sensor_data['json']
            ))
        ts_conn.commit()
        print("TimescaleDB에 데이터 저장 성공")
    except Exception as e:
        print(f"TimescaleDB 저장 오류: {e}")
        ts_conn.rollback()

# MongoDB에서 TimescaleDB로 데이터를 이동하는 함수
def transfer_data_from_mongo_to_timescaledb():
    while True:
        try:
            print("스레드가 시작되었습니다.")
            # MongoDB에서 데이터를 가져와서 TimescaleDB로 옮김
            cursor = mongo_collection.find({}).limit(100)  # 일괄 처리 크기
            if cursor.count() != 0:
                for document in cursor:
                    save_data_to_timescaledb(document)
                    mongo_collection.delete_one({'_id': document['_id']})  # 성공적으로 처리된 데이터 삭제
                sleep(5)  # 5초마다 실행
        except Exception as e:
            print(f"백그라운드 데이터 전송 중 오류: {e}")

if __name__ == '__main__':
    # app.run(debug=True)
    threading.Thread(target=transfer_data_from_mongo_to_timescaledb, daemon=True).start()
    app.run(host="0.0.0.0", port=5000, debug=True, threaded=True)