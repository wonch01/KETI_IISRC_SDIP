from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from pymongo import MongoClient
import psycopg2
from celery import Celery
from flask_restx import Api, Resource, fields, Namespace
import sys
import os

# 프로젝트 루트 디렉토리를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://keti_root:madcoder@bigsoft.iptime.org:55411/KETI_IISRC_Timescale'
db = SQLAlchemy(app)

# Celery 인스턴스 생성
celery = Celery(__name__, 
                broker='redis://keties.iptimes.org:55419/0', 
                backend='redis://keties.iptimes.org:55419/1')

# TimescaleDB 연결 설정 (커넥션 풀 사용 권장)
def get_ts_conn():
    return psycopg2.connect(
        host="bigsoft.iptime.org",
        database="KETI_IISRC_Timescale",
        user="keti_root",
        password="madcoder",
        port="55411"
    )

# MongoDB 연결 설정
def get_mongo_client():
    """각 워커에서 MongoClient를 생성하는 함수"""
    return MongoClient("mongodb://keti_root:madcoder@bigsoft.iptime.org:55410/")

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
    db.create_all()

# Flask-RestX 설정
api = Api(app, version='1.0', title='Sensor Data API', description='API for handling sensor data input and processing.')
ns_sensor = Namespace('sensor', description='Sensor data operations')
api.add_namespace(ns_sensor)

# 요청 데이터 모델 정의
sensor_data_model = ns_sensor.model('SensorData', {
    'time': fields.String(required=True, description='The timestamp of the sensor data (ISO 8601 format)'),
    'sensor_type': fields.String(required=True, description='Type of the sensor'),
    'sensor_name': fields.String(required=True, description='Name of the sensor'),
    'sensor_value': fields.String(required=True, description='The value read from the sensor'),
    'user_id': fields.String(required=True, description='User ID associated with the sensor data'),
    'location': fields.String(required=True, description='Location where the sensor data was recorded'),
    'json': fields.Raw(required=True, description='Additional sensor data in JSON format')
})


@ns_sensor.route('/InputSensorData')
class InputSensorData(Resource):
    @ns_sensor.expect(sensor_data_model)
    def post(self):
        """Input sensor data and store it in MongoDB."""
        client = get_mongo_client()
        mongo_db = client["overflow_data"]
        mongo_collection = mongo_db["sensor_data"]

        data = request.json

        # MongoDB에 먼저 데이터 저장
        try:
            mongo_collection.insert_one(data)
            # Celery 비동기 작업을 호출
            transfer_data.delay()

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
        mongo_collection.delete_one({'_id': document['_id']})
    client.close()

def save_data_to_timescaledb(sensor_data):
    conn = get_ts_conn()
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
    app.run(host="0.0.0.0", port=5000, debug=True)
