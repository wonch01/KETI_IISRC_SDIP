from flask import Flask, request, jsonify, current_app
from flask import make_response, json
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.dialects.postgresql import JSONB
from pymongo import MongoClient
import psycopg2
from celery import Celery
from celery.utils.log import get_task_logger
from flask_restx import Api, Resource, fields, Namespace
from flask_restx import reqparse
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import logging
import sys
import os
from dotenv import load_dotenv

# 프로젝트 루트 디렉토리를 sys.path에 추가
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)
load_dotenv()

app = Flask(__name__)
# app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('POSTGRES_URI_DEV')     #개발용
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('POSTGRES_URI_DEPLOY')    #배포용


db = SQLAlchemy(app)
app.logger.setLevel(logging.DEBUG)

celery = Celery('FlaskServer_main', 
            # broker=os.getenv('CELERY_BROKER_DEV'),        #개발용
            # backend=os.getenv('CELERY_BACKEND_DEV')       #개발용
            broker=os.getenv('CELERY_BROKER_DEPLOY'),       #배포용
            backend=os.getenv('CELERY_BACKEND_DEPLOY')      #배포용
)
# Celery 로거 생성
celery_logger = get_task_logger(__name__)


# TimescaleDB 연결 설정 (커넥션 풀 사용 권장)
def get_ts_conn():
    return psycopg2.connect(app.config['SQLALCHEMY_DATABASE_URI'])

def get_mongo_client():
    """각 워커에서 MongoClient를 생성하는 함수"""
    # mongo_uri = os.getenv('MONGO_URI_DEV')        #개발용
    mongo_uri = os.getenv('MONGO_URI_DEPLOY')       #배포용
    return MongoClient(mongo_uri)


class SensorLog(db.Model):
    __tablename__ = 'sensor_logs'
    time = db.Column(db.DateTime, primary_key=True, nullable=False)
    sensor_type = db.Column(db.String, nullable=False)
    sensor_name = db.Column(db.String, nullable=False)
    sensor_value = db.Column(db.String, nullable=False)
    user_id = db.Column(db.String, nullable=False)
    location = db.Column(db.String, nullable=False)
    json = db.Column(JSONB, nullable=False)


# 테이블 생성 후 하이퍼테이블로 변환
def create_hypertable():
    conn = get_ts_conn()
    try:
        with conn.cursor() as cur:
            # 하이퍼테이블 생성
            cur.execute("SELECT create_hypertable('sensor_logs', 'time', if_not_exists => TRUE);")
            # time 컬럼에 추가 인덱스 생성
            cur.execute("CREATE INDEX IF NOT EXISTS idx_sensor_logs_time ON sensor_logs (time DESC);")
        conn.commit()
        current_app.logger.info("하이퍼테이블 및 인덱스 생성 완료")
    except Exception as e:
        conn.rollback()
        current_app.logger.info(f"하이퍼테이블 생성 중 오류 발생: {e}")
    finally:
        conn.close()

# 서버 시작 시 테이블 자동 생성
with app.app_context():
    db.create_all()  # 이 줄을 추가하여 테이블 자동 생성
    create_hypertable()  # 하이퍼테이블로 변환

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
        current_app.logger.info("Input sensor data and store it in MongoDB")
        """Input sensor data and store it in MongoDB."""
        client = get_mongo_client()
        mongo_db = client["overflow_data"]
        mongo_collection = mongo_db["sensor_data"]
        data = request.json
        # MongoDB에 먼저 데이터 저장
        try:
            mongo_collection.insert_one(data)   

            return {"message": "Sensor log saved to MongoDB and processing"}, 201
        except Exception as e:
            current_app.logger.info(f"MongoDB 저장 오류: {e}")
            return {"message": "Error storing data in MongoDB"}, 500
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
                json.dumps(sensor_data['json'])  # JSON 문자열로 변환
            ))
        conn.commit()
        celery_logger.info("TimescaleDB에 데이터 저장 성공")
    except Exception as e:
        conn.rollback()
        celery_logger.info(f"TimescaleDB 저장 중 오류 발생: {e}")
    finally:
        conn.close()


# APScheduler로 주기적 작업 설정 (10초마다 실행)
scheduler = BackgroundScheduler()
scheduler.add_job(transfer_data, 'interval', seconds=10)
scheduler.start()


# 날짜 및 sensor_name 또는 sensor_type별 데이터 조회 API
query_model = ns_sensor.model('QueryModel', {
    'start_date': fields.String(required=True, description='Start date in ISO 8601 format'),
    'end_date': fields.String(required=True, description='End date in ISO 8601 format'),
    'sensor_name': fields.String(required=False, description='Name of the sensor to filter'),
    'sensor_type': fields.String(required=False, description='Type of the sensor to filter')
})

# 쿼리 파라미터를 정의하기 위한 파서 설정
log_query_sensor_name_parser = reqparse.RequestParser()
log_query_sensor_name_parser.add_argument('start_time', required=True, type=str, help='Start time in ISO 8601 format (e.g., 2024-01-01 or 2024-01-01T00:00)')
log_query_sensor_name_parser.add_argument('end_time', required=True, type=str, help='End time in ISO 8601 format (e.g., 2024-01-02 or 2024-01-01T23:59)')
log_query_sensor_name_parser.add_argument('sensor_names', required=True, action='split', help='Comma-separated list of sensor names to retrieve (e.g., temperature_sensor,humidity_sensor)')

@ns_sensor.route('/GetLogsBySensorNames')
class GetLogsBySensorNames(Resource):
    @ns_sensor.expect(log_query_sensor_name_parser)
    @ns_sensor.response(200, 'Success', fields.List(fields.Raw))
    @ns_sensor.response(400, 'Missing required parameters')
    @ns_sensor.response(500, 'Database query failed or an unexpected error occurred')
    def get(self):
        """Retrieve sensor logs between start_time and end_time for specified sensor names"""
        current_app.logger.info("GetLogsBySensorNames 호출")
        try:
            # 시간 파라미터 (필수)
            start_time_str = request.args.get('start_time')
            end_time_str = request.args.get('end_time')
            
            # 조회할 센서 이름들 (가변적)
            sensor_names = request.args.getlist('sensor_names')
            
            # 기본적인 유효성 검사
            if not start_time_str or not end_time_str or not sensor_names:
                return {"error": "Missing required parameters"}, 400   
            
            # ISO 8601 포맷을 파싱
            try:
                start_time = datetime.fromisoformat(start_time_str)
            except ValueError:
                return {"error": "Invalid start_time format. Use ISO 8601 (e.g., 2024-01-01 or 2024-01-01T00:00)"}, 400
            try:
                end_time = datetime.fromisoformat(end_time_str)
            except ValueError:
                return {"error": "Invalid end_time format. Use ISO 8601 (e.g., 2024-01-02 or 2024-01-01T23:59)"}, 400
            # Sensor names를 SQL 쿼리에 사용할 수 있도록 튜플 형태로 변환
            sensor_names_tuple = tuple(sensor_names)            
            # 쿼리 실행
            conn = get_ts_conn()
            with conn.cursor() as cur:
                query = """
                    SELECT * 
                    FROM sensor_logs 
                    WHERE time BETWEEN %s AND %s
                    AND sensor_name IN %s
                """
                cur.execute(query, (start_time, end_time, sensor_names_tuple))
                result = cur.fetchall()
                # 컬럼명 가져오기 (cursor.description 사용)
                columns = [desc[0] for desc in cur.description]
            # 결과를 JSON으로 반환
            result_data = [dict(zip(columns, row)) for row in result]
            response = make_response(json.dumps(result_data), 200)
            response.headers["Content-Type"] = "application/json"
            return response
        
        except psycopg2.Error as e:
            current_app.logger.info(f"쿼리 실행 중 오류: {e}")
            return {"error": "Database query failed"}, 500
        except Exception as e:
            current_app.logger.info(f"예외 발생: {e}")
            return {"error": "An unexpected error occurred"}, 500
        finally:
            conn.close()


# 쿼리 파라미터를 정의하기 위한 파서 설정 (Type 별 조회용)
log_query_type_parser = reqparse.RequestParser()
log_query_type_parser.add_argument('start_time', required=True, type=str, help='Start time in ISO 8601 format (e.g., 2024-01-01 or 2024-01-01T00:00)')
log_query_type_parser.add_argument('end_time', required=True, type=str, help='End time in ISO 8601 format (e.g., 2024-01-02 or 2024-01-01T23:59)')
log_query_type_parser.add_argument('sensor_type', required=True, type=str, help='Sensor type to filter (e.g., temperature, humidity)')

@ns_sensor.route('/GetLogsBySensorType')
class GetLogsBySensorType(Resource):
    @ns_sensor.expect(log_query_type_parser)
    @ns_sensor.response(200, 'Success', fields.List(fields.Raw))
    @ns_sensor.response(400, 'Missing required parameters')
    @ns_sensor.response(500, 'Database query failed or an unexpected error occurred')
    def get(self):
        """Retrieve sensor logs between start_time and end_time filtered by sensor_type"""
        current_app.logger.info("GetLogsByType 호출")
        try:
            # 시간 파라미터 및 센서 타입 (필수)
            start_time_str = request.args.get('start_time')
            end_time_str  = request.args.get('end_time')
            sensor_type = request.args.get('sensor_type')
            
            # 기본적인 유효성 검사
            if not start_time_str or not end_time_str or not sensor_type:
                return {"error": "Missing required parameters"}, 400
            
            # ISO 8601 포맷을 파싱
            try:
                start_time = datetime.fromisoformat(start_time_str)
            except ValueError:
                return {"error": "Invalid start_time format. Use ISO 8601 (e.g., 2024-01-01 or 2024-01-01T00:00)"}, 400
            try:
                end_time = datetime.fromisoformat(end_time_str)
            except ValueError:
                return {"error": "Invalid end_time format. Use ISO 8601 (e.g., 2024-01-02 or 2024-01-01T23:59)"}, 400
            
            # 쿼리 실행
            conn = get_ts_conn()
            with conn.cursor() as cur:
                query = """
                    SELECT * 
                    FROM sensor_logs 
                    WHERE time BETWEEN %s AND %s
                    AND sensor_type = %s
                """
                cur.execute(query, (start_time, end_time, sensor_type))
                result = cur.fetchall()
                # 컬럼명 가져오기 (cursor.description 사용)
                columns = [desc[0] for desc in cur.description]
            # 결과를 JSON으로 반환
            result_data = [dict(zip(columns, row)) for row in result]
            response = make_response(json.dumps(result_data), 200)
            response.headers["Content-Type"] = "application/json"
            return response
        
        except psycopg2.Error as e:
            current_app.logger.info(f"쿼리 실행 중 오류: {e}")
            return {"error": "Database query failed"}, 500
        except Exception as e:
            current_app.logger.info(f"예외 발생: {e}")
            return {"error": "An unexpected error occurred"}, 500
        finally:
            conn.close()


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
