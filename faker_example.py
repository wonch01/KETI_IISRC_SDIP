import requests
from faker import Faker
import json
import time

# Faker 초기화
fake = Faker()

# API 주소 설정
api_url = "http://127.0.0.1:5000/sensor/InputSensorData"  # Flask API가 실행 중인 주소
# api_url = "http://bigsoft.iptime.org:55414/sensor/InputSensorData"  # Flask API가 실행 중인 주소

# 샘플 데이터 생성 함수
def generate_sample_data():
    data = {
        'time': fake.iso8601(),  # ISO 8601 형식의 날짜/시간
        'sensor_type': fake.random_element(elements=('temperature', 'humidity', 'pressure')),
        'sensor_name': fake.word(),
        'sensor_value': str(fake.random_number(digits=3, fix_len=True)),
        'user_id': fake.uuid4(),
        'location': fake.city(),
        'json': json.dumps({'extra': fake.sentence()})
    }
    return data

# 데이터 전송 함수
def send_data_to_api(num_samples):
    """
    API에 데이터를 전송하는 함수
    :param num_samples: 생성할 총 데이터 수
    """
    for i in range(num_samples):
        data = generate_sample_data()
        
        # API에 데이터 전송
        try:
            response = requests.post(api_url, json=data)
            if response.status_code == 201:
                print(f"성공적으로 전송됨: {response.json()}")
            else:
                print(f"오류 발생: {response.status_code} - {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"API 요청 오류: {e}")

        print(f"{i + 1}개의 데이터를 전송했습니다.")

def query_logs_from_api(start_time, end_time, columns):
    """
    TimescaleDB에서 로그를 조회하는 API 호출 함수
    :param start_time: 조회할 시작 시간 (예: '2024-10-15T00:00:00')
    :param end_time: 조회할 종료 시간 (예: '2024-10-15T23:59:59')
    :param columns: 조회할 컬럼 목록 (예: ['sensor_type', 'sensor_value'])
    :return: JSON 응답 또는 오류 메시지
    """
    url = "http://localhost:5000/query_logs"

    # 요청에 필요한 파라미터 설정
    params = {
        "start_time": start_time,
        "end_time": end_time,
    }
    # 여러 개의 컬럼을 추가하기 위해 `params`에 `columns`를 반복적으로 추가
    for column in columns:
        params.setdefault("columns", []).append(column)
    try:
        # GET 요청 보내기
        response = requests.get(url, params=params)
        # 응답 코드가 200일 때 JSON 반환
        if response.status_code == 200:
            print("API 호출 성공:")
            return response.json()
        else:
            print(f"API 호출 실패: {response.status_code} - {response.text}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"API 호출 중 오류 발생: {e}")

# 실행
if __name__ == "__main__":
    num_samples = 100 # 전송할 데이터 수
    send_data_to_api(num_samples)  # 연속적으로 데이터를 전송
    # query_logs_from_api('2024-07-12 12:08:09.000', '2024-08-23 09:21:34.000', ['sensor_type'])
