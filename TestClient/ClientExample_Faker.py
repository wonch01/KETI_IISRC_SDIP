import requests
from faker import Faker
import json
import time


# Faker 초기화
fake = Faker()

# API 주소 설정
base_url = "http://127.0.0.1:5000"  # Flask API가 실행 중인 주소
# base_url = "http://bigsoft.iptime.org:55414"  # Flask API가 실행 중인 주소

# 샘플 데이터 생성 함수
def generate_sample_data():
    # 현재 시간을 ISO 8601 형식으로 변환
    current_time = time.strftime('%Y-%m-%dT%H:%M:%S%z', time.localtime())
    # UTC 오프셋에 콜론 추가 (예: +0900 -> +09:00)
    iso8601_time = current_time[:-2] + ':' + current_time[-2:]
    data = {
        'time': iso8601_time,  # ISO 8601 형식의 날짜/시간
        'sensor_type': fake.random_element(elements=('temperature', 'humidity', 'pressure')),
        'sensor_name': fake.word(),
        'sensor_value': str(fake.random_number(digits=3, fix_len=True)),
        'user_id': fake.uuid4(),
        'location': fake.city(),
        'json': {'extra': fake.sentence()}
    }
    return data

# 데이터 전송 함수
def send_data_to_api(num_samples):
    """
    API에 데이터를 전송하는 함수
    :param num_samples: 생성할 총 데이터 수
    """
    api_url = base_url + "/sensor/InputSensorData"
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


def get_logs_by_sensor_names(start_time, end_time, sensor_names):
    """
    Get logs for specific sensor names between start_time and end_time.
    """
    url = f"{base_url}/sensor/GetLogsBySensorNames"
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "sensor_names": sensor_names
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            print("GetLogsBySensorNames 호출 성공:")
            print(json.dumps(response.json(), indent=4))
        else:
            print(f"GetLogsBySensorNames 호출 실패: {response.status_code}")
            print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"API 호출 중 오류 발생: {e}")


def get_logs_by_type(start_time, end_time, sensor_type):
    """
    Get logs for a specific sensor type between start_time and end_time.
    """
    url = f"{base_url}/sensor/GetLogsByType"
    params = {
        "start_time": start_time,
        "end_time": end_time,
        "sensor_type": sensor_type
    }
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            print("GetLogsByType 호출 성공:")
            print(json.dumps(response.json(), indent=4))
        else:
            print(f"GetLogsByType 호출 실패: {response.status_code}")
            print(response.text)
    except requests.exceptions.RequestException as e:
        print(f"API 호출 중 오류 발생: {e}")

# 실행
if __name__ == "__main__":
    num_samples = 100 # 전송할 데이터 수
    # send_data_to_api(num_samples)  # 연속적으로 데이터를 전송
    result = get_logs_by_sensor_names('2024-10-12 12:08:09.000', '2024-10-18 09:21:34.000', ['blood'])
    print(result)
    result = get_logs_by_type('2024-10-12 12:08:09.000', '2024-10-18 09:21:34.000', 'humidity')
    print(result)