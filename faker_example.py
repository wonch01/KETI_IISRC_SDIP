import requests
from faker import Faker
import json
import time

# Faker 초기화
fake = Faker()

# API 주소 설정
# api_url = "http://127.0.0.1:5000/add_sensor_log"  # Flask API가 실행 중인 주소
api_url = "http://keties.iptime.org:55403/add_sensor_log"  # Flask API가 실행 중인 주소

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

# 실행
if __name__ == "__main__":
    num_samples = 1  # 전송할 데이터 수
    send_data_to_api(num_samples)  # 연속적으로 데이터를 전송

# # 데이터 전송 함수
# def send_data_to_api(num_samples, batch_size=100, delay=1):
#     """
#     API에 데이터를 배치 단위로 전송하는 함수
#     :param num_samples: 생성할 총 데이터 수
#     :param batch_size: 한 번에 전송할 데이터 수
#     :param delay: 배치 전송 간의 딜레이 (초)
#     """
#     for i in range(0, num_samples, batch_size):
#         batch_data = [generate_sample_data() for _ in range(batch_size)]
        
#         # API에 배치 전송
#         for data in batch_data:
#             try:
#                 response = requests.post(api_url, json=data)
#                 if response.status_code == 201:
#                     print(f"성공적으로 전송됨: {response.json()}")
#                 else:
#                     print(f"오류 발생: {response.status_code} - {response.text}")
#             except requests.exceptions.RequestException as e:
#                 print(f"API 요청 오류: {e}")
        
#         print(f"{i + batch_size}개의 데이터를 전송했습니다.")
#         time.sleep(delay)  # API에 부하를 주지 않도록 배치 사이에 딜레이

# # 실행
# if __name__ == "__main__":
#     num_samples = 50000  # 전송할 데이터 수
#     send_data_to_api(num_samples, batch_size=100, delay=2)  # 배치당 100개의 데이터, 2초 대기