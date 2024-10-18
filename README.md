# KETI_IISRC_SDIP

(KETI)  Korea Electronics Technology Institute  <br/>
(IISRC) Intelligent Integrated Software Research Center  <br/>
(SDIP)  Sensor Data Ingest Process

#### 개발 환경
Window 10 <br/>
Python 3.9 <br/>

#### 동작 환경
ubuntu 22.04 <br/>
python 3.9 <br/>
Docker version 25.0.4, <br/>
TimescaleDB version 2.16.1 <br/>
MongoDB version v7.0.11 <br/>
Redis server v=7.2.4 <br/>
celery==5.4.0 <br/>


### [1] 서버 구성도
![image](https://github.com/user-attachments/assets/7e920969-6839-43ab-9349-d9a0afa77d56)

### [2] 모니터링 웹 서비스 사용 (pgAdmin)
주소 : http://bigsoft.iptime.org:55413 <br/>

### [3] API Docs 접속
주소 : http://bigsoft.iptime.org:55414 <br/>
클릭시 API Docs 페이지 이동 <br/>
위 주소에 API 경로를 병합해서 사용 ( e.g. bigsoft.iptime.org:55414/sensor/InputSensorData )

