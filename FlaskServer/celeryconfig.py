from celery.schedules import crontab

broker_url = 'redis://keties.iptimes.org:55419/0'
result_backend = 'redis://keties.iptimes.org:55419/0'

# 주기적으로 MongoDB에서 데이터를 가져오는 작업 스케줄
beat_schedule = {
    'transfer-mongo-to-timescale': {
        'task': 'FlaskServer.FlaskServer_main.transfer_data',
        'schedule': crontab(second='*/10'), 
    },
}