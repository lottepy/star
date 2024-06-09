import os

RABBITMQ_ADDR = os.getenv("RABBITMQ_ADDR","amqp://aqumon:aqumon2050@172.31.74.168:5673/datamaster_dev")
DINGDING_ADDR = os.getenv("DINGDING_ADDR","https://oapi.dingtalk.com/robot/send?access_token=2ced390605a6cd28038e59662d2810e849606dd5fab0b184fd1fd30ea244ef5f")